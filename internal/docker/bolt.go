package docker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"time"
)

const (
	boltMagic          uint32 = 0xED0CDAED
	boltVersion        uint32 = 2
	boltBranchPageFlag uint16 = 0x01
	boltLeafPageFlag   uint16 = 0x02
	boltMetaPageFlag   uint16 = 0x04
	boltBucketLeafFlag uint32 = 0x01
	boltPageHeaderSize        = 16
	boltMetaSize              = 64
	boltHeaderReadSize        = boltPageHeaderSize + boltMetaSize
	boltElemSize              = 16
	boltInBucketSize          = 16
	maxBoltDepth              = 64
	maxBoltOverflow    uint32 = 4096
	maxBoltPages              = 500000
	maxReadAttempts           = 5
)

var errBoltRetry = errors.New("bolt concurrent modification")

type boltMeta struct {
	pageSize uint32
	root     uint64
	pgid     uint64
	txid     uint64
}

func parseBoltMeta(buf []byte) (boltMeta, bool) {
	if len(buf) < boltHeaderReadSize {
		return boltMeta{}, false
	}
	flags := binary.LittleEndian.Uint16(buf[8:10])
	if flags&boltMetaPageFlag == 0 {
		return boltMeta{}, false
	}
	m := buf[boltPageHeaderSize:boltHeaderReadSize]
	if binary.LittleEndian.Uint32(m[0:4]) != boltMagic {
		return boltMeta{}, false
	}
	if binary.LittleEndian.Uint32(m[4:8]) != boltVersion {
		return boltMeta{}, false
	}
	ps := binary.LittleEndian.Uint32(m[8:12])
	if ps < 4096 || ps > 65536 || ps&(ps-1) != 0 {
		return boltMeta{}, false
	}
	h := fnv.New64a()
	_, _ = h.Write(m[:56])
	if h.Sum64() != binary.LittleEndian.Uint64(m[56:64]) {
		return boltMeta{}, false
	}
	return boltMeta{
		pageSize: ps,
		root:     binary.LittleEndian.Uint64(m[16:24]),
		pgid:     binary.LittleEndian.Uint64(m[40:48]),
		txid:     binary.LittleEndian.Uint64(m[48:56]),
	}, true
}

func readBoltMeta(f *os.File) (boltMeta, error) {
	var p0 [boltHeaderReadSize]byte
	if _, err := f.ReadAt(p0[:], 0); err != nil {
		return boltMeta{}, fmt.Errorf("read bolt meta0: %w", err)
	}
	best, valid := parseBoltMeta(p0[:])

	probeSizes := []uint32{4096, 16384, 65536}
	if valid {
		probeSizes = []uint32{best.pageSize}
	}
	var p1 [boltHeaderReadSize]byte
	for _, ps := range probeSizes {
		if _, err := f.ReadAt(p1[:], int64(ps)); err != nil {
			continue
		}
		if m1, ok1 := parseBoltMeta(p1[:]); ok1 && m1.pageSize == ps {
			if !valid || m1.txid > best.txid {
				best = m1
			}
			valid = true
			break
		}
	}
	if !valid {
		return boltMeta{}, errors.New("not a valid BoltDB file")
	}
	return best, nil
}

// viewBoltNoLock reads a possibly live, exclusively locked BoltDB file directly
// via pread without acquiring flock and without copying the file. It verifies
// BoltDB meta-page txid before and after fn runs, retrying if a write committed
// concurrently. Callers must reset any accumulated state at the start of fn.
func viewBoltNoLock(dbPath string, fn func(root *boltBucket) error) error {
	f, err := os.Open(dbPath)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("%s: not a regular file", dbPath)
	}

	for attempt := 0; attempt < maxReadAttempts; attempt++ {
		before, err := readBoltMeta(f)
		if err != nil {
			return err
		}
		br := &boltReader{
			f:        f,
			pageSize: before.pageSize,
			maxPgid:  before.pgid,
		}
		root := &boltBucket{r: br, root: before.root}
		callErr := fn(root)

		after, metaErr := readBoltMeta(f)
		if metaErr == nil && after.txid == before.txid {
			if callErr == nil {
				return nil
			}
			if !errors.Is(callErr, errBoltRetry) {
				return callErr
			}
			return fmt.Errorf("%s: corrupt BoltDB page layout", dbPath)
		}
		// A write transaction committed while we were reading; retry.
		time.Sleep(time.Duration(attempt+1) * 20 * time.Millisecond)
	}
	return fmt.Errorf("%s: database changed continuously during read", dbPath)
}

type boltReader struct {
	f        *os.File
	pageSize uint32
	maxPgid  uint64
	visited  int
}

func (r *boltReader) readPage(pgid uint64) ([]byte, error) {
	if pgid < 2 || pgid >= r.maxPgid {
		return nil, errBoltRetry
	}
	r.visited++
	if r.visited > maxBoltPages {
		return nil, errBoltRetry
	}
	ps := int64(r.pageSize)
	off := int64(pgid) * ps
	buf := make([]byte, ps)
	if _, err := r.f.ReadAt(buf, off); err != nil {
		return nil, errBoltRetry
	}
	overflow := binary.LittleEndian.Uint32(buf[12:16])
	if overflow == 0 {
		return buf, nil
	}
	if overflow > maxBoltOverflow || pgid+uint64(overflow) >= r.maxPgid {
		return nil, errBoltRetry
	}
	full := make([]byte, (1+int64(overflow))*ps)
	copy(full, buf)
	if _, err := r.f.ReadAt(full[ps:], off+ps); err != nil {
		return nil, errBoltRetry
	}
	return full, nil
}

type boltBucket struct {
	r      *boltReader
	root   uint64
	inline []byte
}

func (b *boltBucket) loadPage(pgid uint64) ([]byte, error) {
	if pgid == 0 && b.inline != nil {
		return b.inline, nil
	}
	return b.r.readPage(pgid)
}

func parseLeafElem(page []byte, idx int) (flags uint32, k, v []byte, ok bool) {
	off := uint64(boltPageHeaderSize) + uint64(idx)*boltElemSize
	if off+boltElemSize > uint64(len(page)) {
		return 0, nil, nil, false
	}
	flags = binary.LittleEndian.Uint32(page[off : off+4])
	pos := uint64(binary.LittleEndian.Uint32(page[off+4 : off+8]))
	ksize := uint64(binary.LittleEndian.Uint32(page[off+8 : off+12]))
	vsize := uint64(binary.LittleEndian.Uint32(page[off+12 : off+16]))
	kStart := off + pos
	vStart := kStart + ksize
	vEnd := vStart + vsize
	if vEnd > uint64(len(page)) || kStart < off || vStart < kStart || vEnd < vStart {
		return 0, nil, nil, false
	}
	return flags, page[kStart:vStart], page[vStart:vEnd], true
}

func parseBranchElem(page []byte, idx int) (k []byte, child uint64, ok bool) {
	off := uint64(boltPageHeaderSize) + uint64(idx)*boltElemSize
	if off+boltElemSize > uint64(len(page)) {
		return nil, 0, false
	}
	pos := uint64(binary.LittleEndian.Uint32(page[off : off+4]))
	ksize := uint64(binary.LittleEndian.Uint32(page[off+4 : off+8]))
	child = binary.LittleEndian.Uint64(page[off+8 : off+16])
	kStart := off + pos
	kEnd := kStart + ksize
	if kEnd > uint64(len(page)) || kStart < off || kEnd < kStart {
		return nil, 0, false
	}
	return page[kStart:kEnd], child, true
}

func (b *boltBucket) subBucketFromVal(v []byte) (*boltBucket, error) {
	if len(v) < boltInBucketSize {
		return nil, errBoltRetry
	}
	root := binary.LittleEndian.Uint64(v[0:8])
	if root == 0 {
		inline := v[boltInBucketSize:]
		if len(inline) < boltPageHeaderSize {
			return nil, errBoltRetry
		}
		return &boltBucket{r: b.r, inline: inline}, nil
	}
	return &boltBucket{r: b.r, root: root}, nil
}

func (b *boltBucket) search(target []byte) (flags uint32, v []byte, found bool, err error) {
	pgid := b.root
	for depth := 0; depth < maxBoltDepth; depth++ {
		page, err := b.loadPage(pgid)
		if err != nil {
			return 0, nil, false, err
		}
		if len(page) < boltPageHeaderSize {
			return 0, nil, false, errBoltRetry
		}
		pflags := binary.LittleEndian.Uint16(page[8:10])
		count := int(binary.LittleEndian.Uint16(page[10:12]))

		if pflags&boltBranchPageFlag != 0 {
			if count == 0 {
				return 0, nil, false, nil
			}
			var bad bool
			idx := sort.Search(count, func(i int) bool {
				k, _, ok := parseBranchElem(page, i)
				if !ok {
					bad = true
					return true
				}
				return bytes.Compare(k, target) > 0
			}) - 1
			if bad {
				return 0, nil, false, errBoltRetry
			}
			if idx < 0 {
				idx = 0
			}
			_, child, ok := parseBranchElem(page, idx)
			if !ok {
				return 0, nil, false, errBoltRetry
			}
			pgid = child
			continue
		}

		if pflags&boltLeafPageFlag != 0 {
			if count == 0 {
				return 0, nil, false, nil
			}
			var bad bool
			idx := sort.Search(count, func(i int) bool {
				_, k, _, ok := parseLeafElem(page, i)
				if !ok {
					bad = true
					return true
				}
				return bytes.Compare(k, target) >= 0
			})
			if bad {
				return 0, nil, false, errBoltRetry
			}
			if idx >= count {
				return 0, nil, false, nil
			}
			eflags, k, val, ok := parseLeafElem(page, idx)
			if !ok {
				return 0, nil, false, errBoltRetry
			}
			if bytes.Equal(k, target) {
				return eflags, val, true, nil
			}
			return 0, nil, false, nil
		}

		return 0, nil, false, errBoltRetry
	}
	return 0, nil, false, errBoltRetry
}

// Bucket looks up a direct child sub-bucket by name. Returns (nil, nil) if absent.
func (b *boltBucket) Bucket(name []byte) (*boltBucket, error) {
	flags, v, found, err := b.search(name)
	if err != nil || !found || flags&boltBucketLeafFlag == 0 {
		return nil, err
	}
	return b.subBucketFromVal(v)
}

// Get looks up a direct leaf value by key. Returns (nil, nil) if absent or a sub-bucket.
func (b *boltBucket) Get(key []byte) ([]byte, error) {
	flags, v, found, err := b.search(key)
	if err != nil || !found || flags&boltBucketLeafFlag != 0 {
		return nil, err
	}
	return v, nil
}

// ForEach visits every key/value and sub-bucket in b in key order.
func (b *boltBucket) ForEach(fn func(k, v []byte, sub *boltBucket) error) error {
	return b.walk(b.root, 0, fn)
}

func (b *boltBucket) walk(pgid uint64, depth int, fn func(k, v []byte, sub *boltBucket) error) error {
	if depth >= maxBoltDepth {
		return errBoltRetry
	}
	page, err := b.loadPage(pgid)
	if err != nil {
		return err
	}
	if len(page) < boltPageHeaderSize {
		return errBoltRetry
	}
	pflags := binary.LittleEndian.Uint16(page[8:10])
	count := int(binary.LittleEndian.Uint16(page[10:12]))

	if pflags&boltBranchPageFlag != 0 {
		for i := 0; i < count; i++ {
			_, child, ok := parseBranchElem(page, i)
			if !ok {
				return errBoltRetry
			}
			if err := b.walk(child, depth+1, fn); err != nil {
				return err
			}
		}
		return nil
	}

	if pflags&boltLeafPageFlag != 0 {
		for i := 0; i < count; i++ {
			eflags, k, v, ok := parseLeafElem(page, i)
			if !ok {
				return errBoltRetry
			}
			if eflags&boltBucketLeafFlag != 0 {
				sub, err := b.subBucketFromVal(v)
				if err != nil {
					return err
				}
				if err := fn(k, nil, sub); err != nil {
					return err
				}
			} else {
				if err := fn(k, v, nil); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return errBoltRetry
}
