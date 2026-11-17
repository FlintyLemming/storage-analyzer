package dupcheck

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	quickChunk = 64 << 10       // bytes read from each end for the quick digest
	smallFile  = 2 * quickChunk // files up to this size are read whole once
	fullBuf    = 1 << 20
)

// Candidates is a compact table of files that share their size with
// another file. Rows hold no pointers, so millions of them stay cheap in
// memory and for the garbage collector; names live in one shared buffer.
type Candidates struct {
	rows  []row
	names []byte
}

type row struct {
	id, dir, size, mtime int64 // scan id, parent dir id, size and mtime (s) at scan time
	dev, ino             uint64
	mtimeNS, ctimeNS     int64
	quick, full          Digest
	nameOff              uint32
	nameLen              uint16
	flags                uint8
}

const (
	fBad   = 1 << iota // dropped: changed, unreadable, or no longer shares its key
	fQuick             // quick digest known
	fFull              // full digest known
)

// Add appends a file: scan id, parent directory id, name, and its apparent
// size and mtime (seconds) at scan time.
func (c *Candidates) Add(id, dir int64, name []byte, size, mtime int64) {
	r := row{id: id, dir: dir, size: size, mtime: mtime, nameOff: uint32(len(c.names)), nameLen: uint16(len(name))}
	if len(name) > math.MaxUint16 {
		r.flags, r.nameLen = fBad, 0
	} else {
		c.names = append(c.names, name...)
	}
	c.rows = append(c.rows, r)
}

// Len returns the number of rows.
func (c *Candidates) Len() int { return len(c.rows) }

func (c *Candidates) name(r *row) []byte { return c.names[r.nameOff : r.nameOff+uint32(r.nameLen)] }

// Progress is updated while a check runs and may be read concurrently.
type Progress struct {
	Phase   atomic.Value // string: prepare | quick | full | rollup | write
	Current atomic.Value // string: file being read

	DoneFiles, TotalFiles, DoneBytes, TotalBytes atomic.Int64
	CacheHits, Changed, Errors, BytesRead        atomic.Int64
}

// Begin starts a phase with known totals.
func (p *Progress) Begin(phase string, files, bytes int64) {
	p.Phase.Store(phase)
	p.TotalFiles.Store(files)
	p.TotalBytes.Store(bytes)
	p.DoneFiles.Store(0)
	p.DoneBytes.Store(0)
}

// PhaseName returns the current phase.
func (p *Progress) PhaseName() string { s, _ := p.Phase.Load().(string); return s }

// CurrentPath returns the file being read.
func (p *Progress) CurrentPath() string { s, _ := p.Current.Load().(string); return s }

// Hasher narrows candidates down to files with identical content: size,
// then quick digest, then full SHA-256. Files that changed since the scan
// (or while being read) are dropped and counted in Progress.Changed.
type Hasher struct {
	FS           FS
	Cache        *Cache // optional
	Progress     *Progress
	QuickWorkers int                                 // default 4
	Path         func(dir int64, name []byte) string // filesystem path of a row; default: the name itself
}

type digestKey struct {
	size int64
	d    Digest
}

// keepShared keeps the live rows whose key is shared with another live row
// and marks the others bad.
func keepShared[K comparable](rows []row, idx []int32, key func(*row) K) []int32 {
	n := map[K]int32{}
	for _, i := range idx {
		if rows[i].flags&fBad == 0 {
			n[key(&rows[i])]++
		}
	}
	out := idx[:0]
	for _, i := range idx {
		r := &rows[i]
		if r.flags&fBad != 0 {
			continue
		}
		if n[key(r)] < 2 {
			r.flags |= fBad
			continue
		}
		out = append(out, i)
	}
	return out
}

func gone(err error) bool {
	return errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR)
}

func (r *row) info() FileInfo {
	return FileInfo{Dev: r.dev, Ino: r.ino, Size: r.size, MTime: r.mtime, MTimeNS: r.mtimeNS, CTimeNS: r.ctimeNS, Regular: true}
}

func (h *Hasher) path(c *Candidates, r *row) string {
	if h.Path == nil {
		return string(c.name(r))
	}
	return h.Path(r.dir, c.name(r))
}

// Run returns the confirmed duplicate files and their full digests. It
// sorts c's rows by id.
func (h *Hasher) Run(ctx context.Context, c *Candidates) (map[int64]Digest, error) {
	if h.FS == nil {
		h.FS = OSFS()
	}
	if h.Progress == nil {
		h.Progress = &Progress{}
	}
	if h.QuickWorkers <= 0 {
		h.QuickWorkers = 4
	}
	p := h.Progress
	rows := c.rows
	// Scan ids follow directory listing order, so reading by id keeps
	// lstat and reads close together on disk instead of seeking at random.
	slices.SortFunc(rows, func(a, b row) int { return compareInt(a.id, b.id) })

	// Verify against the scan and consult the cache.
	p.Begin("prepare", int64(len(rows)), 0)
	idx := make([]int32, 0, len(rows))
	for i := range rows {
		r := &rows[i]
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if r.flags&fBad != 0 {
			continue
		}
		path := h.path(c, r)
		if i%256 == 0 {
			p.Current.Store(path)
		}
		fi, err := h.FS.Lstat(path)
		p.DoneFiles.Add(1)
		switch {
		case err != nil && gone(err):
			p.Changed.Add(1)
			r.flags |= fBad
			continue
		case err != nil:
			p.Errors.Add(1)
			r.flags |= fBad
			continue
		case !fi.Regular || fi.Size != r.size || fi.MTime != r.mtime:
			p.Changed.Add(1)
			r.flags |= fBad
			continue
		}
		r.dev, r.ino, r.mtimeNS, r.ctimeNS = fi.Dev, fi.Ino, fi.MTimeNS, fi.CTimeNS
		if h.Cache != nil {
			if e, ok := h.Cache.Lookup(path, fi); ok {
				r.quick, r.flags = e.Quick, r.flags|fQuick
				if e.Full != nil {
					r.full, r.flags = *e.Full, r.flags|fFull
				}
				p.CacheHits.Add(1)
				if err := h.Cache.Touch(path); err != nil {
					return nil, err
				}
			}
		}
		idx = append(idx, int32(i))
	}
	live := keepShared(rows, idx, func(r *row) int64 { return r.size })

	// Quick digests.
	var need []int32
	var nb int64
	for _, i := range live {
		if rows[i].flags&fQuick == 0 {
			need = append(need, i)
			nb += min(rows[i].size, smallFile)
		}
	}
	p.Begin("quick", int64(len(need)), nb)
	if err := h.parallel(ctx, need, func(ctx context.Context, i int32) error { return h.quick(ctx, c, &rows[i]) }); err != nil {
		return nil, err
	}
	live = keepShared(rows, live, func(r *row) digestKey { return digestKey{r.size, r.quick} })

	// Full digests, one file at a time.
	need, nb = need[:0], 0
	for _, i := range live {
		if rows[i].flags&fFull == 0 {
			need = append(need, i)
			nb += rows[i].size
		}
	}
	p.Begin("full", int64(len(need)), nb)
	for _, i := range need {
		if err := h.fullHash(ctx, c, &rows[i]); err != nil {
			return nil, err
		}
	}
	live = keepShared(rows, live, func(r *row) digestKey { return digestKey{r.size, r.full} })

	out := make(map[int64]Digest, len(live))
	for _, i := range live {
		out[rows[i].id] = rows[i].full
	}
	return out, nil
}

func (h *Hasher) parallel(ctx context.Context, idx []int32, fn func(context.Context, int32) error) error {
	jobs := make(chan int32)
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for w := 0; w < h.QuickWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := fn(cctx, i); err != nil {
					once.Do(func() { firstErr = err; cancel() })
				}
			}
		}()
	}
feed:
	for _, i := range idx {
		select {
		case jobs <- i:
		case <-cctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// unchanged reports whether the file still matches what lstat showed before
// reading.
func (h *Hasher) unchanged(path string, r *row) bool {
	fi, err := h.FS.Lstat(path)
	return err == nil && fi == r.info()
}

func (h *Hasher) put(path string, r *row) error {
	if h.Cache == nil {
		return nil
	}
	e := CacheEntry{Quick: r.quick}
	if r.flags&fFull != 0 {
		full := r.full
		e.Full = &full
	}
	return h.Cache.Put(path, r.info(), e)
}

// quick computes the quick digest; small files are read whole, which also
// gives their full digest.
func (h *Hasher) quick(ctx context.Context, c *Candidates, r *row) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := h.Progress
	path := h.path(c, r)
	p.Current.Store(path)
	defer p.DoneFiles.Add(1)
	f, err := h.FS.Open(path)
	if err != nil {
		p.Errors.Add(1)
		r.flags |= fBad
		return nil
	}
	defer f.Close()
	var buf []byte
	if r.size <= smallFile {
		buf = make([]byte, r.size)
		_, err = io.ReadFull(f, buf)
	} else {
		buf = make([]byte, 2*quickChunk)
		if _, err = f.ReadAt(buf[:quickChunk], 0); err == nil {
			_, err = f.ReadAt(buf[quickChunk:], r.size-quickChunk)
		}
	}
	p.BytesRead.Add(int64(len(buf)))
	p.DoneBytes.Add(int64(len(buf)))
	if err != nil {
		p.Errors.Add(1)
		r.flags |= fBad
		return nil
	}
	r.quick = sha256.Sum256(buf)
	r.flags |= fQuick
	if r.size <= smallFile {
		if !h.unchanged(path, r) {
			p.Changed.Add(1)
			r.flags |= fBad
			return nil
		}
		r.full = r.quick
		r.flags |= fFull
	}
	return h.put(path, r)
}

// fullHash reads the whole file, then checks it did not change meanwhile.
func (h *Hasher) fullHash(ctx context.Context, c *Candidates, r *row) error {
	p := h.Progress
	path := h.path(c, r)
	p.Current.Store(path)
	defer p.DoneFiles.Add(1)
	f, err := h.FS.Open(path)
	if err != nil {
		p.Errors.Add(1)
		r.flags |= fBad
		return nil
	}
	defer f.Close()
	sum := sha256.New()
	buf := make([]byte, fullBuf)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, rerr := f.Read(buf)
		sum.Write(buf[:n])
		total += int64(n)
		p.BytesRead.Add(int64(n))
		p.DoneBytes.Add(int64(n))
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			p.Errors.Add(1)
			r.flags |= fBad
			return nil
		}
	}
	if total != r.size || !h.unchanged(path, r) {
		p.Changed.Add(1)
		r.flags |= fBad
		return nil
	}
	sum.Sum(r.full[:0])
	r.flags |= fFull
	return h.put(path, r)
}
