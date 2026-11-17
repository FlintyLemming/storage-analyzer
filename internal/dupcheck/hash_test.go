package dupcheck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"testing"
)

// countFS wraps the real filesystem, counting opens and optionally
// injecting failures or changes.
type countFS struct {
	FS
	mu        sync.Mutex
	opens     map[string]int
	lstats    map[string]int
	denyOpen  string
	bumpCtime string // second and later Lstat of this path report a new ctime
}

func newCountFS() *countFS {
	return &countFS{FS: OSFS(), opens: map[string]int{}, lstats: map[string]int{}}
}

func (f *countFS) Open(p string) (File, error) {
	f.mu.Lock()
	f.opens[p]++
	f.mu.Unlock()
	if p == f.denyOpen {
		return nil, &os.PathError{Op: "open", Path: p, Err: syscall.EACCES}
	}
	return f.FS.Open(p)
}

func (f *countFS) Lstat(p string) (FileInfo, error) {
	fi, err := f.FS.Lstat(p)
	f.mu.Lock()
	f.lstats[p]++
	n := f.lstats[p]
	f.mu.Unlock()
	if p == f.bumpCtime && n > 1 {
		fi.CTimeNS++
	}
	return fi, err
}

func (f *countFS) totalOpens() int {
	n := 0
	for _, v := range f.opens {
		n += v
	}
	return n
}

var nextCandID int64

// Candidate is a test file: its full path is stored as the row name.
type Candidate struct {
	ID    int64
	Path  string
	Size  int64
	MTime int64
}

func table(groups ...[]Candidate) *Candidates {
	c := &Candidates{}
	for _, g := range groups {
		for _, x := range g {
			c.Add(x.ID, 0, []byte(x.Path), x.Size, x.MTime)
		}
	}
	return c
}

func writeCand(t *testing.T, dir, name string, content []byte) Candidate {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := OSFS().Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	nextCandID++
	return Candidate{ID: nextCandID, Path: p, Size: fi.Size, MTime: fi.MTime}
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) + seed
	}
	return b
}

func runHasher(t *testing.T, h *Hasher, groups ...[]Candidate) map[int64]Digest {
	t.Helper()
	if h.Progress == nil {
		h.Progress = &Progress{}
	}
	got, err := h.Run(context.Background(), table(groups...))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func keys(m map[int64]Digest) []int64 {
	var out []int64
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestHashFindsSameContent(t *testing.T) {
	d := t.TempDir()
	same := pattern(300<<10, 1)
	a, b, c := writeCand(t, d, "a", same), writeCand(t, d, "b", same), writeCand(t, d, "c", same)
	x := writeCand(t, d, "x", pattern(300<<10, 2))
	got := runHasher(t, &Hasher{FS: OSFS()}, []Candidate{a, b, c, x})
	if !slices.Equal(keys(got), []int64{a.ID, b.ID, c.ID}) {
		t.Fatalf("got %v", keys(got))
	}
	want := Digest(sha256.Sum256(same))
	for _, id := range []int64{a.ID, b.ID, c.ID} {
		if got[id] != want {
			t.Fatalf("digest of %d is not the full SHA-256", id)
		}
	}
}

func TestHashMiddleDiffers(t *testing.T) {
	d := t.TempDir()
	one := pattern(1<<20, 3)
	two := bytes.Clone(one)
	two[len(two)/2] ^= 0xff
	a, b := writeCand(t, d, "a", one), writeCand(t, d, "b", two)
	if got := runHasher(t, &Hasher{FS: OSFS()}, []Candidate{a, b}); len(got) != 0 {
		t.Fatalf("files differing in the middle reported as same: %v", keys(got))
	}
}

func TestHashSmallFilesReadOnce(t *testing.T) {
	d := t.TempDir()
	same := pattern(100<<10, 4)
	a, b := writeCand(t, d, "a", same), writeCand(t, d, "b", same)
	fs := newCountFS()
	got := runHasher(t, &Hasher{FS: fs}, []Candidate{a, b})
	if len(got) != 2 {
		t.Fatalf("got %v", keys(got))
	}
	if fs.opens[a.Path] != 1 || fs.opens[b.Path] != 1 {
		t.Fatalf("opens = %v, want each small file opened once", fs.opens)
	}
}

func TestHashChangedSinceScan(t *testing.T) {
	d := t.TempDir()
	same := pattern(10<<10, 5)
	a, b := writeCand(t, d, "a", same), writeCand(t, d, "b", same)
	b.MTime -= 100
	p := &Progress{}
	got := runHasher(t, &Hasher{FS: OSFS(), Progress: p}, []Candidate{a, b})
	if len(got) != 0 || p.Changed.Load() != 1 {
		t.Fatalf("got %v, changed %d", keys(got), p.Changed.Load())
	}
}

func TestHashFileChangedDuringRead(t *testing.T) {
	d := t.TempDir()
	same := pattern(300<<10, 6)
	a, b, c := writeCand(t, d, "a", same), writeCand(t, d, "b", same), writeCand(t, d, "c", same)
	fs := newCountFS()
	fs.bumpCtime = c.Path
	p := &Progress{}
	got := runHasher(t, &Hasher{FS: fs, Progress: p}, []Candidate{a, b, c})
	if !slices.Equal(keys(got), []int64{a.ID, b.ID}) || p.Changed.Load() != 1 {
		t.Fatalf("got %v, changed %d", keys(got), p.Changed.Load())
	}
}

func TestHashUnreadableFile(t *testing.T) {
	d := t.TempDir()
	same := pattern(300<<10, 7)
	a, b, c := writeCand(t, d, "a", same), writeCand(t, d, "b", same), writeCand(t, d, "c", same)
	fs := newCountFS()
	fs.denyOpen = b.Path
	p := &Progress{}
	got := runHasher(t, &Hasher{FS: fs, Progress: p}, []Candidate{a, b, c})
	if !slices.Equal(keys(got), []int64{a.ID, c.ID}) || p.Errors.Load() != 1 {
		t.Fatalf("got %v, errors %d", keys(got), p.Errors.Load())
	}
}

func TestHashCacheSkipsReads(t *testing.T) {
	d := t.TempDir()
	same := pattern(300<<10, 8)
	group := []Candidate{writeCand(t, d, "a", same), writeCand(t, d, "b", same), writeCand(t, d, "c", same),
		writeCand(t, d, "x", pattern(300<<10, 9))}
	cache := openCache(t, filepath.Join(t.TempDir(), "c.db"))
	first := runHasher(t, &Hasher{FS: OSFS(), Cache: cache}, group)
	fs := newCountFS()
	p := &Progress{}
	second := runHasher(t, &Hasher{FS: fs, Cache: cache, Progress: p}, group)
	if fs.totalOpens() != 0 {
		t.Fatalf("second run opened files: %v", fs.opens)
	}
	if p.CacheHits.Load() != int64(len(group)) || p.BytesRead.Load() != 0 {
		t.Fatalf("cache hits %d, bytes read %d", p.CacheHits.Load(), p.BytesRead.Load())
	}
	if !slices.Equal(keys(first), keys(second)) {
		t.Fatalf("first %v second %v", keys(first), keys(second))
	}
}

func TestHashCanceled(t *testing.T) {
	d := t.TempDir()
	same := pattern(10, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&Hasher{FS: OSFS(), Progress: &Progress{}}).Run(ctx, table([]Candidate{writeCand(t, d, "a", same), writeCand(t, d, "b", same)}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

// orderFS records the order in which files are opened.
type orderFS struct {
	FS
	mu    sync.Mutex
	order []string
}

func (f *orderFS) Open(p string) (File, error) {
	f.mu.Lock()
	f.order = append(f.order, p)
	f.mu.Unlock()
	return f.FS.Open(p)
}

func TestHashReadsInIDOrder(t *testing.T) {
	d := t.TempDir()
	small, big := pattern(10<<10, 1), pattern(300<<10, 2)
	var cands []Candidate
	for i := 0; i < 3; i++ {
		cands = append(cands, writeCand(t, d, "s"+string(rune('a'+i)), small), writeCand(t, d, "b"+string(rune('a'+i)), big))
	}
	// Hand groups over in reverse id order; reads must still go by ascending id.
	var g1, g2 []Candidate
	for i := len(cands) - 1; i >= 0; i-- {
		if cands[i].Size == int64(len(small)) {
			g1 = append(g1, cands[i])
		} else {
			g2 = append(g2, cands[i])
		}
	}
	fs := &orderFS{FS: OSFS()}
	runHasher(t, &Hasher{FS: fs, QuickWorkers: 1}, g2, g1)
	byPath := map[string]int64{}
	for _, c := range cands {
		byPath[c.Path] = c.ID
	}
	quick := fs.order[:len(cands)]
	for i := 1; i < len(quick); i++ {
		if byPath[quick[i]] < byPath[quick[i-1]] {
			t.Fatalf("quick reads out of id order: %v", quick)
		}
	}
	full := fs.order[len(cands):]
	for i := 1; i < len(full); i++ {
		if byPath[full[i]] < byPath[full[i-1]] {
			t.Fatalf("full reads out of id order: %v", full)
		}
	}
}

func TestCandidatesCompact(t *testing.T) {
	const n = 200_000
	name := []byte("IMG_20240101_123456_burst.jpg") // 29 bytes
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	c := &Candidates{}
	for i := 0; i < n; i++ {
		c.Add(int64(i+1), int64(i/50+1), name, 4096, 1700000000)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := float64(after.HeapAlloc-before.HeapAlloc) / n
	t.Logf("%.0f bytes per candidate", per)
	if c.Len() != n || per > 220 {
		t.Fatalf("len %d, %.0f bytes per candidate (want ≤ 220)", c.Len(), per)
	}
	runtime.KeepAlive(c)
}
