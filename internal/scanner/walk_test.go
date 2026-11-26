package scanner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

type memSink struct {
	mu sync.Mutex
	m  map[int64]scandb.Entry
}

func (s *memSink) Add(e scandb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[int64]scandb.Entry{}
	}
	if _, dup := s.m[e.ID]; dup {
		return fmt.Errorf("duplicate id %d", e.ID)
	}
	s.m[e.ID] = e
	return nil
}

func (s *memSink) byName(t *testing.T, parent int64, name string) scandb.Entry {
	t.Helper()
	for _, e := range s.m {
		if e.Parent == parent && string(e.Name) == name {
			return e
		}
	}
	t.Fatalf("no entry %q under %d", name, parent)
	return scandb.Entry{}
}

// checkTree verifies the contiguous-children invariant for every directory.
func (s *memSink) checkTree(t *testing.T) {
	t.Helper()
	count := map[int64]int64{}
	for _, e := range s.m {
		count[e.Parent]++
	}
	for _, e := range s.m {
		if e.NChild != count[e.ID] {
			t.Fatalf("%q: NChild=%d but %d entries have it as parent", e.Name, e.NChild, count[e.ID])
		}
		for id := e.FirstChild; id < e.FirstChild+e.NChild; id++ {
			c, ok := s.m[id]
			if !ok || c.Parent != e.ID {
				t.Fatalf("%q: child id %d missing or wrong parent", e.Name, id)
			}
		}
	}
}

func write(t *testing.T, p string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
}

func lsize(t *testing.T, p string) (asize, dsize int64) {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Lstat(p, &st); err != nil {
		t.Fatal(err)
	}
	return st.Size, st.Blocks * 512
}

func walk(t *testing.T, w *Walker, roots ...Root) *memSink {
	t.Helper()
	s := &memSink{}
	if err := w.Walk(context.Background(), roots, s); err != nil {
		t.Fatal(err)
	}
	s.checkTree(t)
	return s
}

func TestWalkTotals(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "a/x"), 100)
	write(t, filepath.Join(root, "a/b/y"), 5000)
	write(t, filepath.Join(root, "z"), 1)
	s := walk(t, &Walker{Workers: 4}, Root{Name: "Vol", Path: root})

	r := s.m[1]
	if r.Parent != 0 || string(r.Name) != "Vol" || r.Kind != scandb.KindDir {
		t.Fatalf("root = %+v", r)
	}
	if r.Items != 5 {
		t.Fatalf("root items = %d, want 5", r.Items)
	}
	aA, _ := lsize(t, filepath.Join(root, "a"))
	bA, _ := lsize(t, filepath.Join(root, "a/b"))
	rA, _ := lsize(t, root)
	a := s.byName(t, 1, "a")
	if want := 5100 + aA + bA; a.ASize != want {
		t.Fatalf("a.asize = %d, want %d", a.ASize, want)
	}
	if want := 5101 + aA + bA + rA; r.ASize != want {
		t.Fatalf("root.asize = %d, want %d", r.ASize, want)
	}
	if a.Items != 3 {
		t.Fatalf("a.items = %d, want 3", a.Items)
	}
}

func TestWalkMultipleRoots(t *testing.T) {
	r1, r2 := t.TempDir(), t.TempDir()
	write(t, filepath.Join(r1, "f"), 10)
	write(t, filepath.Join(r2, "g"), 20)
	s := walk(t, &Walker{}, Root{Name: "V1", Path: r1}, Root{Name: "V2", Path: r2})
	if string(s.m[1].Name) != "V1" || string(s.m[2].Name) != "V2" || s.m[1].Parent != 0 || s.m[2].Parent != 0 {
		t.Fatalf("roots must be ids 1..2 with parent 0: %+v %+v", s.m[1], s.m[2])
	}
}

func TestWalkHardlinkCountedOnce(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "f"), 40960)
	if err := os.Link(filepath.Join(root, "f"), filepath.Join(root, "g")); err != nil {
		t.Fatal(err)
	}
	s := walk(t, &Walker{}, Root{Name: "Vol", Path: root})
	f, g := s.byName(t, 1, "f"), s.byName(t, 1, "g")
	dups := 0
	for _, e := range []scandb.Entry{f, g} {
		if e.Flags&scandb.FlagHardlinkDup != 0 {
			dups++
		}
	}
	if dups != 1 {
		t.Fatalf("want exactly one dup flag, got %d", dups)
	}
	_, fD := lsize(t, filepath.Join(root, "f"))
	_, rD := lsize(t, root)
	if want := fD + rD; s.m[1].DSize != want {
		t.Fatalf("root dsize = %d, want %d (counted once)", s.m[1].DSize, want)
	}
}

func TestWalkSymlinkNotFollowed(t *testing.T) {
	root, big := t.TempDir(), t.TempDir()
	write(t, filepath.Join(big, "huge"), 1<<20)
	if err := os.Symlink(big, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	s := walk(t, &Walker{}, Root{Name: "Vol", Path: root})
	l := s.byName(t, 1, "link")
	if l.Kind != scandb.KindSymlink || l.NChild != 0 {
		t.Fatalf("link = %+v", l)
	}
	if len(s.m) != 2 {
		t.Fatalf("symlink target was walked: %d entries", len(s.m))
	}
}

func TestWalkMountBoundary(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "m/inside"), 1000)
	w := &Walker{IsMountPoint: func(path string, rootDev, dev uint64) bool { return filepath.Base(path) == "m" }}
	s := walk(t, w, Root{Name: "Vol", Path: root})
	m := s.byName(t, 1, "m")
	if m.Kind != scandb.KindMount || m.NChild != 0 || m.DSize != 0 {
		t.Fatalf("m = %+v", m)
	}
	if len(s.m) != 2 {
		t.Fatalf("mount point was descended: %d entries", len(s.m))
	}
}

func TestWalkUnreadableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read everything")
	}
	root := t.TempDir()
	write(t, filepath.Join(root, "locked/secret"), 10)
	write(t, filepath.Join(root, "ok"), 10)
	if err := os.Chmod(filepath.Join(root, "locked"), 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(root, "locked"), 0o755) })
	w := &Walker{}
	s := walk(t, w, Root{Name: "Vol", Path: root})
	if s.byName(t, 1, "locked").Flags&scandb.FlagError == 0 {
		t.Fatal("locked dir should carry FlagError")
	}
	if w.Stats.Errors.Load() < 1 {
		t.Fatal("Errors not counted")
	}
	s.byName(t, 1, "ok")
}

func TestWalkBadName(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "\xff.bin"), []byte("x"), 0o644); err != nil {
		t.Skipf("filesystem rejects invalid UTF-8 names: %v", err)
	}
	s := walk(t, &Walker{}, Root{Name: "Vol", Path: root})
	if s.byName(t, 1, "\xff.bin").Flags&scandb.FlagBadName == 0 {
		t.Fatal("FlagBadName not set")
	}
}

func TestWalkWideDir(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 20000; i++ {
		write(t, filepath.Join(root, "wide", fmt.Sprintf("f%05d", i)), 1)
	}
	w := &Walker{}
	s := walk(t, w, Root{Name: "Vol", Path: root})
	if wide := s.byName(t, 1, "wide"); wide.NChild != 20000 {
		t.Fatalf("NChild = %d", wide.NChild)
	}
	if got := w.Stats.Files.Load(); got != 20000 {
		t.Fatalf("Stats.Files = %d", got)
	}
}

func TestWalkCancel(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "a/b"), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (&Walker{}).Walk(ctx, []Root{{Name: "Vol", Path: root}}, &memSink{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

type failSink struct{ n int }

func (f *failSink) Add(scandb.Entry) error {
	f.n++
	if f.n > 2 {
		return errors.New("disk full")
	}
	return nil
}

func TestWalkSinkError(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 50; i++ {
		write(t, filepath.Join(root, fmt.Sprintf("d%d/f", i)), 1)
	}
	err := (&Walker{}).Walk(context.Background(), []Root{{Name: "Vol", Path: root}}, &failSink{})
	if err == nil || err.Error() != "disk full" {
		t.Fatalf("err = %v, want disk full", err)
	}
}

func TestWalkRootSlashPaths(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "etc", "sub", "f"), 10)
	var mu sync.Mutex
	var seen []string
	w := &Walker{IsMountPoint: func(path string, rootDev, dev uint64) bool {
		mu.Lock()
		seen = append(seen, path)
		mu.Unlock()
		return false
	}}
	walk(t, w, Root{Name: "/", Path: dir + "/"})
	if len(seen) == 0 {
		t.Fatal("IsMountPoint never called")
	}
	for _, p := range seen {
		if strings.Contains(p, "//") {
			t.Errorf("path %q contains //", p)
		}
	}
}
