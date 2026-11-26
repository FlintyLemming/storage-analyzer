package dupcheck

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
	"github.com/FlintyLemming/storage-analyzer/internal/scanner"
)

func put(t *testing.T, p string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

// fixture: vol/{A{m.bin, s/x}, B{m.bin, s/x}, C/m.bin, D/u.bin}
func fixture(t *testing.T) string {
	vol := t.TempDir()
	m := pattern(200<<10, 1)
	for _, d := range []string{"A", "B"} {
		put(t, filepath.Join(vol, d, "m.bin"), m)
		put(t, filepath.Join(vol, d, "s", "x"), []byte("0123456789"))
	}
	put(t, filepath.Join(vol, "C", "m.bin"), m)
	put(t, filepath.Join(vol, "D", "u.bin"), pattern(200<<10, 2))
	return vol
}

func scanOf(t *testing.T, vol string) (string, map[int64]Root) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "scan.db")
	w, err := scandb.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&scanner.Walker{}).Walk(context.Background(), []scanner.Root{{Name: "vol", Path: vol}}, w); err != nil {
		t.Fatal(err)
	}
	w.SetMeta(scandb.MetaRoots, "1")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return p, map[int64]Root{1: {Name: "vol", MountPoint: vol}}
}

func check(t *testing.T, scanDB string, roots map[int64]Root, cache *Cache) (Summary, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "dups-1.db")
	s, err := Run(context.Background(), Options{ScanDB: scanDB, Roots: roots, Cache: cache, Out: out})
	if err != nil {
		t.Fatal(err)
	}
	return s, out
}

func TestCheckEndToEnd(t *testing.T) {
	db, roots := scanOf(t, fixture(t))
	s, out := check(t, db, roots, nil)
	if s.Groups != 2 {
		t.Fatalf("summary %+v", s)
	}
	r, err := OpenResult(out)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	gs, err := r.Page("apparent", 0, 10, 10)
	if err != nil || len(gs) != 2 {
		t.Fatalf("groups %+v %v", gs, err)
	}
	var paths []string
	for _, g := range gs {
		var ps []string
		for _, m := range g.Members {
			ps = append(ps, string(m.Path))
		}
		paths = append(paths, strings.Join(ps, ","))
	}
	// The folder group {A,B} is slightly larger (it also holds s/x), so it comes first.
	if gs[0].Kind != scandb.KindDir || paths[0] != "vol/A,vol/B" || gs[0].Files != 2 {
		t.Fatalf("first group %+v %s", gs[0], paths[0])
	}
	if gs[1].Kind != scandb.KindFile || paths[1] != "vol/A/m.bin,vol/B/m.bin,vol/C/m.bin" {
		t.Fatalf("second group %s", paths[1])
	}
	if gs[1].RASize != 200<<10 {
		t.Fatalf("file group reclaim %d, want one copy", gs[1].RASize)
	}
	if s.ReclaimASize != gs[0].RASize+gs[1].RASize || s.ReclaimDSize <= 0 {
		t.Fatalf("summary reclaim %+v", s)
	}
	if m, _ := r.Meta(); m["finishedAt"] == "" || m["groups"] != "2" {
		t.Fatalf("meta %v", m)
	}
}

func TestCheckSecondRunUsesCache(t *testing.T) {
	db, roots := scanOf(t, fixture(t))
	cache := openCache(t, filepath.Join(t.TempDir(), "c.db"))
	first, _ := check(t, db, roots, cache)
	second, _ := check(t, db, roots, cache)
	if first.BytesRead == 0 || second.BytesRead != 0 || second.CacheHits != 6 || second.Groups != 2 {
		t.Fatalf("first %+v second %+v", first, second)
	}
}

func TestCheckMissingRoot(t *testing.T) {
	db, _ := scanOf(t, fixture(t))
	s, _ := check(t, db, map[int64]Root{1: {Name: "vol", MountPoint: filepath.Join(t.TempDir(), "gone")}}, nil)
	if s.Groups != 0 || s.Changed != 6 {
		t.Fatalf("summary %+v", s)
	}
}

func TestCheckCanceledRemovesTmp(t *testing.T) {
	db, roots := scanOf(t, fixture(t))
	out := filepath.Join(t.TempDir(), "dups-1.db")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(ctx, Options{ScanDB: db, Roots: roots, Out: out})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	for _, p := range []string{out, out + ".tmp"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s exists", p)
		}
	}
}

func TestPatherRootSlash(t *testing.T) {
	p := &pather{
		dirs: map[int64]DirInfo{
			1: {Parent: 0, Name: []byte("/")},
			2: {Parent: 1, Name: []byte("etc")},
		},
		roots:  map[int64]Root{1: {Name: "/", MountPoint: "/"}},
		memo:   map[int64]string{},
		rootOf: map[int64]int64{},
	}
	for _, display := range []bool{false, true} {
		if got, ok := p.path(2, []byte("passwd"), display); !ok || got != "/etc/passwd" {
			t.Errorf("display=%v: got %q %v", display, got, ok)
		}
		if got, ok := p.path(1, []byte("x"), display); !ok || got != "/x" {
			t.Errorf("display=%v: got %q %v", display, got, ok)
		}
	}
}
