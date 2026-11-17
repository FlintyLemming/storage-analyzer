package dupcheck

import (
	"fmt"
	"path/filepath"
	"testing"
)

func openCache(t *testing.T, path string) *Cache {
	t.Helper()
	c, err := OpenCache(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

var fi0 = FileInfo{Dev: 1, Ino: 2, Size: 3, MTime: 4, MTimeNS: 4_000_000_005, CTimeNS: 6, Regular: true}

func TestCacheHitRequiresAllFields(t *testing.T) {
	c := openCache(t, filepath.Join(t.TempDir(), "c.db"))
	full := Digest{7}
	if err := c.Put("/v/a", fi0, CacheEntry{Quick: Digest{1}, Full: &full}); err != nil {
		t.Fatal(err)
	}
	e, ok := c.Lookup("/v/a", fi0)
	if !ok || e.Quick != (Digest{1}) || e.Full == nil || *e.Full != full {
		t.Fatalf("lookup = %+v %v", e, ok)
	}
	for name, mut := range map[string]func(*FileInfo){
		"dev": func(f *FileInfo) { f.Dev++ }, "ino": func(f *FileInfo) { f.Ino++ }, "size": func(f *FileInfo) { f.Size++ },
		"mtime": func(f *FileInfo) { f.MTimeNS++ }, "ctime": func(f *FileInfo) { f.CTimeNS++ },
	} {
		f := fi0
		mut(&f)
		if _, ok := c.Lookup("/v/a", f); ok {
			t.Errorf("%s changed but cache hit", name)
		}
	}
	if _, ok := c.Lookup("/v/b", fi0); ok {
		t.Error("other path hit")
	}
}

func TestCachePersistsAcrossOpen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.db")
	c, err := OpenCache(p)
	if err != nil {
		t.Fatal(err)
	}
	full := Digest{9}
	c.Put("/v/a\xff", fi0, CacheEntry{Quick: Digest{1}, Full: &full})
	c.Put("/v/b", fi0, CacheEntry{Quick: Digest{2}})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := openCache(t, p)
	e, ok := c2.Lookup("/v/a\xff", fi0)
	if !ok || e.Full == nil || *e.Full != full {
		t.Fatalf("a = %+v %v", e, ok)
	}
	e, ok = c2.Lookup("/v/b", fi0)
	if !ok || e.Full != nil || e.Quick != (Digest{2}) {
		t.Fatalf("b = %+v %v", e, ok)
	}
}

func TestCacheAutoCommitSurvivesCrash(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.db")
	c := openCache(t, p)
	for i := 0; i < 600; i++ {
		if err := c.Put(fmt.Sprintf("/v/%d", i), fi0, CacheEntry{Quick: Digest{1}}); err != nil {
			t.Fatal(err)
		}
	}
	other := openCache(t, p)
	hits := 0
	for i := 0; i < 600; i++ {
		if _, ok := other.Lookup(fmt.Sprintf("/v/%d", i), fi0); ok {
			hits++
		}
	}
	if hits < 500 {
		t.Fatalf("only %d of 600 entries committed without Close", hits)
	}
}

func TestCachePrune(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.db")
	c, _ := OpenCache(p)
	c.Put("/v/a", fi0, CacheEntry{Quick: Digest{1}})
	c.Put("/v/b", fi0, CacheEntry{Quick: Digest{2}})
	c.Close()
	c2 := openCache(t, p)
	if err := c2.Touch("/v/a"); err != nil {
		t.Fatal(err)
	}
	n, err := c2.Prune()
	if err != nil || n != 1 {
		t.Fatalf("prune = %d, %v; want 1", n, err)
	}
	if _, ok := c2.Lookup("/v/a", fi0); !ok {
		t.Error("touched entry pruned")
	}
	if _, ok := c2.Lookup("/v/b", fi0); ok {
		t.Error("untouched entry kept")
	}
}
