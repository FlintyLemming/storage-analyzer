package scandb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newDB(t *testing.T, build func(w *Writer)) *DB {
	t.Helper()
	p := filepath.Join(t.TempDir(), "scan.db")
	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	build(w)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func add(t *testing.T, w *Writer, e Entry) {
	t.Helper()
	if err := w.Add(e); err != nil {
		t.Fatal(err)
	}
}

func ids(es []Entry) []int64 {
	var out []int64
	for _, e := range es {
		out = append(out, e.ID)
	}
	return out
}

func TestWriteAndListChildren(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Parent: 0, Name: []byte("Vol"), Kind: KindDir, DSize: 60, ASize: 60, Items: 3, FirstChild: 2, NChild: 3})
		add(t, w, Entry{ID: 2, Parent: 1, Name: []byte("a"), Kind: KindFile, DSize: 10, ASize: 10})
		add(t, w, Entry{ID: 3, Parent: 1, Name: []byte("b"), Kind: KindFile, DSize: 30, ASize: 30})
		add(t, w, Entry{ID: 4, Parent: 1, Name: []byte("c"), Kind: KindFile, DSize: 20, ASize: 20})
		w.SetMeta(MetaRoots, "1")
	})
	got, total, err := d.Children(1, ListOpts{Sort: "size", Desc: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}
	if fmt.Sprint(ids(got)) != "[3 4 2]" {
		t.Fatalf("order = %v, want [3 4 2]", ids(got))
	}
	byName, _, _ := d.Children(1, ListOpts{Sort: "name", Desc: false, Limit: 100})
	if fmt.Sprint(ids(byName)) != "[2 3 4]" {
		t.Fatalf("name order = %v", ids(byName))
	}
}

func TestApparentSizeSort(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir, FirstChild: 2, NChild: 2})
		add(t, w, Entry{ID: 2, Parent: 1, Name: []byte("sparse"), DSize: 4096, ASize: 1 << 30})
		add(t, w, Entry{ID: 3, Parent: 1, Name: []byte("dense"), DSize: 1 << 20, ASize: 1 << 20})
		w.SetMeta(MetaRoots, "1")
	})
	disk, _, _ := d.Children(1, ListOpts{Sort: "size", Desc: true, SizeMode: "disk", Limit: 10})
	app, _, _ := d.Children(1, ListOpts{Sort: "size", Desc: true, SizeMode: "apparent", Limit: 10})
	if disk[0].ID != 3 || app[0].ID != 2 {
		t.Fatalf("disk first=%d (want 3), apparent first=%d (want 2)", disk[0].ID, app[0].ID)
	}
}

func TestListLargeDirPaging(t *testing.T) {
	const n = 200000
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir, FirstChild: 2, NChild: n})
		for i := 0; i < n; i++ {
			// sizes are a permutation so the order is fully determined
			add(t, w, Entry{ID: int64(2 + i), Parent: 1, Name: []byte(fmt.Sprintf("f%06d", i)), DSize: int64((i*7919)%n + 1)})
		}
		w.SetMeta(MetaRoots, "1")
	})
	start := time.Now()
	got, total, err := d.Children(1, ListOpts{Sort: "size", Desc: true, Offset: 199990, Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	el := time.Since(start)
	t.Logf("query took %v", el)
	// Under CPU emulation (scripts/test-linux.sh on a foreign architecture)
	// the time says nothing about real hardware.
	if el > 2*time.Second && os.Getenv("SA_EMULATED") == "" {
		t.Fatalf("query took %v", el)
	}
	if total != n || len(got) != 10 {
		t.Fatalf("total=%d len=%d", total, len(got))
	}
	for i, e := range got {
		if want := int64(10 - i); e.DSize != want {
			t.Fatalf("got[%d].DSize = %d, want %d", i, e.DSize, want)
		}
	}
}

func TestPathBreadcrumb(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir, FirstChild: 2, NChild: 1})
		add(t, w, Entry{ID: 2, Parent: 1, Name: []byte("a"), Kind: KindDir, FirstChild: 3, NChild: 1})
		add(t, w, Entry{ID: 3, Parent: 2, Name: []byte("b"), Kind: KindDir, FirstChild: 4, NChild: 1})
		add(t, w, Entry{ID: 4, Parent: 3, Name: []byte("f")})
		w.SetMeta(MetaRoots, "1")
	})
	p, err := d.Path(4)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids(p)) != "[1 2 3 4]" {
		t.Fatalf("path = %v", ids(p))
	}
}

func TestNamesRoundTrip(t *testing.T) {
	names := [][]byte{{0xff, 'a'}, []byte("a\nb"), []byte(`50%"x`), []byte("中文 文件.mkv")}
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir, FirstChild: 2, NChild: int64(len(names))})
		for i, n := range names {
			add(t, w, Entry{ID: int64(2 + i), Parent: 1, Name: n})
		}
		w.SetMeta(MetaRoots, "1")
	})
	for i, n := range names {
		e, err := d.Get(int64(2 + i))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(e.Name, n) {
			t.Fatalf("name %d = %q, want %q", i, e.Name, n)
		}
	}
}

func TestRootListsVolumes(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol1"), Kind: KindDir, DSize: 5, FirstChild: 3, NChild: 1})
		add(t, w, Entry{ID: 2, Name: []byte("Vol2"), Kind: KindDir, DSize: 9, FirstChild: 4, NChild: 1})
		add(t, w, Entry{ID: 3, Parent: 1, Name: []byte("x"), DSize: 5})
		add(t, w, Entry{ID: 4, Parent: 2, Name: []byte("y"), DSize: 9})
		w.SetMeta(MetaRoots, "2")
	})
	got, total, err := d.Children(0, ListOpts{Sort: "size", Desc: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || fmt.Sprint(ids(got)) != "[2 1]" {
		t.Fatalf("roots = %v total=%d", ids(got), total)
	}
	root, err := d.Get(0)
	if err != nil {
		t.Fatal(err)
	}
	if root.DSize != 14 || root.NChild != 2 || root.Kind != KindDir {
		t.Fatalf("synthetic root = %+v", root)
	}
}

func TestGetMissing(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir})
		w.SetMeta(MetaRoots, "1")
	})
	if _, err := d.Get(99); err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestAbortRemovesFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.db")
	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	add(t, w, Entry{ID: 1, Name: []byte("Vol")})
	w.Abort()
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("file still exists: %v", err)
	}
}

func TestBytesPerEntry(t *testing.T) {
	const n = 100000
	p := filepath.Join(t.TempDir(), "size.db")
	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	letters := []byte("abcdefghijklmnopqrstuvwxyz0123456789_-.")
	// 1 root, 1000 dirs of 99 files each
	add(t, w, Entry{ID: 1, Name: []byte("DataVol1"), Kind: KindDir, FirstChild: 2, NChild: 1000})
	next := int64(1002)
	for d := 0; d < 1000; d++ {
		add(t, w, Entry{ID: int64(2 + d), Parent: 1, Name: []byte(fmt.Sprintf("directory-%04d", d)), Kind: KindDir,
			DSize: rng.Int63n(1 << 36), ASize: rng.Int63n(1 << 36), Items: 99, MTime: 1700000000 + rng.Int63n(1e8),
			FirstChild: next, NChild: 99})
		for f := 0; f < 99; f++ {
			name := make([]byte, 15+rng.Intn(16))
			for i := range name {
				name[i] = letters[rng.Intn(len(letters))]
			}
			sz := rng.Int63n(1 << 30)
			add(t, w, Entry{ID: next, Parent: int64(2 + d), Name: name, DSize: sz, ASize: sz, MTime: 1700000000 + rng.Int63n(1e8)})
			next++
		}
	}
	w.SetMeta(MetaRoots, "1")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(p)
	per := float64(st.Size()) / float64(next-1)
	t.Logf("%d entries, %d bytes, %.1f bytes/entry", next-1, st.Size(), per)
	if per > 70 {
		t.Fatalf("%.1f bytes/entry > 70", per)
	}
}

func TestDupSizeFiles(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir, FirstChild: 2, NChild: 7})
		add(t, w, Entry{ID: 2, Parent: 1, Name: []byte("a"), ASize: 5})
		add(t, w, Entry{ID: 3, Parent: 1, Name: []byte("b"), ASize: 5})
		add(t, w, Entry{ID: 4, Parent: 1, Name: []byte("c"), ASize: 7})
		add(t, w, Entry{ID: 5, Parent: 1, Name: []byte("d"), ASize: 0})
		add(t, w, Entry{ID: 6, Parent: 1, Name: []byte("e"), ASize: 0})
		add(t, w, Entry{ID: 7, Parent: 1, Name: []byte("f"), ASize: 7, Flags: FlagHardlinkDup})
		add(t, w, Entry{ID: 8, Parent: 1, Name: []byte("g"), ASize: 5, Kind: KindSymlink})
		w.SetMeta(MetaRoots, "1")
	})
	var got []int64
	if err := d.DupSizeFiles(func(e Entry) error { got = append(got, e.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[2 3]" {
		t.Fatalf("got %v, want [2 3]", got)
	}
	if d.Roots() != 1 {
		t.Fatalf("roots = %d", d.Roots())
	}
}

func TestEachDescOrder(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir, FirstChild: 2, NChild: 2})
		add(t, w, Entry{ID: 3, Parent: 1, Name: []byte("b")})
		add(t, w, Entry{ID: 2, Parent: 1, Name: []byte("a"), Kind: KindDir})
		w.SetMeta(MetaRoots, "1")
	})
	var all, dirs []int64
	if err := d.EachDesc(func(e Entry) error { all = append(all, e.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := d.EachDir(func(e Entry) error { dirs = append(dirs, e.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(all) != "[3 2 1]" || fmt.Sprint(dirs) != "[1 2]" {
		t.Fatalf("desc %v dirs %v", all, dirs)
	}
}
