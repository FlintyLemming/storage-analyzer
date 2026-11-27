package scandb

import (
	"bytes"
	"database/sql"
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

func TestAnnotationsRoundTrip(t *testing.T) {
	d := newDB(t, func(w *Writer) {
		add(t, w, Entry{ID: 1, Name: []byte("Vol"), Kind: KindDir, FirstChild: 2, NChild: 600})
		for i := 0; i < 600; i++ {
			id := int64(2 + i)
			add(t, w, Entry{ID: id, Parent: 1, Name: []byte(fmt.Sprintf("d%04d", i)), Kind: KindDir})
			if i%2 == 0 {
				if err := w.AddAnnotation(id, "docker", fmt.Sprintf(`{"type":"image_layer","label":"img-%d"}`, i)); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := w.AddAnnotation(1, "docker", `{"type":"dir_overlay2"}`); err != nil {
			t.Fatal(err)
		}
		w.SetMeta(MetaRoots, "1")
	})

	// Query >500 IDs at once to exercise chunking.
	queryIDs := make([]int64, 0, 601)
	queryIDs = append(queryIDs, 1)
	for i := 0; i < 600; i++ {
		queryIDs = append(queryIDs, int64(2+i))
	}
	got, err := d.Annotations(queryIDs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 301 {
		t.Fatalf("len(got) = %d, want 301", len(got))
	}
	if got[1].Kind != "docker" || got[1].Data != `{"type":"dir_overlay2"}` {
		t.Fatalf("got[1] = %+v", got[1])
	}
	if got[2].Kind != "docker" || got[2].Data != `{"type":"image_layer","label":"img-0"}` {
		t.Fatalf("got[2] = %+v", got[2])
	}
	if _, ok := got[3]; ok {
		t.Fatalf("unexpected annotation for id 3: %+v", got[3])
	}
	// Empty slice returns empty map without error.
	empty, err := d.Annotations(nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty = %v err=%v", empty, err)
	}
}

func TestAnnotationsLegacyDBWithoutTable(t *testing.T) {
	p := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", "file:"+p)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`
		CREATE TABLE entries(id INTEGER PRIMARY KEY, parent INTEGER NOT NULL, name BLOB NOT NULL, kind INTEGER NOT NULL, flags INTEGER NOT NULL, dsize INTEGER NOT NULL, asize INTEGER NOT NULL, items INTEGER NOT NULL, mtime INTEGER NOT NULL, first_child INTEGER NOT NULL, nchild INTEGER NOT NULL);
		CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO entries VALUES (1, 0, X'566f6c', 1, 0, 0, 0, 0, 0, 0, 0);
		INSERT INTO meta VALUES ('roots', '1');
	`)
	raw.Close()
	if err != nil {
		t.Fatal(err)
	}

	d, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	got, err := d.Annotations([]int64{1})
	if err != nil {
		t.Fatalf("Annotations on legacy DB failed: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got = %v, want empty", got)
	}
}

func TestMarkDeletedUpdatesTreeAndQueries(t *testing.T) {
	p := filepath.Join(t.TempDir(), "scan.db")
	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	// Tree:
	// 1 Vol (dsize=150, asize=160, items=5)
	// ├── 2 dirA (dsize=110, asize=115, items=2)
	// │   ├── 5 f1 (dsize=100, asize=105)
	// │   └── 6 f2_hl (dsize=100, asize=105, FlagHardlinkDup)
	// ├── 3 f3 (dsize=30, asize=35)
	// └── 4 dirB (dsize=10, asize=10, items=0)
	add(t, w, Entry{ID: 1, Parent: 0, Name: []byte("Vol"), Kind: KindDir, DSize: 150, ASize: 160, Items: 5, FirstChild: 2, NChild: 3})
	add(t, w, Entry{ID: 2, Parent: 1, Name: []byte("dirA"), Kind: KindDir, DSize: 110, ASize: 115, Items: 2, FirstChild: 5, NChild: 2})
	add(t, w, Entry{ID: 3, Parent: 1, Name: []byte("f3"), Kind: KindFile, DSize: 30, ASize: 35})
	add(t, w, Entry{ID: 4, Parent: 1, Name: []byte("dirB"), Kind: KindDir, DSize: 10, ASize: 10, Items: 0})
	add(t, w, Entry{ID: 5, Parent: 2, Name: []byte("f1"), Kind: KindFile, DSize: 100, ASize: 105})
	add(t, w, Entry{ID: 6, Parent: 2, Name: []byte("f2_hl"), Kind: KindFile, DSize: 100, ASize: 105, Flags: FlagHardlinkDup})
	w.SetMeta(MetaRoots, "1")
	w.SetMeta("volumes", `[{"entryId":1,"fsFree":100}]`)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Delete hardlink dup (6) first: items decreases by 1, sizes unchanged.
	tot, err := MarkDeleted(p, []int64{6}, "")
	if err != nil {
		t.Fatal(err)
	}
	if tot.Count != 1 || tot.DSize != 0 || tot.ASize != 0 || tot.Items != 1 {
		t.Fatalf("hardlink delete totals = %+v", tot)
	}

	d, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	dirA, err := d.Get(2)
	if err != nil || dirA.DSize != 110 || dirA.ASize != 115 || dirA.Items != 1 {
		t.Fatalf("dirA after hardlink delete = %+v err=%v", dirA, err)
	}
	vol, err := d.Get(1)
	if err != nil || vol.DSize != 150 || vol.ASize != 160 || vol.Items != 4 {
		t.Fatalf("vol after hardlink delete = %+v err=%v", vol, err)
	}
	if _, err := d.Get(6); err != ErrNotFound {
		t.Fatalf("Get(6) = %v, want ErrNotFound", err)
	}
	ch, total, err := d.Children(2, ListOpts{Sort: "size", Desc: true, Limit: 10})
	if err != nil || total != 1 || fmt.Sprint(ids(ch)) != "[5]" {
		t.Fatalf("dirA children = %v total=%d err=%v", ids(ch), total, err)
	}
	d.Close()

	// Delete dirA (2) and f3 (3), plus pass descendant (5) redundantly and volume root (1) which must be ignored.
	tot, err = MarkDeleted(p, []int64{2, 5, 3, 1}, `[{"entryId":1,"fsFree":240}]`)
	if err != nil {
		t.Fatal(err)
	}
	// dirA contributed dsize=110, asize=115, items=1+1=2; f3 contributed dsize=30, asize=35, items=1.
	if tot.Count != 2 || tot.DSize != 140 || tot.ASize != 150 || tot.Items != 3 {
		t.Fatalf("second delete totals = %+v", tot)
	}

	// Reopen strictly read-only (?mode=ro) and verify every reader API.
	d, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	vol, err = d.Get(1)
	if err != nil || vol.DSize != 10 || vol.ASize != 10 || vol.Items != 1 {
		t.Fatalf("vol = %+v err=%v", vol, err)
	}
	root, err := d.Get(0)
	if err != nil || root.DSize != 10 || root.ASize != 10 {
		t.Fatalf("root = %+v err=%v", root, err)
	}
	if _, err := d.Get(2); err != ErrNotFound {
		t.Fatalf("Get(2) = %v", err)
	}
	if _, err := d.Path(5); err != ErrNotFound {
		t.Fatalf("Path(descendant 5) = %v, want ErrNotFound", err)
	}
	ch, total, err = d.Children(1, ListOpts{Sort: "size", Desc: true, Limit: 10})
	if err != nil || total != 1 || fmt.Sprint(ids(ch)) != "[4]" {
		t.Fatalf("vol children = %v total=%d err=%v", ids(ch), total, err)
	}
	meta, err := d.Meta()
	if err != nil || meta["volumes"] != `[{"entryId":1,"fsFree":240}]` || meta[MetaDeletedCount] != "3" {
		t.Fatalf("meta = %v err=%v", meta, err)
	}
	var dirs []int64
	if err := d.EachDir(func(e Entry) error { dirs = append(dirs, e.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(dirs) != "[1 4]" {
		t.Fatalf("EachDir = %v, want [1 4]", dirs)
	}
}
