package dupcheck

import (
	"slices"
	"testing"

	"storage-analyzer/internal/scandb"
)

// treeB builds an in-memory scan tree. Ids grow in creation order, so a
// child always has a larger id than its parent, as in a real scan.
type treeB struct {
	nodes []Node
	files map[int64]Digest
	links map[int64]string
	root  int64
}

func newTree() *treeB {
	tb := &treeB{files: map[int64]Digest{}, links: map[int64]string{}}
	tb.root = tb.dir(0, "vol")
	return tb
}

func (tb *treeB) add(n Node) int64 {
	n.ID = int64(len(tb.nodes) + 1)
	tb.nodes = append(tb.nodes, n)
	return n.ID
}

func (tb *treeB) dir(parent int64, name string) int64 {
	return tb.add(Node{Parent: parent, Name: []byte(name), Kind: scandb.KindDir})
}

// file adds a regular file; d != 0 marks it as a confirmed duplicate with
// digest {d}, d == 0 leaves it unconfirmed (unique).
func (tb *treeB) file(parent int64, name string, size int64, d byte) int64 {
	id := tb.add(Node{Parent: parent, Name: []byte(name), Kind: scandb.KindFile, ASize: size, DSize: size})
	if d != 0 {
		tb.files[id] = Digest{d}
	}
	return id
}

func (tb *treeB) link(parent int64, name, target string) int64 {
	id := tb.add(Node{Parent: parent, Name: []byte(name), Kind: scandb.KindSymlink})
	tb.links[id] = target
	return id
}

func (tb *treeB) run(t *testing.T) []Group {
	t.Helper()
	dirs := BuildDirs(func(yield func(Node) bool) {
		for _, n := range tb.nodes {
			if n.Kind == scandb.KindDir && !yield(n) {
				return
			}
		}
	})
	gs, err := RollUp(func(yield func(Node) bool) {
		for i := len(tb.nodes) - 1; i >= 0; i-- {
			if !yield(tb.nodes[i]) {
				return
			}
		}
	}, RollUpInput{Dirs: dirs, Files: tb.files, Links: tb.links})
	if err != nil {
		t.Fatal(err)
	}
	return gs
}

func memberIDs(g Group) []int64 {
	var out []int64
	for _, m := range g.Members {
		out = append(out, m.ID)
	}
	return out
}

// find returns the group whose members are exactly ids.
func find(t *testing.T, gs []Group, ids ...int64) Group {
	t.Helper()
	slices.Sort(ids)
	for _, g := range gs {
		if slices.Equal(memberIDs(g), ids) {
			return g
		}
	}
	t.Fatalf("no group with members %v in %+v", ids, gs)
	return Group{}
}

func dirGroups(gs []Group) []Group {
	var out []Group
	for _, g := range gs {
		if g.Kind == scandb.KindDir {
			out = append(out, g)
		}
	}
	return out
}

func TestIgnoredName(t *testing.T) {
	for _, c := range []struct {
		name  string
		isDir bool
		want  bool
	}{
		{"@__thumb", true, true}, {".@__thumb", true, true}, {"@eaDir", true, true},
		{".DS_Store", false, true}, {"Thumbs.db", false, true}, {"._movie.mkv", false, true},
		{"._dir", true, false}, {"movie.mkv", false, false}, {"thumbs.db", false, false},
	} {
		if got := IgnoredName([]byte(c.name), c.isDir); got != c.want {
			t.Errorf("IgnoredName(%q, %v) = %v, want %v", c.name, c.isDir, got, c.want)
		}
	}
}

func TestRollUpNestedFoldersMergeToTop(t *testing.T) {
	tb := newTree()
	p := tb.dir(tb.root, "P")
	a := tb.dir(p, "sub")
	tb.file(a, "x", 100, 1)
	tb.file(a, "y", 200, 2)
	q := tb.dir(tb.root, "Q")
	b := tb.dir(q, "sub")
	tb.file(b, "x", 100, 1)
	tb.file(b, "y", 200, 2)
	gs := tb.run(t)
	if len(gs) != 1 {
		t.Fatalf("want 1 group, got %+v", gs)
	}
	g := find(t, gs, p, q)
	if g.Kind != scandb.KindDir || g.Files != 2 || g.ASize != 300 || g.RASize != 300 {
		t.Fatalf("group = %+v", g)
	}
}

func TestRollUpFolderNameMayDiffer(t *testing.T) {
	tb := newTree()
	a := tb.dir(tb.root, "Photos")
	tb.file(a, "a.jpg", 50, 1)
	b := tb.dir(tb.root, "Photos 副本")
	tb.file(b, "a.jpg", 50, 1)
	gs := tb.run(t)
	if len(gs) != 1 || gs[0].Kind != scandb.KindDir {
		t.Fatalf("groups = %+v", gs)
	}
	find(t, gs, a, b)
}

func TestRollUpPartialCoverage(t *testing.T) {
	tb := newTree()
	a := tb.dir(tb.root, "A")
	ax := tb.file(a, "x", 1000, 1)
	b := tb.dir(tb.root, "B")
	bx := tb.file(b, "x", 1000, 1)
	c := tb.dir(tb.root, "C")
	cx := tb.file(c, "x", 1000, 1)
	tb.file(c, "z", 10, 0)
	gs := tb.run(t)
	if len(gs) != 2 {
		t.Fatalf("want 2 groups, got %+v", gs)
	}
	find(t, gs, a, b)
	g := find(t, gs, ax, bx, cx)
	covered := map[int64]bool{}
	for _, m := range g.Members {
		covered[m.ID] = m.Covered
	}
	if !covered[ax] || !covered[bx] || covered[cx] {
		t.Fatalf("covered = %v", covered)
	}
	if g.RASize != 1000 || g.RDSize != 1000 {
		t.Fatalf("reclaim = %d/%d, want 1000 (C/x plus one copy kept inside A/B)", g.RASize, g.RDSize)
	}
}

func TestRollUpFileNameMismatch(t *testing.T) {
	tb := newTree()
	a := tb.dir(tb.root, "A")
	ax := tb.file(a, "x", 100, 1)
	b := tb.dir(tb.root, "B")
	bx := tb.file(b, "x2", 100, 1)
	gs := tb.run(t)
	if len(dirGroups(gs)) != 0 {
		t.Fatalf("unexpected dir groups %+v", gs)
	}
	g := find(t, gs, ax, bx)
	if g.Kind != scandb.KindFile || g.Files != 1 {
		t.Fatalf("group = %+v", g)
	}
}

func TestRollUpExtraFileBreaksFolder(t *testing.T) {
	tb := newTree()
	a := tb.dir(tb.root, "A")
	tb.file(a, "x", 100, 1)
	b := tb.dir(tb.root, "B")
	tb.file(b, "x", 100, 1)
	by := tb.file(b, "y", 200, 2)
	c := tb.dir(tb.root, "C")
	cy := tb.file(c, "y", 200, 2)
	gs := tb.run(t)
	if len(dirGroups(gs)) != 0 {
		t.Fatalf("unexpected dir groups %+v", gs)
	}
	find(t, gs, by, cy)
}

func TestRollUpStructureMatters(t *testing.T) {
	tb := newTree()
	a := tb.dir(tb.root, "A")
	tb.file(a, "x", 100, 1)
	b := tb.dir(tb.root, "B")
	sub := tb.dir(b, "sub")
	tb.file(sub, "x", 100, 1)
	gs := tb.run(t)
	if len(gs) != 1 {
		t.Fatalf("want only {A, B/sub}, got %+v", gs)
	}
	find(t, gs, a, sub)
}

func TestRollUpIgnoresJunk(t *testing.T) {
	tb := newTree()
	a := tb.dir(tb.root, "A")
	tb.file(a, "x", 100, 1)
	tb.file(a, ".DS_Store", 6, 0)
	ea := tb.dir(a, "@eaDir")
	ta := tb.file(ea, "t", 30, 9)
	b := tb.dir(tb.root, "B")
	tb.file(b, "x", 100, 1)
	th := tb.dir(b, "@__thumb")
	tb.file(th, "t", 40, 0)
	tb.file(b, "._x", 4, 0)
	eb := tb.dir(b, "@eaDir")
	tbb := tb.file(eb, "t", 30, 9)
	gs := tb.run(t)
	if len(gs) != 1 {
		t.Fatalf("want only {A, B}, got %+v", gs)
	}
	g := find(t, gs, a, b)
	if g.ASize != 100 || g.Files != 1 {
		t.Fatalf("ignored entries counted: %+v", g)
	}
	for _, g := range gs {
		for _, m := range g.Members {
			if m.ID == ta || m.ID == tbb {
				t.Fatalf("ignored file listed: %+v", g)
			}
		}
	}
}

func TestRollUpEmptyNotListed(t *testing.T) {
	tb := newTree()
	a := tb.dir(tb.root, "A")
	tb.file(a, "e", 0, 0)
	b := tb.dir(tb.root, "B")
	tb.file(b, "e", 0, 0)
	tb.dir(tb.root, "C")
	tb.dir(tb.root, "D")
	if gs := tb.run(t); len(gs) != 0 {
		t.Fatalf("want no groups, got %+v", gs)
	}
}

func TestRollUpUniqueChildPoisonsFolder(t *testing.T) {
	for name, extra := range map[string]Node{
		"hardlink": {Name: []byte("h"), Kind: scandb.KindFile, ASize: 10, DSize: 10, Flags: scandb.FlagHardlinkDup},
		"mount":    {Name: []byte("m"), Kind: scandb.KindMount},
		"error":    {Name: []byte("e"), Kind: scandb.KindOther, Flags: scandb.FlagError},
		"unique":   {Name: []byte("u"), Kind: scandb.KindFile, ASize: 10, DSize: 10},
	} {
		t.Run(name, func(t *testing.T) {
			tb := newTree()
			a := tb.dir(tb.root, "A")
			ax := tb.file(a, "x", 100, 1)
			ea := extra
			ea.Parent = a
			tb.add(ea)
			b := tb.dir(tb.root, "B")
			bx := tb.file(b, "x", 100, 1)
			eb := extra
			eb.Parent = b
			tb.add(eb)
			gs := tb.run(t)
			if len(gs) != 1 {
				t.Fatalf("want only the file group, got %+v", gs)
			}
			g := find(t, gs, ax, bx)
			if g.Members[0].Covered || g.Members[1].Covered {
				t.Fatalf("members should not be covered: %+v", g)
			}
		})
	}
}

func TestRollUpSymlinkByTarget(t *testing.T) {
	tb := newTree()
	mk := func(name, target string) int64 {
		d := tb.dir(tb.root, name)
		tb.file(d, "x", 100, 1)
		tb.link(d, "l", target)
		return d
	}
	a, b := mk("A", "t"), mk("B", "t")
	mk("C", "t1")
	mk("D", "t2")
	dg := dirGroups(tb.run(t))
	if len(dg) != 1 {
		t.Fatalf("want one dir group, got %+v", dg)
	}
	find(t, dg, a, b)
}

func TestRollUpGroupSizeIsMin(t *testing.T) {
	tb := newTree()
	x1 := tb.add(Node{Parent: tb.root, Name: []byte("x1"), Kind: scandb.KindFile, ASize: 100, DSize: 4096})
	x2 := tb.add(Node{Parent: tb.root, Name: []byte("x2"), Kind: scandb.KindFile, ASize: 100, DSize: 8192})
	tb.files[x1], tb.files[x2] = Digest{1}, Digest{1}
	g := find(t, tb.run(t), x1, x2)
	if g.DSize != 4096 || g.RDSize != 4096 || g.ASize != 100 || g.RASize != 100 {
		t.Fatalf("group = %+v", g)
	}
}

func TestRollUpRejectsAscending(t *testing.T) {
	_, err := RollUp(func(yield func(Node) bool) {
		for _, id := range []int64{1, 2} {
			if !yield(Node{ID: id, Kind: scandb.KindDir}) {
				return
			}
		}
	}, RollUpInput{})
	if err == nil {
		t.Fatal("want error for ascending ids")
	}
}
