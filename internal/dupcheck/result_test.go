package dupcheck

import (
	"fmt"
	"path/filepath"
	"testing"
)

func mkGroup(path string, size, reclaim int64, copies int) OutGroup {
	g := OutGroup{Kind: 0, Files: 1, ASize: size * 2, DSize: size, RASize: reclaim * 2, RDSize: reclaim}
	for i := 0; i < copies; i++ {
		g.Members = append(g.Members, OutMember{EntryID: int64(i + 10), ParentID: 1, Path: []byte(fmt.Sprintf("%s/%03d", path, copies-i))})
	}
	return g
}

func writeOpen(t *testing.T, groups []OutGroup) *Result {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dups-1.db")
	if err := WriteResult(p, groups, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	r, err := OpenResult(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func pageNames(t *testing.T, r *Result, mode string) []string {
	t.Helper()
	gs, err := r.Page(mode, 0, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, g := range gs {
		out = append(out, string(g.Members[0].Path))
	}
	return out
}

func TestResultPageOrderDisk(t *testing.T) {
	a, b, c := mkGroup("a", 10, 10, 2), mkGroup("b", 30, 30, 2), mkGroup("c", 20, 20, 2)
	c.ASize = 1000 // apparent order differs from disk order
	r := writeOpen(t, []OutGroup{a, b, c})
	if got := fmt.Sprint(pageNames(t, r, "disk")); got != "[b/001 c/001 a/001]" {
		t.Fatalf("disk order %s", got)
	}
	if got := fmt.Sprint(pageNames(t, r, "apparent")); got != "[c/001 b/001 a/001]" {
		t.Fatalf("apparent order %s", got)
	}
	m, err := r.Meta()
	if err != nil || m["k"] != "v" {
		t.Fatalf("meta %v %v", m, err)
	}
}

func TestResultTieBreak(t *testing.T) {
	r := writeOpen(t, []OutGroup{mkGroup("z", 10, 10, 2), mkGroup("y", 10, 20, 3), mkGroup("x", 10, 10, 2)})
	if got := fmt.Sprint(pageNames(t, r, "disk")); got != "[y/001 x/001 z/001]" {
		t.Fatalf("order %s", got)
	}
}

func TestResultPagingAndTotal(t *testing.T) {
	var gs []OutGroup
	for i := 0; i < 250; i++ {
		gs = append(gs, mkGroup(fmt.Sprintf("g%03d", i), int64(1000-i), 1, 2))
	}
	r := writeOpen(t, gs)
	n, err := r.Total()
	if err != nil || n != 250 {
		t.Fatalf("total %d %v", n, err)
	}
	page, err := r.Page("disk", 200, 100, 100)
	if err != nil || len(page) != 50 || string(page[0].Members[0].Path) != "g200/001" {
		t.Fatalf("page len %d err %v", len(page), err)
	}
	if page[0].Name == nil || string(page[0].Name) != "001" || page[0].Copies != 2 {
		t.Fatalf("group %+v", page[0])
	}
}

func TestResultMemberCap(t *testing.T) {
	r := writeOpen(t, []OutGroup{mkGroup("m", 10, 10, 300)})
	gs, err := r.Page("disk", 0, 10, 100)
	if err != nil || len(gs) != 1 {
		t.Fatal(err)
	}
	if len(gs[0].Members) != 100 || gs[0].Copies != 300 || string(gs[0].Members[0].Path) != "m/001" {
		t.Fatalf("members %d copies %d first %s", len(gs[0].Members), gs[0].Copies, gs[0].Members[0].Path)
	}
}

func TestResultBadBytesRoundTrip(t *testing.T) {
	g := mkGroup("x", 10, 10, 2)
	g.Members[0].Path = []byte("vol/a\xff\nb")
	g.Members[0].Covered = true
	r := writeOpen(t, []OutGroup{g})
	gs, _ := r.Page("disk", 0, 10, 10)
	m := gs[0].Members
	if string(m[0].Path) != "vol/a\xff\nb" || !m[0].Covered || m[1].Covered {
		t.Fatalf("members %+v", m)
	}
}

func TestPruneDeleted(t *testing.T) {
	p := filepath.Join(t.TempDir(), "dups-1.db")
	// Group 1: 3 file copies (ids 10, 11, 12) at vol/a/f, vol/b/f, vol/c/f (dsize=100, asize=120, rdsize=200, rasize=240)
	g1 := OutGroup{
		Kind: 0, Files: 1, DSize: 100, ASize: 120, RDSize: 200, RASize: 240,
		Members: []OutMember{
			{EntryID: 10, ParentID: 2, Path: []byte("vol/a/f")},
			{EntryID: 11, ParentID: 3, Path: []byte("vol/b/f")},
			{EntryID: 12, ParentID: 4, Path: []byte("vol/c/f")},
		},
	}
	// Group 2: 2 file copies (ids 20, 21) at vol/dir1/x, vol/dir2/x (dsize=50, asize=60, rdsize=50, rasize=60)
	g2 := OutGroup{
		Kind: 0, Files: 1, DSize: 50, ASize: 60, RDSize: 50, RASize: 60,
		Members: []OutMember{
			{EntryID: 20, ParentID: 5, Path: []byte("vol/dir1/x")},
			{EntryID: 21, ParentID: 6, Path: []byte("vol/dir2/x")},
		},
	}
	// Group 3: 2 dir copies (ids 30, 31) at vol/dupdir1, vol/dupdir2 (dsize=40, asize=40, rdsize=40, rasize=40)
	g3 := OutGroup{
		Kind: 1, Files: 2, DSize: 40, ASize: 40, RDSize: 40, RASize: 40,
		Members: []OutMember{
			{EntryID: 30, ParentID: 1, Path: []byte("vol/dupdir1")},
			{EntryID: 31, ParentID: 1, Path: []byte("vol/dupdir2")},
		},
	}
	if err := WriteResult(p, []OutGroup{g1, g2, g3}, map[string]string{
		"groups": "3", "reclaimDSize": "290", "reclaimASize": "340", "finishedAt": "100",
	}); err != nil {
		t.Fatal(err)
	}

	// Delete entry 10 directly, plus delete folder "vol/dir1" (prefix matches vol/dir1/x in g2!).
	sum, err := PruneDeleted(p, []int64{10}, [][]byte{[]byte("vol/a/f"), []byte("vol/dir1")})
	if err != nil {
		t.Fatal(err)
	}
	if !sum.Changed || sum.Groups != 2 || sum.ReclaimDSize != 140 || sum.ReclaimASize != 160 {
		t.Fatalf("PruneSummary = %+v, want Changed=true Groups=2 ReclaimDSize=140 ReclaimASize=160", sum)
	}

	// Reopen read-only and verify updated groups and meta.
	r, err := OpenResult(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	total, err := r.Total()
	if err != nil || total != 2 {
		t.Fatalf("total = %d err=%v", total, err)
	}
	gs, err := r.Page("disk", 0, 10, 10)
	if err != nil || len(gs) != 2 {
		t.Fatalf("page = %v err=%v", gs, err)
	}
	if gs[0].Copies != 2 || len(gs[0].Members) != 2 || gs[0].RDSize != 100 || gs[0].RASize != 120 {
		t.Fatalf("g1 updated = %+v", gs[0])
	}
	if string(gs[0].Members[0].Path) != "vol/b/f" || string(gs[0].Members[1].Path) != "vol/c/f" {
		t.Fatalf("g1 members = %+v", gs[0].Members)
	}
	if gs[1].Copies != 2 || string(gs[1].Members[0].Path) != "vol/dupdir1" {
		t.Fatalf("g3 = %+v", gs[1])
	}
	meta, err := r.Meta()
	if err != nil || meta["groups"] != "2" || meta["reclaimDSize"] != "140" || meta["reclaimASize"] != "160" {
		t.Fatalf("meta = %v err=%v", meta, err)
	}

	// Unmatched delete returns Changed=false.
	sum2, err := PruneDeleted(p, []int64{999}, [][]byte{[]byte("vol/other")})
	if err != nil || sum2.Changed {
		t.Fatalf("no-op prune = %+v err=%v", sum2, err)
	}
}
