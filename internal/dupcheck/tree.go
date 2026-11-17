// Package dupcheck finds files and folders with identical content in a scan.
//
// Files are compared by full SHA-256 (see Hasher). Folders are compared by a
// Merkle-style token over their children's names and tokens, so two folders
// are the same only when names, structure and every file's content match;
// the folder's own name does not matter. Duplicate folders roll up: a group
// whose every member lies inside a duplicate folder is not listed.
package dupcheck

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"slices"

	"storage-analyzer/internal/scandb"
)

// Digest is a SHA-256 sum.
type Digest [32]byte

// Node is one scan entry as the roll-up sees it.
type Node struct {
	ID, Parent   int64
	Name         []byte
	Kind, Flags  int // scandb kinds and flags
	ASize, DSize int64
}

// DirInfo describes one directory of the scan.
type DirInfo struct {
	Parent  int64
	Name    []byte
	Ignored bool // the directory or one of its ancestors is on the ignore list
}

var ignoredNames = map[string]bool{
	"@__thumb": true, ".@__thumb": true, "@eaDir": true, ".DS_Store": true, "Thumbs.db": true,
}

// IgnoredName reports whether an entry with this name is system-generated
// clutter that is neither compared nor listed.
func IgnoredName(name []byte, isDir bool) bool {
	if ignoredNames[string(name)] {
		return true
	}
	return !isDir && bytes.HasPrefix(name, []byte("._"))
}

// BuildDirs indexes directories, which must arrive in ascending id order
// (parents before children).
func BuildDirs(dirs iter.Seq[Node]) map[int64]DirInfo {
	m := map[int64]DirInfo{}
	for n := range dirs {
		m[n.ID] = DirInfo{
			Parent:  n.Parent,
			Name:    n.Name,
			Ignored: IgnoredName(n.Name, true) || m[n.Parent].Ignored,
		}
	}
	return m
}

// RollUpInput is what RollUp needs besides the entries themselves.
type RollUpInput struct {
	Dirs  map[int64]DirInfo
	Files map[int64]Digest // confirmed duplicate files → full SHA-256
	Links map[int64]string // symlink → target
}

// Member is one copy in a group.
type Member struct {
	ID, Parent   int64
	Name         []byte
	ASize, DSize int64 // content size of this copy
	Covered      bool  // lies inside a member of a duplicate folder group
}

// Group is a set of identical files or folders.
type Group struct {
	Kind           int   // scandb.KindDir or scandb.KindFile
	Files          int64 // files per copy (1 for a file group)
	ASize, DSize   int64 // size of one copy: the smallest member
	RASize, RDSize int64 // reclaimable space
	Members        []Member
}

type acc struct {
	x                   [32]byte
	n                   int64
	unique              bool
	asize, dsize, files int64
}

var emptyToken = sha256.Sum256([]byte("E"))

func fileToken(size int64, d Digest) [32]byte {
	var b [1 + 8 + 32]byte
	b[0] = 'F'
	binary.LittleEndian.PutUint64(b[1:9], uint64(size))
	copy(b[9:], d[:])
	return sha256.Sum256(b[:])
}

func dirToken(a *acc) [32]byte {
	var b [1 + 32 + 8]byte
	b[0] = 'D'
	copy(b[1:33], a.x[:])
	binary.LittleEndian.PutUint64(b[33:], uint64(a.n))
	return sha256.Sum256(b[:])
}

// fold adds child (name, token) to a directory accumulator. Names are unique
// within a directory, so XOR of the per-child hashes never cancels out.
func (a *acc) fold(name []byte, tok [32]byte, asize, dsize, files int64) {
	h := sha256.New()
	h.Write(name)
	h.Write([]byte{0})
	h.Write(tok[:])
	var s [32]byte
	h.Sum(s[:0])
	for i := range a.x {
		a.x[i] ^= s[i]
	}
	a.n++
	a.asize += asize
	a.dsize += dsize
	a.files += files
}

type fileKey struct {
	size int64
	d    Digest
}

// RollUp computes the listed duplicate groups. desc must yield every entry of
// the scan in strictly descending id order (children before parents).
func RollUp(desc iter.Seq[Node], in RollUpInput) ([]Group, error) {
	accs := map[int64]*acc{}
	type dirMem struct {
		m     Member
		files int64
	}
	byDirTok := map[[32]byte][]dirMem{}
	byFile := map[fileKey][]Member{}
	last := int64(math.MaxInt64)
	for n := range desc {
		if n.ID >= last {
			return nil, fmt.Errorf("roll-up: ids not descending (%d after %d)", n.ID, last)
		}
		last = n.ID
		isDir := n.Kind == scandb.KindDir
		if IgnoredName(n.Name, isDir) || in.Dirs[n.Parent].Ignored {
			delete(accs, n.ID)
			continue
		}
		var (
			tok                 [32]byte
			unique              bool
			asize, dsize, files int64
		)
		switch {
		case isDir:
			a := accs[n.ID]
			delete(accs, n.ID)
			if a == nil {
				a = &acc{}
			}
			unique = a.unique || n.Flags&scandb.FlagError != 0
			tok = dirToken(a)
			asize, dsize, files = a.asize, a.dsize, a.files
			if !unique && asize > 0 {
				byDirTok[tok] = append(byDirTok[tok], dirMem{Member{ID: n.ID, Parent: n.Parent, Name: n.Name, ASize: asize, DSize: dsize}, files})
			}
		case n.Kind == scandb.KindFile && n.Flags&(scandb.FlagError|scandb.FlagHardlinkDup) == 0:
			files = 1
			if n.ASize == 0 {
				tok = emptyToken
			} else if d, ok := in.Files[n.ID]; ok {
				tok = fileToken(n.ASize, d)
				asize, dsize = n.ASize, n.DSize
				k := fileKey{n.ASize, d}
				byFile[k] = append(byFile[k], Member{ID: n.ID, Parent: n.Parent, Name: n.Name, ASize: n.ASize, DSize: n.DSize})
			} else {
				unique = true
			}
		case n.Kind == scandb.KindSymlink && n.Flags&scandb.FlagError == 0:
			if t, ok := in.Links[n.ID]; ok {
				tok = sha256.Sum256(append([]byte("L"), t...))
			} else {
				unique = true
			}
		default:
			unique = true
		}
		if n.Parent == 0 {
			continue
		}
		p := accs[n.Parent]
		if p == nil {
			p = &acc{}
			accs[n.Parent] = p
		}
		if unique {
			p.unique = true
		} else if !p.unique {
			p.fold(n.Name, tok, asize, dsize, files)
		}
	}

	// Directory groups and the set of their members.
	type rawGroup struct {
		kind    int
		files   int64
		members []Member
	}
	var raw []rawGroup
	dirGroupOf := map[int64]int{} // dir id → index in raw
	for _, mems := range byDirTok {
		if len(mems) < 2 {
			continue
		}
		g := rawGroup{kind: scandb.KindDir, files: mems[0].files}
		for _, dm := range mems {
			g.members = append(g.members, dm.m)
			dirGroupOf[dm.m.ID] = len(raw)
		}
		raw = append(raw, g)
	}
	for _, mems := range byFile {
		if len(mems) >= 2 {
			raw = append(raw, rawGroup{kind: scandb.KindFile, files: 1, members: mems})
		}
	}

	// outermost returns the index of the outermost duplicate-folder group
	// containing a member whose parent is parent, or -1.
	outermost := func(parent int64) int {
		found := -1
		for p, hops := parent, 0; p != 0 && hops < 4096; hops++ {
			if gi, ok := dirGroupOf[p]; ok {
				found = gi
			}
			p = in.Dirs[p].Parent
		}
		return found
	}

	var out []Group
	for _, rg := range raw {
		g := Group{Kind: rg.kind, Files: rg.files, ASize: math.MaxInt64, DSize: math.MaxInt64}
		uncovered := int64(0)
		covering := map[int]bool{}
		for _, m := range rg.members {
			if gi := outermost(m.Parent); gi >= 0 {
				m.Covered = true
				covering[gi] = true
			} else {
				uncovered++
			}
			g.ASize = min(g.ASize, m.ASize)
			g.DSize = min(g.DSize, m.DSize)
			g.Members = append(g.Members, m)
		}
		if uncovered == 0 {
			continue
		}
		eff := uncovered + int64(len(covering))
		g.RASize = g.ASize * (eff - 1)
		g.RDSize = g.DSize * (eff - 1)
		slices.SortFunc(g.Members, func(a, b Member) int { return compareInt(a.ID, b.ID) })
		out = append(out, g)
	}
	slices.SortFunc(out, func(a, b Group) int { return compareInt(a.Members[0].ID, b.Members[0].ID) })
	return out, nil
}

func compareInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
