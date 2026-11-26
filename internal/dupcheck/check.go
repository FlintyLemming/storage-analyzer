package dupcheck

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

// Root maps a volume root entry to where it is mounted now.
type Root struct {
	Name       string // volume name, first element of display paths
	MountPoint string
}

// Summary is the outcome of one check.
type Summary struct {
	Groups                     int64
	ReclaimDSize, ReclaimASize int64
	BytesRead, CacheHits       int64
	Changed, Errors            int64
}

// Options configure Run.
type Options struct {
	ScanDB       string         // scan-<id>.db
	Roots        map[int64]Root // volume root entry id → mount
	Cache        *Cache         // optional
	Out          string         // dups-<id>.db, written via Out+".tmp"
	Progress     *Progress      // optional
	FS           FS             // default OSFS()
	QuickWorkers int            // default 4
}

var errStop = errors.New("stop")

func nodeOf(e scandb.Entry) Node {
	return Node{ID: e.ID, Parent: e.Parent, Name: e.Name, Kind: e.Kind, Flags: e.Flags, ASize: e.ASize, DSize: e.DSize}
}

// seq turns a callback walker into an iterator; the walker's error (other
// than an early stop) is stored in *errp.
func seq(walk func(func(scandb.Entry) error) error, errp *error) func(func(scandb.Entry) bool) {
	return func(yield func(scandb.Entry) bool) {
		err := walk(func(e scandb.Entry) error {
			if !yield(e) {
				return errStop
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			*errp = err
		}
	}
}

// pather builds filesystem and display paths from the directory index.
type pather struct {
	dirs   map[int64]DirInfo
	roots  map[int64]Root
	memo   map[int64]string // dir id → path below its volume root ("" for the root)
	rootOf map[int64]int64
}

func (p *pather) rel(dir int64) (int64, string, bool) {
	if r, ok := p.memo[dir]; ok {
		return p.rootOf[dir], r, true
	}
	d, ok := p.dirs[dir]
	if !ok {
		return 0, "", false
	}
	if d.Parent == 0 {
		p.memo[dir], p.rootOf[dir] = "", dir
		return dir, "", true
	}
	root, parent, ok := p.rel(d.Parent)
	if !ok {
		return 0, "", false
	}
	r := string(d.Name)
	if parent != "" {
		r = parent + "/" + r
	}
	p.memo[dir], p.rootOf[dir] = r, root
	return root, r, true
}

// path joins the volume prefix (mount point or volume name) with the path
// of entry name under directory parent.
func (p *pather) path(parent int64, name []byte, display bool) (string, bool) {
	root, r, ok := p.rel(parent)
	if !ok {
		return "", false
	}
	rt, ok := p.roots[root]
	if !ok {
		return "", false
	}
	prefix := rt.MountPoint
	if display {
		prefix = rt.Name
	}
	if r != "" {
		return prefix + "/" + r + "/" + string(name), true
	}
	return prefix + "/" + string(name), true
}

// Run checks one scan for duplicates and writes the listed groups to o.Out.
func Run(ctx context.Context, o Options) (Summary, error) {
	if o.Progress == nil {
		o.Progress = &Progress{}
	}
	if o.FS == nil {
		o.FS = OSFS()
	}
	p := o.Progress
	p.Begin("prepare", 0, 0)
	if err := ctx.Err(); err != nil {
		return Summary{}, err
	}
	db, err := scandb.Open(o.ScanDB)
	if err != nil {
		return Summary{}, err
	}
	defer db.Close()

	var ierr error
	dirs := BuildDirs(func(yield func(Node) bool) {
		for e := range seq(db.EachDir, &ierr) {
			if !yield(nodeOf(e)) {
				return
			}
		}
	})
	if ierr != nil {
		return Summary{}, ierr
	}
	pt := &pather{dirs: dirs, roots: o.Roots, memo: map[int64]string{}, rootOf: map[int64]int64{}}

	// Candidates: files sharing their size, minus ignored clutter. Paths are
	// built only when a file is touched, to keep the table small.
	cands := &Candidates{}
	n := 0
	err = db.DupSizeFiles(func(e scandb.Entry) error {
		if n++; n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if IgnoredName(e.Name, false) || dirs[e.Parent].Ignored {
			return nil
		}
		if _, _, ok := pt.rel(e.Parent); !ok {
			return nil
		}
		cands.Add(e.ID, e.Parent, e.Name, e.ASize, e.MTime)
		return nil
	})
	if err != nil {
		return Summary{}, err
	}
	h := &Hasher{FS: o.FS, Cache: o.Cache, Progress: p, QuickWorkers: o.QuickWorkers,
		Path: func(dir int64, name []byte) string { s, _ := pt.path(dir, name, false); return s }}
	files, err := h.Run(ctx, cands)
	if err != nil {
		return Summary{}, err
	}
	cands = nil

	// Roll up. Symlink targets are read just before their node is yielded,
	// so RollUp sees them in Links when it reaches the node.
	p.Begin("rollup", 0, 0)
	links := map[int64]string{}
	n = 0
	rolled, err := RollUp(func(yield func(Node) bool) {
		for e := range seq(db.EachDesc, &ierr) {
			if n++; n%4096 == 0 {
				if err := ctx.Err(); err != nil {
					ierr = err
					return
				}
			}
			if e.Kind == scandb.KindSymlink && !dirs[e.Parent].Ignored {
				if fsPath, ok := pt.path(e.Parent, e.Name, false); ok {
					if t, err := o.FS.Readlink(fsPath); err == nil {
						links[e.ID] = t
					}
				}
			}
			if !yield(nodeOf(e)) {
				return
			}
		}
	}, RollUpInput{Dirs: dirs, Files: files, Links: links})
	if ierr != nil {
		return Summary{}, ierr
	}
	if err != nil {
		return Summary{}, err
	}
	if err := ctx.Err(); err != nil {
		return Summary{}, err
	}

	p.Begin("write", int64(len(rolled)), 0)
	s := Summary{
		Groups: int64(len(rolled)), BytesRead: p.BytesRead.Load(), CacheHits: p.CacheHits.Load(),
		Changed: p.Changed.Load(), Errors: p.Errors.Load(),
	}
	out := make([]OutGroup, 0, len(rolled))
	for _, g := range rolled {
		og := OutGroup{Kind: g.Kind, Files: g.Files, ASize: g.ASize, DSize: g.DSize, RASize: g.RASize, RDSize: g.RDSize}
		for _, m := range g.Members {
			var path string
			if m.Parent == 0 {
				path = o.Roots[m.ID].Name
			} else {
				path, _ = pt.path(m.Parent, m.Name, true)
			}
			og.Members = append(og.Members, OutMember{EntryID: m.ID, ParentID: m.Parent, Path: []byte(path), Covered: m.Covered})
		}
		s.ReclaimASize += g.RASize
		s.ReclaimDSize += g.RDSize
		out = append(out, og)
	}
	meta := map[string]string{
		"groups": strconv.FormatInt(s.Groups, 10), "reclaimDSize": strconv.FormatInt(s.ReclaimDSize, 10),
		"reclaimASize": strconv.FormatInt(s.ReclaimASize, 10), "bytesRead": strconv.FormatInt(s.BytesRead, 10),
		"cacheHits": strconv.FormatInt(s.CacheHits, 10), "changed": strconv.FormatInt(s.Changed, 10),
		"errors": strconv.FormatInt(s.Errors, 10), "finishedAt": strconv.FormatInt(time.Now().Unix(), 10),
	}
	if err := WriteResult(o.Out, out, meta); err != nil {
		return Summary{}, err
	}
	if o.Cache != nil {
		if _, err := o.Cache.Prune(); err != nil {
			return s, err
		}
	}
	return s, nil
}
