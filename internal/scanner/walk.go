// Package scanner walks volume trees in parallel and emits scandb entries.
//
// Ids: volume roots get 1..len(roots). When a directory is listed, all of its
// children receive one contiguous block of ids, so the database can list a
// directory as an id range. File-like children are emitted right away; a
// directory row is emitted once its whole subtree is done and its totals are
// known.
package scanner

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unicode/utf8"

	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

// Sink receives entries. Walk calls Add from a single goroutine.
type Sink interface{ Add(scandb.Entry) error }

// Root is one tree to walk; Name becomes the root entry's name.
type Root struct {
	Name string
	Path string
}

// Stats are live counters, safe to read while Walk runs.
type Stats struct {
	Files, Dirs, Errors, Bytes atomic.Int64
	Current                    atomic.Value // string: directory being listed
}

// Walker walks trees. The zero value is ready to use.
type Walker struct {
	Workers      int                                         // default 4
	IsMountPoint func(path string, rootDev, dev uint64) bool // default: dev != rootDev
	Stats        Stats
}

type dirNode struct {
	id, parentID int64
	parent       *dirNode
	name         []byte
	path         string
	rootDev      uint64
	mtime        int64
	flags        int

	mu                  sync.Mutex
	dsize, asize, items int64
	firstChild, nchild  int64
	pending             atomic.Int32 // own listing + unfinished child directories
}

type walkState struct {
	w      *Walker
	ctx    context.Context
	cancel context.CancelCauseFunc
	nextID atomic.Int64
	out    chan []scandb.Entry

	linksMu sync.Mutex
	links   map[[2]uint64]struct{}

	qmu    sync.Mutex
	qcond  *sync.Cond
	stack  []*dirNode
	active int
	done   bool
}

// Walk walks all roots and sends every entry to sink. It returns ctx's error
// if canceled, or the first error returned by sink.
func (w *Walker) Walk(ctx context.Context, roots []Root, sink Sink) error {
	if w.Workers <= 0 {
		w.Workers = 4
	}
	if w.IsMountPoint == nil {
		w.IsMountPoint = func(_ string, rootDev, dev uint64) bool { return dev != rootDev }
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	k := &walkState{w: w, ctx: cctx, cancel: cancel, out: make(chan []scandb.Entry, 256), links: map[[2]uint64]struct{}{}}
	k.qcond = sync.NewCond(&k.qmu)
	k.nextID.Store(int64(len(roots)))

	// Writer goroutine: the only caller of sink.Add.
	writeErr := make(chan error, 1)
	go func() {
		var err error
		for batch := range k.out {
			if err != nil {
				continue // drain
			}
			for _, e := range batch {
				if err = sink.Add(e); err != nil {
					cancel(err)
					break
				}
			}
		}
		writeErr <- err
	}()

	for i, r := range roots {
		n := &dirNode{id: int64(i + 1), name: []byte(r.Name), path: r.Path}
		n.pending.Store(1)
		var st syscall.Stat_t
		if err := syscall.Lstat(r.Path, &st); err != nil {
			n.flags |= scandb.FlagError
			w.Stats.Errors.Add(1)
			k.finishListing(n)
			continue
		}
		dev, _, _, mtime := statFields(&st)
		n.rootDev, n.mtime = dev, mtime
		n.dsize, n.asize = st.Blocks*512, st.Size
		k.push(n)
	}

	var wg sync.WaitGroup
	for i := 0; i < w.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n := k.pop()
				if n == nil {
					return
				}
				k.list(n)
				k.finishWork()
			}
		}()
	}
	wg.Wait()
	close(k.out)
	if err := <-writeErr; err != nil {
		return err
	}
	if err := context.Cause(cctx); err != nil && cctx.Err() != nil {
		return err
	}
	return nil
}

func (k *walkState) push(n *dirNode) {
	k.qmu.Lock()
	k.stack = append(k.stack, n)
	k.qmu.Unlock()
	k.qcond.Signal()
}

func (k *walkState) pop() *dirNode {
	k.qmu.Lock()
	defer k.qmu.Unlock()
	for {
		if k.done {
			return nil
		}
		if k.ctx.Err() != nil {
			k.done = true
			k.qcond.Broadcast()
			return nil
		}
		if len(k.stack) > 0 {
			n := k.stack[len(k.stack)-1]
			k.stack = k.stack[:len(k.stack)-1]
			k.active++
			return n
		}
		if k.active == 0 {
			k.done = true
			k.qcond.Broadcast()
			return nil
		}
		k.qcond.Wait()
	}
}

func (k *walkState) finishWork() {
	k.qmu.Lock()
	k.active--
	if k.active == 0 && len(k.stack) == 0 {
		k.done = true
	}
	k.qmu.Unlock()
	k.qcond.Broadcast()
}

func (k *walkState) emit(batch []scandb.Entry) {
	select {
	case k.out <- batch:
	case <-k.ctx.Done():
	}
}

func nameFlags(name []byte) int {
	if utf8.Valid(name) {
		return 0
	}
	return scandb.FlagBadName
}

// list reads one directory, emits its non-directory children and queues its
// subdirectories.
func (k *walkState) list(n *dirNode) {
	w := k.w
	w.Stats.Current.Store(n.path)
	w.Stats.Dirs.Add(1)
	f, err := os.Open(n.path)
	var names []string
	if err == nil {
		names, err = f.Readdirnames(-1)
		f.Close()
	}
	if err != nil {
		n.flags |= scandb.FlagError
		w.Stats.Errors.Add(1)
		if len(names) == 0 {
			k.finishListing(n)
			return
		}
	}
	cnt := int64(len(names))
	first := k.nextID.Add(cnt) - cnt + 1
	batch := make([]scandb.Entry, 0, len(names))
	var subdirs []*dirNode
	var dsum, asum int64
	for i, name := range names {
		p := join(n.path, name)
		e := scandb.Entry{ID: first + int64(i), Parent: n.id, Name: []byte(name)}
		e.Flags = nameFlags(e.Name)
		var st syscall.Stat_t
		if err := syscall.Lstat(p, &st); err != nil {
			e.Kind = scandb.KindOther
			e.Flags |= scandb.FlagError
			w.Stats.Errors.Add(1)
			batch = append(batch, e)
			continue
		}
		dev, ino, nlink, mtime := statFields(&st)
		e.MTime = mtime
		mode := uint32(st.Mode) & syscall.S_IFMT
		if w.IsMountPoint(p, n.rootDev, dev) {
			e.Kind = scandb.KindMount
			batch = append(batch, e)
			continue
		}
		if mode == syscall.S_IFDIR {
			c := &dirNode{id: e.ID, parentID: n.id, parent: n, name: e.Name, path: p, rootDev: n.rootDev, mtime: mtime, flags: e.Flags}
			c.dsize, c.asize = st.Blocks*512, st.Size
			c.pending.Store(1)
			subdirs = append(subdirs, c)
			continue
		}
		switch mode {
		case syscall.S_IFREG:
			e.Kind = scandb.KindFile
		case syscall.S_IFLNK:
			e.Kind = scandb.KindSymlink
		default:
			e.Kind = scandb.KindOther
		}
		e.DSize, e.ASize = st.Blocks*512, st.Size
		w.Stats.Files.Add(1)
		if nlink > 1 && k.seenLink(dev, ino) {
			e.Flags |= scandb.FlagHardlinkDup
		} else {
			dsum += e.DSize
			asum += e.ASize
			w.Stats.Bytes.Add(e.DSize)
		}
		batch = append(batch, e)
	}
	n.mu.Lock()
	n.dsize += dsum
	n.asize += asum
	n.items += cnt
	n.firstChild, n.nchild = first, cnt
	n.mu.Unlock()
	n.pending.Add(int32(len(subdirs)))
	if len(batch) > 0 {
		k.emit(batch)
	}
	for _, c := range subdirs {
		k.push(c)
	}
	k.finishListing(n)
}

func (k *walkState) seenLink(dev, ino uint64) bool {
	key := [2]uint64{dev, ino}
	k.linksMu.Lock()
	defer k.linksMu.Unlock()
	if _, ok := k.links[key]; ok {
		return true
	}
	k.links[key] = struct{}{}
	return false
}

// finishListing drops one pending unit from n; when n has nothing left it is
// emitted and its totals roll up to the parent, repeating upwards.
func (k *walkState) finishListing(n *dirNode) {
	for n != nil && n.pending.Add(-1) == 0 {
		n.mu.Lock()
		e := scandb.Entry{
			ID: n.id, Parent: n.parentID, Name: n.name, Kind: scandb.KindDir, Flags: n.flags,
			DSize: n.dsize, ASize: n.asize, Items: n.items, MTime: n.mtime,
			FirstChild: n.firstChild, NChild: n.nchild,
		}
		n.mu.Unlock()
		k.emit([]scandb.Entry{e})
		p := n.parent
		if p != nil {
			p.mu.Lock()
			p.dsize += e.DSize
			p.asize += e.ASize
			p.items += e.Items
			p.mu.Unlock()
		}
		n = p
	}
}

// join appends name to dir, avoiding a doubled slash when dir ends in "/".
func join(dir, name string) string {
	if strings.HasSuffix(dir, "/") {
		return dir + name
	}
	return dir + "/" + name
}
