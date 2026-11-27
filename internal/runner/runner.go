// Package runner orchestrates one complete scan: locking, catalog bookkeeping,
// walking, progress reporting and retention.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/docker"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
	"github.com/FlintyLemming/storage-analyzer/internal/scanner"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// Meta keys written into every scan database.
const (
	MetaVolumes    = "volumes"
	MetaSkipped    = "skipped"
	MetaStartedAt  = "startedAt"
	MetaFinishedAt = "finishedAt"
	MetaTrigger    = "trigger"
)

// ErrBusy means another scan holds the lock.
var ErrBusy = errors.New("scan already running")

// ErrNotRunning is returned by Cancel when no scan is running.
var ErrNotRunning = errors.New("no scan running")

// Progress is written to run/progress.json while a scan, duplicate check, or
// delete job runs. Job tells them apart; an empty Job (older binaries) is a scan.
type Progress struct {
	Job       string `json:"job"` // scan | dupcheck | delete
	JobID     string `json:"jobId,omitempty"`
	ScanID    int64  `json:"scanId"`
	PID       int    `json:"pid"`
	Trigger   string `json:"trigger"`
	StartedAt int64  `json:"startedAt"`
	Files     int64  `json:"files"`
	Dirs      int64  `json:"dirs"`
	Bytes     int64  `json:"bytes"`
	Errors    int64  `json:"errors"`
	Current   string `json:"current"`
	PrevFiles int64  `json:"prevFiles"`
	Next      string `json:"next,omitempty"` // dupcheck: a duplicate check follows this scan

	// Duplicate check only.
	Phase      string `json:"phase,omitempty"` // prepare | quick | full | rollup | write
	DoneFiles  int64  `json:"doneFiles,omitempty"`
	TotalFiles int64  `json:"totalFiles,omitempty"`
	DoneBytes  int64  `json:"doneBytes,omitempty"`
	TotalBytes int64  `json:"totalBytes,omitempty"`
	CacheHits  int64  `json:"cacheHits,omitempty"`
	Changed    int64  `json:"changed,omitempty"`

	// Delete only.
	Targets      int   `json:"targets,omitempty"`
	DoneTargets  int   `json:"doneTargets,omitempty"`
	DeletedItems int64 `json:"deletedItems,omitempty"`
	FreedBytes   int64 `json:"freedBytes,omitempty"`
	Failed       int   `json:"failed,omitempty"`
}

// VolumeMeta describes one scanned volume (stored as JSON in MetaVolumes).
type VolumeMeta struct {
	EntryID    int64  `json:"entryId"`
	Name       string `json:"name"`
	Pool       string `json:"pool"`
	Device     string `json:"device"`
	MountPoint string `json:"mountPoint"`
	FSType     string `json:"fsType,omitempty"`
	FSTotal    uint64 `json:"fsTotal"`
	FSUsed     uint64 `json:"fsUsed"`
	FSFree     uint64 `json:"fsFree"`
}

// StatVolumes converts discovered volumes into VolumeMeta populated with
// statfs capacity figures. EntryID is left 0; scans assign root IDs separately.
func StatVolumes(vols []volumes.Volume) []VolumeMeta {
	vm := make([]VolumeMeta, len(vols))
	for i, v := range vols {
		vm[i] = VolumeMeta{Name: v.Name, Pool: v.Pool, Device: v.Device, MountPoint: v.MountPoint, FSType: v.FSType}
		var st syscall.Statfs_t
		if syscall.Statfs(v.MountPoint, &st) == nil {
			bs := uint64(st.Bsize)
			vm[i].FSTotal = uint64(st.Blocks) * bs
			vm[i].FSUsed = (uint64(st.Blocks) - uint64(st.Bfree)) * bs
			vm[i].FSFree = uint64(st.Bavail) * bs
		}
	}
	return vm
}

// Options configure Run.
type Options struct {
	DataDir      string
	Trigger      string
	Discover     func() ([]volumes.Volume, []volumes.Skipped, error)
	IsMountPoint func(path string, parentDev, dev uint64) bool // optional; nil uses scanner default
	Now          func() time.Time
	Workers      int
	FS           dupcheck.FS // RunScheduled: filesystem for the duplicate check (default: real)

	thenDupCheck bool // set by RunScheduled: a duplicate check follows
}

func lockPath(dataDir string) string     { return filepath.Join(dataDir, "run", "scan.lock") }
func progressPath(dataDir string) string { return filepath.Join(dataDir, "run", "progress.json") }

// tryLock takes the scan lock without blocking. A status probe in another
// process can hold it for microseconds, so a few retries avoid false ErrBusy.
func tryLock(dataDir string, attempts int) (*os.File, error) {
	if err := os.MkdirAll(filepath.Join(dataDir, "run"), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(lockPath(dataDir), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	for i := 0; ; i++ {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			f.Close()
			return nil, err
		}
		if i+1 >= attempts {
			f.Close()
			return nil, ErrBusy
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func writeJSONAtomic(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// rootSink forwards to the db writer and remembers the volume root rows.
type rootSink struct {
	w     *scandb.Writer
	roots map[int64]scandb.Entry
}

func (s *rootSink) Add(e scandb.Entry) error {
	if e.Parent == 0 {
		s.roots[e.ID] = e
	}
	return s.w.Add(e)
}

// Run performs one scan and returns its catalog id.
func Run(ctx context.Context, o Options) (int64, error) {
	lock, err := tryLock(o.DataDir, 5)
	if err != nil {
		return 0, err
	}
	defer lock.Close()
	return runScan(ctx, o, lock)
}

// runScan performs one scan while the caller holds lock.
func runScan(ctx context.Context, o Options, lock *os.File) (int64, error) {
	if o.Discover == nil {
		return 0, errors.New("runner: Options.Discover is nil")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	lock.Truncate(0)
	lock.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0)

	cat, err := catalog.Open(o.DataDir)
	if err != nil {
		return 0, err
	}
	defer cat.Close()
	settings, _ := cat.Settings()
	var prevFiles int64
	if list, err := cat.List(); err == nil {
		for _, s := range list {
			if s.Status == "done" {
				prevFiles = s.Files
				break
			}
		}
	}
	started := o.Now()
	id, err := cat.Begin(o.Trigger, started)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (int64, error) {
		status := "failed"
		if errors.Is(err, context.Canceled) {
			status = "canceled"
		}
		cat.Fail(id, status, err.Error(), o.Now())
		os.Remove(progressPath(o.DataDir))
		return id, err
	}

	vols, skipped, err := o.Discover()
	if err != nil {
		return fail(err)
	}
	if len(vols) == 0 {
		return fail(errors.New("no data volumes found"))
	}
	final := catalog.ScanPath(o.DataDir, id)
	tmp := final + ".tmp"
	w, err := scandb.Create(tmp)
	if err != nil {
		return fail(err)
	}

	type observedDir struct {
		id       int64
		path     string
		children []scanner.DirChild
	}
	var obsMu sync.Mutex
	var observed []observedDir

	walker := &scanner.Walker{
		Workers:      o.Workers,
		IsMountPoint: o.IsMountPoint,
		WatchDir: func(dirPath string) bool {
			return docker.IsTargetName(filepath.Base(filepath.Clean(dirPath)))
		},
		OnDir: func(dirID int64, dirPath string, children []scanner.DirChild) {
			obsMu.Lock()
			observed = append(observed, observedDir{id: dirID, path: dirPath, children: children})
			obsMu.Unlock()
		},
	}
	prog := Progress{Job: "scan", ScanID: id, PID: os.Getpid(), Trigger: o.Trigger, StartedAt: started.Unix(), PrevFiles: prevFiles}
	if o.thenDupCheck {
		prog.Next = "dupcheck"
	}
	snapshot := func() Progress {
		p := prog
		p.Files = walker.Stats.Files.Load()
		p.Dirs = walker.Stats.Dirs.Load()
		p.Bytes = walker.Stats.Bytes.Load()
		p.Errors = walker.Stats.Errors.Load()
		if c, ok := walker.Stats.Current.Load().(string); ok {
			p.Current = strings.ToValidUTF8(c, "�")
		}
		return p
	}
	writeJSONAtomic(progressPath(o.DataDir), snapshot())
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				writeJSONAtomic(progressPath(o.DataDir), snapshot())
			}
		}
	}()

	roots := make([]scanner.Root, len(vols))
	for i, v := range vols {
		roots[i] = scanner.Root{Name: v.Name, Path: v.MountPoint}
	}
	sink := &rootSink{w: w, roots: map[int64]scandb.Entry{}}
	werr := walker.Walk(ctx, roots, sink)
	close(stop)
	wg.Wait()
	if werr != nil {
		w.Abort()
		return fail(werr)
	}

	if len(observed) > 0 {
		type targetEntry struct {
			id       int64
			path     string
			kind     string
			children []scanner.DirChild
		}
		byRoot := map[string][]targetEntry{}
		for _, obs := range observed {
			rootPath, kind, ok := docker.FindRootForTarget(obs.path)
			if !ok {
				continue
			}
			byRoot[rootPath] = append(byRoot[rootPath], targetEntry{
				id: obs.id, path: obs.path, kind: kind, children: obs.children,
			})
		}
		tmpDir := filepath.Join(o.DataDir, "run")
		for rootPath, targets := range byRoot {
			res, err := docker.ResolveRoot(rootPath, tmpDir)
			if err != nil {
				log.Printf("docker resolve %s: %v", rootPath, err)
				continue
			}
			for _, tgt := range targets {
				if ws := res.Warnings(tgt.kind); len(ws) > 0 {
					log.Printf("docker metadata partial for %s (%s): %v", tgt.path, tgt.kind, ws)
				}
				if da := res.DirAnnotation(tgt.kind); da != nil {
					if b, err := json.Marshal(da); err == nil {
						_ = w.AddAnnotation(tgt.id, "docker", string(b))
					}
				}
				for _, ch := range tgt.children {
					if ann := res.EntryAnnotation(tgt.kind, ch.Name, ch.Kind == scandb.KindDir); ann != nil {
						if b, err := json.Marshal(ann); err == nil {
							_ = w.AddAnnotation(ch.ID, "docker", string(b))
						}
					}
				}
			}
		}
	}

	finished := o.Now()
	vm := StatVolumes(vols)
	var totalD, totalA int64
	for i := range vm {
		vm[i].EntryID = int64(i + 1)
		r := sink.roots[int64(i+1)]
		totalD += r.DSize
		totalA += r.ASize
	}
	vj, _ := json.Marshal(vm)
	if skipped == nil {
		skipped = []volumes.Skipped{}
	}
	sj, _ := json.Marshal(skipped)
	w.SetMeta(scandb.MetaRoots, strconv.Itoa(len(vols)))
	w.SetMeta(MetaVolumes, string(vj))
	w.SetMeta(MetaSkipped, string(sj))
	w.SetMeta(MetaStartedAt, strconv.FormatInt(started.Unix(), 10))
	w.SetMeta(MetaFinishedAt, strconv.FormatInt(finished.Unix(), 10))
	w.SetMeta(MetaTrigger, o.Trigger)
	if err := w.Close(); err != nil {
		return fail(err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fail(err)
	}
	var dbBytes int64
	if st, err := os.Stat(final); err == nil {
		dbBytes = st.Size()
	}
	err = cat.Finish(catalog.Scan{
		ID: id, FinishedAt: finished.Unix(), TotalDSize: totalD, TotalASize: totalA,
		Files: walker.Stats.Files.Load(), Dirs: walker.Stats.Dirs.Load(), Errors: walker.Stats.Errors.Load(),
		DurationS: int64(finished.Sub(started).Seconds()), DBBytes: dbBytes,
	})
	if err != nil {
		return fail(err)
	}
	os.Remove(progressPath(o.DataDir))
	if _, err := cat.Prune(settings.Retention); err != nil {
		return id, err
	}
	return id, nil
}

// Running reports whether a scan holds the lock, with its latest progress.
func Running(dataDir string) (Progress, bool, error) {
	f, err := os.OpenFile(lockPath(dataDir), os.O_RDWR, 0)
	if os.IsNotExist(err) {
		return Progress{}, false, nil
	}
	if err != nil {
		return Progress{}, false, err
	}
	defer f.Close()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return Progress{}, false, nil
	}
	if !errors.Is(err, syscall.EWOULDBLOCK) {
		return Progress{}, false, err
	}
	var p Progress
	if b, err := os.ReadFile(progressPath(dataDir)); err == nil {
		json.Unmarshal(b, &p)
	}
	if p.PID == 0 {
		b, _ := os.ReadFile(lockPath(dataDir))
		p.PID, _ = strconv.Atoi(strings.TrimSpace(string(b)))
	}
	return p, true, nil
}

// Cancel sends SIGTERM to the running scan.
func Cancel(dataDir string) error {
	p, running, err := Running(dataDir)
	if err != nil {
		return err
	}
	if !running || p.PID <= 0 {
		return ErrNotRunning
	}
	return syscall.Kill(p.PID, syscall.SIGTERM)
}

// CleanupStaleTmp removes leftover scans/*.tmp files when no scan is running.
func CleanupStaleTmp(dataDir string) error {
	lock, err := tryLock(dataDir, 1)
	if err != nil {
		if errors.Is(err, ErrBusy) {
			return nil
		}
		return err
	}
	defer lock.Close()
	matches, _ := filepath.Glob(filepath.Join(dataDir, "scans", "*.tmp"))
	more, _ := filepath.Glob(filepath.Join(dataDir, "scans", "*.compact"))
	reqs, _ := filepath.Glob(filepath.Join(dataDir, "run", "delete-req-*.json"))
	all := append(append(matches, more...), reqs...)
	for _, m := range all {
		if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	os.Remove(progressPath(dataDir))
	return nil
}
