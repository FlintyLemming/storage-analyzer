package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"storage-analyzer/internal/catalog"
	"storage-analyzer/internal/dupcheck"
	"storage-analyzer/internal/scandb"
)

// ErrScanNotFound means the scan to check does not exist or is not done.
var ErrScanNotFound = errors.New("scan not found")

// DupOptions configure RunDupCheck.
type DupOptions struct {
	DataDir string
	ScanID  int64
	Now     func() time.Time
	FS      dupcheck.FS // default: the real filesystem
}

// CachePath is the duplicate-check digest cache.
func CachePath(dataDir string) string { return filepath.Join(dataDir, "dupcache.db") }

// RunDupCheck checks one finished scan for duplicate files and folders. It
// takes the same lock as Run, so scans and checks never overlap.
func RunDupCheck(ctx context.Context, o DupOptions) (dupcheck.Summary, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	lock, err := tryLock(o.DataDir, 5)
	if err != nil {
		return dupcheck.Summary{}, err
	}
	defer lock.Close()
	lock.Truncate(0)
	lock.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0)

	cat, err := catalog.Open(o.DataDir)
	if err != nil {
		return dupcheck.Summary{}, err
	}
	defer cat.Close()
	scanPath := catalog.ScanPath(o.DataDir, o.ScanID)
	if s, err := cat.Get(o.ScanID); err != nil || s.Status != "done" {
		return dupcheck.Summary{}, ErrScanNotFound
	}
	if _, err := os.Stat(scanPath); err != nil {
		return dupcheck.Summary{}, ErrScanNotFound
	}
	started := o.Now()
	if err := cat.BeginDupCheck(o.ScanID, started); err != nil {
		return dupcheck.Summary{}, err
	}
	fail := func(err error) (dupcheck.Summary, error) {
		status := "failed"
		if errors.Is(err, context.Canceled) {
			status = "canceled"
		}
		cat.FailDupCheck(o.ScanID, status, err.Error(), o.Now())
		os.Remove(progressPath(o.DataDir))
		return dupcheck.Summary{}, err
	}

	roots, err := volumeRoots(scanPath)
	if err != nil {
		return fail(err)
	}
	cache, err := dupcheck.OpenCache(CachePath(o.DataDir))
	if err != nil {
		return fail(err)
	}
	defer cache.Close()

	dp := &dupcheck.Progress{}
	base := Progress{Job: "dupcheck", ScanID: o.ScanID, PID: os.Getpid(), Trigger: "manual", StartedAt: started.Unix()}
	snapshot := func() Progress {
		p := base
		p.Phase = dp.PhaseName()
		p.DoneFiles, p.TotalFiles = dp.DoneFiles.Load(), dp.TotalFiles.Load()
		p.DoneBytes, p.TotalBytes = dp.DoneBytes.Load(), dp.TotalBytes.Load()
		p.CacheHits, p.Changed, p.Errors = dp.CacheHits.Load(), dp.Changed.Load(), dp.Errors.Load()
		p.Bytes = dp.BytesRead.Load()
		p.Current = strings.ToValidUTF8(dp.CurrentPath(), "�")
		return p
	}
	dp.Phase.Store("prepare")
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
	sum, err := dupcheck.Run(ctx, dupcheck.Options{
		ScanDB: scanPath, Roots: roots, Cache: cache, Out: catalog.DupsPath(o.DataDir, o.ScanID), Progress: dp, FS: o.FS,
	})
	close(stop)
	wg.Wait()
	if err != nil {
		return fail(err)
	}
	finished := o.Now()
	err = cat.FinishDupCheck(catalog.DupCheck{
		ScanID: o.ScanID, FinishedAt: finished.Unix(), DurationS: int64(finished.Sub(started).Seconds()),
		Groups: sum.Groups, ReclaimDSize: sum.ReclaimDSize, ReclaimASize: sum.ReclaimASize,
		BytesRead: sum.BytesRead, CacheHits: sum.CacheHits, Changed: sum.Changed, Errors: sum.Errors,
	})
	if err != nil {
		return fail(err)
	}
	os.Remove(progressPath(o.DataDir))
	return sum, nil
}

// volumeRoots maps volume root entries of a scan to their mount points.
func volumeRoots(scanPath string) (map[int64]dupcheck.Root, error) {
	db, err := scandb.Open(scanPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	meta, err := db.Meta()
	if err != nil {
		return nil, err
	}
	var vols []VolumeMeta
	if err := json.Unmarshal([]byte(meta[MetaVolumes]), &vols); err != nil {
		return nil, fmt.Errorf("scan volumes: %w", err)
	}
	roots := map[int64]dupcheck.Root{}
	for _, v := range vols {
		roots[v.EntryID] = dupcheck.Root{Name: v.Name, MountPoint: v.MountPoint}
	}
	return roots, nil
}
