package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/deleter"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

// DeleteRequest is written to run/delete-req-<jobId>.json by the API and read
// by the detached `storage-analyzer delete --request <path>` worker.
type DeleteRequest struct {
	JobID         string  `json:"jobId"`
	ScanID        int64   `json:"scanId"`
	EntryIDs      []int64 `json:"entryIds"`
	ConfirmDocker bool    `json:"confirmDocker,omitempty"`
}

// DeleteError records one path that failed during a delete job.
type DeleteError struct {
	EntryID int64  `json:"entryId,omitempty"`
	Path    string `json:"path"`
	Error   string `json:"error"`
}

// DeleteResult is written atomically to run/delete-last.json when a delete job ends.
type DeleteResult struct {
	ID           string        `json:"id"`
	ScanID       int64         `json:"scanId"`
	Status       string        `json:"status"` // done | partial | canceled | failed
	StartedAt    int64         `json:"startedAt"`
	FinishedAt   int64         `json:"finishedAt"`
	Targets      int           `json:"targets"`
	Succeeded    int           `json:"succeeded"`
	DeletedItems int64         `json:"deletedItems"`
	FreedDSize   int64         `json:"freedDSize"`
	FreedASize   int64         `json:"freedASize"`
	DeletedIDs   []int64       `json:"deletedIds"`
	Failures     []DeleteError `json:"failures,omitempty"`
	ErrorMessage string        `json:"errorMessage,omitempty"`
}

// DeleteOptions configures RunDelete.
type DeleteOptions struct {
	DataDir      string
	Request      DeleteRequest
	Policy       deleter.Policy
	IsMountPoint func(path string, parentDev, dev uint64) bool
	Now          func() time.Time
}

const maxDeleteFailures = 50

// LastDeletePath returns the path to the last completed delete result JSON.
func LastDeletePath(dataDir string) string {
	return filepath.Join(dataDir, "run", "delete-last.json")
}

// LastDelete reads the most recent delete result, returning (nil, nil) if none exists.
func LastDelete(dataDir string) (*DeleteResult, error) {
	b, err := os.ReadFile(LastDeletePath(dataDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var res DeleteResult
	if err := json.Unmarshal(b, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// WriteDeleteRequest writes req to a 0600 file inside dataDir/run and returns its path.
func WriteDeleteRequest(dataDir string, req DeleteRequest) (string, error) {
	runDir := filepath.Join(dataDir, "run")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", err
	}
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	safeID := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, req.JobID)
	if safeID == "" {
		safeID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	path := filepath.Join(runDir, "delete-req-"+safeID+".json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return path, nil
}

// ReadDeleteRequest reads a DeleteRequest file written by WriteDeleteRequest.
func ReadDeleteRequest(path string) (DeleteRequest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return DeleteRequest{}, err
	}
	var req DeleteRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return DeleteRequest{}, err
	}
	return req, nil
}

// LoadScanVolumes reads MetaVolumes from an open scan database and returns both
// the slice of VolumeMeta and a lookup map for deleter.ResolveTargets.
func LoadScanVolumes(db *scandb.DB) ([]VolumeMeta, map[int64]deleter.VolumeRoot, error) {
	meta, err := db.Meta()
	if err != nil {
		return nil, nil, err
	}
	var vols []VolumeMeta
	if raw := meta[MetaVolumes]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &vols); err != nil {
			return nil, nil, fmt.Errorf("scan volumes: %w", err)
		}
	}
	roots := make(map[int64]deleter.VolumeRoot, len(vols))
	for _, v := range vols {
		roots[v.EntryID] = deleter.VolumeRoot{EntryID: v.EntryID, Name: v.Name, MountPoint: v.MountPoint}
	}
	return vols, roots, nil
}

func refreshVolumeStatfs(vols []VolumeMeta) string {
	if len(vols) == 0 {
		return ""
	}
	out := make([]VolumeMeta, len(vols))
	copy(out, vols)
	for i := range out {
		var st syscall.Statfs_t
		if out[i].MountPoint != "" && syscall.Statfs(out[i].MountPoint, &st) == nil {
			bs := uint64(st.Bsize)
			out[i].FSTotal = uint64(st.Blocks) * bs
			out[i].FSUsed = (uint64(st.Blocks) - uint64(st.Bfree)) * bs
			out[i].FSFree = uint64(st.Bavail) * bs
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(b)
}

func latestDoneScanID(c *catalog.Catalog) (int64, bool, error) {
	list, err := c.List()
	if err != nil {
		return 0, false, err
	}
	for _, s := range list {
		if s.Status == "done" {
			return s.ID, true, nil
		}
	}
	return 0, false, nil
}

// RunDelete deletes the targets described in o.Request under the shared scan
// lock, writes run/delete-last.json, and synchronizes scandb, dups, and catalog.
func RunDelete(ctx context.Context, o DeleteOptions) (DeleteResult, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	lock, err := tryLock(o.DataDir, 5)
	if err != nil {
		return DeleteResult{}, err
	}
	defer lock.Close()

	lock.Truncate(0)
	lock.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0)

	started := o.Now()
	res := DeleteResult{
		ID:         o.Request.JobID,
		ScanID:     o.Request.ScanID,
		StartedAt:  started.Unix(),
		DeletedIDs: []int64{},
	}

	finishFail := func(err error) (DeleteResult, error) {
		res.Status = "failed"
		res.FinishedAt = o.Now().Unix()
		res.ErrorMessage = err.Error()
		_ = writeJSONAtomic(LastDeletePath(o.DataDir), res)
		os.Remove(progressPath(o.DataDir))
		return res, err
	}

	cat, err := catalog.Open(o.DataDir)
	if err != nil {
		return finishFail(err)
	}
	defer cat.Close()

	latestID, ok, err := latestDoneScanID(cat)
	if err != nil {
		return finishFail(err)
	}
	if !ok || latestID != o.Request.ScanID {
		return finishFail(errors.New("deletion is only allowed on the latest completed scan"))
	}

	scanPath := catalog.ScanPath(o.DataDir, o.Request.ScanID)
	db, err := scandb.Open(scanPath)
	if err != nil {
		return finishFail(err)
	}
	vols, roots, err := LoadScanVolumes(db)
	if err != nil {
		db.Close()
		return finishFail(err)
	}
	resolved, err := deleter.ResolveTargets(db, roots, o.Policy, o.Request.EntryIDs)
	db.Close()
	if err != nil {
		return finishFail(err)
	}
	if resolved.HasProtected {
		return finishFail(errors.New("request contains protected paths"))
	}
	if resolved.HasDockerRisk && !o.Request.ConfirmDocker {
		return finishFail(errors.New("docker directory deletion requires explicit confirmation"))
	}
	if len(resolved.Targets) == 0 {
		res.Status = "done"
		res.FinishedAt = o.Now().Unix()
		_ = writeJSONAtomic(LastDeletePath(o.DataDir), res)
		os.Remove(progressPath(o.DataDir))
		return res, nil
	}

	res.Targets = len(resolved.Targets)
	var doneTargets atomic.Int64
	var deletedItems atomic.Int64
	var freedBytes atomic.Int64
	var failedCount atomic.Int64
	var currentPath atomic.Value

	base := Progress{
		Job:       "delete",
		JobID:     o.Request.JobID,
		ScanID:    o.Request.ScanID,
		PID:       os.Getpid(),
		StartedAt: started.Unix(),
		Targets:   len(resolved.Targets),
	}
	snapshot := func() Progress {
		p := base
		p.DoneTargets = int(doneTargets.Load())
		p.DeletedItems = deletedItems.Load()
		p.FreedBytes = freedBytes.Load()
		p.Failed = int(failedCount.Load())
		if c, ok := currentPath.Load().(string); ok {
			p.Current = strings.ToValidUTF8(c, "\uFFFD")
		}
		return p
	}
	_ = writeJSONAtomic(progressPath(o.DataDir), snapshot())

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				_ = writeJSONAtomic(progressPath(o.DataDir), snapshot())
			}
		}
	}()

	var (
		deletedIDs          []int64
		deletedDisplayPaths [][]byte
		deletedFiles        int64
		deletedDirs         int64
		totalFreedD         int64
		totalFreedA         int64
		failures            []DeleteError
		canceled            bool
	)

	for _, t := range resolved.Targets {
		if ctx.Err() != nil {
			canceled = true
			break
		}
		currentPath.Store(t.Path)
		st := deleter.RemoveTarget(ctx, t, deleter.RemoveCallbacks{
			IsMountPoint: o.IsMountPoint,
			OnProgress: func(path string, fDelta, dDelta, dBytes, _ int64) {
				deletedItems.Add(fDelta + dDelta)
				freedBytes.Add(dBytes)
				currentPath.Store(path)
			},
			OnError: func(path string, err error) {
				if errors.Is(err, context.Canceled) {
					canceled = true
					return
				}
				failedCount.Add(1)
				if len(failures) < maxDeleteFailures {
					failures = append(failures, DeleteError{EntryID: t.EntryID, Path: path, Error: err.Error()})
				}
			},
		})
		deletedFiles += st.Files
		deletedDirs += st.Dirs
		totalFreedD += st.FreedDSize
		totalFreedA += st.FreedASize
		if st.Complete {
			deletedIDs = append(deletedIDs, t.EntryID)
			deletedDisplayPaths = append(deletedDisplayPaths, t.RawDisplayPath)
			res.Succeeded++
		}
		doneTargets.Add(1)
		if ctx.Err() != nil {
			canceled = true
			break
		}
	}

	close(stop)
	wg.Wait()

	// Synchronize scandb, dups, and catalog for every fully deleted top-level target.
	if len(deletedIDs) > 0 {
		volsJSON := refreshVolumeStatfs(vols)
		tot, err := scandb.MarkDeleted(scanPath, deletedIDs, volsJSON)
		if err != nil {
			log.Printf("delete sync scandb %d: %v", o.Request.ScanID, err)
			if len(failures) < maxDeleteFailures {
				failures = append(failures, DeleteError{Path: scanPath, Error: err.Error()})
			}
		} else {
			if err := cat.AdjustScanTotals(o.Request.ScanID, tot.DSize, tot.ASize, deletedFiles, deletedDirs); err != nil {
				log.Printf("delete sync catalog %d: %v", o.Request.ScanID, err)
			}
		}
		dupsPath := catalog.DupsPath(o.DataDir, o.Request.ScanID)
		if _, statErr := os.Stat(dupsPath); statErr == nil {
			ps, err := dupcheck.PruneDeleted(dupsPath, deletedIDs, deletedDisplayPaths)
			if err != nil {
				log.Printf("delete sync dups %d: %v", o.Request.ScanID, err)
			} else if ps.Changed {
				if err := cat.UpdateDupSummary(o.Request.ScanID, ps.Groups, ps.ReclaimDSize, ps.ReclaimASize); err != nil {
					log.Printf("delete sync catalog dups %d: %v", o.Request.ScanID, err)
				}
			}
		}
	}

	res.FinishedAt = o.Now().Unix()
	res.DeletedItems = deletedFiles + deletedDirs
	res.FreedDSize = totalFreedD
	res.FreedASize = totalFreedA
	if deletedIDs != nil {
		res.DeletedIDs = deletedIDs
	}
	res.Failures = failures

	switch {
	case canceled:
		res.Status = "canceled"
	case len(failures) > 0 || res.Succeeded < res.Targets:
		if res.Succeeded == 0 && res.DeletedItems == 0 {
			res.Status = "failed"
		} else {
			res.Status = "partial"
		}
	default:
		res.Status = "done"
	}

	_ = writeJSONAtomic(LastDeletePath(o.DataDir), res)
	os.Remove(progressPath(o.DataDir))
	return res, nil
}
