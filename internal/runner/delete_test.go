package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/deleter"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

func TestRunDeleteEndToEndSyncsAllDatabases(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	volDir := filepath.Join(root, "vol")
	if err := os.MkdirAll(filepath.Join(volDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = byte(i%251 + 1)
	}
	// Two identical files + one folder to delete
	if err := os.WriteFile(filepath.Join(volDir, "dup1.bin"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volDir, "dup2.bin"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volDir, "sub", "junk.dat"), payload, 0o644); err != nil {
		t.Fatal(err)
	}

	discover := func() ([]volumes.Volume, []volumes.Skipped, error) {
		return []volumes.Volume{{Name: volDir, MountPoint: volDir, Device: "/dev/test", FSType: "ext4"}}, nil, nil
	}
	scanID, err := Run(context.Background(), Options{DataDir: dataDir, Trigger: "manual", Discover: discover})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunDupCheck(context.Background(), DupOptions{DataDir: dataDir, ScanID: scanID}); err != nil {
		t.Fatal(err)
	}

	// Find entry IDs for dup1.bin and sub/
	db, err := scandb.Open(catalog.ScanPath(dataDir, scanID))
	if err != nil {
		t.Fatal(err)
	}
	children, _, err := db.Children(1, scandb.ListOpts{Sort: "name", Desc: false, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var dup1ID, subID int64
	for _, ch := range children {
		switch string(ch.Name) {
		case "dup1.bin":
			dup1ID = ch.ID
		case "sub":
			subID = ch.ID
		}
	}
	db.Close()
	if dup1ID == 0 || subID == 0 {
		t.Fatalf("missing ids: dup1=%d sub=%d", dup1ID, subID)
	}

	pol := deleter.DefaultPolicy("linux", dataDir, "")
	res, err := RunDelete(context.Background(), DeleteOptions{
		DataDir: dataDir,
		Request: DeleteRequest{JobID: "job-1", ScanID: scanID, EntryIDs: []int64{dup1ID, subID}},
		Policy:  pol,
		Now:     func() time.Time { return time.Unix(1700000200, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "done" || res.Succeeded != 2 || len(res.DeletedIDs) != 2 || len(res.Failures) != 0 {
		t.Fatalf("DeleteResult = %+v", res)
	}
	if _, err := os.Stat(filepath.Join(volDir, "dup1.bin")); !os.IsNotExist(err) {
		t.Fatalf("dup1.bin still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(volDir, "sub")); !os.IsNotExist(err) {
		t.Fatalf("sub still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(volDir, "dup2.bin")); err != nil {
		t.Fatalf("dup2.bin should remain: %v", err)
	}

	// Verify LastDelete persisted.
	last, err := LastDelete(dataDir)
	if err != nil || last == nil || last.ID != "job-1" || last.Status != "done" {
		t.Fatalf("LastDelete = %+v err=%v", last, err)
	}

	// Verify scandb updated.
	db, err = scandb.Open(catalog.ScanPath(dataDir, scanID))
	if err != nil {
		t.Fatal(err)
	}
	rem, total, err := db.Children(1, scandb.ListOpts{Sort: "name", Limit: 100})
	db.Close()
	if err != nil || total != 1 || len(rem) != 1 || string(rem[0].Name) != "dup2.bin" {
		t.Fatalf("remaining entries = %+v total=%d err=%v", rem, total, err)
	}

	// Verify dups db pruned (group reduced from 2 copies to 1 copy -> 0 groups).
	dr, err := dupcheck.OpenResult(catalog.DupsPath(dataDir, scanID))
	if err != nil {
		t.Fatal(err)
	}
	groups, err := dr.Total()
	dr.Close()
	if err != nil || groups != 0 {
		t.Fatalf("remaining dup groups = %d err=%v", groups, err)
	}

	// Verify catalog updated.
	cat, err := catalog.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	sc, err := cat.Get(scanID)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Files != 1 {
		t.Fatalf("catalog scan files = %d, want 1", sc.Files)
	}
	dc, err := cat.GetDupCheck(scanID)
	if err != nil || dc.Groups != 0 || dc.ReclaimDSize != 0 {
		t.Fatalf("catalog dupcheck = %+v err=%v", dc, err)
	}
}

func TestRunDeleteRejectsOldScanAndBusy(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	volDir := filepath.Join(root, "vol")
	os.MkdirAll(volDir, 0o755)
	os.WriteFile(filepath.Join(volDir, "a.txt"), []byte("hello"), 0o644)

	discover := func() ([]volumes.Volume, []volumes.Skipped, error) {
		return []volumes.Volume{{Name: volDir, MountPoint: volDir}}, nil, nil
	}
	id1, err := Run(context.Background(), Options{DataDir: dataDir, Trigger: "manual", Discover: discover})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Run(context.Background(), Options{DataDir: dataDir, Trigger: "manual", Discover: discover})
	if err != nil {
		t.Fatal(err)
	}

	pol := deleter.DefaultPolicy("linux", dataDir, "")
	res, err := RunDelete(context.Background(), DeleteOptions{
		DataDir: dataDir,
		Request: DeleteRequest{JobID: "job-old", ScanID: id1, EntryIDs: []int64{2}},
		Policy:  pol,
	})
	if err == nil || res.Status != "failed" {
		t.Fatalf("old scan delete: res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(volDir, "a.txt")); err != nil {
		t.Fatalf("file deleted on rejected old scan: %v", err)
	}
}

func TestRunDeleteRequestRoundTripAndCleanup(t *testing.T) {
	dataDir := t.TempDir()
	reqPath, err := WriteDeleteRequest(dataDir, DeleteRequest{JobID: "abc", ScanID: 7, EntryIDs: []int64{3, 4}, ConfirmDocker: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadDeleteRequest(reqPath)
	if err != nil || got.JobID != "abc" || got.ScanID != 7 || len(got.EntryIDs) != 2 || !got.ConfirmDocker {
		t.Fatalf("ReadDeleteRequest = %+v err=%v", got, err)
	}
	if err := CleanupStaleTmp(dataDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reqPath); !os.IsNotExist(err) {
		t.Fatalf("stale delete request not cleaned: %v", err)
	}
}
