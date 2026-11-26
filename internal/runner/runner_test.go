package runner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

func fakeVolumes(t *testing.T) func() ([]volumes.Volume, []volumes.Skipped, error) {
	t.Helper()
	v1, v2 := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(v1, "a.bin"), make([]byte, 5000), 0o644)
	os.MkdirAll(filepath.Join(v2, "dir"), 0o755)
	os.WriteFile(filepath.Join(v2, "dir", "b.bin"), make([]byte, 100), 0o644)
	return func() ([]volumes.Volume, []volumes.Skipped, error) {
		return []volumes.Volume{
				{Name: "DataVol1", Pool: "1", Device: "/dev/x1", MountPoint: v1},
				{Name: "DataVol2", Pool: "2", Device: "/dev/x2", MountPoint: v2},
			},
			[]volumes.Skipped{{Name: "Locked", Reason: "not_mounted"}}, nil
	}
}

func opts(t *testing.T, dataDir string) Options {
	return Options{DataDir: dataDir, Trigger: "manual", Discover: fakeVolumes(t), Now: time.Now}
}

func holdLock(t *testing.T, dataDir string) {
	t.Helper()
	os.MkdirAll(filepath.Join(dataDir, "run"), 0o755)
	f, err := os.OpenFile(filepath.Join(dataDir, "run", "scan.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
}

func TestRunProducesDoneScan(t *testing.T) {
	data := t.TempDir()
	id, err := Run(context.Background(), opts(t, data))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	s, err := c.Get(id)
	if err != nil || s.Status != "done" || s.Trigger != "manual" {
		t.Fatalf("scan = %+v err=%v", s, err)
	}
	if s.Files != 2 || s.TotalASize < 5100 || s.DBBytes == 0 {
		t.Fatalf("totals wrong: %+v", s)
	}
	db, err := scandb.Open(catalog.ScanPath(data, id))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots, total, _ := db.Children(0, scandb.ListOpts{Sort: "name"})
	if total != 2 || string(roots[0].Name) != "DataVol1" || string(roots[1].Name) != "DataVol2" {
		t.Fatalf("roots = %+v", roots)
	}
	meta, _ := db.Meta()
	var vols []VolumeMeta
	if err := json.Unmarshal([]byte(meta[MetaVolumes]), &vols); err != nil || len(vols) != 2 {
		t.Fatalf("volumes meta %q: %v", meta[MetaVolumes], err)
	}
	if vols[0].EntryID != 1 || vols[0].Pool != "1" || vols[0].FSTotal == 0 {
		t.Fatalf("vols[0] = %+v", vols[0])
	}
	var skipped []volumes.Skipped
	if err := json.Unmarshal([]byte(meta[MetaSkipped]), &skipped); err != nil || len(skipped) != 1 {
		t.Fatalf("skipped meta %q: %v", meta[MetaSkipped], err)
	}
	if _, err := os.Stat(filepath.Join(data, "run", "progress.json")); !os.IsNotExist(err) {
		t.Fatal("progress.json left behind")
	}
}

func TestSecondScanSkipped(t *testing.T) {
	data := t.TempDir()
	holdLock(t, data)
	if _, err := Run(context.Background(), opts(t, data)); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	if list, _ := c.List(); len(list) != 0 {
		t.Fatalf("catalog has %d rows", len(list))
	}
}

func TestRunCanceled(t *testing.T) {
	data := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	id, err := Run(ctx, opts(t, data))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	if s, _ := c.Get(id); s.Status != "canceled" {
		t.Fatalf("status = %q", s.Status)
	}
	matches, _ := filepath.Glob(filepath.Join(data, "scans", "*"))
	if len(matches) != 0 {
		t.Fatalf("leftover files: %v", matches)
	}
}

func TestCleanupStaleTmp(t *testing.T) {
	data := t.TempDir()
	os.MkdirAll(filepath.Join(data, "scans"), 0o755)
	tmp := filepath.Join(data, "scans", "scan-9.db.tmp")
	os.WriteFile(tmp, []byte("x"), 0o644)
	if err := CleanupStaleTmp(data); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("tmp file not removed")
	}
}

func TestCleanupStaleTmpKeepsRunningScan(t *testing.T) {
	data := t.TempDir()
	os.MkdirAll(filepath.Join(data, "scans"), 0o755)
	tmp := filepath.Join(data, "scans", "scan-9.db.tmp")
	os.WriteFile(tmp, []byte("x"), 0o644)
	holdLock(t, data)
	CleanupStaleTmp(data)
	if _, err := os.Stat(tmp); err != nil {
		t.Fatal("tmp of a running scan must not be removed")
	}
}

func TestRunPrunes(t *testing.T) {
	data := t.TempDir()
	c, _ := catalog.Open(data)
	s, _ := c.Settings()
	s.Retention = 2
	if err := c.SaveSettings(s); err != nil {
		t.Fatal(err)
	}
	c.Close()
	o := opts(t, data)
	for i := 0; i < 3; i++ {
		if _, err := Run(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
	dbs, _ := filepath.Glob(filepath.Join(data, "scans", "*.db"))
	if len(dbs) != 2 {
		t.Fatalf("db files = %v", dbs)
	}
}

func TestRunningIdle(t *testing.T) {
	_, running, err := Running(t.TempDir())
	if err != nil || running {
		t.Fatalf("running=%v err=%v", running, err)
	}
}

func TestRunningReadsProgress(t *testing.T) {
	data := t.TempDir()
	holdLock(t, data)
	p := Progress{ScanID: 7, PID: 123, Trigger: "manual", Files: 42}
	b, _ := json.Marshal(p)
	os.WriteFile(filepath.Join(data, "run", "progress.json"), b, 0o644)
	got, running, err := Running(data)
	if err != nil || !running || got.ScanID != 7 || got.Files != 42 {
		t.Fatalf("got=%+v running=%v err=%v", got, running, err)
	}
}
