package runner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// dupScan runs a scan over one volume holding two identical files.
func dupScan(t *testing.T, data string) int64 {
	t.Helper()
	v := t.TempDir()
	os.WriteFile(filepath.Join(v, "a.bin"), make([]byte, 5000), 0o644)
	os.WriteFile(filepath.Join(v, "copy.bin"), make([]byte, 5000), 0o644)
	id, err := Run(context.Background(), Options{DataDir: data, Trigger: "manual", Now: time.Now,
		Discover: func() ([]volumes.Volume, []volumes.Skipped, error) {
			return []volumes.Volume{{Name: "DataVol1", Pool: "1", Device: "/dev/x", MountPoint: v}}, nil, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestRunDupCheckDone(t *testing.T) {
	data := t.TempDir()
	id := dupScan(t, data)
	sum, err := RunDupCheck(context.Background(), DupOptions{DataDir: data, ScanID: id})
	if err != nil || sum.Groups != 1 {
		t.Fatalf("sum=%+v err=%v", sum, err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	d, err := c.GetDupCheck(id)
	if err != nil || d.Status != "done" || d.Groups != 1 || d.ReclaimASize != 5000 {
		t.Fatalf("dupcheck = %+v %v", d, err)
	}
	if _, err := os.Stat(catalog.DupsPath(data, id)); err != nil {
		t.Fatalf("result file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(data, "run", "progress.json")); !os.IsNotExist(err) {
		t.Fatalf("progress.json left behind: %v", err)
	}
	if _, err := os.Stat(filepath.Join(data, "dupcache.db")); err != nil {
		t.Fatalf("cache: %v", err)
	}
}

func TestRunDupCheckBusy(t *testing.T) {
	data := t.TempDir()
	id := dupScan(t, data)
	holdLock(t, data)
	if _, err := RunDupCheck(context.Background(), DupOptions{DataDir: data, ScanID: id}); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v", err)
	}
}

func TestRunDupCheckMissingScan(t *testing.T) {
	data := t.TempDir()
	if _, err := RunDupCheck(context.Background(), DupOptions{DataDir: data, ScanID: 999}); !errors.Is(err, ErrScanNotFound) {
		t.Fatalf("err = %v", err)
	}
}

// peekFS reads progress.json the first time the check touches a file.
type peekFS struct {
	dupcheck.FS
	data string
	once sync.Once
	got  Progress
}

func (p *peekFS) Lstat(path string) (dupcheck.FileInfo, error) {
	p.once.Do(func() {
		b, _ := os.ReadFile(filepath.Join(p.data, "run", "progress.json"))
		json.Unmarshal(b, &p.got)
	})
	return p.FS.Lstat(path)
}

func TestRunDupCheckProgressJob(t *testing.T) {
	data := t.TempDir()
	id := dupScan(t, data)
	fs := &peekFS{FS: dupcheck.OSFS(), data: data}
	if _, err := RunDupCheck(context.Background(), DupOptions{DataDir: data, ScanID: id, FS: fs}); err != nil {
		t.Fatal(err)
	}
	if fs.got.Job != "dupcheck" || fs.got.ScanID != id || fs.got.PID != os.Getpid() {
		t.Fatalf("progress = %+v", fs.got)
	}
}

func TestRunDupCheckCanceled(t *testing.T) {
	data := t.TempDir()
	id := dupScan(t, data)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunDupCheck(ctx, DupOptions{DataDir: data, ScanID: id}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	if d, _ := c.GetDupCheck(id); d.Status != "canceled" {
		t.Fatalf("status = %q", d.Status)
	}
}
