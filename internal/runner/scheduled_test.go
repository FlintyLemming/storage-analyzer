package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/dupcheck"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

// dupVolume is one volume holding two identical files.
func dupVolume(t *testing.T) func() ([]volumes.Volume, []volumes.Skipped, error) {
	t.Helper()
	v := t.TempDir()
	os.WriteFile(filepath.Join(v, "a.bin"), make([]byte, 5000), 0o644)
	os.WriteFile(filepath.Join(v, "copy.bin"), make([]byte, 5000), 0o644)
	return func() ([]volumes.Volume, []volumes.Skipped, error) {
		return []volumes.Volume{{Name: "DataVol1", Pool: "1", Device: "/dev/x", MountPoint: v}}, nil, nil
	}
}

func setDupSchedule(t *testing.T, data string, on bool) {
	t.Helper()
	c, err := catalog.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := catalog.DefaultSettings
	s.ScheduleMode = "daily"
	s.ScheduleDupCheck = on
	if err := c.SaveSettings(s); err != nil {
		t.Fatal(err)
	}
}

// noDupCheck fails the test if scan id has a duplicate check record.
func noDupCheck(t *testing.T, data string, id int64) {
	t.Helper()
	c, _ := catalog.Open(data)
	defer c.Close()
	if d, err := c.GetDupCheck(id); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("unexpected duplicate check %+v err=%v", d, err)
	}
}

func TestRunScheduledWithoutDupCheck(t *testing.T) {
	data := t.TempDir()
	res, err := RunScheduled(context.Background(), Options{DataDir: data, Discover: dupVolume(t), Now: time.Now})
	if err != nil || res.Checked {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	c, _ := catalog.Open(data)
	s, err := c.Get(res.ScanID)
	c.Close()
	if err != nil || s.Status != "done" || s.Trigger != "schedule" {
		t.Fatalf("scan = %+v err=%v", s, err)
	}
	noDupCheck(t, data, res.ScanID)
}

func TestRunScheduledWithDupCheck(t *testing.T) {
	data := t.TempDir()
	setDupSchedule(t, data, true)
	fs := &peekFS{FS: dupcheck.OSFS(), data: data}
	res, err := RunScheduled(context.Background(), Options{DataDir: data, Discover: dupVolume(t), Now: time.Now, FS: fs})
	if err != nil || !res.Checked || res.DupErr != nil || res.Dup.Groups != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if res.ScanDone.IsZero() {
		t.Fatal("ScanDone not set")
	}
	c, _ := catalog.Open(data)
	d, err := c.GetDupCheck(res.ScanID)
	c.Close()
	if err != nil || d.Status != "done" || d.Groups != 1 {
		t.Fatalf("dupcheck = %+v err=%v", d, err)
	}
	if fs.got.Job != "dupcheck" || fs.got.Trigger != "schedule" || fs.got.ScanID != res.ScanID {
		t.Fatalf("progress during check = %+v", fs.got)
	}
	if _, err := os.Stat(filepath.Join(data, "run", "progress.json")); !os.IsNotExist(err) {
		t.Fatalf("progress.json left behind: %v", err)
	}
}

func TestRunScheduledBusy(t *testing.T) {
	data := t.TempDir()
	setDupSchedule(t, data, true)
	holdLock(t, data)
	if _, err := RunScheduled(context.Background(), Options{DataDir: data, Discover: dupVolume(t), Now: time.Now}); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v", err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	if list, _ := c.List(); len(list) != 0 {
		t.Fatalf("busy run recorded scans: %+v", list)
	}
}

func TestRunScheduledScanFails(t *testing.T) {
	data := t.TempDir()
	setDupSchedule(t, data, true)
	boom := func() ([]volumes.Volume, []volumes.Skipped, error) { return nil, nil, errors.New("boom") }
	res, err := RunScheduled(context.Background(), Options{DataDir: data, Discover: boom, Now: time.Now})
	if err == nil || res.Checked {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	noDupCheck(t, data, res.ScanID)
}

func TestRunScheduledCanceled(t *testing.T) {
	data := t.TempDir()
	setDupSchedule(t, data, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := RunScheduled(ctx, Options{DataDir: data, Discover: dupVolume(t), Now: time.Now})
	if !errors.Is(err, context.Canceled) || res.Checked {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	noDupCheck(t, data, res.ScanID)
}
