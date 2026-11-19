package catalog

import (
	"errors"
	"os"
	"slices"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 16, 3, 0, 0, 0, time.UTC)

func open(t *testing.T) (*Catalog, string) {
	t.Helper()
	dir := t.TempDir()
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, dir
}

// doneScan creates a finished scan with a data file on disk.
func doneScan(t *testing.T, c *Catalog, dir string, at time.Time) int64 {
	t.Helper()
	id, err := c.Begin("manual", at)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ScanPath(dir, id), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(Scan{ID: id, FinishedAt: at.Add(time.Minute).Unix(), Files: 10, TotalDSize: 100, DurationS: 60, DBBytes: 1}); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestBeginFinishList(t *testing.T) {
	c, dir := open(t)
	id := doneScan(t, c, dir, t0)
	id2, _ := c.Begin("schedule", t0.Add(time.Hour))
	list, err := c.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != id2 || list[1].ID != id {
		t.Fatalf("list order wrong: %+v", list)
	}
	if list[0].Status != "running" || list[0].Trigger != "schedule" {
		t.Fatalf("running scan = %+v", list[0])
	}
	s := list[1]
	if s.Status != "done" || s.Files != 10 || s.TotalDSize != 100 || s.StartedAt != t0.Unix() {
		t.Fatalf("done scan = %+v", s)
	}
}

func TestGetMissing(t *testing.T) {
	c, _ := open(t)
	if _, err := c.Get(42); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestPruneKeepsNewestDone(t *testing.T) {
	c, dir := open(t)
	var ids []int64
	for i := 0; i < 12; i++ {
		ids = append(ids, doneScan(t, c, dir, t0.Add(time.Duration(i)*time.Hour)))
	}
	for i := 0; i < 2; i++ {
		id, _ := c.Begin("manual", t0.Add(-time.Hour))
		c.Fail(id, "failed", "boom", t0)
	}
	deleted, err := c.Prune(10)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(deleted)
	if !slices.Equal(deleted, ids[:2]) {
		t.Fatalf("deleted = %v, want the two oldest %v", deleted, ids[:2])
	}
	for _, id := range ids[:2] {
		if _, err := os.Stat(ScanPath(dir, id)); !os.IsNotExist(err) {
			t.Fatalf("scan %d file still present", id)
		}
	}
	list, _ := c.List()
	if len(list) != 12 { // 10 done + 2 failed
		t.Fatalf("len(list) = %d, want 12", len(list))
	}
}

func TestDeleteRemovesFile(t *testing.T) {
	c, dir := open(t)
	id := doneScan(t, c, dir, t0)
	if err := c.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ScanPath(dir, id)); !os.IsNotExist(err) {
		t.Fatal("file still present")
	}
	if _, err := c.Get(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("row still present: %v", err)
	}
	if err := c.Delete(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete err = %v", err)
	}
}

func TestMarkStaleRunning(t *testing.T) {
	c, dir := open(t)
	doneScan(t, c, dir, t0)
	id, _ := c.Begin("manual", t0)
	n, err := c.MarkStaleRunning(t0.Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	s, _ := c.Get(id)
	if s.Status != "failed" || s.ErrorMessage != "interrupted" || s.FinishedAt != t0.Add(time.Hour).Unix() {
		t.Fatalf("stale scan = %+v", s)
	}
}

func TestSettingsDefaultsAndValidation(t *testing.T) {
	c, _ := open(t)
	s, err := c.Settings()
	if err != nil {
		t.Fatal(err)
	}
	want := Settings{ScheduleMode: "off", Weekday: 0, Time: "03:00", Retention: 10, DefaultSizeMode: "disk", ScheduleDupCheck: false}
	if s != want {
		t.Fatalf("defaults = %+v, want %+v", s, want)
	}
	bad := []Settings{
		{ScheduleMode: "off", Time: "03:00", Retention: 0, DefaultSizeMode: "disk"},
		{ScheduleMode: "off", Time: "03:00", Retention: 101, DefaultSizeMode: "disk"},
		{ScheduleMode: "daily", Time: "25:00", Retention: 10, DefaultSizeMode: "disk"},
		{ScheduleMode: "weekly", Weekday: 7, Time: "03:00", Retention: 10, DefaultSizeMode: "disk"},
		{ScheduleMode: "hourly", Time: "03:00", Retention: 10, DefaultSizeMode: "disk"},
		{ScheduleMode: "off", Time: "03:00", Retention: 10, DefaultSizeMode: "huge"},
	}
	for _, b := range bad {
		if err := c.SaveSettings(b); err == nil {
			t.Fatalf("SaveSettings(%+v) accepted invalid settings", b)
		}
	}
	good := Settings{ScheduleMode: "weekly", Weekday: 6, Time: "23:45", Retention: 3, DefaultSizeMode: "apparent", ScheduleDupCheck: true}
	if err := c.SaveSettings(good); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Settings(); got != good {
		t.Fatalf("got %+v, want %+v", got, good)
	}
}

// Scan results list every user's file names, so only root may read them.
func TestOpenMakesDataDirPrivate(t *testing.T) {
	dir := t.TempDir() + "/data"
	os.MkdirAll(dir, 0o755)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	for _, p := range []string{dir, dir + "/scans"} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if perm := st.Mode().Perm(); perm != 0o700 {
			t.Fatalf("%s mode = %o, want 700", p, perm)
		}
	}
}

func TestDupCheckLifecycle(t *testing.T) {
	c, dir := open(t)
	id := doneScan(t, c, dir, t0)
	if _, err := c.GetDupCheck(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if err := c.BeginDupCheck(id, t0); err != nil {
		t.Fatal(err)
	}
	d, err := c.GetDupCheck(id)
	if err != nil || d.Status != "running" || d.StartedAt != t0.Unix() {
		t.Fatalf("running = %+v %v", d, err)
	}
	if err := c.FinishDupCheck(DupCheck{ScanID: id, FinishedAt: t0.Unix() + 90, DurationS: 90, Groups: 3, ReclaimDSize: 4096, ReclaimASize: 4000, BytesRead: 7, CacheHits: 2, Changed: 1}); err != nil {
		t.Fatal(err)
	}
	list, _ := c.List()
	if list[0].Dup == nil || list[0].Dup.Status != "done" || list[0].Dup.Groups != 3 || list[0].Dup.ReclaimDSize != 4096 || list[0].Dup.Changed != 1 {
		t.Fatalf("list dup = %+v", list[0].Dup)
	}
	if err := c.FailDupCheck(id, "canceled", "context canceled", t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	d, _ = c.GetDupCheck(id)
	if d.Status != "canceled" || d.ErrorMessage != "context canceled" {
		t.Fatalf("failed = %+v", d)
	}
}

func TestDeleteRemovesDups(t *testing.T) {
	c, dir := open(t)
	id := doneScan(t, c, dir, t0)
	c.BeginDupCheck(id, t0)
	os.WriteFile(DupsPath(dir, id), []byte("x"), 0o600)
	if err := c.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(DupsPath(dir, id)); !os.IsNotExist(err) {
		t.Fatalf("dups file still there: %v", err)
	}
	if _, err := c.GetDupCheck(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dupcheck row still there: %v", err)
	}
}

func TestMarkStaleRunningDupCheck(t *testing.T) {
	c, dir := open(t)
	id := doneScan(t, c, dir, t0)
	c.BeginDupCheck(id, t0)
	n, err := c.MarkStaleRunning(t0.Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	d, _ := c.GetDupCheck(id)
	if d.Status != "failed" || d.ErrorMessage != "interrupted" || d.DurationS != 3600 {
		t.Fatalf("stale check = %+v", d)
	}
}
