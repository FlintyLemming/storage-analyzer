package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
)

func at(h, m, s int) time.Time { return time.Date(2026, 11, 25, h, m, s, 0, time.UTC) } // Wednesday

func TestDueDaily(t *testing.T) {
	s := catalog.Settings{ScheduleMode: "daily", Time: "03:00"}
	if !Due(s, at(3, 0, 30)) {
		t.Fatal("want due at 03:00:30")
	}
	if Due(s, at(3, 1, 0)) {
		t.Fatal("want not due at 03:01:00")
	}
}

func TestDueWeekly(t *testing.T) {
	s := catalog.Settings{ScheduleMode: "weekly", Weekday: 3, Time: "03:00"}
	if !Due(s, at(3, 0, 0)) {
		t.Fatal("want due on Wednesday")
	}
	if Due(s, at(3, 0, 0).AddDate(0, 0, 1)) {
		t.Fatal("want not due on Thursday")
	}
}

func TestDueOff(t *testing.T) {
	if Due(catalog.Settings{ScheduleMode: "off", Time: "03:00"}, at(3, 0, 0)) {
		t.Fatal("off must never be due")
	}
}

func newSched(s *catalog.Settings, now *time.Time, fires *int) *Scheduler {
	return &Scheduler{
		Settings: func() (catalog.Settings, error) { return *s, nil },
		Fire:     func() error { *fires++; return nil },
		Now:      func() time.Time { return *now },
	}
}

func TestTickOncePerKey(t *testing.T) {
	s := catalog.Settings{ScheduleMode: "daily", Time: "03:00"}
	now := at(3, 0, 1)
	fires := 0
	sc := newSched(&s, &now, &fires)
	sc.Tick()
	now = at(3, 0, 59)
	sc.Tick()
	if fires != 1 {
		t.Fatalf("fires = %d, want 1", fires)
	}
}

func TestTickDSTRepeat(t *testing.T) {
	s := catalog.Settings{ScheduleMode: "daily", Time: "01:30"}
	now := time.Date(2026, 11, 1, 1, 30, 0, 0, time.FixedZone("PDT", -7*3600))
	fires := 0
	sc := newSched(&s, &now, &fires)
	sc.Tick()
	now = time.Date(2026, 11, 1, 1, 30, 0, 0, time.FixedZone("PST", -8*3600))
	sc.Tick()
	if fires != 1 {
		t.Fatalf("fires = %d, want 1", fires)
	}
}

func TestTickNewTimeSameDay(t *testing.T) {
	s := catalog.Settings{ScheduleMode: "daily", Time: "03:00"}
	now := at(3, 0, 1)
	fires := 0
	sc := newSched(&s, &now, &fires)
	sc.Tick()
	s.Time = "15:00"
	now = at(15, 0, 1)
	sc.Tick()
	if fires != 2 {
		t.Fatalf("fires = %d, want 2", fires)
	}
}

func TestTickSettingsError(t *testing.T) {
	fires := 0
	sc := &Scheduler{
		Settings: func() (catalog.Settings, error) { return catalog.Settings{}, errors.New("boom") },
		Fire:     func() error { fires++; return nil },
	}
	sc.Tick()
	if fires != 0 {
		t.Fatal("Fire must not be called")
	}
}

func TestRunReturnsOnCancel(t *testing.T) {
	sc := &Scheduler{
		Settings: func() (catalog.Settings, error) { return catalog.Settings{}, nil },
		Fire:     func() error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sc.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
