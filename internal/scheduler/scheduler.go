// Package scheduler fires scheduled scans in-process (used by the Linux service).
package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
)

// Due reports whether now (in its own location) matches the schedule.
func Due(s catalog.Settings, now time.Time) bool {
	switch s.ScheduleMode {
	case "daily":
	case "weekly":
		if int(now.Weekday()) != s.Weekday {
			return false
		}
	default:
		return false
	}
	return now.Format("15:04") == s.Time
}

// Scheduler fires at most once per "date + configured time" combination.
type Scheduler struct {
	Settings func() (catalog.Settings, error)
	Fire     func() error
	Now      func() time.Time // nil means time.Now

	mu   sync.Mutex
	last string
}

func (s *Scheduler) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Tick checks the schedule once and fires if due and not already fired for this key.
func (s *Scheduler) Tick() {
	st, err := s.Settings()
	if err != nil {
		log.Printf("scheduler: read settings: %v", err)
		return
	}
	now := s.now()
	if !Due(st, now) {
		return
	}
	key := now.Format("2006-01-02") + " " + st.Time
	s.mu.Lock()
	if key == s.last {
		s.mu.Unlock()
		return
	}
	s.last = key
	s.mu.Unlock()
	if err := s.Fire(); err != nil {
		log.Printf("scheduler: fire: %v", err)
	}
}

// Run wakes just after each minute boundary and calls Tick until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	for {
		now := s.now()
		wait := now.Truncate(time.Minute).Add(time.Minute + time.Second).Sub(now)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
			s.Tick()
		}
	}
}
