package auth

import (
	"net/netip"
	"sync"
	"time"
)

const (
	MaxFailures     = 5
	FailureWindow   = 15 * time.Minute
	LockoutDuration = 15 * time.Minute
	GlobalPerMinute = 20
)

type sourceState struct {
	failures    []time.Time
	lockedUntil time.Time
}

// Limiter throttles password verification per source and globally. It is safe
// for concurrent use.
type Limiter struct {
	mu      sync.Mutex
	sources map[netip.Prefix]*sourceState
	global  []time.Time
}

func NewLimiter() *Limiter {
	return &Limiter{sources: map[netip.Prefix]*sourceState{}}
}

// limiterKey maps an address to its bucket: IPv4 by address, IPv6 by /64, and
// invalid addresses to one shared bucket.
func limiterKey(ip netip.Addr) netip.Prefix {
	if !ip.IsValid() {
		return netip.Prefix{}
	}
	ip = ip.Unmap()
	if ip.Is4() {
		return netip.PrefixFrom(ip, 32)
	}
	return netip.PrefixFrom(ip, 64).Masked()
}

// prune drops expired state. Caller holds l.mu.
func (l *Limiter) prune(now time.Time) {
	for k, s := range l.sources {
		if !s.lockedUntil.After(now) {
			s.lockedUntil = time.Time{}
		}
		kept := s.failures[:0]
		for _, f := range s.failures {
			if now.Sub(f) < FailureWindow {
				kept = append(kept, f)
			}
		}
		s.failures = kept
		if len(s.failures) == 0 && s.lockedUntil.IsZero() {
			delete(l.sources, k)
		}
	}
}

// Allow reports whether a password verification may proceed. When ok, it
// consumes one slot of the global budget.
func (l *Limiter) Allow(ip netip.Addr, now time.Time) (retryAfter time.Duration, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(now)
	if s := l.sources[limiterKey(ip)]; s != nil && s.lockedUntil.After(now) {
		return s.lockedUntil.Sub(now), false
	}
	kept := l.global[:0]
	for _, t := range l.global {
		if now.Sub(t) < time.Minute {
			kept = append(kept, t)
		}
	}
	l.global = kept
	if len(l.global) >= GlobalPerMinute {
		return l.global[0].Add(time.Minute).Sub(now), false
	}
	l.global = append(l.global, now)
	return 0, true
}

// Fail records a failed attempt, locking the source on the 5th within the window.
func (l *Limiter) Fail(ip netip.Addr, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prune(now)
	k := limiterKey(ip)
	s := l.sources[k]
	if s == nil {
		s = &sourceState{}
		l.sources[k] = s
	}
	s.failures = append(s.failures, now)
	if len(s.failures) >= MaxFailures {
		s.lockedUntil = now.Add(LockoutDuration)
		s.failures = nil
	}
}

// Succeed clears the failure history of the source.
func (l *Limiter) Succeed(ip netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.sources, limiterKey(ip))
}
