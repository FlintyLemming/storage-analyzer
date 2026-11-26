package auth

import (
	"fmt"
	"net/netip"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestLockoutAfterFiveFailures(t *testing.T) {
	l := NewLimiter()
	ip := netip.MustParseAddr("198.51.100.1")
	for i := 0; i < 5; i++ {
		if _, ok := l.Allow(ip, t0); !ok {
			t.Fatalf("attempt %d unexpectedly blocked", i)
		}
		l.Fail(ip, t0)
	}
	retry, ok := l.Allow(ip, t0.Add(time.Minute))
	if ok {
		t.Fatal("expected locked")
	}
	if retry <= 13*time.Minute || retry > 15*time.Minute {
		t.Fatalf("retryAfter = %v", retry)
	}
	if _, ok := l.Allow(ip, t0.Add(15*time.Minute+time.Second)); !ok {
		t.Fatal("expected unlocked after lockout")
	}
}

func TestFailuresOutsideWindow(t *testing.T) {
	l := NewLimiter()
	ip := netip.MustParseAddr("198.51.100.1")
	for i := 0; i < 4; i++ {
		l.Fail(ip, t0)
	}
	l.Fail(ip, t0.Add(16*time.Minute))
	if _, ok := l.Allow(ip, t0.Add(16*time.Minute)); !ok {
		t.Fatal("expected ok")
	}
}

func TestSucceedClears(t *testing.T) {
	l := NewLimiter()
	ip := netip.MustParseAddr("198.51.100.1")
	for i := 0; i < 4; i++ {
		l.Fail(ip, t0)
	}
	l.Succeed(ip)
	for i := 0; i < 4; i++ {
		l.Fail(ip, t0)
	}
	if _, ok := l.Allow(ip, t0); !ok {
		t.Fatal("expected ok")
	}
}

func TestGlobalBudget(t *testing.T) {
	l := NewLimiter()
	for i := 0; i < 20; i++ {
		ip := netip.MustParseAddr(fmt.Sprintf("198.51.100.%d", i+1))
		if _, ok := l.Allow(ip, t0); !ok {
			t.Fatalf("allow %d blocked", i)
		}
	}
	retry, ok := l.Allow(netip.MustParseAddr("198.51.100.99"), t0)
	if ok || retry <= 0 {
		t.Fatalf("expected blocked with retry>0, got %v %v", retry, ok)
	}
	if _, ok := l.Allow(netip.MustParseAddr("198.51.100.99"), t0.Add(61*time.Second)); !ok {
		t.Fatal("expected ok after window")
	}
}

func TestLockedDoesNotConsumeGlobal(t *testing.T) {
	l := NewLimiter()
	ip := netip.MustParseAddr("198.51.100.1")
	for i := 0; i < 5; i++ {
		l.Fail(ip, t0)
	}
	for i := 0; i < 50; i++ {
		l.Allow(ip, t0)
	}
	if _, ok := l.Allow(netip.MustParseAddr("198.51.100.2"), t0); !ok {
		t.Fatal("global budget consumed by locked source")
	}
}

func TestIPv6Slash64(t *testing.T) {
	l := NewLimiter()
	for i := 1; i <= 5; i++ {
		l.Fail(netip.MustParseAddr(fmt.Sprintf("2001:db8::%d", i)), t0)
	}
	if _, ok := l.Allow(netip.MustParseAddr("2001:db8::ffff"), t0); ok {
		t.Fatal("expected locked")
	}
	if _, ok := l.Allow(netip.MustParseAddr("2001:db8:0:1::1"), t0); !ok {
		t.Fatal("other /64 should be fine")
	}
}

func TestInvalidAddrSharedBucket(t *testing.T) {
	l := NewLimiter()
	for i := 0; i < 5; i++ {
		l.Fail(netip.Addr{}, t0)
	}
	if retry, ok := l.Allow(netip.Addr{}, t0); ok || retry <= 0 {
		t.Fatal("expected locked")
	}
}
