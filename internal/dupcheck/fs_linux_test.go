package dupcheck

import (
	"syscall"
	"testing"
)

// On 32-bit Linux the Timespec fields are int32: the nanosecond values must
// be computed in int64 or they overflow.
func TestStatInfoTimes(t *testing.T) {
	st := syscall.Stat_t{
		Mtim: syscall.Timespec{Sec: 1795000000, Nsec: 123},
		Ctim: syscall.Timespec{Sec: 1795000001, Nsec: 7},
	}
	fi := statInfo(&st)
	if fi.MTime != 1795000000 {
		t.Fatalf("MTime = %d", fi.MTime)
	}
	if want := int64(1795000000)*1e9 + 123; fi.MTimeNS != want {
		t.Fatalf("MTimeNS = %d, want %d", fi.MTimeNS, want)
	}
	if want := int64(1795000001)*1e9 + 7; fi.CTimeNS != want {
		t.Fatalf("CTimeNS = %d, want %d", fi.CTimeNS, want)
	}
}
