package runner

import (
	"os"
	"strconv"
	"syscall"
)

const (
	ioprioWhoProcess = 1
	ioprioClassIdle  = 3
	ioprioClassShift = 13
)

// SetLowPriority sets nice 19 and the idle I/O class on every thread of the
// process. Both are per-thread on Linux and inherited by threads created
// later, so all existing threads are updated.
func SetLowPriority() {
	tids := []int{0}
	if ents, err := os.ReadDir("/proc/self/task"); err == nil {
		tids = tids[:0]
		for _, e := range ents {
			if tid, err := strconv.Atoi(e.Name()); err == nil {
				tids = append(tids, tid)
			}
		}
	}
	for _, tid := range tids {
		syscall.Setpriority(syscall.PRIO_PROCESS, tid, 19)
		syscall.Syscall(syscall.SYS_IOPRIO_SET, ioprioWhoProcess, uintptr(tid), uintptr(ioprioClassIdle<<ioprioClassShift))
	}
}
