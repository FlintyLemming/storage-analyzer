//go:build !linux

package runner

import "syscall"

// SetLowPriority lowers CPU priority (I/O class is Linux-only).
func SetLowPriority() {
	syscall.Setpriority(syscall.PRIO_PROCESS, 0, 19)
}
