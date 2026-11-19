package scanner

import "syscall"

// Timespec fields are int32 on 32-bit Linux, so convert explicitly.
func statFields(st *syscall.Stat_t) (dev, ino, nlink uint64, mtime int64) {
	return uint64(st.Dev), st.Ino, uint64(st.Nlink), int64(st.Mtim.Sec)
}
