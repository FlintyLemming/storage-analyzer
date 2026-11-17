package scanner

import "syscall"

func statFields(st *syscall.Stat_t) (dev, ino, nlink uint64, mtime int64) {
	return uint64(st.Dev), st.Ino, uint64(st.Nlink), st.Mtimespec.Sec
}
