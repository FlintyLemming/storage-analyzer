package dupcheck

import (
	"os"
	"syscall"
)

func statInfo(st *syscall.Stat_t) FileInfo {
	return FileInfo{
		Dev: uint64(st.Dev), Ino: st.Ino, Size: st.Size,
		MTime:   st.Mtim.Sec,
		MTimeNS: st.Mtim.Sec*1e9 + st.Mtim.Nsec,
		CTimeNS: st.Ctim.Sec*1e9 + st.Ctim.Nsec,
		Regular: st.Mode&syscall.S_IFMT == syscall.S_IFREG,
	}
}

const fadvDontNeed = 4 // POSIX_FADV_DONTNEED

// dropCache asks the kernel to drop f's cached pages (whole file).
func dropCache(f *os.File) {
	syscall.Syscall6(syscall.SYS_FADVISE64, f.Fd(), 0, 0, fadvDontNeed, 0, 0)
}
