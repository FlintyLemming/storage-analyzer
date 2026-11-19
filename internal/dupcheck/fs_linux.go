package dupcheck

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func statInfo(st *syscall.Stat_t) FileInfo {
	// Timespec fields are int32 on 32-bit Linux: widen before multiplying.
	mtimeNS := int64(st.Mtim.Sec)*1e9 + int64(st.Mtim.Nsec)
	ctimeNS := int64(st.Ctim.Sec)*1e9 + int64(st.Ctim.Nsec)
	return FileInfo{
		Dev: uint64(st.Dev), Ino: st.Ino, Size: st.Size,
		MTime:   int64(st.Mtim.Sec),
		MTimeNS: mtimeNS,
		CTimeNS: ctimeNS,
		Regular: st.Mode&syscall.S_IFMT == syscall.S_IFREG,
	}
}

// dropCache asks the kernel to drop f's cached pages (whole file). The
// fadvise syscall differs between architectures; unix.Fadvise handles it.
func dropCache(f *os.File) {
	unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
