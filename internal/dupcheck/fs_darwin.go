package dupcheck

import (
	"os"
	"syscall"
)

func statInfo(st *syscall.Stat_t) FileInfo {
	return FileInfo{
		Dev: uint64(st.Dev), Ino: st.Ino, Size: st.Size,
		MTime:   st.Mtimespec.Sec,
		MTimeNS: st.Mtimespec.Sec*1e9 + st.Mtimespec.Nsec,
		CTimeNS: st.Ctimespec.Sec*1e9 + st.Ctimespec.Nsec,
		Regular: st.Mode&syscall.S_IFMT == syscall.S_IFREG,
	}
}

func dropCache(*os.File) {}
