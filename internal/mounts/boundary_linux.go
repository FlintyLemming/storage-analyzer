//go:build linux

package mounts

import (
	"encoding/binary"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	btrfsFirstFreeObjectID = 256
	btrfsIoctlMagic        = 0x94
	// _IOR(0x94, 31, struct btrfs_ioctl_fs_info_args) (1024 bytes on all Linux arches).
	btrfsIOCFSInfo = 2<<30 | 1024<<16 | btrfsIoctlMagic<<8 | 31
)

func btrfsIOCGetSubvolInfo() uintptr {
	// struct btrfs_ioctl_get_subvol_info_args embeds four btrfs_ioctl_timespec
	// structs ({__u64 sec; __u32 nsec}). On 386, __u64 has 4-byte alignment so
	// each timespec is 12 bytes (total 488); on amd64, arm64 and ARM EABI,
	// __u64 has 8-byte alignment so each timespec is 16 bytes (total 504).
	size := uintptr(504)
	if runtime.GOARCH == "386" {
		size = 488
	}
	return 2<<30 | size<<16 | btrfsIoctlMagic<<8 | 60
}

func defaultBtrfsProber() btrfsProber { return linuxBtrfsProber() }

func linuxBtrfsProber() btrfsProber {
	return btrfsProber{
		fsID:   btrfsFSID,
		subvol: btrfsInspectSubvol,
	}
}

func openDirNoFollow(path string) (int, error) {
	return unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
}

func btrfsFSIDFD(fd int) ([16]byte, bool) {
	var buf [1024]byte
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), btrfsIOCFSInfo, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return [16]byte{}, false
	}
	var fsid [16]byte
	copy(fsid[:], buf[16:32])
	return fsid, true
}

func btrfsFSID(path string) ([16]byte, bool) {
	fd, err := openDirNoFollow(path)
	if err != nil {
		return [16]byte{}, false
	}
	defer unix.Close(fd)
	return btrfsFSIDFD(fd)
}

func btrfsInspectSubvol(path string) (subvolInfo, bool) {
	fd, err := openDirNoFollow(path)
	if err != nil {
		return subvolInfo{}, false
	}
	defer unix.Close(fd)

	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Ino != btrfsFirstFreeObjectID {
		return subvolInfo{}, false
	}
	fsid, ok := btrfsFSIDFD(fd)
	if !ok {
		return subvolInfo{}, false
	}

	var buf [504]byte
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), btrfsIOCGetSubvolInfo(), uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return subvolInfo{}, false
	}
	treeID := binary.NativeEndian.Uint64(buf[0:8])
	isSnap := !allZero(buf[312:328]) || !allZero(buf[328:344])
	return subvolInfo{fsid: fsid, id: treeID, isSnapshot: isSnap}, true
}
