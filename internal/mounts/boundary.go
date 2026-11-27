package mounts

import (
	"os"
	"path/filepath"
	"strconv"
)

type subvolKey struct {
	fsid [16]byte
	id   uint64
}

type subvolInfo struct {
	fsid       [16]byte
	id         uint64
	isSnapshot bool
}

type btrfsProber struct {
	fsID   func(path string) ([16]byte, bool)
	subvol func(path string) (subvolInfo, bool)
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// NewBoundaryChecker builds a scanner IsMountPoint predicate from
// /proc/self/mountinfo on Linux. It allows descending into normal unmounted
// nested Btrfs subvolumes while continuing to skip real mount points, Btrfs
// snapshots, and subvolumes that are separately mounted elsewhere.
//
// Returns nil on non-Linux or if mountinfo cannot be read, letting the scanner
// fall back to dev != parentDev.
func NewBoundaryChecker() func(path string, parentDev, dev uint64) bool {
	probe := defaultBtrfsProber()
	if probe.subvol == nil {
		return nil
	}
	f, err := os.Open(mountinfoPath)
	if err != nil {
		return nil
	}
	defer f.Close()
	ms, err := Parse(f)
	if err != nil {
		return nil
	}
	return newBoundaryChecker(ms, probe)
}

func newBoundaryChecker(ms []Mount, probe btrfsProber) func(path string, parentDev, dev uint64) bool {
	last := make(map[string]int, len(ms))
	mountPoints := make(map[string]bool, len(ms))
	for i, m := range ms {
		mp := filepath.Clean(m.MountPoint)
		last[mp] = i
		mountPoints[mp] = true
	}

	btrfsByDev := map[string][]Mount{}
	for i, m := range ms {
		mp := filepath.Clean(m.MountPoint)
		if last[mp] != i || m.FSType != "btrfs" {
			continue
		}
		btrfsByDev[m.Dev] = append(btrfsByDev[m.Dev], m)
	}

	mountedSubvols := map[subvolKey]bool{}
	if probe.fsID != nil {
		for _, group := range btrfsByDev {
			var (
				fsid  [16]byte
				gotFS bool
			)
			for _, m := range group {
				if id, ok := probe.fsID(m.MountPoint); ok {
					fsid, gotFS = id, true
					break
				}
			}
			if !gotFS {
				continue
			}
			for _, m := range group {
				if m.SubvolID == "" || m.Subvol == "" {
					continue
				}
				// Only whole-subvolume mounts cover the subvolume; bind mounts
				// of a subdirectory inside a subvolume have Root != Subvol.
				if filepath.Clean("/"+m.Root) != filepath.Clean("/"+m.Subvol) {
					continue
				}
				id, err := strconv.ParseUint(m.SubvolID, 10, 64)
				if err != nil {
					continue
				}
				mountedSubvols[subvolKey{fsid: fsid, id: id}] = true
			}
		}
	}

	return func(path string, parentDev, dev uint64) bool {
		if dev == parentDev {
			return false
		}
		clean := filepath.Clean(path)
		if mountPoints[clean] {
			return true
		}
		if probe.subvol == nil {
			return true
		}
		if filepath.Base(clean) == ".snapshots" {
			return true
		}
		info, ok := probe.subvol(clean)
		if !ok {
			return true
		}
		if info.isSnapshot {
			return true
		}
		return mountedSubvols[subvolKey{fsid: info.fsid, id: info.id}]
	}
}
