//go:build linux

package mounts

import (
	"os"
	"syscall"
	"testing"
)

func TestLinuxBtrfsProber(t *testing.T) {
	probe := linuxBtrfsProber()

	// Non-Btrfs directory (or regular temp dir with ino != 256) must not be
	// reported as a Btrfs subvolume root.
	dir := t.TempDir()
	if info, ok := probe.subvol(dir); ok {
		t.Fatalf("TempDir %s wrongly identified as Btrfs subvolume root: %+v", dir, info)
	}

	// Regular file inside TempDir must not panic or be reported as a subvolume.
	f := dir + "/plain"
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if info, ok := probe.subvol(f); ok {
		t.Fatalf("regular file wrongly identified as Btrfs subvolume root: %+v", info)
	}

	// If /etc is not Btrfs, fsID must return ok=false.
	if _, ok := probe.subvol("/etc"); ok {
		t.Fatalf("/etc should not be a Btrfs subvolume root")
	}

	// If the current workspace is on Btrfs, verify non-root dir gets fsID
	// (confirming BTRFS_IOC_FS_INFO works) while subvol returns ok=false
	// (confirming ino == 256 guards against non-root Btrfs directories).
	if cwd, err := os.Getwd(); err == nil {
		var st syscall.Stat_t
		if syscall.Lstat(cwd, &st) == nil && st.Ino != btrfsFirstFreeObjectID {
			if info, ok := probe.subvol(cwd); ok {
				t.Fatalf("non-root dir %s (ino=%d) wrongly identified as subvol root: %+v", cwd, st.Ino, info)
			}
		}
	}

	// If a real readable Btrfs subvolume root is visible (e.g. on this host),
	// verify BTRFS_IOC_FS_INFO and BTRFS_IOC_GET_SUBVOL_INFO both succeed.
	fmi, err := os.Open(mountinfoPath)
	if err != nil {
		return
	}
	defer fmi.Close()
	ms, err := Parse(fmi)
	if err != nil {
		return
	}
	for _, m := range ms {
		if m.FSType != "btrfs" || m.SubvolID == "" || m.Subvol == "" || m.Root != m.Subvol {
			continue
		}
		var st syscall.Stat_t
		if syscall.Lstat(m.MountPoint, &st) != nil || st.Ino != btrfsFirstFreeObjectID {
			continue
		}
		fd, err := os.Open(m.MountPoint)
		if err != nil {
			continue
		}
		fd.Close()

		fsid, ok := probe.fsID(m.MountPoint)
		if !ok || allZero(fsid[:]) {
			t.Fatalf("fsID(%s) failed: fsid=%x ok=%v", m.MountPoint, fsid, ok)
		}
		info, ok := probe.subvol(m.MountPoint)
		if !ok {
			t.Fatalf("subvol(%s) failed on real Btrfs subvolume root", m.MountPoint)
		}
		if info.fsid != fsid || info.id == 0 {
			t.Fatalf("subvol(%s) = %+v, want fsid=%x and non-zero id", m.MountPoint, info, fsid)
		}
		return
	}
}
