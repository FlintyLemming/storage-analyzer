package mounts

import (
	"strings"
	"testing"
)

func TestBoundaryChecker(t *testing.T) {
	fsA := [16]byte{1, 2, 3, 4}
	fsB := [16]byte{9, 8, 7, 6}

	// Realistic mountinfo covering:
	// - ext4 root + ProtectSystem=strict self-bind-mount
	// - vfat /boot/efi
	// - btrfs /mnt/extend (subvolid=5, subvol=/)
	// - subdir bind-mount of unmounted subvolume /Projects (subvolid=259, root=/Projects/storage-analyzer != subvol=/Projects)
	// - separately mounted child subvolumes /var/lib/kubelet (519), /var/lib/etcd (517)
	// - overmounted btrfs subvolume /mnt/overmounted (600 hidden under tmpfs)
	const mi = "" +
		"31 1 259:3 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw\n" +
		"80 31 259:3 /var/lib/storage-analyzer /var/lib/storage-analyzer rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw\n" +
		"109 31 259:2 / /boot/efi rw,relatime shared:93 - vfat /dev/nvme0n1p1 rw\n" +
		"117 31 0:37 / /mnt/extend rw,noatime shared:133 - btrfs /dev/md0 rw,subvolid=5,subvol=/\n" +
		"118 31 0:37 /Projects/storage-analyzer /srv/sa rw,noatime shared:133 - btrfs /dev/md0 rw,subvolid=259,subvol=/Projects\n" +
		"113 31 0:37 /k8s/kubelet /var/lib/kubelet rw,noatime shared:136 - btrfs /dev/md0 rw,subvolid=519,subvol=/k8s/kubelet\n" +
		"114 31 0:37 /k8s/etcd /var/lib/etcd rw,noatime shared:139 - btrfs /dev/md0 rw,subvolid=517,subvol=/k8s/etcd\n" +
		"120 31 0:37 /hidden /mnt/overmounted rw,noatime shared:140 - btrfs /dev/md0 rw,subvolid=600,subvol=/hidden\n" +
		"121 120 0:99 / /mnt/overmounted rw,relatime - tmpfs tmpfs rw\n"

	ms, err := Parse(strings.NewReader(mi))
	if err != nil {
		t.Fatal(err)
	}

	subvols := map[string]subvolInfo{
		"/mnt/extend/Projects":         {fsid: fsA, id: 259},
		"/mnt/extend/appdata":          {fsid: fsA, id: 260},
		"/mnt/extend/k8s":              {fsid: fsA, id: 516},
		"/mnt/extend/k8s/kubelet":      {fsid: fsA, id: 519},
		"/mnt/extend/k8s/etcd":         {fsid: fsA, id: 517},
		"/mnt/extend/hidden":           {fsid: fsA, id: 600},
		"/mnt/extend/Projects-snap":    {fsid: fsA, id: 700, isSnapshot: true},
		"/mnt/extend/.snapshots":       {fsid: fsA, id: 701},
		"/mnt/other-btrfs/same-id-519": {fsid: fsB, id: 519},
	}

	probed := map[string]int{}
	probe := btrfsProber{
		fsID: func(path string) ([16]byte, bool) {
			// Simulate /var/lib/etcd being unreadable while /mnt/extend succeeds.
			if path == "/var/lib/etcd" {
				return [16]byte{}, false
			}
			if path == "/mnt/extend" || path == "/var/lib/kubelet" {
				return fsA, true
			}
			return [16]byte{}, false
		},
		subvol: func(path string) (subvolInfo, bool) {
			probed[path]++
			info, ok := subvols[path]
			return info, ok
		},
	}

	isMount := newBoundaryChecker(ms, probe)

	cases := []struct {
		name      string
		path      string
		parentDev uint64
		dev       uint64
		want      bool
	}{
		{"same dev regular file", "/etc/hosts", 10, 10, false},
		{"same dev ProtectSystem self-bind", "/var/lib/storage-analyzer", 10, 10, false},
		{"real mount point", "/boot/efi", 10, 20, true},
		{"real btrfs mount point", "/mnt/extend", 10, 38, true},
		{"unmounted btrfs subvol with subdir bind elsewhere", "/mnt/extend/Projects", 38, 67, false},
		{"unmounted normal btrfs subvol", "/mnt/extend/appdata", 38, 68, false},
		{"unmounted parent subvol k8s", "/mnt/extend/k8s", 38, 69, false},
		{"nested subvol mounted at /var/lib/kubelet", "/mnt/extend/k8s/kubelet", 69, 40, true},
		{"nested subvol mounted at unreadable /var/lib/etcd", "/mnt/extend/k8s/etcd", 69, 41, true},
		{"subvol whose only mount was overmounted by tmpfs", "/mnt/extend/hidden", 38, 70, false},
		{"btrfs snapshot subvol", "/mnt/extend/Projects-snap", 38, 71, true},
		{"snapper .snapshots subvol", "/mnt/extend/.snapshots", 38, 72, true},
		{"same subvolid on different btrfs filesystem", "/mnt/other-btrfs/same-id-519", 90, 91, false},
		{"non-btrfs cross-dev automount or mid-scan mount", "/mnt/new-usb", 10, 99, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMount(tc.path, tc.parentDev, tc.dev); got != tc.want {
				t.Errorf("isMount(%q, %d, %d) = %v, want %v", tc.path, tc.parentDev, tc.dev, got, tc.want)
			}
		})
	}

	if probed["/boot/efi"] != 0 || probed["/mnt/extend"] != 0 || probed["/mnt/extend/.snapshots"] != 0 {
		t.Errorf("mount points and .snapshots should not be probed via ioctl: %v", probed)
	}
}

func TestBoundaryCheckerNilProberFallsBackToSkip(t *testing.T) {
	isMount := newBoundaryChecker(nil, btrfsProber{})
	if isMount("/a/b", 10, 10) {
		t.Fatal("same dev should not be skipped")
	}
	if !isMount("/a/b", 10, 20) {
		t.Fatal("different dev with nil prober should be skipped")
	}
}
