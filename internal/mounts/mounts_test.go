package mounts

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/FlintyLemming/storage-analyzer/internal/config"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

func load(t *testing.T, name string) []Mount {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name+".txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ms, err := Parse(f)
	if err != nil {
		t.Fatalf("Parse(%s): %v", name, err)
	}
	return ms
}

func mps(vs []volumes.Volume) []string {
	out := []string{}
	for _, v := range vs {
		out = append(out, v.MountPoint)
	}
	return out
}

func TestSelectFixtures(t *testing.T) {
	cases := map[string][]string{
		"ubuntu-ext4":         {"/", "/boot/efi"},
		"btrfs-subvol":        {"/", "/home"},
		"btrfs-nested-subvol": {"/", "/boot/efi", "/mnt/extend", "/var/lib/containerd", "/var/lib/etcd", "/var/lib/kubelet"},
		"proxmox-zfs":         {"/", "/rpool", "/rpool/data", "/rpool/data/subvol-100-disk-0"},
		"docker-host":         {"/"},
		"network":             {"/", "/media/usb"},
		"systemd-strict":      {"/", "/boot/efi"},
		"bind":                {"/", "/data"},
		"escape":              {"/", "/mnt/my disk"},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			got := mps(Select(load(t, name), nil))
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestSelectOvermount(t *testing.T) {
	got := mps(Select(load(t, "overmount"), nil))
	if want := []string{"/"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSelectExclude(t *testing.T) {
	got := mps(Select(load(t, "ubuntu-ext4"), []string{"/boot/efi/"}))
	if want := []string{"/"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSelectVolumeFields(t *testing.T) {
	vs := Select(load(t, "ubuntu-ext4"), nil)
	if len(vs) == 0 {
		t.Fatal("no volumes")
	}
	v := vs[0]
	if v.MountPoint != "/" || v.Device != "/dev/sda2" || v.FSType != "ext4" || v.Name != "/" {
		t.Errorf("unexpected volume %+v", v)
	}
}

func TestParseFields(t *testing.T) {
	ms, err := Parse(strings.NewReader("36 25 8:2 / / rw,relatime shared:1 shared:2 - ext4 /dev/sda2 rw\n\n"))
	if err != nil || len(ms) != 1 {
		t.Fatalf("ms=%v err=%v", ms, err)
	}
	want := Mount{ID: 36, Parent: 25, Dev: "8:2", Root: "/", MountPoint: "/", FSType: "ext4", Source: "/dev/sda2"}
	if ms[0] != want {
		t.Errorf("got %+v, want %+v", ms[0], want)
	}
}

func TestParseMalformed(t *testing.T) {
	for _, line := range []string{
		"36 25 8:2 / / rw,relatime shared:1 ext4 /dev/sda2 rw\n",
		"36 25 8:2 /\n",
		"x 25 8:2 / / rw - ext4 /dev/sda2 rw\n",
	} {
		if _, err := Parse(strings.NewReader(line)); err == nil {
			t.Errorf("expected error for %q", line)
		}
	}
}

func TestFromPaths(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"a", "a/b", "c"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ms := []Mount{
		{MountPoint: "/", FSType: "ext4", Source: "/dev/root"},
		{MountPoint: root, FSType: "xfs", Source: "/dev/sdb1"},
	}
	a, b, c := filepath.Join(root, "a"), filepath.Join(root, "a/b"), filepath.Join(root, "c")
	file := filepath.Join(root, "plain-file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(root, "missing")
	vs, sk := FromPaths([]string{a, b, c, missing, file}, ms)
	if got, want := mps(vs), []string{a, c}; !reflect.DeepEqual(got, want) {
		t.Errorf("volumes %v, want %v", got, want)
	}
	for _, v := range vs {
		if v.Device != "/dev/sdb1" || v.FSType != "xfs" || v.Name != v.MountPoint {
			t.Errorf("bad volume %+v", v)
		}
	}
	wantSkip := []volumes.Skipped{
		{Name: missing, Reason: "not_found"},
		{Name: file, Reason: "not_directory"},
	}
	if !reflect.DeepEqual(sk, wantSkip) {
		t.Errorf("skipped %v, want %v", sk, wantSkip)
	}
}

func TestFromPathsRootAncestor(t *testing.T) {
	root := t.TempDir()
	vs, _ := FromPaths([]string{root, "/"}, nil)
	if got := mps(vs); !reflect.DeepEqual(got, []string{"/"}) {
		t.Errorf("got %v", got)
	}
}

func TestDiscoverExplicitPaths(t *testing.T) {
	root := t.TempDir()
	// mountinfo may be missing (macOS); explicit paths must still work.
	vs, sk, err := Discover(configWithPaths([]string{root}))
	if err != nil || len(sk) != 0 || len(vs) != 1 || vs[0].MountPoint != root {
		t.Errorf("vs=%v sk=%v err=%v", vs, sk, err)
	}
}

func configWithPaths(p []string) config.Config { return config.Config{Paths: p} }
