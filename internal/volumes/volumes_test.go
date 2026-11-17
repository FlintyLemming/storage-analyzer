package volumes

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func loadFixtures(t *testing.T) ([]VolConf, map[string]string, map[string]string) {
	t.Helper()
	vf, err := os.Open("testdata/volume.conf")
	if err != nil {
		t.Fatal(err)
	}
	defer vf.Close()
	vc, err := ParseVolumeConf(vf)
	if err != nil {
		t.Fatal(err)
	}
	mf, err := os.Open("testdata/mounts")
	if err != nil {
		t.Fatal(err)
	}
	defer mf.Close()
	mounts := ParseMounts(mf)
	q, err := os.ReadFile("testdata/qcli.txt")
	if err != nil {
		t.Fatal(err)
	}
	return vc, mounts, ParseQcliPools(string(q))
}

func TestResolveFromDeviceFixtures(t *testing.T) {
	vc, mounts, pools := loadFixtures(t)
	vols, skipped := Resolve(vc, mounts, pools)
	want := []Volume{{Name: "DataVol1", Pool: "2", Device: "/dev/mapper/cachedev2", MountPoint: "/share/CACHEDEV2_DATA"}}
	if !reflect.DeepEqual(vols, want) {
		t.Fatalf("volumes = %+v, want %+v", vols, want)
	}
	wantSkipped := []Skipped{{Name: "DataVol2", Reason: "not_mounted"}}
	if !reflect.DeepEqual(skipped, wantSkipped) {
		t.Fatalf("skipped = %+v, want %+v", skipped, wantSkipped)
	}
}

func TestResolveRejectsNonShareMount(t *testing.T) {
	vc := []VolConf{{Name: "Meta", MappingName: "/dev/mapper/vg2-lv1312"}}
	mounts := ParseMounts(strings.NewReader("/dev/mapper/vg2-lv1312 /mnt/pool2 ext4 rw 0 0\n"))
	vols, skipped := Resolve(vc, mounts, nil)
	if len(vols) != 0 {
		t.Fatalf("expected no volumes, got %+v", vols)
	}
	if len(skipped) != 1 || skipped[0].Reason != "not_share_volume" {
		t.Fatalf("skipped = %+v", skipped)
	}
}

func TestParseQcliPools(t *testing.T) {
	_, _, pools := loadFixtures(t)
	if got := pools["/dev/md2"]; got != "2" {
		t.Fatalf("pool for /dev/md2 = %q, want 2 (map=%v)", got, pools)
	}
	if got := pools["DataVol1"]; got != "2" {
		t.Fatalf("pool for DataVol1 = %q, want 2 (map=%v)", got, pools)
	}
}

func TestParseVolumeConfFields(t *testing.T) {
	vc, _, _ := loadFixtures(t)
	if len(vc) != 2 {
		t.Fatalf("got %d volumes, want 2: %+v", len(vc), vc)
	}
	if vc[0].Name != "DataVol1" || vc[0].MappingName != "/dev/mapper/cachedev2" || vc[0].RaidName != "/dev/md2" {
		t.Fatalf("vc[0] = %+v", vc[0])
	}
}
