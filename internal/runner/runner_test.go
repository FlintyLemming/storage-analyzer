package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/FlintyLemming/storage-analyzer/internal/catalog"
	"github.com/FlintyLemming/storage-analyzer/internal/docker"
	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
	"github.com/FlintyLemming/storage-analyzer/internal/volumes"
)

func fakeVolumes(t *testing.T) func() ([]volumes.Volume, []volumes.Skipped, error) {
	t.Helper()
	v1, v2 := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(v1, "a.bin"), make([]byte, 5000), 0o644)
	os.MkdirAll(filepath.Join(v2, "dir"), 0o755)
	os.WriteFile(filepath.Join(v2, "dir", "b.bin"), make([]byte, 100), 0o644)
	return func() ([]volumes.Volume, []volumes.Skipped, error) {
		return []volumes.Volume{
				{Name: "DataVol1", Pool: "1", Device: "/dev/x1", MountPoint: v1, FSType: "ext4"},
				{Name: "DataVol2", Pool: "2", Device: "/dev/x2", MountPoint: v2},
			},
			[]volumes.Skipped{{Name: "Locked", Reason: "not_mounted"}}, nil
	}
}

func opts(t *testing.T, dataDir string) Options {
	return Options{DataDir: dataDir, Trigger: "manual", Discover: fakeVolumes(t), Now: time.Now}
}

func holdLock(t *testing.T, dataDir string) {
	t.Helper()
	os.MkdirAll(filepath.Join(dataDir, "run"), 0o755)
	f, err := os.OpenFile(filepath.Join(dataDir, "run", "scan.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
}

func TestRunProducesDoneScan(t *testing.T) {
	data := t.TempDir()
	id, err := Run(context.Background(), opts(t, data))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	s, err := c.Get(id)
	if err != nil || s.Status != "done" || s.Trigger != "manual" {
		t.Fatalf("scan = %+v err=%v", s, err)
	}
	if s.Files != 2 || s.TotalASize < 5100 || s.DBBytes == 0 {
		t.Fatalf("totals wrong: %+v", s)
	}
	db, err := scandb.Open(catalog.ScanPath(data, id))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots, total, _ := db.Children(0, scandb.ListOpts{Sort: "name"})
	if total != 2 || string(roots[0].Name) != "DataVol1" || string(roots[1].Name) != "DataVol2" {
		t.Fatalf("roots = %+v", roots)
	}
	meta, _ := db.Meta()
	var vols []VolumeMeta
	if err := json.Unmarshal([]byte(meta[MetaVolumes]), &vols); err != nil || len(vols) != 2 {
		t.Fatalf("volumes meta %q: %v", meta[MetaVolumes], err)
	}
	if vols[0].EntryID != 1 || vols[0].Pool != "1" || vols[0].FSTotal == 0 {
		t.Fatalf("vols[0] = %+v", vols[0])
	}
	if vols[0].FSType != "ext4" || !strings.Contains(meta[MetaVolumes], `"fsType":"ext4"`) {
		t.Fatalf("fsType not carried: %q", meta[MetaVolumes])
	}
	if vols[1].FSType != "" || strings.Count(meta[MetaVolumes], "fsType") != 1 {
		t.Fatalf("empty fsType should be omitted: %q", meta[MetaVolumes])
	}
	var skipped []volumes.Skipped
	if err := json.Unmarshal([]byte(meta[MetaSkipped]), &skipped); err != nil || len(skipped) != 1 {
		t.Fatalf("skipped meta %q: %v", meta[MetaSkipped], err)
	}
	if _, err := os.Stat(filepath.Join(data, "run", "progress.json")); !os.IsNotExist(err) {
		t.Fatal("progress.json left behind")
	}
}

func TestSecondScanSkipped(t *testing.T) {
	data := t.TempDir()
	holdLock(t, data)
	if _, err := Run(context.Background(), opts(t, data)); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	if list, _ := c.List(); len(list) != 0 {
		t.Fatalf("catalog has %d rows", len(list))
	}
}

func TestRunCanceled(t *testing.T) {
	data := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	id, err := Run(ctx, opts(t, data))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	c, _ := catalog.Open(data)
	defer c.Close()
	if s, _ := c.Get(id); s.Status != "canceled" {
		t.Fatalf("status = %q", s.Status)
	}
	matches, _ := filepath.Glob(filepath.Join(data, "scans", "*"))
	if len(matches) != 0 {
		t.Fatalf("leftover files: %v", matches)
	}
}

func TestCleanupStaleTmp(t *testing.T) {
	data := t.TempDir()
	os.MkdirAll(filepath.Join(data, "scans"), 0o755)
	tmp := filepath.Join(data, "scans", "scan-9.db.tmp")
	os.WriteFile(tmp, []byte("x"), 0o644)
	if err := CleanupStaleTmp(data); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("tmp file not removed")
	}
}

func TestCleanupStaleTmpKeepsRunningScan(t *testing.T) {
	data := t.TempDir()
	os.MkdirAll(filepath.Join(data, "scans"), 0o755)
	tmp := filepath.Join(data, "scans", "scan-9.db.tmp")
	os.WriteFile(tmp, []byte("x"), 0o644)
	holdLock(t, data)
	CleanupStaleTmp(data)
	if _, err := os.Stat(tmp); err != nil {
		t.Fatal("tmp of a running scan must not be removed")
	}
}

func TestRunPrunes(t *testing.T) {
	data := t.TempDir()
	c, _ := catalog.Open(data)
	s, _ := c.Settings()
	s.Retention = 2
	if err := c.SaveSettings(s); err != nil {
		t.Fatal(err)
	}
	c.Close()
	o := opts(t, data)
	for i := 0; i < 3; i++ {
		if _, err := Run(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
	dbs, _ := filepath.Glob(filepath.Join(data, "scans", "*.db"))
	if len(dbs) != 2 {
		t.Fatalf("db files = %v", dbs)
	}
}

func TestRunningIdle(t *testing.T) {
	_, running, err := Running(t.TempDir())
	if err != nil || running {
		t.Fatalf("running=%v err=%v", running, err)
	}
}

func TestRunningReadsProgress(t *testing.T) {
	data := t.TempDir()
	holdLock(t, data)
	p := Progress{ScanID: 7, PID: 123, Trigger: "manual", Files: 42}
	b, _ := json.Marshal(p)
	os.WriteFile(filepath.Join(data, "run", "progress.json"), b, 0o644)
	got, running, err := Running(data)
	if err != nil || !running || got.ScanID != 7 || got.Files != 42 {
		t.Fatalf("got=%+v running=%v err=%v", got, running, err)
	}
}

func TestRunNilDiscover(t *testing.T) {
	data := t.TempDir()
	for name, run := range map[string]func() error{
		"Run": func() error {
			_, err := Run(context.Background(), Options{DataDir: data, Trigger: "manual"})
			return err
		},
		"RunScheduled": func() error {
			_, err := RunScheduled(context.Background(), Options{DataDir: data})
			return err
		},
	} {
		err := run()
		if err == nil || err.Error() != "runner: Options.Discover is nil" {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	cat, err := catalog.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	if list, _ := cat.List(); len(list) != 0 {
		t.Fatalf("scan records created: %v", list)
	}
}

func TestStatVolumes(t *testing.T) {
	dir := t.TempDir()
	got := StatVolumes([]volumes.Volume{
		{Name: "/data", Pool: "1", Device: "/dev/sda1", MountPoint: dir, FSType: "ext4"},
		{Name: "/missing", Device: "/dev/sdb1", MountPoint: filepath.Join(dir, "nope"), FSType: "xfs"},
	})
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].EntryID != 0 || got[0].Name != "/data" || got[0].Pool != "1" || got[0].Device != "/dev/sda1" || got[0].MountPoint != dir || got[0].FSType != "ext4" || got[0].FSTotal == 0 {
		t.Fatalf("got[0] = %+v", got[0])
	}
	if got[1].EntryID != 0 || got[1].Name != "/missing" || got[1].FSTotal != 0 {
		t.Fatalf("got[1] = %+v", got[1])
	}
	empty := StatVolumes(nil)
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty = %#v, want non-nil empty slice", empty)
	}
}

func TestRunUsesIsMountPoint(t *testing.T) {
	data := t.TempDir()
	vol := t.TempDir()
	os.MkdirAll(filepath.Join(vol, "skipme"), 0o755)
	os.WriteFile(filepath.Join(vol, "skipme", "hidden.bin"), make([]byte, 4096), 0o644)
	os.WriteFile(filepath.Join(vol, "keep.bin"), make([]byte, 128), 0o644)

	o := Options{
		DataDir: data,
		Trigger: "manual",
		Discover: func() ([]volumes.Volume, []volumes.Skipped, error) {
			return []volumes.Volume{{Name: vol, MountPoint: vol}}, nil, nil
		},
		IsMountPoint: func(path string, parentDev, dev uint64) bool {
			return filepath.Base(path) == "skipme"
		},
		Now: time.Now,
	}
	id, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	db, err := scandb.Open(catalog.ScanPath(data, id))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	children, _, err := db.Children(1, scandb.ListOpts{Sort: "name"})
	if err != nil {
		t.Fatal(err)
	}
	var gotSkip, gotKeep bool
	for _, c := range children {
		switch string(c.Name) {
		case "skipme":
			gotSkip = true
			if c.Kind != scandb.KindMount || c.NChild != 0 {
				t.Fatalf("skipme = %+v, want skipped KindMount", c)
			}
		case "keep.bin":
			gotKeep = true
			if c.Kind != scandb.KindFile || c.ASize != 128 {
				t.Fatalf("keep.bin = %+v, want KindFile 128", c)
			}
		}
	}
	if !gotSkip || !gotKeep {
		t.Fatalf("missing expected entries: %+v", children)
	}
}

func TestRunPersistsDockerAnnotations(t *testing.T) {
	data := t.TempDir()
	vol := t.TempDir()
	dkRoot := filepath.Join(vol, "appdata", "docker")

	// Minimal real Docker layout: 1 image layer, 1 container RW layer, 1 BuildKit committed snapshot, 1 anonymous volume.
	sum := sha256.Sum256([]byte("app-layer"))
	diffID := "sha256:" + hex.EncodeToString(sum[:])
	imgSum := sha256.Sum256([]byte("img-app"))
	imgID := "sha256:" + hex.EncodeToString(imgSum[:])
	imgCacheID := "t8zqkoht5zlcn2c5zobnuc780"
	cid := strings.Repeat("1", 64)
	mountID := strings.Repeat("c", 64)
	anonVol := strings.Repeat("9", 64)
	bkDirID := "b6qvr82r6qipmllaa5c4sw2hw"

	writeF := func(rel, content string) {
		p := filepath.Join(dkRoot, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeF("image/overlay2/repositories.json", `{"Repositories":{"demo":{"demo:v1":"`+imgID+`"}}}`)
	writeF("image/overlay2/imagedb/content/sha256/"+hex.EncodeToString(imgSum[:]),
		`{"rootfs":{"type":"layers","diff_ids":["`+diffID+`"]},"history":[{"created_by":"RUN echo hi"}]}`)
	writeF("image/overlay2/layerdb/sha256/"+hex.EncodeToString(sum[:])+"/cache-id", imgCacheID)
	writeF("image/overlay2/layerdb/mounts/"+cid+"/mount-id", mountID)
	writeF("containers/"+cid+"/config.v2.json", `{
		"ID":"`+cid+`","Name":"/demo-app","Image":"`+imgID+`",
		"Config":{"Image":"demo:v1"},
		"State":{"Running":true,"StartedAt":"2026-10-01T00:00:00Z"},
		"MountPoints":{"/data":{"Name":"`+anonVol+`","Destination":"/data","Type":"volume","RW":true}}
	}`)

	// BuildKit BoltDBs.
	os.MkdirAll(filepath.Join(dkRoot, "buildkit"), 0o755)
	bkMeta, err := bolt.Open(filepath.Join(dkRoot, "buildkit", "metadata_v2.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = bkMeta.Update(func(tx *bolt.Tx) error {
		m, _ := tx.CreateBucketIfNotExists([]byte("_main"))
		r, _ := m.CreateBucketIfNotExists([]byte("rec0000000000000000000001"))
		_ = r.Put([]byte("cache.description"), []byte(`{"value":"RUN make build"}`))
		_ = r.Put([]byte("cache.recordType"), []byte(`{"value":"regular"}`))
		return nil
	})
	bkMeta.Close()

	bkSnap, err := bolt.Open(filepath.Join(dkRoot, "buildkit", "snapshots.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = bkSnap.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("rec0000000000000000000001"))
		_ = b.Put([]byte("committed"), []byte(bkDirID))
		return nil
	})
	bkSnap.Close()

	// Actual scanned dirs.
	writeF("overlay2/"+imgCacheID+"/diff/app.bin", "hello")
	writeF("overlay2/"+mountID+"/diff/log.txt", "log")
	writeF("overlay2/"+bkDirID+"/diff/cache.o", "obj")
	writeF("volumes/"+anonVol+"/_data/db.sqlite", "db")

	o := Options{
		DataDir: data,
		Trigger: "manual",
		Discover: func() ([]volumes.Volume, []volumes.Skipped, error) {
			return []volumes.Volume{{Name: "Vol", MountPoint: vol}}, nil, nil
		},
		Now: time.Now,
	}
	id, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}

	// Remove source Docker dir completely to prove annotations were persisted in scandb.
	if err := os.RemoveAll(vol); err != nil {
		t.Fatal(err)
	}

	db, err := scandb.Open(catalog.ScanPath(data, id))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	findChild := func(parent int64, name string) scandb.Entry {
		t.Helper()
		kids, _, err := db.Children(parent, scandb.ListOpts{Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range kids {
			if string(k.Name) == name {
				return k
			}
		}
		t.Fatalf("child %q not found under %d", name, parent)
		return scandb.Entry{}
	}

	appdata := findChild(1, "appdata")
	dk := findChild(appdata.ID, "docker")
	ov := findChild(dk.ID, "overlay2")
	imgE := findChild(ov.ID, imgCacheID)
	rwE := findChild(ov.ID, mountID)
	bkE := findChild(ov.ID, bkDirID)
	conts := findChild(dk.ID, "containers")
	cE := findChild(conts.ID, cid)
	vols := findChild(dk.ID, "volumes")
	vE := findChild(vols.ID, anonVol)

	anns, err := db.Annotations([]int64{ov.ID, imgE.ID, rwE.ID, bkE.ID, cE.ID, vE.ID})
	if err != nil {
		t.Fatal(err)
	}
	decode := func(id int64) docker.Annotation {
		t.Helper()
		a, ok := anns[id]
		if !ok || a.Kind != "docker" {
			t.Fatalf("missing docker annotation for id %d (got %+v)", id, a)
		}
		var out docker.Annotation
		if err := json.Unmarshal([]byte(a.Data), &out); err != nil {
			t.Fatalf("bad annotation json %q: %v", a.Data, err)
		}
		return out
	}
	if a := decode(ov.ID); a.Type != docker.TypeDirOverlay2 {
		t.Fatalf("ov = %+v", a)
	}
	if a := decode(imgE.ID); a.Type != docker.TypeImageLayer || a.Label != "demo:v1" {
		t.Fatalf("imgE = %+v", a)
	}
	if a := decode(rwE.ID); a.Type != docker.TypeContainerRW || a.Label != "demo-app" {
		t.Fatalf("rwE = %+v", a)
	}
	if a := decode(bkE.ID); a.Type != docker.TypeBuildCache || !strings.Contains(a.BuildDesc, "make build") {
		t.Fatalf("bkE = %+v", a)
	}
	if a := decode(cE.ID); a.Type != docker.TypeContainer || a.Label != "demo-app" {
		t.Fatalf("cE = %+v", a)
	}
	if a := decode(vE.ID); a.Type != docker.TypeVolumeAnon || a.Label != "demo-app" {
		t.Fatalf("vE = %+v", a)
	}
}
