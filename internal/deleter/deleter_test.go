package deleter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FlintyLemming/storage-analyzer/internal/scandb"
)

func TestDefaultPolicyLinux(t *testing.T) {
	p := DefaultPolicy("linux", "/var/lib/storage-analyzer", "/etc/storage-analyzer")
	blocked := map[string]int{
		"/":                                   0,
		"/mnt/data":                           0, // depth 0 volume root
		"/etc":                                1,
		"/etc/hosts":                          2,
		"/etc/storage-analyzer/config":        3,
		"/usr":                                1,
		"/usr/bin/ls":                         3,
		"/boot":                               1,
		"/boot/vmlinuz":                       2,
		"/bin":                                1,
		"/lib64/libc.so.6":                    2,
		"/proc/1":                             2,
		"/sys/fs":                             2,
		"/dev/null":                           2,
		"/run/systemd":                        2,
		"/var":                                1, // ancestor of dataDir AND exact deny
		"/var/lib":                            2, // ancestor of dataDir AND exact deny
		"/var/lib/storage-analyzer":           3,
		"/var/lib/storage-analyzer/catalog.db": 4,
		"/home":                               1, // exact deny
		"/tmp":                                1, // exact deny
		"/var/log":                            2, // exact deny
		"/opt":                                1, // exact deny
		"/srv":                                1, // exact deny
	}
	for path, depth := range blocked {
		if reason := p.Check(path, depth); reason == "" {
			t.Errorf("Linux Check(%q, %d) = allowed, want blocked", path, depth)
		}
	}

	allowed := map[string]int{
		"/home/alice/movie.mkv":         3,
		"/tmp/old-build":                2,
		"/var/log/app.log":              3,
		"/var/cache/apt/pkg.deb":        4,
		"/mnt/data/Movies/old.mkv":      2,
		"/srv/media/show":               2,
		"/opt/old-app":                  2,
		"/var/lib/storage-analyzer-bak": 3, // sibling prefix must NOT be blocked
	}
	for path, depth := range allowed {
		if reason := p.Check(path, depth); reason != "" {
			t.Errorf("Linux Check(%q, %d) = %q, want allowed", path, depth, reason)
		}
	}
}

func TestDefaultPolicyQNAP(t *testing.T) {
	p := DefaultPolicy("qnap", "/share/CACHEDEV1_DATA/.qpkg/StorageAnalyzer/data", "/share/CACHEDEV1_DATA/.qpkg/StorageAnalyzer")
	blocked := map[string]int{
		"/share/CACHEDEV1_DATA":                                   0,
		"/share/CACHEDEV1_DATA/.qpkg":                             1,
		"/share/CACHEDEV1_DATA/.qpkg/StorageAnalyzer":             2,
		"/share/CACHEDEV1_DATA/.qpkg/StorageAnalyzer/data/cat.db": 4,
		"/share/CACHEDEV1_DATA/@Recycle":                          1,
		"/share/CACHEDEV1_DATA/.system":                           1,
	}
	for path, depth := range blocked {
		if reason := p.Check(path, depth); reason == "" {
			t.Errorf("QNAP Check(%q, %d) = allowed, want blocked", path, depth)
		}
	}
	allowed := map[string]int{
		"/share/CACHEDEV1_DATA/Multimedia/movie.mkv":  2,
		"/share/CACHEDEV1_DATA/@Recycle/old.iso":      2, // inside @Recycle is allowed
		"/share/CACHEDEV1_DATA/.qpkg/OldRemovedApp":   2, // other qpkg dir allowed if admin chooses
		"/share/CACHEDEV2_DATA/Downloads/archive.zip": 2,
	}
	for path, depth := range allowed {
		if reason := p.Check(path, depth); reason != "" {
			t.Errorf("QNAP Check(%q, %d) = %q, want allowed", path, depth, reason)
		}
	}
}

func makeTestScanDB(t *testing.T, volMount string) (*scandb.DB, map[int64]VolumeRoot) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "scan.db")
	w, err := scandb.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	entries := []scandb.Entry{
		{ID: 1, Parent: 0, Name: []byte("DataVol1"), Kind: scandb.KindDir, DSize: 1000, ASize: 1000, Items: 7, FirstChild: 2, NChild: 4},
		{ID: 2, Parent: 1, Name: []byte("dir"), Kind: scandb.KindDir, DSize: 500, ASize: 500, Items: 2, FirstChild: 6, NChild: 2},
		{ID: 3, Parent: 1, Name: []byte("mnt"), Kind: scandb.KindMount},
		{ID: 4, Parent: 1, Name: []byte("overlay2"), Kind: scandb.KindDir, DSize: 300, ASize: 300, Items: 1, FirstChild: 8, NChild: 1},
		{ID: 5, Parent: 1, Name: []byte("bad\xffname"), Kind: scandb.KindFile, DSize: 200, ASize: 200, Flags: scandb.FlagBadName},
		{ID: 6, Parent: 2, Name: []byte("child.bin"), Kind: scandb.KindFile, DSize: 400, ASize: 400},
		{ID: 7, Parent: 2, Name: []byte("other.bin"), Kind: scandb.KindFile, DSize: 100, ASize: 100},
		{ID: 8, Parent: 4, Name: []byte("abc123layer"), Kind: scandb.KindDir, DSize: 300, ASize: 300, Items: 1, FirstChild: 9, NChild: 1},
		{ID: 9, Parent: 8, Name: []byte("diff.dat"), Kind: scandb.KindFile, DSize: 300, ASize: 300},
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.AddAnnotation(4, "docker", `{"type":"dir_overlay2"}`)
	_ = w.AddAnnotation(8, "docker", `{"type":"image_layer","label":"nginx:latest"}`)
	_ = w.SetMeta(scandb.MetaRoots, "1")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := scandb.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	roots := map[int64]VolumeRoot{1: {EntryID: 1, Name: "DataVol1", MountPoint: volMount}}
	return db, roots
}

func TestResolveTargetsFoldingDockerAndProtection(t *testing.T) {
	vol := t.TempDir()
	db, roots := makeTestScanDB(t, vol)
	pol := DefaultPolicy("linux", filepath.Join(t.TempDir(), "data"), "")

	// Pass dir(2), its child(6), badName(5), docker child(9), and missing(999).
	res, err := ResolveTargets(db, roots, pol, []int64{2, 6, 5, 9, 999, 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.Folded != 1 {
		t.Fatalf("Folded = %d, want 1", res.Folded)
	}
	if res.Missing != 1 {
		t.Fatalf("Missing = %d, want 1", res.Missing)
	}
	if len(res.Targets) != 3 {
		t.Fatalf("len(Targets) = %d, want 3: %+v", len(res.Targets), res.Targets)
	}
	if !res.HasDockerRisk || res.HasProtected {
		t.Fatalf("HasDockerRisk=%v HasProtected=%v", res.HasDockerRisk, res.HasProtected)
	}
	// TotalDSize: dir(500) + badname(200) + diff.dat(300) = 1000
	// TotalItems: dir(1+2=3) + badname(1) + diff.dat(1) = 5
	if res.TotalDSize != 1000 || res.TotalItems != 5 {
		t.Fatalf("totals dsize=%d items=%d", res.TotalDSize, res.TotalItems)
	}
	byID := map[int64]Target{}
	for _, tg := range res.Targets {
		byID[tg.EntryID] = tg
	}
	if !byID[5].BadName || byID[5].RawPath != filepath.Join(vol, "bad\xffname") || !strings.Contains(byID[5].Path, "\ufffd") {
		t.Fatalf("badName target = %+v", byID[5])
	}
	if !byID[9].DockerRisk || byID[9].DockerType != "image_layer" {
		t.Fatalf("docker descendant target = %+v", byID[9])
	}
	if string(byID[2].RawDisplayPath) != "DataVol1/dir" {
		t.Fatalf("RawDisplayPath = %q", byID[2].RawDisplayPath)
	}

	// Volume root(1) and mount point(3) must be flagged Protected.
	res2, err := ResolveTargets(db, roots, pol, []int64{1, 3})
	if err != nil {
		t.Fatal(err)
	}
	if !res2.HasProtected || len(res2.Targets) != 2 {
		t.Fatalf("protected res = %+v", res2)
	}
	for _, tg := range res2.Targets {
		if !tg.Protected || tg.ProtectReason == "" {
			t.Fatalf("expected protected target: %+v", tg)
		}
	}
}

func TestRemoveTargetFilesDirsSymlinksAndBadName(t *testing.T) {
	vol := t.TempDir()
	// 1. Single file
	f1 := filepath.Join(vol, "hello.txt")
	if err := os.WriteFile(f1, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := RemoveTarget(context.Background(), Target{
		EntryID: 10, Kind: scandb.KindFile, VolMount: vol, RelParts: []string{"hello.txt"}, RawPath: f1,
	}, RemoveCallbacks{})
	if !st.Complete || st.Files != 1 || st.Errors != 0 {
		t.Fatalf("file remove stats = %+v", st)
	}
	if _, err := os.Lstat(f1); !os.IsNotExist(err) {
		t.Fatalf("f1 still exists: %v", err)
	}

	// 2. Non-UTF-8 raw filename
	badName := "raw\xfffile"
	fBad := vol + "/" + badName
	if err := os.WriteFile(fBad, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	st = RemoveTarget(context.Background(), Target{
		EntryID: 11, Kind: scandb.KindFile, VolMount: vol, RelParts: []string{badName}, RawPath: fBad,
	}, RemoveCallbacks{})
	if !st.Complete || st.Files != 1 || st.Errors != 0 {
		t.Fatalf("badName remove stats = %+v", st)
	}

	// 3. Symlink leaf: must remove symlink ONLY, NOT target!
	outside := filepath.Join(t.TempDir(), "precious.txt")
	if err := os.WriteFile(outside, []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(vol, "link-to-outside")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	st = RemoveTarget(context.Background(), Target{
		EntryID: 12, Kind: scandb.KindSymlink, VolMount: vol, RelParts: []string{"link-to-outside"}, RawPath: link,
	}, RemoveCallbacks{})
	if !st.Complete || st.Files != 1 || st.Errors != 0 {
		t.Fatalf("symlink remove stats = %+v", st)
	}
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Fatal("symlink not removed")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "keep me" {
		t.Fatalf("symlink target damaged: %q %v", data, err)
	}

	// 4. Nested directory containing files and a symlink to outside dir
	outsideDir := t.TempDir()
	outsideKeep := filepath.Join(outsideDir, "safe.txt")
	os.WriteFile(outsideKeep, []byte("safe"), 0o644)
	dir := filepath.Join(vol, "tree")
	os.MkdirAll(filepath.Join(dir, "sub", "deep"), 0o755)
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("1234"), 0o644)
	os.WriteFile(filepath.Join(dir, "sub", "deep", "b.txt"), []byte("56789"), 0o644)
	os.Symlink(outsideDir, filepath.Join(dir, "sub", "escape-link"))

	st = RemoveTarget(context.Background(), Target{
		EntryID: 13, Kind: scandb.KindDir, VolMount: vol, RelParts: []string{"tree"}, RawPath: dir,
	}, RemoveCallbacks{})
	if !st.Complete || st.Files != 3 || st.Dirs != 3 || st.Errors != 0 {
		t.Fatalf("dir remove stats = %+v", st)
	}
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Fatal("dir still exists")
	}
	if _, err := os.Stat(outsideKeep); err != nil {
		t.Fatalf("symlinked dir contents damaged: %v", err)
	}

	// 5. Idempotent ENOENT
	st = RemoveTarget(context.Background(), Target{
		EntryID: 14, Kind: scandb.KindFile, VolMount: vol, RelParts: []string{"already-gone"}, RawPath: filepath.Join(vol, "already-gone"),
	}, RemoveCallbacks{})
	if !st.Complete || st.Errors != 0 {
		t.Fatalf("ENOENT stats = %+v", st)
	}
}

func TestRemoveTargetRefusesIntermediateSymlinkAndMountBoundary(t *testing.T) {
	vol := t.TempDir()
	outside := t.TempDir()
	victim := filepath.Join(outside, "victim.txt")
	os.WriteFile(victim, []byte("must survive"), 0o644)
	// Intermediate directory swapped with symlink to outside
	os.Symlink(outside, filepath.Join(vol, "swapped"))

	var gotErr error
	st := RemoveTarget(context.Background(), Target{
		EntryID: 20, Kind: scandb.KindFile, VolMount: vol, RelParts: []string{"swapped", "victim.txt"}, RawPath: filepath.Join(vol, "swapped", "victim.txt"),
	}, RemoveCallbacks{
		OnError: func(_ string, err error) { gotErr = err },
	})
	if st.Complete || st.Errors == 0 || gotErr == nil {
		t.Fatalf("intermediate symlink not refused: stats=%+v err=%v", st, gotErr)
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("victim was deleted through symlink: %v", err)
	}

	// Mount boundary inside directory must NOT be descended or deleted.
	tree := filepath.Join(vol, "with-mount")
	mntSub := filepath.Join(tree, "mounted")
	os.MkdirAll(mntSub, 0o755)
	os.WriteFile(filepath.Join(tree, "sibling.txt"), []byte("del"), 0o644)
	os.WriteFile(filepath.Join(mntSub, "keep.txt"), []byte("keep"), 0o644)

	var errs []string
	st = RemoveTarget(context.Background(), Target{
		EntryID: 21, Kind: scandb.KindDir, VolMount: vol, RelParts: []string{"with-mount"}, RawPath: tree,
	}, RemoveCallbacks{
		IsMountPoint: func(path string, _, _ uint64) bool { return filepath.Clean(path) == filepath.Clean(mntSub) },
		OnError:      func(path string, err error) { errs = append(errs, path+": "+err.Error()) },
	})
	if st.Complete || st.Errors == 0 || len(errs) == 0 {
		t.Fatalf("mount boundary should prevent full deletion: stats=%+v errs=%v", st, errs)
	}
	if _, err := os.Stat(filepath.Join(mntSub, "keep.txt")); err != nil {
		t.Fatalf("file inside mount point was deleted: %v", err)
	}
	// Sibling file outside mount point was deleted.
	if _, err := os.Stat(filepath.Join(tree, "sibling.txt")); !os.IsNotExist(err) {
		t.Fatalf("sibling.txt should have been deleted: %v", err)
	}
}

func TestRemoveTargetCancelStopsEarly(t *testing.T) {
	vol := t.TempDir()
	dir := filepath.Join(vol, "many")
	os.MkdirAll(dir, 0o755)
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x"), 0o644)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var gotErr error
	st := RemoveTarget(ctx, Target{
		EntryID: 30, Kind: scandb.KindDir, VolMount: vol, RelParts: []string{"many"}, RawPath: dir,
	}, RemoveCallbacks{
		OnError: func(_ string, err error) { gotErr = err },
	})
	if st.Complete || !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("canceled remove: stats=%+v err=%v", st, gotErr)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.txt")); err != nil {
		t.Fatalf("file deleted despite pre-canceled ctx: %v", err)
	}
}

func TestRemoveTargetLargeDirMultiBatch(t *testing.T) {
	vol := t.TempDir()
	dir := filepath.Join(vol, "bigdir")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	const count = 650 // exceeds readdirBatchSize (512)
	for i := 0; i < count; i++ {
		p := filepath.Join(dir, "f-"+string(rune('a'+i%26))+"-"+filepath.Base(t.Name())+string(rune('0'+i/100))+string(rune('0'+(i/10)%10))+string(rune('0'+i%10)))
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	st := RemoveTarget(context.Background(), Target{
		EntryID: 40, Kind: scandb.KindDir, VolMount: vol, RelParts: []string{"bigdir"}, RawPath: dir,
	}, RemoveCallbacks{})
	if !st.Complete || st.Files != count || st.Dirs != 1 || st.Errors != 0 {
		t.Fatalf("large dir stats = %+v, want Complete=true Files=%d Dirs=1", st, count)
	}
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Fatalf("bigdir still exists: %v", err)
	}
}
