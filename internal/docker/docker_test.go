package docker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func chainID(diffIDs ...string) string {
	cur := diffIDs[0]
	for _, d := range diffIDs[1:] {
		cur = "sha256:" + sha256Hex(cur+" "+d)
	}
	return cur
}

func createBolt(t *testing.T, path string, fn func(tx *bolt.Tx) error) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(fn); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func bkVal(v any) []byte {
	raw, _ := json.Marshal(v)
	b, _ := json.Marshal(map[string]any{"value": json.RawMessage(raw)})
	return b
}

func TestDetectRootAndFindTarget(t *testing.T) {
	base := t.TempDir()

	// Fake folder named docker/overlay2 without any Docker fingerprint must be rejected.
	fake := filepath.Join(base, "fake-docker")
	os.MkdirAll(filepath.Join(fake, "overlay2", "abc"), 0o755)
	os.MkdirAll(filepath.Join(fake, "volumes", "myvol"), 0o755)
	if DetectRoot(fake) {
		t.Fatal("DetectRoot returned true for fake directory without Docker metadata")
	}
	if _, _, ok := FindRootForTarget(filepath.Join(fake, "overlay2")); ok {
		t.Fatal("FindRootForTarget accepted fake overlay2")
	}

	// Valid Docker root with image/overlay2/layerdb.
	real1 := filepath.Join(base, "appdata", "docker")
	os.MkdirAll(filepath.Join(real1, "overlay2"), 0o755)
	os.MkdirAll(filepath.Join(real1, "image", "overlay2", "layerdb"), 0o755)
	if !DetectRoot(real1) {
		t.Fatal("DetectRoot returned false for valid docker root")
	}
	root, kind, ok := FindRootForTarget(filepath.Join(real1, "overlay2"))
	if !ok || root != real1 || kind != TargetOverlay2 {
		t.Fatalf("FindRootForTarget = (%q, %q, %v), want (%q, %q, true)", root, kind, ok, real1, TargetOverlay2)
	}

	// Valid Docker root with only volumes/metadata.db.
	real2 := filepath.Join(base, "vol-only")
	createBolt(t, filepath.Join(real2, "volumes", "metadata.db"), func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("volumes"))
		return err
	})
	root, kind, ok = FindRootForTarget(filepath.Join(real2, "volumes"))
	if !ok || root != real2 || kind != TargetVolumes {
		t.Fatalf("FindRootForTarget volumes = (%q, %q, %v)", root, kind, ok)
	}

	// Non-target child name under valid Docker root returns false.
	if _, _, ok := FindRootForTarget(filepath.Join(real1, "image")); ok {
		t.Fatal("FindRootForTarget should ignore non-target dir 'image'")
	}
}

func buildFullFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	// Diff IDs and chain IDs for 3 images:
	// - img1 (nginx:latest, also digest ref): baseDiff -> nginxDiff
	// - img2 (7806-gallery:latest, BuildKit 25-char cache-id!): baseDiff -> bkImgDiff
	// - img3 (dangling <none>): baseDiff -> dangDiff
	// - removedImg layer (only referenced by a live container's mounts/<cid>/parent!): baseDiff -> removedDiff
	baseDiff := "sha256:" + sha256Hex("base-layer")
	nginxDiff := "sha256:" + sha256Hex("nginx-layer")
	bkImgDiff := "sha256:" + sha256Hex("bk-img-layer")
	dangDiff := "sha256:" + sha256Hex("dangling-layer")
	removedDiff := "sha256:" + sha256Hex("removed-img-layer")

	baseChain := chainID(baseDiff)
	nginxChain := chainID(baseDiff, nginxDiff)
	bkImgChain := chainID(baseDiff, bkImgDiff)
	dangChain := chainID(baseDiff, dangDiff)
	removedChain := chainID(baseDiff, removedDiff)

	baseCacheID := strings.Repeat("a", 64)
	nginxCacheID := strings.Repeat("b", 64)
	bkImgCacheID := "t8zqkoht5zlcn2c5zobnuc780" // 25-char BuildKit-built image layer!
	dangCacheID := strings.Repeat("d", 64)
	removedCacheID := strings.Repeat("e", 64)

	img1ID := "sha256:" + sha256Hex("img1")
	img2ID := "sha256:" + sha256Hex("img2")
	img3ID := "sha256:" + sha256Hex("img3")

	// repositories.json
	mustWrite(t, filepath.Join(root, "image", "overlay2", "repositories.json"), `{
		"Repositories": {
			"nginx": {
				"nginx:latest": "`+img1ID+`",
				"nginx@sha256:1111111111111111111111111111111111111111111111111111111111111111": "`+img1ID+`"
			},
			"7806-gallery": {
				"7806-gallery:latest": "`+img2ID+`"
			}
		}
	}`)

	// imagedb
	writeImg := func(id string, diffs []string, history []map[string]any) {
		hexID := strings.TrimPrefix(id, "sha256:")
		b, _ := json.Marshal(map[string]any{
			"rootfs":  map[string]any{"type": "layers", "diff_ids": diffs},
			"history": history,
		})
		mustWrite(t, filepath.Join(root, "image", "overlay2", "imagedb", "content", "sha256", hexID), string(b))
	}
	writeImg(img1ID, []string{baseDiff, nginxDiff}, []map[string]any{
		{"created_by": "ADD debian-rootfs.tar.xz /"},
		{"created_by": "ENV NGINX_VERSION=1.27", "empty_layer": true},
		{"created_by": "RUN apt-get install -y nginx"},
	})
	writeImg(img2ID, []string{baseDiff, bkImgDiff}, []map[string]any{
		{"created_by": "ADD debian-rootfs.tar.xz /"},
		{"created_by": "RUN npm ci && npm run build"},
	})
	writeImg(img3ID, []string{baseDiff, dangDiff}, []map[string]any{
		{"created_by": "ADD debian-rootfs.tar.xz /"},
		{"created_by": "RUN echo dangling"},
	})

	// layerdb/sha256
	writeLayer := func(ch, parentCh, cacheID string) {
		dir := filepath.Join(root, "image", "overlay2", "layerdb", "sha256", strings.TrimPrefix(ch, "sha256:"))
		mustWrite(t, filepath.Join(dir, "cache-id"), cacheID)
		if parentCh != "" {
			mustWrite(t, filepath.Join(dir, "parent"), parentCh)
		}
	}
	writeLayer(baseChain, "", baseCacheID)
	writeLayer(nginxChain, baseChain, nginxCacheID)
	writeLayer(bkImgChain, baseChain, bkImgCacheID)
	writeLayer(dangChain, baseChain, dangCacheID)
	writeLayer(removedChain, baseChain, removedCacheID)

	// Containers:
	// - c1 (web-proxy, running, using img1, mounts anon vol + named vol)
	// - c2 (orphan-img-app, exited, its image was force-removed, mounts removedChain)
	c1ID := strings.Repeat("1", 64)
	c2ID := strings.Repeat("2", 64)
	c1MountID := strings.Repeat("c", 64)
	c1InitID := c1MountID + "-init"
	c2MountID := strings.Repeat("f", 64)

	anonVol := strings.Repeat("9", 64)
	unmountedAnonVol := strings.Repeat("8", 64)
	namedVol := "pgdata"

	mustWrite(t, filepath.Join(root, "image", "overlay2", "layerdb", "mounts", c1ID, "mount-id"), c1MountID)
	mustWrite(t, filepath.Join(root, "image", "overlay2", "layerdb", "mounts", c1ID, "init-id"), c1InitID)
	mustWrite(t, filepath.Join(root, "image", "overlay2", "layerdb", "mounts", c1ID, "parent"), nginxChain)

	mustWrite(t, filepath.Join(root, "image", "overlay2", "layerdb", "mounts", c2ID, "mount-id"), c2MountID)
	mustWrite(t, filepath.Join(root, "image", "overlay2", "layerdb", "mounts", c2ID, "parent"), removedChain)

	c1Cfg, _ := json.Marshal(map[string]any{
		"ID":    c1ID,
		"Name":  "/web-proxy",
		"Image": img1ID,
		"Config": map[string]any{
			"Image": "nginx:latest",
			"Labels": map[string]string{
				"com.docker.compose.project": "edge",
				"com.docker.compose.service": "proxy",
			},
		},
		"State": map[string]any{
			"Running":   true,
			"StartedAt": "2026-10-01T12:00:00Z",
		},
		"MountPoints": map[string]any{
			"/var/cache/nginx": map[string]any{
				"Name":        anonVol,
				"Destination": "/var/cache/nginx",
				"Type":        "volume",
				"RW":          true,
			},
			"/var/lib/postgresql/data": map[string]any{
				"Name":        namedVol,
				"Destination": "/var/lib/postgresql/data",
				"Type":        "volume",
				"RW":          false,
			},
		},
	})
	mustWrite(t, filepath.Join(root, "containers", c1ID, "config.v2.json"), string(c1Cfg))

	c2Cfg, _ := json.Marshal(map[string]any{
		"ID":    c2ID,
		"Name":  "/legacy-worker",
		"Image": "sha256:" + sha256Hex("deleted-image"),
		"Config": map[string]any{
			"Image": "old-app:v1",
		},
		"State": map[string]any{
			"Running":   false,
			"StartedAt": "2026-09-01T00:00:00Z",
		},
	})
	mustWrite(t, filepath.Join(root, "containers", c2ID, "config.v2.json"), string(c2Cfg))

	// Orphan container directory without config.v2.json
	orphanCID := strings.Repeat("7", 64)
	os.MkdirAll(filepath.Join(root, "containers", orphanCID), 0o755)

	// Volumes metadata.db
	createBolt(t, filepath.Join(root, "volumes", "metadata.db"), func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("volumes"))
		if err != nil {
			return err
		}
		putVol := func(name, driver string, labels map[string]string) {
			v, _ := json.Marshal(map[string]any{"Name": name, "Driver": driver, "Labels": labels})
			_ = b.Put([]byte(name), v)
		}
		putVol(anonVol, "local", map[string]string{"com.docker.volume.anonymous": ""})
		putVol(unmountedAnonVol, "local", map[string]string{"com.docker.volume.anonymous": ""})
		putVol(namedVol, "local", map[string]string{
			"com.docker.compose.project": "edge",
			"com.docker.compose.volume":  "pgdata",
		})
		return nil
	})

	// BuildKit metadata_v2.db and snapshots.db:
	// - recCommitted ("rec1111111111111111111111") -> snapshots.db committed -> "b6qvr82r6qipmllaa5c4sw2hw" (indirection!)
	// - recCacheMount ("cm22222222222222222222222") -> direct overlay2 dir (mutable cache mount)
	// - recDeleted ("del3333333333333333333333") -> cache.deleted=true (must be ignored!)
	// - recSharedWithImg ("img4444444444444444444444") -> cache.chainID = bkImgChain (layer is already image_layer!)
	createBolt(t, filepath.Join(root, "buildkit", "metadata_v2.db"), func(tx *bolt.Tx) error {
		mainB, err := tx.CreateBucketIfNotExists([]byte("_main"))
		if err != nil {
			return err
		}
		putRec := func(id string, kv map[string]any) {
			sb, _ := mainB.CreateBucketIfNotExists([]byte(id))
			for k, v := range kv {
				_ = sb.Put([]byte(k), bkVal(v))
			}
		}
		putRec("rec1111111111111111111111", map[string]any{
			"cache.description": "mount / from exec /bin/sh -c cargo build --release",
			"cache.recordType":  "regular",
		})
		putRec("cm22222222222222222222222", map[string]any{
			"cache.description": "shared cache mount /root/.cache/pip",
			"cache.recordType":  "exec.cachemount",
		})
		putRec("del3333333333333333333333", map[string]any{
			"cache.description": "deleted build step",
			"cache.recordType":  "regular",
			"cache.deleted":     true,
		})
		putRec("img4444444444444444444444", map[string]any{
			"cache.description": "mount / from exec npm ci",
			"cache.recordType":  "regular",
			"cache.chainID":     bkImgChain,
		})
		return nil
	})
	createBolt(t, filepath.Join(root, "buildkit", "snapshots.db"), func(tx *bolt.Tx) error {
		b1, _ := tx.CreateBucketIfNotExists([]byte("rec1111111111111111111111"))
		_ = b1.Put([]byte("committed"), []byte("b6qvr82r6qipmllaa5c4sw2hw"))
		b2, _ := tx.CreateBucketIfNotExists([]byte("b6qvr82r6qipmllaa5c4sw2hw"))
		_ = b2.Put([]byte("iscommitted"), []byte{})
		return nil
	})

	// Create actual overlay2 and volume dirs on disk.
	for _, d := range []string{
		baseCacheID, nginxCacheID, bkImgCacheID, dangCacheID, removedCacheID,
		c1MountID, c1InitID, c2MountID,
		"b6qvr82r6qipmllaa5c4sw2hw", "cm22222222222222222222222",
		"del3333333333333333333333", "orphan1234567890abcdef123", "l",
	} {
		os.MkdirAll(filepath.Join(root, "overlay2", d), 0o755)
	}
	mustWrite(t, filepath.Join(root, "overlay2", "backingFsBlockDev"), "")
	for _, v := range []string{anonVol, unmountedAnonVol, namedVol} {
		os.MkdirAll(filepath.Join(root, "volumes", v, "_data"), 0o755)
	}
	return root
}

func TestResolveOverlay2ImagesContainersBuildKitOrphans(t *testing.T) {
	root := buildFullFixture(t)
	res, err := ResolveRoot(root, t.TempDir())
	if err != nil {
		t.Fatalf("ResolveRoot failed: %v", err)
	}
	if w := res.Warnings(TargetOverlay2); len(w) != 0 {
		t.Fatalf("unexpected overlay2 warnings: %v", w)
	}

	// 1. Shared base layer: 2 tagged images + 1 dangling image (digest ref deduped when tag exists).
	baseAnn := res.EntryAnnotation(TargetOverlay2, strings.Repeat("a", 64), true)
	if baseAnn == nil || baseAnn.Type != TypeImageLayer {
		t.Fatalf("base layer = %+v", baseAnn)
	}
	if baseAnn.Count != 3 || len(baseAnn.Images) != 3 {
		t.Fatalf("base layer images = %+v (count=%d)", baseAnn.Images, baseAnn.Count)
	}
	// Tagged + in-use image (nginx:latest) should come first!
	if baseAnn.Images[0].Name != "nginx:latest" || !baseAnn.Images[0].InUse {
		t.Fatalf("first image = %+v, want nginx:latest inUse=true", baseAnn.Images[0])
	}
	if baseAnn.Images[1].Name != "7806-gallery:latest" {
		t.Fatalf("second image = %+v, want 7806-gallery:latest", baseAnn.Images[1])
	}
	if !baseAnn.Images[2].Dangling {
		t.Fatalf("third image = %+v, want dangling", baseAnn.Images[2])
	}
	if !strings.Contains(baseAnn.CreatedBy, "debian-rootfs.tar.xz") {
		t.Fatalf("base CreatedBy = %q", baseAnn.CreatedBy)
	}

	// 2. BuildKit-built 25-char image layer keeps primary type image_layer AND carries buildDesc.
	bkImgAnn := res.EntryAnnotation(TargetOverlay2, "t8zqkoht5zlcn2c5zobnuc780", true)
	if bkImgAnn == nil || bkImgAnn.Type != TypeImageLayer || bkImgAnn.Label != "7806-gallery:latest" {
		t.Fatalf("bkImg = %+v", bkImgAnn)
	}
	if !strings.Contains(bkImgAnn.CreatedBy, "npm ci") || !strings.Contains(bkImgAnn.BuildDesc, "npm ci") {
		t.Fatalf("bkImg details: createdBy=%q buildDesc=%q", bkImgAnn.CreatedBy, bkImgAnn.BuildDesc)
	}

	// 3. Container RW and init layers.
	rwAnn := res.EntryAnnotation(TargetOverlay2, strings.Repeat("c", 64), true)
	if rwAnn == nil || rwAnn.Type != TypeContainerRW || rwAnn.Container == nil || rwAnn.Container.Name != "web-proxy" || rwAnn.Container.Status != "running" {
		t.Fatalf("rwAnn = %+v", rwAnn)
	}
	if rwAnn.Container.Compose == nil || rwAnn.Container.Compose.Project != "edge" || rwAnn.Container.Compose.Service != "proxy" {
		t.Fatalf("rwAnn compose = %+v", rwAnn.Container.Compose)
	}
	initAnn := res.EntryAnnotation(TargetOverlay2, strings.Repeat("c", 64)+"-init", true)
	if initAnn == nil || initAnn.Type != TypeContainerInit || initAnn.Container.Name != "web-proxy" {
		t.Fatalf("initAnn = %+v", initAnn)
	}

	// 4. Force-removed image layer still referenced by exited container's mount parent chain must NOT be orphan!
	remAnn := res.EntryAnnotation(TargetOverlay2, strings.Repeat("e", 64), true)
	if remAnn == nil || remAnn.Type != TypeImageLayer {
		t.Fatalf("removed-image live container layer = %+v, must NOT be orphan", remAnn)
	}
	if len(remAnn.Images) == 0 || !remAnn.Images[0].InUse || remAnn.Images[0].Name != "old-app:v1" {
		t.Fatalf("removed-image layer images = %+v", remAnn.Images)
	}

	// 5. BuildKit committed snapshot indirection (metadata_v2 record -> snapshots.db committed -> overlay2 dir).
	bkCommit := res.EntryAnnotation(TargetOverlay2, "b6qvr82r6qipmllaa5c4sw2hw", true)
	if bkCommit == nil || bkCommit.Type != TypeBuildCache || bkCommit.CacheType != "regular" || !strings.Contains(bkCommit.BuildDesc, "cargo build") {
		t.Fatalf("bkCommit = %+v", bkCommit)
	}

	// 6. BuildKit direct cache mount.
	bkMount := res.EntryAnnotation(TargetOverlay2, "cm22222222222222222222222", true)
	if bkMount == nil || bkMount.Type != TypeBuildCache || bkMount.CacheType != "exec.cachemount" || !strings.Contains(bkMount.BuildDesc, "/root/.cache/pip") {
		t.Fatalf("bkMount = %+v", bkMount)
	}

	// 7. Deleted BuildKit record + truly unreferenced dir -> orphan.
	delAnn := res.EntryAnnotation(TargetOverlay2, "del3333333333333333333333", true)
	if delAnn == nil || delAnn.Type != TypeOrphan {
		t.Fatalf("deleted buildkit record = %+v, want orphan", delAnn)
	}
	orphAnn := res.EntryAnnotation(TargetOverlay2, "orphan1234567890abcdef123", true)
	if orphAnn == nil || orphAnn.Type != TypeOrphan {
		t.Fatalf("orphan = %+v, want orphan", orphAnn)
	}

	// 8. Internal overlay2 entries.
	lAnn := res.EntryAnnotation(TargetOverlay2, "l", true)
	if lAnn == nil || lAnn.Type != TypeInternal || lAnn.Note != "l" {
		t.Fatalf("l = %+v", lAnn)
	}
	devAnn := res.EntryAnnotation(TargetOverlay2, "backingFsBlockDev", false)
	if devAnn == nil || devAnn.Type != TypeInternal || devAnn.Note != "backingFsBlockDev" {
		t.Fatalf("backingFsBlockDev = %+v", devAnn)
	}
}

func TestResolveContainersAndVolumes(t *testing.T) {
	root := buildFullFixture(t)
	res, err := ResolveRoot(root, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Container directory.
	c1 := res.EntryAnnotation(TargetContainers, strings.Repeat("1", 64), true)
	if c1 == nil || c1.Type != TypeContainer || c1.Container.Name != "web-proxy" || c1.Container.Status != "running" {
		t.Fatalf("c1 = %+v", c1)
	}
	c2 := res.EntryAnnotation(TargetContainers, strings.Repeat("2", 64), true)
	if c2 == nil || c2.Type != TypeContainer || c2.Container.Name != "legacy-worker" || c2.Container.Status != "exited" {
		t.Fatalf("c2 = %+v", c2)
	}
	cOrph := res.EntryAnnotation(TargetContainers, strings.Repeat("7", 64), true)
	if cOrph == nil || cOrph.Type != TypeOrphan {
		t.Fatalf("cOrph = %+v, want orphan", cOrph)
	}

	// Mounted anonymous volume.
	vAnon := res.EntryAnnotation(TargetVolumes, strings.Repeat("9", 64), true)
	if vAnon == nil || vAnon.Type != TypeVolumeAnon || vAnon.Unmounted || len(vAnon.Mounts) != 1 {
		t.Fatalf("vAnon = %+v", vAnon)
	}
	if vAnon.Mounts[0].Container != "web-proxy" || vAnon.Mounts[0].Destination != "/var/cache/nginx" || !vAnon.Mounts[0].RW {
		t.Fatalf("vAnon mount = %+v", vAnon.Mounts[0])
	}

	// Unmounted anonymous volume.
	vUnm := res.EntryAnnotation(TargetVolumes, strings.Repeat("8", 64), true)
	if vUnm == nil || vUnm.Type != TypeVolumeAnon || !vUnm.Unmounted || len(vUnm.Mounts) != 0 {
		t.Fatalf("vUnm = %+v", vUnm)
	}

	// Mounted named volume with compose labels.
	vNamed := res.EntryAnnotation(TargetVolumes, "pgdata", true)
	if vNamed == nil || vNamed.Type != TypeVolumeNamed || vNamed.Unmounted || vNamed.Driver != "local" {
		t.Fatalf("vNamed = %+v", vNamed)
	}
	if vNamed.Compose == nil || vNamed.Compose.Project != "edge" || vNamed.Compose.Volume != "pgdata" {
		t.Fatalf("vNamed compose = %+v", vNamed.Compose)
	}
	if len(vNamed.Mounts) != 1 || vNamed.Mounts[0].RW {
		t.Fatalf("vNamed mounts = %+v", vNamed.Mounts)
	}

	// volumes/metadata.db internal file.
	vMeta := res.EntryAnnotation(TargetVolumes, "metadata.db", false)
	if vMeta == nil || vMeta.Type != TypeInternal || vMeta.Note != "metadata.db" {
		t.Fatalf("vMeta = %+v", vMeta)
	}
}

func TestPartialMetadataSuppressesOrphanAndUnmountedClaims(t *testing.T) {
	root := buildFullFixture(t)
	// Corrupt buildkit/metadata_v2.db and one container config.v2.json.
	mustWrite(t, filepath.Join(root, "buildkit", "metadata_v2.db"), "corrupt-bolt")
	mustWrite(t, filepath.Join(root, "containers", strings.Repeat("2", 64), "config.v2.json"), "{bad-json")

	res, err := ResolveRoot(root, t.TempDir())
	if err != nil {
		t.Fatalf("ResolveRoot must degrade gracefully, got error: %v", err)
	}

	// Unmatched overlay2 dir MUST NOT be labeled orphan when buildkit failed to parse!
	if got := res.EntryAnnotation(TargetOverlay2, "orphan1234567890abcdef123", true); got != nil {
		t.Fatalf("unmatched overlay2 dir got %+v, want nil (anti-misclassification)", got)
	}
	dirOv := res.DirAnnotation(TargetOverlay2)
	if dirOv == nil || dirOv.Type != TypeDirOverlay2 || len(dirOv.Warnings) == 0 {
		t.Fatalf("dirOv = %+v, want warnings", dirOv)
	}

	// Unmounted volume MUST NOT claim Unmounted=true when containers had a parse error!
	vUnm := res.EntryAnnotation(TargetVolumes, strings.Repeat("8", 64), true)
	if vUnm == nil || vUnm.Type != TypeVolumeAnon || vUnm.Unmounted {
		t.Fatalf("vUnm = %+v, want Unmounted=false when container parsing was partial", vUnm)
	}
	dirVol := res.DirAnnotation(TargetVolumes)
	if dirVol == nil || len(dirVol.Warnings) == 0 {
		t.Fatalf("dirVol = %+v, want containers warning", dirVol)
	}

	// Known good entries (like nginx layer and c1 container) must STILL resolve accurately!
	if got := res.EntryAnnotation(TargetOverlay2, strings.Repeat("b", 64), true); got == nil || got.Type != TypeImageLayer {
		t.Fatalf("known good nginx layer = %+v", got)
	}
}
