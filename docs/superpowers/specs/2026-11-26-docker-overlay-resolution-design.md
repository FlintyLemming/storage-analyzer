# Docker Overlay / Container / Volume Metadata Resolution Design

**Date:** 2026-11-26
**Status:** Approved for implementation

## Goal

During a storage scan, automatically discover Docker data-root directories (whether at `/var/lib/docker`, a custom `data-root` such as `/mnt/extend/appdata/docker`, or QNAP Container Station roots), resolve opaque directories under `overlay2/`, `containers/`, and `volumes/` into human-readable attributions (image layers, container layers, BuildKit build cache, volumes, internal entries, or unreferenced orphans), persist the resolved results directly inside the scan SQLite database, and display them inline in the web UI with expandable details.

## Global Constraints

- **Persisted at scan time:** Resolution runs during `scan` and is saved in the scan SQLite database; opening a scan result never queries Docker or re-reads the host filesystem.
- **Pure on-disk, read-only:** Zero dependency on `docker.sock` or the `docker` CLI. Must work under the Linux systemd hardening (`User=storage-analyzer`, `AmbientCapabilities=CAP_DAC_READ_SEARCH`, `ProtectSystem=strict`, `StateDirectory=storage-analyzer`) and on QNAP QTS (`CGO_ENABLED=0`, `linux/amd64`, `linux/arm64`, `linux/arm`).
- **Non-blocking BoltDB reads:** Never acquire `flock` on live Docker `.db` files (`dockerd` holds `LOCK_EX` while running). Always copy a txid-verified snapshot to a temporary file inside `dataDir/run/` and read the copy with `go.etcd.io/bbolt` in read-only mode.
- **No false orphan labels:** Never label unmatched entries as `orphan` or `unmounted` if any relevant metadata source failed to read or parse; surface a directory-level partial-metadata warning instead.
- **Backwards compatibility:** Existing scan SQLite databases without the `annotations` table must continue to open and render seamlessly. For non-Docker entries, `entries` row size and JSON responses remain unchanged.
- **Bilingual UI:** All user-facing strings in `web/i18n.js` cover both `zh` and `en` (QNAP & Linux builds).

## 1. Docker Data-Root Discovery & Resolution (`internal/docker`)

### 1.1 Data-Root Detection

A directory `R` is recognized as a Docker data-root iff:
1. It contains at least one target directory (`overlay2`, `containers`, or `volumes`), **and**
2. It exhibits at least one Docker structural fingerprint:
   - `image/overlay2/repositories.json`, `image/overlay2/imagedb`, or `image/overlay2/layerdb`
   - `volumes/metadata.db`
   - `buildkit/metadata_v2.db` or `buildkit/snapshots.db`
   - At least one `<64-hex>/config.v2.json` inside `containers/`

When a scanned volume root itself is `overlay2`, `containers`, or `volumes` (for example `paths = /mnt/extend/appdata/docker/overlay2`), the resolver checks `filepath.Dir(targetPath)` as the candidate Docker data-root `R`.

### 1.2 Lock-Free Consistent BoltDB Reader

Live Docker BoltDB files (`buildkit/metadata_v2.db`, `buildkit/snapshots.db`, `volumes/metadata.db`) are exclusively locked by `dockerd`, and `metadata_v2.db` can easily exceed 1–2 GB on busy hosts due to `_external` blobs. To read them safely without blocking and with zero temp disk copies:
1. Open the source file read-only (`os.Open`, no `flock`).
2. Read meta pages 0 and 1 via `ReadAt` (`pread`) using the page size stored in page 0's meta header (validated power of 2 in `[4096, 65536]`, probing standard sizes if page 0 is invalid). Validate BoltDB magic `0xED0CDAED`, version `2`, and FNV-1a-64a checksum. Pick the valid meta page with the higher `txid` (`metaBefore`).
3. Traverse only the required BoltDB B+tree buckets directly via `ReadAt` (for `metadata_v2.db`, only `_main`, skipping `_external` and `_index`), supporting branch pages, leaf pages, multi-page overflow leaves, and inline sub-buckets, with strict bounds/depth/page-count guards.
4. Re-read meta pages 0 and 1 after traversal (`metaAfter`). If `metaAfter.txid == metaBefore.txid`, the read is consistent; if a write committed concurrently, reset state and retry up to 5 times with short backoff.

### 1.3 Metadata Sources & Classification

For each discovered Docker root `R`, parse once and track per-source status (`ok`, `absent`, `error`):

1. **Containers (`containers/<cid>/config.v2.json`)**:
   - Read `Name` (trim leading `/`), `ID`, `Image` (sha256 image ID), `Config.Image`, `Config.Labels` (`com.docker.compose.project`, `com.docker.compose.service`), `State` (`Running`, `Paused`, `Restarting`, `Dead`, `StartedAt` -> status `running|paused|restarting|dead|created|exited`), and `MountPoints`.
   - Annotates `containers/<cid>` as `container`. A 64-hex directory under `containers/` missing `config.v2.json` (with no read error) is annotated `orphan`.

2. **Container Mount Layers (`image/overlay2/layerdb/mounts/<cid>/`)**:
   - `mount-id` -> `overlay2/<mount-id>` as `container_rw`
   - `init-id` -> `overlay2/<init-id>` as `container_init`
   - `parent` -> record chainID; walk `layerdb/sha256/<chainID>/parent` upward to mark all ancestor layers as actively used by that container (protects layers of running/stopped containers whose image was force-removed).

3. **Images & Image Layers (`image/overlay2/`)**:
   - `repositories.json` -> maps `imageID` to tagged refs (`repo:tag`) and digest refs (`repo@sha256:...`).
   - `imagedb/content/sha256/<hex>` -> `rootfs.diff_ids` (base→top) and `history` (non-empty entries aligned 1:1 with `diff_ids` when lengths match to extract `created_by`).
   - Compute deterministic chain IDs: `chainID[0] = diffID[0]`, `chainID[i] = "sha256:" + hex(sha256(chainID[i-1] + " " + diffID[i]))`.
   - `layerdb/sha256/<chainID-hex>/cache-id` -> maps `overlay2/<cache-id>` to the chainID (covers both 64-hex pulled layers and 25-char BuildKit-built image layers).
   - Annotates `overlay2/<cache-id>` as `image_layer` with all referencing images (sorted: tagged first, then digest-only, then dangling `<none>` with short ID; plus `inUse` flag if any container uses that image), optional Dockerfile `createdBy`, and count.
   - If a `layerdb` layer has no image in `imagedb` but IS in a container's `mounts/<cid>/parent` chain, annotate it as `image_layer` referencing the container's image/name rather than `orphan`.

4. **BuildKit Build Cache (`buildkit/metadata_v2.db` + `buildkit/snapshots.db`)**:
   - From `metadata_v2.db`, iterate `_main/<recordID>`: decode each key's JSON `{"value": ...}` (`cache.description`, `cache.recordType`, `cache.snapshot`, `cache.chainID`, `cache.deleted`). Skip records where `cache.deleted == true`.
   - From `snapshots.db`, iterate top-level buckets `<snapKey>`: read `committed` (the actual `overlay2/<graphDriverID>` directory name!), `chainid`, `parent`, and `iscommitted`.
   - Map each BuildKit cache record to `overlay2/<dir>` via:
     - `snapKey = cache.snapshot` if set, else `recordID`
     - If `snapshots.db[snapKey].committed` is set -> `overlay2/<committed>`
     - Else if `snapshots.db[snapKey].chainid` or `cache.chainID` is set -> `layerdb[chainid].cacheID`
     - Else -> `overlay2/<snapKey>`
   - Also mark any `snapshots.db` bucket key (and its `committed` target) as tracked by BuildKit even if missing from `metadata_v2.db`.
   - Precedence: if an `overlay2/<id>` is already an `image_layer` (or `container_rw` / `container_init`), keep its primary type and optionally attach `buildDesc` as supplementary info; otherwise annotate as `build_cache` with `cacheType` (`regular`, `exec.cachemount`, `source.local`, `frontend`, etc.) and `buildDesc`.

5. **Volumes (`volumes/` + `volumes/metadata.db` + container `MountPoints`)**:
   - Special entries `metadata.db` and `backingFsBlockDev` -> `internal`.
   - Cross-reference container `MountPoints` (where `Name != ""` or `Type == "volume"`) to attach all mounted containers (`container`, `destination`, `rw`, `status`).
   - Read `volumes/metadata.db` bucket `volumes` (`Name`, `Driver`, `Labels`, `Options`).
   - Classify anonymous vs named: `volume_anon` if label `com.docker.volume.anonymous` exists OR (64-char lowercase hex name AND no `com.docker.compose.volume` label AND not explicitly named); otherwise `volume_named`.
   - Unmounted state (`mounts` empty) is only considered verified unmounted when `containers` parsed cleanly.

6. **Special `overlay2` Entries & Orphans**:
   - `overlay2/l` and `overlay2/backingFsBlockDev` -> `internal`.
   - Any remaining child of `overlay2/` not matched above:
     - If all of `imagedb`, `layerdb`, `mounts`, `containers`, and (`buildkit` if `buildkit/` exists) succeeded without error -> annotate as `orphan`.
     - If any source errored -> do **not** annotate as `orphan`; record `warnings` on the `overlay2` directory's own annotation (`dir_overlay2`).

## 2. Scan Pipeline & SQLite Persistence (`internal/scandb`, `internal/scanner`, `internal/runner`)

### 2.1 `scandb` Schema & API

Add one table to `schemaSQL` in `internal/scandb/schema.go`:

```sql
CREATE TABLE annotations(
  id   INTEGER PRIMARY KEY,
  kind TEXT    NOT NULL,
  data TEXT    NOT NULL
);
```

- `Writer.AddAnnotation(entryID int64, kind, data string) error`: buffers and writes rows into `annotations` before `Close()` commits and runs `VACUUM INTO`.
- `DB.Open(path)`: checks `sqlite_master` once for table `annotations` (`hasAnnotations bool`) for seamless backwards compatibility with existing scan DBs.
- `DB.Annotations(ids []int64) (map[int64]Annotation, error)`: returns annotations for the given IDs (chunked `IN (...)` query, returning empty map immediately if `!d.hasAnnotations` or `len(ids) == 0`).

### 2.2 Scanner Hook & Runner Integration

In `internal/scanner/walk.go`:
- Add lightweight optional fields on `Walker`:
  ```go
  WatchDir func(dirPath string) bool
  OnDir    func(dirID int64, dirPath string, children []DirChild)
  ```
  where `type DirChild struct { ID int64; Name string; Kind int }`.
- Inside `walkState.list(n)`, only when `w.OnDir != nil && (w.WatchDir == nil || w.WatchDir(n.path))` is a `[]DirChild` slice built from the already-iterated children and passed to `w.OnDir(n.id, n.path, children)`. For non-matching directories (99.999% of directories), cost is a single `filepath.Base` check and zero allocations.

In `internal/runner/runner.go`:
- Wire `WatchDir` to match directory basenames `"overlay2"`, `"containers"`, `"volumes"`.
- Collect observed `(targetDirID, targetDirPath, children)` into a mutex-protected slice.
- After `walker.Walk` succeeds and before `w.Close()`:
  - Group targets by their validated Docker data-root (`docker.FindRootForTarget`).
  - Resolve each unique Docker root once using `docker.ResolveRoot(rootPath, filepath.Join(o.DataDir, "run"))`.
  - Write annotations for each target directory (`dir_overlay2` / `dir_containers` / `dir_volumes`) and each resolved child via `w.AddAnnotation(id, "docker", jsonStr)`.
  - Log any non-fatal metadata warnings; never abort the scan due to Docker metadata errors.

## 3. Backend API Extension (`internal/api`)

- Add `Docker json.RawMessage `json:"docker,omitempty"`` to `api.Item`.
- In `handler.list`:
  - Collect entry IDs for `dir` (when `dirID != 0`), `path`, and `children`.
  - Fetch `anns, err := db.Annotations(ids)` in a single query.
  - Populate `item.Docker` whenever an annotation with `Kind == "docker"` exists and is valid JSON.

## 4. Web UI & i18n (`web/app.js`, `web/i18n.js`, `web/style.css`)

- **Inline row display (`rowFor`)**:
  - When `it.docker` is present (for child types, not `dir_*`):
    - Render a compact color-coded type tag (`容器读写层`, `容器初始化层`, `容器`, `镜像层`, `构建缓存`, `匿名卷`, `命名卷`, `Docker 内部`, `未引用层`).
    - Render the resolved primary label prominently (e.g. `7806-claude-directory-gallery:latest 等 3 个`, `jellyfin · 运行中`, `postgres → /var/lib/postgresql/data`, `mount /root/.cache/...`), followed by the original raw ID in muted monospace when the folder name is an opaque hash (omitted when redundant for named volumes).
    - Render a small `ⓘ` detail trigger button (stops row click propagation so clicking the row still drills into the directory).
- **Detail modal (`showDockerDetail`)**:
  - Reuses the existing `.dialog` modal.
  - Displays: type badge, raw folder name with copy button, container info (name, ID, image, status, Compose project/service), full list of sharing images (tagged/digest/dangling + in-use indicator), Dockerfile `createdBy` instruction, BuildKit cache type & description, volume driver/labels & full mount target list.
- **Breadcrumbs (`renderCrumbs`)**:
  - Entries in `state.path` with a child `docker` annotation display a compact friendly name alongside or replacing the raw hash (with raw name in `title`).
- **Partial-metadata directory banner**:
  - When `state.dir.docker` has `warnings`, show a subtle `.notice` banner above the directory table explaining which metadata source could not be read during the scan and that unmatched entries were not marked as orphaned.

## 5. Testing & Verification

- **`internal/docker`**: Unit tests with synthetic Docker trees and real `bbolt` databases covering all layer/container/volume/BuildKit types, `snapshots.db` `committed` indirection, BuildKit 25-char image layers, force-removed-image container parent chain protection, `flock(LOCK_EX)` contention snapshot safety, partial-metadata anti-misclassification, and false-positive rejection.
- **`internal/scandb`**: Round-trip annotation write/read, `Annotations(ids)` batching, backwards compatibility opening a DB created without the `annotations` table, `TestBytesPerEntry` unchanged.
- **`internal/scanner`**: `WatchDir`/`OnDir` delivery for root and nested dirs, zero calls for non-matching dirs.
- **`internal/runner`**: End-to-end scan of a temp volume containing a realistic Docker root (including BoltDBs), verifying persisted annotations in the resulting `.db` file (even after deleting the source Docker dir before reading!).
- **`internal/api`**: `action=list` includes `docker` on `items`, `path`, and `dir`; omits `docker` on ordinary entries and legacy scans.
- **Build & verification**: `go test ./...`, `go build ./...`, `go build -tags qnap ./...`, and JS syntax + i18n key parity checks.
