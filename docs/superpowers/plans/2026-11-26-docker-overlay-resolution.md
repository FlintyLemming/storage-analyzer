# Docker Overlay / Container / Volume Metadata Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically detect Docker data-root directories during filesystem scans, resolve `overlay2/`, `containers/`, and `volumes/` entries from on-disk Docker metadata (including lock-free BoltDB snapshots), persist annotations inside the scan SQLite DB, and render them inline with details in the web UI.

**Architecture:** Add a self-contained `internal/docker` parser using `go.etcd.io/bbolt` on txid-verified snapshot copies, persist results in a backwards-compatible `annotations` table in `internal/scandb`, hook directory observation into `internal/scanner` + `internal/runner`, pass annotations through `internal/api`'s `list` endpoint, and render badges/labels/details/breadcrumbs in `web/`.

**Tech Stack:** Go 1.26 (`CGO_ENABLED=0`), `modernc.org/sqlite`, `go.etcd.io/bbolt`, vanilla JS/CSS (`web/`).

**Spec:** `docs/superpowers/specs/2026-11-26-docker-overlay-resolution-design.md`

## Global Constraints

- Pure on-disk read-only parsing; zero dependency on `docker.sock` or `docker` CLI.
- Never `flock` live Docker BoltDB files; always read via txid-verified temp snapshot copy under `dataDir/run/` with 256 MiB cap and immediate cleanup.
- Never label unmatched entries as `orphan` or verified `unmounted` if any relevant metadata source had a read/parse error.
- Keep `entries` table untouched so `scandb.TestBytesPerEntry` (<70 bytes/entry) and legacy scan DBs remain 100% compatible.
- Both default (`!qnap`) and `-tags qnap` builds must compile cleanly; UI strings updated in both `zh` and `en`.
- Do not touch or revert pre-existing uncommitted files unrelated to this feature.

---

### Task 1: Add `go.etcd.io/bbolt` & Lock-Free BoltDB Snapshot Reader (`internal/docker`)

**Files:**
- Modify: `go.mod`, `go.sum`
- Create: `internal/docker/bolt.go`
- Create: `internal/docker/bolt_test.go`

**Interfaces:**
- Produces:
  - `openBoltSnapshot(dbPath, tmpDir string) (*bbolt.DB, func(), error)` — copies a live BoltDB file without acquiring `flock` on the source, validates meta page `txid` consistency across the copy (retrying up to 5 times), opens the private temp copy read-only, and returns a cleanup func that closes and deletes the temp file.

- [ ] **Step 1: Add `go.etcd.io/bbolt` dependency**
  Run `go get go.etcd.io/bbolt@v1.5.0`.
- [ ] **Step 2: Write failing tests in `internal/docker/bolt_test.go`**
  - `TestOpenBoltSnapshotReadsWhileExclusivelyLocked`: open a real `bbolt.DB` in a temp dir (which holds `flock(LOCK_EX)`!), write buckets/keys, keep it open, and verify `openBoltSnapshot` reads all data immediately without blocking or failing and deletes its temp copy on cleanup.
  - `TestOpenBoltSnapshotRejectsCorruptAndOversized`: verify non-BoltDB garbage returns an error and leaves no temp file behind.
- [ ] **Step 3: Run test to verify it fails**
  Run: `go test ./internal/docker -run TestOpenBoltSnapshot -v`
- [ ] **Step 4: Implement `internal/docker/bolt.go`**
  Implement BoltDB meta-page header decoding (magic `0xED0CDAED`, version `2`, dynamic page size in `[4096, 65536]`, FNV-1a-64a checksum verification matching bbolt's `meta.sum64`), bounded copy (`256<<20` bytes) to `os.CreateTemp(tmpDir, "sa-bolt-*.db")`, `txidBefore == txidAfter` check with retry, and `bbolt.Open(..., ReadOnly: true, NoFreelistSync: true)`.
- [ ] **Step 5: Run tests to verify pass**
  Run: `go test ./internal/docker -v`

---

### Task 2: Docker Data-Root Detection & On-Disk Metadata Parsers (`internal/docker`)

**Files:**
- Create: `internal/docker/types.go`
- Create: `internal/docker/detect.go`
- Create: `internal/docker/parse_containers.go`
- Create: `internal/docker/parse_images.go`
- Create: `internal/docker/parse_buildkit.go`
- Create: `internal/docker/parse_volumes.go`
- Create: `internal/docker/resolve.go`
- Create: `internal/docker/docker_test.go`

**Interfaces:**
- Consumes: `openBoltSnapshot` from Task 1.
- Produces:
  - `TargetKind` constants (`TargetOverlay2`, `TargetContainers`, `TargetVolumes`)
  - `DetectRoot(dir string) bool`
  - `FindRootForTarget(targetPath string) (rootPath, targetKind string, ok bool)`
  - `Annotation` struct (JSON-serializable) & `Type*` constants
  - `RootResult` struct with `DirAnnotation(targetKind string) *Annotation`, `EntryAnnotation(targetKind, name string, isDir bool) *Annotation`, `Warnings(targetKind string) []string`
  - `ResolveRoot(dockerRoot, tmpDir string) (*RootResult, error)`

- [ ] **Step 1: Write failing comprehensive tests in `internal/docker/docker_test.go`**
  Cover:
  - `TestDetectRootAndFindTarget`: valid Docker roots (full, volumes-only with `metadata.db`, containers-only with `config.v2.json`), fake folders named `docker/overlay2` rejected (zero false positives), direct target path `/x/docker/overlay2` resolves parent `/x/docker`.
  - `TestResolveOverlay2ImagesContainersBuildKitOrphans`:
    - Tagged multi-layer image (64-hex `cache-id`), shared base layer across 2 images + 1 dangling `<none>` image, aligned Dockerfile `created_by`.
    - BuildKit-built image layer (25-char `cache-id` in `layerdb`!).
    - Container `mount-id` (`container_rw`) and `init-id` (`container_init`) with running/exited/created status & Compose labels.
    - Container whose image was force-removed (`mounts/<cid>/parent` -> `layerdb` parent chain): ancestor layer is NOT mislabeled as `orphan`!
    - BuildKit `metadata_v2.db` + `snapshots.db` (`committed` indirection where `metadata_v2.db` record ID != `overlay2` dir name!, plus direct `exec.cachemount`, `source.local`, `frontend`, and `cache.deleted=true` ignored).
    - Layer that is both an image layer and BuildKit cache keeps primary type `image_layer` + attaches `buildDesc`.
    - `overlay2/l` and `overlay2/backingFsBlockDev` -> `internal`.
    - Truly unreferenced dir -> `orphan`.
  - `TestResolveContainersAndVolumes`:
    - `containers/<cid>` -> container name, image, status, compose; missing `config.v2.json` -> `orphan`.
    - `volumes/<anon64hex>` -> `volume_anon` with mounted container & destination; unmounted anonymous volume; named volume with Compose labels; `volumes/metadata.db` -> `internal`.
  - `TestPartialMetadataSuppressesOrphanLabel`:
    - Corrupt `buildkit/metadata_v2.db` -> unmatched `overlay2` dir returns `nil` (NOT `orphan`) and `DirAnnotation(TargetOverlay2)` carries `warnings: ["buildkit"]`.
    - Corrupt `containers/<cid>/config.v2.json` -> unmounted volume is NOT marked as verified unmounted / `containers` warning recorded.
- [ ] **Step 2: Run tests to verify failure**
  Run: `go test ./internal/docker -v`
- [ ] **Step 3: Implement `types.go`, `detect.go`, `parse_containers.go`, `parse_images.go`, `parse_buildkit.go`, `parse_volumes.go`, `resolve.go`**
  Follow exact Moby / BuildKit on-disk formats verified in the spec.
- [ ] **Step 4: Run tests to verify pass**
  Run: `go test -race ./internal/docker -v`

---

### Task 3: `scandb` Annotations Persistence (`internal/scandb`)

**Files:**
- Modify: `internal/scandb/schema.go`
- Modify: `internal/scandb/writer.go`
- Modify: `internal/scandb/reader.go`
- Modify: `internal/scandb/scandb_test.go`

**Interfaces:**
- Produces:
  - `scandb.Annotation{Kind string, Data string}`
  - `(*scandb.Writer).AddAnnotation(id int64, kind, data string) error`
  - `(*scandb.DB).Annotations(ids []int64) (map[int64]Annotation, error)`

- [ ] **Step 1: Write failing tests in `internal/scandb/scandb_test.go`**
  - `TestAnnotationsRoundTrip`: write entries + annotations, close (through `VACUUM INTO`), reopen, fetch subset and >500 IDs via `Annotations(ids)`.
  - `TestAnnotationsLegacyDBWithoutTable`: create a raw SQLite DB with only `entries` and `meta` (no `annotations` table), open with `scandb.Open`, verify `Annotations([]int64{1, 2})` returns empty map and `nil` error.
- [ ] **Step 2: Run tests to verify failure**
  Run: `go test ./internal/scandb -run TestAnnotations -v`
- [ ] **Step 3: Implement `annotations` table in `schema.go`, `AddAnnotation` in `writer.go`, and `hasAnnotations` + `Annotations` in `reader.go`**
- [ ] **Step 4: Run all `scandb` tests (including `TestBytesPerEntry`)**
  Run: `go test ./internal/scandb -v`

---

### Task 4: Scanner Directory Hook, Runner Integration & API Extension

**Files:**
- Modify: `internal/scanner/walk.go`
- Modify: `internal/scanner/walk_test.go`
- Modify: `internal/runner/runner.go`
- Modify: `internal/runner/runner_test.go`
- Modify: `internal/api/api.go`
- Modify: `internal/api/api_test.go`

**Interfaces:**
- Produces:
  - `scanner.DirChild{ID int64, Name string, Kind int}`
  - `scanner.Walker.WatchDir func(dirPath string) bool`
  - `scanner.Walker.OnDir func(dirID int64, dirPath string, children []DirChild)`
  - `api.Item.Docker json.RawMessage` (`json:"docker,omitempty"`)

- [ ] **Step 1: Write failing tests**
  - `internal/scanner/walk_test.go`: `TestWalkerOnDirWatchFilter` — verifies `OnDir` fires only for matching directories (including when a root itself matches) and reports exact child IDs, names, and kinds.
  - `internal/runner/runner_test.go`: `TestRunPersistsDockerAnnotations` — builds a realistic Docker root inside a temp volume, runs `runner.Run`, deletes the temp volume, opens the saved scan DB, and verifies `db.Annotations(...)` returns persisted Docker annotations for `overlay2`, `containers`, and `volumes` entries.
  - `internal/api/api_test.go`: `TestListIncludesDockerAnnotations` — writes a scan with annotations on `dir`, `path`, and `children`, calls `GET /api.cgi?action=list`, verifies `docker` appears on annotated entries and is omitted on unannotated entries and legacy DBs.
- [ ] **Step 2: Run tests to verify failure**
  Run: `go test ./internal/scanner ./internal/runner ./internal/api -v`
- [ ] **Step 3: Implement changes in `walk.go`, `runner.go`, and `api.go`**
  - In `runner.go`: wire `WatchDir` (matching `overlay2`/`containers`/`volumes`) and `OnDir`, then resolve each unique detected Docker root once after `walker.Walk` and call `w.AddAnnotation` before `w.Close()`.
  - In `api.go`: batch-fetch `db.Annotations(ids)` for `dir`, `path`, and `children` in `handler.list`, attach `Docker` when `ann.Kind == "docker"` and `json.Valid`.
- [ ] **Step 4: Run tests to verify pass**
  Run: `go test -race ./internal/scanner ./internal/runner ./internal/api -v`

---

### Task 5: Web UI Badges, Details Modal, Breadcrumbs & Bilingual i18n

**Files:**
- Modify: `web/i18n.js`
- Modify: `web/app.js`
- Modify: `web/style.css`

**Interfaces:**
- Consumes: `item.docker` JSON object on `data.dir`, `data.path[]`, and `data.items[]` returned by `action=list`.

- [ ] **Step 1: Add Docker i18n strings (`zh` and `en`) in `web/i18n.js`**
  Add type badges, status labels, detail dialog labels, breadcrumb formatting, copy toast, and partial-metadata warning banner copy for both `zh` and `en`.
- [ ] **Step 2: Implement inline rendering, detail modal, breadcrumbs & warning notice in `web/app.js` and styles in `web/style.css`**
  - `dockerBadge(dk)` / `dockerSummary(dk, rawName)`: concise human-readable label + color-coded type tag + muted raw ID (skipping raw ID when identical to named volume).
  - `showDockerDetail(it)`: opens existing modal dialog with raw ID + Copy button, container info, full list of images (with in-use / dangling badges), Dockerfile `createdBy`, BuildKit cache type & full description, volume mounts & labels.
  - `renderCrumbs()`: show friendly Docker label on annotated breadcrumb items while preserving raw name in `title`.
  - Directory warning notice banner: show above `#list` when `state.dir.docker.warnings` is non-empty.
- [ ] **Step 3: Verify JS syntax and i18n key parity**
  Run node check verifying `web/i18n.js` and `web/app.js` parse cleanly and every new `zh` key exists in `en` and vice versa.

---

### Task 6: Full Verification & Cross-Platform Build Check

- [ ] **Step 1: Run full test suite with race detector**
  Run: `go test -race ./...`
- [ ] **Step 2: Verify both Linux (`!qnap`) and QNAP (`-tags qnap`) builds**
  Run: `CGO_ENABLED=0 go build ./... && CGO_ENABLED=0 go build -tags qnap ./...`
- [ ] **Step 3: Check `git status` and review diff**
  Ensure pre-existing uncommitted files were not clobbered and all new/edited files are clean.
