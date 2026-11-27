# File & Directory Deletion Design (with Cross-View Delete Basket)

**Date:** 2026-11-26
**Status:** Approved for implementation

## 1. Goal

Add permanent file and directory deletion to Storage Analyzer across both QNAP QTS and generic Linux builds, supporting single-item quick delete as well as a persistent cross-directory / cross-view **Delete Basket** (收集箱) usable from both the Space view and the Duplicates view. Deletion runs as a detached background job reusing the existing `runner` lock/progress/cancel pipeline, and immediately updates `scan-<id>.db`, `dups-<id>.db`, and `catalog.db` so freed space and remaining duplicates appear right away without requiring a full rescan.

## 2. Global Constraints

- **Out-of-the-box parity on QNAP and Linux:** Both platforms enable web deletion by default after normal installation and authentication. Linux additionally provides `allow_delete = true|false` (default `true`) in `/etc/storage-analyzer/config` for read-only deployments.
- **Non-root Linux daemon with kernel OS protection:** Linux stays on `User=storage-analyzer` with `NoNewPrivileges=yes`, `ProtectSystem=full`, `ReadOnlyPaths=-/bin -/sbin -/lib -/lib64`, and `AmbientCapabilities`/`CapabilityBoundingSet` set to `CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE CAP_FOWNER`. Never runs as UID 0; `/usr`, `/boot`, `/efi`, `/etc`, and non-usrmerge system dirs remain kernel read-only.
- **No new third-party dependencies:** Pure Go (`CGO_ENABLED=0`), standard library + existing `golang.org/x/sys/unix` and `modernc.org/sqlite`. Must compile for `linux/amd64`, `linux/arm64`, `linux/arm` (GOARM=7), and `darwin` (dev), both with and without `-tags qnap`.
- **Entry-ID addressing only:** API clients never pass arbitrary filesystem paths; all targets are resolved from `scan-<id>.db` entry IDs + `MetaVolumes` mount points using lossless raw byte names.
- **Permanent deletion (no recycle bin):** Files and directories are unlinked directly to immediately reclaim disk space.
- **Strict SQLite `?mode=ro` compatibility:** `scan-<id>.db` and `dups-<id>.db` must remain in rollback (`DELETE`) journal mode after in-place updates so existing read-only opens never fail or require `-shm`/`-wal` files.
- **Strict CSP & bilingual UI:** No inline HTML event handlers or unsafe inline styles; full `zh`, `en`, `zhLinux`, `enLinux` i18n parity in `web/i18n.js`.

## 3. Permissions & Safety Guardrails

### 3.1 Platform Privileges

- **QNAP (`-tags qnap`):** CGI runs via `_thttpd_` as `root`, authenticated by `QTSAuth` requiring `isAdmin == 1`. Deletion is always enabled (`deleteEnabled: true`).
- **Linux (`!qnap`):**
  - `packaging/linux/storage-analyzer.service`:
    - `ProtectSystem=full` + `ReadOnlyPaths=-/bin -/sbin -/lib -/lib64`
    - `AmbientCapabilities=CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE CAP_FOWNER`
    - `CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE CAP_FOWNER`
    - `CAP_DAC_OVERRIDE` allows unlinking files/directories owned by other users or `root`; `CAP_FOWNER` allows unlinking in sticky-bit (`+t`) directories such as `/tmp` and `/var/tmp`.
    - `mounts.Select` and `mounts.NewBoundaryChecker` already ignore same-device RO bind mounts created by `ProtectSystem=full` (verified by `systemd-full.txt` fixture).
  - `/etc/storage-analyzer/config`: adds optional `allow_delete = true|false` (default `true`). When `false`, `deleteEnabled` is `false`, API rejects delete endpoints with `403 delete_disabled`, and the UI hides all deletion controls.

### 3.2 Application-Level Guardrails (`internal/deleter`)

Enforced on **both** platforms before preview confirmation and again inside the delete worker before any syscall:

1. **Latest scan only:** Target `scanId` must equal the most recent `status = 'done'` scan in `catalog.db`. Historical snapshots are strictly read-only (`409 not_latest_scan`).
2. **No volume roots or mount points:** Entries with `Parent == 0` or `Kind == KindMount` are refused (`403 protected_path`).
3. **Component sanity:** Every path component from `scandb` must be non-empty, not `.` or `..`, and contain neither `/` nor `\x00`.
4. **Two-tier protected path policy (`Policy`):**
   - **Subtree-protected** (cannot delete the path, any descendant, OR any ancestor directory containing it):
     - Both platforms: `dataDir` and `configDir` (Linux) / `installRoot` (QNAP), checking both cleaned and symlink-resolved paths.
     - Linux: `/etc`, `/usr`, `/boot`, `/efi`, `/bin`, `/sbin`, `/lib`, `/lib64`, `/proc`, `/sys`, `/dev`, `/run`.
   - **Exact-protected** (cannot delete the directory itself, CAN delete entries inside it):
     - Always: `/`.
     - Linux: `/home`, `/root`, `/tmp`, `/var`, `/var/lib`, `/var/log`, `/var/cache`, `/var/tmp`, `/opt`, `/srv`, `/mnt`, `/media`.
     - QNAP: depth-1 reserved directories directly under a volume root: `.qpkg`, `@Recycle`, `.system`.
5. **Docker risk detection:** If any entry along `db.Path(entryID)` (self or ancestor) has a `docker` annotation, the target is marked `DockerRisk = true`. `delete_start` requires `confirmDocker: true` whenever any resolved target has `DockerRisk` (`400 docker_confirm_required`).
6. **Ancestor-descendant folding:** If a request includes both a directory and any descendant inside it, the descendant is folded into the ancestor so targets are never deleted or subtracted twice.

## 4. Safe Deletion Engine (`internal/deleter`)

- **No symlink following & no TOCTOU:** Walk from the volume mount root FD down each path component via `unix.Openat(..., O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC, 0)`. If an intermediate directory was swapped for a symlink, `ELOOP` aborts deletion of that target.
- **No mount-boundary crossing:** Compare `st_dev` at each directory step using `IsMountPoint(path, parentDev, childDev)` (using `mounts.NewBoundaryChecker()` on Linux, `dev != parentDev` on QNAP/fallback). Never enter or delete inside mounted filesystems or Btrfs snapshots; if encountered inside a target directory, skip it and record a per-item failure (`mount point skipped`) so the parent directory is not falsely marked fully deleted.
- **Symlinks as leaf targets:** `unix.Fstatat(..., AT_SYMLINK_NOFOLLOW)` identifies symlinks as non-directories; `unix.Unlinkat(dirFD, leaf, 0)` removes the symlink itself without touching its target.
- **Idempotent `ENOENT`:** If a target is already gone on disk when deletion reaches it, treat the target as deleted so stale DB records are cleaned up.
- **Bounded-memory safe directory drain:** For directories, open `dirFD` with `O_NOFOLLOW|O_DIRECTORY`, verify `(Dev, Ino)` matches `Fstatat`, and loop `Readdirnames(512)` → unlink children (recursing into non-mount subdirectories) → rewind (`Seek(0, 0)`) while tracking `failedNames` to avoid infinite loops or filesystem `readdir` cookie skips. Check `ctx.Err()` on every iteration for immediate cancellation.
- **Partial failure semantics:** Continue deleting remaining siblings when an individual file fails (reporting up to 50 failure details). Only mark a top-level target as fully deleted in the databases if `rmdir`/`unlink` on that top-level target succeeded cleanly (or it was already `ENOENT`).

## 5. Background Job & Database Synchronization

### 5.1 Detached Worker (`storage-analyzer delete --request <path>`)

- Reuses `runner`'s `run/scan.lock` (`flock`) and `run/progress.json` (`job: "delete"`), plus `runner.Cancel` (`SIGTERM`).
- `delete_start` writes a `0600` JSON request file `run/delete-req-<jobId>.json`, spawns `storage-analyzer delete --request <path>`, and returns `202 {"started":true,"jobId":"...","targets":N}`.
- Worker sets low priority (`runner.SetLowPriority`), logs to `logs/scan.log`, updates `progress.json` every second (`targets`, `doneTargets`, `deletedItems`, `freedBytes`, `failed`, `current`), and removes the request file upon exit (`CleanupStaleTmp` also sweeps leftover `delete-req-*.json`).
- Before releasing `scan.lock`, worker atomically writes `run/delete-last.json` (`DeleteResult`: `id`, `scanId`, `status` = `done|partial|canceled|failed`, `targets`, `succeeded`, `deletedItems`, `freedDSize`, `freedASize`, `deletedIds`, `failures`, `errorMessage`).

### 5.2 Database Sync (when `len(deletedIDs) > 0`)

1. **`scan-<id>.db` (`scandb.MarkDeleted`):**
   - Adds `FlagDeleted = 8` and `MetaDeletedCount = "deletedCount"`.
   - In a single SQLite transaction (keeping `journal_mode=DELETE`):
     - For each fully deleted top-level `entryID`, sets `flags = flags | FlagDeleted`.
     - Subtracts exact scanner contribution from every ancestor up to the volume root (`MAX(0, ...)`):
       - Directory: `deltaDSize = e.DSize`, `deltaASize = e.ASize`, `deltaItems = 1 + e.Items`.
       - Non-directory: `deltaItems = 1`; `deltaDSize/deltaASize = 0` if `FlagHardlinkDup` is set, else `e.DSize/e.ASize`.
     - Increments `meta["deletedCount"]` and updates `meta["volumes"]` with refreshed `Statfs` capacity values.
   - `scandb.DB` reader changes:
     - `Get(id)` returns `ErrNotFound` if `e.Flags & FlagDeleted != 0`.
     - `Path(id)` automatically returns `ErrNotFound` if any ancestor is deleted (since it walks `Get`).
     - `Children(id)` filters `(flags & FlagDeleted) = 0`; when `hasDeleted` is true, computes `total` via `COUNT(*)` over `[FirstChild, FirstChild+NChild)`.
     - `EachDir`, `EachDesc`, `DupSizeFiles` filter `(flags & FlagDeleted) = 0` (and `dupcheck.Run` ignores descendants of skipped dirs).
2. **`dups-<id>.db` (`dupcheck.PruneDeleted`):**
   - If `dups-<id>.db` exists for `scanId`, opens read-write (`journal_mode=DELETE`) and removes any `members` row matching a deleted `entryID` OR whose display `path` equals/lies under any deleted directory's display path.
   - Recomputes affected groups: removes groups with `< 2` remaining members; otherwise updates `copies`, `name`, `rdsize`, `rasize`.
   - Recomputes `meta["groups"]`, `meta["reclaimDSize"]`, `meta["reclaimASize"]` and returns the updated summary.
3. **`catalog.db`:**
   - `AdjustScanTotals(scanID, deltaDSize, deltaASize, deletedFiles, deletedDirs)` updates `scans` totals (`MAX(0, ...)`).
   - `UpdateDupSummary(scanID, groups, reclaimDSize, reclaimASize)` updates `dupchecks` if `dups-<id>.db` was updated.

## 6. API Contract (`internal/api`)

All endpoints enforce existing auth (`isAdmin == 1` on QNAP, password session on Linux); `delete_preview` and `delete_start` require `POST` + `X-SA-Request: 1`.

- `GET ?action=status`: adds `"deleteEnabled": bool` and optional `"lastDelete": DeleteResult`.
- `POST ?action=delete_preview`:
  - Body: `{"scanId": int64, "entryIds": []int64}` (max 2000 IDs).
  - Returns resolved & folded target list with real disk paths, sizes, item counts, Docker risk flags, protected flags/reasons, and folded/missing counts.
- `POST ?action=delete_start`:
  - Body: `{"scanId": int64, "entryIds": []int64, "confirmDocker": bool}`.
  - Re-validates enabled state, lock availability (`409 task_running`), latest scan (`409 not_latest_scan`), protected paths (`403 protected_path`), and Docker confirmation (`400 docker_confirm_required`).
  - Returns `202 {"started": true, "jobId": "...", "targets": N}`.
- `POST ?action=scan_cancel`: unchanged; cancels whichever job (`scan`, `dupcheck`, `delete`) currently holds `scan.lock`.

## 7. Web UI & Cross-View Delete Basket (`web/`)

- **Visibility:** Deletion controls appear only when `state.deleteEnabled === true` and `state.scanId === latestDoneScanId()`.
- **Space view:**
  - Inside any volume (`dirId !== 0`), each non-mount row shows a checkbox (adding/removing from the persistent Delete Basket) and a compact quick-delete button (`×`/trash) on hover/focus/selection.
  - Header `th.c-name` includes a select-all-page checkbox (stopping click propagation so sorting still works).
  - Rows inside an already-staged ancestor directory show a checked disabled checkbox + `随上级待删` / `Via parent` tag.
- **Duplicates view:**
  - Each expanded member row shows a checkbox (linked to the same Delete Basket) and a quick-delete button next to `↗ 查看位置`.
  - Prevents staging or quick-deleting the **last remaining copy** of any duplicate group (disables checkbox/button with tooltip explaining at least 1 copy must be kept; full deletion remains possible from Space view).
- **Persistent Delete Basket Bar (`#basketBar`):**
  - Docked cleanly at the bottom of `.app` whenever `basket.size > 0 && canDelete()`, surviving directory navigation, sorting, pagination, and Space ↔ Duplicates switching.
  - Shows staged count and estimated reclaimable size (respecting Disk usage / Apparent size toggle), with **Clear** and **Review & delete** buttons.
  - Automatically folds children when a parent directory is staged; clears automatically when a new scan completes or user switches away from the latest scan.
- **Confirmation & Review Dialog:**
  - Fetches authoritative `delete_preview`, displays full absolute disk paths, sizes, item counts, any folded count, per-item remove button (in basket mode), protected-path errors, and required Docker risk acknowledgment checkbox when `hasDockerRisk` is true.
- **Progress & Result Feedback:**
  - Reuses `#progress` banner for `job === 'delete'` with fast initial polling (250ms → 800ms) and cancel confirmation.
  - On completion, removes succeeded IDs from the basket, refreshes `scans` and the current view (stepping up cleanly if the currently open directory was deleted), and shows a summary toast — or a failure-details modal whenever any paths failed.

## 8. Testing & Verification

- **`internal/config`:** `allow_delete` default `true`, explicit `true`/`false`, invalid value rejection, shipped config parsing.
- **`internal/mounts`:** `systemd-full.txt` fixture verifying `ProtectSystem=full` + `ReadOnlyPaths` bind mounts do not duplicate volumes or break boundary detection.
- **`internal/deleter`:** Policy checks (Linux & QNAP subtree/exact/ancestor/depth-1 rules, symlinked dataDir), target resolution & ancestor folding, Docker ancestor risk detection, real filesystem deletion (files, nested dirs, non-UTF-8 `BadName`, symlink target untouched, intermediate symlink `ELOOP` refused, mount boundary skipped, `ENOENT` idempotency, cancellation mid-tree, permission-denied partial failure).
- **`internal/scandb`:** `MarkDeleted` on files, dirs, and `FlagHardlinkDup`; `Get`/`Path`/`Children`/`EachDir`/`EachDesc`/`DupSizeFiles` exclusion; legacy DB backwards compatibility; `?mode=ro` reopen after write.
- **`internal/dupcheck`:** `PruneDeleted` on file groups, folder groups, prefix deletion from Space view, dissolving 1-copy groups, recomputing `copies`/`rdsize`/`rasize`/`meta`, `?mode=ro` reopen.
- **`internal/catalog`:** `AdjustScanTotals` and `UpdateDupSummary`.
- **`internal/runner`:** End-to-end `RunDelete` with real scan + dupcheck DBs + real files on disk, verifying disk cleanup, `scan.db`/`dups.db`/`catalog.db` sync, `delete-last.json`, partial failures, and cancellation.
- **`internal/api` & `cmd/storage-analyzer`:** `status` (`deleteEnabled`, `lastDelete`), `delete_preview`, `delete_start` (auth, CSRF header, disabled, conflict, non-latest scan, protected path, Docker confirm gate, happy path), and full CLI `delete --request` execution on both `!qnap` and `-tags qnap` builds.
- **Frontend checks:** `node --check web/app.js web/i18n.js`, i18n key parity test across `zh`/`en` and `zhLinux`/`enLinux`, and full `go test ./... && go test -tags qnap ./... && go vet ./... && go vet -tags qnap ./...`.
