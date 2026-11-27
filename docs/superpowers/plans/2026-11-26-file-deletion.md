# File & Directory Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe permanent file and directory deletion (single-item and cross-directory/cross-view Delete Basket) across QNAP and Linux with background job execution and immediate SQLite synchronization.

**Architecture:** A new `internal/deleter` package owns path protection policies, scan-DB target resolution/folding, and FD-based `openat`/`unlinkat` recursive deletion without symlink following or mount-boundary crossing. `internal/runner` runs deletion as a detached `storage-analyzer delete --request <file>` job sharing `scan.lock` and `progress.json`, writes `run/delete-last.json`, and atomically syncs `scandb`, `dupcheck.Result`, and `catalog`. `internal/api` exposes `deleteEnabled`/`lastDelete` on `status` plus `delete_preview` and `delete_start`, while `web/` adds row checkboxes, quick delete, a persistent bottom Delete Basket bar, duplicate last-copy protection, and a tiered confirmation dialog.

**Tech Stack:** Go 1.26 (`CGO_ENABLED=0`, `golang.org/x/sys/unix`, `modernc.org/sqlite`), systemd hardening, vanilla HTML/CSS/JS.

**Spec:** [docs/superpowers/specs/2026-11-26-file-deletion-design.md](../specs/2026-11-26-file-deletion-design.md)

## Global Constraints

- Zero new third-party dependencies.
- Both `go test ./...` and `go test -tags qnap ./...` (plus `go vet`) must pass.
- `scan-<id>.db` and `dups-<id>.db` must stay in `DELETE` journal mode so `?mode=ro` opens continue to work everywhere.
- Never accept raw filesystem paths from HTTP clients; always reconstruct raw byte paths from `scandb` entry IDs + volume mount points.
- Respect strict CSP (`style-src 'self'`, no inline JS handlers) and full bilingual `zh`/`en`/`zhLinux`/`enLinux` i18n parity.

---

### Task 1: Linux Config `allow_delete`, Systemd Hardening & Mount Fixture

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/config/shipped_test.go`
- Modify: `packaging/linux/config`
- Modify: `packaging/linux/storage-analyzer.service`
- Create: `internal/mounts/testdata/systemd-full.txt`
- Modify: `internal/mounts/mounts_test.go`

**Interfaces:**
- Produces: `config.Config.AllowDelete bool` (default `true`; parsed from `allow_delete = true|false`).

- [ ] **Step 1:** Add failing tests in `internal/config/config_test.go` (parsing `allow_delete = false`, `allow_delete = true`, default `true`, invalid `allow_delete = yes`) and `internal/mounts/mounts_test.go` (`systemd-full.txt` fixture with `/usr`, `/etc`, `/boot`, `/bin` RO bind mounts).
- [ ] **Step 2:** Run `go test ./internal/config ./internal/mounts` and verify the new tests fail.
- [ ] **Step 3:** Implement `AllowDelete` in `internal/config/config.go`, update `packaging/linux/config`, update `packaging/linux/storage-analyzer.service` (`ProtectSystem=full`, `ReadOnlyPaths=-/bin -/sbin -/lib -/lib64`, `CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE CAP_FOWNER`), and update `internal/config/shipped_test.go`.
- [ ] **Step 4:** Run `go test ./internal/config ./internal/mounts` and verify all pass.

---

### Task 2: `scandb` Deletion Marking & Reader Filtering

**Files:**
- Modify: `internal/scandb/schema.go`
- Modify: `internal/scandb/reader.go`
- Create: `internal/scandb/delete.go`
- Modify: `internal/scandb/scandb_test.go`

**Interfaces:**
- Produces:
  - `scandb.FlagDeleted = 8`, `scandb.MetaDeletedCount = "deletedCount"`
  - `type DeletedTotals struct { Count int; DSize, ASize, Items int64 }`
  - `func MarkDeleted(path string, ids []int64, volumesJSON string) (DeletedTotals, error)`
  - Filtered `DB.Get`, `DB.Path`, `DB.Children`, `DB.EachDir`, `DB.EachDesc`, `DB.DupSizeFiles`

- [ ] **Step 1:** Write failing unit tests in `internal/scandb/scandb_test.go` covering `MarkDeleted` on regular file, directory subtree, `FlagHardlinkDup` file (0 size delta, 1 item delta), ancestor size/items roll-up, `Get`/`Path`/`Children` hiding deleted entries and descendants of deleted dirs, `EachDir`/`EachDesc`/`DupSizeFiles` skipping deleted rows, `volumesJSON` update, and `?mode=ro` reopen.
- [ ] **Step 2:** Run `go test ./internal/scandb` and verify failure.
- [ ] **Step 3:** Implement `FlagDeleted`, `MetaDeletedCount`, `MarkDeleted` in `internal/scandb/delete.go`, and update `internal/scandb/reader.go`.
- [ ] **Step 4:** Run `go test ./internal/scandb` and verify all pass (including `TestBytesPerEntry` and legacy DB compatibility).

---

### Task 3: `dupcheck` Deletion Pruning & `catalog` Summary Updates

**Files:**
- Modify: `internal/dupcheck/result.go`
- Modify: `internal/dupcheck/check.go`
- Modify: `internal/dupcheck/result_test.go`
- Modify: `internal/catalog/catalog.go`
- Modify: `internal/catalog/catalog_test.go`

**Interfaces:**
- Produces:
  - `type PruneSummary struct { Changed bool; Groups, ReclaimDSize, ReclaimASize int64 }`
  - `func PruneDeleted(path string, deletedEntryIDs []int64, deletedDisplayPaths [][]byte) (PruneSummary, error)`
  - `func (c *Catalog) AdjustScanTotals(id, deltaDSize, deltaASize, deletedFiles, deletedDirs int64) error`
  - `func (c *Catalog) UpdateDupSummary(scanID, groups, reclaimDSize, reclaimASize int64) error`

- [ ] **Step 1:** Write failing tests in `internal/dupcheck/result_test.go` (pruning 1 of 3 copies updates `copies=2` and `rdsize`; pruning 1 of 2 removes group; deleting parent folder path prefix removes nested file members; `?mode=ro` reopen) and `internal/catalog/catalog_test.go` (`AdjustScanTotals`, `UpdateDupSummary`).
- [ ] **Step 2:** Run `go test ./internal/dupcheck ./internal/catalog` and verify failure.
- [ ] **Step 3:** Implement `PruneDeleted` in `internal/dupcheck/result.go`, ensure `dupcheck.Run` skips entries whose parent dir was skipped due to `FlagDeleted`, and implement `AdjustScanTotals`/`UpdateDupSummary` in `internal/catalog/catalog.go`.
- [ ] **Step 4:** Run `go test ./internal/dupcheck ./internal/catalog` and verify all pass.

---

### Task 4: Safe Deletion Engine & Path Guard (`internal/deleter`)

**Files:**
- Create: `internal/deleter/policy.go`
- Create: `internal/deleter/resolve.go`
- Create: `internal/deleter/remove.go`
- Create: `internal/deleter/deleter_test.go`

**Interfaces:**
- Produces:
  - `type Policy struct { Platform string; SubtreeDeny []string; ExactDeny []string }`
  - `func DefaultPolicy(platform, dataDir, extraProtectedDir string) Policy`
  - `func (p Policy) Check(absPath string, depth int) error`
  - `type VolumeRoot struct { EntryID int64; Name, MountPoint string }`
  - `type Target struct { EntryID int64; Kind int; RawPath, DisplayPath string; RawDisplayPath []byte; RelParts []string; VolMount string; DSize, ASize, Items int64; BadName, DockerRisk bool; DockerType string; Protected bool; ProtectReason string }`
  - `type ResolveResult struct { Targets []Target; Folded, Missing int; TotalDSize, TotalASize, TotalItems int64; HasDockerRisk, HasProtected bool }`
  - `func ResolveTargets(db *scandb.DB, roots map[int64]VolumeRoot, policy Policy, entryIDs []int64) (ResolveResult, error)`
  - `type RemoveCallbacks struct { IsMountPoint func(path string, parentDev, dev uint64) bool; OnProgress func(path string, filesDelta, dirsDelta, freedDBytes, freedABytes int64); OnError func(path string, err error) }`
  - `type RemoveStats struct { Files, Dirs, FreedDSize, FreedASize int64; Errors int; Complete bool }`
  - `func RemoveTarget(ctx context.Context, t Target, cb RemoveCallbacks) RemoveStats`

- [ ] **Step 1:** Write comprehensive failing tests in `internal/deleter/deleter_test.go` for `Policy` (Linux/QNAP subtree, ancestor, exact, depth-0, depth-1), `ResolveTargets` (raw bytes, BadName, ancestor-descendant folding, Docker self/ancestor risk, protected flags), and `RemoveTarget` (single file, nested directory, non-UTF-8 filename, symlink target preserved, intermediate symlink `ELOOP` refused, mount boundary skipped, `ENOENT` idempotency, cancellation, partial error).
- [ ] **Step 2:** Run `go test ./internal/deleter` and verify failure.
- [ ] **Step 3:** Implement `policy.go`, `resolve.go`, and `remove.go` using `golang.org/x/sys/unix` (`Openat`, `Fstatat`, `Unlinkat` with `O_NOFOLLOW`, `AT_SYMLINK_NOFOLLOW`, `AT_REMOVEDIR`, bounded-batch directory drain).
- [ ] **Step 4:** Run `go test -v ./internal/deleter` and verify all pass.

---

### Task 5: Background Delete Runner & CLI Subcommand

**Files:**
- Modify: `internal/runner/runner.go`
- Create: `internal/runner/delete.go`
- Create: `internal/runner/delete_test.go`
- Modify: `cmd/storage-analyzer/main.go`
- Modify: `cmd/storage-analyzer/platform_generic.go`
- Modify: `cmd/storage-analyzer/platform_qnap.go`

**Interfaces:**
- Produces:
  - `runner.Progress` delete fields (`Targets`, `DoneTargets`, `DeletedItems`, `FreedBytes`, `Failed`, `JobID`)
  - `runner.DeleteRequest`, `runner.DeleteResult`, `runner.DeleteError`, `runner.DeleteOptions`
  - `runner.RunDelete(ctx, DeleteOptions) (DeleteResult, error)`
  - `runner.LastDelete(dataDir string) (*DeleteResult, error)`
  - `runner.WriteDeleteRequest(dataDir string, req DeleteRequest) (string, error)`
  - CLI command `storage-analyzer delete --request <path>` wired on both Linux (`!qnap`) and QNAP (`qnap`)

- [ ] **Step 1:** Write failing end-to-end tests in `internal/runner/delete_test.go` verifying `RunDelete` deletes real files/dirs, syncs `scandb`, `dups`, `catalog`, refreshes volume `statfs`, writes `delete-last.json`, handles partial failure without falsely marking incomplete dir deleted, respects `ctx` cancellation, and respects `scan.lock`.
- [ ] **Step 2:** Run `go test ./internal/runner` and verify failure.
- [ ] **Step 3:** Implement `internal/runner/delete.go`, update `internal/runner/runner.go` (`Progress` fields and `CleanupStaleTmp` sweeping `delete-req-*.json`), and wire `delete` command + `deletePolicy` into `cmd/storage-analyzer/main.go`, `platform_generic.go`, and `platform_qnap.go`.
- [ ] **Step 4:** Run `go test ./internal/runner ./cmd/storage-analyzer && go test -tags qnap ./internal/runner ./cmd/storage-analyzer` and verify all pass.

---

### Task 6: Delete API Endpoints (`status`, `delete_preview`, `delete_start`)

**Files:**
- Modify: `internal/api/api.go`
- Modify: `internal/api/api_test.go`
- Modify: `cmd/storage-analyzer/serve_test.go`

**Interfaces:**
- Consumes: `deleter.Policy`, `deleter.ResolveTargets`, `runner.LastDelete`, `runner.WriteDeleteRequest`
- Produces:
  - `api.Config.DeleteEnabled bool` and `api.Config.DeletePolicy deleter.Policy`
  - `GET ?action=status` returning `deleteEnabled` and optional `lastDelete`
  - `POST ?action=delete_preview`
  - `POST ?action=delete_start`

- [ ] **Step 1:** Write failing API tests in `internal/api/api_test.go` covering `status` (`deleteEnabled`, `lastDelete`), `delete_preview` (resolved paths, folding, protected, Docker risk), `delete_start` (POST/CSRF enforcement, disabled `403`, busy `409`, non-latest scan `409`, protected path `403`, missing Docker confirm `400`, success `202` spawning `delete --request ...`), and end-to-end delete test in `cmd/storage-analyzer/serve_test.go`.
- [ ] **Step 2:** Run `go test ./internal/api ./cmd/storage-analyzer` and verify failure.
- [ ] **Step 3:** Implement the endpoints in `internal/api/api.go` and wire `DeleteEnabled` / `DeletePolicy` in `platform_generic.go` and `platform_qnap.go`.
- [ ] **Step 4:** Run `go test ./internal/api ./cmd/storage-analyzer && go test -tags qnap ./internal/api ./cmd/storage-analyzer` and verify all pass.

---

### Task 7: Web UI, Persistent Cross-View Delete Basket & Bilingual i18n

**Files:**
- Modify: `web/index.html`
- Modify: `web/style.css`
- Modify: `web/i18n.js`
- Modify: `web/app.js`

- [ ] **Step 1:** Add `#basketBar` container to `web/index.html` and styles for checkboxes, row quick-delete button, inherited tag, `#basketBar`, confirmation target list, Docker warning box, and danger-primary button in `web/style.css` (strict CSP compliant, responsive `@media (max-width: 760px)`).
- [ ] **Step 2:** Add complete `zh`, `en`, `zhLinux`, `enLinux` i18n keys in `web/i18n.js` for deletion controls, basket bar, confirmation modal, Docker warning, protected path reasons, progress, cancellation, result summary/failure modal, and new API error codes.
- [ ] **Step 3:** Implement Delete Basket state (with cross-directory/cross-view persistence and ancestor-descendant folding), Space view checkboxes + page select-all + quick delete, Duplicates view checkboxes + last-copy protection + quick delete, confirmation modal backed by `delete_preview` + Docker checkbox gate, delete job progress & fast polling + completion/failure handling in `web/app.js`.
- [ ] **Step 4:** Verify JS syntax (`node --check web/app.js web/i18n.js`) and run an automated i18n key-parity check across `zh`/`en` and `zhLinux`/`enLinux`.

---

### Task 8: Documentation Update & Full Verification Suite

**Files:**
- Modify: `README.md`
- Modify: `README.zh-CN.md`

- [ ] **Step 1:** Update `README.md` and `README.zh-CN.md` (replace "Read-only" notes with accurate description of deletion + Delete Basket + guardrails, update Linux systemd privilege description and `allow_delete` config option, document how to revert to kernel-enforced read-only or directory-scoped `ReadWritePaths` via `systemctl edit`, and keep port `< 1024` override capabilities consistent).
- [ ] **Step 2:** Run full verification: `go vet ./...`, `go vet -tags qnap ./...`, `go test -race ./...`, `go test -race -tags qnap ./...`, cross-compile checks (`GOOS=linux GOARCH=amd64/arm64/arm`), JS syntax & i18n parity check.
