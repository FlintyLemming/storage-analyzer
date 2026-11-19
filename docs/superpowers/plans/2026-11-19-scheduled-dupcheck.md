# 定时扫描后自动检查重复项 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 设置里给定时扫描加一个"扫描完成后检查重复项"勾选项；勾上后，定时扫描成功就在同一进程、不放锁的情况下接着检查这次扫描。

**Architecture:** `catalog.Settings` 加 `ScheduleDupCheck`；`runner` 把"拿锁"和"干活"拆开，新增 `RunScheduled` 一次拿锁跑完扫描 + 检查；`main` 的 `scan --trigger schedule` 改走 `RunScheduled`；前端加勾选框、进度提示，并在任务从扫描切到检查时按"扫描完成"刷新一次。crontab 行不变。

**Tech Stack:** Go 1.26（`CGO_ENABLED=0`），`modernc.org/sqlite`，原生 JS。

**Spec:** `docs/superpowers/specs/2026-11-19-scheduled-dupcheck-design.md`（基于 `docs/superpowers/specs/2026-11-17-duplicate-check-design.md`）

## Global Constraints

- 设置键名 `schedule_dupcheck`，值 `"1"` / `"0"`；JSON 字段 `scheduleDupCheck`；默认 `false`。
- crontab 行保持 `scan --trigger schedule`，`schedule` 包不改。
- 勾选只影响定时任务；`scan --trigger manual` 和 `dupcheck --scan N` 行为不变。
- 锁被占 → 整次跳过（不新增扫描记录、不检查）；扫描失败 / 取消 / 收到取消信号 → 不检查。
- 定时检查的进度 `trigger` 为 `"schedule"`；定时扫描阶段进度带 `next: "dupcheck"`（仅当这次会接着检查）。
- 界面文案：勾选框 `扫描完成后检查重复项`；保存提示 `设置已保存：每天 03:00 自动扫描并检查重复项`（每周时为 `周三 03:00 …`）；进度 `· 定时任务，完成后检查重复项`。
- 不在计划里提交 git；用户要求时再提交。

## Review Focus

1. **页面开着时定时任务从扫描切到检查**：运行状态一直是 true，旧逻辑不会刷新扫描列表 → 前端要在"上一轮是扫描、这一轮是检查"时调用一次 `onScanFinished` → Task 4 浏览器验证。
2. **扫描和检查之间被别的任务插队**：由 `RunScheduled` 只调用一次 `tryLock` 保证 → Task 2 代码评审时确认没有第二次拿锁。
3. **扫描刚完成时收到 SIGTERM**：不应留下一条"已取消"的检查记录 → Task 2 `TestRunScheduledCanceled`（取消发生在扫描前，覆盖同一个 `ctx.Err()` 判断）。
4. **旧版本保存的设置里没有 `schedule_dupcheck`**：读出来是 `false`，不报错 → Task 1 默认值断言。
5. **关闭定时扫描后再打开**：勾选状态应保留（关闭时也照原样保存）→ Task 4 浏览器验证。

---

### Task 1: 设置字段

**Files:**
- Modify: `internal/catalog/catalog.go`（`Settings`、`Settings()`、`SaveSettings()`）
- Test: `internal/catalog/catalog_test.go:127` `TestSettingsDefaultsAndValidation`

**Interfaces:**
- Produces: `catalog.Settings.ScheduleDupCheck bool`（`json:"scheduleDupCheck"`）

- [ ] **Step 1:** 在 `TestSettingsDefaultsAndValidation` 中：`want` 显式写 `ScheduleDupCheck: false`；`good` 改为 `ScheduleDupCheck: true`，往返后仍 `got == good`。
- [ ] **Step 2:** `go test ./internal/catalog -run TestSettings` → 编译失败（字段不存在）。
- [ ] **Step 3:** 加字段；`Settings()` 读 `schedule_dupcheck`（`v == "1"`）；`SaveSettings()` 写 `"1"`/`"0"`。`Validate` 不变。
- [ ] **Step 4:** `go test ./internal/catalog` → PASS。

### Task 2: `runner.RunScheduled`

**Files:**
- Modify: `internal/runner/runner.go`（`Run` 拆为拿锁 + `runScan`；`Progress.Next`；`Options.FS`、`Options.thenDupCheck`）
- Modify: `internal/runner/dupcheck.go`（`RunDupCheck` 拆为拿锁 + `runDupCheck`；`DupOptions.Trigger`；GC 设置移入）
- Create: `internal/runner/scheduled.go`
- Create: `internal/runner/scheduled_test.go`
- Modify: `docs/superpowers/specs/2026-11-17-duplicate-check-design.md:27`（非目标那条后注明"已由 2026-11-19 设计取消"）

**Interfaces:**
- Consumes: `catalog.Settings.ScheduleDupCheck`
- Produces:
  - `type ScheduledRun struct { ScanID int64; Checked bool; Dup dupcheck.Summary; DupErr error }`
  - `func RunScheduled(ctx context.Context, o Options) (ScheduledRun, error)` —— 返回的 error 只表示扫描阶段（含 `ErrBusy`、`context.Canceled`）；检查阶段的错误放在 `DupErr`。`o.Trigger` 被强制为 `"schedule"`。
  - `Progress.Next string` `json:"next,omitempty"`
  - `DupOptions.Trigger string`（空 = `"manual"`）
  - `Options.FS dupcheck.FS`（只给 `RunScheduled` 的检查阶段用，默认真实文件系统）

- [ ] **Step 1: 写测试** `scheduled_test.go`，复用 `holdLock`、`fakeVolumes`、`peekFS`；造一个含两个相同 5000 字节文件的卷（同 `dupScan`），用 `catalog.SaveSettings` 打开/关闭 `ScheduleDupCheck`（其余字段用 `catalog.DefaultSettings`）：
  - `TestRunScheduledWithoutDupCheck`：默认设置 → `err == nil`、`!res.Checked`；`c.Get(res.ScanID)` 为 `done` 且 `Trigger == "schedule"`；`c.GetDupCheck(res.ScanID)` 返回 `catalog.ErrNotFound`。
  - `TestRunScheduledWithDupCheck`：开启 → `res.Checked && res.DupErr == nil && res.Dup.Groups == 1`；检查记录 `Status == "done"`；`run/progress.json` 不存在；用 `Options.FS = peekFS` 断言检查阶段进度 `Job == "dupcheck" && Trigger == "schedule" && ScanID == res.ScanID`。
  - `TestRunScheduledBusy`：开启 + `holdLock` → `errors.Is(err, ErrBusy)`；`c.List()` 为空。
  - `TestRunScheduledScanFails`：开启 + `Discover` 返回错误 → `err != nil`、`!res.Checked`；该扫描没有检查记录。
  - `TestRunScheduledCanceled`：开启 + 预先取消的 ctx → `errors.Is(err, context.Canceled)`、`!res.Checked`；没有检查记录。
- [ ] **Step 2:** `go test ./internal/runner -run Scheduled` → 编译失败。
- [ ] **Step 3: 拆分锁。** `Run` = `tryLock(o.DataDir, 5)` + `defer lock.Close()` + `runScan(ctx, o, lock)`；`RunDupCheck` 同理调用 `runDupCheck(ctx, o, lock)`。两个内部函数各自写 PID 到锁文件（保持现状），不再自己拿锁。`runDupCheck` 开头 `defer debug.SetGCPercent(debug.SetGCPercent(25))`，并删掉 `cmd/storage-analyzer/main.go` 里 `dupCheck` 的 `debug.SetGCPercent(25)`。`runScan` 的进度在 `o.thenDupCheck` 时带 `Next: "dupcheck"`；`runDupCheck` 的进度 `Trigger` 取 `o.Trigger`（空则 `"manual"`）。
- [ ] **Step 4: 实现 `RunScheduled`**（`scheduled.go`）：`tryLock` 一次 → 打开 catalog 读设置后关闭 → `o.Trigger = "schedule"`、`o.thenDupCheck = s.ScheduleDupCheck` → `runScan`；出错直接返回 → 设置关闭或 `ctx.Err() != nil` 则返回 → `Checked = true`，`runDupCheck(ctx, DupOptions{DataDir, ScanID, Now: o.Now, FS: o.FS, Trigger: "schedule"}, lock)`。整个过程只持有这一把锁。
- [ ] **Step 5:** `go test ./internal/runner` → 全部 PASS（含旧测试）。

### Task 3: 命令行接入

**Files:**
- Modify: `cmd/storage-analyzer/main.go`（`scan`、`dupCheck`）

**Interfaces:**
- Consumes: `runner.RunScheduled`、`runner.ScheduledRun`

- [ ] **Step 1:** `scan()` 在 `trigger == "schedule"` 时调用 `runner.RunScheduled(ctx, runner.Options{DataDir, Discover: discover})`，扫描阶段日志与现在相同；`res.Checked` 时用与 `dupCheck()` 相同的格式记录检查结果（抽一个 `logDupResult(id, start, sum, err)` 两处共用；检查阶段的开始时间在扫描结束后取）。`ErrBusy` 日志改为 `scheduled run skipped: another scan or duplicate check is running`。
- [ ] **Step 2:** `go build ./... && go vet ./...` → 无输出。

### Task 4: 前端

**Files:**
- Modify: `web/app.js`（`showSettings`、`renderProgress`、`renderDupProgress`、`pollStatus`）
- Modify: `web/style.css`（仅在勾选框排版需要时）

- [ ] **Step 1: 设置对话框。** "定时扫描"行下加一行：勾选框 `扫描完成后检查重复项`（初值 `!!s.scheduleDupCheck`），下面提示 `首次检查可能要几个小时，之后通常几分钟到几十分钟；期间 NAS 硬盘会比较忙。`；两者在模式为"关闭"时隐藏（跟随现有 `sync()`）。保存时 `next.scheduleDupCheck = 勾选框.checked`（关闭时也照原样保存）。保存提示：开启时末尾 `自动扫描并检查重复项`，否则 `自动扫描`。
- [ ] **Step 2: 进度。** `renderProgress` 的定时后缀：`p.next === 'dupcheck'` 时为 ` · 定时任务，完成后检查重复项`。`renderDupProgress` 在 `p.trigger === 'schedule'` 时给 stats 加 `定时任务`。
- [ ] **Step 3: 扫描→检查切换。** `pollStatus` 中，若 `was && state.running && prev && !isDupJob(prev) && isDupJob(st.progress)`，在 `renderProgress()` 之后 `await onScanFinished(prev)`。
- [ ] **Step 4: 浏览器验证。** 编译 dev 二进制到 `build/dev/bin/storage-analyzer`，`preview_start storage-analyzer-dev`：
  - 设置：切换模式时勾选框显隐正确；勾上保存 → 提示文案正确；重开对话框勾选仍在；关闭后再打开模式，勾选仍在；`build/dev/dev-crontab` 的行仍是 `scan --trigger schedule`。
  - 在终端用小目录跑 `SA_DEV_ROOTS=<小目录> build/dev/bin/storage-analyzer scan --trigger schedule --root build/dev`：页面先显示"定时任务，完成后检查重复项"，扫描结束时提示扫描完成并切到新结果，然后显示检查进度直至完成；控制台无错误。
  - 勾选关闭再跑一次：只扫描，不检查。

### Task 5: 收尾验证

- [ ] **Step 1:** `go test ./... && go vet ./...` → 全部 PASS。
- [ ] **Step 2:** `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /dev/null ./cmd/storage-analyzer` → 成功（目标平台能编过）。
