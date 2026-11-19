# ARM（arm64 / arm-x41）支持 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 一次构建产出 x86_64、arm_64、arm-x41 三个 QPKG，并在三种 Linux 架构上跑完整测试。

**Architecture:** 修正 32 位上的时间戳溢出和 fadvise 调用；新增 `scripts/test-linux.sh` 在 Docker 里跑三种架构的测试；`build.sh` 先跑它再交叉编译三份二进制、一次 qbuild 出三个包。

**Tech Stack:** Go 1.26（`CGO_ENABLED=0`），`golang.org/x/sys/unix`，Docker（`ubuntu:24.04`，非本机架构靠 Docker Desktop 模拟），QDK `qbuild`。

**Spec:** `docs/superpowers/specs/2026-11-19-arm-support-design.md`

## Global Constraints

- 架构与 Go 目标对应：`x86_64` ↔ `GOARCH=amd64`；`arm_64` ↔ `GOARCH=arm64`；`arm-x41` ↔ `GOARCH=arm GOARM=7`。
- 安装包名：`StorageAnalyzer_<ver>_<qdk-arch>.qpkg`。
- 不做 arm-x31 / x09 / x19 / x86；不改版本号、不发布。
- 不提交 git（用户要求时再提交）；在分支 `arm-support` 上工作。

## Review Focus

1. **32 位时间戳**：2026 年的 mtime/ctime 纳秒值必须正确 → Task 1 `TestStatInfoTimes`（在 linux/arm 上跑）。
2. **Linux 专用代码在 x86_64 上的回归**：fadvise 改用 `unix.Fadvise` 后 x86_64 行为不变 → Task 2 在 linux/amd64 跑 dupcheck 全部测试。
3. **某个架构的测试失败时仍然打出包** → Task 3 `build.sh` 用 `set -e` 且先跑 `test-linux.sh`。
4. **qbuild 只出了部分包** → Task 3 构建后断言三个文件都存在。
5. **测试依赖 root 或真实 QTS 路径**：容器里以 root 运行，`catalog` 的 0700 权限测试照常 → Task 2 观察结果。

---

### Task 1: 32 位代码修正

**Files:**
- Create: `internal/dupcheck/fs_linux_test.go`
- Modify: `internal/dupcheck/fs_linux.go`、`internal/scanner/stat_linux.go`、`internal/api/spawn_test.go`、`go.mod`

- [ ] **Step 1:** 写 `TestStatInfoTimes`：`syscall.Stat_t` 的 `Mtim = {Sec: 1795000000, Nsec: 123}`、`Ctim = {Sec: 1795000001, Nsec: 7}`（字段赋值用该架构的类型转换），断言 `MTime == 1795000000`、`MTimeNS == 1795000000*1e9+123`、`CTimeNS == 1795000001*1e9+7`。
- [ ] **Step 2:** `GOOS=linux GOARCH=arm GOARM=7 go vet ./internal/dupcheck` → 编译失败（RED）。
- [ ] **Step 3:** `statInfo` 先转 `int64` 再乘；`dropCache` 用 `unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)`；`statFields` 的 `Mtim.Sec` 转 `int64`；`spawn_test.go` 用 `unix.Getsid`；`go mod tidy` 让 `golang.org/x/sys` 变为直接依赖。
- [ ] **Step 4:** 三个目标 `go vet ./...` 均无输出；宿主 `go test ./...` PASS。

### Task 2: `scripts/test-linux.sh`

- [ ] **Step 1:** 脚本：对 `amd64`、`arm64`、`arm`(GOARM=7) 依次 `go test -c` 每个有测试的包到临时目录，在 `ubuntu:24.04` 容器中以包目录为工作目录逐个运行（`testdata` 相对路径要能找到），任一失败即退出非零并指明架构和包。
- [ ] **Step 2:** 运行 → 三个架构全部 PASS（含 Task 1 的 `TestStatInfoTimes`）。

### Task 3: 三架构打包

- [ ] **Step 1:** `build.sh`：先 `go test ./...` 和 `scripts/test-linux.sh`；交叉编译三份二进制到 `$STAGE/{x86_64,arm_64,arm-x41}/bin/storage-analyzer`；一次 `qbuild` 带三个 `--build-arch`；把三个 `.qpkg` 复制到 `build/`，缺任何一个就失败。
- [ ] **Step 2:** `deploy.sh`：`ARCH=${ARCH:-x86_64}`，选 `build/StorageAnalyzer_*_${ARCH}.qpkg`。
- [ ] **Step 3:** 运行 `scripts/build.sh` → 产出三个包；用 `file` 检查每个包内二进制的架构（解包 data 部分或检查 stage 目录）正确。
