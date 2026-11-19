# ARM（arm64 / 32 位）支持 设计文档（短）

日期：2026-11-19
状态：待确认
基于：[2026-11-16-qnap-storage-analyzer-design.md](2026-11-16-qnap-storage-analyzer-design.md)

## 意图

让 ARM 架构的 QNAP 也能安装存储分析器。除现有 x86_64 外，增加 64 位 ARM 和 32 位 ARM 安装包。

用户的决定：32 位只做 **arm-x41**（Annapurna Labs Alpine，armv7，如 TS-x31P / x31X / x31K），不做 arm-x31（Mindspeed 老机型，最高 QTS 4.3，本来就满足不了 `QTS_MINI_VERSION=5.0.0`）。

## 目标 / 非目标 / 反目标

- 目标：一次构建产出三个安装包 `StorageAnalyzer_<ver>_x86_64.qpkg`、`_arm_64.qpkg`、`_arm-x41.qpkg`，功能完全一致。
- 非目标：arm-x31、arm-x09/x19（ARMv5）、x86 32 位；版本号和发布（做完后另行决定）；在真实 ARM NAS 上安装测试（手边没有 ARM 机器）。
- 反目标：
  - 32 位上能编译、能跑，但结果悄悄出错（时间戳溢出导致"扫描后有变化"误判、校验值缓存永远不命中）。
  - 只在 macOS 上跑测试，Linux 专用代码（`*_linux.go`）在任何架构上都没被测过就发出去。

## 方案

| 方案 | 做法 | 取舍 |
|---|---|---|
| **A（选用）** | 每个架构一个安装包（QDK 标准做法，`qbuild --build-arch` 一次给三个） | 装错架构时 QTS 安装器直接拒绝并提示；用户要选对文件 |
| B（已排除） | 一个通用包，装进三个二进制，启动时按 `uname -m` 选 | 一个文件到处装，但包大三倍，绕开了 QTS 的架构检查，安装脚本要自己写分支 |

## 设计

**代码修正（32 位）**
- `internal/dupcheck/fs_linux.go`：秒和纳秒先转 `int64` 再相乘（32 位上 `Timespec` 字段是 `int32`，`Sec*1e9` 会溢出）；`dropCache` 改用 `golang.org/x/sys/unix.Fadvise`（ARM 32 位的 fadvise 系统调用号和参数顺序不同，`unix` 包已处理）。`golang.org/x/sys` 从间接依赖变为直接依赖。
- `internal/scanner/stat_linux.go`：`Mtim.Sec` 显式转 `int64`。
- `internal/api/spawn_test.go`：`syscall.Getsid` 改 `unix.Getsid`（`syscall` 包在 linux/arm64 上没有它）。
- 新增 `internal/dupcheck/fs_linux_test.go`：`statInfo` 对 2026 年的时间戳给出正确的纳秒值（32 位上修正前会溢出）。

**构建**
- `scripts/build.sh`：交叉编译三个二进制（`amd64`；`arm64`；`arm` + `GOARM=7`），分别放进 `x86_64/bin`、`arm_64/bin`、`arm-x41/bin`，一次 `qbuild --build-arch x86_64 --build-arch arm_64 --build-arch arm-x41`，三个包都复制到 `build/`。
- 新增 `scripts/test-linux.sh`：为 linux/amd64、linux/arm64、linux/arm(v7) 分别编译每个包的测试二进制，在本地 `ubuntu:24.04` 容器里运行（非本机架构靠 Docker Desktop 的模拟）。`build.sh` 在打包前先跑它，任何架构失败就不打包。
- `scripts/deploy.sh`：默认仍装 x86_64 包到测试 NAS，可用 `ARCH=arm_64` 等覆盖。

**风险（不在本次解决）**
- ARM NAS 内存常见 1–2 GB；重复检查峰值目标 300 万条目约 300 MB，1 GB 机器上偏紧。
- 纯 Go 的 SQLite 在 ARM 上更慢，扫描和检查会比 x86 慢。
- 没有在真实 ARM QTS 上安装验证过。

## 留给实现者

脚本输出措辞；测试二进制的临时目录位置。
