# 通用 Linux 版 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 同一套代码编出两种二进制：QNAP 版行为完全不变；通用 Linux 版作为 systemd 常驻服务运行，内置单一密码登录、从 mountinfo 自动发现卷、进程内定时，并以 tar.gz / deb / rpm 发布，与 qpkg 一起由 GitHub Actions 出 Release。

**Architecture:** 用 build tag `qnap` 在编译时选平台：`cmd/storage-analyzer` 拆成公共部分加 `platform_qnap.go` / `platform_linux.go`。`api` 包里和 QNAP 绑定的 crontab 调用改成平台注入的钩子。Linux 专用的新包：`config`（配置文件）、`mounts`（卷发现）、`auth`（密码、会话、限速、登录）、`server`（HTTP 服务、嵌入的前端、安全头、TLS）、`scheduler`（进程内定时）。QNAP 二进制不导入这些包。前端共用，由 Linux 服务在返回 `index.html` 时写入 `data-platform="linux"`，前端据此切换文案并显示登录页。

**Tech Stack:** Go 1.26.5（`CGO_ENABLED=0`）、`modernc.org/sqlite`、`golang.org/x/sys`；新增 `golang.org/x/crypto`（argon2）、`golang.org/x/term`；原生 JS；systemd；nfpm（只在构建时用）；GitHub Actions。

**Spec:** `docs/superpowers/specs/2026-11-26-linux-platform-design.md`（基于 `docs/superpowers/specs/2026-11-16-qnap-storage-analyzer-design.md`）

## Global Constraints

- Build tag：`-tags qnap` → QNAP 二进制；不带 tag → 通用 Linux 二进制（macOS 上开发也编这个）。两种都要通过：`go vet ./...`、`go vet -tags qnap ./...`、`go test ./...`、`go test -tags qnap ./...`。
- 新依赖只允许 `golang.org/x/crypto`、`golang.org/x/term`。不引入 Web 框架、配置库、CLI 库。
- Module 路径：`github.com/FlintyLemming/storage-analyzer`。
- QNAP 不变：QPKG 名 `StorageAnalyzer`、安装与数据目录布局、`catalog.db` 和扫描库的格式、crontab 行 `<bin> scan --trigger schedule >/dev/null 2>&1 # StorageAnalyzer`、QTS 登录、界面文案。
- Linux 路径：二进制 `/usr/bin/storage-analyzer`（tar.gz 装到 `/usr/local/bin`）；配置 `/etc/storage-analyzer/config`；密码文件 = 配置文件同目录下的 `password`（`root:storage-analyzer 0640`）；数据目录默认 `/var/lib/storage-analyzer`（0700）；unit `storage-analyzer.service`；系统用户和组 `storage-analyzer`。
- 默认 `listen = :8099`。配置错误或没有密码时以退出码 **78** 退出。
- 密码：至少 **12** 个字符（按 Unicode 字符计）；argon2id `m=19456`（KiB）、`t=2`、`p=1`、盐 16 字节、输出 32 字节，PHC 格式 `$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`（无填充 base64）。
- 会话 cookie：名 `sa_session`，值为 32 字节随机数的无填充 base64url；`HttpOnly; SameSite=Strict; Path=/; Max-Age=2592000`，HTTPS 时加 `Secure`。有效期 30 天，从登录算起，不滑动。服务端只存 token 的 SHA-256。
- 限速：同一来源 15 分钟内失败 5 次 → 锁 15 分钟；全局每分钟最多 20 次密码校验；IPv6 按 /64 归为同一来源；同一时间只做一次哈希校验；失败响应延迟 1 秒。
- 登录请求体上限 1 KiB；其他 API 请求体上限 64 KiB。
- 错误码：`not_logged_in`（401，Linux 上响应带 `"auth":"password"`）、`bad_password`（401）、`too_many_attempts`（429，带 `Retry-After` 头和 `retryAfter` 秒数字段）、`bad_request`（400）。现有错误码不变。
- 失败日志固定格式：`login failed from <ip>`。
- 每个响应都带（逐字）：
  - `Content-Security-Policy: default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'`
  - `X-Frame-Options: DENY`
  - `X-Content-Type-Options: nosniff`
  - `Referrer-Policy: no-referrer`
  这些头只加在 Linux 服务上，QNAP 不加（它在 QTS 的 iframe 里）。
- 未加密提示文案（逐字）：中文 `连接未加密，建议只在局域网或 Tailscale 内使用，或者放到反向代理后面。`；英文 `This connection isn't encrypted. Use it only on your LAN or Tailscale, or put it behind a reverse proxy.`
- 所有新增界面文案都要有中英文两份。
- 有两处由本计划决定、spec 没有写死的地方：
  - 前端怎么知道自己在哪个平台：spec §9 把字段位置留给实现者。本计划改为在 `index.html` 上写 `data-platform="linux"`，这样页面一加载就知道，不用等第一个 API 请求。401 响应里照样带 `"auth":"password"`。
  - 自动发现时跳过 btrfs 上名为 `.snapshots` 的挂载点：snapper 快照逐个统计会让同一块空间被算很多遍，违反 spec 的反目标。
- 不在计划里提交 git，用户要求时再提交（沿用之前计划的约定）。

## Review Focus

1. **同一挂载点被挂了两次**（例如 tmpfs 盖在 `/mnt/x` 的 ext4 上）：只有最上层可见，扫描时走到的是上层 → 只按最后一条判断，上层不在白名单就整个跳过 → Task 5 `TestSelectOvermount`。
2. **卷根是 `/`**：扫描进度里的当前路径、查重结果的显示路径和实际路径都不能出现 `//etc` → Task 3 `TestWalkRootSlashPaths`、`TestPatherRootSlash`。
3. **服务运行中改了密码**：新密码立刻生效，旧会话立刻失效，不用重启 → Task 7 `TestPasswordFileReload`、Task 10 `TestPasswordChangeInvalidatesSessions`。
4. **放在反代后面但没配 `trusted_proxies`**：所有人都显示为代理的 IP，一个人输错 5 次会把所有人锁住；而配置之后，伪造的 `X-Forwarded-For` 必须被忽略 → Task 9 的 `ClientIP` 测试；README 的 HTTPS 一节写明这一点（Task 17）。
5. **没设密码就启动服务**：以 78 退出，systemd 不会反复重启，`systemctl status` 里能看到提示要运行 `storage-analyzer passwd` → Task 13 `TestServeWithoutPasswordExits78`。

---

### Task 1: Module 路径改名

**Files:**
- Modify: `go.mod`，以及所有 import 了 `storage-analyzer/...` 的 `.go` 文件

**Interfaces:**
- Produces: 所有内部包的导入路径变为 `github.com/FlintyLemming/storage-analyzer/internal/...`

- [ ] **Step 1:** `go.mod` 的 `module storage-analyzer` 改为 `module github.com/FlintyLemming/storage-analyzer`；所有 `"storage-analyzer/` 开头的 import 替换为 `"github.com/FlintyLemming/storage-analyzer/`（可用 `gofmt -r` 或 sed，之后跑 `gofmt -l .` 确认没有格式问题）。
- [ ] **Step 2:** `go build ./... && go test ./...` → PASS。`grep -rn '"storage-analyzer/' --include='*.go' .` → 无输出。

### Task 2: API 改用平台钩子；runner 的 Discover 必填

**Files:**
- Modify: `internal/api/api.go`（`Config`、`New`、`ServeHTTP` 的 401 分支、`settings`、导出 `SpawnDetached`）
- Modify: `internal/api/api_test.go`（`newEnv`、`TestSettingsRoundTrip`、新增 `TestAuthRequiredReportsLoginKind`）
- Modify: `internal/api/spawn_test.go`（改用 `SpawnDetached`）
- Modify: `internal/runner/runner.go`（去掉 `o.Discover = volumes.Discover` 默认值）
- Modify: `cmd/storage-analyzer/main.go`（暂时按新 `Config` 接线，行为不变）

**Interfaces:**
- Produces:
  ```go
  type Config struct {
      DataDir       string
      BinPath       string
      Auth          Authenticator
      Spawn         func(args ...string) error      // nil: SpawnDetached(BinPath, args...)
      ApplySchedule func(catalog.Settings) error    // 设置保存成功后调用；nil 表示不需要
      LoginKind     string                          // 非空时 401 响应带 "auth": LoginKind
  }
  func SpawnDetached(bin string, args ...string) error   // 原 spawnDetached，行为不变
  ```
  `api` 包不再导入 `schedule`，也没有 `CrontabPath`、`RunCmd` 字段。
- `runner.Run`、`runner.RunScheduled`：`o.Discover == nil` 时返回错误 `runner: Options.Discover is nil`，并且不新增扫描记录。

- [ ] **Step 1:** 改 `api_test.go`：
  - `newEnv` 用 `ApplySchedule: func(s catalog.Settings) error { e.applied = append(e.applied, s); return nil }`。
  - `TestSettingsRoundTrip`：保存 weekly 后断言 `len(e.applied) == 1 && e.applied[0].Weekday == 3 && e.applied[0].Time == "02:15"`；保存非法设置后 `len(e.applied)` 仍是 1。
  - 新增：
  ```go
  func TestAuthRequiredReportsLoginKind(t *testing.T) {
      e := newEnvCfg(t, fakeAuth{err: ErrNotLoggedIn}, func(c *Config) { c.LoginKind = "password" })
      rr, m := e.do("GET", "/api.cgi?action=status", "", nil)
      // rr.Code == 401 && m["error"] == "not_logged_in" && m["auth"] == "password"
  }
  ```
  `TestAuthRequired` 再加一条断言：`m` 里没有 `"auth"` 键（QNAP 不变）。crontab 的具体内容已由 `internal/schedule/cron_test.go` 覆盖，这里不再测。
- [ ] **Step 2:** `go test ./internal/api` → 编译失败（`ApplySchedule` 不存在）。
- [ ] **Step 3:** 按 Interfaces 改 `api.go`。`main.go` 的 `cgi` 分支传 `ApplySchedule: func(s catalog.Settings) error { return schedule.Apply(s, p.bin, crontabPath, runCmd) }`；dev `serve` 传一个只打日志的 `ApplySchedule`。在 runner 测试里加 `TestRunNilDiscover`：断言返回错误，且 `catalog.List()` 为空。
- [ ] **Step 4:** `go test ./...` → PASS。

### Task 3: 卷带文件系统类型；卷根为 `/` 时的路径拼接

**Files:**
- Modify: `internal/volumes/volumes.go`（`Volume` 加字段）
- Modify: `internal/runner/runner.go`（`VolumeMeta` 加字段并从 `Volume` 拷贝）
- Modify: `internal/scanner/walk.go:237`（`n.path + "/" + name`）
- Modify: `internal/dupcheck/check.go:93-110`（`pather.path`）
- Test: `internal/scanner/walk_test.go`、`internal/dupcheck/check_test.go`、`internal/runner/runner_test.go`

**Interfaces:**
- Produces: `volumes.Volume.FSType string \`json:"fsType,omitempty"\``；`runner.VolumeMeta.FSType string \`json:"fsType,omitempty"\``。QNAP 保持为空。

- [ ] **Step 1:** 写测试：
  - `TestWalkRootSlashPaths`：自定义 `Sink`，配合 `Walker` 的 `IsMountPoint` 钩子记录每次被调用的 `path`。用 `Root{Name: "/", Path: dir}` 跑一遍，断言所有记录到的路径都不含 `//`。为了不依赖真实的 `/`，在测试里让 `dir` 以 `/` 结尾（`dir + "/"`），触发同样的拼接分支。
  - `TestPatherRootSlash`：构造 `pather{roots: {1: {Name: "/", MountPoint: "/"}}}`，目录 1 是根、目录 2 名为 `etc`、父目录为 1。断言 `path(2, []byte("passwd"), false) == "/etc/passwd"`，`path(1, []byte("x"), true) == "/x"`。
  - `runner_test.go`：假的 Discover 返回 `FSType: "ext4"`，断言扫描库 meta 里 volumes 的 JSON 含 `"fsType":"ext4"`。
- [ ] **Step 2:** `go test ./internal/scanner ./internal/dupcheck ./internal/runner` → FAIL。
- [ ] **Step 3:** 两处拼接改为：前缀以 `/` 结尾时不再加 `/`（各写一个小的 `join(dir, name string) string`）。加 `FSType` 字段并拷贝。
- [ ] **Step 4:** `go test ./...` → PASS。

### Task 4: Linux 配置文件

**Files:**
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Produces:
  ```go
  package config
  const DefaultPath = "/etc/storage-analyzer/config"
  type Config struct {
      Listen         string         // 默认 ":8099"
      TLSCert        string
      TLSKey         string
      TrustedProxies []netip.Prefix // 单个地址存为 /32 或 /128
      DataDir        string         // 默认 "/var/lib/storage-analyzer"
      Paths          []string       // nil = 自动发现
      Exclude        []string
      Dir            string         // 配置文件所在目录（Load 设置；Parse 留空）
  }
  func Parse(r io.Reader) (Config, error)
  func Load(path string) (Config, error)        // Parse 后设置 Dir = filepath.Dir(绝对路径)
  func (c Config) PasswordPath() string         // filepath.Join(c.Dir, "password")
  ```

- [ ] **Step 1:** 写表驱动测试 `TestParse`，覆盖以下用例：
  - 空输入：`Listen == ":8099"`，`DataDir == "/var/lib/storage-analyzer"`，`Paths == nil`。
  - spec §7.2 的示例（去掉注释符号后的全部键）都能解析。
  - `paths = auto` → `nil`；`paths = /, /srv/data` → `[]string{"/", "/srv/data"}`；路径会先 `filepath.Clean`。
  - `trusted_proxies = 127.0.0.1, ::1, 10.0.0.0/8` → `127.0.0.1/32`、`::1/128`、`10.0.0.0/8`。
  - 允许 CRLF 换行、文件开头的 UTF-8 BOM、键值前后的空格、`#` 注释行和空行。
  - 以下情况报错，错误信息要带行号：未知键（信息里带键名）、同一个键出现两次、没有 `=` 的行、`paths`/`exclude`/`data_dir`/`tls_*` 里出现相对路径、只配了 `tls_cert` 或只配了 `tls_key`、`listen = 8099`（`net.SplitHostPort` 失败）、`trusted_proxies = abc`。
  - 另写 `TestLoadSetsDir`：`Load(tmp/config)` 后，`PasswordPath() == tmp/password`。
- [ ] **Step 2:** `go test ./internal/config` → 编译失败。
- [ ] **Step 3:** 实现。按行解析，用 `strings.Cut(line, "=")` 拆键值。
- [ ] **Step 4:** `go test ./internal/config` → PASS。

### Task 5: 从 mountinfo 发现卷

**Files:**
- Create: `internal/mounts/mounts.go`
- Create: `internal/mounts/testdata/{ubuntu-ext4,btrfs-subvol,proxmox-zfs,docker-host,network,systemd-strict,bind,overmount,escape}.txt`
- Modify: `internal/volumes/volumes.go`（导出 `UnescapeMount`，原 `unescapeMount` 的调用改过去）
- Test: `internal/mounts/mounts_test.go`

**Interfaces:**
- Consumes: `config.Config`（Task 4）、`volumes.Volume.FSType`（Task 3）
- Produces:
  ```go
  package mounts
  type Mount struct {
      ID, Parent int
      Dev        string // "maj:min"
      Root       string
      MountPoint string
      FSType     string
      Source     string
  }
  var LocalTypes = map[string]bool{ /* ext2 ext3 ext4 xfs btrfs zfs f2fs bcachefs jfs reiserfs ntfs ntfs3 exfat vfat fuseblk */ }
  func Parse(r io.Reader) ([]Mount, error)
  func Select(ms []Mount, exclude []string) []volumes.Volume
  func FromPaths(paths []string, ms []Mount) ([]volumes.Volume, []volumes.Skipped)
  func Discover(c config.Config) ([]volumes.Volume, []volumes.Skipped, error) // 读 /proc/self/mountinfo
  ```

`Select` 的算法如下（测试也按这个写）：

```
1. 同一个 MountPoint 出现多次时，只保留 mountinfo 里最后一条（最上层）。
2. 丢掉 FSType 不在 LocalTypes 里的。
3. 丢掉 FSType == "btrfs" 且 filepath.Base(MountPoint) == ".snapshots" 的
   （snapper 快照，逐个算会重复计算，违反 spec 的反目标）。
4. 按 Dev 分组；组内按 (len(Root), len(MountPoint), MountPoint) 排序，依次检查：
   如果已保留的挂载里有某个 k 满足 m.Root == k.Root
   或 strings.HasPrefix(m.Root, k.Root 加上结尾的 "/")（k.Root == "/" 时视为所有路径的祖先），
   就丢掉 m；否则保留。
5. 去掉 MountPoint 在 exclude 里的（按 filepath.Clean 后精确匹配）。
6. 按 MountPoint 排序，输出 Volume{Name: mp, MountPoint: mp, Device: Source, FSType: FSType}。
```

- [ ] **Step 1:** 写 fixture，每个文件都是真实格式的 mountinfo（`36 25 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw`），内容和期望如下：

  | fixture | 内容要点 | `Select(…, nil)` 期望的 MountPoint |
  |---|---|---|
  | ubuntu-ext4 | `/` ext4、`/boot/efi` vfat、proc/sysfs/tmpfs/devtmpfs/cgroup2、snap 的 squashfs | `/`、`/boot/efi` |
  | btrfs-subvol | 同一个 Dev：`/`（root=`/@`）、`/home`（root=`/@home`）、`/.snapshots`（root=`/@snapshots`） | `/`、`/home` |
  | proxmox-zfs | `/` zfs、`/rpool`、`/rpool/data`、`/rpool/data/subvol-100-disk-0`，各自不同 Dev | 4 个全保留 |
  | docker-host | `/` ext4，多个 overlay 在 `/var/lib/docker/overlay2/*/merged`，nsfs、shm | `/` |
  | network | `/` ext4、`/mnt/nas` nfs4、`/mnt/smb` cifs、`/mnt/gd` fuse.rclone、`/media/usb` fuseblk | `/`、`/media/usb` |
  | systemd-strict | 和 ubuntu-ext4 相同，外加一条与 `/` 同 Dev、root 为 `/var/lib/storage-analyzer`、挂载点为 `/var/lib/storage-analyzer` 的记录 | `/`、`/boot/efi` |
  | bind | `/` ext4；`/mnt/data` xfs（Dev 8:17，root=`/`）；`/data`（8:17，root=`/`）；`/srv`（8:17，root=`/srv`） | `/`、`/data` |
  | overmount | `/` ext4、`/mnt/x` ext4（另一个 Dev），之后又有 tmpfs 挂在 `/mnt/x` | `/` |
  | escape | `/mnt/my\040disk` ext4 | `/`、`/mnt/my disk` |

  测试：
  - `TestSelectFixtures`：按上表逐个断言 MountPoint 列表。
  - `TestSelectOvermount`：单独断言 overmount 的结果。
  - `TestSelectExclude`：ubuntu-ext4 配 `exclude = []string{"/boot/efi/"}` → 只剩 `/`。
  - `TestSelectVolumeFields`：ubuntu-ext4 的 `/` → `Device == "/dev/sda2"`、`FSType == "ext4"`、`Name == "/"`。
  - `TestParseMalformed`：缺少 `-` 分隔符的行 → 报错。
  - `TestFromPaths`：用 t.TempDir 建 `a`、`a/b`、`c`，传入 `[a, a/b, c, missing]` → 卷为 `a`、`c`；Skipped 为 `{Name: missing, Reason: "not_found"}`；`ms` 里有一条挂载点为 TempDir 根的记录时，卷的 `Device`、`FSType` 取自那条记录（按最长挂载点前缀匹配）。
- [ ] **Step 2:** `go test ./internal/mounts` → 编译失败。
- [ ] **Step 3:** 实现。`Discover`：`c.Paths != nil` 时读 mountinfo（读不到就当作空），调用 `FromPaths`；否则读 mountinfo（读不到就报错），返回 `Select(ms, c.Exclude)` 和空的 Skipped。`FromPaths` 中被包含的路径丢掉，并 `log.Printf("path %s is inside %s, skipped", …)`。
- [ ] **Step 4:** `go test ./internal/mounts ./internal/volumes` → PASS。

### Task 6: 拆分 main，按平台编译

**Files:**
- Modify: `cmd/storage-analyzer/main.go`（只留公共部分）
- Create: `cmd/storage-analyzer/platform_qnap.go`（`//go:build qnap`）
- Create: `cmd/storage-analyzer/platform_linux.go`（`//go:build !qnap`）
- Rename: `cmd/storage-analyzer/main_linux_test.go` → `main_qnap_test.go`，build 约束改为 `//go:build linux && qnap`
- Create: `cmd/storage-analyzer/main_generic_test.go`（`//go:build linux && !qnap`）
- Create: `cmd/storage-analyzer/exit_test.go`（`//go:build !qnap`）
- Modify: `scripts/test-linux.sh`（支持 `TAGS`）、`scripts/build.sh`（qpkg 用 `-tags qnap` 编译；两种 tag 都测试）

**Interfaces:**
- Consumes: `config.Load`、`mounts.Discover`（Task 4、5），`api.Config.ApplySchedule`（Task 2）
- Produces（`main` 包内部，两个平台文件各自实现同名符号）：
  ```go
  // main.go（公共）
  type app struct {
      data      string                                              // 数据目录
      bin       string                                              // 本可执行文件
      discover  func() ([]volumes.Volume, []volumes.Skipped, error)
      childArgs []string                                            // 拉起子进程时追加：QNAP 为 nil；Linux 为 {"--config", <path>}
  }
  type exitError struct{ code int; err error }                     // main 按 code 退出；配置错误用 78
  var errUnknownCommand = errors.New("unknown command")
  var version = "dev"                                               // -ldflags "-X main.version=…"
  func prepareData(data string) error  // 原 initStart 去掉 applySchedule：建 scans/run/logs、CleanupStaleTmp、MarkStaleRunning
  // 公共子命令：scan、dupcheck、prune、version

  // platform_*.go
  type platformFlags struct{ /* 各平台自己的参数 */ }
  func addPlatformFlags(fs *flag.FlagSet) *platformFlags
  func newApp(pf *platformFlags) (*app, error)
  func runPlatform(cmd string, a *app, pf *platformFlags) error     // 不认识的命令返回 errUnknownCommand
  const usage = "…"
  ```
  - QNAP：参数 `--root`；`newApp` 沿用 `resolvePaths`；`discover = volumes.Discover`（`SA_DEV_ROOTS` 删掉，开发改用 Linux 版）；子命令 `cgi`、`apply-schedule`、`init-start`（= `prepareData` + `applySchedule`）、`init-stop`。不再有 `serve`。
  - Linux：参数 `--config`（默认 `config.DefaultPath`）、`--addr`、`--web`、`--insecure-no-auth`；`newApp` 调用 `config.Load`，失败时返回 `exitError{78, …}`；`data = cfg.DataDir`；`discover = func() { return mounts.Discover(cfg) }`；子命令暂时只有开发用的 `serve --insecure-no-auth`，从原 `main.go` 搬过来，`devSpawn` 改为追加 `childArgs`。Task 13 再补正式的 `serve` 和 `passwd`。

- [ ] **Step 1:** 写测试：
  - `main_generic_test.go` `TestSQLiteTempFilesGoToDataDir`：照搬 QNAP 版的子进程探测，改成在临时目录写 `config`（`data_dir = <root>/data`），运行 `prune --config <root>/config`，断言临时文件落在 `<root>/data`。
  - `exit_test.go` `TestBadConfigExits78`：子进程运行 `prune --config <不存在的路径>` → 退出码 78，stderr 包含该路径。
- [ ] **Step 2:** `go test ./cmd/...` → FAIL。
- [ ] **Step 3:** 按 Interfaces 拆分。`scripts/test-linux.sh` 读取 `TAGS` 环境变量，`go list` 和 `go test -c` 都加上 `-tags "$TAGS"`。`build.sh` 依次运行 `go test ./...`、`go test -tags qnap ./...`、`./scripts/test-linux.sh`、`TAGS=qnap ./scripts/test-linux.sh`；qpkg 的交叉编译加 `-tags qnap`。
- [ ] **Step 4:** `go vet ./... && go vet -tags qnap ./... && go test ./... && go test -tags qnap ./...` → PASS。
- [ ] **Step 5:** `./scripts/test-linux.sh && TAGS=qnap ./scripts/test-linux.sh` → 每个架构都是 `ok`。
- [ ] **Step 6:** 符号检查：`GOOS=linux go build -o build/sa-linux ./cmd/storage-analyzer && go tool nm build/sa-linux | grep -E 'QTSAuth|net/http/cgi'` → 无输出。

### Task 7: 密码哈希与密码文件

**Files:**
- Create: `internal/auth/password.go`
- Test: `internal/auth/password_test.go`
- Modify: `go.mod`/`go.sum`（`golang.org/x/crypto`）

**Interfaces:**
- Produces:
  ```go
  package auth
  const MinPasswordLen = 12
  var ErrTooShort = errors.New("password must be at least 12 characters")
  func CheckPolicy(pw string) error
  func Hash(pw string) (string, error)            // PHC，参数见 Global Constraints
  func Verify(phc, pw string) (bool, error)       // 参数从 phc 里读；用 subtle.ConstantTimeCompare；格式不对返回 error
  func Fingerprint(phc string) string             // hex(sha256(phc)) 的前 32 个字符
  type PasswordFile struct{ Path string /* 其余字段私有，带锁 */ }
  func (p *PasswordFile) Current() (string, error) // (mtime, size) 变了才重读；文件不存在时返回 fs.ErrNotExist
  func WritePasswordFile(path, phc string) error  // 同目录临时文件 + rename，权限 0640；以 root 运行且组 storage-analyzer 存在时 chown 为 0:<该组>
  ```

- [ ] **Step 1:** 写测试：
  - `TestHashVerify`：`Hash("correct horse battery")`，用同一个密码 `Verify` → `true, nil`，用 `"wrong horse battery"` → `false, nil`。
  - `TestHashFormat`：结果以 `$argon2id$v=19$m=19456,t=2,p=1$` 开头；对同一密码哈希两次，结果不同（盐不同）。
  - `TestVerifyMalformed`：`Verify("$argon2id$garbage", "x")` 返回 error。
  - `TestCheckPolicy`：`"12345678901"` → `ErrTooShort`；`"123456789012"` → nil；`strings.Repeat("密", 12)` → nil。
  - `TestPasswordFileReload`：先写入 A，`Current() == A`；再写入 B，并用 `os.Chtimes` 把 mtime 推后 2 秒 → `Current() == B`。
  - `TestPasswordFileMissing`：`errors.Is(err, fs.ErrNotExist)`。
  - `TestWritePasswordFileMode`：`Stat().Mode().Perm() == 0o640`。
- [ ] **Step 2:** `go test ./internal/auth` → 编译失败。
- [ ] **Step 3:** 实现（`golang.org/x/crypto/argon2.IDKey`）。
- [ ] **Step 4:** `go test ./internal/auth` → PASS。

### Task 8: 会话存储

**Files:**
- Create: `internal/auth/sessions.go`
- Test: `internal/auth/sessions_test.go`

**Interfaces:**
- Produces:
  ```go
  const SessionTTL = 30 * 24 * time.Hour
  type Sessions struct{ /* *sql.DB */ }
  func OpenSessions(path string) (*Sessions, error)  // SQLite；表 sessions(hash BLOB PRIMARY KEY, created INTEGER, expires INTEGER, pwfp TEXT)；文件权限 0600
  func (s *Sessions) Create(pwfp string, now time.Time) (token string, err error) // 顺便清理过期会话
  func (s *Sessions) Valid(token, pwfp string, now time.Time) bool
  func (s *Sessions) Delete(token string) error
  func (s *Sessions) Close() error
  ```
  Linux 上的路径是 `<data>/sessions.db`，由 Task 13 拼出。QNAP 数据目录里不会出现这个文件。

- [ ] **Step 1:** 写测试：
  - `TestSessionLifecycle`：`Create` 后 `Valid == true`；`len(token) == 43`；在 `now + SessionTTL + time.Second` 时 `Valid == false`；用别的 `pwfp` 时 `Valid == false`；`Delete` 后 `Valid == false`。
  - `TestSessionsSurviveReopen`：`Close` 再 `OpenSessions` 同一个文件后，仍然 `Valid`。
  - `TestSessionsStoreNoRawToken`：读出 db 文件的全部字节，`bytes.Contains(raw, []byte(token)) == false`。
  - `TestSessionsFileMode`：权限为 0600。
- [ ] **Step 2:** `go test ./internal/auth -run Session` → FAIL。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** `go test ./internal/auth` → PASS。

### Task 9: 限速与来源 IP

**Files:**
- Create: `internal/auth/limit.go`、`internal/auth/clientip.go`
- Test: `internal/auth/limit_test.go`、`internal/auth/clientip_test.go`

**Interfaces:**
- Produces:
  ```go
  const (
      MaxFailures     = 5
      FailureWindow   = 15 * time.Minute
      LockoutDuration = 15 * time.Minute
      GlobalPerMinute = 20
  )
  func NewLimiter() *Limiter
  func (l *Limiter) Allow(ip netip.Addr, now time.Time) (retryAfter time.Duration, ok bool) // ok 时占用一次全局额度
  func (l *Limiter) Fail(ip netip.Addr, now time.Time)                                      // 第 5 次失败时开始锁定；同时清理过期记录
  func (l *Limiter) Succeed(ip netip.Addr)
  func ClientIP(r *http.Request, trusted []netip.Prefix) netip.Addr
  func IsHTTPS(r *http.Request, trusted []netip.Prefix) bool
  ```
  来源的键：IPv4 取地址本身（先 `Unmap`），IPv6 取 /64 前缀。只有 `Allow` 成功之后才会调用 `Fail`，而全局额度每分钟 20 次，所以表的大小天然有上限，不需要另设上限。

- [ ] **Step 1:** 写测试（`t0 := time.Date(2026,1,1,0,0,0,0,time.UTC)`）：
  - `TestLockoutAfterFiveFailures`：在 t0 连续失败 5 次；`t0+1m` 时 `Allow` → `ok == false`，且 `retryAfter` 在 `(13m, 15m]` 之间；锁定结束后 1 秒 → `ok == true`。
  - `TestFailuresOutsideWindow`：t0 失败 4 次，`t0+16m` 再失败 1 次 → 仍然 `ok`。
  - `TestSucceedClears`：失败 4 次 → `Succeed` → 再失败 4 次 → 仍然 `ok`。
  - `TestGlobalBudget`：一分钟内来自 20 个不同 IP 的 `Allow` 都 `ok`，第 21 次 `!ok`；`+61s` 后 `ok`。
  - `TestIPv6Slash64`：`2001:db8::1` 到 `2001:db8::5` 各失败一次 → `2001:db8::ffff` 被锁；`2001:db8:0:1::1` 不受影响。
  - `TestClientIP`（表驱动）：

    | RemoteAddr | trusted | X-Forwarded-For | 期望 |
    |---|---|---|---|
    | `203.0.113.9:1234` | 无 | `1.2.3.4` | `203.0.113.9` |
    | `127.0.0.1:5555` | `127.0.0.1/32` | `6.6.6.6, 198.51.100.7` | `198.51.100.7` |
    | `127.0.0.1:5555` | `127.0.0.1/32` | `198.51.100.7, 127.0.0.1` | `198.51.100.7` |
    | `127.0.0.1:5555` | `127.0.0.1/32` | （无） | `127.0.0.1` |
    | `[::ffff:10.0.0.1]:1` | 无 | （无） | `10.0.0.1` |

  - `TestIsHTTPS`：`r.TLS != nil` → true；可信对端带 `X-Forwarded-Proto: https` → true；不可信对端带同样的头 → false。
- [ ] **Step 2:** `go test ./internal/auth -run 'Lock|Fail|Succeed|Global|IPv6|ClientIP|IsHTTPS'` → FAIL。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** `go test ./internal/auth` → PASS。

### Task 10: 登录与退出

**Files:**
- Create: `internal/auth/login.go`
- Test: `internal/auth/login_test.go`

**Interfaces:**
- Consumes: Task 7–9 的全部导出符号；`api.ErrNotLoggedIn`
- Produces:
  ```go
  const CookieName = "sa_session"
  type Login struct {
      Password  *PasswordFile
      Sessions  *Sessions
      Limiter   *Limiter
      Trusted   []netip.Prefix
      Now       func() time.Time
      FailDelay time.Duration
      // 私有：容量为 1 的信号量，保证同一时间只做一次哈希校验
  }
  func NewLogin(pw *PasswordFile, s *Sessions, l *Limiter, trusted []netip.Prefix) *Login // Now = time.Now，FailDelay = time.Second
  func (l *Login) Check(r *http.Request) (string, bool, error) // 实现 api.Authenticator：返回 ("admin", true, nil) 或 api.ErrNotLoggedIn
  func (l *Login) ServeHTTP(w http.ResponseWriter, r *http.Request) // 按 ?action=login|logout 分发
  ```
  `login` 的处理顺序：
  1. 不是 POST → 405 `method_not_allowed`。
  2. 没有 `X-SA-Request: 1` → 403 `bad_request_origin`。
  3. 请求体大于 1 KiB，或不是 `{"password": string}` 的 JSON → 400 `bad_request`。
  4. `Allow` 不通过 → 429 `too_many_attempts`，带 `Retry-After` 头（向上取整的秒数）和 `retryAfter` 字段。
  5. 拿信号量 → `Password.Current()`（出错则 500 `internal`，并写日志）→ `Verify` → 释放信号量。
  6. 密码错误：`Limiter.Fail`，写日志 `login failed from <ip>`，等待 `FailDelay`，返回 401 `bad_password`。
  7. 密码正确：`Succeed`，`Sessions.Create(Fingerprint(phc))`，下发 cookie，返回 200 `{"ok":true}`。

  `logout`：同样要求 POST 加请求头；删除 cookie 对应的会话；下发 `Max-Age=0` 的同名 cookie；返回 200。

  `Check` 每次都用 `Password.Current()` 的指纹去校验会话，所以改密码之后旧会话会立刻失效。

- [ ] **Step 1:** 写测试（`FailDelay = 0`；用 `Hash("correct horse battery")` 写一个临时密码文件；RemoteAddr 为 `192.0.2.1:1000`）：
  - `TestLoginSuccessSetsCookie`：返回 200；cookie 满足 `Name == "sa_session"`、`HttpOnly`、`SameSite == http.SameSiteStrictMode`、`Path == "/"`、`MaxAge == 2592000`、`!Secure`；把这个 cookie 带进新请求调用 `Check` → `("admin", true, nil)`。
  - `TestLoginSecureCookieOverTLS`：`r.TLS = &tls.ConnectionState{}` → cookie 带 `Secure`。
  - `TestLoginWrongPassword`：返回 401 `bad_password`；没有 `Set-Cookie`；用 `log.SetOutput` 捕获的日志里包含 `login failed from 192.0.2.1`。
  - `TestLoginLockout`：错 5 次后，第 6 次即使密码正确也返回 429 `too_many_attempts`，且 `Retry-After` 非空。
  - `TestLoginMethodAndHeader`：GET → 405；POST 但没有 `X-SA-Request` → 403 `bad_request_origin`。
  - `TestLoginBodyTooLarge`：2048 字节的请求体 → 400 `bad_request`。
  - `TestLogout`：登录 → 退出 → 拿原来的 cookie 调用 `Check` → `api.ErrNotLoggedIn`。
  - `TestPasswordChangeInvalidatesSessions`：登录后换一个新哈希写入密码文件（推后 mtime）→ 旧 cookie 调用 `Check` 失败；用新密码登录成功。
  - `TestCheckNoCookie` → `api.ErrNotLoggedIn`。
- [ ] **Step 2:** `go test ./internal/auth -run 'Login|Logout|Password|Check'` → FAIL。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** `go test ./internal/auth` → PASS。

### Task 11: HTTP 服务（嵌入前端、安全头、超时、TLS）

**Files:**
- Create: `web/embed.go`
- Create: `internal/server/server.go`、`internal/server/tls.go`
- Test: `internal/server/server_test.go`、`internal/server/tls_test.go`

**Interfaces:**
- Produces:
  ```go
  package web
  //go:embed index.html app.js i18n.js style.css
  var Files embed.FS

  package server
  type Options struct {
      API    http.Handler // api.New(...)
      Login  http.Handler // *auth.Login；开发模式下为 nil（此时 login/logout 返回 404）
      Static fs.FS        // web.Files，或开发模式下的 os.DirFS(--web)
  }
  func Handler(o Options) http.Handler
  func NewServer(addr string, h http.Handler) *http.Server // ReadHeaderTimeout 10s, ReadTimeout 30s, WriteTimeout 60s, IdleTimeout 120s, MaxHeaderBytes 16<<10
  type CertReloader struct{ CertFile, KeyFile string /* 私有缓存 */ }
  func (c *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) // 每次握手都 stat 两个文件，任一 mtime 变了就重新加载
  func TLSConfig(c *CertReloader) *tls.Config // MinVersion: tls.VersionTLS12
  ```
  路由规则：
  - `/api.cgi`：`action` 为 `login` 或 `logout` → `Login`；其他 → `API`。请求体用 `http.MaxBytesReader` 限制在 64 KiB。
  - `/` 和 `/index.html`：返回 index，把第一个 `<html lang="en">` 替换成 `<html lang="en" data-platform="linux">`。找不到这个标记时，`Handler` 直接 panic；有测试保证这种情况不会发生。
  - `/app.js`、`/i18n.js`：`text/javascript; charset=utf-8`。`/style.css`：`text/css; charset=utf-8`。index：`text/html; charset=utf-8`。静态文件都带 `Cache-Control: no-cache`，只接受 GET 和 HEAD，其他方法返回 405。
  - 其他路径一律 404。
  - 所有响应（包括 404 和 405）都带 Global Constraints 里的四个安全头。

- [ ] **Step 1:** 写测试（`API` 和 `Login` 用桩 handler，记录自己是否被调用）：
  - `TestSecurityHeaders`：对 `/`、`/app.js`、`/api.cgi?action=status`、`/nope` 逐一断言四个头的值完全等于常量。
  - `TestIndexPlatform`：响应体包含 `<html lang="en" data-platform="linux">`。
  - `TestUnknownPaths404`：`/etc/passwd`、`/web/app.js`、`/index.html/`、`/APP.JS` → 404。
  - `TestStaticTypes`：三种 Content-Type；POST `/app.js` → 405。
  - `TestAPIRouting`：`action=login` → 进 `Login` 桩；`action=status` → 进 `API` 桩；`Login == nil` 时 `action=login` → 404。
  - `TestAPIBodyLimit`：向 API 桩 POST 100 KiB，桩里的 `io.ReadAll(r.Body)` 返回 `*http.MaxBytesError`。
  - `TestEmbeddedFiles`：`fs.Stat(web.Files, name)` 对四个文件都成功。
  - `TestNewServerTimeouts`：断言 `NewServer` 返回的五个字段值。
  - `TestCertReloader`（`tls_test.go`）：测试里用 `crypto/x509` 生成自签证书 A，写入临时文件，`GetCertificate` 的 leaf 序列号等于 A；再写入证书 B 并推后 mtime，得到 B。
- [ ] **Step 2:** `go test ./internal/server ./web` → 编译失败。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** `go test ./internal/server ./web` → PASS。

### Task 12: 进程内定时

**Files:**
- Create: `internal/scheduler/scheduler.go`
- Test: `internal/scheduler/scheduler_test.go`

**Interfaces:**
- Consumes: `catalog.Settings`
- Produces:
  ```go
  package scheduler
  func Due(s catalog.Settings, now time.Time) bool  // daily：now 的 "15:04" == s.Time；weekly：另外要求 int(now.Weekday()) == s.Weekday；其他模式返回 false
  type Scheduler struct {
      Settings func() (catalog.Settings, error)
      Fire     func() error
      Now      func() time.Time // nil 时用 time.Now
      // 私有：last string —— 上次触发的 now.Format("2006-01-02") + " " + s.Time
  }
  func (s *Scheduler) Tick()                     // Due 且 key != last 时：先设 last，再调用 Fire；错误只写日志
  func (s *Scheduler) Run(ctx context.Context)   // 每次睡到下一个整分钟再加 1 秒，然后 Tick；ctx 取消时返回
  ```

- [ ] **Step 1:** 写测试：
  - `TestDueDaily`：Time `"03:00"`：在 03:00:30 → true；03:01:00 → false。
  - `TestDueWeekly`：Weekday 3，周三 03:00 → true；周四 03:00 → false。
  - `TestDueOff`：mode `"off"` → false。
  - `TestTickOncePerKey`：在 03:00:01 和 03:00:59 各 Tick 一次 → Fire 只调用 1 次。
  - `TestTickDSTRepeat`：两个不同的时刻，本地时间都是 2026-11-01 01:30（分别用 `time.FixedZone("PDT", -7*3600)` 和 `time.FixedZone("PST", -8*3600)` 构造），Time `"01:30"` → Fire 只调用 1 次。
  - `TestTickNewTimeSameDay`：03:00 触发后把设置改成 `"15:00"`，在 15:00 Tick → 再触发一次。
  - `TestTickSettingsError`：`Settings` 返回 error → 不调用 Fire，也不 panic。
- [ ] **Step 2:** `go test ./internal/scheduler` → 编译失败。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** `go test ./internal/scheduler` → PASS。

### Task 13: Linux `serve` / `passwd` 接线

**Files:**
- Modify: `cmd/storage-analyzer/platform_linux.go`
- Create: `cmd/storage-analyzer/serve_test.go`（`//go:build !qnap`）
- Modify: `go.mod`/`go.sum`（`golang.org/x/term`）
- Modify: `.claude/launch.json`

**Interfaces:**
- Consumes: Task 4–12 的全部导出符号，`api.SpawnDetached`、`runner.Cancel`、`prepareData`
- Produces:
  - `serve`（正式模式），依次执行：
    1. `prepareData(a.data)`。
    2. `PasswordFile{Path: cfg.PasswordPath()}.Current()` 返回 `fs.ErrNotExist` 时 → `exitError{78, fmt.Errorf("no password set: run \"sudo storage-analyzer passwd\" first")}`。
    3. `auth.OpenSessions(filepath.Join(a.data, "sessions.db"))`，然后 `auth.NewLogin(…, cfg.TrustedProxies)`。
    4. `api.New(api.Config{DataDir: a.data, BinPath: a.bin, Auth: login, Spawn: spawn, LoginKind: "password"})`，其中 `spawn(args...) = api.SpawnDetached(a.bin, append(args, a.childArgs...)...)`，不设 `ApplySchedule`。
    5. `server.Handler(server.Options{API, Login, Static: web.Files})`，`server.NewServer(cfg.Listen, h)`；配了 TLS 时用 `TLSConfig(&CertReloader{…})` 和 `ListenAndServeTLS("", "")`。
    6. 启动 `scheduler.Scheduler{Settings: 每次打开 catalog 读设置, Fire: spawn("scan","--trigger","schedule")}.Run(ctx)`。
    7. 收到 SIGTERM 或 SIGINT：先 `runner.Cancel(a.data)`（忽略 `ErrNotRunning`），再 `srv.Shutdown`（超时 5 秒），以 0 退出。
    8. 启动日志：`listening on <addr> (http|https)`；没配 TLS 时再写一行 `warning: serving plain HTTP`。
  - `serve --insecure-no-auth`（开发模式）：保留 Task 6 的行为（只允许回环地址、所有请求放行）。改成通过 `server.Handler(Options{API, Login: nil, Static: 给了 --web 就用 os.DirFS(--web)，否则用 web.Files})` 提供服务，这样开发时也带安全头和 `data-platform`。
  - `passwd`：
    - stdin 是终端（`term.IsTerminal`）时，提示 `New password: ` 和 `Repeat password: `，两次输入不一致就报错。
    - 不是终端时，从 stdin 读一行，去掉行尾的 `\r\n`。
    - 然后 `auth.CheckPolicy` → `auth.Hash` → `auth.WritePasswordFile(cfg.PasswordPath(), phc)`，输出 `password updated; all sessions are signed out`。
- `.claude/launch.json`：`storage-analyzer-dev` 先用 `mkdir -p build/dev && printf` 生成 `build/dev/config`（`data_dir = <绝对路径>/build/dev/data`，`paths = $HOME/Projects`），然后执行 `go build -o build/dev/bin/storage-analyzer ./cmd/storage-analyzer && exec build/dev/bin/storage-analyzer serve --config build/dev/config --insecure-no-auth --web web`，端口 8099。

- [ ] **Step 1:** 写测试。都用子进程重新执行测试二进制，与 Task 6 的写法相同：
  - `TestServeWithoutPasswordExits78`：配置指向临时目录，没有密码文件 → 退出码 78，stderr 包含 `storage-analyzer passwd`。
  - `TestPasswdFromStdin`：stdin 为 `"correct horse battery\n"` → 密码文件存在，且 `auth.Verify(内容, "correct horse battery") == true`；stdin 为 `"short\n"` → 非零退出，不生成文件。
  - `TestServeEndToEnd`：用 `net.Listen("tcp","127.0.0.1:0")` 拿一个空闲端口后关闭；配置 `listen = 127.0.0.1:<port>`，先跑 `passwd` 设好密码，再起 `serve`。等端口可连后断言：
    1. `GET /` 返回 200，且带 CSP 头。
    2. `GET /api.cgi?action=status` 返回 401，响应 `"auth":"password"`。
    3. 带 `X-SA-Request: 1` POST `login` 返回 200，并拿到 cookie。
    4. 带上 cookie 再请求 `status`，返回 200。
    5. 向子进程发 SIGTERM，在 5 秒内以 0 退出。
- [ ] **Step 2:** `go test ./cmd/...` → FAIL。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** `go test ./... && go test -tags qnap ./...` → PASS；`./scripts/test-linux.sh && TAGS=qnap ./scripts/test-linux.sh` → 全部 ok。
- [ ] **Step 5:** 符号检查。下面两条命令都应该没有输出：
  ```bash
  GOOS=linux go build -o build/sa-linux ./cmd/storage-analyzer && go tool nm build/sa-linux | grep -E 'QTSAuth|net/http/cgi'
  GOOS=linux go build -tags qnap -o build/sa-qnap ./cmd/storage-analyzer && go tool nm build/sa-qnap | grep -E 'storage-analyzer/internal/(auth|server|scheduler|mounts|config)\.'
  ```

### Task 14: 前端

**Files:**
- Modify: `web/app.js`（`el()`、`ApiError`、`ERROR_CODES`、`showError`、登录视图、退出按钮、卷列表的小字）
- Modify: `web/i18n.js`（平台检测、Linux 覆盖文案、新文案）
- Modify: `web/index.html`（工具栏加 `#btnLogout`，默认隐藏）
- Modify: `web/style.css`（登录表单、未加密提示的样式）

**Interfaces:**
- Consumes: 服务端写入的 `<html data-platform="linux">`；登录接口与错误码（Task 10）；卷 meta 的 `fsType`（Task 3）
- Produces: `window.I18N.platform`，取值 `'linux'` 或 `'qnap'`（没有这个属性时为 `'qnap'`）

- [ ] **Step 1:** `el()` 遇到 `style` 时改为 `n.style.cssText = v`，不再用 `setAttribute`，否则会被 CSP 拦截。
- [ ] **Step 2:** `i18n.js`：
  - 读取 `document.documentElement.dataset.platform` 得到 `platform`。为 `'linux'` 时，用覆盖表 `zhLinux` / `enLinux` 合并到当前语言的字典上。
  - 需要覆盖的键共 12 个。把"NAS"改成"服务器 / server"，把"QTS 登录"改成"密码登录 / sign in"，把"所有存储池上的数据卷"改成"本机所有磁盘文件系统 / every local disk file system"：`net.down`、`net.badResponse`、`err.not_logged_in`、`empty.running.p`、`empty.p`、`auth.h`、`auth.p`、`del.p`、`set.timeHint`、`set.dupHint`、`dup.checking.p`、`dup.intro`。
  - 新键（中英文都要）：
    - `login.h`：标题
    - `login.password`：密码框标签
    - `login.submit`：按钮
    - `login.insecure`：逐字使用 Global Constraints 里的文案
    - `err.bad_password`
    - `err.too_many_attempts`：带 `{min}` 参数
    - `tb.logout`
    - `vol.fsDev`：`{fstype} · {device}`
  - `window.I18N` 增加 `platform` 字段。
- [ ] **Step 3:** `app.js`：
  - `ApiError` 增加 `data` 字段，保存原始响应。`ERROR_CODES` 加入 `bad_password`、`too_many_attempts`。
  - `showError` 收到 401 时：平台是 `'linux'` 就调用 `showLogin()`，否则保持原来的行为。
  - `showLogin()`：在 `showState` 使用的主内容区渲染一个表单，包含 `<input type="password" autocomplete="current-password" autofocus>` 和提交按钮。登录前隐藏工具栏上的操作按钮。
    - 提交时调用 `api('login', {}, {method:'POST', body:{password}})`。成功后执行 `location.reload()`。
    - `bad_password` → 在表单内显示错误信息。
    - `too_many_attempts` → 显示 `err.too_many_attempts`，其中 `min = Math.ceil(e.data.retryAfter / 60)`。
    - 当 `location.protocol === 'http:'` 且主机名不是 `localhost`、`127.0.0.1`、`[::1]` 时，在表单下方显示 `login.insecure`。
  - `#btnLogout`：仅在 Linux 平台显示。点击后 POST `logout`，然后 `location.reload()`。
  - 卷列表的小字：`v.fsType` 有值时显示 `t('vol.fsDev', {fstype: v.fsType, device: v.device})`，否则保持现在的存储池或挂载点逻辑。
- [ ] **Step 4:** 检查 i18n 键是否齐全。下面的命令应输出 `ok`：
  ```bash
  node -e "global.document={cookie:'',documentElement:{dataset:{}}};global.location={search:''};global.localStorage={getItem:()=>null};global.navigator={languages:['en']};global.window={};require('./web/i18n.js');const d=window.I18N._dicts;const a=Object.keys(d.zh).sort().join(),b=Object.keys(d.en).sort().join();if(a!==b)throw new Error('zh/en keys differ');for(const k of Object.keys(d.zhLinux))if(!(k in d.enLinux))throw new Error('missing enLinux '+k);console.log('ok')"
  ```
  为此 `_dicts` 需要导出 `zhLinux` 和 `enLinux`。
- [ ] **Step 5:** 浏览器验证。用 `preview_start {name: "storage-analyzer-dev"}`：
  - (a) `read_console_messages` 里没有 `Refused to` 开头的 CSP 报错。
  - (b) 新建一次扫描，卷列表显示挂载点和 fsType，下钻、查重和"↗ 定位"都正常，路径里没有 `//`。
  - (c) 另起一个不带 `--insecure-no-auth` 的配置，先跑 `passwd`，然后在页面上检查：输错密码有提示，输对后进入应用；退出后回到登录页；输错 5 次后显示"N 分钟后再试"。
  - 截图作为证明。
- [ ] **Step 6:** QNAP 页面不变的检查：直接用 `web/index.html`（没有 `data-platform`）在 node 里加载 `i18n.js`，断言 `I18N.platform === 'qnap'` 且 `I18N.t('auth.h') === 'Please sign in to QTS'`。

### Task 15: 版本号与 Linux 打包

**Files:**
- Create: `VERSION`（内容为当前版本 `0.5.1`，发版时由维护者修改）
- Modify: `packaging/qpkg/qpkg.cfg`（`QPKG_VER="@VERSION@"`）、`scripts/build.sh`（读 `VERSION`，写进暂存目录里的 qpkg.cfg，并通过 `-ldflags "-s -w -X main.version=$VER"` 注入）
- Create: `packaging/linux/storage-analyzer.service`（内容逐字照抄 spec §5.3，`ExecStart=/usr/bin/storage-analyzer serve --config /etc/storage-analyzer/config`，另加 `[Unit] Description=Storage Analyzer` / `After=network.target` 和 `[Install] WantedBy=multi-user.target`）
- Create: `packaging/linux/config`（spec §7.2 的示例，所有键默认注释掉，只有 `listen = :8099` 生效）
- Create: `packaging/linux/install.sh`、`packaging/linux/uninstall.sh`
- Create: `packaging/linux/nfpm.yaml`、`packaging/linux/scripts/{postinstall,preremove,postremove}.sh`
- Create: `scripts/build-linux.sh`

**Interfaces:**
- Produces: 运行 `scripts/build-linux.sh` 后，`build/linux/` 下有：
  - `storage-analyzer_<ver>_linux_{amd64,arm64,armv7}.tar.gz`，解压后为一个同名目录，内含 `storage-analyzer`、`storage-analyzer.service`、`config`、`install.sh`、`uninstall.sh`、`README.md`、`LICENSE`
  - `storage-analyzer_<ver>_{amd64,arm64,armhf}.deb`
  - `storage-analyzer-<ver>-1.{x86_64,aarch64,armv7hl}.rpm`
  - 交叉编译不带 tag。nfpm 的 arch 依次取 `amd64`、`arm64`、`arm7`。

各脚本的行为（POSIX sh，`set -eu`）：
- `postinstall.sh`：
  - 若组或用户 `storage-analyzer` 不存在，就创建系统用户，shell 为 nologin，home 为 `/var/lib/storage-analyzer`，不建家目录。
  - 若 `/etc/storage-analyzer/password` 已存在，`chgrp storage-analyzer` 并 `chmod 0640`。
  - 执行 `systemctl daemon-reload`。
  - 判断首次安装还是升级：deb 是 `$1 = configure` 且 `$2` 为空，rpm 是 `$1 = 1`。
    - 首次安装：只打印两行，`sudo storage-analyzer passwd` 和 `sudo systemctl enable --now storage-analyzer`，不启动服务。
    - 升级：如果服务正在运行（`systemctl is-active --quiet`），执行 `systemctl try-restart storage-analyzer`。
- `preremove.sh`：仅在真正卸载时（deb 的 `$1 = remove`，rpm 的 `$1 = 0`）执行 `systemctl disable --now storage-analyzer`。
- `postremove.sh`：deb 的 `$1 = purge` 时删除 `/etc/storage-analyzer` 和 `/var/lib/storage-analyzer`，并执行 `daemon-reload`。rpm 不删除数据。
- `install.sh`（tar.gz 用）：
  - 必须以 root 运行。
  - 把二进制装到 `/usr/local/bin`。
  - 把 unit 装到 `/etc/systemd/system`，装的时候把 ExecStart 里的 `/usr/bin/` 换成 `/usr/local/bin/`。
  - `/etc/storage-analyzer/config` 不存在时才拷贝。
  - 其余行为与 postinstall 相同（再次运行即为升级）。
- `uninstall.sh`：`disable --now`，删除二进制和 unit，执行 `daemon-reload`。加 `--purge` 时再删除 `/etc/storage-analyzer` 和 `/var/lib/storage-analyzer`。
- `nfpm.yaml`：
  - `name: storage-analyzer`，`version: ${VERSION}`，`arch: ${ARCH}`，`license: MIT`，`homepage: https://github.com/FlintyLemming/storage-analyzer`。
  - contents：二进制 → `/usr/bin/storage-analyzer`（0755）；unit → `/usr/lib/systemd/system/storage-analyzer.service`（0644）；config → `/etc/storage-analyzer/config`，`type: config|noreplace`（0644）。
  - scripts 指向上面三个脚本。
  - `build-linux.sh` 用 `go run github.com/goreleaser/nfpm/v2/cmd/nfpm@<固定版本> package` 调用 nfpm。版本固定为实施时最新的 v2.x，写进脚本。

- [ ] **Step 1:** `scripts/build.sh` → 3 个 qpkg 的文件名带 `0.5.1`；在暂存目录的 `qpkg.cfg` 里 `QPKG_VER="0.5.1"`。
- [ ] **Step 2:** `scripts/build-linux.sh` → 上面列出的 9 个文件都存在。
- [ ] **Step 3:** 检查包内容：
  - `tar tzf` 能看到 7 个文件。
  - `docker run --rm -v $PWD/build/linux:/p debian:stable dpkg-deb -c /p/storage-analyzer_0.5.1_amd64.deb` 能看到三个安装路径。
  - `docker run --rm -v $PWD/build/linux:/p fedora:latest rpm -qlp /p/storage-analyzer-0.5.1-1.x86_64.rpm` 同样能看到三个路径。
- [ ] **Step 4:** `sh -n` 检查全部 5 个 shell 脚本。如果装了 `shellcheck`，再跑一遍，应无报错。
- [ ] **Step 5:** `build/linux` 里解出 amd64 的二进制，在 Linux 容器里运行 `storage-analyzer version` → 输出 `0.5.1`。

### Task 16: CI

**Files:**
- Create: `.github/workflows/ci.yml`、`.github/workflows/release.yml`

**Interfaces:**
- Consumes: `scripts/build.sh`、`scripts/build-linux.sh`、`scripts/test-linux.sh`（支持 `TAGS`）、`VERSION`

- [ ] **Step 1:** `ci.yml`，在 push 和 pull_request 时触发，运行在 `ubuntu-latest`：
  1. `actions/checkout`。
  2. `actions/setup-go`，`go-version-file: go.mod`。
  3. `go vet ./...` 和 `go vet -tags qnap ./...`。
  4. `go test ./...` 和 `go test -tags qnap ./...`。
  5. `docker/setup-qemu-action`。
  6. `./scripts/test-linux.sh` 和 `TAGS=qnap ./scripts/test-linux.sh`。
- [ ] **Step 2:** `release.yml`，在 push 名为 `v*` 的 tag 时触发，`permissions: contents: write`：
  1. checkout，配置 Go 和 QEMU。
  2. 检查 `"$GITHUB_REF_NAME" = "v$(cat VERSION)"`，不一致就失败。
  3. 依次运行 `./scripts/build.sh` 和 `./scripts/build-linux.sh`。
  4. 在 `build/` 下对全部 qpkg 和 `linux/*` 计算 `sha256sum`，写入 `SHA256SUMS`。
  5. `gh release create "$GITHUB_REF_NAME" --draft --generate-notes` 上传全部产物和 `SHA256SUMS`，`GH_TOKEN` 用 `${{ github.token }}`。
  - 发布成**草稿**，由维护者检查说明后再手动发布。
- [ ] **Step 3:** `go run github.com/rhysd/actionlint/cmd/actionlint@latest` → 无报错。真正的运行要等推送之后，推不推由用户决定。

### Task 17: 文档、LICENSE、仓库改名

**Files:**
- Modify: `README.md`、`README.zh-CN.md`
- Create: `LICENSE`（MIT，`Copyright (c) 2026 FlintyLemming`，与 `qpkg.cfg` 里 `QPKG_LICENSE="MIT"` 一致）

- [ ] **Step 1:** 两份 README 同步修改：
  - 标题改为 `Storage Analyzer`，开头改成通用介绍，写明支持 QNAP QTS 和普通 Linux 服务器。
  - "功能"一节去掉 QTS 专属的说法，把"平时没有后台进程"改为 QNAP 版的特点；Linux 版写成"常驻服务，空闲时不占 CPU"。
  - "安装"分 QNAP 和 Linux 两节。QNAP 节沿用现有内容，链接改为新仓库地址。Linux 节依次写：
    1. 安装 deb、rpm 或 tar.gz（`install.sh`）。
    2. `sudo storage-analyzer passwd`。
    3. `sudo systemctl enable --now storage-analyzer`。
    4. 访问 `http://服务器:8099`。
    5. 配置项表格，与 spec §7.2 对应。
    6. HTTPS：要么配证书，要么放在反向代理后面。附 Caddy 和 nginx 示例，并写明放在代理后面时要把代理地址填进 `trusted_proxies`，否则所有人共用一个限速额度。
    7. 改端口到 1024 以下时，需要在 override 里加 `AmbientCapabilities=CAP_DAC_READ_SEARCH CAP_NET_BIND_SERVICE`。
    8. 改 `data_dir` 时，需要在 override 里加 `ReadWritePaths`。
    9. 卸载方法。
  - 已知限制：没有单独挂载的 btrfs 子卷会被跳过；首个版本不提供 Docker 镜像。
- [ ] **Step 2:** 仓库改名是对外的操作。**先问用户**，得到同意后执行 `gh repo rename storage-analyzer --yes`，然后 `git remote set-url origin https://github.com/FlintyLemming/storage-analyzer.git`，再用 `git remote -v` 确认。
- [ ] **Step 3:** `grep -rn "storage-analyzer-qnap" --exclude-dir=.git .` → 只剩 docs 里的历史 spec 和计划。

### Task 18: 真机验证

- [ ] **Step 1:** 问用户用哪台带 systemd 的 Linux 机器，至少要一台 Debian 或 Ubuntu。可以是用户的服务器，也可以是本机的 Lima、OrbStack 或 multipass 虚拟机。如果方便，再加一台 Fedora 测 rpm。
- [ ] **Step 2:** 在 Debian/Ubuntu 上依次检查：
  1. 安装 deb，`systemctl status storage-analyzer` 为 inactive，postinstall 打印了两条提示。
  2. 不设密码直接 `systemctl start`，unit 进入 failed 状态但没有反复重启，`journalctl -u storage-analyzer` 里能看到 `storage-analyzer passwd`。
  3. `passwd` 后 `enable --now`，从另一台机器的浏览器登录。
  4. 输错 5 次后返回 429，日志里有 5 行 `login failed from`。
  5. 完成一次扫描：`ps -o user,cmd` 显示扫描子进程以 `storage-analyzer` 用户运行。在 `/root` 等普通用户不可读的目录里有文件被计入，说明能读；而且 `/proc/<pid>/status` 的 `CapEff` 只含 `CAP_DAC_READ_SEARCH`。
  6. 对比沙盒内外的结果：用 root 和一个单独的 `data_dir` 跑 `sudo storage-analyzer scan --config /tmp/root.conf`，两次扫描的文件数和总占用都相同。
  7. 查重能完成。
  8. 把定时设到下一分钟，到时间会自动扫描。
  9. 扫描进行中执行 `systemctl restart`，这次扫描记录为"已取消"，重启后可以新建扫描。
  10. 用 VERSION 加 1 的包升级：服务被重启，原来的会话仍然有效。
  11. `apt purge` 后，`/etc/storage-analyzer` 和 `/var/lib/storage-analyzer` 都不存在。
- [ ] **Step 3:** Fedora 上（如果有）：装 rpm、设密码、启动、扫描，冒烟测试即可。
- [ ] **Step 4:** tar.gz 的 `install.sh`、`uninstall.sh --purge` 在任意一台机器上冒烟测试一次。
- [ ] **Step 5:** QNAP 回归：`scripts/build.sh` 后 `scripts/deploy.sh`，在 hf-qts 上覆盖安装。从 QTS 桌面打开，历史扫描和设置都还在。新建扫描、查重、定时设置都正常。界面文案仍然提 QTS，没有登录页和退出按钮。
- [ ] **Step 6:** 向用户汇报结果和截图。提交和推送等用户决定。
