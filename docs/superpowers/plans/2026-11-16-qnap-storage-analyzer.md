# QNAP 存储分析器 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 一个 QTS 桌面小窗口应用（QPKG），以 root 扫描所有用户存储池上的卷，每次扫描存为一个 SQLite 文件，可以 ncdu 式下钻到任意文件，支持手动扫描、定时扫描与历史管理。

**Architecture:** 单个静态 Go 二进制 `storage-analyzer`，子命令 `cgi` / `scan` / `apply-schedule` / `prune` / `init-start` / `init-stop` / `serve`（仅开发用）。无常驻进程：浏览请求走 QTS thttpd 的 CGI，扫描由 CGI 以 `setsid` 拉起或由 crontab 触发。前端为原生 HTML/JS，作为静态文件与 `api.cgi` 放在同一目录，通过 `/cgi-bin/qpkg/StorageAnalyzer/` 访问。

**Tech Stack:** Go 1.26（`CGO_ENABLED=0`，`GOOS=linux GOARCH=amd64`），`modernc.org/sqlite`，原生 JS/CSS，QDK（Docker 内运行 `qbuild`）。

**Spec:** `docs/superpowers/specs/2026-11-16-qnap-storage-analyzer-design.md`

## Global Constraints

- QPKG 名 `StorageAnalyzer`，显示名 `存储分析器`，版本 `0.1.0`，仅 x86_64。
- 窗口：`Desktop = 1`，`Win_Width = 1100`，`Win_Height = 680`，WebUI `/cgi-bin/qpkg/StorageAnalyzer/`，`Web_Port = -1`。
- 不依赖外部 CDN；界面只做中文。
- 工具只读：任何代码路径都不得修改、删除扫描到的用户文件（只会删除自己 `data/` 下的文件）。
- 只有 QTS 管理员可访问 API：`NAS_SID` cookie → `http://127.0.0.1:58080/cgi-bin/authLogin.cgi?sid=<sid>`，要求 `authPassed=1` 且 `isAdmin=1`。
- 修改类请求只接受 POST 且必须带请求头 `X-SA-Request: 1`。
- 扫描进程：nice 19 + ioprio IDLE；同一时间仅一个扫描（`flock data/run/scan.lock`）。
- 保留最近 10 次成功扫描（可设 1–100）；定时默认关闭；定时选项 off / daily / weekly。
- 单份扫描体积目标：平均每条目 ≤ 70 字节；含 1 万子项的目录在设备上列出 < 300ms。
- 扫描峰值内存目标：300 万条目 ≤ 300MB。
- 数据目录：`${QPKG_ROOT}/data/{catalog.db, scans/, run/, logs/}`。
- crontab 行以 ` # StorageAnalyzer` 结尾；应用后执行 `crontab /etc/config/crontab && /etc/init.d/crond.sh restart`。
- 项目目前不是 git 仓库，计划中不含 commit 步骤（是否提交由用户决定）。

## Review Focus

1. **超大目录**（单目录 20 万个文件）：扫描不爆内存，`list` 分页返回且每页按所选列正确排序 → Task 2 `TestListLargeDirPaging`，Task 3 `TestWalkWideDir`。
2. **奇怪的文件名**（非法 UTF-8、换行、引号、`%`）：JSON 合法、面包屑正确、标注 `badName` → Task 2 `TestNamesRoundTrip`，Task 7 `TestListJSONBadName`。
3. **扫描被强杀或重启打断**：catalog 中残留的 `running` 在 `init-start` 时标为 `failed`，`.tmp` 文件被清理，锁不卡死 → Task 4 `TestMarkStaleRunning`，Task 5 `TestCleanupStaleTmp`。
4. **定时扫描撞上正在运行的手动扫描**：定时这次直接跳过（退出码 0，写日志），`scan_start` 返回 409 → Task 5 `TestSecondScanSkipped`，Task 7 `TestScanStartConflict`。
5. **正在查看的扫描被删除 / 被保留策略清理**：`list` 返回 404 `{"error":"scan_not_found"}`，界面回到最新结果 → Task 7 `TestListDeletedScan`，Task 8 手动验证。

---

## 文件结构

```
go.mod                                   module storage-analyzer
cmd/storage-analyzer/main.go             子命令分发、路径解析
internal/volumes/volumes.go              解析 volume.conf / /proc/mounts / qcli_storage → 卷列表
internal/scandb/schema.go                每次扫描的 SQLite：schema、常量
internal/scandb/writer.go                批量写入
internal/scandb/reader.go                列目录（排序/分页）、面包屑、meta
internal/scanner/walk.go                 并行遍历 → 写入 Writer
internal/catalog/catalog.go              catalog.db：扫描记录、设置、保留策略
internal/runner/runner.go                一次完整扫描的编排：锁、进度文件、改名、清理
internal/runner/prio_linux.go            nice + ioprio（linux）
internal/runner/prio_other.go            空实现（darwin，用于本地测试）
internal/schedule/cron.go                crontab 行生成与改写
internal/api/api.go                      http.Handler：路由与各 action
internal/api/auth.go                     QTS 会话校验
web/index.html  web/app.js  web/style.css
packaging/qpkg/qpkg.cfg
packaging/qpkg/package_routines
packaging/qpkg/shared/StorageAnalyzer.sh
packaging/qpkg/shared/web/api.cgi        shell 包装：exec 二进制 cgi
packaging/Dockerfile                     QDK 构建镜像
tools/genicons/main.go                   生成 3 个 gif 图标
scripts/build.sh                         交叉编译 + 组装 + qbuild → build/*.qpkg
scripts/deploy.sh                        scp 到 hf-qts 并 sudo 安装
```

---

### Task 1: 项目骨架 + 卷发现

**Files:**
- Create: `go.mod`, `internal/volumes/volumes.go`, `internal/volumes/volumes_test.go`, `internal/volumes/testdata/{volume.conf,mounts,qcli.txt}`（内容取自 hf-qts 真实输出，另加一个"未挂载的 VOL_2"段）

**Interfaces:**
- Produces:
  ```go
  type Volume struct {
      Name       string // volName, e.g. "DataVol1"
      Pool       string // "2"；未知为 ""
      Device     string // "/dev/mapper/cachedev2"
      MountPoint string // "/share/CACHEDEV2_DATA"
  }
  type Skipped struct { Name, Reason string } // Reason: "not_mounted" | "not_share_volume"
  func ParseVolumeConf(r io.Reader) ([]VolConf, error)          // VolConf{Name, MappingName string}
  func ParseMounts(r io.Reader) map[string]string               // device → mountpoint（首个）
  func ParseQcliPools(out string) map[string]string             // VolName → Pool
  func Resolve(vc []VolConf, mounts, pools map[string]string) ([]Volume, []Skipped)
  func Discover() ([]Volume, []Skipped, error)                  // 读真实文件 + 执行 /sbin/qcli_storage（失败则 Pool=""）
  ```

- [ ] **Step 1: 写失败测试** `TestResolveFromDeviceFixtures`：用 testdata 得到 `[]Volume{{"DataVol1","2","/dev/mapper/cachedev2","/share/CACHEDEV2_DATA"}}`，`Skipped` 含 `{"DataVol2","not_mounted"}`。`TestResolveRejectsNonShareMount`：mappingName 挂在 `/mnt/pool2` → `not_share_volume`。`TestParseQcliPools`：fixture 中 4 行都映射 `DataVol1→"2"`。
- [ ] **Step 2:** `go test ./internal/volumes/` → FAIL（未定义）。
- [ ] **Step 3:** 实现上述函数。挂载点必须匹配 `^/share/CACHEDEV\d+_DATA$`。
- [ ] **Step 4:** `go test ./internal/volumes/` → PASS。

---

### Task 2: 扫描结果数据库（scandb）

**Files:**
- Create: `internal/scandb/{schema.go,writer.go,reader.go,scandb_test.go}`

**Interfaces:**
- Produces:
  ```go
  const (KindFile=0; KindDir=1; KindSymlink=2; KindOther=3; KindMount=4)
  const (FlagError=1; FlagHardlinkDup=2; FlagBadName=4)
  type Entry struct {
      ID, Parent int64; Name []byte; Kind, Flags int
      DSize, ASize, Items, MTime int64
      FirstChild, NChild int64   // 子项 id 连续：[FirstChild, FirstChild+NChild)
  }
  type Writer struct{ ... }
  func Create(path string) (*Writer, error)       // 建表，PRAGMA journal_mode=OFF, synchronous=OFF
  func (w *Writer) Add(e Entry) error             // 缓冲，满 5000 条一次 INSERT 批
  func (w *Writer) SetMeta(key, value string) error
  func (w *Writer) Close() error                   // 提交 + 关闭
  func (w *Writer) Abort()                         // 关闭并删除文件

  type DB struct{ ... }
  func Open(path string) (*DB, error)              // 只读
  type ListOpts struct{ Sort string /*size|name|items|mtime*/; Desc bool; SizeMode string /*disk|apparent*/; Offset, Limit int }
  func (d *DB) Get(id int64) (Entry, error)        // id=0 → 合成的"全部卷"根
  func (d *DB) Children(id int64, o ListOpts) ([]Entry, int /*total*/, error)
  func (d *DB) Path(id int64) ([]Entry, error)     // 从卷根到 id
  func (d *DB) Meta() (map[string]string, error)
  ```
- schema：spec §7.1 的 `entries` 加两列 `first_child INTEGER, nchild INTEGER`，**不建 parent 索引**（子项按 rowid 区间取），另有 `meta(key TEXT PRIMARY KEY, value TEXT)`。卷根的 `parent=0`，id 从 1 起连续。
- `size` 排序在 `SizeMode=apparent` 时按 `asize`。名称排序按字节序。

- [ ] **Step 1: 写失败测试**
  - `TestWriteAndListChildren`：写入根(1)→子 2..4（大小 10/30/20），`Children(1, size desc)` 依次为 3,4,2；`total=3`。
  - `TestListLargeDirPaging`：一个目录 200000 个子项，`Offset=199990, Limit=500` 返回 10 条且仍按大小降序；整个查询 < 2s（本地）。
  - `TestPathBreadcrumb`：`Path(深层 id)` 返回从卷根开始的有序链。
  - `TestNamesRoundTrip`：名字 `[]byte{0xff,'a'}`、`"a\nb"`、`"50%\"x"` 写入后原样读出。
  - `TestRootListsVolumes`：`Children(0, ...)` 返回所有 `parent=0` 的卷根。
  - `TestBytesPerEntry`：写 100000 条真实感数据（名字 15–30 字节，大小随机），文件大小 / 条目数 ≤ 70。
- [ ] **Step 2:** `go test ./internal/scandb/` → FAIL。
- [ ] **Step 3:** 实现。Writer 用预编译的多值 INSERT，单一事务。
- [ ] **Step 4:** `go test ./internal/scandb/` → PASS。

---

### Task 3: 并行遍历器（scanner）

**Files:**
- Create: `internal/scanner/walk.go`, `internal/scanner/walk_test.go`

**Interfaces:**
- Consumes: `scandb.Entry`、`scandb.Kind*`、`scandb.Flag*`
- Produces:
  ```go
  type Sink interface{ Add(scandb.Entry) error }   // *scandb.Writer 满足；写入方保证单线程调用
  type Root struct{ Name string; Path string }      // Name 作为卷根条目的名字
  type Stats struct{ Files, Dirs, Errors, Bytes int64; Current atomic.Value /*string*/ }
  type Walker struct {
      Workers      int                                        // 默认 4
      IsMountPoint func(path string, rootDev, dev uint64) bool // 默认 dev != rootDev；测试可替换
      Stats        Stats
  }
  func (w *Walker) Walk(ctx context.Context, roots []Root, sink Sink) error
  ```
- 规则（spec §6.2）：`lstat` 不跟随；`nlink>1` 的非目录按 `(dev,ino)` 去重，重复者 `FlagHardlinkDup` 且不计入父目录合计；挂载点记为 `KindMount`、大小 0、不进入；readdir 失败给目录加 `FlagError`；lstat 失败（文件消失）给条目加 `FlagError` 并计入 `Errors`；名字非法 UTF-8 加 `FlagBadName`。
- 算法：读完一个目录时一次性为其全部子项分配**连续** id（原子计数器 `Add(n)`），文件类子项立即交给写入 goroutine；子目录入工作栈（LIFO，控制前沿大小）。每个目录节点在内存中保留 `pending` 计数，子树全部完成时把 `dsize/asize/items` 加到父节点并写出自身行，然后递归检查父节点。只有写入 goroutine 调用 `sink.Add`。ctx 取消时尽快返回 `ctx.Err()`。

- [ ] **Step 1: 写失败测试**（在 `t.TempDir()` 中建树，用内存 Sink 收集条目）
  - `TestWalkTotals`：`a/x(100B) a/b/y(5000B) z(1B)`：根 `items=5`，`a` 的 `asize=5100`，根 `asize=5101`；每个目录的子项 id 构成 `[FirstChild, FirstChild+NChild)` 的连续区间且与 parent 一致。
  - `TestWalkHardlinkCountedOnce`：`f` 4096B 与硬链接 `g`：两个条目都存在，恰好一个带 `FlagHardlinkDup`，根 `dsize` 只计一次。
  - `TestWalkSymlinkNotFollowed`：指向大目录的符号链接为 `KindSymlink`，不进入。
  - `TestWalkMountBoundary`：`IsMountPoint` 把 `m` 视为挂载点 → `KindMount`、无子项。
  - `TestWalkVanishedAndUnreadable`：`chmod 000` 的目录（非 root 运行时）→ `FlagError`，`Stats.Errors>=1`，遍历仍完成。
  - `TestWalkBadName`：文件名 `"\xff.bin"` → `FlagBadName`（macOS APFS 拒绝时 `t.Skip`）。
  - `TestWalkWideDir`：单目录 20000 个文件，全部写出，`NChild=20000`。
  - `TestWalkCancel`：已取消的 ctx → 返回 `context.Canceled`。
- [ ] **Step 2:** `go test ./internal/scanner/` → FAIL。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** `go test -race ./internal/scanner/` → PASS。

---

### Task 4: catalog（扫描记录、设置、保留）

**Files:**
- Create: `internal/catalog/catalog.go`, `internal/catalog/catalog_test.go`

**Interfaces:**
- Produces:
  ```go
  type Scan struct {
      ID int64; StartedAt, FinishedAt int64 /*unix*/; Trigger string /*manual|schedule*/
      Status string /*running|done|failed|canceled*/
      TotalDSize, TotalASize, Files, Dirs, Errors, DurationS, DBBytes int64; ErrorMessage string
  }
  type Settings struct {
      ScheduleMode string /*off|daily|weekly*/; Weekday int /*0=周日*/; Time string /*"HH:MM"*/
      Retention int; DefaultSizeMode string /*disk|apparent*/
  }
  func Open(dataDir string) (*Catalog, error)               // dataDir/catalog.db，WAL，busy_timeout 5000
  func (c *Catalog) Begin(trigger string, now time.Time) (int64, error)
  func (c *Catalog) Finish(s Scan) error                      // 写 done 结果
  func (c *Catalog) Fail(id int64, status, msg string, now time.Time) error
  func (c *Catalog) List() ([]Scan, error)                    // 新→旧
  func (c *Catalog) Get(id int64) (Scan, error)               // 不存在 → ErrNotFound
  func (c *Catalog) Delete(id int64) error                    // 删行 + dataDir/scans/scan-<id>.db
  func (c *Catalog) Prune(keep int) ([]int64, error)          // 只针对 done，删最旧的
  func (c *Catalog) MarkStaleRunning(now time.Time) (int, error) // running → failed，msg "interrupted"
  func (c *Catalog) Settings() (Settings, error)              // 缺省：off, 0, "03:00", 10, "disk"
  func (c *Catalog) SaveSettings(s Settings) error            // 校验：Retention 1–100，Time 合法，Weekday 0–6
  func ScanPath(dataDir string, id int64) string              // dataDir/scans/scan-<id>.db
  ```

- [ ] **Step 1: 写失败测试** `TestBeginFinishList`、`TestPruneKeepsNewestDone`（12 个 done + 2 个 failed，keep=10 → 删 2 个最旧 done 的行和文件，failed 保留）、`TestDeleteRemovesFile`、`TestMarkStaleRunning`、`TestSettingsDefaultsAndValidation`（Retention 0 / 101、Time "25:00"、Weekday 7 均报错）。
- [ ] **Step 2:** FAIL → **Step 3:** 实现 → **Step 4:** `go test ./internal/catalog/` PASS。

---

### Task 5: runner（一次完整扫描的编排）

**Files:**
- Create: `internal/runner/runner.go`, `internal/runner/prio_linux.go`, `internal/runner/prio_other.go`, `internal/runner/runner_test.go`

**Interfaces:**
- Consumes: `volumes.Volume`/`Skipped`、`scanner.Walker`、`scandb.Create`、`catalog.*`
- Produces:
  ```go
  type Progress struct {
      ScanID int64 `json:"scanId"`; PID int `json:"pid"`; Trigger string `json:"trigger"`
      StartedAt int64 `json:"startedAt"`; Files, Dirs, Bytes, Errors int64
      Current string `json:"current"`; PrevFiles int64 `json:"prevFiles"` // 上次成功扫描的文件数，0=未知
  }
  type Options struct {
      DataDir string; Trigger string
      Discover func() ([]volumes.Volume, []volumes.Skipped, error) // 测试注入
      Now func() time.Time
  }
  var ErrBusy = errors.New("scan already running")
  func Run(ctx context.Context, o Options) (int64, error)        // 已有扫描 → ErrBusy
  func Running(dataDir string) (Progress, bool, error)            // 试锁判断；运行中则读 progress.json
  func Cancel(dataDir string) error                               // 读锁文件 PID → SIGTERM
  func CleanupStaleTmp(dataDir string) error                      // 删除 scans/*.tmp（仅在未持锁时）
  func SetLowPriority()                                           // linux: setpriority 19 + ioprio_set(IOPRIO_CLASS_IDLE)
  ```
- 流程：`flock(LOCK_EX|LOCK_NB)` 失败 → `ErrBusy`；写 PID 到锁文件；`catalog.Begin`；写 `scan-<id>.db.tmp`；每秒原子写 `run/progress.json`（tmp + rename）；`Walk` 结束后写 meta（`volumes` 为 JSON：每卷 `{entryId,name,pool,mount,fsTotal,fsUsed,fsFree}` 来自 `statfs`；`skipped`；`startedAt`；`finishedAt`；`trigger`）；`Close` → rename 为 `.db`；`catalog.Finish`；`Prune(settings.Retention)`；删 progress.json。ctx 取消 → `Writer.Abort` + `Fail(id,"canceled")`；其他错误 → `Fail(id,"failed",err)`。
- 卷根条目名 = 卷名（如 `DataVol1`）。

- [ ] **Step 1: 写失败测试**（`Discover` 返回指向 `t.TempDir()` 中两个目录的假卷）
  - `TestRunProducesDoneScan`：catalog 中 1 条 `done`，`scan-1.db` 存在，`Children(0)` 有 2 个卷根，meta `volumes` 可解析；progress.json 已删除。
  - `TestSecondScanSkipped`：测试内先持有锁 → `Run` 返回 `ErrBusy` 且 catalog 无新行。
  - `TestRunCanceled`：ctx 取消 → 状态 `canceled`，无 `.db` 与 `.tmp`。
  - `TestCleanupStaleTmp`：放一个 `scan-9.db.tmp` → 被删除。
  - `TestRunPrunes`：Retention=2，连续 3 次 → 只剩 2 个 `.db`。
- [ ] **Step 2:** FAIL → **Step 3:** 实现 → **Step 4:** `go test ./internal/runner/` PASS。

---

### Task 6: 定时（crontab）

**Files:**
- Create: `internal/schedule/cron.go`, `internal/schedule/cron_test.go`

**Interfaces:**
- Consumes: `catalog.Settings`
- Produces:
  ```go
  const Tag = "# StorageAnalyzer"
  func Line(s catalog.Settings, binPath string) (string, bool)  // 关闭 → ("", false)
  func Rewrite(crontab, line string) string                     // 去掉所有含 Tag 的行；line 非空则追加；保留其余行与末尾换行
  func Apply(s catalog.Settings, binPath, crontabPath string, run func(name string, args ...string) error) error
  ```
- 行格式：`MM HH * * DOW <binPath> scan --trigger schedule >/dev/null 2>&1 # StorageAnalyzer`（daily 时 DOW 为 `*`）。`Apply` 写回文件后调用 `run("crontab", crontabPath)` 和 `run("/etc/init.d/crond.sh", "restart")`。

- [ ] **Step 1: 写失败测试** `TestLineDaily`（`"30 3 * * * /x scan --trigger schedule >/dev/null 2>&1 # StorageAnalyzer"`）、`TestLineWeekly`（Weekday=0 → DOW `0`）、`TestLineOff`、`TestRewriteReplacesOnlyTagged`（用 hf-qts 真实 crontab 片段，其他 46 行原样保留）、`TestApplyRunsCrontabAndRestart`（假 run 记录调用顺序）。
- [ ] **Step 2:** FAIL → **Step 3:** 实现 → **Step 4:** `go test ./internal/schedule/` PASS。

---

### Task 7: API（CGI handler + 鉴权）与主程序

**Files:**
- Create: `internal/api/api.go`, `internal/api/auth.go`, `internal/api/api_test.go`, `cmd/storage-analyzer/main.go`

**Interfaces:**
- Consumes: `catalog.*`、`scandb.Open/Children/Path/Get/Meta`、`runner.Running/Cancel`、`schedule.Apply`
- Produces:
  ```go
  type Authenticator interface{ Check(r *http.Request) (user string, admin bool, err error) }
  type QTSAuth struct{ URL string }   // 默认 "http://127.0.0.1:58080/cgi-bin/authLogin.cgi"；sid 取 cookie NAS_SID，否则 query sid
  type Config struct {
      DataDir, BinPath, CrontabPath string
      Auth   Authenticator
      Spawn  func(args ...string) error          // 默认：exec BinPath，Setsid，stdio=nil，Start 后 Release
      RunCmd func(name string, args ...string) error
  }
  func New(cfg Config) http.Handler
  ```
- 路由 `?action=`（全部返回 JSON，错误为 `{"error":"<code>","message":"..."}`）：
  - `status` GET → `{"running":bool,"progress":Progress|null}`
  - `scans` GET → `{"scans":[Scan...]}`
  - `scan_start` POST → 运行中 409 `scan_running`；否则 `Spawn("scan","--trigger","manual")` → 202 `{"started":true}`
  - `scan_cancel` POST → 204/404 `not_running`
  - `scan_delete` POST `scan=<id>` → 正在写入的扫描 409；否则删除
  - `list` GET `scan,dir,sort,order,size,offset,limit(≤1000，默认 500)` → `{"scan":Scan,"dir":Item,"path":[Item],"items":[Item],"total":n,"volumes":[...]/*仅 dir=0*/}`；`Item = {id,name,badName,kind,dsize,asize,items,mtime,flags,hasChildren}`，名字用 `strings.ToValidUTF8(name,"�")`
  - `settings` GET/POST（POST 为 JSON body）→ 保存后 `schedule.Apply`
- 鉴权失败 401 `not_logged_in`，非管理员 403 `not_admin`；POST 缺 `X-SA-Request: 1` → 403 `bad_request_origin`；未知 action 400。
- main：`os.Executable()` + `filepath.EvalSymlinks` → `root = dir(dir(exe))`，`--root` 可覆盖；子命令：
  - `cgi` → `cgi.Serve(api.New(...))`
  - `scan --trigger X` → `runner.SetLowPriority()`；SIGTERM/SIGINT 取消 ctx；`ErrBusy` 打日志后退出码 0
  - `apply-schedule`、`prune`
  - `init-start` → `MarkStaleRunning` + `CleanupStaleTmp` + `apply-schedule`
  - `init-stop` → `Cancel` + 删除 crontab 行
  - `serve --addr 127.0.0.1:8099 --insecure-no-auth` → 仅开发：`http.FileServer(web/)` + 同一 handler（Auth 恒通过）；拒绝非 127.0.0.1 地址

- [ ] **Step 1: 写失败测试**（`httptest`，假 Authenticator，Task 5 的 `Run` 生成真实扫描数据）
  - `TestAuthRequired`（401）、`TestNonAdminForbidden`（403）、`TestPostNeedsHeader`（403）
  - `TestQTSAuthParsesXML`：假 authLogin 服务返回 `<authPassed><![CDATA[1]]></authPassed><isAdmin><![CDATA[1]]></isAdmin><username><![CDATA[admin]]></username>` → admin；返回 `authPassed 0` → 未登录。
  - `TestListRootAndDrill`：dir=0 返回卷与 `volumes`；进入子目录返回 `path` 链与按大小降序的 items。
  - `TestListJSONBadName`：含 `\xff` 名字的条目 → JSON 合法，`badName:true`。
  - `TestListDeletedScan`：删除后再 list → 404 `scan_not_found`。
  - `TestScanStartConflict`：持锁时 → 409 `scan_running`；未持锁 → `Spawn` 被以 `scan --trigger manual` 调用。
  - `TestSettingsRoundTrip`：POST weekly → `RunCmd` 收到 `crontab` 与 `crond.sh restart`，crontab 文件含新行。
- [ ] **Step 2:** FAIL → **Step 3:** 实现 → **Step 4:** `go test ./...` PASS。
- [ ] **Step 5:** `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o build/storage-analyzer ./cmd/storage-analyzer` 成功。

---

### Task 8: 前端界面

**Files:**
- Create: `web/index.html`, `web/app.js`, `web/style.css`

**Interfaces:**
- Consumes: Task 7 的 API（相对路径 `api.cgi?action=...`，POST 带 `X-SA-Request: 1`）

按 spec §9 的布局：顶部工具栏（扫描结果下拉、新建扫描、历史、设置）；面包屑 + 本目录合计 + 磁盘占用/表观大小切换；表格（名称、大小、占比条、项数、修改时间，表头点击排序）；"加载更多"分页；顶层每个卷显示容量条（已扫描 / 未计入 / 可用）；扫描中进度面板（文件数、大小、当前路径、用时、估算百分比 = files/prevFiles，取消按钮），运行中每 2 秒轮询 `status`，结束后自动打开新结果；历史面板（打开、删除）；设置面板；空状态；401/403 显示"请先登录 QTS / 需要管理员权限"；`scan_not_found` 时提示并回到最新结果；键盘 ↑↓ Enter Backspace。大小用 1024 进制，单位 B/KB/MB/GB/TB。

- [ ] **Step 1:** 本地造数据：`go run ./cmd/storage-analyzer scan --root /tmp/... ` 对某个本地目录（开发时 `Discover` 允许 `SA_DEV_ROOTS=/path1:/path2` 环境变量覆盖，仅当未在 QTS 上——即 `/etc/volume.conf` 不存在时生效）。
- [ ] **Step 2:** `go run ./cmd/storage-analyzer serve --insecure-no-auth` 后在浏览器面板打开 `http://127.0.0.1:8099/`，逐项检查：下钻、面包屑、排序、分页、大小模式切换、新建扫描进度与取消、历史删除、设置保存、手机宽度下不横向溢出。截图确认。
- [ ] **Step 3:** 浏览器控制台无报错。

---

### Task 9: 打包 QPKG

**Files:**
- Create: `packaging/qpkg/qpkg.cfg`, `packaging/qpkg/package_routines`, `packaging/qpkg/shared/StorageAnalyzer.sh`, `packaging/qpkg/shared/web/api.cgi`, `packaging/Dockerfile`, `tools/genicons/main.go`, `scripts/build.sh`

- qpkg.cfg 关键值：`QPKG_NAME="StorageAnalyzer"`、`QPKG_DISPLAY_NAME="存储分析器"`、`QPKG_VER="0.1.0"`、`QPKG_AUTHOR="FlintyLemming"`、`QPKG_SERVICE_PROGRAM="StorageAnalyzer.sh"`、`QPKG_WEBUI="/cgi-bin/qpkg/StorageAnalyzer/"`、`QPKG_WEB_PORT="-1"`、`QPKG_DESKTOP_APP="1"`、`QPKG_DESKTOP_APP_WIN_WIDTH="1100"`、`QPKG_DESKTOP_APP_WIN_HEIGHT="680"`、`QTS_MINI_VERSION="5.0.0"`。
- `StorageAnalyzer.sh`：`start` 检查 Enable、建 `data/{scans,run,logs}`、`ln -sf ${QPKG_ROOT}/web /home/httpd/cgi-bin/qpkg/StorageAnalyzer`、`bin/storage-analyzer init-start`；`stop`/`remove` → `init-stop` + 删链接。
- `api.cgi`：`#!/bin/sh` + `exec "$(/sbin/getcfg StorageAnalyzer Install_Path -f /etc/config/qpkg.conf)/bin/storage-analyzer" cgi`（755）。
- `package_routines`：`pkg_pre_remove` 调用 `StorageAnalyzer.sh remove`；`pkg_post_install` 设置 `web/*.{html,js,css}` 为 644、`api.cgi` 与二进制为 755。
- Dockerfile：`ubuntu:24.04` + `build-essential bsdmainutils rsync openssl python3`，克隆 QDK 固定提交 `b7b5f4c86ebe95b10a62d64725515e7cdcf4bb35` 并 `InstallToUbuntu.sh install`。
- `genicons`：64×64 `StorageAnalyzer.gif`、80×80 `StorageAnalyzer_80.gif`、灰度 `StorageAnalyzer_gray.gif`（蓝色圆角方块 + 白色条形图）。
- `build.sh`：交叉编译 → 组装 `build/qpkg/`（`x86_64/bin/storage-analyzer`、`shared/web/{index.html,app.js,style.css,api.cgi}`、`icons/`）→ `docker run ... qbuild --build-arch x86_64` → `build/StorageAnalyzer_0.1.0_x86_64.qpkg`。

- [ ] **Step 1:** `scripts/build.sh` 成功，输出 `.qpkg` 文件。
- [ ] **Step 2:** 解开 qpkg 的数据段检查：二进制、web 文件、权限正确（`head -c` 读脚本头中的 `script_len` 再 `dd | tar -tz`）。

---

### Task 10: 部署到 hf-qts 并端到端验证

**Files:**
- Create: `scripts/deploy.sh`（`scp build/*.qpkg hf-qts:/tmp/` → `ssh hf-qts sudo /sbin/qpkg_cli -m /tmp/<file>`，轮询 `qpkg_cli -s StorageAnalyzer` 直到完成）

- [ ] **Step 1:** 部署；`getcfg StorageAnalyzer Desktop/WebUI/Win_Width -f /etc/config/qpkg.conf` 与预期一致；`/home/httpd/cgi-bin/qpkg/StorageAnalyzer` 链接存在。
- [ ] **Step 2:** `curl http://127.0.0.1:58080/cgi-bin/qpkg/StorageAnalyzer/api.cgi?action=status`（无 cookie）→ 401 `not_logged_in`；`index.html` 正常返回。
- [ ] **Step 3:** `sudo .../bin/storage-analyzer scan --trigger manual` 完整扫描 DataVol1：记录耗时、峰值 RSS（`/proc/<pid>/status` 的 VmHWM）、结果文件大小 / 条目数（目标 ≤ 70B）、合计与 `df` 已用的差额。
- [ ] **Step 4:** 用 `serve --insecure-no-auth` 绑定设备 127.0.0.1 + ssh 隧道，在浏览器面板中验证界面：打开含最多子项的目录 < 300ms；浏览、排序、分页正常。
- [ ] **Step 5:** 设置每天某时间 → `/etc/config/crontab` 出现该行；把时间设为 2 分钟后，确认定时扫描执行（catalog 出现 `schedule` 记录）；关闭后该行消失。
- [ ] **Step 6:** 扫描中 `scan_cancel` → 状态 canceled；`kill -9` 一个扫描后 `StorageAnalyzer.sh restart` → 记录变为 failed、`.tmp` 被清理。
- [ ] **Step 7:** 请用户在 QTS 桌面打开"存储分析器"图标，确认在内嵌窗口中正常显示并能鉴权通过。
- [ ] **Step 8:** 卸载测试：`qpkg_cli -R StorageAnalyzer` → crontab 行与 CGI 链接被清除；再重新安装。
