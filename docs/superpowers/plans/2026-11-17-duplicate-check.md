# 重复文件 / 文件夹检查 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在某次扫描结果上手动发起"重复检查"，经完整 SHA-256 校验找出相同的文件和文件夹，文件夹向上合并到最大一层，按大小混排列出。

**Architecture:** 新包 `internal/dupcheck` 负责算法（向上合并、校验值缓存、分阶段读文件、结果库）；`internal/runner` 新增 `RunDupCheck` 负责锁、catalog、进度；新子命令 `dupcheck --scan N`；API 新增 `dup_start` / `dups`；前端增加 [空间占用 | 重复项] 视图。

**Tech Stack:** Go 1.26（`CGO_ENABLED=0`），`modernc.org/sqlite`，`crypto/sha256`，`golang.org/x/sys/unix`（Fadvise，已在 go.mod），原生 JS/CSS。

**Spec:** `docs/superpowers/specs/2026-11-17-duplicate-check-design.md`（基于 `docs/superpowers/specs/2026-11-16-qnap-storage-analyzer-design.md`）

## Global Constraints

- 只读：只以 `O_RDONLY` 打开用户文件；只写 `data/` 下自己的文件。
- "相同"必须经完整 SHA-256 确认；快速摘要 = SHA-256(前 64KB ‖ 后 64KB)；≤ 128KB 的文件一次读完，快速摘要即完整校验值。
- 忽略名单（任何层级，整棵子树排除）：`@__thumb`、`.@__thumb`、`@eaDir`、`.DS_Store`、`Thumbs.db`（任意类型），以 `._` 开头的非目录。
- 文件名按原始字节比较，区分大小写。
- 与扫描共用 `data/run/scan.lock`；进程 nice 19 + ioprio IDLE；完整校验一次只读一个文件；快速比对 4 个并行；Linux 上至少每 8MB 调用一次 `posix_fadvise(DONTNEED)`。
- 缓存 `data/dupcache.db`，命中条件：路径、dev、inode、大小、mtime_ns、ctime_ns 全部吻合；边算边提交；只在检查成功后清理本次未作为候选出现的路径。
- 结果 `data/scans/dups-<scanID>.db`，先写 `.tmp` 再改名；删除扫描时一并删除。
- 峰值内存目标：300 万条目 ≤ 300MB；`dups` 取一页 < 300ms。
- API：单页默认 100 组（最多 500）；每组最多返回 100 个成员并给出份数。
- 界面只做中文，不依赖外部 CDN；375px 宽无横向滚动。
- 项目不是 git 仓库：计划不含 commit 步骤。

## Review Focus

1. **文件在读的过程中被改写**（大小不变、内容变了）→ 不得报成重复：完整读完后再 lstat 一次，mtime/ctime/大小变了就记为已变化 → Task 3 `TestHashFileChangedDuringRead`。
2. **同一个小文件有成千上万份**（如 5 万个相同的 `cover.jpg`）→ 接口单组最多 100 个成员并返回总份数，界面不卡 → Task 7 `TestDupsMemberCap`。
3. **路径里有非法 UTF-8 / 换行**→ JSON 合法，成员标注 `badName` → Task 7 `TestDupsBadNamePath`。
4. **正在看的扫描被删除 / 被保留策略清理**→ `dups` 与 `dup_start` 返回 404 `scan_not_found` → Task 7 `TestDupsDeletedScan`、`TestDupStartMissingScan`。
5. **文件读不了或卷已不在原处**（权限、卷被锁定）→ 计为出错 / 已变化，检查照常完成而不是整体失败 → Task 3 `TestHashUnreadableFile`、Task 5 `TestCheckMissingRoot`。

---

## 文件结构

```
internal/dupcheck/tree.go          忽略名单、Node/DirInfo/Group 类型、RollUp（纯函数）
internal/dupcheck/tree_test.go
internal/dupcheck/fs.go            FS 抽象 + 真实实现（读时 fadvise）
internal/dupcheck/fadvise_linux.go / fadvise_other.go
internal/dupcheck/cache.go         dupcache.db
internal/dupcheck/cache_test.go
internal/dupcheck/hash.go          分阶段比对
internal/dupcheck/hash_test.go
internal/dupcheck/result.go        dups-<id>.db 写入 / 分页读取
internal/dupcheck/result_test.go
internal/dupcheck/check.go         Run：读扫描库 → 比对 → 合并 → 写结果
internal/dupcheck/check_test.go
internal/scandb/reader.go          新增 EachDir / DupSizeFiles / EachDesc / Roots
internal/catalog/catalog.go        dupchecks 表与方法；Delete/MarkStaleRunning 扩展
internal/runner/runner.go          Progress 增加 job 等字段
internal/runner/dupcheck.go        RunDupCheck
cmd/storage-analyzer/main.go       dupcheck 子命令
internal/api/api.go                dup_start、dups、scans 附带检查状态
web/index.html web/app.js web/style.css
packaging/qpkg/qpkg.cfg            QPKG_VER → 0.2.0
```

---

### Task 1: 向上合并（RollUp）

**Files:** Create `internal/dupcheck/tree.go`, `internal/dupcheck/tree_test.go`

**Interfaces — Produces:**
```go
type Digest [32]byte
type Node struct { ID, Parent int64; Name []byte; Kind, Flags int; ASize, DSize int64 } // scandb 的 kind/flags
type DirInfo struct { Parent int64; Name []byte; Ignored bool }        // Ignored = 自身或任一上级在忽略名单
func IgnoredName(name []byte, isDir bool) bool
func BuildDirs(dirs iter.Seq[Node]) map[int64]DirInfo                 // 输入按 id 升序
type RollUpInput struct {
    Dirs  map[int64]DirInfo
    Files map[int64]Digest   // 已确认重复的文件 → 完整校验值
    Links map[int64]string   // 符号链接 → 目标
}
type Member struct { ID, Parent int64; ASize, DSize int64; Covered bool }
type Group struct {
    Kind  int        // scandb.KindDir / KindFile
    Files int64      // 文件夹组：每份参与比较的文件数；文件组为 1
    ASize, DSize int64   // 单份大小 = 成员最小值
    RASize, RDSize int64 // 可释放
    Members []Member // 按 ID 升序
}
func RollUp(desc iter.Seq[Node], in RollUpInput) ([]Group, error) // desc 必须按 id 降序，否则返回错误
```

**算法要点（签名与测试不能决定的部分）：**
- 按 id 降序处理（子项 id 总大于父项）。每个目录一个累加器 `{x [32]byte; n int64; unique bool; asize, dsize, files int64}`，子项折入父项：`x ^= sha256(name ‖ 0x00 ‖ childToken)`，n++，累加内容大小与文件数（目录内名字唯一，XOR 不会自相抵消）。
- 目录处理到自身时定稿：`token = sha256("D" ‖ x ‖ n)`；任一子项唯一则目录唯一。
- 文件 token：在 `Files` 中 → `sha256("F" ‖ size ‖ digest)`；大小为 0 且无错误 / 硬链接标记 → 常量 `E`；符号链接在 `Links` 中 → `sha256("L" ‖ target)`；其余（含错误、硬链接重复、挂载点、其他类型）→ 唯一。
- 被忽略的条目（`IgnoredName` 或父目录 `Ignored`）直接跳过，不折入父项。
- 目录组：同 token、成员 ≥ 2、单份内容 asize > 0。文件组：同 (size, digest)、成员 ≥ 2。
- 被包含：任一严格上级是某个目录组成员（沿 `Dirs` 向上找）。所有成员都被包含的组不输出。
- 可释放（防止与外层文件夹组重复计算）：`有效份数 = 未被包含的成员数 + 被包含成员所属的不同最外层目录组数`，`可释放 = 单份大小 × (有效份数 − 1)`。

- [ ] **Step 1: 写失败的测试**（用一个小建树助手 `tb := newTree()`；`tb.dir(parent, name)`、`tb.file(parent, name, size, digestByte)` 返回 id，按建树顺序分配递增 id，`tb.run()` 按降序喂给 RollUp）

```go
func TestRollUpNestedFoldersMergeToTop(t *testing.T)   // P{A{x,y}}, Q{B{x,y}} → 只有 1 组：{P,Q}，Kind=Dir，Files=2
func TestRollUpFolderNameMayDiffer(t *testing.T)       // Photos{a} 与 "Photos 副本"{a} → 1 组
func TestRollUpPartialCoverage(t *testing.T)           // §5.3 示例：{A,B} 与 {A/x,B/x,C/x}；后者 A/x、B/x Covered=true，C/x false；RASize = size*1
func TestRollUpFileNameMismatch(t *testing.T)          // A{x}, B{x2} 同内容 → 无目录组；有文件组 {A/x,B/x2}
func TestRollUpExtraFileBreaksFolder(t *testing.T)     // A{x}, B{x,y} → 无目录组
func TestRollUpStructureMatters(t *testing.T)          // A{x}, B{sub{x}} → 无目录组
func TestRollUpIgnoresJunk(t *testing.T)               // A{x,.DS_Store(唯一)}, B{x,@__thumb{t(唯一)},._x(唯一)} → 目录组 {A,B}；忽略项不出现在任何组
func TestRollUpEmptyNotListed(t *testing.T)            // A{e(0B)}, B{e(0B)}, C{}, D{} → 0 组
func TestRollUpUniqueChildPoisonsFolder(t *testing.T)  // 子项带 FlagHardlinkDup / KindMount / 不在 Files 中的文件 → 上级目录不成组
func TestRollUpSymlinkByTarget(t *testing.T)           // 同名同目标的链接 → 目录成组；目标不同 → 不成组
func TestRollUpGroupSizeIsMin(t *testing.T)            // 两份 DSize 4096/8192 → DSize=4096，RDSize=4096
func TestRollUpRejectsAscending(t *testing.T)          // 非降序输入 → err != nil
```

- [ ] **Step 2:** `go test ./internal/dupcheck/ -run RollUp` → 编译失败（未定义）。
- [ ] **Step 3:** 实现 `tree.go`。
- [ ] **Step 4:** `go test ./internal/dupcheck/ -run 'RollUp|Ignored'` → PASS。

---

### Task 2: 校验值缓存

**Files:** Create `internal/dupcheck/cache.go`, `internal/dupcheck/cache_test.go`

**Interfaces — Produces:**
```go
type FileInfo struct { Dev, Ino uint64; Size, MTime, MTimeNS, CTimeNS int64; Regular bool } // MTime 为秒（与 scandb 一致）
type CacheEntry struct { Quick Digest; Full *Digest }
func OpenCache(path string) (*Cache, error)                    // WAL；文件 0600
func (c *Cache) Lookup(path string, fi FileInfo) (CacheEntry, bool) // 六个字段全吻合才命中
func (c *Cache) Put(path string, fi FileInfo, e CacheEntry) error   // 缓冲；满 500 条或距上次提交 ≥ 2s 自动提交
func (c *Cache) Touch(path string) error                         // 标记本次运行见过
func (c *Cache) Flush() error
func (c *Cache) Prune() (int64, error)                           // 删除本次运行未 Put/Touch 的行
func (c *Cache) Close() error                                    // 先 Flush
```
运行标识：`OpenCache` 时取 `time.Now().UnixNano()` 作为 run id，写入每行的 `seen` 列。

- [ ] **Step 1: 写失败的测试**
```go
func TestCacheHitRequiresAllFields(t *testing.T)   // Put 后同 fi 命中；分别改 Dev/Ino/Size/MTimeNS/CTimeNS → 未命中
func TestCachePersistsAcrossOpen(t *testing.T)     // Put → Close → 重新 Open → Lookup 命中，Full 保持
func TestCacheAutoCommitSurvivesCrash(t *testing.T) // Put 600 条后不 Close，另开连接可读到 ≥ 500 条
func TestCachePrune(t *testing.T)                   // 第一次运行 Put a,b；第二次 Touch a → Prune 删 1 行，b 不再命中
```
- [ ] **Step 2:** 运行 → FAIL。 **Step 3:** 实现。 **Step 4:** `go test ./internal/dupcheck/ -run Cache` → PASS。

---

### Task 3: 分阶段比对（Hasher）

**Files:** Create `internal/dupcheck/fs.go`, `fadvise_linux.go`, `fadvise_other.go`, `hash.go`, `hash_test.go`

**Interfaces — Consumes:** Task 2 `Cache`、`FileInfo`、`CacheEntry`、`Digest`。
**Produces:**
```go
type File interface { io.Reader; io.ReaderAt; Close() error }
type FS interface {
    Lstat(path string) (FileInfo, error)
    Open(path string) (File, error)
    Readlink(path string) (string, error)
}
func OSFS() FS                                  // Open 返回的 File 每读 8MB 调一次 fadvise(DONTNEED)，Close 时整文件再调一次
type Candidate struct { ID int64; Path string; Size, MTime int64 } // Size=扫描时 asize，MTime=扫描时秒
type Progress struct {
    Phase   atomic.Value // "prepare" | "quick" | "full" | "rollup" | "write"
    Current atomic.Value // string
    DoneFiles, TotalFiles, DoneBytes, TotalBytes, CacheHits, Changed, Errors, BytesRead atomic.Int64
}
type Hasher struct { FS FS; Cache *Cache; Progress *Progress; QuickWorkers int } // QuickWorkers 默认 4
func (h *Hasher) Run(ctx context.Context, groups [][]Candidate) (map[int64]Digest, error)
// 返回已确认重复（同 size+完整校验值、≥2 份）的文件；ctx 取消返回 ctx.Err()
```
流程：lstat 核对（不存在 / 非普通文件 / 大小或 mtime 秒不同 → Changed++）→ 查缓存（命中 CacheHits++，并 Touch）→ 按大小重新分组去掉单个 → 快速比对（Phase "quick"，TotalBytes = Σmin(size,128KB) 未命中项）→ 按 (size, quick) 分组去单 → 完整校验（Phase "full"，一次一个文件，TotalBytes = Σsize 需读项）→ 读完再 lstat，与读前 FileInfo 不同 → Changed++ 并剔除 → 按 (size, full) 分组去单。读失败 → Errors++ 并剔除。每个读过的文件 `Cache.Put`。

- [ ] **Step 1: 写失败的测试**（真实临时文件，`countFS` 包装 `OSFS()` 统计 Open 次数与读取字节）
```go
func TestHashFindsSameContent(t *testing.T)           // 3 个 300KB 相同 + 1 个同大小不同 → 结果恰为前 3 个，digest 相同
func TestHashMiddleDiffers(t *testing.T)              // 两个 1MB 文件首尾相同、只有中间 1 字节不同 → 结果为空
func TestHashSmallFilesReadOnce(t *testing.T)         // 两个 100KB 相同 → 命中，每个文件只 Open 一次
func TestHashChangedSinceScan(t *testing.T)           // Candidate.MTime 与实际不同 → 剔除，Progress.Changed == 1
func TestHashFileChangedDuringRead(t *testing.T)      // 假 FS：第二次 Lstat 返回不同 CTimeNS → 该文件剔除，Changed == 1
func TestHashUnreadableFile(t *testing.T)             // 假 FS Open 返回 EACCES → Errors == 1，其余照常
func TestHashCacheSkipsReads(t *testing.T)            // 同一批跑两次（同一 Cache）→ 第二次 Open 次数 == 0，CacheHits == 候选数
func TestHashCanceled(t *testing.T)                   // ctx 已取消 → 返回 context.Canceled
```
- [ ] **Step 2:** 运行 → FAIL。 **Step 3:** 实现。 **Step 4:** `go test -race ./internal/dupcheck/ -run Hash` → PASS。

---

### Task 4: 结果库

**Files:** Create `internal/dupcheck/result.go`, `result_test.go`

**Interfaces — Consumes:** Task 1 `Group`、`Member`。
**Produces:**
```go
type OutMember struct { EntryID, ParentID int64; Path []byte; Covered bool }   // Path = "<卷名>/<相对路径>" 原始字节
type OutGroup struct {
    ID int64; Kind int; Name []byte; Copies, Files, ASize, DSize, RASize, RDSize int64
    Members []OutMember // 按 Path 升序
}
func WriteResult(path string, groups []OutGroup, meta map[string]string) error // 写 path+".tmp" → 改名；失败删除 tmp
func OpenResult(path string) (*Result, error)                                 // 只读
func (r *Result) Total() (int, error)
func (r *Result) Page(sizeMode string, offset, limit, maxMembers int) ([]OutGroup, error)
// 排序：单份大小降序 → 可释放降序 → 首个成员 Path 升序；写入时按两种模式预算 rank 列并建索引
func (r *Result) Meta() (map[string]string, error)
func (r *Result) Close() error
```
`Name` = 首个成员路径的最后一段。

- [ ] **Step 1: 写失败的测试**
```go
func TestResultPageOrderDisk(t *testing.T)      // 3 组 DSize 10/30/20 → Page("disk") 顺序 30,20,10；"apparent" 按 ASize
func TestResultTieBreak(t *testing.T)           // 同大小 → 可释放大的在前；再同 → 路径小的在前
func TestResultPagingAndTotal(t *testing.T)     // 250 组，Page(offset=200, limit=100) 返回 50，Total()==250
func TestResultMemberCap(t *testing.T)          // 一组 300 成员，maxMembers=100 → 返回 100 个，Copies==300
func TestResultBadBytesRoundTrip(t *testing.T)  // 路径含 "\xff\n" → 原样读回
```
- [ ] **Step 2:** 运行 → FAIL。 **Step 3:** 实现。 **Step 4:** `go test ./internal/dupcheck/ -run Result` → PASS。

---

### Task 5: 检查编排 + 扫描库读取

**Files:** Modify `internal/scandb/reader.go`（+ `scandb_test.go`）；Create `internal/dupcheck/check.go`, `check_test.go`

**Interfaces — Produces（scandb）：**
```go
func (d *DB) Roots() int64
func (d *DB) EachDir(fn func(Entry) error) error        // kind=Dir，id 升序
func (d *DB) DupSizeFiles(fn func(Entry) error) error   // kind=File，flags&(Error|HardlinkDup)=0，asize>0，且该 asize 至少 2 个这样的文件；id 升序
func (d *DB) EachDesc(fn func(Entry) error) error       // 全部条目，id 降序
```
**Produces（dupcheck）：**
```go
type Root struct { Name, MountPoint string }
type Summary struct { Groups, ReclaimDSize, ReclaimASize, BytesRead, CacheHits, Changed, Errors int64 }
type Options struct {
    ScanDB string; Roots map[int64]Root; Cache *Cache; Out string
    Progress *Progress; FS FS; QuickWorkers int
}
func Run(ctx context.Context, o Options) (Summary, error)
```
流程：`EachDir` → `BuildDirs`（Phase "prepare"）→ `DupSizeFiles` 去掉 Ignored，按大小分组去单 → `Hasher.Run` → `EachDesc` 收集符号链接目标（`FS.Readlink`，失败则不放入 Links）并喂给 `RollUp`（Phase "rollup"）→ 拼路径 → `WriteResult`（Phase "write"）→ `Cache.Prune`。meta 写入 summary 各字段。路径：文件系统路径 = `Roots[rootID].MountPoint + "/" + 相对路径`，显示路径 = `Roots[rootID].Name + "/" + 相对路径`。

- [ ] **Step 1: 写失败的测试**
```go
// scandb
func TestDupSizeFiles(t *testing.T)   // 大小 5,5,7,0,0 与一个 FlagHardlinkDup 的 7 → 只返回两个 5
func TestEachDescOrder(t *testing.T)  // id 严格降序且覆盖全部条目
// dupcheck：真实临时目录 → scanner.Walker 写 scandb → Run
func TestCheckEndToEnd(t *testing.T)  // vol/{A{m.bin 200KB,s/x 10B}, B{m.bin,s/x}, C/m.bin, D/u.bin 同大小不同内容}
                                       // → 2 组：{A,B}（Kind Dir）与 {A/m.bin,B/m.bin,C/m.bin}；A/s 组不出现；D/u.bin 不出现
                                       // → 显示路径以 "vol/" 开头；Summary.Groups==2
func TestCheckSecondRunUsesCache(t *testing.T) // 同 Cache 再跑：Summary.CacheHits == 候选数，BytesRead == 0
func TestCheckMissingRoot(t *testing.T)        // Roots 的 MountPoint 指向不存在目录 → 不报错，Groups==0，Changed>0
func TestCheckCanceledRemovesTmp(t *testing.T) // 取消 → err=context.Canceled，Out 与 Out+".tmp" 都不存在
```
- [ ] **Step 2:** 运行 → FAIL。 **Step 3:** 实现。 **Step 4:** `go test -race ./internal/scandb/ ./internal/dupcheck/` → PASS。

---

### Task 6: catalog、runner、子命令

**Files:** Modify `internal/catalog/catalog.go`（+test），`internal/runner/runner.go`；Create `internal/runner/dupcheck.go`（+test）；Modify `cmd/storage-analyzer/main.go`

**Interfaces — Produces:**
```go
// catalog
type DupCheck struct {
    ScanID int64 `json:"scanId"`; Status string `json:"status"` // running|done|failed|canceled
    StartedAt, FinishedAt, DurationS, Groups, ReclaimDSize, ReclaimASize, BytesRead, CacheHits, Changed, Errors int64
    ErrorMessage string
}   // JSON 字段名 camelCase
func DupsPath(dataDir string, scanID int64) string                // scans/dups-<id>.db
func (c *Catalog) BeginDupCheck(scanID int64, now time.Time) error // INSERT OR REPLACE，status=running
func (c *Catalog) FinishDupCheck(d DupCheck) error
func (c *Catalog) FailDupCheck(scanID int64, status, msg string, now time.Time) error
func (c *Catalog) GetDupCheck(scanID int64) (DupCheck, error)      // ErrNotFound
// Scan 增加字段 Dup *DupCheck `json:"dup"`，List() 附带
// Delete 同时删 dupchecks 行与 DupsPath 文件；MarkStaleRunning 同时处理 dupchecks
// runner
// Progress 增加：Job string `json:"job"`（"scan"|"dupcheck"）, Phase string, DoneFiles, TotalFiles, DoneBytes, TotalBytes, CacheHits, Changed int64
type DupOptions struct { DataDir string; ScanID int64; Now func() time.Time; FS dupcheck.FS }
func RunDupCheck(ctx context.Context, o DupOptions) (dupcheck.Summary, error) // ErrBusy；扫描不存在/未完成 → ErrScanNotFound
var ErrScanNotFound = errors.New("scan not found")
```
子命令：`storage-analyzer dupcheck --scan <id>`：`SetLowPriority` → 写日志 `data/logs/scan.log` → 信号取消 → ErrBusy 时记日志并退出 0。

- [ ] **Step 1: 写失败的测试**
```go
func TestDupCheckLifecycle(t *testing.T)        // catalog: Begin → Get status running → Finish → List()[0].Dup.Groups 正确
func TestDeleteRemovesDups(t *testing.T)        // Delete 后 DupsPath 文件与 GetDupCheck 都不存在
func TestMarkStaleRunningDupCheck(t *testing.T) // running 的检查 → failed，error_message "interrupted"
func TestRunDupCheckDone(t *testing.T)          // runner: 先 Run 一次扫描（fakeVolumes 加两个相同文件）→ RunDupCheck → catalog 状态 done，Groups==1，DupsPath 存在，progress.json 已删除
func TestRunDupCheckBusy(t *testing.T)          // holdLock → ErrBusy
func TestRunDupCheckMissingScan(t *testing.T)   // ScanID 999 → ErrScanNotFound
func TestRunScanProgressJob(t *testing.T)       // 扫描进行中的 progress.json 里 job == "scan"（用已有 TestRunningReadsProgress 的方式）
```
- [ ] **Step 2:** 运行 → FAIL。 **Step 3:** 实现。 **Step 4:** `go test -race ./internal/catalog/ ./internal/runner/` 与 `go build ./...` → PASS。

---

### Task 7: API

**Files:** Modify `internal/api/api.go`, `internal/api/api_test.go`

**Interfaces — Consumes:** Task 4 `OpenResult/Page/Total`，Task 6 catalog/runner。
**Produces:**
- `dup_start`（POST，`scan`）：任务在跑 → 409 `task_running` "已有任务正在进行"；扫描不存在 / 非 done / 文件缺失 → 404 `scan_not_found`；否则 `Spawn("dupcheck","--scan",id)` → 202 `{"started":true}`。
- `dups`（GET，`scan` 必填、`size`、`offset`、`limit` 默认 100 最大 500）→ `{"scan":…, "check": DupCheck|null, "total":n, "groups":[{id,kind,name,badName,copies,files,asize,dsize,rasize,rdsize,members:[{entryId,parentId,path,badName,covered}]}]}`。无检查 → `check:null,total:0,groups:[]`；结果文件存在就返回组（重新检查进行中也返回旧结果）。
- `scan_start` 冲突改为 409 `task_running` "已有任务正在进行"（UI 同步）。
- `scans` 的每项带 `dup`。

- [ ] **Step 1: 写失败的测试**
```go
func TestDupStartSpawns(t *testing.T)       // spawned == [["dupcheck","--scan",id]]，202
func TestDupStartConflict(t *testing.T)     // lockData → 409 task_running
func TestDupStartMissingScan(t *testing.T)  // scan=999 → 404 scan_not_found
func TestDupsNoCheck(t *testing.T)          // check == nil，total == 0
func TestDupsListing(t *testing.T)          // 手写结果库（WriteResult）+ FinishDupCheck → groups 顺序、size=apparent 切换
func TestDupsMemberCap(t *testing.T)        // 一组 300 成员 → members 长度 100，copies 300
func TestDupsBadNamePath(t *testing.T)      // 路径 "a/b\xff\nc" → JSON 合法，badName true
func TestDupsDeletedScan(t *testing.T)      // 删扫描后 → 404 scan_not_found
func TestScansIncludeDup(t *testing.T)      // scans[0].dup.groups 正确
```
- [ ] **Step 2:** 运行 → FAIL。 **Step 3:** 实现。 **Step 4:** `go test ./internal/api/` → PASS。

---

### Task 8: 界面

**Files:** Modify `web/index.html`, `web/app.js`, `web/style.css`

内容按设计 §9：
- 工具栏右侧加视图切换 `[空间占用 | 重复项]`（`.seg`，`data-view`），hash `#scan=N&view=dups`。
- 重复项视图：汇总行（"N 组重复，可释放 X · 检查于 时间 · M 个文件扫描后有变化 [重新检查]"）；表头 名称 / 份数 / 单份大小 / 可释放；组行可展开，展开行列出路径、"位于重复文件夹内"标签、↗ 跳转（切到空间视图 → `openDir(parentId)` → 选中 entryId，若在当前页）；文件夹组展开行显示"每份 N 个文件"；加载更多。
- 未检查：说明（会读取大小相同的文件内容、首次可能需要数小时、以最低优先级运行）+ "开始重复检查"；失败 / 已取消：原因 + "重新检查"，取消时提示"已算过的文件下次不会重读"。
- 进度面板按 `progress.job` 区分：dupcheck 显示 阶段（准备中 / 快速比对 / 完整校验 / 汇总 / 保存结果）、阶段百分比（DoneBytes/TotalBytes）、已读 / 需读、缓存命中、已变化、当前文件；完成时 toast "重复检查完成：N 组，可释放 X"，若正看该扫描的重复项视图则刷新；扫描完成逻辑只在 job=="scan" 时执行。
- 历史表加列"重复检查"：— / 进行中 / 可释放 X / 失败。
- 大小模式切换在重复项视图下重新加载。
- 375px：隐藏"份数"列，无横向滚动。

- [ ] **Step 1:** 实现。
- [ ] **Step 2:** 构建 dev 二进制，`SA_DEV_ROOTS` 指向含重复数据的临时目录，`preview_start storage-analyzer-dev`：新建扫描 → 切到重复项 → 开始检查 → 看进度 → 结果展开 / 跳转 / 加载更多 / 大小切换 / 历史列 / 375px 无横向滚动；控制台无错误。

---

### Task 9: 打包与设备验证

**Files:** Modify `packaging/qpkg/qpkg.cfg`（`QPKG_VER="0.2.0"`）

- [ ] **Step 1:** `go test ./...` 与 `GOOS=linux GOARCH=amd64 go test -c` 各包 → 在 hf-qts 上运行测试二进制，全部 PASS。
- [ ] **Step 2:** `scripts/build.sh` → `build/StorageAnalyzer_0.2.0_x86_64.qpkg`；`scripts/deploy.sh` 安装（升级保留数据）。
- [ ] **Step 3:** 设备上：若无近期扫描先跑一次扫描；对最新扫描跑 `dupcheck`（通过 API `dup_start`），记录耗时、读取字节、峰值内存（VmHWM）、结果大小、组数、可释放；检查期间 NAS 正常响应。
- [ ] **Step 4:** 不改数据再跑一次：缓存命中率与耗时；检查进行中触发扫描 → 409 / 定时跳过。
- [ ] **Step 5:** curl `dups` 首页 < 300ms；结果写入 `.superpowers` 进度记录。
