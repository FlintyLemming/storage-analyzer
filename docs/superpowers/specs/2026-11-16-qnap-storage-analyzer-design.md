# QNAP 存储分析器（Storage Analyzer）设计文档

日期：2026-11-16
状态：待确认

## 1. 意图

在 QNAP QTS 上提供一个"图形版 ncdu"：在 QTS 桌面里以内嵌小窗口打开（与资源监视器等内置程序相同，不跳转新网页），以 root 权限扫描用户存储池上的所有数据，按目录逐层下钻、按占用排序，找出空间被谁吃掉了。

测试机 hf-qts 的数据卷已用 95%（7.5T / 7.9T），这是这个工具最直接的用途。

扫描既可以定时自动执行，也可以手动新建、在界面上等待结果。每一次扫描都作为历史快照保存，打开任意一次历史结果都能下钻到任意一个文件。

## 2. 目标 / 非目标 / 反目标

**目标**
- QPKG 应用，QTS 桌面图标打开内嵌窗口（`Desktop = 1`）。
- 自动识别所有用户存储池上的卷并扫描；排除系统 eMMC、数据盘上的系统分区（md9/md13/md256/md322）、精简池元数据卷（`/mnt/pool2`）。
- 手动新建扫描：显示实时进度，可取消，完成后自动打开结果。
- 定时扫描：关闭 / 每天 HH:MM / 每周某天 HH:MM。
- 历史：列出所有扫描（时间、手动/定时、总占用、文件数、耗时、状态），可打开、可删除，按保留数量自动清理。
- ncdu 式浏览：面包屑路径、占比条、按大小 / 名称 / 文件数 / 修改时间排序、"磁盘占用 / 表观大小"切换、硬链接只计一次。

**非目标（v1 不做）**
- 删除、移动文件等任何修改操作（只读工具）。
- 全盘按文件名搜索、两次扫描对比、全盘最大文件排行。（SQLite 存储为以后加这些留了余地。）
- ARM 机型（测试机为 x86_64，先只出 x86_64 包）。
- 多语言，界面只做中文。

**反目标（即使能跑也算失败）**
- 扫描时拖慢 NAS 的正常服务——扫描必须以最低 CPU / IO 优先级运行。
- 扫描到系统分区，或跨进挂载点重复计算（Container overlay、快照等）。
- 引入常驻后台进程。
- 日常使用需要用户 ssh 上去操作。
- 修改或删除用户的任何文件。

## 3. 约束

- QTS 5.2.11，TS-X53B，Intel J3455（4 核），16GB 内存，x86_64。
- 系统内无 python / perl / php → 静态编译 Go 二进制（`CGO_ENABLED=0`，SQLite 用纯 Go 实现 `modernc.org/sqlite`）。本地有 Go 1.26.5。
- 数据卷剩余约 414GB，单份扫描结果的体积需要控制（见 §6）。
- 当前约 300 万个 inode。
- QTS 的 CGI 由 `_thttpd_`（以 admin，uid 0 运行，监听 127.0.0.1:58080）执行，外层 Apache 代理 `/cgi-bin/`。CGI 和 cron 任务天然都是 root，**不需要 sudo 密码**。
- 鉴权参考机器上已有的 BeszelAgent：读 `NAS_SID` cookie → 请求 `http://127.0.0.1:58080/cgi-bin/authLogin.cgi?sid=...` → 解析 XML 的 `authPassed` / `isAdmin`。

## 4. 方案选择

考虑过的三种做法：

| 方案 | 做法 | 取舍 |
|---|---|---|
| **A（已选）** | 无常驻进程。Go 单二进制同时做 CGI 接口和扫描器；定时用 QTS 自带 crontab；手动扫描临时拉起一个进程 | 空闲零进程零内存；打开历史快 |
| B | 常驻后台服务 + SQLite | 查询灵活，但常驻占内存 |
| C | ncdu 导出格式（压缩 JSON） | 可被 `ncdu -f` 打开，但每次打开要全量解析几百万条，需要常驻缓存 |

**用户决定：采用 A 的进程模型，扫描结果存为 SQLite**（每次扫描一个 SQLite 文件），兼顾"无常驻进程"和"可查询、易扩展"。

## 5. 整体架构

```
QTS 桌面窗口 (iframe)
   │  /cgi-bin/qpkg/StorageAnalyzer/index.html, app.js, style.css   (静态)
   │  /cgi-bin/qpkg/StorageAnalyzer/api.cgi?action=...              (JSON API)
   ▼
Apache → _thttpd_ (root) → api.cgi  ──读──▶ data/catalog.db, data/scans/*.db, data/run/progress.json
                              │
                              └─拉起(setsid, 脱离 CGI)─▶ storage-analyzer scan --trigger manual
cron (/etc/config/crontab) ──────────────────────────▶ storage-analyzer scan --trigger schedule
```

一个二进制 `storage-analyzer`，子命令：
- `cgi`：以 CGI 方式处理请求（`api.cgi` 为调用此模式的入口，可以是包装脚本或二进制副本，由实现决定）。
- `scan --trigger manual|schedule`：执行一次完整扫描。
- `apply-schedule`：按设置重写 crontab 中本应用的那一行。
- `prune`：按保留策略删除旧扫描。

### 5.1 安装目录布局

```
${QPKG_ROOT}/
  StorageAnalyzer.sh          # QPKG 启动/停止脚本
  bin/storage-analyzer
  web/                        # 链接到 /home/httpd/cgi-bin/qpkg/StorageAnalyzer
    index.html  app.js  style.css  (644)
    api.cgi                   (755)
  data/
    catalog.db                # 扫描列表 + 设置
    scans/scan-<id>.db        # 每次扫描一份
    run/scan.lock             # flock + 正在运行扫描的 PID
    run/progress.json         # 运行中扫描的进度
    logs/
```

### 5.2 启动脚本（StorageAnalyzer.sh）

- `start`：建立 `/home/httpd/cgi-bin/qpkg/StorageAnalyzer → ${QPKG_ROOT}/web` 链接；按当前设置写入 crontab；把 catalog 里残留的 `running` 状态（例如扫描中途重启）标为 `failed`。
- `stop` / `remove`：去掉链接；从 crontab 删除本应用那一行；结束正在运行的扫描。
- 没有常驻进程，所以 `start` 只做上面这些准备工作。

### 5.3 qpkg.conf 关键字段

```
Name = StorageAnalyzer
Display_Name = 存储分析器
WebUI = /cgi-bin/qpkg/StorageAnalyzer/
Web_Port = -1
Desktop = 1
Win_Width = 1100
Win_Height = 680
Win_Min_Width = 900
Win_Min_Height = 560
```

## 6. 扫描器

### 6.1 确定扫描范围

1. 解析 `/etc/volume.conf` 中每个 `[VOL_n]` 段：`volName`、`mappingName`（如 `/dev/mapper/cachedev2`）以及存储池标识。
2. 在 `/proc/mounts` 中查 `mappingName` 的挂载点，只接受 `/share/CACHEDEV*_DATA` 形式。
3. 未挂载的卷（例如加密未解锁）跳过，并记入本次扫描的元数据，界面上提示。
4. 存储池编号 / 名称可从 `qcli_storage` 输出或 volume.conf 推出（实现时在设备上确认取哪一个字段）。

每次扫描覆盖所有符合条件的卷；结果的顶层是各个卷（标注所属存储池）。

### 6.2 遍历规则

- `lstat`，不跟随符号链接。
- 不跨文件系统：子目录 `st_dev` 与卷根不同即视为挂载点，记一条"挂载点（已跳过）"的条目，不进入。
- 磁盘占用 = `st_blocks * 512`；表观大小 = `st_size`。
- 硬链接（`nlink > 1`）按 `(dev, ino)` 只计一次，重复的条目照常列出但标记为硬链接重复、不计入父目录合计（与 ncdu 一致）。
- 读取错误（文件在扫描中消失、I/O 错误）记为条目标志，并累计错误数。
- 文件名是原始字节：存储时保留原始字节，非法 UTF-8 的名字在界面上替换显示并标注。
- `@Recycle` 等普通目录照常计入（它们也占空间）。快照不可见于文件树——卷"已用"与"扫描到的合计"之间的差额在界面上单独显示为"未计入（快照 / 文件系统元数据等）"。

### 6.3 性能与资源

- 进程启动后立即设为 nice 19，`ioprio_set` 设为 IDLE 类，避免影响 NAS 正常服务。
- 若干 goroutine 并行 readdir + lstat（数量由实现调优，HDD RAID5 上适度并行会更快），单个写入 goroutine 负责写 SQLite。
- 内存目标：300 万条目时不超过约 300MB。
- 同一时间只允许一个扫描（`flock`）。定时扫描触发时如果已有扫描在运行，直接跳过并记日志。

### 6.4 进度与取消

- 每秒刷新 `run/progress.json`：已扫描文件数、目录数、累计大小、当前路径、开始时间、上一次成功扫描的文件总数（用于估算百分比）。
- 取消：CGI 读取锁文件中的 PID 并发送 SIGTERM；扫描器收到后删除未完成的数据库，并将该次扫描标为 `canceled`。
- 扫描数据先写 `scan-<id>.db.tmp`，成功后改名，所以浏览中的结果永远不会读到写了一半的文件。
- 扫描成功后自动执行 `prune`。

## 7. 存储

### 7.1 每次扫描一份 SQLite（scans/scan-<id>.db）

示意结构（具体列类型、索引方式由实现决定，以达到下面的指标为准）：

```
entries(
  id      INTEGER PRIMARY KEY,
  parent  INTEGER,          -- 卷根的 parent 为 0
  name    BLOB,             -- 原始字节
  kind    INTEGER,          -- 文件 / 目录 / 符号链接 / 其他 / 挂载点(跳过)
  dsize   INTEGER,          -- 磁盘占用（目录为子树合计）
  asize   INTEGER,          -- 表观大小（目录为子树合计）
  items   INTEGER,          -- 目录：子树条目数
  mtime   INTEGER,
  flags   INTEGER           -- 错误 / 硬链接重复 / 非法 UTF-8 等
)
meta(key, value)            -- 开始/结束时间、触发方式、各卷信息（存储池、卷名、挂载点、statfs 总量/已用）、合计
```

指标：
- 一个目录的子项必须能按 `parent` 高效取出并按所选列排序；在设备上打开一个含 1 万个子项的目录 < 300ms。
- 体积目标：每条目平均 ≤ 70 字节，即 300 万条目约 200MB 以内。
- 写入时使用大事务 + 关闭同步（文件本身是临时文件，失败直接丢弃）；完成后建索引。

### 7.2 catalog.db

```
scans(id, started_at, finished_at, trigger, status, total_dsize, total_asize,
      files, dirs, errors, duration_s, db_bytes, error_message)
      -- status: running / done / failed / canceled
settings(key, value)
      -- schedule_mode(off/daily/weekly), schedule_weekday, schedule_time,
      -- retention_count, default_size_mode
```

使用 WAL 模式，CGI 读取与扫描器写入互不阻塞。

### 7.3 保留策略

- 保留最近 N 次成功的扫描，默认 N = 10（可设 1–100）。按单份约 200MB 估算，默认最多占用约 2GB。
- 失败 / 取消的扫描只保留记录，不保留数据文件。
- 用户可在历史列表中手动删除任意一次扫描。

## 8. CGI 接口

所有请求都需要通过 QTS 管理员会话校验；未登录返回 401，非管理员返回 403（界面提示"需要管理员权限"）。修改类请求只接受 POST，并校验同源（如要求自定义请求头），防止跨站请求。

| action | 方法 | 作用 |
|---|---|---|
| `status` | GET | 当前是否有扫描在运行 + 进度 |
| `scans` | GET | 历史列表 |
| `scan_start` | POST | 拉起后台扫描，返回扫描 id；已有扫描运行时返回冲突 |
| `scan_cancel` | POST | 取消当前扫描 |
| `scan_delete` | POST | 删除某次扫描 |
| `list` | GET | 参数 `scan`、`dir`（条目 id，空 = 顶层各卷）、`sort`、`order`、`offset`、`limit`；返回目录自身信息、面包屑路径、子项分页 |
| `settings` | GET/POST | 读取 / 保存定时与保留设置；保存定时后调用 `apply-schedule` |

### 8.1 定时任务

- 设置保存在 catalog.db。
- `apply-schedule` 在 `/etc/config/crontab` 中维护一行带 `# StorageAnalyzer` 标记的任务，然后执行 `crontab /etc/config/crontab` 并重启 crond 使其生效。
- 时间按 NAS 本地时区（当前为 Asia/Hong_Kong）。
- 默认：关闭（安装后由用户在设置里开启）。

## 9. 界面

纯原生 HTML / CSS / JS，所有资源本地提供，不依赖外部 CDN（NAS 可能离线）。风格贴近 QTS 浅色界面。

```
┌ 存储分析器 ───────────────────────────────────────────────────────────┐
│ 扫描结果：[2026-11-16 03:00 · 定时 ▼]   [新建扫描]   [历史]   [设置]     │
├──────────────────────────────────────────────────────────────────────┤
│ 全部卷 › DataVol1（存储池 2）› Multimedia › Movies                     │
│ 本目录 3.2 TB · 812 项        显示：(•)磁盘占用 ( )表观大小              │
├──────────────────────────────────────────────────────────────────────┤
│ 名称 ▾                 大小        占比                 项数    修改时间 │
│ 📁 ..                                                                 │
│ 📁 Anime              1.4 TB  ████████████░░░░  43.8%  12,034  11-10  │
│ 📁 TV                 0.9 TB  ███████░░░░░░░░░  28.1%   8,201  11-02  │
│ 📄 big.mkv           48.2 GB  ▌                  1.5%       —   09-14  │
│ ↪ (挂载点，已跳过) lib/docker/overlay2/…/merged                         │
│                                         [加载更多（剩余 312 项）]       │
└──────────────────────────────────────────────────────────────────────┘
```

- **顶层（全部卷）**：每个卷一行，标注所属存储池，并显示卷容量条：已扫描合计 / 未计入（快照、元数据等）/ 可用空间。
- **浏览**：点目录进入，点面包屑或 `..` 返回；表头点击切换排序；大目录分页（每页约 500 项）。支持键盘 ↑↓ / Enter / Backspace（尽量做，属于实现细节）。
- **扫描中**：顶部进度面板显示已扫描文件数、累计大小、当前路径、用时、相对上一次扫描的估算百分比和"取消"按钮；每 1–2 秒轮询 `status`；完成后自动切换到新结果。扫描期间仍可浏览旧结果。
- **历史**：表格列出时间、手动/定时、状态、总占用、文件数、耗时、结果大小；可打开、可删除。
- **设置**：定时（关闭 / 每天 / 每周 + 时间）、保留数量、默认大小显示方式。
- **空状态**：没有任何扫描时，显示"开始第一次扫描"按钮。
- 正在查看的扫描被删除时，界面给出提示并回到最新一次结果。

## 10. 打包与部署

- 产物：`StorageAnalyzer_<version>_x86_64.qpkg`，通过 App Center"手动安装"。未签名包需要在 App Center 设置中允许"安装没有有效数字签名的应用程序"。
- 在 macOS 上交叉编译（`GOOS=linux GOARCH=amd64 CGO_ENABLED=0`）。QPKG 封装可以用官方开源的 QDK（在 Docker 中运行），也可以按 QPKG 格式自行封装，由实现决定，以 App Center 能安装、卸载为准。
- 图标：64px / 80px / 灰色版 gif。
- 卸载：删除 crontab 行和 CGI 链接；数据目录随安装目录一并删除。

## 11. 测试

- 本地（macOS）单元测试：遍历规则（硬链接只计一次、符号链接不跟随、挂载点边界、错误处理、非法 UTF-8 名字）、SQLite 写入与分页 / 排序查询、保留策略、crontab 行的增改删、CGI 鉴权分支（用假的 authLogin 服务）。
- 设备上端到端：在 hf-qts 安装 → 手动扫描完整个 DataVol1 → 记录扫描耗时、峰值内存、结果文件大小 → 浏览、分页、排序 → 取消扫描 → 定时扫描触发 → 删除历史 / 保留策略生效 → 卸载干净。
- **测试部署的前提**：FlintyLemming 用户的 sudo 需要密码，自动化安装需要 root。需要为该用户配置 NOPASSWD sudo，或者每次由用户在 App Center 手动安装。

## 12. 留给实现的决定

- Go 项目结构、SQLite 的具体 schema 与索引方式（满足 §7.1 的指标即可）。
- 遍历并发度、写入批大小。
- QPKG 封装方式（QDK in Docker 或自行封装）。
- `api.cgi` 入口形式（包装脚本 / 二进制）。
- 同源校验的具体方式。
- 界面的具体视觉样式、图标设计。

## 13. 本文档中由 Claude 做出、待用户确认的决定

1. 工具是**只读**的，不提供删除文件功能。
2. 只有 QTS **管理员**能打开（因为能看到所有人的所有文件名）。
3. 定时选项为 关闭 / 每天 / 每周，**默认关闭**。
4. 默认**保留最近 10 次**扫描（约 2GB）。
5. 一次扫描覆盖**所有**用户存储池上的卷，不单独选卷。
6. 卷"已用"与扫描合计之间的差额显示为"未计入（快照 / 元数据等）"。
7. 界面只做**中文**；只出 **x86_64** 包。
