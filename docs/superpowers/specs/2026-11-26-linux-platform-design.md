# 通用 Linux 版设计文档

日期：2026-11-26
状态：待确认
基于：[2026-11-16-qnap-storage-analyzer-design.md](2026-11-16-qnap-storage-analyzer-design.md) 及其后的查重、ARM、定时查重设计。本次让同一套代码也能装在普通 Linux 服务器上。

## 1. 意图

用户觉得 QNAP 版好用，想在普通 Linux 服务器上也能用，并且像 QNAP 版一样**发布给别人装**。功能与 QNAP 版一致：扫描、逐层下钻、查重、定时、历史。QNAP 版继续发布，现有用户照常覆盖升级。

用户已做的决定：

- **一个仓库**：改名为 `storage-analyzer`，QNAP 作为平台之一。Release 同时出 `.qpkg` 和 Linux 包。不拆子仓库。
- **登录**：内置密码登录。要求简单、安全、攻击面小。
- **进程模型**：常驻服务，定时在进程内（§4 方案 A）。
- **受众**：发布给别人装，不只是自用。

文中标"**（提案）**"的是我替用户做的决定，用户尚未逐条确认，汇总见 §14。

## 2. 目标 / 非目标 / 反目标

**目标**

- 在使用 systemd 的 Linux 发行版上以常驻服务运行，支持 x86_64、arm64、armv7。用浏览器访问，功能与 QNAP 版一致。
- 单一密码登录。密码只能在服务器上用命令设置。
- 默认自动发现本机磁盘上的文件系统。可在配置文件里改为指定路径，或排除某些挂载点。
- 发布 tar.gz、deb、rpm 三种 Linux 包。推 tag 后由 GitHub Actions 自动构建并发布全部产物，包括 qpkg。
- 仓库改名；README 改成通用介绍，分 QNAP 和 Linux 两部分写安装说明，中英文各一份。

**非目标（本次不做）**

- Docker 镜像，以及非 systemd 系统（OpenRC 等）的服务脚本。二进制本身可以手动运行，但不提供、也不测试这些服务文件。**（提案）**
- 多用户、用户名、按用户限定可见范围、找回密码、两步验证、API token、OAuth，以及信任反代传来的用户名头。
- 在网页上选择扫描范围或浏览服务器目录。扫描范围只能在配置文件里改。
- 排除某个挂载点内部的子目录，例如只排除 `/var/lib/docker` 这个目录。v1 只能排除整个挂载点。**（提案）**
- 自动申请证书（ACME）或生成自签证书。
- 进入没有单独挂载的 btrfs 子卷。与 `ncdu -x` 一致，把它当作边界跳过（见 §7.1）。
- macOS / Windows 作为服务端。macOS 只用于开发。
- 软件包仓库（apt/yum 源）和包签名。

**反目标（即使能跑也算失败）**

- QNAP 现有用户升级后，数据、设置或行为有任何变化或丢失。
- Linux 版服务以 root 运行，或者有能力写入、删除用户的文件。
- 没设密码也能访问；或者未登录时，除了登录页和前端静态文件，还能拿到任何东西。
- 默认扫描网络文件系统（NFS、SMB、sshfs 等）或虚拟文件系统，或者同一块空间被重复计算。
- QNAP 二进制里带着密码登录和 HTTP 服务，或者 Linux 二进制里带着 QTS 认证和 CGI 入口。各平台只编进自己需要的代码。
- 引入较大的第三方依赖，例如 Web 框架或配置文件库。
- 修改或删除用户的任何文件。这仍然是一个只读工具。

## 3. 约束

- 沿用现有技术栈：Go 1.26.5，`CGO_ENABLED=0` 静态编译，`modernc.org/sqlite`，`golang.org/x/sys`。
- 新依赖只允许 Go 官方扩展库，即 `golang.org/x/crypto`（argon2）和 `golang.org/x/term`（不回显地读密码）。
- `CAP_DAC_READ_SEARCH` 可以绕过读权限检查，读任何文件和目录，扫描和查重算哈希都够用，但不能写。查重打开文件时用的是普通只读方式（没有 `O_NOATIME`），这个能力就足够。
- **systemd 沙盒不能改变服务看到的文件树**，否则扫描结果会出错。因此不能用 `PrivateTmp`、`ProtectHome=yes/tmpfs`、`TemporaryFileSystem`、`InaccessiblePaths` 这类会遮挡、替换或隐藏目录的选项。`ProtectSystem=strict`（把整个系统重新挂成只读，不隐藏任何东西）可以用。
- QNAP 版在 QTS 桌面的 iframe 里打开，所以禁止被嵌入的响应头只能加在 Linux 版上。
- 前端 `app.js` 的 `el()` 用属性方式写 `style`（如 [app.js:285](../../../web/app.js)）。在严格的 CSP（`style-src 'self'`）下这会被拦截，需要改用 CSSOM（`element.style.xxx`）写样式，或者放宽 `style-src`。优先选前者。
- 现在的发布流程是在本地运行 `scripts/build.sh`，在 Docker 里调用 QDK 的 qbuild 打包。仓库还没有 CI。
- 扫描子进程目前从可执行文件所在位置推出数据目录（`<root>/bin` → `<root>/data`）。Linux 上二进制装在 `/usr/bin`，这个推法不成立，必须显式传入配置。

## 4. 方案选择

### 4.1 进程模型

| 方案 | 做法 | 取舍 |
|---|---|---|
| **A（用户选定）** | 常驻 systemd 服务。HTTP、登录、定时都在一个进程里；扫描和查重仍然作为子进程运行 | 简单通用，改定时不需要碰系统配置。可以用非 root 用户加只读能力运行，再套上沙盒，被攻破也写不了文件。代价是空闲时常驻一个进程，大约十几 MB 内存，CPU 为零 |
| B | socket activation 按需启动，空闲后退出；定时交给 systemd timer | 空闲时零进程。但网页改定时要改 timer 文件并 `daemon-reload`，需要 root，面向网络的进程只能用 root 跑，攻击面更大；而改成每分钟拉起一次检查又省不了资源。只能在 systemd 上用 |
| C | 权限分离：网页进程不带特权，扫描由另一个带读权限的 systemd 单元执行，网页通过 path unit 触发 | 网页进程被攻破也读不到系统文件。但要三个单元配合，进度和取消都靠文件传递，排错难，也离不开 systemd。网页本来就没有读取文件内容的接口，A 加沙盒已经把这个风险压得很低 |

### 4.2 仓库组织

| 方案 | 取舍 |
|---|---|
| **单仓库改名（用户选定）** | 和 QNAP 绑定的代码很少，集中在卷发现、认证、crontab、CGI 入口和打包。前端、API 和数据格式两边共用，一起演进 |
| 拆出核心仓库，QNAP 仓库用 submodule 引用 | 现有包都在 `internal/` 下，跨 module 无法导入，得全部公开并维护对外 API 的稳定。一个功能要改两个仓库、更新 submodule、发两次版本。不采用 |

## 5. 总体结构

### 5.1 编译时选择平台（提案）

用 build tag 区分：`-tags qnap` 编出 QNAP 二进制，不带 tag 编出通用 Linux 二进制。两种包本来就是分开构建的，这样每个平台的二进制只包含自己的入口，攻击面更小（见 §2 反目标）。

| | QNAP（`qnap`） | Linux（默认） |
|---|---|---|
| 入口 | `cgi`，经 QTS 的 thttpd | `serve`，自带 HTTP 服务 |
| 认证 | `QTSAuth`（`NAS_SID` → `authLogin.cgi`），保持不变 | 内置密码（§6） |
| 卷发现 | `/etc/volume.conf` + `qcli_storage`，保持不变 | `/proc/self/mountinfo` + 配置文件（§7） |
| 定时 | 写 `/etc/config/crontab`，保持不变 | 进程内定时器（§8） |
| 静态文件 | QTS 从安装目录的 `web/` 提供 | `go:embed` 打进二进制 |
| 其他子命令 | `apply-schedule`、`init-start`、`init-stop` | `passwd` |
| 共用 | `scan`、`dupcheck`、`prune`，以及扫描器、存储、查重、runner、API 处理器和前端 | |

`api.Config` 里和 QNAP 绑定的部分（`CrontabPath`、在设置接口里直接调用 `schedule.Apply`）改为由平台注入的钩子，例如"设置保存后应用定时"。QNAP 继续写 crontab；Linux 什么都不用做，定时器每分钟会自己读一次设置。

在 macOS 上开发时编出的是 Linux 版（不带 tag）。现有的开发模式 `serve --insecure-no-auth`（只允许本机回环地址）可以保留，便于调前端。

### 5.2 Linux 安装布局

```
/usr/bin/storage-analyzer                       二进制（deb/rpm；tar.gz 的 install.sh 装到 /usr/local/bin）
/etc/storage-analyzer/config                    root:root 0644，配置文件（§7.2）
/etc/storage-analyzer/password                  root:storage-analyzer 0640，argon2id 哈希
/var/lib/storage-analyzer/                      storage-analyzer:storage-analyzer 0700
    catalog.db  scans/  run/  logs/             与 QNAP 的 data/ 目录结构相同
    （会话、服务端密钥等 Linux 专用文件也放这里）
/usr/lib/systemd/system/storage-analyzer.service   （tar.gz 装到 /etc/systemd/system）
```

数据目录默认是 `/var/lib/storage-analyzer`，可以在配置里用 `data_dir` 改。改了之后，还要在 systemd override 里加对应的 `ReadWritePaths`，这一点要写进文档。`SQLITE_TMPDIR` 照旧指向数据目录。

服务进程自己的日志写到 stderr，进 journald。扫描和查重子进程的日志沿用 `data/logs/scan.log`，实现时也可以改成一起写 stderr。

### 5.3 systemd unit 要点

```ini
[Service]
User=storage-analyzer
Group=storage-analyzer
ExecStart=/usr/bin/storage-analyzer serve --config /etc/storage-analyzer/config
AmbientCapabilities=CAP_DAC_READ_SEARCH
CapabilityBoundingSet=CAP_DAC_READ_SEARCH
NoNewPrivileges=yes
ProtectSystem=strict
StateDirectory=storage-analyzer
StateDirectoryMode=0700
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallArchitectures=native
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
Restart=on-failure
RestartPreventExitStatus=78
```

- ambient capability 会被子进程继承，所以扫描和查重子进程同样能读所有文件。
- 服务停止时，systemd 会结束整个 cgroup。正在跑的扫描或查重收到 SIGTERM 后按"已取消"处理，和 QNAP 的 `init-stop` 一样。
- 默认端口 8099 不需要额外权限。用户如果要改成 1024 以下的端口，需要在 override 里加 `CAP_NET_BIND_SERVICE`，这一点写进文档。
- 最终的沙盒选项清单由实现者在真机上验证。原则是遵守 §3：不能遮挡、替换或隐藏任何要扫描的目录。

### 5.4 Linux 子命令

- `serve --config <path>`：常驻服务。
  - 启动时先做现在 `init-start` 做的准备工作：建目录、清理残留的临时文件、把残留的 `running` 状态标为 `failed`。然后开始监听。
  - 没有密码文件或配置有误时，打印明确的提示（例如"请先运行 `sudo storage-analyzer passwd`"），以退出码 78 退出。systemd 遇到这个退出码不会反复重启。
  - 收到 SIGTERM 时，先取消正在运行的任务，再退出。
  - 拉起扫描或查重子进程时传 `--config`，让子进程用同一个数据目录和扫描范围。
- `passwd [--config <path>]`：用 root 运行。在终端里让用户输入两次密码，不回显；不是终端时从 stdin 读一行，方便自动化。写入哈希后，所有已登录的会话立即失效（§6.2）。
- `scan`、`dupcheck`、`prune`：行为不变，新增 `--config` 参数。

## 6. 登录与安全

### 6.1 密码

- 只有一个密码，没有用户名。长度至少 12 个字符，不设复杂度规则。**（提案）**
- 哈希用 argon2id，参数 m=19 MiB、t=2、p=1，16 字节盐，按 PHC 字符串格式（`$argon2id$v=19$m=19456,t=2,p=1$…$…`）存进 `/etc/storage-analyzer/password`。**（提案）**
- 同一时间只做一次哈希校验，排队等待，防止有人并发请求把内存和 CPU 打满。
- 服务启动时读取密码文件；文件变化后自动重新读取。所以运行 `passwd` 之后不用重启服务。

### 6.2 登录流程

- 未登录时请求除 `login` 之外的任何 API，都返回 401 `{"error":"not_logged_in","auth":"password"}`。前端看到 `auth: "password"` 就显示登录表单。为了不多出一个无需登录的接口，不单独做"查询平台信息"的接口。
- `POST /api.cgi?action=login`：请求体是 `{"password":"…"}`，不超过 1 KB，必须带 `X-SA-Request: 1`。
  - 成功：下发 `sa_session` cookie，值为 32 字节随机数，属性为 `HttpOnly; SameSite=Strict; Path=/; Max-Age=2592000`。走 HTTPS 时（直接 TLS，或者可信代理报告 `X-Forwarded-Proto: https`）再加 `Secure`。
  - 失败：返回 401 `bad_password`。响应统一延迟约 1 秒。
- `POST /api.cgi?action=logout`：删除服务端会话，并清掉浏览器 cookie。
- 会话存在服务端的数据目录里，只存 token 的 SHA-256、创建时间、过期时间，以及创建会话时密码哈希的指纹。
  - 有效期 30 天，从登录时算起，期间使用不会延长。**（提案）**
  - 服务重启后会话仍然有效。
  - 改密码会改变指纹，旧会话全部失效。`passwd` 命令和服务进程之间不需要额外通信。
  - 会话怎么存由实现者决定，但不能改动 QNAP 上现有数据的格式。
- 只有 Linux 版在工具栏上显示"退出登录"。任何请求返回 401（例如会话过期）时，前端回到登录表单。

### 6.3 防暴力破解

- **按来源 IP 锁定**：15 分钟内失败 5 次，锁定这个 IP 15 分钟。锁定期间请求直接返回 429 `too_many_attempts`，不做哈希校验。**（提案）**
- **全局限速**：全部来源加起来每分钟最多校验 20 次，超出的返回 429。不做全局锁死，免得别人把主人也锁在门外。**（提案）**
- **来源 IP 怎么取**：默认用 TCP 连接的对端地址。只有配置了 `trusted_proxies`，并且对端地址在其中时，才从 `X-Forwarded-For` 里从右往左取第一个不可信的地址。没有配置时，`X-Forwarded-For` 和 `X-Forwarded-Proto` 一律忽略。
- 每次失败写一行格式固定的日志，例如 `login failed from <ip>`，方便用户自己接 fail2ban。

### 6.4 HTTP 服务加固

- 只提供三类路径：`/`、前端静态文件（来自 embed，不读磁盘）、`/api.cgi`。其他路径一律 404。
- 静态文件不登录也能取，因为登录页需要它们。它们是公开的前端代码，不含任何数据。
- 响应头：
  - `Content-Security-Policy: default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'`（注意 §3 关于 `style` 的约束）
  - `X-Frame-Options: DENY`
  - `X-Content-Type-Options: nosniff`
  - `Referrer-Policy: no-referrer`
  - API 响应保留现有的 `Cache-Control: no-store`
- 超时与大小限制：设置 `ReadHeaderTimeout`、`ReadTimeout`、`WriteTimeout`、`IdleTimeout`，限制请求头和请求体的大小。具体数值由实现者定，原则是防住慢速连接攻击，同时不截断大目录的 `list` 响应。
- 写操作照旧要求 POST 并带 `X-SA-Request` 头，`SameSite=Strict` 的 cookie 再多挡一层。
- API 只读取扫描结果，这一点保持不变。不提供读取文件内容或列出服务器任意目录的接口。
- **HTTPS**：在配置里填了 `tls_cert` 和 `tls_key` 就走 HTTPS，最低 TLS 1.2，加密套件用 Go 默认的。证书续期后不重启服务也要生效，具体做法（检测文件变化或 SIGHUP）由实现者定。没配 TLS 时，登录页显示"连接未加密，建议只在局域网或 Tailscale 内使用，或者放到反向代理后面"。

### 6.5 默认监听（提案）

默认 `listen = :8099`，监听所有地址。理由：服务必须先设密码才能启动；典型用法是在局域网或 Tailscale 里直接访问。只想给本机或反向代理用的话，改成 `127.0.0.1:8099`。

## 7. 扫描范围（Linux）

### 7.1 自动发现（默认）

读取 `/proc/self/mountinfo`：

1. **只保留本地磁盘文件系统**：用白名单，例如 ext2/3/4、xfs、btrfs、zfs、f2fs、bcachefs、jfs、ntfs/ntfs3、exfat、vfat。最终清单由实现者定，原则是"有存储设备、用户会关心其空间的本地文件系统"。
   - 排除虚拟和内存文件系统：proc、sysfs、tmpfs、devtmpfs、overlay、squashfs、cgroup 等。
   - 排除网络文件系统：nfs、cifs、smb3、9p 等。
   - 排除所有 `fuse.*`。
2. **同一文件系统只算一次**：对 major:minor 相同的所有挂载，按 mountinfo 的 root 字段（被挂载的是文件系统里的哪个子目录）去重。
   - 如果一个挂载的 root 落在另一个挂载的 root 之下，丢掉它。这样可以去掉 bind mount，以及 `ProtectSystem=strict` 为 `ReadWritePaths` 生成的重挂载。
   - root 完全相同的，只保留挂载点路径最短的那个。
   - 例子：btrfs 的 `/`（root=`/@`）和 `/home`（root=`/@home`）谁也不在谁之下，两个都保留。`/srv` 如果是 `/mnt/data/srv` 的 bind mount，它的 root 落在 `/mnt/data` 那个挂载（root=`/`）之下，会被丢掉。
3. **每个保留下来的挂载点是一个"卷"**：名称就是挂载点路径。界面显示文件系统类型和设备（mountinfo 的 source 字段）。存储池为空。
   - 卷名本身含 `/`（比如 `/` 和 `/home`），而 QNAP 的卷名里没有。前端的面包屑和查重结果里"定位到所在位置"都要用卷名拼路径，要确认不会拼出 `//etc` 这类错误的路径。
4. **遍历规则不变**：不跨越 `st_dev` 边界。
   - 嵌套的挂载点（比如 `/` 下面的 `/home`）在 `/` 的目录树里显示为"挂载点（已跳过）"，同时 `/home` 作为独立的卷单独扫描，不会重复计算。从跳过条目跳到对应的卷，作为加分项，由实现者决定。
   - 没有单独挂载的 btrfs 子卷，`st_dev` 也不同，同样被跳过，与 `ncdu -x` 一致。这部分空间计入"未计入"。

### 7.2 配置文件

格式是 `key = value` 一行一项，`#` 开头为注释。自己写解析，不引入第三方库。遇到未知的键就报错退出，防止拼写错误被悄悄忽略。改完配置后执行 `systemctl restart storage-analyzer` 生效。

```
# /etc/storage-analyzer/config
listen = :8099
# tls_cert = /etc/storage-analyzer/cert.pem
# tls_key  = /etc/storage-analyzer/key.pem
# trusted_proxies = 127.0.0.1, ::1
# data_dir = /var/lib/storage-analyzer
# paths = auto                      默认自动发现；也可以列出要扫描的目录，逗号分隔
# exclude = /boot/efi, /mnt/backup  自动发现模式下要排除的挂载点
```

- **`paths` 显式列出时**：不再自动发现。
  - 每个路径必须是存在的目录，但不要求是挂载点。
  - 列出的路径不受白名单限制，用户明确写了网络文件系统也照样扫描。
  - 两个路径互相包含时，只扫外层，并写一条警告日志。
  - 路径不存在时，记入本次扫描的"跳过"列表，原因是 `not_found`。
- **`exclude`**：只在自动发现模式下起作用，只能排除整个挂载点。被排除的不出现在"跳过"列表里。

## 8. 定时（Linux）

- `serve` 进程按本地时区，每到整分钟醒来一次，读取设置。如果当前这一分钟正好是每天或每周设定的时间，就以子进程方式运行 `scan --trigger schedule`。
  - 这和 QNAP 上 crontab 那一行完全等价。"已有任务在跑就跳过整次"和"扫描完成后接着查重"仍由扫描进程自己处理，不需要改。
- 同一个"日期 + 设定时间"组合只触发一次，防止夏令时回拨或系统时钟回拨导致同一时刻触发两次。当天已经触发过之后，如果用户把时间改到当天稍晚的时刻，新时刻仍会触发。服务停机期间错过的不补跑，和 cron 一样。
- 在网页上保存设置后，下一分钟就按新设置执行，不需要任何系统操作。

## 9. 界面

- 两个平台共用一套 `web/`。平台差异由 API 告诉前端：401 响应里的 `auth: "password"`，以及 `status` 之类的响应里带上 `platform: "qnap" | "linux"`，具体放在哪个字段由实现者定。
- **Linux 版**：
  - 登录表单：密码框、登录按钮，以及密码错误、被锁定、连接未加密三种提示。
  - 工具栏有"退出登录"。
  - 文案里不提 QTS。
  - 卷列表显示挂载点、文件系统类型和设备。
- **QNAP 版**：界面完全不变。
- 语言检测沿用现有逻辑。Linux 上没有 QTS 的 cookie，会自然落到浏览器语言。所有新增文案都要有中英文。

登录页示意：

```
┌──────────────────────────────────┐
│        Storage Analyzer          │
│                                  │
│  密码  [••••••••••••        ]    │
│              [ 登录 ]            │
│                                  │
│  ⚠ 连接未加密，建议只在局域网 /     │
│    Tailscale 内使用或放到反代后面   │
└──────────────────────────────────┘
```

## 10. 打包与发布

- **版本号**：两个平台共用一个版本号（现在写在 `qpkg.cfg` 的 `QPKG_VER`）。实现时可以改为单独的 `VERSION` 文件，由构建脚本写入 `qpkg.cfg` 和 `-ldflags`。
- **每个 tag `vX.Y.Z` 的产物**：
  - `StorageAnalyzer_<ver>_{x86_64,arm_64,arm-x41}.qpkg`（不变）
  - `storage-analyzer_<ver>_linux_{amd64,arm64,armv7}.tar.gz`：内含二进制、systemd unit、示例配置、`install.sh`、`uninstall.sh`、简短 README
  - `.deb`（amd64、arm64、armhf）和 `.rpm`（x86_64、aarch64、armv7hl），用 nfpm 生成。nfpm 只在构建时用，不进代码依赖。
  - `SHA256SUMS`
- **deb/rpm 安装脚本**：
  - 安装时：创建系统用户 `storage-analyzer`，创建 `/etc/storage-analyzer`，装好 unit 并 `daemon-reload`。
  - **首次安装不自动启动**，因为还没设密码。安装后打印下一步要运行的两条命令：`sudo storage-analyzer passwd` 和 `sudo systemctl enable --now storage-analyzer`。
  - 升级时：如果服务正在运行，就重启它。
  - 卸载时：保留 `/etc/storage-analyzer` 和数据目录，purge 时才删除，按各发行版的惯例处理。
- **`install.sh`**（tar.gz 用）：做同样的事，装到 `/usr/local/bin` 和 `/etc/systemd/system`。
- **CI（GitHub Actions）**：
  - 每次 push 和 PR：`go vet`，`go test ./...` 和 `go test -tags qnap ./...` 都跑，再做多架构 Linux 测试（沿用 `scripts/test-linux.sh` 的做法，在 Actions 上用 QEMU 模拟）。
  - 推 tag 时：先跑测试；再在 Docker 里用 QDK qbuild 构建 qpkg（沿用 `build.sh` 的逻辑）；然后构建 Linux 包；最后创建 GitHub Release，上传全部产物。Release 说明怎么写由实现者定。
  - 本地的 `scripts/build.sh` 继续可用。

## 11. 仓库与文档

- GitHub 仓库从 `storage-analyzer-qnap` 改名为 `storage-analyzer`，由维护者在 GitHub 上操作。GitHub 会把旧地址自动重定向过去，包括 Release 下载链接和 git remote。
- Go module 路径改为 `github.com/FlintyLemming/storage-analyzer`，以后可以直接 `go install`。这是纯机械替换。**（提案）**
- README：
  - 开头改成通用介绍。
  - "安装"分 QNAP 和 Linux 两节。Linux 节写清楚：安装、设置密码、启动、访问地址、HTTPS（配证书，或者放到反向代理后面，附 Caddy 和 nginx 示例）、配置项、卸载。
  - 中英文两份同步更新。
- QPKG 的内部名 `StorageAnalyzer`、显示名和数据目录都不变。

## 12. 测试

**单元测试**

- **mountinfo 解析与选卷**：用真实样本做 fixture，覆盖：
  - Ubuntu ext4，带 `/boot/efi`
  - btrfs 子卷分别挂载（`/` 与 `/home`）
  - Proxmox，ZFS 上有多个 dataset
  - 装了 Docker 的主机（大量 overlay）
  - 有 NFS / SMB 挂载
  - 在 `ProtectSystem=strict` 下看到的 mountinfo
  - bind mount
  - 挂载点路径含空格（转义）
- **配置**：默认值、未知键报错、`paths` 嵌套去重、路径不存在、`exclude` 生效。
- **密码**：哈希与校验、PHC 格式、密码文件变化后生效、改密码后旧会话失效。
- **会话**：过期、退出登录、走不走 HTTPS 时 cookie 属性的区别。
- **限速**：失败 5 次后锁定；锁定期间不做哈希校验；配置了 `trusted_proxies` 时正确取到 IP；不可信对端带来的 `X-Forwarded-For` 被忽略。
- **HTTP**：未登录时除静态文件外全部返回 401；安全响应头齐全；超大请求体被拒；未知路径返回 404；写操作缺 `X-SA-Request` 被拒。
- **定时**：每天和每周的匹配；同一天只触发一次；改设置后下一分钟生效。
- **两种编译方式都跑**：`go test ./...` 和 `go test -tags qnap ./...`，QNAP 现有测试全部通过。

**集成测试**（在带 systemd 的真机或虚拟机上）

- 完整走一遍：装 deb → `passwd` → 启动 → 浏览器登录 → 扫描 → 查重 → 定时扫描 → 扫描进行中重启服务（扫描应被取消并正确标记）→ 升级包 → 卸载。
- 在沙盒里扫描的结果，必须和用 root 直接跑 `scan` 一致（文件数、总大小）。

**QNAP 回归**：在测试机 hf-qts 上覆盖安装新的 qpkg，确认历史和设置都保留，功能不变。

## 13. 留给实现者

- 包结构和文件拆分，平台钩子的具体接口形状。
- 本地文件系统白名单的最终清单。
- 会话存成单独文件还是 SQLite 表（不得改动 QNAP 上现有数据的格式）。
- 证书重载方式；HTTP 超时和大小限制的具体数值。
- systemd 沙盒选项的最终清单（遵守 §3 的原则，并在真机上验证）。
- 登录页、退出按钮和各种提示的样式与文案；`platform` 字段放在哪个响应里。
- CI workflow 怎么拆分，Release 说明怎么生成。
- 日志措辞，以及要不要附 fail2ban 示例。

## 14. 汇总：我替用户做的决定（提案）

1. 用 build tag 在编译时选平台，QNAP 和 Linux 各自的二进制只含自己的入口（§5.1）。
2. 首个版本不出 Docker 镜像，也不提供 OpenRC 等非 systemd 的服务脚本（§2）。
3. `exclude` 只能排除整个挂载点，不能排除挂载点里的子目录（§2、§7.2）。
4. 密码至少 12 个字符；哈希用 argon2id，参数 m=19 MiB、t=2、p=1（§6.1）。
5. 会话 30 天后过期，从登录时算起，使用不会延长（§6.2）。
6. 同一 IP 15 分钟内失败 5 次就锁定 15 分钟；全局每分钟最多校验 20 次；不做全局锁死（§6.3）。
7. 默认监听所有地址的 8099 端口（§6.5）。
8. 没有单独挂载的 btrfs 子卷按 `ncdu -x` 的做法跳过（§7.1）。
9. Go module 路径改为 `github.com/FlintyLemming/storage-analyzer`（§11）。
10. deb/rpm 首次安装后不自动启动，提示用户先设密码（§10）。
