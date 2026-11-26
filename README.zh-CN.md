# Storage Analyzer

[English](README.md) | **简体中文**

带网页界面的"图形版 ncdu"，支持 QNAP QTS 和普通 Linux 服务器。扫描磁盘，按目录逐层下钻，找出空间被谁占用；还能找出内容完全相同的文件和文件夹，并保存历史扫描结果。在 QNAP 上，它和资源监视器等内置程序一样，在 QTS 桌面里以窗口方式打开；在 Linux 上，它是一个 systemd 服务，用浏览器访问。

![全部卷：每个数据卷的容量，分为扫描到的文件、未计入的已用空间和可用空间](docs/images/volumes.png)

![进入卷内：文件夹按大小排序，显示占比、项数和修改时间](docs/images/browse.png)

## 功能

- **扫描所有数据卷**：在 QNAP 上自动识别所有存储池上的卷，跳过系统分区，以及未挂载的卷（例如未解锁的加密卷）；在 Linux 上自动识别本地磁盘文件系统，也可以自己指定要扫描的目录。
- **逐层下钻**：按大小、名称、项数或修改时间排序；可在"磁盘占用"（实际占用的磁盘空间）和"表观大小"（文件内容本身的大小）之间切换。
- **看清整个卷**：每个卷的容量条分为扫描到的文件、未计入任何文件的已用空间（快照、文件系统元数据等）和可用空间。
- **查找重复项**：找出内容完全相同的文件和文件夹，全部经过完整的 SHA-256 校验。两个文件夹整体相同时，只列出最大的那一层相同文件夹，不再逐个列出里面的内容。
- **定时或手动扫描**：随时手动扫描，也可以每天或每周定时扫描，并可在扫描完成后自动检查重复项。
- **扫描历史**：保留最近几次成功的扫描结果（默认 10 次），旧结果同样可以完整浏览。
- **不打扰系统**：扫描和重复检查以最低的 CPU 和 I/O 优先级运行。QNAP 版在两次任务之间没有任何后台进程；Linux 版是常驻服务，空闲时不占 CPU。
- **只读**：不会修改、移动或删除你的任何文件。
- **中英文界面**：QNAP 版跟随 QTS 的语言设置，Linux 版跟随浏览器语言。

## QNAP QTS

### 系统要求

- QTS 5.0 或更高版本
- 以下架构之一：
  - `x86_64`：Intel 和 AMD 机型
  - `arm_64`：64 位 ARM 机型
  - `arm-x41`：Annapurna Labs Alpine 处理器的 32 位 ARM 机型，如 TS-x31P、TS-x31X、TS-x31K
- QTS 管理员账号。应用能看到所有用户的文件名，因此只有管理员可以打开。

> [!NOTE]
> ARM 安装包已在模拟环境中编译并通过测试，但还没有在真实的 ARM NAS 上试过。如果你在 ARM 机型上使用，欢迎反馈。

### 安装

1. 从 [Releases](https://github.com/FlintyLemming/storage-analyzer/releases/latest) 下载与 NAS 架构对应的 `.qpkg` 文件。
2. 安装包没有 QNAP 的数字签名，需要先允许安装未签名的应用：打开 **App Center**，点 **设置**（齿轮图标）→ **常规**，勾选 **允许安装没有有效数字签名的应用程序**。
3. 在 App Center 中点 **手动安装**（齿轮旁边的图标），选择下载的文件并确认。
4. 从 QTS 主菜单打开 **Storage Analyzer**。

升级时按同样的方式安装新版本的 `.qpkg`，扫描历史和设置都会保留。

## Linux

以 systemd 服务方式运行，支持 `x86_64`、`arm64` 和 `armv7`。

### 安装

从 [Releases](https://github.com/FlintyLemming/storage-analyzer/releases/latest) 下载适合你系统的安装包。每个版本包含以下文件，以及一个 `SHA256SUMS` 校验文件：

- `storage-analyzer_<版本>_linux_{amd64,arm64,armv7}.tar.gz`
- `storage-analyzer_<版本>_{amd64,arm64,armhf}.deb`
- `storage-analyzer-<版本>-1.{x86_64,aarch64,armv7hl}.rpm`

1. 安装。Debian、Ubuntu 等：

   ```bash
   sudo apt install ./storage-analyzer_<版本>_amd64.deb
   ```

   Fedora、RHEL 等：

   ```bash
   sudo dnf install ./storage-analyzer-<版本>-1.x86_64.rpm
   ```

   或者解压 `.tar.gz`，运行里面的安装脚本，它会安装到 `/usr/local/bin` 和 `/etc/systemd/system`：

   ```bash
   sudo ./install.sh
   ```

   首次安装不会启动服务。

2. 设置密码。只有一个密码，没有用户名，至少 12 个字符，并且只能在服务器本机设置：

   ```bash
   sudo storage-analyzer passwd
   ```

   再次运行会修改密码，并让所有已登录的会话失效。

3. 启动服务并设为开机自启：

   ```bash
   sudo systemctl enable --now storage-analyzer
   ```

4. 用浏览器打开 `http://服务器:8099` 并登录。

服务以专用用户 `storage-analyzer` 运行，只带 `CAP_DAC_READ_SEARCH` 权限，所以能读取所有文件，但不能写入任何文件，并启用了 systemd 沙箱。数据保存在 `/var/lib/storage-analyzer`，配置文件是 `/etc/storage-analyzer/config`，密码哈希在 `/etc/storage-analyzer/password`，日志在 journal 中（`journalctl -u storage-analyzer`）。扫描和重复检查仍然以最低的 CPU 和 I/O 优先级、作为子进程运行。

### 扫描范围

默认根据挂载表自动识别本地磁盘文件系统（ext2/3/4、xfs、btrfs、zfs、f2fs、bcachefs、jfs、reiserfs、ntfs/ntfs3、exfat、vfat、fuseblk）。网络文件系统（NFS、SMB、sshfs 及其他 FUSE）、虚拟和内存文件系统、对已包含文件系统的 bind 挂载，以及 btrfs 的 `.snapshots` 都会跳过。设置 `paths` 则只扫描你列出的目录，列出的网络文件系统也会被扫描。自动模式下，`exclude` 可以排除整个挂载点。

### 配置

编辑 `/etc/storage-analyzer/config`，然后用 `sudo systemctl restart storage-analyzer` 重启服务。格式为每行一个 `key = value`；只支持整行的 `#` 注释（值后面不能跟注释），出现未知的键会报错。安装包里附带的文件是带注释的模板。

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `listen` | `:8099` | 监听的地址和端口 |
| `tls_cert`、`tls_key` | 未设置 | PEM 格式的证书和私钥路径，必须同时设置或同时不设置。证书续期后无需重启即可生效。 |
| `trusted_proxies` | 未设置 | 以逗号分隔的反向代理 IP 或 CIDR，信任它们发来的 `X-Forwarded-For` 和 `X-Forwarded-Proto` |
| `data_dir` | `/var/lib/storage-analyzer` | 扫描结果和状态的保存位置 |
| `paths` | `auto` | `auto`，或以逗号分隔的目录列表，如 `/a, /b` |
| `exclude` | 未设置 | 自动模式下要排除的挂载点，以逗号分隔，如 `/mnt/x` |

### HTTPS 与反向代理

不使用 HTTPS 时，登录页会提示连接未加密。要么设置 `tls_cert` 和 `tls_key` 直接提供 HTTPS，要么把服务放在反向代理后面。

放在代理后面时，只监听本机回环地址，并把代理的地址加入 `trusted_proxies`，这样才能拿到真实的客户端地址。否则所有用户看起来都来自代理的地址，会共用同一个登录锁定额度。

```ini
listen = 127.0.0.1:8099
trusted_proxies = 127.0.0.1
```

Caddy：

```
storage.example.com {
    reverse_proxy 127.0.0.1:8099
}
```

nginx：

```nginx
location / {
    proxy_pass http://127.0.0.1:8099;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

### 登录保护

同一地址 15 分钟内登录失败 5 次，该地址会被锁定 15 分钟。所有地址合计每分钟最多检查 20 次密码。失败的尝试会记录为 `login failed from <ip>`，可以配合 fail2ban 使用。登录状态（Cookie）有效期 30 天。

### 1024 以下的端口和自定义数据目录

服务默认不能绑定 1024 以下的端口。要使用这类端口，或者把 `data_dir` 换到别处，需要添加 systemd override：

```bash
sudo systemctl edit storage-analyzer
```

使用 1024 以下的端口：

```ini
[Service]
AmbientCapabilities=CAP_DAC_READ_SEARCH CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_NET_BIND_SERVICE
```

使用其他 `data_dir`：先让 `storage-analyzer` 用户对该目录有写权限，再放行：

```ini
[Service]
ReadWritePaths=/your/data/dir
```

然后重启服务。

### 卸载

- Debian、Ubuntu：`sudo apt remove storage-analyzer` 会保留配置和数据；`sudo apt purge storage-analyzer` 会一并删除。
- Fedora、RHEL：`sudo dnf remove storage-analyzer` 会保留数据。如需彻底清除，请手动删除 `/etc/storage-analyzer` 和 `/var/lib/storage-analyzer`。
- tar.gz 安装：运行 `sudo ./uninstall.sh`。加上 `--purge` 会一并删除配置和数据。

### 已知限制

- 没有单独挂载的 btrfs 子卷会被跳过，与 `ncdu -x` 一致。
- 暂时没有 Docker 镜像。
- 安装包只支持 systemd，不支持其他 init 系统。

## 使用

### 扫描

点 **新建扫描**。窗口顶部会显示进度，扫描期间仍可浏览之前的结果。根据文件数量，一次扫描通常需要几分钟到几十分钟。

用 **扫描结果** 下拉框在已保存的扫描之间切换。**历史** 列出每次扫描的时间、方式、状态、总占用、文件数和耗时，可以打开或删除。

### 浏览

顶层列出各个数据卷（Linux 上是各个被扫描的文件系统）。点文件夹进入，点面包屑路径或 **.. 上一级** 返回；点列标题按该列排序。

硬链接只计一次（其余链接会被标注），不跟随符号链接，也不会跨进其他挂载的文件系统。

### 重复项

切换到 **重复项**，点 **开始重复检查**。相同的文件和文件夹按组列出，从大到小排列，并显示删掉多余副本后可释放的空间。**↗ 查看位置** 会在空间占用视图中打开所在目录。

- 文件夹相同，指里面的文件名、子文件夹结构和每个文件的内容都一致；文件夹本身的名字可以不同。
- 空文件和硬链接不算重复。缩略图和系统文件（`@__thumb`、`.@__thumb`、`@eaDir`、`.DS_Store`、`Thumbs.db`、`._*`）会被忽略。
- 扫描后发生变化的文件不参与比较。
- 校验值会缓存下来。第一次检查要读所有候选文件，可能需要几个小时；之后只读新增或变化过的文件，通常几分钟到几十分钟。

Storage Analyzer 不会删除任何东西，不需要的文件请用 File Station 等工具自行删除。

### 设置

- **定时扫描**：关闭、每天或每周，时间按服务器本地时区。
- **扫描完成后检查重复项**：每次定时扫描完成后，接着对这次结果做重复检查。
- **保留结果**：保留最近几次成功的扫描结果（1–100），每份结果约占几十到几百 MB。
- **默认显示**：默认显示"磁盘占用"还是"表观大小"。

## 从源码构建

需要 Go 1.26 或更高版本，以及 Docker。QNAP 打包使用 QNAP 的 [QDK](https://github.com/qnap-dev/QDK)，在容器中运行。

```bash
./scripts/build.sh
```

它会先运行测试（包括在 Docker 中针对每个架构运行的 Linux 测试），然后交叉编译，生成 `x86_64`、`arm_64` 和 `arm-x41` 三个安装包：`build/StorageAnalyzer_<版本>_<架构>.qpkg`。Linux 安装包（`.tar.gz`、`.deb` 和 `.rpm`）用 `./scripts/build-linux.sh` 构建。

在本地开发界面时，先编译本机二进制，再启动开发服务器。它扫描 `SA_DEV_ROOTS` 中列出的目录而不是 NAS 的卷，并且跳过登录验证，因此只监听本机回环地址：

```bash
go build -o build/dev/bin/storage-analyzer ./cmd/storage-analyzer
```

```bash
SA_DEV_ROOTS=$HOME/Projects ./build/dev/bin/storage-analyzer serve --insecure-no-auth --root build/dev --web web
```

然后打开 <http://127.0.0.1:8099/>。

## 工作原理

整个应用是一个静态编译的 Go 程序。在 QNAP 上没有常驻进程：它作为 CGI 程序、通过 QTS 自带的 Web 服务器为界面提供接口，使用 QTS 管理员的登录状态，并在开始扫描或重复检查时被拉起到后台执行任务；定时扫描是 QTS crontab 中的一行。在 Linux 上它是常驻的 systemd 服务，有自己的登录，并把扫描和重复检查作为子进程启动。每次扫描单独保存为一个 SQLite 数据库，所以打开旧结果和打开最新结果一样快。在 QNAP 上，所有数据都保存在应用的安装目录中，卸载应用时一并删除。

## 许可证

MIT
