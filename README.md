# DiskTrend Web - Linux 存储分析器

一个功能强大的 Linux 磁盘存储分析工具，提供 Web Dashboard 用于查看文件夹大小变化趋势。

## 功能特性

- **深度扫描**：递归统计所有层级的文件夹和文件，精确到字节
- **定时任务**：内置调度器，支持每日定时扫描
- **趋势分析**：查看任意路径在过去 7/30/90 天的大小变化
- **异常告警**：自动高亮增长最快的目录
- **文件浏览器**：树状结构下钻查看子目录大小
- **实时更新**：WebSocket 推送扫描进度

## 快速开始

### 1. 安装 uv (如未安装)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 安装依赖

```bash
cd storage-analyzer
uv sync
```

### 3. 配置 (可选)

编辑 `config.yaml` 配置扫描范围和定时任务：

```yaml
scanner:
  mount_points:
    - "/"
  skip_paths:
    - "/proc"
    - "/sys"
    # ...

scheduler:
  enabled: true
  scan_time: "03:00"
  timezone: "Asia/Shanghai"
```

### 4. 运行

**普通用户运行** (部分目录可能无权限访问)：

```bash
uv run disktrend
```

**以 root 权限运行** (完整系统扫描)：

```bash
sudo -E $(which uv) run disktrend
```

或者使用 Python 直接运行：

```bash
sudo -E $(which uv) run python -m disktrend.server
```

### 5. 访问 Dashboard

打开浏览器访问：http://localhost:8080

## 项目结构

```
storage-analyzer/
├── config.yaml              # 配置文件
├── pyproject.toml           # Python 项目配置
├── src/
│   └── disktrend/
│       ├── __init__.py
│       ├── models.py        # SQLite 数据模型
│       ├── scanner.py       # 磁盘扫描引擎
│       ├── scheduler.py     # 定时任务调度
│       └── server.py        # FastAPI Web 服务
├── templates/
│   └── index.html           # Web Dashboard (ECharts)
└── data/
    └── disktrend.db         # SQLite 数据库
```

## API 接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/status` | GET | 获取系统状态 |
| `/api/overview` | GET | 获取 Dashboard 概览数据 |
| `/api/snapshots` | GET | 获取扫描历史 |
| `/api/browse?path=/` | GET | 浏览目录 |
| `/api/history?path=/&days=30` | GET | 获取路径历史趋势 |
| `/api/growth?limit=10` | GET | 获取增长最快的目录 |
| `/api/scan` | POST | 触发手动扫描 |
| `/ws` | WebSocket | 实时状态推送 |

## 配置说明

### config.yaml 完整示例

```yaml
# Web 服务器配置
server:
  host: "0.0.0.0"
  port: 8080

# 数据库配置
database:
  path: "./data/disktrend.db"

# 扫描配置
scanner:
  # 要扫描的挂载点
  mount_points:
    - "/"

  # 跳过的路径 (虚拟文件系统等)
  skip_paths:
    - "/proc"
    - "/sys"
    - "/dev"
    - "/run"
    - "/snap"
    - "/var/snap"
    - "/tmp"
    - "/var/tmp"
    - "/mnt"
    - "/media"
    - "/lost+found"

  # 最大扫描深度 (0 = 无限制)
  max_depth: 0

# 调度器配置
scheduler:
  enabled: true
  scan_time: "03:00"  # 每日扫描时间 (24小时制)
  timezone: "Asia/Shanghai"

# 数据保留配置
retention:
  days: 365  # 保留快照天数 (0 = 永久保留)
```

### 环境变量

- `DISKTREND_CONFIG`: 指定配置文件路径 (默认: `config.yaml`)

## Systemd 服务部署

创建 `/etc/systemd/system/disktrend.service`:

```ini
[Unit]
Description=DiskTrend Web Storage Analyzer
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/disktrend
ExecStart=/root/.local/bin/uv run disktrend
Restart=always
RestartSec=10
Environment=DISKTREND_CONFIG=/opt/disktrend/config.yaml

[Install]
WantedBy=multi-user.target
```

启用服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable disktrend
sudo systemctl start disktrend
```

## 数据库结构

### snapshots 表
记录每次扫描会话：
- `id`: 快照 ID
- `mount_point`: 挂载点
- `started_at`: 开始时间
- `completed_at`: 完成时间
- `total_size`: 总大小
- `total_files`: 文件数
- `total_dirs`: 目录数
- `status`: 状态 (running/completed/failed)

### entries 表
记录目录条目：
- `snapshot_id`: 关联快照
- `path`: 完整路径
- `name`: 目录/文件名
- `size`: 大小 (字节)
- `file_count`: 包含文件数
- `dir_count`: 包含目录数
- `depth`: 层级深度
- `parent_path`: 父目录路径

### scan_errors 表
记录扫描错误：
- `snapshot_id`: 关联快照
- `path`: 错误路径
- `error_type`: 错误类型
- `error_message`: 错误信息

## 许可证

MIT License
