# Storage Analyzer

**English** | [简体中文](README.zh-CN.md)

An ncdu-style disk usage analyzer with a web interface, for QNAP QTS and for ordinary Linux servers. It scans your disks, lets you drill down folder by folder to see what is taking up space, finds files and folders with identical content, and keeps a history of past scans. On QNAP it opens as a window on the QTS desktop, like the built-in apps; on Linux it runs as a systemd service you open in the browser.

![All volumes: capacity of each data volume, split into scanned files, space not accounted for, and free space](docs/images/volumes.png)

![Inside a volume: folders sorted by size, with share of the parent, item count and last modified time](docs/images/browse.png)

## Features

- **Scans every data volume.** On QNAP, volumes on all storage pools are found automatically; system partitions are skipped, and so are volumes that aren't mounted, such as locked encrypted volumes. On Linux, local disk filesystems are discovered automatically, or you can list the directories to scan.
- **Folder-by-folder drill-down.** Sort by size, name, item count or modified time. Switch between *Disk usage* (space taken on disk) and *Apparent size* (size of the file contents).
- **Accounts for the whole volume.** Each volume's bar shows the scanned files, space that isn't accounted for by any file (snapshots, file system metadata, etc.) and free space.
- **Duplicate finder.** Finds files and folders with identical content, verified with a full SHA-256 hash. When two whole folders are identical, only the largest identical folder is listed, not everything inside it.
- **Scheduled or on-demand scans.** Run a scan by hand, or daily or weekly at a set time, optionally followed by a duplicate check.
- **Scan history.** Keeps the most recent successful scans (10 by default), so you can open an older scan and browse it in full.
- **Stays out of the way.** Scans and duplicate checks run at the lowest CPU and I/O priority. The QNAP version has no background process between them; the Linux version is a resident service that uses no CPU while idle.
- **Read-only.** It never changes, moves or deletes your files.
- **English and Chinese UI**, following the QTS language on QNAP and the browser language on Linux.

## QNAP QTS

### Requirements

- QTS 5.0 or later
- One of these architectures:
  - `x86_64`: Intel and AMD models
  - `arm_64`: 64-bit ARM models
  - `arm-x41`: 32-bit ARM models with an Annapurna Labs Alpine CPU, such as TS-x31P, TS-x31X and TS-x31K
- A QTS administrator account. The app shows the names of every user's files, so only administrators can open it.

> [!NOTE]
> The ARM packages are built and tested under emulation but haven't been tried on a real ARM NAS yet. If you run one, feedback is welcome.

### Installation

1. Download the `.qpkg` for your NAS's architecture from [Releases](https://github.com/FlintyLemming/storage-analyzer/releases/latest).
2. The package isn't signed by QNAP, so allow unsigned apps first: in **App Center**, open **Settings** (gear icon) → **General** and turn on **Allow installation of applications without a valid digital signature**.
3. In App Center, click **Install Manually** (the icon next to the gear), choose the downloaded file and confirm.
4. Open **Storage Analyzer** from the QTS main menu.

To update, install the newer `.qpkg` the same way. Your scan history and settings are kept.

## Linux

Runs as a systemd service on `x86_64`, `arm64` and `armv7`.

### Installation

Download the package for your system from [Releases](https://github.com/FlintyLemming/storage-analyzer/releases/latest). Each release has these files, plus a `SHA256SUMS` file:

- `storage-analyzer_<version>_linux_{amd64,arm64,armv7}.tar.gz`
- `storage-analyzer_<version>_{amd64,arm64,armhf}.deb`
- `storage-analyzer-<version>-1.{x86_64,aarch64,armv7hl}.rpm`

1. Install it. On Debian, Ubuntu and similar:

   ```bash
   sudo apt install ./storage-analyzer_<version>_amd64.deb
   ```

   On Fedora, RHEL and similar:

   ```bash
   sudo dnf install ./storage-analyzer-<version>-1.x86_64.rpm
   ```

   Or extract the `.tar.gz` and run its installer, which installs to `/usr/local/bin` and `/etc/systemd/system`:

   ```bash
   sudo ./install.sh
   ```

   The first install doesn't start the service.

2. Set the password. There is a single password and no username, at least 12 characters, and it can only be set on the server itself:

   ```bash
   sudo storage-analyzer passwd
   ```

   Running it again changes the password and signs out every session.

3. Start the service and enable it at boot:

   ```bash
   sudo systemctl enable --now storage-analyzer
   ```

4. Open `http://SERVER:8099` in a browser and sign in.

The service runs as the dedicated user `storage-analyzer` with only the `CAP_DAC_READ_SEARCH` capability, so it can read every file but can't write any, and systemd sandboxing is on. Data is kept in `/var/lib/storage-analyzer`, the configuration is `/etc/storage-analyzer/config`, the password hash is `/etc/storage-analyzer/password`, and logs go to the journal (`journalctl -u storage-analyzer`). Scans and duplicate checks still run at the lowest CPU and I/O priority, as child processes.

### What gets scanned

By default it discovers local disk filesystems from the mount table (ext2/3/4, xfs, btrfs, zfs, f2fs, bcachefs, jfs, reiserfs, ntfs/ntfs3, exfat, vfat, fuseblk). Network filesystems (NFS, SMB, sshfs and other FUSE), virtual and memory filesystems, bind mounts of a filesystem that is already included, and btrfs `.snapshots` are skipped. Set `paths` to scan exactly the directories you list instead; network filesystems are allowed when you list them. In automatic mode, `exclude` drops whole mount points.

### Configuration

Edit `/etc/storage-analyzer/config`, then restart the service with `sudo systemctl restart storage-analyzer`. The format is `key = value`, one per line. Only whole-line `#` comments are allowed (no comments after a value), and unknown keys are errors. The file shipped in the package is a commented template.

| Key | Default | Meaning |
| --- | --- | --- |
| `listen` | `:8099` | Address and port to listen on |
| `tls_cert`, `tls_key` | not set | Paths to a PEM certificate and key. Set both or neither. A renewed certificate is picked up without a restart. |
| `trusted_proxies` | not set | Comma-separated IPs or CIDRs of reverse proxies whose `X-Forwarded-For` and `X-Forwarded-Proto` headers are trusted |
| `data_dir` | `/var/lib/storage-analyzer` | Where scan results and state are stored |
| `paths` | `auto` | `auto`, or a comma-separated list of directories, such as `/a, /b` |
| `exclude` | not set | Comma-separated mount points to leave out in auto mode, such as `/mnt/x` |

### HTTPS and reverse proxy

Without HTTPS, the sign-in page warns that the connection isn't encrypted. Either serve HTTPS directly by setting `tls_cert` and `tls_key`, or put the service behind a reverse proxy.

When using a proxy, listen on loopback only and add the proxy's address to `trusted_proxies`, so the real client address is used. Otherwise every user appears as the proxy's address and they all share one sign-in lockout.

```ini
listen = 127.0.0.1:8099
trusted_proxies = 127.0.0.1
```

Caddy:

```caddy
storage.example.com {
    reverse_proxy 127.0.0.1:8099
}
```

nginx:

```nginx
location / {
    proxy_pass http://127.0.0.1:8099;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

### Sign-in protection

After 5 failed sign-in attempts from one address within 15 minutes, that address is locked out for 15 minutes. Across all addresses, at most 20 password checks are made per minute. Failed attempts are logged as `login failed from <ip>`, which fail2ban can match. A session cookie lasts 30 days.

### Ports below 1024 and a custom data directory

The service can't bind ports below 1024 by default. To use one, or to move `data_dir` elsewhere, add a systemd override:

```bash
sudo systemctl edit storage-analyzer
```

For a port below 1024:

```ini
[Service]
AmbientCapabilities=CAP_DAC_READ_SEARCH CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_NET_BIND_SERVICE
```

For a different `data_dir`, make the directory writable by the `storage-analyzer` user and allow it:

```ini
[Service]
ReadWritePaths=/your/data/dir
```

Then restart the service.

### Uninstalling

- Debian and Ubuntu: `sudo apt remove storage-analyzer` keeps the configuration and data; `sudo apt purge storage-analyzer` removes them too.
- Fedora and RHEL: `sudo dnf remove storage-analyzer` keeps the data. Delete `/etc/storage-analyzer` and `/var/lib/storage-analyzer` by hand if you want them gone.
- Tarball install: `sudo ./uninstall.sh`. Add `--purge` to also delete the configuration and data.

### Known limitations

- btrfs subvolumes that aren't mounted separately are skipped, like `ncdu -x`.
- There is no Docker image yet.
- The packages only support systemd; other init systems aren't supported.

## Usage

### Scanning

Click **New scan**. Progress is shown at the top of the window, and you can keep browsing older results while it runs. A scan usually takes a few minutes to tens of minutes, depending on how many files you have.

Use the **Scan** picker to switch between saved scans. **History** lists every scan with its time, trigger, status, total size, file count and duration, and lets you open or delete one.

### Browsing

The top level shows each data volume (on Linux, each scanned filesystem). Click a folder to open it, and use the breadcrumbs or **.. Up one level** to go back. Click a column header to sort by it.

Hard links are counted only once (the other links are tagged), symbolic links aren't followed, and scans don't cross into other mounted file systems.

### Duplicates

Switch to **Duplicates** and click **Start duplicate check**. Groups of identical files and folders are listed largest first, with how much space removing the extra copies would free up. **↗ Show location** opens the folder in the Space view.

- Folders count as identical when their file names, subfolder structure and file contents all match; the folders' own names may differ.
- Empty files and hard links aren't reported as duplicates. Thumbnail and system files (`@__thumb`, `.@__thumb`, `@eaDir`, `.DS_Store`, `Thumbs.db`, `._*`) are ignored.
- Files that changed after the scan are left out of the comparison.
- Hashes are cached between checks. The first check reads every candidate file and can take a few hours; later checks only read new or changed files and usually take minutes to an hour.

Storage Analyzer doesn't delete anything. Use File Station (on QNAP) or your usual tools to remove what you don't need.

### Settings

- **Scheduled scan:** off, daily or weekly, at a time in the server's local time zone.
- **Check for duplicates after the scan:** runs a duplicate check on each scheduled scan once it finishes.
- **Keep results:** how many recent successful scans to keep (1–100). Each result takes tens to hundreds of MB.
- **Default size:** whether to show *Disk usage* or *Apparent size* by default.

## Building from source

You need Go 1.26 or later and Docker. QNAP packaging uses QNAP's [QDK](https://github.com/qnap-dev/QDK), which runs in a container.

```bash
./scripts/build.sh
```

This runs the tests (including the Linux tests for every architecture, in Docker), cross-compiles the binary and writes `build/StorageAnalyzer_<version>_<arch>.qpkg` for `x86_64`, `arm_64` and `arm-x41`. For the Linux packages (`.tar.gz`, `.deb` and `.rpm`), run `./scripts/build-linux.sh`.

To work on the UI locally, build a native binary, write a dev config file pointing at the folder you want to scan, and start the development server (it skips authentication and only listens on loopback):

```bash
mkdir -p build/dev
printf 'data_dir = %s/build/dev/data\npaths = %s/Projects\n' "$PWD" "$HOME" > build/dev/config
go build -o build/dev/bin/storage-analyzer ./cmd/storage-analyzer
./build/dev/bin/storage-analyzer serve --config build/dev/config --insecure-no-auth --web web
```

Then open <http://127.0.0.1:8099/>.

## How it works

A single static Go binary serves everything. On QNAP there's no resident process: it runs as a CGI program behind QTS's own web server, using the QTS administrator session, and is launched in the background when a scan or duplicate check starts. On Linux it runs as a resident systemd service with its own sign-in, and starts scans and duplicate checks as child processes. Scheduled scans are a line in the QTS crontab on QNAP. Each scan is saved as its own SQLite database, so opening an old scan is as fast as opening the latest. On QNAP, all data lives in the app's install folder and is removed when the app is uninstalled.

## License

MIT
