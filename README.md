# Storage Analyzer for QNAP

**English** | [简体中文](README.zh-CN.md)

An ncdu-style disk usage analyzer for QNAP QTS. It scans the data volumes of every storage pool, lets you drill down folder by folder to see what is taking up space, finds files and folders with identical content, and keeps a history of past scans. It opens as a window on the QTS desktop, like the built-in apps.

![All volumes: capacity of each data volume, split into scanned files, space not accounted for, and free space](docs/images/volumes.png)

![Inside a volume: folders sorted by size, with share of the parent, item count and last modified time](docs/images/browse.png)

## Features

- **Scans every data volume.** Volumes on all storage pools are found automatically. System partitions are skipped, and so are volumes that aren't mounted, such as locked encrypted volumes.
- **Folder-by-folder drill-down.** Sort by size, name, item count or modified time. Switch between *Disk usage* (space taken on disk) and *Apparent size* (size of the file contents).
- **Accounts for the whole volume.** Each volume's bar shows the scanned files, space that isn't accounted for by any file (snapshots, file system metadata, etc.) and free space.
- **Duplicate finder.** Finds files and folders with identical content, verified with a full SHA-256 hash. When two whole folders are identical, only the largest identical folder is listed, not everything inside it.
- **Scheduled or on-demand scans.** Run a scan by hand, or daily or weekly at a set time, optionally followed by a duplicate check.
- **Scan history.** Keeps the most recent successful scans (10 by default), so you can open an older scan and browse it in full.
- **Stays out of the way.** Scans and duplicate checks run at the lowest CPU and I/O priority, and nothing runs in the background between them.
- **Read-only.** It never changes, moves or deletes your files.
- **English and Chinese UI**, following the QTS language.

## Requirements

- QTS 5.0 or later
- One of these architectures:
  - `x86_64`: Intel and AMD models
  - `arm_64`: 64-bit ARM models
  - `arm-x41`: 32-bit ARM models with an Annapurna Labs Alpine CPU, such as TS-x31P, TS-x31X and TS-x31K
- A QTS administrator account. The app shows the names of every user's files, so only administrators can open it.

> [!NOTE]
> The ARM packages are built and tested under emulation but haven't been tried on a real ARM NAS yet. If you run one, feedback is welcome.

## Installation

1. Download the `.qpkg` for your NAS's architecture from [Releases](https://github.com/FlintyLemming/storage-analyzer-qnap/releases/latest).
2. The package isn't signed by QNAP, so allow unsigned apps first: in **App Center**, open **Settings** (gear icon) → **General** and turn on **Allow installation of applications without a valid digital signature**.
3. In App Center, click **Install Manually** (the icon next to the gear), choose the downloaded file and confirm.
4. Open **Storage Analyzer** from the QTS main menu.

To update, install the newer `.qpkg` the same way. Your scan history and settings are kept.

## Usage

### Scanning

Click **New scan**. Progress is shown at the top of the window, and you can keep browsing older results while it runs. A scan usually takes a few minutes to tens of minutes, depending on how many files you have.

Use the **Scan** picker to switch between saved scans. **History** lists every scan with its time, trigger, status, total size, file count and duration, and lets you open or delete one.

### Browsing

The top level shows each data volume. Click a folder to open it, and use the breadcrumbs or **.. Up one level** to go back. Click a column header to sort by it.

Hard links are counted only once (the other links are tagged), symbolic links aren't followed, and scans don't cross into other mounted file systems.

### Duplicates

Switch to **Duplicates** and click **Start duplicate check**. Groups of identical files and folders are listed largest first, with how much space removing the extra copies would free up. **↗ Show location** opens the folder in the Space view.

- Folders count as identical when their file names, subfolder structure and file contents all match; the folders' own names may differ.
- Empty files and hard links aren't reported as duplicates. Thumbnail and system files (`@__thumb`, `.@__thumb`, `@eaDir`, `.DS_Store`, `Thumbs.db`, `._*`) are ignored.
- Files that changed after the scan are left out of the comparison.
- Hashes are cached between checks. The first check reads every candidate file and can take a few hours; later checks only read new or changed files and usually take minutes to an hour.

Storage Analyzer doesn't delete anything. Use File Station or your usual tools to remove what you don't need.

### Settings

- **Scheduled scan:** off, daily or weekly, at a time in the NAS's local time zone.
- **Check for duplicates after the scan:** runs a duplicate check on each scheduled scan once it finishes.
- **Keep results:** how many recent successful scans to keep (1–100). Each result takes tens to hundreds of MB.
- **Default size:** whether to show *Disk usage* or *Apparent size* by default.

## Building from source

You need Go 1.26 or later and Docker. Packaging uses QNAP's [QDK](https://github.com/qnap-dev/QDK), which runs in a container.

```bash
./scripts/build.sh
```

This runs the tests (including the Linux tests for every architecture, in Docker), cross-compiles the binary and writes `build/StorageAnalyzer_<version>_<arch>.qpkg` for `x86_64`, `arm_64` and `arm-x41`.

To work on the UI locally, build a native binary and start the development server. It scans the folders listed in `SA_DEV_ROOTS` instead of NAS volumes and skips authentication, so it only listens on loopback:

```bash
go build -o build/dev/bin/storage-analyzer ./cmd/storage-analyzer
```

```bash
SA_DEV_ROOTS=$HOME/Projects ./build/dev/bin/storage-analyzer serve --insecure-no-auth --root build/dev --web web
```

Then open <http://127.0.0.1:8099/>.

## How it works

There's no resident process. A single static Go binary serves the web UI's API as a CGI program behind QTS's own web server, and is launched in the background when a scan or duplicate check starts. Scheduled scans are a line in the QTS crontab. Each scan is saved as its own SQLite database, so opening an old scan is as fast as opening the latest. All data lives in the app's install folder and is removed when the app is uninstalled.

## License

MIT
