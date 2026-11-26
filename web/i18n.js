// UI language: picks zh or en from the QTS environment and holds the strings.
//
// Language is taken from, in order:
//   1. ?lang=zh|en in the page URL (manual override, handy for testing);
//   2. the nas_lang cookie QTS sets when a language is picked on the desktop
//      (SCH / TCH / ENG / JPN …; missing or "auto" means "follow the browser");
//   3. localStorage QTS_LANG_KEY, the language QTS last loaded its own strings
//      in, which also covers "auto";
//   4. the browser's preferred languages.
// Chinese QTS languages (simplified and traditional) map to zh, every other
// QTS language to en.
(() => {
  'use strict';

  function cookie(name) {
    const m = document.cookie.match(new RegExp('(?:^|;\\s*)' + name + '=([^;]*)'));
    if (!m) return '';
    try { return decodeURIComponent(m[1]); } catch { return m[1]; }
  }

  function fromQts(code) {
    code = String(code || '').trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(code)) return null; // empty, "AUTO" or junk
    return code === 'SCH' || code === 'TCH' ? 'zh' : 'en';
  }

  function fromTag(tag) {
    tag = String(tag || '').trim().toLowerCase();
    if (tag === 'zh' || tag.startsWith('zh-') || tag.startsWith('zh_')) return 'zh';
    if (tag === 'en' || tag.startsWith('en-') || tag.startsWith('en_')) return 'en';
    return null;
  }

  function detect() {
    try {
      const q = fromTag(new URLSearchParams(location.search).get('lang'));
      if (q) return q;
    } catch { /* ignore */ }
    const c = fromQts(cookie('nas_lang'));
    if (c) return c;
    try {
      const s = fromQts(localStorage.getItem('QTS_LANG_KEY'));
      if (s) return s;
    } catch { /* storage blocked */ }
    for (const tag of navigator.languages || [navigator.language]) {
      const l = fromTag(tag);
      if (l) return l;
    }
    return 'en';
  }

  const lang = detect();
  const locale = lang === 'zh' ? 'zh-CN' : 'en-US';
  const num = (n) => (n || 0).toLocaleString(locale);
  // English count with the right noun form: pl(1, 'file', 'files') → "1 file".
  const pl = (n, one, many) => `${num(n)} ${n === 1 ? one : many}`;

  const zh = {
    'app.title': 'Storage Analyzer',

    // toolbar & static page
    'tb.scan': '扫描结果',
    'tb.scan.title': '选择要查看的扫描结果',
    'tb.newScan': '新建扫描',
    'tb.history': '历史',
    'tb.settings': '设置',
    'tb.logout': '退出登录',
    'tb.view': '视图',
    'tb.sizeMode': '大小显示方式',
    'view.space': '空间占用',
    'view.space.title': '按目录查看空间占用',
    'view.dups': '重复项',
    'view.dups.title': '内容完全相同的文件和文件夹',
    'size.disk': '磁盘占用',
    'size.disk.title': '文件实际占用的磁盘空间',
    'size.apparent': '表观大小',
    'size.apparent.title': '文件内容本身的大小',
    'col.name': '名称',
    'col.size': '大小',
    'col.pct': '占比',
    'col.items': '项数',
    'col.mtime': '修改时间',
    'col.copies': '份数',
    'col.sizeEach': '单份大小',
    'col.reclaim': '可释放',
    'dlg.close': '关闭',

    // shared bits
    'trigger.schedule': '定时',
    'trigger.manual': '手动',
    'status.done': '完成',
    'status.failed': '失败',
    'status.canceled': '已取消',
    'status.running': '进行中',
    'dur.h': '{h} 小时 {m} 分',
    'dur.m': '{m} 分 {s} 秒',
    'dur.s': '{s} 秒',
    'elapsed': '用时 {dur}',
    'weekdays': ['周日', '周一', '周二', '周三', '周四', '周五', '周六'],
    'unknownError': '未知错误',
    'opFailed': '操作失败',
    'back': '返回',
    'cancel': '取消',
    'list.empty': '空目录',
    'list.more': '加载更多（剩余 {n} 项）',

    // API errors, by error code (server message is the fallback)
    'net.down': '无法连接到 NAS',
    'net.badResponse': '服务返回了无法识别的内容',
    'err.not_logged_in': '请先登录 QTS',
    'err.not_admin': '需要管理员权限',
    'err.auth_unavailable': '无法验证 QTS 登录状态：{detail}',
    'err.task_running': '已有任务正在进行',
    'err.not_running': '没有正在进行的扫描',
    'err.scan_running': '该扫描正在进行，请先取消',
    'err.scan_not_found': '扫描结果不存在或已被删除',
    'err.no_scans': '还没有扫描结果',
    'err.dir_not_found': '目录不存在',
    'err.invalid_settings': '设置无效：{detail}',
    'err.internal': '内部错误：{detail}',
    'err.bad_password': '密码不正确',
    'err.too_many_attempts': '尝试次数过多，请 {min} 分钟后再试',

    // scan picker & navigation
    'scans.none': '暂无扫描结果',
    'notice.scanDeleted': '该扫描结果已被删除，已切换到最新结果',
    'notice.dirGone': '目录不存在，已返回顶层',
    'crumbs.all': '全部卷',
    'crumbs.summary': '本目录 {size} · {n} 项',
    'row.up': '.. 上一级',

    // volume cards
    'vol.poolMount': '存储池 {pool} · {mount}',
    'vol.fsDev': '{fstype} · {device}',
    'vol.cap': '已用 {used} / {total}（{pct}%）',
    'vol.bar.title': '蓝色：扫描到的文件；橙色：未计入的已用空间；灰色：可用空间',
    'vol.scanned': '扫描到的文件 {size}',
    'vol.unacc': '未计入（快照 / 元数据等）{size}',
    'vol.unacc.title': '快照、文件系统元数据、扫描期间的变化等',
    'vol.free': '可用 {size}',
    'skip.not_mounted': '未挂载（可能是未解锁的加密卷）',
    'skip.not_share_volume': '不是数据卷',
    'skip.not_found': '路径不存在',
    'skip.not_directory': '不是目录',
    'skip.read_error': '读取出错',
    'skip.item': '{name}（{why}）',
    'skip.list': '已跳过：{list}',
    'skip.sep': '，',

    // row tags
    'tag.mount': '挂载点，已跳过',
    'tag.symlink': '符号链接',
    'tag.hardlink': '硬链接（已在别处计入）',
    'tag.hardlink.title': '同一个文件的另一个硬链接已经计入大小',
    'tag.readError': '读取出错',
    'tag.badName': '名称编码异常',
    'tag.badName.title': '文件名不是有效的 UTF-8，无法显示的字符已替换为 �',

    // empty / error states
    'empty.running.h': '正在进行第一次扫描',
    'empty.running.p': '扫描完成后会自动显示结果。扫描以最低优先级运行，不会影响 NAS 的正常使用。',
    'empty.h': '还没有扫描结果',
    'empty.p': '扫描会统计所有存储池上数据卷的空间占用，文件较多时需要几分钟到几十分钟。',
    'empty.start': '开始第一次扫描',
    'auth.h': '请先登录 QTS',
    'auth.p': '登录已过期，请刷新 QTS 页面后重新打开 Storage Analyzer。',
    'admin.h': '需要管理员权限',
    'admin.p': 'Storage Analyzer 可以看到所有用户的文件名，只有管理员可以使用。',
    'login.h': '登录',
    'login.password': '密码',
    'login.submit': '登录',
    'login.insecure': '连接未加密，建议只在局域网或 Tailscale 内使用，或者放到反向代理后面。',

    // scanning
    'scan.started': '扫描已开始',
    'scan.done': '扫描完成：{size}，{files} 个文件，用时 {dur}',
    'scan.canceled': '扫描已取消',
    'scan.failed': '扫描失败：{msg}',
    'scan.cancel.h': '取消扫描',
    'scan.cancel.p': '确定要取消当前扫描吗？已扫描的部分不会保存。',
    'scan.cancel.ok': '取消扫描',
    'scan.canceling': '正在取消扫描…',
    'prog.scanning': '正在扫描…',
    'prog.scanningPct': '正在扫描…约 {pct}%',
    'prog.scanned': '已扫描 {files} 个文件、{dirs} 个目录',
    'prog.errors': '{n} 个读取错误',
    'prog.scheduled': '定时任务',
    'prog.scheduledThenDup': '定时任务，完成后检查重复项',

    // history
    'hist.title': '扫描历史',
    'hist.cols': ['开始时间', '方式', '状态', '总占用', '文件数', '耗时', '结果大小', '重复检查', ''],
    'hist.none': '还没有扫描记录。',
    'hist.keep': '自动保留最近 {n} 次成功的扫描结果，可在设置中修改。',
    'hist.open': '打开',
    'hist.delete': '删除',
    'hist.dupReclaim': '可释放 {size}',
    'hist.dupNone': '无重复',
    'del.h': '删除扫描结果',
    'del.p': '确定删除 {time} 的扫描记录吗？此操作只删除扫描结果，不会影响 NAS 上的任何文件。',
    'del.ok': '删除',
    'del.done': '已删除',

    // settings
    'set.title': '设置',
    'set.schedule': '定时扫描',
    'set.off': '关闭',
    'set.daily': '每天',
    'set.weekly': '每周',
    'set.weekday': '星期',
    'set.time': '时间',
    'set.timeHint': '按 NAS 本地时间执行，建议选择夜间空闲时段。',
    'set.dup': '重复检查',
    'set.dupLabel': '扫描完成后检查重复项',
    'set.dupHint': '首次检查可能要几个小时，之后通常几分钟到几十分钟；期间 NAS 硬盘会比较忙。',
    'set.keep': '保留结果',
    'set.keepUnit': '次',
    'set.keepHint': '保留最近几次成功的扫描结果（1–100），每份结果约占几十到几百 MB。',
    'set.keepRange': '保留次数需要在 1–100 之间',
    'set.defaultSize': '默认显示',
    'set.save': '保存',
    'set.saveFailed': '保存失败',
    'set.savedOff': '设置已保存，定时扫描已关闭',
    'set.savedOn': (p) => `设置已保存：${p.daily ? '每天' : p.weekday} ${p.time} 自动扫描${p.dup ? '并检查重复项' : ''}`,

    // duplicates
    'phase.prepare': '核对文件',
    'phase.quick': '快速比对',
    'phase.full': '完整校验',
    'phase.rollup': '汇总文件夹',
    'phase.write': '保存结果',
    'phase.default': '准备中',
    'dup.started': '重复检查已开始',
    'dup.busyStart': '已有任务正在进行，请等它结束后再检查',
    'dup.busyTitle': '已有扫描或重复检查正在进行',
    'dup.busyNote': '已有扫描或重复检查正在进行，结束后才能开始。',
    'dup.done': '重复检查完成：{groups} 组，可释放 {size}',
    'dup.doneNone': '重复检查完成：没有发现重复项',
    'dup.canceled': '重复检查已取消',
    'dup.failed': '重复检查失败：{msg}',
    'dup.cancel.h': '取消重复检查',
    'dup.cancel.p': '确定要取消重复检查吗？已经算过的文件会保留在缓存里，下次检查不用重读。',
    'dup.cancel.ok': '取消检查',
    'dup.canceling': '正在取消重复检查…',
    'dup.runningHere': (p) => '正在检查重复项，进度见上方。' + (p.prev ? '下面是上一次的结果。' : ''),
    'dup.lastCanceled': (p) => '上次检查已取消，已经算过的文件下次不会重读。' + (p.prev ? '下面是更早一次的结果。' : ''),
    'dup.lastFailed': (p) => `上次检查失败：${p.msg}` + (p.prev ? '。下面是更早一次的结果。' : ''),
    'dup.lastUnfinished': '上次检查没有正常结束。',
    'dup.summary': '{groups} 组重复，可释放 {size}',
    'dup.none': '没有发现重复项',
    'dup.checkedAt': '检查于 {time}',
    'dup.changed': '{n} 个文件扫描后有变化，未参与比较',
    'dup.errors': '{n} 个文件读取出错',
    'dup.recheck': '重新检查',
    'dup.dirNote': '每份 {n} 个文件，文件名、目录结构和内容都相同',
    'dup.covered': '位于重复文件夹内',
    'dup.covered.title': '这一份所在的上级文件夹本身就有完全相同的副本',
    'dup.jump': '↗ 查看位置',
    'dup.jump.title': '在空间占用视图中打开所在目录',
    'dup.moreCopies': '还有 {n} 份未列出',
    'dup.checking.h': '正在检查重复项',
    'dup.checking.p': '检查完成后会自动显示结果。检查以最低优先级运行，不影响 NAS 的正常使用；可以随时取消，已经算过的文件下次不会重读。',
    'dup.unchecked.h': '这次扫描还没有检查结果',
    'dup.never.h': '还没有检查过重复项',
    'dup.intro': '重复检查会读取大小相同的文件的内容，并用完整的校验值确认是否完全相同。第一次检查可能需要数小时，以最低优先级运行，不影响 NAS 的正常使用；之后再检查只会读取新增或改动过的文件。',
    'dup.start': '开始重复检查',
    'dup.noneFound.p': '这次扫描中没有内容完全相同的文件或文件夹（空文件、硬链接和缩略图等系统文件不计入）。',
    'dup.more': '加载更多（剩余 {n} 组）',
    'dup.progTitle': '正在检查重复项 · {phase}',
    'dup.readBytes': '已读 {done} / {total}',
    'dup.files': '{done} / {total} 个文件',
    'dup.cacheHits': '缓存命中 {n}',
    'dup.changedShort': '扫描后有变化 {n}',
    'dup.errorsShort': '读取出错 {n}',
  };

  // Wording for the generic Linux build (<html data-platform="linux">, set by
  // the server): same keys as above, with "NAS" / "QTS" replaced.
  const zhLinux = {
    'net.down': '无法连接到服务器',
    'net.badResponse': '服务返回了无法识别的内容',
    'err.not_logged_in': '请先登录',
    'err.auth_unavailable': '无法验证登录状态：{detail}',
    'empty.running.p': '扫描完成后会自动显示结果。扫描以最低优先级运行，不会影响服务器的正常使用。',
    'empty.p': '扫描会统计本机所有磁盘文件系统的空间占用，文件较多时需要几分钟到几十分钟。',
    'auth.h': '请先登录',
    'auth.p': '登录已过期或尚未登录，请使用密码登录。',
    'del.p': '确定删除 {time} 的扫描记录吗？此操作只删除扫描结果，不会影响服务器上的任何文件。',
    'set.timeHint': '按服务器本地时间执行，建议选择夜间空闲时段。',
    'set.dupHint': '首次检查可能要几个小时，之后通常几分钟到几十分钟；期间服务器硬盘会比较忙。',
    'dup.checking.p': '检查完成后会自动显示结果。检查以最低优先级运行，不影响服务器的正常使用；可以随时取消，已经算过的文件下次不会重读。',
    'dup.intro': '重复检查会读取大小相同的文件的内容，并用完整的校验值确认是否完全相同。第一次检查可能需要数小时，以最低优先级运行，不影响服务器的正常使用；之后再检查只会读取新增或改动过的文件。',
  };

  const en = {
    'app.title': 'Storage Analyzer',

    'tb.scan': 'Scan',
    'tb.scan.title': 'Choose which scan result to view',
    'tb.newScan': 'New scan',
    'tb.history': 'History',
    'tb.settings': 'Settings',
    'tb.logout': 'Sign out',
    'tb.view': 'View',
    'tb.sizeMode': 'Size to show',
    'view.space': 'Space',
    'view.space.title': 'Space used by each folder',
    'view.dups': 'Duplicates',
    'view.dups.title': 'Files and folders with identical content',
    'size.disk': 'Disk usage',
    'size.disk.title': 'Space the files actually take up on disk',
    'size.apparent': 'Apparent size',
    'size.apparent.title': 'Size of the file contents',
    'col.name': 'Name',
    'col.size': 'Size',
    'col.pct': 'Share',
    'col.items': 'Items',
    'col.mtime': 'Modified',
    'col.copies': 'Copies',
    'col.sizeEach': 'Size each',
    'col.reclaim': 'Reclaimable',
    'dlg.close': 'Close',

    'trigger.schedule': 'Scheduled',
    'trigger.manual': 'Manual',
    'status.done': 'Done',
    'status.failed': 'Failed',
    'status.canceled': 'Canceled',
    'status.running': 'Running',
    'dur.h': '{h} h {m} min',
    'dur.m': '{m} min {s} s',
    'dur.s': '{s} s',
    'elapsed': '{dur} elapsed',
    'weekdays': ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
    'unknownError': 'unknown error',
    'opFailed': 'Something went wrong',
    'back': 'Back',
    'cancel': 'Cancel',
    'list.empty': 'Empty folder',
    'list.more': (p) => `Load more (${pl(p.n, 'item', 'items')} left)`,

    'net.down': "Can't reach the NAS",
    'net.badResponse': "The NAS sent a response the app couldn't read",
    'err.not_logged_in': 'Please sign in to QTS',
    'err.not_admin': 'Administrator access required',
    'err.auth_unavailable': "Couldn't check your QTS sign-in: {detail}",
    'err.task_running': 'Another task is already running',
    'err.not_running': 'No scan is running',
    'err.scan_running': 'This scan is still running. Cancel it first.',
    'err.scan_not_found': "This scan result doesn't exist or has been deleted",
    'err.no_scans': 'No scan results yet',
    'err.dir_not_found': 'Folder not found',
    'err.invalid_settings': 'Invalid settings: {detail}',
    'err.internal': 'Internal error: {detail}',
    'err.bad_password': 'Incorrect password',
    'err.too_many_attempts': 'Too many attempts. Try again in {min} min.',

    'scans.none': 'No scan results',
    'notice.scanDeleted': 'That scan result was deleted. Showing the latest one.',
    'notice.dirGone': "That folder doesn't exist. Back at the top level.",
    'crumbs.all': 'All volumes',
    'crumbs.summary': (p) => `This folder: ${p.size} · ${pl(p.n, 'item', 'items')}`,
    'row.up': '.. Up one level',

    'vol.poolMount': 'Storage pool {pool} · {mount}',
    'vol.fsDev': '{fstype} · {device}',
    'vol.cap': '{used} of {total} used ({pct}%)',
    'vol.bar.title': 'Blue: scanned files. Orange: used space not accounted for. Grey: free space.',
    'vol.scanned': 'Scanned files {size}',
    'vol.unacc': 'Not accounted for (snapshots, metadata, etc.) {size}',
    'vol.unacc.title': 'Snapshots, file system metadata, changes during the scan, etc.',
    'vol.free': 'Free {size}',
    'skip.not_mounted': 'not mounted (possibly a locked encrypted volume)',
    'skip.not_share_volume': 'not a data volume',
    'skip.not_found': 'path not found',
    'skip.not_directory': 'not a directory',
    'skip.read_error': 'read error',
    'skip.item': '{name} ({why})',
    'skip.list': 'Skipped: {list}',
    'skip.sep': ', ',

    'tag.mount': 'Mount point, skipped',
    'tag.symlink': 'Symlink',
    'tag.hardlink': 'Hard link (counted elsewhere)',
    'tag.hardlink.title': 'Another hard link to the same file is already counted',
    'tag.readError': 'Read error',
    'tag.badName': 'Bad name encoding',
    'tag.badName.title': "The name isn't valid UTF-8. Characters that can't be shown are replaced with �.",

    'empty.running.h': 'Running the first scan',
    'empty.running.p': "Results appear here when the scan finishes. Scans run at the lowest priority, so they won't slow down the NAS.",
    'empty.h': 'No scan results yet',
    'empty.p': 'A scan measures the space used on the data volumes of every storage pool. With many files it can take from a few minutes to an hour or so.',
    'empty.start': 'Start the first scan',
    'auth.h': 'Please sign in to QTS',
    'auth.p': 'Your session has expired. Reload QTS and open Storage Analyzer again.',
    'admin.h': 'Administrator access required',
    'admin.p': "Storage Analyzer can see every user's file names, so only administrators can use it.",
    'login.h': 'Sign in',
    'login.password': 'Password',
    'login.submit': 'Sign in',
    'login.insecure': "This connection isn't encrypted. Use it only on your LAN or Tailscale, or put it behind a reverse proxy.",

    'scan.started': 'Scan started',
    'scan.done': (p) => `Scan finished: ${p.size}, ${pl(p.files, 'file', 'files')}, took ${p.dur}`,
    'scan.canceled': 'Scan canceled',
    'scan.failed': 'Scan failed: {msg}',
    'scan.cancel.h': 'Cancel scan',
    'scan.cancel.p': "Cancel the current scan? What's been scanned so far won't be saved.",
    'scan.cancel.ok': 'Cancel scan',
    'scan.canceling': 'Canceling scan…',
    'prog.scanning': 'Scanning…',
    'prog.scanningPct': 'Scanning… about {pct}%',
    'prog.scanned': (p) => `Scanned ${pl(p.files, 'file', 'files')} and ${pl(p.dirs, 'folder', 'folders')}`,
    'prog.errors': (p) => pl(p.n, 'read error', 'read errors'),
    'prog.scheduled': 'Scheduled',
    'prog.scheduledThenDup': 'Scheduled, duplicate check follows',

    'hist.title': 'Scan history',
    'hist.cols': ['Started', 'Trigger', 'Status', 'Total', 'Files', 'Duration', 'Result size', 'Duplicates', ''],
    'hist.none': 'No scans yet.',
    'hist.keep': (p) => (p.n === 1
      ? 'Only the latest successful scan is kept. You can change this in Settings.'
      : `The latest ${num(p.n)} successful scans are kept automatically. You can change this in Settings.`),
    'hist.open': 'Open',
    'hist.delete': 'Delete',
    'hist.dupReclaim': '{size} reclaimable',
    'hist.dupNone': 'None',
    'del.h': 'Delete scan result',
    'del.p': 'Delete the scan from {time}? This only deletes the scan result. No files on the NAS are touched.',
    'del.ok': 'Delete',
    'del.done': 'Deleted',

    'set.title': 'Settings',
    'set.schedule': 'Scheduled scan',
    'set.off': 'Off',
    'set.daily': 'Daily',
    'set.weekly': 'Weekly',
    'set.weekday': 'Day',
    'set.time': 'Time',
    'set.timeHint': "Runs on the NAS's local time. Pick a quiet time, such as overnight.",
    'set.dup': 'Duplicates',
    'set.dupLabel': 'Check for duplicates after the scan',
    'set.dupHint': 'The first check can take a few hours; later ones usually take minutes to an hour. The NAS disks stay busy while it runs.',
    'set.keep': 'Keep results',
    'set.keepUnit': 'scans',
    'set.keepHint': 'How many recent successful scans to keep (1–100). Each result takes tens to hundreds of MB.',
    'set.keepRange': 'Keep results must be between 1 and 100',
    'set.defaultSize': 'Default size',
    'set.save': 'Save',
    'set.saveFailed': "Couldn't save the settings",
    'set.savedOff': 'Settings saved. Scheduled scans are off.',
    'set.savedOn': (p) => `Settings saved. Scans run ${p.daily ? 'every day' : 'every ' + p.weekday} at ${p.time}${p.dup ? ', then check for duplicates' : ''}.`,

    'phase.prepare': 'Checking files',
    'phase.quick': 'Quick compare',
    'phase.full': 'Full verification',
    'phase.rollup': 'Grouping folders',
    'phase.write': 'Saving results',
    'phase.default': 'Preparing',
    'dup.started': 'Duplicate check started',
    'dup.busyStart': 'Another task is running. Try again once it finishes.',
    'dup.busyTitle': 'A scan or duplicate check is already running',
    'dup.busyNote': 'A scan or duplicate check is running. You can start once it finishes.',
    'dup.done': (p) => `Duplicate check finished: ${pl(p.groups, 'group', 'groups')}, ${p.size} reclaimable`,
    'dup.doneNone': 'Duplicate check finished: no duplicates found',
    'dup.canceled': 'Duplicate check canceled',
    'dup.failed': 'Duplicate check failed: {msg}',
    'dup.cancel.h': 'Cancel duplicate check',
    'dup.cancel.p': "Cancel the duplicate check? Files already hashed stay in the cache and won't be read again next time.",
    'dup.cancel.ok': 'Cancel check',
    'dup.canceling': 'Canceling duplicate check…',
    'dup.runningHere': (p) => 'Checking for duplicates. Progress is shown above.' + (p.prev ? ' The results below are from the previous check.' : ''),
    'dup.lastCanceled': (p) => "The last check was canceled. Files already hashed won't be read again." + (p.prev ? ' The results below are from an earlier check.' : ''),
    'dup.lastFailed': (p) => `The last check failed: ${p.msg}` + (p.prev ? '. The results below are from an earlier check.' : ''),
    'dup.lastUnfinished': "The last check didn't finish properly.",
    'dup.summary': (p) => `${pl(p.groups, 'duplicate group', 'duplicate groups')}, ${p.size} reclaimable`,
    'dup.none': 'No duplicates found',
    'dup.checkedAt': 'checked {time}',
    'dup.changed': (p) => `${pl(p.n, 'file', 'files')} changed after the scan and ${p.n === 1 ? 'was' : 'were'} skipped`,
    'dup.errors': (p) => `${pl(p.n, 'file', 'files')} couldn't be read`,
    'dup.recheck': 'Check again',
    'dup.dirNote': (p) => `Each copy has ${pl(p.n, 'file', 'files')}, with identical names, folder structure and content`,
    'dup.covered': 'Inside a duplicate folder',
    'dup.covered.title': 'A parent folder of this copy has an identical copy of its own',
    'dup.jump': '↗ Show location',
    'dup.jump.title': 'Open the containing folder in the Space view',
    'dup.moreCopies': (p) => `${pl(p.n, 'more copy', 'more copies')} not listed`,
    'dup.checking.h': 'Checking for duplicates',
    'dup.checking.p': "Results appear here when the check finishes. It runs at the lowest priority, so it won't slow down the NAS. You can cancel at any time; files already hashed won't be read again.",
    'dup.unchecked.h': "This scan hasn't been checked yet",
    'dup.never.h': 'No duplicate check yet',
    'dup.intro': "A duplicate check reads the content of files that have the same size and compares full checksums to confirm they're identical. The first check can take hours. It runs at the lowest priority, so it won't slow down the NAS, and later checks only read new or changed files.",
    'dup.start': 'Start duplicate check',
    'dup.noneFound.p': "This scan has no files or folders with identical content. (Empty files, hard links and system files such as thumbnails aren't counted.)",
    'dup.more': (p) => `Load more (${pl(p.n, 'group', 'groups')} left)`,
    'dup.progTitle': 'Checking for duplicates · {phase}',
    'dup.readBytes': 'Read {done} of {total}',
    'dup.files': (p) => `${num(p.done)} of ${pl(p.total, 'file', 'files')}`,
    'dup.cacheHits': '{n} from cache',
    'dup.changedShort': '{n} changed since the scan',
    'dup.errorsShort': (p) => pl(p.n, 'read error', 'read errors'),
  };

  const enLinux = {
    'net.down': "Can't reach the server",
    'net.badResponse': "The server sent a response the app couldn't read",
    'err.not_logged_in': 'Please sign in',
    'err.auth_unavailable': "Couldn't check your sign-in: {detail}",
    'empty.running.p': "Results appear here when the scan finishes. Scans run at the lowest priority, so they won't slow down the server.",
    'empty.p': 'A scan measures the space used on every local disk file system. With many files it can take from a few minutes to an hour or so.',
    'auth.h': 'Please sign in',
    'auth.p': 'You are not signed in, or your session has expired. Sign in with your password.',
    'del.p': 'Delete the scan from {time}? This only deletes the scan result. No files on the server are touched.',
    'set.timeHint': "Runs on the server's local time. Pick a quiet time, such as overnight.",
    'set.dupHint': 'The first check can take a few hours; later ones usually take minutes to an hour. The server disks stay busy while it runs.',
    'dup.checking.p': "Results appear here when the check finishes. It runs at the lowest priority, so it won't slow down the server. You can cancel at any time; files already hashed won't be read again.",
    'dup.intro': "A duplicate check reads the content of files that have the same size and compares full checksums to confirm they're identical. The first check can take hours. It runs at the lowest priority, so it won't slow down the server, and later checks only read new or changed files.",
  };

  const platform = document.documentElement.dataset.platform === 'linux' ? 'linux' : 'qnap';
  const dict = platform === 'linux'
    ? Object.assign({}, lang === 'zh' ? zh : en, lang === 'zh' ? zhLinux : enLinux)
    : (lang === 'zh' ? zh : en);

  // t looks up a string and fills {name} placeholders from params; numbers
  // are formatted for the locale. Entries may also be functions of params
  // (for English plurals) or arrays (weekday names, table headers).
  function t(key, params = {}) {
    const v = key in dict ? dict[key] : key in zh ? zh[key] : key;
    if (typeof v === 'function') return v(params);
    if (typeof v !== 'string') return v;
    return v.replace(/\{(\w+)\}/g, (m, k) => {
      const x = params[k];
      if (x === undefined || x === null) return m;
      return typeof x === 'number' ? num(x) : String(x);
    });
  }

  // apply translates the static page: data-i18n sets the text,
  // data-i18n-title and data-i18n-aria-label set those attributes.
  function apply(root = document) {
    document.documentElement.lang = locale;
    document.title = t('app.title');
    for (const n of root.querySelectorAll('[data-i18n]')) n.textContent = t(n.dataset.i18n);
    for (const n of root.querySelectorAll('[data-i18n-title]')) n.title = t(n.dataset.i18nTitle);
    for (const n of root.querySelectorAll('[data-i18n-aria-label]')) n.setAttribute('aria-label', t(n.dataset.i18nAriaLabel));
  }

  window.I18N = { lang, locale, platform, t, num, apply, _dicts: { zh, en, zhLinux, enLinux } };
})();
