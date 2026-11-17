(() => {
  'use strict';

  const KIND = { FILE: 0, DIR: 1, SYMLINK: 2, OTHER: 3, MOUNT: 4 };
  const FLAG = { ERROR: 1, HARDLINK_DUP: 2, BAD_NAME: 4 };
  const PAGE = 500;
  const WEEKDAYS = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];

  const $ = (id) => document.getElementById(id);
  const el = (tag, attrs = {}, ...kids) => {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null || v === false) continue;
      if (k === 'class') n.className = v;
      else if (k === 'text') n.textContent = v;
      else if (k.startsWith('on')) n.addEventListener(k.slice(2), v);
      else n.setAttribute(k, v === true ? '' : v);
    }
    for (const c of kids.flat()) if (c !== null && c !== undefined && c !== false) n.append(c);
    return n;
  };

  const state = {
    scans: [],
    scanId: null,
    dirId: 0,
    sort: 'size',
    order: 'desc',
    sizeMode: 'disk',
    items: [],
    total: 0,
    path: [],
    dir: null,
    volumes: [],
    skipped: [],
    sel: -1,
    loading: false,
    running: false,
    progress: null,
    settings: null,
    pollTimer: null,
    seq: 0,
    view: 'space', // space | dups
    dup: { groups: [], total: 0, check: null, result: null, open: new Set(), loaded: false },
    dupSeq: 0,
  };
  const DUP_PAGE = 100;
  const PHASE_TEXT = { prepare: '核对文件', quick: '快速比对', full: '完整校验', rollup: '汇总文件夹', write: '保存结果' };

  // ---------- formatting ----------
  function fmtSize(b) {
    if (b === null || b === undefined) return '';
    if (b < 1024) return b + ' B';
    const u = ['KB', 'MB', 'GB', 'TB', 'PB'];
    let v = b / 1024, i = 0;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return (v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2)) + ' ' + u[i];
  }
  const fmtNum = (n) => (n || 0).toLocaleString('zh-CN');
  const pad = (n) => String(n).padStart(2, '0');
  function fmtTime(sec, withYear = true) {
    if (!sec) return '';
    const d = new Date(sec * 1000);
    const date = `${withYear ? d.getFullYear() + '-' : ''}${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    return `${date} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }
  function fmtDuration(s) {
    s = Math.max(0, Math.round(s || 0));
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
    if (h) return `${h} 小时 ${m} 分`;
    if (m) return `${m} 分 ${r} 秒`;
    return `${r} 秒`;
  }
  const sizeOf = (it) => (state.sizeMode === 'apparent' ? it.asize : it.dsize);
  const triggerText = (t) => (t === 'schedule' ? '定时' : '手动');
  const statusText = { done: '完成', failed: '失败', canceled: '已取消', running: '进行中' };

  // ---------- API ----------
  class ApiError extends Error {
    constructor(status, code, message) { super(message || code); this.status = status; this.code = code; }
  }
  async function api(action, params = {}, opts = {}) {
    const qs = new URLSearchParams({ action, ...params });
    const init = { method: opts.method || 'GET', credentials: 'same-origin', headers: {} };
    if (init.method === 'POST') {
      init.headers['X-SA-Request'] = '1';
      if (opts.body !== undefined) {
        init.headers['Content-Type'] = 'application/json';
        init.body = JSON.stringify(opts.body);
      }
    }
    let res;
    try {
      res = await fetch('api.cgi?' + qs.toString(), init);
    } catch (e) {
      throw new ApiError(0, 'network', '无法连接到 NAS');
    }
    let data = null;
    const text = await res.text();
    if (text) {
      try { data = JSON.parse(text); } catch { throw new ApiError(res.status, 'bad_response', '服务返回了无法识别的内容'); }
    }
    if (!res.ok) throw new ApiError(res.status, data && data.error, data && data.message);
    return data;
  }

  // ---------- toast ----------
  let toastTimer;
  function toast(msg) {
    const t = $('toast');
    t.textContent = msg;
    t.classList.remove('hidden');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => t.classList.add('hidden'), 3200);
  }

  // ---------- icons ----------
  const ICONS = {
    dir: 'M1.5 3.5A1.5 1.5 0 0 1 3 2h3.2l1.6 1.6H13A1.5 1.5 0 0 1 14.5 5v7.5A1.5 1.5 0 0 1 13 14H3a1.5 1.5 0 0 1-1.5-1.5v-9Z',
    file: 'M4 1.5h5.3L13 5.2v8.3A1 1 0 0 1 12 14.5H4a1 1 0 0 1-1-1v-11a1 1 0 0 1 1-1Zm5 .9V5.5h3.1L9 2.4Z',
    link: 'M6.5 9.5a3 3 0 0 0 4.2 0l2.3-2.3a3 3 0 0 0-4.2-4.2l-.9.9 1 1 .9-.9a1.6 1.6 0 1 1 2.2 2.2l-2.3 2.3a1.6 1.6 0 0 1-2.2 0l-1 1Zm3-3a3 3 0 0 0-4.2 0L3 8.8A3 3 0 0 0 7.2 13l.9-.9-1-1-.9.9a1.6 1.6 0 0 1-2.2-2.2l2.3-2.3a1.6 1.6 0 0 1 2.2 0l1-1Z',
    mount: 'M2 3h12v4H2V3Zm0 6h12v4H2V9Zm9.5-4.5a.8.8 0 1 0 0-.01Zm0 6a.8.8 0 1 0 0-.01Z',
    vol: 'M2 3.5A1.5 1.5 0 0 1 3.5 2h9A1.5 1.5 0 0 1 14 3.5v9a1.5 1.5 0 0 1-1.5 1.5h-9A1.5 1.5 0 0 1 2 12.5v-9ZM4 10.5h8V12H4v-1.5Z',
  };
  function icon(kind, isRoot) {
    const key = isRoot ? 'vol' : kind === KIND.DIR ? 'dir' : kind === KIND.SYMLINK ? 'link' : kind === KIND.MOUNT ? 'mount' : 'file';
    const cls = isRoot ? 'ico-mount' : key === 'dir' ? 'ico-dir' : key === 'link' ? 'ico-link' : key === 'mount' ? 'ico-mount' : 'ico-file';
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 16 16');
    svg.setAttribute('class', cls);
    svg.setAttribute('aria-hidden', 'true');
    const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    p.setAttribute('d', ICONS[key]);
    svg.append(p);
    return svg;
  }

  // ---------- hash state ----------
  function writeHash() {
    if (state.scanId === null) return;
    const h = `#scan=${state.scanId}&dir=${state.dirId}` + (state.view === 'dups' ? '&view=dups' : '');
    if (location.hash !== h) history.replaceState(null, '', h);
  }
  function readHash() {
    const p = new URLSearchParams(location.hash.slice(1));
    const scan = parseInt(p.get('scan'), 10), dir = parseInt(p.get('dir'), 10);
    return { scan: Number.isFinite(scan) ? scan : null, dir: Number.isFinite(dir) ? dir : 0, view: p.get('view') === 'dups' ? 'dups' : 'space' };
  }

  // ---------- scans ----------
  async function loadScans() {
    const data = await api('scans');
    state.scans = data.scans || [];
    renderScanSelect();
  }
  const doneScans = () => state.scans.filter((s) => s.status === 'done');

  function renderScanSelect() {
    const sel = $('scanSelect');
    sel.replaceChildren();
    const done = doneScans();
    if (!done.length) {
      sel.append(el('option', { text: '暂无扫描结果', value: '' }));
      sel.disabled = true;
      return;
    }
    sel.disabled = false;
    for (const s of done) {
      sel.append(el('option', { value: s.id, text: `${fmtTime(s.startedAt)} · ${triggerText(s.trigger)} · ${fmtSize(s.totalDSize)}` }));
    }
    if (state.scanId !== null) sel.value = String(state.scanId);
  }

  // ---------- listing ----------
  async function openDir(dirId, { keepSel = false, selectId = null } = {}) {
    const seq = ++state.seq;
    state.loading = true;
    try {
      const data = await api('list', {
        scan: state.scanId, dir: dirId, sort: state.sort, order: state.order, size: state.sizeMode, offset: 0, limit: PAGE,
      });
      if (seq !== state.seq) return;
      const prevDir = state.dirId;
      state.dirId = dirId;
      state.dir = data.dir;
      state.path = data.path || [];
      state.items = data.items || [];
      state.total = data.total || 0;
      if (dirId === 0) { state.volumes = data.volumes || []; state.skipped = data.skipped || []; }
      if (!keepSel || prevDir !== dirId) state.sel = -1;
      if (selectId !== null) state.sel = state.items.findIndex((i) => i.id === selectId);
      writeHash();
      render();
      if (!keepSel) $('listWrap').scrollTop = 0;
      if (selectId !== null) highlight();
    } catch (e) {
      if (seq !== state.seq) return;
      if (e.code === 'scan_not_found' || e.code === 'dir_not_found') {
        toast(e.code === 'scan_not_found' ? '该扫描结果已被删除，已切换到最新结果' : '目录不存在，已返回顶层');
        await loadScans().catch(() => {});
        const latest = doneScans()[0];
        if (latest && (e.code === 'scan_not_found' ? latest.id !== state.scanId : true)) {
          state.scanId = latest.id;
          renderScanSelect();
          return showCurrent();
        }
        if (!latest) { state.scanId = null; return renderEmpty(); }
      }
      showError(e);
    } finally {
      if (seq === state.seq) state.loading = false;
    }
  }

  async function loadMore() {
    const btn = $('btnMore');
    btn.disabled = true;
    try {
      const data = await api('list', {
        scan: state.scanId, dir: state.dirId, sort: state.sort, order: state.order, size: state.sizeMode,
        offset: state.items.length, limit: PAGE,
      });
      state.items = state.items.concat(data.items || []);
      renderRows();
    } catch (e) {
      showError(e);
    } finally {
      btn.disabled = false;
    }
  }

  function goUp() {
    if (state.dirId === 0) return;
    const parent = state.path.length >= 2 ? state.path[state.path.length - 2].id : 0;
    openDir(parent);
  }

  // ---------- rendering ----------
  function render() {
    $('state').classList.add('hidden');
    $('list').classList.remove('hidden');
    renderCrumbs();
    renderVolumes();
    renderHead();
    renderRows();
  }

  function renderCrumbs() {
    const ol = $('crumbs');
    ol.replaceChildren(el('li', {}, el('a', { text: '全部卷', onclick: () => openDir(0) })));
    for (const p of state.path) ol.append(el('li', {}, el('a', { text: p.name, title: p.name, onclick: () => openDir(p.id) })));
    const d = state.dir;
    $('dirSummary').textContent = d ? `本目录 ${fmtSize(sizeOf(d))} · ${fmtNum(d.items)} 项` : '';
  }

  function renderVolumes() {
    const box = $('volumes');
    if (state.dirId !== 0 || !state.volumes.length) { box.classList.add('hidden'); return; }
    box.replaceChildren();
    const byId = new Map(state.items.map((i) => [i.id, i]));
    for (const v of state.volumes) {
      const item = byId.get(v.entryId);
      const scanned = item ? item.dsize : 0;
      const total = v.fsTotal || 0;
      const used = v.fsUsed || 0;
      const unacc = Math.max(0, used - scanned);
      const pct = (x) => (total ? Math.min(100, (x / total) * 100) : 0) + '%';
      box.append(el('div', { class: 'vol' },
        el('div', { class: 'vol-head' },
          el('div', { class: 'vol-name' }, v.name, el('small', { text: (v.pool ? `存储池 ${v.pool} · ` : '') + v.mountPoint })),
          el('div', { class: 'vol-cap', text: total ? `已用 ${fmtSize(used)} / ${fmtSize(total)}（${((used / total) * 100).toFixed(1)}%）` : '' })),
        el('div', { class: 'cap', title: '蓝色：扫描到的文件；橙色：未计入的已用空间；灰色：可用空间' },
          el('span', { class: 's', style: `width:${pct(Math.min(scanned, used || scanned))}` }),
          el('span', { class: 'u', style: `width:${pct(unacc)}` })),
        el('div', { class: 'legend' },
          el('span', {}, el('i', { style: 'background:var(--bar)' }), `扫描到的文件 ${fmtSize(scanned)}`),
          el('span', { title: '快照、文件系统元数据、扫描期间的变化等' }, el('i', { style: 'background:var(--unaccounted)' }), `未计入（快照 / 元数据等）${fmtSize(unacc)}`),
          el('span', {}, el('i', { style: 'background:var(--free)' }), `可用 ${fmtSize(v.fsFree)}`))));
    }
    if (state.skipped.length) {
      const why = { not_mounted: '未挂载（可能是未解锁的加密卷）', not_share_volume: '不是数据卷' };
      box.append(el('div', { class: 'notice', text: '已跳过：' + state.skipped.map((s) => `${s.name}（${why[s.reason] || s.reason}）`).join('，') }));
    }
    box.classList.remove('hidden');
  }

  function renderHead() {
    for (const th of document.querySelectorAll('#list th')) {
      const on = th.dataset.sort === state.sort && !th.classList.contains('c-pct');
      th.classList.toggle('sorted', on);
      th.classList.toggle('asc', on && state.order === 'asc');
    }
  }

  function rowFor(it, idx, parentSize) {
    const isRoot = state.dirId === 0;
    const size = sizeOf(it);
    const pctVal = parentSize > 0 ? (size / parentSize) * 100 : 0;
    const enterable = (it.kind === KIND.DIR) && it.hasChildren;
    const tags = [];
    if (it.kind === KIND.MOUNT) tags.push(el('span', { class: 'tag', text: '挂载点，已跳过' }));
    if (it.kind === KIND.SYMLINK) tags.push(el('span', { class: 'tag', text: '符号链接' }));
    if (it.flags & FLAG.HARDLINK_DUP) tags.push(el('span', { class: 'tag', text: '硬链接（已在别处计入）', title: '同一个文件的另一个硬链接已经计入大小' }));
    if (it.flags & FLAG.ERROR) tags.push(el('span', { class: 'tag warn', text: '读取出错' }));
    if (it.badName) tags.push(el('span', { class: 'tag warn', text: '名称编码异常', title: '文件名不是有效的 UTF-8，无法显示的字符已替换为 �' }));
    const tr = el('tr', { class: [enterable ? 'dir' : '', idx === state.sel ? 'sel' : ''].join(' ').trim(), 'data-idx': idx },
      el('td', { class: 'c-name' }, el('div', { class: 'name', title: it.name }, icon(it.kind, isRoot), el('span', { class: 't', text: it.name }), tags)),
      el('td', { class: 'c-size', text: it.kind === KIND.MOUNT ? '—' : fmtSize(size) }),
      el('td', { class: 'c-pct' }, el('div', { class: 'pct' },
        el('div', { class: 'track' }, el('div', { class: 'fill' + (it.kind === KIND.DIR ? '' : ' file'), style: `width:${Math.min(100, pctVal).toFixed(2)}%` })),
        el('span', { class: 'num', text: it.kind === KIND.MOUNT ? '' : pctVal.toFixed(1) + '%' }))),
      el('td', { class: 'c-items', text: it.kind === KIND.DIR ? fmtNum(it.items) : '' }),
      el('td', { class: 'c-mtime', text: fmtTime(it.mtime) }));
    tr.addEventListener('click', () => { state.sel = idx; highlight(); if (enterable) openDir(it.id); });
    return tr;
  }

  function renderRows() {
    const tbody = $('rows');
    tbody.replaceChildren();
    if (state.dirId !== 0) {
      const up = el('tr', { class: 'dir up' }, el('td', { class: 'c-name' }, el('div', { class: 'name' }, icon(KIND.DIR), el('span', { class: 't', text: '.. 上一级' }))),
        el('td'), el('td'), el('td'), el('td'));
      up.addEventListener('click', goUp);
      tbody.append(up);
    }
    const parentSize = state.dir ? sizeOf(state.dir) : 0;
    const frag = document.createDocumentFragment();
    state.items.forEach((it, i) => frag.append(rowFor(it, i, parentSize)));
    tbody.append(frag);
    const remaining = state.total - state.items.length;
    $('moreWrap').classList.toggle('hidden', remaining <= 0);
    $('btnMore').textContent = `加载更多（剩余 ${fmtNum(remaining)} 项）`;
    if (!state.items.length) {
      tbody.append(el('tr', {}, el('td', { colspan: 5, class: 'muted', text: '空目录' })));
    }
  }

  function highlight() {
    for (const tr of $('rows').querySelectorAll('tr[data-idx]')) {
      tr.classList.toggle('sel', Number(tr.dataset.idx) === state.sel);
    }
    const cur = $('rows').querySelector('tr.sel');
    if (cur) cur.scrollIntoView({ block: 'nearest' });
  }

  function showState(title, text, button) {
    $('list').classList.add('hidden');
    $('moreWrap').classList.add('hidden');
    $('volumes').classList.add('hidden');
    $('crumbs').replaceChildren();
    $('dirSummary').textContent = '';
    const s = $('state');
    s.replaceChildren(el('h3', { text: title }), el('p', { text }), button || '');
    s.classList.remove('hidden');
  }

  function renderEmpty() {
    if (state.view !== 'space') setView('space', { load: false });
    if (state.running) {
      showState('正在进行第一次扫描', '扫描完成后会自动显示结果。扫描以最低优先级运行，不会影响 NAS 的正常使用。');
      return;
    }
    showState('还没有扫描结果', '扫描会统计所有存储池上数据卷的空间占用，文件较多时需要几分钟到几十分钟。',
      el('button', { class: 'btn primary', type: 'button', text: '开始第一次扫描', onclick: startScan }));
  }

  function showError(e) {
    if (e.status === 401) return showState('请先登录 QTS', '登录已过期，请刷新 QTS 页面后重新打开存储分析器。');
    if (e.status === 403 && e.code === 'not_admin') return showState('需要管理员权限', '存储分析器可以看到所有用户的文件名，只有管理员可以使用。');
    toast(e.message || '操作失败');
  }

  // ---------- scanning ----------
  async function startScan() {
    try {
      await api('scan_start', {}, { method: 'POST' });
      toast('扫描已开始');
      state.running = true;
      state.progress = null;
      renderProgress();
      if (state.scanId === null) renderEmpty();
      schedulePoll(800);
    } catch (e) {
      if (e.code === 'task_running') { toast('已有任务正在进行'); schedulePoll(0); return; }
      showError(e);
    }
  }

  async function cancelScan() {
    const dup = isDupJob(state.progress);
    const ok = dup
      ? await confirmDialog('取消重复检查', '确定要取消重复检查吗？已经算过的文件会保留在缓存里，下次检查不用重读。', '取消检查', true)
      : await confirmDialog('取消扫描', '确定要取消当前扫描吗？已扫描的部分不会保存。', '取消扫描', true);
    if (!ok) return;
    try {
      await api('scan_cancel', {}, { method: 'POST' });
      toast(dup ? '正在取消重复检查…' : '正在取消扫描…');
      schedulePoll(800);
    } catch (e) {
      if (e.code === 'not_running') { schedulePoll(0); return; }
      showError(e);
    }
  }

  function schedulePoll(ms) {
    clearTimeout(state.pollTimer);
    state.pollTimer = setTimeout(pollStatus, ms);
  }

  async function pollStatus() {
    let st;
    try {
      st = await api('status');
    } catch (e) {
      if (e.status === 401 || e.status === 403) return showError(e);
      schedulePoll(5000);
      return;
    }
    const was = state.running;
    const prev = state.progress;
    state.running = !!st.running;
    state.progress = st.progress;
    renderProgress();
    if (state.view === 'dups' && state.dup.loaded && was !== state.running) renderDups();
    if (state.running) {
      schedulePoll(2000);
    } else if (was) {
      if (isDupJob(prev)) await onDupFinished(prev.scanId);
      else await onScanFinished(prev);
    }
  }

  const isDupJob = (p) => !!p && p.job === 'dupcheck';

  async function onDupFinished(scanId) {
    await loadScans().catch(() => {});
    const s = state.scans.find((x) => x.id === scanId);
    const d = s && s.dup;
    if (d && d.status === 'done') {
      toast(d.groups ? `重复检查完成：${fmtNum(d.groups)} 组，可释放 ${fmtSize(state.sizeMode === 'apparent' ? d.reclaimASize : d.reclaimDSize)}` : '重复检查完成：没有发现重复项');
    } else if (d && d.status === 'canceled') {
      toast('重复检查已取消');
    } else if (d && d.status === 'failed') {
      toast('重复检查失败：' + (d.errorMessage || '未知错误'));
    }
    if (state.view === 'dups' && state.scanId === scanId) loadDups();
  }

  async function onScanFinished(prevProgress) {
    const before = new Set(state.scans.map((s) => s.id));
    await loadScans().catch(() => {});
    const fresh = state.scans.find((s) => !before.has(s.id) || (s.id === prevProgress?.scanId));
    const last = state.scans.find((s) => s.id === (prevProgress && prevProgress.scanId)) || fresh || state.scans[0];
    if (last && last.status === 'done') {
      toast(`扫描完成：${fmtSize(last.totalDSize)}，${fmtNum(last.files)} 个文件，用时 ${fmtDuration(last.durationS)}`);
      state.scanId = last.id;
      state.dirId = 0;
      renderScanSelect();
      showCurrent();
    } else if (last && last.status === 'canceled') {
      toast('扫描已取消');
      if (state.scanId === null) renderEmpty();
    } else if (last && last.status === 'failed') {
      toast('扫描失败：' + (last.errorMessage || '未知错误'));
      if (state.scanId === null) renderEmpty();
    } else if (state.scanId === null) {
      renderEmpty();
    }
  }

  function renderProgress() {
    const box = $('progress');
    $('btnNewScan').disabled = state.running;
    if (!state.running) { box.classList.add('hidden'); return; }
    box.classList.remove('hidden');
    const p = state.progress || {};
    const elapsed = p.startedAt ? Date.now() / 1000 - p.startedAt : 0;
    const bar = $('progBar');
    $('btnCancel').textContent = isDupJob(p) ? '取消检查' : '取消扫描';
    if (isDupJob(p)) return renderDupProgress(p, elapsed, bar);
    if (p.prevFiles > 0) {
      const pct = Math.min(99, (p.files / p.prevFiles) * 100);
      bar.classList.remove('indeterminate');
      bar.style.width = pct.toFixed(1) + '%';
      $('progTitle').textContent = `正在扫描…约 ${pct.toFixed(0)}%`;
    } else {
      bar.classList.add('indeterminate');
      $('progTitle').textContent = '正在扫描…';
    }
    $('progStats').textContent = `已扫描 ${fmtNum(p.files)} 个文件、${fmtNum(p.dirs)} 个目录 · ${fmtSize(p.bytes || 0)} · 用时 ${fmtDuration(elapsed)}`
      + (p.errors ? ` · ${fmtNum(p.errors)} 个读取错误` : '') + (p.trigger === 'schedule' ? ' · 定时任务' : '');
    $('progPath').textContent = p.current || '';
    $('progPath').title = p.current || '';
  }

  // ---------- dialogs ----------
  function openDialog(title, body, foot = [], narrow = false) {
    $('dlgTitle').textContent = title;
    $('dlgBody').replaceChildren(body);
    $('dlgFoot').replaceChildren(...foot);
    document.querySelector('.dialog').classList.toggle('narrow', narrow);
    $('overlay').classList.remove('hidden');
  }
  function closeDialog() {
    $('overlay').classList.add('hidden');
    if (dialogResolve) { dialogResolve(false); dialogResolve = null; }
  }
  let dialogResolve = null;
  function confirmDialog(title, text, okText, danger) {
    return new Promise((resolve) => {
      dialogResolve = resolve;
      const ok = el('button', { class: 'btn ' + (danger ? 'primary' : 'primary'), type: 'button', text: okText, onclick: () => { dialogResolve = null; $('overlay').classList.add('hidden'); resolve(true); } });
      openDialog(title, el('p', { text, style: 'margin:0;line-height:1.6' }), [el('button', { class: 'btn', type: 'button', text: '返回', onclick: closeDialog }), ok], true);
    });
  }

  async function showHistory() {
    try { await loadScans(); } catch (e) { return showError(e); }
    const rows = state.scans.map((s) => el('tr', { class: s.id === state.scanId ? 'current' : '' },
      el('td', { text: fmtTime(s.startedAt) }),
      el('td', { text: triggerText(s.trigger) }),
      el('td', {}, el('span', { class: 'status ' + s.status, text: statusText[s.status] || s.status, title: s.errorMessage || '' })),
      el('td', { class: 'num', text: s.status === 'done' ? fmtSize(s.totalDSize) : '' }),
      el('td', { class: 'num', text: s.status === 'done' ? fmtNum(s.files) : '' }),
      el('td', { class: 'num', text: s.durationS ? fmtDuration(s.durationS) : '' }),
      el('td', { class: 'num', text: s.dbBytes ? fmtSize(s.dbBytes) : '' }),
      el('td', {}, dupCell(s)),
      el('td', {},
        s.status === 'done' && s.id !== state.scanId ? el('button', { class: 'btn small', type: 'button', text: '打开', onclick: () => { closeDialog(); selectScan(s.id); } }) : '',
        ' ',
        s.status !== 'running' ? el('button', { class: 'btn small danger', type: 'button', text: '删除', onclick: () => deleteScan(s) }) : '')));
    const body = state.scans.length
      ? el('table', { class: 'hist' },
        el('thead', {}, el('tr', {}, ['开始时间', '方式', '状态', '总占用', '文件数', '耗时', '结果大小', '重复检查', ''].map((h) => el('th', { text: h })))),
        el('tbody', {}, rows))
      : el('p', { class: 'muted', text: '还没有扫描记录。' });
    const keep = state.settings ? state.settings.retention : 10;
    openDialog('扫描历史', el('div', {}, body, el('p', { class: 'muted', style: 'margin:12px 0 0;font-size:12px', text: `自动保留最近 ${keep} 次成功的扫描结果，可在设置中修改。` })));
  }

  async function deleteScan(s) {
    if (!(await confirmDialog('删除扫描结果', `确定删除 ${fmtTime(s.startedAt)} 的扫描记录吗？此操作只删除扫描结果，不会影响 NAS 上的任何文件。`, '删除', true))) return;
    try {
      await api('scan_delete', { scan: s.id }, { method: 'POST' });
      toast('已删除');
      if (s.id === state.scanId) {
        await loadScans();
        const latest = doneScans()[0];
        state.scanId = latest ? latest.id : null;
        renderScanSelect();
        state.dirId = 0;
        if (latest) showCurrent(); else renderEmpty();
      }
      showHistory();
    } catch (e) {
      showError(e);
    }
  }

  function showSettings() {
    const s = { ...(state.settings || { scheduleMode: 'off', weekday: 0, time: '03:00', retention: 10, defaultSizeMode: 'disk' }) };
    const radio = (name, value, label, checked) => el('label', {}, el('input', { type: 'radio', name, value, checked }), label);
    const modeBox = el('div', { class: 'radios' },
      radio('mode', 'off', '关闭', s.scheduleMode === 'off'),
      radio('mode', 'daily', '每天', s.scheduleMode === 'daily'),
      radio('mode', 'weekly', '每周', s.scheduleMode === 'weekly'));
    const weekday = el('select', { id: 'fWeekday' }, WEEKDAYS.map((d, i) => el('option', { value: i, text: d, selected: i === s.weekday })));
    const time = el('input', { type: 'time', id: 'fTime', value: s.time, required: true });
    const keep = el('input', { type: 'number', id: 'fKeep', min: 1, max: 100, value: s.retention });
    const sizeSel = el('select', { id: 'fSize' },
      el('option', { value: 'disk', text: '磁盘占用', selected: s.defaultSizeMode === 'disk' }),
      el('option', { value: 'apparent', text: '表观大小', selected: s.defaultSizeMode === 'apparent' }));
    const weekRow = el('div', { class: 'row' }, el('span', { text: '星期' }), weekday);
    const timeRow = el('div', { class: 'row' }, el('span', { text: '时间' }), time);
    const err = el('div', { class: 'err-text' });
    const timeHint = el('div', { class: 'hint', text: '按 NAS 本地时间执行，建议选择夜间空闲时段。' });
    const sync = () => {
      const m = modeBox.querySelector('input:checked').value;
      weekRow.classList.toggle('hidden', m !== 'weekly');
      timeRow.classList.toggle('hidden', m === 'off');
      timeHint.classList.toggle('hidden', m === 'off');
    };
    modeBox.addEventListener('change', sync);
    const form = el('div', { class: 'form' },
      el('div', { class: 'row' }, el('span', { text: '定时扫描' }), modeBox),
      weekRow, timeRow, timeHint,
      el('div', { class: 'row' }, el('span', { text: '保留结果' }), el('div', {}, keep, ' 次')),
      el('div', { class: 'hint', text: '保留最近几次成功的扫描结果（1–100），每份结果约占几十到几百 MB。' }),
      el('div', { class: 'row' }, el('span', { text: '默认显示' }), sizeSel),
      err);
    sync();
    const save = el('button', { class: 'btn primary', type: 'button', text: '保存' });
    save.addEventListener('click', async () => {
      const next = {
        scheduleMode: modeBox.querySelector('input:checked').value,
        weekday: Number(weekday.value),
        time: time.value || '03:00',
        retention: Number(keep.value),
        defaultSizeMode: sizeSel.value,
      };
      if (!(next.retention >= 1 && next.retention <= 100)) { err.textContent = '保留次数需要在 1–100 之间'; return; }
      save.disabled = true;
      try {
        state.settings = await api('settings', {}, { method: 'POST', body: next });
        closeDialog();
        toast(next.scheduleMode === 'off' ? '设置已保存，定时扫描已关闭'
          : `设置已保存：${next.scheduleMode === 'daily' ? '每天' : WEEKDAYS[next.weekday]} ${next.time} 自动扫描`);
      } catch (e) {
        err.textContent = e.message || '保存失败';
      } finally {
        save.disabled = false;
      }
    });
    openDialog('设置', form, [el('button', { class: 'btn', type: 'button', text: '取消', onclick: closeDialog }), save], true);
  }

  // ---------- duplicates ----------
  function setView(view, { load = true } = {}) {
    state.view = view;
    for (const b of document.querySelectorAll('#viewSeg button')) {
      const on = b.dataset.view === view;
      b.classList.toggle('on', on);
      b.setAttribute('aria-checked', on);
    }
    const dups = view === 'dups';
    $('listWrap').classList.toggle('hidden', dups);
    document.querySelector('.crumbs-row').classList.toggle('hidden', dups);
    if (dups) $('volumes').classList.add('hidden');
    $('dupBar').classList.toggle('hidden', !dups);
    $('dupWrap').classList.toggle('hidden', !dups);
    if (!load) return;
    if (dups) { writeHash(); loadDups(); } else { openDir(state.dirId, { keepSel: true }); }
  }

  async function loadDups() {
    if (state.scanId === null) return;
    const seq = ++state.dupSeq;
    const scanId = state.scanId;
    try {
      const data = await api('dups', { scan: scanId, size: state.sizeMode, offset: 0, limit: DUP_PAGE });
      if (seq !== state.dupSeq) return;
      const sameScan = state.dup.scanId === scanId;
      state.dup = {
        scanId, groups: data.groups || [], total: data.total || 0, check: data.check, result: data.result,
        open: sameScan ? state.dup.open : new Set(), loaded: true,
      };
      renderDups();
      if (!sameScan) $('dupWrap').scrollTop = 0;
    } catch (e) {
      if (seq !== state.dupSeq) return;
      if (e.code === 'scan_not_found') {
        toast('该扫描结果已被删除，已切换到最新结果');
        await loadScans().catch(() => {});
        const latest = doneScans()[0];
        if (latest) { state.scanId = latest.id; state.dirId = 0; renderScanSelect(); return loadDups(); }
        state.scanId = null;
        return renderEmpty();
      }
      showError(e);
    }
  }

  async function loadMoreDups() {
    const btn = $('btnDupMore');
    btn.disabled = true;
    try {
      const data = await api('dups', { scan: state.scanId, size: state.sizeMode, offset: state.dup.groups.length, limit: DUP_PAGE });
      state.dup.groups = state.dup.groups.concat(data.groups || []);
      renderDupRows();
    } catch (e) {
      showError(e);
    } finally {
      btn.disabled = false;
    }
  }

  async function startDupCheck() {
    try {
      await api('dup_start', { scan: state.scanId }, { method: 'POST' });
      toast('重复检查已开始');
      state.running = true;
      state.progress = { job: 'dupcheck', scanId: state.scanId, phase: 'prepare', startedAt: Date.now() / 1000 };
      renderProgress();
      renderDups();
      schedulePoll(800);
    } catch (e) {
      if (e.code === 'task_running') { toast('已有任务正在进行，请等它结束后再检查'); schedulePoll(0); return; }
      showError(e);
    }
  }

  const reclaimOf = (x) => (state.sizeMode === 'apparent' ? x.reclaimASize : x.reclaimDSize);
  const dupRunningHere = () => state.running && isDupJob(state.progress) && state.progress.scanId === state.scanId;

  function renderDups() {
    renderDupBar();
    renderDupRows();
  }

  function renderDupBar() {
    const bar = $('dupBar');
    const { check, result } = state.dup;
    if (!state.dup.loaded) { bar.replaceChildren(); return; }
    const parts = [];
    const busy = state.running;
    const again = (text) => el('button', {
      class: 'btn small', type: 'button', text, disabled: busy,
      title: busy ? '已有扫描或重复检查正在进行' : '', onclick: startDupCheck,
    });
    if (dupRunningHere()) {
      parts.push(el('span', { text: '正在检查重复项，进度见上方。' + (result ? '下面是上一次的结果。' : '') }));
    } else if (check && (check.status === 'failed' || check.status === 'canceled')) {
      parts.push(el('span', { class: 'warn', text: check.status === 'canceled'
        ? '上次检查已取消，已经算过的文件下次不会重读。' + (result ? '下面是更早一次的结果。' : '')
        : `上次检查失败：${check.errorMessage || '未知错误'}` + (result ? '。下面是更早一次的结果。' : '') }));
    } else if (check && check.status === 'running') {
      parts.push(el('span', { class: 'warn', text: '上次检查没有正常结束。' }));
    }
    if (result && !dupRunningHere()) {
      const summary = el('span', {},
        result.groups ? el('b', { text: `${fmtNum(result.groups)} 组重复，可释放 ${fmtSize(reclaimOf(result))}` }) : el('b', { text: '没有发现重复项' }),
        ` · 检查于 ${fmtTime(result.finishedAt)}`,
        result.changed ? ` · ${fmtNum(result.changed)} 个文件扫描后有变化，未参与比较` : '',
        result.errors ? ` · ${fmtNum(result.errors)} 个文件读取出错` : '');
      parts.push(summary);
    }
    if (result || check) parts.push(again('重新检查'));
    bar.replaceChildren(...parts);
    bar.classList.toggle('hidden', !parts.length || state.view !== 'dups');
  }

  function dupMemberRows(g) {
    const rows = [];
    if (g.kind === KIND.DIR) rows.push(el('tr', { class: 'member-note' }, el('td', { colspan: 4, text: `每份 ${fmtNum(g.files)} 个文件，文件名、目录结构和内容都相同` })));
    for (const m of g.members) {
      const tags = [];
      if (m.covered) tags.push(el('span', { class: 'tag', text: '位于重复文件夹内', title: '这一份所在的上级文件夹本身就有完全相同的副本' }));
      if (m.badName) tags.push(el('span', { class: 'tag warn', text: '名称编码异常' }));
      rows.push(el('tr', { class: 'member' }, el('td', { colspan: 4 },
        el('div', { class: 'mpath' },
          el('span', { class: 't', title: m.path }, el('bdi', { dir: 'ltr', text: m.path })),
          tags,
          el('button', { class: 'btn small jump', type: 'button', text: '↗ 查看位置', title: '在空间占用视图中打开所在目录', onclick: (ev) => { ev.stopPropagation(); jumpTo(m); } })))));
    }
    if (g.copies > g.members.length) {
      rows.push(el('tr', { class: 'member-note' }, el('td', { colspan: 4, text: `还有 ${fmtNum(g.copies - g.members.length)} 份未列出` })));
    }
    return rows;
  }

  function renderDupRows() {
    const tbody = $('dupRows');
    const st = $('dupState');
    const { groups, total, check, result } = state.dup;
    tbody.replaceChildren();
    if (!result) {
      $('dupList').classList.add('hidden');
      $('dupMoreWrap').classList.add('hidden');
      st.replaceChildren();
      if (dupRunningHere()) {
        st.append(el('h3', { text: '正在检查重复项' }), el('p', { text: '检查完成后会自动显示结果。检查以最低优先级运行，不影响 NAS 的正常使用；可以随时取消，已经算过的文件下次不会重读。' }));
      } else {
        st.append(el('h3', { text: check ? '这次扫描还没有检查结果' : '还没有检查过重复项' }),
          el('p', { text: '重复检查会读取大小相同的文件的内容，并用完整的校验值确认是否完全相同。第一次检查可能需要数小时，以最低优先级运行，不影响 NAS 的正常使用；之后再检查只会读取新增或改动过的文件。' }),
          el('button', { class: 'btn primary', type: 'button', text: '开始重复检查', disabled: state.running, onclick: startDupCheck }),
          state.running ? el('p', { class: 'muted', text: '已有扫描或重复检查正在进行，结束后才能开始。' }) : '');
      }
      st.classList.remove('hidden');
      return;
    }
    if (!groups.length) {
      $('dupList').classList.add('hidden');
      $('dupMoreWrap').classList.add('hidden');
      st.replaceChildren(el('h3', { text: '没有发现重复项' }), el('p', { text: '这次扫描中没有内容完全相同的文件或文件夹（空文件、硬链接和缩略图等系统文件不计入）。' }));
      st.classList.remove('hidden');
      return;
    }
    st.classList.add('hidden');
    $('dupList').classList.remove('hidden');
    const frag = document.createDocumentFragment();
    for (const g of groups) {
      const open = state.dup.open.has(g.id);
      const tags = [];
      if (g.badName) tags.push(el('span', { class: 'tag warn', text: '名称编码异常' }));
      const tr = el('tr', { class: 'group' + (open ? ' open' : ''), 'aria-expanded': open },
        el('td', { class: 'c-name' }, el('div', { class: 'name', title: g.members[0] ? g.members[0].path : g.name },
          el('span', { class: 'caret', 'aria-hidden': 'true', text: '▶' }), icon(g.kind), el('span', { class: 't', text: g.name }), tags)),
        el('td', { class: 'c-copies', text: fmtNum(g.copies) }),
        el('td', { class: 'c-size', text: fmtSize(sizeOf(g)) }),
        el('td', { class: 'c-reclaim', text: fmtSize(state.sizeMode === 'apparent' ? g.rasize : g.rdsize) }));
      tr.addEventListener('click', () => {
        if (state.dup.open.has(g.id)) state.dup.open.delete(g.id); else state.dup.open.add(g.id);
        renderDupRows();
      });
      frag.append(tr);
      if (open) frag.append(...dupMemberRows(g));
    }
    tbody.append(frag);
    const remaining = total - groups.length;
    $('dupMoreWrap').classList.toggle('hidden', remaining <= 0);
    $('btnDupMore').textContent = `加载更多（剩余 ${fmtNum(remaining)} 组）`;
  }

  function jumpTo(m) {
    setView('space', { load: false });
    openDir(m.parentId, { selectId: m.entryId });
  }

  function renderDupProgress(p, elapsed, bar) {
    const phase = PHASE_TEXT[p.phase] || '准备中';
    let pct = null;
    if (p.totalBytes > 0) pct = (p.doneBytes / p.totalBytes) * 100;
    else if (p.totalFiles > 0) pct = (p.doneFiles / p.totalFiles) * 100;
    if (pct !== null) {
      pct = Math.min(100, pct);
      bar.classList.remove('indeterminate');
      bar.style.width = pct.toFixed(1) + '%';
    } else {
      bar.classList.add('indeterminate');
    }
    $('progTitle').textContent = `正在检查重复项 · ${phase}` + (pct !== null ? ` ${pct.toFixed(0)}%` : '…');
    const stats = [];
    if (p.phase === 'quick' || p.phase === 'full') {
      stats.push(`已读 ${fmtSize(p.doneBytes || 0)} / ${fmtSize(p.totalBytes || 0)}`, `${fmtNum(p.doneFiles)} / ${fmtNum(p.totalFiles)} 个文件`);
    } else if (p.phase === 'prepare' && p.totalFiles) {
      stats.push(`${fmtNum(p.doneFiles)} / ${fmtNum(p.totalFiles)} 个文件`);
    }
    if (p.cacheHits) stats.push(`缓存命中 ${fmtNum(p.cacheHits)}`);
    if (p.changed) stats.push(`扫描后有变化 ${fmtNum(p.changed)}`);
    if (p.errors) stats.push(`读取出错 ${fmtNum(p.errors)}`);
    stats.push(`用时 ${fmtDuration(elapsed)}`);
    $('progStats').textContent = stats.join(' · ');
    const cur = p.phase === 'quick' || p.phase === 'full' ? (p.current || '') : '';
    $('progPath').textContent = cur;
    $('progPath').title = cur;
  }

  function dupCell(s) {
    const d = s.dup;
    if (!d) return s.status === 'done' ? el('span', { class: 'muted', text: '—' }) : '';
    if (d.status === 'done') return el('span', { class: 'num', text: d.groups ? `可释放 ${fmtSize(d.reclaimDSize)}` : '无重复' });
    const text = { running: '进行中', failed: '失败', canceled: '已取消' }[d.status] || d.status;
    return el('span', { class: 'status ' + d.status, text, title: d.errorMessage || '' });
  }

  // ---------- controls ----------
  function selectScan(id) {
    state.scanId = id;
    state.dirId = 0;
    renderScanSelect();
    showCurrent();
  }

  // showCurrent shows the current scan in the current view.
  function showCurrent() {
    if (state.view === 'dups') { writeHash(); return loadDups(); }
    return openDir(state.dirId);
  }

  function setSizeMode(mode) {
    state.sizeMode = mode;
    for (const b of document.querySelectorAll('#sizeSeg button')) {
      const on = b.dataset.size === mode;
      b.classList.toggle('on', on);
      b.setAttribute('aria-checked', on);
    }
  }

  function bind() {
    $('btnNewScan').addEventListener('click', startScan);
    $('btnCancel').addEventListener('click', cancelScan);
    $('btnHistory').addEventListener('click', showHistory);
    $('btnSettings').addEventListener('click', showSettings);
    $('btnMore').addEventListener('click', loadMore);
    $('dlgClose').addEventListener('click', closeDialog);
    $('overlay').addEventListener('click', (e) => { if (e.target === $('overlay')) closeDialog(); });
    $('scanSelect').addEventListener('change', (e) => selectScan(Number(e.target.value)));
    for (const b of document.querySelectorAll('#sizeSeg button')) {
      b.addEventListener('click', () => {
        if (state.sizeMode === b.dataset.size) return;
        setSizeMode(b.dataset.size);
        if (state.scanId === null) return;
        if (state.view === 'dups') loadDups();
        else openDir(state.dirId, { keepSel: true });
      });
    }
    for (const b of document.querySelectorAll('#viewSeg button')) {
      b.addEventListener('click', () => {
        if (state.view === b.dataset.view || state.scanId === null) return;
        setView(b.dataset.view);
      });
    }
    $('btnDupMore').addEventListener('click', loadMoreDups);
    for (const th of document.querySelectorAll('#list th')) {
      th.addEventListener('click', () => {
        const key = th.dataset.sort;
        if (state.sort === key) state.order = state.order === 'desc' ? 'asc' : 'desc';
        else { state.sort = key; state.order = key === 'name' ? 'asc' : 'desc'; }
        if (state.scanId !== null) openDir(state.dirId);
      });
    }
    document.addEventListener('keydown', (e) => {
      if (!$('overlay').classList.contains('hidden')) {
        if (e.key === 'Escape') closeDialog();
        return;
      }
      if (e.target.closest && e.target.closest('input, select, textarea')) return;
      if (state.view !== 'space') return;
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        if (!state.items.length) return;
        e.preventDefault();
        state.sel = e.key === 'ArrowDown' ? Math.min(state.items.length - 1, state.sel + 1) : Math.max(0, state.sel - 1);
        highlight();
      } else if (e.key === 'Enter' || e.key === 'ArrowRight') {
        const it = state.items[state.sel];
        if (it && it.kind === KIND.DIR && it.hasChildren) { e.preventDefault(); openDir(it.id); }
      } else if (e.key === 'Backspace' || e.key === 'ArrowLeft') {
        if (state.dirId !== 0) { e.preventDefault(); goUp(); }
      }
    });
  }

  async function init() {
    bind();
    try {
      state.settings = await api('settings');
      setSizeMode(state.settings.defaultSizeMode || 'disk');
      await loadScans();
    } catch (e) {
      return showError(e);
    }
    await pollStatus();
    const done = doneScans();
    if (!done.length) return renderEmpty();
    const h = readHash();
    const fromHash = h.scan !== null && done.some((s) => s.id === h.scan);
    state.scanId = fromHash ? h.scan : done[0].id;
    state.dirId = fromHash ? h.dir : 0;
    renderScanSelect();
    if (fromHash && h.view === 'dups') return setView('dups');
    await openDir(state.dirId);
  }

  init();
})();
