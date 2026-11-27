(() => {
  'use strict';

  const SKIP_REASONS = new Set(['not_mounted', 'not_share_volume', 'not_found', 'not_directory', 'read_error']);
  const LINUX = I18N.platform === 'linux';
  const KIND = { FILE: 0, DIR: 1, SYMLINK: 2, OTHER: 3, MOUNT: 4 };
  const FLAG = { ERROR: 1, HARDLINK_DUP: 2, BAD_NAME: 4 };
  const PAGE = 500;
  const { t } = I18N;
  const WEEKDAYS = t('weekdays');

  const $ = (id) => document.getElementById(id);
  const el = (tag, attrs = {}, ...kids) => {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null || v === false) continue;
      if (k === 'class') n.className = v;
      else if (k === 'text') n.textContent = v;
      else if (k.startsWith('on')) n.addEventListener(k.slice(2), v);
      else if (k === 'style') n.style.cssText = v;
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
    previewLoaded: false,
    volSeq: 0,
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
    deleteEnabled: false,
    basket: new Map(), // entryId -> { entryId, path, kind, dsize, asize, items, name }
    activeDeleteJobId: null,
    activeDeleteTargets: [],
    seenDeleteJobId: null,
    statusLoaded: false,
  };
  const DUP_PAGE = 100;
  const PHASES = new Set(['prepare', 'quick', 'full', 'rollup', 'write']);

  // ---------- formatting ----------
  function fmtSize(b) {
    if (b === null || b === undefined) return '';
    if (b < 1024) return b + ' B';
    const u = ['KB', 'MB', 'GB', 'TB', 'PB'];
    let v = b / 1024, i = 0;
    while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return (v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2)) + ' ' + u[i];
  }
  const fmtNum = I18N.num;
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
    if (h) return t('dur.h', { h, m });
    if (m) return t('dur.m', { m, s: r });
    return t('dur.s', { s: r });
  }
  const sizeOf = (it) => (state.sizeMode === 'apparent' ? it.asize : it.dsize);
  const triggerText = (trigger) => t(trigger === 'schedule' ? 'trigger.schedule' : 'trigger.manual');
  const STATUSES = new Set(['done', 'failed', 'canceled', 'running']);
  const statusText = (status) => (STATUSES.has(status) ? t('status.' + status) : status);

  // ---------- API ----------
  class ApiError extends Error {
    constructor(status, code, message, data) { super(message || code); this.status = status; this.code = code; this.data = data; }
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
      throw new ApiError(0, 'network', t('net.down'));
    }
    let data = null;
    const text = await res.text();
    if (text) {
      try { data = JSON.parse(text); } catch { throw new ApiError(res.status, 'bad_response', t('net.badResponse')); }
    }
    if (!res.ok) throw new ApiError(res.status, data && data.error, errorText(data), data);
    return data;
  }

  // errorText shows a known API error code in the UI language; the server's
  // own message is the fallback and fills {detail} where one is expected.
  const ERROR_CODES = new Set(['not_logged_in', 'not_admin', 'auth_unavailable', 'task_running', 'not_running',
    'scan_running', 'scan_not_found', 'no_scans', 'dir_not_found', 'invalid_settings', 'internal', 'bad_password',
    'delete_disabled', 'not_latest_scan', 'protected_path', 'docker_confirm_required', 'entry_not_found', 'bad_delete_request']);
  function errorText(data) {
    if (!data) return undefined;
    if (data.error === 'too_many_attempts') return t('err.too_many_attempts', { min: Math.max(1, Math.ceil((data.retryAfter || 60) / 60)) });
    if (ERROR_CODES.has(data.error)) return t('err.' + data.error, { detail: data.message || '' });
    return data.message;
  }

  // ---------- toast ----------
  let toastTimer;
  function toast(msg) {
    const box = $('toast');
    box.textContent = msg;
    box.classList.remove('hidden');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => box.classList.add('hidden'), 3200);
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
    svg.setAttribute('width', '16');
    svg.setAttribute('height', '16');
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
  const latestDoneScanId = () => {
    const done = doneScans();
    return done.length ? done[0].id : null;
  };
  const canDelete = () => state.deleteEnabled && state.scanId !== null && state.scanId === latestDoneScanId();

  // ---------- delete basket ----------
  function joinDisplayPath(base, seg) {
    if (!base) return seg;
    return base.endsWith('/') ? base + seg : base + '/' + seg;
  }
  function currentRootId() {
    return state.path.length ? state.path[0].id : null;
  }
  function currentDirDisplayPath() {
    let p = '';
    for (const seg of state.path) p = joinDisplayPath(p, seg.name);
    return p;
  }
  function currentDisplayPath(name) {
    return joinDisplayPath(currentDirDisplayPath(), name);
  }
  function isPathUnder(child, parent) {
    if (!parent || !child || child.length <= parent.length) return false;
    const pfx = parent.endsWith('/') ? parent : parent + '/';
    return child.startsWith(pfx);
  }
  function inferRootId(displayPath) {
    if (!displayPath || !state.volumes.length) return null;
    let best = null;
    let bestLen = -1;
    for (const v of state.volumes) {
      if (!v.name) continue;
      if (displayPath === v.name || isPathUnder(displayPath, v.name)) {
        if (v.name.length > bestLen) {
          best = v.entryId;
          bestLen = v.name.length;
        }
      }
    }
    return best;
  }
  function basketRootId(b) {
    if (b.rootId !== null && b.rootId !== undefined) return b.rootId;
    const id = inferRootId(b.path);
    if (id !== null) b.rootId = id;
    return id;
  }
  function sameVolume(b, rootId) {
    if (rootId === null || rootId === undefined) return true;
    const br = basketRootId(b);
    if (br === null || br === undefined) return true;
    return br === rootId;
  }
  function basketCovers(entryId, displayPath, rootId = null) {
    if (state.basket.has(entryId)) return 'direct';
    for (const b of state.basket.values()) {
      if (b.kind === KIND.DIR && sameVolume(b, rootId) && isPathUnder(displayPath, b.path)) return 'inherited';
    }
    return null;
  }
  function basketDescendantStats(displayPath, rootId = null) {
    let count = 0;
    let dsize = 0;
    let asize = 0;
    for (const b of state.basket.values()) {
      if (!sameVolume(b, rootId)) continue;
      if (isPathUnder(b.path, displayPath)) {
        count++;
        if (!b.hardlinkDup) {
          dsize += b.dsize || 0;
          asize += b.asize || 0;
        }
      }
    }
    return { count, dsize, asize, size: state.sizeMode === 'apparent' ? asize : dsize };
  }
  function basketTotalNonHardlink() {
    let dsize = 0;
    let asize = 0;
    for (const b of state.basket.values()) {
      if (!b.hardlinkDup) {
        dsize += b.dsize || 0;
        asize += b.asize || 0;
      }
    }
    return { dsize, asize, size: state.sizeMode === 'apparent' ? asize : dsize };
  }
  function addToBasket(entry) {
    const rId = entry.rootId !== undefined && entry.rootId !== null ? entry.rootId : inferRootId(entry.path);
    entry.rootId = rId;
    if (basketCovers(entry.entryId, entry.path, rId)) return;
    let folded = 0;
    if (entry.kind === KIND.DIR) {
      for (const [id, b] of Array.from(state.basket.entries())) {
        if (sameVolume(b, rId) && isPathUnder(b.path, entry.path)) {
          state.basket.delete(id);
          folded++;
        }
      }
    }
    state.basket.set(entry.entryId, entry);
    if (folded > 0) toast(t('basket.folded', { n: folded }));
    renderBasketBar();
  }
  function removeFromBasket(entryId) {
    state.basket.delete(entryId);
    renderBasketBar();
  }
  function renderSpaceBasketSync() {
    if (state.scanId === null || !state.dir) return;
    renderCrumbs();
    renderVolumes();
    renderHead();
    renderRows();
  }
  function clearBasket() {
    state.basket.clear();
    renderBasketBar();
    if (state.view === 'dups') renderDupRows();
    else renderSpaceBasketSync();
  }
  function renderBasketBar() {
    const bar = $('basketBar');
    if (!bar) return;
    if (!canDelete() || state.basket.size === 0) {
      bar.classList.add('hidden');
      bar.replaceChildren();
      return;
    }
    let totalSize = 0;
    for (const b of state.basket.values()) totalSize += sizeOf(b);
    const busy = state.running;
    bar.replaceChildren(
      el('div', { class: 'basket-info', text: t('basket.summary', { n: state.basket.size, size: fmtSize(totalSize) }) }),
      el('div', { class: 'basket-actions' },
        el('button', { class: 'btn small', type: 'button', text: t('basket.clear'), disabled: busy, onclick: clearBasket }),
        el('button', {
          class: 'btn small danger-primary', type: 'button', text: t('basket.review'), disabled: busy,
          title: busy ? t('delFile.busyTitle') : '',
          onclick: () => openDeleteConfirm(Array.from(state.basket.keys()), { fromBasket: true }),
        })));
    bar.classList.remove('hidden');
  }

  function renderScanSelect() {
    const sel = $('scanSelect');
    sel.replaceChildren();
    const done = doneScans();
    if (!done.length) {
      sel.append(el('option', { text: t('scans.none'), value: '' }));
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
      state.volSeq++;
      state.previewLoaded = false;
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
        toast(t(e.code === 'scan_not_found' ? 'notice.scanDeleted' : 'notice.dirGone'));
        await loadScans().catch(() => {});
        const latest = doneScans()[0];
        if (latest && (e.code === 'scan_not_found' ? latest.id !== state.scanId : true)) {
          state.scanId = latest.id;
          renderScanSelect();
          return showCurrent();
        }
        if (!latest) { state.scanId = null; return renderEmpty({ reloadVolumes: true }); }
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
      renderHead();
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
  const DK_TYPES = new Set(['image_layer', 'container_rw', 'container_init', 'container', 'build_cache', 'volume_anon', 'volume_named', 'internal', 'orphan']);
  const DK_STATUSES = new Set(['running', 'exited', 'created', 'paused', 'restarting', 'dead']);
  const DK_CACHES = new Set(['exec.cachemount', 'source.local', 'source.git.checkout', 'frontend', 'regular']);
  const DK_NOTES = new Set(['l', 'backingFsBlockDev', 'metadata.db', 'missing_config']);
  const DK_WARNS = new Set(['images', 'containers', 'buildkit', 'volumes']);

  const isDockerEntry = (dk) => !!dk && DK_TYPES.has(dk.type);
  const dkStatusText = (s) => (DK_STATUSES.has(s) ? t('dk.status.' + s) : s || '');
  const dkCacheText = (c) => (DK_CACHES.has(c) ? t('dk.cache.' + c) : c || '');
  const dkNoteText = (n) => (DK_NOTES.has(n) ? t('dk.note.' + n) : n || '');

  function dkTagClass(type) {
    if (type === 'container' || type === 'container_rw' || type === 'container_init') return 'tag dk dk-container';
    if (type === 'image_layer') return 'tag dk dk-image';
    if (type === 'build_cache') return 'tag dk dk-build';
    if (type === 'volume_anon' || type === 'volume_named') return 'tag dk dk-volume';
    if (type === 'orphan') return 'tag dk dk-orphan';
    return 'tag dk dk-internal';
  }

  function dkPrimaryText(dk, rawName) {
    switch (dk.type) {
      case 'image_layer':
        if (dk.count > 1 && dk.label) return t('dk.moreImages', { name: dk.label, n: dk.count });
        return dk.label || '';
      case 'container':
      case 'container_rw':
      case 'container_init': {
        const st = dk.status ? dkStatusText(dk.status) : '';
        return st ? `${dk.label || ''} · ${st}` : (dk.label || '');
      }
      case 'build_cache': {
        const sub = dk.cacheType && dk.cacheType !== 'regular' ? dkCacheText(dk.cacheType) : '';
        if (sub && dk.label) return `${sub} · ${dk.label}`;
        return dk.label || sub || '';
      }
      case 'volume_anon':
        if (dk.mounts && dk.mounts.length > 1) return t('dk.moreMounts', { name: dk.mounts[0].container, n: dk.mounts.length });
        if (dk.mounts && dk.mounts.length === 1) return `${dk.mounts[0].container} → ${dk.mounts[0].destination}`;
        if (dk.unmounted) return t('dk.unmounted');
        return '';
      case 'volume_named': {
        let extra = '';
        if (dk.mounts && dk.mounts.length > 1) extra = t('dk.moreMounts', { name: dk.mounts[0].container, n: dk.mounts.length });
        else if (dk.mounts && dk.mounts.length === 1) extra = `${dk.mounts[0].container} → ${dk.mounts[0].destination}`;
        else if (dk.unmounted) extra = t('dk.unmounted');
        return extra ? `${rawName} · ${extra}` : rawName;
      }
      case 'internal':
        return dk.note ? dkNoteText(dk.note) : '';
      default:
        return '';
    }
  }

  function dkCrumbText(p) {
    const dk = p.docker;
    if (!isDockerEntry(dk) || dk.type === 'volume_named' || !dk.label) return p.name;
    const shortRaw = p.name.length > 12 ? p.name.slice(0, 12) + '…' : p.name;
    return `${dk.label} (${shortRaw})`;
  }

  function renderDirNotice() {
    const box = $('dirNotice');
    if (!box) return;
    const ws = state.dir && state.dir.docker && Array.isArray(state.dir.docker.warnings) ? state.dir.docker.warnings : [];
    if (!ws.length || state.view !== 'space') {
      box.classList.add('hidden');
      box.textContent = '';
      return;
    }
    const parts = ws.map((w) => (DK_WARNS.has(w) ? t('dk.warn.' + w) : w)).join(t('skip.sep'));
    box.textContent = t('dk.warn.banner', { parts });
    box.classList.remove('hidden');
  }

  async function copyText(str) {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(str);
        toast(t('dk.dlg.copied'));
        return;
      }
    } catch { /* fallback below */ }
    const ta = el('textarea', { style: 'position:fixed;opacity:0', text: str });
    document.body.append(ta);
    ta.select();
    try { document.execCommand('copy'); toast(t('dk.dlg.copied')); } catch { /* ignore */ }
    ta.remove();
  }

  function showDockerDetail(it) {
    const dk = it.docker;
    if (!dk) return;
    const dl = el('dl', { class: 'dk-dl' });
    const row = (label, ...val) => dl.append(el('dt', { text: label }), el('dd', {}, ...val));

    row(t('dk.dlg.type'), el('span', { class: dkTagClass(dk.type), text: t('dk.type.' + dk.type) }));
    row(t('dk.dlg.rawName'), el('div', { class: 'dk-raw-row' },
      el('code', { class: 'dk-inline-code', text: it.name }),
      el('button', { class: 'btn small', type: 'button', text: t('dk.dlg.copy'), onclick: () => copyText(it.name) })));

    if (dk.container) {
      const c = dk.container;
      row(t('dk.dlg.container'), `${c.name} (${c.id})`);
      if (c.image) row(t('dk.dlg.containerImg'), el('code', { class: 'dk-inline-code', text: c.image }));
      if (c.status) row(t('dk.dlg.status'), dkStatusText(c.status));
    }
    const comp = dk.compose || (dk.container && dk.container.compose);
    if (comp && (comp.project || comp.service || comp.volume)) {
      const parts = [comp.project, comp.service || comp.volume].filter(Boolean);
      row(t('dk.dlg.compose'), el('code', { class: 'dk-inline-code', text: parts.join(' / ') }));
    }
    if (dk.images && dk.images.length) {
      const ul = el('ul', { class: 'dk-list' });
      for (const img of dk.images) {
        const badges = [];
        if (img.id) badges.push(el('code', { class: 'dk-inline-code', text: img.id }));
        if (img.inUse) badges.push(el('span', { class: 'tag dk dk-container', text: t('dk.inUse') }));
        if (img.dangling) badges.push(el('span', { class: 'tag dk dk-orphan', text: t('dk.dangling') }));
        ul.append(el('li', {}, el('span', { text: img.name }), badges));
      }
      const total = dk.count || dk.images.length;
      const extra = total > dk.images.length
        ? el('div', { class: 'muted', style: 'margin-top:6px;font-size:12px', text: t('dk.dlg.imagesTrunc', { shown: dk.images.length, total }) })
        : '';
      row(t('dk.dlg.images', { n: total }), ul, extra);
    }
    if (dk.createdBy) row(t('dk.dlg.createdBy'), el('pre', { class: 'dk-code', text: dk.createdBy }));
    if (dk.cacheType) row(t('dk.dlg.cacheType'), dkCacheText(dk.cacheType) + (DK_CACHES.has(dk.cacheType) ? ` (${dk.cacheType})` : ''));
    if (dk.buildDesc) row(t('dk.dlg.buildDesc'), el('pre', { class: 'dk-code', text: dk.buildDesc }));
    if (dk.driver) row(t('dk.dlg.driver'), dk.driver);
    if (dk.mounts && dk.mounts.length) {
      const ul = el('ul', { class: 'dk-list' });
      for (const m of dk.mounts) {
        const badges = [el('span', { class: 'tag', text: t(m.rw ? 'dk.rw' : 'dk.ro') })];
        if (m.status) badges.push(el('span', { class: 'tag', text: dkStatusText(m.status) }));
        ul.append(el('li', {}, el('span', { text: m.container }), ' → ', el('code', { class: 'dk-inline-code', text: m.destination }), badges));
      }
      row(t('dk.dlg.mounts'), ul);
    } else if (dk.unmounted) {
      row(t('dk.dlg.mounts'), el('span', { class: 'muted', text: t('dk.unmounted') }));
    }
    if (dk.labels && Object.keys(dk.labels).length) {
      const ul = el('ul', { class: 'dk-list' });
      for (const [k, v] of Object.entries(dk.labels).sort()) {
        ul.append(el('li', {}, el('code', { class: 'dk-inline-code', text: v ? `${k}=${v}` : k })));
      }
      row(t('dk.dlg.labels'), ul);
    }
    if (dk.note) row(t('dk.dlg.note'), dkNoteText(dk.note));
    if (dk.type === 'orphan') row(t('dk.dlg.note'), el('p', { class: 'dk-note', text: t('dk.dlg.orphanNote') }));

    openDialog(t('dk.dlg.title'), dl, [el('button', { class: 'btn', type: 'button', text: t('dlg.close'), onclick: closeDialog })]);
  }

  function render() {
    $('state').classList.add('hidden');
    $('list').classList.remove('hidden');
    renderCrumbs();
    renderVolumes();
    renderDirNotice();
    renderHead();
    renderRows();
    renderBasketBar();
  }

  function renderCrumbs() {
    const ol = $('crumbs');
    ol.replaceChildren(el('li', {}, el('a', { text: t('crumbs.all'), onclick: () => openDir(0) })));
    for (const p of state.path) {
      const label = dkCrumbText(p);
      const title = isDockerEntry(p.docker) && p.docker.label ? `${p.docker.label} — ${p.name}` : p.name;
      ol.append(el('li', {}, el('a', { text: label, title, onclick: () => openDir(p.id) })));
    }
    const d = state.dir;
    const sumEl = $('dirSummary');
    if (!d) { sumEl.textContent = ''; return; }
    const totalSize = sizeOf(d);
    let dirStaged = 0;
    if (canDelete() && state.basket.size > 0) {
      if (state.dirId === 0) {
        dirStaged = Math.min(totalSize, basketTotalNonHardlink().size);
      } else {
        const curPath = currentDirDisplayPath();
        const curRoot = currentRootId();
        if (basketCovers(state.dirId, curPath, curRoot)) dirStaged = totalSize;
        else dirStaged = Math.min(totalSize, basketDescendantStats(curPath, curRoot).size);
      }
    }
    const baseText = t('crumbs.summary', { size: fmtSize(totalSize), n: d.items || 0 });
    if (dirStaged > 0) {
      sumEl.replaceChildren(
        baseText,
        ' · ',
        el('span', {
          class: 'summary-del',
          text: t('crumbs.staged', { size: fmtSize(dirStaged) }),
          title: t('crumbs.staged.title', { remain: fmtSize(Math.max(0, totalSize - dirStaged)) }),
        }));
    } else {
      sumEl.textContent = baseText;
    }
  }

  function renderVolumes() {
    const box = $('volumes');
    const preview = state.scanId === null;
    if ((!preview && state.dirId !== 0) || (!state.volumes.length && !state.skipped.length)) {
      box.classList.add('hidden');
      return;
    }
    box.replaceChildren();
    const byId = preview ? null : new Map(state.items.map((i) => [i.id, i]));
    for (const v of state.volumes) {
      const total = v.fsTotal || 0;
      const used = v.fsUsed || 0;
      const pct = (x) => (total ? Math.min(100, (x / total) * 100) : 0) + '%';
      const subtitle = v.fsType
        ? t('vol.fsDev', { fstype: v.fsType, device: v.device || '' })
        : v.pool ? t('vol.poolMount', { pool: String(v.pool), mount: v.mountPoint }) : v.mountPoint;
      let bar, legend;
      if (preview) {
        bar = el('div', { class: 'cap', title: t('vol.bar.previewTitle') },
          el('span', { class: 's', style: `width:${pct(used)}` }));
        legend = el('div', { class: 'legend' },
          el('span', {}, el('i', { style: 'background:var(--bar)' }), t('vol.used', { size: fmtSize(used) })),
          el('span', {}, el('i', { style: 'background:var(--free)' }), t('vol.free', { size: fmtSize(v.fsFree) })));
      } else {
        const item = byId.get(v.entryId);
        const scanned = item ? item.dsize : 0;
        const unacc = Math.max(0, used - scanned);
        const scannedCap = Math.min(scanned, used || scanned);
        const desc = canDelete() && state.basket.size > 0 ? basketDescendantStats(v.name, v.entryId) : null;
        const stagedCap = desc ? Math.min(scannedCap, desc.dsize) : 0;
        const keepCap = Math.max(0, scannedCap - stagedCap);
        bar = el('div', { class: 'cap', title: t(stagedCap > 0 ? 'vol.bar.stagedTitle' : 'vol.bar.title') },
          keepCap > 0 || stagedCap === 0 ? el('span', { class: 's', style: `width:${pct(keepCap)}` }) : false,
          stagedCap > 0 ? el('span', { class: 'd', style: `width:${pct(stagedCap)}` }) : false,
          el('span', { class: 'u', style: `width:${pct(unacc)}` }));
        legend = el('div', { class: 'legend' },
          el('span', {}, el('i', { style: 'background:var(--bar)' }), t('vol.scanned', { size: fmtSize(scanned) })),
          stagedCap > 0 ? el('span', { class: 'vol-del' }, el('i', { style: 'background:var(--bar-del)' }), t('vol.staged', { size: fmtSize(stagedCap) })) : false,
          el('span', { title: t('vol.unacc.title') }, el('i', { style: 'background:var(--unaccounted)' }), t('vol.unacc', { size: fmtSize(unacc) })),
          el('span', {}, el('i', { style: 'background:var(--free)' }), t('vol.free', { size: fmtSize(v.fsFree) })));
      }
      box.append(el('div', { class: 'vol' },
        el('div', { class: 'vol-head' },
          el('div', { class: 'vol-name' }, v.name, el('small', { text: subtitle })),
          el('div', { class: 'vol-cap', text: total ? t('vol.cap', { used: fmtSize(used), total: fmtSize(total), pct: ((used / total) * 100).toFixed(1) }) : '' })),
        bar, legend));
    }
    if (state.skipped.length) {
      const why = (reason) => (SKIP_REASONS.has(reason) ? t('skip.' + reason) : reason);
      const list = state.skipped.map((s) => t('skip.item', { name: s.name, why: why(s.reason) })).join(t('skip.sep'));
      box.append(el('div', { class: 'notice', text: t('skip.list', { list }) }));
    }
    box.classList.remove('hidden');
  }

  function renderHead() {
    for (const th of document.querySelectorAll('#list th')) {
      const on = th.dataset.sort === state.sort && !th.classList.contains('c-pct');
      th.classList.toggle('sorted', on);
      th.classList.toggle('asc', on && state.order === 'asc');
    }
    const nameTh = document.querySelector('#list th.c-name');
    if (!nameTh) return;
    const oldChk = nameTh.querySelector('input.th-chk');
    if (oldChk) oldChk.remove();
    if (!canDelete() || state.dirId === 0) return;
    const deletable = state.items.filter((it) => it.kind !== KIND.MOUNT);
    if (!deletable.length) return;
    const curRoot = currentRootId();
    let covered = 0;
    let directOnly = 0;
    let partial = 0;
    for (const it of deletable) {
      const p = currentDisplayPath(it.name);
      const cov = basketCovers(it.id, p, curRoot);
      if (cov) covered++;
      if (cov === 'direct') directOnly++;
      else if (!cov && it.kind === KIND.DIR && state.basket.size > 0 && basketDescendantStats(p, curRoot).count > 0) partial++;
    }
    const allCovered = covered === deletable.length;
    const allInherited = allCovered && directOnly === 0;
    const chk = el('input', {
      type: 'checkbox', class: 'th-chk', title: t('basket.selectAll'), 'aria-label': t('basket.selectAll'),
      checked: allCovered, disabled: allInherited || state.running,
      onclick: (ev) => ev.stopPropagation(),
      onchange: (ev) => {
        ev.stopPropagation();
        if (chk.checked) {
          for (const it of deletable) {
            const p = currentDisplayPath(it.name);
            if (!basketCovers(it.id, p, curRoot)) {
              addToBasket({
                entryId: it.id, path: p, kind: it.kind, dsize: it.dsize, asize: it.asize,
                items: it.items, name: it.name, rootId: curRoot, hardlinkDup: !!(it.flags & FLAG.HARDLINK_DUP),
              });
            }
          }
        } else {
          for (const it of deletable) state.basket.delete(it.id);
          renderBasketBar();
        }
        renderSpaceBasketSync();
      },
    });
    chk.indeterminate = (covered > 0 || partial > 0) && !allCovered;
    nameTh.prepend(chk);
  }

  function rowFor(it, idx, parentSize) {
    const isRoot = state.dirId === 0;
    const size = sizeOf(it);
    const pctVal = parentSize > 0 ? (size / parentSize) * 100 : 0;
    const enterable = (it.kind === KIND.DIR) && it.hasChildren;
    const allowDel = canDelete() && !isRoot && it.kind !== KIND.MOUNT;
    const rowRootId = isRoot ? it.id : currentRootId();
    const dispPath = isRoot ? it.name : currentDisplayPath(it.name);
    const cov = allowDel ? basketCovers(it.id, dispPath, rowRootId) : null;
    const desc = (canDelete() && !cov && it.kind === KIND.DIR && state.basket.size > 0)
      ? basketDescendantStats(dispPath, rowRootId) : null;
    const isPartial = !!(desc && desc.count > 0);
    const stagedSize = it.kind === KIND.MOUNT ? 0 : cov ? size : (desc ? Math.min(size, desc.size) : 0);
    const tags = [];
    if (it.kind === KIND.MOUNT) tags.push(el('span', { class: 'tag', text: t('tag.mount') }));
    if (it.kind === KIND.SYMLINK) tags.push(el('span', { class: 'tag', text: t('tag.symlink') }));
    if (it.flags & FLAG.HARDLINK_DUP) tags.push(el('span', { class: 'tag', text: t('tag.hardlink'), title: t('tag.hardlink.title') }));
    if (it.flags & FLAG.ERROR) tags.push(el('span', { class: 'tag warn', text: t('tag.readError') }));
    if (it.badName) tags.push(el('span', { class: 'tag warn', text: t('tag.badName'), title: t('tag.badName.title') }));
    if (cov === 'inherited') tags.push(el('span', { class: 'tag inherited', text: t('basket.inherited'), title: t('basket.inherited.title') }));
    const dk = isDockerEntry(it.docker) ? it.docker : null;
    const nameKids = [];
    if (allowDel) {
      const chk = el('input', {
        type: 'checkbox', class: 'row-chk', checked: !!cov,
        disabled: cov === 'inherited' || state.running,
        title: cov === 'inherited'
          ? t('basket.inherited.title')
          : isPartial
            ? t('basket.partial.title', { n: desc.count, size: fmtSize(stagedSize) })
            : t('basket.selectRow'),
        'aria-label': t('basket.selectRow'),
        'aria-checked': isPartial ? 'mixed' : undefined,
        onclick: (ev) => ev.stopPropagation(),
        onchange: (ev) => {
          ev.stopPropagation();
          if (ev.target.checked) {
            addToBasket({
              entryId: it.id, path: dispPath, kind: it.kind, dsize: it.dsize, asize: it.asize,
              items: it.items, name: it.name, rootId: rowRootId, hardlinkDup: !!(it.flags & FLAG.HARDLINK_DUP),
            });
          } else {
            removeFromBasket(it.id);
          }
          renderSpaceBasketSync();
        },
      });
      chk.indeterminate = isPartial;
      nameKids.push(chk);
    }
    nameKids.push(icon(it.kind, isRoot));
    let nameTitle = it.name;
    const actions = [];
    if (dk) {
      const primary = dkPrimaryText(dk, it.name);
      if (primary) nameTitle = `${primary} — ${it.name}`;
      nameKids.push(el('span', { class: dkTagClass(dk.type), text: t('dk.type.' + dk.type) }));
      if (primary) nameKids.push(el('span', { class: 'dk-label', text: primary }));
      if (dk.type !== 'volume_named') {
        nameKids.push(el('span', { class: primary ? 'dk-raw' : 't', text: it.name }));
      }
      nameKids.push(...tags);
      if (dk.type !== 'internal') {
        actions.push(el('button', {
          class: 'dk-info', type: 'button', text: t('dk.detailBtn'), title: t('dk.detailBtn.title'),
          onclick: (ev) => { ev.stopPropagation(); showDockerDetail(it); },
        }));
      }
    } else {
      nameKids.push(el('span', { class: 't', text: it.name }), ...tags);
    }
    if (allowDel) {
      actions.push(el('button', {
        class: 'row-del', type: 'button', text: t('row.delete'),
        title: state.running ? t('delFile.busyTitle') : t('row.delete.title'),
        disabled: state.running,
        onclick: (ev) => { ev.stopPropagation(); openDeleteConfirm([it.id], { fromBasket: false }); },
      }));
    }
    if (actions.length) nameKids.push(el('span', { class: 'row-actions' }, actions));

    const keepSize = Math.max(0, size - stagedSize);
    let sizeCell;
    if (it.kind === KIND.MOUNT) {
      sizeCell = el('td', { class: 'c-size', text: '—' });
    } else if (cov) {
      sizeCell = el('td', { class: 'c-size', title: t('basket.size.staged', { size: fmtSize(size) }) },
        el('span', { class: 'size-staged', text: fmtSize(size) }));
    } else if (stagedSize > 0) {
      sizeCell = el('td', {
        class: 'c-size',
        title: t('basket.size.partial', { total: fmtSize(size), staged: fmtSize(stagedSize), remain: fmtSize(keepSize) }),
      }, el('div', { class: 'size-cell' },
        el('span', { text: fmtSize(size) }),
        el('span', { class: 'size-del', text: '-' + fmtSize(stagedSize) })));
    } else {
      sizeCell = el('td', { class: 'c-size', text: fmtSize(size) });
    }

    const cappedPct = Math.min(100, pctVal);
    const keepPct = parentSize > 0 ? Math.min(cappedPct, (keepSize / parentSize) * 100) : 0;
    const delPct = Math.max(0, cappedPct - keepPct);
    const fillBase = 'fill' + (it.kind === KIND.DIR ? '' : ' file');
    const trackKids = [];
    if (it.kind !== KIND.MOUNT) {
      if (cov) {
        if (cappedPct > 0) trackKids.push(el('div', { class: fillBase + ' del', style: `width:${cappedPct.toFixed(2)}%` }));
      } else if (stagedSize > 0) {
        if (keepPct > 0) trackKids.push(el('div', { class: fillBase, style: `width:${keepPct.toFixed(2)}%` }));
        if (cappedPct > 0) trackKids.push(el('div', { class: fillBase + ' del', style: `width:${delPct.toFixed(2)}%` }));
      } else {
        trackKids.push(el('div', { class: fillBase, style: `width:${cappedPct.toFixed(2)}%` }));
      }
    }
    const pctTitle = it.kind === KIND.MOUNT
      ? undefined
      : cov
        ? t('basket.pct.staged', { pct: pctVal.toFixed(1), size: fmtSize(size) })
        : stagedSize > 0
          ? t('basket.pct.partial', { pct: pctVal.toFixed(1), delPct: delPct.toFixed(1), staged: fmtSize(stagedSize), keepPct: keepPct.toFixed(1), remain: fmtSize(keepSize) })
          : undefined;

    const tr = el('tr', {
      class: [enterable ? 'dir' : '', idx === state.sel ? 'sel' : '', cov ? 'staged' : isPartial ? 'partial' : ''].join(' ').trim(),
      'data-idx': idx,
    },
      el('td', { class: 'c-name' }, el('div', { class: 'name', title: nameTitle }, nameKids)),
      sizeCell,
      el('td', { class: 'c-pct' }, el('div', { class: 'pct', title: pctTitle },
        el('div', { class: 'track' }, trackKids),
        el('span', { class: 'num' + (cov && it.kind !== KIND.MOUNT ? ' staged' : ''), text: it.kind === KIND.MOUNT ? '' : pctVal.toFixed(1) + '%' }))),
      el('td', { class: 'c-items', text: it.kind === KIND.DIR ? fmtNum(it.items) : '' }),
      el('td', { class: 'c-mtime', text: fmtTime(it.mtime) }));
    tr.addEventListener('click', () => { state.sel = idx; highlight(); if (enterable) openDir(it.id); });
    return tr;
  }

  function renderRows() {
    const tbody = $('rows');
    tbody.replaceChildren();
    if (state.dirId !== 0) {
      const up = el('tr', { class: 'dir up' }, el('td', { class: 'c-name' }, el('div', { class: 'name' }, icon(KIND.DIR), el('span', { class: 't', text: t('row.up') }))),
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
    $('btnMore').textContent = t('list.more', { n: remaining });
    if (!state.items.length) {
      tbody.append(el('tr', {}, el('td', { colspan: 5, class: 'muted', text: t('list.empty') })));
    }
  }

  function highlight() {
    for (const tr of $('rows').querySelectorAll('tr[data-idx]')) {
      tr.classList.toggle('sel', Number(tr.dataset.idx) === state.sel);
    }
    const cur = $('rows').querySelector('tr.sel');
    if (cur) cur.scrollIntoView({ block: 'nearest' });
  }

  function showState(title, text, button, { keepVolumes = false } = {}) {
    $('list').classList.add('hidden');
    $('moreWrap').classList.add('hidden');
    $('dirNotice').classList.add('hidden');
    if (!keepVolumes) {
      state.volSeq++;
      state.previewLoaded = false;
      $('volumes').classList.add('hidden');
      $('crumbs').replaceChildren();
      $('dirSummary').textContent = '';
    }
    const s = $('state');
    s.replaceChildren(el('h3', { text: title }), el('p', { text }), button || '');
    s.classList.remove('hidden');
  }

  function renderPreviewHeader() {
    const ol = $('crumbs');
    ol.replaceChildren(el('li', {}, el('a', { text: t('crumbs.all') })));
    $('dirSummary').textContent = state.volumes.length
      ? t('vol.previewSummary', { n: state.volumes.length })
      : t('vol.previewEmpty');
  }

  async function loadPreviewVolumes() {
    const seq = ++state.volSeq;
    try {
      const data = await api('volumes');
      if (seq !== state.volSeq || state.scanId !== null || state.view !== 'space') return;
      state.volumes = data.volumes || [];
      state.skipped = data.skipped || [];
      state.previewLoaded = true;
      renderPreviewHeader();
      renderVolumes();
    } catch (e) {
      if (seq !== state.volSeq) return;
      if (e.status === 401 || e.status === 403) return showError(e);
      toast(e.message || t('opFailed'));
    }
  }

  function renderEmpty({ reloadVolumes = false } = {}) {
    if (state.view !== 'space') setView('space', { load: false });
    if (reloadVolumes || !state.previewLoaded) {
      state.volumes = [];
      state.skipped = [];
      state.previewLoaded = false;
    }
    if (state.running) {
      showState(t('empty.running.h'), t('empty.running.p'), null, { keepVolumes: true });
    } else {
      showState(t('empty.h'), t('empty.p'),
        el('button', { class: 'btn primary', type: 'button', text: t('empty.start'), onclick: startScan }),
        { keepVolumes: true });
    }
    if (state.previewLoaded) {
      renderPreviewHeader();
      renderVolumes();
    } else {
      $('volumes').classList.add('hidden');
      $('crumbs').replaceChildren();
      $('dirSummary').textContent = '';
      loadPreviewVolumes();
    }
  }

  const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]']);

  // Linux build: password sign-in form in the main content area.
  function showLogin() {
    clearTimeout(state.pollTimer);
    state.volSeq++;
    state.previewLoaded = false;
    closeDialog();
    if ($('loginPassword')) return;
    for (const id of ['progress', 'dupBar', 'dupWrap', 'volumes', 'dirNotice', 'basketBar']) $(id).classList.add('hidden');
    state.basket.clear();
    document.querySelector('.toolbar').classList.add('hidden');
    $('crumbs').replaceChildren();
    $('dirSummary').textContent = '';
    $('list').classList.add('hidden');
    $('moreWrap').classList.add('hidden');
    $('listWrap').classList.remove('hidden');
    const input = el('input', { type: 'password', id: 'loginPassword', class: 'login-input', autocomplete: 'current-password', autofocus: true, required: true });
    const err = el('p', { class: 'login-error hidden', role: 'alert' });
    const submit = el('button', { class: 'btn primary', type: 'submit', text: t('login.submit') });
    const form = el('form', { class: 'login' },
      el('h3', { text: t('login.h') }),
      el('label', { for: 'loginPassword', text: t('login.password') }),
      input, err, submit);
    if (location.protocol === 'http:' && !LOCAL_HOSTS.has(location.hostname)) {
      form.append(el('p', { class: 'login-insecure', text: t('login.insecure') }));
    }
    form.addEventListener('submit', async (ev) => {
      ev.preventDefault();
      submit.disabled = true;
      err.classList.add('hidden');
      try {
        await api('login', {}, { method: 'POST', body: { password: input.value } });
        location.reload();
        return;
      } catch (e) {
        err.textContent = e.message || t('opFailed');
        err.classList.remove('hidden');
        input.select();
      }
      submit.disabled = false;
    });
    const s = $('state');
    s.replaceChildren(form);
    s.classList.remove('hidden');
    input.focus();
  }

  function showError(e) {
    if (e.status === 401) return LINUX ? showLogin() : showState(t('auth.h'), t('auth.p'));
    if (e.status === 403 && e.code === 'not_admin') return showState(t('admin.h'), t('admin.p'));
    toast(e.message || t('opFailed'));
  }

  // ---------- scanning ----------
  async function startScan() {
    try {
      await api('scan_start', {}, { method: 'POST' });
      toast(t('scan.started'));
      state.running = true;
      state.progress = null;
      renderProgress();
      if (state.scanId === null) renderEmpty();
      schedulePoll(800);
    } catch (e) {
      if (e.code === 'task_running') { toast(t('err.task_running')); schedulePoll(0); return; }
      showError(e);
    }
  }

  async function cancelScan() {
    const del = isDeleteJob(state.progress);
    const dup = isDupJob(state.progress);
    const ok = del
      ? await confirmDialog(t('delFile.cancel.h'), t('delFile.cancel.p'), t('delFile.cancel.ok'), true)
      : dup
        ? await confirmDialog(t('dup.cancel.h'), t('dup.cancel.p'), t('dup.cancel.ok'), true)
        : await confirmDialog(t('scan.cancel.h'), t('scan.cancel.p'), t('scan.cancel.ok'), true);
    if (!ok) return;
    try {
      await api('scan_cancel', {}, { method: 'POST' });
      toast(t(del ? 'delFile.canceling' : dup ? 'dup.canceling' : 'scan.canceling'));
      schedulePoll(500);
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
    const prevDelEnabled = state.deleteEnabled;
    state.deleteEnabled = !!st.deleteEnabled;
    const was = state.running;
    const prev = state.progress;
    state.running = !!st.running;
    state.progress = st.progress;
    if (!state.statusLoaded) {
      state.statusLoaded = true;
      if (state.running && isDeleteJob(state.progress) && state.progress.jobId) {
        state.activeDeleteJobId = state.progress.jobId;
      } else if (!state.running && st.lastDelete && st.lastDelete.id && !state.activeDeleteJobId) {
        state.seenDeleteJobId = st.lastDelete.id;
      }
    } else if (state.running && isDeleteJob(state.progress) && state.progress.jobId) {
      state.activeDeleteJobId = state.progress.jobId;
    }
    renderProgress();
    renderBasketBar();
    if (was !== state.running || prevDelEnabled !== state.deleteEnabled) {
      if (state.view === 'dups' && state.dup.loaded) renderDups();
      else renderSpaceBasketSync();
    }
    // A scheduled run goes straight from its scan into a duplicate check
    // without ever going idle: treat the switch as the scan finishing.
    if (was && state.running && prev && !isDupJob(prev) && !isDeleteJob(prev) && isDupJob(state.progress)) await onScanFinished(prev);
    if (state.running) {
      schedulePoll(isDeleteJob(state.progress) ? 800 : 2000);
    } else {
      const ld = st.lastDelete;
      const newDelDone = ld && ld.id && ld.id !== state.seenDeleteJobId &&
        (state.activeDeleteJobId === ld.id || (was && isDeleteJob(prev)));
      if (newDelDone) {
        state.seenDeleteJobId = ld.id;
        state.activeDeleteJobId = null;
        await onDeleteFinished(ld);
      } else if (was) {
        if (isDeleteJob(prev)) {
          state.activeDeleteJobId = null;
          await loadScans().catch(() => {});
          showCurrent();
        } else if (isDupJob(prev)) {
          await onDupFinished(prev.scanId);
        } else {
          await onScanFinished(prev);
        }
      }
    }
  }

  const isDupJob = (p) => !!p && p.job === 'dupcheck';
  const isDeleteJob = (p) => !!p && p.job === 'delete';

  async function onDupFinished(scanId) {
    await loadScans().catch(() => {});
    const s = state.scans.find((x) => x.id === scanId);
    const d = s && s.dup;
    if (d && d.status === 'done') {
      toast(d.groups ? t('dup.done', { groups: d.groups, size: fmtSize(state.sizeMode === 'apparent' ? d.reclaimASize : d.reclaimDSize) }) : t('dup.doneNone'));
    } else if (d && d.status === 'canceled') {
      toast(t('dup.canceled'));
    } else if (d && d.status === 'failed') {
      toast(t('dup.failed', { msg: d.errorMessage || t('unknownError') }));
    }
    if (state.view === 'dups' && state.scanId === scanId) loadDups();
  }

  async function onScanFinished(prevProgress) {
    const before = new Set(state.scans.map((s) => s.id));
    await loadScans().catch(() => {});
    const fresh = state.scans.find((s) => !before.has(s.id) || (s.id === prevProgress?.scanId));
    const last = state.scans.find((s) => s.id === (prevProgress && prevProgress.scanId)) || fresh || state.scans[0];
    if (last && last.status === 'done') {
      toast(t('scan.done', { size: fmtSize(last.totalDSize), files: last.files || 0, dur: fmtDuration(last.durationS) }));
      state.basket.clear();
      state.scanId = last.id;
      state.dirId = 0;
      renderScanSelect();
      renderBasketBar();
      showCurrent();
    } else if (last && last.status === 'canceled') {
      toast(t('scan.canceled'));
      if (state.scanId === null) renderEmpty();
    } else if (last && last.status === 'failed') {
      toast(t('scan.failed', { msg: last.errorMessage || t('unknownError') }));
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
    $('btnCancel').textContent = t(isDeleteJob(p) ? 'delFile.cancel.ok' : isDupJob(p) ? 'dup.cancel.ok' : 'scan.cancel.ok');
    if (isDeleteJob(p)) return renderDeleteProgress(p, elapsed, bar);
    if (isDupJob(p)) return renderDupProgress(p, elapsed, bar);
    if (p.prevFiles > 0) {
      const pct = Math.min(99, (p.files / p.prevFiles) * 100);
      bar.classList.remove('indeterminate');
      bar.style.width = pct.toFixed(1) + '%';
      $('progTitle').textContent = t('prog.scanningPct', { pct: pct.toFixed(0) });
    } else {
      bar.classList.add('indeterminate');
      $('progTitle').textContent = t('prog.scanning');
    }
    const stats = [t('prog.scanned', { files: p.files || 0, dirs: p.dirs || 0 }), fmtSize(p.bytes || 0), t('elapsed', { dur: fmtDuration(elapsed) })];
    if (p.errors) stats.push(t('prog.errors', { n: p.errors }));
    if (p.trigger === 'schedule') stats.push(t(p.next === 'dupcheck' ? 'prog.scheduledThenDup' : 'prog.scheduled'));
    $('progStats').textContent = stats.join(' · ');
    $('progPath').textContent = p.current || '';
    $('progPath').title = p.current || '';
  }

  function renderDeleteProgress(p, elapsed, bar) {
    const targets = p.targets || 0;
    const done = p.doneTargets || 0;
    if (targets > 1) {
      const pct = Math.min(99, (done / targets) * 100);
      bar.classList.remove('indeterminate');
      bar.style.width = pct.toFixed(1) + '%';
      $('progTitle').textContent = t('delFile.progTitleCount', { done, total: targets });
    } else {
      bar.classList.add('indeterminate');
      $('progTitle').textContent = t('delFile.progTitle');
    }
    const stats = [
      t('delFile.progStats', { items: p.deletedItems || 0, size: fmtSize(p.freedBytes || 0) }),
      t('elapsed', { dur: fmtDuration(elapsed) }),
    ];
    if (p.failed) stats.push(t('delFile.progFailed', { n: p.failed }));
    $('progStats').textContent = stats.join(' · ');
    $('progPath').textContent = p.current || '';
    $('progPath').title = p.current || '';
  }

  // ---------- file deletion dialogs & completion ----------
  const PROTECT_REASONS = new Set(['volume_root', 'mount_point', 'system_path', 'app_data', 'bad_path']);
  const protectReasonText = (r) => (PROTECT_REASONS.has(r) ? t('delFile.reason.' + r) : r || '');

  async function openDeleteConfirm(entryIds, { fromBasket = false } = {}) {
    if (!canDelete() || state.running) return;
    if (!entryIds || !entryIds.length) return;
    const title = t(fromBasket ? 'delFile.basketTitle' : 'delFile.title');
    openDialog(title, el('p', { class: 'muted', text: t('delFile.loading') }),
      [el('button', { class: 'btn', type: 'button', text: t('cancel'), onclick: closeDialog })]);

    let data;
    try {
      data = await api('delete_preview', {}, { method: 'POST', body: { scanId: state.scanId, entryIds } });
    } catch (e) {
      closeDialog();
      showError(e);
      return;
    }

    const items = data.items || [];
    if (fromBasket) {
      // Drop any entry IDs that disappeared from the scan since they were staged.
      const keepSet = new Set(items.map((x) => x.entryId));
      let dropped = false;
      for (const id of Array.from(state.basket.keys())) {
        if (!keepSet.has(id) && entryIds.includes(id)) { state.basket.delete(id); dropped = true; }
      }
      renderBasketBar();
      if (dropped) {
        if (state.view === 'dups') renderDupRows(); else renderSpaceBasketSync();
      }
    }

    if (!items.length) {
      openDialog(title, el('p', { class: 'del-summary', text: t('delFile.empty') }),
        [el('button', { class: 'btn', type: 'button', text: t('dlg.close'), onclick: closeDialog })], true);
      if (state.view === 'dups') renderDupRows(); else renderSpaceBasketSync();
      return;
    }

    const bodyKids = [];
    const totalSize = state.sizeMode === 'apparent' ? data.totalASize : data.totalDSize;
    const deletableCount = items.filter((x) => !x.protected).length;
    bodyKids.push(el('p', {
      class: 'del-summary',
      text: t('delFile.summary', { n: deletableCount || items.length, items: data.totalItems || 0, size: fmtSize(totalSize) }),
    }));
    const notes = [];
    if (data.folded > 0) notes.push(t('delFile.foldedNote', { n: data.folded }));
    if (data.missing > 0) notes.push(t('delFile.missingNote', { n: data.missing }));
    if (notes.length) bodyKids.push(el('p', { class: 'del-meta', text: notes.join(' ') }));

    if (data.hasProtected) {
      const protKids = [el('span', { text: t('delFile.protectedBanner') })];
      if (fromBasket) {
        protKids.push(el('button', {
          class: 'btn small', type: 'button', text: t('delFile.removeProtected'),
          onclick: () => {
            for (const it of items) if (it.protected) state.basket.delete(it.entryId);
            renderBasketBar();
            if (state.view === 'dups') renderDupRows(); else renderSpaceBasketSync();
            if (state.basket.size === 0) closeDialog();
            else openDeleteConfirm(Array.from(state.basket.keys()), { fromBasket: true });
          },
        }));
      }
      bodyKids.push(el('div', { class: 'del-warn protected' }, el('div', { class: 'del-warn-head' }, protKids)));
    }

    const listEl = el('ul', { class: 'del-list' });
    for (const it of items) {
      const subKids = [el('span', { text: fmtSize(state.sizeMode === 'apparent' ? it.asize : it.dsize) })];
      if (it.kind === KIND.DIR) subKids.push(el('span', { text: '· ' + t('delFile.itemCount', { n: it.items }) }));
      if (it.badName) subKids.push(el('span', { class: 'tag warn', text: t('tag.badName') }));
      if (it.dockerRisk) {
        const dkLbl = DK_TYPES.has(it.dockerType) ? t('dk.type.' + it.dockerType) : t('delFile.dockerTag');
        subKids.push(el('span', { class: 'tag docker-risk', text: dkLbl }));
      }
      if (it.protected) {
        subKids.push(el('span', { class: 'tag protected', text: protectReasonText(it.protectReason) }));
      }
      const rowKids = [
        icon(it.kind, false),
        el('div', { class: 'del-item-main' },
          el('div', { class: 'del-item-path' }, el('bdi', { dir: 'ltr', text: it.path })),
          el('div', { class: 'del-item-sub' }, subKids)),
      ];
      if (fromBasket) {
        rowKids.push(el('button', {
          class: 'btn small', type: 'button', text: t('delFile.remove'),
          onclick: () => {
            state.basket.delete(it.entryId);
            renderBasketBar();
            if (state.view === 'dups') renderDupRows(); else renderSpaceBasketSync();
            if (state.basket.size === 0) closeDialog();
            else openDeleteConfirm(Array.from(state.basket.keys()), { fromBasket: true });
          },
        }));
      }
      listEl.append(el('li', { class: 'del-item' + (it.protected ? ' is-protected' : '') }, rowKids));
    }
    bodyKids.push(listEl);

    let dockerAck = null;
    if (data.hasDockerRisk) {
      dockerAck = el('input', { type: 'checkbox' });
      bodyKids.push(el('div', { class: 'del-warn docker' },
        el('div', { text: t('delFile.dockerWarn') }),
        el('label', { class: 'del-ack' }, dockerAck, el('span', { text: t('delFile.dockerAck') }))));
    }

    const confirmBtn = el('button', {
      class: 'btn danger-primary', type: 'button',
      text: items.length > 1 ? t('delFile.confirmCount', { n: items.length }) : t('delFile.confirm'),
    });
    const syncConfirm = () => {
      confirmBtn.disabled = state.running || data.hasProtected || (data.hasDockerRisk && (!dockerAck || !dockerAck.checked));
    };
    syncConfirm();
    if (dockerAck) dockerAck.addEventListener('change', syncConfirm);

    confirmBtn.addEventListener('click', async () => {
      confirmBtn.disabled = true;
      try {
        const resp = await api('delete_start', {}, {
          method: 'POST',
          body: {
            scanId: state.scanId,
            entryIds: items.map((x) => x.entryId),
            confirmDocker: !!(dockerAck && dockerAck.checked),
          },
        });
        closeDialog();
        toast(t('delFile.started'));
        state.activeDeleteJobId = resp.jobId;
        state.activeDeleteTargets = items.map((x) => ({ entryId: x.entryId, displayPath: x.displayPath, kind: x.kind }));
        state.running = true;
        state.progress = {
          job: 'delete', jobId: resp.jobId, scanId: state.scanId,
          targets: resp.targets || items.length, doneTargets: 0, deletedItems: 0, freedBytes: 0,
          startedAt: Date.now() / 1000,
        };
        renderProgress();
        renderBasketBar();
        if (state.view === 'dups') renderDups(); else renderSpaceBasketSync();
        schedulePoll(250);
      } catch (e) {
        if (e.code === 'task_running') { closeDialog(); toast(t('err.task_running')); schedulePoll(0); return; }
        syncConfirm();
        showError(e);
      }
    });

    openDialog(title, el('div', {}, bodyKids), [
      el('button', { class: 'btn', type: 'button', text: t('cancel'), onclick: closeDialog }),
      confirmBtn,
    ]);
  }

  async function onDeleteFinished(ld) {
    if (Array.isArray(ld.deletedIds)) {
      const delSet = new Set(ld.deletedIds);
      for (const id of delSet) state.basket.delete(id);
      for (const dt of state.activeDeleteTargets || []) {
        if (dt.kind === KIND.DIR && delSet.has(dt.entryId) && dt.displayPath) {
          for (const [bid, b] of Array.from(state.basket.entries())) {
            if (isPathUnder(b.path, dt.displayPath)) state.basket.delete(bid);
          }
        }
      }
    }
    state.activeDeleteTargets = [];
    await loadScans().catch(() => {});
    renderBasketBar();
    await showCurrent();
    const freed = fmtSize(state.sizeMode === 'apparent' ? ld.freedASize : ld.freedDSize);
    const hasFailures = Array.isArray(ld.failures) && ld.failures.length > 0;
    if (ld.status === 'done' && !hasFailures) {
      toast(t('delFile.done', { items: ld.deletedItems || 0, size: freed }));
    } else if (ld.status === 'canceled' && !hasFailures) {
      toast(t('delFile.canceled', { items: ld.deletedItems || 0, size: freed }));
    } else {
      showDeleteResultModal(ld, freed);
    }
  }

  function showDeleteResultModal(ld, freed) {
    const isFailed = ld.status === 'failed' && (!ld.failures || !ld.failures.length);
    const title = t(isFailed ? 'delFile.failedTitle' : 'delFile.partialTitle');
    const kids = [];
    if (isFailed) {
      kids.push(el('p', { class: 'del-summary', text: t('delFile.failedSummary', { msg: ld.errorMessage || t('unknownError') }) }));
    } else {
      const failCount = (ld.failures && ld.failures.length) || Math.max(0, (ld.targets || 0) - (ld.succeeded || 0));
      kids.push(el('p', {
        class: 'del-summary',
        text: t('delFile.partialSummary', {
          succeeded: ld.succeeded || 0, targets: ld.targets || 0, items: ld.deletedItems || 0, size: freed, failed: failCount,
        }),
      }));
      if (ld.failures && ld.failures.length) {
        const ul = el('ul', { class: 'del-list' });
        for (const f of ld.failures) {
          ul.append(el('li', { class: 'del-item' },
            el('div', { class: 'del-item-main' },
              el('div', { class: 'del-item-path' }, el('bdi', { dir: 'ltr', text: f.path || '' })),
              el('div', { class: 'del-fail-err', text: f.error || '' }))));
        }
        kids.push(ul);
      }
      kids.push(el('p', { class: 'del-meta', text: t('delFile.rescanHint') }));
    }
    openDialog(title, el('div', {}, kids), [el('button', { class: 'btn primary', type: 'button', text: t('dlg.close'), onclick: closeDialog })]);
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
      openDialog(title, el('p', { text, style: 'margin:0;line-height:1.6' }), [el('button', { class: 'btn', type: 'button', text: t('back'), onclick: closeDialog }), ok], true);
    });
  }

  async function showHistory() {
    try { await loadScans(); } catch (e) { return showError(e); }
    const rows = state.scans.map((s) => el('tr', { class: s.id === state.scanId ? 'current' : '' },
      el('td', { text: fmtTime(s.startedAt) }),
      el('td', { text: triggerText(s.trigger) }),
      el('td', {}, el('span', { class: 'status ' + s.status, text: statusText(s.status), title: s.errorMessage || '' })),
      el('td', { class: 'num', text: s.status === 'done' ? fmtSize(s.totalDSize) : '' }),
      el('td', { class: 'num', text: s.status === 'done' ? fmtNum(s.files) : '' }),
      el('td', { class: 'num', text: s.durationS ? fmtDuration(s.durationS) : '' }),
      el('td', { class: 'num', text: s.dbBytes ? fmtSize(s.dbBytes) : '' }),
      el('td', {}, dupCell(s)),
      el('td', {},
        s.status === 'done' && s.id !== state.scanId ? el('button', { class: 'btn small', type: 'button', text: t('hist.open'), onclick: () => { closeDialog(); selectScan(s.id); } }) : '',
        ' ',
        s.status !== 'running' ? el('button', { class: 'btn small danger', type: 'button', text: t('hist.delete'), onclick: () => deleteScan(s) }) : '')));
    const body = state.scans.length
      ? el('table', { class: 'hist' },
        el('thead', {}, el('tr', {}, t('hist.cols').map((h) => el('th', { text: h })))),
        el('tbody', {}, rows))
      : el('p', { class: 'muted', text: t('hist.none') });
    const keep = state.settings ? state.settings.retention : 10;
    openDialog(t('hist.title'), el('div', {}, body, el('p', { class: 'muted', style: 'margin:12px 0 0;font-size:12px', text: t('hist.keep', { n: keep }) })));
  }

  async function deleteScan(s) {
    if (!(await confirmDialog(t('del.h'), t('del.p', { time: fmtTime(s.startedAt) }), t('del.ok'), true))) return;
    try {
      await api('scan_delete', { scan: s.id }, { method: 'POST' });
      toast(t('del.done'));
      if (s.id === state.scanId) {
        await loadScans();
        const latest = doneScans()[0];
        state.scanId = latest ? latest.id : null;
        renderScanSelect();
        state.dirId = 0;
        if (latest) showCurrent(); else renderEmpty({ reloadVolumes: true });
      }
      showHistory();
    } catch (e) {
      showError(e);
    }
  }

  function showSettings() {
    const s = { ...(state.settings || { scheduleMode: 'off', weekday: 0, time: '03:00', retention: 10, defaultSizeMode: 'disk', scheduleDupCheck: false }) };
    const radio = (name, value, label, checked) => el('label', {}, el('input', { type: 'radio', name, value, checked }), label);
    const modeBox = el('div', { class: 'radios' },
      radio('mode', 'off', t('set.off'), s.scheduleMode === 'off'),
      radio('mode', 'daily', t('set.daily'), s.scheduleMode === 'daily'),
      radio('mode', 'weekly', t('set.weekly'), s.scheduleMode === 'weekly'));
    const weekday = el('select', { id: 'fWeekday' }, WEEKDAYS.map((d, i) => el('option', { value: i, text: d, selected: i === s.weekday })));
    const time = el('input', { type: 'time', id: 'fTime', value: s.time, required: true });
    const keep = el('input', { type: 'number', id: 'fKeep', min: 1, max: 100, value: s.retention });
    const sizeSel = el('select', { id: 'fSize' },
      el('option', { value: 'disk', text: t('size.disk'), selected: s.defaultSizeMode === 'disk' }),
      el('option', { value: 'apparent', text: t('size.apparent'), selected: s.defaultSizeMode === 'apparent' }));
    const weekRow = el('div', { class: 'row' }, el('span', { text: t('set.weekday') }), weekday);
    const timeRow = el('div', { class: 'row' }, el('span', { text: t('set.time') }), time);
    const err = el('div', { class: 'err-text' });
    const timeHint = el('div', { class: 'hint', text: t('set.timeHint') });
    const dupBox = el('input', { type: 'checkbox', id: 'fDup', checked: !!s.scheduleDupCheck });
    const dupRow = el('div', { class: 'row' }, el('span', { text: t('set.dup') }),
      el('div', { class: 'radios' }, el('label', {}, dupBox, t('set.dupLabel'))));
    const dupHint = el('div', { class: 'hint', text: t('set.dupHint') });
    const sync = () => {
      const m = modeBox.querySelector('input:checked').value;
      weekRow.classList.toggle('hidden', m !== 'weekly');
      for (const n of [timeRow, timeHint, dupRow, dupHint]) n.classList.toggle('hidden', m === 'off');
    };
    modeBox.addEventListener('change', sync);
    const form = el('div', { class: 'form' },
      el('div', { class: 'row' }, el('span', { text: t('set.schedule') }), modeBox),
      weekRow, timeRow, timeHint, dupRow, dupHint,
      el('div', { class: 'row' }, el('span', { text: t('set.keep') }), el('div', {}, keep, ' ' + t('set.keepUnit'))),
      el('div', { class: 'hint', text: t('set.keepHint') }),
      el('div', { class: 'row' }, el('span', { text: t('set.defaultSize') }), sizeSel),
      err);
    sync();
    const save = el('button', { class: 'btn primary', type: 'button', text: t('set.save') });
    save.addEventListener('click', async () => {
      const next = {
        scheduleMode: modeBox.querySelector('input:checked').value,
        weekday: Number(weekday.value),
        time: time.value || '03:00',
        retention: Number(keep.value),
        defaultSizeMode: sizeSel.value,
        scheduleDupCheck: dupBox.checked,
      };
      if (!(next.retention >= 1 && next.retention <= 100)) { err.textContent = t('set.keepRange'); return; }
      save.disabled = true;
      try {
        state.settings = await api('settings', {}, { method: 'POST', body: next });
        closeDialog();
        toast(next.scheduleMode === 'off' ? t('set.savedOff') : t('set.savedOn', {
          daily: next.scheduleMode === 'daily', weekday: WEEKDAYS[next.weekday], time: next.time, dup: next.scheduleDupCheck,
        }));
      } catch (e) {
        err.textContent = e.message || t('set.saveFailed');
      } finally {
        save.disabled = false;
      }
    });
    openDialog(t('set.title'), form, [el('button', { class: 'btn', type: 'button', text: t('cancel'), onclick: closeDialog }), save], true);
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
    if (dups) { $('volumes').classList.add('hidden'); $('dirNotice').classList.add('hidden'); }
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
        toast(t('notice.scanDeleted'));
        await loadScans().catch(() => {});
        const latest = doneScans()[0];
        if (latest) { state.scanId = latest.id; state.dirId = 0; renderScanSelect(); return loadDups(); }
        state.scanId = null;
        return renderEmpty({ reloadVolumes: true });
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
      toast(t('dup.started'));
      state.running = true;
      state.progress = { job: 'dupcheck', scanId: state.scanId, phase: 'prepare', startedAt: Date.now() / 1000 };
      renderProgress();
      renderDups();
      schedulePoll(800);
    } catch (e) {
      if (e.code === 'task_running') { toast(t('dup.busyStart')); schedulePoll(0); return; }
      showError(e);
    }
  }

  const reclaimOf = (x) => (state.sizeMode === 'apparent' ? x.reclaimASize : x.reclaimDSize);
  const dupRunningHere = () => state.running && isDupJob(state.progress) && state.progress.scanId === state.scanId;

  function renderDups() {
    renderDupBar();
    renderDupRows();
    renderBasketBar();
  }

  function renderDupBar() {
    const bar = $('dupBar');
    const { check, result } = state.dup;
    if (!state.dup.loaded) { bar.replaceChildren(); return; }
    const parts = [];
    const busy = state.running;
    const again = (text) => el('button', {
      class: 'btn small', type: 'button', text, disabled: busy,
      title: busy ? t('dup.busyTitle') : '', onclick: startDupCheck,
    });
    if (dupRunningHere()) {
      parts.push(el('span', { text: t('dup.runningHere', { prev: !!result }) }));
    } else if (check && (check.status === 'failed' || check.status === 'canceled')) {
      parts.push(el('span', { class: 'warn', text: check.status === 'canceled'
        ? t('dup.lastCanceled', { prev: !!result })
        : t('dup.lastFailed', { msg: check.errorMessage || t('unknownError'), prev: !!result }) }));
    } else if (check && check.status === 'running') {
      parts.push(el('span', { class: 'warn', text: t('dup.lastUnfinished') }));
    }
    if (result && !dupRunningHere()) {
      const summary = el('span', {},
        el('b', { text: result.groups ? t('dup.summary', { groups: result.groups, size: fmtSize(reclaimOf(result)) }) : t('dup.none') }),
        ' · ' + t('dup.checkedAt', { time: fmtTime(result.finishedAt) }),
        result.changed ? ' · ' + t('dup.changed', { n: result.changed }) : '',
        result.errors ? ' · ' + t('dup.errors', { n: result.errors }) : '');
      parts.push(summary);
    }
    if (result || check) parts.push(again(t('dup.recheck')));
    bar.replaceChildren(...parts);
    bar.classList.toggle('hidden', !parts.length || state.view !== 'dups');
  }

  function dupGroupStagedCount(g) {
    let n = 0;
    for (const m of g.members) {
      if (basketCovers(m.entryId, m.path, inferRootId(m.path))) n++;
    }
    return n;
  }

  function dupMemberRows(g) {
    const rows = [];
    if (g.kind === KIND.DIR) rows.push(el('tr', { class: 'member-note' }, el('td', { colspan: 4, text: t('dup.dirNote', { n: g.files || 0 }) })));
    const allowDel = canDelete();
    const stagedCount = allowDel ? dupGroupStagedCount(g) : 0;
    for (const m of g.members) {
      const mRoot = inferRootId(m.path);
      const cov = allowDel ? basketCovers(m.entryId, m.path, mRoot) : null;
      const wouldEmpty = allowDel && !cov && (stagedCount + 1 >= g.copies);
      const tags = [];
      if (m.covered) tags.push(el('span', { class: 'tag', text: t('dup.covered'), title: t('dup.covered.title') }));
      if (m.badName) tags.push(el('span', { class: 'tag warn', text: t('tag.badName'), title: t('tag.badName.title') }));
      if (cov === 'inherited') tags.push(el('span', { class: 'tag inherited', text: t('basket.inherited'), title: t('basket.inherited.title') }));
      const mKids = [];
      if (allowDel) {
        mKids.push(el('input', {
          type: 'checkbox', class: 'row-chk', checked: !!cov,
          disabled: cov === 'inherited' || wouldEmpty || state.running,
          title: cov === 'inherited' ? t('basket.inherited.title') : wouldEmpty ? t('basket.lastCopy') : t('basket.selectRow'),
          'aria-label': t('basket.selectRow'),
          onclick: (ev) => ev.stopPropagation(),
          onchange: (ev) => {
            ev.stopPropagation();
            if (ev.target.checked) {
              addToBasket({
                entryId: m.entryId, path: m.path, kind: g.kind,
                dsize: g.dsize, asize: g.asize, items: g.files || 1, name: g.name,
                rootId: mRoot, hardlinkDup: false,
              });
            } else {
              removeFromBasket(m.entryId);
            }
            renderDupRows();
          },
        }));
      }
      const actBtns = [];
      if (allowDel) {
        actBtns.push(el('button', {
          class: 'btn small danger', type: 'button', text: t('row.delete'),
          disabled: state.running || wouldEmpty,
          title: state.running ? t('delFile.busyTitle') : wouldEmpty ? t('basket.lastCopy') : t('row.delete.title'),
          onclick: (ev) => { ev.stopPropagation(); openDeleteConfirm([m.entryId], { fromBasket: false }); },
        }));
      }
      actBtns.push(el('button', {
        class: 'btn small jump', type: 'button', text: t('dup.jump'), title: t('dup.jump.title'),
        onclick: (ev) => { ev.stopPropagation(); jumpTo(m); },
      }));
      mKids.push(
        el('span', { class: 't', title: m.path }, el('bdi', { dir: 'ltr', text: m.path })),
        tags,
        el('span', { class: 'mpath-actions' }, actBtns));
      rows.push(el('tr', { class: 'member' + (cov ? ' staged' : '') }, el('td', { colspan: 4 }, el('div', { class: 'mpath' }, mKids))));
    }
    if (g.copies > g.members.length) {
      rows.push(el('tr', { class: 'member-note' }, el('td', { colspan: 4, text: t('dup.moreCopies', { n: g.copies - g.members.length }) })));
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
        st.append(el('h3', { text: t('dup.checking.h') }), el('p', { text: t('dup.checking.p') }));
      } else {
        st.append(el('h3', { text: t(check ? 'dup.unchecked.h' : 'dup.never.h') }),
          el('p', { text: t('dup.intro') }),
          el('button', { class: 'btn primary', type: 'button', text: t('dup.start'), disabled: state.running, onclick: startDupCheck }),
          state.running ? el('p', { class: 'muted', text: t('dup.busyNote') }) : '');
      }
      st.classList.remove('hidden');
      return;
    }
    if (!groups.length) {
      $('dupList').classList.add('hidden');
      $('dupMoreWrap').classList.add('hidden');
      st.replaceChildren(el('h3', { text: t('dup.none') }), el('p', { text: t('dup.noneFound.p') }));
      st.classList.remove('hidden');
      return;
    }
    st.classList.add('hidden');
    $('dupList').classList.remove('hidden');
    const frag = document.createDocumentFragment();
    for (const g of groups) {
      const open = state.dup.open.has(g.id);
      const tags = [];
      if (g.badName) tags.push(el('span', { class: 'tag warn', text: t('tag.badName'), title: t('tag.badName.title') }));
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
    $('btnDupMore').textContent = t('dup.more', { n: remaining });
  }

  function jumpTo(m) {
    setView('space', { load: false });
    openDir(m.parentId, { selectId: m.entryId });
  }

  function renderDupProgress(p, elapsed, bar) {
    const phase = t(PHASES.has(p.phase) ? 'phase.' + p.phase : 'phase.default');
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
    $('progTitle').textContent = t('dup.progTitle', { phase }) + (pct !== null ? ` ${pct.toFixed(0)}%` : '…');
    const stats = [];
    const files = () => t('dup.files', { done: p.doneFiles || 0, total: p.totalFiles || 0 });
    if (p.phase === 'quick' || p.phase === 'full') {
      stats.push(t('dup.readBytes', { done: fmtSize(p.doneBytes || 0), total: fmtSize(p.totalBytes || 0) }), files());
    } else if (p.phase === 'prepare' && p.totalFiles) {
      stats.push(files());
    }
    if (p.cacheHits) stats.push(t('dup.cacheHits', { n: p.cacheHits }));
    if (p.changed) stats.push(t('dup.changedShort', { n: p.changed }));
    if (p.errors) stats.push(t('dup.errorsShort', { n: p.errors }));
    stats.push(t('elapsed', { dur: fmtDuration(elapsed) }));
    if (p.trigger === 'schedule') stats.push(t('prog.scheduled'));
    $('progStats').textContent = stats.join(' · ');
    const cur = p.phase === 'quick' || p.phase === 'full' ? (p.current || '') : '';
    $('progPath').textContent = cur;
    $('progPath').title = cur;
  }

  function dupCell(s) {
    const d = s.dup;
    if (!d) return s.status === 'done' ? el('span', { class: 'muted', text: '—' }) : '';
    if (d.status === 'done') return el('span', { class: 'num', text: d.groups ? t('hist.dupReclaim', { size: fmtSize(d.reclaimDSize) }) : t('hist.dupNone') });
    return el('span', { class: 'status ' + d.status, text: statusText(d.status), title: d.errorMessage || '' });
  }

  // ---------- controls ----------
  function selectScan(id) {
    state.scanId = id;
    state.dirId = 0;
    renderScanSelect();
    renderBasketBar();
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
    renderBasketBar();
  }

  function bind() {
    $('btnNewScan').addEventListener('click', startScan);
    $('btnCancel').addEventListener('click', cancelScan);
    $('btnHistory').addEventListener('click', showHistory);
    $('btnSettings').addEventListener('click', showSettings);
    if (LINUX) {
      $('btnLogout').classList.remove('hidden');
      $('btnLogout').addEventListener('click', async () => {
        try { await api('logout', {}, { method: 'POST' }); } catch { /* reload shows the login page anyway */ }
        location.reload();
      });
    }
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
    I18N.apply();
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
