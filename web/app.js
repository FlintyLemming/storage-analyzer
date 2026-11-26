(() => {
  'use strict';

  const SKIP_REASONS = new Set(['not_mounted', 'not_share_volume', 'not_found', 'not_directory']);
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
    'scan_running', 'scan_not_found', 'no_scans', 'dir_not_found', 'invalid_settings', 'internal', 'bad_password']);
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
    ol.replaceChildren(el('li', {}, el('a', { text: t('crumbs.all'), onclick: () => openDir(0) })));
    for (const p of state.path) ol.append(el('li', {}, el('a', { text: p.name, title: p.name, onclick: () => openDir(p.id) })));
    const d = state.dir;
    $('dirSummary').textContent = d ? t('crumbs.summary', { size: fmtSize(sizeOf(d)), n: d.items || 0 }) : '';
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
          el('div', { class: 'vol-name' }, v.name, el('small', { text: v.fsType ? t('vol.fsDev', { fstype: v.fsType, device: v.device || '' }) : v.pool ? t('vol.poolMount', { pool: String(v.pool), mount: v.mountPoint }) : v.mountPoint })),
          el('div', { class: 'vol-cap', text: total ? t('vol.cap', { used: fmtSize(used), total: fmtSize(total), pct: ((used / total) * 100).toFixed(1) }) : '' })),
        el('div', { class: 'cap', title: t('vol.bar.title') },
          el('span', { class: 's', style: `width:${pct(Math.min(scanned, used || scanned))}` }),
          el('span', { class: 'u', style: `width:${pct(unacc)}` })),
        el('div', { class: 'legend' },
          el('span', {}, el('i', { style: 'background:var(--bar)' }), t('vol.scanned', { size: fmtSize(scanned) })),
          el('span', { title: t('vol.unacc.title') }, el('i', { style: 'background:var(--unaccounted)' }), t('vol.unacc', { size: fmtSize(unacc) })),
          el('span', {}, el('i', { style: 'background:var(--free)' }), t('vol.free', { size: fmtSize(v.fsFree) })))));
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
  }

  function rowFor(it, idx, parentSize) {
    const isRoot = state.dirId === 0;
    const size = sizeOf(it);
    const pctVal = parentSize > 0 ? (size / parentSize) * 100 : 0;
    const enterable = (it.kind === KIND.DIR) && it.hasChildren;
    const tags = [];
    if (it.kind === KIND.MOUNT) tags.push(el('span', { class: 'tag', text: t('tag.mount') }));
    if (it.kind === KIND.SYMLINK) tags.push(el('span', { class: 'tag', text: t('tag.symlink') }));
    if (it.flags & FLAG.HARDLINK_DUP) tags.push(el('span', { class: 'tag', text: t('tag.hardlink'), title: t('tag.hardlink.title') }));
    if (it.flags & FLAG.ERROR) tags.push(el('span', { class: 'tag warn', text: t('tag.readError') }));
    if (it.badName) tags.push(el('span', { class: 'tag warn', text: t('tag.badName'), title: t('tag.badName.title') }));
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
      showState(t('empty.running.h'), t('empty.running.p'));
      return;
    }
    showState(t('empty.h'), t('empty.p'),
      el('button', { class: 'btn primary', type: 'button', text: t('empty.start'), onclick: startScan }));
  }

  const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]']);

  // Linux build: password sign-in form in the main content area.
  function showLogin() {
    for (const id of ['progress', 'dupBar', 'dupWrap', 'volumes']) $(id).classList.add('hidden');
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
    const dup = isDupJob(state.progress);
    const ok = dup
      ? await confirmDialog(t('dup.cancel.h'), t('dup.cancel.p'), t('dup.cancel.ok'), true)
      : await confirmDialog(t('scan.cancel.h'), t('scan.cancel.p'), t('scan.cancel.ok'), true);
    if (!ok) return;
    try {
      await api('scan_cancel', {}, { method: 'POST' });
      toast(t(dup ? 'dup.canceling' : 'scan.canceling'));
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
    // A scheduled run goes straight from its scan into a duplicate check
    // without ever going idle: treat the switch as the scan finishing.
    if (was && state.running && prev && !isDupJob(prev) && isDupJob(state.progress)) await onScanFinished(prev);
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
      state.scanId = last.id;
      state.dirId = 0;
      renderScanSelect();
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
    $('btnCancel').textContent = t(isDupJob(p) ? 'dup.cancel.ok' : 'scan.cancel.ok');
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
        if (latest) showCurrent(); else renderEmpty();
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
        toast(t('notice.scanDeleted'));
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

  function dupMemberRows(g) {
    const rows = [];
    if (g.kind === KIND.DIR) rows.push(el('tr', { class: 'member-note' }, el('td', { colspan: 4, text: t('dup.dirNote', { n: g.files || 0 }) })));
    for (const m of g.members) {
      const tags = [];
      if (m.covered) tags.push(el('span', { class: 'tag', text: t('dup.covered'), title: t('dup.covered.title') }));
      if (m.badName) tags.push(el('span', { class: 'tag warn', text: t('tag.badName'), title: t('tag.badName.title') }));
      rows.push(el('tr', { class: 'member' }, el('td', { colspan: 4 },
        el('div', { class: 'mpath' },
          el('span', { class: 't', title: m.path }, el('bdi', { dir: 'ltr', text: m.path })),
          tags,
          el('button', { class: 'btn small jump', type: 'button', text: t('dup.jump'), title: t('dup.jump.title'), onclick: (ev) => { ev.stopPropagation(); jumpTo(m); } })))));
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
