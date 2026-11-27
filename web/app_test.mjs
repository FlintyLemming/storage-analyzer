import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const cssText = fs.readFileSync(path.join(__dirname, 'style.css'), 'utf8');
const i18nSrc = fs.readFileSync(path.join(__dirname, 'i18n.js'), 'utf8');
const appSrc = fs.readFileSync(path.join(__dirname, 'app.js'), 'utf8');

// Minimal DOM shim sufficient to execute the real web/i18n.js and web/app.js.
class ClassList {
  constructor(el) { this.el = el; this.set = new Set(); }
  syncFrom(str) {
    this.set = new Set(String(str || '').trim().split(/\s+/).filter(Boolean));
  }
  toString() { return Array.from(this.set).join(' '); }
  add(...cs) { for (const c of cs) if (c) this.set.add(c); }
  remove(...cs) { for (const c of cs) this.set.delete(c); }
  contains(c) { return this.set.has(c); }
  toggle(c, force) {
    if (force === undefined) {
      if (this.set.has(c)) { this.set.delete(c); return false; }
      this.set.add(c); return true;
    }
    if (force) { this.set.add(c); return true; }
    this.set.delete(c); return false;
  }
}

class FakeNode {
  constructor(tag, ns = null) {
    this.tagName = tag.toUpperCase();
    this.namespaceURI = ns;
    this.attrs = new Map();
    this.dataset = {};
    this.classList = new ClassList(this);
    this.children = [];
    this.parentNode = null;
    this.listeners = new Map();
    this._text = '';
    this.style = {
      _css: '',
      get cssText() { return this._css; },
      set cssText(v) {
        this._css = String(v || '');
        const m = this._css.match(/width\s*:\s*([^;]+)/i);
        if (m) this.width = m[1].trim();
      },
      width: '',
    };
    this.checked = false;
    this.indeterminate = false;
    this.disabled = false;
    this.value = '';
    this.scrollTop = 0;
  }
  get id() { return this.attrs.get('id') || ''; }
  set id(v) { this.attrs.set('id', String(v)); }
  get className() { return this.classList.toString(); }
  set className(v) { this.classList.syncFrom(v); }
  get title() { return this.attrs.get('title') || ''; }
  set title(v) { this.attrs.set('title', String(v)); }
  get textContent() {
    if (this.children.length === 0) return this._text;
    return this.children.map((c) => (typeof c === 'string' ? c : c.textContent)).join('');
  }
  set textContent(v) {
    this.children = [];
    this._text = v === null || v === undefined ? '' : String(v);
  }
  setAttribute(k, v) {
    const val = String(v);
    this.attrs.set(k, val);
    if (k === 'class') this.classList.syncFrom(val);
    else if (k === 'checked') this.checked = true;
    else if (k === 'disabled') this.disabled = true;
    else if (k === 'value') this.value = val;
    else if (k.startsWith('data-')) {
      const prop = k.slice(5).replace(/-([a-z])/g, (_, ch) => ch.toUpperCase());
      this.dataset[prop] = val;
    }
  }
  getAttribute(k) {
    if (k === 'class') return this.className;
    return this.attrs.has(k) ? this.attrs.get(k) : null;
  }
  _adopt(items) {
    const out = [];
    for (const item of items) {
      if (item === null || item === undefined || item === false) continue;
      if (item instanceof FakeFragment) {
        for (const ch of item.children) {
          if (typeof ch !== 'string') ch.parentNode = this;
          out.push(ch);
        }
        item.children = [];
      } else if (typeof item === 'object') {
        if (item.parentNode) item.remove();
        item.parentNode = this;
        out.push(item);
      } else {
        out.push(String(item));
      }
    }
    return out;
  }
  append(...items) {
    if (this._text) {
      this.children.push(this._text);
      this._text = '';
    }
    this.children.push(...this._adopt(items));
  }
  prepend(...items) {
    if (this._text) {
      this.children.push(this._text);
      this._text = '';
    }
    this.children.unshift(...this._adopt(items));
  }
  replaceChildren(...items) {
    this._text = '';
    this.children = [];
    this.append(...items);
  }
  remove() {
    if (!this.parentNode) return;
    const idx = this.parentNode.children.indexOf(this);
    if (idx >= 0) this.parentNode.children.splice(idx, 1);
    this.parentNode = null;
  }
  addEventListener(type, fn) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(fn);
  }
  dispatchEvent(ev) {
    ev.target = ev.target || this;
    ev.stopPropagation = ev.stopPropagation || (() => { ev._stopped = true; });
    ev.preventDefault = ev.preventDefault || (() => { ev._defaultPrevented = true; });
    let cur = this;
    while (cur) {
      const fns = cur.listeners.get(ev.type) || [];
      for (const fn of fns) fn(ev);
      if (ev._stopped) break;
      cur = cur.parentNode;
    }
  }
  click() {
    if (this.disabled) return;
    if (this.tagName === 'INPUT' && this.getAttribute('type') === 'checkbox') {
      // Browser standard: clicking an unchecked indeterminate checkbox checks it.
      this.indeterminate = false;
      this.checked = !this.checked;
      this.dispatchEvent({ type: 'click' });
      this.dispatchEvent({ type: 'change' });
      return;
    }
    this.dispatchEvent({ type: 'click' });
  }
  scrollIntoView() {}
  closest(sel) {
    let cur = this;
    while (cur && cur instanceof FakeNode) {
      if (matchesSimple(cur, sel)) return cur;
      cur = cur.parentNode;
    }
    return null;
  }
  querySelectorAll(selector) {
    const out = [];
    const parts = selector.split(',').map((s) => s.trim()).filter(Boolean);
    const walk = (node) => {
      for (const ch of node.children) {
        if (!(ch instanceof FakeNode)) continue;
        if (parts.some((p) => matchesSelector(ch, p, this))) out.push(ch);
        walk(ch);
      }
    };
    walk(this);
    return out;
  }
  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }
}

class FakeFragment {
  constructor() { this.children = []; }
  append(...items) {
    for (const item of items) {
      if (item === null || item === undefined || item === false) continue;
      if (item instanceof FakeFragment) {
        this.children.push(...item.children);
        item.children = [];
      } else {
        this.children.push(item);
      }
    }
  }
}

function matchesSimple(node, sel) {
  if (!sel || !(node instanceof FakeNode)) return false;
  if (sel.includes(',')) return sel.split(',').some((s) => matchesSimple(node, s.trim()));
  let rest = sel;
  // pseudo :checked
  if (rest.includes(':checked')) {
    if (!node.checked) return false;
    rest = rest.replace(':checked', '');
  }
  // attr selectors [name] or [name="val"]
  const attrRe = /\[([^\]=]+)(?:="([^"]*)")?\]/g;
  let m;
  while ((m = attrRe.exec(rest)) !== null) {
    const attrName = m[1].trim();
    const attrVal = m[2];
    const got = node.getAttribute(attrName);
    if (got === null) return false;
    if (attrVal !== undefined && got !== attrVal) return false;
  }
  rest = rest.replace(attrRe, '');
  // tag, #id, .class
  const tokens = rest.match(/(?:^[a-zA-Z0-9_-]+)|(?:#[a-zA-Z0-9_-]+)|(?:\.[a-zA-Z0-9_-]+)/g) || [];
  for (const tok of tokens) {
    if (tok.startsWith('#')) {
      if (node.id !== tok.slice(1)) return false;
    } else if (tok.startsWith('.')) {
      if (!node.classList.contains(tok.slice(1))) return false;
    } else {
      if (node.tagName !== tok.toUpperCase()) return false;
    }
  }
  return tokens.length > 0 || sel.includes('[') || sel.includes(':checked');
}

function matchesSelector(node, sel, root) {
  const parts = sel.trim().split(/\s+/);
  if (!matchesSimple(node, parts[parts.length - 1])) return false;
  let cur = node.parentNode;
  for (let i = parts.length - 2; i >= 0; i--) {
    let found = false;
    while (cur && cur !== root.parentNode) {
      if (matchesSimple(cur, parts[i])) { found = true; cur = cur.parentNode; break; }
      cur = cur.parentNode;
    }
    if (!found) return false;
  }
  return true;
}

function buildAppFixture(apiResponder, { lang = 'zh', platform = 'linux' } = {}) {
  const htmlEl = new FakeNode('html');
  htmlEl.setAttribute('data-platform', platform);
  const body = new FakeNode('body');
  htmlEl.append(body);

  const mk = (tag, attrs = {}, kids = []) => {
    const n = new FakeNode(tag);
    for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
    for (const k of kids) n.append(k);
    return n;
  };

  const app = mk('div', { class: 'app', id: 'app' }, [
    mk('header', { class: 'toolbar' }, [
      mk('select', { id: 'scanSelect' }),
      mk('button', { id: 'btnNewScan' }),
      mk('button', { id: 'btnHistory' }),
      mk('button', { id: 'btnSettings' }),
      mk('div', { id: 'viewSeg' }, [
        mk('button', { 'data-view': 'space', class: 'on' }),
        mk('button', { 'data-view': 'dups' }),
      ]),
      mk('div', { id: 'sizeSeg' }, [
        mk('button', { 'data-size': 'disk', class: 'on' }),
        mk('button', { 'data-size': 'apparent' }),
      ]),
      mk('button', { id: 'btnLogout', class: 'hidden' }),
    ]),
    mk('section', { class: 'progress hidden', id: 'progress' }, [
      mk('span', { id: 'progTitle' }),
      mk('button', { id: 'btnCancel' }),
      mk('div', { id: 'progBar' }),
      mk('div', { id: 'progStats' }),
      mk('div', { id: 'progPath' }),
    ]),
    mk('nav', { class: 'crumbs-row' }, [
      mk('ol', { class: 'crumbs', id: 'crumbs' }),
      mk('div', { class: 'dir-summary', id: 'dirSummary' }),
    ]),
    mk('section', { class: 'volumes hidden', id: 'volumes' }),
    mk('div', { class: 'dir-notice hidden', id: 'dirNotice' }),
    mk('main', { class: 'list-wrap', id: 'listWrap' }, [
      mk('table', { class: 'list', id: 'list' }, [
        mk('thead', {}, [
          mk('tr', {}, [
            mk('th', { class: 'c-name', 'data-sort': 'name', 'data-i18n': 'col.name' }),
            mk('th', { class: 'c-size', 'data-sort': 'size', 'data-i18n': 'col.size' }),
            mk('th', { class: 'c-pct', 'data-sort': 'size', 'data-i18n': 'col.pct' }),
            mk('th', { class: 'c-items', 'data-sort': 'items', 'data-i18n': 'col.items' }),
            mk('th', { class: 'c-mtime', 'data-sort': 'mtime', 'data-i18n': 'col.mtime' }),
          ]),
        ]),
        mk('tbody', { id: 'rows' }),
      ]),
      mk('div', { class: 'more hidden', id: 'moreWrap' }, [mk('button', { id: 'btnMore' })]),
      mk('div', { class: 'state hidden', id: 'state' }),
    ]),
    mk('section', { class: 'dupbar hidden', id: 'dupBar' }),
    mk('main', { class: 'list-wrap hidden', id: 'dupWrap' }, [
      mk('table', { class: 'list dups hidden', id: 'dupList' }, [
        mk('tbody', { id: 'dupRows' }),
      ]),
      mk('div', { class: 'more hidden', id: 'dupMoreWrap' }, [mk('button', { id: 'btnDupMore' })]),
      mk('div', { class: 'state hidden', id: 'dupState' }),
    ]),
    mk('section', { class: 'basket-bar hidden', id: 'basketBar' }),
  ]);

  const overlay = mk('div', { class: 'overlay hidden', id: 'overlay' }, [
    mk('div', { class: 'dialog' }, [
      mk('h2', { id: 'dlgTitle' }),
      mk('button', { id: 'dlgClose' }),
      mk('div', { id: 'dlgBody' }),
      mk('div', { id: 'dlgFoot' }),
    ]),
  ]);
  const toast = mk('div', { class: 'toast hidden', id: 'toast' });
  body.append(app, overlay, toast);

  const document = {
    documentElement: htmlEl,
    body,
    cookie: '',
    title: '',
    getElementById(id) {
      return htmlEl.querySelector('#' + id);
    },
    querySelector(sel) { return htmlEl.querySelector(sel); },
    querySelectorAll(sel) { return htmlEl.querySelectorAll(sel); },
    createElement(tag) { return new FakeNode(tag); },
    createElementNS(ns, tag) { return new FakeNode(tag, ns); },
    createDocumentFragment() { return new FakeFragment(); },
    addEventListener() {},
  };

  const timers = [];
  const sandbox = {
    document,
    window: null,
    location: { search: '?lang=' + lang, hash: '', protocol: 'http:', hostname: 'localhost', reload() {} },
    history: { replaceState(_s, _t, h) { sandbox.location.hash = h; } },
    navigator: { languages: [lang], language: lang },
    localStorage: { getItem() { return null; } },
    URLSearchParams,
    Date,
    Math,
    Number,
    String,
    Array,
    Object,
    Set,
    Map,
    JSON,
    Promise,
    Error,
    RegExp,
    parseInt,
    console,
    setTimeout(fn, ms) { const id = timers.push({ fn, ms }); return id; },
    clearTimeout() {},
    async fetch(url, init) {
      const qs = new URLSearchParams(url.split('?')[1] || '');
      const action = qs.get('action');
      const params = Object.fromEntries(qs.entries());
      const reqBody = init && init.body ? JSON.parse(init.body) : null;
      const result = await apiResponder(action, params, reqBody);
      return {
        ok: true,
        status: 200,
        async text() { return JSON.stringify(result); },
      };
    },
  };
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(i18nSrc, sandbox, { filename: 'i18n.js' });
  vm.runInContext(appSrc, sandbox, { filename: 'app.js' });
  return { document, sandbox };
}

async function flush() {
  for (let i = 0; i < 12; i++) await new Promise((r) => setImmediate(r));
}

// Synthetic two-volume Linux filesystem fixture:
//   Vol 1: "/" (id=1, 200 GB total, 100 GB scanned)
//     -> "/mnt" (id=10, DIR, 4 KB)
//       -> "/mnt/extend" (id=11, MOUNT, 0)
//   Vol 2: "/mnt/extend" (id=2, 20 TB total, 20 TB scanned)
//     -> "/mnt/extend/models" (id=20, DIR, dsize=15TB, asize=14TB)
//       -> "/mnt/extend/models/llm" (id=21, DIR, dsize=12TB, asize=11TB)
//         -> "/mnt/extend/models/llm/Qwen" (id=22, FILE, dsize=10TB, asize=9TB)
//         -> "/mnt/extend/models/llm/hardlink.bin" (id=23, FILE, dsize=5TB, asize=5TB, flags=2 HARDLINK_DUP)
//       -> "/mnt/extend/models/vision" (id=24, DIR, dsize=3TB, asize=3TB)
//     -> "/mnt/extend/backups" (id=25, DIR, dsize=5TB, asize=5TB)
const TB = 1024 ** 4;
const GB = 1024 ** 3;
const KB = 1024;

function makeFixtureApi() {
  const volumes = [
    { entryId: 1, name: '/', mountPoint: '/', fsType: 'ext4', device: '/dev/sda1', fsTotal: 200 * GB, fsUsed: 120 * GB, fsFree: 80 * GB },
    { entryId: 2, name: '/mnt/extend', mountPoint: '/mnt/extend', fsType: 'ext4', device: '/dev/sdb1', fsTotal: 24 * TB, fsUsed: 21 * TB, fsFree: 3 * TB },
  ];
  const entries = {
    0: { id: 0, name: '', kind: 1, dsize: 20 * TB + 100 * GB, asize: 19 * TB + 100 * GB, items: 500000, hasChildren: true },
    1: { id: 1, name: '/', kind: 1, dsize: 100 * GB, asize: 100 * GB, items: 100000, hasChildren: true },
    2: { id: 2, name: '/mnt/extend', kind: 1, dsize: 20 * TB, asize: 19 * TB, items: 400000, hasChildren: true },
    10: { id: 10, name: 'mnt', kind: 1, dsize: 4 * KB, asize: 4 * KB, items: 1, hasChildren: true },
    11: { id: 11, name: 'extend', kind: 4, dsize: 0, asize: 0, items: 0, hasChildren: false },
    20: { id: 20, name: 'models', kind: 1, dsize: 15 * TB, asize: 14 * TB, items: 300000, hasChildren: true },
    21: { id: 21, name: 'llm', kind: 1, dsize: 12 * TB, asize: 11 * TB, items: 200000, hasChildren: true },
    22: { id: 22, name: 'Qwen', kind: 0, dsize: 10 * TB, asize: 9 * TB, items: 0, flags: 0, hasChildren: false },
    23: { id: 23, name: 'hardlink.bin', kind: 0, dsize: 5 * TB, asize: 5 * TB, items: 0, flags: 2, hasChildren: false },
    24: { id: 24, name: 'vision', kind: 1, dsize: 3 * TB, asize: 3 * TB, items: 100000, hasChildren: true },
    25: { id: 25, name: 'backups', kind: 1, dsize: 5 * TB, asize: 5 * TB, items: 100000, hasChildren: true },
  };
  const paths = {
    0: [],
    1: [entries[1]],
    2: [entries[2]],
    10: [entries[1], entries[10]],
    20: [entries[2], entries[20]],
    21: [entries[2], entries[20], entries[21]],
  };
  const children = {
    0: [entries[2], entries[1]],
    1: [entries[10]],
    2: [entries[20], entries[25]],
    10: [entries[11]],
    20: [entries[21], entries[24]],
    21: [entries[22], entries[23]],
  };

  return (action, params, body) => {
    if (action === 'settings') return { defaultSizeMode: 'disk', retention: 10 };
    if (action === 'scans') return { scans: [{ id: 1, status: 'done', startedAt: 1700000000, trigger: 'manual', totalDSize: 20 * TB }] };
    if (action === 'status') return { running: false, progress: null, deleteEnabled: true };
    if (action === 'list') {
      const dirId = Number(params.dir || 0);
      const kids = children[dirId] || [];
      const resp = { dir: entries[dirId], path: paths[dirId] || [], items: kids, total: kids.length };
      if (dirId === 0) { resp.volumes = volumes; resp.skipped = []; }
      return resp;
    }
    if (action === 'delete_preview') {
      const targets = (body.entryIds || []).map((id) => {
        const e = entries[id];
        return {
          entryId: e.id, kind: e.kind, path: '/mnt/extend/models/llm/' + e.name,
          displayPath: '/mnt/extend/models/llm/' + e.name,
          dsize: e.dsize, asize: e.asize, items: e.kind === 1 ? e.items + 1 : 1,
        };
      });
      return {
        scanId: 1, items: targets, folded: 0, missing: 0,
        totalDSize: targets.reduce((s, x) => s + x.dsize, 0),
        totalASize: targets.reduce((s, x) => s + x.asize, 0),
        totalItems: targets.reduce((s, x) => s + x.items, 0),
        hasDockerRisk: false, hasProtected: false,
      };
    }
    throw new Error('unexpected action: ' + action);
  };
}

function dataRows(doc) {
  return doc.getElementById('rows').querySelectorAll('tr[data-idx]');
}

function findRowByName(doc, name) {
  return dataRows(doc).find((tr) => tr.querySelector('.c-name .t')?.textContent === name);
}

test('CSS constrains delete basket modal icon size and button shrink', () => {
  // .del-item svg or .ico-* must declare 16px width/height and flex: none so
  // the SVG cannot expand across the flex row and squash the path column.
  const hasDelItemSvgRule = /\.del-item\s+(?:>\s*)?svg\s*\{[^}]*width\s*:\s*16px[^}]*height\s*:\s*16px[^}]*flex\s*:\s*none/s.test(cssText);
  const hasIcoRule = /\.ico-dir\s*,\s*\.ico-file\s*,\s*\.ico-link\s*,\s*\.ico-mount\s*\{[^}]*width\s*:\s*16px[^}]*height\s*:\s*16px[^}]*flex\s*:\s*none/s.test(cssText);
  assert.ok(hasDelItemSvgRule || hasIcoRule, 'style.css must size SVG icons inside .del-item (16x16, flex: none)');
  assert.match(cssText, /\.del-item\s+(?:>\s*)?\.btn\s*\{[^}]*flex\s*:\s*none/s, 'style.css must prevent .del-item .btn from shrinking');
});

test('folder containing staged descendants shows indeterminate checkbox and clicking it selects whole folder', async () => {
  const { document } = buildAppFixture(makeFixtureApi());
  await flush();

  // Drill into /mnt/extend -> models -> llm and stage Qwen (10 TB).
  findRowByName(document, '/mnt/extend').click();
  await flush();
  findRowByName(document, 'models').click();
  await flush();
  findRowByName(document, 'llm').click();
  await flush();

  const qwenRow = findRowByName(document, 'Qwen');
  qwenRow.querySelector('input.row-chk').click();

  // Go up to /mnt/extend/models.
  document.getElementById('rows').querySelector('tr.up').click();
  await flush();

  const llmRow = findRowByName(document, 'llm');
  const llmChk = llmRow.querySelector('input.row-chk');
  assert.equal(llmChk.checked, false, 'partial folder checkbox must not be fully checked');
  assert.equal(llmChk.indeterminate, true, 'partial folder checkbox must be indeterminate');
  assert.equal(llmChk.getAttribute('aria-checked'), 'mixed', 'partial folder checkbox must expose aria-checked=mixed');
  assert.ok(llmRow.classList.contains('partial'), 'partial folder row should have .partial class');

  const thChk = document.querySelector('#list th.c-name input.th-chk');
  assert.equal(thChk.checked, false);
  assert.equal(thChk.indeterminate, true, 'header checkbox must be indeterminate when a child folder is partial');

  // Clicking indeterminate checkbox selects the entire llm folder (folding Qwen).
  llmChk.click();
  const llmRowAfter = findRowByName(document, 'llm');
  const llmChkAfter = llmRowAfter.querySelector('input.row-chk');
  assert.equal(llmChkAfter.checked, true);
  assert.equal(llmChkAfter.indeterminate, false);
  assert.ok(llmRowAfter.classList.contains('staged'));
});

test('size column, share bar, dirSummary, and volume cards reflect staged deletions without altering denominator', async () => {
  const { document } = buildAppFixture(makeFixtureApi());
  await flush();

  // Navigate to /mnt/extend/models/llm and stage Qwen (10 TB dsize / 9 TB asize).
  findRowByName(document, '/mnt/extend').click();
  await flush();
  findRowByName(document, 'models').click();
  await flush();
  findRowByName(document, 'llm').click();
  await flush();

  findRowByName(document, 'Qwen').querySelector('input.row-chk').click();

  // Directly staged row: Qwen (10 TB out of 12 TB = 83.33%)
  const qwenRow = findRowByName(document, 'Qwen');
  assert.ok(qwenRow.querySelector('.c-size .size-staged'), 'fully staged row should mark original size with .size-staged');
  const qwenFills = qwenRow.querySelectorAll('.c-pct .track .fill');
  assert.equal(qwenFills.length, 1, 'fully staged row should have only a single red bar segment');
  assert.ok(qwenFills[0].classList.contains('del'), 'fully staged row share bar must have red .fill.del segment');
  assert.equal(qwenFills[0].style.width, '83.33%');

  // Check dirSummary inside llm: should show staged -10.0 TB
  assert.ok(document.getElementById('dirSummary').querySelector('.summary-del'), 'dirSummary should highlight staged size');
  assert.match(document.getElementById('dirSummary').textContent, /10\.0 TB/);

  // Navigate up to /mnt/extend (total 20 TB: models=15 TB with 10 TB staged, backups=5 TB untouched)
  document.getElementById('rows').querySelector('tr.up').click();
  await flush();
  document.getElementById('rows').querySelector('tr.up').click();
  await flush();

  const modelsRow = findRowByName(document, 'models');
  assert.match(modelsRow.querySelector('.c-size').textContent, /15\.0 TB/);
  const modelsDelSize = modelsRow.querySelector('.c-size .size-del');
  assert.ok(modelsDelSize, 'partial folder should show red -staged size sub-line');
  assert.equal(modelsDelSize.textContent, '-10.0 TB');

  // Share bar for models: 5/20 = 25.00% blue remaining, 10/20 = 50.00% red staged, total 75.0%
  const modelsFills = modelsRow.querySelectorAll('.c-pct .track .fill');
  assert.equal(modelsFills.length, 2, 'partial folder track should have blue + red segments');
  assert.equal(modelsFills[0].classList.contains('del'), false);
  assert.equal(modelsFills[0].style.width, '25.00%');
  assert.equal(modelsFills[1].classList.contains('del'), true);
  assert.equal(modelsFills[1].style.width, '50.00%');
  assert.equal(modelsRow.querySelector('.c-pct .num').textContent, '75.0%');

  // Sibling backups row (5 TB untouched) must stay at 25.00% blue with no red segment
  const backupsRow = findRowByName(document, 'backups');
  assert.equal(backupsRow.querySelector('.c-size .size-del'), null);
  const backupsFills = backupsRow.querySelectorAll('.c-pct .track .fill');
  assert.equal(backupsFills.length, 1);
  assert.equal(backupsFills[0].style.width, '25.00%');
  assert.equal(backupsFills[0].classList.contains('del'), false);

  // Switch sizeMode to Apparent size -> Qwen is 9 TB, models is 14 TB (remaining 5 TB), /mnt/extend is 19 TB
  document.querySelector('#sizeSeg button[data-size="apparent"]').click();
  await flush();
  const modelsApparent = findRowByName(document, 'models');
  assert.equal(modelsApparent.querySelector('.c-size .size-del').textContent, '-9.00 TB');

  // Switch back to Disk usage and go up to All volumes (dir=0)
  document.querySelector('#sizeSeg button[data-size="disk"]').click();
  await flush();
  document.getElementById('rows').querySelector('tr.up').click();
  await flush();

  // Linux nested mount isolation: /mnt/extend row and card show 10 TB staged; "/" row and card show NONE!
  const rootVolRow = findRowByName(document, '/');
  assert.equal(rootVolRow.querySelector('.c-size .size-del'), null, 'root volume "/" must not inherit staged items from "/mnt/extend"');
  const extVolRow = findRowByName(document, '/mnt/extend');
  assert.equal(extVolRow.querySelector('.c-size .size-del')?.textContent, '-10.0 TB');

  const volCards = document.getElementById('volumes').querySelectorAll('.vol');
  assert.equal(volCards[0].querySelector('.cap .d'), null, 'root volume card must not show red staged bar');
  assert.ok(volCards[1].querySelector('.cap .d'), '/mnt/extend volume card must show red staged bar');

  // Drill into "/" -> check "/mnt" does NOT get cross-contaminated by "/mnt/extend/models/llm/Qwen"
  rootVolRow.click();
  await flush();
  const mntRow = findRowByName(document, 'mnt');
  const mntChk = mntRow.querySelector('input.row-chk');
  assert.equal(mntChk.indeterminate, false, '/mnt on volume "/" must not be indeterminate for items on volume "/mnt/extend"');
  assert.equal(mntRow.querySelector('.c-size .size-del'), null);
});

test('hardlink duplicate staged inside folder marks folder indeterminate without double-subtracting size', async () => {
  const { document } = buildAppFixture(makeFixtureApi());
  await flush();

  findRowByName(document, '/mnt/extend').click();
  await flush();
  findRowByName(document, 'models').click();
  await flush();
  findRowByName(document, 'llm').click();
  await flush();

  // Stage ONLY hardlink.bin (FLAG.HARDLINK_DUP)
  findRowByName(document, 'hardlink.bin').querySelector('input.row-chk').click();

  // Go up to models
  document.getElementById('rows').querySelector('tr.up').click();
  await flush();

  const llmRow = findRowByName(document, 'llm');
  const llmChk = llmRow.querySelector('input.row-chk');
  assert.equal(llmChk.indeterminate, true, 'folder containing staged hardlink dup should still be indeterminate');
  assert.equal(llmRow.querySelector('.c-size .size-del'), null, 'hardlink dup should not subtract from parent folder size');
  assert.equal(llmRow.querySelector('.c-pct .track .fill.del'), null, 'hardlink dup should not add red bar to parent folder');
});

test('removing item in delete basket modal syncs crumbs, volume cards, and rows; modal icons carry 16x16 attributes', async () => {
  const { document } = buildAppFixture(makeFixtureApi());
  await flush();

  findRowByName(document, '/mnt/extend').click();
  await flush();
  findRowByName(document, 'models').click();
  await flush();
  findRowByName(document, 'llm').click();
  await flush();

  findRowByName(document, 'Qwen').querySelector('input.row-chk').click();
  assert.ok(document.getElementById('dirSummary').querySelector('.summary-del'));

  // Open delete basket dialog
  document.querySelector('#basketBar .danger-primary').click();
  await flush();

  const delSvg = document.querySelector('#dlgBody .del-item svg');
  assert.ok(delSvg, 'delete basket modal row must render icon svg');
  assert.equal(delSvg.getAttribute('width'), '16');
  assert.equal(delSvg.getAttribute('height'), '16');

  // Remove Qwen from basket inside modal
  document.querySelector('#dlgBody .del-item button').click();
  await flush();

  assert.equal(document.getElementById('dirSummary').querySelector('.summary-del'), null, 'removing last item in modal must clear dirSummary staged indicator');
  assert.equal(findRowByName(document, 'Qwen').querySelector('.c-size .size-staged'), null);
});

test('i18n zh and en dictionaries have identical keys and cover all t() calls in app.js', () => {
  const loadKeys = (lang) => {
    const sb = {
      document: { documentElement: { dataset: {} }, cookie: '' },
      location: { search: '?lang=' + lang },
      localStorage: { getItem() { return null; } },
      navigator: { languages: [lang], language: lang },
      URLSearchParams,
    };
    sb.window = sb;
    vm.createContext(sb);
    vm.runInContext(i18nSrc, sb);
    return sb.I18N;
  };
  const zhI18n = loadKeys('zh');
  const enI18n = loadKeys('en');
  const zhKeys = new Set(Object.keys(zhI18n._dicts.zh));
  const enKeys = new Set(Object.keys(zhI18n._dicts.en));
  const onlyZh = [...zhKeys].filter((k) => !enKeys.has(k));
  const onlyEn = [...enKeys].filter((k) => !zhKeys.has(k));
  assert.deepEqual(onlyZh, [], 'keys only in zh: ' + onlyZh.join(', '));
  assert.deepEqual(onlyEn, [], 'keys only in en: ' + onlyEn.join(', '));

  for (const m of appSrc.matchAll(/\bt\(\s*'([^']+)'\s*[,)]/g)) {
    const k = m[1];
    assert.ok(zhKeys.has(k), `missing zh i18n key used in app.js: ${k}`);
    assert.ok(enKeys.has(k), `missing en i18n key used in app.js: ${k}`);
    assert.notEqual(zhI18n.t(k, { n: 2, size: '1 GB', remain: '2 GB', total: '3 GB', staged: '1 GB', pct: '50.0', delPct: '25.0', keepPct: '25.0' }), k);
    assert.notEqual(enI18n.t(k, { n: 2, size: '1 GB', remain: '2 GB', total: '3 GB', staged: '1 GB', pct: '50.0', delPct: '25.0', keepPct: '25.0' }), k);
  }
});

test('staging a parent folder marks inherited children as fully staged with red bars and updates dirSummary inside it', async () => {
  const { document } = buildAppFixture(makeFixtureApi());
  await flush();

  findRowByName(document, '/mnt/extend').click();
  await flush();
  findRowByName(document, 'models').querySelector('input.row-chk').click();

  // Drill into staged models directory
  findRowByName(document, 'models').click();
  await flush();

  const llmRow = findRowByName(document, 'llm');
  const llmChk = llmRow.querySelector('input.row-chk');
  assert.equal(llmChk.checked, true);
  assert.equal(llmChk.disabled, true);
  assert.equal(llmChk.indeterminate, false);
  assert.ok(llmRow.classList.contains('staged'));
  assert.ok(llmRow.querySelector('.c-size .size-staged'));
  const llmFills = llmRow.querySelectorAll('.c-pct .track .fill');
  assert.equal(llmFills.length, 1);
  assert.ok(llmFills[0].classList.contains('del'));
  assert.equal(llmFills[0].style.width, '80.00%');
  assert.ok(llmRow.querySelector('.c-pct .num').classList.contains('staged'));

  // dirSummary inside staged models directory should reflect full 15.0 TB staged
  assert.match(document.getElementById('dirSummary').querySelector('.summary-del').textContent, /15\.0 TB/);
});

test('clearing basket via basket bar resets partial checkbox, size, share bar, and volume cards', async () => {
  const { document } = buildAppFixture(makeFixtureApi());
  await flush();

  findRowByName(document, '/mnt/extend').click();
  await flush();
  findRowByName(document, 'models').click();
  await flush();
  findRowByName(document, 'llm').click();
  await flush();
  findRowByName(document, 'Qwen').querySelector('input.row-chk').click();

  // Return to All volumes (dir=0)
  document.getElementById('crumbs').querySelector('a').click();
  await flush();
  assert.ok(document.getElementById('volumes').querySelectorAll('.vol')[1].querySelector('.cap .d'));
  assert.ok(document.getElementById('dirSummary').querySelector('.summary-del'));

  // Click Clear in basketBar
  document.querySelector('#basketBar .basket-actions button').click();
  await flush();

  assert.equal(document.getElementById('volumes').querySelectorAll('.vol')[1].querySelector('.cap .d'), null);
  assert.equal(document.getElementById('dirSummary').querySelector('.summary-del'), null);
  assert.equal(findRowByName(document, '/mnt/extend').querySelector('.c-size .size-del'), null);
});
