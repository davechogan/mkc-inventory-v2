const PAGE = document.body?.dataset?.page || 'inventory';

const INVENTORY_COLUMNS = [
  { id: 'image', label: 'Image', defaultOn: true, sortValue: null, render: (item) => '' },
  { id: 'knife_name', label: 'Knife Name', defaultOn: true, sortValue: (i) => [i.knife_name, i.nickname].filter(Boolean).join(' ') || '', render: (item) => ({ text: [item.knife_name, item.nickname].filter(Boolean).join(' — ') || item.knife_name, html: null }) },
  { id: 'type', label: 'Type', defaultOn: false, sortValue: (i) => (i.knife_type || '').toLowerCase(), render: (item) => ({ text: item.knife_type || '' }) },
  { id: 'family', label: 'Family', defaultOn: false, sortValue: (i) => (i.knife_family || '').toLowerCase(), render: (item) => ({ text: item.knife_family || '' }) },
  { id: 'form', label: 'Form', defaultOn: false, sortValue: (i) => (i.form_name || '').toLowerCase(), render: (item) => ({ text: item.form_name || '' }) },
  { id: 'series', label: 'Series', defaultOn: true, sortValue: (i) => formatSeries(i) || '', render: (item) => ({ text: formatSeries(item) }) },
  { id: 'handle_color', label: 'Handle', defaultOn: true, sortValue: (i) => resolveOptionLabel('handle-colors', i.handle_color) || i.handle_color || '', render: (item) => ({ text: resolveOptionLabel('handle-colors', item.handle_color) || item.handle_color || '' }) },
  { id: 'quantity', label: 'Qty', defaultOn: true, sortValue: (i) => i.quantity ?? 0, render: (item) => ({ text: String(item.quantity || 1) }) },
  { id: 'blade_length', label: 'Blade (in)', defaultOn: true, sortValue: (i) => i.blade_length ?? -1, render: (item) => ({ text: item.blade_length != null ? String(item.blade_length) : '' }) },
  { id: 'blade_color', label: 'Blade color', defaultOn: true, sortValue: (i) => resolveOptionLabel('blade-colors', i.blade_color) || '', render: (item) => ({ text: resolveOptionLabel('blade-colors', item.blade_color) }) },
  { id: 'steel', label: 'Steel', defaultOn: true, sortValue: (i) => resolveOptionLabel('blade-steels', i.blade_steel) || '', render: (item) => ({ text: resolveOptionLabel('blade-steels', item.blade_steel) }) },
  { id: 'finish', label: 'Finish', defaultOn: true, sortValue: (i) => resolveOptionLabel('blade-finishes', i.blade_finish) || '', render: (item) => ({ text: resolveOptionLabel('blade-finishes', item.blade_finish) }) },
  { id: 'price', label: 'Price', defaultOn: true, sortValue: (i) => i.estimated_value ?? -1, render: (item) => ({ text: item.estimated_value != null ? currency(item.estimated_value) : '' }) },
  { id: 'condition', label: 'Condition', defaultOn: false, sortValue: (i) => i.condition || 'Like New', render: (item) => ({ text: item.condition || 'Like New' }) },
  { id: 'location', label: 'Location', defaultOn: false, sortValue: (i) => i.location || '', render: (item) => ({ text: item.location || '' }) },
  { id: 'purchase_price', label: 'Paid', defaultOn: false, sortValue: (i) => i.purchase_price ?? -1, render: (item) => ({ text: item.purchase_price != null ? currency(item.purchase_price) : '' }) },
  { id: 'acquired_date', label: 'Acquired', defaultOn: false, sortValue: (i) => i.acquired_date || '', render: (item) => ({ text: item.acquired_date || '' }) },
  { id: 'mkc_order_number', label: 'MKC order', defaultOn: false, sortValue: (i) => i.mkc_order_number || '', render: (item) => ({ text: item.mkc_order_number || '' }) },
  { id: 'collaboration_name', label: 'Collab', defaultOn: false, sortValue: (i) => (i.is_collab ? (i.collaboration_name || 'Yes') : ''), render: (item) => ({ text: item.is_collab ? (item.collaboration_name || 'Yes') : '' }) },
  { id: 'purchase_source', label: 'Source', defaultOn: false, sortValue: (i) => i.purchase_source || '', render: (item) => ({ text: item.purchase_source || '' }) },
];
const INVENTORY_COLUMNS_STORAGE_KEY = 'mkc_inventory_visible_columns';
const MASTER_COLUMNS_STORAGE_KEY = 'mkc_master_visible_columns';

const MASTER_COLUMNS = [
  { id: 'select', label: 'Select', defaultOn: true, sortValue: null, render: (m) => ({ text: '', html: null }) },
  { id: 'image', label: 'Image', defaultOn: true, sortValue: null, render: (m) => ({ text: '', html: null }) },
  { id: 'name', label: 'Name', defaultOn: true, sortValue: (m) => (m.name || '').toLowerCase(), render: (m) => ({ text: m.name || '', html: `<strong>${escapeHtml(m.name || '')}</strong><div class="muted">${escapeHtml(m.notes || '')}</div>` }) },
  { id: 'canonical_slug', label: 'Slug', defaultOn: true, sortValue: (m) => (m.canonical_slug || '').toLowerCase(), render: (m) => ({ text: m.canonical_slug || '', html: `<span class="muted small">${escapeHtml(m.canonical_slug || '')}</span>` }) },
  { id: 'category', label: 'Type', defaultOn: true, sortValue: (m) => (m.category || '').toLowerCase(), render: (m) => ({ text: m.category || '' }) },
  { id: 'family', label: 'Family', defaultOn: true, sortValue: (m) => (m.family || '').toLowerCase(), render: (m) => ({ text: m.family || '' }) },
  { id: 'form_name', label: 'Form', defaultOn: true, sortValue: (m) => (m.form_name || '').toLowerCase(), render: (m) => ({ text: m.form_name || '' }) },
  { id: 'series_name', label: 'Series', defaultOn: true, sortValue: (m) => (m.series_name || '').toLowerCase(), render: (m) => ({ text: m.series_name || '' }) },
  { id: 'collaboration_name', label: 'Collaborator', defaultOn: true, sortValue: (m) => (m.collaboration_name || '').toLowerCase(), render: (m) => ({ text: m.collaboration_name || '' }) },
  { id: 'generation_label', label: 'Generation', defaultOn: true, sortValue: (m) => (m.generation_label || '').toLowerCase(), render: (m) => ({ text: m.generation_label || '' }) },
  { id: 'size_modifier', label: 'Size', defaultOn: true, sortValue: (m) => (m.size_modifier || '').toLowerCase(), render: (m) => ({ text: m.size_modifier || '' }) },
  { id: 'msrp', label: 'MSRP', defaultOn: true, sortValue: (m) => m.msrp ?? -1, render: (m) => ({ text: m.msrp != null ? currency(m.msrp) : '' }) },
  { id: 'default_blade_length', label: 'Blade (in)', defaultOn: true, sortValue: (m) => m.default_blade_length ?? -1, render: (m) => ({ text: m.default_blade_length != null ? `${m.default_blade_length}"` : '' }) },
  { id: 'default_steel', label: 'Steel', defaultOn: true, sortValue: (m) => (resolveOptionLabel('blade-steels', m.default_steel) || '').toLowerCase(), render: (m) => ({ text: resolveOptionLabel('blade-steels', m.default_steel) || m.default_steel || '' }) },
  { id: 'default_blade_finish', label: 'Finish', defaultOn: true, sortValue: (m) => (resolveOptionLabel('blade-finishes', m.default_blade_finish) || '').toLowerCase(), render: (m) => ({ text: resolveOptionLabel('blade-finishes', m.default_blade_finish) || m.default_blade_finish || '' }) },
  { id: 'handle_type', label: 'Handle type', defaultOn: true, sortValue: (m) => (resolveOptionLabel('handle-types', m.handle_type) || '').toLowerCase(), render: (m) => ({ text: resolveOptionLabel('handle-types', m.handle_type) || m.handle_type || '' }) },
  { id: 'in_inventory_count', label: 'In Inventory', defaultOn: true, sortValue: (m) => (m.in_inventory_count ?? 0), render: (m) => ({ text: (m.in_inventory_count ?? 0) > 0 ? '✓' : '—' }) },
  { id: 'colors', label: 'Colors', defaultOn: true, sortValue: (m) => [resolveOptionLabel('blade-colors', m.default_blade_color), resolveOptionLabel('handle-colors', m.default_handle_color)].filter(Boolean).join(' ').toLowerCase(), render: (m) => ({ text: [resolveOptionLabel('blade-colors', m.default_blade_color), resolveOptionLabel('handle-colors', m.default_handle_color)].filter(Boolean).join(' / ') }) },
  { id: 'status', label: 'Status', defaultOn: true, sortValue: (m) => (m.status || '').toLowerCase(), render: (m) => ({ text: m.status || '' }) },
  { id: 'actions', label: 'Actions', defaultOn: true, sortValue: null, render: (m) => ({ text: '', html: null }) },
];

function deriveBladeFamilyFromName(name) {
  if (!name || !String(name).trim()) return '';
  let s = String(name).trim();
  s = s.replace(/\s+Tactical\s*$/i, '');
  s = s.replace(/\s+2\.0\s*$/i, '');
  s = s.replace(/\s+2\s*$/i, '');
  s = s.replace(/\s+3\.0\s*$/i, '');
  return s.trim() || name.trim();
}

function deriveAndSuggestFamily(form, nameVal) {
  const familyEl = form.elements.family_name || form.elements.family;
  if (!familyEl) return;
  const derived = deriveBladeFamilyFromName(nameVal);
  if (!derived) return;
  const opts = state.options['blade-families'] || [];
  const match = opts.find((o) => (o.name || o.id || '').toString().trim() === derived);
  if (match) {
    familyEl.value = match.name || match.id || derived;
  } else if (!Array.from(familyEl.options).some((o) => o.value === derived)) {
    const opt = document.createElement('option');
    opt.value = derived;
    opt.textContent = `${derived} (derived)`;
    familyEl.appendChild(opt);
    familyEl.value = derived;
  }
}

function formatSeries(item) {
  if (item.catalog_line) return item.catalog_line;
  if (item.is_collab && item.collaboration_name) return item.collaboration_name;
  return item.is_collab ? 'Collab' : 'Standard';
}

/** Resolve option id to display label (e.g. 1 → "Orange"). Handles legacy values stored as name. */
function resolveOptionLabel(optionType, value) {
  if (value == null || value === '') return '';
  const opts = state.options[optionType] || [];
  const v = String(value).trim();
  const match = opts.find(
    (o) => String(o.id) === v || String(o.name || '').toLowerCase() === v.toLowerCase(),
  );
  return match ? match.name : v;
}

function initColumnPicker() {
  const wrap = document.getElementById('columnPickerCheckboxes');
  if (!wrap) return;
  const visible = getVisibleColumnIds();
  wrap.innerHTML = INVENTORY_COLUMNS.map((col) => {
    const checked = visible.includes(col.id);
    return `
      <label>
        <input type="checkbox" data-column-id="${escapeHtml(col.id)}" ${checked ? 'checked' : ''} />
        ${escapeHtml(col.label)}
      </label>`;
  }).join('');
  wrap.querySelectorAll('input[data-column-id]').forEach((cb) => {
    cb.addEventListener('change', () => {
      let visible = getVisibleColumnIds();
      if (cb.checked) {
        visible = [...visible, cb.dataset.columnId];
      } else {
        visible = visible.filter((id) => id !== cb.dataset.columnId);
        if (!visible.length) visible = ['knife_name'];
      }
      setVisibleColumnIds(visible);
      initColumnPicker();
      renderInventoryTable();
    });
  });
}

function getVisibleColumnIds() {
  try {
    const stored = localStorage.getItem(INVENTORY_COLUMNS_STORAGE_KEY);
    if (stored) {
      const parsed = JSON.parse(stored);
      if (Array.isArray(parsed) && parsed.length) {
        const valid = new Set(INVENTORY_COLUMNS.map((c) => c.id));
        // Migrate older saved column sets so new inventory columns appear for spot checks.
        const ensureIds = new Set(["acquired_date", "mkc_order_number"]);
        const merged = [...new Set([...parsed, ...Array.from(ensureIds)])]
          .filter((id) => valid.has(id));
        return merged.length ? merged : ['knife_name'];
      }
    }
  } catch (_) {}
  return INVENTORY_COLUMNS.filter((c) => c.defaultOn).map((c) => c.id);
}

function setVisibleColumnIds(ids) {
  localStorage.setItem(INVENTORY_COLUMNS_STORAGE_KEY, JSON.stringify(ids));
}

function getMasterVisibleColumnIds() {
  const valid = new Set(MASTER_COLUMNS.map((c) => c.id));
  try {
    const stored = localStorage.getItem(MASTER_COLUMNS_STORAGE_KEY);
    if (stored) {
      const parsed = JSON.parse(stored);
      if (Array.isArray(parsed) && parsed.length) {
        const filtered = parsed.filter((id) => valid.has(id));
        if (filtered.length) return filtered;
      }
    }
  } catch (_) {}
  return MASTER_COLUMNS.filter((c) => c.defaultOn).map((c) => c.id);
}

function setMasterVisibleColumnIds(ids) {
  localStorage.setItem(MASTER_COLUMNS_STORAGE_KEY, JSON.stringify(ids));
}

function initMasterColumnPicker() {
  const wrap = document.getElementById('masterColumnPickerCheckboxes');
  if (!wrap) return;
  const visible = getMasterVisibleColumnIds();
  wrap.innerHTML = MASTER_COLUMNS.map((col) => {
    const checked = visible.includes(col.id);
    return `
      <label>
        <input type="checkbox" data-column-id="${escapeHtml(col.id)}" ${checked ? 'checked' : ''} />
        ${escapeHtml(col.label || col.id)}
      </label>`;
  }).join('');
  wrap.querySelectorAll('input[data-column-id]').forEach((cb) => {
    cb.addEventListener('change', () => {
      let visibleIds = getMasterVisibleColumnIds();
      if (cb.checked) {
        visibleIds = [...visibleIds, cb.dataset.columnId];
      } else {
        visibleIds = visibleIds.filter((id) => id !== cb.dataset.columnId);
        if (!visibleIds.length) visibleIds = ['name'];
      }
      setMasterVisibleColumnIds(visibleIds);
      initMasterColumnPicker();
      renderMasterTable();
    });
  });
}

let masterPanelObjectUrl = null;

const state = {
  summary: {},
  masterKnives: [],
  inventory: [],
  inventorySort: { columnId: 'knife_name', dir: 'asc' },
  masterSort: { columnId: 'name', dir: 'asc' },
  options: {
    'handle-colors': [],
    'blade-steels': [],
    'blade-finishes': [],
    'blade-colors': [],
    'blade-types': [],
    'handle-types': [],
    'categories': [],
    'blade-families': [],
    'primary-use-cases': [],
    'collaborators': [],
    'generations': [],
    'size-modifiers': [],
  },
};

let inventoryUrlFiltersApplied = false;

function applyInventoryFiltersFromUrl() {
  if (inventoryUrlFiltersApplied) return;
  const params = new URLSearchParams(window.location.search);
  const map = [
    ['search', 'inventorySearch'],
    ['type', 'inventoryFilterType'],
    ['family', 'inventoryFilterFamily'],
    ['form', 'inventoryFilterForm'],
    ['series', 'inventoryFilterSeries'],
    ['steel', 'inventoryFilterSteel'],
    ['finish', 'inventoryFilterFinish'],
    ['handle_color', 'inventoryFilterHandleColor'],
    ['condition', 'inventoryFilterCondition'],
    ['location', 'inventoryFilterLocation'],
  ];
  map.forEach(([p, id]) => {
    const val = params.get(p);
    const el = document.getElementById(id);
    if (!el || !val) return;
    el.value = val;
  });
  inventoryUrlFiltersApplied = true;
}

function mapV2ModelToMasterRow(r) {
  if (!r) return r;
  return {
    id: r.id,
    v2_id: r.id,
    parent_model_id: r.parent_model_id,
    official_name: r.official_name,
    knife_type: r.knife_type,
    form_name: r.form_name,
    family_name: r.family_name,
    series_name: r.series_name,
    collaborator_name: r.collaborator_name,
    generation_label: r.generation_label,
    size_modifier: r.size_modifier,
    platform_variant: r.platform_variant,
    // Back-compat keys used by existing table rendering/filtering code
    name: r.official_name,
    family: r.family_name,
    category: r.knife_type,
    catalog_line: r.series_name,
    collaboration_name: r.collaborator_name,
    is_collab: !!r.collaborator_name,
    default_steel: r.steel,
    default_blade_finish: r.blade_finish,
    default_blade_color: r.blade_color,
    default_handle_color: r.handle_color,
    handle_type: r.handle_type,
    default_blade_length: r.blade_length,
    is_discontinued: !!r.is_discontinued,
    is_current_catalog: !!r.is_current_catalog,
    msrp: r.msrp,
    in_inventory_count: r.in_inventory_count ?? 0,
    has_identifier_image: !!r.has_identifier_image,
    record_status: r.record_status,
    status: r.record_status,
    canonical_slug: r.slug,
    version: r.generation_label,
    notes: r.notes || '',
    identifier_distinguishing_features: r.distinguishing_features || '',
  };
}

/** Turn FastAPI `detail` (string | object | array of validation errors) into a readable message. */
function formatApiErrorDetail(detail, fallback) {
  const fb = fallback != null && fallback !== '' ? String(fallback) : 'Request failed';
  if (detail == null || detail === '') return fb;
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) {
    const lines = detail
      .map((item) => {
        if (typeof item === 'string') return item;
        if (item && typeof item === 'object') {
          const msg = item.msg != null ? String(item.msg) : '';
          const loc = Array.isArray(item.loc) ? item.loc.filter(Boolean).join(' → ') : '';
          if (msg && loc) return `${loc}: ${msg}`;
          if (msg) return msg;
          try {
            return JSON.stringify(item);
          } catch {
            return String(item);
          }
        }
        try {
          return JSON.stringify(item);
        } catch {
          return String(item);
        }
      })
      .filter(Boolean);
    if (lines.length) return lines.join('\n');
    return fb;
  }
  if (typeof detail === 'object') {
    if (detail.msg != null) return String(detail.msg);
    try {
      const s = JSON.stringify(detail);
      return s === '{}' ? fb : s;
    } catch {
      return String(detail);
    }
  }
  return String(detail);
}

/** Safe string for alerts / UI when catch receives Error or plain API body. */
function userFacingError(err) {
  if (err == null) return 'Unknown error';
  if (typeof err === 'string') return err;
  if (err instanceof Error) {
    const m = err.message;
    if (typeof m === 'string' && m.length > 0 && m !== '[object Object]') return m;
  }
  if (typeof err === 'object' && err !== null && err.detail != null) {
    return formatApiErrorDetail(err.detail);
  }
  if (typeof err === 'object' && err !== null && typeof err.message === 'string') {
    const m = err.message;
    if (m && m !== '[object Object]') return m;
  }
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}

async function api(path, options = {}) {
  const headers = { ...options.headers };
  if (options.body && typeof options.body === 'string' && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }
  const response = await fetch(path, {
    ...options,
    headers,
  });
  if (!response.ok) {
    const statusFallback = `HTTP ${response.status}${response.statusText ? ` ${response.statusText}` : ''}`;
    const text = await response.text();
    let parsed = {};
    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = { detail: text.length > 600 ? `${text.slice(0, 600)}…` : text };
      }
    }
    const rawDetail = parsed.detail !== undefined ? parsed.detail : parsed.message;
    const msg = formatApiErrorDetail(rawDetail, statusFallback);
    const method = (options.method || 'GET').toUpperCase();
    const detailStr = msg || statusFallback;
    throw new Error(`${method} ${path}: ${detailStr}`);
  }
  if (response.status === 204) return null;
  return response.json();
}

const OLLAMA_DEFAULT_MODEL_KEY = 'mkc_ollama_default_model';

async function checkOllamaBeforeRecompute() {
  try {
    const check = await api('/api/ai/ollama/check');
    if (!check.reachable) {
      alert(`Ollama unreachable at ${check.ollama_host || 'server'}. Is it running?`);
      return false;
    }
    if (check.model_ok === false && check.model_error) {
      alert(`Ollama model error: ${check.model_error}`);
      return false;
    }
    return true;
  } catch (err) {
    alert(`Cannot reach Ollama: ${userFacingError(err)}`);
    return false;
  }
}

async function runDistRecompute(opts) {
  const model = localStorage.getItem(OLLAMA_DEFAULT_MODEL_KEY);
  const body = { ...opts };
  if (model) body.model = model;
  const data = await api('/api/v2/admin/distinguishing-features/recompute', { method: 'POST', body: JSON.stringify(body) });
  if (data.failed?.length) {
    const msg = data.failed.map((f) => `${f.name}: ${f.reason}`).join('\n');
    alert(`Updated ${data.updated}. Failed:\n${msg}`);
  }
  return data;
}

async function loadInventoryData() {
  const [summary, inventory, filters, catalogRows, v2Options] = await Promise.all([
    api('/api/v2/inventory/summary'),
    api('/api/v2/inventory'),
    api('/api/v2/inventory/filters'),
    api('/api/v2/catalog'),
    api('/api/v2/options'),
  ]);
  state.summary = summary;
  state.inventory = inventory;
  state.masterKnives = (catalogRows || []).map((r) => mapV2ModelToMasterRow(r));
  state.v2InventoryFilters = filters;
  state.options = { ...v2Options };
  // Merge v2 filter values into options for resolveOptionLabel (ensure inventory values are present)
  const mergeOpt = (key, vals) => {
    const existing = new Map(((state.options[key] || [])).map((o) => [String(o.name || o.id).toLowerCase(), o]));
    (vals || []).forEach((n) => { if (n && !existing.has(n.toLowerCase())) existing.set(n.toLowerCase(), { name: n }); });
    state.options[key] = Array.from(existing.values());
  };
  mergeOpt('blade-steels', filters.steel);
  mergeOpt('blade-finishes', filters.finish);
  mergeOpt('handle-colors', filters.handle_color);
  mergeOpt('blade-colors', filters.blade_color);
  renderSummary();
  renderFamilyStrip();
  populateInventoryFilterSelects();
  applyInventoryFiltersFromUrl();
  renderInventoryTable();
  populateIdentifierSelects();
}

function populateInventoryFilterSelects() {
  const filters = state.v2InventoryFilters || {};
  const dropdowns = [
    ['inventoryFilterType', filters.type],
    ['inventoryFilterFamily', filters.family],
    ['inventoryFilterForm', filters.form],
    ['inventoryFilterSeries', filters.series],
    ['inventoryFilterSteel', filters.steel],
    ['inventoryFilterFinish', filters.finish],
    ['inventoryFilterHandleColor', filters.handle_color],
    ['inventoryFilterCondition', filters.condition || ['New', 'Like New', 'Very Good', 'Good', 'User']],
  ];
  dropdowns.forEach(([id, values]) => {
    const el = document.getElementById(id);
    if (!el) return;
    const isLocation = id === 'inventoryFilterLocation';
    if (isLocation) return;
    el.innerHTML = '<option value="">— All —</option>';
    (values || []).forEach((v) => {
      const opt = document.createElement('option');
      opt.value = v;
      opt.textContent = v;
      el.appendChild(opt);
    });
  });
}

async function loadMasterData() {
  const [catalogRows, filters, v2Options] = await Promise.all([
    api('/api/v2/catalog'),
    api('/api/v2/catalog/filters'),
    api('/api/v2/options'),
  ]);
  state.v2CatalogFilters = filters;
  state.options = { ...v2Options };
  state.masterKnives = catalogRows.map((r) => mapV2ModelToMasterRow(r));
  populateMasterFilterSelects();
  renderMasterTable();
  renderOptions();
}

function currency(value) {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value || 0);
}

function renderSummary() {
  const rowsEl = document.getElementById('inventoryRows');
  if (!rowsEl) return;
  document.getElementById('inventoryRows').textContent = state.summary.inventory_rows;
  document.getElementById('totalQuantity').textContent = state.summary.total_quantity;
  document.getElementById('totalSpend').textContent = currency(state.summary.total_spend);
  document.getElementById('estimatedValue').textContent = currency(state.summary.estimated_value ?? state.summary.total_estimated_value);
  document.getElementById('masterCount').textContent = state.summary.master_count ?? state.summary.master_models ?? 0;
}

function renderFamilyStrip() {
  const wrap = document.getElementById('familyStrip');
  if (!wrap) return;
  const rows = state.summary.by_family || [];
  if (!rows.length) {
    wrap.innerHTML = '<span class="muted">No inventory yet.</span>';
    return;
  }
  wrap.innerHTML = rows.map((r) => `
    <span class="family-chip"><strong>${escapeHtml(r.family)}</strong> · ${r.total_quantity} pcs · ${r.inventory_rows} row${r.inventory_rows === 1 ? '' : 's'}</span>
  `).join('');
}

function masterMatchesFilter(item, query) {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const parts = [
    item.name,
    item.family,
    item.canonical_slug,
    item.notes,
    item.record_type,
    item.catalog_status,
    item.collaboration_name,
    item.primary_use_case,
    item.category,
  ].map((x) => String(x ?? '').toLowerCase());
  return parts.some((p) => p.includes(q));
}

function masterMatchesStructuredFilters(item) {
  const category = document.getElementById('masterFilterCategory')?.value?.trim();
  const family = document.getElementById('masterFilterFamily')?.value?.trim();
  const steel = document.getElementById('masterFilterSteel')?.value?.trim();
  const finish = document.getElementById('masterFilterFinish')?.value?.trim();
  const recordType = document.getElementById('masterFilterRecordType')?.value?.trim();
  const status = document.getElementById('masterFilterStatus')?.value?.trim();
  const catalogLine = document.getElementById('masterFilterCatalogLine')?.value?.trim();
  if (category && (item.category || '').trim() !== category) return false;
  if (family && (item.family || '').trim() !== family) return false;
  const steelLabel = resolveOptionLabel('blade-steels', item.default_steel);
  if (steel && steelLabel !== steel && item.default_steel !== steel) return false;
  const finishLabel = resolveOptionLabel('blade-finishes', item.default_blade_finish);
  if (finish && finishLabel !== finish && item.default_blade_finish !== finish) return false;
  if (recordType && (item.record_type || '').trim() !== recordType) return false;
  if (status && (item.status || '') !== status) return false;
  if (catalogLine && (item.catalog_line || '').trim() !== catalogLine) return false;
  return true;
}

function populateMasterFilterSelects() {
  const filters = state.v2CatalogFilters || {};
  const categoryEl = document.getElementById('masterFilterCategory');
  const familyEl = document.getElementById('masterFilterFamily');
  const steelEl = document.getElementById('masterFilterSteel');
  const finishEl = document.getElementById('masterFilterFinish');
  const catalogLineEl = document.getElementById('masterFilterCatalogLine');
  const fillSelect = (el, values) => {
    if (!el) return;
    el.innerHTML = '<option value="">— All —</option>' + (values || []).map((v) => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
  };
  fillSelect(categoryEl, filters.type || (state.options['categories'] || []).map((o) => o.name));
  fillSelect(familyEl, filters.family || [...new Set((state.masterKnives || []).map((m) => (m.family || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' })));
  fillSelect(catalogLineEl, filters.series || ['VIP', 'Traditions', 'Blood Brothers']);
  fillSelect(steelEl, (state.options['blade-steels'] || []).map((o) => o.name));
  fillSelect(finishEl, (state.options['blade-finishes'] || []).map((o) => o.name));
}

function renderMasterTable() {
  const table = document.getElementById('masterTable');
  const thead = document.getElementById('masterTableHead');
  const tbody = table?.querySelector('tbody');
  if (!thead || !tbody) return;

  const visibleIds = getMasterVisibleColumnIds();
  const columns = MASTER_COLUMNS.filter((c) => visibleIds.includes(c.id));

  thead.innerHTML = columns.map((c) => {
    const sortable = c.sortValue != null;
    const isActive = state.masterSort.columnId === c.id;
    const asc = state.masterSort.dir === 'asc';
    const cls = [
      c.id === 'select' ? 'col-select' : '',
      c.id === 'image' ? 'col-thumb' : '',
      c.id === 'actions' ? 'col-actions' : '',
      sortable ? 'sortable' : '',
      isActive ? (asc ? 'sort-asc' : 'sort-desc') : '',
    ].filter(Boolean).join(' ');
    const ariaSort = sortable && isActive ? (asc ? 'ascending' : 'descending') : sortable ? 'none' : null;
    let content = escapeHtml(c.label);
    if (c.id === 'select') {
      content = '<input type="checkbox" id="masterSelectAll" title="Select all visible" aria-label="Select all" />';
    }
    return `<th class="${cls}" data-sort-column-id="${c.id}"${ariaSort != null ? ` aria-sort="${ariaSort}"` : ''}>${content}</th>`;
  }).join('');

  const searchEl = document.getElementById('masterSearch');
  const query = searchEl ? searchEl.value : '';
  let rows = state.masterKnives.filter((item) => masterMatchesFilter(item, query) && masterMatchesStructuredFilters(item));

  const activeCol = columns.find((c) => c.id === state.masterSort.columnId);
  if (activeCol?.sortValue) {
    const mult = state.masterSort.dir === 'asc' ? 1 : -1;
    rows = [...rows].sort((a, b) => {
      const va = activeCol.sortValue(a);
      const vb = activeCol.sortValue(b);
      const cmp = typeof va === 'number' && typeof vb === 'number'
        ? va - vb
        : String(va ?? '').localeCompare(String(vb ?? ''), undefined, { sensitivity: 'base', numeric: true });
      return mult * cmp;
    });
  }

  tbody.innerHTML = '';
  rows.forEach((item) => {
    const tr = document.createElement('tr');
    tr.classList.add('master-row-clickable');
    tr.dataset.masterId = String(item.id);
    tr.tabIndex = 0;
    tr.setAttribute('role', 'button');
    tr.setAttribute('aria-label', `Open ${item.name}`);
    const cells = columns.map((col) => {
      if (col.id === 'select') {
        const selectable = !!item.has_identifier_image;
        const cb = selectable
          ? `<input type="checkbox" class="master-row-select" data-master-id="${item.id}" data-stop-row-open="1" aria-label="Select ${escapeHtml(item.name)}" />`
          : '<span class="muted">—</span>';
        return `<td class="col-select" data-stop-row-open="1">${cb}</td>`;
      }
      if (col.id === 'image') {
        const idImg = masterIdentifierThumbSrc(item);
        const thumb = idImg
          ? `<img class="master-list-thumb" src="${escapeHtml(idImg)}" alt="" width="44" height="44" loading="lazy" />`
          : '<span class="muted small">—</span>';
        return `<td class="col-thumb">${thumb}</td>`;
      }
      if (col.id === 'actions') {
        return `<td class="col-actions" data-stop-row-open="1">
          <button type="button" class="secondary" data-action="duplicate-master" data-id="${item.id}">Duplicate</button>
          <button type="button" class="danger" data-action="delete-master" data-id="${item.id}">Delete</button>
        </td>`;
      }
      const out = col.render(item);
      return `<td>${out.html != null ? out.html : escapeHtml(out.text || '')}</td>`;
    });
    tr.innerHTML = cells.join('');
    tbody.appendChild(tr);
  });

  const recomputeBtn = document.getElementById('recomputeDistSelectedBtn');
  if (recomputeBtn) {
    const checkedCount = document.querySelectorAll('.master-row-select:checked').length;
    if (checkedCount > 0) {
      recomputeBtn.classList.remove('hidden');
      recomputeBtn.textContent = `Recompute selected (${checkedCount})`;
    } else {
      recomputeBtn.classList.add('hidden');
    }
  }
}

function inventoryMatchesFilter(item, query) {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const parts = [
    item.knife_name,
    item.knife_family,
    item.nickname,
    item.serial_number,
    item.location,
    item.handle_color,
    item.blade_steel,
    item.blade_finish,
    item.blade_color,
    item.purchase_source,
    item.condition,
    item.notes,
    item.blade_length,
  ].map((x) => String(x ?? '').toLowerCase());
  return parts.some((p) => p.includes(q));
}

function inventoryMatchesStructuredFilters(item) {
  const type = document.getElementById('inventoryFilterType')?.value?.trim();
  const family = document.getElementById('inventoryFilterFamily')?.value?.trim();
  const form = document.getElementById('inventoryFilterForm')?.value?.trim();
  const series = document.getElementById('inventoryFilterSeries')?.value?.trim();
  const steel = document.getElementById('inventoryFilterSteel')?.value?.trim();
  const finish = document.getElementById('inventoryFilterFinish')?.value?.trim();
  const handleColor = document.getElementById('inventoryFilterHandleColor')?.value?.trim();
  const condition = document.getElementById('inventoryFilterCondition')?.value?.trim();
  const location = document.getElementById('inventoryFilterLocation')?.value?.trim();
  if (type && (item.knife_type || '') !== type) return false;
  if (family && (item.knife_family || '') !== family) return false;
  if (form && (item.form_name || '') !== form) return false;
  if (series && (item.series_name || item.catalog_line || '') !== series) return false;
  if (steel && (item.blade_steel || '') !== steel) return false;
  if (finish && (item.blade_finish || '') !== finish) return false;
  if (handleColor && (item.handle_color || '') !== handleColor) return false;
  if (condition && (item.condition || 'Like New') !== condition) return false;
  if (location && !String(item.location || '').toLowerCase().includes(location.toLowerCase())) return false;
  return true;
}

function renderInventoryTable() {
  const thead = document.getElementById('inventoryTableHead');
  const tbody = document.querySelector('#inventoryTable tbody');
  if (!thead || !tbody) return;
  const visibleIds = getVisibleColumnIds();
  const columns = INVENTORY_COLUMNS.filter((c) => visibleIds.includes(c.id));

  thead.innerHTML = columns.map((c) => {
    const sortable = c.sortValue != null;
    const isActive = state.inventorySort.columnId === c.id;
    const asc = state.inventorySort.dir === 'asc';
    const cls = [
      sortable ? 'sortable' : '',
      isActive ? (asc ? 'sort-asc' : 'sort-desc') : '',
    ].filter(Boolean).join(' ');
    const ariaSort = isActive ? (asc ? 'ascending' : 'descending') : 'none';
    return `<th class="${cls}" data-sort-column-id="${c.id}"${sortable ? ` aria-sort="${ariaSort}"` : ''}>${escapeHtml(c.label)}</th>`;
  }).join('');

  const searchEl = document.getElementById('inventorySearch');
  const query = searchEl ? searchEl.value : '';
  let rows = state.inventory.filter((item) => inventoryMatchesFilter(item, query) && inventoryMatchesStructuredFilters(item));

  const activeCol = columns.find((c) => c.id === state.inventorySort.columnId);
  if (activeCol?.sortValue) {
    const mult = state.inventorySort.dir === 'asc' ? 1 : -1;
    rows = [...rows].sort((a, b) => {
      const va = activeCol.sortValue(a);
      const vb = activeCol.sortValue(b);
      const cmp = typeof va === 'number' && typeof vb === 'number'
        ? va - vb
        : String(va ?? '').localeCompare(String(vb ?? ''), undefined, { sensitivity: 'base', numeric: true });
      return mult * cmp;
    });
  }

  tbody.innerHTML = '';
  rows.forEach((item) => {
    const tr = document.createElement('tr');
    tr.classList.add('inventory-row-clickable');
    tr.dataset.inventoryId = String(item.id);
    tr.tabIndex = 0;
    tr.setAttribute('role', 'button');
    tr.setAttribute('aria-label', `Edit ${item.knife_name}`);
    const cells = columns.map((col) => {
      if (col.id === 'image') {
        const src = item.has_identifier_image && item.knife_model_id
          ? `/api/v2/models/${item.knife_model_id}/image`
          : null;
        return `<td class="col-thumb">${src ? `<img class="inventory-thumb" src="${escapeHtml(src)}" alt="" loading="lazy" />` : '<span class="muted">—</span>'}</td>`;
      }
      const out = col.render(item);
      return `<td>${out.html != null ? out.html : escapeHtml(out.text || '')}</td>`;
    });
    tr.innerHTML = cells.join('');
    tbody.appendChild(tr);
  });
}

function renderOptions() {
  const map = [
    ['handle-colors', 'handleColorsList'],
    ['blade-steels', 'bladeSteelsList'],
    ['blade-types', 'bladeTypesList'],
    ['blade-finishes', 'bladeFinishesList'],
    ['blade-colors', 'bladeColorsList'],
    ['handle-types', 'handleTypesList'],
    ['categories', 'categoriesList'],
    ['blade-families', 'bladeFamiliesList'],
    ['collaborators', 'collaboratorsList'],
    ['generations', 'generationsList'],
    ['size-modifiers', 'sizeModifiersList'],
  ];
  map.forEach(([type, elementId]) => {
    const el = document.getElementById(elementId);
    if (!el) return;
    el.innerHTML = '';
    (state.options[type] || []).forEach((option) => {
      const li = document.createElement('li');
      li.innerHTML = `
        <span>${escapeHtml(option.name)}</span>
        <button type="button" class="secondary" data-action="delete-option" data-type="${type}" data-id="${option.id}">×</button>
      `;
      el.appendChild(li);
    });
  });
}

function populateSelect(select, optionList, includeBlank = true, useNameAsValue = false) {
  if (!select) return;
  select.innerHTML = '';
  if (includeBlank) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = '';
    select.appendChild(opt);
  }
  optionList.forEach((option) => {
    const opt = document.createElement('option');
    if (typeof option === 'object') {
      opt.value = useNameAsValue ? (option.name ?? String(option.id)) : (option.id ?? option.name);
      opt.textContent = option.name ?? option;
    } else {
      opt.value = option;
      opt.textContent = option;
    }
    select.appendChild(opt);
  });
}

function populateIdentifierSelects() {
  populateSelect(document.getElementById('identifierSteel'), state.options['blade-steels']);
  populateSelect(document.getElementById('identifierFinish'), state.options['blade-finishes']);
  populateSelect(document.getElementById('identifierBladeColor'), state.options['blade-colors']);
}

function activeMasterKnives() {
  return (state.masterKnives || []).filter((k) => k.status !== 'archived');
}

/** Active models plus the current row's model if it was archived (so edits still save). */
function masterChoicesForInventoryForm(editingItem) {
  const active = activeMasterKnives();
  if (!editingItem) return active;
  const cur = state.masterKnives?.find((k) => k.id === editingItem.master_knife_id);
  if (!cur || active.some((k) => k.id === cur.id)) return active;
  return [...active, cur].sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
}

/** Build model snapshot HTML for v2 catalog model. */
function buildV2ModelSnapshotHtml(model) {
  if (!model) return '';
  const bits = [
    model.knife_type && `<strong>Type</strong> ${escapeHtml(model.knife_type)}`,
    model.family_name && `<strong>Family</strong> ${escapeHtml(model.family_name)}`,
    model.form_name && `<strong>Form</strong> ${escapeHtml(model.form_name)}`,
    model.series_name && `<strong>Series</strong> ${escapeHtml(model.series_name)}`,
    model.collaborator_name && `<strong>Collab</strong> ${escapeHtml(model.collaborator_name)}`,
    model.steel && `<strong>Steel</strong> ${escapeHtml(model.steel)}`,
    model.blade_length != null && `<strong>Blade</strong> ${model.blade_length}"`,
  ].filter(Boolean);
  return bits.length ? bits.join(' · ') : '<span class="muted">—</span>';
}

function updateMasterSpecSnapshot(form, masterId) {
  const el = form.querySelector('#masterSpecSnapshot');
  if (!el) return;
  const knife = state.masterKnives?.find((x) => x.id === masterId);
  if (!knife) {
    el.innerHTML = '';
    return;
  }
  const bits = [
    knife.category && `<strong>Category</strong> ${escapeHtml(knife.category)}`,
    knife.family && `<strong>Blade family</strong> ${escapeHtml(knife.family)}`,
    knife.primary_use_case && `<strong>Primary use</strong> ${escapeHtml(knife.primary_use_case)}`,
    knife.record_type && `<strong>Record type</strong> ${escapeHtml(knife.record_type)}`,
    knife.catalog_status && `<strong>Catalog status</strong> ${escapeHtml(knife.catalog_status)}`,
    knife.blade_profile && `<strong>Profile</strong> ${escapeHtml(knife.blade_profile)}`,
    knife.default_blade_length != null && `<strong>Catalog blade</strong> ${knife.default_blade_length}"`,
    knife.has_ring && '<strong>Ring knife</strong>',
    knife.is_tactical && '<strong>Tactical line</strong>',
    knife.is_filleting_knife && '<strong>Fillet style</strong>',
    knife.is_kitchen && '<strong>Kitchen</strong>',
    knife.is_hatchet && '<strong>Hatchet / axe</strong>',
  ].filter(Boolean);
  el.innerHTML = bits.length
    ? `<div class="muted">From master list:</div>${bits.join(' · ')}`
    : '<span class="muted">No extra catalog tags on this model.</span>';
}

async function fetchInventoryOptions(masterId, showAll) {
  if (showAll || !masterId) return { ...state.options, _filtered: false };
  const data = await api(`/api/inventory/options?master_knife_id=${masterId}`);
  return data;
}

async function refreshInventoryVariantSelects(form, masterId, showAll) {
  const opts = await fetchInventoryOptions(masterId, showAll);
  const filtered = !!opts._filtered;
  const labelEl = form.querySelector('#inventoryFilteredLabel');
  const toggleWrap = form.querySelector('.inventory-show-all-toggle');
  if (labelEl) labelEl.classList.toggle('hidden', !filtered || showAll);
  if (toggleWrap) toggleWrap.classList.toggle('hidden', !filtered);
  const optionMap = [
    ['handle-colors', 'handle_color'],
    ['blade-steels', 'blade_steel'],
    ['blade-finishes', 'blade_finish'],
    ['blade-colors', 'blade_color'],
  ];
  optionMap.forEach(([key, elName]) => {
    const currentVal = form.elements[elName]?.value;
    let list = opts[key] || [];
    if (currentVal && !list.some((o) => (o.name ?? o.id) == currentVal || o.id == currentVal)) {
      list = [...list, { id: currentVal, name: currentVal }];
    }
    populateSelect(form.elements[elName], list, true, true);
    if (currentVal) form.elements[elName].value = currentVal;
  });
}

/** Copies catalog defaults onto the inventory form (user can edit before save). */
function applyMasterDefaultsToInventoryForm(form, masterId) {
  const knife = state.masterKnives.find((x) => x.id === masterId);
  if (!knife) return;
  form.elements.handle_color.value = knife.default_handle_color || '';
  form.elements.blade_steel.value = knife.default_steel || '';
  form.elements.blade_finish.value = knife.default_blade_finish || '';
  form.elements.blade_color.value = knife.default_blade_color || '';
  form.elements.blade_length.value = knife.default_blade_length != null ? knife.default_blade_length : '';
  form.elements.is_collab.checked = !!knife.is_collab;
  form.elements.collaboration_name.value = knife.collaboration_name || '';
  const msrpVal = knife.msrp != null ? knife.msrp : '';
  form.elements.purchase_price.value = msrpVal;
  form.elements.estimated_value.value = msrpVal;
}

function openInventoryModal() {
  document.getElementById('inventoryModalBackdrop')?.classList.remove('hidden');
  document.getElementById('inventoryModalPanel')?.classList.remove('hidden');
  document.getElementById('inventoryModalBackdrop')?.setAttribute('aria-hidden', 'false');
  document.getElementById('inventoryModalPanel')?.setAttribute('aria-hidden', 'false');
}

function closeInventoryModal() {
  document.getElementById('inventoryModalBackdrop')?.classList.add('hidden');
  document.getElementById('inventoryModalPanel')?.classList.add('hidden');
  document.getElementById('inventoryModalBackdrop')?.setAttribute('aria-hidden', 'true');
  document.getElementById('inventoryModalPanel')?.setAttribute('aria-hidden', 'true');
}

async function showInventoryForm(item = null, preSelectedMasterId = null, preSelectedModel = null) {
  const host = document.getElementById('inventoryFormHost');
  if (!host) return;
  host.innerHTML = document.getElementById('inventoryFormTemplate').innerHTML;
  const form = host.querySelector('form');
  const isNew = !item;
  let selectedModel = null;

  const opts = state.options || {};
  populateSelect(form.elements.handle_color, (opts['handle-colors'] || []).map((o) => ({ id: o.name, name: o.name })), true, true);
  populateSelect(form.elements.blade_steel, (opts['blade-steels'] || []).map((o) => ({ id: o.name, name: o.name })), true, true);
  populateSelect(form.elements.blade_finish, (opts['blade-finishes'] || []).map((o) => ({ id: o.name, name: o.name })), true, true);
  populateSelect(form.elements.blade_color, (opts['blade-colors'] || []).map((o) => ({ id: o.name, name: o.name })), true, true);

  const modelSearchEl = form.querySelector('#inventoryModelSearch');
  const modelSelectedEl = form.querySelector('#inventoryModelSelected');
  const snapshotEl = form.querySelector('#masterSpecSnapshot');
  const filterLabel = form.querySelector('#inventoryFilteredLabel');
  const showAllWrap = form.querySelector('.inventory-show-all-toggle');
  if (filterLabel) filterLabel.classList.add('hidden');
  if (showAllWrap) showAllWrap.classList.add('hidden');

  const setModel = (model) => {
    if (searchDropdown) {
      searchDropdown.remove();
      searchDropdown = null;
    }
    selectedModel = model;
    form.elements.knife_model_id.value = model ? model.id : '';
    if (modelSearchEl) modelSearchEl.value = model ? model.official_name : '';
    if (modelSelectedEl) {
      modelSelectedEl.textContent = model ? model.official_name : '';
      modelSelectedEl.classList.toggle('hidden', !model);
    }
    if (snapshotEl) snapshotEl.innerHTML = model ? buildV2ModelSnapshotHtml(model) : '';
    if (model && isNew) {
      form.elements.handle_color.value = model.handle_color || '';
      form.elements.blade_steel.value = model.steel || '';
      form.elements.blade_finish.value = model.blade_finish || '';
      form.elements.blade_color.value = model.blade_color || '';
      form.elements.blade_length.value = model.blade_length != null ? model.blade_length : '';
      form.elements.collaboration_name.value = model.collaborator_name || '';
      form.elements.is_collab.checked = !!model.collaborator_name;
      if (model.msrp != null && model.msrp !== '') {
        // Default financial fields from catalog MSRP for new rows; user can override.
        form.elements.purchase_price.value = model.msrp;
        form.elements.estimated_value.value = model.msrp;
      }
    }
  };

  const resolveModelDetail = async (model) => {
    if (!model?.id) return model;
    try {
      const full = await api(`/api/v2/models/${model.id}`);
      return { ...model, ...full };
    } catch (_) {
      return model;
    }
  };

  let searchAbort = null;
  let searchDropdown = null;
  modelSearchEl?.addEventListener('input', async () => {
    const q = modelSearchEl.value.trim();
    if (searchDropdown) { searchDropdown.remove(); searchDropdown = null; }
    if (!q) { setModel(null); return; }
    if (searchAbort) searchAbort.abort();
    searchAbort = new AbortController();
    const qWhenRequested = q;
    try {
      const models = await api(`/api/v2/models/search?q=${encodeURIComponent(q)}&limit=20`, {
        signal: searchAbort.signal,
      });
      if (!modelSearchEl || modelSearchEl.value.trim() !== qWhenRequested) {
        return;
      }
      if (models.length === 1 && models[0].official_name?.toLowerCase() === q.toLowerCase()) {
        setModel(await resolveModelDetail(models[0]));
        return;
      }
      searchDropdown = document.createElement('div');
      searchDropdown.className = 'model-search-dropdown';
      searchDropdown.style.cssText = 'position:absolute;z-index:100;background:var(--bg);border:1px solid var(--border);max-height:200px;overflow:auto;';
      models.slice(0, 15).forEach((m) => {
        const div = document.createElement('div');
        div.className = 'model-search-option';
        div.textContent = m.official_name + (m.family_name ? ` (${m.family_name})` : '');
        div.style.cursor = 'pointer';
        div.style.padding = '6px 10px';
        div.addEventListener('pointerdown', (ev) => {
          ev.preventDefault();
        });
        div.addEventListener('click', async () => {
          setModel(await resolveModelDetail(m));
        });
        searchDropdown.appendChild(div);
      });
      modelSearchEl.parentElement.style.position = 'relative';
      modelSearchEl.parentElement.appendChild(searchDropdown);
    } catch (e) {
      if (e?.name === 'AbortError') return;
    }
  });
  modelSearchEl?.addEventListener('focus', () => {
    const q = modelSearchEl.value.trim();
    if (q && !selectedModel) modelSearchEl.dispatchEvent(new Event('input'));
  });

  const defaultsBtn = form.querySelector('#applyMasterDefaultsBtn');
  defaultsBtn?.addEventListener('click', () => {
    if (selectedModel) {
      form.elements.handle_color.value = selectedModel.handle_color || '';
      form.elements.blade_steel.value = selectedModel.steel || '';
      form.elements.blade_finish.value = selectedModel.blade_finish || '';
      form.elements.blade_color.value = selectedModel.blade_color || '';
      form.elements.blade_length.value = selectedModel.blade_length != null ? selectedModel.blade_length : '';
      form.elements.collaboration_name.value = selectedModel.collaborator_name || '';
      form.elements.is_collab.checked = !!selectedModel.collaborator_name;
      if (selectedModel.msrp != null && selectedModel.msrp !== '') {
        form.elements.purchase_price.value = selectedModel.msrp;
        form.elements.estimated_value.value = selectedModel.msrp;
      }
      snapshotEl.innerHTML = buildV2ModelSnapshotHtml(selectedModel);
    }
  });

  if (item) {
    const applyItemOverrides = () => {
      const purchaseVal = item.purchase_price ?? item.purchasePrice ?? item.price_paid ?? '';
      const estimatedVal = item.estimated_value ?? item.estimatedValue ?? '';
      form.elements.purchase_price.value = purchaseVal ?? '';
      form.elements.estimated_value.value = estimatedVal ?? '';
      form.elements.condition.value = item.condition ?? 'Like New';
      form.elements.handle_color.value = item.handle_color ?? '';
      form.elements.blade_steel.value = item.blade_steel ?? '';
      form.elements.blade_finish.value = item.blade_finish ?? '';
      form.elements.blade_color.value = item.blade_color ?? '';
      form.elements.collaboration_name.value = item.collaboration_name ?? '';
      form.elements.is_collab.checked = !!item.is_collab;
      form.elements.location.value = item.location ?? '';
      form.elements.purchase_source.value = item.purchase_source ?? '';
      form.elements.last_sharpened.value = item.last_sharpened ?? '';
      form.elements.notes.value = item.notes ?? '';
      form.elements.mkc_order_number.value = item.mkc_order_number ?? '';
    };
    form.elements.id.value = item.id ?? '';
    form.elements.knife_model_id.value = item.knife_model_id ?? '';
    form.elements.nickname.value = item.nickname ?? '';
    form.elements.quantity.value = item.quantity ?? 1;
    form.elements.blade_length.value = item.blade_length ?? '';
    form.elements.acquired_date.value = item.acquired_date ?? '';
    applyItemOverrides();
    setModel({
      id: item.knife_model_id,
      official_name: item.knife_name,
      knife_type: item.knife_type,
      family_name: item.knife_family,
      form_name: item.form_name,
      series_name: item.series_name,
      collaborator_name: item.collaborator_name || item.collaboration_name,
      steel: item.blade_steel,
      blade_finish: item.blade_finish,
      blade_color: item.blade_color,
      handle_color: item.handle_color,
      blade_length: item.blade_length,
    });
    if (item.knife_model_id) {
      try {
        const fullModel = await api(`/api/v2/models/${item.knife_model_id}`);
        setModel(fullModel);
        // Keep inventory-row values authoritative for this specific piece.
        applyItemOverrides();
      } catch (_) {
        // Fall back to row-derived snapshot if model detail fetch fails.
      }
    }
  } else if (preSelectedModel) {
    setModel(preSelectedModel);
  } else {
    modelSearchEl?.focus();
  }

  const duplicateBtn = form.querySelector('#duplicateInventoryBtn');
  const deleteBtn = form.querySelector('#deleteInventoryBtn');
  duplicateBtn.hidden = isNew;
  deleteBtn.hidden = isNew;

  if (!isNew && duplicateBtn) {
    duplicateBtn.addEventListener('click', async () => {
      try {
        const created = await api(`/api/v2/inventory/${item.id}/duplicate`, { method: 'POST' });
        await loadInventoryData();
        const newItem = state.inventory.find((x) => x.id === created.id);
        if (newItem) showInventoryForm(newItem);
      } catch (err) {
        alert(userFacingError(err));
      }
    });
  }
  if (!isNew && deleteBtn) {
    deleteBtn.addEventListener('click', async () => {
      if (!confirm('Delete this inventory item?')) return;
      try {
        await api(`/api/v2/inventory/${item.id}`, { method: 'DELETE' });
        closeInventoryModal();
        await loadInventoryData();
      } catch (err) {
        alert(userFacingError(err));
      }
    });
  }

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const knifeModelId = Number(form.elements.knife_model_id?.value || 0);
    if (!knifeModelId) {
      alert('Please search and select a knife model.');
      return;
    }
    const payload = {
      knife_model_id: knifeModelId,
      nickname: form.elements.nickname?.value || null,
      quantity: Number(form.elements.quantity?.value || 1),
      acquired_date: form.elements.acquired_date?.value || null,
      mkc_order_number: form.elements.mkc_order_number?.value?.trim() || null,
      purchase_price: form.elements.purchase_price?.value ? Number(form.elements.purchase_price.value) : null,
      estimated_value: form.elements.estimated_value?.value ? Number(form.elements.estimated_value.value) : null,
      condition: form.elements.condition?.value || 'Like New',
      handle_color: form.elements.handle_color?.value || null,
      steel: form.elements.blade_steel?.value || null,
      blade_finish: form.elements.blade_finish?.value || null,
      blade_color: form.elements.blade_color?.value || null,
      blade_length: form.elements.blade_length?.value ? Number(form.elements.blade_length.value) : null,
      collaboration_name: form.elements.collaboration_name?.value || null,
      location: form.elements.location?.value || null,
      purchase_source: form.elements.purchase_source?.value || null,
      last_sharpened: form.elements.last_sharpened?.value || null,
      notes: form.elements.notes?.value || null,
    };
    const id = form.elements.id.value;
    try {
      if (id) {
        await api(`/api/v2/inventory/${id}`, { method: 'PUT', body: JSON.stringify(payload) });
      } else {
        await api('/api/v2/inventory', { method: 'POST', body: JSON.stringify(payload) });
      }
      closeInventoryModal();
      await loadInventoryData();
    } catch (err) {
      alert(userFacingError(err));
    }
  });

  form.querySelector('.cancelFormBtn').addEventListener('click', closeInventoryModal);
  openInventoryModal();
  document.getElementById('inventoryModalTitle').textContent = isNew ? 'Add knife' : 'Edit knife';
}

function closeMasterPanel() {
  if (masterPanelObjectUrl) {
    URL.revokeObjectURL(masterPanelObjectUrl);
    masterPanelObjectUrl = null;
  }
  document.getElementById('masterDetailBackdrop')?.classList.add('hidden');
  document.getElementById('masterDetailPanel')?.classList.add('hidden');
}

function buildMasterSummaryHtml(item) {
  const blocks = [];
  const push = (title, text) => {
    const t = (text || '').trim();
    if (!t) return;
    blocks.push(
      `<section class="master-summary-block"><h4>${escapeHtml(title)}</h4><div class="master-summary-body">${escapeHtml(t)}</div></section>`,
    );
  };
  push('Notes', item.notes);
  const rows = [
    ['Type', item.category],
    ['Family', item.family],
    ['Form', item.form_name],
    ['Series', item.series_name],
    ['Collaborator', item.collaboration_name],
    ['Generation', item.generation_label],
    ['Size modifier', item.size_modifier],
  ].filter(([, v]) => (v || '').toString().trim());
  const dl = rows.length
    ? `<dl class="master-summary-dl">${rows.map(([k, v]) => `<dt>${escapeHtml(k)}</dt><dd>${escapeHtml(String(v))}</dd>`).join('')}</dl>`
    : '';
  if (!blocks.length && !dl) {
    return '<p class="muted">No summary fields yet.</p>';
  }
  return blocks.join('') + dl;
}

function bindMasterReferenceBlock(form, item) {
  const img = form.querySelector('#masterRefPreview');
  const placeholder = form.querySelector('#masterRefPlaceholder');
  const note = form.querySelector('#masterRefSilhouetteNote');
  const fileInput = form.querySelector('[name="identifier_image_file"]');
  const removeBtn = form.querySelector('#removeMasterRefImageBtn');
  if (!img || !placeholder || !fileInput || !removeBtn) return;

  const showStored = () => {
    if (masterPanelObjectUrl) {
      URL.revokeObjectURL(masterPanelObjectUrl);
      masterPanelObjectUrl = null;
    }
    fileInput.value = '';
    if (item?.has_identifier_image && item?.id) {
      img.src = `/api/v2/models/${item.v2_id || item.id}/image?t=${Date.now()}`;
      img.classList.remove('hidden');
      placeholder.classList.add('hidden');
      removeBtn.hidden = false;
    } else {
      img.removeAttribute('src');
      img.classList.add('hidden');
      placeholder.classList.remove('hidden');
      removeBtn.hidden = true;
    }
    if (item?.has_silhouette_hint) {
      note.textContent = 'Silhouette (Hu) vector is stored for shape matching.';
      note.classList.remove('hidden');
    } else if (item?.has_identifier_image) {
      note.textContent = 'No silhouette vector yet — try a clearer side-profile shot on a plain background.';
      note.classList.remove('hidden');
    } else {
      note.textContent = '';
      note.classList.add('hidden');
    }
  };

  showStored();

  fileInput.addEventListener('change', () => {
    const f = fileInput.files[0];
    if (masterPanelObjectUrl) {
      URL.revokeObjectURL(masterPanelObjectUrl);
      masterPanelObjectUrl = null;
    }
    if (!f) {
      showStored();
      return;
    }
    masterPanelObjectUrl = URL.createObjectURL(f);
    img.src = masterPanelObjectUrl;
    img.classList.remove('hidden');
    placeholder.classList.add('hidden');
    removeBtn.hidden = !item?.id;
    note.textContent = 'Save the form to upload and compute the silhouette.';
    note.classList.remove('hidden');
  });
}

async function postMasterReferenceUpload(knifeId, file) {
  const fd = new FormData();
  fd.append('file', file);
  const visionModel = localStorage.getItem(OLLAMA_DEFAULT_MODEL_KEY);
  if (visionModel) fd.set('model', visionModel);
  const res = await fetch(`/api/v2/models/${knifeId}/image`, { method: 'POST', body: fd });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(formatApiErrorDetail(data.detail, 'Upload failed'));
  return data;
}

async function addInlineOption(form, optionType, selectName) {
  const labelMap = {
    collaborators: 'collaborator',
    'size-modifiers': 'size modifier',
  };
  const label = labelMap[optionType] || 'option';
  const entered = prompt(`Add new ${label}:`);
  const value = (entered || '').trim();
  if (!value) return;
  try {
    await api(`/api/v2/options/${optionType}`, {
      method: 'POST',
      body: JSON.stringify({ name: value }),
    });
  } catch (err) {
    alert(userFacingError(err));
    return;
  }
  const fresh = await api('/api/v2/options');
  state.options = { ...state.options, ...fresh };
  const target = form.elements[selectName];
  if (target) {
    populateSelect(target, state.options[optionType] || [], true, true);
    target.value = value;
  }
}

async function openMasterPanel(item = null) {
  closeMasterPanel();
  const backdrop = document.getElementById('masterDetailBackdrop');
  const panel = document.getElementById('masterDetailPanel');
  const host = document.getElementById('masterDetailFormHost');
  const summaryEl = document.getElementById('masterDetailSummary');
  const title = document.getElementById('masterDetailTitle');
  if (!backdrop || !panel || !host || !summaryEl || !title) return;

  let resolved = item;
  if (item?.id) {
    try {
      resolved = await api(`/api/v2/models/${item.v2_id || item.id}`);
      resolved = mapV2ModelToMasterRow(resolved);
    } catch (_) {
      /* use list row if single-knife fetch fails */
    }
  }

  backdrop.classList.remove('hidden');
  panel.classList.remove('hidden');
  title.textContent = resolved ? resolved.name : 'Add model';

  host.innerHTML = document.getElementById('masterFormTemplate').innerHTML;
  const form = host.querySelector('#masterForm');
  if (!form) return;

  if (resolved) {
    summaryEl.innerHTML = buildMasterSummaryHtml(resolved);
    summaryEl.classList.remove('hidden');
  } else {
    summaryEl.innerHTML = '';
    summaryEl.classList.add('hidden');
  }

  populateSelect(form.elements.default_steel, state.options['blade-steels'], true, true);
  populateSelect(form.elements.default_blade_finish, state.options['blade-finishes'], true, true);
  populateSelect(form.elements.default_blade_color, state.options['blade-colors'], true, true);
  populateSelect(form.elements.default_handle_color, state.options['handle-colors'], true, true);
  populateSelect(form.elements.handle_type, state.options['handle-types'] || [], true, true);
  const identityFilters = state.v2CatalogFilters || {};
  const withResolved = (vals, key) => {
    const base = [...(vals || [])];
    const rv = (resolved?.[key] || '').trim();
    if (rv && !base.includes(rv)) base.push(rv);
    return base;
  };
  populateSelect(
    form.elements.knife_type,
    withResolved(identityFilters.type, 'knife_type').map((x) => ({ id: x, name: x })),
    true,
    true,
  );
  populateSelect(
    form.elements.form_name,
    withResolved(identityFilters.form, 'form_name').map((x) => ({ id: x, name: x })),
    true,
    true,
  );
  populateSelect(
    form.elements.family_name,
    withResolved(identityFilters.family, 'family_name').map((x) => ({ id: x, name: x })),
    true,
    true,
  );
  populateSelect(
    form.elements.series_name,
    withResolved(identityFilters.series, 'series_name').map((x) => ({ id: x, name: x })),
    true,
    true,
  );
  populateSelect(
    form.elements.collaborator_name,
    withResolved(
      Array.from(
        new Set([
          ...(identityFilters.collaboration || []),
          ...((state.options.collaborators || []).map((o) => o.name || o.id).filter(Boolean)),
        ]),
      ),
      'collaborator_name',
    ).map((x) => ({ id: x, name: x })),
    true,
    true,
  );
  populateSelect(
    form.elements.generation_label,
    withResolved(
      (state.options['generations'] || []).map((o) => o.name || o.id).filter(Boolean),
      'generation_label',
    ).map((x) => ({ id: x, name: x })),
    true,
    true,
  );
  populateSelect(
    form.elements.size_modifier,
    withResolved(
      (state.options['size-modifiers'] || []).map((o) => o.name || o.id).filter(Boolean),
      'size_modifier',
    ).map((x) => ({ id: x, name: x })),
    true,
    true,
  );

  if (resolved) {
    for (const [key, value] of Object.entries(resolved)) {
      if (!form.elements[key]) continue;
      if (form.elements[key].type === 'checkbox') form.elements[key].checked = !!value;
      else form.elements[key].value = value ?? '';
    }
    form.elements.id.value = resolved.id;
    const optionFields = [
      ['default_steel', 'blade-steels'],
      ['default_blade_finish', 'blade-finishes'],
      ['default_blade_color', 'blade-colors'],
      ['default_handle_color', 'handle-colors'],
      ['handle_type', 'handle-types'],
    ];
    optionFields.forEach(([field, type]) => {
      const el = form.elements[field];
      if (el) el.value = resolveOptionLabel(type, resolved[field]) || resolved[field] || '';
    });
  }

  const identityNameEl = form.elements.official_name || form.elements.name;
  identityNameEl?.addEventListener('input', () => {
    const slugEl = form.elements.canonical_slug;
    const nameVal = (identityNameEl.value || '').trim();
    if (!resolved?.id && slugEl && !slugEl.value) {
      slugEl.value = nameVal
        .toLowerCase()
        .replace(/\s+/g, '-')
        .replace(/[^a-z0-9-]/g, '');
    }
    if (!resolved?.id && nameVal) {
      deriveAndSuggestFamily(form, nameVal);
    }
  });

  if (!resolved?.id) {
    const nameVal = (form.elements.official_name?.value || '').trim();
    if (nameVal) deriveAndSuggestFamily(form, nameVal);
  }

  const deleteModelBtn = form.querySelector('#deleteMasterModelBtn');
  if (deleteModelBtn) deleteModelBtn.hidden = !resolved?.id;

  bindMasterReferenceBlock(form, resolved);
  form.querySelectorAll('.add-inline-option-btn').forEach((btn) => {
    btn.addEventListener('click', async () => {
      await addInlineOption(form, btn.dataset.optionType, btn.dataset.selectName);
    });
  });

  if (deleteModelBtn) {
    deleteModelBtn.addEventListener('click', async () => {
      if (!resolved?.id || !confirm(`Delete master model “${resolved.name}” permanently?`)) return;
      try {
        await api(`/api/v2/models/${resolved.v2_id || resolved.id}`, { method: 'DELETE' });
        closeMasterPanel();
        await loadMasterData();
      } catch (err) {
        alert(userFacingError(err));
      }
    });
  }

  const removeRefBtn = form.querySelector('#removeMasterRefImageBtn');
  if (removeRefBtn && resolved?.id) {
    removeRefBtn.addEventListener('click', async () => {
      if (!confirm('Remove stored reference image and silhouette data from the database?')) return;
      try {
        await api(`/api/v2/models/${resolved.v2_id || resolved.id}/image`, { method: 'DELETE' });
        await loadMasterData();
        const fresh = state.masterKnives.find((x) => x.id === resolved.id);
        void openMasterPanel(fresh || null);
      } catch (err) {
        alert(userFacingError(err));
      }
    });
  }

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const raw = formToJson(form);
    const officialName = (raw.official_name || '').trim();
    const knifeType = (raw.knife_type || '').trim();
    const formName = (raw.form_name || '').trim();
    const familyName = (raw.family_name || '').trim();
    if (!officialName || !knifeType || !formName || !familyName) {
      alert('Identity fields are required: Official name, Type, Form, and Family.');
      return;
    }
    const payload = {
      official_name: officialName,
      normalized_name: (raw.normalized_name || officialName).trim(),
      canonical_slug: (raw.canonical_slug || '').trim() || null,
      knife_type: knifeType,
      form_name: formName,
      family_name: familyName,
      series_name: (raw.series_name || '').trim() || null,
      collaborator_name: (raw.collaborator_name || '').trim() || null,
      generation_label: (raw.generation_label || '').trim() || null,
      steel: raw.default_steel,
      blade_finish: raw.default_blade_finish,
      blade_color: raw.default_blade_color,
      handle_color: raw.default_handle_color,
      handle_type: raw.handle_type,
      blade_length: raw.default_blade_length,
      record_status: raw.status || raw.record_status || 'active',
      msrp: raw.msrp,
      official_product_url: null,
      official_image_url: null,
      notes: raw.notes,
      size_modifier: (raw.size_modifier || '').trim() || null,
    };
    const idRaw = form.elements.id.value;
    const fileInput = form.querySelector('[name="identifier_image_file"]');
    const file = fileInput?.files?.[0];
    try {
      let knifeId = idRaw ? Number(idRaw) : null;
      if (knifeId) {
        await api(`/api/v2/models/${knifeId}`, { method: 'PUT', body: JSON.stringify(payload) });
      } else {
        const created = await api('/api/v2/models', { method: 'POST', body: JSON.stringify(payload) });
        knifeId = created.id;
        form.elements.id.value = String(knifeId);
      }
      if (file && knifeId) {
        const up = await postMasterReferenceUpload(knifeId, file);
        const msgs = [];
        if (up.silhouette_error) msgs.push(`Silhouette: ${up.silhouette_error}`);
        if (up.distinguishing_features_error) msgs.push(`Vision: ${up.distinguishing_features_error}`);
        if (msgs.length) alert(`Image saved. ${msgs.join(' ')}`);
        fileInput.value = '';
        if (masterPanelObjectUrl) {
          URL.revokeObjectURL(masterPanelObjectUrl);
          masterPanelObjectUrl = null;
        }
      }
      await loadMasterData();
      const fresh = state.masterKnives.find((x) => x.id === knifeId);
      if (fresh) {
        await openMasterPanel(fresh);
      } else {
        closeMasterPanel();
      }
    } catch (err) {
      alert(userFacingError(err));
    }
  });

  form.querySelector('.cancelFormBtn')?.addEventListener('click', closeMasterPanel);

}

function formToJson(form) {
  const fd = new FormData(form);
  const obj = {};
  const arrayKeys = [];
  for (const [k, v] of fd.entries()) {
    if (arrayKeys.includes(k)) {
      if (!obj[k]) obj[k] = [];
      obj[k].push(Number(v));
    } else {
      obj[k] = v;
    }
  }

  ['purchase_price', 'estimated_value', 'default_blade_length', 'blade_length', 'msrp'].forEach((key) => {
    if (!(key in obj)) return;
    if (obj[key] === '') obj[key] = null;
    else if (obj[key] != null) obj[key] = Number(obj[key]);
  });

  ['quantity', 'master_knife_id', 'parent_model_id'].forEach((key) => {
    if (!(key in obj)) return;
    if (obj[key] === '' || obj[key] == null) {
      if (key === 'parent_model_id') obj[key] = null;
      return;
    }
    obj[key] = Number(obj[key]);
  });

  [
    'is_collab', 'has_ring', 'is_filleting_knife', 'is_hatchet', 'is_kitchen', 'is_tactical',
    'is_current_catalog', 'is_discontinued',
  ].forEach((key) => {
    if (form.elements[key]?.type === 'checkbox') obj[key] = form.elements[key].checked;
  });

  [
    'is_collab', 'has_ring', 'is_filleting_knife', 'is_hatchet', 'is_kitchen', 'is_tactical',
  ].forEach((key) => {
    if (form.elements[key]?.tagName === 'SELECT') {
      if (obj[key] === '') obj[key] = null;
      else obj[key] = obj[key] === 'true';
    }
  });

  Object.keys(obj).forEach((key) => {
    if (obj[key] === '' && !arrayKeys.includes(key)) obj[key] = null;
  });

  arrayKeys.forEach((key) => {
    if (!obj[key] || !obj[key].length) obj[key] = null;
  });

  if (form.elements.include_archived?.type === 'checkbox') {
    obj.include_archived = form.elements.include_archived.checked;
  }

  delete obj.identifier_image_file;

  delete obj.id;
  return obj;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function safeHttpUrl(raw) {
  const u = String(raw ?? '').trim();
  if (!/^https?:\/\//i.test(u)) return '';
  return u;
}

/** DB-stored reference bytes only (no external URL fallback). */
function masterIdentifierThumbSrc(item) {
  if (item.has_identifier_image) {
    return `/api/v2/models/${item.v2_id || item.id}/image`;
  }
  return null;
}

function renderIdentifierResults(results) {
  const wrap = document.getElementById('identifierResults');
  if (!wrap) return;
  if (!results.length) {
    wrap.className = 'identifier-results empty-state';
    wrap.innerHTML = 'No catalog matches with a positive score. Try more keywords from the knife or packaging, widen &quot;use / category&quot;, or add the model on the Master list page if it is missing.';
    return;
  }

  wrap.className = 'identifier-results';
  wrap.innerHTML = `
    <p class="muted identify-source-hint">Results are ranked from the <strong>v2 catalog</strong> only — not from your saved inventory.</p>
    ${results.map((item, index) => {
    const idProd = safeHttpUrl(item.identifier_product_url);
    const idImg = item.has_identifier_image
      ? `/api/v2/models/${item.id}/image`
      : null;
    const catLine = [item.catalog_line, item.category, item.record_type].filter(Boolean);
    const thumbBlock = idImg
      ? `<div class="identifier-result-thumb-wrap"><img class="identifier-result-thumb" src=${JSON.stringify(idImg)} alt="" width="80" height="80" loading="lazy" /></div>`
      : '';
    return `
    <article class="candidate-card ${index === 0 ? 'top-hit' : ''}">
      <div class="candidate-head">
        ${thumbBlock}
        <div class="candidate-head-text">
          <h3>${escapeHtml(item.name)}${item.list_status === 'archived' ? ' <span class="archived-badge">archived in app</span>' : ''}</h3>
          <div class="muted">${escapeHtml(item.family || '')}</div>
          ${catLine.length ? `<div class="muted small">${catLine.map((x) => escapeHtml(x)).join(' · ')}</div>` : ''}
        </div>
        <div class="score-pill">${Math.round(item.score)}</div>
      </div>
      ${item.catalog_blurb ? `<p class="catalog-blurb">${escapeHtml(item.catalog_blurb)}</p>` : ''}
      <div class="candidate-meta">
        <span>${escapeHtml(item.default_blade_length == null ? '' : `${item.default_blade_length}"`)}</span>
        <span>${escapeHtml(item.default_steel || '')}</span>
        <span>${escapeHtml(item.default_blade_finish || '')}</span>
        <span>${escapeHtml(item.default_blade_color || '')}</span>
        <span>${item.is_collab ? escapeHtml(item.collaboration_name || 'Collab') : 'Non-collab'}</span>
      </div>
      <div class="muted small">Why it matched: ${escapeHtml((item.reasons || []).join(' • '))}</div>
      <div><a href="/?add=${item.id}" class="button-link add-to-inventory-btn">Add to inventory</a></div>
      ${idProd ? `<div class="candidate-links"><a class="candidate-link" href=${JSON.stringify(idProd)} target="_blank" rel="noopener noreferrer">Storefront (identifier)</a></div>` : ''}
    </article>
  `;
  }).join('')}`;
}

function optionApiBase(optionType) {
  return '/api/v2/options';
}

document.addEventListener('click', async (e) => {
  const btn = e.target.closest('button');
  if (!btn) return;

  const action = btn.dataset.action;
  const id = btn.dataset.id;

  try {
    if (action === 'duplicate-master') {
      const res = await api(`/api/v2/models/${id}/duplicate`, { method: 'POST' });
      await loadMasterData();
      let dup = state.masterKnives.find((x) => x.id === res.id);
      if (!dup) {
        dup = await api(`/api/v2/models/${res.id}`);
      }
      if (dup) void openMasterPanel(dup);
    } else if (action === 'delete-master') {
      if (!confirm('Delete this master knife?')) return;
      await api(`/api/v2/models/${id}`, { method: 'DELETE' });
      await loadMasterData();
    } else if (action === 'delete-option') {
      if (!confirm('Delete this option?')) return;
      const base = optionApiBase(btn.dataset.type);
      await api(`${base}/${btn.dataset.type}/${id}`, { method: 'DELETE' });
      await loadMasterData();
    }
  } catch (err) {
    alert(userFacingError(err));
  }
});

function initInventoryPage() {
  document.getElementById('refreshBtn')?.addEventListener('click', loadInventoryData);
  document.getElementById('showInventoryFormBtn')?.addEventListener('click', () => showInventoryForm());
  document.getElementById('inventorySearch')?.addEventListener('input', () => renderInventoryTable());
  ['inventoryFilterType', 'inventoryFilterFamily', 'inventoryFilterForm', 'inventoryFilterSeries',
   'inventoryFilterSteel', 'inventoryFilterFinish', 'inventoryFilterHandleColor', 'inventoryFilterCondition'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', () => renderInventoryTable());
  });
  document.getElementById('inventoryFilterLocation')?.addEventListener('input', () => renderInventoryTable());

  document.getElementById('inventoryModalBackdrop')?.addEventListener('click', closeInventoryModal);
  document.getElementById('inventoryModalCloseBtn')?.addEventListener('click', closeInventoryModal);

  document.getElementById('inventoryTable')?.addEventListener('click', (e) => {
    const th = e.target.closest('th[data-sort-column-id]');
    if (th) {
      const colId = th.dataset.sortColumnId;
      const col = INVENTORY_COLUMNS.find((c) => c.id === colId);
      if (col?.sortValue) {
        state.inventorySort.dir = state.inventorySort.columnId === colId && state.inventorySort.dir === 'asc' ? 'desc' : 'asc';
        state.inventorySort.columnId = colId;
        renderInventoryTable();
      }
      return;
    }
    const row = e.target.closest('tr[data-inventory-id]');
    if (!row || e.target.closest('button')) return;
    const item = state.inventory.find((x) => x.id === Number(row.dataset.inventoryId));
    if (item) showInventoryForm(item);
  });
  document.getElementById('inventoryTable')?.addEventListener('keydown', (e) => {
    const row = e.target.closest('tr[data-inventory-id]');
    if (!row || (e.key !== 'Enter' && e.key !== ' ')) return;
    e.preventDefault();
    const item = state.inventory.find((x) => x.id === Number(row.dataset.inventoryId));
    if (item) showInventoryForm(item);
  });

  document.getElementById('columnPickerBtn')?.addEventListener('click', (e) => {
    e.stopPropagation();
    const dd = document.getElementById('columnPickerDropdown');
    dd?.classList.toggle('hidden');
  });
  document.addEventListener('click', () => {
    document.getElementById('columnPickerDropdown')?.classList.add('hidden');
  });
  document.getElementById('columnPickerDropdown')?.addEventListener('click', (e) => e.stopPropagation());

  initColumnPicker();

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    const panel = document.getElementById('inventoryModalPanel');
    if (panel && !panel.classList.contains('hidden')) closeInventoryModal();
  });

  loadInventoryData().then(async () => {
    const params = new URLSearchParams(window.location.search);
    const addParam = params.get('add');
    if (addParam) {
      const id = Number(addParam);
      try {
        const model = await api(`/api/v2/models/${id}`);
        if (model) showInventoryForm(null, null, model);
      } catch (_) {}
      window.history.replaceState({}, '', '/');
    }
  }).catch((err) => {
    console.error(err);
    alert(`Failed to load app data: ${userFacingError(err)}`);
  });
}

function initIdentifyPage() {
  document.getElementById('identifierForm')?.addEventListener('submit', async (e) => {
    e.preventDefault();
    const payload = formToJson(e.target);
    try {
      const result = await api('/api/v2/identify', { method: 'POST', body: JSON.stringify(payload) });
      renderIdentifierResults(result.results || []);
    } catch (err) {
      alert(userFacingError(err));
    }
  });

  document.getElementById('clearIdentifierBtn')?.addEventListener('click', () => {
    document.getElementById('identifierForm').reset();
    renderIdentifierResults([]);
  });
}

function initMasterPage() {
  document.getElementById('refreshBtn')?.addEventListener('click', loadMasterData);
  document.getElementById('showMasterFormBtn')?.addEventListener('click', () => openMasterPanel());

  document.getElementById('masterSearch')?.addEventListener('input', () => renderMasterTable());
  ['masterFilterCategory', 'masterFilterFamily', 'masterFilterSteel', 'masterFilterFinish', 'masterFilterRecordType', 'masterFilterStatus', 'masterFilterCatalogLine'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', () => renderMasterTable());
  });

  document.getElementById('masterColumnPickerBtn')?.addEventListener('click', (e) => {
    e.stopPropagation();
    document.getElementById('masterColumnPickerDropdown')?.classList.toggle('hidden');
  });
  document.addEventListener('click', () => {
    document.getElementById('masterColumnPickerDropdown')?.classList.add('hidden');
  });
  document.getElementById('masterColumnPickerDropdown')?.addEventListener('click', (e) => e.stopPropagation());

  initMasterColumnPicker();

  document.getElementById('masterTable')?.addEventListener('click', (e) => {
    const th = e.target.closest('th[data-sort-column-id]');
    if (th) {
      const colId = th.dataset.sortColumnId;
      const col = MASTER_COLUMNS.find((c) => c.id === colId);
      if (col?.sortValue) {
        state.masterSort.dir = state.masterSort.columnId === colId && state.masterSort.dir === 'asc' ? 'desc' : 'asc';
        state.masterSort.columnId = colId;
        renderMasterTable();
      }
      return;
    }
    const tr = e.target.closest('tbody tr[data-master-id]');
    if (!tr) return;
    if (e.target.closest('[data-stop-row-open]') || e.target.closest('button')) return;
    const mid = Number(tr.dataset.masterId);
    const row = state.masterKnives.find((x) => x.id === mid);
    if (row) void openMasterPanel(row);
  });

  document.getElementById('masterTable')?.addEventListener('keydown', (e) => {
    const tr = e.target.closest('tbody tr[data-master-id]');
    if (!tr) return;
    if (e.key !== 'Enter' && e.key !== ' ') return;
    e.preventDefault();
    const mid = Number(tr.dataset.masterId);
    const row = state.masterKnives.find((x) => x.id === mid);
    if (row) void openMasterPanel(row);
  });

  document.getElementById('masterDetailBackdrop')?.addEventListener('click', closeMasterPanel);
  document.getElementById('masterDetailCloseBtn')?.addEventListener('click', closeMasterPanel);

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    const panel = document.getElementById('masterDetailPanel');
    if (panel && !panel.classList.contains('hidden')) closeMasterPanel();
  });

  document.querySelectorAll('.addOptionBtn').forEach((button) => {
    button.addEventListener('click', async () => {
      const type = button.dataset.optionType;
      const inputMap = {
        'handle-colors': 'newHandleColor',
        'blade-steels': 'newBladeSteel',
        'blade-types': 'newBladeType',
        'blade-finishes': 'newBladeFinish',
        'blade-colors': 'newBladeColor',
        'handle-types': 'newHandleType',
        'categories': 'newCategory',
        'blade-families': 'newBladeFamily',
        collaborators: 'newCollaborator',
        generations: 'newGeneration',
        'size-modifiers': 'newSizeModifier',
      };
      const input = document.getElementById(inputMap[type]);
      if (!input) return;
      const name = input.value.trim();
      if (!name) return;
      try {
        const base = optionApiBase(type);
        await api(`${base}/${type}`, { method: 'POST', body: JSON.stringify({ name }) });
        input.value = '';
        await loadMasterData();
      } catch (err) {
        alert(userFacingError(err));
      }
    });
  });

  document.getElementById('csvImportForm')?.addEventListener('submit', async (e) => {
    e.preventDefault();
    const form = e.target;
    const fd = new FormData(form);
    const msg = document.getElementById('csvImportMessage');
    try {
      const response = await fetch('/api/v2/import/models.csv', {
        method: 'POST',
        body: fd,
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(formatApiErrorDetail(data.detail, 'Import failed'));
      }
      msg.classList.remove('hidden');
      msg.textContent = `Imported: ${data.inserted} new, ${data.updated} updated.`;
      form.reset();
      await loadMasterData();
    } catch (err) {
      msg.classList.remove('hidden');
      msg.textContent = userFacingError(err);
    }
  });

  const huReportEl = document.getElementById('huStatusReport');
  const checkHuBtn = document.getElementById('checkHuStatusBtn');
  const recomputeHuBtn = document.getElementById('recomputeHuBtn');
  if (checkHuBtn) {
    checkHuBtn.addEventListener('click', async () => {
      try {
        const data = await api('/api/v2/admin/silhouettes/status');
        const html = [
          `<p><strong>Summary:</strong> ${data.total} masters, ${data.with_image} with image, ${data.with_valid_hu} with valid Hu.</p>`,
          data.missing_hu?.length ? `<p class="warning"><strong>Missing/degenerate Hu (${data.missing_hu.length}):</strong> ${data.missing_hu.map(m => m.name).join(', ')}</p>` : '<p>All masters with images have valid Hu.</p>',
          '<details><summary>View Hu JSON by master</summary>',
          '<table class="small"><thead><tr><th>Name</th><th>Image</th><th>Hu</th><th>Value</th></tr></thead><tbody>',
          ...(data.masters || []).map(m => [
            '<tr>',
            `<td>${escapeHtml(m.name)}</td>`,
            `<td>${m.has_image ? '✓' : '—'}</td>`,
            `<td>${m.has_hu ? '✓' : (m.hu_degenerate ? 'degenerate' : '—')}</td>`,
            `<td><code class="hu-json-cell" title="${m.hu_json ? escapeHtml(m.hu_json) : ''}">${m.hu_json ? escapeHtml(m.hu_json) : '—'}</code></td>`,
            '</tr>'
          ].join('')),
          '</tbody></table></details>'
        ].join('');
        huReportEl.innerHTML = html;
        huReportEl.classList.remove('hidden');
      } catch (err) {
        huReportEl.innerHTML = `<p class="error">${escapeHtml(userFacingError(err))}</p>`;
        huReportEl.classList.remove('hidden');
      }
    });
  }
  if (recomputeHuBtn) {
    recomputeHuBtn.addEventListener('click', async () => {
      if (!confirm('Recompute Hu from stored images for masters with missing or degenerate Hu?')) return;
      try {
        const data = await api('/api/v2/admin/silhouettes/recompute', { method: 'POST' });
        recomputeHuBtn.textContent = `Recompute missing Hu (${data.updated} updated)`;
        if (data.updated > 0) await loadMasterData();
        if (checkHuBtn) checkHuBtn.click();
      } catch (err) {
        alert(userFacingError(err));
      }
    });
  }

  const distReportEl = document.getElementById('distStatusReport');
  const distIndicator = document.getElementById('distRecomputeIndicator');
  const checkDistBtn = document.getElementById('checkDistStatusBtn');
  const recomputeDistMissingBtn = document.getElementById('recomputeDistMissingBtn');
  const recomputeDistAllBtn = document.getElementById('recomputeDistAllBtn');
  const recomputeDistSelectedBtn = document.getElementById('recomputeDistSelectedBtn');

  const distRecomputeButtons = [recomputeDistMissingBtn, recomputeDistAllBtn, recomputeDistSelectedBtn].filter(Boolean);

  function setDistRecomputeRunning(running) {
    if (distIndicator) {
      distIndicator.textContent = running ? 'Recomputing… (1–2 min per image; tail data/mkc_app.log)' : '';
      distIndicator.classList.toggle('hidden', !running);
    }
    distRecomputeButtons.forEach((btn) => { if (btn) btn.disabled = running; });
  }

  function getSelectedMasterIds() {
    return Array.from(document.querySelectorAll('.master-row-select:checked')).map((cb) => Number(cb.dataset.masterId));
  }

  function updateRecomputeSelectedVisibility() {
    const ids = getSelectedMasterIds();
    if (recomputeDistSelectedBtn) {
      if (ids.length > 0) {
        recomputeDistSelectedBtn.classList.remove('hidden');
        recomputeDistSelectedBtn.textContent = `Recompute selected (${ids.length})`;
      } else {
        recomputeDistSelectedBtn.classList.add('hidden');
      }
    }
  }

  document.getElementById('masterTable')?.addEventListener('change', (e) => {
    if (e.target.id === 'masterSelectAll') {
      document.querySelectorAll('.master-row-select').forEach((cb) => { cb.checked = e.target.checked; });
      updateRecomputeSelectedVisibility();
    } else if (e.target.classList.contains('master-row-select')) {
      updateRecomputeSelectedVisibility();
    }
  });

  if (checkDistBtn) {
    checkDistBtn.addEventListener('click', async () => {
      try {
        const data = await api('/api/v2/admin/distinguishing-features/status');
        const html = [
          `<p><strong>Summary:</strong> ${data.total} masters, ${data.with_image} with image, ${data.with_features} with distinguishing features.</p>`,
          data.missing?.length ? `<p class="warning"><strong>Missing (${data.missing.length}):</strong> ${data.missing.map((m) => m.name).join(', ')}</p>` : '<p>All masters with images have distinguishing features.</p>',
          '<details><summary>View by master</summary>',
          '<table class="small"><thead><tr><th>Name</th><th>Image</th><th>Features</th><th>Value</th></tr></thead><tbody>',
          ...(data.masters || []).map((m) => [
            '<tr>',
            `<td>${escapeHtml(m.name)}</td>`,
            `<td>${m.has_image ? '✓' : '—'}</td>`,
            `<td>${m.has_features ? '✓' : '—'}</td>`,
            `<td>${m.features ? escapeHtml(m.features) : '—'}</td>`,
            '</tr>'
          ].join('')),
          '</tbody></table></details>'
        ].join('');
        distReportEl.innerHTML = html;
        distReportEl.classList.remove('hidden');
      } catch (err) {
        distReportEl.innerHTML = `<p class="error">${escapeHtml(userFacingError(err))}</p>`;
        distReportEl.classList.remove('hidden');
      }
    });
  }

  if (recomputeDistMissingBtn) {
    recomputeDistMissingBtn.addEventListener('click', async () => {
      if (!(await checkOllamaBeforeRecompute())) return;
      if (!confirm('Recompute distinguishing features for masters with image but none set? (Uses vision LLM; ~1–2 min per image)')) return;
      try {
        setDistRecomputeRunning(true);
        const data = await runDistRecompute({ missing_only: true });
        recomputeDistMissingBtn.textContent = `Recompute missing (${data.updated} updated)`;
        if (data.updated > 0) await loadMasterData();
        if (checkDistBtn) checkDistBtn.click();
      } catch (err) {
        alert(userFacingError(err));
      } finally {
        setDistRecomputeRunning(false);
      }
    });
  }

  if (recomputeDistAllBtn) {
    recomputeDistAllBtn.addEventListener('click', async () => {
      if (!(await checkOllamaBeforeRecompute())) return;
      if (!confirm('Recompute distinguishing features for ALL masters with images? (Overwrites existing; uses vision LLM; ~1–2 min per image)')) return;
      try {
        setDistRecomputeRunning(true);
        const data = await runDistRecompute({});
        recomputeDistAllBtn.textContent = `Recompute all (${data.updated} updated)`;
        if (data.updated > 0) await loadMasterData();
        if (checkDistBtn) checkDistBtn.click();
      } catch (err) {
        alert(userFacingError(err));
      } finally {
        setDistRecomputeRunning(false);
      }
    });
  }

  if (recomputeDistSelectedBtn) {
    recomputeDistSelectedBtn.addEventListener('click', async () => {
      const ids = getSelectedMasterIds();
      if (!ids.length) return;
      if (!(await checkOllamaBeforeRecompute())) return;
      if (!confirm(`Recompute distinguishing features for ${ids.length} selected master(s)? (~1–2 min per image)`)) return;
      try {
        setDistRecomputeRunning(true);
        const data = await runDistRecompute({ knife_ids: ids });
        recomputeDistSelectedBtn.textContent = `Recompute selected (${data.updated} updated)`;
        recomputeDistSelectedBtn.classList.add('hidden');
        document.querySelectorAll('.master-row-select:checked').forEach((cb) => { cb.checked = false; });
        document.getElementById('masterSelectAll') && (document.getElementById('masterSelectAll').checked = false);
        if (data.updated > 0) await loadMasterData();
        if (checkDistBtn) checkDistBtn.click();
      } catch (err) {
        alert(userFacingError(err));
      } finally {
        setDistRecomputeRunning(false);
      }
    });
  }

  loadMasterData().then(() => {
    const params = new URLSearchParams(window.location.search);
    const search = params.get('search');
    if (search) {
      const el = document.getElementById('masterSearch');
      if (el) { el.value = search; renderMasterTable(); }
    }
  }).catch((err) => {
    console.error(err);
    alert(`Failed to load master data: ${userFacingError(err)}`);
  });
}

if (PAGE === 'inventory') {
  initInventoryPage();
} else if (PAGE === 'identify') {
  initIdentifyPage();
} else if (PAGE === 'master') {
  initMasterPage();
}

// Version footer
fetch('/api/version').then(r => r.json()).then(d => {
  const el = document.getElementById('app-version-footer');
  if (el && d.commit) el.textContent = d.commit + (d.committed_at ? ' · ' + d.committed_at : '');
}).catch(() => {});
