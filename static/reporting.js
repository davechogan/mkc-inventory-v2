const REPORTING_MODEL_KEY = 'mkc_reporting_default_model';

const state = {
  sessionId: null,
  sessions: [],
  messages: [],
  lastResult: null,
  savedQueries: [],
  templates: [],
  /** Client-side grid: sort when a column header is clicked (filter text lives in #reportingGridFilter). */
  gridView: { sortCol: null, sortDir: 'asc' },
};

function escapeHtml(s) {
  return String(s ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

/** FastAPI `detail` may be a string, object, or list of validation errors — avoid `[object Object]` in Error.message. */
function formatApiErrorDetail(detail, fallback) {
  const fb = fallback || 'Request failed';
  if (detail == null || detail === '') return fb;
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        if (typeof item === 'string') return item;
        if (item && typeof item === 'object') {
          const msg = item.msg != null ? String(item.msg) : '';
          const loc = Array.isArray(item.loc) ? item.loc.filter(Boolean).join(' → ') : '';
          if (msg && loc) return `${loc}: ${msg}`;
          if (msg) return msg;
        }
        try {
          return JSON.stringify(item);
        } catch {
          return String(item);
        }
      })
      .join('\n');
  }
  if (typeof detail === 'object') {
    if (detail.msg != null) return String(detail.msg);
    try {
      return JSON.stringify(detail);
    } catch {
      return String(detail);
    }
  }
  return String(detail);
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(formatApiErrorDetail(data.detail, `${res.status} ${res.statusText}`));
  }
  return data;
}

function activeReportTab() {
  const btn = document.querySelector('.tab-btn.active[data-report-tab]');
  return btn?.dataset?.reportTab || 'text';
}

function switchReportTab(tab) {
  document.querySelectorAll('.tab-btn[data-report-tab]').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.reportTab === tab);
  });
  document.getElementById('reportTextPanel')?.classList.toggle('hidden', tab !== 'text');
  document.getElementById('reportGridPanel')?.classList.toggle('hidden', tab !== 'grid');
  document.getElementById('reportGraphPanel')?.classList.toggle('hidden', tab !== 'graph');
}

function friendlyColumnName(raw) {
  if (!raw) return '';
  const key = String(raw);
  const aliases = {
    _drill: 'Drill-through',
    inventory_id: 'Inventory ID',
    model_id: 'Model ID',
    knife_model_id: 'Model ID',
    knife_name: 'Knife Name',
    official_name: 'Knife Name',
    knife_type: 'Knife Type',
    family_name: 'Family',
    form_name: 'Form',
    series_name: 'Series',
    collaborator_name: 'Collaborator',
    generation_label: 'Generation',
    size_modifier: 'Size Modifier',
    acquired_date: 'Acquired Date',
    purchase_price: 'Purchase Price',
    estimated_value: 'Estimated Value',
    purchase_source: 'Purchase Source',
    handle_color: 'Handle Color',
    blade_color: 'Blade Color',
    blade_finish: 'Blade Finish',
    blade_length: 'Blade Length',
    row_count: 'Row Count',
    rows_count: 'Rows',
    total_spend: 'Total Spend',
    total_quantity: 'Total Quantity',
    total_estimated_value: 'Total Estimated Value',
    inventory_quantity: 'Inventory Quantity',
    missing_models_count: 'Missing Models',
    estimated_completion_cost_msrp: 'Estimated Completion Cost (MSRP)',
    avg_missing_model_msrp: 'Average Missing Model MSRP',
    msrp: 'MSRP',
  };
  if (aliases[key]) return aliases[key];
  const withSpaces = key
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .trim();
  if (!withSpaces) return key;
  return withSpaces
    .split(/\s+/)
    .map((w) => (w ? `${w.charAt(0).toUpperCase()}${w.slice(1).toLowerCase()}` : ''))
    .join(' ');
}

function pickBestResultTab(result) {
  if (result?.chart_spec?.data?.length && result?.chart_spec?.x && result?.chart_spec?.y) return 'graph';
  if ((result?.columns || []).length && (result?.rows || []).length) return 'grid';
  return 'text';
}

function renderFeedbackControls(m) {
  if (!m || m.role !== 'assistant' || !m.id || !state.sessionId) return '';
  const fb = m.meta?.feedback_helpful;
  const upOn = fb === true ? 'is-active' : '';
  const dnOn = fb === false ? 'is-active' : '';
  const disabled = typeof fb === 'boolean' ? 'disabled' : '';
  const title = typeof fb === 'boolean' ? 'Feedback recorded' : 'Rate this answer';
  return `
    <div class="reporting-feedback" data-message-id="${escapeHtml(m.id)}">
      <button type="button" class="secondary reporting-feedback-btn ${upOn}" data-helpful="true" ${disabled} title="${escapeHtml(title)}" aria-label="Helpful">👍</button>
      <button type="button" class="secondary reporting-feedback-btn ${dnOn}" data-helpful="false" ${disabled} title="${escapeHtml(title)}" aria-label="Not helpful">👎</button>
    </div>
  `;
}

function renderMessages() {
  const wrap = document.getElementById('reportingMessages');
  if (!wrap) return;
  if (!state.messages.length) {
    wrap.innerHTML = '<div class="muted small">No messages yet. Ask your first reporting question.</div>';
    return;
  }
  wrap.innerHTML = state.messages.map((m) => {
    const role = m.role === 'user' ? 'You' : 'Assistant';
    const cls = m.role === 'user' ? 'report-msg-user' : 'report-msg-assistant';
    return `<div class="report-msg ${cls}"><strong>${role}:</strong> ${escapeHtml(m.content || '')}${renderFeedbackControls(m)}</div>`;
  }).join('');
  wrap.scrollTop = wrap.scrollHeight;
}

function renderText(result) {
  const out = document.getElementById('reportingTextResult');
  if (!out) return;
  if (!result) {
    out.innerHTML = 'Ask a question to start.';
    return;
  }
  const extra = [];
  if (result.confidence != null) extra.push(`Confidence: ${result.confidence}`);
  if (result.execution_ms != null) extra.push(`Execution: ${result.execution_ms}ms`);
  if (result.limitations) extra.push(`Limitations: ${result.limitations}`);
  if (result.generation_mode) extra.push(`Mode: ${result.generation_mode}`);
  out.innerHTML = `
    <div>${escapeHtml(result.answer_text || '')}</div>
    ${extra.length ? `<p class="muted small">${escapeHtml(extra.join(' | '))}</p>` : ''}
    ${result.date_window?.label ? `<p class="muted small">Date window: ${escapeHtml(result.date_window.label)}</p>` : ''}
  `;
}

/**
 * Compare two cell values for sorting (strings with numeric-aware ordering).
 * @param {unknown} a
 * @param {unknown} b
 */
function compareReportingCellValues(a, b) {
  return String(a ?? '').localeCompare(String(b ?? ''), undefined, { numeric: true, sensitivity: 'base' });
}

/**
 * @param {Record<string, unknown>} row
 * @param {string} col
 */
function cellValueForSort(row, col) {
  if (col === '_drill') return row._drill_link ? 'open collection' : '';
  return row[col];
}

/**
 * @param {Record<string, unknown>} row
 * @param {string[]} gridCols
 * @param {string} needle Lowercased search string (may be empty).
 */
function reportingRowMatchesFilter(row, gridCols, needle) {
  if (!needle) return true;
  return gridCols.some((c) => {
    let text;
    if (c === '_drill') {
      text = row._drill_link ? 'open collection' : '';
    } else {
      text = String(row[c] ?? '');
    }
    return text.toLowerCase().includes(needle);
  });
}

/**
 * Renders the reporting table with optional reset of sort/filter (new query).
 * @param {object | null} result
 * @param {boolean} resetView Clear filter and sort state when true.
 */
function paintReportingGrid(result, resetView) {
  const thead = document.getElementById('reportingGridHead');
  const tbody = document.querySelector('#reportingGrid tbody');
  const filterInput = document.getElementById('reportingGridFilter');
  if (!thead || !tbody) return;
  const cols = result?.columns || [];
  const rows = result?.rows || [];
  if (!cols.length) {
    thead.innerHTML = '';
    tbody.innerHTML = '<tr><td class="muted">No tabular result.</td></tr>';
    return;
  }
  if (resetView) {
    state.gridView.sortCol = null;
    state.gridView.sortDir = 'asc';
    if (filterInput) filterInput.value = '';
  }
  const gridCols = [...cols];
  if (rows.some((r) => r._drill_link)) gridCols.push('_drill');
  if (state.gridView.sortCol && !gridCols.includes(state.gridView.sortCol)) {
    state.gridView.sortCol = null;
    state.gridView.sortDir = 'asc';
  }
  const needle = (filterInput?.value ?? '').trim().toLowerCase();
  let displayRows = rows.filter((r) => reportingRowMatchesFilter(r, gridCols, needle));
  const sortCol = state.gridView.sortCol;
  if (sortCol && gridCols.includes(sortCol)) {
    const dir = state.gridView.sortDir === 'desc' ? -1 : 1;
    displayRows = [...displayRows].sort((a, b) => {
      const va = cellValueForSort(a, sortCol);
      const vb = cellValueForSort(b, sortCol);
      return dir * compareReportingCellValues(va, vb);
    });
  }
  thead.innerHTML = gridCols
    .map((c) => {
      const label = friendlyColumnName(c);
      const isSorted = sortCol === c;
      const thCls = ['sortable'];
      if (isSorted) thCls.push(state.gridView.sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
      const ariaSort = isSorted ? (state.gridView.sortDir === 'asc' ? 'ascending' : 'descending') : 'none';
      return `<th class="${thCls.join(' ')}" data-col="${escapeHtml(c)}" scope="col" aria-sort="${ariaSort}">${escapeHtml(label)}</th>`;
    })
    .join('');
  if (!displayRows.length) {
    tbody.innerHTML = `<tr><td colspan="${gridCols.length}" class="muted reporting-grid-empty">No rows match this filter.</td></tr>`;
    return;
  }
  tbody.innerHTML = displayRows
    .map((r) => {
      const cells = gridCols
        .map((c) => {
          if (c === '_drill') {
            if (!r._drill_link) return '<td>—</td>';
            return `<td><a href="${escapeHtml(r._drill_link)}">Open collection</a></td>`;
          }
          const v = r[c];
          return `<td>${escapeHtml(v == null ? '' : v)}</td>`;
        })
        .join('');
      return `<tr>${cells}</tr>`;
    })
    .join('');
}

function renderGrid(result) {
  paintReportingGrid(result, true);
}

/** Re-applies filter and sort to the current grid without resetting (input/sort handlers). */
function refreshReportingGrid() {
  const result = state.lastResult;
  if (!result?.columns?.length) return;
  paintReportingGrid(result, false);
}

function renderGraph(result) {
  const wrap = document.getElementById('reportingGraphWrap');
  if (!wrap) return;
  const spec = result?.chart_spec;
  if (!spec?.data?.length || !spec?.x || !spec?.y) {
    wrap.innerHTML = 'No graph available for this result.';
    return;
  }
  const data = spec.data;
  const x = spec.x;
  const y = spec.y;
  const xLabel = friendlyColumnName(x);
  const yLabel = friendlyColumnName(y);
  const points = data
    .map((d) => ({ label: String(d[x] ?? ''), value: Number(d[y] ?? 0) }))
    .filter((d) => Number.isFinite(d.value));
  if (!points.length) {
    wrap.innerHTML = 'No graph available for this result.';
    return;
  }
  const width = 760;
  const height = 320;
  const max = Math.max(...points.map((p) => p.value), 1);

  if (spec.type === 'pie') {
    let angle = 0;
    const cx = 170;
    const cy = 160;
    const r = 120;
    const total = points.reduce((s, p) => s + p.value, 0) || 1;
    const colors = ['#d47c1c', '#2b9a66', '#4b8efc', '#b74a4a', '#8a63d2', '#2fa6b4', '#d2a63a'];
    const slices = points.map((p, idx) => {
      const frac = p.value / total;
      const a0 = angle;
      const a1 = angle + frac * Math.PI * 2;
      angle = a1;
      const x0 = cx + r * Math.cos(a0);
      const y0 = cy + r * Math.sin(a0);
      const x1 = cx + r * Math.cos(a1);
      const y1 = cy + r * Math.sin(a1);
      const large = a1 - a0 > Math.PI ? 1 : 0;
      return {
        d: `M ${cx} ${cy} L ${x0} ${y0} A ${r} ${r} 0 ${large} 1 ${x1} ${y1} Z`,
        color: colors[idx % colors.length],
        label: `${p.label} (${Math.round(frac * 100)}%)`,
      };
    });
    wrap.innerHTML = `
      <div class="muted small">Breakdown by ${escapeHtml(xLabel)} using ${escapeHtml(yLabel)}</div>
      <svg viewBox="0 0 ${width} ${height}" class="reporting-chart-svg">
        ${slices.map((s) => `<path d="${s.d}" fill="${s.color}" stroke="#111" stroke-width="1"></path>`).join('')}
      </svg>
      <ul class="pill-list">${slices.map((s) => `<li><span>${escapeHtml(s.label)}</span></li>`).join('')}</ul>
    `;
    return;
  }

  const margin = { top: 20, right: 20, bottom: 90, left: 60 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  if (spec.type === 'line') {
    const step = points.length > 1 ? innerW / (points.length - 1) : innerW;
    const coords = points.map((p, i) => {
      const px = margin.left + i * step;
      const py = margin.top + innerH - (p.value / max) * innerH;
      return { ...p, px, py };
    });
    const path = coords.map((c, i) => `${i === 0 ? 'M' : 'L'} ${c.px} ${c.py}`).join(' ');
    wrap.innerHTML = `
      <div class="muted small">${escapeHtml(yLabel)} by ${escapeHtml(xLabel)}</div>
      <svg viewBox="0 0 ${width} ${height}" class="reporting-chart-svg">
        <line x1="${margin.left}" y1="${margin.top + innerH}" x2="${margin.left + innerW}" y2="${margin.top + innerH}" stroke="#55606d"></line>
        <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + innerH}" stroke="#55606d"></line>
        <path d="${path}" fill="none" stroke="#d47c1c" stroke-width="2.5"></path>
        ${coords.map((c) => `<circle cx="${c.px}" cy="${c.py}" r="3.2" fill="#f2ad5f"></circle>`).join('')}
        ${coords.map((c) => `<text x="${c.px}" y="${height - 12}" transform="rotate(30 ${c.px} ${height - 12})" font-size="10" fill="#a8b3bd">${escapeHtml(c.label)}</text>`).join('')}
      </svg>
    `;
    return;
  }

  // default bar
  const barW = innerW / Math.max(points.length, 1);
  wrap.innerHTML = `
    <div class="muted small">${escapeHtml(yLabel)} by ${escapeHtml(xLabel)}</div>
    <svg viewBox="0 0 ${width} ${height}" class="reporting-chart-svg">
      <line x1="${margin.left}" y1="${margin.top + innerH}" x2="${margin.left + innerW}" y2="${margin.top + innerH}" stroke="#55606d"></line>
      <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + innerH}" stroke="#55606d"></line>
      ${points.map((p, i) => {
        const h = (p.value / max) * innerH;
        const xPos = margin.left + i * barW + 6;
        const yPos = margin.top + innerH - h;
        const w = Math.max(8, barW - 12);
        return `<rect x="${xPos}" y="${yPos}" width="${w}" height="${h}" fill="#d47c1c"></rect>`;
      }).join('')}
      ${points.map((p, i) => {
        const xPos = margin.left + i * barW + 8;
        return `<text x="${xPos}" y="${height - 12}" transform="rotate(30 ${xPos} ${height - 12})" font-size="10" fill="#a8b3bd">${escapeHtml(p.label)}</text>`;
      }).join('')}
    </svg>
  `;
}

function renderFollowups(result) {
  const wrap = document.getElementById('reportingFollowups');
  if (!wrap) return;
  const followups = result?.follow_ups || [];
  if (!followups.length) {
    wrap.innerHTML = '<span class="muted small">No suggestions yet.</span>';
    return;
  }
  wrap.innerHTML = followups.map((f) => `<button type="button" class="secondary reporting-followup-btn">${escapeHtml(f)}</button>`).join('');
  wrap.querySelectorAll('.reporting-followup-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const q = document.getElementById('reportingQuestion');
      if (!q) return;
      q.value = btn.textContent || '';
      q.focus();
    });
  });
}

function refreshOutputs(result) {
  renderText(result);
  renderGrid(result);
  renderGraph(result);
  renderFollowups(result);
}

async function loadModels() {
  const sel = document.getElementById('reportingModelSelect');
  const status = document.getElementById('reportingModelStatus');
  const star = document.getElementById('reportingModelDefaultBtn');
  if (!sel) return;
  try {
    const check = await api('/api/ai/ollama/check');
    const models = check.models || [];
    sel.innerHTML = '';
    models.forEach((m) => {
      const n = m?.name || m?.model;
      if (!n) return;
      const opt = document.createElement('option');
      opt.value = n;
      opt.textContent = n;
      sel.appendChild(opt);
    });
    const saved = localStorage.getItem(REPORTING_MODEL_KEY);
    const preferred = saved || 'qwen2.5:7b-instruct';
    if (Array.from(sel.options).some((o) => o.value === preferred)) {
      sel.value = preferred;
    }
    status.textContent = check.reachable ? `Connected to ${check.ollama_host}` : (check.error || 'Model server unavailable');
    const syncStar = () => {
      const isDefault = (sel.value || '') === (localStorage.getItem(REPORTING_MODEL_KEY) || '');
      star?.classList.toggle('is-default', isDefault);
    };
    sel.addEventListener('change', syncStar);
    star?.addEventListener('click', () => {
      if (!sel.value) return;
      localStorage.setItem(REPORTING_MODEL_KEY, sel.value);
      syncStar();
    });
    syncStar();
  } catch (err) {
    status.textContent = String(err.message || err);
    sel.innerHTML = '<option value="">No models available</option>';
  }
}

async function loadSessions() {
  const sel = document.getElementById('reportingSessionSelect');
  if (!sel) return;
  const data = await api('/api/reporting/sessions');
  state.sessions = data.sessions || [];
  sel.innerHTML = '';
  state.sessions.forEach((s) => {
    const opt = document.createElement('option');
    opt.value = s.id;
    opt.textContent = `${s.title || 'Session'} (${s.message_count || 0})`;
    sel.appendChild(opt);
  });
  if (!state.sessions.length) {
    const created = await api('/api/reporting/sessions', { method: 'POST' });
    state.sessionId = created.session.id;
    await loadSessions();
    return;
  }
  if (!state.sessionId || !state.sessions.some((s) => s.id === state.sessionId)) {
    state.sessionId = state.sessions[0].id;
  }
  sel.value = state.sessionId;
  await loadSessionDetail(state.sessionId);
}

async function loadSessionDetail(sessionId) {
  const data = await api(`/api/reporting/sessions/${encodeURIComponent(sessionId)}`);
  state.sessionId = sessionId;
  state.messages = data.messages || [];
  const assistant = [...state.messages].reverse().find((m) => m.role === 'assistant');
  state.lastResult = assistant
    ? {
        answer_text: assistant.content,
        ...(assistant.result || {}),
        chart_spec: assistant.chart_spec,
        sql_executed: assistant.sql_executed,
        follow_ups: assistant.meta?.follow_ups || [],
        confidence: assistant.meta?.confidence,
        limitations: assistant.meta?.limitations,
        execution_ms: assistant.meta?.execution_ms,
        generation_mode: assistant.meta?.generation_mode,
      }
    : null;
  renderMessages();
  refreshOutputs(state.lastResult);
  switchReportTab(pickBestResultTab(state.lastResult));
}

async function loadTemplates() {
  const wrap = document.getElementById('reportingTemplates');
  if (!wrap) return;
  const data = await api('/api/reporting/suggested-questions');
  state.templates = data.questions || [];
  wrap.innerHTML = state.templates.map((q) => `<button type="button" class="secondary reporting-template-btn">${escapeHtml(q)}</button>`).join('');
  wrap.querySelectorAll('.reporting-template-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const qEl = document.getElementById('reportingQuestion');
      if (!qEl) return;
      qEl.value = btn.textContent || '';
      qEl.focus();
    });
  });
}

async function loadSavedQueries() {
  const wrap = document.getElementById('reportingSavedQueries');
  if (!wrap) return;
  const data = await api('/api/reporting/saved-queries');
  state.savedQueries = data.saved_queries || [];
  if (!state.savedQueries.length) {
    wrap.innerHTML = '<li><span class="muted">No saved prompts.</span></li>';
    return;
  }
  wrap.innerHTML = state.savedQueries.map((q) => `
    <li>
      <span>${escapeHtml(q.name)}</span>
      <button type="button" class="secondary reporting-run-saved" data-id="${q.id}">Run</button>
      <button type="button" class="secondary reporting-del-saved" data-id="${q.id}">×</button>
    </li>
  `).join('');
  wrap.querySelectorAll('.reporting-run-saved').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const id = Number(btn.dataset.id);
      const sq = state.savedQueries.find((x) => x.id === id);
      if (!sq) return;
      const qEl = document.getElementById('reportingQuestion');
      if (qEl) qEl.value = sq.question || '';
      await runQuery();
    });
  });
  wrap.querySelectorAll('.reporting-del-saved').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const id = Number(btn.dataset.id);
      await api(`/api/reporting/saved-queries/${id}`, { method: 'DELETE' });
      await loadSavedQueries();
    });
  });
}

function toCsv(columns, rows) {
  const esc = (v) => {
    const s = String(v ?? '');
    return `"${s.replaceAll('"', '""')}"`;
  };
  const lines = [];
  lines.push(columns.map(esc).join(','));
  rows.forEach((r) => lines.push(columns.map((c) => esc(r[c])).join(',')));
  return lines.join('\n');
}

async function runQuery() {
  const qEl = document.getElementById('reportingQuestion');
  const modelSel = document.getElementById('reportingModelSelect');
  const maxRowsEl = document.getElementById('reportingMaxRows');
  const chartPrefEl = document.getElementById('reportingChartPreference');
  if (!qEl || !modelSel || !maxRowsEl || !chartPrefEl) return;
  const question = (qEl.value || '').trim();
  if (!question) return;

  const compareEnabled = document.getElementById('reportingCompareEnabled')?.checked;
  const body = {
    question,
    session_id: state.sessionId,
    model: modelSel.value || null,
    max_rows: Number(maxRowsEl.value || 200),
    chart_preference: chartPrefEl.value || null,
    compare_dimension: compareEnabled ? (document.getElementById('reportingCompareDimension')?.value || null) : null,
    compare_value_a: compareEnabled ? (document.getElementById('reportingCompareA')?.value || null) : null,
    compare_value_b: compareEnabled ? (document.getElementById('reportingCompareB')?.value || null) : null,
  };

  const messagesEl = document.getElementById('reportingMessages');
  if (messagesEl) {
    messagesEl.insertAdjacentHTML('beforeend', `<div class="report-msg report-msg-user"><strong>You:</strong> ${escapeHtml(question)}</div>`);
    messagesEl.insertAdjacentHTML('beforeend', '<div class="report-msg report-msg-assistant muted">Working…</div>');
    messagesEl.scrollTop = messagesEl.scrollHeight;
  }
  qEl.value = '';
  qEl.focus();

  try {
    const result = await api('/api/reporting/query', { method: 'POST', body: JSON.stringify(body) });
    state.sessionId = result.session_id;
    state.lastResult = result;
    await loadSessions();
    await loadSessionDetail(state.sessionId);
  } catch (err) {
    alert(err.message || String(err));
    await loadSessionDetail(state.sessionId);
  }
}

function bindEvents() {
  document.querySelectorAll('.tab-btn[data-report-tab]').forEach((btn) => {
    btn.addEventListener('click', () => switchReportTab(btn.dataset.reportTab));
  });

  document.getElementById('reportingForm')?.addEventListener('submit', async (e) => {
    e.preventDefault();
    await runQuery();
  });
  document.getElementById('reportingQuestion')?.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter') return;
    if (e.shiftKey) return;
    e.preventDefault();
    if (e.isComposing) return;
    document.getElementById('reportingForm')?.requestSubmit();
  });

  document.getElementById('reportingClearBtn')?.addEventListener('click', () => {
    const q = document.getElementById('reportingQuestion');
    if (q) q.value = '';
    q?.focus();
  });

  document.getElementById('newReportSessionBtn')?.addEventListener('click', async () => {
    const modelSel = document.getElementById('reportingModelSelect');
    const created = await api('/api/reporting/sessions', {
      method: 'POST',
      body: JSON.stringify({ model: modelSel?.value || null }),
      headers: {},
    });
    state.sessionId = created.session.id;
    await loadSessions();
  });

  document.getElementById('reportingSessionSelect')?.addEventListener('change', async (e) => {
    await loadSessionDetail(e.target.value);
  });

  document.getElementById('saveReportQueryBtn')?.addEventListener('click', async () => {
    const q = (document.getElementById('reportingQuestion')?.value || '').trim();
    if (!q) {
      alert('Enter a question to save.');
      return;
    }
    const name = prompt('Save prompt as:');
    if (!name || !name.trim()) return;
    const config = {
      chart_preference: document.getElementById('reportingChartPreference')?.value || '',
      compare_enabled: !!document.getElementById('reportingCompareEnabled')?.checked,
      compare_dimension: document.getElementById('reportingCompareDimension')?.value || '',
      compare_value_a: document.getElementById('reportingCompareA')?.value || '',
      compare_value_b: document.getElementById('reportingCompareB')?.value || '',
    };
    await api('/api/reporting/saved-queries', {
      method: 'POST',
      body: JSON.stringify({ name: name.trim(), question: q, config }),
    });
    await loadSavedQueries();
  });

  document.getElementById('reportingExportCsvBtn')?.addEventListener('click', () => {
    const result = state.lastResult;
    if (!result?.columns?.length || !result?.rows?.length) {
      alert('No grid data to export.');
      return;
    }
    const csv = toCsv(result.columns, result.rows);
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `report-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  });

  document.getElementById('reportingGridFilter')?.addEventListener('input', () => {
    refreshReportingGrid();
  });

  document.getElementById('reportingGrid')?.addEventListener('click', (e) => {
    const th = e.target.closest('th.sortable[data-col]');
    if (!th) return;
    const col = th.getAttribute('data-col');
    if (!col) return;
    if (state.gridView.sortCol === col) {
      state.gridView.sortDir = state.gridView.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      state.gridView.sortCol = col;
      state.gridView.sortDir = 'asc';
    }
    refreshReportingGrid();
  });

  document.getElementById('reportingMessages')?.addEventListener('click', async (e) => {
    const btn = e.target.closest('.reporting-feedback-btn[data-helpful]');
    if (!btn) return;
    const row = btn.closest('.reporting-feedback[data-message-id]');
    const messageId = Number(row?.getAttribute('data-message-id') || 0);
    if (!messageId || !state.sessionId) return;
    const helpful = btn.getAttribute('data-helpful') === 'true';
    try {
      await api('/api/reporting/feedback', {
        method: 'POST',
        body: JSON.stringify({
          session_id: state.sessionId,
          message_id: messageId,
          helpful,
        }),
      });
      await loadSessionDetail(state.sessionId);
    } catch (err) {
      alert(err.message || String(err));
    }
  });
}

async function init() {
  bindEvents();
  await Promise.all([loadModels(), loadTemplates(), loadSavedQueries()]);
  await loadSessions();
}

void init();
