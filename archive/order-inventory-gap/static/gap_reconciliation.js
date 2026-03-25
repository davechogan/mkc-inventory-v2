/**
 * Order vs inventory gap reconciliation UI.
 * Loads /api/order-inventory-gaps, saves overrides and manual order→inventory links.
 */

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

let lastPayload = null;

async function fetchJson(url, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (options.body != null) {
    headers['Content-Type'] = headers['Content-Type'] || 'application/json';
  }
  const res = await fetch(url, {
    ...options,
    headers,
  });
  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    throw new Error(text || res.statusText);
  }
  if (!res.ok) {
    const msg = data.detail || data.message || text || res.statusText;
    throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
  }
  return data;
}

function currentMode() {
  return document.getElementById('gapModeSelect').value === 'name_handle' ? 'name_handle' : 'full';
}

function renderVipList(items) {
  const el = document.getElementById('gapVipList');
  if (!items || !items.length) {
    el.innerHTML = '<li>None</li>';
    return;
  }
  el.innerHTML = items
    .map(
      (v) =>
        `<li>${escapeHtml(v.knife_name)} — <strong>${v.inventory_qty_excluded}</strong> pc</li>`
    )
    .join('');
}

function renderLinks(links) {
  const el = document.getElementById('gapLinksList');
  if (!links || !links.length) {
    el.innerHTML = '<li class="muted">No links yet</li>';
    return;
  }
  el.innerHTML = links
    .map((l) => {
      const title = escapeHtml((l.line_title || '').slice(0, 48));
      return `<li data-link-id="${l.id}">#${escapeHtml(l.order_number)} → inv <strong>${l.inventory_item_id}</strong> (${escapeHtml(l.knife_name || '')})<br/><span class="muted">${title}</span> <button type="button" class="secondary gap-del-link" data-id="${l.id}" style="margin-top:0.25rem;font-size:0.8rem;">Remove</button></li>`;
    })
    .join('');
}

function rowMatchesFilter(row, filter) {
  const g = row.gap_ordered_minus_inventory;
  if (filter === 'all') return true;
  if (filter === 'nonzero') return g !== 0;
  return g !== 0 && !row.override?.cleared;
}

function renderTable(rows, filter) {
  const body = document.getElementById('gapTableBody');
  const filtered = rows.filter((r) => rowMatchesFilter(r, filter));
  body.innerHTML = filtered
    .map((r) => {
      const g = r.gap_ordered_minus_inventory;
      const cleared = r.override && (r.override.cleared === 1 || r.override.cleared === true);
      const status = cleared
        ? `<span class="muted">Cleared</span>${r.override.note ? `<div class="small">${escapeHtml(r.override.note)}</div>` : ''}`
        : r.needs_attention
          ? '<span style="color:var(--danger);">Open</span>'
          : g === 0
            ? '<span style="color:var(--success);">Balanced</span>'
            : '<span class="muted">—</span>';
      const gapStyle =
        g > 0 ? 'color:var(--danger);' : g < 0 ? 'color:var(--accent);' : '';
      const invIds = (r.inventory_item_ids || '').trim();
      return `<tr data-bucket="${escapeHtml(r.bucket_key)}">
        <td><strong>${escapeHtml(r.knife_name)}</strong><div class="muted small">id ${r.knife_model_id}</div></td>
        <td><code>${escapeHtml(r.handle_bucket)}</code></td>
        <td><code>${escapeHtml(r.blade_bucket)}</code></td>
        <td>${r.ordered_qty}</td>
        <td>${r.inventory_qty}${invIds ? `<div class="muted small"># ${escapeHtml(invIds)}</div>` : ''}</td>
        <td style="${gapStyle}"><strong>${g > 0 ? '+' : ''}${g}</strong></td>
        <td>${status}</td>
        <td>
          <button type="button" class="secondary gap-clear-btn" style="font-size:0.8rem;">Not a discrepancy</button>
          <details style="margin-top:0.35rem;">
            <summary class="muted small">Examples / note</summary>
            <div class="small" style="margin-top:0.35rem;">${escapeHtml(r.order_examples || '—')}</div>
            <label class="full" style="margin-top:0.5rem;display:block;">
              <span class="muted">Note</span>
              <input type="text" class="gap-note-input" placeholder="Why this is OK or what to fix" value="${escapeHtml((r.override && r.override.note) || '')}" />
            </label>
            <label class="full" style="margin-top:0.35rem;display:block;">
              <span class="muted">Linked inventory ids (optional)</span>
              <input type="text" class="gap-inv-ids-input" placeholder="12, 34" value="${escapeHtml((r.override && r.override.linked_inventory_item_ids) || '')}" />
            </label>
            <button type="button" class="gap-save-btn" style="margin-top:0.35rem;font-size:0.8rem;">Save note / links</button>
            ${cleared ? `<button type="button" class="gap-unclear-btn secondary" style="margin-top:0.25rem;font-size:0.8rem;">Reopen</button>` : ''}
          </details>
        </td>
      </tr>`;
    })
    .join('');
}

async function loadGaps() {
  const errEl = document.getElementById('gapError');
  errEl.classList.add('hidden');
  errEl.textContent = '';
  const mode = currentMode() === 'name_handle' ? 'name_handle' : 'full';
  try {
    const data = await fetchJson(`/api/order-inventory-gaps?mode=${encodeURIComponent(mode)}`);
    lastPayload = data;
    const st = data.stats || {};
    const mtime = data.orders_csv_mtime_iso
      ? `<br/>Orders CSV (mtime UTC): <strong>${escapeHtml(data.orders_csv_mtime_iso)}</strong>`
      : '';
    const tacFull =
      data.match_mode === 'full' && st.tactical_model_ids_count != null
        ? `<br/>Tactical models (blade in match): ${st.tactical_model_ids_count}`
        : '';
    const sing =
      st.singleton_sku_model_ids_count != null
        ? `<br/>Singleton SKUs (Traditions + Ultra + Damascus): ${st.singleton_sku_model_ids_count}`
        : '';
    const dm =
      st.damascus_singleton_model_ids_count != null
        ? ` · Damascus subset: ${st.damascus_singleton_model_ids_count}`
        : '';
    const moQty = st.matched_by_inventory_order_number_qty ?? 0;
    const mo =
      moQty > 0
        ? `<br/>Netted via inv. order # ↔ email (mkc_order_number / notes): <strong>${moQty}</strong> pc`
        : '';
    const resolvedQty = st.resolved_by_inventory_acquired_date_qty ?? 0;
    const resolved =
      resolvedQty > 0
        ? `<br/>Acquired_date set but order # missing in email CSV: <strong>${resolvedQty}</strong> pc excluded`
        : '';
    document.getElementById('gapStats').innerHTML = `
      Open discrepancies: <strong>${data.open_discrepancy_count ?? 0}</strong><br/>
      Buckets: ${st.distinct_bucket_keys ?? '—'} · orders&gt;inv: ${st.buckets_orders_gt_inventory ?? '—'} · inv&gt;orders: ${st.buckets_inventory_gt_orders ?? '—'}<br/>
      Gift VIP qty excluded: ${st.gift_vip_inventory_qty_total ?? 0}${sing}${dm}${mo}${resolved}${tacFull}${mtime}
    `;
    renderVipList(data.vip_inventory_excluded);
    renderLinks(data.line_links);
    renderTable(data.rows, document.getElementById('gapFilterSelect').value);
  } catch (e) {
    errEl.textContent = e.message || String(e);
    errEl.classList.remove('hidden');
  }
}

async function saveOverride(bucketKey, cleared, note, linkedIds) {
  const mode = currentMode() === 'name_handle' ? 'name_handle' : 'full';
  await fetchJson('/api/order-inventory-gaps/override', {
    method: 'POST',
    body: JSON.stringify({
      bucket_key: bucketKey,
      match_mode: mode,
      cleared,
      note: note || null,
      linked_inventory_item_ids: linkedIds || null,
    }),
  });
  await loadGaps();
}

document.getElementById('gapRefreshBtn').addEventListener('click', loadGaps);

document.getElementById('gapRebuildPipelineBtn').addEventListener('click', async () => {
  const btn = document.getElementById('gapRebuildPipelineBtn');
  const logEl = document.getElementById('gapRebuildLog');
  if (!(btn instanceof HTMLButtonElement)) return;
  const prev = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Running…';
  logEl.classList.add('hidden');
  logEl.textContent = '';
  try {
    const data = await fetchJson('/api/order-inventory-gaps/rebuild-order-pipeline', {
      method: 'POST',
    });
    logEl.textContent = data.log || 'OK';
    logEl.classList.remove('hidden');
    await loadGaps();
  } catch (e) {
    logEl.textContent = e.message || String(e);
    logEl.classList.remove('hidden');
  } finally {
    btn.disabled = false;
    btn.textContent = prev;
  }
});
document.getElementById('gapModeSelect').addEventListener('change', loadGaps);
document.getElementById('gapFilterSelect').addEventListener('change', () => {
  if (lastPayload && lastPayload.rows) {
    renderTable(lastPayload.rows, document.getElementById('gapFilterSelect').value);
  }
});

document.getElementById('gapTableBody').addEventListener('click', async (ev) => {
  const t = ev.target;
  if (!(t instanceof HTMLElement)) return;
  const tr = t.closest('tr[data-bucket]');
  if (!tr) return;
  const bucketKey = tr.getAttribute('data-bucket');
  if (!bucketKey) return;

  if (t.classList.contains('gap-clear-btn')) {
    const note = tr.querySelector('.gap-note-input')?.value || '';
    const ids = tr.querySelector('.gap-inv-ids-input')?.value || '';
    await saveOverride(bucketKey, true, note, ids);
  }
  if (t.classList.contains('gap-unclear-btn')) {
    const note = tr.querySelector('.gap-note-input')?.value || '';
    const ids = tr.querySelector('.gap-inv-ids-input')?.value || '';
    await saveOverride(bucketKey, false, note, ids);
  }
  if (t.classList.contains('gap-save-btn')) {
    const note = tr.querySelector('.gap-note-input')?.value || '';
    const ids = tr.querySelector('.gap-inv-ids-input')?.value || '';
    const cleared =
      lastPayload?.rows?.find((r) => r.bucket_key === bucketKey)?.override?.cleared === 1 ||
      lastPayload?.rows?.find((r) => r.bucket_key === bucketKey)?.override?.cleared === true;
    await saveOverride(bucketKey, !!cleared, note, ids);
  }
});

document.getElementById('gapLinksList').addEventListener('click', async (ev) => {
  const t = ev.target;
  if (!(t instanceof HTMLElement)) return;
  if (!t.classList.contains('gap-del-link')) return;
  const id = t.getAttribute('data-id');
  if (!id) return;
  if (!confirm('Remove this link?')) return;
  await fetchJson(`/api/order-inventory-gaps/link/${encodeURIComponent(id)}`, { method: 'DELETE' });
  await loadGaps();
});

document.getElementById('linkSubmitBtn').addEventListener('click', async () => {
  const order_number = document.getElementById('linkOrderNumber').value.trim();
  const order_date = document.getElementById('linkOrderDate').value.trim();
  const line_title = document.getElementById('linkLineTitle').value.trim();
  const matched_catalog_name = document.getElementById('linkCatalogName').value.trim();
  const inventory_item_id = parseInt(document.getElementById('linkInvId').value, 10);
  const note = document.getElementById('linkNote').value.trim();
  if (!order_number || !line_title || !inventory_item_id) {
    alert('Order #, line title, and inventory item id are required.');
    return;
  }
  await fetchJson('/api/order-inventory-gaps/link', {
    method: 'POST',
    body: JSON.stringify({
      order_number,
      order_date: order_date || null,
      line_title,
      matched_catalog_name: matched_catalog_name || null,
      inventory_item_id,
      note: note || null,
    }),
  });
  await loadGaps();
});

document.getElementById('invSearchBtn').addEventListener('click', async () => {
  const q = document.getElementById('invSearchInput').value.trim();
  const out = document.getElementById('invSearchResults');
  if (!q) {
    out.innerHTML = '<span class="muted">Enter a search term.</span>';
    return;
  }
  try {
    const rows = await fetchJson(`/api/v2/inventory?search=${encodeURIComponent(q)}`);
    const slice = rows.slice(0, 25);
    out.innerHTML = slice
      .map(
        (r) =>
          `<div style="margin-bottom:0.35rem;border-bottom:1px solid var(--line);padding-bottom:0.35rem;">
            <button type="button" class="secondary gap-pick-inv" data-id="${r.id}" style="font-size:0.75rem;">Use id ${r.id}</button>
            <strong>${escapeHtml(r.knife_name)}</strong> · ${escapeHtml(r.handle_color || '')} · qty ${r.quantity}
          </div>`
      )
      .join('');
  } catch (e) {
    out.innerHTML = `<span style="color:var(--danger);">${escapeHtml(e.message)}</span>`;
  }
});

document.getElementById('invSearchResults').addEventListener('click', (ev) => {
  const t = ev.target;
  if (!(t instanceof HTMLElement)) return;
  if (!t.classList.contains('gap-pick-inv')) return;
  const id = t.getAttribute('data-id');
  if (id) document.getElementById('linkInvId').value = id;
});

loadGaps();
