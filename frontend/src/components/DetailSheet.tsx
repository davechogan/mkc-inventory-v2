import { useState, useEffect, useCallback, type ReactNode } from 'react';
import type { InventoryItem } from '../types';
import { imageUrl } from '../api';

interface DetailSheetProps {
  item: InventoryItem | null;
  onClose: () => void;
  onChanged: () => void;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(value);
}

function formatDate(value: string | null | undefined): string {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch {
    return value;
  }
}

interface FieldRowProps {
  label: string;
  value: ReactNode;
}

function FieldRow({ label, value }: FieldRowProps) {
  return (
    <div className="flex items-start justify-between gap-4 py-2 border-b border-border/40 last:border-0">
      <span className="text-muted text-xs uppercase tracking-wider font-semibold flex-shrink-0 pt-0.5">{label}</span>
      <span className="text-ink text-sm text-right">{value || '—'}</span>
    </div>
  );
}

interface SectionProps {
  title: string;
  children: ReactNode;
}

function Section({ title, children }: SectionProps) {
  return (
    <div className="mb-5">
      <div className="text-gold text-xs uppercase tracking-widest font-semibold mb-2 pb-1 border-b border-border/60">
        {title}
      </div>
      {children}
    </div>
  );
}

// ── Edit form types ──────────────────────────────────────────────────────────

interface ModelSearchResult {
  id: number;
  official_name: string;
}

interface EditForm {
  knife_model_id: number;
  knife_name: string; // display only, from search
  handle_color: string;
  blade_color: string;
  steel: string;
  blade_finish: string;
  blade_length: string;
  condition: string;
  location: string;
  quantity: string;
  purchase_price: string;
  estimated_value: string;
  acquired_date: string;
  mkc_order_number: string;
  purchase_source: string;
  nickname: string;
  notes: string;
}

function itemToForm(item: InventoryItem): EditForm {
  return {
    knife_model_id: item.knife_model_id,
    knife_name: item.knife_name,
    handle_color: item.handle_color ?? '',
    blade_color: item.blade_color ?? '',
    steel: item.blade_steel ?? '',
    blade_finish: item.blade_finish ?? '',
    blade_length: item.blade_length != null ? String(item.blade_length) : '',
    condition: item.condition ?? 'Like New',
    location: item.location ?? '',
    quantity: String(item.quantity),
    purchase_price: item.purchase_price != null ? String(item.purchase_price) : '',
    estimated_value: item.estimated_value != null ? String(item.estimated_value) : '',
    acquired_date: item.acquired_date ?? '',
    mkc_order_number: item.mkc_order_number ?? '',
    purchase_source: item.purchase_source ?? '',
    nickname: item.nickname ?? '',
    notes: item.notes ?? '',
  };
}

function parseApiError(err: unknown): string {
  if (!err || typeof err !== 'object') return 'Unknown error';
  const detail = (err as { detail?: unknown }).detail;
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) return detail.map((d: { msg?: string }) => d.msg ?? String(d)).join('; ');
  return JSON.stringify(detail ?? err);
}

// ── Component ────────────────────────────────────────────────────────────────

export function DetailSheet({ item, onClose, onChanged }: DetailSheetProps) {
  const isOpen = item !== null;

  // Edit state
  const [editing, setEditing] = useState(false);
  const [form, setForm] = useState<EditForm | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  // Model search state (for reassigning)
  const [modelQuery, setModelQuery] = useState('');
  const [modelResults, setModelResults] = useState<ModelSearchResult[]>([]);
  const [searchingModels, setSearchingModels] = useState(false);

  // Options for dropdowns
  const [opts, setOpts] = useState<Record<string, { name: string }[]> | null>(null);

  useEffect(() => {
    fetch('/api/v2/options')
      .then(r => r.json())
      .then(d => setOpts(d as Record<string, { name: string }[]>))
      .catch(() => {});
  }, []);

  // Reset when item changes
  useEffect(() => {
    setEditing(false);
    setForm(item ? itemToForm(item) : null);
    setMsg(null);
    setModelQuery('');
    setModelResults([]);
  }, [item?.id]);

  const startEditing = () => {
    if (item) {
      setForm(itemToForm(item));
      setEditing(true);
      setMsg(null);
    }
  };

  const setField = (key: keyof EditForm, value: string | number) => {
    setForm(prev => prev ? { ...prev, [key]: value } : prev);
  };

  // Model search
  const searchModels = useCallback(async (q: string) => {
    if (q.length < 2) { setModelResults([]); return; }
    setSearchingModels(true);
    try {
      const res = await fetch(`/api/v2/models/search?q=${encodeURIComponent(q)}`);
      if (res.ok) {
        const data = await res.json() as ModelSearchResult[];
        setModelResults(data.slice(0, 8));
      }
    } finally {
      setSearchingModels(false);
    }
  }, []);

  useEffect(() => {
    const timer = setTimeout(() => searchModels(modelQuery), 300);
    return () => clearTimeout(timer);
  }, [modelQuery, searchModels]);

  const selectModel = (m: ModelSearchResult) => {
    setForm(prev => prev ? { ...prev, knife_model_id: m.id, knife_name: m.official_name } : prev);
    setModelQuery('');
    setModelResults([]);
  };

  // Save
  const handleSave = async () => {
    if (!item || !form) return;
    setSaving(true);
    setMsg(null);
    const payload: Record<string, unknown> = {
      knife_model_id: form.knife_model_id,
      handle_color: form.handle_color || null,
      blade_color: form.blade_color || null,
      steel: form.steel || null,
      blade_finish: form.blade_finish || null,
      blade_length: form.blade_length ? Number(form.blade_length) : null,
      condition: form.condition || 'Like New',
      location: form.location || null,
      quantity: Number(form.quantity) || 1,
      purchase_price: form.purchase_price ? Number(form.purchase_price) : null,
      estimated_value: form.estimated_value ? Number(form.estimated_value) : null,
      acquired_date: form.acquired_date || null,
      mkc_order_number: form.mkc_order_number || null,
      purchase_source: form.purchase_source || null,
      nickname: form.nickname || null,
      notes: form.notes || null,
    };
    try {
      const res = await fetch(`/api/v2/inventory/${item.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(parseApiError(err));
      }
      setMsg({ ok: true, text: 'Saved' });
      setEditing(false);
      onChanged();
    } catch (e) {
      setMsg({ ok: false, text: e instanceof Error ? e.message : 'Save failed' });
    } finally {
      setSaving(false);
    }
  };

  // Delete
  const handleDelete = async () => {
    if (!item || !confirm(`Delete "${item.knife_name}" from your collection? This cannot be undone.`)) return;
    try {
      const res = await fetch(`/api/v2/inventory/${item.id}`, { method: 'DELETE' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(parseApiError(err));
      }
      onChanged();
      onClose();
    } catch (e) {
      setMsg({ ok: false, text: e instanceof Error ? e.message : 'Delete failed' });
    }
  };

  const inputCls = "w-full px-3 py-1.5 bg-card border border-border rounded-lg text-xs text-ink focus:outline-none focus:border-gold/60 transition-colors";
  const labelCls = "text-muted text-[10px] uppercase tracking-wider mb-0.5";

  const optNames = (key: string) => (opts?.[key] ?? []).map(o => o.name);

  const renderSelect = (label: string, key: keyof EditForm, options: string[]) => (
    <div key={key}>
      <div className={labelCls}>{label}</div>
      <select value={form?.[key] as string ?? ''} onChange={e => setField(key, e.target.value)} className={inputCls}>
        <option value="">—</option>
        {options.map(o => <option key={o} value={o}>{o}</option>)}
      </select>
    </div>
  );

  const renderInput = (label: string, key: keyof EditForm, type = 'text', placeholder = '') => (
    <div key={key}>
      <div className={labelCls}>{label}</div>
      <input type={type} value={form?.[key] as string ?? ''} onChange={e => setField(key, e.target.value)} className={inputCls} placeholder={placeholder} />
    </div>
  );

  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      )}

      {/* Panel */}
      <div
        className={`fixed right-0 top-0 h-full w-96 flex flex-col z-50 border-l border-border transition-transform duration-200 ${
          isOpen ? 'translate-x-0' : 'translate-x-full'
        }`}
        style={{ backgroundColor: '#060709' }}
      >
        {item && (
          <>
            {/* Header */}
            <div className="flex items-start justify-between px-5 py-4 border-b border-border flex-shrink-0">
              <div className="min-w-0 flex-1 pr-3">
                <h2 className="text-ink font-bold text-base leading-tight">
                  {editing ? form?.knife_name : item.knife_name}
                </h2>
                {!editing && item.nickname && (
                  <p className="text-muted text-sm mt-0.5">"{item.nickname}"</p>
                )}
              </div>
              <div className="flex items-center gap-1 flex-shrink-0">
                {!editing && (
                  <>
                    <button onClick={startEditing} title="Edit"
                      className="text-muted hover:text-gold transition-colors p-1 rounded-md hover:bg-border/30">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
                      </svg>
                    </button>
                    <button onClick={handleDelete} title="Delete"
                      className="text-muted hover:text-red-400 transition-colors p-1 rounded-md hover:bg-border/30">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="3 6 5 6 21 6" /><path d="M19 6l-1 14H6L5 6" /><path d="M10 11v6M14 11v6" /><path d="M9 6V4h6v2" />
                      </svg>
                    </button>
                  </>
                )}
                <button onClick={onClose}
                  className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30"
                  aria-label="Close detail">
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                  </svg>
                </button>
              </div>
            </div>

            {/* Image (read-only mode only) */}
            {!editing && imageUrl(item) && (
              <div className="flex-shrink-0 w-full bg-card" style={{ aspectRatio: '4/3' }}>
                <img
                  src={imageUrl(item)!}
                  alt={item.knife_name}
                  className="w-full h-full object-contain"
                  onError={(e) => { e.currentTarget.parentElement!.style.display = 'none'; }}
                />
              </div>
            )}

            {/* Content */}
            <div className="flex-1 overflow-y-auto px-5 py-4">
              {editing && form ? (
                /* ── Edit form ── */
                <div className="flex flex-col gap-3">
                  {/* Model reassignment */}
                  <div>
                    <div className={labelCls}>Knife Model</div>
                    <div className="text-ink text-xs mb-1 px-1">{form.knife_name} <span className="text-muted">(#{form.knife_model_id})</span></div>
                    <input
                      type="text"
                      value={modelQuery}
                      onChange={e => setModelQuery(e.target.value)}
                      placeholder="Search to change model..."
                      className={inputCls}
                    />
                    {searchingModels && <p className="text-muted text-[10px] mt-1">Searching...</p>}
                    {modelResults.length > 0 && (
                      <div className="mt-1 border border-border rounded-lg bg-card max-h-32 overflow-y-auto">
                        {modelResults.map(m => (
                          <button key={m.id} onClick={() => selectModel(m)}
                            className="w-full text-left px-3 py-1.5 text-xs text-ink hover:bg-border/40 transition-colors">
                            {m.official_name} <span className="text-muted">#{m.id}</span>
                          </button>
                        ))}
                      </div>
                    )}
                  </div>

                  {renderInput('Nickname', 'nickname')}
                  {renderSelect('Handle Color', 'handle_color', optNames('handle-colors'))}
                  {renderSelect('Blade Color', 'blade_color', optNames('blade-colors'))}
                  {renderSelect('Steel', 'steel', optNames('blade-steels'))}
                  {renderSelect('Blade Finish', 'blade_finish', optNames('blade-finishes'))}
                  {renderInput('Blade Length', 'blade_length', 'number', '3.5')}
                  {renderSelect('Condition', 'condition', optNames('conditions'))}
                  {renderInput('Location', 'location')}
                  {renderInput('Quantity', 'quantity', 'number')}
                  {renderInput('Purchase Price', 'purchase_price', 'number')}
                  {renderInput('Estimated Value', 'estimated_value', 'number')}
                  {renderInput('Date Acquired', 'acquired_date', 'date')}
                  {renderInput('Order #', 'mkc_order_number')}
                  {renderInput('Purchase Source', 'purchase_source')}
                  <div>
                    <div className={labelCls}>Notes</div>
                    <textarea value={form.notes} onChange={e => setField('notes', e.target.value)}
                      className={`${inputCls} resize-none`} rows={3} />
                  </div>

                  {msg && <p className={`text-xs ${msg.ok ? 'text-gold' : 'text-red-400'}`}>{msg.text}</p>}

                  <div className="flex gap-2 mt-1">
                    <button onClick={handleSave} disabled={saving}
                      className="flex-1 py-1.5 rounded-lg bg-gold text-black text-xs font-semibold hover:bg-gold-bright disabled:opacity-40 transition-colors">
                      {saving ? 'Saving...' : 'Save Changes'}
                    </button>
                    <button onClick={() => { setEditing(false); setForm(itemToForm(item)); setMsg(null); }}
                      className="px-4 py-1.5 rounded-lg border border-border text-xs text-muted hover:text-ink transition-colors">
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
                /* ── Read-only view ── */
                <>
                  <Section title="Identity">
                    <FieldRow label="Model ID" value={`#${item.knife_model_id}`} />
                    <FieldRow label="Type" value={item.knife_type} />
                    <FieldRow label="Family" value={item.knife_family} />
                    <FieldRow label="Form" value={item.form_name} />
                    {!!item.is_collab && (
                      <FieldRow label="Collab" value={item.collaboration_name} />
                    )}
                  </Section>

                  <Section title="Specs">
                    <FieldRow label="Handle Color" value={item.handle_color} />
                    <FieldRow label="Blade Steel" value={item.blade_steel} />
                    <FieldRow label="Blade Finish" value={item.blade_finish} />
                    <FieldRow label="Blade Color" value={item.blade_color} />
                    <FieldRow label="Blade Length" value={item.blade_length != null ? `${item.blade_length}"` : null} />
                  </Section>

                  <Section title="Acquisition">
                    {item.quantity > 1 && (
                      <FieldRow label="Quantity" value={`×${item.quantity}`} />
                    )}
                    <FieldRow label="Purchase Price" value={formatCurrency(item.purchase_price)} />
                    <FieldRow label="Est. Value" value={formatCurrency(item.estimated_value)} />
                    <FieldRow label="Acquired" value={formatDate(item.acquired_date)} />
                    <FieldRow label="Order #" value={item.mkc_order_number} />
                    <FieldRow label="Source" value={item.purchase_source} />
                  </Section>

                  {item.notes && (
                    <Section title="Notes">
                      <p className="text-ink text-sm leading-relaxed">{item.notes}</p>
                    </Section>
                  )}
                </>
              )}
            </div>
          </>
        )}
      </div>
    </>
  );
}
