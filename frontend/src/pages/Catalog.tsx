import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { Sidebar } from '../components/Sidebar';

// ── Types ─────────────────────────────────────────────────────────────────────

interface CatalogModel {
  id: number;
  parent_model_id: number | null;
  official_name: string;
  slug: string | null;
  knife_type: string | null;
  family_name: string | null;
  form_name: string | null;
  series_name: string | null;
  collaborator_name: string | null;
  blade_steel: string | null;
  blade_finish: string | null;
  handle_type: string | null;
  blade_length: number | null;
  msrp: number | null;
  official_product_url: string | null;
  model_notes: string | null;
  colorway_image_url: string | null;
  has_identifier_image: boolean;
  in_inventory_count: number;
}

interface CatalogFilters {
  type: string[];
  family: string[];
  form: string[];
  series: string[];
  collaboration: string[];
}

interface ActiveFilters {
  search: string;
  type: string;
  family: string;
  series: string;
  collaboration: string;
}

const emptyFilters: ActiveFilters = {
  search: '',
  type: '',
  family: '',
  series: '',
  collaboration: '',
};

const SIDEBAR_KEY = 'mkc_sidebar_collapsed';

// ── API ───────────────────────────────────────────────────────────────────────

async function fetchCatalog(active: ActiveFilters): Promise<CatalogModel[]> {
  const params = new URLSearchParams();
  if (active.search) params.set('search', active.search);
  if (active.type) params.set('type', active.type);
  if (active.family) params.set('family', active.family);
  if (active.series) params.set('series', active.series);
  if (active.collaboration) params.set('collaboration', active.collaboration);
  const res = await fetch(`/api/v2/catalog?${params.toString()}`);
  if (!res.ok) throw new Error(`API ${res.status}`);
  return res.json();
}

async function fetchCatalogFilters(): Promise<CatalogFilters> {
  const res = await fetch('/api/v2/catalog/filters');
  if (!res.ok) throw new Error(`API ${res.status}`);
  return res.json();
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function specsLine(model: CatalogModel): string {
  const parts: string[] = [];
  if (model.blade_steel) parts.push(model.blade_steel);
  if (model.blade_finish) parts.push(model.blade_finish);
  if (model.handle_type) parts.push(model.handle_type);
  if (model.blade_length) parts.push(`${model.blade_length}"`);
  return parts.join(' · ');
}

// ── ModelCard ─────────────────────────────────────────────────────────────────

function ModelCard({
  model,
  selected,
  onClick,
}: {
  model: CatalogModel;
  selected: boolean;
  onClick: () => void;
}) {
  const imgRef = useRef<HTMLImageElement>(null);
  const [imgLoaded, setImgLoaded] = useState(false);
  const [imgError, setImgError] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const imgSrc = model.colorway_image_url ?? null;

  useEffect(() => {
    if (!imgSrc) return;
    const el = containerRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && imgRef.current && !imgRef.current.src) {
          imgRef.current.src = imgSrc;
          observer.disconnect();
        }
      },
      { rootMargin: '200px' }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [imgSrc]);

  const specs = specsLine(model);

  return (
    <button
      onClick={onClick}
      className={`relative group text-left flex flex-col rounded-xl border overflow-hidden transition-all duration-300 hover:scale-[1.1] hover:z-10 hover:shadow-xl hover:shadow-gold/20 ${
        selected
          ? 'border-gold/50 ring-1 ring-gold/20'
          : 'border-border hover:border-gold/30'
      }`}
      style={{ backgroundColor: '#0f1114' }}
    >
      {/* Image area */}
      <div
        ref={containerRef}
        className="relative w-full aspect-[4/3] bg-card flex items-center justify-center overflow-hidden flex-shrink-0"
      >
        {imgSrc && !imgError ? (
          <>
            {!imgLoaded && (
              <div className="absolute inset-0 skeleton" />
            )}
            <img
              ref={imgRef}
              alt={model.official_name}
              onLoad={() => setImgLoaded(true)}
              onError={() => setImgError(true)}
              className={`w-full h-full object-contain transition-opacity duration-300 ${imgLoaded ? 'opacity-100' : 'opacity-0'}`}
            />
          </>
        ) : (
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" className="text-muted/20">
            <path d="M14.5 10c-.83 0-1.5-.67-1.5-1.5v-5c0-.83.67-1.5 1.5-1.5s1.5.67 1.5 1.5v5c0 .83-.67 1.5-1.5 1.5z" />
            <path d="M20.5 10H19V8.5c0-.83.67-1.5 1.5-1.5s1.5.67 1.5 1.5-.67 1.5-1.5 1.5z" />
            <path d="M9 18v-5c0-.83-.67-1.5-1.5-1.5S6 12.17 6 13v5" />
          </svg>
        )}

        {/* Owned badge */}
        {model.in_inventory_count > 0 && (
          <div className="absolute top-2 right-2 bg-gold text-black text-xs font-bold px-1.5 py-0.5 rounded-md leading-none">
            Owned
          </div>
        )}
      </div>

      {/* Body */}
      <div className="p-3 flex flex-col gap-1 flex-1">
        <div className="text-ink text-sm font-semibold leading-tight line-clamp-2">
          {model.official_name}
        </div>

        {model.family_name && (
          <div className="text-muted text-xs">{model.family_name}</div>
        )}

        {specs && (
          <div className="text-muted/60 text-xs mt-0.5 truncate">{specs}</div>
        )}

        {model.msrp != null && (
          <div className="text-gold text-xs mt-auto pt-1 font-medium">
            ${model.msrp.toLocaleString('en-US', { minimumFractionDigits: 0 })}
          </div>
        )}
      </div>
    </button>
  );
}

function parseApiError(err: unknown): string {
  if (!err || typeof err !== 'object') return 'Unknown error';
  const detail = (err as { detail?: unknown }).detail;
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) return detail.map((d: { msg?: string }) => d.msg ?? String(d)).join('; ');
  return JSON.stringify(detail ?? err);
}

// ── Detail panel ──────────────────────────────────────────────────────────────

interface Colorway {
  id: number;
  handle_color_id: number;
  handle_color: string;
  blade_color_id: number | null;
  blade_color: string | null;
  has_image: number;
  is_transparent: number;
}

interface ColorOption {
  id: number;
  name: string;
}

interface ModelFormData {
  official_name: string;
  knife_type: string;
  family_name: string;
  form_name: string;
  series_name: string;
  collaborator_name: string;
  steel: string;
  blade_finish: string;
  handle_type: string;
  blade_length: string;
  msrp: string;
  official_product_url: string;
  model_notes: string;
  parent_model_id: string;
}

function modelToForm(m: CatalogModel | null): ModelFormData {
  return {
    official_name: m?.official_name ?? '',
    knife_type: m?.knife_type ?? '',
    family_name: m?.family_name ?? '',
    form_name: m?.form_name ?? '',
    series_name: m?.series_name ?? '',
    collaborator_name: m?.collaborator_name ?? '',
    steel: m?.blade_steel ?? '',
    blade_finish: m?.blade_finish ?? '',
    handle_type: m?.handle_type ?? '',
    blade_length: m?.blade_length != null ? String(m.blade_length) : '',
    msrp: m?.msrp != null ? String(m.msrp) : '',
    official_product_url: m?.official_product_url ?? '',
    model_notes: m?.model_notes ?? '',
    parent_model_id: m?.parent_model_id != null ? String(m.parent_model_id) : '',
  };
}

interface OptionSets {
  types: string[];
  families: string[];
  forms: string[];
  series: string[];
  collaborations: string[];
  steels: string[];
  finishes: string[];
  handleTypes: string[];
}

function ModelDetail({
  model,
  onClose,
  onSaved,
  onDeleted,
  isNew,
}: {
  model: CatalogModel | null;
  onClose: () => void;
  onSaved: () => void;
  onDeleted: () => void;
  isNew: boolean;
}) {
  const [imgLoaded, setImgLoaded] = useState(false);
  const imgSrc = model?.colorway_image_url ?? null;

  // Edit mode
  const [editing, setEditing] = useState(isNew);
  const [form, setForm] = useState<ModelFormData>(modelToForm(model));
  const [saving, setSaving] = useState(false);
  const [formMsg, setFormMsg] = useState<{ ok: boolean; text: string } | null>(null);

  // Option sets for dropdowns
  const [opts, setOpts] = useState<OptionSets | null>(null);

  useEffect(() => {
    Promise.all([
      fetch('/api/v2/catalog/filters').then(r => r.json()) as Promise<CatalogFilters>,
      fetch('/api/v2/options').then(r => r.json()) as Promise<Record<string, { name: string }[]>>,
    ]).then(([filters, options]) => {
      setOpts({
        types: filters.type,
        families: filters.family,
        forms: filters.form,
        series: filters.series,
        collaborations: filters.collaboration,
        steels: (options['blade-steels'] ?? []).map(o => o.name),
        finishes: (options['blade-finishes'] ?? []).map(o => o.name),
        handleTypes: (options['handle-types'] ?? []).map(o => o.name),
      });
    }).catch(() => {});
  }, []);

  // Reset form when model changes
  useEffect(() => {
    setForm(modelToForm(model));
    setEditing(isNew);
    setFormMsg(null);
  }, [model?.id, isNew]);

  const setField = (key: keyof ModelFormData, value: string) => {
    setForm(prev => ({ ...prev, [key]: value }));
  };

  const handleSave = async () => {
    setSaving(true);
    setFormMsg(null);
    const payload: Record<string, unknown> = {
      official_name: form.official_name,
      knife_type: form.knife_type || null,
      form_name: form.form_name || null,
      family_name: form.family_name || null,
      series_name: form.series_name || null,
      collaborator_name: form.collaborator_name || null,
      steel: form.steel || null,
      blade_finish: form.blade_finish || null,
      handle_type: form.handle_type || null,
      blade_length: form.blade_length ? Number(form.blade_length) : null,
      msrp: form.msrp ? Number(form.msrp) : null,
      official_product_url: form.official_product_url || null,
      model_notes: form.model_notes || null,
      parent_model_id: form.parent_model_id ? Number(form.parent_model_id) : null,
    };
    try {
      const url = isNew ? '/api/v2/models' : `/api/v2/models/${model!.id}`;
      const method = isNew ? 'POST' : 'PUT';
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(parseApiError(err));
      }
      setFormMsg({ ok: true, text: isNew ? 'Model created' : 'Saved' });
      setEditing(false);
      onSaved();
    } catch (e) {
      setFormMsg({ ok: false, text: e instanceof Error ? e.message : 'Save failed' });
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteModel = async () => {
    if (!model || !confirm(`Delete "${model.official_name}"? This cannot be undone.`)) return;
    try {
      const res = await fetch(`/api/v2/models/${model.id}`, { method: 'DELETE' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(parseApiError(err));
      }
      onDeleted();
    } catch (e) {
      setFormMsg({ ok: false, text: e instanceof Error ? e.message : 'Delete failed' });
    }
  };

  // Colorway list
  const [colorways, setColorways] = useState<Colorway[]>([]);
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [uploadingId, setUploadingId] = useState<number | null>(null);

  const fetchColorways = useCallback(async () => {
    if (!model) return;
    const res = await fetch(`/api/v2/models/${model.id}/colorways`);
    if (res.ok) setColorways(await res.json() as Colorway[]);
  }, [model?.id]);

  // Add-colorway state
  const [handleColorId, setHandleColorId] = useState('');
  const [bladeColorId, setBladeColorId] = useState('');
  const [addCwFile, setAddCwFile] = useState<File | null>(null);
  const addCwFileRef = useRef<HTMLInputElement>(null);
  const [addingCw, setAddingCw] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [colorOptions, setColorOptions] = useState<{ handle_colors: ColorOption[]; blade_colors: ColorOption[] }>({ handle_colors: [], blade_colors: [] });

  useEffect(() => {
    fetchColorways();
    fetch('/api/v2/colors')
      .then((r) => r.json())
      .then((d) => setColorOptions(d as { handle_colors: ColorOption[]; blade_colors: ColorOption[] }))
      .catch(() => {});
  }, [fetchColorways]);

  const handleAddColorway = async () => {
    if (!handleColorId || !model) return;
    setAddingCw(true);
    setMsg(null);
    try {
      const body: Record<string, number> = { handle_color_id: Number(handleColorId) };
      if (bladeColorId) body.blade_color_id = Number(bladeColorId);
      const res = await fetch(`/api/v2/models/${model.id}/colorways`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(parseApiError(err));
      }
      const created = await res.json() as { id: number };
      // Upload image if file was attached
      if (addCwFile && created.id) {
        const fd = new FormData();
        fd.append('file', addCwFile);
        await fetch(`/api/v2/models/${model.id}/colorways/${created.id}/image`, { method: 'PUT', body: fd });
      }
      setMsg({ ok: true, text: addCwFile ? 'Colorway added with image' : 'Colorway added' });
      setHandleColorId('');
      setBladeColorId('');
      setAddCwFile(null);
      if (addCwFileRef.current) addCwFileRef.current.value = '';
      fetchColorways();
    } catch (e) {
      setMsg({ ok: false, text: e instanceof Error ? e.message : 'Failed' });
    } finally {
      setAddingCw(false);
    }
  };

  const handleUploadImage = async (cwId: number, file: File) => {
    if (!model) return;
    setUploadingId(cwId);
    const fd = new FormData();
    fd.append('file', file);
    try {
      const res = await fetch(`/api/v2/models/${model.id}/colorways/${cwId}/image`, { method: 'PUT', body: fd });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(parseApiError(err));
      }
      fetchColorways();
    } catch (e) {
      setMsg({ ok: false, text: e instanceof Error ? e.message : 'Upload failed' });
    } finally {
      setUploadingId(null);
    }
  };

  const handleDeleteCw = async (cwId: number) => {
    if (!model) return;
    setDeletingId(cwId);
    try {
      const res = await fetch(`/api/v2/models/${model.id}/colorways/${cwId}`, { method: 'DELETE' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(parseApiError(err));
      }
      setColorways((prev) => prev.filter((cw) => cw.id !== cwId));
    } finally {
      setDeletingId(null);
    }
  };

  const inputCls = "w-full px-3 py-1.5 bg-card border border-border rounded-lg text-xs text-ink focus:outline-none focus:border-gold/60 transition-colors";
  const labelCls = "text-muted text-[10px] uppercase tracking-wider mb-0.5";

  const specs: Array<[string, string | number | null]> = [
    ['Type', model?.knife_type ?? null],
    ['Family', model?.family_name ?? null],
    ['Series', model?.series_name ?? null],
    ['Form', model?.form_name ?? null],
    ['Steel', model?.blade_steel ?? null],
    ['Finish', model?.blade_finish ?? null],
    ['Handle Type', model?.handle_type ?? null],
    ['Blade Length', model?.blade_length ? `${model.blade_length}"` : null],
    ['MSRP', model?.msrp != null ? `$${model.msrp.toLocaleString('en-US', { minimumFractionDigits: 0 })}` : null],
  ].filter(([, v]) => v != null) as Array<[string, string | number]>;

  // Helper to render a select or text input for a form field
  const renderField = (label: string, key: keyof ModelFormData, options?: string[]) => (
    <div key={key}>
      <div className={labelCls}>{label}</div>
      {options ? (
        <select value={form[key] as string} onChange={e => setField(key, e.target.value)} className={inputCls}>
          <option value="">—</option>
          {options.map(o => <option key={o} value={o}>{o}</option>)}
        </select>
      ) : (
        <input type="text" value={form[key] as string} onChange={e => setField(key, e.target.value)} className={inputCls} />
      )}
    </div>
  );

  return (
    <div className="h-full flex flex-col overflow-hidden" style={{ backgroundColor: '#060709', borderLeft: '1px solid #1d2329' }}>
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-border flex-shrink-0">
        <span className="text-muted text-xs uppercase tracking-widest">
          {isNew ? 'New Model' : editing ? 'Edit Model' : 'Model Detail'}
        </span>
        <div className="flex items-center gap-1">
          {!isNew && !editing && (
            <button onClick={() => setEditing(true)} title="Edit"
              className="text-muted hover:text-gold transition-colors p-1 rounded-md hover:bg-border/30">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" /><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
              </svg>
            </button>
          )}
          {!isNew && !editing && (
            <button onClick={handleDeleteModel} title="Delete model"
              className="text-muted hover:text-red-400 transition-colors p-1 rounded-md hover:bg-border/30">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="3 6 5 6 21 6" /><path d="M19 6l-1 14H6L5 6" /><path d="M10 11v6M14 11v6" /><path d="M9 6V4h6v2" />
              </svg>
            </button>
          )}
          <button onClick={onClose}
            className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-y-auto p-5 flex flex-col gap-5">
        {editing ? (
          /* ── Edit / New form ── */
          <>
            <div className="flex flex-col gap-3">
              {renderField('Name *', 'official_name')}
              {renderField('Type *', 'knife_type', opts?.types)}
              {renderField('Family *', 'family_name', opts?.families)}
              {renderField('Form *', 'form_name', opts?.forms)}
              {renderField('Series', 'series_name', opts?.series)}
              {renderField('Collaborator', 'collaborator_name', opts?.collaborations)}
              {renderField('Steel', 'steel', opts?.steels)}
              {renderField('Blade Finish', 'blade_finish', opts?.finishes)}
              {renderField('Handle Type', 'handle_type', opts?.handleTypes)}
              <div>
                <div className={labelCls}>Blade Length</div>
                <input type="number" step="0.1" value={form.blade_length} onChange={e => setField('blade_length', e.target.value)} className={inputCls} placeholder='e.g. 3.5' />
              </div>
              <div>
                <div className={labelCls}>MSRP</div>
                <input type="number" step="1" value={form.msrp} onChange={e => setField('msrp', e.target.value)} className={inputCls} placeholder='e.g. 225' />
              </div>
              {renderField('Product URL', 'official_product_url')}
              <div>
                <div className={labelCls}>Notes</div>
                <textarea value={form.model_notes} onChange={e => setField('model_notes', e.target.value)}
                  className={`${inputCls} resize-none`} rows={3} placeholder="Model notes..." />
              </div>
              <div>
                <div className={labelCls}>Parent Model ID</div>
                <input type="number" value={form.parent_model_id} onChange={e => setField('parent_model_id', e.target.value)} className={inputCls} placeholder='Optional' />
              </div>
            </div>

            {formMsg && <p className={`text-xs ${formMsg.ok ? 'text-gold' : 'text-red-400'}`}>{formMsg.text}</p>}

            <div className="flex gap-2">
              <button onClick={handleSave} disabled={saving || !form.official_name.trim()}
                className="flex-1 py-1.5 rounded-lg bg-gold text-black text-xs font-semibold hover:bg-gold-bright disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
                {saving ? 'Saving...' : isNew ? 'Create Model' : 'Save Changes'}
              </button>
              <button onClick={() => { setEditing(false); setForm(modelToForm(model)); setFormMsg(null); if (isNew) onClose(); }}
                className="px-4 py-1.5 rounded-lg border border-border text-xs text-muted hover:text-ink transition-colors">
                Cancel
              </button>
            </div>
          </>
        ) : (
          /* ── Read-only view ── */
          <>
            {/* Image */}
            {imgSrc && (
              <div className="rounded-xl overflow-hidden bg-border/10 aspect-[4/3]">
                {!imgLoaded && <div className="skeleton w-full aspect-[4/3]" />}
                <img src={imgSrc} alt={model?.official_name ?? ''} onLoad={() => setImgLoaded(true)}
                  className={`w-full h-full object-contain transition-opacity duration-300 ${imgLoaded ? 'opacity-100' : 'opacity-0'}`} />
              </div>
            )}

            {/* Name */}
            <div>
              <h2 className="text-ink text-base font-bold leading-snug">{model?.official_name}</h2>
              {model?.collaborator_name && (
                <div className="text-gold text-xs mt-0.5">Collaboration: {model.collaborator_name}</div>
              )}
              {model && model.in_inventory_count > 0 && (
                <div className="inline-flex items-center gap-1 mt-1.5 px-2 py-0.5 bg-gold/15 rounded-full text-gold text-xs font-medium">
                  <svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41L9 16.17z"/></svg>
                  In your collection ({model.in_inventory_count})
                </div>
              )}
            </div>

            {/* Specs */}
            <div className="grid grid-cols-2 gap-x-4 gap-y-3">
              {specs.map(([label, value]) => (
                <div key={label}>
                  <div className="text-muted text-xs mb-0.5">{label}</div>
                  <div className="text-ink text-sm">{value}</div>
                </div>
              ))}
            </div>

            {/* Notes */}
            {model?.model_notes && (
              <div>
                <div className="text-muted text-xs mb-0.5">Notes</div>
                <div className="text-ink text-sm leading-relaxed">{model.model_notes}</div>
              </div>
            )}

            {/* Links */}
            {model?.official_product_url && (
              <a href={model.official_product_url} target="_blank" rel="noopener noreferrer"
                className="flex items-center gap-1.5 text-xs text-gold/80 hover:text-gold transition-colors">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" /><polyline points="15 3 21 3 21 9" /><line x1="10" y1="14" x2="21" y2="3" />
                </svg>
                View on MKC website
              </a>
            )}
          </>
        )}

        {/* Colorways — only show for existing models */}
        {!isNew && model && <><div className="border-t border-border/40 pt-4">
          <div className="text-muted text-xs uppercase tracking-widest mb-3">
            Colorways
            {colorways.length > 0 && <span className="ml-2 text-muted/60">({colorways.filter(c => !!c.has_image).length}/{colorways.length} with images)</span>}
          </div>
          {colorways.length === 0 ? (
            <p className="text-muted text-xs italic">No colorways defined yet.</p>
          ) : (
            <div className="flex flex-col gap-2">
              {colorways.map((cw) => (
                <div key={cw.id} className="flex items-center gap-3 group">
                  {/* Thumbnail or placeholder */}
                  {!!cw.has_image ? (
                    <img
                      src={`/api/v2/colorway-images/${cw.id}`}
                      alt={cw.handle_color}
                      className="w-14 h-10 object-contain rounded bg-card flex-shrink-0 border border-border"
                    />
                  ) : (
                    <label className="w-14 h-10 rounded bg-card flex-shrink-0 border border-dashed border-border/60 flex items-center justify-center cursor-pointer hover:border-gold/40 transition-colors">
                      {uploadingId === cw.id ? (
                        <span className="text-muted text-[10px]">...</span>
                      ) : (
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" className="text-muted/40">
                          <path d="M12 5v14M5 12h14" strokeLinecap="round" />
                        </svg>
                      )}
                      <input
                        type="file"
                        accept=".png,image/png"
                        className="hidden"
                        onChange={(e) => {
                          const f = e.target.files?.[0];
                          if (f) handleUploadImage(cw.id, f);
                          e.target.value = '';
                        }}
                      />
                    </label>
                  )}
                  {/* Color names */}
                  <div className="flex-1 min-w-0">
                    <span className="text-xs text-ink truncate block">{cw.handle_color}</span>
                    {cw.blade_color && (
                      <span className="text-[10px] text-muted truncate block">{cw.blade_color} blade</span>
                    )}
                  </div>
                  {/* Upload button for existing images (replace) */}
                  {!!cw.has_image && (
                    <label className="flex-shrink-0 text-muted hover:text-gold p-1 cursor-pointer transition-colors" title="Replace image">
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="17 8 12 3 7 8" /><line x1="12" y1="3" x2="12" y2="15" />
                      </svg>
                      <input
                        type="file"
                        accept=".png,image/png"
                        className="hidden"
                        onChange={(e) => {
                          const f = e.target.files?.[0];
                          if (f) handleUploadImage(cw.id, f);
                          e.target.value = '';
                        }}
                      />
                    </label>
                  )}
                  {/* Delete button */}
                  <button
                    onClick={() => handleDeleteCw(cw.id)}
                    disabled={deletingId === cw.id}
                    title="Remove colorway"
                    className="opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0 text-red-400 hover:text-red-300 disabled:opacity-30 p-1"
                  >
                    {deletingId === cw.id ? '...' : (
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="3 6 5 6 21 6" /><path d="M19 6l-1 14H6L5 6" /><path d="M10 11v6M14 11v6" /><path d="M9 6V4h6v2" />
                      </svg>
                    )}
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Add colorway */}
        <div className="border-t border-border/40 pt-4">
          <div className="text-muted text-xs uppercase tracking-widest mb-3">Add Colorway</div>
          <div className="flex flex-col gap-2">
            <select
              value={handleColorId}
              onChange={(e) => setHandleColorId(e.target.value)}
              className="w-full px-3 py-1.5 bg-card border border-border rounded-lg text-xs text-ink focus:outline-none focus:border-gold/60 transition-colors"
            >
              <option value="">Handle color *</option>
              {colorOptions.handle_colors.map((c) => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
            </select>
            <select
              value={bladeColorId}
              onChange={(e) => setBladeColorId(e.target.value)}
              className="w-full px-3 py-1.5 bg-card border border-border rounded-lg text-xs text-ink focus:outline-none focus:border-gold/60 transition-colors"
            >
              <option value="">Blade color — optional</option>
              {colorOptions.blade_colors.map((c) => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
            </select>
            <input
              ref={addCwFileRef}
              type="file"
              accept=".png,image/png"
              onChange={(e) => setAddCwFile(e.target.files?.[0] ?? null)}
              className="text-xs text-muted file:mr-3 file:py-1 file:px-3 file:rounded-md file:border file:border-border file:bg-card file:text-ink file:text-xs file:cursor-pointer hover:file:border-gold/40 transition-colors"
            />
            <button
              onClick={handleAddColorway}
              disabled={!handleColorId || addingCw}
              className="w-full py-1.5 rounded-lg bg-gold text-black text-xs font-semibold hover:bg-gold-bright disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              {addingCw ? 'Adding...' : addCwFile ? 'Add Colorway + Image' : 'Add Colorway'}
            </button>
            {msg && (
              <p className={`text-xs ${msg.ok ? 'text-gold' : 'text-red-400'}`}>
                {msg.text}
              </p>
            )}
          </div>
        </div></>}
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function Catalog() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true'
  );

  const [filters, setFilters] = useState<ActiveFilters>(emptyFilters);
  const [filterOptions, setFilterOptions] = useState<CatalogFilters | null>(null);
  const [models, setModels] = useState<CatalogModel[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<CatalogModel | null>(null);
  const [addingNew, setAddingNew] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const refreshCatalog = useCallback(() => setRefreshKey(k => k + 1), []);

  // Debounce search
  const [debouncedSearch, setDebouncedSearch] = useState('');
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(filters.search), 300);
    return () => clearTimeout(timer);
  }, [filters.search]);

  // Load filter options once
  useEffect(() => {
    fetchCatalogFilters().then(setFilterOptions).catch(() => {});
  }, []);

  // Listen to sidebar toggle
  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent<{ collapsed: boolean }>;
      setSidebarCollapsed(ce.detail.collapsed);
    };
    window.addEventListener('mkc-sidebar-toggle', handler);
    return () => window.removeEventListener('mkc-sidebar-toggle', handler);
  }, []);

  // Fetch catalog when server-side filters change
  const activeServerFilters = useMemo(() => ({
    search: debouncedSearch,
    type: filters.type,
    family: filters.family,
    series: filters.series,
    collaboration: filters.collaboration,
  }), [debouncedSearch, filters.type, filters.family, filters.series, filters.collaboration]);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetchCatalog(activeServerFilters)
      .then((data) => {
        setModels(data);
        setLoading(false);
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : 'Failed to load catalog');
        setLoading(false);
      });
  }, [activeServerFilters, refreshKey]);

  const handleFilterChange = useCallback(<K extends keyof ActiveFilters>(key: K, value: string) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
    // Deselect when filters change
    setSelected(null);
  }, []);

  const clearFilters = useCallback(() => {
    setFilters(emptyFilters);
    setSelected(null);
  }, []);

  const activeCount = Object.entries(filters).filter(([k, v]) => k !== 'search' && v).length;

  const marginClass = sidebarCollapsed ? 'ml-16' : 'ml-56';
  const hasDetail = selected !== null || addingNew;

  return (
    <div className="min-h-screen bg-surface">
      <Sidebar />

      <main className={`${marginClass} transition-[margin] duration-200 flex flex-col h-screen overflow-hidden`}>
        {/* Top bar */}
        <div className="flex items-center justify-between px-8 py-4 border-b border-border flex-shrink-0 gap-4 flex-wrap">
          <div className="flex items-center gap-3 flex-shrink-0">
            <h1 className="text-ink text-xl font-bold">Catalog</h1>
            <button
              onClick={() => { setSelected(null); setAddingNew(true); }}
              className="px-2.5 py-1 rounded-lg bg-gold text-black text-xs font-semibold hover:bg-gold-bright transition-colors"
            >
              + Add Model
            </button>
          </div>

          {/* Search */}
          <div className="relative flex-1 min-w-[180px] max-w-xs">
            <svg className="absolute left-3 top-1/2 -translate-y-1/2 text-muted pointer-events-none" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
            <input
              type="search"
              placeholder="Search models…"
              value={filters.search}
              onChange={(e) => handleFilterChange('search', e.target.value)}
              className="w-full pl-9 pr-4 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors"
            />
          </div>

          {/* Filter dropdowns */}
          <div className="flex items-center gap-2 flex-wrap">
            <FilterSelect
              value={filters.family}
              onChange={(v) => handleFilterChange('family', v)}
              options={filterOptions?.family ?? []}
              placeholder="Family"
            />
            <FilterSelect
              value={filters.type}
              onChange={(v) => handleFilterChange('type', v)}
              options={filterOptions?.type ?? []}
              placeholder="Type"
            />
            <FilterSelect
              value={filters.series}
              onChange={(v) => handleFilterChange('series', v)}
              options={filterOptions?.series ?? []}
              placeholder="Series"
            />
            {(filterOptions?.collaboration?.length ?? 0) > 0 && (
              <FilterSelect
                value={filters.collaboration}
                onChange={(v) => handleFilterChange('collaboration', v)}
                options={filterOptions?.collaboration ?? []}
                placeholder="Collaboration"
              />
            )}
            {activeCount > 0 && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-xs text-muted hover:text-ink border border-border hover:border-border/70 transition-colors"
              >
                Clear {activeCount}
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            )}
          </div>

          {/* Count */}
          {!loading && (
            <span className="text-muted text-xs flex-shrink-0">
              {models.length.toLocaleString()} model{models.length !== 1 ? 's' : ''}
            </span>
          )}
        </div>

        {/* Body */}
        <div className="flex flex-1 overflow-hidden">
          {/* Grid area */}
          <div className="flex-1 overflow-y-auto p-6">
            {error && (
              <div className="mb-4 px-4 py-3 rounded-lg bg-red-950/40 border border-red-800/50 text-red-300 text-sm">
                {error}
              </div>
            )}

            {loading ? (
              <div className="grid grid-cols-[repeat(auto-fill,minmax(180px,1fr))] gap-4">
                {Array.from({ length: 24 }).map((_, i) => (
                  <div key={i} className="rounded-xl overflow-hidden border border-border">
                    <div className="skeleton aspect-[4/3]" />
                    <div className="p-3 flex flex-col gap-2">
                      <div className="skeleton h-4 rounded w-3/4" />
                      <div className="skeleton h-3 rounded w-1/2" />
                    </div>
                  </div>
                ))}
              </div>
            ) : models.length === 0 ? (
              <div className="h-64 flex flex-col items-center justify-center gap-2">
                <p className="text-muted text-sm">No models match your filters.</p>
                {activeCount > 0 && (
                  <button onClick={clearFilters} className="text-gold text-xs hover:underline">Clear filters</button>
                )}
              </div>
            ) : (
              <div className="grid grid-cols-[repeat(auto-fill,minmax(180px,1fr))] gap-4">
                {models.map((model) => (
                  <ModelCard
                    key={model.id}
                    model={model}
                    selected={selected?.id === model.id}
                    onClick={() => setSelected((prev) => prev?.id === model.id ? null : model)}
                  />
                ))}
              </div>
            )}
          </div>

          {/* Detail panel */}
          <div
            className={`flex-shrink-0 overflow-hidden transition-[width] duration-200 ${hasDetail ? 'w-80' : 'w-0'}`}
          >
            {(selected || addingNew) && (
              <ModelDetail
                model={selected}
                isNew={addingNew}
                onClose={() => { setSelected(null); setAddingNew(false); }}
                onSaved={() => { refreshCatalog(); setAddingNew(false); }}
                onDeleted={() => { setSelected(null); refreshCatalog(); }}
              />
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

// ── FilterSelect ──────────────────────────────────────────────────────────────

function FilterSelect({
  value,
  onChange,
  options,
  placeholder,
}: {
  value: string;
  onChange: (v: string) => void;
  options: string[];
  placeholder: string;
}) {
  if (options.length === 0) return null;
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={`px-3 py-1.5 bg-card border rounded-lg text-sm focus:outline-none focus:border-gold/60 transition-colors ${
        value ? 'border-gold/40 text-gold' : 'border-border text-muted hover:text-ink'
      }`}
    >
      <option value="">{placeholder}</option>
      {options.map((o) => (
        <option key={o} value={o}>{o}</option>
      ))}
    </select>
  );
}
