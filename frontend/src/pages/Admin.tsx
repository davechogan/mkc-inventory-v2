import { useState, useEffect, useCallback, useRef } from 'react';
import { Sidebar } from '../components/Sidebar';

const SIDEBAR_KEY = 'mkc_sidebar_collapsed';

// ── Types ─────────────────────────────────────────────────────────────────────

interface UserRecord {
  id: string;
  email: string;
  name: string | null;
  tenant_id: string;
  role: string;
  first_seen: string;
  last_seen: string;
}

interface OptionItem {
  id: number;
  name: string;
}

type OptionsMap = Record<string, OptionItem[]>;

interface AuditModel {
  id: number;
  official_name: string;
  total_colorways: number;
  with_image: number;
}

interface AuditColorway {
  id: number;
  handle_color_id: number;
  handle_color: string;
  blade_color_id: number | null;
  blade_color: string | null;
  has_image: number;
  is_transparent: number;
}

const OPTION_TYPES: { key: string; label: string }[] = [
  { key: 'blade-steels',      label: 'Blade Steels' },
  { key: 'blade-finishes',    label: 'Blade Finishes' },
  { key: 'blade-colors',      label: 'Blade Colors' },
  { key: 'handle-colors',     label: 'Handle Colors' },
  { key: 'handle-types',      label: 'Handle Types' },
  { key: 'locations',         label: 'Locations' },
  { key: 'conditions',        label: 'Conditions' },
  { key: 'blade-types',       label: 'Blade Types' },
  { key: 'categories',        label: 'Categories' },
  { key: 'blade-families',    label: 'Blade Families' },
  { key: 'primary-use-cases', label: 'Primary Use Cases' },
  { key: 'collaborators',     label: 'Collaborators' },
  { key: 'generations',       label: 'Generations' },
  { key: 'size-modifiers',    label: 'Size Modifiers' },
  { key: 'platform-variants', label: 'Platform Variants' },
];

// ── OptionSection ─────────────────────────────────────────────────────────────

interface OptionSectionProps {
  optionKey: string;
  label: string;
  items: OptionItem[];
  onAdd: (key: string, name: string) => Promise<void>;
  onDelete: (key: string, id: number) => Promise<void>;
}

function OptionSection({ optionKey, label, items, onAdd, onDelete }: OptionSectionProps) {
  const [newName, setNewName] = useState('');
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleAdd = async () => {
    const trimmed = newName.trim();
    if (!trimmed) return;
    setAdding(true);
    setError(null);
    try {
      await onAdd(optionKey, trimmed);
      setNewName('');
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to add option');
    } finally {
      setAdding(false);
    }
  };

  return (
    <div className="bg-card border border-border rounded-xl p-4">
      <h3 className="text-ink font-semibold text-sm mb-3">{label}</h3>

      {/* Add row */}
      <div className="flex gap-2 mb-3">
        <input
          type="text"
          value={newName}
          onChange={e => setNewName(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && handleAdd()}
          placeholder={`Add ${label.toLowerCase()}…`}
          className="flex-1 bg-surface border border-border rounded-lg px-3 py-1.5 text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60"
        />
        <button
          onClick={handleAdd}
          disabled={adding || !newName.trim()}
          className="px-3 py-1.5 rounded-lg bg-gold text-black text-sm font-semibold disabled:opacity-40 hover:bg-gold/90 transition-colors"
        >
          Add
        </button>
      </div>

      {error && <p className="text-red-400 text-xs mb-2">{error}</p>}

      {/* Items */}
      <div className="flex flex-wrap gap-1.5 max-h-40 overflow-y-auto">
        {items.length === 0 && (
          <span className="text-muted text-xs italic">No options yet.</span>
        )}
        {items.map(item => (
          <span
            key={item.id}
            className="flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-border/60 text-muted group"
          >
            {item.name}
            <button
              onClick={() => onDelete(optionKey, item.id)}
              title="Remove"
              className="opacity-0 group-hover:opacity-100 transition-opacity text-red-400 hover:text-red-300 leading-none ml-0.5"
            >
              ×
            </button>
          </span>
        ))}
      </div>
    </div>
  );
}

// ── ImageAudit ───────────────────────────────────────────────────────────────

function AuditRow({ model, onUploaded }: { model: AuditModel; onUploaded: () => void }) {
  const [expanded, setExpanded] = useState(false);
  const [colorways, setColorways] = useState<AuditColorway[]>([]);
  const [uploadingId, setUploadingId] = useState<number | null>(null);

  useEffect(() => {
    if (!expanded) return;
    fetch(`/api/v2/models/${model.id}/colorways`)
      .then(r => r.json())
      .then(d => setColorways(d as AuditColorway[]))
      .catch(() => {});
  }, [expanded, model.id]);

  const handleUpload = async (cwId: number, file: File) => {
    setUploadingId(cwId);
    const fd = new FormData();
    fd.append('file', file);
    try {
      const res = await fetch(`/api/v2/models/${model.id}/colorways/${cwId}/image`, { method: 'PUT', body: fd });
      if (res.ok) {
        setColorways(prev => prev.map(c => c.id === cwId ? { ...c, has_image: 1 } : c));
        onUploaded();
      }
    } finally {
      setUploadingId(null);
    }
  };

  const pct = model.total_colorways > 0 ? Math.round((model.with_image / model.total_colorways) * 100) : 0;
  const missing = model.total_colorways - model.with_image;

  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <button
        onClick={() => setExpanded(e => !e)}
        className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-card/50 transition-colors"
      >
        <svg
          width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
          className={`text-muted flex-shrink-0 transition-transform ${expanded ? 'rotate-90' : ''}`}
        >
          <polyline points="9 18 15 12 9 6" />
        </svg>
        <span className="flex-1 text-sm text-ink truncate">{model.official_name}</span>
        <span className="text-xs text-muted flex-shrink-0 w-20 text-right">
          {model.with_image}/{model.total_colorways}
        </span>
        {/* Progress bar */}
        <div className="w-24 h-1.5 bg-border rounded-full flex-shrink-0 overflow-hidden">
          <div
            className={`h-full rounded-full transition-all ${pct === 100 ? 'bg-green-500' : missing > 0 ? 'bg-gold' : 'bg-border'}`}
            style={{ width: `${pct}%` }}
          />
        </div>
      </button>

      {expanded && (
        <div className="border-t border-border bg-surface/50 px-4 py-3 flex flex-col gap-2">
          {colorways.length === 0 ? (
            <p className="text-muted text-xs italic">No colorways defined.</p>
          ) : colorways.map(cw => (
            <div key={cw.id} className="flex items-center gap-3">
              {!!cw.has_image ? (
                <img
                  src={`/api/v2/colorway-images/${cw.id}`}
                  alt={cw.handle_color}
                  className="w-12 h-8 object-contain rounded bg-card border border-border flex-shrink-0"
                />
              ) : (
                <UploadSlot cwId={cw.id} uploading={uploadingId === cw.id} onFile={f => handleUpload(cw.id, f)} />
              )}
              <span className="text-xs text-ink flex-1 truncate">
                {cw.handle_color}{cw.blade_color ? ` / ${cw.blade_color}` : ''}
              </span>
              {!cw.has_image && (
                <span className="text-[10px] text-red-400/80 flex-shrink-0">needs image</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function UploadSlot({ uploading, onFile }: { cwId: number; uploading: boolean; onFile: (f: File) => void }) {
  const ref = useRef<HTMLInputElement>(null);
  return (
    <label className="w-12 h-8 rounded bg-card border border-dashed border-border/60 flex items-center justify-center cursor-pointer hover:border-gold/40 transition-colors flex-shrink-0">
      {uploading ? (
        <span className="text-muted text-[10px]">...</span>
      ) : (
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" className="text-muted/40">
          <path d="M12 5v14M5 12h14" strokeLinecap="round" />
        </svg>
      )}
      <input
        ref={ref}
        type="file"
        accept=".png,image/png"
        className="hidden"
        onChange={e => {
          const f = e.target.files?.[0];
          if (f) onFile(f);
          if (ref.current) ref.current.value = '';
        }}
      />
    </label>
  );
}

function ImageAudit() {
  const [models, setModels] = useState<AuditModel[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<'all' | 'missing' | 'complete'>('missing');

  const fetchAudit = useCallback(async () => {
    const res = await fetch('/api/v2/colorway-audit');
    if (res.ok) setModels(await res.json() as AuditModel[]);
    setLoading(false);
  }, []);

  useEffect(() => { fetchAudit(); }, [fetchAudit]);

  const filtered = models.filter(m => {
    if (filter === 'missing') return m.total_colorways > 0 && m.with_image < m.total_colorways;
    if (filter === 'complete') return m.total_colorways > 0 && m.with_image === m.total_colorways;
    return true;
  });

  const totalColorways = models.reduce((s, m) => s + m.total_colorways, 0);
  const totalWithImage = models.reduce((s, m) => s + m.with_image, 0);

  return (
    <div>
      {/* Summary */}
      <div className="flex items-center gap-6 mb-4">
        <p className="text-muted text-sm">
          {totalWithImage}/{totalColorways} colorways have images ({totalColorways > 0 ? Math.round((totalWithImage / totalColorways) * 100) : 0}%)
        </p>
        <div className="flex gap-2">
          {(['missing', 'complete', 'all'] as const).map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-2.5 py-1 rounded-md text-xs transition-colors capitalize ${
                filter === f ? 'bg-gold/20 text-gold' : 'text-muted hover:text-ink'
              }`}
            >
              {f === 'missing' ? `Missing (${models.filter(m => m.total_colorways > 0 && m.with_image < m.total_colorways).length})` :
               f === 'complete' ? `Complete (${models.filter(m => m.total_colorways > 0 && m.with_image === m.total_colorways).length})` :
               `All (${models.length})`}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="text-muted text-sm">Loading...</div>
      ) : (
        <div className="flex flex-col gap-2">
          {filtered.map(m => (
            <AuditRow key={m.id} model={m} onUploaded={fetchAudit} />
          ))}
          {filtered.length === 0 && (
            <p className="text-muted text-sm py-8 text-center">No models match this filter.</p>
          )}
        </div>
      )}
    </div>
  );
}

// ── AccessLog ────────────────────────────────────────────────────────────────

function AccessLog() {
  const [users, setUsers] = useState<UserRecord[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/v2/users')
      .then(r => r.json())
      .then(d => setUsers(d as UserRecord[]))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const formatDate = (iso: string) => {
    try {
      // DB stores UTC — append Z so the browser parses as UTC and displays in local time
      const utc = iso.endsWith('Z') ? iso : iso + 'Z';
      return new Date(utc).toLocaleString('en-US', {
        month: 'short', day: 'numeric', year: 'numeric',
        hour: 'numeric', minute: '2-digit',
      });
    } catch { return iso; }
  };

  return (
    <div>
      <p className="text-muted text-sm mb-4">
        Users authenticated via Cloudflare Access. Sorted by most recent activity.
      </p>
      {loading ? (
        <div className="text-muted text-sm">Loading...</div>
      ) : users.length === 0 ? (
        <div className="text-muted text-sm py-8 text-center">No users have accessed the app yet.</div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-border">
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="bg-border/20 text-left">
                <th className="px-4 py-2.5 text-muted font-medium text-xs uppercase tracking-wider">Email</th>
                <th className="px-4 py-2.5 text-muted font-medium text-xs uppercase tracking-wider">Name</th>
                <th className="px-4 py-2.5 text-muted font-medium text-xs uppercase tracking-wider">Tenant</th>
                <th className="px-4 py-2.5 text-muted font-medium text-xs uppercase tracking-wider">Role</th>
                <th className="px-4 py-2.5 text-muted font-medium text-xs uppercase tracking-wider">First Seen</th>
                <th className="px-4 py-2.5 text-muted font-medium text-xs uppercase tracking-wider">Last Seen</th>
              </tr>
            </thead>
            <tbody>
              {users.map(u => (
                <tr key={u.id} className="border-t border-border/50 hover:bg-border/10 transition-colors">
                  <td className="px-4 py-2.5 text-ink">{u.email}</td>
                  <td className="px-4 py-2.5 text-muted">{u.name ?? '—'}</td>
                  <td className="px-4 py-2.5">
                    <span className="px-2 py-0.5 rounded-full bg-border/60 text-muted text-xs">{u.tenant_id}</span>
                  </td>
                  <td className="px-4 py-2.5">
                    <span className={`px-2 py-0.5 rounded-full text-xs ${
                      u.role === 'admin' ? 'bg-gold/20 text-gold' : 'bg-border/60 text-muted'
                    }`}>{u.role}</span>
                  </td>
                  <td className="px-4 py-2.5 text-muted text-xs">{formatDate(u.first_seen)}</td>
                  <td className="px-4 py-2.5 text-muted text-xs">{formatDate(u.last_seen)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── QuickTag ─────────────────────────────────────────────────────────────────

interface TagField {
  key: string;          // JSON field name sent to PATCH
  label: string;        // Display label
  optionsKey: string;   // Key in /api/v2/options response
}

const TAG_FIELDS: TagField[] = [
  { key: 'handle_type',  label: 'Handle Type',  optionsKey: 'handle-types' },
  { key: 'steel',        label: 'Blade Steel',  optionsKey: 'blade-steels' },
  { key: 'blade_finish', label: 'Blade Finish', optionsKey: 'blade-finishes' },
];

interface TagModel {
  id: number;
  official_name: string;
  family_name: string | null;
  knife_type: string | null;
  handle_type: string | null;
  blade_steel: string | null;
  blade_finish: string | null;
  blade_length: number | null;
  [key: string]: unknown;
}

// Map TAG_FIELDS.key to the model property name returned by /api/v2/models/search
const FIELD_TO_PROP: Record<string, keyof TagModel> = {
  handle_type: 'handle_type',
  steel: 'blade_steel',
  blade_finish: 'blade_finish',
};

function QuickTag({ options }: { options: OptionsMap }) {
  const [models, setModels] = useState<TagModel[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeField, setActiveField] = useState<TagField>(TAG_FIELDS[0]);
  const [filter, setFilter] = useState<'unset' | 'all'>('unset');
  const [saving, setSaving] = useState<number | null>(null);
  const [saved, setSaved] = useState<number | null>(null);

  useEffect(() => {
    fetch('/api/v2/models/search?limit=200')
      .then(r => r.json())
      .then(d => setModels(d as TagModel[]))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const propName = FIELD_TO_PROP[activeField.key];
  const filtered = filter === 'unset'
    ? models.filter(m => !m[propName])
    : models;

  // Group by family
  const grouped = new Map<string, TagModel[]>();
  for (const m of filtered) {
    const fam = m.family_name || '(no family)';
    if (!grouped.has(fam)) grouped.set(fam, []);
    grouped.get(fam)!.push(m);
  }

  const fieldOptions = options[activeField.optionsKey] ?? [];
  const unsetCount = models.filter(m => !m[propName]).length;

  const handleChange = async (model: TagModel, value: string) => {
    setSaving(model.id);
    try {
      const res = await fetch(`/api/v2/models/${model.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [activeField.key]: value || null }),
      });
      if (res.ok) {
        setModels(prev => prev.map(m =>
          m.id === model.id ? { ...m, [propName]: value || null } : m
        ));
        setSaved(model.id);
        setTimeout(() => setSaved(prev => prev === model.id ? null : prev), 1200);
      }
    } finally {
      setSaving(null);
    }
  };

  return (
    <div>
      {/* Controls */}
      <div className="flex flex-wrap items-center gap-4 mb-5">
        <div className="flex items-center gap-2">
          <span className="text-muted text-xs">Field:</span>
          <div className="flex rounded-lg border border-border overflow-hidden">
            {TAG_FIELDS.map(f => (
              <button
                key={f.key}
                onClick={() => setActiveField(f)}
                className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                  activeField.key === f.key
                    ? 'bg-gold/20 text-gold'
                    : 'text-muted hover:text-ink hover:bg-border/30'
                }`}
              >
                {f.label}
              </button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-muted text-xs">Show:</span>
          <div className="flex rounded-lg border border-border overflow-hidden">
            {([['unset', `Unset (${unsetCount})`], ['all', `All (${models.length})`]] as const).map(([key, label]) => (
              <button
                key={key}
                onClick={() => setFilter(key)}
                className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                  filter === key
                    ? 'bg-gold/20 text-gold'
                    : 'text-muted hover:text-ink hover:bg-border/30'
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {loading ? (
        <div className="text-muted text-sm">Loading…</div>
      ) : filtered.length === 0 ? (
        <div className="text-muted text-sm py-12 text-center">
          {filter === 'unset' ? `All models have ${activeField.label.toLowerCase()} set.` : 'No models found.'}
        </div>
      ) : (
        <div className="flex flex-col gap-6">
          {[...grouped.entries()].map(([family, familyModels]) => (
            <div key={family}>
              <h3 className="text-ink text-sm font-semibold mb-2 flex items-center gap-2">
                {family}
                <span className="text-muted text-xs font-normal">({familyModels.length})</span>
              </h3>
              <div className="flex flex-col gap-1">
                {familyModels.map(m => (
                  <div
                    key={m.id}
                    className="flex items-center gap-3 px-3 py-2 rounded-lg border border-border bg-card hover:border-border/70 transition-colors"
                  >
                    {/* Thumbnail */}
                    <img
                      src={`/api/v2/models/${m.id}/image`}
                      alt=""
                      className="w-10 h-10 rounded object-contain bg-surface flex-shrink-0"
                      onError={e => { (e.target as HTMLImageElement).style.display = 'none'; }}
                    />
                    {/* Name + type */}
                    <div className="flex-1 min-w-0">
                      <div className="text-ink text-sm truncate">{m.official_name}</div>
                      <div className="text-muted text-xs truncate">{m.knife_type}</div>
                    </div>
                    {/* Current value + dropdown */}
                    <div className="flex items-center gap-2 flex-shrink-0">
                      <select
                        value={(m[propName] as string) ?? ''}
                        onChange={e => handleChange(m, e.target.value)}
                        disabled={saving === m.id}
                        className={`w-44 px-2 py-1.5 bg-surface border rounded-lg text-sm focus:outline-none focus:border-gold/60 transition-colors ${
                          m[propName]
                            ? 'border-border text-ink'
                            : 'border-gold/40 text-muted'
                        }`}
                      >
                        <option value="">— unset —</option>
                        {fieldOptions.map(o => (
                          <option key={o.id} value={o.name}>{o.name}</option>
                        ))}
                      </select>
                      {saving === m.id && <span className="text-muted text-xs">saving…</span>}
                      {saved === m.id && <span className="text-green-400 text-xs">✓</span>}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Vision Debug ─────────────────────────────────────────────────────────────

interface VisionCandidate {
  name: string;
  family: string;
  form: string | null;
  handle_type: string | null;
  score: number;
  reasons: string[];
  image_b64: string;
  silhouette_b64: string | null;
}

interface PipelineResult {
  name: string;
  family: string;
  form: string | null;
  score: number;
  reasons: string[];
  vision_match: string | null;
  vision_reason: string | null;
}

interface VisionDebugResponse {
  original_image: string;
  clean_image: string;
  vision_model: string;
  filters: Record<string, unknown>;
  user_profile: Record<string, unknown> | null;
  families_total: number;
  families_eliminated: number;
  candidates: VisionCandidate[];
  pipeline_results: PipelineResult[];
  vision_used: boolean;
}

function VisionDebug() {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [handleMaterial, setHandleMaterial] = useState('');
  const [handleColor] = useState('');
  const [isCulinary, setIsCulinary] = useState<string>('');
  const [result, setResult] = useState<VisionDebugResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (f) {
      setFile(f);
      setResult(null);
      setError(null);
    }
  };

  const handleSubmit = async () => {
    if (!file) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const fd = new FormData();
      fd.append('image', file);
      if (handleMaterial) fd.append('handle_material', handleMaterial);
      if (handleColor) fd.append('handle_color', handleColor);
      if (isCulinary === 'true') fd.append('is_culinary', 'true');
      if (isCulinary === 'false') fd.append('is_culinary', 'false');
      const res = await fetch('/api/v2/identify/vision-debug', { method: 'POST', body: fd });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json() as VisionDebugResponse;
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col gap-6">
      <p className="text-muted text-sm">
        Upload a knife photo to see exactly what the vision model receives. Add filters to test how they affect candidate selection.
      </p>

      {/* Upload + filters */}
      <div className="flex items-end gap-4 flex-wrap">
        <div>
          <label className="block text-muted text-xs mb-1.5">Photo</label>
          <input type="file" accept="image/*" onChange={handleFileChange}
            className="text-sm text-ink file:mr-3 file:py-1.5 file:px-3 file:rounded-lg file:border file:border-border file:bg-card file:text-ink file:text-xs file:cursor-pointer" />
        </div>
        <div>
          <label className="block text-muted text-xs mb-1.5">Handle material</label>
          <select value={handleMaterial} onChange={e => setHandleMaterial(e.target.value)}
            className="px-2 py-1.5 bg-card border border-border rounded-lg text-xs text-ink">
            <option value="">Any</option>
            <option value="G-10">G-10</option>
            <option value="Paracord">Paracord</option>
            <option value="Burled Carbon Fiber">Burled Carbon Fiber</option>
            <option value="Desert Ironwood">Desert Ironwood</option>
            <option value="Desert Ironwood Burl">Desert Ironwood Burl</option>
          </select>
        </div>
        <div>
          <label className="block text-muted text-xs mb-1.5">Culinary?</label>
          <select value={isCulinary} onChange={e => setIsCulinary(e.target.value)}
            className="px-2 py-1.5 bg-card border border-border rounded-lg text-xs text-ink">
            <option value="">Any</option>
            <option value="true">Yes</option>
            <option value="false">No</option>
          </select>
        </div>
        <button onClick={handleSubmit} disabled={!file || loading}
          className="py-1.5 px-4 rounded-lg bg-gold text-black text-sm font-semibold hover:bg-gold-bright disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
          {loading ? 'Processing…' : 'Analyze'}
        </button>
      </div>

      {error && (
        <div className="px-4 py-3 rounded-lg bg-red-950/40 border border-red-800/50 text-red-300 text-sm">{error}</div>
      )}

      {result && (
        <div className="flex flex-col gap-6">
          {/* Pipeline info */}
          <div className="flex flex-wrap gap-4 text-xs">
            <div className="text-muted">Vision model: <span className="text-ink font-mono">{result.vision_model}</span></div>
            <div className="text-muted">Families: <span className="text-ink">{result.families_total - result.families_eliminated} remaining</span> / <span className="text-red-400">{result.families_eliminated} eliminated</span></div>
            <div className="text-muted">Filters: <span className="text-ink font-mono">{JSON.stringify(result.filters)}</span></div>
          </div>

          {/* User images side by side */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-muted text-[10px] uppercase tracking-wider mb-1.5">Original Upload</div>
              <div className="rounded-xl overflow-hidden bg-border/20 flex items-center justify-center" style={{ minHeight: 200 }}>
                <img src={`data:image/jpeg;base64,${result.original_image}`} alt="Original" className="max-w-full max-h-[400px] object-contain" />
              </div>
            </div>
            <div>
              <div className="text-muted text-[10px] uppercase tracking-wider mb-1.5">After Background Removal</div>
              <div className="rounded-xl overflow-hidden bg-border/20 flex items-center justify-center" style={{ minHeight: 200 }}>
                <img src={`data:image/png;base64,${result.clean_image}`} alt="Cleaned" className="max-w-full max-h-[400px] object-contain" />
              </div>
            </div>
          </div>

          {/* Candidates */}
          <div>
            <div className="text-muted text-xs uppercase tracking-wider mb-3">Candidate Reference Images Sent to Vision Model</div>
            <div className="grid grid-cols-5 gap-3">
              {result.candidates.map((c, i) => (
                <div key={i} className="rounded-xl border border-border bg-card p-3">
                  <div className="text-ink text-xs font-semibold truncate mb-0.5">{c.name}</div>
                  <div className="text-muted text-[10px]">{c.family} · {c.form || '?'}</div>
                  <div className="text-muted text-[10px] mb-1">{c.handle_type || '?'} · score: <span className="text-gold">{c.score}</span></div>
                  {c.reasons.length > 0 && (
                    <div className="text-[10px] text-gold/70 truncate mb-1">{c.reasons.join(', ')}</div>
                  )}
                  {c.silhouette_b64 && (
                    <div className="mb-2">
                      <div className="text-muted text-[10px] mb-0.5">Silhouette</div>
                      <img src={`data:image/png;base64,${c.silhouette_b64}`} alt="Silhouette" className="w-full h-16 object-contain bg-white rounded" />
                    </div>
                  )}
                  <div>
                    <div className="text-muted text-[10px] mb-0.5">Reference Photo</div>
                    <img src={`data:image/jpeg;base64,${c.image_b64}`} alt={c.name} className="w-full h-32 object-contain bg-border/20 rounded" />
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* User photo feature extraction */}
          {result.user_profile && (
            <div>
              <div className="text-muted text-xs uppercase tracking-wider mb-2">Extracted Features (from user photo)</div>
              <div className="grid grid-cols-4 gap-2">
                {Object.entries(result.user_profile).map(([k, v]) => (
                  <div key={k} className="bg-card border border-border rounded-lg px-3 py-2">
                    <div className="text-muted text-[10px]">{k}</div>
                    <div className="text-ink text-xs font-mono">{String(v)}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Pipeline results */}
          {result.pipeline_results && result.pipeline_results.length > 0 && (
            <div>
              <div className="text-muted text-xs uppercase tracking-wider mb-2">
                Pipeline Results {result.vision_used && <span className="text-gold">(vision used)</span>}
              </div>
              <div className="overflow-x-auto rounded-lg border border-border">
                <table className="w-full text-xs border-collapse">
                  <thead>
                    <tr className="bg-border/20 text-left">
                      <th className="px-3 py-2 text-muted font-medium">#</th>
                      <th className="px-3 py-2 text-muted font-medium">Model</th>
                      <th className="px-3 py-2 text-muted font-medium">Family</th>
                      <th className="px-3 py-2 text-muted font-medium">Form</th>
                      <th className="px-3 py-2 text-muted font-medium text-right">Score</th>
                      <th className="px-3 py-2 text-muted font-medium">Vision</th>
                      <th className="px-3 py-2 text-muted font-medium">Reasons</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.pipeline_results.map((r: PipelineResult, i: number) => (
                      <tr key={i} className="border-t border-border/50">
                        <td className="px-3 py-2 text-muted">{i + 1}</td>
                        <td className="px-3 py-2 text-ink font-medium">{r.name}</td>
                        <td className="px-3 py-2 text-muted">{r.family}</td>
                        <td className="px-3 py-2 text-muted">{r.form || '—'}</td>
                        <td className="px-3 py-2 text-gold text-right font-mono">{r.score}</td>
                        <td className="px-3 py-2">
                          {r.vision_match && (
                            <span className={`px-1.5 py-0.5 rounded text-[10px] font-semibold ${
                              r.vision_match === 'STRONG' ? 'bg-green-900/30 text-green-300' :
                              r.vision_match === 'POSSIBLE' ? 'bg-yellow-900/30 text-yellow-300' :
                              'bg-red-900/30 text-red-300'
                            }`}>{r.vision_match}</span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-muted text-[10px]">{r.reasons.join(', ')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Admin page ────────────────────────────────────────────────────────────────

export default function Admin() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true'
  );
  const [options, setOptions] = useState<OptionsMap>({});
  const [loading, setLoading] = useState(true);
  const [activeSection, setActiveSection] = useState<'options' | 'images' | 'access' | 'quicktag' | 'vision'>('options');

  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent<{ collapsed: boolean }>;
      setSidebarCollapsed(ce.detail.collapsed);
    };
    window.addEventListener('mkc-sidebar-toggle', handler);
    return () => window.removeEventListener('mkc-sidebar-toggle', handler);
  }, []);

  const fetchOptions = useCallback(async () => {
    const res = await fetch('/api/v2/options');
    if (!res.ok) throw new Error('Failed to load options');
    const data = await res.json() as OptionsMap;
    setOptions(data);
  }, []);

  useEffect(() => {
    fetchOptions().finally(() => setLoading(false));
  }, [fetchOptions]);

  const handleAdd = async (key: string, name: string) => {
    const res = await fetch(`/api/v2/options/${key}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({})) as { detail?: unknown };
      const detail = body.detail;
      const msg = typeof detail === 'string'
        ? detail
        : Array.isArray(detail)
          ? (detail as { msg?: string }[]).map(d => d.msg ?? String(d)).join('; ')
          : 'Failed to add option';
      throw new Error(msg);
    }
    const created = await res.json() as { id: number };
    setOptions(prev => ({
      ...prev,
      [key]: [...(prev[key] ?? []), { id: created.id, name }].sort((a, b) =>
        a.name.localeCompare(b.name, undefined, { sensitivity: 'base' })
      ),
    }));
  };

  const handleDelete = async (key: string, id: number) => {
    const res = await fetch(`/api/v2/options/${key}/${id}`, { method: 'DELETE' });
    if (!res.ok) {
      const body = await res.json().catch(() => ({})) as { detail?: unknown };
      const detail = body.detail;
      const msg = typeof detail === 'string'
        ? detail
        : Array.isArray(detail)
          ? (detail as { msg?: string }[]).map(d => d.msg ?? String(d)).join('; ')
          : 'Failed to delete option';
      throw new Error(msg);
    }
    setOptions(prev => ({
      ...prev,
      [key]: (prev[key] ?? []).filter(item => item.id !== id),
    }));
  };

  const marginClass = sidebarCollapsed ? 'md:ml-16' : 'md:ml-56';

  return (
    <div className="min-h-screen bg-surface text-ink">
      <Sidebar />

      <div className={`${marginClass} transition-[margin] duration-200`}>
      {/* Header */}
      <header className="border-b border-border px-6 py-4 pl-14 md:pl-6">
        <h1 className="text-lg font-bold text-ink tracking-wide">Admin</h1>
        <p className="text-muted text-xs mt-0.5">Manage dropdowns, images, and access settings</p>
      </header>

      {/* Nav tabs */}
      <div className="border-b border-border px-6">
        <nav className="flex gap-6">
          {([['options', 'Dropdown Options'], ['images', 'Image Audit'], ['access', 'Access Log'], ['quicktag', 'Quick Tag'], ['vision', 'Vision Debug']] as const).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setActiveSection(key)}
              className={`py-3 text-sm font-medium border-b-2 transition-colors ${
                activeSection === key
                  ? 'border-gold text-gold'
                  : 'border-transparent text-muted hover:text-ink'
              }`}
            >
              {label}
            </button>
          ))}
        </nav>
      </div>

      {/* Body */}
      <main className="px-6 py-6 max-w-6xl mx-auto">
        {activeSection === 'options' && (
          <>
            <p className="text-muted text-sm mb-6">
              These lists populate the dropdowns in the inventory and catalog forms. Hover a pill and click × to remove an unused option.
            </p>
            {loading ? (
              <div className="text-muted text-sm">Loading…</div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                {OPTION_TYPES.map(({ key, label }) => (
                  <OptionSection
                    key={key}
                    optionKey={key}
                    label={label}
                    items={options[key] ?? []}
                    onAdd={handleAdd}
                    onDelete={handleDelete}
                  />
                ))}
              </div>
            )}
          </>
        )}

        {activeSection === 'images' && (
          <ImageAudit />
        )}

        {activeSection === 'access' && (
          <AccessLog />
        )}

        {activeSection === 'quicktag' && (
          <QuickTag options={options} />
        )}

        {activeSection === 'vision' && (
          <VisionDebug />
        )}

      </main>
      </div>
    </div>
  );
}
