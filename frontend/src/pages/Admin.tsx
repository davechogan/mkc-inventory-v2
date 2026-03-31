import { useState, useEffect, useCallback, useRef } from 'react';

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

// ── Admin page ────────────────────────────────────────────────────────────────

export default function Admin() {
  const [options, setOptions] = useState<OptionsMap>({});
  const [loading, setLoading] = useState(true);
  const [activeSection, setActiveSection] = useState<'options' | 'images' | 'access' | 'catalog'>('options');

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

  return (
    <div className="min-h-screen bg-surface text-ink">
      {/* Header */}
      <header className="border-b border-border px-6 py-4 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold text-ink tracking-wide">Admin</h1>
          <p className="text-muted text-xs mt-0.5">Hidden — manage dropdowns and catalog data</p>
        </div>
        <a href="/" className="text-muted text-sm hover:text-gold transition-colors">← Collection</a>
      </header>

      {/* Nav tabs */}
      <div className="border-b border-border px-6">
        <nav className="flex gap-6">
          {([['options', 'Dropdown Options'], ['images', 'Image Audit'], ['access', 'Access Log'], ['catalog', 'Catalog']] as const).map(([key, label]) => (
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

        {activeSection === 'catalog' && (
          <div className="text-muted text-sm py-12 text-center">
            Catalog management coming soon.
          </div>
        )}
      </main>
    </div>
  );
}
