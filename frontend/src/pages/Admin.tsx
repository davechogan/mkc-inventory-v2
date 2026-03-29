import { useState, useEffect, useCallback } from 'react';

// ── Types ─────────────────────────────────────────────────────────────────────

interface OptionItem {
  id: number;
  name: string;
}

type OptionsMap = Record<string, OptionItem[]>;

const OPTION_TYPES: { key: string; label: string }[] = [
  { key: 'blade-steels',      label: 'Blade Steels' },
  { key: 'blade-finishes',    label: 'Blade Finishes' },
  { key: 'blade-colors',      label: 'Blade Colors' },
  { key: 'handle-colors',     label: 'Handle Colors' },
  { key: 'conditions',        label: 'Conditions' },
  { key: 'blade-types',       label: 'Blade Types' },
  { key: 'categories',        label: 'Categories' },
  { key: 'blade-families',    label: 'Blade Families' },
  { key: 'primary-use-cases', label: 'Primary Use Cases' },
  { key: 'handle-types',      label: 'Handle Types' },
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

// ── Admin page ────────────────────────────────────────────────────────────────

export default function Admin() {
  const [options, setOptions] = useState<OptionsMap>({});
  const [loading, setLoading] = useState(true);
  const [activeSection, setActiveSection] = useState<'options' | 'catalog'>('options');

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
          {(['options', 'catalog'] as const).map(section => (
            <button
              key={section}
              onClick={() => setActiveSection(section)}
              className={`py-3 text-sm font-medium border-b-2 transition-colors capitalize ${
                activeSection === section
                  ? 'border-gold text-gold'
                  : 'border-transparent text-muted hover:text-ink'
              }`}
            >
              {section === 'options' ? 'Dropdown Options' : 'Catalog'}
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

        {activeSection === 'catalog' && (
          <div className="text-muted text-sm py-12 text-center">
            Catalog management coming soon.
          </div>
        )}
      </main>
    </div>
  );
}
