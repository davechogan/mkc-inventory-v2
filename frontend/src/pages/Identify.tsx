import { useState, useEffect, useCallback } from 'react';
import { Sidebar } from '../components/Sidebar';

// ── Types ─────────────────────────────────────────────────────────────────────

interface IdentifyResult {
  id: number;
  name: string;
  family: string | null;
  category: string | null;
  catalog_line: string | null;
  record_type: string | null;
  catalog_status: string | null;
  has_identifier_image: boolean;
  catalog_blurb: string | null;
  default_blade_length: number | null;
  default_steel: string | null;
  default_blade_finish: string | null;
  default_blade_color: string | null;
  is_collab: boolean;
  collaboration_name: string | null;
  list_status: string;
  score: number;
  reasons: string[];
}

interface IdentifyForm {
  q: string;
  family: string;
  steel: string;
  finish: string;
  blade_color: string;
  catalog_line: string;
  blade_length: string;
  is_collab: boolean | null;
  is_tactical: boolean | null;
  include_archived: boolean;
}

interface OptionItem {
  id: number;
  name: string;
}

interface Options {
  'blade-steels': OptionItem[];
  'blade-finishes': OptionItem[];
  'blade-colors': OptionItem[];
  'blade-families': OptionItem[];
}

// ── API helpers ───────────────────────────────────────────────────────────────

async function fetchOptions(): Promise<Options> {
  const res = await fetch('/api/v2/options');
  if (!res.ok) throw new Error(`Failed to load options: ${res.status}`);
  return res.json();
}

async function identify(form: IdentifyForm): Promise<IdentifyResult[]> {
  const body: Record<string, unknown> = {
    include_archived: form.include_archived,
  };
  if (form.q.trim()) body.q = form.q.trim();
  if (form.family) body.family = form.family;
  if (form.steel) body.steel = form.steel;
  if (form.finish) body.finish = form.finish;
  if (form.blade_color) body.blade_color = form.blade_color;
  if (form.catalog_line) body.catalog_line = form.catalog_line;
  if (form.blade_length) {
    const len = parseFloat(form.blade_length);
    if (!isNaN(len)) body.blade_length = len;
  }
  if (form.is_collab !== null) body.is_collab = form.is_collab;
  if (form.is_tactical !== null) body.is_tactical = form.is_tactical;

  const res = await fetch('/api/v2/identify', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Identify failed: ${res.status}`);
  const data = await res.json();
  return data.results as IdentifyResult[];
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ScoreBadge({ score }: { score: number }) {
  const color =
    score >= 40 ? 'bg-gold/20 text-gold border-gold/30' :
    score >= 20 ? 'bg-blue-900/30 text-blue-300 border-blue-700/40' :
    'bg-border/40 text-muted border-border';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold border ${color}`}>
      {score} pts
    </span>
  );
}

function ResultCard({
  result,
  selected,
  onClick,
}: {
  result: IdentifyResult;
  selected: boolean;
  onClick: () => void;
}) {
  const imgSrc = result.has_identifier_image
    ? `/api/v2/models/${result.id}/image`
    : null;

  return (
    <button
      onClick={onClick}
      className={`w-full text-left flex items-start gap-3 px-4 py-3 rounded-xl border transition-colors ${
        selected
          ? 'border-gold/50 bg-gold/5'
          : 'border-border bg-card hover:border-border/70 hover:bg-border/10'
      }`}
    >
      {/* Thumbnail */}
      <div className="w-14 h-14 flex-shrink-0 rounded-lg overflow-hidden bg-border/20 flex items-center justify-center">
        {imgSrc ? (
          <img src={imgSrc} alt={result.name} className="w-full h-full object-cover" />
        ) : (
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" className="text-muted/40">
            <path d="M14.5 10c-.83 0-1.5-.67-1.5-1.5v-5c0-.83.67-1.5 1.5-1.5s1.5.67 1.5 1.5v5c0 .83-.67 1.5-1.5 1.5z" />
            <path d="M20.5 10H19V8.5c0-.83.67-1.5 1.5-1.5s1.5.67 1.5 1.5-.67 1.5-1.5 1.5z" />
            <path d="M9 18v-5c0-.83-.67-1.5-1.5-1.5S6 12.17 6 13v5" />
          </svg>
        )}
      </div>

      {/* Info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-start justify-between gap-2">
          <span className="text-ink text-sm font-semibold leading-tight line-clamp-2">{result.name}</span>
          <ScoreBadge score={result.score} />
        </div>
        <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-1">
          {result.family && (
            <span className="text-muted text-xs">{result.family}</span>
          )}
          {result.default_steel && (
            <span className="text-muted text-xs">{result.default_steel}</span>
          )}
          {result.default_blade_length && (
            <span className="text-muted text-xs">{result.default_blade_length}&Prime; blade</span>
          )}
        </div>
        {result.reasons.length > 0 && (
          <div className="mt-1 text-xs text-gold/70 truncate">
            {result.reasons[0]}
          </div>
        )}
      </div>
    </button>
  );
}

function ResultDetail({ result }: { result: IdentifyResult }) {
  const imgSrc = result.has_identifier_image
    ? `/api/v2/models/${result.id}/image`
    : null;

  return (
    <div className="flex flex-col gap-4">
      {/* Image */}
      {imgSrc && (
        <div className="w-full rounded-xl overflow-hidden bg-border/20 aspect-[4/3]">
          <img src={imgSrc} alt={result.name} className="w-full h-full object-contain" />
        </div>
      )}

      {/* Name + score */}
      <div>
        <div className="flex items-start justify-between gap-2">
          <h3 className="text-ink text-base font-bold leading-tight">{result.name}</h3>
          <ScoreBadge score={result.score} />
        </div>
        {result.is_collab && result.collaboration_name && (
          <div className="text-gold text-xs mt-0.5">Collab: {result.collaboration_name}</div>
        )}
      </div>

      {/* Specs grid */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
        {[
          ['Family', result.family],
          ['Category', result.category],
          ['Series', result.catalog_line],
          ['Steel', result.default_steel],
          ['Finish', result.default_blade_finish],
          ['Blade Color', result.default_blade_color],
          ['Blade Length', result.default_blade_length ? `${result.default_blade_length}"` : null],
          ['Status', result.list_status],
        ]
          .filter(([, v]) => v)
          .map(([label, value]) => (
            <div key={label as string}>
              <div className="text-muted text-xs">{label}</div>
              <div className="text-ink text-sm">{value}</div>
            </div>
          ))}
      </div>

      {/* Reasons */}
      {result.reasons.length > 0 && (
        <div>
          <div className="text-muted text-xs mb-1.5">Match reasons</div>
          <ul className="flex flex-col gap-1">
            {result.reasons.map((r, i) => (
              <li key={i} className="flex items-start gap-1.5 text-xs text-ink/80">
                <span className="text-gold mt-0.5 flex-shrink-0">›</span>
                {r}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Blurb */}
      {result.catalog_blurb && (
        <div className="text-xs text-muted/80 leading-relaxed border-t border-border pt-3">
          {result.catalog_blurb}
        </div>
      )}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

const emptyForm: IdentifyForm = {
  q: '',
  family: '',
  steel: '',
  finish: '',
  blade_color: '',
  catalog_line: '',
  blade_length: '',
  is_collab: null,
  is_tactical: null,
  include_archived: false,
};

const SIDEBAR_KEY = 'mkc_sidebar_collapsed';

export default function Identify() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true'
  );

  const [form, setForm] = useState<IdentifyForm>(emptyForm);
  const [options, setOptions] = useState<Options | null>(null);
  const [results, setResults] = useState<IdentifyResult[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<IdentifyResult | null>(null);

  // Load options on mount
  useEffect(() => {
    fetchOptions()
      .then(setOptions)
      .catch(() => { /* options are optional — dropdowns fall back to text */ });
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

  const hasAnyInput = Object.entries(form).some(([k, v]) => {
    if (k === 'include_archived') return false;
    if (v === null || v === false || v === '') return false;
    return true;
  });

  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    if (!hasAnyInput) return;
    setLoading(true);
    setError(null);
    setSelected(null);
    try {
      const res = await identify(form);
      setResults(res);
      if (res.length > 0) setSelected(res[0]);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [form, hasAnyInput]);

  const handleReset = () => {
    setForm(emptyForm);
    setResults(null);
    setSelected(null);
    setError(null);
  };

  const marginClass = sidebarCollapsed ? 'ml-16' : 'ml-56';

  return (
    <div className="min-h-screen bg-surface">
      <Sidebar />

      <main className={`${marginClass} transition-[margin] duration-200 flex flex-col min-h-screen`}>
        {/* Top bar */}
        <div className="flex items-center justify-between px-8 py-4 border-b border-border flex-shrink-0">
          <h1 className="text-ink text-xl font-bold">Identify a Knife</h1>
        </div>

        {/* Body */}
        <div className="flex flex-1 overflow-hidden">
          {/* ── Left: form ── */}
          <div className="w-80 flex-shrink-0 border-r border-border overflow-y-auto">
            <form onSubmit={handleSubmit} className="p-6 flex flex-col gap-5">
              {/* Keyword search */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Keyword search</label>
                <input
                  type="text"
                  placeholder="e.g. Battle Goat, fillet, VIP…"
                  value={form.q}
                  onChange={(e) => setForm((f) => ({ ...f, q: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors"
                />
              </div>

              {/* Family */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Family / Use</label>
                <select
                  value={form.family}
                  onChange={(e) => setForm((f) => ({ ...f, family: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any family</option>
                  {options?.['blade-families'].map((o) => (
                    <option key={o.id} value={o.name}>{o.name}</option>
                  ))}
                </select>
              </div>

              {/* Steel */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Blade Steel</label>
                <select
                  value={form.steel}
                  onChange={(e) => setForm((f) => ({ ...f, steel: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any steel</option>
                  {options?.['blade-steels'].map((o) => (
                    <option key={o.id} value={o.name}>{o.name}</option>
                  ))}
                </select>
              </div>

              {/* Finish */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Blade Finish</label>
                <select
                  value={form.finish}
                  onChange={(e) => setForm((f) => ({ ...f, finish: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any finish</option>
                  {options?.['blade-finishes'].map((o) => (
                    <option key={o.id} value={o.name}>{o.name}</option>
                  ))}
                </select>
              </div>

              {/* Blade color */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Blade Color</label>
                <select
                  value={form.blade_color}
                  onChange={(e) => setForm((f) => ({ ...f, blade_color: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any color</option>
                  {options?.['blade-colors'].map((o) => (
                    <option key={o.id} value={o.name}>{o.name}</option>
                  ))}
                </select>
              </div>

              {/* Blade length */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Blade Length (inches)</label>
                <input
                  type="number"
                  step="0.1"
                  min="0"
                  placeholder="e.g. 4.5"
                  value={form.blade_length}
                  onChange={(e) => setForm((f) => ({ ...f, blade_length: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors"
                />
              </div>

              {/* Catalog line */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Catalog Line</label>
                <select
                  value={form.catalog_line}
                  onChange={(e) => setForm((f) => ({ ...f, catalog_line: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any line</option>
                  <option value="standard">Standard</option>
                  <option value="VIP">VIP</option>
                  <option value="Traditions">Traditions</option>
                </select>
              </div>

              {/* Toggles */}
              <div className="flex flex-col gap-2.5">
                <TriToggle
                  label="Collaboration knife"
                  value={form.is_collab}
                  onChange={(v) => setForm((f) => ({ ...f, is_collab: v }))}
                />
                <TriToggle
                  label="Tactical"
                  value={form.is_tactical}
                  onChange={(v) => setForm((f) => ({ ...f, is_tactical: v }))}
                />
                <label className="flex items-center gap-2.5 cursor-pointer select-none">
                  <input
                    type="checkbox"
                    checked={form.include_archived}
                    onChange={(e) => setForm((f) => ({ ...f, include_archived: e.target.checked }))}
                    className="w-4 h-4 rounded accent-gold"
                  />
                  <span className="text-muted text-xs">Include archived models</span>
                </label>
              </div>

              {/* Actions */}
              <div className="flex gap-2 pt-1">
                <button
                  type="submit"
                  disabled={!hasAnyInput || loading}
                  className="flex-1 py-2 px-4 rounded-lg bg-gold text-black text-sm font-semibold hover:bg-gold-bright disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  {loading ? 'Searching…' : 'Identify'}
                </button>
                <button
                  type="button"
                  onClick={handleReset}
                  className="py-2 px-3 rounded-lg border border-border text-muted text-sm hover:text-ink hover:border-border/70 transition-colors"
                >
                  Reset
                </button>
              </div>
            </form>
          </div>

          {/* ── Middle: results list ── */}
          <div className="flex-1 overflow-y-auto border-r border-border">
            {error && (
              <div className="m-6 px-4 py-3 rounded-lg bg-red-950/40 border border-red-800/50 text-red-300 text-sm">
                {error}
              </div>
            )}

            {!results && !loading && !error && (
              <div className="h-full flex flex-col items-center justify-center gap-3 text-center px-8 py-16">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" className="text-muted/30">
                  <circle cx="11" cy="11" r="8" />
                  <line x1="21" y1="21" x2="16.65" y2="16.65" />
                </svg>
                <p className="text-muted text-sm">Fill in one or more clues and click <strong className="text-ink">Identify</strong> to find matching models.</p>
              </div>
            )}

            {loading && (
              <div className="p-6 flex flex-col gap-3">
                {Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="skeleton h-20 rounded-xl" />
                ))}
              </div>
            )}

            {results && !loading && results.length === 0 && (
              <div className="h-full flex flex-col items-center justify-center gap-2 px-8 py-16">
                <p className="text-muted text-sm">No matching models found. Try broadening your clues.</p>
              </div>
            )}

            {results && !loading && results.length > 0 && (
              <div className="p-4 flex flex-col gap-2">
                <div className="text-muted text-xs px-1 mb-1">
                  {results.length} match{results.length !== 1 ? 'es' : ''} — ranked by score
                </div>
                {results.map((r) => (
                  <ResultCard
                    key={r.id}
                    result={r}
                    selected={selected?.id === r.id}
                    onClick={() => setSelected(r)}
                  />
                ))}
              </div>
            )}
          </div>

          {/* ── Right: detail pane ── */}
          <div className="w-80 flex-shrink-0 overflow-y-auto">
            {selected ? (
              <div className="p-6">
                <ResultDetail result={selected} />
              </div>
            ) : (
              <div className="h-full flex items-center justify-center px-6 py-16">
                <p className="text-muted text-xs text-center">Select a result to see details.</p>
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

// ── TriToggle ─────────────────────────────────────────────────────────────────

function TriToggle({
  label,
  value,
  onChange,
}: {
  label: string;
  value: boolean | null;
  onChange: (v: boolean | null) => void;
}) {
  const opts: Array<{ label: string; value: boolean | null }> = [
    { label: 'Any', value: null },
    { label: 'Yes', value: true },
    { label: 'No', value: false },
  ];

  return (
    <div className="flex items-center justify-between gap-2">
      <span className="text-muted text-xs flex-1">{label}</span>
      <div className="flex rounded-lg border border-border overflow-hidden text-xs">
        {opts.map((o) => (
          <button
            key={String(o.value)}
            type="button"
            onClick={() => onChange(o.value)}
            className={`px-2.5 py-1 transition-colors ${
              value === o.value
                ? 'bg-gold/20 text-gold'
                : 'text-muted hover:text-ink hover:bg-border/30'
            }`}
          >
            {o.label}
          </button>
        ))}
      </div>
    </div>
  );
}
