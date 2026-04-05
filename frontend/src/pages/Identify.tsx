import { useState, useEffect, useCallback, useRef } from 'react';
import { Sidebar } from '../components/Sidebar';

// ── Types ─────────────────────────────────────────────────────────────────────

interface IdentifyResult {
  id: number;
  name: string;
  family: string | null;
  category: string | null;
  form: string | null;
  catalog_line: string | null;
  handle_type: string | null;
  has_identifier_image: boolean;
  default_blade_length: number | null;
  default_steel: string | null;
  default_blade_finish: string | null;
  is_collab: boolean;
  collaboration_name: string | null;
  score: number;
  reasons: string[];
  vision_match?: string;
  vision_reason?: string;
}

interface IdentifyResponse {
  results: IdentifyResult[];
  families_eliminated: number;
  families_remaining: number;
  vision_used: boolean;
}

interface OptionItem {
  id: number;
  name: string;
}

interface Options {
  'handle-types': OptionItem[];
  'handle-colors': OptionItem[];
  'blade-colors': OptionItem[];
  'blade-families': OptionItem[];
  [key: string]: OptionItem[];
}

interface FormState {
  handle_material: string;
  handle_color: string;
  blade_color: string;
  is_culinary: boolean | null;
  blade_length_bin: number | null;
  blade_forms: string[];  // multi-select
  use_vision: boolean;
}

// ── Constants ─────────────────────────────────────────────────────────────────

const BLADE_LENGTH_BINS = [
  { value: 1, label: 'Shorter than an index finger', range: '< 3"' },
  { value: 2, label: 'About one index finger', range: '3"–4.5"' },
  { value: 3, label: 'About two index fingers', range: '4.5"–7"' },
  { value: 4, label: 'Longer than two index fingers', range: '> 7"' },
];

const SIDEBAR_KEY = 'mkc_sidebar_collapsed';

const emptyForm: FormState = {
  handle_material: '',
  handle_color: '',
  blade_color: '',
  is_culinary: null,
  blade_length_bin: null,
  blade_forms: [],
  use_vision: true,
};

// ── API helpers ───────────────────────────────────────────────────────────────

async function fetchOptions(): Promise<Options> {
  const res = await fetch('/api/v2/options');
  if (!res.ok) throw new Error(`Failed to load options: ${res.status}`);
  return res.json();
}

async function fetchForms(): Promise<string[]> {
  // Get distinct form names from all models
  const searchRes = await fetch('/api/v2/models/search?limit=200');
  if (!searchRes.ok) return [];
  const models = await searchRes.json() as { form_name: string | null }[];
  const forms = new Set<string>();
  for (const m of models) {
    if (m.form_name) forms.add(m.form_name);
  }
  return [...forms].sort();
}

async function identifyByImage(
  imageFile: File | null,
  form: FormState,
): Promise<IdentifyResponse> {
  const fd = new FormData();
  if (imageFile) fd.append('image', imageFile);
  if (form.handle_material) fd.append('handle_material', form.handle_material);
  if (form.handle_color) fd.append('handle_color', form.handle_color);
  if (form.blade_color) fd.append('blade_color', form.blade_color);
  if (form.is_culinary !== null) fd.append('is_culinary', String(form.is_culinary));
  if (form.blade_length_bin !== null) fd.append('blade_length_bin', String(form.blade_length_bin));
  if (form.blade_forms.length > 0) fd.append('blade_forms', form.blade_forms.join(','));
  fd.append('use_vision', String(form.use_vision && imageFile !== null));

  const res = await fetch('/api/v2/identify/image', { method: 'POST', body: fd });
  if (!res.ok) throw new Error(`Identify failed: ${res.status}`);
  return res.json();
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ScoreBadge({ score }: { score: number }) {
  const color =
    score >= 60 ? 'bg-gold/20 text-gold border-gold/30' :
    score >= 30 ? 'bg-blue-900/30 text-blue-300 border-blue-700/40' :
    'bg-border/40 text-muted border-border';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold border ${color}`}>
      {score} pts
    </span>
  );
}

function VisionBadge({ match }: { match: string }) {
  const color =
    match === 'STRONG' ? 'bg-green-900/30 text-green-300 border-green-700/40' :
    match === 'POSSIBLE' ? 'bg-yellow-900/30 text-yellow-300 border-yellow-700/40' :
    'bg-red-900/30 text-red-300 border-red-700/40';
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold border ${color}`}>
      {match}
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
      <div className="w-14 h-14 flex-shrink-0 rounded-lg overflow-hidden bg-border/20 flex items-center justify-center">
        {imgSrc ? (
          <img src={imgSrc} alt={result.name} className="w-full h-full object-cover" />
        ) : (
          <div className="w-6 h-6 text-muted/30">?</div>
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-start justify-between gap-2">
          <span className="text-ink text-sm font-semibold leading-tight line-clamp-2">{result.name}</span>
          <div className="flex gap-1.5 flex-shrink-0">
            {result.vision_match && <VisionBadge match={result.vision_match} />}
            <ScoreBadge score={result.score} />
          </div>
        </div>
        <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-1">
          {result.family && <span className="text-muted text-xs">{result.family}</span>}
          {result.handle_type && <span className="text-muted text-xs">{result.handle_type}</span>}
          {result.default_blade_length && (
            <span className="text-muted text-xs">{result.default_blade_length}&Prime;</span>
          )}
        </div>
        {result.reasons.length > 0 && (
          <div className="mt-1 text-xs text-gold/70 truncate">{result.reasons[0]}</div>
        )}
      </div>
    </button>
  );
}

function ResultDetail({ result, userImage }: { result: IdentifyResult; userImage: string | null }) {
  const imgSrc = result.has_identifier_image
    ? `/api/v2/models/${result.id}/image`
    : null;

  return (
    <div className="flex flex-col gap-4">
      {/* Side-by-side comparison */}
      {userImage && imgSrc && (
        <div className="grid grid-cols-2 gap-2">
          <div>
            <div className="text-muted text-[10px] uppercase tracking-wider mb-1">Your knife</div>
            <div className="rounded-lg overflow-hidden bg-border/20 aspect-square">
              <img src={userImage} alt="Your knife" className="w-full h-full object-contain" />
            </div>
          </div>
          <div>
            <div className="text-muted text-[10px] uppercase tracking-wider mb-1">Reference</div>
            <div className="rounded-lg overflow-hidden bg-border/20 aspect-square">
              <img src={imgSrc} alt={result.name} className="w-full h-full object-contain" />
            </div>
          </div>
        </div>
      )}

      {/* Single image fallback */}
      {!userImage && imgSrc && (
        <div className="w-full rounded-xl overflow-hidden bg-border/20 aspect-[4/3]">
          <img src={imgSrc} alt={result.name} className="w-full h-full object-contain" />
        </div>
      )}

      <div>
        <div className="flex items-start justify-between gap-2">
          <h3 className="text-ink text-base font-bold leading-tight">{result.name}</h3>
          <div className="flex gap-1.5">
            {result.vision_match && <VisionBadge match={result.vision_match} />}
            <ScoreBadge score={result.score} />
          </div>
        </div>
        {result.is_collab && result.collaboration_name && (
          <div className="text-gold text-xs mt-0.5">Collab: {result.collaboration_name}</div>
        )}
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
        {[
          ['Family', result.family],
          ['Type', result.category],
          ['Form', result.form],
          ['Series', result.catalog_line],
          ['Handle', result.handle_type],
          ['Steel', result.default_steel],
          ['Finish', result.default_blade_finish],
          ['Blade', result.default_blade_length ? `${result.default_blade_length}"` : null],
        ]
          .filter(([, v]) => v)
          .map(([label, value]) => (
            <div key={label as string}>
              <div className="text-muted text-xs">{label}</div>
              <div className="text-ink text-sm">{value}</div>
            </div>
          ))}
      </div>

      {/* Vision reasoning */}
      {result.vision_reason && (
        <div className="border-t border-border pt-3">
          <div className="text-muted text-xs mb-1">Vision analysis</div>
          <p className="text-xs text-ink/80">{result.vision_reason}</p>
        </div>
      )}

      {/* Match reasons */}
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
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function Identify() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true'
  );
  const [form, setForm] = useState<FormState>(emptyForm);
  const [options, setOptions] = useState<Options | null>(null);
  const [forms, setForms] = useState<string[]>([]);
  const [imageFile, setImageFile] = useState<File | null>(null);
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const [results, setResults] = useState<IdentifyResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<IdentifyResult | null>(null);
  const [showAll, setShowAll] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    fetchOptions().then(setOptions).catch(() => {});
    fetchForms().then(setForms).catch(() => {});
  }, []);

  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent<{ collapsed: boolean }>;
      setSidebarCollapsed(ce.detail.collapsed);
    };
    window.addEventListener('mkc-sidebar-toggle', handler);
    return () => window.removeEventListener('mkc-sidebar-toggle', handler);
  }, []);

  const handleImageChange = useCallback((file: File) => {
    setImageFile(file);
    const reader = new FileReader();
    reader.onload = () => setImagePreview(reader.result as string);
    reader.readAsDataURL(file);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file && file.type.startsWith('image/')) handleImageChange(file);
  }, [handleImageChange]);

  const hasAnyInput = imageFile !== null ||
    form.handle_material || form.handle_color || form.blade_color ||
    form.is_culinary !== null || form.blade_length_bin !== null ||
    form.blade_forms.length > 0;

  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    if (!hasAnyInput) return;
    setLoading(true);
    setError(null);
    setSelected(null);
    setShowAll(false);
    try {
      const res = await identifyByImage(imageFile, form);
      setResults(res);
      if (res.results.length > 0) setSelected(res.results[0]);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [form, imageFile, hasAnyInput]);

  const handleReset = () => {
    setForm(emptyForm);
    setImageFile(null);
    setImagePreview(null);
    setResults(null);
    setSelected(null);
    setError(null);
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const toggleForm = (formName: string) => {
    setForm(f => ({
      ...f,
      blade_forms: f.blade_forms.includes(formName)
        ? f.blade_forms.filter(n => n !== formName)
        : [...f.blade_forms, formName],
    }));
  };

  const marginClass = sidebarCollapsed ? 'md:ml-16' : 'md:ml-56';

  return (
    <div className="min-h-screen bg-surface">
      <Sidebar />

      <main className={`${marginClass} transition-[margin] duration-200 flex flex-col min-h-screen`}>
        <div className="flex items-center justify-between px-8 py-4 border-b border-border flex-shrink-0">
          <h1 className="text-ink text-xl font-bold">Identify a Knife</h1>
        </div>

        <div className="flex flex-1 overflow-hidden">
          {/* ── Left: form ── */}
          <div className="w-80 flex-shrink-0 border-r border-border overflow-y-auto">
            <form onSubmit={handleSubmit} className="p-6 flex flex-col gap-5">

              {/* Image upload */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Photo of your knife</label>
                <div
                  onDragOver={e => e.preventDefault()}
                  onDrop={handleDrop}
                  onClick={() => fileInputRef.current?.click()}
                  className={`relative w-full rounded-xl border-2 border-dashed transition-colors cursor-pointer flex items-center justify-center ${
                    imagePreview
                      ? 'border-gold/40 bg-card'
                      : 'border-border hover:border-gold/30 bg-card/50'
                  }`}
                  style={{ aspectRatio: '4/3' }}
                >
                  {imagePreview ? (
                    <img src={imagePreview} alt="Preview" className="w-full h-full object-contain rounded-xl" />
                  ) : (
                    <div className="text-center px-4">
                      <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" className="mx-auto text-muted/40 mb-2">
                        <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4" />
                        <polyline points="17 8 12 3 7 8" />
                        <line x1="12" y1="3" x2="12" y2="15" />
                      </svg>
                      <p className="text-muted text-xs">Drop a photo here or click to upload</p>
                    </div>
                  )}
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept="image/*"
                    className="hidden"
                    onChange={e => {
                      const f = e.target.files?.[0];
                      if (f) handleImageChange(f);
                    }}
                  />
                </div>
              </div>

              {/* Culinary toggle */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Is this a kitchen knife?</label>
                <div className="flex rounded-lg border border-border overflow-hidden text-xs">
                  {([
                    { label: 'Any', value: null },
                    { label: 'Yes', value: true },
                    { label: 'No', value: false },
                  ] as { label: string; value: boolean | null }[]).map(o => (
                    <button
                      key={String(o.value)}
                      type="button"
                      onClick={() => setForm(f => ({ ...f, is_culinary: o.value }))}
                      className={`flex-1 px-3 py-1.5 transition-colors ${
                        form.is_culinary === o.value
                          ? 'bg-gold/20 text-gold'
                          : 'text-muted hover:text-ink hover:bg-border/30'
                      }`}
                    >
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* Handle material */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Handle material</label>
                <select
                  value={form.handle_material}
                  onChange={e => setForm(f => ({ ...f, handle_material: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any</option>
                  {options?.['handle-types'].map(o => (
                    <option key={o.id} value={o.name}>{o.name}</option>
                  ))}
                </select>
              </div>

              {/* Handle color */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Handle color</label>
                <select
                  value={form.handle_color}
                  onChange={e => setForm(f => ({ ...f, handle_color: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any</option>
                  {options?.['handle-colors'].map(o => (
                    <option key={o.id} value={o.name}>{o.name}</option>
                  ))}
                </select>
              </div>

              {/* Blade color */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Blade color</label>
                <select
                  value={form.blade_color}
                  onChange={e => setForm(f => ({ ...f, blade_color: e.target.value }))}
                  className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                >
                  <option value="">Any</option>
                  {options?.['blade-colors'].map(o => (
                    <option key={o.id} value={o.name}>{o.name}</option>
                  ))}
                </select>
              </div>

              {/* Blade length */}
              <div>
                <div className="flex items-center gap-1.5 mb-1.5">
                  <label className="text-muted text-xs">Blade length (estimate)</label>
                  <div className="relative group">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="text-muted/50 cursor-help">
                      <circle cx="12" cy="12" r="10" />
                      <path d="M9.09 9a3 3 0 015.83 1c0 2-3 3-3 3" />
                      <line x1="12" y1="17" x2="12.01" y2="17" />
                    </svg>
                    <div className="absolute left-0 bottom-full mb-2 w-48 p-2.5 rounded-lg bg-card border border-border shadow-lg text-xs text-muted opacity-0 pointer-events-none group-hover:opacity-100 transition-opacity z-10">
                      <p className="font-semibold text-ink mb-1">Blade length scale</p>
                      <p>Use your index finger as a ruler:</p>
                      <ul className="mt-1 space-y-0.5">
                        <li>1 finger ≈ 3–4.5 inches</li>
                        <li>2 fingers ≈ 4.5–7 inches</li>
                      </ul>
                      <p className="mt-1">Measure the blade only, not the handle.</p>
                    </div>
                  </div>
                </div>
                <div className="flex flex-col gap-1">
                  {BLADE_LENGTH_BINS.map(bin => (
                    <button
                      key={bin.value}
                      type="button"
                      onClick={() => setForm(f => ({
                        ...f,
                        blade_length_bin: f.blade_length_bin === bin.value ? null : bin.value,
                      }))}
                      className={`text-left px-3 py-1.5 rounded-lg border text-xs transition-colors ${
                        form.blade_length_bin === bin.value
                          ? 'border-gold/50 bg-gold/10 text-gold'
                          : 'border-border text-muted hover:text-ink hover:border-border/70'
                      }`}
                    >
                      <span>{bin.label}</span>
                      <span className="text-muted/60 ml-1.5">({bin.range})</span>
                    </button>
                  ))}
                </div>
              </div>

              {/* Blade shape multi-select */}
              <div>
                <label className="block text-muted text-xs mb-1.5">Blade shape (select all that look similar)</label>
                <div className="flex flex-wrap gap-1.5">
                  {forms.map(f => (
                    <button
                      key={f}
                      type="button"
                      onClick={() => toggleForm(f)}
                      className={`px-2.5 py-1 rounded-lg border text-xs transition-colors ${
                        form.blade_forms.includes(f)
                          ? 'border-gold/50 bg-gold/10 text-gold'
                          : 'border-border text-muted hover:text-ink hover:border-border/70'
                      }`}
                    >
                      {f}
                    </button>
                  ))}
                </div>
              </div>

              {/* Vision toggle */}
              {imageFile && (
                <label className="flex items-center gap-2.5 cursor-pointer select-none">
                  <input
                    type="checkbox"
                    checked={form.use_vision}
                    onChange={e => setForm(f => ({ ...f, use_vision: e.target.checked }))}
                    className="w-4 h-4 rounded accent-gold"
                  />
                  <span className="text-muted text-xs">Use AI vision to compare shapes (slower)</span>
                </label>
              )}

              {/* Actions */}
              <div className="flex gap-2 pt-1">
                <button
                  type="submit"
                  disabled={!hasAnyInput || loading}
                  className="flex-1 py-2 px-4 rounded-lg bg-gold text-black text-sm font-semibold hover:bg-gold-bright disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  {loading ? (form.use_vision && imageFile ? 'Analyzing…' : 'Searching…') : 'Identify'}
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
                <p className="text-muted text-sm">Upload a photo and/or fill in clues, then click <strong className="text-ink">Identify</strong>.</p>
              </div>
            )}

            {loading && (
              <div className="p-6 flex flex-col gap-3">
                <div className="text-muted text-xs mb-2">
                  {form.use_vision && imageFile
                    ? 'Analyzing photo with AI vision — this may take 10-15 seconds…'
                    : 'Searching catalog…'}
                </div>
                {Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="skeleton h-20 rounded-xl" />
                ))}
              </div>
            )}

            {results && !loading && results.results.length === 0 && (
              <div className="h-full flex flex-col items-center justify-center gap-2 px-8 py-16">
                <p className="text-muted text-sm">No matching models found. Try broadening your clues.</p>
              </div>
            )}

            {results && !loading && results.results.length > 0 && (() => {
              const visible = showAll ? results.results : results.results.slice(0, 5);
              const hasMore = results.results.length > 5;
              return (
                <div className="p-4 flex flex-col gap-2">
                  <div className="text-muted text-xs px-1 mb-1 flex items-center justify-between">
                    <span>
                      Top {visible.length} of {results.results.length} match{results.results.length !== 1 ? 'es' : ''}
                    </span>
                    <span>
                      {results.families_eliminated} eliminated
                      {results.vision_used && ' · vision used'}
                    </span>
                  </div>
                  {visible.map(r => (
                    <ResultCard
                      key={r.id}
                      result={r}
                      selected={selected?.id === r.id}
                      onClick={() => setSelected(r)}
                    />
                  ))}
                  {hasMore && !showAll && (
                    <button
                      onClick={() => setShowAll(true)}
                      className="mt-1 py-2 px-4 rounded-lg border border-border text-muted text-xs hover:text-ink hover:border-border/70 transition-colors"
                    >
                      Show {results.results.length - 5} more results
                    </button>
                  )}
                </div>
              );
            })()}
          </div>

          {/* ── Right: detail pane ── */}
          <div className="w-80 flex-shrink-0 overflow-y-auto">
            {selected ? (
              <div className="p-6">
                <ResultDetail result={selected} userImage={imagePreview} />
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
