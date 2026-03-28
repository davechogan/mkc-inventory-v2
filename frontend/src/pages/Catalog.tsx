import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { Sidebar } from '../components/Sidebar';

// ── Types ─────────────────────────────────────────────────────────────────────

interface CatalogModel {
  id: number;
  parent_model_id: number | null;
  official_name: string;
  normalized_name: string | null;
  slug: string | null;
  knife_type: string | null;
  family_name: string | null;
  form_name: string | null;
  series_name: string | null;
  collaborator_name: string | null;
  generation_label: string | null;
  size_modifier: string | null;
  platform_variant: string | null;
  steel: string | null;
  blade_finish: string | null;
  blade_color: string | null;
  handle_color: string | null;
  handle_type: string | null;
  blade_length: number | null;
  record_status: string | null;
  is_current_catalog: boolean;
  is_discontinued: boolean;
  msrp: number | null;
  official_product_url: string | null;
  official_image_url: string | null;
  in_inventory_count: number;
  has_identifier_image: boolean;
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
  if (model.steel) parts.push(model.steel);
  if (model.blade_finish) parts.push(model.blade_finish);
  if (model.blade_length) parts.push(`${model.blade_length}"`);
  if (model.handle_color) parts.push(model.handle_color);
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

  useEffect(() => {
    if (!model.has_identifier_image) return;
    const el = containerRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && imgRef.current && !imgRef.current.src) {
          imgRef.current.src = `/api/v2/models/${model.id}/image`;
          observer.disconnect();
        }
      },
      { rootMargin: '200px' }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [model.id, model.has_identifier_image]);

  const specs = specsLine(model);

  return (
    <button
      onClick={onClick}
      className={`group text-left flex flex-col rounded-xl border overflow-hidden transition-all duration-150 ${
        selected
          ? 'border-gold/50 ring-1 ring-gold/20'
          : 'border-border hover:border-border/70'
      }`}
      style={{ backgroundColor: '#0f1114' }}
    >
      {/* Image area */}
      <div
        ref={containerRef}
        className="relative w-full aspect-[4/3] bg-border/10 flex items-center justify-center overflow-hidden flex-shrink-0"
      >
        {model.has_identifier_image && !imgError ? (
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

        {/* Discontinued badge */}
        {model.is_discontinued && (
          <div className="absolute top-2 left-2 bg-surface/80 text-muted text-xs px-1.5 py-0.5 rounded-md leading-none border border-border">
            Discontinued
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

// ── Detail panel ──────────────────────────────────────────────────────────────

function ModelDetail({ model, onClose }: { model: CatalogModel; onClose: () => void }) {
  const [imgLoaded, setImgLoaded] = useState(false);
  const imgSrc = model.has_identifier_image ? `/api/v2/models/${model.id}/image` : null;

  const specs: Array<[string, string | number | null]> = [
    ['Type', model.knife_type],
    ['Family', model.family_name],
    ['Series', model.series_name],
    ['Form', model.form_name],
    ['Steel', model.steel],
    ['Finish', model.blade_finish],
    ['Blade Color', model.blade_color],
    ['Handle Color', model.handle_color],
    ['Handle Type', model.handle_type],
    ['Blade Length', model.blade_length ? `${model.blade_length}"` : null],
    ['Generation', model.generation_label],
    ['Status', model.record_status],
    ['MSRP', model.msrp != null ? `$${model.msrp.toLocaleString('en-US', { minimumFractionDigits: 0 })}` : null],
  ].filter(([, v]) => v != null) as Array<[string, string | number]>;

  return (
    <div className="h-full flex flex-col overflow-hidden" style={{ backgroundColor: '#060709', borderLeft: '1px solid #1d2329' }}>
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-border flex-shrink-0">
        <span className="text-muted text-xs uppercase tracking-widest">Model Detail</span>
        <button
          onClick={onClose}
          className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30"
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
          </svg>
        </button>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-y-auto p-5 flex flex-col gap-5">
        {/* Image */}
        {imgSrc && (
          <div className="rounded-xl overflow-hidden bg-border/10 aspect-[4/3]">
            {!imgLoaded && <div className="skeleton w-full aspect-[4/3]" />}
            <img
              src={imgSrc}
              alt={model.official_name}
              onLoad={() => setImgLoaded(true)}
              className={`w-full h-full object-contain transition-opacity duration-300 ${imgLoaded ? 'opacity-100' : 'opacity-0'}`}
            />
          </div>
        )}

        {/* Name */}
        <div>
          <h2 className="text-ink text-base font-bold leading-snug">{model.official_name}</h2>
          {model.collaborator_name && (
            <div className="text-gold text-xs mt-0.5">Collaboration: {model.collaborator_name}</div>
          )}
          {model.in_inventory_count > 0 && (
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

        {/* Links */}
        {model.official_product_url && (
          <a
            href={model.official_product_url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 text-xs text-gold/80 hover:text-gold transition-colors"
          >
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
              <polyline points="15 3 21 3 21 9" />
              <line x1="10" y1="14" x2="21" y2="3" />
            </svg>
            View on MKC website
          </a>
        )}
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
  }, [activeServerFilters]);

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
  const hasDetail = selected !== null;

  return (
    <div className="min-h-screen bg-surface">
      <Sidebar />

      <main className={`${marginClass} transition-[margin] duration-200 flex flex-col min-h-screen`}>
        {/* Top bar */}
        <div className="flex items-center justify-between px-8 py-4 border-b border-border flex-shrink-0 gap-4 flex-wrap">
          <h1 className="text-ink text-xl font-bold flex-shrink-0">Catalog</h1>

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
            {selected && (
              <ModelDetail model={selected} onClose={() => setSelected(null)} />
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
