import { useState, useEffect, useMemo, useCallback } from 'react';
import { Sidebar } from './components/Sidebar';
import { StatStrip } from './components/StatStrip';
import { FamilyChips } from './components/FamilyChips';
import { FilterDrawer } from './components/FilterDrawer';
import { InventoryTable } from './components/InventoryTable';
import { InventoryCardGrid } from './components/InventoryCardGrid';
import { DetailSheet } from './components/DetailSheet';
import { AddInventoryDrawer } from './components/AddInventoryDrawer';
import { useInventoryData } from './hooks/useInventoryData';
import type { FilterState, SortState, InventoryItem } from './types';

type ViewMode = 'table' | 'cards';

const VIEW_KEY = 'mkc_inv_view';
const SIDEBAR_KEY = 'mkc_sidebar_collapsed';

function getInitialView(): ViewMode {
  const stored = localStorage.getItem(VIEW_KEY);
  return stored === 'cards' ? 'cards' : 'table';
}

const emptyFilters: FilterState = {
  search: '',
  family: '',
  handleColor: '',
  condition: '',
  series: '',
  location: '',
};

function countActiveFilters(filters: FilterState): number {
  return (
    (filters.handleColor ? 1 : 0) +
    (filters.condition ? 1 : 0) +
    (filters.series ? 1 : 0) +
    (filters.location ? 1 : 0)
  );
}

function applyFilters(items: InventoryItem[], filters: FilterState): InventoryItem[] {
  return items.filter((item) => {
    // Search
    if (filters.search) {
      const q = filters.search.toLowerCase();
      const searchable = [
        item.knife_name,
        item.nickname,
        item.series_name,
        item.catalog_line,
        item.handle_color,
        item.blade_steel,
        item.knife_family,
        item.knife_type,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      if (!searchable.includes(q)) return false;
    }
    // Family
    if (filters.family && item.knife_family !== filters.family) return false;
    // Handle color
    if (filters.handleColor) {
      const hc = item.handle_color?.toLowerCase() ?? '';
      if (!hc.includes(filters.handleColor.toLowerCase())) return false;
    }
    // Condition
    if (filters.condition && item.condition !== filters.condition) return false;
    // Series
    if (filters.series) {
      const s = [item.series_name, item.catalog_line].filter(Boolean).join(' ').toLowerCase();
      if (!s.includes(filters.series.toLowerCase())) return false;
    }
    // Location
    if (filters.location) {
      const loc = item.location?.toLowerCase() ?? '';
      if (!loc.includes(filters.location.toLowerCase())) return false;
    }
    return true;
  });
}

type SortableKey = keyof InventoryItem;

function applySort(items: InventoryItem[], sort: SortState): InventoryItem[] {
  if (!sort.col) return items;
  return [...items].sort((a, b) => {
    const key = sort.col as SortableKey;
    const av = a[key];
    const bv = b[key];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    let cmp = 0;
    if (typeof av === 'number' && typeof bv === 'number') {
      cmp = av - bv;
    } else {
      cmp = String(av).localeCompare(String(bv));
    }
    return sort.dir === 'asc' ? cmp : -cmp;
  });
}

function IconTable() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <line x1="3" y1="9" x2="21" y2="9" />
      <line x1="3" y1="15" x2="21" y2="15" />
      <line x1="9" y1="9" x2="9" y2="21" />
    </svg>
  );
}

function IconGrid() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="7" height="7" />
      <rect x="14" y="3" width="7" height="7" />
      <rect x="3" y="14" width="7" height="7" />
      <rect x="14" y="14" width="7" height="7" />
    </svg>
  );
}

function IconRefresh() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 4 23 10 17 10" />
      <polyline points="1 20 1 14 7 14" />
      <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
    </svg>
  );
}

function IconFilter() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
    </svg>
  );
}

function IconDownload() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
      <polyline points="7 10 12 15 17 10" />
      <line x1="12" y1="15" x2="12" y2="3" />
    </svg>
  );
}

export default function App() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true'
  );
  const [view, setView] = useState<ViewMode>(getInitialView);
  const [filters, setFilters] = useState<FilterState>(emptyFilters);
  const [sort, setSort] = useState<SortState>({ col: 'knife_name', dir: 'asc' });
  const [filterOpen, setFilterOpen] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [selectedItem, setSelectedItem] = useState<InventoryItem | null>(null);

  const { items, summary, loading, error, reload } = useInventoryData();

  // Listen to sidebar toggle events
  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent<{ collapsed: boolean }>;
      setSidebarCollapsed(ce.detail.collapsed);
    };
    window.addEventListener('mkc-sidebar-toggle', handler);
    return () => window.removeEventListener('mkc-sidebar-toggle', handler);
  }, []);

  // Persist view preference
  useEffect(() => {
    localStorage.setItem(VIEW_KEY, view);
  }, [view]);

  const handleFilterChange = useCallback((key: keyof FilterState, value: string) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  }, []);

  const handleSort = useCallback((col: string) => {
    setSort((prev) => ({
      col,
      dir: prev.col === col && prev.dir === 'asc' ? 'desc' : 'asc',
    }));
  }, []);

  const filteredItems = useMemo(() => {
    const filtered = applyFilters(items, filters);
    return applySort(filtered, sort);
  }, [items, filters, sort]);

  const families = summary?.by_family ?? [];
  const activeFilterCount = countActiveFilters(filters);

  const marginClass = sidebarCollapsed ? 'ml-16' : 'ml-56';

  return (
    <div className="min-h-screen bg-surface">
      <Sidebar />

      <main
        id="appMain"
        className={`${marginClass} transition-[margin] duration-200 flex flex-col min-h-screen`}
      >
        {/* Top bar */}
        <div className="flex items-center justify-between px-8 py-4 border-b border-border flex-shrink-0">
          <h1 className="text-ink text-xl font-bold">Collection</h1>
          <div className="flex items-center gap-2">
            {/* Add knife */}
            <button
              onClick={() => setAddOpen(true)}
              className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-gold text-black text-sm font-semibold hover:bg-gold-bright transition-colors"
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
              </svg>
              Add Knife
            </button>

            {/* View toggle */}
            <div className="flex items-center rounded-lg border border-border overflow-hidden">
              <button
                onClick={() => setView('table')}
                title="Table view"
                className={`px-3 py-2 transition-colors ${
                  view === 'table'
                    ? 'bg-border/60 text-ink'
                    : 'text-muted hover:text-ink hover:bg-border/30'
                }`}
              >
                <IconTable />
              </button>
              <button
                onClick={() => setView('cards')}
                title="Card view"
                className={`px-3 py-2 transition-colors ${
                  view === 'cards'
                    ? 'bg-border/60 text-ink'
                    : 'text-muted hover:text-ink hover:bg-border/30'
                }`}
              >
                <IconGrid />
              </button>
            </div>

            {/* Export CSV */}
            <a
              href="/api/v2/inventory/export"
              className="flex items-center gap-1.5 px-3 py-2 rounded-lg border border-border text-muted hover:text-ink hover:border-border/80 transition-colors text-sm"
              title="Export CSV"
            >
              <IconDownload />
              <span>Export CSV</span>
            </a>

            {/* Refresh */}
            <button
              onClick={() => { void reload(); }}
              title="Refresh"
              className="flex items-center gap-1.5 px-3 py-2 rounded-lg border border-border text-muted hover:text-ink hover:border-border/80 transition-colors text-sm"
            >
              <IconRefresh />
              <span>Refresh</span>
            </button>
          </div>
        </div>

        {/* Stat strip */}
        <StatStrip summary={summary} loading={loading} />

        {/* Family chips */}
        {!loading && families.length > 0 && (
          <div className="px-8 py-3 border-b border-border overflow-x-auto">
            <FamilyChips
              families={families}
              activeFamily={filters.family}
              onSelect={(f) => handleFilterChange('family', f)}
            />
          </div>
        )}

        {/* Toolbar */}
        <div className="flex items-center gap-3 px-8 py-3 border-b border-border flex-shrink-0">
          {/* Search */}
          <div className="relative flex-1 max-w-sm">
            <svg
              className="absolute left-3 top-1/2 -translate-y-1/2 text-muted pointer-events-none"
              width="15"
              height="15"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
            <input
              type="search"
              placeholder="Search knives…"
              value={filters.search}
              onChange={(e) => handleFilterChange('search', e.target.value)}
              className="w-full pl-9 pr-4 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors"
            />
          </div>

          {/* Filters button */}
          <button
            onClick={() => setFilterOpen(true)}
            className={`relative flex items-center gap-2 px-3 py-2 rounded-lg border text-sm transition-colors ${
              activeFilterCount > 0
                ? 'border-gold/50 text-gold bg-gold/5 hover:bg-gold/10'
                : 'border-border text-muted hover:text-ink hover:border-border/80'
            }`}
          >
            <IconFilter />
            <span>Filters</span>
            {activeFilterCount > 0 && (
              <span className="absolute -top-1.5 -right-1.5 bg-gold text-black text-xs font-bold rounded-full w-4 h-4 flex items-center justify-center leading-none">
                {activeFilterCount}
              </span>
            )}
          </button>

          {/* Results count */}
          {!loading && (
            <span className="text-muted text-xs ml-auto">
              {filteredItems.length.toLocaleString()} item{filteredItems.length !== 1 ? 's' : ''}
            </span>
          )}
        </div>

        {/* Error banner */}
        {error && (
          <div className="mx-8 mt-4 px-4 py-3 rounded-lg bg-red-950/40 border border-red-800/50 text-red-300 text-sm">
            {error}
          </div>
        )}

        {/* Loading skeleton */}
        {loading && (
          <div className="flex-1 px-8 py-6 flex flex-col gap-3">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="skeleton h-12 rounded-lg" />
            ))}
          </div>
        )}

        {/* Content */}
        {!loading && !error && (
          <div className="flex-1 overflow-auto px-8 py-4">
            {view === 'table' ? (
              <InventoryTable
                items={filteredItems}
                sort={sort}
                onSort={handleSort}
                onRowClick={setSelectedItem}
              />
            ) : (
              <InventoryCardGrid
                items={filteredItems}
                onCardClick={setSelectedItem}
              />
            )}
          </div>
        )}
      </main>

      {/* Drawers */}
      <FilterDrawer
        open={filterOpen}
        onClose={() => setFilterOpen(false)}
        filters={filters}
        onChange={handleFilterChange}
      />

      <DetailSheet
        item={selectedItem}
        onClose={() => setSelectedItem(null)}
        onChanged={() => { setSelectedItem(null); void reload(); }}
      />

      <AddInventoryDrawer
        open={addOpen}
        onClose={() => setAddOpen(false)}
        onAdded={() => { void reload(); }}
      />

      {/* Floating add button — always accessible while scrolling */}
      <button
        onClick={() => setAddOpen(true)}
        title="Add Knife"
        className="fixed bottom-6 right-6 z-30 flex items-center justify-center w-12 h-12 rounded-full bg-gold text-black shadow-lg shadow-gold/20 hover:bg-gold-bright transition-all duration-200 hover:scale-110"
      >
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
        </svg>
      </button>
    </div>
  );
}
