import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { InventoryItem } from '../types';
import { imageUrl } from '../api';

async function duplicateItem(id: number): Promise<{ id: number }> {
  const res = await fetch(`/api/v2/inventory/${id}/duplicate`, { method: 'POST' });
  if (!res.ok) throw new Error('Failed to duplicate');
  return res.json();
}

async function deleteItem(id: number): Promise<void> {
  const res = await fetch(`/api/v2/inventory/${id}`, { method: 'DELETE' });
  if (!res.ok) throw new Error('Failed to delete');
}

interface InventoryCardGridProps {
  items: InventoryItem[];
  onCardClick: (item: InventoryItem) => void;
  onDataChanged: () => void;
}

function KnifePlaceholderLarge() {
  return (
    <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="text-muted/40">
      <path d="M3 21l7.5-7.5M14 3l7 7-9 9-7-7 2-2 5-5 2-2z" />
      <path d="M5 19l-2 2" />
    </svg>
  );
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(value);
}

interface LazyImageProps {
  src: string;
  alt: string;
}

function LazyImage({ src, alt }: LazyImageProps) {
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState(false);
  const [visible, setVisible] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setVisible(true);
          obs.disconnect();
        }
      },
      { rootMargin: '200px' }
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  return (
    <div ref={ref} className="w-full h-full">
      {visible && !error ? (
        <>
          {!loaded && <div className="skeleton absolute inset-0" />}
          <img
            src={src}
            alt={alt}
            className={`w-full h-full object-contain transition-opacity duration-300 ${loaded ? 'opacity-100' : 'opacity-0'}`}
            onLoad={() => setLoaded(true)}
            onError={() => setError(true)}
          />
        </>
      ) : error ? (
        <div className="w-full h-full flex items-center justify-center">
          <KnifePlaceholderLarge />
        </div>
      ) : (
        <div className="skeleton w-full h-full" />
      )}
    </div>
  );
}

// ── Grouped card types ──────────────────────────────────────────────────────

interface CardGroup {
  key: string;
  modelId: number;
  colorwayId: number | null;
  name: string;
  items: InventoryItem[];
}

function groupItems(items: InventoryItem[]): CardGroup[] {
  const map = new Map<string, CardGroup>();
  for (const item of items) {
    const key = `${item.knife_model_id}:${item.colorway_id ?? 'null'}`;
    let group = map.get(key);
    if (!group) {
      group = {
        key,
        modelId: item.knife_model_id,
        colorwayId: item.colorway_id,
        name: item.knife_name,
        items: [],
      };
      map.set(key, group);
    }
    group.items.push(item);
  }
  return Array.from(map.values());
}

// ── GroupCard ────────────────────────────────────────────────────────────────

interface GroupCardProps {
  group: CardGroup;
  onCardClick: (item: InventoryItem) => void;
  onDataChanged: () => void;
}

function GroupCard({ group, onCardClick, onDataChanged }: GroupCardProps) {
  const [expanded, setExpanded] = useState(false);
  const primary = group.items[0];
  const url = imageUrl(primary);
  const count = group.items.length;

  const handleDuplicate = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      await duplicateItem(primary.id);
      onDataChanged();
    } catch { /* ignore */ }
  }, [primary.id, onDataChanged]);

  const handleDelete = useCallback(async (e: React.MouseEvent, itemId: number) => {
    e.stopPropagation();
    if (count === 1 && !confirm(`Remove "${primary.knife_name}" from your collection?`)) return;
    try {
      await deleteItem(itemId);
      onDataChanged();
    } catch { /* ignore */ }
  }, [count, primary.knife_name, onDataChanged]);

  const pills: string[] = [];
  if (primary.handle_color) pills.push(primary.handle_color);
  if (primary.blade_steel) pills.push(primary.blade_steel.split(' ')[0]);

  return (
    <div className="relative bg-card border border-border rounded-2xl overflow-hidden group hover:border-gold/40 hover:shadow-xl hover:shadow-gold/20 hover:scale-[1.1] hover:z-10 transition-all duration-300">
      {/* Main card — click opens detail or expands */}
      <div
        onClick={() => count > 1 ? setExpanded(v => !v) : onCardClick(primary)}
        className="cursor-pointer"
      >
        {/* Image area */}
        <div className="relative w-full" style={{ paddingBottom: '75%' }}>
          <div className="absolute inset-0 bg-card">
            {url ? <LazyImage src={url} alt={primary.knife_name} /> : (
              <div className="w-full h-full flex items-center justify-center"><KnifePlaceholderLarge /></div>
            )}
          </div>
          {/* Count badge */}
          {count > 1 && (
            <div className="absolute top-2 right-2 bg-gold text-black text-xs font-bold px-1.5 py-0.5 rounded-md">
              x{count}
            </div>
          )}
        </div>

        {/* Body */}
        <div className="p-3 flex flex-col gap-1.5">
          <div className="text-ink font-semibold text-sm leading-tight line-clamp-2">
            {primary.knife_name}
          </div>

          {pills.length > 0 && (
            <div className="flex flex-wrap gap-1 mt-1">
              {pills.map((p, i) => (
                <span key={i} className="text-xs px-2 py-0.5 rounded-full bg-border/60 text-muted">{p}</span>
              ))}
            </div>
          )}

          {/* Footer */}
          <div className="flex items-center justify-between mt-1.5">
            <span className="text-gold text-sm font-bold">
              {formatCurrency(primary.purchase_price)}
            </span>
            <span className="flex items-center gap-1">
              <button onClick={(e) => handleDelete(e, primary.id)} title="Remove one"
                className="opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center w-4 h-4 rounded-full bg-border/40 hover:bg-red-900/40 text-muted hover:text-red-400">
                <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="5" y1="12" x2="19" y2="12" />
                </svg>
              </button>
              <button onClick={handleDuplicate} title="Add another"
                className="opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center w-4 h-4 rounded-full bg-gold/20 hover:bg-gold/40 text-gold">
                <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
                </svg>
              </button>
            </span>
          </div>
        </div>
      </div>

      {/* Expanded instance list */}
      {expanded && count > 1 && (
        <div className="border-t border-border bg-surface/50 px-3 py-2 flex flex-col gap-1.5">
          {group.items.map((item, i) => (
            <div key={item.id}
              onClick={() => onCardClick(item)}
              className="flex items-center justify-between gap-2 px-2 py-1.5 rounded-lg hover:bg-border/20 cursor-pointer transition-colors text-xs">
              <div className="flex-1 min-w-0">
                <span className="text-ink">#{i + 1}</span>
                {item.location && <span className="text-muted ml-2">{item.location}</span>}
                {item.notes && <span className="text-muted/50 ml-2 truncate">{item.notes}</span>}
              </div>
              <span className="text-gold flex-shrink-0">{formatCurrency(item.purchase_price)}</span>
              <button onClick={(e) => handleDelete(e, item.id)} title="Remove this one"
                className="text-muted hover:text-red-400 transition-colors flex-shrink-0 p-0.5">
                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Grid ─────────────────────────────────────────────────────────────────────

export function InventoryCardGrid({ items, onCardClick, onDataChanged }: InventoryCardGridProps) {
  const groups = useMemo(() => groupItems(items), [items]);

  if (groups.length === 0) {
    return (
      <div className="flex items-center justify-center py-24 text-muted text-sm">
        No items match your filters.
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
      {groups.map((group) => (
        <GroupCard key={group.key} group={group} onCardClick={onCardClick} onDataChanged={onDataChanged} />
      ))}
    </div>
  );
}
