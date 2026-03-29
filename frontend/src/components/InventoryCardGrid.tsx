import { useCallback, useEffect, useRef, useState } from 'react';
import type { InventoryItem } from '../types';
import { imageUrl } from '../api';

async function incrementQuantity(id: number): Promise<number> {
  const res = await fetch(`/api/v2/inventory/${id}/quantity`, { method: 'PATCH' });
  if (!res.ok) throw new Error('Failed to update quantity');
  const data = await res.json() as { quantity: number };
  return data.quantity;
}

interface InventoryCardGridProps {
  items: InventoryItem[];
  onCardClick: (item: InventoryItem) => void;
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
            className={`w-full h-full object-contain transition-all duration-300 group-hover:scale-110 ${loaded ? 'opacity-100' : 'opacity-0'}`}
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

interface CardProps {
  item: InventoryItem;
  onClick: () => void;
}

function Card({ item, onClick }: CardProps) {
  const url = imageUrl(item);
  const [qty, setQty] = useState(item.quantity);

  const handleIncrement = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    const next = qty + 1;
    setQty(next); // optimistic
    try {
      const confirmed = await incrementQuantity(item.id);
      setQty(confirmed);
    } catch {
      setQty(qty); // revert on error
    }
  }, [item.id, qty]);

  const pills: string[] = [];
  if (item.handle_color) pills.push(item.handle_color);
  if (item.blade_steel) pills.push(item.blade_steel.split(' ')[0]);

  return (
    <div
      onClick={onClick}
      className="bg-card border border-border rounded-2xl overflow-hidden cursor-pointer group hover:border-gold/40 hover:shadow-lg hover:shadow-gold/5 transition-all duration-200"
    >
      {/* Image area — 4:3 */}
      <div className="relative w-full" style={{ paddingBottom: '75%' }}>
        <div className="absolute inset-0 bg-card">
          {url ? (
            <LazyImage src={url} alt={item.knife_name} />
          ) : (
            <div className="w-full h-full flex items-center justify-center">
              <KnifePlaceholderLarge />
            </div>
          )}
        </div>
      </div>

      {/* Body */}
      <div className="p-3 flex flex-col gap-1.5">
        <div className="text-ink font-semibold text-sm leading-tight line-clamp-2">
          {item.knife_name}
        </div>
        {item.nickname && (
          <div className="text-muted text-xs truncate">{item.nickname}</div>
        )}

        {/* Pills */}
        {pills.length > 0 && (
          <div className="flex flex-wrap gap-1 mt-1">
            {pills.map((p, i) => (
              <span
                key={i}
                className="text-xs px-2 py-0.5 rounded-full bg-border/60 text-muted"
              >
                {p}
              </span>
            ))}
          </div>
        )}

        {/* Footer — price left, quantity right with hover + */}
        <div className="flex items-center justify-between mt-1.5">
          <span className="text-gold text-sm font-bold">
            {formatCurrency(item.purchase_price)}
          </span>
          <span className="flex items-center gap-1">
            <span className="text-gold text-sm font-bold">×{qty}</span>
            <button
              onClick={handleIncrement}
              title="Add another"
              className="opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center w-4 h-4 rounded-full bg-gold/20 hover:bg-gold/40 text-gold"
            >
              <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
              </svg>
            </button>
          </span>
        </div>
      </div>
    </div>
  );
}

export function InventoryCardGrid({ items, onCardClick }: InventoryCardGridProps) {
  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center py-24 text-muted text-sm">
        No items match your filters.
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
      {items.map((item) => (
        <Card key={item.id} item={item} onClick={() => onCardClick(item)} />
      ))}
    </div>
  );
}
