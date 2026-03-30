import type { InventoryItem, SortState } from '../types';
import { imageUrl } from '../api';

interface InventoryTableProps {
  items: InventoryItem[];
  sort: SortState;
  onSort: (col: string) => void;
  onRowClick: (item: InventoryItem) => void;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(value);
}

function KnifePlaceholder() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="text-muted">
      <path d="M3 21l7.5-7.5M14 3l7 7-9 9-7-7 2-2 5-5 2-2z" />
      <path d="M5 19l-2 2" />
    </svg>
  );
}

interface ThProps {
  col?: string;
  label: string;
  sort?: SortState;
  onSort?: (col: string) => void;
  align?: 'left' | 'right';
  sortable?: boolean;
}

function Th({ col, label, sort, onSort, align = 'left', sortable = true }: ThProps) {
  const isActive = sort && col && sort.col === col;
  const handleClick = () => {
    if (sortable && col && onSort) onSort(col);
  };

  return (
    <th
      className={`px-3 py-2.5 text-muted text-xs uppercase tracking-wider font-semibold whitespace-nowrap ${
        align === 'right' ? 'text-right' : 'text-left'
      } ${sortable && col ? 'cursor-pointer select-none hover:text-ink transition-colors' : ''}`}
      onClick={handleClick}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {sortable && col && (
          <span className={isActive ? 'text-gold' : 'text-border'}>
            {isActive ? (sort.dir === 'asc' ? '↑' : '↓') : '↕'}
          </span>
        )}
      </span>
    </th>
  );
}

interface ThumbnailProps {
  item: InventoryItem;
}

function Thumbnail({ item }: ThumbnailProps) {
  const url = imageUrl(item);
  if (!url) {
    return (
      <div className="w-10 h-10 rounded-md bg-card flex items-center justify-center flex-shrink-0">
        <KnifePlaceholder />
      </div>
    );
  }
  return (
    <img
      src={url}
      alt={item.knife_name}
      loading="lazy"
      className="w-10 h-10 rounded-md object-cover bg-card flex-shrink-0"
      onError={(e) => {
        const target = e.currentTarget;
        target.style.display = 'none';
        const placeholder = document.createElement('div');
        placeholder.className = 'w-10 h-10 rounded-md bg-card flex items-center justify-center flex-shrink-0';
        target.parentNode?.insertBefore(placeholder, target);
      }}
    />
  );
}

export function InventoryTable({ items, sort, onSort, onRowClick }: InventoryTableProps) {
  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center py-24 text-muted text-sm">
        No items match your filters.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm border-collapse">
        <thead className="bg-sidebar sticky top-0 z-10">
          <tr>
            <th className="px-3 py-2.5 w-14" />
            <Th col="knife_name" label="Model" sort={sort} onSort={onSort} />
            <Th col="series_name" label="Series" sort={sort} onSort={onSort} />
            <Th col="handle_color" label="Handle" sort={sort} onSort={onSort} />
            <Th col="quantity" label="Qty" sort={sort} onSort={onSort} align="right" />
            <Th col="blade_length" label="Length" sort={sort} onSort={onSort} align="right" />
            <Th col="blade_steel" label="Steel" sort={sort} onSort={onSort} />
            <Th col="blade_finish" label="Finish" sort={sort} onSort={onSort} />
            <Th col="handle_type" label="Handle Type" sort={sort} onSort={onSort} />
            <Th col="purchase_price" label="Price" sort={sort} onSort={onSort} align="right" />
          </tr>
        </thead>
        <tbody>
          {items.map((item) => (
            <tr
              key={item.id}
              onClick={() => onRowClick(item)}
              className="border-b border-border/50 hover:bg-card/60 cursor-pointer transition-colors"
            >
              <td className="px-3 py-2.5">
                <Thumbnail item={item} />
              </td>
              <td className="px-3 py-2.5 max-w-[200px]">
                <div className="text-ink font-medium truncate">{item.knife_name}</div>
              </td>
              <td className="px-3 py-2.5 text-muted">
                {item.series_name || item.catalog_line || '—'}
              </td>
              <td className="px-3 py-2.5 text-muted">{item.handle_color || '—'}</td>
              <td className="px-3 py-2.5 text-ink text-right">{item.quantity}</td>
              <td className="px-3 py-2.5 text-muted text-right">
                {item.blade_length != null ? `${item.blade_length}"` : '—'}
              </td>
              <td className="px-3 py-2.5 text-muted">{item.blade_steel || '—'}</td>
              <td className="px-3 py-2.5 text-muted">{item.blade_finish || '—'}</td>
              <td className="px-3 py-2.5 text-muted">{item.handle_type || '—'}</td>
              <td className="px-3 py-2.5 text-ink text-right font-medium">
                {formatCurrency(item.purchase_price)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
