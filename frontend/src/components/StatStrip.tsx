import { type ReactNode } from 'react';
import type { Summary } from '../types';

interface StatStripProps {
  summary: Summary | null;
  loading: boolean;
}

function formatCurrency(value: number | undefined | null): string {
  if (value == null) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(value);
}

function StatItem({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-muted text-xs uppercase tracking-widest font-semibold">{label}</span>
      <span className="text-ink font-bold text-lg leading-tight">{value}</span>
    </div>
  );
}

function SkeletonStat() {
  return (
    <div className="flex flex-col gap-1">
      <div className="skeleton h-3 w-16 rounded" />
      <div className="skeleton h-6 w-12 rounded" />
    </div>
  );
}

export function StatStrip({ summary, loading }: StatStripProps) {
  return (
    <div className="flex items-center gap-8 px-8 py-3 border-b border-border text-sm">
      {loading || !summary ? (
        <>
          <SkeletonStat />
          <div className="w-px h-8 bg-border" />
          <SkeletonStat />
          <div className="w-px h-8 bg-border" />
          <SkeletonStat />
          <div className="w-px h-8 bg-border" />
          <SkeletonStat />
          <div className="w-px h-8 bg-border" />
          <SkeletonStat />
        </>
      ) : (
        <>
          <StatItem label="Rows" value={summary.inventory_rows.toLocaleString()} />
          <div className="w-px h-8 bg-border flex-shrink-0" />
          <StatItem label="Quantity" value={summary.total_quantity.toLocaleString()} />
          <div className="w-px h-8 bg-border flex-shrink-0" />
          <StatItem label="Spent" value={formatCurrency(summary.total_spend)} />
          <div className="w-px h-8 bg-border flex-shrink-0" />
          <StatItem
            label="Est. Value"
            value={formatCurrency(summary.total_estimated_value ?? summary.estimated_value)}
          />
          <div className="w-px h-8 bg-border flex-shrink-0" />
          <StatItem
            label="Models"
            value={
              summary.master_count != null
                ? summary.master_count.toLocaleString()
                : summary.master_models != null
                ? summary.master_models.toLocaleString()
                : '—'
            }
          />
        </>
      )}
    </div>
  );
}
