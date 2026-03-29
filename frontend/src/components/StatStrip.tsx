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

function StatItem({
  label,
  value,
  gold,
}: {
  label: string;
  value: string;
  gold?: boolean;
}) {
  return (
    <div className="flex flex-col gap-0.5 min-w-0">
      <span className="text-muted text-xs uppercase tracking-widest font-medium">{label}</span>
      <span className={`font-bold text-2xl leading-none tabular-nums ${gold ? 'text-gold' : 'text-ink'}`}>
        {value}
      </span>
    </div>
  );
}

function SkeletonStat() {
  return (
    <div className="flex flex-col gap-2">
      <div className="skeleton h-3 w-16 rounded" />
      <div className="skeleton h-7 w-20 rounded" />
    </div>
  );
}

export function StatStrip({ summary, loading }: StatStripProps) {
  const avgCost =
    summary && summary.total_quantity > 0
      ? summary.total_spend / summary.total_quantity
      : null;

  return (
    <div className="flex items-center gap-10 px-8 py-4 border-b border-border">
      {loading || !summary ? (
        <>
          <SkeletonStat />
          <div className="w-px h-10 bg-border flex-shrink-0" />
          <SkeletonStat />
          <div className="w-px h-10 bg-border flex-shrink-0" />
          <SkeletonStat />
        </>
      ) : (
        <>
          <StatItem
            label="Knives"
            value={summary.total_quantity.toLocaleString()}
          />
          <div className="w-px h-10 bg-border flex-shrink-0" />
          <StatItem
            label="Invested"
            value={formatCurrency(summary.total_spend)}
            gold
          />
          <div className="w-px h-10 bg-border flex-shrink-0" />
          <StatItem
            label="Avg. Cost"
            value={formatCurrency(avgCost)}
            gold
          />
        </>
      )}
    </div>
  );
}
