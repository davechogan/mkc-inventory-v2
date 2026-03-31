import { useState } from 'react';
import type { Summary } from '../types';

const COLLAPSED_KEY = 'mkc_statstrip_collapsed';

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
  const [collapsed, setCollapsed] = useState(() => localStorage.getItem(COLLAPSED_KEY) === 'true');

  const toggle = () => {
    const next = !collapsed;
    setCollapsed(next);
    localStorage.setItem(COLLAPSED_KEY, String(next));
  };

  const owned = summary?.master_models ?? 0;
  const total = summary?.catalog_total ?? 0;
  const coverageValue = total > 0 ? `${owned} / ${total}` : `${owned}`;

  return (
    <div className="flex items-center border-b border-border flex-shrink-0">
      <button onClick={toggle} title={collapsed ? 'Show summary' : 'Hide summary'}
        className="px-2 py-2 text-muted hover:text-ink transition-colors flex-shrink-0">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          className={`transition-transform ${collapsed ? '-rotate-90' : 'rotate-0'}`}>
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>

      {collapsed ? (
        <button onClick={toggle} className="py-2 pr-4 text-muted text-xs hover:text-ink transition-colors">
          {summary ? `${summary.total_quantity} knives` : ''}
        </button>
      ) : (
        <div className="flex items-center gap-10 px-4 py-4">
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
              <StatItem label="Knives" value={summary.total_quantity.toLocaleString()} />
              <div className="w-px h-10 bg-border flex-shrink-0" />
              <StatItem label="Invested" value={formatCurrency(summary.total_spend)} gold />
              <div className="w-px h-10 bg-border flex-shrink-0" />
              <StatItem label="Models" value={coverageValue} />
            </>
          )}
        </div>
      )}
    </div>
  );
}
