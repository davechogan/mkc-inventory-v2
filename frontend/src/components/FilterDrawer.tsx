import { type ReactNode } from 'react';
import type { FilterState } from '../types';

interface FilterDrawerProps {
  open: boolean;
  onClose: () => void;
  filters: FilterState;
  onChange: (key: keyof FilterState, value: string) => void;
}

interface FieldGroupProps {
  label: string;
  children: ReactNode;
}

function FieldGroup({ label, children }: FieldGroupProps) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-muted text-xs uppercase tracking-widest font-semibold">{label}</label>
      {children}
    </div>
  );
}

const inputClass =
  'w-full bg-card border border-border rounded-lg px-3 py-2 text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors';

export function FilterDrawer({ open, onClose, filters, onChange }: FilterDrawerProps) {
  const clearAll = () => {
    onChange('search', '');
    onChange('family', '');
    onChange('handleColor', '');
    onChange('series', '');
    onChange('location', '');
  };

  return (
    <>
      {/* Backdrop */}
      {open && (
        <div
          className="fixed inset-0 bg-black/50 z-40 transition-opacity"
          onClick={onClose}
        />
      )}

      {/* Drawer */}
      <div
        className={`fixed right-0 top-0 h-full w-80 flex flex-col z-50 border-l border-border transition-transform duration-200 ${
          open ? 'translate-x-0' : 'translate-x-full'
        }`}
        style={{ backgroundColor: '#060709' }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-border flex-shrink-0">
          <h2 className="text-ink font-semibold text-base">Filters</h2>
          <button
            onClick={onClose}
            className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30"
            aria-label="Close filters"
          >
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>

        {/* Filter groups */}
        <div className="flex-1 overflow-y-auto px-5 py-5 flex flex-col gap-5">
          <FieldGroup label="Handle Color">
            <input
              type="text"
              className={inputClass}
              placeholder="e.g. Black, OD Green…"
              value={filters.handleColor}
              onChange={(e) => onChange('handleColor', e.target.value)}
            />
          </FieldGroup>

          <FieldGroup label="Series">
            <input
              type="text"
              className={inputClass}
              placeholder="e.g. Handmade, Standard…"
              value={filters.series}
              onChange={(e) => onChange('series', e.target.value)}
            />
          </FieldGroup>

          <FieldGroup label="Location">
            <input
              type="text"
              className={inputClass}
              placeholder="e.g. Safe, Display…"
              value={filters.location}
              onChange={(e) => onChange('location', e.target.value)}
            />
          </FieldGroup>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between gap-3 px-5 py-4 border-t border-border flex-shrink-0">
          <button
            onClick={clearAll}
            className="text-muted text-sm hover:text-ink transition-colors"
          >
            Clear all
          </button>
          <button
            onClick={onClose}
            className="px-4 py-2 bg-gold/90 hover:bg-gold-bright text-black text-sm font-semibold rounded-lg transition-colors"
          >
            Apply
          </button>
        </div>
      </div>
    </>
  );
}
