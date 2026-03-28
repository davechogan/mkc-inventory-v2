import { type ReactNode } from 'react';
import type { InventoryItem } from '../types';
import { imageUrl } from '../api';

interface DetailSheetProps {
  item: InventoryItem | null;
  onClose: () => void;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  }).format(value);
}

function formatDate(value: string | null | undefined): string {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch {
    return value;
  }
}

interface FieldRowProps {
  label: string;
  value: ReactNode;
}

function FieldRow({ label, value }: FieldRowProps) {
  return (
    <div className="flex items-start justify-between gap-4 py-2 border-b border-border/40 last:border-0">
      <span className="text-muted text-xs uppercase tracking-wider font-semibold flex-shrink-0 pt-0.5">{label}</span>
      <span className="text-ink text-sm text-right">{value || '—'}</span>
    </div>
  );
}

interface SectionProps {
  title: string;
  children: ReactNode;
}

function Section({ title, children }: SectionProps) {
  return (
    <div className="mb-5">
      <div className="text-gold text-xs uppercase tracking-widest font-semibold mb-2 pb-1 border-b border-border/60">
        {title}
      </div>
      {children}
    </div>
  );
}

export function DetailSheet({ item, onClose }: DetailSheetProps) {
  const isOpen = item !== null;

  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40"
          onClick={onClose}
        />
      )}

      {/* Panel */}
      <div
        className={`fixed right-0 top-0 h-full w-96 flex flex-col z-50 border-l border-border transition-transform duration-200 ${
          isOpen ? 'translate-x-0' : 'translate-x-full'
        }`}
        style={{ backgroundColor: '#060709' }}
      >
        {item && (
          <>
            {/* Header */}
            <div className="flex items-start justify-between px-5 py-4 border-b border-border flex-shrink-0">
              <div className="min-w-0 flex-1 pr-3">
                <h2 className="text-ink font-bold text-base leading-tight">{item.knife_name}</h2>
                {item.nickname && (
                  <p className="text-muted text-sm mt-0.5">"{item.nickname}"</p>
                )}
              </div>
              <button
                onClick={onClose}
                className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30 flex-shrink-0"
                aria-label="Close detail"
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="18" y1="6" x2="6" y2="18" />
                  <line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>

            {/* Image */}
            {imageUrl(item) && (
              <div className="flex-shrink-0 w-full" style={{ aspectRatio: '16/9' }}>
                <img
                  src={imageUrl(item)!}
                  alt={item.knife_name}
                  className="w-full h-full object-cover"
                  onError={(e) => { e.currentTarget.parentElement!.style.display = 'none'; }}
                />
              </div>
            )}

            {/* Content */}
            <div className="flex-1 overflow-y-auto px-5 py-4">
              <Section title="Identity">
                <FieldRow label="Model ID" value={`#${item.knife_model_id}`} />
                <FieldRow label="Type" value={item.knife_type} />
                <FieldRow label="Family" value={item.knife_family} />
                <FieldRow label="Form" value={item.form_name} />
                {item.is_collab && (
                  <FieldRow label="Collab" value={item.collaboration_name} />
                )}
              </Section>

              <Section title="Specs">
                <FieldRow label="Handle Color" value={item.handle_color} />
                <FieldRow label="Blade Steel" value={item.blade_steel} />
                <FieldRow label="Blade Finish" value={item.blade_finish} />
                <FieldRow label="Blade Color" value={item.blade_color} />
                <FieldRow label="Blade Length" value={item.blade_length != null ? `${item.blade_length}"` : null} />
              </Section>

              <Section title="Acquisition">
                <FieldRow label="Purchase Price" value={formatCurrency(item.purchase_price)} />
                <FieldRow label="Est. Value" value={formatCurrency(item.estimated_value)} />
                <FieldRow label="Acquired" value={formatDate(item.acquired_date)} />
                <FieldRow label="Order #" value={item.mkc_order_number} />
                <FieldRow label="Source" value={item.purchase_source} />
              </Section>

              <Section title="Condition &amp; Storage">
                <FieldRow label="Condition" value={item.condition} />
                <FieldRow label="Quantity" value={item.quantity.toString()} />
                <FieldRow label="Location" value={item.location} />
                <FieldRow label="Last Sharpened" value={formatDate(item.last_sharpened)} />
              </Section>

              {item.notes && (
                <Section title="Notes">
                  <p className="text-ink text-sm leading-relaxed">{item.notes}</p>
                </Section>
              )}

              {/* Footer action */}
              <div className="mt-4 pt-4 border-t border-border/40">
                <a
                  href="#"
                  onClick={(e) => e.preventDefault()}
                  className="text-gold text-sm hover:text-gold-bright transition-colors"
                >
                  Edit in legacy form →
                </a>
              </div>
            </div>
          </>
        )}
      </div>
    </>
  );
}
