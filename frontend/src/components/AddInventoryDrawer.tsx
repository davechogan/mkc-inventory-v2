import { useState, useEffect, useRef, useCallback } from 'react';

// ── Types ─────────────────────────────────────────────────────────────────────

interface ModelResult {
  id: number;
  official_name: string;
  family_name: string | null;
  series_name: string | null;
  steel: string | null;
  blade_length: number | null;
}

interface ColorwayOption {
  id: number;
  handle_color: string;
  blade_color: string | null;
}

interface LocationOption {
  id: number;
  name: string;
}

interface AddInventoryDrawerProps {
  open: boolean;
  onClose: () => void;
  onAdded: () => void;
}

// ── API ───────────────────────────────────────────────────────────────────────

async function searchModels(q: string): Promise<ModelResult[]> {
  const params = new URLSearchParams({ q, limit: '12' });
  const res = await fetch(`/api/v2/models/search?${params.toString()}`);
  if (!res.ok) return [];
  return res.json();
}

async function createInventoryItem(body: Record<string, unknown>): Promise<void> {
  const res = await fetch('/api/v2/inventory', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail ?? `API ${res.status}`);
  }
}

// ── Component ─────────────────────────────────────────────────────────────────

const emptyForm = {
  purchase_price: '',
  acquired_date: '',
  mkc_order_number: '',
  quantity: '1',
  colorway_id: '',
  location_id: '',
  notes: '',
};

export function AddInventoryDrawer({ open, onClose, onAdded }: AddInventoryDrawerProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<ModelResult[]>([]);
  const [searching, setSearching] = useState(false);
  const [selectedModel, setSelectedModel] = useState<ModelResult | null>(null);
  const [form, setForm] = useState(emptyForm);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Colorway options for the selected model
  const [colorways, setColorways] = useState<ColorwayOption[]>([]);

  // Location options
  const [locations, setLocations] = useState<LocationOption[]>([]);

  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Load locations once
  useEffect(() => {
    fetch('/api/v2/options')
      .then(r => r.json())
      .then(d => {
        const data = d as Record<string, { id: number; name: string }[]>;
        setLocations(data['locations'] ?? []);
      })
      .catch(() => {});
  }, []);

  // Focus search input when opened
  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 50);
    } else {
      // Reset on close
      setQuery('');
      setResults([]);
      setSelectedModel(null);
      setForm(emptyForm);
      setError(null);
      setColorways([]);
    }
  }, [open]);

  // Debounced search
  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      return;
    }
    if (searchTimer.current) clearTimeout(searchTimer.current);
    setSearching(true);
    searchTimer.current = setTimeout(async () => {
      const rows = await searchModels(query);
      setResults(rows);
      setSearching(false);
    }, 250);
    return () => {
      if (searchTimer.current) clearTimeout(searchTimer.current);
    };
  }, [query]);

  // Fetch colorways when model is selected
  const fetchColorways = useCallback(async (modelId: number) => {
    try {
      const res = await fetch(`/api/v2/models/${modelId}/colorways`);
      if (res.ok) {
        const data = await res.json() as ColorwayOption[];
        setColorways(data);
      }
    } catch {
      setColorways([]);
    }
  }, []);

  const handleSelect = useCallback((model: ModelResult) => {
    setSelectedModel(model);
    setQuery('');
    setResults([]);
    setForm(prev => ({ ...prev, colorway_id: '' }));
    fetchColorways(model.id);
  }, [fetchColorways]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedModel) return;

    setSaving(true);
    setError(null);

    const body: Record<string, unknown> = {
      knife_model_id: selectedModel.id,
      quantity: Math.max(1, parseInt(form.quantity, 10) || 1),
    };
    if (form.colorway_id) body.colorway_id = parseInt(form.colorway_id, 10);
    if (form.purchase_price) body.purchase_price = parseFloat(form.purchase_price);
    if (form.acquired_date) body.acquired_date = form.acquired_date;
    if (form.mkc_order_number.trim()) body.mkc_order_number = form.mkc_order_number.trim();
    if (form.location_id) body.location_id = parseInt(form.location_id, 10);
    if (form.notes.trim()) body.notes = form.notes.trim();

    try {
      await createInventoryItem(body);
      onAdded();
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to add knife');
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      {/* Backdrop */}
      {open && (
        <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      )}

      {/* Panel */}
      <div
        className={`fixed right-0 top-0 h-full w-96 flex flex-col z-50 border-l border-border transition-transform duration-200 ${
          open ? 'translate-x-0' : 'translate-x-full'
        }`}
        style={{ backgroundColor: '#060709' }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-border flex-shrink-0">
          <h2 className="text-ink font-bold text-base">Add to Collection</h2>
          <button
            onClick={onClose}
            className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30"
          >
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-5 py-5">
          <form onSubmit={handleSubmit} className="flex flex-col gap-5">

            {/* Model search */}
            <div>
              <label className="block text-muted text-xs uppercase tracking-wider mb-2">
                Knife Model <span className="text-gold">*</span>
              </label>

              {selectedModel ? (
                // Selected model card
                <div className="flex items-start justify-between gap-2 p-3 rounded-xl border border-gold/40 bg-gold/5">
                  <div className="min-w-0">
                    <div className="text-ink text-sm font-semibold leading-tight">{selectedModel.official_name}</div>
                    <div className="text-muted text-xs mt-0.5">
                      {[selectedModel.family_name, selectedModel.steel, selectedModel.blade_length ? `${selectedModel.blade_length}"` : null]
                        .filter(Boolean).join(' · ')}
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => { setSelectedModel(null); setColorways([]); }}
                    className="text-muted hover:text-ink transition-colors flex-shrink-0 text-xs px-2 py-1 rounded border border-border hover:border-border/70"
                  >
                    Change
                  </button>
                </div>
              ) : (
                // Search input + results
                <div className="relative">
                  <input
                    ref={inputRef}
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search by name, family, series…"
                    className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors"
                  />
                  {(results.length > 0 || searching) && (
                    <div className="absolute top-full left-0 right-0 mt-1 bg-card border border-border rounded-xl shadow-xl overflow-hidden z-10 max-h-64 overflow-y-auto">
                      {searching && results.length === 0 && (
                        <div className="px-4 py-3 text-muted text-xs">Searching…</div>
                      )}
                      {results.map((m) => (
                        <button
                          key={m.id}
                          type="button"
                          onClick={() => handleSelect(m)}
                          className="w-full text-left px-4 py-3 hover:bg-border/30 transition-colors border-b border-border/40 last:border-0"
                        >
                          <div className="text-ink text-sm font-medium leading-tight">{m.official_name}</div>
                          <div className="text-muted text-xs mt-0.5">
                            {[m.family_name, m.steel, m.blade_length ? `${m.blade_length}"` : null]
                              .filter(Boolean).join(' · ')}
                          </div>
                        </button>
                      ))}
                    </div>
                  )}
                  {query.trim() && !searching && results.length === 0 && (
                    <div className="absolute top-full left-0 right-0 mt-1 bg-card border border-border rounded-xl px-4 py-3 text-muted text-xs z-10">
                      No models found for "{query}"
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Fields — only shown after model is selected */}
            {selectedModel && (
              <>
                <div className="h-px bg-border/40" />

                {/* Colorway dropdown */}
                {colorways.length > 0 && (
                  <div>
                    <label className="block text-muted text-xs uppercase tracking-wider mb-2">Colorway</label>
                    <select
                      value={form.colorway_id}
                      onChange={(e) => setForm((f) => ({ ...f, colorway_id: e.target.value }))}
                      className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                    >
                      <option value="">— Select colorway —</option>
                      {colorways.map(cw => (
                        <option key={cw.id} value={cw.id}>
                          {cw.handle_color}{cw.blade_color ? ` / ${cw.blade_color}` : ''}
                        </option>
                      ))}
                    </select>
                  </div>
                )}

                {/* Purchase price */}
                <div>
                  <label className="block text-muted text-xs uppercase tracking-wider mb-2">Purchase Price</label>
                  <div className="relative">
                    <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted text-sm">$</span>
                    <input
                      type="number"
                      step="0.01"
                      min="0"
                      placeholder="0.00"
                      value={form.purchase_price}
                      onChange={(e) => setForm((f) => ({ ...f, purchase_price: e.target.value }))}
                      className="w-full pl-7 pr-3 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors"
                    />
                  </div>
                </div>

                {/* Date acquired */}
                <div>
                  <label className="block text-muted text-xs uppercase tracking-wider mb-2">Date Acquired</label>
                  <input
                    type="date"
                    value={form.acquired_date}
                    onChange={(e) => setForm((f) => ({ ...f, acquired_date: e.target.value }))}
                    className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                  />
                </div>

                {/* Quantity */}
                <div>
                  <label className="block text-muted text-xs uppercase tracking-wider mb-2">Quantity</label>
                  <input
                    type="number"
                    min="1"
                    value={form.quantity}
                    onChange={(e) => setForm((f) => ({ ...f, quantity: e.target.value }))}
                    className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                  />
                </div>

                {/* MKC order number */}
                <div>
                  <label className="block text-muted text-xs uppercase tracking-wider mb-2">MKC Order #</label>
                  <input
                    type="text"
                    placeholder="Optional"
                    value={form.mkc_order_number}
                    onChange={(e) => setForm((f) => ({ ...f, mkc_order_number: e.target.value }))}
                    className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors"
                  />
                </div>

                {/* Location */}
                {locations.length > 0 && (
                  <div>
                    <label className="block text-muted text-xs uppercase tracking-wider mb-2">Location</label>
                    <select
                      value={form.location_id}
                      onChange={(e) => setForm((f) => ({ ...f, location_id: e.target.value }))}
                      className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink focus:outline-none focus:border-gold/60 transition-colors"
                    >
                      <option value="">— None —</option>
                      {locations.map(loc => (
                        <option key={loc.id} value={loc.id}>{loc.name}</option>
                      ))}
                    </select>
                  </div>
                )}

                {/* Notes */}
                <div>
                  <label className="block text-muted text-xs uppercase tracking-wider mb-2">Notes</label>
                  <textarea
                    rows={3}
                    placeholder="Optional"
                    value={form.notes}
                    onChange={(e) => setForm((f) => ({ ...f, notes: e.target.value }))}
                    className="w-full px-3 py-2 bg-card border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors resize-none"
                  />
                </div>
              </>
            )}

            {/* Error */}
            {error && (
              <div className="px-3 py-2 rounded-lg bg-red-950/40 border border-red-800/50 text-red-300 text-xs">
                {error}
              </div>
            )}

            {/* Actions */}
            <div className="flex gap-2 pt-1">
              <button
                type="submit"
                disabled={!selectedModel || saving}
                className="flex-1 py-2.5 px-4 rounded-lg bg-gold text-black text-sm font-semibold hover:bg-gold-bright disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                {saving ? 'Adding…' : 'Add to Collection'}
              </button>
              <button
                type="button"
                onClick={onClose}
                className="py-2.5 px-4 rounded-lg border border-border text-muted text-sm hover:text-ink hover:border-border/70 transition-colors"
              >
                Cancel
              </button>
            </div>
          </form>
        </div>
      </div>
    </>
  );
}
