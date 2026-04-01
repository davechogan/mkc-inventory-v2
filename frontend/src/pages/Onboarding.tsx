/**
 * Onboarding page — shown to authenticated users with no tenant memberships.
 * Lets them create their own collection.
 */

import { useState } from 'react';

interface OnboardingProps {
  userEmail: string;
  onComplete: () => void;
}

export default function Onboarding({ userEmail, onComplete }: OnboardingProps) {
  const [name, setName] = useState('My Collection');
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleCreate = async () => {
    const trimmed = name.trim();
    if (!trimmed) return;
    setCreating(true);
    setError(null);
    try {
      const res = await fetch('/api/v2/tenants', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: trimmed }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error((err as { detail?: string }).detail ?? `Error ${res.status}`);
      }
      const data = await res.json() as { tenant_id: string };
      // Set the new tenant as active and reload
      const { setActiveTenantId } = await import('../tenantContext');
      setActiveTenantId(data.tenant_id);
      onComplete();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to create collection');
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="min-h-screen bg-surface flex flex-col items-center justify-center px-6">
      <div className="max-w-md w-full">
        <div className="text-center mb-8">
          <div className="w-16 h-16 rounded-full overflow-hidden ring-2 ring-gold/30 mx-auto mb-4 bg-surface">
            <img src="/static/logo.png" alt="MKC" className="w-full h-full object-cover" />
          </div>
          <h1 className="text-ink text-2xl font-bold mb-2">Welcome!</h1>
          <p className="text-muted text-sm">
            Signed in as <span className="text-ink">{userEmail}</span>
          </p>
        </div>

        <div className="bg-card border border-border rounded-xl p-6">
          <h2 className="text-ink font-semibold mb-4">Create your collection</h2>
          <p className="text-muted text-sm mb-4">
            Name your knife collection. You can change this later.
          </p>

          <input
            type="text"
            value={name}
            onChange={e => setName(e.target.value)}
            placeholder="My Collection"
            className="w-full px-4 py-2.5 bg-surface border border-border rounded-lg text-sm text-ink placeholder:text-muted focus:outline-none focus:border-gold/60 transition-colors mb-4"
          />

          {error && (
            <div className="px-3 py-2 rounded-lg bg-red-950/40 border border-red-800/50 text-red-300 text-xs mb-4">
              {error}
            </div>
          )}

          <button
            onClick={handleCreate}
            disabled={!name.trim() || creating}
            className="w-full py-2.5 rounded-lg bg-gold text-black font-semibold text-sm hover:bg-gold-bright disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {creating ? 'Creating...' : 'Create Collection'}
          </button>
        </div>

        <p className="text-muted text-xs text-center mt-6">
          Were you invited to view someone else's collection?<br />
          Ask them to send an invitation to <span className="text-ink">{userEmail}</span>
        </p>
      </div>
    </div>
  );
}
