/**
 * AuthGate — wraps the root route (/) to handle:
 * 1. Unauthenticated → Landing page
 * 2. Authenticated, no memberships → Onboarding
 * 3. Authenticated, has memberships → Collection (App)
 */

import { useState, useEffect, lazy, Suspense } from 'react';
import { setActiveTenantId, getActiveTenantId } from './tenantContext';

const Landing = lazy(() => import('./pages/Landing'));
const Onboarding = lazy(() => import('./pages/Onboarding'));
const App = lazy(() => import('./App'));

type AuthState = 'loading' | 'unauthenticated' | 'needs_onboarding' | 'ready';

interface MeResponse {
  authenticated: boolean;
  user: { id: string; email: string; name: string | null } | null;
  memberships: { tenant_id: string; tenant_name: string; role: string }[];
  is_new: boolean;
  needs_onboarding: boolean;
}

export default function AuthGate() {
  const [state, setState] = useState<AuthState>('loading');
  const [email, setEmail] = useState('');

  const checkAuth = () => {
    fetch('/api/v2/me')
      .then(r => r.json())
      .then((d: MeResponse) => {
        if (!d.authenticated) {
          setState('unauthenticated');
        } else if (d.needs_onboarding) {
          setEmail(d.user?.email ?? '');
          setState('needs_onboarding');
        } else {
          // Set active tenant if not already set
          const saved = getActiveTenantId();
          const valid = d.memberships.find(m => m.tenant_id === saved);
          if (!valid && d.memberships.length > 0) {
            setActiveTenantId(d.memberships[0].tenant_id);
          }
          setState('ready');
        }
      })
      .catch(() => {
        // If /me fails (no auth middleware), assume dev mode — go straight to app
        setState('ready');
      });
  };

  useEffect(() => { checkAuth(); }, []);

  if (state === 'loading') {
    return (
      <div className="min-h-screen bg-surface flex items-center justify-center">
        <div className="text-muted text-sm">Loading...</div>
      </div>
    );
  }

  return (
    <Suspense fallback={null}>
      {state === 'unauthenticated' && <Landing />}
      {state === 'needs_onboarding' && (
        <Onboarding userEmail={email} onComplete={() => window.location.reload()} />
      )}
      {state === 'ready' && <App />}
    </Suspense>
  );
}
