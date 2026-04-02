/**
 * AuthGate — handles auth routing:
 * 1. Unauthenticated → Landing page (Phase 5: app-native login form)
 * 2. Authenticated, no memberships → Onboarding
 * 3. Authenticated, has memberships → Collection (App)
 *
 * Currently used at both / and /collection. With Cloudflare Access active,
 * all visitors are pre-authenticated so state 1 only triggers in dev mode
 * or after Phase 5 (app-native auth).
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

  const isProd = window.location.hostname === 'inventory.davechogan.com';

  const checkAuth = () => {
    fetch('/api/v2/me')
      .then(r => {
        // If Cloudflare blocks the request (user not authenticated), it returns
        // a redirect or HTML page instead of JSON. Detect this and show landing.
        const ct = r.headers.get('content-type') || '';
        if (!ct.includes('application/json')) {
          setState(isProd ? 'unauthenticated' : 'ready');
          return;
        }
        return r.json();
      })
      .then((d: MeResponse | undefined) => {
        if (!d) return; // handled above
        if (!d.authenticated) {
          // No Cloudflare headers — in dev/LAN mode, skip auth and go straight to app.
          // In prod (behind Cloudflare), this shouldn't happen, but show landing if it does.
          setState(isProd ? 'unauthenticated' : 'ready');
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
        // If /me fails entirely (network error, CORS block, etc.):
        // Dev/LAN: go straight to app. Prod: show landing as safe fallback.
        setState(isProd ? 'unauthenticated' : 'ready');
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
