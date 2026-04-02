import { StrictMode, lazy, Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import { getActiveTenantId } from './tenantContext';
import AuthGate from './AuthGate';

// Intercept all fetch calls to add X-Tenant-Id header for tenant-scoped API calls
const _origFetch = window.fetch;
window.fetch = function (input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const tid = getActiveTenantId();
  if (tid) {
    const headers = new Headers(init?.headers);
    if (!headers.has('X-Tenant-Id')) {
      headers.set('X-Tenant-Id', tid);
    }
    return _origFetch(input, { ...init, headers });
  }
  return _origFetch(input, init);
};

const Landing = lazy(() => import('./pages/Landing'));
const Identify = lazy(() => import('./pages/Identify'));
const Catalog = lazy(() => import('./pages/Catalog'));
const Reporting = lazy(() => import('./pages/Reporting'));
const Admin = lazy(() => import('./pages/Admin'));

const rootEl = document.getElementById('root');
if (!rootEl) throw new Error('Root element not found');

const path = window.location.pathname;

let Page: React.ComponentType;
if (path === '/identify') {
  Page = Identify;
} else if (path === '/master') {
  Page = Catalog;
} else if (path === '/reporting') {
  Page = Reporting;
} else if (path === '/admin') {
  Page = Admin;
} else if (path === '/collection') {
  Page = AuthGate;
} else {
  // Root (/) — AuthGate handles auth check.
  // When Cloudflare Access is active, all visitors are already authenticated.
  // When we move to app-native auth (Phase 5), AuthGate will show Landing for
  // unauthenticated users and the /collection route takes over.
  Page = AuthGate;
}

createRoot(rootEl).render(
  <StrictMode>
    <Suspense fallback={null}>
      <Page />
    </Suspense>
  </StrictMode>
);
