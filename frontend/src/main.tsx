import { StrictMode, lazy, Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './App';

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
} else {
  Page = App;
}

createRoot(rootEl).render(
  <StrictMode>
    <Suspense fallback={null}>
      <Page />
    </Suspense>
  </StrictMode>
);
