import { useState, useEffect, type ReactNode } from 'react';

const STORAGE_KEY = 'mkc_sidebar_collapsed';

function IconSearch() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
    </svg>
  );
}

function IconBook() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
      <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z" />
    </svg>
  );
}

function IconBarChart() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="18" y1="20" x2="18" y2="10" />
      <line x1="12" y1="20" x2="12" y2="4" />
      <line x1="6" y1="20" x2="6" y2="14" />
      <line x1="2" y1="20" x2="22" y2="20" />
    </svg>
  );
}

function IconGrid() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="7" height="7" />
      <rect x="14" y="3" width="7" height="7" />
      <rect x="3" y="14" width="7" height="7" />
      <rect x="14" y="14" width="7" height="7" />
    </svg>
  );
}

function IconChevronLeft() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="15 18 9 12 15 6" />
    </svg>
  );
}

function IconChevronRight() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="9 18 15 12 9 6" />
    </svg>
  );
}

interface NavItem {
  label: string;
  href: string;
  icon: ReactNode;
  active: boolean;
}

export function Sidebar() {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    return localStorage.getItem(STORAGE_KEY) === 'true';
  });

  const currentPath = window.location.pathname;

  const navItems: NavItem[] = [
    { label: 'Collection', href: '/', icon: <IconGrid />, active: currentPath === '/' || currentPath === '' },
    { label: 'Identify', href: '/identify', icon: <IconSearch />, active: currentPath === '/identify' },
    { label: 'Catalog', href: '/master', icon: <IconBook />, active: currentPath === '/master' },
    { label: 'Reporting', href: '/reporting', icon: <IconBarChart />, active: currentPath === '/reporting' },
  ];

  const toggle = () => {
    const next = !collapsed;
    setCollapsed(next);
    localStorage.setItem(STORAGE_KEY, String(next));
    window.dispatchEvent(new CustomEvent('mkc-sidebar-toggle', { detail: { collapsed: next } }));
  };

  useEffect(() => {
    window.dispatchEvent(new CustomEvent('mkc-sidebar-toggle', { detail: { collapsed } }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <aside
      className={`fixed left-0 top-0 h-full flex flex-col z-50 transition-all duration-200 ${
        collapsed ? 'w-16' : 'w-56'
      }`}
      style={{ backgroundColor: '#060709', borderRight: '1px solid #1d2329' }}
    >
      {collapsed ? (
        /* ── Collapsed header ── */
        <div className="flex flex-col items-center pt-4 pb-3 border-b border-border flex-shrink-0 gap-2">
          <img src="/static/logo.png" alt="MKC" className="w-10 h-10 object-contain" />
          <button
            onClick={toggle}
            title="Expand sidebar"
            className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30"
          >
            <IconChevronRight />
          </button>
        </div>
      ) : (
        /* ── Expanded header ── */
        <div className="relative flex flex-col items-center px-4 pt-6 pb-4 border-b border-border flex-shrink-0 gap-2">
          <img src="/static/logo.png" alt="MKC Logo" className="w-24 h-24 object-contain" />
          <div className="text-center">
            <div className="text-ink font-bold text-sm leading-tight tracking-wide">Mountain Knife Co.</div>
            <div className="text-muted text-xs tracking-widest uppercase mt-0.5">Collection</div>
          </div>
          <button
            onClick={toggle}
            title="Collapse sidebar"
            className="absolute top-3 right-3 text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30"
          >
            <IconChevronLeft />
          </button>
        </div>
      )}

      {/* Nav */}
      <nav className="flex-1 px-2 py-3 flex flex-col gap-1">
        {navItems.map((item) => (
          <a
            key={item.href}
            href={item.href}
            title={item.label}
            className={`flex items-center gap-3 px-2 py-2.5 rounded-lg transition-colors relative group ${
              item.active
                ? 'text-ink bg-gold/8 border-l-2 border-gold pl-[6px]'
                : 'text-muted hover:text-ink hover:bg-border/30 border-l-2 border-transparent'
            }`}
          >
            <span className="flex-shrink-0">{item.icon}</span>
            {!collapsed && (
              <span className="text-sm font-medium truncate">{item.label}</span>
            )}
            {collapsed && (
              <span className="absolute left-full ml-2 px-2 py-1 bg-card border border-border rounded-md text-xs text-ink whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50">
                {item.label}
              </span>
            )}
          </a>
        ))}
      </nav>
    </aside>
  );
}
