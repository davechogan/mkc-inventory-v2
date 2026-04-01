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

function IconMenu() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="3" y1="6" x2="21" y2="6" /><line x1="3" y1="12" x2="21" y2="12" /><line x1="3" y1="18" x2="21" y2="18" />
    </svg>
  );
}

interface NavItem {
  label: string;
  href: string;
  icon: ReactNode;
  active: boolean;
}

interface TenantMembership {
  tenant_id: string;
  tenant_name: string;
  role: string;
}

interface AuthUser {
  email: string;
  name: string | null;
}

export function Sidebar() {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    return localStorage.getItem(STORAGE_KEY) === 'true';
  });
  const [mobileOpen, setMobileOpen] = useState(false);
  const [user, setUser] = useState<AuthUser | null>(null);
  const [memberships, setMemberships] = useState<TenantMembership[]>([]);
  const [activeTenant, setActiveTenantState] = useState<string | null>(null);

  useEffect(() => {
    import('../tenantContext').then(({ getActiveTenantId, setActiveTenantId }) => {
      fetch('/api/v2/me')
        .then(r => r.json())
        .then(d => {
          if (d.authenticated) {
            setUser(d.user as AuthUser);
            setMemberships(d.memberships ?? []);
            // Set active tenant from localStorage or first membership
            const saved = getActiveTenantId();
            const validSaved = (d.memberships ?? []).find((m: TenantMembership) => m.tenant_id === saved);
            if (validSaved) {
              setActiveTenantState(saved);
            } else if (d.memberships?.length > 0) {
              const first = d.memberships[0].tenant_id;
              setActiveTenantId(first);
              setActiveTenantState(first);
            }
          }
        })
        .catch(() => {});
    });
  }, []);

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

  const handleTenantChange = (tenantId: string) => {
    import('../tenantContext').then(({ setActiveTenantId }) => {
      setActiveTenantId(tenantId);
      setActiveTenantState(tenantId);
      window.location.reload(); // reload to apply new tenant scope
    });
  };

  const roleColor = (role: string) => {
    if (role === 'owner') return 'text-gold';
    if (role === 'editor') return 'text-blue-400';
    return 'text-muted';
  };

  // Tenant picker (shared between desktop and mobile)
  const tenantPicker = (showFull: boolean) => memberships.length > 0 && (
    <div className={`border-b border-border flex-shrink-0 ${showFull ? 'px-3 py-2' : 'px-1 py-2 flex justify-center'}`}>
      {showFull ? (
        memberships.length === 1 ? (
          <div className="flex items-center gap-2 px-1">
            <span className={`text-[10px] font-bold uppercase ${roleColor(memberships[0].role)}`}>{memberships[0].role}</span>
            <span className="text-ink text-xs truncate">{memberships[0].tenant_name}</span>
          </div>
        ) : (
          <select
            value={activeTenant ?? ''}
            onChange={e => handleTenantChange(e.target.value)}
            className="w-full px-2 py-1.5 bg-surface border border-border rounded-lg text-xs text-ink focus:outline-none focus:border-gold/60 transition-colors"
          >
            {memberships.map(m => (
              <option key={m.tenant_id} value={m.tenant_id}>
                {m.tenant_name} ({m.role})
              </option>
            ))}
          </select>
        )
      ) : (
        <div className="w-7 h-7 rounded-full bg-border/40 flex items-center justify-center text-[10px] text-muted font-bold"
          title={memberships.find(m => m.tenant_id === activeTenant)?.tenant_name ?? ''}>
          {(memberships.find(m => m.tenant_id === activeTenant)?.tenant_name ?? '?')[0].toUpperCase()}
        </div>
      )}
    </div>
  );

  // Nav link content (shared between desktop and mobile)
  const navContent = (onNavigate?: () => void) => (
    <nav className="flex-1 px-2 py-3 flex flex-col gap-1">
      {navItems.map((item) => (
        <a
          key={item.href}
          href={item.href}
          title={item.label}
          onClick={onNavigate}
          className={`flex items-center gap-3 px-3 py-3 md:px-2 md:py-2.5 rounded-lg transition-colors relative group ${
            item.active
              ? 'text-ink bg-gold/8 border-l-2 border-gold pl-[10px] md:pl-[6px]'
              : 'text-muted hover:text-ink hover:bg-border/30 border-l-2 border-transparent'
          }`}
        >
          <span className="flex-shrink-0">{item.icon}</span>
          {(!collapsed || mobileOpen) && (
            <span className="text-sm font-medium truncate">{item.label}</span>
          )}
          {collapsed && !mobileOpen && (
            <span className="absolute left-full ml-2 px-2 py-1 bg-card border border-border rounded-md text-xs text-ink whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50">
              {item.label}
            </span>
          )}
        </a>
      ))}
    </nav>
  );

  const userSection = (showFull: boolean) => user && (
    <div className={`border-t border-border px-3 py-3 flex-shrink-0 ${!showFull ? 'flex justify-center' : ''}`}>
      {showFull ? (
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-full bg-gold/20 flex items-center justify-center text-gold text-xs font-bold flex-shrink-0">
            {user.email[0].toUpperCase()}
          </div>
          <div className="min-w-0">
            <div className="text-ink text-xs font-medium truncate">{user.name ?? user.email.split('@')[0]}</div>
            <div className="text-muted text-[10px] truncate">{user.email}</div>
          </div>
        </div>
      ) : (
        <div className="w-7 h-7 rounded-full bg-gold/20 flex items-center justify-center text-gold text-xs font-bold" title={user.email}>
          {user.email[0].toUpperCase()}
        </div>
      )}
    </div>
  );

  return (
    <>
      {/* Mobile hamburger button — fixed top-left, visible only on small screens */}
      <button
        onClick={() => setMobileOpen(true)}
        className="fixed top-3 left-3 z-50 md:hidden p-2 rounded-lg bg-card border border-border text-muted hover:text-ink transition-colors"
        aria-label="Open menu"
      >
        <IconMenu />
      </button>

      {/* Mobile overlay */}
      {mobileOpen && (
        <div className="fixed inset-0 bg-black/60 z-50 md:hidden" onClick={() => setMobileOpen(false)}>
          <aside
            className="w-64 h-full flex flex-col"
            style={{ backgroundColor: '#060709' }}
            onClick={(e) => e.stopPropagation()}
          >
            {/* Mobile header */}
            <div className="flex items-center justify-between px-4 pt-5 pb-4 border-b border-border flex-shrink-0">
              <div className="flex items-center gap-3">
                <img src="/static/logo.png" alt="MKC" className="w-10 h-10 object-contain" />
                <div>
                  <div className="text-ink font-bold text-sm">MKC</div>
                  <div className="text-muted text-[10px] uppercase tracking-widest">Collection</div>
                </div>
              </div>
              <button onClick={() => setMobileOpen(false)} className="text-muted hover:text-ink p-1">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>
            {tenantPicker(true)}
            {navContent(() => setMobileOpen(false))}
            {userSection(true)}
          </aside>
        </div>
      )}

      {/* Desktop sidebar — hidden on mobile */}
      <aside
        className={`fixed left-0 top-0 h-full flex-col z-40 transition-all duration-200 hidden md:flex ${
          collapsed ? 'w-16' : 'w-56'
        }`}
        style={{ backgroundColor: '#060709', borderRight: '1px solid #1d2329' }}
      >
        {collapsed ? (
          <div className="flex flex-col items-center pt-4 pb-3 border-b border-border flex-shrink-0 gap-1.5">
            <img src="/static/logo.png" alt="MKC" className="w-10 h-10 object-contain" />
            <span className="text-gold text-xs font-bold tracking-widest">MKC</span>
            <button onClick={toggle} title="Expand sidebar" className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30">
              <IconChevronRight />
            </button>
          </div>
        ) : (
          <div className="relative flex flex-col items-center px-4 pt-6 pb-4 border-b border-border flex-shrink-0 gap-2">
            <img src="/static/logo.png" alt="MKC Logo" className="w-24 h-24 object-contain" />
            <div className="text-center">
              <div className="text-ink font-bold text-sm leading-tight tracking-wide">Montana Knife Company</div>
              <div className="text-muted text-xs tracking-widest uppercase mt-0.5">Collection</div>
            </div>
            <button onClick={toggle} title="Collapse sidebar" className="absolute top-3 right-3 text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30">
              <IconChevronLeft />
            </button>
          </div>
        )}
        {tenantPicker(!collapsed)}
        {navContent()}
        {userSection(!collapsed)}
      </aside>
    </>
  );
}
