/**
 * Public landing page — shown to unauthenticated users.
 * No sidebar, no tenant context. Just the app description and a Sign In button.
 */

export default function Landing() {
  // Link to a protected path — Cloudflare Access intercepts it and forces login.
  // After authentication, Cloudflare redirects back to this path, which serves
  // the React app. The AuthGate then checks /api/v2/me and routes accordingly.
  const signInUrl = '/auth/login';

  return (
    <div className="min-h-screen bg-surface flex flex-col">
      {/* Header */}
      <header className="flex items-center justify-between px-6 md:px-12 py-4">
        <div className="flex items-center gap-3">
          <img src="/static/logo.png" alt="MKC" className="w-10 h-10 object-contain" />
          <span className="text-gold font-bold text-lg tracking-wide">MKC Collection</span>
        </div>
        <a
          href={signInUrl}
          className="px-5 py-2 rounded-lg bg-gold text-black font-semibold text-sm hover:bg-gold-bright transition-colors"
        >
          Sign In
        </a>
      </header>

      {/* Hero */}
      <main className="flex-1 flex flex-col items-center justify-center px-6 text-center">
        <div className="max-w-2xl mx-auto">
          <div className="w-24 h-24 rounded-full overflow-hidden ring-4 ring-gold/20 mx-auto mb-8">
            <img src="/static/logo.png" alt="MKC" className="w-full h-full object-cover" />
          </div>

          <h1 className="text-ink text-3xl md:text-5xl font-bold leading-tight mb-4">
            Your Knife Collection,<br />
            <span className="text-gold">Organized</span>
          </h1>

          <p className="text-muted text-lg md:text-xl mb-8 leading-relaxed max-w-lg mx-auto">
            Track your Montana Knife Company collection. Browse the catalog.
            Get AI-powered insights about what you own.
          </p>

          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <a
              href={signInUrl}
              className="px-8 py-3 rounded-xl bg-gold text-black font-bold text-base hover:bg-gold-bright transition-colors"
            >
              Get Started
            </a>
          </div>

          {/* Features */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mt-16 text-left">
            <div className="bg-card border border-border rounded-xl p-5">
              <div className="text-gold text-2xl mb-2">📋</div>
              <h3 className="text-ink font-semibold mb-1">Track Your Collection</h3>
              <p className="text-muted text-sm">Every knife, colorway, and purchase — organized and searchable.</p>
            </div>
            <div className="bg-card border border-border rounded-xl p-5">
              <div className="text-gold text-2xl mb-2">📊</div>
              <h3 className="text-ink font-semibold mb-1">Smart Reporting</h3>
              <p className="text-muted text-sm">Ask questions in plain English. Get charts, tables, and insights.</p>
            </div>
            <div className="bg-card border border-border rounded-xl p-5">
              <div className="text-gold text-2xl mb-2">🤝</div>
              <h3 className="text-ink font-semibold mb-1">Share Collections</h3>
              <p className="text-muted text-sm">Invite friends and family to view or help manage your collection.</p>
            </div>
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="py-6 text-center text-muted text-xs">
        Montana Knife Company Collection Manager
      </footer>
    </div>
  );
}
