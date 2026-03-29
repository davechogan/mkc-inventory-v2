# MKC Inventory — UI/UX Redesign Plan
*Created: 2026-03-28 | Status: Approved, in progress*

---

## Design Vision

**Theme: "Dark Collector's Cabinet"**

A premium, editorial dark-mode interface inspired by high-end collector apps (Grailed, StockX, Leica). Product imagery is hero. White space makes items feel curated, not catalogued. Typography is intentional and hierarchical. Orange accent (#d47c1c) is reserved and purposeful.

**Keywords:** Modern · Professional · Hip · Clean · Collector-first · Uncluttered

---

## Design Tokens (CSS Custom Properties)

```css
--bg: #0f1215              /* page background */
--sidebar-bg: #0c0f12      /* sidebar, slightly deeper */
--card: #1c2126            /* card surface */
--card-2: #232a31          /* elevated card / form background */
--line: #343d46            /* borders and dividers */
--text: #eef3f7            /* primary text */
--muted: #a8b3bd           /* secondary text, labels */
--accent: #d47c1c          /* orange — CTAs and active state ONLY */
--danger: #b74a4a
--success: #2b9a66
```

Typography: Inter (already loaded)
- Body: 400
- Labels / nav: 600
- Stats / model names: 700
- Table headers: 600, uppercase, letter-spacing 0.04em

Border radii:
- Cards: 12px
- Inputs / selects: 8px
- Pills / badges: 999px
- Modals / drawers: 16px (top edge only for panels)

Transitions: 150ms ease for hover states, 200ms for panels/drawers

---

## Navigation — Fixed Left Sidebar

**Replaces:** current horizontal pill nav across the top

**Structure:**
- Fixed left, full viewport height
- Collapsed width: 60px (icons only)
- Expanded width: 220px (icon + label)
- Toggle: hamburger icon at top, persists state in localStorage key `mkc_sidebar_collapsed`
- App logo at top of sidebar
- Nav items: Collection, Identify, Catalog, Reporting
- Active indicator: 3px accent-colored left border on active item (NOT a filled background pill)
- Bottom of sidebar: version string (currently in page footer)

**Main content area** shifts right with margin-left matching sidebar width.

**Files to change:** styles.css, index.html, identify.html, master.html, reporting.html

---

## Phase 1 — CSS Foundations + Sidebar

**Deliverable:** All pages get the sidebar. No functional changes.

Changes:
- styles.css: full sidebar CSS, updated layout wrapper, refined color/typography scale
- All 4 HTML files: wrap content in .app-layout grid, replace nav.app-nav with nav.sidebar
- Sidebar state (collapsed/expanded) stored in localStorage key `mkc_sidebar_collapsed`

---

## Phase 2 — Collection Page

### Stat strip (replaces 5 stat cards)

**Remove:** Inventory Rows, Est. Value, Master Models
**Keep:** Total Quantity, Total Spend
**Layout:** Single slim horizontal strip above the collection:
  90 pcs   ·   $31,550 spend
Muted label + bold value pairs, separated by vertical dividers. Single line, minimal height.

### Family chips (replaces .family-strip)

- Names only — NO row counts, NO piece counts
- Chips are clickable quick-filters (clicking one filters the collection to that family)
- Active chip gets accent border
- Horizontal scrollable row, no wrapping

### Card view (new default) + Table toggle

**Card layout:**
- Responsive grid: 4 cols >1100px, 3 cols >768px, 2 cols mobile
- Each card contains:
  - Knife image (top, 16:9 or square crop, object-fit: cover)
  - Model name (bold, 1rem)
  - Key spec pills: steel · finish · series (small, muted)
  - Quantity badge (top-right corner of image)
  - Estimated value or purchase price (bottom of card, muted)
- Click card → opens edit panel

**Table view:** remains as-is for power users
**Toggle:** small icon button in toolbar (grid icon / list icon), persisted in localStorage

---

## Phase 3 — Filter Drawer

**Replaces:** the inline row of 9 filter dropdowns

**Trigger:** "Filter" button in toolbar. When active, shows count badge: "Filter · 2"
**Drawer:** slides in from right, ~320px wide, semi-transparent backdrop
**Contents organized in groups:**
- Knife: Type, Family, Form, Series
- Variants: Steel, Finish, Handle Color, Blade Color
- Acquisition: Condition, Location
- Search bar stays inline (always visible)

**Files to change:** index.html, styles.css, app.js

---

## Phase 4 — Reporting Page Facelift

**Problem:** Has morphed into a developer debug page.
**Goal:** Clean, focused chat interface. Responses auto-render as appropriate format.

### Layout
- Two columns: narrow left sidebar (session/model selector, suggested questions) + wide right chat area
- All developer/debug output hidden by default (pipeline debug, semantic retrieval JSON, SQL)
- Developer debug toggle: subtle gear icon, collapsed by default

### Chat interface
- User messages: right-aligned, subtle accent-tinted bubble
- Assistant responses: left-aligned, clean card:
  - Text answers: well-formatted prose
  - Grid/table answers: clean styled table
  - Chart answers: SVG chart rendered inline
- Message area: proper scroll container
- Input area: full-width pinned to bottom, Send button inline

### Suggested questions
- Clean pill buttons in sidebar
- "New chat" button prominent at top of sidebar
- Model selector smaller/less prominent

### Remove from default view
- Pipeline debug toggle
- Semantic retrieval JSON expander
- Raw SQL display
- Export grid CSV (show only when there's a grid result)
- Follow-up ideas strip (integrate as suggested replies below last message)

**Files to change:** reporting.html, reporting.js, styles.css

---

## Phase 5 — Add/Edit Slide-in Panel

**Replaces:** centered modal dialog for add/edit knife

**New behavior:** Right-side panel, ~560px wide on desktop, full-width on mobile
- Main content dims slightly but remains visible
- Header (knife name or "Add knife"), scrollable form body, sticky footer with actions
- Same form fields, no functional changes

**Files to change:** index.html, styles.css, app.js

---

## Implementation Order

| Phase | Files Changed | Status |
|-------|--------------|--------|
| 1 — Sidebar + CSS foundations | styles.css, all 4 HTML files | Pending |
| 2 — Collection (cards, stat strip, family chips) | index.html, styles.css, app.js | Pending |
| 3 — Filter drawer | index.html, styles.css, app.js | Pending |
| 4 — Reporting facelift | reporting.html, reporting.js, styles.css | Pending |
| 5 — Slide-in edit panel | index.html, styles.css, app.js | Pending |

Each phase is independently reviewable and shippable. Review with user after each phase.

---

## Files Reference

```
/Users/dhogan/Applications/MKC_Inventory/
  static/
    app.js          — all frontend JS (vanilla, no framework)
    styles.css      — all styles
    index.html      — Collection page
    identify.html   — Identify page
    master.html     — Catalog page
    reporting.html  — Reporting page
    reporting.js    — Reporting page JS
    favicon.svg
    logo.png
  routes/
    static_pages_routes.py  — serves HTML shells
    v2_routes.py            — inventory API
    ai_routes.py            — AI/LLM endpoints
    reporting/              — reporting routes
```

---

## Key Design Decisions (user-confirmed)

- Sidebar navigation: YES
- Dark theme: YES (keep as primary)
- Card view default for collection: YES
- Stat strip: Quantity + Spend ONLY (remove Rows, Est. Value, Master Models)
- Family chips: names only, NO counts
- Filter drawer: YES (replaces inline filter row)
- Reporting: full facelift, clean chat UI, hide developer debug by default
- Edit modal → right-side slide-in panel: YES

---

*This document: /Users/dhogan/Applications/MKC_Inventory/docs/ui-redesign-plan.md*
*GitHub: https://github.com/davechogan/mkc-inventory-v2*
