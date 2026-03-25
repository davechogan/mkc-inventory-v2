# MKC UI Rewire Spec (v2)

## Objective
Rewire the existing UI to read from the new normalized v2 tables instead of the legacy tables.

### Normalized tables
- knife_types
- knife_forms
- knife_families
- knife_series
- collaborators
- knife_models_v2
- inventory_items_v2

### Legacy tables still present
- master_knives
- inventory_items

Goal: switch reads first, then writes.

---

## Implementation Strategy

### Phase A — Backend First
Create flattened `/api/v2/...` endpoints.

### Phase B — Rewire UI Reads
Point Collection + Catalog pages to new endpoints.

### Phase C — Rewire Forms
Update add/edit inventory after reads are stable.

---

## Rules
- Do NOT remove legacy tables yet
- Do NOT rewrite entire UI
- Keep FastAPI + SQLite
- Use `/api/v2/...` endpoints
- Flatten data in backend
- Preserve UI structure

---

## Part 1 — Backend Endpoints

### GET /api/v2/inventory
Returns flattened inventory rows from v2 tables.

Includes:
- identity (name, family, type, form, series)
- attributes (steel, finish, colors)
- inventory data (qty, price, condition, location)

Supports filters:
search, type, family, form, series, steel, finish, handle_color, condition, location

---

### GET /api/v2/inventory/summary
Returns:
- inventory_rows
- total_quantity
- total_spend
- estimated_value
- master_models
- by_family counts

---

### GET /api/v2/catalog
Returns flattened model data.

Includes:
- official_name
- normalized_name
- type, family, form
- series, collaboration
- lifecycle
- default attributes

Supports filters:
search, type, family, form, series, collaboration

---

### GET /api/v2/catalog/filters
Returns distinct values for dropdowns.

---

## Part 2 — Collection Page

Use:
- /api/v2/inventory
- /api/v2/inventory/summary
- /api/v2/inventory/filters

Update:
- summary cards
- table data
- filters

Add optional columns:
- Type, Family, Form, Series

---

## Part 3 — Catalog Page

Use:
- /api/v2/catalog
- /api/v2/catalog/filters

Display:
- Official Name
- Type, Family, Form
- Series, Collaboration
- Default attributes
- Status

---

## Part 4 — Inventory Form

Write to `inventory_items_v2`.

Flow:
1. Search/select model
2. Display model context
3. Enter attributes (steel, finish, etc.)

Endpoints:
- GET /api/v2/models/search
- POST /api/v2/inventory
- PUT /api/v2/inventory/{id}

---

## Part 5 — Compatibility

- Keep legacy endpoints
- Prefer v2 endpoints
- Do not break startup/import

---

## Part 6 — Acceptance Criteria

### Collection
- Uses v2 data
- Correct counts
- Filters work

### Catalog
- Uses v2 models
- Fields visible
- Sorting works

### Inventory
- Create/edit works on v2
- Data persists

---

## Part 7 — Implementation Order

1. /api/v2/inventory
2. /api/v2/inventory/summary
3. /api/v2/inventory/filters
4. Rewire Collection page
5. /api/v2/catalog
6. /api/v2/catalog/filters
7. Rewire Catalog page
8. Model search endpoint
9. Rewire forms

---

## Part 8 — Smoke Test

- Counts match DB
- Search works
- Filters work
- Add/edit works
- Restart persists data

---

## Final Principle

Backend = truth + joins  
Frontend = display only

---

## Migration Status (Implemented)

**Phase A — Backend** ✓
- `GET /api/v2/inventory` — flattened inventory with filters
- `GET /api/v2/inventory/summary` — rows, totals, by_family
- `GET /api/v2/inventory/filters` — distinct values for dropdowns
- `GET /api/v2/catalog` — flattened models
- `GET /api/v2/catalog/filters` — distinct catalog filter values
- `GET /api/v2/models/search` — model picker search
- `GET /api/v2/models/{id}` — canonical v2 model detail
- `POST /api/v2/models` / `PUT /api/v2/models/{id}` / `DELETE /api/v2/models/{id}` — canonical v2 catalog CRUD
- `GET/POST/DELETE /api/v2/models/{id}/image` — v2-linked model image storage
- `POST /api/v2/models/{id}/recompute-descriptors` — v2 descriptor recompute from stored image
- `GET /api/v2/options` (+ add/delete) — v2-native controlled option values
- `POST /api/v2/inventory` — create (v2-only write)
- `PUT /api/v2/inventory/{id}` — update (v2-only write)
- `DELETE /api/v2/inventory/{id}` — delete (v2-only write)
- `POST /api/v2/inventory/{id}/duplicate` — duplicate (v2-only write)
- `GET /api/v2/export/inventory.csv` and `GET /api/v2/export/catalog.csv` — v2-native exports
- `POST /api/v2/import/models.csv` — v2-native catalog import
- `POST /api/v2/identify` — identify against v2 catalog

**Phase B — Collection Page** ✓
- Uses v2 summary, inventory, filters
- Model search for add/edit (replaces master_knife_id select)
- Optional columns: Type, Family, Form, Series
- New filter dropdowns: Type, Family, Form, Series

**Phase C — Catalog Page** ✓
- Uses v2/catalog and v2/catalog/filters
- Display: Official Name, Type, Family, Form, Series, default attributes, status
- Row click edits canonical v2 model records

**Phase C — Inventory Form** ✓
- Search/select model via `/api/v2/models/search`
- Writes to `inventory_items_v2` only (canonical)

**Notes:**
- Legacy endpoints are preserved for migration compatibility, but active UI flows use v2 endpoints.
- Startup no longer auto-runs legacy -> v2 migration; run `POST /api/normalized/rebuild` manually when needed.
