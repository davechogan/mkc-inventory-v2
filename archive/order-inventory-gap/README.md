# Archived: order vs inventory gap reconciliation

One-off workflow used to validate inventory against Shopify email order exports, assign `mkc_order_number` / `acquired_date`, and reconcile bucket-level gaps.

**Removed from the active app:** FastAPI routes under `/order-inventory-gaps` and `/api/order-inventory-gaps*`, the gap UI (`static/` copies here), and `gap_analysis_core` as an import of `app.py`. SQLite tables `order_inv_gap_*` may still exist in older databases; they are unused by the running app.

## Layout

- `gap_analysis_core.py` — matching and gap math (import this directory on `PYTHONPATH`, or use the path hack in the tools).
- `static/` — HTML/JS for the old gap page (was served at `/order-inventory-gaps`).
- `tools/` — CLI pipeline (email filter → color enrich → v2 normalize → gap reports → optional inventory assignment).
- `sample-outputs/` — example gap summary/CSV outputs (if present).

## Running archived tools

Run from the **repository root** so default `data/...` paths resolve. Example:

```bash
.venv/bin/python archive/order-inventory-gap/tools/gap_analysis_orders_vs_inventory.py
```

The gap CLI and `assign_mkc_orders_to_inventory_v2.py` add `archive/order-inventory-gap` to `sys.path` so `gap_analysis_core` imports correctly.

Other tools (`filter_email_orders_to_catalog_knives.py`, etc.) use paths relative to the current working directory; keep `cwd` at the repo root.
