# MKC Inventory API (Swagger-Like Reference)

This document is a Swagger-like, code-aligned reference for APIs defined in `app.py`.

## API Info

- **Title:** MKC Inventory Manager API
- **App file:** `app.py` (repository root)
- **Primary DB:** SQLite (`data/mkc_inventory.db`)
- **Content types:**
  - `application/json` for most APIs
  - `multipart/form-data` for image/CSV/AI upload endpoints
  - `text/csv` for export endpoints
- **Auth:** none (local/private network app)

## Major Request Schemas

### `MasterKnifeIn`
- Core fields: `name`, `family`, `category`, `catalog_line`, `blade_profile`
- Defaults/descriptor fields: `default_blade_length`, `default_steel`, `default_blade_finish`, `default_blade_color`, `default_handle_color`
- Status and notes: `status`, `record_type`, `catalog_status`, `notes`, `confidence`, `evidence_summary`, `collector_notes`
- Classification flags: `is_collab`, `collaboration_name`, `has_ring`, `is_filleting_knife`, `is_hatchet`, `is_kitchen`, `is_tactical`
- V2 transition fields: `canonical_slug`, `version`, `parent_model_id`, `is_discontinued`, `is_current_catalog`, `msrp`, `blade_shape`, `tip_style`, `grind_style`, `size_class`, `primary_use_case`, `spine_profile`

### `InventoryItemIn` (legacy inventory)
- `master_knife_id`, `nickname`, `quantity`, `acquired_date`, `purchase_price`, `estimated_value`
- `condition`, `handle_color`, `blade_steel`, `blade_finish`, `blade_color`, `blade_length`
- `is_collab`, `collaboration_name`, `serial_number`, `location`, `purchase_source`, `last_sharpened`, `notes`

### `OptionIn`
- `name`

### `IdentifierQuery`
- Search/classification hints: `q`, `family`, `blade_shape`, `size_class`, `use_case`
- Attributes: `steel`, `finish`, `blade_color`, `blade_length`
- Flags: `is_collab`, `has_ring`, `is_filleting_knife`, `is_fillet`, `is_hatchet`, `is_kitchen`, `is_tactical`
- Catalog filter: `catalog_line`, `include_archived`

### `DistinguishingFeaturesRecomputeBody`
- `knife_id`, `knife_ids`, `missing_only`, `model`

### `ReportingQueryIn`
- `question` (required)
- `session_id`, `model`
- `max_rows`, `chart_preference`
- `compare_dimension`, `compare_value_a`, `compare_value_b`

### `ReportingSaveQueryIn`
- `name`, `question`, `config`

### `ReportingFeedbackIn`
- `session_id`, `message_id`, `helpful`

### `V2ModelIn`
- Identity: `official_name`, `knife_type`, `form_name`, `family_name`, `series_name`, `collaborator_name`
- Controlled dimensions: `generation_label`, `size_modifier`, `platform_variant`, `handle_type`
- Descriptors: `steel`, `blade_finish`, `blade_color`, `handle_color`, `blade_length`
- Lifecycle: `record_status`, `is_current_catalog`, `is_discontinued`
- Commerce/media/notes: `msrp`, `official_product_url`, `official_image_url`, `notes`, `distinguishing_features`
- Relationship: `parent_model_id`

### `InventoryItemV2In` (v2 inventory)
- `knife_model_id`, `nickname`, `quantity`, `acquired_date`, `mkc_order_number`, `purchase_price`, `estimated_value`
- `condition`, `handle_color`, `steel`, `blade_finish`, `blade_color`, `blade_length`
- `collaboration_name`, `serial_number`, `location`, `purchase_source`, `last_sharpened`, `notes`

---

## Endpoints by Area

### UI Page Routes

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Collection page HTML |
| GET | `/identify` | Identify page HTML |
| GET | `/master` | Catalog page HTML |
| GET | `/reporting` | Reporting page HTML |
| GET | `/normalized` | Normalized data page HTML |

### Admin / Diagnostics

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/admin/silhouettes/status` | Legacy master image/silhouette health report |
| POST | `/api/admin/silhouettes/recompute` | Recompute legacy Hu silhouettes |
| GET | `/api/admin/distinguishing-features/status` | Legacy distinguishing feature coverage |
| POST | `/api/admin/distinguishing-features/recompute` | Recompute distinguishing features (`DistinguishingFeaturesRecomputeBody`) |

### Legacy Catalog + Inventory APIs

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/summary` | Legacy dashboard summary |
| GET | `/api/master-knives` | List/filter legacy master knives |
| GET | `/api/master-knives/export.csv` | Export legacy catalog CSV |
| POST | `/api/master-knives/import.csv` | Import legacy catalog CSV (multipart) |
| GET | `/api/master-knives/{knife_id}` | Get one legacy master knife |
| POST | `/api/master-knives` | Create legacy master knife (`MasterKnifeIn`) |
| PUT | `/api/master-knives/{knife_id}` | Update legacy master knife (`MasterKnifeIn`) |
| POST | `/api/master-knives/{knife_id}/duplicate` | Duplicate legacy master knife |
| DELETE | `/api/master-knives/{knife_id}` | Delete legacy master knife |
| GET | `/api/master-knives/{knife_id}/identifier-image` | Download legacy identifier image |
| POST | `/api/master-knives/{knife_id}/identifier-image` | Upload legacy identifier image (multipart) |
| DELETE | `/api/master-knives/{knife_id}/identifier-image` | Delete legacy identifier image |
| GET | `/api/inventory` | List legacy inventory |
| POST | `/api/inventory` | Create legacy inventory item (`InventoryItemIn`) |
| PUT | `/api/inventory/{item_id}` | Update legacy inventory item (`InventoryItemIn`) |
| DELETE | `/api/inventory/{item_id}` | Delete legacy inventory item |
| GET | `/api/inventory/export.csv` | Export legacy inventory CSV |
| POST | `/api/inventory/{item_id}/duplicate` | Duplicate legacy inventory item |
| GET | `/api/derive-blade-family` | Heuristic blade-family derivation |
| GET | `/api/options` | Legacy option lists |
| GET | `/api/inventory/options` | Legacy inventory options for selected master |
| POST | `/api/options/{option_type}` | Add legacy option (`OptionIn`) |
| DELETE | `/api/options/{option_type}/{option_id}` | Delete legacy option |

### Identification + AI Utility APIs

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/identify` | Legacy-compatible identify API (`IdentifierQuery`) |
| GET | `/api/ai/ollama/config` | Return configured Ollama host |
| GET | `/api/ai/ollama/check` | Health/model check for Ollama |
| GET | `/api/ai/ollama/models` | List Ollama models |
| GET | `/api/blade-shapes` | List blade shape templates |
| POST | `/api/ai/identify` | AI identify via text/image (multipart form) |

### Reporting APIs

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/reporting/schema` | Reporting schema/view metadata |
| GET | `/api/reporting/suggested-questions` | Suggested prompt templates |
| GET | `/api/reporting/sessions` | List reporting sessions |
| POST | `/api/reporting/sessions` | Create reporting session |
| GET | `/api/reporting/sessions/{session_id}` | Session detail + message history |
| GET | `/api/reporting/hints` | List learned semantic hints (optional `session_id`) |
| GET | `/api/reporting/saved-queries` | List saved queries |
| POST | `/api/reporting/saved-queries` | Save query (`ReportingSaveQueryIn`) |
| DELETE | `/api/reporting/saved-queries/{saved_id}` | Delete saved query |
| POST | `/api/reporting/query` | Run reporting query (`ReportingQueryIn`) |
| POST | `/api/reporting/feedback` | Record helpful/not-helpful feedback for an assistant message (`ReportingFeedbackIn`) |

### V2 Catalog + Inventory APIs (Canonical)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v2/inventory` | List/filter canonical v2 inventory |
| GET | `/api/v2/inventory/summary` | v2 inventory summary |
| GET | `/api/v2/inventory/filters` | v2 inventory filter options |
| POST | `/api/v2/inventory` | Create v2 inventory item (`InventoryItemV2In`) |
| PUT | `/api/v2/inventory/{item_id}` | Update v2 inventory item (`InventoryItemV2In`) |
| DELETE | `/api/v2/inventory/{item_id}` | Delete v2 inventory item |
| POST | `/api/v2/inventory/{item_id}/duplicate` | Duplicate v2 inventory item |
| GET | `/api/v2/catalog` | List/filter v2 catalog |
| GET | `/api/v2/catalog/filters` | v2 catalog filter options |
| GET | `/api/v2/models/search` | Search v2 models |
| GET | `/api/v2/models/{model_id}` | Get v2 model detail |
| POST | `/api/v2/models` | Create v2 model (`V2ModelIn`) |
| PUT | `/api/v2/models/{model_id}` | Update v2 model (`V2ModelIn`) |
| DELETE | `/api/v2/models/{model_id}` | Delete v2 model |
| POST | `/api/v2/models/{model_id}/duplicate` | Duplicate v2 model |
| GET | `/api/v2/models/by-legacy-master/{legacy_id}` | Resolve v2 model by legacy master id |
| GET | `/api/v2/models/{model_id}/image` | Get v2 model image |
| POST | `/api/v2/models/{model_id}/image` | Upload v2 model image (multipart) |
| DELETE | `/api/v2/models/{model_id}/image` | Delete v2 model image |
| POST | `/api/v2/models/{model_id}/recompute-descriptors` | Recompute v2 distinguishing features |
| GET | `/api/v2/options` | Get v2 controlled options |
| POST | `/api/v2/options/{option_type}` | Add v2 option (`OptionIn`) |
| DELETE | `/api/v2/options/{option_type}/{option_id}` | Delete v2 option |
| GET | `/api/v2/export/inventory.csv` | Export v2 inventory CSV |
| GET | `/api/v2/export/catalog.csv` | Export v2 catalog CSV |
| POST | `/api/v2/import/models.csv` | Import v2 model CSV (multipart) |
| POST | `/api/v2/models/backfill-identity` | Normalize/backfill v2 identity |
| POST | `/api/v2/models/migrate-legacy-media` | Copy legacy media to v2 media tables |
| GET | `/api/v2/models/qa/identity` | Identity QA report |
| POST | `/api/v2/identify` | Identify against canonical v2 models (`IdentifierQuery`) |
| GET | `/api/v2/admin/silhouettes/status` | v2 silhouette status |
| POST | `/api/v2/admin/silhouettes/recompute` | v2 silhouette recompute |
| GET | `/api/v2/admin/distinguishing-features/status` | v2 distinguishing feature status |
| POST | `/api/v2/admin/distinguishing-features/recompute` | v2 distinguishing feature recompute (`DistinguishingFeaturesRecomputeBody`) |

### Normalized Utilities

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/normalized/summary` | Normalized migration summary |
| GET | `/api/normalized/models` | Normalized models listing |
| GET | `/api/normalized/inventory` | Normalized inventory listing |
| POST | `/api/normalized/rebuild` | Rebuild normalized/v2 data from legacy |
| GET | `/api/normalized/export/models.csv` | Export normalized models CSV |

---

## Representative Response Shapes

### `POST /api/reporting/query`
Returns:
- `session_id`, `model`
- `answer_text`
- `columns` (raw field names)
- `rows` (tabular results)
- `chart_spec` (`type`, `x`, `y`, `data`) when available
- `follow_ups`, `confidence`, `limitations`, `generation_mode`, `execution_ms`
- `date_window` (`start`, `end`, `label`)
- `assistant_message_id` (used by feedback API for message-level rating)

### `GET /api/v2/inventory`
Returns a list of item rows that join inventory, model, and dimension metadata (name/type/family/form/series/collaborator + value/condition/location).

### `GET /api/v2/models/{model_id}`
Returns canonical v2 model identity + descriptor defaults + status/lifecycle + media/notes fields.

---

## Notes

- This document is intentionally “Swagger-like” (human-readable) rather than strict OpenAPI YAML/JSON.
- For machine-generated OpenAPI, this FastAPI app can expose schema directly if docs endpoints are enabled in runtime.
