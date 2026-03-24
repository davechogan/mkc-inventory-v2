# MKC Inventory App (normalized bridge build)

This build keeps the app runnable with:

```bash
uvicorn app:app --reload --host 0.0.0.0 --port 8008
```

It adds a normalized catalog layer on top of the legacy schema so you can preserve your current inventory and master records while moving toward a cleaner data model.

## What's new

- Adds normalized tables:
  - `knife_types`
  - `knife_forms`
  - `knife_families`
  - `knife_series`
  - `collaborators`
  - `knife_models_v2`
  - `inventory_items_v2`
- Imports your existing `master_knives` and `inventory_items` into the normalized layer automatically on startup.
- Preserves the legacy tables so the current app keeps working.
- Adds a read-only normalized catalog page at `/normalized`.
- Adds export and rebuild endpoints:
  - `/api/normalized/summary`
  - `/api/normalized/models`
  - `/api/normalized/inventory`
  - `/api/normalized/export/models.csv`
  - `POST /api/normalized/rebuild`

## Normalization rules

Identity is split into:

- **Type**: Hunting, Culinary, Tactical, Everyday Carry, Bushcraft & Camp
- **Form**: Skinner, Petty Knife, Hatchet, EDC Fixed Blade, etc.
- **Family**: Speedgoat, Blackfoot, Stoned Goat, Whitetail, etc.
- **Model**: the normalized model name after stripping noise
- **Series**: Blood Brothers, VIP, Traditions, etc.
- **Collaborator**: separate from series when relevant

Examples:

- `Blood Brothers Blackfoot 2.0` → Type=Hunting, Family=Blackfoot, Model=`Blackfoot 2.0`, Series=Blood Brothers
- `Blood Brothers Mini Speedgoat 2.0` → Type=Hunting, Family=Speedgoat, Model=`Mini Speedgoat 2.0`, Series=Blood Brothers
- `MKC Whitetail VIP` → Type=Hunting, Family=Whitetail, Model=`Whitetail`, Series=VIP
- `Little Bighorn Petty VIP` → Type=Culinary, Form=Petty Knife, Family=Little Bighorn Petty, Model=`Little Bighorn Petty`, Series=VIP
- `Magnacut Stoned Goat` → Type=Hunting, Family=Stoned Goat, Model=`Stoned Goat`, Steel=MagnaCut

## Install

### Option A: manual

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload --host 0.0.0.0 --port 8008
```

### Option B: script

```bash
./scripts/setup.sh
./scripts/run.sh
```

## Blank DB

A clean blank DB is included at:

```text
data/mkc_inventory.blank.db
```

If you want to start from blank:

1. stop the app
2. back up `data/mkc_inventory.db`
3. copy `data/mkc_inventory.blank.db` to `data/mkc_inventory.db`
4. start the app

## Rebuild normalized layer from legacy tables

If you change legacy data and want to rebuild the normalized tables:

```bash
source .venv/bin/activate
python tools/import_legacy_data.py
```

## Notes

This is a bridge build, not a total rewrite. The old UI and endpoints still exist, but the new normalized tables give you a much cleaner foundation for the next iteration.
