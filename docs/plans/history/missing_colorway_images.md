# Missing Colorway Images — Agent Task Brief

## What this is

This document lists every inventory item that is either missing a product image entirely or is showing the wrong colorway image (a fallback). An agent should source images from the MKC website (mountainknifecompany.com) and save them to `Images/MKC_Colors/` using the naming convention described below. After adding images, run `tools/sync_images.py` to sync them into the database.

---

## Naming Convention

Images must be named exactly so the sync tool can match them to the correct knife model and colorway.

**Regular series** (all knives except tactical) — named by handle color only:
```
{ModelName}_{HandleColor}.jpg
```
Examples: `Flathead_Fillet_Black.jpg`, `Flathead_Fillet_Olive.jpg`, `Elkhorn_Skinner_Orange_Black.jpg`

**Tactical series** (Battle Goat, Mini Wargoat, Wargoat, TF24, V24, SERE 25) — named by blade color then handle color:
```
{ModelName}_{BladeColor}_{HandleColor}.jpg
```
Examples: `SERE_25_Coyote_OD_Green.jpg`, `Speedgoat_20_OD_Green.jpg`

**Rules:**
- Use underscores between words and between the model name and color tokens
- Match the capitalization style already in use (title case each word)
- `.jpg` preferred; `.png` acceptable
- Do not include blade color for non-tactical knives — handle color only
- The model name prefix must match exactly how other images for that model are already named (check existing files in `Images/MKC_Colors/` as reference)

---

## Priority 1 — No image at all (showing BLOB fallback or placeholder)

These models have zero colorway images. They currently show whatever legacy blob image was stored, which is typically an Orange/Black shot that may not match what you own.

| Model | Slug | Your Color(s) | Target Filename(s) |
|---|---|---|---|
| Bighorn Chef VIP | bighorn-chef-2 | Black handle | `Bighorn_Chef_VIP_Black.jpg` |
| Blackfoot 2.0 Mike Rowe Works | blackfoot-2-0-mike-rowe-works | Orange/Black handle | `Blackfoot_20_Mike_Rowe_Works_Orange_Black.jpg` |
| Cutbank Paring Knife | cutbank-paring-knife | Black handle | `Cutbank_Paring_Knife_Black.jpg` |
| Cutbank Paring Knife | cutbank-paring-knife | Red handle | `Cutbank_Paring_Knife_Red.jpg` |
| Cutbank Paring Knife | cutbank-paring-knife | Orange/Black handle | `Cutbank_Paring_Knife_Orange_Black.jpg` |
| Hellgate Hatchet | hellgate-hatchet | Orange/Black handle | `Hellgate_Hatchet_Orange_Black.jpg` |
| Little Bighorn Petty VIP | little-bighorn-petty-2 | Black handle | `Little_Bighorn_Petty_VIP_Black.jpg` |
| Meat Church Chef Knife | chef-knife | Black handle | `Meat_Church_Chef_Knife_Black.jpg` |
| MKC Whitetail VIP | whitetail-3 | Black handle | `MKC_Whitetail_VIP_Black.jpg` |
| SERE 25 | sere-25 | Coyote blade / OD Green handle | `SERE_25_Coyote_OD_Green.jpg` |
| Smith River Santoku VIP | smith-river-santoku-2 | Black handle | `Smith_River_Santoku_VIP_Black.jpg` |
| Stoned Goat 2.0 PVD | stoned-goat-2-0-pvd | Orange/Black handle | `Stoned_Goat_20_PVD_Orange_Black.jpg` |
| Stoned Goat 2.0 VIP | stoned-goat-2-0-vip | Black handle | `Stoned_Goat_20_VIP_Black.jpg` |
| The Rocker | rocker | Orange/Black handle | `The_Rocker_Orange_Black.jpg` |
| The Stockyard | stockyard | Orange/Black handle | `The_Stockyard_Orange_Black.jpg` |
| Triumph Hunter | triumph-hunter | Orange/Black handle | `Triumph_Hunter_Orange_Black.jpg` |
| Triumph Hunter XL | triumph-hunter-xl | Orange/Black handle | `Triumph_Hunter_XL_Orange_Black.jpg` |

---

## Priority 2 — Has images, but missing your specific colorway

These models have images in the system but not for the colorway you own. They are currently showing a fallback (wrong color or wrong image entirely).

| Model | Slug | Missing Colorway | Currently Showing | Target Filename |
|---|---|---|---|---|
| Elkhorn Skinner | elk-knife | Orange/Black handle | Black (wrong color) | `Elkhorn_Skinner_Orange_Black.jpg` |
| Flathead Fillet | flathead-fillet | Black handle | Red (wrong color) | `Flathead_Fillet_Black.jpg` |
| Flathead Fillet | flathead-fillet | Olive handle | Red (wrong color) | `Flathead_Fillet_Olive.jpg` |
| Flathead Fillet | flathead-fillet | Orange/Black handle | Red (wrong color) | `Flathead_Fillet_Orange_Black.jpg` |
| Flathead Fillet - HUK | flathead-fillet-copy | Olive handle | Black (wrong color) | `Flathead_Fillet_HUK_Olive.jpg` |
| Speedgoat 2.0 | speedgoat-2-0 | OD Green handle | Orange/Black (wrong color) | `Speedgoat_20_OD_Green.jpg` |
| The Marshall Bushcraft Knife | marshall-bushcraft-knife | Orange/Black handle | Blaze Orange (wrong color) | `The_Marshall_Bushcraft_Knife_Orange_Black.jpg` |

---

## No action needed — acceptable single-image models

These models intentionally have only one image because they were made in a single configuration with no colorway options. The image shown is correct even though it doesn't match the handle_color field exactly.

| Model | Why one image is correct |
|---|---|
| Damascus Blackfoot 2.0 | Limited edition (500 made), Damascus blade + Desert Ironwood handle, one configuration only |
| Blood Brothers Blackfoot 2.0 | Collab knife, fixed Red blade / Black-Red handle, no options |
| Blood Brothers Mini Speedgoat 2.0 | Collab knife, fixed configuration, no options |
| Jackstone Snyder Edition | Collab knife, fixed Olive/Tan handle, no options |
| Traditions Speedgoat | Traditions series, Steel blade + Desert Ironwood handle, one configuration |
| Traditions Jackstone | Traditions series, Steel blade + Desert Ironwood handle, one configuration |
| Traditions MKC Whitetail | Traditions series, Steel blade + Desert Ironwood handle, one configuration |
| Traditions Blackfoot 2.0 | Traditions series, Steel blade + Desert Ironwood handle, one configuration |
| Speedgoat Ultra | Ultra series, Steel blade + Carbon Fiber handle, one configuration |

---

## After adding images

1. Drop the `.jpg` files into `Images/MKC_Colors/`
2. Run: `python tools/sync_images.py` (or `--dry-run` first to verify matches)
3. Restart the app server — images are served statically, no DB migration needed

The sync tool will log any filenames it cannot match to a knife model so you can correct naming issues before they go into the DB.
