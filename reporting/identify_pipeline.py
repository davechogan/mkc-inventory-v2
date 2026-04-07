"""Knife identification pipeline — feature-based candidate pruning + vision comparison.

Three-stage cascade:
  1. Deterministic gating (hard filters from user inputs + catalog truth)
  2. Structured scoring (feature profile matching)
  3. Vision comparison (gemma3:27B image-to-image on surviving candidates)

See: artifacts/plans/knife_vision_identification_design.md
"""

from __future__ import annotations

import base64
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Optional

_log = logging.getLogger(__name__)

# Blade length bins (same as existing pipeline)
_LENGTH_BINS: dict[int, tuple[float, float]] = {
    1: (0.0, 3.0),
    2: (3.0, 4.5),
    3: (4.5, 7.0),
    4: (7.0, 20.0),
}

# Big families that get a dedicated elimination pass
_BIG_FAMILIES = {"Speedgoat", "Blackfoot", "Stoned Goat", "Wargoat"}


@dataclass
class UserInputs:
    """User-provided filter hints from the identify form."""
    image_b64: Optional[str] = None
    handle_material: Optional[str] = None
    handle_color: Optional[str] = None
    blade_color: Optional[str] = None
    is_culinary: Optional[bool] = None
    blade_forms: set[str] = field(default_factory=set)
    blade_length_bin: Optional[int] = None


@dataclass
class Candidate:
    """A model surviving the pipeline with its scores and rationale."""
    model_id: int
    name: str
    family: str
    form: Optional[str]
    handle_type: Optional[str]
    blade_length: Optional[float]
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    gate_stage: str = "initial"
    vision_match: Optional[str] = None
    vision_reason: Optional[str] = None
    has_image: bool = False
    colorway_image_b64: Optional[str] = None
    best_colorway_id: Optional[int] = None


def run_pipeline(
    conn: sqlite3.Connection,
    inputs: UserInputs,
    vision_model: str,
    vision_fn: Any = None,
    extract_fn: Any = None,
) -> dict:
    """Run the full identification pipeline.

    Args:
        conn: SQLite connection (row_factory must be sqlite3.Row)
        inputs: User-provided filter inputs
        vision_model: Ollama model name for vision comparison
        vision_fn: Function to call for vision comparison (blade_ai.vision_compare_candidates)
        extract_fn: Function to extract features from user image (blade_ai.ollama_chat)

    Returns:
        dict with results, families_eliminated, families_remaining, vision_used
    """
    # ── Load all models with profiles ──
    models = _load_models(conn)
    families = _group_by_family(models)
    _log.info(f"Pipeline start: {len(models)} models, {len(families)} families")

    # ── Stage 1: Deterministic gating ──
    eliminated = _gate_families(families, inputs)
    _log.info(f"Stage 1 (gate): {len(eliminated)} families eliminated, {len(families) - len(eliminated)} remaining")

    # ── Stage 2: Feature scoring ──
    user_profile = None
    if inputs.image_b64 and extract_fn:
        user_profile = _extract_user_features(inputs.image_b64, extract_fn)
        _log.info(f"User profile extracted: {user_profile}")

    candidates = _score_candidates(models, families, eliminated, inputs, user_profile)
    _log.info(f"Stage 2 (score): {len(candidates)} candidates scored")

    # ── Stage 3a: Big family elimination pass ──
    if inputs.image_b64 and vision_fn:
        candidates = _big_family_pass(conn, candidates, inputs, vision_model, vision_fn)
        _log.info(f"Stage 3a (big family): {len(candidates)} candidates remaining")

    # ── Stage 3b: Final vision comparison ──
    vision_used = False
    if inputs.image_b64 and vision_fn:
        candidates = _final_vision_compare(conn, candidates, inputs, vision_model, vision_fn)
        vision_used = True
        _log.info(f"Stage 3b (vision): final ranking complete")

    # ── Attach best colorway IDs ──
    with conn:
        for c in candidates:
            c.best_colorway_id = _find_best_colorway_id(conn, c.model_id, inputs.handle_color)

    # ── Format results ──
    results = _format_results(candidates)

    return {
        "results": results[:15],
        "families_eliminated": len(eliminated),
        "families_remaining": len(families) - len(eliminated),
        "vision_used": vision_used,
    }


# ── Data loading ──

def _load_models(conn: sqlite3.Connection) -> list[dict]:
    """Load all models with their vision profiles and catalog data."""
    rows = conn.execute("""
        SELECT km.id, km.official_name, km.blade_length,
               fam.name AS family_name, frm.name AS form_name,
               kt.name AS knife_type, ht.name AS handle_type,
               bs.name AS blade_steel, bf.name AS blade_finish,
               ks.name AS series_name, c.name AS collaborator_name,
               p.observable_profile_json,
               (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_image
        FROM knife_models_v2 km
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
        LEFT JOIN blade_steels bs ON bs.id = km.steel_id
        LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id
        LEFT JOIN knife_model_vision_profiles p ON p.knife_model_id = km.id
        LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
        ORDER BY fam.name, km.official_name
    """).fetchall()
    return [dict(r) for r in rows]


def _group_by_family(models: list[dict]) -> dict[str, list[dict]]:
    """Group models by family name."""
    families: dict[str, list[dict]] = {}
    for m in models:
        fam = m["family_name"] or "(none)"
        families.setdefault(fam, []).append(m)
    return families


# ── Stage 1: Deterministic gating ──

def _gate_families(families: dict[str, list[dict]], inputs: UserInputs) -> set[str]:
    """Eliminate families that contradict user inputs. Returns set of eliminated family names."""
    eliminated: set[str] = set()

    for fam, members in families.items():
        # Culinary toggle
        if inputs.is_culinary is not None:
            fam_types = {m["knife_type"] for m in members if m["knife_type"]}
            if inputs.is_culinary and "Culinary" not in fam_types:
                eliminated.add(fam)
                continue
            if not inputs.is_culinary and fam_types == {"Culinary"}:
                eliminated.add(fam)
                continue

        # Handle material — HARD filter
        if inputs.handle_material:
            fam_handles = {m["handle_type"] for m in members if m["handle_type"]}
            if fam_handles and inputs.handle_material not in fam_handles:
                eliminated.add(fam)
                continue

        # Blade form selection
        if inputs.blade_forms:
            fam_forms = {m["form_name"] for m in members if m["form_name"]}
            if fam_forms and not fam_forms.intersection(inputs.blade_forms):
                eliminated.add(fam)
                continue

        # Blade length bin
        if inputs.blade_length_bin and inputs.blade_length_bin in _LENGTH_BINS:
            lo, hi = _LENGTH_BINS[inputs.blade_length_bin]
            lengths = [m["blade_length"] for m in members if m["blade_length"] is not None]
            if lengths and not any(lo <= bl <= hi for bl in lengths):
                eliminated.add(fam)
                continue

    return eliminated


# ── Feature extraction from user image ──

def _extract_user_features(image_b64: str, extract_fn: Any) -> dict:
    """Ask the vision model the feature questions about the user's uploaded photo."""
    from migrations.extract_vision_features import EXTRACTION_PROMPT
    from blade_ai import try_parse_json_response

    raw = extract_fn(
        "gemma3:27B",
        "You are analyzing a knife photo. Answer each question precisely based on what you see.",
        EXTRACTION_PROMPT,
        images_b64=[image_b64],
        timeout=60.0,
    )
    parsed = try_parse_json_response(raw)
    return parsed if isinstance(parsed, dict) else {}


# ── Stage 2: Feature scoring ──

def _score_candidates(
    models: list[dict],
    families: dict[str, list[dict]],
    eliminated: set[str],
    inputs: UserInputs,
    user_profile: Optional[dict],
) -> list[Candidate]:
    """Score surviving models based on user inputs and feature profile matching."""
    candidates: list[Candidate] = []

    for m in models:
        fam = m["family_name"] or "(none)"
        if fam in eliminated:
            continue

        c = Candidate(
            model_id=m["id"],
            name=m["official_name"],
            family=fam,
            form=m["form_name"],
            handle_type=m["handle_type"],
            blade_length=m["blade_length"],
            has_image=bool(m["has_image"]),
            gate_stage="post_gate",
        )

        # Score from user inputs
        if inputs.handle_material and m["handle_type"]:
            if inputs.handle_material.lower() == m["handle_type"].lower():
                c.score += 20
                c.reasons.append(f"handle: {m['handle_type']}")
            else:
                c.score -= 30

        if inputs.blade_forms and m["form_name"]:
            if m["form_name"] in inputs.blade_forms:
                c.score += 20
                c.reasons.append(f"form: {m['form_name']}")
            else:
                c.score -= 10

        if inputs.blade_length_bin and inputs.blade_length_bin in _LENGTH_BINS and m["blade_length"]:
            lo, hi = _LENGTH_BINS[inputs.blade_length_bin]
            if lo <= m["blade_length"] <= hi:
                c.score += 15
                c.reasons.append(f"blade {m['blade_length']}\" in range")

        if inputs.is_culinary is not None and m["knife_type"]:
            is_model_culinary = m["knife_type"] == "Culinary"
            if inputs.is_culinary == is_model_culinary:
                c.score += 10
            else:
                c.score -= 10

        # Score from feature profile matching (user photo vs catalog profile)
        if user_profile and m["observable_profile_json"]:
            profile = json.loads(m["observable_profile_json"])
            profile_score, profile_reasons = _match_profiles(user_profile, profile)
            c.score += profile_score
            c.reasons.extend(profile_reasons)

        candidates.append(c)

    candidates.sort(key=lambda x: (-x.score, x.name.lower()))
    return candidates


def _match_profiles(user: dict, catalog: dict) -> tuple[float, list[str]]:
    """Compare user-extracted features against catalog profile. Returns (score, reasons)."""
    score = 0.0
    reasons: list[str] = []

    # High-value feature comparisons
    # (feature_key, match_points, mismatch_points, display_label)
    comparisons = [
        # Hard exclusion features (high penalty for mismatch)
        ("blade_color_primary", 20, -40, "blade color"),         # Red blade = Blood Brothers only
        ("finger_ring_presence", 15, -30, "finger ring"),        # Ring = Wargoat family only
        ("handle_type_visual", 15, -20, "handle type"),
        ("knife_type_visual", 10, -15, "knife type"),
        # Medium features
        ("blade_finish_visual", 8, -5, "blade finish"),
        ("handle_color_primary", 8, -3, "handle color"),
        ("blade_to_handle_ratio_bucket", 10, -8, "blade/handle ratio"),
        ("choil_presence", 6, -4, "choil"),
        ("jimping_presence", 5, -3, "jimping"),
        ("lanyard_hole_presence", 4, -2, "lanyard hole"),
        ("guard_prominence", 4, -2, "guard"),
        ("handle_profile_primary", 5, -3, "handle profile"),
        ("spine_profile_primary", 5, -3, "spine profile"),
        ("edge_profile_primary", 5, -3, "edge profile"),
        ("tip_drop_relative_to_spine", 5, -3, "tip position"),
    ]

    for key, match_pts, miss_pts, label in comparisons:
        u_val = user.get(key, "unknown")
        c_val = catalog.get(key, "unknown")
        if u_val == "unknown" or c_val == "unknown":
            continue  # Skip unknowns — no penalty, no reward
        if u_val == c_val:
            score += match_pts
            reasons.append(f"{label} match")
        else:
            score += miss_pts

    return score, reasons


# ── Stage 3a: Big family elimination ──

def _big_family_pass(
    conn: sqlite3.Connection,
    candidates: list[Candidate],
    inputs: UserInputs,
    vision_model: str,
    vision_fn: Any,
) -> list[Candidate]:
    """Send one representative per big family to vision. Eliminate non-STRONG families."""
    big_family_candidates = [c for c in candidates if c.family in _BIG_FAMILIES]
    if not big_family_candidates:
        return candidates

    # Pick one representative per big family (highest scoring)
    seen: set[str] = set()
    representatives: list[Candidate] = []
    for c in big_family_candidates:
        if c.family not in seen and c.has_image:
            seen.add(c.family)
            representatives.append(c)

    if not representatives or not inputs.image_b64:
        return candidates

    # Load colorway-matched images for each representative
    vision_cands = []
    for rep in representatives:
        img_b64 = _load_best_colorway_image(conn, rep.model_id, inputs.handle_color)
        if img_b64:
            vision_cands.append({
                "name": rep.name,
                "family": rep.family,
                "form": rep.form,
                "reference_image_b64": img_b64,
            })

    if not vision_cands:
        return candidates

    _log.info(f"Big family pass: {len(vision_cands)} families ({[c['family'] for c in vision_cands]})")

    # Run vision comparison
    results = vision_fn(vision_model, inputs.image_b64, vision_cands)

    # Parse results — only keep STRONG families
    strong_families: set[str] = set()
    for vr in results:
        match = (vr.get("match") or "").upper()
        name = vr.get("model", "")
        fam = next((vc["family"] for vc in vision_cands if vc["name"] == name), None)
        if match == "STRONG" and fam:
            strong_families.add(fam)
            _log.info(f"  Big family KEEP: {fam} ({match})")
        elif fam:
            _log.info(f"  Big family ELIMINATE: {fam} ({match})")

    # Remove big-family candidates that didn't get STRONG
    # BUT keep all non-big-family candidates
    filtered = []
    for c in candidates:
        if c.family in _BIG_FAMILIES:
            if c.family in strong_families:
                filtered.append(c)
        else:
            filtered.append(c)

    return filtered


# ── Stage 3b: Final vision comparison ──

def _final_vision_compare(
    conn: sqlite3.Connection,
    candidates: list[Candidate],
    inputs: UserInputs,
    vision_model: str,
    vision_fn: Any,
) -> list[Candidate]:
    """Send top candidates to vision for final ranking."""
    if not inputs.image_b64:
        return candidates

    # Pick top 8 candidates, one per family
    seen: set[str] = set()
    top: list[Candidate] = []
    for c in candidates:
        fam = c.family
        if fam not in seen and c.has_image:
            seen.add(fam)
            top.append(c)
        if len(top) >= 8:
            break

    if not top:
        return candidates

    # Load colorway-matched images
    vision_cands = []
    for c in top:
        img_b64 = _load_best_colorway_image(conn, c.model_id, inputs.handle_color)
        if img_b64:
            vision_cands.append({
                "name": c.name,
                "form": c.form,
                "reference_image_b64": img_b64,
            })

    if not vision_cands:
        return candidates

    _log.info(f"Final vision: {len(vision_cands)} candidates")

    results = vision_fn(vision_model, inputs.image_b64, vision_cands)

    # Apply vision scores
    vision_map = {vr.get("model", ""): vr for vr in results}

    for c in candidates:
        vr = vision_map.get(c.name)
        if vr:
            match = (vr.get("match") or "").upper()
            c.vision_match = match
            c.vision_reason = vr.get("reason", "")
            if match == "STRONG":
                c.score += 30
                c.reasons.append(f"vision: STRONG")
            elif match == "POSSIBLE":
                c.score += 10
                c.reasons.append(f"vision: POSSIBLE")
            elif match == "UNLIKELY":
                c.score -= 20
                c.reasons.append(f"vision: UNLIKELY")
            c.gate_stage = "final_compare"

    # Propagate vision results to family members
    family_vision: dict[str, dict] = {}
    for c in candidates:
        if c.vision_match and c.family not in family_vision:
            family_vision[c.family] = {"match": c.vision_match, "reason": c.vision_reason}

    for c in candidates:
        if not c.vision_match and c.family in family_vision:
            fv = family_vision[c.family]
            c.vision_match = fv["match"]
            c.vision_reason = f"family match — {fv['reason']}"
            match = (fv["match"] or "").upper()
            if match == "STRONG":
                c.score += 30
            elif match == "POSSIBLE":
                c.score += 10
            elif match == "UNLIKELY":
                c.score -= 20

    candidates.sort(key=lambda x: (-x.score, x.name.lower()))
    return candidates


# ── Colorway image loading ──

def _find_best_colorway_id(conn: sqlite3.Connection, model_id: int, handle_color: Optional[str]) -> Optional[int]:
    """Find the best-matching colorway ID. Priority: matching color > Orange/Black > Black > any."""
    queries = []
    if handle_color:
        queries.append(("SELECT mc.id FROM model_colorways mc LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id WHERE mc.knife_model_id = ? AND lower(hc.name) = lower(?) AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0 LIMIT 1", (model_id, handle_color)))
    queries.append(("SELECT mc.id FROM model_colorways mc LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id WHERE mc.knife_model_id = ? AND lower(hc.name) = 'orange/black' AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0 LIMIT 1", (model_id,)))
    queries.append(("SELECT mc.id FROM model_colorways mc LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id WHERE mc.knife_model_id = ? AND lower(hc.name) = 'black' AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0 LIMIT 1", (model_id,)))
    queries.append(("SELECT mc.id FROM model_colorways mc WHERE mc.knife_model_id = ? AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0 LIMIT 1", (model_id,)))

    for sql, params in queries:
        row = conn.execute(sql, params).fetchone()
        if row:
            return row["id"]
    return None


def _load_best_colorway_image(conn: sqlite3.Connection, model_id: int, handle_color: Optional[str]) -> Optional[str]:
    """Load the best-matching colorway image as base64. Priority: matching color > Orange/Black > Black > any."""

    # Try matching handle color
    if handle_color:
        row = conn.execute("""
            SELECT mc.image_blob FROM model_colorways mc
            LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
            WHERE mc.knife_model_id = ? AND lower(hc.name) = lower(?)
                  AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0
            LIMIT 1
        """, (model_id, handle_color)).fetchone()
        if row:
            return base64.b64encode(row["image_blob"]).decode("ascii")

    # Try Orange/Black
    row = conn.execute("""
        SELECT mc.image_blob FROM model_colorways mc
        LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
        WHERE mc.knife_model_id = ? AND lower(hc.name) = 'orange/black'
              AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0
        LIMIT 1
    """, (model_id,)).fetchone()
    if row:
        return base64.b64encode(row["image_blob"]).decode("ascii")

    # Try Black
    row = conn.execute("""
        SELECT mc.image_blob FROM model_colorways mc
        LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
        WHERE mc.knife_model_id = ? AND lower(hc.name) = 'black'
              AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0
        LIMIT 1
    """, (model_id,)).fetchone()
    if row:
        return base64.b64encode(row["image_blob"]).decode("ascii")

    # Fallback: any colorway
    row = conn.execute("""
        SELECT mc.image_blob FROM model_colorways mc
        WHERE mc.knife_model_id = ?
              AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0
        LIMIT 1
    """, (model_id,)).fetchone()
    if row:
        return base64.b64encode(row["image_blob"]).decode("ascii")

    # Final fallback: model image
    row = conn.execute("""
        SELECT image_blob FROM knife_model_images
        WHERE knife_model_id = ? AND image_blob IS NOT NULL
    """, (model_id,)).fetchone()
    if row:
        return base64.b64encode(row["image_blob"]).decode("ascii")

    return None


# ── Result formatting ──

def _format_results(candidates: list[Candidate]) -> list[dict]:
    """Format candidates into the API response shape."""
    return [
        {
            "id": c.model_id,
            "name": c.name,
            "family": c.family,
            "form": c.form,
            "handle_type": c.handle_type,
            "default_blade_length": c.blade_length,
            "score": round(c.score, 1),
            "reasons": c.reasons[:5],
            "vision_match": c.vision_match,
            "vision_reason": c.vision_reason,
            "has_identifier_image": c.has_image,
            "best_colorway_id": c.best_colorway_id,
            "category": None,
            "catalog_line": None,
            "default_steel": None,
            "default_blade_finish": None,
            "is_collab": False,
            "collaboration_name": None,
        }
        for c in candidates
    ]
