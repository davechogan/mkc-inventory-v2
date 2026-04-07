"""Interactive knife identification wizard — adaptive decision-tree approach.

Server-driven wizard that narrows 87 models to a handful through step-by-step
questions. The vision model suggests answers; the human confirms.

See: artifacts/plans/knife_vision_identification_design.md
"""

from __future__ import annotations

import base64
import json
import logging
import math
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

_log = logging.getLogger(__name__)

# Blade length bins (same as existing pipeline)
LENGTH_BINS: dict[int, tuple[float, float]] = {
    1: (0.0, 3.0),
    2: (3.0, 4.5),
    3: (4.5, 7.0),
    4: (7.0, 20.0),
}

LENGTH_BIN_LABELS: dict[int, str] = {
    1: 'Under 3"',
    2: '3" to 4.5"',
    3: '4.5" to 7"',
    4: '7" and above',
}

# Session expiry (seconds)
SESSION_TTL = 1800  # 30 minutes

# When to stop asking questions and show candidates
MAX_CANDIDATES_DONE = 8
MAX_FAMILIES_DONE = 5
MAX_QUESTIONS = 8

# ── Session storage ──

_sessions: dict[str, WizardSession] = {}


@dataclass
class WizardSession:
    session_id: str
    created_at: float
    image_b64: Optional[str]
    clean_image_b64: Optional[str]
    all_models: list[dict]
    candidate_ids: set[int]
    answers: dict[str, Any] = field(default_factory=dict)
    history: list[tuple[str, Any, set[int]]] = field(default_factory=list)
    vision_cache: dict[str, Any] = field(default_factory=dict)
    auto_gates: list[dict] = field(default_factory=list)
    _conn: Optional[sqlite3.Connection] = field(default=None, repr=False)


def _cleanup_expired():
    """Remove sessions older than SESSION_TTL."""
    now = time.time()
    expired = [sid for sid, s in _sessions.items() if now - s.created_at > SESSION_TTL]
    for sid in expired:
        del _sessions[sid]


def get_session(session_id: str) -> Optional[WizardSession]:
    _cleanup_expired()
    return _sessions.get(session_id)


# ── Model loading (copied from identify_pipeline, self-contained) ──

def _load_models(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT km.id, km.official_name, km.blade_length,
               fam.name AS family_name, frm.name AS form_name,
               kt.name AS knife_type, ht.name AS handle_type,
               bs.name AS blade_steel, bf.name AS blade_finish,
               ks.name AS series_name, c.name AS collaborator_name,
               km.msrp,
               p.observable_profile_json,
               (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
                     THEN 1 ELSE 0 END) AS has_image
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
    models = []
    for r in rows:
        m = dict(r)
        # Pre-parse the profile JSON
        if m.get("observable_profile_json"):
            try:
                m["_profile"] = json.loads(m["observable_profile_json"])
            except (json.JSONDecodeError, TypeError):
                m["_profile"] = {}
        else:
            m["_profile"] = {}
        models.append(m)
    return models


def _find_best_colorway_id(
    conn: sqlite3.Connection, model_id: int, handle_color: Optional[str] = None
) -> Optional[int]:
    """Find best-matching colorway ID. Priority: matching color > Orange/Black > Black > any."""
    queries = []
    if handle_color:
        queries.append((
            "SELECT mc.id FROM model_colorways mc "
            "LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id "
            "WHERE mc.knife_model_id = ? AND lower(hc.name) = lower(?) "
            "AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0 LIMIT 1",
            (model_id, handle_color),
        ))
    queries.append((
        "SELECT mc.id FROM model_colorways mc "
        "LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id "
        "WHERE mc.knife_model_id = ? AND lower(hc.name) = 'orange/black' "
        "AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0 LIMIT 1",
        (model_id,),
    ))
    queries.append((
        "SELECT mc.id FROM model_colorways mc "
        "LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id "
        "WHERE mc.knife_model_id = ? AND lower(hc.name) = 'black' "
        "AND mc.image_blob IS NOT NULL AND length(mc.image_blob) > 0 LIMIT 1",
        (model_id,),
    ))
    queries.append((
        "SELECT mc.id FROM model_colorways mc "
        "WHERE mc.knife_model_id = ? AND mc.image_blob IS NOT NULL "
        "AND length(mc.image_blob) > 0 LIMIT 1",
        (model_id,),
    ))
    for sql, params in queries:
        row = conn.execute(sql, params).fetchone()
        if row:
            return row["id"]
    return None


# ── Question definitions ──

@dataclass
class WizardQuestion:
    key: str
    display_text: str
    question_type: str  # "boolean", "single_choice", "multi_choice"
    vision_prompt: Optional[str]
    vision_reliability: float  # 0.0–1.0 from empirical testing
    options: Optional[list[dict]] = None  # for choice types
    visual_aid: Optional[str] = None  # "blade_form_silhouettes", etc.
    auto_gate: bool = False  # True = apply without user confirmation if 100% reliable


def _get_profile(models: list[dict], model_id: int) -> dict:
    """Get the parsed vision profile for a model."""
    for m in models:
        if m["id"] == model_id:
            return m.get("_profile", {})
    return {}


# ── Filter functions ──
# Each takes (candidate_ids, answer, all_models, conn) and returns new candidate_ids

def _filter_hatchet(candidate_ids: set[int], answer: bool, models: list[dict],
                    conn: Optional[sqlite3.Connection] = None) -> set[int]:
    if answer:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and (m.get("form_name") == "Hatchet"
                     or m.get("_profile", {}).get("is_hatchet") is True)}
    else:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("form_name") != "Hatchet"
                and m.get("_profile", {}).get("is_hatchet") is not True}


def _filter_blade_wider(candidate_ids: set[int], answer: bool, models: list[dict],
                        conn: Optional[sqlite3.Connection] = None) -> set[int]:
    if answer:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and (m.get("form_name") in ("Cleaver",)
                     or m.get("_profile", {}).get("blade_wider_than_handle") is True)}
    else:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("form_name") not in ("Cleaver",)
                and m.get("_profile", {}).get("blade_wider_than_handle") is not True}


def _filter_paracord(candidate_ids: set[int], answer: bool, models: list[dict],
                     conn: Optional[sqlite3.Connection] = None) -> set[int]:
    if answer:
        # Keep only models with paracord handle type or profile
        return {m["id"] for m in models if m["id"] in candidate_ids
                and (m.get("handle_type") == "Paracord"
                     or m.get("_profile", {}).get("handle_type_visual") == "paracord")}
    else:
        # Remove paracord-ONLY families (all models in family are paracord)
        # But keep families where some models have non-paracord options
        by_family: dict[str, list[dict]] = {}
        for m in models:
            if m["id"] in candidate_ids:
                fam = m.get("family_name") or m["official_name"]
                by_family.setdefault(fam, []).append(m)

        keep = set()
        for fam, members in by_family.items():
            # Keep if at least one model in the family is NOT paracord
            has_non_paracord = any(
                m.get("handle_type") != "Paracord"
                and m.get("_profile", {}).get("handle_type_visual") != "paracord"
                for m in members
            )
            if has_non_paracord:
                keep.update(m["id"] for m in members)
            # If ALL are paracord, eliminate the whole family
        return keep


def _filter_kitchen(candidate_ids: set[int], answer: bool, models: list[dict],
                    conn: Optional[sqlite3.Connection] = None) -> set[int]:
    if answer:  # user says kitchen
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("knife_type") == "Culinary"}
    else:  # user says field
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("knife_type") != "Culinary"}


def _filter_finger_ring(candidate_ids: set[int], answer: bool, models: list[dict],
                        conn: Optional[sqlite3.Connection] = None) -> set[int]:
    if answer:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("_profile", {}).get("finger_ring_presence") is True}
    else:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("_profile", {}).get("finger_ring_presence") is not True}


def _filter_blade_color(candidate_ids: set[int], answer: str, models: list[dict],
                        conn: Optional[sqlite3.Connection] = None) -> set[int]:
    """Filter by blade color using colorway data + model profiles.

    Loose: NULL blade_color in colorways means 'don't exclude'.
    Only distinctive colors (Red, Coyote) do hard elimination.
    """
    if not conn or not answer:
        return candidate_ids

    color_map = {
        "Silver": "steel", "Black": "black", "Red": "red",
        "Coyote": "coyote", "Distressed Gray": "distressed gray",
        "Damascus Wood Grain": "damascus wood grain",
    }
    db_color = color_map.get(answer, answer.lower())

    # For distinctive colors (red, coyote), hard-filter
    if answer in ("Red", "Coyote"):
        keep = set()
        for m in models:
            if m["id"] not in candidate_ids:
                continue
            # Check model profile for blade color
            profile_color = m.get("_profile", {}).get("blade_color_primary", "")
            profile_map = {"red": "Red", "coyote_tan": "Coyote", "black": "Black",
                           "silver": "Steel"}
            if profile_color and profile_map.get(profile_color) == answer:
                keep.add(m["id"])
                continue
            # Check colorway blade colors — only explicit matches, NOT NULLs
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM model_colorways mc "
                "JOIN blade_colors bc ON bc.id = mc.blade_color_id "
                "WHERE mc.knife_model_id = ? AND lower(bc.name) = lower(?)",
                (m["id"], answer),
            ).fetchone()
            if row and row["cnt"] > 0:
                keep.add(m["id"])
        return keep
    else:
        # Silver/Black — too common, don't hard-filter (most have NULL blade_color)
        return candidate_ids


def _filter_handle_color(candidate_ids: set[int], answer: str, models: list[dict],
                         conn: Optional[sqlite3.Connection] = None) -> set[int]:
    """Filter by handle color using colorways. Loose match — substring matching."""
    if not conn or not answer:
        return candidate_ids

    keep = set()
    answer_lower = answer.lower()

    for m in models:
        if m["id"] not in candidate_ids:
            continue
        # Check if any colorway has a matching handle color
        rows = conn.execute(
            "SELECT hc.name FROM model_colorways mc "
            "LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id "
            "WHERE mc.knife_model_id = ? AND mc.image_blob IS NOT NULL",
            (m["id"],),
        ).fetchall()
        for r in rows:
            hc_name = (r["name"] or "").lower()
            # Loose match: "red" matches "red/black", "black/red", etc.
            if answer_lower in hc_name or hc_name in answer_lower:
                keep.add(m["id"])
                break
            # Also match "orange" to "blaze orange", "orange/black"
            if answer_lower.split("/")[0] in hc_name or answer_lower.split("/")[-1] in hc_name:
                keep.add(m["id"])
                break
        else:
            # No matching colorway — but don't eliminate if few colorways
            # (some models might just not have the color photographed)
            if len(rows) <= 1:
                keep.add(m["id"])

    return keep


def _filter_handle_material(candidate_ids: set[int], answer: str, models: list[dict],
                            conn: Optional[sqlite3.Connection] = None) -> set[int]:
    """Filter by handle material.

    Groups related materials (all Carbon Fiber variants together, all Ironwood together).
    Then filters at model level for distinctive groups, family level for common ones.
    """
    answer_lower = answer.lower()

    # Material groups — user picks a group, matches all variants
    _MATERIAL_GROUPS: dict[str, set[str]] = {
        "carbon fiber": {"carbon fiber", "burled carbon fiber", "black burl carbon fiber",
                         "marbled carbon fiber"},
        "wood": {"desert ironwood", "desert ironwood burl"},
    }

    # Expand answer to a set of matching types
    match_set = _MATERIAL_GROUPS.get(answer_lower, {answer_lower})

    # Also check if any group contains the answer
    for group_key, group_vals in _MATERIAL_GROUPS.items():
        if answer_lower in group_vals:
            match_set = group_vals
            break

    def _model_matches(m: dict) -> bool:
        ht = (m.get("handle_type") or "").lower()
        return ht in match_set

    # If the match set is distinctive (not G-10, not Paracord), filter at model level
    common_materials = {"g-10", "paracord"}
    if not (match_set & common_materials):
        return {m["id"] for m in models if m["id"] in candidate_ids and _model_matches(m)}
    else:
        # Common materials — filter at family level
        by_family: dict[str, list[dict]] = {}
        for m in models:
            if m["id"] in candidate_ids:
                fam = m.get("family_name") or m["official_name"]
                by_family.setdefault(fam, []).append(m)

        keep = set()
        for fam, members in by_family.items():
            if any(_model_matches(m) for m in members):
                keep.update(m["id"] for m in members)
        return keep


def _filter_blade_length(candidate_ids: set[int], answer: int, models: list[dict],
                         conn: Optional[sqlite3.Connection] = None) -> set[int]:
    """Filter by blade length bin."""
    bin_range = LENGTH_BINS.get(answer)
    if not bin_range:
        return candidate_ids
    lo, hi = bin_range
    return {m["id"] for m in models if m["id"] in candidate_ids
            and m.get("blade_length") is not None
            and lo <= m["blade_length"] < hi}


def _filter_blade_form(candidate_ids: set[int], answer: list[str], models: list[dict],
                       conn: Optional[sqlite3.Connection] = None) -> set[int]:
    """Filter by blade form. Multi-select — keep models matching any selected form."""
    if not answer:
        return candidate_ids
    forms_lower = {f.lower() for f in answer}
    return {m["id"] for m in models if m["id"] in candidate_ids
            and (m.get("form_name") or "").lower() in forms_lower}


# ── Splitting power computation ──

def _splitting_power_boolean(key: str, candidate_ids: set[int], models: list[dict],
                             profile_field: Optional[str] = None,
                             catalog_field: Optional[str] = None,
                             catalog_value: Optional[str] = None) -> float:
    """Compute splitting power for a boolean question using Gini impurity reduction."""
    if len(candidate_ids) <= 1:
        return 0.0

    yes_count = 0
    no_count = 0
    for m in models:
        if m["id"] not in candidate_ids:
            continue
        is_yes = False
        if profile_field:
            is_yes = m.get("_profile", {}).get(profile_field) is True
        elif catalog_field and catalog_value:
            is_yes = m.get(catalog_field) == catalog_value
        if is_yes:
            yes_count += 1
        else:
            no_count += 1

    total = yes_count + no_count
    if total == 0 or yes_count == 0 or no_count == 0:
        return 0.0

    # Information gain — best when close to 50/50 split
    p_yes = yes_count / total
    p_no = no_count / total
    entropy = -(p_yes * math.log2(p_yes) + p_no * math.log2(p_no))
    return entropy


def _splitting_power_categorical(key: str, candidate_ids: set[int],
                                 models: list[dict],
                                 catalog_field: str) -> float:
    """Compute splitting power for a categorical question."""
    if len(candidate_ids) <= 1:
        return 0.0

    counts: dict[str, int] = {}
    for m in models:
        if m["id"] not in candidate_ids:
            continue
        val = m.get(catalog_field) or "unknown"
        counts[val] = counts.get(val, 0) + 1

    total = sum(counts.values())
    if total == 0 or len(counts) <= 1:
        return 0.0

    # Entropy
    entropy = 0.0
    for cnt in counts.values():
        p = cnt / total
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


# ── Question registry ──

QUESTIONS: list[WizardQuestion] = [
    WizardQuestion(
        key="is_hatchet",
        display_text="Is this a hatchet or axe?",
        question_type="boolean",
        vision_prompt=(
            "Look at this knife or tool. Is it a hatchet or axe shape — "
            "meaning it has a wide chopping head mounted on a handle below? "
            'Answer ONLY "yes" or "no".'
        ),
        vision_reliability=1.0,
        auto_gate=True,
    ),
    WizardQuestion(
        key="blade_wider_than_handle",
        display_text="Is the blade wider/taller than the handle?",
        question_type="boolean",
        vision_prompt=(
            "Look at this knife. Is the blade clearly wider (taller) than the handle? "
            "A cleaver has a blade much taller than its handle. Most hunting knives do not. "
            'Answer ONLY "yes" or "no".'
        ),
        vision_reliability=1.0,
        auto_gate=True,
    ),
    WizardQuestion(
        key="paracord_handle",
        display_text="Is the handle wrapped in paracord or cord?",
        question_type="boolean",
        vision_prompt=(
            "Look at the handle of this knife. Is the handle wrapped in paracord or cord? "
            "Paracord handles have a visible woven/wrapped texture. Solid handles (G-10, "
            'wood, micarta) are smooth or textured but not wrapped. Answer ONLY "yes" or "no".'
        ),
        vision_reliability=1.0,
    ),
    WizardQuestion(
        key="kitchen_or_field",
        display_text="Is this a kitchen/culinary knife or a field/hunting knife?",
        question_type="single_choice",
        vision_prompt=(
            "Is this a kitchen/culinary knife (chef knife, paring knife, butcher knife, "
            "santoku, cleaver — designed for food preparation) or a field/hunting/tactical "
            'knife? Answer ONLY "kitchen" or "field".'
        ),
        vision_reliability=0.8,
        options=[
            {"value": True, "label": "Kitchen / Culinary"},
            {"value": False, "label": "Field / Hunting / Tactical"},
        ],
    ),
    WizardQuestion(
        key="finger_ring",
        display_text="Is there a finger ring at the front of the handle?",
        question_type="boolean",
        vision_prompt=(
            "Look at this knife where the blade meets the handle. "
            "Is there a finger ring or circular loop guard at the front of the handle "
            "(a ring you could put your finger through)? "
            'Answer ONLY "yes" or "no".'
        ),
        vision_reliability=0.94,
    ),
    WizardQuestion(
        key="blade_color",
        display_text="What color is the blade?",
        question_type="single_choice",
        vision_prompt=None,  # User picks — no vision suggestion
        vision_reliability=0.0,
        options=[
            {"value": "Silver", "label": "Silver / Satin / Stonewashed", "color": "#C0C0C0"},
            {"value": "Black", "label": "Black / Dark (PVD, Cerakote)", "color": "#2a2a2a"},
            {"value": "Red", "label": "Red (Cerakote)", "color": "#cc0000"},
            {"value": "Coyote", "label": "Coyote / Tan", "color": "#b8860b"},
        ],
    ),
    WizardQuestion(
        key="handle_color",
        display_text="What is the primary handle color?",
        question_type="single_choice",
        vision_prompt=None,  # User picks
        vision_reliability=0.0,
        options=None,  # Populated from DB at runtime
        visual_aid="color_swatches",
    ),
    WizardQuestion(
        key="handle_material",
        display_text="What is the handle material?",
        question_type="single_choice",
        vision_prompt=None,
        vision_reliability=0.0,
        options=[
            {"value": "G-10", "label": "G-10 (solid, textured scales)"},
            {"value": "Paracord", "label": "Paracord (cord-wrapped)"},
            {"value": "Carbon Fiber", "label": "Carbon Fiber (any pattern)"},
            {"value": "Wood", "label": "Wood (natural grain)"},
        ],
    ),
    WizardQuestion(
        key="blade_length_bin",
        display_text="Approximately how long is the blade?",
        question_type="single_choice",
        vision_prompt=None,
        vision_reliability=0.0,
        options=[
            {"value": 1, "label": 'Under 3"'},
            {"value": 2, "label": '3" to 4.5"'},
            {"value": 3, "label": '4.5" to 7"'},
            {"value": 4, "label": '7" and above'},
        ],
    ),
    WizardQuestion(
        key="blade_form",
        display_text="Which blade shape(s) match your knife?",
        question_type="multi_choice",
        vision_prompt=None,  # Vision suggests from silhouettes
        vision_reliability=0.0,
        options=None,  # Populated from DB at runtime
        visual_aid="blade_form_silhouettes",
    ),
]

# Map question key to filter function
_FILTER_FNS: dict[str, Callable] = {
    "is_hatchet": _filter_hatchet,
    "blade_wider_than_handle": _filter_blade_wider,
    "paracord_handle": _filter_paracord,
    "kitchen_or_field": _filter_kitchen,
    "finger_ring": _filter_finger_ring,
    "blade_color": _filter_blade_color,
    "handle_color": _filter_handle_color,
    "handle_material": _filter_handle_material,
    "blade_length_bin": _filter_blade_length,
    "blade_form": _filter_blade_form,
}


def _compute_splitting_power(q: WizardQuestion, candidate_ids: set[int],
                             models: list[dict]) -> float:
    """Compute how well a question splits the current candidate set."""
    if q.key == "is_hatchet":
        return _splitting_power_boolean(q.key, candidate_ids, models,
                                        profile_field="is_hatchet")
    elif q.key == "blade_wider_than_handle":
        return _splitting_power_boolean(q.key, candidate_ids, models,
                                        profile_field="blade_wider_than_handle")
    elif q.key == "paracord_handle":
        return _splitting_power_boolean(q.key, candidate_ids, models,
                                        catalog_field="handle_type",
                                        catalog_value="Paracord")
    elif q.key == "kitchen_or_field":
        return _splitting_power_boolean(q.key, candidate_ids, models,
                                        catalog_field="knife_type",
                                        catalog_value="Culinary")
    elif q.key == "finger_ring":
        return _splitting_power_boolean(q.key, candidate_ids, models,
                                        profile_field="finger_ring_presence")
    elif q.key == "blade_form":
        return _splitting_power_categorical(q.key, candidate_ids, models,
                                           catalog_field="form_name")
    elif q.key == "blade_length_bin":
        return _splitting_power_categorical(q.key, candidate_ids, models,
                                           catalog_field="blade_length")
    elif q.key in ("blade_color", "handle_color", "handle_material"):
        # User-input questions — always high priority since they're free (no vision call)
        return 0.8
    return 0.0


def _pick_next_question(session: WizardSession) -> Optional[WizardQuestion]:
    """Choose the next question that best splits the current candidate set."""
    answered_keys = set(session.answers.keys())

    best_q = None
    best_power = -1.0

    for q in QUESTIONS:
        if q.key in answered_keys:
            continue
        if q.auto_gate and session.image_b64:
            continue  # Auto-gates already applied at session start (image present)

        power = _compute_splitting_power(q, session.candidate_ids, session.all_models)
        if power > best_power:
            best_power = power
            best_q = q

    if best_q and best_power > 0.01:
        return best_q
    return None


def _get_vision_suggestion(
    session: WizardSession, question: WizardQuestion,
    vision_model: str, vision_fn: Any,
) -> Optional[Any]:
    """Get the vision model's suggestion for a question."""
    if not question.vision_prompt or not session.image_b64 or not vision_fn:
        return None

    # Check cache
    if question.key in session.vision_cache:
        return session.vision_cache[question.key]

    raw = vision_fn(vision_model, "", question.vision_prompt,
                    images_b64=[session.clean_image_b64 or session.image_b64])

    suggestion = None
    raw_lower = raw.lower().strip()[:30] if raw else ""

    if question.question_type == "boolean":
        suggestion = "yes" in raw_lower[:15]
    elif question.key == "kitchen_or_field":
        suggestion = "kitchen" in raw_lower[:20]

    session.vision_cache[question.key] = suggestion
    _log.info(f"Vision suggestion for {question.key}: {suggestion} (raw: {raw_lower})")
    return suggestion


def _remaining_families(session: WizardSession) -> set[str]:
    """Get the set of remaining family names."""
    fams = set()
    for m in session.all_models:
        if m["id"] in session.candidate_ids:
            fams.add(m.get("family_name") or m["official_name"])
    return fams


def _is_done(session: WizardSession) -> bool:
    """Check if the wizard should stop and show candidates."""
    n_candidates = len(session.candidate_ids)
    n_families = len(_remaining_families(session))
    n_answered = len(session.answers)

    if n_candidates <= MAX_CANDIDATES_DONE:
        return True
    if n_families <= MAX_FAMILIES_DONE:
        return True
    if n_answered >= MAX_QUESTIONS:
        return True
    return False


def _populate_dynamic_options(conn: sqlite3.Connection):
    """Load dynamic option lists from the DB into question definitions."""
    for q in QUESTIONS:
        if q.key == "handle_color" and q.options is None:
            rows = conn.execute("SELECT id, name FROM handle_colors ORDER BY name").fetchall()
            q.options = [{"value": r["name"], "label": r["name"]} for r in rows]
        # handle_material has hardcoded grouped options — don't overwrite
        elif q.key == "blade_form" and q.options is None:
            rows = conn.execute("SELECT id, name FROM knife_forms ORDER BY name").fetchall()
            q.options = [{"value": r["name"], "label": r["name"]} for r in rows]


def _format_question(q: WizardQuestion, suggestion: Optional[Any] = None) -> dict:
    """Format a question for the API response."""
    return {
        "key": q.key,
        "display_text": q.display_text,
        "type": q.question_type,
        "options": q.options,
        "visual_aid": q.visual_aid,
        "vision_suggestion": suggestion,
        "vision_reliability": q.vision_reliability,
    }


def _format_candidates(session: WizardSession, conn: sqlite3.Connection,
                       handle_color: Optional[str] = None) -> list[dict]:
    """Format remaining candidates for the final display."""
    candidates = []
    for m in session.all_models:
        if m["id"] not in session.candidate_ids:
            continue
        cw_id = _find_best_colorway_id(conn, m["id"], handle_color)
        candidates.append({
            "model_id": m["id"],
            "name": m["official_name"],
            "family": m.get("family_name"),
            "form": m.get("form_name"),
            "handle_type": m.get("handle_type"),
            "blade_length": m.get("blade_length"),
            "blade_steel": m.get("blade_steel"),
            "blade_finish": m.get("blade_finish"),
            "msrp": m.get("msrp"),
            "best_colorway_id": cw_id,
            "has_image": bool(m.get("has_image")),
        })
    # Sort by family then name
    candidates.sort(key=lambda c: (c.get("family") or "", c["name"]))
    return candidates


# ── Public API ──

def start_session(
    conn: sqlite3.Connection,
    image_b64: Optional[str],
    vision_model: str = "",
    vision_fn: Any = None,
) -> dict:
    """Start a new wizard session.

    Loads models, optionally processes image, runs auto-gates (hatchet, cleaver),
    and returns the first question.
    """
    _cleanup_expired()
    _populate_dynamic_options(conn)

    session_id = str(uuid.uuid4())
    models = _load_models(conn)
    all_ids = {m["id"] for m in models}

    # Background removal
    clean_image = None
    if image_b64:
        try:
            from blade_ai import _remove_background
            clean_image = _remove_background(image_b64)
        except Exception:
            clean_image = image_b64

    session = WizardSession(
        session_id=session_id,
        created_at=time.time(),
        image_b64=image_b64,
        clean_image_b64=clean_image,
        all_models=models,
        candidate_ids=set(all_ids),
        _conn=conn,
    )

    _log.info(f"Wizard session {session_id}: {len(models)} models loaded")

    # Run auto-gates if we have an image and vision model
    if image_b64 and vision_fn:
        for q in QUESTIONS:
            if not q.auto_gate or not q.vision_prompt:
                continue
            suggestion = _get_vision_suggestion(session, q, vision_model, vision_fn)
            if suggestion is not None:
                # Auto-gates: apply the vision answer directly
                filter_fn = _FILTER_FNS.get(q.key)
                if filter_fn:
                    new_ids = filter_fn(session.candidate_ids, suggestion, models, conn)
                    eliminated = len(session.candidate_ids) - len(new_ids)
                    session.candidate_ids = new_ids
                    session.answers[q.key] = suggestion
                    session.auto_gates.append({
                        "question": q.key,
                        "display_text": q.display_text,
                        "vision_answer": suggestion,
                        "eliminated": eliminated,
                    })
                    _log.info(f"Auto-gate {q.key}={suggestion}: eliminated {eliminated}")

    _sessions[session_id] = session

    # Determine first question
    if _is_done(session):
        return {
            "session_id": session_id,
            "total_models": len(models),
            "remaining_models": len(session.candidate_ids),
            "remaining_families": len(_remaining_families(session)),
            "auto_gates": session.auto_gates,
            "done": True,
            "candidates": _format_candidates(session, conn),
        }

    next_q = _pick_next_question(session)
    suggestion = None
    if next_q and next_q.vision_prompt and vision_fn and image_b64:
        suggestion = _get_vision_suggestion(session, next_q, vision_model, vision_fn)

    return {
        "session_id": session_id,
        "total_models": len(models),
        "remaining_models": len(session.candidate_ids),
        "remaining_families": len(_remaining_families(session)),
        "auto_gates": session.auto_gates,
        "done": False,
        "next_question": _format_question(next_q, suggestion) if next_q else None,
    }


def answer_question(
    conn: sqlite3.Connection,
    session_id: str,
    question_key: str,
    answer: Any,
    vision_model: str = "",
    vision_fn: Any = None,
) -> dict:
    """Process a user's answer and return the next question or final candidates."""
    session = get_session(session_id)
    if not session:
        return {"error": "Session not found or expired"}

    # Save history for undo
    session.history.append((question_key, answer, set(session.candidate_ids)))

    # Skip if answer is None (user said "I don't know")
    eliminated = 0
    if answer is None:
        session.answers[question_key] = None
        _log.info(f"Answer {question_key}=SKIP (I don't know)")
    else:
        # Apply filter
        filter_fn = _FILTER_FNS.get(question_key)
        if filter_fn:
            prev_count = len(session.candidate_ids)
            session.candidate_ids = filter_fn(
                session.candidate_ids, answer, session.all_models, conn
            )
            eliminated = prev_count - len(session.candidate_ids)
            session.answers[question_key] = answer
            _log.info(f"Answer {question_key}={answer}: {eliminated} eliminated, "
                      f"{len(session.candidate_ids)} remaining")
        else:
            session.answers[question_key] = answer
            _log.warning(f"No filter function for question {question_key}")

    remaining_fams = _remaining_families(session)
    handle_color = session.answers.get("handle_color")

    if _is_done(session):
        return {
            "remaining_models": len(session.candidate_ids),
            "remaining_families": len(remaining_fams),
            "eliminated_this_step": eliminated if filter_fn else 0,
            "done": True,
            "candidates": _format_candidates(session, conn, handle_color),
        }

    # Pick next question
    next_q = _pick_next_question(session)
    if not next_q:
        return {
            "remaining_models": len(session.candidate_ids),
            "remaining_families": len(remaining_fams),
            "eliminated_this_step": eliminated if filter_fn else 0,
            "done": True,
            "candidates": _format_candidates(session, conn, handle_color),
        }

    # Get vision suggestion for next question
    suggestion = None
    if next_q.vision_prompt and vision_fn and session.image_b64:
        suggestion = _get_vision_suggestion(session, next_q, vision_model, vision_fn)

    return {
        "remaining_models": len(session.candidate_ids),
        "remaining_families": len(remaining_fams),
        "eliminated_this_step": eliminated if filter_fn else 0,
        "done": False,
        "next_question": _format_question(next_q, suggestion),
    }


def go_back(conn: sqlite3.Connection, session_id: str) -> dict:
    """Undo the last answer and return the question to re-answer."""
    session = get_session(session_id)
    if not session:
        return {"error": "Session not found or expired"}

    if not session.history:
        return {"error": "No previous step to go back to"}

    question_key, prev_answer, prev_candidates = session.history.pop()
    session.candidate_ids = prev_candidates
    session.answers.pop(question_key, None)
    session.vision_cache.pop(question_key, None)

    # Find the question to re-present
    q = next((q for q in QUESTIONS if q.key == question_key), None)

    return {
        "remaining_models": len(session.candidate_ids),
        "remaining_families": len(_remaining_families(session)),
        "done": False,
        "next_question": _format_question(q) if q else None,
    }


def get_session_state(session_id: str) -> dict:
    """Return current session state for debugging."""
    session = get_session(session_id)
    if not session:
        return {"error": "Session not found or expired"}

    return {
        "session_id": session.session_id,
        "created_at": session.created_at,
        "total_models": len(session.all_models),
        "remaining_models": len(session.candidate_ids),
        "remaining_families": len(_remaining_families(session)),
        "answers": session.answers,
        "auto_gates": session.auto_gates,
        "history_depth": len(session.history),
        "has_image": session.image_b64 is not None,
    }
