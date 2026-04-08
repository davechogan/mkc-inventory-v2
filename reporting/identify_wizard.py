"""Interactive knife identification wizard — pure decision-tree approach.

Server-driven wizard that narrows 87 models to a handful through step-by-step
questions answered by the user. No AI vision — humans are better at visual
comparison than current vision models.

See: artifacts/plans/knife_vision_identification_design.md
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

_log = logging.getLogger(__name__)

# Blade length bins
LENGTH_BINS: dict[int, tuple[float, float]] = {
    1: (0.0, 3.0),
    2: (3.0, 4.5),
    3: (4.5, 7.0),
    4: (7.0, 20.0),
}

# Session expiry (seconds)
SESSION_TTL = 1800  # 30 minutes

# When to stop asking questions and show candidates
MAX_QUESTIONS = 8

# ── Session storage ──

_sessions: dict[str, WizardSession] = {}


@dataclass
class WizardSession:
    session_id: str
    created_at: float
    all_models: list[dict]
    candidate_ids: set[int]
    answers: dict[str, Any] = field(default_factory=dict)
    history: list[tuple[str, Any, set[int]]] = field(default_factory=list)


def _cleanup_expired():
    """Remove sessions older than SESSION_TTL."""
    now = time.time()
    expired = [sid for sid, s in _sessions.items() if now - s.created_at > SESSION_TTL]
    for sid in expired:
        del _sessions[sid]


def get_session(session_id: str) -> Optional[WizardSession]:
    _cleanup_expired()
    return _sessions.get(session_id)


# ── Model loading ──

def _load_models(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT km.id, km.official_name, km.blade_length,
               fam.name AS family_name, frm.name AS form_name,
               kt.name AS knife_type, ht.name AS handle_type,
               bs.name AS blade_steel, bf.name AS blade_finish,
               ks.name AS series_name, c.name AS collaborator_name,
               km.msrp,
               bc.name AS blade_color,
               p.observable_profile_json,
               (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
                     THEN 1 ELSE 0 END) AS has_image
        FROM knife_models_v2 km
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
        LEFT JOIN blade_colors bc ON bc.id = km.blade_color_id
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


# ── Filter functions ──

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
        return {m["id"] for m in models if m["id"] in candidate_ids
                and (m.get("handle_type") == "Paracord"
                     or m.get("_profile", {}).get("handle_type_visual") == "paracord")}
    else:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("handle_type") != "Paracord"
                and m.get("_profile", {}).get("handle_type_visual") != "paracord"}


def _filter_kitchen(candidate_ids: set[int], answer: bool, models: list[dict],
                    conn: Optional[sqlite3.Connection] = None) -> set[int]:
    if answer:
        return {m["id"] for m in models if m["id"] in candidate_ids
                and m.get("knife_type") == "Culinary"}
    else:
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
    if not answer:
        return candidate_ids

    _answer_to_db: dict[str, set[str]] = {
        "Silver": {"steel"},
        "Black": {"black"},
        "Red": {"red"},
        "Coyote": {"coyote"},
    }
    _answer_to_profile: dict[str, set[str]] = {
        "Silver": {"silver"},
        "Black": {"black"},
        "Red": {"red"},
        "Coyote": {"coyote_tan"},
    }
    db_matches = _answer_to_db.get(answer, set())
    profile_matches = _answer_to_profile.get(answer, set())

    keep = set()
    for m in models:
        if m["id"] not in candidate_ids:
            continue
        model_bc = (m.get("blade_color") or "").lower()
        if model_bc and model_bc in db_matches:
            keep.add(m["id"])
            continue
        if not model_bc and conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM model_colorways mc "
                "JOIN blade_colors bc ON bc.id = mc.blade_color_id "
                "WHERE mc.knife_model_id = ? AND lower(bc.name) = lower(?)",
                (m["id"], answer),
            ).fetchone()
            if row and row["cnt"] > 0:
                keep.add(m["id"])
                continue
        profile_color = m.get("_profile", {}).get("blade_color_primary", "")
        if profile_color in profile_matches:
            keep.add(m["id"])
    return keep


def _filter_handle_color(candidate_ids: set[int], answer: str, models: list[dict],
                         conn: Optional[sqlite3.Connection] = None) -> set[int]:
    if not conn or not answer:
        return candidate_ids
    keep = set()
    answer_lower = answer.lower()
    for m in models:
        if m["id"] not in candidate_ids:
            continue
        rows = conn.execute(
            "SELECT hc.name FROM model_colorways mc "
            "LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id "
            "WHERE mc.knife_model_id = ? AND mc.image_blob IS NOT NULL",
            (m["id"],),
        ).fetchall()
        for r in rows:
            hc_name = (r["name"] or "").lower()
            if answer_lower in hc_name or hc_name in answer_lower:
                keep.add(m["id"])
                break
            if answer_lower.split("/")[0] in hc_name or answer_lower.split("/")[-1] in hc_name:
                keep.add(m["id"])
                break
    return keep


def _filter_handle_material(candidate_ids: set[int], answer: str, models: list[dict],
                            conn: Optional[sqlite3.Connection] = None) -> set[int]:
    answer_lower = answer.lower()
    _MATERIAL_GROUPS: dict[str, set[str]] = {
        "carbon fiber": {"carbon fiber", "burled carbon fiber", "black burl carbon fiber",
                         "marbled carbon fiber"},
        "wood": {"desert ironwood", "desert ironwood burl"},
    }
    match_set = _MATERIAL_GROUPS.get(answer_lower, {answer_lower})
    for group_key, group_vals in _MATERIAL_GROUPS.items():
        if answer_lower in group_vals:
            match_set = group_vals
            break

    def _model_matches(m: dict) -> bool:
        return (m.get("handle_type") or "").lower() in match_set

    common_materials = {"g-10", "paracord"}
    if not (match_set & common_materials):
        return {m["id"] for m in models if m["id"] in candidate_ids and _model_matches(m)}
    else:
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
    bin_range = LENGTH_BINS.get(answer)
    if not bin_range:
        return candidate_ids
    lo, hi = bin_range
    margin = 0.5
    lo_adj = max(0.0, lo - margin)
    hi_adj = hi + margin
    return {m["id"] for m in models if m["id"] in candidate_ids
            and m.get("blade_length") is not None
            and lo_adj <= m["blade_length"] < hi_adj}


def _filter_blade_form(candidate_ids: set[int], answer: list[str], models: list[dict],
                       conn: Optional[sqlite3.Connection] = None) -> set[int]:
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
    p_yes = yes_count / total
    p_no = no_count / total
    return -(p_yes * math.log2(p_yes) + p_no * math.log2(p_no))


def _splitting_power_categorical(key: str, candidate_ids: set[int],
                                 models: list[dict], catalog_field: str) -> float:
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
    entropy = 0.0
    for cnt in counts.values():
        p = cnt / total
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


# ── Question definitions ──

@dataclass
class WizardQuestion:
    key: str
    display_text: str
    question_type: str  # "boolean", "single_choice", "multi_choice"
    options: Optional[list[dict]] = None
    visual_aid: Optional[str] = None


QUESTIONS: list[WizardQuestion] = [
    WizardQuestion(
        key="is_hatchet",
        display_text="Is this a hatchet or axe?",
        question_type="boolean",
    ),
    WizardQuestion(
        key="blade_wider_than_handle",
        display_text="Is the blade wider/taller than the handle (like a cleaver)?",
        question_type="boolean",
    ),
    WizardQuestion(
        key="paracord_handle",
        display_text="Is the handle wrapped in paracord or cord?",
        question_type="boolean",
    ),
    WizardQuestion(
        key="kitchen_or_field",
        display_text="Is this a kitchen/culinary knife or a field/hunting knife?",
        question_type="single_choice",
        options=[
            {"value": True, "label": "Kitchen / Culinary"},
            {"value": False, "label": "Field / Hunting / Tactical"},
        ],
    ),
    WizardQuestion(
        key="finger_ring",
        display_text="Is there a finger ring at the front of the handle?",
        question_type="boolean",
    ),
    WizardQuestion(
        key="blade_color",
        display_text="What color is the blade?",
        question_type="single_choice",
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
        options=None,  # Populated from DB at runtime
        visual_aid="color_swatches",
    ),
    WizardQuestion(
        key="handle_material",
        display_text="What is the handle material?",
        question_type="single_choice",
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
        options=None,  # Populated from DB at runtime
        visual_aid="blade_form_silhouettes",
    ),
]

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
    if q.key == "is_hatchet":
        return _splitting_power_boolean(q.key, candidate_ids, models, profile_field="is_hatchet")
    elif q.key == "blade_wider_than_handle":
        return _splitting_power_boolean(q.key, candidate_ids, models, profile_field="blade_wider_than_handle")
    elif q.key == "paracord_handle":
        return _splitting_power_boolean(q.key, candidate_ids, models, catalog_field="handle_type", catalog_value="Paracord")
    elif q.key == "kitchen_or_field":
        return _splitting_power_boolean(q.key, candidate_ids, models, catalog_field="knife_type", catalog_value="Culinary")
    elif q.key == "finger_ring":
        return _splitting_power_boolean(q.key, candidate_ids, models, profile_field="finger_ring_presence")
    elif q.key == "blade_form":
        return _splitting_power_categorical(q.key, candidate_ids, models, catalog_field="form_name")
    elif q.key == "blade_length_bin":
        return _splitting_power_categorical(q.key, candidate_ids, models, catalog_field="blade_length")
    elif q.key == "handle_material":
        return _splitting_power_categorical(q.key, candidate_ids, models, catalog_field="handle_type")
    elif q.key == "handle_color":
        return 0.5 if len(candidate_ids) > 1 else 0.0
    elif q.key == "blade_color":
        return 0.5 if len(candidate_ids) > 1 else 0.0
    return 0.0


def _pick_next_question(session: WizardSession) -> Optional[WizardQuestion]:
    answered_keys = set(session.answers.keys())
    best_q = None
    best_power = -1.0
    for q in QUESTIONS:
        if q.key in answered_keys:
            continue
        power = _compute_splitting_power(q, session.candidate_ids, session.all_models)
        if power > best_power:
            best_power = power
            best_q = q
    if best_q and best_power > 0.01:
        return best_q
    return None


def _remaining_families(session: WizardSession) -> set[str]:
    fams = set()
    for m in session.all_models:
        if m["id"] in session.candidate_ids:
            fams.add(m.get("family_name") or m["official_name"])
    return fams


def _is_done(session: WizardSession) -> bool:
    n_candidates = len(session.candidate_ids)
    n_answered = len(session.answers)

    if n_candidates <= 1:
        return True
    if n_candidates <= 3:
        return True
    if n_answered >= MAX_QUESTIONS:
        return True

    answered_keys = set(session.answers.keys())
    for q in QUESTIONS:
        if q.key in answered_keys:
            continue
        power = _compute_splitting_power(q, session.candidate_ids, session.all_models)
        if power > 0.01:
            return False
    return True


def _populate_dynamic_options(conn: sqlite3.Connection):
    for q in QUESTIONS:
        if q.key == "handle_color" and q.options is None:
            rows = conn.execute("SELECT id, name FROM handle_colors ORDER BY name").fetchall()
            q.options = [{"value": r["name"], "label": r["name"]} for r in rows]
        elif q.key == "blade_form" and q.options is None:
            rows = conn.execute("SELECT id, name FROM knife_forms ORDER BY name").fetchall()
            q.options = [{"value": r["name"], "label": r["name"]} for r in rows]


def _format_question(q: WizardQuestion) -> dict:
    return {
        "key": q.key,
        "display_text": q.display_text,
        "type": q.question_type,
        "options": q.options,
        "visual_aid": q.visual_aid,
    }


def _format_candidates(session: WizardSession, conn: sqlite3.Connection,
                       handle_color: Optional[str] = None) -> list[dict]:
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
    candidates.sort(key=lambda c: (c.get("family") or "", c["name"]))
    return candidates


# ── Public API ──

def start_session(conn: sqlite3.Connection, **kwargs) -> dict:
    """Start a new wizard session. Loads models and returns the first question."""
    _cleanup_expired()
    _populate_dynamic_options(conn)

    session_id = str(uuid.uuid4())
    models = _load_models(conn)
    all_ids = {m["id"] for m in models}

    session = WizardSession(
        session_id=session_id,
        created_at=time.time(),
        all_models=models,
        candidate_ids=set(all_ids),
    )

    _log.info(f"Wizard session {session_id}: {len(models)} models loaded")
    _sessions[session_id] = session

    if _is_done(session):
        return {
            "session_id": session_id,
            "total_models": len(models),
            "remaining_models": len(session.candidate_ids),
            "remaining_families": len(_remaining_families(session)),
            "done": True,
            "candidates": _format_candidates(session, conn),
        }

    next_q = _pick_next_question(session)
    return {
        "session_id": session_id,
        "total_models": len(models),
        "remaining_models": len(session.candidate_ids),
        "remaining_families": len(_remaining_families(session)),
        "done": False,
        "next_question": _format_question(next_q) if next_q else None,
    }


def answer_question(conn: sqlite3.Connection, session_id: str,
                    question_key: str, answer: Any, **kwargs) -> dict:
    """Process a user's answer and return the next question or final candidates."""
    session = get_session(session_id)
    if not session:
        return {"error": "Session not found or expired"}

    session.history.append((question_key, answer, set(session.candidate_ids)))

    eliminated = 0
    if answer is None:
        session.answers[question_key] = None
        _log.info(f"Answer {question_key}=SKIP")
    else:
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

    remaining_fams = _remaining_families(session)
    handle_color = session.answers.get("handle_color")

    if _is_done(session):
        return {
            "remaining_models": len(session.candidate_ids),
            "remaining_families": len(remaining_fams),
            "eliminated_this_step": eliminated,
            "done": True,
            "candidates": _format_candidates(session, conn, handle_color),
        }

    next_q = _pick_next_question(session)
    if not next_q:
        return {
            "remaining_models": len(session.candidate_ids),
            "remaining_families": len(remaining_fams),
            "eliminated_this_step": eliminated,
            "done": True,
            "candidates": _format_candidates(session, conn, handle_color),
        }

    return {
        "remaining_models": len(session.candidate_ids),
        "remaining_families": len(remaining_fams),
        "eliminated_this_step": eliminated,
        "done": False,
        "next_question": _format_question(next_q),
    }


def go_back(conn: sqlite3.Connection, session_id: str) -> dict:
    session = get_session(session_id)
    if not session:
        return {"error": "Session not found or expired"}
    if not session.history:
        return {"error": "No previous step to go back to"}

    question_key, prev_answer, prev_candidates = session.history.pop()
    session.candidate_ids = prev_candidates
    session.answers.pop(question_key, None)

    q = next((q for q in QUESTIONS if q.key == question_key), None)
    return {
        "remaining_models": len(session.candidate_ids),
        "remaining_families": len(_remaining_families(session)),
        "done": False,
        "next_question": _format_question(q) if q else None,
    }


def get_session_state(session_id: str) -> dict:
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
        "history_depth": len(session.history),
    }
