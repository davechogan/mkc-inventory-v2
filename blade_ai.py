"""
Ollama (local LLM) integration for blade identification and OpenCV silhouette hints.

Environment:
    OLLAMA_HOST — base URL, default ``http://192.168.50.196:11434``
"""
from __future__ import annotations

import base64
import json
import math
import os
from pathlib import Path
from typing import Any, Optional

import httpx

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://192.168.50.196:11434").rstrip("/")

# Normalized 0–100 coords; blade generally points right (tip high-x).
SEED_POLYGONS: dict[str, tuple[str, str, list[list[int]]]] = {
    # ── Blade-only side profiles ──
    # Coordinates are 0-100 normalized. Blade points RIGHT.
    # Left edge = guard/handle junction (vertical). Top = spine. Bottom = edge.
    "drop_point": (
        "Drop Point",
        "Spine curves gently down to tip; moderate belly curve on edge.",
        [
            # Guard top → spine → tip → edge → guard bottom
            [5, 30], [15, 28], [30, 27], [50, 28], [70, 32], [85, 40], [95, 50],
            [85, 60], [70, 66], [50, 70], [30, 72], [15, 72], [5, 70],
        ],
    ),
    "trailing_point": (
        "Trailing Point",
        "Spine curves UPWARD toward tip; tip is above the spine line. Edge is relatively straight.",
        [
            # Guard top → spine rises → tip high → edge → guard bottom
            [5, 38], [15, 35], [30, 30], [50, 24], [70, 18], [85, 16], [95, 22],
            [90, 35], [80, 48], [65, 58], [50, 64], [30, 68], [15, 68], [5, 68],
        ],
    ),
    "clip_point": (
        "Clip Point",
        "Spine runs straight then clips concave down to tip. Strong fine point.",
        [
            # Guard top → straight spine → concave clip → tip → belly → guard bottom
            [5, 28], [20, 27], [40, 27], [55, 27], [65, 28], [75, 32], [85, 40], [95, 50],
            [85, 62], [70, 68], [50, 72], [30, 72], [15, 72], [5, 70],
        ],
    ),
    "sheepsfoot": (
        "Sheepsfoot",
        "Straight cutting edge; spine curves down to meet it at a blunt tip.",
        [
            # Guard top → spine drops → rounded tip → straight edge → guard bottom
            [5, 28], [20, 28], [40, 28], [55, 30], [70, 36], [82, 48], [90, 60], [92, 68],
            [80, 70], [60, 70], [40, 70], [20, 70], [5, 70],
        ],
    ),
    "spear": (
        "Spear Point",
        "Symmetric profile; spine and edge mirror each other toward a centerline tip.",
        [
            # Guard top → spine → tip center → edge → guard bottom
            [5, 32], [15, 30], [30, 28], [50, 30], [70, 36], [85, 44], [95, 50],
            [85, 56], [70, 64], [50, 70], [30, 72], [15, 70], [5, 68],
        ],
    ),
    "dagger": (
        "Dagger",
        "Double-edged, symmetric top and bottom, tapering to a fine point.",
        [
            # Guard top → top edge → tip → bottom edge → guard bottom
            [5, 35], [15, 32], [30, 30], [50, 32], [70, 38], [85, 44], [95, 50],
            [85, 56], [70, 62], [50, 68], [30, 70], [15, 68], [5, 65],
        ],
    ),
    "hawkbill": (
        "Hawkbill",
        "Both spine and edge curve inward/downward like a talon; tip points down.",
        [
            # Guard top → spine dips → tip curves down → edge recurves → guard bottom
            [5, 30], [15, 28], [30, 30], [45, 35], [60, 42], [75, 55], [85, 68], [90, 78],
            [85, 75], [75, 65], [60, 62], [45, 65], [30, 68], [15, 68], [5, 65],
        ],
    ),
    "skinner_belly": (
        "Skinner",
        "Pronounced convex belly curve; blunt swept-up tip for skinning.",
        [
            # Guard top → flat spine → blunt tip → deep belly → guard bottom
            [5, 28], [20, 27], [40, 27], [60, 28], [78, 32], [90, 40], [95, 48],
            [90, 55], [78, 65], [60, 75], [40, 78], [25, 75], [15, 72], [5, 68],
        ],
    ),
    "fillet": (
        "Fillet",
        "Long, thin, flexible blade with gentle upward curve to a fine tip.",
        [
            # Guard top → thin spine → fine tip → gentle belly → guard bottom
            [5, 40], [15, 38], [30, 37], [50, 36], [70, 37], [85, 42], [95, 48],
            [85, 54], [70, 57], [50, 60], [30, 62], [15, 62], [5, 60],
        ],
    ),
    "chef_rocker": (
        "Chef",
        "Tall at heel, long rocker belly curve, spine tapers to tip.",
        [
            # Guard top → spine tapers → tip → big rocker belly → tall heel → guard bottom
            [5, 20], [15, 22], [30, 25], [50, 30], [70, 38], [85, 45], [95, 50],
            [88, 56], [75, 64], [60, 72], [45, 78], [30, 82], [15, 82], [5, 80],
        ],
    ),
    "santoku": (
        "Santoku",
        "Wide blade, nearly flat edge, spine drops to sheepsfoot-like tip.",
        [
            # Guard top → flat spine → drops to tip → nearly flat edge → guard bottom
            [5, 22], [20, 22], [40, 22], [55, 24], [70, 32], [82, 45], [90, 58],
            [88, 65], [75, 72], [55, 75], [35, 76], [20, 76], [5, 75],
        ],
    ),
    "petty": (
        "Petty",
        "Small utility knife; narrow profile, slight belly curve, fine tip.",
        [
            # Guard top → slim spine → fine tip → slight belly → guard bottom
            [5, 38], [15, 36], [30, 35], [50, 35], [70, 38], [85, 44], [95, 50],
            [85, 56], [70, 60], [50, 63], [30, 65], [15, 64], [5, 62],
        ],
    ),
    "paring": (
        "Paring",
        "Short blade with slight curve; compact for hand-held detail work.",
        [
            # Guard top → spine → tip → gentle curve → guard bottom
            [5, 35], [15, 33], [30, 32], [50, 34], [70, 40], [85, 46], [95, 52],
            [85, 58], [70, 63], [50, 66], [30, 68], [15, 68], [5, 65],
        ],
    ),
    "cleaver": (
        "Cleaver",
        "Tall rectangular blade; straight edge, flat spine, squared-off tip.",
        [
            # Rectangular: guard top → spine → squared tip → straight edge → guard bottom
            [5, 18], [20, 18], [40, 18], [60, 18], [80, 18], [90, 20],
            [92, 30], [92, 50], [92, 70], [90, 80],
            [80, 82], [60, 82], [40, 82], [20, 82], [5, 82],
        ],
    ),
    "hatchet": (
        "Hatchet",
        "Short heavy wedge; wide cutting edge, narrow poll/eye end.",
        [
            # Narrow poll → widens → wide edge → back up
            [5, 35], [20, 30], [40, 25], [60, 22], [80, 20], [92, 25],
            [92, 75], [80, 80], [60, 78], [40, 75], [20, 70], [5, 65],
        ],
    ),
    "tanto": (
        "Tanto",
        "Flat spine; edge has angular transition (secondary bevel) near tip.",
        [
            # Guard top → flat spine → angled tip → angular edge → straight edge → guard bottom
            [5, 28], [20, 27], [40, 27], [60, 27], [75, 28], [88, 35], [95, 45],
            [88, 52], [78, 60], [65, 70], [50, 70], [30, 70], [15, 70], [5, 70],
        ],
    ),
}


def _try_cv_np():
    try:
        import cv2
        import numpy as np

        return cv2, np
    except ImportError:
        return None, None


def hu_log_vector(contour: Any) -> list[float]:
    cv2, np = _try_cv_np()
    if cv2 is None or contour is None or len(contour) < 3:
        return [0.0] * 7
    m = cv2.moments(contour)
    hu = cv2.HuMoments(m).flatten()
    out: list[float] = []
    for h in hu:
        out.append(float(-(1 if h >= 0 else -1) * math.log10(abs(h) + 1e-12)))
    return out


def is_hu_vector_degenerate(hu_list: list[float], max_saturated: int = 4) -> bool:
    """
    Hu values saturate at ±12 when raw moment ≈ 0 (log10(1e-12) = -12).
    Degenerate contours (rectangles, image boundary) produce many ±12 values and
    cannot discriminate between different blade shapes.
    """
    if not hu_list or len(hu_list) != 7:
        return True
    saturated = sum(1 for v in hu_list if abs(v) >= 11.0)
    return saturated >= max_saturated


def polygon_to_hu_log(poly: list[list[int]], size: int = 256) -> list[float]:
    cv2, np = _try_cv_np()
    if cv2 is None:
        return [0.0] * 7
    mask = np.zeros((size, size), dtype=np.uint8)
    pts = (np.array(poly, dtype=np.float32) * (size / 100.0)).astype(np.int32)
    cv2.fillPoly(mask, [pts], 255)
    cnts, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not cnts:
        return [0.0] * 7
    cnt = max(cnts, key=cv2.contourArea)
    return hu_log_vector(cnt)


def seed_blade_shape_rows() -> list[tuple[str, str, str, str, str]]:
    """Rows for INSERT: slug, name, description, hu_json, outline_json."""
    rows: list[tuple[str, str, str, str, str]] = []
    for slug, (name, desc, poly) in SEED_POLYGONS.items():
        hu = polygon_to_hu_log(poly)
        rows.append(
            (
                slug,
                name,
                desc,
                json.dumps(hu),
                json.dumps(poly),
            )
        )
    return rows


def fetch_ollama_models() -> dict[str, Any]:
    with httpx.Client(timeout=15.0) as client:
        r = client.get(f"{OLLAMA_HOST}/api/tags")
        r.raise_for_status()
        return r.json()


def check_ollama_model(model: str) -> tuple[bool, Optional[str]]:
    """
    Returns (model_found, error_message). If model_found is True, error is None.
    If Ollama is unreachable or model not in /api/tags, returns (False, "human-readable detail").
    """
    try:
        data = fetch_ollama_models()
    except httpx.ConnectError as exc:
        return False, f"Ollama not reachable at {OLLAMA_HOST}. Is it running?"
    except httpx.TimeoutException:
        return False, f"Ollama timed out at {OLLAMA_HOST}"
    except httpx.HTTPStatusError as exc:
        body = ""
        try:
            body = (exc.response.text or "")[:200]
        except Exception:
            pass
        return False, f"Ollama error {exc.response.status_code}: {body or str(exc)}"
    except Exception as exc:
        return False, str(exc)

    models = data.get("models") or []
    model_names = [m.get("name") or m.get("model", "") for m in models if isinstance(m, dict)]
    model_clean = (model or "").strip()
    if not model_clean:
        return False, "No model specified"
    # Match exact or as prefix (e.g. "llava" matches "llava:latest")
    for n in model_names:
        if n == model_clean or n.startswith(model_clean + ":"):
            return True, None
    return False, (
        f"Model '{model_clean}' not found in Ollama. Available: {', '.join(model_names[:8])}{'...' if len(model_names) > 8 else ''}. "
        f"Run: ollama pull {model_clean.split(':')[0]}"
    )


def ollama_chat(
    model: str,
    system: str,
    user_text: str,
    images_b64: Optional[list[str]] = None,
    timeout: float = 180.0,
) -> str:
    user_msg: dict[str, Any] = {"role": "user", "content": user_text}
    if images_b64:
        user_msg["images"] = images_b64
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            user_msg,
        ],
        "stream": False,
    }
    with httpx.Client(timeout=timeout) as client:
        r = client.post(f"{OLLAMA_HOST}/api/chat", json=payload)
        r.raise_for_status()
        data = r.json()
    msg = data.get("message") or {}
    return (msg.get("content") or data.get("response") or "").strip()


def _extract_blade_subcontour(full_contour: Any) -> Any:
    """
    Isolate the blade from a full knife silhouette (overhead shot, blade+handle).
    Blade is the pointed ~45% of the contour; handle is the wider remainder.
    Returns blade-only subcontour (closed) for Hu computation, or original if extraction fails.
    """
    cv2, np = _try_cv_np()
    if cv2 is None or full_contour is None or len(full_contour) < 6:
        return full_contour

    pts = full_contour.reshape(-1, 2).astype(np.float64)
    n = len(pts)

    # Find tip: sharpest interior angle (blade point)
    angles: list[float] = []
    for i in range(n):
        p_prev = pts[(i - 1) % n]
        p_curr = pts[i]
        p_next = pts[(i + 1) % n]
        v1 = p_curr - p_prev
        v2 = p_next - p_curr
        n1 = np.linalg.norm(v1) + 1e-8
        n2 = np.linalg.norm(v2) + 1e-8
        cos_a = np.clip(np.dot(v1, v2) / (n1 * n2), -1, 1)
        angles.append(math.degrees(math.acos(cos_a)))
    tip_idx = int(np.argmin(angles))

    total_len = float(cv2.arcLength(full_contour, True))
    blade_fraction = 0.48
    max_run = blade_fraction * total_len / 2

    def walk_from_tip(step: int) -> list[int]:
        indices: list[int] = []
        run = 0.0
        i = tip_idx
        for _ in range(n):
            indices.append(i)
            next_i = (i + step) % n
            run += np.linalg.norm(pts[next_i] - pts[i])
            if run >= max_run:
                break
            i = next_i
        return indices

    forward = walk_from_tip(1)
    backward = walk_from_tip(-1)
    blade_indices = list(reversed(backward[1:])) + forward
    if len(blade_indices) < 5:
        return full_contour

    blade_pts = pts[blade_indices]
    blade_closed = np.vstack([blade_pts, blade_pts[0:1]])
    return blade_closed.astype(np.float32).reshape(-1, 1, 2)


def _contour_touches_boundary(cnt: Any, rows: int, cols: int, margin: int = 3) -> bool:
    """True if the contour's bounding box touches the image edge (likely background/frame)."""
    cv2, _ = _try_cv_np()
    if cv2 is None:
        return False
    x, y, w, h = cv2.boundingRect(cnt)
    return x <= margin or y <= margin or (x + w) >= (cols - margin) or (y + h) >= (rows - margin)


def _largest_blade_contour_from_bgr(img: Any) -> tuple[Any, Optional[str]]:  # img: OpenCV BGR ndarray
    """
    Try Otsu threshold (blade as blob) → largest plausible contour.
    Excludes contours touching the image boundary (usually background, not blade).
    Falls back to Canny edge contour if thresholding fails.
    Returns (contour, error_message).
    """
    cv2, np = _try_cv_np()
    if cv2 is None or img is None:
        return None, "OpenCV not installed (pip install opencv-python-headless numpy)"

    rows, cols = img.shape[:2]
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)
    _, bw = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    candidates: list[tuple[float, Any]] = []
    for inv in (False, True):
        im = cv2.bitwise_not(bw) if inv else bw
        im = cv2.morphologyEx(im, cv2.MORPH_CLOSE, np.ones((9, 9), np.uint8))
        im = cv2.morphologyEx(im, cv2.MORPH_OPEN, np.ones((3, 3), np.uint8))
        cnts, _ = cv2.findContours(im, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not cnts:
            continue
        for cnt in cnts:
            a = cv2.contourArea(cnt)
            if a > 400 and not _contour_touches_boundary(cnt, rows, cols):
                candidates.append((a, cnt))

    cnt: Any = None
    if candidates:
        cnt = max(candidates, key=lambda x: x[0])[1]
    else:
        edges = cv2.Canny(gray, 40, 120)
        edges = cv2.dilate(edges, np.ones((4, 4), np.uint8), iterations=2)
        cnts, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not cnts:
            return None, "Could not isolate a blade shape — plain background and side profile help"
        interior = [c for c in cnts if not _contour_touches_boundary(c, rows, cols) and cv2.contourArea(c) >= 200]
        cnt = max(interior, key=cv2.contourArea) if interior else max(cnts, key=cv2.contourArea)
        if cv2.contourArea(cnt) < 200:
            return None, "Blade region too small — crop tighter on the knife"

    return cnt, None


def extract_blade_hu_from_image_bytes(image_bytes: bytes) -> tuple[Optional[list[float]], Optional[str]]:
    """
    Decode ``image_bytes`` (JPEG/PNG) and return Hu log-moment vector for the dominant blade-like contour.
    Used when seeding reference images into the DB; same heuristics as silhouette comparison.
    Rejects degenerate Hu (e.g. from image boundary contour) that cannot discriminate blade shapes.
    """
    cv2, np = _try_cv_np()
    if cv2 is None:
        return None, "OpenCV not installed (pip install opencv-python-headless numpy)"

    arr = np.frombuffer(image_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return None, "Could not decode image (use JPEG or PNG)"

    cnt, err = _largest_blade_contour_from_bgr(img)
    if cnt is None:
        return None, err
    blade_cnt = _extract_blade_subcontour(cnt)
    hu_list = hu_log_vector(blade_cnt)
    if is_hu_vector_degenerate(hu_list):
        return None, (
            "Contour too simple (likely image frame) — crop tighter on the knife, "
            "use side profile on plain background"
        )
    return hu_list, None


def silhouette_hints_from_image(
    image_bytes: bytes,
    template_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], Optional[str]]:
    """
    Try Otsu threshold (blade as blob) → Hu moments vs filled template silhouettes.
    Falls back to Canny edge contour if thresholding fails.
    """
    cv2, np = _try_cv_np()
    if cv2 is None:
        return [], "OpenCV not installed (pip install opencv-python-headless numpy)"

    arr = np.frombuffer(image_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return [], "Could not decode image (use JPEG or PNG)"

    cnt, err = _largest_blade_contour_from_bgr(img)
    if cnt is None:
        return [], err or "Could not isolate a blade shape"

    blade_cnt = _extract_blade_subcontour(cnt)
    hu = np.array(hu_log_vector(blade_cnt), dtype=np.float64)
    ranked: list[dict[str, Any]] = []
    for row in template_rows:
        try:
            ref = np.array(json.loads(row["hu_json"]), dtype=np.float64)
        except (json.JSONDecodeError, TypeError, KeyError):
            continue
        if ref.shape != hu.shape:
            continue
        dist = float(np.linalg.norm(hu - ref))
        ranked.append(
            {
                "slug": row.get("slug"),
                "name": row.get("name"),
                "distance": round(dist, 4),
            }
        )
    ranked.sort(key=lambda x: x["distance"])
    return ranked[:12], None


def try_parse_json_response(text: str) -> Optional[dict[str, Any]]:
    """Best-effort parse when the model returns extra prose around JSON."""
    t = text.strip()
    if not t:
        return None
    if t.startswith("{") and t.endswith("}"):
        try:
            return json.loads(t)
        except json.JSONDecodeError:
            pass
    start = t.find("{")
    end = t.rfind("}")
    if 0 <= start < end:
        try:
            return json.loads(t[start : end + 1])
        except json.JSONDecodeError:
            return None
    return None


VISION_DESCRIBE_SYSTEM = """You are a knife identification assistant. Your job is to DESCRIBE the knife in the photo for catalog search—do NOT guess the model name.

Extract observable features. For models that share the same blade shape, HANDLE and DISTINGUISHING details decide the match. Output VALID JSON ONLY (no markdown):
{
  "keywords": "<space-separated: handle material, blade shape, colors, lanyard hole, ring guard, finish, collab, etc.>",
  "blade_shape": "<drop point, clip point, trailing, fillet, skinner, etc. or null if unclear>",
  "blade_length_inches": <number or null if no ruler visible—BLADE ONLY from guard to tip, not overall>,
  "handle_material": "<paracord, G-10, micarta, ironwood, burled carbon fiber, orange, black, etc. or null>",
  "blade_finish": "<satin, stonewashed, PVD, cerakote, distressed, etc. or null>",
  "distinctive": "<lanyard hole yes/no, ring guard yes/no, finger grooves, tactical styling, collab markings, etc. or null>"
}
CRITICAL for similar models: note lanyard hole (present or absent), handle type (paracord vs G-10 vs micarta), ring guard. Be specific: "paracord wrap" not "cord", "no lanyard hole" if absent. If you see a ruler, measure BLADE LENGTH ONLY."""


DISTINGUISHING_FEATURES_SYSTEM = """You extract distinguishing features from a knife photo for identification. When blade shapes match, HANDLE and DETAILS differentiate models.

Output a SHORT semicolon-separated list of what you SEE. Include:
- Lanyard hole: "lanyard hole" or "no lanyard hole"
- Handle material: paracord, G-10, micarta, ironwood, burled carbon fiber, etc.
- Ring guard: "ring guard" or "no ring guard"
- Finish: satin, stonewash, PVD, cerakote
- Collab markings or branding if visible
- Finger grooves, grip texture if notable

Do NOT include handle or blade color. Colors are recorded separately in the catalog—the reference photo may show a different variant.

Reply with ONLY the semicolon-separated list. Example: lanyard hole; paracord wrap; no ring guard
No preamble, no JSON, no markdown."""


def extract_distinguishing_features_from_image(model: str, image_b64: str) -> tuple[Optional[str], Optional[str]]:
    """
    Have the vision model extract distinguishing features for knife identification.
    Returns (semicolon_separated_string, error_message).
    If successful, error is None. If LLM fails, returns (None, "error detail").
    """
    try:
        raw = ollama_chat(model, DISTINGUISHING_FEATURES_SYSTEM, "List the distinguishing features you see.", images_b64=[image_b64])
    except Exception as exc:
        return None, str(exc)
    t = (raw or "").strip()
    if not t:
        return None, "Empty response"
    # Clean: strip markdown, take first line if multiline, replace newlines with semicolons
    if "```" in t:
        start = t.find("```") + 3
        end = t.find("```", start)
        if end > start:
            t = t[start:end]
    t = t.replace("\n", "; ").strip()
    t = "; ".join(s.strip() for s in t.split(";") if s.strip())
    return t if t else None, None


VISION_COMPARE_SYSTEM = """You are identifying a Montana Knife Company (MKC) knife by comparing the user's photo against reference images of candidate models.

Image 1 is the user's knife photo. Image 2 is a BLADE FORM REFERENCE SHEET — use these names when describing blade shapes in your reasoning. The remaining images are candidate reference photos.

YOUR PRIMARY TASK: Determine whether the user's knife (Image 1) could be the same model as each candidate. Compare VISUALLY — do the blades look like they could be the same knife?

Focus on (in priority order):
1. OVERALL VISUAL SIMILARITY — does the blade profile, curvature, and proportions match?
2. Handle shape and proportions
3. Blade-to-handle ratio and size proportions
4. Distinctive features (finger choil, jimping, lanyard hole, ring guard)

Use the reference sheet (Image 2) names when describing blade shapes in your reason text, but do NOT let shape classification override visual similarity. Photos taken at different angles can make the same blade look like different shape categories. If the blades look visually similar, rate STRONG even if you are unsure about the exact shape name.

For each candidate, rate: STRONG (looks like the same model), POSSIBLE (could be, not sure), or UNLIKELY (clearly different blade/handle profile).

Return VALID JSON ONLY (no markdown):
{"comparisons": [{"model": "<exact model name>", "match": "STRONG|POSSIBLE|UNLIKELY", "reason": "<one sentence>"}]}"""


def _load_reference_sheet_b64() -> Optional[str]:
    """Load the blade form reference sheet as base64 for the vision model."""
    sheet_path = Path(__file__).parent / "static" / "blade-outlines" / "reference_sheet.png"
    if not sheet_path.exists():
        return None
    return base64.b64encode(sheet_path.read_bytes()).decode("ascii")


def _remove_background(image_b64: str) -> str:
    """Remove background from a base64-encoded image using rembg.

    Returns a base64-encoded PNG with the background removed (white bg).
    Falls back to the original image if rembg fails.
    """
    try:
        from io import BytesIO

        from PIL import Image as PILImage
        from rembg import remove

        img_bytes = base64.b64decode(image_b64)
        input_img = PILImage.open(BytesIO(img_bytes)).convert("RGBA")
        output_img = remove(input_img)
        # Composite onto white background for the vision model
        white_bg = PILImage.new("RGBA", output_img.size, (255, 255, 255, 255))
        composite = PILImage.alpha_composite(white_bg, output_img).convert("RGB")
        buf = BytesIO()
        composite.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        return image_b64


def vision_compare_candidates(
    model: str,
    user_image_b64: str,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Compare user's knife photo against candidate reference images via vision LLM.

    Each candidate dict must have 'name' and 'reference_image_b64'.
    Optionally includes 'form' (blade form name) for the prompt.
    Returns list of {model, match, reason} dicts.
    """
    # Remove background from user's photo for cleaner shape comparison
    clean_user_image = _remove_background(user_image_b64)

    # Build image list: user photo, reference sheet, then candidate references
    ref_sheet = _load_reference_sheet_b64()
    images = [clean_user_image]
    if ref_sheet:
        images.append(ref_sheet)

    # Candidate image numbering starts after user photo + reference sheet
    img_offset = len(images) + 1  # next image number (1-indexed)
    candidate_list = []
    for i, c in enumerate(candidates[:5]):
        ref = c.get("reference_image_b64")
        if ref:
            images.append(ref)
            img_num = img_offset + i
            form_note = f" (cataloged blade form: {c['form']})" if c.get("form") else ""
            candidate_list.append(f"Image {img_num}: {c['name']}{form_note}")

    if not candidate_list:
        return []

    user_text = (
        "Image 1 is the user's knife photo."
        + (" Image 2 is the blade form reference sheet." if ref_sheet else "")
        + " Compare against these candidates:\n"
        + "\n".join(candidate_list)
    )

    raw = ollama_chat(model, VISION_COMPARE_SYSTEM, user_text, images_b64=images)
    parsed = try_parse_json_response(raw)
    if isinstance(parsed, dict) and "comparisons" in parsed:
        return parsed["comparisons"]
    return []


def vision_describe_knife(model: str, image_b64: str, user_description: str = "") -> dict[str, Any]:
    """Have the vision model describe the knife for keyword search. Returns parsed JSON or empty dict."""
    user = "Describe this knife for catalog search."
    if user_description.strip():
        user += f" User notes: {user_description.strip()}"
    raw = ollama_chat(model, VISION_DESCRIBE_SYSTEM, user, images_b64=[image_b64])
    parsed = try_parse_json_response(raw)
    return parsed if isinstance(parsed, dict) else {}


RERANK_SYSTEM = """You pick the best MKC catalog match from the candidates.

When models share the same blade (e.g. Speedgoat 2.0 vs Speedgoat Ultra), you must use HANDLE and DISTINGUISHING FEATURES to decide:
- Handle and blade COLOR: use the catalog's handle_color/blade_color—these are authoritative (reference photos may show a different variant)
- Lanyard hole: present or absent
- Handle material: paracord, G-10, micarta, ironwood, burled carbon fiber, etc.
- Ring guard: yes or no
- Finger grooves, grip texture, size relative to sibling models
- Finish: satin, stonewash, PVD, cerakote, distressed
- Collab markings (Meat Church, Traditions, etc.)

The "DISTINGUISHING" field lists structural features. For COLOR, trust the catalog fields only.
Return valid JSON only."""

# Color words to strip from LLM-extracted distinguishing features (image may show wrong variant).
_COLOR_WORDS = frozenset(
    "orange black olive green red blue tan brown gray grey white yellow camo coyote od multicam "
    "orange/black black/orange green/black two-tone".split()
)


def _strip_color_from_distinguishing(dist: str) -> str:
    """Remove color-only phrases from distinguishing features. Master-record colors override image-derived ones."""
    if not dist or not dist.strip():
        return ""
    kept = []
    for seg in (s.strip() for s in dist.split(";")):
        if not seg:
            continue
        words = set(w.lower() for w in seg.replace(",", " ").replace("/", " ").replace("&", " ").split())
        words.discard("and")
        if not words:
            continue
        if words.issubset(_COLOR_WORDS):
            continue  # segment is color-only, strip it
        kept.append(seg)
    return "; ".join(kept) if kept else ""


def build_rerank_prompt(candidates: list[dict[str, Any]], vision_description: str) -> str:
    """Build prompt for LLM to pick best match. Master-record colors override any in distinguishing features."""
    lines = []
    for i, c in enumerate(candidates[:6], 1):
        dist = _strip_color_from_distinguishing(c.get("identifier_distinguishing_features") or "")
        dist_line = f"\n   DISTINGUISHING: {dist}" if dist else ""
        handle_color = (c.get("default_handle_color") or "").strip()
        blade_color = (c.get("default_blade_color") or "").strip()
        color_part = ""
        if handle_color or blade_color:
            parts = []
            if handle_color:
                parts.append(f"handle_color={handle_color}")
            if blade_color:
                parts.append(f"blade_color={blade_color}")
            color_part = "; " + "; ".join(parts)
        lines.append(
            f"{i}. {c.get('name', '?')}: {c.get('category', '')}; catalog_line={c.get('catalog_line', '')}; "
            f"blade={c.get('blade_profile') or c.get('blade_shape', '')}; len={c.get('default_blade_length')}"
            f"{color_part}; "
            f"handle/notes: {c.get('collector_notes', '')} {c.get('evidence_summary', '')} {c.get('identifier_keywords', '')}"
            f"{dist_line}"
        )
    return f"""Given this observed description:
{vision_description}

And these catalog candidates (pick the best match). When blade shapes match, use handle material, lanyard hole, ring guard, finish, and DISTINGUISHING features to decide:
{chr(10).join(lines)}

Return VALID JSON ONLY:
{{"ranked_models": [{{"name": "<exact name>", "confidence": <0-1>, "rationale": "<one sentence>"}}], "caveats": ""}}
Use 1–5 entries, best first. Names must match exactly."""


def build_llm_system_prompt(catalog_lines: str, shape_catalog_text: str) -> str:
    return f"""You are a knife identification assistant for Montana Knife Company (MKC) products.

You must use ONLY the catalog below as the universe of model names. Do not invent models.
If the blade is not plausibly MKC, say so in "caveats".

Catalog (name; category; blade_profile; default blade length in; collab; keywords/notes):
Note: "len" values are BLADE LENGTH ONLY (cutting edge from guard/bolster to tip), not overall knife length.
{catalog_lines}

Blade shape reference profiles (silhouette families for language only, not proof):
{shape_catalog_text}

Respond with VALID JSON ONLY (no markdown fences), exactly this shape:
{{
  "ranked_models": [
    {{"name": "<exact catalog name>", "confidence": <0.0-1.0>, "rationale": "<one sentence>"}}
  ],
  "caveats": "<uncertainty, lighting, non-MKC possibility, or what would help next>",
  "shape_read": "<what you see in the blade outline if an image was provided, else null>"
}}
Use 1–5 entries in ranked_models, best first. Names must match the catalog exactly when possible."""
