"""Private photo share: allowlists, disk storage, and JPEG derivatives.

Pictures are not part of the public catalog. Bytes live under a private
directory (default ``data/private_photos/``) that is never mounted as static
files. Upload and view are separate email allowlists:

- ``PRIVATE_PHOTOS_UPLOAD_EMAILS`` — comma-separated, may upload and delete own
- ``PRIVATE_PHOTOS_VIEW_EMAILS`` — comma-separated, may list and open the viewer
- ``PRIVATE_PHOTOS_ADMIN_EMAILS`` — comma-separated, may view, download, and delete any photo

The HTTP layer serves only re-encoded JPEGs. Originals stay on disk so an
iPhone HEIC can be kept without being sent to the browser. Display and
thumbnail JPEGs are written without EXIF, so location metadata is not served.
"""

from __future__ import annotations

import io
import logging
import os
import re
import sqlite3
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from PIL import Image, ImageOps, UnidentifiedImageError

logger = logging.getLogger("mkc_app.private_photos")

try:
    import pillow_heif

    pillow_heif.register_heif_opener()
    HEIF_AVAILABLE = True
except ImportError:  # pragma: no cover - production installs the extra
    HEIF_AVAILABLE = False

MAX_PHOTO_BYTES = 40 * 1024 * 1024
MAX_AUDIO_BYTES = 40 * 1024 * 1024
MAX_VIDEO_BYTES = 250 * 1024 * 1024
# The request reader stops at the largest accepted file.
MAX_UPLOAD_BYTES = MAX_VIDEO_BYTES
MAX_FILES_PER_REQUEST = 8
MAX_BULK_PHOTOS = 100
_VIDEO_TYPES = {
    ".mp4": "video/mp4",
    ".m4v": "video/mp4",
    ".mov": "video/quicktime",
    ".webm": "video/webm",
}
_AUDIO_TYPES = {
    ".m4a": "audio/mp4",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".aac": "audio/aac",
    ".caf": "audio/x-caf",
}
_MEDIA_LIMITS = {
    "photo": MAX_PHOTO_BYTES,
    "audio": MAX_AUDIO_BYTES,
    "video": MAX_VIDEO_BYTES,
}
_MEDIA_LIMIT_LABELS = {"photo": "40 MB", "audio": "40 MB", "video": "250 MB"}
DISPLAY_MAX_EDGE = 2560
THUMB_MAX_EDGE = 720
_ID_RE = re.compile(r"^[a-f0-9]{32}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_EXIF_TAKEN_RE = re.compile(
    r"(\d{4}):(\d{2}):(\d{2})[ T](\d{2}):(\d{2}):(\d{2})"
)
# Pillow format -> on-disk suffix for the original bytes.
_FORMAT_SUFFIX = {
    "JPEG": ".jpg",
    "MPO": ".jpg",
    "PNG": ".png",
    "WEBP": ".webp",
    "GIF": ".gif",
    "HEIF": ".heic",
    "HEIC": ".heic",
}


@dataclass(frozen=True)
class PhotoAccess:
    """What the signed-in email is allowed to do. Flags may be combined.

    Admin includes view, plus download and delete of any photo. It does not
    include upload.
    """

    can_upload: bool
    can_view: bool
    can_admin: bool


def _email_set(env_name: str) -> set[str]:
    raw = os.environ.get(env_name) or ""
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


def photo_allowlist() -> set[str]:
    """Emails that may use the photo page. This is the local login store.

    The three env allowlists are the only source of permissions. A local
    sign-in is accepted only when the address is in this set.
    """
    return (
        _email_set("PRIVATE_PHOTOS_UPLOAD_EMAILS")
        | _email_set("PRIVATE_PHOTOS_VIEW_EMAILS")
        | _email_set("PRIVATE_PHOTOS_ADMIN_EMAILS")
    )


def allowlisted_photo_email(email: Optional[str]) -> Optional[str]:
    """Return the normalized email when it is on the photo allowlist."""
    if not email:
        return None
    normalized = email.strip().lower()
    if not _EMAIL_RE.fullmatch(normalized):
        return None
    if normalized not in photo_allowlist():
        return None
    return normalized


def photo_access_for_email(email: Optional[str]) -> PhotoAccess:
    """Resolve upload, view, and admin from the env allowlists.

    Upload includes view. Both people can open the two galleries: files they
    sent, and files the other person sent. Admin adds download and delete of
    any file, and does not include upload.
    """
    if not email:
        return PhotoAccess(can_upload=False, can_view=False, can_admin=False)
    normalized = email.strip().lower()
    can_admin = normalized in _email_set("PRIVATE_PHOTOS_ADMIN_EMAILS")
    can_upload = normalized in _email_set("PRIVATE_PHOTOS_UPLOAD_EMAILS")
    return PhotoAccess(
        can_upload=can_upload,
        can_view=can_admin or can_upload or normalized in _email_set("PRIVATE_PHOTOS_VIEW_EMAILS"),
        can_admin=can_admin,
    )


def storage_root() -> Path:
    """Private directory for originals, display JPEGs, and thumbnails."""
    override = (os.environ.get("PRIVATE_PHOTOS_DIR") or "").strip()
    if override:
        root = Path(override).expanduser().resolve()
    else:
        root = Path(__file__).resolve().parent / "data" / "private_photos"
    root.mkdir(parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    for name in ("originals", "display", "thumbs"):
        sub = root / name
        sub.mkdir(parents=True, exist_ok=True)
        os.chmod(sub, 0o700)
    return root


def is_photo_id(photo_id: str) -> bool:
    return bool(_ID_RE.fullmatch(photo_id or ""))


def _taken_at(image: Image.Image) -> Optional[str]:
    exif = image.getexif()
    if not exif:
        return None
    raw = exif.get(36867) or exif.get(306)  # DateTimeOriginal, DateTime
    if not isinstance(raw, str):
        return None
    match = _EXIF_TAKEN_RE.search(raw.strip())
    if not match:
        return None
    year, month, day, hour, minute, second = match.groups()
    try:
        parsed = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%dT%H:%M:%S")


def _display_name(filename: Optional[str]) -> str:
    name = Path(filename or "photo").name.replace("\x00", "").strip()
    return (name or "photo")[:180]


def _write_private(path: Path, data: bytes) -> None:
    """Create a file that is readable only by the server user."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)


def _jpeg_bytes(image: Image.Image, max_edge: int, quality: int) -> tuple[bytes, int, int]:
    frame = image.copy()
    frame.thumbnail((max_edge, max_edge), Image.Resampling.LANCZOS)
    if frame.mode != "RGB":
        frame = frame.convert("RGB")
    buf = io.BytesIO()
    frame.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue(), frame.width, frame.height


def _file_suffix(filename: Optional[str]) -> str:
    suffix = Path(filename or "").suffix.lower()
    if not re.fullmatch(r"\.[a-z0-9]{1,8}", suffix):
        return ""
    return suffix


def _is_iso_bmff(data: bytes) -> bool:
    return len(data) >= 12 and data[4:8] == b"ftyp"


def _audio_matches(suffix: str, data: bytes) -> bool:
    if suffix == ".m4a":
        return _is_iso_bmff(data)
    if suffix == ".mp3":
        return data.startswith(b"ID3") or (len(data) >= 2 and data[0] == 0xFF and (data[1] & 0xE0) == 0xE0)
    if suffix == ".wav":
        return len(data) >= 12 and data.startswith(b"RIFF") and data[8:12] == b"WAVE"
    if suffix == ".aac":
        return len(data) >= 2 and data[0] == 0xFF and data[1] in {0xF1, 0xF9}
    if suffix == ".caf":
        return data.startswith(b"caff")
    return False


def _classify(filename: Optional[str], data: bytes) -> tuple[str, str]:
    """Return ``(photo|video|audio, suffix)``. Reject a declared video or audio that is not one."""
    suffix = _file_suffix(filename)
    if suffix in _VIDEO_TYPES:
        matches = _is_iso_bmff(data) if suffix != ".webm" else data.startswith(b"\x1a\x45\xdf\xa3")
        if not matches:
            raise ValueError("That file is not a supported video.")
        return "video", suffix
    if suffix in _AUDIO_TYPES:
        if not _audio_matches(suffix, data):
            raise ValueError("That file is not a supported voice recording.")
        return "audio", suffix
    return "photo", suffix


def _open_image(data: bytes, filename: Optional[str]) -> Image.Image:
    suffix = Path(filename or "").suffix.lower()
    if suffix in {".heic", ".heif"} and not HEIF_AVAILABLE:
        raise ValueError("This photo is HEIC, and HEIC support is not installed on the server.")
    try:
        image = Image.open(io.BytesIO(data))
        image.load()
    except Image.DecompressionBombError as exc:
        raise ValueError("That photo is too large to process.") from exc
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        raise ValueError("That file is not a supported photo.") from exc
    return image


def _insert_row(
    conn: sqlite3.Connection,
    *,
    photo_id: str,
    filename: Optional[str],
    suffix: str,
    data: bytes,
    width: Optional[int],
    height: Optional[int],
    taken: Optional[str],
    email: str,
    media_kind: str,
) -> None:
    conn.execute(
        """
        INSERT INTO private_photos (
            id, original_name, original_suffix, byte_size, width, height,
            taken_at, uploaded_by_email, media_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            photo_id,
            _display_name(filename),
            suffix,
            len(data),
            width,
            height,
            taken,
            email.strip().lower(),
            media_kind,
        ),
    )


def ingest_photo(
    conn: sqlite3.Connection,
    *,
    email: str,
    filename: Optional[str],
    data: bytes,
) -> dict:
    """Store one upload and return its public metadata. Raises ValueError on bad input."""
    if not data:
        raise ValueError("That file is empty.")
    kind, suffix = _classify(filename, data)
    if len(data) > _MEDIA_LIMITS[kind]:
        raise ValueError(f"That file is too large (max {_MEDIA_LIMIT_LABELS[kind]}).")

    photo_id = uuid.uuid4().hex
    root = storage_root()
    written: list[Path] = []
    try:
        if kind == "photo":
            image = _open_image(data, filename)
            taken = _taken_at(image)
            oriented = ImageOps.exif_transpose(image) or image
            suffix = _FORMAT_SUFFIX.get((image.format or "").upper(), ".bin")
            original_path = root / "originals" / f"{photo_id}{suffix}"
            _write_private(original_path, data)
            written.append(original_path)
            display_bytes, width, height = _jpeg_bytes(oriented, DISPLAY_MAX_EDGE, 86)
            thumb_bytes, _, _ = _jpeg_bytes(oriented, THUMB_MAX_EDGE, 80)
            display_path = root / "display" / f"{photo_id}.jpg"
            thumb_path = root / "thumbs" / f"{photo_id}.jpg"
            _write_private(display_path, display_bytes)
            written.append(display_path)
            _write_private(thumb_path, thumb_bytes)
            written.append(thumb_path)
            _insert_row(
                conn, photo_id=photo_id, filename=filename, suffix=suffix, data=data,
                width=width, height=height, taken=taken, email=email, media_kind="photo",
            )
        else:
            original_path = root / "originals" / f"{photo_id}{suffix}"
            _write_private(original_path, data)
            written.append(original_path)
            _insert_row(
                conn, photo_id=photo_id, filename=filename, suffix=suffix, data=data,
                width=None, height=None, taken=None, email=email, media_kind=kind,
            )
    except Exception:
        for path in written:
            path.unlink(missing_ok=True)
        raise
    logger.info("Stored private %s %s for %s (%d bytes)", kind, photo_id, email, len(data))
    row = conn.execute("SELECT * FROM private_photos WHERE id = ?", (photo_id,)).fetchone()
    return _public_row(row)


def _public_row(row: sqlite3.Row | dict) -> dict:
    created = row["created_at"] or ""
    if isinstance(created, str):
        created = created.replace(" ", "T")
    return {
        "id": row["id"],
        "width": row["width"],
        "height": row["height"],
        "taken_at": row["taken_at"],
        "created_at": created,
        "uploaded_by_email": row["uploaded_by_email"],
        "media_kind": row["media_kind"] or "photo",
    }


def list_photos(
    conn: sqlite3.Connection,
    *,
    email: Optional[str] = None,
    exclude_email: Optional[str] = None,
) -> list[dict]:
    """Newest first.

    ``email`` is the sender's own gallery. ``exclude_email`` is the other
    person's gallery. Omit both for every file.
    """
    if email and exclude_email:
        raise ValueError("Choose one gallery.")
    if email:
        rows = conn.execute(
            """
            SELECT * FROM private_photos
            WHERE uploaded_by_email = ?
            ORDER BY COALESCE(taken_at, created_at) DESC, created_at DESC
            """,
            (email.strip().lower(),),
        ).fetchall()
    elif exclude_email:
        rows = conn.execute(
            """
            SELECT * FROM private_photos
            WHERE uploaded_by_email != ?
            ORDER BY COALESCE(taken_at, created_at) DESC, created_at DESC
            """,
            (exclude_email.strip().lower(),),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT * FROM private_photos
            ORDER BY COALESCE(taken_at, created_at) DESC, created_at DESC
            """
        ).fetchall()
    return [_public_row(row) for row in rows]


def get_photo(conn: sqlite3.Connection, photo_id: str) -> Optional[dict]:
    if not is_photo_id(photo_id):
        return None
    return conn.execute(
        "SELECT * FROM private_photos WHERE id = ?",
        (photo_id,),
    ).fetchone()


def derivative_path(photo_id: str, kind: str) -> Optional[Path]:
    """Resolve a display or thumb file, refusing anything outside the private root."""
    if kind not in {"display", "thumbs"} or not is_photo_id(photo_id):
        return None
    root = storage_root()
    path = (root / kind / f"{photo_id}.jpg").resolve()
    if not path.is_relative_to(root) or not path.is_file():
        return None
    return path


def _unlink_stored(row: dict) -> None:
    photo_id = row["id"]
    root = storage_root()
    suffix = row["original_suffix"] or ""
    if not re.fullmatch(r"\.[a-z0-9]{1,8}", suffix):
        suffix = ""
    candidates = [
        root / "originals" / f"{photo_id}{suffix}",
        root / "display" / f"{photo_id}.jpg",
        root / "thumbs" / f"{photo_id}.jpg",
    ]
    for path in candidates:
        resolved = path.resolve()
        if resolved.is_relative_to(root):
            resolved.unlink(missing_ok=True)


def media_kind_of(row: dict) -> str:
    return row["media_kind"] or "photo"


def original_file_path(row: dict) -> Optional[Path]:
    """The stored original, only when its suffix is a single safe extension."""
    suffix = row["original_suffix"] or ""
    if not re.fullmatch(r"\.[a-z0-9]{1,8}", suffix) or not is_photo_id(row["id"]):
        return None
    root = storage_root()
    path = (root / "originals" / f"{row['id']}{suffix}").resolve()
    if not path.is_relative_to(root) or not path.is_file():
        return None
    return path


def playback_file(row: dict) -> Optional[tuple[Path, str]]:
    """File and content type for viewing or downloading.

    Photos use the GPS-stripped JPEG. Video and voice use the original bytes
    with a content type chosen from the suffix, not from the uploader.
    """
    if media_kind_of(row) == "photo":
        path = derivative_path(row["id"], "display")
        return (path, "image/jpeg") if path is not None else None
    path = original_file_path(row)
    content_type = {**_VIDEO_TYPES, **_AUDIO_TYPES}.get(row["original_suffix"] or "")
    if path is None or content_type is None:
        return None
    return path, content_type


def download_filename(row: dict) -> str:
    """Stable name for a single download or a file inside a zip."""
    taken = str(row["taken_at"] or row["created_at"] or "")[:10]
    stamp = taken if len(taken) == 10 else "file"
    suffix = ".jpg" if media_kind_of(row) == "photo" else (row["original_suffix"] or "")
    return f"{stamp}-{row['id'][:8]}{suffix}"


def _unique_ids(ids: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for photo_id in ids:
        if not isinstance(photo_id, str) or photo_id in seen or not is_photo_id(photo_id):
            continue
        seen.add(photo_id)
        unique.append(photo_id)
    if not unique:
        raise ValueError("Choose at least one photo.")
    if len(unique) > MAX_BULK_PHOTOS:
        raise ValueError(f"Choose at most {MAX_BULK_PHOTOS} photos at a time.")
    return unique


def delete_photos(conn: sqlite3.Connection, ids: list[str]) -> int:
    """Delete every existing photo in ``ids``. Returns how many were removed."""
    removed = 0
    for photo_id in _unique_ids(ids):
        if delete_photo(conn, photo_id):
            removed += 1
    return removed


def zip_selected_files(conn: sqlite3.Connection, ids: list[str]) -> bytes:
    """Zip the files a viewer would open. Missing ids are skipped."""
    buf = io.BytesIO()
    wrote = 0
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as archive:
        for photo_id in _unique_ids(ids):
            row = get_photo(conn, photo_id)
            played = playback_file(row) if row is not None else None
            if row is None or played is None:
                continue
            path, _content_type = played
            archive.write(path, arcname=download_filename(row))
            wrote += 1
    if wrote == 0:
        raise ValueError("Those files could not be found.")
    return buf.getvalue()


def delete_photo(conn: sqlite3.Connection, photo_id: str) -> bool:
    """Delete any photo by id. Returns False when it is missing."""
    row = get_photo(conn, photo_id)
    if row is None:
        return False
    conn.execute("DELETE FROM private_photos WHERE id = ?", (photo_id,))
    _unlink_stored(row)
    return True


def delete_own_photo(conn: sqlite3.Connection, photo_id: str, email: str) -> bool:
    """Delete a photo the email uploaded. Returns False when it is missing or not theirs."""
    row = get_photo(conn, photo_id)
    if row is None or row["uploaded_by_email"] != email.strip().lower():
        return False
    return delete_photo(conn, photo_id)
