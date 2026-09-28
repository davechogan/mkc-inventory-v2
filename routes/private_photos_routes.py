"""HTTP API for the private photo share.

Upload and view are checked separately. Image bytes are returned only through
these routes, with ``Cache-Control: private, no-store``, and only after the
caller is allowed to see that file.
"""

from __future__ import annotations

import logging
from typing import Callable

from fastapi import APIRouter, Body, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, Response

from auth import get_current_user
from private_photos import (
    MAX_FILES_PER_REQUEST,
    MAX_UPLOAD_BYTES,
    delete_own_photo,
    delete_photo,
    delete_photos,
    derivative_path,
    download_filename,
    get_photo,
    ingest_photo,
    list_photos,
    photo_access_for_email,
    zip_display_jpegs,
)

logger = logging.getLogger("mkc_app.private_photos")

_PRIVATE_HEADERS = {
    "Cache-Control": "private, no-store",
    "X-Content-Type-Options": "nosniff",
    "X-Robots-Tag": "noindex, nofollow",
}


def create_private_photos_router(*, get_conn: Callable) -> APIRouter:
    router = APIRouter(prefix="/api/private-photos", tags=["private-photos"])

    def _email(request: Request) -> str | None:
        user = get_current_user(request)
        if user is None or not user.email:
            return None
        return user.email.strip().lower()

    def _require_email(request: Request) -> str:
        email = _email(request)
        if not email:
            raise HTTPException(status_code=401, detail="Sign in required.")
        return email

    @router.get("/access")
    def photo_access(request: Request):
        """Flags for the current user. Unauthenticated callers get both false."""
        email = _email(request)
        access = photo_access_for_email(email)
        return {
            "authenticated": email is not None,
            "can_upload": access.can_upload,
            "can_view": access.can_view,
            "can_admin": access.can_admin,
        }

    @router.get("")
    def photos_list(request: Request, scope: str = "auto"):
        """``all`` requires view. ``mine`` requires upload and returns only that email."""
        email = _require_email(request)
        access = photo_access_for_email(email)
        if scope not in {"auto", "all", "mine"}:
            raise HTTPException(status_code=400, detail="Unknown scope.")
        want_all = scope == "all" or (scope == "auto" and access.can_view)
        if want_all:
            if not access.can_view:
                raise HTTPException(status_code=403, detail="You cannot view these photos.")
            with get_conn() as conn:
                photos = list_photos(conn)
            return {"photos": photos}
        if not access.can_upload:
            raise HTTPException(status_code=403, detail="You cannot view these photos.")
        with get_conn() as conn:
            photos = list_photos(conn, email=email)
        return {"photos": photos}

    @router.post("")
    async def photos_upload(
        request: Request,
        files: list[UploadFile] = File(...),
    ):
        """Accept one or more photos. HEIC from iPhone Photos is converted for viewing."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_upload:
            raise HTTPException(status_code=403, detail="You cannot upload photos.")
        if not files:
            raise HTTPException(status_code=400, detail="Choose at least one photo.")
        if len(files) > MAX_FILES_PER_REQUEST:
            raise HTTPException(
                status_code=400,
                detail=f"Send at most {MAX_FILES_PER_REQUEST} photos at a time.",
            )

        uploaded: list[dict] = []
        errors: list[dict] = []
        with get_conn() as conn:
            for upload in files:
                name = upload.filename or "photo"
                try:
                    data = await _read_limited(upload, MAX_UPLOAD_BYTES)
                    uploaded.append(
                        ingest_photo(conn, email=email, filename=name, data=data)
                    )
                except HTTPException as exc:
                    errors.append({"filename": name, "detail": str(exc.detail)})
                except ValueError as exc:
                    errors.append({"filename": name, "detail": str(exc)})
                except Exception:
                    logger.exception("Failed to store private photo from %s", email)
                    errors.append({"filename": name, "detail": "Could not save that photo."})
                finally:
                    await upload.close()
        if not uploaded and errors:
            raise HTTPException(status_code=400, detail=errors[0]["detail"])
        return {"uploaded": uploaded, "errors": errors}

    @router.post("/bulk-delete")
    def photos_bulk_delete(request: Request, payload: dict = Body(...)):
        """Remove every selected photo. Admin only."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_admin:
            raise HTTPException(status_code=403, detail="You cannot remove these photos.")
        try:
            with get_conn() as conn:
                deleted = delete_photos(conn, _bulk_ids(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"deleted": deleted}

    @router.post("/bulk-download")
    def photos_bulk_download(request: Request, payload: dict = Body(...)):
        """Zip the selected viewer JPEGs. Admin only."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_admin:
            raise HTTPException(status_code=403, detail="You cannot download these photos.")
        try:
            with get_conn() as conn:
                data = zip_display_jpegs(conn, _bulk_ids(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return Response(
            content=data,
            media_type="application/zip",
            headers={
                **_PRIVATE_HEADERS,
                "Content-Disposition": 'attachment; filename="private-photos.zip"',
            },
        )

    @router.get("/{photo_id}/thumb")
    def photo_thumb(photo_id: str, request: Request):
        """Thumbnail. Viewers may see any; uploaders may see only their own."""
        email = _require_email(request)
        with get_conn() as conn:
            row = get_photo(conn, photo_id)
        _require_see_thumb(email, row)
        return _jpeg_file(photo_id, "thumbs")

    @router.get("/{photo_id}/image")
    def photo_image(photo_id: str, request: Request):
        """Full viewer image. View permission only — upload does not include this."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_view:
            raise HTTPException(status_code=403, detail="You cannot view these photos.")
        with get_conn() as conn:
            row = get_photo(conn, photo_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Photo not found.")
        return _jpeg_file(photo_id, "display")

    @router.get("/{photo_id}/download")
    def photo_download(photo_id: str, request: Request):
        """Attachment download of the viewer JPEG. Admin only."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_admin:
            raise HTTPException(status_code=403, detail="You cannot download these photos.")
        with get_conn() as conn:
            row = get_photo(conn, photo_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Photo not found.")
        return _jpeg_file(photo_id, "display", download_name=download_filename(row))

    @router.delete("/{photo_id}")
    def photo_delete(photo_id: str, request: Request):
        """Admins may remove any photo. Uploaders may remove only their own."""
        email = _require_email(request)
        access = photo_access_for_email(email)
        if not access.can_admin and not access.can_upload:
            raise HTTPException(status_code=403, detail="You cannot remove these photos.")
        with get_conn() as conn:
            if access.can_admin:
                removed = delete_photo(conn, photo_id)
            else:
                removed = delete_own_photo(conn, photo_id, email)
        if not removed:
            raise HTTPException(status_code=404, detail="Photo not found.")
        return Response(status_code=204)

    return router


def _require_see_thumb(email: str, row) -> None:
    if row is None:
        raise HTTPException(status_code=404, detail="Photo not found.")
    access = photo_access_for_email(email)
    if access.can_view:
        return
    if access.can_upload and row["uploaded_by_email"] == email:
        return
    raise HTTPException(status_code=403, detail="You cannot view these photos.")


def _bulk_ids(payload: dict) -> list[str]:
    ids = payload.get("ids") if isinstance(payload, dict) else None
    if not isinstance(ids, list):
        raise HTTPException(status_code=400, detail="Choose at least one photo.")
    return ids


def _jpeg_file(photo_id: str, kind: str, download_name: str | None = None) -> FileResponse:
    path = derivative_path(photo_id, kind)
    if path is None:
        raise HTTPException(status_code=404, detail="Photo not found.")
    return FileResponse(
        path,
        media_type="image/jpeg",
        filename=download_name or f"{photo_id}.jpg",
        content_disposition_type="attachment" if download_name else "inline",
        headers=_PRIVATE_HEADERS,
    )


async def _read_limited(upload: UploadFile, limit: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await upload.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise HTTPException(status_code=413, detail="That photo is too large (max 40 MB).")
        chunks.append(chunk)
    return b"".join(chunks)
