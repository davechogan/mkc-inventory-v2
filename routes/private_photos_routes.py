"""HTTP API for the private photo share.

Upload and view are checked separately. Image bytes are returned only through
these routes, with ``Cache-Control: private, no-store``, and only after the
caller is allowed to see that file.
"""

from __future__ import annotations

import logging
from typing import Callable

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response

from auth import LOCAL_USER_COOKIE, get_current_user, is_local_access, is_local_session
from private_chat import list_chat, mark_chat_read, post_chat_message
from private_photos import (
    MAX_FILES_PER_REQUEST,
    MAX_UPLOAD_BYTES,
    allowlisted_photo_email,
    delete_own_photo,
    delete_photo,
    delete_photos,
    derivative_path,
    download_filename,
    get_photo,
    ingest_photo,
    list_deleted,
    list_photos,
    notify_chat_message,
    notify_uploads,
    photo_access_for_email,
    playback_file,
    restore_photos,
    set_photo_caption,
    zip_selected_files,
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

    def _require_chat(request: Request) -> str:
        """Chat is for anyone who can upload or view. Strangers are refused."""
        email = _require_email(request)
        access = photo_access_for_email(email)
        if not access.can_upload and not access.can_view:
            raise HTTPException(status_code=403, detail="You cannot use this chat.")
        return email

    @router.get("/access")
    def photo_access(request: Request):
        """Flags for the current user. Unauthenticated callers get both false."""
        email = _email(request)
        access = photo_access_for_email(email)
        return {
            "authenticated": email is not None,
            "email": email,
            "can_upload": access.can_upload,
            "can_view": access.can_view,
            "can_admin": access.can_admin,
            "local_login": email is None and is_local_access(request),
            "local_session": is_local_session(request),
        }

    @router.post("/local-login")
    def local_login(request: Request, payload: dict = Body(...)):
        """Sign in on the LAN by email. The address must be on a photo allowlist.

        The public site returns 404. Cloudflare identity is never replaced by this.
        """
        if not is_local_access(request):
            raise HTTPException(status_code=404, detail="Not found.")
        raw = payload.get("email") if isinstance(payload, dict) else None
        if not isinstance(raw, str):
            raise HTTPException(status_code=400, detail="Enter an email address.")
        email = allowlisted_photo_email(raw)
        if email is None:
            raise HTTPException(status_code=403, detail="That email does not have access.")
        access = photo_access_for_email(email)
        response = JSONResponse(
            {
                "authenticated": True,
                "email": email,
                "can_upload": access.can_upload,
                "can_view": access.can_view,
                "can_admin": access.can_admin,
                "local_login": False,
                "local_session": True,
            }
        )
        response.set_cookie(
            LOCAL_USER_COOKIE,
            email,
            httponly=True,
            samesite="lax",
            path="/",
            max_age=60 * 60 * 12,
        )
        return response

    @router.post("/local-logout")
    def local_logout(request: Request):
        """Clear the local email session. Unavailable on the public site."""
        if not is_local_access(request):
            raise HTTPException(status_code=404, detail="Not found.")
        response = Response(status_code=204)
        response.delete_cookie(LOCAL_USER_COOKIE, path="/")
        return response

    @router.get("")
    def photos_list(request: Request, scope: str = "auto"):
        """``all`` requires view. ``mine`` requires upload and returns only that email."""
        email = _require_email(request)
        access = photo_access_for_email(email)
        if scope not in {"auto", "all", "mine", "received", "deleted"}:
            raise HTTPException(status_code=400, detail="Unknown scope.")
        if scope == "deleted":
            if not access.can_admin:
                raise HTTPException(status_code=403, detail="You cannot view deleted files.")
            with get_conn() as conn:
                photos = list_deleted(conn)
            return {"photos": photos}
        if scope == "received":
            if not access.can_view:
                raise HTTPException(status_code=403, detail="You cannot view these files.")
            with get_conn() as conn:
                photos = list_photos(conn, exclude_email=email)
            return {"photos": photos}
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

    @router.get("/chat")
    def chat_list(request: Request):
        """The shared thread and how many messages from the other person are unread."""
        email = _require_chat(request)
        with get_conn() as conn:
            return list_chat(conn, email=email)

    @router.post("/chat")
    def chat_post(request: Request, payload: dict = Body(...)):
        """Save one text message, then tell the other person's phone if a key is set."""
        email = _require_chat(request)
        raw = payload.get("body") if isinstance(payload, dict) else None
        if not isinstance(raw, str):
            raise HTTPException(status_code=400, detail="Write a message first.")
        try:
            with get_conn() as conn:
                message = post_chat_message(conn, email=email, body=raw)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        notify_chat_message(email, message["body"])
        return message

    @router.post("/chat/read")
    def chat_read(request: Request):
        """The open thread has been seen. Clears the unread dot for this person."""
        email = _require_chat(request)
        with get_conn() as conn:
            unread = mark_chat_read(conn, email)
        return {"unread": unread}

    @router.post("")
    async def photos_upload(
        request: Request,
        files: list[UploadFile] = File(...),
        captions: list[str] | None = Form(default=None),
    ):
        """Accept one or more photos. HEIC from iPhone Photos is converted for viewing.

        ``captions`` lines up with ``files``. Missing entries mean no caption.
        """
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
        caption_lines = list(captions or [])
        if len(caption_lines) > len(files):
            raise HTTPException(status_code=400, detail="Each file can have one caption.")

        uploaded: list[dict] = []
        errors: list[dict] = []
        with get_conn() as conn:
            for index, upload in enumerate(files):
                name = upload.filename or "photo"
                caption = caption_lines[index] if index < len(caption_lines) else None
                try:
                    data = await _read_limited(upload, MAX_UPLOAD_BYTES)
                    uploaded.append(
                        ingest_photo(
                            conn, email=email, filename=name, data=data, caption=caption,
                        )
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
        notify_uploads(email, uploaded)
        return {"uploaded": uploaded, "errors": errors}

    @router.post("/bulk-delete")
    def photos_bulk_delete(request: Request, payload: dict = Body(...)):
        """Remove every selected photo. Admin only."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_admin:
            raise HTTPException(status_code=403, detail="You cannot remove these photos.")
        try:
            with get_conn() as conn:
                deleted = delete_photos(conn, _bulk_ids(payload), deleted_by=email)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"deleted": deleted}

    @router.post("/restore")
    def photos_restore(request: Request, payload: dict = Body(...)):
        """Put selected deleted files back in the galleries. Admin only. The window is 30 days."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_admin:
            raise HTTPException(status_code=403, detail="You cannot restore these files.")
        try:
            with get_conn() as conn:
                restored = restore_photos(conn, _bulk_ids(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"restored": restored}

    @router.patch("/{photo_id}/caption")
    def photo_caption(photo_id: str, request: Request, payload: dict = Body(...)):
        """Set or clear the caption. The uploader and an admin may change it."""
        email = _require_email(request)
        access = photo_access_for_email(email)
        raw = payload.get("caption") if isinstance(payload, dict) else None
        if not isinstance(raw, str):
            raise HTTPException(status_code=400, detail="Caption must be text.")
        try:
            with get_conn() as conn:
                updated = set_photo_caption(
                    conn, photo_id, raw, email=email, is_admin=access.can_admin,
                )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        if updated is None:
            raise HTTPException(status_code=404, detail="Photo not found.")
        return updated

    @router.post("/bulk-download")
    def photos_bulk_download(request: Request, payload: dict = Body(...)):
        """Zip the selected viewer JPEGs. Admin only."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_admin:
            raise HTTPException(status_code=403, detail="You cannot download these photos.")
        try:
            with get_conn() as conn:
                data = zip_selected_files(conn, _bulk_ids(payload))
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
        _require_visible(email, row)
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
        _require_visible(email, row)
        return _jpeg_file(photo_id, "display")

    @router.get("/{photo_id}/download")
    def photo_download(photo_id: str, request: Request):
        """Attachment download of the viewer JPEG. Admin only."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_admin:
            raise HTTPException(status_code=403, detail="You cannot download these photos.")
        with get_conn() as conn:
            row = get_photo(conn, photo_id)
        _require_visible(email, row)
        return _playback_response(row, download_name=download_filename(row))

    @router.get("/{photo_id}/media")
    def photo_media(photo_id: str, request: Request):
        """Video or voice file for playback. View permission, same as the gallery."""
        email = _require_email(request)
        if not photo_access_for_email(email).can_view:
            raise HTTPException(status_code=403, detail="You cannot view these files.")
        with get_conn() as conn:
            row = get_photo(conn, photo_id)
        _require_visible(email, row)
        if (row["media_kind"] or "photo") == "photo":
            raise HTTPException(status_code=404, detail="File not found.")
        return _playback_response(row)

    @router.delete("/{photo_id}")
    def photo_delete(photo_id: str, request: Request):
        """Admins may remove any photo. Uploaders may remove only their own."""
        email = _require_email(request)
        access = photo_access_for_email(email)
        if not access.can_admin and not access.can_upload:
            raise HTTPException(status_code=403, detail="You cannot remove these photos.")
        with get_conn() as conn:
            if access.can_admin:
                removed = delete_photo(conn, photo_id, deleted_by=email)
            else:
                removed = delete_own_photo(conn, photo_id, email)
        if not removed:
            raise HTTPException(status_code=404, detail="Photo not found.")
        return Response(status_code=204)

    return router


def _require_visible(email: str, row) -> None:
    """Deleted files stay available to an admin for the retention window only."""
    if row is None or (row["deleted_at"] and not photo_access_for_email(email).can_admin):
        raise HTTPException(status_code=404, detail="Photo not found.")


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


def _playback_response(row, download_name: str | None = None) -> FileResponse:
    played = playback_file(row)
    if played is None:
        raise HTTPException(status_code=404, detail="File not found.")
    path, content_type = played
    return FileResponse(
        path,
        media_type=content_type,
        filename=download_name or path.name,
        content_disposition_type="attachment" if download_name else "inline",
        headers=_PRIVATE_HEADERS,
    )


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
