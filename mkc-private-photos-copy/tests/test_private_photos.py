"""Private photo share: upload and view are different permissions."""

from __future__ import annotations

import io
import urllib.parse
import zipfile

import pytest
from fastapi.testclient import TestClient
from PIL import Image

UPLOADER = "she@example.com"
OTHER_UPLOADER = "sister@example.com"
VIEWER = "he@example.com"
ADMIN = "admin@example.com"
STRANGER = "stranger@example.com"


def _as(email: str) -> dict[str, str]:
    return {"Cf-Access-Authenticated-User-Email": email}


def _jpeg(color: tuple[int, int, int] = (20, 40, 80), size: tuple[int, int] = (24, 12)) -> bytes:
    image = Image.new("RGB", size, color)
    buf = io.BytesIO()
    exif = image.getexif()
    exif[270] = "SECRET_GPS_MARKER_XYZ"
    image.save(buf, format="JPEG", exif=exif)
    return buf.getvalue()


def _ftyp(brand: bytes = b"isom") -> bytes:
    payload = brand + b"\x00\x00\x00\x00" + brand
    return (8 + len(payload)).to_bytes(4, "big") + b"ftyp" + payload


def _wav() -> bytes:
    body = b"WAVE" + b"fmt " + (16).to_bytes(4, "little") + b"\x00" * 16
    return b"RIFF" + len(body).to_bytes(4, "little") + body


def _png() -> bytes:
    image = Image.new("RGB", (10, 18), (200, 10, 10))
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


@pytest.fixture
def photos(monkeypatch, tmp_path, invapp):
    monkeypatch.setenv(
        "PRIVATE_PHOTOS_UPLOAD_EMAILS",
        f" {UPLOADER.upper()} , {OTHER_UPLOADER} ",
    )
    monkeypatch.setenv("PRIVATE_PHOTOS_VIEW_EMAILS", VIEWER)
    monkeypatch.setenv("PRIVATE_PHOTOS_DIR", str(tmp_path))
    with TestClient(invapp.app) as client:
        yield client
    with invapp.get_conn() as conn:
        conn.execute("DELETE FROM private_photos")
        conn.execute("DELETE FROM private_chat_messages")
        conn.execute("DELETE FROM private_chat_reads")


def _upload(client: TestClient, email: str, filename: str, data: bytes, content_type: str):
    return client.post(
        "/api/private-photos",
        files={"files": (filename, data, content_type)},
        headers=_as(email),
    )


def test_access_flags_follow_each_allowlist(photos: TestClient):
    uploader = photos.get("/api/private-photos/access", headers=_as(UPLOADER)).json()
    viewer = photos.get("/api/private-photos/access", headers=_as(VIEWER)).json()
    stranger = photos.get("/api/private-photos/access", headers=_as(STRANGER)).json()
    anonymous = photos.get("/api/private-photos/access").json()

    assert uploader == {
        "authenticated": True, "email": UPLOADER, "can_upload": True, "can_view": True, "can_admin": False,
        "local_login": False, "local_session": False,
    }
    assert viewer == {
        "authenticated": True, "email": VIEWER, "can_upload": False, "can_view": True, "can_admin": False,
        "local_login": False, "local_session": False,
    }
    assert stranger == {
        "authenticated": True, "email": STRANGER, "can_upload": False, "can_view": False, "can_admin": False,
        "local_login": False, "local_session": False,
    }
    assert anonymous == {
        "authenticated": False, "email": None, "can_upload": False, "can_view": False, "can_admin": False,
        "local_login": True, "local_session": False,
    }


def test_uploader_can_send_jpeg_and_png_but_not_open_the_viewer(photos: TestClient, tmp_path):
    jpeg = _upload(photos, UPLOADER, "IMG_0001.JPG", _jpeg(), "image/jpeg")
    assert jpeg.status_code == 200, jpeg.text
    photo_id = jpeg.json()["uploaded"][0]["id"]

    png = _upload(photos, UPLOADER, "shot.png", _png(), "image/png")
    assert png.status_code == 200, png.text

    rejected = _upload(photos, UPLOADER, "notes.txt", b"not a photo", "text/plain")
    assert rejected.status_code == 400

    listing = photos.get("/api/private-photos?scope=mine", headers=_as(UPLOADER))
    assert listing.status_code == 200
    ids = [row["id"] for row in listing.json()["photos"]]
    assert photo_id in ids
    assert png.json()["uploaded"][0]["id"] in ids

    own_image = photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(UPLOADER))
    assert own_image.status_code == 200
    received = photos.get("/api/private-photos?scope=received", headers=_as(UPLOADER)).json()["photos"]
    assert photo_id not in [row["id"] for row in received]

    thumb = photos.get(f"/api/private-photos/{photo_id}/thumb", headers=_as(UPLOADER))
    assert thumb.status_code == 200
    assert thumb.headers["content-type"].startswith("image/jpeg")
    assert thumb.content[:2] == b"\xff\xd8"

    original = tmp_path / "originals" / f"{photo_id}.jpg"
    display = tmp_path / "display" / f"{photo_id}.jpg"
    assert original.is_file()
    assert display.is_file()
    assert original.stat().st_mode & 0o777 == 0o600
    assert b"SECRET_GPS_MARKER_XYZ" in original.read_bytes()
    served = photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(VIEWER))
    assert served.status_code == 200
    assert served.headers["cache-control"] == "private, no-store"
    assert served.content[:2] == b"\xff\xd8"
    assert b"SECRET_GPS_MARKER_XYZ" not in served.content
    assert "static" not in str(display)


def test_viewers_can_open_photos_and_cannot_upload_or_delete(photos: TestClient):
    created = _upload(photos, UPLOADER, "day.jpg", _jpeg((1, 2, 3)), "image/jpeg")
    photo_id = created.json()["uploaded"][0]["id"]

    assert photos.post(
        "/api/private-photos",
        files={"files": ("x.jpg", _jpeg(), "image/jpeg")},
        headers=_as(VIEWER),
    ).status_code == 403

    listing = photos.get("/api/private-photos", headers=_as(VIEWER))
    assert listing.status_code == 200
    assert [row["id"] for row in listing.json()["photos"]] == [photo_id]

    image = photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(VIEWER))
    assert image.status_code == 200
    assert image.headers["content-type"].startswith("image/jpeg")

    assert photos.delete(f"/api/private-photos/{photo_id}", headers=_as(VIEWER)).status_code == 403
    still_there = photos.get("/api/private-photos", headers=_as(VIEWER))
    assert still_there.json()["photos"][0]["id"] == photo_id


def test_each_uploader_sees_only_their_own_and_can_remove_them(photos: TestClient):
    first = _upload(photos, UPLOADER, "a.jpg", _jpeg((4, 5, 6)), "image/jpeg")
    second = _upload(photos, OTHER_UPLOADER, "b.jpg", _jpeg((7, 8, 9)), "image/jpeg")
    first_id = first.json()["uploaded"][0]["id"]
    second_id = second.json()["uploaded"][0]["id"]

    mine = photos.get("/api/private-photos?scope=mine", headers=_as(UPLOADER)).json()["photos"]
    assert [row["id"] for row in mine] == [first_id]
    received = photos.get("/api/private-photos?scope=received", headers=_as(UPLOADER)).json()["photos"]
    assert [row["id"] for row in received] == [second_id]
    theirs = photos.get("/api/private-photos?scope=received", headers=_as(OTHER_UPLOADER)).json()["photos"]
    assert [row["id"] for row in theirs] == [first_id]

    other_thumb = photos.get(f"/api/private-photos/{second_id}/thumb", headers=_as(UPLOADER))
    assert other_thumb.status_code == 200
    other_image = photos.get(f"/api/private-photos/{second_id}/image", headers=_as(UPLOADER))
    assert other_image.status_code == 200

    assert photos.delete(f"/api/private-photos/{second_id}", headers=_as(UPLOADER)).status_code == 404
    assert photos.delete(f"/api/private-photos/{first_id}", headers=_as(UPLOADER)).status_code == 204

    viewer_list = photos.get("/api/private-photos?scope=all", headers=_as(VIEWER)).json()["photos"]
    assert [row["id"] for row in viewer_list] == [second_id]


def test_stranger_and_anonymous_cannot_read_or_write(photos: TestClient):
    created = _upload(photos, UPLOADER, "a.jpg", _jpeg(), "image/jpeg")
    photo_id = created.json()["uploaded"][0]["id"]

    for method, url in (
        ("get", "/api/private-photos"),
        ("get", f"/api/private-photos/{photo_id}/image"),
        ("get", f"/api/private-photos/{photo_id}/thumb"),
    ):
        assert getattr(photos, method)(url, headers=_as(STRANGER)).status_code == 403
        assert getattr(photos, method)(url).status_code == 401

    assert photos.post(
        "/api/private-photos",
        files={"files": ("x.jpg", _jpeg(), "image/jpeg")},
        headers=_as(STRANGER),
    ).status_code == 403
    assert photos.post(
        "/api/private-photos",
        files={"files": ("x.jpg", _jpeg(), "image/jpeg")},
    ).status_code == 401
    assert photos.get("/api/private-photos/not-a-real-id/image", headers=_as(VIEWER)).status_code == 404


def test_heic_from_iphone_is_stored_and_served_as_jpeg(photos: TestClient):
    pillow_heif = pytest.importorskip("pillow_heif")
    pillow_heif.register_heif_opener()
    image = Image.new("RGB", (16, 16), (30, 60, 90))
    buf = io.BytesIO()
    try:
        image.save(buf, format="HEIF")
    except Exception as exc:
        pytest.skip(f"HEIF encoder unavailable: {exc}")

    response = _upload(photos, UPLOADER, "IMG_2042.HEIC", buf.getvalue(), "image/heic")
    assert response.status_code == 200, response.text
    photo_id = response.json()["uploaded"][0]["id"]
    served = photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(VIEWER))
    assert served.status_code == 200
    assert served.content[:2] == b"\xff\xd8"


def test_admin_can_download_and_delete_any_photo_viewers_cannot(photos: TestClient, monkeypatch):
    monkeypatch.setenv("PRIVATE_PHOTOS_ADMIN_EMAILS", ADMIN)
    created = _upload(photos, UPLOADER, "shared.jpg", _jpeg(), "image/jpeg")
    photo_id = created.json()["uploaded"][0]["id"]

    flags = photos.get("/api/private-photos/access", headers=_as(ADMIN)).json()
    assert flags["can_admin"] is True
    assert flags["can_view"] is True
    assert flags["can_upload"] is False

    downloaded = photos.get(f"/api/private-photos/{photo_id}/download", headers=_as(ADMIN))
    assert downloaded.status_code == 200
    assert downloaded.content[:2] == b"\xff\xd8"
    disposition = downloaded.headers["content-disposition"]
    assert disposition.startswith("attachment;")
    assert photo_id[:8] in disposition

    assert photos.get(f"/api/private-photos/{photo_id}/download", headers=_as(VIEWER)).status_code == 403
    assert photos.get(f"/api/private-photos/{photo_id}/download", headers=_as(UPLOADER)).status_code == 403
    assert photos.delete(f"/api/private-photos/{photo_id}", headers=_as(VIEWER)).status_code == 403

    assert photos.delete(f"/api/private-photos/{photo_id}", headers=_as(ADMIN)).status_code == 204
    assert photos.get("/api/private-photos", headers=_as(VIEWER)).json()["photos"] == []


def test_admin_can_download_and_delete_a_selection(photos: TestClient, monkeypatch):
    monkeypatch.setenv("PRIVATE_PHOTOS_ADMIN_EMAILS", ADMIN)
    first = _upload(photos, UPLOADER, "one.jpg", _jpeg((1, 2, 3)), "image/jpeg").json()["uploaded"][0]["id"]
    second = _upload(photos, UPLOADER, "two.jpg", _jpeg((4, 5, 6)), "image/jpeg").json()["uploaded"][0]["id"]
    body = {"ids": [first, second]}

    denied = photos.post("/api/private-photos/bulk-download", headers=_as(VIEWER), json=body)
    assert denied.status_code == 403
    assert photos.post("/api/private-photos/bulk-delete", headers=_as(UPLOADER), json=body).status_code == 403
    assert photos.post("/api/private-photos/bulk-delete", headers=_as(ADMIN), json={"ids": []}).status_code == 400

    downloaded = photos.post("/api/private-photos/bulk-download", headers=_as(ADMIN), json=body)
    assert downloaded.status_code == 200
    assert downloaded.headers["content-type"].startswith("application/zip")
    with zipfile.ZipFile(io.BytesIO(downloaded.content)) as archive:
        names = archive.namelist()
        assert len(names) == 2
        assert first[:8] in names[0] or first[:8] in names[1]
        assert archive.read(names[0])[:2] == b"\xff\xd8"

    removed = photos.post("/api/private-photos/bulk-delete", headers=_as(ADMIN), json=body)
    assert removed.status_code == 200
    assert removed.json()["deleted"] == 2
    assert photos.get("/api/private-photos", headers=_as(VIEWER)).json()["photos"] == []


def test_each_person_opens_only_their_gallery_and_video_or_voice_stays_private(photos: TestClient):
    clip = _upload(photos, UPLOADER, "clip.MOV", _ftyp(b"qt  "), "video/quicktime")
    assert clip.status_code == 200, clip.text
    clip_id = clip.json()["uploaded"][0]["id"]
    assert clip.json()["uploaded"][0]["media_kind"] == "video"

    voice = _upload(photos, OTHER_UPLOADER, "memo.m4a", _ftyp(b"M4A "), "audio/mp4")
    assert voice.status_code == 200, voice.text
    voice_id = voice.json()["uploaded"][0]["id"]
    assert voice.json()["uploaded"][0]["media_kind"] == "audio"

    assert _upload(photos, UPLOADER, "evil.mp4", b"<html>nope</html>", "video/mp4").status_code == 400
    assert _upload(photos, UPLOADER, "memo.wav", _wav(), "audio/wav").status_code == 200

    mine = photos.get("/api/private-photos?scope=mine", headers=_as(UPLOADER)).json()["photos"]
    assert clip_id in [row["id"] for row in mine]
    assert voice_id not in [row["id"] for row in mine]
    received = photos.get("/api/private-photos?scope=received", headers=_as(UPLOADER)).json()["photos"]
    assert voice_id in [row["id"] for row in received]
    assert clip_id not in [row["id"] for row in received]

    played = photos.get(f"/api/private-photos/{clip_id}/media", headers=_as(OTHER_UPLOADER))
    assert played.status_code == 200
    assert played.headers["content-type"].startswith("video/quicktime")
    assert played.content.startswith(_ftyp(b"qt  ")[:8])
    assert photos.get(f"/api/private-photos/{clip_id}/image", headers=_as(OTHER_UPLOADER)).status_code == 404
    assert photos.get(f"/api/private-photos/{voice_id}/media", headers=_as(STRANGER)).status_code == 403


def test_upload_sends_one_pushover_notice_and_still_saves_if_pushover_fails(photos: TestClient, monkeypatch):
    calls: list[str] = []

    class _Response:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _capture(request, timeout=0):
        calls.append(request.data.decode())
        return _Response()

    monkeypatch.setenv("PUSHOVER_USER_KEY", "user-key")
    monkeypatch.setenv("PUSHOVER_API_TOKEN", "app-token")
    monkeypatch.setattr("private_photos.urllib.request.urlopen", _capture)

    saved = _upload(photos, UPLOADER, "day.jpg", _jpeg(), "image/jpeg")
    assert saved.status_code == 200, saved.text
    assert len(calls) == 1
    notice = urllib.parse.unquote_plus(calls[0])
    assert "token=app-token" in notice
    assert "user=user-key" in notice
    assert "sent 1 photo." in notice

    monkeypatch.delenv("PUSHOVER_API_TOKEN")
    calls.clear()
    assert _upload(photos, UPLOADER, "quiet.jpg", _jpeg(), "image/jpeg").status_code == 200
    assert calls == []

    monkeypatch.setenv("PUSHOVER_API_TOKEN", "app-token")

    def _down(*args, **kwargs):
        raise OSError("pushover down")

    monkeypatch.setattr("private_photos.urllib.request.urlopen", _down)
    assert _upload(photos, UPLOADER, "still.jpg", _jpeg(), "image/jpeg").status_code == 200


def test_pushover_message_names_natalya_and_counts_each_kind(monkeypatch):
    import private_photos

    captured: list[str] = []

    class _Response:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _capture(request, timeout=0):
        captured.append(urllib.parse.unquote_plus(request.data.decode()))
        return _Response()

    monkeypatch.setenv("PUSHOVER_USER_KEY", "user-key")
    monkeypatch.setenv("PUSHOVER_API_TOKEN", "app-token")
    monkeypatch.setattr(private_photos.urllib.request, "urlopen", _capture)
    private_photos.notify_uploads(
        "natalyashapran1@gmail.com",
        [{"media_kind": "photo"}, {"media_kind": "photo"}, {"media_kind": "video"}, {"media_kind": "audio"}],
    )
    assert captured
    assert "Nataliia sent 2 photos, 1 video, and 1 voice recording." in captured[0]
    private_photos.notify_uploads("stranger@example.com", [])
    assert len(captured) == 1


def test_local_login_uses_the_allowlist_for_each_role(photos: TestClient, monkeypatch):
    monkeypatch.setenv("PRIVATE_PHOTOS_ADMIN_EMAILS", ADMIN)

    uploader = photos.post("/api/private-photos/local-login", json={"email": f"  {UPLOADER.upper()} "})
    assert uploader.status_code == 200, uploader.text
    assert uploader.json()["can_upload"] is True
    assert uploader.json()["can_view"] is True
    assert uploader.json()["email"] == UPLOADER
    flags = photos.get("/api/private-photos/access").json()
    assert flags["authenticated"] is True
    assert flags["can_upload"] is True
    assert flags["local_session"] is True
    assert photos.post("/api/private-photos/local-logout").status_code == 204
    assert photos.get("/api/private-photos/access").json()["authenticated"] is False

    viewer = photos.post("/api/private-photos/local-login", json={"email": VIEWER})
    assert viewer.json()["can_view"] is True
    assert viewer.json()["can_upload"] is False
    assert photos.post("/api/private-photos/local-logout").status_code == 204

    admin = photos.post("/api/private-photos/local-login", json={"email": ADMIN})
    assert admin.json()["can_admin"] is True
    assert admin.json()["can_view"] is True
    assert admin.json()["can_upload"] is False


def test_local_login_rejects_unknown_emails_and_the_public_site(photos: TestClient):
    for bad in (STRANGER, "not-an-email", "dave@", ""):
        denied = photos.post("/api/private-photos/local-login", json={"email": bad})
        assert denied.status_code == 403, bad
    assert photos.get("/api/private-photos/access").json()["authenticated"] is False

    public = photos.post(
        "/api/private-photos/local-login",
        json={"email": UPLOADER},
        headers={"Host": "inventory.davechogan.com"},
    )
    assert public.status_code == 404
    with_jwt = photos.post(
        "/api/private-photos/local-login",
        json={"email": UPLOADER},
        headers={"Cf-Access-Jwt-Assertion": "present"},
    )
    assert with_jwt.status_code == 404

    photos.post("/api/private-photos/local-login", json={"email": UPLOADER})
    hidden = photos.get(
        "/api/private-photos/access",
        headers={"Host": "inventory.davechogan.com"},
    ).json()
    assert hidden["authenticated"] is False
    assert hidden["local_login"] is False
    cloudflare_wins = photos.get("/api/private-photos/access", headers=_as(VIEWER)).json()
    assert cloudflare_wins["can_view"] is True
    assert cloudflare_wins["can_upload"] is False
    assert cloudflare_wins["local_session"] is False


def test_deleted_files_stay_restorable_for_30_days_then_are_removed(photos: TestClient, monkeypatch, tmp_path, invapp):
    """Hide on delete, keep bytes, restore within 30 days, purge after that."""
    monkeypatch.setenv("PRIVATE_PHOTOS_ADMIN_EMAILS", ADMIN)
    photo_id = _upload(photos, UPLOADER, "keep.jpg", _jpeg(), "image/jpeg").json()["uploaded"][0]["id"]
    second_id = _upload(photos, UPLOADER, "bulk.jpg", _jpeg((4, 5, 6)), "image/jpeg").json()["uploaded"][0]["id"]
    voice = _upload(photos, UPLOADER, "memo.m4a", _ftyp(b"M4A "), "audio/mp4")
    assert voice.status_code == 200, voice.text
    voice_id = voice.json()["uploaded"][0]["id"]

    assert photos.delete(f"/api/private-photos/{photo_id}", headers=_as(ADMIN)).status_code == 204
    assert photos.delete(f"/api/private-photos/{voice_id}", headers=_as(UPLOADER)).status_code == 204
    bulk = photos.post("/api/private-photos/bulk-delete", headers=_as(ADMIN), json={"ids": [second_id]})
    assert bulk.status_code == 200
    assert bulk.json()["deleted"] == 1

    mine = [row["id"] for row in photos.get("/api/private-photos?scope=mine", headers=_as(UPLOADER)).json()["photos"]]
    received = [row["id"] for row in photos.get("/api/private-photos?scope=received", headers=_as(ADMIN)).json()["photos"]]
    assert photo_id not in mine and voice_id not in mine and second_id not in mine
    assert photo_id not in received and second_id not in received

    assert (tmp_path / "originals" / f"{photo_id}.jpg").is_file()
    assert (tmp_path / "display" / f"{photo_id}.jpg").is_file()
    assert (tmp_path / "originals" / f"{voice_id}.m4a").is_file()

    hidden = photos.get("/api/private-photos?scope=deleted", headers=_as(ADMIN)).json()["photos"]
    hidden_ids = {row["id"] for row in hidden}
    assert hidden_ids == {photo_id, second_id, voice_id}
    assert all(row["deleted_at"] for row in hidden)

    assert photos.get("/api/private-photos?scope=deleted", headers=_as(UPLOADER)).status_code == 403
    assert photos.get("/api/private-photos?scope=deleted", headers=_as(VIEWER)).status_code == 403
    assert photos.post("/api/private-photos/restore", headers=_as(UPLOADER), json={"ids": [photo_id]}).status_code == 403
    assert photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(UPLOADER)).status_code == 404
    assert photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(VIEWER)).status_code == 404
    assert photos.get(f"/api/private-photos/{voice_id}/media", headers=_as(UPLOADER)).status_code == 404
    assert photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(ADMIN)).status_code == 200

    assert photos.post("/api/private-photos/restore", headers=_as(ADMIN), json={"ids": []}).status_code == 400
    assert photos.post("/api/private-photos/restore", headers=_as(ADMIN), json={"ids": ["not-an-id"]}).status_code == 400
    restored = photos.post("/api/private-photos/restore", headers=_as(ADMIN), json={"ids": [photo_id]})
    assert restored.status_code == 200
    assert restored.json()["restored"] == 1
    mine = [row["id"] for row in photos.get("/api/private-photos?scope=mine", headers=_as(UPLOADER)).json()["photos"]]
    assert photo_id in mine
    still_hidden = [row["id"] for row in photos.get("/api/private-photos?scope=deleted", headers=_as(ADMIN)).json()["photos"]]
    assert photo_id not in still_hidden
    assert (tmp_path / "originals" / f"{photo_id}.jpg").is_file()

    again = photos.post("/api/private-photos/restore", headers=_as(ADMIN), json={"ids": [photo_id]})
    assert again.json()["restored"] == 0

    with invapp.get_conn() as conn:
        conn.execute(
            "UPDATE private_photos SET deleted_at = datetime('now', '-29 days') WHERE id = ?",
            (second_id,),
        )
        conn.execute(
            "UPDATE private_photos SET deleted_at = datetime('now', '-31 days') WHERE id = ?",
            (voice_id,),
        )
    after_window = [row["id"] for row in photos.get("/api/private-photos?scope=deleted", headers=_as(ADMIN)).json()["photos"]]
    assert second_id in after_window
    assert voice_id not in after_window
    assert not (tmp_path / "originals" / f"{voice_id}.m4a").exists()

    with invapp.get_conn() as conn:
        conn.execute(
            "UPDATE private_photos SET deleted_at = datetime('now', '-31 days') WHERE id = ?",
            (second_id,),
        )
    gone = [row["id"] for row in photos.get("/api/private-photos?scope=deleted", headers=_as(ADMIN)).json()["photos"]]
    assert second_id not in gone
    assert not (tmp_path / "originals" / f"{second_id}.jpg").exists()
    assert not (tmp_path / "display" / f"{second_id}.jpg").exists()
    assert not (tmp_path / "thumbs" / f"{second_id}.jpg").exists()


def test_each_upload_keeps_its_own_caption_and_only_the_owner_or_admin_can_change_it(photos: TestClient, monkeypatch):
    monkeypatch.setenv("PRIVATE_PHOTOS_ADMIN_EMAILS", ADMIN)
    created = photos.post(
        "/api/private-photos",
        files=[
            ("files", ("day.jpg", _jpeg(), "image/jpeg")),
            ("captions", (None, "  At the lake  ")),
            ("files", ("clip.MOV", _ftyp(b"qt  "), "video/quicktime")),
            ("captions", (None, "   ")),
        ],
        headers=_as(UPLOADER),
    )
    assert created.status_code == 200, created.text
    uploaded = {row["media_kind"]: row for row in created.json()["uploaded"]}
    photo_id = uploaded["photo"]["id"]
    video_id = uploaded["video"]["id"]
    assert uploaded["photo"]["caption"] == "At the lake"
    assert uploaded["video"]["caption"] is None

    too_many = photos.post(
        "/api/private-photos",
        files=[
            ("files", ("one.jpg", _jpeg(), "image/jpeg")),
            ("captions", (None, "One")),
            ("captions", (None, "Two")),
        ],
        headers=_as(UPLOADER),
    )
    assert too_many.status_code == 400

    too_long = photos.post(
        "/api/private-photos",
        files=[
            ("files", ("long.jpg", _jpeg((1, 2, 3)), "image/jpeg")),
            ("captions", (None, "x" * 501)),
        ],
        headers=_as(UPLOADER),
    )
    assert too_long.status_code == 400

    voice = photos.post(
        "/api/private-photos",
        files=[
            ("files", ("memo.m4a", _ftyp(b"M4A "), "audio/mp4")),
            ("captions", (None, "Voice note")),
        ],
        headers=_as(UPLOADER),
    )
    assert voice.status_code == 200, voice.text
    assert voice.json()["uploaded"][0]["caption"] == "Voice note"

    mine = photos.get("/api/private-photos?scope=mine", headers=_as(UPLOADER)).json()["photos"]
    by_id = {row["id"]: row["caption"] for row in mine}
    assert by_id[photo_id] == "At the lake"
    assert by_id[video_id] is None
    seen = photos.get("/api/private-photos", headers=_as(VIEWER)).json()["photos"]
    assert next(row["caption"] for row in seen if row["id"] == photo_id) == "At the lake"

    denied = photos.patch(
        f"/api/private-photos/{photo_id}/caption",
        headers=_as(VIEWER),
        json={"caption": "Not yours"},
    )
    assert denied.status_code == 403
    assert photos.patch(
        f"/api/private-photos/{photo_id}/caption",
        headers=_as(STRANGER),
        json={"caption": "No"},
    ).status_code == 403
    assert photos.patch(
        f"/api/private-photos/{photo_id}/caption",
        headers=_as(UPLOADER),
        json={"caption": 12},
    ).status_code == 400
    assert photos.patch(
        "/api/private-photos/not-a-real-id/caption",
        headers=_as(UPLOADER),
        json={"caption": "Missing"},
    ).status_code == 404

    changed = photos.patch(
        f"/api/private-photos/{video_id}/caption",
        headers=_as(UPLOADER),
        json={"caption": "The dock"},
    )
    assert changed.status_code == 200
    assert changed.json()["caption"] == "The dock"
    admin_edit = photos.patch(
        f"/api/private-photos/{photo_id}/caption",
        headers=_as(ADMIN),
        json={"caption": "  Evening light "},
    )
    assert admin_edit.status_code == 200
    assert admin_edit.json()["caption"] == "Evening light"
    cleared = photos.patch(
        f"/api/private-photos/{photo_id}/caption",
        headers=_as(UPLOADER),
        json={"caption": "   "},
    )
    assert cleared.status_code == 200
    assert cleared.json()["caption"] is None
    mine_after = photos.get("/api/private-photos?scope=mine", headers=_as(UPLOADER)).json()["photos"]
    assert next(row["caption"] for row in mine_after if row["id"] == photo_id) is None
    assert next(row["caption"] for row in mine_after if row["id"] == video_id) == "The dock"


def test_chat_keeps_one_ordered_thread_and_rejects_blank_or_huge_messages(photos: TestClient):
    first = photos.post("/api/private-photos/chat", headers=_as(UPLOADER), json={"body": "  Hello  "})
    second = photos.post("/api/private-photos/chat", headers=_as(VIEWER), json={"body": "Hi there"})
    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["mine"] is True
    assert first.json()["body"] == "Hello"

    for email in (UPLOADER, VIEWER):
        thread = photos.get("/api/private-photos/chat", headers=_as(email)).json()
        assert [row["body"] for row in thread["messages"]] == ["Hello", "Hi there"]

    assert photos.post("/api/private-photos/chat", headers=_as(UPLOADER), json={"body": "   "}).status_code == 400
    assert photos.post(
        "/api/private-photos/chat", headers=_as(UPLOADER), json={"body": "x" * 2001}
    ).status_code == 400
    assert photos.post("/api/private-photos/chat", headers=_as(UPLOADER), json={}).status_code == 400
    assert photos.get("/api/private-photos/chat", headers=_as(STRANGER)).status_code == 403
    assert photos.get("/api/private-photos/chat").status_code == 401


def test_chat_unread_is_only_the_other_persons_messages(photos: TestClient):
    photos.post("/api/private-photos/chat", headers=_as(UPLOADER), json={"body": "One"})
    hers = photos.get("/api/private-photos/chat", headers=_as(UPLOADER)).json()
    his = photos.get("/api/private-photos/chat", headers=_as(VIEWER)).json()
    assert hers["unread"] == 0
    assert his["unread"] == 1

    photos.post("/api/private-photos/chat", headers=_as(VIEWER), json={"body": "Two"})
    assert photos.get("/api/private-photos/chat", headers=_as(VIEWER)).json()["unread"] == 1
    assert photos.get("/api/private-photos/chat", headers=_as(UPLOADER)).json()["unread"] == 1

    marked = photos.post("/api/private-photos/chat/read", headers=_as(VIEWER))
    assert marked.status_code == 200
    assert marked.json()["unread"] == 0
    assert photos.get("/api/private-photos/chat", headers=_as(VIEWER)).json()["unread"] == 0
    assert photos.get("/api/private-photos/chat", headers=_as(UPLOADER)).json()["unread"] == 1
    assert photos.post("/api/private-photos/chat/read", headers=_as(STRANGER)).status_code == 403


def test_chat_notifies_only_the_other_phone_and_skips_a_blank_key(photos: TestClient, monkeypatch):
    calls: list[str] = []

    class _Response:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _capture(request, timeout=0):
        calls.append(urllib.parse.unquote_plus(request.data.decode()))
        return _Response()

    monkeypatch.setenv("PRIVATE_PHOTOS_UPLOAD_EMAILS", "natalyashapran1@gmail.com,davechogan@gmail.com")
    monkeypatch.setenv("PRIVATE_PHOTOS_VIEW_EMAILS", "")
    monkeypatch.setenv("PRIVATE_PHOTOS_ADMIN_EMAILS", "davechogan@gmail.com")
    monkeypatch.setenv("PUSHOVER_API_TOKEN", "app-token")
    monkeypatch.setenv("PUSHOVER_USER_KEY", "dave-key")
    monkeypatch.delenv("PUSHOVER_NATALYA_USER_KEY", raising=False)
    monkeypatch.setattr("private_photos.urllib.request.urlopen", _capture)

    sent = photos.post(
        "/api/private-photos/chat",
        headers=_as("natalyashapran1@gmail.com"),
        json={"body": "On my way"},
    )
    assert sent.status_code == 200, sent.text
    assert len(calls) == 1
    assert "user=dave-key" in calls[0]
    assert "Nataliia: On my way" in calls[0]
    assert "url=https://inventory.davechogan.com/photos?chat=1" in calls[0]
    assert "Open chat" in calls[0]

    calls.clear()
    dave = photos.post(
        "/api/private-photos/chat",
        headers=_as("davechogan@gmail.com"),
        json={"body": "See you soon"},
    )
    assert dave.status_code == 200, dave.text
    assert calls == []

    monkeypatch.setenv("PUSHOVER_NATALYA_USER_KEY", "natalya-key")
    dave_again = photos.post(
        "/api/private-photos/chat",
        headers=_as("davechogan@gmail.com"),
        json={"body": "Leaving now"},
    )
    assert dave_again.status_code == 200, dave_again.text
    assert len(calls) == 1
    assert "user=natalya-key" in calls[0]
    assert "user=dave-key" not in calls[0]
    assert "Dave: Leaving now" in calls[0]

    def _down(*args, **kwargs):
        raise OSError("pushover down")

    monkeypatch.setattr("private_photos.urllib.request.urlopen", _down)
    still = photos.post(
        "/api/private-photos/chat",
        headers=_as("natalyashapran1@gmail.com"),
        json={"body": "Still here"},
    )
    assert still.status_code == 200, still.text
    dave_thread = photos.get("/api/private-photos/chat", headers=_as("davechogan@gmail.com")).json()
    nataliia_thread = photos.get("/api/private-photos/chat", headers=_as("natalyashapran1@gmail.com")).json()
    assert dave_thread["title"] == "Nataliia"
    assert nataliia_thread["title"] == "Dave"
    assert [row["body"] for row in dave_thread["messages"]][-1] == "Still here"
    # Both people see the same sender on each message, which is what the bubble color uses.
    dave_senders = [(row["sender_email"], row["sender_name"]) for row in dave_thread["messages"]]
    nataliia_senders = [(row["sender_email"], row["sender_name"]) for row in nataliia_thread["messages"]]
    assert dave_senders == nataliia_senders
    assert ("natalyashapran1@gmail.com", "Nataliia") in dave_senders
    assert ("davechogan@gmail.com", "Dave") in dave_senders
