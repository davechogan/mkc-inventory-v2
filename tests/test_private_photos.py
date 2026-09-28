"""Private photo share: upload and view are different permissions."""

from __future__ import annotations

import io
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
        "authenticated": True, "can_upload": True, "can_view": True, "can_admin": False,
        "local_login": False, "local_session": False,
    }
    assert viewer == {
        "authenticated": True, "can_upload": False, "can_view": True, "can_admin": False,
        "local_login": False, "local_session": False,
    }
    assert stranger == {
        "authenticated": True, "can_upload": False, "can_view": False, "can_admin": False,
        "local_login": False, "local_session": False,
    }
    assert anonymous == {
        "authenticated": False, "can_upload": False, "can_view": False, "can_admin": False,
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


def test_local_login_uses_the_allowlist_for_each_role(photos: TestClient, monkeypatch):
    monkeypatch.setenv("PRIVATE_PHOTOS_ADMIN_EMAILS", ADMIN)

    uploader = photos.post("/api/private-photos/local-login", json={"email": f"  {UPLOADER.upper()} "})
    assert uploader.status_code == 200, uploader.text
    assert uploader.json()["can_upload"] is True
    assert uploader.json()["can_view"] is True
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
