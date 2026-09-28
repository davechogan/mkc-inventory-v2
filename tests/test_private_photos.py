"""Private photo share: upload and view are different permissions."""

from __future__ import annotations

import io

import pytest
from fastapi.testclient import TestClient
from PIL import Image

UPLOADER = "she@example.com"
OTHER_UPLOADER = "sister@example.com"
VIEWER = "he@example.com"
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

    assert uploader == {"authenticated": True, "can_upload": True, "can_view": False}
    assert viewer == {"authenticated": True, "can_upload": False, "can_view": True}
    assert stranger == {"authenticated": True, "can_upload": False, "can_view": False}
    assert anonymous == {"authenticated": False, "can_upload": False, "can_view": False}


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

    # Upload permission does not include the shared viewer.
    assert photos.get("/api/private-photos?scope=all", headers=_as(UPLOADER)).status_code == 403
    blocked = photos.get(f"/api/private-photos/{photo_id}/image", headers=_as(UPLOADER))
    assert blocked.status_code == 403

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

    other_thumb = photos.get(f"/api/private-photos/{second_id}/thumb", headers=_as(UPLOADER))
    assert other_thumb.status_code == 403

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
