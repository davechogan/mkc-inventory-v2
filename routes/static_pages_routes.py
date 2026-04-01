"""Web UI page routes (mounted from app; assets via /static mount)."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, RedirectResponse


def create_static_pages_router(*, static_dir: Path) -> APIRouter:
    router = APIRouter(tags=["static-pages"])

    @router.get("/auth/login")
    def auth_login():
        """Login redirect — Cloudflare Access protects this path and forces authentication.
        After auth, the user arrives here with JWT headers set, and we redirect to /.
        The AuthGate at / then checks /api/v2/me and routes to the right page."""
        return RedirectResponse(url="/", status_code=302)

    @router.get("/")
    def root():
        # Serve the React SPA build; falls back to legacy HTML if build is missing
        react_build = static_dir / "dist" / "index.html"
        return FileResponse(react_build if react_build.exists() else static_dir / "index.html")

    @router.get("/identify")
    def identify_page():
        # Serve the React SPA build; falls back to legacy HTML if build is missing
        react_build = static_dir / "dist" / "index.html"
        return FileResponse(react_build if react_build.exists() else static_dir / "identify.html")

    @router.get("/master")
    def master_page():
        # Serve the React SPA build; falls back to legacy HTML if build is missing
        react_build = static_dir / "dist" / "index.html"
        return FileResponse(react_build if react_build.exists() else static_dir / "master.html")

    @router.get("/admin")
    def admin_page():
        react_build = static_dir / "dist" / "index.html"
        return FileResponse(react_build if react_build.exists() else static_dir / "index.html")

    return router
