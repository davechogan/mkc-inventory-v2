"""Legacy web UI HTML shells (mounted from app; assets via /static mount)."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse


def create_static_pages_router(*, static_dir: Path) -> APIRouter:
    router = APIRouter(tags=["static-pages"])

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

    return router
