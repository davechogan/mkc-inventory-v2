"""
Private Photos & Chat Application
Standalone FastAPI app for photo sharing between allowlisted users.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("photos_app")

# Database path - defaults to shared DB with main inventory app
_db_override = (os.environ.get("MKC_INVENTORY_DB") or "").strip()
if _db_override:
    DB_PATH = Path(_db_override).expanduser().resolve()
else:
    # Default: look for main app's DB in typical deployment location
    DB_PATH = Path("/Users/dhogan/invapp_v2/data/mkc_inventory.db")
    # In development, fall back to sibling directory
    if not DB_PATH.exists():
        DB_PATH = Path(__file__).resolve().parent.parent / "mkc-inventory" / "data" / "mkc_inventory.db"

logger.info("Database path: %s (exists: %s)", DB_PATH, DB_PATH.exists())

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


@contextmanager
def get_conn():
    """Database connection context manager. Shares DB with main inventory app."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode for better concurrent access
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Create FastAPI app
app = FastAPI(
    title="MKC Private Photos",
    description="Private photo and chat sharing",
    version="1.0.0",
    docs_url=None,  # Disable Swagger docs in production
    redoc_url=None,  # Disable ReDoc in production
)

# Auth middleware
from auth import CloudflareAccessMiddleware
app.add_middleware(CloudflareAccessMiddleware)

# Routes
from routes.photos_routes import create_photos_router
app.include_router(create_photos_router(get_conn=get_conn))

# Static files - mount assets directory so Vite-built JS/CSS are served at /assets/*
if STATIC_DIR.exists() and (STATIC_DIR / "dist" / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "dist" / "assets")), name="assets")
    logger.info("Mounted static assets from %s", STATIC_DIR / "dist" / "assets")

# Root page - serves Photos SPA
@app.get("/")
def root():
    """Photos page HTML shell."""
    react_build = STATIC_DIR / "dist" / "index.html"
    if not react_build.exists():
        logger.warning("Frontend not built at %s", react_build)
        return {
            "error": "Frontend not built",
            "message": "Run: cd frontend && npm install && npm run build",
            "expected_path": str(react_build)
        }
    return FileResponse(
        react_build,
        headers={
            "Cache-Control": "private, no-store",
            "X-Robots-Tag": "noindex, nofollow",
        },
    )


@app.get("/health")
def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "app": "mkc-private-photos",
        "version": "1.0.0",
        "db_path": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
        "static_built": (STATIC_DIR / "dist" / "index.html").exists(),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8009"))
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
