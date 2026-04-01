"""
Cloudflare Access authentication middleware.

Reads Cf-Access-Authenticated-User-Email from request headers (set by Cloudflare Access
after Google OAuth). Upserts user into the `users` table and stores identity in
request.state for downstream route handlers.

Phase 1: capture & log only. No enforcement, no JWT validation.
See artifacts/plans/AUTH_DESIGN.md for full architecture.
"""

import logging
import sqlite3
import uuid
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("mkc_auth")

# Cloudflare Access headers
CF_EMAIL_HEADER = "Cf-Access-Authenticated-User-Email"
CF_JWT_HEADER = "Cf-Access-Jwt-Assertion"


class UserInfo:
    """Lightweight user identity object stored in request.state.user."""

    __slots__ = ("id", "email", "name", "tenant_id", "role")

    def __init__(self, id: str, email: str, name: Optional[str], tenant_id: str, role: str):
        self.id = id
        self.email = email
        self.name = name
        self.tenant_id = tenant_id
        self.role = role

    def __repr__(self) -> str:
        return f"UserInfo(email={self.email!r}, tenant={self.tenant_id!r}, role={self.role!r})"


def _upsert_user(conn: sqlite3.Connection, email: str) -> UserInfo:
    """Insert or update user by email. Returns UserInfo."""
    row = conn.execute("SELECT id, email, name, tenant_id, role FROM users WHERE email = ?", (email,)).fetchone()
    if row:
        conn.execute("UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", (row["id"],))
        return UserInfo(id=row["id"], email=row["email"], name=row["name"], tenant_id=row["tenant_id"], role=row["role"])
    else:
        user_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO users (id, email, tenant_id, role) VALUES (?, ?, 'default', 'user')",
            (user_id, email),
        )
        logger.info("New user registered: %s (id=%s)", email, user_id)
        return UserInfo(id=user_id, email=email, name=None, tenant_id="default", role="user")


class CloudflareAccessMiddleware(BaseHTTPMiddleware):
    """
    Middleware that reads Cloudflare Access identity headers and populates request.state.user.

    If the header is absent (e.g. local dev without Cloudflare), request.state.user is None.
    No request is blocked — this is Phase 1 (capture only).
    """

    def __init__(self, app, get_conn: Callable):
        super().__init__(app)
        self.get_conn = get_conn

    async def dispatch(self, request: Request, call_next) -> Response:
        email = request.headers.get(CF_EMAIL_HEADER)

        if email:
            try:
                with self.get_conn() as conn:
                    user = _upsert_user(conn, email.strip().lower())
                request.state.user = user
                logger.debug("Authenticated: %s", user.email)
            except Exception:
                logger.exception("Failed to upsert user for %s", email)
                request.state.user = None
        else:
            request.state.user = None

        response = await call_next(request)
        return response


def get_current_user(request: Request) -> Optional[UserInfo]:
    """Helper to extract user from request.state. Returns None if unauthenticated."""
    return getattr(request.state, "user", None)


def get_tenant_id(request: Request) -> Optional[str]:
    """Extract tenant_id from the authenticated user. Returns None in dev mode (no auth)."""
    user = get_current_user(request)
    return user.tenant_id if user else None


def tenant_filter_sql(tenant_id: Optional[str], table_alias: str = "i") -> tuple[str, list]:
    """Return a WHERE clause fragment and params for tenant scoping.

    If tenant_id is None (dev mode / no auth), returns empty string and no params
    so all data is visible. In production with auth, scopes to the user's tenant.
    """
    if tenant_id is None:
        return "", []
    return f"{table_alias}.tenant_id = ?", [tenant_id]
