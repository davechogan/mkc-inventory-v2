"""
Cloudflare Access authentication middleware.

Reads Cf-Access-Authenticated-User-Email from request headers (set by Cloudflare Access
after Google OAuth). Upserts user into the `users` table, claims pending invitations,
and stores identity + memberships in request.state.

See artifacts/plans/AUTH_DESIGN.md and TENANT_AND_ONBOARDING_DESIGN.md.
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

# Tenant selection header (set by frontend)
TENANT_HEADER = "X-Tenant-Id"


class TenantMembership:
    """A user's membership in a tenant."""
    __slots__ = ("tenant_id", "tenant_name", "role")

    def __init__(self, tenant_id: str, tenant_name: str, role: str):
        self.tenant_id = tenant_id
        self.tenant_name = tenant_name
        self.role = role


class UserInfo:
    """Lightweight user identity object stored in request.state.user."""

    __slots__ = ("id", "email", "name", "memberships", "is_new")

    def __init__(self, id: str, email: str, name: Optional[str],
                 memberships: list[TenantMembership], is_new: bool = False):
        self.id = id
        self.email = email
        self.name = name
        self.memberships = memberships
        self.is_new = is_new

    def has_tenant(self, tenant_id: str) -> bool:
        return any(m.tenant_id == tenant_id for m in self.memberships)

    def get_role(self, tenant_id: str) -> Optional[str]:
        for m in self.memberships:
            if m.tenant_id == tenant_id:
                return m.role
        return None

    def __repr__(self) -> str:
        return f"UserInfo(email={self.email!r}, memberships={len(self.memberships)}, is_new={self.is_new})"


def _get_memberships(conn: sqlite3.Connection, user_id: str) -> list[TenantMembership]:
    """Load all tenant memberships for a user."""
    rows = conn.execute(
        """SELECT tm.tenant_id, t.name AS tenant_name, tm.role
           FROM tenant_members tm
           JOIN tenants t ON t.id = tm.tenant_id
           WHERE tm.user_id = ?
           ORDER BY tm.created_at""",
        (user_id,),
    ).fetchall()
    return [TenantMembership(r["tenant_id"], r["tenant_name"], r["role"]) for r in rows]


def _claim_pending_invites(conn: sqlite3.Connection, user_id: str, email: str) -> int:
    """Claim any pending tenant invitations for this email. Returns count claimed."""
    pending = conn.execute(
        "SELECT id, tenant_id, role FROM tenant_members WHERE invited_email = ? AND user_id IS NULL",
        (email,),
    ).fetchall()
    for p in pending:
        conn.execute(
            "UPDATE tenant_members SET user_id = ?, invited_email = NULL WHERE id = ?",
            (user_id, p["id"]),
        )
    if pending:
        logger.info("Claimed %d pending invitation(s) for %s", len(pending), email)
    return len(pending)


def _upsert_user(conn: sqlite3.Connection, email: str) -> UserInfo:
    """Insert or update user by email. Claims pending invites. Returns UserInfo with memberships."""
    row = conn.execute("SELECT id, email, name FROM users WHERE email = ?", (email,)).fetchone()
    is_new = False

    if row:
        user_id = row["id"]
        name = row["name"]
        conn.execute("UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", (user_id,))
    else:
        user_id = str(uuid.uuid4())
        name = None
        is_new = True
        conn.execute(
            "INSERT INTO users (id, email, tenant_id, role) VALUES (?, ?, 'default', 'user')",
            (user_id, email),
        )
        logger.info("New user registered: %s (id=%s)", email, user_id)

    # Claim any pending invitations
    _claim_pending_invites(conn, user_id, email)

    # Load memberships
    memberships = _get_memberships(conn, user_id)

    return UserInfo(id=user_id, email=email, name=name, memberships=memberships, is_new=is_new)


class CloudflareAccessMiddleware(BaseHTTPMiddleware):
    """
    Middleware that reads Cloudflare Access identity headers and populates request.state.user.

    If the header is absent (e.g. local dev without Cloudflare), request.state.user is None.
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
                logger.debug("Authenticated: %s (%d memberships)", user.email, len(user.memberships))
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
    """Extract the active tenant_id from the X-Tenant-Id header.
    Falls back to user's first membership. Returns None in dev mode."""
    # Check header first (frontend sets this)
    header_tid = request.headers.get(TENANT_HEADER)
    if header_tid:
        return header_tid

    # Fall back to user's first membership
    user = get_current_user(request)
    if user and user.memberships:
        return user.memberships[0].tenant_id

    return None


def verify_tenant_access(request: Request, tenant_id: str, required_role: Optional[str] = None) -> None:
    """Verify the user has access to the given tenant. Raises HTTPException if not.
    In dev mode (no user), access is granted."""
    user = get_current_user(request)
    if user is None:
        return  # dev mode

    role = user.get_role(tenant_id)
    if role is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Access denied to this collection.")

    if required_role:
        role_hierarchy = {"owner": 3, "editor": 2, "viewer": 1}
        if role_hierarchy.get(role, 0) < role_hierarchy.get(required_role, 0):
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail=f"Requires {required_role} access.")


def tenant_filter_sql(tenant_id: Optional[str], table_alias: str = "i") -> tuple[str, list]:
    """Return a WHERE clause fragment and params for tenant scoping."""
    if tenant_id is None:
        return "", []
    return f"{table_alias}.tenant_id = ?", [tenant_id]
