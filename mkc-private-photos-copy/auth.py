"""
Cloudflare Access authentication for photos app.
Simplified version: no tenant logic, no user DB upserts.
"""

import logging
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("photos_auth")

# Cloudflare Access headers
CF_EMAIL_HEADER = "Cf-Access-Authenticated-User-Email"
CF_JWT_HEADER = "Cf-Access-Jwt-Assertion"
LOCAL_USER_COOKIE = "mkc_local_user"
PUBLIC_APP_HOSTS = frozenset({"photos.davechogan.com", "inventory.davechogan.com"})


class UserInfo:
    """Minimal user identity for photos app."""
    __slots__ = ("email",)

    def __init__(self, email: str):
        self.email = email

    def __repr__(self) -> str:
        return f"UserInfo(email={self.email!r})"


def is_local_access(request: Request) -> bool:
    """True when not accessed through Cloudflare."""
    if request.headers.get(CF_JWT_HEADER):
        return False
    host = (request.headers.get("host") or "").split(":")[0].strip().lower()
    return host not in PUBLIC_APP_HOSTS


def is_local_session(request: Request) -> bool:
    """True when identity came from local email cookie."""
    return bool(getattr(request.state, "local_session", False))


def get_current_user(request: Request) -> Optional[UserInfo]:
    """Extract user from request.state. Returns None if unauthenticated."""
    return getattr(request.state, "user", None)


class CloudflareAccessMiddleware(BaseHTTPMiddleware):
    """
    Reads Cloudflare Access headers and populates request.state.user.
    On local access, accepts email cookie from photo allowlist.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        from private_photos import allowlisted_photo_email

        request.state.local_session = False
        email = (request.headers.get(CF_EMAIL_HEADER) or "").strip().lower()
        
        if not email and is_local_access(request):
            email = allowlisted_photo_email(request.cookies.get(LOCAL_USER_COOKIE)) or ""
            request.state.local_session = bool(email)

        if email:
            request.state.user = UserInfo(email=email)
            logger.debug("Authenticated: %s", email)
        else:
            request.state.user = None
            request.state.local_session = False

        response = await call_next(request)
        return response
