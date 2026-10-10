# Authentication for the API and the dashboard
# ============================================
# A single shared token, supplied either as an `Authorization: Bearer` header
# (API clients, curl, Prometheus) or as an `hc_token` cookie (browser, set by
# the /login form). Health probes stay public so orchestrators can reach them.
#
# When HC_AUTH_TOKEN is unset, authentication is disabled and a warning is
# logged at startup. That keeps local development friction-free, but every
# endpoint — including the target list, which carries connection strings — is
# then readable by anyone who can reach the port.

from __future__ import annotations

import logging
import secrets

from fastapi import HTTPException, Request, status

from app.config import settings

logger = logging.getLogger(__name__)

COOKIE_NAME = "hc_token"


def auth_enabled() -> bool:
    return settings.auth_token is not None and bool(settings.auth_token.get_secret_value())


def _expected_token() -> str:
    assert settings.auth_token is not None
    return settings.auth_token.get_secret_value()


def token_is_valid(candidate: str | None) -> bool:
    """Constant-time token comparison."""
    if not candidate:
        return False
    return secrets.compare_digest(candidate, _expected_token())


def _token_from_request(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    scheme, _, value = header.partition(" ")
    if scheme.lower() == "bearer" and value:
        return value.strip()
    return request.cookies.get(COOKIE_NAME)


async def require_auth(request: Request) -> None:
    """Dependency guarding every non-public route."""
    if not auth_enabled():
        return
    if token_is_valid(_token_from_request(request)):
        return

    # Browser navigation gets redirected to the login form; API clients get 401.
    accepts_html = "text/html" in request.headers.get("accept", "")
    if accepts_html and request.method == "GET" and not request.url.path.startswith("/api/"):
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
            detail="Authentication required",
        )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing token",
        headers={"WWW-Authenticate": "Bearer"},
    )


def warn_if_unauthenticated() -> None:
    """Called at startup. The target list exposes connection strings."""
    if auth_enabled():
        logger.info("Authentication enabled (HC_AUTH_TOKEN is set)")
        return
    logger.warning(
        "HC_AUTH_TOKEN is not set: the API and dashboard are UNAUTHENTICATED. "
        "Anyone who can reach this port can read and modify the target list. "
        "Set HC_AUTH_TOKEN, or make sure the port is not reachable from outside the host."
    )
