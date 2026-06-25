"""
Cloudflare Access JWT validation for the Bridge origin.

Design
------
CF Access is the ONLY browser auth gate.  The origin's job is to verify the
request actually came through CF (validate the JWT) and to admit trusted
internal automation (shared secret).  No second independent login flow.

The browser sends nothing new — CF injects Cf-Access-Jwt-Assertion on every
proxied request; the middleware here validates it.

Required env vars (set in launchd plist / trader env):
  CF_ACCESS_TEAM_DOMAIN  — e.g. ollietrades.cloudflareaccess.com
  CF_ACCESS_AUD          — Access application AUD tag (CF dashboard → Access →
                           the Bridge application → Overview → Application AUD)
  BRIDGE_INTERNAL_TOKEN  — shared secret for internal callers (trader, scanners,
                           Kirk, sync jobs).  Generate with:
                           python3 -c "import secrets; print(secrets.token_hex(32))"

Loopback-bypass gotcha (explained here so it's not re-litigated)
-----------------------------------------------------------------
It is tempting to whitelist 127.0.0.1 as "trusted internal."  Don't.  The
Bridge is served via the cloudflared tunnel, so ALL public traffic arrives at
the origin from localhost.  The existing AuthMiddleware already guards against
this by checking _is_via_cf_tunnel(); cf_auth.py is the CF side of that guard.
"""
from __future__ import annotations

import hmac
import logging
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import Request

logger = logging.getLogger(__name__)

# JWK client cache keyed by team_domain.  PyJWKClient auto-caches the fetched
# key set and re-fetches when a kid is unknown — no manual TTL needed.
_jwks_clients: dict[str, object] = {}


def _jwks_client(team_domain: str):
    if team_domain not in _jwks_clients:
        try:
            from jwt import PyJWKClient  # PyJWT ≥ 2.4 — already in .venv
            _jwks_clients[team_domain] = PyJWKClient(
                f"https://{team_domain}/cdn-cgi/access/certs"
            )
        except Exception as exc:
            logger.warning("cf_auth: cannot build JWKSClient for %s: %s", team_domain, exc)
            return None
    return _jwks_clients[team_domain]


def _valid_cf_jwt(token: str) -> bool:
    """Return True if token is a valid CF Access JWT for this application."""
    team = os.environ.get("CF_ACCESS_TEAM_DOMAIN", "")
    aud  = os.environ.get("CF_ACCESS_AUD", "")
    if not team or not aud:
        # Not configured → we can't validate → fail closed
        logger.debug("cf_auth: CF_ACCESS_TEAM_DOMAIN or CF_ACCESS_AUD not set; rejecting")
        return False
    client = _jwks_client(team)
    if not client:
        return False
    try:
        import jwt as _jwt
        signing_key = client.get_signing_key_from_jwt(token)  # type: ignore[union-attr]
        _jwt.decode(token, signing_key.key, algorithms=["RS256"], audience=aud)
        return True
    except Exception as exc:
        logger.debug("cf_auth: JWT rejected: %s", exc)
        return False


def try_cf_access(request: "Any") -> bool:
    """Return True if the request carries a valid Cloudflare Access JWT."""
    token = request.headers.get("cf-access-jwt-assertion", "")
    if not token:
        return False
    return _valid_cf_jwt(token)


def try_internal_token(request: "Any") -> bool:
    """
    Return True if the request carries the shared internal token.

    Timing-safe comparison via hmac.compare_digest.
    Returns False (not an error) when BRIDGE_INTERNAL_TOKEN is unset — this
    keeps internal automation working via the existing loopback bypass in
    AuthMiddleware until callers are migrated to send the header.
    """
    expected = os.environ.get("BRIDGE_INTERNAL_TOKEN", "")
    if not expected:
        return False
    got = request.headers.get("x-internal-token", "")
    return hmac.compare_digest(got, expected)


def is_configured() -> bool:
    """True if both CF Access env vars are set (safe to require the JWT)."""
    return bool(os.environ.get("CF_ACCESS_TEAM_DOMAIN") and os.environ.get("CF_ACCESS_AUD"))


# ---------------------------------------------------------------------------
# Request-inspection helpers (used by AuthMiddleware and RateLimitMiddleware)
# Kept here so they can be imported and unit-tested without loading app.py.
# ---------------------------------------------------------------------------

def is_via_cf_tunnel(request: "Any") -> bool:
    """True if the request arrived via Cloudflare Tunnel.

    CF injects cf-ray on every proxied request; cf-connecting-ip and
    cf-access-jwt-assertion are also CF-only headers.  Any one is sufficient.

    The critical use: loopback bypass in AuthMiddleware must check this so that
    browser traffic (127.0.0.1 via tunnel + cf-ray) falls through to JWT
    validation instead of being admitted by the IP bypass.
    """
    h = request.headers
    return (
        "cf-ray" in h
        or "cf-connecting-ip" in h
        or "cf-access-jwt-assertion" in h
    )


def is_localhost(request: "Any") -> bool:
    """True if the request originated from the local machine (not LAN, not tunnel)."""
    client = getattr(request, "client", None)
    host = getattr(client, "host", None) if client else None
    return host in ("127.0.0.1", "::1")
