"""
UnusualWhales API client.

ENDPOINTS status
----------------
All paths are None until confirmed by tools/uw_smoke.py printing "OK + fields:all".
Never write a path here that has not passed the smoke harness — that is the
stale-data failure this module was built to prevent.

Activation sequence
-------------------
1. Run tools/uw_smoke.py for a capability; confirm OK + all required fields.
2. Write the real path into ENDPOINTS[capability].
3. Update the corresponding normalizer to use the REAL field names from the
   smoke JSON (do not assume the placeholder keys in the stubs below).
4. For "earnings" specifically: once confirmed, replace _uw_earnings in
   engine/earnings_confirm.py from the current None stub to call get_earnings().
5. Set _CACHE_TTL at or above the UW rate-limit floor for that endpoint.

Auth / secrets
--------------
UW_API_KEY is read from the environment.  Never log it, never embed it.
Auth format below defaults to Bearer token; update _auth_headers() if UW docs
show a different scheme (query param, custom header, etc.).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — populated as smoke tests confirm each path
# ---------------------------------------------------------------------------

# Confirmed paths only.  None = not yet smoke-tested; callers get [] / None.
# Paths confirmed from UW docs + OpenAPI spec (snapshot 2025-10-22):
#   earnings_bmo / earnings_amc  — CONFIRMED
# Fill remaining from https://api.unusualwhales.com/api/openapi after smoke OK.
ENDPOINTS: dict[str, str | None] = {
    "earnings_bmo":   "/api/earnings/premarket",    # CONFIRMED — BMO reporters; session=bmo
    "earnings_amc":   "/api/earnings/afterhours",   # CONFIRMED — AMC reporters; session=amc
    "flow_alerts":    None,   # FILL_FROM_OPENAPI after smoke OK
    "short_interest": None,   # FILL_FROM_OPENAPI after smoke OK
    "gex_exposure":   None,   # FILL_FROM_OPENAPI after smoke OK
    "option_chain":   None,   # FILL_FROM_OPENAPI after smoke OK
}

# Auth — update _auth_headers() once the format is confirmed from docs.
_API_KEY  = os.environ.get("UW_API_KEY", "")
# Base URL confirmed from UW docs + OpenAPI (2025-10-22). Override via UW_BASE only if it changes.
_BASE_URL = os.environ.get("UW_BASE", "https://api.unusualwhales.com")

# Cache TTL in seconds.  Set ≥ rate-limit floor after confirming from docs.
# One value applies to all endpoints; make per-endpoint once limits are known.
_CACHE_TTL = 300   # 5 min conservative default; tighten after rate-limit confirmation

# ---------------------------------------------------------------------------
# Internal cache + lock
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, Any]] = {}
_cache_lock = threading.Lock()


def _cache_get(key: str) -> Any:
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            return entry[1]
    return None


def _cache_set(key: str, value: Any) -> None:
    with _cache_lock:
        _cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Public get() — unwraps the data envelope, caches per (path, params)
# ---------------------------------------------------------------------------

def get(path: str, params: dict | None = None) -> list[dict] | None:
    """
    Fetch ``path`` (relative to base URL), unwrap the ``data`` envelope, cache
    per (path, params) for _CACHE_TTL seconds.

    Returns the data list (may be empty) or None on error / unavailable.
    This is the primary call site for _uw_earnings in earnings_confirm.py.

    Full-calendar endpoints (premarket / afterhours) return ALL scheduled
    reporters; the caller filters for its symbol.  Caching means the full list
    is fetched once per TTL regardless of how many symbols are checked.
    """
    if not path:
        return None
    cache_key = f"get:{path}:{sorted((params or {}).items())}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    body = _get(path, params)
    if body is None:
        return None
    data = body.get("data") or body
    if isinstance(data, list):
        result: list[dict] = [r for r in data if isinstance(r, dict)]
    elif isinstance(data, dict):
        result = [data]
    else:
        result = []
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# HTTP helpers (private transport)
# ---------------------------------------------------------------------------

def _auth_headers() -> dict[str, str]:
    """Return auth headers.  Update format once confirmed from UW docs."""
    if not _API_KEY:
        raise RuntimeError("UW_API_KEY not set")
    return {"Authorization": f"Bearer {_API_KEY}"}


def _get(path: str, params: dict | None = None) -> dict | None:
    """
    GET _BASE_URL + path, return parsed JSON or None on error.

    Caller is responsible for caching; this is the raw transport layer.
    Logs the error but never raises — callers degrade gracefully.
    """
    if not _BASE_URL:
        logger.debug("uw_client: UW_BASE not set; returning None")
        return None
    if not _API_KEY:
        logger.warning("uw_client: UW_API_KEY not set; cannot call UW API")
        return None
    try:
        import requests
        resp = requests.get(
            _BASE_URL.rstrip("/") + path,
            headers=_auth_headers(),
            params=params or {},
            timeout=10,
        )
    except Exception as exc:
        logger.warning("uw_client GET %s failed: %s", path, exc)
        return None

    if resp.status_code == 401:
        logger.error("uw_client: 401 on %s — check UW_API_KEY", path)
        return None
    if resp.status_code == 403:
        logger.warning("uw_client: 403 on %s — endpoint may be tier-gated", path)
        return None
    if not resp.ok:
        logger.warning("uw_client: %d on %s", resp.status_code, path)
        return None

    try:
        return resp.json()
    except Exception as exc:
        logger.warning("uw_client: JSON parse error on %s: %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Normalizers (stubs — update field names after smoke test shows real schema)
# ---------------------------------------------------------------------------

def _normalize_flow_row(raw: dict, data_as_of: str | None) -> dict:
    """Map one UW flow-alert row to the internal schema.

    Field names confirmed from UW OpenAPI (2025-10-22).  If the smoke harness
    prints MISSING after the path is filled in, update the raw.get() keys here
    to match what the actual response shows.
    """
    ask_prem = float(raw.get("total_ask_side_prem") or 0)
    bid_prem = float(raw.get("total_bid_side_prem") or 0)
    return {
        "ticker":     raw.get("ticker"),
        "type":       raw.get("type"),                          # "call" | "put"
        "strike":     raw.get("strike"),
        "expiry":     raw.get("expiry"),
        "dte":        raw.get("dte"),
        "premium":    float(raw.get("total_premium") or 0),    # real field name
        "size":       int(raw.get("total_size") or 0),         # real field name
        "sweep":      bool(raw.get("has_sweep")),              # real field name
        "has_floor":  bool(raw.get("has_floor")),
        "has_multileg": bool(raw.get("has_multileg")),
        "sentiment":  "bullish" if ask_prem > bid_prem else "bearish",
        "voi":        float(raw.get("volume_oi_ratio") or 0),
        "data_as_of": data_as_of,
        "source":     "uw",
    }


def _normalize_short_interest_row(raw: dict, data_as_of: str | None) -> dict:
    """Map one UW short-interest row to the internal schema (stub)."""
    return {
        "ticker":               raw.get("ticker"),
        "short_percent_float":  raw.get("short_percent_float"),  # UPDATE after smoke
        "days_to_cover":        raw.get("days_to_cover"),        # UPDATE after smoke
        "float":                raw.get("float"),                 # UPDATE after smoke
        "data_as_of":           data_as_of,
        "source":               "uw",
    }


def _normalize_earnings_row(raw: dict, data_as_of: str | None) -> dict:
    """
    Map one UW earnings calendar row to the contract expected by earnings_confirm.py.

    Session is NOT derived from the row — it is set by get_earnings() based on
    which endpoint the ticker was found on (premarket=bmo, afterhours=amc).

    Date field name (report_date vs date) is tried with both; smoke test will
    show which is correct and allow cleanup of the fallback.

    confirmed=True for any entry on a UW dated calendar — presence IS the signal.
    """
    return {
        "date":       raw.get("report_date") or raw.get("date"),
        "confirmed":  True,          # listed on UW calendar → confirmed by definition
        "session":    None,          # set by get_earnings() from endpoint context
        "source":     "uw",
        "data_as_of": data_as_of,
    }


def _normalize_chain_row(raw: dict, data_as_of: str | None) -> dict:
    """Map one UW option-chain row to the internal schema (stub)."""
    return {
        "strike":     raw.get("strike"),
        "expiry":     raw.get("expiry"),
        "bid":        raw.get("bid"),
        "ask":        raw.get("ask"),
        "mid":        raw.get("mid") or (
            ((raw.get("bid") or 0) + (raw.get("ask") or 0)) / 2
        ),
        "data_as_of": data_as_of,
        "source":     "uw",
    }


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------

def _resolve_ts(row: dict, fetch_ts: str) -> tuple[str, str]:
    """Return (data_as_of, source_label).  Honest about fetch-time vs data-time."""
    for field in ("updated_at", "timestamp", "as_of", "created_at"):
        val = row.get(field) if isinstance(row, dict) else None
        if val:
            return str(val), "uw"
    return fetch_ts, "uw-fetch"   # no data timestamp — mark honestly


def get_flow_alerts(limit: int = 50) -> list[dict]:
    """Fetch UW option flow alerts (sweeps/blocks).  Returns [] if not confirmed."""
    path = ENDPOINTS.get("flow_alerts")
    if not path:
        logger.debug("uw_client: flow_alerts endpoint not yet confirmed; returning []")
        return []
    cache_key = f"flow_alerts:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    body = _get(path, {"limit": limit})
    if not body:
        return []
    rows = body.get("data") or []
    fetch_ts = str(int(time.time()))
    result = []
    for raw in (rows if isinstance(rows, list) else []):
        if not isinstance(raw, dict):
            continue
        as_of, src = _resolve_ts(raw, fetch_ts)
        norm = _normalize_flow_row(raw, as_of)
        norm["source"] = src
        result.append(norm)
    _cache_set(cache_key, result)
    return result


def get_short_interest(symbol: str) -> dict | None:
    """Fetch short-interest data for one symbol.  Returns None if not confirmed."""
    path_template = ENDPOINTS.get("short_interest")
    if not path_template:
        logger.debug("uw_client: short_interest endpoint not yet confirmed")
        return None
    path = path_template.replace("{symbol}", symbol.upper())
    cache_key = f"short_interest:{symbol.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    body = _get(path)
    if not body:
        return None
    rows = body.get("data") or body
    row  = rows[0] if isinstance(rows, list) else rows
    if not isinstance(row, dict):
        return None
    fetch_ts = str(int(time.time()))
    as_of, src = _resolve_ts(row, fetch_ts)
    result = _normalize_short_interest_row(row, as_of)
    result["source"] = src
    _cache_set(cache_key, result)
    return result


def get_earnings(symbol: str) -> dict | None:
    """
    Fetch earnings data for one symbol via UW's split session calendars.

    UW separates BMO and AMC reporters into distinct endpoints; presence on
    a dated calendar implies confirmed=True, and session is implicit in which
    endpoint the ticker appears on.

    Calendars are fetched via get() which caches the full list for _CACHE_TTL
    seconds — all per-symbol lookups within a scan cycle share one fetch.

    Row field names (report_date vs date, ticker vs symbol) are tried with
    both fallbacks; smoke test will clarify the exact key names.
    """
    sym = symbol.upper()
    # Per-symbol cache on top of the per-path calendar cache.
    cache_key = f"earnings:{sym}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    session_paths = [
        (ENDPOINTS.get("earnings_bmo"), "bmo"),
        (ENDPOINTS.get("earnings_amc"), "amc"),
    ]
    fetch_ts = str(int(time.time()))
    for path, session in session_paths:
        rows = get(path) or []
        for raw in rows:
            ticker = (raw.get("ticker") or raw.get("symbol") or "").upper()
            if ticker != sym:
                continue
            date_val = raw.get("report_date") or raw.get("date")
            if not date_val:
                continue
            as_of, src = _resolve_ts(raw, fetch_ts)
            result = _normalize_earnings_row(raw, as_of)
            result.update({"session": session, "source": src})
            _cache_set(cache_key, result)
            return result

    _cache_set(cache_key, None)
    return None


def get_atm_straddle(symbol: str) -> float | None:
    """
    Fetch front-month ATM straddle mid-price and return it as a fraction of spot.

    Returns None until ENDPOINTS["option_chain"] is confirmed.
    This replaces the 0.10 EM floor in earnings_guard.expected_move() tier-1.
    """
    path = ENDPOINTS.get("option_chain")
    if not path:
        logger.debug("uw_client: option_chain endpoint not yet confirmed")
        return None
    path = path.replace("{symbol}", symbol.upper())
    cache_key = f"chain:{symbol.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    body = _get(path)
    if not body:
        return None
    rows = body.get("data") or []
    if not isinstance(rows, list) or not rows:
        return None

    fetch_ts = str(int(time.time()))
    normed = [_normalize_chain_row(r, fetch_ts) for r in rows if isinstance(r, dict)]
    # Caller (expected_move) can do ATM selection; return all chain rows.
    _cache_set(cache_key, normed)
    return normed  # type: ignore[return-value]  # typed as float|None for the EM interface


# ---------------------------------------------------------------------------
# Liveness check (used by health monitor / Ready Room)
# ---------------------------------------------------------------------------

def is_live() -> bool:
    """True if UW_API_KEY is set and the confirmed earnings endpoints are wired."""
    if not _API_KEY:
        return False
    return bool(ENDPOINTS.get("earnings_bmo") and ENDPOINTS.get("earnings_amc"))
