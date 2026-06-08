"""HM-FIX-ARCHER-TICKER-NAMES (FIX-4) — verified ticker→company-name resolver.

Grounds Archer's LLM briefings in REAL company names so plutus-v1 / Sonnet
stop inventing plausible-but-wrong names from ticker strings ("FireEye Inc.
(FPS)", "Alithium Energy Corp. (ALHC)"). Names come from Polygon's
`/v3/reference/tickers/{TICKER}` reference endpoint (`results.name`) — the same
endpoint + key the /api/logo branding cache already uses (dashboard/app.py).

Caching mirrors that branding cache — 30-day TTL + negative-caching — but in a
SIBLING file (data/ticker_name_cache.json), not the shared logo meta: the logo
endpoint does full-entry replaces (`meta[sym] = {...}`), which would clobber a
name field. Separate file = no clobber, and engine/ stays decoupled from the
web layer. Cache-first by construction so we never add an un-cached Polygon
call path (Starter rate limits).

Degraded state is the BARE TICKER. `get_company_name` returns None when Polygon
has no name (delisted/unknown) or creds/network are unavailable — callers MUST
fall back to the ticker, never a placeholder, never an inferred name.

ADVISORY/DISPLAY ONLY. No decision logic keys on the name (Archer keys on
ticker — FIX-4 Phase C); this only grounds narration text.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_CACHE = _ROOT / "data" / "ticker_name_cache.json"
_TTL = 30 * 86400  # 30-day TTL — mirrors the /api/logo branding cache
_POLY = "https://api.polygon.io/v3/reference/tickers/{sym}"


def _load() -> dict:
    try:
        return json.loads(_CACHE.read_text()) if _CACHE.exists() else {}
    except Exception:
        return {}


def _save(meta: dict) -> None:
    try:
        _CACHE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE.write_text(json.dumps(meta, indent=0))
    except Exception as e:
        logger.warning("[ticker_names] cache write failed: %s: %r", type(e).__name__, e)


def _fetch_polygon_name(sym: str) -> tuple[str | None, bool]:
    """Resolve one ticker via Polygon reference.

    Returns (name_or_None, cacheable). cacheable is False on transient failure
    (missing key / network error / 429 / 5xx) so a blip is never frozen for 30
    days; True on a definitive answer (200 with/without a name, or 404 unknown).
    """
    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        return None, False
    try:
        r = requests.get(_POLY.format(sym=sym), params={"apikey": api_key}, timeout=8)
    except Exception as e:
        logger.warning("[ticker_names] polygon ref %s: %s: %r", sym, type(e).__name__, e)
        return None, False
    if r.status_code == 404:
        return None, True  # unknown/delisted — legitimately no name, cache it
    if r.status_code != 200:
        return None, False  # 429 / 5xx — transient, don't cache
    try:
        name = ((r.json() or {}).get("results") or {}).get("name") or None
    except Exception:
        return None, False
    return name, True


def annotate(items: list[dict], key: str = "symbol") -> list[dict]:
    """Return copies of `items` each with a verified `name` (None if unresolved).

    Distinct tickers are resolved once per call; one cache load + at most one
    save. Never raises — on any failure the item still gets name=None and the
    caller degrades to the bare ticker.
    """
    if not items:
        return items
    meta = _load()
    resolved: dict[str, str | None] = {}
    dirty = False
    for it in items:
        u = str(it.get(key) or "").upper().strip()
        if not u or u in resolved:
            continue
        entry = meta.get(u)
        if entry and (time.time() - float(entry.get("fetched_at", 0))) < _TTL:
            resolved[u] = entry.get("name") or None
            continue
        name, cacheable = _fetch_polygon_name(u)
        resolved[u] = name
        if cacheable:
            meta[u] = {"name": name, "fetched_at": time.time()}
            dirty = True
    if dirty:
        _save(meta)
    return [
        {**it, "name": resolved.get(str(it.get(key) or "").upper().strip())}
        for it in items
    ]


def get_company_name(symbol: str) -> str | None:
    """Verified company name for one ticker, or None → caller shows bare ticker."""
    if not symbol:
        return None
    return annotate([{"symbol": symbol}])[0].get("name")
