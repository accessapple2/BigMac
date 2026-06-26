"""
Multi-source earnings-date confidence resolver.

CONFIRMED only when:
  1. A provider explicitly flags the date as confirmed (UW, or future providers), OR
  2. Two or more independent sources agree on the exact date.

Everything else is ESTIMATED.  Fail-safe: ESTIMATED never removes a stop.

Sources (in priority order):
  _uw_earnings     — explicit confirmed flag from UnusualWhales (deferred, spec A)
  _nasdaq_earnings — Nasdaq public earnings calendar API (no key required)
  _finnhub_earnings — Finnhub /calendar/earnings (FINNHUB_API_KEY)
  _yahoo_earnings  — Yahoo Finance calendarEvents (no key required, last resort)
"""
from __future__ import annotations

import logging
import time
import threading
from collections import Counter
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache — avoid hammering external APIs every scan cycle
# ---------------------------------------------------------------------------

_confirm_cache: dict[str, tuple[float, dict | None]] = {}
_confirm_lock  = threading.Lock()
_CACHE_TTL     = 900  # 15 minutes: earnings dates change rarely intraday


def _cached_confirm(symbol: str) -> dict | None | str:
    """Returns cached result (may be None), or sentinel "miss" string."""
    hit = _confirm_cache.get(symbol)
    if hit and (time.time() - hit[0]) < _CACHE_TTL:
        return hit[1]   # may be None (miss that was cached)
    return "miss"


def _set_cache(symbol: str, result: dict | None) -> None:
    _confirm_cache[symbol] = (time.time(), result)


# ---------------------------------------------------------------------------
# Confidence enum
# ---------------------------------------------------------------------------

class Confidence(str, Enum):
    CONFIRMED = "confirmed"
    ESTIMATED = "estimated"


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

# Finnhub "hour" field → canonical session tag
_FH_SESSION_MAP: dict[str, str] = {"bmo": "bmo", "amc": "amc"}


def _normalize_session(hour: str | None) -> str | None:
    """Map a provider's timing string to 'bmo'/'amc' or None if unknown."""
    return _FH_SESSION_MAP.get((hour or "").lower())


def _resolve_session(cands: list[dict], date: str) -> str | None:
    """Pick the consensus BMO/AMC session from candidates that share ``date``."""
    same = [c for c in cands if c["date"] == date and c.get("session")]
    conf = [c for c in same if c.get("confirmed") is True]
    pool = conf or same
    if not pool:
        return None
    return Counter(c["session"].lower() for c in pool).most_common(1)[0][0]


# ---------------------------------------------------------------------------
# Individual source functions
# Each returns {"date": "YYYY-MM-DD", "confirmed": bool|None, "source": str,
#               "session": "bmo"|"amc"|None} or None.
# confirmed=True  — provider explicitly marks the date as confirmed.
# confirmed=None  — provider has a date but no explicit confirm signal.
# confirmed=False — provider explicitly marks the date as estimated.
# ---------------------------------------------------------------------------

def _uw_earnings(symbol: str) -> dict | None:
    """
    Query UW's split session calendars (premarket=BMO, afterhours=AMC).

    Presence on a UW dated calendar is confirmation; session is derived from
    which endpoint the ticker appears on — no string parsing required.

    Both endpoints are cached for _CACHE_TTL in uw_client.get(), so all
    per-symbol lookups within a scan cycle share a single calendar fetch.

    Exact row field names (report_date vs date, ticker vs symbol) are handled
    with fallbacks in uw_client.get_earnings(); smoke will show which applies.
    """
    try:
        from engine import uw_client
        if not uw_client.is_live():
            return None
        return uw_client.get_earnings(symbol)
    except Exception as exc:
        logger.debug("_uw_earnings(%s): %s", symbol, exc)
        return None


def _nasdaq_earnings(symbol: str) -> dict | None:
    """Nasdaq public earnings calendar (no API key required).

    Polls the next 8 calendar days and returns the first matching entry.
    Nasdaq is a genuinely independent source from Yahoo/Finnhub.
    """
    try:
        import requests
        today = datetime.now()
        for delta in range(8):
            ds = (today + timedelta(days=delta)).strftime("%Y-%m-%d")
            try:
                resp = requests.get(
                    "https://api.nasdaq.com/api/calendar/earnings",
                    params={"date": ds},
                    headers={
                        "User-Agent": "Mozilla/5.0 (compatible; earnings-confirm/1.0)",
                        "Accept": "application/json",
                    },
                    timeout=5,
                )
            except Exception:
                continue
            if resp.status_code != 200:
                continue
            try:
                payload = resp.json()
            except Exception:
                continue
            rows = payload.get("data", {}).get("rows") or []
            for row in rows:
                if (row.get("symbol") or "").upper() == symbol.upper():
                    return {
                        "date": ds,
                        "confirmed": None,   # Nasdaq doesn't expose confirmed flag
                        "source": "nasdaq",
                        "session": None,     # Nasdaq public API doesn't expose BMO/AMC
                    }
        return None
    except Exception as exc:
        logger.debug("nasdaq_earnings(%s): %s", symbol, exc)
        return None


def _finnhub_earnings(symbol: str) -> dict | None:
    """Finnhub /calendar/earnings for the symbol over the next 7 days.

    Bypasses the watchlist filter used by get_earnings_calendar() so that
    any held position can be confirmed regardless of the active universe.
    """
    try:
        from engine.finnhub_data import _fh_get
        today = datetime.now().strftime("%Y-%m-%d")
        ahead = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        data = _fh_get("/calendar/earnings", {"from": today, "to": ahead})
        if not data:
            return None
        for ev in data.get("earningsCalendar", []):
            if (ev.get("symbol") or "").upper() == symbol.upper():
                return {
                    "date": ev.get("date", ""),
                    "confirmed": None,
                    "source": "finnhub",
                    "session": _normalize_session(ev.get("hour")),  # "bmo"/"amc"/"dmh"
                }
        return None
    except Exception as exc:
        logger.debug("finnhub_earnings(%s): %s", symbol, exc)
        return None


def _yahoo_earnings(symbol: str) -> dict | None:
    """Yahoo Finance calendarEvents — last-resort baseline source.

    confirmed is intentionally NOT derived from len(dates)==1 here: that
    heuristic is the proxy being replaced by this multi-source module.
    Yahoo's date is treated as unconfirmed (confirmed=None) so that
    Rule 1 (explicit flag) is never triggered by Yahoo alone.
    """
    try:
        from engine.earnings_calendar import fetch_earnings
        events = fetch_earnings([symbol])
        for ev in events:
            if (ev.get("symbol") or "").upper() == symbol.upper():
                return {
                    "date": ev["date"],
                    "confirmed": None,   # len-heuristic deliberately dropped
                    "source": "yahoo",
                    "session": None,     # Yahoo fetch_earnings doesn't expose BMO/AMC
                }
        return None
    except Exception as exc:
        logger.debug("yahoo_earnings(%s): %s", symbol, exc)
        return None


def _sources() -> tuple:
    """Lazily resolved source tuple — evaluated at call time so tests can patch."""
    return (_uw_earnings, _nasdaq_earnings, _finnhub_earnings, _yahoo_earnings)


# ---------------------------------------------------------------------------
# Main resolver
# ---------------------------------------------------------------------------

def confirm_earnings(symbol: str) -> dict | None:
    """
    Resolve earnings-date confidence for ``symbol``.

    Returns:
        {
            "date":       "YYYY-MM-DD",
            "confidence": Confidence.CONFIRMED | Confidence.ESTIMATED,
            "sources":    ["yahoo", "finnhub", ...],
        }
        or None if no source has upcoming earnings.

    Cached for 15 minutes to avoid repeated external calls per scan cycle.
    """
    sym = symbol.upper()

    with _confirm_lock:
        hit = _cached_confirm(sym)
        if hit != "miss":
            return hit  # type: ignore[return-value]

    cands: list[dict] = []
    for src_fn in _sources():
        try:
            d = src_fn(sym)
            if d and d.get("date"):
                cands.append(d)
        except Exception:
            continue

    if not cands:
        _set_cache(sym, None)
        return None

    # Rule 1 — any provider's explicit confirmed flag wins immediately.
    explicit = [c for c in cands if c.get("confirmed") is True]
    if explicit:
        result: dict = {
            "date":       explicit[0]["date"],
            "confidence": Confidence.CONFIRMED,
            "sources":    [c["source"] for c in explicit],
            "session":    _resolve_session(explicit, explicit[0]["date"]),
        }
        _set_cache(sym, result)
        return result

    # Rule 2 — two or more independent sources agree on the exact date.
    date_counts = Counter(c["date"] for c in cands)
    top_date, n = date_counts.most_common(1)[0]
    if n >= 2:
        agreeing = [c for c in cands if c["date"] == top_date]
        result = {
            "date":       top_date,
            "confidence": Confidence.CONFIRMED,
            "sources":    [c["source"] for c in agreeing],
            "session":    _resolve_session(agreeing, top_date),
        }
        _set_cache(sym, result)
        return result

    # Rule 3 — single source or disagreement → ESTIMATED.
    result = {
        "date":       cands[0]["date"],
        "confidence": Confidence.ESTIMATED,
        "sources":    [c["source"] for c in cands],
        "session":    _resolve_session(cands, cands[0]["date"]),
    }
    _set_cache(sym, result)
    return result


# ---------------------------------------------------------------------------
# Thin helper used by earnings_calendar / other callers that need the raw flag
# ---------------------------------------------------------------------------

def get_confirmed_flag(symbol: str) -> bool:
    """Return True if confirm_earnings() resolves CONFIRMED, else False."""
    r = confirm_earnings(symbol)
    return bool(r and r["confidence"] == Confidence.CONFIRMED)
