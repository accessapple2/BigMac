# === HM-BL ===
"""HM-BL: In-process memoization for yfinance "delisted" responses.

Background
----------
When `yf.Ticker(sym).history(period=...)` is called for a delisted symbol,
yfinance prints two stderr-style warning lines per call (its internal
fallback retries with a shorter period to confirm the ticker is dead).

OllieTrades has at least one persistent stale ticker (`ATH` — Athene
Holding, delisted Jan 2022) sitting in the internal `positions` table.
Every Kirk advisory cycle / crew scanner pass that pulls per-position
IV history emits the warnings again → 174 hits in trader_error.log today.

Fix
---
`yf_history_safe(symbol, period, interval, ...)` wraps `Ticker.history`
with an in-process set of known-delisted symbols. First miss caches the
symbol and logs one WARNING line; subsequent hits return an empty
DataFrame without touching the network or producing further yfinance
warnings.

Cache is per-process, cleared on restart — so a ticker that gets
re-listed will re-try naturally on the next service start.

This is the structural fix. Adoption at other call sites is natural-
maintenance scope; the dedicated module exists to provide one obvious
landing pad.

See `data/scotty_hm_bkbl_report.md` for the discovery trail.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, TypeVar

import pandas as pd

log = logging.getLogger("yf_safe")

_DELISTED_CACHE: set[str] = set()

# ─────────────────────────────────────────────────────────────────────────
# HM-YF-429-GUARD (2026-08-28): generic pacing/backoff/abort wrapper for
# yfinance calls, added alongside (not replacing) the HM-BL delisted-cache
# logic above. No yfinance 429s were found live in trader_error.log at
# implementation time -- this is precautionary hardening for the ~70
# call sites across the repo that hit yfinance directly with no pacing,
# not a reactive fix for an observed storm (the ntfy 429 storm the 429-
# remediation directive named was traced separately to
# engine/long_range_sensors.py::send_ntfy(), unrelated to yfinance).
# ─────────────────────────────────────────────────────────────────────────

T = TypeVar("T")

MIN_CALL_GAP = 2.0          # seconds between any two yfinance calls via this wrapper
BACKOFF_SCHEDULE = (2.0, 4.0, 8.0)  # retry delays on a rate-limit-shaped error
ABORT_AFTER_CONSECUTIVE = 5  # give up on the whole sweep after this many in a row

_last_call_ts = 0.0
_consecutive_failures = 0


class YFSweepAbort(Exception):
    """Raised when ABORT_AFTER_CONSECUTIVE rate-limit errors happen back to
    back -- signals the caller's loop to stop trying more symbols this
    cycle rather than grinding through the rest doomed to fail too."""


def _looks_rate_limited(exc: Exception) -> bool:
    msg = str(exc).lower()
    if "too many requests" in msg or "429" in msg:
        return True
    return type(exc).__name__ in ("YFRateLimitError",)


def reset_sweep() -> None:
    """Call at the start of a sweep/scan cycle to clear the consecutive-
    failure counter from any prior cycle."""
    global _consecutive_failures
    _consecutive_failures = 0


def yf_call_safe(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Call a yfinance-touching function with pacing + backoff + abort.

    Enforces >=MIN_CALL_GAP seconds since the last call made through this
    wrapper (any symbol), retries on a rate-limit-shaped exception per
    BACKOFF_SCHEDULE, and raises YFSweepAbort once ABORT_AFTER_CONSECUTIVE
    rate-limit errors happen in a row (reset per cycle via reset_sweep()).
    Non-rate-limit exceptions propagate immediately, unretried -- this
    wrapper is only about Yahoo's rate limiting, not a general retry-
    everything shim.
    """
    global _last_call_ts, _consecutive_failures

    if _consecutive_failures >= ABORT_AFTER_CONSECUTIVE:
        raise YFSweepAbort(
            f"{_consecutive_failures} consecutive yfinance rate-limit errors "
            f"this sweep — aborting remaining calls"
        )

    wait = MIN_CALL_GAP - (time.time() - _last_call_ts)
    if wait > 0:
        time.sleep(wait)

    for attempt, delay in enumerate((0.0, *BACKOFF_SCHEDULE)):
        if delay:
            log.warning("yfinance rate-limited (attempt %d), backing off %.0fs",
                        attempt, delay)
            time.sleep(delay)
        try:
            result = fn(*args, **kwargs)
            _last_call_ts = time.time()
            _consecutive_failures = 0
            return result
        except Exception as e:
            _last_call_ts = time.time()
            if not _looks_rate_limited(e):
                raise
            if attempt == len(BACKOFF_SCHEDULE):
                _consecutive_failures += 1
                if _consecutive_failures >= ABORT_AFTER_CONSECUTIVE:
                    raise YFSweepAbort(
                        f"{_consecutive_failures} consecutive yfinance "
                        f"rate-limit errors this sweep — aborting"
                    ) from e
                raise
    raise AssertionError("unreachable")  # pragma: no cover


def yf_history_safe(
    symbol: str,
    period: str = "1y",
    interval: str = "1d",
    **kwargs: Any,
) -> pd.DataFrame:
    """yfinance Ticker.history wrapper with delisted-symbol memoization.

    Returns the live DataFrame from yfinance on success. On first
    encounter with a missing-data symbol, logs one WARNING line, adds
    the symbol to the in-process cache, and returns an empty DataFrame.
    Subsequent calls for the same symbol short-circuit on the cache
    without invoking yfinance.
    """
    sym = (symbol or "").upper()
    if not sym:
        return pd.DataFrame()
    if sym in _DELISTED_CACHE:
        return pd.DataFrame()
    try:
        import yfinance as yf
    except ImportError:
        log.warning("HM-BL: yfinance not importable; returning empty for %s", sym)
        return pd.DataFrame()
    df = yf.Ticker(symbol).history(period=period, interval=interval, **kwargs)
    if df is None or df.empty:
        log.warning(
            "HM-BL: %s returned no data from yfinance (period=%s) — "
            "memoizing as delisted for this session",
            sym,
            period,
        )
        _DELISTED_CACHE.add(sym)
        return pd.DataFrame()
    return df


def is_delisted(symbol: str) -> bool:
    """Test helper / introspection — returns True if the symbol is in the cache."""
    return (symbol or "").upper() in _DELISTED_CACHE


def clear_cache() -> None:
    """Test helper — flush the memoization cache. NOT for production use."""
    _DELISTED_CACHE.clear()
# === /HM-BL ===
