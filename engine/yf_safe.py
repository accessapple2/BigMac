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
from typing import Any

import pandas as pd

log = logging.getLogger("yf_safe")

_DELISTED_CACHE: set[str] = set()


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
