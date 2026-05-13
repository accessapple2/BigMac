# === HM-BL-broader wrapper ===
"""HM-BL-broader: Wrapper for `yfinance.download` with consistent empty-result logging.

Companion to `engine/yf_safe.py` (which wraps `yf.Ticker.history`). This module
wraps the bulk-download path used by backtests + multi-ticker live agents
(holodeck_expansion, premium_etfs, inverse_etfs, crew tools, weekend_backtest,
etc.).

Wrapping rationale
------------------
- Centralizes empty-result handling so every call site gets the same loud-fail
  log line instead of silent `DataFrame.empty == True` propagation that later
  surfaces as KeyError downstream.
- Centralizes yfinance import handling so a missing/broken yfinance install
  doesn't take down the live agent (returns empty DF, logs once).
- Single landing pad for future improvements: retry with backoff, Polygon
  fallback, caching, rate-limit accounting.

Single-ticker memoization (yf_history_safe parity): when a single-ticker
download returns empty, the ticker is cached as delisted/dead for the
process lifetime — subsequent calls short-circuit. Multi-ticker downloads
are not memoized (the cache key shape gets unwieldy and the access patterns
typically rotate the universe).

See `data/scotty_hm_bkbl_report.md` for the discovery trail.
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

log = logging.getLogger("yf_download_safe")

# Per-process single-ticker cache; parallel to yf_safe._DELISTED_CACHE.
_DELISTED_CACHE: set[str] = set()


def yf_download_safe(
    tickers: Any,
    **kwargs: Any,
) -> pd.DataFrame:
    """yfinance.download wrapper with consistent empty-result logging.

    Args:
        tickers: str or list[str] — same shape as yf.download accepts.
        **kwargs: forwarded verbatim to yf.download (start, end, period,
                  interval, group_by, auto_adjust, threads, progress, ...).

    Returns:
        DataFrame from yf.download on success, or empty DataFrame on:
          - yfinance ImportError
          - yf.download exception
          - single-ticker known-delisted (cache hit)
          - empty result (multi-ticker: cache miss; single-ticker: cache add)

    Never raises — guarantees the calling backtest/agent path can continue.
    """
    # Normalize ticker representation for cache lookup.
    if isinstance(tickers, str):
        sym_for_cache = tickers.upper().strip()
        is_single = True
    elif isinstance(tickers, (list, tuple)):
        sym_for_cache = None
        is_single = len(tickers) == 1
        if is_single:
            sym_for_cache = str(tickers[0]).upper().strip()
    else:
        sym_for_cache = None
        is_single = False

    # Short-circuit single-ticker known-delisted.
    if is_single and sym_for_cache and sym_for_cache in _DELISTED_CACHE:
        return pd.DataFrame()

    try:
        import yfinance as yf
    except ImportError:
        log.warning("HM-BL-broader: yfinance not importable; returning empty for %r", tickers)
        return pd.DataFrame()

    try:
        df = yf.download(tickers, **kwargs)
    except Exception as e:
        log.warning(
            "HM-BL-broader: yf.download raised for %r: %s: %s — returning empty",
            tickers, type(e).__name__, e,
        )
        return pd.DataFrame()

    if df is None or df.empty:
        if is_single and sym_for_cache:
            log.warning(
                "HM-BL-broader: %s returned no data from yf.download (kwargs=%s) — "
                "memoizing as delisted for this session",
                sym_for_cache, kwargs,
            )
            _DELISTED_CACHE.add(sym_for_cache)
        else:
            log.warning(
                "HM-BL-broader: yf.download returned empty for %r (kwargs=%s)",
                tickers, kwargs,
            )
        return pd.DataFrame()
    return df


def is_delisted(symbol: str) -> bool:
    """Test helper / introspection."""
    return (symbol or "").upper().strip() in _DELISTED_CACHE


def clear_cache() -> None:
    """Test helper — flush memoization. NOT for production use."""
    _DELISTED_CACHE.clear()
# === /HM-BL-broader wrapper ===
