"""
Detail panel data assembler.

One call per click. Returns multi-timeframe Alpaca bars, fundamentals,
per-ticker fleet signals (last 24h), and a flags placeholder for a
single ticker. Cached for ~30 seconds to absorb double-clicks.

Phase 4 — Admiral Path A:
- flags is a placeholder ([]) — Phase 3 (engine/momentum/flags.py) not yet shipped
- fundamentals parsed from stock_fundamentals.data JSON blob
- next_earnings is inside that blob, so no separate calendar table query
- signals filtered by symbol column (no symbol index, but per-click is fine)
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DB = Path.home() / "autonomous-trader" / "data" / "trader.db"

# Time-bucketed in-memory cache: {(ticker, bucket_30s): payload}
_DETAIL_CACHE: dict[tuple[str, int], dict[str, Any]] = {}
_CACHE_TTL_S = 30

# Which fundamentals fields to surface in the detail payload (subset of the
# 56-key blob — we keep numeric+display-relevant fields, omit raw counts
# like institutions_count).
_FUND_FIELDS = (
    "company_name", "sector", "industry",
    "market_cap", "current_price",
    "pe_trailing", "pe_forward", "peg_ratio", "price_to_book",
    "eps_trailing", "eps_forward",
    "revenue_growth", "earnings_growth",
    "gross_margin", "operating_margin", "profit_margin",
    "roe", "roa", "debt_to_equity",
    "free_cash_flow", "total_revenue",
    "target_mean", "target_high", "target_low",
    "analyst_upside", "recommendation", "num_analysts",
    "next_earnings", "days_to_earnings",
    "beta", "week52_high", "week52_low", "week52_pct",
    "dividend_yield",
    "smart_score", "grade",
)


def compute_detail(ticker: str) -> dict[str, Any]:
    """Return the full detail payload for a ticker; cached 30s."""
    ticker = ticker.upper().strip()
    bucket = int(time.time() // _CACHE_TTL_S)
    cache_key = (ticker, bucket)
    if cache_key in _DETAIL_CACHE:
        return _DETAIL_CACHE[cache_key]

    payload = {
        "ticker": ticker,
        "ts": datetime.utcnow().isoformat() + "Z",
        "bars": _bars_multi_timeframe(ticker),
        "fundamentals": _fundamentals(ticker),
        "signals": _recent_signals(ticker, since_hours=24, limit=20),
        "flags": [],  # placeholder — Phase 3 retrofit
    }
    _DETAIL_CACHE[cache_key] = payload
    # Opportunistic prune of old buckets (keep last 2 windows).
    if len(_DETAIL_CACHE) > 200:
        keep_after = bucket - 2
        for k in list(_DETAIL_CACHE.keys()):
            if k[1] < keep_after:
                _DETAIL_CACHE.pop(k, None)
    return payload


def _bars_multi_timeframe(ticker: str) -> dict[str, list[dict[str, Any]]]:
    """Return three timeframes via Alpaca: 5m intraday, 1h multi-day, 1d multi-month."""
    out: dict[str, list[dict[str, Any]]] = {"5m": [], "1h": [], "1d": []}
    try:
        from engine.market_data import get_alpaca_bars
    except ImportError as e:
        logger.warning("get_alpaca_bars import failed: %s", e)
        return out

    # (timeframe alpaca-string, days lookback, target key)
    requests = [
        ("5Min", 3, "5m"),     # ~3 trading days of 5-min bars
        ("1Hour", 14, "1h"),    # ~2 weeks of 1-hour bars
        ("1Day", 90, "1d"),     # ~3 months of daily bars
    ]
    for alpaca_tf, days, key in requests:
        try:
            df = get_alpaca_bars(ticker, timeframe=alpaca_tf, days=days)
            if df is None or len(df) == 0:
                continue
            out[key] = _df_to_bar_dicts(df)
        except Exception as e:
            logger.warning("bars fetch failed (%s, %s): %s", ticker, alpaca_tf, e)
    return out


def _df_to_bar_dicts(df) -> list[dict[str, Any]]:
    """Convert an OHLCV DataFrame (DatetimeIndex) to a list of JSON-safe dicts."""
    rows: list[dict[str, Any]] = []
    try:
        for ts, r in df.iterrows():
            rows.append({
                "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                "open": float(r["Open"]),
                "high": float(r["High"]),
                "low": float(r["Low"]),
                "close": float(r["Close"]),
                "volume": int(r["Volume"]),
            })
    except (KeyError, TypeError, ValueError) as e:
        logger.warning("bar dict conversion failed: %s", e)
    return rows


def _fundamentals(ticker: str) -> dict[str, Any]:
    """Read + parse stock_fundamentals.data JSON blob for `ticker`.

    Returns a flat dict of the fields listed in _FUND_FIELDS plus
    `score_components` (the per-pillar smart-score breakdown). Empty
    dict if the row is missing or JSON parse fails.
    """
    if not DB.exists():
        return {}
    out: dict[str, Any] = {}
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT data, smart_score, grade, updated_at "
            "FROM stock_fundamentals WHERE symbol = ? LIMIT 1",
            (ticker,),
        ).fetchone()
    except sqlite3.OperationalError as e:
        logger.warning("stock_fundamentals read failed: %s", e)
        conn.close()
        return {}
    conn.close()
    if not row:
        return {}
    # Column-level fallback fields.
    out["smart_score"] = row["smart_score"]
    out["grade"] = row["grade"]
    out["updated_at"] = row["updated_at"]
    # JSON blob fields override / enrich.
    try:
        blob = json.loads(row["data"] or "{}")
    except (json.JSONDecodeError, TypeError):
        return out
    for k in _FUND_FIELDS:
        if k in blob:
            out[k] = blob[k]
    if "score_components" in blob:
        out["score_components"] = blob["score_components"]
    return out


def _recent_signals(ticker: str, since_hours: int = 24, limit: int = 20) -> list[dict[str, Any]]:
    """Return recent non-legacy signals on `ticker` (newest first)."""
    if not DB.exists():
        return []
    cutoff = (datetime.utcnow() - timedelta(hours=since_hours)).isoformat()
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT created_at AS ts, player_id, confidence, signal, reasoning
            FROM signals
            WHERE symbol = ? AND created_at >= ?
              AND (reasoning IS NULL OR reasoning NOT LIKE '%[LEGACY_BIMODAL%')
            ORDER BY id DESC
            LIMIT ?
            """,
            (ticker, cutoff, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError as e:
        logger.warning("signals read failed: %s", e)
        return []
    finally:
        conn.close()
