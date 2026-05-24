"""HM-MINERVINI-TREND-FILTER — daily Stage-2 uptrend evaluation per Mark
Minervini's 8-condition Trend Template.

For each symbol in ``scan_universe`` with ≥ 252 trading days of history,
evaluate the canonical 8 conditions on the latest daily bar and persist
the pass/fail state. RS-rank ≥ 70 is tracked as a separate 9th boolean
(``rs_pass``) so consumers can gate either way.

The 8 conditions:
    1. price > sma150
    2. price > sma200
    3. sma150 > sma200
    4. sma200 trending up: sma200[today] > sma200[22 bars ago]
    5. price > sma50
    6. sma50 > sma150 AND sma50 > sma200
    7. within 25% of 52-week high
    8. ≥ 30% above 52-week low

``template_pass`` is 1 only when all 8 are true (strict 8/8 — Captain spec
2026-05-24). ``template_score`` is the 0–8 count.

Ship state: gated by ``MINERVINI_FILTER_ENABLED`` env flag (default OFF).
Nightly cadence at 20:45 AZ post-close, 15 min after the RS-rank job at
20:30 so the ``rs_pass`` LEFT JOIN sees fresh data.

Surfacing:
    - GET /api/minervini?passing_only=1&top=200
    - GET /api/minervini/{symbol}
    - section-fundamentals card row + per-cond tooltip
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from rich.console import Console

console = Console()

_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

# ─── Scanner constants ────────────────────────────────────────────────────
_SMA50_PERIOD: int = 50
_SMA150_PERIOD: int = 150
_SMA200_PERIOD: int = 200
_SMA200_TREND_LOOKBACK: int = 22       # ~1 trading month
_HIGH_52W_WINDOW: int = 252            # 1 trading year
_WITHIN_PCT_OF_HIGH: float = 0.25      # within 25% of 52-week high
_ABOVE_PCT_OF_LOW: float = 0.30        # ≥ 30% above 52-week low
_MIN_BARS_REQUIRED: int = 252          # full year needed for 52w high/low
_RS_PASS_THRESHOLD: int = 70           # rs_rank ≥ 70 (IBD-style)
_CACHE_TTL: int = 43200                # 12h in-memory cache

_scan_lock = threading.Lock()
_last_result: dict | None = None
_last_scan_ts: float = 0.0


# ─── Indicator helper ────────────────────────────────────────────────────


def _sma(arr: np.ndarray, n: int) -> np.ndarray:
    """Simple moving average. Returns array same length as ``arr`` with the
    first ``n-1`` entries set to NaN."""
    if len(arr) < n:
        return np.full(len(arr), np.nan)
    out = np.full(len(arr), np.nan)
    cumsum = np.cumsum(arr, dtype=float)
    out[n - 1] = cumsum[n - 1] / n
    out[n:] = (cumsum[n:] - cumsum[:-n]) / n
    return out


# ─── Trend Template evaluation ───────────────────────────────────────────


def _evaluate_conditions(df: pd.DataFrame) -> dict:
    """Run the 8-cond Trend Template against the most-recent bar.

    Returns:
        {
          'cond1' .. 'cond8': bool,
          'template_score': int (0-8),
          'template_pass':  bool,
          'price':          float,
          'high_52w':       float | None,
          'low_52w':        float | None,
          'bars_used':      int,
        }

    For insufficient history (< _MIN_BARS_REQUIRED): all conds False,
    score=0, pass=False, bars_used=0.
    """
    empty = {
        f"cond{i}": False for i in range(1, 9)
    } | {
        "template_score": 0,
        "template_pass": False,
        "price": float("nan"),
        "high_52w": None,
        "low_52w": None,
        "bars_used": 0,
    }
    if df is None or df.empty:
        return empty
    closes = df["Close"].to_numpy(dtype=float)
    highs = df["High"].to_numpy(dtype=float)
    lows = df["Low"].to_numpy(dtype=float)
    n = len(closes)
    if n < _MIN_BARS_REQUIRED:
        return empty

    price = float(closes[-1])
    sma50 = _sma(closes, _SMA50_PERIOD)
    sma150 = _sma(closes, _SMA150_PERIOD)
    sma200 = _sma(closes, _SMA200_PERIOD)

    s50_now = float(sma50[-1])
    s150_now = float(sma150[-1])
    s200_now = float(sma200[-1])
    s200_prior = float(sma200[-(_SMA200_TREND_LOOKBACK + 1)])

    # 52-week high/low — use the most recent 252-bar window (high of highs
    # / low of lows). Use the high/low series (not close) for accuracy.
    win = _HIGH_52W_WINDOW
    high_52w = float(np.nanmax(highs[-win:]))
    low_52w = float(np.nanmin(lows[-win:]))

    cond1 = price > s150_now
    cond2 = price > s200_now
    cond3 = s150_now > s200_now
    cond4 = s200_now > s200_prior  # rising 200-SMA over ~1 month
    cond5 = price > s50_now
    cond6 = (s50_now > s150_now) and (s50_now > s200_now)
    cond7 = (
        high_52w > 0
        and ((high_52w - price) / high_52w) <= _WITHIN_PCT_OF_HIGH
    )
    cond8 = (
        low_52w > 0
        and ((price - low_52w) / low_52w) >= _ABOVE_PCT_OF_LOW
    )

    conds = [cond1, cond2, cond3, cond4, cond5, cond6, cond7, cond8]
    score = sum(1 for c in conds if c)
    return {
        "cond1": cond1,
        "cond2": cond2,
        "cond3": cond3,
        "cond4": cond4,
        "cond5": cond5,
        "cond6": cond6,
        "cond7": cond7,
        "cond8": cond8,
        "template_score": score,
        "template_pass": score == 8,
        "price": price,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "bars_used": n,
    }


# ─── Universe + DB helpers ───────────────────────────────────────────────


def _conn(db_path: str | None = None) -> sqlite3.Connection:
    db_path = db_path or _DB_PATH
    c = sqlite3.connect(db_path, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Idempotent: create minervini_trend table + index if missing."""
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS minervini_trend ("
            " symbol TEXT NOT NULL,"
            " computed_at TEXT NOT NULL,"
            " template_score INTEGER NOT NULL,"
            " template_pass INTEGER NOT NULL,"
            " rs_pass INTEGER NOT NULL,"
            " conds_json TEXT NOT NULL,"
            " price_at_scan REAL NOT NULL,"
            " high_52w REAL,"
            " low_52w REAL,"
            " bars_used INTEGER NOT NULL,"
            " PRIMARY KEY (symbol)"
            ")"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_minervini_score "
            "ON minervini_trend(template_pass DESC, template_score DESC)"
        )
        conn.commit()
    except Exception as e:
        console.log(
            f"[yellow]minervini: _ensure_schema: "
            f"{type(e).__name__}: {e!r}"
        )


def _load_universe(db_path: str | None = None) -> list[str]:
    try:
        conn = _conn(db_path or _DB_PATH)
        try:
            rows = conn.execute("SELECT symbol FROM scan_universe").fetchall()
            return [r["symbol"] for r in rows if r["symbol"]]
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow]minervini: scan_universe load failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return []


def _load_rs_ranks(db_path: str | None = None) -> dict[str, int]:
    """LEFT JOIN against rs_rank to populate the rs_pass column. Returns
    {symbol: rs_rank}. Empty dict if rs_rank table missing or unranked."""
    try:
        conn = _conn(db_path or _DB_PATH)
        try:
            rows = conn.execute(
                "SELECT symbol, rs_rank FROM rs_rank"
            ).fetchall()
            return {r["symbol"]: int(r["rs_rank"] or 0) for r in rows}
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[dim]minervini: rs_rank table empty or missing — rs_pass=0 "
            f"for all ({type(e).__name__}: {e!r})"
        )
        return {}


# ─── Persistence ─────────────────────────────────────────────────────────


def _persist_results(
    rows: list[dict], db_path: str | None = None
) -> int:
    """Atomic full-table rewrite. Returns rows inserted."""
    db_path = db_path or _DB_PATH
    if not rows:
        return 0
    try:
        conn = _conn(db_path)
    except Exception as e:
        console.log(
            f"[yellow]minervini: persist conn failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return 0

    inserted = 0
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN")
        conn.execute("DELETE FROM minervini_trend")
        payload = [
            (
                r["symbol"],
                r["computed_at"],
                int(r["template_score"]),
                int(1 if r["template_pass"] else 0),
                int(1 if r["rs_pass"] else 0),
                r["conds_json"],
                float(r["price_at_scan"]),
                None if r.get("high_52w") is None else float(r["high_52w"]),
                None if r.get("low_52w") is None else float(r["low_52w"]),
                int(r["bars_used"]),
            )
            for r in rows
            if r["bars_used"] >= _MIN_BARS_REQUIRED
        ]
        conn.executemany(
            "INSERT INTO minervini_trend "
            "(symbol, computed_at, template_score, template_pass, rs_pass, "
            " conds_json, price_at_scan, high_52w, low_52w, bars_used) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload,
        )
        conn.commit()
        inserted = len(payload)
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        console.log(
            f"[yellow]minervini: persist failed: "
            f"{type(e).__name__}: {e!r}"
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return inserted


# ─── Main scan ────────────────────────────────────────────────────────────


def run_minervini_scan(force: bool = False) -> dict:
    """Compute + persist Minervini Trend Template state for scan_universe.

    Returns ``{scanned, persisted, passing, top_pass_symbols, scanned_at}``.
    Cached 12h unless ``force=True``.
    """
    global _last_result, _last_scan_ts

    if not force and _last_result is not None:
        if time.time() - _last_scan_ts < _CACHE_TTL:
            return _last_result

    with _scan_lock:
        if not force and _last_result is not None:
            if time.time() - _last_scan_ts < _CACHE_TTL:
                return _last_result

        universe = _load_universe()
        rs_ranks = _load_rs_ranks()
        console.log(
            f"[cyan]Minervini Filter: universe={len(universe)} "
            f"(rs_rank cache={len(rs_ranks)})"
        )
        if not universe:
            empty = {
                "scanned": 0,
                "persisted": 0,
                "passing": 0,
                "top_pass_symbols": [],
                "scanned_at": datetime.now().isoformat(),
            }
            _last_result = empty
            _last_scan_ts = time.time()
            return empty

        try:
            from engine.market_data import get_bulk_daily_ohlcv

            bars_by_sym = get_bulk_daily_ohlcv(universe, range_str="1y")
        except Exception as e:
            console.log(
                f"[yellow]minervini: get_bulk_daily_ohlcv failed: "
                f"{type(e).__name__}: {e!r}"
            )
            bars_by_sym = {}

        scanned_at = datetime.now(timezone.utc).isoformat()
        rows: list[dict] = []
        scanned = 0
        passing = 0

        for symbol in universe:
            df = bars_by_sym.get(symbol)
            if df is None or df.empty:
                continue
            scanned += 1
            try:
                conds = _evaluate_conditions(df)
            except Exception as e:
                console.log(
                    f"[yellow]minervini: eval {symbol} failed: "
                    f"{type(e).__name__}: {e!r}"
                )
                continue
            if conds["bars_used"] < _MIN_BARS_REQUIRED:
                continue

            rs_rank = rs_ranks.get(symbol, 0)
            rs_pass = rs_rank >= _RS_PASS_THRESHOLD
            cond_map = {
                f"cond{i}": bool(conds[f"cond{i}"]) for i in range(1, 9)
            }
            rows.append(
                {
                    "symbol": symbol,
                    "computed_at": scanned_at,
                    "template_score": conds["template_score"],
                    "template_pass": conds["template_pass"],
                    "rs_pass": rs_pass,
                    "conds_json": json.dumps(cond_map),
                    "price_at_scan": conds["price"],
                    "high_52w": conds["high_52w"],
                    "low_52w": conds["low_52w"],
                    "bars_used": conds["bars_used"],
                }
            )
            if conds["template_pass"]:
                passing += 1

        persisted = _persist_results(rows)

        # Top pass symbols for log line (prioritize template_pass + rs_pass)
        top_pass = sorted(
            (r for r in rows if r["template_pass"]),
            key=lambda r: (-1 if r["rs_pass"] else 0, r["symbol"]),
        )
        top_pass_symbols = [r["symbol"] for r in top_pass[:10]]

        result = {
            "scanned": scanned,
            "persisted": persisted,
            "passing": passing,
            "top_pass_symbols": top_pass_symbols,
            "scanned_at": datetime.now().isoformat(),
        }
        _last_result = result
        _last_scan_ts = time.time()

        console.log(
            f"[green]Minervini Filter: scanned={scanned} "
            f"persisted={persisted} passing={passing} "
            f"top1={top_pass_symbols[0] if top_pass_symbols else '—'}"
        )
        return result


__all__ = ["run_minervini_scan"]
