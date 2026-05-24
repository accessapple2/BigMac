"""HM-RS-RANK-VS-SPY — daily relative-strength rank vs SPY across the
scan universe.

Computes each symbol's 12-week (~60 trading day) return, compares to SPY's
return over the same window, percentile-ranks across the universe, and
persists the result to ``rs_rank`` for dashboard consumption + downstream
leader-composite scans.

Foundational: HM-MINERVINI-TREND-FILTER + any future leader filter reads
off the ``rs_rank`` column directly.

Ship state: gated by ``RS_RANK_ENABLED`` env flag (default OFF). Nightly
cadence at 20:30 AZ post-close — weekends fine (Friday's close is the
reference).

Surfacing:
    - GET /api/rs-rank  (top-N sortable list)
    - GET /api/rs-rank/{sym}
    - section-fundamentals column (HM-RS-RANK-FUNDAMENTALS-COLUMN)
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from rich.console import Console

console = Console()

_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

# ─── Scanner constants ────────────────────────────────────────────────────
_WINDOW_BARS: int = 60          # 12 weeks = ~60 trading days
_MIN_BARS_REQUIRED: int = 30    # below this, skip the symbol entirely
_BENCHMARK_SYMBOL: str = "SPY"
_CACHE_TTL: int = 43200         # 12h in-memory cache on run_rs_rank() result

_scan_lock = threading.Lock()
_last_result: dict | None = None
_last_scan_ts: float = 0.0


# ─── Indicator helpers ────────────────────────────────────────────────────


def _compute_window_return(
    df: pd.DataFrame, window: int = _WINDOW_BARS
) -> tuple[float, int]:
    """Return percent change over the trailing window. Degrades gracefully:
    if history < window, uses available bars; if < _MIN_BARS_REQUIRED,
    returns (NaN, 0).

    Returns ``(return_pct, bars_used)``.
    """
    if df is None or df.empty:
        return (float("nan"), 0)
    closes = df["Close"].to_numpy(dtype=float)
    closes = closes[~np.isnan(closes)]
    n = len(closes)
    if n < _MIN_BARS_REQUIRED:
        return (float("nan"), 0)
    bars_used = min(window, n - 1)
    if bars_used <= 0:
        return (float("nan"), 0)
    start = float(closes[-(bars_used + 1)])
    end = float(closes[-1])
    if start <= 0:
        return (float("nan"), 0)
    return ((end / start - 1.0) * 100.0, bars_used)


def _percentile_rank(values: list[float]) -> list[int]:
    """1–99 integer percentile rank with mean-rank tie-breaking.

    NaN inputs preserve their position and produce rank 0 (signals "unranked"
    to downstream consumers). The percentile is computed across non-NaN
    values only, so a 50% NaN universe still produces 1-99 spread on the
    valid half.
    """
    arr = np.asarray(values, dtype=float)
    out = np.zeros(len(arr), dtype=int)
    valid_mask = ~np.isnan(arr)
    valid = arr[valid_mask]
    if len(valid) == 0:
        return out.tolist()
    # Use scipy-style "average" tie-breaking via pd.Series.rank
    ranks = pd.Series(valid).rank(method="average", pct=True).to_numpy()
    # Map 0-1 pct rank into 1-99 integer
    scaled = np.clip(np.round(ranks * 99.0).astype(int), 1, 99)
    out[valid_mask] = scaled
    return out.tolist()


# ─── Universe + bar fetch ────────────────────────────────────────────────


def _conn(db_path: str | None = None) -> sqlite3.Connection:
    db_path = db_path or _DB_PATH
    c = sqlite3.connect(db_path, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Idempotent: create rs_rank table + index if missing. Mirrors the
    canonical migration in scripts/migrations/add_rs_rank_table.sql."""
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rs_rank ("
            " symbol TEXT NOT NULL,"
            " computed_at TEXT NOT NULL,"
            " rs_return_pct REAL NOT NULL,"
            " rs_vs_spy_pct REAL NOT NULL,"
            " rs_rank INTEGER NOT NULL,"
            " bars_used INTEGER NOT NULL,"
            " PRIMARY KEY (symbol)"
            ")"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rs_rank_rank "
            "ON rs_rank(rs_rank DESC)"
        )
        conn.commit()
    except Exception as e:
        console.log(
            f"[yellow]rs_rank: _ensure_schema: "
            f"{type(e).__name__}: {e!r}"
        )


def _load_universe(db_path: str | None = None) -> list[str]:
    """Pull symbol list from scan_universe. Defensive on missing table."""
    try:
        conn = _conn(db_path or _DB_PATH)
        try:
            rows = conn.execute("SELECT symbol FROM scan_universe").fetchall()
            return [r["symbol"] for r in rows if r["symbol"]]
        finally:
            conn.close()
    except Exception as e:
        console.log(
            f"[yellow]rs_rank: scan_universe load failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return []


# ─── Persistence ─────────────────────────────────────────────────────────


def _persist_results(
    rs_data: list[dict], db_path: str | None = None
) -> int:
    """Single-transaction full-table rewrite. Returns rows-inserted count.

    Each nightly cycle replaces the entire table — no time-series, the row
    is always "today's rank." History can be added later as ADD COLUMN.
    """
    db_path = db_path or _DB_PATH
    if not rs_data:
        return 0
    try:
        conn = _conn(db_path)
    except Exception as e:
        console.log(
            f"[yellow]rs_rank: persist conn failed: "
            f"{type(e).__name__}: {e!r}"
        )
        return 0
    inserted = 0
    try:
        _ensure_schema(conn)
        # Atomic rewrite — within a transaction, readers see consistent state.
        conn.execute("BEGIN")
        conn.execute("DELETE FROM rs_rank")
        rows_to_insert = [
            (
                r["symbol"],
                r["computed_at"],
                float(r["rs_return_pct"]),
                float(r["rs_vs_spy_pct"]),
                int(r["rs_rank"]),
                int(r["bars_used"]),
            )
            for r in rs_data
            if r.get("rs_rank")  # skip rank=0 (unranked)
        ]
        conn.executemany(
            "INSERT INTO rs_rank "
            "(symbol, computed_at, rs_return_pct, rs_vs_spy_pct, rs_rank, "
            " bars_used) VALUES (?, ?, ?, ?, ?, ?)",
            rows_to_insert,
        )
        conn.commit()
        inserted = len(rows_to_insert)
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        console.log(
            f"[yellow]rs_rank: persist failed: "
            f"{type(e).__name__}: {e!r}"
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return inserted


# ─── Main scan loop ──────────────────────────────────────────────────────


def run_rs_rank(force: bool = False) -> dict:
    """Compute + persist 12wk RS ranks for scan_universe vs SPY.

    Returns ``{scanned, persisted, spy_return_pct, top10, scanned_at}``.
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
        if not universe:
            empty = {
                "scanned": 0,
                "persisted": 0,
                "spy_return_pct": float("nan"),
                "top10": [],
                "scanned_at": datetime.now().isoformat(),
            }
            _last_result = empty
            _last_scan_ts = time.time()
            return empty

        # Ensure SPY is in the fetch list (it's the benchmark, not the ranked
        # universe — strip from the rank pool after fetch).
        fetch_syms = list(universe)
        if _BENCHMARK_SYMBOL not in fetch_syms:
            fetch_syms.append(_BENCHMARK_SYMBOL)
        console.log(
            f"[cyan]RS-Rank Scanner: universe={len(universe)} "
            f"(+{_BENCHMARK_SYMBOL} benchmark)"
        )

        try:
            from engine.market_data import get_bulk_daily_ohlcv

            bars_by_sym = get_bulk_daily_ohlcv(fetch_syms, range_str="6mo")
        except Exception as e:
            console.log(
                f"[yellow]rs_rank: get_bulk_daily_ohlcv failed: "
                f"{type(e).__name__}: {e!r}"
            )
            bars_by_sym = {}

        # SPY benchmark — required; abort if missing
        spy_df = bars_by_sym.get(_BENCHMARK_SYMBOL)
        spy_return, spy_bars_used = _compute_window_return(spy_df)
        if spy_bars_used == 0 or np.isnan(spy_return):
            console.log(
                f"[red]rs_rank: SPY benchmark unavailable — aborting scan"
            )
            empty = {
                "scanned": 0,
                "persisted": 0,
                "spy_return_pct": float("nan"),
                "top10": [],
                "scanned_at": datetime.now().isoformat(),
            }
            _last_result = empty
            _last_scan_ts = time.time()
            return empty

        # Compute per-symbol returns
        scanned_at = datetime.now(timezone.utc).isoformat()
        rows: list[dict] = []
        returns: list[float] = []
        for symbol in universe:
            if symbol == _BENCHMARK_SYMBOL:
                # Benchmark itself doesn't get ranked
                continue
            df = bars_by_sym.get(symbol)
            r, bars_used = _compute_window_return(df)
            rows.append(
                {
                    "symbol": symbol,
                    "rs_return_pct": r,
                    "rs_vs_spy_pct": (r - spy_return) if not np.isnan(r) else float("nan"),
                    "bars_used": bars_used,
                    "computed_at": scanned_at,
                }
            )
            returns.append(r)

        ranks = _percentile_rank(returns)
        for row, rk in zip(rows, ranks):
            row["rs_rank"] = int(rk)

        persisted = _persist_results(rows)

        ranked = sorted(
            (r for r in rows if r["rs_rank"] >= 1),
            key=lambda r: r["rs_rank"],
            reverse=True,
        )
        top10 = [
            {
                "symbol": r["symbol"],
                "rs_rank": r["rs_rank"],
                "rs_return_pct": round(r["rs_return_pct"], 2),
                "rs_vs_spy_pct": round(r["rs_vs_spy_pct"], 2),
            }
            for r in ranked[:10]
        ]

        result = {
            "scanned": len(rows),
            "persisted": persisted,
            "spy_return_pct": round(spy_return, 2),
            "top10": top10,
            "scanned_at": datetime.now().isoformat(),
        }
        _last_result = result
        _last_scan_ts = time.time()

        console.log(
            f"[green]RS-Rank Scanner: scanned={len(rows)} "
            f"persisted={persisted} spy_12wk={spy_return:.2f}% "
            f"top1={top10[0]['symbol'] if top10 else '—'}"
        )
        return result


__all__ = ["run_rs_rank"]
