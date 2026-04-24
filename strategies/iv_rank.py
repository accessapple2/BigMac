"""
strategies/iv_rank.py

Typed wrapper around engine.high_iv_scanner._get_iv_rank().

Returns an IVSnapshot dataclass. When record=True, persists the
implied_vol reading to iv_history so we accumulate true IV history.

Graduation rule: once iv_history has ≥252 rows for a ticker, we
switch from the realized-vol proxy in high_iv_scanner to a proper
true-IV rank computed from our own recorded data.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"
MIN_HISTORY_DAYS = 252  # ~1 trading year before we trust the true rank


@dataclass
class IVSnapshot:
    ticker: str
    iv_rank: float      # 0–100
    source: str         # 'yfinance' | 'iv_history'
    current_iv: float   # raw implied vol %, e.g. 32.4
    spot: float         # underlying price at capture time


def get_iv_rank(ticker: str, record: bool = True) -> IVSnapshot | None:
    """
    Return an IVSnapshot for ticker, or None if data is unavailable.

    Steps:
    1. If mock mode active, return mock IVSnapshot (no DB write).
    2. Pull current IV via yfinance (high_iv_scanner._get_iv_rank).
    3. Optionally record it to iv_history.
    4. If iv_history has ≥252 rows → compute true IV rank from history.
       Otherwise use the realized-vol proxy rank from high_iv_scanner.
    """
    # Mock path — no network, no DB write even if record=True
    from .mock_data import is_mock_mode, mock_iv_rank, MOCK_SPOT
    if is_mock_mode():
        mock = mock_iv_rank(ticker)
        if mock is None:
            return None
        return IVSnapshot(
            ticker=ticker,
            iv_rank=mock.iv_rank,
            source="mock",
            current_iv=mock.iv_rank,   # proxy: use rank as IV% stand-in
            spot=MOCK_SPOT.get(ticker, 0.0),
        )

    # Polygon path — real implied vol from live option chains
    try:
        from .polygon_client import fetch_atm_iv, fetch_spot_price
        current_iv = fetch_atm_iv(ticker, target_dte=30)
    except Exception as e:
        current_iv = None
        print(f"[iv_rank] polygon fetch failed for {ticker}: {e}")

    if current_iv is not None:
        if record:
            try:
                _record_iv(ticker, current_iv, "polygon-atm-call")
            except Exception as e:
                print(f"[iv_rank] record failed for {ticker}: {e}")

        history = _fetch_history(ticker)
        if len(history) < 5:
            # Bootstrap: return neutral 50.0 until we have enough data.
            # Neutral IV rank → bull_call_spread chosen; re-evaluated each day.
            # Change threshold back to 20 after backfill is complete.
            print(f"[iv_rank] {ticker} bootstrapping ({len(history)}/5 days)")
            spot = fetch_spot_price(ticker) or 0.0
            return IVSnapshot(
                ticker=ticker, iv_rank=50.0,
                source="polygon-bootstrapping",
                current_iv=current_iv, spot=spot,
            )

        iv_min = min(history)
        iv_max = max(history)
        rank = 50.0 if iv_max == iv_min else (
            (current_iv - iv_min) / (iv_max - iv_min) * 100.0
        )
        rank = max(0.0, min(100.0, rank))

        spot = fetch_spot_price(ticker) or 0.0
        return IVSnapshot(
            ticker=ticker,
            iv_rank=rank,
            source="polygon",
            current_iv=current_iv,
            spot=spot,
        )

    # Yfinance fallback — late import to avoid loading at module level
    try:
        from engine.high_iv_scanner import _get_iv_rank as _yf_iv
    except ImportError as e:
        print(f"[iv_rank] import error: {e}")
        return None

    raw = _yf_iv(ticker)
    if raw is None:
        return None

    current_iv: float = raw["current_iv"]
    spot: float = raw["spot"]
    proxy_rank: float = raw["iv_rank"]

    today = date.today().isoformat()

    # Record snapshot when requested
    if record:
        _record(ticker, today, current_iv, source="yfinance")

    # Check if we have enough history for a true rank
    history = _load_history(ticker, days=MIN_HISTORY_DAYS)
    if len(history) >= MIN_HISTORY_DAYS:
        iv_rank, source = _true_rank(current_iv, history), "iv_history"
    else:
        iv_rank, source = proxy_rank, "yfinance"

    return IVSnapshot(
        ticker=ticker,
        iv_rank=iv_rank,
        source=source,
        current_iv=current_iv,
        spot=spot,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _record_iv(ticker: str, iv: float, source: str) -> None:
    """Upsert today's IV reading — used by the Polygon path."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute(
            """
            INSERT INTO iv_history (ticker, as_of_date, implied_vol, source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker, as_of_date) DO UPDATE SET
                implied_vol = excluded.implied_vol,
                source = excluded.source,
                captured_at = CURRENT_TIMESTAMP
            """,
            (ticker, date.today().isoformat(), iv, source),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_history(ticker: str, days: int = 252) -> list[float]:
    """Return IV readings within the last `days` calendar days, oldest first."""
    from datetime import timedelta
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute(
            """
            SELECT implied_vol FROM iv_history
            WHERE ticker = ? AND as_of_date >= ?
            ORDER BY as_of_date ASC
            """,
            (ticker, cutoff),
        )
        return [row[0] for row in cur.fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _record(ticker: str, as_of_date: str, implied_vol: float, source: str) -> None:
    """Upsert one IV reading into iv_history. Safe to call repeatedly."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute(
            """
            INSERT INTO iv_history (ticker, as_of_date, implied_vol, source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker, as_of_date) DO UPDATE SET
                implied_vol=excluded.implied_vol,
                source=excluded.source
            """,
            (ticker, as_of_date, implied_vol, source),
        )
        conn.commit()
    except sqlite3.OperationalError as e:
        # Table may not exist if migration 002 hasn't run yet
        print(f"[iv_rank] DB write skipped ({e})")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_history(ticker: str, days: int) -> list[float]:
    """Return up to `days` most-recent implied_vol readings for ticker."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute(
            """
            SELECT implied_vol FROM iv_history
            WHERE ticker = ?
            ORDER BY as_of_date DESC
            LIMIT ?
            """,
            (ticker, days),
        )
        rows = [r[0] for r in cur.fetchall()]
        return rows
    except sqlite3.OperationalError:
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _true_rank(current_iv: float, history: list[float]) -> float:
    """IV rank = (current - low) / (high - low) * 100, clamped 0–100."""
    lo, hi = min(history), max(history)
    if hi == lo:
        return 50.0
    rank = (current_iv - lo) / (hi - lo) * 100.0
    return max(0.0, min(100.0, rank))
