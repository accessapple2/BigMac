#!/usr/bin/env python3
"""
Uhura -> Bridge Vote Integration
Wires institutional signals (from uhura_agent.py) into the convergence system.
Adapted to actual institutional_signals schema: ticker, signal, reasoning, scan_date, created_at.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"

# Vote contribution per signal type
SIGNAL_WEIGHTS: dict[str, float] = {
    "STRONG_BUY":  2.0,
    "BUY":         1.0,
    "NEUTRAL":     0.0,
    "SELL":       -1.0,
    "STRONG_SELL": -2.0,
}

# Confidence boost applied on top of the agent's base confidence
CONFIDENCE_BOOST: dict[str, int] = {
    "STRONG_BUY":  8,   # +8 pts — hedge funds loading up
    "BUY":         4,   # +4 pts
    "NEUTRAL":     0,
    "SELL":       -4,
    "STRONG_SELL": -8,  # -8 pts — institutions dumping
}

# Derived confidence from signal type (no confidence column in DB)
SIGNAL_CONFIDENCE: dict[str, int] = {
    "STRONG_BUY":  85,
    "BUY":         70,
    "NEUTRAL":     50,
    "SELL":        30,
    "STRONG_SELL": 15,
}


def get_institutional_signal(ticker: str, days: int = 30) -> Optional[dict]:
    """
    Return the most recent institutional signal for a ticker within `days`.
    Confidence is derived from signal type (not stored in DB).
    """
    conn        = sqlite3.connect(DB_PATH)
    date_filter = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    row = conn.execute("""
        SELECT ticker, signal, reasoning, scan_date, created_at
        FROM   institutional_signals
        WHERE  ticker = ?
          AND  scan_date >= ?
        ORDER  BY created_at DESC
        LIMIT  1
    """, (ticker.upper(), date_filter)).fetchone()
    conn.close()

    if not row:
        return None

    ticker, signal, reasoning, scan_date, created_at = row
    signal = signal or "NEUTRAL"

    return {
        "ticker":           ticker,
        "signal":           signal,
        "reasoning":        reasoning,
        "scan_date":        scan_date,
        "created_at":       created_at,
        "confidence":       SIGNAL_CONFIDENCE.get(signal, 50),
        "confidence_boost": CONFIDENCE_BOOST.get(signal, 0),
        "vote_weight":      SIGNAL_WEIGHTS.get(signal, 0.0),
    }


def get_bulk_institutional_signals(tickers: list[str], days: int = 30) -> dict[str, dict]:
    """Return the most recent institutional signal for each ticker (batch)."""
    if not tickers:
        return {}

    conn        = sqlite3.connect(DB_PATH)
    date_filter = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    ph          = ",".join("?" * len(tickers))

    rows = conn.execute(
        f"""SELECT ticker, signal, reasoning, scan_date, created_at
            FROM   institutional_signals
            WHERE  ticker IN ({ph}) AND scan_date >= ?
            ORDER  BY ticker, created_at DESC""",
        [t.upper() for t in tickers] + [date_filter],
    ).fetchall()
    conn.close()

    results: dict[str, dict] = {}
    for row in rows:
        ticker = row[0]
        if ticker in results:
            continue  # already captured most recent
        signal = row[1] or "NEUTRAL"
        results[ticker] = {
            "ticker":           ticker,
            "signal":           signal,
            "reasoning":        row[2],
            "scan_date":        row[3],
            "created_at":       row[4],
            "confidence":       SIGNAL_CONFIDENCE.get(signal, 50),
            "confidence_boost": CONFIDENCE_BOOST.get(signal, 0),
            "vote_weight":      SIGNAL_WEIGHTS.get(signal, 0.0),
        }
    return results


def apply_institutional_boost(ticker: str, base_confidence: float, days: int = 30) -> tuple[float, Optional[dict]]:
    """
    Apply institutional signal boost to an agent's confidence score.
    Returns (adjusted_confidence, signal_info | None).
    """
    info = get_institutional_signal(ticker, days)
    if not info:
        return base_confidence, None
    adjusted = min(100.0, max(0.0, base_confidence + info["confidence_boost"]))
    return adjusted, info


def get_institutional_vote(ticker: str) -> float:
    """
    Vote contribution from institutional signals for Bridge Vote.
    Returns -2.0 (STRONG_SELL) to +2.0 (STRONG_BUY), 0.0 if no data.
    """
    info = get_institutional_signal(ticker, days=30)
    return info["vote_weight"] if info else 0.0


def should_block_trade(ticker: str, action: str = "BUY") -> tuple[bool, Optional[str]]:
    """
    Check whether institutions signal hard opposition to this trade.
    Only blocks BUY when STRONG_SELL (not the reverse — never block taking profits).
    """
    info = get_institutional_signal(ticker, days=30)
    if not info:
        return False, None

    if action.upper() == "BUY" and info["signal"] == "STRONG_SELL":
        return True, f"Uhura block: institutions selling {ticker} ({info['signal']})"
    return False, None


def get_institutional_summary() -> dict:
    """Summary of current institutional signals across all tracked tickers."""
    conn = sqlite3.connect(DB_PATH)

    counts = conn.execute("""
        SELECT signal, COUNT(*) FROM institutional_signals
        WHERE scan_date >= date('now', '-30 days')
        GROUP BY signal
    """).fetchall()

    bullish = conn.execute("""
        SELECT ticker, signal FROM institutional_signals
        WHERE signal IN ('STRONG_BUY','BUY')
          AND scan_date >= date('now', '-30 days')
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()

    bearish = conn.execute("""
        SELECT ticker, signal FROM institutional_signals
        WHERE signal IN ('STRONG_SELL','SELL')
          AND scan_date >= date('now', '-30 days')
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()

    conn.close()

    signal_counts = {r[0]: r[1] for r in counts}
    return {
        "signal_counts": signal_counts,
        "top_bullish": [{"ticker": r[0], "signal": r[1]} for r in bullish],
        "top_bearish": [{"ticker": r[0], "signal": r[1]} for r in bearish],
        "total_signals": sum(signal_counts.values()),
    }


if __name__ == "__main__":
    print("\nUHURA BRIDGE INTEGRATION TEST")
    print("=" * 60)

    summary = get_institutional_summary()
    print(f"\nSignal Summary (Last 30 Days): {summary['total_signals']} total")
    for signal, count in summary["signal_counts"].items():
        print(f"  {signal}: {count}")

    if summary["top_bullish"]:
        print("\nTop Bullish:")
        for item in summary["top_bullish"][:5]:
            print(f"  {item['ticker']}: {item['signal']}")

    print("\nTest boost on AAPL:")
    adj, info = apply_institutional_boost("AAPL", 75)
    if info:
        print(f"  75% -> {adj}% (boost {info['confidence_boost']}, signal {info['signal']})")
    else:
        print("  No institutional signal for AAPL")
