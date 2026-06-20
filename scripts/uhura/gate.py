#!/usr/bin/env python3
"""
UHURA Step 3 — Signal Gate
Pre-filters parsed signals without requiring price data.
Marks gate_pass=1 on signals that pass all criteria.

Gate criteria:
- Liquidity: ticker avg_volume >= LIQUIDITY_FLOOR in scan_universe
- Confidence: >= MIN_CONFIDENCE
- Sentiment: not NEUTRAL
- Urgency: >= MIN_URGENCY
- Trading hours: published_at between 09:00 and 18:00 ET (captures pre/post + RTH)
"""
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from db import get_conn  # noqa: E402

LIQUIDITY_FLOOR = 500_000
MIN_CONFIDENCE = 0.55
MIN_URGENCY = 2
TRADER_DB = Path(__file__).resolve().parents[2] / "data" / "trader.db"


def get_liquid_tickers() -> set[str]:
    """Tickers meeting liquidity floor in scan_universe."""
    conn = sqlite3.connect(TRADER_DB, timeout=5)
    rows = conn.execute(
        "SELECT symbol FROM scan_universe WHERE avg_volume >= ?",
        (LIQUIDITY_FLOOR,),
    ).fetchall()
    conn.close()
    return {r[0] for r in rows}


def is_trading_adjacent(ts: str) -> bool:
    """True if timestamp is between 09:00 and 18:00 ET."""
    try:
        # Alpaca timestamps are UTC
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        # ET = UTC-4 (EDT) or UTC-5 (EST); use UTC-4 as approx
        et_hour = (dt.hour - 4) % 24
        return 9 <= et_hour <= 18
    except ValueError:
        return False


def main() -> None:
    liquid = get_liquid_tickers()
    print(f"Liquid tickers: {len(liquid)}")

    conn = get_conn()

    # Reset gate flags for re-run idempotency
    conn.execute("UPDATE uhura_signals SET gate_pass=0, gate_reason=NULL")
    conn.commit()

    rows = conn.execute(
        "SELECT id, ticker, sentiment, confidence, urgency, published_at FROM uhura_signals"
    ).fetchall()
    total = len(rows)
    print(f"Total signals to gate: {total}")

    passed = 0
    reasons: dict[str, int] = {}

    for row in rows:
        sig_id, ticker, sentiment, confidence, urgency, published_at = (
            row["id"], row["ticker"], row["sentiment"],
            row["confidence"], row["urgency"], row["published_at"],
        )
        # Check each criterion
        if ticker not in liquid:
            reasons["low_liquidity"] = reasons.get("low_liquidity", 0) + 1
            continue
        if sentiment == "NEUTRAL":
            reasons["neutral"] = reasons.get("neutral", 0) + 1
            continue
        if (confidence or 0) < MIN_CONFIDENCE:
            reasons["low_confidence"] = reasons.get("low_confidence", 0) + 1
            continue
        if (urgency or 1) < MIN_URGENCY:
            reasons["low_urgency"] = reasons.get("low_urgency", 0) + 1
            continue
        if not is_trading_adjacent(published_at or ""):
            reasons["off_hours"] = reasons.get("off_hours", 0) + 1
            continue

        conn.execute(
            "UPDATE uhura_signals SET gate_pass=1, gate_reason='ok' WHERE id=?",
            (sig_id,),
        )
        passed += 1

    conn.commit()
    conn.close()

    print(f"\nGate results: {passed}/{total} passed ({100*passed//max(total,1)}%)")
    print("Rejection breakdown:")
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")


if __name__ == "__main__":
    main()
