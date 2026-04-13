"""engine/super_trader.py — Soft confidence multipliers for trade signals.

NOT hard gates. Multipliers only — confidence floats up or down, never blocked.

Sources:
  1. GEX alignment   — gex_levels.composite_score + composite_signal
  2. Options flow    — options_flow_history.put_call_ratio (last 24 h)

Logging: signal_multiplier_log in trader.db for 30-day impact analysis.

Multiplier table:

  GEX (action=BUY):
    score > 0.6 AND bullish signal  →  × 1.10  (tailwind)
    score > 0.6 AND bearish signal  →  × 0.85  (headwind)
    score 0.3–0.6 or < 0.3         →  × 1.00  (neutral, never penalise no-data)

  Options flow (put_call_ratio proxy, last 24 h):
    PCR < 0.70  →  × 1.15  (heavy call buying ≈ unusual_call_sweep)
    PCR > 1.30  →  × 0.80  (heavy put buying  ≈ unusual_put_sweep)
    no data     →  × 1.00  (neutral)

  final_confidence = base × gex_mult × flow_mult
  Capped at 95, floored at 10.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger("super_trader")

TRADER_DB  = "data/trader.db"
CONF_CAP   = 95.0
CONF_FLOOR = 10.0


# ── DB setup ──────────────────────────────────────────────────────────────────

def ensure_tables() -> None:
    """Create signal_multiplier_log if not present."""
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""CREATE TABLE IF NOT EXISTS signal_multiplier_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker      TEXT    NOT NULL,
        date        TEXT    NOT NULL,
        base_score  REAL    NOT NULL,
        gex_mult    REAL    NOT NULL,
        flow_mult   REAL    NOT NULL,
        final_score REAL    NOT NULL,
        trade_taken INTEGER NOT NULL DEFAULT 0,
        created_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    c.commit()
    c.close()


# ── GEX multiplier ────────────────────────────────────────────────────────────

def _gex_multiplier(symbol: str, action: str = "BUY") -> float:
    """GEX alignment → confidence multiplier."""
    if action != "BUY":
        return 1.0
    try:
        c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=10)
        c.row_factory = sqlite3.Row
        row = c.execute(
            "SELECT composite_score, composite_signal "
            "FROM gex_levels WHERE symbol=? ORDER BY calc_time DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        c.close()
        if not row or row["composite_score"] is None:
            return 1.0
        score  = float(row["composite_score"])
        signal = str(row["composite_signal"] or "").lower()
        is_bullish = "bull" in signal or "positive" in signal
        is_bearish = "bear" in signal or "negative" in signal
        if score > 0.6 and is_bullish:
            return 1.10
        if score > 0.6 and is_bearish:
            return 0.85
        return 1.0   # 0.3–0.6 or < 0.3 or neutral signal
    except Exception:
        return 1.0


# ── Options flow multiplier ───────────────────────────────────────────────────

def _flow_multiplier(symbol: str, action: str = "BUY") -> float:
    """Options flow alignment → confidence multiplier (put_call_ratio proxy)."""
    if action != "BUY":
        return 1.0
    try:
        c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=10)
        c.row_factory = sqlite3.Row
        row = c.execute(
            "SELECT put_call_ratio FROM options_flow_history "
            "WHERE symbol=? AND created_at > datetime('now','-1 day') "
            "ORDER BY created_at DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        c.close()
        if not row or row["put_call_ratio"] is None:
            return 1.0
        pcr = float(row["put_call_ratio"])
        if pcr < 0.70:
            return 1.15   # heavy call buying → bullish flow
        if pcr > 1.30:
            return 0.80   # heavy put buying  → bearish flow
        return 1.0
    except Exception:
        return 1.0


# ── Main entry ─────────────────────────────────────────────────────────────────

def apply_multipliers(
    symbol: str,
    base_confidence: float,
    action: str = "BUY",
    trade_taken: bool = False,
) -> tuple[float, float, float]:
    """
    Apply GEX and options flow multipliers to base confidence.

    Args:
        symbol:          Ticker symbol.
        base_confidence: LLM confidence score (0–100).
        action:          "BUY" / "SELL" / "PASS" — only BUY gets multiplied.
        trade_taken:     Set True after execution to mark the log row.

    Returns:
        (final_confidence, gex_mult, flow_mult)
    """
    ensure_tables()

    gex_mult  = _gex_multiplier(symbol, action)
    flow_mult = _flow_multiplier(symbol, action)

    final = base_confidence * gex_mult * flow_mult
    final = max(CONF_FLOOR, min(CONF_CAP, round(final, 2)))

    # Persist for impact analysis
    try:
        c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute(
            """INSERT INTO signal_multiplier_log
               (ticker, date, base_score, gex_mult, flow_mult, final_score, trade_taken)
               VALUES (?,?,?,?,?,?,?)""",
            (
                symbol,
                datetime.utcnow().strftime("%Y-%m-%d"),
                round(base_confidence, 2),
                gex_mult,
                flow_mult,
                final,
                1 if trade_taken else 0,
            ),
        )
        c.commit()
        c.close()
    except Exception as e:
        logger.warning(f"signal_multiplier_log insert error: {e}")

    if gex_mult != 1.0 or flow_mult != 1.0:
        logger.info(
            f"[Multipliers] {symbol} {action}: "
            f"base={base_confidence:.1f} × GEX={gex_mult:.2f} "
            f"× flow={flow_mult:.2f} → final={final:.1f}"
        )

    return final, gex_mult, flow_mult
