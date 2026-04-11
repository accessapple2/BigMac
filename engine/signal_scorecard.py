"""
Signal Scorecard — Alpha Engine Component 1
Logs every signal with entry/exit outcome and scores them hourly.

Tables:
  signal_scorecard — per-signal log with outcome fields

Key functions:
  log_signal(signal_data)  — called by scanner to record a new signal
  score_signals()          — hourly job that fills in outcomes for matured signals
  get_scorecard(limit)     — fetch recent scored signals for UI
"""
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

DB = "data/trader.db"
logger = logging.getLogger("signal_scorecard")


def _db():
    return sqlite3.connect(DB, timeout=10)


def ensure_tables():
    con = _db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS signal_scorecard (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            ticker          TEXT    NOT NULL,
            direction       TEXT    NOT NULL,          -- PUT / CALL
            indicator       TEXT    NOT NULL,          -- e.g. RSI_OVERSOLD, GEX_NEG, GAP_FILL
            strategy        TEXT,                      -- e.g. BEAR_PUT_SPREAD, MARKET_PUT
            confidence      REAL    DEFAULT 0,
            entry_price     REAL,
            strike          REAL,
            expiry          TEXT,
            vix_at_entry    REAL,
            gex_at_entry    REAL,
            session         TEXT,
            -- Outcome fields (filled in by score_signals)
            exit_price      REAL,
            outcome_pct     REAL,                      -- % gain/loss vs entry_price
            win             INTEGER,                   -- 1=win 0=loss NULL=pending
            scored_at       TEXT,
            notes           TEXT
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS ix_sc_ticker ON signal_scorecard(ticker)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_sc_indicator ON signal_scorecard(indicator)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_sc_created ON signal_scorecard(created_at)")
    con.commit()
    con.close()


def log_signal(signal_data: dict) -> int:
    """
    Record a new signal. signal_data keys:
      ticker, direction, indicator, strategy, confidence,
      entry_price, strike, expiry, vix_at_entry, gex_at_entry, session
    Returns the new row id.
    """
    ensure_tables()
    con = _db()
    cur = con.execute("""
        INSERT INTO signal_scorecard
          (ticker, direction, indicator, strategy, confidence,
           entry_price, strike, expiry, vix_at_entry, gex_at_entry, session)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        signal_data.get("ticker", "SPY"),
        signal_data.get("direction", "PUT"),
        signal_data.get("indicator", "UNKNOWN"),
        signal_data.get("strategy"),
        signal_data.get("confidence", 0),
        signal_data.get("entry_price"),
        signal_data.get("strike"),
        signal_data.get("expiry"),
        signal_data.get("vix_at_entry"),
        signal_data.get("gex_at_entry"),
        signal_data.get("session"),
    ))
    row_id = cur.lastrowid
    con.commit()
    con.close()
    logger.info(f"Signal logged: {signal_data.get('ticker')} {signal_data.get('direction')} id={row_id}")
    return row_id


def score_signals():
    """
    Hourly job — fill in outcomes for signals >1 hour old that still lack an exit_price.
    Uses yfinance current price vs entry_price to compute outcome_pct.
    Marks win=1 if direction=PUT and price dropped ≥1%, or direction=CALL and price rose ≥1%.
    """
    ensure_tables()
    con = _db()
    pending = con.execute("""
        SELECT id, ticker, direction, entry_price, created_at
        FROM signal_scorecard
        WHERE win IS NULL
          AND entry_price IS NOT NULL
          AND created_at <= datetime('now', '-1 hour')
        LIMIT 50
    """).fetchall()
    con.close()

    if not pending:
        return

    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available — cannot score signals")
        return

    tickers_needed = list({row[1] for row in pending})
    prices = {}
    for sym in tickers_needed:
        try:
            info = yf.Ticker(sym).fast_info
            prices[sym] = float(info.last_price or 0)
        except Exception:
            prices[sym] = 0.0

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    con = _db()
    for row_id, ticker, direction, entry_price, _ in pending:
        current = prices.get(ticker, 0)
        if not current or not entry_price:
            continue
        pct = (current - entry_price) / entry_price * 100.0
        # PUT wins if price fell ≥1%; CALL wins if price rose ≥1%
        if direction == "PUT":
            win = 1 if pct <= -1.0 else 0
        else:
            win = 1 if pct >= 1.0 else 0
        con.execute("""
            UPDATE signal_scorecard
            SET exit_price=?, outcome_pct=?, win=?, scored_at=?
            WHERE id=?
        """, (current, round(pct, 3), win, now_str, row_id))
    con.commit()
    con.close()
    logger.info(f"Scored {len(pending)} signals")


def get_scorecard(limit: int = 50) -> list:
    """Return recent signals (scored and pending)."""
    ensure_tables()
    con = _db()
    rows = con.execute("""
        SELECT id, created_at, ticker, direction, indicator, strategy,
               confidence, entry_price, exit_price, outcome_pct, win, session
        FROM signal_scorecard
        ORDER BY id DESC LIMIT ?
    """, (limit,)).fetchall()
    con.close()
    cols = ["id", "created_at", "ticker", "direction", "indicator", "strategy",
            "confidence", "entry_price", "exit_price", "outcome_pct", "win", "session"]
    return [dict(zip(cols, r)) for r in rows]


def get_indicator_stats() -> list:
    """Aggregate win rate per indicator (used by indicator_bench)."""
    ensure_tables()
    con = _db()
    rows = con.execute("""
        SELECT indicator,
               COUNT(*) as total,
               SUM(win) as wins,
               ROUND(AVG(outcome_pct), 3) as avg_return_pct,
               ROUND(AVG(CASE WHEN win=1 THEN 1.0 ELSE 0.0 END)*100, 1) as win_rate
        FROM signal_scorecard
        WHERE win IS NOT NULL
        GROUP BY indicator
        ORDER BY win_rate DESC
    """).fetchall()
    con.close()
    cols = ["indicator", "total", "wins", "avg_return_pct", "win_rate"]
    return [dict(zip(cols, r)) for r in rows]
