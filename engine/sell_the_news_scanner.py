"""engine/sell_the_news_scanner.py — HM-SHORT-ENGINE (Path B shadow emitter).

Post-earnings FADE → SHORT votes into the W0 shadow substrate (signal-center
trade_signals). Observation-only, never executes. Modeled on engine/signal_bridge.py
(the proven shadow-bridge path), NOT RULES_SCANNERS (BUY/SELL-only, no W0 accrual).
SHORT W0 scoring verified viable by the 2026-06-05 probe (direction 'short',
stop-first R correct). Runs in the trader process (.venv 3.14 — PEP 604 OK here).

Source: deep_scan_results WHERE earnings_today=1 (last N trading days). NOTE: this
        feed is narrow-universe / yfinance / earnings-season-dependent — dry since
        2026-05-07, so the scanner is shadow-QUIET until earnings names repopulate.
        Acceptable for observation mode (flagged, not a blocker).
Prices: engine.market_data.get_bulk_prices (Alpaca snapshot: open_price /
        prev_close / last_price in one batched call).
Emit:   POST :9000/api/signal, agent='shadow-bridge:sell_the_news', action='SHORT'.
        Excluded from execution by the paper_trader.buy shadow chokepoint
        (agent prefix 'shadow-bridge') + can_trade_live=0. No order path touched.

v1 fade definition: fade vs the EARNINGS-DAY REFERENCE price (deep_scan_results
.entry_price on the earnings-day row) — a clean proxy that avoids a daily-bars
lookup. Refine to true earnings-day gap later only if shadow results justify it.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRADER_DB         = os.path.join(_ROOT, "data", "trader.db")
SIGNAL_CENTER_URL = "http://127.0.0.1:9000/api/signal"
AGENT             = "shadow-bridge:sell_the_news"

LOOKBACK_DAYS = 3                 # earnings within last N trading days
MIN_FADE_PCT  = 0.01              # now >= 1% below the earnings-day reference
SKIP_REGIMES  = {"BULL_CROSS"}    # don't short into a strong uptrend (CAUTIOUS_BULL allowed)
MAX_EMIT      = 10


def _current_regime(conn: sqlite3.Connection) -> str | None:
    r = conn.execute("SELECT regime FROM regime_history ORDER BY date DESC LIMIT 1").fetchone()
    return r[0] if r else None


def _recent_earners(conn: sqlite3.Connection) -> list[dict]:
    """earnings_today=1 symbols in the last N trading days + earnings-day ref price."""
    rows = conn.execute(
        """SELECT symbol, MAX(scan_date) AS edate, AVG(entry_price) AS ref_px
             FROM deep_scan_results
            WHERE earnings_today=1 AND scan_date >= date('now', ?)
            GROUP BY symbol""",
        (f"-{LOOKBACK_DAYS * 2} day",),
    ).fetchall()
    return [{"symbol": r[0], "edate": r[1], "ref_px": r[2]} for r in rows if r[2]]


def _emit_short(sym: str, entry: float, stop: float, target: float,
                e: dict, fade_pct: float, regime: str | None) -> bool:
    payload = {
        "symbol": sym, "action": "SHORT", "type": "SWING", "confidence": 60,
        "agent": AGENT, "model": "sell_the_news_rules",
        "reasoning": (f"[SHADOW · sell-the-news · W0] {sym} reported earnings {e['edate']}, "
                      f"faded {fade_pct:.1%} below earnings-day ref ${e['ref_px']:.2f} -> ${entry:.2f}. "
                      f"Regime {regime}. SHORT stop ${stop} target ${target}. NOT executed (obs-only)."),
        "price": entry, "stop_loss": stop, "take_profit": target, "timeframe": "SWING",
        "context_summary": f"sell-the-news fade | edate {e['edate']} | fade {fade_pct:.1%}",
        "sources": ["shadow-bridge", "sell_the_news", "earnings_fade"],
    }
    try:
        r = requests.post(SIGNAL_CENTER_URL, json=payload, timeout=6)
        if r.status_code in (200, 201):
            logger.info("[STN] emitted SHORT shadow %s @ %.2f (fade %.1f%%)", sym, entry, fade_pct * 100)
            return True
        logger.warning("[STN] emit %s HTTP %s", sym, r.status_code)
    except Exception as ex:  # noqa: BLE001 — network/serve errors must not crash the scan
        logger.warning("[STN] emit %s error: %s: %r", sym, type(ex).__name__, ex)
    return False


def scan_and_emit() -> dict:
    """One scan pass: find post-earnings fades, emit SHORT shadow signals.
    Pure read of trader.db (ro) + outbound POST to signal-center. No execution."""
    meta = {"checked": 0, "qualified": 0, "emitted": 0, "skipped_regime": False, "regime": None}
    conn = sqlite3.connect(f"file:{TRADER_DB}?mode=ro", uri=True)
    try:
        regime = _current_regime(conn)
        meta["regime"] = regime
        if regime in SKIP_REGIMES:
            meta["skipped_regime"] = True
            return meta
        earners = _recent_earners(conn)
    finally:
        conn.close()

    meta["checked"] = len(earners)
    if not earners:
        return meta

    from engine.market_data import get_bulk_prices
    prices = get_bulk_prices([e["symbol"] for e in earners])

    for e in earners[:MAX_EMIT]:
        sym, ref = e["symbol"], e["ref_px"]
        px = prices.get(sym) or {}
        last  = px.get("last_price")
        open_ = px.get("open_price")
        prev  = px.get("prev_close")
        vol   = px.get("volume")
        if not (last and open_ and ref):
            continue
        if vol == 40 and open_ == prev:            # known get_bulk_prices stub fixture
            continue
        fade_pct = (ref - last) / ref              # faded below earnings-day reference
        if fade_pct < MIN_FADE_PCT or last >= open_:   # need fade + intraday weakness
            continue
        stop   = round(open_ * 1.015, 2)           # SHORT: stop ABOVE entry
        target = round(last * (1 - fade_pct * 1.5), 2)  # target BELOW (1.5R)
        meta["qualified"] += 1
        if _emit_short(sym, last, stop, target, e, fade_pct, regime):
            meta["emitted"] += 1
    return meta
