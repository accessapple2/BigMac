"""engine/holly_live.py — HM-HOLLY-WORKS Stage 5: live paper-trading loop for the
OOS-validated works-set (the_continuation + count_de_monet).

CRITICAL DESIGN PRINCIPLE — LIVE MUST MATCH VALIDATION. the_continuation's OOS Sharpe 1.47
was measured WITH: the exact entry conditions (_ti_signals), the selectivity gate (rising-
edge + ≤2 entries/symbol/day + 60-min cooldown), 15bps fees, and the per-strategy swing
exit (8% stop / 6% target / 20d max-hold). The live loop replicates ALL of these or the
live edge won't reproduce:
  - ENTRY: reuses engine.holly_intraday._ti_signals / _ti_signals_b2 (SAME code = same
    conditions); fires only on a RISING-EDGE (setup just triggered this bar).
  - SELECTIVITY: live equivalent of _gate_entries — ≤2 holly BUYs/symbol/day + 60-min
    cooldown, checked against trade history.
  - UNIVERSE: small-cap movers ($1-50, vol_ratio≥2) from universe_scan (the validated set).
  - EXIT: per-strategy swing levels from HOLLY_WORKS, managed each cycle.

Two entry points (mirror ollie_auto_check + _ollie_check_tiered_tp):
  - holly_scanner_check()  — scan universe, open new setups
  - holly_manage_exits()   — stop / target / max-hold on open positions

Internal $10k book (holly-scanner NOT in _EXECUTION_PORTFOLIO_BY_PLAYER → route_mode=paper,
no Alpaca co-mingling). FAIL-LOUD (NTFY on error, never silent-None). dry_run=True logs
intended actions without placing trades — used for eyes-on verification before going live.

Runs under the live trader venv (uses paper_trader). Scheduled via reboot-survivable cron.
"""
from __future__ import annotations

import os
import sqlite3
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

PLAYER_ID = "holly-scanner"
TRADER_DB = "data/trader.db"
HOLLY_POSITION_PCT = 0.10     # 10% of book cash per position (≤2/day, multi-symbol → diversified)
MAX_ENTRIES_PER_DAY = 2       # matches engine.holly_intraday.MAX_ENTRIES_PER_DAY
COOLDOWN_MIN = 60             # 60-min cooldown (== 12 × 5min bars)
UNIVERSE_LIMIT = 30


def _notify_fail(msg: str) -> None:
    logger.error("[holly_live] %s", msg)
    try:
        from engine.alert_channels import send_alert, AlertLevel
        send_alert(message=f"🔴 HOLLY-LIVE: {msg}", level=AlertLevel.WARNING,
                   alert_type="hm-holly-live-error", rate_limit_secs=3600)
    except Exception:
        pass


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, timeout=30.0)
    c.row_factory = sqlite3.Row
    return c


def _init_swing_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS holly_swing_trades (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               opened_at TEXT, symbol TEXT, strategy TEXT, entry_price REAL,
               qty REAL, stop_price REAL, target_price REAL, max_hold_until TEXT,
               closed INTEGER DEFAULT 0, closed_at TEXT, exit_price REAL, exit_reason TEXT
           )"""
    )
    conn.commit()


# ── UNIVERSE (the validated small-cap mover set) ──────────────────────────────
def _holly_universe(limit: int = UNIVERSE_LIMIT) -> list[str]:
    """Small-cap movers ($1-50, vol_ratio≥2) from the most recent universe_scan — the
    same selection criteria the works-set was validated on."""
    conn = _conn()
    try:
        rows = conn.execute(
            """SELECT ticker, COUNT(*) app, AVG(volume_ratio) vr
               FROM universe_scan
               WHERE close BETWEEN 1 AND 50 AND volume_ratio >= 2
               GROUP BY ticker ORDER BY app DESC, vr DESC LIMIT ?""",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [r["ticker"] for r in rows]


# ── SELECTIVITY (live equivalent of _gate_entries) ────────────────────────────
def _passes_selectivity(conn: sqlite3.Connection, symbol: str) -> bool:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    n = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE player_id=? AND symbol=? AND action='BUY' "
        "AND date(executed_at)=?", (PLAYER_ID, symbol, today)).fetchone()[0]
    if n >= MAX_ENTRIES_PER_DAY:
        return False
    last = conn.execute(
        "SELECT MAX(executed_at) FROM trades WHERE player_id=? AND symbol=? AND action='BUY'",
        (PLAYER_ID, symbol)).fetchone()[0]
    if last:
        try:
            lt = datetime.fromisoformat(str(last).replace("Z", ""))
            if (datetime.now() - lt) < timedelta(minutes=COOLDOWN_MIN):
                return False
        except Exception:
            pass
    return True


def _fresh_setup(df, strategy: str, symbol: str) -> bool:
    """True iff the strategy's entry just triggered on the LAST bar (rising edge) — the
    live equivalent of the backtest's rising-edge gate. Reuses the validated signal code."""
    from engine.holly_intraday import _ti_signals, _ti_signals_b2, TI_BATCH_2
    e, _x = (_ti_signals_b2(df, strategy, symbol) if strategy in TI_BATCH_2
             else _ti_signals(df, strategy))
    if e is None or len(e) < 2:
        return False
    e = e.fillna(False).astype(bool)
    return bool(e.iloc[-1] and not e.iloc[-2])


# ── REGIME GATE (HM-HOLLY-REGIME-GATE 2026-05-31, SHADOW-FIRST) ────────────────
# The 180-day regime test proved the_continuation is BULL-ONLY: +1.57%/trade & 66% WR in
# BULL_CROSS, but ~0 edge (+0.10%/trade) and a -88% drawdown PATH in bear/cautious-bear.
# This gate would BENCH the momentum works-set in bear/crisis, TRADE in bull/trending —
# mirroring the fleet's stand-down (crew_scanner BEAR/CRISIS). SHADOW-ONLY: it LOGS the
# would-bench-vs-trade decision to holly_regime_gate_shadow; it does NOT block live trades
# yet. Flip to live (actually bench) only after the shadow log proves it reads regime +
# decides sanely over real sessions — same shadow→eyes-on→promote discipline as the lesson-validator.
def _init_regime_shadow(conn) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS holly_regime_gate_shadow (
               id INTEGER PRIMARY KEY AUTOINCREMENT, checked_at TEXT, fleet_regime TEXT,
               history_regime TEXT, would_bench INTEGER, setups_this_cycle INTEGER, note TEXT)""")
    conn.commit()


def _holly_regime_gate() -> dict:
    """Read regime from BOTH the fleet runtime source (_get_regime_from_8080 — what Ollie uses,
    so Holly's gate AGREES with the fleet) AND the regime_history log (the granular BULL_CROSS/
    CAUTIOUS_BEAR taxonomy the 180-day test segmented on). would_bench if EITHER flags bear/
    crisis — the fleet source returns plain 'CAUTIOUS' and can't distinguish CAUTIOUS_BEAR
    (no edge) from CAUTIOUS_BULL (edge), so the UNION is the conservative, backtest-aligned read."""
    fleet = "UNKNOWN"
    try:
        from engine.crew_scanner import _get_regime_from_8080
        fleet = _get_regime_from_8080() or "UNKNOWN"
    except Exception:
        pass
    hist = "UNKNOWN"
    try:
        c = _conn()
        row = c.execute("SELECT regime FROM regime_history ORDER BY date DESC LIMIT 1").fetchone()
        c.close()
        if row:
            hist = row["regime"]
    except Exception:
        pass
    def _bench(r):
        u = (r or "").upper()
        return ("BEAR" in u) or ("CRISIS" in u)
    return {"fleet": fleet, "history": hist, "would_bench": _bench(fleet) or _bench(hist)}


# ── ENTRY LOOP ────────────────────────────────────────────────────────────────
def holly_scanner_check(dry_run: bool = False) -> list[dict]:
    """Scan the validated universe for fresh works-set setups; open positions. Returns the
    list of actions taken (or intended, if dry_run). FAIL-LOUD on any error.

    REGIME GATE is SHADOW-ONLY: a would-bench-vs-trade decision is logged each cycle to
    holly_regime_gate_shadow, but it does NOT block trades yet (the strategy trades live
    unchanged). Promote to live only after eyes-on of the shadow log."""
    from engine.holly_intraday import HOLLY_WORKS, _fetch_polygon_ohlcv
    from engine.market_data import get_stock_price

    enabled = {k: v for k, v in HOLLY_WORKS.items() if v.get("enabled")}
    if not enabled:
        _notify_fail("HOLLY_WORKS has no enabled strategies")
        return []

    actions: list[dict] = []
    conn = _conn()
    try:
        _init_swing_table(conn)
        _init_regime_shadow(conn)
        gate = _holly_regime_gate()   # SHADOW — read but don't block
        universe = _holly_universe()
        if not universe:
            _notify_fail("empty universe (universe_scan has no $1-50 vol≥2 movers)")
            return []

        for symbol in universe:
            try:
                df = _fetch_polygon_ohlcv(symbol, days=10)
                if df is None or len(df) < 120:
                    continue
                # skip if already holding this symbol for holly
                held = conn.execute(
                    "SELECT COUNT(*) FROM holly_swing_trades WHERE symbol=? AND closed=0",
                    (symbol,)).fetchone()[0]
                if held:
                    continue
                for strat, cfg in enabled.items():
                    if not _fresh_setup(df, strat, symbol):
                        continue
                    if not _passes_selectivity(conn, symbol):
                        continue
                    # live price (fall back to last bar close)
                    try:
                        price = float((get_stock_price(symbol) or {}).get("price") or 0) \
                            or float(df["Close"].iloc[-1])
                    except Exception:
                        price = float(df["Close"].iloc[-1])
                    if price <= 0:
                        continue
                    book = conn.execute(
                        "SELECT cash FROM ai_players WHERE id=?", (PLAYER_ID,)).fetchone()
                    cash = float(book["cash"]) if book else 0.0
                    qty = round((cash * HOLLY_POSITION_PCT) / price, 4)
                    if qty <= 0:
                        continue
                    stop = round(price * (1 - cfg["sl"]), 4)
                    target = round(price * (1 + cfg["tp"]), 4)
                    max_hold_until = (datetime.now(timezone.utc)
                                      + timedelta(days=cfg["max_hold"] // 78)).isoformat()
                    act = {"symbol": symbol, "strategy": strat, "price": price, "qty": qty,
                           "stop": stop, "target": target}
                    if dry_run:
                        act["dry_run"] = True
                        actions.append(act)
                        logger.info("[holly_live][DRY] would BUY %s %s @ %.2f stop=%.2f tgt=%.2f",
                                    symbol, strat, price, stop, target)
                        continue
                    from engine.paper_trader import buy as pt_buy
                    res = pt_buy(PLAYER_ID, symbol, price, qty=qty, timeframe="SWING",
                                 confidence=0.0, reasoning=f"Holly {strat} swing setup",
                                 strategy_id=strat)
                    if res:
                        conn.execute(
                            """INSERT INTO holly_swing_trades
                               (opened_at,symbol,strategy,entry_price,qty,stop_price,
                                target_price,max_hold_until)
                               VALUES (?,?,?,?,?,?,?,?)""",
                            (datetime.now(timezone.utc).isoformat(), symbol, strat, price,
                             qty, stop, target, max_hold_until))
                        conn.commit()
                        actions.append(act)
                        logger.info("[holly_live] BUY %s %s @ %.2f (stop %.2f / tgt %.2f)",
                                    symbol, strat, price, stop, target)
            except Exception as e:
                _notify_fail(f"scan error {symbol}: {type(e).__name__}: {e}")

        # REGIME GATE — SHADOW log (does NOT block; trades above executed live as-is).
        # Records what the gate WOULD do this cycle so it can be verified before going live.
        try:
            verb = "BENCH" if gate["would_bench"] else "TRADE"
            note = (f"regime={verb}: fleet={gate['fleet']} history={gate['history']} | "
                    f"{len(actions)} setup(s) this cycle "
                    f"{'WOULD BE SUPPRESSED' if gate['would_bench'] else 'traded'} (SHADOW — not blocked)")
            conn.execute(
                "INSERT INTO holly_regime_gate_shadow "
                "(checked_at,fleet_regime,history_regime,would_bench,setups_this_cycle,note) "
                "VALUES (?,?,?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(), gate["fleet"], gate["history"],
                 1 if gate["would_bench"] else 0, len(actions), note))
            conn.commit()
            logger.info("[holly_live][REGIME-GATE-SHADOW] %s", note)
        except Exception as e:
            logger.warning("[holly_live] regime-gate shadow log error: %s", e)
    finally:
        conn.close()
    return actions


# ── EXIT LOOP (stop / target / max-hold) ──────────────────────────────────────
def holly_manage_exits(dry_run: bool = False) -> list[dict]:
    """Manage open holly swing positions: exit on stop, target, or max-hold. FAIL-LOUD."""
    from engine.market_data import get_stock_price
    actions: list[dict] = []
    conn = _conn()
    try:
        _init_swing_table(conn)
        opens = conn.execute(
            "SELECT * FROM holly_swing_trades WHERE closed=0").fetchall()
        now = datetime.now(timezone.utc)
        for t in opens:
            try:
                px = float((get_stock_price(t["symbol"]) or {}).get("price") or 0)
                if px <= 0:
                    continue
                reason = None
                if px <= t["stop_price"]:
                    reason = "STOP"
                elif px >= t["target_price"]:
                    reason = "TARGET"
                else:
                    try:
                        if now >= datetime.fromisoformat(t["max_hold_until"]):
                            reason = "MAX_HOLD"
                    except Exception:
                        pass
                if not reason:
                    continue
                act = {"symbol": t["symbol"], "strategy": t["strategy"], "price": px,
                       "reason": reason}
                if dry_run:
                    act["dry_run"] = True
                    actions.append(act)
                    logger.info("[holly_live][DRY] would SELL %s @ %.2f (%s)",
                                t["symbol"], px, reason)
                    continue
                from engine.paper_trader import sell_partial
                res = sell_partial(PLAYER_ID, t["symbol"], px, t["qty"],
                                   reasoning=f"Holly {t['strategy']} swing exit {reason}")
                if res:
                    conn.execute(
                        "UPDATE holly_swing_trades SET closed=1, closed_at=?, exit_price=?, "
                        "exit_reason=? WHERE id=?",
                        (now.isoformat(), px, reason, t["id"]))
                    conn.commit()
                    actions.append(act)
                    logger.info("[holly_live] SELL %s @ %.2f (%s)", t["symbol"], px, reason)
            except Exception as e:
                _notify_fail(f"exit error {t['symbol']}: {type(e).__name__}: {e}")
    finally:
        conn.close()
    return actions


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import sys
    dry = "--live" not in sys.argv
    print(f"=== holly_scanner_check (dry_run={dry}) ===")
    a = holly_scanner_check(dry_run=dry)
    print(f"entry actions: {len(a)} -> {a}")
    print(f"=== holly_manage_exits (dry_run={dry}) ===")
    b = holly_manage_exits(dry_run=dry)
    print(f"exit actions: {len(b)} -> {b}")
