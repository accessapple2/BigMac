"""
Battle Station 0DTE — Rules-based SPY 0DTE options agent.
No Ollama. Pure math. Fires every 2 minutes during market hours.

Entry rules:
  CALL: SPY within 0.3% of put wall + momentum flipping positive + VIX < 30 + no open position
  PUT:  SPY within 0.3% of call wall + momentum flipping negative + VIX < 30 + no open position

Exit rules (checked every 2 min):
  Stop loss:   P&L <= -30% from entry price
  Take profit: P&L >= +50% from entry price
  Time stop:   After 2:30 PM ET (close position, don't hold into close)
  Momentum reverse: trend_score reverses 20+ points against position direction

Guardrails:
  Max 2 trades per day
  Max 1 open position at a time
  Only runs 9:45 AM - 2:30 PM ET on market days

Table: battle_station_trades (sacred — never drop)
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import datetime, timezone, date
from typing import Any

logger = logging.getLogger("battle_station_0dte")

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

PLAYER_ID   = "dayblade-0dte"
MAX_DAILY   = 2
STOP_PCT    = -0.30   # initial stop: -30%
TARGET_PCT  = 0.50    # full exit target: +50%
PROXIMITY   = 0.003   # 0.3% from wall to trigger
VIX_MAX     = 30.0
MOMENTUM_FLIP_THRESHOLD = 5
MOMENTUM_REVERSE_POINTS = 20

# Tiered exits — raise stop floor as position profits (item 12)
# [(pnl_milestone, new_stop_floor)] — checked in descending pnl order
_TIER_STOPS: list[tuple[float, float]] = [
    (0.35, 0.15),   # at +35%: floor stop at +15%
    (0.20, 0.05),   # at +20%: floor stop at +5% (break-even+)
]

# Rolling state for flip detection
_prior: dict[str, Any] = {"trend": None, "vix": None, "ts": 0}
_STATE_MAX_AGE = 300  # 5 min


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _ensure_table() -> None:
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS battle_station_trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp    TEXT NOT NULL,
                symbol       TEXT NOT NULL DEFAULT 'SPY',
                option_type  TEXT NOT NULL,
                strike       REAL,
                expiry       TEXT,
                entry_price  REAL,
                exit_price   REAL,
                entry_reason TEXT,
                exit_reason  TEXT,
                pnl          REAL,
                pnl_pct      REAL,
                spy_at_entry REAL,
                spy_at_exit  REAL,
                put_wall     REAL,
                call_wall    REAL,
                vix          REAL,
                momentum     REAL,
                status       TEXT NOT NULL DEFAULT 'OPEN'
            )
        """)
        c.commit()
    finally:
        c.close()


_initialized = False

def _init() -> None:
    global _initialized
    if not _initialized:
        _ensure_table()
        _initialized = True


# ── Data helpers ─────────────────────────────────────────────────────────────

def _get_spy_price() -> float:
    try:
        from engine.market_data import get_stock_price
        return float(get_stock_price("SPY").get("price") or 0)
    except Exception:
        return 0.0


def _get_key_levels() -> dict:
    try:
        from engine.ready_room import get_key_levels
        return get_key_levels()
    except Exception:
        return {}


def _get_vix() -> float:
    try:
        from engine.ready_room import get_latest_briefing
        return float((get_latest_briefing() or {}).get("vix") or 0)
    except Exception:
        return 0.0


def _get_momentum() -> float:
    try:
        from engine.momentum_tracker import get_intraday_momentum
        return float((get_intraday_momentum("SPY") or {}).get("trend_score") or 0)
    except Exception:
        return 0.0


def _get_troi() -> str:
    try:
        from engine.ready_room_advisor import should_i_trade
        return should_i_trade("SPY", "BUY", "battle_station_0dte").get("signal", "GO")
    except Exception:
        return "GO"


def _get_option_price(option_type: str) -> tuple[float, str, float]:
    """Returns (premium, contract_symbol, strike). 0.0 on failure."""
    try:
        from engine.alpaca_options import get_atm_contract, _get_contract_price
        contract = get_atm_contract("SPY", option_type, target_dte=0)
        if not contract:
            return 0.0, "", 0.0
        price = _get_contract_price(contract) or 0.0
        # Parse strike from OCC symbol: SPY250402C00655000 → 655.0
        import re
        m = re.search(r'[CP](\d{8})$', contract)
        strike = int(m.group(1)) / 1000.0 if m else 0.0
        return price, contract, strike
    except Exception:
        return 0.0, "", 0.0


# ── DB queries ────────────────────────────────────────────────────────────────

def _open_position() -> dict | None:
    """Return the current open position row, or None."""
    c = _conn()
    try:
        row = c.execute(
            "SELECT * FROM battle_station_trades WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None
    finally:
        c.close()


def _trades_today() -> int:
    today = date.today().isoformat()
    c = _conn()
    try:
        row = c.execute(
            "SELECT COUNT(*) FROM battle_station_trades WHERE date(timestamp)=? AND status != 'OPEN'",
            (today,)
        ).fetchone()
        return row[0] if row else 0
    finally:
        c.close()


def _open_trade(option_type: str, strike: float, expiry: str, entry_price: float,
                spy: float, put_wall: float, call_wall: float, vix: float,
                momentum: float, reason: str) -> int:
    ts = datetime.now(timezone.utc).isoformat()
    c = _conn()
    try:
        cur = c.execute(
            """INSERT INTO battle_station_trades
               (timestamp, symbol, option_type, strike, expiry, entry_price,
                entry_reason, spy_at_entry, put_wall, call_wall, vix, momentum, status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ts, "SPY", option_type, strike, expiry, entry_price, reason,
             spy, put_wall, call_wall, vix, momentum, "OPEN"),
        )
        c.commit()
        return cur.lastrowid
    finally:
        c.close()


def _close_trade(trade_id: int, exit_price: float, spy: float, reason: str, status: str) -> None:
    c = _conn()
    try:
        row = c.execute(
            "SELECT entry_price FROM battle_station_trades WHERE id=?", (trade_id,)
        ).fetchone()
        if not row:
            return
        entry = float(row[0] or 0)
        pnl = (exit_price - entry) * 100  # 1 contract = 100 shares
        pnl_pct = (exit_price - entry) / entry if entry > 0 else 0
        c.execute(
            """UPDATE battle_station_trades
               SET exit_price=?, spy_at_exit=?, exit_reason=?, pnl=?, pnl_pct=?, status=?
               WHERE id=?""",
            (exit_price, spy, reason, round(pnl, 2), round(pnl_pct, 4), status, trade_id)
        )
        c.commit()
    finally:
        c.close()


# ── Core scan ────────────────────────────────────────────────────────────────

def scan() -> dict[str, Any]:
    """
    Main 2-minute scan. Returns status dict.
    """
    global _prior
    _init()

    now_ts  = time.time()
    spy     = _get_spy_price()
    vix     = _get_vix()
    trend   = _get_momentum()
    levels  = _get_key_levels()
    troi    = _get_troi()

    put_wall  = float(levels.get("put_wall")  or 0)
    call_wall = float(levels.get("call_wall") or 0)
    gamma_flip = float(levels.get("gamma_flip") or 0)

    prior_trend = _prior.get("trend")
    prior_vix   = _prior.get("vix")
    prior_age   = now_ts - _prior.get("ts", 0)
    _prior = {"trend": trend, "vix": vix, "ts": now_ts}

    result: dict[str, Any] = {
        "action": "MONITOR",
        "spy": spy,
        "vix": vix,
        "trend": trend,
        "put_wall": put_wall,
        "call_wall": call_wall,
        "gamma_flip": gamma_flip,
        "troi": troi,
        "reason": "",
    }

    # ── Check open position first ─────────────────────────────────────────────
    pos = _open_position()
    if pos:
        entry_price = float(pos["entry_price"] or 0)
        if entry_price > 0:
            current_price, _, _ = _get_option_price(pos["option_type"])
            if current_price > 0:
                pnl_pct = (current_price - entry_price) / entry_price

                # Dynamic trailing stop floor (item 12)
                dynamic_stop = STOP_PCT
                for milestone, floor in sorted(_TIER_STOPS, key=lambda x: -x[0]):
                    if pnl_pct >= milestone:
                        dynamic_stop = max(dynamic_stop, floor)
                        break

                # Stop / trailing stop
                if pnl_pct <= dynamic_stop:
                    stop_label = "Trailing stop" if dynamic_stop > STOP_PCT else "Stop loss"
                    _close_trade(pos["id"], current_price, spy,
                                 f"{stop_label} hit: {pnl_pct*100:.1f}% (floor {dynamic_stop*100:.0f}%)",
                                 "CLOSED_LOSS" if pnl_pct <= 0 else "CLOSED_PROFIT")
                    logger.info(f"[0DTE] {stop_label}: {pos['option_type']} @ ${current_price:.2f} ({pnl_pct*100:.1f}%)")
                    result["action"] = "STOP_LOSS" if pnl_pct <= 0 else "TRAILING_STOP"
                    result["reason"] = f"{stop_label} at {pnl_pct*100:.1f}% (floor {dynamic_stop*100:.0f}%)"
                    return result

                # Full take profit
                if pnl_pct >= TARGET_PCT:
                    _close_trade(pos["id"], current_price, spy,
                                 f"Target hit: {pnl_pct*100:.1f}%", "CLOSED_PROFIT")
                    logger.info(f"[0DTE] Target hit: {pos['option_type']} @ ${current_price:.2f} ({pnl_pct*100:.1f}%)")
                    result["action"] = "TAKE_PROFIT"
                    result["reason"] = f"Target hit at {pnl_pct*100:.1f}%"
                    return result

                # Momentum reversal
                if pos["option_type"] == "call" and trend < -MOMENTUM_REVERSE_POINTS:
                    _close_trade(pos["id"], current_price, spy,
                                 f"Momentum reversed bearish (score {trend:.0f})", "CLOSED_LOSS")
                    result["action"] = "MOMENTUM_REVERSAL"
                    result["reason"] = f"Momentum reversed vs call position (score {trend:.0f})"
                    return result
                if pos["option_type"] == "put" and trend > MOMENTUM_REVERSE_POINTS:
                    _close_trade(pos["id"], current_price, spy,
                                 f"Momentum reversed bullish (score {trend:.0f})", "CLOSED_LOSS")
                    result["action"] = "MOMENTUM_REVERSAL"
                    result["reason"] = f"Momentum reversed vs put position (score {trend:.0f})"
                    return result

        result["action"] = "HOLDING"
        result["reason"] = f"Position open: SPY {pos['option_type'].upper()} — monitoring"
        return result

    # ── No open position — check entry ───────────────────────────────────────
    if troi == "STAND_DOWN":
        result["reason"] = "Troi STAND_DOWN — no entry"
        return result

    if vix >= VIX_MAX:
        result["reason"] = f"VIX {vix:.1f} >= {VIX_MAX} — no entry in crisis"
        return result

    if _trades_today() >= MAX_DAILY:
        result["reason"] = f"Max {MAX_DAILY} trades/day reached"
        return result

    if spy <= 0:
        result["reason"] = "SPY price unavailable"
        return result

    valid_prior = prior_trend is not None and prior_age < _STATE_MAX_AGE

    # CALL entry: SPY near put wall, momentum flipping up
    if put_wall > 0:
        dist = abs(spy - put_wall) / spy
        if dist <= PROXIMITY:
            momentum_up = trend > MOMENTUM_FLIP_THRESHOLD and (
                not valid_prior or prior_trend <= MOMENTUM_FLIP_THRESHOLD
            )
            vix_ok = not valid_prior or (prior_vix is not None and prior_vix - vix >= 0.2)
            if momentum_up and vix_ok:
                price, contract, strike = _get_option_price("call")
                if price > 0:
                    reason = (
                        f"Put wall bounce: SPY ${spy:.2f} at put wall ${put_wall:.0f} "
                        f"({dist*100:.2f}% away). Momentum {prior_trend:.0f}→{trend:.0f}. "
                        f"VIX {vix:.1f}. BUY CALL stop -30% target +50% max 45min"
                    )
                    _open_trade("call", strike, date.today().isoformat(), price,
                                spy, put_wall, call_wall, vix, trend, reason)
                    logger.info(f"[0DTE] ENTRY BUY CALL @ ${price:.2f}. {reason}")
                    result["action"] = "BUY_CALL"
                    result["reason"] = reason
                    return result

    # PUT entry: SPY near call wall, momentum flipping down
    if call_wall > 0:
        dist = abs(spy - call_wall) / spy
        if dist <= PROXIMITY:
            momentum_down = trend < -MOMENTUM_FLIP_THRESHOLD and (
                not valid_prior or prior_trend >= -MOMENTUM_FLIP_THRESHOLD
            )
            if momentum_down:
                # Volume spike check: sell volume dominant
                volume_spike = False
                try:
                    from engine.momentum_tracker import get_intraday_momentum
                    m = get_intraday_momentum("SPY")
                    buy_vol  = float(m.get("buy_volume",  0) or 0)
                    sell_vol = float(m.get("sell_volume", 0) or 0)
                    total    = buy_vol + sell_vol
                    volume_spike = total > 0 and sell_vol / total > 0.60
                except Exception:
                    pass
                if volume_spike:
                    price, contract, strike = _get_option_price("put")
                    if price > 0:
                        reason = (
                            f"Call wall rejection: SPY ${spy:.2f} at call wall ${call_wall:.0f} "
                            f"({dist*100:.2f}% away). Momentum {prior_trend:.0f}→{trend:.0f}. "
                            f"Sell volume dominant. BUY PUT stop -30% target +50% max 45min"
                        )
                        _open_trade("put", strike, date.today().isoformat(), price,
                                    spy, put_wall, call_wall, vix, trend, reason)
                        logger.info(f"[0DTE] ENTRY BUY PUT @ ${price:.2f}. {reason}")
                        result["action"] = "BUY_PUT"
                        result["reason"] = reason
                        return result

    # No trigger
    parts = []
    if put_wall > 0:
        parts.append(f"put wall ${put_wall:.0f} ({abs(spy-put_wall)/spy*100:.1f}% away)")
    if call_wall > 0:
        parts.append(f"call wall ${call_wall:.0f} ({abs(spy-call_wall)/spy*100:.1f}% away)")
    result["reason"] = (
        f"Armed — no trigger. SPY ${spy:.2f} | trend {trend:+.0f} | VIX {vix:.1f} | "
        + (", ".join(parts) if parts else "GEX levels unavailable")
    )
    return result


# ── Dashboard helpers ─────────────────────────────────────────────────────────

def get_status() -> dict[str, Any]:
    """Full status for the dashboard API."""
    _init()
    pos = _open_position()
    today_count = _trades_today()

    levels = {}
    try:
        from engine.ready_room import get_key_levels
        levels = get_key_levels()
    except Exception:
        pass

    spy  = _get_spy_price()
    vix  = _get_vix()

    put_wall  = float(levels.get("put_wall")  or 0)
    call_wall = float(levels.get("call_wall") or 0)
    gamma_flip = float(levels.get("gamma_flip") or 0)

    # Proximity to walls
    dist_put  = abs(spy - put_wall)  / spy * 100 if put_wall  and spy else None
    dist_call = abs(spy - call_wall) / spy * 100 if call_wall and spy else None

    if pos:
        arm_status = "IN_POSITION"
        entry = float(pos["entry_price"] or 0)
        current, _, _ = _get_option_price(pos["option_type"])
        pnl_pct = (current - entry) / entry if entry > 0 and current > 0 else None
        position_info = {
            "option_type": pos["option_type"],
            "strike": pos["strike"],
            "entry_price": entry,
            "current_price": current,
            "pnl_pct": round(pnl_pct * 100, 1) if pnl_pct is not None else None,
            "entry_reason": pos["entry_reason"],
            "stop_price": round(entry * (1 + STOP_PCT), 2),
            "target_price": round(entry * (1 + TARGET_PCT), 2),
        }
    else:
        arm_status = "ARMED" if today_count < MAX_DAILY else "STAND_DOWN"
        position_info = None

    return {
        "status":         arm_status,
        "spy_price":      spy,
        "vix":            vix,
        "put_wall":       put_wall,
        "call_wall":      call_wall,
        "gamma_flip":     gamma_flip,
        "dist_put_pct":   round(dist_put, 2) if dist_put is not None else None,
        "dist_call_pct":  round(dist_call, 2) if dist_call is not None else None,
        "trades_today":   today_count,
        "max_daily":      MAX_DAILY,
        "position":       position_info,
        "stop_pct":       abs(STOP_PCT) * 100,
        "target_pct":     TARGET_PCT * 100,
        "proximity_pct":  PROXIMITY * 100,
    }


def get_history(limit: int = 20) -> list[dict]:
    """Return recent closed trades."""
    _init()
    c = _conn()
    try:
        rows = c.execute(
            """SELECT id, timestamp, option_type, strike, entry_price, exit_price,
                      pnl, pnl_pct, entry_reason, exit_reason, status,
                      spy_at_entry, spy_at_exit, put_wall, call_wall, vix, momentum
               FROM battle_station_trades
               ORDER BY id DESC LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        c.close()
