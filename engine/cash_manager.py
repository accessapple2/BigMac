"""
cash_manager.py — Automated cash sweep logic for USS TradeMinds.

Rules:
  SWEEP UP   — cash > high_threshold → buy top-3 scanner BUY picks with excess
  SWEEP DOWN — cash < low_threshold  → sell smallest/lowest-conviction position
  RESERVE    — always keep ≥ reserve floor (default $2 K) — never swept away

Runs on-demand (called by scan cycle or CIC). Max 1 sweep action per hour.

CIC commands:
  "set cash ceiling 25000"  → high_threshold
  "set cash floor 3000"     → low_threshold
  "set cash reserve 2000"   → min_reserve
  "cash rules"              → show current thresholds + last action

API: GET /api/cash/status
DB:  cash_sweeps table (INSERT only, never updated or deleted)
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from typing import Optional

logger = logging.getLogger("cash_manager")

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

# ── Defaults ───────────────────────────────────────────────────────────────────

_DEFAULT_HIGH   = 20_000.0   # SWEEP UP  above this
_DEFAULT_LOW    =  5_000.0   # SWEEP DOWN below this
_DEFAULT_RESERVE =  2_000.0  # never drop below this

_SWEEP_COOLDOWN_SECS = 3_600  # 1 sweep action per hour (fleet-wide)

# Player used to execute sweep buys (Bridge Vote representative)
_SWEEP_PLAYER = "claude-sonnet"


# ── DB ─────────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _init_tables() -> None:
    with _conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS cash_sweeps (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                direction   TEXT    NOT NULL,   -- UP | DOWN | NONE
                cash_before REAL    NOT NULL,
                cash_after  REAL,
                threshold   REAL    NOT NULL,
                action      TEXT    NOT NULL,   -- description
                tickers     TEXT,               -- JSON list of symbols touched
                result      TEXT,               -- OK | SKIPPED | ERROR
                detail      TEXT,
                created_at  TEXT    DEFAULT (datetime('now'))
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS cash_manager_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        db.commit()


# ── Settings ───────────────────────────────────────────────────────────────────

def _get_setting(key: str, default: float) -> float:
    try:
        db = _conn()
        row = db.execute(
            "SELECT value FROM cash_manager_settings WHERE key=?", (key,)
        ).fetchone()
        db.close()
        return float(row["value"]) if row else default
    except Exception:
        return default


def _set_setting(key: str, value: float) -> None:
    with _conn() as db:
        db.execute(
            """INSERT INTO cash_manager_settings (key, value, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                   updated_at=datetime('now')""",
            (key, str(value)),
        )
        db.commit()


def get_thresholds() -> dict:
    _init_tables()
    return {
        "high_threshold": _get_setting("high_threshold", _DEFAULT_HIGH),
        "low_threshold":  _get_setting("low_threshold",  _DEFAULT_LOW),
        "min_reserve":    _get_setting("min_reserve",    _DEFAULT_RESERVE),
    }


def set_threshold(key: str, value: float) -> dict:
    """key: high_threshold | low_threshold | min_reserve"""
    _init_tables()
    valid = {"high_threshold", "low_threshold", "min_reserve"}
    if key not in valid:
        return {"ok": False, "error": f"Unknown key: {key}. Use one of {valid}"}
    if value < 0:
        return {"ok": False, "error": "Value must be non-negative"}
    _set_setting(key, value)
    return {"ok": True, "key": key, "value": value}


# ── Cash helpers ───────────────────────────────────────────────────────────────

def _fleet_cash() -> float:
    """Sum of cash across ALL ai_players (fleet-wide idle cash)."""
    try:
        db = _conn()
        row = db.execute("SELECT SUM(cash) FROM ai_players").fetchone()
        db.close()
        return float(row[0] or 0.0)
    except Exception:
        return 0.0


def _alpaca_cash() -> Optional[float]:
    """Live cash from Alpaca paper account. None if unavailable."""
    try:
        from engine.alpaca_bridge import alpaca
        st = alpaca.status()
        if st.get("connected"):
            return float(st["cash"])
    except Exception:
        pass
    return None


def get_cash_balance() -> dict:
    """Fleet DB cash + live Alpaca cash (if connected)."""
    fleet  = _fleet_cash()
    alpaca = _alpaca_cash()
    return {
        "fleet_cash":    round(fleet, 2),
        "alpaca_cash":   round(alpaca, 2) if alpaca is not None else None,
        "display_cash":  round(alpaca if alpaca is not None else fleet, 2),
        "source":        "alpaca" if alpaca is not None else "db",
    }


# ── Cooldown check ─────────────────────────────────────────────────────────────

def _last_sweep_secs_ago() -> float:
    """Seconds since last non-NONE sweep. Returns large number if none."""
    try:
        db = _conn()
        row = db.execute(
            "SELECT created_at FROM cash_sweeps WHERE direction != 'NONE' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        db.close()
        if not row:
            return 1e9
        last = datetime.fromisoformat(row["created_at"])
        return (datetime.now() - last).total_seconds()
    except Exception:
        return 1e9


# ── Log helper ─────────────────────────────────────────────────────────────────

def _log_sweep(direction: str, cash_before: float, cash_after: Optional[float],
               threshold: float, action: str, tickers: list[str],
               result: str, detail: str = "") -> None:
    import json
    with _conn() as db:
        db.execute(
            """INSERT INTO cash_sweeps
               (direction, cash_before, cash_after, threshold, action,
                tickers, result, detail)
               VALUES (?,?,?,?,?,?,?,?)""",
            (direction, round(cash_before, 2),
             round(cash_after, 2) if cash_after is not None else None,
             threshold, action, json.dumps(tickers), result, detail),
        )
        db.commit()


# ── Top scanner picks ──────────────────────────────────────────────────────────

def _top_buy_picks(limit: int = 3) -> list[dict]:
    """
    Return top-N BUY signals from fast_scan_results ordered by confidence.
    Falls back to scanner signals table if fast_scan_results is empty.
    """
    try:
        db = _conn()
        rows = db.execute(
            """SELECT ticker AS symbol, confidence, price, thesis
               FROM fast_scan_results
               WHERE signal='BUY' AND price > 0
               ORDER BY confidence DESC, created_at DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        db.close()
        if rows:
            return [dict(r) for r in rows]
    except Exception:
        pass
    return []


# ── Smallest conviction position ───────────────────────────────────────────────

def _smallest_conviction_position() -> Optional[dict]:
    """
    Find position with lowest (qty * avg_price) across all non-human players.
    Returns {player_id, symbol, qty, avg_price, market_value}.
    """
    try:
        db = _conn()
        rows = db.execute(
            """SELECT p.player_id, p.symbol, p.qty, p.avg_price,
                      (p.qty * p.avg_price) AS market_value
               FROM positions p
               JOIN ai_players ap ON ap.id = p.player_id
               WHERE p.qty > 0
                 AND p.asset_type = 'stock'
                 AND ap.is_human = 0
               ORDER BY market_value ASC
               LIMIT 1"""
        ).fetchone()
        db.close()
        return dict(rows) if rows else None
    except Exception:
        return None


# ── Core sweep logic ──────────────────────────────────────────────────────────

def run_sweep(dry_run: bool = False) -> dict:
    """
    Evaluate cash thresholds and apply SWEEP UP / SWEEP DOWN if triggered.
    Returns {direction, triggered, result, actions, cash_before, cash_after, ...}
    dry_run=True → compute plan without executing trades.
    """
    _init_tables()
    t         = get_thresholds()
    high      = t["high_threshold"]
    low       = t["low_threshold"]
    reserve   = t["min_reserve"]
    bal       = get_cash_balance()
    cash      = bal["display_cash"]
    actions   = []
    direction = "NONE"
    triggered = False
    result    = "OK"

    # ── Cooldown ──────────────────────────────────────────────────────────────
    secs_ago = _last_sweep_secs_ago()
    if secs_ago < _SWEEP_COOLDOWN_SECS:
        wait = int(_SWEEP_COOLDOWN_SECS - secs_ago)
        return {
            "direction": "NONE", "triggered": False,
            "result": "COOLDOWN",
            "detail": f"Next sweep in {wait}s",
            "cash_before": cash, "cash_after": cash,
            "thresholds": t, "balance": bal, "actions": [],
        }

    # ── SWEEP UP ───────────────────────────────────────────────────────────────
    if cash > high:
        direction  = "UP"
        triggered  = True
        excess     = cash - high          # amount above ceiling
        spendable  = max(0.0, excess)     # don't eat into high threshold
        picks      = _top_buy_picks(3)

        if not picks:
            result = "SKIPPED"
            detail = "No scanner BUY picks available for allocation"
            _log_sweep("UP", cash, None, high, "SWEEP UP — no picks",
                       [], "SKIPPED", detail)
        else:
            per_pick   = round(spendable / len(picks), 2)
            tickers_done = []
            for pick in picks:
                sym   = pick["symbol"]
                price = float(pick.get("price") or 0.0)
                if price <= 0:
                    continue
                qty = round(per_pick / price, 4)
                if qty < 0.01:
                    continue
                act = {
                    "action":    "BUY",
                    "symbol":    sym,
                    "qty":       qty,
                    "price":     price,
                    "value":     round(qty * price, 2),
                    "reason":    f"[CashSweep UP] excess cash ${spendable:.0f} → {sym}",
                    "executed":  False,
                    "result":    "DRY_RUN" if dry_run else "PENDING",
                }
                if not dry_run:
                    try:
                        from engine.paper_trader import buy
                        r = buy(
                            player_id  = _SWEEP_PLAYER,
                            symbol     = sym,
                            price      = price,
                            qty        = qty,
                            reasoning  = act["reason"],
                            confidence = float(pick.get("confidence", 5)) * 10,
                            timeframe  = "SWING",
                        )
                        act["executed"] = r is not None
                        act["result"]   = "OK" if r else "BLOCKED"
                    except Exception as e:
                        act["result"] = f"ERROR: {e}"
                actions.append(act)
                if act.get("executed") or dry_run:
                    tickers_done.append(sym)

            cash_after = cash - sum(a["value"] for a in actions if a.get("executed") or dry_run)
            _log_sweep("UP", cash, cash_after, high,
                       f"SWEEP UP ${spendable:.0f} across {len(picks)} picks",
                       tickers_done, "DRY_RUN" if dry_run else result,
                       f"per_pick=${per_pick:.0f}")

    # ── SWEEP DOWN ────────────────────────────────────────────────────────────
    elif cash < low:
        direction = "DOWN"
        triggered = True
        pos       = _smallest_conviction_position()

        if not pos:
            result = "SKIPPED"
            detail = "No positions available to trim"
            _log_sweep("DOWN", cash, None, low, "SWEEP DOWN — no positions",
                       [], "SKIPPED", detail)
        else:
            sym    = pos["symbol"]
            player = pos["player_id"]
            qty    = float(pos["qty"])
            price  = float(pos["avg_price"])
            mv     = float(pos["market_value"])

            # Only sell enough to reach low_threshold + reserve cushion
            target_raise = low - cash + reserve
            sell_qty     = min(qty, round(target_raise / price, 4)) if price > 0 else qty
            sell_qty     = max(sell_qty, 0.01)

            act = {
                "action":   "SELL",
                "symbol":   sym,
                "player_id": player,
                "qty":      round(sell_qty, 4),
                "price":    price,
                "value":    round(sell_qty * price, 2),
                "reason":   f"[CashSweep DOWN] cash ${cash:.0f} < floor ${low:.0f}",
                "executed": False,
                "result":   "DRY_RUN" if dry_run else "PENDING",
            }
            if not dry_run:
                try:
                    from engine.paper_trader import sell_partial
                    r = sell_partial(
                        player_id = player,
                        symbol    = sym,
                        price     = price,
                        qty       = sell_qty,
                        reasoning = act["reason"],
                    )
                    act["executed"] = r is not None
                    act["result"]   = "OK" if r else "BLOCKED"
                except Exception as e:
                    act["result"] = f"ERROR: {e}"

            actions.append(act)
            cash_after = cash + (act["value"] if act.get("executed") or dry_run else 0)
            _log_sweep("DOWN", cash, cash_after, low,
                       f"SWEEP DOWN — trim {sym} {sell_qty:.4f}sh",
                       [sym], "DRY_RUN" if dry_run else result,
                       f"mv=${mv:.0f} raise=${target_raise:.0f}")

    # ── No action ─────────────────────────────────────────────────────────────
    else:
        _log_sweep("NONE", cash, cash, 0, "No sweep needed", [], "OK")

    return {
        "direction":   direction,
        "triggered":   triggered,
        "result":      result,
        "cash_before": round(cash, 2),
        "cash_after":  round(
            cash - sum(a.get("value", 0) for a in actions if direction == "UP" and (a.get("executed") or dry_run))
            + sum(a.get("value", 0) for a in actions if direction == "DOWN" and (a.get("executed") or dry_run)),
            2,
        ),
        "thresholds":  t,
        "balance":     bal,
        "actions":     actions,
        "dry_run":     dry_run,
    }


# ── Status ─────────────────────────────────────────────────────────────────────

def get_status() -> dict:
    """Full cash status: balance, thresholds, last sweep, next eligible."""
    _init_tables()
    t    = get_thresholds()
    bal  = get_cash_balance()
    cash = bal["display_cash"]
    high = t["high_threshold"]
    low  = t["low_threshold"]
    rsv  = t["min_reserve"]

    # Zone classification
    if cash >= high:
        zone = "HIGH"
        zone_color = "yellow"
    elif cash <= low:
        zone = "LOW"
        zone_color = "red"
    elif cash < rsv:
        zone = "CRITICAL"
        zone_color = "red"
    else:
        zone = "OK"
        zone_color = "green"

    # Last sweep
    try:
        db = _conn()
        row = db.execute(
            "SELECT direction, action, result, created_at FROM cash_sweeps "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_sweep = dict(row) if row else None
        db.close()
    except Exception:
        last_sweep = None

    secs_ago = _last_sweep_secs_ago()
    cooldown_remaining = max(0, int(_SWEEP_COOLDOWN_SECS - secs_ago))

    return {
        "cash":                round(cash, 2),
        "balance":             bal,
        "zone":                zone,
        "zone_color":          zone_color,
        "thresholds":          t,
        "sweep_eligible":      cooldown_remaining == 0,
        "cooldown_remaining_s": cooldown_remaining,
        "last_sweep":          last_sweep,
        "checked_at":          datetime.now().isoformat()[:19],
    }


def get_sweep_log(limit: int = 20) -> list[dict]:
    _init_tables()
    db = _conn()
    rows = db.execute(
        "SELECT id, direction, cash_before, cash_after, threshold, action, "
        "tickers, result, detail, created_at "
        "FROM cash_sweeps ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]
