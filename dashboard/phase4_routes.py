from __future__ import annotations
"""
USS TradeMinds — Phase 4 API Routes

Trade execution, deep scan, and strategy rotation endpoints.

POST /execute           — manual trade execution with Troi advisory
GET  /account           — Alpaca paper account info
GET  /positions         — current positions with P&L
POST /close             — close a single position
POST /close-all         — close all positions (requires confirm=True)
GET  /trade-history     — last 50 manual trades

GET  /deep-scan/results   — latest scan results (filterable)
GET  /deep-scan/universe  — universe stats
POST /deep-scan/run       — trigger deep scan in background

GET  /strategy-rotation/latest   — latest rotation record
GET  /strategy-rotation/history  — rotation history (default 14 days)
POST /strategy-rotation/run      — trigger rotation in background

GET  /spx              — SPX levels from ThetaData (or SPY-proxy fallback)
GET  /scanner-status   — combined status for scanner.html
"""

import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from engine.alpaca_bridge import alpaca

router = APIRouter()

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

_DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

_CREATE_MANUAL_TRADES = """
CREATE TABLE IF NOT EXISTS manual_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    price REAL,
    order_type TEXT DEFAULT 'market',
    limit_price REAL,
    troi_signal TEXT,
    troi_multiplier REAL,
    event_shield_status TEXT,
    session_type TEXT,
    condition_score REAL,
    condition TEXT,
    order_id TEXT,
    status TEXT DEFAULT 'submitted',
    closed_at TEXT,
    close_price REAL,
    realized_pnl REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_CREATE_IDX_SYMBOL = "CREATE INDEX IF NOT EXISTS idx_manual_trades_symbol ON manual_trades(symbol);"
_CREATE_IDX_TS = "CREATE INDEX IF NOT EXISTS idx_manual_trades_ts ON manual_trades(timestamp);"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    """Ensure manual_trades table and indexes exist."""
    with _get_conn() as conn:
        conn.execute(_CREATE_MANUAL_TRADES)
        conn.execute(_CREATE_IDX_SYMBOL)
        conn.execute(_CREATE_IDX_TS)
        conn.commit()


_init_db()

# ---------------------------------------------------------------------------
# Background-task guards
# ---------------------------------------------------------------------------

_scanning = False
_scan_lock = threading.Lock()

_rotating = False
_rotate_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Trade Execution Endpoints
# ---------------------------------------------------------------------------


class TradeRequest(BaseModel):
    symbol: str
    side: str
    qty: float
    order_type: str = "market"
    limit_price: Optional[float] = None
    override: bool = False


class CloseRequest(BaseModel):
    symbol: str


class CloseAllRequest(BaseModel):
    confirm: bool = False


@router.post("/execute")
def execute_trade(req: TradeRequest):
    """Execute a manual trade via Alpaca with Counselor Troi advisory and Event Shield check."""
    symbol = req.symbol
    side = req.side
    qty = req.qty
    order_type = req.order_type
    limit_price = req.limit_price
    override = req.override
    try:
        # --- Input validation ---
        if not symbol or not symbol.strip():
            return {"ok": False, "error": "symbol must not be empty"}
        symbol = symbol.strip().upper()

        side_lower = side.strip().lower()
        if side_lower not in ("buy", "sell"):
            return {"ok": False, "error": f"side must be 'buy' or 'sell', got: {side!r}"}

        if qty <= 0:
            return {"ok": False, "error": f"qty must be > 0, got: {qty}"}

        # --- Counselor Troi advisory ---
        troi_signal = None
        troi_reason = None
        troi_multiplier = None
        try:
            from engine.ready_room_advisor import should_i_trade
            advice = should_i_trade(
                symbol=symbol,
                proposed_action=side_lower.upper(),
                player_id="steve-manual",
            )
            troi_signal = advice.get("signal")
            troi_reason = advice.get("reason")
            troi_multiplier = advice.get("multiplier")
        except Exception as troi_err:
            troi_signal = "UNKNOWN"
            troi_reason = f"Troi unavailable: {troi_err}"

        # --- Event Shield ---
        event_shield_status = None
        try:
            from engine.event_shield import get_event_shield_status
            ev = get_event_shield_status()
            event_shield_status = ev.get("status") if isinstance(ev, dict) else str(ev)
        except Exception as ev_err:
            event_shield_status = f"unavailable: {ev_err}"

        # --- STAND_DOWN guard ---
        if troi_signal == "STAND_DOWN" and not override:
            return {
                "ok": False,
                "blocked": True,
                "reason": troi_reason,
                "signal": "STAND_DOWN",
                "can_override": True,
            }

        # --- Execute via Alpaca ---
        if side_lower == "buy":
            result = alpaca.buy(symbol, qty)
        else:
            result = alpaca.sell(symbol, qty)

        order_id = None
        price = None
        if isinstance(result, dict):
            order_id = result.get("id") or result.get("order_id")
            price = result.get("filled_avg_price") or result.get("price")

        # --- Log to DB ---
        now = datetime.now(timezone.utc).isoformat()
        with _get_conn() as conn:
            conn.execute(
                """
                INSERT INTO manual_trades (
                    timestamp, symbol, side, qty, price,
                    order_type, limit_price,
                    troi_signal, troi_multiplier,
                    event_shield_status,
                    order_id, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now, symbol, side_lower, qty, price,
                    order_type, limit_price,
                    troi_signal, troi_multiplier,
                    event_shield_status,
                    order_id, "submitted", now,
                ),
            )
            conn.commit()

        return {
            "ok": True,
            "order_id": order_id,
            "symbol": symbol,
            "side": side_lower,
            "qty": qty,
            "troi_signal": troi_signal,
            "troi_reason": troi_reason,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/account")
def get_account():
    """Return Alpaca paper account summary."""
    try:
        data = alpaca.status()
        if not isinstance(data, dict):
            return {"ok": False, "error": "Unexpected response from alpaca.status()"}
        return {
            "ok": True,
            "equity": data.get("equity"),
            "cash": data.get("cash"),
            "buying_power": data.get("buying_power"),
            "portfolio_value": data.get("portfolio_value"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/positions")
def get_positions():
    """Return current Alpaca positions with unrealized P&L summary."""
    try:
        positions = alpaca.positions()
        if not isinstance(positions, list):
            positions = []
        total_upl = sum(
            float(p.get("unrealized_pl", 0) or 0) for p in positions
        )
        return {
            "ok": True,
            "positions": positions,
            "count": len(positions),
            "total_unrealized_pl": round(total_upl, 4),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/close")
def close_position(req: CloseRequest):
    """Close a single position and mark it closed in manual_trades."""
    symbol = req.symbol
    try:
        symbol = symbol.strip().upper()
        result = alpaca.close_position(symbol)

        now = datetime.now(timezone.utc).isoformat()
        with _get_conn() as conn:
            conn.execute(
                """
                UPDATE manual_trades
                SET status = 'closed', closed_at = ?
                WHERE symbol = ? AND status != 'closed'
                """,
                (now, symbol),
            )
            conn.commit()

        return {"ok": True, "symbol": symbol, "result": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/close-all")
def close_all_positions(req: CloseAllRequest):
    """Close ALL open positions. Requires confirm=True."""
    confirm = req.confirm
    try:
        if not confirm:
            return {
                "ok": False,
                "error": "You must pass confirm=True to close all positions.",
            }
        result = alpaca.close_all()
        return {"ok": True, "result": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/trade-history")
def get_trade_history(limit: int = 50):
    """Return the last N manual trades, most recent first."""
    try:
        limit = min(max(limit, 1), 200)
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM manual_trades ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        trades = [dict(r) for r in rows]
        return {"ok": True, "trades": trades, "count": len(trades)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Deep Scan Endpoints
# ---------------------------------------------------------------------------


@router.get("/deep-scan/results")
def deep_scan_results(limit: int = 50, min_strength: float = 0.0):
    """Return the latest deep scan results, optionally filtered by min_strength."""
    try:
        from engine.deep_scan import get_deep_scan_results
        results = get_deep_scan_results(limit=limit)
        if min_strength > 0.0:
            results = [
                r for r in results
                if float(r.get("strength", 0) or 0) >= min_strength
            ]
        return {"ok": True, "results": results, "count": len(results)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/deep-scan/universe")
def deep_scan_universe():
    """Return deep scan universe stats."""
    try:
        from engine.deep_scan import get_universe_stats
        stats = get_universe_stats()
        return {"ok": True, **stats} if isinstance(stats, dict) else {"ok": True, "stats": stats}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/deep-scan/run")
def deep_scan_run():
    """Trigger a deep scan in the background. Only one scan runs at a time."""
    global _scanning

    with _scan_lock:
        if _scanning:
            return {"ok": False, "message": "Deep scan already running — please wait"}
        _scanning = True

    def _do_scan():
        global _scanning
        try:
            from engine.deep_scan import run_deep_scan
            run_deep_scan()
        except Exception as e:
            from rich.console import Console
            Console().log(f"[red]DeepScan /run error: {e}")
        finally:
            with _scan_lock:
                _scanning = False

    t = threading.Thread(target=_do_scan, daemon=True)
    t.start()
    return {"ok": True, "message": "Deep scan starting in background..."}


# ---------------------------------------------------------------------------
# Strategy Rotation Endpoints
# ---------------------------------------------------------------------------


@router.get("/strategy-rotation/latest")
def strategy_rotation_latest():
    """Return the most recent strategy rotation record."""
    try:
        from engine.strategy_rotator import get_latest_rotation
        result = get_latest_rotation()
        return {"ok": True, "rotation": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/strategy-rotation/history")
def strategy_rotation_history(days: int = 14):
    """Return strategy rotation history for the last N days."""
    try:
        from engine.strategy_rotator import get_rotation_history
        result = get_rotation_history(days=days)
        return {"ok": True, "history": result, "days": days}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/strategy-rotation/run")
def strategy_rotation_run():
    """Trigger nightly strategy rotation in the background. Only one run at a time."""
    global _rotating

    with _rotate_lock:
        if _rotating:
            return {"ok": False, "message": "Strategy rotation already running — please wait"}
        _rotating = True

    def _do_rotate():
        global _rotating
        try:
            from engine.strategy_rotator import run_strategy_rotation
            run_strategy_rotation()
        except Exception as e:
            from rich.console import Console
            Console().log(f"[red]StrategyRotation /run error: {e}")
        finally:
            with _rotate_lock:
                _rotating = False

    t = threading.Thread(target=_do_rotate, daemon=True)
    t.start()
    return {"ok": True, "message": "Strategy rotation running in background..."}


# ---------------------------------------------------------------------------
# SPX Endpoint
# ---------------------------------------------------------------------------


@router.get("/spx")
def get_spx():
    """Return SPX levels from ThetaData, or SPY-proxy fallback if not configured."""
    try:
        from engine.thetadata_spx import get_spx_levels
        return get_spx_levels()
    except Exception as e:
        err_str = str(e).lower()
        if "not configured" in err_str or "unavailable" in err_str or "not found" in err_str:
            return {"available": False, "using": "SPY-proxy"}
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Scanner Page Combined Status
# ---------------------------------------------------------------------------


@router.get("/scanner-status")
def scanner_status():
    """Combined status payload for scanner.html — scan results, universe, strategies, rotation."""
    try:
        from engine.deep_scan import get_deep_scan_results, get_universe_stats
        from engine.strategy_rotator import get_active_strategies, get_latest_rotation

        scan_results = get_deep_scan_results(limit=100)
        universe = get_universe_stats()
        active_strats = get_active_strategies()
        rotation = get_latest_rotation()

        return {
            "ok": True,
            "scan_results": scan_results,
            "universe": universe,
            "active_strategies": active_strats,
            "rotation_summary": rotation,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
