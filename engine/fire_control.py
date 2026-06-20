"""
engine/fire_control.py — OllieTrades tactical fire-control backend
==================================================================

Two pieces, mounted on your existing SwingDesk FastAPI (port 8889):

  tradeability_gate(symbol)  — the interlock. False for halted / delisted /
                               taken-private / non-tradable names. Importable so
                               the SCANNERS call it too, before a signal ever
                               reaches a weapon station (stops the HOLX/INHD ghosts).

  POST /api/fire             — the trigger. Submits an Alpaca PAPER order and
                               nothing else. Schwab / live are refused HERE,
                               server-side, so editing the cockpit HTML can't
                               bypass RULE #1.

SACRED RULES, enforced in code (not just comments):
  1. PAPER ONLY. TradingClient(paper=True) is hard-coded. LIVE is disabled.
  2. SCHWAB IS DISPLAY-ONLY. Any broker but alpaca_paper is refused with 403.
  3. APPEND-ONLY. The fire tape is appended; nothing is deleted/dropped.

Mount it:
    from engine.fire_control import router as fire_router
    app.include_router(fire_router)
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

# ---------------------------------------------------------------------------
# HARD SAFETY CONSTANTS — do not flip without an explicit, deliberate decision.
# ---------------------------------------------------------------------------
LIVE_TRADING_DISABLED = True          # paper only, always
ALLOWED_BROKERS = {"alpaca_paper", "alpaca"}   # everything else is refused
SCHWAB_REFUSAL = "Schwab is display-only. No order can transmit (RULE #1)."

OT_ROOT   = Path(os.getenv("OT_ROOT", Path(__file__).resolve().parent.parent))
FIRE_TAPE = Path(os.getenv("OT_FIRE_TAPE", OT_ROOT / "data" / "alerts" / "fire_tape.jsonl"))

# Names that are gone (taken private / delisted) but a stale universe may still
# surface. Maintain this set; the live Alpaca check below catches the rest.
DELISTED_OR_PRIVATE = {
    "HOLX",   # taken private by Blackstone/TPG, Apr 7 2026
}

router = APIRouter()


# ---------------------------------------------------------------------------
# Alpaca paper client (lazy; paper=True is not negotiable)
# ---------------------------------------------------------------------------
def _trading_client():
    from alpaca.trading.client import TradingClient
    return TradingClient(
        os.environ["ALPACA_API_KEY"],
        os.environ["ALPACA_SECRET_KEY"],
        paper=True,                    # RULE #1 at the wire
    )


# ===========================================================================
# THE INTERLOCK
# ===========================================================================
class Tradeability:
    def __init__(self, ok: bool, reason: str = "", flags: Optional[dict] = None):
        self.ok = ok
        self.reason = reason
        self.flags = flags or {}

    def as_dict(self):
        return {"ok": self.ok, "reason": self.reason, **self.flags}


def tradeability_gate(symbol: str, client=None) -> Tradeability:
    """Return Tradeability(ok=False, reason=...) for any contact that must not
    arm a weapon: taken-private/delisted, inactive, halted, or non-tradable.

    Call this from the scanners BEFORE emitting a signal, and again here before
    a fire. Cheap to run; Alpaca is the live source of truth for status."""
    sym = (symbol or "").strip().upper()
    if not sym:
        return Tradeability(False, "empty symbol")

    if sym in DELISTED_OR_PRIVATE:
        return Tradeability(False, "delisted / taken private", {"delisted": True})

    # Live status from Alpaca (the broker you actually route to).
    try:
        client = client or _trading_client()
        asset = client.get_asset(sym)
    except Exception as e:
        # Unknown to the broker → cannot trade it. Fail closed.
        return Tradeability(False, f"asset not found / lookup failed: {e}", {"unknown": True})

    status = str(getattr(asset, "status", "")).lower()
    tradable = bool(getattr(asset, "tradable", False))
    # alpaca-py exposes halt info on some asset payloads via attributes
    halted = bool(getattr(asset, "trading_halted", False)) \
        or ("halt" in [str(a).lower() for a in (getattr(asset, "attributes", None) or [])])

    if "active" not in status:
        return Tradeability(False, f"inactive ({status or 'unknown'})", {"inactive": True})
    if halted:
        return Tradeability(False, "trading halted", {"halted": True})
    if not tradable:
        return Tradeability(False, "not tradable on Alpaca", {"non_tradable": True})

    return Tradeability(True, "clear", {
        "fractionable": bool(getattr(asset, "fractionable", False)),
        "shortable": bool(getattr(asset, "shortable", False)),
    })


@router.get("/api/tradeable/{symbol}")
def check_tradeable(symbol: str):
    """The cockpit's La Forge interlock calls this to decide if a station may lock."""
    return tradeability_gate(symbol).as_dict()


# ===========================================================================
# THE TRIGGER
# ===========================================================================
class FireRequest(BaseModel):
    station: str
    symbol: str
    side: str            # LONG | SHORT
    qty: object          # int for equity; "2x VERT" etc. routes to SwingDesk
    broker: str = "alpaca_paper"
    entry: Optional[float] = None
    stop: Optional[float] = None
    target: Optional[float] = None

    @field_validator("symbol")
    @classmethod
    def up(cls, v): return v.strip().upper()


def _append_tape(rec: dict) -> None:
    rec["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    FIRE_TAPE.parent.mkdir(parents=True, exist_ok=True)
    with FIRE_TAPE.open("a") as f:           # append-only; never rewrite
        f.write(json.dumps(rec) + "\n")


@router.post("/api/fire")
def fire(req: FireRequest):
    # --- RULE #1: refuse anything that isn't Alpaca paper, server-side ---
    if LIVE_TRADING_DISABLED and req.broker not in ALLOWED_BROKERS:
        _append_tape({"event": "REFUSED", "reason": SCHWAB_REFUSAL,
                      "station": req.station, "symbol": req.symbol, "broker": req.broker})
        raise HTTPException(status_code=403, detail=SCHWAB_REFUSAL)

    # --- interlock: no firing on a ghost ---
    gate = tradeability_gate(req.symbol)
    if not gate.ok:
        _append_tape({"event": "REFUSED", "reason": f"interlock: {gate.reason}",
                      "station": req.station, "symbol": req.symbol})
        raise HTTPException(status_code=409, detail=f"Interlock: {req.symbol} {gate.reason}")

    # --- options / multi-leg go to the SwingDesk spread builder, not here ---
    try:
        qty = int(req.qty)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422,
            detail="Non-integer qty (e.g. a spread). Route options through the SwingDesk builder.")

    # --- submit the PAPER order ---
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce
    side = OrderSide.BUY if req.side.upper() == "LONG" else OrderSide.SELL
    try:
        client = _trading_client()
        order = client.submit_order(MarketOrderRequest(
            symbol=req.symbol, qty=qty, side=side, time_in_force=TimeInForce.DAY,
        ))
    except Exception as e:
        _append_tape({"event": "ERROR", "reason": str(e),
                      "station": req.station, "symbol": req.symbol})
        raise HTTPException(status_code=502, detail=f"Alpaca paper rejected the order: {e}")

    rec = {"event": "RELEASE", "broker": "alpaca_paper", "station": req.station,
           "symbol": req.symbol, "side": req.side, "qty": qty,
           "entry": req.entry, "stop": req.stop, "target": req.target,
           "order_id": str(order.id), "order_status": str(order.status)}
    _append_tape(rec)
    return rec
