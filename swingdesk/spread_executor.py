"""
SwingDesk W2 — spread builder → Alpaca multi-leg (mleg) PAPER execution.

Wires a defined-risk vertical spread to Alpaca **paper** as a single atomic
multi-leg LIMIT order (net-debit at mid, reject above width × max_ratio), and
records the fill in data/trader.db `options_trades`.

HARD CONSTRAINTS (do not relax):
  * Alpaca PAPER only. `paper=True` is hardcoded. Schwab is RULE #1 hands-off —
    there is no Schwab path in this module, by construction.
  * Persistence reuses the EXISTING `options_trades.legs_json` (JSON array) and
    `strategy_id` (strategy name) columns — no schema change, no duplicate cols.
    Single-leg CSP rows are untouched.
  * Manual-trigger only: this module submits nothing on its own. It is invoked
    exclusively by the explicit POST /api/swingdesk/spread/submit endpoint.

Canonical patterns reused verbatim:
  * paper client  — engine/alpaca_options.py::_get_client (APCA_* env, paper=True)
  * OCC symbol    — strategies/executor.py::_occ_symbol  ({u}{yy}{mm}{dd}{C/P}{strike*1000:08d})
  * mleg order    — alpaca.trading.requests.LimitOrderRequest(order_class=MLEG, legs=[OptionLegRequest])
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_REPO = Path(__file__).resolve().parent.parent
_DB = _REPO / "data" / "trader.db"

# Reject any spread whose net debit exceeds width × this ratio (default 0.5).
MAX_DEBIT_RATIO = float(os.getenv("SWINGDESK_MAX_DEBIT_RATIO", "0.5"))
# Manual-spread provenance tag written to options_trades.agent_id / strategy_id.
MANUAL_AGENT_ID = "swingdesk-manual"


# ── Alpaca paper client (RULE #1: paper only) ────────────────────────────────
_client = None


def _creds() -> tuple[str, str]:
    """(key, secret) for Alpaca. Load .env by EXPLICIT path — bare load_dotenv()
    frame-walks and crashes under `python -`/heredoc; the explicit path is robust
    whether run as a file (live swingdesk) or a subprocess (tests)."""
    key, secret = os.getenv("APCA_API_KEY_ID", ""), os.getenv("APCA_API_SECRET_KEY", "")
    if key and secret:
        return key, secret
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=str(_REPO / ".env"))
    except Exception:
        pass
    return os.getenv("APCA_API_KEY_ID", ""), os.getenv("APCA_API_SECRET_KEY", "")


def _get_paper_client():
    """Paper TradingClient. Returns None if creds absent (caller skips)."""
    global _client
    if _client is not None:
        return _client
    try:
        key, secret = _creds()
        if not key or not secret:
            return None
        from alpaca.trading.client import TradingClient
        _client = TradingClient(key, secret, paper=True)  # PAPER — RULE #1
    except Exception:
        return None
    return _client


# ── OCC symbol (canonical format, matches strategies/executor.py::_occ_symbol) ─
def occ_symbol(underlying: str, expiration: str, option_type: str, strike: float) -> str:
    """Build an OCC option symbol, e.g. SPY250425C00700000."""
    from datetime import date
    exp = date.fromisoformat(expiration)
    cp = "C" if str(option_type).lower().startswith("c") else "P"
    strike_int = int(round(float(strike) * 1000))
    return f"{underlying.upper()}{exp.strftime('%y%m%d')}{cp}{strike_int:08d}"


# ── resolve target strikes → REAL tradable Alpaca contracts (one shared expiry) ─
def resolve_vertical(underlying: str, option_type: str, target_strikes: list[float],
                     dte_band: tuple[int, int] = (28, 48)) -> dict:
    """Map target strikes to real tradable contracts that share ONE expiry.

    Blind OCC construction can name non-existent assets (Alpaca rejects "asset not
    found"); this queries the live chain and snaps each target to the nearest real
    tradable strike within a single expiration. Returns {expiration, legs:[{strike,
    symbol}]} aligned to target_strikes order, or {error}.
    """
    client = _get_paper_client()
    if client is None:
        return {"error": "no paper client (creds?)"}
    try:
        from alpaca.trading.requests import GetOptionContractsRequest
        from alpaca.trading.enums import ContractType
        from datetime import date, timedelta
        from collections import defaultdict
        ctype = ContractType.CALL if str(option_type).lower().startswith("c") else ContractType.PUT
        today = date.today()
        req = GetOptionContractsRequest(
            underlying_symbols=[underlying.upper()], type=ctype,
            expiration_date_gte=(today + timedelta(days=dte_band[0])).isoformat(),
            expiration_date_lte=(today + timedelta(days=dte_band[1])).isoformat(),
            limit=1000,
        )
        contracts = [c for c in client.get_option_contracts(req).option_contracts if c.tradable]
        if not contracts:
            return {"error": f"no tradable {underlying} {option_type} contracts in {dte_band} DTE"}
        by_exp: dict = defaultdict(list)
        for c in contracts:
            by_exp[str(c.expiration_date)].append(c)
        # pick the expiry whose strike grid best covers BOTH targets
        def coverage(cs):
            ks = [float(x.strike_price) for x in cs]
            return sum(min(abs(k - t) for k in ks) for t in target_strikes)
        best_exp = min(by_exp, key=lambda e: coverage(by_exp[e]))
        pool = by_exp[best_exp]
        legs = []
        for t in target_strikes:
            c = min(pool, key=lambda x: abs(float(x.strike_price) - t))
            legs.append({"strike": float(c.strike_price), "symbol": c.symbol})
        return {"expiration": best_exp, "legs": legs}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ── leg normalization ────────────────────────────────────────────────────────
def _normalize_leg(leg: dict) -> dict:
    """Normalize a builder/endpoint leg into {occ, side, option_type, strike, expiration}.

    Accepts either a prebuilt `occ`/`symbol`, or `underlying`+`expiration`+
    `option_type`+`strike`. `side` is "long"|"short" (or "buy"|"sell").
    """
    side = str(leg.get("side", "")).lower()
    if side in ("buy", "long"):
        side = "long"
    elif side in ("sell", "short"):
        side = "short"
    else:
        raise ValueError(f"leg side must be long/short (got {leg.get('side')!r})")
    occ = leg.get("occ") or leg.get("symbol")
    underlying = (leg.get("underlying") or leg.get("ticker") or "").upper()
    option_type = str(leg.get("option_type") or leg.get("type") or "").lower()
    option_type = "call" if option_type.startswith("c") else "put"
    strike = leg.get("strike")
    expiration = leg.get("expiration")
    if not occ:
        if underlying and expiration and strike is not None:
            occ = occ_symbol(underlying, expiration, option_type, strike)
        elif underlying and strike is not None:
            occ = None  # deferred — resolve_vertical() snaps to a real contract + expiry
        else:
            raise ValueError("leg needs occ/symbol, or underlying+strike (+optional expiration)")
    return {
        "occ": occ, "side": side, "option_type": option_type,
        "strike": float(strike) if strike is not None else None,
        "expiration": expiration, "underlying": underlying,
    }


# ── net-debit validation (defined-risk vertical) ─────────────────────────────
def spread_width(legs: list[dict]) -> Optional[float]:
    """|long_strike − short_strike| for a 2-leg vertical; None if strikes absent."""
    strikes = [l["strike"] for l in legs if l.get("strike") is not None]
    if len(strikes) < 2:
        return None
    return abs(max(strikes) - min(strikes))


def validate_debit(legs: list[dict], net_debit: float,
                   max_ratio: float = MAX_DEBIT_RATIO) -> dict:
    """Reject if net_debit > width × max_ratio. Returns {ok, width, ceiling, reason}."""
    width = spread_width(legs)
    if width is None or width <= 0:
        return {"ok": False, "reason": "cannot determine spread width from legs"}
    ceiling = round(width * max_ratio, 4)
    if net_debit is None:
        return {"ok": False, "reason": "net_debit_limit unknown (no quote, none supplied)"}
    if net_debit > ceiling:
        return {"ok": False, "width": width, "ceiling": ceiling,
                "reason": f"net debit {net_debit} > width×{max_ratio} ceiling {ceiling}"}
    return {"ok": True, "width": width, "ceiling": ceiling, "reason": "within ceiling"}


# ── mid net-debit from Alpaca option quotes (best-effort) ────────────────────
def leg_mid(occ: str) -> Optional[float]:
    """Latest option mid (bid+ask)/2 from Alpaca, or None if unavailable."""
    try:
        from alpaca.data.historical.option import OptionHistoricalDataClient
        from alpaca.data.requests import OptionLatestQuoteRequest
        key, secret = _creds()
        if not key or not secret:
            return None
        dc = OptionHistoricalDataClient(key, secret)
        q = dc.get_option_latest_quote(OptionLatestQuoteRequest(symbol_or_symbols=occ))
        quote = q.get(occ)
        if not quote or quote.bid_price is None or quote.ask_price is None:
            return None
        bid, ask = float(quote.bid_price), float(quote.ask_price)
        if bid <= 0 and ask <= 0:
            return None
        return round((bid + ask) / 2.0, 4)
    except Exception:
        return None


def mid_net_debit(legs: list[dict]) -> Optional[float]:
    """Net debit at mid = Σ long-leg mids − Σ short-leg mids. None if any mid missing."""
    total = 0.0
    for l in legs:
        m = leg_mid(l["occ"])
        if m is None:
            return None
        total += m if l["side"] == "long" else -m
    return round(total, 4)


# ── mleg order payload ───────────────────────────────────────────────────────
def build_mleg_order(legs: list[dict], qty: int, net_debit_limit: float):
    """Return (LimitOrderRequest, serializable payload dict). Debit → positive limit_price."""
    from alpaca.trading.requests import LimitOrderRequest, OptionLegRequest
    from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, PositionIntent
    leg_reqs, payload_legs = [], []
    for l in legs:
        is_long = l["side"] == "long"
        side = OrderSide.BUY if is_long else OrderSide.SELL
        intent = PositionIntent.BUY_TO_OPEN if is_long else PositionIntent.SELL_TO_OPEN
        leg_reqs.append(OptionLegRequest(
            symbol=l["occ"], ratio_qty=1, side=side, position_intent=intent))
        payload_legs.append({"symbol": l["occ"], "ratio_qty": 1,
                             "side": side.value, "position_intent": intent.value})
    req = LimitOrderRequest(
        qty=int(qty),
        limit_price=round(float(net_debit_limit), 2),
        order_class=OrderClass.MLEG,
        time_in_force=TimeInForce.DAY,
        legs=leg_reqs,
    )
    payload = {
        "order_class": "mleg", "type": "limit", "time_in_force": "day",
        "qty": int(qty), "limit_price": round(float(net_debit_limit), 2),
        "legs": payload_legs,
    }
    return req, payload


# ── persistence (reuses existing options_trades.legs_json + strategy_id) ──────
def _persist(symbol: str, structure: str, strategy: str, expiration: str,
             legs: list[dict], net_debit: float, qty: int,
             broker_order_id: Optional[str], exec_status: str) -> int:
    legs_json = json.dumps([
        {"symbol": l["occ"], "side": l["side"], "type": l["option_type"],
         "strike": l["strike"], "ratio_qty": 1}
        for l in legs
    ])
    conn = sqlite3.connect(str(_DB), timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        cur = conn.execute(
            """INSERT INTO options_trades
                 (book_tag, agent_id, structure, symbol, entry_date, expiration,
                  legs_json, entry_credit_debit, status, strategy_id,
                  broker_order_id, exec_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?)""",
            ("fleet", MANUAL_AGENT_ID, structure, symbol,
             datetime.now(timezone.utc).isoformat(timespec="seconds"),
             expiration, legs_json, net_debit, strategy,
             broker_order_id, exec_status),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


# ── top-level submit (dry_run default TRUE) ──────────────────────────────────
def submit_spread(legs: list[dict], qty: int = 1, structure: str = "vertical_spread",
                  strategy: str = "swingdesk_manual",
                  net_debit_limit: Optional[float] = None,
                  max_ratio: float = MAX_DEBIT_RATIO,
                  dry_run: bool = True, resolve_contracts: bool = True) -> dict:
    """Build → validate → (dry_run: return payload) | (live: submit paper + persist).

    resolve_contracts=True snaps each target strike to a REAL tradable Alpaca
    contract sharing one expiry (avoids "asset not found" on constructed OCC).
    Pass explicit `occ`/`symbol` on legs to bypass resolution.
    """
    norm = [_normalize_leg(l) for l in legs]
    if len(norm) != 2:
        return {"ok": False, "error": "W2 supports 2-leg verticals only"}
    underlying = norm[0]["underlying"] or norm[0]["occ"][:-15]

    # Snap to real contracts unless the caller supplied explicit occ on every leg.
    explicit_occ = all(l.get("occ") or l.get("symbol") for l in legs)
    resolved_info = None
    if resolve_contracts and not explicit_occ:
        res = resolve_vertical(underlying, norm[0]["option_type"],
                               [l["strike"] for l in norm])
        if res.get("error"):
            return {"ok": False, "error": f"contract resolution failed: {res['error']}"}
        for leg, rl in zip(norm, res["legs"]):
            leg["occ"], leg["strike"], leg["expiration"] = rl["symbol"], rl["strike"], res["expiration"]
        resolved_info = {"expiration": res["expiration"],
                         "strikes": [rl["strike"] for rl in res["legs"]]}
    if any(not l.get("occ") for l in norm):
        return {"ok": False, "error": "unresolved legs — supply explicit occ, a full "
                "expiration, or leave resolve_contracts=True"}
    expiration = norm[0]["expiration"]

    # Net debit: explicit wins; else mid from quotes.
    debit = net_debit_limit if net_debit_limit is not None else mid_net_debit(norm)
    debit_source = "supplied" if net_debit_limit is not None else "mid_quote"

    val = validate_debit(norm, debit, max_ratio)
    if not val["ok"]:
        return {"ok": False, "rejected": True, "reason": val["reason"],
                "width": val.get("width"), "ceiling": val.get("ceiling"),
                "net_debit": debit, "debit_source": debit_source}

    req, payload = build_mleg_order(norm, qty, debit)
    base = {"ok": True, "dry_run": dry_run, "underlying": underlying,
            "structure": structure, "strategy": strategy, "qty": qty,
            "net_debit": debit, "debit_source": debit_source, "resolved": resolved_info,
            "width": val["width"], "ceiling": val["ceiling"], "payload": payload}

    if dry_run:
        base["note"] = "DRY-RUN — payload validated, nothing sent to Alpaca"
        return base

    client = _get_paper_client()
    if client is None:
        return {"ok": False, "error": "Alpaca paper client unavailable (no APCA_* creds)"}
    try:
        order = client.submit_order(req)
        oid = str(order.id)
        exec_status = str(getattr(order, "status", "pending"))
        trade_id = _persist(underlying, structure, strategy, expiration,
                            norm, debit, qty, oid, exec_status)
        base.update({"submitted": True, "broker_order_id": oid,
                     "exec_status": exec_status, "options_trade_id": trade_id})
        return base
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "payload": payload}


# ── fill polling (updates options_trades by broker_order_id) ──────────────────
def poll_fill(broker_order_id: str) -> dict:
    """Re-read Alpaca order status and update the options_trades row. Returns {status,...}."""
    client = _get_paper_client()
    if client is None:
        return {"ok": False, "error": "no paper client"}
    try:
        order = client.get_order_by_id(broker_order_id)
        status = str(order.status.value if hasattr(order.status, "value") else order.status)
        filled = order.filled_avg_price
        conn = sqlite3.connect(str(_DB), timeout=30)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            # Only the broker fill-state (exec_status) moves here; lifecycle
            # `status` (open→closed) is owned by the close path, not fill polling.
            conn.execute(
                "UPDATE options_trades SET exec_status=? WHERE broker_order_id=?",
                (status, broker_order_id))
            conn.commit()
        finally:
            conn.close()
        return {"ok": True, "broker_order_id": broker_order_id,
                "exec_status": status, "filled_avg_price": filled}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
