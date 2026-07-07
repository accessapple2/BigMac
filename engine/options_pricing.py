"""engine/options_pricing.py — P0-A 2026-07-07: real-quote entry pricing for
short-premium CSP writers.

Extracted/generalized from engine.paper_trader._csp_current_premium's
Polygon-first pattern (already correct, already used for CSP TP/SL exit
checks) — this module is the ENTRY-side counterpart. Deliberately does NOT
carry a BSM/heuristic fallback: _csp_current_premium's fallback is anchored
on entry_premium, which exists by the time an exit check runs. At entry
there is no prior premium to anchor to, so a Polygon miss here means SKIP
THE TRADE (matching engine.battle_station_0dte's pattern: real quote or no
trade, never a fabricated number).

Root cause this closes: engine/wheel_strategy.py and engine/shadow_csp.py
both priced CSP entries via `min(0.08, vix / 500.0) * spot` -- a pure
VIX-scaled formula, zero chain calls, confirmed the source of Troi's
(options-sosnoff) 95.2% win rate / synthetic P&L. See docs/XO_BACKLOG.md
"P0-A: OPTIONS FILL INTEGRITY" for the full diagnosis.
"""
from __future__ import annotations

from datetime import datetime


def occ_from_csp(symbol: str, expiration: str, strike: float) -> str | None:
    """Build OCC ticker for a short put. Format: O:<UNDERLYING><YYMMDD>P<strike-mil-padded-8>.
    Returns None on bad input. Byte-identical logic to
    engine.paper_trader._occ_from_csp (kept separate to avoid a new
    cross-module dependency for a 10-line pure function)."""
    try:
        ymd = datetime.strptime(expiration[:10], "%Y-%m-%d").strftime("%y%m%d")
        strike_mil = int(round(float(strike) * 1000))
        if strike_mil <= 0 or not symbol:
            return None
        return f"O:{symbol.upper()}{ymd}P{strike_mil:08d}"
    except Exception:
        return None


def get_real_csp_premium(symbol: str, expiration: str, strike: float) -> tuple[float, str] | tuple[None, None]:
    """Real-quote premium for a NEW short put entry. Returns (premium,
    source_tag) or (None, None) if no real quote is available anywhere --
    callers MUST skip the trade on None, never substitute an estimate.

    Source order: Alpaca first, Polygon second.
    LIVE-VERIFIED 2026-07-07: Polygon's /v3/snapshot/options endpoint
    returns populated Greeks but bid=ask=mid=0.0 across every strike/expiry/
    type tested on this account's tier (systemic, not contract-specific --
    checked 4 different SPY contracts, all zero). Matches the already-banked
    "Options Starter doesn't include WebSocket trades" tier gap
    (drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md) -- this looks like a
    sibling gap for snapshot-level bid/ask. Alpaca confirmed live-working
    for both SPY and QQQ (the only 2 wheel tickers not already blocked by
    door1's leveraged-ETF ban) via the same get_contract_at_strike/
    _get_contract_price path battle_station_0dte.py already uses
    successfully. Polygon kept as a second attempt, not removed -- it may
    regain bid/ask coverage on a tier change, and costs nothing to try.
    """
    try:
        from engine.alpaca_options import get_contract_at_strike, _get_contract_price
        dte = max(1, (datetime.strptime(expiration[:10], "%Y-%m-%d").date() - datetime.now().date()).days)
        contract = get_contract_at_strike(symbol, "put", dte, strike)
        if contract:
            price = _get_contract_price(contract)
            if price and price > 0:
                return float(price), "alpaca"
    except Exception:
        pass

    occ = occ_from_csp(symbol, expiration, strike)
    if occ:
        try:
            from engine.providers.polygon_provider import PolygonData
            pd = PolygonData()
            if pd.is_active():
                q = pd.get_option_quote(occ)
                if q and (q.get("mid") or 0) > 0:
                    return float(q["mid"]), "polygon"
        except Exception:
            pass

    return None, None
