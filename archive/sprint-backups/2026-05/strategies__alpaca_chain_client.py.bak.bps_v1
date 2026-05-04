"""
Alpaca options chain adapter for bull spread construction.

API call budget per build_spread_quote():
  1. GET /v2/stocks/{ticker}/bars/latest          — spot price
  2. GET /v2/options/contracts                    — chain discovery + expiry selection
  3. GET /v1beta1/options/snapshots               — batch bid/ask for both legs

Refinements applied (2026-04-24):
  - Expiry window: [dte_target-2, dte_target+5] asymmetric — shorter DTE changes theta profile
  - Bid/ask spread guard: spread_pct > 0.25 -> None (catches illiquid strikes, stale quotes)
  - Fallback: alpaca -> mock only (Polygon Starter plan returns no quotes)
  - Rate limit note: Alpaca paper 200 req/min; 3 calls per invocation, SPY FIRST_TRADE_MODE = safe
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Optional

try:
    from .mock_data import SpreadQuote, OptionLeg
except ImportError:
    # Allows running as __main__ directly
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from strategies.mock_data import SpreadQuote, OptionLeg


_PAPER_BASE = "https://paper-api.alpaca.markets"
_DATA_BASE  = "https://data.alpaca.markets"

# Refinement 3: max bid/ask spread as fraction of mid before rejecting leg
_MAX_SPREAD_PCT = 0.25


def _headers() -> dict:
    key    = os.environ.get("APCA_API_KEY_ID", "")
    secret = os.environ.get("APCA_API_SECRET_KEY", "")
    if not (key and secret):
        raise EnvironmentError("APCA_API_KEY_ID / APCA_API_SECRET_KEY not set")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def _fetch_spot(ticker: str) -> Optional[float]:
    """Latest close from Alpaca stock bars (IEX feed)."""
    import requests
    try:
        r = requests.get(
            f"{_DATA_BASE}/v2/stocks/{ticker}/bars/latest",
            params={"feed": "iex"},
            headers=_headers(),
            timeout=5,
        )
        if r.status_code != 200:
            print(f"[alpaca_chain] spot HTTP {r.status_code} for {ticker}")
            return None
        close = r.json().get("bar", {}).get("c")
        return float(close) if close else None
    except Exception as e:
        print(f"[alpaca_chain] spot error for {ticker}: {e}")
        return None


def _discover_contracts(
    ticker: str,
    option_type: str,
    dte_target: int,
    strike_gte: float,
    strike_lte: float,
) -> tuple[Optional[str], list[dict]]:
    """
    One call: find contracts in the asymmetric DTE window + strike range.
    Returns (chosen_expiry, contracts_for_that_expiry).

    Refinement 2: window is [dte_target-2, dte_target+5].
    Prefers the Friday closest to dte_target; falls back to nearest calendar date.
    """
    import requests
    today = date.today()
    gte = (today + timedelta(days=dte_target - 2)).isoformat()   # Refinement 2
    lte = (today + timedelta(days=dte_target + 5)).isoformat()
    target_dt = today + timedelta(days=dte_target)

    r = requests.get(
        f"{_PAPER_BASE}/v2/options/contracts",
        params={
            "underlying_symbols": ticker,
            "type":               option_type,
            "expiration_date_gte": gte,
            "expiration_date_lte": lte,
            "strike_price_gte":   str(int(strike_gte)),
            "strike_price_lte":   str(int(strike_lte) + 1),
            "status":             "active",
            "limit":              100,
        },
        headers=_headers(),
        timeout=5,
    )
    if r.status_code != 200:
        print(f"[alpaca_chain] contracts HTTP {r.status_code}: {r.text[:120]}")
        return None, []

    all_contracts = r.json().get("option_contracts", [])
    if not all_contracts:
        print(f"[alpaca_chain] no {option_type} contracts for {ticker} "
              f"in [{gte}, {lte}] strikes [{strike_gte:.0f}, {strike_lte:.0f}]")
        return None, []

    # Pick best expiry: prefer Friday, closest to dte_target
    expiries = sorted(set(c["expiration_date"] for c in all_contracts))
    fridays  = [e for e in expiries if date.fromisoformat(e).weekday() == 4]
    candidates = fridays if fridays else expiries
    chosen = min(candidates, key=lambda e: abs((date.fromisoformat(e) - target_dt).days))

    filtered = [c for c in all_contracts if c["expiration_date"] == chosen]
    return chosen, filtered


def _fetch_quotes(symbols: list[str]) -> dict:
    """
    Batch snapshot fetch — returns {symbol: snapshot_dict}.
    One HTTP call for both legs.
    """
    import requests
    r = requests.get(
        f"{_DATA_BASE}/v1beta1/options/snapshots",
        params={"symbols": ",".join(symbols), "feed": "indicative"},
        headers=_headers(),
        timeout=5,
    )
    if r.status_code != 200:
        print(f"[alpaca_chain] snapshots HTTP {r.status_code}: {r.text[:120]}")
        return {}
    return r.json().get("snapshots", {})


def _extract_mid(symbol: str, snapshots: dict) -> Optional[float]:
    """
    Extract validated mid-price with Refinement 3 bid/ask spread guard.

    Guard conditions (return None):
      - bid or ask missing / zero
      - (ask - bid) / mid > MAX_SPREAD_PCT (0.25)
    Fallback: use latestTrade.price if no valid bid/ask (with warning).
    """
    snap = snapshots.get(symbol)
    if not snap:
        print(f"[alpaca_chain] no snapshot data for {symbol}")
        return None

    q   = snap.get("latestQuote", {})
    bid = q.get("bp")
    ask = q.get("ap")

    if not bid or not ask or bid <= 0 or ask <= 0:
        # Fallback to last trade price (warn — not ideal for pricing)
        tp = (snap.get("latestTrade") or {}).get("p")
        if tp and tp > 0:
            print(f"[alpaca_chain] {symbol}: no valid bid/ask (bid={bid} ask={ask}), "
                  f"using last trade ${tp:.2f} — treat as indicative only")
            return float(tp)
        print(f"[alpaca_chain] {symbol}: bid={bid} ask={ask} and no last trade — unusable")
        return None

    mid        = (bid + ask) / 2.0
    spread_pct = (ask - bid) / mid

    if spread_pct > _MAX_SPREAD_PCT:
        print(f"[alpaca_chain] {symbol} spread too wide: "
              f"bid={bid:.2f} ask={ask:.2f} spread_pct={spread_pct:.1%} "
              f"(limit {_MAX_SPREAD_PCT:.0%}) — skipping")
        return None

    return mid


def build_spread_quote(
    ticker: str,
    structure: str,   # "bull_call_spread" | "bull_put_spread"
    dte_target: int,
    width: float,
) -> Optional[SpreadQuote]:
    """
    Build a SpreadQuote from live Alpaca chain data.
    Returns None on any failure — caller should fall back to mock.

    Rate budget: 3 calls (spot + contracts + snapshots).
    Alpaca paper limit: 200 req/min. Safe for FIRST_TRADE_MODE (1 ticker).
    """
    option_type = "call" if structure == "bull_call_spread" else "put"

    # ── 1. Spot price ──────────────────────────────────────────────────────
    spot = _fetch_spot(ticker)
    if spot is None:
        print(f"[alpaca_chain] {ticker}: spot unavailable")
        return None

    # ── 2. Chain discovery ─────────────────────────────────────────────────
    if structure == "bull_call_spread":
        # long ~ATM, short ~ATM+width  →  need strikes from spot-5 to spot+width+10
        strike_gte = spot - 5
        strike_lte = spot + width + 10
    else:
        # bull_put_spread: short ~ATM, long ~ATM-width  →  spot-width-10 to spot+5
        strike_gte = spot - width - 10
        strike_lte = spot + 5

    expiry, contracts = _discover_contracts(
        ticker, option_type, dte_target, strike_gte, strike_lte
    )
    if expiry is None or not contracts:
        return None

    actual_dte = (date.fromisoformat(expiry) - date.today()).days
    strike_map = {float(c["strike_price"]): c for c in contracts}

    # ── 3. Strike selection ────────────────────────────────────────────────
    if structure == "bull_call_spread":
        long_strike  = min(strike_map, key=lambda s: abs(s - spot))
        short_strike = min(strike_map, key=lambda s: abs(s - (spot + width)))
    else:
        short_strike = min(strike_map, key=lambda s: abs(s - spot))
        long_strike  = min(strike_map, key=lambda s: abs(s - (spot - width)))

    if long_strike == short_strike:
        print(f"[alpaca_chain] {ticker}: identical strikes ({long_strike}) — width ${width:.0f} too narrow?")
        return None

    long_symbol  = strike_map[long_strike]["symbol"]
    short_symbol = strike_map[short_strike]["symbol"]

    # ── 4. Batch quote fetch ───────────────────────────────────────────────
    snapshots  = _fetch_quotes([long_symbol, short_symbol])
    long_mid   = _extract_mid(long_symbol, snapshots)
    short_mid  = _extract_mid(short_symbol, snapshots)

    if long_mid is None or short_mid is None:
        print(f"[alpaca_chain] {ticker}: could not get valid mid for both legs "
              f"(long={long_mid}, short={short_mid})")
        return None

    # ── 5. Spread math + sanity check ─────────────────────────────────────
    actual_width = abs(short_strike - long_strike)

    if structure == "bull_call_spread":
        net_debit = long_mid - short_mid
        if net_debit <= 0:
            print(f"[alpaca_chain] {ticker}: call debit={net_debit:.2f} <= 0 — bad quote")
            return None
        return SpreadQuote(
            ticker=ticker, structure=structure,
            long_leg=OptionLeg("buy",  "call", long_strike,  expiry, long_mid),
            short_leg=OptionLeg("sell", "call", short_strike, expiry, short_mid),
            net_debit=net_debit, net_credit=0.0,
            max_profit=(actual_width - net_debit) * 100.0,
            max_loss=net_debit * 100.0,
            width=actual_width, dte=actual_dte,
        )
    else:  # bull_put_spread
        net_credit = short_mid - long_mid
        if net_credit <= 0:
            print(f"[alpaca_chain] {ticker}: put credit={net_credit:.2f} <= 0 — bad quote")
            return None
        if net_credit >= actual_width:
            print(f"[alpaca_chain] {ticker}: credit {net_credit:.2f} >= width {actual_width:.0f} — bad quote")
            return None
        return SpreadQuote(
            ticker=ticker, structure=structure,
            short_leg=OptionLeg("sell", "put", short_strike, expiry, short_mid),
            long_leg=OptionLeg("buy",  "put", long_strike,  expiry, long_mid),
            net_debit=0.0, net_credit=net_credit,
            max_profit=net_credit * 100.0,
            max_loss=(actual_width - net_credit) * 100.0,
            width=actual_width, dte=actual_dte,
        )


# ── Self-test ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(Path.home() / "autonomous-trader" / ".env")

    print("=== alpaca_chain_client self-test ===")
    print()

    ok = True
    tests = [
        ("SPY", "bull_put_spread",  28, 20.0),
        ("SPY", "bull_call_spread", 28, 10.0),
    ]

    for ticker, structure, dte, width in tests:
        print(f"--- {structure}  {ticker}  {dte}d  width=${width:.0f} ---")
        q = build_spread_quote(ticker, structure, dte, width)
        if q is None:
            print(f"  FAIL: returned None")
            ok = False
        else:
            print(f"  expiry={q.long_leg.expiration}  actual_dte={q.dte}  width=${q.width:.0f}")
            print(f"  long_leg:  {q.long_leg.option_type} ${q.long_leg.strike:.0f}"
                  f"  mid=${q.long_leg.premium:.2f}")
            print(f"  short_leg: {q.short_leg.option_type} ${q.short_leg.strike:.0f}"
                  f"  mid=${q.short_leg.premium:.2f}")
            if structure == "bull_put_spread":
                print(f"  net_credit=${q.net_credit:.2f}  "
                      f"max_profit=${q.max_profit:.0f}  max_loss=${q.max_loss:.0f}  "
                      f"credit/width={q.net_credit/q.width*100:.1f}%")
            else:
                print(f"  net_debit=${q.net_debit:.2f}  "
                      f"max_profit=${q.max_profit:.0f}  max_loss=${q.max_loss:.0f}  "
                      f"reward/risk={q.max_profit/q.max_loss:.2f}x")
        print()

    print("=== PASS ===" if ok else "=== FAIL ===")
    sys.exit(0 if ok else 1)
