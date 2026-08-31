"""
engine/gamma_context.py

Native gamma-exposure (GEX) grounding for OllieTrades options / 0DTE agents.

WHY THIS EXISTS
---------------
Mirrors engine/ticker_context.py (commit ff1a920). ticker_context grounds the
EQUITIES agents so they stop hallucinating company identities and price levels.
This module is the OPTIONS-side equivalent: it feeds agents REAL dealer-positioning
structure -- call resistance, put support, the gamma flip -- so a 0DTE agent
reasons against where market makers actually have to hedge instead of inventing
breakout prices.

It computes everything natively from the Polygon options chain (greeks + open
interest) we ALREADY pay for ($29/mo Options Starter). No MenthorQ subscription,
no partner-locked API. We own the stack. This IS the MenthorQ math.

FAIL-SAFE DOCTRINE
------------------
If the chain or greeks are unavailable, get_gamma_context() returns
GammaContext(available=False) and build_gamma_block() returns "" -- the prompt
assembler simply omits the block. Agents are NEVER handed fabricated levels.
A missing block is always safer than a hallucinated one.

DATA RULE
---------
Computed snapshots are APPENDED to data/gamma_snapshots.parquet (archive, never
overwrite), consistent with the append-only / sacred-data doctrine.

Sign convention (env GEX_DEALER_CONVENTION, default "long_call_short_put",
the SpotGamma / MenthorQ retail convention):
    call gamma -> +GEX, put gamma -> -GEX   (dealers long calls, short puts)
Net positive GEX above flip  -> vol suppression (sticky ranges).
Net negative GEX below flip  -> vol amplification (directional moves).
"""

from __future__ import annotations

import math
import os
import time
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import sqlite3
import threading

import requests

log = logging.getLogger("gamma_context")
_SNAPSHOT_LOCK = threading.Lock()

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
POLYGON_KEY = os.getenv("POLYGON_API_KEY", "")
POLYGON_BASE = "https://api.polygon.io"
DEALER_CONVENTION = os.getenv("GEX_DEALER_CONVENTION", "long_call_short_put")
RISK_FREE = float(os.getenv("GEX_RISK_FREE", "0.045"))
CACHE_TTL_SEC = int(os.getenv("GEX_CACHE_TTL_SEC", "1800"))      # 30 min intraday
MAX_EXPIRIES = int(os.getenv("GEX_MAX_EXPIRIES", "3"))           # nearest N expiries
SNAPSHOT_LOG = os.getenv("GEX_SNAPSHOT_LOG", "data/gamma_snapshots.parquet")
TRADER_DB = os.getenv("TRADER_DB", "data/trader.db")
CONTRACT_MULT = 100

_CACHE: dict[str, tuple[float, "GammaContext"]] = {}


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #
@dataclass
class GammaContext:
    ticker: str
    available: bool
    spot: float = 0.0
    net_gex: float = 0.0                 # $ gamma per 1% move (signed)
    regime: str = "unknown"              # "positive" | "negative"
    gamma_flip: Optional[float] = None   # spot level where net GEX crosses zero
    call_wall: Optional[float] = None    # strongest +GEX strike (resistance)
    put_wall: Optional[float] = None     # strongest -GEX strike (support)
    top_strikes: list[dict] = field(default_factory=list)
    n_contracts: int = 0
    asof: str = ""
    source: str = "polygon"
    note: str = ""


# --------------------------------------------------------------------------- #
# Black-Scholes gamma (fallback + flip solver)
# --------------------------------------------------------------------------- #
def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _bs_gamma(spot: float, strike: float, t_years: float, iv: float,
              r: float = RISK_FREE) -> float:
    """Black-Scholes gamma. Returns 0 on degenerate inputs (fail-safe)."""
    if spot <= 0 or strike <= 0 or t_years <= 0 or iv <= 0:
        return 0.0
    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * t_years) / (iv * math.sqrt(t_years))
    return _norm_pdf(d1) / (spot * iv * math.sqrt(t_years))


def _years_to_exp(exp_date: str) -> float:
    try:
        exp = datetime.strptime(exp_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        secs = (exp - datetime.now(timezone.utc)).total_seconds()
        if secs <= 0:
            return 0.0          # expired -- _bs_gamma guards t_years <= 0 -> 0.0
        return max(secs, 3600) / (365.0 * 24 * 3600)   # floor at ~1h for 0DTE
    except Exception:
        return 1.0 / 365.0


# --------------------------------------------------------------------------- #
# Polygon fetch
# --------------------------------------------------------------------------- #
def _fetch_spot(ticker: str) -> Optional[float]:
    """Authoritative underlying spot. Tries snapshot, then last-trade. None on failure."""
    try:
        r = requests.get(
            f"{POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}",
            params={"apiKey": POLYGON_KEY}, timeout=10)
        r.raise_for_status()
        t = r.json().get("ticker", {})
        # prefer last trade, fall back to prev close
        px = (t.get("lastTrade", {}) or {}).get("p") or (t.get("day", {}) or {}).get("c")
        if px:
            return float(px)
    except Exception as e:
        log.warning("spot snapshot failed for %s: %s", ticker, e)
    try:
        r = requests.get(f"{POLYGON_BASE}/v2/last/trade/{ticker.upper()}",
                         params={"apiKey": POLYGON_KEY}, timeout=10)
        r.raise_for_status()
        px = r.json().get("results", {}).get("p")
        return float(px) if px else None
    except Exception as e:
        log.warning("last-trade spot failed for %s: %s", ticker, e)
        return None


def _polygon_snapshot(ticker: str) -> Optional[list[dict]]:
    """
    Pull the options chain snapshot. Returns a list of normalized contract dicts
    or None on failure. Normalized fields:
        strike, type ('call'|'put'), expiry, oi, gamma, iv
    """
    if not POLYGON_KEY:
        log.warning("POLYGON_API_KEY unset -- gamma grounding disabled")
        return None

    url = f"{POLYGON_BASE}/v3/snapshot/options/{ticker.upper()}"
    out: list[dict] = []
    spot: Optional[float] = _fetch_spot(ticker)
    params = {"limit": 250, "apiKey": POLYGON_KEY}

    try:
        for _ in range(8):  # paginate, bounded
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            for c in payload.get("results", []):
                det = c.get("details", {})
                grk = c.get("greeks", {})
                out.append({
                    "strike": det.get("strike_price"),
                    "type": det.get("contract_type"),
                    "expiry": det.get("expiration_date"),
                    "oi": c.get("open_interest") or 0,
                    "gamma": grk.get("gamma"),
                    "iv": c.get("implied_volatility") or grk.get("iv"),
                })
            nxt = payload.get("next_url")
            if not nxt:
                break
            url, params = nxt, {"apiKey": POLYGON_KEY}
    except Exception as e:
        log.warning("polygon snapshot failed for %s: %s", ticker, e)
        return None

    for c in out:
        c["_spot"] = spot
    return out or None


# --------------------------------------------------------------------------- #
# GEX math
# --------------------------------------------------------------------------- #
def _sign(contract_type: str) -> float:
    if DEALER_CONVENTION == "long_call_short_put":
        return 1.0 if contract_type == "call" else -1.0
    # symmetric absolute convention fallback
    return 1.0


def _contract_gex(gamma: float, oi: float, spot: float, ctype: str) -> float:
    """$ GEX for one strike: gamma * OI * 100 * spot^2 * 0.01, signed by convention."""
    return _sign(ctype) * gamma * oi * CONTRACT_MULT * (spot ** 2) * 0.01


def _net_gex_at(contracts: list[dict], hypothetical_spot: float) -> float:
    """Recompute net GEX assuming spot = hypothetical_spot (BS-regamma'd). Used by flip solver."""
    total = 0.0
    for c in contracts:
        strike, ctype, oi = c["strike"], c["type"], c["oi"]
        iv, expiry = c.get("iv"), c.get("expiry")
        if not strike or not ctype or not oi or not iv or not expiry:
            continue
        g = _bs_gamma(hypothetical_spot, strike, _years_to_exp(expiry), iv)
        total += _contract_gex(g, oi, hypothetical_spot, ctype)
    return total


def _find_flip(contracts: list[dict], spot: float) -> Optional[float]:
    """Scan +/-15% around spot for the net-GEX zero crossing (the gamma flip)."""
    lo, hi = spot * 0.85, spot * 1.15
    steps = 60
    prev_s = lo
    prev_v = _net_gex_at(contracts, lo)
    if prev_v == 0:                  # exact zero at left boundary
        return round(lo, 2)
    for i in range(1, steps + 1):
        s = lo + (hi - lo) * i / steps
        v = _net_gex_at(contracts, s)
        if v == 0:                   # exact zero at this step
            return round(s, 2)
        if (prev_v < 0) != (v < 0):  # sign change -> linear interp the crossing
            if v != prev_v:
                cross = prev_s + (s - prev_s) * (0 - prev_v) / (v - prev_v)
                return round(cross, 2)
        prev_s, prev_v = s, v
    return None


def _compute(ticker: str, contracts: list[dict]) -> GammaContext:
    spot = next((c.get("_spot") for c in contracts if c.get("_spot")), None)
    if not spot:
        return GammaContext(ticker=ticker, available=False, note="no spot price")

    # nearest N expiries only (0DTE / near-dated dominate dealer hedging)
    expiries = sorted({c["expiry"] for c in contracts if c.get("expiry")})[:MAX_EXPIRIES]
    contracts = [c for c in contracts if c.get("expiry") in expiries]

    by_strike: dict[float, float] = {}
    net = 0.0
    used = 0
    for c in contracts:
        strike, ctype, oi = c["strike"], c["type"], c["oi"]
        if not strike or not ctype or not oi:
            continue
        gamma = c.get("gamma")
        if gamma is None:  # fallback: compute from IV
            if not c.get("iv") or not c.get("expiry"):
                continue
            gamma = _bs_gamma(spot, strike, _years_to_exp(c["expiry"]), c["iv"])
        gex = _contract_gex(gamma, oi, spot, ctype)
        by_strike[strike] = by_strike.get(strike, 0.0) + gex
        net += gex
        used += 1

    if used == 0:
        return GammaContext(ticker=ticker, available=False, spot=spot,
                            note="no usable greeks/OI")

    call_wall = max(by_strike.items(), key=lambda kv: kv[1], default=(None, 0))[0]
    put_wall = min(by_strike.items(), key=lambda kv: kv[1], default=(None, 0))[0]
    top = sorted(by_strike.items(), key=lambda kv: abs(kv[1]), reverse=True)[:5]
    top_strikes = [{"strike": k, "gex": round(v, 0)} for k, v in top]
    flip = _find_flip(contracts, spot)

    # P0-B.4 2026-07-07 (HM-GEX-UNIFICATION): was `"positive" if net>=0 else
    # "negative"` -- the raw SIGN of aggregate dealer GEX, a genuinely
    # different (and less correct, per standard options-market convention)
    # rule than the canonical source's (engine/options_flow_gex.py,
    # flow_gex.db) spot-vs-flip rule. A market can show negative aggregate
    # GEX while spot sits above the local flip -- confirmed as the actual
    # cause of a real signal-page/bridge regime-label mismatch on 2026-07-07
    # (bridge correctly said LONG GAMMA; this naive check said "negative").
    # Same precedence as the canonical source: spot-vs-flip when a flip
    # exists, raw sign only as the fallback when no zero-crossing was found
    # in range (matches engine/options_flow_gex.py:236-242 exactly).
    if flip is not None:
        regime = "positive" if spot >= flip else "negative"
    else:
        regime = "positive" if net >= 0 else "negative"

    return GammaContext(
        ticker=ticker.upper(),
        available=True,
        spot=round(spot, 2),
        net_gex=round(net, 0),
        regime=regime,
        gamma_flip=flip,
        call_wall=call_wall,
        put_wall=put_wall,
        top_strikes=top_strikes,
        n_contracts=used,
        asof=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


# --------------------------------------------------------------------------- #
# Append-only snapshot log
# --------------------------------------------------------------------------- #
def _log_snapshot(ctx: GammaContext) -> None:
    if not ctx.available:
        return
    # Parquet archive — lock serializes concurrent tickers, lock released before DB touch.
    try:
        import pandas as pd
        row = {k: v for k, v in asdict(ctx).items() if k != "top_strikes"}
        row["top_strikes"] = json.dumps(ctx.top_strikes)
        df = pd.DataFrame([row])
        os.makedirs(os.path.dirname(SNAPSHOT_LOG) or ".", exist_ok=True)
        with _SNAPSHOT_LOCK:                               # serialize concurrent tickers
            if os.path.exists(SNAPSHOT_LOG):
                old = pd.read_parquet(SNAPSHOT_LOG)
                df = pd.concat([old, df], ignore_index=True)   # APPEND, never overwrite
            df.to_parquet(SNAPSHOT_LOG, index=False)
    except Exception as e:
        log.warning("snapshot log skipped: %s", e)
    # DB freshness stamp — separate try so a trader.db write-lock (e.g. Schwab sync)
    # cannot block or fail the parquet write above.
    try:
        conn = sqlite3.connect(TRADER_DB, timeout=5)
        conn.execute(
            "INSERT INTO gex_snapshots "
            "(symbol, timestamp, spot_price, max_gamma_strike, zero_gamma_level, "
            " put_wall, call_wall, gamma_flip, total_gex, levels_json, source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                ctx.ticker, ctx.asof, ctx.spot,
                ctx.top_strikes[0]["strike"] if ctx.top_strikes else None,
                ctx.gamma_flip,
                ctx.put_wall, ctx.call_wall, ctx.gamma_flip,
                ctx.net_gex, json.dumps(ctx.top_strikes), ctx.source,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as _db_e:
        log.warning("gex_snapshots stamp failed: %s", _db_e)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def get_gamma_context(ticker: str, use_cache: bool = True) -> GammaContext:
    ticker = ticker.upper()
    now = time.time()
    if use_cache and ticker in _CACHE:
        ts, ctx = _CACHE[ticker]
        if now - ts < CACHE_TTL_SEC:
            return ctx

    contracts = _polygon_snapshot(ticker)
    if not contracts:
        ctx = GammaContext(ticker=ticker, available=False, note="chain unavailable")
    else:
        ctx = _compute(ticker, contracts)
        _log_snapshot(ctx)

    _CACHE[ticker] = (now, ctx)
    return ctx


def build_gamma_block(ticker: str) -> str:
    """
    The integration point. Call this from generate_hot_take() exactly like the
    ticker grounding block. Returns "" when no real data -> block is omitted.
    """
    ctx = get_gamma_context(ticker)
    if not ctx.available:
        return ""

    flip = f"${ctx.gamma_flip}" if ctx.gamma_flip is not None else "n/a"
    regime_note = ("positive gamma -> dealers dampen moves, expect mean-reversion / "
                   "sticky ranges" if ctx.regime == "positive"
                   else "negative gamma -> dealers amplify moves, expect trending / "
                        "higher realized vol")
    return (
        f"[GAMMA STRUCTURE -- {ctx.ticker} (computed from live options chain, "
        f"as of {ctx.asof}). These are REAL dealer-positioning levels. Do NOT "
        f"invent other levels.]\n"
        f"- Spot: ${ctx.spot}\n"
        f"- Net GEX: {ctx.net_gex:,.0f} ({ctx.regime} gamma) -- {regime_note}\n"
        f"- Gamma flip: {flip} (regime changes through this level)\n"
        f"- Call wall / resistance: ${ctx.call_wall}\n"
        f"- Put wall / support: ${ctx.put_wall}\n"
        f"- Highest-|GEX| strikes: "
        f"{', '.join('$' + str(s['strike']) for s in ctx.top_strikes)}\n"
    )


# --------------------------------------------------------------------------- #
# Self-test:  python gamma_context.py SPY     (live, needs POLYGON_API_KEY)
#             python gamma_context.py --mock   (offline math check)
# --------------------------------------------------------------------------- #
def _mock_test() -> None:
    spot = 500.0
    contracts = []
    for k in range(460, 541, 5):
        for ctype in ("call", "put"):
            contracts.append({
                "strike": float(k), "type": ctype, "expiry": "2026-06-24",
                "oi": 5000 if k in (480, 520) else 1200,
                "gamma": None, "iv": 0.18, "_spot": spot,
            })
    ctx = _compute("SPY", contracts)
    print("MOCK GammaContext:")
    print(json.dumps({k: v for k, v in asdict(ctx).items()}, indent=2, default=str))
    print("\nGrounding block preview:\n")
    # temporarily prime cache so build_gamma_block uses the mock
    _CACHE["SPY"] = (time.time(), ctx)
    print(build_gamma_block("SPY"))


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1 and sys.argv[1] == "--mock":
        _mock_test()
    else:
        tkr = sys.argv[1] if len(sys.argv) > 1 else "SPY"
        print(json.dumps(asdict(get_gamma_context(tkr)), indent=2, default=str))
        print("\n" + build_gamma_block(tkr))
