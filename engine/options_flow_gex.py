"""engine/options_flow_gex.py — HM-FLOW-NATIVE + HM-GEX (Polygon-native, OBSERVATION-ONLY).

⚠️ OBSERVATION-ONLY BY CONSTRUCTION. This module imports NO order/execution path
(no paper_trader, no alpaca_*, no submit). It collects + scores + persists options
data for research. Signals must pass through strategies/validation.py (DSR/PBO)
before any trading is even proposed. Nothing here reaches an order path.

STEP-0 FEASIBILITY (probed 2026-05-31, Polygon Options Starter):
  - Options TRADES tape  /v3/trades  -> 403 NOT_AUTHORIZED  (ABSENT)
  - NBBO QUOTES          /v3/quotes  -> 403 NOT_AUTHORIZED  (ABSENT)
  - Snapshot greeks+OI   /v3/snapshot/options/{u} -> 200 OK  (gamma, OI, day.volume present)

Consequence:
  - HM-GEX: FULLY BUILDABLE (gamma + OI per strike).
  - HM-FLOW print-level (per-print premium / sweep vs block / at-ask vs at-bid):
    NOT BUILDABLE on this tier — those need the trade tape + NBBO. Marked UNAVAILABLE
    in the output, NEVER faked. What IS real: a per-symbol AGGREGATE flow from the
    snapshot's day.volume + OI (call/put notional lean, volume/OI 'unusual' proxy,
    vol>OI opening proxy). True print-level flow is a Polygon-tier-upgrade decision
    (alongside deferred HM-DARKPOOL).

Reuses strategies/polygon_client (no new paid feed). py3.14.
"""
from __future__ import annotations

import math
import os
import sqlite3
import sys
from datetime import date, datetime, timezone

import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

POLYGON_BASE = "https://api.polygon.io"
DB_PATH = os.path.join(_ROOT, "data", "flow_gex.db")
CONTRACT_MULT = 100
PREMIUM_FLOOR = 250_000          # $ — tunable; applies to aggregate notional here
UNUSUAL_VOL_OI = 1.0             # day.volume / OI >= this => 'unusual' (vol>OI opening proxy)

# GEX gamma-relevant band (documented). Full /v3/snapshot chain spans strikes
# 150..1000 over 26 expirations — but gamma is negligible far from spot / far-dated.
# We aggregate strikes within +/-STRIKE_BAND of spot and expiries <= EXPIRY_DTE_MAX
# days. Outside that, BS gamma ~ 0 and only adds noise (the far-OTM put cluster that
# was dragging the cumulative flip to 482).
STRIKE_BAND = 0.20               # +/-20% of spot
EXPIRY_DTE_MAX = 60              # near-term (gamma-relevant) expirations only
RISK_FREE = 0.0                  # r~0 for short-dated flip profile

# ── GEX dealer-sign convention (DOCUMENTED) ────────────────────────────────
# SqueezeMetrics-style: dealers are LONG calls / SHORT puts (they hold the other
# side of the typical customer book where customers buy puts for protection and
# overwrite calls). Therefore:
#   call GEX = +gamma * OI * 100 * spot^2 * 0.01   (positive => stabilizing)
#   put  GEX = -gamma * OI * 100 * spot^2 * 0.01   (negative => destabilizing)
#   net GEX  = sum(call) + sum(put)   ; gamma flip = strike where cumulative net GEX
#   crosses zero (below flip => negative/short-gamma regime => trend-amplifying).
DEALER_LONG_CALLS_SHORT_PUTS = True


def _load_env() -> None:
    env = os.path.join(_ROOT, ".env")
    if os.path.exists(env):
        for line in open(env):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


_load_env()


def _key() -> str | None:
    return os.environ.get("POLYGON_API_KEY") or None


# ── Chain snapshot (paginated) ─────────────────────────────────────────────
def fetch_chain(underlying: str, max_pages: int = 60) -> list:
    """FULL option chain snapshot via /v3/snapshot/options/{u} + next_url.
    Default 60 pages x 250 = up to 15k contracts (SPY full chain ~10k). The old
    6-page (1500) cap was the HM-GEX-SANITY bug — it truncated the chain."""
    key = _key()
    if not key:
        return []
    out = []
    url = f"{POLYGON_BASE}/v3/snapshot/options/{underlying}"
    params = {"apiKey": key, "limit": 250}
    pages = 0
    while url and pages < max_pages:
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code != 200:
                break
            j = r.json()
        except Exception:
            break
        out.extend(j.get("results", []) or [])
        nxt = j.get("next_url")
        if nxt:
            url = nxt + (("&apiKey=" + key) if "apiKey" not in nxt else "")
            params = {}
        else:
            url = None
        pages += 1
    return out


def fetch_spot(underlying: str) -> float | None:
    key = _key()
    if not key:
        return None
    try:
        r = requests.get(f"{POLYGON_BASE}/v2/aggs/ticker/{underlying}/prev",
                         params={"apiKey": key, "adjusted": "true"}, timeout=15)
        res = (r.json().get("results") or [])
        return float(res[0]["c"]) if res else None
    except Exception:
        return None


# ── HM-GEX ──────────────────────────────────────────────────────────────────
_SQRT_2PI = math.sqrt(2.0 * math.pi)


def _bs_gamma(S: float, K: float, T: float, sigma: float) -> float:
    """Black-Scholes gamma at hypothetical spot S (r=0). Lets us re-evaluate
    dealer gamma across spot levels to find the true flip (snapshot gamma is
    only valid at the current spot)."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (RISK_FREE + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return math.exp(-0.5 * d1 * d1) / _SQRT_2PI / (S * sigma * math.sqrt(T))


def _net_gex_at(spot_level: float, contracts: list) -> float:
    """Net dealer GEX (calls +, puts −) at a hypothetical spot, BS-re-gammaed."""
    tot = 0.0
    for K, ctype, oi, iv, T in contracts:
        g = _bs_gamma(spot_level, K, T, iv)
        gex = g * oi * CONTRACT_MULT * spot_level * spot_level * 0.01
        tot += gex if ctype == "call" else -gex
    return tot


def compute_gex(underlying: str, chain: list | None = None, spot: float | None = None) -> dict:
    """Dealer GEX. Aggregates the gamma-relevant band (+/-STRIKE_BAND of spot,
    expiries <= EXPIRY_DTE_MAX). total GEX + walls use snapshot gamma at spot;
    gamma flip = spot level where BS-re-gammaed net GEX crosses zero."""
    chain = chain if chain is not None else fetch_chain(underlying)
    spot = spot if spot is not None else fetch_spot(underlying)
    if not chain or not spot:
        return {"underlying": underlying, "error": "no chain/spot", "spot": spot}

    lo, hi = spot * (1 - STRIKE_BAND), spot * (1 + STRIKE_BAND)
    today = date.today()
    per_strike: dict = {}
    call_gex: dict = {}
    put_gex: dict = {}
    bs_contracts = []           # (K, type, OI, IV, T_years) for the flip profile
    coef = CONTRACT_MULT * spot * spot * 0.01
    used = 0
    n_exp = set()
    for c in chain:
        d = c.get("details") or {}
        k = d.get("strike_price")
        ctype = d.get("contract_type")
        oi = c.get("open_interest") or 0
        g = (c.get("greeks") or {}).get("gamma")
        iv = c.get("implied_volatility") or 0
        exp = d.get("expiration_date")
        if k is None or ctype not in ("call", "put") or not oi or not (lo <= k <= hi):
            continue
        # expiry window
        T = None
        if exp:
            try:
                dte = (date.fromisoformat(exp) - today).days
                if dte < 0 or dte > EXPIRY_DTE_MAX:
                    continue
                T = max(dte, 0) / 365.0
            except ValueError:
                continue
        n_exp.add(exp)
        if g is not None:
            gex = g * oi * coef
            per_strike[k] = per_strike.get(k, 0.0) + (gex if ctype == "call" else -gex)
            (call_gex if ctype == "call" else put_gex)[k] = \
                (call_gex if ctype == "call" else put_gex).get(k, 0.0) + gex
        if T and iv:
            bs_contracts.append((k, ctype, oi, iv, T))
        used += 1

    if not per_strike:
        return {"underlying": underlying, "error": "no usable contracts in band", "spot": spot}

    total_gex = sum(per_strike.values())
    # gamma flip via BS spot-profile zero-cross (scan +/-15% of spot, fine step)
    flip = None
    flip_note = None
    if bs_contracts:
        s_lo, s_hi = spot * 0.85, spot * 1.15
        steps = 240
        prev_s = s_lo
        prev_v = _net_gex_at(prev_s, bs_contracts)
        for i in range(1, steps + 1):
            s = s_lo + (s_hi - s_lo) * i / steps
            v = _net_gex_at(s, bs_contracts)
            if (prev_v <= 0 < v) or (prev_v >= 0 > v):
                span = v - prev_v
                flip = prev_s + (s - prev_s) * (0 - prev_v) / span if span else s
                break
            prev_s, prev_v = s, v
        if flip is None:
            flip_note = ("no zero-cross in +/-15%% of spot — deep %s regime"
                         % ("long-gamma" if total_gex > 0 else "short-gamma"))
    call_wall = max(call_gex, key=call_gex.get) if call_gex else None
    put_wall = max(put_gex, key=put_gex.get) if put_gex else None
    # king_node = strike carrying the largest |net GEX| (dominant gamma node).
    king_node = max(per_strike, key=lambda k: abs(per_strike[k])) if per_strike else None
    # magnets / strikes array for chart consumers (top |net GEX| first)
    strikes_arr = [{"strike": round(k, 2), "net_gex": round(per_strike[k], 0),
                    "call_gex": round(call_gex.get(k, 0.0), 0),
                    "put_gex": round(put_gex.get(k, 0.0), 0)}
                   for k in sorted(per_strike)]
    magnets = sorted(strikes_arr, key=lambda x: -abs(x["net_gex"]))[:5]
    # regime that MATCHES state: above flip = long-gamma/stable, below = short/volatile
    if flip is not None:
        above = spot >= flip
        regime_label = ("LONG GAMMA · stable (spot above flip)" if above
                        else "SHORT GAMMA · volatile (spot below flip)")
    else:
        regime_label = ("LONG GAMMA · stable" if total_gex > 0 else "SHORT GAMMA · volatile")
    return {
        "underlying": underlying, "spot": round(spot, 2),
        "total_gex": total_gex,
        "regime": regime_label,
        "king_node": (round(king_node, 2) if king_node is not None else None),
        "magnets": magnets, "strikes": strikes_arr,
        "gamma_flip": (round(flip, 2) if flip else None),
        "flip_note": flip_note,
        "flip_vs_spot_pct": (round((flip / spot - 1) * 100, 2) if flip else None),
        "call_wall": call_wall, "put_wall": put_wall,
        "contracts_used": used, "expirations_used": len(n_exp),
        "band": "strikes +/-%d%% of spot, expiries <=%dDTE" % (int(STRIKE_BAND * 100), EXPIRY_DTE_MAX),
        "flip_method": "BS re-gamma net-GEX zero-cross across spot (not cumulative-across-strikes)",
        "convention": "dealers long calls / short puts (SqueezeMetrics); call +, put -",
        "per_strike": {round(k, 2): round(v, 0) for k, v in sorted(per_strike.items())},
        "asof": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),  # HM-TZ Stage 3: space-UTC (DB column)
    }


# ── HM-FLOW (aggregate — real dims only; print-level marked UNAVAILABLE) ──────
def compute_flow_aggregate(underlying: str, chain: list | None = None,
                           premium_floor: float = PREMIUM_FLOOR) -> dict:
    """Per-symbol AGGREGATE options flow from snapshot day.volume + OI.
    Print-level dims (sweep/block, at-ask/at-bid, per-print premium) are
    UNAVAILABLE on this tier and are reported as such — NOT faked."""
    chain = chain if chain is not None else fetch_chain(underlying)
    if not chain:
        return {"underlying": underlying, "error": "no chain"}
    call_notional = put_notional = 0.0
    unusual = []
    for c in chain:
        d = c.get("details") or {}
        ctype = d.get("contract_type")
        day = c.get("day") or {}
        vol = day.get("volume") or 0
        px = day.get("vwap") or day.get("close") or 0
        oi = c.get("open_interest") or 0
        if not vol or not px or ctype not in ("call", "put"):
            continue
        notional = vol * px * CONTRACT_MULT
        if ctype == "call":
            call_notional += notional
        else:
            put_notional += notional
        vol_oi = (vol / oi) if oi else float("inf")
        if vol_oi >= UNUSUAL_VOL_OI and notional >= premium_floor:
            unusual.append({"strike": d.get("strike_price"), "type": ctype,
                            "exp": d.get("expiration_date"), "volume": vol,
                            "oi": oi, "vol_oi": round(vol_oi, 2),
                            "notional": round(notional), "opening_proxy": vol_oi >= 1.0})
    net = call_notional - put_notional
    unusual.sort(key=lambda x: -x["notional"])
    return {
        "underlying": underlying,
        "call_notional": round(call_notional), "put_notional": round(put_notional),
        "net_notional": round(net),
        "lean": ("bullish" if net > 0 else "bearish" if net < 0 else "neutral"),
        "cp_notional_ratio": (round(call_notional / put_notional, 3) if put_notional else None),
        "unusual_contracts": unusual[:25], "unusual_count": len(unusual),
        # honest gaps — needed feeds are 403 on this tier:
        "sweep_vs_block": "UNAVAILABLE (needs /v3/trades tape — Polygon tier upgrade)",
        "at_ask_vs_at_bid": "UNAVAILABLE (needs /v3/quotes NBBO — Polygon tier upgrade)",
        "per_print_premium": "UNAVAILABLE (aggregate day-volume only, not per-print)",
        "opening_closing": "ESTIMATED via vol/OI proxy; true confirm = next-day OI delta",
        "spread_leg_filter": "n/a at aggregate level (is_spread_leg is print-level)",
        "premium_floor": premium_floor,
        "asof": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),  # HM-TZ Stage 3: space-UTC (DB column)
    }


# ── Persistence (new non-sacred DB; preserve as validation substrate) ─────────
def _db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS gex_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            underlying TEXT, asof TEXT, spot REAL, total_gex REAL, regime TEXT,
            gamma_flip REAL, call_wall REAL, put_wall REAL, contracts_used INTEGER,
            per_strike_json TEXT, source TEXT DEFAULT 'polygon', created_at TEXT DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS flow_aggregates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            underlying TEXT, asof TEXT, call_notional REAL, put_notional REAL, net_notional REAL,
            lean TEXT, cp_notional_ratio REAL, unusual_count INTEGER, unusual_json TEXT,
            sweep_status TEXT, aggressor_status TEXT, source TEXT DEFAULT 'polygon',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP);
        CREATE INDEX IF NOT EXISTS idx_gex_u ON gex_snapshots(underlying, asof);
        CREATE INDEX IF NOT EXISTS idx_flow_u ON flow_aggregates(underlying, asof);
    """)
    return conn


def persist(gex: dict | None, flow: dict | None) -> None:
    import json
    conn = _db()
    try:
        if gex and not gex.get("error"):
            conn.execute("INSERT INTO gex_snapshots (underlying,asof,spot,total_gex,regime,gamma_flip,"
                         "call_wall,put_wall,contracts_used,per_strike_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (gex["underlying"], gex["asof"], gex["spot"], gex["total_gex"], gex["regime"],
                          gex["gamma_flip"], gex["call_wall"], gex["put_wall"], gex["contracts_used"],
                          json.dumps(gex["per_strike"])))
        if flow and not flow.get("error"):
            conn.execute("INSERT INTO flow_aggregates (underlying,asof,call_notional,put_notional,net_notional,"
                         "lean,cp_notional_ratio,unusual_count,unusual_json,sweep_status,aggressor_status) "
                         "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                         (flow["underlying"], flow["asof"], flow["call_notional"], flow["put_notional"],
                          flow["net_notional"], flow["lean"], flow["cp_notional_ratio"], flow["unusual_count"],
                          json.dumps(flow["unusual_contracts"]), flow["sweep_vs_block"], flow["at_ask_vs_at_bid"]))
        conn.commit()
    finally:
        conn.close()


def collect(symbols=("SPY", "QQQ")) -> dict:
    """Observation-only collection: GEX + aggregate flow for each symbol, persisted."""
    res = {}
    for u in symbols:
        chain = fetch_chain(u)
        spot = fetch_spot(u)
        gex = compute_gex(u, chain=chain, spot=spot)
        flow = compute_flow_aggregate(u, chain=chain)
        persist(gex, flow)
        res[u] = {"gex": gex, "flow": flow}
    return res


# ── Intraday in-process cache (HM-GEX-CANONICAL) ──────────────────────────────
# Serves the live Bridge panels every poll. main.py refreshes it ~every 15 min
# during RTH. The daily-close collect() (cron) is what writes flow_gex.db — the
# clean one-row/day validation series. Intraday recomputes never touch the series.
_LATEST = {"data": {}, "ts": None}


def compute_pair(symbols=("SPY", "QQQ")) -> dict:
    """Compute GEX (+ aggregate flow) for symbols WITHOUT persisting."""
    out = {}
    for u in symbols:
        chain = fetch_chain(u)
        spot = fetch_spot(u)
        out[u] = {"gex": compute_gex(u, chain=chain, spot=spot),
                  "flow": compute_flow_aggregate(u, chain=chain)}
    return out


def refresh_latest(symbols=("SPY", "QQQ")) -> dict:
    """Recompute + update the in-process cache (called by main.py every ~15 min RTH)."""
    _LATEST["data"] = compute_pair(symbols)
    _LATEST["ts"] = datetime.now(timezone.utc).isoformat()
    return _LATEST


def get_latest() -> dict:
    """Latest cached intraday compute (instant; empty dict until first refresh)."""
    return dict(_LATEST)


if __name__ == "__main__":
    import json
    syms = sys.argv[1:] or ["SPY", "QQQ"]
    r = collect(tuple(syms))
    for u, d in r.items():
        g, f = d["gex"], d["flow"]
        print(f"\n{'='*70}\n{u}  spot={g.get('spot')}\n{'='*70}")
        print(f"  [GEX] total={g.get('total_gex'):.3e}  {g.get('regime')}  "
              f"flip={g.get('gamma_flip')}  call_wall={g.get('call_wall')}  put_wall={g.get('put_wall')}  "
              f"(contracts={g.get('contracts_used')})" if not g.get("error") else f"  [GEX] {g.get('error')}")
        print(f"  [FLOW] lean={f.get('lean')}  net_notional=${f.get('net_notional'):,}  "
              f"C/P={f.get('cp_notional_ratio')}  unusual={f.get('unusual_count')}  "
              f"| sweep/block={f.get('sweep_vs_block','?')[:11]}  aggressor=UNAVAILABLE")
