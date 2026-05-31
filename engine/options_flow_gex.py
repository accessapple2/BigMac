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

import os
import sqlite3
import sys
from datetime import datetime, timezone

import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

POLYGON_BASE = "https://api.polygon.io"
DB_PATH = os.path.join(_ROOT, "data", "flow_gex.db")
CONTRACT_MULT = 100
PREMIUM_FLOOR = 250_000          # $ — tunable; applies to aggregate notional here
UNUSUAL_VOL_OI = 1.0             # day.volume / OI >= this => 'unusual' (vol>OI opening proxy)

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
def fetch_chain(underlying: str, max_pages: int = 6) -> list:
    """Full(ish) option chain snapshot via /v3/snapshot/options/{u} + next_url."""
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
def compute_gex(underlying: str, chain: list | None = None, spot: float | None = None) -> dict:
    """Dealer GEX profile. Returns total GEX, gamma flip, call/put walls, per-strike net."""
    chain = chain if chain is not None else fetch_chain(underlying)
    spot = spot if spot is not None else fetch_spot(underlying)
    if not chain or not spot:
        return {"underlying": underlying, "error": "no chain/spot", "spot": spot}

    per_strike: dict = {}     # strike -> net GEX
    call_gex: dict = {}
    put_gex: dict = {}
    coef = CONTRACT_MULT * spot * spot * 0.01
    used = 0
    for c in chain:
        g = (c.get("greeks") or {}).get("gamma")
        oi = c.get("open_interest") or 0
        d = c.get("details") or {}
        k = d.get("strike_price")
        ctype = d.get("contract_type")
        if g is None or not oi or k is None or ctype not in ("call", "put"):
            continue
        gex = g * oi * coef
        signed = gex if ctype == "call" else -gex   # dealers long calls / short puts
        per_strike[k] = per_strike.get(k, 0.0) + signed
        if ctype == "call":
            call_gex[k] = call_gex.get(k, 0.0) + gex
        else:
            put_gex[k] = put_gex.get(k, 0.0) + gex
        used += 1

    if not per_strike:
        return {"underlying": underlying, "error": "no usable contracts", "spot": spot}

    total_gex = sum(per_strike.values())
    strikes = sorted(per_strike)
    # gamma flip: cumulative net GEX zero-cross (interpolated)
    cum = 0.0
    flip = None
    prev_k, prev_cum = None, 0.0
    for k in strikes:
        cum += per_strike[k]
        if prev_k is not None and (prev_cum <= 0 < cum or prev_cum >= 0 > cum):
            # linear interp between prev_k and k
            span = cum - prev_cum
            flip = prev_k + (k - prev_k) * (0 - prev_cum) / span if span else k
            break
        prev_k, prev_cum = k, cum
    call_wall = max(call_gex, key=call_gex.get) if call_gex else None     # largest +call GEX
    put_wall = max(put_gex, key=put_gex.get) if put_gex else None         # largest put GEX (|−|)
    return {
        "underlying": underlying, "spot": round(spot, 2),
        "total_gex": total_gex, "regime": ("positive/long-gamma" if total_gex > 0 else "negative/short-gamma"),
        "gamma_flip": (round(flip, 2) if flip else None),
        "call_wall": call_wall, "put_wall": put_wall,
        "contracts_used": used,
        "convention": "dealers long calls / short puts (SqueezeMetrics); call +, put -",
        "per_strike": {round(k, 2): round(v, 0) for k, v in sorted(per_strike.items())},
        "asof": datetime.now(timezone.utc).isoformat(),
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
        "asof": datetime.now(timezone.utc).isoformat(),
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
