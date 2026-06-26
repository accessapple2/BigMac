#!/usr/bin/env python3
"""
schwab_drawdown_alert.py -- Notify-only drawdown watcher for the REAL Schwab book.
Read-only, never trades, never touches Schwab auth. Safe during the 30-day passive
window because it only sends an NTFY heads-up. Fires once per (symbol,trigger) per
day, re-fires only if the loss deepens another REALERT_STEP percent.
"""
import json, os, sys, datetime as dt
from pathlib import Path
import requests

DAY_PCT      = -5.0
COST_PCT     = -8.0
REALERT_STEP = 3.0

NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "ollietrades-admin")  # <-- VERIFY vs Kirk
NTFY_URL   = f"https://ntfy.sh/{NTFY_TOPIC}"
STATE_PATH = Path(os.environ.get("DRAWDOWN_STATE", "data/drawdown_alert_state.json"))
HOLDINGS   = Path(os.environ.get("REAL_HOLDINGS", "data/real_holdings.json"))


def _ntfy(title, body, priority="default"):
    try:
        requests.post(NTFY_URL, data=body.encode("utf-8"),
                      headers={"Title": title, "Priority": priority}, timeout=6)
    except requests.RequestException as e:
        print(f"[drawdown] NTFY failed: {e}", file=sys.stderr)


def load_positions(test=False):
    """Read-only pull from the canonical Schwab mirror (sync_schwab_live.py writes it)."""
    if test:
        return [
            {"symbol": "VOO",  "qty": 8,  "price": 685.33, "day_pct": -0.53, "cost_pct": -0.53, "day_dollar": -29.39},
            {"symbol": "VST",  "qty": 16, "price": 167.06, "day_pct": +1.44, "cost_pct": +4.85, "day_dollar": +37.93},
            {"symbol": "PANW", "qty": 3,  "price": 286.73, "day_pct": -0.36, "cost_pct": +0.27, "day_dollar": -3.15},
            {"symbol": "MU",   "qty": 3,  "price": 1193.73,"day_pct": +0.01, "cost_pct": +0.01, "day_dollar": +0.38},
            {"symbol": "NVDA", "qty": 20, "price": 207.85, "day_pct": -1.11, "cost_pct": -1.98, "day_dollar": -46.70},
            {"symbol": "FCX",  "qty": 10, "price": 69.26,  "day_pct": +0.15, "cost_pct": +1.12, "day_dollar": +1.01},
            {"symbol": "WDC",  "qty": 3,  "price": 733.42, "day_pct": -5.37, "cost_pct": -5.37, "day_dollar": -124.75},
            {"symbol": "CEG",  "qty": 18, "price": 274.83, "day_pct": +0.36, "cost_pct": -0.74, "day_dollar": +17.61},
        ]
    try:
        raw = json.loads(HOLDINGS.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[drawdown] cannot read {HOLDINGS}: {e}", file=sys.stderr)
        return []
    src = (raw.get("schwab") or {}).get("positions") or []
    out = []
    for p in src:
        try:
            out.append({
                "symbol":     p["symbol"],
                "qty":        p.get("qty", 0),
                "price":      float(p.get("price", 0) or 0),
                "day_pct":    float(p.get("day_change_pct", 0) or 0),
                "cost_pct":   float(p.get("gain_pct", 0) or 0),
                "day_dollar": float(p.get("day_change_dollar", 0) or 0),
            })
        except (KeyError, TypeError, ValueError):
            continue  # skip malformed rows rather than crash the watcher
    return out


def _load_state():
    today = dt.date.today().isoformat()
    try:
        data = json.loads(STATE_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    return today, data.get(today, {})


def _save_state(today, day_state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps({today: day_state}, indent=2))


def _band(pct, threshold):
    if pct > threshold:
        return None
    return int((threshold - pct) // REALERT_STEP)


def evaluate(positions):
    today, day_state = _load_state()
    fires = []
    for p in positions:
        for trigger, pct, thresh in (("DAY", p["day_pct"], DAY_PCT),
                                     ("COST", p["cost_pct"], COST_PCT)):
            band = _band(pct, thresh)
            if band is None:
                continue
            key = f"{p['symbol']}:{trigger}"
            if band <= day_state.get(key, -1):
                continue
            day_state[key] = band
            label = "today" if trigger == "DAY" else "vs cost"
            msg = (f"{p['symbol']} {pct:+.2f}% {label} "
                   f"(${p['price']:.2f}, {p['qty']} sh, {p['day_dollar']:+.2f} day $)")
            fires.append((p["symbol"], trigger, band, msg))
    return today, day_state, fires


def main():
    test = "--test" in sys.argv
    positions = load_positions(test=test)
    if not positions:
        print("[drawdown] no positions loaded.")
        return
    today, day_state, fires = evaluate(positions)
    if not fires:
        print(f"[drawdown] {today}: no thresholds crossed "
              f"(DAY<{DAY_PCT}% / COST<{COST_PCT}%).")
        return
    for symbol, trigger, band, msg in fires:
        title = f"⚠️ DRAWDOWN: {symbol} ({trigger})"
        priority = "urgent" if band >= 1 else "high"
        print(f"[drawdown] FIRE {title} -> {msg}")
        if not test:
            _ntfy(title, msg, priority)
    if not test:
        _save_state(today, day_state)
    else:
        print(f"\n[drawdown] TEST mode: {len(fires)} alert(s) would have fired. "
              "No NTFY sent, state not written.")


if __name__ == "__main__":
    main()
