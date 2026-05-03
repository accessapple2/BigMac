#!/usr/bin/env python3
"""Daily snapshot of Schwab portfolio. Idempotent on (date, account)."""
import sys, os, json, sqlite3, datetime
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from engine.alpaca_bridge import alpaca

RH = os.path.join(ROOT, "data", "real_holdings.json")
DB = os.path.join(ROOT, "data", "trader.db")
CACHE = os.path.join(ROOT, "data", "sector_cache.json")

def load_cache():
    try:
        with open(CACHE) as f: return json.load(f)
    except Exception: return {}

def save_cache(c):
    try:
        with open(CACHE, "w") as f: json.dump(c, f, indent=2)
    except Exception: pass

def get_sector(sym, cache):
    if sym in cache: return cache[sym]
    try:
        import yfinance as yf
        info = yf.Ticker(sym).info or {}
        sec = info.get("sector") or "Unknown"
    except Exception as e:
        print(f"[warn] sector lookup failed for {sym}: {e}")
        sec = "Unknown"
    cache[sym] = sec
    return sec

def main():
    with open(RH) as f: rh = json.load(f)
    schwab = (rh.get("accounts") or {}).get("schwab") or {}
    cash = float(schwab.get("cash_balance") or 0)
    positions = schwab.get("positions") or []
    symbols = [p["symbol"] for p in positions]
    prices = alpaca.latest_prices(symbols) if symbols else {}

    cache = load_cache()
    sectors = {}
    pv = 0.0
    for p in positions:
        sym = p["symbol"]
        qty = float(p.get("qty") or 0)
        last = prices.get(sym) or 0
        mv = last * qty
        pv += mv
        sec = get_sector(sym, cache)
        sectors[sec] = sectors.get(sec, 0) + mv
    save_cache(cache)
    if cash > 0: sectors["Cash"] = cash

    total = pv + cash
    today = datetime.date.today().isoformat()
    now = datetime.datetime.now().isoformat(timespec="seconds")

    conn = sqlite3.connect(DB)
    conn.execute("""
        INSERT INTO real_portfolio_history
            (recorded_at, date, account, total_value, cash, positions_value, sector_breakdown)
        VALUES (?, ?, 'schwab', ?, ?, ?, ?)
        ON CONFLICT(date, account) DO UPDATE SET
            recorded_at=excluded.recorded_at,
            total_value=excluded.total_value,
            cash=excluded.cash,
            positions_value=excluded.positions_value,
            sector_breakdown=excluded.sector_breakdown
    """, (now, today, round(total,2), round(cash,2), round(pv,2), json.dumps(sectors)))
    conn.commit()
    conn.close()
    print(f"OK snapshot {today}: total=${total:,.2f} cash=${cash:,.2f} positions=${pv:,.2f} sectors={len(sectors)}")

if __name__ == "__main__":
    main()
