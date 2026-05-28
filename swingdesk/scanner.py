"""
SwingDesk Pre-Market Scanner
Run nightly: python scanner.py
Pulls candles from Polygon, scores setups, writes signals to DB.
"""
import sqlite3, json, math, urllib.request, os
from datetime import datetime
from pathlib import Path

# HM-OTASTY-ENV-WIRE 2026-05-27: this build ships no env loader. Load the
# swingdesk-local .env (isolated O-Tasty creds) so POLYGON_API_KEY resolves.
# Reads only the .env next to this file — never the main fleet .env.
def _load_local_env():
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())
_load_local_env()

POLYGON_KEY = os.getenv("POLYGON_API_KEY", "YOUR_KEY")
DB_PATH     = "swingdesk.db"

UNIVERSE = [
    "AAPL","MSFT","NVDA","AMD","META","GOOGL","AMZN","TSLA",
    "JPM","GS","XOM","LLY","UNH","SPY","QQQ","SMCI","PLTR"
]

def get_candles(symbol, days=60, tf="day"):
    end   = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now().replace(year=datetime.now().year-1)).strftime("%Y-%m-%d")
    url   = (f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/{tf}"
             f"/{start}/{end}?adjusted=true&sort=asc&limit=120&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            return data.get("results", [])
    except:
        return []

def ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100
    rs = avg_g / avg_l
    return round(100 - (100 / (1 + rs)), 1)

def score_setup(candles):
    """Score a symbol 0-100 for swing entry quality."""
    if len(candles) < 55:
        return None

    closes = [c["c"] for c in candles]
    vols   = [c["v"] for c in candles]

    ema20  = ema(closes, 20)
    ema50  = ema(closes, 50)
    rsi14  = rsi(closes[-30:])
    curr   = closes[-1]
    e20    = ema20[-1] if ema20 else 0
    e50    = ema50[-1] if ema50 else 0
    avg_vol = sum(vols[-20:]) / 20
    curr_vol = vols[-1]

    score  = 0
    setup  = "No Setup"
    signal = "watch"

    # Trend: price above both EMAs
    if curr > e20 > e50:
        score += 25
        signal = "bull"

    # Pullback to 20 EMA (within 1.5%)
    if abs(curr - e20) / e20 < 0.015:
        score += 30
        setup = "EMA Pullback"

    # RSI reset zone (40-55 = healthy pullback)
    if 38 <= rsi14 <= 58:
        score += 20

    # Volume surge
    if curr_vol > avg_vol * 1.3:
        score += 15

    # Not overbought
    if rsi14 < 70:
        score += 10

    return {
        "score":      score,
        "setup":      setup,
        "signal":     signal,
        "rsi":        rsi14,
        "ema20":      round(e20, 2),
        "ema50":      round(e50, 2),
        "price":      round(curr, 2),
        "vol_ratio":  round(curr_vol / avg_vol, 2)
    }

def run_scan():
    conn = sqlite3.connect(DB_PATH)
    results = []
    for sym in UNIVERSE:
        candles = get_candles(sym)
        if not candles:
            print(f"  {sym}: no data")
            continue
        result = score_setup(candles)
        if not result or result["score"] < 50:
            continue
        results.append((sym, result))
        conn.execute("""
            INSERT INTO signals (symbol, signal_type, setup, description, score)
            VALUES (?,?,?,?,?)
        """, (sym, result["signal"], result["setup"],
              f"RSI {result['rsi']} | Vol {result['vol_ratio']}x | EMA20 {result['ema20']}",
              result["score"]))
        print(f"  ✓ {sym}: score={result['score']} setup={result['setup']}")
    conn.commit(); conn.close()
    results.sort(key=lambda x: x[1]["score"], reverse=True)
    print(f"\nTop picks: {[r[0] for r in results[:5]]}")

if __name__ == "__main__":
    print(f"Scanner running {datetime.now()}")
    run_scan()
