"""
SwingDesk Backend — Solo Swing Trading System
FastAPI + SQLite + Polygon.io real data
Phase 1: Real data, manual-assist execution, NO Alpaca wiring yet
Run: cd ~/autonomous-trader && python3 swingdesk/backend.py
     OR: uvicorn swingdesk.backend:app --port 8889
NOTE: reload disabled (HM-OTASTY-NO-RELOAD 2026-05-29) — StatReload pegged a
      full core + churned the worker on file touches, polluting the 30-shadow-
      trade gate soak. Do NOT re-add --reload to the production launch.
"""

import sqlite3, json, math, os, urllib.request, urllib.parse
from datetime import datetime, timedelta
from typing import Optional, List
from contextlib import contextmanager
from pathlib import Path

# ── ENV LOADER ────────────────────────────────────────────────────────────────
def _load_env():
    """Load swingdesk-local .env (O-Tasty isolated creds).
    HM-OTASTY-ENV-WIRE 2026-05-27: this build originally read
    ~/autonomous-trader/.env (the MAIN fleet) — repointed to the .env next to
    this file so the O-Tasty paper account stays isolated from the fleet."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        print(f"[SwingDesk] .env not found at {env_path}")
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and val and key not in os.environ:
            os.environ[key] = val

_load_env()

POLYGON_KEY   = os.environ.get("POLYGON_API_KEY", "")
ALPACA_KEY    = os.environ.get("ALPACA_API_KEY", "")     # loaded but NOT used yet
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")  # loaded but NOT used yet
ALPACA_URL    = "https://paper-api.alpaca.markets"

# ── CONFIG ────────────────────────────────────────────────────────────────────
ACCOUNT_SIZE          = 52340.00
MAX_RISK_PCT          = 1.0
MAX_POSITIONS         = 6
DAILY_LOSS_LIMIT      = ACCOUNT_SIZE * 0.02
MONTHLY_DD_LIMIT      = ACCOUNT_SIZE * 0.06
EARNINGS_BLACKOUT_DAYS = 21
MIN_RR_RATIO          = 2.0

DB_PATH = Path.home() / "autonomous-trader" / "swingdesk.db"

UNIVERSE = [
    "AAPL","MSFT","NVDA","AMD","META","GOOGL","AMZN","TSLA",
    "JPM","GS","BAC","XOM","CVX","LLY","UNH","SMCI","PLTR",
    "SPY","QQQ","SOFI","COIN","MSTR","ARM","AMAT","LRCX"
]

# ── DATABASE ──────────────────────────────────────────────────────────────────
def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL,
            direction       TEXT NOT NULL,
            setup           TEXT NOT NULL,
            entry_price     REAL,
            stop_price      REAL,
            target_price    REAL,
            shares          INTEGER,
            risk_dollars    REAL,
            rr_ratio        REAL,
            status          TEXT DEFAULT 'planned',
            exit_price      REAL,
            pnl             REAL,
            r_multiple      REAL,
            notes           TEXT,
            opened_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
            closed_at       DATETIME,
            broker_order_id TEXT
        );
        CREATE TABLE IF NOT EXISTS signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT,
            signal_type TEXT,
            setup       TEXT,
            description TEXT,
            score       REAL,
            price       REAL,
            rsi         REAL,
            vol_ratio   REAL,
            ema20       REAL,
            ema50       REAL,
            acted_on    INTEGER DEFAULT 0,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            date                        TEXT PRIMARY KEY,
            realized_pnl                REAL DEFAULT 0,
            trades_taken                INTEGER DEFAULT 0,
            circuit_breaker_triggered   INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS candle_cache (
            symbol      TEXT,
            timeframe   TEXT,
            bar_date    TEXT,
            open        REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY (symbol, timeframe, bar_date)
        );
        """)

@contextmanager
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

# ── POLYGON CLIENT ─────────────────────────────────────────────────────────────
POLYGON_BASE = "https://api.polygon.io"

def _polygon_get(path: str, params: dict = {}) -> dict:
    params["apiKey"] = POLYGON_KEY
    qs = urllib.parse.urlencode(params)
    url = f"{POLYGON_BASE}{path}?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "SwingDesk/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e), "results": []}

def get_candles_polygon(symbol: str, days: int = 90, tf: str = "day") -> list:
    """Fetch OHLCV bars from Polygon. Caches to DB."""
    end   = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days + 40)).strftime("%Y-%m-%d")

    # Check cache first (only for daily)
    if tf == "day":
        with get_db() as db:
            rows = db.execute("""
                SELECT bar_date, open, high, low, close, volume
                FROM candle_cache WHERE symbol=? AND timeframe=?
                AND bar_date >= ? ORDER BY bar_date ASC
            """, (symbol, tf, start)).fetchall()
            if len(rows) >= 50:
                return [{"t": r["bar_date"], "o": r["open"], "h": r["high"],
                         "l": r["low"], "c": r["close"], "v": r["volume"]}
                        for r in rows]

    data = _polygon_get(
        f"/v2/aggs/ticker/{symbol}/range/1/{tf}/{start}/{end}",
        {"adjusted": "true", "sort": "asc", "limit": "300"}
    )
    bars = data.get("results", [])

    # Cache daily bars
    if tf == "day" and bars:
        with get_db() as db:
            for b in bars:
                ts = datetime.fromtimestamp(b["t"] / 1000).strftime("%Y-%m-%d")
                db.execute("""
                    INSERT OR REPLACE INTO candle_cache
                    (symbol, timeframe, bar_date, open, high, low, close, volume)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (symbol, tf, ts, b["o"], b["h"], b["l"], b["c"], b["v"]))

    return bars

def get_quote_polygon(symbol: str) -> dict:
    """Last trade / snapshot."""
    data = _polygon_get(f"/v2/last/trade/{symbol}")
    return data.get("results", {})

def get_snapshot_polygon(symbols: list) -> dict:
    """Batch snapshot for watchlist prices."""
    tickers = ",".join(symbols)
    data = _polygon_get(f"/v2/snapshot/locale/us/markets/stocks/tickers",
                        {"tickers": tickers})
    snaps = {}
    for item in data.get("tickers", []):
        sym = item["ticker"]
        day = item.get("day", {})
        prev = item.get("prevDay", {})
        last = item.get("lastTrade", {})
        price = last.get("p") or day.get("c") or 0
        prev_c = prev.get("c") or 1
        chg_pct = ((price - prev_c) / prev_c * 100) if prev_c else 0
        snaps[sym] = {
            "price":   round(price, 2),
            "chg_pct": round(chg_pct, 2),
            "volume":  day.get("v", 0),
            "vwap":    day.get("vw", 0)
        }
    return snaps

# ── TECHNICAL INDICATORS ──────────────────────────────────────────────────────
def calc_ema(prices: list, period: int) -> list:
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for p in prices[period:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema

def calc_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al == 0:
        return 100.0
    return round(100 - (100 / (1 + ag / al)), 1)

def calc_atr(bars: list, period: int = 14) -> float:
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["h"], bars[i]["l"], bars[i-1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return round(sum(trs[-period:]) / period, 2)

def score_symbol(symbol: str) -> Optional[dict]:
    """Score a symbol for swing entry quality. Returns None if insufficient data."""
    bars = get_candles_polygon(symbol, days=120)
    if len(bars) < 55:
        return None

    closes  = [b["c"] for b in bars]
    vols    = [b["v"] for b in bars]
    ema20   = calc_ema(closes, 20)
    ema50   = calc_ema(closes, 50)
    rsi14   = calc_rsi(closes[-30:])
    atr14   = calc_atr(bars[-20:])
    curr    = closes[-1]
    e20     = ema20[-1] if ema20 else 0
    e50     = ema50[-1] if ema50 else 0
    avg_vol = sum(vols[-20:]) / 20 if vols else 1
    curr_vol = vols[-1]

    score = 0
    setup = "No Setup"
    signal = "watch"

    # Trend: above both EMAs
    if curr > e20 > e50:
        score += 20
        signal = "bull"
    elif curr < e20 < e50:
        score += 20
        signal = "bear"

    # EMA pullback (within 1.5% of 20 EMA)
    if abs(curr - e20) / e20 < 0.015 and signal == "bull":
        score += 30
        setup = "EMA Pullback"

    # Bull flag: recent 3-bar consolidation after advance
    if len(closes) >= 8:
        advance = (closes[-5] - closes[-8]) / closes[-8]
        consol  = max(closes[-4:]) - min(closes[-4:])
        if advance > 0.04 and consol / closes[-1] < 0.02:
            score += 25
            setup = "Bull Flag"

    # RSI healthy reset zone
    if 38 <= rsi14 <= 58:
        score += 15
    elif rsi14 > 70:
        score -= 15  # overbought

    # Volume confirmation
    vol_ratio = curr_vol / avg_vol if avg_vol else 1
    if vol_ratio > 1.3:
        score += 10

    # Suggested stop/target via ATR
    stop_atr   = round(curr - 1.5 * atr14, 2) if atr14 else round(curr * 0.97, 2)
    target_atr = round(curr + 3.0 * atr14, 2) if atr14 else round(curr * 1.06, 2)

    return {
        "symbol":    symbol,
        "score":     min(max(score, 0), 100),
        "setup":     setup,
        "signal":    signal,
        "price":     round(curr, 2),
        "rsi":       rsi14,
        "ema20":     round(e20, 2),
        "ema50":     round(e50, 2),
        "atr":       atr14,
        "vol_ratio": round(vol_ratio, 2),
        "sug_stop":  stop_atr,
        "sug_target": target_atr,
        "description": f"RSI {rsi14} | Vol {round(vol_ratio,1)}x avg | ATR {atr14}"
    }

# ── FASTAPI APP ───────────────────────────────────────────────────────────────
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="SwingDesk", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# Serve frontend from same directory
_frontend = Path(__file__).parent / "index.html"
if _frontend.exists():
    app.mount("/static", StaticFiles(directory=str(_frontend.parent)), name="static")

# ── MODELS ────────────────────────────────────────────────────────────────────
class SizeRequest(BaseModel):
    entry: float
    stop: float
    target: float
    account_size: Optional[float] = None
    risk_pct: Optional[float] = None

class TradeRequest(BaseModel):
    symbol: str
    direction: str
    setup: str
    entry_price: float
    stop_price: float
    target_price: float
    shares: int
    risk_dollars: float
    rr_ratio: float
    notes: Optional[str] = ""

class CloseRequest(BaseModel):
    trade_id: int
    exit_price: float
    notes: Optional[str] = ""

# ── ROUTES ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    polygon_ok = bool(POLYGON_KEY)
    alpaca_ok  = bool(ALPACA_KEY)   # loaded, not yet wired
    return {
        "status":        "ok",
        "time":          datetime.now().isoformat(),
        "polygon":       "configured" if polygon_ok else "MISSING",
        "alpaca":        "loaded (not wired yet)" if alpaca_ok else "MISSING",
        "alpaca_phase":  "Phase 2 — not yet active",
        "db":            str(DB_PATH)
    }

@app.get("/api/watchlist")
def get_watchlist():
    """Live prices for watchlist symbols from Polygon snapshot."""
    snaps = get_snapshot_polygon(UNIVERSE[:12])
    return snaps

@app.get("/api/candles/{symbol}")
def get_candles(symbol: str, tf: str = "day", days: int = 90):
    """OHLCV bars for charting — real Polygon data."""
    bars = get_candles_polygon(symbol.upper(), days=days, tf=tf)
    formatted = []
    for b in bars:
        # Polygon returns epoch ms for stocks
        if isinstance(b.get("t"), (int, float)):
            ts = int(b["t"] / 1000) if b["t"] > 1e10 else int(b["t"])
        else:
            ts = b.get("t", 0)
        formatted.append({
            "time":  ts,
            "open":  b["o"], "high": b["h"],
            "low":   b["l"], "close": b["c"],
            "volume": b.get("v", 0)
        })
    return formatted

@app.get("/api/quote/{symbol}")
def get_quote(symbol: str):
    snap = get_snapshot_polygon([symbol.upper()])
    return snap.get(symbol.upper(), {"price": 0, "chg_pct": 0})

@app.post("/api/size")
def calculate_size(req: SizeRequest):
    acct   = req.account_size or ACCOUNT_SIZE
    rpct   = req.risk_pct or MAX_RISK_PCT
    risk_d = acct * (rpct / 100)
    per_sh = abs(req.entry - req.stop)
    if per_sh == 0:
        raise HTTPException(400, "Entry and stop cannot be equal")
    shares   = math.floor(risk_d / per_sh)
    pos_size = shares * req.entry
    gain     = shares * abs(req.target - req.entry)
    rr       = abs(req.target - req.entry) / per_sh
    return {
        "shares":         shares,
        "risk_dollars":   round(risk_d, 2),
        "position_size":  round(pos_size, 2),
        "max_gain":       round(gain, 2),
        "rr_ratio":       round(rr, 2),
        "pct_of_account": round(pos_size / acct * 100, 1),
        "approved":       rr >= MIN_RR_RATIO and shares > 0
    }

@app.get("/api/risk-gate")
def risk_gate():
    today = datetime.now().date().isoformat()
    with get_db() as db:
        row = db.execute(
            "SELECT * FROM daily_stats WHERE date=?", (today,)).fetchone()
        open_count = db.execute(
            "SELECT COUNT(*) as c FROM trades WHERE status='open'"
        ).fetchone()["c"]
    day_pnl = row["realized_pnl"] if row else 0.0
    cb_hit  = row["circuit_breaker_triggered"] if row else 0
    return {
        "circuit_breaker_active": bool(cb_hit),
        "daily_loss":            round(day_pnl, 2),
        "daily_loss_limit":      round(-DAILY_LOSS_LIMIT, 2),
        "daily_limit_ok":        day_pnl > -DAILY_LOSS_LIMIT,
        "open_positions":        open_count,
        "max_positions":         MAX_POSITIONS,
        "positions_ok":          open_count < MAX_POSITIONS,
        "can_trade":             not cb_hit and open_count < MAX_POSITIONS and day_pnl > -DAILY_LOSS_LIMIT,
        "alpaca_wired":          False   # Phase 2
    }

@app.post("/api/trade/plan")
def plan_trade(req: TradeRequest):
    """
    Phase 1: Log planned trade to DB. Does NOT submit to Alpaca yet.
    Phase 2: This becomes the Alpaca submission point.
    """
    gate = risk_gate()
    if not gate["can_trade"]:
        raise HTTPException(400, f"Risk gate blocked: {gate}")
    with get_db() as db:
        cursor = db.execute("""
            INSERT INTO trades
              (symbol, direction, setup, entry_price, stop_price,
               target_price, shares, risk_dollars, rr_ratio, notes, status)
            VALUES (?,?,?,?,?,?,?,?,?,?,'planned')
        """, (req.symbol, req.direction, req.setup, req.entry_price,
              req.stop_price, req.target_price, req.shares,
              req.risk_dollars, req.rr_ratio, req.notes))
        trade_id = cursor.lastrowid
        db.execute("INSERT OR IGNORE INTO daily_stats (date) VALUES (?)",
                   (datetime.now().date().isoformat(),))
        db.execute("UPDATE daily_stats SET trades_taken=trades_taken+1 WHERE date=?",
                   (datetime.now().date().isoformat(),))
    return {
        "status":    "planned",           # NOT 'submitted' until Phase 2
        "trade_id":  trade_id,
        "note":      "Alpaca execution wired in Phase 2. Trade logged to SwingDesk DB.",
        "symbol":    req.symbol,
        "shares":    req.shares,
        "entry":     req.entry_price,
        "stop":      req.stop_price,
        "target":    req.target_price
    }

@app.post("/api/trade/close")
def close_trade(req: CloseRequest):
    with get_db() as db:
        trade = db.execute(
            "SELECT * FROM trades WHERE id=?", (req.trade_id,)).fetchone()
        if not trade:
            raise HTTPException(404, "Trade not found")
        direction = trade["direction"]
        if direction == "LONG":
            pnl = (req.exit_price - trade["entry_price"]) * trade["shares"]
        else:
            pnl = (trade["entry_price"] - req.exit_price) * trade["shares"]
        per_sh_risk = abs(trade["entry_price"] - trade["stop_price"])
        r_multiple  = (pnl / trade["shares"]) / per_sh_risk if per_sh_risk > 0 else 0
        db.execute("""
            UPDATE trades SET status='closed', exit_price=?, pnl=?,
              r_multiple=?, notes=?, closed_at=CURRENT_TIMESTAMP WHERE id=?
        """, (req.exit_price, round(pnl, 2), round(r_multiple, 2),
              req.notes, req.trade_id))
        db.execute("INSERT OR IGNORE INTO daily_stats (date) VALUES (?)",
                   (datetime.now().date().isoformat(),))
        db.execute("UPDATE daily_stats SET realized_pnl=realized_pnl+? WHERE date=?",
                   (round(pnl, 2), datetime.now().date().isoformat()))
    return {"trade_id": req.trade_id, "pnl": round(pnl, 2),
            "r_multiple": round(r_multiple, 2),
            "outcome": "win" if pnl > 0 else "loss"}

@app.get("/api/positions")
def get_positions():
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM trades WHERE status IN ('open','planned') ORDER BY opened_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/journal")
def get_journal(limit: int = 50):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC LIMIT ?",
            (limit,)).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/stats")
def get_stats():
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    with get_db() as db:
        rows = db.execute(
            "SELECT pnl, r_multiple FROM trades WHERE status='closed' AND closed_at > ?",
            (cutoff,)).fetchall()
    if not rows:
        return {"trades": 0, "win_rate": 0, "avg_rr": 0, "net_pnl": 0}
    wins = [r for r in rows if r["pnl"] > 0]
    return {
        "trades":      len(rows),
        "wins":        len(wins),
        "losses":      len(rows) - len(wins),
        "win_rate":    round(len(wins) / len(rows) * 100, 1),
        "avg_rr":      round(sum(r["r_multiple"] for r in rows) / len(rows), 2),
        "net_pnl":     round(sum(r["pnl"] for r in rows), 2),
        "best_trade":  round(max(r["pnl"] for r in rows), 2),
        "worst_trade": round(min(r["pnl"] for r in rows), 2)
    }

@app.get("/api/scan")
def run_scanner():
    """
    Live pre-market scanner using real Polygon candles.
    Scores each symbol, writes signals to DB, returns top picks.
    """
    if not POLYGON_KEY:
        raise HTTPException(503, "POLYGON_API_KEY not configured")
    results = []
    for sym in UNIVERSE:
        try:
            result = score_symbol(sym)
            if result and result["score"] >= 45 and result["setup"] != "No Setup":
                results.append(result)
                with get_db() as db:
                    db.execute("""
                        INSERT INTO signals
                          (symbol, signal_type, setup, description, score,
                           price, rsi, vol_ratio, ema20, ema50)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (sym, result["signal"], result["setup"],
                          result["description"], result["score"],
                          result["price"], result["rsi"], result["vol_ratio"],
                          result["ema20"], result["ema50"]))
        except Exception as e:
            print(f"[scan] {sym} error: {e}")
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:10]

@app.get("/api/signals")
def get_signals(limit: int = 20):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?",
            (limit,)).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/circuit-breaker/trigger")
def trigger_circuit_breaker():
    today = datetime.now().date().isoformat()
    with get_db() as db:
        db.execute("INSERT OR IGNORE INTO daily_stats (date) VALUES (?)", (today,))
        db.execute("UPDATE daily_stats SET circuit_breaker_triggered=1 WHERE date=?", (today,))
    return {"status": "engaged", "date": today}

@app.get("/api/circuit-breaker/reset")
def reset_circuit_breaker():
    today = datetime.now().date().isoformat()
    with get_db() as db:
        db.execute("UPDATE daily_stats SET circuit_breaker_triggered=0 WHERE date=?", (today,))
    return {"status": "reset", "date": today}

@app.on_event("startup")
def startup():
    init_db()
    print(f"[SwingDesk] DB: {DB_PATH}")
    print(f"[SwingDesk] Polygon: {'OK ' + POLYGON_KEY[:4] + '...' if POLYGON_KEY else 'MISSING'}")
    print(f"[SwingDesk] Alpaca:  loaded (Phase 2 — not wired)")
    # HM-O-TASTY WAVE 8: start the SHADOW autopilot scheduler (A/B/C every 5 min
    # RTH + Loop E nightly 6 PM ET). Isolated daemon thread; zero-order.
    try:
        from shadow_autopilot import start_shadow_scheduler
        started = start_shadow_scheduler()
        print(f"[SwingDesk] Shadow autopilot scheduler: {'started' if started else 'already running'} (SHADOW, zero-order)")
    except Exception as e:
        print(f"[SwingDesk] Shadow scheduler start failed: {type(e).__name__}: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend:app", host="0.0.0.0", port=8889, reload=False)


# ═══════════════════════════════════════════════════════════════════════════════
# OPTIONS ENGINE ROUTES — TastyTrade Methodology
# ═══════════════════════════════════════════════════════════════════════════════
import sys
sys.path.insert(0, str(Path(__file__).parent))

try:
    from options_engine import (
        calc_ivr, scan_ivr, build_iron_condor, build_bull_put_spread,
        build_bear_call_spread, build_csp, find_target_expiration,
        get_spot, check_management, UNIVERSE as OPT_UNIVERSE
    )
    OPTIONS_ENGINE_OK = True
except Exception as e:
    OPTIONS_ENGINE_OK = False
    print(f"[SwingDesk] Options engine import failed: {e}")

class SpreadRequest(BaseModel):
    symbol:         str
    structure:      str        # iron_condor | bull_put | bear_call | csp
    expiration:     Optional[str] = None
    portfolio_size: Optional[float] = None
    notes:          Optional[str] = ""

class OptionsTradeRequest(BaseModel):
    symbol:         str
    structure:      str
    expiration:     str
    dte:            int
    credit:         float
    max_loss:       float
    contracts:      int
    legs:           dict
    profit_target:  float
    loss_limit:     float
    notes:          Optional[str] = ""

# SWINGDESK-W2: spread → Alpaca multi-leg (paper) submission.
try:
    import spread_executor as _spread_exec   # swingdesk/ already on sys.path (line ~615)
    import spread_fixtures as _spread_fix     # saved blueprints (e.g. ceg_hedge)
    SPREAD_EXEC_OK = True
except Exception:
    SPREAD_EXEC_OK = False

class SpreadSubmitRequest(BaseModel):
    legs:            Optional[list] = None   # [{underlying,option_type,strike,side}] (2-leg vertical)
    fixture:         Optional[str] = None    # OR a saved blueprint name (e.g. "ceg_hedge"); resolves legs at runtime
    net_debit_limit: Optional[float] = None  # None → mid from Alpaca quotes
    qty:             int = 1
    structure:       str = "vertical_spread"
    strategy:        str = "swingdesk_manual"
    dry_run:         bool = True    # DEFAULT TRUE — explicit false required to send
    force:           bool = False   # W2.1 — bypass the local idempotency guard (re-entry)
    idempotency_key: Optional[str] = None  # W2.1 — caller key for client_order_id
    action:          str = "open"   # W4 — "open" | "close" (close = reverse mleg)

@app.get("/api/options/ivr/{symbol}")
def get_ivr(symbol: str):
    """IV Rank for a single symbol."""
    if not OPTIONS_ENGINE_OK:
        raise HTTPException(503, "Options engine not available")
    return calc_ivr(symbol.upper())

@app.get("/api/options/scan")
def options_scan(limit: int = 15):
    """
    Full IVR scan — all UNIVERSE symbols, ranked by IVR.
    Returns only symbols with IVR >= 50 (sell signal).
    """
    if not OPTIONS_ENGINE_OK:
        raise HTTPException(503, "Options engine not available")
    if not POLYGON_KEY:
        raise HTTPException(503, "POLYGON_API_KEY not configured")
    results = scan_ivr(OPT_UNIVERSE)
    # Save to signals table
    sell_signals = [r for r in results if r.get("sell_signal")]
    with get_db() as db:
        for r in sell_signals[:10]:
            db.execute("""
                INSERT INTO signals
                  (symbol, signal_type, setup, description, score, price)
                VALUES (?,?,?,?,?,?)
            """, (r["symbol"], "options", f"IVR {r['ivr']}",
                  f"IVR={r['ivr']} IV={r['iv_current']}% DTE={r['dte']}",
                  r["ivr"], r["spot"]))
    return results[:limit]

@app.post("/api/options/build")
def build_spread(req: SpreadRequest):
    """
    Build a specific spread structure for a symbol.
    Auto-finds 45 DTE expiration if not provided.
    """
    if not OPTIONS_ENGINE_OK:
        raise HTTPException(503, "Options engine not available")
    sym  = req.symbol.upper()
    port = req.portfolio_size or ACCOUNT_SIZE
    ivr_data = calc_ivr(sym)
    iv   = ivr_data.get("iv_current", 25.0) or 25.0
    spot = get_spot(sym)
    if not spot:
        raise HTTPException(400, f"Could not get spot price for {sym}")
    exp  = req.expiration or find_target_expiration(sym)

    structure = req.structure.lower()
    if structure == "iron_condor":
        return build_iron_condor(sym, spot, iv, exp, port)
    elif structure in ("bull_put", "bull_put_spread"):
        return build_bull_put_spread(sym, spot, iv, exp, port)
    elif structure in ("bear_call", "bear_call_spread"):
        return build_bear_call_spread(sym, spot, iv, exp, port)
    elif structure == "csp":
        return build_csp(sym, spot, iv, exp, port)
    else:
        raise HTTPException(400, f"Unknown structure: {req.structure}")

@app.post("/api/swingdesk/spread/submit")
def submit_spread(req: SpreadSubmitRequest):
    """SWINGDESK-W2 — submit a 2-leg vertical to Alpaca PAPER as one atomic
    multi-leg LIMIT order (net-debit at mid, reject above width×0.5). Manual
    trigger only — no autonomous agent reaches this path in W2. RULE #1: Alpaca
    paper only; Schwab is never touched. dry_run defaults TRUE (returns the exact
    payload without sending); pass dry_run=false to actually submit."""
    if not SPREAD_EXEC_OK:
        raise HTTPException(503, "spread_executor not available")
    # Resolve legs: either explicit `legs` OR a saved `fixture` blueprint (not both).
    legs, structure, qty = req.legs, req.structure, req.qty
    if req.fixture:
        if req.legs:
            raise HTTPException(400, "supply either `legs` or `fixture`, not both")
        try:
            bp = _spread_fix.load_blueprint(req.fixture)
            resolved = _spread_fix.resolve_blueprint(bp)   # live spot + 30-45 DTE expiry
        except Exception as e:
            raise HTTPException(400, f"fixture '{req.fixture}': {type(e).__name__}: {e}")
        legs = resolved["legs"]
        structure = resolved.get("structure", structure)
        qty = resolved.get("qty", qty)
    if not legs:
        raise HTTPException(400, "provide `legs` or a `fixture` name")
    # Live submits respect the SwingDesk circuit breaker; dry-runs never send.
    if not req.dry_run:
        gate = risk_gate()
        if not gate.get("can_trade", False):
            raise HTTPException(400, f"Risk gate blocked: {gate}")
    try:
        result = _spread_exec.submit_spread(
            legs=legs, qty=qty, structure=structure,
            strategy=req.strategy, net_debit_limit=req.net_debit_limit,
            dry_run=req.dry_run, force=req.force,
            idempotency_key=req.idempotency_key, action=req.action,
        )
    except Exception as e:
        raise HTTPException(400, f"{type(e).__name__}: {e}")
    if not result.get("ok"):
        # Validation/rejection is a 200 with ok=False so callers see the reason;
        # only hard errors raise.
        if result.get("error"):
            raise HTTPException(400, result["error"])
    return result

@app.post("/api/options/trade/plan")
def plan_options_trade(req: OptionsTradeRequest):
    """Log a planned options trade to DB. No Alpaca submission yet (Phase 2)."""
    gate = risk_gate()
    if not gate["can_trade"]:
        raise HTTPException(400, f"Risk gate blocked: {gate}")
    legs_json = json.dumps(req.legs)
    with get_db() as db:
        cursor = db.execute("""
            INSERT INTO trades
              (symbol, direction, setup, entry_price, stop_price, target_price,
               shares, risk_dollars, rr_ratio, notes, status)
            VALUES (?,?,?,?,?,?,?,?,?,?,'planned')
        """, (req.symbol,
              "SHORT PREMIUM",
              req.structure,
              req.credit,
              req.loss_limit,
              req.profit_target,
              req.contracts,
              req.max_loss * req.contracts * 100,
              round(req.profit_target / max(req.loss_limit - req.profit_target, 0.01), 2),
              f"{req.structure} | legs:{legs_json} | exp:{req.expiration} | {req.notes}"))
        trade_id = cursor.lastrowid
        db.execute("INSERT OR IGNORE INTO daily_stats (date) VALUES (?)",
                   (datetime.now().date().isoformat(),))
        db.execute("UPDATE daily_stats SET trades_taken=trades_taken+1 WHERE date=?",
                   (datetime.now().date().isoformat(),))
    return {
        "status":   "planned",
        "trade_id": trade_id,
        "note":     "Phase 2: Alpaca bracket submission. Trade logged to swingdesk.db.",
        "symbol":   req.symbol,
        "structure": req.structure,
        "contracts": req.contracts,
        "credit":    req.credit,
        "expiration": req.expiration
    }

@app.get("/api/options/manage/{trade_id}")
def check_trade_management(trade_id: int, current_mark: float = 0.0):
    """Check if a trade needs to be managed (50% profit / 21 DTE / 2x loss)."""
    with get_db() as db:
        trade = db.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()
    if not trade:
        raise HTTPException(404, "Trade not found")
    notes = trade["notes"] or ""
    exp   = None
    if "exp:" in notes:
        try:
            exp = notes.split("exp:")[1].split("|")[0].strip()
        except:
            pass
    trade_dict = dict(trade)
    trade_dict["expiration"] = exp
    trade_dict["credit"]     = trade["entry_price"]
    return check_management(trade_dict, current_mark or trade["entry_price"] * 0.5)
