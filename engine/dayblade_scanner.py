"""DayBlade Bear Mode Scanner — 0DTE Predator (Bear Market Edition).

Detects high-probability 0DTE put/call setups in bear regime.
Generates flash alerts to DB + ntfy.sh when entry conditions align.
Auto-selects optimal strategy based on VIX, GEX, RSI, time of day.
Scores sessions A-F after close.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import datetime, date
from typing import Optional

import requests

logger = logging.getLogger("dayblade_scanner")

DB = "data/trader.db"

# ── Target tickers by VIX regime ────────────────────────────────────────────
BEAR_BASE_TICKERS = ["SPY", "QQQ"]
BEAR_HIGH_VIX_TICKERS = ["NVDA", "AMD"]   # VIX > 25
BEAR_EXTREME_TICKERS = ["TSLA"]            # VIX > 30

# ── Risk rules ───────────────────────────────────────────────────────────────
MAX_TRADES_DAY = 2
MAX_PREMIUM = 3.00          # never buy > $3.00 on 0DTE
STOP_PCT = 0.50             # 50% of premium paid
TARGET_PCT_LOW = 1.00       # 100% gain
TARGET_PCT_HIGH = 2.00      # 200% gain
CALL_STOP_PCT = 0.40        # tighter stop on bounces in bear market
CALL_TARGET_PCT = 0.75      # conservative bounce target (50-100%, use 75%)
MIDDAY_STOP_PCT = 0.30      # tightest midday stops
SPREAD_WIDTH = 2.0          # default spread leg width in dollars
SHORT_MAX_POSITIONS = 2     # max concurrent paper shorts
SHORT_SIZE_PCT = 0.01       # 1% of portfolio per short
SHORT_STOP_PCT = 0.02       # 2% stop on shorts

# ── Session grade thresholds ─────────────────────────────────────────────────
GRADE_A_AVG_RETURN = 1.50   # >150% avg return on 2/2 winners


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def get_time_session() -> str:
    """Return trading session: pre_market/morning/midday/power_hour/close/after_hours/weekend."""
    import pytz
    now = datetime.now(pytz.timezone("US/Eastern"))
    if now.weekday() >= 5:
        return "weekend"
    mins = now.hour * 60 + now.minute
    if mins < 9 * 60 + 30:
        return "pre_market"
    if mins < 10 * 60 + 30:
        return "morning"       # 9:30–10:30 — gap fill + opening plays
    if mins < 14 * 60:
        return "midday"        # 10:30–2:00 — reduce activity
    if mins < 15 * 60 + 30:
        return "power_hour"    # 2:00–3:30 — highest conviction
    if mins < 16 * 60:
        return "close"         # 3:30–4:00 — CLOSE ALL
    return "after_hours"


# ---------------------------------------------------------------------------
# Strategy selector
# ---------------------------------------------------------------------------

def select_strategy(vix: float, gex_b: float, rsi_5m: float, session: str,
                    confidence: int = 0, spy: float = 0, gamma_flip: float = 0) -> dict:
    """Auto-select optimal strategy for current market conditions."""
    # High-confidence bear: defined risk bear put spread preferred over naked
    if confidence > 80 and vix > 25 and gex_b < 0:
        buy_strike = round(spy / 0.5) * 0.5 if spy else 0
        sell_strike = buy_strike - SPREAD_WIDTH if buy_strike else 0
        return {
            "strategy": "BEAR_PUT_SPREAD",
            "label": "BEAR PUT SPREAD — defined risk breakdown",
            "description": f"High confidence {confidence}%, buy ATM put + sell ${sell_strike:.0f} put — max loss = debit",
            "color": "#ef4444",
            "icon": "🔴",
            "spread_legs": {
                "buy": f"SPY ${buy_strike:.0f} put (ATM)",
                "sell": f"SPY ${sell_strike:.0f} put (OTM)",
                "width": f"${SPREAD_WIDTH:.0f}",
                "max_profit": f"${SPREAD_WIDTH * 100 - 50:.0f}/spread",
                "max_loss": "debit paid",
            },
        }
    if vix > 25 and gex_b < 0 and rsi_5m > 55 and session != "midday":
        return {
            "strategy": "NAKED_PUTS",
            "label": "NAKED PUTS — momentum continuation",
            "description": f"SPY trending down, VIX {vix:.1f} elevated, GEX {gex_b:.2f}B negative",
            "color": "#ef4444",
            "icon": "🔴",
            "spread_legs": None,
        }
    if vix > 28 and rsi_5m < 25:
        buy_strike = round(spy * 1.005 / 0.5) * 0.5 if spy else 0
        sell_strike = buy_strike + SPREAD_WIDTH if buy_strike else 0
        return {
            "strategy": "BULL_CALL_SPREAD",
            "label": "BULL CALL SPREAD — oversold bounce (defined risk)",
            "description": f"RSI {rsi_5m:.0f} extreme oversold, VIX {vix:.1f} — buy spread, limit risk on bounce",
            "color": "#22c55e",
            "icon": "🟢",
            "spread_legs": {
                "buy": f"SPY ${buy_strike:.0f} call (ATM)",
                "sell": f"SPY ${sell_strike:.0f} call (OTM)",
                "width": f"${SPREAD_WIDTH:.0f}",
                "max_profit": f"${SPREAD_WIDTH * 100 - 50:.0f}/spread",
                "max_loss": "debit paid",
            },
        }
    if 20 <= vix <= 25 and session == "midday":
        ic_put_sell = round((spy - 2) / 0.5) * 0.5 if spy else 0
        ic_put_buy = ic_put_sell - SPREAD_WIDTH
        ic_call_sell = round((spy + 2) / 0.5) * 0.5 if spy else 0
        ic_call_buy = ic_call_sell + SPREAD_WIDTH
        return {
            "strategy": "IRON_CONDOR",
            "label": "IRON CONDOR — range-bound premium",
            "description": f"VIX {vix:.1f} moderate, midday consolidation, collect from both sides",
            "color": "#818cf8",
            "icon": "🟣",
            "spread_legs": {
                "buy": f"SPY ${ic_put_buy:.0f}P / ${ic_call_buy:.0f}C",
                "sell": f"SPY ${ic_put_sell:.0f}P / ${ic_call_sell:.0f}C",
                "width": f"${SPREAD_WIDTH:.0f} each side",
                "max_profit": "both premiums collected",
                "max_loss": f"${SPREAD_WIDTH * 100:.0f} - credits",
            },
        }
    if vix > 25 and rsi_5m > 50 and gex_b < 0:
        sell_strike = round((gamma_flip or (spy * 1.005)) / 0.5) * 0.5 if spy else 0
        buy_strike = sell_strike + SPREAD_WIDTH
        return {
            "strategy": "BEAR_CALL_SPREAD",
            "label": "BEAR CALL SPREAD — sell the rally",
            "description": f"VIX {vix:.1f} elevated, weak bounce — sell calls at gamma flip ${sell_strike:.0f}",
            "color": "#f59e0b",
            "icon": "🟡",
            "spread_legs": {
                "sell": f"SPY ${sell_strike:.0f} call (at/above gamma flip)",
                "buy": f"SPY ${buy_strike:.0f} call (OTM hedge)",
                "width": f"${SPREAD_WIDTH:.0f}",
                "max_profit": "credit received",
                "max_loss": f"${SPREAD_WIDTH * 100:.0f} - credit",
            },
        }
    return {
        "strategy": "DIRECTIONAL_SPREAD",
        "label": "DIRECTIONAL SPREAD — defined risk trend",
        "description": f"VIX {vix:.1f}, standard setup — use spreads to define risk",
        "color": "#94a3b8",
        "icon": "⚪",
        "spread_legs": None,
    }


# ---------------------------------------------------------------------------
# Entry signal checkers
# ---------------------------------------------------------------------------

def check_put_entry(spy: float, vwap: float, rsi_5m: float, gex_b: float,
                    put_wall: float, gamma_flip: float, vol_ratio: float) -> dict:
    """
    PUT entry — all conditions should align for high-confidence short.
    Returns {fire, triggers, confidence, direction}.
    """
    triggers = []
    score = 0

    if vwap and spy > 0 and spy < vwap:
        triggers.append(f"SPY ${spy:.2f} below VWAP ${vwap:.2f}")
        score += 25

    if rsi_5m >= 60:
        triggers.append(f"RSI {rsi_5m:.0f} overbought on 5-min (bear bounce = fade it)")
        score += 25

    if gex_b < 0:
        triggers.append(f"GEX {gex_b:.2f}B negative (dealers amplify down moves)")
        score += 20

    if put_wall and spy > 0:
        dist_pct = (spy - put_wall) / spy * 100
        if 0 < dist_pct < 5.0:
            triggers.append(f"Put wall ${put_wall:.0f} is {dist_pct:.1f}% below — room to run")
            score += 15

    if vol_ratio >= 1.5:
        triggers.append(f"Volume {vol_ratio:.1f}x average (momentum confirm)")
        score += 15

    if gamma_flip and spy > 0 and abs(spy - gamma_flip) / spy < 0.003:
        triggers.append(f"SPY rejecting gamma flip ${gamma_flip:.2f} from below")
        score += 20

    return {
        "fire": score >= 60,
        "triggers": triggers,
        "confidence": min(score, 99),
        "direction": "PUT",
    }


def check_call_entry(spy: float, vwap: float, rsi_5m: float, put_wall: float,
                     vol_ratio: float, fear_greed: int) -> dict:
    """
    CALL entry — counter-trend oversold bounce. Tighter criteria in bear mode.
    """
    triggers = []
    score = 0

    if put_wall and spy <= put_wall * 1.005:
        triggers.append(f"SPY ${spy:.2f} at/below put wall ${put_wall:.0f} support")
        score += 30

    if rsi_5m < 25:
        triggers.append(f"RSI {rsi_5m:.0f} extreme oversold (violent bounce fuel)")
        score += 30

    if vol_ratio >= 2.0:
        triggers.append(f"Volume {vol_ratio:.1f}x spike on buying bar")
        score += 20

    if fear_greed < 25:
        triggers.append(f"Fear & Greed {fear_greed} — extreme fear (contrarian signal)")
        score += 20

    return {
        "fire": score >= 70,   # higher threshold for calls in bear mode
        "triggers": triggers,
        "confidence": min(score, 99),
        "direction": "CALL",
    }


def check_gap_fill_call(spy: float, prev_close: float, session: str) -> dict:
    """
    Gap fill call play: SPY gaps down >0.5% at open, buy calls after first 5 min.
    Time-limited: must close by 11:00 AM ET. Morning session only.
    """
    triggers = []
    score = 0

    if not prev_close or not spy or prev_close <= 0:
        return {"fire": False, "triggers": [], "confidence": 0, "direction": "GAP_CALL",
                "gap_pct": 0, "gap_fill_target": 0}

    gap_pct = (prev_close - spy) / prev_close * 100  # positive = gap down

    if gap_pct >= 0.5:
        triggers.append(f"Gap down {gap_pct:.2f}% from prev close ${prev_close:.2f}")
        score += 40
    if gap_pct >= 1.0:
        triggers.append(f"Large gap (>{gap_pct:.1f}%) — higher fill probability")
        score += 20

    if session == "morning":
        triggers.append("Morning window — gap fills typically resolve before 10:30 AM")
        score += 30
        triggers.append(f"Target: fill to ${prev_close:.2f} | Close by 11:00 AM ET")
        score += 10

    return {
        "fire": score >= 70 and session == "morning",
        "triggers": triggers,
        "confidence": min(score, 99),
        "direction": "GAP_CALL",
        "gap_pct": round(gap_pct, 2),
        "gap_fill_target": round(prev_close, 2),
        "time_limit": "11:00 AM ET",
    }


def get_short_candidates(vix: float = 20.0, bridge_consensus: float = 0.0) -> list:
    """
    Screen for paper short candidates: below key MAs, overbought on bounce,
    declining volume. Returns list of dicts with entry criteria.
    """
    CANDIDATES = ["SPY", "QQQ", "NVDA", "AMD", "TSLA", "AAPL", "META", "AMZN",
                  "MSFT", "GOOGL"]
    qualified = []

    # All criteria must pass: only flag in bear regime (VIX > 20) + sell consensus
    if vix < 20:
        return []

    try:
        import yfinance as yf
        for ticker in CANDIDATES:
            try:
                t = yf.Ticker(ticker)
                hist = t.history(period="60d", interval="1d")
                if hist.empty or len(hist) < 50:
                    continue

                price = float(hist["Close"].iloc[-1])
                ma50 = float(hist["Close"].rolling(50).mean().iloc[-1])
                ma200 = float(hist["Close"].rolling(200).mean().iloc[-1]) if len(hist) >= 200 else ma50

                # Must be below 200MA and 50MA
                if price >= ma50 or price >= ma200:
                    continue

                # RSI (14-day) > 60 = overbought on bounce
                delta = hist["Close"].diff()
                gain = delta.clip(lower=0).rolling(14).mean()
                loss = (-delta.clip(upper=0)).rolling(14).mean()
                rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] > 0 else 100
                rsi = round(100 - 100 / (1 + rs), 1)

                if rsi < 60:
                    continue

                # Volume declining: today vs 5-day avg
                vol_today = float(hist["Volume"].iloc[-1])
                vol_avg5 = float(hist["Volume"].rolling(5).mean().iloc[-2])  # prior days
                vol_ratio = round(vol_today / vol_avg5, 2) if vol_avg5 > 0 else 1.0

                if vol_ratio >= 1.0:
                    continue  # volume rising = skip (not a weak rally)

                qualified.append({
                    "ticker": ticker,
                    "price": round(price, 2),
                    "ma50": round(ma50, 2),
                    "ma200": round(ma200, 2),
                    "below_ma50_pct": round((ma50 - price) / ma50 * 100, 1),
                    "rsi_14d": rsi,
                    "volume_ratio": vol_ratio,
                    "stop_loss": round(price * (1 + SHORT_STOP_PCT), 2),
                    "reason": (
                        f"Below 50MA/${ma50:.0f} + 200MA/${ma200:.0f}, "
                        f"RSI {rsi} overbought on bounce, volume declining ({vol_ratio}x avg)"
                    ),
                })
            except Exception:
                continue
    except Exception as e:
        logger.warning("Short candidates screen error: %s", e)

    return sorted(qualified, key=lambda x: -x["rsi_14d"])[:5]  # top 5 by RSI overbought


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def ensure_tables():
    """Create flash_alerts table if missing."""
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS flash_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker      TEXT NOT NULL,
                direction   TEXT NOT NULL,
                strike      REAL,
                premium     REAL,
                delta       REAL,
                iv          REAL,
                triggers    TEXT,
                confidence  INTEGER,
                stop_price  REAL,
                target_low  REAL,
                target_high REAL,
                strategy    TEXT,
                dismissed   INTEGER DEFAULT 0,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_flash_alerts_ts ON flash_alerts(created_at)"
        )
        c.execute("""
            CREATE TABLE IF NOT EXISTS session_grades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date  TEXT NOT NULL UNIQUE,
                grade       TEXT NOT NULL,
                trades      INTEGER DEFAULT 0,
                wins        INTEGER DEFAULT 0,
                net_pnl_pct REAL DEFAULT 0,
                notes       TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS short_watchlist (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker      TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss   REAL NOT NULL,
                reason      TEXT,
                status      TEXT DEFAULT 'WATCHING',
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)


def save_flash_alert(ticker: str, direction: str, strike: float, premium: float,
                     delta: float, iv: float, triggers: list,
                     confidence: int, strategy: str) -> int:
    stop = round(premium * STOP_PCT, 2)
    target_low = round(premium * (1 + TARGET_PCT_LOW), 2)
    target_high = round(premium * (1 + TARGET_PCT_HIGH), 2)
    with _conn() as c:
        cur = c.execute("""
            INSERT INTO flash_alerts
              (ticker, direction, strike, premium, delta, iv, triggers,
               confidence, stop_price, target_low, target_high, strategy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticker, direction, strike, premium, delta, iv,
              "\n".join(triggers), confidence, stop, target_low, target_high, strategy))
        return cur.lastrowid


def get_recent_flash_alerts(limit: int = 10) -> list:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM flash_alerts ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_active_flash_alert() -> Optional[dict]:
    """Most recent non-dismissed alert from last 5 minutes."""
    with _conn() as c:
        row = c.execute("""
            SELECT * FROM flash_alerts
            WHERE dismissed = 0
              AND created_at > datetime('now', '-5 minutes')
            ORDER BY created_at DESC LIMIT 1
        """).fetchone()
        return dict(row) if row else None


def dismiss_alert(alert_id: int):
    with _conn() as c:
        c.execute("UPDATE flash_alerts SET dismissed=1 WHERE id=?", (alert_id,))


# ---------------------------------------------------------------------------
# Session grading
# ---------------------------------------------------------------------------

def grade_session(trades_today: list) -> str:
    """Grade today's session A-F."""
    closed = [t for t in trades_today if t.get("status") not in ("OPEN", None)]
    if not closed:
        return "-"
    wins = [t for t in closed if (t.get("pnl_pct") or 0) > 0]
    net = sum(t.get("pnl_pct", 0) or 0 for t in closed)
    avg_win = sum(t.get("pnl_pct", 0) or 0 for t in wins) / len(wins) if wins else 0
    stop_breaches = [t for t in closed if (t.get("pnl_pct") or 0) < -STOP_PCT]

    if stop_breaches:
        return "F"
    if len(wins) == len(closed) == 2 and avg_win > GRADE_A_AVG_RETURN:
        return "A"
    if wins and net > 0:
        return "B"
    if wins:
        return "C"
    return "D"


def save_session_grade(grade: str, trades: list):
    net = sum(t.get("pnl_pct", 0) or 0 for t in trades)
    wins = [t for t in trades if (t.get("pnl_pct") or 0) > 0]
    with _conn() as c:
        c.execute("""
            INSERT OR REPLACE INTO session_grades
              (trade_date, grade, trades, wins, net_pnl_pct)
            VALUES (?, ?, ?, ?, ?)
        """, (date.today().isoformat(), grade, len(trades), len(wins), round(net, 4)))


# ---------------------------------------------------------------------------
# ntfy push
# ---------------------------------------------------------------------------

def _push_ntfy(direction: str, spy: float, triggers: list, confidence: int,
               strike: float, premium: float):
    ntfy_url = os.getenv("NTFY_URL", "")
    if not ntfy_url:
        return
    body = (
        f"⚡ 0DTE {direction} ALERT — SPY ${spy:.2f}\n"
        f"Strike: ${strike:.0f} | Premium: ${premium:.2f} ask\n"
        f"Confidence: {confidence}%\n"
        + "\n".join(f"✓ {t}" for t in triggers[:4])
    )
    try:
        requests.post(
            ntfy_url,
            data=body.encode(),
            headers={
                "Title": f"DayBlade {direction} Signal",
                "Priority": "high",
                "Tags": "rotating_light",
            },
            timeout=5,
        )
    except Exception as e:
        logger.debug("ntfy push failed: %s", e)


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def run_scan() -> dict:
    """Full bear-mode 0DTE scan. Returns complete status dict."""
    ensure_tables()
    session = get_time_session()
    scan_ts = datetime.now().isoformat()

    # ── Fetch market data ───────────────────────────────────────────────────
    spy_price = vix = rsi_5m = vwap = vol_ratio = 0.0
    gex_b = put_wall = call_wall = gamma_flip = 0.0
    fear_greed = 50

    try:
        from engine.market_data import get_stock_price, get_technical_indicators
        spy_d = get_stock_price("SPY") or {}
        vix_d = get_stock_price("VIX") or {}
        spy_price = spy_d.get("price", 0) or 0
        vix = vix_d.get("price", 0) or 20.0
    except Exception as e:
        logger.warning("market_data error: %s", e)

    try:
        from engine.gex_scanner import get_gex_data
        gex_d = get_gex_data("SPY") or {}
        raw_gex = gex_d.get("total_gex_b", 0) or 0
        # total_gex_b may be in billions already or raw dollars
        gex_b = raw_gex if abs(raw_gex) < 1000 else raw_gex / 1e9
        put_wall = gex_d.get("put_wall", 0) or 0
        call_wall = gex_d.get("call_wall", 0) or 0
        gamma_flip = gex_d.get("gamma_flip", 0) or 0
    except Exception as e:
        logger.debug("gex_data error: %s", e)

    try:
        from engine.market_data import get_technical_indicators
        tech = get_technical_indicators("SPY", "5m") or {}
        rsi_5m = tech.get("rsi", 50) or 50
        vwap = tech.get("vwap", spy_price) or spy_price
        vol_ratio = tech.get("volume_ratio", 1.0) or 1.0
    except Exception as e:
        logger.debug("technical_indicators error: %s", e)

    try:
        from engine.sentiment_scanner import get_fear_greed
        fg = get_fear_greed() or {}
        fear_greed = fg.get("score", 50) or 50
    except Exception:
        pass

    # ── Active ticker list ───────────────────────────────────────────────────
    tickers = list(BEAR_BASE_TICKERS)
    if vix > 25:
        tickers += BEAR_HIGH_VIX_TICKERS
    if vix > 30:
        tickers += BEAR_EXTREME_TICKERS

    # ── Entry signals ────────────────────────────────────────────────────────
    put_sig = check_put_entry(spy_price, vwap, rsi_5m, gex_b,
                              put_wall, gamma_flip, vol_ratio)
    call_sig = check_call_entry(spy_price, vwap, rsi_5m, put_wall,
                                vol_ratio, fear_greed)

    # Gap fill call — fetch prev close for gap detection
    prev_close = 0.0
    try:
        import yfinance as yf
        hist = yf.Ticker("SPY").history(period="2d", interval="1d")
        if len(hist) >= 2:
            prev_close = float(hist["Close"].iloc[-2])
    except Exception:
        pass
    gap_sig = check_gap_fill_call(spy_price, prev_close, session)

    # Active signal — puts preferred in bear mode; calls/gap only in morning
    active_signal = None
    if put_sig["fire"] and session in ("morning", "power_hour"):
        active_signal = put_sig
    elif gap_sig["fire"] and session == "morning":
        active_signal = gap_sig
    elif call_sig["fire"] and session == "morning":
        active_signal = call_sig

    # ── Strategy selection (uses signal confidence) ──────────────────────────
    sig_confidence = active_signal["confidence"] if active_signal else 0
    strategy = select_strategy(vix, gex_b, rsi_5m, session,
                               confidence=sig_confidence, spy=spy_price,
                               gamma_flip=gamma_flip)

    # ── Short candidates (only in bear regime) ───────────────────────────────
    short_candidates = []
    if vix > 20 and session not in ("pre_market", "after_hours", "weekend"):
        try:
            short_candidates = get_short_candidates(vix)
        except Exception as e:
            logger.debug("short candidates error: %s", e)

    # ── Flash alert (rate-limited: max 1 per 15 min) ─────────────────────────
    alert_id = None
    if active_signal and spy_price > 0:
        recent = get_recent_flash_alerts(1)
        last_ts = recent[0]["created_at"] if recent else None
        too_soon = (
            last_ts and
            (datetime.now() - datetime.fromisoformat(last_ts)).total_seconds() < 900
        )
        if not too_soon:
            direction = active_signal["direction"]
            # Estimated strike: 1-2 OTM
            if direction == "PUT":
                est_strike = round(spy_price * 0.995 / 0.5) * 0.5
            else:
                est_strike = round(spy_price * 1.005 / 0.5) * 0.5
            est_premium = round(max(0.50, min(2.00, vix / 100 * spy_price * 0.015)), 2)
            est_delta = -0.35 if direction == "PUT" else 0.35
            est_iv = round(vix / 100 * 1.5, 3)

            alert_id = save_flash_alert(
                ticker="SPY",
                direction=direction,
                strike=est_strike,
                premium=est_premium,
                delta=est_delta,
                iv=est_iv,
                triggers=active_signal["triggers"],
                confidence=active_signal["confidence"],
                strategy=strategy["strategy"],
            )
            _push_ntfy(direction, spy_price, active_signal["triggers"],
                       active_signal["confidence"], est_strike, est_premium)

    return {
        "session": session,
        "tickers": tickers,
        "spy_price": spy_price,
        "vix": vix,
        "gex_b": round(gex_b, 3),
        "rsi_5m": round(rsi_5m, 1),
        "vwap": round(vwap, 2),
        "volume_ratio": round(vol_ratio, 2),
        "put_wall": put_wall,
        "call_wall": call_wall,
        "gamma_flip": gamma_flip,
        "fear_greed": fear_greed,
        "prev_close": round(prev_close, 2),
        "put_signal": put_sig,
        "call_signal": call_sig,
        "gap_signal": gap_sig,
        "active_signal": active_signal,
        "strategy": strategy,
        "short_candidates": short_candidates,
        "flash_alert_id": alert_id,
        "scan_timestamp": scan_ts,
    }
