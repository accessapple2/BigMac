"""0DTE Battle Station — real-time tactical intelligence for DayBlade/Sulu.

Three components:
  A) Morning Briefing (6:25 AM MST): prior day levels + GEX key levels → morning_levels table
  B) Opening Range (6:45 AM MST): OR high/low from first 15 min of trading
  C) Position Monitor (every 60s): Greeks, P&L, signals for active options positions

Signal types:
  HOLD      — position on track, no action
  TIGHTEN   — partial profit or warning, post to War Room
  CLOSE_NOW — cut loss / protect gains, auto-close via alpaca_options.py + War Room RED ALERT

Early-exit if no options positions (lightweight 60s loop).
EOD auto-close at 12:45 PM MST is handled separately in main.py — NOT duplicated here.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import sqlite3
import time
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from scipy.stats import norm

TRADER_DB = "data/trader.db"
ALPACA_DATA_BASE = "https://data.alpaca.markets"
RISK_FREE_RATE = 0.05
EOD_MST_HOUR = 13      # 1:00 PM MST = 3:00 PM ET (EOD close cutoff for 0DTE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [battle_station] %(levelname)s: %(message)s",
)
logger = logging.getLogger("battle_station")

# Spam-throttle: last signal per option_symbol, last TIGHTEN post timestamp
_last_signal: dict[str, str] = {}
_last_tighten_ts: dict[str, float] = {}
TIGHTEN_POST_COOLDOWN = 900  # 15 minutes between TIGHTEN posts per position


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_tables():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS morning_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date DATE NOT NULL,
                symbol TEXT NOT NULL,
                prior_high REAL,
                prior_low REAL,
                prior_close REAL,
                prior_vwap REAL,
                gex_king REAL,
                gex_flip REAL,
                gex_put_wall REAL,
                gex_call_wall REAL,
                or_high REAL,
                or_low REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_ml_date_sym ON morning_levels(trade_date, symbol)"
        )
        c.execute("""
            CREATE TABLE IF NOT EXISTS battle_station_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                symbol TEXT,
                option_symbol TEXT,
                underlying_price REAL,
                option_price REAL,
                delta REAL,
                gamma REAL,
                theta REAL,
                pnl_pct REAL,
                signal TEXT,
                reason TEXT
            )
        """)
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_bsl_ts ON battle_station_log(timestamp)"
        )
        c.commit()


# ---------------------------------------------------------------------------
# Black-Scholes Greeks
# ---------------------------------------------------------------------------

def _bs_greeks(
    S: float, K: float, T: float, sigma: float,
    r: float = RISK_FREE_RATE, is_call: bool = True
) -> dict:
    """Black-Scholes Greeks: delta, gamma, theta (per day), vega (per 1% IV)."""
    if S <= 0 or K <= 0 or sigma <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    T = max(T, 1 / (252 * 24))  # minimum 1 hour

    try:
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T

        gamma = norm.pdf(d1) / (S * sigma * sqrt_T)
        vega = S * norm.pdf(d1) * sqrt_T / 100  # per 1% IV change

        if is_call:
            delta = norm.cdf(d1)
            theta_raw = (
                -(S * norm.pdf(d1) * sigma) / (2 * sqrt_T)
                - r * K * math.exp(-r * T) * norm.cdf(d2)
            )
        else:
            delta = norm.cdf(d1) - 1  # negative for puts
            theta_raw = (
                -(S * norm.pdf(d1) * sigma) / (2 * sqrt_T)
                + r * K * math.exp(-r * T) * norm.cdf(-d2)
            )

        theta = theta_raw / 252  # per calendar day

        return {
            "delta": round(delta, 4),
            "gamma": round(gamma, 6),
            "theta": round(theta, 4),
            "vega": round(vega, 4),
        }
    except Exception:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}


# ---------------------------------------------------------------------------
# OCC option symbol parser
# ---------------------------------------------------------------------------

def _parse_occ_symbol(occ: str) -> dict:
    """Parse OCC option symbol: AAPL260117C00220000
    Returns {underlying, expiry_date, option_type, strike}.
    """
    m = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", occ.upper().strip())
    if not m:
        return {}

    underlying = m.group(1)
    date_str = m.group(2)  # YYMMDD
    option_type = "call" if m.group(3) == "C" else "put"
    strike = int(m.group(4)) / 1000

    try:
        expiry = datetime.strptime(date_str, "%y%m%d").date()
    except Exception:
        expiry = None

    return {
        "underlying": underlying,
        "expiry_date": expiry,
        "option_type": option_type,
        "strike": strike,
    }


def _dte_in_years(expiry_date) -> float:
    """Time to expiry in years. For 0DTE uses hours remaining until EOD."""
    if expiry_date is None:
        return 1 / 252

    if isinstance(expiry_date, str):
        try:
            expiry_date = date.fromisoformat(expiry_date)
        except Exception:
            return 1 / 252

    today = date.today()
    days_left = (expiry_date - today).days

    if days_left <= 0:
        # 0DTE: use time remaining until 3 PM ET / 1 PM MST
        now = datetime.now()
        close = now.replace(hour=EOD_MST_HOUR, minute=0, second=0, microsecond=0)
        hours_left = max(0.25, (close - now).total_seconds() / 3600)
        return max(hours_left / (252 * 24), 1 / (252 * 24))

    return max(days_left / 252, 1 / 252)


def _minutes_until_eod_close() -> float:
    """Minutes until 1:00 PM MST (3:00 PM ET) EOD close."""
    import pytz
    now_et = datetime.now(pytz.timezone("US/Eastern"))
    close_et = now_et.replace(hour=15, minute=0, second=0, microsecond=0)
    mins = (close_et - now_et).total_seconds() / 60
    return max(0, mins)


# ---------------------------------------------------------------------------
# Alpaca helpers
# ---------------------------------------------------------------------------

def _alpaca_headers() -> dict:
    from dotenv import load_dotenv
    load_dotenv()
    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }


def _get_prior_day_bars(symbol: str) -> dict:
    """Fetch prior completed trading day OHLCV from Alpaca."""
    headers = _alpaca_headers()
    try:
        resp = requests.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars",
            headers=headers,
            params={"timeframe": "1Day", "limit": 3, "feed": "iex"},
            timeout=15,
        )
        resp.raise_for_status()
        bars = resp.json().get("bars", [])
        if not bars:
            return {}
        # bars[-2] = yesterday's completed bar (bars[-1] may be today's partial)
        bar = bars[-2] if len(bars) >= 2 else bars[0]
        return {
            "high": bar.get("h"),
            "low": bar.get("l"),
            "close": bar.get("c"),
            "vwap": bar.get("vw"),
            "open": bar.get("o"),
        }
    except Exception as e:
        logger.warning(f"Prior day bars failed for {symbol}: {e}")
        return {}


def _get_opening_range_bars(symbol: str) -> dict:
    """Fetch 5-min bars for today's first 15 minutes (9:30–9:45 AM ET)."""
    headers = _alpaca_headers()
    today = date.today().isoformat()
    # 9:30 AM ET = 13:30 UTC, 9:46 AM ET = 13:46 UTC
    start = f"{today}T13:30:00Z"
    end = f"{today}T13:46:00Z"
    try:
        resp = requests.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/{symbol}/bars",
            headers=headers,
            params={"timeframe": "5Min", "start": start, "end": end, "feed": "iex"},
            timeout=15,
        )
        resp.raise_for_status()
        bars = resp.json().get("bars", [])
        if not bars:
            return {}
        or_high = max(b.get("h", 0) for b in bars)
        or_low = min(b.get("l", float("inf")) for b in bars)
        if or_low == float("inf") or or_high == 0:
            return {}
        return {"or_high": or_high, "or_low": or_low}
    except Exception as e:
        logger.warning(f"Opening range bars failed for {symbol}: {e}")
        return {}


def _get_alpaca_options_positions() -> list[dict]:
    """Get open options positions from Alpaca paper account."""
    try:
        from engine.alpaca_options import _get_client
        client = _get_client()
        if not client:
            return []

        positions = client.get_all_positions()
        result = []

        for pos in positions:
            sym = pos.symbol
            # Options symbols are long (e.g. SPY260331C00560000)
            if len(sym) < 10:
                continue

            parsed = _parse_occ_symbol(sym)
            if not parsed:
                continue

            try:
                entry_price = float(pos.avg_entry_price or 0)
                current_price = float(pos.current_price or 0)
                unrealized_plpc = float(pos.unrealized_plpc or 0)

                result.append({
                    "option_symbol": sym,
                    "underlying": parsed["underlying"],
                    "strike": parsed["strike"],
                    "option_type": parsed["option_type"],
                    "expiry_date": parsed["expiry_date"],
                    "qty": abs(float(pos.qty)),
                    "current_price": current_price,
                    "avg_entry_price": entry_price,
                    "unrealized_pl": float(pos.unrealized_pl or 0),
                    "unrealized_plpc": unrealized_plpc,  # decimal: -0.52 = -52%
                })
            except Exception:
                continue

        return result
    except Exception as e:
        logger.warning(f"Alpaca options positions fetch failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Part A: Morning Briefing
# ---------------------------------------------------------------------------

def generate_morning_briefing(symbols: list[str] | None = None) -> dict:
    """Pre-market morning levels. Run once at 6:25 AM MST.

    Fetches prior day bars from Alpaca and GEX levels.
    Stores to morning_levels table (or_high/or_low filled later at 6:45 AM).
    """
    _init_tables()
    if symbols is None:
        symbols = ["SPY", "QQQ"]

    today = date.today().isoformat()
    results = {}

    for sym in symbols:
        bars = _get_prior_day_bars(sym)

        # GEX key levels
        gex: dict = {}
        try:
            from engine.gex_overlay import get_latest_gex, calculate_gex, _save_gex_levels
            gex = get_latest_gex(sym) or {}
            if not gex:
                fresh = calculate_gex(sym)
                if fresh:
                    _save_gex_levels(sym, fresh)
                    gex = get_latest_gex(sym) or {}
        except Exception as e:
            logger.warning(f"GEX fetch failed for {sym}: {e}")

        try:
            with _conn() as c:
                c.execute(
                    """INSERT INTO morning_levels
                       (trade_date, symbol, prior_high, prior_low, prior_close, prior_vwap,
                        gex_king, gex_flip, gex_put_wall, gex_call_wall, or_high, or_low)
                       VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL)
                       ON CONFLICT(trade_date, symbol) DO UPDATE SET
                           prior_high=excluded.prior_high,
                           prior_low=excluded.prior_low,
                           prior_close=excluded.prior_close,
                           prior_vwap=excluded.prior_vwap,
                           gex_king=excluded.gex_king,
                           gex_flip=excluded.gex_flip,
                           gex_put_wall=excluded.gex_put_wall,
                           gex_call_wall=excluded.gex_call_wall""",
                    (
                        today, sym,
                        bars.get("high"), bars.get("low"), bars.get("close"), bars.get("vwap"),
                        gex.get("king_node"), gex.get("gamma_flip"),
                        gex.get("put_wall"), gex.get("call_wall"),
                    ),
                )
                c.commit()
        except Exception as e:
            logger.warning(f"morning_levels insert failed for {sym}: {e}")

        results[sym] = {
            "prior_high": bars.get("high"), "prior_low": bars.get("low"),
            "prior_close": bars.get("close"), "prior_vwap": bars.get("vwap"),
            "gex_king": gex.get("king_node"), "gex_flip": gex.get("gamma_flip"),
        }
        logger.info(
            f"Morning briefing {sym}: H={bars.get('high')} L={bars.get('low')} "
            f"C={bars.get('close')} GEX King={gex.get('king_node')}"
        )

    return results


def update_opening_range(symbols: list[str] | None = None) -> None:
    """Set OR high/low after first 15 min of trading. Run at 6:45 AM MST.

    Also posts War Room briefing once OR is established.
    """
    _init_tables()
    if symbols is None:
        symbols = ["SPY", "QQQ"]

    today = date.today().isoformat()

    for sym in symbols:
        or_data = _get_opening_range_bars(sym)
        if not or_data:
            logger.warning(f"No opening range bars for {sym}")
            continue

        try:
            with _conn() as c:
                c.execute(
                    "UPDATE morning_levels SET or_high=?, or_low=? WHERE trade_date=? AND symbol=?",
                    (or_data["or_high"], or_data["or_low"], today, sym),
                )
                c.commit()
            logger.info(
                f"OR set for {sym}: H={or_data['or_high']:.2f} L={or_data['or_low']:.2f}"
            )
        except Exception as e:
            logger.warning(f"OR update failed for {sym}: {e}")

    # Post War Room briefing using SPY levels (once after OR is filled)
    try:
        spy = _get_morning_levels_db("SPY")
        qqq = _get_morning_levels_db("QQQ")

        if spy and spy.get("or_high"):
            def _fmt(v, d=0):
                return f"${v:.{d}f}" if v is not None else "?"

            gex_line = ""
            if spy.get("gex_king"):
                regime = "POSITIVE γ" if (spy.get("gex_king") or 0) > (spy.get("gex_flip") or 0) else "NEGATIVE γ"
                gex_line = (
                    f"\nGEX: King {_fmt(spy['gex_king'],0)} | "
                    f"Flip {_fmt(spy.get('gex_flip'),0)} | "
                    f"Put Wall {_fmt(spy.get('gex_put_wall'),0)} | "
                    f"Call Wall {_fmt(spy.get('gex_call_wall'),0)} | Regime: {regime}"
                )

            msg = (
                f"⚡ BATTLE STATION ONLINE — Morning Levels Set:\n"
                f"SPY: Prior High {_fmt(spy.get('prior_high'))} | "
                f"Prior Low {_fmt(spy.get('prior_low'))} | "
                f"VWAP {_fmt(spy.get('prior_vwap'))} | "
                f"OR High {_fmt(spy.get('or_high'))} | "
                f"OR Low {_fmt(spy.get('or_low'))}"
                f"{gex_line}\n"
                f"DayBlade and Sulu are cleared for operations."
            )

            from engine.war_room import save_hot_take
            save_hot_take("dayblade-0dte", "SPY", msg)
    except Exception as e:
        logger.warning(f"War Room OR post failed: {e}")


def _get_morning_levels_db(symbol: str) -> dict | None:
    """Get today's morning_levels row from DB."""
    _init_tables()
    today = date.today().isoformat()
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT * FROM morning_levels WHERE trade_date=? AND symbol=? ORDER BY id DESC LIMIT 1",
                (today, symbol),
            ).fetchone()
        return dict(row) if row else None
    except Exception:
        return None


def get_morning_levels_for_prompt(symbol: str) -> str:
    """Formatted morning levels text for DayBlade's decision prompt."""
    row = _get_morning_levels_db(symbol)
    if not row:
        return ""

    def _fmt(v, d=2):
        return f"${v:.{d}f}" if v is not None else "?"

    lines = [f"MORNING LEVELS ({symbol}):"]

    # Resistance: highest to lowest above current price
    resistance = []
    if row.get("gex_call_wall"):
        resistance.append(f"Call Wall {_fmt(row['gex_call_wall'], 0)}")
    if row.get("prior_high"):
        resistance.append(f"Prior High {_fmt(row['prior_high'])}")
    if row.get("or_high"):
        resistance.append(f"OR High {_fmt(row['or_high'])}")
    if row.get("gex_king"):
        resistance.append(f"GEX King {_fmt(row['gex_king'], 0)}")

    # Support: highest to lowest below current price
    support = []
    if row.get("or_low"):
        support.append(f"OR Low {_fmt(row['or_low'])}")
    if row.get("gex_flip"):
        support.append(f"Gamma Flip {_fmt(row['gex_flip'], 0)}")
    if row.get("prior_low"):
        support.append(f"Prior Low {_fmt(row['prior_low'])}")
    if row.get("gex_put_wall"):
        support.append(f"Put Wall {_fmt(row['gex_put_wall'], 0)}")

    if resistance:
        lines.append(f"Key resistance: {' > '.join(resistance)}")
    if support:
        lines.append(f"Key support: {' > '.join(support)}")

    # Regime hint from spot vs gamma flip
    try:
        from engine.market_data import get_stock_price
        spot_data = get_stock_price(symbol)
        spot = spot_data.get("price", 0) if spot_data else 0
        flip = row.get("gex_flip")
        king = row.get("gex_king")
        if spot and flip and king:
            regime = "POSITIVE — expect price to gravitate toward King" if spot > flip \
                     else "NEGATIVE — expect trending moves, use momentum"
            lines.append(f"Gamma regime: {regime} {_fmt(king, 0)}")
    except Exception:
        pass

    lines.append("RULE: Do NOT buy calls above Call Wall. Do NOT buy puts below Put Wall.")
    or_h, or_l = row.get("or_high"), row.get("or_low")
    if or_h and or_l:
        lines.append(
            f"Best zone for entries: between OR Low {_fmt(or_l)} and OR High {_fmt(or_h)} on pullbacks."
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Part B: Position Greek Monitor
# ---------------------------------------------------------------------------

def _get_current_spot(symbol: str) -> float:
    """Get live spot price for an underlying."""
    try:
        from engine.market_data import get_stock_price
        d = get_stock_price(symbol)
        return float(d.get("price", 0)) if d else 0.0
    except Exception:
        return 0.0


def _estimate_iv(symbol: str, strike: float, option_type: str) -> float:
    """Estimate IV from Yahoo options chain; fall back to 0.30."""
    try:
        from engine.gex_overlay import _fetch_yahoo_chain
        chain = _fetch_yahoo_chain(symbol)
        if chain:
            key = "calls" if option_type == "call" else "puts"
            for opt in chain.get(key, []):
                k = opt.get("strike")
                iv = opt.get("impliedVolatility")
                if k and iv and abs(float(k) - strike) < 1.0:
                    iv_val = float(iv)
                    if 0.01 < iv_val < 5.0:  # sanity check
                        return iv_val
    except Exception:
        pass
    return 0.30  # default: 30% IV


def _generate_signal(
    pos: dict,
    greeks: dict,
    gex: dict | None,
    minutes_left: float,
) -> tuple[str, str]:
    """Return (signal, reason): HOLD / TIGHTEN / CLOSE_NOW."""
    pnl_pct_decimal = pos.get("unrealized_plpc", 0)  # Alpaca: decimal, e.g. -0.52 = -52%
    option_type = pos.get("option_type", "call")
    spot = pos.get("spot_price", 0)
    expiry_date = pos.get("expiry_date")
    is_0dte = expiry_date == date.today() if expiry_date else False

    # Convert to percentage for readability
    pnl_pct = pnl_pct_decimal * 100

    # ── CLOSE_NOW ──────────────────────────────────────────────────

    # 1. Max loss: -50% of premium paid
    if pnl_pct <= -50:
        return "CLOSE_NOW", f"Down {pnl_pct:.0f}% — max loss rule (50% premium). Cut now."

    # 2. 0DTE with <90 min left and losing
    if is_0dte and minutes_left < 90 and pnl_pct < 0:
        return "CLOSE_NOW", (
            f"Less than 90 min to EOD close, position losing ({pnl_pct:.0f}%). Exit now."
        )

    # 3. 0DTE <30 min left — theta is destroying value
    if is_0dte and minutes_left < 30 and pnl_pct < 0:
        return "CLOSE_NOW", f"<30 min left on 0DTE, theta destroying value ({pnl_pct:.0f}%)"

    # 4. Wrong side of gamma flip
    if gex and spot:
        gamma_flip = gex.get("gamma_flip")
        if gamma_flip and spot > 0:
            if option_type == "call" and spot < gamma_flip:
                return "CLOSE_NOW", (
                    f"Price ${spot:.2f} broke BELOW gamma flip ${gamma_flip:.0f}. "
                    f"Long call now in negative gamma — dealers amplifying downside. Exit."
                )
            if option_type == "put" and spot > gamma_flip:
                return "CLOSE_NOW", (
                    f"Price ${spot:.2f} broke ABOVE gamma flip ${gamma_flip:.0f}. "
                    f"Long put now in positive gamma — momentum fading. Exit."
                )

    # ── TIGHTEN ────────────────────────────────────────────────────

    # 5. Big winner: +100% gain — take partials
    if pnl_pct >= 100:
        return "TIGHTEN", f"Up {pnl_pct:.0f}% — take partial profits. Let remainder run."

    # 6. Price approaching gamma wall (within 0.3%)
    if gex and spot:
        if option_type == "call":
            cw = gex.get("call_wall")
            if cw and abs(spot - cw) / spot < 0.003:
                return "TIGHTEN", (
                    f"Price ${spot:.2f} approaching call wall ${cw:.0f} (within 0.3%). "
                    f"Up {pnl_pct:.0f}% — consider partial exit."
                )
        else:
            pw = gex.get("put_wall")
            if pw and abs(spot - pw) / spot < 0.003:
                return "TIGHTEN", (
                    f"Price ${spot:.2f} approaching put wall ${pw:.0f} (within 0.3%). "
                    f"Up {pnl_pct:.0f}% — consider partial exit."
                )

    # ── HOLD ───────────────────────────────────────────────────────
    return "HOLD", "Position on track — no action required."


def _auto_close(pos: dict, reason: str):
    """Close a position and post RED ALERT to War Room."""
    option_sym = pos.get("option_symbol", "")
    underlying = pos.get("underlying", "")
    current_price = pos.get("current_price", 0)
    entry_price = pos.get("avg_entry_price", 0)
    pnl_pct = pos.get("unrealized_plpc", 0) * 100

    try:
        from engine.alpaca_options import close_options_position
        qty = int(max(1, pos.get("qty", 1)))
        close_options_position("dayblade-0dte", option_sym, qty)
        logger.warning(f"Battle Station auto-close: {option_sym} ({reason})")
    except Exception as e:
        logger.warning(f"Auto-close failed for {option_sym}: {e}")

    try:
        from engine.war_room import save_hot_take
        msg = (
            f"🔴 BATTLE STATION: Closing {underlying} {option_sym[-10:]} — {reason} "
            f"Closed at ${current_price:.2f} (entry ${entry_price:.2f}, {pnl_pct:+.0f}%)."
        )
        save_hot_take("dayblade-0dte", underlying, msg)
    except Exception as e:
        logger.warning(f"War Room red alert post failed: {e}")


def monitor_active_options() -> None:
    """60-second position monitor. Early-exits if no options positions.

    Called every 60 seconds during market hours from main.py scheduler.
    """
    try:
        from engine.risk_manager import RiskManager
        mh = RiskManager.is_market_hours()
        if mh != "market":
            return
    except Exception:
        pass

    _init_tables()

    # LIGHTWEIGHT: exit immediately if no options positions
    positions = _get_alpaca_options_positions()
    if not positions:
        return

    logger.info(f"Battle Station: monitoring {len(positions)} active option(s)")

    minutes_left = _minutes_until_eod_close()

    # Cache GEX per underlying (avoid fetching same symbol repeatedly)
    gex_cache: dict[str, dict] = {}
    now_ts = datetime.now().isoformat()

    for pos in positions:
        underlying = pos.get("underlying", "")
        option_sym = pos.get("option_symbol", "")
        option_type = pos.get("option_type", "call")
        strike = pos.get("strike", 0)
        expiry_date = pos.get("expiry_date")

        # Spot price
        spot = _get_current_spot(underlying)
        pos["spot_price"] = spot

        # Greeks via Black-Scholes
        T = _dte_in_years(expiry_date)
        iv = _estimate_iv(underlying, strike, option_type)
        greeks = _bs_greeks(spot, strike, T, iv, is_call=(option_type == "call"))

        # GEX for underlying
        if underlying not in gex_cache:
            try:
                from engine.gex_overlay import get_latest_gex
                gex_cache[underlying] = get_latest_gex(underlying) or {}
            except Exception:
                gex_cache[underlying] = {}
        gex = gex_cache[underlying]

        # Generate signal
        signal, reason = _generate_signal(pos, greeks, gex, minutes_left)
        pnl_pct = pos.get("unrealized_plpc", 0) * 100

        # Save to log
        try:
            with _conn() as c:
                c.execute(
                    """INSERT INTO battle_station_log
                       (timestamp, symbol, option_symbol, underlying_price, option_price,
                        delta, gamma, theta, pnl_pct, signal, reason)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        now_ts, underlying, option_sym,
                        spot, pos.get("current_price", 0),
                        greeks.get("delta"), greeks.get("gamma"), greeks.get("theta"),
                        pnl_pct, signal, reason,
                    ),
                )
                c.commit()
        except Exception as e:
            logger.warning(f"battle_station_log save failed: {e}")

        # Act on signal
        if signal == "CLOSE_NOW":
            logger.warning(f"CLOSE_NOW triggered: {option_sym} — {reason}")
            _auto_close(pos, reason)

        elif signal == "TIGHTEN":
            # Throttle: one TIGHTEN post per 15 min per position
            last_post = _last_tighten_ts.get(option_sym, 0)
            if time.time() - last_post > TIGHTEN_POST_COOLDOWN:
                try:
                    from engine.war_room import save_hot_take
                    msg = (
                        f"⚠️ BATTLE STATION: {underlying} {option_sym[-10:]} — {reason}"
                    )
                    save_hot_take("dayblade-0dte", underlying, msg)
                    _last_tighten_ts[option_sym] = time.time()
                except Exception:
                    pass

        _last_signal[option_sym] = signal


# ---------------------------------------------------------------------------
# Status / API helpers
# ---------------------------------------------------------------------------

def get_battle_station_status() -> dict:
    """Return current status for /api/battle-station/status endpoint."""
    _init_tables()

    try:
        from engine.risk_manager import RiskManager
        mh = RiskManager.is_market_hours()
        market_open = mh == "market"  # only True during regular market hours
        market_status = mh if isinstance(mh, str) else ("open" if mh else "closed")
    except Exception:
        market_open = False
        market_status = "closed"

    positions = _get_alpaca_options_positions()

    # Latest signal per option from last 5 minutes
    latest_signals = []
    try:
        with _conn() as c:
            rows = c.execute(
                """SELECT option_symbol, symbol, signal, reason, pnl_pct, timestamp
                   FROM battle_station_log
                   WHERE timestamp >= datetime('now', '-5 minutes')
                   ORDER BY timestamp DESC""",
            ).fetchall()
        seen: set[str] = set()
        for r in rows:
            if r["option_symbol"] not in seen:
                latest_signals.append(dict(r))
                seen.add(r["option_symbol"])
    except Exception:
        pass

    # Morning levels from DB
    spy_levels = _get_morning_levels_db("SPY")
    qqq_levels = _get_morning_levels_db("QQQ")

    # GEX regime
    gex_regime = "unknown"
    try:
        from engine.gex_overlay import get_latest_gex
        spy_gex = get_latest_gex("SPY")
        if spy_gex:
            gex_regime = spy_gex.get("regime", "unknown")
    except Exception:
        pass

    return {
        "status": "ONLINE" if (market_open or len(positions) > 0) else "OFFLINE",
        "market_status": market_status,
        "market_open": market_open,
        "active_positions": len(positions),
        "positions": positions,
        "latest_signals": latest_signals,
        "morning_levels": {"SPY": spy_levels, "QQQ": qqq_levels},
        "gex_regime": gex_regime,
        "minutes_until_eod": round(_minutes_until_eod_close(), 1),
    }


def get_recent_log(limit: int = 20) -> list[dict]:
    """Return recent battle_station_log entries for /api/battle-station/log."""
    _init_tables()
    try:
        with _conn() as c:
            rows = c.execute(
                "SELECT * FROM battle_station_log ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
