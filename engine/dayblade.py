"""DayBlade Options — Season 2 Multi-DTE Options Engine.

Capital: $5,000. Trades 0DTE scalps, 1-7DTE swings on top 25 options tickers.
Features: scalp ladder entries, momentum doubling, GEX/gamma-aware sizing,
intelligent DTE selection, power hour mode, win streak tracking, cocky war room personality.
"""
from __future__ import annotations
import sqlite3
import time
import threading
from datetime import datetime, timedelta
from rich.console import Console

from engine.market_data import get_stock_price, get_technical_indicators
from engine.news_fetcher import get_news_for_symbol
from engine.providers.base import AIProvider, TradeDecision, RateLimiter

console = Console()

DB = "data/trader.db"
DAYBLADE_PLAYER = "dayblade-0dte"
DAYBLADE_CASH = 5000.00
DAYBLADE_TICKERS = [
    "SPY", "QQQ", "TSLA", "NVDA", "IWM", "ORCL", "HIMS", "AMZN", "MSFT", "AAPL",
    "MU", "AMD", "META", "SLV", "NIO", "INTC", "PLTR", "IBIT", "RIVN", "SOFI",
    "GOOGL", "NFLX", "MSTR", "AVGO", "BABA",
]

# ── Season 2 Risk Parameters ─────────────────────────────────────
MAX_POSITIONS = 4   # Fewer, better trades
MAX_TRADE_PCT = 0.06  # 6% base per trade (bigger conviction bets)

# DTE-specific stop/target rules
DTE_RULES = {
    "0dte":   {"stop": -0.30, "target": 1.00, "partial": 0.50, "label": "0DTE Scalp"},
    "1dte":   {"stop": -0.40, "target": 1.00, "partial": 0.50, "label": "1DTE Overnight"},
    "2-3dte": {"stop": -0.40, "target": 0.75, "partial": 0.40, "label": "2-3DTE Short Swing"},
    "4-7dte": {"stop": -0.50, "target": 1.50, "partial": 0.75, "label": "4-7DTE Weekly Thesis"},
}

# Stop-loss cooldown: after a stop, wait 30 min before re-entering same symbol
_stop_cooldown: dict = {}  # {symbol: timestamp}
STOP_COOLDOWN_SECONDS = 1800  # 30 minutes

# Scalp ladder: 40% first signal, 30% confirmation, 30% momentum
LADDER_TRANCHES = [0.40, 0.30, 0.30]

# Momentum chaser: double down if +50% in 30 min
MOMENTUM_DOUBLE_THRESHOLD = 0.50
MOMENTUM_WINDOW_MINUTES = 30

# War Room personality — cocky, fast-talking trash talker
WAR_ROOM_LINES = [
    "Another day, another scalp. You're welcome.",
    "While you were reading charts, I already banked it.",
    "Stop-loss? That's for the other guys. I just don't pick losers.",
    "Three-peat! Somebody call the fire department, DayBlade is ON FIRE.",
    "I don't chase. The market chases ME.",
    "Gamma flipped and I was already positioned. Stay mad.",
    "This is what a win streak looks like. Take notes.",
    "Your favorite model's annual return? That's my Tuesday.",
    "0DTE is not for the faint of heart. Good thing I don't have one.",
    "Doubled down and doubled up. Math is undefeated.",
]


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def _now_et():
    """Return current datetime in US/Eastern."""
    import pytz
    return datetime.now(pytz.timezone("US/Eastern"))


def _current_season() -> int:
    try:
        c = _conn()
        row = c.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        c.close()
        return int(row[0]) if row else 2
    except Exception:
        return 2


# ── Time Windows ─────────────────────────────────────────────────

def is_dayblade_open_window() -> bool:
    """True if between 9:45 AM and 3:30 PM ET on a weekday (extended for Season 2)."""
    now = _now_et()
    if now.weekday() >= 5:
        return False
    t = now.time()
    from datetime import time as dtime
    return dtime(9, 45) <= t < dtime(15, 30)


def is_power_hour() -> bool:
    """True if 2:00 PM - 3:30 PM ET — scan every 15 seconds."""
    now = _now_et()
    if now.weekday() >= 5:
        return False
    t = now.time()
    from datetime import time as dtime
    return dtime(14, 0) <= t < dtime(15, 30)


def is_dayblade_close_window() -> bool:
    """True if 3:50 PM ET or later (force close 0DTE)."""
    now = _now_et()
    if now.weekday() >= 5:
        return False
    t = now.time()
    from datetime import time as dtime
    return dtime(15, 50) <= t < dtime(16, 0)


def is_market_hours_for_dayblade() -> bool:
    """True if within 9:30 AM - 4:00 PM ET on weekday."""
    now = _now_et()
    if now.weekday() >= 5:
        return False
    t = now.time()
    from datetime import time as dtime
    return dtime(9, 30) <= t < dtime(16, 0)


# ── DTE Selection ────────────────────────────────────────────────

def select_dte(symbol: str, signal_type: str, indicators: dict, news: list) -> tuple[int, str]:
    """Intelligent DTE selection — minimum 3 DTE to avoid instant theta decay.

    Returns (dte_days, dte_bucket) where dte_bucket is one of: "0dte", "1dte", "2-3dte", "4-7dte".
    """
    # Check for overnight catalyst (earnings, FDA, etc.) — only case for short DTE
    has_catalyst = False
    for n in (news or []):
        headline = (n.get("headline") or "").lower()
        if any(k in headline for k in ["earnings", "fda", "guidance", "acquisition", "merger"]):
            has_catalyst = True
            break

    if has_catalyst:
        return 3, "2-3dte"  # Even catalysts get 3 DTE minimum

    # Strong trend signal → 3 DTE swing
    rsi = indicators.get("rsi", 50)
    vol_ratio = indicators.get("volume_ratio", 1.0)
    if (rsi < 25 or rsi > 75) and vol_ratio > 1.5:
        return 3, "2-3dte"

    # High volume → 5 DTE weekly play
    if vol_ratio > 2.0:
        return 5, "4-7dte"

    # Default: 5 DTE — give trades time to work
    return 5, "4-7dte"


def _get_dte_bucket(expiry_date: str) -> str:
    """Determine DTE bucket from expiry date string."""
    try:
        exp = datetime.strptime(expiry_date, "%Y-%m-%d")
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        dte = (exp - today).days
        if dte <= 0:
            return "0dte"
        elif dte == 1:
            return "1dte"
        elif dte <= 3:
            return "2-3dte"
        else:
            return "4-7dte"
    except Exception:
        return "0dte"


# ── Portfolio Management ─────────────────────────────────────────

def ensure_player():
    """Ensure DayBlade player row exists in ai_players."""
    conn = _conn()
    row = conn.execute("SELECT id FROM ai_players WHERE id=?", (DAYBLADE_PLAYER,)).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO ai_players (id, display_name, provider, model_id, cash, season) VALUES (?,?,?,?,?,?)",
            (DAYBLADE_PLAYER, "DayBlade Options", "dayblade", "options-s2", DAYBLADE_CASH, _current_season())
        )
        conn.commit()
    conn.close()


def get_portfolio() -> dict:
    conn = _conn()
    row = conn.execute("SELECT cash FROM ai_players WHERE id=?", (DAYBLADE_PLAYER,)).fetchone()
    positions = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, opened_at "
        "FROM positions WHERE player_id=?", (DAYBLADE_PLAYER,)
    ).fetchall()
    conn.close()
    return {
        "cash": row[0] if row else DAYBLADE_CASH,
        "positions": [
            {"symbol": p[0], "qty": p[1], "avg_price": p[2], "asset_type": p[3],
             "option_type": p[4], "strike_price": p[5], "expiry_date": p[6],
             "opened_at": p[7] if len(p) > 7 else None}
            for p in positions
        ],
    }


def get_portfolio_with_pnl(prices: dict) -> dict:
    """Enrich positions with live P&L."""
    portfolio = get_portfolio()
    enriched = []
    total_unreal = 0.0
    total_val = 0.0
    total_cost = 0.0
    for pos in portfolio["positions"]:
        sym = pos["symbol"]
        avg = pos["avg_price"]
        qty = pos["qty"]
        cost_basis = qty * avg
        stock_price = prices.get(sym, {}).get("price", avg)
        # Use option pricing for options positions (not raw stock price)
        if pos.get("asset_type") == "option":
            from engine.paper_trader import estimate_option_price
            cur = estimate_option_price(
                pos.get("option_type"), pos.get("strike_price"),
                stock_price, avg, pos.get("expiry_date"))
        else:
            cur = stock_price
        mv = qty * cur
        pnl = mv - cost_basis
        pnl_pct = ((cur - avg) / avg * 100) if avg > 0 else 0.0
        dte_bucket = _get_dte_bucket(pos.get("expiry_date") or "")
        enriched.append({
            **pos,
            "current_price": round(cur, 2),
            "market_value": round(mv, 2),
            "cost_basis": round(cost_basis, 2),
            "unrealized_pnl": round(pnl, 2),
            "unrealized_pnl_pct": round(pnl_pct, 2),
            "dte_bucket": dte_bucket,
        })
        total_unreal += pnl
        total_val += mv
        total_cost += cost_basis
    total = portfolio["cash"] + total_val
    return {
        "cash": portfolio["cash"],
        "positions": enriched,
        "total_positions_value": round(total_val, 2),
        "total_cost_basis": round(total_cost, 2),
        "total_unrealized_pnl": round(total_unreal, 2),
        "total_value": round(total, 2),
        "return_pct": round((total - DAYBLADE_CASH) / DAYBLADE_CASH * 100, 2),
    }


# ── Gamma-Aware Sizing ──────────────────────────────────────────

def _get_gamma_sizing() -> tuple[float, dict]:
    """Get sizing factor and gamma environment info.

    Negative gamma = size down 50% (factor 0.50)
    Positive gamma = size up 25% (factor 1.25)
    """
    try:
        from engine.gamma_environment import detect_gamma_environment
        genv = detect_gamma_environment()
        env = genv.get("environment", "unknown")
        if env == "negative":
            return 0.50, genv
        elif env == "positive":
            return 1.25, genv
        return 1.0, genv
    except Exception:
        return 1.0, {}


def _get_top3_gex(symbol: str) -> list:
    """Get top 3 GEX levels for a symbol."""
    try:
        from engine.gex_scanner import get_gex_magnets
        return get_gex_magnets(symbol)[:3]
    except Exception:
        return []


# ── Trade Execution ──────────────────────────────────────────────

def _estimate_atm_premium(stock_price: float, dte_days: int, option_type: str = "call") -> float:
    """Estimate ATM option premium from stock price and DTE.
    Uses a rough approximation: premium ~ stock_price * volatility * sqrt(DTE/365).
    Typical ATM options are 1-5% of stock price for short DTE.
    """
    import math
    # Assume ~30% annualized vol for typical stocks
    vol = 0.30
    dte = max(dte_days, 0.5)  # min half day for 0DTE
    time_value = stock_price * vol * math.sqrt(dte / 365)
    # ATM option = mostly time value, add small intrinsic buffer
    premium = max(0.50, round(time_value, 2))
    return premium


def buy_option(symbol: str, price: float, option_type: str,
               reasoning: str = "", confidence: float = 0.0,
               dte_days: int = 0, tranche: str = "full",
               strike_price: float = None) -> dict | None:
    """Buy an option position with gamma-aware sizing.

    `price` is the STOCK price. We estimate the option premium from it.
    """
    # Estimate option premium (NOT the stock price)
    premium = _estimate_atm_premium(price, dte_days, option_type)
    atm_strike = round(price)  # ATM strike = nearest whole dollar

    # Try to get real premium from options chain
    try:
        from engine.openbb_data import get_options_chain
        chain = get_options_chain(symbol)
        if chain:
            chain_key = "calls" if option_type == "call" else "puts"
            options = chain.get(chain_key, [])
            # Find closest ATM option
            best = None
            best_dist = float("inf")
            for opt in options:
                if opt.get("strike") and opt.get("ask") and opt["ask"] > 0:
                    dist = abs(opt["strike"] - price)
                    if dist < best_dist:
                        best_dist = dist
                        best = opt
            if best:
                # Use mid price (or ask if no bid)
                bid = best.get("bid", 0) or 0
                ask = best.get("ask", 0) or 0
                premium = round((bid + ask) / 2, 2) if bid > 0 else round(ask, 2)
                atm_strike = best["strike"]
    except Exception:
        pass  # Fall back to estimated premium

    # Override strike if explicitly provided
    if strike_price:
        atm_strike = strike_price

    portfolio = get_portfolio()
    cash = portfolio["cash"]
    total_value = cash + sum(
        p["qty"] * (p["avg_price"] if p.get("asset_type") != "option" else p["avg_price"])
        for p in portfolio["positions"]
    )

    if len(portfolio["positions"]) >= MAX_POSITIONS:
        console.log(f"[yellow]DayBlade: Max {MAX_POSITIONS} positions reached")
        return None

    # Gamma-aware sizing
    gamma_factor, _ = _get_gamma_sizing()

    # VIX factor
    vix_factor = 1.0
    try:
        from engine.cross_asset import get_vix_sizing_factor
        vix_factor = get_vix_sizing_factor()
    except Exception:
        pass

    sizing_factor = max(0.25, min(1.5, gamma_factor * vix_factor))

    # Ladder tranche sizing
    tranche_pct = 1.0
    if tranche == "first":
        tranche_pct = LADDER_TRANCHES[0]  # 40%
    elif tranche == "confirm":
        tranche_pct = LADDER_TRANCHES[1]  # 30%
    elif tranche == "momentum":
        tranche_pct = LADDER_TRANCHES[2]  # 30%

    # Size based on PREMIUM, not stock price
    max_cost = total_value * MAX_TRADE_PCT * sizing_factor * tranche_pct
    qty = max(1, round(max_cost / premium, 4))
    cost = round(qty * premium, 2)
    if cost > cash:
        qty = max(1, round((cash * 0.90) / premium, 4))
        cost = round(qty * premium, 2)
    if cost > cash or qty <= 0:
        console.log(f"[red]DayBlade: Not enough cash for {symbol}")
        return None

    # Compute expiry date
    now = _now_et()
    expiry = (now + timedelta(days=dte_days)).strftime("%Y-%m-%d")

    season = _current_season()

    conn = _conn()
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (round(cash - cost, 2), DAYBLADE_PLAYER))
    conn.execute(
        "INSERT INTO positions(player_id, symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (DAYBLADE_PLAYER, symbol, qty, premium, "option", option_type, atm_strike, expiry)
    )
    conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "strike_price, expiry_date, reasoning, confidence, season) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (DAYBLADE_PLAYER, symbol, f"BUY_{option_type.upper()}", qty, premium, "option",
         option_type, atm_strike, expiry, f"[{tranche.upper()}] {reasoning}", confidence, season)
    )
    conn.commit()
    conn.close()
    dte_label = f"{dte_days}DTE" if dte_days > 0 else "0DTE"
    console.log(f"[bold cyan]DayBlade: BUY {option_type.upper()} {qty} {symbol} @ ${premium:.2f} strike=${atm_strike:.0f} ({dte_label}, {tranche})")
    return {"action": f"BUY_{option_type.upper()}", "symbol": symbol, "qty": qty, "price": premium, "dte": dte_days}


def sell_position(symbol: str, price: float, option_type: str,
                  reasoning: str = "", qty_override: float = None) -> dict | None:
    conn = _conn()
    row = conn.execute(
        "SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=? AND option_type=?",
        (DAYBLADE_PLAYER, symbol, option_type)
    ).fetchone()
    if not row:
        conn.close()
        return None

    # GUARD: Refuse to sell options at $0.00 — price was not captured correctly
    if price < 0.01:
        console.log(f"[bold red]⚠ BLOCKED DayBlade SELL {option_type.upper()} {symbol}: exit price ${price:.4f} < $0.01 — skipping to protect position")
        conn.close()
        return None

    qty = qty_override if qty_override and qty_override < row[0] else row[0]
    proceeds = round(qty * price, 2)
    pnl = round(proceeds - (qty * row[1]), 2)

    portfolio = get_portfolio()
    new_cash = round(portfolio["cash"] + proceeds, 2)
    conn.execute("UPDATE ai_players SET cash=? WHERE id=?", (new_cash, DAYBLADE_PLAYER))

    remaining = round(row[0] - qty, 4)
    if remaining <= 0:
        conn.execute(
            "DELETE FROM positions WHERE player_id=? AND symbol=? AND option_type=?",
            (DAYBLADE_PLAYER, symbol, option_type)
        )
    else:
        conn.execute(
            "UPDATE positions SET qty=? WHERE player_id=? AND symbol=? AND option_type=?",
            (remaining, DAYBLADE_PLAYER, symbol, option_type)
        )

    season = _current_season()
    conn.execute(
        "INSERT INTO trades(player_id, symbol, action, qty, price, asset_type, option_type, "
        "reasoning, confidence, entry_price, exit_price, realized_pnl, season) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (DAYBLADE_PLAYER, symbol, "SELL", qty, price, "option", option_type, reasoning, 0.0,
         row[1], price, pnl, season)
    )
    conn.commit()
    conn.close()

    # Update win streak
    _update_win_streak(pnl > 0)

    # War room trash talk on wins
    if pnl > 0:
        _post_war_room(symbol, pnl, price)

    console.log(f"[bold yellow]DayBlade: SELL {option_type.upper()} {qty} {symbol} @ ${price:.2f} PnL: ${pnl:.2f}")
    return {"action": "SELL", "symbol": symbol, "qty": qty, "pnl": pnl}


def force_close_all(prices: dict, dte_filter: str = None):
    """Close DayBlade positions. If dte_filter set, only close that bucket."""
    portfolio = get_portfolio()
    closed = 0
    for pos in portfolio["positions"]:
        sym = pos["symbol"]
        if dte_filter:
            bucket = _get_dte_bucket(pos.get("expiry_date") or "")
            if bucket != dte_filter:
                continue
        cur_price = prices.get(sym, {}).get("price", pos["avg_price"])
        sell_position(sym, cur_price, pos.get("option_type", "call"),
                      reasoning=f"Auto-close {'0DTE 3:50 PM' if dte_filter == '0dte' else 'EOD'}")
        closed += 1
    if closed:
        console.log(f"[bold red]DayBlade: Force-closed {closed} positions")


# ── Momentum Chaser ──────────────────────────────────────────────

# Track recent prices for momentum detection: {symbol: [(timestamp, price)]}
_momentum_history: dict[str, list] = {}
_momentum_lock = threading.Lock()


def _track_price(symbol: str, price: float):
    """Record price tick for momentum detection."""
    now = time.time()
    with _momentum_lock:
        if symbol not in _momentum_history:
            _momentum_history[symbol] = []
        _momentum_history[symbol].append((now, price))
        # Keep only last 60 minutes
        cutoff = now - 3600
        _momentum_history[symbol] = [(t, p) for t, p in _momentum_history[symbol] if t > cutoff]


def _check_momentum_double(symbol: str, current_price: float) -> bool:
    """Check if position has gained +50% in last 30 minutes → momentum double signal."""
    with _momentum_lock:
        history = _momentum_history.get(symbol, [])
    if not history:
        return False

    now = time.time()
    cutoff = now - (MOMENTUM_WINDOW_MINUTES * 60)
    window_prices = [p for t, p in history if t >= cutoff]
    if not window_prices:
        return False

    oldest = window_prices[0]
    if oldest <= 0:
        return False
    gain = (current_price - oldest) / oldest
    return gain >= MOMENTUM_DOUBLE_THRESHOLD


# ── Win Streak & War Room ────────────────────────────────────────

_win_streak = {"current": 0, "best": 0}


def _update_win_streak(is_win: bool):
    """Update win streak counter."""
    if is_win:
        _win_streak["current"] += 1
        if _win_streak["current"] > _win_streak["best"]:
            _win_streak["best"] = _win_streak["current"]
    else:
        _win_streak["current"] = 0


def get_win_streak() -> dict:
    """Get current and best win streaks. Also loads from DB on first call."""
    if _win_streak["current"] == 0 and _win_streak["best"] == 0:
        # Bootstrap from recent trades
        try:
            conn = _conn()
            conn.row_factory = sqlite3.Row
            recent = conn.execute(
                "SELECT realized_pnl FROM trades WHERE player_id=? AND action='SELL' "
                "AND season=? ORDER BY executed_at DESC LIMIT 50",
                (DAYBLADE_PLAYER, _current_season())
            ).fetchall()
            conn.close()
            # Walk backwards to find current streak
            streak = 0
            for t in recent:
                if t["realized_pnl"] and t["realized_pnl"] > 0:
                    streak += 1
                else:
                    break
            _win_streak["current"] = streak
            _win_streak["best"] = max(streak, _win_streak["best"])
        except Exception:
            pass
    return dict(_win_streak)


def _post_war_room(symbol: str, pnl: float, price: float):
    """Post trash talk to the war room on profitable trades."""
    import random
    streak = _win_streak["current"]

    # Pick a line, personalize it
    base = random.choice(WAR_ROOM_LINES)

    if streak >= 3:
        take = f"{streak}-streak and counting! {base} ${symbol} just paid me ${pnl:.2f}."
    elif pnl > 100:
        take = f"Big money! ${pnl:.2f} on {symbol}. {base}"
    else:
        take = f"${pnl:.2f} on {symbol}. {base}"

    try:
        from engine.war_room import save_hot_take
        save_hot_take(DAYBLADE_PLAYER, symbol, take)
    except Exception:
        pass


# ── DayBlade AI Prompt ───────────────────────────────────────────

def build_dayblade_prompt(symbol: str, price: float, change_pct: float,
                          high: float, low: float, indicators: dict,
                          news: list, portfolio: dict,
                          gamma_env: dict = None, gex_levels: list = None) -> str:
    positions = portfolio.get("positions", [])
    cash = portfolio.get("cash", 0)
    pos_str = ", ".join(
        f"{p['symbol']} {(p.get('option_type') or '?').upper()}({p['qty']}@${p['avg_price']:.2f})"
        for p in positions
    ) or "None"

    streak = get_win_streak()
    streak_str = f"Win Streak: {streak['current']} (Best: {streak['best']})"

    # Indicators
    ind_lines = []
    rsi = indicators.get("rsi")
    if rsi is not None:
        zone = "OVERSOLD" if rsi < 30 else "OVERBOUGHT" if rsi > 70 else "NEUTRAL"
        ind_lines.append(f"- RSI(14): {rsi} [{zone}]")
    macd_h = indicators.get("macd_histogram")
    if macd_h is not None:
        direction = "BULLISH" if macd_h > 0 else "BEARISH"
        ind_lines.append(f"- MACD Histogram: {macd_h} [{direction}]")
    vol_r = indicators.get("volume_ratio")
    if vol_r is not None:
        vol_label = "SPIKE" if vol_r > 2.0 else "HIGH" if vol_r > 1.5 else "NORMAL"
        ind_lines.append(f"- Volume Ratio: {vol_r}x [{vol_label}]")
    sma50 = indicators.get("sma_50")
    if sma50 is not None:
        rel = "ABOVE" if price > sma50 else "BELOW"
        ind_lines.append(f"- Price vs SMA50: ${sma50:.2f} [{rel}]")
    ind_block = "\n".join(ind_lines) if ind_lines else "- No indicators available"

    # News
    news_lines = []
    for n in (news or [])[:5]:
        headline = n.get("headline", "")[:120]
        news_lines.append(f"  - {headline}")
    news_block = "\n".join(news_lines) if news_lines else "  - No breaking news"

    # GEX levels (top 3)
    gex_block = ""
    if gex_levels:
        gex_lines = ["=== TOP 3 GEX LEVELS ==="]
        for i, m in enumerate(gex_levels, 1):
            label = "CALL WALL (pin/support)" if m.get("type") == "call_wall" else "PUT WALL (accelerator)"
            gex_lines.append(f"  #{i}: ${m['strike']:.2f} — {label} (GEX: {m.get('net_gex', 0):+,.0f})")
        gex_block = "\n".join(gex_lines)
    else:
        try:
            from engine.gex_scanner import build_gex_prompt_section
            gex_block = build_gex_prompt_section(symbol)
        except Exception:
            pass

    # Gamma environment
    gamma_block = ""
    genv = gamma_env
    if not genv:
        try:
            from engine.gamma_environment import detect_gamma_environment
            genv = detect_gamma_environment()
        except Exception:
            genv = {}
    if genv and genv.get("environment") != "unknown":
        env_label = genv.get("label", "UNKNOWN")
        sizing = "SIZE DOWN 50%" if genv.get("environment") == "negative" else "SIZE UP 25%"
        gamma_block = (
            f"\n=== GAMMA: {env_label} → {sizing} ===\n"
            f"{genv.get('description', '')}\n"
            f"Intensity: {genv.get('intensity', 'unknown').upper()}"
        )
        if genv.get("gamma_flip"):
            gamma_block += f" | Gamma Flip: ${genv['gamma_flip']}"

    # Options chain context
    chain_block = ""
    try:
        from engine.openbb_data import get_options_chain
        chain = get_options_chain(symbol)
        if chain:
            calls = [c for c in chain.get("calls", []) if c["strike"] and abs(c["strike"] - price) / price < 0.02]
            puts = [p for p in chain.get("puts", []) if p["strike"] and abs(p["strike"] - price) / price < 0.02]
            if calls or puts:
                chain_lines = [f"\n=== ATM Options (exp: {chain['expiry']}) ==="]
                for c in calls[:2]:
                    chain_lines.append(f"  CALL ${c['strike']}: bid=${c.get('bid','?')} ask=${c.get('ask','?')} IV={c.get('iv','?')} OI={c.get('open_interest',0)}")
                for p in puts[:2]:
                    chain_lines.append(f"  PUT ${p['strike']}: bid=${p.get('bid','?')} ask=${p.get('ask','?')} IV={p.get('iv','?')} OI={p.get('open_interest',0)}")
                chain_block = "\n".join(chain_lines)
    except Exception:
        pass

    # Volatility Breakout — highest-priority 0DTE setup
    breakout_block = ""
    try:
        from engine.volatility_breakout import build_dayblade_breakout_section
        breakout_block = build_dayblade_breakout_section(symbol)
    except Exception:
        pass

    # Held options for this symbol
    held_opts = {p.get("option_type") for p in positions if p["symbol"] == symbol}
    hold_note = ""
    if "call" in held_opts and "put" in held_opts:
        hold_note = f"\nYou already hold BOTH a CALL and PUT on {symbol}. HOLD only."
    elif "call" in held_opts:
        hold_note = f"\nYou already hold a CALL on {symbol}. BUY_PUT only if bearish."
    elif "put" in held_opts:
        hold_note = f"\nYou already hold a PUT on {symbol}. BUY_CALL only if bullish."

    power = " [POWER HOUR MODE - AGGRESSIVE]" if is_power_hour() else ""

    return f"""You are DayBlade — the fastest, most ruthless options trader in the arena.{power}
Season 2: $5,000 capital, multi-DTE engine, ladder entries, momentum doubling.
You are COCKY because you WIN. {streak_str}

SEASON 2 RULES:
1. Max {MAX_POSITIONS} simultaneous positions. You have {len(positions)} now.
2. Scalp ladder: 40% on first signal, 30% on confirmation, 30% on momentum.
3. STOP LOSS: -35% of premium. No exceptions. Cut fast, move on.
4. DTE SELECTION — tell me which DTE you want:
   - 0DTE: Quick scalps, intraday only. Auto-close at 3:50 PM.
   - 1DTE: Overnight catalyst plays (earnings, FDA, macro).
   - 2-3DTE: Short swing setups (strong trend + volume).
   - 4-7DTE: Weekly thesis plays (sector rotation, breakouts).
5. PROFIT TARGETS: 0-1DTE: +100%. 2-3DTE: +100%. 4-7DTE: +150%.
6. Gamma awareness: Negative gamma = reduce size 50%. Positive = size up 25%.
7. Momentum chaser: If holding position gains +50% in 30 min, DOUBLE DOWN.
8. Confidence >= 0.70 to trade. A+ setups only.
{hold_note}

Current Positions: {pos_str}
Cash: ${cash:,.2f}

{symbol} RIGHT NOW:
- Price: ${price:.2f}
- Day Change: {change_pct:+.2f}%
- High: ${high:.2f} | Low: ${low:.2f} | Range: ${high - low:.2f} ({(high - low) / price * 100:.1f}%)

Intraday Signals:
{ind_block}

Breaking News:
{news_block}

{gex_block}
{gamma_block}
{chain_block}
{breakout_block}

Respond with EXACTLY:
Decision: BUY_CALL or BUY_PUT or HOLD
DTE: 0 or 1 or 2 or 3 or 5 or 7
Confidence: [0.0 to 1.0]
Reasoning: [1-2 sentences]"""


def parse_dayblade_decision(text: str, symbol: str) -> TradeDecision:
    """Parse AI response into a TradeDecision with DTE info."""
    import re
    action = "HOLD"
    option_type = ""

    for line in text.split("\n"):
        ls = line.strip().lower()
        if ls.startswith("decision:"):
            val = ls.replace("decision:", "").strip()
            if "buy_put" in val:
                action = "BUY_PUT"
                option_type = "put"
            elif "buy_call" in val:
                action = "BUY_CALL"
                option_type = "call"
            break

    # Parse DTE
    dte = 0
    for line in text.split("\n"):
        ls = line.strip().lower()
        if ls.startswith("dte:"):
            nums = re.findall(r'\d+', ls)
            if nums:
                dte = min(int(nums[0]), 7)
            break

    confidence = 0.5
    for line in text.split("\n"):
        ls = line.strip().lower()
        if ls.startswith("confidence:"):
            nums = re.findall(r'[\d.]+', ls)
            for n in nums:
                try:
                    val = float(n)
                    if 0 < val <= 1:
                        confidence = val
                        break
                    elif 1 < val <= 100:
                        confidence = val / 100
                        break
                except ValueError:
                    pass
            break

    if action != "HOLD" and confidence < 0.70:
        action = "HOLD"
        option_type = ""

    reasoning = ""
    for line in text.split("\n"):
        if line.strip().lower().startswith("reasoning:"):
            reasoning = line.strip()[len("reasoning:"):].strip()
            break
    if not reasoning:
        reasoning = text.strip()[:200]

    decision = TradeDecision(
        action=action, confidence=confidence, reasoning=reasoning,
        symbol=symbol, option_type=option_type,
    )
    # Attach DTE as extra attribute
    decision._dte = dte
    return decision


# ── DayBlade Scanner (Season 2) ─────────────────────────────────

class DayBladeScanner:
    """Runs the multi-DTE options loop using the configured AI provider."""

    def __init__(self, provider: AIProvider):
        self.provider = provider
        self._scan_count = 0
        self._last_power_scan = 0

    def run_scan(self):
        """One scan cycle: check SL/TP, momentum doubles, entries, auto-close."""
        # Check if DayBlade is paused or deactivated
        try:
            _c = sqlite3.connect(DB, check_same_thread=False, timeout=10)
            _r = _c.execute("SELECT is_active, COALESCE(is_paused,0) as is_paused FROM ai_players WHERE id=?", (DAYBLADE_PLAYER,)).fetchone()
            _c.close()
            if _r and (not _r[0] or _r[1]):
                return
        except Exception:
            pass

        if not is_market_hours_for_dayblade():
            return

        # Power hour: skip if last scan was < 15 seconds ago
        now = time.time()
        if is_power_hour():
            if now - self._last_power_scan < 15:
                return
            self._last_power_scan = now

        ensure_player()

        # Fetch prices
        prices = {}
        for sym in DAYBLADE_TICKERS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data
                _track_price(sym, data["price"])

        if not prices:
            return

        # Preload gamma environment (used in sizing + prompt)
        gamma_factor, gamma_env = _get_gamma_sizing()

        # ── EOD: Auto-close 0DTE at 3:50 PM ──
        if is_dayblade_close_window():
            force_close_all(prices, dte_filter="0dte")
            return

        # ── Check SL/TP per DTE bucket ──
        from engine.paper_trader import estimate_option_price
        portfolio = get_portfolio()
        for pos in portfolio["positions"]:
            sym = pos["symbol"]
            if sym not in prices:
                continue
            stock_price = prices[sym]["price"]
            avg = pos["avg_price"]  # This is the option PREMIUM entry
            if avg <= 0:
                continue
            # Estimate current option value from stock price
            cur = estimate_option_price(
                pos.get("option_type", "call"),
                pos.get("strike_price"),
                stock_price, avg,
                pos.get("expiry_date")
            )
            pnl_pct = (cur - avg) / avg
            bucket = _get_dte_bucket(pos.get("expiry_date") or "")
            rules = DTE_RULES.get(bucket, DTE_RULES["0dte"])

            # Hard stop — with cooldown to prevent re-buying same symbol
            if pnl_pct <= rules["stop"]:
                _stop_cooldown[sym] = time.time()
                sell_position(sym, cur, pos.get("option_type", "call"),
                              reasoning=f"STOP [{bucket}]: {pnl_pct:.0%} loss")
                continue

            # Full target
            if pnl_pct >= rules["target"]:
                sell_position(sym, cur, pos.get("option_type", "call"),
                              reasoning=f"TARGET [{bucket}]: +{pnl_pct:.0%} gain, closing full")
                continue

            # Partial at threshold
            if pnl_pct >= rules["partial"]:
                half_qty = round(pos["qty"] / 2, 4)
                if half_qty > 0:
                    sell_position(sym, cur, pos.get("option_type", "call"),
                                  reasoning=f"PARTIAL [{bucket}]: +{pnl_pct:.0%} gain, taking half",
                                  qty_override=half_qty)

            # ── Momentum chaser: +50% in 30 min → double down ──
            if pnl_pct > 0 and _check_momentum_double(sym, stock_price):
                if len(get_portfolio()["positions"]) < MAX_POSITIONS:
                    opt_type = pos.get("option_type", "call")
                    buy_option(sym, stock_price, opt_type,
                               reasoning=f"MOMENTUM DOUBLE: {sym} +50% in 30 min, riding the wave",
                               confidence=0.85, dte_days=0, tranche="momentum")
                    console.log(f"[bold magenta]DayBlade: MOMENTUM DOUBLE on {sym}!")

        # ── Only open new positions during entry window ──
        if not is_dayblade_open_window():
            self._scan_count += 1
            return

        # Refresh portfolio after SL/TP
        portfolio = get_portfolio()
        if len(portfolio["positions"]) >= MAX_POSITIONS:
            self._scan_count += 1
            return

        # Rank tickers — prioritize today's top momentum movers
        try:
            # Sort by absolute % change * volume ratio for momentum ranking
            momentum_ranked = sorted(
                [(sym, abs(d.get("change_pct", 0)), d.get("volume", 0))
                 for sym, d in prices.items()],
                key=lambda x: x[1],
                reverse=True
            )
            # Top 3 momentum stocks go first (these are highest probability 0DTE)
            priority_symbols = [sym for sym, pct, vol in momentum_ranked[:3] if pct >= 1.0]

            # Then fill with DTE scanner picks
            from engine.dte_scanner import get_top_scored
            top_opps = get_top_scored(10)
            seen = set(priority_symbols)
            for opp in top_opps:
                sym = opp["symbol"]
                if sym not in seen and sym in prices:
                    priority_symbols.append(sym)
                    seen.add(sym)
            if not priority_symbols:
                priority_symbols = list(prices.keys())[:10]
        except Exception:
            priority_symbols = list(prices.keys())[:10]

        # Fetch indicators
        indicators = {}
        for sym in priority_symbols:
            ind = get_technical_indicators(sym)
            if ind:
                indicators[sym] = ind

        # Analyze top tickers
        for sym in priority_symbols[:6]:
            if sym not in prices:
                continue
            if len(get_portfolio()["positions"]) >= MAX_POSITIONS:
                break

            # Cooldown check: skip if recently stopped out
            if sym in _stop_cooldown and time.time() - _stop_cooldown[sym] < STOP_COOLDOWN_SECONDS:
                console.log(f"[yellow]DayBlade: {sym} on cooldown ({int((STOP_COOLDOWN_SECONDS - (time.time() - _stop_cooldown[sym])) / 60)}m left)")
                continue

            data = prices[sym]
            sym_ind = indicators.get(sym, {})
            sym_news = get_news_for_symbol(sym, limit=3)
            portfolio = get_portfolio()

            held_types = {p.get("option_type") for p in portfolio["positions"] if p["symbol"] == sym}

            # Get GEX levels for this specific symbol
            gex_levels = _get_top3_gex(sym)

            prompt = build_dayblade_prompt(
                sym, data["price"], data["change_pct"],
                data["high"], data["low"], sym_ind, sym_news, portfolio,
                gamma_env=gamma_env, gex_levels=gex_levels,
            )

            try:
                self.provider.limiter.wait()
                response = self.provider.call_model(prompt)
                decision = parse_dayblade_decision(response, sym)

                # Save signal
                from engine.paper_trader import save_signal
                save_signal(DAYBLADE_PLAYER, sym, decision.action, decision.confidence,
                            decision.reasoning, asset_type="option", option_type=decision.option_type)

                if decision.action == "HOLD":
                    continue

                opt_type = "call" if decision.action == "BUY_CALL" else "put"
                if opt_type in held_types:
                    console.log(f"[yellow]DayBlade: Already holding {sym} {opt_type.upper()}, skip")
                    continue

                # Direction check: don't buy calls on falling stock, puts on rising
                change_pct = data.get("change_pct", 0)
                if opt_type == "call" and change_pct < -0.5:
                    console.log(f"[yellow]DayBlade: {sym} down {change_pct:.1f}% — skipping CALL (wrong direction)")
                    continue
                if opt_type == "put" and change_pct > 0.5:
                    console.log(f"[yellow]DayBlade: {sym} up {change_pct:+.1f}% — skipping PUT (wrong direction)")
                    continue

                # DTE from AI or auto-select (minimum 3 DTE)
                dte_days = getattr(decision, "_dte", 0)
                if dte_days < 3:
                    dte_days, _ = select_dte(sym, "scalp", sym_ind, sym_news)

                # First tranche entry (40%)
                result = buy_option(
                    sym, data["price"], opt_type,
                    reasoning=decision.reasoning, confidence=decision.confidence,
                    dte_days=dte_days, tranche="first",
                )
                if result:
                    console.log(f"[bold cyan]DayBlade: {result}")

            except Exception as e:
                console.log(f"[red]DayBlade error on {sym}: {e}")

        self._scan_count += 1

        # Log status every 10 scans
        if self._scan_count % 10 == 0:
            pnl = get_portfolio_with_pnl(prices)
            streak = get_win_streak()
            console.log(
                f"[cyan]DayBlade: ${pnl['total_value']:,.2f} | "
                f"Pos: {len(pnl['positions'])}/{MAX_POSITIONS} | "
                f"P&L: ${pnl['total_unrealized_pnl']:+,.2f} | "
                f"Streak: {streak['current']}"
            )


# ── Stats (Season 2) ────────────────────────────────────────────

def get_dayblade_stats() -> dict:
    """Get DayBlade trading stats including DTE-bucket breakdowns and win streak."""
    conn = _conn()
    conn.row_factory = sqlite3.Row
    season = _current_season()

    total = conn.execute(
        "SELECT COUNT(*) as cnt FROM trades WHERE player_id=? AND season=?",
        (DAYBLADE_PLAYER, season)
    ).fetchone()

    # Sells with P&L
    sells = conn.execute(
        "SELECT t.symbol, t.qty, t.price as sell_price, t.option_type, t.executed_at, "
        "t.expiry_date, t.realized_pnl, t.entry_price "
        "FROM trades t WHERE t.player_id=? AND t.action='SELL' AND t.season=? "
        "ORDER BY t.executed_at DESC",
        (DAYBLADE_PLAYER, season)
    ).fetchall()

    wins = 0
    losses = 0
    total_pnl = 0.0
    today_pnl = 0.0
    today_str = datetime.now().strftime("%Y-%m-%d")
    trades_today = 0

    # DTE bucket tracking
    dte_stats = {
        "0dte": {"wins": 0, "losses": 0, "pnl": 0.0},
        "1dte": {"wins": 0, "losses": 0, "pnl": 0.0},
        "2-3dte": {"wins": 0, "losses": 0, "pnl": 0.0},
        "4-7dte": {"wins": 0, "losses": 0, "pnl": 0.0},
    }

    for s in sells:
        pnl = s["realized_pnl"] or 0
        if pnl == 0 and s["entry_price"]:
            pnl = (s["sell_price"] - s["entry_price"]) * s["qty"]

        total_pnl += pnl
        bucket = _get_dte_bucket(s["expiry_date"] or today_str)

        if pnl > 0:
            wins += 1
            dte_stats[bucket]["wins"] += 1
        else:
            losses += 1
            dte_stats[bucket]["losses"] += 1
        dte_stats[bucket]["pnl"] += pnl

        if s["executed_at"] and today_str in s["executed_at"]:
            today_pnl += pnl
            trades_today += 1

    today_buys = conn.execute(
        "SELECT COUNT(*) as cnt FROM trades WHERE player_id=? AND action LIKE 'BUY%' AND date(executed_at)=? AND season=?",
        (DAYBLADE_PLAYER, today_str, season)
    ).fetchone()
    trades_today += today_buys["cnt"] if today_buys else 0

    conn.close()

    total_closed = wins + losses
    win_rate = (wins / total_closed * 100) if total_closed > 0 else 0.0

    # Compute win rates per DTE bucket
    dte_win_rates = {}
    for bucket, data in dte_stats.items():
        total_b = data["wins"] + data["losses"]
        dte_win_rates[bucket] = {
            "wins": data["wins"],
            "losses": data["losses"],
            "total": total_b,
            "win_rate": round(data["wins"] / total_b * 100, 1) if total_b > 0 else 0,
            "pnl": round(data["pnl"], 2),
        }

    streak = get_win_streak()

    return {
        "total_trades": total["cnt"] if total else 0,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "today_pnl": round(today_pnl, 2),
        "trades_today": trades_today,
        "win_streak": streak["current"],
        "best_streak": streak["best"],
        "dte_stats": dte_win_rates,
        "max_positions": MAX_POSITIONS,
        "gamma_env": _get_gamma_sizing()[1].get("label", "Unknown"),
    }
