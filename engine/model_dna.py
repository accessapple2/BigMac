"""Model DNA — behavioral fingerprint for each AI model."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from collections import Counter
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def get_model_dna(player_id: str) -> dict:
    """Build behavioral fingerprint for an AI model.

    Returns {player_id, name, traits: {
        favorite_sectors, buy_dip_vs_breakout, avg_hold_hours, time_of_day_pattern,
        conviction_distribution, action_distribution, sector_distribution,
        avg_confidence, trade_frequency, personality_summary
    }}.
    """
    conn = _conn()

    player = conn.execute(
        "SELECT display_name FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()
    if not player:
        conn.close()
        return {"error": "Player not found"}

    # All trades
    trades = conn.execute(
        "SELECT symbol, action, qty, price, confidence, reasoning, executed_at "
        "FROM trades WHERE player_id=? ORDER BY executed_at ASC",
        (player_id,)
    ).fetchall()

    # All signals
    signals = conn.execute(
        "SELECT symbol, signal, confidence, created_at "
        "FROM signals WHERE player_id=? ORDER BY created_at ASC",
        (player_id,)
    ).fetchall()

    conn.close()

    if not trades and not signals:
        return {
            "player_id": player_id,
            "name": player["display_name"],
            "traits": _empty_traits(),
        }

    from engine.sector_tracker import SECTOR_MAP

    # --- Sector distribution ---
    sector_counts = Counter()
    symbol_counts = Counter()
    for t in trades:
        if t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
            sector = SECTOR_MAP.get(t["symbol"], "Other")
            sector_counts[sector] += 1
            symbol_counts[t["symbol"]] += 1

    favorite_sectors = [{"sector": s, "count": c} for s, c in sector_counts.most_common(5)]
    favorite_stocks = [{"symbol": s, "count": c} for s, c in symbol_counts.most_common(5)]

    # --- Buy-dip vs breakout tendency ---
    # Analyze if model tends to buy when change_pct < 0 (buy dip) or > 0 (breakout)
    buy_dip_count = 0
    breakout_count = 0
    for t in trades:
        if t["action"] in ("BUY", "BUY_CALL"):
            reasoning = (t["reasoning"] or "").lower()
            # Check if reasoning mentions dip/oversold/support
            if any(w in reasoning for w in ["oversold", "dip", "support", "pullback", "discount"]):
                buy_dip_count += 1
            elif any(w in reasoning for w in ["breakout", "momentum", "surge", "rally", "overbought"]):
                breakout_count += 1

    total_style = buy_dip_count + breakout_count
    if total_style > 0:
        buy_dip_pct = round(buy_dip_count / total_style * 100, 1)
        breakout_pct = round(breakout_count / total_style * 100, 1)
    else:
        buy_dip_pct = 50
        breakout_pct = 50

    if buy_dip_pct > 65:
        style = "Value/Dip Buyer"
    elif breakout_pct > 65:
        style = "Momentum/Breakout Trader"
    else:
        style = "Balanced"

    # --- Average hold time ---
    buys: dict[str, list] = {}
    hold_times = []
    for t in trades:
        sym = t["symbol"]
        if t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
            if sym not in buys:
                buys[sym] = []
            buys[sym].append(t["executed_at"])
        elif t["action"] == "SELL" and sym in buys and buys[sym]:
            try:
                buy_dt = datetime.fromisoformat(buys[sym][0].replace("Z", ""))
                sell_dt = datetime.fromisoformat(t["executed_at"].replace("Z", ""))
                hold_hours = (sell_dt - buy_dt).total_seconds() / 3600
                hold_times.append(hold_hours)
                buys[sym].pop(0)
            except Exception:
                pass

    avg_hold_hours = round(sum(hold_times) / len(hold_times), 1) if hold_times else 0

    # --- Time of day pattern ---
    hour_counts = Counter()
    for t in trades:
        if t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
            try:
                dt = datetime.fromisoformat(t["executed_at"].replace("Z", ""))
                hour_counts[dt.hour] += 1
            except Exception:
                pass

    time_pattern = []
    for h in range(6, 18):  # Market hours roughly
        count = hour_counts.get(h, 0)
        time_pattern.append({"hour": h, "trades": count})

    peak_hour = hour_counts.most_common(1)[0] if hour_counts else (12, 0)
    if peak_hour[0] < 10:
        time_preference = "Early Bird (pre-10 AM)"
    elif peak_hour[0] < 13:
        time_preference = "Mid-Day Trader (10 AM - 1 PM)"
    else:
        time_preference = "Afternoon Trader (after 1 PM)"

    # --- Action distribution ---
    action_counts = Counter(t["action"] for t in trades)
    total_actions = sum(action_counts.values()) or 1
    action_dist = {a: round(c / total_actions * 100, 1) for a, c in action_counts.items()}

    # --- Conviction distribution ---
    confidences = [t["confidence"] for t in trades if t["confidence"] is not None]
    avg_confidence = round(sum(confidences) / len(confidences), 2) if confidences else 0
    high_conv = sum(1 for c in confidences if c >= 0.8)
    med_conv = sum(1 for c in confidences if 0.65 <= c < 0.8)
    low_conv = sum(1 for c in confidences if c < 0.65)
    total_conv = len(confidences) or 1

    conviction_dist = {
        "high_80_plus": round(high_conv / total_conv * 100, 1),
        "medium_65_80": round(med_conv / total_conv * 100, 1),
        "low_under_65": round(low_conv / total_conv * 100, 1),
    }

    # --- Options vs stock preference ---
    options_trades = sum(1 for t in trades if t["action"] in ("BUY_CALL", "BUY_PUT"))
    stock_trades = sum(1 for t in trades if t["action"] == "BUY")
    total_buys = options_trades + stock_trades or 1
    options_pct = round(options_trades / total_buys * 100, 1)

    # --- Trade frequency ---
    if trades:
        try:
            first_trade = datetime.fromisoformat(trades[0]["executed_at"].replace("Z", ""))
            last_trade = datetime.fromisoformat(trades[-1]["executed_at"].replace("Z", ""))
            days_active = max(1, (last_trade - first_trade).days)
            trades_per_day = round(len(trades) / days_active, 1)
        except Exception:
            trades_per_day = 0
    else:
        trades_per_day = 0

    # --- Personality summary ---
    personality = _build_personality_summary(
        style, avg_confidence, options_pct, trades_per_day, avg_hold_hours, time_preference
    )

    return {
        "player_id": player_id,
        "name": player["display_name"],
        "traits": {
            "favorite_sectors": favorite_sectors,
            "favorite_stocks": favorite_stocks,
            "trading_style": style,
            "buy_dip_pct": buy_dip_pct,
            "breakout_pct": breakout_pct,
            "avg_hold_hours": avg_hold_hours,
            "time_preference": time_preference,
            "time_pattern": time_pattern,
            "action_distribution": action_dist,
            "conviction_distribution": conviction_dist,
            "avg_confidence": avg_confidence,
            "options_pct": options_pct,
            "trades_per_day": trades_per_day,
            "total_trades": len(trades),
            "personality_summary": personality,
        },
    }


def _build_personality_summary(style, avg_conf, options_pct, trades_per_day, avg_hold, time_pref):
    parts = []

    if style == "Value/Dip Buyer":
        parts.append("Contrarian dip-buyer who hunts for oversold setups")
    elif style == "Momentum/Breakout Trader":
        parts.append("Momentum-chaser who rides breakouts and surges")
    else:
        parts.append("Balanced trader with no strong directional bias")

    if avg_conf >= 0.80:
        parts.append("with extremely high conviction")
    elif avg_conf >= 0.70:
        parts.append("with solid conviction on trades")
    else:
        parts.append("who takes low-conviction shots")

    if options_pct > 40:
        parts.append("Heavy options user")
    elif options_pct > 10:
        parts.append("Occasional options player")
    else:
        parts.append("Stock-focused")

    if trades_per_day > 5:
        parts.append("Hyperactive trader")
    elif trades_per_day > 2:
        parts.append("Active trader")
    else:
        parts.append("Patient, selective trader")

    if avg_hold < 2:
        parts.append("with scalper-fast exits")
    elif avg_hold < 24:
        parts.append("with day-trade holding periods")
    elif avg_hold < 168:
        parts.append("with multi-day swing holds")
    else:
        parts.append("with position-trade patience")

    return ". ".join(parts) + "."


def _empty_traits():
    return {
        "favorite_sectors": [],
        "favorite_stocks": [],
        "trading_style": "Unknown",
        "buy_dip_pct": 50,
        "breakout_pct": 50,
        "avg_hold_hours": 0,
        "time_preference": "Unknown",
        "time_pattern": [],
        "action_distribution": {},
        "conviction_distribution": {},
        "avg_confidence": 0,
        "options_pct": 0,
        "trades_per_day": 0,
        "total_trades": 0,
        "personality_summary": "Not enough data to build profile.",
    }


def get_all_model_dna() -> dict:
    """Get DNA for all active players."""
    conn = _conn()
    players = conn.execute(
        "SELECT id FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()
    conn.close()

    return {p["id"]: get_model_dna(p["id"]) for p in players}
