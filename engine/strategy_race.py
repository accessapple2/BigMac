"""Strategy Race — compare AI swing strategy vs buy-and-hold SPY."""
from __future__ import annotations
import sqlite3
import json
import os
from engine.market_data import _is_yf_limited, _set_yf_limited
from datetime import datetime, timedelta
from rich.console import Console

console = Console()
DB = "data/trader.db"
RACE_FILE = "data/strategy_race.json"
STARTING_CASH = 10000.0


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _load_race_data() -> dict:
    if os.path.exists(RACE_FILE):
        try:
            with open(RACE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"spy_start_price": None, "start_date": None, "history": []}


def _save_race_data(data: dict):
    os.makedirs(os.path.dirname(RACE_FILE), exist_ok=True)
    with open(RACE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def update_strategy_race():
    """Called daily to record both AI and SPY equity curves."""
    race = _load_race_data()

    # Get SPY current price
    if _is_yf_limited():
        return
    try:
        spy = yf.Ticker("SPY")
        spy_hist = spy.history(period="5d", interval="1d")
        if spy_hist.empty:
            return
        spy_price = float(spy_hist["Close"].iloc[-1])
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        console.log(f"[red]Strategy race SPY error: {e}")
        return

    # Initialize start price if first run
    if race["spy_start_price"] is None:
        race["spy_start_price"] = spy_price
        race["start_date"] = datetime.now().strftime("%Y-%m-%d")

    # Calculate SPY buy-and-hold value
    spy_shares = STARTING_CASH / race["spy_start_price"]
    spy_value = round(spy_shares * spy_price, 2)

    # Get AI portfolio values (sum of all active players, averaged)
    conn = _conn()
    players = conn.execute(
        "SELECT id, cash FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()

    # Get latest portfolio snapshots
    ai_values = []
    for p in players:
        # Get positions value
        positions = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id=?",
            (p["id"],)
        ).fetchall()
        pos_value = sum(pos["qty"] * pos["avg_price"] for pos in positions)
        total = p["cash"] + pos_value
        ai_values.append(total)

    conn.close()

    if ai_values:
        ai_avg_value = round(sum(ai_values) / len(ai_values), 2)
        ai_best_value = round(max(ai_values), 2)
    else:
        ai_avg_value = STARTING_CASH
        ai_best_value = STARTING_CASH

    today = datetime.now().strftime("%Y-%m-%d")

    # Don't duplicate same-day entries
    if race["history"] and race["history"][-1]["date"] == today:
        race["history"][-1] = {
            "date": today,
            "spy_value": spy_value,
            "ai_avg_value": ai_avg_value,
            "ai_best_value": ai_best_value,
            "spy_price": round(spy_price, 2),
        }
    else:
        race["history"].append({
            "date": today,
            "spy_value": spy_value,
            "ai_avg_value": ai_avg_value,
            "ai_best_value": ai_best_value,
            "spy_price": round(spy_price, 2),
        })

    # Keep last 365 days
    race["history"] = race["history"][-365:]
    _save_race_data(race)
    return race


def get_strategy_race() -> dict:
    """Get strategy race data for the dashboard."""
    race = _load_race_data()

    if not race["history"]:
        # Generate initial data point
        race = update_strategy_race()
        if not race:
            return {
                "spy_start_price": None,
                "start_date": None,
                "history": [],
                "stats": {},
            }

    history = race.get("history", [])
    if not history:
        return {
            "spy_start_price": race.get("spy_start_price"),
            "start_date": race.get("start_date"),
            "history": [],
            "stats": {},
        }

    latest = history[-1]
    spy_return = round((latest["spy_value"] / STARTING_CASH - 1) * 100, 2)
    ai_return = round((latest["ai_avg_value"] / STARTING_CASH - 1) * 100, 2)
    alpha = round(ai_return - spy_return, 2)

    # Calculate max drawdowns
    spy_peak = 0
    spy_max_dd = 0
    ai_peak = 0
    ai_max_dd = 0
    for h in history:
        if h["spy_value"] > spy_peak:
            spy_peak = h["spy_value"]
        if spy_peak > 0:
            dd = (spy_peak - h["spy_value"]) / spy_peak * 100
            spy_max_dd = max(spy_max_dd, dd)
        if h["ai_avg_value"] > ai_peak:
            ai_peak = h["ai_avg_value"]
        if ai_peak > 0:
            dd = (ai_peak - h["ai_avg_value"]) / ai_peak * 100
            ai_max_dd = max(ai_max_dd, dd)

    return {
        "spy_start_price": race.get("spy_start_price"),
        "start_date": race.get("start_date"),
        "history": history,
        "stats": {
            "spy_return_pct": spy_return,
            "ai_return_pct": ai_return,
            "alpha": alpha,
            "spy_value": latest["spy_value"],
            "ai_avg_value": latest["ai_avg_value"],
            "ai_best_value": latest.get("ai_best_value", latest["ai_avg_value"]),
            "spy_max_drawdown": round(spy_max_dd, 2),
            "ai_max_drawdown": round(ai_max_dd, 2),
            "days_tracked": len(history),
            "winning": "AI" if ai_return > spy_return else "SPY",
        },
    }
