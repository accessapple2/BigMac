"""Webull OpenAPI client — fetches live portfolio from Webull brokerage."""

import os
import time
from dotenv import load_dotenv

load_dotenv(override=True)

_cache = {}
_CACHE_TTL = 60  # seconds


def _get_api():
    app_key = os.environ.get("WEBULL_APP_KEY", "")
    app_secret = os.environ.get("WEBULL_APP_SECRET", "")
    if not app_key or not app_secret:
        return None, None, "Webull API keys not configured"

    try:
        from webullsdkcore.client import ApiClient
        from webullsdktrade.api import API
        from webullsdkcore.common.region import Region
    except ImportError:
        return None, None, "Webull SDK not installed"

    client = ApiClient(app_key, app_secret, Region.US.value)
    api = API(client)
    return api, os.environ.get("WEBULL_ACCOUNT_ID", ""), None


def _resolve_account_id(api, account_number):
    """Resolve account_number (e.g. CVU599Y4) to internal account_id."""
    res = api.account.get_app_subscriptions()
    if res.status_code != 200:
        return None
    subs = res.json()
    match = next((s for s in subs if s["account_number"] == account_number), None)
    return match["account_id"] if match else (subs[0]["account_id"] if subs else None)


def get_portfolio():
    """Fetch live Webull portfolio: balance + positions. Cached for 60s."""
    now = time.time()
    if "portfolio" in _cache and now - _cache["portfolio"]["ts"] < _CACHE_TTL:
        return _cache["portfolio"]["data"]

    api, account_number, error = _get_api()
    if api is None:
        return {"error": error or "Webull unavailable"}

    real_id = _resolve_account_id(api, account_number)
    if not real_id:
        return {"error": "Could not resolve account ID"}

    # Fetch balance
    bal_res = api.account.get_account_balance(real_id, "USD")
    balance = {}
    if bal_res.status_code == 200:
        b = bal_res.json()
        usd = b.get("account_currency_assets", [{}])[0] if b.get("account_currency_assets") else {}
        balance = {
            "total_value": float(b.get("total_market_value", 0)) + float(b.get("total_cash_balance", 0)),
            "market_value": float(b.get("total_market_value", 0)),
            "cash": float(b.get("total_cash_balance", 0)),
            "buying_power": float(usd.get("cash_power", 0)),
            "available_withdrawal": float(usd.get("available_withdrawal", 0)),
        }

    # Fetch all positions (paginate if needed)
    holdings = []
    last_instrument_id = None
    for _ in range(10):  # max 10 pages
        if last_instrument_id:
            pos_res = api.account.get_account_position(real_id, page_size=100, last_instrument_id=last_instrument_id)
        else:
            pos_res = api.account.get_account_position(real_id, page_size=100)
        if pos_res.status_code != 200:
            break
        data = pos_res.json()
        for h in data.get("holdings", []):
            holdings.append({
                "symbol": h.get("symbol", ""),
                "qty": float(h.get("qty", 0)),
                "avg_cost": float(h.get("unit_cost", 0)),
                "last_price": float(h.get("last_price", 0)),
                "market_value": float(h.get("market_value", 0)),
                "total_cost": float(h.get("total_cost", 0)),
                "unrealized_pnl": float(h.get("unrealized_profit_loss", 0)),
                "pnl_pct": float(h.get("unrealized_profit_loss_rate", 0)) * 100,
                "weight": float(h.get("holding_proportion", 0)) * 100,
                "type": h.get("instrument_type", "STOCK"),
            })
        if not data.get("has_next"):
            break
        if holdings:
            last_instrument_id = data["holdings"][-1].get("instrument_id")

    result = {
        "account_number": account_number,
        "balance": balance,
        "positions": holdings,
        "position_count": len(holdings),
        "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    _cache["portfolio"] = {"data": result, "ts": now}
    return result


def sync_positions_to_db():
    """Sync live Webull positions into the positions table for steve-webull.

    Full mirror: DELETE all steve-webull positions, INSERT fresh from Webull.
    This guarantees the DB exactly matches Webull — no stale/merged state.

    SAFETY: Never deletes trade history. Only updates current positions table.
    Returns dict with sync summary.
    """
    import sqlite3
    from datetime import datetime
    from rich.console import Console
    console = Console()

    portfolio = get_portfolio()
    if "error" in portfolio:
        console.log(f"[red]Webull sync failed: {portfolio['error']}")
        return {"error": portfolio["error"]}

    positions = portfolio.get("positions", [])
    balance = portfolio.get("balance", {})
    total_value = balance.get("total_value", 0)

    now = datetime.now().isoformat()

    conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row

    # Full replace: delete all current positions and re-insert from Webull
    conn.execute("DELETE FROM positions WHERE player_id='steve-webull'")

    inserted = []
    for p in positions:
        sym = p.get("symbol")
        if not sym:
            continue
        qty = p.get("qty", 0)
        avg_cost = p.get("avg_cost", 0)
        conn.execute(
            "INSERT INTO positions (player_id, symbol, qty, avg_price, asset_type, opened_at) "
            "VALUES ('steve-webull', ?, ?, ?, 'stock', ?)",
            (sym, qty, avg_cost, now)
        )
        inserted.append(sym)

    # Sync total value for leaderboard
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_value', ?)",
        (str(total_value),)
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_at', ?)",
        (now,)
    )

    conn.commit()
    conn.close()

    summary = {
        "ok": True,
        "total_value": total_value,
        "positions": len(inserted),
        "inserted": inserted,
        "synced_at": now,
    }

    console.log(f"[green]Webull sync (full replace): {len(inserted)} positions — {', '.join(inserted)} (${total_value:,.2f})")

    return summary
