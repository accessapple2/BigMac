"""Webull OpenAPI client — fetches live portfolio from Webull brokerage.

Uses direct HTTP requests with HMAC-SHA1 signing, bypassing the vendored
urllib3/requests in webull-python-sdk-core which are broken on Python 3.12.
"""

import hashlib
import hmac
import json
import os
import socket
import time
import uuid
from base64 import b64encode
from datetime import datetime
from urllib.parse import quote, urlencode

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

_WEBULL_HOST = "api.webull.com"
_WEBULL_BASE = f"https://{_WEBULL_HOST}"

_cache = {}
_CACHE_TTL = 60  # seconds


# ---------------------------------------------------------------------------
# Signing helpers (ported from webullsdkcore without the broken vendored libs)
# ---------------------------------------------------------------------------

def _get_uuid() -> str:
    name = socket.gethostname() + str(uuid.uuid1())
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def _iso8601_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _hmac_sha1_b64(source: str, secret: str) -> str:
    key = (secret + "&").encode()
    h = hmac.new(key, source.encode(), hashlib.sha1)
    return b64encode(h.digest()).decode().strip()


def _build_signed_headers(uri: str, query_params: dict, app_key: str, app_secret: str) -> dict:
    """Build signed request headers per Webull OpenAPI HMAC-SHA1 scheme."""
    nonce = _get_uuid()
    timestamp = _iso8601_now()

    sign_headers = {
        "x-app-key": app_key,
        "x-timestamp": timestamp,
        "x-signature-version": "1.0",
        "x-signature-algorithm": "HMAC-SHA1",
        "x-signature-nonce": nonce,
    }

    # sign_params = lowercased sign_headers + query_params
    sign_params = {k.lower(): v for k, v in sign_headers.items()}
    sign_params["host"] = _WEBULL_HOST
    for k, v in query_params.items():
        cv = sign_params.get(k)
        sign_params[k] = f"{cv}&{v}" if cv is not None else str(v)

    # Sorted key=value pairs joined by &, then URL-encode the whole string
    sorted_kv = "&".join(f"{k}={v}" for k, v in sorted(sign_params.items()))
    string_to_sign = quote(f"{uri}&{sorted_kv}", safe="")

    signature = _hmac_sha1_b64(string_to_sign, app_secret)
    sign_headers["x-signature"] = signature
    sign_headers["Content-Type"] = "application/json"
    return sign_headers


def _webull_get(path: str, params: dict, app_key: str, app_secret: str) -> dict:
    headers = _build_signed_headers(path, params, app_key, app_secret)
    url = f"{_WEBULL_BASE}{path}"
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Portfolio API
# ---------------------------------------------------------------------------

def _get_creds():
    app_key = os.environ.get("WEBULL_APP_KEY", "")
    app_secret = os.environ.get("WEBULL_APP_SECRET", "")
    account_number = os.environ.get("WEBULL_ACCOUNT_ID", "")
    if not app_key or not app_secret:
        return None, None, None, "Webull API keys not configured"
    return app_key, app_secret, account_number, None


def _resolve_account_id(app_key: str, app_secret: str, account_number: str):
    """Resolve account_number (e.g. CVU599Y4) to internal account_id."""
    data = _webull_get("/app/subscriptions/list", {}, app_key, app_secret)
    subs = data if isinstance(data, list) else data.get("data", data.get("list", []))
    match = next((s for s in subs if s.get("account_number") == account_number), None)
    if match:
        return match.get("account_id")
    return subs[0].get("account_id") if subs else None


def get_portfolio():
    """Fetch live Webull portfolio: balance + positions. Cached for 60s."""
    now = time.time()
    if "portfolio" in _cache and now - _cache["portfolio"]["ts"] < _CACHE_TTL:
        return _cache["portfolio"]["data"]

    app_key, app_secret, account_number, error = _get_creds()
    if app_key is None:
        return {"error": error}

    try:
        real_id = _resolve_account_id(app_key, app_secret, account_number)
    except Exception as e:
        return {"error": f"Could not resolve account ID: {e}"}

    if not real_id:
        return {"error": "Could not resolve account ID"}

    # Fetch balance
    balance = {}
    try:
        b = _webull_get("/account/balance", {
            "account_id": real_id,
            "total_asset_currency": "USD",
        }, app_key, app_secret)
        usd = b.get("account_currency_assets", [{}])[0] if b.get("account_currency_assets") else {}
        balance = {
            "total_value": float(b.get("total_market_value", 0)) + float(b.get("total_cash_balance", 0)),
            "market_value": float(b.get("total_market_value", 0)),
            "cash": float(b.get("total_cash_balance", 0)),
            "buying_power": float(usd.get("cash_power", 0)),
            "available_withdrawal": float(usd.get("available_withdrawal", 0)),
        }
    except Exception as e:
        balance = {"error": str(e)}

    # Fetch all positions (paginate)
    holdings = []
    last_instrument_id = None
    for _ in range(10):
        params = {"account_id": real_id, "page_size": 100}
        if last_instrument_id:
            params["last_instrument_id"] = last_instrument_id
        try:
            data = _webull_get("/account/positions", params, app_key, app_secret)
        except Exception:
            break
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


_STARTING_VALUE = 7021.81  # Captain Kirk's season starting capital


def get_portfolio_for_dashboard() -> dict:
    """Return live Webull portfolio in the format dashboard JS expects.

    Maps field names from the Webull API to what fetchWebullPortfolio() renders:
      avg_cost      → avg_price
      last_price    → current_price
      pnl_pct       → unrealized_pnl_pct
      type (STOCK)  → asset_type (stock)
    Also computes total_cost_basis, total_unrealized_pnl, return_pct, and win_rate
    from DB trade history.
    """
    import sqlite3

    data = get_portfolio()
    if "error" in data:
        return data

    balance = data.get("balance", {})
    raw_positions = data.get("positions", [])

    total_cost_basis = sum(p.get("total_cost", 0) for p in raw_positions)
    total_unrealized_pnl = sum(p.get("unrealized_pnl", 0) for p in raw_positions)
    total_value = balance.get("total_value", 0)
    return_pct = round((total_value - _STARTING_VALUE) / _STARTING_VALUE * 100, 2) if _STARTING_VALUE else 0.0

    positions = []
    for p in raw_positions:
        raw_type = (p.get("type") or "STOCK").upper()
        asset_type = "option" if raw_type in ("OPTIONS", "OPTION") else "stock"
        positions.append({
            "symbol": p.get("symbol", ""),
            "qty": p.get("qty", 0),
            "avg_price": p.get("avg_cost", 0),
            "current_price": p.get("last_price", 0),
            "market_value": p.get("market_value", 0),
            "unrealized_pnl": p.get("unrealized_pnl", 0),
            "unrealized_pnl_pct": p.get("pnl_pct", 0),
            "day_change_pct": 0.0,  # not provided by Webull positions API
            "asset_type": asset_type,
            "market": "webull",
        })

    # Win rate from closed trade history
    win_rate, win_count, loss_count = 0.0, 0, 0
    try:
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT CASE WHEN realized_pnl IS NOT NULL THEN realized_pnl "
            "ELSE (price - (SELECT b.price FROM trades b WHERE b.player_id='steve-webull' "
            "AND b.action='BUY' AND b.symbol=t.symbol AND b.executed_at<=t.executed_at "
            "ORDER BY b.executed_at DESC LIMIT 1)) * qty END AS pnl "
            "FROM trades t WHERE player_id='steve-webull' AND action='SELL'"
        ).fetchall()
        conn.close()
        if rows:
            win_count = sum(1 for r in rows if r["pnl"] is not None and r["pnl"] > 0)
            loss_count = len(rows) - win_count
            win_rate = round(win_count / len(rows) * 100, 1)
    except Exception:
        pass

    return {
        "cash": balance.get("cash", 0),
        "buying_power": balance.get("buying_power", 0),
        "total_value": total_value,
        "total_cost_basis": round(total_cost_basis, 2),
        "total_unrealized_pnl": round(total_unrealized_pnl, 2),
        "return_pct": return_pct,
        "total_day_pnl": 0.0,
        "total_day_pnl_pct": 0.0,
        "starting_value": _STARTING_VALUE,
        "win_rate": win_rate,
        "win_count": win_count,
        "loss_count": loss_count,
        "positions": positions,
        "position_count": len(positions),
        "last_synced_label": data.get("fetched_at", ""),
        "account_number": data.get("account_number", ""),
        "source": "webull_live",
    }


def sync_positions_to_db():
    """Sync live Webull positions into the positions table for steve-webull.

    Full mirror: DELETE all steve-webull positions, INSERT fresh from Webull.
    This guarantees the DB exactly matches Webull — no stale/merged state.

    SAFETY: Never deletes trade history. Only updates current positions table.
    Returns dict with sync summary.
    """
    import sqlite3
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

    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_value', ?)",
        (str(total_value),)
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('webull_synced_at', ?)",
        (now,)
    )

    # Sync Webull cash to ai_players so kirk_advisory reads the right value
    webull_cash = balance.get("cash", 0)
    if webull_cash > 0:
        conn.execute(
            "UPDATE ai_players SET cash=? WHERE id='steve-webull'",
            (webull_cash,)
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
