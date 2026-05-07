"""HM-AM Phase 1 (2026-05-07): Total Portfolio reader.

Aggregates Schwab + Dilithium Reserve (metals) + Alpaca paper into a unified
view. Read-only data layer — Phase 1 ships ONLY this module + standalone
smoke. No consumer integration (Kirk, Advisory Team, dalio-metals
realign) — those are Phase 2/3/4 deferred to fresh sessions.

Per-source resilience: each source loader catches its own exceptions and
records failures in `sources_failed`. A single broken source doesn't
silently degrade the unified view; callers can inspect `sources_failed`
and decide.

Cache: 30-second TTL, process-local (matches engine/universe.py pattern).

Usage:
    from engine.total_portfolio import get_total_portfolio, get_portfolio_summary
    tp = get_total_portfolio()
    print(tp["total_value"], len(tp["positions"]))

Standalone smoke:
    venv/bin/python3 engine/total_portfolio.py
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TypedDict

# Ensure repo root on sys.path when run standalone (same pattern as
# engine/universe_refresh.py — without this, `from engine...` imports
# fail because Python's auto-added path is the script's directory).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_DB_PATH = _REPO_ROOT / "data" / "trader.db"
_REAL_HOLDINGS_PATH = _REPO_ROOT / "data" / "real_holdings.json"

# Cache config — process-local 30s TTL.
CACHE_TTL_SEC = 30
_cache: dict = {"data": None, "ts": 0.0}

# Spot-price symbols for metals market-value calc.
_METAL_YAHOO_SYMBOL = {"gold": "GC=F", "silver": "SI=F"}

# Regex to parse market_value from Schwab note text:
#   "market_value=$3625.40, gain=$+22.90 (+0.64%) [from snapshot ...]"
_MARKET_VALUE_RE = re.compile(r"market_value=\$([+-]?\d+\.?\d*)")


# ─── Types ──────────────────────────────────────────────────────────────────


class Position(TypedDict, total=False):
    symbol: str
    account: str        # "schwab" | "tradestation" | "metals" | "alpaca_paper"
    qty: float
    avg_cost: Optional[float]
    market_value: Optional[float]
    asset_type: str     # "equity" | "etf" | "metal"
    notes: str


class TotalPortfolio(TypedDict, total=False):
    positions: list[Position]
    cash_by_account: dict[str, float]
    total_value: float
    total_cash: float
    total_invested: float
    last_updated: str
    sources_loaded: list[str]
    sources_failed: list[str]


# ─── Helpers ────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_market_value(notes: str) -> Optional[float]:
    """Extract market_value from Schwab note text. Returns None if not present."""
    if not notes:
        return None
    m = _MARKET_VALUE_RE.search(notes)
    if not m:
        return None
    try:
        return float(m.group(1))
    except (ValueError, TypeError):
        return None


# ─── Source loaders ─────────────────────────────────────────────────────────


def _load_schwab() -> dict:
    """Read data/real_holdings.json. Returns {positions, cash}.

    Aggregates across all is_active accounts (currently 'schwab' and
    'tradestation'). Each account's cash_balance is summed into the
    return value's `cash_by_account` keyed by account label.
    """
    if not _REAL_HOLDINGS_PATH.exists():
        raise FileNotFoundError(f"{_REAL_HOLDINGS_PATH} not present")
    with _REAL_HOLDINGS_PATH.open() as f:
        data = json.load(f)
    positions: list[Position] = []
    cash_by_account: dict[str, float] = {}
    for acct_key, acct in (data.get("accounts") or {}).items():
        if not acct.get("is_active"):
            continue
        cash_by_account[acct_key] = float(acct.get("cash_balance") or 0)
        for p in acct.get("positions") or []:
            positions.append({
                "symbol": (p.get("symbol") or "").upper(),
                "account": acct_key,
                "qty": float(p.get("qty") or 0),
                "avg_cost": float(p["avg_cost"]) if p.get("avg_cost") else None,
                "market_value": _parse_market_value(p.get("notes") or ""),
                "asset_type": "equity",  # Schwab holdings — equity/ETF mix; refine in Phase 2 if needed
                "notes": p.get("notes") or "",
            })
    return {"positions": positions, "cash_by_account": cash_by_account}


def _load_metals() -> dict:
    """Aggregate metals_ledger by metal, fetch spot prices, compute market values.

    Returns {positions, cash_by_account: {"metals": 0.0}}. No cash component
    (metals are physical holdings, no associated cash account).
    """
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    try:
        rows = conn.execute(
            "SELECT metal, SUM(qty_oz) AS total_oz, SUM(total_cost) AS total_cost "
            "FROM metals_ledger GROUP BY metal"
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"positions": [], "cash_by_account": {"metals": 0.0}}

    # Spot prices via yfinance (best-effort; fall back to NULL market_value).
    spot_prices: dict[str, Optional[float]] = {}
    try:
        import yfinance as yf
        for metal, _, _ in rows:
            sym = _METAL_YAHOO_SYMBOL.get(metal.lower())
            if sym is None:
                spot_prices[metal] = None
                continue
            try:
                hist = yf.Ticker(sym).history(period="2d")
                if not hist.empty:
                    spot_prices[metal] = float(hist["Close"].iloc[-1])
                else:
                    spot_prices[metal] = None
            except Exception:
                spot_prices[metal] = None
    except ImportError:
        for metal, _, _ in rows:
            spot_prices[metal] = None

    positions: list[Position] = []
    for metal, total_oz, total_cost in rows:
        spot = spot_prices.get(metal)
        market_value = (spot * total_oz) if spot is not None else None
        avg_cost_per_oz = (total_cost / total_oz) if total_oz else None
        positions.append({
            "symbol": metal.upper(),         # "GOLD" / "SILVER"
            "account": "metals",
            "qty": float(total_oz),
            "avg_cost": avg_cost_per_oz,
            "market_value": market_value,
            "asset_type": "metal",
            "notes": f"spot=${spot:.2f}/oz" if spot is not None else "spot price unavailable",
        })
    return {"positions": positions, "cash_by_account": {"metals": 0.0}}


def _load_alpaca_paper() -> dict:
    """Load Alpaca paper account positions + cash.

    Uses the existing AlpacaBridge instance pattern (engine/alpaca_bridge.py).
    Returns {positions, cash_by_account: {"alpaca_paper": <cash>}}.
    """
    from engine.alpaca_bridge import AlpacaBridge  # local import — bridge can be slow to construct
    bridge = AlpacaBridge()
    # AlpacaBridge.status() returns {'connected': bool, 'cash': float, ...}
    # (the method is named status(), not account() — wraps client.get_account())
    acc = bridge.status()
    if not acc.get("connected"):
        raise RuntimeError(f"Alpaca not connected: {acc.get('reason') or 'unknown'}")
    cash = float(acc.get("cash") or 0)
    raw_positions = bridge.positions() or []
    positions: list[Position] = []
    for p in raw_positions:
        if "error" in p:
            # Bridge returned a single-element error sentinel; skip but bubble
            raise RuntimeError(f"Alpaca positions error: {p['error']}")
        sym = (p.get("symbol") or "").upper()
        # Detect option vs equity by symbol shape (OCC options ≥15 chars).
        asset_type = "option" if len(sym) >= 15 else "equity"
        positions.append({
            "symbol": sym,
            "account": "alpaca_paper",
            "qty": float(p.get("qty") or 0),
            "avg_cost": float(p["avg_entry"]) if p.get("avg_entry") else None,
            "market_value": float(p["market_value"]) if p.get("market_value") else None,
            "asset_type": asset_type,
            "notes": f"unrealized_pl={p.get('unrealized_pl', 0):+.2f} ({p.get('unrealized_plpc', 0):+.2f}%)",
        })
    return {"positions": positions, "cash_by_account": {"alpaca_paper": cash}}


# ─── Public API ─────────────────────────────────────────────────────────────


def get_total_portfolio(force_refresh: bool = False) -> TotalPortfolio:
    """Return unified portfolio view across all 3 sources.

    Per-source resilience: each source loaded independently; failures recorded
    in `sources_failed` rather than raising. Callers can inspect.

    30s TTL cache — process-local. Pass force_refresh=True to bypass.
    """
    now = time.time()
    if not force_refresh and _cache["data"] is not None and (now - _cache["ts"] < CACHE_TTL_SEC):
        return _cache["data"]

    result: TotalPortfolio = {
        "positions": [],
        "cash_by_account": {},
        "total_value": 0.0,
        "total_cash": 0.0,
        "total_invested": 0.0,
        "last_updated": "",
        "sources_loaded": [],
        "sources_failed": [],
    }

    # Source 1: Schwab + TradeStation (real_holdings.json)
    try:
        s = _load_schwab()
        result["positions"].extend(s["positions"])
        result["cash_by_account"].update(s["cash_by_account"])
        result["sources_loaded"].append("schwab")
    except Exception as e:
        result["sources_failed"].append(f"schwab: {type(e).__name__}: {e}")

    # Source 2: Dilithium Reserve (metals_ledger)
    try:
        m = _load_metals()
        result["positions"].extend(m["positions"])
        result["cash_by_account"].update(m["cash_by_account"])
        result["sources_loaded"].append("metals")
    except Exception as e:
        result["sources_failed"].append(f"metals: {type(e).__name__}: {e}")

    # Source 3: Alpaca paper
    try:
        a = _load_alpaca_paper()
        result["positions"].extend(a["positions"])
        result["cash_by_account"].update(a["cash_by_account"])
        result["sources_loaded"].append("alpaca_paper")
    except Exception as e:
        result["sources_failed"].append(f"alpaca_paper: {type(e).__name__}: {e}")

    # Totals
    result["total_invested"] = sum(
        float(p.get("market_value") or 0) for p in result["positions"]
    )
    result["total_cash"] = sum(result["cash_by_account"].values())
    result["total_value"] = result["total_invested"] + result["total_cash"]
    result["last_updated"] = _now_iso()

    _cache["data"] = result
    _cache["ts"] = now
    return result


def get_portfolio_summary() -> dict:
    """Lightweight summary without full position list.

    Cheap to call from dashboard endpoints — uses cached total view.
    """
    tp = get_total_portfolio()
    return {
        "total_value": tp["total_value"],
        "total_cash": tp["total_cash"],
        "total_invested": tp["total_invested"],
        "position_count": len(tp["positions"]),
        "sources_loaded": tp["sources_loaded"],
        "sources_failed": tp["sources_failed"],
        "last_updated": tp["last_updated"],
    }


__all__ = [
    "get_total_portfolio",
    "get_portfolio_summary",
    "Position",
    "TotalPortfolio",
    "CACHE_TTL_SEC",
]


# ─── Standalone smoke test ──────────────────────────────────────────────────


if __name__ == "__main__":
    tp = get_total_portfolio(force_refresh=True)
    by_source: dict[str, int] = {}
    for p in tp["positions"]:
        by_source[p["account"]] = by_source.get(p["account"], 0) + 1
    summary = {
        "total_value": round(tp["total_value"], 2),
        "total_cash": round(tp["total_cash"], 2),
        "total_invested": round(tp["total_invested"], 2),
        "position_count": len(tp["positions"]),
        "by_source": by_source,
        "cash_by_account": {k: round(v, 2) for k, v in tp["cash_by_account"].items()},
        "sources_loaded": tp["sources_loaded"],
        "sources_failed": tp["sources_failed"],
        "last_updated": tp["last_updated"],
    }
    print(json.dumps(summary, indent=2))
