"""HM-AM Phase 1 (2026-05-07) → HM-NEXT-WAVE Phase 5 (2026-05-23) Total
Portfolio reader.

Aggregates real-money accounts (Schwab + TradeStation + Webull-historical
+ IBKR + Dilithium Reserve metals) into a unified view.

EXCLUDES Alpaca paper per CLAUDE.md HM-AM doctrine (added 2026-05-12,
HM-CLOSE-GAP W1.1): "Total Portfolio = real-world net worth only.
EXCLUDES Alpaca paper trading book — that's a separate research /
strategy-validation surface and must not co-mingle with real-world
capital reporting." The previous May-12 ship of this module included
Alpaca paper as a source; Phase 5 corrects that.

PUBLIC API
==========
read_total_portfolio() → Captain-spec shape:
    {ts, accounts, total_real_value, by_account, by_symbol,
     metals_pct, errors, note}

get_total_portfolio() → legacy shape (kept for backward compat with
existing kirk_advisory + team_advisor_grok + providers callers):
    {positions, cash_by_account, total_value, total_cash,
     total_invested, last_updated, sources_loaded, sources_failed}

get_portfolio_summary() → lightweight summary (legacy).

build_portfolio_summary_text() → compact text block for LLM advisor
prompts. Used by Kirk + Advisory Team to surface real-money context.

Per-source resilience: each source loader catches its own exceptions
and records failures. A single broken source doesn't silently degrade
the unified view; callers can inspect.

Cache: 30-second TTL, process-local (matches engine/universe.py pattern).

NTFY ollietrades-admin on sync errors via _ntfy_admin (best-effort).

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

    # Spot prices — route through the canonical metals_tracker.get_spot_prices()
    # source so this surface (/api/portfolio/real → unified) reconciles with
    # /api/networth (which prices metals via get_dilithium_portfolio → the same
    # get_spot_prices). HM-METALS-SINGLE-SOURCE 2026-06-02: both endpoints now
    # share the 300s spot cache. The prior independent yfinance 2d-history call
    # drifted ~$1-3 vs networth's Polygon-preferred spot, surfacing as the
    # "two Net Worth cards disagree by ~$2.70" reconciliation bug.
    spot_prices: dict[str, Optional[float]] = {}
    try:
        from engine.metals_tracker import get_spot_prices, LEDGER_TO_SPOT
        _spot = get_spot_prices() or {}
    except Exception:
        _spot, LEDGER_TO_SPOT = {}, {}
    for metal, _, _ in rows:
        key = LEDGER_TO_SPOT.get(metal.lower(), metal.upper())
        entry = _spot.get(key)
        price = entry.get("price") if isinstance(entry, dict) else None
        spot_prices[metal] = (
            float(price) if isinstance(price, (int, float)) and price > 0 else None
        )

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


def _load_webull() -> dict:
    """Webull was liquidated 2026-05-13. Always returns empty positions
    + $0 cash so downstream consumers don't have to special-case it.
    Historical trades remain in the trades table for audit.
    """
    return {
        "positions": [],
        "cash_by_account": {"webull": 0.0},
        "notes": (
            "Liquidated 2026-05-13 (HM-WEBULL-LIQUIDATED). "
            "Historical positions in trades table only."
        ),
    }


def _load_ibkr() -> dict:
    """IBKR placeholder — $0 until a sync path is wired."""
    return {
        "positions": [],
        "cash_by_account": {"ibkr": 0.0},
        "notes": "Placeholder — no sync path wired yet.",
    }


def _ntfy_admin(title: str, message: str, priority: str = "default") -> None:
    """Best-effort NTFY to ollietrades-admin. Never raises."""
    try:
        from engine.alert_channels import _send_ntfy
        _send_ntfy(
            title=title, message=message, priority=priority,
            tags="ollietrades,total-portfolio",
            topic="ollietrades-admin",
        )
    except Exception:
        pass


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

    # Source 3: Webull (HM-WEBULL-LIQUIDATED 2026-05-13 — placeholder)
    try:
        w = _load_webull()
        result["positions"].extend(w["positions"])
        result["cash_by_account"].update(w["cash_by_account"])
        result["sources_loaded"].append("webull")
    except Exception as e:
        result["sources_failed"].append(f"webull: {type(e).__name__}: {e}")

    # Source 4: IBKR (placeholder, no sync path)
    try:
        i = _load_ibkr()
        result["positions"].extend(i["positions"])
        result["cash_by_account"].update(i["cash_by_account"])
        result["sources_loaded"].append("ibkr")
    except Exception as e:
        result["sources_failed"].append(f"ibkr: {type(e).__name__}: {e}")

    # HM-AM PHASE 5 2026-05-23: Alpaca paper is EXCLUDED from total portfolio.
    # Paper book is a separate research surface per CLAUDE.md Two-Book
    # Bridge Policy + HM-AM Scope doctrine. The earlier (2026-05-12)
    # _load_alpaca_paper source has been removed; do NOT re-add it.

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


def read_total_portfolio(force_refresh: bool = False) -> dict:
    """HM-NEXT-WAVE Phase 5 — Captain-spec'd shape:
        {ts, accounts, total_real_value, by_account, by_symbol,
         metals_pct, errors, note}

    Wraps get_total_portfolio() and reshapes for downstream LLM advisor
    consumption. EXCLUDES Alpaca paper per HM-AM doctrine (already
    enforced at the source-loader layer).

    Crash-safe — NTFYs ollietrades-admin on any sync errors found in
    sources_failed.
    """
    tp = get_total_portfolio(force_refresh=force_refresh)
    cash_by_account = tp.get("cash_by_account") or {}
    positions = tp.get("positions") or []

    # Re-bucket positions by account
    by_account_positions: dict[str, list] = {}
    for p in positions:
        by_account_positions.setdefault(p.get("account") or "unknown", []).append(p)

    # Build per-account view
    accounts: dict = {}
    account_labels = {
        "schwab": "Schwab",
        "tradestation": "TradeStation",
        "webull": "Webull (liquidated)",
        "ibkr": "IBKR",
        "metals": "Dilithium Reserve (Metals)",
    }
    for acct_key in ("schwab", "tradestation", "webull", "ibkr", "metals"):
        acct_positions = by_account_positions.get(acct_key) or []
        cash = float(cash_by_account.get(acct_key, 0.0))
        positions_value = sum(
            float(p.get("market_value") or 0) for p in acct_positions
        )
        accounts[acct_key] = {
            "label": account_labels.get(acct_key, acct_key.title()),
            "cash": round(cash, 2),
            "positions": acct_positions,
            "positions_value": round(positions_value, 2),
            "total_value": round(cash + positions_value, 2),
        }

    total_real_value = round(
        sum(a["total_value"] for a in accounts.values()), 2
    )

    by_account = [
        {
            "name": k, "label": v["label"], "value": v["total_value"],
            "pct_of_total": (
                round(v["total_value"] / total_real_value * 100, 2)
                if total_real_value > 0 else 0.0
            ),
        }
        for k, v in accounts.items()
    ]
    by_account.sort(key=lambda x: x["value"], reverse=True)

    # by_symbol aggregation across accounts
    by_symbol: dict[str, dict] = {}
    for p in positions:
        sym = (p.get("symbol") or "").upper()
        if not sym:
            continue
        by_symbol.setdefault(sym, {
            "symbol": sym, "qty": 0.0,
            "market_value": 0.0, "accounts": [],
        })
        by_symbol[sym]["qty"] += float(p.get("qty") or 0)
        by_symbol[sym]["market_value"] += float(
            p.get("market_value") or 0
        )
        acct = p.get("account")
        if acct and acct not in by_symbol[sym]["accounts"]:
            by_symbol[sym]["accounts"].append(acct)

    metals_value = accounts.get("metals", {}).get("total_value", 0)
    metals_pct = (
        round(metals_value / total_real_value * 100, 2)
        if total_real_value > 0 else 0.0
    )

    errors = list(tp.get("sources_failed") or [])
    if errors:
        _ntfy_admin(
            title="⚠ Total Portfolio sync errors",
            message=f"{len(errors)} source error(s): " + "; ".join(errors[:3]),
        )

    return {
        "ts": tp.get("last_updated"),
        "accounts": accounts,
        "total_real_value": total_real_value,
        "by_account": by_account,
        "by_symbol": by_symbol,
        "metals_pct": metals_pct,
        "errors": errors,
        "note": (
            "EXCLUDES Alpaca paper per CLAUDE.md HM-AM doctrine — "
            "paper is a separate research surface."
        ),
    }


def build_portfolio_summary_text(portfolio: dict | None = None,
                                 max_chars: int = 800) -> str:
    """Compact text block for inclusion in LLM advisor prompts. Used by
    Kirk advisory + Advisory Team (Grok / Troi / Worf) so they see
    real-money context, not just Alpaca paper book.

    Pass `portfolio` from a fresh read_total_portfolio() to skip the
    inner refresh; otherwise the function self-loads.
    """
    if portfolio is None:
        try:
            portfolio = read_total_portfolio()
        except Exception as e:
            return f"[Total Portfolio: unavailable ({type(e).__name__})]"
    if not portfolio:
        return "[Total Portfolio: unavailable]"
    lines = []
    lines.append(
        f"REAL-MONEY PORTFOLIO (excludes Alpaca paper): "
        f"${portfolio['total_real_value']:,.0f} "
        f"({portfolio['metals_pct']}% metals)"
    )
    for b in portfolio.get("by_account") or []:
        if b["value"] <= 0:
            continue
        lines.append(
            f"  {b['label']}: ${b['value']:,.0f} ({b['pct_of_total']}%)"
        )
    top_syms = sorted(
        (portfolio.get("by_symbol") or {}).values(),
        key=lambda s: s["market_value"], reverse=True,
    )[:5]
    surfaceable = [s for s in top_syms if s["market_value"] > 0]
    if surfaceable:
        lines.append("Top positions by value:")
        for s in surfaceable:
            lines.append(
                f"  {s['symbol']}: {s['qty']:.2f} @ "
                f"${s['market_value']:,.0f} ({','.join(s['accounts'])})"
            )
    if portfolio.get("errors"):
        lines.append(f"[{len(portfolio['errors'])} sync error(s)]")
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[: max_chars - 3] + "..."
    return text


__all__ = [
    "get_total_portfolio",
    "get_portfolio_summary",
    "read_total_portfolio",
    "build_portfolio_summary_text",
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
