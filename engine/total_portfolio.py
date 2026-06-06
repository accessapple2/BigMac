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

# HM-AM TOTAL PORTFOLIO UNIFICATION (2026-06-06): only these accounts count
# toward net worth AND display. Others (tradestation, webull, ibkr) are
# SUSPENDED — their loaders STILL RUN (data/plumbing stays fresh) but they are
# filtered out of the total and the returned accounts. Flip an account back on
# by adding it to this list — the single toggle point. Alpaca paper / SIM are
# NEVER here (Two-Book Policy — paper money is never summed into net worth).
INCLUDED_ACCOUNTS = ["schwab", "metals"]

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


def _ensure_networth_history() -> None:
    """HM-AM UNIFICATION: daily net-worth snapshot table — baseline for daily
    change (close-to-close). One row per day, last-write-wins."""
    conn = sqlite3.connect(_DB_PATH, timeout=15)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS networth_history (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date  TEXT NOT NULL,
                net_worth      REAL NOT NULL,
                schwab_value   REAL,
                metals_value   REAL,
                gold_value     REAL,
                silver_value   REAL,
                breakdown_json TEXT,
                created_at     TEXT DEFAULT (datetime('now')),
                UNIQUE(snapshot_date)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


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


def _coerce_market_value(p: dict) -> Optional[float]:
    """Resolve a position's market_value.

    HM-SCHWAB-API-READ: the live_api sync (sync_schwab_live.py) writes an
    explicit ``market_value`` field. The legacy CSV path embeds it as "$X" in
    the ``notes`` string instead. Prefer the explicit field; fall back to
    notes-parsing only when the field is absent/null so BOTH paths report MV.
    """
    mv = p.get("market_value")
    if mv is not None:
        try:
            return float(mv)
        except (ValueError, TypeError):
            pass
    return _parse_market_value(p.get("notes") or "")


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
                "market_value": _coerce_market_value(p),
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

    # HM-AM UNIFICATION 2026-06-06: apply INCLUDED_ACCOUNTS filter POST-load.
    # Loaders ran above (plumbing/data stays fresh); here we drop suspended
    # accounts from positions + cash so they leave the total AND the display.
    # Filter is key-level (not per-loader) because _load_schwab bundles both
    # schwab + tradestation. `sources_loaded` keeps the full ran-list for
    # observability; `sources_excluded` records what was filtered out.
    result["sources_excluded"] = [
        a for a in result["cash_by_account"]
        if a not in INCLUDED_ACCOUNTS
    ] + sorted({
        (p.get("account") or "unknown") for p in result["positions"]
        if (p.get("account") or "unknown") not in INCLUDED_ACCOUNTS
    })
    result["positions"] = [
        p for p in result["positions"]
        if (p.get("account") or "unknown") in INCLUDED_ACCOUNTS
    ]
    result["cash_by_account"] = {
        k: v for k, v in result["cash_by_account"].items()
        if k in INCLUDED_ACCOUNTS
    }

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


def _az_today() -> str:
    """AZ date 'YYYY-MM-DD' for snapshot keys."""
    try:
        from engine.market_calendar import az_now
        return az_now().strftime("%Y-%m-%d")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _prior_snapshot(before_date: str) -> Optional[dict]:
    """Most recent networth_history row strictly before `before_date` (the daily
    baseline). Returns None if none exists (first-ever snapshot → daily = '—')."""
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        try:
            row = conn.execute(
                "SELECT snapshot_date, net_worth, schwab_value, metals_value "
                "FROM networth_history WHERE snapshot_date < ? "
                "ORDER BY snapshot_date DESC LIMIT 1",
                (before_date,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        return {"snapshot_date": row[0], "net_worth": row[1],
                "schwab_value": row[2], "metals_value": row[3]}
    except Exception:
        return None


def get_unified_networth(force_refresh: bool = False) -> dict:
    """HM-AM UNIFICATION — the SINGLE canonical real-net-worth view.

    Schwab + Metals only (INCLUDED_ACCOUNTS). Per-bucket + per-metal gain/loss:
    all-time (vs cost basis) and daily (per-metal live spot change_pct;
    total/bucket close-to-close vs the prior networth_history snapshot).
    Daily total is None until a prior snapshot exists (first day → '—').
    """
    tp = get_total_portfolio(force_refresh=force_refresh)
    positions = tp.get("positions") or []
    cash_by_account = tp.get("cash_by_account") or {}

    # Live spot (per-metal daily change_pct) — 300s cached, cheap.
    spot: dict = {}
    LEDGER_TO_SPOT: dict = {}
    try:
        from engine.metals_tracker import get_spot_prices, LEDGER_TO_SPOT as _l2s
        spot = get_spot_prices() or {}
        LEDGER_TO_SPOT = _l2s or {}
    except Exception as e:
        _ntfy_admin("Net worth: spot fetch failed", f"{type(e).__name__}: {e}")

    def _pct(num: float, den: float) -> float:
        return round(num / den * 100, 2) if den else 0.0

    # ── Schwab bucket (cash + any equities; all-time ready for when it holds them)
    schwab_cash = float(cash_by_account.get("schwab", 0.0) or 0.0)
    schwab_positions = [p for p in positions if (p.get("account") == "schwab")]
    schwab_equity = sum(float(p.get("market_value") or 0) for p in schwab_positions)
    schwab_value = round(schwab_cash + schwab_equity, 2)
    schwab_all_time = 0.0
    schwab_cost = 0.0
    for p in schwab_positions:
        mv = p.get("market_value")
        avg = p.get("avg_cost")
        qty = float(p.get("qty") or 0)
        if mv is not None and avg:
            cost = float(avg) * qty
            schwab_cost += cost
            schwab_all_time += float(mv) - cost
    schwab_bucket = {
        "value": schwab_value,
        "daily_dollar": 0.0, "daily_pct": 0.0,            # cash has no daily move
        "all_time_dollar": round(schwab_all_time, 2),
        "all_time_pct": _pct(schwab_all_time, schwab_cost),
        "type": "cash" if not schwab_positions else "mixed",
    }

    # ── Metals bucket — per metal
    metals_positions = [p for p in positions if (p.get("account") == "metals")]
    metal_detail = []
    metals_value = metals_basis = metals_daily_dollar = 0.0
    gold_value = silver_value = 0.0
    for p in metals_positions:
        metal = (p.get("symbol") or "").lower()           # GOLD/SILVER -> gold/silver
        qty = float(p.get("qty") or 0)
        mv = float(p.get("market_value") or 0)
        avg = float(p.get("avg_cost") or 0)
        cost_basis = round(avg * qty, 2)
        at_dollar = round(mv - cost_basis, 2)
        # daily change_pct from live spot
        key = LEDGER_TO_SPOT.get(metal, metal.upper())
        entry = spot.get(key)
        chg = entry.get("change_pct") if isinstance(entry, dict) else None
        spot_price = entry.get("price") if isinstance(entry, dict) else None
        daily_pct = float(chg) if isinstance(chg, (int, float)) else None
        daily_dollar = None
        if daily_pct is not None and daily_pct != -100:
            daily_dollar = round(mv - mv / (1 + daily_pct / 100), 2)
            metals_daily_dollar += daily_dollar
        metals_value += mv
        metals_basis += cost_basis
        if metal == "gold":
            gold_value = mv
        elif metal == "silver":
            silver_value = mv
        metal_detail.append({
            "metal": metal, "qty_oz": qty,
            "spot": round(float(spot_price), 2) if spot_price else None,
            "value": round(mv, 2), "cost_basis": cost_basis,
            "all_time_dollar": at_dollar, "all_time_pct": _pct(at_dollar, cost_basis),
            "daily_pct": round(daily_pct, 2) if daily_pct is not None else None,
        })
    metals_at = round(metals_value - metals_basis, 2)
    metals_bucket = {
        "value": round(metals_value, 2),
        "daily_dollar": round(metals_daily_dollar, 2) if metals_positions else 0.0,
        "all_time_dollar": metals_at,
        "all_time_pct": _pct(metals_at, metals_basis),
        "detail": metal_detail,
    }

    net_worth = round(schwab_value + metals_value, 2)
    all_time_total = round(schwab_all_time + metals_at, 2)
    all_time_cost = schwab_cost + metals_basis

    # ── Daily (total) — close-to-close vs prior snapshot
    today = _az_today()
    prior = _prior_snapshot(today)
    if prior and isinstance(prior.get("net_worth"), (int, float)):
        d_dollar = round(net_worth - float(prior["net_worth"]), 2)
        daily = {"dollar": d_dollar, "pct": _pct(d_dollar, float(prior["net_worth"])),
                 "baseline_date": prior["snapshot_date"]}
    else:
        daily = {"dollar": None, "pct": None, "baseline_date": None}  # first day → '—'

    return {
        "net_worth": net_worth,
        "daily": daily,
        "all_time": {"dollar": all_time_total, "pct": _pct(all_time_total, all_time_cost)},
        "buckets": {"schwab": schwab_bucket, "metals": metals_bucket},
        "gold_value": round(gold_value, 2),
        "silver_value": round(silver_value, 2),
        "excluded": tp.get("sources_excluded", []) + ["alpaca paper (Two-Book)", "SIM", "ghost books"],
        "real_holdings_last_updated": _schwab_last_updated(),
        "freshness": {
            "schwab": "as-of last broker sync (real_holdings_last_updated) — not live-quoted",
            "metals": "live spot",
        },
        "sources_failed": tp.get("sources_failed", []),
        "_source": "HM-AM get_unified_networth",
    }


def _schwab_last_updated() -> Optional[str]:
    """Read real_holdings.json top-level last_updated (sync freshness)."""
    try:
        with _REAL_HOLDINGS_PATH.open() as f:
            return (json.load(f) or {}).get("last_updated")
    except Exception:
        return None


def snapshot_networth() -> dict:
    """HM-AM UNIFICATION — persist today's net-worth snapshot (one row per AZ
    day, last-write-wins). Provides the close-to-close baseline for daily change.
    Returns {snapshot_date, net_worth, seeded}."""
    _ensure_networth_history()
    d = get_unified_networth(force_refresh=True)
    today = _az_today()
    nw = float(d.get("net_worth") or 0.0)
    schwab_v = float((d.get("buckets", {}).get("schwab", {}) or {}).get("value") or 0.0)
    metals_v = float((d.get("buckets", {}).get("metals", {}) or {}).get("value") or 0.0)
    gold_v = float(d.get("gold_value") or 0.0)
    silver_v = float(d.get("silver_value") or 0.0)
    conn = sqlite3.connect(_DB_PATH, timeout=15)
    try:
        conn.execute(
            """INSERT INTO networth_history
               (snapshot_date, net_worth, schwab_value, metals_value, gold_value,
                silver_value, breakdown_json)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(snapshot_date) DO UPDATE SET
                 net_worth=excluded.net_worth, schwab_value=excluded.schwab_value,
                 metals_value=excluded.metals_value, gold_value=excluded.gold_value,
                 silver_value=excluded.silver_value, breakdown_json=excluded.breakdown_json,
                 created_at=datetime('now')""",
            (today, nw, schwab_v, metals_v, gold_v, silver_v, json.dumps(d)),
        )
        conn.commit()
    finally:
        conn.close()
    return {"snapshot_date": today, "net_worth": round(nw, 2), "seeded": True}


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
    # HM-AM UNIFICATION: display only the included accounts (suspended ones are
    # already filtered out of positions/cash upstream in get_total_portfolio).
    for acct_key in INCLUDED_ACCOUNTS:
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
