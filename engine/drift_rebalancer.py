"""
drift_rebalancer.py — Portfolio drift detection and rebalancing for TradeMinds.

Compares actual position weights against target weights per sub-portfolio.
Flags or auto-corrects when any ticker drifts past the threshold.

Modes:
  ALERT  (default) — report drift in CIC, Captain decides
  AUTO            — execute paper trades to restore target weights

Target weights are stored in rebalance_targets table and managed via:
  set_target(sub_portfolio, ticker, target_pct)
  /api/rebalance/status   → drift report
  /api/rebalance/execute  → run AUTO rebalance (paper only)

CIC commands (intercepted client-side):
  "rebalance check"    → drift report in chat
  "rebalance execute"  → AUTO mode, execute paper trades
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from typing import Optional

logger = logging.getLogger("drift_rebalancer")

import os
DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

DEFAULT_THRESHOLD = 5.0   # percent drift before flagging
DEFAULT_MODE      = "ALERT"

# Sub-portfolio → player_id patterns (mirrors sub_portfolio.py)
_STRATEGY_PLAYERS: dict[str, list[str]] = {
    "Bridge Vote Picks": [
        "claude-sonnet", "gemini-2.5-pro", "gpt-4o", "gpt-o3", "grok-3",
        "captain-sisko", "seven-of-nine", "captain-janeway", "lt-tuvok",
        "ensign-hoshi", "bridge",
    ],
    "DayBlade Options": ["dayblade", "sulu", "dte-", "options"],
    "User Agents":      ["user-agent"],
    "Scanner Picks":    ["chekov", "scotty", "scanner", "warp10", "momentum", "gap-", "volume-"],
}


# ── DB ─────────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=30)


def _init_tables() -> None:
    with _conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS rebalance_targets (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                sub_portfolio   TEXT NOT NULL,
                ticker          TEXT NOT NULL,
                target_pct      REAL NOT NULL DEFAULT 0.0,
                mode            TEXT NOT NULL DEFAULT 'ALERT',
                threshold_pct   REAL NOT NULL DEFAULT 5.0,
                updated_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(sub_portfolio, ticker)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS rebalance_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                sub_portfolio   TEXT NOT NULL,
                ticker          TEXT NOT NULL,
                action          TEXT NOT NULL,
                target_pct      REAL,
                actual_pct      REAL,
                drift_pct       REAL,
                trade_qty       REAL,
                trade_price     REAL,
                mode            TEXT,
                result          TEXT,
                created_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        db.commit()


# ── Target management ──────────────────────────────────────────────────────────

def set_target(sub_portfolio: str, ticker: str, target_pct: float,
               mode: str = DEFAULT_MODE, threshold_pct: float = DEFAULT_THRESHOLD) -> dict:
    """Upsert a target weight for a ticker within a sub-portfolio."""
    _init_tables()
    with _conn() as db:
        db.execute(
            """INSERT INTO rebalance_targets
                   (sub_portfolio, ticker, target_pct, mode, threshold_pct, updated_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(sub_portfolio, ticker) DO UPDATE SET
                   target_pct    = excluded.target_pct,
                   mode          = excluded.mode,
                   threshold_pct = excluded.threshold_pct,
                   updated_at    = datetime('now')""",
            (sub_portfolio, ticker.upper(), target_pct, mode.upper(), threshold_pct),
        )
        db.commit()
    return {"ok": True, "sub_portfolio": sub_portfolio, "ticker": ticker.upper(),
            "target_pct": target_pct}


def list_targets(sub_portfolio: Optional[str] = None) -> list[dict]:
    _init_tables()
    db = _conn()
    if sub_portfolio:
        rows = db.execute(
            "SELECT sub_portfolio, ticker, target_pct, mode, threshold_pct, updated_at "
            "FROM rebalance_targets WHERE sub_portfolio = ? ORDER BY ticker",
            (sub_portfolio,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT sub_portfolio, ticker, target_pct, mode, threshold_pct, updated_at "
            "FROM rebalance_targets ORDER BY sub_portfolio, ticker"
        ).fetchall()
    db.close()
    return [
        {"sub_portfolio": r[0], "ticker": r[1], "target_pct": r[2],
         "mode": r[3], "threshold_pct": r[4], "updated_at": r[5]}
        for r in rows
    ]


# ── Position helpers ───────────────────────────────────────────────────────────

def _positions_for_strategy(strategy_name: str) -> list[dict]:
    patterns = _STRATEGY_PLAYERS.get(strategy_name, [])
    db = _conn()
    try:
        rows = db.execute(
            "SELECT player_id, symbol, qty, avg_price FROM positions WHERE qty > 0"
        ).fetchall()
    finally:
        db.close()
    result = []
    for player_id, symbol, qty, avg_price in rows:
        pid = (player_id or "").lower()
        for pat in patterns:
            if pat.lower() in pid:
                result.append({"player_id": player_id, "symbol": symbol,
                                "qty": float(qty or 0), "avg_price": float(avg_price or 0)})
                break
    return result


def _live_price(ticker: str) -> float:
    """Fetch live price via yfinance. Falls back to 0."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.fast_info
        p = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
        return float(p) if p else 0.0
    except Exception:
        return 0.0


def _enrich_with_prices(positions: list[dict]) -> list[dict]:
    """Add live_price and market_value to each position."""
    symbols = list({p["symbol"] for p in positions})
    prices: dict[str, float] = {}
    try:
        import yfinance as yf
        tickers = yf.Tickers(" ".join(symbols))
        for sym in symbols:
            try:
                info = tickers.tickers[sym].fast_info
                p = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
                prices[sym] = float(p) if p else 0.0
            except Exception:
                prices[sym] = 0.0
    except Exception:
        pass
    for pos in positions:
        lp = prices.get(pos["symbol"]) or pos["avg_price"]
        pos["live_price"]    = lp
        pos["market_value"]  = pos["qty"] * lp
    return positions


# ── Drift calculation ──────────────────────────────────────────────────────────

def drift_report(sub_portfolio: Optional[str] = None,
                 threshold_pct: float = DEFAULT_THRESHOLD) -> dict:
    """
    Calculate drift for all sub-portfolios (or one) against stored targets.
    Returns {sub_portfolios: [{name, total_value, positions: [{...drift data}],
                               drifting_count, max_drift}]}
    """
    _init_tables()
    targets = list_targets(sub_portfolio)
    targets_by_sp: dict[str, list[dict]] = {}
    for t in targets:
        targets_by_sp.setdefault(t["sub_portfolio"], []).append(t)

    all_sps = list(_STRATEGY_PLAYERS.keys()) if not sub_portfolio else [sub_portfolio]
    results = []

    for sp_name in all_sps:
        positions = _positions_for_strategy(sp_name)
        if not positions:
            results.append({"name": sp_name, "total_value": 0.0,
                             "positions": [], "drifting_count": 0, "max_drift": 0.0})
            continue

        positions = _enrich_with_prices(positions)

        # Aggregate by symbol across all players in this sub-portfolio
        by_symbol: dict[str, dict] = {}
        for pos in positions:
            sym = pos["symbol"]
            if sym not in by_symbol:
                by_symbol[sym] = {"symbol": sym, "qty": 0.0, "market_value": 0.0,
                                  "live_price": pos["live_price"]}
            by_symbol[sym]["qty"]          += pos["qty"]
            by_symbol[sym]["market_value"] += pos["market_value"]

        total_value = sum(p["market_value"] for p in by_symbol.values())
        sp_targets  = {t["ticker"]: t for t in targets_by_sp.get(sp_name, [])}
        thr         = threshold_pct

        drift_rows = []
        for sym, agg in by_symbol.items():
            actual_pct = (agg["market_value"] / total_value * 100) if total_value > 0 else 0.0
            target_t   = sp_targets.get(sym)
            target_pct = target_t["target_pct"] if target_t else None
            thr_sym    = target_t["threshold_pct"] if target_t else thr
            drift      = (actual_pct - target_pct) if target_pct is not None else None
            status     = "NO_TARGET"
            if drift is not None:
                abs_drift = abs(drift)
                if abs_drift > thr_sym:
                    status = "OVER" if drift > 0 else "UNDER"
                else:
                    status = "OK"

            drift_rows.append({
                "symbol":       sym,
                "qty":          round(agg["qty"], 4),
                "live_price":   round(agg["live_price"], 2),
                "market_value": round(agg["market_value"], 2),
                "actual_pct":   round(actual_pct, 2),
                "target_pct":   target_pct,
                "drift_pct":    round(drift, 2) if drift is not None else None,
                "threshold":    thr_sym,
                "status":       status,
            })

        drift_rows.sort(key=lambda x: abs(x["drift_pct"] or 0), reverse=True)
        drifting = [r for r in drift_rows if r["status"] in ("OVER", "UNDER")]
        max_drift = max((abs(r["drift_pct"] or 0) for r in drift_rows), default=0.0)

        results.append({
            "name":           sp_name,
            "total_value":    round(total_value, 2),
            "positions":      drift_rows,
            "drifting_count": len(drifting),
            "max_drift":      round(max_drift, 2),
        })

    return {"sub_portfolios": results, "generated_at": datetime.now().isoformat()[:19]}


# ── Rebalance execution ────────────────────────────────────────────────────────

def execute_rebalance(sub_portfolio: str, dry_run: bool = True) -> dict:
    """
    Bring drifting positions back to target weights via paper_trader.
    dry_run=True → calculate trades but don't execute (ALERT mode).
    dry_run=False → execute via paper_trader.buy / sell_partial (AUTO mode).
    Returns {ok, trades_planned, trades_executed, actions}
    """
    _init_tables()
    report = drift_report(sub_portfolio)
    sp_data = next((s for s in report["sub_portfolios"] if s["name"] == sub_portfolio), None)
    if not sp_data or not sp_data["positions"]:
        return {"ok": False, "error": f"No positions in {sub_portfolio}"}

    targets = {t["ticker"]: t for t in list_targets(sub_portfolio)}
    total_value = sp_data["total_value"]
    if total_value <= 0:
        return {"ok": False, "error": "Total value is zero — cannot rebalance"}

    # Find the representative player_id for this sub-portfolio
    patterns = _STRATEGY_PLAYERS.get(sub_portfolio, [])
    player_id = patterns[0] if patterns else "user-agent"

    actions    = []
    executed   = 0
    planned    = 0

    for row in sp_data["positions"]:
        sym        = row["symbol"]
        target_t   = targets.get(sym)
        if not target_t or row["status"] not in ("OVER", "UNDER"):
            continue

        target_pct  = target_t["target_pct"]
        target_val  = total_value * target_pct / 100.0
        current_val = row["market_value"]
        diff_val    = target_val - current_val
        price       = row["live_price"] or row.get("avg_price", 0)
        if price <= 0:
            continue

        qty_delta = diff_val / price
        action    = "BUY" if qty_delta > 0 else "SELL"
        abs_qty   = abs(qty_delta)

        act = {
            "symbol":      sym,
            "action":      action,
            "qty":         round(abs_qty, 4),
            "price":       price,
            "drift_pct":   row["drift_pct"],
            "target_pct":  target_pct,
            "actual_pct":  row["actual_pct"],
            "executed":    False,
            "result":      "DRY_RUN" if dry_run else "PENDING",
        }
        planned += 1

        if not dry_run and abs_qty >= 0.01:
            try:
                from engine.paper_trader import buy, sell_partial
                if action == "BUY":
                    r = buy(player_id=player_id, symbol=sym, price=price,
                            qty=abs_qty, reasoning=f"[DriftRebalancer] {sub_portfolio} target {target_pct:.1f}%",
                            confidence=80.0, timeframe="SWING")
                else:
                    r = sell_partial(player_id=player_id, symbol=sym, price=price,
                                     qty=abs_qty,
                                     reasoning=f"[DriftRebalancer] {sub_portfolio} target {target_pct:.1f}%")
                act["executed"] = r is not None
                act["result"]   = "OK" if r else "BLOCKED"
                if act["executed"]:
                    executed += 1
            except Exception as e:
                act["result"] = f"ERROR: {e}"

        # Log every planned action
        with _conn() as db:
            db.execute(
                """INSERT INTO rebalance_log
                   (sub_portfolio, ticker, action, target_pct, actual_pct, drift_pct,
                    trade_qty, trade_price, mode, result)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (sub_portfolio, sym, action, target_pct, row["actual_pct"],
                 row["drift_pct"], round(abs_qty, 4), price,
                 "DRY_RUN" if dry_run else "AUTO", act["result"]),
            )
            db.commit()

        actions.append(act)

    return {
        "ok":              True,
        "sub_portfolio":   sub_portfolio,
        "dry_run":         dry_run,
        "trades_planned":  planned,
        "trades_executed": executed,
        "actions":         actions,
        "total_value":     total_value,
    }


# ── Rebalance log ──────────────────────────────────────────────────────────────

def get_rebalance_log(sub_portfolio: Optional[str] = None, limit: int = 50) -> list[dict]:
    _init_tables()
    db = _conn()
    if sub_portfolio:
        rows = db.execute(
            "SELECT id, sub_portfolio, ticker, action, target_pct, actual_pct, drift_pct, "
            "trade_qty, trade_price, mode, result, created_at "
            "FROM rebalance_log WHERE sub_portfolio = ? ORDER BY created_at DESC LIMIT ?",
            (sub_portfolio, limit),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, sub_portfolio, ticker, action, target_pct, actual_pct, drift_pct, "
            "trade_qty, trade_price, mode, result, created_at "
            "FROM rebalance_log ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    db.close()
    keys = ["id","sub_portfolio","ticker","action","target_pct","actual_pct",
            "drift_pct","trade_qty","trade_price","mode","result","created_at"]
    return [dict(zip(keys, r)) for r in rows]
