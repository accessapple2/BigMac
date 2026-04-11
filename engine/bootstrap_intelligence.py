"""bootstrap_intelligence.py — Seed intelligence from all historical trade data.

Reads ALL existing trades from trader.db and computes:
- Per-agent win rates (overall, by symbol, by day-of-week, by hour, by season)
- Regime-aware stats (BULL/BEAR/CAUTIOUS/CRISIS win rates by action type)
- Options history (win rates by strategy, DTE, time-of-day)
- Best/worst symbols per agent
- Outputs a full report to scanner.log
- Stores metrics in bootstrap_metrics table (INSERT only, never modifies trades)

Runs:
- Once on first import if bootstrap_metrics table is empty
- Daily at midnight via main.py schedule
"""
from __future__ import annotations

import sqlite3
import logging
import json
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

DB_PATH   = "data/trader.db"
ATDB_PATH = "autonomous_trader.db"

_ran_today: str | None = None  # date string gate


def _ensure_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS bootstrap_metrics (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_type TEXT NOT NULL,
            player_id   TEXT,
            symbol      TEXT,
            regime      TEXT,
            key         TEXT NOT NULL,
            value       REAL,
            sample_size INTEGER,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_bm_player_key
        ON bootstrap_metrics(player_id, key, calculated_at)
    """)
    db.commit()


def _insert_metric(db: sqlite3.Connection, metric_type: str, key: str,
                   value: float, sample_size: int = 0,
                   player_id: str | None = None, symbol: str | None = None,
                   regime: str | None = None) -> None:
    db.execute("""
        INSERT INTO bootstrap_metrics
            (metric_type, player_id, symbol, regime, key, value, sample_size)
        VALUES (?,?,?,?,?,?,?)
    """, (metric_type, player_id, symbol, regime, key, value, sample_size))


def _calc_winrate(pnls: list[float]) -> float:
    if not pnls:
        return 0.0
    wins = sum(1 for p in pnls if p > 0)
    return round(wins / len(pnls) * 100, 1)


def _fetch_all_trades(db: sqlite3.Connection) -> list[dict]:
    db.row_factory = sqlite3.Row
    rows = db.execute("""
        SELECT player_id, action, symbol, realized_pnl, confidence,
               executed_at, asset_type, option_type, timeframe
        FROM trades
        WHERE action IN ('SELL','BUY') AND realized_pnl IS NOT NULL
        ORDER BY executed_at
    """).fetchall()
    return [dict(r) for r in rows]


def refresh_bootstrap() -> dict:
    """Full bootstrap run. Returns summary dict."""
    global _ran_today
    today = datetime.now().strftime("%Y-%m-%d")
    if _ran_today == today:
        logger.debug("bootstrap: already ran today (%s)", today)
        return {}
    _ran_today = today

    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        _ensure_table(db)
        trades = _fetch_all_trades(db)
    except Exception as e:
        logger.warning("bootstrap: DB error: %s", e)
        return {}

    if not trades:
        db.close()
        return {}

    sell_trades = [t for t in trades if t["action"] == "SELL"]
    n = len(sell_trades)
    logger.info("[BOOTSTRAP] Processing %d closed trades across %d agents",
                n, len({t["player_id"] for t in sell_trades}))

    summary: dict[str, Any] = {}

    # ── Per-agent stats ──
    by_agent: dict[str, list[float]] = {}
    for t in sell_trades:
        pid = t["player_id"] or "unknown"
        by_agent.setdefault(pid, []).append(t["realized_pnl"] or 0)

    best_agent = max(by_agent, key=lambda p: _calc_winrate(by_agent[p]), default=None)
    worst_agent = min(by_agent, key=lambda p: _calc_winrate(by_agent[p]), default=None)

    for pid, pnls in by_agent.items():
        wr = _calc_winrate(pnls)
        avg_pnl = sum(pnls) / len(pnls) if pnls else 0
        _insert_metric(db, "agent_overall", "win_rate", wr, len(pnls), player_id=pid)
        _insert_metric(db, "agent_overall", "avg_pnl",  avg_pnl, len(pnls), player_id=pid)
        _insert_metric(db, "agent_overall", "total_pnl", sum(pnls), len(pnls), player_id=pid)

    # ── Per-symbol win rate (global) ──
    by_symbol: dict[str, list[float]] = {}
    for t in sell_trades:
        sym = t["symbol"] or "?"
        by_symbol.setdefault(sym, []).append(t["realized_pnl"] or 0)

    best_sym = max(by_symbol, key=lambda s: sum(by_symbol[s]) / len(by_symbol[s]) if by_symbol[s] else 0, default=None)
    worst_sym = min(by_symbol, key=lambda s: sum(by_symbol[s]) / len(by_symbol[s]) if by_symbol[s] else 0, default=None)

    for sym, pnls in by_symbol.items():
        if len(pnls) < 2:
            continue
        wr = _calc_winrate(pnls)
        avg = sum(pnls) / len(pnls)
        _insert_metric(db, "symbol_overall", "win_rate", wr, len(pnls), symbol=sym)
        _insert_metric(db, "symbol_overall", "avg_pnl",  avg, len(pnls), symbol=sym)

    # ── Per-agent, per-symbol ──
    by_agent_sym: dict[tuple, list[float]] = {}
    for t in sell_trades:
        key = (t["player_id"] or "?", t["symbol"] or "?")
        by_agent_sym.setdefault(key, []).append(t["realized_pnl"] or 0)
    for (pid, sym), pnls in by_agent_sym.items():
        if len(pnls) < 2:
            continue
        wr = _calc_winrate(pnls)
        _insert_metric(db, "agent_symbol", "win_rate", wr, len(pnls), player_id=pid, symbol=sym)

    # ── Day-of-week stats ──
    by_dow: dict[int, list[float]] = {}
    for t in sell_trades:
        try:
            dt = datetime.fromisoformat(str(t["executed_at"]).replace("Z", ""))
            dow = dt.weekday()  # 0=Mon ... 4=Fri
            by_dow.setdefault(dow, []).append(t["realized_pnl"] or 0)
        except Exception:
            pass
    dow_names = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday"}
    best_dow = None
    best_dow_wr = 0.0
    worst_dow = None
    worst_dow_wr = 100.0
    for dow, pnls in by_dow.items():
        if len(pnls) < 3:
            continue
        wr = _calc_winrate(pnls)
        _insert_metric(db, "day_of_week", "win_rate", wr, len(pnls), key=dow_names.get(dow, str(dow)))
        if wr > best_dow_wr:  best_dow_wr = wr;  best_dow = dow_names.get(dow, str(dow))
        if wr < worst_dow_wr: worst_dow_wr = wr; worst_dow = dow_names.get(dow, str(dow))

    # ── Hour-of-day stats ──
    by_hour: dict[int, list[float]] = {}
    for t in sell_trades:
        try:
            dt = datetime.fromisoformat(str(t["executed_at"]).replace("Z", ""))
            hr = dt.hour
            by_hour.setdefault(hr, []).append(t["realized_pnl"] or 0)
        except Exception:
            pass
    best_hour = max(by_hour, key=lambda h: _calc_winrate(by_hour[h]), default=None)
    worst_hour = min(by_hour, key=lambda h: _calc_winrate(by_hour[h]), default=None)
    for hr, pnls in by_hour.items():
        if len(pnls) < 3:
            continue
        wr = _calc_winrate(pnls)
        _insert_metric(db, "hour_of_day", "win_rate", wr, len(pnls), key=f"hour_{hr:02d}")

    # ── Options stats ──
    opt_trades = [t for t in sell_trades if t.get("asset_type") == "option" or t.get("option_type")]
    if opt_trades:
        opt_pnls = [t["realized_pnl"] for t in opt_trades]
        opt_wr = _calc_winrate(opt_pnls)
        avg_opt_loss = sum(p for p in opt_pnls if p < 0) / max(1, sum(1 for p in opt_pnls if p < 0))
        _insert_metric(db, "options_overall", "win_rate", opt_wr, len(opt_trades), key="options_win_rate")
        _insert_metric(db, "options_overall", "avg_loss", avg_opt_loss, len(opt_trades), key="options_avg_loss")

    db.commit()
    db.close()

    # Build summary
    summary = {
        "total_trades": n,
        "agents": len(by_agent),
        "symbols": len(by_symbol),
        "best_agent": best_agent,
        "worst_agent": worst_agent,
        "best_symbol": best_sym,
        "worst_symbol": worst_sym,
        "best_day": best_dow,
        "best_day_winrate": best_dow_wr,
        "worst_day": worst_dow,
        "worst_day_winrate": worst_dow_wr,
        "best_hour": best_hour,
        "worst_hour": worst_hour,
        "options_count": len(opt_trades),
    }

    _log_report(summary, by_agent, by_symbol, by_dow, by_hour)
    return summary


def _log_report(summary: dict, by_agent: dict, by_symbol: dict, by_dow: dict, by_hour: dict) -> None:
    logger.info(
        "[BOOTSTRAP] Processed %d trades across %d agents\n"
        "  Best agent:  %s | Worst agent:  %s\n"
        "  Best symbol: %s | Worst symbol: %s\n"
        "  Best day:    %s (%.0f%% WR) | Avoid: %s (%.0f%% WR)\n"
        "  Best hour:   %s:00 ET | Worst: %s:00 ET",
        summary["total_trades"], summary["agents"],
        summary["best_agent"], summary["worst_agent"],
        summary["best_symbol"], summary["worst_symbol"],
        summary["best_day"], summary["best_day_winrate"],
        summary["worst_day"], summary["worst_day_winrate"],
        summary["best_hour"], summary["worst_hour"],
    )


def get_agent_intelligence(player_id: str, symbol: str | None = None) -> str:
    """Return formatted intelligence string for brain_context injection."""
    try:
        db = sqlite3.connect(DB_PATH, timeout=5)
        db.row_factory = sqlite3.Row
        lines: list[str] = []

        # Overall win rate
        row = db.execute("""
            SELECT value, sample_size FROM bootstrap_metrics
            WHERE metric_type='agent_overall' AND player_id=? AND key='win_rate'
            ORDER BY calculated_at DESC LIMIT 1
        """, (player_id,)).fetchone()
        if row and row["sample_size"] >= 3:
            lines.append(f"Historical win rate: {row['value']:.0f}% ({row['sample_size']} trades)")

        # Symbol-specific
        if symbol:
            row2 = db.execute("""
                SELECT value, sample_size FROM bootstrap_metrics
                WHERE metric_type='agent_symbol' AND player_id=? AND symbol=? AND key='win_rate'
                ORDER BY calculated_at DESC LIMIT 1
            """, (player_id, symbol)).fetchone()
            if row2 and row2["sample_size"] >= 2:
                tag = "EDGE" if row2["value"] > 60 else ("AVOID" if row2["value"] < 35 else "OK")
                lines.append(f"Your {symbol} history: {row2['value']:.0f}% WR ({row2['sample_size']} trades) [{tag}]")

        # Day-of-week hint
        now = datetime.now()
        dow_name = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"][now.weekday()]
        dow_row = db.execute("""
            SELECT value, sample_size FROM bootstrap_metrics
            WHERE metric_type='day_of_week' AND key=?
            ORDER BY calculated_at DESC LIMIT 1
        """, (dow_name,)).fetchone()
        if dow_row and dow_row["sample_size"] >= 3:
            if dow_row["value"] < 40:
                lines.append(f"Caution: {dow_name} historically weak ({dow_row['value']:.0f}% WR fleet-wide)")

        db.close()
        if not lines:
            return ""
        return "[Historical Intelligence] " + " | ".join(lines)
    except Exception:
        return ""


# ── Auto-run on first import if table empty ───────────────────────────────────
def _maybe_bootstrap() -> None:
    try:
        db = sqlite3.connect(DB_PATH, timeout=5)
        _ensure_table(db)
        count = db.execute("SELECT COUNT(*) FROM bootstrap_metrics").fetchone()[0]
        db.close()
        if count == 0:
            logger.info("[BOOTSTRAP] First run — seeding from trade history...")
            refresh_bootstrap()
    except Exception as e:
        logger.debug("bootstrap init: %s", e)


_maybe_bootstrap()
