#!/usr/bin/env python3
"""
Nightly Auto-Backtest Pipeline
Runs at 3:00 AM AZ daily via launchd.
Calls run_super_backtest_v5(), then queries backtest_history for today's results,
compares with prior night, fires ntfy if any agent beats prior Sharpe.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NIGHTLY] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH   = Path(__file__).parent.parent / "data" / "trader.db"
NTFY_TOPIC = "ollietrades-admin"


# ── ntfy ─────────────────────────────────────────────────────────────────────
def send_ntfy(title: str, message: str, priority: str = "default") -> None:
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": priority},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"ntfy error: {e}")


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_prior_best() -> dict:
    """Return yesterday's best (return_pct, win_rate) per player_id."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT player_id,
               MAX(return_pct)  AS best_return,
               MAX(win_rate)    AS best_wr
        FROM   backtest_history
        WHERE  date(run_date) = date('now', '-1 day')
        GROUP  BY player_id
    """).fetchall()
    conn.close()
    return {r[0]: {"return": r[1] or 0, "wr": r[2] or 0} for r in rows}


def get_todays_results() -> list[dict]:
    """Return today's records from backtest_history."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT player_id, player_name, return_pct, win_rate, total_trades,
               win_count, loss_count, spy_return_pct
        FROM   backtest_history
        WHERE  date(run_date) = date('now')
        ORDER  BY return_pct DESC
    """).fetchall()
    conn.close()
    return [
        {
            "player_id":   r[0],
            "player_name": r[1] or r[0],
            "return_pct":  round(r[2] or 0, 2),
            "win_rate":    round(r[3] or 0, 1),
            "trades":      r[4] or 0,
            "wins":        r[5] or 0,
            "losses":      r[6] or 0,
            "spy_return":  round(r[7] or 0, 2),
        }
        for r in rows
    ]


# ── Main ──────────────────────────────────────────────────────────────────────
def run_nightly_backtest() -> list[dict]:
    t0     = datetime.now()
    run_id = f"nightly_{t0.strftime('%Y%m%d_%H%M')}"

    log.info(f"=== Nightly Backtest — {t0.strftime('%Y-%m-%d %H:%M')} | run_id={run_id} ===")

    # ── Run the backtest ──────────────────────────────────────────────────────
    log.info("Running super_backtest_v5 (all agents, 60-day window)...")
    try:
        from engine.super_backtest_v5 import run_super_backtest_v5
        bt_result = run_super_backtest_v5()
        log.info(f"Backtest finished: {bt_result.get('status')} in {bt_result.get('elapsed_s', 0):.0f}s")
    except Exception as e:
        log.error(f"Backtest failed: {e}")
        send_ntfy("Nightly Backtest FAILED", str(e), priority="high")
        return []

    # ── Read results written to backtest_history ──────────────────────────────
    prior   = get_prior_best()
    results = get_todays_results()

    if not results:
        log.warning("No results written to backtest_history today — nothing to compare")
        send_ntfy("Nightly Backtest", "Ran but no records in backtest_history today.")
        return []

    # ── Compare with yesterday ────────────────────────────────────────────────
    improvements = []
    for r in results:
        pid = r["player_id"]
        if pid in prior and r["return_pct"] > prior[pid]["return"]:
            delta = r["return_pct"] - prior[pid]["return"]
            improvements.append({
                "agent":       r["player_name"],
                "old_return":  prior[pid]["return"],
                "new_return":  r["return_pct"],
                "delta":       round(delta, 2),
            })

    # ── Build notification ────────────────────────────────────────────────────
    elapsed = (datetime.now() - t0).total_seconds() / 60
    lines   = [
        f"Run ID: {run_id}",
        f"Duration: {elapsed:.1f} min",
        "",
        "TOP PERFORMERS:",
    ]
    for r in results[:5]:
        vs = r["return_pct"] - r["spy_return"]
        lines.append(f"  {r['player_name']}: {r['return_pct']:+.2f}% (WR {r['win_rate']}%, {r['trades']} trades, {vs:+.2f}% vs SPY)")

    if improvements:
        lines += ["", "IMPROVEMENTS vs YESTERDAY:"]
        for imp in improvements:
            lines.append(f"  {imp['agent']}: {imp['old_return']:+.2f}% -> {imp['new_return']:+.2f}% (+{imp['delta']:.2f}%)")

    msg      = "\n".join(lines)
    priority = "high" if improvements else "default"
    send_ntfy("Nightly Backtest", msg, priority=priority)

    log.info(f"\n{msg}")
    log.info("=== Nightly Backtest complete ===")
    return results


if __name__ == "__main__":
    run_nightly_backtest()
