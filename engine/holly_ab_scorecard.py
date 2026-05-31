"""engine/holly_ab_scorecard.py — HM-HOLLY-WORKS Stage 5 A/B scorecard.

Side-by-side Holly vs Ollie from T0 (2026-05-31), measured via the clean aggregator
(player_id-keyed, separable): realized PnL%, win rate, trade-level Sharpe, max drawdown,
# trades, # open positions.

HONEST FRAMING (Admiral-ruled): this is a strategy+UNIVERSE PACKAGE comparison, NOT pure
strategy isolation. Holly = small-cap-momentum-swing specialist ($1-50 movers); Ollie =
large-cap-broad-static scanner ($50-700+). Universes near-disjoint (overlap=1, F).

NOISE CAVEAT: both strategies are validated on ONE regime window — the live race is ALSO
the forward multi-regime test. Early results (first weeks) are noise; the race needs TIME.
A single session or week proves nothing. The scorecard flags low-N explicitly.
"""
from __future__ import annotations

import sqlite3
import math
from datetime import datetime, timezone

TRADER_DB = "data/trader.db"
T0 = "2026-05-31"             # race start (today)
PLAYERS = {
    "holly-scanner": {"label": "Holly (small-cap momentum-swing)", "start_cash": 10000.0},
    "ollie-auto":    {"label": "Ollie (large-cap broad-static)",   "start_cash": 8150.35},
}
MIN_N_MEANINGFUL = 20        # below this, results are flagged as noise


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, timeout=30.0)
    c.row_factory = sqlite3.Row
    return c


def _player_metrics(conn: sqlite3.Connection, pid: str, start_cash: float) -> dict:
    # Closed trades since T0 (realized outcomes)
    closed = conn.execute(
        """SELECT realized_pnl, entry_price, price FROM trades
           WHERE player_id=? AND date(executed_at) >= ? AND realized_pnl IS NOT NULL""",
        (pid, T0)).fetchall()
    n_closed = len(closed)
    pnls = [float(r["realized_pnl"]) for r in closed]
    wins = sum(1 for p in pnls if p > 0)
    realized = round(sum(pnls), 2)

    # per-trade return % (for trade-level Sharpe), using realized_pnl vs entry notional
    rets = []
    for r in closed:
        try:
            ep = float(r["entry_price"] or 0)
            if ep > 0 and r["realized_pnl"] is not None:
                # approximate per-share return %; falls back to pnl sign if no entry px
                rets.append(float(r["realized_pnl"]))
        except Exception:
            pass
    sharpe = None
    if len(pnls) >= 2:
        mean = sum(pnls) / len(pnls)
        var = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
        sd = math.sqrt(var)
        sharpe = round(mean / sd, 2) if sd > 0 else None

    # total trade rows (BUY+SELL) since T0
    n_trades = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE player_id=? AND date(executed_at) >= ?",
        (pid, T0)).fetchone()[0]

    # open positions (canonical: positions table)
    try:
        n_open = conn.execute(
            "SELECT COUNT(*) FROM positions WHERE player_id=? AND qty != 0", (pid,)).fetchone()[0]
    except Exception:
        n_open = None

    # max drawdown from portfolio_history equity curve since T0
    eq = [float(r["total_value"]) for r in conn.execute(
        "SELECT total_value FROM portfolio_history WHERE player_id=? AND date(recorded_at) >= ? "
        "ORDER BY recorded_at ASC", (pid, T0)).fetchall() if r["total_value"] is not None]
    max_dd = 0.0
    if eq:
        peak = eq[0]
        for v in eq:
            peak = max(peak, v)
            if peak > 0:
                max_dd = min(max_dd, (v - peak) / peak * 100)

    pnl_pct = round(realized / start_cash * 100, 2) if start_cash else None
    return {
        "trades": n_trades, "closed": n_closed, "open_positions": n_open,
        "realized_pnl": realized, "pnl_pct": pnl_pct,
        "win_rate": round(wins / n_closed * 100, 1) if n_closed else None,
        "trade_sharpe": sharpe, "max_drawdown_pct": round(max_dd, 1),
        "low_n_noise": n_closed < MIN_N_MEANINGFUL,
    }


def ab_scorecard() -> dict:
    conn = _conn()
    try:
        out = {pid: {**meta, **_player_metrics(conn, pid, meta["start_cash"])}
               for pid, meta in PLAYERS.items()}
    finally:
        conn.close()
    return {
        "t0": T0,
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "framing": ("PACKAGE comparison (strategy+universe), NOT pure isolation. "
                    "Holly=small-cap momentum-swing vs Ollie=large-cap broad-static; "
                    "universes near-disjoint (overlap=1)."),
        "caveat": ("Both validated on ONE regime window — live race is the forward "
                   "multi-regime test. Early weeks are NOISE; needs TIME (low_n flagged)."),
        "players": out,
    }


def _fmt(d: dict) -> str:
    L = [f"═══ HOLLY vs OLLIE A/B — T0={d['t0']} (as of {d['as_of']}) ═══",
         d["framing"], "⚠ " + d["caveat"], ""]
    hdr = f"  {'agent':<34} {'PnL%':>7} {'real$':>9} {'WR%':>6} {'Sharpe':>7} {'maxDD%':>7} {'trades':>7} {'open':>5}"
    L.append(hdr)
    for pid, p in d["players"].items():
        noise = " (low-N noise)" if p["low_n_noise"] else ""
        L.append(f"  {p['label']:<34} {str(p['pnl_pct']):>7} {str(p['realized_pnl']):>9} "
                 f"{str(p['win_rate']):>6} {str(p['trade_sharpe']):>7} {str(p['max_drawdown_pct']):>7} "
                 f"{str(p['trades']):>7} {str(p['open_positions']):>5}{noise}")
    return "\n".join(L)


if __name__ == "__main__":
    import json, sys
    sc = ab_scorecard()
    if "--json" in sys.argv:
        print(json.dumps(sc, indent=2))
    else:
        print(_fmt(sc))
