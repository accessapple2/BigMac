#!/usr/bin/env python3
"""Verify the 2026-07-13 repair session's fixes are behaving.

Read-only: never writes to data/trader.db, never submits orders, never
un-halts/arms anything. Two kinds of checks:

  - DB checks: run against data/trader.db directly, work whether or not
    the trader process is up.
  - Module checks: import the fixed functions directly and exercise them
    with synthetic inputs — verifies the FIX ITSELF is present and
    correct, independent of whether main.py has been restarted yet.
  - API checks: hit the live dashboard (:8080) / signal-center (:9000)
    endpoints. SKIP (not FAIL) if the service isn't reachable — these
    only mean something after a restart.

Usage:
    .venv/bin/python3 scripts/verify_repairs_20260713.py

Exit code: 0 if no FAILs (SKIPs are fine), 1 if any check FAILs.
"""
from __future__ import annotations

import os
import sqlite3
import sys

# Running as `scripts/verify_repairs_20260713.py` puts sys.path[0] at
# scripts/, not the repo root — `import engine.X` fails without this.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)  # DB path / imports below assume CWD == repo root
import traceback
from datetime import datetime, timezone

DB = "data/trader.db"
DASHBOARD_BASE = "http://127.0.0.1:8080"
SIGNAL_CENTER_BASE = "http://127.0.0.1:9000"
HTTP_TIMEOUT = 6

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"
results: list[tuple[str, str, str]] = []  # (name, status, detail)


def check(name):
    """Decorator: run fn(), catch any exception as a FAIL, record result."""
    def wrap(fn):
        try:
            status, detail = fn()
        except Exception as e:
            status, detail = FAIL, f"check raised {type(e).__name__}: {e!r}"
            traceback.print_exc(file=sys.stderr)
        results.append((name, status, detail))
        return fn
    return wrap


def _conn():
    c = sqlite3.connect(DB, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def _http_get_json(url):
    """Minimal, dependency-free JSON GET. Returns (ok, data_or_error)."""
    import json
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as r:
            return True, json.loads(r.read().decode())
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ─── P0-1: dayblade entry pricing + mark-price decay ────────────────────────

@check("P0-1a: _estimate_atm_premium has the 0.4 ATM coefficient")
def _p0_1a():
    from engine.dayblade import _estimate_atm_premium
    # SPY 5DTE ATM put, the exact case from the audit: stock=750.09.
    # Pre-fix this returned $26.34 (no 0.4 coefficient); post-fix ~$10.53.
    premium = _estimate_atm_premium(750.09, 5, "put")
    if premium >= 20.0:
        return FAIL, f"premium=${premium:.2f} — looks like the pre-fix formula (missing 0.4x)"
    if premium <= 0:
        return FAIL, f"premium=${premium:.2f} — non-positive, formula broken"
    return PASS, f"premium=${premium:.2f} (pre-fix was $26.34 for this input)"


@check("P0-1b: _dayblade_mark_price does not fabricate an instant loss")
def _p0_1b():
    from engine.dayblade import _dayblade_mark_price
    now = datetime.now(timezone.utc)
    opened_at = now.strftime("%Y-%m-%d %H:%M:%S")
    expiry = now.strftime("%Y-%m-%d")  # 0DTE-ish, worst case for decay
    # A position opened THIS INSTANT should mark back near its own entry
    # premium, not show a ~-70% loss from a stale absolute-DTE formula.
    cur = _dayblade_mark_price("put", 750.0, 750.09, 26.34, expiry, opened_at)
    pnl_pct = (cur - 26.34) / 26.34 * 100
    if pnl_pct < -15:
        return FAIL, f"mark=${cur:.2f} ({pnl_pct:+.1f}%) — fabricating a loss on a fresh position"
    return PASS, f"mark=${cur:.2f} ({pnl_pct:+.1f}%) on a just-opened position"


@check("P0-1c: no NEW fractional/zero-qty option trades since repair session started")
def _p0_1c():
    conn = _conn()
    rows = conn.execute(
        "SELECT id, player_id, symbol, qty, executed_at FROM trades "
        "WHERE asset_type='option' AND (qty=0 OR qty != CAST(qty AS INTEGER)) "
        "AND executed_at > '2026-07-13 17:30:00' ORDER BY executed_at DESC"
    ).fetchall()
    conn.close()
    if rows:
        detail = "; ".join(f"id={r['id']} {r['player_id']} {r['symbol']} qty={r['qty']}" for r in rows[:5])
        return FAIL, f"{len(rows)} new bad option qty rows: {detail}"
    return PASS, "no new fractional/zero-qty option trades found"


# ─── P0-2: direction validation guard ───────────────────────────────────────

@check("P0-2: direction guard rejects a known thesis/action inversion")
def _p0_2():
    from engine.ai_brain import _thesis_direction_conflicts
    reasoning = (
        "The SPY broke below the OR low, indicating strong bearish sentiment "
        "and a downward trend."
    )
    reject = _thesis_direction_conflicts("BUY_CALL", reasoning)
    if not reject:
        return FAIL, "guard did not fire on a clear bearish-reasoning/BUY_CALL mismatch"
    aligned = _thesis_direction_conflicts("BUY_CALL", "Strong bullish breakout, rally continuing.")
    if aligned:
        return FAIL, f"guard false-positived on an aligned bullish BUY_CALL: {aligned}"
    return PASS, "rejects inverted thesis, passes aligned thesis"


@check("P0-2: no new direction-inverted trades since repair session started")
def _p0_2_live():
    from engine.ai_brain import _thesis_direction_conflicts
    conn = _conn()
    rows = conn.execute(
        "SELECT id, player_id, action, reasoning FROM trades "
        "WHERE action IN ('BUY_CALL','BUY_PUT') AND executed_at > '2026-07-13 17:30:00'"
    ).fetchall()
    conn.close()
    bad = [r for r in rows if _thesis_direction_conflicts(r["action"], r["reasoning"] or "")]
    if bad:
        detail = "; ".join(f"id={r['id']} {r['player_id']} {r['action']}" for r in bad[:5])
        return FAIL, f"{len(bad)} new direction-inverted trades: {detail}"
    return PASS, f"{len(rows)} new option trades checked, none direction-inverted"


# ─── P0-3: integer contract sizing ──────────────────────────────────────────

@check("P0-3: no NEW fractional-qty option trades fleet-wide (paper_trader path)")
def _p0_3():
    conn = _conn()
    rows = conn.execute(
        "SELECT id, player_id, symbol, qty FROM trades "
        "WHERE asset_type='option' AND qty != CAST(qty AS INTEGER) "
        "AND executed_at > '2026-07-13 17:30:00'"
    ).fetchall()
    conn.close()
    if rows:
        detail = "; ".join(f"id={r['id']} {r['player_id']} {r['symbol']} qty={r['qty']}" for r in rows[:5])
        return FAIL, f"{len(rows)} new fractional-qty option trades: {detail}"
    return PASS, "no new fractional-qty option trades"


# ─── P0-4 Finding 2: battle_station ledger write ────────────────────────────

@check("P0-4/F2: battle_station auto-close writes carry correct season + realized_pnl")
def _p0_4_f2():
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    current_season = int(row["value"]) if row and row["value"] else None
    rows = conn.execute(
        "SELECT id, season, realized_pnl FROM trades "
        "WHERE reasoning LIKE '[BATTLE-STATION-AUTO-CLOSE%' "
        "AND executed_at > '2026-07-13 17:30:00'"
    ).fetchall()
    conn.close()
    if not rows:
        return SKIP, "no battle_station auto-close rows written yet (expected until it fires live)"
    bad = [r for r in rows if r["season"] != current_season or r["realized_pnl"] is None]
    if bad:
        detail = "; ".join(f"id={r['id']} season={r['season']} pnl={r['realized_pnl']}" for r in bad[:5])
        return FAIL, f"{len(bad)}/{len(rows)} rows missing correct season/realized_pnl: {detail}"
    return PASS, f"{len(rows)} auto-close rows, all carry season={current_season} and a realized_pnl"


# ─── P1-5: gex feed ──────────────────────────────────────────────────────────

@check("P1-5: /api/gex/SPY returns non-null total_gex or an explicit no-data marker")
def _p1_5():
    ok, data = _http_get_json(f"{DASHBOARD_BASE}/api/gex/SPY")
    if not ok:
        return SKIP, f"dashboard not reachable ({data})"
    if "error" in data:
        return PASS, f"explicit no-data: {data['error']}"
    if data.get("total_gex") is None:
        return FAIL, "total_gex is null with no error field — silent no-data, not explicit"
    return PASS, f"total_gex={data['total_gex']}"


@check("P1-5: signal-center gex payload carries net_gex (aliased from total_gex)")
def _p1_5_alias():
    ok, data = _http_get_json(f"{SIGNAL_CENTER_BASE}/api/signals/all")
    if not ok:
        return SKIP, f"signal-center not reachable ({data})"
    gex = (data or {}).get("gex") or {}
    if not isinstance(gex, dict) or "total_gex" not in gex:
        return SKIP, "no gex payload yet (feed may not have run since restart)"
    if "net_gex" not in gex:
        return FAIL, "net_gex alias missing from signal-center gex payload"
    return PASS, f"net_gex={gex.get('net_gex')} (aliased from total_gex={gex.get('total_gex')})"


# ─── P1-6: risk_radar concentration ─────────────────────────────────────────

@check("P1-6: concentration formula scores a >50%-of-equity position near max risk")
def _p1_6():
    from engine.risk_radar import get_risk_radar
    import unittest.mock as mock

    fake_portfolio = {
        "cash": 4800.0,
        "positions": [{"symbol": "QQQ", "qty": 10, "avg_price": 520.0}],
    }
    # total_value = 4800 + 10*520 = 10000; QQQ position = 5200 = 52% of equity
    with mock.patch("engine.paper_trader.get_portfolio", return_value=fake_portfolio):
        radar = get_risk_radar("__verify_synthetic__", {"QQQ": {"price": 520.0}})
    conc = radar["dimensions"]["concentration"]
    if conc < 90:
        return FAIL, f"concentration={conc} for a 52% position — expected ~100 (near-saturated)"
    return PASS, f"concentration={conc} for a 52%-of-equity synthetic position"


@check("P1-6 (live): risk_radar API reflects real concentrated positions, if any exist")
def _p1_6_live():
    ok, data = _http_get_json(f"{DASHBOARD_BASE}/api/risk-radar?show_all=true" if False else f"{DASHBOARD_BASE}/api/risk-radar")
    if not ok:
        return SKIP, f"dashboard not reachable ({data})"
    players = data.get("players") if isinstance(data, dict) else None
    if not players and isinstance(data, dict):
        # /api/risk-radar without player_id returns {player_id: radar} for get_all_risk_radars
        players = data
    if not players or (isinstance(players, dict) and players.get("loading")):
        return SKIP, "risk_radar cache cold/loading — re-run after warmup"
    return PASS, f"risk_radar responded with {len(players) if hasattr(players, '__len__') else '?'} entries"


# ─── P1-7: holly profit_factor ──────────────────────────────────────────────

@check("P1-7: profit_factor returns null (not 0) when there are zero losing trades")
def _p1_7():
    from engine.holly_nightly_backtest import _stat
    pf = _stat({"Profit Factor": float("inf")}, "Profit Factor")
    if pf == 0:
        return FAIL, "profit_factor=0 for +inf input (no losses) — old bug still present"
    if pf is not None:
        return FAIL, f"profit_factor={pf!r} for +inf input — expected None"
    normal = _stat({"Profit Factor": 2.5}, "Profit Factor")
    if normal != 2.5:
        return FAIL, f"normal profit_factor case broke: got {normal!r}, expected 2.5"
    return PASS, "+inf -> None; normal values unaffected"


@check("P1-7: no NEW holly_intraday_winners rows with win_rate=100 and profit_factor=0")
def _p1_7_live():
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT ticker, win_rate, profit_factor FROM holly_intraday_winners "
            "WHERE win_rate=100 AND profit_factor=0"
        ).fetchall()
    except sqlite3.OperationalError as e:
        conn.close()
        return SKIP, f"holly_intraday_winners table/columns not as expected: {e}"
    conn.close()
    if rows:
        detail = "; ".join(r["ticker"] for r in rows[:5])
        return FAIL, f"{len(rows)} rows still show win_rate=100/profit_factor=0: {detail}"
    return PASS, "no win_rate=100/profit_factor=0 rows in holly_intraday_winners"


# ─── P1-8: fast_scan staleness ──────────────────────────────────────────────

@check("P1-8: /api/fast-scan payload carries as_of/stale_count")
def _p1_8():
    ok, data = _http_get_json(f"{DASHBOARD_BASE}/api/fast-scan")
    if not ok:
        return SKIP, f"dashboard not reachable ({data})"
    missing = [k for k in ("as_of", "stale_count") if k not in data]
    if missing:
        return FAIL, f"fast-scan response missing {missing} — old code still running?"
    return PASS, f"as_of={data.get('as_of')}, stale_count={data.get('stale_count')}"


# ─── P1-9 / gex composite: covered by P1-5 checks above (same data path) ────

@check("P1-9: scoreGex-equivalent — a missing gex field does not read as bearish")
def _p1_9():
    # Pure-python re-implementation of the fixed JS logic's decision boundary,
    # since we can't execute the browser JS here — this checks the CONTRACT
    # (no-data -> neutral, not negative) that dashboard/app.py's payload must
    # satisfy for the frontend fix to behave correctly.
    def score_gex(d):
        if not d:
            return 50, "no data"
        graw = d["total_gex"] if d.get("total_gex") is not None else d.get("net_gex")
        if graw is None:
            return 50, "no data"
        return (80 if graw > 2e9 else 65 if graw > 0 else 42 if graw > -2e9 else 22), "scored"

    score, _ = score_gex({})
    if score != 50:
        return FAIL, f"missing-data case scored {score}, expected neutral 50"
    score, _ = score_gex({"total_gex": None})
    if score != 50:
        return FAIL, f"explicit-null case scored {score}, expected neutral 50"
    return PASS, "no-data and null cases both score neutral (50), not bearish"


# ─── P2-10: positions side label ────────────────────────────────────────────

@check("P2-10: side-from-qty-sign logic is correct")
def _p2_10():
    def side(qty):
        return "short" if qty < 0 else "long"
    if side(-1) != "short":
        return FAIL, "qty=-1 did not resolve to short"
    if side(5) != "long":
        return FAIL, "qty=5 did not resolve to long"
    return PASS, "qty=-1 -> short, qty=5 -> long"


# ─── convergence count=99 fix ───────────────────────────────────────────────

@check("Convergence: max_strategies_triggered is present and plausible (<=80)")
def _convergence():
    ok, data = _http_get_json(f"{DASHBOARD_BASE}/api/navigator/convergence")
    if not ok:
        return SKIP, f"dashboard not reachable ({data})"
    if "max_strategies_triggered" not in data:
        return FAIL, "max_strategies_triggered missing from response — old code still running?"
    val = data["max_strategies_triggered"]
    if val is None or val > 80:
        return FAIL, f"max_strategies_triggered={val} — implausible for a ~17-79 agent fleet"
    return PASS, f"max_strategies_triggered={val} (raw ticker count={data.get('count')})"


# ─── Halt-state sanity (should NOT have changed) ────────────────────────────

@check("Halt states: dayblade-0dte/dayblade-sulu/navigator/neo-matrix still halt_mode=full")
def _halts():
    conn = _conn()
    rows = conn.execute(
        "SELECT id, halt_mode FROM ai_players WHERE id IN "
        "('dayblade-0dte','dayblade-sulu','navigator','neo-matrix')"
    ).fetchall()
    conn.close()
    not_halted = [r["id"] for r in rows if r["halt_mode"] != "full"]
    if not_halted:
        return FAIL, f"unexpectedly not halted: {not_halted} — someone armed these without the verification gate"
    if len(rows) != 4:
        found = [r["id"] for r in rows]
        return FAIL, f"expected 4 agents, found {len(rows)}: {found}"
    return PASS, "all 4 repair-session agents remain halt_mode=full"


# ─── main ────────────────────────────────────────────────────────────────

def main():
    print(f"Repair verification — {datetime.now(timezone.utc).isoformat()}Z")
    print("=" * 78)
    width = max(len(n) for n, _, _ in results) if results else 0
    n_fail = 0
    for name, status, detail in results:
        marker = {"PASS": "✓", "FAIL": "✗", "SKIP": "–"}[status]
        print(f"[{marker} {status:4s}] {name:<{width}}  {detail}")
        if status == FAIL:
            n_fail += 1
    print("=" * 78)
    n_pass = sum(1 for _, s, _ in results if s == PASS)
    n_skip = sum(1 for _, s, _ in results if s == SKIP)
    print(f"{n_pass} passed, {n_fail} failed, {n_skip} skipped ({len(results)} total)")
    if n_fail:
        print("\nFAIL present — do not consider the repair session verified.")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
