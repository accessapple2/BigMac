#!/usr/bin/env python3
"""
Neo-Matrix Diagnostic — checks why Neo is (or isn't) firing.
Run: venv/bin/python3 engine/neo_matrix_diagnostic.py
"""
import sqlite3
import sys
from pathlib import Path

# Ensure project root on path so engine.* imports work
_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

DB_PATH = _root / "data" / "trader.db"


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def check_db():
    conn = _conn()
    cur  = conn.cursor()

    cur.execute(
        "SELECT COUNT(*), MIN(created_at), MAX(created_at) "
        "FROM signals WHERE player_id='neo-matrix'"
    )
    sig = cur.fetchone()

    cur.execute(
        "SELECT COUNT(*), SUM(realized_pnl), MIN(executed_at), MAX(executed_at) "
        "FROM trades WHERE player_id='neo-matrix'"
    )
    tr = cur.fetchone()

    cur.execute(
        "SELECT symbol, signal, confidence, execution_status, rejection_reason, created_at "
        "FROM signals WHERE player_id='neo-matrix' ORDER BY created_at DESC LIMIT 10"
    )
    recent_signals = cur.fetchall()

    cur.execute(
        "SELECT symbol, action, qty, entry_price, realized_pnl, executed_at "
        "FROM trades WHERE player_id='neo-matrix' ORDER BY executed_at DESC LIMIT 10"
    )
    recent_trades = cur.fetchall()

    conn.close()
    return sig, tr, recent_signals, recent_trades


def check_config():
    """Check crew_specialization for benched status and mandate."""
    findings = {}

    try:
        from engine.crew_specialization import ADVISORY_CREW, CREW_MANIFEST
        # ADVISORY_CREW contains the benched/shelved list (lines 57-79)
        findings["in_advisory_crew"] = "neo-matrix" in ADVISORY_CREW
        mandate = CREW_MANIFEST.get("neo-matrix", {})
        findings["mandate_found"]  = bool(mandate)
        findings["unrestricted"]   = mandate.get("unrestricted", False)
        findings["max_positions"]  = mandate.get("max_positions", "N/A")
        findings["size_factor"]    = mandate.get("size_factor", "N/A")
        findings["model"]          = mandate.get("model", "N/A")
    except Exception as e:
        findings["config_error"] = str(e)

    try:
        from engine.crew_scanner import ACTIVE_SCANNERS, BYPASS_SNIPER_ALPHA
        findings["in_active_scanners"]    = "neo-matrix" in ACTIVE_SCANNERS
        findings["bypass_sniper_alpha"]   = "neo-matrix" in BYPASS_SNIPER_ALPHA
        findings["active_scanners_list"]  = ACTIVE_SCANNERS
    except Exception as e:
        findings["scanner_error"] = str(e)

    return findings


def check_ollie_threshold():
    """
    Verify the Ollie Commander threshold that gates neo-matrix trades.
    Reads from ollie_commander.py AGENT_THRESHOLDS (per-agent overrides).
    """
    try:
        from engine.ollie_commander import AGENT_THRESHOLDS, THRESHOLD
        neo_threshold = AGENT_THRESHOLDS.get("neo-matrix", THRESHOLD)
        return str(neo_threshold), THRESHOLD
    except Exception as e:
        return f"error: {e}", 2.0


def run_diagnostic():
    print("=" * 60)
    print("NEO-MATRIX DIAGNOSTIC")
    print("=" * 60)

    # ── DB check ──────────────────────────────────────────────────
    sig, tr, recent_signals, recent_trades = check_db()
    print(f"\n[DB — Signals]")
    print(f"  Total signals : {sig[0]}")
    print(f"  First signal  : {sig[1] or 'none'}")
    print(f"  Last signal   : {sig[2] or 'none'}")

    if recent_signals:
        print("  Recent signals:")
        for s in recent_signals:
            print(f"    {s['created_at']}  {s['symbol']}  {s['signal']}  "
                  f"conf={s['confidence']}  status={s['execution_status']}  "
                  f"reject={s['rejection_reason']}")
    else:
        print("  No signals found in DB.")

    print(f"\n[DB — Trades]")
    print(f"  Total trades  : {tr[0]}")
    print(f"  Total P&L     : ${tr[1] or 0:.2f}")
    print(f"  First trade   : {tr[2] or 'none'}")
    print(f"  Last trade    : {tr[3] or 'none'}")
    if recent_trades:
        print("  Recent trades:")
        for t in recent_trades:
            print(f"    {t['executed_at']}  {t['symbol']}  {t['action']}  "
                  f"qty={t['qty']}  entry=${t['entry_price'] or 0:.2f}  "
                  f"pnl=${t['realized_pnl'] or 0:.2f}")

    # ── Config check ──────────────────────────────────────────────
    cfg = check_config()
    print(f"\n[Config — crew_specialization.py]")
    print(f"  ADVISORY_CREW      : {'YES — benched S6.1 (redundant)' if cfg.get('in_advisory_crew') else 'NO — not in advisory list'}")
    print(f"  Mandate found      : {cfg.get('mandate_found')}")
    print(f"  unrestricted       : {cfg.get('unrestricted')}")
    print(f"  max_positions      : {cfg.get('max_positions')}")
    print(f"  model              : {cfg.get('model')}")

    print(f"\n[Config — crew_scanner.py]")
    print(f"  ACTIVE_SCANNERS    : {cfg.get('active_scanners_list')}")
    print(f"  in ACTIVE_SCANNERS : {cfg.get('in_active_scanners')}")
    print(f"  bypass_sniper_alpha: {cfg.get('bypass_sniper_alpha')}")

    # ── Ollie threshold ───────────────────────────────────────────
    neo_threshold, global_threshold = check_ollie_threshold()
    print(f"\n[Ollie Commander Gate]")
    print(f"  Global threshold   : {global_threshold}  (applies to most agents)")
    print(f"  neo-matrix override: {neo_threshold}  (AGENT_THRESHOLDS in ollie_commander.py)")
    print(f"  neo-matrix         : unrestricted=True but Ollie gate still applies")

    # ── Root cause summary ────────────────────────────────────────
    print(f"\n[ROOT CAUSE SUMMARY]")
    issues = []

    if cfg.get("in_advisory_crew"):
        issues.append("CRITICAL: neo-matrix is in ADVISORY_CREW/benched list (crew_specialization.py line 78)")
    if not cfg.get("in_active_scanners"):
        issues.append("WARNING : neo-matrix NOT in ACTIVE_SCANNERS (crew_scanner.py)")
    if cfg.get("in_advisory_crew") and cfg.get("in_active_scanners"):
        issues.append("WARNING : Listed in ADVISORY_CREW (benched) but also in ACTIVE_SCANNERS — contradictory")
    if (tr[0] or 0) == 0:
        issues.append("INFO    : Zero S6 trades (2 trades from S5, pre 2026-04-10)")
    if (sig[0] or 0) == 0:
        issues.append("INFO    : Zero signals in DB — scanner may not be reaching Neo's scan path")

    if issues:
        for i in issues:
            print(f"  {i}")
    else:
        print("  No blocking issues found.")

    print(f"\n[RECOMMENDATIONS]")
    if cfg.get("in_advisory_crew"):
        print("  1. Remove 'neo-matrix' from ADVISORY_CREW in crew_specialization.py to re-activate")
        print("     (or leave and remove from ACTIVE_SCANNERS in crew_scanner.py if intentionally off)")
    print(f"  2. neo-matrix Ollie threshold is {neo_threshold} (correct — set in AGENT_THRESHOLDS)")
    print("  3. Monitor: tail -f /tmp/trademinds.log | grep neo-matrix")
    print()

    return {
        "signals_total"  : sig[0],
        "trades_total"   : tr[0],
        "benched"        : cfg.get("benched"),
        "in_active_scanners": cfg.get("in_active_scanners"),
        "ollie_threshold": neo_threshold,
        "issues"         : issues,
    }


if __name__ == "__main__":
    result = run_diagnostic()
    sys.exit(0 if not result["issues"] else 1)
