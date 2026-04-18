#!/usr/bin/env python3
"""
Full Wiring Verification
Checks that ALL S6 upgrades are properly connected before backtest.
Run: venv/bin/python3 engine/full_wiring_check.py
"""
import sys
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime

_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

DB_PATH = _root / "data" / "trader.db"

print("\n" + "=" * 70)
print("🔍 OLLIETRADES S6 FULL WIRING VERIFICATION")
print("=" * 70)
print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

checks_passed = 0
checks_failed = 0
warnings_list = []


def check(name: str, condition: bool, critical: bool = True) -> bool:
    global checks_passed, checks_failed, warnings_list
    if condition:
        print(f"  ✅ {name}")
        checks_passed += 1
    elif critical:
        print(f"  ❌ {name}")
        checks_failed += 1
    else:
        print(f"  ⚠️  {name} (optional)")
        warnings_list.append(name)
    return condition


# ── Module imports ────────────────────────────────────────────────────────────
print("\n📦 MODULE IMPORTS:")

try:
    from engine.strategy_breakdown import get_strategy_breakdown
    check("strategy_breakdown", True)
except ImportError as e:
    check(f"strategy_breakdown: {e}", False)

try:
    from engine.trade_log import get_trade_log, get_trade_summary
    check("trade_log", True)
except ImportError as e:
    check(f"trade_log: {e}", False)

try:
    from engine.regime_analyzer import get_regime_summary, get_best_agents_by_regime
    check("regime_analyzer", True)
except ImportError as e:
    check(f"regime_analyzer: {e}", False)

try:
    from engine.uhura_bridge_integration import get_institutional_vote, apply_institutional_boost
    check("uhura_bridge_integration", True)
except ImportError as e:
    check(f"uhura_bridge_integration: {e}", False)

try:
    from engine.danelfin_parser import get_danelfin_score, get_top_danelfin_picks
    check("danelfin_parser", True)
except ImportError as e:
    check(f"danelfin_parser: {e}", False)

try:
    from engine.riker_synthesis import generate_synthesis
    check("riker_synthesis", True)
except ImportError as e:
    check(f"riker_synthesis: {e}", False)

try:
    from engine.holly_patterns import holly_rules
    check("holly_patterns (holly_rules)", True)
except ImportError as e:
    check(f"holly_patterns: {e}", False)

try:
    from engine.neo_matrix_diagnostic import run_diagnostic
    check("neo_matrix_diagnostic", True)
except ImportError as e:
    check(f"neo_matrix_diagnostic: {e}", False)

try:
    from engine.nightly_backtest import run_nightly_backtest
    check("nightly_backtest", True)
except ImportError as e:
    check(f"nightly_backtest: {e}", False)

try:
    from engine.backtest_api import router as _bt_router
    check("backtest_api (FastAPI router)", True)
except ImportError as e:
    check(f"backtest_api router: {e}", False)

try:
    from engine.intelligence_api import router as _intel_router
    check("intelligence_api (FastAPI router)", True)
except ImportError as e:
    check(f"intelligence_api router: {e}", False)

# Optional
try:
    from engine.long_range_sensors import scan_for_whales
    check("long_range_sensors (optional)", True, critical=False)
except ImportError:
    check("long_range_sensors", False, critical=False)

try:
    from engine.archer_morning_synthesis import run_archer_synthesis
    check("archer_morning_synthesis (optional)", True, critical=False)
except ImportError:
    check("archer_morning_synthesis", False, critical=False)


# ── Database tables ───────────────────────────────────────────────────────────
print("\n📊 DATABASE TABLES:")

conn = sqlite3.connect(DB_PATH)
required_tables = [
    "trades", "signals", "positions", "players",
    "backtest_history", "regime_history", "institutional_signals",
    "danelfin_scores", "rikers_log",
]

for table in required_tables:
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        check(f"  {table}: {count:,} rows", True)
    except Exception as e:
        critical = table in ("trades", "signals", "backtest_history")
        check(f"  {table}: {e}", False, critical=critical)

print("\n📈 KEY DATA:")

try:
    inst = conn.execute("SELECT COUNT(*) FROM institutional_signals").fetchone()[0]
    check(f"  Institutional signals: {inst:,}", inst > 0)
except Exception as e:
    check(f"  institutional_signals: {e}", False)

try:
    trades_total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    check(f"  Total trades: {trades_total:,}", trades_total > 0)
except Exception as e:
    check(f"  trades count: {e}", False)

try:
    sigs_total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    check(f"  Total signals: {sigs_total:,}", sigs_total > 0)
except Exception as e:
    check(f"  signals count: {e}", False)

conn.close()


# ── Crew scanner wiring ───────────────────────────────────────────────────────
print("\n🔌 CREW SCANNER WIRING:")

scanner = (_root / "engine" / "crew_scanner.py").read_text()
check("  Uhura wired (UHURA_AVAILABLE)", "UHURA_AVAILABLE" in scanner)
check("  LRS wired (LRS_AVAILABLE)", "LRS_AVAILABLE" in scanner)
check("  Holly wired (holly-scanner)", "holly-scanner" in scanner)
check("  LRS called in scan cycle (_lrs_scan())", "_lrs_scan()" in scanner)
check("  Uhura boost called (apply_institutional_boost)", "apply_institutional_boost(symbol" in scanner)
check("  Neo-matrix in ACTIVE_SCANNERS", "neo-matrix" in scanner and "ACTIVE_SCANNERS" in scanner)

spec = (_root / "engine" / "crew_specialization.py").read_text()
check("  Neo-matrix removed from ADVISORY_CREW", '    "neo-matrix",' not in spec)
check("  holly-scanner in CREW_MANIFEST", '"holly-scanner"' in spec)


# ── API wiring ────────────────────────────────────────────────────────────────
print("\n🌐 API ROUTES (app.py):")

app_content = (_root / "dashboard" / "app.py").read_text()
check("  Backtest API router mounted", "backtest_analytics_router" in app_content or "backtest_api" in app_content)
check("  Intelligence API router mounted", "intelligence_router" in app_content or "intelligence_api" in app_content)

index_html = (_root / "dashboard" / "static" / "index.html").read_text()
check("  backtest_panels.css in index.html", "backtest_panels.css" in index_html)
check("  backtest_panels.js in index.html", "backtest_panels.js" in index_html)


# ── LaunchAgents ──────────────────────────────────────────────────────────────
print("\n⏰ LAUNCHD AGENTS:")

try:
    result = subprocess.run(["launchctl", "list"], capture_output=True, text=True)
    loaded = result.stdout

    launchd_jobs = [
        ("com.ollietrades.nightly-backtest",  True),
        ("com.ollietrades.riker-synthesis",   True),
        ("com.ollietrades.danelfin-update",   True),
        ("com.ollietrades.archer-briefing",   False),
        ("com.ollietrades.morningbriefing",   False),
        ("com.ollietrades.ghost-trader",      False),
        ("com.ollietrades.etfregime",         False),
    ]
    for job, crit in launchd_jobs:
        check(f"  {job}", job in loaded, critical=crit)
except Exception as e:
    check(f"  launchctl list: {e}", False, critical=False)


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("📋 VERIFICATION SUMMARY")
print("=" * 70)
print(f"  ✅ Passed  : {checks_passed}")
print(f"  ❌ Failed  : {checks_failed}")
print(f"  ⚠️  Optional: {len(warnings_list)}")

if checks_failed > 0:
    print("\n🚨 Fix failed checks before running backtest.")
    sys.exit(1)
else:
    print("\n🎉 ALL CRITICAL CHECKS PASSED — Ready for 6-month backtest!")
    sys.exit(0)
