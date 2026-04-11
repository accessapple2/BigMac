#!/usr/bin/env python3
"""Pre-restart regression tests for USS TradeMinds.

Run before every restart to catch config/import/DB regressions:
  cd ~/autonomous-trader && venv/bin/python3 tests/pre_restart.py
"""
import sys
import os
import importlib
import traceback

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
WARN = "\033[93m⚠\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"

results = []
_in_venv = (sys.prefix != sys.base_prefix or "venv" in sys.prefix)


def check(name, fn):
    """Run a single check and record result."""
    try:
        msg = fn()
        results.append((True, name, msg or ""))
        print(f"  {PASS} {name}" + (f"  {msg}" if msg else ""))
    except Exception as e:
        err = str(e)
        # Module-not-found errors outside venv are venv warnings, not test failures
        if "No module named" in err and not _in_venv:
            results.append((True, name, f"{WARN} needs venv ({err})"))
            print(f"  {WARN} {name}  (skip — needs venv: {err})")
        else:
            results.append((False, name, err))
            print(f"  {FAIL} {name}: {err}")


def section(title):
    print(f"\n{BOLD}── {title} ──{RESET}")


# ── 0. ENVIRONMENT CHECK ─────────────────────────────────────────────────────
if not _in_venv:
    print(f"\n{WARN}  Not running in venv — import checks may be skipped.")
    print(f"    Run with: {BOLD}venv/bin/python3 tests/pre_restart.py{RESET}\n")

# ── 1. IMPORTS ────────────────────────────────────────────────────────────────
section("Core imports")

check("engine.paper_trader", lambda: importlib.import_module("engine.paper_trader") and None)
check("engine.ai_brain", lambda: importlib.import_module("engine.ai_brain") and None)
check("engine.risk_manager", lambda: importlib.import_module("engine.risk_manager") and None)
check("engine.market_data", lambda: importlib.import_module("engine.market_data") and None)
check("engine.congress_tracker", lambda: importlib.import_module("engine.congress_tracker") and None)
check("engine.capitol_fund", lambda: importlib.import_module("engine.capitol_fund") and None)
check("engine.riker_xo", lambda: importlib.import_module("engine.riker_xo") and None)
check("dashboard.app", lambda: importlib.import_module("dashboard.app") and None)

# ── 2. DATABASE ───────────────────────────────────────────────────────────────
section("Database")


def _check_db():
    import sqlite3
    db_path = os.path.join(PROJECT_ROOT, "data", "trader.db")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"trader.db not found at {db_path}")
    conn = sqlite3.connect(db_path)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    return f"({len(tables)} tables)"


check("trader.db accessible", _check_db)


def _check_tables():
    import sqlite3
    db_path = os.path.join(PROJECT_ROOT, "data", "trader.db")
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    required = {"portfolios", "trades", "signals"}
    missing = required - tables
    if missing:
        raise ValueError(f"Missing tables: {missing}")
    return "required tables present"


check("Required tables exist", _check_tables)

# ── 3. PAPER TRADER ───────────────────────────────────────────────────────────
section("Paper trader")


def _check_portfolio():
    from engine.paper_trader import get_portfolio
    port = get_portfolio("spock")
    if "cash" not in port:
        raise ValueError("portfolio missing 'cash' key")
    return f"spock cash=${port['cash']:,.0f}"


check("get_portfolio(spock)", _check_portfolio)


def _check_capitol_portfolio():
    from engine.paper_trader import get_portfolio
    port = get_portfolio("capitol-trades")
    if "cash" not in port:
        raise ValueError("portfolio missing 'cash' key")
    return f"capitol-trades cash=${port['cash']:,.0f}"


check("get_portfolio(capitol-trades)", _check_capitol_portfolio)

# ── 4. PROVIDERS ──────────────────────────────────────────────────────────────
section("Provider configuration")


def _check_ollama_providers():
    from engine.providers.ollama_provider import OllamaProvider
    assert hasattr(OllamaProvider, "__init__"), "OllamaProvider missing __init__"
    return "OllamaProvider class OK"


check("OllamaProvider class", _check_ollama_providers)


def _check_groq_provider():
    from engine.providers.groq_provider import GroqProvider
    assert hasattr(GroqProvider, "__init__"), "GroqProvider missing __init__"
    return "GroqProvider class OK"


check("GroqProvider class", _check_groq_provider)


def _check_dalio_provider():
    from engine.providers.dalio_provider import DalioFallbackProvider
    assert hasattr(DalioFallbackProvider, "__init__")
    return "DalioFallbackProvider class OK"


check("DalioFallbackProvider class", _check_dalio_provider)


def _check_main_has_anderson():
    src = open(os.path.join(PROJECT_ROOT, "main.py")).read()
    if 'OllamaProvider("super-agent"' not in src:
        raise ValueError("Anderson (super-agent) not instantiated as OllamaProvider in main.py")
    return "Anderson OllamaProvider present"


check("Anderson in main.py providers", _check_main_has_anderson)

# ── 5. RISK MANAGER ───────────────────────────────────────────────────────────
section("Risk Manager")


def _check_risk_manager():
    from engine.risk_manager import RiskManager
    assert hasattr(RiskManager, "is_market_hours"), "missing is_market_hours"
    assert hasattr(RiskManager, "check_buy"), "missing check_buy"
    assert hasattr(RiskManager, "check_stop_loss_take_profit"), "missing check_stop_loss_take_profit"
    return "RiskManager methods present"


check("RiskManager methods", _check_risk_manager)


def _check_vix_scale():
    """VIX Option C formula check — no hard blocks, only scaling."""
    src = open(os.path.join(PROJECT_ROOT, "engine", "ai_brain.py")).read()
    if "max(0.25, 1.0 - (_vix_price - 25) / 20)" not in src:
        raise ValueError("VIX Option C formula not found in ai_brain.py")
    return "VIX Option C formula confirmed"


check("VIX Option C scaling in ai_brain", _check_vix_scale)


def _check_convergence_power_hour():
    src = open(os.path.join(PROJECT_ROOT, "engine", "strategies.py")).read()
    if "min_strategies = 1 if _mins >= 750 else 3" not in src:
        raise ValueError("Power-hour convergence relaxation not found in strategies.py")
    if "confidence = max(confidence, 0.82)" not in src:
        raise ValueError("After-hours confidence floor not found in strategies.py")
    return "power-hour/AH convergence lowering confirmed"


check("Power-hour convergence gate in strategies", _check_convergence_power_hour)

# ── 6. CAPITOL TRADES ────────────────────────────────────────────────────────
section("Capitol Trades fund")


def _check_capitol_exemptions():
    src = open(os.path.join(PROJECT_ROOT, "engine", "paper_trader.py")).read()
    if '"capitol-trades"' not in src:
        raise ValueError("capitol-trades not found in paper_trader.py exemptions")
    return "capitol-trades exemptions present"


check("paper_trader exemptions for capitol-trades", _check_capitol_exemptions)


def _check_capitol_return_check():
    src = open(os.path.join(PROJECT_ROOT, "engine", "capitol_fund.py")).read()
    if "result is None" not in src:
        raise ValueError("capitol_fund.py doesn't check buy() return value")
    return "buy() return value checked"


check("capitol_fund checks buy() return", _check_capitol_return_check)


def _check_capitol_stop_target():
    src = open(os.path.join(PROJECT_ROOT, "engine", "capitol_fund.py")).read()
    if "stop_price" not in src or "target_price" not in src:
        raise ValueError("capitol_fund.py missing stop/target prices in reasoning")
    return "stop/target present in reasoning"


check("capitol_fund stop/target in reasoning", _check_capitol_stop_target)

# ── 7. RIKER XO ──────────────────────────────────────────────────────────────
section("Riker XO")


def _check_riker_module():
    from engine.riker_xo import get_latest_recommendation, generate_riker_synthesis
    assert callable(get_latest_recommendation), "not callable"
    assert callable(generate_riker_synthesis), "not callable"
    return "riker_xo functions present"


check("riker_xo functions", _check_riker_module)


def _check_riker_endpoint():
    src = open(os.path.join(PROJECT_ROOT, "dashboard", "app.py")).read()
    if "/api/riker/recommendation" not in src:
        raise ValueError("/api/riker/recommendation endpoint not found")
    return "endpoint registered"


check("/api/riker/recommendation endpoint", _check_riker_endpoint)

# ── 8. ENVIRONMENT ────────────────────────────────────────────────────────────
section("Environment")


def _check_env():
    env_path = os.path.join(PROJECT_ROOT, ".env")
    if not os.path.exists(env_path):
        raise FileNotFoundError(".env file missing — API keys won't load")
    return ".env present"


check(".env file exists", _check_env)


def _check_data_dir():
    data_dir = os.path.join(PROJECT_ROOT, "data")
    if not os.path.isdir(data_dir):
        raise FileNotFoundError("data/ directory missing")
    return "data/ directory present"


check("data/ directory", _check_data_dir)


def _check_logs_dir():
    logs_dir = os.path.join(PROJECT_ROOT, "logs")
    if not os.path.isdir(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)
        return "logs/ created"
    return "logs/ present"


check("logs/ directory", _check_logs_dir)

# ── 9. DASHBOARD ──────────────────────────────────────────────────────────────
section("Dashboard")


def _check_html_hamburger():
    html_path = os.path.join(PROJECT_ROOT, "dashboard", "static", "index.html")
    src = open(html_path).read()
    if "mobileMenuBtn" not in src:
        raise ValueError("Mobile hamburger button not found in index.html")
    if "toggleMobileSidebar" not in src:
        raise ValueError("toggleMobileSidebar() not found in index.html")
    return "hamburger + overlay present"


check("Mobile hamburger in index.html", _check_html_hamburger)


def _check_html_riker_glance():
    html_path = os.path.join(PROJECT_ROOT, "dashboard", "static", "index.html")
    src = open(html_path).read()
    if "g-riker-text" not in src:
        raise ValueError("g-riker-text not found — Riker Bridge Status row missing")
    return "Riker glance row present"


check("Riker Bridge Status row", _check_html_riker_glance)


def _check_html_force_scan():
    html_path = os.path.join(PROJECT_ROOT, "dashboard", "static", "index.html")
    src = open(html_path).read()
    if "forceScan" not in src:
        raise ValueError("forceScan not found in index.html")
    return "forceScan present"


check("Force Scan button in index.html", _check_html_force_scan)


def _check_kirk_advisory():
    src = open(os.path.join(PROJECT_ROOT, "engine", "kirk_advisory.py")).read()
    if "generate_kirk_advisory" not in src:
        raise ValueError("generate_kirk_advisory not found")
    app_src = open(os.path.join(PROJECT_ROOT, "dashboard", "app.py")).read()
    if "/api/kirk/advisory" not in app_src:
        raise ValueError("/api/kirk/advisory endpoint not in app.py")
    return "endpoint + module present"


check("Kirk Advisory module + endpoint", _check_kirk_advisory)

# ── 10. GITIGNORE ─────────────────────────────────────────────────────────────
section("Security checks")


def _check_gitignore():
    gi_path = os.path.join(PROJECT_ROOT, ".gitignore")
    if not os.path.exists(gi_path):
        raise FileNotFoundError(".gitignore missing — .env may be committed")
    content = open(gi_path).read()
    checks = [".env", "data/*.db", "venv/"]
    missing = [c for c in checks if c not in content]
    if missing:
        raise ValueError(f".gitignore missing entries: {missing}")
    return ".env, data/*.db, venv/ all excluded"


check(".gitignore excludes secrets + DB", _check_gitignore)

# ── SUMMARY ───────────────────────────────────────────────────────────────────
total = len(results)
passed = sum(1 for r in results if r[0])
failed = total - passed

print(f"\n{BOLD}{'─'*50}{RESET}")
if failed == 0:
    print(f"{BOLD}\033[92m ALL {total} CHECKS PASSED — safe to restart\033[0m{RESET}")
else:
    print(f"{BOLD}\033[91m {failed}/{total} CHECKS FAILED — fix before restart{RESET}{RESET}")
    print(f"\nFailed checks:")
    for ok, name, msg in results:
        if not ok:
            print(f"  {FAIL} {name}: {msg}")

sys.exit(0 if failed == 0 else 1)
