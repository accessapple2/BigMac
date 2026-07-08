import warnings
import logging as _logging
logger = _logging.getLogger(__name__)
# Suppress yfinance FutureWarning spam (auto_adjust default change) and urllib3 LibreSSL notice
warnings.filterwarnings("ignore", category=FutureWarning, module="yfinance")
warnings.filterwarnings("ignore", message=".*auto_adjust.*")
warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")
warnings.filterwarnings("ignore", message=".*LibreSSL.*")
# Silence httpx INFO logs that echo every 429 response before our exception handler fires
_logging.getLogger("httpx").setLevel(_logging.WARNING)
_logging.getLogger("openai").setLevel(_logging.WARNING)
import threading
import schedule
import time
import sqlite3
import os
from datetime import datetime
from datetime import datetime as _dt
from engine.market_calendar import az_now  # Arizona-now via zoneinfo (corruption-proof) — HM-TZ-AZNOW 2026-06-01
from rich.console import Console
from rich.panel import Panel

# HM-LOG-DATE-PREFIX: monkey-patch rich.console.Console.__init__ to inject
# log_time_format="[%Y-%m-%d %H:%M:%S]" so every console.log() line in
# trader.log carries an ISO date prefix (was time-only [HH:MM:SS]). Must
# import BEFORE the module-level `console = Console()` below and BEFORE
# any engine.* imports so their own `console = Console()` calls also pick
# up the patched __init__ at module-load time. See engine/_rich_patch.py
# for trade-offs and rollback instructions.
import engine._rich_patch  # noqa: F401  (intentional import for side effect)

# Load .env before anything else
from dotenv import load_dotenv
load_dotenv(override=True)

# Hoist Ollie Box URL to module scope so all scheduled jobs and initialize_dayblade()
# can reference it directly. Previously only imported inside initialize_arena() (local
# scope), causing NameError in the first scan cycle of every restart. P8 fix 2026-04-21.
OLLIE_URL = os.getenv("OLLIE_URL", "http://192.168.1.166:11434")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")

# Monkey-patch sqlite3.connect to always use a 30s busy timeout.
# This prevents "database is locked" errors when 13+ AI models write concurrently.
_original_sqlite3_connect = sqlite3.connect

# === HM-BQ-instr === [HM-BQ-INSTR-EARLY: moved to top 2026-05-13] per-handler wall-time decorator (Phase 1)
# Logs wall time for scheduler handlers to identify which is contributing
# to the cadence drift documented in HM-AS-β. Logs only when wall>1.0s
# to keep signal-to-noise high. Output routes via logger.info to
# trader_error.log (alongside the [HM-AS-β] cadence-drift warnings).
import functools as _hm_bq_functools
import time as _hm_bq_time
def _hm_bq_instr(name):
    def _deco(fn):
        @_hm_bq_functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            _t0 = _hm_bq_time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                _wall = _hm_bq_time.perf_counter() - _t0
                if _wall > 1.0:
                    console.log(f'[HM-BQ-instr] {name} wall={_wall:.3f}s')
        return _wrapper
    return _deco
# === /HM-BQ-instr ===


def _patched_connect(*args, **kwargs):
    kwargs.setdefault("timeout", 30)
    conn = _original_sqlite3_connect(*args, **kwargs)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn
sqlite3.connect = _patched_connect

console = Console()
arena = None
dayblade = None
_news_counter = 0
_vix_alerted_today = False
_dashboard_started = threading.Event()
_dashboard_error = None


def is_extended_or_market_hours() -> bool:
    """Return True during pre-market, regular hours, and after-hours (Mon–Fri).
    Pre-market:  4:00 AM – 9:30 AM ET  (2:00 AM – 7:30 AM MST)
    Regular:     9:30 AM – 4:00 PM ET  (7:30 AM – 2:00 PM MST)
    After-hours: 4:00 PM – 8:00 PM ET  (2:00 PM – 6:00 PM MST)
    Use this gate for DATA REFRESH only — trading logic uses is_market_hours().
    """
    import pytz
    from datetime import datetime as _dt
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:  # Saturday / Sunday
        return False
    h = now.hour + now.minute / 60.0
    return 2.0 <= h < 18.0  # 2:00 AM – 6:00 PM MST


def initialize_arena():
    from config import (
        OPENAI_API_KEY, OPENAI_CODEX_MODEL, OPENAI_CODEX_MINI_MODEL,
        GEMINI_API_KEY, GROK_API_KEY, GROQ_API_KEY,
        OLLAMA_MODEL, OLLAMA_URL, OLLIE_URL, MLX_URL, MLX_MODEL
    )
    from engine.ai_brain import Arena
    from engine.risk_manager import RiskManager
    from engine.providers.ollama_provider import OllamaProvider  # noqa: F401 (lazy-imported inside agent_routing)
    from engine.agent_routing import build_all_providers

    # HM-CN Phase 2 (Option B) 2026-05-17: providers built from ai_players DB
    # (single source of truth). config.AI_PLAYERS supplies static metadata
    # (url, timeout, local_redirect flag for Free-Models-First). halt_mode='full'
    # agents are skipped. skip_ids excludes agents handled by non-Ollama
    # provider classes below (GroqProvider for ollama-llama).
    #
    # The silent-bypass class is now structurally impossible: changing
    # ai_players.model_id + restart updates production routing. main.py no
    # longer hardcodes per-agent model assignments. setup_db.py:289-322
    # still enforces canonical model_ids on startup (HM-BN doctrine); change
    # those lines in lockstep with any future runtime UPDATE.
    providers = build_all_providers(
        default_url=OLLIE_URL,
        default_timeout=180,
        skip_ids={"ollama-llama"},  # handled by GroqProvider when GROQ_API_KEY set
    )
    console.log(f"[green]HM-CN Phase 2 routing: {len(providers)} providers built from ai_players DB")

    if GROQ_API_KEY:
        from engine.providers.groq_provider import GroqProvider
        providers.append(GroqProvider(GROQ_API_KEY, "ollama-llama", "llama-3.3-70b-versatile", "Llama 3.3 70B"))

    from config import OPTIONS_MAX_PCT, OPTIONS_TOTAL_MAX_PCT
    risk = RiskManager(options_max_pct=OPTIONS_MAX_PCT, options_total_max_pct=OPTIONS_TOTAL_MAX_PCT)
    return Arena(providers, risk)


def initialize_dayblade():
    from config import OLLAMA_MODEL, OLLAMA_URL, OLLIE_URL
    from engine.providers.ollama_provider import OllamaProvider
    from engine.dayblade import DayBladeScanner, ensure_player

    ensure_player()
    provider = OllamaProvider(player_id="dayblade-0dte", model="0xroyce/plutus", url=OLLIE_URL, timeout=300)
    return DayBladeScanner(provider)


def _stagger_schedule_jobs() -> None:
    """Spread interval checks after boot so same-cadence jobs do not spike together.

    This only shifts the initial next_run within a small bounded window; the job
    functions still enforce their own exact time gates and cooldown rules.
    """
    import datetime as _dt

    spacing_by_unit = {
        "seconds": 2,
        "minutes": 12,
        "hours": 45,
    }
    buckets = {}
    now = _dt.datetime.now()

    for job in schedule.jobs:
        interval = getattr(job, "interval", None)
        unit = getattr(job, "unit", "")
        if not interval or unit not in spacing_by_unit:
            continue

        key = (unit, interval)
        slot = buckets.get(key, 0)
        buckets[key] = slot + 1

        max_offset = max(1, int(interval * 0.4))
        spacing = spacing_by_unit[unit]
        offset = min(slot * spacing, max_offset)
        if offset <= 0:
            continue

        if unit == "seconds":
            job.next_run = now + _dt.timedelta(seconds=offset)
        elif unit == "minutes":
            job.next_run = now + _dt.timedelta(minutes=offset)
        elif unit == "hours":
            job.next_run = now + _dt.timedelta(minutes=offset)


_last_scan_time = 0
_scan_lock = threading.Lock()
# HM-SCAN-LIVENESS-WATCHDOG 2026-07-08: last COMPLETED scan (not just last
# attempt -- _last_scan_time above bumps on every no-tier-due pass, which is
# what made the 09:16-09:55 gap invisible in logs). Seeded at import time so
# a slow startup doesn't false-alarm before the first real scan.
_last_scan_complete_ts = time.time()
_scan_liveness_alert_sent = False

# === MARKET HOURS TASK THROTTLE (Opt 5) ===
# Non-essential tasks run at reduced frequency during market hours
# to free Ollama VRAM / CPU for live trading scans.
_MARKET_HOURS_DISABLED: frozenset = frozenset({
    "run_strategy_race",
    "run_signal_scorecard",
    "run_trend_forecast",
    "run_strategy_presets",
    "run_auto_screener",
})
_market_throttle_last: dict = {}  # task_name → last_run_epoch


def should_run_task(task_name: str, throttle_mins: int = 30) -> bool:
    """Return False for non-essential tasks during active market hours.

    Non-essential tasks are throttled to ``throttle_mins`` interval during
    regular market session and power hour to prioritise Ollama bandwidth.
    """
    import time as _trt
    from engine.risk_manager import RiskManager
    session = RiskManager.is_market_hours()
    if session in ("market", "power_hour") and task_name in _MARKET_HOURS_DISABLED:
        now = _trt.time()
        if now - _market_throttle_last.get(task_name, 0) < throttle_mins * 60:
            return False
        _market_throttle_last[task_name] = now
    return True

# === SCAN TIER DEFINITIONS ===
# Agents sorted within each tier by model_id to minimise Ollama load/unload swaps
# (ai_brain.py re-sorts by _MODEL_RUN_ORDER internally, but grouping here keeps
#  the intent explicit).

# Tier 1 — Bridge Crew: core decision-makers, every 30 min during market hours
# HM-AGENT-RULES-CONSOLIDATION 2026-07-04 (AGENT-RULES-REVIEW-2026-07-03.md
# Inconsistency #8): dayblade-sulu (exit_only since 2026-03-31 — Sulu is now
# Iron Condor King only, see crew_specialization.py), super-agent (halt_mode=
# 'full', shelved), deepseek-7b-grok4 (halt_mode='full', Door-1 kill-gate cut
# 2026-06-19/20), and ollama-coder (halt_mode='full', same cut) removed — they
# were never scanned anyway (build_all_providers skips 'full'; exit_only never
# opens new positions), but a stale roster here misleads anyone reading this
# file about who's actually live. mlx-qwen3 kept even though it's also
# halt_mode='full' today — reopening it is a Batch-1 candidate (persona/naming
# fix is a prereq, see AGENT-RULES-REVIEW Inconsistency #13/#18), not a dead
# entry to prune.
_SCAN_TIER1: frozenset = frozenset({
    "mlx-qwen3",         # Chekov roster label / Ensign Ro in CREW_MANIFEST (phi3:mini) — halt_mode='full', Batch-1 reopen candidate
})

# Tier 2 — Department Heads: secondary signals, every 2 hours
_SCAN_TIER2: frozenset = frozenset({
    "ollama-plutus",     # McCoy         (ministral-3:3b — HM-BN.1 canonical; was mistral:7b)
    "ollama-qwen3",      # Scotty        (phi3:mini)
    # ── benched ADVISORY_CREW agents removed from this scan roster ──────────────
    # qwen3-8b-flash (Worf) REMOVED 2026-05-29 (HM-WORF-DRIFT-RECONCILE): benched
    #   S6.1 → ADVISORY_CREW (bridge-vote only), non-emitting since 2026-05-07.
    # HM-ADVISORY-CREW-DRIFT-SWEEP 2026-05-29: removed 4 more ADVISORY_CREW (bridge-
    #   vote only) agents that lied to the scanner roster — options-sosnoff (Troi),
    #   energy-arnold (Trip), ollama-local (Geordi), ollama-llama (Uhura). All benched
    #   S6.x, all non-emitting since early May (last signals 2026-05-02..05-07); all
    #   keep ai_players active for WR bridge-voting (war_room.py skips
    #   halt_mode!='active'/is_active=0). Same disease/fix as Worf. Only the true
    #   active scanners (McCoy, Scotty) remain in TIER2.
})

# Tier 3 — Cadets: market open + close only (learning, not real-time)
_SCAN_TIER3: frozenset = frozenset({
    "qwen3-8b-4o",            # (phi3:mini)
    "qwen3-8b-sonnet",     # (phi3:mini)
    "qwen3-14b-grok3",            # (phi3:mini)
    "ollama-gemma27b",   # (phi3:mini)
    "ollama-glm4",       # (phi3:mini)
    "ollama-kimi",       # (phi3:mini)
    "qwen3-8b-o3",            # (deepseek-r1:7b)
    "ollama-deepseek",   # (deepseek-r1:7b)
    "qwen-coder-haiku",      # (qwen2.5-coder:7b)
    "cto-grok42",        # (qwen2.5-coder:7b)
})

_TIER1_INTERVAL = 30 * 60       # 30 minutes
_TIER2_INTERVAL = 120 * 60      # 2 hours
_TIER3_INTERVAL = 4 * 60 * 60   # min gap between tier3 runs (open → close separation)
_tier_last_scan: dict = {1: 0.0, 2: 0.0, 3: 0.0}

# Per-category scan frequency reference (seconds).  Informational + used by earnings-day logic.
_sectionIntervalDefs: dict = {
    "tier1_agents":    30 * 60,          # 30 min  — core agent signals
    "tier2_agents":   120 * 60,          # 2 hrs   — secondary agents
    "tier3_agents":     4 * 60 * 60,     # open/close only
    "deep_scan":       30 * 60,          # 30 min  — universe-wide strategy scan
    "gex_refresh":     15 * 60,          # 15 min
    "vix_check":       15 * 60,          # 15 min
    "premarket_gaps":  15 * 60,          # 15 min
    "ah_scanner":      30 * 60,          # 30 min  — after-hours earnings movers
    "intel_report":    "06:00 AZ daily", # morning brief + ntfy push
    "earnings_day":     5 * 60,          # 5 min   — earnings-day tickers (priority rescan)
}

# Earnings tickers injected at 6 AM AZ — rescanned every 5 min during market hours
_earnings_today_tickers: set = set()


def _tier3_window_open() -> bool:
    """True during market-open (6:30–7:00 AM MST) and pre-close (12:45–1:30 PM MST) windows."""
    import pytz
    from datetime import datetime as _dt
    az = pytz.timezone("US/Arizona")
    mins = az_now().hour * 60 + az_now().minute
    # Open: 6:30–7:00 AM MST = 390–420 min; Close: 12:45–1:30 PM MST = 765–810 min
    return (390 <= mins < 420) or (765 <= mins < 810)

def _get_scan_interval():
    """Return the appropriate scan interval based on current market session.

    Dilithium Crystal Protocol v2 — Pre-market intelligence starts at 10:30 PM MST (1:30 AM ET).

    HM-AGENT-RULES-CONSOLIDATION 2026-07-04 (Inconsistency #19): this docstring
    used to promise market-hours=3min/180s and power-hour=90s, but v3
    (2026-03-23, config.py comment "Widened to cut API costs ~60%") set both
    SCAN_INTERVAL_MARKET and SCAN_INTERVAL_POWER_HOUR to 300s and the docstring
    was never updated. `_sectionIntervalDefs` above and config.py are the
    values actually in effect — this docstring now matches them.

    Schedule (all MST, MST = ET - 3 during EDT):
      10:30 PM - 1:00 AM MST (1:30 AM - 4:00 AM ET): Early pre-market — every 5 min (300s)
      1:00 AM - 6:30 AM MST  (4:00 AM - 9:30 AM ET): Full pre-market — every 5 min (300s)
      6:30 AM - 12:00 PM MST (9:30 AM - 3:00 PM ET): Market hours — every 5 min (300s, SCAN_INTERVAL_MARKET)
      12:00 PM - 1:30 PM MST (3:00 PM - 4:30 PM ET): Power hour — every 5 min (300s, SCAN_INTERVAL_POWER_HOUR)
      1:30 PM - 5:00 PM MST  (4:30 PM - 8:00 PM ET): After hours — every 10 min (600s)
      5:00 PM - 9:00 PM MST  (8:00 PM - 12:00 AM ET): Evening — every 30 min (1800s)
      9:00 PM - 10:30 PM MST (12:00 AM - 1:30 AM ET): Overnight — every 30 min (1800s)
      Weekends: every 1 hour (3600s)
    """
    from config import (SCAN_INTERVAL_MARKET, SCAN_INTERVAL_POWER_HOUR,
                        SCAN_INTERVAL_EXTENDED, SCAN_INTERVAL_OVERNIGHT, SCAN_INTERVAL_WEEKEND)
    import pytz
    from datetime import datetime as _dt

    az = pytz.timezone("US/Arizona")
    now = az_now()
    hour, minute = now.hour, now.minute
    mins = hour * 60 + minute
    day = now.weekday()  # 0=Mon, 6=Sun

    # Weekends: 1 hour scans (but allow Sunday night pre-market from 10:30 PM)
    if day == 6 and mins >= 1350:  # Sunday after 10:30 PM MST — pre-market starts
        return 300  # 5 min
    if day >= 5:  # Saturday or Sunday before 10:30 PM
        return SCAN_INTERVAL_WEEKEND  # 3600s

    # Weeknight schedule (MST times)
    if mins >= 1350 or mins < 60:
        # 10:30 PM - 1:00 AM MST: Early pre-market (5 min)
        return 300
    if 60 <= mins < 390:
        # 1:00 AM - 6:30 AM MST: Full pre-market (5 min — sequential Ollama needs breathing room)
        return 300
    if 390 <= mins < 720:
        if 540 <= mins < 600:
            # Lunch lull 9:00–10:00 AM MST (12:00–1:00 PM ET) — scan 10 min (low volume)
            return 600
        # 6:30 AM - 12:00 PM MST: Market hours (5 min — v3 2026-03-23 cost cut)
        return SCAN_INTERVAL_MARKET  # 300s
    if 720 <= mins < 810:
        # 12:00 PM - 1:30 PM MST: Power hour (5 min — v3 2026-03-23 cost cut)
        return SCAN_INTERVAL_POWER_HOUR  # 300s
    if 810 <= mins < 1020:
        # 1:30 PM - 5:00 PM MST: After hours (10 min)
        return SCAN_INTERVAL_EXTENDED  # 600s
    # 5:00 PM - 10:30 PM MST: Evening/overnight (30 min)
    return SCAN_INTERVAL_OVERNIGHT  # 1800s


# HM-AS-β §C instrumentation (2026-05-28) — read-only _scan_lock telemetry.
# Measures lock contention: hold duration (scan-only vs scan+WR) + how often each
# tier is "due but skipped" because the lock was held. Pure telemetry, zero
# behavior change. Routes to trader.log. Soak target: confirm or kill §C with data.
_scan_skip_due_by_tier = {1: 0, 2: 0, 3: 0}


@_hm_bq_instr("run_scanner")
def run_scanner():
    global arena, _news_counter, _last_scan_time, _tier_last_scan
    import time as _time
    from engine.fleet_halt import check_or_bail
    if check_or_bail("run_scanner"):
        return

    # Prevent scan stacking — skip if previous scan still running
    if not _scan_lock.acquire(blocking=False):
        # HM-AS-β §C: record which tiers are due-but-skipped because lock is held
        _now_skip = _time.time()
        _due = []
        if _now_skip - _tier_last_scan[1] >= _TIER1_INTERVAL:
            _due.append("T1"); _scan_skip_due_by_tier[1] += 1
        if _now_skip - _tier_last_scan[2] >= _TIER2_INTERVAL:
            _due.append("T2"); _scan_skip_due_by_tier[2] += 1
        if _tier3_window_open() and _now_skip - _tier_last_scan[3] >= _TIER3_INTERVAL:
            _due.append("T3"); _scan_skip_due_by_tier[3] += 1
        _msg = "[yellow]Scan skipped — previous scan still running"
        if _due:
            _msg += (f" | [HM-AS-β-C] due-but-skipped: {','.join(_due)}"
                     f" (cum T1={_scan_skip_due_by_tier[1]} T2={_scan_skip_due_by_tier[2]} T3={_scan_skip_due_by_tier[3]})")
        console.log(_msg)
        return

    # From here we OWN the lock — every code path must release it exactly once.
    _scan_lock_acq_ts = _time.time()  # HM-AS-β §C: lock-hold start
    interval = _get_scan_interval()
    if interval is None:
        _scan_lock.release()
        return  # Market closed

    now = _time.time()
    if now - _last_scan_time < interval:
        _scan_lock.release()
        return  # Cooldown not met
    _last_scan_time = now

    if arena is None:
        arena = initialize_arena()
    from engine.universe import get_active_universe

    # Fetch news BEFORE each scan so the AI has fresh headlines
    _news_counter += 1
    if _news_counter % 5 == 1:  # Every 5 cycles
        try:
            from engine.news_fetcher import fetch_news
            fetch_news(get_active_universe(), max_per_symbol=5)
            console.log("[cyan]News updated")
        except Exception as e:
            console.log(f"[red]News error: {e}")

    # ── Tiered scan scheduling ──────────────────────────────────────────
    # Each tier has its own cooldown. We build the union of all due tiers,
    # then pass the resulting player_ids set to arena.run_scan().
    # Ollama will group them by model_id internally to minimise swaps.
    active_players: set = set()
    tier_labels: list = []

    if now - _tier_last_scan[1] >= _TIER1_INTERVAL:
        active_players |= _SCAN_TIER1
        _tier_last_scan[1] = now
        tier_labels.append("T1:BridgeCrew")

    if now - _tier_last_scan[2] >= _TIER2_INTERVAL:
        active_players |= _SCAN_TIER2
        _tier_last_scan[2] = now
        tier_labels.append("T2:DeptHeads")

    if _tier3_window_open() and now - _tier_last_scan[3] >= _TIER3_INTERVAL:
        active_players |= _SCAN_TIER3
        _tier_last_scan[3] = now
        tier_labels.append("T3:Cadets")

    if not active_players:
        _scan_lock.release()
        return  # No tier is due this cycle

    tier_label = " + ".join(tier_labels)
    _captured_arena    = arena
    _captured_stocks   = list(get_active_universe())
    _captured_players  = frozenset(active_players)
    _captured_counter  = _news_counter
    _captured_acq_ts   = _scan_lock_acq_ts  # HM-AS-β §C: lock-hold start
    _captured_mode     = "scan+WR" if (_captured_counter % 3 == 0) else "scan-only"
    console.log(f"[cyan]Market scan triggered [{tier_label}] — {len(active_players)} agents (interval={interval}s)")

    # HM-AS-β §C in-flight heartbeat (2026-05-29): the held-line below only logs
    # on COMPLETION, so a long/hung run_scan is invisible until it returns (it may
    # never — a scan stalled ~14min holding the lock on 2026-05-29, starving T1/T2).
    # This read-only heartbeat surfaces the lock-hold duration every 60s WHILE the
    # scan is in flight — pure logging, zero behavior change. Stops the instant the
    # scan thread sets _scan_done_evt in its finally. (run_scan watchdog = separate
    # behavior-changing item; this just makes the next stall visible within 60s.)
    _scan_done_evt = threading.Event()

    def _hold_heartbeat():
        while not _scan_done_evt.wait(60):
            _inflight = _time.time() - _captured_acq_ts
            # HM-RUN-SCAN-WATCHDOG Loop 1: name the in-flight run_scan subcall so a
            # stall pinpoints WHICH phase is hung (not just total hold duration).
            _phase = "?"
            try:
                from engine.ai_brain import _CURRENT_SCAN_PHASE as _csp, _hm_scan_time as _cst
                _phase = (f"{_csp.get('name','?')} "
                          f"({_cst.perf_counter() - _csp.get('started', _cst.perf_counter()):.0f}s in phase)")
            except Exception:
                pass
            console.log(f"[yellow][HM-AS-β-C] scan_lock HELD-INFLIGHT {_inflight:.0f}s "
                        f"({_captured_mode}) — phase: {_phase}")
    threading.Thread(target=_hold_heartbeat, daemon=True, name="scan_hold_hb").start()

    # Run arena.run_scan() in a background thread so the scheduler main thread
    # is never blocked.  The lock is released by the thread when done.
    def _arena_scan_thread():
        try:
            _captured_arena.run_scan(_captured_stocks, player_ids=_captured_players)
            # Trigger War Room after every 3rd scan cycle (~9 min intervals)
            if _captured_counter % 3 == 0:
                try:
                    run_war_room()
                except Exception as e:
                    console.log(f"[red]War Room post-scan error: {e}")
        except Exception as e:
            console.log(f"[red]Scan error: {e}")
        finally:
            _scan_lock.release()
            _scan_done_evt.set()  # HM-AS-β §C: stop the in-flight heartbeat
            # HM-AS-β §C: lock-hold duration + whether WR ran inside this thread
            _hold = _time.time() - _captured_acq_ts
            console.log(f"[cyan][HM-AS-β-C] scan_lock held {_hold:.1f}s ({_captured_mode})")
            global _last_scan_complete_ts
            _last_scan_complete_ts = _time.time()  # HM-SCAN-LIVENESS-WATCHDOG

    threading.Thread(target=_arena_scan_thread, daemon=True, name="arena_scanner").start()


def check_scan_liveness():
    """HM-SCAN-LIVENESS-WATCHDOG 2026-07-08: alarm on a silent scan stall.

    2026-07-08 finding: `_last_scan_time` (the global cooldown gate in
    run_scanner) advances on every scheduler tick that clears the interval
    check, even when zero tiers are due -- that no-op pass is never logged.
    So a real stall (lock stuck, worker wedged) and normal "T1 not due yet"
    both look identical from the logs: silence. This watchdog instead tracks
    `_last_scan_complete_ts`, stamped only when a scan thread actually
    finishes, and pages if that goes stale past 2x the T1 tier interval --
    the fastest cadence, so the most conservative bound.
    """
    global _scan_liveness_alert_sent
    from engine.fleet_halt import check_or_bail
    if check_or_bail("check_scan_liveness"):
        return
    if _get_scan_interval() is None:
        return  # market closed — no cadence to violate
    age = time.time() - _last_scan_complete_ts
    threshold = 2 * _TIER1_INTERVAL
    if age > threshold:
        if not _scan_liveness_alert_sent:
            try:
                from engine.alert_channels import send_alert, AlertLevel
                send_alert(
                    f"Scan cycle stalled — last completed scan {age/60:.1f}min ago "
                    f"(threshold {threshold/60:.0f}min = 2x T1 interval)",
                    level=AlertLevel.WARNING,
                    alert_type="scan_liveness",
                    title="Scan cycle stall",
                    source="sys_scan_liveness",
                )
            except Exception as e:
                console.log(f"[red][HM-SCAN-LIVENESS] send_alert failed: {e}")
            console.log(f"[red][HM-SCAN-LIVENESS] ALERT — age={age:.0f}s > threshold={threshold:.0f}s")
            _scan_liveness_alert_sent = True
    elif _scan_liveness_alert_sent:
        console.log(f"[green][HM-SCAN-LIVENESS] recovered — age={age:.0f}s")
        _scan_liveness_alert_sent = False


_dayblade_tick_count = 0

@_hm_bq_instr("run_dayblade")
def run_dayblade():
    # HM-DAYBLADE-SCHEDULER-AUDIT 2026-05-27: log every outer-wrapper invocation
    # to disambiguate "scheduler not firing" vs "scheduler firing but run_scan
    # gates early-return". If ticks increment but no positions open/no signals
    # land, blame is downstream in engine/dayblade.py::run_scan (most likely
    # the is_paused=1 gate at line ~921). Pure observability, no behavior change.
    global dayblade, _dayblade_tick_count
    _dayblade_tick_count += 1
    console.log(f"[cyan][DAYBLADE-CYCLE-START] tick={_dayblade_tick_count}")
    if dayblade is None:
        dayblade = initialize_dayblade()
    try:
        # DayBlade handles its own power hour throttling internally (15s during 2-3:30 PM)
        dayblade.run_scan()
    except Exception as e:
        console.log(f"[red]DayBlade error: {e}")


_last_ma_regime: str = ""

@_hm_bq_instr("run_ma_regime_update")
def run_ma_regime_update():
    """Refresh 8/21 MA cross regime every 15 minutes, log any regime change."""
    global _last_ma_regime
    try:
        from engine.regime_ma import detect_ma_cross_regime
        r = detect_ma_cross_regime()
        cross_info = ""
        if r.get("cross_date") and r.get("cross_days_ago") is not None:
            cross_info = f" | cross {r['cross_date']} ({r['cross_days_ago']}d ago)"
        console.log(
            f"[cyan]8/21 Regime: {r['regime']} "
            f"SPY ${r.get('spy_close',0)} 8MA=${r.get('spy_ma8',0)} 21MA=${r.get('spy_ma21',0)}"
            f" size={r.get('size_modifier',1):.0%}{cross_info}"
        )
        new_regime = r.get("regime", "")
        if new_regime and _last_ma_regime and new_regime != _last_ma_regime:
            try:
                from engine.ntfy import notify_regime_change
                notify_regime_change(
                    _last_ma_regime, new_regime,
                    spy_close=r.get("spy_close", 0),
                    spy_ma8=r.get("spy_ma8", 0),
                    spy_ma21=r.get("spy_ma21", 0),
                )
            except Exception:
                pass
        if new_regime:
            _last_ma_regime = new_regime
    except Exception as e:
        console.log(f"[yellow]MA regime update error: {e}")


@_hm_bq_instr("run_vix_check")
def run_vix_check():
    """Check VIX every 5 minutes, alert on spike > 5%."""
    global _vix_alerted_today
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        _vix_alerted_today = False  # Reset for next day
        return
    try:
        from engine.vix_monitor import check_vix_spike
        spike = check_vix_spike(threshold_pct=5.0)
        if spike and not _vix_alerted_today:
            from engine.telegram_alerts import alert_vix_spike
            alert_vix_spike(spike["price"], spike["change_pct"])
            _vix_alerted_today = True
            console.log(f"[bold red]VIX SPIKE ALERT sent: {spike['price']:.2f} ({spike['change_pct']:+.1f}%)")
        elif spike is None:
            from engine.vix_monitor import get_vix_status
            vix = get_vix_status()
            if vix and vix.get("price"):
                console.log(f"[dim]VIX: {vix['price']:.2f} ({vix['change_pct']:+.1f}%)")
    except Exception as e:
        console.log(f"[red]VIX check error: {e}")


@_hm_bq_instr("run_earnings_check")
def run_earnings_check():
    """Check earnings calendar once per hour."""
    try:
        from engine.universe import get_active_universe
        from engine.earnings_calendar import get_earnings_warnings
        from engine.telegram_alerts import alert_earnings_upcoming

        _EARNINGS_MEGA = [
            "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AMD","PLTR","CRM",
            "NFLX","AVGO","COST","LLY","JPM","BAC","GS","V","MA","UNH","JNJ",
            "PFE","ABBV","XOM","CVX","COP","WMT","HD","TGT","DIS","CMCSA",
            "BA","CAT","GE","RTX","COIN","SQ","HOOD",
        ]
        upcoming = get_earnings_warnings(list(set(get_active_universe()) | set(_EARNINGS_MEGA)))
        if upcoming:
            symbols = [e["symbol"] for e in upcoming]
            console.log(f"[yellow]Earnings next 7 days: {', '.join(symbols)}")
            # Only send Telegram alert for stocks reporting today or tomorrow
            urgent = [e for e in upcoming if e["days_until"] <= 1]
            if urgent:
                alert_earnings_upcoming(urgent)
    except Exception as e:
        console.log(f"[red]Earnings check error: {e}")


_journal_written_today = False

@_hm_bq_instr("run_journal")
def run_journal():
    """Write daily journal entries — runs during market hours and post_market.
    Each AI gets one entry per day (duplicate check in generate_journal_entry).
    """
    global arena, _journal_written_today
    from engine.risk_manager import RiskManager
    session = RiskManager.is_market_hours()

    # Reset flag overnight so it fires again next day
    if not session:
        _journal_written_today = False
        return

    # Only run during market or post_market (not pre_market)
    if session == "pre_market":
        return

    if _journal_written_today:
        return

    if arena is None:
        return

    try:
        from engine.ai_journal import generate_journal_entry, save_journal_entry
        # HM-AQ-β v3 2026-05-07: bulk endpoint (~25× faster than per-symbol loop).
        from engine.market_data import get_bulk_prices
        from engine.universe import get_active_universe

        prices = get_bulk_prices(get_active_universe())

        wrote_any = False
        for pid, provider in arena.providers.items():
            try:
                entry = generate_journal_entry(provider, pid, prices)
                if entry:
                    save_journal_entry(pid, entry)
                    console.log(f"[magenta]{pid} journal: {entry[:80]}...")
                    wrote_any = True
            except Exception as e:
                console.log(f"[red]{pid} journal error: {e}")

        if wrote_any:
            _journal_written_today = True
            console.log("[green]AI journals written for the day")
    except Exception as e:
        console.log(f"[red]Journal error: {e}")


@_hm_bq_instr("run_gex_snapshot_refresh")
def run_gex_snapshot_refresh():
    """HM-GEX-CANONICAL 2026-05-31: refresh the canonical GEX (Polygon, engine.options_flow_gex)
    in-process cache every 15 min during RTH. Serves ALL Bridge GEX displays via /api/gex-snapshot
    + the adapter endpoints. The daily-close flow_gex.db write (collector cron) stays the clean
    one-row/day validation series — intraday recomputes never touch it. Observation-only."""
    try:
        from engine.market_calendar import is_us_market_open
        if not is_us_market_open():
            return
        from engine import options_flow_gex
        options_flow_gex.refresh_latest(("SPY", "QQQ"))
    except Exception as e:
        console.log(f"[red]GEX snapshot refresh error: {type(e).__name__}: {e}")


@_hm_bq_instr("run_gex_refresh")
def run_gex_refresh():
    """[DORMANT — HM-GEX-CANONICAL] legacy CBOE/gex_scanner refresh; no longer scheduled."""
    try:
        from engine.gex_scanner import refresh_gex_cache
        result = refresh_gex_cache()
        try:
            from engine.signal_poster import post_to_9000
            post_to_9000("GEX_REFRESH", {
                "summary": "GEX cache refreshed",
                "result": str(result)[:200] if result else "ok",
            })
        except Exception:
            pass
    except Exception as e:
        console.log(f"[red]GEX refresh error: {e}")


# ---------------------------------------------------------------------------
# GEX Overlay + Battle Station scheduler runners
# ---------------------------------------------------------------------------

@_hm_bq_instr("run_gex_overlay_update")
def run_gex_overlay_update():
    """Update GEX Overlay DB levels every 15 min during any market session."""
    try:
        from engine.gex_overlay import update_all_gex_levels
        update_all_gex_levels()
    except Exception as e:
        console.log(f"[red]GEX Overlay update error: {e}")


@_hm_bq_instr("run_morning_briefing")
def run_morning_briefing():
    """Generate morning levels at 6:25 AM MST (pre-market)."""
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:  # Skip weekends
        return
    if now.hour != 6 or now.minute > 30:
        return
    try:
        from engine.battle_station import generate_morning_briefing
        results = generate_morning_briefing()
        console.log(f"[cyan]Battle Station: morning briefing generated for {list(results.keys())}")
    except Exception as e:
        console.log(f"[red]Morning briefing error: {e}")


@_hm_bq_instr("run_archer_morning_briefing")
def run_archer_morning_briefing():
    """Phase 3.6 — Comprehensive Archer briefing at 6:00 AM AZ (9:00 AM ET)."""
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:  # Skip weekends
        return
    if now.hour != 6 or now.minute > 15:  # Fire 6:00–6:15 AM AZ window
        return
    try:
        # HM-ARCHER-REBUILD: Captain Archer (plutus-v1) synthesizes a fresh
        # briefing from every live surface. engine.morning_briefing remains a
        # data source Archer can read — it is no longer the briefing output.
        import sqlite3 as _sql
        import requests as _rq
        from engine.archer.brain import morning_briefing as _archer_brief
        briefing = (_archer_brief() or "").strip()
        if not briefing:
            console.log("[yellow]Archer briefing: plutus returned empty — skipping")
            return
        # Persist
        _c = _sql.connect("data/trader.db")
        _c.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
        _c.execute(
            "CREATE TABLE IF NOT EXISTS archer_briefings ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, briefing TEXT NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        _c.execute("INSERT INTO archer_briefings (briefing) VALUES (?)", (briefing,))
        _c.commit()
        _c.close()
        # NTFY (admin)
        try:
            _rq.post(
                "https://ntfy.sh/ollietrades-admin",
                data=briefing.encode("utf-8"),
                headers={"Title": "Captain Archer -- Morning Briefing",
                         "Priority": "default"},
                timeout=10,
            )
        except Exception as _ne:
            console.log(f"[yellow]Archer briefing ntfy failed: {type(_ne).__name__}: {_ne!r}")
        console.log(f"[cyan]Captain Archer briefing generated ({len(briefing)} chars, plutus-v1)")
    except Exception as e:
        console.log(f"[red]Archer briefing error: {type(e).__name__}: {e!r}")


@_hm_bq_instr("run_archer_alert_cycle")
def run_archer_alert_cycle():
    """HM-ARCHER-REBUILD: tiered Red/Yellow alert sweep, RTH-gated.

    Fires only when convergence hits RED/YELLOW or a short signal emits;
    deduped so the same (tier, symbol, systems) never re-alerts. Advisory only.
    """
    try:
        now = az_now()
    except Exception:
        return
    # Regular trading hours only (6:30am-1:00pm AZ ≈ 9:30-16:00 ET), weekdays
    if now.weekday() >= 5:
        return
    if not ((now.hour == 6 and now.minute >= 30) or (7 <= now.hour < 13)):
        return
    try:
        from engine.archer.alerts import run_alert_cycle
        result = run_alert_cycle()
        if result.get("red") or result.get("yellow"):
            console.log(f"[cyan]Captain Archer alerts: {result}")
    except Exception as e:
        console.log(f"[red]Archer alert cycle error: {type(e).__name__}: {e!r}")


# HM-ARCHER-REBUILD: run the alert sweep on a daemon thread so the per-alert
# plutus narration (up to 150s each) never blocks the scheduler thread. Same
# skip-if-prior-running lock pattern as _bg_whisper/_bg_autopilot, plus an
# enabled flag for one-line reversal (flag flip requires a restart).
_ARCHER_ALERTS_ENABLED = True
_archer_alerts_bg_lock = threading.Lock()


@_hm_bq_instr("_bg_archer_alerts")
def _bg_archer_alerts():
    """Daemon-thread wrapper around run_archer_alert_cycle() — never blocks the
    scheduler. Skips if disabled or if a prior sweep is still running."""
    if not _ARCHER_ALERTS_ENABLED:
        return
    if not _archer_alerts_bg_lock.acquire(blocking=False):
        console.log("[dim]Archer alerts bg: prior sweep still running — skip")
        return

    def _runner():
        try:
            run_archer_alert_cycle()
        finally:
            _archer_alerts_bg_lock.release()

    threading.Thread(target=_runner, daemon=True, name="sched_archer_alerts").start()


@_hm_bq_instr("run_crew_dissent_nightly")
def run_crew_dissent_nightly():
    """HM-CREW-DISSENT: nightly resolve crew dissents vs realized 5d outcomes
    (signals.db scored_predictions) + recompute per-officer accuracy stats.
    Advisory measurement only — no order path."""
    try:
        from engine.crew_dissent import resolve_dissent_outcomes, recompute_dissent_stats
        r = resolve_dissent_outcomes()
        s = recompute_dissent_stats()
        console.log(f"[cyan][crew_dissent] nightly: resolved {r.get('resolved', 0)} "
                    f"(pending {r.get('still_pending', 0)}), stats upserted {s.get('rows_upserted', 0)}")
    except Exception as e:
        console.log(f"[red][crew_dissent] nightly error: {type(e).__name__}: {e!r}")


# HM-AM UNIFICATION: daily net-worth snapshot (close-to-close baseline for the
# unified Net Worth card's daily change). Runs on a daemon thread (spot fetch
# can block ~6s cold). Enabled-flag for one-line reversal.
_NETWORTH_SNAPSHOT_ENABLED = True
_networth_snapshot_lock = threading.Lock()


@_hm_bq_instr("run_networth_snapshot")
def run_networth_snapshot():
    """Persist today's real net-worth (Schwab + Metals) into networth_history.
    Advisory/reporting only — no order path, no writes beyond the snapshot row."""
    try:
        from engine.total_portfolio import snapshot_networth
        r = snapshot_networth()
        console.log(f"[cyan][networth] snapshot {r.get('snapshot_date')}: "
                    f"net worth ${r.get('net_worth')}")
    except Exception as e:
        console.log(f"[red][networth] snapshot error: {type(e).__name__}: {e!r}")


def _bg_networth_snapshot():
    """Daemon-thread wrapper — never blocks the scheduler; skip if prior running."""
    if not _NETWORTH_SNAPSHOT_ENABLED:
        return
    if not _networth_snapshot_lock.acquire(blocking=False):
        console.log("[dim]Net-worth snapshot: prior run still active — skip")
        return

    def _runner():
        try:
            run_networth_snapshot()
        finally:
            _networth_snapshot_lock.release()

    threading.Thread(target=_runner, daemon=True, name="sched_networth_snapshot").start()


@_hm_bq_instr("run_intel_report_morning")
def run_intel_report_morning():
    """6:00 AM AZ — daily intel report + ntfy push to ollietrades-admin."""
    import datetime as _dt
    import pytz
    az  = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:          # Skip weekends
        return
    if now.hour != 6 or now.minute > 20:
        return
    try:
        from engine.morning_briefing import generate_daily_intel_report
        result = generate_daily_intel_report(force=True, push_ntfy=True)
        gp = result.get("game_plan", {})
        console.log(f"[cyan]Intel Report (AM): {gp.get('headline', 'generated')}")
    except Exception as e:
        console.log(f"[red]Intel Report AM error: {e}")


@_hm_bq_instr("run_intel_report_evening")
def run_intel_report_evening():
    """8:00 PM AZ — evening intel prep (no ntfy, just refresh JSON)."""
    import datetime as _dt
    import pytz
    az  = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:          # Skip weekends
        return
    if now.hour != 20 or now.minute > 20:
        return
    try:
        from engine.morning_briefing import generate_daily_intel_report
        result = generate_daily_intel_report(force=True, push_ntfy=False)
        gp = result.get("game_plan", {})
        console.log(f"[cyan]Intel Report (PM): {gp.get('headline', 'generated')}")
    except Exception as e:
        console.log(f"[red]Intel Report PM error: {e}")


@_hm_bq_instr("run_reveille_morning")
def run_reveille_morning():
    """5:45 AM AZ — HM-REVEILLE pre-market brief. Market-day-aware (skips weekends
    + holidays via is_trading_day), fail-closed. Self-triggers a FRESH intel report
    (runs before the 06:00 job, so disk substrate would otherwise be yesterday's)."""
    now = az_now()
    if now.hour != 5 or not (45 <= now.minute <= 59):
        return
    try:
        from engine.market_calendar import is_trading_day
        if not is_trading_day(now.date()):     # weekends + NYSE holidays
            return
    except Exception:
        return
    try:
        from engine.reveille import run_reveille
        brief = run_reveille(dry_run=False, fresh=True)
        console.log(f"[cyan]Reveille: {brief.get('headline', 'generated')} "
                    f"| delivery={brief.get('_delivery')}")
    except Exception as e:
        console.log(f"[red]Reveille error: {type(e).__name__}: {e}")


@_hm_bq_instr("run_phaser_lock_morning")
def run_phaser_lock_morning():
    """6:05 AM AZ — HM-PHASER-LOCK daily Trade-of-the-Day (top-3 ranked, fail-closed).
    Market-day-aware; runs after the 06:00 intel refresh so setups are current."""
    now = az_now()
    if now.hour != 6 or not (12 <= now.minute <= 29):  # HM-DRYDOCK-2 HIGH1: 06:15 fire (after ~06:12 scan), pre-open
        return
    try:
        from engine.market_calendar import is_trading_day
        if not is_trading_day(now.date()):     # weekends + NYSE holidays
            return
    except Exception:
        return
    try:
        from engine.phaser_lock import run_phaser_lock
        r = run_phaser_lock(dry_run=False)
        console.log(f"[cyan]Phaser-Lock: {r.get('headline', 'generated')} "
                    f"| qualified={r.get('qualified')} delivery={r.get('_delivery')}")
    except Exception as e:
        console.log(f"[red]Phaser-Lock error: {type(e).__name__}: {e}")


@_hm_bq_instr("run_filter_contribution_sweep")
def run_filter_contribution_sweep():
    """02:30 AZ (OFF-PEAK) — HM-DRYDOCK #4 bounded leave-one-out filter-contribution ablation → cache.
    Deliberately off-peak: NEVER during the 05:45–06:05 morning cadence or market hours, so the sweep
    can't compete with live scans or the Ollama warm. The endpoint only ever READS the cache."""
    now = az_now()
    if now.hour != 2 or not (30 <= now.minute <= 50):
        return
    try:
        from engine.filter_contribution import run_sweep
        r = run_sweep()
        console.log(f"[cyan]Filter-contribution sweep: {r.get('samples')} samples, "
                    f"baseline {r.get('baseline_all_pass_fwd_pct')}%, timed_out={r.get('timed_out')}")
    except Exception as e:
        console.log(f"[red]Filter-contribution sweep error: {type(e).__name__}: {e}")


# ── Earnings → scan_universe Injection ────────────────────────────────────────
# HM-AR-β 2026-05-07: renamed from run_earnings_universe_inject. The old name was
# a naming-drift artifact — function injects into scan_universe (via
# engine.deep_scan.inject_earnings_tickers), NOT the earnings_universe SQLite
# table. The earnings_universe orphan was retired in the same commit; see
# docs/EARNINGS.md and archive/earnings_injector.py.retired-20260507.

@_hm_bq_instr("run_earnings_scan_inject")
def run_earnings_scan_inject():
    """6:00 AM AZ — detect today's earnings reporters and inject into scan_universe.

    Sweeps _AH_PM_UNIVERSE + morning_briefing._EARN_UNIVERSE via yfinance calendar.
    Populates _earnings_today_tickers so run_earnings_day_scan() can rescan every 5 min.
    """
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:
        return
    if now.hour != 6 or now.minute > 30:
        return
    global _earnings_today_tickers
    try:
        import yfinance as _yf
        from engine.deep_scan import inject_earnings_tickers

        today_str = now.strftime("%Y-%m-%d")

        # Build sweep list: AH/PM universe + morning briefing universe
        sweep = list(_AH_PM_UNIVERSE)
        try:
            from engine.morning_briefing import _EARN_UNIVERSE as _mb_earn  # type: ignore
            sweep = list(dict.fromkeys(sweep + list(_mb_earn)))
        except Exception:
            pass

        reporters = []
        for sym in sweep:
            try:
                cal = _yf.Ticker(sym).calendar
                if cal is None or (hasattr(cal, "empty") and cal.empty):
                    continue
                if hasattr(cal, "get"):
                    dates = cal.get("Earnings Date") or cal.get("Earnings Dates")
                elif hasattr(cal, "columns") and "Earnings Date" in cal.columns:
                    dates = cal["Earnings Date"]
                else:
                    dates = None
                if dates is None:
                    continue
                date_list = list(dates) if hasattr(dates, "__iter__") else [dates]
                for d in date_list:
                    if str(d)[:10] == today_str:
                        reporters.append(sym)
                        break
            except Exception:
                continue

        if reporters:
            injected = inject_earnings_tickers(reporters)
            _earnings_today_tickers = set(reporters)
            console.log(
                f"[cyan][EarningsInject] {injected} tickers added for today: {reporters}"
            )
        else:
            console.log("[dim][EarningsInject] No earnings reporters found for today")

    except Exception as e:
        console.log(f"[red]run_earnings_scan_inject error: {e}")


def run_carts_persist():
    """HM-AO 2026-05-17 — daily 06:00 AZ CARTS retail nowcast persistence.

    Calls engine.fred_data.persist_carts_all(). Fail-soft: errors logged,
    never raised — CARTS is observability data, not a trading gate.
    ntfy fires only on NEW MAX(obs_date) per the persist helper.
    Morpheus consumer deferred to HM-AN per HM-AO scope.
    """
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    # Daily 06:00 AZ — single fire window
    if now.hour != 6 or now.minute > 30:
        return
    try:
        from engine.fred_data import persist_carts_all
        result = persist_carts_all()
        console.log(
            f"[cyan][CARTS] persist: {result.get('series_count', 0)} series, "
            f"{result.get('rows_written', 0)} rows, "
            f"errors={len(result.get('errors', []))}"
        )
        if result.get("errors"):
            console.log(f"[yellow][CARTS] errors: {result['errors'][:3]}")
    except Exception as e:
        console.log(f"[red][CARTS] run_carts_persist error: {e}")


@_hm_bq_instr("run_earnings_day_scan")
def run_earnings_day_scan():
    """Every 5 min market hours — rescan earnings-day tickers at high frequency.

    Only runs if _earnings_today_tickers is non-empty (populated by run_earnings_scan_inject).
    Results are stored with earnings_today=1 so the dashboard can surface them separately.
    """
    if not _earnings_today_tickers:
        return
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:
        return
    # Market hours gate: 6:30 AM – 1:15 PM AZ (9:30 AM – 4:15 PM ET)
    mins = now.hour * 60 + now.minute
    if not (390 <= mins <= 795):
        return
    try:
        from engine.deep_scan import scan_earnings_tickers
        result = scan_earnings_tickers(list(_earnings_today_tickers))
        if result.get("signals_found", 0) > 0:
            console.log(
                f"[yellow][EarningsDay] {result['signals_found']} signals on "
                f"{sorted(_earnings_today_tickers)}: top={result.get('top_symbols', [])}"
            )
    except Exception as e:
        console.log(f"[red]run_earnings_day_scan error: {e}")


# ── After-Hours & Pre-Market Scanners ─────────────────────────────────────────

_AH_PM_UNIVERSE = [
    "AAPL","MSFT","NVDA","AMZN","GOOG","META","TSLA","AMD","PLTR","CRM",
    "NFLX","AVGO","COST","LLY","JPM","BAC","GS","V","MA","UNH","JNJ",
    "PFE","ABBV","XOM","CVX","WMT","HD","DIS","COIN","ORCL","NOW",
    "MU","DELL","INTC","QCOM","ADBE","MRVL","SNOW","NET","DDOG","SMCI",
]

_ah_ntfy_sent: set = set()   # dedup: "SYM-DATE" strings already pushed today


def _fetch_prepost_gaps(symbols: list, mode: str) -> list:
    """
    Fetch pre/post-market gaps for a list of symbols.
    mode='ah'  → compare AH price  vs regular close (bars after  16:00 ET)
    mode='pre' → compare PM price  vs prev-day close (bars before 09:30 ET)
    Returns list of dicts sorted by abs(gap_pct) desc.
    """
    import datetime as _dt
    import yfinance as _yf

    results = []
    # Batch download to reduce HTTP round-trips
    batch_size = 10
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            raw = _yf.download(
                batch, period="2d", interval="1m",
                prepost=True, auto_adjust=True,
                group_by="ticker", progress=False, threads=True,
                timeout=20,
            )
        except Exception:
            continue

        for sym in batch:
            try:
                df = raw[sym] if len(batch) > 1 else raw
                if df is None or df.empty:
                    continue
                closes = df["Close"].dropna()
                if len(closes) < 5:
                    continue

                # Convert index → Eastern time for comparison
                idx_et = closes.index.tz_convert("America/New_York")

                if mode == "ah":
                    # Regular close = last bar on or before 16:00 ET today
                    today_et = idx_et.date[-1]
                    today_regular = closes[
                        (idx_et.date == today_et) &
                        (idx_et.time <= _dt.time(16, 0, 0))
                    ]
                    today_ah = closes[
                        (idx_et.date == today_et) &
                        (idx_et.time > _dt.time(16, 0, 0))
                    ]
                    if today_regular.empty or today_ah.empty:
                        continue
                    reg_close = float(today_regular.iloc[-1])
                    ext_price = float(today_ah.iloc[-1])
                    ext_ts    = idx_et[closes.index.get_loc(today_ah.index[-1])].strftime("%H:%M ET")
                else:  # pre-market
                    # today's pre-market = bars before 09:30 ET today
                    today_et = idx_et.date[-1]
                    prev_regular = closes[
                        (idx_et.date < today_et) &
                        (idx_et.time <= _dt.time(16, 0, 0))
                    ]
                    today_pre = closes[
                        (idx_et.date == today_et) &
                        (idx_et.time < _dt.time(9, 30, 0))
                    ]
                    if prev_regular.empty or today_pre.empty:
                        continue
                    reg_close = float(prev_regular.iloc[-1])
                    ext_price = float(today_pre.iloc[-1])
                    ext_ts    = idx_et[closes.index.get_loc(today_pre.index[-1])].strftime("%H:%M ET")

                if reg_close <= 0:
                    continue
                gap_pct = (ext_price - reg_close) / reg_close * 100
                if abs(gap_pct) < (2.0 if mode == "pre" else 2.5):
                    continue   # skip noise — only keep meaningful moves

                results.append({
                    "symbol":    sym,
                    "reg_close": round(reg_close, 2),
                    "ext_price": round(ext_price, 2),
                    "gap_pct":   round(gap_pct, 2),
                    "timestamp": ext_ts,
                    "direction": "up" if gap_pct > 0 else "down",
                })
            except Exception:
                continue

    results.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
    return results


def _update_brief_json(key: str, data) -> None:
    """Merge a key into data/morning_brief.json in-place."""
    import json as _json, os as _os
    path = _os.path.expanduser("~/autonomous-trader/data/morning_brief.json")
    try:
        existing = {}
        if _os.path.exists(path):
            with open(path) as fh:
                existing = _json.load(fh)
        existing[key] = data
        import datetime as _dt
        existing[f"{key}_updated"] = _dt.datetime.now().isoformat()
        with open(path, "w") as fh:
            _json.dump(existing, fh, indent=2, default=str)
    except Exception as e:
        console.log(f"[red]_update_brief_json({key}) error: {e}")


@_hm_bq_instr("run_ah_scanner")
def run_ah_scanner():
    """AH Earnings Scanner — runs every 30 min from 4 PM to 7 PM AZ (7–10 PM ET).
    Flags stocks moving > 3% after hours, ntfy push if > 5%.
    Writes 'ah_movers' to morning_brief.json.
    """
    import datetime as _dt
    import pytz
    global _ah_ntfy_sent

    az  = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:
        return
    # AZ 4 PM – 7 PM = ~7 PM – 10 PM ET (extended AH window)
    if not (16 <= now.hour < 19):
        return

    console.log("[cyan]AH Scanner: checking post-market movers...")
    try:
        from engine.universe import get_active_universe
        universe = list(set(list(get_active_universe()) + _AH_PM_UNIVERSE))
    except Exception:
        universe = _AH_PM_UNIVERSE

    movers = _fetch_prepost_gaps(universe, mode="ah")
    flagged = [m for m in movers if abs(m["gap_pct"]) >= 3.0]

    if flagged:
        console.log(f"[yellow]AH movers (>3%): {[(m['symbol'], m['gap_pct']) for m in flagged]}")
        _update_brief_json("ah_movers", flagged)

        # ntfy push for big gaps > 5%
        big = [m for m in flagged if abs(m["gap_pct"]) >= 5.0]
        today_str = now.strftime("%Y-%m-%d")
        for m in big:
            key = f"{m['symbol']}-{today_str}"
            if key in _ah_ntfy_sent:
                continue
            _ah_ntfy_sent.add(key)
            arrow = "▲" if m["gap_pct"] > 0 else "▼"
            title = f"AH Mover: {m['symbol']} {arrow}{abs(m['gap_pct']):.1f}%"
            body  = (
                f"Close: ${m['reg_close']:.2f} → AH: ${m['ext_price']:.2f} "
                f"({m['gap_pct']:+.1f}%) @ {m['timestamp']}"
            )
            try:
                from engine.morning_briefing import _push_admin_ntfy
                _push_admin_ntfy(title, body, priority=4)
            except Exception:
                pass
    else:
        console.log("[dim]AH Scanner: no movers > 3%")


@_hm_bq_instr("run_premarket_scanner")
def run_premarket_scanner():
    """Pre-Market Scanner — runs every 15 min from 6 AM to 9:25 AM AZ (9 AM – 12:25 PM ET).
    Flags pre-market gaps > 2%, writes 'premarket_movers' to morning_brief.json.
    """
    import datetime as _dt
    import pytz

    az  = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:
        return
    # AZ 6:00 AM – 9:25 AM
    if not (6 <= now.hour < 9 or (now.hour == 9 and now.minute <= 25)):
        return

    console.log("[cyan]Pre-Market Scanner: checking pre-market movers...")
    try:
        from engine.universe import get_active_universe
        universe = list(set(list(get_active_universe()) + _AH_PM_UNIVERSE))
    except Exception:
        universe = _AH_PM_UNIVERSE

    movers = _fetch_prepost_gaps(universe, mode="pre")
    flagged = [m for m in movers if abs(m["gap_pct"]) >= 2.0]

    if flagged:
        top20 = sorted(flagged, key=lambda x: abs(x["gap_pct"]), reverse=True)[:20]
        console.log(f"[yellow]Pre-market movers (>2%): {[(m['symbol'], m['gap_pct']) for m in top20[:5]]}")
        _update_brief_json("premarket_movers", top20)
    else:
        console.log("[dim]Pre-Market Scanner: no movers > 2%")


@_hm_bq_instr("run_opening_range")
def run_opening_range():
    """Set opening range at 6:45 AM MST (after first 15 min of trading)."""
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:
        return
    if now.hour != 6 or now.minute < 45 or now.minute > 55:
        return
    try:
        from engine.battle_station import update_opening_range
        update_opening_range()
        console.log("[cyan]Battle Station: opening range set for SPY/QQQ")
    except Exception as e:
        console.log(f"[red]Opening range update error: {e}")



_last_battle_station_run = 0.0
# HM-AS-β 2026-05-07: cadence drift observability
_battle_station_last_fire_ts = 0.0


@_hm_bq_instr("battle_station_monitor")
def run_battle_station_monitor():
    """60-second options position monitor (early-exit if no positions)."""
    # HM-AS-β 2026-05-07: cadence drift observability — log if scheduler
    # tick interval exceeds 180s (target: 120s every 2 min). Tail driven
    # by single-threaded schedule.run_pending() blocking on slow jobs.
    global _last_battle_station_run, _battle_station_last_fire_ts
    import time as _t
    _now_ts = _t.time()
    if _battle_station_last_fire_ts > 0:
        _interval = _now_ts - _battle_station_last_fire_ts
        if _interval > 180:
            logger.warning(
                "[HM-AS-β] battle_station_monitor cadence drift: "
                "%.1fs since last fire (target: 120s)", _interval
            )
    _battle_station_last_fire_ts = _now_ts

    if _now_ts - _last_battle_station_run < 55:  # deduplicate on 30s tick
        return
    _last_battle_station_run = _now_ts
    try:
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        from engine.battle_station import monitor_active_options
        monitor_active_options()
    except Exception as e:
        console.log(f"[red]Battle Station monitor error: {e}")


# Alpaca GEX: 4x per trading day at key times (MST = ET - 3h)
# ET 9:00 = 6:00 MST, ET 9:35 = 6:35 MST, ET 12:00 = 9:00 MST, ET 15:00 = 12:00 MST
_ALPACA_GEX_WINDOWS_MST = [(6, 0), (6, 35), (9, 0), (12, 0)]
_last_alpaca_gex_window: dict = {}

@_hm_bq_instr("run_alpaca_gex_refresh")
def run_alpaca_gex_refresh():
    """Refresh Alpaca GEX at 4 key ET times. Runs on 5-min polling; deduplicates per window."""
    import datetime as _dt
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    now = _dt.datetime.now()
    h, m = now.hour, now.minute
    in_window = any(wh == h and abs(wm - m) <= 3 for wh, wm in _ALPACA_GEX_WINDOWS_MST)
    if not in_window:
        return
    window_key = f"{h}:{m // 6}"  # 6-minute bucket
    import time as _t
    last_run = _last_alpaca_gex_window.get(window_key, 0)
    if _t.time() - last_run < 300:
        return
    _last_alpaca_gex_window[window_key] = _t.time()
    try:
        from gex_calculator import refresh_alpaca_gex
        results = refresh_alpaca_gex()
        console.log(f"[cyan]Alpaca GEX: refreshed {len(results)} symbols at {h:02d}:{m:02d} MST")
    except Exception as e:
        console.log(f"[red]Alpaca GEX refresh error: {e}")


_last_war_room_time = 0
_war_room_running = threading.Event()  # guard: prevents two-thread overlap (2026-04-26)

# HM-WAR-ROOM-LATENCY Layer 1 (2026-05-15): cycle-duration log + stall NTFY threshold.
# Stalls above this wall-clock fire one [WR-STALL] WARNING NTFY per error class per
# process lifetime (see CLAUDE.md "Alert rate-limit semantics — in-memory only"). The
# 10-min value matches the diagnostic signature documented in
# data/scotty_hm_war_room_latency_scope_2026-05-15.md.
# HM-WR-STALL-ALARM-RATE-LIMIT 2026-05-20: raised from 600 → 750 (12.5 min) to
# reduce false-alarm volume. First [WR-PROVIDER-DUR] cycle observed wall=1175s
# with all 8 LLM providers in 91-202s range (see project_hm_wr_provider_latency).
# 750s gives breathing room above the typical post-restart cycle while still
# catching genuine multi-cycle stalls.
_WR_STALL_THRESHOLD_S = 750

# HM-WR-STALL-ALARM-RATE-LIMIT 2026-05-20: per-hour NTFY rate limit replaces
# engine.alert_channels per-process-lifetime dedup. The prior dedup let one
# alert through per process; with frequent restarts during dev that produced
# either every-cycle noise OR multi-cycle silence. Per-hour gate gives both
# bounded notification volume AND restart-survival of the limit window.
_wr_stall_last_ntfy: float = 0.0

# HM-WAR-ROOM-LATENCY Layer 2a v1 (2026-05-20): cycle-wall budget threshold.
# Instrumentation-only — emits [WR-BUDGET-EXCEEDED] when wall > budget but
# does NOT abort the cycle. One trading session of telemetry informs the
# decision to flip to actual deadline-check abort (v2 ship, deferred per
# project_hm_layer_2a_design.md). Distinct from [WR-STALL] (which fires at
# 600s with NTFY rate-limited per-process-lifetime); this is a higher
# threshold for the budget-abort discussion, log-only by design + lands in
# logs/trader.log alongside [WR-DUR] for joint grep'ability.
_WR_CYCLE_BUDGET_S = int(os.getenv("WAR_ROOM_CYCLE_BUDGET_S", "925"))


def _emit_wr_duration(wall_seconds: float) -> None:
    """HM-WAR-ROOM-LATENCY Layer 1: log cycle wall-clock and NTFY if over threshold.

    Called from the finally-block of _war_room_thread. Must not raise — the
    caller is a daemon thread whose finally also clears _war_room_running;
    propagating here would skip the clear and latch the guard forever.
    """
    console.log(f"[WR-DUR] cycle wall={wall_seconds:.1f}s")
    if wall_seconds > _WR_STALL_THRESHOLD_S:
        # HM-WR-STALL-ALARM-RATE-LIMIT 2026-05-20: per-hour rate limit on the
        # NTFY (not the log line — log always fires for full observability).
        # 3600s = 1h gate. Prevents every-cycle alert spam when the system is
        # in sustained-slow mode (e.g., VRAM thrashing causing 1100s+ cycles).
        global _wr_stall_last_ntfy
        _now = time.time()
        if _now - _wr_stall_last_ntfy > 3600:
            try:
                from engine.alert_channels import AlertLevel, send_alert
                send_alert(
                    message=(
                        f"[WR-STALL] War Room cycle wall={wall_seconds / 60:.1f}min "
                        f"exceeded threshold ({_WR_STALL_THRESHOLD_S / 60:.0f}min). "
                        f"Scheduler will skip ticks until cycle releases."
                    ),
                    level=AlertLevel.WARNING,
                    alert_type="war_room_slow_cycle",
                )
                _wr_stall_last_ntfy = _now
            except Exception as e:
                console.log(
                    f"[red][WR-STALL] NTFY dispatch failed: {type(e).__name__}: {e!r}"
                )
        else:
            _suppressed_min = (3600 - (_now - _wr_stall_last_ntfy)) / 60.0
            console.log(
                f"[WR-STALL-SUPPRESSED] cycle wall={wall_seconds / 60:.1f}min "
                f"(rate-limited; next NTFY in {_suppressed_min:.0f}min)"
            )


def _emit_wr_budget_exceeded(wall_seconds: float) -> None:
    """HM-WAR-ROOM-LATENCY Layer 2a v1: log-only budget-exceeded marker.

    Same crash-safe contract as ``_emit_wr_duration`` — must not raise; daemon
    thread's finally clears ``_war_room_running`` and a propagated exception
    here would latch the guard.

    Distinct from ``[WR-STALL]`` (600s, NTFY, rate-limited):
    - Higher threshold (925s default; ``WAR_ROOM_CYCLE_BUDGET_S`` env-var
      override; restart-to-retune for v1).
    - Log-only — does NOT call ``engine.alert_channels.send_alert``. v1
      ships as observability data for the v2 deadline-cap decision.
    - Emits to ``console.log`` (→ ``logs/trader.log``) so the line lands
      next to ``[WR-DUR]`` for joint grep'ability; NOT ``logger.warning``
      which would split-channel to ``logs/trader_error.log`` per CLAUDE.md
      "logger.info vs console.log" lesson.

    See ``project_hm_layer_2a_design.md`` for the full design + v2 path.
    """
    if wall_seconds > _WR_CYCLE_BUDGET_S:
        try:
            over_by = wall_seconds - _WR_CYCLE_BUDGET_S
            console.log(
                f"[WR-BUDGET-EXCEEDED] cycle wall={wall_seconds:.1f}s "
                f"budget={_WR_CYCLE_BUDGET_S}s over_by={over_by:.1f}s"
            )
        except Exception:
            pass  # truly crash-safe: a nested console.log fallback can also fail


@_hm_bq_instr("run_war_room")
def run_war_room():
    """War Room: all AIs give hot takes. Free models 24/7, paid models market hours only."""
    global arena, _last_war_room_time
    # === HM-WR-CYCLE-DEBUG-PROBE 2026-05-20 ===
    # Temporary instrumentation to diagnose post-restart cycle dormancy. Logs every
    # entry + which early-return path fires. REMOVE once root cause is identified.
    try:
        from engine.risk_manager import RiskManager as _RM_dbg
        _sess_dbg = _RM_dbg.is_market_hours()
    except Exception as _e:
        _sess_dbg = f"ERR:{type(_e).__name__}"
    _age = time.time() - _last_war_room_time
    console.log(
        f"[WR-DEBUG] entry arena={arena is not None} session={_sess_dbg!r} "
        f"throttle_age={_age:.0f}s guard={_war_room_running.is_set()}"
    )
    # === /HM-WR-CYCLE-DEBUG-PROBE ===
    if arena is None:
        console.log("[WR-DEBUG] early-return: arena is None")
        return
    from engine.risk_manager import RiskManager
    import time as _time

    session = RiskManager.is_market_hours()
    if not session:
        console.log(f"[WR-DEBUG] early-return: session falsy ({session!r})")
        return  # Fully closed (weekends, overnight)

    # Throttle: 3 min during active market, 5 min pre/post-market
    now = _time.time()
    if session == "market" or session == "power_hour":
        if now - _last_war_room_time < 180:
            console.log(f"[WR-DEBUG] early-return: market throttle ({now - _last_war_room_time:.0f}s < 180s)")
            return
    elif session in ("pre_market", "post_market"):
        if now - _last_war_room_time < 300:
            console.log(f"[WR-DEBUG] early-return: pre/post throttle ({now - _last_war_room_time:.0f}s < 300s)")
            return
    _last_war_room_time = now

    # Guard: if previous cycle's daemon is still working through agents, skip this tick.
    # _war_room_running is set at thread-start and cleared in finally — crash-safe.
    if _war_room_running.is_set():
        console.log("[yellow]⚠️ War Room: previous cycle still running — SKIPPING this tick (guard fired)")
        return

    console.log("[magenta]War Room: launching cycle...")

    def _war_room_thread():
        _war_room_running.set()   # mark busy before any work starts
        _wr_t0 = _time.perf_counter()  # HM-WAR-ROOM-LATENCY Layer 1
        try:
            # HM-AQ-β v3 2026-05-07: bulk endpoint (~25× faster than per-symbol loop).
            from engine.market_data import get_bulk_prices
            from engine.war_room import run_war_room as _run_wr
            from engine.universe import get_active_universe
            prices = get_bulk_prices(get_active_universe())
            if prices:
                _run_wr(arena.providers, prices)
            else:
                console.log("[yellow]War Room: no prices available, skipping")
        except Exception as e:
            console.log(f"[red]War Room error: {e}")
        finally:
            # HM-WAR-ROOM-LATENCY Layer 1: emit cycle wall + NTFY if stall>threshold.
            # HM-WAR-ROOM-LATENCY Layer 2a v1 (2026-05-20): also emit
            # [WR-BUDGET-EXCEEDED] when wall > _WR_CYCLE_BUDGET_S (log-only).
            # Both helpers absorb their own errors; never raise here.
            _wr_wall = _time.perf_counter() - _wr_t0
            _emit_wr_duration(_wr_wall)
            _emit_wr_budget_exceeded(_wr_wall)
            _war_room_running.clear()   # always clears — crash cannot latch the guard

    threading.Thread(target=_war_room_thread, daemon=True).start()


@_hm_bq_instr("run_autopilot")
def run_autopilot():
    """Autopilot: auto-rebalance overweight positions and maintain cash floor."""
    try:
        from engine.autopilot import run_autopilot as _run_ap
        from engine.market_data import get_stock_price
        from engine.universe import get_active_universe
        prices = {}
        # Build symbol universe: watchlist + all currently open positions
        syms_to_fetch = set(get_active_universe())
        try:
            import sqlite3 as _sq
            _pc = _sq.connect("data/trader.db", timeout=5)
            _pc.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
            _rows = _pc.execute(
                "SELECT DISTINCT symbol FROM positions WHERE qty > 0"
            ).fetchall()
            _pc.close()
            syms_to_fetch.update(r[0] for r in _rows)
        except Exception as _pe:
            console.log(f"[yellow]Autopilot: position symbol fetch error: {_pe}")
        for sym in syms_to_fetch:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data
        if prices:
            _run_ap(prices)
    except Exception as e:
        console.log(f"[red]Autopilot error: {e}")


# HM-AS-β §B Loop 2 (2026-05-29): fire-and-forget thread wrapper for run_autopilot.
# After Loop 1 backgrounded run_whisper, [HM-BQ-instr] shows run_autopilot is the
# most FREQUENT remaining synchronous loop-blocker: avg 96s / max 169s on a 30-min
# cadence — its wall lands inside schedule.run_pending() and delays the 30s-tick
# battle_station_monitor, a primary [HM-AS-β] drift victim. Backgrounding it frees
# the scheduler thread. Same proven pattern as _bg_whisper: skip-if-prior-running
# lock keeps max 1 in-flight (169s max ≪ 1800s cadence, so overlap is belt-and-
# braces, not expected). NOTE: drift has multiple synchronous contributors
# (run_scanner/run_imbalance_scan/run_strategy_scan); Loop 2 reduces but may not
# zero drift — measure, then decide on further wraps (loop-by-loop discipline).
_autopilot_bg_lock = threading.Lock()


@_hm_bq_instr("_bg_autopilot")
def _bg_autopilot():
    """Daemon-thread wrapper around run_autopilot() — never blocks the scheduler.
    If a prior invocation is still running, skip this tick."""
    if not _autopilot_bg_lock.acquire(blocking=False):
        console.log("[dim]Autopilot bg: prior tick still running — skip")
        return
    def _runner():
        try:
            run_autopilot()
        finally:
            _autopilot_bg_lock.release()
    threading.Thread(target=_runner, daemon=True, name="sched_autopilot").start()


@_hm_bq_instr("run_whisper")
def run_whisper():
    """Whisper Network: check for trending watchlist stocks."""
    try:
        from engine.whisper_network import run_whisper_check
        run_whisper_check()
    except Exception as e:
        console.log(f"[red]Whisper error: {e}")


# HM-AS-β §B Loop 1 (2026-05-28): fire-and-forget thread wrapper for run_whisper.
# Wall-time instrumentation ([HM-BQ-instr]) identified run_whisper as THE
# single-thread schedule.run_pending() loop-blocker: avg 831s / max 1194s on a
# 10-min cadence — its 1194s max ≈ battle_station_monitor's 1183s max drift.
# Backgrounding it (not the drift VICTIMS) is the real fix: squeeze_watcher was
# already _bg-wrapped since β.2 yet still drifted, proving the blocker was elsewhere.
# At ~14min on a 10-min cadence it WILL overlap — skip-if-prior-running lock keeps
# max 1 in-flight (no unbounded thread spawn).
_whisper_bg_lock = threading.Lock()


@_hm_bq_instr("_bg_whisper")
def _bg_whisper():
    """Daemon-thread wrapper around run_whisper() — never blocks the scheduler.
    If a prior invocation is still running, skip this tick."""
    if not _whisper_bg_lock.acquire(blocking=False):
        console.log("[dim]Whisper bg: prior tick still running — skip")
        return
    def _runner():
        try:
            run_whisper()
        finally:
            _whisper_bg_lock.release()
    threading.Thread(target=_runner, daemon=True, name="sched_whisper").start()


@_hm_bq_instr("run_strength_scan")
def run_strength_scan():
    """Relative Strength Scanner: rank watchlist stocks vs SPY."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.strength_scanner import scan_relative_strength
        from engine.universe import get_active_universe
        rankings = scan_relative_strength(get_active_universe())
        if rankings:
            top = rankings[0]
            bottom = rankings[-1]
            console.log(
                f"[cyan]Strength: #{1} {top['symbol']}({top['score']}) ... "
                f"#{len(rankings)} {bottom['symbol']}({bottom['score']})"
            )
    except Exception as e:
        console.log(f"[red]Strength scan error: {e}")


@_hm_bq_instr("run_trend_forecast")
def run_trend_forecast():
    """Trend Forecast: predict trends for all watchlist stocks."""
    if not should_run_task("run_trend_forecast", throttle_mins=60):
        return
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.trend_predictor import predict_all_trends
        from engine.universe import get_active_universe
        results = predict_all_trends(get_active_universe())
        if results:
            top = results[0]
            console.log(f"[dim]Trend: top {top['symbol']} {top['direction']} ({top['confidence']:.0f}%)")
    except Exception as e:
        console.log(f"[red]Trend forecast error: {e}")


@_hm_bq_instr("run_strategy_presets")
def run_strategy_presets():
    """Strategy Presets: evaluate strategy fits for watchlist."""
    if not should_run_task("run_strategy_presets", throttle_mins=60):
        return
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.strategy_presets import scan_strategies
        results = scan_strategies()
        if results:
            console.log(f"[dim]Strategies: {len(results)} active presets")
    except Exception as e:
        console.log(f"[red]Strategy presets error: {e}")


@_hm_bq_instr("run_discovery_scan")
def run_discovery_scan():
    """Discovery Scanner — RETIRED. Replaced by Volume Radar (run_volume_market_scan).
    Kept as archive reference. Scheduler call below is commented out.
    """
    pass


# ---------------------------------------------------------------------------
# Volume Radar — Full Market Scanner (replaces Discovery Scanner)
# ---------------------------------------------------------------------------

@_hm_bq_instr("run_volume_universe_refresh")
def run_volume_universe_refresh():
    """Weekly: refresh the full ~10,000-stock universe from Alpaca (Sunday 10 PM MST)."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() != 6:  # Sunday only
        return
    if now.hour != 22:
        return
    try:
        from engine.full_universe import refresh_universe
        count = refresh_universe()
        console.log(f"[green]🌐 Universe refreshed: {count} tradeable symbols")
    except Exception as e:
        console.log(f"[red]Universe refresh error: {e}")


@_hm_bq_instr("run_volume_baselines")
def run_volume_baselines():
    """Nightly: update 20-day average volume baselines from Alpaca bars (weeknights 11 PM MST)."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:  # Skip Saturday (5) and Sunday (6)
        return
    if now.hour != 23:
        return
    try:
        from engine.volume_baselines import update_baselines
        count = update_baselines()
        console.log(f"[green]📊 Volume baselines updated: {count} stocks")
    except Exception as e:
        console.log(f"[red]Volume baselines error: {e}")


@_hm_bq_instr("run_volume_market_scan")
def run_volume_market_scan():
    """Every 15 min during market hours: full market volume scan (replaces discovery_scanner)."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.volume_scanner import scan_full_market
        hot = scan_full_market()
        if hot:
            top = hot[0]
            console.log(
                f"[magenta]🔴 Volume Radar: {len(hot)} hot stocks — "
                f"top: {top['symbol']} {top['relative_volume']:.0f}x"
            )
            try:
                from engine.signal_poster import post_to_9000
                post_to_9000("VOLUME_SPIKE", {
                    "count": len(hot),
                    "symbol": top.get("symbol"),
                    "relative_volume": round(top.get("relative_volume", 0), 1),
                    "price": top.get("price"),
                    "message": (
                        f"Volume Radar: {len(hot)} hot stocks — "
                        f"top: {top.get('symbol')} {top.get('relative_volume',0):.0f}x rvol"
                    ),
                    "top_stocks": [
                        {"symbol": h.get("symbol"), "rvol": round(h.get("relative_volume", 0), 1)}
                        for h in hot[:5]
                    ],
                })
            except Exception:
                pass
    except Exception as e:
        console.log(f"[red]Volume market scan error: {e}")


@_hm_bq_instr("run_volume_red_alert")
def run_volume_red_alert():
    """Every 2 min during market hours: check today's hot stocks for extreme spikes."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.volume_scanner import red_alert_check
        red_alert_check()
    except Exception as e:
        console.log(f"[red]Volume red alert error: {e}")


@_hm_bq_instr("run_impulse_check")
def run_impulse_check():
    """Hourly Impulse Detector: check watchlist for volume/price/breakout impulses."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.universe import get_active_universe
        from engine.impulse_detector import scan_all_impulses
        alerts = scan_all_impulses(get_active_universe())
        if alerts:
            top = alerts[0]
            icon = "▲" if top["direction"] == "bullish" else "▼"
            console.log(
                f"[bold cyan]Impulse: {len(alerts)} signal(s) — "
                f"top: {icon} {top['ticker']} ({top['direction']}) "
                f"strength={top['strength_score']}/10"
            )
        else:
            console.log("[dim]Impulse: no signals this hour")
    except Exception as e:
        console.log(f"[red]Impulse check error: {e}")


_gap_scan_done_today = False
_gap_fill_last_run = 0.0
_holly_nightly_done = False


@_hm_bq_instr("run_gap_scan")
def run_gap_scan():
    """Morning Gap Scanner: runs once per day at market open (7:30-8:30 AM AZ / 9:30-10:30 AM ET)."""
    global _gap_scan_done_today
    import pytz
    from datetime import datetime as _dt
    az = pytz.timezone("US/Arizona")
    now = az_now()

    # Reset flag at midnight
    if now.hour == 0:
        _gap_scan_done_today = False
        return

    # Weekdays only, 7:30 AM - 8:30 AM AZ (9:30 AM - 10:30 AM ET) — after market opens
    if now.weekday() >= 5:
        return
    if not (7 <= now.hour <= 8) or _gap_scan_done_today:
        return
    # Don't fire before 7:30 AM AZ
    if now.hour == 7 and now.minute < 30:
        return

    _gap_scan_done_today = True
    try:
        from engine.universe import get_active_universe
        from engine.gap_scanner import scan_all_gaps
        gaps = scan_all_gaps(get_active_universe())
        if gaps:
            top = gaps[0]
            icon = "▲" if top["gap_direction"] == "up" else "▼"
            console.log(
                f"[bold cyan]Gap scan: {len(gaps)} gap(s) — "
                f"top: {icon} {top['ticker']} {top['gap_pct']:+.2f}% "
                f"({top['gap_type']}) fill={top['fill_probability']:.0f}%"
            )
        else:
            console.log("[dim]Gap scan: no gaps ≥ 0.5% today")
    except Exception as e:
        console.log(f"[red]Gap scan error: {e}")


@_hm_bq_instr("run_gap_fill_check")
def run_gap_fill_check():
    """Track gap fills throughout the trading day (every 5 min during market hours)."""
    global _gap_fill_last_run
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    import time as _time
    now = _time.time()
    if now - _gap_fill_last_run < 300:  # at most every 5 minutes
        return
    _gap_fill_last_run = now
    try:
        from engine.universe import get_active_universe
        from engine.gap_scanner import update_gap_fills
        update_gap_fills(get_active_universe())
    except Exception as e:
        console.log(f"[yellow]Gap fill check error: {e}")


# HM-AO-β 2026-05-08: Squeeze Watcher (Ghost pattern, default-OFF via env flag)
_squeeze_watcher_last_run = 0.0
_squeeze_watcher_last_fire_ts = 0.0
_squeeze_watcher_disabled_logged = False

# HM-AS-β.2 Option A pilot (2026-05-08): fire-and-forget thread wrapper
# for the squeeze watcher only — prevents the 34-second Finviz/yfinance
# scan from blocking the single-threaded `schedule.run_pending()` queue.
# Background: scheduler diagnostic at docs/HM-AS-B_SCHEDULER_DIAGNOSTIC.md
# documented 137-min backlog spikes; squeeze watcher's first fire was
# queued for 37+ min after restart on 2026-05-08. Pilot scope: this one
# job only. Broader rollout is HM-AS-β.3 after 1-2 weeks of soak.
_squeeze_bg_lock = threading.Lock()  # prevents overlap if a prior 30-min run is still in flight


@_hm_bq_instr("_bg_squeeze_watcher")
def _bg_squeeze_watcher():
    """Daemon-thread wrapper around run_squeeze_watcher() — never blocks
    the scheduler. If a prior invocation is still running, skip this tick."""
    if not _squeeze_bg_lock.acquire(blocking=False):
        console.log("[dim]Squeeze Watcher bg: prior tick still running — skip")
        return
    def _runner():
        try:
            run_squeeze_watcher()
        finally:
            _squeeze_bg_lock.release()
    threading.Thread(target=_runner, daemon=True, name="sched_squeeze_watcher").start()


@_hm_bq_instr("run_squeeze_watcher")
def run_squeeze_watcher():
    """Run engine.squeeze_scanner.run_scan() on a 30-min cadence and persist
    candidates to squeeze_watch table. Default-OFF via env flag — set
    SQUEEZE_WATCHER_ENABLED=True in .env to activate.

    HM-AO-β Ghost Watcher pattern — surfaces candidates only; does NOT
    write to signals table and does NOT route to paper_trader. Promotion
    to voter is a future epic gated on >=30d of evidence.
    """
    global _squeeze_watcher_last_run, _squeeze_watcher_last_fire_ts, _squeeze_watcher_disabled_logged
    import os as _os
    import time as _t

    if _os.environ.get("SQUEEZE_WATCHER_ENABLED", "").lower() not in ("1", "true", "yes", "on"):
        if not _squeeze_watcher_disabled_logged:
            console.log("[dim]Squeeze Watcher: SQUEEZE_WATCHER_ENABLED not set — skipping (HM-AO-β default-off)")
            _squeeze_watcher_disabled_logged = True
        return

    _now_ts = _t.time()
    # HM-AS-β cadence drift observability — target 1800s (30 min)
    if _squeeze_watcher_last_fire_ts > 0:
        _interval = _now_ts - _squeeze_watcher_last_fire_ts
        if _interval > 2400:  # 40-min ceiling before drift alarm
            logger.warning(
                "[HM-AS-β] squeeze_watcher cadence drift: "
                "%.1fs since last fire (target: 1800s)", _interval
            )
    _squeeze_watcher_last_fire_ts = _now_ts

    # Hard dedupe — at most one fire per 25 min even if scheduler ticks faster
    if _now_ts - _squeeze_watcher_last_run < 1500:
        return
    _squeeze_watcher_last_run = _now_ts

    try:
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
    except Exception:
        # If risk manager unavailable, default to skip (don't crash main loop)
        return

    try:
        # Lazy import — keeps load-time clean even if engine.squeeze_scanner
        # import fails for any reason. main.py never sees the import error.
        from engine.squeeze_scanner import run_scan as _squeeze_run_scan
        result = _squeeze_run_scan(force=True)
        n = len(result.get("results") or [])
        ws = result.get("watch_persist") or {}
        console.log(
            f"[cyan]Squeeze Watcher: {n} candidates "
            f"(inserted={ws.get('inserted', 0)} "
            f"deferred={ws.get('deferred', 0)} "
            f"ntfy={ws.get('ntfy_fired', 0)})"
        )
    except Exception as e:
        console.log(f"[yellow]Squeeze Watcher error: {type(e).__name__}: {e!r}")


# HM-SQUEEZE-BBKC-COMPRESSION 2026-05-24 — BB/KC volatility-compression
# scanner. Same daemon-thread wrapper pattern as squeeze watcher above.
# Default-OFF via BBKC_SQUEEZE_WATCHER_ENABLED env flag.
_bbkc_squeeze_bg_lock = threading.Lock()
_bbkc_squeeze_watcher_last_run = 0.0
_bbkc_squeeze_watcher_last_fire_ts = 0.0
_bbkc_squeeze_watcher_disabled_logged = False


@_hm_bq_instr("_bg_bbkc_squeeze_watcher")
def _bg_bbkc_squeeze_watcher():
    """Daemon-thread wrapper around run_bbkc_squeeze_watcher(). Skip if a
    prior 30-min run is still in flight."""
    if not _bbkc_squeeze_bg_lock.acquire(blocking=False):
        console.log("[dim]BBKC Squeeze Watcher bg: prior tick still running — skip")
        return
    def _runner():
        try:
            run_bbkc_squeeze_watcher()
        finally:
            _bbkc_squeeze_bg_lock.release()
    threading.Thread(
        target=_runner, daemon=True, name="sched_bbkc_squeeze_watcher"
    ).start()


@_hm_bq_instr("run_bbkc_squeeze_watcher")
def run_bbkc_squeeze_watcher():
    """30-min cadence wrapper for engine.bbkc_squeeze_scanner.run_scan.
    Default-OFF via BBKC_SQUEEZE_WATCHER_ENABLED env flag.

    Ghost Watcher pattern — surfaces candidates only; does NOT write to
    signals table and does NOT route to paper_trader.
    """
    global _bbkc_squeeze_watcher_last_run, _bbkc_squeeze_watcher_last_fire_ts
    global _bbkc_squeeze_watcher_disabled_logged
    import os as _os
    import time as _t

    if _os.environ.get("BBKC_SQUEEZE_WATCHER_ENABLED", "").lower() not in (
        "1", "true", "yes", "on"
    ):
        if not _bbkc_squeeze_watcher_disabled_logged:
            console.log(
                "[dim]BBKC Squeeze Watcher: BBKC_SQUEEZE_WATCHER_ENABLED not set"
                " — skipping (HM-SQUEEZE-BBKC default-off)"
            )
            _bbkc_squeeze_watcher_disabled_logged = True
        return

    _now_ts = _t.time()
    if _bbkc_squeeze_watcher_last_fire_ts > 0:
        _interval = _now_ts - _bbkc_squeeze_watcher_last_fire_ts
        if _interval > 2400:
            logger.warning(
                "[HM-AS-β] bbkc_squeeze_watcher cadence drift: "
                "%.1fs since last fire (target: 1800s)", _interval
            )
    _bbkc_squeeze_watcher_last_fire_ts = _now_ts

    if _now_ts - _bbkc_squeeze_watcher_last_run < 1500:
        return
    _bbkc_squeeze_watcher_last_run = _now_ts

    try:
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
    except Exception:
        return

    try:
        from engine.bbkc_squeeze_scanner import run_scan as _bbkc_run_scan
        result = _bbkc_run_scan(force=True)
        n = len(result.get("results") or [])
        ws = result.get("watch_persist") or {}
        console.log(
            f"[cyan]BBKC Squeeze Watcher: {n} in-squeeze "
            f"(scanned={result.get('candidate_count', 0)} "
            f"inserted={ws.get('inserted', 0)} "
            f"deferred={ws.get('deferred', 0)} "
            f"ntfy={ws.get('ntfy_fired', 0)})"
        )
    except Exception as e:
        console.log(
            f"[yellow]BBKC Squeeze Watcher error: {type(e).__name__}: {e!r}"
        )


# ── HM-BK confirmatory scanners (shadow-first, default-OFF) ────────────────
# Each gated on its OWN config flag. When the flag is False the wrapper logs once
# and skips — no scan, no NTFY, no DB write. Daemon-thread so a slow nightly bulk
# fetch never blocks the scheduler tick (same pattern as the squeeze watchers).
_bk_avwap_lock = threading.Lock()
_bk_box_lock = threading.Lock()
_bk_avwap_off_logged = False
_bk_box_off_logged = False


@_hm_bq_instr("run_bk_avwap_scan")
def run_bk_avwap_scan():
    """HM-BK-B nightly anchored-VWAP confirmatory scan. Default-OFF via
    AVWAP_CONFIRMATORY_VOTE_ENABLED — shadow-quiet (no ntfy/count) until flipped."""
    global _bk_avwap_off_logged
    try:
        from config import AVWAP_CONFIRMATORY_VOTE_ENABLED as _en
    except Exception:
        _en = False
    if not _en:
        if not _bk_avwap_off_logged:
            console.log("[dim]HM-BK-B aVWAP scan: AVWAP_CONFIRMATORY_VOTE_ENABLED off — skipping")
            _bk_avwap_off_logged = True
        return
    if not _bk_avwap_lock.acquire(blocking=False):
        return

    def _runner():
        try:
            from engine.bk_avwap_scanner import run_scan
            r = run_scan()  # persist + shadow_ntfy follow the flag (ON here)
            console.log(
                f"[cyan]HM-BK-B aVWAP: {r.get('signals',0)} signals "
                f"(bull={r.get('bull',0)} bear={r.get('bear',0)} "
                f"confluence={r.get('confluence',0)} scanned={r.get('scanned',0)})"
            )
        except Exception as e:
            console.log(f"[yellow]HM-BK-B aVWAP error: {type(e).__name__}: {e!r}")
        finally:
            _bk_avwap_lock.release()
    threading.Thread(target=_runner, daemon=True, name="sched_bk_avwap").start()


@_hm_bq_instr("run_bk_box_scan")
def run_bk_box_scan():
    """HM-BK-C nightly tight-box breakout confirmatory scan (runs after B —
    reuses B's 30-min bulk-OHLCV cache). Default-OFF via BOX_CONFIRMATORY_VOTE_ENABLED."""
    global _bk_box_off_logged
    try:
        from config import BOX_CONFIRMATORY_VOTE_ENABLED as _en
    except Exception:
        _en = False
    if not _en:
        if not _bk_box_off_logged:
            console.log("[dim]HM-BK-C box scan: BOX_CONFIRMATORY_VOTE_ENABLED off — skipping")
            _bk_box_off_logged = True
        return
    if not _bk_box_lock.acquire(blocking=False):
        return

    def _runner():
        try:
            from engine.bk_box_scanner import run_scan
            r = run_scan()
            console.log(
                f"[cyan]HM-BK-C box: {r.get('signals',0)} breakouts "
                f"(bull={r.get('bull',0)} bear={r.get('bear',0)} "
                f"tight_boxes={r.get('boxes',0)} scanned={r.get('scanned',0)})"
            )
        except Exception as e:
            console.log(f"[yellow]HM-BK-C box error: {type(e).__name__}: {e!r}")
        finally:
            _bk_box_lock.release()
    threading.Thread(target=_runner, daemon=True, name="sched_bk_box").start()


_bk_orb_lock = threading.Lock()
_bk_orb_off_logged = False


@_hm_bq_instr("run_bk_orb_scan")
def run_bk_orb_scan():
    """HM-BK-A intraday opening-range-breakout confirmatory scan. Default-OFF via
    ORB_CONFIRMATORY_VOTE_ENABLED. Only acts in the post-OR window (09:46–12:00 ET);
    run_scan guards against re-firing the same symbol/day."""
    global _bk_orb_off_logged
    try:
        from config import ORB_CONFIRMATORY_VOTE_ENABLED as _en
    except Exception:
        _en = False
    if not _en:
        if not _bk_orb_off_logged:
            console.log("[dim]HM-BK-A ORB scan: ORB_CONFIRMATORY_VOTE_ENABLED off — skipping")
            _bk_orb_off_logged = True
        return
    # window gate: only the post-opening-range window (ET)
    try:
        from zoneinfo import ZoneInfo
        et = datetime.now(ZoneInfo("America/New_York"))
        mins = et.hour * 60 + et.minute
        if not (9 * 60 + 46 <= mins <= 12 * 60):
            return
    except Exception:
        pass
    if not _bk_orb_lock.acquire(blocking=False):
        return

    def _runner():
        try:
            from engine.bk_orb_scanner import run_scan
            r = run_scan()
            if r.get("signals"):
                console.log(
                    f"[cyan]HM-BK-A ORB: {r.get('signals',0)} breakouts "
                    f"(bull={r.get('bull',0)} bear={r.get('bear',0)} scanned={r.get('scanned',0)})"
                )
        except Exception as e:
            console.log(f"[yellow]HM-BK-A ORB error: {type(e).__name__}: {e!r}")
        finally:
            _bk_orb_lock.release()
    threading.Thread(target=_runner, daemon=True, name="sched_bk_orb").start()


# ── Ollie Machine P3 scheduled loop (SIM-only, tracking-mode) ──────────────
# Assembles the P1/P2 convergence modules (engine/ollie_machine_loop.py) into a
# daily evaluate→enter + intraday exit-monitor. Writes ollie_machine_ledger ONLY —
# NEVER paper_trader.buy()/executor (sim_enter is a direct INSERT). Default-OFF via
# OLLIE_MACHINE_LOOP_ENABLED. The player stays can_trade_live=0 + tracking-mode +
# absent from every scan/exec roster, so the live trader's own loops never act on it.
_ollie_machine_daily_lock = threading.Lock()
_ollie_machine_enter_lock = threading.Lock()
_ollie_machine_exits_lock = threading.Lock()
_ollie_machine_disabled_logged = False


def _ollie_machine_enabled() -> bool:
    import os as _os
    return _os.environ.get("OLLIE_MACHINE_LOOP_ENABLED", "").lower() in (
        "1", "true", "yes", "on"
    )


@_hm_bq_instr("_bg_ollie_machine_daily")
def _bg_ollie_machine_daily():
    """P3 daily evaluate→enter (post-close, fresh nightly signals). SIM/tracking.
    No market-hours gate — runs post-close after the nightly RS/Minervini rebuild."""
    global _ollie_machine_disabled_logged
    if not _ollie_machine_enabled():
        if not _ollie_machine_disabled_logged:
            console.log("[dim]Ollie Machine loop: OLLIE_MACHINE_LOOP_ENABLED not set — skipping (P3 default-off)")
            _ollie_machine_disabled_logged = True
        return
    if not _ollie_machine_daily_lock.acquire(blocking=False):
        console.log("[dim]Ollie Machine daily: prior run still in flight — skip")
        return

    def _runner():
        try:
            # HM-OLLIE-MACHINE-BRACKET-WINDOW 2026-06-05: 21:00 does pick SELECTION
            # only. Bracketing + SIM-entry moved to _bg_ollie_machine_enter (06:30),
            # when /api/trade-levels is healthy (the 21:00 window had it cold).
            from engine.ollie_machine_loop import run_pick_generation
            r = run_pick_generation()
            console.log(
                f"[cyan]Ollie Machine PICKS (SIM/tracking): universe "
                f"{r['universe_pre']}→{r['universe_post']} | top {r['top']} "
                f"(bracket+enter at 06:30)"
            )
        except Exception as e:
            console.log(f"[yellow]Ollie Machine picks error: {type(e).__name__}: {e!r}")
        finally:
            _ollie_machine_daily_lock.release()

    threading.Thread(target=_runner, daemon=True, name="sched_ollie_machine_daily").start()


@_hm_bq_instr("_bg_ollie_machine_enter")
def _bg_ollie_machine_enter():
    """P3 pre-open bracket+enter (06:30, market window). SIM/tracking — brackets the
    latest 21:00 picks via /api/trade-levels + ledger-direct SIM-enter. Split out of
    the 21:00 job so the trade-levels endpoint is healthy at run time
    (HM-OLLIE-MACHINE-BRACKET-WINDOW)."""
    if not _ollie_machine_enabled():
        return
    if not _ollie_machine_enter_lock.acquire(blocking=False):
        console.log("[dim]Ollie Machine enter: prior run still in flight — skip")
        return

    def _runner():
        try:
            from engine.ollie_machine_loop import run_bracket_and_enter
            r = run_bracket_and_enter()
            console.log(
                f"[cyan]Ollie Machine ENTER (SIM/tracking): entered "
                f"{[o['symbol'] for o in r['opened']]} | skipped {len(r['skipped'])} | "
                f"breaker={'TRIPPED' if r['breaker_tripped'] else 'ok'}"
            )
        except Exception as e:
            console.log(f"[yellow]Ollie Machine enter error: {type(e).__name__}: {e!r}")
        finally:
            _ollie_machine_enter_lock.release()

    threading.Thread(target=_runner, daemon=True, name="sched_ollie_machine_enter").start()


@_hm_bq_instr("_bg_ollie_machine_exits")
def _bg_ollie_machine_exits():
    """P3 intraday exit-monitor (RTH-gated). SIM/tracking — ledger-direct close on stop/tp."""
    if not _ollie_machine_enabled():
        return
    try:
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
    except Exception:
        return
    if not _ollie_machine_exits_lock.acquire(blocking=False):
        return

    def _runner():
        try:
            from engine.ollie_machine_loop import run_exit_monitor
            mon = run_exit_monitor()
            if mon.get("closed"):
                console.log(
                    "[cyan]Ollie Machine EXITS (SIM): closed "
                    f"{[(c['symbol'], c['reason'], c['realized_pnl']) for c in mon['closed']]}"
                )
        except Exception as e:
            console.log(f"[yellow]Ollie Machine exits error: {type(e).__name__}: {e!r}")
        finally:
            _ollie_machine_exits_lock.release()

    threading.Thread(target=_runner, daemon=True, name="sched_ollie_machine_exits").start()


# ── HM-SHORT-ENGINE — sell-the-news shadow scanner (Path B, observation-only) ──
# Post-earnings fade → SHORT votes into the W0 shadow substrate via
# engine/sell_the_news_scanner.py (POST :9000/api/signal, agent
# 'shadow-bridge:sell_the_news'). NEVER executes: shadow-bridge prefix is refused
# at the paper_trader.buy chokepoint + the ai_players row is can_trade_live=0.
# Default-OFF via SELL_THE_NEWS_ENABLED (mirrors OLLIE_MACHINE_LOOP_ENABLED).
_sell_the_news_lock = threading.Lock()


def _sell_the_news_enabled() -> bool:
    import os as _os
    return _os.environ.get("SELL_THE_NEWS_ENABLED", "").lower() in ("1", "true", "yes", "on")


@_hm_bq_instr("_bg_sell_the_news")
def _bg_sell_the_news():
    """HM-SHORT-ENGINE shadow scanner — post-earnings fade → SHORT shadow signals.
    Observation-only (agent 'shadow-bridge:sell_the_news'); RTH-gated; default-OFF."""
    if not _sell_the_news_enabled():
        return
    try:
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
    except Exception:
        return
    if not _sell_the_news_lock.acquire(blocking=False):
        return

    def _runner():
        try:
            from engine.sell_the_news_scanner import scan_and_emit
            m = scan_and_emit()
            console.log(
                f"[cyan]Sell-The-News (SHADOW): checked {m['checked']} | "
                f"qualified {m['qualified']} | emitted {m['emitted']} | regime {m.get('regime')}"
            )
        except Exception as e:
            console.log(f"[yellow]Sell-The-News error: {type(e).__name__}: {e!r}")
        finally:
            _sell_the_news_lock.release()

    threading.Thread(target=_runner, daemon=True, name="sched_sell_the_news").start()


# ── Ollie Machine P4 promotion gate (SIM, read-only observability) ────────────
# Scores the SIM ledger (Observe/Eligible/Promote) into ollie_machine_p4_status.
# PURE measurement: zero executor calls, writes only its status row, never touches
# ai_players (can_trade_live stays 0); Eligible/Promote raise a flag, never auto-advance.
# Runs unconditionally (measuring an empty ledger is harmless + reports Observe) — the
# eval is internally rate-limited to "every 10 closed trades OR weekly" via the persisted
# status row, so the daily tick is cheap and carries no lazy module state.
_ollie_machine_p4_lock = threading.Lock()


@_hm_bq_instr("_bg_ollie_machine_p4_gate")
def _bg_ollie_machine_p4_gate():
    """P4 gate tick — eval if due (10-closed / weekly cadence). SIM/read-only."""
    if not _ollie_machine_p4_lock.acquire(blocking=False):
        return

    def _runner():
        try:
            from engine.ollie_machine_p4_gate import run_eval
            r = run_eval()
            if r.get("evaluated"):
                m = r["metrics"]
                console.log(
                    f"[cyan]Ollie Machine P4 GATE: tier={r['tier']} "
                    f"(prev={r['prev_tier']}) | {m['count']} closed | "
                    f"WR={(m['win_rate'] or 0) * 100:.0f}% exp={m['expectancy_r'] or 0:+.2f}R | "
                    f"failed={r['failed_floors'] or 'none'} | flag={r['flag_raised']}"
                )
        except Exception as e:
            console.log(f"[yellow]Ollie Machine P4 gate error: {type(e).__name__}: {e!r}")
        finally:
            _ollie_machine_p4_lock.release()

    threading.Thread(target=_runner, daemon=True, name="sched_ollie_machine_p4_gate").start()


# HM-SOURCE-HEALTH-WATCHER (2026-06-02) — dead-man's-switch for the independent
# source-health watcher cron (scripts/source_health_watcher.py). The watcher writes
# data/source_health_watcher_heartbeat.json each run; this in-process check reads it
# and NTFYs if it's stale. Cron-watcher watched by the always-on trader = a DIFFERENT
# mechanism (CLAUDE.md "Alarms must not share a failure mode with what they watch":
# who-watches-the-watcher). Read-only — reads one JSON file, may send NTFY.
_SOURCE_HEALTH_HB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "source_health_watcher_heartbeat.json"
)
# Stale if no heartbeat for > this many seconds (~3.5 missed 10-min runs).
_SOURCE_HEALTH_HB_STALE_S = int(os.environ.get("SOURCE_HEALTH_HB_STALE_S", str(35 * 60)))


def _bg_source_health_dms():
    """Alarm if the source-health watcher's heartbeat has gone stale (or never
    ran). Different mechanism than the cron it guards, so one failure can't take
    out both the watcher and its watchdog."""
    try:
        import json as _json
        import time as _time
        from engine.alert_channels import send_alert, AlertLevel

        if not os.path.exists(_SOURCE_HEALTH_HB_PATH):
            send_alert(
                message=("source-health watcher heartbeat MISSING "
                         f"({_SOURCE_HEALTH_HB_PATH}) — the watcher cron may never have "
                         "run. Source-staleness alerting (incl. the Movers-gap backstop) "
                         "is DOWN."),
                level=AlertLevel.WARNING,
                alert_type="source-health-watcher-dead",
                rate_limit_secs=3600,
            )
            return

        with open(_SOURCE_HEALTH_HB_PATH) as f:
            hb = _json.load(f)
        last_run = float(hb.get("last_run", 0) or 0)
        age = _time.time() - last_run
        if age > _SOURCE_HEALTH_HB_STALE_S:
            mins = int(age // 60)
            send_alert(
                message=(f"source-health watcher heartbeat STALE — last run {mins} min ago "
                         f"(threshold {_SOURCE_HEALTH_HB_STALE_S // 60} min). The watcher cron "
                         "is dead/hung; source-staleness alerting is DOWN. Check "
                         "logs/source_health_watcher_cron.log + crontab."),
                level=AlertLevel.WARNING,
                alert_type="source-health-watcher-stale",
                rate_limit_secs=3600,
            )
    except Exception as e:
        console.log(f"[yellow]source-health DMS error: {type(e).__name__}: {e!r}")


# HM-RS-RANK-VS-SPY 2026-05-24 — nightly 12wk relative-strength rank scanner.
# Default-OFF via RS_RANK_ENABLED env flag. Foundational for downstream
# leader-composite scans (Minervini Trend Template etc.).
_rs_rank_bg_lock = threading.Lock()
_rs_rank_disabled_logged = False


@_hm_bq_instr("_bg_rs_rank")
def _bg_rs_rank():
    """Daemon-thread wrapper around run_rs_rank_nightly()."""
    if not _rs_rank_bg_lock.acquire(blocking=False):
        console.log("[dim]RS-Rank bg: prior tick still running — skip")
        return
    def _runner():
        try:
            run_rs_rank_nightly()
        finally:
            _rs_rank_bg_lock.release()
    threading.Thread(
        target=_runner, daemon=True, name="sched_rs_rank"
    ).start()


@_hm_bq_instr("run_rs_rank_nightly")
def run_rs_rank_nightly():
    """Nightly RS-rank rebuild. Default-OFF via RS_RANK_ENABLED.

    No market-hours gate — runs post-close (20:30 AZ); weekends fine
    (Friday's close is the reference).
    """
    global _rs_rank_disabled_logged
    import os as _os

    if _os.environ.get("RS_RANK_ENABLED", "").lower() not in (
        "1", "true", "yes", "on"
    ):
        if not _rs_rank_disabled_logged:
            console.log(
                "[dim]RS-Rank: RS_RANK_ENABLED not set — skipping "
                "(HM-RS-RANK-VS-SPY default-off)"
            )
            _rs_rank_disabled_logged = True
        return

    try:
        from engine.rs_rank import run_rs_rank as _rs_rank_run
        result = _rs_rank_run(force=True)
        top1 = (result.get("top10") or [{}])[0]
        console.log(
            f"[cyan]RS-Rank: scanned={result.get('scanned', 0)} "
            f"persisted={result.get('persisted', 0)} "
            f"spy_12wk={result.get('spy_return_pct', 0):.2f}% "
            f"top1={top1.get('symbol', '—')}"
        )
    except Exception as e:
        console.log(
            f"[yellow]RS-Rank error: {type(e).__name__}: {e!r}"
        )


# HM-MINERVINI-TREND-FILTER 2026-05-24 — nightly Stage-2 uptrend template
# evaluation. Default-OFF via MINERVINI_FILTER_ENABLED env flag. Depends on
# rs_rank table being fresh (LEFT JOIN'd at scan time for the rs_pass
# field); scheduled 15 min after the rs_rank nightly job.
_minervini_bg_lock = threading.Lock()
_minervini_disabled_logged = False


@_hm_bq_instr("_bg_minervini_filter")
def _bg_minervini_filter():
    """Daemon-thread wrapper around run_minervini_filter_nightly()."""
    if not _minervini_bg_lock.acquire(blocking=False):
        console.log("[dim]Minervini bg: prior tick still running — skip")
        return
    def _runner():
        try:
            run_minervini_filter_nightly()
        finally:
            _minervini_bg_lock.release()
    threading.Thread(
        target=_runner, daemon=True, name="sched_minervini_filter"
    ).start()


@_hm_bq_instr("run_minervini_filter_nightly")
def run_minervini_filter_nightly():
    """Nightly Minervini Trend Template rebuild. Default-OFF via
    MINERVINI_FILTER_ENABLED. Runs post-close at 20:45 AZ (15 min after
    rs_rank at 20:30 so the rs_pass join sees fresh data)."""
    global _minervini_disabled_logged
    import os as _os

    if _os.environ.get("MINERVINI_FILTER_ENABLED", "").lower() not in (
        "1", "true", "yes", "on"
    ):
        if not _minervini_disabled_logged:
            console.log(
                "[dim]Minervini Filter: MINERVINI_FILTER_ENABLED not set "
                "— skipping (HM-MINERVINI-TREND-FILTER default-off)"
            )
            _minervini_disabled_logged = True
        return

    try:
        from engine.minervini_filter import run_minervini_scan
        result = run_minervini_scan(force=True)
        top = (result.get("top_pass_symbols") or [])[:3]
        console.log(
            f"[cyan]Minervini: scanned={result.get('scanned', 0)} "
            f"persisted={result.get('persisted', 0)} "
            f"passing={result.get('passing', 0)} "
            f"top3={','.join(top) if top else '—'}"
        )
    except Exception as e:
        console.log(
            f"[yellow]Minervini error: {type(e).__name__}: {e!r}"
        )


_theta_last_run = 0.0

@_hm_bq_instr("run_theta_scan")
def run_theta_scan():
    """Theta Collection Scanner: find premium-selling opportunities. Runs every 4 hours."""
    global _theta_last_run
    import time as _time
    now = _time.time()
    if now - _theta_last_run < 14400:  # 4-hour minimum between scans
        return
    _theta_last_run = now
    try:
        from engine.universe import get_active_universe
        from engine.theta_scanner import scan_all_theta
        results = scan_all_theta(get_active_universe())
        if results:
            top = results[0]
            console.log(
                f"[bold cyan]⏱ Theta scan: {len(results)} opportunity(s) — "
                f"top: {top['ticker']} score={top['theta_score']}/10 "
                f"IVR={top['iv_rank']:.0f}% · {top['strategy_type']}"
            )
        else:
            console.log("[dim]⏱ Theta scan: no qualifying opportunities this run")
    except Exception as e:
        console.log(f"[red]Theta scan error: {e}")


_imbalance_last_run = 0.0

@_hm_bq_instr("run_imbalance_scan")
def run_imbalance_scan():
    """Supply/Demand Imbalance Zone Scanner: detect FVG zones across daily+hourly candles."""
    global _imbalance_last_run
    import time as _time
    now = _time.time()
    if now - _imbalance_last_run < 7200:  # 2-hour minimum between runs
        return
    _imbalance_last_run = now
    try:
        from engine.universe import get_active_universe
        from engine.imbalance_detector import scan_all_imbalances
        results = scan_all_imbalances(get_active_universe())
        total = sum(len(z) for z in results.values())
        console.log(f"[cyan]Imbalance scan complete: {total} zone(s) across {len(results)} symbol(s)")
    except Exception as e:
        console.log(f"[red]Imbalance scan error: {e}")


_sma_last_run = 0.0

@_hm_bq_instr("run_sma_scan")
def run_sma_scan():
    """200 SMA Filter: scan watchlist for Bounce/Breakdown/Reclaim signals. Runs every 4 hours."""
    global _sma_last_run
    import time as _time
    now = _time.time()
    if now - _sma_last_run < 14400:  # 4 hours between full scans
        return
    _sma_last_run = now
    try:
        from engine.universe import get_active_universe
        from engine.sma_filter import scan_all_sma_signals
        results = scan_all_sma_signals(get_active_universe())
        signals = [v for v in results.values() if v.get("signal_type")]
        testing = [v for v in results.values() if v.get("is_testing") and not v.get("signal_type")]
        console.log(f"[cyan]200 SMA: {len(results)} stocks scanned, "
                    f"{len(signals)} signals, {len(testing)} testing SMA")
    except Exception as e:
        console.log(f"[red]SMA scan error: {e}")


@_hm_bq_instr("run_strategy_race")
def run_strategy_race():
    """Strategy Race: update AI vs SPY comparison (daily)."""
    if not should_run_task("run_strategy_race", throttle_mins=120):
        return
    try:
        from engine.strategy_race import update_strategy_race
        result = update_strategy_race()
        if result and result.get("history"):
            latest = result["history"][-1]
            console.log(
                f"[dim]Strategy Race: AI ${latest['ai_avg_value']:,.2f} vs SPY ${latest['spy_value']:,.2f}[/dim]"
            )
    except Exception as e:
        console.log(f"[red]Strategy race error: {e}")


_weekly_picks_sent = False

@_hm_bq_instr("run_weekly_picks")
def run_weekly_picks():
    """Weekly Picks: Sunday 6 PM ET — top 5 conviction picks."""
    global arena, _weekly_picks_sent
    from datetime import datetime
    import pytz

    try:
        et = pytz.timezone("US/Eastern")
    except Exception:
        return

    now = datetime.now(et)

    # Only run on Sunday between 6:00-6:10 PM ET
    if now.weekday() != 6 or now.hour != 18 or now.minute > 10:
        if now.weekday() != 6:
            _weekly_picks_sent = False
        return

    if _weekly_picks_sent:
        return

    if arena is None:
        return

    try:
        from engine.weekly_picks import run_weekly_picks as _run_wp
        from engine.universe import get_active_universe
        # Use first available provider
        pid = list(arena.providers.keys())[0]
        provider = arena.providers[pid]
        _run_wp(provider, get_active_universe())
        _weekly_picks_sent = True
    except Exception as e:
        console.log(f"[red]Weekly picks error: {e}")


@_hm_bq_instr("run_cross_asset_check")
def run_cross_asset_check():
    """Cross-Asset Monitor: check VIX spikes, macro signals."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.cross_asset import check_vix_auto_reduce, get_cross_asset_monitor
        reduced = check_vix_auto_reduce()
        if reduced:
            console.log("[bold red]Cross-Asset: VIX spike — position sizes auto-reduced 50%")

        data = get_cross_asset_monitor()
        signals = data.get("signals", [])
        if signals:
            for s in signals[:2]:
                console.log(f"[yellow]Cross-Asset: {s['signal']} — {s['description'][:80]}")
    except Exception as e:
        console.log(f"[red]Cross-asset error: {e}")


@_hm_bq_instr("run_skew_check")
def run_skew_check():
    """Put/Call Skew Monitor: check for extreme fear."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.put_call_skew import check_extreme_skew
        check_extreme_skew()
    except Exception as e:
        console.log(f"[red]Skew check error: {e}")


@_hm_bq_instr("run_flow_lean")
def run_flow_lean():
    """Market Flow Lean: aggregate options premium directional bias every 15 min."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.market_flow import refresh_flow_lean
        refresh_flow_lean()
    except Exception as e:
        console.log(f"[red]Flow lean error: {e}")


@_hm_bq_instr("run_ai_saas_disruption")
def run_ai_saas_disruption():
    """AI SaaS Disruption Scanner: monitors IGV + 13 SaaS names for disruption signals."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.ai_saas_disruption_scanner import run_scan
        result = run_scan()
        sigs = result.get("signals", [])
        if sigs:
            console.log(
                f"[bold cyan]AI SaaS: {len(sigs)} signal(s) — "
                + ", ".join(f"{s['symbol']}({s['direction']})" for s in sigs)
            )
    except Exception as e:
        console.log(f"[red]AI SaaS Disruption error: {e}")


# ── Ready Room ──────────────────────────────────────────────────────────────

_ready_room_slots_done_today: set = set()

# Ready Room schedule: (slot_name, hour_az, minute_az)
# Arizona = ET - 3 (no DST): 8:00 ET=5:00 AZ, 9:15 ET=6:15 AZ, 12:00 ET=9:00 AZ, 3:30 ET=12:30 AZ
_READY_ROOM_SCHEDULE = [
    ("pre_open",   5,  0),   # 5:00 AM AZ / 8:00 AM ET  — pre-open gameplan
    ("post_open",  6, 15),   # 6:15 AM AZ / 9:15 AM ET  — post-open update
    ("midday",     9,  0),   # 9:00 AM AZ / 12:00 PM ET — midday reset
    ("pre_close", 12, 30),   # 12:30 PM AZ / 3:30 PM ET — pre-close positioning
]


@_hm_bq_instr("run_ready_room")
def run_ready_room():
    """Ready Room briefing at 4 scheduled ET times on weekdays."""
    global _ready_room_slots_done_today
    from datetime import datetime
    import pytz

    try:
        az = pytz.timezone("US/Arizona")
        now = az_now()
    except Exception:
        return

    # Reset flags overnight
    if now.hour < 4:
        _ready_room_slots_done_today = set()
        return

    # Weekdays only
    if now.weekday() >= 5:
        return

    for slot, sched_hour, sched_min in _READY_ROOM_SCHEDULE:
        if slot in _ready_room_slots_done_today:
            continue
        now_mins = now.hour * 60 + now.minute
        sched_mins = sched_hour * 60 + sched_min
        # Fire within a 10-minute window after scheduled time
        if sched_mins <= now_mins <= sched_mins + 10:
            _ready_room_slots_done_today.add(slot)
            try:
                from engine.ready_room import generate_ready_room_briefing
                console.log(f"[cyan]Ready Room: generating {slot} briefing…")
                result = generate_ready_room_briefing(force=True)
                if result and not result.get("error"):
                    stype = result.get("session_type", "?")
                    spot  = result.get("spot_price", 0)
                    console.log(f"[bold green]Ready Room [{slot}]: {stype} — SPY ${spot:.2f}")
                else:
                    console.log(f"[yellow]Ready Room [{slot}]: {result.get('error', 'no data')}")
            except Exception as e:
                console.log(f"[red]Ready Room [{slot}] error: {e}")


_oi_snapshot_done_today = False


@_hm_bq_instr("run_oi_morning_snapshot")
def run_oi_morning_snapshot():
    """Take SPY OI baseline at market open (6:30 AM AZ / 9:30 AM ET)."""
    global _oi_snapshot_done_today
    from datetime import datetime
    import pytz

    try:
        az = pytz.timezone("US/Arizona")
        now = az_now()
    except Exception:
        return

    if now.hour < 4:
        _oi_snapshot_done_today = False
        return
    if now.weekday() >= 5:
        return
    if _oi_snapshot_done_today:
        return

    # Fire between 6:30–6:40 AM AZ (9:30–9:40 AM ET)
    now_mins = now.hour * 60 + now.minute
    if 390 <= now_mins <= 400:  # 6:30–6:40 AZ
        _oi_snapshot_done_today = True
        try:
            from engine.oi_tracker import take_morning_snapshot
            console.log("[cyan]OI Tracker: taking morning baseline snapshot…")
            res = take_morning_snapshot("SPY")
            console.log(f"[green]OI Tracker: {res.get('snaps_saved', 0)} strikes saved.")
        except Exception as e:
            console.log(f"[red]OI Tracker morning snapshot error: {e}")


_cto_slots_done_today = set()

# CTO briefing schedule: (briefing_type, hour_az, minute_az)
# Arizona = ET - 3 (no DST)
_CTO_SCHEDULE = [
    ("pre_market",  6,  0),   # 6:00 AM AZ / 9:00 AM ET
    ("post_open",   6, 45),   # 6:45 AM AZ / 9:45 AM ET
    ("pre_close",  12, 45),   # 12:45 PM AZ / 3:45 PM ET
    ("post_close", 13, 15),   # 1:15 PM AZ / 4:15 PM ET
]


@_hm_bq_instr("run_cto_advisory")
def run_cto_advisory():
    """CTO Advisory: 4x daily briefings at scheduled Arizona times."""
    global _cto_slots_done_today
    from datetime import datetime
    import pytz

    try:
        az = pytz.timezone("US/Arizona")
        now = az_now()
    except Exception:
        return

    # Reset flags overnight
    if now.hour < 5:
        _cto_slots_done_today = set()
        return

    # Weekdays only
    if now.weekday() >= 5:
        return

    for btype, sched_hour, sched_min in _CTO_SCHEDULE:
        if btype in _cto_slots_done_today:
            continue
        # Fire within a 10-minute window after scheduled time
        now_mins = now.hour * 60 + now.minute
        sched_mins = sched_hour * 60 + sched_min
        if sched_mins <= now_mins <= sched_mins + 10:
            try:
                from engine.cto_advisor import generate_cto_briefing, BRIEFING_TYPES
                bt_label = BRIEFING_TYPES[btype]["label"]
                console.log(f"[cyan]CTO Advisory: firing {bt_label}...")
                briefing = generate_cto_briefing(briefing_type=btype)
                _cto_slots_done_today.add(btype)
                if briefing:
                    console.log(f"[bold green]CTO Advisory [{bt_label}]: generated ({len(briefing)} chars)")
                else:
                    console.log(f"[dim]CTO Advisory [{bt_label}]: skipped (already generated today)")
            except Exception as e:
                # STRUCTURAL FIX 2026-06-01: a model/generate failure is LOUD + RETRYABLE.
                # Do NOT mark the slot done (that's how the removed devstral-small-2 went dark
                # for 13 days). NTFY fires in generate_cto_briefing; leave the slot open to retry.
                console.log(f"[red]CTO Advisory [{btype}] FAILED (slot left open for retry): {e}")
            break  # Only fire one per cycle


# ── Kirk Advisory persistence (HM-KIRK-REHOME 2026-06-01) ─────────────────────
# The Kirk advisory COMPUTE is fine (live /api/kirk/advisory works), but the persisted
# kirk_advisory_log only got written on-demand → went stale (W1 RED) since 2026-05-18.
# Schedule it at open / midday / close (AZ weekdays) so the log stays fresh and W1 tracks it.
_kirk_slots_done_today: set = set()
_KIRK_SCHEDULE = [(6, 35), (9, 30), (13, 5)]  # AZ: ~market open, midday, ~close
# W1 liveness heartbeat: kirk_advisory_log is EVENT-gated (never logs HOLD, 30-min dedup), so its
# created_at can't signal "the job is alive". This file's mtime is stamped on each successful run
# (below) and is what W1 reads instead — GREEN while the job runs, RED if it stops (preserves the
# death-detection that missed kirk dying for 2 weeks). Market-aware daily handles weekend gaps.
_KIRK_HEARTBEAT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "kirk_advisory.heartbeat")

@_hm_bq_instr("run_kirk_advisory")
def run_kirk_advisory_job():
    global _kirk_slots_done_today
    now = az_now()
    if now.hour < 5:
        _kirk_slots_done_today = set()
        return
    if now.weekday() >= 5:
        return
    now_mins = now.hour * 60 + now.minute
    for sh, sm in _KIRK_SCHEDULE:
        slot = f"{sh:02d}{sm:02d}"
        if slot in _kirk_slots_done_today:
            continue
        sched_mins = sh * 60 + sm
        if sched_mins <= now_mins <= sched_mins + 10:
            try:
                from engine.kirk_advisory import generate_kirk_advisory
                console.log(f"[cyan]Kirk Advisory: firing slot {slot}...")
                generate_kirk_advisory()
                # Stamp the W1 liveness heartbeat regardless of whether an actionable (non-HOLD)
                # row was logged — proves the advisory COMPUTE ran. This is what W1 reads.
                try:
                    from pathlib import Path as _P
                    _P(_KIRK_HEARTBEAT).touch()
                except Exception:
                    pass
                _kirk_slots_done_today.add(slot)
                console.log(f"[bold green]Kirk Advisory [{slot}]: computed + heartbeat stamped")
            except Exception as e:
                # Same loud+retryable contract as CTO: don't mark the slot done on failure.
                console.log(f"[red]Kirk Advisory [{slot}] FAILED (slot left open for retry): {e}")
                try:
                    from engine.alert_channels import _send_ntfy
                    _send_ntfy("Kirk Advisory FAILED", f"slot {slot}: {e}",
                               priority="high", tags="rotating_light", topic="ollietrades-admin")
                except Exception:
                    pass
            break


_team_advisor_slots_done_today: set = set()

@_hm_bq_instr("run_team_advisor")
def run_team_advisor():
    """Advisory Team scheduler: fires at 9:30 AM and 1:30 PM ET on weekdays.

    Calls engine.wb_advisory_team.run_team_scan() which orchestrates three
    sub-advisors (Grok-via-Ollie on Schwab, Troi + Worf on alpaca-mirror paper).
    Output saved to portfolio_advice table; surfaced via /api/wb-team/advice.

    NOT to be confused with engine.kirk_advisory.generate_kirk_advisory() which
    is a separate rule-based advisor on the same Schwab portfolio.
    """
    global _team_advisor_slots_done_today
    from datetime import datetime
    import pytz

    try:
        et = pytz.timezone("US/Eastern")
        now = datetime.now(et)
    except Exception:
        return

    # Reset flags at midnight
    if now.hour < 1:
        _team_advisor_slots_done_today = set()
        return

    # Weekdays only
    if now.weekday() >= 5:
        return

    # Fire within a 20-minute window after each target time
    # Slot "open"  = 9:30 AM ET  (market open)
    # Slot "mid"   = 1:30 PM ET  (midday check)
    slots = [("open", 9, 30), ("mid", 13, 30)]
    for slot_id, target_h, target_m in slots:
        if slot_id in _team_advisor_slots_done_today:
            continue
        now_mins = now.hour * 60 + now.minute
        target_mins = target_h * 60 + target_m
        if target_mins <= now_mins <= target_mins + 20:
            try:
                from engine.wb_advisory_team import run_team_scan
                result = run_team_scan()
                if result.get("skipped"):
                    console.log(f"[dim]Advisory Team [{slot_id}]: skipped — {result.get('reason')}")
                else:
                    g = result.get("grok", {})
                    t = result.get("troi", {})
                    w = result.get("worf", {})
                    console.log(
                        f"[green]Advisory Team [{slot_id}]: "
                        f"Grok {g.get('symbols_analyzed',g.get('error','skip'))} sym "
                        f"${g.get('cost_usd',0):.4f} | "
                        f"Troi {t.get('symbols_analyzed',t.get('error','skip'))} | "
                        f"Worf {w.get('symbols_analyzed',w.get('error','skip'))}"
                    )
            except Exception as e:
                console.log(f"[red]Advisory Team [{slot_id}] error: {e}")
            finally:
                _team_advisor_slots_done_today.add(slot_id)
            break  # One slot per poll cycle


@_hm_bq_instr("run_portfolio_monitor")
def run_portfolio_monitor():
    """Ship's Computer: check Captain's Portfolio every 5 min during market hours."""
    from datetime import datetime
    import pytz
    try:
        et = pytz.timezone("US/Eastern")
        now = datetime.now(et)
        if now.weekday() >= 5:
            return
        h = now.hour
        if not (9 <= h < 16):
            return
        from engine.portfolio_monitor import check_captains_portfolio
        alerts = check_captains_portfolio()
        for a in alerts:
            console.log(
                f"[{'red' if 'STOP' in a['type'] else 'yellow'}]"
                f"Ship's Computer [{a['type']}]: {a['message']}"
            )
    except Exception as e:
        logger.warning("Portfolio monitor error: %s", e)


_elimination_done_this_week = False

@_hm_bq_instr("run_weekly_elimination")
def run_weekly_elimination():
    """Friday elimination: pause models below -15% return at market close."""
    global _elimination_done_this_week
    from engine.risk_manager import RiskManager
    from datetime import datetime
    import pytz

    session = RiskManager.is_market_hours()

    # Reset flag Monday morning
    try:
        et = pytz.timezone("US/Eastern")
        now = datetime.now(et)
        if now.weekday() == 0 and now.hour < 10:  # Monday before 10 AM ET
            _elimination_done_this_week = False
    except Exception:
        pass

    # Only fire Friday post_market
    if session != "post_market":
        return
    try:
        et = pytz.timezone("US/Eastern")
        now = datetime.now(et)
        if now.weekday() != 4:  # Friday = 4
            return
    except Exception:
        return

    if _elimination_done_this_week:
        return

    try:
        from engine.leader_signal import run_weekly_elimination
        eliminated = run_weekly_elimination()
        _elimination_done_this_week = True
        if eliminated:
            names = ", ".join(e["name"] for e in eliminated)
            console.log(f"[bold red]WEEKLY ELIMINATION: {len(eliminated)} model(s) removed — {names}")
        else:
            console.log("[green]Weekly elimination check: all models above -15% threshold")
    except Exception as e:
        console.log(f"[red]Weekly elimination error: {e}")


@_hm_bq_instr("run_fundamental_scan")
def run_fundamental_scan():
    """Fundamental Score Scanner: refresh fundamental data periodically."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.fundamental_score import scan_fundamentals
        results = scan_fundamentals()
        if results:
            top = results[0]
            console.log(f"[cyan]Fundamentals: top {top['symbol']} grade {top['grade']} ({top['total_score']}/100)")
    except Exception as e:
        console.log(f"[red]Fundamental scan error: {e}")


_budget_alerted_today = False

@_hm_bq_instr("run_cost_monitor")
def run_cost_monitor():
    """Check daily API budget and auto-pause expensive losers."""
    global _budget_alerted_today
    from engine.risk_manager import RiskManager
    session = RiskManager.is_market_hours()
    if not session:
        _budget_alerted_today = False
        return

    try:
        from engine.cost_tracker import check_budget_alert, check_auto_pause_losers, get_total_daily_cost

        # Budget alert
        if not _budget_alerted_today:
            alert = check_budget_alert(daily_limit=5.0)
            if alert:
                _budget_alerted_today = True
                console.log(f"[bold red]BUDGET ALERT: ${alert['total_today']:.2f} today (limit ${alert['limit']}). Top: {alert['top_spender']} ${alert['top_cost']:.4f}")
                try:
                    from engine.telegram_alerts import send_alert
                    send_alert(
                        f"BUDGET ALERT\n"
                        f"Daily cost: ${alert['total_today']:.2f} (limit: ${alert['limit']:.2f})\n"
                        f"Top spender: {alert['top_spender']} (${alert['top_cost']:.4f})"
                    )
                except Exception:
                    pass

        # Auto-pause expensive losers
        paused = check_auto_pause_losers()
        for p in paused:
            console.log(f"[bold red]AUTO-PAUSED: {p['player_id']} — 3 day losing streak, ${p['cost_wasted']:.4f} wasted")
            try:
                from engine.telegram_alerts import send_alert
                send_alert(f"AUTO-PAUSED: {p['player_id']} — 3 day losing streak, ${p['cost_wasted']:.4f} wasted in API calls")
            except Exception:
                pass

        # Log daily cost summary
        daily = get_total_daily_cost()
        if daily > 0:
            console.log(f"[dim]Daily API cost: ${daily:.4f}")
    except Exception as e:
        console.log(f"[red]Cost monitor error: {e}")


_ratings_fired_today: set[str] = set()


@_hm_bq_instr("run_daily_rating_update")
def run_daily_rating_update():
    """Run fleet report card at 4:30 PM ET (after market close). Fires once per day."""
    import zoneinfo
    from datetime import datetime as _dt
    et = zoneinfo.ZoneInfo("America/New_York")
    now = _dt.now(et)
    today_key = now.strftime("%Y-%m-%d")
    # Fire between 16:30 and 17:00 ET on weekdays only
    if now.weekday() >= 5:
        return
    if not (now.hour == 16 and 30 <= now.minute < 60):
        return
    if today_key in _ratings_fired_today:
        return
    _ratings_fired_today.add(today_key)
    try:
        from engine.agent_ratings import fleet_report_card, lineup_advisor
        from setup_db import setup as _setup_db
        _setup_db()   # schema + INSERTs default agents + UNCONDITIONAL canonical model_id enforcement for 18 agents (see setup_db.py:setup() docstring) — runs every startup
        report  = fleet_report_card()
        advice  = lineup_advisor()
        console.log("[bold cyan][RATINGS] Daily fleet report card:[/bold cyan]")
        for r in report:
            if r.get("rating") == "N/A":
                continue
            console.log(
                f"[RATINGS]  {r['player_id']:<22} "
                f"Grade={r['rating']}  Score={r['rating_score']:.0f}/100  "
                f"W/L={r['wins']}/{r['losses']}  WR={r['win_rate']}%  "
                f"PnL=${r['total_pnl']:.2f}"
            )
        console.log("[bold cyan][RATINGS] Lineup advice:[/bold cyan]")
        for a in advice:
            console.log(f"[RATINGS]  {a['icon']} {a['player_id']}: {a['action']} — {a['reason']}")
    except Exception as e:
        console.log(f"[red][RATINGS] Error: {e}")


@_hm_bq_instr("run_daily_summary")
def run_daily_summary():
    """Send daily summary at market close via Telegram."""
    from engine.risk_manager import RiskManager
    session = RiskManager.is_market_hours()
    # Only trigger right after market close (post_market)
    if session != "post_market":
        return
    try:
        from engine.paper_trader import get_portfolio_with_pnl
        from engine.market_data import get_stock_price
        from engine.universe import get_active_universe
        from engine.telegram_alerts import send_daily_summary
        import sqlite3

        # HM-AQ-β v3 2026-05-07: bulk endpoint (~25× faster than per-symbol loop).
        from engine.market_data import get_bulk_prices
        prices = get_bulk_prices(get_active_universe())

        conn = sqlite3.connect("data/trader.db", check_same_thread=False)
        conn.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
        conn.row_factory = sqlite3.Row
        players = conn.execute(
            # HM-AK-β 2026-05-07: halt_mode filter — skip halted_full and exit_only rows
            # HM-AK-γ 2026-05-07: drop redundant dayblade-0dte exclusion (halt_mode='full' covers it)
            "SELECT id, display_name FROM ai_players WHERE is_active=1 AND halt_mode='active'"
        ).fetchall()

        today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
        summary = []
        for p in players:
            pnl = get_portfolio_with_pnl(p["id"], prices)
            trades_today = conn.execute(
                "SELECT COUNT(*) as cnt FROM trades WHERE player_id=? AND date(executed_at)=?",
                (p["id"], today)
            ).fetchone()
            summary.append({
                "name": p["display_name"],
                "total_value": pnl["total_value"],
                "return_pct": pnl["return_pct"],
                "unrealized_pnl": pnl["total_unrealized_pnl"],
                "trades_today": trades_today["cnt"] if trades_today else 0,
            })
        conn.close()

        send_daily_summary(summary)
        console.log("[green]Daily summary sent via Telegram")
    except Exception as e:
        console.log(f"[red]Daily summary error: {e}")


@_hm_bq_instr("run_universe_scan")
def run_universe_scan():
    """Nightly universe scan — Ensign Chekov sweeps 500+ stocks.
    Runs at 9 PM MST / 12 AM ET (weeknights + Sunday). Takes 2-3 minutes."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    # Run weeknights at 9 PM MST, plus Sunday night
    if now.hour != 21:
        return
    if now.weekday() == 5:  # Saturday — skip
        return
    try:
        from engine.universe_scanner import scan_universe
        console.log("[bold cyan]🧭 Ensign Chekov: Starting nightly universe scan...")
        results = scan_universe()
        console.log(f"[green]🧭 Universe scan complete: {len(results)} candidates found")
    except Exception as e:
        console.log(f"[red]Universe scan error: {e}")


@_hm_bq_instr("run_strategy_scan")
def run_strategy_scan():
    """Nightly strategy scan — run 15 strategies against top 50 universe stocks.
    Runs at 10 PM MST / 1 AM ET (weeknights + Sunday). Takes 1-2 minutes."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    # Run weeknights at 10 PM MST, plus Sunday night
    if now.hour != 22:
        return
    if now.weekday() == 5:  # Saturday — skip
        return
    try:
        from engine.strategies import scan_strategies, post_scanner_to_war_room
        console.log("[bold cyan]🧭 Running morning strategy scan...")
        signals = scan_strategies()
        console.log(f"[green]🧭 Strategy scan: {len(signals)} convergence signals")
        # Post results to War Room as Ensign Chekov
        post_scanner_to_war_room()
        # Auto-trade on convergence signals
        try:
            from engine.chekov_autotrade import execute_convergence_trades
            execute_convergence_trades(signals)
        except Exception as e:
            console.log(f"[yellow]Chekov auto-trade error: {e}")
    except Exception as e:
        console.log(f"[red]Strategy scan error: {e}")


@_hm_bq_instr("run_chekov_stoploss")
def run_chekov_stoploss():
    """Check Chekov's positions against stop-loss/take-profit prices."""
    try:
        from engine.chekov_autotrade import check_stop_loss_take_profit
        check_stop_loss_take_profit()
    except Exception as e:
        console.log(f"[yellow]Chekov SL/TP check error: {e}")


@_hm_bq_instr("run_guardian_sweep")
def run_guardian_sweep():
    """HM-GUARDIAN-ADOPTION: exit-only stop sweep. The scan loop only stop-checks
    halt_mode='active' players. HM-AGENT-RULES-CONSOLIDATION 2026-07-04: generalized
    from guardian-of-forever-only to every exit_only agent holding an open position
    (queried fresh each run) after an audit found 4 more seats silently uncovered.
    Flat 12% stop + tiered TP; exits route to Alpaca Paper (real close);
    stuck_stop_guard covers it."""
    try:
        from engine.guardian_sweep import run_guardian_sweep as _sweep
        _sweep()
    except Exception as e:
        console.log(f"[yellow]Guardian sweep error: {e}")


# === HM-EVENTS-BUS-CONSUMER 2026-05-26 ===
@_hm_bq_instr("run_events_bus_consumer")
def run_events_bus_consumer():
    """Drain pending signals_v2 rows → paper_trader.buy(). NYSE-hours only."""
    try:
        from engine.market_calendar import is_us_market_open
        if not is_us_market_open():
            return
        from engine.events_bus_consumer import consume_pending_signals
        consume_pending_signals(max_batch=10)
    except Exception as e:
        console.log(
            f"[red][EVENTS-BUS-CONSUMER] wrapper error: "
            f"{type(e).__name__}: {e!r}"
        )


@_hm_bq_instr("run_signal_center_refresh")
def run_signal_center_refresh():
    """HM-SIGNAL-CENTER-REFRESH 2026-05-26: keep signal_history fresh by polling
    /api/signals/all every 5 minutes during market hours. Before this fix,
    signal_history was only updated as a side effect of browser-tab polling
    which stopped 2026-05-23T07:49."""
    from engine.market_calendar import is_us_market_open
    if not is_us_market_open():
        return
    try:
        import requests as _req
        _req.get("http://localhost:9000/api/signals/all", timeout=60)
    except Exception:
        pass  # fail-safe, never block scheduler


@_hm_bq_instr("run_daily_snapshot_refresh")
def run_daily_snapshot_refresh():
    """HM-DAILY-SNAPSHOT-REFRESH 2026-05-26: scheduled EOD trigger for the
    daily_snapshot writer. Hits /api/morpheus/awareness during the post-close
    13:00-14:00 AZ window. The writer (signal-center/server.py:1416) is
    idempotent — first-call-of-day inserts, later calls no-op. Fixes the
    daily_snapshot write dropout: 4 rows total, last write 2026-05-24."""
    from engine.market_calendar import is_us_market_open
    from datetime import datetime as _dt
    if is_us_market_open():
        return  # only fire after market close
    _hr = _dt.now().hour  # Arizona local; no DST
    if not (13 <= _hr < 14):
        return  # only in the 13:00-14:00 AZ post-close window
    try:
        import requests as _req
        _req.get("http://localhost:9000/api/morpheus/awareness", timeout=120)
    except Exception:
        pass  # fail-safe, never block scheduler


@_hm_bq_instr("run_pending_manual_closes")
def run_pending_manual_closes():
    """Fire pending manual close requests queued by Admiral. Reads data/pending_manual_closes.json,
    attempts sell() for each entry, removes on success."""
    import json, os
    from engine.market_calendar import is_us_market_open
    if not is_us_market_open():
        return
    path = "data/pending_manual_closes.json"
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            entries = json.load(f)
        if not entries:
            return
        from engine.paper_trader import sell
        from engine.market_data import get_all_prices
        remaining = []
        symbols = [e["symbol"] for e in entries]
        prices = get_all_prices(symbols)
        for entry in entries:
            sym = entry["symbol"]
            price = prices.get(sym, {}).get("price", 0)
            if price <= 0:
                remaining.append(entry)
                continue
            result = sell(entry["player_id"], sym, price, reasoning=entry["reason"])
            if result is None:
                remaining.append(entry)
            else:
                console.log(f"[green][MANUAL-CLOSE] {entry['player_id']} {sym} @ ${price:.2f} — done")
        with open(path, "w") as f:
            json.dump(remaining, f)
    except Exception as e:
        console.log(f"[yellow][MANUAL-CLOSE] error: {e}")


# === HM-AW: Chekov intraday convergence buyer ===
@_hm_bq_instr("run_chekov_intraday_convergence")
def run_chekov_intraday_convergence():
    """Chekov intraday convergence buyer — runs during market hours.

    Fixes 5+ week silent gap discovered 2026-05-11: the original convergence
    buyer was gated to 22:00 AZ (1 AM ET), when intraday convergence patterns
    have decayed and scan_strategies returns 0. This job runs the same buyer
    during US regular market hours, so the 1+ signals scan_strategies finds
    every morning actually trigger trades. (HM-AW)
    """
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()
    # Market hours: 6:30 AM – 1:00 PM AZ (= 9:30 AM – 4:00 PM ET).
    # Coarse-gate at the hour level; lets us run 06:00-06:29 too (catches opening setup window).
    if not (6 <= now.hour < 13):
        return
    # Skip weekends
    if now.weekday() >= 5:
        return
    try:
        from engine.strategies import scan_strategies
        from engine.chekov_autotrade import execute_convergence_trades
        signals = scan_strategies()
        if signals:
            execute_convergence_trades(signals)
            console.log(
                f"[green]🧭 Chekov intraday: processed {len(signals)} convergence signals"
            )
    except Exception as e:
        console.log(f"[yellow]Chekov intraday convergence error: {e}")
# === end HM-AW ===


_premarket_gaps_done = False

@_hm_bq_instr("run_premarket_gaps")
def run_premarket_gaps():
    """Pre-market gap scanner — Chekov posts gaps > 2% to War Room.
    Runs at 1 AM MST (4 AM ET) weekdays."""
    global _premarket_gaps_done
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()

    # Reset flag at midnight
    if now.hour == 0:
        _premarket_gaps_done = False
        return

    # Only run weekdays at 1 AM MST (4 AM ET)
    if now.weekday() >= 5 or now.hour != 1 or _premarket_gaps_done:
        return

    _premarket_gaps_done = True
    try:
        # === HM-BL-broad ===
        from engine.yf_safe import yf_history_safe
        # === /HM-BL-broad ===
        from engine.war_room import save_hot_take
        from engine.universe_scanner import get_latest_universe_scan

        # Get top universe stocks
        scan = get_latest_universe_scan()
        tickers = [s["ticker"] for s in scan.get("results", [])[:50]] if scan else []
        if not tickers:
            from engine.universe import get_active_universe
            tickers = get_active_universe()

        gaps = []
        for sym in tickers:
            try:
                # === HM-BL-broad ===
                hist = yf_history_safe(sym, period="2d", prepost=True)
                # === /HM-BL-broad ===
                if len(hist) < 2:
                    continue
                prev_close = float(hist["Close"].iloc[-2])
                # Try to get pre-market price
                current = float(hist["Close"].iloc[-1])
                gap_pct = ((current / prev_close) - 1) * 100
                if abs(gap_pct) >= 2.0:
                    gaps.append({"ticker": sym, "gap_pct": gap_pct, "price": current})
            except Exception:
                continue

        if gaps:
            gaps.sort(key=lambda g: abs(g["gap_pct"]), reverse=True)
            for g in gaps[:5]:
                direction = "up" if g["gap_pct"] > 0 else "down"
                arrow = "▲" if g["gap_pct"] > 0 else "▼"
                msg = (
                    f"🧭 Keptin! Pre-market gap detected — {g['ticker']} gapping "
                    f"{direction} {arrow} {abs(g['gap_pct']):.1f}% at ${g['price']:.2f}!"
                )
                save_hot_take("navigator", g["ticker"], msg)
            console.log(f"[bold cyan]🧭 Chekov: {len(gaps)} pre-market gaps posted to War Room")
        else:
            console.log("[dim]🧭 Chekov: No significant pre-market gaps found")
    except Exception as e:
        console.log(f"[red]Pre-market gaps error: {e}")


_finviz_scan_done = False


@_hm_bq_instr("run_finviz_premarket_scan")
def run_finviz_premarket_scan():
    """Finviz pre-market watchlist builder — runs at 6:15 AZ (9:15 ET) weekdays."""
    global _finviz_scan_done
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = az_now()

    # Reset flag at midnight
    if now.hour == 0:
        _finviz_scan_done = False
        return

    # Only run weekdays inside the 6:15–6:45 AZ window
    if now.weekday() >= 5 or now.hour != 6 or now.minute < 15 or _finviz_scan_done:
        return

    _finviz_scan_done = True
    try:
        from engine.premarket_scanner import run_finviz_watchlist_scan
        result = run_finviz_watchlist_scan()
        count = len(result.get("symbols", []))
        console.log(f"[cyan][Finviz] Pre-market scan: {count} symbols in daily_watchlist.json")

        # Holly priority injection: boost tickers that matched overnight winning strategies
        try:
            from engine.holly_nightly_backtest import get_holly_winning_tickers
            holly_winners = get_holly_winning_tickers(10)
            if holly_winners:
                holly_tickers = [w["ticker"] for w in holly_winners]
                scan_symbols  = result.get("symbols", [])
                # HM-HOLLY-CONSUMER-BUG fix 2026-05-30: the prior filter
                #   [s for s in holly_tickers if s in scan_symbols or s not in scan_symbols]
                # was ALWAYS-TRUE (every s is either in or not-in) → it kept the full
                # holly list regardless of overlap, defeating the "priority boost".
                # Correct intent: surface Holly's overnight winners that ARE in today's
                # scan universe to the FRONT, then the rest of the scan list.
                matched   = [s for s in holly_tickers if s in scan_symbols]
                boosted   = list(dict.fromkeys(matched + scan_symbols))  # deduplicated, Holly-matched first
                result["symbols"]       = boosted
                result["holly_matches"] = [w for w in holly_winners if w["ticker"] in set(scan_symbols)]
                console.log(
                    f"[bold magenta]🔬 Holly priority: {len(matched)} overnight winners → "
                    + ", ".join(f"{w['ticker']}({w['strategy']} {w['total_return']:+.1f}%)"
                                for w in holly_winners[:5])
                )
        except Exception as _hpe:
            console.log(f"[dim]Holly priority injection skipped: {_hpe}")

        try:
            from engine.war_room import save_hot_take
            syms_preview = ", ".join(result.get("symbols", [])[:10])
            extra = count - 10 if count > 10 else 0
            msg = f"Today's watchlist ready: {syms_preview}" + (f" +{extra}" if extra else "")
            save_hot_take("FINVIZ", "watchlist", msg)
        except Exception:
            pass
    except Exception as e:
        console.log(f"[yellow]Finviz pre-market scan error: {e}")


@_hm_bq_instr("run_metals_commentary")
def run_metals_commentary():
    """Dalio's daily metals report — 7 AM MST weekdays."""
    from datetime import datetime as _dt
    import pytz
    mt = pytz.timezone("US/Mountain")
    now = _dt.now(mt)
    if now.weekday() >= 5 or now.hour != 7:
        return
    try:
        from engine.metals_commentary import generate_commentary, post_to_war_room
        console.log("[bold yellow]🪙 Lt. Cmdr. Dalio generating metals report...")
        generate_commentary()
        post_to_war_room()
    except Exception as e:
        console.log(f"[red]Metals commentary error: {e}")


def run_dashboard():
    global _dashboard_error
    import uvicorn, time as _t
    _attempt = 0
    while True:
        _attempt += 1
        try:
            from dashboard.app import app
            _dashboard_started.set()
            # HM-BRIDGE-BIND (2026-06-12): bind 0.0.0.0 so http://bigmac:8080 is
            # reachable over Tailscale/LAN without an SSH tunnel. Override via env
            # DASHBOARD_HOST (e.g. "127.0.0.1") to revert to loopback-only.
            # SECURITY: network exposure is SAFE because the localhost auth bypass
            # (dashboard/app.py AuthMiddleware) keys on request.client.host == 127.0.0.1,
            # and forwarded_allow_ips="127.0.0.1" means uvicorn only honors X-Forwarded-For
            # from genuinely-loopback connections — a LAN/Tailscale client arrives from a
            # non-loopback source IP, so it can neither hit the bypass nor spoof client.host;
            # it must authenticate. Do NOT set proxy_headers=False or widen forwarded_allow_ips.
            _dash_host = os.getenv("DASHBOARD_HOST", "0.0.0.0")
            uvicorn.run(app, host=_dash_host, port=8080, log_level="warning",
                        proxy_headers=True, forwarded_allow_ips="127.0.0.1")
            return  # Clean exit (shutdown)
        except Exception as exc:
            _dashboard_error = exc
            # Backoff: 5s → 15s → 30s → 60s cap
            delay = min(5 * (2 ** min(_attempt - 1, 3)), 60)
            console.log(f"[yellow]Dashboard attempt {_attempt} failed ({exc}), retrying in {delay}s...")
            _t.sleep(delay)


_sulu_closed_today = False

@_hm_bq_instr("run_sulu_autoclose")
def run_sulu_autoclose():
    """Lt. Sulu DayBlade EOD auto-close: sell ALL positions at 3:45 PM ET (12:45 PM MST)."""
    global _sulu_closed_today
    import pytz
    from datetime import datetime as _dt

    try:
        az = pytz.timezone("US/Arizona")
        now = az_now()
    except Exception:
        return

    # Reset flag overnight
    if now.hour < 5:
        _sulu_closed_today = False
        return

    # Skip weekends
    if now.weekday() >= 5:
        return

    # Fire at 12:45 PM MST (3:45 PM ET) — only once per day
    if now.hour == 12 and now.minute >= 45 and now.minute < 50 and not _sulu_closed_today:
        _sulu_closed_today = True
        try:
            from engine.paper_trader import sell, get_portfolio
            from engine.market_data import get_all_prices
            portfolio = get_portfolio("dayblade-sulu")
            positions = portfolio.get("positions", [])
            if not positions:
                console.log("[dim]Sulu EOD: No positions to close")
            else:
                # Get current prices
                symbols = [p["symbol"] for p in positions]
                prices = get_all_prices(symbols)
                closed = 0
                for pos in positions:
                    sym = pos["symbol"]
                    if sym in prices:
                        sell(
                            "dayblade-sulu", sym, prices[sym]["price"],
                            asset_type=pos.get("asset_type", "stock"),
                            reasoning="EOD AUTO-CLOSE: Lt. Sulu never holds overnight. All positions closed at 3:45 PM ET.",
                            option_type=pos.get("option_type"),
                        )
                        closed += 1
                        console.log(f"[yellow]Sulu EOD: Closed {sym} @ ${prices[sym]['price']:.2f}")
                console.log(f"[bold yellow]Lt. Sulu EOD auto-close complete: {closed} position(s) closed")

            # Close ALL Alpaca options positions (covers both Sulu and DayBlade 0DTE)
            try:
                from engine.alpaca_options import close_all_options
                close_all_options("dayblade-sulu + dayblade-0dte EOD sweep")
            except Exception as _ae:
                console.log(f"[yellow]Alpaca options EOD close error: {_ae}")

        except Exception as e:
            console.log(f"[red]Sulu EOD auto-close error: {e}")


# ---------------------------------------------------------------------------
# Crew Scanner: agent signal pipeline
# ---------------------------------------------------------------------------

_crew_scanner_lock      = threading.Lock()
_crew_scanner_t1_last   = 0.0   # epoch seconds of last Alpha crew scan
_crew_scanner_slots_done_today: set = set()

_CREW_SCANNER_T1_INTERVAL = 2 * 60    # 2 min between Alpha interval scans (4 agents ~40s each)


@_hm_bq_instr("run_crew_scanner_job")
def run_crew_scanner_job() -> None:
    """
    Crew Scanner: feed live market signals to every mandated agent.

    Fires on a schedule keyed to Arizona (MST, no DST) clock:
      6:35 AM  AZ (9:35 AM  ET) — market open: full (all 10 agents)
      7:00 AM  AZ (10:00 AM ET) — second scan: alpha (4 agents)
      Every 15 min during 6:35–12:45 AZ — alpha squad only
      9:00 AM  AZ (12:00 PM ET) — midday: full (all 10 agents)
      12:00 PM AZ (3:00 PM  ET) — last hour: alpha only
      12:45 PM AZ (3:45 PM  ET) — close scan: alpha only
    """
    global _crew_scanner_t1_last, _crew_scanner_slots_done_today

    from engine.risk_manager import RiskManager
    import pytz
    import time as _time

    session = RiskManager.is_market_hours()
    if not session:
        return

    try:
        az  = pytz.timezone("US/Arizona")
        now = az_now()
    except Exception:
        return

    if now.weekday() >= 5:
        return

    # Reset daily slots overnight
    if now.hour < 4:
        _crew_scanner_slots_done_today = set()
        return

    now_mins  = now.hour * 60 + now.minute
    wall_time = _time.time()

    # Determine which tier/scope to scan this cycle
    tier_to_scan: str | None = None

    # ── One-off time slots ───────────────────────────────────────────────────
    # Market open  6:35–6:45 AZ = 9:35–9:45 ET
    if 395 <= now_mins <= 405 and "open" not in _crew_scanner_slots_done_today:
        _crew_scanner_slots_done_today.add("open")
        tier_to_scan = "full"

    # Second scan  7:00–7:10 AZ = 10:00–10:10 ET
    elif 420 <= now_mins <= 430 and "second" not in _crew_scanner_slots_done_today:
        _crew_scanner_slots_done_today.add("second")
        tier_to_scan = "alpha"

    # Midday  9:00–9:10 AZ = 12:00–12:10 ET
    elif 540 <= now_mins <= 550 and "midday" not in _crew_scanner_slots_done_today:
        _crew_scanner_slots_done_today.add("midday")
        tier_to_scan = "full"

    # Last hour  12:00–12:10 AZ = 3:00–3:10 ET
    elif 720 <= now_mins <= 730 and "last_hour" not in _crew_scanner_slots_done_today:
        _crew_scanner_slots_done_today.add("last_hour")
        tier_to_scan = "alpha"

    # Pre-close  12:45–12:55 AZ = 3:45–3:55 ET
    elif 765 <= now_mins <= 775 and "close" not in _crew_scanner_slots_done_today:
        _crew_scanner_slots_done_today.add("close")
        tier_to_scan = "alpha"

    # ── Interval-based scans (between open and pre-close window) ────────────
    elif 395 <= now_mins < 765:
        if wall_time - _crew_scanner_t1_last >= _CREW_SCANNER_T1_INTERVAL:
            _crew_scanner_t1_last = wall_time
            tier_to_scan = "alpha"

    if tier_to_scan is None:
        return

    # Run in background thread; skip if previous scan still running
    if not _crew_scanner_lock.acquire(blocking=False):
        console.log("[yellow]Crew Scanner: skipping — previous cycle still running")
        return

    _tier_label = tier_to_scan  # capture for closure

    def _scan_thread() -> None:
        # Raise OS scheduling priority for the trading thread.
        # os.nice(-5) lowers the niceness by 5 (higher CPU priority).
        # Requires elevated permissions on macOS/Linux; silently skipped if denied.
        try:
            current_nice = os.nice(0)           # read current niceness
            os.nice(-5)                          # request higher priority
            new_nice = os.nice(0)
            console.log(f"[cyan]Crew Scanner thread nice: {current_nice} → {new_nice}")
        except (PermissionError, OSError) as _e:
            console.log(f"[yellow]Crew Scanner nice(-5) skipped (not root): {_e}")
        try:
            from engine.crew_scanner import run_scan_cycle
            summary = run_scan_cycle(tier_filter=_tier_label, verbose=True)
            console.log(
                f"[bold cyan]🚀 Crew Scanner [{_tier_label}]: "
                f"{summary['agents_scanned']} scanned, "
                f"{summary['passed_mandate']} cleared mandate, "
                f"{summary['made_trades']} trade(s), "
                f"{summary['blocked_by_gates']} gate-blocked"
            )
        except Exception as e:
            console.log(f"[red]Crew Scanner error: {e}")
        finally:
            _crew_scanner_lock.release()

    threading.Thread(target=_scan_thread, daemon=True, name="crew_scanner").start()


# ---------------------------------------------------------------------------
# Battle Station 0DTE: rules-based SPY 0DTE scanner (every 2 min)
# ---------------------------------------------------------------------------

_bs0dte_lock = threading.Lock()

@_hm_bq_instr("run_battle_station_0dte_job")
def run_battle_station_0dte_job() -> None:
    """Rules-based 0DTE battle station — fires every 5 min during 9:45 AM - 2:30 PM ET."""
    import pytz
    from datetime import datetime as _dt
    az = pytz.timezone("US/Arizona")
    now = az_now()
    if now.weekday() >= 5:
        return
    mins = now.hour * 60 + now.minute
    # 9:45 AM ET = 6:45 AM AZ = 405 mins
    # 2:30 PM ET = 11:30 AM AZ = 690 mins
    if not (405 <= mins <= 690):
        return
    if not _bs0dte_lock.acquire(blocking=False):
        return
    def _run():
        try:
            from engine.battle_station_0dte import scan
            scan()
        except Exception as e:
            console.log(f"[red]Battle Station 0DTE error: {e}")
        finally:
            _bs0dte_lock.release()
    threading.Thread(target=_run, daemon=True, name="battle_station_0dte").start()


def maybe_reset_equity():
    """One-time S5 equity reset — runs once per deployment, guarded by system_settings."""
    import sqlite3 as _sq
    _db = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
    c = _sq.connect(_db, timeout=10)
    c.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
    c.execute('''CREATE TABLE IF NOT EXISTS system_settings
        (key TEXT PRIMARY KEY, value TEXT,
         updated_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
    done = c.execute(
        "SELECT value FROM system_settings WHERE key='s5_equity_reset'"
    ).fetchone()
    if not done or done[0] != 'done':
        c.execute("UPDATE ai_players SET cash = 10000.00")
        c.execute('''INSERT OR REPLACE INTO system_settings
            (key, value) VALUES ('s5_equity_reset', 'done')''')
        c.commit()
        console.log("[bold green]EQUITY RESET: All agents reset to $10,000 (Season 6.3)")
    c.close()


if __name__ == "__main__":
    # Re-bind module logger explicitly so inner scheduled-job functions can resolve
    # it via closure — the module-level definition at line 3 can be shadowed or
    # unreachable from nested closures in some Python import orderings.
    logger = _logging.getLogger(__name__)

    # === HM-BRIDGE-WEDGE-2 (2026-06-11): raise the file-descriptor soft limit ===
    # Root cause of the recurring "Process listening but not responding" watchdog
    # restarts (4× in 8 days). NOT an Ollama wedge — the dashboard runs on its own
    # uvicorn thread and Ollama calls complete remotely in 1-7s. The real cause is a
    # SQLite connection FD leak: queries raising under scan-cycle DB contention skip
    # a non-try/finally conn.close(), and the raised exception's traceback pins the
    # frame-local connection so refcounting can't reclaim the FD. When the process
    # reaches the inherited soft NOFILE cap (launchd/cron default 256), accept()
    # returns EMFILE — the kernel holds the listen socket but the app can't answer
    # HTTP — and every new sqlite3.connect() fails with "unable to open database
    # file". Raising the soft limit removes the wedge CEILING; the per-site
    # try/finally fixes (this commit) stop the leak itself. Belt-and-braces.
    try:
        import resource as _resource
        _soft, _hard = _resource.getrlimit(_resource.RLIMIT_NOFILE)
        _target = _hard if _hard != _resource.RLIM_INFINITY else 65536
        _target = max(_soft, min(_target, 65536))  # stay under macOS kern.maxfilesperproc
        if _target > _soft:
            _resource.setrlimit(_resource.RLIMIT_NOFILE, (_target, _hard))
            console.log(f"[green][HM-BRIDGE-WEDGE-2] RLIMIT_NOFILE soft {_soft} → {_target} (hard={_hard})")
        else:
            console.log(f"[dim][HM-BRIDGE-WEDGE-2] RLIMIT_NOFILE soft already {_soft} (hard={_hard})")
    except Exception as _rlim_e:
        console.log(f"[yellow][HM-BRIDGE-WEDGE-2] could not raise RLIMIT_NOFILE: {type(_rlim_e).__name__}: {_rlim_e!r}")

    # === HM-BRIDGE-WEDGE-2: FD-pressure GC reaper (defense-in-depth) ===========
    # The per-site try/finally fixes (this commit) close connections on the hot,
    # small endpoints + daemons. A few large multi-connection endpoints (e.g. the
    # 280-line leaderboard) are NOT re-indented (re-indent risk > reward in a
    # money-adjacent file). For those, leaked connections survive only as
    # Connection↔Cursor reference cycles — which gc.collect() reclaims (it runs
    # the finalizers on unreachable cycles, closing the FD). This daemon polls the
    # process FD count every 120s and forces a collect ONLY when pressure is high,
    # so it can never silently approach the (now-raised) NOFILE cap. NOT a
    # substitute for the try/finally fixes or the rlimit raise — a backstop behind
    # both. No trading-logic impact (gc timing only).
    def _fd_gc_reaper():
        import gc as _gc, time as _t, os as _os
        try:
            import psutil as _ps
            _proc = _ps.Process(_os.getpid())
        except Exception:
            _proc = None
        while True:
            _t.sleep(120)
            try:
                nfds = None
                if _proc is not None:
                    try:
                        nfds = _proc.num_fds()
                    except Exception:
                        nfds = None
                if nfds is None or nfds > 1500:
                    _gc.collect()
                    if nfds is not None:
                        console.log(f"[dim][HM-BRIDGE-WEDGE-2] fd_gc_reaper: {nfds} FDs open → gc.collect()")
            except Exception:
                pass
    threading.Thread(target=_fd_gc_reaper, daemon=True, name="fd_gc_reaper").start()

    # Bootstrap DB: schema + INSERTs default agents + UNCONDITIONAL canonical
    # model_id enforcement for 18 agents (see setup_db.setup() docstring).
    # NOTE: runtime UPDATEs to ai_players.model_id for the 18 enforced IDs are
    # silently reverted by this call. HM-BN 2026-05-15 root cause (commit 7eed0ca).
    from setup_db import setup
    setup()

    # Enable WAL mode on both databases — reduces lock contention under concurrent writes.
    # Safe to call every startup; WAL persists across connections once set.
    for _wal_db in (
        os.path.join(os.path.dirname(__file__), "data", "trader.db"),
        os.path.join(os.path.dirname(__file__), "autonomous_trader.db"),
    ):
        try:
            _wc = sqlite3.connect(_wal_db, timeout=10)
            _wc.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
            _wc.execute("PRAGMA journal_mode=WAL")
            _wc.execute("PRAGMA synchronous=NORMAL")   # safe with WAL; faster than FULL
            _wc.close()
            console.log(f"[green]WAL mode enabled: {os.path.basename(_wal_db)}")
        except Exception as _wal_e:
            console.log(f"[yellow]WAL mode warning ({os.path.basename(_wal_db)}): {_wal_e}")

    # Season 6.3 equity reset
    maybe_reset_equity()

    # Init fallback columns (idempotent migration)
    from engine.fallback import init_fallback_columns
    init_fallback_columns()

    # Init trade outcomes table (idempotent — CREATE IF NOT EXISTS)
    try:
        from engine.trade_outcomes import init_trade_outcomes_table
        init_trade_outcomes_table()
    except Exception as _e:
        console.log(f"[yellow]Trade outcomes table init warning: {_e}")

    # Seed/refresh agent ratings with clean Season 6.3 data on startup
    try:
        from engine.agent_ratings import recalculate_all_ratings
        recalculate_all_ratings()
    except Exception as _e:
        console.log(f"[yellow]Agent ratings recalculation warning: {_e}")

    # Init data ingestion tables + seed patterns (idempotent)
    try:
        from engine.data_ingestion import init_all as _init_ingestion
        _init_ingestion()
    except Exception as _e:
        console.log(f"[yellow]Data ingestion init warning: {_e}")

    # Backfill market history on startup (fills gaps, skips already-loaded bars)
    def _startup_market_backfill():
        try:
            from engine.data_ingestion import backfill_market_history
            console.log("[cyan]Data Ingestion: backfilling market history (365d)...")
            r = backfill_market_history(days=365)
            console.log(f"[green]Market history backfill: {r['bars_inserted']} bars inserted")
        except Exception as e:
            console.log(f"[yellow]Market history startup backfill error: {e}")
    threading.Thread(target=_startup_market_backfill, daemon=True).start()

    # Init Telegram
    from engine.telegram_alerts import init_telegram
    init_telegram()

    # Monday morning startup checklist
    import logging as _logging
    _startup_log = _logging.getLogger("startup")
    _anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    from engine.crew_scanner import ACTIVE_SCANNERS, RULES_SCANNERS
    _startup_log.info("=" * 60)
    _startup_log.info(f"USS OLLIETRADES — SEASON 6.3 — {datetime.now().strftime('%A').upper()} STARTUP")
    _startup_log.info("=" * 60)
    _startup_log.info(f"Active Scanners: {ACTIVE_SCANNERS}")
    _startup_log.info(f"Rules Scanners: {RULES_SCANNERS}")
    _startup_log.info(f"Scan Model: T'Pol=0xroyce/plutus (0DTE) | McCoy=ministral-3:3b (triage)")
    _startup_log.info(f"API Key: {'SET' if _anthropic_key else 'MISSING'}")
    _startup_log.info(f"Ollama: Ollie Max (.168) — fleet inference")
    _startup_log.info(f"Bridge: bridge.accessapple.com")
    _startup_log.info(f"Daily Cost: $0.00 (Ollama) + ~$0.50 (Sonnet CIC)")
    _startup_log.info("=" * 60)
    _startup_log.info("ALL SYSTEMS OPERATIONAL — ENGAGE")

    console.print(Panel.fit(
        "[bold green]USS OllieTrades[/bold green] — [bold cyan]All systems operational[/bold cyan]\n"
        "[dim]Season 6.3 — Fleet Online | Dashboard: http://127.0.0.1:8080[/dim]",
        border_style="green"
    ))

    # === HM-EQ ===
    # Spawn the equity snapshot daemon at trader-boot time, independent of
    # Arena lifecycle. The single-threaded `schedule.run_pending()` queue
    # can defer Arena's first run_scanner() call by tens of minutes when
    # startup work piles up, so an Arena-coupled daemon doesn't fire until
    # then. Module-level spawn fires within 30s of trader boot.
    try:
        from engine.ai_brain import start_equity_snapshot_daemon
        start_equity_snapshot_daemon()
    except Exception as _hmeq_e:
        console.log(f"[red][HM-EQ] daemon startup failed: {type(_hmeq_e).__name__}: {_hmeq_e!r}[/red]")
    # === /HM-EQ ===

    # === HM-TRADE-DESK-AUTOPILOT-PHASE2 (HM-NEXT-WAVE Phase 3) 2026-05-23 ===
    # OCO reconciliation daemon for attach-after-fill children
    # (fractional/notional Trade Desk autopilot path). Module-level spawn
    # at boot per HM-EQ daemon lifecycle rule — NEVER lazy. 30s poll loop
    # monitors autopilot_oco_watch active rows, cancels sibling on
    # terminal-fill. NTFY ollietrades-admin on OCO trigger + daemon crash.
    try:
        from engine.trade_desk_autopilot import start_trade_desk_autopilot_daemon
        start_trade_desk_autopilot_daemon()
    except Exception as _tda_e:
        console.log(
            f"[red][TDA-PHASE2-DAEMON] startup failed: "
            f"{type(_tda_e).__name__}: {_tda_e!r}"
        )
    # === /HM-TRADE-DESK-AUTOPILOT-PHASE2 ===

    # === HM-WAR-ROOM-INIT-FIX 2026-05-15 ===
    # Eagerly initialize Arena at startup so arena-coupled scheduled jobs
    # (run_war_room, run_journal, etc.) can fire on the first scheduler tick.
    # Previously, arena was lazy-initialized inside run_scanner (main.py:384),
    # which sat behind every other registered job in the single-threaded
    # schedule.run_pending() queue. A long-running job (e.g. run_fundamental_scan
    # fanning out to Yahoo for ~50+ symbols, rate-limited) would block run_scanner
    # from ever firing, leaving arena=None and causing run_war_room to silently
    # early-return for the entire post-restart window.
    # Verified post-restart 2026-05-15 14:25:49 AZ: arena stayed None for 50+ min;
    # War Room produced zero "launching cycle" log lines; HM-EQ daemon (which
    # bypasses arena lifecycle) kept firing normally — proving the issue is
    # scoped to arena-coupled scheduled jobs.
    # Per CLAUDE.md HM-Z/HM-AA error-handling posture: bare except is OK when
    # the handler accommodates unknown failures; we log type + repr and fall
    # through to the lazy-init path in run_scanner if eager init throws, so
    # the fix is strictly additive.
    if arena is None:
        try:
            arena = initialize_arena()
            console.log("[green][STARTUP] Arena initialized eagerly (HM-WAR-ROOM-INIT-FIX)")
        except Exception as _arena_e:
            console.log(
                f"[red][STARTUP] Arena eager init failed: "
                f"{type(_arena_e).__name__}: {_arena_e!r} — "
                f"falling back to lazy init in run_scanner[/red]"
            )
    # === /HM-WAR-ROOM-INIT-FIX ===

    # === HM-WR-DAEMON-THREAD 2026-05-20 ===
    # Spawn run_war_room on its own daemon thread, bypassing the single-threaded
    # schedule.run_pending() queue. Phase 1+2 RCA (project_hm_wr_cycle_rca.md)
    # established that one hanging scheduled job blocks ALL others; run_war_room
    # was registered correctly with sensible next_run, but never dispatched
    # because something ahead in the queue hung. Mirrors the HM-EQ daemon
    # pattern: independent threading.Thread(daemon=True) with own sleep loop.
    # run_war_room's internal is_market_hours()/throttle/guard gates still apply,
    # so this is purely a dispatch substitution — no behavior change to the
    # cycle logic itself.
    def _war_room_scheduler_thread():
        """Daemon scheduler for run_war_room — independent of schedule library.

        HM-WR-CYCLE-RCA Phase 2 (2026-05-21): the dispatch mystery from
        2026-05-20 was resolved by HM-WR-DAEMON-THREAD (this very block) which
        bypasses `schedule.run_pending()`. Phase 2 now adds liveness
        instrumentation to detect daemon-thread death between cycles — without
        it, a silent thread crash would only surface as "no [WR-DUR] for
        >10 min" which is hard to distinguish from a long-running cycle.

        Heartbeat layout (cadence preserved at 300s sleep between cycles):
          [WR-DAEMON-HB] tick=N starting   — at the top of each iteration
          [WR-DAEMON-HB] tick=N done wall=Xs gated_market_closed=bool
                                            — after run_war_room returns or skips
        On exception path the existing [HM-WR-DAEMON] tick error log stands.
        """
        import time as _wr_dt
        _tick = 0
        while True:
            _tick += 1
            _t0 = _wr_dt.time()
            try:
                from engine.risk_manager import RiskManager
                _in_market = RiskManager.is_market_hours()
                console.log(
                    f"[cyan][WR-DAEMON-HB] tick={_tick} starting "
                    f"(market_hours={_in_market})"
                )
                if _in_market:
                    run_war_room()
                    _wall = _wr_dt.time() - _t0
                    console.log(
                        f"[cyan][WR-DAEMON-HB] tick={_tick} done "
                        f"wall={_wall:.1f}s (cycle ran)"
                    )
                else:
                    console.log(
                        f"[cyan][WR-DAEMON-HB] tick={_tick} done "
                        f"wall={_wr_dt.time() - _t0:.1f}s (market closed — skipped)"
                    )
            except Exception as _wr_err:
                console.log(
                    f"[red][HM-WR-DAEMON] tick error: "
                    f"{type(_wr_err).__name__}: {_wr_err!r}[/red]"
                )
            _wr_dt.sleep(300)

    try:
        threading.Thread(
            target=_war_room_scheduler_thread,
            daemon=True,
            name="war_room_scheduler",
        ).start()
        console.log("[green][HM-WR-DAEMON] War Room scheduler thread started (300s cadence)")
    except Exception as _wrd_e:
        console.log(
            f"[red][HM-WR-DAEMON] thread startup failed: "
            f"{type(_wrd_e).__name__}: {_wrd_e!r}[/red]"
        )
    # === /HM-WR-DAEMON-THREAD ===

    # === HM-AS-β LOOP 3 — battle_station dedicated daemon thread (2026-05-29) ===
    # Decouple the 60s-critical options monitor from the shared schedule.run_pending()
    # queue. Loops 1+2 backgrounded whisper+autopilot, but every-cycle scanners still
    # batch into 180-584s blocks that starved battle_station (the [HM-AS-β] drift).
    # A dedicated thread makes battle_station immune to ALL scheduler-thread load —
    # the durable fix vs whack-a-mole job-wrapping. Mirrors HM-WR-DAEMON-THREAD.
    # Dispatch substitution only: the monitor's internal is_market_hours/dedup gates
    # still apply; its [HM-AS-β] drift log now doubles as a thread-liveness check
    # (sleeps exactly 120s, no contention → interval stays ~120s, never trips 180s).
    def _battle_station_scheduler_thread():
        import time as _bs_dt
        _tick = 0
        while True:
            _tick += 1
            try:
                run_battle_station_monitor()
            except Exception as _bs_err:
                console.log(
                    f"[red][HM-BS-DAEMON] tick={_tick} error: "
                    f"{type(_bs_err).__name__}: {_bs_err!r}[/red]"
                )
            _bs_dt.sleep(120)

    try:
        threading.Thread(
            target=_battle_station_scheduler_thread,
            daemon=True,
            name="battle_station_scheduler",
        ).start()
        console.log("[green][HM-BS-DAEMON] Battle Station scheduler thread started (120s cadence)")
    except Exception as _bsd_e:
        console.log(
            f"[red][HM-BS-DAEMON] thread startup failed: "
            f"{type(_bsd_e).__name__}: {_bsd_e!r}[/red]"
        )
    # === /HM-AS-β LOOP 3 ===

    # Scanner ticks every 30s; run_scanner enforces dynamic cooldown internally
    schedule.every(2).minutes.do(run_scanner)
    schedule.every(2).minutes.do(check_scan_liveness)  # HM-SCAN-LIVENESS-WATCHDOG 2026-07-08
    schedule.every(5).minutes.do(run_dayblade)  # DayBlade 0DTE: T'Pol on plutus, every 5 min
    schedule.every(15).minutes.do(run_ma_regime_update)  # 8/21 MA Cross Regime: every 15 min
    schedule.every(15).minutes.do(run_vix_check)          # VIX: every 15 min
    schedule.every(1).hours.do(run_earnings_check)       # Earnings: hourly
    schedule.every(30).minutes.do(run_daily_summary)      # Daily summary: checks every 30 min, sends once at close
    schedule.every(30).minutes.do(run_daily_rating_update) # Agent ratings: checks every 30 min, fires once at 4:30 PM ET
    schedule.every(30).minutes.do(run_journal)             # AI journal: checks every 30 min, writes once at close
    # HM-EXEC-PIPELINE observe-first: score expired signal observations every 30 min
    def _run_signal_evaluator():
        try:
            from engine.signal_evaluator import evaluate_pending
            evaluate_pending()
        except Exception as _exc:
            logger.debug("[signal_eval] scheduler call failed: %s", _exc)
    schedule.every(30).minutes.do(_run_signal_evaluator)
    # HM-EXEC-PIPELINE measurement-health watchdog: ntfy RED probes every 15 min
    _mhealth_cooldown = {}  # probe_key -> epoch_secs of last alert
    _MHEALTH_CD_SECS = 3600  # 60-min cooldown per probe
    _mhealth_state = {"last_filled": None}  # cross-call state for drain-progress tracking

    def _run_measurement_health_watch():
        import time as _mh_time
        import sqlite3 as _mh_sqlite3
        from datetime import datetime as _mh_dt, timezone as _mh_tz
        import threading as _mh_threading

        now = _mh_dt.now(_mh_tz.utc)
        now_epoch = _mh_time.time()

        def _mh_age(ts_str):
            if not ts_str:
                return None
            try:
                s = ts_str.strip().replace(" ", "T")
                if "+" not in s and not s.endswith("Z"):
                    s += "+00:00"
                return (now - _mh_dt.fromisoformat(s)).total_seconds()
            except Exception:
                return None

        def _mh_fire(probe: str, title: str, body: str) -> None:
            last = _mhealth_cooldown.get(probe, 0)
            if now_epoch - last < _MHEALTH_CD_SECS:
                return
            _mhealth_cooldown[probe] = now_epoch
            try:
                from engine.ntfy import _send as _ntfy_send
                _mh_threading.Thread(
                    target=_ntfy_send,
                    args=(title, body, 4, "red_circle"),
                    kwargs={"topic": "ollietrades-admin"},
                    daemon=True,
                ).start()
            except Exception:
                pass

        try:
            _mh_conn = _mh_sqlite3.connect("data/trader.db", timeout=5)
            _mh_conn.row_factory = _mh_sqlite3.Row

            # RTH = Mon–Fri 13:30–20:00 UTC (06:30–13:00 MST; AZ = UTC-7 year-round)
            is_rth = (
                now.weekday() < 5
                and (now.hour, now.minute) >= (13, 30)
                and (now.hour, now.minute) < (20, 0)
            )

            # --- probe: obs writer stale during RTH > 2h ---
            obs_row = _mh_conn.execute(
                "SELECT MAX(ts) AS last_ts FROM signal_observations"
            ).fetchone()
            obs_age = _mh_age(obs_row["last_ts"])
            if is_rth and (obs_age is None or obs_age > 7200):
                age_str = (
                    f"{int((obs_age or 0) // 3600)}h"
                    f"{int(((obs_age or 0) % 3600) // 60):02d}m"
                    if obs_age else "never"
                )
                _mh_fire(
                    "obs_stale",
                    "\U0001f534 MEASUREMENT: obs writer stalled",
                    f"signal_observations last insert {age_str} ago during RTH.\n"
                    "BK scanners may have crashed — check bridge health panel.",
                )

            # --- probe: evaluator not run in > 90 min (3 missed 30-min cycles) ---
            ev_row = _mh_conn.execute(
                "SELECT MAX(evaluated_at) AS last_run, COUNT(*) AS total, "
                "SUM(CASE WHEN fwd_return_1d IS NOT NULL THEN 1 ELSE 0 END) AS filled "
                "FROM signal_observations"
            ).fetchone()
            ev_age   = _mh_age(ev_row["last_run"])
            ev_total  = ev_row["total"] or 0
            ev_filled = ev_row["filled"] or 0
            fill_rate = round(ev_filled / ev_total * 100, 1) if ev_total else 0.0

            # Snapshot prev_filled before updating so we can detect a stalled drain.
            prev_filled = _mhealth_state["last_filled"]
            _mhealth_state["last_filled"] = ev_filled

            if ev_age is not None and ev_age > 14400:
                age_str = f"{int(ev_age // 3600)}h{int((ev_age % 3600) // 60):02d}m"
                _mh_fire(
                    "eval_stalled",
                    "\U0001f534 MEASUREMENT: evaluator stalled",
                    f"signal_evaluator last ran {age_str} ago (cadence=30min, threshold=4h).\n"
                    f"fill_rate={fill_rate}% ({ev_filled}/{ev_total}). Restart may be needed.",
                )
            elif fill_rate < 5.0 and ev_total > 100:
                # Only fire if drain is STALLED — filled_1d flat since last check.
                # Suppress while drain is actively progressing (low fill_rate is
                # expected for the entire ~20h backlog drain window).
                if prev_filled is not None and ev_filled <= prev_filled:
                    _mh_fire(
                        "eval_low_fill",
                        "\U0001f534 MEASUREMENT: evaluator drain stalled",
                        f"fwd_return fill_rate={fill_rate}% ({ev_filled}/{ev_total} obs) — "
                        f"no progress since last check (prev={prev_filled}).\n"
                        "Evaluator may be stuck or throwing errors — check logs.",
                    )

            _mh_conn.close()
        except Exception as _mh_exc:
            logger.debug("[mhealth_watch] error: %s", _mh_exc)

    schedule.every(15).minutes.do(_run_measurement_health_watch)
    # SWINGDESK-W3: agent auto-spread daemon — BUILT GATED-OFF (config.AUTO_SPREADS_ENABLED=False).
    # Bound at startup (lifecycle doctrine: not lazy). When OFF it heartbeats only; on a
    # detected flip it NTFYs. The master gate is enforced in auto_spread.submit_if_allowed.
    from engine.auto_spread import run_auto_spread_cycle as _run_auto_spread_cycle
    schedule.every(15).minutes.do(_run_auto_spread_cycle)
    # SWINGDESK-W4: spread exit manager — monitors open filled spreads, alert-only for
    # manual / gated auto-close for auto. Bound at startup (lifecycle doctrine).
    from engine.spread_exit_manager import run_spread_exit_cycle as _run_spread_exit_cycle
    schedule.every(5).minutes.do(_run_spread_exit_cycle)
    # HM-GEX-CANONICAL 2026-05-31: /api/gex-snapshot (engine.options_flow_gex, Polygon) is the
    # SINGLE GEX source. The 3 legacy refreshers are DISABLED (modules preserved/dormant — see
    # XO_BACKLOG). One canonical intraday refresh replaces them; feeds all displays via adapters.
    schedule.every(15).minutes.do(run_gex_snapshot_refresh)  # canonical GEX (Polygon): 15 min during RTH
    # schedule.every(15).minutes.do(run_gex_refresh)        # DISABLED HM-GEX-CANONICAL — CBOE/gex_scanner
    # schedule.every(30).minutes.do(run_alpaca_gex_refresh)  # DISABLED HM-GEX-CANONICAL — Alpaca/gex_calculator
    # schedule.every(15).minutes.do(run_gex_overlay_update) # DISABLED HM-GEX-CANONICAL — gex_overlay
    schedule.every().day.at("06:00").do(run_morning_briefing)         # Battle Station: 6:00 AM AZ (was every 5 min)
    schedule.every().day.at("06:00").do(run_archer_morning_briefing)  # Phase 3.6: Archer briefing 6:00 AM AZ
    schedule.every(15).minutes.do(_bg_archer_alerts)                  # HM-ARCHER-REBUILD: tiered alerts (RTH-gated)
    schedule.every().day.at("05:45").do(run_reveille_morning)         # HM-REVEILLE: pre-market XO brief 5:45 AM AZ (before 06:00 intel; market-day-aware)
    schedule.every().day.at("06:00").do(run_intel_report_morning)     # Intel Report + ntfy push: 6:00 AM AZ
    schedule.every().day.at("06:15").do(run_phaser_lock_morning)      # HM-PHASER-LOCK: daily Trade-of-the-Day 6:15 AM AZ — HM-DRYDOCK-2 HIGH1: moved 06:05→06:15 so it reads AFTER the ~06:12 strategy_signals scan (was firing 7min early on an empty feed → 0 qualified); stays pre-open (06:30 ET).
    schedule.every().day.at("02:30").do(run_filter_contribution_sweep)  # HM-DRYDOCK #4: OFF-PEAK filter-contribution ablation → cache (never near cadence/market hours)
    schedule.every().day.at("20:00").do(run_intel_report_evening)     # Intel Report evening prep: 8:00 PM AZ

    # === HM-IC-SQUADRON Pillar 2 — 6-agent stack (SHADOW MODE) ===
    # Nightly Scout → Structurer → Risk Officer → Trigger chain at 21:00 AZ.
    # Manager walks shadow positions every 30 min during market hours.
    # ZERO live execution until capital_ladder Stage-0 promotion criteria
    # (50 closes ≥70% WR ≥30 days sustained). Crash-safe scheduler wrappers
    # so any Squadron error never affects other scheduled jobs.
    def _run_ic_squadron_cycle_safe():
        try:
            from engine.ic_squadron import run_ic_squadron_cycle
            return run_ic_squadron_cycle(max_proposals=5)
        except Exception as _e:
            console.log(
                f"[red][IC-SQUADRON] nightly cycle crash: "
                f"{type(_e).__name__}: {_e!r}"
            )
            return None
    schedule.every().day.at("21:00").do(_run_ic_squadron_cycle_safe)  # nightly Scout

    def _run_ic_manager_safe():
        from engine.risk_manager import RiskManager
        try:
            if not RiskManager.is_market_hours():
                return None
        except Exception:
            pass
        try:
            from engine.ic_squadron import run_manager
            return run_manager()
        except Exception as _e:
            console.log(
                f"[red][IC-MANAGER] cycle crash: "
                f"{type(_e).__name__}: {_e!r}"
            )
            return None
    schedule.every(30).minutes.do(_run_ic_manager_safe)  # Manager every 30min mkt hours
    # === /HM-IC-SQUADRON Pillar 2 ===

    # === HM-PRIME-DIRECTIVE-MONITOR v1 — 2026-05-22 ===
    # Polls USTR / Commerce / Federal Register RSS feeds for tariff +
    # trade-policy events every 30 min during market hours. On match,
    # emits to events bus event_type='macro' source='prime_directive'.
    # IC Risk Officer + future regime auto-tuner consume from the bus.
    # Spec: ~/.claude/projects/.../project_hm_overnight_cook_2026-05-22.md
    def _run_prime_directive_safe():
        from engine.risk_manager import RiskManager
        try:
            if not RiskManager.is_market_hours():
                return None
        except Exception:
            pass
        try:
            from engine.prime_directive_monitor import scan_prime_directive
            return scan_prime_directive()
        except Exception as _e:
            console.log(
                f"[red][PRIME-DIRECTIVE] scheduler-side crash: "
                f"{type(_e).__name__}: {_e!r}"
            )
            return None
    schedule.every(30).minutes.do(_run_prime_directive_safe)  # HM-PRIME-DIRECTIVE v1
    # === /HM-PRIME-DIRECTIVE-MONITOR ===

    # === HM-IC-SQUADRON Pillar 5 — Nightly Strategy Lab sweep ===
    # 2026-05-22 — orchestrator on top of engine/strategy_lab.py. Fires at
    # 20:00 AZ alongside the evening intel report so morning_brief.json
    # has top regime-strategy fits ready for the 06:00 AZ briefing.
    # Spec: project_hm_ic_squadron_approved.md Pillar 5.
    def _run_strategy_lab_sweep_safe():
        from engine.strategy_lab_sweep import run_strategy_lab_sweep
        try:
            return run_strategy_lab_sweep()
        except Exception as _e:
            console.log(
                f"[red][LAB-SWEEP] scheduler-side crash: "
                f"{type(_e).__name__}: {_e!r}"
            )
            return None
    schedule.every().day.at("20:00").do(_run_strategy_lab_sweep_safe)  # IC Pillar 5
    # === /HM-IC-SQUADRON Pillar 5 ===

    # === HM-POST-EXIT-TRACKER 2026-05-20 ===
    # Daily scan of post_exit_watch rows: flag any exit where the symbol
    # subsequently traded >5% above the exit price. Runs at 06:30 AZ
    # (after morning briefing at 06:00) so the day's price discovery has
    # had time to populate. Crash-safe: scanner swallows all per-row errors.
    @_hm_bq_instr("run_post_exit_scan")
    def run_post_exit_scan():
        try:
            from engine.post_exit_tracker import run_daily_scan as _pex_scan
            _out = _pex_scan()
            console.log(
                f"[cyan][HM-POST-EXIT-TRACKER] daily scan: "
                f"checked={_out.get('checked')} flagged={_out.get('newly_flagged')} "
                f"aged_out={_out.get('aged_out')} errors={_out.get('errors')}"
            )
        except Exception as _pex_top:
            console.log(
                f"[red][HM-POST-EXIT-TRACKER] daily scan top-level error: "
                f"{type(_pex_top).__name__}: {_pex_top!r}"
            )
    schedule.every().day.at("06:30").do(run_post_exit_scan)
    # === /HM-POST-EXIT-TRACKER ===

    # HM-I-β-Item5: daily reconciliation canary (replaces ε noise canary, commit d06c33c).
    # Compares internal book vs Alpaca paper at market close, writes JSON, NTFYs on drift.
    try:
        from engine.reconciliation import run_reconciliation
        schedule.every().day.at("13:30").do(run_reconciliation)       # 13:30 AZ = 4:30 PM ET, 30 min post NYSE close
    except Exception as _re:
        console.log(f"[yellow]reconciliation schedule registration skipped: {type(_re).__name__}: {_re!r}")
    schedule.every(30).minutes.do(run_ah_scanner)                     # AH Earnings Scanner: 4–7 PM AZ (30 min)
    schedule.every(15).minutes.do(run_premarket_scanner)              # Pre-Market Scanner: 6–9:25 AM AZ (15 min)
    schedule.every().day.at("06:00").do(run_earnings_scan_inject)     # Earnings → scan_universe inject: 6:00 AM AZ (HM-AR-β rename 2026-05-07)
    schedule.every().day.at("06:00").do(run_carts_persist)            # CARTS retail nowcast persistence: 6:00 AM AZ (HM-AO 2026-05-17)
    schedule.every(5).minutes.do(run_earnings_day_scan)               # Earnings Day: every 5 min market hours
    schedule.every(3).minutes.do(run_bk_orb_scan)                     # HM-BK-A opening-range-breakout confirmatory (intraday); self-gates ET 09:46–12:00 + flag, default-OFF via ORB_CONFIRMATORY_VOTE_ENABLED
    schedule.every().day.at("06:45").do(run_opening_range)            # Battle Station: opening range 6:45 AM AZ
    # HM-AS-β LOOP 3 (2026-05-29): run_battle_station_monitor MOVED to its own daemon
    # thread (_battle_station_scheduler_thread above) — decoupled from the shared
    # scheduler so heavy-scanner batching can't starve the 60s-critical options monitor.
    # Was: schedule.every(2).minutes.do(run_battle_station_monitor)
    # HM-WR-DAEMON-THREAD 2026-05-20: run_war_room moved to its own daemon
    # thread (see _war_room_scheduler_thread above) to bypass single-threaded
    # schedule.run_pending() queue blocking. Cadence preserved at 300s.
    schedule.every(30).minutes.do(_bg_autopilot)           # Autopilot: every 30 min (HM-AS-β §B Loop 2: _bg wrap — synchronous loop-blocker, avg 96s/max 169s)
    schedule.every(10).minutes.do(_bg_whisper)             # Whisper Network: every 10 min (HM-AS-β §B Loop 1: _bg wrap — was the loop-blocker, avg 831s)
    schedule.every(15).minutes.do(run_strength_scan)        # Strength Scanner: every 15 min
    schedule.every(1).hours.do(run_strategy_race)           # Strategy Race: hourly update
    schedule.every(30).minutes.do(run_weekly_picks)          # Weekly Picks: checks every 30 min, sends Sunday 6PM ET
    schedule.every(15).minutes.do(run_cross_asset_check)    # Cross-Asset: every 15 min
    schedule.every(15).minutes.do(run_flow_lean)            # Flow Lean: every 15 min (options premium directional bias)
    schedule.every(15).minutes.do(run_ai_saas_disruption)   # AI SaaS Disruption: IGV + 13 SaaS names, 4 triggers, posts to 9000
    schedule.every(5).minutes.do(run_cto_advisory)            # CTO Advisory: checks every 5 min, fires 4x daily (pre_market, post_open, pre_close, post_close). Was every(30) -- identical HM-KIRK-REHOME phase-drift bug (flagged 2026-07-06, confirmed missing both 07-07 slots): a 30-min poll's phase can drift outside every 10-min slot window depending on restart timing. 5-min cadence (< the 10-min window) guarantees a hit regardless of restart phase, same fix as run_kirk_advisory_job.
    schedule.every(5).minutes.do(run_kirk_advisory_job)      # Kirk Advisory: persist kirk_advisory_log at open/midday/close (AZ weekdays) — HM-KIRK-REHOME 2026-06-01. Was every(30) until HM-OPS-SENTINEL P3.8 (2026-07-06): a 30-min poll's phase (set by whenever main.py last restarted) can drift outside every one of the three 10-min-wide slot windows -- confirmed today all 3 slots (06:35/09:30/13:05) were missed because the post-restart tick landed at :17:28/:47:28, never inside :x5-:x5+10. 5-min cadence (< the 10-min window) guarantees a hit regardless of restart phase.
    schedule.every(30).minutes.do(run_ready_room)             # Ready Room: checks every 30 min, fires 4x daily (8:00/9:15/12:00/3:30 ET)
    schedule.every(30).minutes.do(run_team_advisor)           # Advisory Team (Grok/Ollie+Troi+Worf): fires at 9:30 AM and 1:30 PM ET
    schedule.every(5).minutes.do(run_portfolio_monitor)       # Ship's Computer: Captain's Portfolio monitor (stop breaches, big moves, new advice)
    schedule.every(5).minutes.do(run_oi_morning_snapshot)    # OI Tracker: baseline snapshot at market open (9:30 ET)

    def run_bull_spread_signals():
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        try:
            import strategies.bull_spread_v1  # noqa: F401 — auto-registers BullSpreadV1 on first call
            from strategies.registry import registry
            from strategies.signal_store import persist
            from strategies.executor import execute_signal
            from strategies.base import MarketContext
            from datetime import datetime, timezone
            # Reuse cached regime from run_ma_regime_update (fires every 15 min)
            # Normalize regime_ma vocabulary → strategy-facing BULL/BEAR
            _r = _last_ma_regime or "BULL_CROSS"
            regime = "BULL" if _r in ("BULL_CROSS", "CAUTIOUS_BULL") else "BEAR"
            ctx = MarketContext(
                as_of=datetime.now(timezone.utc),
                regime=regime,
                vix=0.0,        # not used by BullSpreadV1.evaluate()
                spy_price=0.0,  # not used by BullSpreadV1.evaluate()
            )
            signals = registry().evaluate_all(ctx)
            if not signals:
                console.log("[dim]bull_spread_v1: no signals this tick")
                return
            for sig in signals:
                sid = persist(sig)
                result = execute_signal(sig, signal_id=sid)
                console.log(
                    f"[cyan]bull_spread_v1: {sig.ticker} {sig.exit_tag} → {result.status}"
                )
        except Exception as _bse:
            console.log(f"[yellow]bull_spread_v1 signals skip: {_bse}")
    schedule.every(15).minutes.do(run_bull_spread_signals)  # Bull Spread v1: entry signals, 15 min

    def run_bull_spread_exits():
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        try:
            from strategies.exit_manager import run_cycle
            summary = run_cycle("bull_spread_v1")
            if summary.get("total", 0):
                console.log(f"[cyan]bull_spread_v1 exits: {summary}")
        except Exception as _bee:
            console.log(f"[yellow]bull_spread_v1 exits skip: {_bee}")
    schedule.every(5).minutes.do(run_bull_spread_exits)     # Bull Spread v1: exit evaluation, 5 min

    def run_bear_put_spread_signals():
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        try:
            import strategies.bear_put_spread_v1  # noqa: F401 — auto-registers BearPutSpreadV1
            from strategies.registry import registry
            from strategies.signal_store import persist
            from strategies.executor import execute_signal
            from strategies.base import MarketContext
            from datetime import datetime, timezone
            # Same regime normalization as bull_spread_v1 scheduler
            _r = _last_ma_regime or "BULL_CROSS"
            regime = "BULL" if _r in ("BULL_CROSS", "CAUTIOUS_BULL") else "BEAR"
            ctx = MarketContext(
                as_of=datetime.now(timezone.utc),
                regime=regime,
                vix=0.0,
                spy_price=0.0,
            )
            all_sigs = [s for s in registry().evaluate_all(ctx)
                        if s.strategy_id == "bear_put_spread_v1"]
            if not all_sigs:
                console.log("[dim]bear_put_spread_v1: no signals this tick")
                return
            for sig in all_sigs:
                sid = persist(sig)
                result = execute_signal(sig, signal_id=sid)
                console.log(
                    f"[cyan]bear_put_spread_v1: {sig.ticker} tier={sig.payload.get('tier','?')} "
                    f"→ {result.status}"
                )
        except Exception as _bpse:
            console.log(f"[yellow]bear_put_spread_v1 signals skip: {_bpse}")
    schedule.every(15).minutes.do(run_bear_put_spread_signals)  # Bear Spread v1: entry signals, 15 min

    def run_bear_put_spread_exits():
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        try:
            from strategies.exit_manager import run_cycle
            summary = run_cycle("bear_put_spread_v1")
            if summary.get("total", 0):
                console.log(f"[cyan]bear_put_spread_v1 exits: {summary}")
        except Exception as _bpee:
            console.log(f"[yellow]bear_put_spread_v1 exits skip: {_bpee}")
    schedule.every(5).minutes.do(run_bear_put_spread_exits)     # Bear Spread v1: exit evaluation, 5 min

    def run_bull_call_spread_signals():
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        try:
            import strategies.bull_call_spread_v1  # noqa: F401 — auto-registers BullCallSpreadV1
            from strategies.registry import registry
            from strategies.signal_store import persist
            from strategies.executor import execute_signal
            # engine.market_data.get_regime does not exist — ImportError every tick since May 1.
            # Use same normalization as other spread schedulers + proper MarketContext.
            from strategies.base import MarketContext
            from datetime import datetime, timezone
            _r = _last_ma_regime or "BULL_CROSS"
            _regime = "BULL" if _r in ("BULL_CROSS", "CAUTIOUS_BULL") else "BEAR"
            ctx = MarketContext(as_of=datetime.now(timezone.utc), regime=_regime, vix=0.0, spy_price=0.0)
            all_sigs = [s for s in registry().evaluate_all(ctx)
                        if s.strategy_id == "bull_call_spread_v1"]
            if not all_sigs:
                console.log("[dim]bull_call_spread_v1: no signals this tick")
                return
            for sig in all_sigs:
                sid = persist(sig)
                result = execute_signal(sig, signal_id=sid)
                console.log(
                    f"[cyan]bull_call_spread_v1: {sig.ticker} tier={sig.payload.get('tier','?')} "
                    f"→ {result.status}"
                )
        except Exception as _bcse:
            console.log(f"[yellow]bull_call_spread_v1 signals skip: {_bcse}")
    schedule.every(15).minutes.do(run_bull_call_spread_signals)  # Bull Call Spread v1: entry signals, 15 min

    def run_bull_call_spread_exits():
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        try:
            from strategies.exit_manager import run_cycle
            summary = run_cycle("bull_call_spread_v1")
            if summary.get("total", 0):
                console.log(f"[cyan]bull_call_spread_v1 exits: {summary}")
        except Exception as _bcee:
            console.log(f"[yellow]bull_call_spread_v1 exits skip: {_bcee}")
    schedule.every(5).minutes.do(run_bull_call_spread_exits)     # Bull Call Spread v1: exit evaluation, 5 min

    # Auto-Screener: runs presets every 15 min, posts new finds to port 9000
    def run_auto_screener():
        if not should_run_task("run_auto_screener", throttle_mins=30):
            return
        try:
            from engine.screener_engine import run_screener, PRESETS
            from engine.signal_poster import post_to_9000
            import time as _t
            _seen_key = getattr(run_auto_screener, "_seen", set())
            run_auto_screener._seen = _seen_key
            now_key = _t.strftime("%Y-%m-%d")
            for preset_name, preset_filters in PRESETS.items():
                try:
                    results = run_screener({**preset_filters, "limit": 10})
                    for r in results:
                        uid = f"{preset_name}:{r['symbol']}:{now_key}"
                        if uid not in _seen_key:
                            _seen_key.add(uid)
                            post_to_9000("SCREENER", {
                                "preset": preset_name,
                                "symbol": r["symbol"],
                                "score": r.get("score", 0),
                                "change": r.get("change", 0),
                                "rsi": r.get("rsi"),
                                "fleet_bull": r.get("fleet_bull", 0),
                            })
                except Exception:
                    pass
        except Exception as _se:
            console.log(f"[yellow]Auto-screener skip: {_se}")
    schedule.every(15).minutes.do(run_auto_screener)          # Auto-Screener: runs presets, posts new finds to 9000

    # Bootstrap Intelligence: seeds adaptive engine from trade history
    def run_bootstrap():
        try:
            from engine.bootstrap_intelligence import refresh_bootstrap
            refresh_bootstrap()
        except Exception as _be:
            console.log(f"[yellow]Bootstrap skip: {_be}")
    schedule.every().day.at("00:01").do(run_bootstrap)        # Daily midnight refresh

    # First-boot seed: run bootstrap immediately so brain context has data on day 1
    try:
        from engine.bootstrap_intelligence import refresh_bootstrap
        refresh_bootstrap()
    except Exception as e:
        console.log(f"[yellow]Bootstrap first run skip: {e}")

    # Adaptive Strategy: hourly trust score update
    def run_adaptive():
        try:
            from engine.adaptive_strategy import update_trust_scores
            update_trust_scores()
        except Exception as _ae:
            console.log(f"[yellow]Adaptive skip: {_ae}")
    schedule.every(1).hours.do(run_adaptive)                  # Trust scores: hourly

    def run_weekly_agent_review():
        try:
            from engine.adaptive_strategy import weekly_agent_review
            weekly_agent_review()
        except Exception as _we:
            console.log(f"[yellow]Weekly agent review skip: {_we}")
    schedule.every().sunday.at("16:00").do(run_weekly_agent_review)  # Weekly review at 4 PM AZ Sunday

    def run_daily_enrichment():
        try:
            from engine.daily_enrichment import run_daily_enrichment as _enrich
            _enrich()
        except Exception as _de:
            console.log(f"[yellow]Daily enrichment skip: {_de}")
    schedule.every(30).minutes.do(run_daily_enrichment)        # Enrichment gate fires at 2:30 PM AZ

    # Dr. Crusher Healthcheck — auto-detect and repair common failures every 5 min
    def dr_crusher_check():
        import subprocess, requests as _req, logging as _log
        _hc = _log.getLogger("dr_crusher")

        # Check 2: Is the scan model responsive? (plutus lives on Ollie Box)
        try:
            r = _req.post(
                f"{OLLIE_URL}/api/generate",
                json={"model": "0xroyce/plutus", "prompt": "ok",
                      "stream": False, "think": False,
                      "options": {"num_predict": 3}},
                timeout=30,
            )
            if not r.ok:
                _hc.warning(f"Scan model not responding: {r.status_code}")
        except Exception:
            _hc.warning("Scan model timeout — will retry next cycle")

        # Check 3: DB accessible?
        _db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "trader.db")
        try:
            _tc = sqlite3.connect(_db_path, timeout=5)
            _tc.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
            _tc.execute("SELECT 1")
            _tc.close()
        except sqlite3.OperationalError as _dbe:
            _hc.error(f"DB LOCKED — killing stale backtest processes: {_dbe}")
            subprocess.run(["pkill", "-9", "-f", "backtest"], capture_output=True)

        # Check 4: Last scan timestamp (only warn during market hours)
        try:
            import pytz as _ptz
            from datetime import datetime as _dtc
            _az = _ptz.timezone("US/Arizona")
            _now_h = az_now().hour
            _now_wd = az_now().weekday()
            if _now_wd < 5 and 7 <= _now_h <= 14:
                _sc = sqlite3.connect(_db_path, timeout=5)
                _sc.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
                _last_scan = _sc.execute(
                    "SELECT MAX(created_at) as last FROM crew_decisions "
                    "WHERE created_at >= datetime('now', '-10 minutes')"
                ).fetchone()
                _sc.close()
                if not _last_scan or not _last_scan[0]:
                    _hc.warning("No scan activity in last 10 minutes during market hours")
        except Exception:
            pass

    schedule.every(15).minutes.do(dr_crusher_check)

    # EOD Scorecard — Captain's Log (4:15 PM ET = 13:15 AZ)
    def run_eod_scorecard_job():
        try:
            from engine.eod_scorecard import run_eod_scorecard_job as _run
            _run()
        except Exception as e:
            console.log(f"[yellow]EOD Scorecard error: {e}")
    schedule.every(5).minutes.do(run_eod_scorecard_job)      # EOD: checks every min, fires once at 4:15 PM ET

    # Pattern Matcher fingerprint — Mr. Spock (capture at morning briefing time ~8 AM ET = 5 AM AZ)
    _fingerprint_done_today = [False]
    def run_fingerprint_capture():
        import pytz as _pytz
        from datetime import datetime as _dt
        az = _pytz.timezone("US/Arizona")
        now = az_now()
        if now.weekday() >= 5:
            return
        if now.hour < 4:
            _fingerprint_done_today[0] = False
            return
        if not _fingerprint_done_today[0] and now.hour >= 5:
            _fingerprint_done_today[0] = True
            try:
                from engine.pattern_matcher import capture_fingerprint
                result = capture_fingerprint()
                if result.get("saved"):
                    console.log(f"[cyan]Spock: session fingerprint captured — {result.get('session_type','?')}")
            except Exception as e:
                console.log(f"[yellow]Pattern fingerprint error: {e}")
    schedule.every(30).minutes.do(run_fingerprint_capture)    # Fingerprint: capture once per morning
    schedule.every(30).minutes.do(run_weekly_elimination)    # Weekly Elimination: checks every 5 min, fires Friday post_market only
    schedule.every(15).minutes.do(run_skew_check)          # Skew Monitor: every 15 min
    schedule.every(1).hours.do(run_fundamental_scan)       # Fundamentals: hourly refresh

    # Alpha Engine — Signal Scorecard (hourly outcome scoring)
    def run_signal_scorecard():
        if not should_run_task("run_signal_scorecard", throttle_mins=120):
            return
        try:
            from engine.signal_scorecard import score_signals
            score_signals()
        except Exception as e:
            console.log(f"[yellow]Signal Scorecard error: {e}")
    schedule.every(1).hours.do(run_signal_scorecard)       # Alpha Engine: score signal outcomes hourly

    # Alpha Engine — Indicator Benchmark (daily 4:30 PM ET)
    def run_indicator_bench():
        try:
            from engine.indicator_bench import run_indicator_bench
            run_indicator_bench()
        except Exception as e:
            console.log(f"[yellow]Indicator Bench error: {e}")
    schedule.every(30).minutes.do(run_indicator_bench)      # Alpha Engine: daily 4:30 PM ET benchmark
    schedule.every(30).minutes.do(run_trend_forecast)       # Trend Forecast: every 30 min
    schedule.every(30).minutes.do(run_strategy_presets)      # Strategy Presets: every 30 min
    # schedule.every(30).minutes.do(run_discovery_scan)      # RETIRED: replaced by Volume Radar below
    schedule.every().sunday.at("22:00").do(run_volume_universe_refresh)   # Universe refresh: Sunday 10 PM MST
    schedule.every().day.at("23:00").do(run_volume_baselines)             # Baselines: nightly 11 PM MST (skip weekends internally)
    schedule.every().day.at("13:05").do(_bg_networth_snapshot)            # HM-AM: net-worth snapshot, 13:05 AZ (post 16:00 ET close) — daily baseline
    schedule.every().day.at("23:30").do(run_crew_dissent_nightly)         # HM-CREW-DISSENT: nightly resolve + recompute (post-close, after scoring)
    schedule.every(15).minutes.do(run_volume_market_scan)                 # Volume Radar: every 15 min during market hours
    schedule.every(5).minutes.do(run_volume_red_alert)                    # Red Alert: every 2 min during market hours
    schedule.every(30).minutes.do(run_sma_scan)               # 200 SMA Filter: checks every 30 min, runs every 4 hours
    schedule.every(1).hours.do(run_impulse_check)              # Impulse Detector: hourly during market hours
    schedule.every(2).hours.do(run_imbalance_scan)             # Imbalance Zones: every 2 hours (zones are stable)
    schedule.every(30).minutes.do(run_theta_scan)              # Theta Scanner: checks every 30 min, runs every 4 hours
    schedule.every(15).minutes.do(run_gap_scan)                 # Gap Scanner: checks every 5 min, fires once at market open
    schedule.every(15).minutes.do(run_gap_fill_check)           # Gap Fill Tracker: every 5 min during market hours
    schedule.every(30).minutes.do(_bg_squeeze_watcher)          # HM-AO-β Squeeze Watcher (HM-AS-β.2 thread-wrapper): 30-min, default-OFF via SQUEEZE_WATCHER_ENABLED
    schedule.every(30).minutes.do(_bg_bbkc_squeeze_watcher)     # HM-SQUEEZE-BBKC-COMPRESSION (2026-05-24): 30-min, default-OFF via BBKC_SQUEEZE_WATCHER_ENABLED
    schedule.every().day.at("20:30").do(_bg_rs_rank)            # HM-RS-RANK-VS-SPY (2026-05-24): nightly post-close, default-OFF via RS_RANK_ENABLED
    schedule.every().day.at("20:45").do(_bg_minervini_filter)   # HM-MINERVINI-TREND-FILTER (2026-05-24): nightly post-close +15min so rs_rank LEFT JOIN sees fresh data, default-OFF via MINERVINI_FILTER_ENABLED
    schedule.every().day.at("21:10").do(run_bk_avwap_scan)      # HM-BK-B anchored-VWAP confirmatory (nightly post-close), default-OFF via AVWAP_CONFIRMATORY_VOTE_ENABLED
    schedule.every().day.at("21:20").do(run_bk_box_scan)        # HM-BK-C tight-box breakout confirmatory (nightly, AFTER B — shares 30-min OHLCV cache), default-OFF via BOX_CONFIRMATORY_VOTE_ENABLED
    schedule.every().day.at("21:00").do(_bg_ollie_machine_daily) # OLLIE-MACHINE-P3 (2026-06-01): SIM pick SELECTION, +15min after Minervini so signals are fresh. tracking-mode, default-OFF via OLLIE_MACHINE_LOOP_ENABLED
    schedule.every().day.at("06:30").do(_bg_ollie_machine_enter) # OLLIE-MACHINE-P3 (HM-OLLIE-MACHINE-BRACKET-WINDOW 2026-06-05): bracket+SIM-enter in the market window (trade-levels healthy; 21:00 had endpoint cold). default-OFF via OLLIE_MACHINE_LOOP_ENABLED
    schedule.every(20).minutes.do(_bg_ollie_machine_exits)      # OLLIE-MACHINE-P3 (2026-06-01): SIM exit-monitor, RTH-gated. ledger-direct close, default-OFF via OLLIE_MACHINE_LOOP_ENABLED
    schedule.every().day.at("21:30").do(_bg_ollie_machine_p4_gate) # OLLIE-MACHINE-P4 (2026-06-02): SIM promotion gate, read-only. Daily tick; internally eval'd every 10 closed trades OR weekly. Writes ollie_machine_p4_status only.
    schedule.every(30).minutes.do(_bg_sell_the_news)            # HM-SHORT-ENGINE (2026-06-05): sell-the-news shadow scanner, RTH-gated, observation-only (shadow-bridge). default-OFF via SELL_THE_NEWS_ENABLED
    schedule.every(30).minutes.do(_bg_source_health_dms)        # HM-SOURCE-HEALTH-WATCHER (2026-06-02): dead-man's-switch for the source-health watcher cron (reads its heartbeat, NTFYs if stale). Different mechanism than the cron it guards.
    # Capitol Trades Fund — Congress copycat scan (daily at market open, 9:35 AM ET)
    from engine.capitol_fund import run_capitol_scan as _raw_capitol_scan
    def run_capitol_scan():
        result = _raw_capitol_scan()
        try:
            from engine.signal_poster import post_to_9000
            post_to_9000("CONGRESS", {
                "message": "Capitol Trades scan completed",
                "result": str(result)[:300] if result else "no new trades",
            })
        except Exception:
            pass
    schedule.every(15).minutes.do(run_capitol_scan)
    schedule.every(15).minutes.do(run_cost_monitor)         # Cost Monitor: every 15 min (budget alert, auto-pause)
    # Bridge Vote — Tier 3 morning vote at 9:00 AM ET (fires once per day)
    try:
        from engine.bridge_vote import run_bridge_vote_job
        schedule.every(5).minutes.do(run_bridge_vote_job)   # Bridge Vote: checks every 5 min, fires 9:00-9:10 AM ET
    except Exception as _bv_sched_err:
        console.log(f"[yellow]Bridge Vote scheduler skip: {_bv_sched_err}")
    schedule.every(30).minutes.do(run_universe_scan)         # Universe Scanner: checks every 30 min, runs 9 PM MST (12 AM ET)
    schedule.every(30).minutes.do(run_strategy_scan)         # Strategy Scan: checks every 30 min, runs 10 PM MST (1 AM ET)
    schedule.every(10).minutes.do(run_chekov_stoploss)        # Chekov SL/TP: every 10 min, check positions vs stop/target
    schedule.every(10).minutes.do(run_guardian_sweep)         # HM-GUARDIAN-ADOPTION: exit-only stop/TP sweep, ALL exit_only agents w/ open positions (not just guardian-of-forever, since 2026-07-04), routes exits to Alpaca

    # HM-GUARDIAN-ADOPTION: one-time immediate sweep at startup so there is NO
    # ~10-min coverage gap after a restart (the 10-min schedule above is the
    # ongoing cadence). Daemon thread + warm-up delay so it never blocks boot and
    # market data / the Alpaca bridge are ready. Idempotent; run_guardian_sweep
    # is fully self-guarded (never raises). Fires every restart by design.
    def _guardian_startup_sweep():
        import time as _t
        _t.sleep(30)  # let market_data + Alpaca bridge warm up post-boot
        try:
            console.log("[cyan]🛡️ Guardian startup sweep (zero-gap coverage after restart)")
            run_guardian_sweep()
        except Exception as _gss_e:
            console.log(f"[yellow]Guardian startup sweep error: {_gss_e}")
    threading.Thread(target=_guardian_startup_sweep, daemon=True,
                     name="guardian-startup-sweep").start()

    schedule.every(1).minutes.do(run_events_bus_consumer)     # HM-EVENTS-BUS-CONSUMER: drain pending signals_v2 (NYSE hours only)
    schedule.every(1).minutes.do(run_pending_manual_closes)   # Manual-close queue (NYSE hours only) — reads data/pending_manual_closes.json
    schedule.every(5).minutes.do(run_signal_center_refresh)   # HM-SIGNAL-CENTER-REFRESH: keep signal_history fresh (NYSE hours only)
    schedule.every(5).minutes.do(run_daily_snapshot_refresh)  # HM-DAILY-SNAPSHOT-REFRESH: EOD trigger 13:00-14:00 AZ post-close window
    schedule.every(15).minutes.do(run_chekov_intraday_convergence)  # HM-AW: Chekov intraday convergence buyer (market hours only)
    schedule.every(30).minutes.do(run_metals_commentary)     # Dalio Metals: checks every 30 min, runs 7 AM MST only
    schedule.every(15).minutes.do(run_premarket_gaps)         # Pre-market gaps: checks every 15 min, fires 1 AM MST (4 AM ET)
    schedule.every(15).minutes.do(run_finviz_premarket_scan)   # Finviz watchlist: 5-min check, fires 6:15 AZ (9:15 ET)
    schedule.every(5).minutes.do(run_sulu_autoclose)          # Lt. Sulu EOD: auto-close all positions at 12:45 PM MST (3:45 PM ET)
    schedule.every(2).minutes.do(run_crew_scanner_job)        # Crew Scanner: agent signal pipeline (every 2 min, alpha squad only)

    # Ollie Extended-Hours Scan — pre-market (7–9:30 AM ET) + after-hours (4–6 PM ET)
    # HM-CHANNEL-SCANNER-DEADLOCK P2 (2026-05-27): re-entrancy guard. The
    # underlying `ollie_auto_check → _ollie_channel_scan → _scan_all` chain
    # holds the schedule-loop thread while it serializes 8 ThreadPoolExecutor
    # workers on `_yahoo_lock`. If the 10-min tick fires while a previous
    # instance is still draining, the schedule library would queue the second
    # call behind the first, compounding the lockup. Non-blocking acquire
    # short-circuits the re-entry: caller logs and bails immediately.
    _ollie_ext_scan_lock = threading.Lock()

    def run_ollie_extended_scan():
        """Run ollie-auto signal pipeline during extended trading hours.
        Other agents are market-hours only; this fires exclusively for ollie-auto.
        """
        if not _ollie_ext_scan_lock.acquire(blocking=False):
            console.log("[yellow]Ollie Extended-Hours: previous scan still running, skipping this tick")
            return
        try:
            from engine.risk_manager import RiskManager
            if not RiskManager.is_extended_trading_hours():
                return
            try:
                from engine.crew_scanner import ollie_auto_check, gather_market_context
                ctx = gather_market_context()
                trades = ollie_auto_check(ctx)
                if trades:
                    console.log(f"[bold cyan]🌙 Ollie Extended-Hours: {len(trades)} trade(s) executed")
            except Exception as _oex_err:
                console.log(f"[yellow]Ollie Extended-Hours error: {_oex_err}")
        finally:
            _ollie_ext_scan_lock.release()
    schedule.every(10).minutes.do(run_ollie_extended_scan)   # Ollie Extended-Hours: every 10 min during pre/post market
    schedule.every(5).minutes.do(run_battle_station_0dte_job) # Battle Station 0DTE: rules-based SPY 0DTE scanner
    from engine.recovery_protocol import run_recovery_scan
    schedule.every(15).minutes.do(run_recovery_scan)           # Recovery Protocol: checks every 15 min during market hours
    from engine.wheel_strategy import run_wheel_scan, check_wheel_assignments
    schedule.every(15).minutes.do(run_wheel_scan)              # Wheel Strategy: scan for put-selling opportunities every 15 min
    schedule.every(1).hours.do(check_wheel_assignments)        # Wheel Strategy: check for option assignments hourly

    # === HM-SHADOW-CSP (2026-06-07): options-income bake-off, shadow-first ===
    # Two candidate CSP seats emit to the GHOST options book and are scored
    # forward (return-on-collateral) vs Troi's wheel baseline. Default-OFF via
    # SHADOW_CSP_ENABLED + per-seat envs. Ghost CSPs are auto-managed/expired by
    # paper_trader's book-agnostic short-premium + expiry paths — no exit wiring.
    #   • plutus seat shares McCoy's resident plutus-v1 (zero new VRAM) → RTH.
    #   • qwen3.5 seat runs OFF-HOURS (21:30 AZ, queue idle) → never evicts
    #     the 7 live qwen3:8b agents.
    def _run_shadow_csp_plutus_safe():
        try:
            from engine.shadow_csp import run_shadow_csp_plutus
            return run_shadow_csp_plutus()
        except Exception as _e:
            console.log(f"[red][SHADOW-CSP] plutus seat crash: {type(_e).__name__}: {_e!r}")
            return None

    def _run_shadow_csp_qwen35_safe():
        try:
            from engine.shadow_csp import run_shadow_csp_qwen35
            return run_shadow_csp_qwen35()
        except Exception as _e:
            console.log(f"[red][SHADOW-CSP] qwen35 seat crash: {type(_e).__name__}: {_e!r}")
            return None
    schedule.every(2).hours.do(_run_shadow_csp_plutus_safe)        # shadow CSP A: shares McCoy plutus-v1, RTH-gated, ghost-only
    schedule.every().day.at("21:30").do(_run_shadow_csp_qwen35_safe)  # shadow CSP B: qwen3.5 off-hours batch, ghost-only (model id from .env)
    # === /HM-SHADOW-CSP ===

    # Self-Improvement Loop — 2:30 PM AZ (4:30 PM ET), generates 3 rules per agent
    try:
        from engine.self_improvement import run_daily_reflection
        schedule.every(30).minutes.do(run_daily_reflection)  # gate fires once at 2:30 PM AZ
    except Exception as _si_err:
        console.log(f"[yellow]Self-Improvement scheduler skip: {_si_err}")

    # ── Phase 2 automated systems ──────────────────────────────────────────────

    def _run_agent_watchdog():
        from engine.risk_manager import RiskManager
        """Check all user-defined trading agents against current market conditions."""
        if not RiskManager.is_market_hours():
            return
        try:
            from engine.agent_builder import check_user_agents
            from engine.market_data import get_stock_price
            from engine.universe import get_active_universe
            ctx = {"prices": {s: get_stock_price(s) for s in get_active_universe()[:5]}}
            fired = check_user_agents(ctx)
            if fired:
                console.log(f"[cyan][STARTUP] Agent Watchdog: {fired} agent(s) triggered")
        except Exception as _e:
            logger.debug(f"Agent watchdog error: {_e}")

    def _run_cash_sweep_check():
        from engine.risk_manager import RiskManager
        """Evaluate cash sweep rules (SWEEP UP/DOWN) every 30 min during market hours."""
        if not RiskManager.is_market_hours():
            return
        try:
            from engine.cash_manager import run_sweep
            result = run_sweep(dry_run=False)
            if result.get("triggered"):
                console.log(f"[cyan][CashManager] Sweep {result['direction']}: {result['result']}")
        except Exception as _e:
            logger.debug(f"Cash sweep error: {_e}")

    def _run_tax_harvest_scan():
        """Daily tax-loss harvest scan — runs at market close window (3:30-4:00 PM ET)."""
        import pytz as _pz
        from datetime import datetime as _dt_loc
        _et = _dt_loc.now(_pz.timezone("America/New_York"))
        # Only run 3:30–4:00 PM ET on weekdays
        if _et.weekday() >= 5:
            return
        if not (15 <= _et.hour < 16 and _et.minute >= 30):
            return
        try:
            from engine.tax_harvester import scan_opportunities
            scan = scan_opportunities()
            if scan.get("harvestable_count", 0) > 0:
                console.log(
                    f"[cyan][TaxHarvester] {scan['harvestable_count']} harvestable position(s)"
                    f" — est. saving ~${scan.get('estimated_tax_saving',0):.0f}"
                    f" | mode={scan.get('mode','ALERT')}"
                )
        except Exception as _e:
            logger.debug(f"Tax harvest scan error: {_e}")

    def _run_drift_check():
        from engine.risk_manager import RiskManager
        """Check portfolio drift against target weights every 30 min during market hours."""
        if not RiskManager.is_market_hours():
            return
        try:
            from engine.drift_rebalancer import drift_report
            report = drift_report()
            drifting = sum(sp.get("drifting_count", 0) for sp in report.get("sub_portfolios", []))
            if drifting:
                max_d = max((sp.get("max_drift", 0) for sp in report.get("sub_portfolios", [])), default=0)
                console.log(f"[yellow][DriftRebalancer] {drifting} position(s) drifting (max {max_d:.1f}%)")
        except Exception as _e:
            logger.debug(f"Drift check error: {_e}")

    def _run_var_calculation():
        """Daily VaR snapshot — runs once at market close (4:05–4:15 PM ET)."""
        import pytz as _pz
        from datetime import datetime as _dt_loc
        _et = _dt_loc.now(_pz.timezone("America/New_York"))
        if _et.weekday() >= 5:
            return
        if not (16 <= _et.hour < 17 and _et.minute <= 15):
            return
        try:
            from engine.risk_var import calculate_var
            v = calculate_var()
            if "error" not in v:
                console.log(
                    f"[cyan][VaR] 95%=${v.get('var_95_param',0):.0f}"
                    f" 99%=${v.get('var_99_param',0):.0f}"
                    f" vol={v.get('daily_vol_pct',0):.2f}%"
                    f" gauge={v.get('risk_gauge','?').upper()}"
                )
        except Exception as _e:
            logger.debug(f"VaR calculation error: {_e}")

    schedule.every(15).minutes.do(_run_agent_watchdog)        # User agent conditions checked every 5 min (market hours)
    schedule.every(30).minutes.do(_run_cash_sweep_check)     # Cash sweep rules every 30 min (market hours)
    schedule.every(15).minutes.do(_run_tax_harvest_scan)     # Tax harvest scan every 15 min (fires only at 3:30-4 PM ET)
    schedule.every(30).minutes.do(_run_drift_check)          # Drift rebalancer every 30 min (market hours)
    schedule.every(15).minutes.do(_run_var_calculation)      # VaR snapshot every 15 min (fires only at 4:05-4:15 PM ET)

    # Webull Portfolio Auto-Sync: every 5 min during extended + regular hours
    # 2026-04-23: Webull account liquidated, migrated to Schwab.
    # Sync disabled to stop phantom "Captain's Portfolio" alerts.
    # Re-enable when steve-schwab sync is wired. (XO)
    # def run_webull_sync():
    #     """Auto-sync Steve's Webull portfolio positions to DB."""
    #     if not is_extended_or_market_hours():
    #         return
    #     try:
    #         from engine.webull_client import sync_positions_to_db
    #         sync_positions_to_db()
    #     except Exception as e:
    #         console.log(f"[red]Webull auto-sync error: {e}")
    # schedule.every(15).minutes.do(run_webull_sync)

    # Alpaca Portfolio Sync — tiered schedule (2min market / 10min pre-post / 60min after / 6hr weekend)
    def run_alpaca_portfolio_sync():
        """Full Alpaca account sync: cash, positions, portfolio value — tiered by market session."""
        try:
            from shared.alpaca_portfolio_sync import run_full_alpaca_sync
            result = run_full_alpaca_sync()
            if result.get("skipped"):
                return
            if result.get("ok"):
                console.log(
                    f"[dim cyan][SYNC] Portfolio: ${result['portfolio_value']:,.2f}"
                    f" | Cash: ${result['cash']:,.2f}"
                    f" | {result['positions']} positions"
                    f" | {result.get('synced_label','')}"
                    f"[/dim cyan]"
                )
            else:
                logger.debug(f"Alpaca portfolio sync failed: {result.get('error')}")
        except Exception as e:
            logger.debug(f"Alpaca portfolio sync error: {e}")

    schedule.every(5).minutes.do(run_alpaca_portfolio_sync)   # Runs every minute; interval gating inside

    # Q's daily quote: 6 AM MST weekdays
    def run_q_daily_quote():
        """Q's daily market observation — 6 AM MST weekdays."""
        from datetime import datetime as _dt
        import pytz
        az = pytz.timezone("US/Arizona")
        now = az_now()
        if now.weekday() >= 5 or now.hour != 6:
            return
        try:
            from engine.q_daily import generate_q_daily_quote
            generate_q_daily_quote()
        except Exception as e:
            console.log(f"[red]Q daily quote error: {e}")

    schedule.every(30).minutes.do(run_q_daily_quote)          # Q daily quote: 6 AM MST weekdays

    # UOA pre-market scan: 6 AM MST weekdays (top 50 stocks, ~2 min)
    _uoa_premarket_done = False
    def run_uoa_premarket():
        global _uoa_premarket_done
        from datetime import datetime as _dt
        import pytz
        az = pytz.timezone("US/Arizona")
        now = az_now()
        today = now.strftime('%Y-%m-%d')
        # Reset flag at midnight
        if now.hour == 0:
            _uoa_premarket_done = False
        if now.weekday() >= 5 or now.hour != 6 or _uoa_premarket_done:
            return
        _uoa_premarket_done = True
        try:
            from uoa.scheduler import run_premarket
            run_premarket()
            console.log("[bold cyan]UOA pre-market scan complete")
        except Exception as e:
            console.log(f"[red]UOA pre-market scan error: {e}")

    schedule.every(15).minutes.do(run_uoa_premarket)         # UOA pre-market: 6 AM MST weekdays

    # Riker XO: synthesize after each CTO briefing cycle.
    # HM-RIKER-AH-SYNTHESIS 2026-05-22: Riker is advisory, not execution —
    # Admiral Archer: frontier scanner (Sunday 10:30 PM MST)
    def run_archer_frontier():
        """Admiral Archer: weekend forward-looking briefing, Sunday 10:30 PM MST.

        HM-ARCHER-REBUILD: routes to plutus-v1 (engine.archer.brain) instead of
        the archived engine.archer_frontier module. Idempotent per Sunday via a
        recency guard on archer_briefings. No bare except:pass — the timezone
        guard now logs instead of silently swallowing.
        """
        try:
            now = az_now()
        except Exception as _tz_e:
            console.log(f"[red]Archer frontier tz error: {type(_tz_e).__name__}: {_tz_e!r}")
            return
        # Sunday (weekday 6) between 10:30-11:00 PM MST
        if now.weekday() != 6 or now.hour != 22 or now.minute < 30:
            return
        try:
            import sqlite3 as _sql
            import requests as _rq
            from engine.archer.brain import morning_briefing as _archer_brief
            # Recency guard: skip if a briefing was already stored in the last 6h.
            _c = _sql.connect("data/trader.db")
            _c.execute("PRAGMA busy_timeout=30000")  # HM-DRYDOCK EPIC1: explicit (belt-and-braces over main.py:74 patch)
            _c.execute(
                "CREATE TABLE IF NOT EXISTS archer_briefings ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, briefing TEXT NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            _recent = _c.execute(
                "SELECT 1 FROM archer_briefings WHERE created_at >= datetime('now','-6 hours') LIMIT 1"
            ).fetchone()
            if _recent:
                _c.close()
                return
            briefing = (_archer_brief() or "").strip()
            if not briefing:
                console.log("[yellow]Archer frontier: plutus returned empty — skipping")
                _c.close()
                return
            _c.execute("INSERT INTO archer_briefings (briefing) VALUES (?)", (briefing,))
            _c.commit()
            _c.close()
            try:
                _rq.post(
                    "https://ntfy.sh/ollietrades-admin",
                    data=briefing.encode("utf-8"),
                    headers={"Title": "Captain Archer -- Frontier Briefing",
                             "Priority": "default"},
                    timeout=10,
                )
            except Exception as _ne:
                console.log(f"[yellow]Archer frontier ntfy failed: {type(_ne).__name__}: {_ne!r}")
            console.log(f"[cyan]Captain Archer frontier briefing generated ({len(briefing)} chars, plutus-v1)")
        except Exception as e:
            console.log(f"[red]Archer frontier error: {type(e).__name__}: {e!r}")

    schedule.every(30).minutes.do(run_archer_frontier)       # Archer: Sunday 10:30 PM MST frontier scan

    # Debate pipeline: weekdays at 5 PM MST (after market close) — refreshes trade cards
    _debate_pipeline_done_today: list[str] = []
    def run_debate_pipeline():
        """Run full debate pipeline after market close. Refreshes debate_history_v2 / trade cards."""
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = az_now()
        today = now.strftime("%Y-%m-%d")
        if now.weekday() >= 5:
            return  # skip weekends
        if now.hour != 17 or now.minute > 30:
            return  # only fire in the 17:00–17:30 window
        if today in _debate_pipeline_done_today:
            return  # once per day
        _debate_pipeline_done_today.clear()
        _debate_pipeline_done_today.append(today)
        try:
            from engine.pipeline import run_pipeline
            console.log("[bold cyan]Debate pipeline: starting post-close run...")
            result = run_pipeline(top_n=10, execute=False)
            n = len(result.get("debates", {}))
            console.log(f"[bold green]Debate pipeline complete: {n} tickers debated")
        except Exception as e:
            console.log(f"[red]Debate pipeline error: {e}")

    schedule.every(15).minutes.do(run_debate_pipeline)       # Debate pipeline: weekdays 5 PM MST post-close

    # Season rotation: every Sunday at 11:59 PM MST
    def run_season_rotation():
        """Auto-rotate season every Sunday at 11:59 PM MST."""
        from datetime import datetime as _dt
        import pytz
        az = pytz.timezone("US/Arizona")
        now = az_now()
        # Sunday (weekday 6) at 11:59 PM MST
        if now.weekday() != 6 or now.hour != 23 or now.minute < 55:
            return
        try:
            from engine.season_manager import rotate_season
            new = rotate_season()
            console.log(f"[bold green]⭐ Season auto-rotation complete → Season {new}")
        except Exception as e:
            console.log(f"[red]Season rotation error: {e}")

    schedule.every(30).minutes.do(run_season_rotation)        # Season rotation: checks every 5 min, fires Sunday 11:59 PM MST

    # Trade Memory Loop: backfill closed trade outcomes every 5 minutes (no market hours gate)
    def run_trade_outcomes_backfill():
        """Scan for newly closed trades and record outcomes for memory injection."""
        try:
            from engine.trade_outcomes import auto_record_closed_trades
            auto_record_closed_trades()
        except Exception as e:
            console.log(f"[yellow]Trade outcomes backfill error: {e}")

    schedule.every(15).minutes.do(run_trade_outcomes_backfill)  # Trade Memory: backfill every 5 min, no gate

    # === DATA INGESTION SCHEDULER (Module 8) ===

    # Market history daily backfill — 5 PM MST (8 PM ET, after market close)
    def run_market_history_backfill():
        """Daily market history refresh. Fires once at 5 PM MST weekdays."""
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = az_now()
        if now.weekday() >= 5:
            return  # Skip weekends
        if not (17 <= now.hour < 18):
            return  # Only fire during 5:00–5:59 PM MST window
        try:
            from engine.data_ingestion import backfill_market_history
            console.log("[cyan]Data Ingestion: daily market history refresh...")
            r = backfill_market_history(days=30)  # Only last 30 days for daily refresh
            console.log(f"[green]Market history daily: {r['bars_inserted']} new bars")
        except Exception as e:
            console.log(f"[yellow]Market history daily refresh error: {e}")

    schedule.every(30).minutes.do(run_market_history_backfill)  # Data Ingestion: daily 5 PM MST

    # Insider trades — 5:30 PM MST (8:30 PM ET) weekdays
    def run_insider_fetch():
        """Fetch SEC Form 4 insider filings. Fires once at 5:30 PM MST weekdays."""
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = az_now()
        if now.weekday() >= 5:
            return
        if not (17 <= now.hour < 18 and now.minute >= 30):
            return
        try:
            from engine.data_ingestion import fetch_insider_trades
            console.log("[cyan]Data Ingestion: fetching SEC insider filings...")
            r = fetch_insider_trades(days_back=3)
            console.log(f"[green]Insider trades: {r['inserted']} new filings")
        except Exception as e:
            console.log(f"[yellow]Insider fetch error: {e}")

    schedule.every(30).minutes.do(run_insider_fetch)  # Data Ingestion: daily 5:30 PM MST

    # Pattern matching — every 15 minutes during market hours
    def run_pattern_match():
        """Detect active market patterns. Every 15 min during market hours."""
        try:
            from engine.risk_manager import RiskManager
            if not RiskManager.is_market_hours():
                return
            from engine.data_ingestion import match_current_patterns
            results = match_current_patterns()
            if results:
                names = [p["pattern"] for p in results[:5]]
                console.log(f"[cyan]Patterns detected: {', '.join(names)}")
        except Exception as e:
            console.log(f"[yellow]Pattern match error: {e}")

    schedule.every(15).minutes.do(run_pattern_match)  # Data Ingestion: pattern scan every 15 min

    # Strategy Lab: auto-optimize every Sunday at midnight
    def run_strategy_lab_auto():
        """Run full auto-optimization pipeline. Scheduled Sundays at midnight."""
        from datetime import datetime as _dt
        now = _dt.now()
        # Only run on Sunday (weekday 6) between midnight and 1 AM
        if now.weekday() != 6 or now.hour >= 1:
            return
        try:
            from engine.strategy_lab import auto_optimize_all
            console.log("[bold cyan]Strategy Lab: Starting weekly auto-optimization...")
            report = auto_optimize_all()
            proposed = report.get("proposed", [])
            best = report.get("best_strategy", {})
            if proposed:
                console.log(f"[bold yellow]Strategy Lab: {len(proposed)} proposal(s) for Admiral review "
                            f"(NOT applied). Best: {best.get('strategy_name', 'N/A')} "
                            f"(PF={best.get('avg_profit_factor', 0):.2f})")
            else:
                console.log(f"[green]Strategy Lab: Complete. Best: {best.get('strategy_name', 'N/A')} "
                            f"(PF={best.get('avg_profit_factor', 0):.2f}). No proposals.")
        except Exception as e:
            console.log(f"[red]Strategy Lab auto-optimize error: {e}")

    schedule.every(30).minutes.do(run_strategy_lab_auto)   # Checks every 30 min, runs Sunday midnight only

    # CrewAI Strategy Crew: every Sunday at 10:30 PM MST
    # (after Picard's weekly briefing at 10 PM, before Strategy Lab at midnight)
    def run_crew_strategy():
        """Run CrewAI strategy generation crew. Scheduled Sundays 10:30 PM."""
        from datetime import datetime as _dt
        now = _dt.now()
        # Only run on Sunday (weekday 6) between 10:30 and 11:00 PM
        if now.weekday() != 6 or now.hour != 22 or now.minute < 30:
            return
        try:
            from engine.crew.strategy_crew import run_strategy_crew
            console.log("[bold cyan]CrewAI: Launching weekly Strategy Crew...")
            result = run_strategy_crew()
            console.log(f"[bold green]CrewAI: Strategy Crew complete ({len(str(result))} chars)")
        except Exception as e:
            console.log(f"[red]CrewAI Strategy Crew error: {e}")

    schedule.every(30).minutes.do(run_crew_strategy)  # Checks every 30 min, runs Sunday 10:30 PM only

    # Daily Post-Market Review: Mon-Fri at 1:15 PM MST (4:15 PM ET)
    def run_daily_review():
        """Daily post-market review crew — grades trades, finds patterns, writes adjustments."""
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = az_now()
        # Mon-Fri only, 1:15 PM MST (fire within 10-min window)
        if now.weekday() >= 5:
            return
        if now.hour != 13 or now.minute < 15 or now.minute > 25:
            return
        try:
            from engine.crew.daily_review_crew import run_daily_review as _run
            console.log("[bold cyan]Daily Review Crew: launching post-market review...")
            result = _run()
            console.log(f"[bold green]Daily Review complete: {result.get('trades_graded', 0)} graded, "
                        f"{result.get('adjustments_saved', 0)} adjustments")
        except Exception as e:
            console.log(f"[red]Daily Review error: {e}")

    schedule.every(30).minutes.do(run_daily_review)  # Checks every 5 min, fires 1:15 PM MST Mon-Fri

    # Reference Data Import: Sunday at 8:00 PM MST (before Weekly Tuning at 9 PM)
    def run_reference_import():
        """Import reference data from external arenas. Sundays 8 PM MST."""
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = az_now()
        if now.weekday() != 6 or now.hour != 20 or now.minute > 30:
            return
        try:
            from engine.importers.ai4trade_importer import import_signals
            console.log("[bold cyan]Reference Import: pulling ai4trade.ai signals...")
            result = import_signals(200)
            console.log(f"[bold green]Reference Import: {result.get('imported', 0)} signals, "
                        f"{result.get('discussions', 0)} discussions imported")
        except Exception as e:
            console.log(f"[red]Reference Import error: {e}")

    schedule.every(30).minutes.do(run_reference_import)  # Checks every 30 min, fires Sunday 8 PM

    # Weekly Model Tuning: Sunday at 9:00 PM MST
    def run_weekly_tuning():
        """Weekly model tuning — scores fleet, promotes/demotes, tunes prompts."""
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = az_now()
        if now.weekday() != 6 or now.hour != 21 or now.minute > 30:
            return
        try:
            from engine.crew.weekly_tuning_crew import run_weekly_tuning as _run
            console.log("[bold cyan]Weekly Tuning Crew: launching fleet review...")
            result = _run()
            console.log(f"[bold green]Weekly Tuning complete: {result.get('models_scored', 0)} scored, "
                        f"{result.get('adjustments_saved', 0)} adjustments")
        except Exception as e:
            console.log(f"[red]Weekly Tuning error: {e}")

    schedule.every(30).minutes.do(run_weekly_tuning)  # Checks every 30 min, fires Sunday 9 PM only

    # Aladdin: BlackRock intelligence brief — every 4 hours
    _aladdin_state = {"last_run": 0.0}
    def run_aladdin_brief():
        now_ts = time.time()
        if now_ts - _aladdin_state["last_run"] < 14400:  # 4-hour minimum
            return
        _aladdin_state["last_run"] = now_ts
        try:
            from agents.aladdin import get_aladdin_brief
            brief = get_aladdin_brief(force=True)
            console.log(
                f"[bold cyan]Aladdin brief: {brief['macro_signal']} "
                f"(confidence={brief['confidence']}, "
                f"congress_flags={len(brief.get('congress_flags', []))})"
            )
        except Exception as e:
            console.log(f"[red]Aladdin brief error: {e}")

    schedule.every(30).minutes.do(run_aladdin_brief)   # Aladdin: checks every 30 min, runs every 4 hours

    # HM-T-retire 2026-05-04: post_earnings_drift retired (Verdict B silently inert).
    # Module archived to archive/retired/2026-05-04-post-earnings-drift/.
    # See docs/HM-T_PED_OPERATIONAL_PROBE_2026-05-04.md for forensic record.
    # Side effect: closes HM-S-code (phantom agent_state reference removed from active paths).

    # ── Elder Council (S6.3, 2026-04-16): Sarek / Janeway / Surak ────────────
    # Patient long-horizon agents. Scheduler fires daily at 05:30-05:45 AZ, but
    # each handler gates itself by calendar date so DCAs only run on their cadence.
    _elder_state = {"sarek_mo": "", "janeway_q": "", "surak_yr": ""}

    def _az_today():
        from datetime import datetime, timezone, timedelta
        return (datetime.now(timezone.utc) + timedelta(hours=-7))

    def run_sarek_monthly():
        """Sarek (5yr): DCA on 1st of each month."""
        today = _az_today()
        if today.day != 1:
            return
        ym = today.strftime("%Y-%m")
        if _elder_state["sarek_mo"] == ym:
            return  # already ran this month
        try:
            from agents.sarek import run_monthly_dca
            rows = run_monthly_dca(500.0)
            _elder_state["sarek_mo"] = ym
            console.log(f"[bold cyan]Sarek monthly DCA: {len(rows)} tranches @ $500 total")
        except Exception as e:
            console.log(f"[red]Sarek DCA error: {e}")

    def run_janeway_quarterly():
        """Janeway (10yr): DCA on 1st of Jan/Apr/Jul/Oct."""
        today = _az_today()
        if today.day != 1 or today.month not in (1, 4, 7, 10):
            return
        q_key = f"{today.year}Q{(today.month - 1) // 3 + 1}"
        if _elder_state["janeway_q"] == q_key:
            return
        try:
            from agents.janeway import run_quarterly_dca
            rows = run_quarterly_dca(1000.0)
            _elder_state["janeway_q"] = q_key
            console.log(f"[bold cyan]Janeway quarterly DCA: {len(rows)} tranches @ $1000 total")
        except Exception as e:
            console.log(f"[red]Janeway DCA error: {e}")

    def run_surak_annual():
        """Surak (20yr): DCA on Jan 1."""
        today = _az_today()
        if today.day != 1 or today.month != 1:
            return
        yr_key = str(today.year)
        if _elder_state["surak_yr"] == yr_key:
            return
        try:
            from agents.surak import run_annual_dca
            rows = run_annual_dca(2000.0)
            _elder_state["surak_yr"] = yr_key
            console.log(f"[bold cyan]Surak annual DCA: {len(rows)} tranches @ $2000 total")
        except Exception as e:
            console.log(f"[red]Surak DCA error: {e}")

    # Brief refreshers (cheap — just regenerates thesis cache; DCA only fires on cadence)
    def run_elder_briefs():
        try:
            from agents.sarek   import get_sarek_brief
            from agents.janeway import get_janeway_brief
            from agents.surak   import get_surak_brief
            get_sarek_brief()      # 24h cache
            get_janeway_brief()    # 7d  cache
            get_surak_brief()      # 30d cache
        except Exception as e:
            console.log(f"[yellow]Elder Council brief refresh: {e}")

    schedule.every().day.at("05:30").do(run_sarek_monthly)     # 8:30 AM ET
    schedule.every().day.at("05:35").do(run_janeway_quarterly) # 8:35 AM ET
    schedule.every().day.at("05:40").do(run_surak_annual)      # 8:40 AM ET
    schedule.every().day.at("05:45").do(run_elder_briefs)      # thesis-cache warm

    # ── Phase 4: Event Shield, News Pulse, Breadth/Sector/Correlation ─────────
    def run_event_shield():
        try:
            from engine.event_shield import run_event_shield_daily
            run_event_shield_daily()
        except Exception as e:
            console.log(f"[yellow]Event Shield refresh error: {e}")
    schedule.every().day.at("03:00").do(run_event_shield)   # 7:00 AM ET

    # ── Dilithium Crystal Alpha Signals: 7:00 AM AZ (before market open) ────
    def run_alpha_signals():
        try:
            from engine.alpha_signals import run_alpha_signals_job
            run_alpha_signals_job()
        except Exception as e:
            console.log(f"[yellow]Alpha signals error: {e}")
    schedule.every().day.at("07:00").do(run_alpha_signals)   # 7:00 AM AZ = 10:00 AM ET

    def run_news_pulse():
        try:
            from engine.news_pulse import run_news_pulse_morning
            run_news_pulse_morning()
        except Exception as e:
            console.log(f"[yellow]News Pulse error: {e}")
    schedule.every().day.at("03:30").do(run_news_pulse)     # 7:30 AM ET

    def run_breadth_sector_corr():
        try:
            from engine.breadth_scanner import get_breadth_snapshot
            from engine.sector_heatmap import get_sector_heatmap
            from engine.correlation_monitor import get_correlations
            get_breadth_snapshot(force=True)
            get_sector_heatmap(force=True)
            get_correlations(force=True)
        except Exception as e:
            console.log(f"[yellow]Breadth/sector/correlation error: {e}")
    schedule.every(15).minutes.do(run_breadth_sector_corr)

    def run_holodeck_weekly():
        try:
            from engine.holodeck_readyroom import run_holodeck_weekly as _holo
            _holo()
        except Exception as e:
            console.log(f"[yellow]Holodeck weekly error: {e}")
    schedule.every().sunday.at("10:00").do(run_holodeck_weekly)

    # ── Holly Nightly Backtest — 3 AM ET (midnight AZ in EDT, 1 AM AZ in EST) ──

    def run_holly_nightly_job():
        """Holly-style nightly backtest: top 50 volume movers × 4 strategies.
        Fires at midnight AZ (3 AM ET EDT) on weeknights.
        Saves top 10 winning strategies for morning scan prioritization.
        """
        global _holly_nightly_done
        import pytz as _pz
        _az = az_now()  # HM-TZ Stage 3: zoneinfo (corruption-proof) vs pytz singleton

        # Reset flag each day at noon so it can fire again the next night
        if _az.hour == 12:
            _holly_nightly_done = False
            return

        # Weekdays only (Mon–Fri), midnight AZ window (0:00–0:45)
        if _az.weekday() >= 5:
            return
        if _az.hour != 0 or _holly_nightly_done:
            return

        _holly_nightly_done = True
        try:
            from engine.holly_nightly_backtest import run_holly_nightly
            console.log("[bold magenta]🔬 Holly Nightly Backtest starting (3 AM ET)...")
            result = run_holly_nightly()
            n = len(result.get("top_10", []))
            top = result.get("top_10", [{}])[0] if result.get("top_10") else {}
            console.log(
                f"[bold magenta]🔬 Holly Nightly complete: {result.get('total_runs', 0)} runs, "
                f"top={top.get('ticker','?')} {top.get('strategy','?')} "
                f"{top.get('total_return', 0):+.2f}% | {n} winners saved"
            )
            try:
                from engine.war_room import save_hot_take
                top_names = ", ".join(
                    f"{w['ticker']}({w['strategy']})" for w in result.get("top_10", [])[:5]
                )
                save_hot_take("HOLLY", "nightly_backtest",
                              f"Top strategies tonight: {top_names} — SPY={result.get('spy_return', 0):+.2f}%")
            except Exception:
                pass
        except Exception as _he:
            console.log(f"[red]Holly Nightly Backtest error: {_he}")

    # HM-HOLLY-REPAIR 2026-05-30: in-process Holly nightly DISABLED. Two reasons:
    # (1) it dies on every trader restart (the 8-day stall, last good 2026-05-22), and
    # (2) it runs under the live .venv which lacks vectorbt → would now fail-loud + NTFY
    # every midnight = alarm noise. Authoritative path is now cron under .venv-backtest:
    #   scripts/holly_nightly_cron.sh  (crontab: 0 0 * * 1-5)
    # which survives restarts AND has the vectorbt engine. Re-enable this line only if
    # vectorbt is ever installed into the live .venv.
    # schedule.every(15).minutes.do(run_holly_nightly_job)   # DISABLED — see cron wrapper

    def run_adaptive_tuner():
        try:
            from engine.adaptive_tuner import run_adaptive_tuner_weekly
            run_adaptive_tuner_weekly()
        except Exception as e:
            console.log(f"[yellow]Adaptive Tuner error: {e}")
    schedule.every().sunday.at("11:00").do(run_adaptive_tuner)

    # ── Phase 4: Deep Scan & Strategy Rotation ─────────────────────────────────
    def run_deep_scan_job():
        """Chekov's Deep Space Scan — 8:00 AM ET (05:00 AZ) on market days."""
        from datetime import datetime as _dt
        _h = _dt.now().hour
        # Only run Mon-Fri between 5:00-5:30 AM AZ
        if _h != 5:
            return
        try:
            from engine.deep_scan import run_deep_scan
            console.log("[cyan]🔭 Chekov's Deep Space Scan starting...")
            result = run_deep_scan()
            console.log(f"[cyan]🔭 Deep Scan complete: {result.get('signals_found', 0)} signals in {result.get('symbols_scanned', 0)} symbols")
        except Exception as _e:
            console.log(f"[red]Deep Scan error: {_e}")

    def run_strategy_rotation_job():
        """Holodeck Nightly Simulation — 5:30 PM ET (14:30 AZ) after market close."""
        from datetime import datetime as _dt
        _h, _m = _dt.now().hour, _dt.now().minute
        # Only run 14:30-15:00 AZ
        if not (_h == 14 and _m >= 30):
            return
        try:
            from engine.strategy_rotator import run_strategy_rotation
            console.log("[purple]🖥 Holodeck Nightly Simulation starting...")
            result = run_strategy_rotation()
            active = result.get('active_strategies', 0)
            console.log(f"[purple]🖥 Strategy Rotation complete: {active} active strategies identified")
        except Exception as _e:
            console.log(f"[red]Strategy Rotation error: {_e}")

    def run_universe_refresh_job():
        """Universe refresh — Sunday 11:30 PM AZ."""
        try:
            from engine.deep_scan import build_universe
            console.log("[cyan]🔭 Universe refresh starting (weekly)...")
            result = build_universe(force=True)
            console.log(f"[cyan]🔭 Universe refreshed: {result.get('universe_size', 0)} stocks")
        except Exception as _e:
            console.log(f"[red]Universe refresh error: {_e}")

    def run_signal_bridge_job():
        """HM-BRINGBACK 2026-05-31: bridge W0-edge deep_scan_results -> trade_signals
        as SHADOW signals (observation-only, W0 forward-scoring). In-process => inherits
        the trader's @reboot survivability (NOT a launchd agent). Runs during RTH; the
        bridge dedupes per (symbol,setup,scan_date) so frequent runs are idempotent.
        Observation-only — emitted rows are agent='shadow-bridge:*', excluded from execution."""
        try:
            from engine.market_calendar import is_us_market_open
            if not is_us_market_open():
                return
            from engine.signal_bridge import run_signal_bridge, run_flow_bridge
            p = run_signal_bridge()
            f = run_flow_bridge()
            if (p.get("emitted") or 0) or (f.get("emitted") or 0):
                console.log(f"[cyan][SHADOW-BRIDGE] emitted primary={p.get('emitted')} "
                            f"flow={f.get('emitted')} by_setup={p.get('by_setup')} (observation-only)")
        except Exception as _e:
            console.log(f"[red]Signal bridge error: {type(_e).__name__}: {_e}")

    schedule.every(30).minutes.do(run_deep_scan_job)        # deep scan at 8AM ET
    schedule.every(30).minutes.do(run_signal_bridge_job)    # HM-BRINGBACK: shadow bridge (RTH, observation-only)
    schedule.every(30).minutes.do(run_strategy_rotation_job) # rotation at 5:30PM ET
    schedule.every().sunday.at("20:30").do(run_universe_refresh_job)  # Sunday 11:30PM AZ

    # ── Season 6: Proving Ground — 30-Day Sniper Mode Trial ────────────────────
    def run_proving_ground_scorecard():
        """Daily scorecard at 1:15 PM AZ (4:15 PM ET) — market close."""
        import pytz as _pz
        _az = az_now()  # HM-TZ Stage 3: zoneinfo (corruption-proof) vs pytz singleton
        if _az.weekday() >= 5:
            return  # weekdays only
        try:
            from engine.proving_ground import run_daily_scorecard
            result = run_daily_scorecard()
            console.log(
                f"[bold cyan]📊 Proving Ground rolling 30d window, Day {result['trial_day']} | "
                f"Trades: {result['total_trades']} | WR: {result['win_rate']:.1f}% | "
                f"Sharpe: {result['sharpe']:.3f} | Go: {result['go_count']}/6"
            )
        except Exception as _e:
            console.log(f"[yellow]Proving Ground scorecard error: {_e}")

    def run_proving_ground_report():
        """Daily ntfy push at 1:30 PM AZ (4:30 PM ET)."""
        import pytz as _pz
        _az = az_now()  # HM-TZ Stage 3: zoneinfo (corruption-proof) vs pytz singleton
        if _az.weekday() >= 5:
            return
        try:
            from engine.proving_ground import send_daily_ntfy_report
            send_daily_ntfy_report()
        except Exception as _e:
            console.log(f"[yellow]Proving Ground ntfy report error: {_e}")

    def run_proving_ground_weekly():
        """Sunday backtest vs actual comparison."""
        try:
            from engine.proving_ground import send_weekly_comparison
            send_weekly_comparison()
        except Exception as _e:
            console.log(f"[yellow]Proving Ground weekly report error: {_e}")

    def run_proving_ground_evaluator():
        """HM-PROVING-GROUND-FORMALIZE-V2 SUB-2 2026-05-25 — daily state
        machine evaluator. Runs at 13:18 AZ (3 min after the scorecard
        write, 12 min before the ntfy report) so state transitions are
        visible in the daily summary that follows."""
        import pytz as _pz
        _az = az_now()  # HM-TZ Stage 3: zoneinfo (corruption-proof) vs pytz singleton
        if _az.weekday() >= 5:
            return
        try:
            from engine.proving_ground import ship_kill_evaluator
            result = ship_kill_evaluator()
            console.log(
                f"[bold cyan]🎯 Proving Ground evaluator: "
                f"day={result['trial_day']} state={result['target_state']} "
                f"transitioned={result['transitioned']}"
            )
        except Exception as _e:
            console.log(f"[yellow]Proving Ground evaluator error: {_e}")

    schedule.every().day.at("13:15").do(run_proving_ground_scorecard)   # 4:15 PM ET
    schedule.every().day.at("13:18").do(run_proving_ground_evaluator)   # 4:18 PM ET (HM-PROVING-GROUND-FORMALIZE-V2 SUB-2)
    schedule.every().day.at("13:30").do(run_proving_ground_report)      # 4:30 PM ET
    schedule.every().sunday.at("12:00").do(run_proving_ground_weekly)   # Sunday 3 PM ET

    # ── Rallies.ai hourly scraper (market hours Mon-Fri) ───────────────────────
    def run_rallies_scraper_job():
        """Hourly Rallies.ai scrape during market hours."""
        import pytz as _pz
        _az = az_now()  # HM-TZ Stage 3: zoneinfo (corruption-proof) vs pytz singleton
        if _az.weekday() >= 5:
            return
        # Market hours: 6:30 AM – 1:00 PM AZ
        if not (6 <= _az.hour < 13):
            return
        try:
            from engine.rallies_scraper import run_once as run_rallies_scrape
            run_rallies_scrape()
        except Exception as _e:
            console.log(f"[yellow]Rallies scraper error: {_e}")

    schedule.every(1).hours.do(run_rallies_scraper_job)    # Rallies.ai: hourly market hours

    # ── ai4trade.ai nightly import ──
    def run_ai4trade():
        try:
            from dotenv import load_dotenv; load_dotenv()
            from engine.importers.ai4trade_importer import run_import as _run
            r = _run()
            print(f"[ai4trade] {r}")
        except Exception as e:
            print(f"[ai4trade] error: {e}")
    schedule.every().day.at("20:30").do(run_ai4trade)

    # ── Season 6 Opening Bell — April 10, 2026 9:30 AM ET (6:30 AM AZ) ────────
    _s6_bell_state = [False]  # mutable container to avoid nonlocal

    def run_season6_opening_bell():
        if _s6_bell_state[0]:
            return
        import pytz as _pz
        from datetime import date as _date
        _az = az_now()  # HM-TZ Stage 3: zoneinfo (corruption-proof) vs pytz singleton
        if _az.date() != _date(2026, 4, 10):
            return
        if not (6 <= _az.hour < 7):
            return
        _s6_bell_state[0] = True
        try:
            from engine.ntfy import _fire, P_MAX
            _fire(
                title="SEASON 6: SNIPER MODE",
                body=(
                    "Fleet is live. 6 agents, 3 strategies, 12 Dilithium Crystals.\n"
                    "Proving Ground active (30-day trial).\n"
                    "Ollie Commander online. Make it so."
                ),
                priority=P_MAX,
                tags="vulcan_salute"
            )
            console.log("[bold green]🖖 Season 6: Sniper Mode — Opening Bell fired")
        except Exception as _e:
            console.log(f"[yellow]Season 6 opening bell error: {_e}")

    schedule.every(15).minutes.do(run_season6_opening_bell)   # check window every 5 min

    _stagger_schedule_jobs()

    # === HM-WR-CYCLE-RCA-PHASE2 2026-05-20 ===
    # Diagnostic dump of all registered jobs + their next_run values post-stagger.
    # Spec: project_hm_wr_cycle_rca.md Phase 2.
    try:
        console.log(f"[WR-DEBUG-INIT] schedule.jobs registered: {len(schedule.jobs)}")
        for j in schedule.jobs:
            fn_name = getattr(j.job_func, '__name__', repr(j.job_func))
            console.log(
                f"[WR-DEBUG-INIT]   job={fn_name} interval={j.interval}{j.unit} "
                f"next_run={j.next_run} last_run={j.last_run}"
            )
    except Exception as _wr_dbg_e:
        console.log(f"[red][WR-DEBUG-INIT] dump failed: {type(_wr_dbg_e).__name__}: {_wr_dbg_e!r}")
    # === /HM-WR-CYCLE-RCA-PHASE2 ===

    dash_thread = threading.Thread(target=run_dashboard, daemon=True)
    dash_thread.start()
    _dashboard_started.wait(timeout=5)
    if _dashboard_started.is_set():
        console.log("[green]Dashboard starting at http://localhost:8080")
    elif _dashboard_error is not None:
        console.log(f"[red]Dashboard failed before bind: {_dashboard_error}")
    else:
        console.log("[yellow]Dashboard thread did not confirm startup within 5s")
    console.log("[green]Scanner active (300s all sessions, 900s extended, 1800s overnight). Press Ctrl+C to stop.")
    console.log("[cyan]DayBlade Options S2 armed (15s power hour, multi-DTE, 8 max positions)")
    console.log("[cyan]VIX monitor armed (5 min interval, >5% spike alert)")
    console.log("[cyan]Earnings calendar armed (hourly check)")
    console.log("[cyan]AI journal armed (writes at market close)")
    console.log("[cyan]GEX scanner armed (15 min refresh, market hours)")
    console.log("[cyan]War Room armed (3 min trash talk mode, leaderboard rivalry, market hours)")
    console.log("[cyan]Autopilot armed (30 min rebalance, when enabled)")
    console.log("[cyan]Whisper Network armed (10 min trending check)")
    console.log("[cyan]Strength Scanner armed (5 min relative strength rankings)")
    console.log("[cyan]Strategy Race armed (hourly AI vs SPY comparison)")
    console.log("[cyan]Weekly Picks armed (Sunday 6 PM ET top 5)")
    console.log("[cyan]Cross-Asset Monitor armed (5 min SPY/VIX/DXY/Oil)")
    console.log("[cyan]Skew Monitor armed (15 min put/call skew)")
    console.log("[cyan]Fundamental Scanner armed (hourly fundamental scores)")
    console.log("[cyan]Bot Auto-Restart armed (immediate rescan after SL/TP close)")
    console.log("[cyan]Cost Monitor armed (15 min budget check, auto-pause losers)")
    console.log("[cyan]Flow Lean armed (15 min options flow directional bias, BULL/BEAR lean)")
    console.log("[cyan]Leader Signal armed (leader's trades injected into all prompts)")
    console.log("[cyan]CTO Advisory armed (Grok 4.2 — 4x daily: 9:00/9:45 AM, 3:45/4:15 PM ET)")
    console.log("[cyan]Weekly Elimination armed (Friday close, -15% threshold = paused)")
    console.log("[cyan]Strategy Lab armed (auto-optimize Sundays midnight, all stocks × all strategies)")
    console.log("[cyan]Universe Scanner armed (9 PM MST nightly, 500+ stocks → top 50)")
    console.log("[cyan]Strategy Engine armed (10 PM MST nightly, 15 strategies × top 50 stocks)")
    console.log("[cyan]Pre-market Gaps armed (1 AM MST / 4 AM ET, Chekov posts gaps >2%)")
    console.log("[cyan]Pre-market scanning: 10:30 PM MST every 5m → 1 AM MST every 2m → 6:30 AM market open")
    console.log("[cyan]Dalio Metals commentary armed (7 AM MST daily, Ollama Gemma3 4B)")
    console.log("[cyan]Advisory Team armed (Grok+Troi+Worf — fires 9:30 AM, 1:30 PM ET; portfolio_advice table; 8h TTL)")
    console.log("[cyan]Commander Riker XO armed (10 min crew synthesis during market hours)")
    console.log("[cyan]Admiral Picard armed (Sunday 10 PM MST weekly strategy thesis)")
    console.log("[cyan]Admiral Archer armed (Sunday 10:30 PM MST frontier scanner)")
    console.log("[cyan]Lt. Sulu DayBlade armed (Qwen3 8B, intraday only, EOD auto-close 3:45 PM ET)")
    console.log("[cyan]Season auto-rotation armed (Sunday 11:59 PM MST weekly rotation)")
    console.log("[cyan]Data Ingestion armed (market history 5 PM MST, insiders 5:30 PM MST, patterns every 15 min)")
    console.log("[cyan]🔭 Phase 4: Deep Scan & Strategy Rotation modules loaded")
    console.log("[bold magenta]🔬 Holly Nightly Backtest armed (midnight AZ / 3 AM ET, top-50 volume movers × RSI/MACD/Bollinger/Gap)")
    console.log("[cyan]⚔️  Warp Core Governor: rate limiter active (150 calls/min)")

    # ── Phase 2 autostart confirmation ────────────────────────────────────────
    import pytz as _stz
    from datetime import datetime as _dtm
    _az_now = az_now()  # HM-TZ Stage 3: zoneinfo (corruption-proof)
    _is_weekday = _az_now.weekday() < 5
    console.log(f"[STARTUP] DayBlade: {'auto-armed for market day (9:30–4:00 PM ET)' if _is_weekday else 'standby (weekend)'}")
    console.log("[STARTUP] Bridge Vote: scheduled (every 5 min, fires 9:00–9:10 AM ET)")
    console.log("[STARTUP] Fast Scanner: daemon running via launchd (com.trademinds.scanner)")
    console.log("[STARTUP] Dr. Crusher Healthcheck: scheduled 6AM + 7AM–1PM MST via launchd")
    console.log("[STARTUP] Cloudflare Tunnel: KeepAlive via launchd (com.trademinds.tunnel)")
    console.log("[STARTUP] Agent Watchdog: every 5 min during market hours")
    console.log("[STARTUP] Cash Manager: sweep check every 30 min during market hours")
    console.log("[STARTUP] Tax Harvester: daily scan at 3:30–4:00 PM ET")
    console.log("[STARTUP] Drift Rebalancer: check every 30 min during market hours")
    console.log("[STARTUP] VaR Calculator: daily snapshot at 4:05–4:15 PM ET")
    console.log("[STARTUP] Ollama: gemma3:4b (Picard) → Ollie GPU; phi3:mini cold-loads on demand — 5.8 Ollie migration (mistral:7b never migrated; decommissioned)")

    # ── Season 6.3 Fleet Cache ─────────────────────────────────────────────
    try:
        from engine.fleet_cache import init_fleet_cache as _init_fc
        _fc = _init_fc()
        console.log("[green][STARTUP] Fleet Cache: ACTIVE — collective intelligence <1ms (5-min refresh)")
    except Exception as _fc_err:
        console.log(f"[yellow][STARTUP] Fleet Cache: failed to init — {_fc_err}")

    # ── Season 6.3 Tiered Exits ────────────────────────────────────────────
    try:
        from engine.tiered_exits import MODEL_F_THRESHOLDS as _mf
        console.log("[green][STARTUP] Tiered Exits: Model F loaded — 50/30/20 @ 50/75/90% max profit, 2× stop")
    except Exception as _te_err:
        console.log(f"[yellow][STARTUP] Tiered Exits: failed to load — {_te_err}")

    # Warm up price cache in background so dashboard loads fast
    def _warmup():
        from engine.universe import get_active_universe
        from engine.market_data import get_all_prices
        console.log("[cyan]Warming up price cache (16 stocks)...")
        prices = get_all_prices(get_active_universe())
        console.log(f"[green]Price cache warm: {len(prices)}/16 stocks loaded")
    threading.Thread(target=_warmup, daemon=True).start()

    # Pre-load all required Ollama models — auto-pull if missing, then warm each
    # Models over 6 GB are skipped at startup to prevent RAM starvation.
    # deepseek-r1:14b (9 GB) removed — 15-min warm blocks scanner startup.
    # 5.8: gemma3:4b (Picard) moved to Ollie GPU (mistral:7b never migrated; decommissioned).
    def _warmup_ollama():
        import requests as _req, subprocess as _sp
        _MAX_STARTUP_GB = 6.0
        # (model, think, size_gb)
        _REQUIRED_MODELS = [
            # RAM patch 2026-04-17: phi3:mini actual loaded size is 8.6 GB (not 5.5 GB metadata).
            # Bumped to 7.0 so it EXCEEDS the 6.0 GB MAX_STARTUP threshold → cold-loads on first
            # real query instead of inflating baseline. Pairs with keep_alive=5s in OllamaProvider.
            ("phi3:mini",     False, 7.0),   # DayBlade Sulu + Chekov + main arena — cold-load on demand
            # 2026-05-17 Wave 1 Fix #3: mistral:7b entry removed — Pike migration to Ollie never completed; model lives on bigmac, not pre-loaded
            ("0xroyce/plutus", False, 8.0),   # T'Pol (dayblade-0dte) — >6 GB, skip startup
        ]
        # Check which models are installed on Ollie Max (.168)
        _installed: set[str] = set()
        _ollie_url = os.getenv("OLLIE_URL", "http://192.168.1.166:11434")
        try:
            _tags = _req.get(f"{_ollie_url}/api/tags", timeout=10).json()
            _installed |= {m["name"].split(":")[0] for m in _tags.get("models", [])}
            _installed |= {m["name"] for m in _tags.get("models", [])}
        except Exception:
            pass
        if not _installed:
            console.log("[yellow][STARTUP] Ollama: Ollie Max (.168) unreachable — skipping model warmup")
            return

        for _model, _think, _size_gb in _REQUIRED_MODELS:
            if _size_gb > _MAX_STARTUP_GB:
                console.log(f"[yellow][STARTUP] Ollama: {_model} skipped ({_size_gb} GB > {_MAX_STARTUP_GB} GB limit)")
                continue
            _base = _model.split(":")[0]
            _warm_url = f"{_ollie_url}/api/generate"
            # Auto-pull if not installed (non-blocking background pull)
            if _base not in _installed and _model not in _installed:
                console.log(f"[yellow][STARTUP] Ollama: {_model} not found — pulling...")
                try:
                    _sp.Popen(
                        ["/usr/local/bin/ollama", "pull", _model],
                        stdout=_sp.DEVNULL, stderr=_sp.DEVNULL,
                    )
                    console.log(f"[cyan][STARTUP] Ollama: {_model} pull launched in background")
                except Exception as _pe:
                    console.log(f"[red][STARTUP] Ollama: pull {_model} failed: {_pe}")
                continue   # can't warm what isn't installed yet

            # Warm the model (load into VRAM)
            try:
                _payload = {
                    "model":   _model,
                    "prompt":  "ready",
                    "stream":  False,
                    "options": {"num_predict": 1},
                }
                if not _think:
                    _payload["think"] = False
                _resp = _req.post(_warm_url, json=_payload, timeout=120)
                _resp.raise_for_status()  # 2026-05-17 Wave 1 Fix #3: catch HTTP 404 so missing-model errors stop silently logging as "warm ✓"
                console.log(f"[green][STARTUP] Ollama: {_model} warm ✓ (Ollie Max .168)")
            except Exception as _e:
                console.log(f"[yellow][STARTUP] Ollama: {_model} warmup skipped: {_e}")

    threading.Thread(target=_warmup_ollama, daemon=True).start()

    # Run earnings check on startup
    run_earnings_check()

    # Start Red Alert intraday monitor (5-min polling, market hours only)
    try:
        from engine.red_alert import start_red_alert
        start_red_alert()
        console.log("[cyan]Red Alert armed (5-min intraday polling: session changes, wall breaches, GEX flips, VIX spikes)")
    except Exception as e:
        console.log(f"[yellow]Red Alert failed to start: {e}")

    # Start realtime price monitor (Finnhub WebSocket or polling fallback)
    try:
        from engine.realtime_monitor import start_monitor
        start_monitor()
        console.log("[cyan]Realtime Monitor armed (Finnhub WebSocket, 5-min spike detection, instant Gemini Flash scans)")
    except Exception as e:
        console.log(f"[yellow]Realtime Monitor failed to start: {e}")

    # HM-OLLIE-EVENT-TAPE-V2-REALTIME Phase 2.5 Component 1 — Alpaca IEX tick recorder.
    # Spawns 5 daemon threads (ws, writer, cleanup, heartbeat, universe-refresh).
    # Heartbeat fires every 60s into trader.log so a silent recorder is visible.
    # Per CLAUDE.md Daemon Lifecycle Rule — module-level startup, not scan-coupled.
    # Polygon pivot 2026-05-27: Starter doesn't include WS trades; Alpaca IEX is
    # free and works. SIP ($99/mo) upgrade path documented in spec.
    try:
        from engine.tick_recorder import start_tick_recorder
        start_tick_recorder()
        console.log("[cyan]Tick Recorder armed (Alpaca IEX WebSocket, price_ticks table, 4h retention)")
    except Exception as e:
        console.log(f"[yellow]Tick Recorder failed to start: {e}")

    # HM-OLLIE-EVENT-TAPE-V2-REALTIME Phase 2.5 Component 2 — event detector.
    # Reads price_ticks, fires events into event_tape. 30s detection cadence,
    # daemon-thread pattern (matches tick_recorder). Heartbeat per HM-EQ doctrine.
    try:
        from engine.event_tape import start_event_detector
        start_event_detector()
        console.log("[cyan]Event Detector armed (event_tape table, 5 v1 detectors)")
    except Exception as e:
        console.log(f"[yellow]Event Detector failed to start: {e}")

    # === HM-WR-CYCLE-RCA-PHASE2 2026-05-20 (heartbeat repointed 2026-06-14) ===
    # 60-second heartbeat in the schedule loop — confirms run_pending() is being
    # called and reports the war_room_scheduler daemon thread's liveness.
    # NOTE: run_war_room no longer lives in schedule.jobs — HM-WR-DAEMON-THREAD
    # (commit 3c59497) moved it to the `war_room_scheduler` daemon thread, so the
    # old schedule.jobs lookup always reported "NOT in schedule.jobs" (false alarm).
    # The heartbeat now checks the actual thread instead.
    _wr_hb_last = time.time()
    # === /HM-WR-CYCLE-RCA-PHASE2 ===
    try:
        while True:
            try:
                schedule.run_pending()
            except Exception as _job_exc:
                console.log(f"[red]Scheduler job error (continuing): {_job_exc}")
            # === HM-WR-CYCLE-RCA-PHASE2 2026-05-20 ===
            _wr_hb_now = time.time()
            if _wr_hb_now - _wr_hb_last > 60:
                try:
                    _wr_alive = any(
                        t.name == "war_room_scheduler" and t.is_alive()
                        for t in threading.enumerate()
                    )
                    console.log(
                        f"[WR-DEBUG-HB] loop alive jobs={len(schedule.jobs)} "
                        f"war_room_scheduler_thread={'alive' if _wr_alive else 'DEAD'}"
                    )
                except Exception as _hb_e:
                    console.log(f"[red][WR-DEBUG-HB] error: {type(_hb_e).__name__}: {_hb_e!r}")
                _wr_hb_last = _wr_hb_now
            # === /HM-WR-CYCLE-RCA-PHASE2 ===
            time.sleep(1)
    except KeyboardInterrupt:
        try:
            from engine.realtime_monitor import stop_monitor
            stop_monitor()
        except Exception:
            pass
        console.print("\n[yellow]Trader stopped.")