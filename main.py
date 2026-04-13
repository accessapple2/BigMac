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
from rich.console import Console
from rich.panel import Panel

# Load .env before anything else
from dotenv import load_dotenv
load_dotenv(override=True)

# Monkey-patch sqlite3.connect to always use a 30s busy timeout.
# This prevents "database is locked" errors when 13+ AI models write concurrently.
_original_sqlite3_connect = sqlite3.connect
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
    now = _dt.now(az)
    if now.weekday() >= 5:  # Saturday / Sunday
        return False
    h = now.hour + now.minute / 60.0
    return 2.0 <= h < 18.0  # 2:00 AM – 6:00 PM MST


def initialize_arena():
    from config import (
        OPENAI_API_KEY, OPENAI_CODEX_MODEL, OPENAI_CODEX_MINI_MODEL,
        GEMINI_API_KEY, GROK_API_KEY, GROQ_API_KEY,
        OLLAMA_MODEL, OLLAMA_URL, MLX_URL, MLX_MODEL
    )
    from engine.ai_brain import Arena
    from engine.risk_manager import RiskManager
    from engine.providers.ollama_provider import OllamaProvider

    providers = [
        OllamaProvider(model=OLLAMA_MODEL, url=OLLAMA_URL),
        OllamaProvider(player_id="ollama-gemma27b", model="qwen3.5:9b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-deepseek", model="deepseek-r1:14b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-qwen3", model="qwen3.5:9b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-kimi", model="qwen3.5:9b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-glm4", model="qwen3.5:9b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-plutus", model="0xroyce/plutus:latest", url=OLLAMA_URL, timeout=300),
        # Lt. Sulu — DayBlade 2.0 (intraday day trader, free local compute)
        OllamaProvider(player_id="dayblade-sulu", model="qwen3.5:9b", url=OLLAMA_URL, timeout=90),
    ]

    # Ensign Chekov — routed through Ollama qwen3.5:9b (was qwen3:8b)
    providers.append(OllamaProvider(
        player_id="mlx-qwen3", model="qwen3.5:9b", url=OLLAMA_URL, timeout=180,
    ))
    console.log("[green]Chekov (mlx-qwen3) → Ollama qwen3.5:9b")

    # gpt-4o / gpt-o3 — routed to free local Ollama (no OpenAI spend)
    providers.append(OllamaProvider("gpt-4o", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    providers.append(OllamaProvider("gpt-o3", "deepseek-r1:14b", url=OLLAMA_URL, timeout=180))

    # Gemini players — local Ollama
    providers.append(OllamaProvider("gemini-2.5-pro", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    providers.append(OllamaProvider("gemini-2.5-flash", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    providers.append(OllamaProvider("options-sosnoff", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    providers.append(OllamaProvider("energy-arnold", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    # Lt. Cmdr. Data — coding specialist
    providers.append(OllamaProvider("ollama-coder", "qwen2.5-coder:7b", url=OLLAMA_URL, timeout=180))
    # Mr. Anderson — CrewAI collective / The One
    providers.append(OllamaProvider("super-agent", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    # Mr. Dalio — metals specialist
    providers.append(OllamaProvider("dalio-metals", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    # Codex players → free local Ollama
    providers.append(OllamaProvider("claude-sonnet", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    providers.append(OllamaProvider("claude-haiku", "qwen2.5-coder:7b", url=OLLAMA_URL, timeout=180))
    # Grok players → free local Ollama
    providers.append(OllamaProvider("grok-3", "qwen3.5:9b", url=OLLAMA_URL, timeout=180))
    providers.append(OllamaProvider("grok-4", "deepseek-r1:14b", url=OLLAMA_URL, timeout=180))
    providers.append(OllamaProvider("cto-grok42", "qwen2.5-coder:7b", url=OLLAMA_URL, timeout=180))

    if GROQ_API_KEY:
        from engine.providers.groq_provider import GroqProvider
        providers.append(GroqProvider(GROQ_API_KEY, "ollama-llama", "llama-3.3-70b-versatile", "Llama 3.3 70B"))

    from config import OPTIONS_MAX_PCT, OPTIONS_TOTAL_MAX_PCT
    risk = RiskManager(options_max_pct=OPTIONS_MAX_PCT, options_total_max_pct=OPTIONS_TOTAL_MAX_PCT)
    return Arena(providers, risk)


def initialize_dayblade():
    from config import OLLAMA_MODEL, OLLAMA_URL
    from engine.providers.ollama_provider import OllamaProvider
    from engine.dayblade import DayBladeScanner, ensure_player

    ensure_player()
    provider = OllamaProvider(player_id="dayblade-0dte", model="0xroyce/plutus", url=OLLAMA_URL, timeout=300)
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

        if unit in ("seconds", "minutes"):
            job.next_run = now + _dt.timedelta(seconds=offset)
        elif unit == "hours":
            job.next_run = now + _dt.timedelta(minutes=offset)


_last_scan_time = 0
_scan_lock = threading.Lock()

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
_SCAN_TIER1: frozenset = frozenset({
    "dayblade-sulu",     # Sulu    S6.3 primary options trader (qwen3.5:9b) — PRIORITY 1
    "super-agent",       # Anderson      (crewai)
    "grok-4",            # Spock         (deepseek-r1:7b)
    "ollama-coder",      # Data          (qwen2.5-coder:7b)
    "mlx-qwen3",         # Chekov        (qwen3.5:9b)
})

# Tier 2 — Department Heads: secondary signals, every 2 hours
_SCAN_TIER2: frozenset = frozenset({
    "options-sosnoff",   # Troi          (qwen3.5:9b)
    "energy-arnold",     # Trip Tucker   (qwen3.5:9b)
    "ollama-plutus",     # McCoy         (mistral:7b)
    "ollama-local",      # Geordi        (gemma3:4b)
    "ollama-llama",      # Uhura         (llama3.1:latest)
    "gemini-2.5-flash",  # Worf          (qwen3.5:9b)
    "ollama-qwen3",      # Scotty        (qwen3.5:9b)
})

# Tier 3 — Cadets: market open + close only (learning, not real-time)
_SCAN_TIER3: frozenset = frozenset({
    "gpt-4o",            # (qwen3.5:9b)
    "claude-sonnet",     # (qwen3.5:9b)
    "grok-3",            # (qwen3.5:9b)
    "ollama-gemma27b",   # (qwen3.5:9b)
    "ollama-glm4",       # (qwen3.5:9b)
    "ollama-kimi",       # (qwen3.5:9b)
    "gpt-o3",            # (deepseek-r1:7b)
    "ollama-deepseek",   # (deepseek-r1:7b)
    "claude-haiku",      # (qwen2.5-coder:7b)
    "cto-grok42",        # (qwen2.5-coder:7b)
})

_TIER1_INTERVAL = 30 * 60       # 30 minutes
_TIER2_INTERVAL = 120 * 60      # 2 hours
_TIER3_INTERVAL = 4 * 60 * 60   # min gap between tier3 runs (open → close separation)
_tier_last_scan: dict = {1: 0.0, 2: 0.0, 3: 0.0}


def _tier3_window_open() -> bool:
    """True during market-open (6:30–7:00 AM MST) and pre-close (12:45–1:30 PM MST) windows."""
    import pytz
    from datetime import datetime as _dt
    az = pytz.timezone("US/Arizona")
    mins = _dt.now(az).hour * 60 + _dt.now(az).minute
    # Open: 6:30–7:00 AM MST = 390–420 min; Close: 12:45–1:30 PM MST = 765–810 min
    return (390 <= mins < 420) or (765 <= mins < 810)

def _get_scan_interval():
    """Return the appropriate scan interval based on current market session.

    Dilithium Crystal Protocol v2 — Pre-market intelligence starts at 10:30 PM MST (1:30 AM ET).

    Schedule (all MST, MST = ET - 3 during EDT):
      10:30 PM - 1:00 AM MST (1:30 AM - 4:00 AM ET): Early pre-market — every 5 min (300s)
      1:00 AM - 6:30 AM MST  (4:00 AM - 9:30 AM ET): Full pre-market — every 2 min (120s)
      6:30 AM - 12:00 PM MST (9:30 AM - 3:00 PM ET): Market hours — every 3 min (180s)
      12:00 PM - 1:30 PM MST (3:00 PM - 4:30 PM ET): Power hour — every 90s
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
    now = _dt.now(az)
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
        # 6:30 AM - 12:00 PM MST: Market hours (3 min)
        return SCAN_INTERVAL_MARKET  # 180s
    if 720 <= mins < 810:
        # 12:00 PM - 1:30 PM MST: Power hour (90s)
        return SCAN_INTERVAL_POWER_HOUR  # 90s
    if 810 <= mins < 1020:
        # 1:30 PM - 5:00 PM MST: After hours (10 min)
        return SCAN_INTERVAL_EXTENDED  # 600s
    # 5:00 PM - 10:30 PM MST: Evening/overnight (30 min)
    return SCAN_INTERVAL_OVERNIGHT  # 1800s


def run_scanner():
    global arena, _news_counter, _last_scan_time, _tier_last_scan
    import time as _time

    # Prevent scan stacking — skip if previous scan still running
    if not _scan_lock.acquire(blocking=False):
        console.log("[yellow]Scan skipped — previous scan still running")
        return

    # From here we OWN the lock — every code path must release it exactly once.
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
    from config import WATCH_STOCKS

    # Fetch news BEFORE each scan so the AI has fresh headlines
    _news_counter += 1
    if _news_counter % 5 == 1:  # Every 5 cycles
        try:
            from engine.news_fetcher import fetch_news
            fetch_news(WATCH_STOCKS, max_per_symbol=5)
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
    _captured_stocks   = list(WATCH_STOCKS)
    _captured_players  = frozenset(active_players)
    _captured_counter  = _news_counter
    console.log(f"[cyan]Market scan triggered [{tier_label}] — {len(active_players)} agents (interval={interval}s)")

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

    threading.Thread(target=_arena_scan_thread, daemon=True, name="arena_scanner").start()


def run_dayblade():
    global dayblade
    if dayblade is None:
        dayblade = initialize_dayblade()
    try:
        # DayBlade handles its own power hour throttling internally (15s during 2-3:30 PM)
        dayblade.run_scan()
    except Exception as e:
        console.log(f"[red]DayBlade error: {e}")


_last_ma_regime: str = ""

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


def run_earnings_check():
    """Check earnings calendar once per hour."""
    try:
        from config import WATCH_STOCKS
        from engine.earnings_calendar import get_earnings_warnings
        from engine.telegram_alerts import alert_earnings_upcoming

        _EARNINGS_MEGA = [
            "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AMD","PLTR","CRM",
            "NFLX","AVGO","COST","LLY","JPM","BAC","GS","V","MA","UNH","JNJ",
            "PFE","ABBV","XOM","CVX","COP","WMT","HD","TGT","DIS","CMCSA",
            "BA","CAT","GE","RTX","COIN","SQ","HOOD",
        ]
        upcoming = get_earnings_warnings(list(set(WATCH_STOCKS) | set(_EARNINGS_MEGA)))
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
        from engine.market_data import get_stock_price
        from config import WATCH_STOCKS

        prices = {}
        for sym in WATCH_STOCKS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data

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


def run_gex_refresh():
    """Refresh GEX cache every 15 minutes during market hours."""
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

def run_gex_overlay_update():
    """Update GEX Overlay DB levels every 15 min during any market session."""
    try:
        from engine.gex_overlay import update_all_gex_levels
        update_all_gex_levels()
    except Exception as e:
        console.log(f"[red]GEX Overlay update error: {e}")


def run_morning_briefing():
    """Generate morning levels at 6:25 AM MST (pre-market)."""
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.datetime.now(az)
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


def run_archer_morning_briefing():
    """Phase 3.6 — Comprehensive Archer briefing at 6:00 AM AZ (9:00 AM ET)."""
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.datetime.now(az)
    if now.weekday() >= 5:  # Skip weekends
        return
    if now.hour != 6 or now.minute > 15:  # Fire 6:00–6:15 AM AZ window
        return
    try:
        from engine.morning_briefing import generate_morning_briefing
        result = generate_morning_briefing()
        audio = result.get("audio_url") or "no audio"
        console.log(f"[cyan]Archer Morning Briefing generated — audio: {audio}")
    except Exception as e:
        console.log(f"[red]Archer briefing error: {e}")


def run_opening_range():
    """Set opening range at 6:45 AM MST (after first 15 min of trading)."""
    import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.datetime.now(az)
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


def run_battle_station_monitor():
    """60-second options position monitor (early-exit if no positions)."""
    global _last_battle_station_run
    import time as _t
    if _t.time() - _last_battle_station_run < 55:  # deduplicate on 30s tick
        return
    _last_battle_station_run = _t.time()
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

def run_war_room():
    """War Room: all AIs give hot takes. Free models 24/7, paid models market hours only."""
    global arena, _last_war_room_time
    if arena is None:
        return
    from engine.risk_manager import RiskManager
    import time as _time

    session = RiskManager.is_market_hours()
    if not session:
        return  # Fully closed (weekends, overnight)

    # Throttle: 10 min during active market (Ollama bandwidth), 5 min pre/post-market
    now = _time.time()
    if session == "market" or session == "power_hour":
        if now - _last_war_room_time < 600:
            return
    elif session in ("pre_market", "post_market"):
        if now - _last_war_room_time < 300:
            return
    _last_war_room_time = now

    console.log("[magenta]War Room: launching cycle...")

    def _war_room_thread():
        try:
            from engine.market_data import get_stock_price
            from engine.war_room import run_war_room as _run_wr
            from config import WATCH_STOCKS
            prices = {}
            for sym in WATCH_STOCKS:
                data = get_stock_price(sym)
                if "error" not in data:
                    prices[sym] = data
            if prices:
                _run_wr(arena.providers, prices)
            else:
                console.log("[yellow]War Room: no prices available, skipping")
        except Exception as e:
            console.log(f"[red]War Room error: {e}")

    threading.Thread(target=_war_room_thread, daemon=True).start()


def run_autopilot():
    """Autopilot: auto-rebalance overweight positions and maintain cash floor."""
    try:
        from engine.autopilot import run_autopilot as _run_ap
        from engine.market_data import get_stock_price
        from config import WATCH_STOCKS
        prices = {}
        for sym in WATCH_STOCKS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data
        if prices:
            _run_ap(prices)
    except Exception as e:
        console.log(f"[red]Autopilot error: {e}")


def run_whisper():
    """Whisper Network: check for trending watchlist stocks."""
    try:
        from engine.whisper_network import run_whisper_check
        run_whisper_check()
    except Exception as e:
        console.log(f"[red]Whisper error: {e}")


def run_strength_scan():
    """Relative Strength Scanner: rank watchlist stocks vs SPY."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.strength_scanner import scan_relative_strength
        from config import WATCH_STOCKS
        rankings = scan_relative_strength(WATCH_STOCKS)
        if rankings:
            top = rankings[0]
            bottom = rankings[-1]
            console.log(
                f"[cyan]Strength: #{1} {top['symbol']}({top['score']}) ... "
                f"#{len(rankings)} {bottom['symbol']}({bottom['score']})"
            )
    except Exception as e:
        console.log(f"[red]Strength scan error: {e}")


def run_trend_forecast():
    """Trend Forecast: predict trends for all watchlist stocks."""
    if not should_run_task("run_trend_forecast", throttle_mins=60):
        return
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.trend_predictor import predict_all_trends
        from config import WATCH_STOCKS
        results = predict_all_trends(WATCH_STOCKS)
        if results:
            top = results[0]
            console.log(f"[dim]Trend: top {top['symbol']} {top['direction']} ({top['confidence']:.0f}%)")
    except Exception as e:
        console.log(f"[red]Trend forecast error: {e}")


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


def run_discovery_scan():
    """Discovery Scanner — RETIRED. Replaced by Volume Radar (run_volume_market_scan).
    Kept as archive reference. Scheduler call below is commented out.
    """
    pass


# ---------------------------------------------------------------------------
# Volume Radar — Full Market Scanner (replaces Discovery Scanner)
# ---------------------------------------------------------------------------

def run_volume_universe_refresh():
    """Weekly: refresh the full ~10,000-stock universe from Alpaca (Sunday 10 PM MST)."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)
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


def run_volume_baselines():
    """Nightly: update 20-day average volume baselines from Alpaca bars (weeknights 11 PM MST)."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)
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


def run_impulse_check():
    """Hourly Impulse Detector: check watchlist for volume/price/breakout impulses."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from config import WATCH_STOCKS
        from engine.impulse_detector import scan_all_impulses
        alerts = scan_all_impulses(WATCH_STOCKS)
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


def run_gap_scan():
    """Morning Gap Scanner: runs once per day at market open (7:30-8:30 AM AZ / 9:30-10:30 AM ET)."""
    global _gap_scan_done_today
    import pytz
    from datetime import datetime as _dt
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)

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
        from config import WATCH_STOCKS
        from engine.gap_scanner import scan_all_gaps
        gaps = scan_all_gaps(WATCH_STOCKS)
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
        from config import WATCH_STOCKS
        from engine.gap_scanner import update_gap_fills
        update_gap_fills(WATCH_STOCKS)
    except Exception as e:
        console.log(f"[yellow]Gap fill check error: {e}")


_theta_last_run = 0.0

def run_theta_scan():
    """Theta Collection Scanner: find premium-selling opportunities. Runs every 4 hours."""
    global _theta_last_run
    import time as _time
    now = _time.time()
    if now - _theta_last_run < 14400:  # 4-hour minimum between scans
        return
    _theta_last_run = now
    try:
        from config import WATCH_STOCKS
        from engine.theta_scanner import scan_all_theta
        results = scan_all_theta(WATCH_STOCKS)
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

def run_imbalance_scan():
    """Supply/Demand Imbalance Zone Scanner: detect FVG zones across daily+hourly candles."""
    global _imbalance_last_run
    import time as _time
    now = _time.time()
    if now - _imbalance_last_run < 7200:  # 2-hour minimum between runs
        return
    _imbalance_last_run = now
    try:
        from config import WATCH_STOCKS
        from engine.imbalance_detector import scan_all_imbalances
        results = scan_all_imbalances(WATCH_STOCKS)
        total = sum(len(z) for z in results.values())
        console.log(f"[cyan]Imbalance scan complete: {total} zone(s) across {len(results)} symbol(s)")
    except Exception as e:
        console.log(f"[red]Imbalance scan error: {e}")


_sma_last_run = 0.0

def run_sma_scan():
    """200 SMA Filter: scan watchlist for Bounce/Breakdown/Reclaim signals. Runs every 4 hours."""
    global _sma_last_run
    import time as _time
    now = _time.time()
    if now - _sma_last_run < 14400:  # 4 hours between full scans
        return
    _sma_last_run = now
    try:
        from config import WATCH_STOCKS
        from engine.sma_filter import scan_all_sma_signals
        results = scan_all_sma_signals(WATCH_STOCKS)
        signals = [v for v in results.values() if v.get("signal_type")]
        testing = [v for v in results.values() if v.get("is_testing") and not v.get("signal_type")]
        console.log(f"[cyan]200 SMA: {len(results)} stocks scanned, "
                    f"{len(signals)} signals, {len(testing)} testing SMA")
    except Exception as e:
        console.log(f"[red]SMA scan error: {e}")


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
        from config import WATCH_STOCKS
        # Use first available provider
        pid = list(arena.providers.keys())[0]
        provider = arena.providers[pid]
        _run_wp(provider, WATCH_STOCKS)
        _weekly_picks_sent = True
    except Exception as e:
        console.log(f"[red]Weekly picks error: {e}")


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


def run_ready_room():
    """Ready Room briefing at 4 scheduled ET times on weekdays."""
    global _ready_room_slots_done_today
    from datetime import datetime
    import pytz

    try:
        az = pytz.timezone("US/Arizona")
        now = datetime.now(az)
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


def run_oi_morning_snapshot():
    """Take SPY OI baseline at market open (6:30 AM AZ / 9:30 AM ET)."""
    global _oi_snapshot_done_today
    from datetime import datetime
    import pytz

    try:
        az = pytz.timezone("US/Arizona")
        now = datetime.now(az)
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


def run_cto_advisory():
    """CTO Advisory: 4x daily briefings at scheduled Arizona times."""
    global _cto_slots_done_today
    from datetime import datetime
    import pytz

    try:
        az = pytz.timezone("US/Arizona")
        now = datetime.now(az)
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
                    console.log(f"[dim]CTO Advisory [{bt_label}]: skipped (already done or no API key)")
            except Exception as e:
                console.log(f"[red]CTO Advisory [{btype}] error: {e}")
                _cto_slots_done_today.add(btype)  # Don't retry on error
            break  # Only fire one per cycle


_grok_advisor_slots_done_today: set = set()

def run_grok_advisor():
    """Kirk Grok Swing Advisor: fires at 9:30 AM and 1:30 PM ET on weekdays."""
    global _grok_advisor_slots_done_today
    from datetime import datetime
    import pytz

    try:
        et = pytz.timezone("US/Eastern")
        now = datetime.now(et)
    except Exception:
        return

    # Reset flags at midnight
    if now.hour < 1:
        _grok_advisor_slots_done_today = set()
        return

    # Weekdays only
    if now.weekday() >= 5:
        return

    # Fire within a 20-minute window after each target time
    # Slot "open"  = 9:30 AM ET  (market open)
    # Slot "mid"   = 1:30 PM ET  (midday check)
    slots = [("open", 9, 30), ("mid", 13, 30)]
    for slot_id, target_h, target_m in slots:
        if slot_id in _grok_advisor_slots_done_today:
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
                _grok_advisor_slots_done_today.add(slot_id)
            break  # One slot per poll cycle


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
        _setup_db()   # ensure agent_ratings table exists
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
        from config import WATCH_STOCKS
        from engine.telegram_alerts import send_daily_summary
        import sqlite3

        prices = {}
        for sym in WATCH_STOCKS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data

        conn = sqlite3.connect("data/trader.db", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        players = conn.execute(
            "SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
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


def run_universe_scan():
    """Nightly universe scan — Ensign Chekov sweeps 500+ stocks.
    Runs at 9 PM MST / 12 AM ET (weeknights + Sunday). Takes 2-3 minutes."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)
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


def run_strategy_scan():
    """Nightly strategy scan — run 15 strategies against top 50 universe stocks.
    Runs at 10 PM MST / 1 AM ET (weeknights + Sunday). Takes 1-2 minutes."""
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)
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


def run_chekov_stoploss():
    """Check Chekov's positions against stop-loss/take-profit prices."""
    try:
        from engine.chekov_autotrade import check_stop_loss_take_profit
        check_stop_loss_take_profit()
    except Exception as e:
        console.log(f"[yellow]Chekov SL/TP check error: {e}")


_premarket_gaps_done = False

def run_premarket_gaps():
    """Pre-market gap scanner — Chekov posts gaps > 2% to War Room.
    Runs at 1 AM MST (4 AM ET) weekdays."""
    global _premarket_gaps_done
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)

    # Reset flag at midnight
    if now.hour == 0:
        _premarket_gaps_done = False
        return

    # Only run weekdays at 1 AM MST (4 AM ET)
    if now.weekday() >= 5 or now.hour != 1 or _premarket_gaps_done:
        return

    _premarket_gaps_done = True
    try:
        import yfinance as yf
        from engine.war_room import save_hot_take
        from engine.universe_scanner import get_latest_universe_scan

        # Get top universe stocks
        scan = get_latest_universe_scan()
        tickers = [s["ticker"] for s in scan.get("results", [])[:50]] if scan else []
        if not tickers:
            from config import WATCH_STOCKS
            tickers = WATCH_STOCKS

        gaps = []
        for sym in tickers:
            try:
                stock = yf.Ticker(sym)
                hist = stock.history(period="2d", prepost=True)
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


def run_finviz_premarket_scan():
    """Finviz pre-market watchlist builder — runs at 6:15 AZ (9:15 ET) weekdays."""
    global _finviz_scan_done
    from datetime import datetime as _dt
    import pytz
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)

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
                # Move matching tickers to the front of the watchlist
                matched   = [s for s in holly_tickers if s in scan_symbols or s not in scan_symbols]
                boosted   = list(dict.fromkeys(matched + scan_symbols))  # deduplicated, Holly first
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
            uvicorn.run(app, host="127.0.0.1", port=8080, log_level="warning")
            return  # Clean exit (shutdown)
        except Exception as exc:
            _dashboard_error = exc
            # Backoff: 5s → 15s → 30s → 60s cap
            delay = min(5 * (2 ** min(_attempt - 1, 3)), 60)
            console.log(f"[yellow]Dashboard attempt {_attempt} failed ({exc}), retrying in {delay}s...")
            _t.sleep(delay)


_sulu_closed_today = False

def run_sulu_autoclose():
    """Lt. Sulu DayBlade EOD auto-close: sell ALL positions at 3:45 PM ET (12:45 PM MST)."""
    global _sulu_closed_today
    import pytz
    from datetime import datetime as _dt

    try:
        az = pytz.timezone("US/Arizona")
        now = _dt.now(az)
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
        now = __import__("datetime").datetime.now(az)
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

def run_battle_station_0dte_job() -> None:
    """Rules-based 0DTE battle station — fires every 2 min during 9:45 AM - 2:30 PM ET."""
    import pytz
    from datetime import datetime as _dt
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)
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
        console.log("[bold green]EQUITY RESET: All agents reset to $10,000 (Season 5)")
    c.close()


if __name__ == "__main__":
    # Re-bind module logger explicitly so inner scheduled-job functions can resolve
    # it via closure — the module-level definition at line 3 can be shadowed or
    # unreachable from nested closures in some Python import orderings.
    logger = _logging.getLogger(__name__)

    # Ensure DB exists
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
            _wc.execute("PRAGMA journal_mode=WAL")
            _wc.execute("PRAGMA synchronous=NORMAL")   # safe with WAL; faster than FULL
            _wc.close()
            console.log(f"[green]WAL mode enabled: {os.path.basename(_wal_db)}")
        except Exception as _wal_e:
            console.log(f"[yellow]WAL mode warning ({os.path.basename(_wal_db)}): {_wal_e}")

    # Season 5 one-time equity reset
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

    # Seed/refresh agent ratings with clean Season 5 data on startup
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
    _ollama_warm = False
    try:
        import requests as _req
        _r = _req.get("http://127.0.0.1:11434/api/tags", timeout=5)
        _ollama_warm = _r.ok
    except Exception:
        pass

    from engine.crew_scanner import ACTIVE_SCANNERS, RULES_SCANNERS
    _startup_log.info("=" * 60)
    _startup_log.info("USS TRADEMINDS — SEASON 5 — MONDAY STARTUP")
    _startup_log.info("=" * 60)
    _startup_log.info(f"Active Scanners: {ACTIVE_SCANNERS}")
    _startup_log.info(f"Rules Scanners: {RULES_SCANNERS}")
    _startup_log.info(f"Scan Model: T'Pol=0xroyce/plutus (0DTE) | McCoy=mistral:7b (triage)")
    _startup_log.info(f"API Key: {'SET' if _anthropic_key else 'MISSING'}")
    _startup_log.info(f"Ollama: {'WARM' if _ollama_warm else 'COLD'}")
    _startup_log.info(f"Bridge: bridge.accessapple.com")
    _startup_log.info(f"Daily Cost: $0.00 (Ollama) + ~$0.50 (Sonnet CIC)")
    _startup_log.info("=" * 60)
    _startup_log.info("ALL SYSTEMS OPERATIONAL — ENGAGE")

    console.print(Panel.fit(
        "[bold green]USS TradeMinds[/bold green] — [bold cyan]All systems operational[/bold cyan]\n"
        "[dim]Season 5 — Final Seven active | Dashboard: http://127.0.0.1:8080[/dim]",
        border_style="green"
    ))

    # Scanner ticks every 30s; run_scanner enforces dynamic cooldown internally
    schedule.every(2).minutes.do(run_scanner)
    schedule.every(5).minutes.do(run_dayblade)  # DayBlade 0DTE: T'Pol on plutus, every 2min
    schedule.every(15).minutes.do(run_ma_regime_update)  # 8/21 MA Cross Regime: every 15 min
    schedule.every(15).minutes.do(run_vix_check)          # VIX: every 5 min
    schedule.every(1).hours.do(run_earnings_check)       # Earnings: hourly
    schedule.every(30).minutes.do(run_daily_summary)      # Daily summary: checks every 5 min, sends once at close
    schedule.every(30).minutes.do(run_daily_rating_update) # Agent ratings: checks every 5 min, fires once at 4:30 PM ET
    schedule.every(30).minutes.do(run_journal)             # AI journal: checks every 5 min, writes once at close
    schedule.every(15).minutes.do(run_gex_refresh)        # GEX (CBOE): every 15 min during market hours
    schedule.every(30).minutes.do(run_alpaca_gex_refresh)  # GEX (Alpaca): 4x/day at 9:00/9:35/12:00/15:00 ET
    schedule.every(15).minutes.do(run_gex_overlay_update) # GEX Overlay DB: every 15 min (king node, flip, walls)
    schedule.every().day.at("06:00").do(run_morning_briefing)         # Battle Station: 6:00 AM AZ (was every 5 min)
    schedule.every().day.at("06:00").do(run_archer_morning_briefing)  # Phase 3.6: Archer briefing 6:00 AM AZ
    schedule.every().day.at("06:45").do(run_opening_range)            # Battle Station: opening range 6:45 AM AZ
    schedule.every(2).minutes.do(run_battle_station_monitor)  # Battle Station: 60s options position monitor
    schedule.every(10).minutes.do(run_war_room)             # War Room: every 3 min during market hours (trash talk mode)
    schedule.every(30).minutes.do(run_autopilot)           # Autopilot: every 30 min
    schedule.every(10).minutes.do(run_whisper)             # Whisper Network: every 10 min
    schedule.every(15).minutes.do(run_strength_scan)        # Strength Scanner: every 5 min
    schedule.every(1).hours.do(run_strategy_race)           # Strategy Race: hourly update
    schedule.every(30).minutes.do(run_weekly_picks)          # Weekly Picks: checks every 5 min, sends Sunday 6PM ET
    schedule.every(15).minutes.do(run_cross_asset_check)    # Cross-Asset: every 5 min
    schedule.every(15).minutes.do(run_flow_lean)            # Flow Lean: every 15 min (options premium directional bias)
    schedule.every(15).minutes.do(run_ai_saas_disruption)   # AI SaaS Disruption: IGV + 13 SaaS names, 4 triggers, posts to 9000
    schedule.every(30).minutes.do(run_cto_advisory)          # CTO Advisory: checks every 5 min, fires 4x daily (pre_market, post_open, pre_close, post_close)
    schedule.every(30).minutes.do(run_ready_room)             # Ready Room: checks every 5 min, fires 4x daily (8:00/9:15/12:00/3:30 ET)
    schedule.every(30).minutes.do(run_grok_advisor)           # Advisory Team (Grok/Ollie+Troi+Worf): fires at 9:30 AM and 1:30 PM ET
    schedule.every(5).minutes.do(run_portfolio_monitor)       # Ship's Computer: Captain's Portfolio monitor (stop breaches, big moves, new advice)
    schedule.every(5).minutes.do(run_oi_morning_snapshot)    # OI Tracker: baseline snapshot at market open (9:30 ET)

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

        # Check 1: Is Ollama alive?
        try:
            r = _req.get("http://127.0.0.1:11434/api/tags", timeout=5)
            if not r.ok:
                _hc.warning("Ollama API down — restarting")
                subprocess.run(["pkill", "-9", "ollama"], capture_output=True)
                subprocess.run(["open", "-a", "Ollama"], capture_output=True)
        except Exception:
            _hc.warning("Ollama unreachable — restarting")
            subprocess.run(["pkill", "-9", "ollama"], capture_output=True)
            subprocess.run(["open", "-a", "Ollama"], capture_output=True)

        # Check 2: Is the scan model responsive?
        try:
            r = _req.post(
                "http://127.0.0.1:11434/api/generate",
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
            _now_h = _dtc.now(_az).hour
            _now_wd = _dtc.now(_az).weekday()
            if _now_wd < 5 and 7 <= _now_h <= 14:
                _sc = sqlite3.connect(_db_path, timeout=5)
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
        now = _dt.now(az)
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
    schedule.every(15).minutes.do(run_volume_market_scan)                 # Volume Radar: every 15 min during market hours
    schedule.every(5).minutes.do(run_volume_red_alert)                    # Red Alert: every 2 min during market hours
    schedule.every(30).minutes.do(run_sma_scan)               # 200 SMA Filter: checks every 30 min, runs every 4 hours
    schedule.every(1).hours.do(run_impulse_check)              # Impulse Detector: hourly during market hours
    schedule.every(2).hours.do(run_imbalance_scan)             # Imbalance Zones: every 2 hours (zones are stable)
    schedule.every(30).minutes.do(run_theta_scan)              # Theta Scanner: checks every 30 min, runs every 4 hours
    schedule.every(15).minutes.do(run_gap_scan)                 # Gap Scanner: checks every 5 min, fires once at market open
    schedule.every(15).minutes.do(run_gap_fill_check)           # Gap Fill Tracker: every 5 min during market hours
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
    schedule.every(10).minutes.do(run_chekov_stoploss)        # Chekov SL/TP: every 5 min, check positions vs stop/target
    schedule.every(30).minutes.do(run_metals_commentary)     # Dalio Metals: checks every 30 min, runs 7 AM MST only
    schedule.every(15).minutes.do(run_premarket_gaps)         # Pre-market gaps: checks every 15 min, fires 1 AM MST (4 AM ET)
    schedule.every(15).minutes.do(run_finviz_premarket_scan)   # Finviz watchlist: 5-min check, fires 6:15 AZ (9:15 ET)
    schedule.every(5).minutes.do(run_sulu_autoclose)          # Lt. Sulu EOD: auto-close all positions at 12:45 PM MST (3:45 PM ET)
    schedule.every(2).minutes.do(run_crew_scanner_job)        # Crew Scanner: agent signal pipeline (every 2 min, alpha squad only)

    # Ollie Extended-Hours Scan — pre-market (7–9:30 AM ET) + after-hours (4–6 PM ET)
    def run_ollie_extended_scan():
        """Run ollie-auto signal pipeline during extended trading hours.
        Other agents are market-hours only; this fires exclusively for ollie-auto.
        """
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
    schedule.every(10).minutes.do(run_ollie_extended_scan)   # Ollie Extended-Hours: every 10 min during pre/post market
    schedule.every(5).minutes.do(run_battle_station_0dte_job) # Battle Station 0DTE: rules-based SPY 0DTE scanner
    from engine.recovery_protocol import run_recovery_scan
    schedule.every(15).minutes.do(run_recovery_scan)           # Recovery Protocol: checks every 15 min during market hours
    from engine.wheel_strategy import run_wheel_scan, check_wheel_assignments
    schedule.every(15).minutes.do(run_wheel_scan)              # Wheel Strategy: scan for put-selling opportunities every 15 min
    schedule.every(1).hours.do(check_wheel_assignments)        # Wheel Strategy: check for option assignments hourly
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
            from config import WATCH_STOCKS
            ctx = {"prices": {s: get_stock_price(s) for s in WATCH_STOCKS[:5]}}
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
    def run_webull_sync():
        """Auto-sync Steve's Webull portfolio positions to DB."""
        if not is_extended_or_market_hours():
            return
        try:
            from engine.webull_client import sync_positions_to_db
            sync_positions_to_db()
        except Exception as e:
            console.log(f"[red]Webull auto-sync error: {e}")

    schedule.every(15).minutes.do(run_webull_sync)

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
        now = _dt.now(az)
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
        now = _dt.now(az)
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

    # Riker XO: synthesize after each CTO briefing cycle (every 10 min during market hours)
    def run_riker_synthesis():
        """Commander Riker: synthesize crew input into recommendation."""
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return
        try:
            from engine.riker_xo import get_latest_recommendation, generate_riker_synthesis
            latest = get_latest_recommendation()
            # Only regenerate if stale (>10 min) or missing
            if latest.get("fresh"):
                return
            generate_riker_synthesis()
        except Exception as e:
            console.log(f"[red]Riker synthesis error: {e}")

    schedule.every(10).minutes.do(run_riker_synthesis)       # Riker XO: every 10 min during market hours

    # Admiral Picard: weekly strategy briefing (Sunday 10 PM MST)
    def run_picard_briefing():
        """Admiral Picard: generate weekly strategy briefing Sunday 10 PM MST."""
        from datetime import datetime as _dt
        import pytz
        try:
            az = pytz.timezone("US/Arizona")
            now = _dt.now(az)
        except Exception:
            return
        # Sunday (weekday 6) between 10:00-10:30 PM MST
        if now.weekday() != 6 or now.hour != 22 or now.minute > 30:
            return
        try:
            from engine.picard_strategy import get_latest_briefing, generate_picard_briefing
            latest = get_latest_briefing()
            # Don't regenerate if briefing is less than 6 hours old
            if latest.get("briefing") and latest.get("generated_at"):
                return
            generate_picard_briefing()
        except Exception as e:
            console.log(f"[red]Picard briefing error: {e}")

    schedule.every(30).minutes.do(run_picard_briefing)       # Picard: Sunday 10 PM MST weekly strategy

    # Admiral Archer: frontier scanner (Sunday 10:30 PM MST)
    def run_archer_frontier():
        """Admiral Archer: scan frontier stocks Sunday 10:30 PM MST."""
        from datetime import datetime as _dt
        import pytz
        try:
            az = pytz.timezone("US/Arizona")
            now = _dt.now(az)
        except Exception:
            return
        # Sunday (weekday 6) between 10:30-11:00 PM MST
        if now.weekday() != 6 or now.hour != 22 or now.minute < 30:
            return
        try:
            from engine.archer_frontier import get_latest_report, generate_archer_report
            latest = get_latest_report()
            if latest.get("report") and latest.get("generated_at"):
                return
            generate_archer_report()
        except Exception as e:
            console.log(f"[red]Archer frontier error: {e}")

    schedule.every(30).minutes.do(run_archer_frontier)       # Archer: Sunday 10:30 PM MST frontier scan

    # Season rotation: every Sunday at 11:59 PM MST
    def run_season_rotation():
        """Auto-rotate season every Sunday at 11:59 PM MST."""
        from datetime import datetime as _dt
        import pytz
        az = pytz.timezone("US/Arizona")
        now = _dt.now(az)
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
        now = _dt.now(az)
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
        now = _dt.now(az)
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
            deployed = report.get("deployed", [])
            best = report.get("best_strategy", {})
            if deployed:
                console.log(f"[bold green]Strategy Lab: Deployed {len(deployed)} parameter update(s). "
                            f"Best: {best.get('strategy_name', 'N/A')} (PF={best.get('avg_profit_factor', 0):.2f})")
            else:
                console.log(f"[green]Strategy Lab: Complete. Best: {best.get('strategy_name', 'N/A')} "
                            f"(PF={best.get('avg_profit_factor', 0):.2f}). No deployment needed.")
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
        now = _dt.now(az)
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
        now = _dt.now(az)
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
        now = _dt.now(az)
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

    # DayBlade pre-market warm: 6:15 AM AZ (9:15 AM ET) — warm Ollama models before market open
    def _dayblade_premarket_warm():
        import pytz as _pz
        _az = datetime.now(_pz.timezone("US/Arizona"))
        if _az.weekday() >= 5:
            return
        import requests as _rq
        for _mdl in ("qwen3.5:9b", "gemma3:4b"):
            try:
                _rq.post(
                    "http://127.0.0.1:11434/api/generate",
                    json={"model": _mdl, "prompt": "ready", "stream": False,
                          "think": False, "options": {"num_predict": 1}},
                    timeout=90,
                )
                console.log(f"[green][STARTUP] DayBlade pre-market warm: {_mdl} ✓")
            except Exception as _e:
                console.log(f"[yellow][STARTUP] DayBlade warm {_mdl} skipped: {_e}")
    schedule.every().day.at("06:15").do(_dayblade_premarket_warm)   # 6:15 AM AZ = 9:15 AM ET

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
        _az = datetime.now(_pz.timezone("US/Arizona"))

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

    schedule.every(15).minutes.do(run_holly_nightly_job)   # Holly Nightly: midnight AZ (3 AM ET)

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

    schedule.every(30).minutes.do(run_deep_scan_job)        # deep scan at 8AM ET
    schedule.every(30).minutes.do(run_strategy_rotation_job) # rotation at 5:30PM ET
    schedule.every().sunday.at("20:30").do(run_universe_refresh_job)  # Sunday 11:30PM AZ

    # ── Season 6: Proving Ground — 30-Day Sniper Mode Trial ────────────────────
    def run_proving_ground_scorecard():
        """Daily scorecard at 1:15 PM AZ (4:15 PM ET) — market close."""
        import pytz as _pz
        _az = datetime.now(_pz.timezone("US/Arizona"))
        if _az.weekday() >= 5:
            return  # weekdays only
        try:
            from engine.proving_ground import run_daily_scorecard
            result = run_daily_scorecard()
            console.log(
                f"[bold cyan]📊 Proving Ground Day {result['trial_day']}/30 | "
                f"Trades: {result['total_trades']} | WR: {result['win_rate']:.1f}% | "
                f"Sharpe: {result['sharpe']:.3f} | Go: {result['go_count']}/6"
            )
        except Exception as _e:
            console.log(f"[yellow]Proving Ground scorecard error: {_e}")

    def run_proving_ground_report():
        """Daily ntfy push at 1:30 PM AZ (4:30 PM ET)."""
        import pytz as _pz
        _az = datetime.now(_pz.timezone("US/Arizona"))
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

    schedule.every().day.at("13:15").do(run_proving_ground_scorecard)   # 4:15 PM ET
    schedule.every().day.at("13:30").do(run_proving_ground_report)      # 4:30 PM ET
    schedule.every().sunday.at("12:00").do(run_proving_ground_weekly)   # Sunday 3 PM ET

    # ── Rallies.ai hourly scraper (market hours Mon-Fri) ───────────────────────
    def run_rallies_scraper_job():
        """Hourly Rallies.ai scrape during market hours."""
        import pytz as _pz
        _az = datetime.now(_pz.timezone("US/Arizona"))
        if _az.weekday() >= 5:
            return
        # Market hours: 6:30 AM – 1:00 PM AZ
        if not (6 <= _az.hour < 13):
            return
        try:
            from engine.rallies_scraper import run_rallies_scrape
            run_rallies_scrape()
        except Exception as _e:
            console.log(f"[yellow]Rallies scraper error: {_e}")

    schedule.every(1).hours.do(run_rallies_scraper_job)    # Rallies.ai: hourly market hours

    # ── Season 6 Opening Bell — April 10, 2026 9:30 AM ET (6:30 AM AZ) ────────
    _s6_bell_state = [False]  # mutable container to avoid nonlocal

    def run_season6_opening_bell():
        if _s6_bell_state[0]:
            return
        import pytz as _pz
        from datetime import date as _date
        _az = datetime.now(_pz.timezone("US/Arizona"))
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
    _az_now = _dtm.now(_stz.timezone("US/Arizona"))
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
    console.log("[STARTUP] Ollama: warming qwen3.5:9b + gemma3:4b + 0xroyce/plutus in background")

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
        from config import WATCH_STOCKS
        from engine.market_data import get_all_prices
        console.log("[cyan]Warming up price cache (16 stocks)...")
        prices = get_all_prices(WATCH_STOCKS)
        console.log(f"[green]Price cache warm: {len(prices)}/16 stocks loaded")
    threading.Thread(target=_warmup, daemon=True).start()

    # Pre-load all required Ollama models — auto-pull if missing, then warm each
    def _warmup_ollama():
        import requests as _req, subprocess as _sp
        _REQUIRED_MODELS = [
            ("0xroyce/plutus",  False),   # T'Pol (dayblade-0dte) — finance-trained 0DTE brain
            ("qwen3.5:9b",      False),   # DayBlade Sulu + Chekov + main arena
            ("mistral:7b",      False),   # McCoy (ollama-plutus) — Mistral 7B scanner
            ("deepseek-r1:14b", False),   # Deep analysis
        ]
        # Check which models are installed
        try:
            _tags = _req.get("http://127.0.0.1:11434/api/tags", timeout=10).json()
            _installed = {m["name"].split(":")[0] for m in _tags.get("models", [])}
            _installed |= {m["name"] for m in _tags.get("models", [])}
        except Exception:
            _installed = set()
            console.log("[yellow][STARTUP] Ollama: API unreachable — skipping model warmup")
            return

        for _model, _think in _REQUIRED_MODELS:
            _base = _model.split(":")[0]
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
                _req.post(
                    "http://127.0.0.1:11434/api/generate",
                    json=_payload,
                    timeout=120,
                )
                console.log(f"[green][STARTUP] Ollama: {_model} warm ✓")
            except Exception as _e:
                console.log(f"[yellow][STARTUP] Ollama: {_model} warmup skipped: {_e}")

    threading.Thread(target=_warmup_ollama, daemon=True).start()

    # Run earnings check on startup
    run_earnings_check()

    # Riker XO: synthesize immediately on startup if market is open and stale
    def _riker_startup():
        try:
            from engine.risk_manager import RiskManager
            if not RiskManager.is_market_hours():
                return
            from engine.riker_xo import get_latest_recommendation, generate_riker_synthesis
            latest = get_latest_recommendation()
            if not latest.get("fresh"):
                console.log("[cyan]Riker XO: startup synthesis (no fresh recommendation)...")
                generate_riker_synthesis()
        except Exception as e:
            console.log(f"[red]Riker XO startup error: {e}")
    threading.Thread(target=_riker_startup, daemon=True).start()

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

    try:
        while True:
            try:
                schedule.run_pending()
            except Exception as _job_exc:
                console.log(f"[red]Scheduler job error (continuing): {_job_exc}")
            time.sleep(1)
    except KeyboardInterrupt:
        try:
            from engine.realtime_monitor import stop_monitor
            stop_monitor()
        except Exception:
            pass
        console.print("\n[yellow]Trader stopped.")