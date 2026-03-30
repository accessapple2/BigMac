import threading
import schedule
import time
import sqlite3
import os
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
        OllamaProvider(player_id="ollama-gemma27b", model="gemma3:27b", url=OLLAMA_URL, timeout=300),
        OllamaProvider(player_id="ollama-deepseek", model="deepseek-r1:14b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-qwen3", model="qwen3:8b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-kimi", model="kimi-k2.5:cloud", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-glm4", model="glm4:9b", url=OLLAMA_URL, timeout=180),
        OllamaProvider(player_id="ollama-plutus", model="0xroyce/plutus", url=OLLAMA_URL, timeout=300),
        # Lt. Sulu — DayBlade 2.0 (intraday day trader, free local compute)
        # Uses gemma3:4b (fast, 3.3GB) instead of qwen3:8b (5.2GB) — speed > depth for day trading
        OllamaProvider(player_id="dayblade-sulu", model="gemma3:4b", url=OLLAMA_URL, timeout=90),
    ]

    # Ensign Chekov — routed through Ollama qwen3:8b (was MLX)
    providers.append(OllamaProvider(
        player_id="mlx-qwen3", model="qwen3:8b", url=OLLAMA_URL, timeout=180,
    ))
    console.log("[green]Chekov (mlx-qwen3) → Ollama qwen3:8b")

    if OPENAI_API_KEY:
        from engine.providers.openai_provider import OpenAIProvider
        providers.append(OpenAIProvider(OPENAI_API_KEY, "claude-sonnet", OPENAI_CODEX_MODEL, "Codex Prime"))
        providers.append(OpenAIProvider(OPENAI_API_KEY, "claude-haiku", OPENAI_CODEX_MINI_MODEL, "Codex Scout"))
        providers.append(OpenAIProvider(OPENAI_API_KEY, "gpt-4o", "gpt-4o", "GPT-4o")) 
        providers.append(OpenAIProvider(OPENAI_API_KEY, "gpt-o3", "o3", "GPT-o3"))  

    # Gemini players — redirected to local Ollama gemma3:4b (zero Google API calls)
    from engine.providers.ollama_provider import OllamaProvider
    providers.append(OllamaProvider("gemini-2.5-pro", "gemma3:4b"))
    providers.append(OllamaProvider("gemini-2.5-flash", "gemma3:4b"))
    providers.append(OllamaProvider("options-sosnoff", "gemma3:4b"))
    providers.append(OllamaProvider("energy-arnold", "gemma3:4b"))
    # Mr. Anderson — CrewAI collective / The One (gemma3:4b for crew synthesis)
    providers.append(OllamaProvider("super-agent", "gemma3:4b"))
    # Mr. Dalio — All Weather portfolio: Gemini Flash → gemma3:27b → gemma3:4b fallback chain
    from engine.providers.dalio_provider import DalioFallbackProvider
    providers.append(DalioFallbackProvider(
        api_key=GEMINI_API_KEY,
        player_id="dalio-metals",
        url=OLLAMA_URL,
    ))

    if GROK_API_KEY:
        from engine.providers.grok_provider import GrokProvider
        providers.append(GrokProvider(GROK_API_KEY, "grok-3", "grok-4-1-fast-reasoning", "Grok 3"))
        providers.append(GrokProvider(GROK_API_KEY, "grok-4", "grok-4.20-0309-reasoning", "Grok 4"))

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
    provider = OllamaProvider(player_id="dayblade-0dte", model=OLLAMA_MODEL, url=OLLAMA_URL)
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
    global arena, _news_counter, _last_scan_time
    import time as _time

    # Prevent scan stacking — skip if previous scan still running
    if not _scan_lock.acquire(blocking=False):
        console.log("[yellow]Scan skipped — previous scan still running")
        return

    try:
        interval = _get_scan_interval()
        if interval is None:
            return  # Market closed

        # Enforce dynamic cooldown
        now = _time.time()
        if now - _last_scan_time < interval:
            return
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

        console.log(f"[cyan]Market scan triggered (interval={interval}s)...")
        try:
            arena.run_scan(WATCH_STOCKS)
        except Exception as e:
            console.log(f"[red]Scan error: {e}")

        # Trigger War Room after every 3rd scan cycle (~9 min intervals)
        if _news_counter % 3 == 0:
            try:
                run_war_room()
            except Exception as e:
                console.log(f"[red]War Room post-scan error: {e}")
    finally:
        _scan_lock.release()


def run_dayblade():
    global dayblade
    if dayblade is None:
        dayblade = initialize_dayblade()
    try:
        # DayBlade handles its own power hour throttling internally (15s during 2-3:30 PM)
        dayblade.run_scan()
    except Exception as e:
        console.log(f"[red]DayBlade error: {e}")


def run_ma_regime_update():
    """Refresh 8/21 MA cross regime every 15 minutes, log any regime change."""
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
        refresh_gex_cache()
    except Exception as e:
        console.log(f"[red]GEX refresh error: {e}")


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

    # Slower interval during post-market (5 min) to reduce DB contention
    now = _time.time()
    if session in ("pre_market", "post_market"):
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
    """Discovery Scanner: find new opportunities outside watchlist."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    try:
        from engine.discovery_scanner import run_discovery_scan as _run_ds
        discoveries = _run_ds()
        if discoveries:
            console.log(f"[magenta]Discovery: {len(discoveries)} opportunities found")
    except Exception as e:
        console.log(f"[red]Discovery scan error: {e}")


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
    import uvicorn
    try:
        from dashboard.app import app
        _dashboard_started.set()
        uvicorn.run(app, host="127.0.0.1", port=8080, log_level="warning")
    except Exception as exc:
        _dashboard_error = exc
        console.log(f"[red]Dashboard startup error: {exc}")


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
                return
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
        except Exception as e:
            console.log(f"[red]Sulu EOD auto-close error: {e}")


if __name__ == "__main__":
    # Ensure DB exists
    from setup_db import setup
    setup()

    # Init Telegram
    from engine.telegram_alerts import init_telegram
    init_telegram()

    console.print(Panel.fit(
        "[bold green]TradeMinds[/bold green]\n[dim]Season 3 — AI Arena + DayBlade Options[/dim]",
        border_style="green"
    ))

    # Scanner ticks every 30s; run_scanner enforces dynamic cooldown internally
    schedule.every(30).seconds.do(run_scanner)
    schedule.every(15).seconds.do(run_dayblade)         # DayBlade S2: every 15s (power hour), throttled internally
    schedule.every(15).minutes.do(run_ma_regime_update)  # 8/21 MA Cross Regime: every 15 min
    schedule.every(5).minutes.do(run_vix_check)          # VIX: every 5 min
    schedule.every(1).hours.do(run_earnings_check)       # Earnings: hourly
    schedule.every(5).minutes.do(run_daily_summary)      # Daily summary: checks every 5 min, sends once at close
    schedule.every(5).minutes.do(run_journal)             # AI journal: checks every 5 min, writes once at close
    schedule.every(15).minutes.do(run_gex_refresh)        # GEX (CBOE): every 15 min during market hours
    schedule.every(5).minutes.do(run_alpaca_gex_refresh)  # GEX (Alpaca): 4x/day at 9:00/9:35/12:00/15:00 ET
    schedule.every(3).minutes.do(run_war_room)             # War Room: every 3 min during market hours (trash talk mode)
    schedule.every(30).minutes.do(run_autopilot)           # Autopilot: every 30 min
    schedule.every(10).minutes.do(run_whisper)             # Whisper Network: every 10 min
    schedule.every(5).minutes.do(run_strength_scan)        # Strength Scanner: every 5 min
    schedule.every(1).hours.do(run_strategy_race)           # Strategy Race: hourly update
    schedule.every(5).minutes.do(run_weekly_picks)          # Weekly Picks: checks every 5 min, sends Sunday 6PM ET
    schedule.every(5).minutes.do(run_cross_asset_check)    # Cross-Asset: every 5 min
    schedule.every(15).minutes.do(run_flow_lean)            # Flow Lean: every 15 min (options premium directional bias)
    schedule.every(5).minutes.do(run_cto_advisory)          # CTO Advisory: checks every 5 min, fires 4x daily (pre_market, post_open, pre_close, post_close)
    schedule.every(5).minutes.do(run_weekly_elimination)    # Weekly Elimination: checks every 5 min, fires Friday post_market only
    schedule.every(15).minutes.do(run_skew_check)          # Skew Monitor: every 15 min
    schedule.every(1).hours.do(run_fundamental_scan)       # Fundamentals: hourly refresh
    schedule.every(30).minutes.do(run_trend_forecast)       # Trend Forecast: every 30 min
    schedule.every(30).minutes.do(run_strategy_presets)      # Strategy Presets: every 30 min
    schedule.every(30).minutes.do(run_discovery_scan)        # Discovery Scanner: every 30 min (find new tickers)
    schedule.every(30).minutes.do(run_sma_scan)               # 200 SMA Filter: checks every 30 min, runs every 4 hours
    schedule.every(1).hours.do(run_impulse_check)              # Impulse Detector: hourly during market hours
    schedule.every(2).hours.do(run_imbalance_scan)             # Imbalance Zones: every 2 hours (zones are stable)
    schedule.every(30).minutes.do(run_theta_scan)              # Theta Scanner: checks every 30 min, runs every 4 hours
    schedule.every(5).minutes.do(run_gap_scan)                 # Gap Scanner: checks every 5 min, fires once at market open
    schedule.every(5).minutes.do(run_gap_fill_check)           # Gap Fill Tracker: every 5 min during market hours
    # Capitol Trades Fund — Congress copycat scan (daily at market open, 9:35 AM ET)
    from engine.capitol_fund import run_capitol_scan
    schedule.every(5).minutes.do(run_capitol_scan)
    schedule.every(15).minutes.do(run_cost_monitor)         # Cost Monitor: every 15 min (budget alert, auto-pause)
    schedule.every(30).minutes.do(run_universe_scan)         # Universe Scanner: checks every 30 min, runs 9 PM MST (12 AM ET)
    schedule.every(30).minutes.do(run_strategy_scan)         # Strategy Scan: checks every 30 min, runs 10 PM MST (1 AM ET)
    schedule.every(5).minutes.do(run_chekov_stoploss)        # Chekov SL/TP: every 5 min, check positions vs stop/target
    schedule.every(30).minutes.do(run_metals_commentary)     # Dalio Metals: checks every 30 min, runs 7 AM MST only
    schedule.every(15).minutes.do(run_premarket_gaps)         # Pre-market gaps: checks every 15 min, fires 1 AM MST (4 AM ET)
    schedule.every(1).minutes.do(run_sulu_autoclose)          # Lt. Sulu EOD: auto-close all positions at 12:45 PM MST (3:45 PM ET)
    from engine.recovery_protocol import run_recovery_scan
    schedule.every(15).minutes.do(run_recovery_scan)           # Recovery Protocol: checks every 15 min during market hours
    from engine.wheel_strategy import run_wheel_scan, check_wheel_assignments
    schedule.every(15).minutes.do(run_wheel_scan)              # Wheel Strategy: scan for put-selling opportunities every 15 min
    schedule.every(1).hours.do(check_wheel_assignments)        # Wheel Strategy: check for option assignments hourly


    # Webull Portfolio Auto-Sync: every 30 min during market hours
    def run_webull_sync():
        """Auto-sync Steve's Webull portfolio positions to DB."""
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = _dt.now(az)
        # Mon-Fri only, 6 AM - 2 PM MST (9 AM - 5 PM ET)
        if now.weekday() >= 5:
            return
        if now.hour < 6 or now.hour > 14:
            return
        try:
            from engine.webull_client import sync_positions_to_db
            sync_positions_to_db()
        except Exception as e:
            console.log(f"[red]Webull auto-sync error: {e}")

    schedule.every(5).minutes.do(run_webull_sync)

    # Alpaca Paper Position Sync: every 5 min — updates current_price + unrealized_pnl
    def run_alpaca_position_sync():
        """Keep portfolio_positions current_price in sync with live Alpaca Paper data."""
        try:
            from shared.alpaca_sync import sync_positions_from_alpaca
            result = sync_positions_from_alpaca()
            if result.get("synced", 0) > 0:
                console.log(f"[dim cyan]Alpaca position sync: updated {result['synced']} position(s) — {result.get('alpaca_tickers', [])}[/dim cyan]")
        except Exception as e:
            console.log(f"[dim]Alpaca position sync error: {e}[/dim]")

    schedule.every(5).minutes.do(run_alpaca_position_sync)

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

    schedule.every(5).minutes.do(run_season_rotation)        # Season rotation: checks every 5 min, fires Sunday 11:59 PM MST

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

    schedule.every(5).minutes.do(run_daily_review)  # Checks every 5 min, fires 1:15 PM MST Mon-Fri

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

    # Warm up price cache in background so dashboard loads fast
    def _warmup():
        from config import WATCH_STOCKS
        from engine.market_data import get_all_prices
        console.log("[cyan]Warming up price cache (16 stocks)...")
        prices = get_all_prices(WATCH_STOCKS)
        console.log(f"[green]Price cache warm: {len(prices)}/16 stocks loaded")
    threading.Thread(target=_warmup, daemon=True).start()

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

    # Start realtime price monitor (Finnhub WebSocket or polling fallback)
    try:
        from engine.realtime_monitor import start_monitor
        start_monitor()
        console.log("[cyan]Realtime Monitor armed (Finnhub WebSocket, 5-min spike detection, instant Gemini Flash scans)")
    except Exception as e:
        console.log(f"[yellow]Realtime Monitor failed to start: {e}")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        try:
            from engine.realtime_monitor import stop_monitor
            stop_monitor()
        except Exception:
            pass
        console.print("\n[yellow]Trader stopped.")
from agents.momentum import MomentumAgent
from engine.agent_manager import AgentManager

# test data (so it runs no matter what)
market_data = {
    "AAPL": [180, 181, 182, 183, 187],
    "TSLA": [250, 249, 248, 247, 246],
}

agents = [MomentumAgent()]
manager = AgentManager(agents)

signals = manager.run(market_data)

print("SIGNALS:", signals)