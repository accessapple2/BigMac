from __future__ import annotations
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from engine.providers.base import AIProvider, TradeDecision
from engine.paper_trader import (
    get_portfolio, get_portfolio_with_pnl, execute_signal,
    record_portfolio_snapshot, save_signal, save_equity_snapshot,
    update_signal_status, _last_rejection,
)
from engine.market_data import get_stock_price, get_technical_indicators
from engine.news_fetcher import get_news_for_symbol
from engine.risk_manager import RiskManager
from engine.ai_chat import generate_chat_message, save_chat_message
from engine.telegram_alerts import alert_trade, alert_stop_loss

console = Console()

# === OPT 2/3: CYCLE-SCOPED CACHES ===
# Premarket gaps: same for all agents — cache 5 min to avoid N localhost HTTP calls/cycle.
_premarket_gap_cache: dict = {"ts": 0.0, "gaps": []}
_PREMARKET_GAP_TTL = 300  # seconds

# === SCAN SCHEDULING ===
# Paid models (API cost) scan 3x/day. Gemma3 4B scans 5x/day (strategic deep scans).
# Qwen3 8B and Plutus 9B scan continuously every 120-180s (free, always running).
# grok-4 removed — Spock now runs on ollama/deepseek-r1:7b (free). No API cost.
PAID_MODEL_IDS = {"cto-grok42"}
STRATEGIC_SCAN_MODEL_IDS = {"ollama-local"}  # Gemma3 4B: fewer but deeper scans

# Models that run independently — not gated by tier1_has_signal (they have their own signal sources)
INDEPENDENT_TIER2_IDS = {"ollama-llama"}  # Uhura (Groq): runs every cycle without tier1 gate

# === TIERED SCANNING (Cost Reduction v3) ===
# Tier 1 (free, always scan): local Ollama models
# Tier 2 (cloud, signal-triggered only): expensive API models
# If Tier 1 finds no actionable signal, Tier 2 is skipped entirely.
TIER2_SIGNAL_THRESHOLD = 0.6  # Minimum confidence from Tier 1 to escalate

_PAID_SCAN_WINDOWS_ET = [
    (9, 35),   # Post-open
    (12, 0),   # Midday
    (15, 50),  # Pre-close
]
# Spock (grok-4) reduced from 3x to 2x/day — was overtrading at 513 trades/month
_SPOCK_SCAN_WINDOWS_ET = [
    (9, 35),   # Post-open — catch the setup
    (15, 50),  # Pre-close — final decision only
]
_STRATEGIC_SCAN_WINDOWS_ET = [
    (9, 35),   # Post-open — catch opening moves
    (12, 0),   # Midday — reassess
    (15, 45),  # Pre-close — final decisions
    # Reduced from 5x to 3x/day — Geordi was overtrading (2,795 trades/month)
]
_SCAN_WINDOW_MINUTES = 10  # ±10 min window around each time
_last_paid_scan_window: str | None = None
_last_spock_scan_window: str | None = None
_last_strategic_scan_window: str | None = None

# Spock cycle counter — scans every 3rd cycle only (1/3 frequency = ~66% cost reduction)
_spock_cycle_count: int = 0

_RECOVERY_REJECTION_PREFIX = "Recovery mode:"


def _dalio_recovery_state():
    import sqlite3

    conn = sqlite3.connect("data/trader.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        event = conn.execute(
            """
            SELECT created_at, amount
            FROM player_funding_events
            WHERE player_id='dalio-metals' AND event_type='recapitalization'
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if not event:
            return None

        legacy_open = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM positions
            WHERE player_id='dalio-metals' AND datetime(opened_at) < datetime(?)
            """,
            (event["created_at"],),
        ).fetchone()["cnt"]
        recovery_open = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM positions
            WHERE player_id='dalio-metals' AND datetime(opened_at) >= datetime(?)
            """,
            (event["created_at"],),
        ).fetchone()["cnt"]
        recent_errors = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM signals
            WHERE player_id='dalio-metals'
              AND datetime(created_at) >= datetime('now', '-30 minutes')
              AND lower(reasoning) LIKE '%error:%'
            """
        ).fetchone()["cnt"]

        return {
            "active": True,
            "event_at": event["created_at"],
            "amount": float(event["amount"] or 0),
            "legacy_open": int(legacy_open or 0),
            "recovery_open": int(recovery_open or 0),
            "provider_health_degraded": int(recent_errors or 0) >= 3,
        }
    finally:
        conn.close()


def _is_losing_position(position: dict, current_price: float) -> bool:
    avg_price = float(position.get("avg_price") or 0)
    qty = float(position.get("qty") or 0)
    if avg_price <= 0 or qty == 0:
        return False
    if qty > 0:
        return current_price < avg_price
    return current_price > avg_price


def _check_scan_window(windows: list, last_key_attr: str) -> bool:
    """Generic scan window checker. Returns True if within a window and haven't scanned it yet."""
    from datetime import datetime
    import pytz

    et = pytz.timezone("US/Eastern")
    now = datetime.now(et)
    now_minutes = now.hour * 60 + now.minute

    current_state = globals().get(last_key_attr)

    for hour, minute in windows:
        window_center = hour * 60 + minute
        if abs(now_minutes - window_center) <= _SCAN_WINDOW_MINUTES:
            window_key = f"{hour}:{minute}"
            if current_state == window_key:
                return False
            globals()[last_key_attr] = window_key
            return True

    globals()[last_key_attr] = None
    return False


def _is_paid_scan_window() -> bool:
    return _check_scan_window(_PAID_SCAN_WINDOWS_ET, "_last_paid_scan_window")


def _is_spock_scan_window() -> bool:
    """Spock scans 2x/day only (reduced from 3x — was overtrading)."""
    return _check_scan_window(_SPOCK_SCAN_WINDOWS_ET, "_last_spock_scan_window")


def _is_strategic_scan_window() -> bool:
    return _check_scan_window(_STRATEGIC_SCAN_WINDOWS_ET, "_last_strategic_scan_window")


def _check_cost_budget() -> bool:
    """Check if daily cost budget allows cloud model scanning.

    Returns True if cloud scanning is allowed, False if budget exceeded.
    """
    try:
        from engine.cost_tracker import get_total_daily_cost
        from config import DAILY_API_BUDGET, DAILY_COST_WARNING
        today_cost = get_total_daily_cost()
        if today_cost >= DAILY_API_BUDGET:
            console.log(f"[bold red]COST CIRCUIT BREAKER: ${today_cost:.2f} >= ${DAILY_API_BUDGET:.2f} — cloud scanning paused")
            return False
        if today_cost >= DAILY_COST_WARNING:
            console.log(f"[yellow]COST WARNING: ${today_cost:.2f} / ${DAILY_API_BUDGET:.2f}")
        return True
    except Exception:
        return True  # If check fails, allow scanning


class Arena:
    def __init__(self, providers: list, risk_manager: RiskManager = None):
        self.providers: dict[str, AIProvider] = {p.player_id: p for p in providers}
        self.risk = risk_manager or RiskManager()
        self._indicators_cache: dict[str, dict] = {}
        self._equity_counter: int = 0

        # Register post-sell callback for AI trade grading
        from engine.paper_trader import register_sell_callback
        register_sell_callback(self._grade_trade)

    def _grade_trade(self, player_id: str, symbol: str, entry_price: float,
                     exit_price: float, pnl: float, reasoning: str):
        """Callback: have the AI grade its own closed trade."""
        provider = self.providers.get(player_id)
        if not provider:
            return
        try:
            from engine.ai_journal import grade_closed_trade
            grade_closed_trade(provider, player_id, symbol, entry_price,
                               exit_price, pnl, reasoning)
        except Exception as e:
            console.log(f"[red]Trade grade callback error for {player_id}: {e}")

    def run_scan(self, symbols: list, force: bool = False, player_ids: set | None = None):
        """Run one full scan cycle: fetch prices, indicators, all AIs analyze, execute trades.

        player_ids — optional allowlist of player IDs to run this cycle.
        If None, all active providers run (legacy behaviour).
        """
        session = self.risk.is_market_hours()
        if not session and not force:
            console.log("[yellow]Market closed (weekends & 6PM-2AM MT). Skipping scan.")
            return
        session_label = session if isinstance(session, str) else ("FORCED" if force else "market")
        console.log(f"[cyan]Session: {session_label.upper()}")

        # Check global pause
        import sqlite3 as _sqlite3
        try:
            _c = _sqlite3.connect("data/trader.db", check_same_thread=False, timeout=30)
            _c.row_factory = _sqlite3.Row
            pause_all = _c.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
            _c.close()
            if pause_all and pause_all[0] == '1':
                console.log("[yellow]All scanning PAUSED via Model Control Panel. Skipping scan.")
                return
        except Exception:
            pass

        # Increment Spock's cycle counter (every 3rd cycle he actually scans)
        global _spock_cycle_count
        _spock_cycle_count += 1

        # 0.5 Merge high-conviction discoveries into scan symbols
        try:
            from engine.discovery_scanner import get_cached_discoveries
            discoveries = get_cached_discoveries()
            discovery_syms = [d["symbol"] for d in discoveries if d.get("score", 0) >= 50 and d["symbol"] not in symbols]
            if discovery_syms:
                symbols = list(symbols) + discovery_syms[:5]  # Top 5 discoveries
                console.log(f"[magenta]Discovery adds: {', '.join(discovery_syms[:5])}")
        except Exception:
            pass

        # 1. Fetch all prices in parallel (shared across AIs)
        from engine.market_data import get_all_prices
        console.log("[cyan]Fetching market data (parallel)...")
        prices = get_all_prices(symbols)
        for symbol, data in prices.items():
            console.log(f"[dim]{symbol}: ${data['price']} [{data.get('source','?')}][/dim]")
        failed = set(symbols) - set(prices.keys())
        for symbol in failed:
            console.log(f"[red]{symbol}: price fetch failed")

        if not prices:
            console.log("[red]No price data available")
            return

        # Check active signal tracker against current prices
        try:
            from engine.signal_tracker import check_active_signals, check_reentry_opportunities
            result = check_active_signals(prices)
            if result["hits"] or result["expires"]:
                console.log(f"[cyan]Signal Tracker: {result['hits']} hit target, {result['expires']} expired")
            check_reentry_opportunities(prices)
        except Exception as e:
            console.log(f"[red]Signal tracker check error: {e}")

        # 1.5 Expire options that have passed their expiry date
        try:
            from engine.paper_trader import expire_options as _expire_opts
            _exp_result = _expire_opts(prices)
            if _exp_result.get("expired", 0) > 0:
                console.log(f"[yellow]Options expiry: closed {_exp_result['expired']} position(s)")
        except Exception as _exp_e:
            console.log(f"[red]Options expiry check error: {_exp_e}")

        # 1.6 Auto-exit options at 50% TP / 50% SL / 21 DTE time stop
        try:
            from engine.paper_trader import check_option_exits as _check_opt_exits
            _opt_exit_result = _check_opt_exits(prices)
            if _opt_exit_result.get("auto_exited", 0) > 0:
                console.log(f"[cyan]Option auto-exits: {_opt_exit_result['auto_exited']} position(s) closed")
        except Exception as _opt_exit_e:
            console.log(f"[red]Option exit check error: {_opt_exit_e}")

        # 2. Fetch technical indicators once (shared across AIs)
        console.log("[cyan]Computing technical indicators...")
        indicators = {}
        for symbol in prices:
            ind = get_technical_indicators(symbol)
            if ind:
                indicators[symbol] = ind
                rsi = ind.get("rsi", "?")
                macd_h = ind.get("macd_histogram", "?")
                vol_r = ind.get("volume_ratio", "?")
                console.log(f"[dim]{symbol}: RSI={rsi} MACD_H={macd_h} Vol={vol_r}x[/dim]")
        self._indicators_cache = indicators

        # 3. Fetch latest news per symbol from DB (already fetched by main.py cycle)
        news_by_symbol = {}
        for symbol in prices:
            news_by_symbol[symbol] = get_news_for_symbol(symbol, limit=5)

        # 4. Run AI players in batches of 4 to limit RAM usage
        import _sqlite3
        paused_ids: set = set()
        try:
            _paused_conn = _sqlite3.connect("data/trader.db", check_same_thread=False, timeout=10)
            _paused_rows = _paused_conn.execute(
                "SELECT id FROM ai_players WHERE is_paused=1 OR is_active=0"
            ).fetchall()
            _paused_conn.close()
            paused_ids = {r[0] for r in _paused_rows}
        except Exception as _e:
            console.log(f"[yellow]Paused IDs query failed (using empty set): {_e}")

        # Separate fast (API) and slow (Ollama/MLX) providers so slow models don't block API models
        # Paid models scan 3x/day, Gemma3 4B scans 5x/day, free models scan continuously
        from engine.providers.ollama_provider import OllamaProvider as _OllamaP
        try:
            from engine.providers.mlx_provider import MLXProvider as _MlxP
        except ImportError:
            _MlxP = None
        _is_local = lambda p: isinstance(p, _OllamaP) or (_MlxP and isinstance(p, _MlxP))

        # Build fallback providers for paused players — free local Ollama brain,
        # same player_id so trading history/personality/portfolio stay intact.
        from engine.fallback import is_fallbacks_enabled, get_fallback_model, set_player_fallback_state
        from config import OLLAMA_URL as _OLLAMA_URL
        fallback_providers: dict = {}
        if is_fallbacks_enabled():
            for _fb_pid in list(paused_ids):
                if _fb_pid in self.providers:
                    _fb_model = get_fallback_model(_fb_pid)
                    if _fb_model:
                        _fb_prov = _OllamaP(player_id=_fb_pid, model=_fb_model,
                                            url=_OLLAMA_URL, timeout=180)
                        fallback_providers[_fb_pid] = _fb_prov
                        set_player_fallback_state(_fb_pid, True)
                        console.log(f"[yellow]FALLBACK: {_fb_pid} → {_fb_model} (free)")
        # Clear fallback state for non-paused players
        for _pid in self.providers:
            if _pid not in paused_ids:
                set_player_fallback_state(_pid, False)

        paid_window_open = _is_paid_scan_window()
        spock_window_open = _is_spock_scan_window()
        strategic_window_open = _is_strategic_scan_window()
        api_providers = []
        skipped_paid = []
        for pid, prov in self.providers.items():
            if pid in paused_ids or _is_local(prov):
                continue
            # Spock (grok-4) uses reduced 2x/day schedule + every-3rd-cycle gate
            if pid == "grok-4":
                if not spock_window_open or _spock_cycle_count % 3 != 0:
                    skipped_paid.append(pid)
                    continue
            elif pid in PAID_MODEL_IDS and not paid_window_open:
                skipped_paid.append(pid)
                continue
            api_providers.append((pid, prov))

        ollama_providers = []
        mlx_providers = []
        skipped_strategic = []
        for pid, prov in self.providers.items():
            if pid in paused_ids or not _is_local(prov):
                continue
            # MLX providers run separately (no Ollama model swap needed)
            if _MlxP and isinstance(prov, _MlxP):
                mlx_providers.append((pid, prov))
                continue
            if pid in STRATEGIC_SCAN_MODEL_IDS and not strategic_window_open:
                skipped_strategic.append(pid)
                continue
            ollama_providers.append((pid, prov))

        # Run Ollama FIRST (free), then API models only if signal detected + budget allows
        if skipped_paid:
            console.log(f"[yellow]Paid models waiting: {', '.join(skipped_paid)} (3x/day)")
        if skipped_strategic:
            console.log(f"[yellow]Strategic models waiting: {', '.join(skipped_strategic)} (5x/day: 9:35, 10:30, 12:00, 2:00, 3:45 ET)")

        # === TIERED SCANNING v4: Model-grouped Ollama + parallel API ===
        # - Independent API models (Uhura/Groq, Anderson/CrewAI) fire immediately in background threads
        # - Ollama models grouped by model_id → load once, run all, unload once (no swap overhead)
        # - Gated API models run after Ollama only if signal found
        # - Result: ~2-3 min cycle vs 15-20 min before
        # Inject fallback providers into Tier 1 Ollama batch
        for _fb_pid, _fb_prov in fallback_providers.items():
            if not any(p == _fb_pid for p, _ in ollama_providers):
                ollama_providers.append((_fb_pid, _fb_prov))

        # Apply scan-tier allowlist (set by main.py tier scheduler)
        if player_ids is not None:
            ollama_providers = [(pid, prov) for pid, prov in ollama_providers if pid in player_ids]
            api_providers    = [(pid, prov) for pid, prov in api_providers    if pid in player_ids]
            mlx_providers    = [(pid, prov) for pid, prov in mlx_providers    if pid in player_ids]

        console.log(f"[cyan]TIER 1 (free): {len(ollama_providers)} Ollama + {len(mlx_providers)} MLX | TIER 2 (paid): {len(api_providers)} API ({len(paused_ids)} paused/{len(fallback_providers)} fallback, {len(skipped_paid)} paid-waiting)")

        # Separate independent (always-run) vs gated API providers upfront
        independent_providers = [(pid, prov) for pid, prov in api_providers if pid in INDEPENDENT_TIER2_IDS]
        gated_api_providers = [(pid, prov) for pid, prov in api_providers if pid not in INDEPENDENT_TIER2_IDS]

        # Fire independent API providers NOW in background (runs parallel to Ollama)
        _api_futures_executor = None
        _api_futures = {}
        if independent_providers and _check_cost_budget():
            _api_futures_executor = ThreadPoolExecutor(max_workers=len(independent_providers))
            for pid, provider in independent_providers:
                console.log(f"[cyan]ASYNC: {pid} — starting in background (parallel to Ollama)")
                _api_futures[_api_futures_executor.submit(
                    self._run_player, pid, provider, prices, indicators, news_by_symbol
                )] = pid

        # --- Phase 1: Run Ollama models grouped by model_id to minimize load/unload swaps ---
        tier1_has_signal = False
        if ollama_providers:
            import time as _time
            import requests as _requests
            from collections import defaultdict as _defaultdict

            # Sort by model_id so same-model agents are consecutive.
            # Ollama loads each model once, runs all agents in the group, then unloads.
            _MODEL_RUN_ORDER = {
                "deepseek-r1:7b":   0,  # Spock (grok-4) runs first — small group, gets signals early
                "qwen3.5:9b":       1,  # largest group — stays loaded for all 10 players after Spock
                "qwen3:14b":        2,
                "gemma3:4b":        3,
                "qwen2.5-coder:7b": 4,
                "0xroyce/plutus":   5,
                "mistral:7b":       6,  # McCoy / Plutus
                "mistral-small":    7,
            }
            # Opt 6 — Agent priority within model group (lower = runs first)
            _AGENT_PRIORITY = {
                "dayblade-sulu":  0,   # S6.3 Iron Condor King — primary options trader
                "grok-4":         1,   # Spock — pure data signal leader
                "super-agent":    2,   # Anderson — crewai collective
                "ollama-coder":   3,   # Data — code/technicals specialist
                "mlx-qwen3":      4,   # Chekov — navigator
                "ollama-plutus":  5,   # McCoy — options specialist
            }
            ollama_providers.sort(key=lambda x: (
                _MODEL_RUN_ORDER.get(x[1].model_id, 99),
                _AGENT_PRIORITY.get(x[0], 50),
                x[0],
            ))

            # Group providers by model_id so we load each model exactly once
            model_groups = _defaultdict(list)
            for pid, prov in ollama_providers:
                model_groups[prov.model_id].append((pid, prov))

            console.log(f"[cyan]TIER 1: {len(ollama_providers)} models in {len(model_groups)} model group(s): {', '.join(model_groups.keys())}")
            # Opt 1 — Lazy loading: skip clear-slate blast.
            # keep_alive=60s in every call auto-unloads after 60s of inactivity.
            # Scan intervals are 90s+ so models are already unloaded before each cycle.
            # Sending N unload requests + sleep(3) was pure overhead.

            # Flush brain_context cache so each cycle gets fresh intelligence
            try:
                from engine.brain_context import invalidate_cache as _bc_invalidate
                _bc_invalidate()
            except Exception:
                pass

            # Run each model group: load once → scan all providers → unload once
            from engine.ollama_watchdog import get_watchdog as _get_watchdog
            _wd = _get_watchdog()
            _cycle_total = 0
            _cycle_responded = 0
            _cycle_timeouts_by_model: dict = {}
            _cycle_response_times: list = []

            for model_id, group in model_groups.items():
                # Check pause_all before loading — skip if backtest is in progress
                try:
                    import sqlite3 as _sq_pause
                    _pc = _sq_pause.connect("data/trader.db", check_same_thread=False, timeout=5)
                    _prow = _pc.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
                    _pc.close()
                    if _prow and _prow[0] == '1':
                        console.log(f"[yellow]⏸ Model load skipped — pause_all=1 (backtest in progress)")
                        continue
                except Exception:
                    pass  # If check fails, proceed normally

                if _wd.is_skipped(model_id):
                    console.log(f"[yellow]WATCHDOG: {model_id} in skip window — bypassing this cycle")
                    continue

                pids_in_group = [pid for pid, _ in group]
                console.log(f"[cyan]TIER 1: loading {model_id} for [{', '.join(pids_in_group)}]...")

                for pid, provider in group:
                    console.log(f"[cyan]TIER 1: scanning {pid} ({model_id})...")
                    _cycle_total += 1
                    _t0 = _time.monotonic()
                    try:
                        self._run_player(pid, provider, prices, indicators, news_by_symbol)
                        _cycle_responded += 1
                        _wd.record_success(model_id)
                        _cycle_response_times.append(_time.monotonic() - _t0)
                    except Exception as e:
                        _elapsed = _time.monotonic() - _t0
                        _is_to = isinstance(e, TimeoutError) or "timed out" in str(e).lower()
                        if _is_to:
                            _cycle_timeouts_by_model[model_id] = (
                                _cycle_timeouts_by_model.get(model_id, 0) + 1
                            )
                            _action = _wd.record_timeout(model_id)
                            if _action == "recycle":
                                console.log(f"[yellow]WATCHDOG: recycling {model_id} after consecutive timeouts")
                                _wd.recycle_model(model_id)
                            console.log(f"[red]{pid} TIMEOUT ({_elapsed:.0f}s): {e}")
                        else:
                            console.log(f"[red]{pid} failed: {e}")
                    # No sleep between same-model providers — model stays loaded

                # Unload after all providers in this group are done
                console.log(f"[cyan]TIER 1: unloading {model_id}...")
                try:
                    _requests.post("http://localhost:11434/api/generate",
                                   json={"model": model_id, "keep_alive": 0}, timeout=10)
                except Exception:
                    pass
                _time.sleep(10)  # Stagger between model groups — gives Ollama time to fully unload and free VRAM before next group loads

            # --- Scan health report + circuit breaker (runs once per full Ollama cycle) ---
            _avg_rt = (
                round(sum(_cycle_response_times) / len(_cycle_response_times), 1)
                if _cycle_response_times else None
            )
            _wd.record_scan_health(
                _cycle_total, _cycle_responded, _cycle_timeouts_by_model, _avg_rt
            )
            _wd.check_and_fire_circuit_breaker(
                _cycle_total, sum(_cycle_timeouts_by_model.values())
            )

            # Check if any Ollama model found an actionable signal
            try:
                import sqlite3 as _sq
                _sc = _sq.connect("data/trader.db", check_same_thread=False, timeout=10)
                _sc.row_factory = _sq.Row
                recent = _sc.execute(
                    "SELECT signal, confidence FROM signals "
                    "WHERE created_at > datetime('now', '-10 minutes') "
                    "AND signal IN ('BUY', 'SELL') "
                    "AND confidence >= ? LIMIT 1",
                    (TIER2_SIGNAL_THRESHOLD,)
                ).fetchone()
                _sc.close()
                tier1_has_signal = recent is not None
            except Exception:
                pass

            if tier1_has_signal:
                console.log(f"[bold green]TIER 1 SIGNAL DETECTED — escalating to cloud models")
            else:
                console.log(f"[dim]TIER 1 clear — no actionable signals, skipping cloud models this cycle [COST SAVED]")

        # --- Phase 1b: Run MLX models (free, deep analysis, no Ollama model swap) ---
        if mlx_providers:
            for pid, provider in mlx_providers:
                console.log(f"[cyan]TIER 1 MLX: {pid} ({provider.model_id}) — deep analysis...")
                try:
                    self._run_player(pid, provider, prices, indicators, news_by_symbol)
                except Exception as e:
                    console.log(f"[red]{pid} failed: {e}")

        # --- Collect results from background API threads (started before Ollama) ---
        if _api_futures:
            for future in as_completed(_api_futures, timeout=120):
                pid = _api_futures[future]
                try:
                    future.result(timeout=5)
                    console.log(f"[green]ASYNC: {pid} completed")
                except Exception as e:
                    console.log(f"[red]ASYNC: {pid} failed: {e}")
            if _api_futures_executor:
                _api_futures_executor.shutdown(wait=False)

        # --- Phase 2: Gated API models — only run if Tier 1 found signal + budget allows ---
        api_providers = gated_api_providers
        if api_providers and tier1_has_signal:
            if not _check_cost_budget():
                console.log(f"[bold red]COST CIRCUIT BREAKER — Tier 2 cloud models blocked (budget exceeded)")
                api_providers = []  # Clear to skip
        if api_providers and tier1_has_signal:
            BATCH_SIZE = 4
            for batch_start in range(0, len(api_providers), BATCH_SIZE):
                batch = api_providers[batch_start:batch_start + BATCH_SIZE]
                batch_pids = [pid for pid, _ in batch]
                console.log(f"[cyan]API Batch: {', '.join(batch_pids)}")
                executor = ThreadPoolExecutor(max_workers=BATCH_SIZE)
                futures = {
                    executor.submit(
                        self._run_player, pid, provider, prices, indicators, news_by_symbol
                    ): pid
                    for pid, provider in batch
                }
                try:
                    for future in as_completed(futures, timeout=300):
                        pid = futures[future]
                        try:
                            future.result(timeout=5)
                        except TimeoutError:
                            console.log(f"[red]{pid} timed out (>300s)")
                        except Exception as e:
                            console.log(f"[red]{pid} failed: {e}")
                except TimeoutError:
                    for future, pid in futures.items():
                        if not future.done():
                            console.log(f"[red]{pid} BATCH TIMEOUT (>300s) — cancelling")
                            future.cancel()
                executor.shutdown(wait=False)

        # (Old Phase 2 Ollama code removed — Ollama now runs as Phase 1 / Tier 1 above)

        # 5. Log unrealized P&L for each player
        for pid in self.providers:
            pnl_data = get_portfolio_with_pnl(pid, prices)
            if pnl_data["positions"]:
                console.log(
                    f"[dim]{pid}: Portfolio ${pnl_data['total_value']:,.2f} | "
                    f"Unrealized P&L: ${pnl_data['total_unrealized_pnl']:+,.2f} | "
                    f"Return: {pnl_data['return_pct']:+.2f}%[/dim]"
                )
                # Flag stop-loss breaches
                for pos in pnl_data["positions"]:
                    if pos["unrealized_pnl_pct"] <= -12.0:
                        console.log(
                            f"[bold red]STOP-LOSS ALERT: {pid} {pos['symbol']} "
                            f"down {pos['unrealized_pnl_pct']:.1f}% "
                            f"(${pos['unrealized_pnl']:+.2f})[/bold red]"
                        )
                        alert_stop_loss(
                            pid, self.providers[pid].display_name,
                            pos["symbol"], pos["unrealized_pnl_pct"], pos["unrealized_pnl"],
                        )

        # 6. Save equity curve snapshot every 30 cycles (~30 min at 60s interval)
        self._equity_counter += 1
        if self._equity_counter % 30 == 1:
            for pid in self.providers:
                save_equity_snapshot(pid, prices)
            # Also snapshot Steve's Webull portfolio (benchmark)
            try:
                record_portfolio_snapshot("steve-webull", prices)
            except Exception:
                pass
            console.log("[dim]Equity curve snapshot saved[/dim]")

        # 6b. Daily journal — write once per day, triggered from scan cycle
        try:
            from engine.ai_journal import generate_journal_entry, save_journal_entry
            if self._equity_counter % 30 == 1:  # Same cadence as equity snapshots
                wrote_journal = False
                for pid, provider in self.providers.items():
                    try:
                        entry = generate_journal_entry(provider, pid, prices)
                        if entry:
                            save_journal_entry(pid, entry)
                            wrote_journal = True
                    except Exception as je:
                        console.log(f"[red]{pid} journal error: {je}")
                if wrote_journal:
                    console.log("[magenta]AI journals written")
        except Exception as e:
            console.log(f"[red]Journal error: {e}")

        # 7. Smart Money detection: check if 3+ models bought the same stock
        try:
            from engine.smart_money import detect_and_alert
            detect_and_alert()
        except Exception as e:
            console.log(f"[red]Smart money check error: {e}")

        # 8. Pair trade detection
        try:
            from engine.pair_trades import detect_pair_opportunities
            pairs = detect_pair_opportunities()
            for pair in pairs:
                console.log(f"[cyan]PAIR TRADE: {pair['long_symbol']}(long) / {pair['short_symbol']}(short) [{pair['sector']}] by {pair['display_name']}")
        except Exception:
            pass

        # 9a. Dynamic alerts (trendline breaks, RSI extremes, volume spikes, MACD crosses)
        try:
            from engine.dynamic_alerts import run_dynamic_alerts
            run_dynamic_alerts(prices, indicators)
        except Exception as e:
            console.log(f"[red]Dynamic alerts error: {e}")

        # 9a2. Volatility Breakout scanner
        try:
            from engine.volatility_breakout import run_breakout_scan
            breakouts = run_breakout_scan(list(prices.keys()))
            if breakouts:
                for b in breakouts[:3]:
                    console.log(
                        f"[bold yellow]BREAKOUT: {b['symbol']} {b['direction']} "
                        f"score={b['score']:.0f} vol={b['volume_ratio']:.1f}x"
                    )
        except Exception as e:
            console.log(f"[red]Breakout scanner error: {e}")

        # 9b. Update ghost trade outcomes
        try:
            from engine.ghost_trades import update_ghost_outcomes
            update_ghost_outcomes(prices)
        except Exception:
            pass

        # 9. Generate AI chat (pick 1-2 random AIs per cycle to chat)
        chat_count = min(2, len(self.providers))
        chatters = random.sample(list(self.providers.keys()), chat_count)
        for pid in chatters:
            provider = self.providers[pid]
            try:
                msg = generate_chat_message(provider, pid, prices=prices)
                if msg:
                    save_chat_message(pid, msg)
                    console.log(f"[magenta]{pid} chat: {msg[:80]}")
            except Exception as e:
                console.log(f"[red]{pid} chat error: {e}")

        # 10. Super Agent War Room — post crew consensus every 5 cycles; VIX spike alerts
        try:
            from engine.war_room import (
                post_super_agent_pipeline_take as _sa_pipeline,
                post_super_agent_vix_take as _sa_vix,
            )
            # Post pipeline take every 5 scans (~5 min at 60s interval)
            if self._equity_counter % 5 == 0:
                _sa_pipeline(prices)

            # VIX spike alert: post whenever VIX crosses above 25
            try:
                from engine.paper_trader import _get_vix_cached as _get_vix
                _vix_val = _get_vix()
                if _vix_val and _vix_val > 25:
                    _sa_vix(_vix_val)
            except Exception:
                pass
        except Exception as _sa_e:
            console.log(f"[red]Super Agent War Room error: {_sa_e}")

        console.log("[green]Scan complete.")

    def _run_player(self, player_id: str, provider: AIProvider, prices: dict,
                    indicators: dict, news_by_symbol: dict):
        """Run a single AI player's analysis and trading cycle."""
        # GUARD: Never trade human portfolios (Steve's Webull benchmark)
        if "steve" in player_id.lower() or "webull" in player_id.lower():
            console.log(f"[dim]{player_id} is HUMAN — skipping scan[/dim]")
            return
        import sqlite3 as _sq
        try:
            _hc = _sq.connect("data/trader.db", check_same_thread=False)
            _hr = _hc.execute("SELECT is_human FROM ai_players WHERE id=?", (player_id,)).fetchone()
            _hc.close()
            if _hr and _hr[0]:
                console.log(f"[dim]{player_id} is HUMAN — skipping scan[/dim]")
                return
        except Exception:
            pass

        # Check if player is paused
        import sqlite3
        try:
            conn = sqlite3.connect("data/trader.db", check_same_thread=False)
            paused = conn.execute("SELECT is_paused FROM ai_players WHERE id=?", (player_id,)).fetchone()
            conn.close()
            if paused and paused[0]:
                console.log(f"[dim]{player_id} PAUSED — skipping scan[/dim]")
                return
        except Exception:
            pass

        # RAM safety check for local Ollama models
        if player_id.startswith("ollama-"):
            try:
                import psutil
                mem = psutil.virtual_memory()
                avail_mb = mem.available / (1024 * 1024)
                if avail_mb < 1024:
                    console.log(
                        f"[bold yellow]\u26a0\ufe0f RAM LOW ({avail_mb:.0f}MB free) — skipping {player_id} this cycle"
                    )
                    return
            except Exception:
                pass

        # Check if player is halted
        is_halted, drawdown = self.risk.check_drawdown(player_id)
        if is_halted:
            console.log(f"[red]{player_id} HALTED: {drawdown:.1%} drawdown — stop-loss still active")

        portfolio = get_portfolio(player_id)

        # Check stop-loss / take-profit first (runs even when halted — must exit losers)
        sl_tp_actions = self.risk.check_stop_loss_take_profit(
            player_id, portfolio["positions"], prices
        )
        for action in sl_tp_actions:
            from engine.paper_trader import sell, sell_partial
            price = prices[action["symbol"]]["price"]
            if action["action"] == "SELL_PARTIAL":
                sell_partial(
                    player_id, action["symbol"], price,
                    qty=action["qty"],
                    asset_type=action.get("asset_type", "stock"),
                    reasoning=action["reason"],
                    option_type=action.get("option_type"),
                )
            else:
                sell(
                    player_id, action["symbol"], price,
                    asset_type=action.get("asset_type", "stock"),
                    reasoning=action["reason"],
                    option_type=action.get("option_type"),
                )
            console.log(f"[yellow]{player_id}: {action['reason']} on {action['symbol']}")

        # Skip new trade scanning when halted
        if is_halted:
            return

        # Refresh portfolio after SL/TP sells — auto-restart: immediately scan for next opportunity
        if sl_tp_actions:
            portfolio = get_portfolio(player_id)
            console.log(f"[cyan]{player_id}: Auto-restart — scanning for replacement after {len(sl_tp_actions)} close(s)")

        # Build shared scan context once per model (market regime, options, catalysts, intel)
        scan_ctx = ""
        try:
            from engine.scan_context import build_scan_context, build_earnings_alerts
            scan_ctx = build_scan_context(prices, indicators, player_id)
            ea = build_earnings_alerts(prices)
            if ea:
                scan_ctx += "\n\n=== EARNINGS ALERTS ===\n" + ea
        except Exception as e:
            console.log(f"[yellow]{player_id}: scan context build error: {e}")

        # Inject learning context (lessons, adjustments, ticker warnings)
        try:
            from engine.learning_engine import get_learning_context
            learning_ctx = get_learning_context(player_id)
            if learning_ctx:
                scan_ctx += learning_ctx
        except Exception as e:
            console.log(f"[dim]{player_id}: learning context error: {e}")

        # Inject trade memory — player's own 30-day track record (before market data)
        try:
            from engine.trade_memory import get_memory_block_for_player
            memory_block = get_memory_block_for_player(player_id)
            if memory_block:
                scan_ctx += memory_block
        except Exception:
            pass

        # Inject pre-market gap data — shared cache (5 min TTL) avoids N HTTP calls/cycle
        try:
            import time as _gc_time
            import requests as _req
            _now_gc = _gc_time.time()
            if _now_gc - _premarket_gap_cache["ts"] >= _PREMARKET_GAP_TTL:
                _gap_resp = _req.get("http://127.0.0.1:8080/api/premarket-gaps", timeout=10).json()
                _premarket_gap_cache["gaps"] = _gap_resp.get("gaps", [])
                _premarket_gap_cache["ts"] = _now_gc
            _gaps = _premarket_gap_cache["gaps"]
            if _gaps:
                gap_context = "\n=== PRE-MARKET GAPS (today) ===\n"
                for g in _gaps:
                    direction = "▲ GAP UP" if g.get("gap_pct", 0) > 0 else "▼ GAP DOWN"
                    odte = " [0DTE CANDIDATE]" if g.get("odte_candidate") else ""
                    gap_context += f"  {g['symbol']}: {direction} {g['gap_pct']:+.2f}% (prev close ${g['prev_close']:.2f} → pre-market ${g['premarket_price']:.2f}){odte}\n"
                gap_context += "Use this data to inform your thesis. Large gaps often signal catalysts.\n"
                gap_context += "=== END GAPS ===\n"
                scan_ctx += gap_context
        except Exception:
            pass

        # Inject scan context into the provider for prompt building
        provider._scan_context = scan_ctx

        # DayBlade Sulu: focused scan — only top movers + gap stocks (speed > breadth)
        _scan_prices = prices
        if player_id == "dayblade-sulu":
            _DAY_BLADE_FOCUS = {"SPY", "QQQ", "TSLA", "NVDA"}  # Always scan these
            # Add stocks with big moves (>1.5% either direction)
            _movers = {s for s, d in prices.items() if abs(d.get("change_pct", 0)) > 1.5}
            # Add gap stocks from scan context
            _gap_syms = set()
            try:
                import requests as _req2
                _gr = _req2.get("http://127.0.0.1:8080/api/premarket-gaps", timeout=5).json()
                _gap_syms = {g["symbol"] for g in _gr.get("gaps", []) if abs(g.get("gap_pct", 0)) > 2}
            except Exception:
                pass
            _focus = _DAY_BLADE_FOCUS | _movers | _gap_syms
            _scan_prices = {s: d for s, d in prices.items() if s in _focus}
            if len(_scan_prices) < 3:
                _scan_prices = prices  # Fallback if nothing qualifies
            console.log(f"[cyan]Sulu DayBlade: focused scan on {len(_scan_prices)} stocks: {', '.join(sorted(_scan_prices.keys()))}")

        # Mr. Dalio: All Weather scan — include DALIO_SYMBOLS (TLT, IEF, GLD, GSG) plus WATCH_STOCKS
        if player_id == "dalio-metals":
            try:
                from config import DALIO_SYMBOLS as _DALIO_SYMS
                _missing = [s for s in _DALIO_SYMS if s not in prices]
                if _missing:
                    from engine.market_data import get_all_prices as _gap
                    _extra = _gap(_missing)
                    _scan_prices = {**prices, **_extra}
                    console.log(f"[cyan]Dalio: added {', '.join(_missing)} to scan universe")
                else:
                    _scan_prices = {**prices}
            except Exception as _de:
                console.log(f"[yellow]Dalio symbol fetch error: {_de}")

        # Analyze each symbol (multi-step research chain when Flash available)
        for symbol, data in _scan_prices.items():
            sym_indicators = indicators.get(symbol, {})
            sym_news = news_by_symbol.get(symbol, [])

            decision = provider.analyze_chain(
                symbol=symbol,
                price=data["price"],
                change_pct=data["change_pct"],
                high=data["high"],
                low=data["low"],
                portfolio_context=portfolio,
                indicators=sym_indicators,
                news=sym_news,
            )

            # Save signal — capture rowid for status tracking
            signal_id = save_signal(
                player_id, symbol, decision.action, decision.confidence,
                decision.reasoning, option_type=decision.option_type,
                sources=decision.sources, timeframe=decision.timeframe,
            )

            # Track high-confidence BUY signals for multi-day monitoring
            if decision.action in ("BUY", "BUY_CALL", "BUY_PUT") and decision.confidence >= 0.65:
                try:
                    from engine.signal_tracker import record_signal
                    record_signal(
                        player_id, provider.display_name, symbol,
                        data["price"], decision.confidence, decision.reasoning
                    )
                except Exception:
                    pass

            # Ghost trade: log high-confidence HOLDs as missed opportunities
            if decision.action == "HOLD" and decision.confidence >= 0.60:
                try:
                    from engine.ghost_trades import log_ghost_trade
                    log_ghost_trade(player_id, symbol, decision.confidence,
                                    decision.reasoning, data["price"])
                except Exception:
                    pass

            # GHOST-TO-REAL PROMOTION: if 0.85+ confidence HOLD reaffirmed 3+ times, promote to BUY
            if decision.action == "HOLD" and decision.confidence >= 0.85:
                try:
                    import sqlite3 as _g3
                    _gc = _g3.connect("data/trader.db", check_same_thread=False)
                    _ghost_count = _gc.execute(
                        "SELECT COUNT(*) FROM ghost_trades WHERE player_id=? AND symbol=? "
                        "AND confidence >= 0.85 AND created_at >= datetime('now', '-48 hours')",
                        (player_id, symbol)
                    ).fetchone()[0]
                    _gc.close()
                    if _ghost_count >= 3:
                        console.log(
                            f"[bold green]👻→💰 {player_id}: GHOST PROMOTED to BUY on {symbol} "
                            f"(conf={decision.confidence:.0%}, reaffirmed {_ghost_count}x)"
                        )
                        decision = TradeDecision(
                            action="BUY", confidence=decision.confidence,
                            reasoning=f"Ghost promotion: {_ghost_count}x reaffirmed at 85%+ confidence. {decision.reasoning}",
                            symbol=symbol
                        )
                except Exception:
                    pass

            # THESIS ENFORCEMENT: reject trades without a real thesis
            # Per-model stricter checks (Spock: 50+ chars, must cite data)
            if decision.action != "HOLD":
                thesis = (decision.reasoning or "").strip()
                _min_thesis = self.risk.get_model_guardrail(player_id, "min_thesis_length", 20)
                _must_cite = self.risk.get_model_guardrail(player_id, "thesis_must_cite_data", False)
                if not thesis or len(thesis) < _min_thesis or thesis.upper() in ("NONE", "N/A", "NO THESIS"):
                    console.log(
                        f"[red][REJECTED] {player_id} tried to {decision.action} {symbol} — "
                        f"no thesis (len={len(thesis)}, required={_min_thesis})"
                    )
                    update_signal_status(signal_id, "REJECTED", "No thesis provided")
                    decision = TradeDecision(action="HOLD", confidence=0.0,
                                             reasoning="Rejected: no thesis provided", symbol=symbol)
                elif _must_cite:
                    # Spock must reference specific data points (RSI, volume, MACD, flow, VIX, earnings, etc.)
                    _data_keywords = ["rsi", "volume", "macd", "vix", "flow", "earnings", "sma",
                                      "bollinger", "support", "resistance", "catalyst", "breakout",
                                      "divergence", "ratio", "momentum", "relative strength", "%"]
                    thesis_lower = thesis.lower()
                    _cites_data = any(kw in thesis_lower for kw in _data_keywords)
                    if not _cites_data:
                        console.log(
                            f"[red][REJECTED] {player_id} tried to {decision.action} {symbol} — "
                            f"thesis lacks specific data references"
                        )
                        update_signal_status(signal_id, "REJECTED", "Thesis must cite specific data (RSI, volume, MACD, etc.)")
                        decision = TradeDecision(action="HOLD", confidence=0.0,
                                                 reasoning="Rejected: thesis must cite specific data (RSI, volume, MACD, etc.)",
                                                 symbol=symbol)

            # CONVICTION-WEIGHTED SIZING: scale alloc by conviction
            # (applied later in position sizing block)

            # MANDATORY STOP-LOSS ENFORCEMENT (Geordi: every trade must acknowledge stop-loss)
            if decision.action != "HOLD" and self.risk.get_model_guardrail(player_id, "mandatory_stop_loss", False):
                _model_sl = self.risk.get_model_guardrail(player_id, "stop_loss_pct", self.risk.stop_loss_pct)
                # Inject stop-loss into reasoning so it's tracked
                if "stop" not in (decision.reasoning or "").lower():
                    decision = TradeDecision(
                        action=decision.action,
                        confidence=decision.confidence,
                        reasoning=f"{decision.reasoning} [AUTO-STOP: -{_model_sl:.0%} enforced]",
                        symbol=symbol,
                        option_type=decision.option_type,
                        strike_price=decision.strike_price,
                        expiry_date=decision.expiry_date,
                        sources=decision.sources,
                    )
                console.log(f"[cyan]{player_id}: Mandatory stop-loss -{_model_sl:.0%} on {symbol}")

            # Execute if not HOLD
            if decision.action != "HOLD":
                # Scale-in logic: conviction >= 0.90, existing profitable position → add 25%
                if decision.action == "BUY" and decision.confidence >= 0.90:
                    existing = next(
                        (p for p in portfolio["positions"]
                         if p["symbol"] == symbol and p.get("asset_type", "stock") == "stock"),
                        None
                    )
                    if existing and data["price"] > existing["avg_price"]:
                        scale_qty = round(existing["qty"] * 0.25, 4)
                        scale_cost = scale_qty * data["price"]
                        if scale_qty > 0 and scale_cost <= portfolio["cash"] * 0.5:
                            allowed, reason = self.risk.check_buy(
                                player_id, symbol, data["price"], scale_qty, portfolio,
                                confidence=decision.confidence,
                            )
                            if allowed:
                                from engine.paper_trader import buy
                                result = buy(
                                    player_id, symbol, data["price"],
                                    qty=scale_qty,
                                    reasoning=f"Scale-in: conviction {decision.confidence:.0%}, +{((data['price']/existing['avg_price'])-1)*100:.1f}% profitable",
                                    confidence=decision.confidence,
                                )
                                if result:
                                    console.log(f"[cyan]{player_id}: SCALE-IN {scale_qty} {symbol} @ ${data['price']:.2f}")
                                    alert_trade(
                                        player_id, provider.display_name,
                                        "SCALE_IN", symbol,
                                        scale_qty, data["price"],
                                        f"Scale-in: conviction {decision.confidence:.0%}",
                                    )
                                    portfolio = get_portfolio(player_id)  # Refresh after scale-in
                                continue  # Don't open a duplicate BUY

                # Learning engine gate — apply adjustments before execution
                try:
                    from engine.learning_engine import apply_learning
                    trade_sig = {
                        "action": decision.action, "symbol": symbol,
                        "qty": 0, "price": data["price"],
                        "confidence": decision.confidence,
                        "reasoning": decision.reasoning,
                    }
                    modified = apply_learning(player_id, trade_sig)
                    if modified is None:
                        update_signal_status(signal_id, "REJECTED", "Blocked by learning engine")
                        continue  # Trade blocked by learning engine
                    # Apply modified confidence back
                    if modified.get("confidence_adjusted"):
                        decision.confidence = modified["confidence"]
                    if modified.get("ghost_promotion_disabled") and "ghost promotion" in (decision.reasoning or "").lower():
                        continue  # Ghost promotion disabled
                except Exception as e:
                    console.log(f"[dim]{player_id}: learning gate error: {e}")

                # Risk check for buys
                if decision.action in ("BUY", "BUY_CALL", "BUY_PUT"):
                    if decision.action in ("BUY_CALL", "BUY_PUT"):
                        asset_type = "option"
                    elif player_id == "dalio-metals":
                        try:
                            from config import DALIO_BOND_SYMBOLS as _DBONDS
                            asset_type = "bond" if symbol in _DBONDS else "stock"
                        except Exception:
                            asset_type = "stock"
                    else:
                        asset_type = "stock"
                    # Options get smaller allocation (5% of cash) to fit within per-ticker limits
                    alloc_pct = 0.05 if asset_type == "option" else 0.10
                    max_override = 0.0

                    if player_id == "dalio-metals":
                        recovery = _dalio_recovery_state()
                        if recovery and recovery["active"]:
                            console.log(
                                f"[yellow]dalio-metals: RECOVERY MODE active "
                                f"(legacy_open={recovery['legacy_open']}, recovery_open={recovery['recovery_open']}, "
                                f"recap=${recovery['amount']:.0f})"
                            )
                            if recovery["provider_health_degraded"]:
                                reason = f"{_RECOVERY_REJECTION_PREFIX} provider health degraded"
                                console.log(f"[red]dalio-metals: {reason} — blocking new entry on {symbol}")
                                update_signal_status(signal_id, "REJECTED", reason)
                                continue

                            existing = next((p for p in portfolio["positions"] if p["symbol"] == symbol), None)
                            if existing and _is_losing_position(existing, data["price"]):
                                reason = f"{_RECOVERY_REJECTION_PREFIX} averaging down disabled on losing position"
                                console.log(f"[red]dalio-metals: {reason} for {symbol}")
                                update_signal_status(signal_id, "REJECTED", reason)
                                continue

                            if recovery["legacy_open"] > 0 and recovery["recovery_open"] >= 1 and not existing:
                                reason = f"{_RECOVERY_REJECTION_PREFIX} max 1 recovery entry while legacy positions remain open"
                                console.log(f"[red]dalio-metals: {reason}")
                                update_signal_status(signal_id, "REJECTED", reason)
                                continue

                            # Recovery mode: sharply reduce new capital deployment.
                            alloc_pct = min(alloc_pct, 0.02)
                            max_override = min(max_override, 0.02) if max_override > 0 else 0.02

                    # CONVICTION-WEIGHTED SIZING: scale by confidence
                    # 0.80-1.0 = full size, 0.50-0.79 = 50%, below 0.50 = 25%
                    if decision.confidence >= 0.80:
                        pass  # full alloc_pct
                    elif decision.confidence >= 0.50:
                        alloc_pct *= 0.50
                    else:
                        alloc_pct *= 0.25

                    # Extended hours: half position sizes (wider spreads, lower liquidity)
                    _session = self.risk.is_market_hours()
                    if _session in ("pre_market", "post_market"):
                        alloc_pct *= 0.5

                    # Triple-alignment conviction multiplier:
                    # 0.90+ confidence + flow lean confirms direction + catalyst within 3 days → up to 40%
                    if decision.confidence >= 0.90 and asset_type == "stock":
                        _flow_confirms = False
                        _has_catalyst = False
                        try:
                            from engine.market_flow import get_flow_lean
                            _fl = get_flow_lean()
                            if _fl:
                                # BUY aligns with BULL lean
                                if decision.action == "BUY" and _fl["lean"] == "BULL" and _fl["conviction"] >= 30:
                                    _flow_confirms = True
                        except Exception:
                            pass
                        try:
                            from engine.earnings_calendar import get_earnings_warnings
                            _ew = get_earnings_warnings([symbol])
                            if _ew:
                                _has_catalyst = any(e["days_until"] <= 3 for e in _ew)
                        except Exception:
                            pass
                        # Also count major news as a catalyst
                        if not _has_catalyst:
                            try:
                                import sqlite3 as _sq3
                                _ndb = _sq3.connect("data/trader.db", check_same_thread=False)
                                _recent_news = _ndb.execute(
                                    "SELECT COUNT(*) FROM market_news "
                                    "WHERE symbol=? AND fetched_at >= datetime('now', '-6 hours')",
                                    (symbol,)
                                ).fetchone()
                                _ndb.close()
                                if _recent_news and _recent_news[0] >= 3:
                                    _has_catalyst = True
                            except Exception:
                                pass

                        if _flow_confirms and _has_catalyst:
                            alloc_pct = 0.40
                            max_override = 0.40
                            console.log(
                                f"[bold green]{player_id}: TRIPLE ALIGNED on {symbol} — "
                                f"conf {decision.confidence:.0%} + BULL flow + catalyst → 40% sizing"
                            )
                        elif _flow_confirms or _has_catalyst:
                            # Partial alignment: bump to 25%
                            alloc_pct = 0.25
                            max_override = 0.30

                    # VIX regime filter: scale position sizes by volatility regime
                    # Sulu (day trader) gets a more aggressive threshold — volatility is opportunity
                    # Dalio (All Weather) is exempt — bonds/gold are safe havens, buy MORE during fear
                    try:
                        from engine.vix_monitor import get_vix_status
                        _vix = get_vix_status()
                        _vix_price = _vix.get("price", 0) if _vix else 0
                        _DEFENSIVE_SYMBOLS = {"XLE", "UNH", "XLU", "XLP", "JNJ", "PG", "KO", "PEP", "WMT", "CL"}
                        _is_dayblade = player_id == "dayblade-sulu"
                        _is_dalio = player_id == "dalio-metals"
                        try:
                            from config import DALIO_SYMBOLS as _DS
                            _dalio_symbols = set(_DS)
                        except Exception:
                            _dalio_symbols = {"TLT", "IEF", "GLD", "GSG"}
                        # VIX regime: scale position sizes, never hard-block (Option C)
                        # Formula: max(0.25, 1.0 - (vix - 25) / 20)
                        #   VIX 25 → 100%  |  VIX 30 → 75%  |  VIX 35 → 50%  |  VIX 40+ → 25%
                        # Dalio (All Weather) and Sulu (day trader) have their own handling
                        if _is_dalio:
                            pass  # No VIX filter for Dalio — All Weather is designed for any market
                        elif _vix_price >= 25:
                            if _is_dayblade:
                                # Sulu: vol = opportunity for scalps, less aggressive scaling
                                # VIX 25→100%, VIX 35→75%, VIX 45→50% (min)
                                _vix_scale = max(0.50, 1.0 - (_vix_price - 25) / 40)
                                alloc_pct *= _vix_scale
                                console.log(f"[yellow]{player_id}: VIX={_vix_price:.1f} — day-trade sizing {_vix_scale:.0%} on {symbol}")
                            else:
                                # Swing models: scale down, NEVER hard-block (Option C)
                                # VIX 25→100%, VIX 30→75%, VIX 35→50%, VIX 40+→25%
                                _vix_scale = max(0.25, 1.0 - (_vix_price - 25) / 20)
                                alloc_pct *= _vix_scale
                                if max_override > 0:
                                    max_override = min(max_override, _vix_scale * 0.20)
                                console.log(f"[yellow]{player_id}: VIX={_vix_price:.1f} — {_vix_scale:.0%} sizing on {symbol} (no block)")
                        elif _vix_price >= 20:
                            if not _is_dayblade:
                                # Caution: 75% sizing
                                alloc_pct *= 0.75
                                console.log(f"[yellow]{player_id}: VIX={_vix_price:.1f} — caution, 75% sizing on {symbol}")
                    except Exception:
                        pass

                    qty = round((portfolio["cash"] * alloc_pct) / data["price"], 4)
                    allowed, reason = self.risk.check_buy(
                        player_id, symbol, data["price"], qty, portfolio,
                        asset_type=asset_type, max_position_override=max_override,
                        confidence=decision.confidence,
                    )
                    if not allowed:
                        console.log(f"[yellow]{player_id}: {decision.action} {symbol} blocked - {reason}")
                        update_signal_status(signal_id, "REJECTED", reason)
                        continue

                result = execute_signal(
                    player_id,
                    {
                        "action": decision.action,
                        "symbol": symbol,
                        "reasoning": decision.reasoning,
                        "confidence": decision.confidence,
                        "sources": decision.sources,
                        "timeframe": decision.timeframe,
                        "asset_type": locals().get("asset_type", "stock"),
                    },
                    data["price"]
                )
                if result:
                    _signal_status = result.get("execution_status", "EXECUTED")
                    _reason = None
                    if _signal_status == "LOG_ONLY":
                        _reason = f"{result.get('portfolio_name', 'portfolio')} is tracking-only"
                    update_signal_status(signal_id, _signal_status, _reason)
                    console.log(f"[green]{player_id}: {_signal_status} {result}")

                    # Post to Signal Center (port 9000) for high-confidence decisions
                    if decision.confidence >= 0.70:
                        try:
                            import threading as _sc_th
                            _sl_pct = 0.12  # default 12% stop-loss
                            _sc_stop = round(data["price"] * (1 - _sl_pct), 2)
                            _sc_tp   = round(data["price"] * 1.20, 2)  # default 20% target
                            _sig_type = "SWING"
                            _src_list = (decision.sources or "").split(",") if decision.sources else []
                            _src_lower = [s.lower() for s in _src_list]
                            _reason_lower = (decision.reasoning or "").lower()
                            # Classify signal type
                            if decision.option_type or decision.action in ("BUY_CALL", "BUY_PUT"):
                                _sig_type = "0DTE" if decision.timeframe == "SCALP" else "OPTIONS"
                            elif decision.action in ("BUY_PUT", "SHORT"):
                                _sig_type = "BEARISH"
                            elif "congress" in _reason_lower or any("congress" in s for s in _src_lower):
                                _sig_type = "CONGRESS"
                            elif "volume" in _reason_lower or any("volume" in s for s in _src_lower):
                                _sig_type = "VOLUME"
                            elif "rsi" in _reason_lower and ("oversold" in _reason_lower or "bounce" in _reason_lower):
                                _sig_type = "OVERSOLD"
                            elif decision.timeframe == "SWING":
                                _sig_type = "SWING"
                            _sc_payload = {
                                "type": _sig_type,
                                "action": decision.action,
                                "symbol": symbol,
                                "price": data["price"],
                                "confidence": round(decision.confidence * 100),
                                "agent": getattr(provider, "display_name", player_id),
                                "model": getattr(provider, "model_id", player_id),
                                "reasoning": (decision.reasoning or "")[:500],
                                "sources": _src_list,
                                "timeframe": decision.timeframe or "SWING",
                                "stop_loss": _sc_stop,
                                "take_profit": _sc_tp,
                                "context_summary": "",
                                "timestamp": decision.timestamp if hasattr(decision, 'timestamp') else __import__('datetime').datetime.now().isoformat(),
                            }
                            def _post_sc(payload=_sc_payload):
                                try:
                                    import requests as _req
                                    _req.post(
                                        "http://localhost:9000/api/signal",
                                        json=payload, timeout=3,
                                    )
                                except Exception:
                                    pass
                            _sc_th.Thread(target=_post_sc, daemon=True).start()
                        except Exception:
                            pass

                elif decision.action not in ("HOLD", "SELL"):
                    _rej = _last_rejection.get(player_id, "Execution failed")
                    update_signal_status(signal_id, "REJECTED", _rej)
                    # Telegram alert
                    alert_trade(
                        player_id, provider.display_name,
                        decision.action, symbol,
                        result.get("qty", 0) if result else 0, data["price"],
                        decision.reasoning,
                    )

        # Record portfolio snapshot
        record_portfolio_snapshot(player_id, prices)
