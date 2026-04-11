"""
USS TradeMinds — Unified Trader (CrewAI + Multi-Portfolio)

Runs on port 8000. The arena benchmark stays on main.py:8080.

Usage:
    .venv-crew/bin/python main_crew.py
"""

import json
import os
import sqlite3
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
load_dotenv(override=True)

_MST = ZoneInfo("US/Arizona")

# Monkey-patch sqlite3.connect for consistent busy timeout across all modules
_original_sqlite3_connect = sqlite3.connect
def _patched_connect(*args, **kwargs):
    kwargs.setdefault("timeout", 30)
    conn = _original_sqlite3_connect(*args, **kwargs)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn
sqlite3.connect = _patched_connect

os.environ.setdefault("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from crew.routes import (
    crew_router,
    portfolio_router,
    positions_router,
    _scheduler_state,
    NeoMirrorRequest,
    neo_mirror,
    neo_status,
)
from shared.matrix_bridge import sync_neo_from_native_portfolio

# ---------------------------------------------------------------------------
# Scheduler job functions
# ---------------------------------------------------------------------------

_pipeline_lock = threading.Lock()  # prevent overlapping pipeline runs


def _is_weekday() -> bool:
    return datetime.now(_MST).weekday() < 5  # 0=Mon … 4=Fri


def _run_scout(trigger: str = "scheduled", job_id: str = "scout_premarket", focus: str = "pre-market opportunities — gap ups, unusual volume, overnight news catalysts"):
    """Scout-only scan (no pipeline, no deploy)."""
    now = datetime.now(_MST).isoformat()
    _scheduler_state[job_id] = {"last_run": now, "last_outcome": "running", "trigger": trigger}
    try:
        from crew.pipeline import CrewPipeline
        result = CrewPipeline().run_scout_only(focus)
        _scheduler_state[job_id]["last_outcome"] = result.get("status", "unknown")
        _scheduler_state[job_id]["duration"] = result.get("duration")
    except Exception as e:
        _scheduler_state[job_id]["last_outcome"] = f"error: {e}"


def _run_pipeline(job_id: str, trigger: str, focus: str, dollar_amount: int = 2000):
    """Full pipeline with auto-deploy of high-conviction strategies."""
    now = datetime.now(_MST).isoformat()
    _scheduler_state[job_id] = {"last_run": now, "last_outcome": "running", "trigger": trigger}

    if not _pipeline_lock.acquire(blocking=False):
        _scheduler_state[job_id]["last_outcome"] = "skipped — pipeline already running"
        return

    try:
        # Sync closed positions → learning log before each run
        try:
            from crew.learning import sync_closed_positions, sync_outcomes_to_scoreboard
            sync_result = sync_closed_positions()
            if sync_result.get("recorded", 0) > 0:
                print(f"[crew learning] Recorded {sync_result['recorded']} closed position(s)")
            # Sync trade outcomes to scoreboard for dynamic weighting
            scoreboard_result = sync_outcomes_to_scoreboard()
            if scoreboard_result.get("synced", 0) > 0:
                print(f"[crew learning] Synced {scoreboard_result['synced']} trade outcomes to scoreboard")
        except Exception as le:
            print(f"[crew learning] sync error: {le}")

        from crew.pipeline import CrewPipeline
        result = CrewPipeline().run_full_pipeline(
            focus_area=focus,
            target_asset_class="stock",
            target_portfolio_id=1,
            trigger=trigger,
        )
        outcome = result.get("status", "unknown")
        _scheduler_state[job_id]["last_outcome"] = outcome
        _scheduler_state[job_id]["duration"] = result.get("duration")
        if outcome == "completed":
            _auto_deploy_high_conviction(dollar_amount=dollar_amount)
    except Exception as e:
        _scheduler_state[job_id]["last_outcome"] = f"error: {e}"
    finally:
        _pipeline_lock.release()


def _run_sunday():
    """Sunday strategy session — 3 strategies across stocks, options, metals."""
    job_id = "sunday_strategy"
    now = datetime.now(_MST).isoformat()
    _scheduler_state[job_id] = {"last_run": now, "last_outcome": "running", "trigger": "sunday_auto"}

    if not _pipeline_lock.acquire(blocking=False):
        _scheduler_state[job_id]["last_outcome"] = "skipped — pipeline already running"
        return

    try:
        # Sync closed positions before Sunday review
        try:
            from crew.learning import sync_closed_positions
            sync_result = sync_closed_positions()
            if sync_result.get("recorded", 0) > 0:
                print(f"[crew learning] Recorded {sync_result['recorded']} closed position(s) before Sunday review")
        except Exception as le:
            print(f"[crew learning] sync error: {le}")

        from crew.pipeline import CrewPipeline
        result = CrewPipeline().run_sunday_review()
        _scheduler_state[job_id]["last_outcome"] = result.get("status", "unknown")
        _scheduler_state[job_id]["strategies_generated"] = result.get("strategies_generated", 0)
        _scheduler_state[job_id]["duration"] = result.get("total_duration")
        _auto_deploy_high_conviction()
    except Exception as e:
        _scheduler_state[job_id]["last_outcome"] = f"error: {e}"
    finally:
        _pipeline_lock.release()


def _sync_neo_shared_views():
    """Lightweight safety sync for Neo's shared 8080 mirror."""
    try:
        sync_neo_from_native_portfolio()
    except Exception as e:
        print(f"[neo sync] error: {e}")


def _auto_deploy_high_conviction(dollar_amount: int = 2000):
    """
    Mr. Anderson collective execution layer.

    Gather undeployed crew strategy candidates, rank them by weighted confidence,
    filter by a minimum threshold, cap the final selections per cycle, and then
    execute only the strongest collective signals on Alpaca Paper.
    """
    DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        pause_all = conn.execute(
            "SELECT value FROM settings WHERE key='pause_all'"
        ).fetchone()
        if pause_all and pause_all["value"] == "1":
            print("[mr-anderson] skipped: global pause_all is enabled")
            return

        anderson = conn.execute(
            "SELECT COALESCE(is_paused, 0) AS is_paused FROM ai_players WHERE id='super-agent'"
        ).fetchone()
        if anderson and anderson["is_paused"]:
            print("[mr-anderson] skipped: super-agent is paused in model controls")
            return

        # All recent undeployed strategies — we'll decide per-row
        candidates = conn.execute(
            "SELECT id, name, target_tickers, conviction_score, critic_score, critic_notes, "
            "direction, thesis, status, scout_brief, architect_reasoning, commander_decision "
            "FROM crew_strategies "
            "WHERE status IN ('draft', 'approved') "
            "AND deployed_to_portfolio_id IS NULL "
            "AND created_at >= datetime('now', '-2 hours') "
            "ORDER BY conviction_score DESC"
        ).fetchall()
    finally:
        conn.close()

    if not candidates:
        return

    try:
        from portfolios.manager import PortfolioManager

        pm = PortfolioManager()
        target_portfolio = pm.get_portfolio(1)
        execution = pm.can_execute(target_portfolio)
        if not execution["allowed"]:
            portfolio_name = (target_portfolio or {}).get("name", "portfolio_id=1")
            print(f"[mr-anderson] skipped: {portfolio_name} is {execution['reason']}")
            return
    except Exception as e:
        print(f"[mr-anderson] skipped: portfolio execution guard error: {e}")
        return

    from crew.ensemble import select_collective_signals

    selected = select_collective_signals(candidates)
    candidate_signals = selected["candidate_signals"]
    surviving_signals = selected["surviving_signals"]
    exploit_signals = selected["exploit_signals"]
    explore_signals = selected["explore_signals"]
    final_signals = selected["final_signals"]

    print(
        f"[mr-anderson] incoming={len(candidate_signals)} "
        f"surviving={len(surviving_signals)} "
        f"exploit={len(exploit_signals)} "
        f"explore={len(explore_signals)} "
        f"selected={len(final_signals)} "
        f"min_weighted_confidence={selected['min_weighted_confidence']:.3f} "
        f"top_n={selected['top_n']} "
        f"exploration_pct={selected['exploration_pct']:.2f}"
    )
    for signal in candidate_signals:
        status = signal.get("selection_type", "survived") if signal in final_signals else (
            "survived" if signal in surviving_signals else "filtered"
        )
        print(
            f"[mr-anderson] {status.upper()} strategy={signal['id']} "
            f"symbol={signal['symbol'] or '?'} "
            f"weighted_confidence={signal['weighted_confidence']:.3f} "
            f"conviction={float(signal.get('conviction_score') or 0):.1f}/10"
        )

    if not final_signals:
        return

    import sys
    sys.path.insert(0, os.path.expanduser("~/autonomous-trader"))

    try:
        from engine.alpaca_bridge import AlpacaBridge
        bridge = AlpacaBridge()
        if not bridge.client:
            print("[crew auto-deploy] Alpaca not connected, skipping")
            return
    except Exception as e:
        print(f"[crew auto-deploy] Alpaca init error: {e}")
        return

    from crew.agents import execute_paper_trade

    for s in final_signals:
        try:
            ticker = s["symbol"]

            if not ticker:
                continue

            direction = (s["direction"] or "long").lower()
            if direction not in ("long", "short"):
                direction = "long"

            trade_params = json.dumps({
                "ticker": ticker,
                "direction": direction,
                "dollar_amount": dollar_amount,
                "stop_loss_pct": 7.0,
                "take_profit_pct": 20.0,
                "strategy_id": s["id"],
                "notes": (
                    f"Mr. Anderson collective execution | "
                    f"source_bucket={'LegacyCrew' if str(s.get('agent', '')).startswith('strategy-') else (s.get('agent') or 'Unknown')} | "
                    f"source_agent={s.get('agent') or 'unknown'} | "
                    f"weighted_confidence={s['weighted_confidence']:.3f} | "
                    f"contributors={','.join(meta['agent'] for meta in s['source_agent_metadata']) or 'crew'} | "
                    f"conviction={s['conviction_score']}/10 — {(s['thesis'] or '')[:100]}"
                ),
            })

            print(
                f"[mr-anderson] → Executing {ticker} {direction.upper()} "
                f"${dollar_amount} | weighted_confidence={s['weighted_confidence']:.3f} "
                f"| selection={s['selection_type']} "
                f"| conviction={s['conviction_score']}/10 | strategy_id={s['id']}"
            )

            result_str = execute_paper_trade(trade_params)
            result = json.loads(result_str)

            DB2 = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
            conn2 = sqlite3.connect(DB2, timeout=30)
            conn2.execute("PRAGMA busy_timeout=30000")
            try:
                if result.get("executed"):
                    new_status = "approved"
                    print(
                        f"[mr-anderson] ✓ EXECUTED {ticker} "
                        f"qty={result.get('qty')} @ ${result.get('entry_price')} "
                        f"(${result.get('dollar_value')}) | "
                        f"stop=${result.get('stop_loss')} target=${result.get('take_profit')} | "
                        f"order={result.get('alpaca_order_id')}"
                    )
                else:
                    new_status = "rejected"
                    print(f"[mr-anderson] ✗ FAILED {ticker}: {result.get('error', '?')}")
                conn2.execute(
                    "UPDATE crew_strategies SET status=?, deployed_to_portfolio_id=?, updated_at=? WHERE id=?",
                    (new_status, 1 if result.get("executed") else None,
                     datetime.now().isoformat(), s["id"]),
                )
                conn2.commit()
            finally:
                conn2.close()

        except Exception as e:
            print(f"[mr-anderson] Error for strategy {s['id']}: {e}")


# ---------------------------------------------------------------------------
# APScheduler — 5 jobs, all MST (US/Arizona, no DST)
# ---------------------------------------------------------------------------

_scheduler = BackgroundScheduler(timezone=_MST)

# 6:00 AM Mon–Fri: Pre-market scout
_scheduler.add_job(
    lambda: _run_scout("premarket_auto"),
    CronTrigger(hour=6, minute=0, day_of_week="mon-fri", timezone=_MST),
    id="scout_premarket",
    max_instances=1,
    misfire_grace_time=300,
)

# 7:00 AM Mon–Fri: Morning full pipeline
_scheduler.add_job(
    lambda: _run_pipeline(
        "morning_pipeline", "morning_auto",
        "morning market opportunities — gap plays, pre-market movers, overnight news catalysts"
    ),
    CronTrigger(hour=7, minute=0, day_of_week="mon-fri", timezone=_MST),
    id="morning_pipeline",
    max_instances=1,
    misfire_grace_time=300,
)

# 12:00 PM Mon–Fri: Midday review
_scheduler.add_job(
    lambda: _run_pipeline(
        "midday_review", "midday_auto",
        "midday opportunities — lunchtime breakouts, sector rotations, earnings movers"
    ),
    CronTrigger(hour=12, minute=0, day_of_week="mon-fri", timezone=_MST),
    id="midday_review",
    max_instances=1,
    misfire_grace_time=300,
)

# 1:30 PM Mon–Fri: Afternoon pipeline
_scheduler.add_job(
    lambda: _run_pipeline(
        "afternoon_pipeline", "afternoon_auto",
        "afternoon setups — late-day momentum, power hour plays, after-hours catalysts"
    ),
    CronTrigger(hour=13, minute=30, day_of_week="mon-fri", timezone=_MST),
    id="afternoon_pipeline",
    max_instances=1,
    misfire_grace_time=300,
)

# Sunday 5:00 PM: Weekly strategy session
_scheduler.add_job(
    _run_sunday,
    CronTrigger(hour=17, minute=0, day_of_week="sun", timezone=_MST),
    id="sunday_strategy",
    max_instances=1,
    misfire_grace_time=600,
)

# 4:00 AM Mon–Fri: Early pre-market scout (50% sizing — scout only, no deploy)
_scheduler.add_job(
    lambda: _run_scout(
        trigger="early_premarket_auto",
        job_id="early_premarket_scout",
        focus="early pre-market movers — overnight gaps, futures positioning, Asian market close, earnings pre-open",
    ),
    CronTrigger(hour=4, minute=0, day_of_week="mon-fri", timezone=_MST),
    id="early_premarket_scout",
    max_instances=1,
    misfire_grace_time=300,
)

# 2:00 PM Mon–Fri: Post-market review (50% sizing — $1000)
_scheduler.add_job(
    lambda: _run_pipeline(
        "postmarket_review", "postmarket_auto",
        "post-market review — position management, after-hours movers, next-day setups",
        dollar_amount=1000,
    ),
    CronTrigger(hour=14, minute=0, day_of_week="mon-fri", timezone=_MST),
    id="postmarket_review",
    max_instances=1,
    misfire_grace_time=300,
)

# 4:30 PM Mon–Fri: After-hours scout (50% sizing — scout only, no deploy)
_scheduler.add_job(
    lambda: _run_scout(
        trigger="afterhours_auto",
        job_id="afterhours_scout",
        focus="after-hours earnings movers, news catalysts, extended-hours volume spikes, next-day gap candidates",
    ),
    CronTrigger(hour=16, minute=30, day_of_week="mon-fri", timezone=_MST),
    id="afterhours_scout",
    max_instances=1,
    misfire_grace_time=300,
)

# Every 5 minutes, offset from the top of the hour: keep Neo shared mirror fresh.
_scheduler.add_job(
    _sync_neo_shared_views,
    IntervalTrigger(minutes=5, start_date=datetime.now(_MST).replace(second=30, microsecond=0), timezone=_MST),
    id="neo_shared_sync",
    max_instances=1,
    misfire_grace_time=120,
    coalesce=True,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _scheduler.start()
    print("[crew scheduler] APScheduler started — 9 jobs armed (MST)")
    print("  Mon–Fri: EarlyScout 4AM · Scout 6AM · Pipeline 7AM · Midday 12PM · Afternoon 1:30PM · PostMarket 2PM · AHScout 4:30PM")
    print("  Every 5m: Neo shared mirror sync (lightweight, staggered)")
    print("  Sunday:  Strategy session 5PM")
    yield
    _scheduler.shutdown(wait=False)
    print("[crew scheduler] APScheduler stopped")


app = FastAPI(
    title="USS TradeMinds — Unified Trader",
    description="CrewAI strategy pipeline + multi-portfolio management",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(crew_router, prefix="/api/crew", tags=["CrewAI"])
app.include_router(portfolio_router, prefix="/api/portfolios", tags=["Portfolios"])
app.include_router(positions_router, prefix="/api/positions", tags=["Positions"])


_BOOT_TIME = time.time()

# Clean up any stale Ollama lock left by a crashed process
import logging as _logging
from shared.ollama_lock import cleanup_stale_lock as _cleanup_stale_lock
_stale_msg = _cleanup_stale_lock()
if _stale_msg:
    _logging.warning("[startup] %s", _stale_msg)

DB_PATH = os.environ.get("TRADEMINDS_DB", "")


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@app.get("/", response_class=HTMLResponse)
def root():
    conn = _db()
    try:
        # Portfolios — exclude human accounts (Webull, Physical Metals)
        portfolios = [dict(r) for r in conn.execute(
            "SELECT id, name, broker, account_type, initial_balance, current_balance, is_human, is_active FROM portfolios WHERE is_human=0 ORDER BY id"
        ).fetchall()]
        total_balance = sum(p["current_balance"] for p in portfolios if p["name"] not in ("Super Agent", "Mr. Anderson"))

        # Crew runs
        runs = [dict(r) for r in conn.execute(
            "SELECT id, run_type, outcome, duration_seconds, error_log, created_at "
            "FROM crew_runs ORDER BY created_at DESC LIMIT 5"
        ).fetchall()]

        # Strategies
        strat_count = conn.execute("SELECT COUNT(*) FROM crew_strategies").fetchone()[0]
        open_positions = conn.execute(
            "SELECT COUNT(*) FROM portfolio_positions WHERE status='open'"
        ).fetchone()[0]

        # Realized P&L
        realized = conn.execute(
            "SELECT COALESCE(SUM(closed_pnl), 0) FROM portfolio_positions WHERE status='closed'"
        ).fetchone()[0]

        # Live balance per portfolio: initial_balance + unrealized_pnl + realized_pnl
        pnl_rows = conn.execute(
            "SELECT portfolio_id, "
            "  COALESCE(SUM(CASE WHEN status='open' THEN unrealized_pnl ELSE 0 END), 0) as unrealized, "
            "  COALESCE(SUM(CASE WHEN status='closed' THEN closed_pnl ELSE 0 END), 0) as realized "
            "FROM portfolio_positions GROUP BY portfolio_id"
        ).fetchall()
        _pnl_map = {r[0]: (r[1], r[2]) for r in pnl_rows}
        live_balance_map = {}
        for p in portfolios:
            unrealized, realized = _pnl_map.get(p["id"], (0, 0))
            live_balance_map[p["id"]] = p["initial_balance"] + unrealized + realized
        # Super Agent (id=6) mirrors Alpaca Paper (id=1) — same positions, same live balance
        if 6 in live_balance_map and 1 in live_balance_map:
            live_balance_map[6] = live_balance_map[1]
        # Recompute total_balance using live values, excluding Super Agent (same money as Alpaca)
        total_balance = sum(live_balance_map.get(p["id"], p["initial_balance"]) for p in portfolios if p["name"] not in ("Super Agent", "Mr. Anderson"))

        # Chart data — portfolio allocation & asset class breakdown
        ac_rows = conn.execute(
            "SELECT asset_class, ROUND(SUM(current_price * quantity), 2) as val "
            "FROM portfolio_positions WHERE status='open' GROUP BY asset_class"
        ).fetchall()
    finally:
        conn.close()

    import json as _json
    # Portfolio donut: only non-human portfolios with balance > 0, using live balance
    _pf_chart = [(p["name"], live_balance_map.get(p["id"], p["current_balance"])) for p in portfolios if live_balance_map.get(p["id"], p["current_balance"]) > 0]
    chart_pf_labels = _json.dumps([x[0] for x in _pf_chart])
    chart_pf_values = _json.dumps([round(x[1], 2) for x in _pf_chart])
    # Asset class donut
    chart_ac_labels = _json.dumps([r["asset_class"].upper() for r in ac_rows])
    chart_ac_values = _json.dumps([float(r["val"] or 0) for r in ac_rows])

    uptime_s = int(time.time() - _BOOT_TIME)
    uptime_m, uptime_sec = divmod(uptime_s, 60)
    uptime_h, uptime_m = divmod(uptime_m, 60)
    uptime_str = f"{uptime_h}h {uptime_m}m {uptime_sec}s"

    # Build portfolio rows
    pf_rows = ""
    for p in portfolios:
        status = "ACTIVE" if p["is_active"] else "INACTIVE"
        status_cls = "active" if p["is_active"] else "inactive"
        human_tag = ' <span class="human-tag">HUMAN</span>' if p["is_human"] else ""
        is_super = p["name"] in ("Super Agent", "Mr. Anderson")
        acct_type = p["account_type"]
        type_cls = {"paper": "type-paper", "live": "type-live", "physical": "type-physical", "crewai": "type-paper"}.get(acct_type, "")
        super_style = 'border-left:3px solid #f59e0b;' if is_super else ''
        super_name = f'<span style="color:#f59e0b;font-weight:700">{p["name"]}</span> <span style="font-size:9px;color:#92400e;background:#451a03;padding:1px 5px;border-radius:3px">AI CREW</span>' if is_super else f'{p["name"]}{human_tag}'
        display_balance = live_balance_map.get(p["id"], p["current_balance"])
        pf_rows += (
            f'<tr class="pf-row" onclick="showPortfolioDetail({p["id"]})" style="cursor:pointer;{super_style}">'
            f'<td>{super_name}</td><td>{p["broker"].upper()}</td>'
            f'<td><span class="acct-type {type_cls}">{acct_type.upper()}</span></td>'
            f'<td class="num">${display_balance:,.2f}</td>'
            f'<td><span class="badge {status_cls}">{status}</span></td>'
            f'<td style="text-align:right;font-size:11px;color:#3b82f6">&#8594;</td></tr>\n'
        )

    # Build crew run rows
    run_rows = ""
    if runs:
        for r in runs:
            outcome = r["outcome"] or "unknown"
            out_cls = "ok" if outcome == "completed" else "err"
            dur = f'{r["duration_seconds"]:.1f}s' if r["duration_seconds"] else "--"
            err_snip = ""
            if r["error_log"]:
                err_snip = f'<span class="err-snip" title="{r["error_log"][:200]}"> [!]</span>'
            run_rows += (
                f'<tr><td>#{r["id"]}</td><td>{r["run_type"]}</td>'
                f'<td><span class="badge {out_cls}">{outcome}</span>{err_snip}</td>'
                f'<td class="num">{dur}</td><td>{r["created_at"]}</td></tr>\n'
            )
    else:
        run_rows = '<tr><td colspan="5" class="dim">No crew runs yet</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>USS TradeMinds — Unified Trader</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
/* ── CSS variables — dark (default) ─────────────────────────────────── */
:root{{
  --bg:#0a0e1a;--bg-grad:radial-gradient(ellipse at 50% 0%,#1a2040 0%,#0a0e1a 70%);
  --card-bg:linear-gradient(135deg,#111827,#1a2040);--card-border:#2d4a7a;
  --text:#e0e6f0;--text-dim:#475569;--text-muted:#64748b;
  --th-color:#64748b;--td-border:#111827;--h2-color:#94a3b8;--h2-border:#1e3a5f;
  --btn-bg:#1e293b;--btn-border:#334155;--btn-color:#60a5fa;
  --btn-hover-bg:#2d4a7a;--btn-hover-border:#60a5fa;
  --result-bg:#0f172a;--result-border:#1e3a5f;
  --pf-card-bg:#0d1526;--pf-card-border:#1e3a5f;
  --run-hover:#0d1526;--run-detail-bg:#080c18;
  --input-bg:#0f172a;--input-border:#334155;--input-color:#e0e6f0;--input-select-color:#94a3b8;
  --accent:#60a5fa;--title-color:#60a5fa;--sub-color:#f59e0b;
  --footer-color:#334155;--section-label-color:#64748b;
  /* P&L */
  --pnl-pos:#4ade80;--pnl-neg:#f87171;
  /* Badges */
  --badge-ok-bg:#065f46;--badge-ok-fg:#6ee7b7;
  --badge-err-bg:#7f1d1d;--badge-err-fg:#fca5a5;
  --badge-warn-bg:#78350f;--badge-warn-fg:#fcd34d;
  --badge-inactive-bg:#1e293b;--badge-inactive-fg:#64748b;
  /* Direction badges */
  --dir-long-bg:#052e16;--dir-long-fg:#4ade80;--dir-long-border:#166534;
  --dir-short-bg:#450a0a;--dir-short-fg:#f87171;--dir-short-border:#7f1d1d;
  /* Chart text */
  --chart-text:#94a3b8;--chart-grid:rgba(148,163,184,.12);
}}
/* ── Light mode ─────────────────────────────────────────────────────── */
[data-theme="light"]{{
  --bg:#f0f4f8;--bg-grad:none;
  --card-bg:linear-gradient(135deg,#ffffff,#f8fafc);--card-border:#cbd5e1;
  --text:#1e293b;--text-dim:#64748b;--text-muted:#94a3b8;
  --th-color:#64748b;--td-border:#e2e8f0;--h2-color:#475569;--h2-border:#e2e8f0;
  --btn-bg:#f1f5f9;--btn-border:#cbd5e1;--btn-color:#2563eb;
  --btn-hover-bg:#dbeafe;--btn-hover-border:#93c5fd;
  --result-bg:#ffffff;--result-border:#e2e8f0;
  --pf-card-bg:#f8fafc;--pf-card-border:#e2e8f0;
  --run-hover:#f1f5f9;--run-detail-bg:#f8fafc;
  --input-bg:#ffffff;--input-border:#cbd5e1;--input-color:#1e293b;--input-select-color:#475569;
  --accent:#2563eb;--title-color:#1d4ed8;--sub-color:#d97706;
  --footer-color:#94a3b8;--section-label-color:#64748b;
  --pnl-pos:#16a34a;--pnl-neg:#dc2626;
  --badge-ok-bg:#dcfce7;--badge-ok-fg:#166534;
  --badge-err-bg:#fee2e2;--badge-err-fg:#991b1b;
  --badge-warn-bg:#fef3c7;--badge-warn-fg:#92400e;
  --badge-inactive-bg:#f1f5f9;--badge-inactive-fg:#64748b;
  --dir-long-bg:#dcfce7;--dir-long-fg:#166534;--dir-long-border:#86efac;
  --dir-short-bg:#fee2e2;--dir-short-fg:#991b1b;--dir-short-border:#fca5a5;
  --chart-text:#475569;--chart-grid:rgba(71,85,105,.12);
}}
/* ── Colorblind mode — blue/orange replaces green/red ───────────────── */
[data-cb="1"]{{
  --pnl-pos:#3b82f6;--pnl-neg:#f97316;
  --badge-ok-bg:#1e3a5f;--badge-ok-fg:#93c5fd;
  --badge-err-bg:#431407;--badge-err-fg:#fdba74;
  --dir-long-bg:#1e3a5f;--dir-long-fg:#93c5fd;--dir-long-border:#3b82f6;
  --dir-short-bg:#431407;--dir-short-fg:#fdba74;--dir-short-border:#f97316;
}}
[data-theme="light"][data-cb="1"]{{
  --pnl-pos:#1d4ed8;--pnl-neg:#ea580c;
  --badge-ok-bg:#dbeafe;--badge-ok-fg:#1e40af;
  --badge-err-bg:#ffedd5;--badge-err-fg:#9a3412;
  --dir-long-bg:#dbeafe;--dir-long-fg:#1e40af;--dir-long-border:#93c5fd;
  --dir-short-bg:#ffedd5;--dir-short-fg:#9a3412;--dir-short-border:#fdba74;
}}
/* ── Base styles ─────────────────────────────────────────────────────── */
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--text);font-family:'Courier New',monospace;padding:24px;
  background-image:var(--bg-grad);transition:background .2s,color .2s}}
a{{color:var(--accent);text-decoration:none}}a:hover{{opacity:.8}}
.header{{text-align:center;margin-bottom:32px;position:relative}}
.header h1{{font-size:22px;color:var(--title-color);letter-spacing:3px}}
.header .sub{{font-size:12px;color:var(--sub-color);letter-spacing:1px;margin-top:4px}}
.header-status{{display:flex;gap:8px;justify-content:center;align-items:center;margin-top:10px;flex-wrap:wrap}}
.badge-online{{display:inline-block;background:var(--badge-ok-bg);color:var(--badge-ok-fg);
  padding:3px 10px;border-radius:4px;font-size:11px;letter-spacing:1px}}
.btn-icon{{background:var(--btn-bg);border:1px solid var(--btn-border);border-radius:6px;
  padding:4px 9px;font-size:14px;cursor:pointer;transition:all .15s;line-height:1;
  color:var(--text);font-family:inherit}}
.btn-icon:hover{{background:var(--btn-hover-bg);border-color:var(--btn-hover-border)}}
.btn-icon.icon-active{{background:var(--btn-hover-bg);border-color:var(--accent)}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;max-width:1200px;margin:0 auto}}
@media(max-width:800px){{.grid{{grid-template-columns:1fr}}}}
.card{{background:var(--card-bg);border:1px solid var(--card-border);border-radius:10px;padding:20px;transition:background .2s,border-color .2s}}
.card h2{{font-size:13px;color:var(--h2-color);letter-spacing:2px;margin-bottom:12px;
  border-bottom:1px solid var(--h2-border);padding-bottom:8px}}
.full{{grid-column:1/-1}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;color:var(--th-color);font-size:11px;letter-spacing:1px;
  padding:6px 8px;border-bottom:1px solid var(--h2-border)}}
td{{padding:6px 8px;border-bottom:1px solid var(--td-border)}}
tr:last-child td{{border-bottom:none}}
tfoot td{{border-top:1px solid var(--h2-border);border-bottom:none;padding-top:8px}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
.badge{{padding:2px 8px;border-radius:3px;font-size:11px;letter-spacing:1px}}
.badge.ok,.badge.active{{background:var(--badge-ok-bg);color:var(--badge-ok-fg)}}
.badge.inactive{{background:var(--badge-inactive-bg);color:var(--badge-inactive-fg)}}
.badge.err{{background:var(--badge-err-bg);color:var(--badge-err-fg)}}
.badge.warn{{background:var(--badge-warn-bg);color:var(--badge-warn-fg)}}
.badge-dir{{padding:2px 7px;border-radius:3px;font-size:11px}}
.badge-dir.long{{background:var(--dir-long-bg);color:var(--dir-long-fg);border:1px solid var(--dir-long-border)}}
.badge-dir.short{{background:var(--dir-short-bg);color:var(--dir-short-fg);border:1px solid var(--dir-short-border)}}
.badge-human{{background:#92400e;color:#fcd34d;padding:2px 7px;border-radius:3px;font-size:10px;letter-spacing:1px}}
.badge-type,.acct-type{{padding:2px 7px;border-radius:3px;font-size:10px;letter-spacing:1px}}
.badge-type.paper,.acct-type.type-paper{{background:#1e3a5f;color:#93c5fd}}
.badge-type.live,.acct-type.type-live{{background:#14532d;color:#86efac}}
.badge-type.physical,.acct-type.type-physical{{background:#451a03;color:#fbbf24}}
.pnl-pos{{color:var(--pnl-pos)}}
.pnl-neg{{color:var(--pnl-neg)}}
.stat-row{{display:flex;gap:24px;flex-wrap:wrap;margin-bottom:16px}}
.stat{{text-align:center;flex:1;min-width:100px}}
.stat .val{{font-size:22px;color:var(--text);font-weight:bold}}
.stat .label{{font-size:10px;color:var(--text-muted);letter-spacing:1px;margin-top:2px}}
.dim{{color:var(--text-dim);font-style:italic}}
.links{{display:flex;gap:8px;flex-wrap:wrap;align-items:center}}
.btn{{background:var(--btn-bg);padding:6px 14px;border-radius:6px;border:1px solid var(--btn-border);
  font-size:12px;transition:all .15s;cursor:pointer;color:var(--btn-color);font-family:inherit}}
.btn:hover{{background:var(--btn-hover-bg);border-color:var(--btn-hover-border)}}
.btn.active-btn{{background:var(--btn-hover-bg);border-color:var(--accent);color:var(--text)}}
.btn-scout{{background:linear-gradient(135deg,#1e3a5f,#2d4a7a);border-color:#3b82f6;color:#93c5fd}}
.btn-scout:hover{{background:linear-gradient(135deg,#2563eb,#1d4ed8);color:#fff}}
.btn-scout:disabled,.btn-pipeline:disabled{{opacity:0.5;cursor:wait}}
.btn-pipeline{{background:linear-gradient(135deg,#3b0764,#5b21b6);border-color:#7c3aed;color:#c4b5fd}}
.btn-pipeline:hover{{background:linear-gradient(135deg,#5b21b6,#7c3aed);color:#fff}}
.btn-json{{padding:2px 10px;font-size:10px;letter-spacing:.5px;color:var(--text-muted);border-color:var(--result-border)}}
.btn-json:hover{{color:var(--text);border-color:var(--accent)}}
.crew-form{{display:flex;gap:8px;align-items:center;margin-top:10px;flex-wrap:wrap}}
.crew-form input,.crew-form select{{background:var(--input-bg);border:1px solid var(--input-border);
  border-radius:6px;padding:6px 10px;color:var(--input-color);font-family:inherit;font-size:12px;outline:none}}
.crew-form input{{width:220px}}.crew-form input:focus,.crew-form select:focus{{border-color:var(--accent)}}
.crew-form select{{color:var(--input-select-color)}}
#run-status{{font-size:11px;color:var(--text-muted);margin-left:4px}}
#result-panel{{background:var(--result-bg);border:1px solid var(--result-border);border-radius:8px;
  padding:16px;margin-top:14px;display:none;max-height:560px;overflow:auto;font-size:12px;line-height:1.6}}
.result-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;gap:8px}}
.result-title{{color:var(--sub-color);font-size:11px;letter-spacing:1px;flex:1;min-width:0;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.result-actions{{display:flex;gap:6px;align-items:center;flex-shrink:0}}
.result-close{{cursor:pointer;color:var(--text-muted);font-size:14px;padding:2px 6px;border-radius:3px}}
.result-close:hover{{color:var(--pnl-neg);background:var(--btn-bg)}}
#result-formatted table{{font-size:12px}}
#result-formatted .pf-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}}
#result-formatted .pf-card{{background:var(--pf-card-bg);border:1px solid var(--pf-card-border);border-radius:8px;padding:14px}}
#result-formatted .pf-name{{font-size:13px;color:var(--text);font-weight:bold;margin-bottom:4px;
  display:flex;gap:6px;align-items:center;flex-wrap:wrap}}
#result-formatted .pf-broker{{font-size:10px;color:var(--text-muted);letter-spacing:1px;margin-bottom:8px}}
#result-formatted .pf-balance{{font-size:20px;color:var(--accent);font-weight:bold;margin-bottom:8px}}
.run-row{{cursor:pointer;transition:background .1s}}
.run-row:hover td{{background:var(--run-hover)}}
.pf-row{{transition:background .15s}}
.pf-row:hover td{{background:rgba(59,130,246,.08)}}
.run-detail td{{background:var(--run-detail-bg)!important}}
#result-json{{white-space:pre-wrap;word-break:break-word;color:var(--text);display:none}}
.key{{color:#7dd3fc}}.str{{color:#86efac}}.num-val{{color:#fcd34d}}
.bool{{color:#c084fc}}.null-val{{color:var(--text-muted)}}
.footer{{text-align:center;margin-top:24px;font-size:10px;color:var(--footer-color)}}
.section-label{{font-size:10px;color:var(--section-label-color);letter-spacing:1px;margin-bottom:8px;margin-top:12px}}
.chart-row{{display:flex;gap:16px;margin-top:20px;flex-wrap:wrap}}
.chart-wrap{{flex:1;min-width:160px;max-width:260px}}
.chart-label{{font-size:10px;color:var(--text-muted);letter-spacing:1px;text-align:center;margin-bottom:6px}}
</style>
</head>
<body>

<div class="header">
  <h1>USS TRADEMINDS</h1>
  <div class="sub">UNIFIED TRADER &mdash; CrewAI + Multi-Portfolio</div>
  <div class="header-status">
    <div class="badge-online">ONLINE &mdash; Port 8000 &mdash; Uptime {uptime_str}</div>
    <button class="btn-icon" id="theme-btn" onclick="toggleTheme()" title="Toggle light/dark mode">🌙</button>
    <button class="btn-icon" id="cb-btn" onclick="toggleCB()" title="Toggle colorblind mode">&#128065;</button>
  </div>
</div>

<div class="grid">

  <div class="card full">
    <h2>CREW CONTROL</h2>
    <div class="links">
      <button class="btn" onclick="fetchAPI('/api/portfolios/','Portfolios')">Portfolios</button>
      <button class="btn" onclick="fetchAPI('/api/portfolios/unified','Unified P&amp;L')">Unified P&amp;L</button>
      <button class="btn" onclick="fetchAPI('/api/portfolios/exposure','Exposure')">Exposure</button>
      <button class="btn" onclick="fetchAPI('/api/portfolios/positions/open','Open Positions')">Open Positions</button>
      <button class="btn" onclick="fetchAPI('/api/positions','Starfleet Positions')">Starfleet Positions</button>
      <button class="btn" onclick="fetchAPI('/api/crew/strategies','Strategies')">Strategies</button>
      <button class="btn" onclick="fetchAPI('/api/crew/runs','Crew Runs')">Crew Runs</button>
      <a class="btn" href="/docs" target="_blank">Swagger Docs</a>
      <a class="btn" href="http://localhost:8080" target="_blank">Arena &rarr;</a>
    </div>
    <div class="crew-form">
      <input type="text" id="run-topic" placeholder="Focus area (e.g. semiconductors)">
      <select id="run-asset-class">
        <option value="stock">Stock</option>
        <option value="option">Options</option>
        <option value="metals">Metals</option>
      </select>
      <button class="btn btn-scout" id="scout-btn" onclick="runScout()">&#9741; Scout</button>
      <button class="btn btn-pipeline" id="pipeline-btn" onclick="runPipeline()">&#9654; Full Pipeline</button>
      <button class="btn" onclick="forceScan()" title="Kill Ollama lock and run Scout immediately" style="background:#1a2e1a;border-color:#16a34a;color:#4ade80">&#9889; Force Scan</button>
      <span id="run-status"></span>
    </div>
    <div id="ollama-lock-bar" style="display:flex;align-items:center;gap:10px;padding:8px 12px;margin-top:8px;border-radius:6px;font-size:12px;background:#0d1526;border:1px solid #1e3a5f">
      <span style="font-size:10px;color:#64748b;letter-spacing:1px;font-weight:700">OLLAMA LOCK</span>
      <span id="lock-dot" style="width:9px;height:9px;border-radius:50%;background:#22c55e;display:inline-block;flex-shrink:0"></span>
      <span id="lock-status-text" style="color:#94a3b8;flex:1">Checking…</span>
      <button id="kill-lock-btn" onclick="killLock()" style="display:none;background:#7f1d1d;border:1px solid #ef4444;color:#fca5a5;padding:3px 12px;border-radius:5px;cursor:pointer;font-size:11px;font-weight:700">&#9760; Kill Lock</button>
    </div>
    <div id="result-panel">
      <div class="result-header">
        <span class="result-title" id="result-title">RESPONSE</span>
        <div class="result-actions">
          <button id="json-toggle-btn" class="btn btn-json" onclick="toggleJson()" style="display:none">Show JSON</button>
          <span class="result-close" onclick="closeResult()">[X]</span>
        </div>
      </div>
      <div id="result-formatted"></div>
      <pre id="result-json"></pre>
    </div>
  </div>

  <div class="card">
    <h2>PORTFOLIO OVERVIEW</h2>
    <div class="stat-row">
      <div class="stat"><div class="val">${total_balance:,.0f}</div><div class="label">TOTAL BALANCE</div></div>
      <div class="stat"><div class="val">{open_positions}</div><div class="label">OPEN POSITIONS</div></div>
      <div class="stat"><div class="val">${realized:,.2f}</div><div class="label">REALIZED P&amp;L</div></div>
      <div class="stat"><div class="val">{strat_count}</div><div class="label">STRATEGIES</div></div>
    </div>
    <table>
      <tr><th>NAME</th><th>BROKER</th><th>TYPE</th><th style="text-align:right">BALANCE</th><th>STATUS</th></tr>
      {pf_rows}
    </table>
    <div class="chart-row">
      <div class="chart-wrap">
        <div class="chart-label">BY PORTFOLIO</div>
        <canvas id="chart-pf" height="190"></canvas>
      </div>
      <div class="chart-wrap">
        <div class="chart-label">BY ASSET CLASS</div>
        <canvas id="chart-ac" height="190"></canvas>
      </div>
    </div>
  </div>

  <div class="card full">
    <h2>STARFLEET POSITIONS</h2>
    <div class="dim" style="font-size:11px;margin-bottom:10px">Arena positions from the legacy <code>positions</code> table on port 8000.</div>
    <div id="starfleet-positions-panel"><div class="dim" style="padding:12px">Loading positions…</div></div>
  </div>

  <!-- Portfolio Detail Panel -->
  <div id="pf-detail-panel" style="display:none;margin-bottom:20px" class="card">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
      <button onclick="hidePortfolioDetail()" style="background:#1e293b;border:1px solid #334155;color:#60a5fa;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600">&#8592; BACK</button>
      <h2 id="pf-detail-title" style="margin:0;font-size:15px;color:#94a3b8">PORTFOLIO DETAIL</h2>
      <span id="pf-detail-badge" style="margin-left:auto"></span>
    </div>
    <div id="pf-detail-stats" class="stat-row" style="margin-bottom:16px"></div>
    <div class="section-label">OPEN POSITIONS</div>
    <div id="pf-detail-positions"></div>
    <div class="section-label" style="margin-top:16px">RECENT CLOSED TRADES</div>
    <div id="pf-detail-trades"></div>
  </div>

  <div class="card">
    <h2>RECENT CREW RUNS</h2>
    <table>
      <tr><th>ID</th><th>TYPE</th><th>OUTCOME</th><th style="text-align:right">DURATION</th><th>TIMESTAMP</th></tr>
      {run_rows}
    </table>
  </div>

</div>

<div class="footer">NCC-1701-D &bull; Unified Trader &bull; Auto-refresh paused while result panel open</div>

<script>
/* ── Theme & CB persistence ──────────────────────────────────────────── */
(function(){{
  const th=localStorage.getItem('tm-theme')||'dark';
  const cb=localStorage.getItem('tm-cb')||'0';
  if(th==='light') document.documentElement.setAttribute('data-theme','light');
  if(cb==='1') document.documentElement.setAttribute('data-cb','1');
}})();

const _REFRESH_MS=60000;
const _LOCK_POLL_MS=10000;
let _refreshTimer=null;
let _lockPollTimer=null;
let _lastRawData=null;
let _showingJson=false;

function _pageVisible(){{return document.visibilityState==='visible';}}

function startRefresh(){{
  stopRefresh();
  _refreshTimer=setInterval(()=>{{if(_pageVisible()) location.reload();}},_REFRESH_MS);
}}

function stopRefresh(){{if(_refreshTimer){{clearInterval(_refreshTimer);_refreshTimer=null;}}}}

function toggleTheme(){{
  const html=document.documentElement;
  const isLight=html.getAttribute('data-theme')==='light';
  if(isLight){{html.removeAttribute('data-theme');localStorage.setItem('tm-theme','dark');}}
  else{{html.setAttribute('data-theme','light');localStorage.setItem('tm-theme','light');}}
  document.getElementById('theme-btn').textContent=isLight?'🌙':'☀️';
  _rebuildCharts();
}}

function toggleCB(){{
  const html=document.documentElement;
  const isCB=html.getAttribute('data-cb')==='1';
  if(isCB){{html.removeAttribute('data-cb');localStorage.setItem('tm-cb','0');}}
  else{{html.setAttribute('data-cb','1');localStorage.setItem('tm-cb','1');}}
  document.getElementById('cb-btn').classList.toggle('icon-active',!isCB);
  _rebuildCharts();
}}

/* Apply persisted state to buttons on load */
window.addEventListener('DOMContentLoaded',()=>{{
  const th=localStorage.getItem('tm-theme')||'dark';
  const cb=localStorage.getItem('tm-cb')||'0';
  document.getElementById('theme-btn').textContent=th==='light'?'☀️':'🌙';
  if(cb==='1') document.getElementById('cb-btn').classList.add('icon-active');
  _buildCharts();
  loadStarfleetPositions();
}});

function syntaxHL(j){{
  if(typeof j!=='string')j=JSON.stringify(j,null,2);
  return j.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"([^"]+)":/g,'<span class="key">"$1"</span>:')
    .replace(/: "([^"]*)"/g,': <span class="str">"$1"</span>')
    .replace(/: (\\d+\\.?\\d*)/g,': <span class="num-val">$1</span>')
    .replace(/: (true|false)/g,': <span class="bool">$1</span>')
    .replace(/: (null)/g,': <span class="null-val">$1</span>');
}}

function fmt$(n){{return '$'+(n||0).toLocaleString('en-US',{{minimumFractionDigits:2,maximumFractionDigits:2}})}}
function fmtPnl(n){{
  const cb=document.documentElement.getAttribute('data-cb')==='1';
  const prefix=cb?(n>=0?'▲ ':'▼ '):(n>=0?'+':'-');
  return prefix+'$'+Math.abs(n||0).toFixed(2);
}}
function pnlCls(n){{return n>=0?'pnl-pos':'pnl-neg'}}

/* ── Renderers ─────────────────────────────────────────────────────────── */

function renderPositions(data){{
  const rows=Array.isArray(data)?data:(data.positions||[]);
  if(!rows.length) return '<div class="dim" style="padding:12px">No open positions</div>';
  let totalPnl=0, totalMktVal=0;
  const trs=rows.map(p=>{{
    const pnl=p.unrealized_pnl||0;
    const isMetal=p.asset_class==='metal';
    totalPnl+=pnl;
    if(isMetal) totalMktVal+=(p.current_price||0)*(p.metal_oz||p.quantity||0);

    const chg=p.entry_price?((p.current_price-p.entry_price)/p.entry_price*100):0;
    const dir=p.direction==='long'?'<span class="badge-dir long">&#9650; LONG</span>':'<span class="badge-dir short">&#9660; SHORT</span>';

    // Metals: show oz + metal_type label; stocks: show quantity
    const qtyCell=isMetal
      ? `${{p.metal_oz}} oz<br><span style="color:#fbbf24;font-size:10px">${{(p.metal_type||'').toUpperCase()}}</span>`
      : p.quantity;

    // Metals: entry per oz; stocks: entry price
    const entryCell=isMetal
      ? (p.entry_price>0?fmt$(p.entry_price)+'/oz':'<span class="dim">unknown basis</span>')
      : fmt$(p.entry_price);

    const curCell=isMetal
      ? `${{fmt$(p.current_price)}}/oz<br><span class="dim" style="font-size:10px">MV: ${{fmt$(p.current_price*(p.metal_oz||0))}}</span>`
      : `${{fmt$(p.current_price)}}<br><span class="dim" style="font-size:10px">${{chg>=0?'+':''}}${{chg.toFixed(2)}}%</span>`;

    const stopCell=isMetal?'<span class="dim">—</span>':(p.stop_loss?fmt$(p.stop_loss):'—');
    const tgtCell=isMetal?'<span class="dim">—</span>':(p.take_profit?fmt$(p.take_profit):'—');

    const pnlCell=isMetal&&p.entry_price===0
      ? `<span class="dim" style="font-size:10px">basis unknown</span>`
      : `<span class="${{pnlCls(pnl)}}" style="font-weight:bold">${{fmtPnl(pnl)}}</span>`;

    const portfolio=p.portfolio_name||'';
    return `<tr>
      <td><strong>${{p.ticker}}</strong><br><span class="dim" style="font-size:10px">${{portfolio}}</span></td>
      <td>${{dir}}</td>
      <td class="num">${{qtyCell}}</td>
      <td class="num">${{entryCell}}</td>
      <td class="num">${{curCell}}</td>
      <td class="num dim">${{stopCell}}</td>
      <td class="num dim">${{tgtCell}}</td>
      <td class="num">${{pnlCell}}</td>
      <td><span class="badge ok">${{p.status}}</span></td>
    </tr>`;
  }}).join('');
  const knownPnlRows=rows.filter(p=>!(p.asset_class==='metal'&&!p.entry_price));
  const knownPnl=knownPnlRows.reduce((s,p)=>s+(p.unrealized_pnl||0),0);

  // Winners / Losers strip — sort by unrealized_pnl
  const stockRows=rows.filter(p=>p.asset_class!=='metal');
  const byPnl=[...stockRows].sort((a,b)=>(b.unrealized_pnl||0)-(a.unrealized_pnl||0));
  const winners=byPnl.slice(0,5).filter(p=>(p.unrealized_pnl||0)>0);
  const losers=[...byPnl].reverse().slice(0,5).filter(p=>(p.unrealized_pnl||0)<0);
  function wlRow(p,isWin){{
    const pnl=p.unrealized_pnl||0;
    const chg=p.entry_price?((p.current_price-p.entry_price)/p.entry_price*100):0;
    const color=isWin?'#4ade80':'#f87171';
    const arrow=isWin?'&#9650;':'&#9660;';
    return `<tr>
      <td style="font-weight:700">${{arrow}} ${{p.ticker}}</td>
      <td class="dim" style="font-size:10px">${{p.portfolio_name||''}}</td>
      <td class="num" style="color:${{color}};font-weight:700">${{fmtPnl(pnl)}}</td>
      <td class="num" style="color:${{color}}">${{chg>=0?'+':''}}${{chg.toFixed(2)}}%</td>
      <td class="num">${{fmt$(p.current_price||0)}}</td>
    </tr>`;
  }}
  const wlHtml=(winners.length||losers.length)?`
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px">
      <div>
        <div style="font-size:10px;font-weight:700;color:#4ade80;letter-spacing:1px;padding:4px 0 6px">&#9650; TOP WINNERS</div>
        ${{winners.length?`<table><thead><tr><th>TICKER</th><th>PORTFOLIO</th><th class="num">UNREAL P&amp;L</th><th class="num">RETURN</th><th class="num">PRICE</th></tr></thead><tbody>${{winners.map(p=>wlRow(p,true)).join('')}}</tbody></table>`:'<div class="dim" style="font-size:11px">None in profit</div>'}}
      </div>
      <div>
        <div style="font-size:10px;font-weight:700;color:#f87171;letter-spacing:1px;padding:4px 0 6px">&#9660; TOP LOSERS</div>
        ${{losers.length?`<table><thead><tr><th>TICKER</th><th>PORTFOLIO</th><th class="num">UNREAL P&amp;L</th><th class="num">RETURN</th><th class="num">PRICE</th></tr></thead><tbody>${{losers.map(p=>wlRow(p,false)).join('')}}</tbody></table>`:'<div class="dim" style="font-size:11px">None underwater</div>'}}
      </div>
    </div>`:'';

  return wlHtml+`<table>
    <thead><tr><th>TICKER</th><th>DIR</th><th class="num">QTY / OZ</th><th class="num">ENTRY</th>
      <th class="num">CURRENT</th><th class="num">STOP</th><th class="num">TARGET</th>
      <th class="num">UNREAL P&amp;L</th><th>STATUS</th></tr></thead>
    <tbody>${{trs}}</tbody>
    <tfoot><tr>
      <td colspan="7" style="text-align:right;font-size:11px;color:#64748b">KNOWN P&amp;L (excl. unknown-basis)</td>
      <td class="num ${{pnlCls(knownPnl)}}" style="font-weight:bold">${{fmtPnl(knownPnl)}}</td>
      <td></td>
    </tr></tfoot>
  </table>`;
}}

function renderStarfleetPositions(data){{
  const rows=Array.isArray(data)?data:(data.positions||[]);
  if(!rows.length) return '<div class="dim" style="padding:12px">No Arena positions found in data/trader.db</div>';
  const trs=rows.map(p=>{{
    const asset=(p.asset_type||'stock').toUpperCase();
    const optionBits=(p.asset_type==='option')
      ? `<br><span class="dim" style="font-size:10px">${{(p.option_type||'').toUpperCase()}} ${{p.strike_price||'—'}} ${{p.expiration_date||'—'}}</span>`
      : '';
    const provider=p.provider?`<span class="dim" style="font-size:10px">${{p.provider}}</span>`:'';
    const opened=(p.opened_at||'').replace('T',' ').substring(0,16);
    const current=(p.current_price||0);
    const entry=(p.entry_price||0);
    const change=entry?(((current-entry)/entry)*100):0;
    const currentCell=current
      ? `${{fmt$(current)}}<br><span class="dim" style="font-size:10px">${{change>=0?'+':''}}${{change.toFixed(2)}}%</span>`
      : '<span class="dim">—</span>';
    const hwm=(p.high_watermark && p.high_watermark!==p.current_price)?fmt$(p.high_watermark):'—';
    return `<tr>
      <td><strong>${{p.ticker}}</strong>${{optionBits}}</td>
      <td><strong>${{p.player_name||p.player_id}}</strong><br>${{provider}}</td>
      <td>${{asset}}</td>
      <td class="num">${{p.quantity||0}}</td>
      <td class="num">${{fmt$(entry)}}</td>
      <td class="num">${{currentCell}}</td>
      <td class="num">${{hwm}}</td>
      <td class="dim" style="font-size:11px">${{opened||'—'}}</td>
    </tr>`;
  }}).join('');
  return `<div style="font-size:11px;color:#64748b;margin-bottom:10px">Rows loaded: ${{rows.length}}</div>
  <table><thead><tr><th>TICKER</th><th>PLAYER</th><th>ASSET</th><th class="num">QTY</th>
    <th class="num">ENTRY</th><th class="num">CURRENT</th><th class="num">HIGH WATER</th><th>OPENED</th></tr></thead>
  <tbody>${{trs}}</tbody></table>`;
}}

function renderPortfolios(data){{
  const pfs=Array.isArray(data)?data:(data.portfolios||[]);
  if(!pfs.length) return '<div class="dim" style="padding:12px">No portfolios</div>';
  const cards=pfs.map(p=>{{
    const bal=p.current_balance||p.balance||0;
    const humanBadge=p.is_human?'<span class="badge-human">HUMAN</span>':'';
    const acctType=p.account_type||'paper';
    const typeBadge=`<span class="badge-type ${{acctType}}">${{acctType.toUpperCase()}}</span>`;
    const statusBadge=p.is_active?'<span class="badge ok">ACTIVE</span>':'<span class="badge inactive">INACTIVE</span>';
    return `<div class="pf-card" style="cursor:pointer" onclick="showPortfolioDetail(${{p.id}})">
      <div class="pf-name">${{p.name}} ${{humanBadge}} ${{typeBadge}}</div>
      <div class="pf-broker">${{(p.broker||'').toUpperCase()}}</div>
      <div class="pf-balance">${{fmt$(bal)}}</div>
      <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">${{statusBadge}}
        ${{p.notes?`<span class="dim" style="font-size:10px">${{p.notes.substring(0,60)}}</span>`:''}}
      </div>
    </div>`;
  }}).join('');
  return `<div class="pf-grid">${{cards}}</div>`;
}}

function renderCrewRuns(data){{
  const runs=Array.isArray(data)?data:(data.runs||[]);
  if(!runs.length) return '<div class="dim" style="padding:12px">No crew runs yet</div>';
  const trs=runs.map(r=>{{
    const oc=r.outcome||'unknown';
    const ocCls=oc==='completed'?'ok':oc==='error'?'err':'warn';
    const dur=r.duration_seconds?(r.duration_seconds.toFixed(1)+'s'):'—';
    const ts=(r.created_at||'').replace('T',' ').substring(0,16);
    const detId='rd-'+r.id;
    const detContent=r.error_log
      ?`<span style="color:#fca5a5">ERROR: ${{r.error_log.substring(0,400)}}</span>`
      :`<span style="color:#94a3b8">Agents: ${{r.agents_used||'—'}}</span>${{r.strategy_id?` &bull; <span style="color:#93c5fd">Strategy #${{r.strategy_id}}</span>`:''}}${{r.trigger?` &bull; trigger: ${{r.trigger}}`:''}}`;
    return `<tr class="run-row" onclick="toggleRunDetail('${{detId}}')">
      <td style="color:#94a3b8">#${{r.id}}</td>
      <td><strong>${{r.run_type}}</strong></td>
      <td style="color:#64748b;font-size:11px">${{r.trigger||'—'}}</td>
      <td><span class="badge ${{ocCls}}">${{oc}}</span></td>
      <td class="num">${{dur}}</td>
      <td class="dim" style="font-size:11px">${{ts}}</td>
    </tr>
    <tr id="${{detId}}" class="run-detail" style="display:none">
      <td colspan="6" style="padding:8px 16px;font-size:11px">${{detContent}}</td>
    </tr>`;
  }}).join('');
  return `<table>
    <thead><tr><th>ID</th><th>TYPE</th><th>TRIGGER</th><th>OUTCOME</th>
      <th class="num">DURATION</th><th>TIMESTAMP</th></tr></thead>
    <tbody>${{trs}}</tbody>
  </table>
  <div class="dim" style="font-size:10px;margin-top:8px">&#9654; Click any row to expand details</div>`;
}}

function renderUnified(data){{
  const upnl=data.total_unrealized_pnl||0;
  const rpnl=data.total_realized_pnl||0;
  const ret=data.total_return_pct||0;
  let html=`<div class="stat-row" style="margin-bottom:20px">
    <div class="stat"><div class="val">${{fmt$(data.total_balance)}}</div><div class="label">TOTAL BALANCE</div></div>
    <div class="stat"><div class="val ${{pnlCls(upnl)}}">${{fmtPnl(upnl)}}</div><div class="label">UNREALIZED P&amp;L</div></div>
    <div class="stat"><div class="val ${{pnlCls(rpnl)}}">${{fmtPnl(rpnl)}}</div><div class="label">REALIZED P&amp;L</div></div>
    <div class="stat"><div class="val ${{pnlCls(ret)}}">${{(ret>=0?'+':'')+ret.toFixed(2)}}%</div><div class="label">TOTAL RETURN</div></div>
  </div>`;
  if(data.by_asset_class&&Object.keys(data.by_asset_class).length){{
    const acRows=Object.entries(data.by_asset_class).map(([cls,v])=>{{
      const p=v.unrealized_pnl||0;
      return `<tr><td style="text-transform:uppercase">${{cls}}</td><td class="num">${{v.count}}</td><td class="num ${{pnlCls(p)}}">${{fmtPnl(p)}}</td></tr>`;
    }}).join('');
    html+=`<div class="section-label">BY ASSET CLASS</div>
      <table><thead><tr><th>CLASS</th><th class="num">POSITIONS</th><th class="num">UNREALIZED P&amp;L</th></tr></thead>
      <tbody>${{acRows}}</tbody></table>`;
  }}
  if(data.portfolios&&data.portfolios.length){{
    const pfRows=data.portfolios.map(p=>{{
      const pnl=p.unrealized_pnl||0;
      const hb=p.is_human?'<span class="badge-human" style="font-size:9px">HUMAN</span>':'';
      return `<tr><td>${{p.name}} ${{hb}}</td>
        <td class="num">${{fmt$(p.balance)}}</td>
        <td class="num">${{p.open_positions||0}}</td>
        <td class="num ${{pnlCls(pnl)}}">${{fmtPnl(pnl)}}</td></tr>`;
    }}).join('');
    html+=`<div class="section-label" style="margin-top:16px">BY PORTFOLIO</div>
      <table><thead><tr><th>PORTFOLIO</th><th class="num">BALANCE</th>
        <th class="num">OPEN</th><th class="num">UNREALIZED P&amp;L</th></tr></thead>
      <tbody>${{pfRows}}</tbody></table>`;
  }}
  return html;
}}

function renderExposure(data){{
  const exp=data.exposure||[];
  if(!exp.length) return '<div class="dim" style="padding:12px">No open exposure</div>';
  const rows=exp.map(e=>{{
    const dir=e.direction==='long'?'<span class="badge-dir long">&#9650; LONG</span>':'<span class="badge-dir short">&#9660; SHORT</span>';
    return `<tr><td><strong>${{e.ticker}}</strong></td><td>${{dir}}</td>
      <td class="num">${{e.total_quantity}}</td>
      <td class="num">${{fmt$(e.total_value)}}</td>
      <td class="num" style="color:#64748b">${{e.positions}}</td></tr>`;
  }}).join('');
  return `<div style="font-size:11px;color:#64748b;margin-bottom:10px">Total open positions: ${{data.total_positions||0}}</div>
  <table><thead><tr><th>TICKER</th><th>DIRECTION</th><th class="num">QTY</th>
    <th class="num">MARKET VALUE</th><th class="num">LEGS</th></tr></thead>
  <tbody>${{rows}}</tbody></table>`;
}}

function renderStrategies(data){{
  const strats=Array.isArray(data)?data:(data.strategies||[]);
  if(!strats.length) return '<div class="dim" style="padding:12px">No strategies yet</div>';
  const rows=strats.map(s=>{{
    const stCls={{approved:'ok',active:'ok',rejected:'err',draft:'warn'}}[s.status]||'inactive';
    const conv=s.conviction_score!=null?s.conviction_score.toFixed(1):'—';
    const ts=(s.created_at||'').substring(0,10);
    const dir=s.direction==='long'?'<span class="badge-dir long">LONG</span>':
               s.direction==='short'?'<span class="badge-dir short">SHORT</span>':
               `<span style="color:#94a3b8">${{s.direction||'—'}}</span>`;
    return `<tr>
      <td style="color:#64748b">#${{s.id}}</td>
      <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
          title="${{(s.name||'').replace(/"/g,'&quot;')}}">${{s.name||'—'}}</td>
      <td><span class="badge ${{stCls}}">${{s.status}}</span></td>
      <td style="color:#94a3b8;font-size:11px">${{s.asset_class||'—'}}</td>
      <td>${{dir}}</td>
      <td class="num">${{conv}}</td>
      <td class="dim" style="font-size:11px">${{ts}}</td>
    </tr>`;
  }}).join('');
  return `<table><thead><tr><th>ID</th><th>NAME</th><th>STATUS</th><th>CLASS</th>
    <th>DIR</th><th class="num">CONVICTION</th><th>CREATED</th></tr></thead>
  <tbody>${{rows}}</tbody></table>`;
}}

const RENDERERS={{
  '/api/portfolios/': renderPortfolios,
  '/api/portfolios/unified': renderUnified,
  '/api/portfolios/exposure': renderExposure,
  '/api/portfolios/positions/open': renderPositions,
  '/api/positions': renderStarfleetPositions,
  '/api/crew/strategies': renderStrategies,
  '/api/crew/runs': renderCrewRuns,
}};

/* ── Panel helpers ─────────────────────────────────────────────────────── */

function showResult(title, data, renderedHtml){{
  stopRefresh();
  _lastRawData=data;
  _showingJson=false;
  document.getElementById('result-title').textContent=title;
  const fmtDiv=document.getElementById('result-formatted');
  const jsonPre=document.getElementById('result-json');
  const toggleBtn=document.getElementById('json-toggle-btn');
  if(renderedHtml!=null){{
    fmtDiv.innerHTML=renderedHtml;
    fmtDiv.style.display='block';
    jsonPre.style.display='none';
    toggleBtn.style.display='inline-block';
    toggleBtn.textContent='Show JSON';
  }}else{{
    fmtDiv.innerHTML='';
    fmtDiv.style.display='none';
    jsonPre.innerHTML=syntaxHL(typeof data==='string'?data:JSON.stringify(data,null,2));
    jsonPre.style.display='block';
    toggleBtn.style.display='none';
  }}
  document.getElementById('result-panel').style.display='block';
  document.getElementById('result-panel').scrollTop=0;
}}

function toggleJson(){{
  const fmtDiv=document.getElementById('result-formatted');
  const jsonPre=document.getElementById('result-json');
  const btn=document.getElementById('json-toggle-btn');
  _showingJson=!_showingJson;
  if(_showingJson){{
    jsonPre.innerHTML=syntaxHL(typeof _lastRawData==='string'?_lastRawData:JSON.stringify(_lastRawData,null,2));
    jsonPre.style.display='block';
    fmtDiv.style.display='none';
    btn.textContent='Show Formatted';
  }}else{{
    jsonPre.style.display='none';
    fmtDiv.style.display='block';
    btn.textContent='Show JSON';
  }}
}}

function toggleRunDetail(id){{
  const row=document.getElementById(id);
  if(row) row.style.display=row.style.display==='none'?'table-row':'none';
}}

/* ── Portfolio Detail ───────────────────────────────────────────────────── */

function _renderClosedTrades(rows){{
  if(!rows||!rows.length) return '<div class="dim" style="padding:10px">No closed trades</div>';
  const trs=rows.map(r=>{{
    const pnl=r.closed_pnl||0;
    const cls=pnl>=0?'pnl-pos':'pnl-neg';
    const pnlTxt=(pnl>=0?'+':'')+fmt$(pnl);
    const date=(r.closed_at||r.created_at||'').substring(0,16).replace('T',' ');
    return `<tr>
      <td><strong>${{r.ticker}}</strong></td>
      <td style="color:#94a3b8;font-size:11px">${{r.asset_class||'stock'}}</td>
      <td class="num">${{r.quantity}}</td>
      <td class="num">${{fmt$(r.entry_price||0)}}</td>
      <td class="num">${{fmt$(r.current_price||0)}}</td>
      <td class="num ${{cls}}" style="font-weight:bold">${{pnlTxt}}</td>
      <td class="dim" style="font-size:10px">${{date}}</td>
    </tr>`;
  }}).join('');
  return `<table><thead><tr><th>TICKER</th><th>CLASS</th><th class="num">QTY</th>
    <th class="num">ENTRY</th><th class="num">EXIT</th><th class="num">P&amp;L</th><th>CLOSED</th></tr></thead>
    <tbody>${{trs}}</tbody></table>`;
}}

function showPortfolioDetail(pfId){{
  stopRefresh();
  const panel=document.getElementById('pf-detail-panel');
  panel.style.display='block';
  panel.scrollIntoView({{behavior:'smooth',block:'start'}});
  document.getElementById('pf-detail-title').textContent='Loading…';
  document.getElementById('pf-detail-stats').innerHTML='';
  document.getElementById('pf-detail-positions').innerHTML='<div class="dim">Loading positions…</div>';
  document.getElementById('pf-detail-trades').innerHTML='<div class="dim">Loading trades…</div>';

  Promise.all([
    fetch(`/api/portfolios/${{pfId}}`).then(r=>r.json()),
    fetch(`/api/portfolios/positions/open?portfolio_id=${{pfId}}`).then(r=>r.json()),
    fetch(`/api/portfolios/${{pfId}}/closed_trades?limit=20`).then(r=>r.json()),
  ]).then(([pf, positions, trades])=>{{
    const initBal=pf.initial_balance||pf.current_balance||0;
    const totalUnrealized=(Array.isArray(positions)?positions:[]).reduce((s,p)=>s+(p.unrealized_pnl||0),0);
    const realizedRows=trades.filter(t=>t.closed_pnl!=null);
    const totalRealized=realizedRows.reduce((s,t)=>s+(t.closed_pnl||0),0);
    const balance=initBal+totalUnrealized+totalRealized;
    const ret=initBal?(((balance-initBal)/initBal)*100):0;
    const human=pf.is_human?'<span class="badge-human">HUMAN</span>':'';
    const acctType=pf.account_type||'paper';
    const typeCls={{'paper':'type-paper','live':'type-live','physical':'type-physical'}}[acctType]||'';
    document.getElementById('pf-detail-title').textContent=pf.name||`Portfolio #${{pfId}}`;
    document.getElementById('pf-detail-badge').innerHTML=`${{human}} <span class="acct-type ${{typeCls}}">${{acctType.toUpperCase()}}</span> <span class="dim" style="font-size:11px">${{(pf.broker||'').toUpperCase()}}</span>`;

    document.getElementById('pf-detail-stats').innerHTML=`
      <div class="stat"><div class="val">${{fmt$(balance)}}</div><div class="label">BALANCE</div></div>
      <div class="stat"><div class="val ${{pnlCls(ret)}}">${{(ret>=0?'+':'')+ret.toFixed(2)}}%</div><div class="label">RETURN</div></div>
      <div class="stat"><div class="val ${{pnlCls(totalUnrealized)}}">${{fmtPnl(totalUnrealized)}}</div><div class="label">UNREALIZED P&amp;L</div></div>
      <div class="stat"><div class="val ${{pnlCls(totalRealized)}}">${{fmtPnl(totalRealized)}}</div><div class="label">REALIZED P&amp;L</div></div>
      <div class="stat"><div class="val">${{Array.isArray(positions)?positions.length:0}}</div><div class="label">OPEN</div></div>
      <div class="stat"><div class="val">${{trades.length}}</div><div class="label">CLOSED TRADES</div></div>
    `;
    document.getElementById('pf-detail-positions').innerHTML=renderPositions(Array.isArray(positions)?positions:[]);
    document.getElementById('pf-detail-trades').innerHTML=_renderClosedTrades(Array.isArray(trades)?trades:[]);
  }}).catch(err=>{{
    document.getElementById('pf-detail-title').textContent='Error loading portfolio';
    document.getElementById('pf-detail-positions').innerHTML=`<div style="color:#fca5a5">${{err.message}}</div>`;
  }});
}}

function hidePortfolioDetail(){{
  document.getElementById('pf-detail-panel').style.display='none';
  startRefresh();
}}

/* ── Ollama Lock Monitor ────────────────────────────────────────────────── */

function _updateLockUI(d){{
  const dot=document.getElementById('lock-dot');
  const txt=document.getElementById('lock-status-text');
  const killBtn=document.getElementById('kill-lock-btn');
  if(!dot||!txt) return;
  if(d.locked){{
    dot.style.background='#ef4444';
    const elapsed=d.elapsed_seconds||0;
    const elapsedStr=elapsed>=60?Math.floor(elapsed/60)+'m '+(elapsed%60)+'s':elapsed+'s';
    txt.innerHTML=`<span style="color:#fca5a5;font-weight:600">${{d.caller||'unknown'}}</span>`+
      ` &bull; PID ${{d.pid||'?'}} &bull; `+
      `<span style="color:#f59e0b">held ${{elapsedStr}}</span>`+
      (d.acquired_at?` &bull; <span style="color:#64748b;font-size:10px">since ${{d.acquired_at}}</span>`:'');
    killBtn.style.display='inline-block';
  }}else{{
    dot.style.background='#22c55e';
    txt.innerHTML='<span style="color:#4ade80">Free</span>'+(d.caller?` &bull; <span style="color:#475569;font-size:10px">last: ${{d.caller}}</span>`:'');
    killBtn.style.display='none';
  }}
}}

function _pollLock(){{
  if(!_pageVisible()) return;
  fetch('/api/crew/lock/status').then(r=>r.json()).then(_updateLockUI).catch(()=>{{}});
}}

function killLock(){{
  if(!confirm('Force-release the Ollama lock? This will unblock Scout immediately but may corrupt an active Ollama run.')) return;
  fetch('/api/crew/lock/kill',{{method:'POST'}}).then(r=>r.json()).then(d=>{{
    _pollLock();
    const status=document.getElementById('run-status');
    if(status) {{status.textContent='Lock killed — ready to run'; status.style.color='#6ee7b7';}}
  }}).catch(err=>alert('Kill failed: '+err.message));
}}

function forceScan(){{
  // Kill lock first, then immediately run Scout
  fetch('/api/crew/lock/kill',{{method:'POST'}}).then(()=>{{
    _pollLock();
    const status=document.getElementById('run-status');
    if(status){{status.textContent='Lock cleared — launching Scout…'; status.style.color='#f59e0b';}}
    // Small delay to let the kill settle, then fire Scout
    setTimeout(()=>{{
      const topic=document.getElementById('run-topic').value||'market opportunities';
      stopRefresh();
      fetch('/api/crew/scout',{{
        method:'POST',
        headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{focus_area:topic}}),
      }}).then(r=>r.json()).then(d=>{{
        if(status){{status.textContent='Scout complete in '+(d.duration||0).toFixed(1)+'s'; status.style.color='#6ee7b7';}}
        showResult('Force Scout',d,null);
      }}).catch(e=>{{if(status){{status.textContent='Scout error: '+e.message; status.style.color='#fca5a5';}}}});
    }},800);
  }}).catch(err=>alert('Force scan failed: '+err.message));
}}

function startLockPolling(){{
  if(_lockPollTimer) return;
  _lockPollTimer=setInterval(_pollLock, _LOCK_POLL_MS);
}}

startLockPolling();
_pollLock(); // immediate first check

function closeResult(){{
  document.getElementById('result-panel').style.display='none';
  document.querySelectorAll('.btn').forEach(b=>b.classList.remove('active-btn'));
  startRefresh();
}}

function loadStarfleetPositions(){{
  fetch('/api/positions?limit=25').then(r=>r.json()).then(d=>{{
    document.getElementById('starfleet-positions-panel').innerHTML=renderStarfleetPositions(d);
  }}).catch(e=>{{
    document.getElementById('starfleet-positions-panel').innerHTML=`<div style="color:#fca5a5;padding:12px">${{e.message}}</div>`;
  }});
}}

document.addEventListener('visibilitychange',()=>{{
  if(_pageVisible()) _pollLock();
}});

startRefresh();

function fetchAPI(url,title){{
  document.querySelectorAll('.btn').forEach(b=>b.classList.remove('active-btn'));
  event.target.classList.add('active-btn');
  showResult(title+' — loading…','');
  fetch(url).then(r=>r.json()).then(d=>{{
    const renderer=RENDERERS[url];
    showResult(title,d,renderer?renderer(d):null);
  }}).catch(e=>showResult('ERROR — '+title,{{error:e.message}},null));
}}

/* ── Scout / Pipeline ──────────────────────────────────────────────────── */

function _setRunning(running, resultTitle){{
  const scoutBtn=document.getElementById('scout-btn');
  const pipeBtn=document.getElementById('pipeline-btn');
  const status=document.getElementById('run-status');
  scoutBtn.disabled=running; pipeBtn.disabled=running;
  if(running){{
    status.textContent=resultTitle;
    status.style.color='#f59e0b';
  }}
}}

function _onRunDone(d, title){{
  const status=document.getElementById('run-status');
  _setRunning(false);
  status.textContent='Done in '+((d.duration||0)).toFixed(1)+'s';
  status.style.color=(d.status==='completed')?'#6ee7b7':'#fca5a5';
  showResult(title, d, null);
}}

function runScout(){{
  const topic=document.getElementById('run-topic').value||'market opportunities';
  stopRefresh();
  _setRunning(true,'Scout scanning…');
  showResult('SCOUT — '+topic+' — running…','Waiting for Ollama…');
  fetch('/api/crew/scout',{{method:'POST',headers:{{'Content-Type':'application/json'}},
    body:JSON.stringify({{focus_area:topic}})}})
  .then(r=>r.json()).then(d=>_onRunDone(d,'SCOUT — '+topic))
  .catch(e=>{{_setRunning(false);showResult('SCOUT ERROR',{{error:e.message}},null)}});
}}

function runPipeline(){{
  const topic=document.getElementById('run-topic').value||'market opportunities';
  const assetClass=document.getElementById('run-asset-class').value;
  stopRefresh();
  _setRunning(true,'Full pipeline running (may take 10-25 min)…');
  showResult('PIPELINE — '+topic+' — running…','Waiting for Ollama…');
  fetch('/api/crew/run',{{method:'POST',headers:{{'Content-Type':'application/json'}},
    body:JSON.stringify({{focus_area:topic,target_asset_class:assetClass,trigger:'dashboard'}})}})
  .then(r=>r.json()).then(d=>_onRunDone(d,'PIPELINE — '+topic))
  .catch(e=>{{_setRunning(false);showResult('PIPELINE ERROR',{{error:e.message}},null)}});
}}

document.getElementById('run-topic').addEventListener('keydown',e=>{{
  if(e.key==='Enter')runScout();
}});

/* ── Charts ──────────────────────────────────────────────────────────── */
const _PF_LABELS={chart_pf_labels};
const _PF_VALUES={chart_pf_values};
const _AC_LABELS={chart_ac_labels};
const _AC_VALUES={chart_ac_values};

let _chartPf=null, _chartAc=null;

function _chartColors(n){{
  const cb=document.documentElement.getAttribute('data-cb')==='1';
  // CB-safe palette: blue, orange, teal, purple, yellow
  const cbPal=['#3b82f6','#f97316','#06b6d4','#a855f7','#eab308'];
  // Normal dark palette
  const normPal=['#3b82f6','#a855f7','#f59e0b','#06b6d4','#10b981','#ec4899'];
  const pal=cb?cbPal:normPal;
  return Array.from({{length:n}},(_,i)=>pal[i%pal.length]);
}}

function _chartTextColor(){{
  return getComputedStyle(document.documentElement).getPropertyValue('--chart-text').trim()||'#94a3b8';
}}

function _buildCharts(){{
  if(typeof Chart==='undefined') return;
  const textColor=_chartTextColor();
  const commonOpts={{
    responsive:true,maintainAspectRatio:true,
    plugins:{{legend:{{labels:{{color:textColor,font:{{family:"'Courier New',monospace",size:11}},
      boxWidth:12,padding:8}},position:'bottom'}},
      tooltip:{{callbacks:{{label:ctx=>` ${{ctx.label}}: $${{(ctx.raw||0).toLocaleString('en-US',{{maximumFractionDigits:0}})}}` }} }} }}
  }};

  // Portfolio donut
  const pfCtx=document.getElementById('chart-pf');
  if(pfCtx&&_PF_VALUES.length){{
    if(_chartPf)_chartPf.destroy();
    _chartPf=new Chart(pfCtx,{{type:'doughnut',
      data:{{labels:_PF_LABELS,datasets:[{{data:_PF_VALUES,
        backgroundColor:_chartColors(_PF_VALUES.length),
        borderWidth:2,borderColor:'transparent'}}]}},
      options:{{...commonOpts,cutout:'60%'}}
    }});
  }}

  // Asset class donut
  const acCtx=document.getElementById('chart-ac');
  if(acCtx&&_AC_VALUES.length){{
    if(_chartAc)_chartAc.destroy();
    _chartAc=new Chart(acCtx,{{type:'doughnut',
      data:{{labels:_AC_LABELS,datasets:[{{data:_AC_VALUES,
        backgroundColor:_chartColors(_AC_VALUES.length),
        borderWidth:2,borderColor:'transparent'}}]}},
      options:{{...commonOpts,cutout:'60%'}}
    }});
  }}
}}

function _rebuildCharts(){{
  // Re-read text color after theme change then redraw
  setTimeout(_buildCharts, 50);
}}
</script>

</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/health")
def health():
    db_ok = os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0
    return {"status": "ok" if db_ok else "degraded", "db": DB_PATH, "db_ok": db_ok}


@app.get("/healthz")
def healthz():
    """Compatibility alias for environments expecting /healthz."""
    return health()


@app.get("/api/crew/schedule")
def schedule_status():
    """Show scheduler status: next fire times and last run results for all jobs."""
    jobs = []
    for job in _scheduler.get_jobs():
        next_run = job.next_run_time
        state = _scheduler_state.get(job.id, {})
        jobs.append({
            "id": job.id,
            "next_run": next_run.isoformat() if next_run else None,
            "next_run_mst": next_run.astimezone(_MST).strftime("%a %b %d %I:%M %p MST") if next_run else None,
            "last_run": state.get("last_run"),
            "last_outcome": state.get("last_outcome"),
            "last_duration_s": state.get("duration"),
            "trigger": state.get("trigger"),
            "strategies_generated": state.get("strategies_generated"),
        })

    # Last 5 runs from DB
    conn = _db()
    try:
        recent = conn.execute(
            "SELECT run_type, trigger, outcome, duration_seconds, error_log, created_at "
            "FROM crew_runs ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        recent_runs = [dict(r) for r in recent]
    finally:
        conn.close()

    return {
        "scheduler_running": _scheduler.running,
        "now_mst": datetime.now(_MST).strftime("%a %b %d %I:%M %p MST"),
        "jobs": jobs,
        "recent_db_runs": recent_runs,
    }


@app.get("/api/schedule")
def schedule_status_alias():
    """Compatibility alias for older callers."""
    return schedule_status()


@app.get("/api/matrix/neo/status")
def matrix_neo_status_alias():
    """Direct Matrix alias for Neo shared-state status."""
    return neo_status()


@app.post("/api/matrix/neo/mirror")
def matrix_neo_mirror_alias(req: NeoMirrorRequest):
    """Direct Matrix alias for Neo shared-state mirroring."""
    return neo_mirror(req)


# DECOMMISSIONED 2026-04-07 — archived to backups/port8000_archive
# CrewAI Unified Trader (port 8000) has been consolidated into main.py (port 8080).
# All data preserved in data/trader.db. Plist: com.trademinds.crew — UNLOADED.
#
# if __name__ == "__main__":
#     import uvicorn
#     print("=" * 50)
#     print("  USS TradeMinds — Unified Trader")
#     print("  Port 8000 | CrewAI + Portfolios")
#     print("=" * 50)
#     print()
#     print(f"  DB: {os.environ.get('TRADEMINDS_DB')}")
#     print(f"  Docs: http://localhost:8000/docs")
#     print()
#     uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
