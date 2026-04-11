"""
Ready Room API Routes

GET  /api/ready-room/briefing   — latest full briefing
GET  /api/ready-room/levels     — key levels only (for agents)
POST /api/ready-room/run        — trigger a new briefing
GET  /api/ready-room/history    — past 7 briefings (no gameplan text, lightweight)
GET  /api/ready-room/vix        — VIX term structure (contango/backwardation)
GET  /api/ready-room/momentum   — intraday momentum score (-100..+100)
GET  /api/ready-room/skew       — 25-delta IV skew (put fear premium)
GET  /api/ready-room/gamma-map  — multi-timeframe GEX map
GET  /api/ready-room/oi-changes — OI growth flags (>20% since morning)
"""
from __future__ import annotations

import threading

from fastapi import APIRouter, BackgroundTasks

router = APIRouter()

# Guard: only one briefing generation at a time
_generating = False
_gen_lock = threading.Lock()


@router.get("/briefing")
def ready_room_briefing():
    """Return the latest Ready Room session briefing (cached up to 10 min)."""
    try:
        from engine.ready_room import get_latest_briefing
        data = get_latest_briefing()
        if not data:
            return {"briefing": None, "message": "No briefing available yet. POST /api/ready-room/run to generate."}
        return data
    except Exception as e:
        return {"error": str(e), "briefing": None}


@router.get("/levels")
def ready_room_levels():
    """
    Return structured key options levels for SPY.
    Agents use this to incorporate options structure into their analysis.
    """
    try:
        from engine.ready_room import get_key_levels
        return get_key_levels()
    except Exception as e:
        return {"error": str(e)}


@router.post("/run")
def ready_room_run(background_tasks: BackgroundTasks, force: bool = False):
    """
    Trigger a new Ready Room briefing.
    Runs in the background; returns immediately with status.
    Only one generation runs at a time.
    """
    global _generating
    with _gen_lock:
        if _generating:
            return {"ok": False, "message": "Briefing already generating — please wait"}
        _generating = True

    def _do_generate():
        global _generating
        try:
            from engine.ready_room import generate_ready_room_briefing
            generate_ready_room_briefing(force=True)
        except Exception as e:
            from rich.console import Console
            Console().log(f"[red]ReadyRoom /run error: {e}")
        finally:
            with _gen_lock:
                _generating = False

    background_tasks.add_task(_do_generate)
    return {"ok": True, "message": "Ready Room briefing generating in background…"}


@router.get("/history")
def ready_room_history(limit: int = 7):
    """Return the last N briefings (lightweight — no full gameplan text)."""
    try:
        from engine.ready_room import get_briefing_history
        history = get_briefing_history(limit=min(limit, 20))
        # Strip heavy gameplan text for the history list
        for h in history:
            h.pop("gameplan", None)
            h.pop("signals_json", None)
        return {"history": history, "count": len(history)}
    except Exception as e:
        return {"error": str(e), "history": []}


# ---------------------------------------------------------------------------
# Phase 2 sub-module endpoints
# ---------------------------------------------------------------------------

@router.get("/vix")
def ready_room_vix(force: bool = False):
    """VIX term structure: contango / backwardation state + regime."""
    try:
        from engine.vix_monitor import get_vix_term_structure
        return get_vix_term_structure(force=force)
    except Exception as e:
        return {"error": str(e)}


@router.get("/momentum")
def ready_room_momentum(symbol: str = "SPY", force: bool = False):
    """Intraday momentum score (-100..+100) from 5-min volume delta."""
    try:
        from engine.momentum_tracker import get_intraday_momentum
        return get_intraday_momentum(symbol=symbol, force=force)
    except Exception as e:
        return {"error": str(e)}


@router.get("/skew")
def ready_room_skew(symbol: str = "SPY", force: bool = False):
    """25-delta IV skew: positive = put fear premium, negative = call greed."""
    try:
        from engine.iv_skew import get_iv_skew
        return get_iv_skew(symbol=symbol, force=force)
    except Exception as e:
        return {"error": str(e)}


@router.get("/gamma-map")
def ready_room_gamma_map(symbol: str = "SPY", force: bool = False):
    """Multi-timeframe GEX map: 0DTE / weekly / monthly + confluence zones."""
    try:
        from engine.gamma_map import get_gamma_map
        return get_gamma_map(symbol=symbol, force=force)
    except Exception as e:
        return {"error": str(e)}


@router.get("/oi-changes")
def ready_room_oi_changes(symbol: str = "SPY", force: bool = False):
    """OI change flags: strikes with >20% OI growth since morning snapshot."""
    try:
        from engine.oi_tracker import get_oi_changes
        return get_oi_changes(symbol=symbol, force=force)
    except Exception as e:
        return {"error": str(e)}


@router.post("/oi-snapshot")
def ready_room_oi_snapshot(symbol: str = "SPY"):
    """Take the morning OI baseline snapshot (call at market open)."""
    try:
        from engine.oi_tracker import take_morning_snapshot
        return take_morning_snapshot(symbol=symbol)
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Phase 2b — Red Alert: condition, alerts, intraday snapshots
# ---------------------------------------------------------------------------

@router.get("/condition")
def ready_room_condition():
    """
    Current traffic-light condition: GREEN (All Clear) / YELLOW (Yellow Alert) / RED (Red Alert).
    Weighted from session_type(30%) + momentum(25%) + VIX(20%) + volume(15%) + skew(10%).
    """
    try:
        from engine.red_alert import get_current_condition
        return get_current_condition()
    except Exception as e:
        return {"error": str(e), "condition": "UNKNOWN"}


@router.get("/alerts")
def ready_room_alerts(limit: int = 50):
    """Today's Red Alert log — session changes, wall breaches, GEX flips, etc."""
    try:
        from engine.red_alert import get_today_alerts
        alerts = get_today_alerts(limit=min(limit, 100))
        return {"alerts": alerts, "count": len(alerts)}
    except Exception as e:
        return {"error": str(e), "alerts": []}


@router.get("/intraday")
def ready_room_intraday():
    """Today's intraday snapshot history — trend scores, VIX, condition over time."""
    try:
        from engine.red_alert import get_today_snapshots
        snaps = get_today_snapshots()
        return {"snapshots": snaps, "count": len(snaps)}
    except Exception as e:
        return {"error": str(e), "snapshots": []}


@router.post("/poll")
def ready_room_poll():
    """
    Manually trigger one Red Alert poll cycle (for testing / forced refresh).
    Returns the assembled snapshot with condition score.
    """
    try:
        from engine.red_alert import run_poll_cycle
        result = run_poll_cycle()
        return {"ok": True, "snapshot": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Phase 3 endpoints — Advisor, Scorecard, Pattern Matcher
# ---------------------------------------------------------------------------

@router.get("/advisor")
def ready_room_advisor(symbol: str = "SPY", action: str = "BUY"):
    """
    Counselor Troi's pre-trade consultation.
    Returns GO / CAUTION / STAND_DOWN with position size multiplier and reason.
    """
    try:
        from engine.ready_room_advisor import should_i_trade
        return should_i_trade(symbol=symbol, proposed_action=action)
    except Exception as e:
        return {"error": str(e), "signal": "GO", "position_size_multiplier": 0.8}


@router.get("/trade-context")
def ready_room_trade_context():
    """Formatted market context string for injecting into agent LLM prompts."""
    try:
        from engine.ready_room_advisor import get_trade_context
        return {"context": get_trade_context()}
    except Exception as e:
        return {"error": str(e), "context": ""}


@router.get("/scorecard")
def ready_room_scorecard():
    """Latest EOD forecast scorecard + rolling accuracy stats."""
    try:
        from engine.eod_scorecard import get_rolling_accuracy
        import sqlite3, os
        db = os.path.expanduser("~/autonomous-trader/data/trader.db")
        conn = sqlite3.connect(db, timeout=10)
        conn.row_factory = sqlite3.Row
        latest = conn.execute(
            "SELECT * FROM forecast_scorecards ORDER BY trade_date DESC LIMIT 1"
        ).fetchone()
        conn.close()
        rolling = get_rolling_accuracy(14)
        return {
            "latest":  dict(latest) if latest else None,
            "rolling": rolling,
        }
    except Exception as e:
        return {"error": str(e)}


@router.post("/scorecard/run")
def ready_room_scorecard_run(force: bool = False):
    """Manually trigger today's EOD scorecard grading."""
    try:
        from engine.eod_scorecard import run_eod_scorecard
        result = run_eod_scorecard(force=force)
        return result
    except Exception as e:
        return {"error": str(e)}


@router.get("/similar-days")
def ready_room_similar_days(limit: int = 3):
    """
    Mr. Spock's Historical Analysis — top-3 similar past sessions
    with outcome summaries. Returns 'not enough data' until 10+ days stored.
    """
    try:
        from engine.pattern_matcher import get_similar_days
        return get_similar_days(limit=min(limit, 5))
    except Exception as e:
        return {"error": str(e), "similar_days": []}


@router.post("/fingerprint")
def ready_room_fingerprint():
    """Capture today's morning session fingerprint for pattern matching."""
    try:
        from engine.pattern_matcher import capture_fingerprint
        return capture_fingerprint()
    except Exception as e:
        return {"error": str(e)}


@router.get("/health")
def ready_room_health_check():
    """Dr. Crusher's Extended Scan — Ready Room + Red Alert health status."""
    try:
        from engine.ready_room_health import check_ready_room_health
        return check_ready_room_health()
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Phase 4 endpoints — Event Shield, Breadth, Sectors, Correlations,
#                     News Pulse, Holodeck, Adaptive Weights
# ---------------------------------------------------------------------------

@router.get("/events")
def ready_room_events():
    """Deflector Shield — today's scheduled market-moving events with impact levels."""
    try:
        from engine.event_shield import get_event_shield_status
        return get_event_shield_status()
    except Exception as e:
        return {"error": str(e), "all_clear": True, "events_today": []}


@router.get("/breadth")
def ready_room_breadth():
    """Chekov's Broad Scan — market breadth: A/D ratio, sector participation, divergences."""
    try:
        from engine.breadth_scanner import get_breadth_snapshot
        return get_breadth_snapshot()
    except Exception as e:
        return {"error": str(e)}


@router.get("/sectors")
def ready_room_sectors():
    """Sector Grid Display — 11 GICS sector ETF performance ranked, rotation type."""
    try:
        from engine.sector_heatmap import get_sector_heatmap
        return get_sector_heatmap()
    except Exception as e:
        return {"error": str(e)}


@router.get("/correlations")
def ready_room_correlations():
    """Stellar Cartography — TLT/GLD/UUP/HYG intermarket alignment vs SPY."""
    try:
        from engine.correlation_monitor import get_correlations
        return get_correlations()
    except Exception as e:
        return {"error": str(e)}


@router.get("/news-pulse")
def ready_room_news_pulse(force: bool = False):
    """Lt. Uhura's News Intercept — Finnhub sentiment scan, mood score, convergence."""
    try:
        from engine.news_pulse import fetch_news_pulse
        return fetch_news_pulse(force=force)
    except Exception as e:
        return {"error": str(e)}


@router.get("/holodeck")
def ready_room_holodeck():
    """Holodeck Simulation — latest backtest comparing buy-and-hold vs Ready Room filtered."""
    try:
        from engine.holodeck_readyroom import get_latest_holodeck_results
        return get_latest_holodeck_results()
    except Exception as e:
        return {"error": str(e)}


@router.post("/holodeck/run")
def ready_room_holodeck_run():
    """Trigger a fresh Holodeck backtest simulation."""
    try:
        from engine.holodeck_readyroom import run_holodeck_backtest
        return run_holodeck_backtest(force=True)
    except Exception as e:
        return {"error": str(e)}


@router.get("/weights")
def ready_room_weights():
    """Computer, Recalibrate Sensors — current adaptive signal weights and accuracy scores."""
    try:
        from engine.adaptive_tuner import get_weights_status
        return get_weights_status()
    except Exception as e:
        return {"error": str(e)}


@router.get("/advisory")
def ready_room_advisory(force: bool = False):
    """Counselor Troi's Dynamic Market Advisory — interprets ALL signals together, plain English."""
    try:
        from engine.dynamic_advisor import generate_advisory
        return generate_advisory(force=force)
    except Exception as e:
        return {"error": str(e)}


# ── Edge-TTS voice generation ────────────────────────────────────────────────

import asyncio
import os
import time as _time
from fastapi import Request

_AUDIO_OUT   = os.path.join(os.path.dirname(__file__), "static", "briefing_audio.mp3")
_AUDIO_URL   = "/static/briefing_audio.mp3"
_AUDIO_VOICE = "en-US-AndrewNeural"
_last_speak_ts: float = 0.0
_speak_lock = threading.Lock()


async def _edge_tts_generate(text: str, path: str) -> None:
    import edge_tts
    communicate = edge_tts.Communicate(text, _AUDIO_VOICE)
    await communicate.save(path)


@router.post("/speak")
async def ready_room_speak(request: Request):
    """
    Generate edge-tts audio for the Ship's Computer briefing.
    Body: { "text": "..." }
    Returns: { "audio_url": "/static/briefing_audio.mp3", "voice": "...", "generated_at": ... }
    """
    global _last_speak_ts
    try:
        body = await request.json()
        text = str(body.get("text", "")).strip()
        if not text:
            return {"error": "text field is required"}
        if len(text) > 8000:
            text = text[:8000]

        with _speak_lock:
            await _edge_tts_generate(text, _AUDIO_OUT)
            _last_speak_ts = _time.time()

        return {
            "audio_url":    _AUDIO_URL,
            "voice":        _AUDIO_VOICE,
            "generated_at": _last_speak_ts,
            "size_bytes":   os.path.getsize(_AUDIO_OUT) if os.path.exists(_AUDIO_OUT) else 0,
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/speak/latest")
def ready_room_speak_latest():
    """Return URL of the most recently generated briefing audio and its age in seconds."""
    exists = os.path.exists(_AUDIO_OUT)
    return {
        "audio_url":    _AUDIO_URL if exists else None,
        "voice":        _AUDIO_VOICE,
        "generated_at": _last_speak_ts if _last_speak_ts else None,
        "age_seconds":  round(_time.time() - _last_speak_ts, 1) if _last_speak_ts else None,
        "exists":       exists,
    }
