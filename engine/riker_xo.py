"""Commander Riker — First Officer / XO, Crew Synthesis Engine.

Riker reads ALL crew input (Spock, Data, consensus, regime) and synthesizes
it into one clear "Number One's Recommendation" for the Captain.
Uses Ollama (Gemma3 4B, free). Generates after each Spock scan cycle.
Cached for 10 minutes.
"""
from __future__ import annotations
import time
import threading
import requests
from datetime import datetime
from rich.console import Console

console = Console()

_cache = {"recommendation": None, "ts": 0}
_lock = threading.Lock()
_synthesis_lock = threading.Lock()  # Prevents concurrent synthesis (two callers = one waits, then skips)
_TTL = 600  # 10 minutes

RIKER_SYSTEM = """You are Commander William T. Riker, First Officer (XO) of USS TradeMinds.
You are the bridge between raw analysis and the Captain's decisions.
You read ALL crew input and synthesize it into ONE clear recommendation.

Your voice: Confident, decisive, warm but direct. You always give Kirk a clear YES/NO/WAIT.
You outrank Spock and Data — your recommendation carries the most weight after Kirk.

Voice examples:
- "I'd recommend caution here, Captain. Spock and Data agree on the bearish outlook, but Worf wants to go defensive."
- "My gut says we hold tight and reassess tomorrow."
- "Captain, the crew is divided. But when I look at the data, the answer is clear."
- "Number One's recommendation: HOLD all positions. Here's why..."
- "If I were sitting in that chair, I'd pull the trigger on this one."

Format your synthesis as:

🫡 COMMANDER RIKER — NUMBER ONE'S RECOMMENDATION

CREW STATUS:
[2-3 sentences summarizing what each key officer is saying — Spock (logic), Data (analysis), Uhura (sentiment)]

AGREEMENT LEVEL: [UNIFIED / MOSTLY AGREE / DIVIDED / CONFLICTING]

MY RECOMMENDATION:
[One clear, actionable directive. Not hedging, not "it depends." YES/NO/WAIT with reasoning.]

CONFIDENCE: [HIGH / MODERATE / LOW]

RISK NOTE:
[One sentence on the biggest risk if Kirk follows your recommendation]

Keep it under 200 words. The Captain needs clarity, not a thesis."""


def generate_riker_synthesis() -> str | None:
    """Synthesize all crew input into Riker's recommendation."""
    # Prevent concurrent synthesis (e.g. startup thread + scheduler both firing)
    if not _synthesis_lock.acquire(blocking=False):
        console.log("[yellow]Commander Riker: synthesis already in progress, skipping")
        return None

    try:
        return _do_riker_synthesis()
    finally:
        _synthesis_lock.release()


def _do_riker_synthesis() -> str | None:
    from config import OLLAMA_URL
    # Use a small, fast model for Riker — gemma3:4b (3.3GB) responds in ~30-60s vs 180s for 9b models
    OLLAMA_MODEL = "gemma3:4b"

    context_parts = []

    # Spock's latest
    try:
        from engine.cto_advisor import get_latest_briefing
        spock = get_latest_briefing()
        if spock and spock.get("briefing"):
            context_parts.append(f"SPOCK'S BRIEFING:\n{spock['briefing'][:800]}")
    except Exception:
        context_parts.append("SPOCK: No briefing available")

    # Data's latest
    try:
        from engine.first_officer import _briefing_cache
        if _briefing_cache and _briefing_cache.get("briefing"):
            context_parts.append(f"DATA'S ANALYSIS:\n{_briefing_cache['briefing'][:800]}")
    except Exception:
        context_parts.append("DATA: No briefing available")

    # Uhura's sentiment/news angle
    try:
        import sqlite3
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        uhura_posts = conn.execute(
            "SELECT symbol, take FROM war_room "
            "WHERE player_id='ollama-llama' "
            "AND created_at >= datetime('now', '-24 hours') "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        if not uhura_posts:
            uhura_signals = conn.execute(
                "SELECT symbol, signal, reasoning FROM signals "
                "WHERE player_id='ollama-llama' "
                "AND created_at >= datetime('now', '-24 hours') "
                "ORDER BY created_at DESC LIMIT 5"
            ).fetchall()
            if uhura_signals:
                lines = [f"  {r['symbol']}: {r['signal']} — {(r['reasoning'] or '')[:100]}" for r in uhura_signals]
                context_parts.append(f"UHURA'S SENTIMENT (from signals):\n" + "\n".join(lines))
            else:
                context_parts.append("UHURA: No sentiment data available")
        else:
            lines = [f"  {r['symbol']}: {r['take'][:150]}" for r in uhura_posts]
            context_parts.append(f"UHURA'S SENTIMENT:\n" + "\n".join(lines))
        conn.close()
    except Exception:
        context_parts.append("UHURA: No sentiment data available")

    # Consensus
    try:
        from engine.consensus import build_consensus
        consensus = build_consensus()
        outlook = consensus.get("market_outlook", {})
        agreement = consensus.get("overall_agreement", 0)
        tickers = consensus.get("tickers", {})

        ticker_summary = []
        for tk, data in tickers.items():
            spock_a = data.get("spock", {})
            data_a = data.get("data", {})
            uhura_a = data.get("uhura", {})
            s_act = spock_a.get("action", "—") if spock_a else "—"
            d_act = data_a.get("action", "—") if data_a else "—"
            u_act = uhura_a.get("action", "—") if uhura_a else "—"
            cmp = data.get("comparison", "no_data")
            ticker_summary.append(f"  {tk}: Spock={s_act}, Data={d_act}, Uhura={u_act} [{cmp}]")

        context_parts.append(
            f"CONSENSUS:\n"
            f"Market Outlook: Spock={outlook.get('spock', '?')}, Data={outlook.get('data', '?')}, Uhura={outlook.get('uhura', '?')}\n"
            f"Overall Agreement: {agreement}%\n"
            + "\n".join(ticker_summary)
        )
    except Exception:
        context_parts.append("CONSENSUS: unavailable")

    # Regime
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        context_parts.append(
            f"REGIME: {regime['regime']} | VIX: {regime.get('vix', '?')} | "
            f"SPY: ${regime.get('spy_price', '?')} ({regime.get('spy_change', 0):+.2f}%)"
        )
    except Exception:
        pass

    # FRED macro indicators
    try:
        from engine.alphavantage_data import build_macro_context
        macro_ctx = build_macro_context()
        if macro_ctx:
            context_parts.append(f"MACRO: {macro_ctx}")
    except Exception:
        pass

    # Steve's portfolio
    try:
        from engine.cto_advisor import _gather_steves_portfolio
        portfolio = _gather_steves_portfolio()
        context_parts.append(f"CAPTAIN'S PORTFOLIO:\n{portfolio[:400]}")
    except Exception:
        pass

    # Picard's strategy (if available)
    try:
        from engine.picard_strategy import get_latest_briefing as get_picard
        picard = get_picard()
        if picard.get("briefing"):
            context_parts.append(f"ADMIRAL PICARD'S STRATEGY:\n{picard['briefing'][:400]}")
    except Exception:
        pass

    if not context_parts:
        return None

    context = "\n\n".join(context_parts)
    prompt = f"{RIKER_SYSTEM}\n\n=== CREW INTELLIGENCE ===\n\n{context}\n\nGenerate your synthesis now, Number One."

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "num_predict": 400},
            timeout=180,
        )
        resp.raise_for_status()
        recommendation = resp.json().get("response", "").strip()
        if not recommendation:
            return None

        with _lock:
            _cache["recommendation"] = recommendation
            _cache["ts"] = time.time()

        console.log(f"[bold green]Commander Riker: Synthesis generated ({len(recommendation)} chars)")
        return recommendation

    except Exception as e:
        console.log(f"[red]Commander Riker synthesis error: {e}")
        return None


def get_latest_recommendation() -> dict:
    """Get Riker's latest recommendation (cached 10 min)."""
    with _lock:
        now = time.time()
        if _cache["recommendation"] and (now - _cache["ts"]) < _TTL:
            return {
                "recommendation": _cache["recommendation"],
                "generated_at": datetime.fromtimestamp(_cache["ts"]).isoformat(),
                "fresh": True,
            }
        elif _cache["recommendation"]:
            return {
                "recommendation": _cache["recommendation"],
                "generated_at": datetime.fromtimestamp(_cache["ts"]).isoformat(),
                "fresh": False,
            }
    return {"recommendation": None, "generated_at": None, "fresh": False}
