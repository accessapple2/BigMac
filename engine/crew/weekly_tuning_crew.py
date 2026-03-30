"""Weekly Model Tuning Crew — scores fleet, promotes/demotes, tunes prompts.

Runs Sundays at 9:00 PM MST (before Picard at 10 PM, before strategy gen at 10:30 PM).
Uses direct Ollama/Gemini calls (Python 3.9 compatible).
"""
from __future__ import annotations
import json
import os
import re
import sqlite3
import requests
from datetime import date, datetime, timedelta
from rich.console import Console

console = Console()
DB = "data/trader.db"
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("CREWAI_MODEL", "qwen3:8b")


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _ollama(prompt, system="", model=None):
    payload = {"model": model or OLLAMA_MODEL, "prompt": prompt, "stream": False}
    if system:
        payload["system"] = system
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=120)
        if r.ok:
            return r.json().get("response", "").strip()
    except Exception as e:
        console.log(f"[red]Ollama error: {e}")
    return ""


def _gemini(prompt, system=""):
    """Call Gemini free tier (gemini-3.1-flash-lite, 400/day cap) with Ollama fallback."""
    from engine.gemini_free_tier import call_gemini
    return call_gemini(prompt, system)


def run_weekly_tuning():
    """Run the weekly model tuning crew. 3 agents: Scorer → Promoter → Prompt Tuner."""
    console.log("[bold cyan]Weekly Tuning Crew: Assembling...")

    conn = _conn()

    # Gather weekly data
    week_lessons = conn.execute("""
        SELECT player_id, grade, symbol, pnl, lesson
        FROM daily_lessons
        WHERE date >= date('now', '-7 days')
        ORDER BY player_id, date
    """).fetchall()

    week_trades = conn.execute("""
        SELECT t.player_id, p.display_name, COUNT(*) as trade_count,
               SUM(CASE WHEN t.action LIKE 'BUY%' THEN 1 ELSE 0 END) as buys,
               SUM(CASE WHEN t.action = 'SELL' THEN 1 ELSE 0 END) as sells
        FROM trades t
        JOIN ai_players p ON t.player_id = p.id
        WHERE t.executed_at >= date('now', '-7 days')
        GROUP BY t.player_id
    """).fetchall()

    active_models = conn.execute("""
        SELECT id, display_name, provider, model_id, is_active, is_paused
        FROM ai_players WHERE is_active = 1 OR is_paused = 1
    """).fetchall()

    prev_scores = conn.execute("""
        SELECT player_id, overall_score FROM model_scores
        WHERE period = 'weekly' AND date >= date('now', '-14 days')
        ORDER BY date DESC
    """).fetchall()

    conn.close()

    lessons_by_model = {}
    for l in week_lessons:
        pid = l["player_id"]
        if pid not in lessons_by_model:
            lessons_by_model[pid] = []
        lessons_by_model[pid].append(dict(l))

    prev_score_map = {}
    for s in prev_scores:
        if s["player_id"] not in prev_score_map:
            prev_score_map[s["player_id"]] = s["overall_score"]

    # ── Agent 1: Fleet Performance Scorer ──
    console.log("[cyan]  Agent 1: Fleet Performance Officer scoring...")
    trades_text = json.dumps([dict(t) for t in week_trades], indent=2, default=str)
    lessons_text = json.dumps(
        {k: v[:5] for k, v in lessons_by_model.items()},
        indent=2, default=str
    )

    scores_output = _ollama(
        f"Score each model's weekly performance 0-100.\n\n"
        f"WEEKLY TRADE COUNTS:\n{trades_text}\n\n"
        f"DAILY LESSONS (grades A-F):\n{lessons_text}\n\n"
        f"PREVIOUS SCORES: {json.dumps(prev_score_map)}\n\n"
        f"For each model compute: win_rate estimate, regime_alignment (0-100), "
        f"confidence_calibration (0-100), overall_score (0-100).\n"
        f"Output JSON: {{"
        f'"model_id":{{"overall_score":75,"win_rate":60,"regime_alignment":70,"confidence_calibration":65}}}}',
        system=(
            "You are a fleet performance officer. Score models based on trade grades, "
            "patterns, and improvement trends. Be fair but strict."
        )
    )

    # Parse and save scores
    scores_saved = 0
    score_map = {}
    try:
        match = re.search(r'\{.*\}', scores_output, re.DOTALL)
        if match:
            parsed = json.loads(match.group())
            for pid, data in parsed.items():
                if isinstance(data, dict) and "overall_score" in data:
                    score_map[pid] = data
                    conn2 = _conn()
                    conn2.execute("""
                        INSERT INTO model_scores
                        (player_id, period, date, win_rate, regime_alignment,
                         confidence_calibration, overall_score)
                        VALUES (?, 'weekly', ?, ?, ?, ?, ?)
                    """, (
                        pid, date.today().isoformat(),
                        data.get("win_rate", 0), data.get("regime_alignment", 0),
                        data.get("confidence_calibration", 0), data["overall_score"]
                    ))
                    conn2.commit()
                    conn2.close()
                    scores_saved += 1
    except (json.JSONDecodeError, Exception) as e:
        console.log(f"[yellow]  Score parsing failed: {e}")

    console.log(f"[green]  Scored {scores_saved} models")

    # ── Agent 2: Fleet Admiral — Promote/Demote ──
    console.log("[cyan]  Agent 2: Fleet Admiral deciding promotions/demotions...")
    scores_text = json.dumps(score_map, indent=2, default=str)

    promo_output = _gemini(
        f"Based on weekly scores, decide for each model: PROMOTED/STEADY/PROBATION/DEMOTED.\n\n"
        f"SCORES:\n{scores_text}\n\n"
        f"PREVIOUS SCORES: {json.dumps(prev_score_map)}\n\n"
        f"Rules: PROMOTED (>=75, improving), STEADY (40-74), PROBATION (20-39), "
        f"DEMOTED (<20 or 2 weeks probation).\n"
        f"For PROMOTED: increase capital 10-20%. For PROBATION: reduce capital 20%, tighten stops.\n"
        f"Output JSON array of adjustments: "
        f'[{{"player_id":"...","adjustment_type":"...","new_value":"...","reason":"..."}}]',
        system="You are a fleet admiral. Promote winners, put losers on probation. Be decisive."
    )

    adj_saved = 0
    try:
        match = re.search(r'\[.*\]', promo_output, re.DOTALL)
        if match:
            adj_list = json.loads(match.group())
            conn3 = _conn()
            for adj in adj_list[:20]:
                conn3.execute("""
                    INSERT INTO model_adjustments
                    (player_id, adjustment_type, old_value, new_value, reason, source, effective_date)
                    VALUES (?, ?, ?, ?, ?, 'weekly_crew', ?)
                """, (
                    adj["player_id"], adj["adjustment_type"],
                    adj.get("old_value"), adj["new_value"],
                    adj.get("reason", ""), date.today().isoformat()
                ))
                adj_saved += 1
            conn3.commit()
            conn3.close()
    except (json.JSONDecodeError, Exception) as e:
        console.log(f"[yellow]  Promotion parsing failed: {e}")

    console.log(f"[green]  {adj_saved} fleet adjustments saved")

    # ── Agent 3: Prompt Tuner ──
    console.log("[cyan]  Agent 3: Prompt Tuner writing behavioral adjustments...")

    prompt_output = _ollama(
        f"Based on scores and patterns, write prompt-level adjustments for underperforming models.\n\n"
        f"SCORES:\n{scores_text}\n\n"
        f"LESSONS:\n{lessons_text}\n\n"
        f"Write adjustments that inject warnings into model prompts. Examples:\n"
        f'- {{"player_id":"grok-4","adjustment_type":"confidence_modifier","new_value":"0.85","reason":"overconfident last week"}}\n'
        f'- {{"player_id":"energy-arnold","adjustment_type":"regime_filter","new_value":"BEAR_ONLY_SELL","reason":"bought in BEAR, lost money"}}\n'
        f"Only adjust models scoring below 60. Max 2 per model.\n"
        f"Output JSON array.",
        system="You are an AI personality engineer. Tune prompts to fix behavioral issues."
    )

    try:
        match = re.search(r'\[.*\]', prompt_output, re.DOTALL)
        if match:
            prompt_adj = json.loads(match.group())
            conn4 = _conn()
            for adj in prompt_adj[:10]:
                conn4.execute("""
                    INSERT INTO model_adjustments
                    (player_id, adjustment_type, old_value, new_value, reason, source, effective_date)
                    VALUES (?, ?, ?, ?, ?, 'weekly_prompt_tuner', ?)
                """, (
                    adj["player_id"], adj["adjustment_type"],
                    adj.get("old_value"), adj["new_value"],
                    adj.get("reason", ""), date.today().isoformat()
                ))
                adj_saved += 1
            conn4.commit()
            conn4.close()
    except (json.JSONDecodeError, Exception):
        pass

    console.log(f"[bold green]Weekly Tuning complete: {scores_saved} scored, {adj_saved} adjustments")

    return {
        "status": "complete",
        "models_scored": scores_saved,
        "adjustments_saved": adj_saved,
        "scores": score_map,
    }


if __name__ == "__main__":
    result = run_weekly_tuning()
    print(json.dumps(result, indent=2, default=str))
