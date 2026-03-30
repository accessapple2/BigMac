"""Daily Post-Market Review Crew — grades trades, finds patterns, writes adjustments.

Runs Mon-Fri at 1:15 PM MST (4:15 PM ET), 15 min after market close.
Uses direct Ollama/Gemini calls (Python 3.9 compatible, no crewai dependency).
All results saved to daily_lessons, model_scores, model_adjustments tables.
"""
from __future__ import annotations
import json
import os
import re
import sqlite3
import requests
from datetime import date, datetime
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


def _get_today_trades():
    conn = _conn()
    today = date.today().isoformat()
    trades = conn.execute("""
        SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price,
               t.confidence, t.reasoning, t.executed_at, t.asset_type
        FROM trades t
        LEFT JOIN ai_players p ON t.player_id = p.id
        WHERE date(t.executed_at) = ?
        ORDER BY t.executed_at
    """, (today,)).fetchall()
    conn.close()
    return [dict(t) for t in trades]


def _get_model_history(player_id, days=30):
    conn = _conn()
    trades = conn.execute("""
        SELECT symbol, action, qty, price, reasoning, confidence, executed_at
        FROM trades
        WHERE player_id = ? AND executed_at >= date('now', ?)
        ORDER BY executed_at DESC
    """, (player_id, f"-{days} days")).fetchall()
    conn.close()
    return [dict(t) for t in trades]


def _get_regime():
    try:
        r = requests.get("http://127.0.0.1:8080/api/regime", timeout=5)
        return r.json() if r.ok else {}
    except Exception:
        return {}


def _save_lesson(data):
    conn = _conn()
    conn.execute("""
        INSERT INTO daily_lessons
        (date, player_id, symbol, action, entry_price, current_price,
         pnl, grade, lesson, recommendation, regime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("date", date.today().isoformat()),
        data["player_id"], data.get("symbol"), data.get("action"),
        data.get("entry_price"), data.get("current_price"),
        data.get("pnl"), data.get("grade", "C"),
        data.get("lesson", ""), data.get("recommendation", ""),
        data.get("regime", "")
    ))
    conn.commit()
    conn.close()


def _save_score(data):
    conn = _conn()
    conn.execute("""
        INSERT INTO model_scores
        (player_id, period, date, win_rate, avg_pnl, sharpe, max_drawdown,
         regime_alignment, thesis_accuracy, confidence_calibration, overall_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["player_id"], data.get("period", "daily"),
        data.get("date", date.today().isoformat()),
        data.get("win_rate", 0), data.get("avg_pnl", 0),
        data.get("sharpe", 0), data.get("max_drawdown", 0),
        data.get("regime_alignment", 0), data.get("thesis_accuracy", 0),
        data.get("confidence_calibration", 0), data.get("overall_score", 50)
    ))
    conn.commit()
    conn.close()


def _save_adjustment(data):
    conn = _conn()
    conn.execute("""
        INSERT INTO model_adjustments
        (player_id, adjustment_type, old_value, new_value,
         reason, source, effective_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data["player_id"], data["adjustment_type"],
        data.get("old_value"), data["new_value"],
        data.get("reason", ""), data.get("source", "daily_crew"),
        data.get("effective_date", date.today().isoformat())
    ))
    conn.commit()
    conn.close()


def run_daily_review():
    """Run the full daily post-market review. 3 agents sequentially."""
    console.log("[bold cyan]Daily Review Crew: Assembling...")

    trades = _get_today_trades()
    regime = _get_regime()
    regime_label = regime.get("regime", "UNKNOWN")

    if not trades:
        console.log("[dim]Daily Review: No trades today, skipping review")
        return {"status": "skipped", "reason": "no trades today"}

    console.log(f"[cyan]Daily Review: {len(trades)} trades to analyze, regime={regime_label}")

    # ── Agent 1: Trade Analyst — grade each trade ──
    console.log("[cyan]  Agent 1: Trade Performance Analyst grading...")
    trades_text = json.dumps(trades[:20], indent=2, default=str)

    grades_output = _ollama(
        f"Grade each of these trades A through F. Current regime: {regime_label}.\n\n"
        f"TRADES:\n{trades_text}\n\n"
        f"For each trade output JSON: "
        f'[{{"player_id":"...","symbol":"...","action":"...","grade":"A/B/C/D/F","lesson":"one sentence","pnl":0}}]\n'
        f"Only output the JSON array, no other text.",
        system=(
            "You are a trade analyst. Grade A=thesis correct+profitable, B=partially correct, "
            "C=mediocre/no edge, D=thesis wrong but manageable, F=thesis contradicted or regime violation. "
            "Ghost promotion overriding a model's own thesis is always F."
        )
    )

    # Parse grades
    graded_trades = []
    try:
        match = re.search(r'\[.*\]', grades_output, re.DOTALL)
        if match:
            graded_trades = json.loads(match.group())
    except (json.JSONDecodeError, Exception) as e:
        console.log(f"[yellow]  Grade parsing failed: {e}, using defaults")

    # Save lessons
    for gt in graded_trades:
        gt["date"] = date.today().isoformat()
        gt["regime"] = regime_label
        try:
            _save_lesson(gt)
        except Exception as e:
            console.log(f"[red]  Save lesson error: {e}")

    console.log(f"[green]  Graded {len(graded_trades)} trades")

    # ── Agent 2: Pattern Detective — find recurring issues ──
    console.log("[cyan]  Agent 2: Pattern Detective analyzing 30-day history...")

    # Get unique players who traded today
    player_ids = list(set(t["player_id"] for t in trades))
    histories = {}
    for pid in player_ids[:5]:  # Cap at 5 to avoid timeout
        histories[pid] = _get_model_history(pid, 30)

    # Get reference data for cross-arena pattern comparison
    ref_context = ""
    try:
        from engine.reference_data import get_reference_for_learning
        for pid in player_ids[:5]:
            ref = get_reference_for_learning(pid)
            if ref:
                ref_context += f"\n{pid}: {ref}"
    except Exception:
        pass

    histories_text = json.dumps(
        {pid: h[:10] for pid, h in histories.items()},
        indent=2, default=str
    )
    grades_text = json.dumps(graded_trades[:10], indent=2, default=str)

    ref_section = ""
    if ref_context:
        ref_section = f"\n\nREFERENCE DATA (external arena — same LLMs, different platform):\n{ref_context}\nCompare: are our mistakes shared (systemic LLM bias) or unique (our prompting issue)?\n"

    patterns_output = _ollama(
        f"Find recurring patterns in these models' trading history.\n\n"
        f"TODAY'S GRADES:\n{grades_text}\n\n"
        f"30-DAY HISTORIES (last 10 trades each):\n{histories_text}\n"
        f"{ref_section}\n"
        f"For each model, identify: repeat mistakes, confidence inflation, "
        f"regime blindness, ticker fixation, ghost promotion abuse.\n"
        f"If reference data is available, note which mistakes are systemic (both platforms) vs unique (ours only).\n"
        f"Output JSON: {{"
        f'"model_id":{{"patterns":["..."],"severity":"low/medium/high","details":"..."}}}}',
        system=(
            "You are a behavioral pattern analyst. Find recurring mistakes and biases. "
            "Be specific — cite ticker names, win rates, confidence levels."
        )
    )

    console.log(f"[green]  Patterns analyzed ({len(patterns_output)} chars)")

    # ── Agent 3: Adjustment Writer — write specific adjustments ──
    console.log("[cyan]  Agent 3: Calibration Engineer writing adjustments...")

    adjustments_output = _gemini(
        f"Based on these trade grades and patterns, write specific model adjustments.\n\n"
        f"GRADES:\n{grades_text}\n\n"
        f"PATTERNS:\n{patterns_output}\n\n"
        f"RULES: Only adjust models with grade C or below. Max 3 adjustments per model. "
        f"Each adjustment changes value by max 20%. Types: confidence_modifier (0.8-1.2), "
        f"regime_filter (BEAR_ONLY_SELL/BULL_ONLY_BUY), position_size (decimal pct), "
        f"stop_loss (decimal pct), cooldown (Xmin), ghost_promotion_override (disabled).\n\n"
        f"Output JSON array: "
        f'[{{"player_id":"...","adjustment_type":"...","new_value":"...","reason":"..."}}]',
        system="You are a model calibration engineer. Be precise and conservative."
    )

    # Parse and save adjustments
    saved_adj = 0
    try:
        match = re.search(r'\[.*\]', adjustments_output, re.DOTALL)
        if match:
            adj_list = json.loads(match.group())
            for adj in adj_list[:15]:  # Cap total adjustments
                adj["source"] = "daily_crew"
                adj["effective_date"] = date.today().isoformat()
                try:
                    _save_adjustment(adj)
                    saved_adj += 1
                except Exception as e:
                    console.log(f"[red]  Save adjustment error: {e}")
    except (json.JSONDecodeError, Exception) as e:
        console.log(f"[yellow]  Adjustment parsing failed: {e}")

    # Save daily scores for each model that traded
    for pid in player_ids:
        pid_grades = [g for g in graded_trades if g.get("player_id") == pid]
        if not pid_grades:
            continue
        grade_scores = {"A": 95, "B": 75, "C": 50, "D": 25, "F": 5}
        avg_score = sum(grade_scores.get(g.get("grade", "C"), 50) for g in pid_grades) / len(pid_grades)
        _save_score({
            "player_id": pid,
            "period": "daily",
            "date": date.today().isoformat(),
            "overall_score": round(avg_score, 1),
        })

    console.log(f"[bold green]Daily Review complete: {len(graded_trades)} grades, {saved_adj} adjustments")

    return {
        "status": "complete",
        "trades_graded": len(graded_trades),
        "adjustments_saved": saved_adj,
        "players_reviewed": player_ids,
    }


if __name__ == "__main__":
    result = run_daily_review()
    print(json.dumps(result, indent=2, default=str))
