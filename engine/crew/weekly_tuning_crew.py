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
# HM-FLEET-REBASELINE-2026-07-04: GATE 0 data-integrity cutoff (docs/XO_BACKLOG.md).
# Hard floor on every scorecard query below -- currently a no-op (trailing 7/14-day
# lookbacks are already well past this date) but guards against a future lookback
# widening silently pulling in provisional pre-cutoff data.
CLEAN_CUTOFF = "2026-05-14"
from config import OLLIE_URL as _OLLIE_URL
OLLAMA_URL = os.getenv("ADVISORY_OLLAMA_URL",
             os.getenv("OLLAMA_BASE_URL", _OLLIE_URL))
OLLAMA_MODEL = os.getenv("CREWAI_MODEL", "qwen3:8b")  # 2026-04-20: qwen3:8b → qwen3:8b on Ollie GPU


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


def _weekly_spam_rates(player_ids: list) -> dict:
    """spam_rate_pct = reentry_blocked / signals_tested * 100, raw mode, trailing 7d.
    Same methodology as fleet_realism_sweep.py (HM-FLEET-REALISM-SWEEP), scoped to the
    current week rather than full history. HM-FLEET-REBASELINE-2026-07-04."""
    import engine.backtester as bt
    from engine.backtester import backtest_player

    bt.ENFORCE_REENTRY = True
    rates = {}
    for pid in player_ids:
        try:
            bt._vix_cache = {}
            raw = backtest_player(pid, days=7, apply_guardrails=False)
            tested = raw.get("signals_tested") or 0
            blocked = (raw.get("stats") or {}).get("reentry_blocked") or 0
            rates[pid] = round(blocked / tested * 100.0, 1) if tested else None
        except Exception as e:
            console.log(f"[yellow]  spam_rate calc failed for {pid}: {type(e).__name__}: {e!r}")
            rates[pid] = None
    return rates


def _run_auditions(conn) -> dict:
    """HM-AUDITION-SCORING-2026-07-05: score every non-executing candidate
    against config.AUDITION_CRITERIA on clean-window data, emit a
    pass/fail/insufficient_data proposal row per candidate into
    model_adjustments. Reuses fleet_realism_sweep_clean_window.py's exact
    methodology (backtest_player(start_date=CLEAN_CUTOFF, ...), same
    spam_rate_pct/friction_to_pnl formulas) so a candidate's numbers are
    directly comparable to the standalone sweep report.

    "Accumulate across weeks" is automatic, not a separate running total:
    backtest_player(start_date=CLEAN_CUTOFF) always replays from the clean
    floor through "now", so trade counts and honest-guarded-return grow on
    their own each week as more real signal history accrues.

    Scope limit (reported honestly, not papered over): an audition only
    gains NEW data while the candidate is still being scanned (halt_mode=
    'active' via a dedicated shadow/sim loop, or during its own exit_only
    wind-down window before halt_gate stops it emitting new signals). A
    halt_mode='full' candidate's clean-window numbers are frozen at
    whatever it produced before being cut -- verdict stays whatever it was.
    """
    from config import AUDITION_CRITERIA
    from engine.trades_filter import TRACKING_PLAYERS
    import engine.backtester as bt
    from engine.backtester import backtest_player

    bt.ENFORCE_STALENESS = True
    bt.ENFORCE_REENTRY = True
    bt.ENFORCE_COST_MODEL = True

    cutoff = AUDITION_CRITERIA["clean_window_start"]
    track_sql = ",".join("?" for _ in TRACKING_PLAYERS)
    # Candidate pool = non-executing agents that could plausibly earn a seat back:
    # excludes humans/manual desks (is_human), pure audit-trail bakeoff clones,
    # the broker mirror, the structural exit-only stop guardian, and tracking-
    # route players (dalio-metals/enterprise-computer/schwab -- never compete
    # for an execution seat, see setup_db.py HM-ROSTER-CAP). Currently-executing
    # (halt_mode='active') agents aren't candidates -- they hold a seat already.
    candidates = conn.execute(f"""
        SELECT id, COALESCE(halt_mode,'active') AS halt_mode, crew_role
        FROM ai_players
        WHERE COALESCE(is_human,0) = 0
          AND COALESCE(crew_role,'active') NOT IN ('bakeoff','mirror','guardian')
          AND COALESCE(halt_mode,'active') != 'active'
          AND id NOT IN ({track_sql})
        ORDER BY id
    """, TRACKING_PLAYERS).fetchall()

    results = []
    for row in candidates:
        pid = row["id"]
        n, _oldest = conn.execute("""
            SELECT COUNT(*), MIN(created_at) FROM signals
             WHERE player_id = ? AND signal IN ('BUY', 'BUY_CALL', 'BUY_PUT')
               AND created_at >= ?
        """, (pid, cutoff)).fetchone()

        verdict = "insufficient_data"
        detail = {"clean_signals_in_db": n}
        if not n:
            # HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT fix (2026-07-05): signals=0
            # doesn't mean "no data" for candidates that route through
            # options_trades or a non-standard pipeline (confirmed live:
            # neo-matrix, cto-grok42, options-sosnoff). Check real trade
            # history before giving up. HM-EDGE-PROVENANCE (2026-07-05):
            # this now requires broker-execution evidence, not just any
            # clean trade -- an internal-sim trade doesn't count either.
            from engine.crew.audition_tracking import score_bench_candidate_from_real_trades
            real_result = score_bench_candidate_from_real_trades(conn, pid, cutoff)
            if real_result:
                verdict = real_result["verdict"]
                detail.update(real_result["detail"])
        if n:
            try:
                bt._vix_cache = {}
                guarded = backtest_player(pid, start_date=cutoff, apply_guardrails=True)
                raw = backtest_player(pid, start_date=cutoff, apply_guardrails=False)
                g_stats = guarded.get("stats", {})
                trades = g_stats.get("total_trades") or 0
                return_pct = g_stats.get("total_return_pct")
                friction = round(g_stats.get("friction_paid") or 0.0, 2)
                total_pnl = g_stats.get("total_pnl")
                friction_to_pnl = round(friction / abs(total_pnl), 3) if total_pnl else None
                r_tested = raw.get("signals_tested") or 0
                r_blocked = (raw.get("stats") or {}).get("reentry_blocked") or 0
                spam_pct = round(r_blocked / r_tested * 100.0, 1) if r_tested else None

                detail.update({
                    "guarded_trades": trades,
                    "honest_guarded_return_pct": return_pct,
                    "spam_rate_pct": spam_pct,
                    "friction_to_pnl": friction_to_pnl,
                })

                if trades < AUDITION_CRITERIA["min_guarded_trades"]:
                    verdict = "insufficient_data"
                else:
                    bars = {
                        "trades": trades >= AUDITION_CRITERIA["min_guarded_trades"],
                        "spam": spam_pct is not None and spam_pct <= AUDITION_CRITERIA["max_spam_rate_pct"],
                        "return": return_pct is not None and return_pct > AUDITION_CRITERIA["min_honest_guarded_return_pct"],
                        "friction": friction_to_pnl is not None and friction_to_pnl <= AUDITION_CRITERIA["max_friction_to_pnl"],
                    }
                    detail["bars_passed"] = bars
                    verdict = "pass" if all(bars.values()) else "fail"
            except Exception as exc:
                verdict = "insufficient_data"
                detail["error"] = f"{type(exc).__name__}: {exc}"

        # ONE-IN-ONE-OUT (docs/DOCTRINE.md HM-ROSTER-CAP): activating a candidate
        # requires naming the incumbent it replaces. Doctrine-only today
        # (activation is still manual SQL) -- this field exists so the
        # paperwork is on record even though nothing enforces it yet.
        reason = (
            f"AUDITION [{verdict}] vs AUDITION_CRITERIA (clean_window_start={cutoff}): "
            f"{json.dumps(detail, default=str)}. "
            f"REPLACES: <blank -- required before activation; name the incumbent "
            f"player_id this seat would displace under MAX_ACTIVE_AGENTS, or NONE "
            f"if an execution slot is currently empty>."
        )
        conn.execute("""
            INSERT INTO model_adjustments
            (player_id, adjustment_type, old_value, new_value, reason, source, effective_date)
            VALUES (?, 'audition_proposed', ?, ?, ?, 'weekly_tuning_crew_audition', ?)
        """, (pid, row["halt_mode"], verdict, reason, date.today().isoformat()))
        results.append({"player_id": pid, "verdict": verdict, **detail})

    conn.commit()
    return {
        "candidates_scored": len(results),
        "pass": sum(1 for r in results if r["verdict"] == "pass"),
        "fail": sum(1 for r in results if r["verdict"] == "fail"),
        "insufficient_data": sum(1 for r in results if r["verdict"] == "insufficient_data"),
        "results": results,
    }


def run_weekly_tuning():
    """Run the weekly model tuning crew. 3 agents: Scorer → Promoter → Prompt Tuner."""
    console.log("[bold cyan]Weekly Tuning Crew: Assembling...")

    conn = _conn()

    # Gather weekly data
    # HM-FLEET-REBASELINE-2026-07-04: explicit CLEAN_CUTOFF floor on every query below,
    # in addition to the trailing lookback -- currently a no-op, protects against a
    # future lookback-window widening silently reaching into provisional pre-cutoff data.
    week_lessons = conn.execute("""
        SELECT player_id, grade, symbol, pnl, lesson
        FROM daily_lessons
        WHERE date >= date('now', '-7 days') AND date >= ?
        ORDER BY player_id, date
    """, (CLEAN_CUTOFF,)).fetchall()

    week_trades = conn.execute("""
        SELECT t.player_id, p.display_name, COUNT(*) as trade_count,
               SUM(CASE WHEN t.action LIKE 'BUY%' THEN 1 ELSE 0 END) as buys,
               SUM(CASE WHEN t.action = 'SELL' THEN 1 ELSE 0 END) as sells
        FROM trades t
        JOIN ai_players p ON t.player_id = p.id
        WHERE t.executed_at >= date('now', '-7 days') AND t.executed_at >= ?
        GROUP BY t.player_id
    """, (CLEAN_CUTOFF,)).fetchall()

    active_models = conn.execute("""
        SELECT id, display_name, provider, model_id, is_active, is_paused
        FROM ai_players WHERE is_active = 1 OR is_paused = 1
    """).fetchall()

    # data_window filter: never pull a 'provisional_pre_cutoff' row into PREVIOUS SCORES
    # context, even if some future change relaxes the '-14 days' lookback.
    prev_scores = conn.execute("""
        SELECT player_id, overall_score FROM model_scores
        WHERE period = 'weekly' AND date >= date('now', '-14 days') AND date >= ?
          AND (data_window IS NULL OR data_window != 'provisional_pre_cutoff')
        ORDER BY date DESC
    """, (CLEAN_CUTOFF,)).fetchall()

    conn.close()

    # HM-FLEET-REBASELINE-2026-07-04: spam_rate_pct per active agent (raw reentry_blocked /
    # signals_tested over the trailing week) — feeds the scorecard alongside win_rate/
    # regime_alignment/confidence_calibration below.
    spam_rates = _weekly_spam_rates([m["id"] for m in active_models])

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
                    spam_pct = spam_rates.get(pid)
                    conn2 = _conn()
                    conn2.execute("""
                        INSERT INTO model_scores
                        (player_id, period, date, win_rate, regime_alignment,
                         confidence_calibration, overall_score, spam_rate_pct, data_window)
                        VALUES (?, 'weekly', ?, ?, ?, ?, ?, ?, 'clean_window_only')
                    """, (
                        pid, date.today().isoformat(),
                        data.get("win_rate", 0), data.get("regime_alignment", 0),
                        data.get("confidence_calibration", 0), data["overall_score"],
                        spam_pct
                    ))
                    # 'clean_window_only' is always correct here: week_lessons/week_trades
                    # above are hard-floored at CLEAN_CUTOFF, so this row's underlying data
                    # can never include provisional pre-cutoff signals.
                    conn2.commit()
                    conn2.close()
                    scores_saved += 1

                    if spam_pct is not None and spam_pct > 30:
                        console.log(f"[yellow]  {pid}: spam_rate_pct={spam_pct} (>30% threshold)")
                    if spam_pct is not None and spam_pct > 60:
                        conn5 = _conn()
                        prior = conn5.execute("""
                            SELECT spam_rate_pct FROM model_scores
                            WHERE player_id = ? AND period = 'weekly' AND spam_rate_pct IS NOT NULL
                            ORDER BY date DESC LIMIT 2
                        """, (pid,)).fetchall()
                        # prior[0] is the row just inserted above; prior[1] is last week's.
                        if len(prior) >= 2 and prior[1]["spam_rate_pct"] > 60:
                            conn5.execute("""
                                INSERT INTO model_adjustments
                                (player_id, adjustment_type, old_value, new_value, reason, source, effective_date)
                                VALUES (?, 'halt_review_proposed', ?, 'active', ?, 'weekly_crew_spam_gate', ?)
                            """, (
                                pid, str(prior[1]["spam_rate_pct"]),
                                f"spam_rate_pct > 60% for 2 consecutive weekly runs "
                                f"({prior[1]['spam_rate_pct']}%, {spam_pct}%) — proposal only, "
                                f"does not change halt_mode",
                                date.today().isoformat()
                            ))
                            conn5.commit()
                            console.log(f"[red]  {pid}: spam_rate_pct>60% sustained 2 weeks -- halt review proposed")
                        conn5.close()
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
        f'- {{"player_id":"deepseek-7b-grok4","adjustment_type":"confidence_modifier","new_value":"0.85","reason":"overconfident last week"}}\n'
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

    # ── Agent 4: Audition Scorer ──
    console.log("[cyan]  Agent 4: Audition Scorer grading bench candidates...")
    conn6 = _conn()
    try:
        audition_summary = _run_auditions(conn6)
    finally:
        conn6.close()
    console.log(
        f"[green]  Auditions: {audition_summary['candidates_scored']} scored -- "
        f"{audition_summary['pass']} pass, {audition_summary['fail']} fail, "
        f"{audition_summary['insufficient_data']} insufficient_data"
    )

    # ── Agent 5: Incumbent Audition Tracker ──
    # HM-INCUMBENT-AUDITION-TRACKER-2026-07-05: _run_auditions() above only
    # scores halt_mode != 'active' bench candidates -- it never touches the
    # two currently-active ACTIVE-AUDITIONING seats, since they already hold
    # seats. This closes that gap. Both are SUSPENDED as of HM-EDGE-
    # PROVENANCE (2026-07-05) -- see engine.crew.audition_tracking module
    # docstring for why.
    console.log("[cyan]  Agent 5: Incumbent Audition Tracker...")
    conn7 = _conn()
    try:
        from engine.crew.audition_tracking import track_incumbent_auditions
        incumbent_auditions = track_incumbent_auditions(conn7)
    finally:
        conn7.close()
    for entry in incumbent_auditions:
        if entry.get("suspended"):
            console.log(
                f"[yellow]  {entry['player_id']}: SUSPENDED -- "
                f"{entry['clean_guarded_trades']}/{entry['target']} broker-executed"
            )
        else:
            console.log(
                f"[green]  {entry['player_id']}: {entry['clean_guarded_trades']}/{entry['target']}"
            )

    return {
        "status": "complete",
        "models_scored": scores_saved,
        "adjustments_saved": adj_saved,
        "scores": score_map,
        "auditions": audition_summary,
        "incumbent_auditions": incumbent_auditions,
    }


if __name__ == "__main__":
    result = run_weekly_tuning()
    print(json.dumps(result, indent=2, default=str))
