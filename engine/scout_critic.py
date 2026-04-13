"""engine/scout_critic.py — Scout→Critic deep-dive for A+ Holly signals.

Scout:  pure data gather (GEX levels, options flow, F&G, regime, sectors, earnings)
Critic: LLM evaluation — scores 1-10, approves if score >= 7

Total timeout budget: 90 s (Scout 45 s + Critic 45 s).
On any timeout or infrastructure failure: APPROVE with scout_timeout=True flag.
Never blocks a good trade because of an infra hiccup.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger("scout_critic")

TRADER_DB        = "data/trader.db"
OLLAMA_URL       = "http://127.0.0.1:11434/api/generate"
CRITIC_MODEL     = "qwen3.5:9b"
SCOUT_TIMEOUT    = 45   # seconds
CRITIC_TIMEOUT   = 45   # seconds
CRITIC_MIN_SCORE = 7    # approve if Critic scores >= this


# ── DB setup ──────────────────────────────────────────────────────────────────

def ensure_tables() -> None:
    """Create holly_deepdives table if not present."""
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""CREATE TABLE IF NOT EXISTS holly_deepdives (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id        TEXT,
        ticker           TEXT    NOT NULL,
        grade            TEXT,
        scout_brief      TEXT,
        critic_score     INTEGER,
        critic_reasoning TEXT,
        approved         INTEGER NOT NULL DEFAULT 0,
        created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
    )""")
    c.commit()
    c.close()


# ── Scout: data gathering (no LLM) ────────────────────────────────────────────

def _gather_scout_brief(symbol: str, market_ctx: dict[str, Any]) -> str:
    """Collect GEX, options flow, F&G, regime, sectors, and earnings into a brief."""
    parts: list[str] = []

    # GEX level from DB
    try:
        c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=10)
        c.row_factory = sqlite3.Row
        row = c.execute(
            "SELECT composite_score, composite_signal, gamma_flip, call_wall, put_wall "
            "FROM gex_levels WHERE symbol=? ORDER BY calc_time DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        c.close()
        if row:
            parts.append(
                f"GEX: score={row['composite_score']:.2f} "
                f"signal={row['composite_signal']} "
                f"gamma_flip={row['gamma_flip']} "
                f"call_wall={row['call_wall']} put_wall={row['put_wall']}"
            )
    except Exception:
        pass

    # Market context from market_ctx (already fetched by crew_scanner)
    vix     = market_ctx.get("vix", "?")
    fg      = market_ctx.get("fear_greed", market_ctx.get("fg_score", "?"))
    regime  = market_ctx.get("session_type", market_ctx.get("regime", "?"))
    spy_ret = market_ctx.get("spy_day_return", "?")
    parts.append(f"Market: VIX={vix} F&G={fg} Regime={regime} SPY={spy_ret}")

    # Sector momentum
    sectors = market_ctx.get("sector_heatmap", {})
    if isinstance(sectors, dict) and sectors:
        top = sorted(sectors.items(), key=lambda x: float(x[1] or 0), reverse=True)[:3]
        parts.append("Sectors leading: " + ", ".join(f"{s}({v:.1f}%)" for s, v in top))

    # Options flow (truncated — just orientation)
    flow = market_ctx.get("options_flow", "")
    if flow:
        parts.append(f"Options flow: {str(flow)[:150]}")

    # Upcoming earnings — yfinance, best-effort
    try:
        import yfinance as yf
        from datetime import datetime as _dt
        t = yf.Ticker(symbol)
        dates = getattr(t, "earnings_dates", None)
        if dates is not None and len(dates) > 0:
            next_d = dates.index[0]
            nd = next_d.date() if hasattr(next_d, "date") else next_d
            days_until = (nd - _dt.now().date()).days
            if 0 <= days_until <= 7:
                parts.append(f"EARNINGS WARNING: {symbol} reports in {days_until}d")
    except Exception:
        pass

    return "\n".join(parts) or "No additional context available."


# ── Critic: LLM evaluation ────────────────────────────────────────────────────

def _build_critic_prompt(
    symbol: str, grade: str, scout_brief: str, signal_reason: str
) -> str:
    return (
        "/no_think\n"
        "You are the Critic. Score this A+ trade signal 1-10.\n\n"
        f"TICKER: {symbol} | GRADE: {grade}\n"
        f"SIGNAL REASON: {signal_reason}\n\n"
        f"SCOUT BRIEF:\n{scout_brief}\n\n"
        "Score criteria:\n"
        "  • Thesis strength and clarity\n"
        "  • Risk/reward profile\n"
        "  • Regime fit (VIX, F&G, sector alignment)\n"
        "  • Correlation risk with existing positions\n\n"
        "Respond ONLY with:\n"
        "SCORE: [1-10]\n"
        "REASONING: [2 sentences max]"
    )


def _parse_critic_response(text: str) -> tuple[int, str]:
    """Extract SCORE and REASONING from Critic output."""
    score = 5
    reasoning = text.strip()[:300]
    for line in text.splitlines():
        ls = line.strip()
        if ls.upper().startswith("SCORE:"):
            try:
                digits = "".join(c for c in ls.split(":", 1)[1] if c.isdigit())[:2]
                score = max(1, min(10, int(digits)))
            except Exception:
                pass
        elif ls.upper().startswith("REASONING:"):
            reasoning = ls.split(":", 1)[1].strip()[:300]
    return score, reasoning


# ── Main entry ─────────────────────────────────────────────────────────────────

def run_scout_critic(
    symbol: str,
    grade: str,
    signal_id: str,
    signal_reason: str,
    market_ctx: dict[str, Any],
) -> dict[str, Any]:
    """
    Run Scout→Critic deep-dive for an A+ signal.

    Returns dict with keys:
        approved (bool), critic_score (int), critic_reasoning (str),
        scout_brief (str), scout_timeout (bool)
    """
    ensure_tables()
    scout_timeout_flag = False

    # Scout phase — data gather in thread so we can enforce timeout
    _scout_result: dict = {}

    def _scout_worker() -> None:
        _scout_result["brief"] = _gather_scout_brief(symbol, market_ctx)

    t_scout = threading.Thread(target=_scout_worker, daemon=True)
    t_scout.start()
    t_scout.join(timeout=SCOUT_TIMEOUT)

    if "brief" not in _scout_result:
        scout_brief = "Scout timed out — no brief available."
        scout_timeout_flag = True
        logger.warning(f"[Scout] timeout on {symbol} ({SCOUT_TIMEOUT}s)")
    else:
        scout_brief = _scout_result["brief"]

    # Critic phase — LLM evaluation
    critic_score = 5
    critic_reasoning = "Critic not run (scout timed out)."

    if not scout_timeout_flag:
        try:
            prompt = _build_critic_prompt(symbol, grade, scout_brief, signal_reason)
            resp = requests.post(
                OLLAMA_URL,
                json={"model": CRITIC_MODEL, "prompt": prompt, "stream": False},
                timeout=CRITIC_TIMEOUT,
            )
            if resp.status_code == 200:
                raw = resp.json().get("response", "")
                critic_score, critic_reasoning = _parse_critic_response(raw)
            else:
                critic_reasoning = f"Critic HTTP {resp.status_code}."
                scout_timeout_flag = True
        except requests.exceptions.Timeout:
            critic_reasoning = "Critic LLM timed out."
            scout_timeout_flag = True
            logger.warning(f"[Critic] LLM timeout on {symbol} ({CRITIC_TIMEOUT}s)")
        except Exception as e:
            critic_reasoning = f"Critic error: {e}"
            scout_timeout_flag = True
            logger.warning(f"[Critic] error on {symbol}: {e}")

    # Decision: timeout → APPROVE with flag; otherwise apply threshold
    if scout_timeout_flag:
        approved = True
        critic_reasoning = f"[scout_timeout=true] {critic_reasoning}"
    else:
        approved = critic_score >= CRITIC_MIN_SCORE

    # Persist to holly_deepdives
    try:
        c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute(
            """INSERT INTO holly_deepdives
               (signal_id, ticker, grade, scout_brief,
                critic_score, critic_reasoning, approved, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                signal_id, symbol, grade, scout_brief[:2000],
                critic_score, critic_reasoning[:500],
                1 if approved else 0,
                datetime.utcnow().isoformat(),
            ),
        )
        c.commit()
        c.close()
    except Exception as e:
        logger.error(f"holly_deepdives insert error: {e}")

    verdict = "APPROVED" if approved else "REJECTED"
    logger.info(
        f"🧠 Scout→Critic {symbol} grade={grade} "
        f"score={critic_score}/10 → {verdict} | {critic_reasoning[:80]}"
    )

    return {
        "approved": approved,
        "critic_score": critic_score,
        "critic_reasoning": critic_reasoning,
        "scout_brief": scout_brief,
        "scout_timeout": scout_timeout_flag,
    }
