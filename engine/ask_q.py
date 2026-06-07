"""HM-ASK-Q — Q's second role: on-demand DEEP analyst (separate from the live
voting role q-witness uses in the War Room).

You ASK Q a question; it answers deeply on **grok-4.20-0309-reasoning** (low
volume, pricier per call due to reasoning tokens). The flagship path is a
portfolio synthesis that pulls BOTH the unified net-worth view AND the real
tradeable Schwab book + the platform's own Kirk-Grok advisor recs, then gives
an INDEPENDENT take that cross-references the advisor.

ADVISORY ONLY — reads real_holdings.json read-only, recommends, NEVER trades.
Exact-tick cost accounting (cost_in_usd_ticks). Per-day cap Q_ASK_DAILY_CAP.
X Search (live sentiment, $5/1K) is FLAG-GATED OFF by default.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from pathlib import Path

import requests

logger = logging.getLogger("ask_q")

_ROOT = Path(__file__).resolve().parent.parent
TRADER_DB = _ROOT / "data" / "trader.db"
REAL_HOLDINGS = _ROOT / "data" / "real_holdings.json"

XAI_BASE_URL = "https://api.x.ai/v1"
MODEL = os.getenv("Q_ASK_MODEL", "grok-4.20-0309-reasoning")   # the DEEP model
ASK_DAILY_CAP = float(os.getenv("Q_ASK_DAILY_CAP", "0.50"))
# Fallback per-M rates if a response ever lacks cost_in_usd_ticks (reasoning bills
# reasoning tokens as output).
_FB_IN, _FB_OUT = 1.25, 2.50

_PORTFOLIO_KEYWORDS = (
    "portfolio", "net worth", "networth", "net-worth", "how's", "how is",
    "recommendation", "recommend", "holdings", "positions", "metals", "schwab",
    "should i", "my book", "allocat", "rebalance", "gold", "silver",
)

_SYSTEM = (
    "You are Q — an independent deep analyst for the Captain's REAL-MONEY book. "
    "Detached, sharp; you see what the crew misses. You have the platform's own "
    "advisor (Kirk-Grok) on hand — reference its recommendations explicitly and "
    "state where you AGREE or DISAGREE, and WHY. Advisory only: you analyze and "
    "recommend, you NEVER place trades. Be specific, no filler."
)


# ─── cost accounting ────────────────────────────────────────────────────────
def _ask_daily_spend() -> float:
    """Today's Ask-Q spend (call_type LIKE 'ask%'), independent of the voting role."""
    try:
        conn = sqlite3.connect(str(TRADER_DB), timeout=10)
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(cost_usd),0) FROM api_costs "
                "WHERE player_id='q-witness' AND call_type LIKE 'ask%' "
                "AND date(timestamp)=date('now')"
            ).fetchone()
        finally:
            conn.close()
        return float(row[0] or 0.0)
    except Exception:
        return 0.0


# ─── context gathering (all read-only) ──────────────────────────────────────
def _schwab_positions() -> list[dict]:
    try:
        with REAL_HOLDINGS.open() as f:
            data = json.load(f)
        acct = (data.get("accounts") or {}).get("schwab") or {}
        return acct.get("positions") or []
    except Exception:
        return []


def _advisor_recs(limit: int = 8) -> list[dict]:
    """Latest Kirk-Grok advisor recs (portfolio_advice advisor='grok')."""
    try:
        conn = sqlite3.connect(str(TRADER_DB), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT symbol, action, confidence, reasoning, created_at "
                "FROM portfolio_advice WHERE advisor='grok' "
                "ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        finally:
            conn.close()
        # Only "fresh" recs (last 3 days) count as live; older = standby.
        out = []
        for r in rows:
            out.append({"symbol": r["symbol"], "action": r["action"],
                        "confidence": r["confidence"], "reasoning": (r["reasoning"] or "")[:200],
                        "created_at": r["created_at"]})
        return out
    except Exception:
        return []


def _gather_context() -> dict:
    ctx: dict = {}
    try:
        from engine.total_portfolio import get_unified_networth
        ctx["networth"] = get_unified_networth()
    except Exception as e:
        logger.warning("[ask_q] networth failed: %s: %r", type(e).__name__, e)
        ctx["networth"] = {}
    ctx["positions"] = _schwab_positions()
    ctx["advisor"] = _advisor_recs()
    try:
        from engine.archer.intel_sources import get_regime
        ctx["regime"] = get_regime()
    except Exception:
        ctx["regime"] = {}
    try:
        from engine.archer.convergence import compute_convergence
        conv = compute_convergence()
        ctx["convergence"] = [c for c in conv if c.get("count", 0) >= 2][:6]
    except Exception:
        ctx["convergence"] = []
    return ctx


def _fmt_pct(v) -> str:
    return f"{v:+.2f}%" if isinstance(v, (int, float)) else "—"


def _build_portfolio_user(query: str, ctx: dict) -> str:
    nw = ctx.get("networth") or {}
    buckets = nw.get("buckets") or {}
    schwab = buckets.get("schwab") or {}
    metals = buckets.get("metals") or {}
    daily = nw.get("daily") or {}
    allt = nw.get("all_time") or {}
    positions = ctx.get("positions") or []
    advisor = ctx.get("advisor") or []
    regime = ctx.get("regime") or {}
    conv = ctx.get("convergence") or []

    L = [f"CAPTAIN'S QUESTION: {query}", "", "=== REAL NET WORTH (live) ==="]
    daily_txt = (f"{daily.get('dollar')}$ ({_fmt_pct(daily.get('pct'))})"
                 if daily.get("dollar") is not None else "— (baseline pending)")
    L.append(f"Net worth ${nw.get('net_worth')} | Today {daily_txt} | "
             f"All-time ${allt.get('dollar')} ({_fmt_pct(allt.get('pct'))})")
    L.append(f"  Cash (Schwab): ${schwab.get('value')}")
    L.append(f"  Metals ${metals.get('value')} (all-time {_fmt_pct(metals.get('all_time_pct'))}):")
    for m in (metals.get("detail") or []):
        L.append(f"    {m.get('metal')}: {m.get('qty_oz')}oz @ ${m.get('spot')} — "
                 f"all-time {_fmt_pct(m.get('all_time_pct'))}, today {_fmt_pct(m.get('daily_pct'))}")

    L += ["", "=== TRADEABLE SCHWAB POSITIONS (the real book) ==="]
    if positions:
        for p in positions:
            L.append(f"  {p.get('symbol')}: {p.get('qty')} @ ${p.get('avg_cost')} avg, "
                     f"mv ${p.get('market_value')}, gain {_fmt_pct(p.get('gain_pct'))}")
    else:
        L.append("  All cash, 0 equity positions.")

    L += ["", "=== KIRK-GROK ADVISOR'S LATEST RECS ==="]
    if positions and advisor:
        for a in advisor:
            L.append(f"  {a['symbol']}: {a['action']} (conf {a.get('confidence')}) — {a['reasoning']}")
    else:
        L.append("  No live advisor recs — book is all-cash so the Kirk-Grok advisor is on "
                 "standby (it only runs on real positions). Do NOT force a cross-check.")

    L += ["", "=== MARKET CONTEXT ==="]
    L.append(f"Regime {regime.get('regime')} | VIX {regime.get('vix')} | SPY {regime.get('spy_price')}")
    L.append("Convergence: " + (", ".join(
        f"{c['symbol']} {c['count']}/5[{'+'.join(c.get('systems', []))}]" for c in conv) or "none flagged"))

    L += ["", "Give the Captain your INDEPENDENT read. Structure:",
          "1. NET WORTH — what stands out (e.g. the metals drawdown).",
          "2. TRADEABLE SIDE — your view on the Schwab positions; if all-cash, what you'd "
          "deploy into given the regime.",
          "3. ADVISOR CROSS-CHECK — name Kirk-Grok's calls and say where you AGREE / DISAGREE "
          "and WHY. If the advisor is on standby (all-cash), say so and skip the cross-check.",
          "4. ONE clear recommendation. Advisory only — you never execute."]
    return "\n".join(L)


def _build_general_user(query: str, ctx: dict) -> str:
    regime = ctx.get("regime") or {}
    conv = ctx.get("convergence") or []
    return (
        f"CAPTAIN'S QUESTION: {query}\n\n"
        f"Market context — Regime {regime.get('regime')} | VIX {regime.get('vix')} | "
        f"SPY {regime.get('spy_price')}\n"
        f"Convergence: " + (", ".join(f"{c['symbol']} {c['count']}/5" for c in conv) or "none flagged") +
        "\n\nAnswer the Captain's question directly and specifically, grounded in the context "
        "above where relevant. Give a clear view. Advisory only — you never place trades."
    )


# ─── the reasoning call ─────────────────────────────────────────────────────
def _call_q(system: str, user: str, max_tokens: int = 1500, x_search: bool = False) -> dict:
    key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
    if not key:
        raise ValueError("XAI_API_KEY not set")
    payload = {
        "model": MODEL,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": 0.4,
        "max_tokens": max_tokens,
    }
    # X Search — FLAG-GATED OFF by default ($5/1K). Opt-in only.
    if x_search:
        payload["search_parameters"] = {"mode": "on", "sources": [{"type": "x"}]}
    resp = requests.post(
        f"{XAI_BASE_URL}/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json=payload, timeout=150,   # reasoning is slower
    )
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {}) or {}
    in_tok = usage.get("prompt_tokens") or (len(user) // 4)
    out_tok = usage.get("completion_tokens") or (len(content) // 4)
    ticks = usage.get("cost_in_usd_ticks")
    exact = (float(ticks) * 1e-10) if ticks is not None else (
        in_tok / 1e6 * _FB_IN + out_tok / 1e6 * _FB_OUT)
    return {"content": content, "in_tok": in_tok, "out_tok": out_tok,
            "exact_cost": exact, "x_sources": usage.get("num_sources_used", 0)}


def ask_q(query: str, x_search: bool = False) -> dict:
    """Ask Q (deep reasoning analyst). Returns {answer, cost_usd, model, intent, ...}."""
    query = (query or "").strip()
    if not query:
        return {"answer": None, "error": "query is required"}

    spent = _ask_daily_spend()
    if spent >= ASK_DAILY_CAP:
        return {"answer": None, "capped": True,
                "error": f"Ask-Q daily cap reached (${spent:.4f} / ${ASK_DAILY_CAP:.2f}). Resets at UTC midnight."}

    intent = "portfolio" if any(k in query.lower() for k in _PORTFOLIO_KEYWORDS) else "general"
    ctx = _gather_context()
    if intent == "portfolio":
        user = _build_portfolio_user(query, ctx)
    else:
        user = _build_general_user(query, ctx)

    try:
        r = _call_q(_SYSTEM, user, x_search=bool(x_search))
    except Exception as e:
        logger.warning("[ask_q] call failed: %s: %r", type(e).__name__, e)
        return {"answer": None, "error": f"{type(e).__name__}: {e}"}

    # Exact-tick accounting (separate call_type so it caps independently of voting).
    try:
        from engine.cost_tracker import log_cost_exact
        log_cost_exact("q-witness", "ask", r["in_tok"], r["out_tok"], r["exact_cost"])
    except Exception:
        pass

    return {
        "answer": r["content"],
        "cost_usd": round(r["exact_cost"], 6),
        "input_tokens": r["in_tok"], "output_tokens": r["out_tok"],
        "model": MODEL, "intent": intent,
        "x_search": bool(x_search), "x_sources_used": r["x_sources"],
        "daily_spend_after": round(spent + r["exact_cost"], 6),
        "daily_cap": ASK_DAILY_CAP,
    }
