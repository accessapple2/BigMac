"""engine/shadow_csp.py — HM-SHADOW-CSP (2026-06-07).

Options-income BAKE-OFF, shadow-first. A 2nd CSP options-income seat, run as
TWO candidate shadow agents that emit cash-secured puts to the GHOST options
book and are scored forward (return-on-collateral) against the proven live
CSP producer — Counselor Troi's deterministic wheel (`engine/wheel_strategy.py`,
agent_id 'options-sosnoff', book_tag 'fleet').

WHY this shape (verified 2026-06-07 against live code/DB, not doctrine):
  • The live scorecard (engine/agent_scorecard.py) reads the `trades` table and
    CANNOT see `options_trades` — so shadow CSPs are scored by the sibling
    module engine/shadow_csp_scorecard.py, never by the live scorecard.
  • McCoy (ollama-plutus) is NOT a live CSP producer — his scorecard line is
    ~97% STOCK trades; he has 0 rows in options_trades. The real proven CSP
    seat is Troi's wheel. Baseline = Troi, not McCoy.
  • Troi's wheel is fully DETERMINISTIC (no LLM). The bake-off question is
    therefore: does an LLM-DRIVEN CSP selection (plutus-v1 vs qwen3.5) beat
    or match the deterministic baseline? Each seat builds the SAME deterministic
    candidate set Troi would, then lets its model pick which subset to sell.

SHADOW BOUNDARY (hard, by construction):
  • Writes ONLY to options_trades book_tag='ghost' via options_exec — a paper
    book that never touches a broker (options are paper-only system-wide).
  • agent_id is prefixed 'shadow-' so any accidental stock-executor path also
    trips the paper_trader.buy chokepoint.
  • Ghost CSPs are auto-managed (TP 50% / SL 2x / time-stop ≤21 DTE) by
    paper_trader._check_option_exits_canonical_short_premium and auto-expired by
    _expire_canonical — both book/agent-agnostic, already live (ai_brain.py).
    No exit wiring needed; closes accrue toward the graduation N automatically.

VRAM (protects the 7 live qwen3:8b agents):
  • plutus-v1 seat shares McCoy's already-resident model — zero new VRAM.
  • qwen3.5 seat runs OFF-HOURS (21:30 AZ) when the queue is idle and the
    live fleet is asleep; routed through the shared OllamaQueue (fair, FIFO) and
    gets the default short keep_alive (loads, scores, releases) → never evicts
    qwen3:8b.

Enablement (default-OFF; activate = set env + restart trader):
  SHADOW_CSP_ENABLED         master switch
  SHADOW_CSP_PLUTUS_ENABLED  plutus-v1 seat
  SHADOW_CSP_QWEN35_ENABLED  qwen3.5 seat (model id from SHADOW_CSP_QWEN35_MODEL in .env)
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timedelta

import pytz
from rich.console import Console

from config import OLLIE_URL
from engine.market_calendar import az_now
from engine.options_exec import open_options_trade, LEVERAGED_ETF_TICKERS

console = Console()

DB_PATH = "data/trader.db"

# ── Deterministic candidate mechanics — mirror Troi's wheel for a fair baseline ──
WHEEL_TICKERS = ["TQQQ", "SOXL", "UPRO", "TNA", "QQQ", "SPY"]
DTE_TARGET = 30
OTM_PCT = 0.12
MIN_VIX = 18.0
MIN_PREMIUM_RETURN = 3.0       # percent, return on collateral at entry
MAX_POSITIONS = 3              # max concurrent open ghost CSPs per seat
POSITION_SIZE_PCT = 0.25

# Ghost research notional — the ghost options_book holds only ~$2.5K, far below
# one CSP's cash-secured collateral, so we size on a fixed research notional.
# Return-on-collateral is a RATIO; absolute book cash is irrelevant to scoring.
SHADOW_CSP_NOTIONAL = float(os.environ.get("SHADOW_CSP_NOTIONAL", "100000"))
# Wildcard seat's model id is sourced from .env ONLY (SHADOW_CSP_QWEN35_MODEL) — the
# guarded model tag is never a literal in tracked code; empty default → seat self-skips.
QWEN35_MODEL = os.getenv("SHADOW_CSP_QWEN35_MODEL", "")

SEATS = {
    "plutus": {
        "agent_id": "shadow-plutus-csp",
        "model": "plutus-v1",
        "enable_env": "SHADOW_CSP_PLUTUS_ENABLED",
        "rth_only": True,   # shares McCoy's resident model → run during RTH
    },
    "qwen35": {
        "agent_id": "shadow-qwen35-csp",
        "model": QWEN35_MODEL,
        "enable_env": "SHADOW_CSP_QWEN35_ENABLED",
        "rth_only": False,  # off-hours batch → no market-hours gate (no eviction)
    },
}

_done_today: dict[str, str] = {}   # seat -> YYYY-MM-DD last completed


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes", "on")


def _seat_enabled(seat: dict) -> bool:
    return _truthy("SHADOW_CSP_ENABLED") and _truthy(seat["enable_env"])


def _is_market_hours() -> bool:
    now = az_now()
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return 400 <= mins <= 780  # 6:40 AM–1:00 PM AZ (9:30–4 PM ET)


def _latest_regime() -> str | None:
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT regime FROM regime_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _vix() -> float:
    try:
        from engine.fear_greed import get_fear_greed_index
        fg = get_fear_greed_index()
        v = fg.get("signals", {}).get("vix", {}).get("value")
        if v:
            return float(v)
    except Exception:
        pass
    return 20.0


def _open_ghost_csps(agent_id: str) -> set[str]:
    """Symbols this seat currently has open in the ghost book (avoid doubling up)."""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT symbol FROM options_trades "
            "WHERE book_tag='ghost' AND agent_id=? AND status='open' AND structure='csp'",
            (agent_id,),
        ).fetchall()
        conn.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _build_candidates(vix: float, held: set[str]) -> list[dict]:
    """Deterministic CSP candidate set — identical mechanics to run_wheel_scan,
    so the LLM seats and the deterministic baseline see the same universe."""
    from engine.market_data import get_stock_price

    budget = SHADOW_CSP_NOTIONAL * POSITION_SIZE_PCT
    out: list[dict] = []
    expiry = (datetime.now() + timedelta(days=DTE_TARGET)).strftime("%Y-%m-%d")
    for ticker in WHEEL_TICKERS:
        if ticker in held:
            continue
        # HM-DOOR1-CENTRALIZE 2026-07-03: this file never checked door1 at all
        # (confirmed: a 2026-06-28 ghost-book UPRO write slipped through here).
        # open_options_trade() itself now blocks this centrally regardless of
        # what happens above it -- this early-exit is purely to avoid wasting
        # an LLM scoring call on a candidate that would be rejected downstream.
        if ticker in LEVERAGED_ETF_TICKERS:
            continue
        try:
            price = float(get_stock_price(ticker).get("price", 0) or 0)
        except Exception:
            price = 0.0
        if price <= 0:
            continue
        strike = round(price * (1 - OTM_PCT), 2)
        # P0-A 2026-07-07: real-quote premium, replacing the VIX-scaled
        # formula that was byte-identical to wheel_strategy.py's (this
        # file's own docstring: "builds the SAME deterministic candidate set
        # Troi would") -- confirmed the shadow CSP bakeoff was tainted by
        # the exact same synthetic-fill bug as the live baseline it scores
        # against. No fallback: a None quote means skip this ticker, never
        # fabricate a candidate. See docs/XO_BACKLOG.md "P0-A: OPTIONS FILL
        # INTEGRITY".
        from engine.options_pricing import get_real_csp_premium
        premium, premium_source = get_real_csp_premium(ticker, expiry, strike)
        if premium is None:
            continue
        contracts = max(1, int(budget / (strike * 100)))
        collateral = strike * 100 * contracts
        if collateral <= 0:
            continue
        premium_return = (premium * 100 * contracts) / collateral * 100.0
        if premium_return < MIN_PREMIUM_RETURN:
            continue
        out.append({
            "ticker": ticker, "price": round(price, 2), "strike": strike,
            "premium": premium, "premium_source": premium_source,
            "contracts": contracts, "dte": DTE_TARGET,
            "expiry": expiry, "premium_return_pct": round(premium_return, 2),
        })
    return out


def _strip_think(text: str) -> str:
    """Remove <think>…</think> blocks that reasoning models (qwen3.x) emit."""
    return re.sub(r"<think>.*?</think>", "", text or "", flags=re.DOTALL | re.IGNORECASE)


def _llm_select(model: str, candidates: list[dict], regime: str | None,
                vix: float) -> list[str]:
    """Ask the seat's model which candidates to SELL. Returns selected tickers.
    On any failure → [] (skip the cycle; never fabricate a trade)."""
    if not candidates:
        return []
    try:
        from engine.providers.ollama_provider import OllamaProvider
    except Exception as e:
        console.log(f"[yellow][SHADOW-CSP] provider import failed: {type(e).__name__}: {e!r}")
        return []

    lines = "\n".join(
        f"- {c['ticker']}: sell {c['contracts']}x {c['strike']}P "
        f"({OTM_PCT*100:.0f}% OTM from ${c['price']}), ~${c['premium']}/sh premium, "
        f"{c['premium_return_pct']}% return-on-collateral, {c['dte']}d"
        for c in candidates
    )
    prompt = (
        "You are a cash-secured-put (CSP) options-income selector. Sell premium on "
        "high-IV ETFs only when the risk/reward is favorable; skip names you would "
        f"not want to be assigned. Market regime: {regime or 'unknown'}. VIX: {vix:.1f}.\n\n"
        f"Candidate CSPs (already 12% OTM, sized):\n{lines}\n\n"
        "Reply with ONLY a JSON object: {\"sell\": [\"TICKER\", ...], \"reason\": \"...\"}. "
        "Pick the subset worth selling now (may be empty)."
    )
    try:
        provider = OllamaProvider(player_id=model, model=model, url=OLLIE_URL)
        raw = provider.call_model(prompt)
    except Exception as e:
        console.log(f"[yellow][SHADOW-CSP] {model} call failed: {type(e).__name__}: {e!r}")
        return []

    raw = _strip_think(raw)
    m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if not m:
        console.log(f"[yellow][SHADOW-CSP] {model} returned no JSON: {raw[:120]!r}")
        return []
    try:
        parsed = json.loads(m.group(0))
        valid = {c["ticker"] for c in candidates}
        return [t for t in parsed.get("sell", []) if t in valid]
    except Exception as e:
        console.log(f"[yellow][SHADOW-CSP] {model} JSON parse failed: {type(e).__name__}: {e!r}")
        return []


def _emit(agent_id: str, c: dict, regime: str | None, vix: float, model: str) -> int | None:
    reason = (
        f"[SHADOW-CSP · {model}] Sell {c['contracts']}x {c['ticker']} ${c['strike']}P "
        f"({OTM_PCT*100:.0f}% OTM from ${c['price']}), real quote ({c.get('premium_source','?')}) "
        f"${c['premium']}/sh, {c['premium_return_pct']}% ROC, {c['dte']}d. "
        f"VIX {vix:.1f}, regime {regime or '?'}. "
        f"Ghost-book observation only — scored forward vs Troi baseline."
    )
    return open_options_trade(
        book_tag="ghost",
        agent_id=agent_id,
        structure="csp",
        symbol=c["ticker"],
        expiration=c["expiry"],
        legs=[{"side": "short", "type": "put", "strike": c["strike"],
               "qty": c["contracts"], "entry_price": c["premium"]}],
        regime=regime,
        vix=vix,
        notes=reason[:500],
    )


def run_shadow_csp(seat_key: str) -> dict:
    """Run one shadow CSP seat. Idempotent per day. Returns a small summary."""
    seat = SEATS[seat_key]
    agent_id, model = seat["agent_id"], seat["model"]
    if not _seat_enabled(seat):
        return {"seat": seat_key, "skipped": "disabled"}
    if not model:
        # e.g. qwen35 seat with SHADOW_CSP_QWEN35_MODEL unset in .env — skip, don't
        # call an empty model id.
        return {"seat": seat_key, "skipped": "no_model_configured"}

    today = az_now().strftime("%Y-%m-%d")
    if _done_today.get(seat_key) == today:
        return {"seat": seat_key, "skipped": "done_today"}
    if seat["rth_only"] and not _is_market_hours():
        return {"seat": seat_key, "skipped": "outside_rth"}

    held = _open_ghost_csps(agent_id)
    if len(held) >= MAX_POSITIONS:
        _done_today[seat_key] = today
        return {"seat": seat_key, "skipped": "max_positions", "open": len(held)}

    vix = _vix()
    if vix < MIN_VIX:
        _done_today[seat_key] = today
        return {"seat": seat_key, "skipped": f"vix_{vix:.0f}_below_{MIN_VIX:.0f}"}

    regime = _latest_regime()
    candidates = _build_candidates(vix, held)
    selected = _llm_select(model, candidates, regime, vix)

    emitted: list[str] = []
    by_ticker = {c["ticker"]: c for c in candidates}
    for ticker in selected:
        if len(held) + len(emitted) >= MAX_POSITIONS:
            break
        c = by_ticker.get(ticker)
        if not c:
            continue
        tid = _emit(agent_id, c, regime, vix, model)
        if tid:
            emitted.append(ticker)
            console.log(
                f"[bold magenta]🌑 SHADOW-CSP[{seat_key}] sold {c['contracts']}x "
                f"{ticker} ${c['strike']}P @ ${c['premium']} | {c['premium_return_pct']}% ROC "
                f"| ghost trade_id={tid}"
            )

    _done_today[seat_key] = today
    return {"seat": seat_key, "model": model, "candidates": len(candidates),
            "selected": selected, "emitted": emitted}


def run_shadow_csp_plutus() -> dict:
    return run_shadow_csp("plutus")


def run_shadow_csp_qwen35() -> dict:
    return run_shadow_csp("qwen35")


if __name__ == "__main__":
    # Manual dry-run helper (still gated by env flags).
    import pprint
    pprint.pprint(run_shadow_csp_plutus())
    pprint.pprint(run_shadow_csp_qwen35())
