"""
Counselor Troi's Guidance — Ready Room Trade Advisor
------------------------------------------------------
Consults session briefing + Red Alert condition before any trade.

Usage (call from any agent before submitting an order):

    from engine.ready_room_advisor import should_i_trade, get_trade_context

    advice = should_i_trade("SPY", "BUY")
    context_str = get_trade_context()   # inject into LLM prompt

Tables: trade_advisories  (SACRED — never dropped/truncated)
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

# ── DB references ─────────────────────────────────────────────────────────────
# Ready Room lives in data/trader.db; Red Alert lives in autonomous_trader.db
_TRADEMINDS_DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
_ALERT_DB = os.environ.get("TRADER_DB", "autonomous_trader.db")


def _init_db() -> None:
    conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_advisories (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date      TEXT NOT NULL,
            player_id       TEXT,
            symbol          TEXT,
            proposed_action TEXT,
            signal          TEXT,
            multiplier      REAL,
            reason          TEXT,
            condition       TEXT,
            session_type    TEXT,
            vix_regime      TEXT,
            overridden      INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


_init_db()


# ── Data gathering ────────────────────────────────────────────────────────────
def _get_briefing() -> dict[str, Any]:
    try:
        from engine.ready_room import get_latest_briefing
        return get_latest_briefing() or {}
    except Exception:
        return {}


def _get_condition() -> dict[str, Any]:
    try:
        from engine.red_alert import get_current_condition
        return get_current_condition() or {}
    except Exception:
        return {}


def _get_alerts_count() -> int:
    try:
        from engine.red_alert import get_today_alerts
        return len(get_today_alerts(limit=100))
    except Exception:
        return 0


def _get_momentum() -> dict[str, Any]:
    try:
        from engine.momentum_tracker import get_intraday_momentum
        return get_intraday_momentum() or {}
    except Exception:
        return {}


# ── Core advisory logic ───────────────────────────────────────────────────────
def should_i_trade(
    symbol: str,
    proposed_action: str,
    player_id: str = "unknown",
) -> dict[str, Any]:
    """
    Counselor Troi's pre-trade consultation.

    Returns:
      {
        "signal":                "GO" | "CAUTION" | "STAND_DOWN",
        "position_size_multiplier": float (0.0 / 0.5 / 0.75 / 1.0),
        "reason":                str,
        "levels":                {"put_wall": float, "call_wall": float, "max_pain": float},
        "session_type":          str,
        "vix_regime":            str,
        "alerts_active":         int,
        "condition":             str,
        "condition_score":       float | None,
      }
    """
    briefing   = _get_briefing()
    condition  = _get_condition()
    alerts_cnt = _get_alerts_count()

    # ── No data fallback ──────────────────────────────────────────────────────
    if not briefing or not briefing.get("session_type"):
        result = {
            "signal":                   "GO",
            "position_size_multiplier": 0.8,
            "reason":                   "No Ready Room data yet — using default sizing (Counselor Troi recommends caution until morning briefing runs)",
            "levels":                   {},
            "session_type":             "UNKNOWN",
            "vix_regime":               "UNKNOWN",
            "alerts_active":            0,
            "condition":                "UNKNOWN",
            "condition_score":          None,
        }
        _log_advisory(result, player_id, symbol, proposed_action, overridden=False)
        return result

    # ── Extract briefing data ─────────────────────────────────────────────────
    session_type  = briefing.get("session_type", "UNKNOWN")
    spot_price    = briefing.get("spot_price")  or 0.0
    call_wall     = briefing.get("call_wall")   or 0.0
    put_wall      = briefing.get("put_wall")    or 0.0
    max_pain      = briefing.get("max_pain")    or 0.0
    gamma_flip    = briefing.get("gamma_flip")  or 0.0
    vix           = briefing.get("vix")         or 0.0
    pc_ratio      = briefing.get("pc_ratio")    or 1.0
    total_gex_b   = briefing.get("total_gex_b") or 0.0

    cond_label    = condition.get("condition", "UNKNOWN")
    cond_score    = condition.get("condition_score")
    vix_regime    = condition.get("vix_regime") or briefing.get("vix_regime") or "UNKNOWN"

    levels = {"put_wall": put_wall, "call_wall": call_wall, "max_pain": max_pain, "gamma_flip": gamma_flip}

    # ── Traffic-light → base signal ──────────────────────────────────────────
    action_upper = proposed_action.upper()
    is_long  = action_upper in ("BUY", "BUY_CALL")
    is_short = action_upper in ("SHORT", "SELL_SHORT", "BUY_PUT")

    reason_parts: list[str] = []

    if cond_label == "RED":
        signal     = "STAND_DOWN"
        multiplier = 0.0
        reason_parts.append(f"🚨 Red Alert condition ({cond_score:.0f}/100) — market structure unfavorable")
    elif cond_label == "YELLOW":
        signal     = "CAUTION"
        multiplier = 0.5
        reason_parts.append(f"⚠️ Yellow Alert condition ({cond_score:.0f}/100) — mixed signals, half size")
    else:
        # GREEN — check if direction aligns with session structure
        signal     = "GO"
        multiplier = 1.0

        if session_type == "TRENDING_BULL":
            if is_long:
                reason_parts.append("🟢 TRENDING_BULL session + long direction = aligned, full size")
            elif is_short:
                multiplier = 0.5
                reason_parts.append("⚠️ TRENDING_BULL session but shorting against trend — half size")
        elif session_type == "TRENDING_BEAR":
            if is_short:
                reason_parts.append("🟢 TRENDING_BEAR session + short direction = aligned, full size")
            elif is_long:
                multiplier = 0.5
                reason_parts.append("⚠️ TRENDING_BEAR session but buying against trend — half size")
        elif session_type in ("CHOP", "REVERSAL_RISK"):
            signal     = "CAUTION"
            multiplier = 0.5
            icon       = "🔄" if session_type == "CHOP" else "⚠️"
            reason_parts.append(f"{icon} {session_type} session — options structure suggests range-bound, half size")
        elif session_type == "VOLATILE":
            signal     = "CAUTION"
            multiplier = 0.5
            reason_parts.append("⚡ VOLATILE session (VIX>30, negative GEX) — half size, widen stops")

    # ── VIX stress cap ────────────────────────────────────────────────────────
    if vix_regime == "STRESSED" and multiplier > 0.75:
        multiplier = 0.75
        reason_parts.append(f"VIX STRESSED ({vix:.1f}) — capping at 75% size")
    elif vix_regime == "CRISIS" and multiplier > 0:
        multiplier = min(multiplier, 0.5)
        reason_parts.append(f"VIX CRISIS ({vix:.1f}) — hard cap at 50% size")

    # ── Wall proximity warning ────────────────────────────────────────────────
    if spot_price > 0:
        if call_wall > 0:
            dist_call_pct = (call_wall - spot_price) / spot_price * 100
            if 0 <= dist_call_pct <= 0.3:
                reason_parts.append(f"🚧 SPY within {dist_call_pct:.2f}% of call wall ${call_wall:.2f} — resistance zone ahead")
                if is_long and multiplier > 0.5:
                    multiplier = 0.5
        if put_wall > 0:
            dist_put_pct = (spot_price - put_wall) / spot_price * 100
            if 0 <= dist_put_pct <= 0.3:
                reason_parts.append(f"🛡️ SPY within {dist_put_pct:.2f}% of put wall ${put_wall:.2f} — support zone nearby")
                if is_short and multiplier > 0.5:
                    multiplier = 0.5

    # ── Active alerts note ────────────────────────────────────────────────────
    if alerts_cnt > 0:
        reason_parts.append(f"⚡ {alerts_cnt} active alert(s) today — review before sizing up")

    # ── P/C ratio context ─────────────────────────────────────────────────────
    if pc_ratio > 1.3:
        reason_parts.append(f"P/C {pc_ratio:.2f} — heavy put buying, market hedged bearishly")
    elif pc_ratio < 0.7:
        reason_parts.append(f"P/C {pc_ratio:.2f} — call heavy, watch for complacency reversal")

    # ── Deflector Shield (Event Shield) ───────────────────────────────────────
    try:
        from engine.event_shield import get_advisor_cap
        ev_cap, ev_reason = get_advisor_cap()
        if ev_cap < multiplier:
            multiplier = ev_cap
            if ev_reason:
                reason_parts.insert(0, ev_reason)
            if ev_cap == 0.0 and signal != "STAND_DOWN":
                signal = "STAND_DOWN"
            elif ev_cap <= 0.25 and signal == "GO":
                signal = "CAUTION"
    except Exception:
        pass

    # ── Chekov's Broad Scan (breadth warning) ─────────────────────────────────
    try:
        from engine.breadth_scanner import get_breadth_advisor_note
        b_note = get_breadth_advisor_note()
        if b_note:
            reason_parts.append(b_note)
    except Exception:
        pass

    # ── Stellar Cartography (intermarket divergences) ─────────────────────────
    try:
        from engine.correlation_monitor import get_correlations
        corr = get_correlations()
        divs = corr.get("divergences", [])
        if divs:
            reason_parts.append(f"🌐 {divs[0]}")
    except Exception:
        pass

    # ── Lt. Uhura News Divergence ─────────────────────────────────────────────
    try:
        from engine.news_pulse import get_latest_news_pulse
        np_data = get_latest_news_pulse()
        if np_data.get("convergence_signal") == "DIVERGENCE":
            mood = np_data.get("mood_score", 0)
            direction = "bullish" if mood > 0 else "bearish"
            reason_parts.append(f"📰 News {direction} (mood {mood:+.0f}) diverges from options structure")
    except Exception:
        pass

    reason = " | ".join(reason_parts) if reason_parts else f"Standard session conditions ({session_type})"

    result = {
        "signal":                   signal,
        "position_size_multiplier": round(multiplier, 2),
        "reason":                   reason,
        "levels":                   levels,
        "session_type":             session_type,
        "vix_regime":               vix_regime,
        "alerts_active":            alerts_cnt,
        "condition":                cond_label,
        "condition_score":          cond_score,
    }
    _log_advisory(result, player_id, symbol, proposed_action, overridden=False)
    return result


def log_advisory_override(player_id: str, symbol: str, proposed_action: str, advisory: dict) -> None:
    """Call this when an agent proceeds despite STAND_DOWN advisory."""
    _log_advisory(advisory, player_id, symbol, proposed_action, overridden=True)
    from rich.console import Console
    Console().log(
        f"[bold red]⚠️  STAND_DOWN OVERRIDE: {player_id} is proceeding with {proposed_action} "
        f"{symbol} against Counselor Troi's guidance ({advisory.get('reason','')})"
    )


def _log_advisory(
    result: dict,
    player_id: str,
    symbol: str,
    proposed_action: str,
    overridden: bool,
) -> None:
    try:
        conn = sqlite3.connect(_TRADEMINDS_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT INTO trade_advisories
                (trade_date, player_id, symbol, proposed_action, signal, multiplier,
                 reason, condition, session_type, vix_regime, overridden)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            player_id, symbol, proposed_action,
            result.get("signal"), result.get("position_size_multiplier"),
            result.get("reason"), result.get("condition"),
            result.get("session_type"), result.get("vix_regime"),
            1 if overridden else 0,
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ── Context string for LLM prompts ───────────────────────────────────────────
def get_trade_context() -> str:
    """
    Returns a formatted string any agent can inject into its LLM prompt
    to incorporate market structure awareness.
    """
    briefing  = _get_briefing()
    condition = _get_condition()
    momentum  = _get_momentum()
    alerts    = _get_alerts_count()

    if not briefing or not briefing.get("session_type"):
        return "[Ready Room] No session briefing available yet — standard risk rules apply."

    session  = briefing.get("session_type", "UNKNOWN")
    spot     = briefing.get("spot_price", 0) or 0
    call_w   = briefing.get("call_wall", 0) or 0
    put_w    = briefing.get("put_wall", 0) or 0
    max_p    = briefing.get("max_pain", 0) or 0
    gflip    = briefing.get("gamma_flip", 0) or 0
    vix      = briefing.get("vix", 0) or 0
    pc_r     = briefing.get("pc_ratio", 0) or 0
    gex_b    = briefing.get("total_gex_b", 0) or 0
    cond     = condition.get("condition", "UNKNOWN")
    cond_s   = condition.get("condition_score")
    ts       = momentum.get("trend_score")

    _ST_LABELS = {
        "TRENDING_BULL":  "Trending Bull — negative GEX, above gamma flip, moves amplify",
        "TRENDING_BEAR":  "Trending Bear — negative GEX, below gamma flip, moves amplify",
        "CHOP":           "Chop — positive GEX, dealers pinning price, fade extremes",
        "REVERSAL_RISK":  "Reversal Risk — near key walls/max pain, expect rejection",
        "VOLATILE":       "Volatile — VIX>30, negative GEX, extreme swings possible",
    }
    session_desc = _ST_LABELS.get(session, session)

    lines = [
        "═══ Counselor Troi's Ready Room Intelligence ═══",
        f"Session Type : {session}  ({session_desc})",
        f"Condition    : {cond}" + (f" ({cond_s:.0f}/100)" if cond_s else ""),
        f"SPY Spot     : ${spot:.2f}",
        f"Key Levels   : Call Wall ${call_w:.2f} | Max Pain ${max_p:.2f} | Put Wall ${put_w:.2f}",
        f"Gamma Flip   : ${gflip:.2f}  ({'above' if spot > gflip > 0 else 'below'} flip)",
        f"GEX          : {'+' if gex_b >= 0 else ''}{gex_b:.2f}B  "
        f"({'dealers long gamma — mean revert' if gex_b > 0 else 'dealers short gamma — trending'})",
        f"VIX          : {vix:.1f}",
        f"P/C Ratio    : {pc_r:.2f}  "
        f"({'bearish hedging' if pc_r > 1.1 else 'bullish lean' if pc_r < 0.8 else 'neutral'})",
    ]
    if ts is not None:
        lines.append(f"Momentum     : {'+' if ts >= 0 else ''}{ts:.0f}/100  "
                     f"({'bullish' if ts > 30 else 'bearish' if ts < -30 else 'neutral'})")
    if alerts > 0:
        lines.append(f"Active Alerts: {alerts} alert(s) fired today")
    lines.append("═══════════════════════════════════════════════")

    return "\n".join(lines)
