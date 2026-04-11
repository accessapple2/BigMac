"""
Counselor Troi's Dynamic Market Advisory
-----------------------------------------
Pure-logic advisory engine. Reads ALL current market signals together,
interprets them in plain English, and generates actionable direction.

No LLM calls — deterministic, fast, always available.

Endpoint: GET /api/ready-room/advisory
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_cache: dict[str, Any] = {}
_cache_ts: float = 0.0
_CACHE_TTL = 300  # 5-minute cache


# ─── SAFE CALL HELPER ─────────────────────────────────────────────────────────

def _s(fn, default=None):
    """Call fn(); return default on any exception."""
    try:
        return fn()
    except Exception as e:
        logger.debug("advisory data gather: %s", e)
        return default


# ─── DATA GATHERING ───────────────────────────────────────────────────────────

def _gather() -> dict[str, Any]:
    """Pull all current market data from existing engines."""
    d: dict[str, Any] = {}

    try:
        from engine.red_alert import get_current_condition
        d["condition"] = _s(get_current_condition, {}) or {}
    except Exception:
        d["condition"] = {}

    try:
        from engine.vix_monitor import get_vix_term_structure
        d["vix"] = _s(get_vix_term_structure, {}) or {}
    except Exception:
        d["vix"] = {}

    try:
        from engine.fear_greed import get_fear_greed_index
        d["fear_greed"] = _s(get_fear_greed_index, {}) or {}
    except Exception:
        d["fear_greed"] = {}

    try:
        from engine.breadth_scanner import get_breadth_snapshot
        d["breadth"] = _s(get_breadth_snapshot, {}) or {}
    except Exception:
        d["breadth"] = {}

    try:
        from engine.correlation_monitor import get_correlations
        d["correlations"] = _s(get_correlations, {}) or {}
    except Exception:
        d["correlations"] = {}

    try:
        from engine.event_shield import get_event_shield_status
        d["events"] = _s(get_event_shield_status, {}) or {}
    except Exception:
        d["events"] = {}

    try:
        from engine.news_pulse import fetch_news_pulse
        d["news"] = _s(fetch_news_pulse, {}) or {}
    except Exception:
        d["news"] = {}

    return d


# ─── SIGNAL INTERPRETATION HELPERS ────────────────────────────────────────────

def _fg_zone(score: float) -> tuple[str, str, str]:
    """Return (zone_name, color, headline_text) for an F&G score."""
    if score < 25:
        return "EXTREME_FEAR", "green", "Extreme Fear — historically a contrarian buying zone"
    elif score < 40:
        return "FEAR", "yellow", "Fear — market is worried, risk is elevated"
    elif score < 60:
        return "NEUTRAL", "yellow", "Neutral — no strong directional edge from sentiment"
    elif score < 75:
        return "GREED", "orange", "Greed — market is confident, trends tend to continue"
    else:
        return "EXTREME_GREED", "red", "Extreme Greed — historically a profit-taking zone"


def _vix_zone(vix: float) -> tuple[str, str, str]:
    """Return (zone_name, color, guidance) for a VIX reading."""
    if vix < 15:
        return "CALM", "green", "Very low — complacent market, cheap to buy protection"
    elif vix < 20:
        return "NORMAL", "green", "Normal vol — healthy market, trade your plan"
    elif vix < 25:
        return "ELEVATED", "yellow", "Elevated — reduce size, widen stops, expect bigger swings"
    elif vix < 30:
        return "HIGH", "orange", "High fear — only conviction setups, size down"
    else:
        return "CRISIS", "red", "Extreme — crisis territory, cash is a position"


def _breadth_zone(sectors_above: int, breadth_score: float) -> tuple[str, str, str]:
    """Return (zone, color, guidance) for market breadth."""
    if sectors_above >= 8:
        return "BROAD", "green", "Broad participation — healthy rally, trust the trend"
    elif sectors_above >= 5:
        return "MIXED", "yellow", "Mixed — some sectors pulling back, be selective"
    else:
        return "NARROW", "red", "Narrow/deteriorating — rally is thin, don't trust headline moves"


def _pc_zone(pc: float) -> tuple[str, str, str]:
    """Return (zone, color, guidance) for put/call ratio."""
    if pc < 0.7:
        return "CALL_HEAVY", "yellow", "Extreme call buying — bullish sentiment but contrarian caution"
    elif pc < 1.0:
        return "NORMAL", "green", "Normal — no strong edge from P/C alone"
    elif pc < 1.5:
        return "PUT_TILT", "yellow", "Put-heavy — some fear, mild contrarian bullish"
    else:
        return "PUT_EXTREME", "orange", "Very put-heavy — genuine panic or heavy hedging, watch for squeeze"


def _vix_term_zone(state: str) -> tuple[str, str]:
    """Return (color, guidance) for VIX term structure state."""
    if state == "CONTANGO":
        return "green", "Normal — no imminent panic expected, trends favored"
    elif state == "FLAT":
        return "yellow", "Flat term structure — market in tension, stay alert"
    elif state == "PARTIAL_BACKWARDATION":
        return "orange", "Partial inversion — near-term fear elevated, proceed with caution"
    elif state == "BACKWARDATION":
        return "red", "Full backwardation — acute near-term fear, something acute is happening"
    else:
        return "dim", "Term structure unclear — insufficient VIX data"


# ─── GAUGE HINTS ──────────────────────────────────────────────────────────────

def _build_gauge_hints(
    mom_score: float,
    vix: float,
    vix_state: str,
    skew: float,
    buy_pct: float,
    sectors_above: int,
) -> dict[str, str]:
    """One-line interpretation for each gauge card on tactical.html."""

    # Momentum
    if mom_score >= 50:
        mom = "Strong bullish — press longs, trend has power"
    elif mom_score >= 20:
        mom = "Mild bullish — trend up, but watch for fading"
    elif mom_score >= -20:
        mom = "Neutral — no directional edge, wait for breakout"
    elif mom_score >= -50:
        mom = "Mild bearish — selling pressure present"
    else:
        mom = "Strong bearish — do not fight the tape"

    # VIX
    vix_zone, _, vix_g = _vix_zone(vix)
    if vix > 0:
        vix_hint = vix_g
    else:
        vix_hint = "VIX unavailable"

    _, ts_g = _vix_term_zone(vix_state)
    vix_hint = f"{vix_hint} · {ts_g.split('—')[0].strip()}"

    # Skew
    if skew > 5:
        skew_h = "Steep put skew — heavy downside hedging, fear elevated"
    elif skew > 2:
        skew_h = "Mild put skew — normal hedging, slight caution"
    elif skew > -2:
        skew_h = "Near neutral — balanced options sentiment"
    else:
        skew_h = "Call skew — bullish bets dominating, watch for overconfidence"

    # Volume delta
    if buy_pct >= 65:
        vol_h = "Strong buy flow — conviction behind the move"
    elif buy_pct >= 55:
        vol_h = "Slight buy lean — mild bullish bias"
    elif buy_pct >= 45:
        vol_h = "Balanced flow — no clear buyer/seller edge"
    elif buy_pct >= 35:
        vol_h = "Sell-side lean — distribution possible"
    else:
        vol_h = "Heavy selling — institutional unloading"

    # Breadth
    if sectors_above >= 8:
        brd_h = "Broad — all sectors participating, healthy"
    elif sectors_above >= 6:
        brd_h = "Decent — majority of sectors on board"
    elif sectors_above >= 4:
        brd_h = "Mixed — only select sectors supporting"
    else:
        brd_h = f"Weak — only {sectors_above}/11 sectors above 20MA"

    return {
        "momentum": mom,
        "vix":      vix_hint,
        "skew":     skew_h,
        "volume":   vol_h,
        "breadth":  brd_h,
    }


# ─── SIGNAL GROUP INTERPRETATIONS ─────────────────────────────────────────────

def _interp_fg(fg_score: float, vix: float, fg_signals: dict) -> dict:
    """Interpret Fear & Greed composite."""
    zone, color, headline = _fg_zone(fg_score)

    # Layered line1 / line2 / line3
    _l1_map = {
        "EXTREME_FEAR":  "EXTREME FEAR — the crowd is panicking",
        "FEAR":          "Market is fearful — elevated risk, elevated opportunity",
        "NEUTRAL":       "Market mood is NEUTRAL — no clear fear or greed",
        "GREED":         "GREED — the crowd is getting comfortable",
        "EXTREME_GREED": "EXTREME GREED — everyone's euphoric",
    }
    _l2_map = {
        "EXTREME_FEAR":  "Historically a buying zone. Don't catch knives — wait for stabilization first.",
        "FEAR":          "Fear creates opportunity. Size small, be patient, wait for confirmation.",
        "NEUTRAL":       "No edge from sentiment alone. Trade the technicals.",
        "GREED":         "Trends continue in greed. Tighten stops — reversals hit fast.",
        "EXTREME_GREED": "Take profits. Tighten stops. Don't chase.",
    }
    line1 = _l1_map.get(zone, headline)
    line2 = _l2_map.get(zone, "")

    # Component breakdown for line3
    components = []
    for name, key in [("VIX Fear", "vix"), ("Breadth", "breadth"), ("Safe Haven", "safe_haven")]:
        val = fg_signals.get(key, {})
        if isinstance(val, dict):
            v = val.get("value")
            if v is not None:
                components.append(f"{name}: {v:.0f}")
    line3 = f"F&G {fg_score:.0f} | " + " | ".join(components) if components else f"F&G {fg_score:.0f}"

    # VIX divergence note
    vix_implied_fg = None
    if vix > 0:
        if vix < 15: vix_implied_fg = 75
        elif vix < 20: vix_implied_fg = 55
        elif vix < 25: vix_implied_fg = 40
        elif vix < 30: vix_implied_fg = 28
        else: vix_implied_fg = 15
    line3_bullets = []
    if vix_implied_fg is not None and abs(fg_score - vix_implied_fg) > 20:
        line3_bullets = [f"⚠ VIX at {vix:.1f} implies F&G closer to {vix_implied_fg} — trust components over composite"]

    return {
        "signal_group": "Fear & Greed",
        "reading": f"{fg_score:.0f} — {zone.replace('_', ' ')}",
        "interpretation": f"F&G {fg_score:.0f} ({zone.replace('_',' ')}) — {line2}",
        "line1": line1, "line2": line2, "line3": line3, "line3_bullets": line3_bullets,
        "color": color,
    }


def _interp_vix(vix: float, state: str, vix_signal: str) -> dict:
    """Interpret VIX level and term structure."""
    vix_zone, color, guidance = _vix_zone(vix)
    ts_color, ts_guidance = _vix_term_zone(state)

    # Blend colors: worse of the two wins
    color_rank = {"green": 0, "yellow": 1, "orange": 2, "red": 3, "dim": 1}
    final_color = color if color_rank.get(color, 0) >= color_rank.get(ts_color, 0) else ts_color

    _l1_map = {
        "CALM":     "Calm seas — market is relaxed",
        "NORMAL":   "Normal conditions — trade your plan",
        "ELEVATED": "Choppy waters ahead — market is nervous",
        "HIGH":     "Storm warning — big moves likely",
        "CRISIS":   "HURRICANE — market in crisis",
    }
    _l2_map = {
        "CALM":     "Good conditions to trade. Cheap to buy protection.",
        "NORMAL":   "Standard volatility. No adjustments needed.",
        "ELEVATED": "Cut position sizes in half. Expect bigger swings.",
        "HIGH":     "Only trade your highest-conviction setups. Small size.",
        "CRISIS":   "Cash is a position. Sit this out unless you're experienced.",
    }
    line1 = _l1_map.get(vix_zone, f"VIX at {vix:.1f}")
    line2 = _l2_map.get(vix_zone, guidance)
    line3 = f"VIX {vix:.1f} | Term: {state.replace('_', ' ')} — {ts_guidance.split('—')[0].strip()}"

    return {
        "signal_group": "VIX & Volatility",
        "reading": f"{vix:.1f} — {state.replace('_', ' ')}",
        "interpretation": f"VIX {vix:.1f} ({vix_zone}) — {guidance}. Term: {state.replace('_',' ')}.",
        "line1": line1, "line2": line2, "line3": line3, "line3_bullets": [],
        "color": final_color,
    }


def _interp_breadth(sectors_above: int, breadth_score: float,
                    spy_pct: float, iwm_pct: float,
                    gld_pct: float, tlt_pct: float) -> dict:
    """Interpret market breadth and safe-haven flows."""
    zone, color, guidance = _breadth_zone(sectors_above, breadth_score)

    _l1_map = {
        "BROAD":  "Broad rally — most of the market is participating",
        "MIXED":  "Mixed signals — the market is selective",
        "NARROW": "Thin ice — rally has no support underneath",
    }
    _l2_map = {
        "BROAD":  "Trust the trend. Buy dips.",
        "MIXED":  "Be picky. Only trade sectors that ARE working.",
        "NARROW": "Don't trust index moves. Most stocks aren't participating.",
    }
    line1 = _l1_map.get(zone, f"{sectors_above}/11 sectors")
    line2 = _l2_map.get(zone, "Trade selectively by sector.")

    # Notable divergences (max 2)
    line3_bullets: list[str] = []
    diff = spy_pct - iwm_pct
    if gld_pct >= 2.0 and spy_pct <= 0.3:
        line3_bullets.append(f"⚠ Gold up {gld_pct:+.2f}% — smart money is hedging")
    elif diff < -0.5 and sectors_above >= 7:
        line3_bullets.append(f"✅ Small caps leading ({iwm_pct:+.2f}% vs SPY {spy_pct:+.2f}%) — broad appetite, healthy")
    elif diff > 1.0:
        line3_bullets.append(f"⚠ Large caps only (SPY {spy_pct:+.2f}% vs IWM {iwm_pct:+.2f}%) — narrow, fragile")
    if tlt_pct >= 1.0 and spy_pct < 0:
        line3_bullets.append(f"⚠ Bonds rallying {tlt_pct:+.2f}% — classic risk-off rotation")
    elif spy_pct < 0 and iwm_pct < 0:
        line3_bullets.append(f"⚠ Both SPY and IWM down — broad risk-off selling")

    spy_str = f"SPY {spy_pct:+.2f}%" if spy_pct != 0 else "SPY —"
    iwm_str = f"IWM {iwm_pct:+.2f}%" if iwm_pct != 0 else "IWM —"
    line3 = f"{sectors_above}/11 sectors above 20MA | {spy_str} {iwm_str}"

    return {
        "signal_group": "Market Breadth",
        "reading": f"{sectors_above}/11 sectors — {zone.replace('_', ' ')}",
        "interpretation": f"{sectors_above}/11 sectors — {guidance}",
        "line1": line1, "line2": line2, "line3": line3, "line3_bullets": line3_bullets[:2],
        "color": color,
    }


def _interp_options(session_type: str, pc_ratio: float, skew: float) -> dict:
    """Interpret options structure: session type, P/C ratio, IV skew."""
    pc_zone, pc_color, pc_guidance = _pc_zone(pc_ratio)

    _l1_map = {
        "TRENDING_BULL": "Bulls in control — momentum is up",
        "TRENDING_BEAR": "Bears in control — selling pressure dominates",
        "CHOP":          "No man's land — market going nowhere",
        "REVERSAL_RISK": "⚠ Watch out — the market could snap either way",
        "VOLATILE":      "High volatility — wild swings in both directions",
    }
    _l2_map = {
        "TRENDING_BULL": "Favor long entries. Let winners run.",
        "TRENDING_BEAR": "Favor shorts or puts. Don't buy the dip yet.",
        "CHOP":          "Reduce size or sit out. Quick trades only.",
        "REVERSAL_RISK": "Don't commit big. Wait for direction to confirm.",
        "VOLATILE":      "Small size, defined risk only. Both sides possible.",
    }
    line1 = _l1_map.get(session_type, f"{session_type.replace('_',' ')} session")
    line2 = _l2_map.get(session_type, "Trade to the current session type.")

    line3_bullets: list[str] = []
    if pc_ratio > 1.5:
        line3_bullets.append(f"• Heavy put buying (P/C {pc_ratio:.2f}) — real fear or hedging. Squeeze possible.")
    elif pc_ratio < 0.7:
        line3_bullets.append(f"• Heavy call buying (P/C {pc_ratio:.2f}) — bullish crowd. Fade risk if overextended.")

    if skew > 5:
        line3_bullets.append(f"• Steep IV skew ({skew:.1f}) — heavy downside hedging in the options market.")
    elif skew < -2:
        line3_bullets.append(f"• Call skew ({skew:.1f}) — options crowd is leaning bullish.")

    line3 = f"Session: {session_type.replace('_',' ')} | P/C: {pc_ratio:.2f} | Skew: {skew:.1f}"

    color_rank = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
    session_color = {
        "TRENDING_BULL": "green", "TRENDING_BEAR": "red",
        "CHOP": "yellow", "REVERSAL_RISK": "orange", "VOLATILE": "orange",
    }.get(session_type, "yellow")
    final_color = pc_color if color_rank.get(pc_color, 1) >= color_rank.get(session_color, 1) else session_color

    return {
        "signal_group": "Options Structure",
        "reading": f"Session: {session_type.replace('_',' ')}, P/C: {pc_ratio:.2f}",
        "interpretation": f"{line1} — {line2}",
        "line1": line1, "line2": line2, "line3": line3, "line3_bullets": line3_bullets[:2],
        "color": final_color,
    }


def _interp_correlations(risk_mode: str, divergences: list[str],
                          alignment_score: float, corr_signal: str) -> dict:
    """Interpret intermarket correlation and alignment."""
    _cfg = {
        "RISK_ON":   ("green",  "RISK ON",   "All signals say GO — stocks, bonds, gold all confirming",      "Full conviction on directional trades."),
        "RISK_OFF":  ("red",    "RISK OFF",  "Flight to safety — money leaving stocks",                      "Defensive mode. Cash or hedges."),
        "DIVERGENT": ("orange", "DIVERGENT", "Markets disagree — no clear direction",                        "Don't over-commit. Wait for alignment."),
        "MIXED":     ("yellow", "MIXED",     "Mixed signals — no clear direction from other markets",        "Don't over-commit. Wait for alignment."),
    }
    color, read_label, line1, line2 = _cfg.get(risk_mode, ("yellow", "MIXED", "Mixed intermarket signals", "Wait for signal alignment."))

    line3_bullets = [divergences[0]] if divergences else []
    line3 = f"Alignment: {alignment_score:.0f} | Mode: {risk_mode.replace('_',' ')}"

    return {
        "signal_group": "Intermarket",
        "reading": f"Alignment: {alignment_score:.0f} — {read_label}",
        "interpretation": line1,
        "line1": line1, "line2": line2, "line3": line3, "line3_bullets": line3_bullets,
        "color": color,
    }


def _interp_news(mood: str, mood_score: float) -> dict | None:
    """Interpret news pulse if available."""
    if not mood or mood == "NEUTRAL":
        return None

    if mood == "BULLISH" and mood_score > 20:
        color, line1 = "green", "News mood: BULLISH — headlines are positive"
        line2 = "News confirming the move. Headlines supporting current bias."
    elif mood == "BEARISH" and mood_score < -20:
        color, line1 = "red", "News mood: BEARISH — headlines are negative"
        line2 = "Headlines adding pressure. Don't fight the tape."
    elif mood == "BULLISH":
        color, line1 = "yellow", "Mildly bullish news — nothing dramatic"
        line2 = "Slight tailwind from headlines. Focus on technicals."
    elif mood == "BEARISH":
        color, line1 = "yellow", "Mildly bearish news — watch for headline risk"
        line2 = "Mild headwind from news. Watch for resolution."
    else:
        color, line1 = "yellow", f"News mood: {mood}"
        line2 = "No strong headline driver. Focus on technicals."

    line3 = f"Mood score: {mood_score:.0f}"

    return {
        "signal_group": "News Mood",
        "reading": f"{mood} ({mood_score:.0f})",
        "interpretation": line1,
        "line1": line1, "line2": line2, "line3": line3, "line3_bullets": [],
        "color": color,
    }


# ─── CROSS-SIGNAL SYNTHESIS ───────────────────────────────────────────────────

_COLOR_RANK = {"green": 0, "yellow": 1, "orange": 2, "red": 3, "dim": 1}


def _synthesize(
    condition: str,
    vix: float,
    vix_state: str,
    fg_score: float,
    sectors_above: int,
    pc_ratio: float,
    gld_pct: float,
    tlt_pct: float,
    risk_mode: str,
    session_type: str,
    event_active: bool,
    news_mood: str,
) -> tuple[str, str, str, list[str], list[str], str]:
    """
    Cross-signal synthesis.
    Returns: (signal, emoji, headline, summary_lines, watch_for, conditions_favor)
    signal: GO / CAUTION / STAND_DOWN
    """
    # ── Scores ──
    fear_score = 0  # higher = more defensive

    if condition == "RED":
        fear_score += 4
    elif condition == "YELLOW":
        fear_score += 2

    if vix >= 30:
        fear_score += 3
    elif vix >= 25:
        fear_score += 2
    elif vix >= 20:
        fear_score += 1

    if vix_state == "BACKWARDATION":
        fear_score += 3
    elif vix_state == "PARTIAL_BACKWARDATION":
        fear_score += 2

    if sectors_above < 4:
        fear_score += 2
    elif sectors_above < 6:
        fear_score += 1

    if gld_pct >= 3.0:
        fear_score += 2
    elif gld_pct >= 1.5:
        fear_score += 1

    if tlt_pct >= 1.5:
        fear_score += 1

    if pc_ratio >= 1.5:
        fear_score += 1

    if risk_mode == "RISK_OFF":
        fear_score += 2
    elif risk_mode == "DIVERGENT":
        fear_score += 1

    bull_score = 0
    if condition == "GREEN":
        bull_score += 3
    if vix < 18:
        bull_score += 2
    if sectors_above >= 8:
        bull_score += 2
    if risk_mode == "RISK_ON":
        bull_score += 2
    if session_type == "TRENDING_BULL":
        bull_score += 2
    if gld_pct < 0.5 and tlt_pct < 0.3:
        bull_score += 1
    if news_mood == "BULLISH":
        bull_score += 1

    # ── Pattern recognition ──
    pattern_name = ""
    headline = ""
    summary_lines = []
    watch_for = []

    # FULL DEFENSIVE
    if fear_score >= 8:
        signal = "STAND_DOWN"
        emoji = "🔴"
        pattern_name = "FULL DEFENSIVE"
        headline = "Capital Preservation Mode — Multiple Fear Signals Converging"
        summary_lines = [
            f"VIX at {vix:.1f} with only {sectors_above}/11 sectors above their 20MA — "
            "breadth is breaking down beneath the index surface.",
            "Safe-haven flows and correlations are all pointing defensive.",
            "This is not a buying opportunity — wait for the dust to settle.",
        ]
        watch_for = [
            f"IF VIX drops below {max(20, vix*0.85):.0f} → first sign of stabilization",
            f"IF 6+ sectors clear their 20MA → breadth is healing, re-evaluate",
            "IF P/C ratio drops below 1.2 → hedges coming off, squeeze possible",
            "IF RED condition flips to YELLOW → first regime change signal",
        ]

    elif fear_score >= 5:
        signal = "CAUTION"
        emoji = "🟡"

        # Check washout potential
        if fg_score < 35 and sectors_above < 4:
            pattern_name = "WASHOUT POTENTIAL"
            headline = "Fear Is Extreme — Watch for Capitulation Bottom"
            summary_lines = [
                f"F&G at {fg_score:.0f} (extreme fear) with narrow breadth often marks "
                "where bottoms form historically.",
                "This is not a signal to load up — start watching for stabilization clues.",
                "Begin with small pilot positions only when VIX starts reversing.",
            ]
            watch_for = [
                "IF VIX spikes then reverses → classic capitulation signal, start pilot long",
                "IF P/C ratio hits extreme then drops sharply → squeeze setup incoming",
                "IF multiple sectors bounce together → panic-bottom reversal in progress",
                "IF strong close on heavy volume → buying exhaustion has cleared",
            ]
        else:
            pattern_name = "RISK REDUCTION"
            headline = "Defensive Posture — Conditions Favor Reduced Exposure"
            summary_lines = [
                f"Only {sectors_above}/11 sectors above 20MA with elevated volatility — "
                "more risk than reward right now.",
                "Multiple signals pointing to caution — this is not the time to press.",
                "Reduce sizes, tighten stops, wait for clarity.",
            ]
            watch_for = [
                f"IF VIX drops below {max(18, vix*0.9):.0f} → vol regime improving, cautious re-entry",
                "IF 6+ sectors clear their 20MA → breadth healing, increase exposure",
                "IF GREEN condition restored → Troi gives all-clear to trade normally",
                "IF P/C ratio drops below 1.0 → fear fading, hedges coming off",
            ]

    elif bull_score >= 7:
        signal = "GO"
        emoji = "🟢"
        pattern_name = "FULL RISK-ON"
        headline = "All Systems Nominal — Conditions Favor Active Trading"
        summary_lines = [
            f"{sectors_above}/11 sectors participating with VIX at {vix:.1f} — "
            "broad, healthy tape.",
            "Intermarket signals aligned. Momentum and breadth both confirming.",
            "Press your winners, add to trending positions with confidence.",
        ]
        watch_for = [
            f"IF VIX spikes above {vix + 5:.0f} → watch for volatility regime shift, cut size",
            "IF sectors drop below 7 → participation deteriorating, tighten stops",
            "IF P/C ratio spikes above 1.2 → smart money hedging, reduce exposure",
            "IF F&G crosses 80 → extreme greed, take partial profits",
        ]

    else:
        signal = "CAUTION"
        emoji = "🟡"
        pattern_name = "MIXED SIGNALS"
        headline = "Indecision — Market at a Crossroads"
        summary_lines = [
            "Signals are not aligned in a clear direction.",
            "Don't force trades — wait for resolution before committing.",
            "When the signals agree, act decisively. Until then, stay patient.",
        ]
        watch_for = [
            "IF condition shifts to GREEN → buy side confirmed, increase exposure",
            f"IF 7+ sectors clear 20MA → trend emerging, lean long",
            f"IF sectors drop below 4 → defensive signal, cut exposure",
            "IF session type shifts from CHOP → trend starting, follow it",
        ]

    # Event shield override
    event_prefix = ""
    if event_active:
        event_prefix = "⚠️ SCHEDULED EVENT ACTIVE — reduce exposure regardless of other signals. "
        signal = "CAUTION"  # Always at least CAUTION during events

    if event_prefix and headline:
        headline = event_prefix + headline
    elif event_prefix:
        headline = event_prefix

    # Conditions favor (for scanner bar)
    if signal == "GO" and session_type in ("TRENDING_BULL",):
        conditions_favor = "momentum long setups"
    elif signal == "GO" and session_type == "TRENDING_BEAR":
        conditions_favor = "short momentum and put spreads"
    elif signal == "STAND_DOWN":
        conditions_favor = "sitting out — conditions unfavorable"
    elif pattern_name == "WASHOUT POTENTIAL":
        conditions_favor = "mean reversion / oversold bounces with tight stops"
    elif session_type in ("REVERSAL_RISK", "VOLATILE"):
        conditions_favor = "quick scalps both ways — no overnight holds"
    else:
        conditions_favor = "selective entries with confirmation only"

    return signal, emoji, headline, summary_lines, watch_for, conditions_favor


# ─── ACTION PLAN BUILDER ──────────────────────────────────────────────────────

def _build_action_plan(
    signal: str,
    session_type: str,
    vix: float,
    sectors_above: int,
    gld_pct: float,
    put_wall: float,
    call_wall: float,
    fg_score: float,
) -> dict:
    """Build the structured action plan."""
    details = []

    if signal == "STAND_DOWN":
        primary = "🔴 DEFENSE MODE — sit tight, protect capital"
        details = [
            f"→ Cut size to 25% or less until VIX drops below {max(20, vix*0.85):.0f}",
            "→ If already in positions, tighten stops to limit damage",
        ]
        if put_wall > 0:
            details.append(f"→ SPY put wall at ${put_wall:.0f} is your floor — if it breaks, step aside entirely")
        elif gld_pct > 2:
            details.append(f"→ Gold up {gld_pct:.2f}% — smart money is hedging, not a buy-the-dip moment")

    elif signal == "CAUTION":
        primary = "🟡 SELECTIVE — only A+ setups, half size"
        details = [
            "→ Reduce standard position size by 50% — not a full-conviction environment",
            "→ Only take setups that fit the current session type exactly",
        ]
        if sectors_above < 6:
            details.append(f"→ Only {sectors_above}/11 sectors above 20MA — stick to sectors that ARE working")
        elif call_wall > 0:
            details.append(f"→ Call wall at ${call_wall:.0f} is resistance — size down approaching that level")
        elif fg_score < 35:
            details.append(f"→ F&G at {fg_score:.0f} (fear zone) — wait for stabilization before adding")
        elif fg_score > 70:
            details.append(f"→ F&G at {fg_score:.0f} (greed zone) — lock in gains if approaching targets")
        else:
            details.append("→ Avoid overnight holds until conditions improve")

    else:  # GO
        primary = "🟢 GREEN LIGHT — trade your plan with full conviction"
        details = [
            f"→ {sectors_above}/11 sectors above 20MA — broad participation, trust the trend",
            "→ Standard or slightly elevated position sizes — market is rewarding participation",
        ]
        if session_type == "TRENDING_BULL":
            details.append("→ Trending Bull session — buy dips, let winners run, use trailing stops")
        elif call_wall > 0:
            details.append(f"→ Call wall at ${call_wall:.0f} — watch for pinning behavior near that level")
        elif fg_score > 70:
            details.append(f"→ F&G at {fg_score:.0f} (greed) — consider partial profits on biggest winners")
        else:
            details.append("→ Trend-following strategies have the edge in this environment")

    return {"primary": primary, "details": details[:3], "signal": signal}


# ─── MAIN ADVISORY FUNCTION ───────────────────────────────────────────────────

def generate_advisory(force: bool = False) -> dict:
    """
    Generate the full dynamic advisory. Cached for 5 minutes.
    Returns a structured dict with market_read, what_it_means,
    action_plan, watch_for, gauge_hints, conditions_favor.
    """
    global _cache, _cache_ts
    now = time.time()
    if not force and _cache and (now - _cache_ts) < _CACHE_TTL:
        return _cache

    try:
        raw = _gather()

        # ── Extract values with safe defaults ──
        cond       = raw.get("condition", {})
        vix_d      = raw.get("vix", {})
        fg         = raw.get("fear_greed", {})
        brd        = raw.get("breadth", {})
        corr       = raw.get("correlations", {})
        ev         = raw.get("events", {})
        news       = raw.get("news", {})

        condition      = cond.get("condition", "UNKNOWN")
        cond_score     = float(cond.get("condition_score") or 50)
        session_type   = cond.get("session_type", "UNKNOWN")
        trend_score    = float(cond.get("trend_score") or 0)
        spy_price      = float(cond.get("spy_price") or 0)
        put_wall       = float(cond.get("put_wall") or 0)
        call_wall      = float(cond.get("call_wall") or 0)
        gamma_flip     = float(cond.get("gamma_flip") or 0)
        max_pain       = float(cond.get("max_pain") or 0)
        pc_ratio       = float(cond.get("pc_ratio") or 1.0)
        skew_val       = float(cond.get("skew_value") or 0)
        buy_pct        = float(cond.get("buy_pct") or 50)

        vix            = float(vix_d.get("vix") or 20)
        vix_state      = vix_d.get("state", "UNKNOWN")

        fg_score       = float(fg.get("score") or 50)
        fg_signals     = fg.get("signals", {})

        sectors_above  = int(brd.get("sectors_above_20ma") or 5)
        breadth_score  = float(brd.get("breadth_score") or 0)
        breadth_etfs   = brd.get("breadth_etfs", {})
        spy_pct        = float((breadth_etfs.get("SPY") or {}).get("pct_change") or 0)
        iwm_pct        = float((breadth_etfs.get("IWM") or {}).get("pct_change") or 0)

        gld_pct        = float(corr.get("gld_pct") or 0)
        tlt_pct        = float(corr.get("tlt_pct") or 0)
        risk_mode      = corr.get("risk_mode", "MIXED")
        divergences    = corr.get("divergences", [])
        align_score    = float(corr.get("alignment_score") or 0)
        corr_signal    = corr.get("signal", "")

        event_active   = not ev.get("all_clear", True)
        news_mood      = news.get("mood", "NEUTRAL")
        news_score     = float(news.get("mood_score") or 0)

        # VIX sanity — same rule as kirk_advisory
        if vix < 5 or vix > 90:
            vix = 20.0

        # ── Signal interpretations ──
        what_it_means = []

        fg_interp = _interp_fg(fg_score, vix, fg_signals)
        what_it_means.append(fg_interp)

        vix_interp = _interp_vix(vix, vix_state, vix_d.get("signal", ""))
        what_it_means.append(vix_interp)

        brd_interp = _interp_breadth(sectors_above, breadth_score,
                                      spy_pct, iwm_pct, gld_pct, tlt_pct)
        what_it_means.append(brd_interp)

        opt_interp = _interp_options(session_type, pc_ratio, skew_val)
        what_it_means.append(opt_interp)

        corr_interp = _interp_correlations(risk_mode, divergences, align_score, corr_signal)
        what_it_means.append(corr_interp)

        news_interp = _interp_news(news_mood, news_score)
        if news_interp:
            what_it_means.append(news_interp)

        # ── Confidence: how many signals agree ──
        colors = [x["color"] for x in what_it_means]
        green_count = colors.count("green")
        red_count = colors.count("red") + colors.count("orange")
        total = len(colors)
        dominant_faction = max(green_count, red_count)
        confidence = int(50 + (dominant_faction / total) * 50) if total > 0 else 50

        # ── Cross-signal synthesis ──
        signal, emoji, headline, summary_lines, watch_for, conditions_favor = _synthesize(
            condition, vix, vix_state, fg_score, sectors_above,
            pc_ratio, gld_pct, tlt_pct, risk_mode, session_type,
            event_active, news_mood,
        )

        # ── Action plan ──
        action_plan = _build_action_plan(
            signal, session_type, vix, sectors_above,
            gld_pct, put_wall, call_wall, fg_score,
        )
        action_plan["emoji"] = emoji

        # ── Market read ──
        summary = " ".join(summary_lines)
        # Add a key level line if available
        if put_wall > 0 or call_wall > 0:
            levels = []
            if call_wall > 0:
                levels.append(f"call wall ${call_wall:.0f}")
            if put_wall > 0:
                levels.append(f"put wall ${put_wall:.0f}")
            if gamma_flip > 0:
                levels.append(f"gamma flip ${gamma_flip:.0f}")
            if levels:
                summary += f" Key levels: {', '.join(levels)}."

        market_read = {
            "headline":   headline,
            "summary":    summary,
            "confidence": confidence,
            "signal":     signal,
            "emoji":      emoji,
        }

        # ── Gauge hints ──
        gauge_hints = _build_gauge_hints(
            trend_score, vix, vix_state, skew_val, buy_pct, sectors_above
        )

        result = {
            "market_read":       market_read,
            "what_it_means":     what_it_means,
            "action_plan":       action_plan,
            "watch_for":         watch_for,
            "gauge_hints":       gauge_hints,
            "conditions_favor":  conditions_favor,
            "inputs": {
                "condition":       condition,
                "condition_score": round(cond_score, 1),
                "session_type":    session_type,
                "vix":             round(vix, 1),
                "vix_state":       vix_state,
                "fg_score":        round(fg_score, 1),
                "sectors_above":   sectors_above,
                "breadth_score":   round(breadth_score, 1),
                "pc_ratio":        round(pc_ratio, 2),
                "risk_mode":       risk_mode,
                "gld_pct":         round(gld_pct, 2),
                "tlt_pct":         round(tlt_pct, 2),
                "news_mood":       news_mood,
                "event_active":    event_active,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        _cache = result
        _cache_ts = now
        return result

    except Exception as e:
        logger.error("generate_advisory failed: %s", e)
        return {
            "market_read": {
                "headline":   "Advisory temporarily unavailable",
                "summary":    str(e),
                "confidence": 0,
                "signal":     "CAUTION",
                "emoji":      "🟡",
            },
            "what_it_means":    [],
            "action_plan":      {"primary": "Data unavailable", "details": [], "signal": "CAUTION", "emoji": "🟡"},
            "watch_for":        [],
            "gauge_hints":      {},
            "conditions_favor": "data unavailable",
            "inputs":           {},
            "generated_at":     datetime.now(timezone.utc).isoformat(),
            "error":            str(e),
        }
