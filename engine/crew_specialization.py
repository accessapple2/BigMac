"""
USS TradeMinds — Crew Specialization Protocol
==============================================
Each agent has a unique strategy mandate that determines WHAT they trade,
WHEN they trade, and HOW they size positions.

player_id keys match the ai_players.id column in trader.db.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CREW_MANIFEST
# ---------------------------------------------------------------------------
# Each entry:
#   tier        : 1 | 2 | 3 | "standalone" | "special"
#   display_name: human-readable
#   role        : short role subtitle shown on leaderboard
#   strategy    : one-line mandate
#   model       : preferred Ollama model (for mandate evaluation)
#   max_positions: int
#   size_factor : float — multiplied against Troi confidence multiplier
#   conditions  : dict of gate conditions (checked by should_agent_trade)
#   universe    : optional list[str] — if set, agent may ONLY trade these symbols
#   bridge_voter: bool — Tier 3 agents vote but don't trade individually

# ── Lean Fleet Protocol — 12 active agents ─────────────────────────────────
# ALPHA_SQUAD: scan every 15 min, rotate in pairs to cap RAM at 2 models loaded
#   Pair 1 (llama3.1+qwen3):       Uhura (ollama-llama) [LEAD] + Seven (gemini-2.5-pro)
#   Pair 2 (qwen3:14b+deepseek):   Worf (gemini-2.5-flash)    + Spock (grok-4)
#   McCoy (ollama-plutus) solo: crisis doctor, low-alpha fill
# T'Pol (dayblade-0dte) SHELVED: 0DTE with execution delay = guaranteed loss
# Sulu (dayblade-sulu) SHELVED: Sniper Go Live — momentum too noisy, bridge vote only
ALPHA_SQUAD: list[str] = [
    "ollama-llama",     "gemini-2.5-pro",    # Pair 1 — Uhura (Alpha Lead) + Seven
    "gemini-2.5-flash", "grok-4",            # Pair 2 — Worf + Spock
    "ollama-plutus",                         # McCoy solo (Sulu shelved)
]

SCAN_PAIRS: list[list[str]] = [
    ["ollama-llama",     "gemini-2.5-pro"],  # Pair 1: Uhura (Alpha Lead) + Seven
    ["gemini-2.5-flash", "grok-4"],          # Pair 2: Worf + Spock
    ["ollama-plutus",    "ollama-plutus"],   # McCoy solo (Sulu shelved — deduped in get_alpha_pair)
]

# Advisory crew — bridge vote only, no individual scanning
ADVISORY_CREW: list[str] = [
    "energy-arnold",   # Trip Tucker
    "options-sosnoff", # Troi
    "ollama-coder",    # Data
    "mlx-qwen3",       # Ensign Ro
    "ollama-local",    # Geordi
    "gpt-4o",          # Janeway
    "claude-haiku",    # Reed
    "claude-sonnet",   # Sisko
    "gpt-o3",          # Tuvok
    "grok-3",          # Hoshi
    "ollama-kimi",     # Bashir
    "ollama-qwen3",    # Dax
    "ollama-glm4",     # Q
    "ollama-deepseek", # Odo
    "ollama-gemma27b", # Dax (duplicate)
    "dayblade-0dte",   # T'Pol — shelved (0DTE execution delay = guaranteed loss)
    "dayblade-sulu",   # Sulu — shelved (Sniper Go Live: momentum too noisy)
    "super-agent",     # Anderson — shelved (Sniper Go Live: unrestricted aggression conflicts with alpha gate)
    "dalio-metals",    # Dalio — shelved (Sniper Go Live: metals macro not in Sniper whitelist)
    "capitol-trades",  # Capitol Trades — shelved (Sniper Go Live: congress data latency too high)
]

CREW_MANIFEST: dict[str, dict[str, Any]] = {

    # ── Tier 1 ── Active Core Officers (Alpha Squad) ──────────────────────

    "grok-4": {
        "tier": 1,
        "display_name": "Lt. Cmdr. Spock",
        "role": "Mean Reversion",
        "strategy": "RSI extremes only — buy oversold, short overbought. Avoids trending sessions.",
        "model": "deepseek-r1:14b",  # upgraded from 7b
        "max_positions": 2,
        "size_factor": 0.8,
        "bridge_voter": False,
        "conditions": {
            "blocked_session_types": ["TRENDING_BULL", "TRENDING_BEAR"],
            "required_rsi_extreme": True,   # rsi < 30 or rsi > 70
        },
        "universe": None,
    },

    "dayblade-sulu": {
        "tier": "advisory",
        "display_name": "Lt. Sulu",
        "role": "Momentum Pilot [ADVISORY]",
        "strategy": "Trend-following only. SHELVED — Sniper Go Live: momentum too noisy. Bridge vote only.",
        "model": "phi4:14b",  # upgraded from gemma3:4b — math-heavy momentum
        "max_positions": 3,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {
            "required_session_types": ["TRENDING_BULL", "TRENDING_BEAR"],
            "min_momentum_score": 20,
        },
        "universe": None,
    },

    "energy-arnold": {
        "tier": "advisory",
        "display_name": "Cmdr. Trip Tucker",
        "role": "Contrarian [ADVISORY]",
        "strategy": "Fades crowd extremes. P/C >1.5 = buy, <0.6 = short. F&G extremes.",
        "model": "qwen3.5:9b",
        "max_positions": 2,
        "size_factor": 0.6,
        "bridge_voter": True,
        "conditions": {
            "pc_ratio_min_buy": 1.5,    # p/c > 1.5 → buy signal
            "pc_ratio_max_short": 0.6,  # p/c < 0.6 → short signal
            "pc_ratio_dead_zone": (0.9, 1.1),  # stand down when p/c in this range
            "fg_buy_below": 25,
            "fg_short_above": 75,
        },
        "universe": None,
    },

    "gemini-2.5-flash": {
        "tier": 1,
        "display_name": "Lt. Cmdr. Worf",
        "role": "Bear Specialist",
        "strategy": "Bearish positions only — shorts, inverse ETFs. Stands down in confirmed bulls.",
        "model": "qwen3:14b",  # upgraded from qwen3.5:9b
        "max_positions": 2,
        "size_factor": 0.7,
        "bridge_voter": False,
        "conditions": {
            "bearish_only": True,
            "blocked_session_types": ["TRENDING_BULL"],
            "min_vix_for_entry": 16,
            "max_breadth_for_entry": 55,  # breadth_score < 55% to enter
        },
        "universe": ["SH", "SQQQ", "UVXY", "SPXS", "PSQ", "DOG", "RWM", "SDOW"],
    },

    "options-sosnoff": {
        "tier": "advisory",
        "display_name": "Counselor Troi",
        "role": "Sentiment Reader [ADVISORY]",
        "strategy": "Sentiment divergences between news_pulse, F&G, and options structure.",
        "model": "qwen3.5:9b",
        "max_positions": 2,
        "size_factor": 1.0,   # 1.0x on divergence, 0.5x single-signal (applied in logic)
        "bridge_voter": True,
        "conditions": {
            "fg_buy_below": 20,
            "fg_short_above": 80,
            "divergence_bonus": True,  # 1.0x size on divergence vs 0.5x single
        },
        "universe": None,
    },

    "ollama-coder": {
        "tier": "advisory",
        "display_name": "Lt. Cmdr. Data",
        "role": "Pure Quant [ADVISORY]",
        "strategy": "Deep Scan signal_strength ≥ 0.6 only. No sentiment. Pure numbers.",
        "model": "qwen2.5-coder:7b",
        "max_positions": 4,
        "size_factor": 0.9,
        "bridge_voter": False,
        "conditions": {
            "min_deep_scan_strength": 0.6,
            "no_sentiment": True,   # advisory: ignore news/F&G inputs
        },
        "universe": None,
    },

    # ── Tier 2 ── Specialists ──────────────────────────────────────────────

    "mlx-qwen3": {
        "tier": "advisory",
        "display_name": "Ensign Ro",
        "role": "Breakout Hunter [ADVISORY]",
        "strategy": "20-day highs on 2x+ volume in trending sessions only. Max 2-day hold.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 0.9,
        "bridge_voter": True,
        "conditions": {
            "required_session_types": ["TRENDING_BULL"],
            "breakout_only": True,
        },
        "universe": None,
    },

    "ollama-local": {
        "tier": "advisory",
        "display_name": "Lt. Cmdr. Geordi",
        "role": "Sector Rotation [ADVISORY]",
        "strategy": "Buy leading sector ETF, short lagging. Rotates weekly via sector_heatmap.",
        "model": "gemma3:4b",
        "max_positions": 2,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {
            "sector_etfs_only": True,
        },
        "universe": ["XLK", "XLF", "XLV", "XLE", "XLI", "XLC", "XLY", "XLP", "XLB", "XLRE", "XLU"],
    },

    "ollama-plutus": {
        "tier": 2,
        "display_name": "Dr. McCoy",
        "role": "Crisis Doctor",
        "strategy": "Buys the blood. Active when VIX>18, F&G<55, breadth<50%.",
        "model": "0xroyce/plutus:latest",
        "max_positions": 2,
        "size_factor": 1.2,
        "bridge_voter": False,
        "conditions": {
            "min_vix_for_entry": 15,
            "max_fg_for_entry": 55,
            "max_breadth_for_entry": 50,
            "hold_until_vix_below": 12,
            "completely_inactive_vix_below": 12,
        },
        "universe": ["TQQQ", "NVDA", "AMD", "META", "SPY", "QQQ", "IWM"],
    },

    "ollama-qwen3": {
        "tier": "advisory",
        "display_name": "Lt. Jadzia Dax",
        "role": "Swing Breakout [ADVISORY]",
        "strategy": "Breakout above 20MA on 1.5x+ volume, or volume momentum spikes. Sits out bear sessions.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 0.9,
        "bridge_voter": False,
        "conditions": {
            "blocked_session_types": ["TRENDING_BEAR"],
        },
        "universe": None,
    },

    "ollama-llama": {
        "tier": 1,
        "display_name": "Lt. Cmdr. Uhura",
        "role": "Options Flow",
        "strategy": "Trades when 2+ options flow signals align. Local llama3.3:8b.",
        "model": "llama3.1:latest",
        "max_positions": 2,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {
            "min_flow_signals": 2,
        },
        "universe": None,
    },

    # ── Tier 3 ── Bridge Vote (no individual trading) ──────────────────────

    "claude-haiku": {
        "tier": "advisory",
        "display_name": "Lt. Malcolm Reed",
        "role": "Tactical / Defense [ADVISORY]",
        "strategy": "Defensive bridge voter. Risk-aware, cautious posture. No individual trades.",
        "model": "qwen3.5:9b",  # used for local vote simulation fallback
        "max_positions": 0,
        "size_factor": 0.0,
        "bridge_voter": True,
        "conditions": {},
        "universe": None,
    },

    "claude-sonnet": {
        "tier": "advisory",
        "display_name": "Captain Sisko",
        "role": "Decisive Strategist [ADVISORY]",
        "strategy": "Big-picture decisive bridge voter. Commands clarity under pressure. No individual trades.",
        "model": "qwen3.5:9b",
        "max_positions": 0,
        "size_factor": 0.0,
        "bridge_voter": True,
        "conditions": {},
        "universe": None,
    },

    "gemini-2.5-pro": {
        "tier": 1,
        "display_name": "Seven of Nine",
        "role": "Pure Data Analysis",
        "strategy": "Pure quant analyst. Unemotional data-driven signals. Trades based on highest signal_strength from Deep Scan.",
        "model": "qwen3:14b",
        "max_positions": 2,
        "size_factor": 0.8,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },

    "gpt-4o": {
        "tier": "advisory",
        "display_name": "Captain Janeway",
        "role": "All-Conditions Trader [ADVISORY]",
        "strategy": "Trades in ANY market condition. Resourceful adapter — finds the path regardless of session.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 1.0,
        "bridge_voter": False,
        "unrestricted": True,  # only Event Shield CRITICAL and Troi STAND_DOWN can stop her
        "conditions": {},
        "universe": None,
    },

    "gpt-o3": {
        "tier": "advisory",
        "display_name": "Lt. Tuvok",
        "role": "Vulcan Risk Assessment [ADVISORY]",
        "strategy": "Logical risk-weighted bridge voter. Pure Vulcan reasoning, no emotion. No individual trades.",
        "model": "deepseek-r1:7b",  # reasoning model analog
        "max_positions": 0,
        "size_factor": 0.0,
        "bridge_voter": True,
        "conditions": {},
        "universe": None,
    },

    "grok-3": {
        "tier": "advisory",
        "display_name": "Ensign Hoshi",
        "role": "Signal Interceptor [ADVISORY]",
        "strategy": "Comms and flow analysis bridge voter. Reads between the signals. No individual trades.",
        "model": "qwen3:14b",
        "max_positions": 0,
        "size_factor": 0.0,
        "bridge_voter": True,
        "conditions": {},
        "universe": None,
    },

    "ollama-glm4": {
        "tier": "advisory",
        "display_name": "Q",
        "role": "Wildcard [ADVISORY]",
        "strategy": "Omnipotent wildcard. Ignores most gates. Trades anything, anytime, any condition.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 1.2,
        "bridge_voter": False,
        "unrestricted": True,  # only Event Shield CRITICAL and Troi STAND_DOWN can stop Q
        "conditions": {},
        "universe": None,
    },

    "ollama-kimi": {
        "tier": "advisory",
        "display_name": "Dr. Bashir",
        "role": "Sharp Diagnostics [ADVISORY]",
        "strategy": "Genetically enhanced analytical bridge voter. Sharp pattern recognition. No individual trades.",
        "model": "qwen3.5:9b",
        "max_positions": 0,
        "size_factor": 0.0,
        "bridge_voter": True,
        "conditions": {},
        "universe": None,
    },

    # ── Standalones ────────────────────────────────────────────────────────

    "ollama-deepseek": {
        "tier": "advisory",
        "display_name": "Constable Odo",
        "role": "Contrarian / Deception Detector [ADVISORY]",
        "strategy": "Sees through market deception. Contrarian plays when consensus is too comfortable.",
        "model": "deepseek-r1:7b",
        "max_positions": 2,
        "size_factor": 0.7,
        "bridge_voter": True,
        "conditions": {
            "signal_divergence_required": True,
        },
        "universe": None,
    },

    "ollama-gemma27b": {
        "tier": "advisory",
        "display_name": "Lt. Jadzia Dax",
        "role": "Patient Swing Trader [ADVISORY]",
        "strategy": "300 years of experience. Patient swing setups. Pullback to 20-day MA in uptrend.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 0.9,
        "bridge_voter": False,
        "conditions": {
            "swing_only": True,         # 2-5 day hold target
            "pullback_required": True,  # MA pullback entry
        },
        "universe": None,
    },

    # ── Special Agents (unchanged behavior) ────────────────────────────────

    "super-agent": {
        "tier": "advisory",
        "display_name": "Mr. Anderson",
        "role": "Neo of the Fleet — Aggressive Momentum Predator [ADVISORY]",
        "strategy": "SHELVED — Sniper Go Live: unrestricted aggression conflicts with alpha gate 0.3. Bridge vote only.",
        "model": "qwen3.5:9b",
        "max_positions": 5,
        "size_factor": 2.0,
        "bridge_voter": False,
        "unrestricted": True,
        "min_confidence": 0.45,
        "focus": "small_cap_momentum",
        "signal_sources": ["momentum_breakout", "gap_and_go", "channel_scanner"],
        "conditions": {},
        "universe": None,
    },

    "neo-matrix": {
        "tier": "special",
        "display_name": "Neo",
        "role": "Matrix Agent",
        "strategy": "Independent — no mandate restrictions.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 1.0,
        "bridge_voter": False,
        "unrestricted": True,  # only Event Shield CRITICAL and Troi STAND_DOWN can stop him
        "conditions": {},
        "universe": None,
    },

    "dalio-metals": {
        "tier": "advisory",
        "display_name": "Mr. Dalio",
        "role": "Metals / Macro [ADVISORY]",
        "strategy": "Gold, silver, macro hedge. SHELVED — Sniper Go Live: metals not in strategy whitelist. Bridge vote only.",
        "model": "qwen3.5:9b",
        "max_positions": 4,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },

    "capitol-trades": {
        "tier": "advisory",
        "display_name": "Capitol Trades",
        "role": "Congress Tracker [ADVISORY]",
        "strategy": "Follows congressional trade disclosures. SHELVED — Sniper Go Live: congress data latency too high for alpha gate. Bridge vote only.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },

    "navigator": {
        "tier": "special",
        "display_name": "Ensign Chekov",
        "role": "Navigator",
        "strategy": "Navigates course corrections. No mandate restrictions.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },

    "enterprise-computer": {
        "tier": "special",
        "display_name": "Dilithium Reserve",
        "role": "Reserve Fund",
        "strategy": "Capital reserve and rebalancing. No mandate restrictions.",
        "model": "qwen3.5:9b",
        "max_positions": 2,
        "size_factor": 0.5,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },

    "steve-webull": {
        "tier": "special",
        "display_name": "Captain Kirk",
        "role": "Captain",
        "strategy": "Captain's discretion. No mandate restrictions.",
        "model": "qwen3:14b",
        "max_positions": 10,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },

    "dayblade-0dte": {
        "tier": "advisory",        # SHELVED: 0DTE with execution delay = guaranteed loss
        "display_name": "T'Pol",
        "role": "Vulcan Precision Options [ADVISORY]",
        "strategy": "Vulcan precision 0DTE options specialist. Logic-driven, no emotional holds.",
        "model": "qwen3.5:9b",
        "max_positions": 3,
        "size_factor": 1.0,
        "bridge_voter": True,      # still votes on bridge
        "conditions": {},
        "universe": None,
    },

    "cto-grok42": {
        "tier": "special",
        "display_name": "CTO Grok 4.2",
        "role": "CTO Advisor",
        "strategy": "Strategic technology sector advisor. No mandate restrictions.",
        "model": "qwen3:14b",
        "max_positions": 3,
        "size_factor": 1.0,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },

    "red-alert": {
        "tier": "special",
        "display_name": "Red Alert System",
        "role": "Risk Monitor",
        "strategy": "Automated risk system. No mandate restrictions.",
        "model": "qwen3.5:9b",
        "max_positions": 0,
        "size_factor": 0.0,
        "bridge_voter": False,
        "conditions": {},
        "universe": None,
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_agent_mandate(player_id: str) -> dict | None:
    """Return mandate dict for a player, or None if not found."""
    return CREW_MANIFEST.get(player_id)


def is_bridge_voter(player_id: str) -> bool:
    """True if this agent is a Tier 3 bridge voter (no individual trading)."""
    m = CREW_MANIFEST.get(player_id)
    return bool(m and m.get("bridge_voter"))


def should_agent_trade(player_id: str, market_data: dict) -> tuple[bool, str]:
    """
    Gate check: should this agent make a trade given current market conditions?

    Args:
        player_id  : DB player id (e.g. "grok-4")
        market_data: dict with any of:
            session_type    : str  (TRENDING_BULL | TRENDING_BEAR | CHOP | REVERSAL_RISK | VOLATILE)
            vix             : float
            pc_ratio        : float
            fg_score        : int   (Fear & Greed 0-100)
            breadth_score   : float (0-100, % advancing)
            momentum_score  : float (trend_score from momentum_tracker)
            deep_scan_top   : list[dict] with 'signal_strength' key
            spy_day_return  : float (SPY % change today, e.g. -2.5)

    Returns:
        (allowed: bool, reason: str)
    """
    mandate = CREW_MANIFEST.get(player_id)
    if not mandate:
        return True, "No mandate on file — unrestricted."

    if mandate.get("bridge_voter"):
        return False, f"{mandate['display_name']} is a Bridge Voter — votes only, no individual trades."

    tier = mandate.get("tier")
    if tier == "advisory":
        return False, f"{mandate['display_name']} is Advisory — bridge vote only, not scanning."
    if tier == "special":
        return True, f"{mandate['display_name']} — special agent, unrestricted."

    cond = mandate.get("conditions", {})
    session = (market_data.get("session_type") or "").upper()
    vix = float(market_data.get("vix") or 0)
    pc = float(market_data.get("pc_ratio") or 1.0)
    fg = market_data.get("fg_score")
    breadth = market_data.get("breadth_score")
    momentum = float(market_data.get("momentum_score") or 0)
    spy_ret = float(market_data.get("spy_day_return") or 0)

    # Blocked session types
    blocked = cond.get("blocked_session_types", [])
    if session and blocked and session in [s.upper() for s in blocked]:
        return False, (
            f"{mandate['display_name']} stands down during {session} sessions "
            f"(mandate: {mandate['strategy']})"
        )

    # Required session types
    required_sessions = cond.get("required_session_types", [])
    if required_sessions and session:
        if session not in [s.upper() for s in required_sessions]:
            return False, (
                f"{mandate['display_name']} only trades in {required_sessions} "
                f"— current session: {session or 'unknown'}"
            )

    # Min momentum score
    min_mom = cond.get("min_momentum_score")
    if min_mom is not None and abs(momentum) < min_mom:
        return False, (
            f"{mandate['display_name']}: momentum_score {momentum:.1f} below "
            f"threshold {min_mom} (mandate: momentum trading)"
        )

    # Contrarian: P/C dead zone
    dead_lo, dead_hi = cond.get("pc_ratio_dead_zone", (None, None))
    if dead_lo is not None and dead_hi is not None:
        if dead_lo <= pc <= dead_hi:
            return False, (
                f"{mandate['display_name']} stands down — P/C ratio {pc:.2f} "
                f"in neutral zone ({dead_lo}–{dead_hi})"
            )
        # Need at least one trigger to be actionable
        pc_buy_min = cond.get("pc_ratio_min_buy", 999)
        pc_short_max = cond.get("pc_ratio_max_short", -999)
        fg_buy = cond.get("fg_buy_below", -999)
        fg_short = cond.get("fg_short_above", 999)
        has_pc_signal = pc > pc_buy_min or pc < pc_short_max
        has_fg_signal = (fg is not None) and (fg < fg_buy or fg > fg_short)
        if not has_pc_signal and not has_fg_signal:
            return False, (
                f"{mandate['display_name']}: no contrarian trigger — "
                f"P/C {pc:.2f}, F&G {fg}"
            )

    # Worf: bearish only — must have a bearish signal
    if cond.get("bearish_only"):
        min_vix = cond.get("min_vix_for_entry", 0)
        max_breadth = cond.get("max_breadth_for_entry", 100)
        if vix < min_vix:
            return False, (
                f"{mandate['display_name']}: VIX {vix:.1f} below entry threshold {min_vix}"
            )
        if breadth is not None and breadth > max_breadth:
            return False, (
                f"{mandate['display_name']}: breadth {breadth:.0f}% too strong "
                f"for bear trades (max {max_breadth}%)"
            )

    # McCoy: completely inactive below VIX threshold
    inactive_below = cond.get("completely_inactive_vix_below")
    if inactive_below is not None and vix > 0 and vix < inactive_below:
        return False, (
            f"{mandate['display_name']} completely inactive — "
            f"VIX {vix:.1f} < {inactive_below} (crisis doctor needs fear)"
        )
    min_vix = cond.get("min_vix_for_entry")
    if min_vix and vix > 0 and vix < min_vix:
        return False, f"{mandate['display_name']}: VIX {vix:.1f} below entry threshold {min_vix}"

    max_fg = cond.get("max_fg_for_entry")
    if max_fg is not None and fg is not None and fg > max_fg:
        return False, f"{mandate['display_name']}: F&G {fg} too high (max {max_fg} for crisis entry)"

    max_breadth_entry = cond.get("max_breadth_for_entry")
    if max_breadth_entry is not None and breadth is not None and breadth > max_breadth_entry:
        return False, (
            f"{mandate['display_name']}: breadth {breadth:.0f}% too high "
            f"(max {max_breadth_entry}% for crisis entry)"
        )

    # Data: needs deep scan hit
    min_ds = cond.get("min_deep_scan_strength")
    if min_ds is not None:
        top = market_data.get("deep_scan_top") or []
        best = max((r.get("signal_strength", 0) for r in top), default=0)
        if best < min_ds:
            return False, (
                f"{mandate['display_name']}: best Deep Scan signal {best:.2f} "
                f"below threshold {min_ds}"
            )

    # Troi: F&G extreme trigger
    fg_buy_below = cond.get("fg_buy_below")
    fg_short_above = cond.get("fg_short_above")
    if fg_buy_below is not None and fg_short_above is not None and fg is not None:
        if not (fg < fg_buy_below or fg > fg_short_above):
            return False, (
                f"{mandate['display_name']}: F&G {fg} not at extreme "
                f"(buy below {fg_buy_below}, short above {fg_short_above})"
            )

    # Scotty: risk-off or dip required
    if cond.get("risk_off_or_dip"):
        risk_off_sessions = ["CHOP", "REVERSAL_RISK", "VOLATILE"]
        if cond.get("neutral_ok"):
            risk_off_sessions.append("NEUTRAL")
        spy_min_dip = cond.get("min_spy_dip_pct", -2.0)
        is_risk_off = session in risk_off_sessions or session == "TRENDING_BEAR"
        is_dip = spy_ret <= spy_min_dip
        if not is_risk_off and not is_dip:
            return False, (
                f"{mandate['display_name']} stands down — session {session}, "
                f"SPY {spy_ret:+.1f}% (needs RISK_OFF/NEUTRAL or dip ≤{spy_min_dip}%)"
            )

    return True, f"{mandate['display_name']} cleared for trading ({mandate['role']})"


def get_agent_trade_idea(player_id: str, market_data: dict) -> dict:
    """
    Generate a mandate-aware trade idea for an agent.

    Returns:
        {
            "player_id"  : str,
            "action"     : "BUY" | "SELL" | "SHORT" | "HOLD",
            "symbol"     : str | None,
            "confidence" : float  (0-1),
            "reason"     : str,
            "size_factor": float,
        }
    """
    allowed, gate_reason = should_agent_trade(player_id, market_data)
    if not allowed:
        return {
            "player_id": player_id,
            "action": "HOLD",
            "symbol": None,
            "confidence": 0.0,
            "reason": gate_reason,
            "size_factor": 0.0,
        }

    mandate = CREW_MANIFEST.get(player_id, {})
    cond = mandate.get("conditions", {})
    session = (market_data.get("session_type") or "").upper()
    vix = float(market_data.get("vix") or 0)
    pc = float(market_data.get("pc_ratio") or 1.0)
    fg = market_data.get("fg_score")
    momentum = float(market_data.get("momentum_score") or 0)
    universe = mandate.get("universe")
    size_factor = mandate.get("size_factor", 1.0)

    # ── Worf: bearish only ──────────────────────────────────────────────
    if cond.get("bearish_only"):
        symbol = (universe or ["SQQQ"])[0]
        return {
            "player_id": player_id,
            "action": "BUY",  # buy the inverse ETF
            "symbol": symbol,
            "confidence": min(1.0, (vix - 16) / 20),
            "reason": f"Bearish entry: VIX {vix:.1f}, session {session}",
            "size_factor": size_factor,
        }

    # ── McCoy: crisis buy ───────────────────────────────────────────────
    if cond.get("min_vix_for_entry", 0) >= 25:
        return {
            "player_id": player_id,
            "action": "BUY",
            "symbol": "SPY",
            "confidence": min(1.0, (vix - 25) / 20),
            "reason": f"Crisis entry: VIX {vix:.1f}, blood in the streets",
            "size_factor": size_factor,
        }

    # ── Sulu: momentum direction ────────────────────────────────────────
    if cond.get("min_momentum_score") is not None:
        action = "BUY" if session == "TRENDING_BULL" else "SHORT"
        return {
            "player_id": player_id,
            "action": action,
            "symbol": "SPY",
            "confidence": min(1.0, abs(momentum) / 100),
            "reason": f"Momentum {action}: session {session}, score {momentum:.1f}",
            "size_factor": size_factor,
        }

    # ── Trip: contrarian ────────────────────────────────────────────────
    pc_buy = cond.get("pc_ratio_min_buy", 999)
    pc_short = cond.get("pc_ratio_max_short", -999)
    if pc > pc_buy:
        action, conf_raw = "BUY", min(1.0, (pc - pc_buy) / 1.0)
    elif pc < pc_short:
        action, conf_raw = "SHORT", min(1.0, (pc_short - pc) / 0.5)
    elif fg is not None and fg < cond.get("fg_buy_below", 25):
        action, conf_raw = "BUY", (25 - fg) / 25
    elif fg is not None and fg > cond.get("fg_short_above", 75):
        action, conf_raw = "SHORT", (fg - 75) / 25
    else:
        action, conf_raw = "HOLD", 0.0
    if action != "HOLD":
        return {
            "player_id": player_id,
            "action": action,
            "symbol": "SPY",
            "confidence": round(conf_raw, 2),
            "reason": f"Contrarian {action}: P/C {pc:.2f}, F&G {fg}",
            "size_factor": size_factor,
        }

    # ── Scotty: defensive buy ───────────────────────────────────────────
    if universe and cond.get("risk_off_or_dip"):
        return {
            "player_id": player_id,
            "action": "BUY",
            "symbol": universe[0],
            "confidence": 0.6,
            "reason": f"Defensive/value entry: session {session}",
            "size_factor": size_factor,
        }

    # ── Geordi: sector rotation ────────────────────────────────────────
    if universe and cond.get("sector_etfs_only"):
        top_sector = market_data.get("sector_leader") or universe[0]
        return {
            "player_id": player_id,
            "action": "BUY",
            "symbol": top_sector if top_sector in universe else universe[0],
            "confidence": 0.65,
            "reason": f"Sector rotation: leading {top_sector}",
            "size_factor": size_factor,
        }

    # ── Generic fallback ────────────────────────────────────────────────
    return {
        "player_id": player_id,
        "action": "HOLD",
        "symbol": None,
        "confidence": 0.0,
        "reason": f"No qualifying setup for {mandate.get('display_name', player_id)}",
        "size_factor": size_factor,
    }


# ---------------------------------------------------------------------------
# Convenience: tier badge CSS class
# ---------------------------------------------------------------------------

TIER_BADGE: dict[Any, str] = {
    1: "tier-badge-gold",
    2: "tier-badge-silver",
    3: "tier-badge-blue",
    "standalone": "tier-badge-teal",
    "special": "tier-badge-gray",
}


def get_tier_badge_class(player_id: str) -> str:
    mandate = CREW_MANIFEST.get(player_id)
    if not mandate:
        return "tier-badge-gray"
    return TIER_BADGE.get(mandate.get("tier"), "tier-badge-gray")


def get_role_subtitle(player_id: str) -> str:
    mandate = CREW_MANIFEST.get(player_id)
    if not mandate:
        return ""
    return mandate.get("role", "")


# ---------------------------------------------------------------------------
# Quick self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== CREW_MANIFEST ===")
    for pid, m in CREW_MANIFEST.items():
        tier_label = f"Tier {m['tier']}" if isinstance(m['tier'], int) else m['tier'].upper()
        print(f"  [{tier_label:12s}] {pid:22s}  {m['display_name']:22s}  {m['role']}")

    print("\n=== GATE TESTS ===")
    tests = [
        ("grok-4",     {"session_type": "TRENDING_BULL"},                "Spock → blocked (trending)"),
        ("grok-4",     {"session_type": "CHOP", "rsi": 28},              "Spock → allowed (chop+oversold)"),
        ("ollama-plutus", {"vix": 18},                                   "McCoy → blocked (VIX too low)"),
        ("ollama-plutus", {"vix": 28, "fg_score": 22, "breadth_score": 25}, "McCoy → allowed (crisis)"),
        ("dayblade-sulu", {"session_type": "CHOP", "momentum_score": 10}, "Sulu → blocked (chop)"),
        ("dayblade-sulu", {"session_type": "TRENDING_BULL", "momentum_score": 45}, "Sulu → allowed"),
        ("claude-haiku", {},                                             "Lt. Malcolm Reed → bridge voter"),
    ]
    for pid, md, label in tests:
        ok, reason = should_agent_trade(pid, md)
        status = "✅ ALLOWED" if ok else "🚫 BLOCKED"
        print(f"  {status}  {label}")
        print(f"           → {reason}")

    print("\n=== BRIDGE VOTERS ===")
    voters = [pid for pid, m in CREW_MANIFEST.items() if m.get("bridge_voter")]
    print(f"  {len(voters)} voters: {', '.join(voters)}")
