"""HM-ARCHER-REBUILD — Captain Archer's plutus-v1 reasoning brain.

Calls plutus-v1 on Ollie Max (.168). Three entry points:
  morning_briefing()  — fresh daily synthesis (no filler)
  alert_narrative()   — 2-3 sentence tiered alert text
  interactive(query)  — ask-Archer, grounded in live convergence context

ADVISORY ONLY. Archer reasons and reports; he never executes.
"""
from __future__ import annotations

import logging
import requests
from datetime import datetime

from engine.archer import intel_sources as src
from engine.archer.convergence import compute_convergence

logger = logging.getLogger(__name__)

OLLAMA = "http://192.168.1.168:11434/api/generate"
MODEL = "plutus-v1"   # resident, non-retiring tag (ruling #1) — NOT 0xroyce/plutus

PERSONA = (
    "You are Captain Jonathan Archer, intel officer of the OllieTrades fleet. "
    "You synthesize signals from across the platform into sharp, specific, "
    "non-repetitive reads. You NAME the systems your intel comes from "
    "(Ollie AI Scanner, GEX/options flow, Uhura, Congress, crew consensus, "
    "SUPER_MAX expectancy, sell-the-news shorts). You never pad with filler "
    "like 'we are monitoring' or 'stay tuned'. If nothing is new or notable, "
    "you say so plainly and briefly. You are advisory only — you flag and "
    "explain, you do not place trades. Always use the real current date."
)


def _call(prompt: str, max_tokens: int = 800, temperature: float = 0.7) -> str:
    try:
        r = requests.post(
            OLLAMA,
            json={
                "model": MODEL,
                "prompt": (
                    f"{PERSONA}\n\nToday is {datetime.now():%A, %B %d, %Y}.\n\n{prompt}"
                ),
                "stream": False,
                "think": False,  # disable any chain-of-thought wrapper
                "options": {"num_predict": max_tokens, "temperature": temperature},
            },
            timeout=150,
        )
        if not r.ok:
            logger.error("[Archer/brain] plutus HTTP %s", r.status_code)
            return ""
        return r.json().get("response", "").strip()
    except Exception as e:
        logger.error("[Archer/brain] plutus call failed: %s: %r", type(e).__name__, e)
        return ""


def _intel_bundle() -> dict:
    conv = compute_convergence()
    return {
        "regime": src.get_regime(),
        "gex": src.get_gex(),
        "uhura": src.get_uhura(),
        "top_convergences": conv[:6],
        "short_signals": src.get_short_signals(),
        "supermax_edges": src.get_supermax_edges()[:6],
        "ollie_scanner": src.get_ollie_scanner()[:6],
        "congress": src.get_congress()[:6],
    }


def morning_briefing() -> str:
    """Fresh daily synthesis briefing. Returns plutus-v1 text (may be '')."""
    intel = _intel_bundle()
    reds = [c for c in intel["top_convergences"] if c["tier"] == "RED"]
    yellows = [c for c in intel["top_convergences"] if c["tier"] == "YELLOW"]
    prompt = (
        "Write today's morning intel briefing for the Admiral. Live intel below "
        "(JSON):\n\n"
        f"{intel}\n\n"
        "Instructions:\n"
        "- Lead with the single most important thing in the data.\n"
        f"- RED-tier convergences (5/5 systems): {reds or 'none today'}.\n"
        f"- YELLOW-tier (3-4/5): {yellows or 'none today'}.\n"
        "- Call out any new short signals by name (sell-the-news). If none, say "
        "shorts are quiet.\n"
        "- Note that the Congress leg is offline today (scraper down) so "
        "convergence caps at 4 of 5 — don't pretend it's 5.\n"
        "- Name the systems behind each call. Be specific and fresh — NOT a "
        "generic template. 200-300 words. End with one clear watch-list line."
    )
    return _call(prompt, max_tokens=900)


def alert_narrative(item: dict) -> str:
    """2-3 sentence narrative for a fired alert."""
    prompt = (
        f"A {item.get('tier')} alert just fired. Details (JSON):\n{item}\n\n"
        "Write a 2-3 sentence alert. Name the system(s) that triggered it, say "
        "what it means, and what to watch next. No filler, no preamble."
    )
    return _call(prompt, max_tokens=220, temperature=0.5)


def interactive(query: str) -> str:
    """Ask-Archer. Grounded in live convergence + regime context."""
    conv = compute_convergence()
    ctx = {
        "convergences": conv[:8],
        "regime": src.get_regime(),
        "gex": src.get_gex(),
        "uhura": src.get_uhura(),
        "shorts": src.get_short_signals(),
    }
    prompt = (
        f'The Admiral asks: "{query}"\n\nLive context (JSON):\n{ctx}\n\n'
        "Answer directly and specifically. If asked to explain a model "
        "(plutus, qwen) or a strategy (sell-the-news, GEX, convergence, "
        "SUPER_MAX), explain how it works AND how it applies right now given "
        "the context. If asked for a read on a ticker, give your call with "
        "reasoning. Keep it tight."
    )
    return _call(prompt, max_tokens=700)
