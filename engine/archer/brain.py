"""HM-ARCHER-REBUILD — Captain Archer's plutus-v1 reasoning brain.

Calls plutus-v1 on Ollie Max (.168). Three entry points:
  morning_briefing()  — fresh daily synthesis (no filler)
  alert_narrative()   — 2-3 sentence tiered alert text
  interactive(query)  — ask-Archer, grounded in live convergence context

ADVISORY ONLY. Archer reasons and reports; he never executes.
"""
from __future__ import annotations

import logging
import re
import requests
from datetime import datetime

from engine.archer import intel_sources as src
from engine.archer.convergence import compute_convergence
from engine import ticker_names as _names  # FIX-4: verified ticker→company name

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
    "explain, you do not place trades. Always use the real current date. "
    "When you refer to a company, use ONLY the company name given in the intel "
    "data (the 'name' field next to each ticker). If a ticker has no name "
    "provided, refer to the security by its ticker symbol alone. NEVER infer or "
    "invent a company name from the ticker string."
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
        # convergence output keys on ticker only (FIX-4 Phase C); annotate names
        # here at the prompt boundary so the LLM has a grounded name to use.
        "top_convergences": _names.annotate(conv[:6]),
        "short_signals": src.get_short_signals(),
        "supermax_edges": src.get_supermax_edges()[:6],
        "ollie_scanner": src.get_ollie_scanner()[:6],
        "congress": src.get_congress()[:6],
    }


_NAME_TICKER_RE = re.compile(r"([A-Z][A-Za-z0-9&.,'’\- ]{1,40}?)\s*\(([A-Z]{1,5})\)")
_NAME_NOISE = re.compile(r"[^a-z0-9 ]+")
_NAME_SUFFIX = {"inc", "corp", "corporation", "co", "company", "llc", "ltd",
                "lp", "plc", "the", "holdings", "group", "partners"}


def _name_tokens(name: str) -> set[str]:
    base = _NAME_NOISE.sub(" ", (name or "").lower())
    return {w for w in base.split() if w and w not in _NAME_SUFFIX}


def _flag_unverified_names(text: str) -> str:
    """Optional post-gen check (flag-only, non-destructive): log any
    'Company Name (TICKER)' whose narrated name shares no significant token with
    the Polygon reference name. Flag-only — we never strip, to avoid mangling a
    correct-but-cosmetically-different name. Returns text unchanged."""
    try:
        for m in _NAME_TICKER_RE.finditer(text or ""):
            narrated, ticker = m.group(1).strip(), m.group(2)
            ref = _names.get_company_name(ticker)
            if not ref:
                continue  # unresolved → can't verify, leave it
            if not (_name_tokens(narrated) & _name_tokens(ref)):
                logger.warning(
                    "[Archer/brain] possible hallucinated name: narrated %r for "
                    "%s but Polygon says %r", narrated, ticker, ref)
    except Exception as e:
        logger.warning("[Archer/brain] name validator failed: %s: %r", type(e).__name__, e)
    return text


def morning_briefing() -> str:
    """Fresh daily synthesis briefing. Returns plutus-v1 text (may be '')."""
    intel = _intel_bundle()
    reds = [c for c in intel["top_convergences"] if c["tier"] == "RED"]
    yellows = [c for c in intel["top_convergences"] if c["tier"] == "YELLOW"]
    # Congress status is DYNAMIC — derived from whether the leg returned data
    # THIS run, never hardcoded. Self-corrects in both directions.
    n_congress = len(intel.get("congress") or [])
    if n_congress > 0:
        congress_line = (
            f"- The Congress leg is LIVE this run ({n_congress} recent disclosures) "
            "— all 5 systems can converge; reflect the real congressional signals.\n"
        )
    else:
        congress_line = (
            "- The Congress leg returned no data this run (degraded) so convergence "
            "caps at 4 of 5 — say so plainly; do not infer congressional activity.\n"
        )
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
        f"{congress_line}"
        "- Name the systems behind each call. Be specific and fresh — NOT a "
        "generic template. 200-300 words. End with one clear watch-list line."
    )
    return _flag_unverified_names(_call(prompt, max_tokens=900))


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
        "convergences": _names.annotate(conv[:8]),
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
