"""Q's Daily Quote — morning market observation posted to War Room.

Uses Ollama Gemma3 4B (free). Generates one witty quote per trading day.
Scheduled at 6:00 AM MST weekdays.
"""
from __future__ import annotations
import time
import requests
from datetime import datetime
from rich.console import Console

console = Console()
_last_quote_date = ""


def generate_q_daily_quote() -> str | None:
    """Generate Q's daily market observation using Ollama."""
    global _last_quote_date
    today = datetime.now().strftime("%Y-%m-%d")
    if _last_quote_date == today:
        return None

    from config import OLLAMA_URL, OLLAMA_MODEL

    # Get current market context
    regime_text = "unknown"
    try:
        from engine.regime_detector import detect_regime
        r = detect_regime()
        regime_text = f"{r['regime']} mode, VIX at {r.get('vix', '?')}, SPY at ${r.get('spy_price', '?')}"
    except Exception:
        pass

    prompt = (
        "You are Q from Star Trek — the omnipotent, sardonic, brilliant being "
        "who loves to tease and challenge Captain Kirk.\n\n"
        f"Current market: {regime_text}.\n\n"
        "Write a SHORT (1-3 sentences) daily observation for the War Room. "
        "Be witty, insightful about markets, and in character. "
        "Mix genuine wisdom with Q's signature sarcasm.\n\n"
        "Examples of the tone:\n"
        '- "Really, Captain? You\'re worried about a 2% dip? I\'ve watched '
        'civilizations rise and fall. Buy the dip."\n'
        '- "VIX at 27? How delightfully chaotic. I love it when humans '
        'pretend they understand volatility."\n'
        '- "I could snap my fingers and show you tomorrow\'s prices, but '
        'where\'s the fun in that?"\n\n'
        "Write ONE new quote. No preamble, just the quote."
    )

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        quote = resp.json().get("response", "").strip()
        if not quote:
            return None

        # Post to War Room
        from engine.war_room import save_hot_take
        save_hot_take("q-entity", "DAILY", f"✨ Q'S DAILY OBSERVATION: {quote}")
        _last_quote_date = today

        console.log(f"[bold purple]✨ Q's daily quote posted ({len(quote)} chars)")
        return quote

    except Exception as e:
        console.log(f"[red]Q daily quote error: {e}")
        return None
