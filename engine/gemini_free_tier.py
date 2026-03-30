"""Gemini Free Tier Rate Limiter.

Hard cap: 400 requests/day (free tier allows 500, keeping 100-request buffer).
Auto-fallback to Ollama gemma3:4b when limit is hit.
Counter persists to data/gemini_daily_count.json — survives restarts.
Resets at midnight.

Usage:
    from engine.gemini_free_tier import call_gemini, get_daily_status
    result = call_gemini(prompt, system="optional system prompt")
"""
from __future__ import annotations
import json
import os
import threading
from datetime import date
from pathlib import Path
from rich.console import Console

console = Console()

GEMINI_MODEL = "gemini-3.1-flash-lite"
DAILY_LIMIT = 400
_COUNT_FILE = Path("data/gemini_daily_count.json")
_OLLAMA_URL = "http://localhost:11434"
_OLLAMA_MODEL = "gemma3:4b"

_lock = threading.Lock()
_state: dict = {"date": "", "count": 0}
_initialized = False


def _load():
    global _state, _initialized
    if _initialized:
        return
    _initialized = True
    try:
        if _COUNT_FILE.exists():
            loaded = json.loads(_COUNT_FILE.read_text())
            if isinstance(loaded, dict) and "date" in loaded:
                _state.update(loaded)
    except Exception:
        pass


def _save():
    try:
        _COUNT_FILE.parent.mkdir(parents=True, exist_ok=True)
        _COUNT_FILE.write_text(json.dumps(_state, indent=2))
    except Exception as e:
        console.log(f"[yellow]gemini_free_tier: could not save count file: {e}")


def _reset_if_new_day():
    today = date.today().isoformat()
    if _state.get("date") != today:
        _state["date"] = today
        _state["count"] = 0
        _save()
        console.log(f"[cyan]Gemini free tier: counter reset for {today}")


def get_daily_status() -> dict:
    """Return current usage — safe to call from dashboard or logs."""
    with _lock:
        _load()
        _reset_if_new_day()
        return {
            "date": _state["date"],
            "count": _state["count"],
            "limit": DAILY_LIMIT,
            "remaining": max(0, DAILY_LIMIT - _state["count"]),
            "model": GEMINI_MODEL,
        }


def call_gemini(prompt: str, system: str = "", timeout: int = 90) -> str:
    """Call Gemini free tier with daily rate cap. Falls back to Ollama on limit or error.

    Args:
        prompt: The user prompt.
        system: Optional system/context prefix.
        timeout: Seconds before giving up on the Gemini call.

    Returns:
        Model response text (from Gemini or Ollama fallback).
    """
    with _lock:
        _load()
        _reset_if_new_day()

        if _state["count"] >= DAILY_LIMIT:
            console.log(
                f"[bold yellow]⚠ Gemini free tier LIMIT REACHED "
                f"({_state['count']}/{DAILY_LIMIT} today) — falling back to Ollama gemma3:4b"
            )
            return _ollama_fallback(prompt, system)

        _state["count"] += 1
        count_now = _state["count"]
        _save()

    console.log(
        f"[cyan]Gemini free tier call #{count_now}/{DAILY_LIMIT} "
        f"(date: {_state['date']}, model: {GEMINI_MODEL})"
    )

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        console.log("[yellow]Gemini free tier: GEMINI_API_KEY not set — using Ollama fallback")
        with _lock:
            _state["count"] = max(0, _state["count"] - 1)
            _save()
        return _ollama_fallback(prompt, system)

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(GEMINI_MODEL)
        full_prompt = f"{system}\n\n{prompt}" if system else prompt
        resp = model.generate_content(full_prompt)
        return (resp.text or "").strip()

    except Exception as e:
        err = str(e)
        console.log(f"[red]Gemini free tier error (call #{count_now}): {err[:200]}")
        # Don't count errored calls against limit — refund
        with _lock:
            _state["count"] = max(0, _state["count"] - 1)
            _save()
        return _ollama_fallback(prompt, system)


def _ollama_fallback(prompt: str, system: str = "") -> str:
    """Call local Ollama gemma3:4b as fallback."""
    import requests
    payload = {"model": _OLLAMA_MODEL, "prompt": prompt, "stream": False}
    if system:
        payload["system"] = system
    try:
        r = requests.post(f"{_OLLAMA_URL}/api/generate", json=payload, timeout=120)
        if r.ok:
            return r.json().get("response", "").strip()
    except Exception as e:
        console.log(f"[red]Ollama fallback error: {e}")
    return ""
