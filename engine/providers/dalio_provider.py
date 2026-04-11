"""DalioFallbackProvider — Mr. Dalio's tiered model chain.

Primary:    Google Gemini 2.5 Flash (REST API, real call)
Fallback 1: Ollama gemma3:27b (macro-scale thinking — bigger model fits Dalio's style)
Fallback 2: Ollama qwen3:14b  (last resort)

Stagger: module-level lock enforces ≥15s between Gemini API calls so Worf,
Troi, and Dalio don't hammer the same endpoint simultaneously.
"""
from __future__ import annotations
import threading
import time
import requests as _req
from rich.console import Console

from .ollama_provider import OllamaProvider, _ollama_lock

console = Console()

# --------------------------------------------------------------------------
# Module-level Gemini call stagger (shared across ALL Gemini-using providers)
# --------------------------------------------------------------------------
_gemini_stagger_lock = threading.Lock()
_last_gemini_ts: float = 0.0
_GEMINI_STAGGER_S: int = 15


class GeminiRateLimitError(Exception):
    """Raised when Gemini returns 429 Too Many Requests."""


def _call_gemini_api(api_key: str, prompt: str) -> str:
    """Call Google Gemini 2.5 Flash REST API with stagger enforcement.

    Raises:
        GeminiRateLimitError: on HTTP 429
        requests.RequestException: on connection / timeout errors
    """
    global _last_gemini_ts
    with _gemini_stagger_lock:
        elapsed = time.time() - _last_gemini_ts
        if elapsed < _GEMINI_STAGGER_S:
            wait = _GEMINI_STAGGER_S - elapsed
            console.log(f"[dim]Gemini stagger: waiting {wait:.1f}s before next call")
            time.sleep(wait)
        _last_gemini_ts = time.time()   # mark BEFORE the call so stagger still applies on error

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.5-flash:generateContent?key={api_key}"
    )
    resp = _req.post(url, json={
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": 800, "temperature": 0.7},
    }, timeout=60)

    if resp.status_code == 429:
        raise GeminiRateLimitError(f"Gemini 429: {resp.text[:200]}")
    resp.raise_for_status()

    candidates = resp.json().get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return parts[0].get("text", "") if parts else ""


class DalioFallbackProvider(OllamaProvider):
    """Mr. Dalio's fallback chain: Gemini Flash → qwen3:14b.

    Extends OllamaProvider so it slots into the Ollama provider batch
    (serialised via _ollama_lock for local calls, stagger lock for Gemini).
    """

    def __init__(self, api_key: str = "", player_id: str = "dalio-metals",
                 url: str = "http://localhost:11434"):
        # Parent init: model_id=qwen3:14b is the final fallback
        super().__init__(player_id=player_id, model="qwen3:14b", url=url)
        self.api_key = api_key
        self._ollama_base_url = url
        self.model_used = "qwen3:14b (default)"   # updated on every call

    # ------------------------------------------------------------------
    # Override call_model — this is the only thing the base analyze()
    # needs; everything else (prompt building, personality, etc.) flows
    # from the base class unchanged.
    # ------------------------------------------------------------------
    def call_model(self, prompt: str) -> str:
        # ── 1. Try Gemini Flash ────────────────────────────────────────
        if self.api_key:
            try:
                result = _call_gemini_api(self.api_key, prompt)
                if result.strip():
                    self.model_used = "gemini-2.5-flash ✓"
                    console.log("[bold green]Dalio: gemini-2.5-flash")
                    return result
            except GeminiRateLimitError:
                console.log(f"[yellow]Dalio: Gemini rate-limited → qwen3:14b (skipping 27b for speed)")
            except _req.exceptions.RequestException as e:
                console.log(f"[yellow]Dalio: Gemini connection error ({e}) → qwen3:14b")
            except Exception as e:
                console.log(f"[yellow]Dalio: Gemini error ({e}) → qwen3:14b")

        # ── 2. Fallback: qwen3:14b ──
        # gemma3:27b removed from fallback chain — 5+ min inference blocks the entire scan cycle
        self.model_used = "qwen3:14b (Gemini fallback)"
        console.log("[yellow]Dalio: qwen3:14b (Gemini unavailable)")
        return super().call_model(prompt)
