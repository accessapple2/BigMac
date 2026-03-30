"""Research caller — uses local Ollama gemma3:4b for cheap research calls.

Was Gemini Flash. Now routes to local inference. Zero Google API calls.
"""
from __future__ import annotations
import requests
from rich.console import Console

console = Console()
_OLLAMA_URL = "http://localhost:11434"
_MODEL = "gemma3:4b"


def call_flash(prompt: str, timeout_ms: int = 60_000) -> str | None:
    """Call local Ollama gemma3:4b for research. Returns None on failure."""
    try:
        r = requests.post(
            f"{_OLLAMA_URL}/api/generate",
            json={"model": _MODEL, "prompt": prompt, "stream": False},
            timeout=timeout_ms / 1000,
        )
        if r.ok:
            return r.json().get("response", "") or None
    except Exception as e:
        console.log(f"[red]Research caller (Ollama) error: {e}")
    return None
