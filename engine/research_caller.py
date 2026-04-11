"""Research caller — uses local Ollama qwen3:14b for cheap research calls.

Was Gemini Flash. Now routes to local inference. Zero Google API calls.
"""
from __future__ import annotations
import threading
import requests
from rich.console import Console

console = Console()
_OLLAMA_URL = "http://localhost:11434"
_MODEL = "qwen3:14b"

# Limit concurrent Ollama research calls to 2 — prevents deepseek-r1:7b
# from being overwhelmed when multiple model groups + background threads
# all hit Ollama simultaneously during market scans.
_semaphore = threading.Semaphore(2)


def call_flash(prompt: str, timeout_ms: int = 120_000) -> str | None:
    """Call local Ollama qwen3:14b for research. Returns None on failure."""
    with _semaphore:
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
