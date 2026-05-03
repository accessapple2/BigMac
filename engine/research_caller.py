"""Research caller — uses local Ollama qwen3:14b for cheap research calls.

Was Gemini Flash. Now routes to local inference. Zero Google API calls.
"""
from __future__ import annotations
import threading
import requests
from rich.console import Console
from config import OLLIE_URL as _OLLAMA_URL

console = Console()
_MODEL = "qwen3:8b"  # 2026-04-27 monday: was 14b causing G1 thrash

# Limit concurrent Ollama research calls to 2 — prevents deepseek-r1:7b
# from being overwhelmed when multiple model groups + background threads
# all hit Ollama simultaneously during market scans.
_semaphore = threading.Semaphore(2)


def call_flash(prompt: str, timeout_ms: int = 120_000) -> str | None:
    """Call local Ollama (fast path) for research. Returns None on failure.

    think=False disables qwen3 extended reasoning — this is the Flash
    equivalent, designed for speed not depth. Without it, qwen3:8b
    generates 10k+ thinking tokens and consistently times out at 120s.
    """
    with _semaphore:
        try:
            r = requests.post(
                f"{_OLLAMA_URL}/api/generate",
                json={"model": _MODEL, "prompt": prompt, "stream": False, "think": False},
                timeout=timeout_ms / 1000,
            )
            if r.ok:
                return r.json().get("response", "") or None
        except Exception as e:
            console.log(f"[red]Research caller (Ollama) error: {e}")
    return None
