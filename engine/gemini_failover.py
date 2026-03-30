"""Gemini Failover Manager — DISABLED. All calls now route to local Ollama.

This module is kept as a stub so existing imports don't break.
No Google API calls are made anywhere in the codebase.
"""
from __future__ import annotations
import requests
from rich.console import Console

console = Console()
BACKUP_MODEL = "gemma3:4b"
BACKUP_URL = "http://localhost:11434"


def is_in_failover() -> bool:
    return False  # No Gemini — always "in failover" semantically, but unused


def is_quota_error(exc) -> bool:
    return False


def activate(source: str = "", reason: str = "") -> None:
    pass


def get_status() -> dict:
    return {"active": False, "backup_model": BACKUP_MODEL, "reason": "Gemini disabled — using Ollama"}


def check_monthly_reset(api_key: str = "") -> None:
    pass


def call_ollama_backup(prompt: str, timeout: int = 90) -> str:
    """Call local Ollama gemma3:4b — the primary (and only) inference backend."""
    try:
        r = requests.post(
            f"{BACKUP_URL}/api/generate",
            json={"model": BACKUP_MODEL, "prompt": prompt, "stream": False},
            timeout=timeout,
        )
        if r.ok:
            return r.json().get("response", "")
    except Exception as e:
        console.log(f"[red]Ollama (gemma3:4b) error: {e}")
    return ""
