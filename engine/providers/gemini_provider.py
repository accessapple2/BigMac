"""GeminiProvider — redirected to local Ollama qwen3:14b.

Kept as a drop-in class so existing code (dashboard, backtest) that imports
GeminiProvider continues to work without changes.  Zero Google API calls.
"""
from __future__ import annotations
from .ollama_provider import OllamaProvider


def gemini_quota_ok() -> bool:
    return True  # No quota — local model


def _trip_quota(hours: float = 1.0) -> None:
    pass


class GeminiProvider(OllamaProvider):
    """Routes all calls to local Ollama qwen3:14b instead of Google API."""

    def __init__(self, api_key: str = "", player_id: str = "gemini-2.5-pro",
                 model: str = "qwen3:14b", display_name: str = "Qwen3 14B"):
        super().__init__(player_id=player_id, model="qwen3:14b")
        self.display_name = display_name
