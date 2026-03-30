"""GeminiProvider — redirected to local Ollama gemma3:4b.

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
    """Routes all calls to local Ollama gemma3:4b instead of Google API."""

    def __init__(self, api_key: str = "", player_id: str = "gemini-2.5-pro",
                 model: str = "gemma3:4b", display_name: str = "Gemma3 4B"):
        super().__init__(player_id=player_id, model="gemma3:4b")
        self.display_name = display_name
