from __future__ import annotations
import requests
from .base import AIProvider


class GrokProvider(AIProvider):
    """Formerly xAI Grok — now routed to local Ollama qwen3.5:9b (zero cost).

    RAM patch 2026-04-17: swapped deepseek-r1:14b (9.7GB) → qwen3.5:9b (already
    warm for most agents; funnel keeps RAM pressure low on 16GB box).
    """

    def __init__(self, api_key: str = "", player_id: str = "grok-3",
                 model: str = "qwen3.5:9b", display_name: str = "Grok 3"):
        super().__init__(player_id, display_name, model, rate_limit=30)
        from config import OLLAMA_URL
        self._ollama_url = OLLAMA_URL
        self._ollama_model = "qwen3.5:9b"

    def call_model(self, prompt: str) -> str:
        resp = requests.post(
            self._ollama_url + "/api/generate",
            json={"model": self._ollama_model, "prompt": prompt, "stream": False},
            timeout=90,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
