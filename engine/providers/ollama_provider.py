from __future__ import annotations
import requests
from .base import AIProvider
from engine.ollama_queue import get_queue


class OllamaProvider(AIProvider):
    def __init__(self, player_id: str = "ollama-local", model: str = "qwen3:14b",
                 url: str = "http://localhost:11434", timeout: int = 120):
        super().__init__(player_id, f"Ollama {model}", model, rate_limit=999)
        self.url = f"{url}/api/generate"
        self.timeout = timeout
        self._is_cloud = ":cloud" in model
        self._temperature = 0.6 if self._is_cloud else 0.7

    def call_model(self, prompt: str) -> str:
        # Route through global FIFO queue — one Ollama inference at a time system-wide.
        # keep_alive=60s so the model unloads quickly when the next agent needs a different model.
        payload = {
            "model": self.model_id,
            "prompt": prompt,
            "stream": False,
            "keep_alive": "60s",
            "options": {"temperature": self._temperature},
        }

        def _do_request() -> str:
            r = requests.post(self.url, json=payload, timeout=self.timeout)
            return r.json().get("response", "")

        return get_queue().submit(_do_request, model_id=self.model_id)

    def analyze_chain(self, symbol: str, price: float, change_pct: float,
                      high: float, low: float, portfolio_context: dict,
                      indicators: dict = None, news: list = None):
        """Skip Gemini Flash pre-research for Ollama — go straight to single-prompt.

        Flash research (Step 1) calls the Gemini API which times out after 60s
        for local models. Skipping saves ~60s per stock and eliminates timeout errors.
        Flash research is only useful for paid cloud models that benefit from cross-model context.
        """
        return self.analyze(symbol, price, change_pct, high, low,
                            portfolio_context, indicators, news)
