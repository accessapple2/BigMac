from __future__ import annotations
import threading
import requests
from .base import AIProvider

# Global lock: only 1 Ollama inference at a time across all providers.
# Prevents gemma3:4b from choking when multiple players queue up concurrently.
_ollama_lock = threading.Lock()


class OllamaProvider(AIProvider):
    def __init__(self, player_id: str = "ollama-local", model: str = "gemma3:4b",
                 url: str = "http://localhost:11434", timeout: int = 120):
        super().__init__(player_id, f"Ollama {model}", model, rate_limit=999)
        self.url = f"{url}/api/generate"
        self.timeout = timeout
        self._is_cloud = ":cloud" in model
        self._temperature = 0.6 if self._is_cloud else 0.7

    def call_model(self, prompt: str) -> str:
        # Serialize all Ollama calls — one inference at a time system-wide
        acquired = _ollama_lock.acquire(timeout=600)  # wait up to 10 min
        if not acquired:
            raise RuntimeError("Ollama lock timeout — inference queue backed up >10 min")
        try:
            payload = {
                "model": self.model_id,
                "prompt": prompt,
                "stream": False,
                "keep_alive": "5m",  # 5 min: model stays loaded across multi-symbol scan
                "options": {"temperature": self._temperature},
            }
            r = requests.post(self.url, json=payload, timeout=self.timeout)
            return r.json().get("response", "")
        finally:
            _ollama_lock.release()

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
