from __future__ import annotations
import logging
import time
from openai import OpenAI, RateLimitError
from .base import AIProvider

logger = logging.getLogger("groq_provider")

# Exponential backoff state per-instance: (consecutive_429s, backoff_until)
_BACKOFF_DELAYS = [30, 60, 120]   # seconds after 1st, 2nd, 3rd consecutive 429


class GroqProvider(AIProvider):
    def __init__(self, api_key: str, player_id: str = "ollama-llama",
                 model: str = "llama-3.3-70b-versatile", display_name: str = "Llama 3.3 70B"):
        super().__init__(player_id, display_name, model, rate_limit=30)
        self.client = OpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1", timeout=90.0)
        self._consecutive_429s = 0
        self._backoff_until: float = 0.0

    def call_model(self, prompt: str) -> str:
        # Honor active backoff — skip this call rather than hammering Groq
        if time.time() < self._backoff_until:
            remaining = int(self._backoff_until - time.time())
            logger.debug(f"[Groq] Backoff active — skipping call ({remaining}s remaining)")
            return ""

        try:
            response = self.client.chat.completions.create(
                model=self.model_id,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
            )
            self._consecutive_429s = 0   # successful call resets counter
            return response.choices[0].message.content or ""

        except RateLimitError:
            idx = min(self._consecutive_429s, len(_BACKOFF_DELAYS) - 1)
            delay = _BACKOFF_DELAYS[idx]
            self._consecutive_429s += 1
            self._backoff_until = time.time() + delay
            if self._consecutive_429s <= len(_BACKOFF_DELAYS):
                logger.warning(
                    f"[Groq] 429 rate-limited (#{self._consecutive_429s}) — "
                    f"backing off {delay}s (until next cycle)"
                )
            return ""
