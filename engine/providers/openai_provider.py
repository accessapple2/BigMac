from __future__ import annotations

from .base import AIProvider
from engine.openai_text import DEFAULT_CODEX_MODEL, generate_text


class OpenAIProvider(AIProvider):
    def __init__(self, api_key: str, player_id: str = "gpt-4o",
                 model: str = DEFAULT_CODEX_MODEL, display_name: str = "OpenAI"):
        super().__init__(player_id, display_name, model, rate_limit=60)
        self.api_key = api_key

    def call_model(self, prompt: str) -> str:
        return generate_text(
            prompt,
            model=self.model_id,
            api_key=self.api_key,
            max_output_tokens=500,
            reasoning_effort="medium",
        )
