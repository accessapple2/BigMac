from __future__ import annotations
from openai import OpenAI
from .base import AIProvider


class GroqProvider(AIProvider):
    def __init__(self, api_key: str, player_id: str = "ollama-llama",
                 model: str = "llama-3.3-70b-versatile", display_name: str = "Llama 3.3 70B"):
        super().__init__(player_id, display_name, model, rate_limit=30)
        self.client = OpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1", timeout=90.0)

    def call_model(self, prompt: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model_id,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500
        )
        return response.choices[0].message.content or ""
