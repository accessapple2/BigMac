from __future__ import annotations
import requests
from .base import AIProvider


class MLXProvider(AIProvider):
    """Provider for MLX-served models via OpenAI-compatible API.

    Supports Qwen3 /think and /no_think modes:
    - deep_analysis=True:  prepends /think for thesis generation, scorecard reasoning
    - deep_analysis=False: prepends /no_think for fast scanning (default)
    """

    def __init__(self, player_id: str = "mlx-qwen3",
                 model: str = "mlx-community/Qwen3-8B-4bit",
                 url: str = "http://localhost:8899",
                 display_name: str = "Qwen3 8B MLX",
                 timeout: int = 180,
                 deep_analysis: bool = True):
        super().__init__(player_id, display_name, model, rate_limit=999)
        self.url = url
        self.timeout = timeout
        self.deep_analysis = deep_analysis

    def call_model(self, prompt: str) -> str:
        prefix = "/think " if self.deep_analysis else "/no_think "
        max_tokens = 1024 if self.deep_analysis else 512

        payload = {
            "model": self.model_id,
            "messages": [{"role": "user", "content": prefix + prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.6,
        }
        r = requests.post(
            f"{self.url}/v1/chat/completions",
            json=payload,
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()
        content = data["choices"][0]["message"]["content"] or ""

        # Strip <think>...</think> block from response so downstream parsers see clean output
        import re
        content = re.sub(r"<think>[\s\S]*?</think>\s*", "", content).strip()
        return content
