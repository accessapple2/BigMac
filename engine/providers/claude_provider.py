from __future__ import annotations
from .openai_provider import OpenAIProvider


class ClaudeProvider(OpenAIProvider):
    def __init__(self, api_key: str, player_id: str = "claude-sonnet",
                 model: str = "gpt-5.2-codex", display_name: str = "Codex"):
        super().__init__(api_key, player_id, model, display_name)
