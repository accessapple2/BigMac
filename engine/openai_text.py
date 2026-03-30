from __future__ import annotations

import os
from typing import Any

from openai import OpenAI


DEFAULT_CODEX_MODEL = os.environ.get("OPENAI_CODEX_MODEL", "gpt-5.2-codex")
DEFAULT_CODEX_MINI_MODEL = os.environ.get("OPENAI_CODEX_MINI_MODEL", DEFAULT_CODEX_MODEL)


def resolve_openai_api_key(api_key: str | None = None) -> str:
    return api_key or os.environ.get("OPENAI_API_KEY", "")


def _supports_reasoning(model: str) -> bool:
    return model.startswith("gpt-5") or model.startswith("o")


def _extract_output_text(response: Any) -> str:
    text = getattr(response, "output_text", None)
    if text:
        return text

    for item in getattr(response, "output", []) or []:
        for content in getattr(item, "content", []) or []:
            chunk = getattr(content, "text", None)
            if chunk:
                return chunk

    return ""


def generate_text(
    prompt: str,
    *,
    system: str | None = None,
    model: str | None = None,
    max_output_tokens: int = 500,
    reasoning_effort: str = "medium",
    api_key: str | None = None,
    timeout: float = 90.0,
) -> str:
    key = resolve_openai_api_key(api_key)
    if not key:
        return ""

    model_id = model or DEFAULT_CODEX_MODEL
    client = OpenAI(api_key=key, timeout=timeout)

    if hasattr(client, "responses"):
        input_items = []
        if system:
            input_items.append({
                "role": "system",
                "content": [{"type": "input_text", "text": system}],
            })
        input_items.append({
            "role": "user",
            "content": [{"type": "input_text", "text": prompt}],
        })

        payload: dict[str, Any] = {
            "model": model_id,
            "input": input_items,
            "max_output_tokens": max_output_tokens,
            "text": {"format": {"type": "text"}},
        }
        if _supports_reasoning(model_id):
            payload["reasoning"] = {"effort": reasoning_effort}
        response = client.responses.create(**payload)
        return _extract_output_text(response).strip()

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    token_param = "max_completion_tokens" if _supports_reasoning(model_id) else "max_tokens"
    response = client.chat.completions.create(
        model=model_id,
        messages=messages,
        **{token_param: max_output_tokens},
    )
    return (response.choices[0].message.content or "").strip()
