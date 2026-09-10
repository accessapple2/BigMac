"""HM-XO-PLAN-2026-09 Phase 2 -- MiniMax M3 bakeoff arm.

BAKEOFF ARM ONLY. Not wired to config.AI_PLAYERS / agent_routing.py -- no
fleet seat reads this provider. Exists so scripts/mccoy_bakeoff_arms.py (and
whatever Phase 2 harness follows it) can swap this in wherever an
OllamaProvider would otherwise go.

Hits MiniMax's native OpenAI-compatible chat-completions endpoint directly
with `requests` rather than through the `openai` SDK's base_url routing --
MiniMax's real path is /v1/text/chatcompletion_v2, not the SDK's assumed
/v1/chat/completions, so pointing the SDK's base_url at MiniMax would 404.
Mirrors xo_brief.py::call_grok's shape (OpenAI-style message list, an
OpenAI-compatible non-OpenAI vendor, key from .env) but as a class with a
call_model(prompt) -> str method so it drops into the same slot as
OllamaProvider for a harness that swaps arms by object, not by function.

MiniMax-M3 is a reasoning model: the API returns `reasoning_content`
(hidden chain-of-thought, billed as output tokens) separately from
`content` (the actual answer) -- unlike qwen3's inline <think> tags, there
is nothing to strip here, but a too-small max_tokens budget can exhaust
itself on reasoning before any `content` is emitted, returning "" (smoke-
tested live 2026-09-10: max_tokens=8 -> content="", reasoning consumed all
8; max_tokens=200 -> content="PONG", 23 of 25 completion tokens were
reasoning). Default budget below is sized generously for this reason.
"""
from __future__ import annotations

import logging
import os
import time

import requests

from .base import AIProvider

_logger = logging.getLogger("minimax_provider")
_logger.setLevel(logging.INFO)

MINIMAX_CHAT_URL = "https://api.minimax.io/v1/text/chatcompletion_v2"
MINIMAX_MODEL = "MiniMax-M3"

# $/1M tokens, confirmed against api.minimax.io 2026-09-10 (standard tier).
_INPUT_RATE_PER_M = 0.30
_OUTPUT_RATE_PER_M = 1.20

_DEFAULT_TIMEOUT_S = 90
_DEFAULT_MAX_TOKENS = 4096


class MiniMaxProvider(AIProvider):
    """Bakeoff arm — same call_model(prompt) -> str interface as
    OllamaProvider, so a harness can swap arms without special-casing the
    call shape per provider."""

    def __init__(self, player_id: str = "bakeoff-minimax", model: str = MINIMAX_MODEL,
                 timeout: int = _DEFAULT_TIMEOUT_S, max_tokens: int = _DEFAULT_MAX_TOKENS,
                 daily_cost_cap: float | None = None):
        super().__init__(player_id, f"MiniMax {model}", model, rate_limit=999)
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.daily_cost_cap = daily_cost_cap
        # In-process only — this arm has no ai_players row and no api_costs
        # table entry (bakeoff-only, not wired to fleet cost tracking).
        self.total_cost_usd = 0.0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self._api_key = os.environ.get("MINIMAX_API_KEY")
        if not self._api_key:
            raise RuntimeError("MINIMAX_API_KEY not set in environment -- required for MiniMaxProvider")

    def call_model(self, prompt: str) -> str:
        if self.daily_cost_cap is not None and self.total_cost_usd >= self.daily_cost_cap:
            raise RuntimeError(
                f"minimax daily cost cap reached (${self.total_cost_usd:.4f} of "
                f"${self.daily_cost_cap:.2f}) -- call skipped"
            )

        payload = {
            "model": self.model_id,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": self.max_tokens,
            # Standard tier only — deliberately NOT setting service_tier so
            # the account default ("standard", confirmed live) applies.
        }

        t0 = time.time()
        try:
            r = requests.post(
                MINIMAX_CHAT_URL,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=self.timeout,
            )
            r.raise_for_status()
            body = r.json()
        except Exception as e:
            _logger.error("[MINIMAX-FAIL] model=%s agent=%s err=%s", self.model_id, self.player_id, e)
            raise

        base_resp = body.get("base_resp") or {}
        if base_resp.get("status_code", 0) != 0:
            err = base_resp.get("status_msg", "unknown minimax error")
            _logger.error("[MINIMAX-FAIL] model=%s agent=%s err=%s", self.model_id, self.player_id, err)
            raise RuntimeError(f"minimax error for {self.model_id}: {err}")

        usage = body.get("usage") or {}
        in_tok = int(usage.get("prompt_tokens", 0) or 0)
        out_tok = int(usage.get("completion_tokens", 0) or 0)
        cost = (in_tok / 1_000_000) * _INPUT_RATE_PER_M + (out_tok / 1_000_000) * _OUTPUT_RATE_PER_M
        self.total_input_tokens += in_tok
        self.total_output_tokens += out_tok
        self.total_cost_usd += cost

        _logger.info(
            "minimax_call model=%s agent=%s wall=%.2fs tokens_in=%d tokens_out=%d "
            "cost_usd=%.6f running_total_usd=%.6f",
            self.model_id, self.player_id, time.time() - t0, in_tok, out_tok,
            cost, self.total_cost_usd,
        )

        choices = body.get("choices") or []
        if not choices:
            return ""
        return choices[0].get("message", {}).get("content", "") or ""
