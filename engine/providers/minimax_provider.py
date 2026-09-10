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

UNLIKE qwen3:30b-a3b's unsuppressible CoT (ollama_provider.py's
_NO_THINK_PROMPT_MODELS), M3's reasoning is a real, working API lever --
live-verified 2026-09-10: `"thinking": {"type": "disabled"}` cut a call
from 25 completion tokens (23 reasoning) to 2, with no reasoning_content
in the response at all. Default here is thinking ON (the API's own
default when the field is omitted) because the reasoning trace is the one
arm in this bakeoff whose thinking is actually observable -- pass
thinking_mode="disabled" to a given instance to test the cheaper/faster
path instead. Either way, reasoning tokens are billed at the same
output rate as visible tokens (no separate reasoning price tier per
MiniMax's published pricing) -- the split below is for cost *attribution*
per call, not a different rate.
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
                 daily_cost_cap: float | None = None, thinking_mode: str | None = None,
                 reasoning_sample_every: int = 5):
        super().__init__(player_id, f"MiniMax {model}", model, rate_limit=999)
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.daily_cost_cap = daily_cost_cap
        # None = omit the field, API default (thinking ON). Pass "disabled"
        # to send {"type": "disabled"} instead — live-verified to actually
        # suppress reasoning for M3 (see module docstring).
        self.thinking_mode = thinking_mode
        # In-process only — this arm has no ai_players row and no api_costs
        # table entry (bakeoff-only, not wired to fleet cost tracking).
        self.total_cost_usd = 0.0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        # completion_tokens splits into reasoning + visible; tracked
        # separately so cost-per-decision isn't inferred from answer length
        # alone (a short answer can still have burned a lot of reasoning).
        self.total_reasoning_tokens = 0
        self.total_visible_output_tokens = 0
        # Every Nth call's full reasoning_content is kept here (call_index,
        # reasoning_content, content) — reading a sample of M3's actual
        # chain-of-thought is more informative than the score alone, and
        # this is the only arm in the bakeoff where that's visible at all.
        # Capped sampling (not every call) keeps this from growing
        # unbounded across a full bakeoff run.
        self.reasoning_sample_every = max(1, reasoning_sample_every)
        self.reasoning_samples: list[dict] = []
        self._call_count = 0
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
        if self.thinking_mode is not None:
            payload["thinking"] = {"type": self.thinking_mode}

        self._call_count += 1
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
        # completion_tokens_details.reasoning_tokens is absent entirely when
        # thinking is disabled (confirmed live) — 0 is the correct read then,
        # not a missing-data gap.
        reasoning_tok = int((usage.get("completion_tokens_details") or {}).get("reasoning_tokens", 0) or 0)
        visible_tok = out_tok - reasoning_tok
        cost = (in_tok / 1_000_000) * _INPUT_RATE_PER_M + (out_tok / 1_000_000) * _OUTPUT_RATE_PER_M
        self.total_input_tokens += in_tok
        self.total_output_tokens += out_tok
        self.total_reasoning_tokens += reasoning_tok
        self.total_visible_output_tokens += visible_tok
        self.total_cost_usd += cost

        _logger.info(
            "minimax_call model=%s agent=%s wall=%.2fs tokens_in=%d tokens_out_visible=%d "
            "tokens_out_reasoning=%d cost_usd=%.6f running_total_usd=%.6f",
            self.model_id, self.player_id, time.time() - t0, in_tok, visible_tok,
            reasoning_tok, cost, self.total_cost_usd,
        )

        choices = body.get("choices") or []
        message = choices[0].get("message", {}) if choices else {}
        content = message.get("content", "") or ""

        # Sample every Nth call's full reasoning_content — see __init__
        # comment. reasoning_content is absent (not just empty) when
        # thinking_mode="disabled", so .get() naturally yields "" there.
        if self._call_count % self.reasoning_sample_every == 0:
            reasoning_content = message.get("reasoning_content", "") or ""
            self.reasoning_samples.append({
                "call_index": self._call_count,
                "reasoning_content": reasoning_content,
                "content": content,
            })
            if reasoning_content:
                _logger.info(
                    "minimax_reasoning_sample model=%s agent=%s call=%d reasoning=%r",
                    self.model_id, self.player_id, self._call_count, reasoning_content[:500],
                )

        return content
