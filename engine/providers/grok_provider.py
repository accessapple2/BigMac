from __future__ import annotations
import os
import requests
from .base import AIProvider

# HM-Q-WARROOM 2026-06-06: GrokProvider restored to the REAL xAI client for Q
# (q-witness) — the War Room's one Admiral-approved paid debate voice. It hits
# api.x.ai (OpenAI-compatible), logs the EXACT cost from usage.cost_in_usd_ticks,
# and gracefully degrades to local Ollama (Ollie GPU qwen3:8b) on ANY xAI failure
# OR once the per-agent daily cost cap is hit — so the debate never blocks.
# Advisory/voice only (q-witness.can_trade_live=0). The model string + cost cap
# are env-overridable; Q_ENABLED (in agent_routing) is the kill switch.

XAI_BASE_URL = "https://api.x.ai/v1"
_FALLBACK_IN_RATE_PER_M = 1.25   # only if a response ever lacks cost_in_usd_ticks
_FALLBACK_OUT_RATE_PER_M = 2.50


class GrokProvider(AIProvider):
    """Grok (xAI) War Room debate voice with local-Ollama graceful fallback."""

    def __init__(self, api_key: str = "", player_id: str = "q-witness",
                 model: str = "grok-4.20-0309-non-reasoning", display_name: str = "Q",
                 use_xai: bool = True, daily_cap: float | None = None):
        super().__init__(player_id, display_name, model, rate_limit=30)
        self._api_key = api_key or os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
        self._model = model
        self._use_xai = bool(use_xai) and bool(self._api_key)
        self._daily_cap = float(daily_cap) if daily_cap is not None else float(
            os.getenv("Q_DAILY_COST_CAP", "0.50"))
        # Local fallback (Ollie GPU) — same target the gutted provider used.
        from config import OLLIE_URL as _ollie_url
        self._ollama_url = _ollie_url
        self._ollama_model = os.getenv("CREWAI_MODEL", "qwen3:8b")
        # Tell war_room.generate_hot_take to defer cost logging to us (exact ticks).
        self._logs_own_cost = True
        self._last_source = "none"   # 'xai' | 'ollama' — observability/smoke

    def _over_cap(self) -> bool:
        try:
            from engine.cost_tracker import get_daily_costs
            spent = float((get_daily_costs() or {}).get(self.player_id, 0.0) or 0.0)
            return spent >= self._daily_cap
        except Exception:
            return False

    def _call_ollama(self, prompt: str) -> str:
        self._last_source = "ollama"
        resp = requests.post(
            self._ollama_url + "/api/generate",
            json={"model": self._ollama_model, "prompt": prompt, "stream": False},
            timeout=90,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")

    def call_model(self, prompt: str) -> str:
        # Graceful degrade to local Ollama if xAI is disabled or the cap is hit.
        if not self._use_xai or self._over_cap():
            return self._call_ollama(prompt)
        try:
            resp = requests.post(
                f"{XAI_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}",
                         "Content-Type": "application/json"},
                # Plain chat completion — NO tools / web search. The persona is
                # already baked into the War Room prompt, sent as the user message.
                json={"model": self._model,
                      "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.4, "max_tokens": 400},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {}) or {}
            in_tok = usage.get("prompt_tokens") or (len(prompt) // 4)
            out_tok = usage.get("completion_tokens") or (len(content) // 4)
            ticks = usage.get("cost_in_usd_ticks")
            exact = (float(ticks) * 1e-10) if ticks is not None else (
                in_tok / 1e6 * _FALLBACK_IN_RATE_PER_M + out_tok / 1e6 * _FALLBACK_OUT_RATE_PER_M)
            try:
                from engine.cost_tracker import log_cost_exact
                log_cost_exact(self.player_id, "war_room", in_tok, out_tok, exact)
            except Exception:
                pass
            self._last_source = "xai"
            return content
        except Exception:
            # Any xAI error → local fallback (no paid cost). Never block the debate.
            return self._call_ollama(prompt)
