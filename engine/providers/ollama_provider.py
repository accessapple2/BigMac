from __future__ import annotations
import logging
import time
import requests
from .base import AIProvider
from engine.ollama_queue import get_queue  # per-host registry (D1 dual-queue)

# HM-CN 2026-05-17: latency telemetry for ministral-3:3b post-HM-BN.1 + future bakeoffs.
# Routes to trader_error.log via stdlib logger (per HM-LOG-CHANNEL doctrine).
# Parser format: "ollama_call model=<m> agent=<a> wall=<s>s"
# Explicit INFO level — root logger defaults to WARNING in this codebase.
_latency_logger = logging.getLogger("ollama_provider")
_latency_logger.setLevel(logging.INFO)


class OllamaProvider(AIProvider):
    def __init__(self, player_id: str = "ollama-local", model: str = "qwen3:14b",
                 url: str = "http://localhost:11434", timeout: int = 180):
        super().__init__(player_id, f"Ollama {model}", model, rate_limit=999)
        self.url = f"{url}/api/generate"
        self.timeout = timeout
        self._is_cloud = ":cloud" in model
        self._temperature = 0.6 if self._is_cloud else 0.7

    def call_model(self, prompt: str) -> str:
        # Route through global FIFO queue — one Ollama inference at a time system-wide.
        # HM-WR-VRAM-THRASHING 2026-05-20 (Fix 4): keep_alive raised 45s → 10m.
        # Per project_hm_wr_provider_latency: WR cycle wall 19m 35s with 8 LLM
        # providers 91-202s each due to VRAM model-swap thrashing on RTX 5060 8GB.
        # Combined with Fix 3 (model batching), 10m residency means same-model
        # agents within a WR cycle reuse loaded weights instead of re-loading on
        # each call. The earlier 45s compromise was tuned for 16GB Mac Mini RAM
        # contention; canonical Ollie Box (Linux + RTX 5060) does not have the
        # same stacking concern so the longer keep_alive is safe.
        payload = {
            "model": self.model_id,
            "prompt": prompt,
            "stream": False,
            "keep_alive": "10m",
            "options": {"temperature": self._temperature},
        }
        # 2026-04-27: qwen3 family streams chain-of-thought tokens before JSON,
        # which blew the 180s timeout for qwen3-14b-pro (Dalio, 47% timeout rate)
        # and qwen3-8b-flash (Worf, 22%). debate_engine.py was patched 04-26 but
        # this provider path was missed. Disable thinking for qwen3 only —
        # other model families ignore the flag harmlessly.
        if self.model_id.startswith("qwen3"):
            payload["think"] = False

        def _do_request() -> str:
            r = requests.post(self.url, json=payload, timeout=self.timeout)
            return r.json().get("response", "")

        # HM-CN 2026-05-17: time the queue submit (queue wait + Ollama inference).
        t0 = time.time()
        result = get_queue(self.url).submit(_do_request, model_id=self.model_id)
        _latency_logger.info("ollama_call model=%s agent=%s wall=%.2fs", self.model_id, self.player_id, time.time() - t0)
        return result

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
