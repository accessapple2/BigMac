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

# HM-WR-CANCEL-ON-TIMEOUT 2026-05-21: per-call HTTP timeout, picked to fire
# BEFORE war_room._WR_PROVIDER_TIMEOUT_S (90s). When `requests.post(timeout=…)`
# raises ReadTimeout, urllib3 closes the socket; Ollama detects the disconnect
# and cancels the in-flight inference server-side. The previous 180s default
# left WR-timed-out calls grinding to completion, blocking the OllamaQueue and
# starving subsequent agents in the same WR cycle (project_hm_wr_ollama_queue_starvation).
_HM_WR_CANCEL_BUDGET_S = 85

# HM-MODEL-LOUD 2026-06-01: a missing/failing model must ALARM, not silently return "".
# (How devstral-small-2 etc. went dark — _do_request swallowed the 404 to an empty string.)
# Throttled to ONE NTFY per (model_id) per process lifetime — the fleet inference path is
# high-frequency, so an un-throttled dead model would storm. Same loud contract as the CTO fix.
_alerted_models: set = set()


def _model_failure_alert(model_id: str, player_id: str, err) -> None:
    _latency_logger.error("[OLLAMA-MODEL-FAIL] model=%s agent=%s err=%s", model_id, player_id, err)
    if model_id in _alerted_models:
        return
    _alerted_models.add(model_id)
    try:
        from engine.alert_channels import _send_ntfy
        _send_ntfy("Fleet model FAILED",
                   f"{player_id} model {model_id}: {err} — check ollama on .168",
                   priority="high", tags="rotating_light", topic="ollietrades-admin")
    except Exception:
        pass


class OllamaProvider(AIProvider):
    def __init__(self, player_id: str = "ollama-local", model: str = "qwen3:14b",
                 url: str = "http://localhost:11434",
                 timeout: int = _HM_WR_CANCEL_BUDGET_S,
                 keep_alive: str = "10m"):
        super().__init__(player_id, f"Ollama {model}", model, rate_limit=999)
        self.url = f"{url}/api/generate"
        self.timeout = timeout
        # HM-FORGE P1.2: per-provider keep_alive override. Default "10m" preserves
        # the HM-WR-VRAM-THRASHING Fix-4 residency for fleet agents; the report-only
        # WR witness passes "0s" so a non-fleet model (gemma4:12b-it-qat, 7.4GB)
        # unloads right after its single call and never pins co-resident VRAM.
        self.keep_alive = keep_alive
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
            "keep_alive": self.keep_alive,
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
            # STRUCTURAL: do NOT swallow a missing/failing model to "". A 404 (model not on
            # the host) or an {"error":...} body must alarm + raise, not return empty.
            try:
                r.raise_for_status()
                body = r.json()
            except Exception as e:
                _model_failure_alert(self.model_id, self.player_id, e)
                raise
            err = body.get("error")
            if err:
                _model_failure_alert(self.model_id, self.player_id, err)
                raise RuntimeError(f"ollama error for {self.model_id}: {err}")
            return body.get("response", "")

        # HM-CN 2026-05-17: time the queue submit (queue wait + Ollama inference).
        t0 = time.time()
        try:
            result = get_queue(self.url).submit(_do_request, model_id=self.model_id)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # HM-WR-CANCEL-ON-TIMEOUT 2026-05-21: log the cancellation path so we
            # can observe in trader_error.log that the socket was closed and the
            # queue slot freed before WR's outer 90s budget tripped.
            _latency_logger.warning(
                "[OLLAMA-CANCEL] model=%s agent=%s wall=%.2fs reason=%s",
                self.model_id, self.player_id, time.time() - t0, type(e).__name__,
            )
            raise
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
