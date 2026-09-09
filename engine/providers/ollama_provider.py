from __future__ import annotations
import logging
import time
import requests
from .base import AIProvider
from engine.ollama_queue import get_queue  # per-host registry (D1 dual-queue)

# HM-CN 2026-05-17: latency telemetry for ministral-3:3b post-HM-BN.1 + future bakeoffs.
# Routes to trader_error.log via stdlib logger (per HM-LOG-CHANNEL doctrine).
# Parser format: "ollama_call model=<m> agent=<a> wall=<s>s queue_wait=<s>s model_time=<s>s"
# (queue_wait/model_time added HM-OLLIE-QUEUE-CONTENTION-2026-09-09; "n/a" if
# the queue timing dict wasn't populated, e.g. on a cancel-path exception)
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

# HM-BRIDGE-WEDGE-2 2026-06-11: explicit (connect, read) timeout. A bare float
# bounds both, but a tight CONNECT budget makes an unreachable Ollie Max
# (.166/.168 down, route flap) fail fast instead of consuming the full read
# budget before erroring — the call can never block indefinitely either way.
_HM_OLLAMA_CONNECT_TIMEOUT_S = 5

# HM-OLLAMA-ALIAS 2026-08-27: plutus-v1, ministral-3:3b, and (added
# 2026-08-27 ~7:50, manually `ollama cp qwen3:8b qwen2.5-coder:7b`)
# qwen2.5-coder:7b are CURRENTLY aliases of qwen3:8b (same weights, ID
# 500a1f067a9f — see project memory project_ollama_model_aliases_2026-08-25)
# and need the same thinking-mode suppression as native qwen3:* tags below,
# or War Room's witness arm, McCoy (ollama-plutus, model plutus-v1), and
# Data (ollama-coder, model qwen2.5-coder:7b) leak <think> tokens through
# this path. qwen3:4b is already covered by the startswith("qwen3") check.
# REVISIT AT UN-ALIASING (target 2026-09-04): once the roster gets real,
# distinct models back, plutus-v1 becomes a Llama-based model and
# qwen2.5-coder:7b becomes the real (non-thinking) Qwen2.5-Coder — sending
# think:false to a non-thinking model can error on some Ollama versions.
# Shrink this set back to genuine qwen3 tags only once that happens.
_QWEN3_ALIAS_MODEL_IDS = {
    "plutus-v1", "plutus-v1:latest", "ministral-3:3b", "qwen2.5-coder:7b",
}

# HM-OLLIE-30B-THINK-LEAK 2026-09-09: the API-level "think": False field
# (below) is confirmed NOT honored for qwen3:30b-a3b -- direct /api/generate
# probe against olliemax with think:false explicitly set returned no separate
# "thinking" key at all; the full chain-of-thought was baked straight into
# "response" on every call (6/6 in testing: 5 real McCoy War Room prompts +
# 1 raw probe). This is the qwen3moe (MoE) family template, distinct from the
# dense qwen3:8b/14b tags the API flag was verified against on 2026-04-27.
# scout_critic.py and mlx_provider.py already use the template-level
# "/no_think" prompt token for the same reason (Qwen3's chat template always
# honors this in-band control, independent of Ollama's API-level support for
# a given model). Applying the same fix here, scoped to this one model until
# broader qwen3moe API support is verified. IMPACT: any McCoy/Troi/Worf
# decision output from qwen3:30b-a3b between the 2026-09-08 21:18 cutover and
# this fix (2026-09-09 ~07:55 confirmation) carried leaked CoT instead of a
# clean take -- treat that window's War Room output as suspect.
_NO_THINK_PROMPT_MODELS = {"qwen3:30b-a3b"}

# HM-OLLIE-30B-CUTOVER 2026-09-08: num_ctx was a single global constant
# (10240, sized off qwen3:8b's p95 real-traffic token usage) applied to
# every model regardless of size. qwen3:30b-a3b needs more headroom -- last
# 24h actual input+output tokens across McCoy/Troi/Worf run p50=9429,
# max=9727, so 8192 would truncate the median call outright. 16384 covers
# the observed max with room; live-verified on olliemax at that context
# length: 18.4GB VRAM for the model alone, ~3GB+ still free (of ~21.5GB
# across both cards), and bge-m3 stays co-resident (77MB) rather than being
# evicted. Per-model override, default unchanged for every other model.
_NUM_CTX_OVERRIDES = {
    "qwen3:30b-a3b": 16384,
    # HM-OLLIE-30B-LIVE-2026-09-09: instruct-2507 build, same context sizing
    # rationale as the thinking build above (same real-traffic token
    # distribution across McCoy/Troi/Worf) -- this is the tag actually live
    # now, the thinking tag above is retired from any fleet seat.
    "qwen3:30b-a3b-instruct-2507-q4_K_M": 16384,
}
_DEFAULT_NUM_CTX = 10240


def _num_ctx_for(model_id: str) -> int:
    return _NUM_CTX_OVERRIDES.get(model_id, _DEFAULT_NUM_CTX)

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
        # Route through the per-host queue (engine.ollama_queue) — up to
        # OLLAMA_QUEUE_WORKERS concurrent inferences system-wide (default 2
        # as of HM-PERF-FLEET-THROUGHPUT 2026-07-07; was "one at a time"
        # premised on a since-corrected hardware assumption, see below).
        # HM-WR-VRAM-THRASHING 2026-05-20 (Fix 4): keep_alive raised 45s → 10m.
        # Per project_hm_wr_provider_latency: WR cycle wall 19m 35s with 8 LLM
        # providers 91-202s each due to VRAM model-swap thrashing. Combined
        # with Fix 3 (model batching), 10m residency means same-model agents
        # within a WR cycle reuse loaded weights instead of re-loading on
        # each call. The earlier 45s compromise was tuned for 16GB Mac Mini
        # RAM contention; Ollie Max — RTX 5080, 16GB VRAM, corrected
        # 2026-05-28 HM-AUDIT-T0 (this comment previously said "RTX 5060,"
        # which was wrong) — does not have the same stacking concern so the
        # longer keep_alive is safe.
        # HM-OLLIE-30B-THINK-LEAK 2026-09-09: see _NO_THINK_PROMPT_MODELS above --
        # the API "think" field alone doesn't suppress CoT for this model.
        if self.model_id in _NO_THINK_PROMPT_MODELS:
            prompt = "/no_think\n" + prompt

        payload = {
            "model": self.model_id,
            "prompt": prompt,
            "stream": False,
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": self._temperature,
                # HM-PERF-FLEET-THROUGHPUT 2026-07-07: was uncapped (Ollama's
                # model-default context, unbounded VRAM variable — the main
                # risk once >1 concurrent slot exists). Measured against
                # real traffic first, not guessed: api_costs.input_tokens +
                # output_tokens for call_type='scan' across the 5 active
                # Ollama agents, last 30 days, n=5,845 — p50=7,698,
                # p95=8,719, p99=31,977 (qwen3 thinking-mode leakage tail,
                # not the normal distribution — see the think:False guard
                # below, which doesn't reliably suppress it for every
                # response). NUM_CTX=10240 covers p95 with ~17% headroom;
                # deliberately not sized to the p99 tail, which would cost
                # 3-4x the VRAM per slot to protect <1% of calls that are
                # already anomalous. A call that would have exceeded this
                # now degrades (truncated context) instead of the model
                # generating unboundedly — an acceptable trade given the
                # goal here is predictable per-slot VRAM for 2-worker
                # co-residency, not zero-truncation guarantees.
                "num_ctx": _num_ctx_for(self.model_id),
            },
        }
        # 2026-04-27: qwen3 family streams chain-of-thought tokens before JSON,
        # which blew the 180s timeout for qwen3-14b-pro (Dalio, 47% timeout rate)
        # and qwen3-8b-flash (Worf, 22%). debate_engine.py was patched 04-26 but
        # this provider path was missed. Disable thinking for qwen3 only —
        # other model families ignore the flag harmlessly. 2026-08-27: also
        # covers the current qwen3-aliased tags — see _QWEN3_ALIAS_MODEL_IDS
        # comment above for the un-aliasing revisit date.
        if self.model_id.startswith("qwen3") or self.model_id in _QWEN3_ALIAS_MODEL_IDS:
            payload["think"] = False

        def _do_request() -> str:
            r = requests.post(self.url, json=payload,
                              timeout=(_HM_OLLAMA_CONNECT_TIMEOUT_S, self.timeout))
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
        # HM-OLLIE-QUEUE-CONTENTION-2026-09-09: split via OllamaQueue.submit's
        # timing dict so contention is measured directly, not inferred from
        # direct-call vs production average comparisons.
        t0 = time.time()
        _timing: dict = {}
        try:
            result = get_queue(self.url).submit(_do_request, model_id=self.model_id, timing=_timing)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # HM-WR-CANCEL-ON-TIMEOUT 2026-05-21: log the cancellation path so we
            # can observe in trader_error.log that the socket was closed and the
            # queue slot freed before WR's outer 90s budget tripped.
            _latency_logger.warning(
                "[OLLAMA-CANCEL] model=%s agent=%s wall=%.2fs reason=%s",
                self.model_id, self.player_id, time.time() - t0, type(e).__name__,
            )
            raise
        _qw = _timing.get("queue_wait_s")
        _mt = _timing.get("model_time_s")
        _latency_logger.info(
            "ollama_call model=%s agent=%s wall=%.2fs queue_wait=%s model_time=%s",
            self.model_id, self.player_id, time.time() - t0,
            f"{_qw:.2f}s" if _qw is not None else "n/a",
            f"{_mt:.2f}s" if _mt is not None else "n/a",
        )
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
