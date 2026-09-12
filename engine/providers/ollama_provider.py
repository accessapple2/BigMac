from __future__ import annotations
import logging
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
#
# HM-OLLIE-STALE-SOCKET-2026-09-10 CORRECTION: this constant is only the
# __init__ default for callers that don't pass an explicit `timeout=` —
# it was NOT the live production value despite reading as if it were. The
# real scan-path Arena (main.py::initialize_arena -> build_all_providers)
# passed default_timeout=180 explicitly, overriding this. That 180s was the
# actual budget behind today's stale-pooled-socket incident (52 [OLLAMA-
# CANCEL] cancellations, all wall~180.01s, logs/trader_error.log) and cost
# a full scan cycle per hang.
#
# SECOND-OPINION REVISION (same day): both this and main.py's call site now
# read config.OLLAMA_GENERATE_TIMEOUT_S -- one named source instead of two
# independently-hardcoded literals that silently drifted (85 vs 180) with
# no single place to check which was live. See config.py's comment and
# main.py's Arena wiring for the other half.
from config import OLLAMA_GENERATE_TIMEOUT_S as _HM_WR_CANCEL_BUDGET_S
from config import OLLAMA_CONNECT_RETRY_TOTAL as _CONNECT_RETRY_TOTAL

# HM-BRIDGE-WEDGE-2 2026-06-11: explicit (connect, read) timeout. A bare float
# bounds both, but a tight CONNECT budget makes an unreachable Ollie Max
# (.166/.168 down, route flap) fail fast instead of consuming the full read
# budget before erroring — the call can never block indefinitely either way.
_HM_OLLAMA_CONNECT_TIMEOUT_S = 5

# HM-OLLIE-STALE-SOCKET-2026-09-10: root cause was a dead pooled keep-alive
# connection reused after an olliemax runner reschedule (09:32 UTC) tore
# down existing sockets without the client detecting it -- the write onto
# the dead socket didn't error immediately, so the call just blocked in
# recv() for the full read-timeout budget with nothing ever coming back
# (mechanism: HTTPConnectionPool(host='100.95.195.20', port=11434): Read
# timed out).
#
# SECOND-OPINION REVISION (same day): the first cut of this fix mounted a
# SHARED, persistent Session -- which reintroduces exactly the class of bug
# this incident exposed (a keep-alive connection sitting idle across calls,
# vulnerable to being silently killed server-side between uses). Don't pool
# at all for /api/generate: at this call volume (a handful of calls/min
# across the whole fleet) a fresh TCP handshake per call costs nothing next
# to multi-second generation time, and removes the failure class
# structurally instead of mitigating it. _RETRY_ADAPTER_FACTORY below is
# still mounted per call -- for genuine connect-phase failures WITHIN that
# one call (DNS, refused, connect timeout), never for reuse across calls,
# which no longer happens. read=0 is deliberate and load-bearing, NOT an
# oversight: a read-timeout is indistinguishable, from the client side,
# between "the pooled socket was already dead" and "the model is just
# genuinely slow this call" -- /api/generate has no idempotency key, so
# auto-retrying a read-timeout risks firing a second real generation for
# one that may still be in flight server-side. status=0/total=connect_total
# keep this narrowly scoped to the connect-failure case, not a general
# retry-everything policy.
#
# urllib3 2.x's Retry defaults `allowed_methods` to the idempotent-only set
# (GET/HEAD/PUT/DELETE/OPTIONS/TRACE) -- POST is excluded by design. Without
# explicitly re-including it here, connect=N above would silently never
# fire for this module's only verb and the whole mount would be a no-op.
_retry = Retry(
    total=_CONNECT_RETRY_TOTAL, connect=_CONNECT_RETRY_TOTAL, read=0, status=0,
    allowed_methods=frozenset({"POST"}),
)

_latency_logger.info(
    "[OLLAMA-PROVIDER-CONFIG] generate_timeout=%ss connect_timeout=%ss "
    "connect_retry_total=%s pooling=disabled (fresh connection per call)",
    _HM_WR_CANCEL_BUDGET_S, _HM_OLLAMA_CONNECT_TIMEOUT_S, _CONNECT_RETRY_TOTAL,
)

# HM-OLLAMA-ALIAS 2026-08-27: plutus-v1, ministral-3:3b, and (added
# 2026-08-27 ~7:50, manually `ollama cp qwen3:8b qwen2.5-coder:7b`)
# qwen2.5-coder:7b are CURRENTLY aliases of qwen3:8b (same weights, ID
# 500a1f067a9f — see project memory project_ollama_model_aliases_2026-08-25)
# and need the same thinking-mode suppression as native qwen3:* tags below,
# or War Room's witness arm, McCoy (ollama-plutus, model plutus-v1), and
# Data (ollama-coder, model qwen2.5-coder:7b) leak <think> tokens through
# this path. qwen3:4b is already covered by the startswith("qwen3") check.
#
# STILL LIVE — RE-VERIFIED 2026-09-10, DO NOT SHRINK: the original comment
# targeted un-aliasing by 2026-09-04. That date passed with the aliasing
# fully intact -- live digest check against olliemax's /api/tags on
# 2026-09-10 shows plutus-v1, plutus-v1:latest, ministral-3:3b, qwen3:4b,
# and qwen2.5-coder:7b ALL share digest f112024b4d65a1ec6a84... (identical
# to qwen3:8b's own digest) -- same weights, not just same size. Shrinking
# this set on the assumption un-aliasing happened would silently
# reintroduce the <think>-leak bug for ollama-plutus (McCoy, the only
# still-active seat among these -- everything else on this alias list is
# halt_mode='full'). Only `plutus-v1-real` (restored HM-PLUTUS-V5-WIN
# checkpoint, digest 06148c3401a6d74bab0c, family qwen2) and
# `0xroyce/plutus` (digest 0bf0c307b6a4a673f8ee, family llama) are
# genuinely distinct models today -- neither is in this set, correctly.
# Re-verify digests before ever shrinking this set; do not trust a target
# date alone. See docs/orders/... alias-era note and CLAUDE.md (both
# copies) for the same correction.
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
    # now, the thinking tag above is retired from any fleet seat. NOTE
    # 2026-09-11: both 30B tags reverted out of every active seat the same
    # day this override was written (HM-OLLIE-30B-BAKEOFF-REVERT) -- McCoy
    # and Worf are back on 8B. These entries are inert today but harmless;
    # left in place rather than removed since a future 30B re-bakeoff would
    # need the same sizing.
    "qwen3:30b-a3b-instruct-2507-q4_K_M": 16384,
    # HM-OLLIE-SILENT-SEAT-2026-09-12: gemma3:4b and phi3:mini are the two
    # small "resident" models outside the qwen3-weight family (see
    # _QWEN3_ALIAS_MODEL_IDS) -- the 24576 default below is sized for
    # qwen3:8b's real prompt distribution and is unnecessary VRAM for these
    # two. Sized instead for Riker's crew-intelligence synthesis prompt
    # (engine/riker_xo.py, ~8.2K tokens observed) plus headroom: 12288
    # covers that with ~4K to spare. Before this override existed, three
    # independent callers (agents/sarek.py, janeway.py, surak.py) each
    # hardcoded "num_ctx": 4096 locally instead of importing a shared value,
    # and engine/riker_xo.py sent no num_ctx at all (inheriting whatever
    # context happened to be resident) -- both are the same underlying bug:
    # a caller that doesn't state its own context requirement silently
    # reconfigures a live seat for whoever runs next. Confirmed live
    # 2026-09-12 05:45-05:52: Surak's daily brief loaded gemma3:4b at 4096,
    # and Riker's next 10-min tick rode on that same undersized seat purely
    # by scheduling accident -- no truncation that time only because no
    # Riker prompt that cycle needed more than 4096 tokens of room. All four
    # callers now route through num_ctx_for() below instead of a literal.
    "gemma3:4b": 12288,
    "phi3:mini": 12288,
}
# HM-OLLIE-TRUNCATION-2026-09-11: was 10240, sized off qwen3:8b's p95 from
# HM-PERF-FLEET-THROUGHPUT (2026-07-07) -- that p95 no longer reflects real
# traffic. McCoy's screened-scan prompts actually run 15,455-17,942 tokens
# (median 16,875), and this default silently truncated 4,575 real prompts
# to ~30% of their content between 2026-09-09 and 2026-09-11 with no error
# raised anywhere (Ollama logs a WARN to its own journal, not ours, and
# returns a normal-looking 200) -- see olliemax's ~/modelworks/fleet_checks/
# ollama_churn/FINDINGS_FOR_SCOTTY.md for the full trace + a 4,575-row CSV
# of every truncation. Every caller that doesn't have an explicit entry in
# _NUM_CTX_OVERRIDES above shares this one default -- that's McCoy
# (ollama-plutus, model_id "plutus-v1", the Arena's screened scans), Worf
# (qwen3-8b-flash, model_id literally "qwen3:8b" -- same weights as McCoy's
# alias, different tag name, also active, also hit this default and was the
# "other caller" running qwen3:8b-named loads at 10240 alongside McCoy's
# plutus-v1-named ones), and the McCoy-bakeoff arms (qwen3:8b / fin-r1 /
# plutus-v1-real in scripts/mccoy_bakeoff_arms.py + the readiness-check
# script, which smoke-tests fin-r1 against McCoy's real, full-size prompt).
# Raised to 24576 to match what olliemax's qwen3-weight tags (plutus-v1,
# qwen3:8b, qwen2.5-coder:7b, qwen3:4b, ministral-3:3b -- all five are
# byte-identical, see _QWEN3_ALIAS_MODEL_IDS above) already carry as their
# own Modelfile num_ctx -- 24,576 covers the largest real prompt seen
# (17,942) plus ~6.6K of output headroom. Sending an explicit value that
# matches the tag's own default (rather than omitting num_ctx and trusting
# whatever the tag happens to be set to) keeps the original HM-PERF-FLEET-
# THROUGHPUT intent -- a known, predictable per-slot VRAM budget for
# 2-worker co-residency -- while actually covering real traffic instead of
# a stale p95. This also ends the runner-restart churn documented in the
# same findings doc (Priority 3): the runner reloads when a request's
# context differs from the currently-loaded one, and 50 of 52 back-to-back
# restarts on the qwen3 weights were exactly this default (10240) fighting
# the tags' own configured 24576, not a real model swap.
_DEFAULT_NUM_CTX = 24576


def _num_ctx_for(model_id: str) -> int:
    return _NUM_CTX_OVERRIDES.get(model_id, _DEFAULT_NUM_CTX)


def num_ctx_for(model_id: str) -> int:
    """Public accessor -- the one shared source of a model's configured seat
    context size. Every caller that hits olliemax's /api/generate for a
    given model_id should source num_ctx from here rather than a local
    literal (see HM-OLLIE-SILENT-SEAT-2026-09-12 above)."""
    return _num_ctx_for(model_id)


class UndersizedNumCtxError(ValueError):
    """Raised by require_num_ctx() -- a caller asked for less context than
    the model's configured seat size, which would silently shrink a live
    seat for whoever runs next."""


def require_num_ctx(model_id: str, requested_num_ctx: int) -> int:
    """HM-OLLIE-SILENT-SEAT-2026-09-12 systemic guard: fail loudly, before
    the request goes out, if a caller asks for less context than the
    model's configured seat size. Same shape as the Friday done_reason==
    "length" guard in engine/riker_xo.py -- the bug there wasn't that a
    call could truncate, it's that truncation was silent. This is the same
    fix one step earlier: a caller that doesn't state its real requirement
    (or hardcodes a stale/too-small literal) must not be allowed to run
    quietly -- it must error, not degrade the seat for the next caller.
    Returns requested_num_ctx unchanged when it's sufficient, so call sites
    can wrap their options value: "num_ctx": require_num_ctx(model_id, N).
    """
    expected = _num_ctx_for(model_id)
    if requested_num_ctx < expected:
        raise UndersizedNumCtxError(
            f"num_ctx={requested_num_ctx} requested for model {model_id!r} is "
            f"below its configured seat size ({expected}). This would silently "
            f"reconfigure a live olliemax seat for whoever runs next -- use "
            f"engine.providers.ollama_provider.num_ctx_for({model_id!r}) instead "
            f"of a hardcoded or omitted value."
        )
    return requested_num_ctx

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
                 keep_alive: str | None = None):
        super().__init__(player_id, f"Ollama {model}", model, rate_limit=999)
        self.url = f"{url}/api/generate"
        self.timeout = timeout
        # HM-OLLIE-KEEPALIVE-SERVER-DEFAULT 2026-09-09: was a blanket "10m"
        # default (HM-FORGE P1.2) sent on every single call, silently
        # overriding the server's own OLLAMA_KEEP_ALIVE=-1 (never unload) on
        # a per-request basis -- Ollama's API keep_alive field always wins
        # over the server default when present. Default is now None, which
        # omits the field entirely (see call_model()) so the server's own
        # -1 holds for the common case. Callers that need an explicit
        # override (e.g. the report-only WR witness's "0s", so a non-fleet
        # model unloads right after its single call and never pins
        # co-resident VRAM) still pass one explicitly -- that path is
        # unchanged.
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
            "options": {
                "temperature": self._temperature,
                # HM-PERF-FLEET-THROUGHPUT 2026-07-07: was uncapped (Ollama's
                # model-default context, unbounded VRAM variable — the main
                # risk once >1 concurrent slot exists), then capped at 10240
                # off a stale p95 that undercounted McCoy's real screened-scan
                # prompt size (15,455-17,942 tokens) -- see _DEFAULT_NUM_CTX
                # above for the 2026-09-11 correction to 24576 and the
                # truncation incident that surfaced it. A call that still
                # exceeds this now degrades (truncated context) instead of
                # the model generating unboundedly — an acceptable trade
                # given the goal here is a known, predictable per-slot VRAM
                # budget for 2-worker co-residency, not a zero-truncation
                # guarantee against every future prompt-size drift.
                "num_ctx": _num_ctx_for(self.model_id),
            },
        }
        if self.keep_alive is not None:
            payload["keep_alive"] = self.keep_alive
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
            # HM-OLLIE-STALE-SOCKET-2026-09-10 (second-opinion revision):
            # fresh Session + HTTPAdapter per call, closed immediately after
            # -- no cross-call connection reuse, so there's no pooled socket
            # left around for a later call to inherit in a dead state. See
            # the module-level comment above for why this replaced the
            # earlier shared-Session revision. Connection: close is sent
            # explicitly too, so Ollama itself doesn't hold the socket open
            # expecting a reuse that will never come.
            with requests.Session() as _s:
                _s.mount("http://", HTTPAdapter(max_retries=_retry))
                _s.mount("https://", HTTPAdapter(max_retries=_retry))
                r = _s.post(self.url, json=payload,
                            headers={"Connection": "close"},
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
            # HM-OLLIE-STALE-SOCKET-2026-09-10: this line already reaches
            # trader_error.log today (stdlib logger -> Logging Sink Split,
            # docs/runbooks/logging.md) -- 52 of these on 2026-09-10, all
            # traceable to the dead-pooled-socket incident. Was logging only
            # type(e).__name__; added the actual exception text (%s on e)
            # so a log-only reader (a sentinel check, a human grepping
            # trader_error.log) doesn't have to cross-reference decision_audit
            # to see the underlying "Read timed out" / host:port detail.
            #
            # SECOND-OPINION REVISION (same day): no pool-discard step here
            # anymore -- _do_request() no longer holds a connection across
            # calls at all (fresh Session per call, closed immediately after
            # use), so there's nothing left to discard. Still deliberately
            # NOT a retry-and-resend of THIS failed call on a read-timeout:
            # a read-timeout can't be told apart, client-side, from a
            # genuinely slow in-flight generate, and /api/generate has no
            # idempotency key -- resending risks firing a second real
            # generation on top of one that may still be running
            # server-side.
            _latency_logger.warning(
                "[OLLAMA-CANCEL] model=%s agent=%s wall=%.2fs reason=%s detail=%s",
                self.model_id, self.player_id, time.time() - t0, type(e).__name__, e,
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
