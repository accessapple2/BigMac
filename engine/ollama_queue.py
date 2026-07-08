"""Ollama two-lane fair queue — serializes/parallelizes Ollama inference per host.

HM-PERF-FLEET-THROUGHPUT (2026-07-07): Ollie Max is an RTX 5080, 16GB VRAM
(corrected 2026-05-28 HM-AUDIT-T0 — this docstring previously said "RTX 5060,
8GB, one model fits," which was wrong and drove the original one-worker
design; see docs/runbooks/ram-discipline.md for the live-verified budget:
TWO 7-8B-class models fully co-resident, ~10-12GB together). Server-side
NUM_PARALLEL=2 / MAX_LOADED_MODELS=2 let the host actually serve 2 concurrent
requests, but the client queue was still serializing to exactly 1 in-flight
inference system-wide, leaving that throughput on the table. The queue now
runs OLLAMA_QUEUE_WORKERS (config.py, default 2) worker threads. Feature-
flagged: set OLLAMA_QUEUE_WORKERS=1 to roll back to the original fully-serial
behavior — with 1 worker, the model-affinity bookkeeping below collapses to
tracking exactly one resident model, identical to the pre-2026-07-07 design.

HM-TIER3-SIGNAL-DROP (2026-05-28): the worker(s) do not serve strict FIFO.
War Room enqueues bursts of latency-tolerant "bulk" calls that used to park
scan-path agent calls behind the whole burst, blowing the scan budget and
leaving 8/9 LLM agents silent for ~3 weeks. Work is split into two lanes —
``scan`` (interactive, must get a timely turn to persist a signal) and
``wr`` (War Room bulk debate) — and scheduled fairly:

  * scan-priority by default (a scan call waits behind at most one in-flight
    WR inference, not the whole burst);
  * a configurable anti-starvation cap (``WR_ANTI_STARVE_K``) guarantees WR
    still drains, and a symmetric cap prevents the affinity rules below from
    ever starving scan;
  * model-affinity tiebreak, two tiers:
      1. ACTIVE affinity (new 2026-07-07): prefer the lane whose head task
         matches a model another worker is executing RIGHT NOW — this is
         what actually captures the NUM_PARALLEL=2 win (two concurrent
         requests against one already-loaded model copy).
      2. RESIDENT affinity (was: single _resident_model string; now an
         LRU set sized to OLLAMA_QUEUE_WORKERS, matching MAX_LOADED_MODELS):
         prefer the lane whose head task matches a recently-dispatched
         model that's probably still loaded, even if nothing is actively
         running it this instant.
  * every model swap (dispatching a model outside BOTH the active and
    resident sets, when the resident set is already at capacity) is logged
    ([OLLAMA-QUEUE-SWAP]) so thrash is observable.

Lane is auto-detected from the calling thread name (War Room runs providers on
``wr_provider_pool`` threads — see war_room._WR_PROVIDER_POOL), so existing
call sites need no changes.
"""
from __future__ import annotations

import collections
import threading
import time
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)
# Root logger defaults to WARNING in this codebase; raise to INFO so the
# [OLLAMA-QUEUE-SWAP] observability lines actually emit (mirrors ollama_provider).
logger.setLevel(logging.INFO)

# Per-request timeout (seconds). Exceeded requests are skipped, not retried.
# 2026-04-20: raised 120 → 300 — war_room has many agents queuing simultaneously;
# Ollie cold-loads a 14b model in ~20s, so a late agent could wait 200s+ before
# its slot opens. 300s gives the full queue room to breathe without indefinite blocks.
REQUEST_TIMEOUT = 300

# How long to retain response-time samples for avg calculation.
_MAX_SAMPLES = 50

# HM-TIER3-SIGNAL-DROP 2026-05-28: anti-starvation cap (tunable without a code
# dive). Max consecutive times one lane may be passed over while the other lane
# has work waiting, before the worker forces the starved lane through. Applied
# symmetrically: bounds WR starvation from scan-priority AND bounds scan
# starvation from the model-affinity tiebreaks.
#
# Tuning: lower K = more alternation (scan served sooner, but more model swaps);
# higher K = longer same-lane bursts (fewer swaps, but a *different-model* scan
# call can wait up to K WR inferences). Keep K small enough that
# K × typical_inference_s < REQUEST_TIMEOUT (300s), or a passed-over scan submit
# can time out before it runs. At ~90s/inference, K=3 ⇒ ~270s worst-case wait.
# Same-model scan calls (the qwen3:8b majority) match the resident model, so the
# affinity rules never divert them — they get scan-priority immediately.
WR_ANTI_STARVE_K = 3

_SCAN_LANE = "scan"
_WR_LANE = "wr"
# Must match war_room._WR_PROVIDER_POOL's thread_name_prefix.
_WR_THREAD_PREFIX = "wr_provider_pool"


def _default_num_workers() -> int:
    try:
        import config
        return max(1, int(config.OLLAMA_QUEUE_WORKERS))
    except Exception:
        return 1  # fail-safe: never crash queue construction over a config miss


class _Task:
    """A queued inference call plus its result plumbing."""
    __slots__ = ("fn", "model_id", "lane", "result", "exc", "done")

    def __init__(self, fn: Callable[[], Any], model_id: str, lane: str) -> None:
        self.fn = fn
        self.model_id = model_id
        self.lane = lane
        self.result: Any = None
        self.exc: BaseException | None = None
        self.done = threading.Event()


class OllamaQueue:
    """Thread-safe per-host queue with N workers and two fair lanes."""

    def __init__(self, num_workers: int | None = None) -> None:
        self._lanes: dict[str, "collections.deque[_Task]"] = {
            _SCAN_LANE: collections.deque(),
            _WR_LANE: collections.deque(),
        }
        self._cv = threading.Condition()

        self.num_workers = num_workers if num_workers is not None else _default_num_workers()
        self._workers: list[threading.Thread] = []
        for i in range(self.num_workers):
            t = threading.Thread(
                target=self._worker_loop, daemon=True, name=f"ollama-queue-worker-{i}"
            )
            t.start()
            self._workers.append(t)

        # Scheduler state (mutated only under self._cv)
        # _active_models: model_id -> count of workers executing it RIGHT NOW.
        # _resident_models: LRU set (OrderedDict used as an ordered set) of
        # recently-dispatched distinct models, capped at self.num_workers --
        # deliberately tied to worker count: with 1 worker this collapses to
        # tracking exactly one model, byte-for-byte matching the pre-2026-07-07
        # single-_resident_model design (exact rollback parity at WORKERS=1).
        # Server MAX_LOADED_MODELS=2 means capacities beyond 2 don't buy
        # anything further; num_workers is expected to be 1 or 2 in practice.
        self._active_models: collections.Counter = collections.Counter()
        self._resident_models: "collections.OrderedDict[str, None]" = collections.OrderedDict()
        self._wr_skipped: int = 0        # consecutive times WR lane passed over
        self._scan_skipped: int = 0      # consecutive times scan lane passed over

        # Metrics
        self._last_success_ts: float = 0.0
        self._response_times: list[float] = []
        self._total_requests: int = 0
        self._total_timeouts: int = 0
        self._total_model_swaps: int = 0
        self._current_model: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, fn: Callable[[], Any], model_id: str = "", lane: str | None = None) -> Any:
        """Submit a callable to the queue and block until it completes (or times out).

        Args:
            fn: Zero-argument callable that performs the Ollama HTTP request.
            model_id: Model being called (for metrics, swap detection, logging).
            lane: ``"scan"`` or ``"wr"``. If omitted, auto-detected from the
                calling thread (War Room provider threads → ``"wr"``, else
                ``"scan"``), so existing call sites need no changes.

        Returns:
            Whatever fn() returns.

        Raises:
            TimeoutError: If the call exceeds REQUEST_TIMEOUT seconds.
            Exception: Any exception raised by fn() is re-raised.
        """
        if lane not in (_SCAN_LANE, _WR_LANE):
            lane = (
                _WR_LANE
                if threading.current_thread().name.startswith(_WR_THREAD_PREFIX)
                else _SCAN_LANE
            )

        task = _Task(fn, model_id, lane)
        with self._cv:
            self._lanes[lane].append(task)
            self._total_requests += 1
            self._current_model = model_id
            # notify_all (not notify): with >1 worker, a single notify() only
            # wakes one waiter, which under a burst of near-simultaneous
            # submit() calls can leave a second idle worker asleep even
            # though work is now available. notify_all is the safe choice --
            # every worker re-checks under the lock and only proceeds if
            # _take_next_locked actually finds work, so spurious wakeups are
            # harmless.
            self._cv.notify_all()

        fired = task.done.wait(timeout=REQUEST_TIMEOUT)

        if not fired:
            with self._cv:
                self._total_timeouts += 1
            logger.error(
                "OllamaQueue: request timed out after %ds (model=%s lane=%s)",
                REQUEST_TIMEOUT, model_id, lane,
            )
            raise TimeoutError(
                f"Ollama request timed out after {REQUEST_TIMEOUT}s (model={model_id})"
            )

        if task.exc is not None:
            raise task.exc
        return task.result

    # ------------------------------------------------------------------
    # Scheduler
    # ------------------------------------------------------------------

    def _mark_resident_locked(self, model_id: str) -> None:
        """Move model_id to the most-recently-used end of the resident set,
        evicting the least-recently-used entry if over capacity. Caller must
        hold self._cv."""
        if not model_id:
            return
        if model_id in self._resident_models:
            self._resident_models.move_to_end(model_id)
        else:
            self._resident_models[model_id] = None
        while len(self._resident_models) > max(1, self.num_workers):
            self._resident_models.popitem(last=False)

    def _take_next_locked(self) -> tuple[_Task, bool, str] | None:
        """Pick the next task per the fair policy, for a worker that just
        became free. Caller must hold self._cv. Returns None if both lanes
        are empty (caller should wait).

        Returns (task, is_swap, prev_resident_summary). Updates lane
        counters, _active_models, and _resident_models under the lock; the
        actual inference call happens outside the lock, in the worker.
        """
        s = self._lanes[_SCAN_LANE]
        w = self._lanes[_WR_LANE]

        if not s and not w:
            return None

        if s and not w:
            task = s.popleft()
            self._scan_skipped = 0
        elif w and not s:
            task = w.popleft()
            self._wr_skipped = 0
        else:
            # Both lanes have work — fairness + two-tier model affinity.
            serve_wr: bool
            if self._wr_skipped >= WR_ANTI_STARVE_K:
                serve_wr = True             # WR starved → force WR
            elif self._scan_skipped >= WR_ANTI_STARVE_K:
                serve_wr = False            # scan starved (by affinity) → force scan
            else:
                s_head, w_head = s[0].model_id, w[0].model_id
                s_active = bool(s_head) and self._active_models.get(s_head, 0) > 0
                w_active = bool(w_head) and self._active_models.get(w_head, 0) > 0
                if w_active and not s_active:
                    serve_wr = True         # tier 1: WR head is running RIGHT NOW elsewhere
                elif s_active and not w_active:
                    serve_wr = False        # tier 1: scan head is running RIGHT NOW elsewhere
                else:
                    s_resident = bool(s_head) and s_head in self._resident_models
                    w_resident = bool(w_head) and w_head in self._resident_models
                    if w_resident and not s_resident:
                        serve_wr = True     # tier 2: serving WR avoids a swap
                    else:
                        serve_wr = False    # scan-priority default

            if serve_wr:
                task = w.popleft()
                self._wr_skipped = 0
                self._scan_skipped += 1
            else:
                task = s.popleft()
                self._scan_skipped = 0
                self._wr_skipped += 1

        # A "swap" means dispatching a model that isn't already resident AND
        # the resident set is already full (so this dispatch would evict one
        # of the currently-loaded models) -- not just "differs from the last
        # dispatched task," which with >1 co-resident model would over-log.
        is_swap = bool(
            task.model_id
            and task.model_id not in self._resident_models
            and len(self._resident_models) >= max(1, self.num_workers)
        )
        prev_resident = ",".join(self._resident_models) or "(none)"
        if is_swap:
            self._total_model_swaps += 1
        if task.model_id:
            self._current_model = task.model_id
            self._active_models[task.model_id] += 1
            self._mark_resident_locked(task.model_id)
        return task, is_swap, prev_resident

    def _worker_loop(self) -> None:
        while True:
            try:
                with self._cv:
                    picked = self._take_next_locked()
                    while picked is None:
                        self._cv.wait(timeout=5)
                        picked = self._take_next_locked()
                    task, is_swap, prev_resident = picked
                    swap_no = self._total_model_swaps

                if is_swap:
                    logger.info(
                        "[OLLAMA-QUEUE-SWAP] resident={%s} -> %s (lane=%s swap#%d)",
                        prev_resident, task.model_id, task.lane, swap_no,
                    )

                t0 = time.monotonic()
                try:
                    task.result = task.fn()
                    elapsed = time.monotonic() - t0
                    with self._cv:
                        self._last_success_ts = time.time()
                        self._response_times.append(elapsed)
                        if len(self._response_times) > _MAX_SAMPLES:
                            self._response_times.pop(0)
                except Exception as e:  # noqa: BLE001 — propagated to submit() caller
                    task.exc = e
                finally:
                    with self._cv:
                        if task.model_id:
                            self._active_models[task.model_id] -= 1
                            if self._active_models[task.model_id] <= 0:
                                del self._active_models[task.model_id]
                    task.done.set()
            except Exception as e:  # worker must never die
                logger.error("OllamaQueue worker error: %s", e)

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Return a snapshot of queue health metrics."""
        with self._cv:
            scan_depth = len(self._lanes[_SCAN_LANE])
            wr_depth = len(self._lanes[_WR_LANE])
            avg_rt = (
                round(sum(self._response_times) / len(self._response_times), 2)
                if self._response_times else None
            )
            last_success = self._last_success_ts
            current_model = self._current_model
            total_requests = self._total_requests
            total_timeouts = self._total_timeouts
            total_swaps = self._total_model_swaps
            active_models = dict(self._active_models)
            resident_models = list(self._resident_models)

        age_min: float | None = None
        stale = False
        if last_success > 0:
            age_min = round((time.time() - last_success) / 60, 1)
            stale = age_min > 30  # warn if >30 min since last success

        return {
            "queue_depth": scan_depth + wr_depth,
            "scan_depth": scan_depth,
            "wr_depth": wr_depth,
            "total_requests": total_requests,
            "total_timeouts": total_timeouts,
            "total_model_swaps": total_swaps,
            "avg_response_time_s": avg_rt,
            "last_success_age_min": age_min,
            "stale": stale,
            "current_model": current_model,
            "num_workers": self.num_workers,
            "active_models": active_models,
            "resident_models": resident_models,
            "worker_alive": all(t.is_alive() for t in self._workers),
        }

    def last_success_age_min(self) -> float | None:
        """Return minutes since last successful analysis, or None if never run."""
        with self._cv:
            ts = self._last_success_ts
        if ts == 0.0:
            return None
        return (time.time() - ts) / 60


# ── Per-host queue registry ──────────────────────────────────────────────────
# Each distinct Ollama host (bigmac localhost vs Ollie GPU) gets its own
# independent two-lane queue with its own worker pool. A slow job on Ollie no
# longer blocks jobs on bigmac.
#
# Added 2026-04-20 (D1 dual-queue refactor). ~19 LOC total across two files.

from urllib.parse import urlparse

_queues: dict[str, "OllamaQueue"] = {}
_queues_lock = threading.Lock()


def _host_key(url: str) -> str:
    """Normalise a full Ollama URL to a stable host:port key."""
    p = urlparse(url or "")
    return f"{p.scheme}://{p.netloc}" if p.netloc else "default"


def get_queue(url: str = "") -> OllamaQueue:
    """Return (or lazily create) the per-host OllamaQueue for *url*.

    Each unique host gets its own worker pool.  Callers that omit *url*
    get the "default" singleton (backwards-compatible with any code that
    was not updated to pass a URL).
    """
    key = _host_key(url)
    with _queues_lock:
        if key not in _queues:
            _queues[key] = OllamaQueue()
        return _queues[key]


def get_all_queues_status() -> dict[str, dict]:
    """Aggregate status of every registered host queue — for dashboard."""
    with _queues_lock:
        keys = list(_queues.keys())
    return {key: _queues[key].status() for key in keys}
