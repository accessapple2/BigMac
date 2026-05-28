"""Ollama two-lane fair queue — serializes all Ollama inference calls per host.

On the Ollie Box (RTX 5060, 8GB VRAM) only one 7B-class model fits resident at
a time, so a single worker thread per host runs exactly one inference at a time
(this is intentional — concurrent inference would thrash VRAM).

HM-TIER3-SIGNAL-DROP (2026-05-28): the worker no longer serves strict FIFO.
War Room enqueues bursts of latency-tolerant "bulk" calls that used to park
scan-path agent calls behind the whole burst, blowing the scan budget and
leaving 8/9 LLM agents silent for ~3 weeks. The worker now splits work into two
lanes — ``scan`` (interactive, must get a timely turn to persist a signal) and
``wr`` (War Room bulk debate) — and schedules them fairly:

  * scan-priority by default (a scan call waits behind at most one in-flight
    WR inference, not the whole burst);
  * a configurable anti-starvation cap (``WR_ANTI_STARVE_K``) guarantees WR
    still drains, and a symmetric cap prevents the affinity rule below from
    ever starving scan;
  * a model-affinity tiebreak prefers the lane whose head task matches the
    VRAM-resident model, preserving the HM-WR-VRAM-THRASHING batching win;
  * every model swap is logged ([OLLAMA-QUEUE-SWAP]) so thrash is observable.

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
# starvation from the model-affinity tiebreak.
#
# Tuning: lower K = more alternation (scan served sooner, but more model swaps);
# higher K = longer same-lane bursts (fewer swaps, but a *different-model* scan
# call can wait up to K WR inferences). Keep K small enough that
# K × typical_inference_s < REQUEST_TIMEOUT (300s), or a passed-over scan submit
# can time out before it runs. At ~90s/inference, K=3 ⇒ ~270s worst-case wait.
# Same-model scan calls (the qwen3:8b majority) match the resident model, so the
# affinity rule never diverts them — they get scan-priority immediately.
WR_ANTI_STARVE_K = 3

_SCAN_LANE = "scan"
_WR_LANE = "wr"
# Must match war_room._WR_PROVIDER_POOL's thread_name_prefix.
_WR_THREAD_PREFIX = "wr_provider_pool"


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
    """Thread-safe per-host queue with a single worker and two fair lanes."""

    def __init__(self) -> None:
        self._lanes: dict[str, "collections.deque[_Task]"] = {
            _SCAN_LANE: collections.deque(),
            _WR_LANE: collections.deque(),
        }
        self._cv = threading.Condition()
        self._worker = threading.Thread(
            target=self._worker_loop, daemon=True, name="ollama-queue-worker"
        )
        self._worker.start()

        # Scheduler state (mutated only under self._cv)
        self._resident_model: str = ""   # model the GPU currently has loaded (proxy)
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
            self._cv.notify()

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

    def _take_next_locked(self) -> tuple[_Task, bool, str]:
        """Pick the next task per the fair policy. Caller must hold self._cv.

        Returns (task, is_swap, prev_model). Updates lane counters and the
        resident-model proxy under the lock; the actual model swap (and its log)
        happens when the worker runs the task.
        """
        s = self._lanes[_SCAN_LANE]
        w = self._lanes[_WR_LANE]

        if s and not w:
            task = s.popleft()
            self._scan_skipped = 0
        elif w and not s:
            task = w.popleft()
            self._wr_skipped = 0
        else:
            # Both lanes have work — fairness + model affinity.
            serve_wr: bool
            if self._wr_skipped >= WR_ANTI_STARVE_K:
                serve_wr = True            # WR starved → force WR
            elif self._scan_skipped >= WR_ANTI_STARVE_K:
                serve_wr = False           # scan starved (by affinity) → force scan
            elif (
                self._resident_model
                and w[0].model_id == self._resident_model
                and s[0].model_id != self._resident_model
            ):
                serve_wr = True            # affinity: serving WR avoids a swap
            else:
                serve_wr = False           # scan-priority default

            if serve_wr:
                task = w.popleft()
                self._wr_skipped = 0
                self._scan_skipped += 1
            else:
                task = s.popleft()
                self._scan_skipped = 0
                self._wr_skipped += 1

        is_swap = bool(
            self._resident_model and task.model_id
            and task.model_id != self._resident_model
        )
        prev_model = self._resident_model
        if is_swap:
            self._total_model_swaps += 1
        if task.model_id:
            self._resident_model = task.model_id
            self._current_model = task.model_id
        return task, is_swap, prev_model

    def _worker_loop(self) -> None:
        while True:
            try:
                with self._cv:
                    while not self._lanes[_SCAN_LANE] and not self._lanes[_WR_LANE]:
                        self._cv.wait(timeout=5)
                    task, is_swap, prev_model = self._take_next_locked()
                    swap_no = self._total_model_swaps

                if is_swap:
                    logger.info(
                        "[OLLAMA-QUEUE-SWAP] %s -> %s (lane=%s swap#%d)",
                        prev_model or "(none)", task.model_id, task.lane, swap_no,
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
            "worker_alive": self._worker.is_alive(),
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
# independent two-lane queue and worker thread. A slow job on Ollie no longer
# blocks jobs on bigmac.
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

    Each unique host gets its own worker thread.  Callers that omit *url*
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
