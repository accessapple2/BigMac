"""Ollama FIFO Queue — serializes all Ollama inference calls system-wide.

On a 16GB Mac Mini only one 9B model fits in VRAM at a time.
This queue ensures agents take turns rather than fighting for VRAM,
and sets keep_alive=60s so models unload quickly between different models.
"""
from __future__ import annotations

import queue
import threading
import time
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

# Per-request timeout (seconds). Exceeded requests are skipped, not retried.
REQUEST_TIMEOUT = 120

# How long to retain response-time samples for avg calculation.
_MAX_SAMPLES = 50


class OllamaQueue:
    """Thread-safe singleton FIFO queue for Ollama HTTP calls."""

    def __init__(self) -> None:
        self._q: queue.Queue = queue.Queue()
        self._lock = threading.Lock()
        self._worker = threading.Thread(target=self._worker_loop, daemon=True, name="ollama-queue-worker")
        self._worker.start()

        # Metrics
        self._last_success_ts: float = 0.0   # epoch time of last successful call
        self._queue_depth: int = 0
        self._response_times: list[float] = []
        self._total_requests: int = 0
        self._total_timeouts: int = 0
        self._current_model: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, fn: Callable[[], Any], model_id: str = "") -> Any:
        """Submit a callable to the queue and block until it completes (or times out).

        Args:
            fn: Zero-argument callable that performs the Ollama HTTP request.
            model_id: Model being called (for metrics/logging).

        Returns:
            Whatever fn() returns.

        Raises:
            TimeoutError: If the call exceeds REQUEST_TIMEOUT seconds.
            Exception: Any exception raised by fn() is re-raised.
        """
        result_holder: list = [None]
        exc_holder: list = [None]
        done = threading.Event()

        def task():
            t0 = time.monotonic()
            try:
                result_holder[0] = fn()
                elapsed = time.monotonic() - t0
                with self._lock:
                    self._last_success_ts = time.time()
                    self._response_times.append(elapsed)
                    if len(self._response_times) > _MAX_SAMPLES:
                        self._response_times.pop(0)
            except Exception as e:
                exc_holder[0] = e
            finally:
                done.set()

        with self._lock:
            self._queue_depth += 1
            self._total_requests += 1
            self._current_model = model_id

        self._q.put(task)

        fired = done.wait(timeout=REQUEST_TIMEOUT)

        with self._lock:
            self._queue_depth = max(0, self._queue_depth - 1)

        if not fired:
            with self._lock:
                self._total_timeouts += 1
            logger.error("OllamaQueue: request timed out after %ds (model=%s)", REQUEST_TIMEOUT, model_id)
            raise TimeoutError(f"Ollama request timed out after {REQUEST_TIMEOUT}s (model={model_id})")

        if exc_holder[0] is not None:
            raise exc_holder[0]

        return result_holder[0]

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Return a snapshot of queue health metrics."""
        with self._lock:
            avg_rt = (
                round(sum(self._response_times) / len(self._response_times), 2)
                if self._response_times
                else None
            )
            last_success = self._last_success_ts
            current_model = self._current_model

        age_min: float | None = None
        stale = False
        if last_success > 0:
            age_min = round((time.time() - last_success) / 60, 1)
            stale = age_min > 30  # warn if >30 min since last success

        return {
            "queue_depth": self._queue_depth,
            "total_requests": self._total_requests,
            "total_timeouts": self._total_timeouts,
            "avg_response_time_s": avg_rt,
            "last_success_age_min": age_min,
            "stale": stale,
            "current_model": current_model,
            "worker_alive": self._worker.is_alive(),
        }

    def last_success_age_min(self) -> float | None:
        """Return minutes since last successful analysis, or None if never run."""
        with self._lock:
            ts = self._last_success_ts
        if ts == 0.0:
            return None
        return (time.time() - ts) / 60

    # ------------------------------------------------------------------
    # Internal worker
    # ------------------------------------------------------------------

    def _worker_loop(self) -> None:
        while True:
            try:
                task = self._q.get(timeout=5)
                task()
                self._q.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error("OllamaQueue worker error: %s", e)


# Module-level singleton — imported everywhere
_queue = OllamaQueue()


def get_queue() -> OllamaQueue:
    """Return the global OllamaQueue singleton."""
    return _queue
