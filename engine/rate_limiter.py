from __future__ import annotations
"""Warp Core Governor — Thread-safe token bucket rate limiter for Alpaca API calls.

Shared singleton used by all modules that call Alpaca. Enforces the 200 req/min
limit (we conservatively cap at 150 to stay well clear).

Usage:
    from engine.rate_limiter import limiter
    limiter.acquire()          # consume 1 token before an Alpaca call
    limiter.acquire(n=5)       # consume 5 tokens for a heavy request
    limiter.get_stats()        # dashboard-friendly utilization dict
"""

import threading
import time
from collections import deque

from rich.console import Console

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_CAPACITY: int = 150          # max tokens in bucket
_REFILL_RATE: float = 150 / 60   # tokens per second (2.5/s)
_MAX_WAIT: float = 30.0       # seconds to wait before proceeding anyway
_WARN_UTILIZATION: float = 0.80   # log WARNING above this fraction (120 calls/min)

console = Console()


# ---------------------------------------------------------------------------
# Core class
# ---------------------------------------------------------------------------

class AlpacaRateLimiter:
    """Thread-safe token bucket rate limiter for the Alpaca API.

    Capacity:    150 tokens
    Refill rate: 2.5 tokens/sec (150 tokens/min)
    Max wait:    30 seconds — proceeds anyway to avoid deadlock
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens: float = float(_CAPACITY)
        self._last_refill: float = time.monotonic()

        # Rolling 60-second call window
        self._call_timestamps: deque[float] = deque()

        # Aggregate counters
        self._total_calls: int = 0
        self._throttled_calls: int = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """Refill tokens based on elapsed time. Must be called under self._lock."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            new_tokens = elapsed * _REFILL_RATE
            self._tokens = min(float(_CAPACITY), self._tokens + new_tokens)
            self._last_refill = now

    def _calls_last_minute(self) -> int:
        """Count calls in the past 60 seconds. Must be called under self._lock."""
        cutoff = time.monotonic() - 60.0
        while self._call_timestamps and self._call_timestamps[0] < cutoff:
            self._call_timestamps.popleft()
        return len(self._call_timestamps)

    def _record_call(self) -> None:
        """Record a call timestamp. Must be called under self._lock."""
        self._call_timestamps.append(time.monotonic())
        self._total_calls += 1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, n: int = 1) -> None:
        """Block until n tokens are available, then consume them.

        Waits at most MAX_WAIT seconds before proceeding anyway to prevent
        a hung thread from blocking the entire system.

        Args:
            n: Number of tokens to consume (default 1).
        """
        deadline = time.monotonic() + _MAX_WAIT
        waited = False

        while True:
            with self._lock:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    self._record_call()
                    if waited:
                        self._throttled_calls += 1
                    # Warn on high utilisation
                    calls_1min = self._calls_last_minute()
                    if calls_1min / _CAPACITY >= _WARN_UTILIZATION:
                        console.log(
                            f"[yellow][RateLimiter] WARNING — utilisation "
                            f"{calls_1min}/{_CAPACITY} calls/min "
                            f"({calls_1min / _CAPACITY:.0%}). Throttling may occur."
                        )
                    return

                # Check deadline before sleeping
                if time.monotonic() >= deadline:
                    # Proceed anyway — consume whatever is available
                    self._tokens = max(0.0, self._tokens - n)
                    self._record_call()
                    self._throttled_calls += 1
                    console.log(
                        "[red][RateLimiter] Max wait exceeded — proceeding anyway "
                        f"(throttled_calls={self._throttled_calls})"
                    )
                    return

            waited = True
            # Sleep for the time needed to accumulate the required tokens
            with self._lock:
                deficit = n - self._tokens
            sleep_for = min(deficit / _REFILL_RATE, 0.5)
            time.sleep(max(sleep_for, 0.05))

    def get_stats(self) -> dict:
        """Return a dashboard-friendly stats snapshot.

        Returns:
            dict with keys:
                tokens_available    — current token count (float)
                calls_last_minute   — calls in past 60 s (int)
                utilization_pct     — 0-100 float
                throttled_count     — total times had to wait or forced-proceed (int)
                total_calls         — lifetime call count (int)
        """
        with self._lock:
            self._refill()
            calls_1min = self._calls_last_minute()
            utilization = round(calls_1min / _CAPACITY * 100, 1)
            return {
                "tokens_available": round(self._tokens, 2),
                "calls_last_minute": calls_1min,
                "utilization_pct": utilization,
                "throttled_count": self._throttled_calls,
                "total_calls": self._total_calls,
            }


# ---------------------------------------------------------------------------
# Module-level singleton — import this everywhere
# ---------------------------------------------------------------------------

limiter = AlpacaRateLimiter()
