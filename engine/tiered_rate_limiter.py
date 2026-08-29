"""engine/tiered_rate_limiter.py — HM-429-REMEDIATION-C/B design (2026-08-28).

Shared base for a per-provider rate limiter with:
  1. A reserved-live-caller budget carved out of a smaller-than-the-real-cap
     managed budget, plus a shared pool the rest draw from.
  2. Fail-loud semantics for live-tier callers: skip + log + RED_ALERT
     rather than ever silently serving stale data during market hours.
  3. An env-var kill switch (mode "off" = pure passthrough, identical to
     calling the wrapped function directly -- this file doing nothing).
  4. A shadow mode that changes NO real behavior, only computes and logs
     what enforcement would have done, for a full session's worth of data
     before anyone flips it to actually enforce.

NOT WIRED IN. This module, engine/rate_limiter.py (Polygon), and
engine/alpaca_pacer.py (Alpaca) all exist as designs only -- no live caller
imports any of them yet. Enabling is a separate, later decision; even then,
default mode is "off" everywhere, so merely deploying this code changes
nothing until an env var is explicitly set.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class LimiterMode(Enum):
    OFF = "off"          # pure passthrough -- the kill switch
    SHADOW = "shadow"    # passthrough + log what enforcement would do
    ENFORCE = "enforce"


class BudgetExhausted(Exception):
    """Raised to a live-tier caller in ENFORCE mode when the shared budget
    is exhausted AND the cache is stale beyond the market-hours threshold.

    Callers MUST catch this specifically and skip their decision cycle for
    this data point -- catching it and falling through to some other
    default/stale value defeats the entire point of this exception existing.
    """


class TieredRateLimiter:
    def __init__(
        self,
        name: str,
        live_callers: set[str],
        cap_per_min: int,
        live_reserved_per_min: int,
        live_max_stale_secs: float,
        cache_path: str,
        mode_env_var: str,
        alert_fn: Optional[Callable[[str, str], None]] = None,
        market_hours_fn: Optional[Callable[[], bool]] = None,
    ):
        if live_reserved_per_min > cap_per_min:
            raise ValueError("live_reserved_per_min cannot exceed cap_per_min")
        self.name = name
        self.live_callers = set(live_callers)
        self.cap_per_min = cap_per_min
        self.live_reserved_per_min = live_reserved_per_min
        self.shared_per_min = cap_per_min - live_reserved_per_min
        self.live_max_stale_secs = live_max_stale_secs
        self.cache_path = Path(cache_path)
        self.mode_env_var = mode_env_var
        self._alert_fn = alert_fn or self._default_alert
        self._market_hours_fn = market_hours_fn or self._default_market_hours

        self._lock = threading.Lock()
        self._live_tokens = self.live_reserved_per_min
        self._shared_tokens = self.shared_per_min
        self._last_refill = time.time()
        self._cache: dict = self._load_cache()
        self._shadow_stats = {
            "total": 0, "would_throttle": 0, "would_fail_loud": 0,
            "would_serve_stale": 0, "by_caller_fail_loud": {},
        }

    # ── mode / kill switch ──────────────────────────────────────────────
    @property
    def mode(self) -> LimiterMode:
        raw = os.environ.get(self.mode_env_var, "off").strip().lower()
        try:
            return LimiterMode(raw)
        except ValueError:
            logger.warning(f"{self.mode_env_var}={raw!r} not a valid mode "
                           f"(off/shadow/enforce) -- defaulting to off")
            return LimiterMode.OFF

    # ── on-disk cache (best-effort; never fatal) ────────────────────────
    def _load_cache(self) -> dict:
        try:
            return json.loads(self.cache_path.read_text())
        except Exception:
            return {}

    def _save_cache(self) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.cache_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._cache))
            tmp.replace(self.cache_path)
        except Exception as e:
            logger.debug(f"{self.name} limiter cache persist failed (non-fatal): {e}")

    # ── token buckets ────────────────────────────────────────────────────
    def _refill(self) -> None:
        now = time.time()
        if now - self._last_refill >= 60:
            self._live_tokens = self.live_reserved_per_min
            self._shared_tokens = self.shared_per_min
            self._last_refill = now

    def _try_acquire(self, is_live: bool) -> bool:
        with self._lock:
            self._refill()
            if is_live and self._live_tokens > 0:
                self._live_tokens -= 1
                return True
            if self._shared_tokens > 0:
                self._shared_tokens -= 1
                return True
            return False

    # ── defaults (overridable for testing / different call context) ────
    def _default_market_hours(self) -> bool:
        try:
            from engine.risk_manager import RiskManager
            return RiskManager.is_market_hours() in ("market", "power_hour")
        except Exception:
            return True  # can't tell -> fail toward strictness, not silence

    def _default_alert(self, title: str, message: str) -> None:
        try:
            from engine.alert_channels import send_alert, AlertLevel
            send_alert(message=message, level=AlertLevel.RED_ALERT,
                       alert_type=f"{self.name}_limiter_fail_loud", title=title)
        except Exception as e:
            logger.error(f"{self.name} limiter: fail-loud alert itself failed: {e}")

    # ── the single entry point ──────────────────────────────────────────
    def gated_call(self, caller_name: str, cache_key: str, fetch_fn: Callable):
        """Returns fetch_fn()'s result, a cached value, or raises
        BudgetExhausted (ENFORCE + live tier + budget and cache both
        exhausted during market hours). OFF mode is a byte-for-byte
        passthrough to fetch_fn() -- no cache, no budget, no side effects.
        """
        mode = self.mode
        is_live = caller_name in self.live_callers

        if mode == LimiterMode.OFF:
            return fetch_fn()

        self._shadow_stats["total"] += 1
        cached = self._cache.get(cache_key)
        age = (time.time() - cached["ts"]) if cached else None
        got_token = self._try_acquire(is_live)

        if mode == LimiterMode.SHADOW:
            # Real behavior is UNCHANGED here -- fetch_fn() always actually
            # runs. We only compute and log what ENFORCE would have done,
            # so a full session's log is a faithful preview of enforcement.
            if not got_token:
                self._shadow_stats["would_throttle"] += 1
                in_market = self._market_hours_fn()
                stale = age is None or age > self.live_max_stale_secs
                if is_live and in_market and stale:
                    self._shadow_stats["would_fail_loud"] += 1
                    bc = self._shadow_stats["by_caller_fail_loud"]
                    bc[caller_name] = bc.get(caller_name, 0) + 1
                    logger.warning(
                        f"[{self.name} SHADOW] would FAIL LOUD: {caller_name}/"
                        f"{cache_key} (budget exhausted, cache age={age})")
                elif is_live:
                    self._shadow_stats["would_serve_stale"] += 1
            result = fetch_fn()
            if result is not None:
                self._cache[cache_key] = {"ts": time.time(), "data": result}
                self._save_cache()
            return result

        # ENFORCE
        if got_token:
            result = fetch_fn()
            if result is not None:
                self._cache[cache_key] = {"ts": time.time(), "data": result}
                self._save_cache()
            return result

        in_market = self._market_hours_fn()
        cache_ok_to_serve = cached is not None and (
            not is_live or age <= self.live_max_stale_secs or not in_market
        )
        if cache_ok_to_serve:
            return cached["data"]

        if is_live and in_market:
            self._alert_fn(
                f"{self.name} limiter: live data unavailable",
                f"{caller_name} needed {cache_key}: budget exhausted and cache "
                f"is {'missing' if cached is None else f'{age:.0f}s stale'} "
                f"(max {self.live_max_stale_secs}s during market hours). "
                f"Skipping this cycle rather than serving stale data.",
            )
            raise BudgetExhausted(
                f"{caller_name}: {cache_key} unavailable (budget exhausted, "
                f"cache {'missing' if cached is None else 'too stale'})")

        # Cached tier, no budget, no usable cache -- degrade gracefully.
        return None

    def shadow_report(self) -> dict:
        return dict(self._shadow_stats, mode=self.mode.value, name=self.name)
