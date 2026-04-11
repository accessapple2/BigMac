"""Ollama timeout watchdog — per-model circuit breaker, auto-recycle, scan health metrics.

Wraps the Arena scan loop. When a model times out 3x consecutively:
  1. Force-unload + reload (recycle)
  2. If recycle fails → skip that model for 30 min + notify

Circuit breaker: if ALL models in a cycle time out → restart Ollama entirely.
Scan health: logged after every cycle as [SCAN HEALTH] ... for dashboard consumption.
"""
from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

OLLAMA_URL = "http://localhost:11434"
RECYCLE_AFTER_N_TIMEOUTS = 3       # consecutive timeouts before recycle attempt
SKIP_AFTER_FAIL_MIN = 30           # skip model for N minutes if recycle also fails
CIRCUIT_RESTART_WAIT_S = 15        # seconds to wait after `ollama serve` before re-warming
PRIMARY_WARMUP_MODEL = "qwen3.5:9b"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _notify(title: str, msg: str) -> None:
    """macOS notification — fire-and-forget."""
    try:
        script = f'display notification "{msg}" with title "{title}" sound name "Sosumi"'
        subprocess.Popen(
            ["osascript", "-e", script],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _post_war_room(message: str) -> None:
    """Post a message to the dashboard war room feed — best-effort."""
    try:
        body = json.dumps({"message": message, "source": "ollama-watchdog"}).encode()
        requests.post(
            "http://127.0.0.1:8080/api/war-room",
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=4,
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------

class OllamaWatchdog:
    """Thread-safe watchdog tracking per-model timeout counts and scan health."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # model_id → consecutive timeout count
        self._consecutive_timeouts: dict[str, int] = {}
        # model_id → epoch timestamp until which the model is skipped
        self._skip_until: dict[str, float] = {}
        # last full scan cycle health snapshot
        self._scan_health: dict = {}

    # ------------------------------------------------------------------
    # Skip state
    # ------------------------------------------------------------------

    def is_skipped(self, model_id: str) -> bool:
        """Return True if this model is in its post-recycle-failure skip window."""
        with self._lock:
            return time.time() < self._skip_until.get(model_id, 0.0)

    # ------------------------------------------------------------------
    # Per-call recording
    # ------------------------------------------------------------------

    def record_success(self, model_id: str) -> None:
        """Reset consecutive timeout counter on any successful response."""
        with self._lock:
            self._consecutive_timeouts[model_id] = 0

    def record_timeout(self, model_id: str) -> str:
        """Increment timeout counter. Returns 'recycle' or 'continue'."""
        with self._lock:
            count = self._consecutive_timeouts.get(model_id, 0) + 1
            self._consecutive_timeouts[model_id] = count
        if count >= RECYCLE_AFTER_N_TIMEOUTS:
            return "recycle"
        return "continue"

    # ------------------------------------------------------------------
    # Model recycle
    # ------------------------------------------------------------------

    def recycle_model(self, model_id: str) -> bool:
        """Force-unload then reload model. Returns True on success.

        On failure, places model in skip window for SKIP_AFTER_FAIL_MIN minutes
        and fires a macOS notification + war room post.
        """
        logger.warning(
            "Ollama auto-recovery: recycling %s after %d consecutive timeouts",
            model_id, RECYCLE_AFTER_N_TIMEOUTS,
        )
        try:
            # Step 1 — force unload
            requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": model_id, "keep_alive": 0},
                timeout=10,
            )
            time.sleep(5)
            # Step 2 — reload with a probe
            r = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": model_id,
                    "keep_alive": "5m",
                    "prompt": "ready",
                    "stream": False,
                    "options": {"num_predict": 1},
                },
                timeout=90,
            )
            if r.ok:
                logger.info("Ollama auto-recovery: %s recycled successfully", model_id)
                with self._lock:
                    self._consecutive_timeouts[model_id] = 0
                return True
        except Exception as e:
            logger.error("Ollama auto-recovery: recycle %s failed: %s", model_id, e)

        # Recycle failed — skip model for SKIP_AFTER_FAIL_MIN minutes
        skip_ts = time.time() + SKIP_AFTER_FAIL_MIN * 60
        with self._lock:
            self._skip_until[model_id] = skip_ts
            self._consecutive_timeouts[model_id] = 0
        msg = f"Ollama {model_id}: recycle failed — skipping for {SKIP_AFTER_FAIL_MIN}min"
        logger.warning(msg)
        _notify("⚠️ USS TradeMinds", msg)
        _post_war_room(f"⚠️ {msg}")
        return False

    # ------------------------------------------------------------------
    # Circuit breaker
    # ------------------------------------------------------------------

    def check_and_fire_circuit_breaker(self, total_attempted: int, total_timed_out: int) -> bool:
        """If every model in the cycle timed out, restart Ollama entirely.

        Returns True if the circuit breaker fired.
        """
        if total_attempted == 0 or total_timed_out < total_attempted:
            return False

        msg = (
            f"Ollama CIRCUIT BREAKER: full restart — all {total_timed_out}/{total_attempted} "
            f"models timed out this cycle"
        )
        logger.critical(msg)
        _notify("🚨 USS TradeMinds", msg)
        _post_war_room(f"🚨 {msg}")

        try:
            subprocess.run(["pkill", "ollama"], capture_output=True)
            time.sleep(5)
            subprocess.Popen(
                ["/usr/local/bin/ollama", "serve"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            time.sleep(CIRCUIT_RESTART_WAIT_S)
            # Re-warm primary model
            requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": PRIMARY_WARMUP_MODEL,
                    "keep_alive": "5m",
                    "prompt": "ready",
                    "stream": False,
                    "options": {"num_predict": 1},
                },
                timeout=60,
            )
            logger.info("Ollama circuit breaker: server restarted and %s warmed", PRIMARY_WARMUP_MODEL)
        except Exception as e:
            logger.error("Ollama circuit breaker restart failed: %s", e)

        # Reset all per-model state so next cycle starts clean
        with self._lock:
            self._consecutive_timeouts.clear()
            self._skip_until.clear()

        return True

    # ------------------------------------------------------------------
    # Scan health
    # ------------------------------------------------------------------

    def record_scan_health(
        self,
        total: int,
        responded: int,
        timeouts_by_model: dict[str, int],
        avg_response_s: float | None,
    ) -> None:
        """Log a [SCAN HEALTH] line and store the snapshot for the /api/health endpoint."""
        timeout_parts = [f"{m}({n})" for m, n in timeouts_by_model.items() if n > 0]
        avg_str = f"{avg_response_s:.1f}s" if avg_response_s is not None else "n/a"
        line = f"[SCAN HEALTH] {responded}/{total} models responded | avg {avg_str}"
        if timeout_parts:
            line += f" | timeouts: {', '.join(timeout_parts)}"
        logger.info(line)
        with self._lock:
            self._scan_health = {
                "total": total,
                "responded": responded,
                "avg_response_s": avg_response_s,
                "timeouts_by_model": dict(timeouts_by_model),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }

    def get_scan_health(self) -> dict:
        """Return last scan health snapshot (for /api/health endpoint)."""
        with self._lock:
            return dict(self._scan_health)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_watchdog = OllamaWatchdog()


def get_watchdog() -> OllamaWatchdog:
    """Return the global OllamaWatchdog singleton."""
    return _watchdog
