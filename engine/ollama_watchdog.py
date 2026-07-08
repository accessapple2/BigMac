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
import os
from datetime import datetime, timezone

import requests
from config import OLLAMA_URL, OLLIE_URL

logger = logging.getLogger(__name__)

_TIMEOUT_LOG = os.path.join(os.path.dirname(__file__), "..", "logs", "ollama_timeouts.jsonl")


def _write_timeout_log(model_id: str, consecutive: int, action: str) -> None:
    """Append one JSON line to logs/ollama_timeouts.jsonl. Never raises."""
    try:
        entry = json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "model_id": model_id,
            "consecutive_timeouts": consecutive,
            "action": action,  # "continue" or "recycle"
        })
        os.makedirs(os.path.dirname(_TIMEOUT_LOG), exist_ok=True)
        with open(_TIMEOUT_LOG, "a") as f:
            f.write(entry + "\n")
    except Exception as e:
        logger.warning("_write_timeout_log append failed for %s: %s", _TIMEOUT_LOG, e)

RECYCLE_AFTER_N_TIMEOUTS = 3       # consecutive timeouts before recycle attempt
SKIP_AFTER_FAIL_MIN = 30           # skip model for N minutes if recycle also fails
CIRCUIT_RESTART_WAIT_S = 15        # seconds to wait after `ollama serve` before re-warming
# 2026-04-20: qwen3:8b replaced — it loaded 8GB on bigmac localhost and caused swap storms.
# qwen3:8b lives on Ollie GPU; warmup goes there, not bigmac.
PRIMARY_WARMUP_MODEL = "qwen3:8b"

# Models that live on Ollie GPU. All others fall back to bigmac localhost.
_OLLIE_MODELS: frozenset[str] = frozenset({
    "qwen3:8b", "qwen3:14b", "deepseek-r1:14b", "deepseek-r1:7b",
    "0xroyce/plutus:latest", "0xroyce/plutus", "qwen2.5-coder:7b",
    "gemma3:4b",  # 2026-05-17 Wave 1 Fix #3: mistral:7b removed — Pike migration to Ollie never completed; model lives on bigmac
})


def _model_url(model_id: str) -> str:
    """Return the Ollama server URL for a given model_id.

    Ollie GPU (192.168.1.166:11434) hosts the heavy war-room models + Picard/Pike.
    Bigmac localhost is reserved for small resident models (phi3:mini).
    """
    return OLLIE_URL if model_id in _OLLIE_MODELS else OLLAMA_URL


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
    """Post a message to the dashboard war room feed — best-effort.

    HM-SILENT-CATCH-SWEEP 2026-07-07: was POSTing to /api/war-room, which is
    GET-only (dashboard/app.py:7404) -- every call has 405'd and been
    silently swallowed since this was written. The real POST-capable route
    is /api/war-room/post (dashboard/app.py:7618), which attributes the
    message to player_id='webull' regardless of source -- message is
    tagged "[Ollama Watchdog]" so it's still visually distinguishable in
    the feed despite the attribution.
    """
    try:
        body = json.dumps({"message": f"[Ollama Watchdog] {message}"}).encode()
        requests.post(
            "http://127.0.0.1:8080/api/war-room/post",
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=4,
        )
    except Exception as e:
        logger.warning("_post_war_room failed: %s", e)


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
        action = "recycle" if count >= RECYCLE_AFTER_N_TIMEOUTS else "continue"
        _write_timeout_log(model_id, count, action)
        return action

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
        _url = _model_url(model_id)  # Ollie GPU or bigmac localhost
        try:
            # Step 1 — force unload (against the server that actually holds the model)
            requests.post(
                f"{_url}/api/generate",
                json={"model": model_id, "keep_alive": 0},
                timeout=10,
            )
            time.sleep(5)
            # Step 2 — reload with a probe
            r = requests.post(
                f"{_url}/api/generate",
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
            # Re-warm primary model on Ollie GPU (qwen3:8b lives there, not bigmac)
            requests.post(
                f"{OLLIE_URL}/api/generate",
                json={
                    "model": PRIMARY_WARMUP_MODEL,
                    "keep_alive": "5m",
                    "prompt": "ready",
                    "stream": False,
                    "options": {"num_predict": 1},
                },
                timeout=60,
            )
            logger.info("Ollama circuit breaker: server restarted and %s warmed on Ollie", PRIMARY_WARMUP_MODEL)
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
