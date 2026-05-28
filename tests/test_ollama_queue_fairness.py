"""SC-7 #3 — two-lane fair OllamaQueue scheduler (commit 09112a1).

This scheduler is the fix for HM-TIER3-SIGNAL-DROP, where strict-FIFO parked
scan-path agent calls behind whole War Room bursts and left 8/9 LLM agents
silent for ~3 weeks. The fairness policy in _take_next_locked had zero tests.

We exercise _take_next_locked directly (seeding lanes + counters under the cv
lock) to avoid worker-thread timing flakiness — the agent-recommended approach.
"""
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from engine.ollama_queue import (  # noqa: E402
    OllamaQueue, _Task, _SCAN_LANE, _WR_LANE, WR_ANTI_STARVE_K,
)


def _task(model_id, lane):
    return _Task(fn=lambda: None, model_id=model_id, lane=lane)


def _seed_and_take(q, scan=None, wr=None, resident="", scan_skipped=0, wr_skipped=0):
    """Seed lanes/counters and run one scheduling decision, holding the cv so
    the background worker can't race us."""
    with q._cv:
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_WR_LANE].clear()
        for t in (scan or []):
            q._lanes[_SCAN_LANE].append(t)
        for t in (wr or []):
            q._lanes[_WR_LANE].append(t)
        q._resident_model = resident
        q._scan_skipped = scan_skipped
        q._wr_skipped = wr_skipped
        task, is_swap, prev = q._take_next_locked()
        return task, is_swap, prev, q._scan_skipped, q._wr_skipped


def test_scan_priority_default():
    """Both lanes ready, no starvation, no affinity bias → scan wins (priority)."""
    q = OllamaQueue()
    task, _, _, scan_sk, wr_sk = _seed_and_take(
        q, scan=[_task("qwen3:8b", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b",
    )
    assert task.lane == _SCAN_LANE, "scan-priority default should serve scan first"
    assert scan_sk == 0 and wr_sk == 1, "wr lane should be marked skipped once"


def test_wr_anti_starvation_forces_wr():
    """After K consecutive WR skips, WR is forced through even with scan waiting."""
    q = OllamaQueue()
    task, _, _, scan_sk, wr_sk = _seed_and_take(
        q, scan=[_task("qwen3:8b", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b", wr_skipped=WR_ANTI_STARVE_K,
    )
    assert task.lane == _WR_LANE, "WR must be forced once wr_skipped hits the cap"
    assert wr_sk == 0, "wr_skipped resets after WR is served"


def test_affinity_prefers_resident_lane():
    """When WR head matches the resident model and scan head doesn't, serve WR
    to avoid a VRAM swap (the HM-WR-VRAM-THRASHING batching win)."""
    q = OllamaQueue()
    task, is_swap, _, _, _ = _seed_and_take(
        q, scan=[_task("llama3.1", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b",
    )
    assert task.lane == _WR_LANE, "affinity should serve the resident-model (WR) lane"
    assert is_swap is False, "serving the resident model must not count as a swap"


def test_scan_anti_starvation_overrides_affinity():
    """The symmetric cap: scan starved by affinity for K turns is forced through,
    overriding the affinity tiebreak (prevents affinity from starving scan)."""
    q = OllamaQueue()
    task, is_swap, _, scan_sk, _ = _seed_and_take(
        q, scan=[_task("llama3.1", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b", scan_skipped=WR_ANTI_STARVE_K,
    )
    assert task.lane == _SCAN_LANE, "scan must be forced once scan_skipped hits the cap"
    assert scan_sk == 0, "scan_skipped resets after scan is served"
    assert is_swap is True, "serving llama3.1 over resident qwen3:8b is a model swap"


def test_anti_starvation_cap_under_request_timeout():
    """Docstring invariant: K * typical_inference must stay under REQUEST_TIMEOUT,
    else a passed-over scan submit can time out before it ever runs."""
    from engine.ollama_queue import REQUEST_TIMEOUT
    typical_inference_s = 90  # documented ~90s/inference on Ollie Box
    assert WR_ANTI_STARVE_K * typical_inference_s < REQUEST_TIMEOUT, (
        f"K={WR_ANTI_STARVE_K} * {typical_inference_s}s exceeds REQUEST_TIMEOUT="
        f"{REQUEST_TIMEOUT}s — a starved scan call could time out before running"
    )
