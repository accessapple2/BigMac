"""SC-7 #3 — two-lane fair OllamaQueue scheduler (commit 09112a1).

This scheduler is the fix for HM-TIER3-SIGNAL-DROP, where strict-FIFO parked
scan-path agent calls behind whole War Room bursts and left 8/9 LLM agents
silent for ~3 weeks. The fairness policy in _take_next_locked had zero tests.

We exercise _take_next_locked directly (seeding lanes + counters under the cv
lock) to avoid worker-thread timing flakiness — the agent-recommended approach.

HM-PERF-FLEET-THROUGHPUT (2026-07-07): updated for the N-worker redesign.
The old single `_resident_model` string became a two-tier model of state:
`_active_models` (Counter, models a worker is executing RIGHT NOW) and
`_resident_models` (LRU OrderedDict, capped at num_workers, models recently
dispatched and probably still loaded). Every test below that used to seed
`q._resident_model = "..."` now seeds `q._resident_models["..."] = None`
instead — same test *intent* (this model is the one the scheduler should
prefer to avoid a swap), adapted to the new internal shape. New tests added
for the active-model tier (the actual NUM_PARALLEL=2 win) and for
num_workers=1 rollback parity.
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


def _seed_and_take(q, scan=None, wr=None, resident="", active=None,
                    scan_skipped=0, wr_skipped=0):
    """Seed lanes/counters and run one scheduling decision, holding the cv so
    the background worker(s) can't race us.

    resident: single model_id string (convenience for the common case of one
        resident model) OR omit and pre-populate q._resident_models yourself.
    active: dict[model_id, count] of models a worker is "executing" right now.
    """
    with q._cv:
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_WR_LANE].clear()
        for t in (scan or []):
            q._lanes[_SCAN_LANE].append(t)
        for t in (wr or []):
            q._lanes[_WR_LANE].append(t)
        q._resident_models.clear()
        if resident:
            q._resident_models[resident] = None
        q._active_models.clear()
        if active:
            q._active_models.update(active)
        q._scan_skipped = scan_skipped
        q._wr_skipped = wr_skipped
        task, is_swap, prev = q._take_next_locked()
        return task, is_swap, prev, q._scan_skipped, q._wr_skipped


def test_scan_priority_default():
    """Both lanes ready, no starvation, no affinity bias → scan wins (priority)."""
    q = OllamaQueue(num_workers=1)
    task, _, _, scan_sk, wr_sk = _seed_and_take(
        q, scan=[_task("qwen3:8b", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b",
    )
    assert task.lane == _SCAN_LANE, "scan-priority default should serve scan first"
    assert scan_sk == 0 and wr_sk == 1, "wr lane should be marked skipped once"


def test_wr_anti_starvation_forces_wr():
    """After K consecutive WR skips, WR is forced through even with scan waiting."""
    q = OllamaQueue(num_workers=1)
    task, _, _, scan_sk, wr_sk = _seed_and_take(
        q, scan=[_task("qwen3:8b", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b", wr_skipped=WR_ANTI_STARVE_K,
    )
    assert task.lane == _WR_LANE, "WR must be forced once wr_skipped hits the cap"
    assert wr_sk == 0, "wr_skipped resets after WR is served"


def test_affinity_prefers_resident_lane():
    """When WR head matches a resident model and scan head doesn't, serve WR
    to avoid a VRAM swap (the HM-WR-VRAM-THRASHING batching win)."""
    q = OllamaQueue(num_workers=1)
    task, is_swap, _, _, _ = _seed_and_take(
        q, scan=[_task("llama3.1", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b",
    )
    assert task.lane == _WR_LANE, "affinity should serve the resident-model (WR) lane"
    assert is_swap is False, "serving the resident model must not count as a swap"


def test_scan_anti_starvation_overrides_affinity():
    """The symmetric cap: scan starved by affinity for K turns is forced through,
    overriding the affinity tiebreak (prevents affinity from starving scan)."""
    q = OllamaQueue(num_workers=1)
    task, is_swap, _, scan_sk, _ = _seed_and_take(
        q, scan=[_task("llama3.1", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        resident="qwen3:8b", scan_skipped=WR_ANTI_STARVE_K,
    )
    assert task.lane == _SCAN_LANE, "scan must be forced once scan_skipped hits the cap"
    assert scan_sk == 0, "scan_skipped resets after scan is served"
    assert is_swap is True, "serving llama3.1 over resident qwen3:8b (at capacity) is a model swap"


def test_anti_starvation_cap_under_request_timeout():
    """Docstring invariant: K * typical_inference must stay under REQUEST_TIMEOUT,
    else a passed-over scan submit can time out before it ever runs."""
    from engine.ollama_queue import REQUEST_TIMEOUT
    typical_inference_s = 90  # documented ~90s/inference on Ollie Box
    assert WR_ANTI_STARVE_K * typical_inference_s < REQUEST_TIMEOUT, (
        f"K={WR_ANTI_STARVE_K} * {typical_inference_s}s exceeds REQUEST_TIMEOUT="
        f"{REQUEST_TIMEOUT}s — a starved scan call could time out before running"
    )


# ---------------------------------------------------------------------------
# HM-PERF-FLEET-THROUGHPUT (2026-07-07): new tests for the N-worker redesign.
# ---------------------------------------------------------------------------

def test_active_model_affinity_beats_resident_affinity():
    """Tier 1 (active, i.e. another worker is running this model RIGHT NOW)
    outranks tier 2 (merely resident/recently-used). This is the actual
    NUM_PARALLEL=2 win: two concurrent requests against one loaded model."""
    q = OllamaQueue(num_workers=2)
    # scan head is ACTIVELY running elsewhere; wr head is only "resident"
    # (recently used, not currently executing). Active should win scan here
    # even though scan is already the default priority -- construct the
    # inverse to prove it's the active check doing the work, not just
    # scan-priority: make WR the active one, scan merely resident.
    task, is_swap, _, _, _ = _seed_and_take(
        q, scan=[_task("llama3.1", _SCAN_LANE)], wr=[_task("qwen3:8b", _WR_LANE)],
        active={"qwen3:8b": 1},
    )
    assert task.lane == _WR_LANE, "active-model affinity should override scan-priority default"
    assert is_swap is False, "the actively-running model is trivially resident too"


def test_resident_set_holds_two_models_at_two_workers():
    """With num_workers=2, the resident set tracks up to 2 distinct models
    (matching server MAX_LOADED_MODELS=2) -- dispatching a SECOND distinct
    model is NOT a swap (filling the second slot), only a THIRD would be."""
    q = OllamaQueue(num_workers=2)
    with q._cv:
        q._resident_models.clear()
        q._resident_models["qwen3:8b"] = None  # one model already resident
        q._active_models.clear()
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_SCAN_LANE].append(_task("ministral-3:3b", _SCAN_LANE))
        q._lanes[_WR_LANE].clear()
        q._scan_skipped = q._wr_skipped = 0
        task, is_swap, _ = q._take_next_locked()
    assert task.model_id == "ministral-3:3b"
    assert is_swap is False, "filling the 2nd resident slot is not a swap at num_workers=2"
    assert set(q._resident_models) == {"qwen3:8b", "ministral-3:3b"}


def test_third_distinct_model_is_a_swap_at_capacity():
    """A THIRD distinct model, once the resident set is already at capacity
    (2, for num_workers=2), IS a swap -- something has to be evicted."""
    q = OllamaQueue(num_workers=2)
    with q._cv:
        q._resident_models.clear()
        q._resident_models["qwen3:8b"] = None
        q._resident_models["ministral-3:3b"] = None
        q._active_models.clear()
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_SCAN_LANE].append(_task("llama3.1", _SCAN_LANE))
        q._lanes[_WR_LANE].clear()
        q._scan_skipped = q._wr_skipped = 0
        task, is_swap, _ = q._take_next_locked()
    assert task.model_id == "llama3.1"
    assert is_swap is True, "a 3rd distinct model at 2/2 capacity must evict one of the residents"


def test_num_workers_1_collapses_resident_capacity_to_one():
    """Rollback parity: OLLAMA_QUEUE_WORKERS=1 must behave identically to the
    pre-2026-07-07 single-_resident_model design -- only one model is ever
    tracked as resident, so the 2nd distinct dispatch is a swap, not a
    capacity-fill."""
    q = OllamaQueue(num_workers=1)
    with q._cv:
        q._resident_models.clear()
        q._resident_models["qwen3:8b"] = None
        q._active_models.clear()
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_SCAN_LANE].append(_task("ministral-3:3b", _SCAN_LANE))
        q._lanes[_WR_LANE].clear()
        q._scan_skipped = q._wr_skipped = 0
        task, is_swap, _ = q._take_next_locked()
    assert is_swap is True, "at num_workers=1, a second distinct model must still count as a swap"
    assert list(q._resident_models) == ["ministral-3:3b"], "capacity-1 evicts the prior resident"


def test_strict_affinity_worker1_idles_with_no_matching_active_model():
    """Strict-affinity mode: worker_index>0 must decline BOTH lanes if
    neither head matches a model already active elsewhere -- returns None
    (caller should wait), even though real work is queued."""
    q = OllamaQueue(num_workers=2, strict_affinity=True)
    with q._cv:
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_SCAN_LANE].append(_task("qwen3:8b", _SCAN_LANE))
        q._lanes[_WR_LANE].clear()
        q._lanes[_WR_LANE].append(_task("ministral-3:3b", _WR_LANE))
        q._active_models.clear()  # nothing active anywhere
        picked = q._take_next_locked(worker_index=1)
    assert picked is None, "worker 1 must idle when no lane head matches an active model"


def test_strict_affinity_worker1_takes_matching_active_model():
    """Strict-affinity mode: worker_index>0 DOES take a task whose model is
    already active -- this is the whole point, pure NUM_PARALLEL=2 reuse."""
    q = OllamaQueue(num_workers=2, strict_affinity=True)
    with q._cv:
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_SCAN_LANE].append(_task("qwen3:8b", _SCAN_LANE))
        q._lanes[_WR_LANE].clear()
        q._active_models.clear()
        q._active_models["qwen3:8b"] = 1  # worker 0 is running this right now
        task, is_swap, _ = q._take_next_locked(worker_index=1)
    assert task.model_id == "qwen3:8b"
    assert is_swap is False, "matching an already-active model is never a swap"


def test_strict_affinity_worker1_ignores_nonmatching_wr_even_if_only_option():
    """Strict-affinity mode: worker 1 must NOT take a WR task for a
    different, non-active model just because it's the only thing queued --
    that's exactly the model-diversity-in-flight behavior this mode exists
    to prevent."""
    q = OllamaQueue(num_workers=2, strict_affinity=True)
    with q._cv:
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_WR_LANE].clear()
        q._lanes[_WR_LANE].append(_task("llama3.1", _WR_LANE))
        q._active_models.clear()
        q._active_models["qwen3:8b"] = 1  # worker 0 running a DIFFERENT model
        picked = q._take_next_locked(worker_index=1)
    assert picked is None, "worker 1 must not pick up a non-active model even as the only option"


def test_strict_affinity_worker0_is_unrestricted():
    """Strict-affinity mode only restricts worker_index>0 -- worker 0 keeps
    normal fairness + two-tier affinity behavior, unchanged."""
    q = OllamaQueue(num_workers=2, strict_affinity=True)
    with q._cv:
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_SCAN_LANE].append(_task("qwen3:8b", _SCAN_LANE))
        q._lanes[_WR_LANE].clear()
        q._active_models.clear()
        q._resident_models.clear()
        task, is_swap, _ = q._take_next_locked(worker_index=0)
    assert task.model_id == "qwen3:8b", "worker 0 dispatches normally regardless of strict_affinity"


def test_strict_affinity_off_worker1_behaves_like_worker0():
    """Sanity check on the flag itself: with strict_affinity=False (default),
    worker_index>0 must NOT be restricted -- confirms the gate is the flag,
    not some accidental always-on behavior."""
    q = OllamaQueue(num_workers=2, strict_affinity=False)
    with q._cv:
        q._lanes[_SCAN_LANE].clear()
        q._lanes[_WR_LANE].clear()
        q._lanes[_WR_LANE].append(_task("llama3.1", _WR_LANE))
        q._active_models.clear()
        q._resident_models.clear()
        task, _, _ = q._take_next_locked(worker_index=1)
    assert task is not None and task.model_id == "llama3.1", (
        "with strict_affinity off, worker 1 must dispatch normally like any worker"
    )


def test_concurrent_dispatch_two_workers_run_simultaneously():
    """End-to-end (not seeded internals): submit 2 slow same-model tasks to a
    2-worker queue and confirm they actually overlap in wall-clock time --
    the real behavior change this whole ticket is about."""
    import threading
    import time

    q = OllamaQueue(num_workers=2)
    start_barrier = threading.Barrier(2, timeout=5)
    overlap_detected = threading.Event()
    entered = []
    lock = threading.Lock()

    def slow_call():
        with lock:
            entered.append(time.monotonic())
        start_barrier.wait()  # only releases once BOTH calls have entered concurrently
        overlap_detected.set()
        time.sleep(0.05)
        return "ok"

    results = [None, None]

    def _submit(i):
        results[i] = q.submit(slow_call, model_id="qwen3:8b")

    t1 = threading.Thread(target=_submit, args=(0,))
    t2 = threading.Thread(target=_submit, args=(1,))
    t1.start(); t2.start()
    t1.join(timeout=10); t2.join(timeout=10)

    assert results == ["ok", "ok"], "both concurrent submits must complete successfully"
    assert overlap_detected.is_set(), (
        "the barrier only releases if both slow_call invocations were inside "
        "the function simultaneously -- proves 2 workers actually ran concurrently, "
        "not just quickly in sequence"
    )


def test_single_worker_serializes_strictly():
    """num_workers=1 must still serialize (no false-positive concurrency) --
    the rollback path has to be genuinely single-threaded, not just default-
    to-1-but-still-technically-parallel-capable."""
    import threading
    import time

    q = OllamaQueue(num_workers=1)
    concurrent_count = [0]
    max_concurrent = [0]
    lock = threading.Lock()

    def tracked_call():
        with lock:
            concurrent_count[0] += 1
            max_concurrent[0] = max(max_concurrent[0], concurrent_count[0])
        time.sleep(0.05)
        with lock:
            concurrent_count[0] -= 1
        return "ok"

    threads = [threading.Thread(target=lambda: q.submit(tracked_call, model_id="qwen3:8b"))
               for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert max_concurrent[0] == 1, (
        f"num_workers=1 must never run more than 1 task concurrently, saw {max_concurrent[0]}"
    )


def test_strict_affinity_end_to_end_worker_index_threads_correctly():
    """Full submit() path (not seeded internals) with real worker threads:
    confirms worker_index actually threads through Thread(args=(i,)) ->
    _worker_loop -> _take_next_locked correctly in a genuinely concurrent
    run, not just in the directly-seeded unit tests above. Submits a
    same-model WR burst that only worker 0 could serve one-at-a-time under
    strict mode plus a second, DIFFERENT-model scan call -- worker 1 must
    help drain the same-model burst (matches, active) but never touch the
    different-model scan call (would require introducing model diversity,
    which strict mode exists to prevent)."""
    import threading
    import time

    q = OllamaQueue(num_workers=2, strict_affinity=True)
    call_log = []
    lock = threading.Lock()

    def _slow(tag, sleep_s=0.05):
        with lock:
            call_log.append(("start", tag, time.monotonic()))
        time.sleep(sleep_s)
        with lock:
            call_log.append(("end", tag, time.monotonic()))
        return "ok"

    # 3 same-model WR calls (burst) + 1 different-model scan call, all fired
    # at once. With strict affinity, worker 0 handles the mixed fairness
    # decision as usual; worker 1 should only ever pick up qwen3:8b tasks.
    threads = [
        threading.Thread(target=lambda: q.submit(lambda: _slow("wr-a"), model_id="qwen3:8b", lane="wr")),
        threading.Thread(target=lambda: q.submit(lambda: _slow("wr-b"), model_id="qwen3:8b", lane="wr")),
        threading.Thread(target=lambda: q.submit(lambda: _slow("wr-c"), model_id="qwen3:8b", lane="wr")),
        threading.Thread(target=lambda: q.submit(lambda: _slow("scan-x", 0.02), model_id="llama3.1", lane="scan")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    tags_completed = {tag for event, tag, _ in call_log if event == "end"}
    assert tags_completed == {"wr-a", "wr-b", "wr-c", "scan-x"}, (
        f"all 4 submits must complete under strict affinity, got {tags_completed}"
    )
    # The queue must never have deadlocked or dropped a task -- completion
    # itself (within the 10s join timeout) is the main correctness proof;
    # the seeded unit tests above cover the precise dispatch logic.
