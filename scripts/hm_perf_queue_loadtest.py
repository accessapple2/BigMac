#!/usr/bin/env python3
"""scripts/hm_perf_queue_loadtest.py — HM-PERF-FLEET-THROUGHPUT, 2026-07-07.

Standalone load test for the N-worker OllamaQueue redesign. Does NOT touch
the live trader process (main.py) -- imports engine.ollama_queue fresh in
this process, gets its own independent OllamaQueue instance, and fires
synthetic scan-lane + WR-lane calls at the real Ollie Max host.

Runs the SAME batch twice: num_workers=1 (baseline, simulating the
pre-2026-07-07 serial behavior) then num_workers=2 (the new default),
comparing wall-clock time -- the direct "before/after" this ticket asks for.

Usage:
    python3 scripts/hm_perf_queue_loadtest.py
"""
from __future__ import annotations

import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

OLLIE_URL = "http://192.168.1.168:11434"
MODEL = "qwen3:8b"  # the shared fleet workhorse, per ram-discipline.md

# Representative-but-modest synthetic prompt (~700 words, not the full
# measured p95 ~8.7k-token production prompt) -- keeps the test's total wall
# time reasonable while still exercising a real multi-hundred-token round
# trip against the real host. The num_ctx cap itself (10240) was already set
# from the REAL p95 measurement (api_costs.input_tokens, see
# ollama_provider.py) -- this test validates CONCURRENCY mechanics, not
# context-window sizing.
#
# CORRECTNESS NOTE (self-caught on first run): the prompt body must be
# UNIQUE per call. A first pass used a fixed, identical filler string for
# every call and saw a suspicious "6.63x speedup" with 2-worker p95 at
# 0.25s -- too fast for a genuine generation call. Root cause: Ollama's
# prompt-prefix KV-cache means an IDENTICAL prompt on a warm model (10m
# keep_alive) re-processes near-zero input tokens on the second call,
# confounding the 1-worker-then-2-worker comparison with a cache-warming
# effect, not a concurrency effect. Each call now gets a unique nonce
# appended so every request is a genuine cold-prefix generation.
_FILLER = ("The market showed mixed signals today with technology stocks "
           "leading gains while energy names lagged behind broader index "
           "performance. ") * 40


def _scan_prompt(nonce: str) -> str:
    return f"{_FILLER}\n[req-id {nonce}] Given this context, respond with exactly one word: HOLD."


def _wr_prompt(nonce: str) -> str:
    return (f"{_FILLER}\n[req-id {nonce}] Debate context: three other agents "
            "leaned bullish. Respond with exactly one word: HOLD.")


def _real_call(prompt: str) -> dict:
    import requests
    t0 = time.monotonic()
    r = requests.post(
        f"{OLLIE_URL}/api/generate",
        json={
            "model": MODEL, "prompt": prompt, "stream": False,
            "keep_alive": "10m",
            "options": {"temperature": 0.7, "num_ctx": 10240},
            "think": False,
        },
        timeout=(5, 120),
    )
    r.raise_for_status()
    elapsed = time.monotonic() - t0
    return {"elapsed": elapsed, "ok": True}


def _run_batch(num_workers: int, run_tag: str, n_scan: int = 4, n_wr: int = 2) -> dict:
    import engine.ollama_queue as oq
    # Fresh queue instance per run -- do NOT use the module-level get_queue
    # registry (that would reuse worker threads across the two comparison
    # runs and muddy the "before/after" isolation).
    q = oq.OllamaQueue(num_workers=num_workers)

    results: list[dict] = []
    errors: list[str] = []
    lock = threading.Lock()

    def _submit_scan(i: int):
        nonce = f"{run_tag}-scan-{i}-{time.time_ns()}"
        try:
            r = q.submit(lambda: _real_call(_scan_prompt(nonce)), model_id=MODEL, lane="scan")
            with lock:
                results.append({"lane": "scan", "i": i, **r})
        except Exception as e:
            with lock:
                errors.append(f"scan-{i}: {type(e).__name__}: {e}")

    def _submit_wr(i: int):
        nonce = f"{run_tag}-wr-{i}-{time.time_ns()}"
        try:
            r = q.submit(lambda: _real_call(_wr_prompt(nonce)), model_id=MODEL, lane="wr")
            with lock:
                results.append({"lane": "wr", "i": i, **r})
        except Exception as e:
            with lock:
                errors.append(f"wr-{i}: {type(e).__name__}: {e}")

    threads = []
    for i in range(n_scan):
        threads.append(threading.Thread(target=_submit_scan, args=(i,)))
    for i in range(n_wr):
        threads.append(threading.Thread(target=_submit_wr, args=(i,)))

    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=180)
    wall = time.monotonic() - t0

    scan_times = [r["elapsed"] for r in results if r["lane"] == "scan"]
    return {
        "num_workers": num_workers,
        "wall_s": round(wall, 2),
        "n_completed": len(results),
        "n_expected": n_scan + n_wr,
        "errors": errors,
        "scan_times": [round(x, 2) for x in scan_times],
        "scan_p95": round(statistics.quantiles(scan_times, n=20)[18], 2) if len(scan_times) >= 2 else (scan_times[0] if scan_times else None),
        "queue_status": q.status(),
    }


def _check_olliemax_journal_for_offload(since_marker: str, until_marker: str) -> str:
    """SSH to Ollie Max, grep the ollama journal in a TIGHT [since, until)
    window for VRAM-offload/eviction indicators. Read-only, no live-trading-
    path risk.

    CORRECTNESS NOTE (self-caught): an open-ended `--since` with no `--until`
    picked up ~2.5 HOURS of subsequent, unrelated live-fleet journal activity
    on a first pass (this script's own read of the output happened much
    later in wall-clock time than the test itself) -- easily misread as "the
    test caused this thrashing" when the vast majority of those entries were
    ordinary ongoing production traffic on the STILL-OLD, unrestarted trader
    process, nothing to do with this test. Both bounds now required."""
    try:
        cmd = [
            "ssh", "bigmac@192.168.1.168",
            f"journalctl -u ollama --since '{since_marker}' --until '{until_marker}' --no-pager "
            f"| grep -iE 'evict|offload|unload|out of memory|oom' || echo NO_MATCHES"
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return r.stdout.strip() or "(empty)"
    except Exception as e:
        return f"(journal check failed: {type(e).__name__}: {e})"


def main():
    since = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"=== HM-PERF-FLEET-THROUGHPUT load test — {since} ===\n")

    print("--- Baseline: num_workers=1 (simulates pre-2026-07-07 serial behavior) ---")
    r1 = _run_batch(num_workers=1, run_tag="baseline")
    print(f"wall={r1['wall_s']}s completed={r1['n_completed']}/{r1['n_expected']} "
          f"scan_times={r1['scan_times']} scan_p95={r1['scan_p95']}s "
          f"swaps={r1['queue_status']['total_model_swaps']} errors={r1['errors']}")

    print("\n--- New: num_workers=2 ---")
    r2 = _run_batch(num_workers=2, run_tag="newqueue")
    print(f"wall={r2['wall_s']}s completed={r2['n_completed']}/{r2['n_expected']} "
          f"scan_times={r2['scan_times']} scan_p95={r2['scan_p95']}s "
          f"swaps={r2['queue_status']['total_model_swaps']} errors={r2['errors']}")

    until = time.strftime("%Y-%m-%d %H:%M:%S")

    speedup = round(r1["wall_s"] / r2["wall_s"], 2) if r2["wall_s"] else None
    print(f"\nWall-time speedup (1w -> 2w): {speedup}x")

    print(f"\n--- Ollie Max journal check (VRAM offload/eviction, {since} to {until} ONLY) ---")
    journal = _check_olliemax_journal_for_offload(since, until)
    print(journal)

    print("\n=== Summary ===")
    print(f"1 worker : wall={r1['wall_s']}s  scan_p95={r1['scan_p95']}s  errors={len(r1['errors'])}")
    print(f"2 workers: wall={r2['wall_s']}s  scan_p95={r2['scan_p95']}s  errors={len(r2['errors'])}")
    print(f"speedup  : {speedup}x")
    print(f"journal  : {'CLEAN (no offload/evict/oom matches in the test window)' if journal in ('(empty)', 'NO_MATCHES') else 'FOUND MATCHES -- see above'}")


if __name__ == "__main__":
    main()
