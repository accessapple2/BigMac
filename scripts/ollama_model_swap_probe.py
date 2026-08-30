#!/usr/bin/env python3
"""scripts/ollama_model_swap_probe.py — HM-OLLAMA-SWAP-INSTRUMENTATION-2026-08-29.

Built specifically so Monday's live signal cadence gives a DEFINITIVE
verdict on the cross-model-eviction hypothesis (engine/ai_brain.py calls
into Ollama; a genuinely different model interleaving with the qwen3:8b-
alias-family under OLLAMA_MAX_LOADED_MODELS can force an expensive real
reload -- confirmed live-costs ~8.8s in a synthetic test 2026-08-29, but
not yet confirmed as something that actually happens in production
traffic). Without this, correlating "why was this signal gap 90s" against
"was a model evicted right before it" is a wall-clock guess. With it,
it's a join.

Polls Ollama's own /api/ps (the resident-model list) every POLL_SECONDS,
diffs the resident set against the previous poll, and logs a row per
LOAD/EVICT transition to ollama_model_swap_log (data/trader.db) --
timestamped, so tests/reports can join it directly against
signals.created_at gaps.

Runs as a persistent loop (not a cron one-shot) -- swaps can happen
within seconds, a 5-10 min cron cadence would miss most of them. Meant
to be supervised by launchd (KeepAlive=true), not run manually long-term.

Exit codes:
  0 - clean shutdown (SIGTERM/SIGINT)
  1 - fatal error before the loop could start
"""
from __future__ import annotations

import json
import signal
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trader.db"
OLLAMA_PS_URL = "http://127.0.0.1:11434/api/ps"
POLL_SECONDS = 10

_running = True


def _handle_signal(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS ollama_model_swap_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        event TEXT NOT NULL,          -- 'loaded' | 'evicted'
        model_name TEXT NOT NULL,
        model_digest TEXT,
        model_size_bytes INTEGER,
        resident_models_json TEXT     -- full resident set at the time of this event, for context
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ollama_swap_ts ON ollama_model_swap_log(ts)"
    )
    conn.commit()


def _poll_resident() -> dict[str, dict] | None:
    """Returns {model_name: {digest, size}} or None on any failure
    (Ollama down, network hiccup) -- the probe just skips that tick,
    never crashes the loop over a transient failure."""
    try:
        with urllib.request.urlopen(OLLAMA_PS_URL, timeout=5) as r:
            data = json.loads(r.read())
        out = {}
        for m in data.get("models", []):
            out[m["name"]] = {"digest": m.get("digest"), "size": m.get("size")}
        return out
    except Exception:
        return None


def main() -> int:
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    _ensure_table(conn)

    prev: dict[str, dict] | None = None
    print(f"[ollama-swap-probe] started, polling every {POLL_SECONDS}s")

    while _running:
        cur = _poll_resident()
        if cur is not None and prev is not None:
            prev_names = set(prev)
            cur_names = set(cur)
            evicted = prev_names - cur_names
            loaded = cur_names - prev_names
            for name in evicted:
                conn.execute(
                    "INSERT INTO ollama_model_swap_log "
                    "(event, model_name, model_digest, model_size_bytes, resident_models_json) "
                    "VALUES ('evicted', ?, ?, ?, ?)",
                    (name, prev[name].get("digest"), prev[name].get("size"), json.dumps(sorted(cur_names))),
                )
                print(f"[ollama-swap-probe] EVICTED {name}")
            for name in loaded:
                conn.execute(
                    "INSERT INTO ollama_model_swap_log "
                    "(event, model_name, model_digest, model_size_bytes, resident_models_json) "
                    "VALUES ('loaded', ?, ?, ?, ?)",
                    (name, cur[name].get("digest"), cur[name].get("size"), json.dumps(sorted(cur_names))),
                )
                print(f"[ollama-swap-probe] LOADED {name}")
            if evicted or loaded:
                conn.commit()
        if cur is not None:
            prev = cur
        time.sleep(POLL_SECONDS)

    conn.close()
    print("[ollama-swap-probe] shutting down cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
