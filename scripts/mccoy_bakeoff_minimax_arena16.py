#!/usr/bin/env python3
"""HM-XO-PLAN-2026-09 Phase 2 -- MiniMax-M3 arm, run against olliemax's fixed
16-prompt arena_calls.jsonl set, for a clean apples-to-apples comparison
against Trip's (olliemax/Model Works) uncontended qwen3:8b (721 calls) and
fin-r1 (69 calls) numbers -- see relay_2026-09-11_mccoy_bakeoff_run1.md and
the session-close handoff doc for why run 1's mixed-set numbers needed a
clean redo. This script is MiniMax-only: it does not touch olliemax at all
(remote API call, zero GPU load), so it carries none of run 1's contention
risk and needed no coordination window.

Same real prompt Trip used verbatim (exact request body captured live from
the Arena, /home/olliemax/modelworks/fleet_checks/mccoy/arena_calls.jsonl,
pulled to /tmp/arena_calls.jsonl this session) -- not a freshly-built prompt
against today's screened list, so this run's 16 symbols exactly match
Trip's 16, letting the three arms' numbers sit side by side without a
different-prompt-set confound.

Scored with the fleet's own parse_decision() (engine/providers/base.py),
same as Trip's methodology and run 1's. Logs to the same additive-only
mccoy_bakeoff_log table run 1 used, tagged with a distinct run_id and
source so the two runs are distinguishable in the DB.

Usage:
    .venv/bin/python3 scripts/mccoy_bakeoff_minimax_arena16.py [--cost-cap USD]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from engine.providers.minimax_provider import MiniMaxProvider  # noqa: E402

DB_PATH = REPO_ROOT / "data" / "trader.db"
ARENA_CALLS_PATH = Path("/tmp/arena_calls.jsonl")
SOURCE = "scripts/mccoy_bakeoff_minimax_arena16.py"


def _ensure_table(conn: sqlite3.Connection) -> None:
    # Same schema mccoy_bakeoff_run.py already created -- CREATE TABLE IF
    # NOT EXISTS is a no-op if it's already there (it is, from run 1).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mccoy_bakeoff_log (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id                 TEXT NOT NULL,
            ts                     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            symbol                 TEXT NOT NULL,
            price                  REAL,
            regime                 TEXT,
            arm                    TEXT NOT NULL,
            wall_s                 REAL,
            ok                     INTEGER,
            error                  TEXT,
            action                 TEXT,
            confidence             REAL,
            timeframe              TEXT,
            invalidation           TEXT,
            format_valid           INTEGER,
            raw_response           TEXT,
            input_tokens           INTEGER,
            visible_output_tokens  INTEGER,
            reasoning_tokens       INTEGER,
            cost_usd               REAL,
            source                 TEXT NOT NULL DEFAULT 'scripts/mccoy_bakeoff_run.py'
        )
    """)
    conn.commit()


def _load_arena_calls() -> list[dict]:
    if not ARENA_CALLS_PATH.exists():
        print(f"[abort] {ARENA_CALLS_PATH} not found -- pull it first: "
              f"scp olliemax:~/modelworks/fleet_checks/mccoy/arena_calls.jsonl {ARENA_CALLS_PATH}")
        sys.exit(1)
    calls = []
    with ARENA_CALLS_PATH.open() as f:
        for line in f:
            line = line.strip()
            if line:
                calls.append(json.loads(line))
    return calls


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cost-cap", type=float, default=2.0, help="MiniMax daily cost cap USD (default 2.0)")
    args = parser.parse_args()

    run_id = f"arena16-minimax-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    print(f"run_id={run_id}")

    calls = _load_arena_calls()
    print(f"[loaded] {len(calls)} arena calls from {ARENA_CALLS_PATH}")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    _ensure_table(conn)

    provider = MiniMaxProvider(player_id="bakeoff-minimax-m3", daily_cost_cap=args.cost_cap)

    results = []
    for call in calls:
        symbol = call["symbol"]
        prompt = call["request"]["body"]["prompt"]
        t0 = time.time()
        try:
            raw = provider.call_model(prompt)
            wall = time.time() - t0
        except Exception as e:
            wall = time.time() - t0
            print(f"  [FAIL] {symbol:<8} wall={wall:6.2f}s error={type(e).__name__}: {e}")
            conn.execute(
                "INSERT INTO mccoy_bakeoff_log (run_id, symbol, arm, wall_s, ok, error, source) "
                "VALUES (?,?,?,?,?,?,?)",
                (run_id, symbol, "minimax-m3", wall, 0, f"{type(e).__name__}: {e}", SOURCE),
            )
            conn.commit()
            results.append({"symbol": symbol, "ok": False})
            continue

        has_decision_line = any(
            line.strip().lower().startswith("decision:") for line in (raw or "").split("\n")
        )
        try:
            decision = provider.parse_decision(raw, symbol=symbol, price=None)
            parse_ok = decision.action in ("BUY", "BUY_CALL", "BUY_PUT", "SHORT", "HOLD")
        except Exception as e:
            print(f"  [PARSE-FAIL] {symbol:<8} wall={wall:6.2f}s error={e}")
            conn.execute(
                "INSERT INTO mccoy_bakeoff_log (run_id, symbol, arm, wall_s, ok, error, raw_response, source) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (run_id, symbol, "minimax-m3", wall, 0, f"parse_decision {type(e).__name__}: {e}", raw, SOURCE),
            )
            conn.commit()
            results.append({"symbol": symbol, "ok": False})
            continue

        format_valid = bool(has_decision_line and parse_ok)
        print(f"  [OK]   {symbol:<8} wall={wall:6.2f}s action={decision.action:<9} "
              f"conf={decision.confidence} format_valid={format_valid}")
        conn.execute(
            "INSERT INTO mccoy_bakeoff_log "
            "(run_id, symbol, arm, wall_s, ok, action, confidence, timeframe, invalidation, "
            "format_valid, raw_response, input_tokens, visible_output_tokens, reasoning_tokens, cost_usd, source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, symbol, "minimax-m3", wall, 1, decision.action, decision.confidence,
             decision.timeframe, decision.invalidation, int(format_valid), raw,
             provider.total_input_tokens, provider.total_visible_output_tokens,
             provider.total_reasoning_tokens, provider.total_cost_usd, SOURCE),
        )
        conn.commit()
        results.append({
            "symbol": symbol, "ok": True, "action": decision.action,
            "confidence": decision.confidence, "wall_s": wall, "format_valid": format_valid,
        })

    conn.close()

    ok_results = [r for r in results if r["ok"]]
    n = len(results)
    n_ok = len(ok_results)
    print(f"\n=== SUMMARY (run_id={run_id}) ===")
    print(f"calls: {n}  ok: {n_ok}  failed: {n - n_ok}")
    if ok_results:
        n_valid = sum(1 for r in ok_results if r["format_valid"])
        actions = {}
        for r in ok_results:
            actions[r["action"]] = actions.get(r["action"], 0) + 1
        avg_conf = sum(r["confidence"] for r in ok_results) / n_ok
        avg_wall = sum(r["wall_s"] for r in ok_results) / n_ok
        print(f"strict contract valid: {n_valid}/{n_ok} ({100*n_valid/n_ok:.1f}%)")
        print(f"parsed actions: {actions}")
        print(f"confidence mean: {avg_conf:.2f}")
        print(f"wall_s mean: {avg_wall:.1f}")
    print(f"total cost: ${provider.total_cost_usd:.4f}")
    print(f"input tokens: {provider.total_input_tokens}  "
          f"visible output: {provider.total_visible_output_tokens}  "
          f"reasoning: {provider.total_reasoning_tokens}")
    if provider.total_visible_output_tokens > 0:
        ratio = provider.total_reasoning_tokens / provider.total_visible_output_tokens
        print(f"reasoning:visible ratio: {ratio:.1f}:1")

    return 0


if __name__ == "__main__":
    sys.exit(main())
