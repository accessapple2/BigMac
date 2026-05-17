#!/usr/bin/env python3
"""HM-CN 2026-05-17 — Daily ministral-3:3b metrics aggregator.

Reports for the previous UTC day:
  - Total ministral-3:3b inference calls (trader_error.log latency lines)
  - Latency p50/p95/max
  - BUY/HOLD/SELL distribution (trader.log [OLLAMA] lines)
  - Fallback fire count (ollama-err for ministral)
  - Agents currently in is_fallback=1 state via ai_players DB

Usage:
    venv/bin/python3 scripts/daily_ministral_metrics.py
    venv/bin/python3 scripts/daily_ministral_metrics.py --date 2026-05-18

Companion to: reports/ministral_observation_day0_baseline.md
"""
from __future__ import annotations
import argparse
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median


ROOT = Path(__file__).resolve().parent.parent
TRADER_LOG = ROOT / "logs" / "trader.log"
TRADER_ERR = ROOT / "logs" / "trader_error.log"
TRADER_DB = ROOT / "data" / "trader.db"
MODEL = "ministral-3:3b"


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[idx]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: yesterday UTC)")
    args = parser.parse_args()

    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    day_str = day.strftime("%Y-%m-%d")

    # --- Latency from trader_error.log (HM-CN ollama_provider latency lines) ---
    latencies = []
    agent_calls = Counter()
    if TRADER_ERR.exists():
        with TRADER_ERR.open(errors="replace") as f:
            for line in f:
                if day_str not in line or "ollama_call" not in line:
                    continue
                m = re.search(r"model=(\S+)\s+agent=(\S+)\s+wall=([\d.]+)s", line)
                if not m:
                    continue
                model, agent, wall = m.group(1), m.group(2), float(m.group(3))
                if model != MODEL:
                    continue
                latencies.append(wall)
                agent_calls[agent] += 1

    # --- Signal distribution from trader.log [OLLAMA] lines ---
    distribution = Counter()
    if TRADER_LOG.exists():
        with TRADER_LOG.open(errors="replace") as f:
            for line in f:
                if day_str not in line or "[OLLAMA]" not in line or MODEL not in line:
                    continue
                m = re.search(r"(BUY|SELL|HOLD)\((\d+)/10\)", line)
                if m:
                    distribution[(m.group(1), int(m.group(2)))] += 1

    # --- Fallback errors for ministral ---
    err_count = 0
    for log_path in (TRADER_LOG, TRADER_ERR):
        if not log_path.exists():
            continue
        with log_path.open(errors="replace") as f:
            for line in f:
                if day_str in line and "ollama-err" in line and MODEL in line:
                    err_count += 1

    # --- DB fallback state ---
    fallback_agents = []
    try:
        conn = sqlite3.connect(TRADER_DB)
        rows = conn.execute(
            "SELECT id, is_fallback, halt_mode FROM ai_players WHERE model_id=?",
            (MODEL,),
        ).fetchall()
        conn.close()
        total_assigned = len(rows)
        fallback_agents = [r[0] for r in rows if r[1] == 1]
        active_count = sum(1 for r in rows if r[2] == "active")
    except sqlite3.Error as e:
        print(f"WARN: DB read failed: {e}", file=sys.stderr)
        total_assigned = active_count = None

    # --- Output ---
    print(f"=== ministral-3:3b daily metrics — {day_str} ===")
    print(f"  agents assigned (ai_players.model_id={MODEL}): {total_assigned}")
    print(f"  agents halt_mode='active':                     {active_count}")
    print(f"  agents currently is_fallback=1:                {len(fallback_agents)}")
    if fallback_agents:
        print(f"    -> {fallback_agents}")
    print()
    print(f"  total calls (from ollama_call latency log):    {len(latencies)}")
    print(f"  errors (ollama-err lines):                     {err_count}")
    if latencies:
        print(f"  latency p50 / p95 / max (sec): "
              f"{median(latencies):.2f} / {percentile(latencies, 95):.2f} / {max(latencies):.2f}")
    else:
        print("  latency: n/a (no calls)")
    print()
    if agent_calls:
        print("  per-agent call counts:")
        for agent, n in agent_calls.most_common():
            print(f"    {agent:<32} {n}")
        print()
    total_sig = sum(distribution.values())
    if total_sig:
        print(f"  signal distribution ({total_sig} parsed):")
        for sig in ("BUY", "HOLD", "SELL"):
            sig_total = sum(n for (s, _c), n in distribution.items() if s == sig)
            pct = 100 * sig_total / total_sig
            print(f"    {sig:<5} {sig_total:5d} ({pct:5.1f}%)")
        print("  by signal+confidence (top 10):")
        for (sig, conf), n in sorted(distribution.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    {sig}({conf}/10): {n}")
    else:
        print("  signal distribution: n/a (no parsed [OLLAMA] lines today)")


if __name__ == "__main__":
    main()
