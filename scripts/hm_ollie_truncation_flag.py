#!/usr/bin/env python3
"""HM-OLLIE-TRUNCATION-2026-09-11: load olliemax's truncation CSV and flag
affected decision_audit rows. Additive only -- RULE #1 (CLAUDE.md): new
table + new columns, never a DELETE or an UPDATE of an existing decision's
substantive fields.

Source: olliemax's ~/modelworks/fleet_checks/ollama_churn/FINDINGS_FOR_SCOTTY.md
and truncated_prompts_2026-09-09_to_2026-09-11.csv (4,575 rows) -- Ollama
silently truncated McCoy's (ollama-plutus) screened-scan prompts to ~30% of
their content, 2026-09-09 07:42 MST through 2026-09-11 06:50 MST, with no
error raised anywhere. Root cause fixed separately in
engine/providers/ollama_provider.py (_DEFAULT_NUM_CTX 10240 -> 24576).

What this does:
  1. Creates `ollama_prompt_truncations` and loads the CSV into it verbatim
     -- the raw source of truth, in-DB, for any future re-derivation.
  2. Adds `prompt_truncation_flag` / `prompt_truncation_detail` columns to
     `decision_audit` (nullable, additive -- existing columns/rows untouched).
  3. Flags decision_audit rows for player_id='ollama-plutus' (McCoy, the
     only agent with a live, recent decision_audit trail) whose created_at
     (UTC -- decision_audit uses SQLite CURRENT_TIMESTAMP) falls inside a
     day's documented truncation window (converted from the CSV's MST
     timestamps, +25min trailing buffer for generation/queue lag observed
     empirically against real rows).

Honest scope note (see the relay doc for the full writeup): this is a
TIME-WINDOW flag, not a per-call exact match. The CSV has no symbol or
player_id column, and McCoy's screened-scan prompt-building path does not
log through engine/cost_tracker.py (a separate, real gap -- its api_costs
rows for this window top out at ~10.4K estimated tokens, nowhere near the
CSV's 15-18K real-tokenizer counts, confirming the screened scan bypasses
cost_tracker entirely), so no table in this DB carries a token count or
timestamp precise enough for row-exact correlation. Worf (qwen3-8b-flash)
shared the 78b329e716e7 (qwen3:30b-a3b) bucket on 09-09 09:19-17:00 per
config.py's HM-OLLIE-30B-BAKEOFF-REVERT comment, but has zero decision_audit
rows since 2026-05-07 -- there is nothing live to flag for Worf in this
table; noted as an open gap, not silently dropped.
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

DB = "data/trader.db"
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else (
    "/private/tmp/claude-501/-Users-bigmac/8aa41418-33aa-4686-ac46-4c0cdce6a2a6/"
    "scratchpad/truncated_prompts.csv"
)

# Per-day windows from FINDINGS_FOR_SCOTTY.md's Priority 1 table, local MST
# (olliemax's CSV timestamps carry an explicit -07:00 offset -- MST,
# no DST in Arizona). decision_audit.created_at is UTC (SQLite
# CURRENT_TIMESTAMP) -- MST + 7h = UTC. +25min trailing buffer on each
# window's end for generation/queue lag (empirically ~15min observed
# between a truncation timestamp and the nearest decision_audit cluster
# for the same call).
_BUFFER = timedelta(minutes=25)
WINDOWS_MST = [
    # (start, end, player_ids, label)
    ("2026-09-09 07:42:00", "2026-09-09 20:42:00", ("ollama-plutus",), "qwen3_8b_default_ctx"),
    ("2026-09-10 02:58:00", "2026-09-10 19:39:00", ("ollama-plutus",), "qwen3_8b_default_ctx"),
    ("2026-09-11 02:31:00", "2026-09-11 06:50:00", ("ollama-plutus",), "qwen3_8b_default_ctx"),
    ("2026-09-09 09:19:00", "2026-09-09 17:00:00", ("ollama-plutus", "qwen3-8b-flash"), "qwen3_30b_bakeoff_shared"),
]


def _mst_to_utc(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") + timedelta(hours=7)


def main() -> None:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # --- Step 1: raw CSV table (additive, source of truth) -----------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ollama_prompt_truncations (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts_mst            TEXT NOT NULL,
            weights                 TEXT NOT NULL,
            num_ctx                 INTEGER NOT NULL,
            original_prompt_tokens  INTEGER NOT NULL,
            kept_tokens             INTEGER NOT NULL,
            kept_pct                REAL NOT NULL,
            loaded_at               TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            source                  TEXT NOT NULL DEFAULT
                'olliemax:~/modelworks/fleet_checks/ollama_churn/truncated_prompts_2026-09-09_to_2026-09-11.csv'
        )
    """)
    cur.execute("SELECT count(*) FROM ollama_prompt_truncations")
    already_loaded = cur.fetchone()[0]
    if already_loaded:
        print(f"[skip] ollama_prompt_truncations already has {already_loaded} rows -- not re-loading")
    else:
        rows = []
        with open(CSV_PATH, newline="") as f:
            for row in csv.DictReader(f):
                rows.append((
                    row["timestamp"], row["weights"], int(row["num_ctx"]),
                    int(row["original_prompt_tokens"]), int(row["kept_tokens"]),
                    float(row["kept_pct"]),
                ))
        cur.executemany(
            "INSERT INTO ollama_prompt_truncations "
            "(event_ts_mst, weights, num_ctx, original_prompt_tokens, kept_tokens, kept_pct) "
            "VALUES (?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
        print(f"[loaded] {len(rows)} rows into ollama_prompt_truncations")

    # --- Step 2: additive flag columns on decision_audit --------------------
    cols = {r[1] for r in cur.execute("PRAGMA table_info(decision_audit)")}
    if "prompt_truncation_flag" not in cols:
        cur.execute("ALTER TABLE decision_audit ADD COLUMN prompt_truncation_flag TEXT")
        print("[schema] added decision_audit.prompt_truncation_flag")
    if "prompt_truncation_detail" not in cols:
        cur.execute("ALTER TABLE decision_audit ADD COLUMN prompt_truncation_detail TEXT")
        print("[schema] added decision_audit.prompt_truncation_detail")
    conn.commit()

    # --- Step 3: window-flag decision_audit (UPDATE of new columns only) ---
    total_flagged = 0
    for start_mst, end_mst, player_ids, label in WINDOWS_MST:
        start_utc = _mst_to_utc(start_mst)
        end_utc = _mst_to_utc(end_mst) + _BUFFER
        placeholders = ",".join("?" for _ in player_ids)
        detail = (
            f"window-flagged, not per-call exact match; see "
            f"data/reports/relay/relay_2026-09-11_ollie_truncation_flag.md; "
            f"src=ollama_prompt_truncations label={label}"
        )
        cur.execute(
            f"""UPDATE decision_audit
                SET prompt_truncation_flag = ?,
                    prompt_truncation_detail = ?
                WHERE player_id IN ({placeholders})
                  AND created_at BETWEEN ? AND ?
                  AND prompt_truncation_flag IS NULL""",
            (label, detail, *player_ids,
             start_utc.strftime("%Y-%m-%d %H:%M:%S"), end_utc.strftime("%Y-%m-%d %H:%M:%S")),
        )
        n = cur.rowcount
        total_flagged += n
        print(f"[flag] {label} {player_ids} {start_mst}->{end_mst} MST "
              f"({start_utc}..{end_utc} UTC): {n} decision_audit rows flagged")
    conn.commit()

    cur.execute("SELECT prompt_truncation_flag, count(*) FROM decision_audit "
                "WHERE prompt_truncation_flag IS NOT NULL GROUP BY prompt_truncation_flag")
    print("\n[summary]")
    for label, n in cur.fetchall():
        print(f"  {label}: {n}")
    print(f"  total: {total_flagged}")
    conn.close()


if __name__ == "__main__":
    main()
