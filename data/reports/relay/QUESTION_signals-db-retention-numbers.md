# Question: signals.db retention — the "2.19 GB" figure doesn't match live measurement

**Date:** 2026-09-10
**Context:** Backlog item "signals.db retention | decision | unslotted."
Instruction this morning: propose a retention policy with numbers before
deciding, since RULE #1 means the answer is archive/roll, never delete.

**Before drafting a policy, measured the actual numbers and they don't
match what was given ("2.19 GB, with none at all"):**

- There is no live `signals.db` file with real data in it. `data/signals.db`
  exists but is **0 bytes** — an empty leftover file, not the real data
  store.
- The real signal data lives in `trader.db`'s `signals` and `signals_v2`
  tables (per `CLAUDE.md`'s RULE #1 sacred-table list). Measured via
  `dbstat`:
  - `signals`: 108,042 rows, **87.7 MB**, date range 2026-03-11 →
    2026-09-10 (today, live)
  - `signals_v2`: **30.3 MB**
  - Combined: **~118 MB**, not 2.19 GB
- For context, the other RULE #1 tables in the same DB: `decision_audit`
  73.7 MB (134,336 rows), `crew_decisions` 9.7 MB, `notifications` 8.2 MB,
  `agent_ratings` 5.4 MB, `trades` 1.0 MB, `desk_execution_trace` 28 KB.
- `data/trader.db` itself (the whole file, every table) is **1.13 GB**
  (1,210,445,824 bytes) — still short of 2.19 GB even counting everything
  in the database, not just signals.
- No other file named `signals*.db*` exists anywhere in the repo at any
  size close to 2.19 GB (checked with `find`).

**So "2.19 GB" doesn't match `signals`/`signals_v2` alone, doesn't match
`trader.db` as a whole, and no other candidate file exists.** Possible
explanations, unverified: a different measurement method (e.g. `du` on a
directory that includes rotated backups or WAL/SHM artifacts, several of
which sit alongside `trader.db` in `data/` — see `data/trader.db.*.gz`
backup snapshots, some tens-of-MB each, which could sum close to that
figure if bundled together), or a number from a different box/path
entirely.

## Options

- **Reconcile the figure, then get the retention proposal** — tell me
  what you measured and how (file/path, tool used), so the policy is
  built on a number both of us can reproduce.
- **Proceed on the `signals`+`signals_v2` numbers above (Recommended if
  no other source was intended)** — ~118 MB combined, growing ~10-45k
  rows/month (see per-month counts logged in this session), oldest row
  2026-03-11. I can draft a proposal (archive-to-parquet-or-cold-table
  cutoff, e.g. rows older than N months moved to an archive table/file,
  never deleted per RULE #1) against these real numbers.

No policy numbers drafted yet pending which figure is authoritative.
