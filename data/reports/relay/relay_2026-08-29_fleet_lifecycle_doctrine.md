# Relay — Fleet Lifecycle Doctrine + Tooling (2026-08-29)

Built in direct response to the day's P0 investigation (fleet emission
outage root-cause) turning up an undocumented, incompletely-reversed
2026-07-22 stand-down. Full detail in the commit message
(`cd5f253`) and `docs/FLEET_LIFECYCLE.md`; this is the short version.

## What shipped

- **`docs/FLEET_LIFECYCLE.md`** — the doctrine. Every agent/job state
  change must produce, atomically: a dated order doc, the live change,
  sentinel registration, dashboard consistency, and a tombstone (retire)
  or reversal checklist with resume-by/review-by (pause actions).
- **`scripts/fleet_lifecycle.py`** — the enforcing tool. `retire | bench |
  shadow | halt | revive <name> --reason "..."`. Validates before
  touching anything live, refuses partial work, auto-detects agent vs.
  job. Live-fire tested on real targets this session (caught and fixed
  two real bugs: idempotent revive against an already-loaded launchd job,
  and label resolution for `com.trademinds.*` jobs like `premarket`, not
  just `com.ollietrades.*`).
- **`fleet_lifecycle_ledger`** table (`data/trader.db`) — INSERT-only
  source of truth. Backfilled with all 107 current targets: 25 jobs (18
  revived through the real tool today, 3 pre-dead deferred,
  `riker-synthesis` retired, `crew` orphan, `premarket` deferred,
  `signal-center`'s 08-28 reactivation recorded) + all 82 `ai_players`
  rows (reconstructed from existing `halt_mode`/`halt_reason`, marked
  `backfilled=1` — honest about provenance).
- **`hm_ops_sentinel.py::check_fleet_lifecycle_drift`** — new. Flags any
  target where live state disagrees with the ledger's latest action
  (someone bypassed the tool), and any paused target past its
  resume_by/review_by. `check_launchd_jobs_health` now reads the ledger
  to skip staleness checks on intentionally-paused jobs instead of a
  hardcoded list.
- **`GET /api/fleet-lifecycle`** — read-only dashboard endpoint for job
  state (agent state was already dashboard-live via `halt_mode`). Trader
  restarted + verified healthy post-change.

Full suite: 1126 passed, 9 pre-existing failures (unchanged baseline).

## Still open — needs you

1. **Ollama plist fix** (P0 root cause #2 — the ~15x signal-throughput
   regression from the 08-27 `OLLAMA_FLASH_ATTENTION`/`KV_CACHE_TYPE`
   plist edit): commands are staged, waiting on you to run them (sudo-
   gated, this session has no passwordless access). See the earlier
   message in this session for the exact 6-line sequence.
2. **`mlx-qwen3` revival**: needs the original `mlx_lm.server` startup
   invocation from you — not recorded anywhere in this codebase.
3. Once both land, I'll re-measure signal timing to confirm the Ollama
   fix, and the options-gap report's items 2-4 (shadow-CSP activity
   floor, reactivate the three frozen options pathways, standardize
   Quark's graduation N) can proceed against a live, healthy fleet as
   you sequenced.
