# Fleet Lifecycle Doctrine

**Established:** 2026-08-29, directly in response to the 2026-07-22
stand-down class of mystery: a state change (fleet-wide, deliberate, and
correctly reasoned at the time) that produced no durable, queryable
record of what was touched, why, or when it should be revisited — so five
weeks later "why is X off?" took a multi-hour forensic investigation
(trader.log line-counts, memory-file archaeology, `launchctl
print-disabled` spelunking) instead of one lookup. Full incident:
`data/reports/relay/relay_2026-08-29_2026-07-21-22-postmortem.md`.

## The rule

**Every agent or job state change — retire, bench, shadow, halt, revive —
MUST produce, in one action, all five of:**

1. **A dated order doc** (`docs/orders/ORDER_<date>_<action>_<type>_<name>.md`)
   with the reason, written before the live change is attempted.
2. **The actual state change** — `ai_players.halt_mode` for an agent,
   `launchctl enable/disable` + `bootstrap/bootout` for a job.
3. **Sentinel registration or deregistration**, so what's monitored
   matches what's actually running. In practice this is automatic: the
   sentinel (`scripts/hm_ops_sentinel.py`) reads its expectations live
   from the ledger (below), so writing the ledger row *is* the
   registration step — nothing separate to remember or forget.
4. **The dashboard reflecting the same source of truth.** For agents,
   this was already true by construction — every dashboard panel reads
   `ai_players.halt_mode` live, never a cache. For jobs, `GET
   /api/fleet-lifecycle` exposes the ledger read-only for any future
   dashboard surface.
5. **For retirement: a tombstone.** For a pause (bench/shadow/halt): an
   explicit reversal checklist with a resume-by or review-by date. Both
   are the order doc from step 1 — the tool below writes the right shape
   automatically based on the action.

**The test of success:** *"why is X off?"* must never again require
forensics. The answer is one lookup — `scripts/fleet_lifecycle.py status
<name>` — with a date, a reason, and a name on it.

## The tool

```
scripts/fleet_lifecycle.py <action> <name> --reason "..." \
    [--type agent|job] [--resume-by YYYY-MM-DD] [--review-by YYYY-MM-DD] [--dry-run]

scripts/fleet_lifecycle.py status <name>
scripts/fleet_lifecycle.py list [--type agent|job] [--action ACTION]
```

Actions: `retire`, `bench`, `shadow`, `halt`, `revive`. `bench`/`shadow`
are agent-only (crew-roster concepts that don't map to a launchd job);
the tool rejects them for `--type job` with a clear error rather than
doing something ambiguous.

**Target-type auto-detection:** if `<name>` matches an `ai_players.id`,
it's an agent; if a `com.ollietrades.<name>.plist` or
`com.trademinds.<name>.plist` exists in `~/Library/LaunchAgents`, it's a
job. Pass `--type` explicitly to disambiguate or to act on a job whose
plist doesn't exist yet.

**Pause-type actions require a date.** `bench`/`shadow`/`halt` refuse to
run without at least one of `--resume-by`/`--review-by` — a pause with no
review date is exactly how the 07-22 stand-down sat for five weeks past
its own "post-trip" intent.

**Refuses partial work.** Validates target existence, action validity for
the target type, a non-blank reason, and required dates *before* touching
anything live. If the live change fails after the order doc is written,
the doc is marked `FAILED` and no ledger row is inserted — a ledger row
must never claim a change that didn't actually happen. If the ledger
write itself fails *after* a successful live change (rare), the tool
prints a `CRITICAL` message rather than pretending success; that gap
needs manual reconciliation, not a silent retry.

**Action → mechanism:**

| Action | Agent (`ai_players`) | Job (launchd) |
|---|---|---|
| `active` (backfill baseline only) | `halt_mode='active'` | n/a |
| `retire` | `halt_mode='full'`, tombstone doc | `disable` + `bootout`, tombstone doc |
| `bench` | `halt_mode='full'`, reversal checklist | — (agent-only) |
| `shadow` | `halt_mode='exit_only'` (lets the audition tracker's backtest-replay mechanism keep accruing data — see `engine/crew/weekly_tuning_crew.py`) | — (agent-only) |
| `halt` | `halt_mode='full'`, reversal checklist | `disable` + `bootout`, reversal checklist |
| `revive` | `halt_mode='active'` | `enable` + `bootstrap` (idempotent — skips bootstrap if already loaded) |

## The ledger

`fleet_lifecycle_ledger` (table in `data/trader.db`), written exclusively
by the tool (or the one-time backfill script). INSERT ONLY — a target's
current state is its latest row by `created_at`; nothing is ever mutated
or deleted, matching the sacred-data rule elsewhere in this project.

```sql
CREATE TABLE fleet_lifecycle_ledger (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type   TEXT    NOT NULL,   -- 'agent' | 'job'
    target_name   TEXT    NOT NULL,
    action        TEXT    NOT NULL,   -- 'active'|'retire'|'bench'|'shadow'|'halt'|'revive'
    reason        TEXT    NOT NULL,
    order_doc     TEXT,               -- path to the dated order doc (NULL only for backfill)
    resume_by     TEXT,
    review_by     TEXT,
    backfilled    INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    created_by    TEXT    NOT NULL DEFAULT 'fleet_lifecycle.py'
);
```

`backfilled=1` rows are reconstructed from pre-doctrine state (existing
`ai_players.halt_mode`/`halt_reason`, or the 2026-07-22 stand-down's own
documentation for jobs) — honest about provenance, not presented as real
dated orders. Every state change from 2026-08-29 onward goes through the
tool and gets `backfilled=0`, a real `order_doc`, and (for pauses) a real
date.

## Drift enforcement

**"Manual plist/cron edits to fleet jobs become a sentinel finding of
their own."** `scripts/hm_ops_sentinel.py::check_fleet_lifecycle_drift`
runs every sentinel cycle (5-10 min cron) and checks, both ways:

- **Drift:** does the ledger's latest recorded action for a target match
  its *actual* live state (`launchctl print-disabled` for jobs,
  `ai_players.halt_mode` for agents)? A mismatch means someone bypassed
  `scripts/fleet_lifecycle.py` — hand-edited a plist, ran raw
  `launchctl`/SQL, or a future stand-down forgets to record itself. Fires
  `sentinel_lifecycle_drift`.
- **Overdue review:** does a paused target (`bench`/`shadow`/`halt`) have
  a `resume_by` or `review_by` date that's already passed? Fires
  `sentinel_lifecycle_review_overdue`. Per the order doc's own language:
  *"a sentinel finding against this target before its review-by date is a
  false alarm; after it, it is a legitimate 'this pause was forgotten'
  alert."*

`check_launchd_jobs_health` (staleness — is a job's log still being
written on schedule) reads the ledger too, but only to *skip* targets the
ledger says are intentionally off — an intentionally-halted job going log
stale is the plan working, not a finding.

## What this doesn't cover (yet)

- No enforcement that every state change actually goes through the tool
  — the drift check catches it *after the fact* (next sentinel cycle),
  not before. There's no pre-commit-style block on a raw `UPDATE
  ai_players` or `launchctl disable`.
- The dashboard has no dedicated visual panel for job lifecycle state yet
  — `GET /api/fleet-lifecycle` exists for a future one to consume, but
  nothing renders it today. Agent state was already fully covered by
  existing panels (they read `halt_mode` live).
- `shadow` for an agent sets `halt_mode='exit_only'`, which is necessary
  but not sufficient for the full CSP-shadow-bakeoff pattern
  (`engine/shadow_csp_scorecard.py`) — that pattern also needs a
  dedicated non-executing sim loop wired into the scheduler per agent,
  which this tool does not build. It sets the state; it doesn't build new
  execution paths.
