# Relay — Morning check-in, 2026-09-10

## What this covers
Follow-up to `relay_2026-09-09_overnight_session.md`'s top-priority open
item (McCoy scan-path trace) plus a live 05:41 MST check the Admiral ran
directly, three decisions on standing backlog items, and one new
low-priority finding. Read-only + doc updates only — nothing touched on
the trading path.

## Closed

### McCoy scan-path — NOT stalled, closing the open question from last night
Admiral's 05:41 live check: 844 `decision_audit` rows for McCoy in the
prior 8 hours, calls at 05:36/05:37/05:40 running 1.75-3.39s on
`plutus-v1`, `queue_wait` 0.00. Last night's 64-minute quiet window
(03:42 → post-restart) was after-hours quiet, not a fault. **Closing as
non-issue.**

**Re-scoped, not dropped:** the underlying question — what IS McCoy's real
cadence by hour, and what drives it — carries forward as a required input
to `docs/XO_PLAN_2026-09.md` Phase 1.2 (screen-then-score redesign), filed
there directly. The `_SCAN_TIER1/2/3` mechanism alone doesn't explain
McCoy's observed frequency; Phase 1.2's twice-daily cut can't be sized
correctly without knowing what's driving today's cadence.

### Archive second copy — rsync completed
257 GB, "speedup is 1.00", full Plutus lineage confirmed present on
olliemax. Removed from the `docs/XO_BACKLOG.md` consolidated table.

### Disk-alert volume — housekeeping close
Investigated and closed in substance on 2026-09-09 (relay doc §7, no
defect found — sentinel already reads the correct shared APFS pool), but
was left in the backlog table overnight without being marked closed.
Closed now for the record; no new work done here today.

### gex_collector cron fix — resolved, was already retired 2026-08-30
Admiral's decision: retire it, don't leave it acked forever. Checked the
live crontab (not just docs, which is what the 9/9 overnight pass relied
on and got wrong): the cron line is already commented with a clear,
dated, reasoned annotation —

```
# RETIRED 2026-08-30 (HM-GEX-RETIRED): /v3/snapshot/options 403
# NOT_AUTHORIZED -- options chain requires a paid Polygon/Massive plan,
# cancelled 7/22. Verified 8/30. Data preserved in data/flow_gex.db; see
# XO_BACKLOG.
```

`scripts/hm_gex_daily_collect.py` still exists on disk (not deleted, per
Archive Convention), just no longer cron-invoked. The 2026-09-09
overnight relay's claim that "the script file no longer exists" was
stale/incorrect — it was checking a state that predated the 08-29
restoration commit (`85b5f7e`, see `relay_2026-08-29_gex-collector-dead-
since-0722.md`).

**The actual gap this session closes:** the 2026-08-30 retirement decision
was applied at the crontab level but never written up in
`docs/XO_BACKLOG.md` — which is exactly why the 9/9 session re-flagged it
as still open. Fixed: the backlog table's "8/30 verify-or-close list" row
now documents the retirement with its reason and pointer, closing that
sub-item for good. Root Polygon options-tier entitlement (cancelled 7/22,
separate from the 9/9-night Stocks-tier Massive upgrade, which is
stocks-only per its name and does not appear to restore options access)
remains a billing decision, not a code task — unchanged.

## New, low priority — offhost backup slowdown + DB count gap

Last night's `offhost_backup.sh` run took 9,638s vs. ~1,065s a week ago,
and reported 9 DBs backed up, not the usual 10. All integrity checks
passed. Most likely explanation is rsync contention on the X9, not a
backup-content problem, but not confirmed. Filed in
`docs/XO_BACKLOG.md` — **after close today:** identify which DB was
skipped, and check whether tonight's run returns to normal timing/count.

## Open — needs Admiral input before proceeding

Two items from the backlog got real research this morning but stopped
short of a change, per standing doctrine (uncertain → write up, don't
guess):

1. **Door1 expiry fix** — traced the actual spec (there wasn't one written
   down beyond a single prescriptive sentence in a 2026-08-30 case memo),
   and found the file that sentence would naturally point at
   (`OLLIETRADES_KILL_GATE.md`) is explicitly locked against edits by its
   own text. Full writeup + options:
   `data/reports/relay/QUESTION_door1-expiry-fix-scope.md`.
2. **signals.db retention** — measured the real numbers (table sizes,
   `trader.db` total) and they don't match the "2.19 GB" figure given this
   morning. Flagging the discrepancy before drafting a policy so the
   proposal isn't built on a wrong number. Full writeup:
   `data/reports/relay/QUESTION_signals-db-retention-numbers.md`.

## Rest of the after-close list
Unchanged from last night's order: Phase 1.1 acceptance read → Polygon
limiter cap raise → Polygon key rotation → bk_orb re-scope → Bridge
cosmetics, then the new offhost-backup investigation above.

---

## Update — same morning, three follow-up answers

### gex_collector — closed, no further action
Confirmed correct.

### signals.db — resolved, was `signal-center/signals.db`, not `trader.db`
Admiral's "2.19 GB" was right — just a different DB than first checked.
`/Users/bigmac/autonomous-trader/signal-center/signals.db` = 2,208,968,704
bytes, the standalone DB behind the :9000 Signal Center. 99.6% of it is
one table, `signal_history` (207,178 rows, 2.04 GB, `raw_data` JSON blobs
averaging 9.7 KB). Found something worth flagging on its own: a 30-day
rolling archive policy was already designed and half-built on 2026-04-26
(`signal-center/signals_archive.db` + `archive_metadata` table, documented
in `docs/SUNDAY_DRYDOCK_2026-04-26_FINAL.md` item E1) — the archive DB and
schema exist, but the actual mover job was never written, so it's archived
zero rows in four-plus months while the live table grew unbounded.
Proposal to finish it (script + cron, one-time backfill ~2.19GB → ~350-
400MB, steady-state after) written up in
`data/reports/relay/QUESTION_signals-db-retention-numbers.md`, along with
a plausible link to today's offhost-backup slowdown item (same DB is the
single largest thing that script rsyncs). Not built — awaiting go-ahead.

### Door1 expiry fix — closed as unactionable
Agreed: one sentence in a memo isn't a spec, and `OLLIETRADES_KILL_GATE.md`
is append-only by design. Closed in `docs/XO_BACKLOG.md`'s consolidated
table with the reasoning recorded; comes back only as a fresh proposal
with a real spec if it matters later.

Nothing else queued before the open. After-close list unchanged from this
morning's ordering, plus signals.db retention now sitting in that queue
pending approval.
