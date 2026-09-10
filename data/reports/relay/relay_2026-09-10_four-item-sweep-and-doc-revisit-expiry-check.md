# Relay — ollie-machine/monday-check/situation_report research + ollama_prewarm retirement + doc-revisit expiry check, 2026-09-10

Four items from the Captain, read-only research on the first three,
action on the fourth, plus a systemic fix for the pattern that let two of
them go stale unnoticed.

## Item 1 — ollie-machine HALT (research only, no action taken)

`ollie-machine` is `halt_mode='full'` since 2026-08-30 07:11:29
(`ai_players`, ledger id 114). Cause: the pre-committed
`HM-OLLIE-MACHINE-KILLGATE` (filed 07-05) — zero trades in `trades` AND
`options_trades` by 2026-07-24 triggers a halt proposal. The agent (created
06-01) had recorded zero trades the entire time; the check itself never
fired on 07-24 (disabled two days early by the 07-22 trip-quietdown) and
was rendered retroactively on 08-30 against the original window's data —
still zero trades, so the delay changed nothing.

**The 9/29 date is not a re-test of the trade-count condition** — that
gate already fired and is consumed. `review_by=2026-09-29` is the generic
fleet-lifecycle tripwire (`docs/FLEET_LIFECYCLE.md`): a sentinel finding
fires if nobody has looked at this halt again by then. Reversal path is
`scripts/fleet_lifecycle.py revive ollie-machine --reason "..."` — no
other documented option (partial revive, forced retire) exists for this
target specifically. Separately: the same-night fleet-wide Door-1 KEEP
consequence (would have halted 6 *other* agents) was waived by the
Admiral — a different gate, not conflated with this one.

## Item 2 — the "stuck" monday-check jobs (research only, no action taken)

Four monday-check-family jobs exist. Three are cleanly retired and
ledgered with no live drift: `hm-signals-v2-monday-check`,
`hm-signals-v2-monday-check-verify`, `hm-wr-dur-monday-check` (ledger ids
112/113/115). **The stuck one is `hm-bridge-consensus-monday-check`** — a
one-shot system LaunchDaemon (`StartCalendarInterval` hardcoded to
2026-07-20 07:00) that watches `bridge_consensus` source freshness. It
fired exactly once, successfully (`state=GREEN`, no alert needed,
2026-07-20), and will never fire again — its trigger date is 7 weeks in
the past — but it was never retired or ledgered, and isn't in
`hm_ops_sentinel`'s tracked launchd set, so nothing will ever flag it
stale. Only one stuck job found, not two — noted the discrepancy rather
than forcing a second candidate; most likely source of "two" is the
2026-09-02→09-04 transient drift incident that briefly re-enabled the
three already-retired jobs (fully reconciled since, per
`relay_2026-09-04_four-decisions.md`).

## Item 3 — situation_report (research only, no action taken)

`scripts/situation_report.py` (dark since 07-22,
`.quietdown-disabled-2026-07-22`) produced a twice-daily fleet snapshot
(process/log/signal-count/portfolio/regime) as a full `.txt` report plus a
3-line NTFY push to `ollietrades-admin` — human consumers only, nothing
downstream reads its output files. Last real run: 2026-07-22 10:00. Its
2026-08-30 revisit condition ("~2026-09-06, against `kirt_briefing.py`'s
live coverage, before deciding if it's redundant") passed with **no
revisit ever recorded** — the redundancy question is still open. This is
now caught going forward (see the systemic fix below) rather than staying
silently missed.

## Item 4 — ollama_prewarm: confirmed clean, retired

Confirmed nothing depends on it before touching anything: crontab line
already commented, no launchd plist ever existed for it (pure cron job),
no other script reads its log files, its only external effect (an
on-failure-only NTFY) has no downstream consumer keyed off it.

**Retired.** `OLLAMA_KEEP_ALIVE=-1` shipped live with the olliemax
hardware migration — the idle-unload cold-start pattern this script
existed to pre-empt (~06:51 AZ bridge-wedge) structurally can't recur.
Independently confirmed stale regardless: the script hardcodes
`192.168.1.168`, the pre-migration Ollie Max address — live inference now
routes through `OLLAMA_URL=http://100.95.195.20:11434`. Reviving it
unmodified would prewarm a dead address.

**Real gap found while retiring it:** `scripts/fleet_lifecycle.py`'s
`_apply_job_change()` only handles launchd targets — it has no cron code
path at all. Running `retire --type job ollama-prewarm` for real raises
`RuntimeError: no plist found` and (correctly, per its own "refuses
partial work" design) writes nothing. `status`/`list` can't resolve a
cron-only target either. Backfilled by hand instead — ledger row 119
(`fleet_lifecycle_ledger`, `backfilled=1`, same shape as the existing
`job|crew|retire` orphan row 26), tombstone at
`docs/orders/ORDER_2026-09-10_retire_job_ollama-prewarm.md`. Flagging
this as a real doctrine gap, not routing around it silently: ~40
cron-based scripts in this repo have no first-class lifecycle path today,
only launchd jobs do.

## Item 5 (systemic) — doc-prose revisit tags now have an expiry check

The actual bug: `check_fleet_lifecycle_drift`'s `resume_by`/`review_by`
overdue check only reads `fleet_lifecycle_ledger` rows — and
`situation_report.py`/`ollama_prewarm.sh`'s revisit notes were
specifically the ones the Admiral chose to leave **out** of the ledger
("STAYS DARK, no ledger action" in `docs/XO_BACKLOG.md`). So the exact
dead-man's-switch class this repo already has a fix for on the ledger
side had a second, un-fixed instance on the doc-prose side.

**Fix shipped:** `scripts/hm_ops_sentinel.py::check_doc_revisit_dates()`
(new, wired into `main()`'s existing 5-10 min cron cycle — not a separate
manually-run script, so it can't itself become another forgotten
tripwire). Scans `DOC_REVISIT_PATHS` (today: `docs/XO_BACKLOG.md`,
extensible) for a `REVISIT-BY: YYYY-MM-DD` tag and fires
`sentinel_doc_revisit_overdue` once the date passes.

- Retrofitted `docs/XO_BACKLOG.md`'s `situation_report.py` note with the
  tag (kept the real 2026-09-06 date rather than resetting it — it's
  genuinely overdue and should alert). **Live-verified**: `--dry-run`
  fires `[warning/sentinel_doc_revisit_overdue] ... docs/XO_BACKLOG.md:10745
  (REVISIT-BY 2026-09-06)`, exactly once (first pass double-counted
  because I'd also echoed the tag into the summary table row — fixed by
  keeping the machine-parseable tag in one canonical place only, doctrine
  note added in `docs/FLEET_LIFECYCLE.md` so a future entry doesn't repeat
  that).
- `ollama_prewarm.sh`'s note updated to RESOLVED (retired, this session)
  rather than tagged — it no longer needs watching.
- `docs/FLEET_LIFECYCLE.md` updated with a "Doc-prose revisit tags"
  subsection under Drift enforcement, matching the existing
  ledger-`review_by` doctrine language.
- `py_compile` clean on `hm_ops_sentinel.py` and `fleet_lifecycle.py`.

## NOT done — pending Admiral decisions

- `ollie-machine` — untouched, awaiting 9/29 or an earlier explicit call.
- `hm-bridge-consensus-monday-check` — identified as the stuck job, not
  retired; needs a decision (default-retire was the Captain's stated
  prior, not yet executed).
- `situation_report.py` — still dark, redundancy-vs-`kirk_briefing.py`
  question still genuinely open; will now surface as a live sentinel
  alert instead of silently persisting, but no revive/retire call made
  here.
