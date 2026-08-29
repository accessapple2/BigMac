# Question: reverse more of the 2026-07-22 fleet stand-down, or just Signal Center?

**Date:** 2026-08-28
**Context:** Restoring Signal Center (see companion relay doc, same batch)
required re-enabling a `launchctl disable` override. Checking the full
disabled list (`launchctl print-disabled gui/501`) turned up **~23 more
`com.ollietrades.*` / `com.trademinds.*` jobs disabled the same way, all
dated to the 2026-07-22 fleet stand-down** — this was clearly one
deliberate, comprehensive action, not scattered accidents. I only asked
about Signal Center; I have not touched anything else on this list.

**Already done:** `com.trademinds.signal-center` re-enabled + running, per
the directive. Nothing else changed.

## The rest of the disabled list

**Recommend leaving disabled regardless of the stand-down** (unrelated to
it): `com.ollietrades.riker-synthesis` — CLAUDE.md documents this as
permanently retired at the code level back on 2026-06-24 (`main.py`'s
scheduler for it was removed, not just paused). Re-enabling the launchd job
wouldn't do anything.

**Orphaned, not actionable:** `com.trademinds.crew` — disabled-override
exists but there's no plist file backing it anymore (`~/Library/LaunchAgents/
com.trademinds.crew.plist` doesn't exist). Inert either way.

**A separate, older job — not part of the Kirk briefing pipeline:**
`com.trademinds.premarket` (`premarket-scan.sh`, 4:00 AM AZ weekdays,
last ran 2026-07-22 04:00 — same day as the stand-down). This is NOT the
same thing as `kirk_briefing.py --mode premarket` (which never stopped
running, per the companion relay doc) — it's an older, independent scanner.

**The rest — genuine fleet automation, needs your call:**
`ti-email-poller`, `enrichment-poller`, `model-watcher`, `nightly-backtest`,
`hm-wr-dur-monday-check`, `daily-watch`, `ti-picks-watcher`,
`hm-signals-v2-monday-check` (+ `-verify`), `morning-cd-instr`,
`danelfin-update`, `universe-refresh`, `nightly-regression`,
`stale-trim-obs`, `iv-backfill`, `crusher`, `archer-briefing`, `scotty`,
`morning-an2-observation`, `finetune-reminder`, `uhura-watch`.

(Minor forensic footnote, not actionable: `launchctl print-disabled` also
shows one corrupted-looking entry — that same list of ~23 labels
concatenated into a single literal string, mapped to `enabled`. Looks like
a bug in whatever bulk-disable process ran on 07-22, not a real service.
Harmless; flagging in case it's a clue about what tool was used.)

## Options

- **Leave everything else disabled (Recommended)** — matches the literal
  scope of what was asked (Signal Center + the Kirk briefings, which turned
  out not to need re-enabling at all). The stand-down was clearly deliberate
  and comprehensive; reversing 20+ jobs without knowing why they were stood
  down together risks undoing something intentional.
- **Review job-by-job with me first** — I list what each does and its last
  activity before touching anything, you decide per-job.
- **Reverse the stand-down wholesale now** — re-enable + restart everything
  on the list except `riker-synthesis` (retired) and `crew` (orphaned).
