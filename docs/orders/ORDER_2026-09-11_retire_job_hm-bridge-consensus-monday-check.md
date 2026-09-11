# TOMBSTONE — hm-bridge-consensus-monday-check (job)

**Retired:** 2026-09-11
**Reason:** One-shot StartCalendarInterval hardcoded to 2026-07-20 07:00 MST (RunAtLoad=false) -- already fired once successfully that date per docs/XO_BACKLOG.md, can never fire again regardless of loaded state. Confirmed the only remaining unledgered monday-check monitor (hm-signals-v2-monday-check/-verify/hm-wr-dur-monday-check all already retired 2026-08-30, ledger rows 112/113/115). XO directive P4 item 11, 2026-09-11.

This is permanent under current criteria. No resume-by date — revival requires a new explicit `revive` order, not a calendar trigger.
