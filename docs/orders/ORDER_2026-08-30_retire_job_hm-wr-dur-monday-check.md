# TOMBSTONE — hm-wr-dur-monday-check (job)

**Retired:** 2026-08-30
**Reason:** one-shot StartCalendarInterval hardcoded to 2026-07-20 09:00 (RunAtLoad=false) -- confirmed via plist read, not a recurring schedule. Never fires again regardless of enabled state. Same pattern as hm-signals-v2-monday-check/-verify (retired 2026-08-30 earlier tonight, commit 8bed0fc) -- missed in that pass, caught now while diagnosing why hm_ops_sentinel's launchd staleness check keeps flagging it. Revived 2026-08-29 22:02:55 alongside the other launchd jobs without this being checked; reversing that revive.

This is permanent under current criteria. No resume-by date — revival requires a new explicit `revive` order, not a calendar trigger.
