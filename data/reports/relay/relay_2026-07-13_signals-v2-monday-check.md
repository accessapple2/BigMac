# Relay: HM-SIGNALS-V2-STARVATION-RECURRENCE Monday check

**Date:** 2026-07-13 (automated one-shot cron, HM-SIGNALS-V2-STARVATION-RECURRENCE)
**Baseline:** hm_signals_v2_monday_check_baseline_20260712.json -- 140 rows, ids 67350-67489,
83 ollama-plutus + 57 ollama-qwen3, dated 2026-07-10 20:01:52 - 2026-07-11 02:59:23.

## What was asked

Queued from `docs/XO_BACKLOG.md` (`HM-SIGNALS-V2-STARVATION-RECURRENCE`, filed
2026-07-12): after Monday's open, are the 140 weekend-idle rows draining, or
being permanently outranked by newest-first ordering the same way two prior
backlogs required one-time archive cleanups?

## Result

```
baseline rows still pending:     140 / 140
baseline rows transitioned:      0 / 140
by transitioned status:          {'pending': 140}
current total pending (all):     435
current oldest-pending age:      66.0h wall-clock, 0.5 market-hours
newer (id>67489) rows already terminal: 10
```

**Verdict:** CONFIRMED OUTRANKED. 140/140 baseline rows are still pending while 10 newer row(s) (id > 67489) already reached a terminal status ahead of them -- the newest-first + drain-cap mechanism is recurring exactly as the ticket predicted. Recommend HM-SIGNALS-V2-STARVATION-RECURRENCE becomes active work (needs Admiral sign-off on candidate fix (a) TTL vs (b) hybrid ordering).

## Open items

Ticket status left as-is in `docs/XO_BACKLOG.md` (still 🔵, not auto-closed
or auto-escalated) -- this report records the verified numbers; closing or
escalating the ticket needs Admiral sign-off per its own text.
