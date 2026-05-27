# HM-EVENT-TAPE-DYNAMIC-SUBSCRIPTION

## Issue
Tick recorder subscribes to a static 28-symbol IEX list. When scanner surfaces
new Tier 1/2 convergence candidates (e.g., CRSR and MUU at 12:13 PM 2026-05-27),
the event detector cannot see their tick movement and never fires events on
them, even though they are the most actionable candidates.

## Repro 2026-05-27 12:13 PM AZ
- Tier 2 scanner: CRSR (Vol 5 Min 379%), MUU (239%)
- Event tape: 0 events
- price_ticks has zero rows for CRSR or MUU (not subscribed)

## Fix paths

Path 1 (recommended): dynamic subscription
- Every 30 sec, fetch current scanner Tier 1/2/3 ticker list
- Diff against current IEX subscriptions
- Call ws.send({action:subscribe, params:T.NEW1,T.NEW2,...}) for adds
- Call ws.send({action:unsubscribe, params:T.OLD1,...}) for drops
- Cap total subscribed at 200 to respect IEX free-tier limits

Path 2: bigger static list
- Subscribe to top 500 most-active tickers via daily refresh
- Simpler but less adaptive

## Priority
Medium. Phase 2.5 works for in-fleet event detection but misses the most
actionable signals (fresh convergence). Add to power paste backlog.

---

## Diagnostic confirmation 2026-05-27 12:50 PM AZ (15 min before close)

Detector logic VERIFIED CORRECT against engine/event_tape.py:
- _detect_running_fast: 0.5% move in 60-second window — CORRECT, narrow by design
- _detect_session_high: requires last_px > prior_high × 1.001 — CORRECT, fires once per high
- _detect_volume_burst: 60s vol >= 3x rolling 20-min baseline — CORRECT

Hourly tick range of subscribed mega-caps today:
- META 2.1%, IREN 2.45%, LII 1.05%, ON 0.99%, AVGO 0.87%
- Most others < 0.7%

Spread over 60 min, 2% across hour = ~0.03%/min. To trip running_up_fast,
need sustained 0.5% in single 60-sec window — requires a real news pop or
algo flush. Did not happen on subscribed tickers today.

CRSR (scanner Tier 2, 379% vol burst) and MUU (239%) WOULD HAVE fired
running_up_fast and volume_burst — but are not in the IEX subscription.

## Conclusion

Phase 2.5 is correctly built. The issue is structural:
- Detector calibrated for actionable events (right)
- Subscription doesn't include the volatile tickers that produce events (wrong)

Dynamic subscription is the correct fix. Do NOT tune thresholds down to
create false noise on subscribed tickers — that masks the real problem
and reduces the value of the signal when it does fire.

Priority bump: MEDIUM -> HIGH. This is the gating issue for Phase 2.5
proving its worth. Without dynamic subscription, the tape will continue
to show ~1 event per session even during active markets.

## Diagnostic 2026-05-27 12:50 PM AZ
Detector logic verified correct: 0.5% in 60s, session-high +0.1% buffer, 3x vol burst.
Subscribed mega-caps moved 0.17-2.45% hourly = 0.03%/min sustained. Threshold needs a 60s burst that didn't happen today.
CRSR (Tier 2, 379% vol) and MUU (239%) would have fired — not subscribed.
Conclusion: detectors correct, subscription wrong. Dynamic sub is the fix. Priority MEDIUM -> HIGH.
