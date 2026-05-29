If any return errors → stop and triage before validation starts.

## Validation timeline

### 05:30-06:30 MST — Premarket warmup
- Open bridge.ollietrades.com (hard-refresh Cmd+Shift+R)
- Watch the 📡 LIVE EVENT TAPE card
- Expected: events should start trickling in as premarket trades fire
- Watch for: new_session_high, new_session_low, crossed_above_close,
  crossed_below_close, volume_burst on premarket movers

### 06:30 MST — Market open
- This is the firehose moment
- Tape should flood with events in the first 5 minutes
- Acceptance criteria:
  - At least 10 events in the live tape in the first 10 min of open
  - Click-through on a Tier 1 ticker event → drawer opens correctly
  - Audio ding fires when event matches T1 scanner ticker (if 🔔 T1 checked)
  - No UI flash on 30s refresh
  - No console errors in DevTools

### 06:30-07:00 MST — Position-risk check
- Check overnight gaps on holdings (especially DKS, COST after earnings)
- Riker XO flagged "COST earnings TOMORROW" yesterday — that hits today after close
- DKS earnings hit AFTER market close tonight — not a premarket concern
- COST: still holding. After-close earnings call risk. Decision needed BEFORE close.

### 09:12 MST (12:12 ET) — Batch alert window
- Volume_alerts table should refresh around this time (per yesterday's cadence)
- 📋 BATCH SCAN ALERTS card should populate
- Validates Phase 2 still works alongside Phase 2.5

## Failure modes to watch for

| Symptom | Likely cause | Recovery |
|---------|--------------|----------|
| No events at all by 07:00 | Tick recorder died overnight | Check `sqlite3 ... price_ticks`. If 0 rows, restart trader |
| Events firing but UI empty | API endpoint cache issue | curl `/api/scanner/events_realtime`. If JSON has events, hard-refresh browser |
| Same event firing repeatedly | Dedupe logic broken | Check event_tape for dupes within 60s same symbol/type |
| UI flash on every refresh | Frontend re-renders not incremental | Console error inspection |
| All Tier 1/2/3 scanners empty | strategy_signals stale | Confirm strategy_signals refreshing — `SELECT MAX(created_at) FROM strategy_signals` |
| Cloudflared down | Tunnel dropped | Restart cloudflared nohup |

## Decisions to make today

### COST earnings risk (afternoon)
- Current position: holding COST
- Earnings: TODAY after close (per Riker XO and earnings panel)
- Pre-earnings IV is elevated; options are expensive both ways
- Three options:
  1. **HOLD through earnings** — accept overnight gap risk
  2. **SELL before close** — take any profit/loss, reset Friday
  3. **HEDGE with protective put** — buy 1 OTM put expiring tomorrow
- Recommendation: check unrealized P&L first. If green, consider taking profit.
  If meaningfully red, the position is already wrong-sized — fix it.

### DKS earnings (after close tonight)
- Less critical than COST since smaller position
- Same three options
- Likely just HOLD given smaller exposure

## Success criteria for Phase 2.5

If by 07:00 MST we see:
- ≥10 events in the live tape from real market action
- At least 1 event tied to a scanner Tier 1/2 ticker
- Click → drawer working
- No errors in console or trader_error.log

→ Phase 2.5 is fully validated. Ready to add more event detectors (C5+).

If we miss any of those → triage the failure mode above, fix, then revalidate
during the next live session.

## Power-paste prep
Separately documented in drafts/HM-CLOSET-POWER-PASTE.md — single command block
to clear backlog items 1-12 from the closet list in sequence.
