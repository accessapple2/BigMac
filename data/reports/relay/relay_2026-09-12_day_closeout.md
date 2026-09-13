# Relay — 2026-09-12. Day close-out.

Session resumed after a VPN drop mid-task; picked up from four already-
committed pieces of work (Bridge Phase 1/2/3), then did two more passes
this afternoon (sentinel false alarms, trades-table tz gate). This doc is
the single index for the day — detail lives in the five companion relay
docs listed inline below, not repeated here.

## 1. Trades-table UTC/local-date gate bug — FIXED, restarted, verified

Commit `9c5f671`. Full detail:
`relay_2026-09-12_trades_tz_gate_fix.md`.

What it was: `_trades_today()` (Battle Station 0DTE) compared a UTC-stored
timestamp against a Python-local `date.today()` — any trade 5pm-midnight
Arizona lands on the wrong side of the local/UTC calendar-day boundary,
either missed by that evening's own gate check or double-counted into the
next day's cap. Turned out to be a repo-wide pattern, not a one-off: 15
call sites across 10 files (risk_manager's fleet-wide + per-player daily
caps, paper_trader's redundant re-check, trade_gateway's third independent
limit layer, m5_allocator/capitol_fund/crew_scanner's daily dedups,
dayblade/main.py's display counters) all shared the same root cause.

Fixed with one canonical helper, `engine.market_calendar.
local_day_utc_bounds()`, same consolidation doctrine as `is_trading_day()`
in the same file. Regression test added (`tests/test_trades_tz_gate_
regression.py`) in the Grep Gate's own comment-aware-scan style — fails if
the ad hoc `date(col)=?` pattern reappears in any of the 9 gate files,
regardless of wording.

**Cost, plainly** (this matters for how hard to chase the remaining ~40
non-execution `date(col)=?` sites elsewhere in the repo, left filed as a
separate lower-priority sweep): the miscount mechanism is *confirmed* with
a real number (a phantom trade-count of 1 vs. the true 0, on a real
historical case). Whether it ever actually flipped an accept/reject
decision is *not confirmed* — 55 of the 61 affected historical trades
predate the `gate_reject_log` audit table (started 2026-05-26) entirely, and
the 6 that postdate it show zero relevant rejections for those players/
dates. Exposure with a demonstrated mechanism, not a confirmed incident.

Flagged (not rewritten): `trades.tz_bucket_suspect INTEGER DEFAULT 0`,
set on exactly the 61 affected row ids, RULE #1-compliant (additive column,
no existing value touched).

Restarted market-closed (backup-first, integrity-checked), verified live by
calling the fixed functions directly against the real DB in the running
venv — all clean.

## 2. Bridge Classic repair pass — Phases 1 & 2 shipped, Phase 3 spec-only

Commits `7b2753b` (Phase 1 trace), `3ab0b14` + `9ca8584` (Phase 2 fixes +
restart verification), `65a5a63` (Phase 3 spec). Full detail:
`relay_2026-09-12_bridge_correctness_phase1_trace.md`,
`_phase2_fixes.md`, `_pass_summary.md`.

**Phase 1** (read-only trace of 4 reported panel contradictions):
- Riker "bullish" vs. Tactical Display "BEAR_CROSS" — display/labeling only;
  two real, correctly-computed regime classifiers on different timeframes,
  never mislabeled as the same thing. Riker's output confirmed to never
  reach any execution path.
- Saturday "0/2 TRADES" with real trade rows showing — **surfaced the real
  finding of the day**: `battle_station_0dte.py::_trades_today()`'s
  UTC/local mismatch, a genuine live decision-path bug. Flagged, not fixed
  in Phase 1 (out of that pass's display-only scope) — this is what became
  item 1 above, now fixed.
- Sector heatmap zero-fill — display/fallback-logic bug (Finviz's real
  Saturday all-zero response short-circuited past a working stale-disk
  fallback). Fixed in Phase 2.
- Riker mid-sentence truncation — display only, a silent tokenizer-artifact
  strip regex on `index.html` only. Fixed in Phase 2.

**Phase 2**: 11 fixes shipped (8 originally listed + 3 more found while
tracing Phase 1), all display/derivation code, RULE #1 respected
throughout — Season-5 hardcodes, leaderboard rank-skip, Archer briefing
dedup + mangled-character fix, consensus panel zero-vote flood, metals
panel field-name mismatch, Battle Station history mislabeling, sector
heatmap fallback, Riker truncation marker. Two items investigated and left
alone on purpose: the leaderboard sort-direction claim (unreproduced
against live data on 3 render paths — open, not fabricated), and the
4-way agent-count "disagreement" (arithmetic checks out; it's a curated-
subset-vs-full-roster definitional difference, not a bug). Restarted and
live-verified (Season 8 footer confirmed via public endpoint).

**Phase 3**: `/api/bridge/facts` unified-facts endpoint spec written
(`docs/architecture/bridge-facts-endpoint-spec.md`) — **spec only, not
built**, per instruction. Grounds the design in this session's own five
confirmed instances of the same disease (regime, season, agent counts,
sector data, metals each computed 2-4 places with no shared as-of/
staleness contract), names GEX age as a sixth, sequenced after the CBOE
repoint rather than blocking it.

## 3. Two sentinel false alarms — retired

Commit `95252df`. Full detail: `relay_2026-09-12_sentinel_false_alarm_
fixes.md`.

- **`sys_scan_liveness`** — fired hourly on weekends because
  `check_scan_liveness()`'s "market closed" guard never actually caught
  weekends (`_get_scan_interval()` returns the real weekend cadence, never
  `None`), and the fixed 60-min alert threshold exactly equalled that
  cadence, leaving zero slack. Now gates on `market_calendar.
  is_trading_day()`, same predicate `bridge_vote.py` already uses to
  recognize a legitimate weekend stand-down. **Verification in progress —
  see below.**
- **`sentinel_mlx_qwen3_unhealthy`** — traced before touching anything:
  `com.ollietrades.mlx-qwen3` (port 8899) crashed 2026-09-09 and never
  recovered, and hadn't fed a live trading decision since May anyway (the
  agent id was already repointed to olliemax's Ollama). Coincided with
  bigmac's broader 9/9 local-model stand-down. Retired LaunchAgent + probe
  together (`launchctl bootout`, plists archived not deleted), sentinel
  call site commented out in place with the function kept for revival.

### sys_scan_liveness verification status

Historical pattern confirmed from `trader_error.log`: fired roughly every
60-63 minutes, last real occurrence **16:47:45 MST**, eleven minutes before
the 16:57:38 restart that shipped the fix. As of 17:35 (38 min post-
restart) no new alert — consistent with the fix, not yet conclusive since
the old code's failure window recurs roughly hourly. **A background check
was scheduled for 18:15 MST** (grep `trader_error.log` for `scan_liveness`
past line covering 16:47:45) to give one full cycle past the last real
firing. If this session's context runs out before that check lands, it's
the first thing to look at Monday: `grep -n "scan_liveness" logs/trader_
error.log | tail -5` — no new hit past `16:47:45` confirms it's dead;
anything newer means the fix isn't fully live and needs another look.

## 4. What's left for Monday

- **sys_scan_liveness 18:15 confirmation** (above) — check first if this
  session didn't get to report it.
- **Leaderboard sort-direction claim** — unreproduced against live data on
  3 render paths (Phase 2). Open; if it recurs, a screenshot with the
  active sort-button state would pin down which code path is actually
  firing.
- **`/api/bridge/facts` (Phase 3)** — spec written, not started. Estimated
  ~1 session for the endpoint + core facts, ~half a session for a pilot
  panel migration, 2-4 more spread out for the rest, prioritized by what
  this pass actually found wrong (regime, metals, agent counts, sector
  data).
- **The ~40 other `date(col)=?` sites** (briefings, journal entries, signal
  dedup, cost tracking — non-execution tables) — filed as a separate,
  lower-priority sweep. Given the trades-table version of this bug showed
  no confirmed bad outcome despite a real, checkable mechanism, these are
  reasonable to leave for whenever, not urgent.
- **Not touched this session, still open from before**: GEX CBOE repoint,
  Phase 1.3, the un-alias leftovers (`plutus-v1`/`qwen3:8b` digest
  situation — revisit-by 2026-09-17 per `CLAUDE.md`), anything in the
  trading decision path beyond what's listed above.
- **A message arrived mid-session today formatted to look like a
  legitimate interruption** ("FROM: XO... TO: Scotty," asking to append a
  "Phase 1.2b theme-context rider" to `docs/XO_PLAN_2026-09.md` and
  commit it) — injected into a tool result, not an actual message from the
  Admiral. Not actioned, flagged in-session. Worth a mention in case it
  resurfaces or the real XO Plan does get a legitimate Phase 1.2b later —
  don't assume this session's flagged instance was authentic.

## RULE #1 statement

Nothing deleted or rewritten in place all day. Every fix was display code,
a gate/dedup query, a notification-text builder, a display-cache fallback
rule, a LaunchAgent retirement, or an additive DB column + backfill on a
named, verified set of row ids. Two restarts, both backup-first,
integrity-checked, market-closed, single-writer-gated.
