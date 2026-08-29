# Options-Trade Gap Investigation — 2026-08-29

**Trigger:** OPS TRIAGE flagged "no active-fleet agent has closed an options
trade since 2026-07-04." This report investigates each options pathway,
answers the graduation-timeline question, and assesses impact on OllieTrades
Signal. **Analysis only — no live changes made**, per directive.

## Headline finding: the options gap is a symptom, not six separate diseases

Before triaging pathway-by-pathway, one fact reframes almost everything
below: **`logs/trader.log` has zero lines between 2026-07-22 and
2026-08-24 — a 33-day gap — and the `trades` table confirms zero fleet-wide
trade executions across the *entire active fleet* for that same window**
(not one options or stock trade, from any of ~78 active agents). Verified
two ways (line-count-per-day on the log, `COUNT(*)` on `trades.executed_at`
by day) so this isn't a log-rotation artifact.

| Day | trader.log lines | Fleet-wide trades |
|---|---:|---:|
| 2026-07-22 | 18,128 | 5 |
| 2026-07-23 … 2026-08-23 | **0 every day** | **0 every day** |
| 2026-08-24 | 3,081 (partial) | 0 |
| 2026-08-25 | 16,603 | 10 |

The fleet has not been stably up since recovering, either: `trader_reboot_start.log`
shows restart events at 2026-08-27 10:42 and 2026-08-28 20:08, and the
currently-running process (checked live, `/api/health`) has an uptime of
only **79 minutes** as of this writing — another restart happened today
that isn't yet in that log. Trade volume since "recovery" is thin (8-13
trades/day fleet-wide, some days zero) — consistent with a process that
keeps going down, not a fully healthy fleet.

**This is bigger than the options desk.** It explains why Troi's wheel,
the CSP shadows, and (per OPS TRIAGE item 2) the GEX collector and
source-health watcher all went quiet in the same window — they're
downstream of one upstream failure, not six independent ones. **This
deserves its own dedicated investigation** (uptime/crash root-cause,
same treatment this session gave the GEX collector) — flagged here as
priority-0 context, not solved in this pass; solving it is a prerequisite
for any of the pathway-specific proposals below actually mattering.

## Per-pathway classification

| Pathway | player_id | halt_mode | Classification | Evidence |
|---|---|---|---|---|
| Troi wheel | `options-sosnoff` | active | **(b)+(c) hybrid** | Entry gate (VIX<18) legitimately blocks most attempts, *and* the process-outage above silenced it for 33 days |
| McCoy BPS | `mccoy-bps` | full (retired 2026-05-05) | **(a) intentional, gate unmeetable** | Zero signals/trades ever; audition frozen |
| Anderson BCS | `anderson-bcs` | full (retired 2026-05-05) | **(a) intentional, gate unmeetable** | Zero signals/trades ever; audition frozen |
| Quark IC | `quark-ic` | full (roster-cap 2026-07-05) | **(a) intentional, gate unmeetable** | Zero signals/trades ever; audition frozen |
| CSP shadow: qwen3.5 | `shadow-qwen35-csp` | n/a (ghost book) | **(b)+(c) hybrid** | 3/30 era-closes, stalled 40+ days |
| CSP shadow: plutus | `shadow-plutus-csp` | n/a (ghost book) | **unclear — flag separately** | Zero rows ever, no evidence it has ever run |
| 0DTE dayblade | `dayblade-0dte` (T'Pol) | full (2026-07-13, repair) | **(b) stalled/broken** | Known bug, halt explicit, fix not yet applied |

### Troi wheel (options-sosnoff) — (b)+(c)

`engine/wheel_strategy.py` writes one row to `csp_wheel_scan_log` per
completed scan attempt (throttled to once/trading-day via a module-level
`_done_today` flag — the 15-min schedule interval in `main.py` is
misleading; it only *executes* once a day). Of the **39 logged scan
attempts since 2026-07-06**, **38 were blocked at `MIN_VIX = 18`**
(`vix_skip`, actual VIX ranged 14.5–17.9 — the market's been in a
persistently low-vol regime). Exactly **one** scan (2026-07-20) got past
the VIX gate, evaluated 6 tickers, and opened 0 positions (didn't clear
`MIN_PREMIUM_RETURN = 3.0%` either — thin premiums are exactly what low
VIX predicts). Zero scan-log rows exist for the 33-day outage window
(matches the fleet-wide silence above, not a separate bug in this file).

Troi currently has **zero clean season-7 stock trades** → `calculate_rating()`
returns `N/A` for her → she cannot be a "winning model" for OllieTrades
Signal regardless of her options activity (see Signal section below).

### McCoy BPS / Anderson BCS — (a), permanently closed loop

Both retired 2026-05-05 ("HM-T-fleet bundle Option 1 halt-only; code
preserved"). Checked every activity table (`trades`, `options_trades`,
`signals`) — **zero rows, ever, for either player_id, at any point in the
system's history.** Not "was active then retired" — never traded even
before retirement.

`weekly_tuning_crew.py::_run_auditions()` has proposed the identical
verdict — `insufficient_data`, `clean_signals_in_db: 0` — three weeks
running (2026-07-05, -12, -19) for both, then the crew itself went quiet
(no `audition_proposed` rows since 07-19, consistent with the fleet
outage; not separately investigated here). The module's own docstring is
explicit about why this can never change: *"a `halt_mode='full'`
candidate's clean-window numbers are frozen at whatever it produced
before being cut — verdict stays whatever it was."* **This is a closed
loop by construction, not a slow-moving one.** No amount of calendar time
will move either of these off `insufficient_data`.

### Quark IC — (a), permanently closed loop, same mechanism

Identical situation to McCoy BPS/Anderson BCS: halted 2026-07-05
(`HM-ROSTER-CAP`, not in the "measured/auditioning 8," requires a passing
audition to reactivate), **zero signals/trades ever**, frozen
`insufficient_data` verdict for the same three weeks, same "can't accrue
new data while halt_mode='full'" structural reason.

**Correction to the directive's premise:** the audition bar Quark is
actually held to is `AUDITION_CRITERIA["min_guarded_trades"] = 20`
(`config.py:386`), not 50 — I found no "50" threshold anywhere in the
codebase for Quark specifically (checked `AUDITION_CRITERIA`,
`TARGET_GUARDED_TRADES` in `audition_tracking.py`, and grepped for
"quark" across all `.py`/`.md` files — this pathway has no bespoke
scorecard module the way the CSP shadows do). Flagging rather than
silently substituting a number — if 50 came from an earlier
conversation not in my context, let me know and I'll re-derive against
that instead. Either way, the answer doesn't change: **0 progress at 0
signals means the required-N number is moot.**

### CSP shadows — shadow-qwen35-csp (b)+(c), shadow-plutus-csp unclear

`engine/shadow_csp_scorecard.py::GRADUATE_N = 30` confirms the number in
the directive exactly. Real-quotes-era-gated (`exit_date >=
TROI_REAL_QUOTES_ERA_START`, i.e. 2026-07-07) closed CSP count:

- `shadow-qwen35-csp`: **3 of 30** (closes on 07-08 ×2, 07-20). Last
  *any* activity (open or close) 2026-07-20 — 40 days ago. Zero new
  activity through the outage and after.
- `shadow-plutus-csp`: **0 rows in `options_trades` at all, ever.** Not
  "stalled" in the sense of having started and stopped — no evidence
  this seat has ever executed a single shadow trade. Worth a direct check
  of whether its shadow/sim loop is even wired into the scheduler (out of
  scope for this pass; flagging so it doesn't get silently assumed
  equivalent to its sibling).

### 0DTE dayblade (dayblade-0dte / T'Pol) — (b), same treatment as GEX collector

This one doesn't need re-diagnosis — it's already correctly triaged and
sitting in a known state: `halt_mode='full'` since 2026-07-13, halt_reason
verbatim: *"Paused for repair: entry pricing marks options ~3x rich
causing systematic -71% stops; reasoning/action inversion (bearish
reasoning → BUY_CALL); fractional contract qtys. Revert:
halt_mode=active."* 317 option-typed trades in the `trades` table before
the halt (last 2026-07-13, the halt date itself) confirm it was genuinely
active and productive before the bug forced the stop. This is a real,
already-diagnosed, fix-pending bug — same category and same next step as
the GEX collector: apply the pricing/reasoning-inversion/fractional-qty
fix, then flip `halt_mode='active'` per the halt_reason's own instruction.
Not re-investigated further here since the root cause is already on
record; flagging it as the one pathway with a concrete, scoped fix
waiting rather than a policy question.

## The graduation-timeline question

**shadow-qwen35-csp → 30 closes:**
At the *literal* current rate (0 new era-closes in the 40 days since
2026-07-20, including the 33-day outage), the honest answer is **never**.
Using instead its best historical rate — 3 closes in the 13 days between
the era boundary (07-07) and its last close (07-20), ≈0.23 closes/day —
naive extrapolation gives ~117 more days, landing **~2026-12-24**. But
that rate was measured while VIX briefly sat near/above 18 in early July;
VIX has since settled into the 14.5–17.9 band that's blocked 38 of the
last 39 scan attempts. **Neither number should be trusted as a real
forecast** — the honest statement is "the graduation gate has no
activity floor, and under the current VIX regime plus the demonstrated
uptime instability, there is no credible date."

**Quark IC → its actual 20-trade bar (or a hypothetical 50):**
Unconditionally **never**, at any N, under current mechanics — 0 signals
in the system's entire history, and the audition mechanism structurally
cannot gain new data while `halt_mode='full'`. This isn't a rate problem;
it's a zero problem. The gate doesn't need a lower N, it needs a path to
generate ANY data at all.

**This confirms the directive's suspicion directly: the graduation gates
have no activity floor**, and for three of six pathways (McCoy BPS,
Anderson BCS, Quark IC) the honest answer is a hard "never," not a slow
"eventually."

## Impact on OllieTrades Signal

The live `PLAYBOOK_REGISTRY` (`engine/ollietrades_signal.py:28-50`) has
**four** playbooks, not three as referenced in the directive — worth
correcting since it changes the fraction:

| Playbook | option_types | Style |
|---|---|---|
| `bull_put_spread` | BUY_PUT, SHORT_PUT, CSP | options |
| `leveraged_put` | BUY_PUT | options |
| `bear_play` | None *or* BUY_PUT | mixed (matches either) |
| `ollie_live_swing` | None (requires scanner tier) | stock |

So: 2 of 4 are options-only, 1 is stock-only, 1 matches either. Directionally
the same concern as the directive raised, just not literally "2 of 3."

**Can the gate ever fire the options playbooks right now? No — and not
for the reason you'd expect.** Playbook matching (Step 3) reads
`option_type` off each approving model's row in the **`signals`** table
(directional opinion log), not off `options_trades` (actual executed
positions) — so in principle a model could vote `BUY_PUT` and trigger
`bull_put_spread` without ever having closed a real options trade. But
checking what's actually been emitted **since the fleet recovered
(2026-08-24 onward): zero `BUY_PUT`/`SHORT_PUT`/`CSP`-typed signal rows,
from any player.** In fact only **three** player_ids have emitted *any*
signal at all since recovery — `ollama-plutus` (907 rows), `ollama-qwen3`
(820), `capitol-trades` (4) — out of the ~78-agent active fleet, and none
of those three are options-typed. The last options-typed signal from
anyone, anywhere, was `ollama-plutus` on 2026-07-21, the day before the
outage began.

Separately, `get_winning_models()`'s Step 1 requires `rating` A/B +
`total_trades >= 20` via the same `calculate_rating()` from OPS TRIAGE
item 3 — and Troi (the one agent whose whole job is options) is currently
`N/A` (0 season-7 stock trades), so she can't be a winning model even if
she were emitting options-typed signals.

**Bottom line: `bull_put_spread` and `leveraged_put` are currently
structurally unreachable — not from a strategy-design flaw, but because
(a) almost nothing in the fleet is emitting signals of any kind
post-recovery, and (b) what little is emitting is 100% stock-directional.**
Right now, the gate can only ever resolve to `ollie_live_swing` or the
stock-matching half of `bear_play`. This is a direct, measurable
consequence of the same fleet-wide outage above, not a Signal-gate bug —
but it means Phase 1's "ghost-logs across the whole roster" premise is
not currently true for 2 of its 4 playbooks, and won't be until either
the fleet's signal generation broadly recovers or Troi specifically
starts voting again.

## Recommendations (proposed — no changes made)

1. **Priority 0, not scoped to options:** investigate and fix the
   recurring trader-process instability (33-day dark period, 2+ restarts
   in the last 3 days, 79-minute current uptime). This is the actual
   root cause behind most of what's below; every other recommendation is
   secondary until the process reliably stays up. Recommend the same
   treatment this session gave the GEX collector — dedicated root-cause
   pass, not folded into this report.

2. **shadow-qwen35-csp / shadow-plutus-csp graduation gate:** `GRADUATE_N
   = 30` has no time bound or activity floor — propose adding one of:
   (a) a documented "stale audition" state if `N_closed` hasn't grown in
   X calendar days (surfaces the stall instead of silently sitting at
   3/30 forever), or (b) a VIX-independent low-vol CSP variant (wider
   delta / smaller target return) so the wheel can still write premium
   income in a persistently sub-18-VIX regime, rather than going fully
   dormant for weeks at a time. Either is an Admiral call, not something
   to default into.

3. **McCoy BPS / Anderson BCS / Quark IC:** these three are not "slowly
   auditioning" — they are permanently frozen at `insufficient_data` by
   construction. Propose picking one of two honest end-states, same
   framing OPS TRIAGE item 1 used for the dead-letter queue: (a)
   explicitly reactivate into the "dedicated shadow/sim loop" the
   crew's own code describes as the valid non-executing path to accrue
   audition data, or (b) write a dated decision (XO_BACKLOG.md, same
   pattern as other closed-loop retirements) that these three are
   intentionally retired with no reactivation path under current
   criteria, so the weekly `insufficient_data` proposal stops being
   generated (it hasn't run since 07-19, likely also outage-silenced,
   but shouldn't spin forever regardless).

4. **Quark's actual bar:** if 50 closes was meant as the real target
   (versus the codified 20), that needs to be reconciled explicitly in
   `AUDITION_CRITERIA` or a Quark-specific override — currently there is
   no such override anywhere in code.

5. **OllieTrades Signal:** the options playbooks being unreachable right
   now is a downstream symptom of #1, not a gate-design flaw — no gate
   change proposed. Once fleet-wide signal generation recovers, re-check
   whether options-typed signals resume; if they don't (e.g., because
   Troi/shadow seats stay VIX-gated), that would be the point to revisit
   `min_trades`/`min_rating` specifically for the options-style playbooks,
   or accept `ollie_live_swing` as the only practically-reachable playbook
   for now and reflect that in how Phase 1 is described.

**Admiral disposes** — this report proposes, does not implement, per
directive.
