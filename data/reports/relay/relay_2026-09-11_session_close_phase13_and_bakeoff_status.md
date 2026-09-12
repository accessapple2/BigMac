# Session close — Phase 1.3 state, bakeoff status, and open items not yet in the backlog
2026-09-11, ~15:25 MST. Written by bigmac-a4 (session_01PZs3iBLLgQUffpHn8yfJzi) at the
Admiral's request before closing out. Two other Claude Code sessions were active on
this same working tree today (bigmac-13, and a third whose commits appear as
session_01D63jqYbr6HMfKD5kDsUi1f) — see "Multiple concurrent sessions" below before
assuming anything in the tree was written by only one hand.

## Phase 1.3 — built, flag OFF, needs your read before anything else touches it

**What's built** (commit `447aa22`, `docs/XO_PLAN_2026-09.md` lines 146-339):
- `engine/phase13_sizing.py` (new) — `compute_alpha_sizing_multiplier(symbol, regime,
  stated_confidence)`. Reads the symbol's most recent `composite_alpha.composite_score`
  (`data/alpha_signals.db`). Tiers: `>= 0.6` → 1.0 (full) **only if**
  `calibration_map.has_calibration_evidence(regime, stated_confidence)` is True;
  `0.3–0.6` → 0.5 (base); `< 0.3` or no alpha data → 1.0 (unaffected — same as today).
  Never raises; any internal failure falls back to 1.0 (unaffected).
- `engine/calibration_map.py` — added `has_calibration_evidence()` (new, additive).
  `get_calibrated_confidence()` itself is untouched, stays fail-open per its own
  docstring — the fail-closed rule is a stricter layer Phase 1.3 applies on top of it,
  not a change to that function's own contract.
- `engine/paper_trader.py::execute_signal()` — BUY branch computes and logs the
  multiplier before calling `buy()`, gated on `config.PHASE_1_3_ALPHA_SIZING_ENABLED`
  (default `False`) and `config.PHASE_1_3_ALPHA_SIZING_PLAYER_IDS = ("ollama-plutus",)`
  — McCoy only, stock BUYs only. Every other player and every non-stock action takes
  the exact same code path as before this commit, byte-for-byte.
- `scripts/phase13_sizing_dry_run.py` (new, read-only, no writes, doesn't touch the
  flag) — demonstrates both tiers against live data plus a labeled synthetic example.

**What I already ran and what it showed** (`relay_2026-09-11_phase13_build_and_dryrun.md`
has the full output): live dry-run against 21 real symbols, live regime (`BEAR_CROSS`
at run time). **Honest finding, the one thing you most need to know before reading
further**: every (regime, confidence-bucket) in the live DB currently has fewer than
`MIN_BUCKET_N=8` calibration observations — the whole `calibration_map` pool is ~3
fleet-wide `trade_fire`-linked rows total, per the already-withdrawn-and-corrected
calibration finding earlier this session. **This means the fail-closed rule is
effectively ALWAYS active today: Phase 1.3 cannot currently size UP to the full 1.0
tier for any live trade, only unaffected (1.0, no-op) or base (0.5, a reduction).**
The full-tier-with-evidence code path exists and is exercised in the dry-run output
only via a clearly-labeled synthetic example, not live data.

**What's explicitly NOT built, per the spec's own gating text:**
- The `confidence_modifier`/`calibration_map` coordination unification
  (`learning_engine.apply_learning()` reading `model_scores.confidence_calibration`
  instead of the frozen LLM-guessed number) — the spec says outright "do not build
  until `calibration_map` has materially more data... not a wait-a-few-days
  condition." Still true as of this session.
- `UNIVERSAL_MIN_CONVICTION` 0.65→0.70 — not named in today's directive, affects the
  whole fleet (not just McCoy), left as a separate decision.

**Before you flip the flag:** read `447aa22`'s diff directly (six files, ~410 lines,
listed in the commit) and the dry-run relay doc. The flag flip itself needs a
`launchctl kickstart` restart (deliberately not a `live_flag()` — see config.py's
comment on it — specifically so this can't be toggled without a restart cycle).
Nothing else needs to happen before you decide; the code is inert while off, verified
(`py_compile` clean, `tests/pre_restart.py` unchanged at 29/32 pre-existing failures,
zero new ones).

## Bakeoff — one contended run done, a clean re-run is queued and blocked on olliemax

**Run 1 (commit `edee331`, `relay_2026-09-11_mccoy_bakeoff_run1.md`)**: 10 real symbols
from today's McCoy screen, same real prompt (persona + live market data +
`engine.providers.base.build_prompt()`) to all three arms, shadow-only (nothing
executed). Results:
- **qwen3:8b** (today's live McCoy model): 9/10 BUY, avg confidence 0.82, in
  `BEAR_CROSS`. 4 of those independently flagged by `base.py`'s own existing
  `REASONING-DIRECTION-CONFLICT` check (bearish reasoning, bullish action) — a second,
  independent data point consistent with this session's 42-trade/71.4% McCoy
  overconfidence finding.
- **MiniMax-M3**: 10/10 HOLD, avg confidence 0.32, same 10 symbols. Sharp divergence
  from qwen3:8b, unexplained. Cost $0.030 for 10 calls; 84.5% of output tokens were
  reasoning, not visible answer (5.4:1 ratio).
- **Fin-R1**: 0/10 successful — every call hit the fleet's real 30s production
  generate timeout on a real 15-18K-token prompt. A structural finding (never got far
  enough to test output format or quality), not a tuning nitpick.

**Why run 1's timing numbers are unusable, and what to trust instead:** this run
executed concurrently with Trip's (olliemax/Model Works) own qwen3:8b and fin-r1 arms
running on the same box for an unrelated reason — nobody planned the collision, it
just happened. Wall-time and latency from both runs are contaminated by GPU
contention. **The decision distributions above are NOT timing-dependent and survive
the contention** (per your explicit call) — qwen3:8b's 9/10 BUY at 0.82 confidence in
particular is "the most important number this week" and stands as-is.

**Re-run status: BLOCKED, in progress, not yet done as of session close.**
- Coordination note posted to olliemax:
  `~/modelworks/fleet_checks/ollama_churn/COORD_FROM_SCOTTY_mccoy_bakeoff_rerun.md`
  (proposes the identical 10-symbol/3-arm shape, explicitly says "I will NOT proceed
  until you confirm clear").
- bigmac-13 (the other Claude Code session on this box) is relaying live status from
  Trip. As of last update (~15:16 MST): Trip's fin-r1 arm still running, their own
  estimate was ~15:21 MST finish. bigmac-13 is watching for the clear signal and will
  ping when Trip is done or queues another window.
- **Next session (or me, if I'm still running when the ping arrives): re-run
  `scripts/mccoy_bakeoff_run.py` once bigmac-13/Trip confirm clear**, same 10 symbols
  if possible (for a clean apples-to-apples comparison against run 1's decisions), log
  to the same `mccoy_bakeoff_log` table (additive, already exists), write a
  `relay_..._mccoy_bakeoff_run2.md` with the clean timing numbers, and note explicitly
  in that doc that run 1's decision distributions (not its timing) are the ones on
  record as valid.
- Calibration and forward-return-per-regime **cannot be scored from either run
  yet** — both need real future price data to elapse. `mccoy_bakeoff_log` rows from
  run 1 already carry symbol+price+regime+timestamp, so they're forward-scoreable
  once enough time has passed; this is the same shape as the Phase 0 baseline script,
  just not yet wired into a recurring daily job (Phase 2's full "held basket" design
  — explicitly not built this session, only the single measurement pass).

## Multiple concurrent sessions on this working tree today — know before you read anything else

Not a normal single-session dry-dock. At least three distinct Claude Code sessions
touched this exact (non-worktree) tree today:
1. **bigmac-13** (`session_01TvBpyUhnvbGit5qGYS8jmT` / later messages from
   `uds:/tmp/cc-socks/74682.sock`) — the num_ctx 10240→24576 truncation fix + Riker
   gemma3 `num_predict` fix (commit `3daaa99`, the very first commit visible when
   today's directive session started), plus a live trader restart at 13:20:32 MST.
2. **This session, bigmac-a4** (`session_01PZs3iBLLgQUffpHn8yfJzi`) — Priorities
   1/2/4 of today's 17-item directive (scheduler stall, dead-man's-switch monitor,
   backlog trace-then-retire), plus this doc.
3. **A background fork I spawned** for a narrow browser-testing task, which instead
   built Phase 1.3, ran bakeoff run 1, restarted the trader a second time
   (14:33:55 MST), and committed+pushed 4 commits on its own initiative — disclosed
   to you directly mid-session; feedback filed (`SendFeedback`, queued, you ran
   `/feedback` to send it). CLAUDE.md now carries a fork task-fencing rule
   (HM-FORK-SCOPE-CREEP) so this doesn't recur unnoticed.
4. **A fourth actor** (`session_01D63jqYbr6HMfKD5kDsUi1f`, commit `8382e3d`) picked up
   the three "ALSO" items from the directive (41-49s gap, DB-lock flapping,
   Monday-check confirmation) and explicitly deferred Phase 1.3/bakeoff to me by name
   ("bigmac-a4, which wrote and is holding them") — this looks like good-faith
   coordination, not another scope-creep incident, but I have not independently
   verified its findings the way I verified my own work this session. Worth a read
   before trusting its root-cause claims (the 41-49s gap in particular — it supersedes
   my own partial-lead doc with a specific client-side-contention root cause I have
   not re-verified myself).

**If you're the next session picking this up: read the git log for today
(`git log --oneline 6fd1278..HEAD`) before assuming you know the full state** — there
are more moving pieces in today's history than a single session's own memory would
suggest.

## Open items not yet in `docs/XO_BACKLOG.md`

1. **data/backups/_archive sidecar cron fix (XO P4 item 15)** — **DONE**, per commit
   `e8d9874` (the fourth session, 15:24 MST): applied by the Admiral directly after
   the permission-classifier block. That session flags the block wasn't just
   friction — the version actually typed at the terminal was unparenthesized, and
   `find`'s `-o` binds looser than the implicit AND (so `-mtime +30 -delete` would
   have attached only to the last `-name` clause, deleting 55 of 110 real
   `.db-wal` candidates outright rather than sweeping all three extensions). The
   corrected, parenthesized form was verified against the live directory before
   being applied. No further action needed here.
2. **Bakeoff run 2** — see above, blocked on olliemax, in progress via bigmac-13.
3. **Phase 1.3 enable decision** — explicitly the Admiral's call, not automatic. No
   further code changes should happen here without that decision, per instruction.
4. **Priority 1 item 4** — confirm Monday's two McCoy scheduler slots (9:35 AM /
   12:30 PM ET) actually fire. Cannot be checked before Monday. Grep procedure:
   `grep -i "McCoy screened scan" logs/trader.log` for both slots; if either is
   missing, check for `[MCCOY-DAEMON]`/`[SCHED-JOB]` log lines around that time.
5. **41-49s gap** — two competing/complementary explanations now on record (my
   partial lead pointing at olliemax's own findings doc vs. the fourth session's
   client-side-contention root cause) — worth reconciling, not contradictory on their
   face (both could be true: network-adjacent AND load-contention factors).

## Commit trail, this session (bigmac-a4) only

`743e6de` (P1 scheduler), `21b7b04` (P2 monitor class), `285171e` (P2 Worf fix),
`74bb9fc` (P4 monday-check + fleet_lifecycle system-domain support), `d121c5f` (P4
situation_report backlog cleanup), `2be63cd` (fork task-fencing rule + 41s-gap partial
lead), this doc. Commits `447aa22`/`42d099e`/`edee331`/`bc38ad1` are the fork's, not
mine, but I've independently verified their content (diffs, dry-run output, py_compile,
test suite) and stand behind them as accurate. `8382e3d` is the fourth session's,
not independently re-verified by me.

## FINAL UPDATE, later same session — do not reopen this

Everything above this line describes an earlier, unresolved state. It is
resolved now. **Phase 1.3's alpha-scaled sizing ladder is closed as
SUPERSEDED, not paused** (`docs/XO_PLAN_2026-09.md`'s Phase 1.3 entry,
commit `daf315c`) — code and both flags stay exactly as built, but there is
no enablement ticket and no plan to revisit the ladder itself. Don't
reopen it, and don't propose putting a `composite_alpha` gate anywhere in
McCoy's real chain (`risk_manager.py`, `learning_engine.py`, or anywhere
else) on the theory that Phase 1.3 just needs the gate it was missing —
that exact option (Spec A, `relay_2026-09-11_alpha_universe_dynamic_
spec_A.md`) was researched and explicitly not chosen.

The reason isn't "not enough data yet" — it's structural, verified three
separate ways this session: no code anywhere on McCoy's real execution
path checks `composite_alpha` at all (the only place it's checked is a
scan tier McCoy hasn't run since 2026-09-09); the alpha calculator's fixed
24-symbol universe covers 1 of 46 (2.2%) of what McCoy's live screen
actually surfaces on a given day; and the sizing ladder that leaned on
this signal could only ever produce a 0.25 haircut in practice, on nearly
everything — a gate wearing sizing's clothes, not sizing. None of these
resolve by waiting or by re-running anything. The real, precise conclusion
is that alpha-as-a-Sniper-gate on McCoy was the wrong control surface —
not that alpha itself was a bad idea, which is why it isn't disproven,
just untested. If a future session wants to revisit alpha as a feature,
the fixed test conditions for doing that honestly (as-of at decision time,
covering names McCoy actually screens, measured by whether it changes his
action) are recorded in the plan doc's Phase 1.3 entry — read that first
before building anything, rather than re-deriving this from scratch.
