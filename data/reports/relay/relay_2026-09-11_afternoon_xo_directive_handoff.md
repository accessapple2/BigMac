# Session handoff — 2026-09-11 afternoon. XO 17-item directive, worked top to bottom.

Everything below is committed and pushed to `exec-pipeline`. Fleet is live,
restarted 14:33:55 MST (PID 10282), verified clean. Read this before
anything else.

## #1 thing to check first next session: did Monday's two McCoy slots fire?

**Priority 1's fix is deployed but not yet verified end-to-end** — that
needs a real trading day. Monday, `grep -i "McCoy screened scan"
logs/trader.log` for both the 9:35 AM ET and 12:30 PM ET slots. If either
is missing, check `logs/trader.log` for `[MCCOY-DAEMON]`/`[SCHED-JOB]`
lines around that time — the new instrumentation should make root-causing
a repeat failure fast, unlike last time.

## Priority 1 — scheduler stall — SHIPPED, not yet Monday-verified

- `[SCHED-JOB]` instrumentation on every `schedule.run_pending()` dispatch
  (entry+exit+wall time) — live-verified firing.
- `run_mccoy_screened_scan` moved off the shared queue onto its own 60s-
  poll daemon thread (`mccoy_scheduler`), mirroring the existing WR daemon
  pattern. Phase 1.2's cadence no longer depends on the shared queue's
  health.
- Same-day recovery: a missed slot now fires up to 60 min late (logged
  `[LATE — same-day recovery]`) instead of silently never firing.
- Commit `743e6de`.

## Priority 2 — dead-man's-switch monitor class — SHIPPED

- `ExpectedWorkMonitor` (generic, `scripts/hm_ops_sentinel.py`) — alerts
  when expected work doesn't happen, parameterized by (what, how often,
  what evidence). Commit `21b7b04`.
- Concrete instances: per-advisor staleness on `portfolio_advice`
  (grok/troi/worf individually), per-player `decision_audit` silence
  (McCoy named), and a REAL one-token `/api/generate` probe against
  olliemax per model (not `/api/ps`/`/api/tags` — both stayed green
  through the actual 2026-09-10 stale-socket outage). Live-verified: both
  probed models answered.
- McCoy's learning_engine coverage gap (item 7): **already fixed earlier
  today**, before this directive — `554a3fc`, this morning's session. I
  closed the one explicitly-noted remaining gap: Worf's real activity
  lives in `portfolio_advice`, not `signals`, so the original fix's
  freshness check couldn't see him. `_ADVISOR_PLAYER_MAP` fixes that.
  Commit `285171e`. **Needs today's restart to take effect for tomorrow's
  13:15 MST run — done, see below.**

## Priority 3 — Phase 1.3 + bakeoff

- **Phase 1.3 alpha-scaled sizing: built behind a flag, NOT enabled.**
  `config.PHASE_1_3_ALPHA_SIZING_ENABLED = False`. Scoped to McCoy only,
  stock BUYs only. Fail-closed on missing calibration evidence, per spec.
  Honest finding: every live (regime, confidence-bucket) currently has
  <8 calibration observations, so the fail-closed rule is effectively
  ALWAYS active today — this feature can currently only ever size DOWN
  from the existing 1.0 baseline, never up, until real calibration data
  accrues. Full diff summary + dry-run output:
  `relay_2026-09-11_phase13_build_and_dryrun.md`. Commit `447aa22`.
  **Admiral: read that doc + the diff, then decide go/no-go — nothing
  further happens on this without an explicit enable.**
- **Bakeoff: first real measurement run done.** 10 real symbols from
  today's McCoy screen, same real prompt to qwen3:8b / Fin-R1 /
  MiniMax-M3. **Headline: qwen3:8b went 9/10 BUY at avg confidence 0.82 in
  BEAR_CROSS (4 flagged by the existing REASONING-DIRECTION-CONFLICT
  check); MiniMax-M3 went 10/10 HOLD, avg confidence 0.32, same symbols.
  Fin-R1 timed out on all 10 calls (real 30s production timeout, real
  15-18K-token prompts) — zero usable responses.** Full detail + the
  timeout-scope caveat (worth its own look, not touched here):
  `relay_2026-09-11_mccoy_bakeoff_run1.md`. Commit `edee331`. Coordinated
  with olliemax first via `COORD_FROM_SCOTTY_mccoy_bakeoff.md`.

## Priority 4 — backlog trace-then-retire

Six of eight items turned out to be **already resolved earlier today**,
before this directive landed (situation_report, tour.ollietrades.com, the
guarded_connect 395-file sweep, Grep Gate CI, and two of the "ALSO"
exploratory items — gemma3's runaway and, per the earlier truncation-flag
relay doc, the 4,575-row CSV flagging). I verified each live rather than
trusting the backlog text, and updated the one stale duplicate entry
(situation_report's "still open" row in the top consolidated table).
Genuinely new work this session:

- **Item 11 (monday-check monitors):** retired the one actually-stuck
  monitor (`hm-bridge-consensus-monday-check`) — the other three were
  already retired 2026-08-30. Found and fixed a real gap along the way:
  `fleet_lifecycle.py` had no concept of System LaunchDaemons at all
  (only `~/Library/LaunchAgents/`), so this hard-failed with a confusing
  error. Extended it to recognize `/Library/LaunchDaemons/` and degrade
  gracefully when the live `launchctl` step needs root this session
  doesn't have (ledger+doc still write; a one-line manual `sudo launchctl
  bootout` is flagged, purely cosmetic — the job is a dead one-shot that
  already fired once, mathematically cannot fire again). Commit `74bb9fc`.
  Note: the directive said "two stuck monday-check monitors" — only found
  one, backed by a live launchd inventory + the ledger; flagging the
  discrepancy rather than inventing a second one.
- **Item 13 (Bridge cosmetics):** fixed the one real, verified stale item
  — a hardcoded "Season 6" label on bridge-v2.html's Equity Curve heading
  (now binds to the same live season number the leaderboard label already
  fetches) and a conceptually-wrong "Season 6" badge on index.html's
  Production Truth panel (relabeled "SINCE INCEPTION" — that panel was
  never season-scoped to begin with). Commit `42d099e`. **Near-miss,
  disclosed:** I initially misjudged `/api/riker/recommendation` as dead
  code and started removing it across both dashboard files — it's
  actually live, backed by a separate `riker_xo.py` mechanism distinct
  from the 2026-06-24 retirement I was thinking of. Caught and fully
  reverted before commit (confirmed via `tests/pre_restart.py`, which has
  its own dedicated check for this exact endpoint + UI row — now passing
  clean). No Riker-related file changes shipped. Gamma Map and Autopilot
  (the other two named sub-items) were investigated and found already
  correctly wired — no defect, no action.
- **Item 15 (backups/_archive 3.2GB retention): BLOCKED, needs your
  permission.** The 3.2GB figure is mostly the *already-correctly-managed*
  `.gz` archive tier (17 files, real 30-day TTL cron already running) —
  the genuinely-unmanaged part is ~2MB of orphaned `.db-shm`/`.db-wal`
  sidecar debris (130 files, 110 already 30+ days old). I drafted the
  one-line cron extension (same `-mtime +30 -delete` pattern the existing
  `.gz` tier already uses) but the auto-mode classifier refused the edit
  as "Irreversible Local Destruction" before I could apply it. I didn't
  attempt to work around that. **If you want this applied**, the change
  is: extend the existing `HM-ARCHIVE-TTL` cron line's `find` pattern from
  `-name "trader_*.db.gz"` to also match `-o -name "*.db-shm" -o -name
  "*.db-wal"`, same `-mtime +30 -delete`. I can apply it the moment you
  say go, following the CLAUDE.md safe cron-edit procedure (dump-to-file,
  diff, count-guard, install file).

## The "ALSO" items

- gemma3's ~20K-token runaway: **already fixed earlier today** (before
  this directive) — `num_predict` was a top-level JSON key instead of
  inside `options`, so the cap was silently never applied. Fixed +
  restarted, confirmed in this morning's relay doc.
- origin_healthcheck's "database is locked" flapping: partially
  investigated in an *earlier* session (one real cause confirmed — main.py
  down at 12:10 — the fuller 3hr multi-service pattern from 8/31 stayed
  unexplained, no surviving logs). Not touched further this session — no
  new evidence to add.
- 41-49s gaps outside Ollama's own timing on three Arena calls: **not
  investigated** — no prior trace exists anywhere in the backlog and I
  found no specific reference to identify which three calls; would need
  either a pointer to the original observation or a fresh multi-day log
  trace, out of the time budget for a same-session "if time allows" item.

## Deploy status

Trader restarted 14:33:55 MST (PID 10282, single-PID clean, no orphans) —
picks up the scheduler fix (already live since the first restart this
session), the Worf learning_engine fix, and Phase 1.3's dormant (flag-off)
code. Pre-restart backups: `data/backups/{trader,signals}_prerestart_
20260911_143334.db`, integrity_check=ok both.

## Full commit trail this session (exec-pipeline)

`743e6de` (P1 scheduler), `21b7b04` (P2 monitor class), `285171e` (P2 Worf
fix), `74bb9fc` (P4 monday-check + fleet_lifecycle system-domain support),
`d121c5f` (P4 situation_report backlog cleanup), `447aa22` (P3 Phase 1.3
build), `42d099e` (P4 Bridge cosmetics), `edee331` (P3 bakeoff run 1).
Nothing force-pushed or rewritten.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PZs3iBLLgQUffpHn8yfJzi
