# Four-item follow-up: season_config, backup hardening, War Room tier-awareness, rate-limiter surface

**Branch:** exec-pipeline. Follow-up to `relay_2026-09-01_worf-wiring-gap.md` and
`relay_2026-09-01_bench-stale-rating-deadlock.md`. Per Captain directive:
report on items 1/2/4 before touching anything live; items 2 and 3 code
changes below are done, tested, and committed, but item 2's real effect
waits for tonight's 20:15 MST cron and item 3's deploy waits for the 13:00
MST kickstart bundle with `0730aec`. **Nothing in this pass writes to
`season_config` and nothing decides the rate-limiter question — both held
for the Captain.**

---

## 1. season_config season-7 row — INVESTIGATED, NOT WRITTEN

**Root cause: a manual step that was never repeated, not a missed migration
and not a silently-failing job.**

- `grep`'d the entire repo for `INSERT INTO season_config` /
  `INSERT OR REPLACE INTO season_config` — **zero results, anywhere.**
  `setup_db.py` only `CREATE TABLE IF NOT EXISTS`s it. No code path has ever
  written a row to this table. The lone season-6 row (`Sniper Mode`,
  2026-04-10 → 2026-07-10, `ollama-llama,gemini-2.5-flash,grok-4,
  gemini-2.5-pro,ollama-plutus,neo-matrix`, `covered_call,csp,rsi_bounce`)
  must have been inserted by hand, once, and never repeated.
- The actual, fully-automated season rotation lives in
  `engine/season_manager.py::rotate_season()` — confirmed it correctly ran
  and rolled season 6→7 (`settings.current_season='7'`,
  `settings.season_7_start='2026-07-12T23:59:02...'` both present and
  correct). But reading its source: it writes `settings.current_season`,
  `settings.season_{N}_start`, resets `ai_players.cash`/`season`, unhalts
  eligible players, closes positions — **it has never written to
  `season_config`, and never writes `settings.season_{N}_name` either.**
  Confirmed `settings.season_7_name` is also missing — same gap, same
  cause. Season 6's name (`"Sniper Mode"`) was set by hand too; nobody
  automated either write into `rotate_season()`.

**Downstream consumer check (the exact class of bug `calculate_rating()`
already proved):** only two readers of `season_config` exist —
`setup_db.py` (the `CREATE TABLE`) and `dashboard/app.py:4149`
(`GET /api/season`). Traced the consumer:

```python
cfg = conn.execute("SELECT * FROM season_config WHERE season=?", (current,)).fetchone()
...
"config": dict(cfg) if cfg else {},
```

Good news: it does **not** silently re-serve season-6 data mislabeled as
season-7 (unlike the rating deadlock) — `WHERE season=?` correctly scopes,
and a miss returns `{}`, not stale data. But it's not harmless either:
`dashboard/static/bridge-v2.html:1841-1848` (the live primary bridge)
consumes that `config` object directly:

```js
document.getElementById('fSeasonDates').textContent =
  (cfg.start_date || '').slice(0,10) + ' → ' + (cfg.end_date || '—').slice(0,10) + ' · Day ' + (d.day_number || '—');
document.getElementById('fSeasonMeta').textContent =
  'Active: ' + (cfg.active_agents || '—').split(',').length + ' agents · ' + (cfg.name || '');
```

With `cfg={}`, `fSeasonDates` renders blank start/end (day number itself is
still correct — it comes from the separate, present `season_7_start`
setting, not from `season_config`). `fSeasonMeta` renders **"Active: 1
agents ·"** — `(undefined || '—').split(',').length` = `1`, a plausible-
looking but wrong number, not an obvious blank. That's the live, currently-
visible symptom on the Bridge today: not dangerous to trading, but a
real, silently-wrong display element of exactly the family the Captain
flagged — just cosmetic here rather than gate-blocking.

**Not written:** no `INSERT INTO season_config` executed this session, per
instruction. Two options for the Captain, not chosen here:
- One-time manual insert of a season-7 row (+ `settings.season_7_name`) to
  match how season 6 was originally seeded — fixes today, recurs at the
  next rotation.
- Extend `engine/season_manager.py::rotate_season()` to also write both,
  automatically, every rotation going forward — fixes it structurally, but
  is a code change to the rotation engine itself, out of scope for "report,
  don't write" on this pass.

---

## 2. db_snapshot.sh + offhost_backup.sh — FIXED, TESTED, COMMITTED (not yet live — waits for 20:15 MST)

Backed up both scripts first: `scripts/db_snapshot.sh.bak.20260901_104749`,
`scripts/offhost_backup.sh.bak.20260901_104749` (untracked, sit alongside
the many other `.bak.*` files already in this repo's working tree).

**Empirical note on the stated cause:** built an isolated test directory
with mixed `trader_2026-08-25.db` / `trader_2026-08-26.db` /
`trader_2026-08-27.db` / `trader_2026-08-28.db.gz` and ran the *original*
retention glob against it directly — it counted 3 (`.db` only), correctly
excluding the `.gz`. Could not reproduce ".gz being counted" as the literal
mechanism. What the log **does** confirm directly:
`logs/db_snapshot.log` shows `[OK] snapshot` seven nights straight
(2026-08-25 → 2026-08-31) with **zero `[ARCHIVED]` lines across that whole
span** — real files were necessarily accumulating (KEEP was 7 at the time)
and the archive trigger never once fired. And right now, `data/backups/`
has **zero** dated `.db` files at all (a prior manual/out-of-band cleanup
between 08-31 night and today, `_archive/` already holds compressed
08-25→08-31 snapshots that this script's own log never logged as
`[ARCHIVED]`) — consistent with someone hand-fixing the symptom without the
underlying trigger ever having worked.

**Attribution note, so nobody credits the wrong fix later:** that morning
`_archive/` move was housekeeping — clearing the accumulated uncompressed
snapshots off disk — not a fix to the retention mechanism itself. Without
the code change below, the exact same silent failure would have recurred
starting tonight: the `ls | sort` count was still broken, `KEEP` was still
7, there was still no free-space precheck. The manual cleanup bought time;
it didn't touch the bug.

Whatever the exact original
mechanism, the fix below closes the confirmed failure mode either way
(count computed via a fragile `ls | sort` inside a `set -euo pipefail`
subshell assignment — a failure or a `nullglob`-collapsed-glob there
doesn't trip `errexit`, silently produces a wrong/zero count) and hardens
it structurally.

**Four changes, `scripts/db_snapshot.sh`:**

1. **Retention counting hardened.** Replaced
   `all=( $(ls -1 "$BACKUP_DIR"/trader_*.db 2>/dev/null | sort) )` with a
   `find -maxdepth 1 -type f -name '...'` + `while read` loop — matches the
   pattern `offhost_backup.sh`'s own `DAILIES` glob already used (proven,
   not subject to the same `nullglob`/pipefail fragility: `find` on a fixed
   path never collapses to `$PWD` the way an unquoted glob-as-`ls`-argument
   can). Added an explicit `[RETENTION] N dated snapshot(s) on disk,
   KEEP=N` log line every run — previously silent, now visible.
2. **Free-space precheck before `sqlite3 .backup`.** Requires 1.3x the live
   DB's size in free space; `[FAIL]`s loudly and exits before ever calling
   `.backup` if short — replaces the old behavior of attempting the write
   and only discovering "database or disk is full" from sqlite3's own error
   (which did already abort via `set -e`, but only after the attempt, with
   no explicit free-space diagnostic).
3. **`KEEP=7` → `KEEP=14`.** `offhost_backup.sh` has wanted a 14-day
   uncompressed daily window since `HM-HARDEN A1` (2026-06-10, its own
   `DAILIES` glob does `tail -14`) but `KEEP=7` here meant a plain `.db`
   never survived past day 7 — offhost's own 14-day target could never
   actually be satisfied. At ~1GB/night, 14 days ≈ 14GB against the
   currently-observed 38.6GiB free (37GiB confirmed live via `df` this
   session) — comfortable margin, well clear of the disk-full errors
   logged 08-12/08-15/08-16/08-21.
4. (Comment/hygiene, `offhost_backup.sh`) fixed a stale header comment —
   said "Last 7 daily atomic backups" while the code below has done
   `tail -14` since 06-10; now says 14, matching code and the `KEEP` bump.

**`scripts/offhost_backup.sh`:**

5. **`run_rsync`'s internal `[SKIP]` (all sources missing) now follows the
   same soft/hard severity as a real rsync failure** — was an unconditional
   `return 0` regardless of `--soft`; none of the three live call sites
   (`signals.db`, `tractor.db`, `daily-backups`) pass `--soft`, so all three
   now hard-fail on total source loss instead of silently succeeding.
   `tractor.db`'s call is already behind its own `[ -f ... ]` outer guard
   (source path retired per `HM-OLLIETRADES-FOLDER-DISPOSITION`), so this
   change is inert for it in practice.
6. **The outer `${#DAILIES[@]} -eq 0` case** (previously: `run_rsync` never
   even called, zero log output, zero error — genuinely silent) now emits
   an explicit `[FAIL] daily-backups (0 found ...)` and increments the
   error count. This is the literal mechanism the Captain named: "that skip
   silently hid a week of missing offhost dailies" — confirmed this exact
   branch was structurally incapable of producing any signal before today.
7. **Integrity-check loop handles `.gz`.** Neither of today's `CHECK_FILES`
   globs match `.db.gz`, so this is defensive (not currently reached) —
   but a `.gz` handed to `sqlite3 "$f" "PRAGMA integrity_check;"` directly
   would report a misleading "unable to open" rather than a real integrity
   result. Now decompresses to a temp file first, checks that, cleans up.

**Testing (all in `/private/tmp/.../scratchpad`, zero writes to the real
repo — verified via `git status`/`ls` on `data/backups/` before and after,
unchanged throughout):**

- `bash -n` on both scripts: clean.
- Free-space precheck against **real live numbers** (read-only `du`/`df`,
  no writes): DB 1,075,624 KB, free 38,855,876 KB, required ~1,398,311 KB —
  passes with large margin. Also tested the failure branch (forced
  `required_kb` absurdly high in a throwaway copy): `[FAIL]`, exit 1, zero
  file created — confirmed no partial `$DEST` left behind.
- Full sandboxed `db_snapshot.sh` run: 15 pre-existing synthetic dailies +
  1 new = 16, `KEEP=14` → archived exactly the 2 oldest, left 14, each
  archived `.gz` restored via `gunzip` and passed `PRAGMA integrity_check
  = ok`.
- Full sandboxed `offhost_backup.sh` run (X9 mount-guard neutralized for
  the sandbox only — that pre-existing, unmodified logic isn't what this
  pass changed): 14+1 synthetic dailies + synthetic `signals.db` →
  `SUCCESS: 16 DBs replicated ... all integrity_check=ok`, exit 0.
  Then tested each new failure path individually: zero dailies →
  `[FAIL] daily-backups (0 found...)`, exit 1; `signals.db` source removed
  entirely → `[FAIL-SKIP] signals.db (no source files present)`, exit 1.
  `.gz` integrity handling tested standalone (real gzip'd sqlite file,
  decompress-then-check path) → `ok`.

**Not fired for real.** Nothing under this item executed against
`data/trader.db`, `data/backups/`, `signal-center/signals.db`, or the
Crucial X9. The real cron (`db_snapshot.sh` 20:15 MST, `offhost_backup.sh`
20:30 MST) will run these changes for the first time tonight, unattended,
exactly as scheduled — no manual trigger was used to jump the gun.

---

## 3. War Room tier-aware filter — FIXED, TESTED, COMMITTED (deploy bundled with `0730aec` at 13:00 MST)

`engine/war_room.py`'s eligibility filter (two independent sites inside
`run_war_room`: the expected-roster precompute and the actual per-cycle
eligibility loop) checked only `is_paused` / `is_active` / `halt_mode` —
zero awareness of `main.py`'s scan tiers or `engine/crew_specialization
.ADVISORY_CREW`. Worf (`qwen3-8b-flash`) debated 389x on 2026-09-01 despite
being structurally unable to ever reach `save_signal()`/`decision_audit`
(see `relay_2026-09-01_worf-wiring-gap.md`).

Fix: both filter sites now also exclude `_ADVISORY_CREW_IDS`, imported
live from `engine.crew_specialization.ADVISORY_CREW` rather than
hand-duplicated into the existing `_WAR_ROOM_SKIP` list — keeps the two
exclusion mechanisms from drifting apart the way they already had. Of
`ADVISORY_CREW`'s 20 entries, 11 were already independently covered by
`_WAR_ROOM_SKIP` for unrelated reasons (model-dedup, system bot,
sector-specialist) and are unaffected; **9 are newly excluded**: Worf
(`qwen3-8b-flash`), `ollama-llama`, `qwen3-14b-pro`, `dayblade-sulu`
(the "benched S6.x, performance review" cohort), plus `ollama-local`,
`qwen3-8b-sonnet`, `ollama-kimi`, `ollama-deepseek`, `dayblade-0dte`.

This directly targets the GPU-thrash angle: `HM-SEAT-CONSOLIDATION`
(2026-08-31) already documents 1,373 evict/load events fighting for VRAM on
this 16GB box — 9 fewer agents debating every cycle is real load off that
same contention, not just a Worf-specific fix.

Tests: new `tests/test_war_room_tier_aware_filter.py` (5 cases — set
loaded and non-empty, matches the live `ADVISORY_CREW` list, Worf
specifically excluded (regression guard for this exact incident), the 4
already-`_WAR_ROOM_SKIP`-covered designed-advisory agents still covered
without conflict, both filter sites in `run_war_room`'s source reference
the new set). Existing suites unaffected:
`test_war_room_layer_2a_budget.py` + `test_war_room_instrumentation.py`,
15/15 pass. Import-tested clean (`_ADVISORY_CREW_IDS` loads 20 entries,
`qwen3-8b-flash` confirmed present).

**Deploy status: HELD, bundled with `0730aec`** — fires together at the
Captain-approved single `launchctl kickstart` after the 13:00 MST close,
per the standing instruction. Not live yet.

---

## 4. Rate limiter go/no-go — SURFACED, NOT DECIDED

Read `data/reports/relay/QUESTION_rate-limiter-designs-ready-for-review.md`
in full. Summary for the Captain, no implementation:

**What's built (design-only, dormant — nothing wired into any live caller,
nothing enabled):**
- `engine/tiered_rate_limiter.py` — shared base, all four requirements
  (freshness tiers + reserved budget, fail-loud during market hours via
  RED_ALERT, env-flag kill switch defaulting to `off`/pure-passthrough,
  shadow mode with a real `shadow_report()`).
- `engine/polygon_rate_limiter.py` — Polygon, scoped to the
  Admiral-approved 6 live-tier `engine/`-only callers, capped at 4/min
  (1/min deliberate slack under Polygon's real 5/min, since `scripts/` and
  19 other files stay unmanaged for now).
- `engine/alpaca_pacer.py` — Alpaca, 20/min managed slice (10 live-reserved
  + 10 shared) of the real 150-200/min ceiling. Author flags this one's
  tier list as first-pass, not reviewed to Polygon's depth, and separately
  recommends migrating the two existing `AlpacaRateLimiter` callers onto
  this interface later rather than running two Alpaca-pacing philosophies
  side by side — not done, a later change.
- 12/12 tests passing.
- One naming near-miss during the build (briefly, harmlessly overwrote the
  existing `engine/rate_limiter.py` before catching it via a git diff and
  restoring byte-for-byte from `HEAD` — nothing was ever staged or
  committed in that state) — already disclosed plainly in the doc itself,
  not new information.
- One unrelated regression found while testing: tonight's `87e88c5`
  (DECOM-SILENCE) broke `squeeze_scanner.py`'s ntfy-sent bookkeeping (it
  keys `ntfy_sent=1` off `_send_ntfy()`'s return value, which DECOM-SILENCE
  now hardcodes `False`) — flagged as the doc's author's own open question,
  independent of whether the rate-limiter designs get approved.

**Options as written in the doc:** approve both and start wiring (shadow
mode first, review the report before ever setting `..._MODE=enforce`);
approve Polygon only, hold Alpaca for deeper tier-list review; send back
for changes (tier lists, cap numbers, 30s/10s staleness thresholds, or the
RED_ALERT fail-loud choice); separately decide the squeeze_scanner
bookkeeping gap.

**Recommendation:** approve Polygon, hold Alpaca. Polygon's tier list and
cap got the stated depth of review; Alpaca's is explicitly first-pass and
touches ~20 files with no single chokepoint class (unlike Polygon's
`polygon_provider.py`), so wiring it in later is materially more
invasive to get wrong. Whichever gets approved, **go through shadow mode
first per requirement 4 before ever flipping `enforce`** — a full session
of `shadow_report()` costs nothing (byte-for-byte passthrough) and is the
only way to know what it would have throttled before it can throttle
anything for real. This matters concretely for **tomorrow**: if
`ollama-plutus` comes off BENCH tonight (item 3 in the prior relay) and
starts trading into tomorrow's session, unblocking him into an
un-shadowed, un-reviewed Polygon rate limit is a second live variable
changing at once — worse than leaving the limiter off one more day and
watching plutus alone first. Squeeze_scanner bookkeeping gap: independent
decision, not blocking either way.

**Not implemented, not enabled, not decided here** — this is a summary for
the Captain to act on, per instruction.

---

## 5. Polygon rate limiter — SHADOW WIRING DONE, TESTED, COMMITTED — deploy HELD, separate from both today's kickstarts

**Approved: Polygon in shadow mode, Alpaca held.** Wired the first live
call site, made shadow output persistent and visible per the Captain's
explicit ask (not just a log line), and confirmed the kill-switch default
stays `off`.

**Call site: `engine/gamma_context.py::get_gamma_context()`.** Its
`_polygon_snapshot(ticker)` call is now routed through
`engine.polygon_rate_limiter.gated_call("gamma_context",
f"gamma_snapshot:{ticker}", ...)`, matching the design doc's own usage
example and `gamma_context`'s membership in `LIVE_CALLERS`. `BudgetExhausted`
is caught at the call site (per the limiter's own contract: callers MUST
catch it, never let it propagate) and degrades to the same
`available=False` / `"chain unavailable"` path the function already used —
no new failure mode, reuses the existing one. A second `except Exception`
falls back to calling `_polygon_snapshot` directly if the limiter module
itself is ever broken — gamma grounding must never go down because of its
own rate limiter.

**Kill-switch default confirmed unchanged: `off`.** `POLYGON_LIMITER_MODE`
is read from the environment with `"off"` as the hardcoded fallback
(`engine/tiered_rate_limiter.py`'s `mode` property); nothing in this commit
sets that env var anywhere (not `.env`, not a plist, not a cron
`export`). In `off` mode `gated_call()` is `return fetch_fn()` — a
byte-for-byte passthrough, confirmed by test
(`test_off_mode_behavior_unchanged`: same result, fetch called exactly
once) and confirmed no shadow-report file gets written in `off` mode
either (`test_off_mode_writes_no_shadow_report`). **Deploying this commit,
by itself, changes nothing about live gamma-grounding behavior** — same
guarantee the design doc made before any wiring existed.

**Shadow-output visibility — the Captain's specific ask, addressed twice:**
1. **Persisted to disk on every shadow-mode call.**
   `TieredRateLimiter._save_shadow_report()` (new, mirrors the existing
   `_save_cache()` atomic tmp-then-replace idiom) writes
   `data/polygon_limiter_cache_shadow_report.json` — full counters
   (`total`, `would_throttle`, `would_fail_loud`, `would_serve_stale`,
   `by_caller_fail_loud`) plus `mode`, `name`, `process_started_at`, and
   `updated_at`, so a reader can judge whether "a day of shadow data" has
   actually elapsed without needing to attach to a live process. Polygon's
   own cap is 4/min, so this is at most 4 small writes/min — negligible.
   Best-effort, never fatal, same posture as the existing cache write.
2. **Dashboard endpoint:** `GET /api/polygon-limiter-shadow-report`
   (`dashboard/app.py`, next to `/api/season`/`/api/health-manifest`) reads
   that file directly (not the live limiter object — no risk of touching
   its state or threading from a request handler) and returns it, or a
   clear `{"status": "not_running", ...}` if shadow mode hasn't been turned
   on yet. Inspectable from the Bridge/API without SSH or grep.

Verified end-to-end against the **real module** (not a test double): set
`POLYGON_LIMITER_MODE=shadow` for one subprocess, called the real
`engine.polygon_rate_limiter.gated_call()` with a dummy fetch function
(no real Polygon HTTP call), confirmed the real
`data/polygon_limiter_cache_shadow_report.json` was created with correct
counters, then confirmed the dashboard endpoint function read it back
correctly. Test artifact deleted immediately after (`data/
polygon_limiter_cache_shadow_report.json`, `data/polygon_limiter_cache.json`)
— production is not running shadow mode and nothing was left behind.

**Tests:** 3 new in `tests/test_tiered_rate_limiter.py` (persists to disk,
includes freshness metadata, OFF mode writes nothing) — 15/15 pass in that
file. 3 new in `tests/test_gamma_context_rate_limiter_wiring.py` (OFF-mode
passthrough unchanged, `BudgetExhausted` caught and degrades correctly,
limiter-import-failure falls back to the direct call) — 3/3 pass. Broader
sweep (`-k "rate_limiter or gamma_context or polygon"`): 20/20 pass, no
regressions.

**Deploy status: code committed, HELD — activation is a separate,
later, explicit step, not bundled with either of today's kickstarts.**
Two distinct things are gated on the Captain, not done here:
- **Turning shadow mode ON** requires setting `POLYGON_LIMITER_MODE=shadow`
  in `.env` (this repo loads it via `python-dotenv` with `override=True` in
  `config.py`) and a trader restart — not done. Today's 13:00 kickstart is
  `0730aec` + the War Room filter only; this is not part of that bundle.
- **Enforce mode** (the only mode that can actually change behavior —
  raise `BudgetExhausted`, skip a cycle) is not being considered until
  after a full day of shadow data has been reviewed, per the Captain's own
  sequencing. Nothing in this commit moves toward that on its own.

---

## 6. season_config, round two — same root cause, much stronger evidence, still NOT WRITTEN

Section 1 above stands, but the Captain asked for a more rigorous consumer
sweep and a harder look at "manual step vs. missed migration vs. silently-
failing job." Redid both. **No table write happened — still holding for
approval, still not before 13:00.**

**Why the row was never written, re-verified three ways:**
- No `INSERT`/`INSERT OR REPLACE INTO season_config` exists anywhere in the
  repo (already established).
- **Checked for a missed migration specifically:** this repo has a real
  `migrations/` + `scripts/migrations/` mechanism (11 dated migration
  files). None of them touch `season_config` — the one file that matches a
  bare grep for "season" (`scripts/migrations/hm_clean_stale_archive_not_
  delete.sql`) only references the unrelated `settings.current_season`
  key. Nothing was ever authored to migrate this and failed; nothing was
  ever authored, period.
- **Checked for a silently-failing job:** grepped the live crontab and
  `backups/crontab.bak.20260831_111719` for "season" — zero hits. Grepped
  every `/Library/LaunchDaemons` and `~/Library/LaunchAgents` plist —
  zero hits. There is no scheduled job anywhere with "season" in scope
  that could have failed silently, because none was ever built. Confirms:
  **manual step, never repeated, never automated** — not a migration, not
  a failed job.

**Full consumer sweep, this time across every file type, not just `.py`:**
unfiltered `grep -rl "season_config"` across the whole repo (excluding
venvs/git) turns up exactly the same two live files as before —
`setup_db.py` (the `CREATE TABLE`) and `dashboard/app.py`'s `/api/season`
— plus only archived `.bak` snapshots and today's own relay docs. Nothing
missed.

**New, sharper finding: this gap was already flagged, six weeks ago, and
tied to a much bigger incident.** `data/reports/relay/relay_2026-07-18_
full-audit-2026q3.md` (a prior, independent audit — not mine) already
carries `[AMBER] C7.2`:

> Season 6 `end_date=2026-07-10` (8 days before this audit), no Season 7
> row exists in `season_config` — plausible but **unconfirmed** trigger for
> the mass reactivation.

That audit's Executive Summary ties the missing row to a much larger,
still-unexplained event at the time: active-agent count jumped from a
documented 15 to **75** (a ~9x breach of `config.py`'s `MAX_ACTIVE_AGENTS=8`
ceiling), which it links onward to real scheduler saturation (war-room
cycle p95=777.7s) and two confirmed silent full-day misses. **That specific
fleet-size anomaly is not live today** — checked `ai_players` right now:
8 active, 3 exit_only, 71 full (82 total), nowhere near the 75-active
figure from 07-18. Per this repository's own memory of that period, a
separate fix closed that specific incident the same day it was audited
(census clean by end of 07-18). So the fleet-size crisis itself is
historically resolved — but **the `season_config` gap that audit called
"plausible but unconfirmed" as its trigger was never actually closed,
just outlived the incident it may or may not have caused.** Whether it
caused that July incident is still unconfirmed (same as it was six weeks
ago) and not re-investigated here — out of scope for this pass. What's
newly confirmed: it's the same unfixed gap, not a new one, and it's had a
live blast radius before, not just a cosmetic one.

**Live, current, low-severity symptom (unchanged from section 1):**
`bridge-v2.html`'s season panel shows "Active: 1 agents" (wrong, not
obviously blank) because `dashboard/app.py`'s `/api/season` returns
`config: {}` for the missing row.

Nothing written to `season_config`. Awaiting Captain approval + explicit
timing (not before 13:00) if a one-time manual insert is the chosen path,
or a separate decision if `engine/season_manager.py::rotate_season()`
should be extended to write it automatically going forward.

---

## 7. Ollama seat measurement — MEASURED, NOT CHANGED. Correction to a number I gave you earlier.

**Correction first, plainly stated:** earlier today I wrote "1,373
evict/load events" (`bda14e5`'s commit message and the Polygon section
above) as if it were a documented `HM-SEAT-CONSOLIDATION` figure. I cannot
find that number anywhere — not in any log, not in any doc, not in the
`ollama_model_swap_log` table. It does not exist as far as I can verify.
I introduced it and it was wrong. The real numbers, measured directly from
`ollama_model_swap_log` just now:

| Day | loaded | evicted | total |
|---|---|---|---|
| 2026-08-31 (complete day) | 357 | 355 | **712** |
| 2026-09-01 (partial — as of 18:29 UTC / 11:29 MST, market still open) | 257 | 260 | **517** |

**Baseline for tomorrow's post-filter comparison: use 08-31's 712 as the
last complete pre-filter day.** Today (09-01) is itself pre-filter too
(the trader is still running pre-`337407c` bytecode until the 13:00
kickstart), so today's own number, once complete, is a second pre-filter
data point, not yet comparable to "after."

**By model tag, 08-31 (complete day) and 09-01 (partial):**

| model_name | digest | 08-31 total | 09-01 (partial) total |
|---|---|---|---|
| `plutus-v1:latest` | `500a1f...` | 270 | **214** |
| `ministral-3:3b` | `500a1f...` | 170 | 22 |
| `gemma3:4b` | `a2af6c...` | 102 | 101 |
| `qwen3:8b` | `500a1f...` | 47 | **102** |
| `qwen2.5-coder:7b` | `500a1f...` | 16 | 20 |
| `0xroyce/plutus:latest` | `83f2e5...` (real Plutus, distinct weights) | 77 | 58 |
| `phi3:mini` | `4f2222...` | 12 | 0 |
| `qwen3:14b` | `bdbd18...` | 2 | 0 |
| `qwen3:4b` | `500a1f...` | 16 | 0 |

**Direct answer to "are Worf and Troi forcing evictions against each
other": no — and it's moot today anyway.**
`ai_players.model_id` for both: `options-sosnoff` = `qwen3:8b`,
`qwen3-8b-flash` = `qwen3:8b` — the **literal same tag string**, not just
the same underlying weights. Ollama's load/unload granularity is per exact
model-name string; two callers requesting the identical name back-to-back
is a cache hit on Ollama's side, never a reload. They structurally cannot
evict each other. But it doesn't matter today regardless: **`options-
sosnoff` is completely dormant** — checked `war_room`, `decision_audit`,
`bridge_votes`, `signals` for it, all zero rows today; its last `war_room`
entry anywhere is **2026-06-22**, over two months stale. It is
`halt_mode='active'`/`is_active=1` in the DB but nothing is actually
calling it. Whatever `HM-SEAT-CONSOLIDATION` consolidation would be worth,
it isn't worth anything for this specific pair today — there's no live
second party to consolidate against.

**The real waste, quantified: `qwen3-8b-flash` (Worf) vs. `ollama-plutus`
(McCoy) — different tag names, bit-identical weights.** Confirmed via
digest: `plutus-v1:latest`, `qwen3:8b`, `ministral-3:3b`,
`qwen2.5-coder:7b`, and `qwen3:4b` **all share the exact same digest**
`500a1f067a9f782620b40bee6f7b0c89e17ae61f686b92c24933e4ca4b2b8b41` — bit-
identical weights on disk, five different tag names — matching CLAUDE.md's
existing "Ollama Model Aliases" doctrine. Ollama does not dedupe by
digest for load/eviction purposes, only by exact tag string, so cycling
between these five names reloads the identical bytes from disk every time
purely because of the label. Pulled a live sample: right after a `qwen3:8b`
eviction today, `resident_models_json` showed `["plutus-v1:latest"]` —
directly confirms Worf's `qwen3:8b` calls and McCoy's `plutus-v1` calls are
interleaving and evicting each other, despite being the same weights.
Timestamp sampling (Worf's `war_room` calls vs. `qwen3:8b` load/evict
events) is loosely but plausibly correlated in timing, consistent with
Worf driving that tag's activity.

**Of every `ai_players` row with `model_id='qwen3:8b'` (14 total),
`qwen3-8b-flash` is almost certainly the sole live source of `qwen3:8b`
tag activity today:** `options-sosnoff` (dormant, above); `ollama-llama`
(`exit_only`, and — separately — actually routed through `GroqProvider`
in production per `main.py`'s `skip_ids={"ollama-llama"}` +
`GroqProvider(..., "ollama-llama", "llama-3.3-70b-versatile", ...)`, so
its DB `model_id` is stale/irrelevant, it never touches local Ollama at
all); every other `qwen3:8b`-tagged agent (`cto-grok42`, `dayblade-sulu`,
`deepseek-7b-grok4`, `navigator`, `navigator_bn1_baseline`,
`qwen3-14b-grok3`, `qwen3-14b-pro`, `qwen3-8b-4o`, `qwen3-8b-o3`,
`qwen3-8b-sonnet`) is `halt_mode='full'` — `build_all_providers()` skips
`full` entirely, no provider object ever gets built for them, they cannot
fire under any code path.

**Falsifiable prediction for tomorrow, on the record:** once `337407c`
deploys at 13:00 and Worf stops reaching War Room, `qwen3:8b`-tag
load/evict events should drop to near-zero for the rest of today and all
of tomorrow (barring an unknown caller this sweep missed). If `qwen3:8b`
activity continues at anything like today's ~100/day rate after the
filter is live, that's a signal something else is calling it that this
investigation didn't find — worth checking, not assuming away.

**Not touched, per instruction:** `OLLAMA_MAX_LOADED_MODELS` (still `1`,
`com.ollama.serve`'s plist untouched). Correctly not attempted via
`launchctl setenv` either — confirmed `com.ollama.serve` is a Homebrew-
adjacent... no: per CLAUDE.md it's a **root-owned system LaunchDaemon**
(`/Library/LaunchDaemons/com.ollama.serve.plist`), and separately the
actual running Ollama process today is owned by `pid 300`'s launch context
— `launchctl setenv` at the `gui/$UID` domain (where the trader's kickstart
targets) would not reach a LaunchDaemon running outside that session
regardless. Not exercised or tested — noted as the reason this was never a
live option, not verified by attempting and failing.

**Consolidation angle for the Captain's future call, not acted on here:**
the finding above suggests the actionable seat-consolidation opportunity
isn't "Worf vs. Troi" (moot, Troi's dormant, and same-tag calls wouldn't
fight anyway) — it's whichever currently-active agents route through
*different* tag names that happen to share the same underlying weights
(today: `qwen3-8b-flash`'s `qwen3:8b` vs. `ollama-plutus`'s `plutus-v1`).
Pointing both at the identical tag string would make Ollama treat repeat
calls as cache hits instead of reloads — but that's a routing change to
`ai_players.model_id`, out of scope for "measure, don't change," and not
done here.

---

## 8. Deploy sequence — RUN, smoke-verify PASSED, season_config WRITTEN

Captain-approved, executed after 13:00 on the box clock.

### Deploy mechanism note (deviation, flagged before acting)

The specified `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`
was checked first (`launchctl print gui/501/com.trademinds.trader`) and
does not apply: **"Could not find service ... in domain for user gui: 501"**
— `com.trademinds.trader` is not bootstrapped in launchd on this box,
matching CLAUDE.md's own Reboot Lifecycle doctrine (trader is still on the
cron+nohup fallback, not a LaunchDaemon/LaunchAgent). CLAUDE.md's Git &
Deployment section documents the actual current mechanism:
`./scripts/trader_restart.sh` (orphan-prevention kill, mutex lock, WAL
checkpoint at the zero-reader window, single-writer gate that fails loud).
Used that instead of retrying the literal `launchctl` command against a
service that provably isn't there. Flagging the substitution plainly
rather than silently swapping commands.

### Restart

```
[2026-09-01 12:58:59] restart lock acquired (pid 56937)
[2026-09-01 12:59:00] killing trader instance(s): 84647
[2026-09-01 12:59:00] all trader instances dead (zero trader.log writers)
[2026-09-01 12:59:00] checkpointing WAL (zero-reader window)
[2026-09-01 12:59:00] starting trader: .venv/bin/python3 main.py
[2026-09-01 12:59:10] listener PID=56983 | trader.log writers=1
[2026-09-01 12:59:10] RESTART OK — single trader pid=56983 bound :8080 (orphan-free)
```

Old process (PID 84647, running since 2026-08-31 11:34 — pre-`0730aec`
bytecode) killed cleanly, new process (PID 56983) up in 10s, single-writer
gate passed, exit 0.

### Smoke-verify (CLAUDE.md Restart-then-verify doctrine — runtime, not just py_compile)

- **`trader.log`/`trader_error.log` since restart:** no tracebacks, no
  `NameError`/`AttributeError`/`ImportError` from any of today's changes.
  Startup banner clean: `"ALL SYSTEMS OPERATIONAL — ENGAGE"`. The only
  errors present are pre-existing/unrelated (Polygon options-chain 403s on
  the free tier, `datetime.utcnow()` deprecation warnings — neither
  touched by anything shipped today).
- **`engine/war_room.py`'s new `ADVISORY_CREW` import:** the fallback
  warning path (`"ADVISORY_CREW import failed"`) is silent in the fresh
  log — confirms the import succeeded cleanly, the tier-aware filter is
  live, not degraded to inert.
- **`engine/gamma_context.py`'s Polygon limiter wiring:** the fallback
  warning path (`"rate limiter unavailable"`) is also silent — confirms
  clean import, and mode is confirmed still `off` (see below) — inert as
  designed, not a live behavior change.
- **Dashboard reachable:** `GET /api/season` → `200`.
- **New shadow-report endpoint reachable and correctly inert:**
  `GET /api/polygon-limiter-shadow-report` → `{"status":"not_running", ...}`
  — confirms `POLYGON_LIMITER_MODE` is still unset/off on the live process,
  exactly as instructed (shadow stays held, separate from this deploy).
- **`0730aec` (BENCH fix) verified live against the new process, not just
  imported:** `engine.paper_trader._bench_block_reason('ollama-plutus')` →
  `None` (was `"BENCH: rating D (40/100)"` before the fix) — logged
  `[BENCH-STALE] ollama-plutus rating D (40/100) is 50d old (> 30d) —
  treating as expired, not blocking entries`. The fix is live, not just
  present on disk.

**Smoke-verify: PASS.** No stop condition hit.

### season_config season-7 row — WRITTEN

Approved. Populated with **verified values only** — nothing fabricated:

| column | value | source |
|---|---|---|
| `season` | 7 | — |
| `name` | `"Season 7"` | no real season-7 theme name exists anywhere in this repo (CLAUDE.md's own "Season 6.3 Config (current)" section still describes 6.3, predating the DB's actual rollover to 7) — used the same neutral fallback `dashboard/app.py` already falls back to (`f"Season {current}"`) rather than inventing one |
| `start_date` | `2026-07-12` | matches the real, already-present `settings.season_7_start` |
| `end_date` | `NULL` | season 7 is ongoing |
| `active_agents` | `qwen3-8b-flash,ollama-plutus,options-sosnoff,enterprise-computer,capitol-trades,trade-desk,desk-manual,m5-allocator` | live `ai_players` query, `halt_mode='active'`, taken today (09-01) — **not** the season's original 07-12 launch roster, which was never recorded and can't be reconstructed; this is a current snapshot, not a historical one, and will drift the same way season 6's did unless something keeps it current |
| `strategies`, `triple_filter` | `NULL` | no verified source for season-7-specific values exists; left blank rather than guessed |
| `alpha_gate` | `0.3` (schema default) | |
| `proving_ground` | `0` (schema default) | |

Also set the sibling gap in the same pass: `settings.season_7_name` was
also missing (only `season_7_start` existed) — set to `"Season 7"` to
match, since `dashboard/app.py`'s `/api/season` reads both keys
independently and the missing `_name` key was the other half of the same
display bug.

**Live-verified after the write:** `GET /api/season` now returns a fully
populated `config` object (was `{}`) — the Bridge's "Active: N agents"
line will now show the real count (8), not the misleading `1` that came
from `('—').split(',').length` on an empty object.

**Attribution note, as requested — this gap was found and flagged twice,
not once:** `relay_2026-07-18_full-audit-2026q3.md`'s `[AMBER] C7.2`
already reported "no Season 7 row exists in `season_config`" **on
2026-07-18**, six weeks before today's independent rediscovery. Both
times the finding was correct; between them, nobody wrote the row. That
audit tentatively linked the gap to a much larger active-agent-count
anomaly (75 vs. documented 15) which has separately resolved since (live
count today: 8 active) — whether the missing row actually caused that
incident was unconfirmed then and is still unconfirmed now; not
re-investigated as part of writing the row today.

**Not done:** extending `engine/season_manager.py::rotate_season()` /
`start_season()` to write both `season_config` and `settings.season_N_name`
automatically at every future rotation, so this doesn't require a third
discovery at season 8. Flagged as the structural fix; today's write is the
one-time catch-up, not that.

### 20:15 MST snapshot — PENDING, not yet due

It's 13:00 on the box clock as this section is written — tonight's
`db_snapshot.sh` cron (20:15) and `offhost_backup.sh` (20:30) haven't run
yet. This is the first real, unattended test of `337407c`'s `find`-based
retention counting, the free-space precheck, and `KEEP=14`. **Expected
correct result: no `[ARCHIVED]` lines** — `data/backups/` currently holds
zero dated `.db` files (confirmed empty earlier today), so after tonight's
run there will be exactly 1 (today's), and `1 > 14` is false. An
`[ARCHIVED]` line tonight would actually be the surprising/wrong outcome,
not the expected one. Checking `logs/db_snapshot.log` after 20:15 and
appending the result here is still owed — not done as of this section.

### Corrected baseline, restated plainly for tomorrow's comparison

Per section 7 above: the **712** (2026-08-31, complete day) /
**517** (2026-09-01, partial as of 18:29 UTC) `ollama_model_swap_log`
totals are the real, verified pre-filter baseline. The **"1,373"** figure
in this doc's section 5 commit message and section 2 was wrong — I
introduced it without a source and could not find it anywhere on
re-check. Worf/Troi correction, restated: they share the literal same
Ollama tag (`qwen3:8b`) and structurally cannot evict each other (same
name = cache hit); the pairing that's actually costing anything is Worf
(`qwen3:8b`) against `ollama-plutus` (`plutus-v1`) — bit-identical
weights, different tag names.

### What tomorrow's read should check (Captain's framing, recorded verbatim intent)

1. Does `ollama-plutus` actually fire a trade now that it's off BENCH?
2. Does `qwen3:8b` Ollama swap activity drop to near-zero, per the
   falsifiable prediction in section 7 — or does something this
   investigation missed keep it non-zero?
3. Is the first `20:15` retention run clean — zero `[ARCHIVED]` lines is
   the *correct* result tonight (count will be 1 against `KEEP=14`), not a
   failure to flag.

---

## 9. Polygon 429 characterization — REPORT ONLY, shadow mode confirmed still OFF

Nothing enabled, nothing restarted. `GET /api/polygon-limiter-shadow-report`
re-checked live: no report file on disk, `POLYGON_LIMITER_MODE` unset/off.
Verified via the same isolated method as section 5, not by touching the
live process's mode.

### How many, which endpoints, what times, burst or sustained

Pulled every dated line in `trader.log` from `2026-09-01 00:00:00` through
now (~13:00) and grouped by source, carrying the last-seen timestamp
forward across the many lines Rich's console renderer leaves blank when
several events land in the same second (confirmed real, not
double-counted — see the raw excerpt below).

| source | endpoint / code path | total today | pattern |
|---|---|---|---|
| `engine/market_data.py:893` (`get_intraday_candles`, `HM-CB` block) | `/v2/aggs/ticker/{sym}/range/{mult}/{span}/...` (candles) | **37,174** | **sustained all day** — every hour 00:00–13:00 has 558–4,701 events; even the quietest overnight hour (13:00, partial) had 558. Roughly doubles during 07:00–12:00 vs. overnight but never drops near zero. |
| `engine/bk_orb_scanner.py` (`_fetch_minutes_polygon`, direct) | `/v2/aggs/ticker/{sym}/range/1/minute/...` (1-min bars, 40d) | **2,471** | **pure burst, exactly 2 hours** — 978 at 07:00, 1,493 at 08:00, **zero at every other hour**. Matches its own `09:46–12:00 ET` self-gate precisely (that window is 06:46–09:00 on this box's fixed-MST clock). |
| `strategies/polygon_client.py` (`_get`, no rate-limit delay on `fetch_daily_bars`) | `/v2/aggs/ticker/{sym}/prev` and daily-bars endpoints | 53 | Scattered thin across the whole day (1–10/hour) — this client already has a partial `_rate_limited_get` helper with a `time.sleep`, which is very likely why its count is two orders of magnitude below the other two. |

**Total measured Polygon 429s today: ~39,700.** Overwhelmingly dominated
by one code path (`get_intraday_candles`'s Polygon leg) that isn't
bk_orb_scanner at all — it's a shared utility called from **15+ separate
modules** (`ollietrades_signal.py`, `benchmark.py`, `impulse_detector.py`,
`imbalance_detector.py`, `gap_scanner.py`, `volatility_breakout.py`,
`theta_scanner.py`, `chekov_autotrade.py`, `crew/ensemble.py`,
`dashboard/app.py`, and more), each independently walking its own ticker
universe through the same unmanaged, un-cached, un-coordinated function.
**This is sustained fan-out overload, not a single scanner's burst** — the
37,174-event figure is the correct headline number for "how bad is it,"
not bk_orb_scanner's slice of it.

Sample raw excerpt proving the same-second lines are real distinct events,
not a wrapping artifact (each names a different symbol):
```
[2026-09-01 00:09:15] HM-CB Polygon candles fallback to  Alpaca for GOOGL: RuntimeError('Polygon HTTP 429')
[2026-09-01 00:09:16] HM-CB Polygon candles fallback to  Alpaca for V: RuntimeError('Polygon HTTP 429')
                       HM-CB Polygon candles fallback to  Alpaca for ASML: RuntimeError('Polygon HTTP 429')
                       HM-CB Polygon candles fallback to  Alpaca for MA: RuntimeError('Polygon HTTP 429')
```

### Alpaca 429s — corrected count, and the fallback-doubling question answered

**Correction:** the "58 today" figure doesn't match what's in the log.
Direct count, same methodology, same day: **777** Alpaca 429s —
`get_alpaca_bars HTTP 429 for {sym}` (525), `_alpaca_bulk_bars_chunk HTTP
429` (236), `get_alpaca_bars batch HTTP 429` (16). Verified not a
duplication artifact the same way as the Polygon figure (raw sample
above the fold: each line names a distinct symbol). Not sure where "58"
came from — flagging rather than reconciling a source I can't find, same
posture as the "1,373" correction in section 7.

**Yes — the fallback path doubles requests per ticker, confirmed by
reading the code, not just inferring it from timing:**
`get_intraday_candles()` (`engine/market_data.py:808`) tries Polygon
first; on ANY failure (429 included) it falls straight through to a
second, full Alpaca request for the identical symbol/interval — no
memoization, no "Polygon is currently rate-limited, skip it for the next
N seconds" state of any kind. The very next ticker in the same caller's
loop tries Polygon again, fails again, doubles again. Every one of the
37,174 `HM-CB` events this section counted is, by construction, paired
with a second live request to Alpaca for the same symbol. The hours with
the heaviest `HM-CB` volume (07:00, 08:00) are also elevated for two of
the three Alpaca 429 patterns — consistent with the cascade (Polygon
overload pushes more traffic onto Alpaca, which then also saturates), not
proof of it on its own; the code-level mechanism is the solid part of
this claim, the timing correlation is supporting, not conclusive.

`bk_orb_scanner.py` specifically does **not** double onto Alpaca on a
Polygon 429 — its fallback to the shared `get_intraday_candles` cascade is
gated on `POLYGON_API_KEY` being *unset*, and it's set, so that branch is
dead in production. bk_orb's 2,471 failures each cost exactly one wasted
Polygon call, not two.

### bk_orb_scanner.py — theoretical rate vs. Polygon's real limit

`UNIVERSE_SIZE = 150` (env-overridable, default 150 — confirmed live
value, no override present). `run_scan()` loops all 150 sequentially,
**zero throttling, zero sleep between calls** — one `requests.get(...,
timeout=8)` per ticker, back to back. Scheduled `schedule.every(3)
.minutes.do(run_bk_orb_scan)`, self-gated to the `09:46–12:00 ET` window
(134 minutes → up to ~44 scan cycles/day) and to
`ORB_CONFIRMATORY_VOTE_ENABLED` — **checked and confirmed `True`
in `config.py`** (a hardcoded Python bool, not env-configurable despite
its name; `main.py`'s own inline comment calling it "default-OFF" is
stale — the scan is live today, which the 2,471-failure burst above
directly confirms).

**Answer: a single scan cycle is ~30x over Polygon's real free-tier
limit** — 150 tickers requested against a stated 5 calls/minute cap is a
150:5 ratio. That's the clean per-cycle multiple. It's not a one-time
30x either: with zero backoff between cycles, this repeats up to ~44
times across the window, each cycle re-attempting the same 150-ticker
burst against a quota that was already exhausted 5 requests in. Whether
the realized instantaneous rate is higher than 30x depends on how fast
the failing requests return (429 rejections are typically much faster
than a real 200 OK, so a 150-request loop plausibly completes its burst
in under a minute of wall-clock time, which would push the effective
peak rate well past 30x for that window) — not measured directly here,
flagged as the reason "30x" is a floor, not a ceiling.

### Shadow report — zero data, not "not enough," exactly as intended

`POLYGON_LIMITER_MODE` has never been set today (confirmed both by the
live `/api/polygon-limiter-shadow-report` endpoint returning
`not_running` and by the report file's absence on disk). **There is no
shadow data to read yet — zero, not partial.** This is the correct state
per the Captain's explicit instruction (shadow stays held tonight,
plutus off BENCH is already the one live variable for tomorrow). Nothing
to report on "what the limiter would have done" because it hasn't run at
all. Enabling shadow mode is a separate, later decision, not made here.

---

## 10. Ollama tag duplication — CONFIRMED HARD at the manifest/blob level. No tags touched.

Section 7's finding (same `model_digest` in `ollama_model_swap_log`) was
observed behavior from a poller. This section confirms it a completely
independent way — reading Ollama's own local manifest and blob store
directly, not inferring from `/api/ps` output.

### Same underlying layers, confirmed byte-for-byte

`ollama show plutus-v1:latest --modelfile` and `ollama show qwen3:8b
--modelfile` both resolve to:

```
FROM /Users/bigmac/.ollama/models/blobs/sha256-a3de86cd1c132c822487ededd47a324c50491393e6565cd14bafa40d0b8e686f
```

— the identical blob path, not just the identical digest string reported
by a poller. Went one level deeper: the raw manifest JSON on disk for
`library/plutus-v1/latest`, `library/qwen3/8b`, and `library/ministral-3/3b`
are **byte-identical**, every layer digest matching exactly (model,
template, license, params — all four):

```
{"schemaVersion":2, ... "layers":[
  {"mediaType":"...image.model","digest":"sha256:a3de86cd...","size":5225374496},
  {"mediaType":"...image.template","digest":"sha256:ae370d88...","size":1723},
  {"mediaType":"...image.license","digest":"sha256:d18a5cc7...","size":11338},
  {"mediaType":"...image.params","digest":"sha256:cff3f395...","size":120}
]}
```

Not "similar" or "same family" — the same four content-addressed blobs,
referenced by three different tag names. `ollama list`'s own `ID` column
(a separate, independently-derived identifier) agrees: `plutus-v1`,
`qwen3:8b`, `ministral-3:3b`, `qwen2.5-coder:7b`, `qwen3:4b` all show
`500a1f067a9f`, all `5.2 GB`.

### What created the tags: `ollama cp` (or equivalent local copy), not a pull or a Modelfile build

Filesystem timestamps settle this directly:

| file | mtime |
|---|---|
| the shared weights blob (`sha256-a3de86...`, 5.2 GB) | **2026-08-24 21:02:40** |
| `qwen3/8b` manifest | 2026-08-24 21:02:48 (8s after the blob — consistent with a real `ollama pull`: blob downloads, then the manifest is written) |
| `plutus-v1/latest` manifest | **2026-08-25 20:09:01** — almost 23 hours *later*, with **no corresponding change to the blob's mtime** |

A fresh `ollama pull plutus-v1` or a Modelfile `FROM <upstream>` build
would have written new blob data at the time of that pull/build — it
didn't; the blob is untouched since the original `qwen3:8b` pull. The
only thing that changed 23 hours later was a new manifest file pointing
at the *existing* blob set. That's the exact signature of `ollama cp
qwen3:8b plutus-v1` (or the local API equivalent) — matches the precedent
CLAUDE.md already documents for this exact family (`ollama cp qwen3:8b
qwen2.5-coder:7b`, done 2026-08-27 per that doctrine).

### Bare alias, confirmed twice — no system prompt, no distinct parameters, nothing to lose

Two independent checks, same answer:
- **Ollama-model level:** the manifest has exactly four layers — model,
  template, license, params. **No `system` layer** (Ollama's own
  mediaType for a baked-in system prompt, `application/vnd.ollama.image.
  system`, is simply absent). `PARAMETER` values in both rendered
  Modelfiles are identical (`temperature 0.6`, `top_k 20`, `top_p 0.95`,
  `repeat_penalty 1`, same stop tokens) — only the *display order* differs,
  not the values. `plutus-v1` carries zero Ollama-level customization
  over bare `qwen3:8b`.
- **Application level:** McCoy's persona (`"You are Dr. McCoy (Bones),
  Chief Medical Officer..."`) lives entirely in
  `engine/providers/base.py`, keyed by `player_id="ollama-plutus"` — sent
  as part of the prompt at inference time, from application code,
  **completely independent of which Ollama tag the request routes
  through.** Consolidating the tag `ollama-plutus` calls would not touch
  McCoy's persona at all; the persona was never coupled to the tag name in
  the first place.

**Nothing would be lost by deduping.** Both checks the Captain asked for
came back the same way: bare alias, not a distinct build.

### Not touched today — the test in flight would be destroyed

No tag copied, cp'd, or deleted. No `ai_players.model_id` changed. The
War Room filter deployed an hour before this investigation started
(section 8) — tomorrow's read on whether `qwen3:8b` swap activity drops
to near-zero (section 7's falsifiable prediction) requires the current
tag layout to stay exactly as it is until that read happens. Touching
`plutus-v1` or `qwen3:8b` today would conflate two independent variables
in the same measurement window.

### Conditional dedupe plan — write-up only, not executed, contingent on tomorrow's read

**If tomorrow's `qwen3:8b` swap count does NOT drop toward zero**
(falsifying the War Room theory, or revealing a second live caller this
investigation missed), the next candidate fix is consolidating the tag
layout so `ollama-plutus` and any other alias-family agent call the
*same* tag name Ollama already has resident, instead of a differently-
named tag pointing at identical bytes.

**The fix:** change `ai_players.model_id` for `ollama-plutus` from
`plutus-v1` to `qwen3:8b` (a single-row `UPDATE`, no `ollama rm`/`ollama
cp` needed — the alias tags themselves can stay on disk, unused, rather
than being deleted). Confirmed above this changes nothing behaviorally:
same weights, same template, same params, no system-prompt coupling.
Ollama would then see repeated requests for the literal same tag from
both McCoy and Worf's calls and serve them without an evict/reload cycle
between them, the way `qwen3-8b-flash` and `options-sosnoff` already do
today (same tag, confirmed no mutual eviction, section 7).

**What it risks:**
- **Attribution, not behavior.** `ai_players.model_id` is read in places
  beyond inference routing — anywhere that logs, reports, or filters by
  model name (e.g. `engine/agent_routing.py`'s routing table, any
  dashboard panel that groups/labels agents by `model_id`, the
  `_QWEN3_ALIAS_MODEL_IDS` thinking-mode-suppression set in
  `engine/providers/ollama_provider.py` which already has to know about
  this alias family) would need to keep working with McCoy now reporting
  as `qwen3:8b` instead of `plutus-v1` — not confirmed broken, not audited
  here, a real "check every consumer of `ai_players.model_id`" pass would
  be needed before flipping it, same rigor as the `season_config` consumer
  sweep in section 6.
- **Losing the per-agent VRAM-thrash signal.** Right now, distinct tag
  names are incidentally useful as a debugging aid — `ollama_model_swap_
  log.model_name` currently tells you *which alias* (hence which agent)
  drove a given load/evict event, the exact thing this investigation used
  to attribute today's swap activity to Worf specifically. Consolidating
  the tag removes that signal — a future thrash investigation would only
  see "qwen3:8b" swapping, with no free way to tell which agent's calls
  caused it, unless something else (a per-call log) is added first.
- **Re-alias risk if the roster changes.** `HM-SEAT-CONSOLIDATION`
  (2026-08-31) already touched this family once; a second consolidation
  pass on top of a still-recent one raises the chance of losing track of
  which agent maps to which tag, especially if it's done piecemeal rather
  than as one deliberate, documented pass across the whole alias family
  (`plutus-v1`, `ministral-3:3b`, `qwen2.5-coder:7b`, `qwen3:4b` all share
  this same underlying blob, not just `plutus-v1` — a real consolidation
  decision should look at all four at once, not just McCoy's).

**Recommendation if the condition triggers:** don't do this as a quick
single-row patch. Scope it as its own small ticket — audit every
`ai_players.model_id` consumer first, decide whether the swap-log
attribution signal needs replacing before it's removed, and decide
whether to consolidate the whole alias family in one pass rather than
one row at a time. Not needed unless tomorrow's read falsifies the
current prediction.

---

## 11. Items 1 & 2 — CODE DONE, COMMITTED, NOT DEPLOYED. Items 3 & 4 wait for tomorrow.

Per Captain directive: two safe-now code fixes, no restart, no enabling.
Both go live at whatever the next real trader restart is — not triggered
here.

### Item 1 — 429 backoff in `get_intraday_candles` (`engine/market_data.py`)

Added a module-level cooldown, mirroring the existing (already-proven)
Yahoo pattern in the same file (`_is_yahoo_limited`/`_set_yahoo_limited`,
`_COOLDOWN_SECONDS = 60`) rather than inventing a new idiom:
`_is_polygon_limited()` / `_set_polygon_limited()`,
`_POLYGON_COOLDOWN_SECONDS = 60`.

**A 429 specifically sets the cooldown — nothing else does.** The status
check now branches: `429` → `_set_polygon_limited()` then raise (falls
through to Alpaca, same as any failure); any other non-200 → raises
exactly as before, cooldown untouched. A generic failure (timeout,
malformed JSON, 0 bars) is a one-off; only a 429 means "the API said slow
down," and only a 429 should penalize every other caller.

**Concurrency:** plain module globals, no lock — the same soft-advisory
pattern the Yahoo cooldown already uses in this exact file. With 15+
uncoordinated callers, a missed race (two threads both see "not limited"
in the same instant) is an acceptable imperfection under the GIL, not a
correctness bug — the goal is collapsing "every caller keeps hammering a
429'ing endpoint" into "one caller discovers it, the rest skip for 60s,"
not a hard mutex.

**Doesn't swallow exhaustion:** the final fallback (`return []` after
Polygon → Alpaca → Yahoo all fail) is unchanged — this cascade has never
cached or served stale data on total failure, only ever an honest empty
list. The fix only removes the *wasted* Polygon attempt once it's already
known to be doomed; the caller still gets a full, real attempt at Alpaca
(and Yahoo) either way, same as before.

**What this does NOT do:** reduce Alpaca's own request volume. Alpaca is
still tried exactly as often as before — this fix removes wasted Polygon
calls, it doesn't change how many times Alpaca gets asked. Flagging so
tomorrow's Alpaca 429 count isn't read as "this fix didn't work" if it
doesn't drop — that was never the mechanism.

Tests: `tests/test_polygon_429_backoff.py`, 9 new cases (429 sets
cooldown, non-429 does not, second call during cooldown skips Polygon
entirely — the actual fix, being verified directly via a call-count
assertion — cooldown expires and Polygon is retried, happy path
unchanged, and the skip-path still falls through to Alpaca/Yahoo rather
than returning early). Caught and fixed a real problem while writing
these: the first draft made real Yahoo network calls (18s per run) since
`_yahoo_chart` wasn't mocked — added an autouse fixture blocking it;
reran at 0.81s. All 9 pass. Existing `test_polygon_candle_freshness.py`
(2 tests) still passes unchanged.

### Item 2 — rewired the limiter from `gamma_context.py` to `get_intraday_candles`

**Reverted `engine/gamma_context.py`** to a plain direct
`_polygon_snapshot(ticker)` call — removed the `gated_call`/
`BudgetExhausted` wiring from `bda14e5` entirely. Deleted
`tests/test_gamma_context_rate_limiter_wiring.py` (its subject no longer
exists there, not left behind as a stale/misleading suite).

**Wired `engine/market_data.py::get_intraday_candles`'s Polygon leg
instead** — the real chokepoint. Refactored the existing fetch-and-parse
logic into a local closure (`_do_polygon_fetch`, no behavior change, same
code, same 429-detection from item 1) and route it through
`engine.polygon_rate_limiter.gated_call("get_intraday_candles",
f"candles:{symbol}:{interval}:{range_}", _do_polygon_fetch)`. Added
`"get_intraday_candles"` to `polygon_rate_limiter.LIVE_CALLERS` — this
collapses all 15+ real callers into one shared caller identity for
budget/shadow-accounting purposes (the function doesn't currently know
which of its 15+ callers is asking, and threading that through every call
site would be a much bigger change than "move the limiter" — this matches
"one shared budget," not per-caller attribution). Updated the module's
own docstring/usage example in `polygon_rate_limiter.py` to match (it
still said "not done yet" in its usage example).

**Same off-by-default guarantee as the first (reverted) wiring:**
`POLYGON_LIMITER_MODE` unset → `gated_call()` is `return fetch_fn()`,
byte-for-byte identical to calling `_do_polygon_fetch()` directly.
Deploying this rewiring alone changes zero live behavior — verified by
test, not just asserted (`test_off_mode_passthrough_unchanged`: exactly
one fetch call, shadow report `total=0`).

**The cooldown (item 1) and the limiter (item 2) compose correctly, tested
directly:** a call already blocked by `_is_polygon_limited()` never even
reaches `gated_call()` — `test_cooldown_check_runs_before_offering_call_to_limiter`
confirms the shadow report sees `total=0` when the cooldown is active,
proving no double-instrumentation and no wasted limiter bookkeeping on a
call that was never going to be attempted anyway.

**Important interaction to know about before reading a future shadow
report:** once both items are eventually live, item 1's cooldown will
suppress most of the storm *before* item 2's shadow accounting ever sees
it. A future shadow report will reflect Polygon demand *after* the
cooldown fix, not the raw pre-fix 37,174/day figure — that's the two
fixes working together correctly, not the limiter under-reporting the
original problem. Noting this now so a smaller-than-expected future
shadow number doesn't read as "the limiter can't represent overshoot
this large."

**Can the shadow counters actually represent a 30x-scale overshoot?**
Yes, mechanically — `total`/`would_throttle`/`would_fail_loud`/
`would_serve_stale` are plain unbounded Python integers incremented once
per `gated_call()`, no cap, no overflow risk at this scale, confirmed
correct at small scale by test (`test_shadow_mode_observes_calls_through_get_intraday_candles`).
**But there's a coverage gap, not a counting gap:** `bk_orb_scanner.py`
does **not** call `get_intraday_candles` in production — it has its own
direct `_fetch_minutes_polygon`, and only falls back to the shared
cascade when `POLYGON_API_KEY` is unset, which it isn't (dead code
today). Its 150-tickers-per-cycle, 2,471-429s-today burst is therefore
**invisible to this wiring** — this rewire fixes the biggest single
source (the 37,174-event `get_intraday_candles` fan-out) but does not
capture bk_orb_scanner's separate, also-real burst. A shadow report from
this wiring alone would still under-represent the full daily picture by
that amount. Flagged, not fixed here — a second, separate wiring of
`bk_orb_scanner.py`'s own direct Polygon call would be needed to close
that gap; out of scope for "rewire the existing wiring."

Tests: `tests/test_polygon_limiter_rewire.py`, 5 new cases (off-mode
passthrough exactly unchanged, `BudgetExhausted` caught and degrades to
the Alpaca fallback, shadow mode actually observes calls through this
function now — the literal point of the rewire, `get_intraday_candles`
confirmed present in `LIVE_CALLERS`, cooldown-skip never reaches the
limiter). All 5 pass. Full regression sweep across every touched area
(`market_data`, `polygon`, `intraday`, `rate_limiter`, `gamma_context`,
`candle`, `vwap`): **63 passed, 0 failed.**

### Not done today

- Not deployed. Nothing restarted, `POLYGON_LIMITER_MODE` never set —
  confirmed unset throughout every test and every manual check this
  session. Both items go live at the next real trader restart, whenever
  that's decided — not triggered by this commit.
- bk_orb_scanner's own direct Polygon path is not wired into the limiter
  (see coverage-gap note above) — a real gap in what tomorrow's shadow
  report, once eventually turned on, would be able to show.
- Item 3 (tomorrow's three checks) and item 4 (conditional Ollama dedupe)
  are unchanged from sections 7/10 above — nothing here supersedes them.

---

## 12. Polygon limiter shadow mode — ENABLED LIVE, verified real data accumulating

Captain-approved, executed after confirming shadow mode's exact behavior
and restart safety first, per instruction.

### What shadow mode actually does — confirmed from the live code before flipping it

Read `engine/tiered_rate_limiter.py::gated_call()`'s SHADOW branch
directly, not from memory:

```python
if mode == LimiterMode.SHADOW:
    if not got_token:
        self._shadow_stats["would_throttle"] += 1
        ...
    self._save_shadow_report()
    result = fetch_fn()          # <-- ALWAYS runs, unconditionally
    if result is not None:
        self._cache[cache_key] = {"ts": time.time(), "data": result}
        self._save_cache()
    return result
```

`fetch_fn()` executes on **every single call**, regardless of whether a
token was available (`got_token`) — there is no `if got_token: ... else:
skip/wait` branch gating the real request. `_try_acquire()` (the token
check) only mutates in-memory counters under a `threading.Lock` — no
`time.sleep`, no queue, no async scheduling, no batching. Calls happen
synchronously in the exact order callers invoke them — nothing reorders
them. **Confirmed: shadow mode cannot delay, drop, or reorder a request.**
The only side effects are (1) incrementing in-memory counters and (2) a
disk write of the aggregate report — never touches the request itself.
Did not need to stop and flag anything here; the code matches the design
doc's claim exactly.

### Restart requirement and safety — checked before restarting

**Needs a restart, confirmed:** `TieredRateLimiter.mode` reads
`os.environ.get(self.mode_env_var, "off")` fresh on every access, but
`os.environ` is a per-process snapshot — `.env` is only loaded into it
via `config.py`'s `load_dotenv(override=True)` at process import time.
Editing the `.env` file on disk does not touch the already-running
process's environment at all; only a fresh process picks up the new
value. Verified this distinction concretely: a standalone check that
didn't import `config` first (and thus never triggered `load_dotenv`)
incorrectly showed `LimiterMode.OFF` even after the `.env` edit and the
restart — a methodology bug on my end, not a real problem (caught by
cross-checking against the live process's own HTTP endpoint, which is the
actually-authoritative source and showed `shadow` correctly all along).

**Restart safety, same checks as the 13:00 restart:** `is_market_hours()`
returned `post_market` (confirmed live, not assumed). No stale restart
lock at `/tmp/uss_trader_restart.lock`. `lsof` on `trader.log` showed
exactly one writer (the running PID) before restarting — clean
single-writer state, same gate `scripts/trader_restart.sh` itself
enforces after relaunch. ~16:41 local, nowhere near the 20:15/20:30
snapshot/offhost cron windows. Safe to proceed.

### `.env` access — blocked, Captain made the edit

Could not read or write `.env` myself — hard tool-permission denial, not
worked around. Flagged the tradeoff explicitly rather than silently
substituting a transient shell-export-before-restart (which would have
worked for tonight only, then silently reverted to `off` on any *other*
future restart — crash, watchdog, tomorrow's routine one — with no
record anywhere that it had reverted; a real risk to a full-day shadow
measurement, not just a cosmetic difference from "the .env way"). Captain
added `POLYGON_LIMITER_MODE=shadow` directly (line 169, backup at
`backups/.env.bak.*`).

### Restart executed

```
[2026-09-01 16:44:44] restart lock acquired (pid 72144)
[2026-09-01 16:44:44] killing trader instance(s): 56983
[2026-09-01 16:44:44] all trader instances dead (zero trader.log writers)
[2026-09-01 16:44:44] checkpointing WAL (zero-reader window)
[2026-09-01 16:44:45] starting trader: .venv/bin/python3 main.py
[2026-09-01 16:44:55] listener PID=72190 | trader.log writers=1
[2026-09-01 16:44:55] RESTART OK — single trader pid=72190 bound :8080 (orphan-free)
```

`scripts/trader_restart.sh`, not `launchctl kickstart` — same reason as
the 13:00 restart, `com.trademinds.trader` isn't bootstrapped in launchd
on this box.

### Smoke-verify: PASS

- No tracebacks/`NameError`/`AttributeError`/`ImportError`/`TypeError`
  anywhere in `trader.log` from the restart's `ALL SYSTEMS OPERATIONAL`
  banner onward.
- Process stable, single PID (72190), single `trader.log` writer.
- Mode confirmed `shadow` (not `off`, not `enforce`) three independent
  ways: a corrected local check (loading `config` first, so
  `load_dotenv` actually runs), the raw shadow-report file on disk, and
  the live `/api/polygon-limiter-shadow-report` HTTP endpoint via a
  proper Python client (curl's own terminal rendering garbled the JSON
  in a way that looked like a formatting anomaly on first glance — a
  display quirk, re-verified against the raw file and a real HTTP client
  before trusting it, not a real data problem).

### Real data accumulating — the actual point of tonight's check

Not an empty scaffold. Raw file
(`data/polygon_limiter_cache_shadow_report.json`), read directly:

```json
{
  "total": 8,
  "would_throttle": 4,
  "would_fail_loud": 0,
  "would_serve_stale": 4,
  "by_caller_fail_loud": {},
  "mode": "shadow",
  "name": "polygon",
  "process_started_at": "2026-09-01T23:45:16.299444+00:00",
  "updated_at": "2026-09-01T23:45:47.627455+00:00"
}
```

Watched `total` go `0 → 1 → 8` across the first ~30 seconds post-restart,
`updated_at` advancing past `process_started_at` — real, live accrual,
not a static write-once file. `would_fail_loud=0` is expected right now
(post-market, `is_market_hours()` gates that counter to market hours
only) — the meaningful counters post-market are `would_throttle` and
`would_serve_stale`, both already non-zero. Endpoint independently
confirms the same numbers via a real HTTP round-trip, not just a file
read.

### The undercount caveat — repeating it here where the numbers actually are, not just where it was first found

**`bk_orb_scanner.py`'s own direct Polygon path
(`_fetch_minutes_polygon`) bypasses this wiring entirely** — it does not
call `get_intraday_candles`, so its 150-tickers-per-cycle burst (2,471
429s on 2026-09-01, confirmed in section 9) will not appear anywhere in
this shadow report. **Whoever reads tomorrow's full-day shadow numbers
needs to read them as a floor on total Polygon demand, not the total** —
the real number, if bk_orb_scanner's burst were included, would be
higher than whatever this report shows. Also matters for the interaction
noted in section 11: the `HM-429-BACKOFF` cooldown (same commit) will
suppress most of `get_intraday_candles`'s own repeat-storm behavior
*before* the shadow counters ever see it — so tomorrow's shadow numbers
undercount from *two* independent directions (bk_orb_scanner's coverage
gap, and the cooldown fix's own suppression), not one. Both are expected
and explained, not a sign the limiter is broken.

### Enforcement — confirmed NOT enabled

`.env`'s value is `shadow`, verified live via the running process three
ways above. `enforce` was never set. No `BudgetExhausted` can be raised
in shadow mode (confirmed from the code read above — that branch is only
reachable under `LimiterMode.ENFORCE`). Nothing today changes real
request behavior for any of the 15+ callers.

### Failure contingency (not triggered — smoke-verify passed)

Had smoke-verify failed, the plan was: ask the Captain to revert the
`.env` line (same access constraint as setting it — cannot do this
myself), restart via the same script, confirm the revert took, and stop
there without further action. Not needed tonight.

### Tomorrow's reads — now four, all in one place

1. Did `ollama-plutus` actually fire a trade — first discretionary fire
   since 2026-08-27, confirms the BENCH-deadlock diagnosis (section 2 of
   `relay_2026-09-01_bench-stale-rating-deadlock.md`).
2. Did `qwen3:8b` Ollama swap activity drop toward zero against the
   corrected 712 (08-31) / 517 (09-01 partial) baseline (section 7),
   confirming the War Room tier-aware filter's falsifiable prediction.
3. Was the 20:15 MST `db_snapshot.sh` run clean — **zero `[ARCHIVED]`
   lines is the correct result** (count will be 1 vs. `KEEP=14`), not a
   miss. First real test of `337407c`'s `find`-based retention counting.
4. **New:** what does a full day of shadow data say the limiter would
   have done against today's 37,174 Polygon / 777 Alpaca 429s — read
   `would_throttle` / `would_fail_loud` / `would_serve_stale` from
   `GET /api/polygon-limiter-shadow-report` (or the raw file) once the
   day's session has closed, remembering the undercount caveat above
   before drawing conclusions from the absolute numbers.

Item 4 (Ollama tag dedupe, section 10) stays conditional on read #2 above
— not touched, no change either way until that read comes back.
