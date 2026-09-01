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
