# Relay — 2026-09-13. Bridge Classic repair, STEPs 0–6 (Admiral directive).

RULE #1 held: no trade, signal, decision, rating, or prompt row deleted or
rewritten. Every change is display logic, payload fields, tests, or backups.

## STEP 0 — backups (before any change)
- `sqlite3 .backup` (online API; a `cp` of a live WAL DB can miss unmerged pages).
- `data/signals.db` is a 0-byte placeholder; the real one is `signal-center/signals.db`.
- Source + copy `PRAGMA integrity_check` = ok for both:
  `data/backups/trader_2026-09-13_0840_pre-bridge-repair.db` (1.34 GB),
  `data/backups/signals_2026-09-13_0840_pre-bridge-repair.db` (194 MB).
- Rsynced to the X9 with the offhost script's mount guard; they land in
  `OLLIETRADES_BACKUPS/backups/` (top-level grep is empty by design); X9 copies
  integrity_check = ok.

## Item 10 trace (answered before changing the number)
The gate from 3ab0b14 (MIN_N=5) was wired into `bridge-v2.html` only.
`/classic` (`index.html:6954`) had no gate and rendered `s.dsr` whenever non-null,
and `/api/shadow-csp/standings` served `dsr=0.9876` at `n_closed=3` — so 0.99
rendered under either 5 or 30. Fixed to 30 from the payload's `graduate_n` on the
server and both tiers (`9c9aa93`).

## STEP 3 — staleness contract (`1ec1a82`)
`engine/bridge_staleness.stamp()` → `as_of/source/age_hours/stale`; all six GEX
routes stamped; `battle-station-0dte/status` (Ready Room briefing, UTC) and
`gamma-environment` (legacy gex_scanner, local) given an `as_of`. One render rule in
both tiers (`otStaleApply`/`otStaleText`): grey + "STALE · as of … · age · source",
never hidden. Not yet routed: sector heatmap, metals, Reveille, Riker.

## STEP 4 — `tests/test_bridge_consistency.py` (`5101c87`), in pre-commit under `.venv`
9 pass + 2 strict xfails that document still-open items (turn red when fixed):
item 1 (Riker regime source) and item 2 (backend leaderboard sorts `total_value`).

## STEP 5 — restart + live curl
`trader_restart.sh`: PID 53839 → 86359 at 08:58:08 MST, single writer, orphan-free,
no new `trader_error.log` lines. There is no separate bridge process — the Bridge is
served by `main.py` on :8080.

## STEP 6 — table
| # | Result | Commit | Data or display |
|---|---|---|---|
| 1 | UNVERIFIED | — | Data-input wiring: Riker prompt reads `regime_detector` (50/200MA), `/api/regime` = `regime_history` BEAR_CROSS. Recommendation empty after restart (in-memory), so live stance can't be compared yet. Open (strict xfail). |
| 2 | UNVERIFIED | 3ab0b14 | Display: ranks client-side (node test passes). Backend sorts `total_value` (open, xfail); payload happens to be return-ordered today. |
| 2b | UNVERIFIED | — | systems-status 8+74 = fleet/status 82 (curl agrees); on-screen badges not checked. |
| 3 | UNVERIFIED | 1ec1a82 | Display: payload contract PASS on all 6 routes (4 canonical 53.8d stale:true, 0DTE 47.9h stale:true, gamma env fresh). Grey render not browser-checked. Defect found: gamma-env `as_of` text shown 7h off by the TZ display layer (local-naive treated as UTC); age/stale correct. |
| 3b | UNVERIFIED | 3ab0b14 | Display: trades_today=0, history 06-08/04-27; TODAY label client-side. |
| 4 | UNVERIFIED | 3ab0b14 | Display/fallback: 12 sectors, not all zero; not yet stamped. |
| 5 | FAIL | 3ab0b14 | Display: payloads derived (Season 8); literal grep still hits 2 historical "benched Season 5" tooltips. |
| 6 | UNVERIFIED | 3ab0b14 | Display: 2 positions in payload; three-view agreement render-side. |
| 7 | N/A | — | Withdrawn (false premise). |
| 8 | UNVERIFIED | 3ab0b14 | Data (writer): writer test passes; today's live body too thin to exercise it. |
| 9 | UNVERIFIED | 3ab0b14 | Display: 316 tickers / 301 empty in payload; filter client-side (node test passes). |
| 10 | PASS | 9c9aa93 | Display: live payload withholds qwen3.5-shadow DSR (`N=3 < 30`); no below-gate value served. |
| 11 | FAIL | d9771d7 | Data timing race fixed + poll registered live; can't fire until Mon 06:12 AZ; "panel says WHY" not built. |
| 12 | N/A | — | Backlogged with un-alias work. |
| Riker cadence | PASS (closed side) | 2f8aab9 | First post-restart tick 09:08:09: `[SCHED-JOB] start/done _run_riker_xo_synthesis`, 0 "Synthesis generated" lines, recommendation null. In-hours firing unobservable until Mon. |
| Phaser poll | UNVERIFIED | d9771d7 | `[SCHED-JOB] run_phaser_lock_morning` firing every 2 min post-restart (returns: not 06:xx). |

## Reserved to the Admiral (not decided)
CBOE GEX wall-label question (needs a fresh same-moment CBOE vs Polygon read, no repoint).
