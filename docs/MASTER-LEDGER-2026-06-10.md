# HM-LEDGER — Master Open/Closed Sweep (2026-06-10)

**READ-ONLY evidence sweep. Zero changes made.** Status per item: `OPEN` (not
done / absent), `CLOSED` (done / present), `PARTIAL` (scaffolded but incomplete
or built-but-unwired). Evidence is `file:line` / commit / `ABSENT`, verified live
this date. Method: targeted grep/read + 3 parallel read-only investigators + live
SSH to `.168` + the authoritative `crontab -l`.

| # | Item | Status | Evidence |
|---|------|--------|----------|
| 1 | Scotty parallel Fable audit ("list 10 top recommendations", this AM) | **OPEN** | **ABSENT** — no doc in `docs/` or `drafts/` dated 2026-06-10 mentioning Fable / Scotty audit / "10 recommendations". Only HM-FORGE Phase specs carry that date. |
| 2 | Hard-coded `PIN=2026` still in code | **OPEN** | `signal-center/server.py:227` → `PIN = "2026"`. Still hard-coded. ⚠️ security. |
| 3 | `.env.bak` stale credential values present | **OPEN** | `.env.bak` exists (128 lines); **24 keys differ in value** vs `.env` → stale secrets retained on disk. ⚠️ (values not printed; archive/scrub recommended — separate change). |
| 4 | Bare-except / silent-error count vs May audit | **PARTIAL** | Repo-wide today: **6** bare `except:`; **3,220** broad `except Exception`; many silent swallows. May (`docs/SCOTTY_AUDIT_2.md:544`) found server.py had 0 bare `except:` / 53 named — "directionally right, syntax claim wrong". Posture is going-forward (CLAUDE.md HM-Z/HM-AA); 6 bare excepts remain repo-wide. |
| 5 | Off-host backup for trader/arena/tractor.db beyond git | **PARTIAL** | Script EXISTS: `scripts/offhost_backup.sh` (rsync-only, no `--delete`, no source mutation → `.168`; covers trader.db/signals.db/tractor.db + daily-backups, lines 57-69). **But NOT in `crontab -l`** and no backup LaunchAgent → built, **not scheduled**. |
| 6 | Port 9000 signal-center — retire test run? still load-bearing? | **OPEN** (retire never run; **still load-bearing**) | 49 refs; ≥10 live callers incl. `engine/{ai_brain,crew_scanner,premarket_scanner,signal_bridge,signal_poster,danelfin_parser,sell_the_news_scanner,long_range_sensors,ollie_machine_p2a}.py` + `engine/fleet_status.py` (new). No retire test on record. Cannot retire — heavily depended-on. |
| 7a | Volume Radar 10x alerts | **CLOSED** | `engine/volume_scanner.py:41` → `REL_VOL_TRIGGER = 10.0`; full `scan_full_market()` / `red_alert_check()`. |
| 7b | GEX threshold triggers | **PARTIAL** | `engine/orcl_gex_alerts.py:34-42` (ORCL-specific level alerts only). No generic per-ticker GEX-threshold alerter; `canonical_gex.py` has no alert logic. |
| 7c | F&G + VIX recovery protocol | **OPEN** | `engine/fear_greed.py` computes F&G (`_compute_fear_greed()` :51) + VIX (`get_vix()` :71) **separately**; no combined recovery/re-entry protocol. |
| 7d | Congress + insider overlap surfacing | **CLOSED** | `engine/congress_tracker.py:54` → `get_congress_overlap(...)`; insider via `engine/insider_tracker.py`; surfaced in `morning_briefing.py:286` + dashboard. |
| 7e | Bridge OHLCV de-yfinance (→ Polygon/Alpaca) | **PARTIAL** | Core path de-yfinanced: `engine/market_data.py:158` `_is_yf_limited()→True` (yfinance stubbed) with 5-layer Alpaca→Yahoo→Finnhub→AV→cache chain (`:585-638`). But `signal_bridge.py` has no Polygon/Alpaca OHLCV replacement and yfinance still imported in `volume_scanner.py`/`fear_greed.py`. |
| 7f | Signal Center watchlist sync | **OPEN** | Read-only consumption of `signals.db` exists (`dashboard/app.py`); no bidirectional watchlist sync (signal-center ⇄ trader) found. |
| 8 | Trip-hardening phases 0-5 (= "DAEMON GRAVEYARD — Reboot Survival Gap", `docs/DAEMON-GRAVEYARD-REHOME-PLAN-2026-05-30.md`) | **PARTIAL** ⚠️ | See 8a-8g. **Headline discrepancy:** `XO_BACKLOG.md` claims "ARC COMPLETE / 12 cron-managed daemons restored", but the authoritative `crontab -l` shows the watchdog/healthcheck/sitrep/uhura/real-portfolio/fleet-auditor cron entries were **never installed** (plists retired, some re-homed in-process, several neither). Claims-vs-disk gap. |
| 8a | Ollie Max systemd `Restart=always` (.168) | **CLOSED** | **Live SSH verified:** `systemctl show ollama` → `ActiveState=active`, `Restart=always`. (Resolves prior "cannot verify from bigmac".) |
| 8b | Trader restart hardening (flock/orphan-proof) | **CLOSED** | `scripts/trader_restart.sh:1-105` (atomic mutex, SIGTERM→SIGKILL, single-writer gate), commit `0f1c2cf`. |
| 8c | Watchdog (`*/5` + `@reboot` cron supervisor) | **PARTIAL** | `scripts/watchdog_supervisor.sh` + `watchdog.py::restart_trader()` ready; **cron entry MISSING** in `crontab -l`. Plist retired. |
| 8d | Healthcheck cron (`0 6-13`) | **OPEN** | Plist retired; `healthcheck.py` exists; cron MISSING; XO_BACKLOG marks "DEFERRED/OFF until restart_server()→trader_restart.sh". |
| 8e | Tailscale mesh | **PARTIAL** | Referenced as access method (`directives/hm_morpheus_execution.md`, `reports/SESSION_HANDOFF_2026-05-17.md`); not a dedicated hardening config. |
| 8f | pmset power recovery (autorestart/disablesleep) | **OPEN** | Reference only in `docs/SCOTTY_INFRA_AUDIT.md` checklist; no script/plist sets pmset. |
| 8g | Kasa smart-plug power-cycle integration | **OPEN** | **ABSENT** — zero `kasa`/`smartplug` references in codebase. |
| 9a | Alpaca/yfinance fallback health | **CLOSED** | `engine/market_data.py:585-638` — 5-layer chain (Alpaca→Yahoo→Finnhub→AlphaVantage→DB cache), intact; yfinance limited/stubbed at `:158`. |
| 9b | Danelfin stub presence | **CLOSED** (real, not stub) | `engine/danelfin_parser.py` + `engine/danelfin_weekly_cron.py` — reads scores, posts via `/api/signal`. Live integration. |
| 9c | SUPER_MAX W2 gate (bracket sizing) | **OPEN** (gated) | `drafts/SPEC_W2_BRACKET_SIZING.md:2` — "design draft 2026-06-01. No build. Gated AFTER graduation." Spec-only, observation-first. |
| 9d | SUPER_MAX W3 gate (gamma mapper / unusual-OI) | **OPEN** (gated) | `drafts/SPEC_W3_GAMMA_STRATEGY_MAPPER.md:3` — "design draft 2026-06-01. No build." Observation-only; refused at executor chokepoint. |

## 10. Other OPEN/PARTIAL items found in docs/ + drafts/ (not in the list above)

| Item | Status | Evidence |
|------|--------|----------|
| HM-PLUTUS-V6 corpus build | OPEN | `docs/XO_BACKLOG.md:100-104` |
| HM-BM-BAKEOFF (4-candidate) | OPEN (gated on v6) | `docs/XO_BACKLOG.md:93-98` |
| HM-RISK-MANAGER-CONVICTION-STOP | OPEN | `docs/XO_BACKLOG.md:249` (~57% NULL backfill pending) |
| HM-SCHWAB-CROSS-MECHANISM-ALARM | OPEN | `docs/XO_BACKLOG.md:249` (shared cron fate) |
| HM-TRADES-MIRROR-GAP (re-measure) | OPEN | `docs/XO_BACKLOG.md:249` |
| HM-CONVICTION-TIER-BOUNDARY | OPEN (Admiral-gated) | `docs/XO_BACKLOG.md:249` |
| HM-CAPITAL-LADDER | OPEN | `drafts/THE-ALL-OUT-PLAN-2026-05-28.md:211` (gated on IC Squadron) |
| HM-TIER-5-MEAN-REVERSION | OPEN | `drafts/THE-ALL-OUT-PLAN-2026-05-28.md:212` |
| HM-AS-β SCHEDULER cadence drift | OPEN | `drafts/HM-AS-BETA-SCHEDULER-TOP-PRIORITY.md` (root: run_autopilot holds scan lock) |
| HM-FLOW-NATIVE (unusual-options) | OPEN (P2) | `docs/XO_BACKLOG.md:151-155` (Polygon-tier-blocked) |
| HM-GEX (dealer gamma OI+greeks) | OPEN (P2) | `docs/XO_BACKLOG.md:156-157` |
| HM-CONSENSUS-WEIGHTING | OPEN (P3) | `docs/XO_BACKLOG.md:165-168` |
| HM-LESSON-GRADUATE (shadow→live) | OPEN (P3) | `docs/XO_BACKLOG.md:170-172` |
| HM-AO-β-2 squeeze-watcher frontend | PARTIAL | `docs/SCOTTY_PHASE_5_STATUS.md:23` (backend shipped, FE deferred) |
| HM-TZ-COMPLETION engine sweep | PARTIAL | `drafts/HM-TZ-COMPLETION-ENGINE-SWEEP.md` (naive-local writers staged) |
| HM-SIGNALS-V2-STALE-SWEEP | OPEN | `docs/CLOSET-SWEEP-2026-05-30.md:42` (3,076 pending) |
| HM-HOLLY-ENTRY-FIDELITY | OPEN (deferred) | `docs/XO_BACKLOG.md:83-86` |
| HM-INLINE-STYLE-SWEEP | OPEN (backlog-low) | per memory; ~6,169 inline styles, cosmetic |

## Flagged anomalies (action candidates — not acted on here)
1. **`PIN="2026"` hard-coded** at `signal-center/server.py:227` — auth secret in source.
2. **`.env.bak` retains 24 differing credential values** — stale-secret exposure on disk; archive/scrub.
3. **Daemon-Graveyard "ARC COMPLETE" overstated** — 6 reboot-survival daemons (watchdog/healthcheck/sitrep/uhura/real-portfolio/fleet-auditor) have retired plists but **no cron entry** in the live crontab; reboot-survival gap is partially open despite the backlog marking it closed.
4. **Off-host backup built but unscheduled** — `scripts/offhost_backup.sh` exists yet is absent from cron; the only off-machine DB backup path is not running automatically.

_— HM-LEDGER, read-only, 2026-06-10._
