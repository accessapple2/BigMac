# DAEMON GRAVEYARD — DIAGNOSIS + RE-HOME PLAN (PROPOSED, NOT APPLIED)

**Status: PROPOSAL ONLY. Nothing applied. Captain reviews before we restore monitoring.**
Surfaced by ALL-OUT-AUDIT-2026-05-30. Root cause = **reboot-survival-gap** (see memory `feedback_reboot_survival_gap`):
launchd `gui/501` LaunchAgents don't bootstrap on an SSH-only reboot; only cron `@reboot` does. The 2026-05-23 fix
re-homed 4 services (trader/signal-center/cloudflared/swingdesk) but left ~10 others on launchd → **dead 7 days**,
plus ~7 that were already dead before the reboot. The `com.trademinds.watchdog` safety monitor is among the dead.

═══════════════════════════════════════════════════════════════════════════════════════
## ⚠️ TWO CAVEATS THAT GATE THE WHOLE PLAN (the reason this is review-first)
═══════════════════════════════════════════════════════════════════════════════════════
**CAVEAT 1 — double-run / single-writer violation.** The plists STILL EXIST and are still loadable. If we cron-rehome
a job AND the Captain later logs into the GUI session (or runs `launchctl bootstrap gui/501 …`), **both** the cron
copy and the launchd copy fire → duplicate processes → orphan / single-writer breach (the exact failure the
restart-script gate guards against). **Every cron-rehome MUST be paired with one of:** (a) `launchctl disable
gui/501/<label>` + move the plist aside, OR (b) a single-instance lockfile guard inside the script.

**CAVEAT 2 — watchdog loses KeepAlive respawn under plain cron.** launchd ran `watchdog` with `RunAtLoad=true`
(daemon) and the launchd supervisor would respawn it on crash. A plain `@reboot` line fires **once at boot** — if
watchdog later dies, nothing relaunches it. To replicate KeepAlive, the watchdog re-home needs a **`*/5` supervisor**
(`pgrep -f watchdog.py || relaunch`), not just `@reboot`. Otherwise we restore a watchdog that can silently die again.

═══════════════════════════════════════════════════════════════════════════════════════
## THE GRAVEYARD — full list (16 dead jobs), by priority tier
═══════════════════════════════════════════════════════════════════════════════════════
Cadence keys: RAL=RunAtLoad daemon · SI=StartInterval(sec) · SCI=StartCalendarInterval(time).

### 🔴 TIER 1 — RESTORE FIRST (safety monitors, no in-process equivalent)
| job | script | cadence | last-fired | re-home | notes |
|---|---|---|---|---|---|
| **com.trademinds.watchdog** | `venv/bin/python3 watchdog.py` | RAL daemon | 05-23 08:27 | `@reboot` + **`*/5` supervisor** (Caveat 2) | THE safety net. Watches the trader, restarts on death. Restoring this first means the others get caught even if we miss one. |
| **com.trademinds.healthcheck** | `venv/bin/python3 healthcheck.py` | SCI hourly 06–13 | 05-23 08:00 | cron `0 6-13 * * *` | Stateless one-shot, clean cron fit. Reads autonomous_trader.db. |

### 🟠 TIER 2 — RESTORE (real function, gap CONFIRMED)
| job | script | cadence | last-fired | re-home | notes |
|---|---|---|---|---|---|
| **com.ollietrades.morningbriefing** | `venv/bin/python3 engine/morning_briefing.py` | SCI 06:00 | 05-23 06:00 | cron `0 6 * * *` **OR** fix in-process | **Gap CONFIRMED:** `data/morning_brief.json` stale since **05-29 20:00** — the in-process `run_archer_morning_briefing` (main.py:705) did NOT refresh it today. Decide: cron-restore the launchd job, OR fix the in-process registration (don't run both → Caveat 1). |

### 🟡 TIER 3 — RESTORE-OR-REVIEW (standalone pollers — check overlap with in-process daemons first)
| job | script | cadence | re-home | ⚠️ before restoring |
|---|---|---|---|---|
| com.ollietrades.squeeze-scan | `venv/bin/python3 scripts/run_squeeze_scan.py` | SI 900 | cron `*/15 * * * *` | squeeze alerts — confirm not already covered by an in-process scan |
| com.ollietrades.ghost-advisor | `venv/bin/python3 scripts/ghost_advisor.py` | SI 600 | cron `*/10 * * * *` | ghost-tracking advisor — standalone, likely safe |
| com.ollietrades.metals-sync | `venv/bin/python3 engine/metals_sync.py` | SCI 06:15,13:10 | cron `15 6 * * *` + `10 13 * * *` | feeds dalio/enterprise-computer metals prices |
| com.ollietrades.sitrep | `/usr/bin/python3 scripts/situation_report.py` | SCI 06:30,10:00,13:30 | cron 3 lines | **⚠️ uses system Python 3.9** — PEP604 crash risk (see `feedback_signal_center_python39`); switch to venv on restore |
| com.ollietrades.ollie-scan | `venv/bin/python3 scripts/run_ollie_scan.py` | SI 600 | cron `*/10 * * * *` | **⚠️ likely OVERLAPS the in-process LRS arena scan → double-scan risk. VERIFY before restoring.** |

### ⚫ TIER 4 — RETIRE / DO-NOT-RESTORE (intentionally off, crashed-long-dead, or wrong-project-path)
| job | why retire |
|---|---|
| com.ollietrades.crusher (`dr_crusher.sh`, SI 360) | cron already shows `dr_crusher ×2 DISABLED` — intentionally off. |
| com.trademinds.scanner (`engine.fast_scanner --daemon`, RAL) | `Crashed=true`, dead since **04-11**; superseded by in-process scanners. |
| com.ollietrades.optionsflow (`/Users/bigmac/ollietrades/options_flow_scanner.py`, 07:00) | points at the **old `/ollietrades` project dir** (not autonomous-trader), homebrew python. Wrong project. |
| com.ollietrades.etfregime (`/Users/bigmac/ollietrades/etf_regime_trader.py`, 06:35) | old `/ollietrades` dir — REVIEW (err log wrote 05-23, may still be wanted). |
| com.ollietrades.uhura (`agents/uhura_agent.py`, 05:30) | dead since **04-17** — pre-existing rot, REVIEW if still wanted. |
| com.ollietrades.fleet-auditor (`/usr/bin/python3 engine/fleet_auditor.py`, SI 900) | dead since **04-20**; system py 3.9 (PEP604 risk). REVIEW. |
| com.ollietrades.real-portfolio-snapshot (`scripts/snapshot_real_portfolio.py`, 13:45) | dead since **05-22** — snapshots real broker portfolio; REVIEW (may be wanted). |
| com.ollietrades.movers-poller (`scrapers/polygon_movers.py`, SI 300) | writes to `~/ollietrades` (old path); likely superseded. REVIEW. |

═══════════════════════════════════════════════════════════════════════════════════════
## RECOMMENDED APPROACH (for Captain sign-off)
═══════════════════════════════════════════════════════════════════════════════════════
1. **Restore the 2 safety monitors first** (watchdog + healthcheck) via cron, watchdog with the `*/5` supervisor
   (Caveat 2), each paired with `launchctl disable` of its plist (Caveat 1). This re-arms "the monitors that watch
   the monitors" before anything else — and once watchdog is back, a future single missed re-home gets caught.
2. **Decide morningbriefing**: cron-restore OR fix the in-process `run_archer_morning_briefing` — **pick one**, not both.
3. **Tier 3**: restore squeeze-scan/ghost-advisor/metals-sync; **verify ollie-scan doesn't double the in-process arena
   scan** before restoring it; fix sitrep's interpreter to venv.
4. **Tier 4**: leave retired; archive the old-`/ollietrades`-path plists.
5. **Make it durable**: put the re-homed cron block under a single managed script (e.g. `scripts/cron_daemons.sh`)
   so the next reboot-survival audit has one place to check, and the plists are disabled to prevent double-run.

**Implementation is NOT in this doc — show-and-review only.** On your `GO`, I'll produce the exact cron block +
the `launchctl disable` / plist-archive commands as a second proposal for a final eyes-on before applying. The
`launchctl disable` / GUI-bootstrap steps may need your logged-in session (gui/501 is unreachable from SSH).
