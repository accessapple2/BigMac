# REPAIR BOARD — 2026-06-01 (single-terminal, sequential)

RAILS: frontend BUILT but HELD for Admiral browser smoke (never shipped); all backend
batched into ONE `./scripts/trader_restart.sh` at the end → verify shadow boundary
(chokepoint live, 45 shadow signals intact, 0 shadow-originated trades) + season/Troi
green; sacred-data; revert-on-failure. Report shipped vs held vs scoped.

## STATE LEGEND: ⬜ todo · 🔨 building · ✅ done(committed) · 🟡 HELD(smoke) · 📋 scoped · 💤 summarized

### BUILD NOW
- ✅ 1. PBO matrix — N=36 configs, PBO=0.4787 FRAGILE → FAILS ≤0.30 leg (gate NOT cleared). `strategies/pbo_relative_strength.py` + report. Commit.
- ✅ 2. #2 0DTE MIN_PREMIUM=0.30 + STOP_DOLLARS=50 — verified offline (gate skips <$0.30; $stop binds -25% on $2). 0 open. Trader restart ACTIVATED.
- ✅ 3a. bridge-Kirk: dead `regime_indicators` import → repointed live (fg=72,vix=16.08,as_of). Trader restart ACTIVATED.
- ✅ 3c. fleet-count doc 20→21 (DB-verified). Shipped.
- 🟡 3b. scanner MU/DELL: NO backend defect (DELL payload correctly fielded). HELD for Admiral repro (frontend/other surface).
- 🟡 4. #8 crew dormant-drawer — built, node-clean. HELD (smoke: drawer toggles).
- 🟡 5. #9 LiveChart observer guard — built, node-clean. HELD (smoke: guard holds under stream).
- 🟡 6a. W1 frontend health grid — built, node-clean. HELD (smoke: kirk/riker RED + as-of render).
- ✅ 6b. W1 NTFY auto-quarantine BACKEND (source_gate tracker, report-only/AUTO-off). Signal-center restart ACTIVATED.

### RESTART GATE ✅ (trader 84573 + signal-center 84753, both single-writer)
- ✅ Boundary: chokepoint live · 45 shadow signals intact (both tables) · 0 shadow trades
- ✅ #1 season=0 live 224/$316/75.9%/1.69 (not zeros) · #3 Troi 750.0/754.01 (canonical, fresh snapshots)
- ✅ W1 /api/sources/health live + tracker state written · bridge-Kirk live 72/16.08

### SCOPE-ONLY (hold for go) ✅ docs written
- 📋 7. #10 DOM lazy-load → `SCOPE_DOM_LAZYLOAD_2026-06-01.md` (110 sections all in DOM; lazy-mount via <template>; P0 instrument + P1 top-5 pilot)
- 📋 8. Daemon re-home → `SCOPE_DAEMON_REHOME_COMMANDS_2026-06-01.md` (signal-center @reboot ALREADY done; close Caveat-1 plist; Kirk = unscheduled → cron; Tier-1 watchdog+*/5)

### SUMMARIZE ✅
- 💤 9. 4 specs → `SPEC_SUMMARIES_2026-06-01.md`. Build order: W3-OI → W2-obs → W3-gamma → W4(last).

---
## LOG
- PBO real result: 0.4787 FRAGILE (DSR passes, PBO fails → gate not cleared). Caveat: 36 collinear configs.
- #2 verified: -30% stop tick-realizable at ≥$0.30; $50 cap binds before -30% above ~$1.67 entry.
- bridge-Kirk root cause: `engine.regime_indicators` module deleted → except always fired → silent 50/20.
- W1 tracker: tick1 alerts 'signals' RED; tick3 recommends quarantine (report-only, no auto-disable).
- Restart gate: both services clean, boundary held, season/Troi/W1/Kirk all green.

## CORRECTION 2026-06-01 (HM-TZ-AZNOW root cause) — was NOT pytz
The scanner_status/holdings_top 7h skew is NOT pytz-singleton corruption (that diagnosis
was WRONG). Real cause = `dashboard/app.py` **TimezoneRoute middleware + _to_arizona** (line
670/687): it assumes EVERY response timestamp string is UTC and shifts -7h. Endpoints that
emit AZ (scanner_status, holdings_top) get DOUBLE-converted → 05:xx. Endpoints that emit UTC
(movers) convert once → correct. pytz/now(az) were always fine (clean-process tests bypass
the middleware). The az_now() swaps (f6abebc, e9dbd6d) are harmless+correct but were not the
fix; kept per Admiral as defensive no-ops.
- **Option A (this commit):** scanner_status + holdings_top → emit `datetime.now(timezone.utc)`
  so the middleware localizes ONCE (movers parity). Dormant; activates on after-close restart.
- **SECOND bug found (separate, NOT fixed):** `source_gate._market_aware_age` localizes naive
  bridge timestamps as **ET** (`mc.ET.localize`) but the middleware emits **AZ** (-7) → ~3h
  false-staleness on ALL bridge_iso intraday sources. PROOF: movers fresh (as_of=now) reads
  age=3h→RED. So Option A fixes the DISPLAY but the grid RED for scanner_status/holdings_top/
  movers likely persists (~3h) until this ET↔AZ mismatch is reconciled (bundle with Option B).
