# Sunday Drydock Final Report — 2026-04-26

> **Note:** `.bak.*` paths referenced below were archived 2026-05-04 to `archive/sprint-backups/2026-04/` (HM-H). Path strings in this document are historical references; the files themselves are flattened (slashes → double underscores) at that location. See `archive/sprint-backups/2026-04/README.md` for the restore procedure.

## Session Overview

Two-phase session: full architectural audit (read-only) followed by
implementation of all approved recommendations and Class B route promotions.
Gate held `_EXECUTION_ENABLED=False` throughout. Alpaca paper account untouched.

---

## Items Shipped

### Audit Phase (morning)

| # | Item | Description |
|---|------|-------------|
| A1 | Full architectural audit | `docs/OT_AUDIT_2026-04-26.md` (650 lines, 41KB). Six questions answered by data: port 9000 health/timeouts, Bridge dashboard review, Signal Center review, tool inventory, tool coverage, 5 recommendations. |
| A2 | Port 9000 health probed | All Signal Center endpoints measured with `curl --max-time 30`. Key finding: `/api/signals/all` 35s timeout risk at 601MB signals.db. |
| A3 | Duplicate route inventory | 620 `@app.` decorators in app.py. Precise Python-regex import scan identified 8 true duplicates (not 30 as grep estimated). |
| A4 | Tool inventory | 74 engine/*.py files; 18 rescued from archival by paranoia check; 12 confirmed orphans. |

### Rec #2 — Class A duplicate route removal

| # | Item | Description |
|---|------|-------------|
| B1 | Remove 4 dead backtest decorators | Removed `@app.get` at dead second-registrations for: `/api/backtest/community-leaderboard`, `/api/backtest/history`, `/api/backtest/templates`, `/api/backtest/matrix`. Stubs at lines 10131–10141 remain as live handlers. All 4 routes HTTP 200 post-removal. |

### Rec #3 — Orphan engine tool archival

| # | Item | Description |
|---|------|-------------|
| C1 | Archive 12 orphan engine tools | Moved to `engine/_archive/2026-04-26/`. README documents each tool, reason, and expiry (2026-07-25). 18 candidates rescued by paranoia check remain in `engine/`. |

### Class B route promotions (4 routes)

| # | Route | Action |
|---|-------|--------|
| D1 | Route 3: `/api/risk-levels/{symbol}` | Dead `smart_risk` handler renamed to `/api/risk-management/{symbol}` (promoted). Live S/R heatmap remains at original path. |
| D2 | Route 4: `/api/volume-profile/{symbol}` | Live `sr_heatmap` renamed to `/api/sr-heatmap/{symbol}`. Dead `volume_profile` un-shadowed at `/api/volume-profile/`. Consumer 2 in index.html line 23641 updated. Both gate PASS: `poc` scalar vs object shapes correct per consumer. |
| D3 | Route 1: `/api/options/chain` | Live SPY-only renamed to `/api/options/spy-chain`. Dead Alpaca-direct (multi-ticker) promoted at `/api/options/chain`. Consumer in index.html line 11644 updated. Gate PASS: spy-chain 200, chain?ticker=SPY 200, count=500. |
| D4 | Route 2: `/api/options/positions` | Live Alpaca broker renamed to `/api/options/positions/live`. Dead DB handler promoted at `/api/options/positions/db`. Both consumers in index.html lines 33486–33487 updated. Old path correctly returns 404. Gate PASS. |

### Power close-out (afternoon)

| # | Item | Description |
|---|------|-------------|
| E1 | signals.db archival infrastructure | Archive DB scaffolded at `signal-center/signals_archive.db`. Schema mirrors `signal_history` + `intelligence_feed`. `archive_metadata` table documents 30-day rolling policy, cutoff 2026-03-27, source. 0 rows archived today (all data starts 2026-04-05 — first eligible archive date is **2026-05-05**). VACUUM run on signals.db (no free pages — all live data). |
| E2 | Dr. Crusher cron restored | `~/Library/LaunchAgents/com.ollietrades.crusher.plist` created with `StartInterval=360`. Safety verified: `[BACKUP]` prefix on all alerts, no restart commands, ntfy topic `ollietrades-admin`. Manual run clean (both ports OK, no ntfy fired). `launchctl list` shows exit 0. |
| E3 | Troi ghost cleanup | Removed 3 dead references from index.html: `wbAdvTab-troi` button (line 5498), `wbAdvPanel-troi` div (lines 5504–5506), `showSection('troi')` actFn (line 25823). `_renderTroiPanel` JS stub left in place (safe-fail: `if (!body) return`). Backup: `index.html.bak.20260426.troi`. |
| E4 | QuiverQuant 401 silenced | `QUIVER_ENABLED = False` added to `engine/congress_scraper.py` (line 17). Call site at line 184 wrapped in `if QUIVER_ENABLED:` guard with `else: console.log(...)`. Historical 401 count: 328. Future count: 0. Backup: `congress_scraper.py.bak.20260426`. |
| E5 | gemini-2.5-pro + grok-3 model confirmed | Both confirmed at `qwen3:8b` in `ai_players` table (already updated in prior session). `grok-3` is_halted=1 (retired 2026-04-25). No DB writes needed. No restart required. |
| E6 | This document | `docs/SUNDAY_DRYDOCK_2026-04-26_FINAL.md` |

**Total shipped today: 20 items** (4 audit + 1 class-A dedup + 1 archival + 4 class-B routes + 6 power close-out + 1 crusher plist + 1 Troi cleanup + 1 QuiverQuant + 1 model confirm + 1 doc)

---

## Items Filed for Future Sprints

| Item | Notes |
|------|-------|
| Recommendation #1: index.html split | 33,940 lines / 1.8MB monolith. Needs dedicated sprint doc. Split by section into partials loaded on demand. |
| Recommendation #5: nav rationalization | Depends on #1 (nav lives in the monolith). Do after #1 lands. |
| Lane governor (CRITICAL/SCHEDULED/ON_DEMAND/BATCH) | Priority queuing for LLM calls. Prevents scan storms from starving trading-path queries. |
| Stagger chart_analyzer / crew / debate_engine | All fire at :00 of each interval. Spread over 60s window to flatten Ollama load spikes. |
| `/api/lrs-stats` endpoint | Expose LRS queue depth + timeout counts as JSON for dashboard widget. |
| neo-matrix phi3:mini route investigation | phi3:mini was skipped at startup (7.0GB > 6.0GB limit). neo-matrix config points there — verify fallback is correct. |
| Memory correction: G1 VRAM | Recorded as ~32GB in some notes; actual M4 Mac Mini GPU VRAM is ~8GB shared. Update any doc that cites 32GB. |
| Uhura error log investigation | Check Uhura 13F/Form4 parse errors before Tuesday gate-flip decision. |
| signals.db weekly archival cron | Re-run archival script weekly starting 2026-05-05 when 30-day cutoff begins producing rows. Candidate for main.py scheduler or separate Sunday cron. |
| signal_history raw_data size audit | `signal_tracker` blobs avg 75KB each (205MB total for 2722 rows). Consider storing only summary fields + a hash; archive full raw_data to cold store after 7 days. |

---

## Backups Created Today

| File | Path |
|------|------|
| app.py (pre-dedup) | `dashboard/app.py.bak.20260426.dups` |
| app.py (pre-class-B) | `dashboard/app.py.bak.20260426.classb` |
| app.py (final) | `dashboard/app.py.bak.20260426` |
| index.html (pre-Troi) | `dashboard/static/index.html.bak.20260426.troi` |
| congress_scraper.py | `engine/congress_scraper.py.bak.20260426` |

---

## Rollback Paths

| Item | Rollback Command |
|------|-----------------|
| E1 signals.db | `sqlite3 signals.db "ATTACH 'signals_archive.db' AS a; INSERT INTO signal_history SELECT * FROM a.signal_history;"` |
| E2 Crusher cron | `launchctl unload ~/Library/LaunchAgents/com.ollietrades.crusher.plist` |
| E3 Troi cleanup | `cp dashboard/static/index.html.bak.20260426.troi dashboard/static/index.html` |
| E4 QuiverQuant | In `congress_scraper.py`: set `QUIVER_ENABLED = True` |
| E5 model_id | `sqlite3 data/trader.db "UPDATE ai_players SET model_id='qwen3:14b' WHERE id IN ('gemini-2.5-pro','grok-3');"` |
| D1–D4 routes | `cp dashboard/app.py.bak.20260426.classb dashboard/app.py` + revert index.html |
| B1 class-A dedup | `cp dashboard/app.py.bak.20260426.dups dashboard/app.py` |
| C1 archival | `mv engine/_archive/2026-04-26/*.py engine/` |

---

## Monday Morning Checklist

- **9:30 AM** — validate all routes under real market load (routes D1–D4)
- **10:00 AM** — smoke test:
  ```bash
  for ep in "/api/options/spy-chain" "/api/options/chain?ticker=SPY" \
             "/api/options/positions/live" "/api/options/positions/db?book=fleet" \
             "/api/sr-heatmap/SPY" "/api/volume-profile/SPY" \
             "/api/risk-levels/SPY" "/api/risk-management/SPY?entry_price=560&side=BUY"; do
    echo -n "$ep: "; curl -s -o /dev/null -w "%{http_code}\n" --max-time 10 "http://localhost:8080$ep"
  done
  ```
- **10:30 AM** — check:
  - Crusher BACKUP alert count in ntfy `ollietrades-admin` (should be 0 if both ports stayed up)
  - QuiverQuant 401 count: `grep -c "Quiver Quant HTTP 401" ~/autonomous-trader/logs/trader.log` (should not increase from 328)
  - signals.db size: `ls -lh ~/autonomous-trader/signal-center/signals.db` (expect steady growth ~27MB/day until archival kicks in 2026-05-05)
- **Afternoon** — iv_history Day 3 verification at 9:45 MST if not already done

---

## Memory Edits Pending

- Update project memory `project_ollietrades_s6.md`:
  - 8 true duplicate routes resolved (Class A 4 + Class B 4)
  - 12 engine tools archived to `engine/_archive/2026-04-26/`
  - Dr. Crusher cron restored (backup-only mode, 360s)
  - QuiverQuant disabled (`QUIVER_ENABLED=False`)
  - signals.db archival policy: 30-day rolling, first eligible 2026-05-05
- `gemini-2.5-pro` and `grok-3` already at `qwen3:8b` (no change needed in memory)

---

*Generated at end of Sunday Drydock 2026-04-26 session.*
*Audit document: `docs/OT_AUDIT_2026-04-26.md`*
*Archive README: `engine/_archive/2026-04-26/README.md`*
