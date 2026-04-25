# OllieTrades S6.3 Upgrade Report — 2026-04-20

*Master repair run. Trader was halted for the full session. All 9 Alpaca paper positions untouched.*

---

## Session Overview

A full-fleet repair run was executed with the KILL_SWITCH armed and launchd unloaded. The session resolved 20+ outstanding issues across database seeds, dashboard UX, infrastructure hardening, and fleet routing.

---

## Architectural Lessons

### L1 — Seed vs. UPDATE race condition (latent bomb pattern)

**What happened:** `setup_db.py` used `INSERT OR IGNORE` seeds with `qwen3.5:9b` as the initial `model_id`, followed by `UPDATE` statements to replace those values. The seeds only fire on an empty DB, but the UPDATEs run every time. This created a "latent bomb": the UPDATE was masking the seed bug. A DB wipe (e.g. disaster recovery) would leave agents running the deprecated model that caused 8GB swap storms.

**Lesson:** Seeds and their overriding UPDATEs must always agree. Write seeds to the correct final state so UPDATEs become no-ops. Never rely on a subsequent UPDATE to "fix" a bad seed.

---

### L2 — CSS grid vs. fixed-sidebar residue

**What happened:** The sidebar overlap on the Charts page at 769–1024px was caused by a `margin-left:60px` on `.layout` that was leftover from an older design where the sidebar was `position:fixed`. When the sidebar was refactored into the CSS grid as `position:sticky`, the margin was never removed — it now pushed the entire grid right without resizing any column, leaving a 180px dead zone and causing LightweightCharts to miscalculate its canvas width.

**Lesson:** When changing layout paradigms (fixed→grid→flex), audit ALL breakpoint-specific overrides. A CSS override written for one paradigm can silently break layout in another. Grid template columns must match actual element widths at every breakpoint.

---

### L3 — Hardcoded localhost vs. provider URL

**What happened:** The Ollama model unload call in `ai_brain.py` was hardcoded to `http://localhost:11434/api/generate`. Models that ran on the Ollie GPU (192.168.1.166) would never be unloaded after their scan group completed — causing VRAM accumulation. The `OllamaProvider` already carries `.url` as the correct endpoint (set at construction time from `OLLIE_URL`), so the fix was a one-liner: use `group[0][1].url` instead of the hardcoded string.

**Lesson:** Never hardcode infrastructure addresses in business logic. Always derive URLs from provider instances or config constants that carry the correct host. This is especially important in GPU-accelerated systems where multiple hosts serve different model sizes.

---

### L4 — Two metrics, one label

**What happened:** The dashboard showed "Fleet Value: $79k" while `/api/status` reported `total_portfolio_value: $332k`. This looked like a data integrity bug but was actually two legitimately different aggregations: the dashboard filtered to `_FLEET_CORE` (8 agents), while the API summed all 20+ active players. No math was wrong; the label was misleading.

**Lesson:** When two numbers at the same scope appear to disagree, always verify the aggregation boundary before assuming a bug. Disambiguate at the label level (e.g., "Core Fleet Value" vs. "Total Portfolio Value") before diving into data integrity investigation.

---

### L5 — Free vs. paid API data source divergence (earnings badge)

**What happened:** The earnings glance badge fetched from `/api/market/earnings` (yfinance, free, no key), while the expanded earnings hub section fetched from `/api/earnings/countdown` (Finnhub, API key required). Finnhub silently returned empty when the key was missing. The badge showed "1 upcoming earnings" while the hub showed "No upcoming earnings."

**Lesson:** When two UI surfaces display the same conceptual data, use the same API endpoint — even if the other one has richer data. Silent empty returns from keyed APIs are indistinguishable from "no data" in the frontend without explicit error surfacing.

---

### L6 — Log path archaeology (old project root vs new)

**What happened:** Three launchd plists (morningbriefing, etfregime, optionsflow) still pointed to `~/ollietrades/logs/` — the old project root before migration to `~/autonomous-trader/`. The scripts were running successfully (visible in `*_err.log` at the old path), but operators looking in the new `logs/` directory saw nothing.

**Lesson:** When a project root is renamed or migrated, audit all external files that reference absolute paths: launchd plists, cron entries, logrotate configs, systemd units. These do not get updated by `mv` or `cp` and fail silently if the old path still exists.

---

### L7 — `position:sticky` inside CSS grid vs. overflow ancestors

**What happened:** The Charts page used `#section-charts .card { overflow: visible !important; }` to prevent chart tooltips from being clipped. While correct for tooltip display, `overflow: visible` on a child of the grid's content area interacts with the sticky sidebar differently than `overflow: hidden`. Specifically, if any ancestor between `.main` and `body` has `overflow-x: hidden`, `position: sticky` can fail to track correctly, causing the sidebar to appear to "jump."

**Lesson:** `overflow: hidden` on any ancestor element between a sticky element and the viewport will cause sticky positioning to fail — the browser treats that ancestor as the scroll container, not the viewport. When debugging sticky layout issues, trace the full overflow chain from the sticky element to the root.

---

### L8 — Retry before alarm (healthcheck design)

**What happened:** Dr. Crusher's healthcheck declared a port down on the first failed curl, immediately triggering a full restart. Network hiccups, GC pauses, or a slow startup response would all cause false alarms and unnecessary restarts, which interrupted in-flight Ollama scans.

**Lesson:** Healthcheck decisions that trigger destructive actions (restart, failover, alert) should require multiple consecutive failures across a time window. "3 failures with 2s gap" is a minimum viable retry pattern. Single-failure triggers are appropriate only for observational metrics, not for actions.

---

### L9 — Model routing must be co-located with model assignment

**What happened:** Picard's `picard_strategy.py` used `OLLAMA_URL` (bigmac localhost) to call `gemma3:4b`. When `gemma3:4b` was moved to Ollie, only `_OLLIE_MODELS` in `ollama_watchdog.py` was updated (for the circuit breaker), but the actual generate call still hit bigmac. The model would cold-load from bigmac disk (slow) or fail if not installed there.

**Lesson:** Every file that makes an Ollama generate call must derive its URL from the same source of truth as the model routing table. The routing table (`_OLLIE_MODELS`) and all call sites must move together. Centralizing via `_model_url(model_id)` eliminates this class of drift.

---

### L10 — Structured logs are forensic evidence

**What happened:** Ollama timeouts were visible in console logs but as freeform text. Post-mortems required parsing logs with regex. When multiple models time out simultaneously (e.g., Ollie GPU overload), the incident timeline is impossible to reconstruct from unstructured text.

**Lesson:** Any event that crosses a threshold (timeout, circuit break, error rate) should be logged as structured JSON with: timestamp, model_id, count, action. JSONL files are trivially parseable by `jq` and can be aggregated into dashboards. Operational logs should be designed as evidence, not commentary.

---

## Systems Changed

| File | Change Type | Description |
|------|------------|-------------|
| `setup_db.py` | Bug fix | qwen3.5:9b seeds replaced with correct model IDs |
| `engine/ai_brain.py` | Bug fix | Ollama unload URL hardcode → provider.url |
| `dashboard/static/sw.js` | Version bump | v4→v6 to force cache refresh |
| `dashboard/static/index.html` | Multiple fixes | Core Fleet Value label, F&G null guard, earnings hub alignment, sidebar grid fix, strategy picker options, close button CSS |
| `dashboard/app.py` | Feature | CBOE fallback for GEX, /api/health-manifest endpoint |
| `engine/fleet_auditor.py` | New file | Autonomous health manifest generator |
| `engine/picard_strategy.py` | Routing | OLLAMA_URL → OLLIE_URL for gemma3:4b |
| `engine/ollama_watchdog.py` | Feature + routing | JSON timeout log, gemma3:4b + mistral:7b added to _OLLIE_MODELS |
| `engine/fleet_halt.py` | Referenced | KILL_SWITCH respected throughout |
| `dr_crusher.sh` | Enhancement | Retry 3x before declaring port down |
| `main.py` | Multiple | Warmup Ollie-aware, startup log updated |
| `scripts/nightly_regression.sh` | New file | Weekday nightly backtest + ntfy |
| `.github/workflows/grep-gate.yml` | New file | 4-check forbidden pattern CI gate |
| `~/Library/LaunchAgents/*.plist` | Log paths | 3 plists: ollietrades/logs → autonomous-trader/logs |

---

## Outstanding (Deferred)

| Item | Reason Deferred |
|------|----------------|
| 5.2 — ALPACA_* vs APCA_* consolidation | 73 refs, env var naming conventions, needs Admiral decision on canonical var name |
| Morningbriefing script path (plist) | Script at ~/ollietrades/ is older version; autonomous-trader engine version is 2.5x larger; migration needs explicit review |
| Webull advisory team migration | `wb_advisory_team.py` reads `steve-webull` positions; needs Alpaca position reader |
| Polygon.io Options Starter activation | Approved-in-principle; requires billing activation + API key wiring for Neo + McCoy/Dax |

---

## First-Run Fleet Auditor Baseline (trader halted)

```
Jobs:   3 OK / 5 stale (expected — trader halted)
APIs:   9 OK / 0 down
Ollama: 2 OK / 0 down (bigmac + Ollie both online)

Stale jobs (market off + trader halted):
  signals:          8933m stale (trader halted — OK)
  portfolio_positions: 39m stale
  gex_snapshots:    77m stale
  picard_briefings: 21 days stale (weekly cadence — expected)
  premarket_scan:   438m stale (trader halted — OK)
```

---

*End of report. Trader remains halted pending Admiral restart approval.*
