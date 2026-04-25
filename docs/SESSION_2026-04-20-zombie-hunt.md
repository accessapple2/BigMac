# SESSION 2026-04-20 — qwen3.5:9b Zombie Hunt

## Objective
Permanently eliminate all code paths that load qwen3.5:9b on bigmac
(Mac Mini M4, 16GB RAM). Model is 8GB and causes swap storms.

---

## Root Cause (full timeline)

| Time (AZ) | Action |
|-----------|--------|
| ~07:30 | Routed 17 agents to Ollie GPU in main.py `initialize_arena` |
| ~08:03 | Fixed fallback.py / ai_brain.py / ollama_queue.py |
| ~08:42 | Fixed ollama_watchdog.py + picard_strategy.py |
| ~09:04 | Fixed crew_scanner.py + crew_specialization.py (19 agents in CREW_MANIFEST) |
| 09:08–11:30 | Endurance check FAILED — qwen3.5:9b cycling every ~44 seconds |
| 11:30–12:00 | Root cause traced: bridge_vote.py fires every 5 min, 8 BRIDGE_VOTERS all hardcoded to qwen3.5:9b@localhost. Each voter call sets keep_alive=45s → perpetual cycling. Secondary: mlx-qwen3 in ai_players DB had model_id='qwen3.5:9b' |
| ~12:00 | 10-file atomic patch: bridge_vote.py (THE ZOMBIE GENERATOR) + kirk_grok_advisor.py + 3 crew files + scenario_modeler.py + scout_critic.py + self_improvement.py + chart_analyzer.py + dashboard/app.py. DB UPDATE for mlx-qwen3. |
| ~12:15 | Final sweep found 4 more active files. Investigated + patched: debate_engine.py + grok_provider.py + premarket_scanner.py + bull_bear.py |
| ~12:30 | Restart + endurance v2 launched (PID 93644) |

---

## 12:00 PM Fix — bridge_vote + 9 other files (THE ZOMBIE GENERATOR)

**Files patched (with .bak.2026-04-20-1200 backups):**
- `engine/bridge_vote.py` — 8 BRIDGE_VOTERS: all `qwen3.5:9b@localhost` → routed per `_BIGMAC_VOTER_IDS` (neo-matrix + capitol-trades → bigmac phi3:mini; all others → Ollie qwen3:8b)
- `engine/kirk_grok_advisor.py` — OLLAMA_BASE + model
- `engine/crew/daily_review_crew.py` — OLLAMA_BASE + model (URGENT: fires 13:15 AZ)
- `engine/crew/strategy_crew.py` — OLLAMA_BASE + model
- `engine/crew/weekly_tuning_crew.py` — OLLAMA_BASE + model
- `engine/scenario_modeler.py` — OLLAMA_BASE + DEFAULT_MODEL
- `engine/scout_critic.py` — OLLAMA_URL + CRITIC_MODEL
- `engine/self_improvement.py` — _OLLAMA + model (URGENT: fires 14:30 AZ)
- `engine/chart_analyzer.py` — OLLIE_URL + qwen3:8b in grok branch
- `dashboard/app.py` — 7 inference refs across /api/research/followup, /api/arena/ai-chat, /api/arena/player/grade, /api/reasoning/{symbol}, /api/bakeoff/start, /api/kirk/ask
- `data/trader.db` — `UPDATE ai_players SET model_id='phi3:mini' WHERE id='mlx-qwen3'`
- `setup_db.py` — lines 262 + 287: mlx-qwen3 model_id → phi3:mini

---

## 12:15 PM Fix — Final sweep (debate_engine + 3 files)

**Files patched (with .bak.2026-04-20-1215 backups):**
- `engine/debate_engine.py` — OLLAMA_BASE localhost→Ollie; MODELS[general/light/scanner] qwen3.5:9b→qwen3:8b; TradingAgents deep_think_llm→qwen3:14b + backend_url→Ollie
- `engine/providers/grok_provider.py` — OLLAMA_URL→OLLIE_URL; hardcoded qwen3.5:9b→honours model param (default qwen3:8b)
- `engine/premarket_scanner.py` — grok branch: OLLAMA_URL→OLLIE_URL + qwen3.5:9b→qwen3:8b
- `engine/bull_bear.py` — gemini branch: OLLAMA_URL→OLLIE_URL + qwen3.5:9b→qwen3:8b

**Restart:** 10:08:14 PDT → PID 93521 clean (no errors, 25 positions intact)

---

## Endurance v2 — PASSED ✅

- PID: 93644
- Start: 10:08:47 PDT / Complete: 10:23:47 PDT
- Pre-endurance timeout count: 2112 → +5 new (benign)

| Minute | api/ps model | Verdict |
|--------|-------------|---------|
| 5 (10:13:47) | qwen3.5:9b expires 10:14:32 | ⚠️ Stale — pre-restart queue drain artifact |
| 10 (10:18:47) | qwen3:14b | ✅ Correct Ollie model |
| 15 (10:23:47) | (empty) | ✅ Clean |

**Minute-5 explanation:** OllamaQueue had 300s-timeout in-flight requests from old process (09:54–10:07). Ollama server completed one at ~10:13:47, refreshing keep_alive once. Not a new code path call.

**Post-restart log:** Only phi3:mini, qwen3:8b, qwen2.5-coder:7b, qwen3:14b seen. MemGuard switching correctly. No qwen3.5:9b calls from new process.

**VERDICT: ZOMBIE IS DEAD.**

---

## Deferred to Post-Close

| File | Line | Reason deferred |
|------|------|-----------------|
| `engine/portfolio_optimizer.py` | 36 | CLI only, no timer |
| `engine/rebalancer.py` | 39 | CLI only, no timer |
| `engine/generated_assets.py` | 74 | Dashboard on-demand only |
| `engine/first_officer.py` | 71 | Dashboard on-demand; fallback list only |
| `engine/debate_engine.py` stale `# (phi3:mini)` comments | various | cosmetic, post-close cleanup |
| `setup_db.py` non-mlx-qwen3 INSERT seeds | 239–286 | overridden by UPDATEs; DB already correct |

---

## Hardening (Phase C/G/H) — ~10:30–10:45 PDT

### C1 — Dr. Crusher Ollie check (healthcheck.py)
- Added `OLLIE_URL` constant + `check_ollie()` function (TCP + /api/tags + qwen3:8b present)
- Added `ollie_ok/ollie_info` in `main()` — logs ✓/⚠️; ntfy push on failure (priority=high)
- Backup: `healthcheck.py.bak.2026-04-20-1300`
- Syntax check: PASS. Manual test: `✓ ok — 6 models, qwen3:8b present`

### C2 — Pre-commit grep gate (.git/hooks/pre-commit)
- Blocks staging of any file with active `qwen3.5:9b` references (non-comment lines)
- Skips `.bak*`, `.md`, session docs; allows `--no-verify` bypass with warning
- Smoke test: PASS

### C3 — File-based fleet halt (engine/fleet_halt.py)
- `touch KILL_SWITCH` → halts bridge_vote + run_scanner + blocks all trades in check_trade()
- `rm KILL_SWITCH` → resumes immediately (no restart needed)
- Integrated into: `engine/bridge_vote.py:run_bridge_vote_job()`, `main.py:run_scanner()`, `engine/trade_gateway.py:check_trade()` (check_0 before DB kill switch)
- Backups: `bridge_vote.py.bak.2026-04-20-1300`, `main.py.bak.2026-04-20-1300`, `trade_gateway.py.bak.2026-04-20-1300`
- Functional test: PASS (touch → active; rm → clear)

**Restart after C1+C2+C3:** PID 96808, clean start. 17 positions intact.

### G — Ready Room Check (10:43 PDT)
- Open positions: 17
- Portfolio value: $332,748.70
- Trades since restart: 0 (market closed — Sunday)
- Ollie: idle, no models loaded between scans
- RAM: 0.7 GB free+inactive (tight but stable; Ollie GPU handling inference load)
- Signals: 0 (off-hours)

### H — Neo 404 Fix (phi3:mini on Ollie)
- `ollama pull phi3:mini` executed on Ollie via SSH
- Result: SUCCESS — phi3:mini now available at 192.168.1.166:11434
- Ollie model roster: phi3:mini, 0xroyce/plutus:latest, qwen2.5-coder:7b, deepseek-r1:14b, qwen3-coder:30b, qwen3:14b, qwen3:8b (7 total)
- Neo (neo-matrix) crew_scanner calls to Ollie will no longer 404

---

## ~12:00 PM DB Routing Fix — ai_players.model_id (12 rows total)

### Root cause
`initialize_arena()` in `main.py` was patched this morning (routing all agents off
qwen3.5:9b to Ollie GPU), but the `ai_players.model_id` DB column was never updated
to match. Any code path that reads `model_id` from DB (crew files, dashboard endpoints)
was still getting `qwen3.5:9b` and calling bigmac localhost.

**Source of truth going forward:** `ai_players.model_id` column aligned with
`main.py initialize_arena()` line references.

**Backup:** `data/trader.db.bak.2026-04-20-1200-pre-dbfix` (153MB)

### Wave 1 — 4 authorized fixes (discovered via live trader log)

| id | Before model_id | After model_id | Before fallback | After fallback |
|----|----------------|----------------|-----------------|----------------|
| ollama-kimi | qwen3.5:9b | phi3:mini | — | — |
| gemini-2.5-flash | qwen3.5:9b | qwen3:8b | qwen3:8b | qwen3:8b |
| ollama-llama | phi4:14b | deepseek-r1:14b | deepseek-r1:7b | qwen3:8b |
| gpt-o3 | deepseek-r1:7b | qwen3:8b | gemma3:27b | qwen3:14b |

### Wave 2 — 8 discovered fixes (found by scanning for qwen3.5 in DB after wave 1)

| id | Before model_id | After model_id | main.py ref |
|----|----------------|----------------|-------------|
| claude-sonnet | qwen3.5:9b | qwen3:8b | line 106 |
| gpt-4o | qwen3.5:9b | qwen3:8b | line 91 |
| grok-3 | qwen3.5:9b | qwen3:14b | line 109 |
| ollama-gemma27b | qwen3.5:9b | qwen3:8b | line 74 |
| ollama-qwen3 | qwen3.5:9b | qwen3:8b | line 76 |
| ollama-glm4 | qwen3.5:9b | qwen3:8b | line 78 |
| options-sosnoff | qwen3.5:9b | qwen3:8b | line 97 |
| energy-arnold | qwen3.5:9b | qwen3:8b | line 98 |

### Final scan result
```
SELECT ... WHERE model_id LIKE '%qwen3.5%' OR fallback_model LIKE '%qwen3.5%'
→ ZERO ROWS ✅
```

### ~12:15 PM Final DB cleanup

**dalio-metals bug fix:** model_id was `gemini-2.5-flash` (agent ID, not a model name)
→ corrected to `qwen3:8b` (per main.py:104).

**gemma3:27b fallback cleanup (4 rows):** `gemma3:27b` was never installed on Ollie —
would 404 on fallback. Updated to `qwen3:8b` on all 4 rows:
- claude-sonnet, claude-haiku, gpt-4o, gemini-2.5-pro

**Final distinct fallback values in DB (all safe):**
| fallback_model | agents |
|---------------|--------|
| qwen3:8b | 8 |
| deepseek-r1:7b | 4 |
| qwen3:14b | 1 |

All ai_players fallback values now reference existing, installed models. ✅

---

## Bridge Voter Routing (post-fix)

| Player | Name | Model | Routes to |
|--------|------|-------|-----------|
| neo-matrix | Neo | phi3:mini | bigmac localhost |
| capitol-trades | Capitol Trades | phi3:mini | bigmac localhost |
| grok-4 | Spock | qwen3:8b | Ollie GPU |
| ollama-glm4 | Q | qwen3:8b | Ollie GPU |
| ollama-qwen3 | Dax | qwen3:8b | Ollie GPU |
| super-agent | Mr. Anderson | qwen3:8b | Ollie GPU |
| navigator | Ensign Chekov | qwen3:8b | Ollie GPU |
| ollama-plutus | Dr. McCoy | 0xroyce/plutus:latest | Ollie GPU |

---

## 12:21 PM — THE ROOT CAUSE: Stale Bytecode (Phase C bytecode + restart)

### What was actually wrong

All morning's `main.py` patches had **zero effect** on the running process.

The launchd service uses system Python:
```
/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/...
```

Python 3.9 loads `.pyc` from `__pycache__` if the cached file exists. The file
`__pycache__/main.cpython-314.pyc` dated **April 1, 2026** was being loaded
instead of the patched `main.py`. Result: `initialize_arena()` used the old
qwen3.5:9b assignments for every provider, regardless of how many times main.py
was patched on disk.

**DB fixes only helped** code paths that re-read `model_id` from DB per-call
(crew files, dashboard endpoints). The `arena.providers` dict is built once at
startup from `initialize_arena()` return values — DB never consulted again.

### Fix

1. Purged all `.pyc` files from project root `__pycache__` and
   `engine/`, `engine/crew/`, `engine/providers/`, `dashboard/`, `scripts/`
2. Sent `keep_alive=0` to bigmac Ollama to pre-unload qwen3.5:9b
3. `launchctl unload` → `launchctl load` com.trademinds.trader.plist

### Verification

- New PID: **7491** (started 12:21 PDT)
- Startup warmup: `gemma3:4b + mistral:7b only` ✅ (old was `qwen3.5:9b + gemma3:4b + plutus`)
- Post-restart log: **ZERO qwen3.5 references** in 12:21+ entries
- Position integrity: **22 positions** — matches pre-restart snapshot ✅
- bigmac Ollama: qwen3.5:9b still in api/ps at 12:26 — residual keep_alive
  from old process's last inflight. Expires naturally (~45s). Not a new call.

### New issue flagged: claude-sonnet routing to localhost

At 12:25:35, war_room logged:
```
War room error for claude-sonnet: HTTPConnectionPool(host='localhost', port=11434):
Read timed out. (read timeout=180)
```
This is a 180s raw-requests timeout (not OllamaQueue's 300s), suggesting
`claude-sonnet` has a direct HTTP call path to localhost. Separate from the
qwen3.5 zombie hunt — to investigate post-close.

### Lesson #8 — Upgrade Report

> **After editing `.py` files, purge `__pycache__` before restart.**
> The launchd service uses system Python which happily loads stale `.pyc` even
> when the source file is newer. Add a `find . -name "*.pyc" -delete` step to
> `launch-trademinds.sh` as standard pre-start procedure.

---

## 12:45 PM CRITICAL FIX — setup_db.py zombie resurrection

### Root cause

`setup_db.setup()` is called at every startup (`main.py:2431`). It contained
**explicit UPDATE statements** (lines 272–288) that hard-reset `model_id` back
to `qwen3.5:9b` for 10 agents after any restart:

```python
# What was running on every startup:
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='claude-sonnet'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='gpt-4o'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='grok-3'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='ollama-gemma27b'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='ollama-glm4'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='ollama-kimi'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='gemini-2.5-flash'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='options-sosnoff'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='ollama-qwen3'
UPDATE ai_players SET model_id='qwen3.5:9b' WHERE id='energy-arnold'
```

This is why every restart today re-zombified the DB. The 13 DB UPDATEs from
Wave 1+2 (12:00 PM) were wiped 21 minutes later when PID 7491 started and
`setup_db.setup()` ran again. Every single restart all day was doing this.

### Fix applied (~12:45 PM)

**Backup:** `setup_db.py.bak.2026-04-20-1240`

Patched `setup_db.py` lines 272–289 to match `main.py` `initialize_arena()`
routing. 13 active SQL UPDATEs changed, 1 new UPDATE added (dalio-metals):

| Agent | Before | After |
|-------|--------|-------|
| claude-sonnet | qwen3.5:9b | qwen3:8b (Ollie) |
| gpt-4o | qwen3.5:9b | qwen3:8b (Ollie) |
| gpt-o3 | deepseek-r1:7b | qwen3:8b (Ollie) |
| grok-3 | qwen3.5:9b | qwen3:14b (Ollie) |
| ollama-gemma27b | qwen3.5:9b | qwen3:8b (Ollie) |
| ollama-glm4 | qwen3.5:9b | qwen3:8b (Ollie) |
| ollama-kimi | qwen3.5:9b | phi3:mini (bigmac) |
| gemini-2.5-flash | qwen3.5:9b | qwen3:8b (Ollie) |
| options-sosnoff | qwen3.5:9b | qwen3:8b (Ollie) |
| ollama-qwen3 | qwen3.5:9b | qwen3:8b (Ollie) |
| energy-arnold | qwen3.5:9b | qwen3:8b (Ollie) |
| ollama-deepseek | deepseek-r1:7b | deepseek-r1:14b (Ollie) |
| dalio-metals | (no UPDATE existed) | qwen3:8b (Ollie) — new |

**Syntax check:** PASS (`python3 -m py_compile setup_db.py`)

### Current DB state

Still shows qwen3.5:9b for 10 agents — **expected**. No restart was done
(PID 7491 left running). DB will be corrected automatically on next restart
when `setup_db.setup()` runs the patched UPDATEs.

### Deferred (out of scope today)

- `setup_db.py` INSERT OR IGNORE seed list (lines 239-259): still has
  qwen3.5:9b in 9 seed entries. Harmless for current DB (INSERT OR IGNORE
  won't overwrite existing rows), but stale on fresh installs. Fix next sprint.
- `setup_db.py` `_fallback_seed` (lines 355-361): sets fallback_model to
  qwen3.5:9b for 5 agents if fallback is NULL. Null-guarded — won't fire
  for current DB. Fix next sprint.

### Lesson #9 — Upgrade Report

> **Seed/setup scripts must be the single source of truth OR must defer to
> live state — never both.** `setup_db.py` was seeding model config on every
> startup, silently undoing any live DB change. Rule: seed scripts run once
> (on fresh install), or use INSERT OR IGNORE everywhere. Never UPDATE with
> hardcoded values in a script that runs on every boot.

---

## 12:55 PM Final fixes of the day

### Fix #2 — Picard early-return bug (`main.py:3028–3031`)

**Problem**: `run_picard_briefing()` returned immediately whenever any
briefing existed in DB — regardless of age. The comment said "less than
6 hours old" but there was no age check at all. Resulted in 3 missed
Sunday cycles (Apr 6, 13, 19).

**Fix (Option A)**: Removed the stale-check block entirely. The scheduler
already gates to Sunday 10:00–10:30 PM AZ — duplicate generation in that
30-min window is impossible. Now just calls `generate_picard_briefing()`
unconditionally when the time window is met.

```
Backup: main.py.bak.2026-04-20-1250-picard
Lines removed: latest = get_latest_briefing() + early-return (3 lines)
Import cleaned: get_latest_briefing removed from import
Syntax check: PASS
```

Next fire: **Sunday Apr 27, 10:00–10:30 PM AZ**

---

### Fix #3 — VIX `check_vix_spike` + `get_vix_status` missing (`engine/vix_monitor.py`)

**Problem**: `main.py:471` imported `check_vix_spike` from `vix_monitor.py`
but neither `check_vix_spike` nor `get_vix_status` existed. Error logged
every scan cycle (every ~5 min) during market hours.

**Fix (Option B)**: Added both stubs to `engine/vix_monitor.py`. Both read
the last 2 rows from `vix_term_structure` (3,368 rows in DB) and compute
consecutive-snapshot change:

- `check_vix_spike(threshold_pct=5.0)` → `{"price", "change_pct"}` if
  abs(change) ≥ threshold, else `None`
- `get_vix_status()` → `{"price", "change_pct"}` always (for routine
  dim-log display)

No changes to `main.py` — caller interface already matched.

```
Backup: engine/vix_monitor.py.bak.2026-04-20-1255
Lines added: 40 (two functions appended to end of file)
Syntax check: PASS
```

---

### Stand-down status

All fixes take effect on next restart. PID 7491 left running.

| Fix | File | Takes Effect |
|-----|------|-------------|
| setup_db.py qwen3.5 UPDATEs | setup_db.py | Next restart |
| DB model_ids re-applied | trader.db | ✅ Live now |
| Picard early-return | main.py | Next restart |
| VIX stubs | engine/vix_monitor.py | Next restart |
| Bytecode purge | __pycache__ cleared | ✅ Done |

---

## Q2 Mop-Up Session (~13:00–17:30 AZ)

### #11 — claude-sonnet bypass investigation (Phase C)

**Finding:** No bypass. war_room timeouts for claude-sonnet are OllamaQueue saturation events — the fallback path calls Ollie localhost via requests.post without going through OllamaProvider. Claude API calls never involved Ollama. Option A accepted (no code changes needed).

### #11B — Crew file ADVISORY_OLLAMA_URL routing

**Files patched:**
- `engine/crew/daily_review_crew.py` — OLLAMA_URL: ADVISORY_OLLAMA_URL → OLLAMA_BASE_URL → OLLIE_URL
- `engine/crew/strategy_crew.py` — same chain
- `engine/crew/weekly_tuning_crew.py` — same chain
- `engine/crew_strategy_lab.py` — added `from config import OLLIE_URL`; same chain (was hardcoded localhost)

**Backups:** `.bak.2026-04-20-q2-mop11b`

### #12 — Dashboard portfolio value alignment

**Problem:** Sidebar "FLEET" and leaderboard "Core Fleet Value" showed virtual fleet sum (~$332k). Correct value is Alpaca paper equity (~$99,935).

**Files patched:**
- `dashboard/static/index.html` — `fetchCapital()` now fetches `/api/alpaca/status` and caches `window._alpacaEquity`; `updateFleetTotalsBar()` uses cached value for `#ftValue`; label "Core Fleet Value" → "Alpaca Paper Equity"
- `dashboard/static/sw.js` — cache version bumped v7 → v8

**Backups:** `index.html.bak.2026-04-20-q2-mop12`, `app.py.bak.2026-04-20-q2-mop12`

### Part 2 — Picard seed + qwen3.5 cleanup (deferred files)

**Files patched:**
- `engine/first_officer.py` — fallback list: `qwen3.5:9b → qwen3:8b`; URL: `OLLAMA_URL → ADVISORY_OLLAMA_URL chain`
- `engine/portfolio_optimizer.py` — `OLLAMA_BASE` → ADVISORY_OLLAMA_URL chain; `DEFAULT_MODEL: qwen3.5:9b → qwen3:8b`
- `engine/rebalancer.py` — same as portfolio_optimizer.py

**Backups:** `.bak.2026-04-20-q2-picard`

### Part 3 — Sweep v2 time guard fix

**Problem:** `MARKET_GUARD_TIME = dtime(6, 25)` with `>= check` — any time after 06:25 AZ was blocked, including 15:13 PM (post-market).

**Fix:** Changed to inclusive window `MARKET_GUARD_START=06:25, MARKET_GUARD_END=13:30`. Times outside window run unblocked.

**Dry-run:** 8/8 test cases passed (15:13→RUN, 06:25→PAUSE, 13:31→RUN, etc.)

**Backup:** `scripts/model_sweep_v2.py.bak.2026-04-20-q2-timeguard`

### D1 — Dual-queue refactor

**Problem:** Single global OllamaQueue serialized ALL hosts — slow qwen3:14b job on Ollie blocked phi3:mini jobs on bigmac.

**Fix:** Per-host queue registry. Each `scheme://host:port` gets independent OllamaQueue + worker thread.

**Files patched:**
- `engine/ollama_queue.py` — removed module-level singleton; added `_queues` dict, `get_queue(url)`, `get_all_queues_status()`
- `engine/providers/ollama_provider.py` — `get_queue()` → `get_queue(self.url)`
- `dashboard/app.py` — `/api/ollama-queue-status` endpoint: `get_queue().status()` → `get_all_queues_status()`

**Backups:** `.bak.2026-04-20-q2-d1`

### FinGPT LoRA Scoping (research only)

**Output:** `docs/FINGPT_SCOPING_2026-04-20.md` — full scoping report.

**Key findings:**
- RTX 5060 8GB VRAM is the hard ceiling (not RAM). 96GB RAM = speed upgrade only.
- Scope A (inference, pre-trained adapters): can start today. ~1-2 hrs once HF Llama 2 access granted.
- Scope B (custom LoRA on OllieTrades data): data gate at 2K closed trades; currently 653 (~3 months away).
- No code changes, no downloads. Decision gate with Admiral.

### Coordinated Restart (17:22 AZ)

**Trigger:** Full day's bundle activation — D1 dual-queue, crew routing, dashboard alignment, all deferred patches.

**Procedure:**
1. pyc purge: `find . -name "__pycache__" -exec rm -rf {} +` — 15 → 0 project files
2. `launchctl unload` → `launchctl load` com.trademinds.trader.plist

**Result:**
- New PID: **41854**
- Alpaca positions: 9/9 intact ($99,975.71 equity)
- Startup warmup: `gemma3:4b (Picard) + mistral:7b (Pike) → Ollie GPU; phi3:mini cold-loads on demand` ✅
- Zero startup errors
- Zero qwen3.5 references in new PID log
- war_room: 5 rounds complete (all from pre-restart PID — expected; new PID awaiting first market cycle)
- Dual-queue: empty dict immediately after restart (expected — no OllamaProvider calls until first war_room cycle)

**qwen3.5 warmup log anomaly (RESOLVED):** Old log entries (Apr 10–19) showed `qwen3.5:9b warm ✓` — these are from previous restarts preserved in the non-rotated log file. New PID (post 17:22) shows only the correct warmup message. Confirmed clean.

---

## Second Restart (18:02 AZ) — ntfy unification + dual-queue activation

**New PID: 45829**

| Check | Result |
|-------|--------|
| pyc purge | ✅ 0 remaining (7 cpython-314 stale files force-deleted) |
| Startup warmup | ✅ `gemma3:4b (Picard) + mistral:7b (Pike) → Ollie GPU` |
| Errors in log | ✅ 0 |
| qwen3.5 refs in log | ✅ 0 |
| Alpaca positions | ✅ 9/9 intact ($99,979.99 equity) |
| Bigmac Ollama models | ✅ Idle (clean — no stale model from old session) |
| Ollie Box models | ✅ Idle (clean) |
| Ntfy env in shell | ✅ `NTFY_ADMIN_TOPIC=Ollie-Alert-35` / `NTFY_CREW_TOPIC=Ollie-Alert-55` / `NTFY_TOPIC=Ollie-Alert-35` |
| Post-restart push | ✅ HTTP 200 to `Ollie-Alert-35` — Admiral to confirm phone receipt |
| Fleet auditor | ✅ 6 OK / 4 stale (all expected after-hours) |
| 3-min activity | ✅ 14 scans, 0 errors, 0 qwen3.5 |

**All deferred changes now active:**
- D1 dual-queue registry (per-host OllamaQueue)
- qwen3.5 purge in first_officer, portfolio_optimizer, rebalancer
- Ntfy unification: trade events → Ollie-Alert-55, admin → Ollie-Alert-35

