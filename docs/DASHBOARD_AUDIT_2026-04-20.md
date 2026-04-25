# Dashboard Audit — 2026-04-20
**Time**: ~12:30 PM PDT (post zombie-hunt restart, PID 7491)
**Method**: Terminal API sweeps + source code analysis (no Chrome access from Claude Code CLI)
**Phase 1 note**: Chrome screenshots need manual confirmation from Steve

---

## CRITICAL DISCOVERY (not in original audit scope)

### setup_db.py re-seeds qwen3.5:9b on EVERY restart

`main.py:2431` calls `setup_db.setup()` at startup. `setup_db.py` contains
explicit **UPDATE statements** (lines 272–288) that hard-reset model_id back to
`qwen3.5:9b` for 10 agents:

```python
# setup_db.py:272–288 (runs every restart)
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

**Impact**: All of today's Wave 1+2 DB fixes were wiped when PID 7491 started at
12:21 PM. The DB currently has 10 rows with `qwen3.5:9b`. However, the live
process (PID 7491) has the correct in-memory providers from `initialize_arena()`
— trading is unaffected because war_room uses `arena.providers`, not the DB.

**Code paths affected by stale DB model_id (re-checked):**
- Crew files (daily_review_crew, strategy_crew, weekly_tuning_crew) — read
  model_id from DB per-call → will call bigmac qwen3.5:9b when they fire
- Dashboard endpoints (app.py /api/arena/ai-chat, /api/research/followup, etc.)
  that do per-call DB lookups
- Fallback.py — reads `fallback_model` column (separate column, that fix may
  have survived since setup_db doesn't reset fallback_model)

**Must fix before next restart**: Patch `setup_db.py` lines 272–288 to use the
correct Ollie models. This is a **Day 1 action** — every restart from now until
fixed will re-introduce the zombie.

---

## Phase 2 — API Endpoint Health

| Endpoint | HTTP | Size | Status |
|----------|------|------|--------|
| `/api/bridge/votes` | 200 | ✅ | Live votes present (Dr. McCoy HOLD today) |
| `/api/arena/leaderboard` | 200 | 7.6KB | ✅ Working (but Dax shows qwen3.5:9b — see above) |
| `/api/fear-greed` | 200 | 322B | ✅ Score 81 "EXTREME GREED" |
| `/api/kirk/advisory` | 200 | 2.1KB | ✅ 9 positions with HOLD advice |
| `/api/sector-heatmap` | 200 | 1.8KB | ✅ Returns sector data |
| `/api/market-movers` | 200 | 1.0KB | ✅ Gainers/losers/active present |
| `/api/gex` | 200 | 65B | ⚠️ Returns `{"status":…,"message":…}` — status msg only |
| `/api/congress/trades` | 200 | 10.4KB | ✅ Trade data present |
| `/api/universe/status` | 200 | 172B | ✅ |
| `/api/wb-team/advice` | 200 | 221B | ⚠️ Empty advisors — last scan 2026-04-12 (8 days) |
| `/api/picard/strategy` | 200 | 2.9KB | ⚠️ STALE — generated_at 2026-03-30 (21 days) |
| `/api/volume-radar?limit=5` | 200 | 1.7KB | ⚠️ All PULS, timestamps 2026-04-19 18:xx |
| `/api/sectors` | 404 | — | ❌ Route doesn't exist (HTML uses `/api/sector-heatmap`) |
| `/api/volume-alerts` | 404 | — | ❌ Route doesn't exist (HTML uses `/api/volume-radar`) |
| `/api/ready-room` | 404 | — | ❌ Route doesn't exist as bare path |
| `/api/advisory-team` | 404 | — | ❌ Route doesn't exist (actual: `/api/wb-team/advice`) |
| `/api/generated-indexes` | 404 | — | ❌ Route not registered |
| `/api/congress/` | 404 | — | ❌ Bare path; actual: `/api/congress/trades` |

---

## Phase 3 — Specific Issues

### 3a. Picard Weekly Thesis — STALE (Mar 30, 3 weeks old) 🔥

**Root cause: Logic bug in `main.py:3030`**

```python
# main.py:3014–3034
def run_picard_briefing():
    # ... Sunday 10-10:30 PM AZ check ...
    latest = get_latest_briefing()
    # BUG: This check returns if ANY briefing exists — regardless of age.
    # Comment says "less than 6 hours old" but there is NO age check.
    if latest.get("briefing") and latest.get("generated_at"):
        return                          # ← always returns, briefing from Mar 30 satisfies this
    generate_picard_briefing()
```

**Fires only Sunday 10:00–10:30 PM AZ**, every 30 minutes via `schedule`.
Missed: Apr 6, Apr 13, Apr 19 (3 Sundays) — each time saw the Mar 30 briefing
and returned without regenerating.

**Fix needed**: Add age comparison:
```python
from datetime import datetime, timezone
if latest.get("briefing") and latest.get("generated_at"):
    age_hours = (datetime.now(timezone.utc) - datetime.fromisoformat(...)).total_seconds() / 3600
    if age_hours < 168:  # < 1 week old
        return
```
Or simpler: delete the early-return entirely (just regenerate every Sunday).

**Risk**: Low. Picard is advisory-only (not a voter). Stale briefing has no
trading impact. Fix on Sunday before 10 PM window.

---

### 3b. Ready Room "21h 58m ago · STALE" — RESOLVED ✅

**Was**: Stale pre-restart. Last fire was Friday Apr 18, 3:30 PM ET (pre_close
slot). Weekend gap is **expected** — the scheduler correctly skips Sat/Sun
(weekday >= 5 check at `main.py:1570`).

**After restart**: Log at `12:24:40` confirms: `"Ready Room: REVERSAL_RISK
briefing saved (SPY $708.17, VIX 19.0, P/C 1.35)"`. New briefing generated.

**Schedule** (AZ time, weekdays only):
- 5:00 AM — pre_open
- 6:15 AM — post_open
- 9:00 AM — midday
- 12:30 PM — pre_close

The pre_close (12:30 PM AZ) fires in ~6 minutes from time of restart.
**No action needed.**

---

### 3c. Volume Radar — All PULS, timestamps Apr 19 🟡

**API**: `/api/volume-radar?limit=5` returns 200 with 5 entries, all:
```
symbol: PULS, detected_at: 2026-04-19 18:02–18:37
```

The scanner service (`com.trademinds.scanner`, PID 876) IS running per launchctl.
GEX and AI SaaS scanners both ran at 12:26 PM today. Volume radar data is from
yesterday's post-market hours (6 PM AZ = market closed).

**Likely causes** (investigation needed, not a crash):
1. No volume spikes detected today yet (market recovering from tariff shock —
   low unusual-volume candidates)
2. PULS is a thinly-traded biotech that triggers volume alerts easily; may be the
   only stock meeting threshold today
3. Volume radar may use a different update cadence than GEX

**Not zombie-related.** The scanner is running; it's a data/threshold question.

---

### 3d. Starfleet Advisory / Grok / Troi / Worf — Empty (not erroring) 🟡

**API**: `/api/wb-team/advice` returns 200 with:
```json
{"advisors":{"grok":[],"troi":[],"worf":[]},"meta":{"last_scan":"2026-04-12 17:06:05","model":"grok-4-0709"}}
```

**Not returning HTML / not 500** — the "Unexpected token '<'" was likely pre-
restart when the server was crashing. Now returns valid JSON.

**Root cause of empty advisors**: `last_scan: 2026-04-12` (8 days ago).
The advisory team scan uses `grok-4-0709` — this is xAI Grok API (paid model).
The scan has not run in 8 days. Possible causes:
- Grok API key expired or rate-limited
- `wb_advisory_team.py` daily cost cap hit (`daily_cap: 0.5`)
- Scheduled scan not firing (no com.trademinds.starfleet in launchctl)

**Was pre-existing before zombie hunt.** Warrants separate investigation.

---

### 3e. Sector Heatmap — Working ✅

`/api/sector-heatmap` returns 200 with 1.8KB of data. If the dashboard showed
"unavailable" pre-restart, it was a zombie-cascade symptom (server returning HTML
500s). **Resolved by restart.**

---

### 3f. "Generated Indexes (0)" — Route 404 🟢

`/api/generated-indexes` returns 404. The `generated_assets.py` module exists and
is imported by `app.py` (lines 14325+). The route is likely `/api/generated-assets`
or similar — needs checking in `app.py`. Low priority: feature may be undeployed
or dashboard label references wrong endpoint.

---

### 3g. Fear & Greed "Market closed" — NOT REPRODUCED ✅

`/api/fear-greed` returns `{"score":81,"label":"EXTREME GREED","signals":{...}}`.
Market IS correctly recognized as open. The "Market closed" display was likely a
zombie-cascade artifact (endpoint returning cached stale data during crash cycles).
**Resolved by restart.**

---

### 3h. Jadzia Dax leaderboard shows "qwen3.5:9b" 🔥

**Confirmed in live API**: `/api/arena/leaderboard` shows `model_id: qwen3.5:9b`
for `ollama-qwen3`.

**Root cause**: `setup_db.setup()` (called at every restart) resets the DB
back to qwen3.5:9b. The leaderboard reads `model_id` directly from DB at
`app.py:1903`. This will self-fix once `setup_db.py` is patched.

**Trading impact**: None. In-memory `arena.providers` has correct qwen3:8b.
Dashboard display is cosmetically wrong.

---

### Bonus: VIX `check_vix_spike` import error 🟡

Every scan cycle logs:
```
[12:26:24] VIX check error: cannot import name 'check_vix_spike'   main.py:484
```

`main.py:471` imports `check_vix_spike` from `engine.vix_monitor`, but
`vix_monitor.py` only exports `get_vix_term_structure`, `get_latest_vix_snapshot`,
etc. — no `check_vix_spike` function exists. Pre-existing bug; the VIX spike
alert never fires. Low impact (VIX is monitored by other code paths).

---

### Bonus: claude-sonnet routing to localhost:11434 🟡

Post-restart log at 12:25:35:
```
War room error for claude-sonnet: HTTPConnectionPool(host='localhost', port=11434):
Read timed out. (read timeout=180)
```

This is a **raw requests call** (180s timeout, not OllamaQueue's 300s) to
bigmac localhost. After today's initialize_arena() patches, claude-sonnet should
route to Ollie. The 180s timeout suggests a direct HTTP call path somewhere in
war_room.py or a provider subclass that bypasses OllamaQueue. Not qwen3.5:9b —
separate routing investigation needed.

---

## Phase 4 — Service Worker

`sw.js` exists at `dashboard/static/sw.js`, version `v4`:
```javascript
// v4
self.addEventListener('install', () => self.skipWaiting());
// On activate: delete ALL caches, then navigate all clients
```

This is an aggressive cache-busting worker — on activate it deletes all caches
and hard-navigates all open tabs. Version `v4` means 4 generations of cache
busting. If the browser still shows stale "Loading..." after hard refresh, the
sw.js itself may need a version bump to force re-install.

**Action**: After any dashboard code fix, bump `// v4` → `// v5` to force
service worker re-install across all sessions.

---

## Panel Status Summary

| Panel | Before Restart | After Restart | Status | Root Cause |
|-------|---------------|---------------|--------|------------|
| Bridge Votes | ❌ Error/offline | ✅ Live | Fixed | Zombie cascade (server crashes) |
| Fear & Greed | ❌ "Market closed" | ✅ Score 81 EXTREME GREED | Fixed | Zombie cascade |
| Ready Room | ⚠️ STALE 21h | ✅ Fired at 12:24 | Fixed | Weekend gap (expected) |
| Sector Heatmap | ❌ "unavailable" | ✅ Data present | Fixed | Zombie cascade |
| GEX | ❌ Error | ⚠️ Status msg only | Partial | GEX data init issue |
| Market Movers | ❌ Error | ✅ Data present | Fixed | Zombie cascade |
| Congress Trades | ❌ Error | ✅ 10.4KB data | Fixed | Zombie cascade |
| Kirk Advisory | ❌ Error | ✅ 9-position advice | Fixed | Zombie cascade |
| Dax Leaderboard | ❌ qwen3.5:9b | ❌ Still qwen3.5:9b | NOT FIXED | setup_db.py re-seeds on restart |
| Picard Thesis | ❌ Mar 30 stale | ❌ Still Mar 30 | NOT FIXED | Logic bug in run_picard_briefing |
| Starfleet Advisory | ❌ Error | ⚠️ Empty (200 OK) | Partial | Grok scan not firing (8 days) |
| Volume Radar | ❌ All PULS | ⚠️ Still PULS | No change | Scanner running; no new spikes |
| Generated Indexes | ❌ 0 | ❌ 404 | Unknown | Route may not be registered |
| VIX Spike Alert | ❌ Import error | ❌ Import error | Pre-existing | check_vix_spike missing in vix_monitor.py |

---

## Priority Fix List

### 🔥 CRITICAL — Fix today (before next restart)

**C1. `setup_db.py` lines 272–288: patch all 10 UPDATEs to use correct Ollie models**

This is the highest-priority fix. Every restart from now until patched re-introduces
the zombie DB state. Even though in-memory providers are correct, crew files and
dashboard per-call DB lookups will call bigmac qwen3.5:9b.

Affected lines: 272 (claude-sonnet), 274 (gpt-4o), 276 (grok-3), 280 (ollama-gemma27b),
281 (ollama-glm4), 282 (ollama-kimi), 283 (gemini-2.5-flash), 285 (options-sosnoff),
286 (ollama-qwen3), 288 (energy-arnold)

Correct values from `initialize_arena()` / earlier DB Wave 1+2 fixes (see SESSION doc).

- **Time**: 15 min
- **Risk**: Low — just updating string literals
- **Must do**: Yes, before any future restart

---

### 🟡 MEDIUM — This week

**M1. Picard briefing early-return bug (`main.py:3030`)**

Add a real age check (or remove the early-return entirely). Next fire window:
Sunday Apr 26, 10:00 PM AZ. Fix before then.

- **Time**: 5 min
- **Risk**: None (advisory only)

**M2. `check_vix_spike` import error (`main.py:471`, `engine/vix_monitor.py`)**

Either add `check_vix_spike()` to `vix_monitor.py`, or update the import to use
`get_latest_vix_snapshot()` and derive spike detection. Error fires every scan cycle.

- **Time**: 15 min
- **Risk**: Low

**M3. claude-sonnet routing to localhost:11434**

Find the direct requests call path (war_room.py or a provider subclass) and route
it to OLLIE_URL. Could generate qwen3.5:9b calls if bigmac ever has a qwen3.5 model
still warm.

- **Time**: 30 min investigation + 5 min fix
- **Risk**: Low

**M4. Starfleet Advisory (wb-team) scan not running**

Check if Grok API key is valid, daily_cap hit, or schedule broken. If Grok paid
tier is an issue, the fallback model is `qwen3:14b` (Ollie) — check why fallback
isn't kicking in.

- **Time**: 20 min
- **Risk**: Low (advisory only)

---

### 🟢 LOW — Post-close or weekend

**L1. Volume Radar all-PULS investigation**

Check threshold / scanner logic in tractor_beam / volume_radar. May just need
a lower relative-volume threshold or broader symbol universe.

- **Time**: 30 min
- **Risk**: None

**L2. GEX endpoint returning status message instead of data**

`/api/gex` returns `{"status":"…","message":"…"}` — likely an initialization
guard that returns early if GEX cache hasn't populated. Check `gex_scanner.py`
and `app.py` GEX endpoint. GEX scanner ran at 12:26 and refreshed 5 tickers —
the cache should be populated. May need a cache-read bug fix.

- **Time**: 20 min
- **Risk**: Low

**L3. Service worker version bump**

After any HTML/JS fix, bump `sw.js` `// v4` → `// v5` to force re-install.

- **Time**: 2 min

**L4. Generated Indexes route — register or remove**

Either register `/api/generated-indexes` in `app.py` (there's machinery in
`engine/generated_assets.py`) or remove the panel from the HTML if the feature
is undeployed.

- **Time**: 10 min

---

## Recommended Fix Sequence

Ordered by (impact × frequency) / (effort × risk):

1. **`setup_db.py` C1** — Highest impact, trivial effort, prevents every future
   restart from re-zombifying the DB. Do this NOW before the next restart.

2. **`check_vix_spike` M2** — Fires every scan cycle (every ~5 min). Silences
   a repeating error with minimal effort.

3. **Picard early-return M1** — 5-minute fix, prevents a 3-week briefing from
   growing to 4+ weeks. Do before Sunday Apr 26.

4. **claude-sonnet localhost routing M3** — Risk of re-introducing a qwen3.5
   call path if bigmac's Ollama ever loads the model. Medium urgency.

5. Everything else — post-close or weekend.

---

## Phase 1 Note (Chrome Visual Audit)

The following need manual confirmation in Chrome (not automatable from CLI):

- [ ] Hard refresh Cmd+Shift+R after restart — confirm "Loading..." panels cleared
- [ ] Service Worker tab in DevTools — confirm `v4` installed, no old versions pending
- [ ] Leaderboard: Dax still shows "qwen3.5:9b" label (expected until C1 fix)
- [ ] Ready Room badge — should show fresh timestamp after 12:24:40 fire
- [ ] Bridge view: vote panel shows Dr. McCoy / today's votes
- [ ] GEX panel display — does UI show the status message or handle it gracefully?

---

_Audit completed 2026-04-20 ~12:30 PM PDT by Claude Code._
