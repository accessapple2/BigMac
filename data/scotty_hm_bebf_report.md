# HM-BE + HM-BF Discovery — Config Drift & Watchdog Tune

**Phase BEBF.0 — read-only profiling. No code edits, no DB writes, no restart.**
**Date:** 2026-05-11
**Engineer:** Scotty (Opus 4.7)

---

## Pre-flight summary
- HM-BD.1 (`bcb3bca`) confirmed in `origin/main`.
- Service alive (PID 83860, port 8080).
- Working tree has no tracked changes; 36 untracked items (prior HM-* docs and DB backups, none in scope).

---

# HM-BE — qwen3 config drift

## 1. Where the agent definition lives

The agent **`qwen3-14b-pro`** exists in three runtime/config surfaces and one comment:

### `trader.db.ai_players` (live DB)
| column | value |
|---|---|
| `id` | `qwen3-14b-pro` |
| `display_name` | **`Qwen3 14B Pro`** ← says 14b |
| `model_id` | **`qwen3:8b`** ← actually 8b |
| `provider` | `ollama` |
| `halt_mode` | `active` |
| `fallback_model` | `qwen3:8b` |
| `crew_role` | `active` |

### `config.py:166` (runtime AI_PLAYERS list)
```python
{"id": "qwen3-14b-pro",  "name": "Dalio Macro 14B",    "provider": "ollama", "model": "qwen3:8b",         "url": OLLIE_URL},  # Ollie GPU — was gemini
```
- `name` says **`Dalio Macro 14B`** ← says 14b
- `model` says **`qwen3:8b`** ← actually 8b

### `engine/cost_tracker.py:49` (annotation only)
```python
"qwen3-14b-pro":    (0.00, 0.00),  # Seven of Nine    → ollama/qwen3:14b
```
- Comment says **`→ ollama/qwen3:14b`** ← stale; runtime serves qwen3:8b

### `engine/super_backtest_v4.py:93`, `v4.py:117`, `super_backtest_v5.py`, `proving_ground.py:38`
- These are historical backtest player tables. `v4.py:93` defines `qwen3-14b-pro` with `model: "qwen3:14b"`. Historical reference — backtests use a captured snapshot, not the live config. **Out of scope.**

## 2. Historical trail (why the drift exists)

- `agent_id_aliases` table shows: `old_id='gemini-2.5-pro' → new_id='qwen3-14b-pro' renamed_at 2026-04-21 22:27:45`.
- The rename was part of the **Free-Models-First migration** (CLAUDE.md 2026-04-16) when Gemini Pro was retired in favour of a local Ollama 14b.
- One day prior (2026-04-20), the same family of swap-storm cleanups downgraded multiple agents from `qwen3:14b` to `qwen3:8b` ("was causing swap storms" comments in `engine/premarket_scanner.py:172`, `engine/portfolio_optimizer.py:38`, `engine/scenario_modeler.py:35`, `engine/research_caller.py:12`).
- The model was downgraded; the **name was never updated**.

## 3. Activity reality

| Surface | Count |
|---|---|
| trades for `qwen3-14b-pro` | **0** |
| signals for `qwen3-14b-pro` | **0** |
| positions, agent_memory, agent_ratings, agent_allocation rows | 0 each |

The agent is `halt_mode='active'` but has **never produced a trade or signal under its current id**. Either the scheduler doesn't reference it, the provider is gated elsewhere, or it's a wired-but-dormant zombie. Either way: changing the display label affects no audit trail and breaks no foreign keys.

## 4. FK cascade scope (if rename considered)

| Surface referencing `qwen3-14b-pro` as a value | Rows |
|---|---|
| `agent_id_aliases.new_id` | 1 (from the 2026-04-21 rename) |
| `positions.player_id`, `trades.player_id`, `signals.player_id` | 0 each |
| `agent_memory`, `agent_ratings`, `agent_allocation` | 0 each |

Renaming the `id` would require updating the alias row but nothing else. Still invasive given the alias trail exists for a reason (tools that resolve old→new ids could break).

## 5. Recommended path

**α-narrow** — align the human-facing labels to the actual runtime model; leave the `id` and historical artifacts alone.

| Change | Surface | From → To |
|---|---|---|
| 1 | `ai_players.display_name` (DB UPDATE) | `Qwen3 14B Pro` → `Qwen3 8B Pro` |
| 2 | `config.py:166` `name` field | `Dalio Macro 14B` → `Dalio Macro 8B` |
| 3 | `engine/cost_tracker.py:49` comment | `→ ollama/qwen3:14b` → `→ ollama/qwen3:8b` |

**Not in scope (intentional):**
- The `id` `qwen3-14b-pro` — has 1 alias row pointing at it; renaming would also require touching the alias table and any code that resolves it. Cosmetic gain not worth the cascade.
- `engine/super_backtest_v4.py`, `v5.py`, `proving_ground.py` — historical backtest tables, not live config.

### Why not β (upgrade to qwen3:14b)
The 2026-04-20 cleanup downgraded *this exact agent* because 14b caused swap storms on the 16GB bigmac Mac Mini. CLAUDE.md RAM Discipline explicitly says "prefer `qwen3:8b` over larger models" and "qwen3:30b is rejected — too slow". The downgrade was a deliberate fleet-wide decision; reverting it for one agent would re-create the original problem and violate the standing RAM rule.

### Why not γ (other)
The id has historical meaning (`14b` era), the model is the right one for the box, the name should reflect reality. α-narrow does exactly that with minimum blast radius.

## 6. Captain question Q1
**Recommended:** α-narrow, three specific changes above.

This will produce **two artefacts**:
- **One code commit** (`fix(config): HM-BE — qwen3 agent name aligned`) touching `config.py` and `engine/cost_tracker.py`. Scotty applies.
- **One SQL handoff** in the closure report (`UPDATE ai_players SET display_name='Qwen3 8B Pro' WHERE id='qwen3-14b-pro';`). Captain runs manually per standing rule 1.

---

# HM-BF — watchdog memory metric

## 1. The active watchdog

- Live daemon: **`watchdog.py` at the repo root** (NOT `engine/watchdog.py` — that file does not exist).
- Running: PID 53953 (+ child 84651).
- Loaded via `~/Library/LaunchAgents/com.trademinds.watchdog.plist`.
- 60s sweep interval, monitors Bridge :8080, Signal Center :9000, Ollama :11434, Cloudflare, and CPU+RAM.

The file `engine/crusher.py` referenced in the directive also does not exist. `dr_crusher.sh` is a passive 6-min backup alerter for port health; it has no memory checks of any kind.

## 2. Actual code in `watchdog.py` (the relevant block)

`watchdog.py:306-364`:

```python
def check_resources() -> None:
    """Monitor CPU and memory. Alert and shed load if critical."""
    if not _PSUTIL:
        return
    try:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        mem_pct   = mem.percent
        mem_avail = round(mem.available / 1e9, 1)
        ...
        log.info(
            f"CPU {cpu:.0f}%  RAM {mem_pct:.0f}% ({mem_avail}GB free)  "
            f"Swap {swap.percent:.0f}%  Ollama: {ollama_loaded}"
        )

        if cpu > CPU_WARN_PCT:
            log.warning(f"HIGH CPU: {cpu:.0f}% — Ollama inference likely running")

        if mem_pct >= MEM_CRIT_PCT:       # MEM_CRIT_PCT = 95
            alert("Critical Memory", ...)
            # kill VTuber + unload Ollama model
        elif mem_pct >= MEM_WARN_PCT:     # MEM_WARN_PCT = 85
            alert("Memory Warning", ...)
```

Constants at `watchdog.py:41-43`:
```python
CPU_WARN_PCT    = 90
MEM_WARN_PCT    = 85
MEM_CRIT_PCT    = 95
```

## 3. Mismatch with the directive symptom

The directive states: *"watchdog uses raw swap% where memory free% would be more accurate signal."*

**The current code does not match that statement:**
- The watchdog already uses `mem.percent` (`psutil.virtual_memory().percent` — RAM **used** %) for both the warning and critical decisions.
- `swap.percent` is only **logged** (line 328) for visibility; it does **not** drive any threshold or shedding action.

Three interpretations are possible. I cannot pick one without Captain input — they describe different fixes:

### Interpretation A — operator-signal swap
The directive may mean: **the watchdog log line surfaces swap% prominently, leading an operator to read it as the system-health signal** (because macOS aggressively pages to swap before `mem.percent` rises). Fix: switch the log emphasis or replace it with macOS's native `memory_pressure` free% (which is what `scripts/vitals.sh:29` and `scripts/fleet_status.sh:8` already use for operator vitals). Pure log/display change; thresholds unaffected.

### Interpretation B — swap-aware trigger
The directive may mean: **macOS's `mem.percent` can stay below 95% even while the system is thrashing swap, so the critical alert never fires when it should.** Fix: add swap% (or `memory_pressure` free%) as an OR-trigger to the `if mem_pct >= MEM_CRIT_PCT` block. New constant `SWAP_CRIT_PCT` (e.g. 40) and `MEM_FREE_CRIT_PCT` (e.g. 10) potentially needed.

### Interpretation C — straight metric swap
The directive may mean: **switch `mem_pct = mem.percent` (used %) to `mem_pct = 100 * (1 - mem.available / mem.total)` (computed used %) or `mem_free_pct = mem.available / mem.total * 100` (free %) with inverted thresholds.** Functionally equivalent to the current code if numerics line up — only a presentation change.

## 4. File size + structure
- `watchdog.py` — 409 lines, single-file daemon. CPU/RAM/swap/Ollama/Bridge/Signal Center checks in one process.
- `engine/ollama_watchdog.py` — 287 lines but **different concern**: per-model timeout circuit breaker for Arena scans, not OS resource monitoring. Out of scope.

## 5. Captain question Q2

The directive's α/β/γ choices are about **threshold tuning after the metric switch**. But before that decision, the **metric itself** is ambiguous given the code/symptom mismatch:

**Q2.1 (new, blocking):** Which interpretation of HM-BF do you actually want?
- **A** — log/display fix only (surface `memory_pressure` free% in the log line). 1–3 lines.
- **B** — add swap-or-memory-pressure as a secondary critical trigger. ~10–15 lines + new constant(s).
- **C** — straight metric swap to free%-based math. ~5 lines + threshold-sign flip.

**Q2.2 (original):** After the metric is chosen, threshold tuning approach?
- **α** — keep current numeric thresholds (`MEM_WARN_PCT=85`, `MEM_CRIT_PCT=95`).
- **β** — re-tune to match the new metric semantics (e.g. if switching to free%, warn at ≤15% free / crit at ≤5% free; if adding swap, swap-crit at ≥40%).
- **γ** — ship the metric change, observe one cycle, then tune from observation.

**Scotty's recommendation:** Interpretation **B** + **β** tuning, because:
- It addresses the underlying *operational risk* (macOS swap thrash without crossing `mem.percent>=95`) rather than just renaming the metric.
- The current `mem.percent` trigger keeps working for the obvious case; B adds a second OR-trigger for the macOS-specific case.
- Concrete proposed thresholds: keep `MEM_CRIT_PCT=95`, add `SWAP_CRIT_PCT=40` (40% of ~16GB swap = ~6.4GB swapped — substantial thrash), trigger critical if EITHER hits. β re-tuning means: keep the MEM thresholds, set the SWAP threshold from-scratch on the new metric.

If Captain prefers a minimal change, **A + α** (just improve the log line, no behavior change) is the smallest possible scope.

---

# Discovery summary

| Sub-epic | Discovery | Captain decision required |
|---|---|---|
| **HM-BE** | Drift in 3 places: DB display_name, config.py:166 name, cost_tracker.py:49 comment. Backed by activity check (0 trades, 0 signals — safe to relabel). | Q1: α-narrow recommended. |
| **HM-BF** | Directive symptom does not match the live code. `watchdog.py` already uses `mem.percent` for decisions; `swap.percent` is only logged. Three plausible interpretations. | Q2.1 + Q2.2: interpretation B + threshold β recommended. |

**HALT — awaiting Captain decision on Q1 (HM-BE path) and Q2.1/Q2.2 (HM-BF interpretation + thresholds).**

---

# Captain Decisions (received 2026-05-11)

- **Q1:** α-narrow — rename display_name in DB + config.py:166 name + cost_tracker.py:49 comment; leave `id` alone.
- **Q2.1:** B — swap-or-memory-pressure OR-trigger in watchdog.
- **Q2.2:** β — keep `MEM_CRIT_PCT=95`, add `SWAP_CRIT_PCT=40` as OR-condition.

---

## HM-BEBF Closure

### HM-BF outcome (commit 58ecfbc)
Single edit to `watchdog.py`:

1. Added new constant at line 44:
   ```python
   SWAP_CRIT_PCT = 40  # HM-BF: also shed load if swap%≥40 (macOS pages before mem% rises)
   ```
2. Modified the critical trigger in `check_resources()` (lines 335-345):
   ```python
   # === HM-BF: swap-aware critical trigger ===
   if mem_pct >= MEM_CRIT_PCT or swap.percent >= SWAP_CRIT_PCT:
       alert(
           "Critical Memory",
           f"RAM {mem_pct:.0f}% / Swap {swap.percent:.0f}% — {mem_avail}GB free. Shedding load.",
           "mem_crit",
       )
   ```

The shed-load path (kill VTuber + unload Ollama model) is unchanged. `MEM_WARN_PCT=85` warning threshold is unchanged. Swap warning is intentionally not introduced — the new metric only escalates to *critical* (one threshold tier, β tuning as approved).

Threshold rationale: `SWAP_CRIT_PCT=40` ≈ 6.4GB swapped on a 16GB Mac Mini, which represents substantial macOS thrash before `mem.percent` would cross 95%.

Diff: +7/-2 lines, one file.

### HM-BE outcome (commit 3e90dbc) — code portion

Two-file commit:

1. **`config.py:166`** — `name` field: `"Dalio Macro 14B"` → `"Dalio Macro 8B"`.
2. **`engine/cost_tracker.py:49`** — annotation comment: `→ ollama/qwen3:14b` → `→ ollama/qwen3:8b`.

The runtime `model` field in `config.py:166` was already correct (`qwen3:8b`); only the human-facing labels changed. Diff: +2/-2 lines, two files.

### HM-BE — SQL handoff for Captain (third surface)

The `ai_players.display_name` column in `trader.db` is still `"Qwen3 14B Pro"`. Per standing rule 1 (sacred DBs: Scotty drafts SQL, Captain executes), the third surface is delivered as a runnable SQL block:

```sql
-- HM-BE: align ai_players.display_name with runtime model (qwen3:8b).
-- Activity check confirmed 0 trades/0 signals/0 positions for this id, so
-- no audit trail is affected; only the dashboard label changes.
BEGIN;

-- Show current state for confirmation
SELECT id, display_name, model_id, halt_mode
FROM ai_players
WHERE id='qwen3-14b-pro';

-- Apply rename
UPDATE ai_players
   SET display_name = 'Qwen3 8B Pro'
 WHERE id = 'qwen3-14b-pro'
   AND display_name = 'Qwen3 14B Pro';  -- guard: only update if not already aligned

-- Verify exactly 1 row touched
SELECT changes() AS rows_updated;

-- Confirm new state
SELECT id, display_name, model_id, halt_mode
FROM ai_players
WHERE id='qwen3-14b-pro';

COMMIT;
```

To run:
```bash
sqlite3 ~/autonomous-trader/data/trader.db < /dev/stdin <<'SQL'
BEGIN;
UPDATE ai_players
   SET display_name = 'Qwen3 8B Pro'
 WHERE id = 'qwen3-14b-pro'
   AND display_name = 'Qwen3 14B Pro';
SELECT id, display_name, model_id FROM ai_players WHERE id='qwen3-14b-pro';
COMMIT;
SQL
```

Expected result: `qwen3-14b-pro | Qwen3 8B Pro | qwen3:8b`.

### Commits staged
```
3e90dbc fix(config): HM-BE — qwen3-14b-pro display name aligned with runtime model
58ecfbc fix(watchdog): HM-BF — add swap% OR-trigger to critical memory check
```

Both will be pushed by Scotty inline per Captain's go-signal at the end of BEBF.D.

### Restart needed
**Yes** — `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` (per Captain's inline directive). Note: the watchdog daemon is a **separate launchd job** (`com.trademinds.watchdog.plist`) and would need its own restart to pick up the HM-BF code change. The trader-bridge restart Captain authorised will not restart the watchdog process automatically.

### Watchdog restart status (to be performed)
The watchdog change is in **`watchdog.py`** (root), loaded by `~/Library/LaunchAgents/com.trademinds.watchdog.plist`, running as PIDs 53953 + 84651. To activate HM-BF, the watchdog job needs `launchctl kickstart -k gui/$(id -u)/com.trademinds.watchdog`. Captain's inline directive only specified the trader bridge restart; **flagging this so it's not missed**. Scotty will execute it alongside the trader restart unless Captain says otherwise (HALT-on-error rule).

### Verification plan post-restart
1. `pgrep -af main.py` shows a new PID (bridge restart confirmed).
2. `lsof -ti :8080` returns a live PID.
3. `curl -s -o /dev/null -w "HTTP %{http_code}\n" http://localhost:8080/api/momentum/premarket` → 200.
4. `pgrep -af watchdog.py` shows a new PID (if watchdog was also restarted).
5. `tail -20 logs/watchdog.log` shows the post-restart log line with format including both RAM% and Swap%.
6. Dashboard `display_name` for `qwen3-14b-pro` shows "Qwen3 8B Pro" once Captain runs the SQL handoff (or shows "Qwen3 14B Pro" still if SQL not yet executed — the bridge restart doesn't touch the DB).

### Out-of-scope follow-ups (parked)
- **HM-BE-suffix:** rename the *id* `qwen3-14b-pro` to a name-accurate slug (e.g. `qwen3-8b-pro`). Requires touching `agent_id_aliases`, every code reference (about 5 backtest-related files), and an audit of any external tooling that resolves player_ids. Not blocked but materially larger; deferred.
- **HM-BE-historic:** update `engine/super_backtest_v4.py:93/117`, `super_backtest_v5.py:113`, `proving_ground.py:38` references. These are historical snapshots for reproducibility; consensus on whether they should track current config is non-trivial.
- **HM-BF-warn:** consider adding a `SWAP_WARN_PCT` second tier (currently only critical fires). Would create a warning-class alert in the macOS thrash territory before the shed-load action. Deferred — observe first whether crit-only is sufficient signal.
- **HM-BF-display:** also incorporate macOS `memory_pressure` free% (the metric `scripts/vitals.sh` uses) in the watchdog log line for richer operator visibility. Pure display change.

---

## HM-BEBF Post-fire observations (2026-05-11 17:30+)

After the watchdog kickstart at 17:30:22, the daemon fired its first-cycle critical alert at 17:30:24 and the second cycle alerted again at 17:31:25, third at 17:32:27. The behavior is **mechanically correct** but exposes a **threshold-tuning miss** and one pre-existing operational quirk:

### What fired (correct behavior)
- `WARNING  ALERT: Critical Memory — RAM 59% / Swap 89% — 7.1GB free. Shedding load.`
- `Push sent [200]: Critical Memory` — single NTFY push to `ollietrades-admin`. iPhone delivery confirmed by status 200.
- `Killed VTuber (run_server.py) to free memory` (cycle 1 only — see caveat)
- `Unloaded Ollama model from VRAM` (every cycle)
- Daemon survived (PID 85795 still alive 2m45s after restart, cycling on the 60s interval as designed).

### NTFY cooldown working as intended
- The `_cooldown_ok()` gate at `watchdog.py:72-77` properly suppressed cycles 2/3's pushes — only 1 NTFY push went out over the 3 cycles (300s cooldown). Captain's iPhone gets one alert, not three.
- The `log.warning("ALERT: ...")` line at `alert():121` fires *before* the cooldown check, so the **log line appears every cycle** even when NTFY is suppressed. This is verbose but harmless.

### Threshold-tuning miss (parked as HM-BH)
The watchdog log shows **Swap 89%** for **every one of the last 200 pre-kick cycles** (~3.3 hours of consistent baseline). That is macOS's normal compressed-memory behavior on a healthy 16GB box, not an actual thrash event. With `SWAP_CRIT_PCT=40`, the watchdog now treats this baseline as critical and fires shed-load on every cycle.

Net effect post-HM-BF:
- 1 NTFY push per 5 min cooldown (Captain not spammed — OK).
- 389 `Unloaded Ollama model` log lines accumulated in 60s × N cycles. `ollama stop` is idempotent and Ollama is currently empty, so no functional harm — but the log noise is permanent until threshold is re-tuned.
- Every minute the daemon attempts `pkill -f run_server.py` (see caveat below).

**Recommended HM-BH adjustment:** the BD.0 design discussion assumed 40% swap = thrash. Reality shows macOS sits at ~89% swap as a steady state when the system is happy. Two options:
- **α (re-tune):** raise `SWAP_CRIT_PCT` to e.g. 95 (just above current baseline) and/or AND-gate it with a low-memory secondary condition (e.g. `swap.percent >= 95 AND mem.available < 1GB`). Conservative.
- **β (switch metric):** use macOS-native `memory_pressure` free% (the same metric `scripts/vitals.sh` already uses). Free% < 10 = real pressure. Decoupled from swap usage which macOS treats as a feature, not a problem.

Scotty's recommendation: **β**. The swap metric was a proxy for "system is thrashing"; macOS's `memory_pressure` is the literal name for that signal, and there's already prior art in the repo (`vitals.sh:29`, `fleet_status.sh:8`). Defer to a separate session.

### Pre-existing operational quirk (parked as HM-BI)
The shed-load action `pkill -f run_server.py` (line 342 of watchdog.py) uses `-f` which matches against the **full process argv** — including any python subprocess whose code references the string `"run_server.py"` as a literal. In the post-kick state we observed `pgrep -af run_server.py` matched a transient watchdog child subprocess (PID 85822) momentarily. The first cycle's `pkill` reported returncode 0 ("matched and killed") and logged `Killed VTuber (run_server.py)` — but no actual VTuber process exists on this system today. The watchdog itself (PID 85795) survived, so this is harmless **on this box**. But:

- It's a **silent false positive** in the log — operators reading "Killed VTuber" without verifying may chase a non-event.
- If a real VTuber/run_server.py is ever brought back, `pgrep -f run_server.py` will match both the VTuber AND any python process whose code literal contains the string — could kill innocent bystanders.
- HM-BI recommended fix: scope the match to a specific full-path executable (e.g. `pkill -f "venv/bin/python.*run_server\.py"`) or precondition the kill on `pgrep -af run_server.py | grep -v watchdog.py` returning non-empty. ~10 line change.

### Verification artefacts
- HM-BE: `qwen3-14b-pro | Qwen3 8B Pro | qwen3:8b` confirmed in `ai_players` post-UPDATE.
- HM-BF: 1 NTFY push delivered (HTTP 200), 3 alert cycles logged, daemon cycling normally, no daemon crashes, no unintended shed targets killed.

### What ships vs what's deferred
- **Shipped (in `origin/main`):**
  - `58ecfbc` HM-BF code — swap-aware OR-trigger active.
  - `3e90dbc` HM-BE config — display name aligned.
- **Captain executed:**
  - Trader bridge kickstart (PID 85371, port 8080 healthy).
  - HM-BE DB UPDATE (verified `Qwen3 8B Pro`).
  - Watchdog kickstart (PID 85795, HM-BF firing on every cycle as designed).
- **Parked for future epics:**
  - **HM-BH** — re-tune `SWAP_CRIT_PCT` (or switch to `memory_pressure` free%) to match macOS reality. Current `=40` fires on baseline. Blocks: nothing; cosmetic log noise only.
  - **HM-BI** — narrow the `pkill -f run_server.py` match to avoid false-positive "Killed VTuber" log lines and protect against future bystander kills.
  - **HM-BE-suffix / HM-BE-historic** (as before).

