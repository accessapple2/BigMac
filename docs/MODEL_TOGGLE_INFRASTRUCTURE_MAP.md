# Model Toggle Infrastructure Map

**Author:** Scotty 2.8.2 (Phase 3, HM-AY-α follow-up)
**Date:** 2026-05-08
**Hard rule applied throughout this doc:** **No state changes performed.**
Pure diagnostic. Every SQL shown is read-only or labelled
"Saturday-execute".

---

## 1. Toggle infrastructure location

OllieTrades has **two distinct control surfaces** layered on `ai_players`,
plus a global pause and a global fallback toggle in the `settings` table.
Confusing one with the other has caused at least three prior misreads
(grok-4 "halted but trading", "20 zombies from HM-AK", "Sniper sub-mode
toggle").

### 1.A — `ai_players` row state (per-player)

| Column | Used by | Effect when "off" |
|---|---|---|
| `is_paused` (0/1) | Dashboard ModelControl panel toggle button | **Scanning paused** for that player. Set/unset via `POST /api/model-control/pause/{player_id}` |
| `halt_mode` ('active'/'exit_only'/'full') | `paper_trader.py::buy()` and `sell()` execution gates (HM-A single source of truth) | **`exit_only`** = no opens, sells allowed. **`full`** = neither opens nor sells |
| `is_active` | Decorative (per 2026-04-25 audit) | n/a |
| `is_fallback` | Whether this player is currently routing through its `fallback_model` | n/a (not a kill, a routing state) |
| `fallback_model` | Free-local model that takes over when paid model is paused (cost-doctrine) | The fallback runs in the original's place |

**The toggle button on the dashboard writes `is_paused`, NOT `halt_mode`.**
A player can be `is_paused=1` (UI toggle off) AND `halt_mode='active'` —
those are independent states.

### 1.B — `settings` table (global)

| Key | Meaning | Endpoint |
|---|---|---|
| `pause_all` | Global scan pause across the whole fleet | `POST /api/model-control/pause-all` |
| `fallbacks_enabled` | When 1, paused-paid-models fall through to their `fallback_model` (free local Ollama) for scans | `POST /api/model-control/fallbacks` |

### 1.C — Hardcoded Python lists (in `dashboard/app.py`)

These are **not** in any DB. Editing them requires a code change + restart.

| Constant | File:Line | Purpose |
|---|---|---|
| `PROTECTED_AGENTS` | `dashboard/app.py:1431` | Roster lock — toggle endpoints refuse to mutate these via UI (returns 403). Bypassed by direct SQL. |
| `FLEET_ACTIVE` | `dashboard/app.py:1439` | Season 6.3 "Iron Condor King" / Sniper Mode active list (8 entries). Used at line 1922 as a leaderboard filter. |
| `SNIPER_AGENTS` | `engine/proving_ground.py:34` | Trial-tracker list (6 entries). Used to roll up daily Sniper scorecard. **Different from `FLEET_ACTIVE`**. |

### 1.D — API surface (per Phase 1 Auth route inventory queue)

| Method | Path | Mutating? | Effect |
|---|---|---|---|
| `GET` | `/api/model-control` | no | Returns full toggle state: `pause_all`, `fallbacks_enabled`, per-player `is_paused`+`is_halted` (derived) |
| `POST` | `/api/model-control/pause-all` | **yes** | Toggles global `pause_all` |
| `POST` | `/api/model-control/fallbacks` | **yes** | Toggles `fallbacks_enabled` |
| `POST` | `/api/model-control/pause/{player_id}` | **yes** | Toggles `is_paused` for one player. Refuses if in `PROTECTED_AGENTS` (403) or in Matrix-bridge (403) |
| `GET/POST` | `/api/settings/pause-all` | yes (POST) | Direct setter (older endpoint, same column) |
| `POST` | `/api/model-control/record-call/{player_id}` | yes | Increments per-day API call counter |
| `POST` | `/api/model-control/force-scan` | yes | Triggers a manual scan cycle |

### 1.E — Dashboard component

`dashboard/frontend/src/components/ModelControl.jsx` (renders the React
toggle UI). Frontend client wrappers at
`dashboard/frontend/src/api/client.js:57-59`. Static HTML buttons at
`dashboard/static/index.html:3705-3706` (`mc-pause-all-btn`,
`mc-fallbacks-btn`) per the production port-8080 path.

---

## 2. Current state inventory

Source: `SELECT id, halt_mode, is_paused, is_fallback, fallback_model FROM
ai_players ORDER BY halt_mode, id;` at 2026-05-08 ~03:55 UTC.

Classifications:
- **TOGGLE-ON** = currently active, signal-emitting / trade-firing
- **TOGGLE-OFF (cost)** = `halt_mode='full'` with paid-model `fallback_model`
  populated (deliberate cost-doctrine OFF; fallback Ollama covers the slot)
- **TOGGLE-OFF (paused)** = `is_paused=1` via dashboard toggle
- **RETIRED (genuine)** = `halt_mode='full'` with retirement reason but no
  paid-model cost angle (truly out of the fleet)
- **DEFERRED** = `halt_mode='exit_only'` (winding down — sells allowed)

### 25 active rows (`halt_mode='active'`)

| Player | is_paused | Notes / classification |
|---|---:|---|
| `alpaca-mirror` | 0 | TOGGLE-ON — internal book mirror of Alpaca paper |
| `capitol-trades` | 0 | TOGGLE-ON — Active 4 |
| `chekov` | 0 | TOGGLE-ON — momentum agent, threshold-muted (5.0) but row active |
| `cto-grok42` | 0 | TOGGLE-ON via fallback `deepseek-r1:7b` |
| `dalio-metals` | 0 | TOGGLE-ON via fallback `qwen3:8b` (Enterprise Computer route_mode=tracking) |
| `deepseek-7b-grok4` | 0 | TOGGLE-ON — confirmed in `FLEET_ACTIVE` (Spock scout). **178 signals last 24h** — top emitter. |
| `energy-arnold` | 0 | TOGGLE-ON — high-volume noise generator (92 signals 24h) |
| `enterprise-computer` | 0 | TOGGLE-ON — Dalio routing target |
| `mlx-qwen3` | 0 | TOGGLE-ON — 75 signals 24h |
| `navigator` | 0 | TOGGLE-ON — Chekov scout |
| `neo-matrix` | 0 | TOGGLE-ON — Active 4 (Ranked #2) |
| `ollama-coder` | 0 | TOGGLE-ON — Data scout |
| `ollama-deepseek` | 0 | TOGGLE-ON |
| `ollama-kimi` | 0 | TOGGLE-ON — Kimi/Moonshot AI monitor |
| `ollama-local` | 0 | TOGGLE-ON |
| `ollama-plutus` | 0 | TOGGLE-ON — Active 4 (Ranked #1: McCoy CSP) |
| `ollama-qwen3` | 0 | TOGGLE-ON — Dax scout |
| `ollie-auto` | 0 | TOGGLE-ON — **Sniper Mode / Fleet Commander gate**. Saturday's KILL target. 10 trades last 7d. |
| `options-sosnoff` | 0 | TOGGLE-ON via fallback `qwen3:8b` (45 signals 24h) |
| `qwen3-14b-pro` | 0 | TOGGLE-ON via fallback `qwen3:8b` |
| `qwen3-8b-flash` | 0 | TOGGLE-ON via fallback `qwen3:8b` — confirmed `FLEET_ACTIVE` member but **NOT** in current `FLEET_ACTIVE` list. See §3. |
| `qwen3-8b-sonnet` | 0 | TOGGLE-ON via fallback `qwen3:8b` |
| `red-alert` | 0 | TOGGLE-ON |
| `super-agent` | **1** | TOGGLE-OFF (paused). Routed player (Alpaca Paper portfolio id=1 per CLAUDE.md). Deliberate UI pause. |
| `webull` | 0 | TOGGLE-ON (`is_active=0` but row active for Alpaca-mirror writes — see broker-policy CLAUDE.md) |

### 5 deferred rows (`halt_mode='exit_only'`)

| Player | is_paused | Reason | Classification |
|---|---:|---|---|
| `dayblade-sulu` | 1 | "S6.3 bench: R:R 0.10, dormant since 2026..." | **TOGGLE-OFF (deliberate)** — see §5. Halt holding clean. |
| `gemini-2.5-flash` | 0 | "HM-AK 2026-05-07 dormant cleanup (2 open positions)" | RETIRED (genuine), winding down 2 positions |
| `gemini-2.5-pro` | 0 | "Retired S6.3 — qwen3:14b too heavy for box" | RETIRED (genuine) — RAM doctrine |
| `grok-3` | 0 | "S6 review: routing zombie, retired 2026-04-23" | RETIRED (genuine) |
| `ollama-llama` | 0 | "S6 review: routing zombie, retired 2026-04-23" | **Saturday's sunset target.** Already exit_only since 2026-04-23; still a `PROTECTED_AGENTS` member; still in `proving_ground.py::SNIPER_AGENTS`. |

### 20 halted rows (`halt_mode='full'`)

The Captain's hypothesis: some of these 20 are deliberate cost-OFF
toggles, not zombies. Evidence:

#### TOGGLE-OFF (cost) — 5 paid-model rows with active fallback (deliberate, KEEP)

| Player | fallback_model | Reason |
|---|---|---|
| `grok-4` | `deepseek-r1:7b` | HM-AK cost-doctrine (xAI charges; deepseek covers scan slot) |
| `claude-haiku` | `qwen2.5-coder:7b` | HM-AK cost-doctrine |
| `claude-sonnet` | `qwen3:8b` | HM-AK cost-doctrine |
| `gpt-4o` | `qwen3:8b` | HM-AK cost-doctrine |
| `gpt-o3` | `deepseek-r1:7b` | HM-AK cost-doctrine |

These are **not zombies.** They are intentionally OFF to spare paid API
charges; their `fallback_model` runs in their place when fallbacks are
enabled globally. Removing them would erase the Admiral's ability to
A/B these LLM lineages later. **Keep all 5.**

#### RETIRED (genuine) — 12 rows safely in halt_mode='full'

| Player | fallback_model | Reason — drawn from `halt_reason` text |
|---|---|---|
| `anderson-bcs` | (none) | HM-T-fleet bundle Option 1 |
| `covered-call` | (none) | HM-T-fleet bundle Option 1 |
| `mccoy-bps` | (none) | HM-T-fleet bundle Option 1 |
| `quark-ic` | (none) | HM-T-fleet bundle Option 1 |
| `ghost-kirk-0dte-bc` | (none) | Option-4 ghost bundle |
| `ghost-kirk-bc` | (none) | Option-4 ghost bundle |
| `ghost-long-call` | (none) | Option-4 ghost bundle |
| `ghost-naked-put` | (none) | Option-4 ghost bundle |
| `ollama-gemma27b` | (none) | HM-AK 2026-05-07 dormant cleanup (Ollama, no cost angle) |
| `ollama-glm4` | (none) | HM-AK 2026-05-07 dormant cleanup (Ollama, no cost angle) |
| `qwen-coder-haiku` | `qwen3:8b` | HM-AK dormant cleanup (Ollama; fallback decorative) |
| `qwen3-14b-grok3` | `deepseek-r1:7b` | HM-AK dormant cleanup |
| `qwen3-8b-4o` | `qwen3:8b` | HM-AK dormant cleanup |
| `qwen3-8b-o3` | `qwen3:14b` | HM-AK dormant cleanup |

Some of these (`qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`,
`qwen3-8b-o3`) **do** have a `fallback_model` populated, but they are
all local Ollama lineages — there is no API-cost reason to keep them
toggleable. Their `fallback_model` field is artifact of an earlier
fleet-roster pattern, not a deliberate cost-OFF.

#### Special case — 1 row

| Player | Notes |
|---|---|
| `dayblade-0dte` | `halt_mode='full', is_paused=1`. 2026-05-06 spread cannibalization halt (closed long 727P leg of bull_put_spread_v1 2 min after MLEG fill). **Operational halt-on-incident, not a roster decision.** Whether to thaw is open per CLAUDE.md HM-AF lifting decision. |

### Signal-emission gate status (last 24h)

```
deepseek-7b-grok4   178   2026-05-07 20:35:08
energy-arnold        92   2026-05-07 20:58:02
mlx-qwen3            75   2026-05-07 12:22:22
ollama-coder         75   2026-05-07 12:02:19
ollama-qwen3         59   2026-05-07 21:19:36
options-sosnoff      45   2026-05-07 21:34:42
ollama-plutus        25   2026-05-07 12:43:39
qwen3-8b-flash       25   2026-05-07 11:46:23
```

All 8 emitters are `halt_mode='active'`. **Zero signal leak from any
halt_mode!='active' row in the last 24h.** The 6 halted-but-emitting rows
flagged from the 7-day sweep all stopped emitting around 2026-05-05/06 —
their schedulers wound down on retirement. The 2026-05-03 reconciliation
finding ("is_halted does NOT gate signal emission") remains structurally
true, but in current operations no halted agent is leaking signals.

---

## 3. Roster doc gap analysis

The Captain noted Phase 1's roster reconciliation flagged
`deepseek-7b-grok4` and `qwen3-8b-flash` as "ghost agents." Looking at
the code:

| Row | In `dashboard/app.py:FLEET_ACTIVE`? | In `engine/proving_ground.py:SNIPER_AGENTS`? | Verdict |
|---|---|---|---|
| `deepseek-7b-grok4` | **yes** (line 1446 — Spock scout) | **yes** (line 37) | **Active fleet member** — 178 signals/24h. Just missing from the textual roster docs. |
| `qwen3-8b-flash` | **no** (line 1439 list does NOT include it) | **yes** (line 36 — Worf) | **Active Sniper participant**, not Alpha Squad. 25 signals/24h. Not a zombie. |

**Recommendation:** the gap is *documentation*, not state. These two
are toggle-ON, in `PROTECTED_AGENTS` (line 1432), and emitting signals.
Add them to the legacy roster docs at follow-up; do not retire either.

---

## 4. Genuine zombie candidates

**Subset of the 20 `halt_mode='full'` rows that are TRULY orphaned.**
Excludes the 5 paid-LLM cost-OFF toggles (§2 deliberate) and the
operational halt (`dayblade-0dte`).

The candidate set is the 12 RETIRED (genuine) rows from §2:
`anderson-bcs`, `covered-call`, `mccoy-bps`, `quark-ic`,
`ghost-kirk-0dte-bc`, `ghost-kirk-bc`, `ghost-long-call`,
`ghost-naked-put`, `ollama-gemma27b`, `ollama-glm4`,
`qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`, `qwen3-8b-o3`
(14 rows).

**No retirement action is in scope tonight.** Cleanup only goes through
the standard sacred-data-rule path (no DELETE; halt_mode stays `full`
forever; row preserved). No zombies require Saturday action.

---

## 5. dayblade-sulu verdict

Evidence:

```
halted_at:        2026-03-31 00:00:00
halt_mode:        exit_only
is_paused:        1   (toggle button explicitly OFF on dashboard)
halt_reason:      "S6.3 bench: R:R 0.10, dormant since 2026..."
trades since 2026-03-31: 6 (all on 2026-03-31 same day; halt-cleanup closes)
trades last 30d:  0
trades last 7d:   0
```

**Verdict: TOGGLE-OFF (deliberate).** Halt is holding cleanly. CLAUDE.md
`2026-05-03 fleet reality` doc says exactly this:

> `dayblade-sulu`: `is_halted=1` since 2026-03-31 (R:R 0.10 dormancy).

Not a halt-gap. Not a zombie. No Saturday action.

---

## 6. Sniper Mode halt mechanism

**Sniper Mode is the role of `ollie-auto`** — not a separate flag, not a
sub-mode, not a dashboard toggle. The proving-ground 30-day trial that
ends Saturday 2026-05-09 is implemented as:

- A **time window** (`engine/proving_ground.py::TRIAL_START = date(2026, 4, 10)`,
  `TRIAL_DAYS = 30`) which expires at end-of-day Saturday
- A **rollup** (proving-ground daily_trades) that aggregates trades from
  `SNIPER_AGENTS = [ollama-llama, qwen3-8b-flash, deepseek-7b-grok4,
  qwen3-14b-pro, ollama-plutus, neo-matrix]`
- The actual **routing** that puts trades on `ollie-auto`'s book lives
  in `engine/paper_trader.py::_EXECUTION_PORTFOLIO_BY_PLAYER` (per
  CLAUDE.md Two-Book Bridge Policy)

There is **no** `sniper_active` flag in `config.py`, no `SNIPER_MODE`
env var, no `is_sniper` column. The trial is a doc-and-rollup mechanism
on top of the normal halt/route stack.

### Saturday's KILL is exactly as `SNIPER_MODE_CLOSURE_PLAN.md` describes:

1. **Halt the agent.** Set `ai_players.halt_mode='full'` on `ollie-auto`
   via the standard CLAUDE.md halt SQL pattern (with `halted_at` +
   `halt_reason` populated). This closes the execution gate at
   `paper_trader.py::buy()` and `sell()` for that player.

2. **Remove from the fleet roster constants.** Edit
   `dashboard/app.py:1445` to drop `"ollie-auto"` from `FLEET_ACTIVE`.
   This stops the leaderboard / Sniper-display surfaces from treating
   ollie-auto as live.

3. **Stop or fix the proving-ground rollup.** Either stop scheduling
   the proving_ground job or rewrite to track only `ollie-auto`'s own
   trades (not the 6-agent SNIPER_AGENTS rollup which is the
   misleading metric source).

4. **Document.** Lessons file under `docs/lessons/SNIPER_MODE_CLOSURE_2026-05-09.md`.

5. **Sacred data rule:** **No DELETE / DROP.** Rows in `trades`,
   `portfolio_history`, `proving_ground.*` stay forever.

### Critical question answered

> Does setting `halt_mode='full'` on `ollie-auto` halt only Sniper, or
> does it also kill the agent's other capabilities?

`ollie-auto` **is** the Sniper / Fleet Commander gate. It has no other
"capabilities" to over-halt — its sole role is to route Alpha-Squad
signals through OllieScore ≥ 2.0 quality gating onto the Alpaca-paper
book (super-agent and ollie-auto both map to portfolio id=1 per the
Two-Book Bridge Policy). Halting it means the Sniper book stops taking
new entries. Other routed players (`super-agent`, `neo-matrix`,
`dalio-metals`) are unaffected — they have their own `ai_players` rows
and routing entries.

So: `halt_mode='full'` on ollie-auto **is** the Sniper KILL. No
under-halt, no over-halt.

### ollama-llama sunset mechanism

Already at `halt_mode='exit_only'` since 2026-04-23. Saturday's sunset
needs to:

1. Convert `halt_mode='exit_only'` → `'full'` (close trade gate fully)
2. Optionally remove from `dashboard/app.py:1432 PROTECTED_AGENTS` (so
   future toggles aren't artificially locked)
3. Optionally remove from `engine/proving_ground.py:34 SNIPER_AGENTS`
   (post-trial cleanup; the rollup goes away anyway under §6.3)
4. Document at `docs/lessons/AGENT_RETIRED_OLLAMA_LLAMA_2026-05-09.md`

Item 1 is mandatory; items 2-3 are doc-cleanup that can slip.

---

## 7. Saturday-readiness verdict

**Verdict: GO-WITH-DOC-FIX.**

The mechanism is unambiguous and the data is clean. Two doc-fix
follow-ups are recommended in the same Saturday window so the kill
ritual is consistent end-to-end:

### Mandatory for Saturday

| Action | Mechanism | Effect |
|---|---|---|
| Halt `ollie-auto` | Direct SQL: `UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP, halt_reason='[date] Sniper trial ended; KILL per SNIPER_MODE_CLOSURE_PLAN'` | Closes Sniper trade gate |
| Halt `ollama-llama` (sunset) | Same pattern, halt_mode='full' | Final retirement |
| Write 2 lesson docs | File create | Audit trail |

### Recommended doc fix in same window

| Action | File:Line | Effect |
|---|---|---|
| Remove `ollie-auto` from `FLEET_ACTIVE` | `dashboard/app.py:1445` | Sniper UI stops claiming ollie-auto live |
| Remove `ollama-llama` from `PROTECTED_AGENTS` | `dashboard/app.py:1432` | UI toggles can be edited freely post-retirement |
| Remove `ollama-llama` from `SNIPER_AGENTS` | `engine/proving_ground.py:34` | Trial rollup no longer attempts to query its trades |
| Stop/fix proving-ground rollup | `engine/proving_ground.py` scheduler entry | Bug-affected scorecard (`+1259.99` cumulative_return) stops accruing |

### Hold conditions (none currently met — verdict stays GO-WITH-DOC-FIX)

- Open ollie-auto position at 13:00 MST Saturday → **HOLD until closed**
- Open ollama-llama position → **HOLD until closed**
- Markets open Saturday 13:00 MST (won't be — stays GO)
- `_EXECUTION_ENABLED` somehow flipped to False on the trader before kill → run kill anyway, but flag for follow-up

### Pre-flight per `scripts/saturday_kill.sh` (Task 2)

The script will check:
- Markets closed
- All 4 `_EXECUTION_ENABLED` gates True (sanity)
- No in-flight ollie-auto or ollama-llama trade in `pending_trades`
- This doc's verdict is `GO` or `GO-WITH-DOC-FIX`
- Interactive `KILL` confirmation typed at prompt

---

## Halt condition

**No state changes performed.** This document is read-only research.
Saturday execute is gated on the Admiral, the script's `--execute`
flag, and the interactive `KILL` confirmation.
