# SCOTTY 2.8.2 — Phase 3 Status

> Three pre-Saturday tasks shipped (toggle map, kill script, auth Phase 1
> inventory) + status doc + push. **No live state mutated.** No halts
> performed. No `_EXECUTION_ENABLED` flips. No service restarts.

**Date:** 2026-05-08 (local 2026-05-07 evening MST)
**Branch:** `main`
**Saturday verdict:** **GO-WITH-DOC-FIX**
**Commits added this sprint:** 4 (Tasks 1–3 + this status report)

---

## 1. Tasks shipped

| # | Task | Status | Commit | Evidence |
|---|---|---|---|---|
| 1 | Model toggle infrastructure map | **SHIPPED** | `eec3320` | `docs/MODEL_TOGGLE_INFRASTRUCTURE_MAP.md` (379 lines, 7 sections); reads ai_players state for 50 rows; classifies 25 active / 5 exit_only / 20 halted; calls Saturday-readiness verdict **GO-WITH-DOC-FIX** |
| 2 | `scripts/saturday_kill.sh` orchestrator | **SHIPPED** | `9de5350` | 384 lines bash; default `--dry-run`; verified pre-flights all 4 checks pass: markets-closed ✓, verdict GO-WITH-DOC-FIX ✓, all 4 `_EXECUTION_ENABLED=True` ✓, no in-flight trades for either kill target |
| 3 | Auth Phase 1 route inventory | **SHIPPED** | `da365d9` | 232-line `docs/AUTH_PHASE_1_ROUTE_TIERS.md`; 54 routes mapped (49 from plan + 3 newly-flagged + 2 outliers); 3-tier rubric (TIER A 11 / TIER B 24 / TIER C 17 + 2); per-PR ship order |
| 4 | This status report | **SHIPPED** | (this commit) | — |

---

## 2. Toggle Infrastructure Map headline

### Verdict — `GO-WITH-DOC-FIX`

The Sniper kill mechanism is unambiguous and the data is clean. Sniper
Mode is the role of `ollie-auto` — not a sub-mode flag, not a dashboard
toggle. Saturday's KILL is exactly what `SNIPER_MODE_CLOSURE_PLAN.md`
describes: `UPDATE ai_players SET halt_mode='full' WHERE id='ollie-auto'`
plus the parallel ollama-llama sunset. Two doc-fix follow-ups recommended
in the same window:

- Remove `"ollie-auto"` from `dashboard/app.py:1445 FLEET_ACTIVE`
- Remove `"ollama-llama"` from `dashboard/app.py:1432 PROTECTED_AGENTS`
- Remove `"ollama-llama"` from `engine/proving_ground.py:34 SNIPER_AGENTS`
- Stop or fix the proving-ground daily rollup

### Captain's hypothesis confirmed (mostly)

> Some of the 20 `halt_mode='full'` agents from HM-AK may be intentional
> OFF toggles, not zombies.

**Confirmed: 5 of 20.** The 5 paid-LLM rows
(`grok-4`, `claude-haiku`, `claude-sonnet`, `gpt-4o`, `gpt-o3`) all have
populated `fallback_model` columns and HM-AK reasons consistent with
deliberate cost-doctrine OFF, not retirement. These belong to the toggle
infrastructure as **TOGGLE-OFF (cost)** entries — keep them.

**Refuted: 12 of 20.** The remaining halted rows are genuine retirements
(HM-T-fleet bundle, Option-4 ghost bundle, ollama-gemma27b /
ollama-glm4 dormant cleanup, dayblade-0dte spread cannibalization
operational halt). No cost-doctrine angle.

### Captain's hypothesis confirmed (clean)

> The two "ghost agents" flagged in roster recon (`deepseek-7b-grok4`,
> `qwen3-8b-flash`) are likely current ON-state toggles whose names
> just aren't in legacy roster docs.

**Confirmed cleanly.** Both are `halt_mode='active'`, in
`PROTECTED_AGENTS`, in either `FLEET_ACTIVE` (deepseek) or
`SNIPER_AGENTS` (qwen3-8b-flash). 178 + 25 signals in the last 24h
respectively. Doc gap, not state gap.

### dayblade-sulu

**TOGGLE-OFF (deliberate).** `halted_at=2026-03-31`, `is_paused=1`,
zero trades in last 30 days. The 6 trades on the halt date itself were
position-close cleanups. Halt is holding cleanly. No Saturday action.

### Sniper Mode kill mechanism

Setting `halt_mode='full'` on `ollie-auto` halts **only** the Sniper
role — `ollie-auto` has no other capabilities to over-halt. Other
routed players (`super-agent`, `neo-matrix`, `dalio-metals`) have
their own `ai_players` rows and routing entries; they are unaffected.

### Signal-emission gate sweep
Last 24h: zero halted-row signal leaks. The 6 halt_mode='full' rows
that were flagged from a 7-day signal sweep all stopped emitting around
2026-05-05/06 — their schedulers wound down on retirement. Structural
finding from `2026-05-03` reconciliation ("is_halted does NOT gate
signal emission") still true, but currently dormant.

---

## 3. Saturday readiness — dry-run output excerpt

```
[2026-05-07T21:12:30-0700] Saturday KILL orchestrator starting (mode=dry-run)
[Pre-flight 1] ✓  Markets closed (DOW=4, 21:12).
[Pre-flight 2] ✓  Toggle-map verdict: GO-WITH-DOC-FIX — verdict permits execution.
[Pre-flight 3] ✓  All 4 _EXECUTION_ENABLED gates True.
                  strategies/bull_call_spread_v1.py  _EXECUTION_ENABLED=True ✓
                  strategies/bear_put_spread_v1.py   _EXECUTION_ENABLED=True ✓
                  strategies/bull_spread_v1.py       _EXECUTION_ENABLED=True ✓
                  strategies/executor.py             _EXECUTION_ENABLED=True ✓
[Pre-flight 4]    ollie-auto    last-6h trades=0  open positions=5
[Pre-flight 4]    ollama-llama  last-6h trades=0  open positions=0
[Pre-flight 4] ⚠   ollie-auto has 5 open positions. They will not auto-close
                   on halt; Admiral closes per Saturday checklist.

[ Sniper kill — ollie-auto ]
SQL: UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP,
     halt_reason='[date] Sniper trial ended (Day 30/30) ...' WHERE id='ollie-auto';

[ ollama-llama sunset ]
SQL: UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP,
     halt_reason='[date] ollama-llama sunset ...' WHERE id='ollama-llama';

[ Source-of-truth list edits — NOT auto-applied; Admiral runs these ]
  dashboard/app.py:1445   remove "ollie-auto" from FLEET_ACTIVE
  dashboard/app.py:1432   remove "ollama-llama" from PROTECTED_AGENTS
  engine/proving_ground.py:34  remove "ollama-llama" from SNIPER_AGENTS

Mode is dry-run — NO SQL executed, NO files written.
Run with --execute on Saturday after 13:00 MST to perform.
```

**Key observation:** `ollie-auto` has **5 open positions**. Per the
script's pre-flight 4, halt does not auto-close them — they need to be
closed manually before or during the Saturday window per
`SNIPER_MODE_CLOSURE_PLAN.md` Section 3 ritual.

---

## 4. Roster doc updates needed

The toggle-page reality has drifted from the legacy roster docs. Models
listed as "current/active in fleet" but missing from `CLAUDE.md` /
roster docs:

| Player | Active surface | Doc home it should land in |
|---|---|---|
| `deepseek-7b-grok4` | `FLEET_ACTIVE` (Spock scout), `SNIPER_AGENTS` (Spock), 178 signals/24h | Active 4 / Sniper Squad section |
| `qwen3-8b-flash` | `SNIPER_AGENTS` (Worf), `PROTECTED_AGENTS`, 25 signals/24h | Sniper Squad section |
| `mlx-qwen3` | `is_active=1, halt_mode='active'`, 75 signals/24h, MLX provider | Provider catalog (MLX path) |
| `ollama-deepseek` | active (no recent signals); CLAUDE.md mentions deepseek-r1:7b but row is `ollama-deepseek` | Naming reconciliation |
| `ollama-kimi` | active per `project_kimi_monitor.md` memory | Add to fleet roster |
| `ollama-local` | active | Verify role |
| `options-sosnoff` | active via `qwen3:8b` fallback, 45 signals/24h | Add to fleet — looks like options-flow scout |
| `super-agent` | `is_paused=1` (toggle-OFF), routed to Alpaca paper portfolio id=1 | Confirm if this is the real-money-tier slot |
| `cto-grok42` | active via `deepseek-r1:7b` fallback | CTO advisory slot — verify |
| `qwen3-14b-pro` | active via `qwen3:8b` fallback | Confirm role |
| `qwen3-8b-sonnet` | active via `qwen3:8b` fallback | Confirm role |
| `red-alert` | active | Confirm role |

**No retirement is recommended for any of these** — the gap is
documentation, not state. Recommend a follow-up sprint that does a
sweep through CLAUDE.md and current roster docs to bring them in sync
with `ai_players`.

---

## 5. Genuine zombie candidates

**14 rows** at `halt_mode='full'` are truly orphaned (NOT toggle-OFF
cost-doctrine). Listed for the eventual cleanup sprint, but **no
cleanup is in scope** — sacred-data rule says no DELETE. They sit at
`halt_mode='full'` indefinitely.

| Row | Reason class |
|---|---|
| `anderson-bcs`, `covered-call`, `mccoy-bps`, `quark-ic` | HM-T-fleet bundle 2026-05-05 |
| `ghost-kirk-0dte-bc`, `ghost-kirk-bc`, `ghost-long-call`, `ghost-naked-put` | Option-4 ghost bundle 2026-05-05 |
| `ollama-gemma27b`, `ollama-glm4`, `qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`, `qwen3-8b-o3` | HM-AK 2026-05-07 dormant cleanup |

(`dayblade-0dte` is `halt_mode='full'` from the 2026-05-06 spread
cannibalization operational halt — that's a separate decision per
HM-AF flag-lift review, not a zombie.)

---

## 6. Wall clock + commit count

| | |
|---|---|
| Commits added this sprint | **4** (3 task commits + status report) |
| Lines added | ~1,000 across docs / 1 script |
| Tests added | 0 (Phase 1a will add when wiring routes) |
| Production routes touched | **0** |
| `paper_trader.py` / `main.py` / gate files / strategy files touched | **0** |
| `dashboard/app.py` touched | **0** |
| Service restarts | **0** |
| Halt mutations | **0** |
| `_EXECUTION_ENABLED` flips | **0** (all 4 still True, untouched) |
| `DROP TABLE` calls | **0** |
| Force pushes | **0** |
| Secret values generated | **0** |

---

## 7. Outstanding for Admiral go

### Immediate (Saturday 2026-05-09 after 13:00 MST)

1. **Close 5 ollie-auto open positions** before halt (per
   `SNIPER_MODE_CLOSURE_PLAN.md` Section 3 ritual)
2. Run `bash scripts/saturday_kill.sh --execute` — type `KILL` to confirm
3. Apply the 3 doc-fix list edits manually:
   - `dashboard/app.py:1445` remove `"ollie-auto"` from `FLEET_ACTIVE`
   - `dashboard/app.py:1432` remove `"ollama-llama"` from `PROTECTED_AGENTS`
   - `engine/proving_ground.py:34` remove `"ollama-llama"` from `SNIPER_AGENTS`
4. Restart the trader so `dashboard/app.py` constants reload
5. Acknowledge the post-fire ntfy

### Auth Phase 1 prep (any time)

1. Generate the 3 secrets per `docs/AUTH_SETUP.md`:
   - `OLLIETRADES_TOTP_SECRET` (pyotp.random_base32 + QR enroll)
   - `OLLIETRADES_SERVICE_TOKEN` (secrets.token_urlsafe 48)
   - `OLLIETRADES_RECOVERY_KEY_HASH` (sha256 of token_urlsafe 32)
2. Verify `pytest tests/test_auth.py -v` still 11/11 green from the
   trader venv
3. Identify all programmatic dashboard callers that POST to TIER A
   routes and grant them the service token (likely 0-3 callers)
4. Phase 1a — wire `Depends(verify_admin_token)` on the 11 TIER A
   routes (one PR, ~12 LOC). Smoke test all 3 auth sources.
5. Phase 1b through 1e per `AUTH_PHASE_1_ROUTE_TIERS.md` Section 6.

### Roster doc reconciliation (any time)

Update `CLAUDE.md` / `docs/ROSTER_RECONCILIATION.md` with the 12 active
players currently missing from the legacy roster (per §4 above). No
state changes — just docs catching up to reality.

### Post-Saturday cleanup (next sprint)

- Decide per `dayblade-0dte` HM-AF flag lift (currently
  `halt_mode='full'` from 2026-05-06 spread cannibalization)
- Sniper Mode v2 if Admiral wants the concept revived
  (notional-parity sizing, fresh 30-day window — see
  `SNIPER_MODE_CLOSURE_PLAN.md` Section 2 HARD-EXTEND option)

---

## 8. Push readiness

4 new commits ahead of `origin/main`. No untracked production code,
no modified gate / strategy / `paper_trader.py` / `dashboard/app.py`
files, no secrets in any diff, no halt mutations. Push authorized in
the Captain's Phase 3 brief — proceeding with Task 4 push.
