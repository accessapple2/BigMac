# HM-Q — `execution_status` vs `halted_emit` Investigation
*2026-05-04, Scotty investigation, no fixes applied*

## What `execution_status` is

A lifecycle tag on every `signals` row. Tracks the downstream fate of an emitted signal.

### Value distribution (61,362 rows total)

| execution_status | count | % |
|---|---|---|
| EXPIRED | 42,626 | 69.5% |
| SKIPPED | 11,157 | 18.2% |
| REJECTED | 7,169 | 11.7% |
| PENDING | 310 | 0.5% |
| SIMULATED | 83 | 0.1% |
| EXECUTED | 17 | 0.03% |

### Writers

- **Insert path** (`engine/paper_trader.py:1862-1879`, `save_signal`): default `_default_status = "SKIPPED" if signal == "HOLD" else "PENDING"`. So all HOLD signals are immediately marked SKIPPED at write time.
- **Update path** (`engine/paper_trader.py:1894-1904`, `set_signal_status`): callers update PENDING → EXECUTED / SIMULATED / REJECTED / LOG_ONLY after gate evaluation, with a `rejection_reason` payload (300-char cap).
- **Caller examples:** `engine/paper_trader.py:430,1027,1220,2228` (route-mode-dependent EXECUTED/SIMULATED), `engine/ai_brain.py:1299` (passes through external result).

### Readers

- `dashboard/app.py:3121,3127` — capability probe + leaderboard panel includes the field if present.
- `engine/_archive/2026-04-26/agent_coaching_report.py` — archived diagnostic.
- `engine/_archive/2026-04-26/neo_matrix_diagnostic.py` — archived diagnostic.

**Notable:** no production scoring/calibration code branches on `execution_status`. The dashboard surfaces it for human-eyes diagnostics; that's it.

### Inferred meaning of each value

| Value | Inferred meaning |
|---|---|
| **PENDING** | New non-HOLD signal awaiting gate evaluation |
| **SKIPPED** | HOLD signal — set unconditionally at write time (no downstream evaluation) |
| **REJECTED** | Failed a gate / mandate / position-limit / risk check (rich `rejection_reason`) |
| **EXPIRED** | Old PENDING that was never resolved — likely a background sweeper, but no code path for the writer was found in the audit (TBD). 70% of all rows. |
| **SIMULATED** | Paper-trade simulation route succeeded |
| **EXECUTED** | Real broker route succeeded (`route_mode == "trading"`) |
| **LOG_ONLY** | Signal not converted to trade; informational only (declared at line 35 but only 0 rows in DB — possibly a dead value) |

---

## What `halted_emit` is (HM-C)

A boolean flag on every `signals` row. Captures whether the player was halted (`halt_mode != 'active'`) **at the moment of emission**.

### Value distribution

| halted_emit | count | % |
|---|---|---|
| 0 | 60,219 | 98.1% |
| 1 | 1,143 | 1.9% |

Writers/readers are concentrated in `engine/halt_gate.py` (single source of truth: `HALTED_EMIT_FILTER = "halted_emit = 0"`). The HM-C comment at lines 51-58 documents the design: "scoring/calibration/leaderboard read paths" use this filter; raw display/forensic panels do not.

### Players responsible for halted_emit=1 rows

| player_id | halt_mode (current) | rows |
|---|---|---|
| ollama-llama | exit_only | 947 |
| dayblade-sulu | exit_only | 196 |

Both currently `exit_only` — emitting via the legacy code path that pre-dated full halt-gate adoption. HM-C backfilled their pre-fix-#1 leaks.

---

## Overlap with `halted_emit` — cross-tab

```
execution_status × halted_emit
EXPIRED    × 0  → 42,626
SKIPPED    × 0  → 10,093
REJECTED   × 0  →  7,090
SKIPPED    × 1  →  1,064
PENDING    × 0  →    310
SIMULATED  × 0  →     83
REJECTED   × 1  →     79
EXECUTED   × 0  →     17

(no halted_emit=1 rows reach EXPIRED, PENDING, SIMULATED, EXECUTED)
```

### Concrete row examples

- `execution_status='EXECUTED', halted_emit=0`: normal completed trade by an active player.
- `execution_status='SKIPPED', halted_emit=0`: HOLD signal from an active player (legitimate HOLD analysis input).
- `execution_status='SKIPPED', halted_emit=1`: HOLD signal from a halted player — should be excluded from BOTH halt scoring AND HOLD-rate analysis.
- `execution_status='REJECTED', halted_emit=1`: a non-HOLD signal from a halted player that the downstream gate also caught.

The 1,143 halted_emit=1 rows split as 1,064 SKIPPED + 79 REJECTED + 0 of any other status, which makes physical sense (halted players' signals never reach EXECUTED/SIMULATED/PENDING/EXPIRED).

---

## Architectural verdict: **A — Independent concerns**

Both columns measure orthogonal things:

| Question | Answered by |
|---|---|
| "What happened to this signal downstream?" | `execution_status` |
| "Was the player allowed to act when this signal was emitted?" | `halted_emit` |

### Why `halted_emit` cannot be derived from `execution_status`

`execution_status='SKIPPED'` is set for **every HOLD signal** regardless of halt state. It cannot tell you whether the SKIPPED HOLD came from an active player (legitimate input to HOLD-rate analysis) or a halted one (should be excluded). The information is not recoverable from execution_status alone.

### Why `halted_emit` cannot be derived from a `JOIN ai_players ON halt_mode`

`halted_emit` snapshots halt state **at emission time**. `ai_players.halt_mode` is mutable — when the Captain flips a player from `exit_only` back to `active` tomorrow, a JOIN-based filter would suddenly include all of yesterday's halt-period emissions in scoring. The whole point of HM-C was to freeze the at-emission-time bit so that historical scoring is reproducible. The DB has no audit log of halt_mode transitions to reconstruct from.

### Why HM-C was the right call

A reviewer this morning could have argued "just add `WHERE NOT (execution_status = 'SKIPPED' AND ai_players.halt_mode != 'active')` to scoring queries" — but that would fail for all the reasons above. HM-C added a column, but the column captures information that was otherwise lost. Different concern, different bit, both valid. Verdict A is unambiguous.

---

## Recommended action

**Nothing.** Both columns stay. HM-C work this morning shipped clean and is doing its job.

If anything, the lesson is "document the orthogonality" so the next person who notices the `SKIPPED`/`halted_emit=1` overlap doesn't propose to collapse them. A one-line annotation in `engine/halt_gate.py` near `HALTED_EMIT_FILTER` would settle it: *"Note: SKIPPED execution_status overlaps but does not subsume halted_emit — see docs/HM-Q_EXECUTION_STATUS_INVESTIGATION_2026-05-04.md."*

---

## Open questions for the Admiral

1. **What writes `execution_status='EXPIRED'`?** 42,626 rows (69.5% of the table) and the audit found no writer in code. Likely a sweeper script or a SQL job — worth tracing for completeness, but no functional issue.
2. **Is `LOG_ONLY` a dead value?** Declared in `paper_trader.py:35` and `:430` but 0 rows currently exist with that status. Either a never-taken code path, or a never-flushed write — minor data-quality oddity.
3. **Should `halted_emit` evolve to `halt_mode_at_emission TEXT`?** Today it's a boolean. If the Admiral ever wants to score `exit_only` separately from `full` (e.g. "exit_only players' HOLDs still count, full players' don't"), the boolean drops information. Defer; not urgent.
