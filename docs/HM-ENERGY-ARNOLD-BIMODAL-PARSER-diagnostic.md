# HM-ENERGY-ARNOLD-BIMODAL-PARSER — Diagnostic Report 2026-05-26

## Status: DEFERRED — not a parser bug; architectural fix required

## Premise reframe

The "bimodal confidence" pattern flagged in the cleanup sweep
(69% at 0.0, 13% at 1.0) is NOT a parser malfunction. It is an
artifact of two unrelated data-pollution sources mixed into the
`signals` table:

1. Error rows logged as signal rows with `confidence=0.0`
2. Legitimate energy-arnold output (4.4 → 1.0 saturation when it
   does fire)

## Confidence=0.0 breakdown (6,691 rows total)

| Reasoning pattern                                     | Count | Class      |
|-------------------------------------------------------|-------|------------|
| `HTTPConnectionPool host='localhost' port=11434 timed out` | 2,591 | ACTIVE     |
| `Error: 429 RESOURCE_EXHAUSTED` (Gemini quota)        | 1,859 | Historical |
| `Gemini quota exhausted — breaker reset [LEGACY]`     | 1,514 | Historical |
| `Thesis step failed: HTTPConnectionPool ... timed out` |  369 | ACTIVE     |
| `Error: 400 INVALID_ARGUMENT API Key not found`       |  101 | Historical |
| `Already holding QQQ stock. Skipping to avoid double-buy.` | 48 | LEGITIMATE |
| `Thesis step failed: Connection aborted RemoteDisconnected` | 46 | ACTIVE  |
| `HTTPConnectionPool ... Max retries exceeded`         |   25 | ACTIVE     |
| Other / scattered                                     |  138 | mixed      |

**Active issues**: ~3,030 connection failures targeting `localhost:11434`.
Per HM-CD-MIGRATE 2026-05-20, Ollama is now exclusively on Ollie Max
(192.168.1.168:11434) — localhost:11434 has nothing listening.

## Three architectural issues uncovered

### 1. Wiring drift (ACTIVE — primary culprit)

energy-arnold's provider chain still resolves to `localhost:11434`.
Source needs audit (likely `engine/agent_routing.py` or a stale
default in `config.AI_PLAYERS`). After HM-CD-MIGRATE the URL should
resolve to OLLIE_URL (192.168.1.168:11434).

### 2. Error-row-as-signal-row anti-pattern

Connection failures, quota exhaustions, and breaker-resets currently
INSERT into `signals` with `confidence=0.0` instead of logging to
trader_error.log + returning early. This:

- Pollutes ~6,400 of 9,000+ signal-history rows
- Skews every confidence-distribution / fire-rate / bimodality
  analysis downstream
- Caused the misleading "10,160 signals → 0% fire rate" finding in
  the 2026-05-26 cleanup sweep (Task 5)

### 3. DB model_id drift

- `ai_players.model_id = 'ministral-3:3b'`
- `crew_specialization.MODELS['energy-arnold']['model'] = 'qwen3:8b'`

Per CLAUDE.md drift catalog 2026-05-17 #1, HM-BN.1's silent-bypass
pattern (main.py per-call overrides shadowing DB) means runtime
likely uses qwen3:8b regardless — but the DB row is misleading to
any auditor querying ai_players for the canonical model.

## ai_players state (2026-05-26)

```
id             model_id        halt_mode  halted_at
energy-arnold  ministral-3:3b  active     (null)
```

## Recent trade activity

Last 5 trades on energy-arnold are ALL autopilot-driven (RSI trim,
dust cleanup, profit-take). All `confidence=0.0` because autopilot
writes placeholder confidence for its own auto-actions. Energy-arnold
itself has not fired an originating trade in weeks.

## Recommendations (deferred for Captain review)

**Priority 1 — Wiring fix** (~1-2h)
Audit `engine/agent_routing.py` build_all_providers() for
energy-arnold's resolved provider URL. Confirm it routes to
OLLIE_URL not localhost. If localhost is hardcoded somewhere,
flip to OLLIE_URL. Validates: zero new "localhost:11434 timed
out" rows post-fix.

**Priority 2 — Error-row gating** (~2-3h)
Find the call site that INSERTs an error reasoning into signals
with conf=0.0. Replace with `console.log` + `return None`.
Optionally backfill-delete the 6,400+ historical error rows after
Captain decision.

**Priority 3 — DB model_id reconcile** (~5 min)
`UPDATE ai_players SET model_id='qwen3:8b' WHERE id='energy-arnold';`
Cosmetic-only since runtime overrides; deferrable until
HM-BN.2/HM-CN cleanup wave touches the broader drift.

## NOT shipping tonight

This is architectural (wiring + anti-pattern + drift), not a
one-line parser fix per the original ticket framing. Three
separate sub-tickets would be the natural follow-up structure.

## Cross-references

- CLAUDE.md "RAM Discipline (post-MSI-migration 2026-05-20)" doctrine
- CLAUDE.md "Drift Catalog 2026-05-17" #1, #4, #5
- HM-CD-MIGRATE-GPU-RECOVERY (Ollama topology change)
- 2026-05-26 cleanup sweep Task 5 (rejection-rate finding that
  surfaced this investigation)
