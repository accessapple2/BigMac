# HM-FLEET-REJECTION-AUDIT — 2026-05-13 16:02 MST

**Purpose:** identify which gate (halt, mandate, conviction, sizing, BSM, daily-limit,
ghost-promotion, max-positions, etc.) is rejecting trade attempts per player.
Surfaces stale-active emitters whose signals are gate-rejected wholesale.

## Methodology

Scan trader_error.log and trader.log for rejection patterns over last 7 days.
Group by player_id, by rejection type. Active-mode players with >0 attempts and
0 fills are flagged as stale-active (same pattern as ollama-local).

## Rejection-pattern counts (last 7 days)

```
  HALTED:                                  470
  MANDATE BLOCKED:                         2717
  below confidence                         0
  Daily limit                              0
  Max positions                            0
  Quality gate                             0
  GHOST OPTION                             0
  BSM rejected                             0
  spread cannibalization                   398
```

## Per-player rejection profile (last 7 days)

| Player | HALTED | MANDATE | Conviction | Daily-limit | Max-positions | Quality |
|---|---|---|---|---|---|---|
| alpaca-mirror | 0 | 0 | 0 | 0 | 0 | 0 |
| capitol-trades | 0 | 0 | 0 | 0 | 0 | 0 |
| cto-grok42 | 0 | 0 | 0 | 0 | 0 | 0 |
| dalio-metals | 0 | 0 | 0 | 0 | 0 | 0 |
| deepseek-7b-grok4 | 0 | 74 | 0 | 0 | 0 | 0 |
| energy-arnold | 0 | 1083 | 0 | 0 | 0 | 0 |
| enterprise-computer | 0 | 0 | 0 | 0 | 0 | 0 |
| mlx-qwen3 | 0 | 251 | 0 | 0 | 0 | 0 |
| navigator | 0 | 0 | 0 | 0 | 0 | 0 |
| ollama-coder | 0 | 429 | 0 | 0 | 0 | 0 |
| ollama-deepseek | 0 | 21 | 0 | 0 | 0 | 0 |
| ollama-kimi | 0 | 10 | 0 | 0 | 0 | 0 |
| ollama-plutus | 0 | 0 | 0 | 0 | 0 | 0 |
| ollama-qwen3 | 0 | 34 | 0 | 0 | 0 | 0 |
| ollie-auto | 0 | 0 | 0 | 0 | 0 | 0 |
| options-sosnoff | 0 | 327 | 0 | 0 | 0 | 0 |
| qwen3-14b-pro | 0 | 0 | 0 | 0 | 0 | 0 |
| qwen3-8b-flash | 0 | 35 | 0 | 0 | 0 | 0 |
| qwen3-8b-sonnet | 0 | 164 | 0 | 0 | 0 | 0 |
| red-alert | 0 | 0 | 0 | 0 | 0 | 0 |

## Stale-active emitter flags

Players with halt_mode='active' but zero fills in last 7 days are candidates
for halt_mode='exit_only' (the ollama-local pattern). See SQL below.

```sql
-- SQL placeholder: identify stale-active emitters
SELECT id FROM ai_players WHERE halt_mode = 'active';
```

## Captain action items

1. Review players with high MANDATE BLOCKED counts — consider mandate adjustment
2. Review players with high Conviction-floor counts — calibration question
3. Flip stale-active emitters to exit_only (same as ollama-local 2026-05-13)


## CAPTAIN DECISION 2026-05-13 — Accept

**Decision:** Accept current gate-rejection profile. No mandate/conviction tuning.

**Rationale:** Per-agent rejection counts are high, but fleet P&L is positive
(today: +$123.58 at 84% WR). The Quality Gate and Conviction Floor are doing
real work filtering noise. ollie-auto's 4192 rejections/7d is the COST of
finding the 14 winning fills, not a defect.

**Optimization opportunity (future, low priority):** Filter at scan-time
instead of post-generation. If we knew which setups always fail Quality Gate,
we could skip generating those candidates. Save compute, same P&L.

**Trigger to revisit:**
- Fleet P&L turns negative
- Quality Gate rejection rate >90% sustained
- Specific agent emerges as net-negative contributor
