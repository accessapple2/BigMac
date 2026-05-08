# Roster Reconciliation Diagnostic (HM-AY-γ)

**Author:** Scotty 2.4 (Claude Code Opus 4.7)
**Date:** 2026-05-07 ~20:30 MST
**Status:** **Diagnostic only — no DB writes, no code edits.**
**Source audit:** `docs/SCOTTY_AUDIT_2.md` Section G (Agent Edge Scorecard) + Top-10 #4.
**Goal:** make the documented roster, the FLEET_ACTIVE list, and what actually trades all match.

---

## 1. Three Sources of "the Roster"

| Source | What it controls | Authoritative? |
|---|---|---|
| **`data/trader.db::ai_players`** (25+ rows) | Halt state (`halt_mode`), legacy `is_active` flag, crew_role | DB-of-record for halts. `is_active`/`crew_role` fields are decorative per CLAUDE.md (HM-S finding). |
| **`dashboard/app.py:1439 FLEET_ACTIVE`** (8 names) | Dashboard leaderboard cards + scan-loop iteration | Per CLAUDE.md HM-AK-β.2 (commit `83d5684`), 5 dashboard iteration sites already filter by `halt_mode='active'`. The hardcoded list at line 1439 is now mostly cosmetic for the leaderboard panel. |
| **`docs/SCOTTY_AUDIT_2.md` "Active 9"** (per audit prompt) | Documentation of the working core | Drift from reality flagged in Top-10 #4. |

---

## 2. `ai_players` Dump

### `halt_mode='active'` (25 rows)

| id | crew_role | is_active | Trade volume 30d | Notes |
|---|---|---|---|---|
| alpaca-mirror | mirror | 1 | n/a (broker mirror) | Real-state mirror of Alpaca paper book |
| capitol-trades | advisory | 1 | 62 | Active |
| chekov | active | 1 | **0** | **Zero trades ever, muted per S6.3** |
| cto-grok42 | active | 1 | 14 | Unrostered |
| dalio-metals | advisory | 1 | 6 | Enterprise Computer tracking only (route_mode='tracking') |
| deepseek-7b-grok4 | active | 1 | **133** | **HIGHEST-VOLUME PLAYER ON SHIP, NOT IN FLEET_ACTIVE** |
| energy-arnold | advisory | 1 | 12 | Per 2026-05-03 reconciliation: "noise generator, IMPROVE pending" |
| enterprise-computer | active | 1 | 0 | Routing destination, not a player |
| mlx-qwen3 | advisory | 1 | n/a | Active per CLAUDE.md May 3 fleet reality reconciliation |
| navigator | active | 1 | 2 | Idle since 04-09 |
| neo-matrix | active | 1 | 12 | 0/3 30d, idle since 04-22 |
| ollama-coder | advisory | 1 | **0** | **Utility role, 1,759 signals, 0 trades ever** |
| ollama-deepseek | advisory | 1 | 0 | Advisory only |
| ollama-kimi | advisory | 1 | 22 | Advisory but trading? Verify role |
| ollama-local | advisory | 1 | 0 | Advisory |
| ollama-plutus | active | 1 | **96** | **Alpha engine. 82.7% WR, +$4,108/30d** |
| ollama-qwen3 | advisory | 1 | 101 | Active scout |
| ollie-auto | commander | 1 | 74 | Sniper Mode — KILL recommended |
| options-sosnoff | advisory | 1 | 4 | Wheel/Troi (VIX-gated) |
| qwen3-14b-pro | active | 1 | 0 | Verify usage |
| qwen3-8b-flash | active | 1 | **78** | **Unrostered, second-highest volume non-fleet** |
| qwen3-8b-sonnet | advisory | 1 | 0 | Advisory |
| red-alert | active | 1 | 0 | Alert agent, not trader |
| super-agent | advisory | 1 | 8 | Routing destination |
| webull | active | **0** | n/a | Schwab/real account mirror; is_active=0 already |

### `halt_mode='exit_only'` (5 rows)

| id | halted_at | Notes |
|---|---|---|
| dayblade-sulu | 2026-03-31 | R:R 0.10 dormancy |
| gemini-2.5-flash | 2026-05-07 17:18 | HM-AK halt today |
| gemini-2.5-pro | 2026-04-30 | Pre-retired |
| grok-3 | 2026-04-25 | Pre-retired |
| ollama-llama | 2026-04-25 | **−$5,536 / 90d — finish exit (Top-10 #6)** |

### `halt_mode='full'` (15+ rows)

All HM-AK family (12 zombies halted today) plus older retirements (anderson-bcs, covered-call, dayblade-0dte, ghost-* family, gpt-4o, gpt-o3, claude-haiku, claude-sonnet, mccoy-bps, qwen3-14b-grok3, qwen3-8b-4o, qwen3-8b-o3, qwen-coder-haiku, ollama-gemma27b, ollama-glm4, quark-ic, grok-4).

---

## 3. `dashboard/app.py:FLEET_ACTIVE` (8 names — current)

```python
FLEET_ACTIVE = [
    # SEASON 6.3 "IRON CONDOR KING" — 180-day backtest: +572% realistic
    "dayblade-sulu",        # PRIMARY — Spread King — HALTED (exit_only since 2026-03-31)
    "ollama-plutus",        # PRIMARY — Income — ACTIVE (alpha engine)
    "ollie-auto",           # Fleet Commander (gate) — Sniper Mode KILL pending
    "deepseek-7b-grok4",    # Scout — Spock — ACTIVE (133 trades/30d)
    "navigator",            # Scout — Chekov — IDLE since 04-09
    "capitol-trades",       # Scout — Congress — ACTIVE
    "ollama-coder",         # Scout — Data — UTILITY (0 trades ever)
    "ollama-qwen3",         # Scout — Dax — ACTIVE
]
```

**Header comment is from S6.3 IRON CONDOR KING era** (~Apr 23). The fleet has shipped a lot since (HM-T, HM-AB, HM-AK family). Header is stale.

---

## 4. Audit Prompt's "Active 9"

```
ollie-auto · navigator · chekov · ollama-llama · ollama-plutus
ollama-qwen3 · ollama-coder · neo-matrix · capitol-trades
```

Drifted from FLEET_ACTIVE: **+chekov, +ollama-llama, +neo-matrix; -dayblade-sulu, -deepseek-7b-grok4**.

---

## 5. Three-Way Diff

| Player | In ai_players (active)? | In FLEET_ACTIVE? | In audit "Active 9"? | Trades 30d | Reality |
|---|---|---|---|---|---|
| ollama-plutus | ✓ | ✓ | ✓ | 96 | **TRUE WORKHORSE — keep everywhere** |
| ollie-auto | ✓ | ✓ | ✓ | 74 | Sniper KILL pending |
| capitol-trades | ✓ | ✓ | ✓ | 62 | Keep |
| ollama-qwen3 | ✓ | ✓ | ✓ | 101 | Keep |
| navigator | ✓ | ✓ | ✓ | 2 | **WATCH — silent dead** |
| ollama-coder | ✓ | ✓ | ✓ | 0 | **CUT — utility role, not trader** |
| chekov | ✓ | ✗ | ✓ | 0 | **CUT — never traded** |
| neo-matrix | ✓ | ✗ | ✓ | 12 | **WATCH — recovering** |
| ollama-llama | exit_only | ✗ | ✓ | 9 | **CUT — finish exit (Top-10 #6)** |
| dayblade-sulu | exit_only | ✓ | ✗ | 0 | **REMOVE from FLEET_ACTIVE** — halted since 03-31 |
| **deepseek-7b-grok4** | ✓ | ✓ | **✗** | **133** | **HIGHEST-VOLUME — must be in audit doc** |
| **qwen3-8b-flash** | ✓ | ✗ | ✗ | **78** | **PHANTOM — second-highest** |
| cto-grok42 | ✓ | ✗ | ✗ | 14 | Phantom — verify role |
| gemini-2.5-flash | exit_only | ✗ | ✗ | 20 | Halted today (HM-AK) |
| ollama-kimi | ✓ | ✗ | ✗ | 22 | Advisory but trading — verify |
| energy-arnold | ✓ | ✗ | ✗ | 12 | Noise generator |
| grok-4 | full | ✗ | ✗ | 11 | **Halted but still trading?** Verify halt-emit gate |

---

## 6. Zombies in Registry Beyond HM-AK's 12

CLAUDE.md HM-AK halted 12 today. Beyond those, the older `halt_mode='full'` rows (15+) have been there since 2026-04-25 → 2026-05-07. None are firing per cross-check (no signals/trades in last 24h).

**Two oddballs worth flagging:**
- **`grok-4`** (`halt_mode='full'`, 11 trades in last 30d). Either the halt didn't take effect or the 30-day window includes pre-halt trades. Investigate.
- **`enterprise-computer`** (`halt_mode='active'`, `crew_role='active'`, 0 trades). It's a routing destination (`portfolios.id=5`, dalio-metals tracker), not a player. `is_active=1` is decorative. Reclassify `crew_role='infrastructure'` or similar.

---

## 7. Proposed Reconciliation

**Goal:** rosters match reality. Three changes, no DB row deletions (sacred-data rule).

### Change A — Update `dashboard/app.py:FLEET_ACTIVE`

Remove halted (`dayblade-sulu`, `ollama-llama`-eventual). Add the unrostered top-volume (`deepseek-7b-grok4`, `qwen3-8b-flash`).

```python
# Proposed FLEET_ACTIVE (post-reconciliation, post-Sniper-KILL):
FLEET_ACTIVE = [
    # SEASON 7 working core — reconciled to actual 30d trade volume 2026-05-07
    "ollama-plutus",        # PRIMARY — alpha engine (CSP, 82.7% WR, +$4108/30d)
    "ollama-qwen3",         # SECONDARY — scout (101 trades/30d, recovering)
    "deepseek-7b-grok4",    # SECONDARY — top-volume (133 trades/30d) [PROMOTED from phantom]
    "qwen3-8b-flash",       # SECONDARY — second-volume (78 trades/30d) [PROMOTED from phantom]
    "capitol-trades",       # SCOUT — Congress signals
    # Removed: dayblade-sulu (exit_only since 03-31), ollie-auto (Sniper KILL pending),
    # navigator (idle 04-09), ollama-coder (utility, not trader)
]
```

### Change B — Update CLAUDE.md "Active 9" → "Active 5 + Watch + Cut"

Replace prompt's stale "Active 9" with reality-based:

| Bucket | Players | Reason |
|---|---|---|
| **Workhorses (5)** | ollama-plutus · ollama-qwen3 · deepseek-7b-grok4 · qwen3-8b-flash · capitol-trades | All ≥50 trades/30d; producing alpha |
| **Watch (3)** | neo-matrix · navigator · cto-grok42 | Marginal volume, decision pending |
| **Sunset (2)** | ollama-llama · dayblade-sulu | Already exit_only; Top-10 #6 finishes |
| **Demote-to-utility** | chekov · ollama-coder · ollama-deepseek · qwen3-8b-sonnet · ollama-local · ollama-kimi · red-alert · enterprise-computer · alpaca-mirror · webull | Advisory/infra/halt — not traders. Set `crew_role='utility'` or `'infra'` to make this legible. |

### Change C — DB metadata flag (no row deletion)

Add a `crew_role='utility'` reclassification migration that distinguishes traders from utility/infra roles. Pure documentation in the data model; no behavior change. SQL pattern:

```sql
UPDATE ai_players SET crew_role='utility'
 WHERE id IN ('ollama-coder','chekov','ollama-deepseek','qwen3-8b-sonnet',
              'ollama-local','red-alert');
UPDATE ai_players SET crew_role='infra'
 WHERE id IN ('enterprise-computer','alpaca-mirror','webull');
UPDATE ai_players SET crew_role='advisory'
 WHERE id IN ('dalio-metals','options-sosnoff','energy-arnold','ollama-kimi');
```

**No `DELETE` statements anywhere** in this proposal.

---

## 8. Open Questions for Admiral

1. **Promote deepseek-7b-grok4 + qwen3-8b-flash to FLEET_ACTIVE — yes/no?** They are the 2nd and 4th highest-volume players on the ship. Either they belong in the active fleet (and the doc was stale) or they should be halted to match reality.
2. **Reclassify `crew_role` field — SQL above acceptable?** Pure documentation; no read-path consumes `crew_role` per HM-S audit.
3. **`grok-4` is halted but still trading 11×/30d** — investigate the halt-emit gate per the 2026-05-03 reconciliation finding (signal-emit ≠ trade-execute gate).

**Halt condition:** read-only — no `dashboard/app.py:1439` edit, no `ai_players` UPDATE.
