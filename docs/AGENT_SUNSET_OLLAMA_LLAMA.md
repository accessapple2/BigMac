# Agent Sunset Plan — `ollama-llama`

**Author:** Scotty 2.4 (Claude Code Opus 4.7)
**Date:** 2026-05-07 ~20:30 MST
**Status:** **Plan only — awaiting Admiral go.** No halt SQL, no scheduler edits, no archive moves performed by this document.
**Source audit:** `docs/SCOTTY_AUDIT_2.md` Top-10 #6 + Section G.

---

## 1. Evidence — Last 60 Days

### From `data/trader.db::trades`

| Metric | Value | Window |
|---|---|---|
| Total trades | 53 | 2026-03-13 → 2026-05-04 |
| Wins | 6 | (11.3% WR over 60d) |
| Realized P&L (60d) | **−$5,536.45** | |
| Last trade | 2026-05-04 04:58:04 | (3 days before this plan) |
| 30d trades | 9 | 2026-04-25 halt → 2026-05-04 (final exits) |
| 30d wins | 1 | 14.3% WR |
| 30d realized P&L | **−$28.47** | |

### From `data/trader.db::signals`

| Metric | Value |
|---|---|
| 30d signals emitted | **1,199** |
| Last signal | 2026-05-02 02:37:15 |
| Signal-to-trade ratio | 133:1 |

**Confirms audit claim:** ollama-llama has the worst absolute P&L on the ship over 60d (−$5,536) AND the worst signal-to-trade conversion (1,199 signals / 9 trades = pure compute waste).

---

## 2. Edge Claim — Hard Numbers

| Compare | ollama-llama | Fleet median | Δ |
|---|---|---|---|
| 60d WR | 11.3% | 70%+ | **−59 pp** |
| 60d realized P&L | −$5,536 | +$4,108 (Plutus alone) | **−$9,644 swing** |
| Sig-to-trade | 133:1 | ~12:1 | **11× wasteful** |

**Negative-edge claim is verified.** Halt is justified by data, not opinion.

---

## 3. Halt State Today

```
sqlite3 data/trader.db "SELECT id, halt_mode, halted_at, halt_reason FROM ai_players WHERE id='ollama-llama'"
ollama-llama|exit_only|2026-04-25 00:00:00|<reason TBD — inspect>
```

Halted to `exit_only` on 2026-04-25 (12 days ago). Has executed 9 exit trades since (closing legacy positions). **Today's reality:** trade execution path is gated by `halt_mode != 'active'` per HM-AK-β + HM-C reconciliation. But the **signal-emit path is not** — confirmed by 1,199 signals emitted in last 30 days despite `exit_only` status. Per 2026-05-03 reconciliation, this is the documented bug: halt_mode gates execution, not emission.

---

## 4. Downstream Dependents

`grep -rn "ollama-llama\|ollama_llama" --include="*.py" .` (excluding `_archive/` and `.venv*/`):

| File:line | Reference | Effect of full halt |
|---|---|---|
| `config.py:159` | `{"id": "ollama-llama", "name": "Llama 3.1 8B", ...}` in AI_PLAYERS list | Removing line → stops scheduler picking it up. **Required edit** for full sunset. |
| `dashboard/app.py:1433` | hardcoded list (separate from FLEET_ACTIVE) | Likely an iteration list. Audit at edit time. |
| `dashboard/app.py:7822` | `"ollama-llama": 0.0,` — looks like a confidence-default dict | Removing safe (default to 0.0 in another way). |
| `dashboard/app.py:16953` | `agents = ["ollie-auto", "navigator", "chekov", "ollama-llama", "ollama-plutus", ...]` | One of the dashboard panel iteration sites. **Required edit.** |
| `scripts/backtest_baseline.py:36, 46, 56, 92` | Backtest agent definition | Leave alone — historical backtest results reference this id. |
| `scripts/s6_sim_180d_backtest.py:131` | Backtest scaffolding | Leave alone — historical scaffolding. |
| `scripts/s6_60d_run.py:8` | Backtest run script | Leave alone. |

**No production code path has a hard dependency on ollama-llama**: removal does not silently break Plutus, Capitol, ollie-auto, or any active player.

---

## 5. Halt Plan (precise SQL + edits, for Admiral execute, after go)

### Step 1 — Confirm zero open positions

```bash
sqlite3 data/trader.db "SELECT symbol, qty, avg_price FROM positions WHERE player_id='ollama-llama' AND qty != 0"
```

If non-empty → keep `exit_only` until exits land. If empty → proceed.

### Step 2 — Promote halt to `full`

```sql
UPDATE ai_players
   SET halt_mode  = 'full',
       halted_at  = CURRENT_TIMESTAMP,
       halt_reason = '[2026-05-XX] Sunset — 60d net −$5,536 / 11% WR / 1,199 sig-to-9 trade waste. See docs/AGENT_SUNSET_OLLAMA_LLAMA.md'
 WHERE id = 'ollama-llama';
```

### Step 3 — Stop signal-emit (the long pole)

The 1,199 signals/30d are the actual compute drain. Halt-emit gate is per 2026-05-03 reconciliation; not yet generally implemented. Concrete options:

- **Option A (preferred):** add `halt_mode != 'active'` filter to signal-emit path. The known sites are in `engine/ai_brain.py` cycle loop and `engine/crew_scanner.py` plus per-player scheduler entries. Single-file fix per call site.
- **Option B (faster, narrower):** delete `ollama-llama` from `config.py:AI_PLAYERS` (line 159). Removes from scheduler picks. **Loses ability to ghost-trade or recall.**
- **Option C (intermediate):** comment-out the player config line (don't delete). Same effect as B, recoverable.

Recommend **A + C combined**: fix the architectural emit-gate bug (so future halts work right) AND comment-out ollama-llama config to stop the bleeding now.

### Step 4 — Dashboard cleanups (low priority, can defer to Roster Reconciliation)

- `dashboard/app.py:7822` — remove `"ollama-llama": 0.0,` line.
- `dashboard/app.py:16953` — remove `"ollama-llama"` from agents list.
- `dashboard/app.py:1433` — audit list at edit time, remove if present.

### Step 5 — Archive convention

Per CLAUDE.md: *"Retired agents: keep code in engine/ (muted via threshold), DO NOT delete."* No `engine/ollama_llama_*.py` exists (it's a generic LLM player using shared scaffolding) — no module to archive. Just the config-line removal in step 3 + the scheduler entry.

---

## 6. Data Retention

**ALL historical rows preserved.** Per sacred-data rule:

- `trades` rows for `player_id='ollama-llama'` (53 rows) — kept forever.
- `signals` rows (1,199 in 30d, more historical) — kept forever.
- `portfolio_history` rows — kept forever.
- `ai_players` row — `halt_mode='full'` flag, NOT deleted.

The only mutation is `halt_mode` and metadata. The audit trail is intact for any future post-mortem.

---

## 7. Rehab Path (sacred-data optimism)

If at some future point Admiral wants to re-evaluate llama3.1 as a paper trader:

1. Restore `config.py:AI_PLAYERS` line.
2. Set `halt_mode='active'` in `ai_players`.
3. Ghost-trade for 30 days (no real-position writes, just `signals` + virtual P&L).
4. Promote only if Sharpe ≥ 2.0 AND positive 30d return AND R:R better than 30:1 sig-to-trade.

The data, the prompts, the scaffolding all remain. Recovery is config-only.

---

## 8. Open Questions for Admiral

1. **`halt_mode='full'` now or after Sniper Mode KILL on Saturday?** Recommend pairing them — single sweep through `ai_players` UPDATE plus single restart.
2. **Architectural fix for signal-emit gate (Option A) — separate ticket or rolled in?** Recommend separate ticket (`HM-AY-δ`) — the architectural fix benefits the 5 `exit_only` players, not just ollama-llama.
3. **dayblade-sulu** is in identical posture (`exit_only` since 2026-03-31, longer than ollama-llama). Sunset same way? Recommend yes — the plan template applies.

**Halt condition:** await Admiral go. No execution.
