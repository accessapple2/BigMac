# HM-CLOSET-POWER-PASTE

Single message to hand to Scotty to clear small backlog items in one session.
Use when ~1-2 hours available to drain queue.

---

## The power paste — copy block below to Scotty next session

Work through this backlog in order. Stop and report after each item. Sacred
rules apply. Manual browser smoke for any frontend change.

### ITEM 1 — HM-CLAUDE-MD-TRIM (high, 30 min)
Audit CLAUDE.md, archive completed/deprecated sections to
`docs/CLAUDE-archive-2026-05.md`. Goal: under 40k chars from 71.5k. Do NOT
delete active subsystem docs (Riker XO, strategy_signals, sacred rules) —
only move COMPLETED/SHIPPED items.

### ITEM 2 — HM-TRADES-VIEW-OPTION-TYPE (medium, 1-2 hrs)
Per `drafts/HM-TRADES-VIEW-OPTION-TYPE.md`. In `dashboard/static/index.html`
trades JS, when `asset_type=option` prefix symbol with C/P + strike + expiry.
Show premium per contract. Target: navigator MNTS covered call renders
`MNTS C21.15 06/10 -1 $0.57 prem` not `MNTS -1.0 $0.57`. Browser smoke
required.

### ITEM 3 — HM-SCANNER-EVENT-DETECTORS-C5 (medium, half day)
Add to `engine/event_tape.py`: `gap_fill_complete`, `breakout_resistance`
(clears 20-day high), `failed_breakdown` (undercut prev low then reclaimed),
`vwap_reclaim` (crosses back above session VWAP), `power_hour_thrust`
(last 60min > 1.5x session avg). Same dedupe as existing detectors. Smoke
against live ticks.

### ITEM 4 — HM-CLOUDFLARED-LAUNCHDAEMON (high, 1 hr)
Migrate cloudflared from nohup PID 97985 to LaunchDaemon at
`~/Library/LaunchAgents/com.ollietrades.cloudflared.plist`. Tunnel ID
`dee0002c-c451-4919-8b16-d649ad19d029`. Test: kill cloudflared, verify
respawn within 30 sec.

### ITEM 5 — HM-FLEET-VERIFY-DALIO-KIMI (low, 15 min)
Verify `dalio-metals` and `ollama-kimi` producing coherent decisions on
`gemma3:4b`. Run cycles manually. Query:
`SELECT COUNT FROM crew_decisions WHERE player_id IN ('dalio-metals',`
`'ollama-kimi') AND date(timestamp) >= date('now')`. If incoherent, halt
and report.

### ITEM 6 — HM-PLUTUS-V6-CORPUS-PREP (medium, 1 hr)
Draft `drafts/HM-PLUTUS-V6-CORPUS.md`. Sources: rallie.ai scraper plus
additional Plutus output review. Target 2500+ examples vs v5's 1199. Same
tag stripping. No identity tiling. Lower LoRA rank ~8 vs 16. Target train
mid-June. **SPEC ONLY** — do not start training.

### ITEM 7 — HM-BM-BAKEOFF-PREP (medium, 1 hr)
Draft `drafts/HM-BM-BAKEOFF-SPEC.md` for June 15 bakeoff. Candidates:
`plutus-v1`, `0xroyce/plutus`, `qwen3:8b`, `qwen3:14b`, `gemma3:4b`,
`ollama-kimi`. Corpus: 100 representative trades last 30 days. Rubric:
critique accuracy, risk ID, trade-quality grade. Output: leaderboard,
recommended swap. **SPEC ONLY**.

---

## Reporting rule

After each item, post 3-line update — what shipped, what commit, blockers.
Halt chain if anything unexpected.

---

## ITEM 3 prerequisites flagged by Scotty (2026-05-27)
Two of the 5 detectors need data not currently in volume_baselines:
- **vwap_reclaim** — needs session VWAP per symbol
- **power_hour_thrust** — needs last 60min avg vs session avg
- **breakout_resistance** — needs 20-day high per symbol

Two paths to get there:
1. Compute on-the-fly from price_ticks (already have minute-level data)
2. Add columns to volume_baselines (or new table session_metrics)

Recommend Path 1 for vwap/power_hour (price_ticks is already there), Path 2
for 20-day-high (compute once daily, cache).
