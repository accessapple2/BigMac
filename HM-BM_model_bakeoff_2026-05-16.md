HM-BM: New Model Bakeoff vs Incumbents — May 2026 (Revised)
Issued: 2026-05-16
From: Admiral Steve (via Claude XO)
To: Scotty (Claude Code, executor on Ollie Box + bigmac)
Classification: Hardening Milestone — Model Evaluation
Depends on: HM-BN closure (Phase 7 trader restart complete, all phases green)
Revision note: Rewrote after HM-BN discovery surfaced (a) canonical schema is `ai\_players` table with `model\_id` column — NOT `agents.model\_name`; (b) `chekov` / `capitol-trades` / `neo-matrix` from Season 6 roster are NOT LLM-driven and excluded from this bakeoff.
---
OBJECTIVE
Backtest each new-fleet model against its closest incumbent on identical signal windows. Generate a one-page retention report driving the HM-BN.1 retirement decision.
Hard rules:
NEVER `rm` on `.db` files or any dir under `\~/autonomous-trader/data/`
Archive/rename only — no destructive deletes
Clone via INSERT; do NOT modify production Season 6 rows in `ai\_players`
Verify model toggle page (CLAUDE.md item 13) before excluding any halted-but-toggled agent
SSH user is `bigmac` across all machines (CLAUDE.md item 12) — never `ollie@`
Frontend ship rule does not apply (no `dashboard/static/index.html` changes expected)
---
PHASE 0 — Pre-flight (AUTONOMOUS gate)
```bash
ssh bigmac@192.168.1.166 '
echo "=== Ollama version (must be 0.24.0+) ==="
ollama --version

echo ""
echo "=== ai\_players schema verification ==="
sqlite3 \~/autonomous-trader/data/trader.db ".schema ai\_players" | head -30

echo ""
echo "=== Active LLM-driven agents (model\_id present, non-deterministic) ==="
sqlite3 \~/autonomous-trader/data/trader.db "
SELECT id, model\_id, fallback\_model, halt\_mode
FROM ai\_players
WHERE halt\_mode = '\\''active'\\''
  AND model\_id IS NOT NULL
  AND model\_id != '\\'''\\''
  AND LOWER(model\_id) NOT LIKE '\\''%matrix%'\\''
  AND LOWER(model\_id) NOT LIKE '\\''%independent%'\\''
ORDER BY id;
"

echo ""
echo "=== HM-BM filename collision check ==="
ls -1 \~/autonomous-trader/HM-BM\*.md 2>/dev/null || echo "(no collision)"
'
```
AUTONOMOUS DECISION:
If Ollama < 0.24.0 → HALT and paste-back. HM-BN Phase 1 isn't closed; cannot proceed.
If `ai\_players` schema unexpected → HALT and paste-back with the actual schema.
If no active LLM-driven agents found → HALT and paste-back. Bakeoff has no production reference shape.
Otherwise → write Phase 0 result to `logs/HM-BM\_phase0.md` and proceed to Phase 1.
---
PAIRINGS
New    Incumbent    Role    Primary Metric
`gemma4:26b`    best current production tool-caller (Scotty picks from Phase 0 query)    Production agent / tool calls    tool-call success rate + signal hit %
`gemma4:31b`    `gemma3:27b-it-qat`    Heavy reasoning / vision    reasoning quality, signal hit %
`qwen3.6:27b`    `qwen3:14b`    Mid-tier general    tool-call reliability, PF
`qwen3.6:35b-a3b`    `qwen3-coder:30b`    Agentic coding    tool-call success + signal hit %
`llama4:scout`    `llama3.1:latest`    General reliability    signal hit %, no-hallucination rate
`devstral-small-2`    `qwen2.5-coder:7b`    Lightweight agentic coding    tool-call success rate
`ministral-3:3b`    `phi3:mini`    Real-time / edge    mean latency, 0DTE/ghost-trader signal hit %
EXCLUDED from bakeoff (not LLM-driven, per Phase 4 of HM-BN discovery):
`chekov` — halt_mode='full' orphan, no LLM model_id
`capitol-trades` — data feed, not LLM
`neo-matrix` — deterministic, provider="matrix"
---
PHASE 1 — Backtest harness self-discovery (AUTONOMOUS)
Locate the backtest entry point. CLAUDE.md item 25 references "Baseline TB 268sigs/34.3% hit/PF 2.02/+1.74%" — find what produced this.
Discovery sequence:
`find \~/autonomous-trader -maxdepth 3 -type f \\( -name "backtest\*.py" -o -name "\*\_bt.py" -o -name "tb\_\*.py" \\) 2>/dev/null`
`grep -rln "268sigs\\|baseline.\*PF\\|backtest.\*window\\|tb\_run\\|run\_backtest" \~/autonomous-trader/{scripts,engine,\*.py} 2>/dev/null | head -10`
`grep -l "argparse\\|click" \~/autonomous-trader/main.py 2>/dev/null` (check for backtest subcommand)
Sanity-run with `--help` if invocation found
Write findings to `\~/autonomous-trader/logs/HM-BM\_phase1\_discovery.md`.
AUTONOMOUS DECISION:
If invocation clear AND dry-run succeeds → proceed to Phase 2
If multiple candidates → pick the one with most recent log entries matching the baseline metrics; document choice; proceed to Phase 2
If NO harness found → safe-halt; paste-back the discovery report
---
PHASE 2 — Clone bakeoff agents in `ai\_players` (NO TOUCHING PRODUCTION)
```bash
ssh bigmac@192.168.1.166 '
cd \~/autonomous-trader
TS=$(date +%Y%m%d\_%H%M%S)
sqlite3 data/trader.db ".dump ai\_players" > backups/ai\_players\_pre\_BM\_${TS}.sql
echo "snapshot: backups/ai\_players\_pre\_BM\_${TS}.sql"
'
```
Cloning strategy:
Pick ONE representative production LLM agent from Phase 0 active list (suggested: `navigator` if active; otherwise Scotty picks the highest-traffic LLM agent).
For each of the 7 pairings, INSERT two new rows into `ai\_players`:
id: `<base>\_bm\_<newmodel>\_new` (e.g. `navigator\_bm\_gemma4-26b\_new`)
id: `<base>\_bm\_<newmodel>\_old` (incumbent control)
Copy ALL other columns from the source row — only id, model_id, halt_mode (force 'full'), and any "bakeoff_marker" column (if Scotty wants to add one) change.
All clones: `halt\_mode='full'`. They will NOT trade live; they run only inside the backtest harness.
Verify:
```bash
sqlite3 \~/autonomous-trader/data/trader.db "
SELECT id, model\_id, halt\_mode
FROM ai\_players
WHERE id LIKE '%\_bm\_%'
ORDER BY id;
"
```
CHECKPOINT: Snapshot exists. 14 clone rows (7 pairings × new+old). All halt_mode='full'. Source Season 6 rows untouched. Production trader process unaffected.
---
PHASE 3 — Run identical backtest windows
For each pairing, run the SAME backtest window through both clones (`\_new` and `\_old`):
Window: last 90 calendar days, or last 1000 signals — whichever yields more data
Universe: FIXED_WATCHLIST (CLAUDE.md item 10) including TQQQ
Identical random seeds where the harness supports it
Identical entry/exit logic — ONLY `model\_id` changes
Per-pairing capture (write each as a row to CSV):
Total signals emitted
Hit rate %
Profit factor
Sharpe
Max drawdown
Mean inference latency (tokens/sec)
Tool-call success rate (successful function calls / total attempts)
Halt-by-error count (model crashes, repetition loops, etc.)
Output: `\~/autonomous-trader/logs/bakeoff\_results\_2026-05-16.csv` with columns: pairing, side (new/old), metric, value.
CHECKPOINT: CSV exists. All 7 pairings have new + old rows. Production data uncorrupted.
---
PHASE 4 — Bakeoff report
Generate `\~/autonomous-trader/reports/HM-BM\_bakeoff\_report.md`. Per pairing, a 5-line block:
```
PAIRING: <new> vs <incumbent>
  NEW:  hit=X% PF=Y Sharpe=Z latency=Wms toolcall=V%
  OLD:  hit=X% PF=Y Sharpe=Z latency=Wms toolcall=V%
  DELTA: <signed % change on primary metric>
  RECOMMENDATION: RETAIN\_NEW | RETAIN\_OLD | KEEP\_BOTH | INCONCLUSIVE
```
Recommendation rules:
`RETAIN\_NEW` — NEW beats OLD by >10% on primary metric AND tool-call success ≥ OLD
`RETAIN\_OLD` — OLD beats NEW by >10% on primary metric
`KEEP\_BOTH` — within ±10% but serving different niches (latency vs quality)
`INCONCLUSIVE` — sample too small (<50 signals) or both crashed
At the END of the report, include a "Roster Anomalies" section listing the 3 excluded agents (chekov / capitol-trades / neo-matrix) with their reason for exclusion. This becomes the audit trail for the corrected Season 6 LLM roster.
CHECKPOINT: Report exists. All 7 pairings have a recommendation. Roster Anomalies section present.
---
PHASE 5 — Cleanup
Leave bakeoff clones in `ai\_players` (halt_mode='full') for Admiral review and replay capability.
DO NOT delete clones. They are audit trail.
Archive clones for retirement only on Admiral directive (HM-BN.1 or HM-BM follow-on).
CHECKPOINT: Production agents unchanged. Bakeoff clones halt_mode='full' and discoverable via `id LIKE '%\_bm\_%'`.
---
PASTE-BACK RULES (overnight autonomous run)
Routine green = silent until closure. ONLY paste back if:
Phase 0: Ollama < 0.24, schema mismatch, or no LLM-driven agents found
Phase 1: no backtest harness found (safe-halt with discovery report)
Phase 2: ai_players schema or INSERT mechanism fails
Phase 3: ALL pairings crash (partial crashes → log + INCONCLUSIVE, continue)
Phase 4: completed report (this IS the morning brief)
Partial failures within a pairing: log to CSV with `halt\_by\_error=true`, mark recommendation INCONCLUSIVE, continue to next pairing. Goal is a complete report with as many pairings as possible, not perfect runs.
---
ADMIRAL DECISION POINT (post-HM-BM)
Once the report lands, Admiral issues one of:
HM-BN.1 — retire incumbents per RETAIN_NEW recommendations
HM-BM.1 — extended bakeoff for INCONCLUSIVE pairings (longer window, more samples)
HOLD — keep current routing, archive results for next monthly review
---
END DIRECTIVE
