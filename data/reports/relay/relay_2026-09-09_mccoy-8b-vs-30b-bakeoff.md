# Relay — McCoy/Worf/Troi 8B-vs-30B after-hours bakeoff (2026-09-09)

## Ask
Pull today's McCoy prompts (8B morning + 30B-instruct afternoon), replay through
qwen3:8b and qwen3:30b-a3b-instruct-2507-q4_K_M, score, and set tomorrow's
McCoy/Worf/Troi/Geordi-pin config per "8B unless the 30B is clearly better."

## Premise correction (found before running anything)
`decision_audit` has no prompt column and no per-row model tag — only a
300-char snippet of the model's *output* reasoning. A literal prompt replay
from that table is not possible; `build_prompt()` pulls live price/news/
indicator data at call time and none of it is persisted. Confirmed with the
Captain and re-scoped to three parts instead: (a) outcome-score the real
production decisions (no replay needed), (b) a small same-input sample
replayed through both models via the real `build_prompt()`, (c) start
persisting `prompt_text` going forward so this is byte-for-byte replayable
next time.

**Era boundaries corrected against your own commits, not "before/after 09:12"
at face value:**
- `ea1269e` (07:55:21 MST) reverted McCoy/Worf from `qwen3:30b-a3b` (thinking)
  to 8B after confirming (that same commit) 6/6 think-leak test calls —
  **everything before 07:55:21 MST today, including the earlier portion of
  "before 09:12," was the broken-thinking 30B, not 8B.** Excluded from both
  eras rather than mislabeled as 8B.
- `e2aed48` (09:12:36 MST) went live with `qwen3:30b-a3b-instruct-2507-q4_K_M`
  (verified 0/5 think leaks) for McCoy/Worf/Troi + the Geordi advisory pin.
- Clean windows used: **8B = 07:55:21–09:12:36 MST** (150 signal_emits, 135
  BUY). **30B-instruct = 09:12:36 MST–~16:37 MST** (795 signal_emits so far,
  538 BUY + 3 BUY_CALL).

## (a) Outcome-scoring — real production BUYs, no replay
Forward return from signal timestamp to the 13:00 MST close, Alpaca IEX
1-min bars, McCoy only, pre-close BUYs only (BUYs emitted after the 13:00
close have no meaningful "forward to close" figure and were excluded):

| | n | mean fwd ret | median | stdev | win-rate |
|---|---|---|---|---|---|
| 8B (07:55–09:12) | 135 | **-0.253%** | -0.202% | 0.822% | 38.5% |
| 30B-instruct (09:12–13:00) | 177 | **-0.148%** | -0.122% | 0.482% | 29.9% |

Welch's t = **-1.32** — not significant at 95%. 30B's mean is nominally
less bad, but its win-rate is *worse* than 8B's. **Does not clearly favor
the 30B.**

BUY-share/confidence calibration (full window each, not just pre-close):
- 8B: 90.0% BUY-share (135/150), mean BUY confidence 0.80
- 30B-instruct: 68.1% BUY-share (541/795), mean BUY confidence 0.85

## (b) Same-input sample — 40 decisions (20/20 per era) through both models
Reconstructed price/change_pct/high/low from today's Alpaca bars as of each
signal's timestamp, built the *real* McCoy `build_prompt()` (persona, rules,
sizing — all genuine production text), with `news=[]` and `indicators={}`
(not recoverable after the fact — acknowledged gap, see caveat below). Same
exact prompt sent to both models, sequential (8B batch, then 30B batch,
matching `run_bm_bakeoff.py`'s VRAM-sequencing convention).

| | parsed | BUY/HOLD | mean conf | direction/reasoning conflicts | wall (mean/median/max) |
|---|---|---|---|---|---|
| qwen3:8b | 40/40 | 30 BUY / 10 HOLD | 0.723 | **19/40 (47.5%)** | 9.40s / 2.99s / 29.85s |
| qwen3:30b-instruct | 40/40 | 1 BUY / 39 HOLD | 0.461 | 0/40 | 4.82s / 3.69s / 10.42s |

Direction agreement: **11/40 (27.5%)**.

**Caveat — read this before trusting the 47.5% number at face value:** with
`indicators={}` and `news=[]`, the prompt literally states "No indicator
data available" / "No recent news." qwen3:8b routinely invented specific
technicals anyway (breakout %, volume multiples, RSI, price levels) not
present anywhere in the prompt, and in 19/40 cases its own stated reasoning
direction (e.g. "showing a strong bearish breakout") contradicted its BUY
action — caught by the existing `check_reasoning_direction_conflict()`
production QA check, not a bespoke heuristic. qwen3:30b-instruct almost
always recognized the data gap and defaulted to HOLD (39/40), with zero
self-contradictions. This is a real, mechanically-detected coherence gap
between the two models *on this test*, but the test's blank-context
condition doesn't match real production (which does have indicators/news)
— treat this as a flag for a follow-up test with real context, not as
proof of 8B's live-production behavior.

## Recommendation
Rule: 8B unless (a) **and** (b) both clearly favor the 30B.
- (a): does not clearly favor 30B (not significant; win-rate favors 8B).
- (b): favors 30B on rationale discipline, but under an artificial
  data-sparse condition that may not reflect real production quality.

(a) alone fails the "both" bar, so **the rule resolves to 8B** regardless
of (b) — reverted McCoy/Worf/Troi/Geordi-pin to 8B for tomorrow. Flagging
the (b) coherence gap as worth re-testing with real (non-blank) context
before it's dismissed — if it holds up with real data, that would be a
legitimate reason to revisit the 30B despite (a).

## Config changes (live, DB-authoritative + config.py mirror)
| Seat | id | before (tonight) | after |
|---|---|---|---|
| McCoy | `ollama-plutus` | `qwen3:30b-a3b-instruct-2507-q4_K_M` | `plutus-v1` |
| Worf | `qwen3-8b-flash` | `qwen3:30b-a3b-instruct-2507-q4_K_M` | `qwen3:8b` |
| Troi | `options-sosnoff` | `qwen3:30b-a3b-instruct-2507-q4_K_M` | `qwen3:8b` |
| Geordi advisory pin | `ollama-local` (crew_specialization.py, CREW_MANIFEST) | `qwen3:30b-a3b-instruct-2507-q4_K_M` | `gemma3:4b` |

`ai_players.model_id` updated to match (DB is the authoritative source for
decision_audit's signal_emit path per `e2aed48`'s own verified call-chain
trace); `config.py` AI_PLAYERS mirror updated for McCoy/Worf (Troi has no
config.py entry — DB-only, consistent with `e2aed48`'s diff).

## Shipped alongside: `decision_audit.prompt_text` (item c)
Added `prompt_text TEXT` column (live DB + `setup_db.py`). Threaded the
exact LLM input through: `providers/base.py::analyze()` now stashes
`self._last_prompt`; `ai_brain.py`'s `save_signal()` call passes
`prompt_text=getattr(provider, "_last_prompt", None)`; `save_signal()` and
`_write_decision_audit()` both accept and persist it. Scoped to the
ai_brain.py fleet path only — `dayblade.py`'s separate `save_signal()` call
is unaffected (`prompt_text` defaults to `None`). Tonight's replay
limitation is now a one-time cost — a future bakeoff is byte-for-byte
replayable.

## Verification
- `py_compile` clean on all 4 touched files + `setup_db.py`.
- olliemax reachable pre-run; both candidate models present.
- One restart, model-load verified on olliemax post-restart (see below).
