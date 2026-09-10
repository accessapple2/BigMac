# Relay — 2026-09-09 — Plutus v1 archaeology + archived-season PSR/DSR

Two read-only archaeology asks, no changes made to any live system, config,
or DB. Findings only.

## 1. The real Plutus v1 fine-tune (HM-PLUTUS-V5-WIN-2026-05-27)

**Bottom line: it survives intact, but not where it's being served from.**

**What was asked for, and what survives:**

| Artifact | Status | Location | Size |
|---|---|---|---|
| `plutus-v1-q4_k_m.gguf` (the trained GGUF) | ✅ survives | `/Volumes/Crucial X9/OLLIETRADES_ARCHIVE/olliemax_salvage/plutus-v1-gguf/` | 4,683,073,568 bytes (4.4 GiB) — matches the doc's "4.46 GB" |
| `unsloth.Q4_K_M.gguf` / `unsloth.BF16.gguf` (same export, alt formats) | ✅ survives | same dir | 4.4 GiB / 14.2 GiB |
| Modelfile with the "financial intelligence officer" SYSTEM prompt | ✅ survives, verbatim | same dir, file `Modelfile` | 1,856 bytes |
| `plutus_corpus_v4.jsonl` (1,199-example training corpus) | ✅ survives, **doubly** | X9 salvage (1.1M) **and** git-tracked in the live repo at `data/plutus_corpus_v4.jsonl` (1,199 lines, confirmed) | — |
| LoRA adapter (`adapter_model.safetensors`) + training checkpoints | ✅ survive | `plutus-v1-lora/`, `plutus-v1-checkpoints/checkpoint-{75,79,150,158,225,234,237}/` | 161 MB adapter |
| Full decommission tarball | ❌ **doesn't exist as a tarball** — see note below | — | — |

**Verified, not assumed:** read the GGUF's first 4 bytes — `GGUF` magic header intact, not truncated. Read the Modelfile — `SYSTEM """You are Plutus, financial intelligence officer for OllieTrades. Analyze trades, evaluate signals, critique positions. Report to the Admiral. Be concise."""` — exact match to the win doc.

**Correction on "June decommission tarball":** no June-dated decommission
artifact exists anywhere searched (bigmac home dir, X9, git history). The
real event is **DECOM-MASTER Gate 1, 2026-07-18/19** — `~/DECOM_STAGING` on
bigmac (docs-only today: `MANIFEST.txt`, `CHECKSUMS_BIGMAC.txt`,
`checkpoints/README.md`) plus the actual salvaged payload, **261 GB**, at
`/Volumes/Crucial X9/OLLIETRADES_ARCHIVE/olliemax_salvage/` — not a tarball,
an uncompressed directory tree (covers v1 through v7d plus the archived v2).
`olliemax_checksums.txt`, referenced in `MANIFEST.txt` as covering this
salvage, was **not found** on either bigmac or X9 — a documented-vs-actual
gap, flagged, not fixed (read-only ask).

**The part that matters operationally:** today's live `plutus-v1:latest` on
olliemax (`config.OLLIE_URL`) has digest `f112024b...`, **identical to
`qwen3:8b`/`qwen3:4b`/`qwen2.5-coder:7b`** — confirmed live via `/api/tags`
just now. The real fine-tune (`4bea908c0348`, Qwen2.5-7B-Instruct-based,
family `qwen2`) is **not served anywhere on today's live olliemax** — not
as `plutus-v1`, and not as `0xroyce/plutus` either (that tag is a
different, generic public base model, family `llama`, 8.0B — CLAUDE.md's
claim that `0xroyce/plutus` is "the real finance-tuned model" is itself
imprecise, per this live digest check). This doesn't change tonight's
earlier 8B-vs-30B bakeoff decision (that comparison used `qwen3:8b`
directly and reasoned from the documented alias, not from an assumption
that `plutus-v1` was secretly special) — but it does mean **restoring the
actual fine-tune to a live seat requires re-importing the salvaged GGUF**
(`ollama create` from the salvaged Modelfile + `.gguf`), not just renaming
a tag. Not done here — read-only ask, flagging for a deliberate decision.

## 2. Archived-season PSR/DSR

**`archive_harness.py` does not exist** — searched bigmac (filesystem +
full git history, all branches, including deleted files) and the X9. No
trace. What *does* exist and is real: `strategies/validation.py` —
Bailey & López de Prado's Deflated/Probabilistic Sharpe Ratio + CSCV/PBO,
unit-tested, already used elsewhere in this codebase. Ran that module
directly (ad hoc, read-only DB connection, nothing written or committed) —
"the harness" as asked-for doesn't exist, so this is the closest honest
substitute, not a stand-in that should be mistaken for it.

**Provenance:** "six archived seasons" = seasons 1-6 in the live
`trader.db` `trades` table (season 7 is the current live season) — there's
no separately-exported per-season archive file; seasons live in-place,
distinguished by the `season` column. "Per strategy" = per `player_id`
(agent) — there's no separate strategy-identifier column with usable
coverage across all six seasons; this codebase's own doctrine (`CLAUDE.md`
"Duplicate Role Policy") already treats agent ≈ strategy in practice.

**Data-quality finding (real, not a script bug):** initial run produced
absurd total-return figures (up to 3.1×10⁸ %) for several players. Root
cause, verified by reading the raw rows: for `asset_type='option'` trades,
`trades.entry_price` holds the **option premium** but `trades.price` (used
as exit) holds the **underlying stock's price** — a column-semantics
mismatch across asset types, not random corruption. Restricted the return
series to `asset_type='stock'` only, which fixes it for equities but means
**options-only seasons have zero usable rows here** — the options side
would need `options_trades`' own premium fields, not `trades`, to do this
correctly; not attempted tonight (scope creep beyond "run read-only").

**Seasons 1, 2, and 4 have zero closed stock trades with `realized_pnl`
recorded at all** — every closed trade in those three seasons is
`asset_type='option'`. So half of the six seasons produce **no PSR/DSR
here**, not from a computation failure but because the underlying data
literally isn't there in a form this check can use.

Also dropped `trade_metrics()`'s `total_return_pct` from the table below —
it naively compounds every trade serially (`cumprod`), which is the wrong
model for a multi-position book with partial exits and produces the
absurd figures above even after the options fix (e.g. season 6 shows
100+ trades compounding into four- and five-digit "returns" that aren't a
real account return). Per-trade Sharpe, PSR, DSR, and win-rate are the
trustworthy outputs; total-return is not, for this data shape.

### Season 3 (n_trials=10, SR0-null=0.65)
| Strategy (player_id) | n | raw SR | PSR | DSR | Win% |
|---|---|---|---|---|---|
| gpt-o3 | 5 | +0.450 | 0.853 | 0.325 | 80% |
| ollama-local | 44 | +0.179 | 0.887 | 0.001 | 43% |
| gemini-2.5-pro | 6 | -0.118 | 0.401 | 0.052 | 50% |
| ollama-llama | 2 | -0.530 | 0.310 | 0.136 | 50% |
| ollama-deepseek | 3 | -0.614 | 0.173 | 0.026 | 33% |
| claude-sonnet | 13 | -0.405 | 0.095 | 0.000 | 15% |
| grok-3 | 21 | -0.809 | 0.025 | 0.000 | 14% |
| gpt-4o | 17 | -0.642 | 0.020 | 0.000 | 12% |
| ollama-kimi | 15 | -0.714 | 0.002 | 0.000 | 20% |
| claude-haiku | 24 | -0.437 | 0.001 | 0.000 | 33% |

### Season 5 (n_trials=10, SR0-null=18.72 — dominated by one outlier, see below)
| Strategy (player_id) | n | raw SR | PSR | DSR | Win% |
|---|---|---|---|---|---|
| grok-4 | 8 | +1.911 | 1.000 | 0.000 | 100% |
| dayblade-sulu | 7 | +1.422 | 0.981 | 0.000 | 86% |
| energy-arnold | 2 | +3.577 | 0.906 | 0.000 | 100% |
| gemini-2.5-flash | 2 | +2.144 | 0.881 | 0.000 | 100% |
| navigator | 5 | +0.094 | 0.579 | 0.000 | 20% |
| neo-matrix | 1 | 0.000 | n/a | n/a | 100% |
| dalio-metals | 10 | -0.080 | 0.394 | 0.000 | 80% |
| ollie-auto | 4 | -0.260 | 0.323 | 0.000 | 50% |
| super-agent | 8 | -0.977 | 0.005 | 0.000 | 25% |
| ollama-llama | 2 | -36.467 | 0.079 | 0.016 | 0% |

Season 5's `ollama-llama` raw SR of -36.5 on n=2 is a variance artifact
(two trades, near-identical losses) — flagged, not filtered, so the table
stays honest about small-n noise rather than hiding it.

### Season 6 (n_trials=15, SR0-null=2.27)
| Strategy (player_id) | n | raw SR | PSR | DSR | Win% |
|---|---|---|---|---|---|
| dalio-metals | 6 | +4.634 | 1.000 | **0.968** | 100% |
| gemini-2.5-flash | 19 | +2.955 | 1.000 | **0.966** | 100% |
| energy-arnold | 11 | +2.239 | 1.000 | 0.451 | 100% |
| cto-grok42 | 16 | +1.930 | 1.000 | 0.221 | 94% |
| qwen3-8b-flash | 90 | +1.022 | 1.000 | 0.000 | 78% |
| grok-4 | 9 | +1.580 | 1.000 | 0.020 | 100% |
| neo-matrix | 43 | +1.147 | 1.000 | 0.000 | 86% |
| ollama-qwen3 | 128 | +0.877 | 1.000 | 0.000 | 80% |
| capitol-trades | 108 | +0.729 | 1.000 | 0.000 | 63% |
| guardian-of-forever | 28 | +0.396 | 0.958 | 0.000 | 75% |
| deepseek-7b-grok4 | 131 | +0.403 | 0.964 | 0.000 | 82% |
| ollie-auto | 181 | +0.291 | 1.000 | 0.000 | 66% |
| ollama-plutus | 158 | +0.249 | 1.000 | 0.000 | 85% |
| navigator | 37 | -0.034 | 0.421 | 0.000 | 51% |
| ollama-llama | 5 | -0.035 | 0.473 | 0.000 | 20% |

**Reading the DSR column correctly:** with 15 strategies compared in one
season, the "expected max Sharpe under pure luck" (SR0-null = 2.27) is
already high — DSR asks "is this Sharpe above what the *best of 15 random*
strategies would produce by chance," not "is this Sharpe good." That's why
most of season 6's positive, plausible-looking raw Sharpes (0.25-1.1)
still deflate to ~0: they're good, but not good enough to be distinguished
from the best-of-15 selection-bias baseline. Only `dalio-metals` and
`gemini-2.5-flash` clear that bar decisively — this is exactly what
`strategies/validation.py`'s `DSR_GRADUATE = 0.95` gate is designed to
surface, and it's working as intended here, not misbehaving.

## Provenance summary
- Source: `data/trader.db` (live, read-only connection: `mode=ro`).
- Module: `strategies/validation.py` (`trade_metrics`, `deflated_sharpe`,
  `probabilistic_sharpe`, `expected_max_sharpe`) — existing, tested,
  unmodified.
- Filter: `action='SELL' AND realized_pnl IS NOT NULL AND asset_type='stock'
  AND entry_price>0`, grouped by `(season, player_id)`.
- No files written, no commits, no DB writes. Ad hoc analysis script kept
  in the session scratchpad only, not added to the repo.

---

## Addendum (same night, later) — alias-era flag, restoration, and Task 2 follow-ups

### Alias-era flag
**Every McCoy (`ollama-plutus`) rating dated 2026-07-19 or later is
ALIAS-ERA.** 2026-07-19 is DECOM-MASTER Gate 1 — the day the old Ollie Max
(`192.168.1.168`, the box that actually held the real fine-tune) was wiped
and returned to Costco. The real GGUF was salvaged *from* that box before
the wipe (see above) but was never re-imported anywhere live afterward —
so any `plutus-v1` serving from 2026-07-19 onward, on whatever host took
over, was necessarily a fresh tag pointing at something else (empirically,
the `qwen3:8b`-digest alias confirmed live tonight). **Cross-check against
this doc's own season-6 PSR/DSR table above: season 6's entire trade range
(2026-04-10 to 2026-07-10) predates 2026-07-19 by more than a week — that
table is NOT alias-era-contaminated.** Nothing else in this doc's tables
needed a retroactive correction.

### plutus-v1-real is now live
Restored by the Captain directly (scp from the X9 salvage path + `ollama
create` on olliemax, num_ctx 2048→16384, persona verified) — registered as
Phase 2 bakeoff arm 5 in `docs/XO_PLAN_2026-09.md`, with an explicit flag
that it was fine-tuned on critique-style (`SCORE:`/`VERDICT:`/`REASONING:`)
targets, not the fleet's decision format or the Phase 2 scoring format —
format-parse rate needs to be measured, not assumed.

### Pre-July wiring (verified in code)
`_resolve_plutus_model()` (`engine/debate_engine.py:593`) reads the exact
same `ai_players.model_id WHERE id='ollama-plutus'` row that `ai_brain.py`'s
fleet scan loop reads for the Arena decision seat ("Dr. McCoy"). **Plutus-v1
fed both roles simultaneously off one shared DB value** — the Arena BUY/HOLD
decision seat, and the War Room "Expert Witness" step
(`run_plutus_witness`, free-form BULLISH/BEARISH/NEUTRAL assessment,
written to `debate_history_v2.plutus_analysis`). War Room is live in
production (`main.py`: triggered every 3rd scan cycle, ~9min cadence), so
this isn't a dormant path. It does **not** feed `engine/scout_critic.py`'s
`CRITIC_MODEL` — that's a separately hardcoded `"qwen3:8b"` literal, unrelated
to `ai_players`/plutus despite the naming similarity. Restoring
`plutus-v1-real` to the one shared knob puts it back exactly where history
says it ran — no additional wiring needed or done.

### Task 2 follow-up (1): the options premium/underlying mix — recording bug, scoped, not applied
**Confirmed RECORDING bug**, not an analysis-side misread. Direct evidence:
the corrupted value appears in *both* the raw `price` column and baked into
that row's own `reasoning` text (e.g. `"Take-profit tier 10% hit
(+2047.1%)"`) — the corruption happened at trade-write time, not introduced
by reading it differently just now. Root cause, read in
`engine/ai_brain.py` (~line 1082): the stop-loss/take-profit exit path does
`price = prices[action["symbol"]]["price"]` — the underlying stock's quote
— with no branch to fetch an option's own premium before calling
`sell()`/`sell_partial()`. The corrupted rows' `reasoning` text ("Take-profit
tier X% hit", "Autopilot trim") matches that exact code path.

**Scope (exhaustive query, not a sample):** exactly **season 1**, **27 of
31** option SELL rows with a recorded `realized_pnl`, **2 players**
(`claude-sonnet`: TSLA/AMD; `gemini-2.5-pro`: AAPL/AMZN/AVGO). Seasons 2
through 7 were checked with the identical ratio filter (exit price >15x
entry price) and show **zero** matches — this is not a persistent,
ongoing bug across all seasons, just season 1. Cannot pin an exact fix
commit: this repo's own git history starts fresh at commit `5498c34`
("OllieTrades April 10"), after season 1 (March 2026) already happened, so
there's no earlier diff to point to.

**"Corrected" P&L: cannot be precisely restated.** The true historical
option exit premium was never durably recorded anywhere recoverable —
`options_trades` (which does have real entry/exit premium fields) only
covers multi-leg spread strategies, not these single-leg closes. The
`reasoning` text's tier labels put a **lower bound** on the true gain (e.g.
a row where the "50% tier" fired means the option was up *at least* 50% on
premium at exit) — nowhere close to the currently-booked numbers, but
still only a floor, not an exact figure. Currently-booked `realized_pnl`
summed across the 27 flagged rows: **$283,484.20** — a number with no
basis in reality, sourced from the underlying's price standing in for the
option's premium. **Not applied. `trades` is unchanged.** This finding
does not affect this doc's PSR/DSR tables above — those were already
scoped to `asset_type='stock'` only, so the corrupted option rows were
never included.

### Task 2 follow-up (2): archive_harness.py rebuilt, in the repo
No 2026-08-24 spec was found anywhere (relay doc history, git history,
X9) — searched, not there. Rebuilt from scratch at `scripts/archive_harness.py`
using `strategies/validation.py`'s DSR/PSR math as the documented starting
point, reimplemented **stdlib-only** (`statistics.NormalDist` in place of
`scipy.stats.norm`; hand-rolled population-moment skew/kurtosis,
cross-validated against `strategies/validation.py`'s own scipy-backed
`trade_metrics()` in tests). Contract, all tested:
- **Provenance ceiling** on every ranked strategy: `INSUFFICIENT_N` (n below
  a minimum, DSR/PSR withheld entirely — never reported on a near-meaningless
  sample), `OPTIONS_EXCLUDED` (some option closes this season were dropped as
  price-implausible), or `OK`.
- **Hard refusal without `trials_tested`** — mandatory keyword-only arg, no
  default; omitting it is a `TypeError`, and passing fewer trials than
  strategies actually ranked is a `ValueError` — directly enforces the
  undercounting warning already in `strategies/validation.py`'s own
  docstring, which the module itself only *documented*, not enforced.
- **Options handled correctly** — same >15x entry/exit ratio filter as the
  follow-up (1) finding above, formalized: excluded rows are counted in the
  output, never silently dropped, and multi-leg spreads (`options_trades`)
  are explicitly out of scope, not silently mishandled.
- **No naive compounding** — arithmetic mean/median return only; no
  `StrategyResult.total_return_pct` field exists (guarded by its own test),
  precisely because that number went to 10^8% on real data earlier tonight.

13/13 tests pass, including a byte-for-byte reproduction of
`strategies/validation.py`'s frozen HM-BACKTEST-123 golden DSR (0.8695).
Ran against real seasons 3 and 6 — now correctly surfaces strategies that
were invisible in this doc's earlier stock-only tables (e.g. season 3's
`dayblade-0dte`, n=122 once its option closes are properly included and
none exceed the implausibility ratio). PBO/CSCV deliberately out of scope
for this rebuild — `strategies/validation.py`'s numpy-based `cscv_pbo()`
remains the place for that.
