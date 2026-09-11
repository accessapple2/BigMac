# Relay — B8: Recall-in-prompt wired into the live path. 2026-09-11

## What shipped

`engine/providers/base.py::build_prompt()` now calls `engine.recall_prompt
.build_recall_prompt_section()` for every agent's prompt, gated on
`config.RECALL_IN_PROMPT_ENABLED` (still `False` on disk — this wiring
does not turn the feature on for anyone, it only makes the existing flag
actually reach the live prompt path). Same pattern as every other optional
context block in this function (impulse/SMA/imbalance) — try/except,
append to `self._sources` only when non-empty.

**Setup text**: `engine/recall_prompt.py`'s `get_recall_neighbors()` needs
a `(symbol, timeframe, setup)` triple to embed. `build_prompt()` has no
explicit timeframe/setup parameters, so the wiring derives a short
technical-setup summary from indicators already computed in this exact
function scope (RSI zone, MACD cross direction, golden/death cross) —
e.g. `"RSI OVERSOLD, MACD BULLISH, GOLDEN CROSS"` — rather than passing an
uninformative default. `timeframe` passed as `None`, which
`make_setup_text()` already defaults to `"SWING"` for.

## Verified live, not just read

Direct smoke test against a real `AIProvider` instance (bypassing the
scheduler, no trader restart needed):
- **Flag OFF (default)**: `"Recall ("` does not appear anywhere in the
  built prompt — confirmed true no-op.
- **Flag ON**: real neighbors returned and formatted correctly —
  `"Recall (5 similar past setups... 5W/0L, avg pnl +0.89) ... Nearest
  analogs: AAPL(win,+0.49), ..."`, `self._sources` correctly includes
  `"Recall"`.
- `python3 -m py_compile` clean; `pytest -k "prompt or provider or
  recall"` (11 tests, excluding two collection failures pre-existing and
  unrelated — `test_holodeck_drawdown_sign.py`/
  `test_riker_synthesis_lock_retry.py` both fail on `import vectorbt`,
  confirmed identical failure on the pre-change tree via `git stash`, not
  caused by this change): **11 passed, no regressions.**

## What this does NOT do

Does not flip `RECALL_IN_PROMPT_ENABLED` to `True` for anyone — that's a
bakeoff-arm decision, not a wiring decision, and stays with the Admiral.
The directive's "bakeoff-flagged so it can be measured with and without"
is now literally true for the live path (flip the flag, restart, compare)
rather than only true for the offline `scripts/recall_bakeoff.py` harness.

## Cost/latency note

`build_recall_prompt_section()`'s corpus load is cached in-process for 10
minutes (`CORPUS_CACHE_TTL_S`); the one per-call cost when the flag is ON
is the `bge-m3` embed call itself, already measured live at 0.035-0.171s
warm 2026-09-10 (`relay_2026-09-10_recall-url-fix-and-host-sweep.md`) —
the latency concern the directive named is closed, not re-litigated here.
