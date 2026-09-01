# BENCH stale-rating deadlock — found + fixed 2026-09-01

**Branch:** exec-pipeline. Follow-up to `relay_2026-09-01_worf-wiring-gap.md` —
that relay flagged the Aug 27 gate-rejection pattern as the live thread; this
is the traced root cause and the fix.

## What was found

Checked whether any `halt_mode='active'` agent is caught in a rating-lockout
deadlock — a BENCH (D/E) `agent_ratings` snapshot that can structurally never
refresh because the agent it blocks can never generate the trade history
needed to recompute it.

- `engine/agent_ratings.py::calculate_rating()` scopes stock trades to
  `season = _CURRENT_SEASON` (computed dynamically). Live trades are now
  tagged season 7; `ollama-plutus`'s last 160 closed trades are all season 6
  (last one 2026-07-09).
- With 0 season-7 closed trades, every `calculate_rating('ollama-plutus',
  'alltime')` call returns early (`< 2 clean_rows` → `rating: "N/A"`) and —
  confirmed by reading the function — **never reaches the `INSERT INTO
  agent_ratings` line** on that path.
- `engine/paper_trader.py::_bench_block_reason()` (the live entry gate, from
  `HM-BENCH-ENTRY-GATE-2026-07-09`) reads `ORDER BY timestamp DESC LIMIT 1`
  with no age check — so it kept retrieving his last *real* snapshot,
  **D/39.9 dated 2026-07-13**, and blocking every BUY through today.
- Closed loop: blocked from buying → can't close new trades → can't
  accumulate season-7 history → rating stuck at N/A forever → stale D
  verdict never overwritten. No code path breaks this on its own.
- Swept every `is_active=1` player for "most recent alltime rating is D/E":
  8 hits (`super-agent`, `ollama-llama`, `deepseek-7b-grok4`,
  `guardian-of-forever`, `navigator`, `ollama-plutus`, `ollama-qwen3`,
  `capitol-trades`). Of those, only **`ollama-plutus`** and **`capitol-trades`**
  have `halt_mode='active'` (the rest are `full`/`exit_only`, already blocked
  at a higher level regardless of BENCH). `capitol-trades`'s E/0.0 rating is
  **fresh** (written today, 2026-09-01 16:54) — it has exactly 2 season-7
  closed trades (2026-07-16), clears the `<2` floor, and is a live, currently-
  earned E, not stuck. So **`ollama-plutus` was the only agent actually
  caught in the deadlock.**
- Confirms and explains the earlier finding in the Worf relay: the Aug 27
  cutoff wasn't a new bug appearing that day — `ollama-plutus` kept emitting
  signals normally throughout, but every entry attempt had already been
  gate-rejected since his last real closed trade in season 6/early season 7,
  and the gate had no way to ever reconsider.

## Fix applied

`engine/paper_trader.py::_bench_block_reason()` — added a staleness check.
A BENCH (D/E) snapshot is only trusted for `_BENCH_STALE_DAYS = 30` days;
past that it fails open (same posture the function already uses for
"never-rated" players), with a `console.log` line so an expiry is visible
in `trader.log`, not silent. Age is computed in SQL (`julianday`) rather
than parsed in Python, per `feedback_db_max_lexical_format_freshness.md`
(mixed timestamp text formats have broken naive freshness checks before —
not applicable here since this column is DB-default `CURRENT_TIMESTAMP`,
but SQL-side computation sidesteps the whole class of bug regardless).

Verified against the live DB before and after:
- `ollama-plutus` → was `"BENCH: rating D (40/100)"`, now `None` (unblocked).
- `capitol-trades` → unchanged, still `"BENCH: rating E (0/100)"` (fresh
  rating, correctly still gated).
- `deepseek-7b-grok4`, `super-agent` → now also fail open on this specific
  gate, but both are `halt_mode='full'` — already blocked upstream, so this
  has no live effect for them.

30 days chosen as a reasonable default: long enough not to flap on an
agent's ordinary quiet periods, short enough to bound how long a stale
verdict can persist. This is a judgment call, not a mechanically-derived
number — revisit if it proves too permissive or too tight in practice.

Tests: extended `tests/test_bench_entry_gate.py` with 3 new cases (fresh
BENCH still blocks, stale BENCH fails open, boundary at exactly
`_BENCH_STALE_DAYS`). Full file: 15/15 pass. Broader sweep
(`-k "paper_trader or bench or rating"`, excluding two pre-existing/
unrelated collection errors — `test_holodeck_drawdown_sign.py` needs
`vectorbt`, not installed in `.venv` by design per
`feedback_venv_backtest_vectorbt_isolation.md`; `test_riker_synthesis_lock_retry.py`
imports `engine.riker_synthesis`, a module retired per CLAUDE.md's
"Riker XO synthesis job — STOOD DOWN", test file appears orphaned): 47/47
pass, no regressions.

## Deploy status: HELD (Captain directive, 2026-09-01)

Fix is committed (`0730aec`) and pushed — safe on disk, not live. Trader
process is still running pre-fix bytecode.

**If deploying tonight:** `launchctl kickstart -k
gui/$(id -u)/com.trademinds.trader` **after the 13:00 MST close**, then the
smoke-verify per CLAUDE.md's Restart-then-verify doctrine. Doing this after
close means the fix lands with no live market window left today —
**first trading day the unblock actually applies is expected to be
tomorrow (2026-09-02)**, not today.

## Residual risk from the staleness fail-open (Captain-requested note)

Asked: could the 30-day fail-open unblock anything else unexpectedly at
restart? Re-verified against the live DB — no immediate effect: of the 8
`is_active=1` agents with a D/E most-recent alltime rating, only 2 are
`halt_mode='active'` today (`ollama-plutus`, now unblocked; `capitol-trades`,
still correctly gated on a fresh rating). The other **6** are already
`halt_mode` full/exit_only — this change doesn't touch them today:
`super-agent`, `ollama-llama`, `deepseek-7b-grok4`, `guardian-of-forever`,
`navigator`, `ollama-qwen3`.

The residual risk is later, not now: those 6 all carry a stale D/E snapshot
that the staleness fail-open has already neutered as a gate. If any of them
gets un-halted in the future (`scripts/fleet_lifecycle.py revive`) without a
fresh rating being computed first, it goes straight back to unrestricted
entries — no rating-based check at all until it accumulates ≥2 new closed
trades, because the only rating on file for it is already past the 30-day
mark. Before this fix, an old D/E snapshot would have kept blocking it as a
second, independent check even after `halt_mode` was lifted; that check is
now gone for anyone whose rating is already stale. Fleet Lifecycle Doctrine
already expects a deliberate reason + record for any revive, so this
probably doesn't bite in practice — but it's a real coverage gap, not a
hypothetical one, and worth a beat of manual judgment (or a rating recompute)
at revive time rather than assuming BENCH will catch a bad-rated agent that
comes back online.

## Not done / open

- Not restarted/deployed — see "Deploy status" above.
- `season_config` table's latest row is still season 6 (never got a
  season-7 row) — separate metadata drift from the same rollover, not
  fixed here; flagged for whoever owns season-config hygiene.
- Did not check whether agents that are currently rated well (A/B/C) could
  hit the same N/A-forever mechanic later — it's currently harmless for
  them (BENCH only blocks D/E), but if any of them stop trading for an
  extended period their rating will also permanently freeze at N/A (just
  not at a blocking grade). Not a bug today; worth knowing if report-card
  accuracy ever becomes the concern rather than entry-blocking.
- `_BENCH_STALE_DAYS = 30` is a first-pass default, not Admiral-reviewed.
