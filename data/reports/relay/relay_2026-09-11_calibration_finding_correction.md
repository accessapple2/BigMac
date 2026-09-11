# Relay — Calibration finding correction: the 169-trade number is withdrawn. 2026-09-11

## Direct answer to the question asked

**No, it didn't reconcile. The finding is withdrawn and replaced.** My
earlier same-day reconciliation (`relay_2026-09-11_B9_calibration_
reconciliation.md`) concluded "different populations, both correct,
nothing to withdraw" — that conclusion was itself wrong, for a reason
worth stating precisely: it validated the wrong population.

## What actually happened, in order

1. **2026-09-10**: `docs/XO_PLAN_2026-09.md` recorded "169 clean,
   non-placeholder McCoy trades stated confidence in 0.9–1.0... realized
   hit rate 78.6%," citing `scripts/print_calibration_curve.py` as the
   source.
2. **2026-09-11, dry-dock B9**: live-checked `engine/calibration_map.py`
   and found only 3 total rows, 0 populated buckets. Asked whether this
   contradicted the 169 figure.
3. **My B9 answer**: ran a direct `trades.confidence`/`trades.
   realized_pnl` query (bypassing `calibration_map` entirely) and got
   168 trades at 78.6% hit rate — close enough to 169/78.6% that I
   reported it as confirming the original finding, different mechanism,
   no contradiction.
4. **Today, checked more carefully** (prompted by being asked to verify
   this again): confirmed `scripts/print_calibration_curve.py` is not a
   separate mechanism — it's a two-line wrapper that calls `engine.
   calibration_map.load_calibration_data()` directly. That function has
   **no player_id filter at all** — it's fleet-wide. Run live: **3 rows,
   fleet-wide, ever** (every player combined). It structurally cannot
   have produced "169... McCoy" on any date — the cited source is wrong.
5. **Checked my own B9 query for the same bug**: it had no `player_id=
   'ollama-plutus'` filter either. Broke down the 168-row population by
   player: `neo-matrix` (52), `ollama-plutus` (42), `deepseek-7b-grok4`
   (23), `ollama-qwen3` (22), `navigator` (21), plus three smaller
   contributors. **McCoy was 25% of the population I said confirmed a
   McCoy-specific claim.**

## The corrected number

McCoy only, same date floor (`executed_at >= 2026-05-14`, matching the
codebase's standard `CLEAN_TRADES_WHERE`), stated confidence 0.9–1.0,
settled trades only:

**n=42, wins=30, hit rate=71.4%.**

Avg stated confidence in this bucket is still ~0.93 (same as the
withdrawn figure — the bucket definition didn't change). Gap: **~22
points overconfident**, not the withdrawn 15 points.

## What this changes, and what it doesn't

- **Withdrawn**: "169 trades," "78.6% hit rate," "15 points
  overconfident," and the citation to `scripts/print_calibration_curve.py`
  / `calibration_map.py` as the source. None of these hold up.
- **Replaced with**: 42 trades, 71.4% hit rate, ~22 points overconfident,
  sourced from a direct `trades`-table query (not `calibration_map`,
  which — separately and correctly documented in `docs/XO_BACKLOG.md` —
  covers 0.6% of settled trades fleet-wide and isn't a viable data source
  for this at all today).
- **Unchanged**: the directional conclusion. McCoy is still meaningfully
  overconfident at the top of his stated range — the corrected gap is
  *wider*, not narrower, so **Phase 1.3's fail-closed sizing rule is not
  weakened by this correction**; if anything a 42-trade sample showing a
  22-point gap is at least as strong a case for "don't let unverified
  high confidence drive position size" as the withdrawn 169-trade figure
  was. The rule itself was never built (correctly held pending real
  calibration data, per the Admiral's B9 decision) — nothing needs
  unwinding on the code side.
- **Also unchanged**: the separate, correctly-diagnosed finding that
  `calibration_map.py`'s `trade_fire`-join mechanism is fleet-wide and
  covers ~0.6% of settled trades — that finding didn't depend on the 169
  number and holds on its own evidence.

## Where the corrections landed

- `docs/XO_PLAN_2026-09.md` — Phase 1.3's "First real calibration read"
  section rewritten in place with an explicit withdrawal notice, not
  silently edited (the wrong numbers are struck through in prose, not
  deleted from history).
- `relay_2026-09-11_B9_calibration_reconciliation.md` — correction notice
  added at the top; original body kept as-written for the record.
- This document is the canonical account of what was wrong and why.

## Lesson, stated plainly

`CLEAN_TRADES_WHERE` is an **exclusion** list (`player_id NOT IN
(dalio-metals, enterprise-computer, schwab)`), not an inclusion filter
for any specific player. Any query that needs one player's data must add
an explicit `player_id = '<id>'` clause — `CLEAN_TRADES_WHERE` alone will
silently return fleet-wide results that can look deceptively close to a
single-player claim when that player is a large fraction of the fleet's
activity, as McCoy was here. This is the same class of error as an
unfiltered aggregate being mistaken for a scoped one — worth checking
explicitly next time a "does this number reconcile" question involves
`CLEAN_TRADES_WHERE`.
