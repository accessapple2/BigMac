# TZ assumptions across the stack — for the Option B holistic review (2026-06-01)

The scanner_status/holdings_top skew and the grid-RED-won't-clear are symptoms of FOUR layers
each assuming a different timezone. Point-fixes (Option A; a gate localize swap) clear visible
symptoms but the cure is standardizing ONE convention. Audited empirically (box AZ=13:00 / UTC=20:00
/ ET=16:00).

## The four layers + their assumption
| layer | tz behavior | evidence |
|---|---|---|
| **box / host** | America/Phoenix (AZ = MST, UTC-7, **no DST**). naive `datetime.now()` = AZ. | `date` = 13:00 MST; clean `datetime.now()` = 13:00 |
| **TimezoneRoute middleware** (app.py:670/687) | assumes EVERY response timestamp string is **UTC**, converts → AZ, **strips offset** (space-format). | movers UTC-emit → displays AZ 13:00 ✓; scanner_status AZ-emit → double-shift 06:00 (pre-Option-A) |
| **source_gate `_market_aware_age`** (line 215) | assumes naive timestamps are **ET**, `mc.ET.localize`. | movers fresh (as_of=now) computes age=3h (ET-vs-AZ gap) |
| **db_max producers** | **MIXED** — some write AZ-local, some UTC. | signal_outcomes `12:52`≈AZ-now; **intelligence `19:52`≈UTC-now**; signals `07:38`=AZ morning |
| **file_mtime / epoch resolvers** | **UTC** (`datetime.utcfromtimestamp`) | schwab_snapshot, any epoch source |

## Per-source served-tz (enabled sources)
| source | resolver | served tz |
|---|---|---|
| movers, scanner_status*, holdings_top*, bridge_consensus, riker_synthesis†, cto_briefing, macro, morning_brief | bridge_iso | **AZ** (middleware) |
| signal_outcomes, signals | db_max | **AZ** (producer-local) |
| **intelligence** | db_max | **UTC** (producer) |
| daily_snapshot, predictions, execution_log, kirk_advisory | db_max | producer-dependent (daily/weekly-tolerant; TBD) |
| schwab_snapshot | file_mtime | **UTC** |
| metals | manual | none |

\* scanner_status/holdings_top: Option A makes them emit UTC → join the bridge-AZ group.
† riker_synthesis still emits AZ → still middleware-double-converted (daily-tolerant; not yet fixed).

## Why no single gate-localize constant works
`_market_aware_age` localizes ALL naive timestamps as one tz. But inputs are AZ **and** UTC.
- localize-as-ET (today): wrong for all (off 3h vs AZ, 4h vs UTC).
- localize-as-AZ (the proposed 1-liner): correct for bridge + AZ-db_max, **wrong for intelligence/schwab/epoch (UTC)** → would mis-age/flip them.
There is no constant that's right for a heterogeneous-tz input set. **Held — do not point-fix the gate.**

## The real cure (Option B bundle — deliberate, tested, not rushed)
Standardize on **UTC end-to-end**; localize ONLY at the browser/display edge:
1. **Resolvers return tz-AWARE UTC.** `_resolve_ts`: bridge values are middleware-AZ → tag/convert to
   UTC; db_max → normalize each producer to UTC (or fix producers to write UTC); file_mtime already UTC.
   Then `_market_aware_age` compares aware-UTC to aware-UTC — no localize guess.
2. **Middleware**: stop mangling for machine consumers. Either (a) make `_to_arizona` offset-aware
   (don't assume UTC, don't strip offset) — Option B; or (b) localize only in the frontend, serve UTC
   from the API. (b) is the cleaner long-term shape.
3. **Producers**: write UTC to the DB (signal_outcomes/signals/intelligence consistently).
Each step independently testable; do NOT ship as one big-bang. This stops the whack-a-mole; the
point-fixes (Option A) only clear the visible symptom.
