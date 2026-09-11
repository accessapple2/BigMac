# Relay — McCoy pipeline trace, decision funnel, gate-chain map. 2026-09-11 (dry-dock)

Full trace requested after A4 was halted on a wrong premise (Gate 7 was
about to be restored on a path — `_scan_single_agent`'s alpha-squad
rotation — that turns out to govern almost none of McCoy's real trading).
This is the sixth premise this week that turned out wrong on inspection;
the dry-dock stopped for it as instructed.

## The map — four mechanisms, not three

**1. Arena `run_scan()` (`ai_brain.py`) — THE real McCoy path.**
`main.py::run_scanner()`'s tiered loop (was `_SCAN_TIER2`; now the A3
twice-daily screened scan) → `arena.run_scan()` → `provider.analyze_chain()`
(real Ollama call) → `parse_decision()` → `TradeDecision` → **`paper_trader.
buy()` directly**. Output: `decision_audit` + `signals`. This produces
essentially all of McCoy's real decision volume.

**2. `mccoy_rules()` via `RULES_SCANNERS` — dead, now retired.**
`ollama-plutus` was never a member of `RULES_SCANNERS`
(`dayblade-0dte, capitol-trades, dalio-metals, navigator, deepseek-7b-
grok4, holly-scanner` — confirmed by direct read). `_scan_rules_agent()`
has exactly one live caller, iterating that list. The `elif player_id ==
"ollama-plutus": decision = mccoy_rules(...)` branch was structurally
unreachable in production. **Removed** from the live dispatch in
`engine/crew_scanner.py` (commit below); `mccoy_rules()` itself is left
intact — `engine/weekend_backtest.py` still calls it directly for
backtesting.

**3. `ollie_auto_check()` — real, but a different identity entirely.**
Scheduled `every(10).minutes` via `run_ollie_extended_scan`, pre/post-
market only. Its own docstring: entry triggers include *"Signal Center
Grade A (score>=75) or B (score>=60)"* — **this is where the spec's grade
condition already lives, live, today** — but it executes as player_id
`ollie-auto`, not `ollama-plutus`. Doesn't touch McCoy's attribution at
all. `ollie-auto` itself: `halt_mode='exit_only'` since 2026-07-17
("Proving Ground kill_warning unacknowledged for 10 days... pending
manual ship/kill decision" — still unresolved two months later). 5 trades
2026-08-25 to 08-31, none since. Filed to `docs/XO_BACKLOG.md` as a
decision item, same as #4 below.

**4. Alpha-squad `_scan_single_agent()`/Gate 7 via `SCAN_PAIRS` — dormant,
comment corrected.**
`ollama-plutus` was a member of `SCAN_PAIRS` (feeds `_scan_single_agent`,
scheduled every 2 min via `run_crew_scanner_job`, reaches real
`paper_trader.buy()` — this is where Gate 7's `SNIPER_ALPHA_THRESHOLD`/
`SNIPER_MIN_CONFIDENCE` actually live). Commented out 2026-09-09, "paused
for TODAY ONLY" over a VRAM collision with `qwen3:30b-a3b` — never
restored, comment never corrected, two days stale. **`crew_decisions`
table, 30 days: 272 decisions for ollama-plutus, 0 executed.** Activity
stopped exactly 2026-09-09, confirming the pause. The VRAM-collision
reason is now moot (live-verified 2026-09-11: the 30B isn't even loaded
on olliemax — only `plutus-v1`/`fin-r1` resident, ~11.9GB of a now-larger
budget) — but that's not a reason to silently restore it, since it was
already producing zero trades while active. Comment in `engine/
crew_specialization.py` corrected to say what's actually true (dormant,
pending a ship/kill decision) and filed to `docs/XO_BACKLOG.md`.

## Where `sizing_multiplier` actually lives

`paper_trader.py::buy()` already accepts `sizing_multiplier: float = 1.0`
(line 762) and applies it at line 1663-1667 (`qty = round(qty *
sizing_multiplier, 4)` when `< 1.0`). **The only live caller that passes
it explicitly is `crew_scanner.py:3417` (`troi_caution_multiplier`)** —
for the Arena/McCoy path, `buy()` is called with no explicit
`sizing_multiplier`, defaulting to full size (1.0) always. This is the
real, clean, already-existing hook for Phase 1.3's alpha-scaled sizing —
currently unused for McCoy.

## The full 30-day funnel for ollama-plutus (Arena path)

| Stage | Count |
|---|---|
| Signals emitted (`signals` table, all types) | 11,247 (9,862 BUY + 1,379 HOLD + 6 options) |
| `execution_status = REJECTED` | 9,856 |
| `execution_status = SKIPPED` (the HOLDs — never attempted) | 1,379 |
| `execution_status = PENDING` | 9 |
| `execution_status = EXECUTED` | **2** |
| `execution_status = SIMULATED` | 1 |
| Real completed trade (`trades` table) | **1** |

(Small EXECUTED=2 vs. trade_fire=1 discrepancy noted, not chased further
tonight — doesn't change the conclusion.)

**Gate-reject reasons, ranked (30 days, `decision_audit.gate_verdict`,
n=14,092 total, top 20 shown):**

| Reason | Count | % of rejects |
|---|---|---|
| `stale_signal` | 2,920 | 20.7% |
| `[HM-MARKET-CLOSED] after_hours` | 2,749 | 19.5% |
| `LOW_CONVICTION: 60% below 65% minimum` | 1,605 | 11.4% |
| `[HM-MARKET-CLOSED] before_hours` | 1,583 | 11.2% |
| `Blocked by learning engine` | 1,563 | 11.1% |
| `regime_mismatch` | 1,324 | 9.4% |
| `REGIME-ROUTER: long_equity not approved in BEAR_CROSS` | 761 | 5.4% |
| `BENCH: rating D (40/100)` | 421 | 3.0% |
| `LOW_CONVICTION` (other %, several buckets) | ~217 | 1.5% |
| `[HM-MARKET-CLOSED] weekend` | 72 | 0.5% |
| `GRADE-B-FLEET-GATE: regime=CAUTIOUS_BEAR` | 8 | 0.1% |
| GEX dealer-wall rejects (several symbols) | ~12 | 0.1% |

**Market-closed rejects alone (after/before-hours + weekend) = 4,404, or
31.2% of all rejects** — a direct byproduct of the pre-A3 continuous
2-hour-interval, full-universe firing pattern, which fired around the
clock including outside market hours. **A3's cadence cut (market-hours-
only firing, 9:35 AM / 12:30 PM ET) should eliminate this entire category
at the source.**

## The big question, answered

**Both, but the gate chain is doing almost all of the work, and it's
already a real, rich, mostly-effective system — not empty.** 9,862 BUY
signals -> 2 execution attempts -> 1 real trade is a ~0.01% conversion
rate. Two real, already-live mechanisms account for most of the
non-market-hours rejects:

- `engine.risk_manager.UNIVERSAL_MIN_CONVICTION = 0.65` — a global
  confidence floor, already stricter than Gate 7's dormant
  `SNIPER_MIN_CONFIDENCE=55` (though looser than the Sniper spec's
  intended 70%). This, not Gate 7, is the real confidence gate in
  McCoy's actual path.
- `engine.learning_engine.apply_learning()` — a real, already-live
  adaptive mechanism reading `model_adjustments` (per-player, dated,
  confidence/other modifiers) and `daily_lessons` (recent D/F-graded
  losses by symbol) to block or modify a trade before execution.
  Conceptually adjacent to the calibration-map work from the earlier
  dry-dock session, structurally different (rating-based, not a
  stated-confidence-vs-realized-hit-rate binned map) — Phase 1.3 needs to
  integrate with this, not duplicate or ignore it.

**Removing the market-closed rejects (A3 already does this) drops the
funnel from 14,092 to roughly 9,688 gate rejects for the same signal
volume** — real progress, but the remaining categories (stale_signal,
low_conviction, learning-engine blocks, regime filters, bench rating) are
genuine gate-chain rejections that will still apply to market-hours-only
signals. **Phase 1.2's cadence cut was necessary but was solving the
volume/cost side of the pipe. Phase 1.3, correctly re-scoped around
`paper_trader.buy()`'s real gate chain (`risk_manager.
UNIVERSAL_MIN_CONVICTION`, `learning_engine.apply_learning`, the regime
router, the bench-rating gate — not Gate 7), is what governs whether
anything actually survives to become a trade.** Both phases are real and
necessary; they're not solving the same problem.

## Not done tonight

Phase 1.3 itself is not built — this is the map it was requested against.
`sizing_multiplier` wiring, `UNIVERSAL_MIN_CONVICTION` restoration to
70%, and `learning_engine` integration are all real, scoped follow-ups
once the Captain confirms this map.
