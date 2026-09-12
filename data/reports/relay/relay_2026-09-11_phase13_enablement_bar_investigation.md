# Relay — Phase 1.3 enablement-bar investigation: gate location, coverage, trade_id

Answers to the Admiral's three tightened pre-enablement questions, all directly
verified against live code/data this session, not inferred from docs.

## 1. Gate location — ABSENT from McCoy's real execution path

`composite_alpha >= 0.3` (the Sniper spec's pass requirement) is a named check
in exactly one place: `engine/crew_scanner.py:3909`, inside `_scan_single_agent()`
— the `SCAN_PAIRS` scan tier. Direct grep confirms zero references to
`composite_alpha` in `engine/risk_manager.py` (where `UNIVERSAL_MIN_CONVICTION`
lives), `engine/learning_engine.py`, `engine/regime_router.py`, or
`engine/ai_brain.py`. `paper_trader.py`'s bench-rating gate has none either.

McCoy's `SCAN_PAIRS` entry has been commented out since 2026-09-09 (unrelated,
pre-existing), and even before that this path produced 272 decisions / 0
executed trades in its final 30 days (already traced, `relay_2026-09-11_
mccoy_pipeline_trace_and_funnel.md`).

Checked separately: does `trade_fire` (the event calibration depends on)
originate from a different path than the one just audited? No — `trade_fire`
is written in exactly one place, `paper_trader.py:1818-1820`, inside the same
`buy()` Phase 1.3 hooks into. There is no second path producing it. So this
isn't "the check exists somewhere McCoy's fires come from but gets skipped" —
the check and McCoy's real fires are on structurally separate code paths.

**Verdict: absent**, not present-but-bypassed and not checked against the
wrong table — the real chain never attempts the check at all.

## 2. Coverage — universe mismatch, not a refresh/calculator bug

Measured against McCoy's actual live screen today
(`engine.mccoy_screen.get_mccoy_screened_symbols()`, 46 symbols) — not the 10
trades that already fired, which are a post-gate, filtered subset and the
wrong denominator:

**1 of 46 (2.2%)** of McCoy's screened candidates are inside
`ALPHA_UNIVERSE` (`engine/alpha_signals.py:39`) at all. That list is a fixed,
hardcoded 24 symbols (SPY, QQQ, TQQQ, NVDA, TSLA, AAPL, AMD, META, MSFT,
GOOGL, AMZN, MU, AVGO, PLTR, COIN, BAC, MARA, SOFI, NFLX, MRVL, SMR, XLE,
INTC, STAA) — mega-caps plus a few popular ETFs/leveraged products. McCoy's
real screen surfaces small/mid-cap movers (today: ACVA, RWT, COO, LMUB, NAVN,
etc.) that were never in scope for this list.

Split of the original 5 no-alpha names (NU, P, TSLL, LMT, NUKZ): **all 5 are
not in `ALPHA_UNIVERSE`** — zero are "in-universe but missing that day."
Refresh health checked directly and is clean: all 24 `ALPHA_UNIVERSE`
symbols have exactly 64/64 consecutive daily rows, 2026-04-09 through
2026-09-10, no gaps, no per-symbol dropout.

**Verdict: this is a universe-scope mismatch, not a calculator or refresh
bug.** `alpha_signals.py` was built against a small, fixed watchlist that
predates Phase 1.2's dynamic screened-candidate design and has never been
extended to match it. At 2.2% overlap, Phase 1.3's alpha ladder is
structurally unable to score the vast majority of what McCoy actually
screens, regardless of anything in `phase13_sizing.py` itself.

## 3. trade_id — filed with an owner, but the "not populated" framing was wrong (corrected)

Filed: `docs/XO_BACKLOG.md` line 94, owner **Scotty**, target date
**unslotted** — no decision made yet between its two named directions.

**Correction, made directly in `engine/phase13_sizing.py`'s docstring this
session:** the framing "most real trades never get a populated trade_id" —
which I wrote into that file's first rebuild and repeated in chat — is
factually wrong. Verified directly against `decision_audit`/`trades`:

| | count |
|---|---|
| total `trade_fire` events, fleet-wide | 198 |
| with a populated `trade_id` | 198 (100%) |
| `trade_id` resolves to a real `trades` row | 198 (100%, 0 dangling) |
| matched trade is still open (no realized P&L) | 195 |
| matched trade has settled | 3 |

`trade_id` is populated on every single `trade_fire` event and joins
cleanly every time. **The actual constraint is settlement lag, not a logging
gap**: 195 of 198 matched trades haven't closed yet, so there's no realized
P&L to score calibration against. This isn't fixable by backfilling
anything — a position's P&L doesn't exist until it closes, and closing
faster isn't a code change. `engine/phase13_sizing.py`'s docstring is
corrected to reflect this; the backlog row itself (line 94) already gets the
practical conclusion right (few settled trades) even though its phrasing
leans on "linkage" language that reads more like a logging-gap than a
settlement-lag story — worth a follow-up edit to that row for precision,
not done in this pass.

## Where this leaves Phase 1.3

Built, committed (`a197a9b`), both flags `False`. Per the Admiral: stays off
until all three of the enablement conditions are met — (a) the live gate
chain rejects `alpha < 0.3` and missing alpha before sizing ever sees the
trade (currently: no such rejection exists anywhere in the real chain — see
#1), (b) McCoy's real candidate-alpha coverage is known (now measured: 2.2%
today, and structurally limited by `ALPHA_UNIVERSE`'s scope, not a bug to
patch in `phase13_sizing.py`), and (c) `trade_fire` events join to real
settled fills so calibration is scoring against a real table (currently:
198/198 join, 3/198 settled — a volume-of-closed-trades problem, not a
join-integrity problem). None of these three are Phase 1.3 code changes;
all three are upstream of it.

## Status: CLOSED (Admiral, 2026-09-11) — built-and-off, not pending

Phase 1.3 is done for this session. Its three preconditions are structural,
not a "wait and recheck" list: (a) no gate rejects low/missing alpha
anywhere McCoy's real chain runs — the check exists but on a bench McCoy
doesn't execute; (b) alpha covers 1/46 of what McCoy actually screens
today, not a transient dip; (c) calibration's blocker is settlement lag,
not something that resolves by re-running a script. None of the three
change on their own. The real finding underneath all three is the same
one: **alpha isn't a usable dimension on McCoy's live path today** — see
`relay_2026-09-11_alpha_universe_dynamic_spec_A.md` for the two options
this surfaces (make the universe dynamic and add a real gate, or drop
alpha as a McCoy dimension entirely) and the cost/location spec for the
first option, not built, pending an Admiral choice.
