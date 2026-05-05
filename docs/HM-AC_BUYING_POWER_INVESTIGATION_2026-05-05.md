# HM-AC — Insufficient Buying Power Investigation
*2026-05-05, Scotty investigation, no fixes applied. Admiral picks remediation.*

## Question
HM-AA enrichment (commit a9d0649 + cfc452f) surfaced a flood of `submit_single error: APIError: insufficient options buying power for cash-secured put` errors immediately post-restart. Pre-HM-AA invisible (empty error body). Which code path fires these? What's the architectural shape of the fix?

## Headline finding
**The executor's spread-close path uses single-leg submits, but the 19 open `bull_put_spread` positions were submitted as multi-leg (`OrderClass.MLEG`) orders. Single-leg SELL on a put leg makes Alpaca interpret it as a fresh SHORT-PUT (cash-secured put), requiring strike × 100 × qty collateral instead of the much smaller defined-risk margin Alpaca holds against the spread.** The error flood is `exit_manager` repeatedly trying to close 19 stacked spreads via single-leg, repeatedly being rejected.

## The error signature (HM-AA enriched)

```
submit_single error: APIError: APIError('{"code":40310000,
  "message":"insufficient options buying power for cash-secured put
            (required: 217200, available: 21529.47)",
  "options_buying_power":"21529.47",
  "required_options_buying_power":"217200"}')
```

Three sample required-vs-available values from the log:
| required | available | implied SPY put count @ ~$720 strike |
|---:|---:|---:|
| $72,400 | $21,529 | ~1 contract |
| $217,200 | $21,529 | ~3 contracts |
| $362,000 | $21,529 | ~5 contracts |

All within the bull_put_spread strike range — confirms the SHORT leg of the spreads is what's getting rejected.

## Volume + cadence

- **221 pre-HM-AA empty-body errors** (line 254, before 11:45 restart) — building up since ~08:00 MST
- **Continuing post-HM-AA enriched errors** (line 257) at ~1/sec during exit_manager ticks
- **80/hour during 10:00 and 11:00** — a tight loop, not a per-tick fire
- All from `engine/alpaca_options.py::submit_single_option`

## The code path

```
strategies/exit_manager (every 5 min per main.py:2635)
  → strategies/executor.py::_close_live(intent)
    → for each leg in spread.legs_json:
       if entry_action == 'buy':   # long leg
         close_options_position(player_id, occ, qty)  # → submit_single_option(side='sell')
       else:                       # short leg
         submit_single_option(player_id, occ, qty, side='buy')  # BTC, this works
```

**The bug:** the close-long path (`close_options_position` → `submit_single_option(side='sell')`) treats the position as a standalone long. But Alpaca tracks the spread as a combined MLEG position. A single-leg SELL on a put that's part of an open MLEG spread is interpreted as **opening a fresh SHORT-PUT**, which requires cash-secured-put collateral (strike × 100 × qty per contract).

`close_options_position` source (engine/alpaca_options.py:379):
```python
def close_options_position(player_id, contract_symbol, qty):
    """Close (sell to close) a specific options position."""
    return submit_single_option(player_id, contract_symbol, qty, side="sell")
```

That docstring's "(sell to close)" assumption only holds for a true standalone long — not for a leg of an MLEG order.

## Buying-power picture

Live Alpaca paper account state (post-restart, 11:54 MST):
- **equity:** $100,101.15
- **cash:** $96,969.64
- **stock buying_power:** $43,046.79
- **options_buying_power:** $21,529 (per error message — much tighter than stock buying power because Alpaca options margin rules differ)

Open spread positions:
- **19 open** `bull_put_spread` rows, all SPY, all strategy_id=`bull_spread_v1` (halted at commit 44c80c2)
- All 19 have `broker_order_id` populated → real Alpaca paper positions submitted as MLEG
- Total credits received: ~$16.73 across the 19 spreads (matches ~$1-2 per spread)
- Total margin held by Alpaca for these spreads: opaque from our DB but the gap (cash $96.9k vs options_buying_power $21.5k) suggests **~$75k options-margin locked** — which is more than the spreads' notional defined-risk should require

## What's missing

**No pre-flight buying-power check anywhere in the broker-submit code paths.** Verified via `grep -rn "buying_power"`:
- `shared/alpaca_portfolio_sync.py:113` reads it for sync metadata only (no gating)
- `dashboard/app.py:16045` reads it for display only
- `crew/agents.py:604,742` reads it for agent prompt context only
- **Zero call sites read buying_power *before* a `client.submit_order(...)` call.**

So every submit blindly fires and lets Alpaca reject with API error. Pre-HM-AA, those errors were invisible (empty `str(e)`); HM-AA fixed the diagnostic gap, which is exactly what surfaced this issue.

## Architectural options for the Admiral

### Option A — Pre-flight buying-power check in submit_single_option / submit_vertical_spread
Add a `client.get_account()` read before each submit; skip with clear error if estimated_required > options_buying_power × safety_margin.
- **Pros:** universal defense-in-depth; catches insufficient-funds class errors before hitting Alpaca; cheap.
- **Cons:** doesn't fix the *root* issue (single-leg-close-of-MLEG-spread); just changes the failure mode from "Alpaca rejects" to "we skip locally". The 19 spreads still don't close until expiration.
- **Effort:** ~30 min (1 read + threshold check per submit function).
- **Reversibility:** trivial.

### Option B — Fix close_options_position for multi-leg spreads
Rewrite `close_options_position` (or add `close_spread_position`) to submit an OPPOSITE multi-leg order via `MarketOrderRequest(legs=[...])` with each leg's side flipped. Closes the spread atomically as Alpaca expects.
- **Pros:** **addresses the root cause**; future spreads close cleanly without pre-flight gymnastics.
- **Cons:** more invasive — needs spread-vs-single detection at the executor level (or new dedicated close-spread function); needs reversed-side leg construction; tests against Alpaca paper API.
- **Effort:** ~2 hours including verification on at least one spread close.
- **Reversibility:** medium (the new code path; the 19 existing spreads still need their first successful close to verify).

### Option C — Halt the close attempts; let positions ride to expiration
Add an early-return in `_close_live` if any leg's exec_status='open' AND strategy is among `{bull_spread_v1, bear_put_spread_v1, bull_call_spread_v1}` AND not yet within N days of expiration. The 19 SPY bull_put_spreads expire ~2026-05-15 (10 days); they self-resolve at exit anyway.
- **Pros:** zero broker-API calls until expiration; instant noise stop.
- **Cons:** abandons mid-life close optionality (TP/SL by underlying movement); the 19 spreads can drift toward max-loss without intervention. For credit spreads with $1-2 credit and $5-wide width, max loss is ~$3-4 per contract, contained.
- **Effort:** ~30 min.
- **Reversibility:** easy.

### Option D — Wait it out (do nothing)
The 19 bull_put_spreads expire 2026-05-15 (10 days). bull_spread_v1 is halted (commit 44c80c2). Each exit_manager tick continues to fire ~5-10 rejected submit_single calls; HM-U NTFY rate-limits to 1/day per error class. exit_manager will eventually succeed when Alpaca auto-exercises or expires the positions.
- **Pros:** zero engineering effort.
- **Cons:** 80/hour error rate against Alpaca paper API for 10 days. Rate-limit risk on the API. NTFY suppressed but log churn nontrivial. The 19 spreads might hit max-loss without exit-manager intervention.
- **Effort:** zero.
- **Reversibility:** n/a (no change to revert).

### Option E — Force-close via the manual `scripts/kill_bull_spread.py` helper
A dedicated script (`scripts/kill_bull_spread.py`) already exists — uses the same single-leg pattern, so likely also fails. Could be extended with the multi-leg close logic.
- **Pros:** explicit operator-driven action; doesn't change the running service.
- **Cons:** still needs the multi-leg close fix from Option B; runs as a one-shot rather than fix-in-place.
- **Effort:** mostly overlaps with Option B.

## Recommended (but Admiral picks)

**Option C as immediate noise-stopper, with Option B as the proper fix.**

C buys 10 days. B is the right architectural answer but takes ~2 hours and benefits from being unhurried (test with at least one spread close before broad rollout). Option A is a useful defense-in-depth addition orthogonal to B/C — catches future cash-tight scenarios across all submit paths.

**Doing nothing (Option D) is acceptable today but accumulates error log noise for 10 days. NTFY suppression keeps it from being annoying; the underlying tight loop just chews logs.**

## Trade-offs the Admiral should consider

1. **Today's risk is bounded.** No real money at stake; Alpaca paper rejects are harmless except as noise. HM-U NTFY suppresses to 1/day.
2. **Option C is the lowest-risk noise-stop**: short fix, easily reversible, breaks no current behavior beyond stopping noise.
3. **Option B is the only path that actually closes 19 stacked spreads pre-expiration.** If the spreads need exit-management (TP/SL on underlying movement), B is required; if let-them-expire is acceptable, C suffices for 10 days.
4. **Option A is a fence; it prevents the noise but doesn't free the 19 spreads either.** Useful as part of a B+A bundle.
5. **The buying-power gap is bull_put_spread-specific:** the SHORT-PUT collateral requirement is what creates the cash-secured-put framing. Other spread structures (bull_call, bear_put) submit-then-close via different leg semantics; the bug may be wider but unobserved while bull_call/bear_put are rarely halted.

## Open questions for the Admiral

1. **Are the 19 bull_put_spreads worth closing pre-expiration?** Each spread max-loss ≈ $3-4. Total potential additional loss if held to expiration vs closed early: bounded by max_loss × 19 = ~$60-80. Probably not worth the engineering urgency of Option B if the answer is "let them expire."
2. **Was the bull_put_spread CHOICE intentional vs a side-effect of bull_spread_v1's IV-rank logic?** Per `evaluate()` line 170-171, spread structure is `bull_call` if IV rank < 40, else `bull_put`. SPY IV likely ≥40 most of the time, so bull_put dominates. That's spec, not bug.
3. **Should the submit_single_option API surface refuse to close MLEG-leg positions explicitly?** That's the safer-by-design version of Option B — refuse the operation rather than silently fall through to Alpaca rejection.
4. **The bear_put_spread_v1 strategy** has the same close-via-single-leg path. Has it ever fired and closed? If yes, it has the same bug latent. (Probably never fired at scale yet given today's halts and gate-flip recency.)

## What I (Scotty) deliberately did NOT do
- Did not patch `close_options_position` or any submit function (no Option B)
- Did not add pre-flight buying-power checks (no Option A)
- Did not halt exit_manager close attempts for spread strategies (no Option C)
- Did not force-close any of the 19 spreads (no Option E)
- Did not pick A/B/C/D/E (Admiral picks)

The Admiral reads this doc, picks A/B/C/D/E (or hybrid), implementation lands as a separate session prompt with the chosen option pre-baked.
