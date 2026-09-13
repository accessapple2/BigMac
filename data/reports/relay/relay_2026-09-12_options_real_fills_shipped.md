# Relay — 2026-09-12. Options real fills, DEX, CBOE breakdown — SHIPPED.

Builds on `relay_2026-09-12_options_real_fills_scoping.md` (the read-only
trace). Approved and built, in the ordered sequence given. RULE #1
respected throughout: every DB change is additive (new columns, new
tables), no existing trade/options row's recorded value was rewritten.
Two real bugs were found and fixed along the way, both directly blocking
the requested work — reported plainly below, not buried.

## 1. Real fill writeback — `strategy:bull_spread_v1` and `swingdesk-manual`

Ported `alpaca_bridge.py`'s proven equity `_poll_fill()` (poll
`get_order_by_id` until `filled_avg_price` populates or a 3s timeout) into
`engine/alpaca_options.py`, wired into `submit_single_option()`,
`submit_vertical_spread()`, and `close_vertical_spread()`. Every options
order-submit function now returns `filled_avg_price`/`filled_qty`/`status`
instead of discarding them.

**Two real bugs found and fixed while wiring the close side, both
pre-existing and directly blocking it:**
- `strategies/executor.py`'s close path checked `leg.get("action")=="buy"/
  "sell"` — a key the canonical `legs_json` schema (2026-05-17) never
  writes (confirmed against real stored data, e.g. row 140:
  `{"side": "long"/"short", ...}`, no `"action"` key at all). Every real
  2-leg close since the schema changed silently mis-defaulted both legs to
  "buy" and fell through to a per-leg fallback with the identical bug —
  the atomic MLEG close (`close_vertical_spread`) has never actually fired
  for a real position. Fixed to read `side`.
- `_occ_symbol()` read `leg["expiration"]`/`leg["option_type"]` — also
  keys the canonical schema doesn't carry per-leg (expiration lives once
  on the `options_trades` row; the key is `type`). Fixed to accept an
  explicit `expiration` param and read `type` (with graceful fallback to
  the open-side payload's own shape, which does carry both).

**Sign convention — resolved structurally, not by trusting Alpaca's own
MLEG sign (still unverified, same as `docs/XO_BACKLOG.md`'s
`HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET` already flagged):** a bull call
/ bear put spread is *always* a net debit, a bull put / bear call spread
*always* a net credit — a mathematical property of which strike is worth
more, not a per-trade assumption. Alpaca's fill supplies the magnitude
only; the sign comes from the spread's own structural type. Applied
symmetrically on open (`entry_credit_debit`) and close
(`exit_credit_debit` = opposite sign of entry, closing a debit spread
returns money) — the same opposite-of-entry relationship the restatement
script's own hand-verified formula already used, not a new assumption.
`restatement_basis='real_fill'` stamped immediately on a confirmed real
fill — these rows will never need the restatement script; the number
already is the verified answer. `swingdesk-manual`'s own `poll_fill()`
got the equivalent open-side writeback (its close side is untouched — zero
historical closes exist to reason about, and it's a separate, non-MLEG
limit-order path with its own build).

11 new/extended tests (`tests/test_options_real_fills.py`,
`tests/test_swingdesk_zombie_status_sync.py`) cover: the schema-mismatch
fixes against the exact real-data shape, both structural-sign directions
on open and close, partial-close exclusion, and poll-timeout /
already-recorded idempotency.

## 2. Wired to real orders: `battle_station_0dte`, `wheel_strategy`. Flagged, not wired: `shadow_csp`

`options-sosnoff` (Troi) and `battle_station_0dte` had never submitted a
single real Alpaca order in their history (0/84 and 0/2 `broker_order_id`,
confirmed before touching anything) — pure internal simulation, entry
price from a quote that was never checked against a fill. Both now call
`submit_single_option()` (already-proven machinery, the same function
`swingdesk-manual`'s 8/8 real orders already use) before recording, with
the real fill overriding the quote when confirmed and a clean fallback to
the quote on any skip/timeout/error — never breaks the strategy's existing
behavior, only improves on it. `battle_station_0dte.py`'s
`battle_station_trades` table gained two additive columns
(`broker_order_id`, `restatement_basis`) since it had neither.
`options-sosnoff`/`shadow-qwen35-csp` added to `alpaca_options.
OPTIONS_PLAYERS` (required — neither was previously whitelisted, so a
submit call would have been silently skipped).

**`shadow-qwen35-csp` deliberately NOT wired.** Its `open_options_trade()`
call uses `book_tag="ghost"` — explicitly documented as "observation only
— scored forward vs Troi baseline," a hypothetical comparison construct,
not a real trading strategy. Placing a real order there would change what
"ghost" means for this construct (and risks doubling real exposure if the
same candidate is independently selected by both the real baseline and
the shadow arm). Flagging this distinction rather than mechanically
forcing the wiring — worth a direct decision if real fills are wanted
here too, but that decision changes the ghost-book's fundamental design,
not just its data quality.

5 new tests (`tests/test_battle_station_0dte_real_fills.py`) cover the
three-way fallback (confirmed fill / order-placed-no-fill / skip-or-error)
in isolation, no live Alpaca calls.

**Confirmed live, not SPY-only**: GEX (and now these real-order paths) are
symbol-parameterized — `gex_calculator.GEX_SYMBOLS` already includes NVDA,
live-tested working (spot $218.17, correctly structured walls) before any
of today's other GEX work touched it.

## 3. DEX alongside GEX — same loop, zero new fetch

`gex_calculator.py`'s per-contract loop already reads `snapshot.greeks.
gamma`; `delta` is on the exact same object. Added `call_dex`/`put_dex`/
`net_dex` per `GEXLevel` and `total_dex` on `GEXProfile`, computed in the
same loop that already iterates every contract for GEX — no new API call,
no new pagination. `dex_contrib = delta * OI * 100 * spot` (no spot²
term — that's specific to gamma's second-derivative nature); uses the
option's own natural delta sign (calls positive, puts negative), which
already aligns with `_gex_contrib`'s calls-positive/puts-negative
convention without introducing a new sign question. `gex_snapshots`
gained an additive `total_dex` column; per-strike DEX rides in the
existing `levels_json` blob alongside `call_gex`/`put_gex`. Live-tested on
NVDA: `total_dex=4,505,702,868` in the same call that already computed
GEX. 4 new tests (`tests/test_gex_dex.py`).

## 4. CBOE Index/Total extraction — plus a second silent bug found

`engine/alpha_signals.py`'s HTML-scrape fallback already fetches CBOE's
full daily statistics page (confirmed live: it carries TOTAL, INDEX,
EXCHANGE TRADED PRODUCTS, and EQUITY put/call ratio rows all in one
response) but kept only Equity. **Found live, before writing any fix:
the real row labels are UPPERCASE with a "PUT/CALL RATIO" suffix
("EQUITY PUT/CALL RATIO"), and the existing code's `"Equity" in cells[0]`
check is case-sensitive — it has likely never matched anything, ever,
meaning this fallback has been silently returning nothing from the HTML
path since it was written**, independent of the Index/Total question.
Fixed with exact-label matching (not loose substring — a "CBOE VOLATILITY
INDEX (VIX) PUT/CALL RATIO" row later on the same page also contains the
word "INDEX" and would have been wrongly matched by a naive check).
New additive `cboe_pc_breakdown` table (one row/day) now persists
Equity/Index/ETF/Total together. Live-tested: `equity=0.58 index=1.06
etf=0.99 total=0.86`, matches the raw page exactly. 2 new tests
(`tests/test_cboe_pc_breakdown.py`), one specifically proving the
VIX-row false-match doesn't happen.

**Not touched**: the CSV-primary sites (`bull_call_spread_v1.py`,
`bear_put_spread_v1.py`, `ready_room.py`) — their primary source,
`cdn.cboe.com/.../daily_pcr.csv`, returned `AccessDenied` (S3-style) on
every header combination tried live today; it appears to be currently
non-functional independent of anything in this repo. Extending those
three to the HTML fallback (which does work) is a natural follow-on, not
done in this pass per the item's own lower-priority framing.

## Not part of the ordered build — skipped as instructed

Vanna/charm: skipped per explicit instruction ("modest work, no clear
consumer yet"). Would need either this repo's existing Black-Scholes
machinery (`options_flow_gex.py::_bs_gamma`) or a numerical bump using
already-fetched `implied_volatility` — real but bounded effort, no new
data source needed, whenever there's a consumer for it.

## FlashAlpha validation — built separately, per its own scoped instruction

`scripts/flashalpha_gex_validation.py`. Validation only — never imported
by any trading/decision-path code, never in a prompt.

- **Symbol**: NVDA (free tier is individual equities only, confirmed via
  the service's own docs — SPY/QQQ/SPX need Basic).
- **Expiration**: pulled from our OWN already-fetched real Alpaca
  options-contracts data for NVDA (`_pick_expiration()`), nearest real
  listed date strictly after today — never invented, and costs nothing
  against FlashAlpha's budget since it's our own broker's chain, not
  theirs. Live-tested (no FlashAlpha call involved): picked 2026-09-14,
  a real listed NVDA expiry.
- **Structure only, matching today's own wall-label fix**: same sign on
  net GEX, call wall ≥ spot, put wall ≤ spot (checked on *both* sources,
  not just agreement between them), gamma flip within ±15% of spot — the
  same band this repo's own `options_flow_gex.py` zero-cross scan and
  `gex_calculator.py`'s `relevant`-strikes filter already use, not a new
  number invented for this. Wall sides are derived from FlashAlpha's own
  `strikes` array by position (their API doesn't label call/put wall
  directly on the free-tier GEX endpoint) — using the identical
  position-based logic fixed in `gex_scanner.py` today, not trusting an
  external, unverified convention.
- **Budget: exactly one call per day, hard-gated, fails closed.** A row in
  the new `gex_validation_log` table for today's date is the ONLY gate —
  no override flag exists (deliberately removed one I'd drafted, since a
  bypass switch is a foot-gun for a shared 5/day budget). A row is written
  *only* after a real FlashAlpha response comes back — a failed/skipped
  call, our own GEX compute failing, or no real expiration found all exit
  without writing a row and without spending anything, so a genuine
  failure earlier in the day doesn't burn the day's one shot.
- **Logs both values and the delta** to `gex_validation_log`, one row per
  run: full our-side and their-side spot/net_gex/walls/flip, three delta
  percentages, the structural match boolean, and a specific
  `contradiction_type` string when it fails (not a bare true/false).
- **Alerts only on genuine structural contradiction** via the existing
  `alert_channels.send_alert`, explicit in its own message text that this
  is validation, never a trade signal.

12 tests (`tests/test_flashalpha_gex_validation.py`), all mocked — **no
test in this suite makes a real network call to FlashAlpha**, since a test
run that did would itself spend part of the shared 5/day budget. Verified
live, without spending FlashAlpha's budget: the expiration-picker and our
own single-expiry GEX compute both work correctly against real NVDA data.
**The actual live FlashAlpha call itself has not been exercised** — that
would cost one of the 5, and doing it as part of this build (rather than
the one real scheduled run) would be exactly the kind of unbudgeted spend
this script exists to prevent.

**Not yet done: the cron entry.** The script is ready to schedule but
adding a live crontab line is a system-level change this repo's own
doctrine treats with real caution (`HM-CRON-EMPTY-PIPE-INCIDENT`) —
proposing it rather than installing it silently:
```
0 7 * * 1-5 cd /Users/bigmac/autonomous-trader && .venv/bin/python3 scripts/flashalpha_gex_validation.py >> logs/flashalpha_gex_validation.log 2>&1
```
(weekdays only, once, well after market open so NVDA has a live quote to
compare against — exact time is a preference call, not a technical
constraint). Say the word and it goes in using the file-based
dump/edit/diff/count-guard procedure this repo's cron doctrine requires,
never a raw pipe into `crontab -`.

## Verification

Full suite (`.venv/bin/python3`): 1237 passed, 18 pre-existing unrelated
failures (confirmed via `git stash` diff against completely unmodified
code — same 18 fail with none of today's changes present), zero
regressions. Backup-first (`trader_prerestart_2026-09-12T193921.db`,
`integrity_check=ok`), market-closed restart (PID 25772→41592,
single-writer, orphan-free, clean `ALL SYSTEMS OPERATIONAL` startup, no
import errors from any of today's changes). Live-verified post-restart:
`OPTIONS_PLAYERS` whitelist correctly includes the two new entries,
`wheel_strategy.py`/`battle_station_0dte.py` import cleanly with their new
wiring, `total_dex` persists and reads back correctly, `cboe_pc_breakdown`
holds today's real scraped values.

**What Monday's premarket should confirm** (live order flow, not code):
whether `wheel_strategy.py`'s next real SPY/QQQ CSP entry and
`battle_station_0dte`'s next real trade actually carry a `broker_order_id`
and `restatement_basis='real_fill'` — today's verification is as far as a
closed market allows.
