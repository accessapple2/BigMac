# Relay — 2026-09-12. Full day handoff (written at auto-compact).

Session resumed after a VPN drop mid-task, picked up four already-committed
pieces (Ollie seat fix + Bridge Phase 1/2/3), then ran a long chain of
work through the evening. This doc is the single, complete handoff —
detail lives in the relay docs listed inline, not repeated here. Written
because this session is ending (auto-compact) with one piece explicitly
waiting on a fresh session (the FlashAlpha cron).

## Everything shipped today, in order

1. **HM-OLLIE-SILENT-SEAT / HM-OLLIE-SEAT-OVERSIZE** (`d639064`, `a153926`,
   `ebcd94b`) — before this session's active work; already committed when
   the VPN dropped. Every olliemax caller now states its own `num_ctx`.
2. **Bridge Classic repair, Phase 1/2/3** (`7b2753b`, `3ab0b14`, `9ca8584`,
   `65a5a63`) — Phase 1 traced 4 panel contradictions (one, `battle_
   station_0dte.py`'s UTC/local mismatch, was real and became today's
   headline fix); Phase 2 shipped 11 display fixes, restart-verified;
   Phase 3 is a spec only (`docs/architecture/bridge-facts-endpoint-spec.md`),
   not built.
3. **Sentinel false alarms retired** (`95252df`) — `sys_scan_liveness` now
   gates on `is_trading_day()` (was firing hourly all weekend);
   `com.ollietrades.mlx-qwen3` (dead since 2026-09-09, no live caller)
   unloaded and archived, sentinel check commented out in place.
   **Confirmed dead later in the session**: checked `trader_error.log`
   1h28m post-restart, zero new alerts past the last real one (16:47:45).
4. **Trades-table UTC/local-date gate bug, full pass** (`9c5f671`) — the
   Bridge Phase 1 finding generalized: 15 call sites across 10 files
   compared a UTC-stored timestamp against a local date. One canonical
   fix (`engine.market_calendar.local_day_utc_bounds()`), all sites
   converted, 61 historical rows flagged (`trades.tz_bucket_suspect`,
   additive). Exposure confirmed real (a demonstrated phantom-count bug)
   but no confirmed bad historical outcome found.
5. **Phase 1.2b doc addition** (`b90fb5f`) — plan-only theme-context rider
   added to `docs/XO_PLAN_2026-09.md` per a mid-session directive from the
   Admiral. **Correction (2026-09-13):** this was flagged on arrival, but
   it was genuinely the Admiral's own input the whole time — it arrived
   attached to a tool result because that's how Claude Code delivers a
   mid-turn user message, not because anything was injected. The Admiral
   confirmed authorship directly. See "Judgment calls" below for the
   corrected read and the one case that day that actually did warrant
   caution.
6. **GEX repoint to Alpaca** (`884d090` scoping, `ea09662` build, `3719cea`
   docs) — canonical GEX now reads Alpaca (`gex_calculator.py`) as tier 0,
   Polygon demoted to fallback. Condition 1: fixed `gex_scanner.py`'s
   wall-labeling (position-based, not sign-based — the exact defect named
   `HM-DRYDOCK A1` on 2026-06-09 and never actually fixed until today).
   Condition 2: found mid-build that Alpaca's own refresh scheduler had
   been disabled since 2026-05-31 (the table wasn't actually being kept
   fresh); restored a real 15-min cadence and added a freshness gate.
   Live-verified: the real gate, the LLM prompt injection, and
   `canonical_gex()` all agree on the same repointed numbers.
7. **Bull spread audit** (read-only, no separate relay doc, folded into
   chat + this handoff) — 24 of 26 historical `strategy:bull_spread_v1`
   rows are zombie/failed/reconciliation artifacts; zero real
   `bull_call_spread` market exits ever; exactly one genuinely real,
   organically-decided outcome in the whole table (+$1.71, 2026-05-14).
   Strategy's entry gate was structurally dead for its first ~10 weeks
   (fixed 2026-07-10), one trade since. **No action taken — reviving it is
   the Admiral's call, not made here.**
8. **Options premium restatement gap-fix** (`0bdf93f`) — the 2026-09-11 B6
   restatement (`d6eaf3c`, already `DONE` per `XO_BACKLOG.md` — corrected
   a wrong "never executed" premise) had a real scoping bug: it filtered
   on `exit_date` when the pricing bug it restates lives in *entry*
   pricing. Found and closed the gap (5 rows, 4 in scope), all resolved
   `unrecoverable_no_alpaca_bar`. Also caught and fixed my own mistake
   mid-pass (a report-file overwrite) before it became a real loss.
9. **Options real-fills scoping + build** (`59fb7eb` scoping, `2649270`
   build) — traced why only one real premium-priced outcome exists across
   this system's whole options history: one strategy (`bull_spread_v1`)
   places real Alpaca orders but never reads the fill back; two others
   (`options-sosnoff`, `battle_station_0dte`) never place an order at all.
   Ported the proven equity poll-and-writeback pattern. **Found and fixed
   two more real, pre-existing bugs along the way**: the close path
   checked a field key (`action`) the canonical schema stopped writing in
   May, and `_occ_symbol()` read per-leg fields that schema doesn't carry
   either — the atomic MLEG close has never once fired correctly. Sign
   convention resolved structurally (a bull call spread is mathematically
   always a debit), not trusted from Alpaca's own unverified convention.
   Wired `wheel_strategy.py` and `battle_station_0dte` to real orders;
   **`shadow_csp.py` deliberately not wired** (its `book_tag="ghost"` is
   an explicit hypothetical-comparison construct, not a real strategy —
   flagging this is a decision point, not an oversight).
10. **DEX alongside GEX** (`4157eb8`) — same loop, same snapshot, zero new
    fetch. Live-tested on NVDA.
11. **CBOE Index/Total extraction** (`297d60b`) — plus found the existing
    Equity-only match was case-sensitive against real uppercase page
    labels and had likely never worked at all.
12. **FlashAlpha GEX validation, built** (`5121e89`) — NVDA only (free
    tier is individual equities), real listed expiration pulled from our
    own Alpaca chain, structure-only checks (same sign, correct wall
    sides, flip in band), hard-gated to exactly one call/day with **no
    override flag** (removed one deliberately — a bypass switch is a
    foot-gun on a budget shared with the Admiral's own chat use). 12
    tests, zero real FlashAlpha calls made anywhere in this session —
    the budget is untouched. **Not scheduled yet — see "What's parked."**
13. **ThetaData filed as parked** (`d98c816`) — `docs/XO_BACKLOG.md`, with
    the reasoning: answers a lower-value question (historical pricing)
    than the fills work that just shipped (future rows verifiable by
    construction), and today's restatement work found most historical
    options "profit" in this system is unverifiable anyway. Revisit
    trigger is "fills land and a real strategy emerges," not a date.

**Full test suite** (`.venv/bin/python3` — bare `python3` under-reports
today, several files import `fastapi`/`alpaca` only present in the
project venv): 1237 passed at last full run, 18 pre-existing unrelated
failures confirmed via `git stash` against completely unmodified code
across every change today. Zero regressions introduced.

**Two live restarts today**, both backup-first (`integrity_check=ok`),
both market-closed (Saturday), both single-writer/orphan-free verified.

## What's parked, and why (so nobody re-does this without reason)

- **FlashAlpha cron — waits for a fresh session, per direct instruction.**
  The script (`scripts/flashalpha_gex_validation.py`) is built, tested
  (12 mocked tests, zero real calls made), and verified live on the parts
  that don't spend budget (expiration lookup, our own GEX compute).
  Proposed cron line is in `relay_2026-09-12_options_real_fills_shipped.md`:
  ```
  0 7 * * 1-5 cd /Users/bigmac/autonomous-trader && .venv/bin/python3 scripts/flashalpha_gex_validation.py >> logs/flashalpha_gex_validation.log 2>&1
  ```
  Installing it means editing the live crontab — this repo's own doctrine
  (`HM-CRON-EMPTY-PIPE-INCIDENT`) treats that as a real, careful,
  file-based-diff operation, not a quick edit, and it's the one piece of
  today's work that's a genuine system-level change rather than a code
  change. Do it via: dump `crontab -l` to a file, edit the file (not a
  pipe), `diff` before/after, `wc -l` count-guard, only then `crontab
  <file>` — never `crontab -l | ... | crontab -`.
- **`shadow_csp.py` not wired to real orders** — deliberately. Its
  `book_tag="ghost"` makes it an explicit hypothetical scoring construct
  ("scored forward vs Troi baseline"), not a real strategy. Wiring it
  would change what "ghost" means, not just fix a data gap. Worth a
  direct decision if real fills are wanted there too.
- **ThetaData** — see item 13 above and `docs/XO_BACKLOG.md` directly for
  the full reasoning. Do not install a JRE for this on a stale premise.
- **Bull spread revival** — audited, not decided. No real record exists to
  revive from; reviving it means starting from zero track record, not
  restoring a proven one. Admiral's call.
- **CBOE CSV-primary sites** (`bull_call_spread_v1.py`,
  `bear_put_spread_v1.py`, `ready_room.py`) — their primary source
  (`cdn.cboe.com/.../daily_pcr.csv`) returned `AccessDenied` on every
  header combination tried live today, independent of this repo. The
  working HTML fallback (fixed today in `alpha_signals.py`) hasn't been
  extended to these three — natural follow-on, not done, per this item's
  own lower-priority framing.
- **Vanna/charm** — skipped per explicit instruction ("modest work, no
  clear consumer yet"). The math and data (implied_volatility, the BS
  machinery already in `options_flow_gex.py`) are already in hand
  whenever there's a reason to build it.
- **The ~40 other `date(col)=?` sites** (briefings, journals, signal
  dedup, cost tracking) — filed as a separate, lower-priority sweep. The
  trades-table version of this bug showed no confirmed bad outcome
  despite a real, checkable mechanism, so these are reasonable to leave.
- **Not touched this session, still open from before**: Phase 3
  (`/api/bridge/facts`, spec only), the leaderboard sort-direction claim
  (Bridge Phase 2, unreproduced against live data), Phase 1.3
  (superseded, not paused), the `plutus-v1`/`qwen3:8b` un-alias question
  (revisit-by 2026-09-17 per `CLAUDE.md`).

## What Monday's premarket needs to confirm (live behavior, not code)

1. **GEX repoint cadence** — `grep "Alpaca GEX: refreshed" logs/trader.log`
   should show hits roughly every 15 minutes once premarket scanning
   starts; `gex_snapshots` rows (`source='alpaca'`) should land at that
   same cadence, not the old sporadic multi-hour gaps.
2. **Real fill writeback** — `wheel_strategy.py`'s next real SPY/QQQ CSP
   entry and `battle_station_0dte`'s next real trade should carry a
   `broker_order_id` and `restatement_basis='real_fill'` in their
   `options_trades`/`battle_station_trades` rows. This is the first time
   either path will have ever done so.
3. **`strategy:bull_spread_v1`'s fixed close path** — if a real 2-leg
   close fires (the atomic MLEG close was unreachable before today's fix),
   confirm it actually submits via `close_vertical_spread` rather than
   falling through, and that `exit_credit_debit`/`pnl` land with the
   structurally-derived sign.
4. **sys_scan_liveness** — already confirmed dead this session (1h28m
   clean post-restart); no further Monday action needed, just noting it's
   closed, not open.

## A judgment call worth knowing about, not just the outcome

**Corrected 2026-09-13** — the original write-up of this section
mischaracterized one of the two cases below as suspicious-turned-genuine.
It was never suspicious in substance, only in delivery: Claude Code
attaches a mid-turn user message to a tool result, which is simply how
the harness delivers it, not a sign of injection. That case is the
Phase 1.2b rider (item 5 above) — genuinely the Admiral's own input,
confirmed directly, the whole time.

The one case that day that did warrant real caution was different in
kind, not just in delivery: a request, arriving the same way, to sign up
for and integrate an unverified external service mid-investigation,
directly contradicting an explicit "don't build" given earlier in the
same conversation. That one was not actioned — flagged instead — and the
Admiral later did that signup independently and gave a proper scoped
instruction for it afterward.

The lesson to keep is narrower than "flag anything delivered mid-turn":
the delivery mechanism (message attached to a tool result) is normal and
not itself a signal. What's worth flagging is content that contradicts a
just-given instruction, references an unverified external service, or
otherwise doesn't fit the conversation's own context — flag those before
acting, but don't treat a normal mid-turn message as suspect on
mechanism alone.

## RULE #1 statement

Nothing deleted or rewritten in place all day, across every piece of work.
Every fix was display code, a gate/dedup query, a real-order-fill
writeback (additive to newly-inserted rows, never touching a historical
one), a LaunchAgent retirement, or an additive DB column/table. Two
restarts, both backup-first, integrity-checked, market-closed,
single-writer-gated. Every relay doc referenced above is committed and
pushed to `exec-pipeline`.
