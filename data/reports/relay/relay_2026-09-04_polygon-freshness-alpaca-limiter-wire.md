# Polygon $29/mo decision: two free fixes wired, Monday's bk_orb measurement decides the rest

Admiral was evaluating whether to buy Massive/Polygon's Stocks Starter
($29/mo, unlimited calls, 15-min delayed) to fix the 09-01 429 storm
(37,174 Polygon + 777 Alpaca 429s/day). Before buying, asked two questions
that turned into two live-verified findings and two shipped code fixes.

---

## What was found, live-verified, not just reasoned about

**Free Polygon tier is worse than assumed.** Tested the live key directly
and confirmed against Massive's own pricing page: Stocks Basic (free) =
**End-of-Day data only**, 5 calls/min — not "15-min delayed," genuinely
no same-day intraday bars at all. A burst of 8 rapid calls succeeded 3
times then hit 429 with the API's own text: *"exceeded the maximum
requests per minute... upgrade your subscription."*

**Concrete proof of a real correctness bug, not just noise.** Called the
real production `get_intraday_candles('MSFT', '5m', '1d')` — Polygon
succeeded silently (HTTP 200, no error), returned 117 candles all dated
2026-09-03, last bar **22.3 hours stale**. Fed unmodified into the real
`detect_breakout('MSFT')` — produced a live "BEAR breakout" signal
(score 45.7) timestamped as if detected today. That result feeds
verbatim into every AI trading agent's prompt via
`build_breakout_prompt_section()` (`engine/providers/base.py:892`) and
`build_dayblade_breakout_section()` (`engine/dayblade.py:741`). This is
not hypothetical — it's what the system does on any scan burst where
Polygon happens to land inside its free-tier budget.

**Alpaca free (IEX feed) is genuinely real-time, not delayed.** Tested
live with `feed=DataFeed.IEX` — bars came back right up through market
close, no embargo. Confirmed production code already uses `feed="iex"`
explicitly (the team already discovered and worked around Alpaca's
"recent SIP data" restriction on the free tier). Real limit: 200 req/min,
confirmed in `engine/rate_limiter.py`'s own docstring — which also
revealed an **already-built, unused token-bucket limiter** (150/min
conservative cap) wired into `deep_scan.py`/`strategy_rotator.py` but
never into `market_data.py`, the exact unmanaged path that took 777
Alpaca 429s.

**The 09-01 fix (`18cf837`) does not already solve the Alpaca side.**
Pulled its own commit message: *"does not reduce Alpaca's own call volume
(removes the wasted Polygon attempt only, Alpaca is still tried exactly
as before)."* The Admiral's initial hypothesis that this was already
fixed didn't hold — confirmed directly from the fix's own text, not
inferred.

---

## Two free fixes shipped, commit `5f30ad2`, code only — not deployed

**1. Polygon freshness check** (`engine/market_data.py`,
`_do_polygon_fetch`): named constant `_POLYGON_STALENESS_THRESHOLD_HOURS
= 1.0`. Any Polygon response whose newest bar exceeds it is rejected and
logged (`HM-CB Polygon candles stale for {symbol}: ...h old ...
rejecting, falling back to Alpaca`), falling through the existing
cascade exactly like a normal Polygon failure. Does not set the 429
cooldown — staleness isn't a rate-limit signal. Accepted side effect:
`interval="1d"` callers (e.g. `gap_scanner._get_daily_candles`) will
routinely trip this by construction (a completed daily bar is
timestamped at that day's start) and fall through to Alpaca too, which
serves daily bars fine.

**2. AlpacaRateLimiter wired into `market_data.py`**: `limiter.acquire()`
added at all three real HTTP call sites that produced the 777 429s —
`get_intraday_candles`'s HM-CA block, `get_alpaca_bars` (both the batch
call and its per-symbol fallback loop), `_alpaca_bulk_bars_chunk`
(`_BULK_BARS_PARALLELISM=8` concurrent workers now coordinate through the
same shared, thread-safe bucket every other caller already does). Each
acquired immediately before its network call, not earlier — a request
that fails before reaching Alpaca (missing creds, bad interval) never
spends a token.

**Tests:** `tests/test_polygon_candle_freshness.py` (+6: reject/accept/
boundary/no-cooldown-on-staleness/logs-on-reject/daily-interval side
effect), `tests/test_alpaca_rate_limiter_wiring.py` (5 new: all three
call sites acquire, per-symbol fallback acquires per-call not just once,
no-creds path never touches the limiter). **69 passed, 0 failed** across
every touched test file. `py_compile` clean.

**Not deployed tonight.** Goes live with the already-planned reboot, same
as everything else queued for that restart — no separate trigger.

---

## What Monday answers

Neither fix reaches `bk_orb_scanner.py` — it has its own direct Polygon
path (`_fetch_minutes_polygon`) with **no Alpaca fallback at all**, 150
tickers/cycle, ~44 cycles/day (09:46–12:00 ET), already ~10-30x over
Polygon's free 5/min ceiling per the 09-01 analysis. Its own failure log
line (`bk_orb: polygon minutes {symbol}: ...`) already exists and is
countable — no change needed to measure it.

**The measurement:** after both fixes are live (post-reboot), is
`bk_orb_scanner.py` still returning nothing on ~44 cycles/day? If yes —
Starter is justified, Admiral buys it. If the rest of the system is
healthy and bk_orb is the only casualty, the choice becomes giving it a
fallback vs. paying, decided separately.

---

## Files changed

`engine/market_data.py`, `tests/test_polygon_candle_freshness.py`,
`tests/test_alpaca_rate_limiter_wiring.py` (new). Commit `5f30ad2`. No
ledger, no restart, no deploy tonight — reboot carries this live along
with everything else already staged for it.
