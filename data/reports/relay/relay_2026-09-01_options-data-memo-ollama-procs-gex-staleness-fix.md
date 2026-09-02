# Relay — options-data decision memo, ollama process check, GEX staleness fix
**2026-09-01, evening. Three items from the Captain: two investigate-and-report, one small fix (shipped, not deployed).**

---

## Item 1 — Options-data decision memo (report only, no action taken)

### The opening question first: what's writing `gex_snapshots`, and what changes

**Correction to how I'd have answered this an hour ago.** `data/trader.db`'s
`gex_snapshots` table is written by **two independent pipelines**, and I'd
only fully traced one of them:

1. **`engine/gamma_context.py`** (Polygon-sourced, `source='polygon'`) —
   genuinely dead. Its only consumer is `war_room.py::build_gamma_block()`
   (LLM-prompt grounding text, fails safe to `""`). The 4 rows dated
   2026-09-01 18:06-18:07 with `symbol='TEST', spot_price=100.0` are **my
   own test pollution** from earlier tonight (`tests/test_gamma_context_
   rate_limiter_wiring.py`, since deleted) — the test mocked the network
   call but not the DB write, so `get_gamma_context("TEST", use_cache=False)`
   wrote a real row to the real production DB. Flagging per SACRED DATA
   RULES rather than deleting unilaterally — 4 rows, `symbol='TEST'`,
   harmless (no live code reads `symbol='TEST'`), your call whether to
   clean up.

2. **`gex_calculator.py`** (Alpaca-sourced, `source='alpaca'`) — **this one
   is live and I was wrong earlier tonight to say "zero other callers
   found."** `engine/ready_room.py::generate_ready_room_briefing()` calls
   `compute_gex_sync("SPY", force=force)` directly (line 472) — a real
   write path, not the disabled `main.py:1467` scheduler entry point I'd
   traced before. It's on a live 30-min schedule (`main.py:4768`,
   `run_ready_room`, market-hours/slot-gated). Latest real row: **2026-08-31
   19:39:53 UTC** — no row today because **2026-09-01 is Labor Day, US
   markets closed**, and the job only fires in market-hours slots. This is
   not a gap, it's the job correctly not firing on a holiday.

**Why this matters more than a provenance footnote:** this Alpaca-sourced
GEX **directly gates live paper trades today**, independent of Polygon
entirely. `engine/risk_manager.py:797-813` reads `gex_calculator.
get_latest_snapshot("SPY")` on every position-sizing check: negative total
GEX → 25% size cut; price within 1% of the call wall → **trade blocked
outright** ("dealer resistance, likely rejection"). `engine/kirk_advisory.py`
and `dashboard/app.py`'s GEX-history endpoint also read this table directly.

So: **"restore Polygon" is not "turn GEX back on."** GEX is on right now,
free (Alpaca paper account, already provisioned), and already gating real
trade sizing. What Polygon buys back is *accuracy*, not existence —
`ready_room.py`'s own inline comment (line 497-505) documents the Alpaca
walls measurably drift from Polygon's ("put_wall 740 / flip 749 vs
canonical 750 / 755") and explicitly overlays canonical (Polygon) values
over the Alpaca ones whenever canonical succeeds — meaning the system was
already designed to prefer Polygon and silently degrade to Alpaca, and it's
been running in degraded mode since the 403 started. One important risk-
gate nuance: `risk_manager.py`'s call reads `gex_calculator` directly, not
through `canonical_gex.py` — it never benefits from the Polygon overlay at
all, alpaca-only, always. It's also unguarded by any staleness check
(unlike the dashboard fix below) — a multi-day-old snapshot would gate
trades identically to a fresh one. Not touched tonight; flagging as a
follow-on if it matters (given the 30-min cadence and market-hours gating,
staleness in practice tops out around one closed weekend/holiday, not
weeks).

**Historical Alpaca rows (08-25 → 08-31) are now fully explained** — no
longer an open provenance question.

### What restoring Polygon costs

**Options Starter, $29/mo** — the same tier this project already paid for
2026-05-12 through the 2026-07-22 cancellation (per CLAUDE.md's Free Models
First section). Confirmed via Polygon's own pricing/docs pages: Starter
tier covers `/v3/snapshot/options` (the exact endpoint the daily collector
calls and the one now 403'ing), 15-minute delayed data. **Zero code
changes needed** — `scripts/hm_gex_daily_collect.py` and
`engine/options_flow_gex.py` are unchanged since they last worked; this is
purely a subscription flip. Delayed-15-min data is adequate for a daily EOD
collector's use case (it isn't intraday-latency-sensitive).

### Alternatives (real prices, researched tonight)

| Source | Cost | Coverage/latency | Integration cost |
|---|---|---|---|
| **Polygon Options Starter** (restore) | $29/mo | 15-min delayed, full chain snapshot | $0 — existing collector works as-is |
| **Tradier** (brokerage-linked) | Free with a funded/paper brokerage account | Real-time chain + Greeks | New brokerage account + new collector (not a drop-in for `options_flow_gex.py`'s Polygon-shaped fetch) |
| **ThetaData** | $29-199/mo tiered | Chain + Greeks, latency varies by tier | Collector rewrite — different response shape |
| **ORATS** | $99+/mo | Pre-computed analytics (walls/flip-adjacent, not raw chain) | Collector rewrite, possibly less rewrite since it's closer to pre-computed |
| **CBOE DataShop/LiveVol Pro** | $380/mo | The plan's own originally-stated "CBOE repoint" target | Most expensive option found — full rewrite, no existing code targets it |

**Two already-built, currently-disabled, $0-subscription alternatives
already live in this codebase:**
- `gex_calculator.py` (Alpaca) — **already running today**, see above; this
  isn't really "an alternative to restore," it's the thing that's already
  covering the gap in production.
- `engine/gex_overlay.py` (CBOE-scrape + Yahoo-chain fallback) — disabled
  same commit as the Alpaca scheduler entry (`473a4765`, 2026-05-31,
  "DISABLED HM-GEX-CANONICAL"). Re-enabling costs $0 but carries real
  Yahoo-scraping ToS/reliability risk — not something to flip back on
  without deliberately accepting that tradeoff.

### "Retire it" — what actually breaks

Five consumers total, now with the risk-gate finding folded in:

1. **`engine/risk_manager.py`** — position-size gate, Alpaca-sourced,
   already running independent of Polygon. Retiring Polygon touches this
   **not at all**.
2. **`engine/war_room.py`** (Troi/options-sosnoff's context) — Polygon-
   sourced only, already fails safe to empty string on unavailability
   (which is the current live state). Retiring Polygon formally = no
   change from today.
3. **`engine/ready_room.py`** — Alpaca primary + Polygon overlay-if-
   available. Retiring Polygon = stays on Alpaca permanently instead of
   intermittently; loses the wall/flip precision correction, doesn't lose
   the briefing.
4. **`dashboard/app.py`** (`/api/market/gex`, both endpoints) — this is the
   one genuinely user-facing consumer of the frozen Polygon-derived
   `flow_gex.db` snapshot. Now marked stale (see item 3 below) rather than
   presented as current. Retiring formally = the endpoint would need an
   explicit "GEX retired" state instead of a stale one; small change, not
   done tonight (out of scope for "small, safe, backend-only" per the
   Captain's item-3 framing).
5. **`engine/kirk_advisory.py`** — Alpaca-sourced (`gex_calculator.
   get_latest_snapshot`), same as risk_manager — unaffected by a Polygon
   decision either way.

**Bottom line for the decision:** GEX gating is not "on or off" — it's
already running in a degraded-but-functional (Alpaca-only) mode, and has
been since the 07-22 Polygon cancellation. The $29/mo restore buys back
wall/flip *precision* for the risk gate, the ready-room briefing, and the
dashboard — not GEX's existence. Not recommending either way — this is
for you to weigh against the $29/mo against how much that precision
delta matters, which I can't judge from here.

---

## Item 2 — Ollama processes (report only, nothing killed)

**Confirmed: exactly one real server.** `pgrep -fl ollama` plus a direct
listen-port check:

- **PID 300** — `ollama serve`, the real LaunchDaemon (`com.ollama.serve`),
  bound to `127.0.0.1:11434`. This is who the trader talks to.
- **PID 58429** — `llama-server` (a `ppid=300` **child** of PID 300).
  Normal Ollama 0.30+ architecture: the parent `ollama serve` process
  spawns one `llama-server` subprocess per loaded model to actually run
  inference, binding its own internal ephemeral port, not 11434. This is
  not a second competing server and not evidence of config landing on the
  wrong process — it's the same process tree, same env, same everything.
- **PID 38376** — an unrelated Python probe script, not an Ollama server.

So the "config landing on a different process" concern doesn't hold up:
there's no second server to have landed on. The `ps eww` env-var read
(`OLLAMA_MAX_LOADED_MODELS=2`, `KEEP_ALIVE=-1`) off PID 300 is reading the
actual serving process's real environment — that baseline stands.

**Two loose ends surfaced while confirming this, neither chased further
tonight (report only, flagging for whenever this gets picked up again):**

- **Refinement, not a reversal, of the earlier flash-attention finding.**
  The prior write-up said the serving process "has neither
  `OLLAMA_FLASH_ATTENTION` nor `OLLAMA_KV_CACHE_TYPE` set at all" — true
  for the env var. But PID 58429's actual command line includes
  `--flash-attn auto` (a llama.cpp-level flag, independent of the env var).
  So flash attention may still be active via runtime auto-detection even
  with the env var unset — "flash attention isn't being used" is less
  certain than the prior note implied. Doesn't change the env-var fact,
  changes confidence in the downstream inference.
- **Unreconciled:** PID 58429's `-c 10240` context-length flag doesn't
  match the parent's `OLLAMA_CONTEXT_LENGTH=16384` env var. No explanation
  found; not chased further tonight.

---

## Item 3 — Dashboard GEX staleness marker (SHIPPED, not deployed)

**Fix:** `dashboard/app.py` — new `_gex_age_days(as_of)` helper (parses
both `' '` and `'T'`-separated timestamps, handles trailing `Z`, returns
`None` only when unparseable/missing — never silently `0`). Wired into
both `/api/market/gex` (`gex_all()`, multi-ticker) and
`/api/market/gex/{ticker}` (`gex_ticker()`, single-ticker) responses as new
`age_days`/`stale` fields (`stale = age_days is None or age_days >= 1.0`).

**Why the existing `_stale_since` field wasn't enough:** traced it
(`dashboard/app.py:6819`) — it's only set on one specific serve path (an
in-memory cache entry that's merely aging during market hours,
`_canonical_gex_cached`'s "stale but usable" branch). The cold-cache /
market-closed fallback to the durable `flow_gex.db` snapshot — **the exact
branch serving the frozen 2026-07-21 data that triggered this ticket** —
never sets `_stale_since` at all. My fix computes age directly from
`as_of` on every response regardless of which serve path produced it, so
it covers the case `_stale_since` misses. Both fields now coexist on the
single-ticker endpoint; `_stale_since` untouched, `age_days`/`stale` added
alongside it.

**Battle-station / gating check (explicit ask):** searched
`engine/battle_station.py`, `engine/ready_room.py`, `engine/dynamic_
advisor.py` for any consumer of the *dashboard's* canonical/`flow_gex.db`
GEX path that gates real decisions, not just displays. Found none —
`battle_station.py` doesn't touch GEX at all. The one real gating consumer
(`risk_manager.py`) reads a *different* table via a *different* pipeline
(Alpaca, item 1 above) and was already unaffected by this fix either way.
So: nothing live gates on the data this fix marks stale — it's purely a
display-layer fix, which matches the "small, safe" framing.

**Verification:** `py_compile` clean. New test file `tests/test_gex_
staleness_marker.py`, 6 tests on the pure `_gex_age_days()` helper (no DB,
no network, no app startup) — all passing:
- None/empty/unparseable → `None`
- fresh timestamp → age ≈ 0, not stale
- the literal `2026-07-21T13:05:00` frozen value → correctly flags >40 days
  stale
- `' '` vs `'T'` separator → same answer
- 1-day threshold boundary (23h under, 25h over)

**Explicitly not done tonight (scoped out, per Frontend Ship Rule):** no
frontend HTML/JS touched. `bridge-v2.html`'s Gamma Map, `index.html`'s
0DTE-panel (which already computes staleness client-side per commit
`174a593`, 08-30 — this backend fix now gives every *other* consumer that
same signal), and `tactical.html` all still need someone to actually
render `age_days`/`stale` — none of them read it yet. That's a real
frontend task with a required browser smoke test, not something to bundle
into tonight's backend-only, no-restart change.

**Deploy status:** committed, not restarted. Per instruction, ships with
the next natural restart, not tonight.

---

## Summary for the Captain

1. **GEX isn't off** — it's degraded to a free Alpaca fallback that's
   already gating live paper-trade sizing, running since 07-22. $29/mo buys
   back precision, not existence. No action recommended either way; your
   call on the tradeoff.
2. **No split-brain Ollama server** — one real process, one normal child.
   The env-var baseline from the earlier `ps eww` read is trustworthy.
   Two small open threads (flash-attn refinement, context-length mismatch)
   flagged for later, not chased tonight.
3. **Dashboard staleness marker shipped**, tested, not deployed — commit
   below, restart at your convenience.
