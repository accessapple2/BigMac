# Relay — 2026-09-12. Bridge Correctness Pass, Phase 1 (read-only trace).

RULE #1 respected throughout: nothing in this pass deletes or rewrites
trade/trader history rows. Everything below is tracing, display code, and
one flagged (not fixed) decision-path finding.

## Item 1 — Riker "bullish, golden cross" vs Tactical Display "BEAR_CROSS"

**Not a live decision-path bug. Not stopping.**

Two entirely different, both genuinely live, regime classifiers exist:

- **`engine/regime_detector.py::detect_regime()`** — 50-day/200-day SMA
  golden-cross classifier (labels: `CRASH_MODE`, `BEAR_TREND`, `MELT_UP`,
  `BULL_TREND`, `CHOPPY`). This is Riker's ONLY regime source
  (`engine/riker_xo.py` line ~163-172, `from engine.regime_detector import
  detect_regime`). **Never produces "BEAR_CROSS"** — that label isn't in
  this classifier's vocabulary.
- **`engine/regime_ma.py::detect_ma_cross_regime()`** — 8-day/21-day MA
  cross classifier (labels: `BULL_CROSS`, `CAUTIOUS_BULL`, `CAUTIOUS_BEAR`,
  `BEAR_CROSS`, ...). Served via `/api/regime/ma-cross`. This is the one
  that feeds **`engine/regime_router.py`**, which gates real strategy
  selection in `paper_trader.buy()` (`REGIME_STRATEGY_MATRIX`, keyed
  exactly on this taxonomy) — the genuine live decision path.

Called both live, seconds apart, same SPY print (~$764.14-764.22):

| Source | Result | Basis |
|---|---|---|
| `regime_detector.detect_regime()` (Riker's input) | `BULL_TREND` | SPY +7.0% vs 200MA, +0.74% vs 50MA, 50MA>200MA (golden cross intact), VIX 15.84 |
| `regime_ma.detect_ma_cross_regime()` (Tactical Display / decision path) | `BEAR_CROSS` | SPY 8MA (765.07) < 21MA (767.36), price below 8MA, cross happened 2026-08-27 (16 days ago) |

Both are correctly computed, live, non-stale. They're just measuring
different things on different timeframes — a real, coherent market state
("long-term uptrend intact, short-term momentum has been down for 16
days") that two unlabeled panels present as if they contradict each
other. **Verified Riker's synthesis never feeds back into any execution
path** — `get_latest_recommendation()` is read only by
`dashboard/app.py` (display), `engine/q_entity.py` (a separate narrative
widget), and test files. No trading code imports it.

**Conclusion: display/labeling bug (two real regimes, no timeframe
label), not a live decision-path bug, not a model hallucination.** Riker
faithfully reported what its (different, legitimate) source told it.

## Item 2 — "TODAY — 0/2 TRADES" with two SPY losses on a Saturday

**Two separate findings — one is a live decision-path bug, flagged not fixed.**

**(a) FLAGGED, NOT FIXED — real live decision-path bug.**
`engine/battle_station_0dte.py::_trades_today()`:
```python
def _trades_today() -> int:
    today = date.today().isoformat()          # LOCAL date (server is MST, UTC-7)
    ... "WHERE date(timestamp)=?", (today,)    # timestamp stored via datetime.now(timezone.utc)
```
`_open_trade()` stores timestamps as UTC. `_trades_today()` compares
against the server's **local** date (confirmed live: `date` command
shows `MST`, `datetime.now(timezone.utc)` is 7 hours ahead). Any trade
between 5pm and midnight MST gets a UTC timestamp already dated the next
day — undercounted the day it actually happens, then falsely counted as
"today's" trade the following day. **This function gates real order
flow**: `if _trades_today() >= MAX_DAILY: <skip>` at
`battle_station_0dte.py:367`. This is exactly the class of bug the
directive said to stop on. Per NOT-IN-SCOPE ("anything in the trading
decision path"), **not touched in this pass** — flagging for a dedicated
fix.

**(b) Display bug, safe to fix (folded into Phase 2).**
The "0/2" counter (`bsTrades`, from `/api/battle-station-0dte/status`)
and the trade rows the Captain saw are **two independent data sources**
stacked in the same card with no distinguishing label:
- `bsTrades` = `d.trades_today / d.max_daily` — correct for actual
  today (Saturday, market closed → legitimately 0).
- `bsHistory` = `/api/battle-station-0dte/history` — the **last 4 trades
  ever, unfiltered by date** (each row does carry its own real date
  string, `t.timestamp.slice(0,10)`, but nothing calls out "not today").
  Sitting directly under a "Trades today" label, it reads as if those
  trades happened today when they didn't.

Not stale rows and not a wrong date filter on the counter itself — it's
an unlabeled unrelated-data-adjacency problem on the history list.

## Item 3 — Sector heatmap 0.00%/0-11 vs Game Plan's Tech +0.9%/Healthcare -4.6%

**Confirmed zero-filled, not genuinely empty. Display/fallback-logic bug, safe to fix.**

Queried the live disk cache directly (`engine.premarket_scanner._sector_disk_cache`,
refreshed live during this trace, timestamp = this exact minute):

```
Technology       chg=0.0  source=finviz
Financials       chg=0.0  source=finviz
... (all 11 Finviz-covered sectors: chg=0.0, source=finviz)
Defense/Aero     chg=0.22 source=yahoo   <- real, live, nonzero
```

`get_sector_heatmap()`'s own priority order is Finviz → Yahoo → stale
disk cache → 0.0 placeholder. Finviz is genuinely returning **0.00% for
every sector right now** (Saturday, market closed — this is Finviz's own
site behavior, not a scrape failure) and the code treats "Finviz returned
a value" as sufficient, even when that value is a flat 0.0 for all 11
sectors simultaneously — it never falls through to the stale-disk-cache
layer that exists specifically for this situation and does hold real
Friday data (proven: Defense/Aero, which skips Finviz entirely and uses
Yahoo, shows a real number at the same instant). The fallback chain's own
design intent (graceful degradation to stale-but-real data) is being
short-circuited by a technically-non-empty-but-meaningless live response.

"Tomorrow's Game Plan" (`tactical.html`, `/api/ready-room/briefing`'s
`.gameplan` field) is a **completely separate source**: a cached
LLM-generated narrative from `engine.ready_room.get_latest_briefing()`,
written once at Friday's premarket briefing generation time, with the
real numbers baked into the text and no live "as of" comparison shown to
the reader. Two panels, two sources, two different "as of" times, no
shared ground truth, no staleness label on either.

## Item 4 — Riker's reasoning starts mid-sentence "all positions. Here's why"

**Confirmed: display-only, not stored-truncated.** Fix included in Phase 2.

Traced the full storage path: `engine/riker_xo.py`'s
`_do_riker_synthesis()` caches `(body.get("response") or "").strip()`
verbatim — no truncation, no slicing, anywhere server-side.
`get_latest_recommendation()` returns that same string unmodified.
`dashboard/app.py`'s `/api/riker/recommendation` and `/api/riker/synthesis`
both return it as-is. **The stored record is not truncated.**

Found the actual mechanism in `dashboard/static/index.html` (the v1
classic dashboard's Riker card, `fetchRikerRecommendation()`, line 4934):
```js
// C1 HM-RIKER-LLM-TOKENIZER-ARTIFACT: strip <unusedNNNN> tokenizer leakage
el.textContent = String(d.recommendation).replace(/<unused\d+>\s*/g, '');
```
This exists to hide raw gemma3 tokenizer-leakage artifacts
(`<unused1234>`-style tokens that occasionally appear in Ollama's raw
output). It works as designed for genuine leakage mid-text — but when the
artifact lands where the model's verdict word should be (e.g. a garbled
"HOLD" comes back as a raw `<unused####>` token instead of the real
word), the regex silently deletes it with **no visible marker**, leaving
a grammatically-plausible sentence that starts mid-thought. `bridge-v2.html`'s
separate Riker widget (`loadWarRoomStatus()`) does **not** run this strip
at all — same underlying stored text, different rendering, so the same
incident would show *raw* `<unused####>` garbage there instead of a silent
gap. Two render paths, two different failure presentations of the same
underlying (rare) model-tokenizer glitch.

## Summary before Phase 2

| Item | Data or display | Live decision-path bug? | Action |
|---|---|---|---|
| 1. Riker vs Tactical regime | Display/labeling | No (verified no execution path reads Riker) | Phase 3 candidate: label both regimes by name/timeframe |
| 2a. `_trades_today()` UTC/local mismatch | **Decision path** | **Yes** | Flagged only, not fixed (out of scope) |
| 2b. `bsHistory` unlabeled adjacency | Display | No | Phase 2 fix |
| 3. Sector heatmap zero-fill | Display/fallback logic | No | Phase 2 fix |
| 4. Riker mid-sentence | Display (index.html only) | No | Phase 2 fix |

Proceeding to Phase 2 now — the 8 originally-listed items plus 2b, 3, and
4 above (all confirmed safe, display-only).
