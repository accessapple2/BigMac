# Scotty — Phase 6 blockers / open questions

Generated 2026-05-10 20:25 MST during Phase 6.0 discovery.

## Q1 — Plan imports wrong module paths

The plan's `engine/momentum/premarket.py` template imports:

```python
from engine.momentum.flags import get_flags_bulk          # ❌ module doesn't exist
from engine.momentum.universe import get_universe         # ❌ module doesn't exist
from engine.providers.alpaca_provider import get_snapshots # ❌ module doesn't exist
```

Correct paths (confirmed by reading existing `engine/momentum/race.py`):

```python
from engine.market_data import get_bulk_snapshots         # ✅ engine/market_data.py:318
from engine.universe import get_active_universe           # ✅ engine/universe.py:112
from engine.momentum.race import _market_status_now       # ✅ reuse existing window helper
```

`engine.momentum.flags` simply doesn't exist anywhere. See Q2.

**Recommend:** rewrite the engine template with correct paths during Phase 6.1.

## Q2 — `engine.momentum.flags.get_flags_bulk()` doesn't exist

The plan's UI shows earnings / squeeze / lowfloat flag chips on each pre-market hit. The function `get_flags_bulk()` is called but does not exist anywhere in the repo.

Options:

- **A. Skip flags for v1.** Ship Phase 6 with `flags: []` always. Add `engine/momentum/flags.py` as a future ticket. **Recommend.** Pre-market gap + direction is already useful without the flag overlay; flags can be a follow-up.
- **B. Build a minimal earnings-only flag.** Read `data/earnings_cache.json` for tickers near earnings and tag them. ~30 lines of code added to Phase 6.1.
- **C. Block Phase 6.** Build a full `flags.py` first as a separate ticket.

Recommendation: **A**. If you want **B**, say so and I'll bundle the minimal earnings flag.

## Q3 — Legacy `engine/premarket_scanner.py` already exists

This is a 520-line existing module with:
- `scan_premarket_gaps()` — yfinance per-symbol over `config.get_effective_watchlist()` (~15 symbols), filters at 2% with no volume filter
- Finviz watchlist scraper that writes to DB table `premarket_scan`
- `analyze_gaps_with_ai()` AI catalyst analysis
- Output to `data/premarket_gaps.json`
- Live launchd job `com.trademinds.premarket.plist` at 6:00 AM MST daily
- Consumer: `engine/ai_brain.py:898` calls `/api/premarket-gaps` with a 5-minute cache

The plan's new `engine/momentum/premarket.py` would be a different design entirely:
- Full active universe (~hundreds of tickers) via `get_active_universe()`
- Single batched Alpaca snapshots call (not per-symbol yfinance)
- 60s polling, not nightly cron
- UI consumer (`/api/momentum/premarket`), not AI brain

**These do not conflict if both are kept.** They serve different consumers. The new endpoint is parallel.

**Recommend:** keep both. Build `/api/momentum/premarket` alongside `/api/premarket-gaps`. Don't touch the legacy scanner or its AI brain consumer.

Side note: `/api/premarket-gaps` returned an **empty/zero-byte response** during discovery — the legacy scanner appears to be having issues. **Functional gap, separate from Phase 6.** Filed below as Q6.

## Q4 — Alpaca pre-market volume field semantics

Plan filter: `premarket_volume ≥ 50,000` shares. The Alpaca snapshot field that returns "cumulative pre-market session volume" is not explicitly named — common candidates:

- `dailyBar.v` — most likely candidate; should be cumulative session volume so far, which during 4–9:30 ET would equal pre-market volume.
- `minuteBar.v` — only current minute, not cumulative.
- Sum of multi-bar query — heavier; not what `get_bulk_snapshots` returns.

**Recommend:** use `dailyBar.v` and log a sample value at engine boot. If it shows 0 during pre-market hours, fall back to "no volume filter for Phase 6 v1" and revisit after Alpaca docs review.

## Q5 — Race desktop sidebar nav STILL missing

Carry-over from Phase 4-static.0 discovery. The Race tab — where Phase 6's tile will mount — has only a **mobile** nav entry at line 12796 of `dashboard/static/index.html`. There is **no desktop sidebar entry**. Phase 4-static.3 (the ~3-line addition) was never executed because the user halted before approving Q2 in that discovery.

**This means Phase 6's pre-market tile will be invisible to desktop users** unless we also bundle the nav fix.

**Recommend:** add the desktop sidebar Race entry as part of Phase 6.3 (UI tile commit) or as a tiny Phase 6.0b commit. Single line:

```html
<div class="sidebar-item" onclick="showSection('race');if(typeof raceStart==='function')raceStart()"><span class="icon">🏁</span> Race</div>
```

Inserted at line ~2173 between "Sniff Scan" and "Backtest" in the main sidebar block.

## Q6 — Legacy `/api/premarket-gaps` appears broken

Discovery `curl --max-time 30 http://localhost:8080/api/premarket-gaps` returned empty body / failed JSON parse. The legacy `scan_premarket_gaps()` does per-symbol yfinance calls; likely timing out or returning malformed output. AI brain consumer at `engine/ai_brain.py:898` would be affected silently (it catches exceptions and uses a 5-minute cache).

**Out of scope for Phase 6** but worth flagging for a future investigation ticket. Not a Phase 6 blocker since Phase 6 builds a parallel endpoint.

## Q7 — Phase 5 helpers still absent (affects Phase 6 CSS)

The plan's HTML uses `class="heartbeat-dot stale"` for the live/stale indicator. `.heartbeat-dot` is defined nowhere — Phase 5.1 (where it was supposed to land) never ran.

Two options:
- **A.** Include the `.heartbeat-dot` CSS inline in the Phase 6.3 CSS block. Self-contained.
- **B.** Skip the heartbeat dot entirely; show only the textual `🟢 LIVE` / `○ Market closed` label that's already in the markup.

**Recommend A** — adds ~6 lines of CSS, self-contained. The dot is a nice low-effort detail.

## Summary of recommended defaults

| # | Question | Recommendation |
|---|---|---|
| Q1 | Plan import paths | **REWRITE** template with correct paths |
| Q2 | Flags handling | **A. Skip for v1** (`flags: []`) |
| Q3 | Coexist with legacy scanner | **YES** — keep both |
| Q4 | Volume filter source | **`dailyBar.v` with empirical fallback** |
| Q5 | Race desktop nav | **BUNDLE the 3-line fix** into Phase 6 |
| Q6 | Legacy endpoint broken | **OUT OF SCOPE** — file follow-up ticket |
| Q7 | `.heartbeat-dot` CSS | **A. Self-contained inline** in Phase 6.3 |

If all defaults are accepted, Phase 6 ships in 4 feature commits + 1 closure commit + 1 optional nav-fix commit.
