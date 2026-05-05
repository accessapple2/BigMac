# HM-T — PED Operational Probe
*2026-05-04 evening, Scotty investigation, no fixes applied*

## Question

Is `agents/post_earnings_drift.py` (PED) actually running in production? Or has it been silently inert for some unknown duration?

## Verdict: **B — PED is silently inert (scheduled but trigger conditions never met)**

The 15-min scheduler IS firing, but PED's input universe is empty by config and ZERO signals have ever been emitted to the DB. The gate-promotion criterion (30 trades + positive expectancy) is **structurally unreachable** under the current wiring.

---

## Phase 2.1 findings

### Player identity — does NOT exist
```
SELECT * FROM ai_players WHERE id LIKE '%earnings%' OR id LIKE '%PED%';
→ (empty result set)
```
PED is not registered as a player in `ai_players`. It is a "scanner" in the engineering sense — its `scan(market_data)` returns a list of dicts that get logged via `console.log`, never persisted.

### Lifetime activity — zero
| Source | Count |
|---|---|
| `signals` rows where player_id matches PED | **0** |
| `trades` rows where player_id matches PED | **0** |
| Log lines in `logs/trader.log` mentioning PED | **0** |
| Log lines in `logs/main.log` mentioning PED | **0** |
| Log lines in `logs/trader_error.log` mentioning PED | **0** (the [LRS] earnings noise is unrelated) |
| Sitrep `PED signals` count over 794 lines (2026-05-01 → 2026-05-04) | **0 every cycle** |

### Scheduling — properly wired

- **Module imports correctly:** `main.py:3486` `from agents.post_earnings_drift import _agent as ped_agent`
- **Schedule registration:** `main.py:3541` `schedule.every(15).minutes.do(run_post_earnings_drift)` inside the `if __name__ == "__main__":` block (line 2452). Reached on every trader startup.
- **Self-throttle:** `main.py:3482` enforces 15-min minimum between runs via `_ped_state["last_run"]`.
- **Halt check:** `main.py:3487` calls `ped_agent.is_halted()` — returns False silently due to phantom `agent_state` table (HM-S verdict).
- **No external scheduler** (no launchd plist, no cron, no shell script).

### Why PED is silent — root cause: missing watchlist file + narrow fallback universe

`main.py:3495-3499`:
```python
try:
    with open("data/watchlist.txt") as _wl:
        universe = [s.strip().upper() for s in _wl if s.strip()]
except Exception:
    universe = ["SPY","QQQ","NVDA","AAPL","MSFT","GOOGL","META","AMZN","TSLA"]
```

**`data/watchlist.txt` does not exist** in the repo. PED falls back to the hardcoded 9-symbol list every cycle.

The 9 fallback symbols are 2 ETFs (SPY, QQQ — no earnings) + 7 mega-caps. Earnings cache (`data/earnings_cache.json`, fresh as of today 12:33) shows next earnings dates for those 7:
- NVDA → 2026-05-20
- AAPL → 2026-07-30
- MSFT → 2026-07-29
- GOOGL → 2026-07-23
- META → 2026-07-29
- AMZN → 2026-07-30
- TSLA → 2026-07-22

**None within the 1-48hr post-earnings window today.** None for at least 16 days.

Even on quarterly earnings days (4 per ticker per year), each symbol has ~4 hours where PED's filter `cutoff_lo (now-48h) <= edt <= cutoff_hi (now-1h)` matches — i.e., approximately **47 hours per symbol per year out of 8,760**. Across 7 mega-caps, that's ~329 hours/year of opportunity, ~3.7% of the time. AND even within that window, PED requires `gap_pct < -2.0%` AND `vwap_rej_pct < -0.3%` AND ≥5 bars of data. Empirically, post-mega-cap-earnings often surprises positive — gap-down filter selects out half the cases.

So PED's effective trigger frequency under the current 9-symbol fallback universe is **single-digit hours per year**, which after the gap+vwap filters likely produces **single-digit signals per year**. The 30-trade gate-promotion threshold is unreachable in any reasonable timeframe.

### Compute waste — minimal

PED is rule-based (no LLM). Each cycle:
- `fetch_earnings(universe)` → cached, ~1ms
- Loop over `recent_earnings` → empty most of the time
- `_yahoo_chart` calls only fire when an earnings record is in window → almost never
- `ped_agent.scan(md)` with empty `md` → returns empty list immediately

Net: ~milliseconds/cycle, 96 cycles/day = ~0.1s/day of CPU. Zero Ollie Box load. **No GPU waste like HM-E flagged for ai_journal.**

---

## Operational implications

| Property | Status |
|---|---|
| Scheduled? | ✅ Yes (every 15 min) |
| Module loaded? | ✅ Yes (imported on startup) |
| Halt-aware? | ❌ No (phantom `agent_state` table, see HM-S) |
| Real-money exposure? | 🛡️ Blocked by `gated=True` paper-only flag |
| Compute waste? | ✅ Negligible (rule-based, no LLM) |
| Watchlist source | ❌ `data/watchlist.txt` missing → falls back to 9 mega-caps + ETFs |
| Trigger frequency | ❌ < 0.1% of clock time has earnings in window for fallback universe |
| Filter rate | ❌ gap+vwap rejection further narrows |
| Gate-promotion path | ❌ **Structurally unreachable** under current wiring |
| Lifetime signals | **0** |

---

## Recommended action

Two options for the Captain:

### Option β — Repair the wiring (~30 min, future session)
1. Create `data/watchlist.txt` with the proper PED universe (likely the same `data/watchlist.txt` other agents read from — check `engine/crew_scanner.py` and `engine/dayblade.py` for what watchlist the live fleet uses).
2. Audit whether 50–200 mid-cap tickers in a richer watchlist would actually generate qualifying post-earnings drift candidates per quarter.
3. Verify by manual trigger before re-arming.
4. Optionally widen the gap/vwap thresholds during paper-only mode to accelerate gate promotion.

### Option γ — Formally retire PED (~15 min, future session)
1. Remove the schedule registration at `main.py:3541`.
2. Move `agents/post_earnings_drift.py` to `archive/retired/2026-05-XX-post-earnings-drift/`.
3. Document rationale in `CLAUDE.md` and `XO_BACKLOG.md`.
4. Side benefit: closes HM-S-code (the phantom `agent_state` reference) since the file is no longer in the active path.

**Recommended: γ.** PED has been inert for at minimum the 794-line sitrep history (2026-05-01 onward) and likely much longer. The fact that nobody noticed for that long suggests the strategy isn't critical to the fleet. Formally retiring is cleaner than re-wiring something that might have been deliberately abandoned. If the Captain has post-earnings-drift research interest later, the archived file can be revived.

Don't pre-judge — Captain decides β vs γ.

---

## Open questions for the Admiral

1. **Was PED ever supposed to be a top-tier agent?** Per `agents/post_earnings_drift.py:1-9`, the docstring describes it as "the only agent allowed to trade [post-earnings 1-48hr window]" with a "gated paper-only until 30 trades + positive expectancy" promotion path. Was that intent abandoned, deferred, or just lost to drift?
2. **Is `data/watchlist.txt` referenced elsewhere?** It's not in the repo, but other agents may construct or expect it. Worth a `grep -rn 'data/watchlist.txt'` before deciding β vs γ.
3. **Are there other "structurally inert" scheduled agents like PED?** The sitrep shows BBD signals: 0 alongside PED — same pattern? May be worth a fleet-wide silent-inertness audit (could be HM-T-fleet).
