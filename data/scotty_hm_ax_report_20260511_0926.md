# HM-AX Closure — Convergence Threshold + Alpaca Batch Resilience

**Generated:** 2026-05-11 09:26 (after HM-AW.1+HM-AW.2 deploy, before HM-AX restart)
**Branch:** main
**Commits staged:** **4** (1 docs + 3 functional) on top of the 8 from prior sessions = **12 commits ahead of `origin/main`**
**Push performed:** NO
**Service restart performed:** NO

## What shipped

### `engine/strategies.py` (HM-AX.1 + HM-AX.2)

**Code (HM-AX.1):**
- Line 447: `min_strategies = 1 if _mins >= 750 else 2` (was `3`)
- Line 884–885: SQL `HAVING real_strat_count >= 2 OR (real_strat_count = 1 AND tb_conf >= 85)` (was `>= 3 OR (= 2 AND tb_conf >= 85)`)

**Docs/logs (HM-AX.2, 7 string-only changes):**
- Lines 3, 426, 550 — `3+ strategies agree` → `2+ strategies agree`
- Line 622 — log label `_threshold_label` from `"3+ strategies"` → `"2+ strategies"`
- Line 865 — docstring `real_strat_count >= 3 (or 2 + TB tiebreaker)` → `>= 2 (or 1 + TB)`
- Lines 929, 947 — prompt-section copy

### `engine/market_data.py` (HM-AX.3)

- Replaced the failing-batch branch in `get_alpaca_bars()` at line 819 with a per-symbol fallback (48 lines added inside the block)
- Single-symbol path unchanged
- Bulk path: on Alpaca non-OK, iterates `{_ALPACA_BASE}/{sym}/bars` per symbol with 10s timeout. Returns the same `dict[str, DataFrame]` shape callers expect. Empty DataFrame for any per-symbol failure (halted ticker stays empty, good tickers populate).

All 13 callers of `get_alpaca_bars()` in `engine/` confirmed compatible.

## Commits (newest first)

```
92a59ad fix(market-data): HM-AX.3 — per-symbol fallback when Alpaca batch returns non-OK
4fcde63 chore(navigator): HM-AX.2 — doc/log consistency for 2+ threshold
2062209 feat(navigator): HM-AX.1 — lower convergence threshold 3→2 for regular hours
562fffd docs(scotty): HM-AX.0 — convergence threshold + Alpaca batch resilience discovery
```

Plus the 8 prior session commits (5017964 down to 206b0a4).

## Pre-patch state

From Captain's 09:04 AZ field evidence:
```
[09:04:38] 🧭 Scan universe: 68 hot stocks + core = 209 total (cap 700)
[09:04:39] 🧭 Strategy scan complete: 0 convergence signals (3+ strategies agree)
[09:04:39] get_alpaca_bars HTTP 400 for ['MCD', 'BSX', 'CTRA']  <-- whole batch dropped
```

History: March 7 trades, April 2, May 0.

## Post-patch test (running in this session, 8:30 PM MST Sunday off-hours)

```bash
python3 -c "from engine.strategies import scan_strategies; \
            print(scan_strategies(tickers=['SPY','QQQ','AAPL','MSFT','NVDA','TSLA','META','AMZN','GOOGL','AMD'], save=False))"
```

Output:
```
[09:26:32] 🧭 Strategy scan complete: 4 convergence signals (2+ strategies agree)
             QQQ:  3.0 strategies (volume_dry_up, ema_ribbon, relative_strength_high)  conf 82%
             SPY:  2.0 strategies (ema_ribbon, relative_strength_high)                  conf 82%
             MSFT: 2.0 strategies (pullback_sma20, volume_dry_up)                       conf 82%
             NVDA: 2.0 strategies (ema_ribbon, trend_resumption)                        conf 82%
```

- ✅ Log label changed to `(2+ strategies agree)` — HM-AX.2 doc loaded
- ✅ 4 signals from 10 mega-caps; **3 of 4 are exactly at the new 2-strategy floor** (would have been filtered out before)
- ✅ Tickers + strategies are reasonable (no spurious convergence)

## ⚠️ Frequency-magnitude flag for Captain

10 mega-caps → 4 signals = **40% hit rate at the new floor**. The full universe is ~700 names. Even allowing for the mega-cap tendency to cluster (they all dance to SPY), this suggests live regular-hours scans could produce **dozens to hundreds of 2-strategy signals daily**, vs. zero at the 3-floor for May.

Plan's threshold for Captain attention was "20+ signals worth flagging." Today's sample suggests post-patch volume could exceed that on day 1.

**Mitigations already in place:**
- TB tiebreaker rule lowered to `1+ strategy AND tb_conf >= 85` — still requires confidence floor for the borderline cases
- Ollie's quality gate (OllieScore ≥ 2.0) still filters downstream
- Navigator's HM-AW.1+.2 buyer fires every 15 min, not per-signal

**Recommendation:** ship HM-AX as designed and watch the next 5 trading days for trade-quality metrics (win rate, P&L per trade). If frequency spikes without win-rate degradation, the lower floor is validated. If win rate drops below 40%, revert HM-AX.1+.2 via `git revert 4fcde63 2062209` and restart.

## Files modified

```
 data/scotty_hm_ax_discovery.md         |  61 ++++++++++ (new)
 data/scotty_hm_ax_report_20260511_0926.md| (this file, new)
 engine/strategies.py                   |  10 ++- (3 code + 7 doc/log)
 engine/market_data.py                  |  50 ++++- (1 branch rewrite, +48 net)
```

## Standing rule compliance

- ✅ No `git push` performed
- ✅ No service restart performed
- ✅ No DB writes — `scan_strategies` test invoked with `save=False`
- ✅ Sacred directories untouched
- ✅ Anchored: `=== HM-AX` matches 5 sites across the two files
- ✅ Diff-then-apply on every edit
- ✅ One commit per sub-phase (4 commits total)
- ✅ NTFY on each commit (HM-AX.0/.1/.2/.3 + verify all sent to `ollietrades-crew`)

## Admiral action

```bash
cd ~/autonomous-trader

# 1. Sanity check
git log origin/main..HEAD --oneline   # expect 12 commits

# 2. Pause VPN, push
git push origin main

# 3. Restart picks up new threshold AND new fallback logic
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 5

# 4. Watch ntfy ollietrades-crew during next market open for:
#    🧭 Strategy scan complete: N convergence signals (2+ strategies agree)
#    (the log label confirms the patch is live)

# 5. Smoke verify pre-market endpoint also lit up from Phase 6
curl -s "http://localhost:8080/api/momentum/premarket?force=true&limit=3" | python3 -m json.tool
```

## A/B trial plan (Captain's call)

- **Day 1–5 (May 12–16):** monitor `trades` table for navigator entries. Watch frequency + win-rate.
- **Decision gate (Day 5 close):**
  - If win-rate ≥ 40% AND P&L per trade ≥ historical 3-floor average: keep 2-floor as new baseline. Update CLAUDE.md doctrine.
  - If win-rate drops below 40% OR P&L per trade collapses: `git revert 4fcde63 2062209` (keep HM-AX.3 batch fix — it's independent), restart, update XO_BACKLOG.

## Carry-forward

- HM-AX.3 batch fix is **independent of the threshold change** and should stay regardless of A/B trial outcome — one halted ticker dropping the whole batch is a clear bug.
- Phase 6 pre-market scanner from prior session is **still unactivated** (also pending the restart that ships HM-AX). Both phases will go live in the same restart.
- Phase 4-static.3 Race desktop nav from prior session is already in `dashboard/static/index.html` — visible immediately on browser hard-refresh (no restart needed for HTML).

— Scotty
