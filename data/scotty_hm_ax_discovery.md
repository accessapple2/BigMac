# HM-AX Discovery — 2026-05-11

Read-only sweep. No edits.

## A — Threshold (`engine/strategies.py`)

**Code changes (2 lines):**
- `447`: `min_strategies = 1 if _mins >= 750 else 3` → change to `2`
- `884`: `"HAVING real_strat_count >= 3 "` → change to `2`

**Doc/log changes (string-only, 6 lines):**
- `3`   — module docstring "3+ strategies agree" → "2+ strategies agree"
- `426` — function docstring "3+ strategies normally" → "2+ strategies normally"
- `550` — function docstring "3+ strategies agree" → "2+ strategies agree"
- `622` — log label `else "3+ strategies"` → `else "2+ strategies"`
- `929` — log line "🎯 HIGH-CONVICTION CONVERGENCE SIGNALS (3+ strategies agree):" → "(2+ strategies agree)"
- `947` — descriptive text "where 3+ strategies agree..." → "where 2+ strategies agree..."

**Care points:**
- Line `865` ("real_strat_count >= 3 (or 2 + TB tiebreaker at 85%)") has both `3` and `2` in context. The `3` is the threshold; the `2` is the tiebreaker rule. Both need to drop by 1 if we're being consistent: "real_strat_count >= 2 (or 1 + TB tiebreaker at 85%)". **Will update both in HM-AX.2.**

## B — Alpaca batch (`engine/market_data.py`)

**Function:** `get_alpaca_bars()` at line ~793 (docstring at 795)

**Failure block:**
```
819-821:
        if not r.ok:
            console.log(f"[yellow]get_alpaca_bars HTTP {r.status_code} for {sym_list[:3]}")
            return pd.DataFrame() if single else {}
```

**Fix:** on `not r.ok`, fall back to per-symbol calls so good tickers survive the batch's bad apple.

**Per-symbol URL pattern (from plan, confirmed by Phase 0 curl evidence):**
`{_ALPACA_BASE}/{sym}/bars` with same params minus `symbols`.

**Callers** (13 sites — none assume batch-only):
- `engine/ghost_trader.py:268`        — single symbol
- `engine/squeeze_scanner.py:77`      — single symbol
- `engine/chekov_autotrade.py:138`    — single symbol
- `engine/regime_ma.py:91`            — bulk `["SPY","QQQ"]`
- `engine/daily_enrichment.py:80`     — bulk
- `engine/alpha_signals.py:687`       — single symbol
- `engine/premarket_scanner.py:27`    — import-only (single via `get_alpaca_bars`)
- plus internals

All callers receive `pd.DataFrame()` for single or `dict` for bulk — semantics preserved by the fallback.

## Idempotency

`grep "HM-AX" engine/ main.py` returns empty. Safe to insert anchors.

## Restart impact

YES — both files imported at process boot. The convergence threshold and `get_alpaca_bars` fallback won't take effect until `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`.

## Plan acceptance

All defaults accepted. Proceeding to HM-AX.1 (threshold code change).
