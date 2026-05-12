# 🔧 HM-BL-broad — Discovery Report (Phase BLB.0)

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** HALT — awaiting Captain scope confirmation
**Prior art:** `engine/yf_safe.py` (HM-BL, commit `803bb5f`), `data/scotty_hm_bkbl_report.md`

---

## Total inventory

`grep` hit count for `yf.download | yf.Ticker | .history(` across `engine/ scripts/ shared/ main.py`:
**103 raw call sites** (after excluding `yf_safe.py` itself + `test_*` + `.pyc`).

After classification, the real swap-candidate count is **much smaller** (see Q1).

---

## Categorization

### Category A — `.history(period=...)` single-ticker calls — **IN-SCOPE for HM-BL-broad**

Direct 1:1 swap to `yf_history_safe(symbol, period=..., interval=..., **kwargs)`. Signature is identical except wrapper takes `symbol` (string) instead of being bound to a pre-built `yf.Ticker(symbol)` object.

| # | Site | Period | Notes |
|---|---|---|---|
| 1 | `engine/high_iv_scanner.py:32` | `"1d"` | **BKBL adoption miss** — line 62 was swapped but line 32 (spot-price hist) still raw. The very file HM-BL anchored. |
| 2 | `engine/theta_scanner.py:100` | `"2d"` | Per-position theta passes — likely hits ATH on every cycle. |
| 3 | `engine/theta_scanner.py:121` | `"1y"` | 1-year history for theta decay. |
| 4 | `engine/total_portfolio.py:162` | `"2d"` | Portfolio per-symbol price refresh. |
| 5 | `engine/chart_analyzer.py:32` | `"60d"` | Chart synthesis pre-LLM. |
| 6 | `engine/trend_predictor.py:31` | `"3mo"` | Trend regression pre-cycle. |
| 7 | `engine/multi_timeframe.py:36` | dynamic | `period=period, interval=interval` — kwargs pass-through fine. |
| 8 | `engine/cross_asset.py:34` | `"5d"` | Cross-asset correlation. |
| 9 | `engine/strategy_presets.py:31` | `"2mo"` | Preset evaluation. |
| 10 | `engine/archer_frontier.py:48` | `"5d"` | Archer frontier scanner. |
| 11 | `engine/archer_frontier.py:77` | `"1mo"` | Archer frontier scanner. |
| 12 | `engine/smart_levels.py:74` | `"15d"` | S/R level scan. |
| 13 | `engine/strategy_race.py:48` | `"5d"` | SPY race benchmark — fixed `"SPY"`, won't hit ATH but cheap to convert for consistency. |
| 14 | `engine/earnings_catalyst.py:101` | `"10d"` | Earnings catalyst scan. |
| 15 | `engine/options_chain.py:78` | `"1d", "1m"` | Spot price for options chain. |
| 16 | `main.py:2236` | `"2d"`, `prepost=True` | Pre/post-market refresh. `**kwargs` accepts `prepost`. |

**Subtotal: 16 sites.**

### Category B — `.history(start=, end=...)` start/end pattern — **IN-SCOPE (1 site)**

| Site | Pattern | Notes |
|---|---|---|
| `scripts/scotty_backtest.py:86` | `tk.history(start=, end=)` | Signature compatible via `**kwargs`. Cached-delisted returns empty DataFrame regardless of start/end, which matches the caller's `if hist.empty: return None` path. |

**Subtotal: 1 site.** Low priority (backtest-only).

### Category C — Non-`.history()` `yf.Ticker` methods — **OUT OF SCOPE**

These call `yf.Ticker(symbol).XXX` where XXX is NOT `.history()` — they hit different yfinance subsystems with different signatures, different return types, and different error modes. `yf_history_safe` doesn't help here.

| Method | Sites |
|---|---|
| `.info` | `scripts/ghost_advisor.py:129`, `engine/universe_refresh.py:205`, `scripts/scotty_backtest.py:154`, `scripts/snapshot_real_portfolio.py:26` |
| `.fast_info` | `engine/metals_sync.py:39` |
| `.calendar` | `main.py:713`, `engine/earnings_catalyst.py:38` (full file uses `stock.calendar`) |
| `.options` / `.option_chain` | `engine/gex_engine.py:358/364`, `engine/options_chain.py:96/118`, `engine/options_greeks.py:103`, `engine/options_selector.py:40` |
| `.earnings_dates` | `engine/scout_critic.py:100`, `engine/strategy_lab.py:707` |
| Bare `yf.Ticker(...)` for handle | `engine/premium_tracker.py:148`, `engine/high_iv_scanner.py:26` (handle used for `.options` + `.option_chain`) |

**These need separate wrappers (`yf_info_safe`, `yf_options_safe`, etc.) if delisted-symbol memoization matters for them. Not HM-BL-broad's scope.**

### Category D — `yf.download(tickers, ...)` multi-ticker bulk — **OUT OF SCOPE (signature mismatch)**

`yf.download` accepts a list of tickers and returns a multi-level-column DataFrame keyed by ticker. `yf_history_safe` is single-ticker only. To memoize bulk downloads we'd need a NEW companion `yf_download_safe(tickers: list, ...)`.

| Site | Pattern |
|---|---|
| `engine/holodeck_expansion.py:51/154/155` | single-ticker `yf.download(symbol, ...)` |
| `engine/premium_etfs.py:138` | multi-ticker list |
| `engine/master_backtest.py:396/406` | multi + single |
| `engine/benchmark.py:118` | single |
| `engine/crew/tools.py:69`, `engine/crew/strategy_crew.py:94` | single |
| `main.py:803` | bulk |
| `engine/backtester.py:49/54/822/825` | single |
| `engine/generated_assets.py:281/304` | multi + single |
| `engine/warp10_engine.py:330` | bulk multi-level |
| `engine/holly_nightly_backtest.py:173/204/364` | single |
| `engine/strategy_backtest.py:390` | bulk |
| `engine/strategy_lab.py:166` | single |
| `engine/arsenal_backtest.py:202` | bulk |
| Scripts: `s6_sim_180d`, `s6_sim_2022_bear`, `s6_60d_run`, `s6_90d_run`, `model_sweep_*`, `ollama_bulk_backtest` | mostly bulk historical |

**Subtotal: ~25 sites.** Some are single-ticker `yf.download(sym, ...)` that could trivially be re-expressed as `.Ticker(sym).history(...)` → `yf_history_safe(sym, ...)` — call those Category D′ — but most are bulk multi-ticker that legitimately need the multi-level DataFrame shape.

### Category E — Archive (skip)

`engine/_archive/2026-04-26/triple_threat.py:276/285/311/1542` — retired code, do not touch.

### Category F — False positives

Docstrings/comments matched by `grep`, not actual call sites:
- `engine/insider_tracker.py:15`, `engine/alpha_signals.py:1199`, `engine/fear_greed.py:69/102`, `engine/volume_scanner.py:4`, `engine/options_chain.py:3`, `engine/earnings_catalyst.py:23`, `engine/market_data.py:791`

---

## Q1: Scope recommendation

**Recommend: Category A (16 sites) + Category B (1 site) = 17 sites total.**

Rationale:
1. All 16 Category A sites are direct 1:1 swaps to the existing `yf_history_safe` signature. No new wrapper, no new code paths.
2. The BKBL closure listed ~10 followups; this discovery surfaces 17 including the partial-adoption miss at `high_iv_scanner.py:32` — the very file HM-BL was anchored in. **Closing that gap matters for the architectural-win claim.**
3. Category B (1 site, `scotty_backtest.py:86`) is backtest-only and low-priority but cheap to include for consistency. Optional.
4. Category C (non-`.history()` methods) is a **separate decision**. Whether to build `yf_info_safe` / `yf_options_safe` companions depends on whether those paths produce $ATH-style log spam in production. They don't appear in the current `trader_error.log` $ATH count, so deferring is correct.
5. Category D (`yf.download(...)` bulk) is a **separate refactor** — needs a new wrapper that handles multi-level DataFrames. Suggest filing as `HM-BL-broader` (or HM-BLC) for a future session. Don't bundle into HM-BL-broad.

**Alternative (more conservative): Category A only, drop Category B.** Backtest tooling spam is invisible to production logs. Defer the 1-site backtest swap. Total: 16 sites.

**Recommendation: ship 16 Category A sites in this bundle.** Add Category B (1 site) if you want a tidy "broad" sweep; skip it if you prefer minimum-change discipline.

## Q2: Signature compatibility

`yf_history_safe(symbol: str, period="1y", interval="1d", **kwargs) -> pd.DataFrame`

Coverage analysis:

| Pattern in scope | Compat? | Notes |
|---|---|---|
| `Ticker(sym).history(period="2d")` | ✓ | Direct |
| `Ticker(sym).history(period="1y", interval="1d")` | ✓ | Direct |
| `Ticker(sym).history(period=p, interval=i)` (dynamic, multi_timeframe.py) | ✓ | Direct |
| `Ticker(sym).history(period="2d", prepost=True)` | ✓ | `prepost` passes through `**kwargs` to yfinance |
| `Ticker(sym).history(start=, end=)` | ✓ | `start`/`end` pass through `**kwargs`. Cached-delisted returns empty DataFrame regardless — caller `.empty` guards handle it correctly |
| Multi-ticker `yf.download([t1, t2, ...], ...)` | ✗ | Different return type (multi-level DataFrame). Out of scope. |

**Conclusion: existing signature covers all 17 in-scope sites without modification.** No `yf_download_safe` companion is needed for this bundle.

---

## Proposed bundle (Phase BLB.1 preview)

If Captain approves Category A only (16 sites):

```python
# === HM-BL-broad ===
from engine.yf_safe import yf_history_safe
hist = yf_history_safe(symbol, period="2d")
# === /HM-BL-broad ===
```

Replaces each site's `ticker = yf.Ticker(symbol); hist = ticker.history(period=...)`. Where the calling code re-uses `ticker` for `.options`, `.option_chain`, etc. afterwards (e.g., `high_iv_scanner.py` lines 26–34), the swap is **insert** not **replace** — keep the `ticker = yf.Ticker(...)` line and only swap the `.history()` call. Anchored comment per site.

Compile + smoke (live SPY through wrapper + delisted ATH through wrapper) via `venv/bin/python3` per HM-BHBI lesson. Single bundle commit. Push → restart → 10-min `$ATH` soak.

---

## HALT — Captain decisions needed

**Q1 — Scope.** Pick one:
- (A) **16 Category A sites** — minimal, surgical, closes the BKBL miss at `high_iv_scanner.py:32`. **Recommended.**
- (B) **17 sites** = 16 Category A + 1 Category B (scotty_backtest backtest tool). Cosmetic extra.
- (C) Wider — pull in some Category C or D. **Not recommended** without first designing companion wrappers.

**Q2 — Signature compat.** Answer: existing `yf_history_safe(symbol, period, interval, **kwargs)` covers every in-scope site. No companion wrapper needed for HM-BL-broad. Defer `yf_download_safe` to a future HM-BL-broader/HM-BLC sweep if/when Category D spam shows up.

---

ntfy pending: `🔧 HM-BL-broad BLB.0 discovery: 16 in-scope sites + 1 optional. Awaiting Captain scope confirmation.`
