# HM-BK + HM-BL — Discovery Report (BKBL.0)

**Date:** 2026-05-12
**Operator:** Scotty (Opus 4.7)
**Status:** BKBL.0 discovery only. NO writes. Awaiting Captain's Q1 + Q2 before BKBL.1/BKBL.2.

---

## HM-BK — Kirk's bridge re-instantiation

### Root cause (one line in one file)

`engine/total_portfolio.py:195–197`:

```python
def _load_alpaca_paper() -> dict:
    """Uses the existing AlpacaBridge instance pattern (engine/alpaca_bridge.py)."""
    from engine.alpaca_bridge import AlpacaBridge  # local import — bridge can be slow to construct
    bridge = AlpacaBridge()
```

The docstring claims to reuse the existing instance; the code imports the *class* and constructs a fresh one. Kirk advisory calls `total_portfolio()` every ~2 min, so every cycle → fresh AlpacaBridge → new "Alpaca Paper Trading bridge initialized" log line.

### Singleton already exists

`engine/alpaca_bridge.py:185`:

```python
alpaca = AlpacaBridge()
```

Module-level, created once at import. Python caches modules, so every `from engine.alpaca_bridge import alpaca` returns the same instance. Almost every caller already does this correctly:

```
dashboard/app.py:8611      from engine.alpaca_bridge import alpaca
dashboard/app.py:9527      from engine.alpaca_bridge import alpaca as _alpaca_bridge
dashboard/app.py:13177     from engine.alpaca_bridge import alpaca as _alp
dashboard/app.py:17392     from engine.alpaca_bridge import alpaca as _alpaca_bridge
engine/reconciliation.py:119, 281  from engine.alpaca_bridge import alpaca
engine/paper_trader.py:185         from engine.alpaca_bridge import alpaca
engine/cash_manager.py:144         from engine.alpaca_bridge import alpaca
engine/tax_harvester.py:330        from engine.alpaca_bridge import alpaca
```

The only offender is `total_portfolio.py:_load_alpaca_paper()`.

### Fix shape (two-line diff)

```python
# === HM-BK ===
def _load_alpaca_paper() -> dict:
    """Load Alpaca paper account positions + cash.

    Reuses the module-level AlpacaBridge singleton from engine.alpaca_bridge
    (was constructing a fresh instance per call, which logged a re-init banner
    every Kirk advisory cycle — see HM-BK).
    """
    from engine.alpaca_bridge import alpaca as bridge  # singleton; constructed once at module import
    acc = bridge.status()
    ...
# === /HM-BK ===
```

No new singleton infrastructure needed.

### Verification plan

Restart trader → wait 5 min → grep `"Alpaca Paper Trading bridge initialized"` in trader.log post-restart. Should appear **exactly 1×** (the initial module import), not every 2 min.

---

## HM-BL — `$ATH` is not what the directive expected

### Hypothesis check

| Directive said | Evidence |
|---|---|
| `$ATH` is an all-time-high flag accidentally serialized as a ticker | **No.** No `$ATH` literal anywhere in code. The only `"ATH"` literal is at `dashboard/app.py:14198` inside `_TICKER_EXCLUDE` (chat-NLP stop-word set, never used as input to yfinance). |
| Need to guard the ticker-fetch input against non-alphanumeric leaks | **No.** The `$` prefix in the log line is yfinance's error format, not from our code. yfinance wraps the symbol that way when it errors. |

### Actual cause

`ATH` exists as a real position in `data/trader.db::positions` (count=1). That's the internal AI fleet book per CLAUDE.md two-book policy.

`ATH` was the NYSE ticker for **Athene Holding Ltd** until Apollo acquired it (Jan 2022) and it was delisted. yfinance correctly reports `No data found, symbol may be delisted` for it.

Kirk's advisory iterates positions and pulls a yfinance price per symbol; ATH fails both `period=1y` and `period=5d` fetches → two paired error lines per cycle.

### Today's blast radius

- 174 `$ATH:` error lines in `trader_error.log` today
- ~2 per Kirk cycle × ~85 cycles ≈ matches the volume
- No data corruption — just noise + ~2 unnecessary yfinance calls per cycle

### Three fix paths (directive's input-guard framing doesn't apply)

| Option | What | Where | Risk |
|---|---|---|---|
| **(a)** Filter delisted symbols from Kirk's iteration | Pre-filter — skip yfinance lookup for known-bad tickers | engine/kirk_advisory.py (per-symbol fetch loop) | Needs a "known-delisted" set or DB column |
| **(b)** Clean the stale `positions` row | One SQL UPDATE setting ATH to closed/archived | data/trader.db (sacred-DB; needs backup) | Data-only; doesn't fix the next delisting that lands |
| **(c)** Memoize yfinance "delisted" outcomes | In-process cache of `{symbol: 'delisted'}`; first miss logs loud, subsequent hits return sentinel | engine/market_data.py or wherever the yfinance wrapper lives | Code-only; generalizes to every future delisting |

### Recommendation: (c) — memoize the delisted response

- Code-only, no DB touch, no service surprise
- Generalizes to next delisted ticker (we will get another one — happens routinely)
- Doesn't require maintaining a hand-rolled delisting list
- Tiny: ~10-line wrapper

Skip (b) for now — leave the ATH row as historical data unless Captain wants the row cleaned separately.

Skip (a) — strictly less general than (c).

### What I'd code for (c)

A thin wrapper in `engine/market_data.py` (or wherever Kirk's price fetch lives — needs to be located in BKBL.2 discovery):

```python
# === HM-BL ===
_DELISTED_CACHE: set[str] = set()

def _fetch_yf_history_cached(symbol: str, period: str):
    """yfinance Ticker.history wrapper with in-process delisted memoization (HM-BL).

    First call for a delisted symbol errors loud and records the symbol;
    subsequent calls return None without hitting the network. Cache is per-process
    and clears on restart, so a re-listed ticker eventually re-tries naturally.
    """
    if symbol in _DELISTED_CACHE:
        return None
    df = yf.Ticker(symbol).history(period=period)
    if df is None or df.empty:
        log.warning("HM-BL: %s appears delisted (period=%s); caching for this session", symbol, period)
        _DELISTED_CACHE.add(symbol)
        return None
    return df
# === /HM-BL ===
```

Needs locating: which Kirk code path fires the two paired errors per cycle. BKBL.2 discovery would pin that down before the diff.

---

## Summary for Captain

| | Recommendation |
|---|---|
| **Q1 — HM-BK shape** | Import swap to the existing `alpaca` module-level singleton; 2-line diff in `engine/total_portfolio.py:_load_alpaca_paper()`. No new singleton infrastructure. |
| **Q2 — HM-BL shape** | Option (c) — in-process memoization of yfinance "delisted" outcomes in the price-fetch wrapper. Skip the input-guard framing in the directive (no `$ATH` literal exists). |

If Captain approves, BKBL.1 ships HM-BK (smaller, lower risk, immediate cadence win). BKBL.2 needs a brief follow-up discovery to locate Kirk's exact yfinance entry point before applying the (c) wrapper.

---

## BKBL.1 — HM-BK applied (commit 7a61467)

Two-line import swap in `engine/total_portfolio.py::_load_alpaca_paper()`:
- Removed: `from engine.alpaca_bridge import AlpacaBridge` + `bridge = AlpacaBridge()`
- Added: `from engine.alpaca_bridge import alpaca as bridge`
- Wrapped with `# === HM-BK ===` anchors

Smoke results: compile OK, import OK, singleton-id check passes (sees a single `id()` for the imported `alpaca`).

## BKBL.2 — HM-BL applied (commit 803bb5f)

### Offender located

`engine/high_iv_scanner.py:_get_iv_rank()` line 56 — calls `ticker.history(period="1y", interval="1d")` per symbol. Triggered by `engine/crew_scanner.py:3114` (the 2-min crew scanner cycle that iterates the alpha-squad symbol set). Each delisted symbol fires two paired warnings (yfinance internally retries with period=5d to confirm), explaining the 174 hits/day pattern.

### Wrapper module: `engine/yf_safe.py`

New file. Exports `yf_history_safe(symbol, period, interval, ...)` plus `is_delisted()` and `clear_cache()` test helpers. In-process `_DELISTED_CACHE: set[str]` — process-local, clears on restart so re-listed tickers re-try naturally. First miss emits one WARNING line (Python logger sink → `trader_error.log`); subsequent calls short-circuit without touching the network or producing yfinance warnings.

### Single call-site update

`engine/high_iv_scanner.py:_get_iv_rank` now imports `yf_history_safe` and uses it in place of `ticker.history(period="1y", ...)`. Anchor: `# === HM-BL ===`.

### Smoke results

| Check | Result |
|---|---|
| `engine/yf_safe.py` compile | ✅ |
| `engine/high_iv_scanner.py` compile | ✅ |
| `yf_safe` imports | ✅ |
| `high_iv_scanner` imports | ✅ |
| 1st call `yf_history_safe("ATH", "1y")` | yfinance error (one only) + HM-BL WARNING + empty df + cached |
| 2nd call same symbol | NO yfinance call, NO warning, immediate empty df |
| Live ticker (`SPY`, "5d") | 5 rows, NOT cached |

Note: the wrapper turns the 174 hits/day on `ATH` into **1 hit per process lifetime per delisted symbol**. Other yfinance call sites that also hit ATH (e.g. `engine/theta_scanner.py` runs every 30 min on the theta watchlist) will still emit on first encounter — but each is now also one-shot. Adoption at remaining call sites is natural-maintenance scope.

## Followups (NOT in this commit)

- **HM-BL.E** — audit why stale 0-qty positions persist in the `positions` table. ATH was delisted Jan 2022 (Athene Holding → Apollo acquisition) yet still has a row. Likely a Schwab/Alpaca sync retention bug that never zero-removes positions. Investigation only — no automatic cleanup without explicit decision.
- **HM-BL-broad** — adopt `yf_safe.yf_history_safe` at remaining yfinance call sites:
  - `engine/theta_scanner.py:121` (period="1y")
  - `engine/theta_scanner.py:100` (period="2d")
  - `engine/total_portfolio.py:162` (period="2d")
  - `engine/chart_analyzer.py:32` (period="60d")
  - `engine/trend_predictor.py:31` (period="3mo")
  - `engine/scout_critic.py:100` (yf.Ticker)
  - … and ~10 more. Not scheduled.

## BKBL.D — Post-restart verification

(To be filled in after push + restart + 5-min cadence verify.)

