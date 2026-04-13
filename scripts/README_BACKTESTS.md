# OllieTrades Backtest Scripts

## BACKTEST BASELINE STANDARD

**ALL backtests MUST import `backtest_baseline.py`.**
Never run a standalone equity-only backtest again — always use the full signal stack.

```
scripts/
├── backtest_baseline.py      ← shared foundation (import from ALL backtests)
├── ollie_backtest_v6.py      ← Season 6 fleet comparison (Tier 2)
└── README_BACKTESTS.md       ← this file
```

---

## backtest_baseline.py

The canonical shared module. Every future backtest imports from here.

### What It Provides

| Function / Constant | Description |
|---|---|
| `fetch_ohlcv(ticker, start, end)` | OHLCV with file cache → `data/backtest_cache/` |
| `fetch_vix_history(start, end)` | VIX as GEX proxy, returns `{YYYY-MM-DD: float}` |
| `fetch_fear_greed_history(start, end)` | CNN F&G historical, returns `{YYYY-MM-DD: float}` |
| `fetch_spy_vs_200ma(start, end)` | SPY vs 200MA per day, returns `{YYYY-MM-DD: bool}` |
| `get_daily_regime(date, vix_data, fg_data, spy200_data)` | Combines all 3 → BULL_CALM / NEUTRAL / CAUTIOUS / BEAR / CRISIS |
| `get_position_size_multiplier(regime, agent_id)` | Regime-aware sizing; McCoy gets 1.5× in crisis, 0× in calm |
| `should_agent_trade_today(agent_id, regime, vix)` | Per-agent regime gate; McCoy only above VIX 22 |
| `build_agent_prompt(...)` | Persona + regime + technicals + per-agent constraint |
| `query_ollama(agent_id, prompt, timeout)` | Real Ollama HTTP call, returns `(signal, conf, reason)` |
| `build_results_summary(...)` | Standard summary table printed at end of every backtest |
| `AGENT_MODELS` | `{agent_id: model_name}` — single source of truth |
| `AGENT_STRATEGIES` | Per-agent strategy list injected into prompts |
| `AGENT_INSTRUMENTS` | McCoy universe override; `None` = full universe |

### Regime Logic

```
VIX > 30  or  F&G < 20  →  CRISIS    (only McCoy trades)
VIX > 25  or  F&G < 35  →  BEAR      (size = 0.50×)
VIX > 20  or  F&G < 45  →  CAUTIOUS  (size = 0.75×)
SPY > 200MA and VIX < 15 →  BULL_CALM (size = 1.25×)
otherwise               →  NEUTRAL   (size = 1.00×)
```

### McCoy (ollama-plutus) Special Rules

- **Activates:** VIX ≥ 22 only
- **Universe:** GLD, TLT, XLU, SH, PSQ, GDX
- **Sits out** in BULL_CALM and NEUTRAL
- **Gets 1.5× size multiplier** in BEAR/CRISIS

### Agent Personas + Constraints

| Agent | Persona | Constraint |
|---|---|---|
| navigator (Chekov) | Signal scanner | BUY only when 3+ indicators agree |
| ollama-plutus (McCoy) | Crisis doctor | BUY only when VIX > 22 |
| ollama-qwen3 (Dax) | Swing trader | BUY for 3-7 day holds only |
| ollama-coder (Data) | Rules machine | BUY only if composite score > 0.6 |
| neo-matrix (Neo) | High conviction | BUY only if confidence ≥ 80 |

---

## ollie_backtest_v6.py

Season 6 fleet comparison. 3 strategy versions × 5 agents × N days.

### Usage

```bash
# Quick 5-day verification
venv/bin/python3 scripts/ollie_backtest_v6.py --days 5

# Full 60-day run (background)
nohup venv/bin/python3 scripts/ollie_backtest_v6.py --days 60 \
  > /tmp/backtest_v6.log 2>&1 &
echo "PID: $!"
```

### Strategy Versions

| Version | Stop Loss | Allocation | Exit Logic |
|---|---|---|---|
| BASELINE | 8% | 20% / 10% (low alpha) × regime_mult | SL/TP + RSI sell |
| V2_ALPHA | 12% | Linear 0.5–2.0× BASE × regime_mult | SL/TP + RSI sell |
| V3_CONC | Trailing (5% → 8%) | 20% × regime_mult | Trail stop + pyramid + RSI sell |

### Expected Output Per Day

```
[2026-01-15] VIX=18.5  F&G=62  Regime=NEUTRAL  McCoy=inactive  [navigator/BASELINE]
    [OLLAMA] Chekov/qwen3.5:9b AAPL 2026-01-15: BUY(7/10) regime=NEUTRAL vix=18.5 fg=62
```

### Cache

All data fetches (VIX, F&G, SPY/200MA, OHLCV) are cached to `data/backtest_cache/`.
Ollama responses are cached in-memory per `(agent_id, sym, date, version)`.

### Sacred Rules

- **NEVER delete** `trader.db` or `arena.db`
- **APPEND ONLY** writes to `backtest_runs` + `backtest_results`
- **NEVER touch** trade history tables
- **Always use** `venv/bin/python3` (not `.venv` or bare `python3`)

---

## Adding a New Backtest Script

```python
#!/usr/bin/env python3
# ============================================================
# MY_BACKTEST — imports backtest_baseline. BASELINE STANDARD.
# Sacred: never delete trader.db or arena.db
# ============================================================
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backtest_baseline import (
    fetch_vix_history, fetch_fear_greed_history, fetch_spy_vs_200ma,
    get_daily_regime, get_position_size_multiplier, should_agent_trade_today,
    build_agent_prompt, build_results_summary,
    AGENT_MODELS, AGENT_STRATEGIES, AGENT_INSTRUMENTS,
    _closest_prior,
)

# fetch signal sources before your day loop
vix_data  = fetch_vix_history(start_str, end_str)
fg_data   = fetch_fear_greed_history(start_str, end_str)
spy200    = fetch_spy_vs_200ma(start_str, end_str)

# per-day context
day_vix    = _closest_prior(vix_data, ds, 20.0)
day_fg     = float(fg_data.get(ds, 50))
day_regime = get_daily_regime(ds, vix_data, fg_data, spy200)
mccoy_on   = (agent_id == "ollama-plutus" and day_vix >= 22)
print(f"[{ds}] VIX={day_vix:.1f}  F&G={day_fg:.0f}  Regime={day_regime}  McCoy={'active' if mccoy_on else 'inactive'}")

# gate entries
if should_agent_trade_today(agent_id, day_regime, day_vix):
    regime_mult = get_position_size_multiplier(day_regime, agent_id)
    universe = AGENT_INSTRUMENTS.get(agent_id) or YOUR_UNIVERSE
    # ... entry logic ...

print("\nBASELINE STANDARD: CONFIRMED")
```
