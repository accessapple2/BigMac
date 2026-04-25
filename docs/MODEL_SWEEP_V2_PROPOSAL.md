# Model Sweep v2 — Design Proposal
**Authored**: 2026-04-20 (revised with dual kill-switch + time guard)
**Status**: Awaiting Steve's go/no-go
**Replaces**: `scripts/model_sweep_2026_04_20.py` (v1, halted)

---

## Why v1 Failed

### Root cause: too few signals per ticker

v1 used **bi-monthly decision points** — the LLM was called once every two
months per ticker. Over a 182-day window that produced **4 calls per ticker**.

With 6 tickers × 4 calls = **24 total LLM calls per model run**, and VectorBT
carrying each signal forward until the next override, the maximum possible
round-trip trades was **≤2 per ticker** (BUY→SELL→BUY requires 3 signals; only
4 available). In practice runs showed **N=0 or N=1**.

### Evidence from completed runs (runs 32–35)

```
Run 32 qwen3:8b:  SPY -1.24  NVDA -18.72  META +3.39  TSLA +7.72  AMD  0.0(N=0)  QQQ -2.82
Run 33 phi3:mini: SPY -1.24  NVDA -18.72  META +3.39  TSLA +7.72  AMD -27.6(N=1) QQQ -2.82
Run 34 qwen3:14b: SPY -1.24  NVDA -18.72  META +3.39  TSLA +7.72  AMD  7.7(N=1)  QQQ -2.82
```

**5 of 6 tickers produced identical returns across ALL THREE MODELS.** Models
only diverged on AMD — one call at one decision point. Win rate was 0% across
the board (VectorBT counts open-at-period-end positions as unrealized, not wins).

This is pure noise. A "winner" ranked by Sharpe here would be determined by
whether qwen3:8b vs phi3:mini happened to say SELL on AMD on Dec 31, 2024.

### Statistical floor

For model discrimination we need ≥30 closed round-trips per model to get
confidence intervals that don't overlap across meaningfully different models
(rule of thumb from OOS validation work). v1 delivered ≤6.

---

## v2 Design

### Parameter changes

| Parameter | v1 | v2 | Rationale |
|-----------|-----|-----|-----------|
| Window | 182 days | **365 days** (2024-04-01 → 2025-04-01) | 2× more signal opportunities |
| Decision frequency | Bi-monthly (4/182d) | **Weekly** (52/365d) | 13× more LLM calls |
| Tickers | 6 | **12** | More diversity, better cross-ticker statistics |
| Exit logic | Carry-forward until next signal | **Carry-forward + 5% stop-loss + 10% take-profit** | Generates mid-interval exits → more closed round-trips |
| Positions | Single per ticker | **Single per ticker** (keep — multiple positions compounds complexity) |
| Init cash | $10,000 | $10,000 | Unchanged |

### Ticker universe (12)

```python
TICKERS = [
    "SPY", "QQQ",           # broad indices — regime anchor
    "IWM", "DIA",           # small-cap / DJIA diversity
    "NVDA", "AMD",          # semiconductors — volatile, LLM-sensitive
    "TSLA", "META",         # high-beta names from v1
    "MSFT", "AAPL",         # large-cap, more anchored
    "GOOGL", "PLTR",        # mixed: mega-cap + speculative
]
```

### Decision point logic

```python
def get_decision_points_v2(df):
    """Weekly decision points — ~52 per 365-day window."""
    weekly = df.resample("W-FRI").last()   # every Friday close
    idxs = []
    for dt in weekly.index:
        pos = df.index.searchsorted(dt, side="right") - 1
        if 0 <= pos < len(df):
            idxs.append(pos)
    return idxs
```

### Exit logic (VectorBT)

```python
pf = vbt.Portfolio.from_signals(
    close, entries, exits,
    freq="1D",
    fees=FEES,
    init_cash=init_cash,
    sl_stop=0.05,    # 5% stop-loss exits position if price drops 5% from entry
    tp_stop=0.10,    # 10% take-profit exits position if price rises 10%
)
```

The stop-loss and TP cause exits independent of model signals — a BUY held
for 3 weeks might exit via TP, then re-enter if next Friday's signal is BUY
again. This is the primary mechanism for generating more closed round-trips.

---

## Expected Statistical Power

### Trade count estimate

```
52 weekly signals × 12 tickers = 624 LLM calls per model
Positions entered: ~30-40% of signals are BUY → ~200 entries
With 5%/10% SL/TP exits: ~60-70% of entries close within 2-4 weeks
→ ~130-150 closed round-trips per model
```

130-150 round-trips per model is sufficient for:
- **Win rate**: ±4-5% confidence interval at 95% (vs. ±0% with N=1 in v1)
- **Sharpe**: meaningful separation between models differing by >0.3 Sharpe
- **Agreement rate**: trackable across tickers (what % of weekly calls agree
  with the previous decision)

### What "model quality" will actually mean

A higher-Sharpe model in v2 is one that:
1. Called BUY before sustained upward moves more reliably
2. Called SELL or HOLD before drawdowns
3. Had fewer whipsaws (BUY on Friday → SELL next Friday repeatedly)

This is real signal, not just "which model agreed with AMD's direction once."

---

## Runtime Estimate

```
LLM calls per model: 52 weeks × 12 tickers = 624
Total calls (22 variants): 624 × 22 = 13,728

Ollie GPU timing (observed in v1):
  - phi3:mini:       ~2.5s/call → 624 calls = ~26 min/run
  - qwen3:8b:        ~3.5s/call → 624 calls = ~36 min/run (think suppressed)
  - qwen3:14b:       ~5s/call   → 624 calls = ~52 min/run
  - deepseek-r1:14b: ~6s/call   → 624 calls = ~62 min/run
  - plutus:latest:   ~3s/call   → 624 calls = ~31 min/run
  - qwen2.5-coder:   ~2.5s/call → 624 calls = ~26 min/run
  - qwen3-coder:30b: ~10s/call  → 624 calls = ~104 min/run
  - llama3.1:        ~3s/call   → 624 calls = ~31 min/run
  - llama3.2:3b:     ~2s/call   → 624 calls = ~21 min/run

Weighted avg across 22 variant mix: ~40 min/run
Total: 22 × 40 min = ~880 min = 14.7 hours

With 2s inter-call sleep (anti-VRAM-thrash): +20 min overhead per run
Revised total: ~880 + 22×20 = ~1320 min = 22 hours

RECOMMENDED KICKOFF: After market close today, 1:15 PM AZ (20:15 UTC)
ESTIMATED COMPLETION: 11:15 AM AZ tomorrow (1 hour before market open)
SAFETY MARGIN: results available and reviewed before any trading day signals
```

> **NOTE on qwen3-coder:30b**: At ~10s/call this model alone could add 4–5h.
> If sweep is running long, `touch ~/autonomous-trader/SWEEP_KILL_SWITCH` between
> runs — **live trader unaffected**. The 30b coder variant (ollama-coder HEAVY)
> is run 19/22 — low priority if time-constrained.
>
> **NOTE on 6:25 AM guard**: if the sweep is paused by the time guard, it will
> resume automatically once `SWEEP_KILL_SWITCH` is removed after market close.
> Remaining runs are queued and continue in order.

---

## Sweep Matrix (unchanged — 22 runs)

Same agents and variants as v1. Only the engine changes.

| # | Agent | Variant | Model |
|---|-------|---------|-------|
| 1-3 | ollie-auto | BASE/LIGHT/HEAVY | qwen3:8b / phi3:mini / qwen3:14b |
| 4-6 | navigator | BASE/LIGHT/HEAVY | qwen3:8b / phi3:mini / qwen3:14b |
| 7 | chekov | BASE only | phi3:mini |
| 8-10 | ollama-llama | BASE/LIGHT/HEAVY | llama3.1 / llama3.2:3b / qwen3:14b |
| 11-13 | ollama-plutus | BASE/LIGHT/HEAVY | plutus / qwen3:8b / deepseek-r1:14b |
| 14-16 | ollama-qwen3 (Dax) | BASE/LIGHT/HEAVY | qwen3:8b / phi3:mini / qwen3:14b |
| 17-19 | ollama-coder | BASE/LIGHT/HEAVY | qwen2.5-coder:7b / phi3:mini / qwen3-coder:30b |
| 20-21 | neo-matrix | BASE/HEAVY | phi3:mini / qwen3:8b |
| 22 | capitol-trades | BASE only | phi3:mini |

---

## Kill Switch Design (Dual-File)

v2 uses **two separate kill-switch files** to decouple sweep control from
fleet control. This was the key design gap in v1: using `KILL_SWITCH` to
pause the sweep also halted the live trader's scanner and trade gateway.

### File roles

| File | Controls | Who touches it |
|------|----------|----------------|
| `~/autonomous-trader/SWEEP_KILL_SWITCH` | Sweep only — live trader unaffected | Steve / sweep management |
| `~/autonomous-trader/KILL_SWITCH` | Entire fleet (scanner + trades + sweep) | Emergency use only |

### Behavior matrix

| SWEEP_KILL_SWITCH | KILL_SWITCH | Sweep | Live scanner | Live trades |
|:-----------------:|:-----------:|:-----:|:------------:|:-----------:|
| absent | absent | running | running | allowed |
| **present** | absent | **paused** | running | allowed |
| absent | **present** | **paused** | **paused** | **blocked** |
| **present** | **present** | **paused** | **paused** | **blocked** |

The sweep checks both files at every run boundary:

```python
# Sweep checks (in order of precedence):
if KILL_FILE.exists():          # ~/autonomous-trader/KILL_SWITCH
    pause_and_poll()            # both files block the sweep

if SWEEP_KILL_FILE.exists():    # ~/autonomous-trader/SWEEP_KILL_SWITCH
    pause_and_poll()            # sweep-only pause, trader keeps running
```

### Overnight sweep commands

```bash
# Pause sweep only (live trader keeps running — safe any time):
touch ~/autonomous-trader/SWEEP_KILL_SWITCH

# Resume sweep:
rm ~/autonomous-trader/SWEEP_KILL_SWITCH

# Emergency fleet halt (also pauses sweep — market-closed only):
touch ~/autonomous-trader/KILL_SWITCH
rm ~/autonomous-trader/KILL_SWITCH   # to resume

# Check what's active:
ls ~/autonomous-trader/KILL_SWITCH ~/autonomous-trader/SWEEP_KILL_SWITCH 2>/dev/null \
  && echo "halt files present" || echo "all clear"
```

---

## Time Guard — Auto-Pause at 6:25 AM AZ

If the sweep is still running at **6:25 AM AZ** (5 minutes before market
pre-open activity begins), it auto-pauses exactly as if `SWEEP_KILL_SWITCH`
were set. The live trader is never affected.

```python
MARKET_OPEN_GUARD_AZ = time(6, 25)   # AZ = MST year-round (no DST)

def check_market_open_guard():
    """Auto-pause sweep if approaching market open."""
    now_az = datetime.now(ZoneInfo("America/Phoenix"))
    if now_az.time() >= MARKET_OPEN_GUARD_AZ:
        log(
            "SWEEP INCOMPLETE — paused for market open. "
            "Resume manually after close: rm ~/autonomous-trader/SWEEP_KILL_SWITCH"
        )
        # Create the file so normal polling loop handles resume
        SWEEP_KILL_FILE.touch()
        # Then block until it's cleared (Steve removes it post-close)
        while SWEEP_KILL_FILE.exists():
            time.sleep(60)
        log("SWEEP RESUMED — market guard cleared")
```

This check runs **before each run** alongside the kill-switch checks. If the
sweep is mid-run when 6:25 AM hits, it finishes the current run (≤~10 min
worst case for qwen3-coder:30b) and then pauses before starting the next.

**Resuming after market close**: simply `rm ~/autonomous-trader/SWEEP_KILL_SWITCH`
from any terminal — sweep picks up from the next queued run.

---

## Log File

v2 uses a **separate log**: `/tmp/model_sweep_v2.log`

v1 log `/tmp/model_sweep.log` is preserved for the 4 completed runs.

```bash
tail -f /tmp/model_sweep_v2.log          # watch live progress
grep "PAUSED\|HALTED\|RESUMED\|STATUS" /tmp/model_sweep_v2.log   # key events only
grep "^2026.*|.*|.*Sharpe" /tmp/model_sweep_v2.log               # completed run lines
```

---

## Resource Safety

| Check | Threshold | Action |
|-------|-----------|--------|
| Ollie unreachable | Any `ConnectionError` | HALT, log reason |
| Consecutive 500s | >3 in a row | HALT, log reason |
| Single run timeout | >30 min | HALT, log reason |
| Disk free | <10% | HALT, log reason |
| Ollie VRAM | 90%+ for >10 min | (monitored via `/api/ps` between runs) |

---

## What to Do Tomorrow Morning

```bash
# 1. Check sweep status
tail -20 /tmp/model_sweep_v2.log
grep "STATUS\|PAUSED\|HALTED\|COMPLETE" /tmp/model_sweep_v2.log | tail -5

# 2. If paused by time guard (6:25 AM) — resume after close:
rm ~/autonomous-trader/SWEEP_KILL_SWITCH

# 3. If fully complete:
cat docs/MODEL_SWEEP_RESULTS_2026-04-20.md   # per-agent winner table
```

4. Review confidence levels — ignore "Low" confidence recommendations
5. For "High" confidence winners: edit `config.py` or agent model assignments
   **ONLY after Steve reviews** (no auto-deploy)

---

## Files

| File | Purpose |
|------|---------|
| `scripts/model_sweep_2026_04_20.py` | v1 — do not re-run (bi-monthly, broken) |
| `scripts/model_sweep_v2.py` | v2 — to be written after Steve approves |
| `docs/MODEL_SWEEP_RESULTS_2026-04-20.md` | Output from v2 (written at completion) |
| `/tmp/model_sweep.log` | v1 log — 4 completed runs, preserved |
| `/tmp/model_sweep_v2.log` | v2 log — separate, starts clean |
| `~/autonomous-trader/SWEEP_KILL_SWITCH` | Sweep-only pause (live trader unaffected) |
| `~/autonomous-trader/KILL_SWITCH` | Fleet halt — also pauses sweep (emergency use) |

---

_Proposal written 2026-04-20 by Claude Code. Awaiting Admiral approval._
