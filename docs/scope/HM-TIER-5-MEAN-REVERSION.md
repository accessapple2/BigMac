# HM-TIER-5-MEAN-REVERSION — Scotty Scope Brief

**Banked:** 2026-05-24 (session close)
**Status:** SCOPED, NOT STARTED
**Type:** Feature / two-scanner sprint
**Owner:** Scotty
**Sister sprints:** HM-MINERVINI-TREND-FILTER (shipped) · HM-SQUEEZE-BBKC-COMPRESSION (shipped) · HM-RS-RANK-IBD-BLENDED (shipped)
**Banked priority:** AFTER HM-INLINE-STYLE-SWEEP resumes. Mean reversion is doctrine-additive; inline sweep is foundation cleanup.
**Doctrine fit:** Mean-reversion is the **counter-cycle** to the trend-leadership scanners shipped today — Minervini finds buyers chasing strength, Tier 5 finds **forced sellers about to exhaust**. Same engine, opposite tail.

## Two deliverables in one sprint

### 5A — Weekly Combined RSI Ensemble (14/9/5)

**Concept:** Single RSI(14) is noisy on weekly bars and famously bad at calling bottoms. Combine three lookbacks — RSI(14), RSI(9), RSI(5) — and gate on **all three** in oversold territory simultaneously. The faster RSIs catch the inflection; the slower confirms it's structural, not a one-bar wick.

**Pass criteria (proposed):**
- Weekly RSI(14) ≤ 30
- Weekly RSI(9) ≤ 25
- Weekly RSI(5) ≤ 20
- All three rising vs. prior week (true mean-reversion trigger, not freefall)
- Optional gate: price within 40% of 52w low (filter dead-money fallers)

**Output:**
- New table `combined_rsi`: `symbol`, `computed_at`, `rsi_14`, `rsi_9`, `rsi_5`, `all_oversold` (0/1), `all_rising` (0/1), `signal` (0/1 = both pass), `bars_used`, `weekly_close`
- New endpoint `/api/combined-rsi?signal=true` → `[{symbol, rsi_14, rsi_9, rsi_5, signal_age_days}]`
- Nightly job: `engine/combined_rsi.py::scan_all()` after Friday close (weekly bars); cron same slot as `rs_rank` nightly job

### 5B — Capitulation Finder

**Concept:** Identifies the **single bar** of forced selling that often marks intermediate lows — climactic volume + extreme price drop + intraday reversal. Different from 5A: 5A is structural oversold (multiple weeks), Capitulation is event-driven (one session).

**Pass criteria (proposed):**
- Daily close down ≥ N% (N tuned per market regime; default 4% in normal vol, 6% in high vol)
- Volume ratio ≥ 2.0× 30-day avg (climactic volume gate)
- Intraday range: close in **upper 40%** of day's range (reversal signature — buyers stepped in)
- Optional: RSI(14) ≤ 30 daily (oversold prior to capitulation bar)

**Output:**
- New table `capitulation`: `symbol`, `detected_at`, `close_pct`, `volume_ratio`, `range_position` (0-1, where close sits in day's range), `rsi_14`, `vol_regime` (low/normal/high), `outcome_pct`, `outcome_hit` (mirror `volatility_breakouts` outcome-tracking pattern)
- New endpoint `/api/capitulation?lookback=5` → recent N days of detections w/ outcome tracking
- Real-time-ish job (intraday-after-close): `engine/capitulation.py::scan_all()` at 16:30 ET daily; outcome resolver runs T+3 / T+5 / T+10

## Doctrine integration

| Layer | How Tier 5 wires in |
|---|---|
| **AI brain prompt** | Add `build_combined_rsi_section(symbol)` and `build_capitulation_section(symbol)` similar to `build_breakout_prompt_section` in `volatility_breakout.py:386` — agents see the signal alongside other context |
| **Bridge votes** | Capitulation should fire NTFY (HIGH severity) — extreme single-day signals warrant Captain awareness; Combined RSI is LOW (research feed) |
| **Gate interaction** | Mean-reversion BUYs on capitulation candidates should NOT trigger Grade-A/B regime block (different mechanism than trend-following) — flag in `paper_trader.buy()` to bypass Grade-B fleet gate when `signal_source='capitulation'` |
| **Backtest** | Outcome tracking (T+3/T+5/T+10) builds the empirical foundation for tuning N% threshold + vol regime cutoffs — same pattern Scotty used for HM-SQUEEZE-RELEASE outcome tracking |

## Schema deltas

```sql
CREATE TABLE combined_rsi (
    symbol         TEXT NOT NULL,
    computed_at    TEXT NOT NULL,
    rsi_14         REAL NOT NULL,
    rsi_9          REAL NOT NULL,
    rsi_5          REAL NOT NULL,
    all_oversold   INTEGER NOT NULL,  -- 1 if all three under thresholds
    all_rising     INTEGER NOT NULL,  -- 1 if all three > prior-week value
    signal         INTEGER NOT NULL,  -- 1 if all_oversold AND all_rising
    weekly_close   REAL NOT NULL,
    bars_used      INTEGER NOT NULL,
    PRIMARY KEY (symbol)
);
CREATE INDEX idx_combined_rsi_signal ON combined_rsi(signal DESC, computed_at DESC);

CREATE TABLE capitulation (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    detected_at     TEXT NOT NULL,
    close_pct       REAL NOT NULL,         -- daily close % change (negative)
    volume_ratio    REAL NOT NULL,         -- vs 30d avg
    range_position  REAL NOT NULL,         -- 0=low, 1=high
    rsi_14_daily    REAL,
    vol_regime      TEXT NOT NULL,         -- low/normal/high
    outcome_3d_pct  REAL,                  -- filled by resolver
    outcome_5d_pct  REAL,
    outcome_10d_pct REAL,
    outcome_hit     INTEGER DEFAULT 0      -- 1 if any outcome > +5%
);
CREATE INDEX idx_capit_detected ON capitulation(detected_at DESC);
```

## Endpoints (mirror existing patterns)

```
GET /api/combined-rsi?signal=true&limit=50
GET /api/combined-rsi/symbol/{sym}
GET /api/capitulation?lookback_days=10
GET /api/capitulation/stats        -> outcome hit-rate, by vol_regime
```

## Effort estimate

| Phase | Scope | Hours |
|---|---|---|
| 5A engine + table + nightly job | `engine/combined_rsi.py` (~250 LOC; mirror `engine/minervini_filter.py` structure) | 3-4h |
| 5A endpoint + AI prompt section | route in `signal-center/server.py` + dashboard tile + AI brain integration | 1.5-2h |
| 5B engine + table + intraday job | `engine/capitulation.py` (~300 LOC; outcome tracking like `volatility_breakouts`) | 4-5h |
| 5B endpoint + NTFY wire + AI prompt | route + ntfy.high() on detection + AI brain | 2-3h |
| Tests + backtest harness | Pytest + 90-day backtest replay on existing universe | 3-4h |
| **Total** | | **13.5-18h** (2-3 sittings) |

## Risks / open decisions

1. **Threshold tuning** — N% capitulation threshold needs market-regime awareness. Default 4%/6% is a starting point; backtest will calibrate. **Decision needed:** ship with hardcoded defaults and tune via shadow mode, or wire to `vol_regime` table from day 1?
2. **Weekly bar alignment** — Combined RSI needs **completed** weekly bars. Friday close ET cron, but partial-week data is misleading. Need a `weekly_complete` flag in the scan.
3. **Capitulation false-positives in earnings windows** — earnings-driven gaps can pattern-match capitulation but are NOT structural. Recommend gating on `earnings_within_5d=false` (data already in `chart_data` table from HM-CHART-DATA-EARNINGS-DATES-POPULATE).
4. **Gate bypass for mean-reversion BUYs** — sister sprint has Grade-B gate blocking BUYs on bear days. Capitulation BUYs are **specifically meant to fire on bear days**. Either bypass via `signal_source` tag (clean) or carve out a `mean_reversion_exception` flag in gate logic (intrusive). **Recommend tag-based bypass.**
5. **VCP precedence** — Tier 2 still has Stage 2 (Weinstein) and VCP open. If those should go before Tier 5, redirect — but per XO instruction Tier 5 is the call.

## Verification checklist (pre-merge)

- [ ] `combined_rsi` table created + indexed
- [ ] `capitulation` table created with outcome columns
- [ ] Nightly cron runs without errors for 3 consecutive sessions
- [ ] `/api/combined-rsi?signal=true` returns non-empty result on a known oversold week (backfill test)
- [ ] `/api/capitulation` outcome resolver fills T+3 / T+5 / T+10 correctly
- [ ] NTFY fires on first live capitulation detection (HIGH severity)
- [ ] AI brain prompt sections render in `build_*_prompt_section` calls
- [ ] Backtest 90d hit-rate ≥ 55% on capitulation signal (otherwise raise threshold)

## Banked follow-ups (not in v1)

- Pair Combined RSI with sector ETF capitulation (broader signal)
- Add Capitulation Finder NTFY classification for "single-name vs sector-wide" capitulation
- Tier 2 Stage 2 Breakout (Weinstein) — separate sprint after Tier 5 bakes
- Tier 2 VCP Detector — separate sprint, complements Minervini for entry timing

## Prod-state verification at scope time (2026-05-24)

Tier 2 status (confirmed against `git log`, DB schema, `engine/` files):

| Component | Status |
|---|---|
| Minervini Trend Template | SHIPPED — commit `84f2524`, table `minervini_trend`, 8 trend conds + rs_pass |
| RS Rank (Minervini cond #6 helper) | SHIPPED — commits `b265ff7` + `bd6f5fc` + `2eccbb8`, table `rs_rank` |
| Stage 2 Breakout (Weinstein weekly 1→2) | NOT SHIPPED |
| VCP Detector | NOT SHIPPED |

Tier 5 status: clean (no commits, no files, no tables, no routes for Combined RSI or Capitulation).
