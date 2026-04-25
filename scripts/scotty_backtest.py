"""
scripts/scotty_backtest.py

Scotty 6-Month "For Fun" Backtest
==================================

Produces three outputs:
  1. EVENT STUDY (Design C) — would Scotty have caught known squeezes?
  2. NOISE FLOOR  (Design A) — how often does the rubric fire; forward returns
  3. TODAY'S TOP — sniff test of what Scotty would flag tomorrow

HONEST CAVEATS (Admiral, read this):
  - Output 1 uses today's short_interest/float as a proxy for historical values.
    This is LOOKAHEAD BIAS. If a name squeezed and ended up on today's "still high
    short interest" list, we'll falsely credit Scotty for "catching" it. A name
    that squeezed and then saw short covering (lower SI today) will LOOK LIKE
    Scotty missed it when he might not have. This is a vibes check, not a proof.
  - Output 2 has the same lookahead bias. Sampling N random days over 6mo gives
    us the approximate shape of hits/noise.
  - Forward returns are RAW stock returns, not risk-adjusted, not fee-adjusted.
  - Universe is today's Finviz Ownership screener (Float Short > 20%).
    Delisted names from the last 6 months are NOT in the universe = survivorship bias.
  - Small sample: expect 5-30 4/4 hits in 6 months. Vibes, not statistics.

USAGE:
    cd ~/BigMac
    python -m scripts.scotty_backtest

DEPENDENCIES (should all already be installed in .venv-crew):
    yfinance, pandas, numpy, finvizfinance (already pulled for shared/finviz_scanner.py)
"""
from __future__ import annotations

import sys
import time
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import numpy as np

# We reuse Scotty's own scoring module so we're testing what Scotty actually does.
from agents.scotty.scoring import (
    TickerSnapshot, score_ticker,
    SHORT_FLOAT_MIN, FLOAT_MAX_M, DAYS_TO_COVER, VOL_RATIO_MIN,
)

log = logging.getLogger("scotty_backtest")


# --- Editable knobs -------------------------------------------------------

# Known squeezes in the last 6 months. EDIT THIS LIST based on what you know.
# Format: (ticker, approximate_peak_date_YYYY-MM-DD, brief note)
KNOWN_SQUEEZES = [
    ("CAR",  "2026-04-21", "Avis — 54% SI, 10M float, SRS+Pentwater. +500%"),
    ("CVNA", "2026-01-22", "Carvana — earnings + short cover, peak $478.45"),
    ("RXT",  "2026-02-23", "Rackspace — Palantir partnership + short cover, +265% in 5d"),
]

# Universe for noise-floor analysis: today's high-short-interest names.
# Pulled fresh via finvizfinance ownership screener.
UNIVERSE_SHORT_FLOAT_MIN = 15.0  # slightly looser than Scotty's 20 to broaden the universe

# Forward-return windows (trading days after the hit)
FORWARD_WINDOWS = [5, 10, 20]

# For Output 2 (noise floor): how many random historical dates to sample.
# More = slower. 30 is a reasonable vibes check.
NOISE_SAMPLE_DAYS = 30

# Lookback window in calendar days
LOOKBACK_DAYS = 180


# --- Yfinance helper ------------------------------------------------------

def _yf_history(ticker: str, start: datetime, end: datetime) -> Optional[pd.DataFrame]:
    """Wrapper that handles the yfinance MultiIndex quirk we just patched in squeeze_scanner."""
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        hist = tk.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
        if hist.empty:
            return None
        # yf.Ticker().history returns single-level columns, no squeeze needed
        return hist
    except Exception as e:
        log.warning(f"yf history failed for {ticker}: {e}")
        return None


def _vol_ratio_on_date(hist: pd.DataFrame, as_of_idx: int,
                      short_window: int = 5, long_window: int = 30) -> Optional[float]:
    """Compute rel volume = avg(last 5d) / avg(last 30d) as of given index."""
    if as_of_idx < long_window:
        return None
    short_avg = hist["Volume"].iloc[as_of_idx - short_window: as_of_idx].mean()
    long_avg  = hist["Volume"].iloc[as_of_idx - long_window: as_of_idx].mean()
    if not long_avg or np.isnan(long_avg) or long_avg == 0:
        return None
    return float(short_avg / long_avg)


# --- Output 1: Event Study ------------------------------------------------

def event_study() -> list[dict]:
    """
    For each known squeeze, check what Scotty's rubric would have scored
    ~30 and ~14 days before the peak. Uses today's SI/float/d2c as proxies
    (LOOKAHEAD BIAS — noted in header).
    """
    print("\n" + "=" * 70)
    print("OUTPUT 1 — EVENT STUDY (Design C)")
    print("Would Scotty have caught the known squeezes of the last 6 months?")
    print("=" * 70)
    print("\n(Uses today's SI/float/d2c as proxy — has lookahead bias.)\n")

    # Pull today's Finviz ownership data once
    try:
        from finvizfinance.screener.ownership import Ownership
        ownership = Ownership()
        ownership.set_filter(filters_dict={
            "Float Short": "Over 10%",  # cast a wide net for the event study
        })
        own_df = ownership.screener_view()
        if "Ticker" in own_df.columns:
            own_df = own_df.set_index("Ticker")
    except Exception as e:
        print(f"  ERROR: couldn't pull Finviz ownership: {e}")
        return []

    results = []
    for ticker, peak_date_str, note in KNOWN_SQUEEZES:
        peak = datetime.strptime(peak_date_str, "%Y-%m-%d")
        t30 = peak - timedelta(days=30)
        t14 = peak - timedelta(days=14)

        # Today's signals (proxy) from Finviz
        si_pct = float_m = d2c = None
        if ticker in own_df.index:
            row = own_df.loc[ticker]
            si_pct = _parse_pct(row.get("Float Short"))
            float_m = _parse_float_shares(row.get("Float"))
            d2c = _parse_scalar(row.get("Short Ratio"))
        if si_pct is None:
            # Fallback: pull from yfinance — these fields are also lookahead-biased
            # but it's the best we have for already-squeezed names.
            try:
                import yfinance as yf
                info = yf.Ticker(ticker).info
                sh_short = info.get("sharesShort")
                sh_float = info.get("floatShares")
                short_ratio = info.get("shortRatio")
                if sh_short and sh_float and sh_float > 0:
                    si_pct = (sh_short / sh_float) * 100
                    float_m = sh_float / 1_000_000
                    d2c = short_ratio
                    print(f"  {ticker:6s}  (not in Finviz today — using yfinance fallback: SI={si_pct:.1f}%, float={float_m:.1f}M, d2c={d2c})")
                else:
                    print(f"  {ticker:6s}  NOT in Finviz and no yfinance fallback data")
                    results.append({
                        "ticker": ticker, "peak_date": peak_date_str, "note": note,
                        "in_finviz_today": False,
                    })
                    continue
            except Exception as e:
                print(f"  {ticker:6s}  yfinance fallback failed: {e}")
                continue

        # Fetch history for volume-ratio signal
        hist = _yf_history(ticker, peak - timedelta(days=LOOKBACK_DAYS), peak + timedelta(days=5))
        if hist is None or hist.empty:
            print(f"  {ticker:6s}  no price history")
            continue

        # Find the idx closest to t30 and t14
        def _score_at(target_date: datetime) -> tuple[int, dict]:
            hist_idx_series = hist.index
            # find the last bar on or before target_date
            mask = hist_idx_series.tz_localize(None) <= pd.Timestamp(target_date)
            if not mask.any():
                return 0, {}
            idx = mask.sum() - 1  # position index
            vr = _vol_ratio_on_date(hist, idx)
            snap = TickerSnapshot(
                ticker=ticker,
                short_pct=si_pct,
                float_shares_m=float_m,
                days_to_cover=d2c,
                vol_ratio=vr,
            )
            s = score_ticker(snap)
            return s.score, s.signals

        s30, sig30 = _score_at(t30)
        s14, sig14 = _score_at(t14)

        # Realized move from t30 to peak
        try:
            t30_idx = (hist.index.tz_localize(None) <= pd.Timestamp(t30)).sum() - 1
            peak_idx = (hist.index.tz_localize(None) <= pd.Timestamp(peak)).sum() - 1
            px_t30 = float(hist["Close"].iloc[t30_idx])
            px_peak_area = float(hist["Close"].iloc[peak_idx])
            pct_gain = (px_peak_area / px_t30 - 1) * 100
        except Exception:
            pct_gain = None

        if pct_gain is not None:
            print(f"  {ticker:6s}  T-30d: {s30}/4 {list(k for k,v in sig30.items() if v)}  "
                  f"T-14d: {s14}/4 {list(k for k,v in sig14.items() if v)}  "
                  f"realized: +{pct_gain:.0f}%")
        else:
            print(f"  {ticker:6s}  T-30d: {s30}/4  T-14d: {s14}/4  (history issue)")
        print(f"          SI={si_pct}%  float={float_m}M  d2c={d2c}  note: {note}")

        results.append({
            "ticker": ticker, "peak_date": peak_date_str, "note": note,
            "in_finviz_today": True,
            "si_pct_today": si_pct, "float_m_today": float_m, "d2c_today": d2c,
            "score_t30": s30, "signals_t30": sig30,
            "score_t14": s14, "signals_t14": sig14,
            "realized_gain_pct": pct_gain,
        })

    # Summary
    hits_4 = sum(1 for r in results if r.get("score_t30") == 4 or r.get("score_t14") == 4)
    hits_3_plus = sum(1 for r in results if max(r.get("score_t30", 0), r.get("score_t14", 0)) >= 3)
    total = len([r for r in results if r.get("in_finviz_today")])
    print(f"\n  SUMMARY: of {total} names still in Finviz ownership, "
          f"Scotty would have flagged {hits_3_plus} at >=3/4, {hits_4} at 4/4")
    return results


# --- Output 2: Noise Floor ------------------------------------------------

def noise_floor() -> dict:
    """
    Sample NOISE_SAMPLE_DAYS random trading days over the last 6 months.
    For each day, apply Scotty's rubric to the current Finviz ownership universe
    using volume-ratio computed as of that date. Track forward returns.
    """
    print("\n" + "=" * 70)
    print("OUTPUT 2 — NOISE FLOOR (Design A)")
    print(f"Sampling {NOISE_SAMPLE_DAYS} random days from last {LOOKBACK_DAYS}d")
    print("=" * 70)
    print("\n(Lookahead bias on SI/float — noise floor is approximate.)\n")

    # Pull ownership universe once
    try:
        from finvizfinance.screener.ownership import Ownership
        ownership = Ownership()
        ownership.set_filter(filters_dict={"Float Short": f"Over 15%"})
        own_df = ownership.screener_view()
    except Exception as e:
        print(f"  ERROR: Finviz pull failed: {e}")
        return {}

    if own_df is None or own_df.empty:
        print("  No universe — aborting noise floor test.")
        return {}

    # Build static snapshot (today's SI/float/d2c, no vol_ratio yet)
    universe: list[dict] = []
    for _, row in own_df.iterrows():
        ticker = row.get("Ticker")
        si = _parse_pct(row.get("Float Short"))
        fl = _parse_float_shares(row.get("Float"))
        d2c = _parse_scalar(row.get("Short Ratio"))
        if not ticker or si is None or si < SHORT_FLOAT_MIN:
            continue
        universe.append({"ticker": ticker, "si": si, "float_m": fl, "d2c": d2c})

    # Debug: show the raw parsed distribution
    sample_raw = [row.get("Float Short") for _, row in own_df.head(5).iterrows()]
    print(f"  DEBUG: first 5 raw 'Float Short' values: {sample_raw}")
    sample_parsed = [_parse_pct(v) for v in sample_raw]
    print(f"  DEBUG: parsed to: {sample_parsed}")

    print(f"  Universe size (today's SI >= {SHORT_FLOAT_MIN}%): {len(universe)} tickers")

    # Sample random dates over last 180 days
    rng = np.random.default_rng(seed=42)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    business_days = pd.bdate_range(start=start_date, end=end_date - timedelta(days=25))
    # end cutoff: leave 25d at the end to compute forward returns
    sample_dates = rng.choice(business_days, size=min(NOISE_SAMPLE_DAYS, len(business_days)),
                              replace=False)

    # For each universe ticker, cache history once
    print(f"  Fetching price history for {len(universe)} tickers...")
    histories: dict[str, pd.DataFrame] = {}
    for i, u in enumerate(universe):
        if i % 25 == 0:
            print(f"    ... {i}/{len(universe)}")
        h = _yf_history(u["ticker"],
                        start_date - timedelta(days=35),
                        end_date + timedelta(days=2))
        if h is not None and not h.empty:
            h.index = h.index.tz_localize(None)  # normalize for comparison
            histories[u["ticker"]] = h
        time.sleep(0.1)  # be polite

    print(f"  Got history for {len(histories)}/{len(universe)} tickers")

    # Run the rubric for each (date, ticker) combo
    hits = []  # list of dicts with score, date, ticker, forward_returns
    for d in sample_dates:
        d_ts = pd.Timestamp(d)
        for u in universe:
            hist = histories.get(u["ticker"])
            if hist is None:
                continue
            mask = hist.index <= d_ts
            if not mask.any():
                continue
            idx = int(mask.sum() - 1)
            if idx < 30:
                continue  # need 30 bars for vol_ratio
            vr = _vol_ratio_on_date(hist, idx)
            snap = TickerSnapshot(
                ticker=u["ticker"], short_pct=u["si"],
                float_shares_m=u["float_m"], days_to_cover=u["d2c"], vol_ratio=vr,
            )
            s = score_ticker(snap)
            if s.score < 3:
                continue
            # Forward returns
            entry_px = float(hist["Close"].iloc[idx])
            fwd = {}
            for w in FORWARD_WINDOWS:
                if idx + w < len(hist):
                    exit_px = float(hist["Close"].iloc[idx + w])
                    fwd[f"fwd_{w}d_pct"] = (exit_px / entry_px - 1) * 100
            hits.append({
                "date": d_ts.date().isoformat(),
                "ticker": u["ticker"],
                "score": s.score,
                "entry_px": entry_px,
                **fwd,
            })

    # Summarize
    if not hits:
        print("  No hits found across sample.")
        return {"hits": 0}

    hits_df = pd.DataFrame(hits)
    print(f"\n  Total hits >= 3/4 across {NOISE_SAMPLE_DAYS} sampled days: {len(hits_df)}")
    print(f"  Unique tickers hit: {hits_df['ticker'].nunique()}")
    print(f"  Breakdown by score: {hits_df['score'].value_counts().sort_index().to_dict()}")
    print(f"  Avg hits per sampled day: {len(hits_df) / NOISE_SAMPLE_DAYS:.1f}")

    print("\n  Forward return stats (all hits >=3/4, all sampled days):")
    for w in FORWARD_WINDOWS:
        col = f"fwd_{w}d_pct"
        if col in hits_df.columns:
            s = hits_df[col].dropna()
            if len(s):
                print(f"    {w:2d}-day:  n={len(s):4d}  median={s.median():+6.2f}%  "
                      f"mean={s.mean():+6.2f}%  win_rate={(s>0).mean()*100:5.1f}%  "
                      f"p90={s.quantile(0.9):+6.2f}%  p10={s.quantile(0.1):+6.2f}%")

    print("\n  Same stats, filtered to 4/4 hits only:")
    hits_4 = hits_df[hits_df["score"] == 4]
    if len(hits_4):
        for w in FORWARD_WINDOWS:
            col = f"fwd_{w}d_pct"
            if col in hits_4.columns:
                s = hits_4[col].dropna()
                if len(s):
                    print(f"    {w:2d}-day:  n={len(s):4d}  median={s.median():+6.2f}%  "
                          f"mean={s.mean():+6.2f}%  win_rate={(s>0).mean()*100:5.1f}%  "
                          f"p90={s.quantile(0.9):+6.2f}%  p10={s.quantile(0.1):+6.2f}%")
    else:
        print("    (no 4/4 hits in sample)")

    # Top tickers by hit frequency
    print("\n  Top 15 tickers by # of hits:")
    for tk, cnt in hits_df["ticker"].value_counts().head(15).items():
        sub = hits_df[hits_df["ticker"] == tk]
        med_20 = sub.get("fwd_20d_pct", pd.Series()).median()
        print(f"    {tk:6s}  hits={cnt:3d}  median_20d={med_20:+6.2f}%")

    return {"hits": len(hits_df), "hits_df": hits_df}


# --- Output 3: Today's Sniff Test -----------------------------------------

def todays_snapshot() -> None:
    """Run Scotty's rubric on today's universe. The sniff test."""
    print("\n" + "=" * 70)
    print("OUTPUT 3 — TODAY'S TOP (Sniff test)")
    print("What would Scotty flag if he ran right now?")
    print("=" * 70)

    # Reuse the actual Scotty pipeline
    try:
        from agents.scotty.scotty import run_daily_scan
        results = run_daily_scan()
    except Exception as e:
        print(f"  ERROR: {e}")
        return

    top = [r for r in results if r.score >= 3][:20]
    if not top:
        print("  (no names at >=3/4 right now — market may be closed or gates are tight)")
        return

    print(f"\n  {'ticker':<8}{'score':>6}{'SI%':>8}{'float_M':>9}{'d2c':>7}{'vol_x':>7}")
    print(f"  {'-'*8}{'-'*6:>6}{'-'*8:>8}{'-'*9:>9}{'-'*7:>7}{'-'*7:>7}")
    for r in top:
        s = r.snapshot
        print(f"  {r.ticker:<8}{r.score}/4  {(s.short_pct or 0):>6.1f}%  "
              f"{(s.float_shares_m or 0):>7.1f}M  {(s.days_to_cover or 0):>5.1f}  "
              f"{(s.vol_ratio or 0):>5.1f}x")


# --- helpers for parsing Finviz strings -----------------------------------

def _parse_pct(val) -> Optional[float]:
    """Handle Finviz's inconsistent representation: '21.69%', 21.69, or 0.2169."""
    if val is None or val == "-" or (isinstance(val, float) and np.isnan(val)):
        return None
    # Already numeric?
    if isinstance(val, (int, float)):
        v = float(val)
        # If it looks like a ratio (0.0-1.0), scale it up to percent
        return v * 100 if 0 < v < 1 else v
    # String form
    s = str(val).strip().rstrip("%")
    try:
        v = float(s)
        return v * 100 if 0 < v < 1 else v
    except ValueError:
        return None


def _parse_float_shares(val) -> Optional[float]:
    if val is None or val == "-":
        return None
    if isinstance(val, (int, float)):
        if np.isnan(val):
            return None
        v = float(val)
        # Assume raw share count if > 1M, else already in millions
        return v / 1_000_000 if v > 1_000_000 else v
    s = str(val).strip()
    try:
        if s.endswith("M"): return float(s[:-1])
        if s.endswith("B"): return float(s[:-1]) * 1000
        if s.endswith("K"): return float(s[:-1]) / 1000
        v = float(s)
        return v / 1_000_000 if v > 1_000_000 else v
    except ValueError:
        return None


def _parse_scalar(val) -> Optional[float]:
    if val is None or val == "-":
        return None
    if isinstance(val, (int, float)):
        return None if np.isnan(val) else float(val)
    try:
        return float(str(val).strip())
    except ValueError:
        return None


# --- entry point ----------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    print("\n" + "#" * 70)
    print("# SCOTTY BACKTEST — 6-MONTH VIBES CHECK")
    print("# For fun. Not decision-quality. See script header for honest caveats.")
    print("#" * 70)

    # Output 1: Event study (the hero output)
    event_results = event_study()

    # Output 2: Noise floor
    noise_results = noise_floor()

    # Output 3: Today's top
    todays_snapshot()

    print("\n" + "#" * 70)
    print("# BACKTEST COMPLETE")
    print("#" * 70)
    print("\nWhat this told you:")
    print("  1. Did Scotty's rubric flag the known squeezes before their peaks?")
    print("  2. What's the approximate hit rate and forward return distribution?")
    print("  3. What would Scotty flag tomorrow?\n")
    print("What this did NOT tell you:")
    print("  - Whether Scotty's signal is actually profitable (need live data)")
    print("  - Point-in-time accuracy (used today's SI as proxy)")
    print("  - Survivorship-free universe (missing delisted names)\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
