"""
agents/scotty/scoring.py

Scotty v1 — Short Squeeze Surveillance Scoring
4-signal rubric. Concentration check (5th signal) deferred to v2.

"She cannae take much more, Captain!"

Pure functions. No I/O. Easy to unit test.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


# ---------- Thresholds (hardcoded, simple) ----------
# If you want these in config later, move to agents/scotty/config.py

SHORT_FLOAT_MIN = 20.0       # % of float sold short
FLOAT_MAX_M     = 20.0       # float size in millions
DAYS_TO_COVER   = 5.0        # minimum days to cover
VOL_RATIO_MIN   = 2.0        # relative volume (5d vs. 30d or similar)

WATCHLIST_SCORE = 3          # min score to persist & notify Kirk
ALERT_SCORE     = 4          # min score for ntfy push


@dataclass
class TickerSnapshot:
    """Normalized view of one candidate. Built from run_scan() output."""
    ticker: str
    short_pct: Optional[float]         # % of float sold short
    float_shares_m: Optional[float]    # float in millions
    days_to_cover: Optional[float]     # Finviz "Short Ratio"
    vol_ratio: Optional[float]         # relative volume
    # carried through for dashboard/debugging, not scored in v1:
    price: Optional[float] = None
    rsi: Optional[float] = None
    above_10d_high: Optional[bool] = None


@dataclass
class SqueezeScore:
    ticker: str
    score: int                         # 0..4 in v1
    signals: dict                      # which signals fired
    snapshot: TickerSnapshot


def score_ticker(snap: TickerSnapshot) -> SqueezeScore:
    """Run the 4-signal rubric. Missing data = signal doesn't fire (conservative).

    Note: engine.squeeze_scanner._parse_float_val returns 0.0 (not None) for
    missing values. For most signals that works fine (0.0 fails the >= gate).
    For small_float we need a positive lower bound: a float of 0.0 means
    "missing data", not "zero shares outstanding".
    """
    signals = {
        "short_float":   _gte(snap.short_pct, SHORT_FLOAT_MIN),
        "small_float":   _between(snap.float_shares_m, 0.01, FLOAT_MAX_M),
        "days_to_cover": _gte(snap.days_to_cover, DAYS_TO_COVER),
        "volume_surge":  _gte(snap.vol_ratio, VOL_RATIO_MIN),
    }
    score = sum(1 for v in signals.values() if v)
    return SqueezeScore(ticker=snap.ticker, score=score, signals=signals, snapshot=snap)


def snapshot_from_run_scan_row(row: dict) -> TickerSnapshot:
    """
    Adapt one row from engine.squeeze_scanner.run_scan()["results"] to TickerSnapshot.

    run_scan() returns results with keys like:
        ticker, short_pct, float, days_to_cover (after patch), vol_ratio,
        price, rsi, above_10d_high, score (old 1-10 score — ignored here)

    We re-score using Scotty's rubric, not the old one.
    """
    # "float" in run_scan is in millions already per squeeze_scanner convention.
    # If that's not true in your repo, fix here.
    return TickerSnapshot(
        ticker=row.get("ticker") or row.get("Ticker") or "",
        short_pct=_safe_float(row.get("short_interest_pct") or row.get("short_pct")),
        float_shares_m=_safe_float(row.get("float_m") or row.get("float")),
        days_to_cover=_safe_float(row.get("days_to_cover")),
        vol_ratio=_safe_float(row.get("vol_ratio")),
        price=_safe_float(row.get("price")),
        rsi=_safe_float(row.get("rsi")),
        above_10d_high=row.get("above_10d_high"),
    )


# ---------- helpers ----------

def _gte(value: Optional[float], threshold: float) -> bool:
    return value is not None and value >= threshold


def _lte(value: Optional[float], threshold: float) -> bool:
    return value is not None and value <= threshold


def _between(value: Optional[float], low: float, high: float) -> bool:
    """Inclusive range check. Used when 0.0 means 'missing' rather than 'zero'."""
    return value is not None and low <= value <= high


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------- sanity check: Scotty vs. historical CAR ----------

if __name__ == "__main__":
    # CAR in late March 2026 — the setup Scotty should catch BEFORE the rip.
    car_late_march = TickerSnapshot(
        ticker="CAR",
        short_pct=54.0,
        float_shares_m=10.1,
        days_to_cover=7.3,
        vol_ratio=3.2,  # estimated
    )
    r = score_ticker(car_late_march)
    print(f"CAR (pre-squeeze): {r.score}/4  signals={r.signals}")
    assert r.score == 4, "v1 rubric should catch CAR at 4/4"

    # Edge case: ticker with missing float data (parse returned 0.0).
    # Must NOT falsely fire "small_float" just because 0.0 <= 20.0.
    missing_float = TickerSnapshot(
        ticker="XXXX",
        short_pct=25.0,
        float_shares_m=0.0,   # missing, not literally zero shares
        days_to_cover=6.0,
        vol_ratio=2.5,
    )
    r2 = score_ticker(missing_float)
    print(f"XXXX (missing float): {r2.score}/4  signals={r2.signals}")
    assert r2.signals["small_float"] is False, "small_float must not fire on 0.0 (treated as missing)"
    assert r2.score == 3, "other 3 signals should still fire"

    print("OK: v1 rubric flags CAR at full score AND handles missing-float safely")
