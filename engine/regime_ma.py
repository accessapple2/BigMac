"""8/21 MA Cross Regime Filter — primary trend regime for position sizing.

Uses daily SPY and QQQ close prices to compute 8-day and 21-day SMAs.
Determines one of four regimes with a position size modifier:

  BULL_CROSS:    8MA > 21MA, price > 8MA  → 100% size (full aggression)
  CAUTIOUS_BULL: 8MA > 21MA, price < 8MA  → 75%  size (pullback inside uptrend)
  CAUTIOUS_BEAR: 8MA < 21MA, price > 8MA  → 50%  size (first sign of life in downtrend)
  BEAR_CROSS:    8MA < 21MA, price < 8MA  → 25%  size (defensive only)

The cross date (when 8MA last crossed 21MA) and days-since are tracked so
the AI can reason about how mature the trend is.

Data saved to regime_history in trader.db after each calculation.
In-memory cache with 5-minute TTL prevents redundant yfinance calls.
"""
from __future__ import annotations
import threading
import time
from datetime import datetime, timedelta
from rich.console import Console

console = Console()

_cache: dict = {"data": None, "ts": 0}
_lock = threading.Lock()
CACHE_TTL = 300  # 5 minutes

# Regime → position size multiplier
SIZE_MODIFIERS = {
    "BULL_CROSS":    1.00,
    "CAUTIOUS_BULL": 0.75,
    "CAUTIOUS_BEAR": 0.50,
    "BEAR_CROSS":    0.25,
}

# Human-readable descriptions
REGIME_DESCRIPTIONS = {
    "BULL_CROSS":    "8MA > 21MA, price above 8MA. Full trend confirmed — normal sizing.",
    "CAUTIOUS_BULL": "8MA > 21MA but price below 8MA. Uptrend intact, pullback underway — reduce size.",
    "CAUTIOUS_BEAR": "8MA < 21MA but price above 8MA. Downtrend with bounce — small positions only.",
    "BEAR_CROSS":    "8MA < 21MA, price below 8MA. Downtrend confirmed — defensive only.",
}

# Colorblind-safe icons
REGIME_ICONS = {
    "BULL_CROSS":    "▲▲",
    "CAUTIOUS_BULL": "▲◇",
    "CAUTIOUS_BEAR": "▼◇",
    "BEAR_CROSS":    "▼▼",
}


def _compute_sma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return round(sum(closes[-period:]) / period, 2)


def _find_cross_date(dates: list[str], ma8: list[float], ma21: list[float]) -> str | None:
    """Walk backward through history to find when 8MA last crossed 21MA."""
    if len(ma8) < 2 or len(ma21) < 2:
        return None
    # Current state: is 8MA above or below 21MA?
    current_above = ma8[-1] > ma21[-1]
    for i in range(len(ma8) - 2, 0, -1):
        prev_above = ma8[i - 1] > ma21[i - 1]
        if prev_above != current_above:
            # Cross happened between index i-1 and i
            return dates[i] if i < len(dates) else None
    return None


def detect_ma_cross_regime() -> dict:
    """Compute 8/21 MA cross regime for SPY (QQQ as confirmation).

    Returns dict with keys:
      regime, size_modifier, description, icon,
      spy_close, spy_ma8, spy_ma21,
      qqq_close, qqq_ma8, qqq_ma21,
      cross_date, cross_days_ago,
      updated
    """
    now = time.time()

    with _lock:
        if _cache["data"] is not None and (now - _cache["ts"]) < CACHE_TTL:
            return dict(_cache["data"])

    try:
        import yfinance as yf
        import pandas as pd

        # Fetch 60 trading days (~3 months) of daily data for SPY and QQQ
        raw = yf.download(
            "SPY QQQ",
            period="90d",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )

        # yfinance multi-ticker returns MultiIndex columns
        if isinstance(raw.columns, pd.MultiIndex):
            spy_closes = raw["Close"]["SPY"].dropna().tolist()
            spy_dates  = [str(d.date()) for d in raw["Close"]["SPY"].dropna().index]
            qqq_closes = raw["Close"]["QQQ"].dropna().tolist()
        else:
            # Single ticker fallback
            spy_closes = raw["Close"].dropna().tolist()
            spy_dates  = [str(d.date()) for d in raw["Close"].dropna().index]
            qqq_closes = []

        if len(spy_closes) < 21:
            raise ValueError(f"Insufficient history: {len(spy_closes)} days")

        # SPY MAs
        spy_ma8  = _compute_sma(spy_closes, 8)
        spy_ma21 = _compute_sma(spy_closes, 21)
        spy_close = round(spy_closes[-1], 2)

        # Build per-day 8/21 lists for cross-date detection
        ma8_series  = [_compute_sma(spy_closes[:i+1], 8)  for i in range(len(spy_closes))]
        ma21_series = [_compute_sma(spy_closes[:i+1], 21) for i in range(len(spy_closes))]
        # Filter out None entries
        valid_idx = [i for i, (m8, m21) in enumerate(zip(ma8_series, ma21_series)) if m8 and m21]
        v_dates = [spy_dates[i] for i in valid_idx]
        v_ma8   = [ma8_series[i] for i in valid_idx]
        v_ma21  = [ma21_series[i] for i in valid_idx]

        cross_date_str = _find_cross_date(v_dates, v_ma8, v_ma21)
        cross_days_ago = None
        if cross_date_str:
            try:
                cross_dt = datetime.strptime(cross_date_str, "%Y-%m-%d")
                cross_days_ago = (datetime.now() - cross_dt).days
            except Exception:
                pass

        # QQQ confirmation
        qqq_ma8 = qqq_ma21 = qqq_close = None
        if len(qqq_closes) >= 21:
            qqq_ma8   = _compute_sma(qqq_closes, 8)
            qqq_ma21  = _compute_sma(qqq_closes, 21)
            qqq_close = round(qqq_closes[-1], 2)

        # Determine regime
        bull_cross = spy_ma8 > spy_ma21
        price_above_ma8 = spy_close > spy_ma8

        if bull_cross and price_above_ma8:
            regime = "BULL_CROSS"
        elif bull_cross and not price_above_ma8:
            regime = "CAUTIOUS_BULL"
        elif not bull_cross and price_above_ma8:
            regime = "CAUTIOUS_BEAR"
        else:
            regime = "BEAR_CROSS"

        result = {
            "regime": regime,
            "size_modifier": SIZE_MODIFIERS[regime],
            "description": REGIME_DESCRIPTIONS[regime],
            "icon": REGIME_ICONS[regime],
            "spy_close": spy_close,
            "spy_ma8": spy_ma8,
            "spy_ma21": spy_ma21,
            "qqq_close": qqq_close,
            "qqq_ma8": qqq_ma8,
            "qqq_ma21": qqq_ma21,
            "cross_date": cross_date_str,
            "cross_days_ago": cross_days_ago,
            "updated": datetime.now().isoformat(),
        }

        with _lock:
            _cache["data"] = result
            _cache["ts"] = now

        # Persist to DB (non-blocking, ignore errors)
        try:
            _save_to_db(result)
        except Exception:
            pass

        return result

    except Exception as e:
        console.log(f"[yellow]MA cross regime error: {e}")
        # Return stale cache if available, else a safe default
        if _cache["data"] is not None:
            return dict(_cache["data"])
        return _default_result()


def _default_result() -> dict:
    return {
        "regime": "UNKNOWN",
        "size_modifier": 0.75,
        "description": "Unable to compute 8/21 MA cross.",
        "icon": "?",
        "spy_close": None,
        "spy_ma8": None,
        "spy_ma21": None,
        "qqq_close": None,
        "qqq_ma8": None,
        "qqq_ma21": None,
        "cross_date": None,
        "cross_days_ago": None,
        "updated": datetime.now().isoformat(),
    }


def get_ma_cross_size_modifier() -> float:
    """Return the position size multiplier for current 8/21 MA regime.
    Safe to call from buy() — uses cache, never blocks more than TTL seconds.
    Falls back to 0.75 on any error.
    """
    try:
        regime = detect_ma_cross_regime()
        return regime.get("size_modifier", 0.75)
    except Exception:
        return 0.75


def build_ma_cross_prompt_section() -> str:
    """Format 8/21 MA cross regime as a text block for AI prompts."""
    try:
        r = detect_ma_cross_regime()
        if r["regime"] == "UNKNOWN" or r["spy_ma8"] is None:
            return ""

        cross_info = ""
        if r["cross_date"] and r["cross_days_ago"] is not None:
            cross_info = f" (cross: {r['cross_date']}, {r['cross_days_ago']} days ago)"

        qqq_line = ""
        if r["qqq_ma8"] and r["qqq_ma21"]:
            qqq_trend = "bull" if r["qqq_ma8"] > r["qqq_ma21"] else "bear"
            qqq_line = f"\nQQQ: ${r['qqq_close']} | 8MA=${r['qqq_ma8']} | 21MA=${r['qqq_ma21']} ({qqq_trend} cross)"

        size_pct = int(r["size_modifier"] * 100)
        return (
            f"\n=== MARKET REGIME (8/21 MA Cross): {r['regime']}{cross_info} ===\n"
            f"{r['description']}\n"
            f"SPY: ${r['spy_close']} | 8MA=${r['spy_ma8']} | 21MA=${r['spy_ma21']}{qqq_line}\n"
            f"Position sizing: {size_pct}% of normal. "
            f"{'Trend is HIGHER until bear cross confirmed.' if 'BULL' in r['regime'] else 'Trend is LOWER until bull cross confirmed.'}\n"
        )
    except Exception:
        return ""


def _save_to_db(r: dict):
    """Persist regime reading to regime_history table."""
    import sqlite3
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect("data/trader.db")
        conn.execute(
            """INSERT OR REPLACE INTO regime_history
               (date, spy_close, ma_8, ma_21, qqq_close, qqq_ma_8, qqq_ma_21,
                regime, cross_date, cross_days_ago, size_modifier, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
            (
                today,
                r.get("spy_close"),
                r.get("spy_ma8"),
                r.get("spy_ma21"),
                r.get("qqq_close"),
                r.get("qqq_ma8"),
                r.get("qqq_ma21"),
                r["regime"],
                r.get("cross_date"),
                r.get("cross_days_ago"),
                r.get("size_modifier"),
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
