from __future__ import annotations
"""Holodeck Nightly Simulation — strategy_rotator.py

Backtests all strategies against recent Alpaca data nightly and ranks
which are currently working. Results are stored in strategy_rotation table.

Usage:
    from engine.strategy_rotator import run_strategy_rotation, get_active_strategies
    result = run_strategy_rotation()
    active = get_active_strategies()
"""

import gc
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("TRADEMINDS_DB", "data/trader.db")

STRATEGY_CONFIGS: dict[str, dict] = {
    "breakout_volume":       {"type": "momentum",  "hold_days": 5,  "stop_pct": 0.03, "target_mult": 2.0},
    "pullback_sma20":        {"type": "momentum",  "hold_days": 5,  "stop_pct": 0.03, "target_mult": 2.0},
    "rsi_oversold_bounce":   {"type": "reversal",  "hold_days": 3,  "stop_pct": 0.04, "target_mult": 1.5},
    "macd_crossover":        {"type": "momentum",  "hold_days": 5,  "stop_pct": 0.03, "target_mult": 2.0},
    "golden_cross":          {"type": "trend",     "hold_days": 10, "stop_pct": 0.05, "target_mult": 2.5},
    "bollinger_bounce":      {"type": "reversal",  "hold_days": 3,  "stop_pct": 0.04, "target_mult": 1.5},
    "rsi_divergence":        {"type": "reversal",  "hold_days": 4,  "stop_pct": 0.04, "target_mult": 2.0},
    "gap_fill":              {"type": "reversal",  "hold_days": 2,  "stop_pct": 0.05, "target_mult": 1.5},
    "unusual_volume":        {"type": "momentum",  "hold_days": 3,  "stop_pct": 0.03, "target_mult": 2.0},
    "volume_dry_up":         {"type": "momentum",  "hold_days": 5,  "stop_pct": 0.03, "target_mult": 2.0},
    "accumulation":          {"type": "momentum",  "hold_days": 5,  "stop_pct": 0.03, "target_mult": 2.0},
    "ema_ribbon":            {"type": "trend",     "hold_days": 8,  "stop_pct": 0.04, "target_mult": 2.0},
    "higher_highs":          {"type": "trend",     "hold_days": 8,  "stop_pct": 0.04, "target_mult": 2.5},
    "trend_resumption":      {"type": "momentum",  "hold_days": 5,  "stop_pct": 0.03, "target_mult": 2.0},
    "relative_strength_high":{"type": "momentum",  "hold_days": 5,  "stop_pct": 0.03, "target_mult": 2.0},
}

# Minimum trades required to include a strategy in results
MIN_TRADES = 5
# Active strategy filter thresholds
ACTIVE_WIN_RATE = 0.55
ACTIVE_RISK_REWARD = 1.5
ACTIVE_PROFIT_FACTOR = 1.2
# Max active strategies to mark
MAX_ACTIVE = 10
# Max symbols for memory management
MAX_SYMBOLS = 300
# Batch size for Alpaca requests
ALPACA_BATCH_SIZE = 50
# Lookback days for bar fetch
LOOKBACK_DAYS = 30

# ---------------------------------------------------------------------------
# S&P 500 fallback universe (100 liquid names)
# ---------------------------------------------------------------------------

_FALLBACK_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK.B", "LLY", "AVGO",
    "JPM", "UNH", "XOM", "V", "MA", "HD", "PG", "COST", "JNJ", "ORCL",
    "ABBV", "MRK", "CVX", "NFLX", "CRM", "BAC", "KO", "PEP", "TMO", "WMT",
    "AMD", "DIS", "INTC", "CSCO", "ADBE", "ABT", "MCD", "PM", "LIN", "NEE",
    "WFC", "ACN", "DHR", "TXN", "LOW", "QCOM", "IBM", "HON", "AMGN", "SPGI",
    "CAT", "RTX", "UPS", "INTU", "GE", "ISRG", "SBUX", "NOW", "BKNG", "AXP",
    "GS", "T", "MDT", "PLD", "VRTX", "DE", "GILD", "SYK", "ADI", "REGN",
    "MDLZ", "CI", "PANW", "TJX", "BDX", "CME", "ZTS", "C", "ADP", "MMC",
    "MO", "EQIX", "USB", "EOG", "COP", "NSC", "ITW", "WM", "SHW", "F",
    "GM", "SO", "PNC", "DUK", "NOC", "GD", "LMT", "ETN", "APD", "EMR",
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_db() -> None:
    """Create tables and indexes if they don't exist (SACRED: no DROP/DELETE/TRUNCATE)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS strategy_rotation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date TEXT NOT NULL,
        strategy_name TEXT NOT NULL,
        win_rate REAL,
        avg_win_pct REAL,
        avg_loss_pct REAL,
        risk_reward REAL,
        profit_factor REAL,
        total_return_pct REAL,
        trades_count INTEGER,
        rank INTEGER,
        is_active INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_rotation_date ON strategy_rotation(run_date);
    CREATE INDEX IF NOT EXISTS idx_rotation_strategy ON strategy_rotation(strategy_name);
    """
    try:
        with _conn() as c:
            c.executescript(ddl)
        logger.debug("strategy_rotation table ready")
    except Exception as exc:
        logger.error("_init_db failed: %s", exc)


# ---------------------------------------------------------------------------
# Alpaca bar fetch
# ---------------------------------------------------------------------------

def _fetch_bars_for_symbols(symbols: list[str], lookback_days: int = LOOKBACK_DAYS) -> dict[str, list]:
    """Fetch daily OHLCV bars from Alpaca for given symbols.

    Returns:
        {symbol: [{"o","h","l","c","v","t"}, ...]} sorted oldest→newest.
        Missing or errored symbols are omitted.
    """
    try:
        from alpaca.data import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        from engine.rate_limiter import limiter
    except ImportError as exc:
        logger.error("Alpaca import failed: %s", exc)
        return {}

    api_key = os.environ.get("ALPACA_API_KEY", "")
    secret_key = os.environ.get("ALPACA_SECRET_KEY", "")
    if not api_key or not secret_key:
        logger.warning("ALPACA_API_KEY / ALPACA_SECRET_KEY not set")
        return {}

    client = StockHistoricalDataClient(api_key, secret_key)
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=lookback_days + 10)  # extra buffer for weekends

    result: dict[str, list] = {}

    # Process in batches of ALPACA_BATCH_SIZE
    batches = [symbols[i: i + ALPACA_BATCH_SIZE] for i in range(0, len(symbols), ALPACA_BATCH_SIZE)]
    for batch in batches:
        limiter.acquire()
        try:
            req = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=start_dt,
                end=end_dt,
                feed="iex",
            )
            bars_response = client.get_stock_bars(req)
            raw = bars_response.data if hasattr(bars_response, "data") else {}

            for sym, bar_list in raw.items():
                rows = []
                for bar in bar_list:
                    rows.append({
                        "o": float(bar.open),
                        "h": float(bar.high),
                        "l": float(bar.low),
                        "c": float(bar.close),
                        "v": float(bar.volume),
                        "t": str(bar.timestamp),
                    })
                # Ensure oldest→newest order
                rows.sort(key=lambda x: x["t"])
                if rows:
                    result[sym] = rows
        except Exception as exc:
            logger.warning("Alpaca batch fetch error (batch size=%d): %s", len(batch), exc)

    logger.info("_fetch_bars_for_symbols: got bars for %d/%d symbols", len(result), len(symbols))
    return result


# ---------------------------------------------------------------------------
# Strategy simulation helpers
# ---------------------------------------------------------------------------

def _detect_trigger(strategy_name: str, cfg: dict, closes: np.ndarray,
                    highs: np.ndarray, lows: np.ndarray, volumes: np.ndarray,
                    idx: int) -> bool:
    """Detect whether strategy triggers at bar index `idx`.

    Uses a window of bars ending at idx (inclusive).
    Requires at least 21 bars before idx.
    """
    if idx < 20:
        return False

    window_c = closes[idx - 20: idx + 1]  # 21 bars
    window_h = highs[idx - 20: idx + 1]
    window_l = lows[idx - 20: idx + 1]
    window_v = volumes[idx - 20: idx + 1]

    c_now = float(closes[idx])
    h_now = float(highs[idx])
    l_now = float(lows[idx])
    v_now = float(volumes[idx])
    c_prev = float(closes[idx - 1]) if idx >= 1 else c_now

    strategy_type = cfg["type"]

    # ---- momentum / breakout ----
    if strategy_type == "momentum":
        twenty_day_high = float(np.max(window_h[:-1]))  # exclude today
        avg_vol = float(np.mean(window_v[:-1]))
        volume_surge = v_now > 1.5 * avg_vol if avg_vol > 0 else False
        breakout = c_now > twenty_day_high
        return breakout and volume_surge

    # ---- reversal / rsi ----
    if strategy_type == "reversal":
        if strategy_name == "bollinger_bounce":
            mean_c = float(np.mean(window_c[:-1]))
            std_c = float(np.std(window_c[:-1]))
            lower_band = mean_c - 2 * std_c
            # Was below lower band yesterday, bouncing today
            prev_below = float(closes[idx - 1]) < lower_band
            bounce = c_now > float(closes[idx - 1])
            return prev_below and bounce
        # Generic reversal: near 20-day low and prior day was down
        twenty_day_low = float(np.min(window_l[:-1]))
        near_low = c_now <= twenty_day_low * 1.05  # within 5%
        prior_down = c_now > c_prev  # today is rebounding
        prev_day_down = c_prev < float(closes[idx - 2]) if idx >= 2 else False
        return near_low and prior_down and prev_day_down

    # ---- trend ----
    if strategy_type == "trend":
        sma10 = float(np.mean(closes[idx - 9: idx + 1]))
        sma20 = float(np.mean(closes[idx - 19: idx + 1])) if idx >= 19 else float(np.mean(closes[:idx + 1]))
        return c_now > sma10 > sma20

    return False


def _simulate_strategy(strategy_name: str, bars_data: dict[str, list]) -> dict:
    """Simulate one strategy across all symbols and compute performance stats.

    Returns:
        dict with keys: strategy_name, win_rate, avg_win_pct, avg_loss_pct,
        risk_reward, profit_factor, total_return_pct, trades_count
    """
    cfg = STRATEGY_CONFIGS.get(strategy_name)
    if cfg is None:
        logger.warning("Unknown strategy: %s", strategy_name)
        return _empty_result(strategy_name)

    hold_days: int = cfg["hold_days"]
    stop_pct: float = cfg["stop_pct"]
    target_mult: float = cfg["target_mult"]
    target_pct: float = stop_pct * target_mult

    returns: list[float] = []
    wins: list[float] = []
    losses: list[float] = []

    for sym, bars in bars_data.items():
        if len(bars) < 25:
            continue

        closes = np.array([b["c"] for b in bars], dtype=float)
        highs  = np.array([b["h"] for b in bars], dtype=float)
        lows   = np.array([b["l"] for b in bars], dtype=float)
        volumes = np.array([b["v"] for b in bars], dtype=float)
        n = len(bars)

        # Scan day -21 through day -2 (leave last 2 as "today")
        scan_end = n - 2
        scan_start = max(20, n - 21)

        for idx in range(scan_start, scan_end):
            if not _detect_trigger(strategy_name, cfg, closes, highs, lows, volumes, idx):
                continue

            # Entry: next day open approximated as close + 0.1%
            entry = float(closes[idx]) * 1.001
            stop_price = entry * (1 - stop_pct)
            target_price = entry * (1 + target_pct)

            # Exit window: up to hold_days after entry
            exit_start = idx + 1
            exit_end = min(idx + 1 + hold_days, n)

            if exit_start >= n:
                continue

            outcome = None
            for exit_idx in range(exit_start, exit_end):
                day_high = float(highs[exit_idx])
                day_low = float(lows[exit_idx])

                if day_high >= target_price:
                    outcome = ("win", target_pct)
                    break
                if day_low <= stop_price:
                    outcome = ("loss", -stop_pct)
                    break

            if outcome is None:
                # Held to end of window: use close of last day
                last_close = float(closes[exit_end - 1])
                ret = (last_close - entry) / entry
                outcome = ("neutral", ret)

            _, ret_pct = outcome
            returns.append(ret_pct)
            if ret_pct > 0:
                wins.append(ret_pct)
            elif ret_pct < 0:
                losses.append(ret_pct)

    trades_count = len(returns)
    if trades_count < MIN_TRADES:
        return _empty_result(strategy_name, trades_count)

    win_rate = len(wins) / trades_count
    avg_win_pct = float(np.mean(wins)) if wins else 0.0
    avg_loss_pct = float(np.mean(losses)) if losses else 0.0  # negative
    risk_reward = abs(avg_win_pct / avg_loss_pct) if avg_loss_pct != 0 else 0.0
    sum_wins = sum(wins) if wins else 0.0
    sum_losses = abs(sum(losses)) if losses else 0.0
    profit_factor = sum_wins / sum_losses if sum_losses > 0 else 0.0
    total_return_pct = float(np.sum(returns))

    return {
        "strategy_name": strategy_name,
        "win_rate": round(win_rate, 4),
        "avg_win_pct": round(avg_win_pct, 4),
        "avg_loss_pct": round(avg_loss_pct, 4),
        "risk_reward": round(risk_reward, 4),
        "profit_factor": round(profit_factor, 4),
        "total_return_pct": round(total_return_pct, 4),
        "trades_count": trades_count,
    }


def _empty_result(strategy_name: str, trades_count: int = 0) -> dict:
    return {
        "strategy_name": strategy_name,
        "win_rate": 0.0,
        "avg_win_pct": 0.0,
        "avg_loss_pct": 0.0,
        "risk_reward": 0.0,
        "profit_factor": 0.0,
        "total_return_pct": 0.0,
        "trades_count": trades_count,
    }


# ---------------------------------------------------------------------------
# Main rotation runner
# ---------------------------------------------------------------------------

def run_strategy_rotation(symbols: list[str] | None = None, force: bool = False) -> dict:
    """Run nightly strategy rotation: simulate all strategies, rank, and persist.

    Args:
        symbols: List of symbols to backtest against. If None, uses full universe
                 (from full_universe.get_universe()) or 100-name S&P fallback.
        force:   If True, re-run even if today's rotation already exists.

    Returns:
        dict with run_date, total_strategies, active_strategies, top_3, duration_seconds.
        On data unavailability, returns a message dict.
    """
    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    t0 = time.monotonic()

    # Check if already ran today
    if not force:
        try:
            with _conn() as c:
                row = c.execute(
                    "SELECT COUNT(*) as cnt FROM strategy_rotation WHERE run_date = ?",
                    (run_date,)
                ).fetchone()
                if row and row["cnt"] > 0:
                    logger.info("Strategy rotation already ran today (%s). Use force=True to re-run.", run_date)
                    return get_latest_rotation()
        except Exception as exc:
            logger.warning("Could not check existing rotation: %s", exc)

    # Resolve symbol universe
    if not symbols:
        try:
            from engine.full_universe import get_universe
            symbols = get_universe()
            logger.info("Using full_universe: %d symbols", len(symbols))
        except Exception as exc:
            logger.warning("full_universe unavailable (%s), using S&P 500 fallback", exc)
            symbols = list(_FALLBACK_SYMBOLS)

    # Cap at MAX_SYMBOLS for memory management
    if len(symbols) > MAX_SYMBOLS:
        symbols = symbols[:MAX_SYMBOLS]
        logger.info("Capped symbols at %d for memory management", MAX_SYMBOLS)

    # Fetch bars (single batch call for all symbols)
    logger.info("Fetching 30-day bars for %d symbols...", len(symbols))
    try:
        bars_data = _fetch_bars_for_symbols(symbols, lookback_days=LOOKBACK_DAYS)
    except Exception as exc:
        logger.error("Bar fetch failed: %s", exc)
        return {"message": "Strategy rotation requires market data — run after market hours"}

    if not bars_data:
        return {"message": "Strategy rotation requires market data — run after market hours"}

    logger.info("Bars available for %d symbols. Running strategy simulations...", len(bars_data))

    # Simulate each strategy sequentially (memory management)
    results: list[dict] = []
    for strategy_name in STRATEGY_CONFIGS:
        try:
            result = _simulate_strategy(strategy_name, bars_data)
            if result["trades_count"] >= MIN_TRADES:
                results.append(result)
                logger.debug(
                    "%-25s trades=%d  win_rate=%.2f  pf=%.2f",
                    strategy_name, result["trades_count"],
                    result["win_rate"], result["profit_factor"]
                )
            del result
        except Exception as exc:
            logger.warning("Simulation error for %s: %s", strategy_name, exc)
        gc.collect()

    # Free bars data
    del bars_data
    gc.collect()

    # Determine active strategies
    def _qualifies(r: dict) -> bool:
        return (
            r["win_rate"] >= ACTIVE_WIN_RATE
            and r["risk_reward"] >= ACTIVE_RISK_REWARD
            and r["profit_factor"] >= ACTIVE_PROFIT_FACTOR
            and r["trades_count"] >= MIN_TRADES
        )

    active_candidates = [r for r in results if _qualifies(r)]
    # Rank by profit_factor * win_rate descending
    active_candidates.sort(key=lambda r: r["profit_factor"] * r["win_rate"], reverse=True)
    active_set = {r["strategy_name"] for r in active_candidates[:MAX_ACTIVE]}

    # Assign ranks (rank only among active; inactive get None)
    rank_map: dict[str, int | None] = {}
    for i, r in enumerate(active_candidates[:MAX_ACTIVE], start=1):
        rank_map[r["strategy_name"]] = i

    # Persist all strategies
    try:
        rows_to_insert = []
        for r in results:
            is_active = 1 if r["strategy_name"] in active_set else 0
            rank = rank_map.get(r["strategy_name"])
            rows_to_insert.append((
                run_date,
                r["strategy_name"],
                r["win_rate"],
                r["avg_win_pct"],
                r["avg_loss_pct"],
                r["risk_reward"],
                r["profit_factor"],
                r["total_return_pct"],
                r["trades_count"],
                rank,
                is_active,
            ))

        with _conn() as c:
            c.executemany(
                """INSERT INTO strategy_rotation
                   (run_date, strategy_name, win_rate, avg_win_pct, avg_loss_pct,
                    risk_reward, profit_factor, total_return_pct, trades_count, rank, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows_to_insert,
            )
        logger.info("Persisted %d strategy results for %s", len(rows_to_insert), run_date)
    except Exception as exc:
        logger.error("Failed to persist rotation results: %s", exc)

    duration = round(time.monotonic() - t0, 1)
    top_3 = [r["strategy_name"] for r in active_candidates[:3]]

    return {
        "run_date": run_date,
        "total_strategies": len(results),
        "active_strategies": len(active_set),
        "top_3": top_3,
        "duration_seconds": duration,
    }


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_active_strategies() -> list[dict]:
    """Return the active strategies from the most recent rotation run.

    Falls back to returning all strategy names as active if no DB data exists.
    """
    try:
        with _conn() as c:
            latest_date = c.execute(
                "SELECT MAX(run_date) as d FROM strategy_rotation"
            ).fetchone()
            if not latest_date or not latest_date["d"]:
                raise ValueError("No rotation data")

            run_date = latest_date["d"]
            rows = c.execute(
                """SELECT strategy_name, win_rate, avg_win_pct, avg_loss_pct,
                          risk_reward, profit_factor, total_return_pct,
                          trades_count, rank, run_date
                   FROM strategy_rotation
                   WHERE run_date = ? AND is_active = 1
                   ORDER BY rank ASC""",
                (run_date,),
            ).fetchall()

            if rows:
                return [dict(r) for r in rows]
            raise ValueError("No active strategies in DB")

    except Exception as exc:
        logger.warning("get_active_strategies DB fallback: %s", exc)
        # Fallback: return all strategy names as active
        return [
            {
                "strategy_name": name,
                "win_rate": None,
                "avg_win_pct": None,
                "avg_loss_pct": None,
                "risk_reward": None,
                "profit_factor": None,
                "total_return_pct": None,
                "trades_count": None,
                "rank": i + 1,
                "run_date": None,
                "is_active": True,
            }
            for i, name in enumerate(STRATEGY_CONFIGS)
        ]


def get_rotation_history(days: int = 14) -> dict:
    """Read strategy_rotation for last N days.

    Returns:
        {strategies: [{name, avg_win_rate, days_active, last_rank, trend}], days_covered}
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        with _conn() as c:
            rows = c.execute(
                """SELECT strategy_name, run_date, win_rate, rank, is_active
                   FROM strategy_rotation
                   WHERE run_date >= ?
                   ORDER BY strategy_name, run_date ASC""",
                (cutoff,),
            ).fetchall()

        if not rows:
            return {"strategies": [], "days_covered": days}

        # Group by strategy
        by_strategy: dict[str, list] = {}
        for row in rows:
            name = row["strategy_name"]
            by_strategy.setdefault(name, []).append(dict(row))

        strategies_out = []
        for name, entries in by_strategy.items():
            active_entries = [e for e in entries if e["is_active"]]
            days_active = len(active_entries)
            win_rates = [e["win_rate"] for e in entries if e["win_rate"] is not None]
            avg_win_rate = round(float(np.mean(win_rates)), 4) if win_rates else None
            last_rank = entries[-1]["rank"] if entries else None

            # Trend: compare first half vs second half avg win_rate
            trend = "stable"
            if len(win_rates) >= 4:
                mid = len(win_rates) // 2
                first_half = float(np.mean(win_rates[:mid]))
                second_half = float(np.mean(win_rates[mid:]))
                if second_half > first_half + 0.02:
                    trend = "improving"
                elif second_half < first_half - 0.02:
                    trend = "declining"

            strategies_out.append({
                "name": name,
                "avg_win_rate": avg_win_rate,
                "days_active": days_active,
                "last_rank": last_rank,
                "trend": trend,
            })

        # Sort by days_active desc, then avg_win_rate desc
        strategies_out.sort(
            key=lambda x: (x["days_active"], x["avg_win_rate"] or 0),
            reverse=True,
        )

        return {"strategies": strategies_out, "days_covered": days}

    except Exception as exc:
        logger.error("get_rotation_history error: %s", exc)
        return {"strategies": [], "days_covered": days}


def get_latest_rotation() -> dict:
    """Return the most recent rotation run's full results.

    Returns:
        {run_date, active_strategies, inactive_strategies, stats}
    """
    try:
        with _conn() as c:
            latest = c.execute(
                "SELECT MAX(run_date) as d FROM strategy_rotation"
            ).fetchone()

            if not latest or not latest["d"]:
                return {"run_date": None, "active_strategies": [], "inactive_strategies": [], "stats": {"total": 0, "active_count": 0}}

            run_date = latest["d"]
            rows = c.execute(
                """SELECT strategy_name, win_rate, avg_win_pct, avg_loss_pct,
                          risk_reward, profit_factor, total_return_pct,
                          trades_count, rank, is_active
                   FROM strategy_rotation
                   WHERE run_date = ?
                   ORDER BY is_active DESC, rank ASC NULLS LAST""",
                (run_date,),
            ).fetchall()

        active = [dict(r) for r in rows if r["is_active"] == 1]
        inactive = [dict(r) for r in rows if r["is_active"] == 0]

        return {
            "run_date": run_date,
            "active_strategies": active,
            "inactive_strategies": inactive,
            "stats": {
                "total": len(rows),
                "active_count": len(active),
            },
        }

    except Exception as exc:
        logger.error("get_latest_rotation error: %s", exc)
        return {
            "run_date": None,
            "active_strategies": [],
            "inactive_strategies": [],
            "stats": {"total": 0, "active_count": 0},
        }


# ---------------------------------------------------------------------------
# Module initialization
# ---------------------------------------------------------------------------

_init_db()
