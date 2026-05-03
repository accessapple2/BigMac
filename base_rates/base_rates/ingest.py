"""Backfill OHLCV → compute features → write to signals.db.

Resumable: skips symbols already up-to-date in base_rate_ingest_log.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from .buckets import assign_buckets, DEFAULT_BUCKETS
from .features import compute_features
from .migrate import migrate

# yfinance is heavy; import lazily so tests/imports stay fast
def _yf():
    import yfinance as yf
    return yf


DEFAULT_LOOKBACK_YEARS = 20
DEFAULT_DB = "signals.db"
DEFAULT_FORWARD_DAYS = 5


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance returns capitalized columns and sometimes MultiIndex; normalize."""
    if df.empty:
        return df
    df = df.copy()
    # Flatten MultiIndex columns FIRST (newer yfinance returns this for single symbols)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    # Then lowercase everything
    df.columns = [str(c).lower() for c in df.columns]
    # Some indices (^VIX) may not report volume
    if "volume" not in df.columns:
        df["volume"] = 0
    return df[["open", "high", "low", "close", "volume"]]


def _download(symbol: str, start: str, end: str, max_retries: int = 3) -> pd.DataFrame:
    yf = _yf()
    last_err = None
    for attempt in range(max_retries):
        try:
            df = yf.download(
                symbol,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            return _normalize_ohlcv(df)
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"yfinance failed for {symbol}: {last_err}")


def _row_to_buckets(row: pd.Series) -> dict[str, int] | None:
    """Compute bucket vector. Returns None if any required field is NaN."""
    required = ["pct_change", "rsi14", "rsi_slope", "vix_close", "vix_pct_change", "spy_above_200"]
    for f in required:
        v = row.get(f)
        if v is None or (isinstance(v, float) and v != v) or pd.isna(v):
            return None
    return assign_buckets(
        pct_change=row["pct_change"],
        rsi14=row["rsi14"],
        rsi_slope=row["rsi_slope"],
        vix_close=row["vix_close"],
        vix_pct_change=row["vix_pct_change"],
        spy_above_200=int(row["spy_above_200"]),
    )


def _last_ingested(conn: sqlite3.Connection, symbol: str) -> str | None:
    cur = conn.execute(
        "SELECT last_date FROM base_rate_ingest_log WHERE symbol = ?",
        (symbol,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _write_log(conn: sqlite3.Connection, symbol: str, last_date: str, rows: int) -> None:
    conn.execute(
        """INSERT INTO base_rate_ingest_log(symbol, last_date, rows, updated)
           VALUES (?, ?, ?, datetime('now'))
           ON CONFLICT(symbol) DO UPDATE SET
             last_date=excluded.last_date,
             rows=excluded.rows,
             updated=datetime('now')""",
        (symbol, last_date, rows),
    )


def ingest_symbol(
    symbol: str,
    db_path: str,
    vix_df: pd.DataFrame,
    spy_df: pd.DataFrame,
    start: str,
    end: str,
    forward_days: int = DEFAULT_FORWARD_DAYS,
    skip_if_current: bool = True,
) -> int:
    """Ingest one symbol. Returns rows written."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        if skip_if_current:
            last = _last_ingested(conn, symbol)
            # If we've ingested up to within 5 days of `end`, skip
            if last:
                last_dt = datetime.fromisoformat(last).date()
                end_dt = datetime.fromisoformat(end).date()
                if (end_dt - last_dt).days < forward_days:
                    print(f"  [{symbol}] up to date (last={last}), skipping")
                    return 0

        df = _download(symbol, start, end)
        if df.empty or len(df) < 250:
            print(f"  [{symbol}] insufficient history ({len(df)} rows), skipping")
            return 0

        feats = compute_features(df, vix_df, spy_df, forward_days=forward_days)

        # Drop rows with no bucket vector (early history, missing VIX/SPY align, etc.)
        rows_to_write = []
        for date_idx, row in feats.iterrows():
            buckets = _row_to_buckets(row)
            if buckets is None:
                continue
            rows_to_write.append((
                symbol,
                date_idx.strftime("%Y-%m-%d"),
                _f(row["close"]),
                _f(row["pct_change"]),
                _f(row["rsi14"]),
                _f(row["rsi_slope"]),
                _f(row["vix_close"]),
                _f(row["vix_pct_change"]),
                int(row["spy_above_200"]) if not pd.isna(row["spy_above_200"]) else None,
                _f(row["fwd_return"]),  # may be NaN at the tail
                _f(row["fwd_maxdd"]),
                buckets["move_intensity"],
                buckets["rsi_zone"],
                buckets["rsi_slope"],
                buckets["vix_level"],
                buckets["vix_move"],
                buckets["market_trend"],
            ))

        if not rows_to_write:
            print(f"  [{symbol}] no valid rows after feature computation")
            return 0

        conn.executemany(
            """INSERT OR REPLACE INTO base_rate_features
               (symbol, date, close, pct_change, rsi14, rsi_slope,
                vix_close, vix_pct_change, spy_above_200,
                fwd_5d_return, fwd_5d_maxdd,
                b_move, b_rsi, b_rsi_slope, b_vix, b_vix_move, b_trend)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows_to_write,
        )
        last_date = rows_to_write[-1][1]
        _write_log(conn, symbol, last_date, len(rows_to_write))
        conn.commit()
        print(f"  [{symbol}] wrote {len(rows_to_write)} rows (through {last_date})")
        return len(rows_to_write)


def _f(v):
    """Convert to float or None for SQLite. Handles NaN/pd.NA."""
    if v is None:
        return None
    if pd.isna(v):
        return None
    return float(v)


def load_universe(path: str | Path) -> list[str]:
    """Read newline-separated tickers from a file. Strips comments and blanks."""
    out = []
    with open(path) as f:
        for line in f:
            t = line.strip().split("#", 1)[0].strip().upper()
            if t:
                out.append(t)
    return out


def ingest_universe(
    universe: list[str],
    db_path: str = DEFAULT_DB,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    forward_days: int = DEFAULT_FORWARD_DAYS,
    sleep_between: float = 0.2,
) -> None:
    """Ingest a list of tickers. Pulls VIX and SPY once."""
    migrate(db_path)

    end = datetime.now().date()
    start = end - timedelta(days=int(lookback_years * 365.25))
    start_s, end_s = start.isoformat(), end.isoformat()

    print(f"[ingest] window: {start_s} → {end_s}")
    print(f"[ingest] universe: {len(universe)} symbols")
    print(f"[ingest] db: {db_path}")

    print("[ingest] downloading ^VIX ...")
    vix = _download("^VIX", start_s, end_s)
    if vix.empty:
        sys.exit("FATAL: failed to download ^VIX")

    print("[ingest] downloading SPY ...")
    spy = _download("SPY", start_s, end_s)
    if spy.empty:
        sys.exit("FATAL: failed to download SPY")

    total_rows = 0
    failed = []
    for i, sym in enumerate(universe, 1):
        print(f"[{i}/{len(universe)}] {sym}")
        try:
            n = ingest_symbol(sym, db_path, vix, spy, start_s, end_s, forward_days=forward_days)
            total_rows += n
        except Exception as e:
            print(f"  [{sym}] ERROR: {e}")
            failed.append(sym)
        time.sleep(sleep_between)

    print(f"\n[ingest] done. wrote {total_rows} rows. failures: {len(failed)}")
    if failed:
        print(f"[ingest] failed symbols: {', '.join(failed)}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--universe", required=True, help="Path to ticker list (one per line)")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--years", type=int, default=DEFAULT_LOOKBACK_YEARS)
    p.add_argument("--forward-days", type=int, default=DEFAULT_FORWARD_DAYS)
    p.add_argument("--sleep", type=float, default=0.2, help="Sleep between symbols (sec)")
    args = p.parse_args()

    tickers = load_universe(args.universe)
    ingest_universe(
        tickers,
        db_path=args.db,
        lookback_years=args.years,
        forward_days=args.forward_days,
        sleep_between=args.sleep,
    )


if __name__ == "__main__":
    main()
