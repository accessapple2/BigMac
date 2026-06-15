#!/usr/bin/env python3
"""HM-STRATEGY-OUTCOME-TRACKER — per-strategy precision substrate (standalone, no trader impact).

Scores data/trader.db::strategy_signals against realized daily bars, mirroring SUPER_MAX W0
expectancy_engine: from entry, stop-first R over 3d/5d horizons (long BUY signals). Writes
strategy_outcomes so EVERY strategy (not just volume) gets a real precision number with n AND
ticker-breadth (so a single-ticker artifact like unusual_volume/NUKZ can't masquerade as edge).

Dedup: one outcome per (ticker, strategy_name, scan_date) — the scanner re-emits the same signal
every ~25 min, so we score the FIRST emit per strategy-ticker-day (min id), not all 1000s of repeats.
Maturity: a signal is scored only once >= horizon trading bars exist after its scan_date.
Idempotent: skips (signal_id, horizon) already scored. Read-only on strategy_signals; only writes
its own strategy_outcomes table. Standalone (cron) — never touches the trader.
"""
from __future__ import annotations
import os, sys, sqlite3, collections, datetime as dt

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(REPO); sys.path.insert(0, REPO)
DB = os.path.join(REPO, "data", "trader.db")
LOG = os.path.join(REPO, "logs", "strategy_outcome_tracker.log")
HORIZONS = (3, 5)
SINCE = os.environ.get("SOT_SINCE", "2026-06-02")  # backfill floor
BAR_DAYS = 30  # daily-bar lookback to cover SINCE..now + horizon
try:
    from dotenv import load_dotenv; load_dotenv(os.path.join(REPO, ".env"))
except Exception:
    pass


def log(m):
    line = f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {m}"
    print(line)
    try:
        open(LOG, "a").write(line + "\n")
    except Exception:
        pass


def ensure_table(c):
    c.execute("""CREATE TABLE IF NOT EXISTS strategy_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id INTEGER, strategy_name TEXT, ticker TEXT, scan_date TEXT,
        horizon_days INTEGER, entry REAL, stop REAL, target REAL,
        exit_R REAL, hit INTEGER, resolution TEXT,
        scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(signal_id, horizon_days))""")


def load_distinct_signals(c):
    """One signal per (ticker, strategy_name, scan_date): the first emit (min id)."""
    rows = c.execute(
        "SELECT MIN(id), ticker, strategy_name, scan_date, "
        "       entry_price, stop_price, target_price "
        "FROM strategy_signals "
        "WHERE signal_type='BUY' AND scan_date >= ? "
        "GROUP BY ticker, strategy_name, scan_date", (SINCE,)).fetchall()
    return rows


def fetch_bars(tickers):
    from engine.market_data import get_alpaca_bars
    out = {}
    uniq = sorted(set(tickers))
    for i in range(0, len(uniq), 40):
        chunk = uniq[i:i+40]
        try:
            d = get_alpaca_bars(chunk, days=BAR_DAYS)
            if isinstance(d, dict):
                out.update(d)
            elif len(chunk) == 1:
                out[chunk[0]] = d
        except Exception as e:
            log(f"bars fetch err {chunk[:3]}..: {type(e).__name__}")
    return out


def score_one(entry, stop, target, fwd):
    """Stop-first R over forward daily bars `fwd` (list of (high,low,close)). Long only.
    Returns (exit_R, hit, resolution) or None if malformed."""
    if entry is None or stop is None or target is None:
        return None
    if not (stop < entry < target):
        return None  # malformed / non-long bracket
    r_unit = entry - stop
    target_R = (target - entry) / r_unit
    for hi, lo, _cl in fwd:
        stop_hit = lo is not None and lo <= stop
        tgt_hit = hi is not None and hi >= target
        if stop_hit:                       # stop-first convention if same bar
            return (-1.0, 0, "stop")
        if tgt_hit:
            return (round(target_R, 3), 1, "target")
    last_close = fwd[-1][2]
    return (round((last_close - entry) / r_unit, 3), 0, "mtm_close")


def run():
    c = sqlite3.connect(DB, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    ensure_table(c)
    done = {(r[0], r[1]) for r in c.execute("SELECT signal_id, horizon_days FROM strategy_outcomes")}
    sigs = load_distinct_signals(c)
    log(f"distinct signals since {SINCE}: {len(sigs)}; already scored pairs: {len(done)}")
    bars = fetch_bars([s[1] for s in sigs])

    new = 0
    for sid, ticker, strat, scan_date, entry, stop, target in sigs:
        df = bars.get(ticker)
        if df is None or getattr(df, "empty", True):
            continue
        try:
            sd = dt.date.fromisoformat(scan_date)
        except Exception:
            continue
        fwd_all = [(float(r.High), float(r.Low), float(r.Close))
                   for idx, r in df.iterrows() if idx.date() > sd]
        for h in HORIZONS:
            if (sid, h) in done:
                continue
            if len(fwd_all) < h:
                continue  # not matured
            res = score_one(entry, stop, target, fwd_all[:h])
            if res is None:
                continue
            exit_R, hit, resolution = res
            c.execute(
                "INSERT OR IGNORE INTO strategy_outcomes "
                "(signal_id,strategy_name,ticker,scan_date,horizon_days,entry,stop,target,exit_R,hit,resolution) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (sid, strat, ticker, scan_date, h, entry, stop, target, exit_R, hit, resolution))
            new += 1
    c.commit()
    log(f"wrote {new} new outcome rows")
    report(c)
    c.close()


def report(c):
    log("=== per-strategy precision (horizon=5d) ===")
    rows = c.execute(
        "SELECT strategy_name, COUNT(*) n, "
        "       ROUND(100.0*SUM(hit)/COUNT(*),1) hit_pct, ROUND(AVG(exit_R),3) avg_R, "
        "       COUNT(DISTINCT ticker) breadth "
        "FROM strategy_outcomes WHERE horizon_days=5 "
        "GROUP BY strategy_name ORDER BY n DESC").fetchall()
    for strat, n, hit_pct, avg_R, breadth in rows:
        # top-ticker concentration guard
        top = c.execute(
            "SELECT ticker, COUNT(*) k FROM strategy_outcomes "
            "WHERE horizon_days=5 AND strategy_name=? GROUP BY ticker ORDER BY k DESC LIMIT 1",
            (strat,)).fetchone()
        top_share = round(100.0*top[1]/n, 0) if n else 0
        flag = "  <-- LOW BREADTH" if breadth <= 2 or top_share >= 60 else ""
        log(f"  {strat:22s} n={n:4d} hit={hit_pct}% avgR={avg_R} breadth={breadth} "
            f"top={top[0]}({top_share:.0f}%){flag}")


if __name__ == "__main__":
    run()
