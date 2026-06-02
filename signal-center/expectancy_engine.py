"""expectancy_engine.py — Expectancy & R-Multiple Scoring Engine (Wave 0).

Runs inside the signal-center (Flask, Python 3.9). Self-contained:
  - Optional[...] only, no PEP 604.
  - Polygon daily-OHLCV fetch via plain `requests` (no trader-engine import).
  - Reuses engine.source_gate (provenance gate) + engine.market_calendar
    (trading-day math) — both 3.9-safe.

Canonical substrate (resolved at source 2026-05-31):
  signal_outcomes (1:1, signal_id) -> trade_signals  — NOT predictions.
  Direction = trade_signals.action: BUY/BUY_CALL=long, SELL=short,
              WATCH=non_directional (excluded from R).
  Levels    = trade_signals.entry_price / stop_loss / take_profit (100% present).
  Setup tag = normalized setup token from sources_json (whitelist), agent fallback.

Outputs to scored_predictions (NEVER mutates source rows). Per (signal_id,
horizon) with horizon in {1,3,5,10} trading days.

Horizon completeness: a signal needs N forward trading-day bars to be CLOSED at
horizon N. Immature windows are marked OPEN and EXCLUDED from the closed-sample
count — never scored as a stop or a loss (keeps the >=20 gate honest per horizon).
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# repo root on path so `engine.*` imports resolve from signal-center/
_SC_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SC_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from engine import source_gate  # noqa: E402
from engine import market_calendar as mc  # noqa: E402

def _load_env() -> None:
    """Idempotent .env loader (server.py already does this on its import path;
    needed for standalone CLI runs). setdefault never clobbers a real env var."""
    env_path = os.path.join(_ROOT, ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path) as ef:
            for line in ef:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
    except Exception:
        pass


_load_env()

DB_PATH = os.path.join(_SC_DIR, "signals.db")
HORIZONS = [1, 3, 5, 10]
OOS_TRADING_DAYS = 30

POLYGON_BASE = "https://api.polygon.io"

# ── Setup-tag taxonomy (Captain directive: setup vs data-source) ───────────
# Genuine entry setups / factors. Everything else in sources_json is a data
# provider (yahoo_finance, finnhub_news, fred, rss_news, danelfin_newsletter,
# yfinance_options), an analysis dimension dumped by a multi-factor agent
# (the ~93-count cluster: sentiment, s_r_levels, regime, multi_tf, fundamentals,
# flow_lean, analyst_ratings, trade_memory, discovery, whisper), or an agent
# identifier (tractor_beam, morning_briefing, ...). Those are NOT setups.
SETUP_TAGS = {
    "relative_strength", "sector_leader", "premarket_gap", "bull_flag",
    "first_green_day", "volume_surge", "squeeze_candidate", "vwap_reclaim",
    "vol_breakout", "gap", "imbalance", "gex", "re_entry", "impulse",
}


def _norm(tok: Any) -> str:
    return str(tok).strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")


def classify_setup_tag(sources_json: Optional[str], agent_name: Optional[str]) -> Tuple[str, bool]:
    """Return (setup_tag, is_real_setup). Scans sources_json for a whitelisted
    setup token; falls back to agent_name when none present."""
    if sources_json:
        try:
            arr = json.loads(sources_json)
            if isinstance(arr, list):
                for tok in arr:
                    n = _norm(tok)
                    if n in SETUP_TAGS:
                        return n, True
        except Exception:
            pass
    return ("agent:" + (agent_name or "unknown")), False


def direction_of(action: Optional[str]) -> str:
    a = (action or "").upper()
    if a in ("BUY", "BUY_CALL"):
        return "long"
    if a in ("SELL", "BUY_PUT", "SHORT"):
        return "short"
    return "non_directional"  # WATCH and anything else


# ── DB ──────────────────────────────────────────────────────────────────
def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_columns() -> None:
    """Idempotent: add is_complete column if a prior migration predates it."""
    conn = _db()
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(scored_predictions)").fetchall()}
        if "is_complete" not in cols:
            conn.execute("ALTER TABLE scored_predictions ADD COLUMN is_complete INTEGER DEFAULT 0")
            conn.commit()
    finally:
        conn.close()


# ── Polygon daily backfill -> daily_bars ───────────────────────────────────
def _polygon_key() -> Optional[str]:
    return os.environ.get("POLYGON_API_KEY") or None


def fetch_polygon_daily(symbol: str, start: str, end: str) -> List[Dict[str, Any]]:
    """One range-aggregate call -> list of {date,open,high,low,close,volume}."""
    key = _polygon_key()
    if not key:
        return []
    url = "%s/v2/aggs/ticker/%s/range/1/day/%s/%s" % (POLYGON_BASE, symbol, start, end)
    try:
        r = requests.get(url, params={"adjusted": "true", "sort": "asc",
                                      "limit": 5000, "apiKey": key}, timeout=20)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []
    out = []
    for b in (data.get("results") or []):
        ts_ms = b.get("t", 0)
        d = datetime.utcfromtimestamp(ts_ms / 1000).date().isoformat()
        out.append({"date": d, "open": b.get("o"), "high": b.get("h"),
                    "low": b.get("l"), "close": b.get("c"), "volume": b.get("v")})
    return out


def backfill_daily_bars(symbols: Optional[List[str]] = None,
                        start: Optional[str] = None,
                        end: Optional[str] = None,
                        sleep_s: float = 0.0,
                        verbose: bool = True) -> Dict[str, Any]:
    """Fetch daily OHLCV for substrate symbols and cache to daily_bars.
    Default window covers the full backlog + 10-day horizon buffer."""
    conn = _db()
    try:
        if symbols is None:
            symbols = [r["symbol"] for r in conn.execute(
                "SELECT DISTINCT symbol FROM trade_signals ORDER BY symbol"
            ).fetchall()]
        if start is None:
            mn = conn.execute("SELECT min(date(created_at)) FROM trade_signals").fetchone()[0]
            start = mn or "2026-04-01"
        if end is None:
            end = date.today().isoformat()
        ok, empty, total_rows = 0, 0, 0
        for i, sym in enumerate(symbols):
            bars = fetch_polygon_daily(sym, start, end)
            if not bars:
                empty += 1
            else:
                ok += 1
                for b in bars:
                    conn.execute(
                        "INSERT OR REPLACE INTO daily_bars "
                        "(symbol,date,open,high,low,close,volume,source,fetched_at) "
                        "VALUES (?,?,?,?,?,?,?, 'polygon', ?)",
                        (sym, b["date"], b["open"], b["high"], b["low"],
                         b["close"], b["volume"],
                         datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')))  # HM-TZ-COMPLETION 2026-06-02: was local .isoformat() (daily_bars.fetched_at, provenance-only, no UTC reader — canonicalized for consistency)
                    total_rows += 1
                conn.commit()
            if verbose and (i + 1) % 25 == 0:
                print("  backfill %d/%d (ok=%d empty=%d rows=%d)" % (i + 1, len(symbols), ok, empty, total_rows))
            if sleep_s:
                time.sleep(sleep_s)
        return {"symbols": len(symbols), "ok": ok, "empty": empty, "rows_written": total_rows,
                "start": start, "end": end}
    finally:
        conn.close()


# ── Trading-day math (market_calendar) ─────────────────────────────────────
def _is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and not mc.is_us_market_holiday(d)


def oos_cutoff_date(today: Optional[date] = None, n: int = OOS_TRADING_DAYS) -> date:
    """Date n trading days before today (inclusive walk back)."""
    d = today or date.today()
    count = 0
    while count < n:
        d = d - timedelta(days=1)
        if _is_trading_day(d):
            count += 1
    return d


# ── Stop-first path-aware R (the core) ─────────────────────────────────────
def stop_first_r(direction: str, entry: float, stop: float, target: float,
                 hi: float, lo: float, close: float) -> Tuple[str, float]:
    """Return (outcome_label, r_multiple). Stop-first, magnitude-aware.
    Assumes risk = abs(entry-stop) > 0 (validated by caller)."""
    risk = abs(entry - stop)
    if direction == "long":
        if lo <= stop:
            return "STOP", -1.0
        if target and hi >= target:
            return "TP", (target - entry) / risk
        return "OPEN", (close - entry) / risk
    # short
    if hi >= stop:
        return "STOP", -1.0
    if target and lo <= target:
        return "TP", (entry - target) / risk
    return "OPEN", (entry - close) / risk


# ── Backlog scoring ────────────────────────────────────────────────────────
def _load_bars_by_symbol(conn: sqlite3.Connection) -> Dict[str, List[sqlite3.Row]]:
    bars: Dict[str, List[sqlite3.Row]] = {}
    for r in conn.execute("SELECT symbol,date,high,low,close FROM daily_bars ORDER BY symbol,date"):
        bars.setdefault(r["symbol"], []).append(r)
    return bars


def score_backlog() -> Dict[str, Any]:
    """Re-score the full substrate into scored_predictions. Idempotent
    (INSERT OR REPLACE on (signal_id,horizon))."""
    ensure_columns()
    conn = _db()
    try:
        bars_by_sym = _load_bars_by_symbol(conn)
        rows = conn.execute("""
            SELECT so.signal_id AS signal_id,
                   so.tracked_high AS t_high, so.tracked_low AS t_low,
                   so.tracked_current AS t_cur, so.tracked_entry AS t_entry,
                   ts.symbol AS symbol, ts.action AS action,
                   ts.entry_price AS entry, ts.stop_loss AS stop,
                   ts.take_profit AS target, ts.agent_name AS agent_name,
                   ts.sources_json AS sources_json, ts.created_at AS created_at
            FROM signal_outcomes so
            JOIN trade_signals ts ON ts.id = so.signal_id
        """).fetchall()

        cutoff = oos_cutoff_date()
        stats = {"signals": len(rows), "scored_rows": 0,
                 "unscoreable": {"non_directional": 0, "no_stop": 0,
                                 "missing_levels": 0, "no_bars": 0, "stale_gated": 0,
                                 "direction_level_mismatch": 0}}

        for r in rows:
            sid = r["signal_id"]
            symbol = r["symbol"]
            action = r["action"]
            direction = direction_of(action)
            setup_tag, _is_setup = classify_setup_tag(r["sources_json"], r["agent_name"])
            agent = r["agent_name"] or "unknown"
            entry = r["entry"]
            stop = r["stop"]
            target = r["target"]
            try:
                entry_date = (r["created_at"] or "")[:10]
            except Exception:
                entry_date = ""
            is_oos = 1 if (entry_date and entry_date >= cutoff.isoformat()) else 0

            # provenance gate (backlog): all substrate signals -> 'signals' feed.
            stale_gated = 1 if source_gate.is_quarantined("signals") else 0

            # determine a single unscoreable reason (applies to all horizons)
            reason = None
            if stale_gated:
                reason = "stale_gated"
            elif direction == "non_directional":
                reason = "non_directional"
            elif entry is None or not entry:
                reason = "missing_levels"
            elif stop is None or not stop or abs(entry - stop) == 0:
                reason = "no_stop"
            elif direction == "long" and not (stop < entry):
                # incoherent long: stop must sit below entry
                reason = "direction_level_mismatch"
            elif direction == "short" and not (stop > entry):
                # incoherent short: stop must sit above entry. ALL 88 'SELL'
                # signals carry long-style geometry (stop<entry, tp>entry) —
                # they are sell-to-close/exit signals, not new shorts. Excluded
                # rather than fabricating an insta-stop -1R (measurement contamination).
                reason = "direction_level_mismatch"

            forward = []
            if reason is None:
                allbars = bars_by_sym.get(symbol, [])
                if not allbars:
                    reason = "no_bars"
                else:
                    forward = [b for b in allbars if b["date"] > entry_date]

            if reason is not None:
                # count once (per signal, not per horizon)
                stats["unscoreable"][reason] = stats["unscoreable"].get(reason, 0) + 1
                for h in HORIZONS:
                    conn.execute(_UPSERT, (
                        sid, h, symbol, entry_date, action, direction, setup_tag, agent,
                        entry, stop, target, (abs(entry - stop) if (entry and stop) else None),
                        None, None, None, "UNSCOREABLE", None, None,
                        0, 0, reason, stale_gated, is_oos, 0))
                continue

            risk = abs(entry - stop)
            # realized cross-check: cumulative path-aware over the FULL lifetime
            realized_outcome, realized_r = stop_first_r(
                direction, entry, stop, target,
                r["t_high"] or entry, r["t_low"] or entry, r["t_cur"] or entry)

            for h in HORIZONS:
                window = forward[:h]
                complete = 1 if len(forward) >= h else 0
                if not window:
                    # immature, no forward bars yet -> OPEN at entry, not closed
                    conn.execute(_UPSERT, (
                        sid, h, symbol, entry_date, action, direction, setup_tag, agent,
                        entry, stop, target, risk, None, None, None,
                        "OPEN", 0.0, round(realized_r, 4), 0, 0, None, 0, is_oos, 0))
                    continue
                hi = max(b["high"] for b in window)
                lo = min(b["low"] for b in window)
                close = window[-1]["close"]
                if complete:
                    outcome, rmult = stop_first_r(direction, entry, stop, target, hi, lo, close)
                    closed = 1
                else:
                    # immature: never a stop/loss; mark-to-market OPEN, excluded from closed
                    rmult = (close - entry) / risk if direction == "long" else (entry - close) / risk
                    outcome = "OPEN"
                    closed = 0
                conn.execute(_UPSERT, (
                    sid, h, symbol, entry_date, action, direction, setup_tag, agent,
                    entry, stop, target, risk, round(hi, 4), round(lo, 4), round(close, 4),
                    outcome, round(rmult, 4), round(realized_r, 4),
                    closed, 1, None, 0, is_oos, complete))
                stats["scored_rows"] += 1
        conn.commit()
        return stats
    finally:
        conn.close()


_UPSERT = """
INSERT OR REPLACE INTO scored_predictions
 (signal_id, horizon_days, symbol, entry_date, action, direction, setup_tag, agent_name,
  entry, stop, target, risk, window_high, window_low, window_close,
  outcome_v2, r_multiple, realized_r, closed, scoreable, unscoreable_reason,
  stale_gated, is_oos, is_complete)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


# ── Aggregation / leaderboard ──────────────────────────────────────────────
_GROUP_COL = {"action": "action", "recommendation": "action",
              "setup_tag": "setup_tag", "agent": "agent_name", "agent_name": "agent_name"}


def expectancy(group: str = "action", horizon: int = 5,
               sample_min: int = 20, oos: Optional[str] = None) -> Dict[str, Any]:
    """Per-group expectancy at a horizon. Only CLOSED (mature, scoreable)
    directional rows count. INSUFFICIENT SAMPLE pinned to the bottom."""
    col = _GROUP_COL.get(group, "action")
    conn = _db()
    try:
        where = ["horizon_days=?", "scoreable=1", "closed=1", "direction IN ('long','short')"]
        params: List[Any] = [horizon]
        if oos == "is":
            where.append("is_oos=0")
        elif oos == "oos":
            where.append("is_oos=1")
        sql = ("SELECT %s AS grp, r_multiple, is_oos FROM scored_predictions WHERE %s"
               % (col, " AND ".join(where)))
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    groups: Dict[str, List[float]] = {}
    for r in rows:
        groups.setdefault(r["grp"], []).append(r["r_multiple"])

    out = []
    for grp, rs in groups.items():
        closed = len(rs)
        wins = [x for x in rs if x > 0]
        losses = [x for x in rs if x <= 0]
        win_rate = len(wins) / closed if closed else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        expectancy_r = win_rate * avg_win + (1 - win_rate) * avg_loss
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = (gross_win / gross_loss) if gross_loss else None
        sample_ok = closed >= sample_min and expectancy_r > 0
        out.append({
            "group": grp, "horizon": horizon, "closed": closed,
            "win_rate": round(win_rate, 4), "avg_win_R": round(avg_win, 4),
            "avg_loss_R": round(avg_loss, 4), "expectancy_R": round(expectancy_r, 4),
            "profit_factor": (round(profit_factor, 3) if profit_factor is not None else None),
            "sample_ok": bool(closed >= sample_min),
            "status": ("EDGE" if sample_ok else
                       ("NO EDGE" if closed >= sample_min else "INSUFFICIENT SAMPLE")),
        })
    # rank: sufficient-sample first by expectancy desc, INSUFFICIENT pinned bottom
    out.sort(key=lambda x: (0 if x["sample_ok"] else 1,
                            -x["expectancy_R"] if x["sample_ok"] else x["group"]))
    return {"group_by": group, "horizon": horizon, "sample_min": sample_min,
            "rows": out}


def equity_curve(group_value: str, horizon: int) -> Dict[str, Any]:
    """Cumulative R + max drawdown for a SPECIFIC group value (e.g. 'long' or
    'premarket_gap'). Matches the value against any axis column."""
    conn = _db()
    try:
        # group_value may match any axis column; try them in order
        rows = []
        for col in ("setup_tag", "agent_name", "action"):
            rows = conn.execute(
                "SELECT entry_date, r_multiple FROM scored_predictions "
                "WHERE %s=? AND horizon_days=? AND scoreable=1 AND closed=1 "
                "ORDER BY entry_date, signal_id" % col,
                (group_value, horizon)).fetchall()
            if rows:
                break
    finally:
        conn.close()
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    curve = []
    for r in rows:
        cum += r["r_multiple"]
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)
        curve.append({"date": r["entry_date"], "cum_R": round(cum, 4)})
    return {"group": group_value, "horizon": horizon, "n": len(rows),
            "final_cum_R": round(cum, 4), "max_drawdown_R": round(max_dd, 4),
            "curve": curve}


def unscoreable_counts() -> Dict[str, Any]:
    conn = _db()
    try:
        # per-signal reasons (use horizon=1 slice to avoid 4x counting)
        rows = conn.execute(
            "SELECT unscoreable_reason AS reason, COUNT(*) AS n FROM scored_predictions "
            "WHERE horizon_days=1 AND unscoreable_reason IS NOT NULL GROUP BY reason"
        ).fetchall()
        total_signals = conn.execute(
            "SELECT COUNT(DISTINCT signal_id) FROM scored_predictions").fetchone()[0]
        scoreable_signals = conn.execute(
            "SELECT COUNT(DISTINCT signal_id) FROM scored_predictions "
            "WHERE horizon_days=1 AND scoreable=1").fetchone()[0]
    finally:
        conn.close()
    return {"total_signals": total_signals, "scoreable_signals": scoreable_signals,
            "unscoreable": {r["reason"]: r["n"] for r in rows}}


def taxonomy_report() -> Dict[str, Any]:
    """Report the derived setup-vs-data-source tag classification over the substrate."""
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT sources_json, agent_name FROM trade_signals WHERE json_valid(sources_json)"
        ).fetchall()
    finally:
        conn.close()
    setup_counts: Dict[str, int] = {}
    excluded_counts: Dict[str, int] = {}
    fallback = 0
    for r in rows:
        tag, is_setup = classify_setup_tag(r["sources_json"], r["agent_name"])
        if is_setup:
            setup_counts[tag] = setup_counts.get(tag, 0) + 1
        else:
            fallback += 1
        try:
            for tok in json.loads(r["sources_json"]):
                n = _norm(tok)
                if n not in SETUP_TAGS:
                    excluded_counts[n] = excluded_counts.get(n, 0) + 1
        except Exception:
            pass
    return {"setup_tags_used": dict(sorted(setup_counts.items(), key=lambda kv: -kv[1])),
            "fallback_to_agent_signals": fallback,
            "excluded_as_data_or_dimension": dict(sorted(excluded_counts.items(), key=lambda kv: -kv[1]))}


def rebuild_all(do_backfill: bool = True) -> Dict[str, Any]:
    res = {}
    if do_backfill:
        res["backfill"] = backfill_daily_bars()
    res["score"] = score_backlog()
    res["unscoreable"] = unscoreable_counts()
    return res


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--score", action="store_true")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    if args.all:
        print(json.dumps(rebuild_all(True), indent=2, default=str))
    else:
        if args.backfill:
            print(json.dumps(backfill_daily_bars(), indent=2, default=str))
        if args.score:
            print(json.dumps(score_backlog(), indent=2, default=str))
