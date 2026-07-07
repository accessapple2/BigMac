#!/usr/bin/env python3
"""scripts/counterfactual_report.py — P1 measurement layer, 2026-07-07.

For every REJECTED/SKIPPED signal in the last 30 days (deduped), compute
hypothetical forward return (1d/3d/5d) from the signal's captured price
using real Alpaca daily bars (same data source + close-to-close convention
as engine.signal_evaluator._fetch_realized_return, reused/adapted here
with per-ticker caching since a naive per-row call against ~900 distinct
tickers would be needlessly slow and rate-limit-risky).

Groups by gate_name (from gate_reject_log). Output: per-gate table --
signals blocked, avg forward return blocked, vs. the average realized
return of EXECUTED signals over the same period (trades table). This is
the evidence base for "which of the ~15 gates earn their keep" -- READ-
ONLY, makes no gating decisions itself.

Dedup: gate_reject_log has zero dedup at write time (same root cause as
the direct_buy_intent signal storm, commit 1311da3) -- a single blocked
attempt can log dozens of near-identical rows across scan cycles. Deduped
here by (player_id, symbol, gate_name, date(ts)) -- one row per
agent/symbol/gate/day, matching the dedup key already established for
signals_v2.

Usage:
    python3 scripts/counterfactual_report.py            # full run, writes DB + prints
    python3 scripts/counterfactual_report.py --dry-run   # print only

Cron (weekly, 20:00 slot family):
    0 20 * * 0 cd ~/autonomous-trader && .venv/bin/python3 scripts/counterfactual_report.py >> logs/counterfactual_report.log 2>&1
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DB_PATH = ROOT / "data" / "trader.db"
ALPACA_BARS = "https://data.alpaca.markets/v2/stocks/{sym}/bars"

LOOKBACK_DAYS = 30
FWD_HORIZONS = (1, 3, 5)  # trading-day-ish horizons, resolved via nearest available bar

# Gates that are structural (halt state, market hours) rather than
# discretionary risk/quality decisions -- included in the table for
# completeness but flagged separately, since "does this gate earn its
# keep" isn't a meaningful question for them (an agent that's halted or a
# market that's closed isn't a tunable decision).
STRUCTURAL_GATES = {"HALT", "MARKET_CLOSED"}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH), timeout=15)
    c.row_factory = sqlite3.Row
    return c


def _fetch_daily_closes(symbol: str, start: str, end: str) -> dict[str, float]:
    """Fetch a WIDE daily-bar window once per symbol (not per signal) and
    return a {date: close} map. Mirrors engine.signal_evaluator's Alpaca
    bars convention (IEX feed, 1Day timeframe) for consistency with the
    existing evaluator's realized-return numbers."""
    try:
        import requests
        key = os.environ.get("APCA_API_KEY_ID", "")
        secret = os.environ.get("APCA_API_SECRET_KEY", "")
        if not key or not secret:
            return {}
        r = requests.get(
            ALPACA_BARS.format(sym=symbol),
            headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret},
            params={
                "timeframe": "1Day", "start": start, "end": end,
                "feed": "iex", "sort": "asc", "limit": 10000,
            },
            timeout=8,
        )
        if not r.ok:
            return {}
        bars = r.json().get("bars") or []
        return {(b.get("t") or "")[:10]: float(b["c"]) for b in bars if b.get("t")}
    except Exception:
        return {}


def _closest_close_on_or_after(closes: dict[str, float], target_date: str) -> float | None:
    """Nearest available bar close on/after target_date (handles weekends/
    holidays the same way as engine.signal_evaluator's fallback logic)."""
    candidates = sorted(d for d in closes if d >= target_date)
    return closes[candidates[0]] if candidates else None


# Sanity bounds. Found live: ~1.5% of gate_reject_log.price values for
# MARKET_CLOSED specifically are sub-$1 for normally-priced stocks (e.g.
# HIMS logged at $0.91 against a real ~$30-60 range) -- traced to whatever
# upstream caller computed `price` before it reached _log_gate_reject, not
# a bug in the logging call itself (paper_trader.py just passes through
# what it's given). A handful of these can dominate a naive mean with
# +1000%+ "returns" that are entry-price data-quality artifacts, not real
# market moves. Not root-caused further tonight -- flagged in
# docs/XO_BACKLOG.md as its own finding. Filtered here so the report is
# trustworthy in the meantime.
_MIN_SANE_ENTRY_PRICE = 1.0
_MAX_SANE_ABS_RETURN = 1.0  # 100% -- a real single-name move this large in
                            # 1-5 days is rare enough to treat as suspect
                            # for THIS report's purpose (spotting gate
                            # quality, not chasing tail events)


def _fwd_returns(entry_price: float, closes: dict[str, float], entry_date: str) -> dict[int, float | None]:
    if entry_price is None or entry_price < _MIN_SANE_ENTRY_PRICE:
        return {h: None for h in FWD_HORIZONS}
    out = {}
    for h in FWD_HORIZONS:
        target = (date.fromisoformat(entry_date) + timedelta(days=h)).isoformat()
        c = _closest_close_on_or_after(closes, target)
        if c is None:
            out[h] = None
            continue
        r = round((c - entry_price) / entry_price, 6)
        out[h] = r if abs(r) <= _MAX_SANE_ABS_RETURN else None
    return out


def _collect_rejected(conn: sqlite3.Connection, since: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT gate_name, symbol, price, MIN(ts) as ts, player_id
        FROM gate_reject_log
        WHERE ts >= ? AND symbol IS NOT NULL AND price IS NOT NULL AND price > 0
        GROUP BY player_id, symbol, gate_name, date(ts)
        """,
        (since,),
    ).fetchall()


def _collect_executed(conn: sqlite3.Connection, since: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT symbol, price, executed_at as ts, player_id
        FROM trades
        WHERE executed_at >= ? AND symbol IS NOT NULL AND price IS NOT NULL AND price > 0
          AND action LIKE 'BUY%'
        GROUP BY player_id, symbol, date(executed_at)
        """,
        (since,),
    ).fetchall()


def run(dry_run: bool = False) -> dict:
    since = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    conn = _conn()
    rejected = _collect_rejected(conn, since)
    executed = _collect_executed(conn, since)
    conn.close()

    print(f"Deduped rejected signals (last {LOOKBACK_DAYS}d): {len(rejected)}")
    print(f"Deduped executed BUY signals (last {LOOKBACK_DAYS}d): {len(executed)}")

    symbols = {r["symbol"] for r in rejected} | {r["symbol"] for r in executed}
    print(f"Fetching daily bars for {len(symbols)} distinct symbols...")

    bar_end = (date.today() + timedelta(days=8)).isoformat()
    bars_cache: dict[str, dict[str, float]] = {}
    for i, sym in enumerate(sorted(symbols)):
        bars_cache[sym] = _fetch_daily_closes(sym, since, bar_end)
        if (i + 1) % 100 == 0:
            print(f"  ...{i + 1}/{len(symbols)} symbols fetched")
        time.sleep(0.05)  # light pacing, avoid hammering the bars endpoint

    have_bars = sum(1 for v in bars_cache.values() if v)
    print(f"Bars available for {have_bars}/{len(symbols)} symbols "
          f"({len(symbols) - have_bars} skipped -- no bars / no APCA creds / API error)")

    # Executed baseline, all gates blended (one number per horizon)
    exec_fwd: dict[int, list[float]] = {h: [] for h in FWD_HORIZONS}
    for row in executed:
        closes = bars_cache.get(row["symbol"], {})
        if not closes:
            continue
        fwd = _fwd_returns(row["price"], closes, row["ts"][:10])
        for h in FWD_HORIZONS:
            if fwd[h] is not None:
                exec_fwd[h].append(fwd[h])
    exec_avg = {h: (sum(v) / len(v) if v else None) for h, v in exec_fwd.items()}
    exec_n = {h: len(v) for h, v in exec_fwd.items()}

    # Rejected, grouped by gate
    by_gate: dict[str, dict[int, list[float]]] = {}
    gate_counts: dict[str, int] = {}
    for row in rejected:
        gate = row["gate_name"]
        gate_counts[gate] = gate_counts.get(gate, 0) + 1
        closes = bars_cache.get(row["symbol"], {})
        if not closes:
            continue
        fwd = _fwd_returns(row["price"], closes, row["ts"][:10])
        by_gate.setdefault(gate, {h: [] for h in FWD_HORIZONS})
        for h in FWD_HORIZONS:
            if fwd[h] is not None:
                by_gate[gate][h].append(fwd[h])

    def _median(v: list[float]) -> float | None:
        if not v:
            return None
        s = sorted(v)
        n = len(s)
        mid = n // 2
        return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0

    exec_median = {h: _median(v) for h, v in exec_fwd.items()}

    # Render table -- median alongside mean (a naive mean is not robust to
    # the sub-$1 garbage-price artifact noted above; report both so a
    # mean/median gap itself signals when a gate's numbers are outlier-
    # driven rather than trustworthy).
    lines = []
    lines.append(f"# Counterfactual Report — {date.today().isoformat()}")
    lines.append(f"Window: last {LOOKBACK_DAYS} days ({since} to {date.today().isoformat()})")
    lines.append(f"Outlier guard: entry price >= ${_MIN_SANE_ENTRY_PRICE:.2f}, "
                 f"|forward return| <= {_MAX_SANE_ABS_RETURN*100:.0f}% (see script docstring for why)")
    lines.append("")
    lines.append(f"Executed baseline (BUY signals that went live): n={exec_n[1]}/{exec_n[3]}/{exec_n[5]} "
                 f"(1d/3d/5d with bars available)")
    for h in FWD_HORIZONS:
        v, m = exec_avg[h], exec_median[h]
        v_s = f"{v*100:.3f}%" if v is not None else "n/a"
        m_s = f"{m*100:.3f}%" if m is not None else "n/a"
        lines.append(f"  fwd_{h}d: mean={v_s}, median={m_s}")
    lines.append("")
    lines.append("| Gate | Blocked (deduped) | Bars avail | mean/median fwd_1d | mean/median fwd_3d | mean/median fwd_5d | Structural? |")
    lines.append("|---|---|---|---|---|---|---|")
    for gate in sorted(gate_counts, key=lambda g: -gate_counts[g]):
        n_blocked = gate_counts[gate]
        fwd_data = by_gate.get(gate, {h: [] for h in FWD_HORIZONS})
        n_bars = max((len(fwd_data[h]) for h in FWD_HORIZONS), default=0)
        cells = []
        for h in FWD_HORIZONS:
            v = fwd_data[h]
            if v:
                mean_s = f"{(sum(v)/len(v))*100:.2f}"
                med_s = f"{_median(v)*100:.2f}"
                cells.append(f"{mean_s}/{med_s}%")
            else:
                cells.append("n/a")
        structural = "yes" if gate in STRUCTURAL_GATES else ""
        lines.append(f"| {gate} | {n_blocked} | {n_bars} | {cells[0]} | {cells[1]} | {cells[2]} | {structural} |")

    report_text = "\n".join(lines)
    print("\n" + report_text)

    if not dry_run:
        backlog = ROOT / "docs" / "XO_BACKLOG.md"
        with open(backlog, "a") as f:
            f.write("\n\n---\n## Counterfactual Report — " + date.today().isoformat() +
                     " (P1 measurement layer, scripts/counterfactual_report.py)\n\n" +
                     report_text + "\n")
        print(f"\nAppended to {backlog}")

    return {"rejected_n": len(rejected), "executed_n": len(executed), "by_gate": gate_counts}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
