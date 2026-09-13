#!/usr/bin/env python3
"""HM-OPTIONS-TRADES-RESTATEMENT (dry-dock B6, 2026-09-11): reconstruct
options_trades' pre-real-quote-era rows (entry/exit before 2026-07-07, the
window where engine/wheel_strategy.py and engine/shadow_csp.py's synthetic
vix/500-formula CSP pricing bug -- see docs/XO_BACKLOG.md P0-A -- was still
live) from real Alpaca historical option bars where a bar actually exists;
mark unrecoverable where it doesn't.

Read-only by default (prints a report). --write applies the result as new
columns on options_trades (entry_credit_debit_restated, exit_credit_debit_
restated, pnl_restated, restatement_basis) -- ALTER TABLE ADD COLUMN only,
never an UPDATE to an existing column. RULE #1 compliant: no existing value
in trader.db is ever changed. See setup_db.py's HM-OPTIONS-TRADES-
RESTATEMENT block for the columns/view definition (options_trades_restated)
and relay_2026-09-11_B6_options_premium_restatement.md for full findings.

Formula (verified against real rows 28/30/31/32 by hand before writing this):
  entry_credit_debit = sum_legs(sign_open * entry_price * qty * mult)
  exit_credit_debit  = sum_legs(-sign_open * exit_price  * qty * mult)   (0 if OTM-expired)
  pnl = entry_credit_debit + exit_credit_debit
  sign_open = +1 for a short leg (premium received), -1 for a long leg (premium paid)
  mult = 100 for CSP/wheel-convention rows, 1 for the equity-spread-convention
         rows (strategy:bull_spread_v1 / swingdesk-manual) -- detected per-row
         by which value reproduces the row's own recorded entry_credit_debit
         (a real, live units inconsistency between the two code paths that
         both write this table -- flagged, not silently normalized away).
"""
import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
import config
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionBarsRequest
from alpaca.data.timeframe import TimeFrame

DB = str(REPO / "data" / "trader.db")
client = OptionHistoricalDataClient(config.ALPACA_API_KEY, config.ALPACA_SECRET_KEY)

_bar_cache: dict[tuple[str, str], dict] = {}  # (occ, date_str) -> {open,high,low,close}


def occ_symbol(symbol: str, expiration: str, opt_type: str, strike: float) -> str:
    exp = datetime.strptime(expiration, "%Y-%m-%d")
    return f"{symbol}{exp:%y%m%d}{'C' if opt_type == 'call' else 'P'}{int(round(strike * 1000)):08d}"


def _bar_at_or_before(df, occ, target_ts):
    sub = df.loc[occ] if occ in df.index.get_level_values(0) else df
    cutoff = target_ts.replace(tzinfo=sub.index.tz) if sub.index.tz else target_ts
    sub = sub[sub.index <= cutoff]
    return sub


def fetch_price(occ: str, ts_str: str, window_days: int = 5) -> float | None:
    """Real price at-or-before ts_str. Uses HOURLY bars first (needed for
    same-day open/close trades -- daily bars collapse a same-day entry and
    exit to the identical close, which silently produces a fake $0 restated
    P&L for anything that opened and closed within one session, discovered
    live while debugging the first pass of this script against the SPY
    bull_put_spread rows). Falls back to daily bars (wider window) if no
    hourly bar exists (older/less liquid contracts, weekends). Returns None
    -- i.e. genuinely unrecoverable -- only if BOTH fail."""
    key = (occ, ts_str)
    if key in _bar_cache:
        return _bar_cache[key]
    # Both "2026-05-05T15:34:44.107685+00:00" (ISO, entry_date) and
    # "2026-05-05 22:49:31" (space-separated SQLite CURRENT_TIMESTAMP,
    # exit_date) carry real time-of-day and must both keep it -- an earlier
    # version of this parser dropped the time for the space-separated form,
    # which silently defeated the hourly-precision fix for every exit.
    normalized = ts_str.replace(" ", "T").replace("Z", "+00:00")
    try:
        target = datetime.fromisoformat(normalized)
    except ValueError:
        target = datetime.strptime(ts_str[:10], "%Y-%m-%d")
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)

    # Hourly pass: tight window around the exact timestamp.
    try:
        req = OptionBarsRequest(
            symbol_or_symbols=occ, timeframe=TimeFrame.Hour,
            start=target - timedelta(hours=36), end=target + timedelta(hours=1),
        )
        bars = client.get_option_bars(req)
        df = bars.df
        if df is not None and not df.empty:
            sub = _bar_at_or_before(df, occ, target)
            if not sub.empty:
                price = float(sub.iloc[-1]["close"])
                _bar_cache[key] = price
                return price
    except Exception as e:
        print(f"    [warn] hourly fetch_price({occ}, {ts_str}) -> {type(e).__name__}: {e}", file=sys.stderr)

    # Daily fallback: wider window.
    try:
        req = OptionBarsRequest(
            symbol_or_symbols=occ, timeframe=TimeFrame.Day,
            start=target - timedelta(days=window_days), end=target + timedelta(days=1),
        )
        bars = client.get_option_bars(req)
        df = bars.df
        if df is not None and not df.empty:
            sub = _bar_at_or_before(df, occ, target)
            if not sub.empty:
                price = float(sub.iloc[-1]["close"])
                _bar_cache[key] = price
                return price
    except Exception as e:
        print(f"    [warn] daily fetch_price({occ}, {ts_str}) -> {type(e).__name__}: {e}", file=sys.stderr)

    _bar_cache[key] = None
    return None


fetch_close = fetch_price  # alias -- name kept for the call sites below


def detect_mult(entry_credit_debit: float, legs: list[dict]) -> float | None:
    """Return 100.0 or 1.0, whichever reproduces the recorded entry_credit_debit
    from the recorded (possibly-synthetic) leg entry_prices. None if neither matches
    (shouldn't happen given every row was written by one of two known code paths,
    but fail loud rather than silently pick one)."""
    for mult in (100.0, 1.0):
        total = 0.0
        for leg in legs:
            sign = 1.0 if leg["side"] == "short" else -1.0
            total += sign * leg["entry_price"] * leg["qty"] * mult
        if abs(total - entry_credit_debit) < 0.05:
            return mult
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true",
                     help="apply results as new columns on options_trades (additive only)")
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    # HM-OPTIONS-TRADES-RESTATEMENT-GAP-2026-09-12: the original 2026-09-11
    # pass filtered on exit_date, but the bug this restates lived in ENTRY
    # pricing (engine/wheel_strategy.py / shadow_csp.py computed
    # entry_credit_debit from the synthetic vix/500 formula at OPEN time) --
    # a row opened 06-26/06-28 (bug window) that happened to close a day or
    # more after 07-07 was silently skipped entirely. Filtering on entry_date
    # instead (OR'd with the original exit_date condition, so nothing
    # previously in scope drops out) catches those. `restatement_basis IS
    # NULL` makes this idempotent against the 120 rows the first pass already
    # committed -- re-running never re-fetches or overwrites an already-set
    # value, only fills the gap.
    rows = conn.execute(
        "SELECT id, structure, symbol, entry_date, exit_date, expiration, "
        "entry_credit_debit, exit_credit_debit, pnl, status, exit_reason, legs_json "
        "FROM options_trades "
        "WHERE (entry_date < '2026-07-07' OR exit_date < '2026-07-07' OR exit_date IS NULL) "
        "AND (restatement_basis IS NULL OR restatement_basis = '') "
        "ORDER BY id"
    ).fetchall()
    conn.close()

    results = []
    for (rid, structure, symbol, entry_date, exit_date, expiration,
         entry_cd, exit_cd, pnl, status, exit_reason, legs_json) in rows:
        legs = json.loads(legs_json)
        row_result = {
            "id": rid, "structure": structure, "symbol": symbol, "status": status,
            "exit_reason": exit_reason, "recorded_entry_cd": entry_cd,
            "recorded_exit_cd": exit_cd, "recorded_pnl": pnl,
        }

        # Only status='closed' rows with a real exit_reason get restated --
        # open/canceled/failed/expired(non-otm) rows have no settled P&L claim
        # to check in the first place.
        if status != "closed":
            row_result["basis"] = "not_closed_no_restatement_needed"
            results.append(row_result)
            continue

        mult = detect_mult(entry_cd, legs)
        if mult is None:
            row_result["basis"] = "unrecoverable_unknown_unit_convention"
            results.append(row_result)
            continue
        row_result["mult"] = mult

        missing_strike_expiry = any(
            leg.get("strike") is None or not expiration for leg in legs
        )
        if missing_strike_expiry:
            row_result["basis"] = "unrecoverable_missing_contract_data"
            results.append(row_result)
            continue

        # Entry side
        entry_ok = True
        entry_restated = 0.0
        for leg in legs:
            occ = occ_symbol(symbol, expiration, leg["type"], leg["strike"])
            price = fetch_close(occ, entry_date)
            if price is None:
                entry_ok = False
                break
            sign = 1.0 if leg["side"] == "short" else -1.0
            entry_restated += sign * price * leg["qty"] * mult

        # Exit side: OTM expiry means worth $0 by definition, no bar needed.
        # Otherwise fetch a real close at/near exit_date.
        exit_ok = True
        exit_restated = 0.0
        if exit_reason == "expired_otm":
            exit_restated = 0.0
        elif exit_date:
            for leg in legs:
                occ = occ_symbol(symbol, expiration, leg["type"], leg["strike"])
                price = fetch_close(occ, exit_date)
                if price is None:
                    exit_ok = False
                    break
                sign = -1.0 if leg["side"] == "short" else 1.0
                exit_restated += sign * price * leg["qty"] * mult
        else:
            exit_ok = False

        if not entry_ok or not exit_ok:
            row_result["basis"] = "unrecoverable_no_alpaca_bar"
            results.append(row_result)
            continue

        row_result["basis"] = "real_alpaca_bar"
        row_result["entry_cd_restated"] = round(entry_restated, 4)
        row_result["exit_cd_restated"] = round(exit_restated, 4)
        row_result["pnl_restated"] = round(entry_restated + exit_restated, 4)
        results.append(row_result)
        print(f"  id={rid:4d} {structure:18s} {symbol:6s} basis=real_alpaca_bar "
              f"pnl recorded={pnl!s:>10s} restated={row_result['pnl_restated']:>10.2f}")

    # Summary
    basis_counts = Counter(r["basis"] for r in results)
    print("\n=== SUMMARY ===")
    for basis, n in basis_counts.most_common():
        print(f"  {basis}: {n}")

    # HM-OPTIONS-TRADES-RESTATEMENT-GAP-2026-09-12: this file used to be
    # overwritten wholesale on every run -- fine for a true one-shot script,
    # but the 2026-09-12 gap-fill rerun (idempotent against the DB via the
    # restatement_basis filter above) clobbered the original 120-row report
    # down to just the 4 newly-covered rows, silently losing the historical
    # detail as a standalone artifact (the DB columns were untouched and
    # remained the real source of truth, but the JSON export briefly lied
    # about being complete). Merge by id instead of blind-overwriting so a
    # future incremental run can't repeat this.
    out_path = REPO / "data" / "reports" / "options_restatement" / "options_trades_restatement.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    existing = []
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text())
        except Exception:
            existing = []
    merged = {r["id"]: r for r in existing}
    merged.update({r["id"]: r for r in results})
    combined = sorted(merged.values(), key=lambda r: r["id"])
    with open(out_path, "w") as f:
        json.dump(combined, f, indent=2, default=str)
    print(f"\nwrote {len(results)} new/updated row(s), {len(combined)} total, to {out_path}")

    if args.write:
        wconn = sqlite3.connect(DB)
        for r in results:
            wconn.execute(
                "UPDATE options_trades SET entry_credit_debit_restated=?, "
                "exit_credit_debit_restated=?, pnl_restated=?, restatement_basis=? WHERE id=?",
                (r.get("entry_cd_restated"), r.get("exit_cd_restated"),
                 r.get("pnl_restated"), r["basis"], r["id"]),
            )
        wconn.commit()
        wconn.close()
        print(f"--write: applied to {len(results)} rows in {DB}")
    else:
        print("(dry run -- pass --write to apply as new columns on options_trades)")


if __name__ == "__main__":
    main()
