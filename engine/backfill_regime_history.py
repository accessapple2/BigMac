#!/usr/bin/env python3
"""
Backfill regime_history using Alpaca historical bars (SPY + QQQ).
Matches actual DB schema: date, spy_close, ma_8, ma_21,
qqq_close, qqq_ma_8, qqq_ma_21, regime, cross_date,
cross_days_ago, size_modifier.
"""
import os, sqlite3, requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

_root   = Path(__file__).parent.parent
DB_PATH = _root / "data" / "trader.db"

# Load .env manually (lightweight, no dotenv dep)
_env = _root / ".env"
if _env.exists():
    for line in _env.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

API_KEY    = os.environ.get("ALPACA_API_KEY", "")
SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
DATA_BASE  = "https://data.alpaca.markets/v2"
HEADERS    = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": SECRET_KEY}

BACKFILL_DAYS = 210   # fetch extra for MA warm-up


def _fetch_bars(symbol: str, days: int) -> list[dict]:
    """Fetch daily bars from Alpaca data API."""
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    end   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bars  = []
    url   = f"{DATA_BASE}/stocks/{symbol}/bars"
    params = {
        "timeframe": "1Day",
        "start": start,
        "end": end,
        "limit": 1000,
        "feed": "iex",
        "sort": "asc",
    }
    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
        if r.status_code != 200:
            print(f"  ⚠️  Alpaca {symbol}: HTTP {r.status_code} — {r.text[:120]}")
            break
        data = r.json()
        bars.extend(data.get("bars", []))
        token = data.get("next_page_token")
        if not token:
            break
        params["page_token"] = token
    return bars


def _to_closes(bars: list[dict]) -> dict[str, float]:
    """Map date-string → close price."""
    return {b["t"][:10]: b["c"] for b in bars}


def _rolling_mean(closes: dict[str, float], dates: list[str], window: int) -> dict[str, float]:
    out = {}
    vals = []
    for d in dates:
        c = closes.get(d)
        if c is None:
            out[d] = float("nan")
            continue
        vals.append(c)
        if len(vals) >= window:
            out[d] = sum(vals[-window:]) / window
        else:
            out[d] = float("nan")
    return out


def _regime(ma8: float, ma21: float) -> str:
    if ma8 > ma21:
        return "BULL_CROSS"
    diff_pct = (ma21 - ma8) / ma21 * 100
    return "CAUTIOUS_BEAR" if diff_pct < 1.5 else "BEAR_CROSS"


def run_backfill():
    print(f"\n{'=' * 60}")
    print("📅 REGIME HISTORY BACKFILL (Alpaca data)")
    print(f"{'=' * 60}")

    print("Fetching SPY bars …")
    spy_bars = _fetch_bars("SPY", BACKFILL_DAYS)
    print(f"  SPY: {len(spy_bars)} bars")

    print("Fetching QQQ bars …")
    qqq_bars = _fetch_bars("QQQ", BACKFILL_DAYS)
    print(f"  QQQ: {len(qqq_bars)} bars")

    if not spy_bars:
        print("❌  No SPY data — check Alpaca credentials")
        return 0

    spy_closes = _to_closes(spy_bars)
    qqq_closes = _to_closes(qqq_bars)

    # Union of all dates, sorted
    all_dates = sorted(set(spy_closes) | set(qqq_closes))

    # Rolling MAs
    spy_ma8  = _rolling_mean(spy_closes, all_dates, 8)
    spy_ma21 = _rolling_mean(spy_closes, all_dates, 21)
    qqq_ma8  = _rolling_mean(qqq_closes, all_dates, 8)
    qqq_ma21 = _rolling_mean(qqq_closes, all_dates, 21)

    conn     = sqlite3.connect(DB_PATH)
    existing = {r[0] for r in conn.execute("SELECT date FROM regime_history").fetchall()}

    inserted = 0
    for date_str in all_dates:
        if date_str in existing:
            continue
        sc   = spy_closes.get(date_str, float("nan"))
        sm8  = spy_ma8.get(date_str, float("nan"))
        sm21 = spy_ma21.get(date_str, float("nan"))
        qc   = qqq_closes.get(date_str, sc)
        qm8  = qqq_ma8.get(date_str, sm8)
        qm21 = qqq_ma21.get(date_str, sm21)

        # Skip rows without valid MAs
        if sm8 != sm8 or sm21 != sm21:
            continue

        reg      = _regime(sm8, sm21)
        size_mod = 1.25 if reg == "BULL_CROSS" else (1.0 if reg == "CAUTIOUS_BEAR" else 0.75)

        conn.execute(
            """INSERT INTO regime_history
               (date, spy_close, ma_8, ma_21,
                qqq_close, qqq_ma_8, qqq_ma_21,
                regime, size_modifier)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (date_str,
             round(sc,2),  round(sm8,2),  round(sm21,2),
             round(qc,2),  round(qm8,2),  round(qm21,2),
             reg, size_mod),
        )
        inserted += 1

    conn.commit()

    rows  = conn.execute(
        "SELECT regime, COUNT(*) FROM regime_history GROUP BY regime ORDER BY 2 DESC"
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM regime_history").fetchone()[0]
    span  = conn.execute("SELECT MIN(date), MAX(date) FROM regime_history").fetchone()
    conn.close()

    print(f"\n✅  Inserted {inserted} new rows  (total: {total}  |  {span[0]} → {span[1]})")
    print("\n📊 Regime distribution:")
    for reg, cnt in rows:
        print(f"   {reg:<18} {cnt:>4} days  ({cnt/total*100:.0f}%)")

    return inserted


if __name__ == "__main__":
    run_backfill()
