#!/usr/bin/env python3
"""HM-REGIME-REFRESH-FIX — standalone, no-restart regime refresher.

ROOT CAUSE: the in-trader 15-min regime job (run_ma_regime_update) starves after the 06:30 open
(single-thread scheduler hogged by ~720s WR cycles), so regime_history freezes at the pre-open value
all session and the grade-B gate reads a stale (often bearish) regime.

This decoupled runner (cron every 15 min during RTH) recomputes engine.regime_ma.detect_ma_cross_regime
in its OWN process and ensures the result lands in regime_history — fully independent of the trader's
scheduler. The 8/21 DAILY cross stays a daily signal; this only restores intraday responsiveness of the
price_above_ma8 boundary (CAUTIOUS_BEAR<->BULL_CROSS).

Robustness: detect_ma_cross_regime persists canonically (with the HM-FINMEM change-fanout) but SWALLOWS
sqlite "database is locked" silently and caches for 5 min. So we (1) call it once to compute+persist,
then (2) VERIFY created_at advanced; if not (lock swallowed), do a direct busy_timeout=30 upsert from the
result so the row always lands. Read-only on everything except its own regime_history upsert.
"""
from __future__ import annotations
import os, sys, time, sqlite3
from datetime import datetime, timezone

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(REPO)                       # regime_ma._save_to_db uses relative "data/trader.db"
sys.path.insert(0, REPO)
DB = os.path.join(REPO, "data", "trader.db")
LOG = os.path.join(REPO, "logs", "regime_refresh.log")

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(REPO, ".env"))
except Exception:
    pass


def log(msg: str):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def latest_created_at(today: str):
    try:
        c = sqlite3.connect(DB, timeout=30)
        row = c.execute(
            "SELECT created_at FROM regime_history WHERE date=? ORDER BY id DESC LIMIT 1",
            (today,),
        ).fetchone()
        c.close()
        return row[0] if row else None
    except Exception as e:
        log(f"created_at read err: {type(e).__name__}: {e!r}")
        return None


def fresh(created_at: str | None, max_age_s: int = 180) -> bool:
    if not created_at:
        return False
    try:
        t = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds() < max_age_s
    except Exception:
        return False


def fallback_upsert(r: dict, today: str) -> bool:
    """Direct busy_timeout upsert when detect's own persist got starved (lock-swallowed).
    Columns mirror engine/regime_ma.py::_save_to_db (INSERT OR REPLACE keyed on date)."""
    for attempt in range(3):
        try:
            c = sqlite3.connect(DB, timeout=30)
            c.execute("PRAGMA busy_timeout=30000")
            c.execute(
                """INSERT OR REPLACE INTO regime_history
                   (date, spy_close, ma_8, ma_21, qqq_close, qqq_ma_8, qqq_ma_21,
                    regime, cross_date, cross_days_ago, size_modifier, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
                (today, r.get("spy_close"), r.get("spy_ma8"), r.get("spy_ma21"),
                 r.get("qqq_close"), r.get("qqq_ma8"), r.get("qqq_ma21"),
                 r.get("regime"), r.get("cross_date"), r.get("cross_days_ago"),
                 r.get("size_modifier")),
            )
            c.commit()
            c.close()
            return True
        except Exception as e:
            log(f"fallback upsert attempt {attempt+1} err: {type(e).__name__}: {e!r}")
            time.sleep(5 * (attempt + 1))
    return False


def main():
    now = datetime.now()  # bigmac local = Arizona (MST, no DST)
    today = now.strftime("%Y-%m-%d")

    # RTH gate: trading day + 06:30-13:00 AZ (= 09:30-16:00 ET). Off-hours = no-op.
    try:
        from engine.market_calendar import is_trading_day
        if not is_trading_day(now.date()):
            log("skip: not a trading day"); return
    except Exception as e:
        log(f"is_trading_day import err (proceeding on weekday gate): {e!r}")
        if now.weekday() >= 5:
            log("skip: weekend"); return
    mins = now.hour * 60 + now.minute
    if not (6 * 60 + 30 <= mins <= 13 * 60):
        log(f"skip: outside RTH ({now.strftime('%H:%M')} AZ)"); return

    from engine.regime_ma import detect_ma_cross_regime
    r = detect_ma_cross_regime()  # compute + canonical persist (+fanout on change)
    regime = r.get("regime")
    spy_close, spy_ma8 = r.get("spy_close"), r.get("spy_ma8")
    price_above_ma8 = (spy_close is not None and spy_ma8 is not None and spy_close > spy_ma8)

    landed = fresh(latest_created_at(today))
    if not landed:
        log(f"detect persist not fresh (lock?) — fallback upsert regime={regime}")
        landed = fallback_upsert(r, today)

    ca = latest_created_at(today)
    status = "OK" if landed and fresh(ca) else "FAIL"
    log(f"{status} regime={regime} spy_close={spy_close} spy_ma8={spy_ma8} "
        f"price_above_ma8={price_above_ma8} created_at={ca}")
    if status == "FAIL":
        log("ERROR: regime_history did NOT advance this cycle (persistent lock?) — investigate")
        sys.exit(1)


if __name__ == "__main__":
    main()
