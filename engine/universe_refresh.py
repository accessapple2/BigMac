"""HM-AQ-β 2026-05-07 — Polygon-driven weekly universe refresh.

Captain decision (HM-AQ commit 773effe, docs/UNIVERSE.md):
 - market_cap >= $5B
 - avg_dollar_volume >= $50M
 - Polygon Stocks + Polygon Options (both paid, $58/mo total)
 - Sunday 14:00 MST via launchd

3-step pipeline:

  Step 1: /v2/aggs/grouped/locale/us/market/stocks/{prev-trading-day}
          → 12K rows in ONE call. Filter to dollar_volume >= $50M.
          Yields ~500-1500 candidates.

  Step 2: /v3/reference/tickers/{TICKER} per candidate
          → market_cap field. Filter to >= $5B.
          Yields final ~500-800.

  Step 3: /v3/reference/options/contracts?underlying_ticker=X&limit=1
          → set options_eligible = 1 if results found.

Fallback chain per-symbol when Polygon fails (HM-AQ-β spec 5):
  yfinance market_cap → Alpaca avg_volume → skip symbol.

Sanity bounds: 100 <= final_count <= 1500. Outside band → fail-safe
(retain prior scan_universe), NTFY ollietrades-admin.

Throttle: 5 calls/sec (Polygon Starter tier).

Usage:
  python3 engine/universe_refresh.py --dry-run   # log only, no DB write
  python3 engine/universe_refresh.py             # full wet refresh
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [UNIVERSE-REFRESH] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DB_PATH = _REPO_ROOT / "data" / "trader.db"

# Filter thresholds (mirror engine/universe.py — keep in sync).
MIN_MARKET_CAP = 5_000_000_000.0
MIN_DOLLAR_VOLUME = 50_000_000.0

# Sanity bounds: refusing to write a wildly different universe.
MIN_FINAL_COUNT = 100
MAX_FINAL_COUNT = 1500

# Polygon throttle: Stocks Starter + Options Starter both 5 cps.
# We share the budget; 5 cps total is conservative.
_POLYGON_CPS = 5
_THROTTLE_INTERVAL = 1.0 / _POLYGON_CPS


def _polygon_key() -> str:
    key = os.getenv("POLYGON_API_KEY", "")
    if not key:
        try:
            from config import POLYGON_API_KEY as _k
            key = _k or ""
        except Exception:
            pass
    return key


def _ntfy(message: str, priority: str = "default") -> None:
    """Send NTFY alert to ollietrades-admin (HM-AQ-β failure posture)."""
    try:
        import requests
        requests.post(
            "https://ntfy.sh/ollietrades-admin",
            data=message.encode("utf-8"),
            headers={"Title": "HM-AQ-β refresh", "Priority": priority},
            timeout=10,
        )
    except Exception as e:
        log.warning("NTFY send failed: %s", e)


def _prev_trading_day() -> str:
    """Return previous US trading day as YYYY-MM-DD (skips weekends)."""
    now = datetime.now(timezone.utc)
    d = now.date() - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.isoformat()


# ─── Step 1: grouped daily aggregates ────────────────────────────────────────

def _fetch_grouped_daily(api_key: str, day: str) -> list[dict]:
    """One call: returns ~12K rows of (T=ticker, v=volume, c=close, ...)."""
    import requests
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{day}"
    log.info("Step 1: fetching grouped daily for %s ...", day)
    r = requests.get(url, params={"apiKey": api_key, "adjusted": "true"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    results = data.get("results") or []
    log.info("Step 1: %d rows received", len(results))
    return results


def _filter_by_dollar_volume(rows: list[dict], min_dv: float) -> list[dict]:
    """Filter grouped rows by dollar_volume = v * c."""
    candidates = []
    for r in rows:
        v = r.get("v") or 0
        c = r.get("c") or 0
        dv = v * c
        if dv >= min_dv:
            candidates.append({
                "symbol": r.get("T"),
                "avg_volume": v,
                "avg_price": c,
                "dollar_volume": dv,
            })
    log.info("Step 1: %d symbols pass dollar_volume >= $%.0fM", len(candidates), min_dv / 1e6)
    return candidates


# ─── Step 2: per-symbol market_cap ───────────────────────────────────────────

def _fetch_market_cap_polygon(api_key: str, ticker: str) -> Optional[float]:
    """One call to /v3/reference/tickers/{TICKER}; returns market_cap or None."""
    import requests
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    try:
        r = requests.get(url, params={"apiKey": api_key}, timeout=10)
        if r.status_code != 200:
            return None
        result = r.json().get("results") or {}
        mc = result.get("market_cap")
        return float(mc) if mc else None
    except Exception:
        return None


def _fetch_market_cap_yfinance(ticker: str) -> Optional[float]:
    """Fallback path 1 — yfinance bulk Ticker.info['marketCap']."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        mc = info.get("marketCap")
        return float(mc) if mc else None
    except Exception:
        return None


def _resolve_market_cap(api_key: str, ticker: str) -> tuple[Optional[float], str]:
    """Return (market_cap, source). source ∈ {'polygon', 'yfinance', 'failed'}."""
    mc = _fetch_market_cap_polygon(api_key, ticker)
    if mc is not None:
        return mc, "polygon"
    mc = _fetch_market_cap_yfinance(ticker)
    if mc is not None:
        return mc, "yfinance"
    return None, "failed"


# ─── Step 3: options eligibility ─────────────────────────────────────────────

def _check_options_eligible(api_key: str, ticker: str) -> bool:
    """One call to /v3/reference/options/contracts; True if any contract found."""
    import requests
    url = "https://api.polygon.io/v3/reference/options/contracts"
    try:
        r = requests.get(
            url,
            params={"underlying_ticker": ticker, "limit": 1, "apiKey": api_key},
            timeout=10,
        )
        if r.status_code != 200:
            return False
        return bool(r.json().get("results"))
    except Exception:
        return False


# ─── Throttle helper ─────────────────────────────────────────────────────────

class _Throttle:
    """Simple Polygon 5cps throttle — sleep before each call to maintain rate."""
    def __init__(self, cps: float):
        self.interval = 1.0 / cps
        self.last = 0.0

    def wait(self) -> None:
        now = time.time()
        elapsed = now - self.last
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last = time.time()


# ─── DB write ────────────────────────────────────────────────────────────────

def _write_universe(rows: list[dict]) -> int:
    """Upsert rows into scan_universe with new columns. Returns affected count."""
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn = sqlite3.connect(_DB_PATH, timeout=30)
    try:
        n = 0
        for r in rows:
            conn.execute(
                """
                INSERT INTO scan_universe
                    (symbol, name, exchange, sector, avg_volume, avg_price,
                     last_updated, market_cap, options_eligible)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    avg_volume       = excluded.avg_volume,
                    avg_price        = excluded.avg_price,
                    last_updated     = excluded.last_updated,
                    market_cap       = excluded.market_cap,
                    options_eligible = excluded.options_eligible
                """,
                (
                    r["symbol"], r.get("name") or r["symbol"], r.get("exchange") or "",
                    r.get("sector") or "", r["avg_volume"], r["avg_price"],
                    now_iso, r["market_cap"], 1 if r["options_eligible"] else 0,
                ),
            )
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()


# ─── Orchestrator ────────────────────────────────────────────────────────────

def run_refresh(dry_run: bool = False) -> dict:
    """Main entry. Returns summary dict.

    Sets `final` count, `polygon_failed`, `yfinance_used`, `skipped`, `options_eligible`.
    On sanity-bounds failure: returns dict with `status='failed'` and NTFYs.
    """
    api_key = _polygon_key()
    if not api_key:
        log.error("POLYGON_API_KEY missing — cannot refresh")
        _ntfy("HM-AQ-β refresh aborted: POLYGON_API_KEY missing", priority="high")
        return {"status": "failed", "reason": "no_api_key"}

    summary = {
        "status": "ok",
        "step1_rows": 0,
        "step1_passing": 0,
        "step2_polygon_ok": 0,
        "step2_yfinance_fallback": 0,
        "step2_skipped": 0,
        "step2_passing_cap": 0,
        "step3_options_eligible": 0,
        "final": 0,
        "dry_run": dry_run,
    }

    throttle = _Throttle(_POLYGON_CPS)

    # Step 1
    day = _prev_trading_day()
    grouped = _fetch_grouped_daily(api_key, day)
    summary["step1_rows"] = len(grouped)
    candidates = _filter_by_dollar_volume(grouped, MIN_DOLLAR_VOLUME)
    summary["step1_passing"] = len(candidates)
    if not candidates:
        log.error("Step 1 produced 0 candidates — aborting")
        _ntfy(f"HM-AQ-β refresh: step 1 produced 0 candidates for {day}", priority="high")
        return {**summary, "status": "failed", "reason": "step1_empty"}

    # Step 2 — per-symbol market_cap (rate-limited)
    log.info("Step 2: fetching market_cap for %d candidates (throttled at %d cps) ...",
             len(candidates), _POLYGON_CPS)
    finalists = []
    for i, c in enumerate(candidates):
        sym = c["symbol"]
        if not sym or "." in sym or "/" in sym:  # skip BRK.B, RDS/B etc — Polygon often nulls these
            summary["step2_skipped"] += 1
            continue
        throttle.wait()
        mc, source = _resolve_market_cap(api_key, sym)
        if source == "polygon":
            summary["step2_polygon_ok"] += 1
        elif source == "yfinance":
            summary["step2_yfinance_fallback"] += 1
            log.info("  fallback yfinance for %s -> market_cap=%s", sym, mc)
        else:
            summary["step2_skipped"] += 1
            log.info("  skipped %s (no market_cap from polygon or yfinance)", sym)
            continue
        if mc is None or mc < MIN_MARKET_CAP:
            continue
        c["market_cap"] = mc
        c["options_eligible"] = False
        finalists.append(c)
        if (i + 1) % 100 == 0:
            log.info("  step 2 progress: %d/%d candidates processed, %d finalists",
                     i + 1, len(candidates), len(finalists))
    summary["step2_passing_cap"] = len(finalists)
    log.info("Step 2: %d symbols pass market_cap >= $%.1fB", len(finalists), MIN_MARKET_CAP / 1e9)

    # Sanity bounds before Step 3 (catches mass failures early)
    if not (MIN_FINAL_COUNT <= len(finalists) <= MAX_FINAL_COUNT):
        log.error("Final count %d outside bounds [%d, %d] — fail-safe abort, retain prior",
                  len(finalists), MIN_FINAL_COUNT, MAX_FINAL_COUNT)
        _ntfy(
            f"HM-AQ-β refresh aborted: post-cap-filter count {len(finalists)} "
            f"outside bounds [{MIN_FINAL_COUNT}, {MAX_FINAL_COUNT}]",
            priority="high",
        )
        return {**summary, "status": "failed", "reason": "out_of_bounds", "final": len(finalists)}

    # Step 3 — options eligibility
    log.info("Step 3: checking options_eligible for %d finalists ...", len(finalists))
    for i, c in enumerate(finalists):
        throttle.wait()
        eligible = _check_options_eligible(api_key, c["symbol"])
        c["options_eligible"] = eligible
        if eligible:
            summary["step3_options_eligible"] += 1
        if (i + 1) % 100 == 0:
            log.info("  step 3 progress: %d/%d", i + 1, len(finalists))

    summary["final"] = len(finalists)

    # Write
    if dry_run:
        log.info("DRY RUN — would have written %d rows. Sample:", len(finalists))
        for c in finalists[:5]:
            log.info("  %s | cap=$%.1fB | $vol=$%.0fM | opt=%s",
                     c["symbol"], c["market_cap"] / 1e9, c["dollar_volume"] / 1e6,
                     c["options_eligible"])
    else:
        n_written = _write_universe(finalists)
        summary["written"] = n_written
        log.info("Wrote %d rows to scan_universe", n_written)

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="HM-AQ-β universe refresh")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log only; do not write to scan_universe")
    args = parser.parse_args()
    summary = run_refresh(dry_run=args.dry_run)
    log.info("=" * 60)
    log.info("Summary: %s", summary)
    log.info("=" * 60)
    return 0 if summary.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
