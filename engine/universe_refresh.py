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

# HM-AQ-β 2026-05-07: ensure project root is on sys.path when run standalone
# (`python3 engine/universe_refresh.py`). Without this, `from config import ...`
# fails because Python's auto-added path is the script's directory (engine/),
# not the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [UNIVERSE-REFRESH] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_DB_PATH = _REPO_ROOT / "data" / "trader.db"

# Filter thresholds (mirror engine/universe.py — keep in sync).
MIN_MARKET_CAP = 5_000_000_000.0
# HM-AQ-β v3 2026-05-07: $50M → $100M floor (Captain decision after v2 dry-run
# produced 1,554 finalists, exceeding the dashboard latency budget). $100M
# still catches the "missed movers" (DDOG/FTNT/MDB all >$100M); drops thin
# mid-caps that add noise without value. Predicted final: 600-900.
MIN_DOLLAR_VOLUME = 100_000_000.0
# Captain refinement 2026-05-07 during dry-run: ETF inclusion.
# ETFs lack a market_cap analog from Polygon (they have AUM, not cap), so
# the cap filter would exclude every ETF — losing TQQQ, IWM, XLE, etc.
# ETFs included when dollar_volume passes the same threshold as stocks.
# ETNs (debt notes, different risk profile) are skipped at refresh time.
ETF_DOLLAR_VOLUME_THRESHOLD = MIN_DOLLAR_VOLUME  # parity with stocks ($100M v3)
INCLUDE_ETFS = True
INCLUDE_ETNS = False

# Sanity bounds: refusing to write a wildly different universe.
# Captain bump 2026-05-07 v3: MAX_FINAL_COUNT raised to 2500 to accommodate
# broader stock+ETF universe (post-ETF-inclusion the natural band is 1400-1700,
# headroom for growth).
MIN_FINAL_COUNT = 100
MAX_FINAL_COUNT = 2500

# HM-AO-α 2026-05-08: physical-trust ETFs that Polygon's reference API
# misclassifies as ticker_type='CS'. They have no market_cap analog
# (trusts report AUM, not market cap), so the CS-branch filter would
# reject them. Force-coerce to 'ETF' so the ETF branch (dollar-volume
# only) accepts them. The 5 named below are the Phase 4 audit's
# Grok-diff finding (docs/SCOTTY_INFRA_AUDIT.md follow-up); see
# docs/HM-AO-A_TRUST_ETF_FIX.md for the investigation log.
TRUST_ETF_OVERRIDES: frozenset[str] = frozenset({
    "GLD",   # SPDR Gold Trust
    "GLDM",  # SPDR Gold MiniShares Trust
    "IAU",   # iShares Gold Trust
    "SIVR",  # abrdn Physical Silver Shares
    "SLV",   # iShares Silver Trust
})

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
    """Send NTFY alert to ollietrades-admin (HM-AQ-β failure posture).

    Title uses ASCII-only "HM-AQ-beta" because HTTP headers must be latin-1
    encodable; the β character would raise UnicodeEncodeError. Body is
    UTF-8 so β survives there if used.
    """
    try:
        import requests
        requests.post(
            "https://ntfy.sh/ollietrades-admin",
            data=message.encode("utf-8"),
            headers={"Title": "HM-AQ-beta refresh", "Priority": priority},
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

def _fetch_ticker_details_polygon(api_key: str, ticker: str) -> tuple[Optional[float], Optional[str]]:
    """One call to /v3/reference/tickers/{TICKER}; returns (market_cap, type).

    Returns (None, None) on any failure. `type` is Polygon's classification:
    'CS' (common stock), 'ETF', 'ETN', 'ADRC', 'PFD' (preferred), etc.
    """
    import requests
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    try:
        r = requests.get(url, params={"apiKey": api_key}, timeout=10)
        if r.status_code != 200:
            return None, None
        result = r.json().get("results") or {}
        mc = result.get("market_cap")
        tt = result.get("type")
        return (float(mc) if mc else None, tt)
    except Exception:
        return None, None


def _fetch_market_cap_yfinance(ticker: str) -> Optional[float]:
    """Fallback path 1 — yfinance bulk Ticker.info['marketCap']. Stocks only."""
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info or {}
        mc = info.get("marketCap")
        return float(mc) if mc else None
    except Exception:
        return None


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
                     last_updated, market_cap, options_eligible, ticker_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    avg_volume       = excluded.avg_volume,
                    avg_price        = excluded.avg_price,
                    last_updated     = excluded.last_updated,
                    market_cap       = excluded.market_cap,
                    options_eligible = excluded.options_eligible,
                    ticker_type      = excluded.ticker_type
                """,
                (
                    r["symbol"], r.get("name") or r["symbol"], r.get("exchange") or "",
                    r.get("sector") or "", r["avg_volume"], r["avg_price"],
                    now_iso, r["market_cap"], 1 if r["options_eligible"] else 0,
                    r.get("ticker_type") or "CS",
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
        "step2_etn_skipped": 0,
        "step2_etf_included": 0,
        "step2_other_type_skipped": 0,
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

    # Step 2 — per-symbol ticker details (rate-limited)
    # Captain refinement 2026-05-07: branch on ticker_type:
    #   CS  → require market_cap >= $5B (with yfinance fallback)
    #   ETF → include if dollar_volume >= $50M (already passed Step 1)
    #   ETN → skip
    #   other (ADRC, PFD, etc.) → skip for now
    log.info("Step 2: fetching ticker details for %d candidates (throttled at %d cps) ...",
             len(candidates), _POLYGON_CPS)
    finalists = []
    for i, c in enumerate(candidates):
        sym = c["symbol"]
        if not sym or "." in sym or "/" in sym:  # skip BRK.B, RDS/B etc — Polygon often nulls these
            summary["step2_skipped"] += 1
            continue
        throttle.wait()
        mc, ttype = _fetch_ticker_details_polygon(api_key, sym)

        # HM-AO-α 2026-05-08: physical-trust ETF override. Polygon classifies
        # GLD/GLDM/IAU/SIVR/SLV as CS despite their being trust ETFs.
        # Coerce ttype to ETF so they reach the ETF branch (dollar-volume only).
        if sym in TRUST_ETF_OVERRIDES and ttype != "ETF":
            log.info("  trust_etf_override %s polygon_type=%s -> ETF", sym, ttype)
            ttype = "ETF"

        # ETN branch — Captain decision 2026-05-07: skip ETNs entirely
        if ttype == "ETN":
            summary["step2_etn_skipped"] += 1
            log.info("  type_skipped %s type=ETN", sym)
            continue

        # ETF branch — Captain decision 2026-05-07: include on dollar_volume only
        if ttype == "ETF" and INCLUDE_ETFS:
            c["market_cap"] = None  # ETFs have no cap analog
            c["options_eligible"] = False
            c["ticker_type"] = "ETF"
            finalists.append(c)
            summary["step2_etf_included"] += 1
            log.info("  etf_included %s dollar_volume=$%.1fM",
                     sym, c["dollar_volume"] / 1e6)
            if (i + 1) % 100 == 0:
                log.info("  step 2 progress: %d/%d processed, %d finalists",
                         i + 1, len(candidates), len(finalists))
            continue

        # Stock (CS) branch + fallback chain. Other types (ETV, PFD, ADRC,
        # FUND, SP, OS, etc.) are explicitly skipped — Captain v3 2026-05-07.
        if ttype not in ("CS", None):
            summary["step2_other_type_skipped"] += 1
            log.info("  type_skipped %s type=%s", sym, ttype)
            continue

        if mc is not None:
            summary["step2_polygon_ok"] += 1
        else:
            mc = _fetch_market_cap_yfinance(sym)
            if mc is not None:
                summary["step2_yfinance_fallback"] += 1
                log.info("  fallback yfinance for %s -> market_cap=$%.1fB",
                         sym, mc / 1e9)
            else:
                summary["step2_skipped"] += 1
                log.info("  no_cap_skipped %s (polygon=None, yfinance=None)", sym)
                continue

        if mc < MIN_MARKET_CAP:
            log.info("  stock_capfail %s cap=$%.2fB threshold=$%.1fB",
                     sym, mc / 1e9, MIN_MARKET_CAP / 1e9)
            continue
        c["market_cap"] = mc
        c["options_eligible"] = False
        c["ticker_type"] = ttype or "CS"
        finalists.append(c)
        if (i + 1) % 100 == 0:
            log.info("  step 2 progress: %d/%d processed, %d finalists",
                     i + 1, len(candidates), len(finalists))
    summary["step2_passing_cap"] = len(finalists)
    log.info("Step 2: %d total finalists (CS pass cap, ETF pass volume) | "
             "ETF=%d, ETN_skipped=%d, other_type_skipped=%d, fallback=%d, skipped=%d",
             len(finalists), summary["step2_etf_included"], summary["step2_etn_skipped"],
             summary["step2_other_type_skipped"], summary["step2_yfinance_fallback"],
             summary["step2_skipped"])

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
            # HM-AQ-β v3 fix: ETFs have market_cap=None; format defensively.
            cap_str = (f"${c['market_cap']/1e9:.1f}B"
                       if c.get("market_cap") is not None else "ETF (no cap)")
            log.info("  %s [%s] | cap=%s | $vol=$%.0fM | opt=%s",
                     c["symbol"], c.get("ticker_type", "?"), cap_str,
                     c["dollar_volume"] / 1e6, c["options_eligible"])
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
