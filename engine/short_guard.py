"""HM-SHORT-ACTIVATION (2026-05-30) — unbounded-risk safeguards for stock shorts.

Admiral-approved 6 params (LLM/arena path only; rules-path SHORT_LOGGED stub left
as-is per HM-SHORT-RULES-PATH for a later pass):

  1. Hard stop      : flat 8% adverse → buy-to-cover (entry × 1.08). Tighter + flat
                      than the long side (8-18% conviction-scaled) because a short's
                      loss is structurally unbounded and squeezes gap violently.
  2. Profit target  : cover 50% at -10%; trail remaining 50% with a 5% ratcheting
                      buy-stop that steps down as price falls.
  3. Size           : per-position 10% of agent cash (was 15%); NEW aggregate cap
                      20% of book across all open shorts.
  4. Squeeze guard  : block the short if days-to-cover > 5 OR earnings within 3
                      days. **FAILS CLOSED** — if EITHER input can't be fetched
                      => DO NOT SHORT (deliberate opposite of the long-side
                      fail-open; never take an unbounded-risk trade blind to its
                      single largest tail risk).
                      DATA SOURCES — ALL PAID, NO FREE-SCRAPE (2026-05-30 audit):
                        • days-to-cover : Polygon /stocks/v1/short-interest (paid)
                        • earnings date : Finnhub /calendar/earnings (paid, wired)
                      SI %-of-float DROPPED for now — Polygon has no float field and
                      Finviz Elite is paid-but-unwired (HM-FINVIZ-ELITE-AUTH). NO
                      finvizfinance (free scrape) and NO yfinance anywhere in this
                      guard — both silently return empty-but-clean under throttle.
  5. Agents         : navigator, energy-arnold, qwen3-8b-flash (short_enabled=1).
                      dalio-metals excluded (tracking-only route → would log, not
                      fill).
  6. Stop upgrade   : the weak "stop in reasoning" substring check is superseded by
                      the real attached buy-stop (#1).

This module is pure-logic + read-only data fetch. It executes no orders and is
import-safe whether SHORT_ENABLED is True or False, so the dry-run can exercise it
with the flag still OFF.
"""
from __future__ import annotations

import time as _time

# DTC cache (in-process): {symbol: (mono_ts, value|None)}. Polygon SI settles
# bi-monthly so a long TTL is fine; keeps the buy gate fast.
_dtc_cache: dict = {}
_DTC_TTL = 12 * 3600  # 12h


def _now_mono() -> float:
    return _time.monotonic()


# ── Admiral-approved constants ───────────────────────────────────────────────
SHORT_HARD_STOP_PCT     = 0.08   # buy-to-cover at entry × 1.08
SHORT_TARGET_PCT        = 0.10   # cover 50% at -10%
SHORT_TARGET_COVER_FRAC = 0.50   # fraction covered at first target
SHORT_TRAIL_PCT         = 0.05   # 5% ratcheting buy-stop on the runner
SHORT_MAX_POSITION_PCT  = 0.10   # per-position cap (of agent cash)
SHORT_MAX_AGGREGATE_PCT = 0.20   # aggregate open-short cap (of book)

SQUEEZE_SI_PCT_MAX      = 20.0   # block if short-interest %-of-float exceeds this
SQUEEZE_DTC_MAX         = 5.0    # block if days-to-cover exceeds this
SQUEEZE_EARNINGS_DAYS   = 3      # block if earnings within this many days

SHORT_AUTHORIZED_AGENTS = {"navigator", "energy-arnold", "qwen3-8b-flash"}
# dalio-metals deliberately excluded (route_mode=tracking → log-only, never fills)


def short_levels(entry_price: float) -> dict:
    """Pre-computed exit ladder for a short opened at entry_price.

    hard_stop : buy-stop price that force-covers the whole position (8% adverse)
    target    : price at which 50% is covered (-10%)
    trail_pct : ratcheting buy-stop width for the remaining 50%
    """
    return {
        "hard_stop": round(entry_price * (1.0 + SHORT_HARD_STOP_PCT), 2),
        "target": round(entry_price * (1.0 - SHORT_TARGET_PCT), 2),
        "target_cover_frac": SHORT_TARGET_COVER_FRAC,
        "trail_pct": SHORT_TRAIL_PCT,
    }


def _fetch_dtc(symbol: str) -> tuple[float | None, str]:
    """Days-to-cover — POLYGON ONLY (paid /stocks/v1/short-interest, all plans).

    Returns (value, source). value=None means Polygon couldn't return it → the
    caller FAILS CLOSED (refuses the short). NO finvizfinance (free scrape) and NO
    yfinance anywhere — both can return empty-but-clean under throttle, the exact
    silent-degradation trap an unbounded-risk guard must never have. SI %-of-float
    is intentionally DROPPED for now (Polygon has no float field; Finviz Elite is
    paid-but-unwired — see HM-FINVIZ-ELITE-AUTH); DTC is the reliable squeeze
    metric (GME 4.2, CVNA 6.7 probed).
    """
    now = _now_mono()
    cached = _dtc_cache.get(symbol)
    if cached and (now - cached[0]) < _DTC_TTL:
        return cached[1], ("polygon" if cached[1] is not None else "none")
    val = None
    try:
        from engine.squeeze_scanner import _fetch_polygon_si
        psi = _fetch_polygon_si(symbol)
        if psi and psi.get("days_to_cover"):
            val = float(psi["days_to_cover"])
    except Exception:
        val = None
    _dtc_cache[symbol] = (now, val)
    return val, ("polygon" if val is not None else "none")


def _earnings_within(symbol: str, days: int):
    """True/False/None — is `symbol` reporting earnings within `days`?

    RELIABLE SOURCE ONLY — Finnhub `/calendar/earnings` (paid, already wired at
    engine/finnhub_data.get_earnings_calendar; used live by event_shield +
    channel_scanner; probed 2026-05-30 HTTP 200, 122 rows/6d). Repointed FROM the
    old yfinance `.calendar` which returned empty under Yahoo throttle (the guard
    was inert). NO yfinance, NO Finviz.

    Returns:
      True  — symbol is in the Finnhub earnings window
      False — fetch succeeded, symbol NOT reporting in window
      None  — FETCH FAILED. The market-wide earnings calendar over a multi-day
              window is never genuinely empty (122 rows/6d), so an empty return
              means the Finnhub call failed → caller FAILS CLOSED. This is how we
              distinguish "no earnings" (False) from "couldn't check" (None).
    """
    from datetime import datetime, timedelta, timezone
    try:
        from engine.finnhub_data import get_earnings_calendar
        today = datetime.now(timezone.utc).date()
        frm = today.strftime("%Y-%m-%d")
        to = (today + timedelta(days=days)).strftime("%Y-%m-%d")
        rows = get_earnings_calendar(from_date=frm, to_date=to)
        if not rows:
            return None  # market-wide window never truly empty → fetch failed → fail-closed
        syms = {str(r.get("symbol", "")).upper() for r in rows}
        return symbol.upper() in syms
    except Exception:
        return None


def squeeze_block(symbol: str) -> tuple[bool, str]:
    """Squeeze guard — FAILS CLOSED.

    Returns (blocked, reason). blocked=True means DO NOT short.
    Blocks if SI%>20 OR DTC>5 OR earnings<=3d. CRITICALLY: if BOTH the SI% and DTC
    probes return None (no squeeze data at all), we BLOCK — missing data on the
    single largest tail risk of an unbounded-loss trade fails closed, the opposite
    of the long-side fail-open policy.
    """
    dtc, dtc_src = _fetch_dtc(symbol)
    earn = _earnings_within(symbol, SQUEEZE_EARNINGS_DAYS)

    # FAIL-CLOSED on EITHER required input being unfetchable. Both come from paid
    # feeds (Polygon DTC, Finnhub earnings) with NO free-source fallback, so a
    # None can never be a throttled-empty masquerading as clean — it always means
    # "couldn't verify", and we never short an unbounded-risk trade unverified.
    if dtc is None:
        return True, "SHORT REFUSED (days-to-cover unavailable from Polygon — fail-closed)"
    if earn is None:
        return True, "SHORT REFUSED (earnings calendar unavailable from Finnhub — fail-closed)"

    if dtc > SQUEEZE_DTC_MAX:
        return True, f"SHORT REFUSED (days-to-cover {dtc:.1f} > {SQUEEZE_DTC_MAX:.0f} [polygon])"
    if earn is True:
        return True, f"SHORT REFUSED (earnings within {SQUEEZE_EARNINGS_DAYS}d — gap risk [finnhub])"

    return False, f"squeeze-clear (DTC {dtc:.1f}[polygon], earnings clear[finnhub])"


def aggregate_short_room(open_short_value: float, book_value: float) -> tuple[float, bool]:
    """Remaining short capacity under the 20%-of-book aggregate cap.

    Returns (room_dollars, at_cap). at_cap=True means no new short may open.
    """
    cap = book_value * SHORT_MAX_AGGREGATE_PCT
    room = max(0.0, cap - open_short_value)
    return round(room, 2), (room <= 0)
