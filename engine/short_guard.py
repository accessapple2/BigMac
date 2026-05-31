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

# Finviz Elite SI%-of-float cache: {symbol: (mono_ts, value|None)}. SI updates
# ~2×/month; 12h TTL. A cached None still means "Elite gave no value" (degrade-skip),
# NOT a free-source clean read.
_elite_si_cache: dict = {}
_ELITE_SI_TTL = 12 * 3600  # 12h
_elite_session = None         # requests.Session with .ASPXAUTH cookie, lazily logged-in
_elite_login_attempted = False


def _now_mono() -> float:
    return _time.monotonic()


def _get_elite_session():
    """Authenticated Finviz Elite session (.ASPXAUTH cookie). None if login fails.

    HM-FINVIZ-ELITE-AUTH 2026-05-30: replaces the finvizfinance FREE SCRAPE with the
    PAID Elite export. Probed schema: export.ashx?v=131 returns 'Short Float' (%-of-
    float) + 'Short Ratio' (DTC). Credentials FINVIZ_EMAIL/FINVIZ_PASSWORD from .env
    (gitignored). One login per process; session reused. A login failure returns None
    → SI% gate degrades to SKIP (Option B) — DTC + earnings still enforced.
    """
    global _elite_session, _elite_login_attempted
    if _elite_session is not None:
        return _elite_session
    if _elite_login_attempted:
        return None  # already tried this process; don't hammer login on every call
    _elite_login_attempted = True
    try:
        import os
        import requests
        from dotenv import load_dotenv
        load_dotenv("/Users/bigmac/autonomous-trader/.env")
        email = os.getenv("FINVIZ_EMAIL")
        pw = os.getenv("FINVIZ_PASSWORD")
        if not email or not pw:
            return None
        s = requests.Session()
        s.headers["User-Agent"] = "Mozilla/5.0"
        s.post("https://finviz.com/login_submit.ashx",
               data={"email": email, "password": pw}, timeout=12)
        if ".ASPXAUTH" not in s.cookies:
            return None  # login did not authenticate → treat as Elite-down
        _elite_session = s
        return s
    except Exception:
        return None


def _pct_to_float(val):
    """'14.17%' -> 14.17 ; '-' / '' / None -> None."""
    if val is None:
        return None
    s = str(val).replace("%", "").strip()
    if not s or s in ("-", "—"):
        return None
    try:
        return float(s)
    except Exception:
        return None


def _fetch_si_pct_elite(symbol: str) -> tuple[float | None, str]:
    """SI %-of-float from AUTHENTICATED Finviz Elite export. (value, source).

    Returns (float, "finviz-elite") on success; (None, "elite-unavailable") when
    Elite login/fetch fails or the column is empty. The None branch is the Option-B
    degrade signal — the caller SKIPS the SI% gate but STILL enforces DTC + earnings.
    NEVER falls to finvizfinance free-scrape or yfinance.
    """
    now = _now_mono()
    cached = _elite_si_cache.get(symbol)
    if cached and (now - cached[0]) < _ELITE_SI_TTL:
        return cached[1], ("finviz-elite" if cached[1] is not None else "elite-unavailable")
    sess = _get_elite_session()
    if sess is None:
        return None, "elite-unavailable"
    try:
        import csv as _csv
        import io as _io
        r = sess.get(f"https://elite.finviz.com/export.ashx?v=131&t={symbol.upper()}", timeout=12)
        if r.status_code != 200:
            return None, "elite-unavailable"
        rows = list(_csv.DictReader(_io.StringIO(r.text)))
        si = None
        for row in rows:
            if str(row.get("Ticker", "")).upper() == symbol.upper():
                si = _pct_to_float(row.get("Short Float"))
                break
        _elite_si_cache[symbol] = (now, si)
        return si, ("finviz-elite" if si is not None else "elite-unavailable")
    except Exception:
        return None, "elite-unavailable"


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
    """Squeeze guard — three gates, two fail-CLOSED + one degrade-to-skip (Option B).

    Returns (blocked, reason). blocked=True means DO NOT short.

    Gates:
      1. DTC > 5         [Polygon]  — ALWAYS enforced; fail-CLOSED (None → block)
      2. earnings ≤ 3d   [Finnhub]  — ALWAYS enforced; fail-CLOSED (None → block)
      3. SI% > 20        [Finviz Elite, authed] — layered ON TOP. When Elite is UP:
         block if SI%>20 (3-gate mode). When Elite is DOWN: SKIP this gate only —
         DTC + earnings still enforced (2-gate degraded mode), made VISIBLE in the
         reason string. NEVER 0 gates; NEVER falls to a free/empty source.

    The reason string tags which feeds ran so the degrade is auditable, e.g.
    "[finviz-elite SI …, polygon DTC …, finnhub earn] (3 gates)" vs
    "[polygon DTC …, finnhub earn — Elite unavailable, SI% gate skipped] (2 gates,
    degraded)".
    """
    dtc, dtc_src = _fetch_dtc(symbol)
    earn = _earnings_within(symbol, SQUEEZE_EARNINGS_DAYS)
    si, si_src = _fetch_si_pct_elite(symbol)

    # ── DTC + earnings: ALWAYS fail-CLOSED (Polygon / Finnhub paid feeds, no free
    # fallback). A None always means "couldn't verify" → refuse. These two gates
    # NEVER degrade-to-skip. ──
    if dtc is None:
        return True, "SHORT REFUSED (days-to-cover unavailable from Polygon — fail-closed)"
    if earn is None:
        return True, "SHORT REFUSED (earnings calendar unavailable from Finnhub — fail-closed)"
    if dtc > SQUEEZE_DTC_MAX:
        return True, f"SHORT REFUSED (days-to-cover {dtc:.1f} > {SQUEEZE_DTC_MAX:.0f} [polygon])"
    if earn is True:
        return True, f"SHORT REFUSED (earnings within {SQUEEZE_EARNINGS_DAYS}d — gap risk [finnhub])"

    # ── SI%-of-float: the THIRD gate, layered on top. HM-FINVIZ-ELITE-AUTH Option B:
    # when Elite returns a value → block if > 20% (3-gate mode). When Elite is
    # unavailable → SKIP this gate only (DTC + earnings above already passed), and
    # make the degrade VISIBLE in the reason. This is the ONLY gate that degrades;
    # it NEVER falls to a free source. The bounded accepted risk: a name that passes
    # DTC+earnings where SI% would have been the sole blocker gets through during an
    # Elite outage. ──
    if si is not None:
        if si > SQUEEZE_SI_PCT_MAX:
            return True, f"SHORT REFUSED (SI {si:.1f}% > {SQUEEZE_SI_PCT_MAX:.0f}% [finviz-elite])"
        return False, (f"squeeze-clear [finviz-elite SI {si:.1f}%, polygon DTC {dtc:.1f}, "
                       f"finnhub earn] (3 gates)")
    # Elite unavailable → degrade to DTC+earnings (2 gates), made explicit.
    return False, (f"squeeze-clear [polygon DTC {dtc:.1f}, finnhub earn — Elite unavailable, "
                   f"SI% gate skipped] (2 gates, degraded)")


def aggregate_short_room(open_short_value: float, book_value: float) -> tuple[float, bool]:
    """Remaining short capacity under the 20%-of-book aggregate cap.

    Returns (room_dollars, at_cap). at_cap=True means no new short may open.
    """
    cap = book_value * SHORT_MAX_AGGREGATE_PCT
    room = max(0.0, cap - open_short_value)
    return round(room, 2), (room <= 0)
