"""
Earnings-event stop guard (Plan B).

Prevents a fixed share stop from firing inside a confirmed binary earnings window,
where pre-print volatility hits the stop and the post-print gap is missed.
Risk into a binary event is controlled by SIZE, not by a tight stop.

MU test case:
  entry=$1051, em=14%, em_low=$1051*(1−1.25*0.14)=$867
  normal stop=$991 → WIDEN_TO_EM stop=min($991,$867)=$867
  $991 wick does NOT trigger. Position holds through; captures +16% gap.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from enum import Enum
from zoneinfo import ZoneInfo

from engine.earnings_confirm import confirm_earnings, Confidence

logger = logging.getLogger(__name__)

DB = "data/trader.db"
_audit_table_ready = False


class EarningsPolicy(str, Enum):
    HOLD_THROUGH = "hold_through"
    WIDEN_TO_EM  = "widen_to_em"
    CLOSE_BEFORE = "close_before"
    ALERT_ONLY   = "alert_only"


GUARD_CONFIG: dict = {
    "policy":             EarningsPolicy.WIDEN_TO_EM,
    "window_hours":       24,    # legacy — superseded by _WINDOW per-session below
    "em_multiplier":      1.25,
    "max_event_loss_pct": 0.06,
    "alert_lead_hours":   18,
    "require_confirmed":  True,
    "em_floor":           0.10,   # fallback EM if all tiers fail
}

# ---------------------------------------------------------------------------
# Session-aware window anchoring (ET → UTC, DST-correct via zoneinfo)
# ---------------------------------------------------------------------------

ET = ZoneInfo("America/New_York")

# Canonical ET report time per session.  AMC at 16:30 (after close), BMO at 07:00.
_ANCHOR_ET: dict[str, tuple[int, int]] = {"bmo": (7, 0), "amc": (16, 30)}
_DEFAULT_ANCHOR_ET: tuple[int, int] = (12, 0)   # unknown session → midday (fail-safe wide)

# (pre, post) active window around the anchor.
# AMC gap appears at NEXT morning's open; post-window must span it (+20h).
# BMO gap resolves same morning; 8h post is enough to re-arm by midday.
# Unknown session → widest window (over-protect, never under-protect).
_WINDOW: dict[str | None, tuple[timedelta, timedelta]] = {
    "bmo":  (timedelta(hours=2),  timedelta(hours=8)),
    "amc":  (timedelta(hours=2),  timedelta(hours=20)),
    None:   (timedelta(hours=4),  timedelta(hours=24)),
}


def _anchor_utc(date_str: str, session: str | None) -> datetime:
    """Return the ET-anchored event time as a UTC datetime (DST-correct)."""
    parsed = datetime.strptime(date_str, "%Y-%m-%d")
    hh, mm = _ANCHOR_ET.get((session or "").lower(), _DEFAULT_ANCHOR_ET)
    return datetime(
        parsed.year, parsed.month, parsed.day, hh, mm, tzinfo=ET
    ).astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def _ensure_audit_table() -> None:
    global _audit_table_ready
    if _audit_table_ready:
        return
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS earnings_guard_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                logged_at        TEXT    NOT NULL,
                player_id        TEXT,
                symbol           TEXT    NOT NULL,
                action           TEXT    NOT NULL,
                policy           TEXT,
                normal_stop_pct  REAL,
                new_stop_pct     REAL,
                em               REAL,
                event_time       TEXT,
                reason           TEXT
            )
        """)
        conn.commit()
        conn.close()
        _audit_table_ready = True
    except Exception as exc:
        logger.warning("earnings_guard: audit table setup failed: %s", exc)


def _log_guard_action(
    player_id: str,
    symbol: str,
    action: str,
    *,
    policy: str | None = None,
    normal_stop_pct: float | None = None,
    new_stop_pct: float | None = None,
    em: float | None = None,
    event_time: str | None = None,
    reason: str | None = None,
) -> None:
    _ensure_audit_table()
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.execute(
            """INSERT INTO earnings_guard_log
               (logged_at, player_id, symbol, action, policy,
                normal_stop_pct, new_stop_pct, em, event_time, reason)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                player_id, symbol, action, policy,
                normal_stop_pct, new_stop_pct, em, event_time, reason,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("earnings_guard_log write failed for %s: %s", symbol, exc)


# ---------------------------------------------------------------------------
# Earnings data
# ---------------------------------------------------------------------------

def get_next_earnings_for_guard(symbol: str) -> dict | None:
    """
    Return next confirmed earnings event dict or None.

    Uses fetch_earnings() (6h cache). Confirmed detection: Yahoo returns a
    single-element earningsDate list when the date is officially announced;
    two elements indicate an estimated range.
    """
    from engine.earnings_calendar import fetch_earnings
    events = fetch_earnings([symbol])
    for ev in events:
        if ev["symbol"].upper() == symbol.upper():
            try:
                earn_dt = datetime.strptime(ev["date"], "%Y-%m-%d").replace(
                    hour=20, minute=0, tzinfo=timezone.utc
                )
                return {
                    "when":      earn_dt,
                    "session":   "amc",
                    "confirmed": bool(ev.get("confirmed", False)),
                }
            except ValueError:
                return None
    return None


# ---------------------------------------------------------------------------
# Expected-move estimation (tiered fallback)
# ---------------------------------------------------------------------------

def _atm_straddle_em(symbol: str) -> float | None:
    """Tier 1: live ATM straddle / spot. Deferred until UW integration (spec A)."""
    return None


def _iv_implied_em(symbol: str) -> float | None:
    """Tier 2: front-month IV * sqrt(dte/365)."""
    try:
        from engine.market_data import get_iv_for_symbol  # type: ignore[import]
        iv, dte = get_iv_for_symbol(symbol)
        if iv and dte and dte > 0:
            return float(iv) * (dte / 365) ** 0.5
    except Exception:
        pass
    return None


def historical_earnings_em(symbol: str, k: int = 8) -> float | None:
    """
    Tier 3: average absolute price move over last K earnings prints.

    Uses get_chart_earnings_dates() for past dates and Polygon daily bars to
    measure |next_open - prev_close| / prev_close for each event.
    """
    try:
        import pandas as pd  # noqa: F401 (validate availability early)
        from engine.earnings_calendar import get_chart_earnings_dates
        from engine.market_data import get_polygon_bars

        dates_info = get_chart_earnings_dates(symbol)
        past_dates = sorted(d["date"] for d in dates_info if d["type"] == "past")[-k:]
        if len(past_dates) < 2:
            return None

        df = get_polygon_bars(symbol, timeframe="1Day", days=730)
        if df is None or (hasattr(df, "empty") and df.empty):
            return None

        moves = []
        for ds in past_dates:
            try:
                earn_ts = pd.Timestamp(ds)
                prev = df[df.index < earn_ts]
                nxt  = df[df.index >= earn_ts]
                if prev.empty or nxt.empty:
                    continue
                pre_close  = float(prev.iloc[-1]["Close"])
                post_open  = float(nxt.iloc[0].get("Open", nxt.iloc[0]["Close"]))
                if pre_close > 0:
                    moves.append(abs(post_open - pre_close) / pre_close)
            except Exception:
                continue

        return round(sum(moves) / len(moves), 4) if moves else None
    except Exception as exc:
        logger.debug("historical_earnings_em(%s): %s", symbol, exc)
        return None


def expected_move(symbol: str, spot: float) -> float:  # noqa: ARG001 (spot reserved)
    """Tiered fallback: ATM straddle → IV-implied → historical avg |move|."""
    em = _atm_straddle_em(symbol)
    if em is None:
        em = _iv_implied_em(symbol)
    if em is None:
        em = historical_earnings_em(symbol)
    if em is None:
        em = GUARD_CONFIG["em_floor"]
        logger.info("[EG] %s: no EM source; using %.0f%% floor", symbol, em * 100)
    return em


# ---------------------------------------------------------------------------
# Window check
# ---------------------------------------------------------------------------

def in_earnings_window(symbol: str, now: datetime | None = None) -> dict | None:
    """
    Return earnings event dict if symbol is inside the session-aware guard window.

    Window is anchored in ET by session (BMO=07:00, AMC=16:30, unknown=12:00),
    then converted to UTC via zoneinfo — DST-correct automatically.

    Fail-safe: unknown session uses the WIDEST window (over-protect, never under).
    The returned dict carries ``confidence`` and ``sources`` so guard_stop can
    apply the fail-safe rule (ESTIMATED → alert_only, stop kept).
    """
    now = now or datetime.now(timezone.utc)
    result = confirm_earnings(symbol)
    if not result:
        return None
    try:
        sess = (result.get("session") or "").lower() or None
        anchor = _anchor_utc(result["date"], sess)
    except (ValueError, KeyError):
        return None
    pre, post = _WINDOW.get(sess, _WINDOW[None])
    if not (anchor - pre <= now <= anchor + post):
        return None
    return {
        "when":        anchor,
        "session":     sess,
        "anchor_utc":  anchor,
        "confidence":  result["confidence"],
        "sources":     result["sources"],
        "confirmed":   result["confidence"] == Confidence.CONFIRMED,
    }


# ---------------------------------------------------------------------------
# Main decision function
# ---------------------------------------------------------------------------

def guard_stop(
    pos: dict,
    model_sl_pct: float,
    equity: float,
    player_id: str = "",
    now: datetime | None = None,
) -> dict:
    """
    Route every stock stop through this before enforcement.

    pos         – position row dict; must include symbol, avg_price, qty,
                  last_price (current market price).
    model_sl_pct – normal stop-loss fraction (e.g. 0.12 for 12%).
    equity      – total account equity for size-guard math; 0 disables size guard.
    player_id   – for audit logging only.

    Returns a decision dict with keys:
      action      : "normal" | "widen_stop" | "suppress_stop" |
                    "reduce_size" | "close_before" | "alert_only"
      stop_pct    : adjusted stop fraction (widen_stop / alert_only)
      target_shares : trim target (reduce_size)
      event, em   : event dict + expected move fraction
    """
    symbol = pos.get("symbol", "")
    ev = in_earnings_window(symbol, now)
    if not ev:
        return {"action": "normal", "stop_pct": model_sl_pct}

    # Fail-safe: ESTIMATED date in window → alert only, stop is NEVER removed.
    # Protection-removal requires CONFIRMED confidence.
    if ev.get("confidence") != Confidence.CONFIRMED:
        logger.warning(
            "EARNINGS-GUARD ALERT %s: event in window but confidence=ESTIMATED "
            "(src=%s date=%s) — normal stop %.1f%% kept",
            symbol, ev.get("sources", []), ev.get("when", "?"),
            model_sl_pct * 100,
        )
        _log_guard_action(
            player_id, symbol, "alert_only",
            normal_stop_pct=model_sl_pct,
            new_stop_pct=model_sl_pct,
            event_time=ev["when"].isoformat() if "when" in ev else None,
            reason="earnings in window but UNCONFIRMED — stop kept",
        )
        return {
            "action":   "alert_only",
            "stop_pct": model_sl_pct,
            "event":    ev,
            "em":       None,
            "reason":   "earnings date in window but UNCONFIRMED — stop kept",
        }

    entry = float(pos.get("avg_price") or 0)
    spot  = float(pos.get("last_price") or pos.get("current_price") or entry)
    qty   = float(pos.get("qty") or 0)

    if entry <= 0 or spot <= 0:
        return {"action": "normal", "stop_pct": model_sl_pct}

    em              = expected_move(symbol, spot)
    em_low          = spot  * (1 - GUARD_CONFIG["em_multiplier"] * em)
    normal_stop_price = entry * (1 - model_sl_pct)
    em_stop_pct     = max(0.0, (entry - em_low) / entry)

    # Size guard — if holding to em_low risks too much, trim; don't tighten.
    risk_ps    = max(0.0, entry - em_low)
    event_loss = risk_ps * qty
    max_loss   = GUARD_CONFIG["max_event_loss_pct"] * equity if equity > 0 else float("inf")

    if event_loss > max_loss:
        target = int(max_loss / max(risk_ps, 1e-9))
        result: dict = {
            "action":        "reduce_size",
            "target_shares": target,
            "stop_pct":      em_stop_pct,
            "reason":        (
                f"Earnings guard: event risk ${event_loss:.0f} > "
                f"max ${max_loss:.0f} at EM stop; trim to {target} shares"
            ),
            "event": ev,
            "em":    em,
        }
        _log_guard_action(
            player_id, symbol, "reduce_size",
            normal_stop_pct=model_sl_pct,
            new_stop_pct=em_stop_pct,
            em=em,
            event_time=ev["when"].isoformat(),
            reason=result["reason"],
        )
        logger.warning(
            "EARNINGS-GUARD FIRED %s reduce_size→%d "
            "conf=%s src=%s date=%s normal_stop=%.1f%% em=%.1f%%",
            symbol, target,
            ev.get("confidence"), ev.get("sources", []),
            ev["when"].strftime("%Y-%m-%d"),
            model_sl_pct * 100, em * 100,
        )
        return result

    p = GUARD_CONFIG["policy"]

    if p == EarningsPolicy.HOLD_THROUGH:
        result = {"action": "suppress_stop", "stop_pct": None, "event": ev, "em": em}
    elif p == EarningsPolicy.WIDEN_TO_EM:
        # Widen stop to max(normal stop, EM band low) expressed as fraction from entry.
        widened_price = min(normal_stop_price, em_low)
        widened_pct   = max(0.0, (entry - widened_price) / entry)
        result = {"action": "widen_stop", "stop_pct": widened_pct, "event": ev, "em": em}
    elif p == EarningsPolicy.CLOSE_BEFORE:
        result = {"action": "close_before", "event": ev, "em": em}
    else:
        result = {"action": "alert_only", "stop_pct": model_sl_pct, "event": ev, "em": em}

    _log_guard_action(
        player_id, symbol, result["action"],
        policy=str(p),
        normal_stop_pct=model_sl_pct,
        new_stop_pct=result.get("stop_pct"),
        em=em,
        event_time=ev["when"].isoformat(),
    )
    new_stop_label = (
        f"{result.get('stop_pct', 0) * 100:.1f}%"
        if result.get("stop_pct") is not None else "suppressed"
    )
    logger.warning(
        "EARNINGS-GUARD FIRED %s %s | "
        "conf=%s src=%s date=%s normal_stop=%.1f%% em=%.1f%% new_stop=%s",
        symbol, result["action"],
        ev.get("confidence"), ev.get("sources", []),
        ev["when"].strftime("%Y-%m-%d"),
        model_sl_pct * 100, em * 100, new_stop_label,
    )
    return result
