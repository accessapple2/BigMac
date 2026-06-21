#!/usr/bin/env python3
"""
bk_orb_scanner.py — HM-BK-A Opening-Range Breakout confirmatory scanner (intraday).

PURPOSE: confirm intraday strength when a name breaks its opening range on volume.
CONFIRMATORY-ONLY — never originates a trade; emits a BULL (or optional BEAR) vote
that counts only when the fleet already has >= MIN_FLEET_VOTES independent
directional votes (8a83f17 FRED-BANKRATE rail, reused exactly). Independent of the
nightly daily-OHLCV scanners B/C (this is the intraday minute path).

Detection (tunable):
  - Opening range = first OR_MINUTES of the session (default 15 = 09:30-09:45 ET).
    Configurable via ORB_MINUTES env / config.
  - OR_high / OR_low captured at the end of the OR window per symbol.
  - LONG trigger: a 1-min close breaks above OR_high after the window closes, with
    CUMULATIVE session volume at the break >= VOL_MULT (1.5x) the MEDIAN first-window
    volume over the trailing TRAIL_SESSIONS (20) sessions.
  - SHORT mirror (close < OR_low) is OFF by default (ORB_SHORT_ENABLED).
  - One signal per symbol per day (first valid break only).

Data: Polygon minute aggregates (Stocks Starter, paid/active) via a dedicated
paginated fetch (get_intraday_candles caps at 500 bars — too few for the trailing
baseline). Falls back to engine.market_data.get_intraday_candles when no key.

Doctrine: is_trigger hardcoded False; default OFF via ORB_CONFIRMATORY_VOTE_ENABLED;
rows written, never deleted. Intraday => bounded universe (ORB_UNIVERSE_SIZE).
"""

from __future__ import annotations

import os
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    _ET = timezone.utc

try:
    from rich.console import Console
    console = Console()
except Exception:  # pragma: no cover
    class _Stub:
        def log(self, *a, **k):
            print(*a)
    console = _Stub()

_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

# ─── Tunable defaults ────────────────────────────────────────────────────────
OR_MINUTES = int(os.environ.get("ORB_MINUTES", 15))   # opening-range window
VOL_MULT = 1.5                                         # cum vol vs trailing median
TRAIL_SESSIONS = 20                                    # trailing baseline window
SESSION_OPEN = (9, 30)
SESSION_CLOSE = (16, 0)
UNIVERSE_SIZE = int(os.environ.get("ORB_UNIVERSE_SIZE", 150))  # bounded (intraday)
FETCH_DAYS = 40                                        # calendar days of minute history

MIN_FLEET_VOTES = 2
_ntfy_fired_classes: set[str] = set()


# ─── DB ──────────────────────────────────────────────────────────────────────

def _conn(db_path: str | None = None) -> sqlite3.Connection:
    c = sqlite3.connect(db_path or _DB_PATH, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _ensure_schema(conn: sqlite3.Connection) -> None:
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bk_orb_signals (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL,
                session_date    TEXT NOT NULL,
                or_high         REAL,
                or_low          REAL,
                break_price     REAL,
                break_time      TEXT,
                vol_mult        REAL,
                signal          TEXT NOT NULL,
                created_at      TEXT NOT NULL,
                UNIQUE(symbol, session_date, signal)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bk_orb_session "
            "ON bk_orb_signals(session_date DESC)"
        )
        conn.commit()
    except Exception as e:
        console.log(f"[yellow]bk_orb: _ensure_schema: {type(e).__name__}: {e!r}")


def _excluded_symbols() -> frozenset[str]:
    """Leveraged/inverse ETFs to skip for ORB (config-driven, ORB-only)."""
    try:
        from config import ORB_EXCLUDE_LEVERAGED_INVERSE
        return ORB_EXCLUDE_LEVERAGED_INVERSE
    except Exception:
        return frozenset()


def _load_universe(db_path: str | None = None, limit: int = UNIVERSE_SIZE) -> list[str]:
    # The leveraged/inverse ETFs we exclude cluster at the TOP of the avg_volume
    # ordering, so fetch with headroom and filter BEFORE truncating to `limit` —
    # otherwise the exclusion would just shrink the effective universe instead of
    # backfilling with the next normal liquid names.
    excluded = _excluded_symbols()
    try:
        conn = _conn(db_path)
        try:
            rows = conn.execute(
                "SELECT symbol FROM scan_universe ORDER BY avg_volume DESC LIMIT ?",
                (limit + len(excluded) + 32,),
            ).fetchall()
            syms = [r["symbol"] for r in rows if r["symbol"]
                    and r["symbol"].upper() not in excluded]
            return syms[:limit]
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]bk_orb: universe load failed: {type(e).__name__}: {e!r}")
        return []


def _already_fired(symbol: str, session_date: str, db_path: str | None = None) -> bool:
    try:
        conn = _conn(db_path)
        try:
            _ensure_schema(conn)
            return conn.execute(
                "SELECT 1 FROM bk_orb_signals WHERE symbol=? AND session_date=? LIMIT 1",
                (symbol, session_date),
            ).fetchone() is not None
        finally:
            conn.close()
    except Exception:
        return False


# ─── Minute fetch (paginated Polygon; cascade fallback) ──────────────────────

def _fetch_minutes(symbol: str, days: int = FETCH_DAYS) -> list[dict]:
    """Ascending 1-min bars [{ts_ms, et, o,h,l,c,v}] over the last `days` calendar
    days. Polygon aggs (limit 50000 + next_url pagination). Falls back to the shared
    get_intraday_candles cascade (≤500 bars) if no Polygon key."""
    key = os.environ.get("POLYGON_API_KEY", "")
    if key:
        try:
            return _fetch_minutes_polygon(symbol, days, key)
        except Exception as e:
            console.log(f"[yellow]bk_orb: polygon minutes {symbol}: {type(e).__name__}: {e!r}")
    # fallback (limited history — trailing baseline may be short)
    try:
        from engine.market_data import get_intraday_candles
        rows = get_intraday_candles(symbol, interval="1m", range_="5d") or []
        out = []
        for r in rows:
            dt = datetime.fromisoformat(r["time"].replace("Z", "+00:00"))
            out.append({"ts_ms": int(dt.timestamp() * 1000), "et": dt.astimezone(_ET),
                        "o": r["open"], "h": r["high"], "l": r["low"],
                        "c": r["close"], "v": int(r["volume"])})
        out.sort(key=lambda b: b["ts_ms"])
        return out
    except Exception as e:
        console.log(f"[yellow]bk_orb: fallback minutes {symbol}: {type(e).__name__}: {e!r}")
        return []


def _fetch_minutes_polygon(symbol: str, days: int, key: str) -> list[dict]:
    import requests
    from datetime import timedelta
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{symbol.upper()}"
        f"/range/1/minute/{start.isoformat()}/{end.isoformat()}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={key}"
    )
    out: list[dict] = []
    pages = 0
    while url and pages < 10:
        r = requests.get(url, timeout=8)
        if r.status_code != 200:
            raise RuntimeError(f"Polygon HTTP {r.status_code}")
        data = r.json()
        for row in data.get("results", []) or []:
            ts = row.get("t")
            if ts is None:
                continue
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            out.append({"ts_ms": ts, "et": dt.astimezone(_ET),
                        "o": float(row.get("o", 0)), "h": float(row.get("h", 0)),
                        "l": float(row.get("l", 0)), "c": float(row.get("c", 0)),
                        "v": int(row.get("v", 0) or 0)})
        nxt = data.get("next_url")
        url = (nxt + f"&apiKey={key}") if nxt else None
        pages += 1
    out.sort(key=lambda b: b["ts_ms"])
    return out


def _group_sessions(bars: list[dict]) -> dict[str, list[dict]]:
    """Ordered {session_date(ET): [RTH bars]}. Regular hours 09:30–16:00 ET only."""
    sessions: dict[str, list[dict]] = {}
    for b in bars:
        et = b["et"]
        mins = et.hour * 60 + et.minute
        if mins < SESSION_OPEN[0] * 60 + SESSION_OPEN[1]:
            continue
        if mins >= SESSION_CLOSE[0] * 60 + SESSION_CLOSE[1]:
            continue
        sessions.setdefault(et.date().isoformat(), []).append(b)
    return dict(sorted(sessions.items()))


def _or_window(session_bars: list[dict], or_minutes: int):
    """(or_high, or_low, or_window_vol, post_bars) for one session."""
    open_min = SESSION_OPEN[0] * 60 + SESSION_OPEN[1]
    or_bars, post_bars = [], []
    for b in session_bars:
        mins = b["et"].hour * 60 + b["et"].minute
        if mins < open_min + or_minutes:
            or_bars.append(b)
        else:
            post_bars.append(b)
    if not or_bars:
        return None
    or_high = max(b["h"] for b in or_bars)
    or_low = min(b["l"] for b in or_bars)
    or_vol = sum(b["v"] for b in or_bars)
    return or_high, or_low, or_vol, post_bars


# ─── Detection ───────────────────────────────────────────────────────────────

def detect_session(symbol: str, session_date: str, session_bars: list[dict],
                   median_first_vol: float | None, or_minutes: int = OR_MINUTES,
                   short_enabled: bool | None = None) -> dict | None:
    """First valid OR break for one session, or None."""
    if short_enabled is None:
        try:
            from config import ORB_SHORT_ENABLED as _se
        except Exception:
            _se = False
        short_enabled = bool(_se)

    win = _or_window(session_bars, or_minutes)
    if win is None or not median_first_vol:
        return None
    or_high, or_low, or_vol, post_bars = win
    if not post_bars:
        return None

    cum = or_vol  # cumulative session volume through the OR window
    for b in post_bars:
        cum += b["v"]
        vm = cum / median_first_vol if median_first_vol else 0.0
        if b["c"] > or_high and vm >= VOL_MULT:
            return _row(symbol, session_date, or_high, or_low, b["c"], b["et"], vm, "BULL")
        if short_enabled and b["c"] < or_low and vm >= VOL_MULT:
            return _row(symbol, session_date, or_high, or_low, b["c"], b["et"], vm, "BEAR")
    return None


def _row(symbol, session_date, or_high, or_low, break_price, break_et, vm, signal) -> dict:
    return {
        "symbol": symbol, "session_date": session_date,
        "or_high": round(or_high, 4), "or_low": round(or_low, 4),
        "break_price": round(break_price, 4),
        "break_time": break_et.isoformat() if hasattr(break_et, "isoformat") else str(break_et),
        "vol_mult": round(vm, 2), "signal": signal,
    }


def _median_first_vol(sessions: dict[str, list[dict]], before_date: str,
                      or_minutes: int) -> tuple[float | None, int]:
    """Median OR-window volume over up to TRAIL_SESSIONS sessions before `before_date`.
    Returns (median, n_sessions_used)."""
    vols = []
    for d, bars in sessions.items():
        if d >= before_date:
            continue
        win = _or_window(bars, or_minutes)
        if win:
            vols.append(win[2])
    vols = vols[-TRAIL_SESSIONS:]
    if not vols:
        return None, 0
    return statistics.median(vols), len(vols)


# ─── Confirmatory-only convergence contract (8a83f17 rail) ───────────────────

def confirmatory_vote(fleet_directional_votes: int, signal: str | None) -> dict:
    is_directional = signal in ("BULL", "BEAR")
    is_sole_voter = fleet_directional_votes < MIN_FLEET_VOTES
    counts = is_directional and not is_sole_voter

    trade_permitted_on_orb_alone = False
    assert not (is_sole_voter and counts), (
        "ORB is confirmatory-only: the sole voter must never count toward a "
        "trade (MIN_FLEET_VOTES=%d not met)" % MIN_FLEET_VOTES
    )
    assert trade_permitted_on_orb_alone is False

    return {
        "source": "bk_orb",
        "signal": signal,
        "direction": ("BULLISH" if signal == "BULL"
                      else "BEARISH" if signal == "BEAR" else "NEUTRAL"),
        "counts_toward_convergence": counts,
        "is_sole_voter": is_sole_voter,
        "fleet_directional_votes": fleet_directional_votes,
        "min_fleet_votes_required": MIN_FLEET_VOTES,
        "trade_permitted_on_orb_alone": trade_permitted_on_orb_alone,
        "is_trigger": False,
    }


# ─── UHURA market-level confirmatory vote ────────────────────────────────────
# Weight 1.0 = TECHNICAL-CONFIRMATORY class (vs FRED's 0.5 macro-context).
CONFIRMATORY_WEIGHT = 1.0


def _fresh_rows(db_path: str | None = None, watchlist: list[str] | None = None) -> list[dict]:
    """TODAY's (ET) ORB rows — intraday freshness."""
    today_et = datetime.now(_ET).date().isoformat()
    try:
        conn = _conn(db_path)
        try:
            _ensure_schema(conn)
            rows = [dict(r) for r in conn.execute(
                "SELECT symbol, signal, vol_mult, session_date "
                "FROM bk_orb_signals WHERE session_date=?", (today_et,)).fetchall()]
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]bk_orb: _fresh_rows: {type(e).__name__}: {e!r}")
        return []
    if watchlist:
        wl = set(watchlist)
        rows = [r for r in rows if r["symbol"] in wl]
    return rows


def market_vote(watchlist: list[str] | None = None, db_path: str | None = None,
                enabled: bool | None = None) -> dict | None:
    """Aggregate today's ORB breakouts into ONE market-level confirmatory lean,
    or None to ABSTAIN. Long-only default -> BULLISH bias."""
    if enabled is None:
        try:
            from config import ORB_CONFIRMATORY_VOTE_ENABLED as enabled
        except Exception:
            enabled = False
    if not enabled:
        return None
    rows = _fresh_rows(db_path, watchlist)
    if not rows:
        return None
    assert confirmatory_vote(0, "BULL")["counts_toward_convergence"] is False

    bull = [r for r in rows if r["signal"] == "BULL"]
    bear = [r for r in rows if r["signal"] == "BEAR"]
    if len(bull) > len(bear):
        direction, n, ex = "BULLISH", len(bull), bull[0]["symbol"]
    elif len(bear) > len(bull):
        direction, n, ex = "BEARISH", len(bear), bear[0]["symbol"]
    else:
        direction, n, ex = "NEUTRAL", 0, rows[0]["symbol"]
    reasoning = (f"[confirm] {len(rows)} ORB break today "
                 f"({len(bull)} up / {len(bear)} down) e.g. {ex}")
    return {"direction": direction, "weight": CONFIRMATORY_WEIGHT,
            "reasoning": reasoning, "n": n, "bull": len(bull), "bear": len(bear)}


# ─── NTFY (shadow only) ──────────────────────────────────────────────────────

def _fire_ntfy(sig: dict) -> bool:
    key = f"orb::{sig['symbol']}::{sig['session_date']}"
    if key in _ntfy_fired_classes:
        return False
    _ntfy_fired_classes.add(key)
    try:
        from engine.alert_channels import send_alert
        arrow = "🟢🚀" if sig["signal"] == "BULL" else "🔴🪂"
        send_alert(
            level="info",
            alert_type=f"bk_orb_{sig['signal'].lower()}_{sig['symbol']}",
            message=(
                f"{arrow} ORB {sig['signal']} (shadow) — {sig['symbol']} broke "
                f"{'OR_high ' + str(sig['or_high']) if sig['signal']=='BULL' else 'OR_low ' + str(sig['or_low'])} "
                f"@ {sig['break_price']} ({sig['break_time']}), vol {sig['vol_mult']}×. "
                f"Confirmatory-only — never originates."
            ),
            title=f"{arrow} ORB {sig['signal']} — {sig['symbol']}",
            audience="admin",
            rate_limit_secs=86400,
        )
        return True
    except Exception as e:
        console.log(f"[yellow]bk_orb: NTFY {sig.get('symbol')} failed: {type(e).__name__}: {e!r}")
        return False


# ─── Persistence ─────────────────────────────────────────────────────────────

def _persist(signals: list[dict], db_path: str | None = None) -> int:
    if not signals:
        return 0
    conn = _conn(db_path)
    try:
        _ensure_schema(conn)
        now = datetime.now(timezone.utc).isoformat()
        n = 0
        for s in signals:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO bk_orb_signals "
                    "(symbol, session_date, or_high, or_low, break_price, break_time, "
                    "vol_mult, signal, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (s["symbol"], s["session_date"], s["or_high"], s["or_low"],
                     s["break_price"], s["break_time"], s["vol_mult"], s["signal"], now),
                )
                n += conn.total_changes and 1 or 0
            except Exception:
                pass
        conn.commit()
        return len(signals)
    finally:
        conn.close()


# ─── Orchestrator ────────────────────────────────────────────────────────────

def run_scan(universe: list[str] | None = None, persist: bool = True,
             shadow_ntfy: bool | None = None, db_path: str | None = None) -> dict:
    """Intraday scan for TODAY's session. Fires once per symbol/day. Bounded universe."""
    if shadow_ntfy is None:
        try:
            from config import ORB_CONFIRMATORY_VOTE_ENABLED as _en
        except Exception:
            _en = False
        shadow_ntfy = bool(_en)

    syms = universe if universe is not None else _load_universe(db_path)
    if not syms:
        return {"scanned": 0, "signals": 0, "bull": 0, "bear": 0}

    today_et = datetime.now(_ET).date().isoformat()
    sigs: list[dict] = []
    for sym in syms:
        if _already_fired(sym, today_et, db_path):
            continue
        try:
            bars = _fetch_minutes(sym)
            sessions = _group_sessions(bars)
            if today_et not in sessions:
                continue
            med, _ = _median_first_vol(sessions, today_et, OR_MINUTES)
            sig = detect_session(sym, today_et, sessions[today_et], med, OR_MINUTES)
            if sig:
                sigs.append(sig)
        except Exception as e:
            console.log(f"[yellow]bk_orb: scan {sym}: {type(e).__name__}: {e!r}")

    if persist:
        _persist(sigs, db_path)
    if shadow_ntfy:
        for s in sigs:
            _fire_ntfy(s)

    # [OBS] HM-EXEC-PIPELINE measurement hook — pure side-effect, never raises
    try:
        from engine.signal_observation import emit_observation
        for _s in sigs:
            emit_observation(
                source="bk_orb",
                ticker=_s["symbol"],
                direction=_s["signal"],
                conviction=f"vol_mult={_s.get('vol_mult', 0):.1f}x",
                confluence_meta={
                    "or_high": _s.get("or_high"),
                    "or_low": _s.get("or_low"),
                    "break_price": _s.get("break_price"),
                    "session_date": _s.get("session_date"),
                },
            )
    except Exception:
        pass

    bull = sum(1 for s in sigs if s["signal"] == "BULL")
    bear = sum(1 for s in sigs if s["signal"] == "BEAR")
    return {"scanned": len(syms), "signals": len(sigs), "bull": bull, "bear": bear, "rows": sigs}


def shadow_pass(sessions_back: int = 5, universe: list[str] | None = None,
                db_path: str | None = None, max_symbols: int = 40) -> dict:
    """Replay ORB over the last `sessions_back` available sessions (no persist/ntfy).
    Reports signals/day, a sample, and intraday-history sufficiency."""
    syms = (universe if universe is not None
            else _load_universe(db_path, limit=max_symbols))[:max_symbols]
    all_sigs: list[dict] = []
    per_day: dict[str, int] = {}
    sess_counts: list[int] = []
    enough = 0
    for sym in syms:
        try:
            bars = _fetch_minutes(sym)
        except Exception:
            continue
        sessions = _group_sessions(bars)
        dates = list(sessions.keys())
        sess_counts.append(len(dates))
        for d in dates[-sessions_back:]:
            med, n_trail = _median_first_vol(sessions, d, OR_MINUTES)
            if n_trail >= TRAIL_SESSIONS:
                enough += 1
            sig = detect_session(sym, d, sessions[d], med, OR_MINUTES)
            if sig:
                all_sigs.append(sig)
                per_day[d] = per_day.get(d, 0) + 1
    return {
        "symbols": len(syms), "sessions_back": sessions_back,
        "total_signals": len(all_sigs), "per_day": dict(sorted(per_day.items())),
        "sample": all_sigs[:5],
        "sessions_per_symbol_min": min(sess_counts) if sess_counts else 0,
        "sessions_per_symbol_max": max(sess_counts) if sess_counts else 0,
        "symbol_sessions_with_full_trailing_baseline": enough,
        "trail_required": TRAIL_SESSIONS,
    }


if __name__ == "__main__":
    import json
    print(json.dumps(shadow_pass(sessions_back=5, max_symbols=20), indent=2, default=str))
