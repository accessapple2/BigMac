#!/usr/bin/env python3
"""
bk_avwap_scanner.py — HM-BK-B Anchored-VWAP confirmatory scanner.

PURPOSE: confirm strength/weakness when price reclaims or loses an event-anchored
VWAP. This is a CONFIRMATORY-ONLY signal — it NEVER originates a trade. It emits a
vote tagged BULL/BEAR that the confluence layer may count as ONE confirmatory source,
and only when the fleet has already produced >= MIN_FLEET_VOTES independent
directional votes (the 8a83f17 FRED-BANKRATE rail, reused exactly).

Doctrine (asserted in code, not assumed):
  - is_trigger is hardcoded False. Confirmatory-sole-voter -> no trade.
  - Default OFF: AVWAP_CONFIRMATORY_VOTE_ENABLED gates the live vote + the nightly
    scheduler. When False, this is shadow-only.
  - Snapshots/rows are written, never deleted (sacred-data rule).

Data: engine.market_data.get_bulk_daily_ohlcv (Alpaca IEX bulk bars, 30-min cache) —
the SAME daily-OHLCV source HM-BK-C (box scanner) uses.

Anchors per symbol:
  (a) most recent swing high      (fractal pivot, OHLCV-derived)
  (b) most recent swing low       (fractal pivot, OHLCV-derived)
  (c) last earnings date          (best-effort from earnings_calendar; degrades to skip)
  (d) last major gap day          (|gap| >= GAP_PCT, OHLCV-derived)

Signals:
  RECLAIM   (BULL)  close crosses from below to above a key aVWAP today
  LOSS      (BEAR)  close crosses from above to below
  CONFLUENCE        >= 2 aVWAPs within CONFLUENCE_BAND_PCT of each other AND price
                    interacting with that band (stronger; tagged on the cross)
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from rich.console import Console
    console = Console()
except Exception:  # pragma: no cover - rich always present in prod
    class _Stub:
        def log(self, *a, **k):
            print(*a)
    console = _Stub()

_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

# ─── Tunable defaults ────────────────────────────────────────────────────────
SWING_PIVOT_K = 3           # bars each side for a fractal swing pivot
GAP_PCT = 4.0               # |open/prev_close - 1| >= this (%) = major gap anchor
CONFLUENCE_BAND_PCT = 1.5   # aVWAPs within this % of each other = confluence band
INTERACT_PCT = 1.5          # price within this % of an aVWAP = "interacting"
LOOKBACK_RANGE = "6mo"      # OHLCV history pulled per symbol
MIN_BARS = 25               # need at least this many bars to anchor meaningfully

# Confirmatory-only rail (mirrors fred_bankrate_signal.MIN_FLEET_VOTES): the aVWAP
# lean may COUNT toward convergence only when the fleet already has >= this many
# independent directional votes. aVWAP can confirm; it can NEVER originate.
MIN_FLEET_VOTES = 2

# In-process NTFY dedupe (per-symbol-per-signal-per-process lifetime).
_ntfy_fired_classes: set[str] = set()


# ─── DB ──────────────────────────────────────────────────────────────────────

def _conn(db_path: str | None = None) -> sqlite3.Connection:
    c = sqlite3.connect(db_path or _DB_PATH, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Idempotent create of bk_avwap_signals."""
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bk_avwap_signals (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL,
                asof            TEXT NOT NULL,
                anchor_type     TEXT NOT NULL,
                signal          TEXT NOT NULL,
                avwap_price     REAL,
                close           REAL,
                confluence_n    INTEGER DEFAULT 0,
                created_at      TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bk_avwap_sym_asof "
            "ON bk_avwap_signals(symbol, asof DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bk_avwap_asof "
            "ON bk_avwap_signals(asof DESC)"
        )
        conn.commit()
    except Exception as e:
        console.log(f"[yellow]bk_avwap: _ensure_schema: {type(e).__name__}: {e!r}")


def _load_universe(db_path: str | None = None) -> list[str]:
    try:
        conn = _conn(db_path)
        try:
            rows = conn.execute("SELECT symbol FROM scan_universe").fetchall()
            return [r["symbol"] for r in rows if r["symbol"]]
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]bk_avwap: universe load failed: {type(e).__name__}: {e!r}")
        return []


# ─── Frame helpers ───────────────────────────────────────────────────────────

def _norm(df: pd.DataFrame) -> pd.DataFrame | None:
    """Normalize an OHLCV frame to lowercase open/high/low/close/volume, sorted."""
    if df is None or len(df) == 0:
        return None
    cols = {c.lower(): c for c in df.columns}
    need = ("open", "high", "low", "close", "volume")
    if not all(k in cols for k in need):
        return None
    out = pd.DataFrame({k: pd.to_numeric(df[cols[k]], errors="coerce") for k in need})
    out = out.dropna(subset=["high", "low", "close", "volume"]).copy()
    return out if len(out) >= MIN_BARS else None


def _swing_anchors(df: pd.DataFrame, k: int = SWING_PIVOT_K) -> dict[str, int]:
    """Most-recent confirmed swing-high / swing-low pivot indices (positional)."""
    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()
    n = len(df)
    hi_idx = lo_idx = None
    # iterate newest-first over confirmable pivots (need k bars of right shoulder)
    for i in range(n - 1 - k, k - 1, -1):
        win_h = highs[i - k:i + k + 1]
        win_l = lows[i - k:i + k + 1]
        if hi_idx is None and highs[i] == win_h.max():
            hi_idx = i
        if lo_idx is None and lows[i] == win_l.min():
            lo_idx = i
        if hi_idx is not None and lo_idx is not None:
            break
    out: dict[str, int] = {}
    if hi_idx is not None:
        out["swing_high"] = hi_idx
    if lo_idx is not None:
        out["swing_low"] = lo_idx
    return out


def _gap_anchor(df: pd.DataFrame, gap_pct: float = GAP_PCT) -> int | None:
    """Most-recent bar whose open gapped >= gap_pct vs prior close."""
    opens = df["open"].to_numpy()
    closes = df["close"].to_numpy()
    for i in range(len(df) - 1, 0, -1):
        prev = closes[i - 1]
        if prev and abs(opens[i] / prev - 1.0) * 100.0 >= gap_pct:
            return i
    return None


def _earnings_anchor(df: pd.DataFrame, symbol: str, db_path: str | None) -> int | None:
    """Best-effort: positional index of the last earnings date that falls inside
    the frame. Degrades to None if no earnings table/row — the scanner still runs
    on the OHLCV-derived anchors."""
    try:
        conn = _conn(db_path)
        try:
            tbls = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            row = None
            if "earnings_calendar" in tbls:
                row = conn.execute(
                    "SELECT earnings_date FROM earnings_calendar "
                    "WHERE symbol=? AND earnings_date<=? "
                    "ORDER BY earnings_date DESC LIMIT 1",
                    (symbol, str(df.index[-1].date()) if hasattr(df.index[-1], "date")
                     else str(df.index[-1])),
                ).fetchone()
            if not row:
                return None
            edate = str(row[0])[:10]
        finally:
            conn.close()
    except Exception:
        return None
    # map the earnings date to the nearest positional bar at-or-after it
    idx_dates = [str(d)[:10] for d in df.index]
    for i, d in enumerate(idx_dates):
        if d >= edate:
            return i
    return None


def _anchored_vwap(df: pd.DataFrame, anchor_idx: int) -> np.ndarray:
    """Anchored VWAP series from anchor_idx -> end. Positions < anchor are NaN."""
    tp = (df["high"].to_numpy() + df["low"].to_numpy() + df["close"].to_numpy()) / 3.0
    vol = df["volume"].to_numpy()
    n = len(df)
    out = np.full(n, np.nan)
    cum_pv = 0.0
    cum_v = 0.0
    for i in range(anchor_idx, n):
        cum_pv += tp[i] * vol[i]
        cum_v += vol[i]
        out[i] = (cum_pv / cum_v) if cum_v > 0 else np.nan
    return out


# ─── Detection ───────────────────────────────────────────────────────────────

def detect(df: pd.DataFrame, symbol: str, db_path: str | None = None) -> list[dict]:
    """Return aVWAP cross signals as-of the LAST bar of df. Each dict:
    {symbol, asof, anchor_type, signal(BULL/BEAR), avwap_price, close, confluence_n}."""
    ndf = _norm(df)
    if ndf is None:
        return []
    closes = ndf["close"].to_numpy()
    n = len(ndf)
    asof = str(ndf.index[-1])[:10] if hasattr(ndf.index[-1], "__str__") else str(n)

    anchors = dict(_swing_anchors(ndf))
    g = _gap_anchor(ndf)
    if g is not None:
        anchors["gap"] = g
    e = _earnings_anchor(ndf, symbol, db_path)
    if e is not None:
        anchors["earnings"] = e

    # today's aVWAP value per anchor (for confluence) + per-anchor cross
    avwap_today: dict[str, float] = {}
    crosses: list[tuple[str, str, float]] = []  # (anchor_type, BULL/BEAR, avwap_price)
    today_close, prev_close = closes[-1], closes[-2]
    for atype, aidx in anchors.items():
        if aidx > n - 2:
            continue  # need at least today + yesterday inside the anchored window
        series = _anchored_vwap(ndf, aidx)
        av_today, av_prev = series[-1], series[-2]
        if np.isnan(av_today) or np.isnan(av_prev):
            continue
        avwap_today[atype] = float(av_today)
        if prev_close <= av_prev and today_close > av_today:
            crosses.append((atype, "BULL", float(av_today)))   # RECLAIM
        elif prev_close >= av_prev and today_close < av_today:
            crosses.append((atype, "BEAR", float(av_today)))   # LOSS

    # confluence: largest cluster of aVWAPs within band that price is interacting with
    conf_n = _confluence_n(today_close, list(avwap_today.values()))

    out = []
    for atype, signal, avp in crosses:
        out.append({
            "symbol": symbol,
            "asof": asof,
            "anchor_type": atype,
            "signal": signal,
            "avwap_price": round(avp, 4),
            "close": round(float(today_close), 4),
            "confluence_n": conf_n,
        })
    return out


def _confluence_n(close: float, avwaps: list[float]) -> int:
    """Size of the largest cluster of aVWAPs within CONFLUENCE_BAND_PCT of each
    other that price is also interacting with (within INTERACT_PCT)."""
    if not close or len(avwaps) < 2:
        return 0
    best = 0
    for base in avwaps:
        if abs(close / base - 1.0) * 100.0 > INTERACT_PCT:
            continue
        cluster = [v for v in avwaps if abs(v / base - 1.0) * 100.0 <= CONFLUENCE_BAND_PCT]
        best = max(best, len(cluster))
    return best if best >= 2 else 0


# ─── Confirmatory-only convergence contract (8a83f17 rail) ───────────────────

def confirmatory_vote(fleet_directional_votes: int, signal: str | None) -> dict:
    """Decide whether an aVWAP BULL/BEAR lean may COUNT as a confirmatory vote.

    RAIL: aVWAP may CONFIRM an existing fleet convergence but may NEVER ORIGINATE
    a trade. It counts only once the fleet has >= MIN_FLEET_VOTES independent
    directional votes. Sole voter -> contributes nothing, no trade permitted.
    """
    is_directional = signal in ("BULL", "BEAR")
    is_sole_voter = fleet_directional_votes < MIN_FLEET_VOTES
    counts = is_directional and not is_sole_voter

    trade_permitted_on_avwap_alone = False
    assert not (is_sole_voter and counts), (
        "aVWAP is confirmatory-only: the sole voter must never count toward a "
        "trade (MIN_FLEET_VOTES=%d not met)" % MIN_FLEET_VOTES
    )
    assert trade_permitted_on_avwap_alone is False

    return {
        "source": "bk_avwap",
        "signal": signal,
        "direction": ("BULLISH" if signal == "BULL"
                      else "BEARISH" if signal == "BEAR" else "NEUTRAL"),
        "counts_toward_convergence": counts,
        "is_sole_voter": is_sole_voter,
        "fleet_directional_votes": fleet_directional_votes,
        "min_fleet_votes_required": MIN_FLEET_VOTES,
        "trade_permitted_on_avwap_alone": trade_permitted_on_avwap_alone,
        "is_trigger": False,
    }


# ─── NTFY (shadow only) ──────────────────────────────────────────────────────

def _fire_ntfy(sig: dict) -> bool:
    key = f"avwap::{sig['symbol']}::{sig['signal']}::{sig['anchor_type']}"
    if key in _ntfy_fired_classes:
        return False
    _ntfy_fired_classes.add(key)
    try:
        from engine.alert_channels import send_alert
        arrow = "🟢↑" if sig["signal"] == "BULL" else "🔴↓"
        conf = f" +{sig['confluence_n']}× confluence" if sig.get("confluence_n") else ""
        send_alert(
            level="info",
            alert_type=f"bk_avwap_{sig['signal'].lower()}_{sig['symbol']}",
            message=(
                f"{arrow} aVWAP {sig['signal']} (shadow) — {sig['symbol']} "
                f"{'reclaimed' if sig['signal']=='BULL' else 'lost'} {sig['anchor_type']} "
                f"aVWAP @ {sig['avwap_price']} (close {sig['close']}){conf}. "
                f"Confirmatory-only — never originates."
            ),
            title=f"{arrow} aVWAP {sig['signal']} — {sig['symbol']}",
            audience="admin",
            rate_limit_secs=86400,
        )
        return True
    except Exception as e:
        console.log(f"[yellow]bk_avwap: NTFY {sig.get('symbol')} failed: {type(e).__name__}: {e!r}")
        return False


# ─── Persistence ─────────────────────────────────────────────────────────────

def _persist(signals: list[dict], db_path: str | None = None) -> int:
    if not signals:
        return 0
    conn = _conn(db_path)
    try:
        _ensure_schema(conn)
        now = datetime.now(timezone.utc).isoformat()
        conn.executemany(
            "INSERT INTO bk_avwap_signals "
            "(symbol, asof, anchor_type, signal, avwap_price, close, confluence_n, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [(s["symbol"], s["asof"], s["anchor_type"], s["signal"],
              s["avwap_price"], s["close"], s.get("confluence_n", 0), now)
             for s in signals],
        )
        conn.commit()
        return len(signals)
    finally:
        conn.close()


# ─── Orchestrator ────────────────────────────────────────────────────────────

def run_scan(
    universe: list[str] | None = None,
    persist: bool = True,
    shadow_ntfy: bool | None = None,
    db_path: str | None = None,
) -> dict:
    """Nightly scan. shadow_ntfy defaults to the AVWAP_CONFIRMATORY_VOTE_ENABLED
    flag (fires only when ON — shadow-quiet by default)."""
    if shadow_ntfy is None:
        try:
            from config import AVWAP_CONFIRMATORY_VOTE_ENABLED as _en
        except Exception:
            _en = False
        shadow_ntfy = bool(_en)

    syms = universe if universe is not None else _load_universe(db_path)
    if not syms:
        return {"scanned": 0, "signals": 0, "bull": 0, "bear": 0, "confluence": 0}

    try:
        from engine.market_data import get_bulk_daily_ohlcv
        bars = get_bulk_daily_ohlcv(syms, range_str=LOOKBACK_RANGE)
    except Exception as e:
        console.log(f"[yellow]bk_avwap: get_bulk_daily_ohlcv failed: {type(e).__name__}: {e!r}")
        return {"scanned": 0, "signals": 0, "bull": 0, "bear": 0, "confluence": 0, "error": True}

    all_sigs: list[dict] = []
    for sym in syms:
        df = bars.get(sym) if isinstance(bars, dict) else None
        if df is None:
            continue
        try:
            all_sigs.extend(detect(df, sym, db_path))
        except Exception as e:
            console.log(f"[yellow]bk_avwap: detect {sym}: {type(e).__name__}: {e!r}")

    if persist:
        _persist(all_sigs, db_path)
    if shadow_ntfy:
        for s in all_sigs:
            _fire_ntfy(s)

    bull = sum(1 for s in all_sigs if s["signal"] == "BULL")
    bear = sum(1 for s in all_sigs if s["signal"] == "BEAR")
    conf = sum(1 for s in all_sigs if s.get("confluence_n", 0) >= 2)
    return {
        "scanned": len(syms), "signals": len(all_sigs),
        "bull": bull, "bear": bear, "confluence": conf, "rows": all_sigs,
    }


def shadow_pass(sessions: int = 30, universe: list[str] | None = None,
                db_path: str | None = None) -> dict:
    """Replay detection as-of each of the last `sessions` bars (validation only,
    no persist, no ntfy, flag untouched). Prints counts + a 5-row sample."""
    syms = universe if universe is not None else _load_universe(db_path)
    from engine.market_data import get_bulk_daily_ohlcv
    bars = get_bulk_daily_ohlcv(syms, range_str="1y")
    sigs: list[dict] = []
    for sym in syms:
        df = bars.get(sym) if isinstance(bars, dict) else None
        ndf = _norm(df)
        if ndf is None:
            continue
        n = len(ndf)
        start = max(MIN_BARS + 1, n - sessions)
        for cut in range(start, n + 1):
            try:
                sigs.extend(detect(ndf.iloc[:cut], sym, db_path))
            except Exception:
                pass
    bull = sum(1 for s in sigs if s["signal"] == "BULL")
    bear = sum(1 for s in sigs if s["signal"] == "BEAR")
    conf = sum(1 for s in sigs if s.get("confluence_n", 0) >= 2)
    summary = {"sessions": sessions, "symbols": len(syms), "total_signals": len(sigs),
               "bull": bull, "bear": bear, "confluence": conf, "sample": sigs[:5]}
    return summary


if __name__ == "__main__":
    import json
    res = shadow_pass(sessions=30)
    print(json.dumps(res, indent=2, default=str))
