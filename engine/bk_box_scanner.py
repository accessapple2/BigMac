#!/usr/bin/env python3
"""
bk_box_scanner.py — HM-BK-C Darvas/Donchian tight-box breakout confirmatory scanner.

PURPOSE: confirm an edge breakout out of a tight consolidation box. CONFIRMATORY-
ONLY — never originates a trade; emits a BULL (or optional BEAR) vote that counts
only when the fleet already has >= MIN_FLEET_VOTES independent directional votes
(8a83f17 FRED-BANKRATE rail, reused exactly). Distinct from the BB/KC squeeze.

Detection (tunable):
  - Donchian channel, lookback DONCHIAN_LOOKBACK (20) sessions ->
    box_top = 20d high, box_bottom = 20d low (prior window, excluding today).
  - Box must be TIGHT and MATURE:
      width  = (box_top - box_bottom) / midpoint <= WIDTH_MAX_PCT (8%)
      duration = consecutive recent closes inside the channel >= MIN_DURATION (7)
  - Trigger (long edge): today's close breaks above box_top on volume
    >= VOL_MULT (1.5x) the 20-day average volume.
  - Short side (close < box_bottom -> BEAR) is OFF by default (BOX_SHORT_ENABLED).

Data: engine.market_data.get_bulk_daily_ohlcv — the SAME daily-OHLCV source as
HM-BK-B. Runs nightly AFTER B (within the 30-min bulk cache window -> reuses bars).

Doctrine: is_trigger hardcoded False; default OFF via BOX_CONFIRMATORY_VOTE_ENABLED;
rows written, never deleted.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

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
DONCHIAN_LOOKBACK = 20      # box window (sessions, prior to today)
WIDTH_MAX_PCT = 8.0         # (box_top-box_bottom)/midpoint <= this (%)
MIN_DURATION = 7            # consecutive recent closes inside the channel
VOL_MULT = 1.5              # breakout volume vs 20d avg
LOOKBACK_RANGE = "6mo"
MIN_BARS = DONCHIAN_LOOKBACK + 5

# Confirmatory-only rail (mirrors fred_bankrate_signal.MIN_FLEET_VOTES).
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
            CREATE TABLE IF NOT EXISTS bk_box_signals (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL,
                asof            TEXT NOT NULL,
                box_top         REAL,
                box_bottom      REAL,
                width_pct       REAL,
                duration_days   INTEGER,
                vol_mult        REAL,
                signal          TEXT NOT NULL,
                created_at      TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bk_box_sym_asof "
            "ON bk_box_signals(symbol, asof DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_bk_box_asof "
            "ON bk_box_signals(asof DESC)"
        )
        conn.commit()
    except Exception as e:
        console.log(f"[yellow]bk_box: _ensure_schema: {type(e).__name__}: {e!r}")


def _load_universe(db_path: str | None = None) -> list[str]:
    try:
        conn = _conn(db_path)
        try:
            rows = conn.execute("SELECT symbol FROM scan_universe").fetchall()
            return [r["symbol"] for r in rows if r["symbol"]]
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]bk_box: universe load failed: {type(e).__name__}: {e!r}")
        return []


def _norm(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or len(df) == 0:
        return None
    cols = {c.lower(): c for c in df.columns}
    need = ("open", "high", "low", "close", "volume")
    if not all(k in cols for k in need):
        return None
    out = pd.DataFrame({k: pd.to_numeric(df[cols[k]], errors="coerce") for k in need})
    out = out.dropna(subset=["high", "low", "close", "volume"]).copy()
    return out if len(out) >= MIN_BARS else None


# ─── Detection ───────────────────────────────────────────────────────────────

def detect(df: pd.DataFrame, symbol: str, short_enabled: bool | None = None) -> list[dict]:
    """Return tight-box breakout signal(s) as-of the LAST bar of df."""
    if short_enabled is None:
        try:
            from config import BOX_SHORT_ENABLED as _se
        except Exception:
            _se = False
        short_enabled = bool(_se)

    ndf = _norm(df)
    if ndf is None:
        return []
    highs = ndf["high"].to_numpy()
    lows = ndf["low"].to_numpy()
    closes = ndf["close"].to_numpy()
    vols = ndf["volume"].to_numpy()
    n = len(ndf)
    i = n - 1
    asof = str(ndf.index[-1])[:10] if hasattr(ndf.index[-1], "__str__") else str(n)

    lo, hi = i - DONCHIAN_LOOKBACK, i  # prior window [i-20, i)  (excludes today)
    if lo < 0:
        return []
    box_top = float(highs[lo:hi].max())
    box_bottom = float(lows[lo:hi].min())
    mid = (box_top + box_bottom) / 2.0
    if mid <= 0:
        return []
    width_pct = (box_top - box_bottom) / mid * 100.0

    # duration: consecutive recent closes (ending yesterday) inside the channel
    dur = 0
    for j in range(i - 1, lo - 1, -1):
        if box_bottom <= closes[j] <= box_top:
            dur += 1
        else:
            break

    avg_vol = float(vols[lo:hi].mean())
    vol_mult = (vols[i] / avg_vol) if avg_vol > 0 else 0.0
    today_close = float(closes[i])

    if width_pct > WIDTH_MAX_PCT or dur < MIN_DURATION:
        return []

    out = []
    if today_close > box_top and vol_mult >= VOL_MULT:
        out.append(_row(symbol, asof, box_top, box_bottom, width_pct, dur, vol_mult, "BULL"))
    elif short_enabled and today_close < box_bottom and vol_mult >= VOL_MULT:
        out.append(_row(symbol, asof, box_top, box_bottom, width_pct, dur, vol_mult, "BEAR"))
    return out


def _row(symbol, asof, box_top, box_bottom, width_pct, dur, vol_mult, signal) -> dict:
    return {
        "symbol": symbol, "asof": asof,
        "box_top": round(box_top, 4), "box_bottom": round(box_bottom, 4),
        "width_pct": round(width_pct, 2), "duration_days": int(dur),
        "vol_mult": round(vol_mult, 2), "signal": signal,
    }


# ─── Confirmatory-only convergence contract (8a83f17 rail) ───────────────────

def confirmatory_vote(fleet_directional_votes: int, signal: str | None) -> dict:
    """Box breakout may CONFIRM an existing fleet convergence but may NEVER
    ORIGINATE. Counts only once the fleet has >= MIN_FLEET_VOTES directional votes.
    """
    is_directional = signal in ("BULL", "BEAR")
    is_sole_voter = fleet_directional_votes < MIN_FLEET_VOTES
    counts = is_directional and not is_sole_voter

    trade_permitted_on_box_alone = False
    assert not (is_sole_voter and counts), (
        "box breakout is confirmatory-only: the sole voter must never count "
        "toward a trade (MIN_FLEET_VOTES=%d not met)" % MIN_FLEET_VOTES
    )
    assert trade_permitted_on_box_alone is False

    return {
        "source": "bk_box",
        "signal": signal,
        "direction": ("BULLISH" if signal == "BULL"
                      else "BEARISH" if signal == "BEAR" else "NEUTRAL"),
        "counts_toward_convergence": counts,
        "is_sole_voter": is_sole_voter,
        "fleet_directional_votes": fleet_directional_votes,
        "min_fleet_votes_required": MIN_FLEET_VOTES,
        "trade_permitted_on_box_alone": trade_permitted_on_box_alone,
        "is_trigger": False,
    }


# ─── UHURA market-level confirmatory vote ────────────────────────────────────
# Weight 1.0 = TECHNICAL-CONFIRMATORY class (vs FRED's 0.5 macro-context).
CONFIRMATORY_WEIGHT = 1.0


def _fresh_rows(db_path: str | None = None, watchlist: list[str] | None = None) -> list[dict]:
    """Rows from the most recent nightly session (asof == MAX(asof))."""
    try:
        conn = _conn(db_path)
        try:
            _ensure_schema(conn)
            mx = conn.execute("SELECT MAX(asof) FROM bk_box_signals").fetchone()[0]
            if not mx:
                return []
            rows = [dict(r) for r in conn.execute(
                "SELECT symbol, signal, width_pct, vol_mult, asof "
                "FROM bk_box_signals WHERE asof=?", (mx,)).fetchall()]
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]bk_box: _fresh_rows: {type(e).__name__}: {e!r}")
        return []
    if watchlist:
        wl = set(watchlist)
        rows = [r for r in rows if r["symbol"] in wl]
    return rows


def market_vote(watchlist: list[str] | None = None, db_path: str | None = None,
                enabled: bool | None = None) -> dict | None:
    """Aggregate the latest session's box breakouts into ONE market-level
    confirmatory lean, or None to ABSTAIN. Long-only default -> BULLISH bias."""
    if enabled is None:
        try:
            from config import BOX_CONFIRMATORY_VOTE_ENABLED as enabled
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
    reasoning = (f"[confirm] {len(rows)} fresh box breakout "
                 f"({len(bull)} up / {len(bear)} down) e.g. {ex}")
    return {"direction": direction, "weight": CONFIRMATORY_WEIGHT,
            "reasoning": reasoning, "n": n, "bull": len(bull), "bear": len(bear)}


# ─── NTFY (shadow only) ──────────────────────────────────────────────────────

def _fire_ntfy(sig: dict) -> bool:
    key = f"box::{sig['symbol']}::{sig['signal']}"
    if key in _ntfy_fired_classes:
        return False
    _ntfy_fired_classes.add(key)
    try:
        from engine.alert_channels import send_alert
        arrow = "🟢⬆" if sig["signal"] == "BULL" else "🔴⬇"
        send_alert(
            level="info",
            alert_type=f"bk_box_{sig['signal'].lower()}_{sig['symbol']}",
            message=(
                f"{arrow} Box breakout {sig['signal']} (shadow) — {sig['symbol']} "
                f"broke {'above' if sig['signal']=='BULL' else 'below'} "
                f"{sig['box_top'] if sig['signal']=='BULL' else sig['box_bottom']} "
                f"({sig['duration_days']}d box, width {sig['width_pct']}%, "
                f"vol {sig['vol_mult']}×). Confirmatory-only — never originates."
            ),
            title=f"{arrow} Box {sig['signal']} — {sig['symbol']}",
            audience="admin",
            rate_limit_secs=86400,
        )
        return True
    except Exception as e:
        console.log(f"[yellow]bk_box: NTFY {sig.get('symbol')} failed: {type(e).__name__}: {e!r}")
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
            "INSERT INTO bk_box_signals "
            "(symbol, asof, box_top, box_bottom, width_pct, duration_days, vol_mult, signal, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            [(s["symbol"], s["asof"], s["box_top"], s["box_bottom"], s["width_pct"],
              s["duration_days"], s["vol_mult"], s["signal"], now) for s in signals],
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
    if shadow_ntfy is None:
        try:
            from config import BOX_CONFIRMATORY_VOTE_ENABLED as _en
        except Exception:
            _en = False
        shadow_ntfy = bool(_en)

    syms = universe if universe is not None else _load_universe(db_path)
    if not syms:
        return {"scanned": 0, "signals": 0, "bull": 0, "bear": 0, "boxes": 0}

    try:
        from engine.market_data import get_bulk_daily_ohlcv
        bars = get_bulk_daily_ohlcv(syms, range_str=LOOKBACK_RANGE)
    except Exception as e:
        console.log(f"[yellow]bk_box: get_bulk_daily_ohlcv failed: {type(e).__name__}: {e!r}")
        return {"scanned": 0, "signals": 0, "bull": 0, "bear": 0, "boxes": 0, "error": True}

    all_sigs: list[dict] = []
    boxes = 0  # tight+mature boxes seen (selectivity denominator)
    for sym in syms:
        df = bars.get(sym) if isinstance(bars, dict) else None
        if df is None:
            continue
        try:
            sigs = detect(df, sym)
            all_sigs.extend(sigs)
            boxes += _count_box(df)
        except Exception as e:
            console.log(f"[yellow]bk_box: detect {sym}: {type(e).__name__}: {e!r}")

    if persist:
        _persist(all_sigs, db_path)
    if shadow_ntfy:
        for s in all_sigs:
            _fire_ntfy(s)

    bull = sum(1 for s in all_sigs if s["signal"] == "BULL")
    bear = sum(1 for s in all_sigs if s["signal"] == "BEAR")
    return {"scanned": len(syms), "signals": len(all_sigs),
            "bull": bull, "bear": bear, "boxes": boxes, "rows": all_sigs}


def _count_box(df: pd.DataFrame) -> int:
    """1 if the symbol currently presents a tight+mature box (breakout or not)."""
    ndf = _norm(df)
    if ndf is None:
        return 0
    highs = ndf["high"].to_numpy(); lows = ndf["low"].to_numpy(); closes = ndf["close"].to_numpy()
    n = len(ndf); i = n - 1; lo = i - DONCHIAN_LOOKBACK
    if lo < 0:
        return 0
    box_top = float(highs[lo:i].max()); box_bottom = float(lows[lo:i].min())
    mid = (box_top + box_bottom) / 2.0
    if mid <= 0:
        return 0
    width_pct = (box_top - box_bottom) / mid * 100.0
    dur = 0
    for j in range(i - 1, lo - 1, -1):
        if box_bottom <= closes[j] <= box_top:
            dur += 1
        else:
            break
    return 1 if (width_pct <= WIDTH_MAX_PCT and dur >= MIN_DURATION) else 0


def shadow_pass(sessions: int = 30, universe: list[str] | None = None,
                db_path: str | None = None) -> dict:
    """Replay detection as-of each of the last `sessions` bars (no persist/ntfy)."""
    syms = universe if universe is not None else _load_universe(db_path)
    from engine.market_data import get_bulk_daily_ohlcv
    bars = get_bulk_daily_ohlcv(syms, range_str="1y")
    sigs: list[dict] = []
    boxes = 0
    for sym in syms:
        df = bars.get(sym) if isinstance(bars, dict) else None
        ndf = _norm(df)
        if ndf is None:
            continue
        n = len(ndf)
        start = max(MIN_BARS, n - sessions)
        for cut in range(start, n + 1):
            try:
                sigs.extend(detect(ndf.iloc[:cut], sym))
            except Exception:
                pass
        boxes += _count_box(ndf)
    bull = sum(1 for s in sigs if s["signal"] == "BULL")
    bear = sum(1 for s in sigs if s["signal"] == "BEAR")
    return {"sessions": sessions, "symbols": len(syms), "total_signals": len(sigs),
            "bull": bull, "bear": bear, "tight_boxes_now": boxes, "sample": sigs[:5]}


if __name__ == "__main__":
    import json
    print(json.dumps(shadow_pass(sessions=30), indent=2, default=str))
