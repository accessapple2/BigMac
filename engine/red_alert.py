"""
Red Alert — Intraday Monitor
-----------------------------
Polls SPY options structure + VIX + momentum + IV skew every 5 minutes
during market hours. Detects 9 regime-change alert types, fires macOS
notifications, and maintains a traffic-light condition signal for agents.

Tables: intraday_snapshots, red_alert_log  (SACRED — never dropped/truncated)
Endpoints wired via ready_room_routes.py:
  GET /api/ready-room/condition  — current GO / CAUTION / STAND DOWN
  GET /api/ready-room/alerts     — today's alert log
  GET /api/ready-room/intraday   — today's snapshot history (sparkline)
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import threading
import time
from collections import deque
from datetime import date, datetime, timezone
from typing import Any

DB = os.environ.get("TRADER_DB", "autonomous_trader.db")

# ── In-memory state ──────────────────────────────────────────────────────────
_lock             = threading.Lock()
_morning_baseline: dict[str, Any] = {}
_prev_snapshot:    dict[str, Any] = {}
_alert_cooldowns:  dict[str, float] = {}          # alert_type → last fired epoch
_momentum_ring:    deque = deque(maxlen=7)         # (epoch, trend_score) — 35-min window
_running          = False

POLL_INTERVAL = 300   # 5 minutes
COOLDOWN_SECS = 600   # 10-minute alert cooldown per type


# ── DB init ──────────────────────────────────────────────────────────────────
def _init_db() -> None:
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intraday_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snap_date       TEXT NOT NULL,
            snap_time       TEXT NOT NULL,
            session_type    TEXT,
            spot_price      REAL,
            call_wall       REAL,
            put_wall        REAL,
            gamma_flip      REAL,
            max_pain        REAL,
            total_gex_b     REAL,
            pc_ratio        REAL,
            vix             REAL,
            vix_state       TEXT,
            vix_regime      TEXT,
            trend_score     REAL,
            buy_volume      REAL,
            sell_volume     REAL,
            bars_count      INTEGER,
            skew_score      REAL,
            condition       TEXT,
            condition_score REAL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS red_alert_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_date  TEXT NOT NULL,
            alert_time  TEXT NOT NULL,
            alert_type  TEXT NOT NULL,
            severity    TEXT NOT NULL,
            title       TEXT NOT NULL,
            message     TEXT NOT NULL,
            data_json   TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


_init_db()


# ── Market hours ─────────────────────────────────────────────────────────────
def _is_market_hours() -> bool:
    """True 9:30 AM – 4:00 PM ET (6:30 AM – 1:00 PM AZ). No DST in AZ."""
    try:
        import pytz
        from datetime import datetime as _dt
        az = pytz.timezone("US/Arizona")
        now = _dt.now(az)
        if now.weekday() >= 5:
            return False
        mins = now.hour * 60 + now.minute
        return 390 <= mins < 780   # 6:30–13:00 AZ = 9:30–16:00 ET
    except Exception:
        return False


# ── Data collection ──────────────────────────────────────────────────────────
def _fetch_snapshot() -> dict[str, Any]:
    """Pull all data sources into one flat snapshot dict."""
    snap: dict[str, Any] = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    # Ready Room briefing — session type + key levels
    try:
        from engine.ready_room import get_latest_briefing
        b = get_latest_briefing() or {}
        snap.update({
            "session_type": b.get("session_type"),
            "spot_price":   b.get("spot_price"),
            "call_wall":    b.get("call_wall"),
            "put_wall":     b.get("put_wall"),
            "gamma_flip":   b.get("gamma_flip"),
            "max_pain":     b.get("max_pain"),
            "total_gex_b":  b.get("total_gex_b"),
            "pc_ratio":     b.get("pc_ratio"),
            "vix":          b.get("vix"),
        })
    except Exception as exc:
        snap["briefing_error"] = str(exc)

    # VIX term structure (fresh each poll)
    try:
        from engine.vix_monitor import get_vix_term_structure
        v = get_vix_term_structure(force=True)
        snap["vix"]       = v.get("vix") or snap.get("vix")
        snap["vix_state"] = v.get("state")
        snap["vix_regime"] = v.get("regime")
    except Exception as exc:
        snap["vix_error"] = str(exc)

    # Intraday momentum (fresh each poll)
    try:
        from engine.momentum_tracker import get_intraday_momentum
        m = get_intraday_momentum(force=True)
        snap["trend_score"] = m.get("trend_score")
        snap["buy_volume"]  = m.get("buy_volume")
        snap["sell_volume"] = m.get("sell_volume")
        snap["bars_count"]  = m.get("bars_count")
        snap["vwap"]        = m.get("vwap")
        snap["last_price"]  = m.get("last_price")
    except Exception as exc:
        snap["momentum_error"] = str(exc)

    # IV skew (fresh each poll)
    try:
        from engine.iv_skew import get_iv_skew
        s = get_iv_skew(force=True)
        snap["skew_score"] = s.get("skew_score")
    except Exception as exc:
        snap["skew_error"] = str(exc)

    return snap


# ── Condition (traffic light) ─────────────────────────────────────────────────
_SESSION_SCORES: dict[str, float] = {
    "TRENDING_BULL":  85.0,
    "TRENDING_BEAR":  70.0,
    "CHOP":           42.0,
    "REVERSAL_RISK":  30.0,
    "VOLATILE":       38.0,
}
_VIX_REGIME_SCORES: dict[str, float] = {
    "CALM":     90.0,
    "ELEVATED": 65.0,
    "STRESSED": 35.0,
    "CRISIS":   10.0,
}


def _compute_condition(snap: dict[str, Any]) -> tuple[str, float]:
    """
    Weighted condition → (label, 0-100 score).
    Default weights: session(30%) · momentum(25%) · VIX(20%) · volume(10%) · skew(5%) · breadth(10%)
    Weights auto-adjust via adaptive_tuner after 15+ days of data.
    """
    # Adaptive weights (fall back to defaults if tuner unavailable)
    try:
        from engine.adaptive_tuner import get_current_weights
        w = get_current_weights()
    except Exception:
        w = {}
    w_session  = w.get("session_type", 0.30)
    w_momentum = w.get("momentum",     0.25)
    w_vix      = w.get("vix",          0.20)
    w_volume   = w.get("volume",        0.10)
    w_skew     = w.get("skew",          0.05)
    w_breadth  = w.get("breadth",       0.10)

    session_raw  = _SESSION_SCORES.get(snap.get("session_type") or "", 45.0)

    ts           = snap.get("trend_score")
    momentum_raw = float((ts + 100) / 2) if ts is not None else 50.0

    vix_raw = _VIX_REGIME_SCORES.get(snap.get("vix_regime") or "", 50.0)
    if snap.get("vix_state") == "BACKWARDATION":
        vix_raw = min(vix_raw, 25.0)

    bv = snap.get("buy_volume") or 0.0
    sv = snap.get("sell_volume") or 0.0
    total_vol    = bv + sv
    volume_raw   = float(bv / total_vol * 100) if total_vol > 0 else 50.0

    sk = snap.get("skew_score")
    if sk is None:
        skew_raw = 50.0
    elif sk > 5:
        skew_raw = 20.0   # extreme fear premium
    elif sk > 2:
        skew_raw = 40.0   # mild fear
    elif sk < -2:
        skew_raw = 60.0   # call demand (greed)
    else:
        skew_raw = 65.0   # neutral

    # Breadth score from breadth_scanner (cached, fast after first call)
    breadth_raw = 50.0
    try:
        from engine.breadth_scanner import get_breadth_snapshot
        b = get_breadth_snapshot()
        bs = b.get("breadth_score")
        if bs is not None:
            breadth_raw = float((bs + 100) / 2)  # convert -100..+100 → 0..100
    except Exception:
        pass

    score = round(
        session_raw  * w_session  +
        momentum_raw * w_momentum +
        vix_raw      * w_vix      +
        volume_raw   * w_volume   +
        skew_raw     * w_skew     +
        breadth_raw  * w_breadth,
        1,
    )

    if score >= 50:     # GO: was 65
        label = "GREEN"
    elif score >= 35:   # CAUTION 35-49: was 45-64; STAND_DOWN now only below 35
        label = "YELLOW"
    else:
        label = "RED"

    return label, score


# ── Alert helpers ─────────────────────────────────────────────────────────────
def _cooldown_ok(alert_type: str) -> bool:
    return (time.time() - _alert_cooldowns.get(alert_type, 0.0)) >= COOLDOWN_SECS


def _fire_alert(
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    data: dict | None = None,
) -> None:
    _alert_cooldowns[alert_type] = time.time()

    now   = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    tstr  = now.strftime("%H:%M:%S")

    # Persist to red_alert_log
    try:
        conn = sqlite3.connect(DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT INTO red_alert_log
                (alert_date, alert_time, alert_type, severity, title, message, data_json)
            VALUES (?,?,?,?,?,?,?)
        """, (today, tstr, alert_type, severity, title, message, json.dumps(data or {})))
        conn.commit()
        conn.close()
    except Exception:
        pass

    # macOS notification
    _sounds   = {"critical": "Submarine", "warning": "Ping", "info": "Pop"}
    _labels   = {"critical": "🚨 Red Alert", "warning": "⚠️ Yellow Alert", "info": "✅ All Clear"}
    snd       = _sounds.get(severity, "Ping")
    st_title  = f"{_labels.get(severity, title)}: {title}"
    try:
        subprocess.Popen(
            ["osascript", "-e",
             f'display notification "{message}" with title "{st_title}" sound name "{snd}"'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass

    # Rich log
    from rich.console import Console
    _clrs = {"critical": "red", "warning": "yellow", "info": "green"}
    Console().log(f"[{_clrs.get(severity,'white')}]RedAlert [{severity.upper()}] {title}: {message}")

    # Bridge Communications — post to War Room + Discord
    try:
        from engine.war_room_feed import post_to_war_room
        post_to_war_room(alert_type, severity, title, message, data)
    except Exception:
        pass

    # Signal Center — post to port 9000 intelligence feed
    try:
        from engine.signal_poster import post_to_9000
        post_to_9000("RED_ALERT", {
            "alert_type": alert_type,
            "severity": severity,
            "title": title,
            "message": message,
            "data": data or {},
        })
    except Exception:
        pass


# ── Alert detection ───────────────────────────────────────────────────────────
def _detect_alerts(
    snap: dict[str, Any],
    prev: dict[str, Any],
    baseline: dict[str, Any],
) -> None:
    spot      = snap.get("spot_price") or 0.0
    prev_spot = prev.get("spot_price") or 0.0

    # ── a. Session Type Change ────────────────────────────────────────────────
    prev_st = prev.get("session_type")
    curr_st = snap.get("session_type")
    if prev_st and curr_st and prev_st != curr_st and _cooldown_ok("session_type_change"):
        sev = "critical" if curr_st in ("REVERSAL_RISK", "VOLATILE") else "warning"
        _fire_alert(
            "session_type_change", sev,
            "Session Regime Change",
            f"{prev_st} → {curr_st}  SPY ${spot:.2f}",
            {"from": prev_st, "to": curr_st, "spot": spot},
        )

    # ── b. Wall Breach (within 0.2%) ─────────────────────────────────────────
    call_wall = snap.get("call_wall") or 0.0
    put_wall  = snap.get("put_wall")  or 0.0
    if spot > 0:
        if call_wall > 0 and _cooldown_ok("call_wall_breach"):
            dist_pct = (call_wall - spot) / spot * 100
            if 0 <= dist_pct <= 0.2:
                _fire_alert(
                    "call_wall_breach", "warning",
                    "Call Wall Approaching",
                    f"SPY ${spot:.2f} within {dist_pct:.2f}% of call wall ${call_wall:.2f}",
                    {"spot": spot, "call_wall": call_wall, "dist_pct": round(dist_pct, 3)},
                )
        if put_wall > 0 and _cooldown_ok("put_wall_breach"):
            dist_pct = (spot - put_wall) / spot * 100
            if 0 <= dist_pct <= 0.2:
                _fire_alert(
                    "put_wall_breach", "warning",
                    "Put Wall Approaching",
                    f"SPY ${spot:.2f} within {dist_pct:.2f}% of put wall ${put_wall:.2f}",
                    {"spot": spot, "put_wall": put_wall, "dist_pct": round(dist_pct, 3)},
                )

    # ── c. GEX Flip Crossing ──────────────────────────────────────────────────
    gamma_flip = snap.get("gamma_flip") or 0.0
    if gamma_flip > 0 and spot > 0 and prev_spot > 0 and _cooldown_ok("gex_flip_cross"):
        crossed = (
            (prev_spot < gamma_flip <= spot) or
            (prev_spot > gamma_flip >= spot)
        )
        if crossed:
            direction = "above" if spot >= gamma_flip else "below"
            _fire_alert(
                "gex_flip_cross", "critical",
                "Gamma Flip Crossed",
                f"SPY ${spot:.2f} crossed gamma flip ${gamma_flip:.2f} — now {direction} flip",
                {"spot": spot, "gamma_flip": gamma_flip, "direction": direction},
            )

    # ── d. P/C Ratio Drift (0.15+) ───────────────────────────────────────────
    pc_now  = snap.get("pc_ratio")
    pc_base = baseline.get("pc_ratio")
    if pc_now and pc_base and _cooldown_ok("pc_ratio_drift"):
        drift = abs(pc_now - pc_base)
        if drift >= 0.15:
            sev       = "critical" if drift >= 0.30 else "warning"
            direction = "bearish" if pc_now > pc_base else "bullish"
            _fire_alert(
                "pc_ratio_drift", sev,
                "P/C Ratio Shift",
                f"P/C drifted {drift:.2f} from open: {pc_base:.2f} → {pc_now:.2f} ({direction})",
                {"baseline": pc_base, "current": pc_now, "drift": round(drift, 3)},
            )

    # ── e. Volume Surge (current bar avg >= 2× session avg) ──────────────────
    bv = snap.get("buy_volume") or 0.0
    sv = snap.get("sell_volume") or 0.0
    total_v  = bv + sv
    bars     = snap.get("bars_count") or 0
    base_bv  = baseline.get("buy_volume") or 0.0
    base_sv  = baseline.get("sell_volume") or 0.0
    base_ttl = base_bv + base_sv
    base_bars = baseline.get("bars_count") or 1
    if total_v > 0 and bars > 0 and base_ttl > 0 and _cooldown_ok("volume_surge"):
        session_avg = base_ttl / base_bars
        recent_avg  = total_v  / bars
        if recent_avg >= session_avg * 2:
            direction = "BUY" if bv > sv else "SELL"
            _fire_alert(
                "volume_surge", "warning",
                "Volume Surge",
                f"{direction} surge: {recent_avg/1e6:.1f}M avg/bar vs {session_avg/1e6:.1f}M session avg",
                {"direction": direction, "recent_avg": recent_avg, "session_avg": session_avg},
            )

    # ── f. VIX Spike (10%+ from baseline) + term structure flip ──────────────
    vix_now   = snap.get("vix") or 0.0
    vix_base  = baseline.get("vix") or 0.0
    if vix_now > 0 and vix_base > 0 and _cooldown_ok("vix_spike"):
        chg_pct = (vix_now - vix_base) / vix_base * 100
        if chg_pct >= 10:
            sev = "critical" if chg_pct >= 20 else "warning"
            _fire_alert(
                "vix_spike", sev,
                "VIX Spike",
                f"VIX +{chg_pct:.1f}% from open ({vix_base:.1f} → {vix_now:.1f})",
                {"baseline": vix_base, "current": vix_now, "change_pct": round(chg_pct, 1)},
            )
    prev_vix_state = prev.get("vix_state") or ""
    curr_vix_state = snap.get("vix_state") or ""
    if prev_vix_state and curr_vix_state and prev_vix_state != curr_vix_state and _cooldown_ok("vix_term_flip"):
        was_back = "BACKWARDATION" in prev_vix_state
        is_back  = "BACKWARDATION" in curr_vix_state
        if was_back != is_back:
            sev = "critical" if is_back else "info"
            _fire_alert(
                "vix_term_flip", sev,
                "VIX Term Structure Flip",
                f"VIX structure flipped: {prev_vix_state} → {curr_vix_state}",
                {"from": prev_vix_state, "to": curr_vix_state},
            )

    # ── g. IV Skew Shift (3pp+) ───────────────────────────────────────────────
    skew_now  = snap.get("skew_score")
    skew_base = baseline.get("skew_score")
    if skew_now is not None and skew_base is not None and _cooldown_ok("iv_skew_shift"):
        delta = abs(skew_now - skew_base)
        if delta >= 3:
            sev       = "critical" if delta >= 6 else "warning"
            direction = "fear spike" if skew_now > skew_base else "fear unwind"
            _fire_alert(
                "iv_skew_shift", sev,
                "IV Skew Shift",
                f"25Δ skew moved {delta:.1f}pp ({skew_base:.1f} → {skew_now:.1f}) — {direction}",
                {"baseline": skew_base, "current": skew_now, "delta": round(delta, 2)},
            )

    # ── h. Momentum Shift (30+ pts in 30 min) ────────────────────────────────
    ts_now = snap.get("trend_score")
    if ts_now is not None and _cooldown_ok("momentum_shift"):
        now_epoch = time.time()
        _momentum_ring.append((now_epoch, ts_now))
        cutoff = now_epoch - 1800   # 30 min
        window = [(t, s) for t, s in _momentum_ring if t >= cutoff]
        if len(window) >= 2:
            oldest_score = window[0][1]
            delta = abs(ts_now - oldest_score)
            if delta >= 30:
                sev       = "critical" if delta >= 50 else "warning"
                direction = "bullish" if ts_now > oldest_score else "bearish"
                _fire_alert(
                    "momentum_shift", sev,
                    "Momentum Shift",
                    f"Trend score shifted {delta:.0f}pts in 30min → {direction} "
                    f"({oldest_score:.0f} → {ts_now:.0f})",
                    {"from": oldest_score, "to": ts_now, "delta": round(delta, 1)},
                )

    # ── i. OI Buildup (3+ strikes with >20% growth) ───────────────────────────
    if _cooldown_ok("oi_buildup"):
        try:
            from engine.oi_tracker import get_oi_changes
            oi_data  = get_oi_changes(force=False)
            flagged  = oi_data.get("flagged") or []
            if len(flagged) >= 3:
                top = flagged[0]
                _fire_alert(
                    "oi_buildup", "warning",
                    "OI Accumulation",
                    f"{len(flagged)} strikes >20% OI growth — "
                    f"top: {str(top.get('type','')).upper()} ${top.get('strike')} "
                    f"+{top.get('change_pct')}%",
                    {"flagged_count": len(flagged), "top_strike": top},
                )
        except Exception:
            pass


# ── Persist snapshot ──────────────────────────────────────────────────────────
def _save_snapshot(snap: dict[str, Any], condition: str, score: float) -> None:
    now = datetime.now(timezone.utc)
    try:
        conn = sqlite3.connect(DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            INSERT INTO intraday_snapshots
                (snap_date, snap_time, session_type, spot_price, call_wall, put_wall,
                 gamma_flip, max_pain, total_gex_b, pc_ratio, vix, vix_state, vix_regime,
                 trend_score, buy_volume, sell_volume, bars_count, skew_score,
                 condition, condition_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S"),
            snap.get("session_type"),
            snap.get("spot_price"),
            snap.get("call_wall"),
            snap.get("put_wall"),
            snap.get("gamma_flip"),
            snap.get("max_pain"),
            snap.get("total_gex_b"),
            snap.get("pc_ratio"),
            snap.get("vix"),
            snap.get("vix_state"),
            snap.get("vix_regime"),
            snap.get("trend_score"),
            snap.get("buy_volume"),
            snap.get("sell_volume"),
            snap.get("bars_count"),
            snap.get("skew_score"),
            condition, score,
        ))
        conn.commit()
        conn.close()
    except Exception as exc:
        from rich.console import Console
        Console().log(f"[yellow]RedAlert: snapshot save error: {exc}")


# ── Public poll entry point ───────────────────────────────────────────────────
def run_poll_cycle() -> dict[str, Any]:
    """
    Run one complete 5-min poll.  Callable from outside for testing.
    Returns the assembled snapshot dict with condition appended.
    """
    global _morning_baseline, _prev_snapshot

    snap = _fetch_snapshot()
    condition, score = _compute_condition(snap)
    snap["condition"]       = condition
    snap["condition_score"] = score

    with _lock:
        today = date.today().isoformat()
        # Record morning baseline on first poll of the day
        if not _morning_baseline or _morning_baseline.get("_snap_date") != today:
            _morning_baseline = {**snap, "_snap_date": today}

        prev = _prev_snapshot.copy()
        _detect_alerts(snap, prev, _morning_baseline)
        _save_snapshot(snap, condition, score)
        _prev_snapshot = {**snap, "_snap_date": today}

    from rich.console import Console
    _clr = {"GREEN": "green", "YELLOW": "yellow", "RED": "red"}.get(condition, "white")
    Console().log(
        f"[{_clr}]RedAlert ▶ {condition} ({score:.0f}) "
        f"| {snap.get('session_type','?')} "
        f"SPY ${(snap.get('spot_price') or 0):.2f} "
        f"| trend {(snap.get('trend_score') or 0):.0f} "
        f"| VIX {(snap.get('vix') or 0):.1f}"
    )
    return snap


# ── Public API helpers (used by routes) ──────────────────────────────────────
_ST_LABELS = {
    "GREEN":   "All Clear",
    "YELLOW":  "Yellow Alert",
    "RED":     "Red Alert",
}


def get_current_condition() -> dict[str, Any]:
    """Most recent intraday snapshot for /condition endpoint."""
    try:
        today = date.today().isoformat()
        conn  = sqlite3.connect(DB, timeout=30)
        conn.row_factory = sqlite3.Row
        row   = conn.execute("""
            SELECT * FROM intraday_snapshots
            WHERE snap_date = ?
            ORDER BY id DESC LIMIT 1
        """, (today,)).fetchone()
        conn.close()
        if not row:
            return {
                "condition":       "UNKNOWN",
                "condition_label": "Awaiting Data",
                "condition_score": None,
                "message": "No intraday data yet — Red Alert polls every 5 min during 9:30–16:00 ET.",
            }
        d = dict(row)
        d["condition_label"] = _ST_LABELS.get(d.get("condition"), d.get("condition"))
        d["is_market_hours"] = _is_market_hours()
        return d
    except Exception as exc:
        return {"error": str(exc)}


def get_today_alerts(limit: int = 50) -> list[dict]:
    """All alerts fired today from red_alert_log."""
    try:
        today = date.today().isoformat()
        conn  = sqlite3.connect(DB, timeout=30)
        conn.row_factory = sqlite3.Row
        rows  = conn.execute("""
            SELECT * FROM red_alert_log
            WHERE alert_date = ?
            ORDER BY id DESC LIMIT ?
        """, (today, limit)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        return [{"error": str(exc)}]


def get_today_snapshots() -> list[dict]:
    """All intraday snapshots today — for sparkline/chart rendering."""
    try:
        today = date.today().isoformat()
        conn  = sqlite3.connect(DB, timeout=30)
        conn.row_factory = sqlite3.Row
        rows  = conn.execute("""
            SELECT snap_time, trend_score, vix, spot_price,
                   condition, condition_score, session_type, total_gex_b, pc_ratio
            FROM intraday_snapshots
            WHERE snap_date = ?
            ORDER BY id ASC
        """, (today,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        return [{"error": str(exc)}]


# ── Background thread ─────────────────────────────────────────────────────────
def _poll_loop() -> None:
    from rich.console import Console
    console = Console()
    console.log("[cyan]RedAlert: armed — polling every 5 min during 9:30–16:00 ET")

    while _running:
        try:
            if _is_market_hours():
                run_poll_cycle()
            else:
                # Reset daily state when day rolls over
                today = date.today().isoformat()
                with _lock:
                    if _morning_baseline.get("_snap_date") not in (None, today):
                        _morning_baseline.clear()
                        _momentum_ring.clear()
        except Exception as exc:
            console.log(f"[red]RedAlert poll error: {exc}")

        # Sleep in 10-second ticks so the thread exits promptly on stop()
        for _ in range(POLL_INTERVAL // 10):
            if not _running:
                break
            time.sleep(10)


def start_red_alert() -> None:
    """Start the Red Alert daemon thread.  Safe to call multiple times."""
    global _running
    if _running:
        return
    _running = True
    t = threading.Thread(target=_poll_loop, name="RedAlert", daemon=True)
    t.start()


def stop_red_alert() -> None:
    global _running
    _running = False
