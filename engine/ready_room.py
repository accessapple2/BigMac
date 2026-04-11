"""
Ready Room — Daily Session Gameplan (Phase 1)

Stardate analysis: SPY options structure → session type forecast → Captain's orders.

Uses GEX (gamma exposure) from Alpaca options, put/call ratio, and VIX to
determine if the session will trend, chop, or face reversal risk.

Session Types:
  CHOP          — Positive GEX: MMs long gamma, will fade moves. Trade the range.
  TRENDING_BULL — Negative GEX + above gamma flip: amplified upside. Ride dips.
  TRENDING_BEAR — Negative GEX + below gamma flip: amplified downside. Ride rips.
  REVERSAL_RISK — Spot within 0.5% of a key wall or max pain. Fade the move.
  VOLATILE      — VIX >30 AND negative GEX. Reduce size, wait for confirmation.

Saved to ready_room_briefings table. Agents query /api/ready-room/levels for
structured key levels to incorporate into signal generation.

SACRED DATA RULE: this module never drops, deletes, or truncates any table.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Optional

from rich.console import Console

console = Console()

DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

_CACHE: dict = {}
_CACHE_TTL = 600  # 10 minutes — briefings are heavy; don't regenerate on every API hit


# ── Database ─────────────────────────────────────────────────────────────────


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_db() -> None:
    """Create ready_room_briefings table if it doesn't exist. Safe to call repeatedly."""
    try:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ready_room_briefings (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol        TEXT    NOT NULL DEFAULT 'SPY',
                session_date  TEXT    NOT NULL,
                session_time  TEXT    NOT NULL,
                spot_price    REAL,
                call_wall     REAL,
                put_wall      REAL,
                max_pain      REAL,
                gamma_flip    REAL,
                max_gamma_strike REAL,
                total_gex     REAL,
                pc_ratio      REAL,
                vix           REAL,
                session_type  TEXT,
                signals_json  TEXT,
                gameplan      TEXT,
                created_at    TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[red]ReadyRoom: DB init error: {e}")


# ── Data Gathering ────────────────────────────────────────────────────────────


def _get_vix() -> float:
    """Fetch latest VIX from yfinance. Returns 20.0 on failure."""
    try:
        import yfinance as yf
        df = yf.download("^VIX", period="2d", progress=False, timeout=10)
        if df is not None and not df.empty:
            val = float(df["Close"].dropna().iloc[-1])
            if val > 0:
                return round(val, 2)
    except Exception as e:
        console.log(f"[yellow]ReadyRoom: VIX fetch error: {e}")
    return 20.0


def _get_pc_ratio_cboe() -> Optional[float]:
    """
    Try to fetch total equity P/C ratio from CBOE's free daily CSV.
    Returns None on any failure; caller falls back to SPY OI calculation.
    """
    try:
        import requests
        url = "https://cdn.cboe.com/data/us/options/market_statistics/daily_pcr.csv"
        resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        lines = [l for l in resp.text.strip().splitlines() if l.strip()]
        if len(lines) < 2:
            return None
        # Header: DATE,CBOE TOTAL PUT/CALL RATIO,...
        # Take the most recent data row
        last = lines[-1].split(",")
        val = float(last[1].strip())
        return round(val, 3) if 0.1 <= val <= 5.0 else None
    except Exception:
        return None


def _calc_pc_ratio_from_oi(levels) -> float:
    """
    Calculate SPY put/call ratio directly from the option chain OI.
    Uses the GEXLevel list from gex_calculator.
    """
    total_call_oi = sum(getattr(l, "call_oi", 0) or 0 for l in levels)
    total_put_oi  = sum(getattr(l, "put_oi",  0) or 0 for l in levels)
    if total_call_oi > 0:
        return round(total_put_oi / total_call_oi, 3)
    return 1.0


def _calc_max_pain(levels) -> float:
    """
    Max pain = strike where option sellers (MMs) experience minimum total loss.

    For each candidate strike S:
        call pain = Σ call_oi[K] × max(0, S - K) × 100  for all K
        put  pain = Σ put_oi[K]  × max(0, K - S) × 100  for all K
        total = call_pain + put_pain

    Max pain = argmin(total pain).
    """
    if not levels:
        return 0.0
    strikes = sorted(set(getattr(l, "strike", 0) for l in levels))
    if not strikes:
        return 0.0

    # Build quick lookup
    call_oi = {getattr(l, "strike", 0): getattr(l, "call_oi", 0) or 0 for l in levels}
    put_oi  = {getattr(l, "strike", 0): getattr(l, "put_oi",  0) or 0 for l in levels}

    min_pain = float("inf")
    max_pain_strike = strikes[len(strikes) // 2]  # default: middle strike

    for s in strikes:
        cp = sum(call_oi.get(k, 0) * max(0.0, s - k) * 100 for k in strikes)
        pp = sum(put_oi.get(k, 0)  * max(0.0, k - s) * 100 for k in strikes)
        total = cp + pp
        if total < min_pain:
            min_pain = total
            max_pain_strike = s

    return round(max_pain_strike, 2)


# ── Session Analysis ──────────────────────────────────────────────────────────


_SESSION_COLORS = {
    "CHOP":           "#f59e0b",  # amber
    "TRENDING_BULL":  "#22c55e",  # green
    "TRENDING_BEAR":  "#ef4444",  # red
    "REVERSAL_RISK":  "#a855f7",  # purple
    "VOLATILE":       "#f97316",  # orange
}

_SESSION_ICONS = {
    "CHOP":           "↔️",
    "TRENDING_BULL":  "🚀",
    "TRENDING_BEAR":  "🐻",
    "REVERSAL_RISK":  "⚠️",
    "VOLATILE":       "⚡",
}


def _determine_session_type(
    spot: float,
    total_gex: float,
    gamma_flip: float,
    put_wall: float,
    call_wall: float,
    max_pain: float,
    pc_ratio: float,
    vix: float,
) -> tuple[str, list[str]]:
    """
    Classify session type and generate a list of human-readable signals.

    Returns (session_type: str, signals: list[str])
    """
    signals: list[str] = []

    # Safe distance calculations
    def pct_from(a: float, b: float) -> float:
        return abs(a - b) / b * 100 if b > 0 else 999.0

    dist_call  = (call_wall  - spot) / spot * 100 if call_wall > spot  and spot > 0 else 999.0
    dist_put   = (spot - put_wall)   / spot * 100 if put_wall  < spot  and spot > 0 else 999.0
    dist_pain  = pct_from(spot, max_pain)
    dist_flip  = pct_from(spot, gamma_flip)

    above_flip = (spot > gamma_flip) if gamma_flip > 0 else True

    # ── GEX regime ─────────────────────────────────────────────────────────
    gex_b = total_gex / 1e9
    if total_gex > 1e9:
        signals.append(
            f"GEX strongly positive (+{gex_b:.1f}B) — dealers long gamma, price pinning active"
        )
        gex_regime = "pinned"
    elif total_gex > 0:
        signals.append(
            f"GEX positive (+{gex_b:.2f}B) — mild pinning, MMs will fade sharp moves"
        )
        gex_regime = "mild_pin"
    elif total_gex > -1e9:
        signals.append(
            f"GEX slightly negative ({gex_b:.2f}B) — mild trend amplification possible"
        )
        gex_regime = "mild_trend"
    else:
        signals.append(
            f"GEX negative ({gex_b:.1f}B) — dealers short gamma, moves will amplify"
        )
        gex_regime = "trending"

    # ── Gamma flip ─────────────────────────────────────────────────────────
    flip_tag = "above" if above_flip else "below"
    signals.append(
        f"Spot {flip_tag} gamma flip at ${gamma_flip:.2f} ({dist_flip:.1f}% away) "
        f"— {'positive dealer flow, dips supported' if above_flip else 'negative dealer flow, rallies faded'}"
    )

    # ── VIX ────────────────────────────────────────────────────────────────
    if vix > 30:
        signals.append(f"⚡ VIX {vix:.1f} — fear elevated, expect whipsaw, widen stops")
    elif vix > 20:
        signals.append(f"VIX {vix:.1f} — moderate volatility, normal sizing")
    else:
        signals.append(f"VIX {vix:.1f} — low vol, complacency risk, watch for spike")

    # ── P/C ratio ──────────────────────────────────────────────────────────
    if pc_ratio > 1.3:
        signals.append(f"P/C ratio {pc_ratio:.2f} — heavy put buying, fear elevated, watch for short squeeze")
    elif pc_ratio > 1.0:
        signals.append(f"P/C ratio {pc_ratio:.2f} — mild bearish hedging")
    elif pc_ratio < 0.7:
        signals.append(f"P/C ratio {pc_ratio:.2f} — call heavy, complacency risk, protect longs")
    else:
        signals.append(f"P/C ratio {pc_ratio:.2f} — balanced positioning")

    # ── Key level proximity ────────────────────────────────────────────────
    at_call_wall = dist_call < 0.5
    at_put_wall  = dist_put  < 0.5
    at_max_pain  = dist_pain < 0.3

    if at_call_wall:
        signals.append(f"🚧 At CALL WALL ${call_wall:.2f} — strong options resistance zone")
    elif dist_call < 1.5:
        signals.append(
            f"Approaching call wall ${call_wall:.2f} ({dist_call:.1f}% above) — resistance ahead"
        )
    else:
        signals.append(f"Call wall ${call_wall:.2f} ({dist_call:.1f}% above spot)")

    if at_put_wall:
        signals.append(f"🛡️ At PUT WALL ${put_wall:.2f} — strong options support zone")
    elif dist_put < 1.5:
        signals.append(
            f"Near put wall ${put_wall:.2f} ({dist_put:.1f}% below) — support nearby"
        )
    else:
        signals.append(f"Put wall ${put_wall:.2f} ({dist_put:.1f}% below spot)")

    if at_max_pain:
        signals.append(
            f"🎯 Spot AT max pain ${max_pain:.2f} — MMs incentivized to pin price here"
        )
    else:
        direction = "above" if spot > max_pain else "below"
        signals.append(
            f"Max pain ${max_pain:.2f} ({dist_pain:.1f}% {direction} spot)"
            + (" — gravity will pull price down" if spot > max_pain else " — gravity will pull price up")
        )

    # ── Session type classification ────────────────────────────────────────
    if at_call_wall or at_put_wall or at_max_pain:
        session_type = "REVERSAL_RISK"
    elif vix > 30 and gex_regime == "trending":
        session_type = "VOLATILE"
    elif gex_regime in ("pinned", "mild_pin"):
        session_type = "CHOP"
    else:
        # Negative GEX — trending
        session_type = "TRENDING_BULL" if above_flip else "TRENDING_BEAR"

    return session_type, signals


def _generate_gameplan(
    session_type: str,
    spot: float,
    call_wall: float,
    put_wall: float,
    max_pain: float,
    gamma_flip: float,
    pc_ratio: float,
    vix: float,
    signals: list[str],
) -> str:
    """Generate a structured plain-English gameplan for the Captain."""
    now_str = datetime.now().strftime("%Y.%m.%d %H:%M MST")
    icon = _SESSION_ICONS.get(session_type, "📋")
    trade_range = call_wall - put_wall if call_wall > put_wall > 0 else 0

    descriptions = {
        "CHOP": (
            "Range-bound session. Market Makers are long gamma and will systematically "
            "fade directional moves. Price is pinned between the put wall and call wall. "
            "Mean-reversion and scalping setups are favored. Avoid chasing breakouts."
        ),
        "TRENDING_BULL": (
            "Bullish trending session. Dealers are short gamma — they must buy as price "
            "rises, amplifying upside moves. Spot is above the gamma flip, meaning dealer "
            "hedging flow supports dips. Momentum and dip-buy setups favored. Do not fade strength."
        ),
        "TRENDING_BEAR": (
            "Bearish trending session. Dealers are short gamma — they must sell as price "
            "falls, accelerating downside moves. Spot is below the gamma flip, meaning dealer "
            "hedging creates negative feedback on rallies. Short setups or cash preservation favored."
        ),
        "REVERSAL_RISK": (
            "Spot is at or within 0.5% of a key options wall / max pain level. "
            "Market makers are incentivized to defend these levels. High probability of "
            "directional rejection or pinning. Fade the current move; do not chase."
        ),
        "VOLATILE": (
            "High volatility regime. VIX is elevated AND GEX is negative — expect wide "
            "intraday ranges, potential gap risk, and false breakouts. Reduce position size "
            "by at least 50%. Wait for volatility to contract before entering."
        ),
    }

    orders = {
        "CHOP": [
            f"Trade the range: buy near ${put_wall:.2f} put wall, sell near ${call_wall:.2f} call wall",
            f"Max pain gravity: price drifts toward ${max_pain:.2f} as expiry approaches",
            "Avoid breakout plays — MMs will fade them back into range",
            "Use tight stops (0.3–0.5%), take profits quickly",
            f"Ideal range to work: ${put_wall:.2f} – ${call_wall:.2f}"
            + (f" ({trade_range:.2f} pts)" if trade_range > 0 else ""),
        ],
        "TRENDING_BULL": [
            f"Bias long above ${gamma_flip:.2f} (gamma flip) — stop below it",
            f"Buy dips toward ${gamma_flip:.2f} or ${put_wall:.2f}; do not chase tops",
            f"First target: ${call_wall:.2f} (call wall) — expect pause/distribution there",
            "Trail stops — negative GEX amplifies both recoveries and reversals",
            "If price reclaims gamma flip after a pullback, that is the entry signal",
        ],
        "TRENDING_BEAR": [
            f"Bias short below ${gamma_flip:.2f} (gamma flip) — stop above it",
            f"Sell rips toward ${gamma_flip:.2f}; do not short into put wall ${put_wall:.2f}",
            f"First target: ${put_wall:.2f} (put wall) — expect oversold bounce there",
            "Trail stops on shorts — downside can accelerate in negative GEX",
            "Consider hedges (SPY puts, inverse ETFs) if SPY breaks ${put_wall:.2f}",
        ],
        "REVERSAL_RISK": [
            f"Do NOT chase current direction — probability favors reversal at ${spot:.2f}",
            f"Watch for price rejection / wick candles at current level",
            f"Max pain at ${max_pain:.2f} — MMs profit most if SPY closes there",
            "Wait for confirmed reversal candle before entering counter-trend",
            "If price breaks through the wall with volume, reassess — level may be failing",
        ],
        "VOLATILE": [
            "Reduce position size by 50% minimum before any entry",
            f"VIX at {vix:.1f} — options premium is expensive; avoid buying options",
            "Wait for first 30–60 min of session before committing capital",
            "Wide stops required — use ATR-based sizing, not fixed points",
            f"Key anchors: ${put_wall:.2f} (support) and ${call_wall:.2f} (resistance)",
        ],
    }

    lines = [
        f"═══════════════════════════════════════════",
        f" {icon} READY ROOM — STARDATE {now_str}",
        f"═══════════════════════════════════════════",
        f"",
        f"SESSION TYPE: {session_type}",
        f"",
        descriptions.get(session_type, ""),
        f"",
        f"───── KEY LEVELS ─────",
        f"  Call Wall (resistance) : ${call_wall:.2f}",
        f"  Max Pain (gravity)     : ${max_pain:.2f}",
        f"  Gamma Flip (regime)    : ${gamma_flip:.2f}",
        f"  Put Wall (support)     : ${put_wall:.2f}",
    ]
    if trade_range > 0:
        lines.append(f"  Option Range           : {trade_range:.2f} pts")

    lines += [
        f"  P/C Ratio              : {pc_ratio:.2f}",
        f"  VIX                    : {vix:.1f}",
        f"",
        f"───── CAPTAIN'S ORDERS ─────",
    ]
    for order in orders.get(session_type, []):
        lines.append(f"  • {order}")

    # PC ratio addendum
    if pc_ratio > 1.2:
        lines.append(
            f"  ⚠️  HIGH P/C ({pc_ratio:.2f}): Heavy put buying — watch for short squeeze if market firms"
        )
    elif pc_ratio < 0.75:
        lines.append(
            f"  ⚠️  LOW P/C ({pc_ratio:.2f}): Complacency risk — protect profits, watch for shakeout"
        )

    lines.append("")
    lines.append("Make it so, Captain. — Ready Room, out.")

    return "\n".join(lines)


# ── Main Briefing Function ────────────────────────────────────────────────────


def generate_ready_room_briefing(force: bool = False) -> dict:
    """
    Generate a complete Ready Room session briefing for SPY.

    Steps:
      1. Check in-memory cache (10 min TTL) unless force=True
      2. Load GEX profile (from Alpaca via gex_calculator; falls back to DB snapshot)
      3. Fetch VIX from yfinance
      4. Fetch P/C ratio (CBOE → SPY OI fallback)
      5. Calculate max pain from OI data
      6. Determine session type + signals
      7. Generate gameplan text
      8. Save to ready_room_briefings
      9. Return result dict

    Never drops or modifies existing rows.
    """
    global _CACHE

    now = time.time()
    if not force and _CACHE.get("ts") and (now - _CACHE["ts"]) < _CACHE_TTL:
        return dict(_CACHE.get("data", {}))

    _init_db()

    # ── 1. GEX profile ────────────────────────────────────────────────────
    profile = None
    try:
        from gex_calculator import compute_gex_sync, get_latest_snapshot
        profile = compute_gex_sync("SPY", force=force)
    except Exception as e:
        console.log(f"[yellow]ReadyRoom: live GEX unavailable ({e}), trying DB snapshot")

    if profile is None:
        # Fall back to cached DB snapshot
        try:
            from gex_calculator import get_latest_snapshot, _profile_from_snapshot
            snap = get_latest_snapshot("SPY")
            if snap:
                profile = _profile_from_snapshot(snap)
        except Exception as e2:
            console.log(f"[yellow]ReadyRoom: DB snapshot fallback failed: {e2}")

    if profile is None:
        return {"error": "GEX data unavailable — Alpaca API required", "briefing": None}

    spot        = profile.spot_price
    call_wall   = profile.call_wall
    put_wall    = profile.put_wall
    gamma_flip  = profile.zero_gamma_level
    max_gamma   = profile.max_gamma_strike
    total_gex   = profile.total_gex
    levels      = profile.levels  # list[GEXLevel]

    # ── 2. VIX ────────────────────────────────────────────────────────────
    vix = _get_vix()

    # ── 3. P/C ratio ──────────────────────────────────────────────────────
    pc_ratio = _get_pc_ratio_cboe()
    if pc_ratio is None:
        pc_ratio = _calc_pc_ratio_from_oi(levels)
        console.log(f"[dim]ReadyRoom: using SPY OI for P/C ratio = {pc_ratio:.2f}")
    else:
        console.log(f"[dim]ReadyRoom: CBOE P/C ratio = {pc_ratio:.2f}")

    # ── 4. Max pain ───────────────────────────────────────────────────────
    max_pain = _calc_max_pain(levels)
    if max_pain <= 0:
        max_pain = max_gamma  # fallback to max gamma strike

    # ── 5. Session type + signals ─────────────────────────────────────────
    session_type, signals = _determine_session_type(
        spot, total_gex, gamma_flip, put_wall, call_wall, max_pain, pc_ratio, vix
    )

    # ── 6. Gameplan ───────────────────────────────────────────────────────
    gameplan = _generate_gameplan(
        session_type, spot, call_wall, put_wall, max_pain, gamma_flip, pc_ratio, vix, signals
    )

    # ── 7. Build result ───────────────────────────────────────────────────
    now_dt = datetime.now()
    result = {
        "symbol":            "SPY",
        "session_date":      now_dt.strftime("%Y-%m-%d"),
        "session_time":      now_dt.strftime("%H:%M"),
        "spot_price":        round(spot, 2),
        "call_wall":         round(call_wall, 2),
        "put_wall":          round(put_wall, 2),
        "max_pain":          round(max_pain, 2),
        "gamma_flip":        round(gamma_flip, 2),
        "max_gamma_strike":  round(max_gamma, 2),
        "total_gex":         round(total_gex, 4),
        "total_gex_b":       round(total_gex / 1e9, 3),
        "pc_ratio":          round(pc_ratio, 3),
        "vix":               round(vix, 2),
        "session_type":      session_type,
        "session_color":     _SESSION_COLORS.get(session_type, "#888"),
        "session_icon":      _SESSION_ICONS.get(session_type, "📋"),
        "signals":           signals,
        "gameplan":          gameplan,
        "generated_at":      now_dt.isoformat(),
    }

    # ── 8. Persist ────────────────────────────────────────────────────────
    try:
        conn = _conn()
        conn.execute(
            """
            INSERT INTO ready_room_briefings
              (symbol, session_date, session_time, spot_price, call_wall, put_wall,
               max_pain, gamma_flip, max_gamma_strike, total_gex, pc_ratio, vix,
               session_type, signals_json, gameplan)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "SPY",
                result["session_date"],
                result["session_time"],
                result["spot_price"],
                result["call_wall"],
                result["put_wall"],
                result["max_pain"],
                result["gamma_flip"],
                result["max_gamma_strike"],
                result["total_gex"],
                result["pc_ratio"],
                result["vix"],
                result["session_type"],
                json.dumps(signals),
                gameplan,
            ),
        )
        conn.commit()
        conn.close()
        console.log(
            f"[bold green]Ready Room: {session_type} briefing saved "
            f"(SPY ${spot:.2f}, VIX {vix:.1f}, P/C {pc_ratio:.2f})"
        )
    except Exception as e:
        console.log(f"[red]ReadyRoom: save error: {e}")

    # ── 9. Cache and return ───────────────────────────────────────────────
    _CACHE["ts"]   = time.time()
    _CACHE["data"] = result
    return result


# ── Query Functions ───────────────────────────────────────────────────────────


def get_latest_briefing() -> dict:
    """Return the most recent ready_room_briefings row, or empty dict."""
    _init_db()
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT * FROM ready_room_briefings ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            d["signals"] = json.loads(d.get("signals_json") or "[]")
            d["session_color"] = _SESSION_COLORS.get(d.get("session_type", ""), "#888")
            d["session_icon"]  = _SESSION_ICONS.get(d.get("session_type", ""), "📋")
            d["total_gex_b"]   = round((d.get("total_gex") or 0) / 1e9, 3)
            return d
        return {}
    except Exception as e:
        console.log(f"[red]ReadyRoom: get_latest error: {e}")
        return {}


def get_briefing_history(limit: int = 7) -> list:
    """Return the last N briefings, newest first."""
    _init_db()
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT * FROM ready_room_briefings ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        results = []
        for row in rows:
            d = dict(row)
            d["signals"] = json.loads(d.get("signals_json") or "[]")
            d["session_color"] = _SESSION_COLORS.get(d.get("session_type", ""), "#888")
            d["session_icon"]  = _SESSION_ICONS.get(d.get("session_type", ""), "📋")
            d["total_gex_b"]   = round((d.get("total_gex") or 0) / 1e9, 3)
            results.append(d)
        return results
    except Exception as e:
        console.log(f"[red]ReadyRoom: get_history error: {e}")
        return []


def get_key_levels() -> dict:
    """
    Return just the structured key levels from the latest briefing.
    Used by agents to incorporate options structure into their analysis.
    """
    briefing = get_latest_briefing()
    if not briefing:
        return {"error": "No Ready Room briefing available yet"}
    return {
        "symbol":           briefing.get("symbol", "SPY"),
        "spot_price":       briefing.get("spot_price"),
        "call_wall":        briefing.get("call_wall"),
        "put_wall":         briefing.get("put_wall"),
        "max_pain":         briefing.get("max_pain"),
        "gamma_flip":       briefing.get("gamma_flip"),
        "max_gamma_strike": briefing.get("max_gamma_strike"),
        "total_gex_b":      briefing.get("total_gex_b"),
        "pc_ratio":         briefing.get("pc_ratio"),
        "vix":              briefing.get("vix"),
        "session_type":     briefing.get("session_type"),
        "generated_at":     briefing.get("created_at") or briefing.get("generated_at"),
    }
