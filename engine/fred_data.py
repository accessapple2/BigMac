"""
FRED (Federal Reserve Economic Data) integration.
Fetches macro indicators and computes regime signals.
Requires FRED_API_KEY environment variable.
"""
# HM-AO 2026-05-17: orphan revival surfaced latent Py3.9 incompatibility — the
# existing `float | None` annotations (e.g. L35) only parse under 3.10+. This
# pragma makes ALL annotations lazy strings (PEP 563), so 3.9 + 3.10+ both work.
# Doesn't change runtime semantics. Required because trader runs CPython 3.9.
from __future__ import annotations

import os
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def _fetch_series(api_key: str, series_id: str, limit: int = 5) -> list[dict]:
    """Fetch the most recent N observations for a FRED series."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit,
    }
    try:
        resp = requests.get(FRED_BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("observations", [])
    except Exception as e:
        logger.warning(f"FRED fetch failed for {series_id}: {e}")
        return []


def _parse_value(obs: dict) -> float | None:
    """Parse a FRED observation value, returning None for missing data."""
    val = obs.get("value", ".")
    if val == "." or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _compute_trend(current: float | None, previous: float | None) -> str:
    """Return RISING, FALLING, or FLAT based on direction of change."""
    if current is None or previous is None:
        return "FLAT"
    delta = current - previous
    if delta > 0.01:
        return "RISING"
    elif delta < -0.01:
        return "FALLING"
    return "FLAT"


def get_fred_indicators() -> dict:
    """
    Fetch key macro indicators from the FRED API.

    Returns a dict keyed by series ID, each containing:
        name, signal description, value, previous, trend, last_updated
    """
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        logger.error("FRED_API_KEY not set in environment")
        return {}

    indicators = {
        'T10Y2Y': {
            'name': 'Yield Curve (10Y-2Y)',
            'signal': 'BEARISH if inverted (<0), watch flattening',
        },
        'UNRATE': {
            'name': 'Unemployment Rate',
            'signal': 'RISK_OFF if rising, RISK_ON if falling',
        },
        'CPIAUCSL': {
            'name': 'CPI Inflation (All Items)',
            'signal': 'RISK_OFF if rising (hot inflation), RISK_ON if falling',
        },
        'ICSA': {
            'name': 'Initial Jobless Claims',
            'signal': 'RISK_OFF if >250k, RISK_ON if below',
        },
        'DFF': {
            'name': 'Federal Funds Effective Rate',
            'signal': 'Context for monetary policy tightness',
        },
        'BAMLH0A0HYM2': {
            'name': 'High Yield Credit Spread (OAS)',
            'signal': 'RISK_OFF if widening, RISK_ON if tightening',
        },
    }

    for series_id, meta in indicators.items():
        observations = _fetch_series(api_key, series_id, limit=5)

        # FRED returns desc order; index 0 = most recent
        current = _parse_value(observations[0]) if len(observations) > 0 else None
        previous = _parse_value(observations[1]) if len(observations) > 1 else None

        last_updated = observations[0].get("date") if observations else None

        meta["value"] = current
        meta["previous"] = previous
        meta["trend"] = _compute_trend(current, previous)
        meta["last_updated"] = last_updated

    return indicators


def get_macro_regime_signal(indicators: dict | None = None) -> dict:
    """
    Score macro conditions and return a regime signal.

    Args:
        indicators: output of get_fred_indicators(). Fetched fresh if not provided.

    Returns:
        {
            "regime": "BULLISH" | "BEARISH" | "NEUTRAL",
            "bullish_signals": int,
            "bearish_signals": int,
            "details": list[str],
        }
    """
    if indicators is None:
        indicators = get_fred_indicators()

    bullish_signals = 0
    bearish_signals = 0
    details = []

    # Yield curve
    if 'T10Y2Y' in indicators:
        val = indicators['T10Y2Y']['value']
        if val is not None:
            if val < 0:
                bearish_signals += 2
                details.append(f"T10Y2Y={val:.2f} (inverted) → bearish +2")
            elif val < 0.5:
                bearish_signals += 1
                details.append(f"T10Y2Y={val:.2f} (flat) → bearish +1")
            else:
                bullish_signals += 1
                details.append(f"T10Y2Y={val:.2f} (normal) → bullish +1")

    # Unemployment rate
    if 'UNRATE' in indicators:
        trend = indicators['UNRATE']['trend']
        val = indicators['UNRATE']['value']
        if trend == 'RISING':
            bearish_signals += 1
            details.append(f"UNRATE={val} (rising) → bearish +1")
        elif trend == 'FALLING':
            bullish_signals += 1
            details.append(f"UNRATE={val} (falling) → bullish +1")

    # Initial jobless claims
    if 'ICSA' in indicators:
        val = indicators['ICSA']['value']
        if val is not None:
            if val > 250000:
                bearish_signals += 1
                details.append(f"ICSA={val:,.0f} (>250k) → bearish +1")
            else:
                bullish_signals += 1
                details.append(f"ICSA={val:,.0f} (healthy) → bullish +1")

    # CPI inflation
    if 'CPIAUCSL' in indicators:
        trend = indicators['CPIAUCSL']['trend']
        val = indicators['CPIAUCSL']['value']
        if trend == 'RISING':
            bearish_signals += 1
            details.append(f"CPIAUCSL={val} (rising) → bearish +1")
        elif trend == 'FALLING':
            bullish_signals += 1
            details.append(f"CPIAUCSL={val} (falling) → bullish +1")

    # Credit spreads - widening = risk off, tightening = risk on
    if 'BAMLH0A0HYM2' in indicators:
        val = indicators['BAMLH0A0HYM2']['value']
        trend = indicators['BAMLH0A0HYM2']['trend']
        if val is not None:
            if val > 5.0:
                bearish_signals += 3
                details.append(f"BAMLH0A0HYM2={val:.2f} (extreme stress) → bearish +3")
            elif val > 4.0:
                bearish_signals += 2
                details.append(f"BAMLH0A0HYM2={val:.2f} (elevated risk) → bearish +2")
            elif val > 3.0:
                bearish_signals += 1
                details.append(f"BAMLH0A0HYM2={val:.2f} (caution zone) → bearish +1")
            else:
                bullish_signals += 1
                details.append(f"BAMLH0A0HYM2={val:.2f} (compressed spreads) → bullish +1")
        if trend == 'RISING':
            bearish_signals += 1
            details.append("BAMLH0A0HYM2 widening → bearish +1")
        elif trend == 'FALLING':
            bullish_signals += 1
            details.append("BAMLH0A0HYM2 tightening → bullish +1")

    # Final regime
    if bearish_signals >= 3:
        regime = "BEARISH"
    elif bullish_signals >= 3:
        regime = "BULLISH"
    else:
        regime = "NEUTRAL"

    return {
        "regime": regime,
        "bullish_signals": bullish_signals,
        "bearish_signals": bearish_signals,
        "details": details,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HM-AO 2026-05-17: CARTS retail nowcast (FRED release_id=494)
# APPENDED — does NOT modify any existing function above.
# Sacred: INSERT OR REPLACE only. NO DROP, NO TRUNCATE, NO DELETE.
# ─────────────────────────────────────────────────────────────────────────────

import sqlite3
from datetime import date as _date

CARTS_RELEASE_ID = 494
_FRED_RELEASE_SERIES_URL = "https://api.stlouisfed.org/fred/release/series"
_TRADER_DB = os.path.expanduser("~/autonomous-trader/data/trader.db")
_CARTS_NTFY_TOPIC = "ollietrades-admin"


def get_carts_series_catalog() -> list[dict]:
    """
    Discover all current CARTS series from FRED release_id=494.
    Self-healing — auto-picks-up new series when Chicago Fed adds them.
    Returns list of dicts (FRED's 'seriess' shape). [] on any failure, never raises.
    """
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        logger.warning("FRED_API_KEY not set — CARTS catalog skipped")
        return []
    params = {
        "release_id": CARTS_RELEASE_ID,
        "api_key":    api_key,
        "file_type":  "json",
    }
    try:
        resp = requests.get(_FRED_RELEASE_SERIES_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("seriess", [])  # FRED's actual key is 'seriess'
    except Exception as e:
        logger.warning(f"FRED CARTS catalog fetch failed: {e}")
        return []


def fetch_carts_observations(series_id: str, limit: int = 260) -> list[dict]:
    """Up to ~5 years of weekly CARTS observations for one series.
    Reuses _fetch_series helper above. [] on any failure."""
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        return []
    return _fetch_series(api_key, series_id, limit=limit)


def _pick_primary_nowcast_series(catalog: list[dict]) -> dict | None:
    """
    Pick the catalog series whose title contains 'projection' AND 'sales'
    AND 'month' (case-insensitive). Fall back to catalog[0] if no match.
    Returns None only if catalog is empty.
    """
    if not catalog:
        return None
    for s in catalog:
        title = (s.get("title") or "").lower()
        if "projection" in title and "sales" in title and "month" in title:
            return s
    return catalog[0]


def get_carts_nowcast() -> dict:
    """
    Latest CARTS retail sales nowcast for downstream consumers.

    Returns:
        {
            nowcast_pct_mom: float | None,
            prev_pct_mom:    float | None,
            direction:       "RISING" | "FALLING" | "FLAT",
            last_obs_date:   "YYYY-MM-DD",
            series_id:       str,
            series_title:    str,
            stale_days:      int  (-1 if undeterminable)
        }
    {} on any failure, never raises.
    """
    try:
        catalog = get_carts_series_catalog()
        primary = _pick_primary_nowcast_series(catalog)
        if not primary:
            return {}
        series_id = primary.get("id", "")
        series_title = primary.get("title", "")
        obs = fetch_carts_observations(series_id, limit=2)
        if not obs:
            return {}
        current  = _parse_value(obs[0]) if len(obs) > 0 else None
        previous = _parse_value(obs[1]) if len(obs) > 1 else None
        last_obs_date = obs[0].get("date") if obs else None
        direction = _compute_trend(current, previous)
        stale_days = -1
        if last_obs_date:
            try:
                d = _date.fromisoformat(last_obs_date)
                stale_days = (_date.today() - d).days
            except Exception:
                stale_days = -1
        return {
            "nowcast_pct_mom": current,
            "prev_pct_mom":    previous,
            "direction":       direction,
            "last_obs_date":   last_obs_date,
            "series_id":       series_id,
            "series_title":    series_title,
            "stale_days":      stale_days,
        }
    except Exception as e:
        logger.warning(f"get_carts_nowcast failed: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Persistence — fred_carts table (HM-AO Phase 2)
# ─────────────────────────────────────────────────────────────────────────────

def _carts_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_TRADER_DB, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_carts_schema(conn: sqlite3.Connection) -> None:
    """Idempotent — CREATE IF NOT EXISTS only. Never touches existing tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fred_carts (
            series_id   TEXT NOT NULL,
            obs_date    TEXT NOT NULL,
            value       REAL,
            fetched_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (series_id, obs_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fred_carts_date ON fred_carts(obs_date)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fred_carts_meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()


def _carts_meta_get(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM fred_carts_meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def _carts_meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO fred_carts_meta(key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


def _ntfy_carts_release(series_title: str, nowcast: float | None, prev: float | None) -> None:
    """Fire ntfy on NEW obs_date — fail-soft, never raises into the persist loop.
    HTTP headers must be ASCII/latin-1; emoji in Title bombs requests' default
    encoding. Use ntfy's Tags-to-emoji substitution instead — `bar_chart` → 📊.
    """
    try:
        nc = f"{nowcast:+.2f}" if nowcast is not None else "n/a"
        pv = f"{prev:+.2f}"   if prev   is not None else "n/a"
        body = (
            f"{series_title}: {nc}% m/m (prev {pv}%). "
            f"Census retail sales release likely tomorrow."
        )
        # HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10: delegates to the hardened
        # engine.alert_channels._send_ntfy() (forces IPv4) instead of a
        # separate requests.post() implementation of the same POST.
        from engine.alert_channels import _send_ntfy
        _send_ntfy(
            title="CARTS nowcast updated",   # ASCII-only: emoji via Tags
            message=body,
            priority="default",
            tags="bar_chart,carts,fred,nowcast",  # bar_chart renders 📊
            topic=_CARTS_NTFY_TOPIC,
        )
    except Exception as e:
        logger.warning(f"CARTS ntfy failed: {e}")


def persist_carts_all() -> dict:
    """
    Fetch all CARTS series from catalog, persist observations to fred_carts.
    Uses INSERT OR REPLACE on PK (series_id, obs_date) — fully idempotent.
    Fires ntfy iff the primary nowcast series has a NEW MAX(obs_date) vs prior.

    Returns:
        {series_count: int, rows_written: int, errors: list[str]}

    SACRED: no DROP, no TRUNCATE, no DELETE.
    """
    out = {"series_count": 0, "rows_written": 0, "errors": []}
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        out["errors"].append("FRED_API_KEY missing")
        return out

    catalog = get_carts_series_catalog()
    out["series_count"] = len(catalog)
    if not catalog:
        out["errors"].append("CARTS catalog empty")
        return out

    primary = _pick_primary_nowcast_series(catalog)
    primary_id    = (primary or {}).get("id", "")
    primary_title = (primary or {}).get("title", "")

    conn = _carts_conn()
    try:
        _ensure_carts_schema(conn)

        for s in catalog:
            sid = s.get("id")
            if not sid:
                continue
            try:
                obs = _fetch_series(api_key, sid, limit=260)
                for o in obs:
                    obs_date = o.get("date")
                    if not obs_date:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO fred_carts "
                        "(series_id, obs_date, value, fetched_at) "
                        "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                        (sid, obs_date, _parse_value(o)),
                    )
                    out["rows_written"] += 1
                conn.commit()
            except Exception as e:
                out["errors"].append(f"{sid}: {e}")

        # ntfy iff primary's MAX(obs_date) advanced vs prior run
        if primary_id:
            row = conn.execute(
                "SELECT MAX(obs_date) FROM fred_carts WHERE series_id=?",
                (primary_id,),
            ).fetchone()
            current_max = row[0] if row else None
            prior_max   = _carts_meta_get(conn, "last_seen_primary")
            if current_max and current_max != prior_max:
                # Pull current + prev values for the body
                obs2 = _fetch_series(api_key, primary_id, limit=2)
                nc = _parse_value(obs2[0]) if len(obs2) > 0 else None
                pv = _parse_value(obs2[1]) if len(obs2) > 1 else None
                _ntfy_carts_release(primary_title, nc, pv)
                _carts_meta_set(conn, "last_seen_primary", current_max)
    finally:
        conn.close()

    return out
