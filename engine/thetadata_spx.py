"""Long Range Sensors (Optional Upgrade) — ThetaData SPX 0DTE Integration.

Completely optional module. If ThetaData is not configured or unreachable,
ALL functions return graceful fallbacks silently. Never crashes, never blocks.

Configuration via environment variables:
    THETADATA_USERNAME   — ThetaData account username
    THETADATA_PASSWORD   — ThetaData account password
    USE_SPX_DATA         — Must be "true" to enable (default: false)
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_USERNAME = os.environ.get("THETADATA_USERNAME", "")
_PASSWORD = os.environ.get("THETADATA_PASSWORD", "")
_USE_SPX  = os.environ.get("USE_SPX_DATA", "false").lower() == "true"
_BASE_URL = "http://127.0.0.1:25510"
_AVAILABLE = bool(_USERNAME and _PASSWORD and _USE_SPX)

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

# ---------------------------------------------------------------------------
# Module-level caches
# ---------------------------------------------------------------------------

_availability_cache: dict = {}          # keys: "result", "ts"
_availability_lock = threading.Lock()
_AVAIL_TTL = 300  # 5 minutes

_levels_cache: dict = {}                # keys: "data", "ts"
_levels_lock = threading.Lock()
_LEVELS_TTL = 600  # 10 minutes

# ---------------------------------------------------------------------------
# DB initialisation
# ---------------------------------------------------------------------------

_db_init_done = False
_db_init_lock = threading.Lock()


def _init_db() -> None:
    """Create spx_snapshots table if ThetaData is available. Always safe to call."""
    global _db_init_done
    with _db_init_lock:
        if _db_init_done:
            return
        _db_init_done = True

    if not _AVAILABLE:
        return

    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        con.execute("""
            CREATE TABLE IF NOT EXISTS spx_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT    NOT NULL,
                trade_date  TEXT    NOT NULL,
                spx_price   REAL,
                spx_max_pain  REAL,
                spx_put_wall  REAL,
                spx_call_wall REAL,
                spx_pc_ratio  REAL,
                spx_net_gex   REAL,
                source      TEXT    DEFAULT 'SPY-proxy',
                created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
            )
        """)
        con.commit()
        con.close()
    except Exception as exc:
        logger.warning("thetadata_spx: DB init failed — %s", exc)


# Run once at import time (safe even when _AVAILABLE is False)
_init_db()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SPY_PROXY_FALLBACK = {"available": False, "using": "SPY-proxy"}

_FULL_FALLBACK = {
    "available": False,
    "using": "SPY-proxy",
    "message": (
        "ThetaData not configured — set THETADATA_USERNAME, "
        "THETADATA_PASSWORD, USE_SPX_DATA=true in .env"
    ),
}


def _auth() -> tuple[str, str]:
    return (_USERNAME, _PASSWORD)


def _today() -> str:
    return datetime.now().strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def is_available() -> bool:
    """Return True only if ThetaData is configured AND the terminal is reachable.

    Result is cached for 5 minutes to avoid hammering the local terminal.
    """
    if not _AVAILABLE:
        return False

    with _availability_lock:
        if (
            _availability_cache.get("result") is not None
            and time.time() - _availability_cache.get("ts", 0) < _AVAIL_TTL
        ):
            return _availability_cache["result"]

    try:
        resp = requests.get(
            f"{_BASE_URL}/v2/at_time",
            auth=_auth(),
            timeout=2,
        )
        reachable = resp.status_code < 500
    except Exception:
        reachable = False

    with _availability_lock:
        _availability_cache["result"] = reachable
        _availability_cache["ts"] = time.time()

    return reachable


def get_spx_chain(expiry: str = None) -> dict:
    """Fetch the SPXW option chain from ThetaData and compute key levels.

    Args:
        expiry: Option expiration date in YYYYMMDD format. Defaults to today.

    Returns:
        Dict with computed levels, or SPY-proxy fallback if unavailable.
    """
    if not is_available():
        return _SPY_PROXY_FALLBACK.copy()

    if expiry is None:
        expiry = _today()

    try:
        resp = requests.get(
            f"{_BASE_URL}/v2/snapshot/option/chain",
            params={"root": "SPXW", "exp": expiry},
            auth=_auth(),
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("thetadata_spx: chain fetch failed — %s", exc)
        return _SPY_PROXY_FALLBACK.copy()

    # ThetaData returns a list of contract snapshots
    contracts = data if isinstance(data, list) else data.get("response", [])

    if not contracts:
        logger.warning("thetadata_spx: empty chain response for expiry=%s", expiry)
        return _SPY_PROXY_FALLBACK.copy()

    # Separate calls and puts; gather strike-level aggregates
    calls: dict[float, dict] = {}
    puts:  dict[float, dict] = {}

    spx_price = 0.0

    for c in contracts:
        try:
            strike    = float(c.get("strike", 0))
            opt_type  = str(c.get("option_type", c.get("right", ""))).upper()
            bid       = float(c.get("bid", 0) or 0)
            ask       = float(c.get("ask", 0) or 0)
            oi        = float(c.get("open_interest", c.get("oi", 0)) or 0)
            volume    = float(c.get("volume", 0) or 0)
            gamma     = float(c.get("gamma", 0) or 0)
            mid_price = (bid + ask) / 2 if (bid or ask) else 0.0

            # Rough spot estimate from near-ATM bid/ask midpoints (updated each iter)
            underlying = float(c.get("underlying_price", c.get("spot", 0)) or 0)
            if underlying:
                spx_price = underlying

            if opt_type in ("C", "CALL"):
                if strike not in calls:
                    calls[strike] = {"oi": 0.0, "volume": 0.0, "gamma": 0.0}
                calls[strike]["oi"]     += oi
                calls[strike]["volume"] += volume
                calls[strike]["gamma"]  += gamma
            elif opt_type in ("P", "PUT"):
                if strike not in puts:
                    puts[strike] = {"oi": 0.0, "volume": 0.0, "gamma": 0.0}
                puts[strike]["oi"]     += oi
                puts[strike]["volume"] += volume
                puts[strike]["gamma"]  += gamma
        except (TypeError, ValueError, KeyError):
            continue

    all_strikes = sorted(set(calls) | set(puts))

    if not all_strikes:
        logger.warning("thetadata_spx: no usable strikes parsed")
        return _SPY_PROXY_FALLBACK.copy()

    # ------------------------------------------------------------------
    # Max pain — strike that minimises total option holder loss
    # ------------------------------------------------------------------
    min_pain  = float("inf")
    max_pain  = all_strikes[0]

    for test_strike in all_strikes:
        pain = 0.0
        # Calls in-the-money below test_strike lose to call holders
        for cs, cd in calls.items():
            if cs < test_strike:
                pain += (test_strike - cs) * cd["oi"] * 100
        # Puts in-the-money above test_strike lose to put holders
        for ps, pd in puts.items():
            if ps > test_strike:
                pain += (ps - test_strike) * pd["oi"] * 100
        if pain < min_pain:
            min_pain = pain
            max_pain = test_strike

    # ------------------------------------------------------------------
    # Put wall — highest put OI strike below spot
    # ------------------------------------------------------------------
    put_wall: float | None = None
    max_put_oi = -1.0
    for strike in all_strikes:
        if strike < spx_price and puts.get(strike, {}).get("oi", 0) > max_put_oi:
            max_put_oi = puts[strike]["oi"]
            put_wall = strike

    # ------------------------------------------------------------------
    # Call wall — highest call OI strike above spot
    # ------------------------------------------------------------------
    call_wall: float | None = None
    max_call_oi = -1.0
    for strike in all_strikes:
        if strike > spx_price and calls.get(strike, {}).get("oi", 0) > max_call_oi:
            max_call_oi = calls[strike]["oi"]
            call_wall = strike

    # ------------------------------------------------------------------
    # P/C ratio
    # ------------------------------------------------------------------
    total_put_oi  = sum(d["oi"] for d in puts.values())
    total_call_oi = sum(d["oi"] for d in calls.values())
    pc_ratio = round(total_put_oi / total_call_oi, 4) if total_call_oi else 0.0

    # ------------------------------------------------------------------
    # Net GEX — (call gamma * call OI - put gamma * put OI) * 100 * spot
    # ------------------------------------------------------------------
    net_gex = 0.0
    if spx_price:
        call_gex = sum(d["gamma"] * d["oi"] for d in calls.values())
        put_gex  = sum(d["gamma"] * d["oi"] for d in puts.values())
        net_gex  = round((call_gex - put_gex) * 100 * spx_price, 2)

    # ------------------------------------------------------------------
    # Persist snapshot
    # ------------------------------------------------------------------
    now_str   = datetime.now().isoformat(timespec="seconds")
    today_str = datetime.now().strftime("%Y-%m-%d")

    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        con.execute(
            """
            INSERT INTO spx_snapshots
                (timestamp, trade_date, spx_price, spx_max_pain, spx_put_wall,
                 spx_call_wall, spx_pc_ratio, spx_net_gex, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_str, today_str,
                spx_price, max_pain, put_wall, call_wall, pc_ratio, net_gex,
                "THETADATA",
            ),
        )
        con.commit()
        con.close()
    except Exception as exc:
        logger.warning("thetadata_spx: snapshot DB write failed — %s", exc)

    return {
        "available":     True,
        "spx_price":     round(spx_price, 2),
        "max_pain":      max_pain,
        "put_wall":      put_wall,
        "call_wall":     call_wall,
        "pc_ratio":      pc_ratio,
        "net_gex":       net_gex,
        "source":        "THETADATA",
        "expiry":        expiry,
        "strikes_count": len(all_strikes),
    }


def get_spx_levels(force: bool = False) -> dict:
    """Return SPX key levels, cached for 10 minutes.

    Args:
        force: Bypass the cache and fetch fresh data.

    Returns:
        Dict with SPX levels, or a descriptive fallback if unavailable.
    """
    if not _AVAILABLE:
        return _FULL_FALLBACK.copy()

    with _levels_lock:
        if (
            not force
            and _levels_cache.get("data")
            and time.time() - _levels_cache.get("ts", 0) < _LEVELS_TTL
        ):
            return _levels_cache["data"]

    try:
        result = get_spx_chain()
    except Exception as exc:
        logger.warning("thetadata_spx: get_spx_levels error — %s", exc)
        return _FULL_FALLBACK.copy()

    if not result.get("available"):
        return _FULL_FALLBACK.copy()

    with _levels_lock:
        _levels_cache["data"] = result
        _levels_cache["ts"]   = time.time()

    return result


def get_latest_spx_snapshot() -> dict | None:
    """Return the most recent spx_snapshots row from the DB as a dict.

    Returns:
        Dict with snapshot data, or None if the table is empty or unavailable.
    """
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM spx_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        con.close()
        return dict(row) if row else None
    except Exception as exc:
        logger.warning("thetadata_spx: get_latest_spx_snapshot failed — %s", exc)
        return None


def get_data_source_badge() -> str:
    """Return a display badge indicating the active data source.

    Returns:
        "SPX DIRECT" if ThetaData is available, "SPY PROXY" otherwise.
    """
    return "SPX DIRECT" if is_available() else "SPY PROXY"
