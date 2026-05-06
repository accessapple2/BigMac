"""Options utility helpers — OCC parsing, spread-leg awareness.

HM-AF-β 2026-05-06: ``is_spread_leg(symbol)`` gate for close paths
P1 (battle_station), P2 (alpaca_options.close_all_options), and
P3 (dayblade.py post-trade close). Lets standalone option closes
through while skipping legs of currently-open spreads.

Implementation notes:
- OCC symbol is parsed locally; ``options_trades.legs_json`` does NOT store
  OCC strings — it stores ``(action, option_type, strike, expiration, premium)``
  per leg. Matching is therefore on (underlying, expiration, option_type, strike).
- Filter is ``status='open' AND exec_status='open'`` so test_cleanup,
  failed_pre_fix, and other dormant rows are ignored.
- 30s in-memory TTL cache: P1 fires every 2 min over ≤10 positions, so the
  cache caps SQL hits at one read per 30s regardless of leg-set size.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
from datetime import datetime

logger = logging.getLogger("options_utils")

DB = "data/trader.db"

_OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")

_LEG_CACHE_TTL_SECS = 30.0
_leg_cache_lock = threading.Lock()
_leg_cache_ts: float = 0.0
_leg_cache_set: frozenset[tuple[str, str, str, float]] = frozenset()


def parse_occ_symbol(occ: str) -> dict | None:
    """Parse an OCC option symbol like ``SPY260515P00732000``.

    Returns ``{underlying, expiration, option_type, strike}`` with
    ``expiration`` as ISO ``YYYY-MM-DD`` and ``option_type`` as
    ``'call'`` or ``'put'``. Returns ``None`` for non-OCC strings.
    """
    if not occ:
        return None
    m = _OCC_RE.match(occ.upper().strip())
    if not m:
        return None
    underlying = m.group(1)
    yymmdd = m.group(2)
    option_type = "call" if m.group(3) == "C" else "put"
    strike = int(m.group(4)) / 1000.0
    try:
        expiration = datetime.strptime(yymmdd, "%y%m%d").date().isoformat()
    except ValueError:
        return None
    return {
        "underlying": underlying,
        "expiration": expiration,
        "option_type": option_type,
        "strike": float(strike),
    }


def _load_open_spread_legs() -> frozenset[tuple[str, str, str, float]]:
    """Read open-spread legs from options_trades. Returns frozenset of
    (underlying, expiration_iso, option_type, strike) tuples."""
    legs: set[tuple[str, str, str, float]] = set()
    try:
        conn = sqlite3.connect(DB)
        try:
            rows = conn.execute(
                "SELECT symbol, expiration, legs_json "
                "FROM options_trades "
                "WHERE status = 'open' AND exec_status = 'open'"
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as e:
        logger.warning(f"open spread legs query failed: {type(e).__name__}: {e!r}")
        return frozenset()

    for symbol, expiration, legs_json in rows:
        if not symbol or not legs_json:
            continue
        try:
            parsed = json.loads(legs_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(parsed, list):
            continue
        for leg in parsed:
            if not isinstance(leg, dict):
                continue
            try:
                option_type = str(leg["option_type"]).lower()
                strike = float(leg["strike"])
            except (KeyError, TypeError, ValueError):
                continue
            leg_exp = str(leg.get("expiration") or expiration or "")
            if not leg_exp:
                continue
            legs.add((str(symbol).upper(), leg_exp, option_type, strike))
    return frozenset(legs)


def _get_open_spread_legs(force_refresh: bool = False) -> frozenset[tuple[str, str, str, float]]:
    """Cached open-leg set. Refreshes every ``_LEG_CACHE_TTL_SECS`` seconds."""
    global _leg_cache_ts, _leg_cache_set
    with _leg_cache_lock:
        now = time.time()
        if force_refresh or (now - _leg_cache_ts) > _LEG_CACHE_TTL_SECS:
            _leg_cache_set = _load_open_spread_legs()
            _leg_cache_ts = now
        return _leg_cache_set


def is_spread_leg(occ_symbol: str) -> bool:
    """True iff ``occ_symbol`` matches a leg of a currently-open spread trade.

    HM-AF-β 2026-05-06. Used by close paths to skip spread legs while
    allowing standalone option closes through. Cache TTL 30s.
    """
    parsed = parse_occ_symbol(occ_symbol)
    if not parsed:
        return False
    key = (
        parsed["underlying"],
        parsed["expiration"],
        parsed["option_type"],
        parsed["strike"],
    )
    return key in _get_open_spread_legs()


def has_open_spread_legs() -> bool:
    """True iff any ``status='open' AND exec_status='open'`` spread trade exists.

    Defense-in-depth helper for P3 observability.
    """
    return bool(_get_open_spread_legs())
