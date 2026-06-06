"""HM-ARCHER-REBUILD — Convergence counter.

Counts how many of 5 independent systems flag the same symbol:
  crew     — war_room takes (last 24h)
  gex      — SPY/QQQ notable gamma regime ∪ Uhura tickers_flagged/suggested
  congress — congressional trades (0 today; scraper broken — caps at 4/5)
  ollie    — Ollie AI deep-scan convergence (≥2 strategies)
  supermax — SUPER_MAX shadow-bridge edges

Tier: RED = 5/5, YELLOW = 3-4/5, INFO = <3. (5/5 unreachable until congress
is restored — that's correct, not forced.)
"""
from __future__ import annotations

import logging
from collections import defaultdict

from engine.archer import intel_sources as src

logger = logging.getLogger(__name__)

SYSTEMS = ["crew", "gex", "congress", "ollie", "supermax"]


def _gex_options_symbols() -> set[str]:
    """GEX-options leg: notable index gamma regimes ∪ Uhura flagged tickers."""
    flagged: set[str] = set()

    # Index leg — flag SPY/QQQ only when in a notable (non-stable/short-gamma)
    # regime. Read the LABEL, never the sign of total_gex.
    gex = src.get_gex()
    for idx in ("SPY", "QQQ"):
        d = gex.get(idx) or {}
        label = (d.get("regime") or "").upper()
        if "SHORT GAMMA" in label or "VOLATILE" in label or "BELOW FLIP" in label:
            flagged.add(idx)

    # Options-flow leg — Uhura's per-symbol confluence
    uh = src.get_uhura()
    for t in (uh.get("tickers_flagged") or []):
        sym = t if isinstance(t, str) else (t or {}).get("ticker") or (t or {}).get("symbol")
        if sym:
            flagged.add(str(sym).upper())
    # suggested_ticker only counts when there's an actual directional rec
    if uh.get("suggested_ticker") and uh.get("recommended_trade") not in (None, "", "NO_TRADE"):
        flagged.add(str(uh["suggested_ticker"]).upper())

    return flagged


def compute_convergence() -> list[dict]:
    """Return symbols ranked by how many systems flag them, with which ones."""
    hits: dict[str, set[str]] = defaultdict(set)

    try:
        for c in src.get_crew_consensus():
            if c.get("symbol"):
                hits[str(c["symbol"]).upper()].add("crew")
    except Exception as e:
        logger.warning("[Archer/conv] crew leg failed: %s: %r", type(e).__name__, e)

    try:
        for sym in _gex_options_symbols():
            hits[sym].add("gex")
    except Exception as e:
        logger.warning("[Archer/conv] gex leg failed: %s: %r", type(e).__name__, e)

    try:
        for c in src.get_congress():
            if c.get("symbol"):
                hits[str(c["symbol"]).upper()].add("congress")
    except Exception as e:
        logger.warning("[Archer/conv] congress leg failed: %s: %r", type(e).__name__, e)

    try:
        for o in src.get_ollie_scanner():
            if o.get("signals", 0) >= 2 and o.get("symbol"):
                hits[str(o["symbol"]).upper()].add("ollie")
    except Exception as e:
        logger.warning("[Archer/conv] ollie leg failed: %s: %r", type(e).__name__, e)

    try:
        for e_ in src.get_supermax_edges():
            if e_.get("symbol"):
                hits[str(e_["symbol"]).upper()].add("supermax")
    except Exception as e:
        logger.warning("[Archer/conv] supermax leg failed: %s: %r", type(e).__name__, e)

    out = []
    for sym, systems in hits.items():
        n = len(systems)
        out.append({
            "symbol": sym,
            "count": n,
            "systems": sorted(systems),
            "tier": "RED" if n >= 5 else ("YELLOW" if n >= 3 else "INFO"),
        })
    out.sort(key=lambda x: x["count"], reverse=True)
    return out
