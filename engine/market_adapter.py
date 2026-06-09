"""engine/market_adapter.py — HM-PHASE2 config-swappable market-data adapter.

The single seam between the fleet and market-data providers. DEFAULT = Alpaca tier
(free; daily bars via Alpaca IEX, futures via get_stock_price). Flip
`MARKET_DATA_PROVIDER=polygon` (or `alpaca_sip`) for a paid upgrade — ONE env change,
no rewiring; both providers return identical {symbol: df[OHLCV]} shape.

Every return is tagged {source, tier} so free-vs-paid labeling stays honest downstream
(REVEILLE brief, PHASER-LOCK targets).

GRADUATION GUARD (cost doctrine): stays on the free tier until a DEMONSTRATED trigger —
real-time precision need, or a coverage gap producing bad targets. No auto-graduation;
paid is a deliberate, logged env flip. See CLAUDE.md "Free Models First".
"""
from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)

_PAID_PROVIDERS = {"polygon", "alpaca_sip"}


def provider() -> str:
    return os.getenv("MARKET_DATA_PROVIDER", "alpaca").lower()


def tier() -> str:
    return "paid" if provider() in _PAID_PROVIDERS else "free"


def bulk_daily_ohlcv(symbols, range_str: str = "3mo"):
    """Prior-session daily OHLCV for many symbols → ({symbol: df}, meta).

    df columns: Open, High, Low, Close, Volume. Used by PHASER-LOCK's ATR/level
    target model. Provider-swappable; falls back to free Alpaca on paid-path error.
    """
    syms = list(symbols)
    if not syms:
        return {}, {"source": "none", "tier": tier()}
    prov = provider()
    if prov == "polygon":
        try:
            from engine.market_data import get_polygon_bars
            out = get_polygon_bars(syms)
            if isinstance(out, dict) and out:
                return out, {"source": "polygon", "tier": "paid"}
            logger.warning("market_adapter: polygon returned empty — falling back to alpaca")
        except Exception as e:
            logger.warning("market_adapter: polygon bulk failed (%s) — falling back to alpaca", type(e).__name__)
    from engine.market_data import get_bulk_daily_ohlcv
    return get_bulk_daily_ohlcv(syms, range_str), {"source": "alpaca_iex", "tier": "free"}


def futures():
    """Index/commodity futures + VIX snapshot for the REVEILLE tape → ({label: {...}}, meta)."""
    from engine.market_data import get_stock_price
    syms = {"S&P 500": "ES=F", "Nasdaq 100": "NQ=F", "Dow": "YM=F",
            "Russell 2000": "RTY=F", "WTI Crude": "CL=F", "Gold": "GC=F", "VIX": "^VIX"}
    out = {}
    for label, sym in syms.items():
        try:
            d = get_stock_price(sym)
            price = (d or {}).get("price")
            if price:
                out[label] = {"symbol": sym, "price": price,
                              "change_pct": (d.get("change_pct") if d else None)}
        except Exception:
            continue
    return out, {"source": "alpaca/yahoo", "tier": tier()}


def sector_tape():
    """Sector rotation heatmap for the REVEILLE tape → (heatmap, meta)."""
    try:
        from engine.sector_heatmap import get_sector_heatmap
        return get_sector_heatmap(), {"source": "sector_heatmap", "tier": "free"}
    except Exception as e:
        logger.warning("market_adapter: sector_tape failed: %r", e)
        return {}, {"source": "sector_heatmap", "tier": "free", "error": type(e).__name__}
