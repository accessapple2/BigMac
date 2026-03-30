"""Sector rotation tracker — categorize watchlist by GICS sector, track money flow."""
from __future__ import annotations
import sqlite3
from datetime import datetime

DB = "data/trader.db"
DEFAULT_SECTOR_CAPS = {
    "Tech/Semi": 0.35,
    "Software": 0.30,
    "Mega Cap": 0.35,
    "Index": 0.40,
    "Other": 0.25,
}

# GICS sector mapping for watchlist stocks
SECTOR_MAP = {
    "SPY": "Index",
    "QQQ": "Index",
    "NVDA": "Semiconductors",
    "AMD": "Semiconductors",
    "MU": "Semiconductors",
    "AVGO": "Semiconductors",
    "TSLA": "Consumer Discretionary",
    "AAPL": "Consumer Electronics",
    "META": "Communication Services",
    "GOOGL": "Communication Services",
    "MSFT": "Software",
    "NOW": "Software",
    "DELL": "Hardware",
    "AMZN": "Consumer Discretionary",
    "ORCL": "Software",
    "PLTR": "Software",
}

# Broader grouping for rotation analysis
SECTOR_GROUPS = {
    "Tech/Semi": ["NVDA", "AMD", "MU", "AVGO"],
    "Software": ["MSFT", "NOW", "ORCL", "PLTR"],
    "Mega Cap": ["AAPL", "GOOGL", "META", "AMZN"],
    "Index": ["SPY", "QQQ"],
    "Other": ["TSLA", "DELL"],
}


def get_sector(symbol: str) -> str:
    return SECTOR_MAP.get(symbol, "Unknown")


def get_sector_group(symbol: str) -> str:
    for group, symbols in SECTOR_GROUPS.items():
        if symbol in symbols:
            return group
    return "Other"


def get_sector_caps() -> dict:
    return dict(DEFAULT_SECTOR_CAPS)


def build_sector_bucket_profile(positions: list[dict], proposed_symbol: str | None = None,
                                proposed_value: float = 0.0, total_value: float = 0.0) -> dict:
    """Build sector bucket exposure and cap diagnostics for stock positions."""
    exposures: dict[str, float] = {}
    symbol_groups: dict[str, str] = {}

    computed_total = total_value
    if computed_total <= 0:
        computed_total = 0.0

    for pos in positions:
        if (pos.get("asset_type") or "stock") != "stock":
            continue
        symbol = (pos.get("symbol") or "").upper()
        if not symbol:
            continue
        current = pos.get("current_price") or pos.get("avg_price") or 0
        value = max(0.0, float(pos.get("qty", 0)) * float(current))
        group = get_sector_group(symbol)
        symbol_groups[symbol] = group
        exposures[group] = exposures.get(group, 0.0) + value
        if total_value <= 0:
            computed_total += value

    proposed_group = None
    if proposed_symbol and proposed_value > 0:
        proposed_group = get_sector_group(proposed_symbol.upper())
        exposures[proposed_group] = exposures.get(proposed_group, 0.0) + proposed_value
        computed_total += proposed_value

    caps = get_sector_caps()
    buckets = []
    warnings = []
    for group, value in exposures.items():
        cap_pct = caps.get(group, caps["Other"]) * 100
        pct = (value / computed_total * 100) if computed_total > 0 else 0.0
        cap_status = "ok"
        if pct > cap_pct:
            cap_status = "over_cap"
            warnings.append(f"{group} bucket at {pct:.1f}% above {cap_pct:.0f}% cap")
        elif pct > cap_pct * 0.85:
            cap_status = "near_cap"
            warnings.append(f"{group} bucket at {pct:.1f}% near {cap_pct:.0f}% cap")
        buckets.append({
            "sector": group,
            "value": round(value, 2),
            "pct": round(pct, 2),
            "cap_pct": round(cap_pct, 2),
            "status": cap_status,
            "includes_proposed": bool(proposed_group and group == proposed_group),
        })
    buckets.sort(key=lambda item: item["value"], reverse=True)
    return {
        "buckets": buckets,
        "warnings": warnings,
        "proposed_group": proposed_group,
        "caps": {k: round(v * 100, 2) for k, v in caps.items()},
        "updated": datetime.now().isoformat(),
    }


def get_sector_rotation(prices: dict) -> list:
    """Calculate sector performance from current prices.

    Returns list of {sector, symbols, avg_change_pct, total_volume, flow_direction}.
    """
    sectors = {}
    for sym, data in prices.items():
        group = get_sector_group(sym)
        if group not in sectors:
            sectors[group] = {"symbols": [], "changes": [], "volumes": []}
        sectors[group]["symbols"].append(sym)
        sectors[group]["changes"].append(data.get("change_pct", 0))
        sectors[group]["volumes"].append(data.get("volume", 0))

    result = []
    for sector, info in sectors.items():
        avg_change = sum(info["changes"]) / len(info["changes"]) if info["changes"] else 0
        total_vol = sum(info["volumes"])

        if avg_change > 0.5:
            flow = "inflow"
        elif avg_change < -0.5:
            flow = "outflow"
        else:
            flow = "neutral"

        result.append({
            "sector": sector,
            "symbols": info["symbols"],
            "avg_change_pct": round(avg_change, 2),
            "total_volume": total_vol,
            "flow_direction": flow,
            "symbol_details": [
                {"symbol": sym, "change_pct": chg}
                for sym, chg in zip(info["symbols"], info["changes"])
            ],
        })

    result.sort(key=lambda x: x["avg_change_pct"], reverse=True)
    return result


def get_sector_exposure(player_id: str = None) -> list:
    """Get portfolio exposure by sector group across all players (or one)."""
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    if player_id:
        positions = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id=? AND asset_type='stock'",
            (player_id,)
        ).fetchall()
    else:
        positions = conn.execute(
            "SELECT symbol, SUM(qty * avg_price) as cost_basis "
            "FROM positions WHERE asset_type='stock' AND player_id != 'dayblade-0dte' "
            "GROUP BY symbol"
        ).fetchall()
    conn.close()

    sector_exposure = {}
    total = 0
    for pos in positions:
        sym = pos["symbol"]
        value = pos["qty"] * pos["avg_price"] if player_id else pos["cost_basis"]
        group = get_sector_group(sym)
        sector_exposure[group] = sector_exposure.get(group, 0) + value
        total += value

    result = []
    for sector, value in sector_exposure.items():
        result.append({
            "sector": sector,
            "value": round(value, 2),
            "pct": round(value / total * 100, 2) if total > 0 else 0,
        })
    result.sort(key=lambda x: x["value"], reverse=True)
    return result
