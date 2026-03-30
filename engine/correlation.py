"""Correlation matrix and exposure groups for active symbols."""
from __future__ import annotations
import sqlite3
import threading
import time
from datetime import datetime
import numpy as np
import pandas as pd
from engine.market_data import _yahoo_chart

DB = "data/trader.db"
_CACHE_TTL = 900  # 15 minutes
_cache: dict = {}
_cache_lock = threading.Lock()


def _cache_key(symbols: list[str], period: int, threshold: float) -> str:
    return f"{period}:{threshold:.2f}:{','.join(sorted(set(symbols)))}"


def _build_groups(corr: pd.DataFrame, threshold: float) -> list[dict]:
    symbols = list(corr.columns)
    parent = {sym: sym for sym in symbols}

    def find(sym: str) -> str:
        while parent[sym] != sym:
            parent[sym] = parent[parent[sym]]
            sym = parent[sym]
        return sym

    def union(a: str, b: str):
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    pairs = []
    for i, left in enumerate(symbols):
        for j in range(i + 1, len(symbols)):
            right = symbols[j]
            value = float(corr.iloc[i, j])
            if value > threshold:
                union(left, right)
                pairs.append({"left": left, "right": right, "correlation": round(value, 3)})

    grouped: dict[str, dict] = {}
    for sym in symbols:
        root = find(sym)
        grouped.setdefault(root, {"symbols": [], "max_pair_correlation": 0.0})
        grouped[root]["symbols"].append(sym)

    for pair in pairs:
        root = find(pair["left"])
        grouped[root]["max_pair_correlation"] = max(
            grouped[root]["max_pair_correlation"],
            pair["correlation"],
        )

    result = []
    for group in grouped.values():
        if len(group["symbols"]) < 2:
            continue
        result.append({
            "symbols": sorted(group["symbols"]),
            "max_pair_correlation": round(group["max_pair_correlation"], 3),
        })
    result.sort(key=lambda g: (-len(g["symbols"]), -g["max_pair_correlation"], g["symbols"]))
    return result


def _build_group_exposure(positions: list[dict], groups: list[dict], total_value: float) -> list[dict]:
    if not groups or total_value <= 0:
        return []

    by_symbol = {}
    for pos in positions:
        symbol = (pos.get("symbol") or "").upper()
        if not symbol:
            continue
        current = pos.get("current_price") or pos.get("avg_price") or 0
        value = max(0.0, float(pos.get("qty", 0)) * float(current))
        by_symbol[symbol] = by_symbol.get(symbol, 0.0) + value

    exposures = []
    for idx, group in enumerate(groups, start=1):
        value = round(sum(by_symbol.get(sym, 0.0) for sym in group["symbols"]), 2)
        exposures.append({
            "group_id": idx,
            "symbols": group["symbols"],
            "value": value,
            "pct_of_portfolio": round((value / total_value) * 100, 2) if total_value > 0 else 0.0,
            "max_pair_correlation": group["max_pair_correlation"],
        })
    exposures.sort(key=lambda item: item["value"], reverse=True)
    return exposures


def get_correlation_matrix(symbols: list, period: int = 60, threshold: float = 0.7) -> dict:
    """Calculate correlation matrix for given symbols using Yahoo direct daily close data.

    Returns matrix, pair warnings, and correlated groups.
    """
    deduped = [s.upper() for s in symbols if s]
    if len(deduped) < 2:
        return {
            "matrix": [],
            "symbols": deduped,
            "warnings": [],
            "groups": [],
            "pairs": [],
            "lookback_days": period,
            "threshold": threshold,
            "updated": datetime.now().isoformat(),
        }

    key = _cache_key(deduped, period, threshold)
    with _cache_lock:
        cached = _cache.get(key)
        if cached and (time.time() - cached["ts"]) < _CACHE_TTL:
            return cached["data"]

    try:
        # Fetch daily closes for each symbol via Yahoo direct
        closes_dict = {}
        for sym in sorted(set(deduped)):
            chart = _yahoo_chart(sym, interval="1d", range_=f"{period + 10}d")
            if not chart:
                continue
            timestamps = chart.get("timestamp", [])
            quotes = chart.get("indicators", {}).get("quote", [{}])[0]
            close_list = quotes.get("close", [])
            if not timestamps or not close_list:
                continue
            # Build date->close map
            date_prices = {}
            for i, ts in enumerate(timestamps):
                if i < len(close_list) and close_list[i] is not None:
                    date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    date_prices[date_str] = close_list[i]
            if date_prices:
                closes_dict[sym] = date_prices

        valid_symbols = [s for s in sorted(set(deduped)) if s in closes_dict]
        if len(valid_symbols) < 2:
            return {
                "matrix": [],
                "symbols": valid_symbols,
                "warnings": ["Not enough data"],
                "groups": [],
                "pairs": [],
                "lookback_days": period,
                "threshold": threshold,
                "updated": datetime.now().isoformat(),
            }

        # Build DataFrame from aligned dates
        df = pd.DataFrame(closes_dict)
        df = df.dropna(axis=0, how="any")

        if len(df) < 5:
            return {
                "matrix": [],
                "symbols": valid_symbols,
                "warnings": ["Insufficient trading days"],
                "groups": [],
                "pairs": [],
                "lookback_days": period,
                "threshold": threshold,
                "updated": datetime.now().isoformat(),
            }

        # Calculate returns and correlation
        returns = df.pct_change().dropna()
        corr = returns.corr()

        matrix = [[round(v, 3) for v in row] for row in corr.values.tolist()]

        # Find high-correlation warnings (> 0.7, excluding self-correlation)
        warnings = []
        pairs = []
        cols = list(corr.columns)
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                c = corr.iloc[i, j]
                if c > threshold:
                    pairs.append({
                        "left": cols[i],
                        "right": cols[j],
                        "correlation": round(float(c), 3),
                    })
                    warnings.append(f"{cols[i]}/{cols[j]}: {c:.2f} correlation")

        groups = _build_groups(corr, threshold)

        result = {
            "matrix": matrix,
            "symbols": cols,
            "warnings": warnings,
            "pairs": pairs,
            "groups": groups,
            "lookback_days": period,
            "threshold": threshold,
            "updated": datetime.now().isoformat(),
        }
        with _cache_lock:
            _cache[key] = {"data": result, "ts": time.time()}
        return result

    except Exception as e:
        return {
            "matrix": [],
            "symbols": deduped,
            "warnings": [str(e)],
            "groups": [],
            "pairs": [],
            "lookback_days": period,
            "threshold": threshold,
            "updated": datetime.now().isoformat(),
        }


def get_portfolio_correlation(player_id: str) -> dict:
    """Get correlation matrix for a specific player's current positions."""
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    positions = conn.execute(
        "SELECT symbol, qty, avg_price FROM positions WHERE player_id=? AND asset_type='stock'",
        (player_id,)
    ).fetchall()
    conn.close()

    symbols = [p["symbol"] for p in positions]
    if len(symbols) < 2:
        total_value = round(sum(float(p["qty"]) * float(p["avg_price"]) for p in positions), 2)
        return {
            "matrix": [],
            "symbols": symbols,
            "warnings": [],
            "concentrated": False,
            "groups": [],
            "group_exposure": [],
            "symbol_exposure": [],
            "lookback_days": 60,
            "threshold": 0.7,
            "updated": datetime.now().isoformat(),
        }

    result = get_correlation_matrix(symbols)
    total_value = sum(float(p["qty"]) * float(p["avg_price"]) for p in positions)
    result["symbol_exposure"] = [
        {
            "symbol": p["symbol"],
            "value": round(float(p["qty"]) * float(p["avg_price"]), 2),
            "pct_of_portfolio": round((float(p["qty"]) * float(p["avg_price"]) / total_value) * 100, 2) if total_value > 0 else 0.0,
        }
        for p in positions
    ]
    result["symbol_exposure"].sort(key=lambda item: item["value"], reverse=True)
    result["group_exposure"] = _build_group_exposure(
        [dict(p) for p in positions],
        result.get("groups", []),
        total_value,
    )
    result["concentrated"] = len(result["warnings"]) > 0
    return result


def get_position_correlation_profile(positions: list[dict], proposed_symbol: str | None = None,
                                     proposed_cost: float = 0.0, total_value: float = 0.0,
                                     threshold: float = 0.7) -> dict:
    """Return per-symbol and correlated-group exposure for an in-memory position list."""
    stock_positions = [
        p for p in positions
        if (p.get("asset_type") or "stock") == "stock" and p.get("symbol")
    ]
    symbols = [p["symbol"].upper() for p in stock_positions]
    result = get_correlation_matrix(symbols, period=60, threshold=threshold) if len(symbols) >= 2 else {
        "groups": [], "symbols": list(sorted(set(symbols))), "warnings": [], "pairs": [],
        "matrix": [], "lookback_days": 60, "threshold": threshold, "updated": datetime.now().isoformat(),
    }

    computed_total = total_value
    if computed_total <= 0:
        computed_total = sum(
            float(p.get("qty", 0)) * float(p.get("current_price") or p.get("avg_price") or 0)
            for p in stock_positions
        )

    symbol_values = {}
    for pos in stock_positions:
        sym = pos["symbol"].upper()
        current = pos.get("current_price") or pos.get("avg_price") or 0
        value = max(0.0, float(pos.get("qty", 0)) * float(current))
        symbol_values[sym] = symbol_values.get(sym, 0.0) + value
    if proposed_symbol and proposed_cost > 0:
        symbol_values[proposed_symbol.upper()] = symbol_values.get(proposed_symbol.upper(), 0.0) + proposed_cost
        computed_total = max(computed_total, 0.0) + proposed_cost

    group_exposure = []
    for idx, group in enumerate(result.get("groups", []), start=1):
        value = sum(symbol_values.get(sym, 0.0) for sym in group["symbols"])
        includes = proposed_symbol and proposed_symbol.upper() in group["symbols"]
        group_exposure.append({
            "group_id": idx,
            "symbols": group["symbols"],
            "value": round(value, 2),
            "pct_of_portfolio": round((value / computed_total) * 100, 2) if computed_total > 0 else 0.0,
            "includes_proposed": bool(includes),
            "max_pair_correlation": group["max_pair_correlation"],
        })

    return {
        "symbol_values": {k: round(v, 2) for k, v in symbol_values.items()},
        "group_exposure": sorted(group_exposure, key=lambda item: item["value"], reverse=True),
        "groups": result.get("groups", []),
        "threshold": threshold,
        "lookback_days": result.get("lookback_days", 60),
        "warnings": result.get("warnings", []),
        "updated": result.get("updated", datetime.now().isoformat()),
    }


def get_watchlist_correlation() -> dict:
    """Get correlation matrix for all watchlist stocks."""
    from config import WATCH_STOCKS
    return get_correlation_matrix(WATCH_STOCKS, period=60, threshold=0.7)
