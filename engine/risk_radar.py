"""Risk Radar — spider chart with 6 risk dimensions scored 0-100."""
from __future__ import annotations
import sqlite3
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def get_risk_radar(player_id: str, prices: dict) -> dict:
    """Compute 6 risk dimensions for a player, each scored 0-100 (100 = max risk).

    Returns {player_id, dimensions: {concentration, sector_exposure, correlation,
             drawdown_proximity, vix_level, cash_reserves}, overall_risk, zone}.
    """
    from engine.paper_trader import get_portfolio

    portfolio = get_portfolio(player_id)
    positions = portfolio["positions"]
    cash = portfolio["cash"]

    total_value = cash + sum(
        p["qty"] * prices.get(p["symbol"], {}).get("price", p["avg_price"])
        for p in positions
    )
    if total_value <= 0:
        return _empty_radar(player_id)

    # 1. Concentration: how much of portfolio is in the largest position
    position_values = []
    for p in positions:
        price = prices.get(p["symbol"], {}).get("price", p["avg_price"])
        position_values.append(p["qty"] * price)

    if position_values:
        max_pct = max(position_values) / total_value
        concentration = min(100, int(max_pct * 100 / 0.25 * 100))  # 25% = 100 risk
    else:
        concentration = 0

    # 2. Sector exposure: how concentrated in one sector
    sector_exposure = _calc_sector_exposure(positions)

    # 3. Correlation: average pairwise correlation of held symbols
    correlation = _calc_correlation_risk(positions)

    # 4. Drawdown proximity: how close to max drawdown limit (20%)
    drawdown_proximity = _calc_drawdown_proximity(player_id)

    # 5. VIX level: current VIX mapped to risk
    vix_level = _calc_vix_risk()

    # 6. Cash reserves: lower cash = higher risk
    cash_pct = cash / total_value
    cash_reserves = max(0, min(100, int((1 - cash_pct / 0.30) * 100)))  # 30% cash = 0 risk

    dimensions = {
        "concentration": concentration,
        "sector_exposure": sector_exposure,
        "correlation": correlation,
        "drawdown_proximity": drawdown_proximity,
        "vix_level": vix_level,
        "cash_reserves": cash_reserves,
    }

    overall = sum(dimensions.values()) / len(dimensions)
    zone = "green" if overall < 40 else "yellow" if overall < 65 else "red"

    return {
        "player_id": player_id,
        "dimensions": dimensions,
        "overall_risk": round(overall, 1),
        "zone": zone,
    }


def _empty_radar(player_id: str) -> dict:
    return {
        "player_id": player_id,
        "dimensions": {
            "concentration": 0, "sector_exposure": 0, "correlation": 0,
            "drawdown_proximity": 0, "vix_level": 0, "cash_reserves": 0,
        },
        "overall_risk": 0, "zone": "green",
    }


def _calc_sector_exposure(positions: list) -> int:
    """Score sector concentration 0-100."""
    from engine.sector_tracker import SECTOR_MAP
    sectors: dict[str, int] = {}
    for p in positions:
        sector = SECTOR_MAP.get(p["symbol"], "Other")
        sectors[sector] = sectors.get(sector, 0) + 1

    if not sectors:
        return 0
    total = sum(sectors.values())
    max_sector_pct = max(sectors.values()) / total
    return min(100, int(max_sector_pct * 100 / 0.60 * 100))  # 60% in one sector = 100


def _calc_correlation_risk(positions: list) -> int:
    """Score portfolio correlation risk 0-100."""
    symbols = [p["symbol"] for p in positions]
    if len(symbols) < 2:
        return 0
    try:
        from engine.correlation import get_correlation_matrix
        matrix = get_correlation_matrix(symbols)
        if not matrix:
            return 30  # Unknown = moderate risk

        # Average off-diagonal correlation
        total_corr = 0
        count = 0
        for i, s1 in enumerate(symbols):
            for j, s2 in enumerate(symbols):
                if i < j and s1 in matrix and s2 in matrix.get(s1, {}):
                    total_corr += abs(matrix[s1][s2])
                    count += 1

        if count == 0:
            return 30
        avg_corr = total_corr / count
        return min(100, int(avg_corr * 100))  # 1.0 correlation = 100 risk
    except Exception:
        return 30


def _calc_drawdown_proximity(player_id: str) -> int:
    """Score how close to max drawdown limit (20%)."""
    try:
        from engine.risk_manager import RiskManager
        rm = RiskManager()
        _, drawdown = rm.check_drawdown(player_id)
        # 20% drawdown = 100 risk
        return min(100, int(drawdown / 0.20 * 100))
    except Exception:
        return 0


def _calc_vix_risk() -> int:
    """Map VIX level to risk score 0-100."""
    try:
        from engine.vix_monitor import get_vix_status
        vix = get_vix_status()
        if not vix or not vix.get("price"):
            return 30
        price = vix["price"]
        # VIX 12 = 0 risk, VIX 35+ = 100 risk
        return max(0, min(100, int((price - 12) / 23 * 100)))
    except Exception:
        return 30


def get_all_risk_radars(prices: dict) -> dict:
    """Get risk radar for all active players."""
    conn = _conn()
    players = conn.execute(
        "SELECT id FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()
    conn.close()

    result = {}
    for p in players:
        result[p["id"]] = get_risk_radar(p["id"], prices)
    return result
