"""Premium ETF Arsenal — 50+ ETFs across 6 categories, crew-managed.

Each category is assigned to a crew member based on their specialty.
Regime-based recommendations guide which categories to focus on.
"""
from __future__ import annotations
import time
import threading
from rich.console import Console

console = Console()

_cache = {"data": None, "ts": 0}
_lock = threading.Lock()
_TTL = 600  # 10 minutes

PREMIUM_ETFS = {
    "OFFENSE": {
        "owner": "Chekov", "emoji": "🧭", "label": "Offensive Growth",
        "reason": "Navigator finds the fastest routes",
        "etfs": {
            "QQQ": "Invesco QQQ Nasdaq 100",
            "SMH": "VanEck Semiconductor",
            "ARKK": "ARK Innovation",
            "ARKQ": "ARK Autonomous Tech",
            "VUG": "Vanguard Growth",
            "MGK": "Vanguard Mega Cap Growth",
            "MTUM": "iShares Momentum Factor",
            "IWM": "iShares Russell 2000",
        },
    },
    "DEFENSE": {
        "owner": "Worf", "emoji": "⚔️", "label": "Defensive Shields",
        "reason": "Head of Security protects the ship",
        "etfs": {
            "GLD": "SPDR Gold Shares",
            "SLV": "iShares Silver Trust",
            "PPLT": "Abrdn Physical Platinum",
            "TLT": "iShares 20+ Year Treasury",
            "SCHD": "Schwab US Dividend Equity",
            "VOOV": "Vanguard S&P 500 Value",
            "XLU": "Utilities Select Sector",
            "XLP": "Consumer Staples Select",
            "GDXJ": "VanEck Junior Gold Miners",
        },
    },
    "INCOME": {
        "owner": "Sosnoff", "emoji": "🎯", "label": "Income Generation",
        "reason": "Weapons Officer — premium selling specialist",
        "etfs": {
            "JEPI": "JPMorgan Equity Premium Income",
            "JEPQ": "JPMorgan Nasdaq Equity Premium",
            "XYLD": "Global X S&P 500 Covered Call",
            "QYLD": "Global X Nasdaq 100 Covered Call",
            "DVY": "iShares Select Dividend",
            "HYG": "iShares High Yield Corporate Bond",
        },
    },
    "SECTOR": {
        "owner": "Chekov", "emoji": "🧭", "label": "Sector Rotation",
        "reason": "Navigator scans all sectors",
        "etfs": {
            "XLK": "Technology Select Sector",
            "XLV": "Health Care Select Sector",
            "XLF": "Financial Select Sector",
            "XLE": "Energy Select Sector",
            "XLI": "Industrial Select Sector",
            "XLB": "Materials Select Sector",
            "XLRE": "Real Estate Select Sector",
            "XLC": "Communication Select Sector",
            "KBE": "SPDR S&P Bank ETF",
        },
    },
    "COMMODITY": {
        "owner": "Dalio", "emoji": "🪙", "label": "Commodities",
        "reason": "Quartermaster manages physical reserves",
        "etfs": {
            "GLD": "SPDR Gold Shares",
            "SLV": "iShares Silver Trust",
            "PPLT": "Abrdn Physical Platinum",
            "USO": "United States Oil Fund",
            "DBA": "Invesco DB Agriculture",
            "GDXJ": "VanEck Junior Gold Miners",
        },
    },
    "INVERSE": {
        "owner": "Worf", "emoji": "⚔️", "label": "Inverse / Short",
        "reason": "Defensive arsenal is security domain",
        "etfs": {
            "SH": "ProShares Short S&P500",
            "SDS": "ProShares UltraShort S&P500",
            "SQQQ": "ProShares UltraPro Short QQQ",
            "SDOW": "ProShares UltraPro Short Dow30",
            "TZA": "Direxion Small Cap Bear 3x",
        },
    },
}

REGIME_RECOMMENDATIONS = {
    "BULL": {
        "primary": "OFFENSE", "secondary": "INCOME", "avoid": "INVERSE",
        "message": "🟢 BULL — Offense at full power. QQQ, SMH, ARKK. Add income (JEPI, SCHD). Avoid inverse.",
    },
    "CAUTIOUS": {
        "primary": "INCOME", "secondary": "DEFENSE", "avoid": None,
        "message": "🟡 CAUTIOUS — Shift to income and dividends. Keep some growth but tighten stops.",
    },
    "BEAR": {
        "primary": "DEFENSE", "secondary": "INVERSE", "avoid": "OFFENSE",
        "message": "🔴 BEAR — Defense first. GLD, SLV, TLT, SCHD. Consider SH at 5-10%. Avoid growth.",
    },
    "BEAR_TREND": {
        "primary": "DEFENSE", "secondary": "INVERSE", "avoid": "OFFENSE",
        "message": "🔴 BEAR TREND — Defense first. GLD, SLV, TLT. Consider SH at 5-10%. Avoid growth.",
    },
    "CRISIS": {
        "primary": "INVERSE", "secondary": "COMMODITY", "avoid": "OFFENSE",
        "message": "⚫ CRISIS — Maximum defense. SH/SQQQ at 10-15%. Gold, platinum. Cash is king.",
    },
}


def get_all_etf_data() -> dict:
    """Get live prices for all premium ETFs with 1d and 30d changes."""
    with _lock:
        if _cache["data"] and time.time() - _cache["ts"] < _TTL:
            return _cache["data"]

    import yfinance as yf

    # Collect all unique tickers
    all_tickers = set()
    for cat in PREMIUM_ETFS.values():
        all_tickers.update(cat["etfs"].keys())
    all_tickers.add("SPY")  # benchmark

    try:
        data = yf.download(list(all_tickers), period="30d", progress=False, group_by="ticker")
    except Exception as e:
        console.log(f"[red]Premium ETF download error: {e}")
        return {"error": str(e)}

    # Build per-ticker price data
    ticker_data = {}
    for ticker in all_tickers:
        try:
            d = data[ticker] if ticker in data.columns.get_level_values(0) else None
            if d is None or d.empty or len(d) < 2:
                continue
            current = float(d["Close"].iloc[-1])
            prev = float(d["Close"].iloc[-2])
            month_ago = float(d["Close"].iloc[0])
            ticker_data[ticker] = {
                "price": round(current, 2),
                "change_1d": round(((current - prev) / prev) * 100, 2),
                "change_30d": round(((current - month_ago) / month_ago) * 100, 2),
            }
        except Exception:
            continue

    # Build category results
    categories = {}
    for cat_key, cat_info in PREMIUM_ETFS.items():
        etfs = []
        for ticker, name in cat_info["etfs"].items():
            td = ticker_data.get(ticker)
            if td:
                etfs.append({
                    "ticker": ticker,
                    "name": name,
                    "price": td["price"],
                    "change_1d": td["change_1d"],
                    "change_30d": td["change_30d"],
                })
        # Sort by 30d performance
        etfs.sort(key=lambda x: x["change_30d"], reverse=True)
        categories[cat_key] = {
            "owner": cat_info["owner"],
            "emoji": cat_info["emoji"],
            "label": cat_info["label"],
            "reason": cat_info["reason"],
            "etfs": etfs,
        }

    # Get regime recommendation
    regime = "CAUTIOUS"
    try:
        from engine.regime_detector import detect_regime
        r = detect_regime()
        regime = r.get("regime", "CAUTIOUS")
    except Exception:
        pass

    rec = REGIME_RECOMMENDATIONS.get(regime, REGIME_RECOMMENDATIONS["CAUTIOUS"])

    result = {
        "categories": categories,
        "regime": regime,
        "recommendation": rec,
        "spy": ticker_data.get("SPY", {}),
    }

    with _lock:
        _cache["data"] = result
        _cache["ts"] = time.time()

    return result


def get_category_data(category: str) -> dict:
    """Get ETF data for a specific category."""
    all_data = get_all_etf_data()
    if "error" in all_data:
        return all_data
    cat = all_data.get("categories", {}).get(category.upper())
    if not cat:
        return {"error": f"Unknown category: {category}"}
    return {
        "category": category.upper(),
        **cat,
        "regime": all_data["regime"],
        "recommendation": all_data["recommendation"],
    }
