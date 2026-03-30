"""Worf's Defensive Arsenal — Inverse ETF tracking and recommendations.

Inverse ETFs are defensive weapons for BEAR/CRISIS regimes ONLY.
Worf monitors decay risk and urgently warns to exit in bull markets.
"""
from __future__ import annotations
import time
import threading
from rich.console import Console

console = Console()

_cache = {"data": None, "ts": 0}
_lock = threading.Lock()
_TTL = 300  # 5 minutes

INVERSE_ETFS = {
    "SH": {"name": "ProShares Short S&P500", "leverage": -1, "index": "SPY", "decay_risk": "LOW"},
    "SDS": {"name": "ProShares UltraShort S&P500", "leverage": -2, "index": "SPY", "decay_risk": "MEDIUM"},
    "SPXU": {"name": "ProShares UltraPro Short S&P500", "leverage": -3, "index": "SPY", "decay_risk": "HIGH"},
    "SDOW": {"name": "ProShares UltraPro Short Dow30", "leverage": -3, "index": "DIA", "decay_risk": "HIGH"},
    "SQQQ": {"name": "ProShares UltraPro Short QQQ", "leverage": -3, "index": "QQQ", "decay_risk": "HIGH"},
    "TZA": {"name": "Direxion Small Cap Bear 3x", "leverage": -3, "index": "IWM", "decay_risk": "HIGH"},
    "VXX": {"name": "iPath S&P 500 VIX Short-Term", "leverage": 1, "index": "VIX", "decay_risk": "EXTREME"},
}


def get_inverse_etf_data() -> list:
    """Get current prices and daily changes for all inverse ETFs."""
    with _lock:
        if _cache["data"] and time.time() - _cache["ts"] < _TTL:
            return _cache["data"]

    import yfinance as yf

    tickers = list(INVERSE_ETFS.keys())
    results = []

    try:
        data = yf.download(tickers, period="5d", progress=False, group_by="ticker")

        for ticker, info in INVERSE_ETFS.items():
            try:
                d = data[ticker] if ticker in data.columns.get_level_values(0) else None
                if d is None or d.empty:
                    continue
                current = float(d["Close"].iloc[-1])
                prev = float(d["Close"].iloc[-2])
                five_day = float(d["Close"].iloc[0])
                change_1d = round(((current - prev) / prev) * 100, 2)
                change_5d = round(((current - five_day) / five_day) * 100, 2)

                results.append({
                    "ticker": ticker,
                    "name": info["name"],
                    "leverage": info["leverage"],
                    "index": info["index"],
                    "decay_risk": info["decay_risk"],
                    "price": round(current, 2),
                    "change_1d": change_1d,
                    "change_5d": change_5d,
                })
            except Exception:
                continue
    except Exception as e:
        console.log(f"[red]Inverse ETF data error: {e}")

    with _lock:
        _cache["data"] = results
        _cache["ts"] = time.time()

    return results


def should_recommend_inverse(regime: str, vix: float,
                             spy_vs_200ma: float, spy_vs_50ma: float) -> dict | None:
    """Worf recommends inverse ETFs ONLY in BEAR/CRISIS regimes."""
    if regime not in ("BEAR", "BEAR_TREND", "CRISIS"):
        return None

    if vix < 25:
        return None

    # Both conditions: VIX elevated AND SPY below key MAs
    if not (spy_vs_200ma < 0):
        return None

    if regime == "CRISIS":
        return {
            "action": "DEPLOY",
            "primary": "SH",
            "tactical": "SQQQ" if vix > 35 else "SDS",
            "allocation": "10-15% of portfolio",
            "message": (
                "⚔️ LT. CMDR. WORF: CRISIS detected. Recommend deploying SH at 10% "
                "allocation for strategic cover. If tech is leading the decline, SQQQ "
                "provides targeted firepower. This is not a drill, Captain."
            ),
            "stop_condition": "EXIT immediately when VIX drops below 25 or SPY reclaims 200MA",
        }
    else:  # BEAR / BEAR_TREND
        return {
            "action": "CONSIDER",
            "primary": "SH",
            "tactical": None,
            "allocation": "5-10% of portfolio",
            "message": (
                "⚔️ LT. CMDR. WORF: Defensive perimeters suggest inverse ETF deployment. "
                "SH provides tactical cover without the decay risk of leveraged variants. "
                "I recommend SH over SDOW — a warrior fights smart, not reckless."
            ),
            "stop_condition": "EXIT when regime shifts to CAUTIOUS or VIX drops below 22",
        }


def backtest_inverse_etfs(days: int = 180, start_capital: float = 7000) -> dict:
    """Backtest Worf's inverse ETF strategy across ETFs and allocation sizes.

    Entry: VIX > 25 AND SPY < 200MA AND SPY < 50MA
    Exit:  VIX < 22 OR SPY reclaims 200MA
    """
    import yfinance as yf
    import numpy as np
    from datetime import datetime, timedelta

    end_date = datetime.now()
    # Need extra 200 days for SMA warmup
    start_date = end_date - timedelta(days=days + 210)

    tickers = ["SPY", "^VIX", "SH", "SDS", "SQQQ", "SDOW", "TZA"]
    try:
        data = yf.download(tickers, start=start_date, end=end_date,
                           progress=False, group_by="ticker")
    except Exception as e:
        return {"error": str(e)}

    try:
        spy_close = data["SPY"]["Close"].dropna()
        vix_close = data["^VIX"]["Close"].dropna()
    except Exception:
        return {"error": "Failed to get SPY/VIX data"}

    spy_sma200 = spy_close.rolling(200).mean()
    spy_sma50 = spy_close.rolling(50).mean()

    # Only use dates after SMA warmup
    valid_start = spy_sma200.dropna().index[0]
    results = {}

    for etf in ["SH", "SDS", "SQQQ", "SDOW", "TZA"]:
        try:
            etf_close = data[etf]["Close"].dropna()
        except Exception:
            continue

        for alloc_pct in [5, 10, 15, 20]:
            portfolio = start_capital
            cash_portion = start_capital
            etf_shares = 0.0
            etf_cost = 0.0
            in_position = False
            trades = []

            common_dates = spy_close.index.intersection(
                etf_close.index
            ).intersection(vix_close.index)
            common_dates = common_dates[common_dates >= valid_start]

            for dt in common_dates:
                spy_price = float(spy_close.loc[dt])
                vix_val = float(vix_close.loc[dt])
                sma200 = float(spy_sma200.loc[dt]) if dt in spy_sma200.index and not np.isnan(spy_sma200.loc[dt]) else spy_price
                sma50 = float(spy_sma50.loc[dt]) if dt in spy_sma50.index and not np.isnan(spy_sma50.loc[dt]) else spy_price
                etf_price = float(etf_close.loc[dt])

                # ENTRY
                if not in_position and vix_val > 25 and spy_price < sma200 and spy_price < sma50:
                    deploy = portfolio * (alloc_pct / 100)
                    etf_shares = deploy / etf_price
                    etf_cost = etf_price
                    cash_portion = portfolio - deploy
                    in_position = True
                    trades.append({
                        "type": "BUY", "date": str(dt.date()),
                        "price": round(etf_price, 2), "vix": round(vix_val, 1),
                    })

                # EXIT
                elif in_position and (vix_val < 22 or spy_price > sma200):
                    pnl = (etf_price - etf_cost) * etf_shares
                    portfolio = cash_portion + etf_shares * etf_price
                    in_position = False
                    trades.append({
                        "type": "SELL", "date": str(dt.date()),
                        "price": round(etf_price, 2), "pnl": round(pnl, 2),
                        "vix": round(vix_val, 1),
                    })
                    etf_shares = 0.0

                if in_position:
                    portfolio = cash_portion + etf_shares * etf_price

            # Close open position at end
            if in_position and len(common_dates) > 0:
                last_dt = common_dates[-1]
                final_price = float(etf_close.loc[last_dt])
                pnl = (final_price - etf_cost) * etf_shares
                portfolio = cash_portion + etf_shares * final_price
                trades.append({
                    "type": "SELL_END", "date": str(last_dt.date()),
                    "price": round(final_price, 2), "pnl": round(pnl, 2),
                })

            # SPY return over same period
            spy_start_price = float(spy_close.loc[common_dates[0]]) if len(common_dates) > 0 else 1
            spy_end_price = float(spy_close.loc[common_dates[-1]]) if len(common_dates) > 0 else 1
            spy_return = ((spy_end_price - spy_start_price) / spy_start_price) * 100
            total_return = ((portfolio - start_capital) / start_capital) * 100

            key = f"{etf}_{alloc_pct}pct"
            wins = sum(1 for t in trades if t.get("pnl", 0) > 0)
            losses = sum(1 for t in trades if t.get("pnl", 0) < 0)
            results[key] = {
                "etf": etf,
                "allocation_pct": alloc_pct,
                "final_value": round(portfolio, 2),
                "total_return": round(total_return, 2),
                "spy_return": round(spy_return, 2),
                "alpha": round(total_return - spy_return, 2),
                "total_trades": len([t for t in trades if t["type"] == "BUY"]),
                "trades": trades,
                "wins": wins,
                "losses": losses,
            }

    return results


def check_inverse_exit_warning(regime: str, holdings: list) -> dict | None:
    """If regime shifts bullish and user holds inverse ETFs, Worf warns urgently."""
    if regime in ("BULL", "CAUTIOUS"):
        inverse_held = [h for h in holdings if h in INVERSE_ETFS]
        if inverse_held:
            return {
                "urgent": True,
                "message": (
                    f"⚔️ LT. CMDR. WORF: URGENT — Regime has shifted to {regime}. "
                    f"You are holding inverse positions: {', '.join(inverse_held)}. "
                    f"These DECAY in rising markets. Exit ALL inverse positions IMMEDIATELY. "
                    f"Every day you hold in a bull market, the Borg take a piece."
                ),
                "tickers": inverse_held,
            }
    return None
