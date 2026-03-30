"""Insider Tracker — monitor insider buying/selling across the watchlist."""
from __future__ import annotations
import json
from datetime import datetime, timedelta
from pathlib import Path
from rich.console import Console

import config

console = Console()
DATA_FILE = Path("data/insider_trades.json")


def get_insider_trades(symbol: str) -> list:
    """Get insider transactions and purchases for a symbol using yfinance.

    Returns list of dicts with: insider_name, relation, transaction_type,
    shares, value, date, ownership_type.
    """
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        trades = []

        # Insider transactions (buys + sells)
        try:
            txns = ticker.insider_transactions
            if txns is not None and not txns.empty:
                for _, row in txns.iterrows():
                    trade = {
                        "symbol": symbol,
                        "insider_name": str(row.get("Insider", row.get("insider", "Unknown"))),
                        "relation": str(row.get("Relationship", row.get("relation", ""))),
                        "transaction_type": str(row.get("Transaction", row.get("transaction", ""))),
                        "shares": _safe_int(row.get("Shares", row.get("shares", 0))),
                        "value": _safe_float(row.get("Value", row.get("value", 0))),
                        "date": str(row.get("Date", row.get("Start Date", "")))[:10],
                        "ownership_type": str(row.get("Ownership", row.get("ownership", ""))),
                        "source": "insider_transactions",
                    }
                    trades.append(trade)
        except Exception as e:
            console.log(f"[red]Insider transactions error for {symbol}: {e}")

        # Insider purchases (aggregated buy data)
        try:
            purchases = ticker.insider_purchases
            if purchases is not None and not purchases.empty:
                for _, row in purchases.iterrows():
                    trade = {
                        "symbol": symbol,
                        "insider_name": str(row.get("Insider", row.get("insider", "Aggregated"))),
                        "relation": str(row.get("Relationship", row.get("relation", ""))),
                        "transaction_type": "Purchase",
                        "shares": _safe_int(row.get("Shares", row.get("shares", 0))),
                        "value": _safe_float(row.get("Value", row.get("value", 0))),
                        "date": str(row.get("Date", row.get("Start Date", "")))[:10],
                        "ownership_type": "",
                        "source": "insider_purchases",
                    }
                    trades.append(trade)
        except Exception as e:
            console.log(f"[red]Insider purchases error for {symbol}: {e}")

        # Deduplicate by (insider_name, date, shares)
        seen = set()
        unique_trades = []
        for t in trades:
            key = (t["insider_name"], t["date"], t["shares"])
            if key not in seen:
                seen.add(key)
                unique_trades.append(t)

        # Sort by date descending
        unique_trades.sort(key=lambda x: x["date"], reverse=True)
        return unique_trades

    except Exception as e:
        console.log(f"[red]Insider data error for {symbol}: {e}")
        return []


def scan_insider_alerts() -> list:
    """Scan all WATCH_STOCKS for significant insider buying in the last 30 days.

    Flags purchases > $500,000 as notable alerts.

    Returns list of alert dicts with: symbol, insider_name, value, shares,
    date, alert_type, significance.
    """
    cutoff = datetime.now() - timedelta(days=30)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    alerts = []

    for symbol in config.WATCH_STOCKS:
        try:
            trades = get_insider_trades(symbol)
            for trade in trades:
                # Filter to recent purchases only
                txn_type = trade.get("transaction_type", "").lower()
                is_buy = any(kw in txn_type for kw in ["purchase", "buy", "acquisition"])
                if not is_buy:
                    continue

                # Check date is within 30 days
                trade_date = trade.get("date", "")
                if trade_date and trade_date < cutoff_str:
                    continue

                value = trade.get("value", 0) or 0

                # Flag significant purchases (> $500k)
                if value >= 500_000:
                    alert = {
                        "symbol": symbol,
                        "insider_name": trade["insider_name"],
                        "relation": trade.get("relation", ""),
                        "value": value,
                        "shares": trade.get("shares", 0),
                        "date": trade_date,
                        "alert_type": "large_insider_buy",
                        "significance": _classify_significance(value),
                        "scanned_at": datetime.now().isoformat(),
                    }
                    alerts.append(alert)

        except Exception as e:
            console.log(f"[red]Insider alert scan error for {symbol}: {e}")

    # Sort by value descending
    alerts.sort(key=lambda x: x.get("value", 0), reverse=True)

    # Save alerts
    _save_alerts(alerts)
    return alerts


def get_insider_summary(symbol: str) -> dict:
    """Get a summary of insider activity for a symbol.

    Returns dict with: total_buys, total_sells, net_value, recent_trades,
    notable_buyers, sentiment.
    """
    trades = get_insider_trades(symbol)
    if not trades:
        return {
            "symbol": symbol,
            "total_buys": 0,
            "total_sells": 0,
            "net_value": 0,
            "recent_trades": [],
            "notable_buyers": [],
            "sentiment": "neutral",
        }

    total_buy_value = 0
    total_sell_value = 0
    buy_count = 0
    sell_count = 0
    notable_buyers = []

    for trade in trades:
        txn_type = trade.get("transaction_type", "").lower()
        value = trade.get("value", 0) or 0
        is_buy = any(kw in txn_type for kw in ["purchase", "buy", "acquisition"])
        is_sell = any(kw in txn_type for kw in ["sale", "sell", "disposition"])

        if is_buy:
            buy_count += 1
            total_buy_value += value
            if value >= 100_000:
                notable_buyers.append({
                    "name": trade["insider_name"],
                    "value": value,
                    "date": trade["date"],
                })
        elif is_sell:
            sell_count += 1
            total_sell_value += value

    net_value = total_buy_value - total_sell_value

    # Determine sentiment
    if buy_count > sell_count * 2 and net_value > 0:
        sentiment = "strongly_bullish"
    elif buy_count > sell_count and net_value > 0:
        sentiment = "bullish"
    elif sell_count > buy_count * 2 and net_value < 0:
        sentiment = "strongly_bearish"
    elif sell_count > buy_count and net_value < 0:
        sentiment = "bearish"
    else:
        sentiment = "neutral"

    return {
        "symbol": symbol,
        "total_buys": buy_count,
        "total_sells": sell_count,
        "total_buy_value": total_buy_value,
        "total_sell_value": total_sell_value,
        "net_value": net_value,
        "recent_trades": trades[:10],
        "notable_buyers": notable_buyers,
        "sentiment": sentiment,
    }


# ── Helpers ──────────────────────────────────────────────────────────

def _classify_significance(value: float) -> str:
    """Classify the significance of an insider purchase by dollar value."""
    if value >= 5_000_000:
        return "exceptional"
    elif value >= 1_000_000:
        return "very_high"
    elif value >= 500_000:
        return "high"
    elif value >= 100_000:
        return "moderate"
    return "low"


def _safe_float(val) -> float:
    """Safely convert to float."""
    try:
        if val is None:
            return 0.0
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _safe_int(val) -> int:
    """Safely convert to int."""
    try:
        if val is None:
            return 0
        return int(float(val))
    except (ValueError, TypeError):
        return 0


# ── Persistence ──────────────────────────────────────────────────────

def _save_alerts(alerts: list):
    """Save insider alerts to disk."""
    try:
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Merge with existing alerts, keep last 200
        existing = _load_alerts()
        combined = alerts + existing
        # Deduplicate by (symbol, insider_name, date)
        seen = set()
        unique = []
        for a in combined:
            key = (a.get("symbol"), a.get("insider_name"), a.get("date"))
            if key not in seen:
                seen.add(key)
                unique.append(a)
        unique = unique[:200]
        DATA_FILE.write_text(json.dumps(unique, indent=2, default=str))
    except Exception as e:
        console.log(f"[red]Error saving insider alerts: {e}")


def _load_alerts() -> list:
    """Load insider alerts from disk."""
    try:
        if DATA_FILE.exists():
            return json.loads(DATA_FILE.read_text())
    except Exception as e:
        console.log(f"[red]Error loading insider alerts: {e}")
    return []
