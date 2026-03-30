"""Sentiment analysis for news headlines — lightweight keyword-based scoring."""
from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta

DB = "data/trader.db"

# Sentiment keyword dictionaries
_BULLISH_KEYWORDS = [
    "beat", "beats", "surpass", "exceeds", "record", "upgrade", "upgrades",
    "bullish", "rally", "surge", "soar", "jump", "gain", "growth",
    "breakout", "outperform", "buy", "strong", "boost", "positive",
    "approval", "approved", "partnership", "contract", "deal", "launch",
    "innovation", "optimis", "dividend", "buyback", "repurchase",
    "insider buy", "insider buying", "insider purchase",
    "raised", "raises", "hike", "increase", "expand",
    "beat expectations", "above consensus", "upside",
]

_BEARISH_KEYWORDS = [
    "miss", "misses", "below", "disappoint", "downgrade", "downgrades",
    "bearish", "crash", "plunge", "drop", "decline", "fall", "loss",
    "sell", "weak", "negative", "warning", "risk", "concern",
    "lawsuit", "investigation", "probe", "sec", "fraud", "scandal",
    "layoff", "layoffs", "restructur", "bankruptcy", "default",
    "insider sell", "insider selling", "insider sale",
    "cut", "cuts", "slash", "reduce", "shrink",
    "miss expectations", "below consensus", "downside",
    "recall", "delay", "suspend",
]

_INSIDER_KEYWORDS = [
    "insider buy", "insider buying", "insider purchase", "insider acquired",
    "insider sell", "insider selling", "insider sale", "insider sold",
    "insider transaction", "insider trading", "10b5-1", "form 4",
    "director buy", "director sell", "ceo buy", "ceo sell",
    "officer buy", "officer sell", "executive buy", "executive sell",
]


def score_headline(headline: str) -> dict:
    """Score a single headline for sentiment.

    Returns {score: float (-1 to 1), label: str, is_insider: bool, insider_direction: str}.
    """
    h = headline.lower()
    bull_count = sum(1 for kw in _BULLISH_KEYWORDS if kw in h)
    bear_count = sum(1 for kw in _BEARISH_KEYWORDS if kw in h)

    total = bull_count + bear_count
    if total == 0:
        score = 0.0
        label = "neutral"
    else:
        score = (bull_count - bear_count) / total
        if score > 0.2:
            label = "bullish"
        elif score < -0.2:
            label = "bearish"
        else:
            label = "neutral"

    # Check insider activity
    is_insider = any(kw in h for kw in _INSIDER_KEYWORDS)
    insider_direction = ""
    if is_insider:
        if any(kw in h for kw in ["insider buy", "insider buying", "insider purchase", "insider acquired",
                                    "director buy", "ceo buy", "officer buy", "executive buy"]):
            insider_direction = "buy"
        elif any(kw in h for kw in ["insider sell", "insider selling", "insider sale", "insider sold",
                                     "director sell", "ceo sell", "officer sell", "executive sell"]):
            insider_direction = "sell"

    return {
        "score": round(score, 2),
        "label": label,
        "is_insider": is_insider,
        "insider_direction": insider_direction,
    }


def get_sentiment_for_symbol(symbol: str, hours: int = 24) -> dict:
    """Get aggregated sentiment for a symbol from recent news.

    Returns {symbol, avg_score, label, headline_count, bullish, bearish, neutral,
             insider_alerts: [{headline, direction}]}.
    """
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT headline, summary FROM market_news WHERE symbol=? "
        "AND fetched_at >= datetime('now', ?) ORDER BY fetched_at DESC",
        (symbol, f"-{hours} hours")
    ).fetchall()
    conn.close()

    if not rows:
        return {
            "symbol": symbol, "avg_score": 0, "label": "neutral",
            "headline_count": 0, "bullish": 0, "bearish": 0, "neutral": 0,
            "insider_alerts": [],
        }

    scores = []
    counts = {"bullish": 0, "bearish": 0, "neutral": 0}
    insider_alerts = []

    for row in rows:
        headline = row["headline"] or ""
        result = score_headline(headline)
        scores.append(result["score"])
        counts[result["label"]] += 1

        if result["is_insider"]:
            insider_alerts.append({
                "headline": headline,
                "direction": result["insider_direction"],
            })

    avg_score = sum(scores) / len(scores) if scores else 0
    if avg_score > 0.15:
        label = "bullish"
    elif avg_score < -0.15:
        label = "bearish"
    else:
        label = "neutral"

    return {
        "symbol": symbol,
        "avg_score": round(avg_score, 3),
        "label": label,
        "headline_count": len(rows),
        "bullish": counts["bullish"],
        "bearish": counts["bearish"],
        "neutral": counts["neutral"],
        "insider_alerts": insider_alerts,
    }


def get_watchlist_sentiment(symbols: list) -> list:
    """Get sentiment for all watchlist symbols."""
    return [get_sentiment_for_symbol(sym) for sym in symbols]


def build_sentiment_prompt_section(symbol: str) -> str:
    """Build a text block for injection into the AI trading prompt."""
    sent = get_sentiment_for_symbol(symbol, hours=48)
    if sent["headline_count"] == 0:
        return ""

    lines = [f"=== SENTIMENT ANALYSIS for {symbol} ==="]
    lines.append(f"Overall: {sent['label'].upper()} (score: {sent['avg_score']:+.2f} from {sent['headline_count']} headlines)")
    lines.append(f"Breakdown: {sent['bullish']} bullish, {sent['bearish']} bearish, {sent['neutral']} neutral")

    if sent["insider_alerts"]:
        lines.append("\n*** INSIDER ACTIVITY DETECTED ***")
        for alert in sent["insider_alerts"]:
            direction = alert["direction"].upper() if alert["direction"] else "UNKNOWN"
            lines.append(f"  !!! INSIDER {direction}: {alert['headline']}")
        lines.append("Insider buying is a strong bullish signal. Insider selling may indicate caution.")

    return "\n".join(lines)
