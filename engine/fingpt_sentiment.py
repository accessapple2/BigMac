"""
USS TradeMinds — FinGPT-Inspired Sentiment Scorer (engine/fingpt_sentiment.py)

Classifies recent headlines from market_news table using mistral:7b via Ollama.
Aggregates per-symbol: BULLISH/BEARISH/NEUTRAL with strength 1-10.
15-minute cache per symbol. Returns None gracefully if Ollama is busy.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time

from config import OLLIE_URL as _OLLIE_URL

logger = logging.getLogger(__name__)

_DB_PATH   = "data/trader.db"
_OLLAMA    = os.environ.get("OLLAMA_URL", _OLLIE_URL)  # Ollie Box GPU (was localhost)
_CACHE_TTL = 900   # 15 minutes
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=5)
    c.row_factory = sqlite3.Row
    return c


def _score_headline(symbol: str, headline: str) -> dict | None:
    """Ask mistral:7b to classify one headline. Returns dict or None on failure."""
    import requests
    prompt = (
        f"Rate this headline's impact on {symbol} stock price. "
        f'Reply with ONLY a JSON: {{"sentiment":"BULLISH" or "BEARISH" or "NEUTRAL",'
        f'"strength":1-10,"reasoning":"one sentence"}}\n\nHeadline: {headline}'
    )
    for model in ["mistral:7b", "0xroyce/plutus:latest"]:
        try:
            r = requests.post(
                f"{_OLLAMA}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 80},
                },
                timeout=15,
            )
            r.raise_for_status()
            raw = r.json().get("response", "").strip()
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            result = json.loads(raw.strip())
            if result.get("sentiment"):
                return result
        except Exception:
            continue
    return None


def get_sentiment(symbol: str) -> dict | None:
    """
    Return aggregated sentiment dict for symbol from recent headlines.
    Uses 15-minute cache. Returns None if no headlines or all calls fail.
    """
    with _cache_lock:
        e = _cache.get(symbol)
        if e and time.time() - e["ts"] < _CACHE_TTL:
            return e["data"]

    # Pull headlines (last 4 hours, symbol or general)
    try:
        db = _conn()
        rows = db.execute("""
            SELECT headline, source FROM market_news
            WHERE (symbol = ? OR symbol IS NULL)
              AND fetched_at >= datetime('now', '-4 hours')
              AND headline IS NOT NULL AND headline != ''
            ORDER BY fetched_at DESC LIMIT 12
        """, (symbol,)).fetchall()
        db.close()
    except Exception:
        return None

    if not rows:
        with _cache_lock:
            _cache[symbol] = {"data": None, "ts": time.time()}
        return None

    scores: list[dict] = []
    key_item: dict | None = None
    key_strength = 0

    for row in rows[:8]:   # cap at 8 to keep latency reasonable
        result = _score_headline(symbol, row["headline"])
        if not result:
            continue
        sentiment = str(result.get("sentiment", "NEUTRAL")).upper()
        if sentiment not in ("BULLISH", "BEARISH", "NEUTRAL"):
            sentiment = "NEUTRAL"
        strength  = max(1, min(10, int(result.get("strength", 5))))
        reasoning = str(result.get("reasoning", ""))[:120]
        scores.append({"headline": row["headline"][:120], "sentiment": sentiment,
                        "strength": strength, "reasoning": reasoning})
        if strength > key_strength:
            key_strength = strength
            key_item = {"headline": row["headline"][:120],
                        "sentiment": sentiment, "strength": strength}

    if not scores:
        with _cache_lock:
            _cache[symbol] = {"data": None, "ts": time.time()}
        return None

    bull_n = sum(1 for s in scores if s["sentiment"] == "BULLISH")
    bear_n = sum(1 for s in scores if s["sentiment"] == "BEARISH")
    neut_n = sum(1 for s in scores if s["sentiment"] == "NEUTRAL")
    avg_str = round(sum(s["strength"] for s in scores) / len(scores), 1)

    if bull_n > bear_n + neut_n:
        agg = "BULLISH"
    elif bear_n > bull_n + neut_n:
        agg = "BEARISH"
    elif bull_n > bear_n:
        agg = "MILDLY BULLISH"
    elif bear_n > bull_n:
        agg = "MILDLY BEARISH"
    else:
        agg = "NEUTRAL"

    text_lines = [
        f"{symbol}: {len(scores)} headlines, sentiment {agg} ({avg_str}/10)",
        f"  {bull_n} bullish | {bear_n} bearish | {neut_n} neutral",
    ]
    if key_item:
        text_lines.append(
            f"  Key: '{key_item['headline']}' ({key_item['sentiment']} {key_item['strength']}/10)"
        )

    data = {
        "label":         "News Sentiment",
        "symbol":        symbol,
        "total":         len(scores),
        "sentiment":     agg,
        "avg_strength":  avg_str,
        "bullish_count": bull_n,
        "bearish_count": bear_n,
        "neutral_count": neut_n,
        "key_headline":  key_item,
        "scores":        scores,
        "text":          "\n".join(text_lines),
    }
    with _cache_lock:
        _cache[symbol] = {"data": data, "ts": time.time()}

    logger.debug("[SENTIMENT] %s: %s %.1f/10 (%d headlines)", symbol, agg, avg_str, len(scores))
    return data
