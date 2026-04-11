"""
Lt. Uhura's News Intercept — Morning news sentiment scan using Finnhub free tier.
Computes mood score, top themes, and convergence signal vs options structure.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta

from engine.finnhub_data import _fh_get

DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

TTL = 1800  # 30 minutes
_cache: dict = {}

AZ_OFFSET = timedelta(hours=-7)

BULLISH_WORDS = [
    "rally", "surge", "gain", "rise", "record", "high", "bull", "beat",
    "strong", "recovery", "optimism", "upbeat", "positive", "breakout",
    "upgrade", "outperform", "buy", "growth", "boom", "soar",
]

BEARISH_WORDS = [
    "crash", "plunge", "drop", "fall", "decline", "recession", "fear",
    "panic", "sell", "downgrade", "miss", "warning", "crisis", "concern",
    "weak", "underperform", "risk", "tariff", "inflation", "rate hike",
]

THEME_KEYWORDS = {
    "Fed/Rates":      ["fed", "fomc", "rate", "powell", "interest", "monetary", "taper"],
    "Earnings":       ["earnings", "eps", "revenue", "quarterly", "results", "profit"],
    "Tariffs/Trade":  ["tariff", "trade", "china", "import", "export", "sanction"],
    "Geopolitical":   ["war", "ukraine", "middle east", "israel", "russia", "conflict"],
    "Tech":           ["ai", "artificial intelligence", "nvidia", "semiconductor", "tech"],
    "Economy":        ["gdp", "jobs", "unemployment", "recession", "inflation", "cpi"],
    "Crypto":         ["bitcoin", "crypto", "ethereum", "blockchain"],
}


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table():
    try:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news_pulse (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                mood_score REAL,
                headline_count INTEGER,
                bullish_count INTEGER,
                bearish_count INTEGER,
                neutral_count INTEGER,
                top_themes_json TEXT,
                convergence_signal TEXT,
                news_summary TEXT,
                fetched_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[NewsPulse] DB init error: {e}")


def _score_headline(headline: str) -> int:
    lower = headline.lower()
    score = 0
    for w in BULLISH_WORDS:
        if w in lower:
            score += 1
    for w in BEARISH_WORDS:
        if w in lower:
            score -= 1
    return score


def _get_top_themes(headlines: list[str], top_n: int = 3) -> list[str]:
    theme_counts: dict[str, int] = {t: 0 for t in THEME_KEYWORDS}
    for headline in headlines:
        lower = headline.lower()
        for theme, keywords in THEME_KEYWORDS.items():
            for kw in keywords:
                if kw in lower:
                    theme_counts[theme] += 1
                    break  # count each theme once per headline
    sorted_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)
    return [t for t, count in sorted_themes[:top_n] if count > 0]


def _get_latest_briefing_context() -> tuple[float | None, str | None]:
    """Read pc_ratio and session_type from latest ready_room_briefing."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT pc_ratio, session_type FROM ready_room_briefings ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            return row["pc_ratio"], row["session_type"]
    except Exception:
        pass
    return None, None


def _compute_convergence(mood_score: float, pc_ratio: float | None) -> str:
    if pc_ratio is None:
        return "UNKNOWN"
    if mood_score > 30 and pc_ratio > 1.2:
        return "DIVERGENCE"   # news bullish, options bearish
    if mood_score < -30 and pc_ratio < 0.8:
        return "DIVERGENCE"   # news bearish, options bullish
    if (mood_score > 30 and pc_ratio < 0.8) or (mood_score < -30 and pc_ratio > 1.2):
        return "CONVERGENCE"
    return "MIXED"


def _build_summary(mood_score: float, bullish: int, bearish: int,
                   themes: list[str], convergence: str) -> str:
    direction = "bullish" if mood_score > 10 else ("bearish" if mood_score < -10 else "neutral")
    theme_str = ", ".join(themes) if themes else "general market news"
    line1 = (
        f"Morning news flow is {direction} (score {mood_score:+.0f}) "
        f"with {bullish} bullish and {bearish} bearish signals across {theme_str}."
    )
    conv_label = {
        "CONVERGENCE": "aligns with options structure — signal is reinforced.",
        "DIVERGENCE":  "diverges from options structure — caution advised.",
        "MIXED":       "shows mixed alignment with options positioning.",
        "UNKNOWN":     "options structure data unavailable for cross-check.",
    }.get(convergence, "")
    line2 = f"News sentiment {conv_label}"
    return f"{line1} {line2}"


def _mood_signal(mood_score: float) -> str:
    if mood_score >= 50:
        return f"News Mood: {mood_score:+.0f} (STRONGLY BULLISH)"
    if mood_score >= 20:
        return f"News Mood: {mood_score:+.0f} (BULLISH)"
    if mood_score > -20:
        return f"News Mood: {mood_score:+.0f} (NEUTRAL)"
    if mood_score > -50:
        return f"News Mood: {mood_score:+.0f} (BEARISH)"
    return f"News Mood: {mood_score:+.0f} (STRONGLY BEARISH)"


def _store_pulse(trade_date: str, result: dict):
    try:
        conn = _conn()
        conn.execute(
            """INSERT INTO news_pulse
               (trade_date, mood_score, headline_count, bullish_count, bearish_count,
                neutral_count, top_themes_json, convergence_signal, news_summary, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                trade_date,
                result["mood_score"],
                result["headline_count"],
                result["bullish_count"],
                result["bearish_count"],
                result.get("neutral_count", 0),
                json.dumps(result["top_themes"]),
                result["convergence_signal"],
                result["news_summary"],
                result["fetched_at"],
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[NewsPulse] DB store error: {e}")


def fetch_news_pulse(force: bool = False) -> dict:
    """
    Fetch Finnhub market news and compute mood score.
    Returns dict with mood_score, signal, convergence, themes, and summary.
    """
    now_ts = time.time()
    if not force and _cache.get("ts") and (now_ts - _cache["ts"]) < TTL:
        return dict(_cache["data"])

    _ensure_table()

    now_az = datetime.now(timezone.utc).astimezone(timezone(AZ_OFFSET))
    trade_date = now_az.strftime("%Y-%m-%d")
    fetched_at = datetime.utcnow().isoformat() + "Z"
    cutoff_ts = now_ts - (12 * 3600)  # last 12 hours

    try:
        time.sleep(0.1)  # Finnhub rate limit courtesy
        raw_news = _fh_get("/news?category=general") or []
    except Exception as e:
        print(f"[NewsPulse] Finnhub fetch error: {e}")
        raw_news = []

    # Filter to last 12 hours
    recent = [
        item for item in raw_news
        if isinstance(item.get("datetime"), (int, float)) and item["datetime"] >= cutoff_ts
    ]

    headlines = [item.get("headline", "") for item in recent if item.get("headline")]
    total = len(headlines)

    bullish_count = 0
    bearish_count = 0
    neutral_count = 0

    for h in headlines:
        s = _score_headline(h)
        if s > 0:
            bullish_count += 1
        elif s < 0:
            bearish_count += 1
        else:
            neutral_count += 1

    if total == 0:
        mood_score = 0.0
    else:
        mood_score = round((bullish_count - bearish_count) / max(1, total) * 100, 1)

    top_themes = _get_top_themes(headlines)
    pc_ratio, session_type = _get_latest_briefing_context()
    convergence = _compute_convergence(mood_score, pc_ratio)
    news_summary = _build_summary(mood_score, bullish_count, bearish_count, top_themes, convergence)
    signal = _mood_signal(mood_score)

    result = {
        "mood_score": mood_score,
        "headline_count": total,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "neutral_count": neutral_count,
        "top_themes": top_themes,
        "convergence_signal": convergence,
        "news_summary": news_summary,
        "signal": signal,
        "fetched_at": fetched_at,
        "error": None,
    }

    if total > 0:
        _store_pulse(trade_date, result)

    _cache["ts"] = now_ts
    _cache["data"] = result
    return dict(result)


def get_latest_news_pulse() -> dict:
    """Return latest stored news pulse from DB (for briefing injection)."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT * FROM news_pulse ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            d["top_themes"] = json.loads(d.get("top_themes_json") or "[]")
            return d
    except Exception as e:
        print(f"[NewsPulse] DB read error: {e}")
    return {
        "mood_score": 0,
        "convergence_signal": "UNKNOWN",
        "news_summary": "No news pulse data available.",
        "signal": "News Mood: +0 (NEUTRAL)",
        "top_themes": [],
        "error": "No data",
    }


def run_news_pulse_morning():
    """Called at 7:30 AM ET daily."""
    fetch_news_pulse(force=True)
