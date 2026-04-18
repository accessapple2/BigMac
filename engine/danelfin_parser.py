#!/usr/bin/env python3
"""
Danelfin AI Score Parser
Parses newsletter scores and posts signals for high-scoring stocks (8–10).

Usage:
  # From command line:
  cd ~/autonomous-trader
  venv/bin/python3 engine/danelfin_parser.py

  # From Python:
  from engine.danelfin_parser import manual_input
  manual_input("AG-10, HL-10, AAL-9, HOOD-8")
"""
from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DANELFIN] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH           = Path(__file__).parent.parent / "data" / "trader.db"
SIGNAL_CENTER_URL = "http://localhost:9000/api/signal"


def init_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS danelfin_scores (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            ai_score    INTEGER NOT NULL,
            category    TEXT DEFAULT 'US_STOCKS',
            scan_date   TEXT NOT NULL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def parse_scores(raw_text: str) -> list[dict]:
    """
    Parse score text into list of {symbol, score} dicts.
    Handles formats: "AG - 10", "HOOD (8)", "MSFT: 8", "AAL-9", "TSLA 10"
    """
    scores = []
    for item in re.split(r"[,\n]", raw_text):
        item = item.strip()
        if not item:
            continue
        # Extract ticker (1-5 uppercase letters) + score (1-2 digits)
        m = re.search(r"\b([A-Z]{1,5})\b.*?(\b([1-9]|10)\b)", item)
        if m:
            ticker = m.group(1)
            score  = int(m.group(2))
            if 1 <= score <= 10:
                scores.append({"symbol": ticker, "score": score})
    return scores


def save_scores(scores: list[dict], category: str = "US_STOCKS") -> int:
    conn  = sqlite3.connect(DB_PATH)
    today = datetime.now().strftime("%Y-%m-%d")
    saved = 0
    for item in scores:
        conn.execute(
            "INSERT INTO danelfin_scores (symbol, ai_score, category, scan_date) "
            "VALUES (?, ?, ?, ?)",
            (item["symbol"], item["score"], category, today),
        )
        saved += 1
    conn.commit()
    conn.close()
    log.info(f"Saved {saved} scores to danelfin_scores")
    return saved


def post_signals(scores: list[dict]) -> int:
    """Post signals for AI score ≥ 8 to Signal Center."""
    posted = 0
    for item in scores:
        score  = item["score"]
        symbol = item["symbol"]
        if score < 8:
            continue

        # 8→70%, 9→80%, 10→90%
        confidence = 70 + (score - 8) * 10
        action     = "BUY" if score >= 9 else "WATCH"

        # Fetch current price from Yahoo Finance
        price = 0.0
        try:
            r = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"interval": "1d", "range": "1d"},
                headers={"User-Agent": "Mozilla/5.0 OllieTrades/6.0"},
                timeout=8,
            )
            if r.status_code == 200:
                price = float(
                    r.json()["chart"]["result"][0]["meta"].get("regularMarketPrice", 0)
                )
        except Exception:
            pass

        payload = {
            "symbol":          symbol,
            "action":          action,
            "type":            "SWING",
            "confidence":      confidence,
            "agent":           "danelfin_ai",
            "model":           "danelfin_newsletter",
            "reasoning":       f"[DANELFIN] AI Score {score}/10 — high conviction signal",
            "price":           price,
            "stop_loss":       round(price * 0.95, 2) if price else 0,
            "take_profit":     round(price * 1.12, 2) if price else 0,
            "timeframe":       "SWING",
            "context_summary": f"Danelfin AI Score: {score}/10",
            "sources":         ["danelfin_ai", "danelfin_newsletter"],
        }
        try:
            r = requests.post(SIGNAL_CENTER_URL, json=payload, timeout=5)
            if r.status_code in (200, 201):
                log.info(f"  ✓ {symbol}: score {score} → {action} @ {confidence}%  ${price:.2f}")
                posted += 1
            else:
                log.warning(f"  ✗ {symbol} → HTTP {r.status_code}")
        except Exception as e:
            log.warning(f"  ✗ {symbol} post failed: {e}")

    return posted


def manual_input(scores_text: str, category: str = "US_STOCKS") -> list[dict]:
    """
    Parse, save, and post signals from newsletter text.
    Example: manual_input("AG-10, HL-10, AAL-9, HOOD-8")
    """
    init_table()
    scores = parse_scores(scores_text)
    if not scores:
        log.warning("No valid scores found in input")
        return []
    save_scores(scores, category)
    posted = post_signals(scores)
    log.info(f"manual_input: {len(scores)} parsed, {posted} signals posted")
    return scores


def get_danelfin_score(symbol: str) -> dict | None:
    """Return the most recent Danelfin score for a symbol (for API/agent queries)."""
    init_table()
    conn = sqlite3.connect(DB_PATH)
    row  = conn.execute("""
        SELECT symbol, ai_score, category, scan_date, created_at
        FROM   danelfin_scores
        WHERE  symbol = ?
        ORDER  BY scan_date DESC, created_at DESC
        LIMIT  1
    """, (symbol.upper(),)).fetchone()
    conn.close()
    if not row:
        return None
    score = row[1]
    confidence = 70 + (score - 8) * 10 if score >= 8 else max(10, score * 10)
    return {
        "symbol":     row[0],
        "ai_score":   score,
        "confidence": confidence,
        "signal":     "BUY" if score >= 9 else ("WATCH" if score >= 8 else "NEUTRAL"),
        "category":   row[2],
        "scan_date":  row[3],
        "created_at": row[4],
    }


def get_top_danelfin_picks(n: int = 10, min_score: int = 8) -> list[dict]:
    """Return top N most recent picks with ai_score >= min_score."""
    init_table()
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT symbol, ai_score, category, scan_date
        FROM   danelfin_scores
        WHERE  ai_score >= ?
          AND  scan_date >= date('now', '-14 days')
        ORDER  BY ai_score DESC, scan_date DESC
        LIMIT  ?
    """, (min_score, n)).fetchall()
    conn.close()
    results = []
    for r in rows:
        score = r[1]
        confidence = 70 + (score - 8) * 10 if score >= 8 else max(10, score * 10)
        results.append({
            "symbol":     r[0],
            "ai_score":   score,
            "confidence": confidence,
            "signal":     "BUY" if score >= 9 else ("WATCH" if score >= 8 else "NEUTRAL"),
            "category":   r[2],
            "scan_date":  r[3],
        })
    return results


def run():
    init_table()
    log.info("=== Danelfin AI Score Parser ===")
    log.info("Usage: manual_input('AG-10, HL-10, AAL-9, HOOD-8')")

    # Smoke test with April 7 example data
    example = "AG-10, HL-10, AAL-9, HOOD-8, BTG-8, U-8, CDE-8, BAC-8, MSFT-8"
    log.info(f"Parsing example: {example}")
    scores = parse_scores(example)
    log.info(f"Parsed {len(scores)} scores: {scores}")
    log.info("(Run manual_input() to actually save + post signals)")
    log.info("=== Done ===")


if __name__ == "__main__":
    run()
