#!/usr/bin/env python3
"""
UHURA Step 2 — Parse Signals
For each raw news item, call Ollama (qwen3:8b) to extract structured signal.
Idempotent: skips already-parsed news_ids.
"""
import os
import json
import time
import hashlib
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from db import get_conn  # noqa: E402

OLLAMA_URL = "http://192.168.1.168:11434/api/chat"
# ministral-3:3b: fast, reliable JSON output; qwen3:8b returns empty (thinking-only mode)
PARSE_MODEL = "ministral-3:3b"
BATCH_SIZE = int(os.environ.get("UHURA_BATCH", "500"))
# Optional date filter: UHURA_AFTER=2025-02-01 to target specific periods
PARSE_AFTER = os.environ.get("UHURA_AFTER", "")
PARSE_BEFORE = os.environ.get("UHURA_BEFORE", "")

SYSTEM_PROMPT = """You are a financial news classifier. Given a news headline and optional summary, output ONLY a valid JSON object with these fields:
- sentiment: "BULLISH", "BEARISH", or "NEUTRAL"
- confidence: float 0.0 to 1.0 (how confident you are in the sentiment)
- event_type: one of "earnings", "merger", "guidance", "macro", "other"
- urgency: integer 1 to 5 (1=routine, 5=market-moving breaking news)

Rules:
- earnings: quarterly results, EPS beats/misses, revenue surprises
- merger: M&A, acquisitions, buyouts, divestitures
- guidance: forward outlook, raised/lowered guidance, analyst targets
- macro: Fed/rates/inflation/jobs data, geopolitical, sector-wide news
- other: product launches, lawsuits, executive changes, etc.
Output ONLY the JSON object. No explanation, no markdown."""


def ollama_parse(headline: str, summary: str | None) -> dict | None:
    text = headline
    if summary and summary.strip():
        text = f"{headline}\n\nContext: {summary.strip()[:300]}"

    payload = {
        "model": PARSE_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Classify this news:\n{text}"},
        ],
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 128},
    }
    try:
        r = requests.post(OLLAMA_URL, json=payload, timeout=30)
        r.raise_for_status()
        content = r.json()["message"]["content"].strip()
        # Strip <think>...</think> blocks (qwen3 reasoning tokens)
        if "<think>" in content:
            end = content.find("</think>")
            content = content[end + 8:].strip() if end != -1 else content
        # Strip markdown code fences (```json ... ```)
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        # Extract JSON
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        parsed = json.loads(content[start:end])
        # Validate and clamp
        return {
            "sentiment": parsed.get("sentiment", "NEUTRAL").upper()
                if parsed.get("sentiment", "").upper() in ("BULLISH", "BEARISH", "NEUTRAL")
                else "NEUTRAL",
            "confidence": min(1.0, max(0.0, float(parsed.get("confidence", 0.5)))),
            "event_type": parsed.get("event_type", "other")
                if parsed.get("event_type") in ("earnings", "merger", "guidance", "macro", "other")
                else "other",
            "urgency": min(5, max(1, int(parsed.get("urgency", 2)))),
        }
    except (requests.RequestException, json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
        print(f"  Parse error: {e}")
        return None


def main() -> None:
    conn = get_conn()

    # Build date filters
    clauses = ["s.id IS NULL"]
    params: list = []
    if PARSE_AFTER:
        clauses.append("n.created_at >= ?")
        params.append(PARSE_AFTER)
    if PARSE_BEFORE:
        clauses.append("n.created_at < ?")
        params.append(PARSE_BEFORE)
    where = " AND ".join(clauses)

    # Find unprocessed raw news
    rows = conn.execute(
        f"""SELECT n.id, n.ticker, n.headline, n.summary, n.created_at
           FROM uhura_raw_news n
           LEFT JOIN uhura_signals s ON s.news_id = n.id AND s.ticker = n.ticker
           WHERE {where}
           ORDER BY n.created_at ASC
           LIMIT ?""",
        (*params, BATCH_SIZE),
    ).fetchall()

    total = len(rows)
    print(f"Unprocessed: {total} items (batch cap {BATCH_SIZE})")
    if not total:
        total_parsed = conn.execute("SELECT COUNT(*) FROM uhura_signals").fetchone()[0]
        print(f"All done. Total parsed: {total_parsed}")
        conn.close()
        return

    success = 0
    fail = 0
    for i, row in enumerate(rows):
        news_id, ticker, headline, summary, created_at = (
            row["id"], row["ticker"], row["headline"], row["summary"], row["created_at"]
        )
        result = ollama_parse(headline, summary)

        if not result:
            fail += 1
            # Insert as NEUTRAL/other so it doesn't get retried endlessly
            result = {"sentiment": "NEUTRAL", "confidence": 0.0, "event_type": "other", "urgency": 1}

        h = hashlib.sha256(headline.encode()).hexdigest()[:16]
        try:
            conn.execute(
                """INSERT OR IGNORE INTO uhura_signals
                   (news_id, ticker, published_at, headline_hash, sentiment, confidence,
                    event_type, urgency, parse_model)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    news_id, ticker, created_at, h,
                    result["sentiment"], result["confidence"],
                    result["event_type"], result["urgency"], PARSE_MODEL,
                ),
            )
            success += 1
        except Exception as e:
            print(f"  DB insert error: {e}")
            fail += 1

        if (i + 1) % 50 == 0:
            conn.commit()
            print(f"  {i + 1}/{total} processed ({success} ok, {fail} err)")
            time.sleep(0.5)

    conn.commit()
    total_parsed = conn.execute("SELECT COUNT(*) FROM uhura_signals").fetchone()[0]
    conn.close()
    print(f"\nParse complete: {success} ok, {fail} err | total in DB: {total_parsed}")


if __name__ == "__main__":
    main()
