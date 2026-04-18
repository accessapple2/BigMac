#!/usr/bin/env python3
"""
Danelfin Weekly Cron
Reads data/danelfin_scores.json (or .txt) and calls danelfin_parser.manual_input().
Runs every Sunday at 8 PM AZ via launchd.

Score file formats supported:
  JSON: {"date": "2026-04-14", "scores": {"AAPL": 8, "NVDA": 10}}
  Text: data/danelfin_scores.txt  →  "AAPL-8, NVDA-10, MSFT-9"
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DANELFIN_CRON] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR  = Path(__file__).parent.parent / "data"
JSON_FILE = DATA_DIR / "danelfin_scores.json"
TXT_FILE  = DATA_DIR / "danelfin_scores.txt"


def load_scores_from_json(path: Path) -> str:
    """Convert JSON score map to text format for parse_scores()."""
    with open(path) as f:
        data = json.load(f)
    scores_map = data.get("scores", {})
    if not scores_map:
        return ""
    parts = [f"{ticker}-{score}" for ticker, score in scores_map.items()]
    score_date = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    log.info(f"Loaded {len(parts)} scores from {path.name} (date: {score_date})")
    return ", ".join(parts)


def run():
    log.info(f"=== Danelfin Weekly Cron — {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    # Pick source file
    scores_text = ""
    if JSON_FILE.exists():
        try:
            scores_text = load_scores_from_json(JSON_FILE)
        except Exception as e:
            log.error(f"Failed to read {JSON_FILE}: {e}")
    elif TXT_FILE.exists():
        scores_text = TXT_FILE.read_text().strip()
        log.info(f"Loaded scores from {TXT_FILE.name}")

    if not scores_text:
        log.warning("No score file found. Create data/danelfin_scores.json or data/danelfin_scores.txt")
        log.info('JSON format: {"date": "2026-04-14", "scores": {"AAPL": 8, "NVDA": 10}}')
        log.info('Text format: "AAPL-8, NVDA-10, MSFT-9"')
        sys.exit(0)

    # Import the production parser
    from engine.danelfin_parser import manual_input
    scores = manual_input(scores_text)
    log.info(f"Weekly update complete: {len(scores)} scores processed")
    log.info("=== Done ===")


if __name__ == "__main__":
    run()
