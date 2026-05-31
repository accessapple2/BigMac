#!/usr/bin/env python3
"""scripts/backfill_prose_corpus.py — HM-EXTERNAL-INTEL Tier-2 backfill (2026-05-31).

Re-parse the already-processed .eml in inbox/trade_ideas/processed/ into external_intel_text,
now that (a) prose capture exists and (b) the eM Client forward-bug is fixed — so prose
newsletters/theses the OLD parser silently DROPPED are recovered.

Captures PROSE_FEEDS only (trendspider_alerts, ti_barrie_picks theses, trade_of_week). Excludes
grok_kirk_scan (OT's own digests), danelfin (structured), ti_unknown (ads), ti_swing_picks
(already → external_picks). REDUNDANCY: prose/theme/thesis layer only — earnings/insider/congress
stay API-fed, never re-extracted (write_external_prose only writes external_intel_text).

IDEMPOTENT: UNIQUE(source,intel_date,raw_text) dedupes — already-captured rows skip. SACRED-DATA:
append-only; .eml stay in processed/ (copy-extract, never moved/deleted). Safe to re-run.

    venv/bin/python3 scripts/backfill_prose_corpus.py
"""
import sys
import glob
import sqlite3
import logging
import email
import email.policy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ti_picks_parser import (
    classify, extract_forwarded_sender, get_html_body, write_external_prose, PROSE_FEEDS,
)
from bs4 import BeautifulSoup

DB = str(ROOT / "data" / "trader.db")
PROCESSED = ROOT / "inbox" / "trade_ideas" / "processed"


def main() -> int:
    log = logging.getLogger("backfill")
    log.addHandler(logging.NullHandler())
    c = sqlite3.connect(DB, timeout=30.0)
    before = c.execute("SELECT COUNT(*) FROM external_intel_text").fetchone()[0] \
        if c.execute("SELECT 1 FROM sqlite_master WHERE name='external_intel_text'").fetchone() else 0
    print(f"external_intel_text BEFORE: {before}")

    attempted = captured = skipped_dup = 0
    for f in sorted(glob.glob(str(PROCESSED / "*.eml"))):
        try:
            with open(f, "rb") as fh:
                m = email.message_from_binary_file(fh, policy=email.policy.default)
            fa = extract_forwarded_sender(m) or str(m.get("From", ""))
            ft = classify(fa, str(m.get("Subject", "")))
            if ft not in PROSE_FEEDS:
                continue
            html = get_html_body(m)
            if len(BeautifulSoup(html or "", "lxml").get_text(" ", strip=True)) < 80:
                continue
            attempted += 1
            n0 = c.execute("SELECT COUNT(*) FROM external_intel_text").fetchone()[0]
            write_external_prose(log, ft, html, str(m.get("Date", "")))
            n1 = c.execute("SELECT COUNT(*) FROM external_intel_text").fetchone()[0]
            captured += (n1 > n0)
            skipped_dup += (n1 == n0)
        except Exception as e:
            print(f"  ERR {Path(f).name}: {type(e).__name__}: {e}")

    after = c.execute("SELECT COUNT(*) FROM external_intel_text").fetchone()[0]
    rng = c.execute("SELECT MIN(intel_date),MAX(intel_date) FROM external_intel_text").fetchone()
    c.close()
    print(f"attempted={attempted} captured(new)={captured} skipped(dup)={skipped_dup}")
    print(f"external_intel_text AFTER: {after} (net-new {after - before}) | span {rng[0]} → {rng[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
