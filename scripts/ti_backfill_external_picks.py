#!/usr/bin/env python3
"""scripts/ti_backfill_external_picks.py — HM-EXTERNAL-INTEL backfill 2026-05-31.

Recover historical TI swing picks from the 70 already-processed .eml in
inbox/trade_ideas/processed/ into external_picks (the CLEAN store). The live pipeline
buried these in intelligence_feed (junk table); this re-extracts the structured picks ONLY.

SACRED-DATA: writes ONLY external_picks (append-only, idempotent on source+date+ticker).
Does NOT touch intelligence_feed (no dupes — Golden Rule: copy clean, don't move/delete).

    venv/bin/python3 scripts/ti_backfill_external_picks.py [--verbose]
"""
import sys
import email
import email.policy
import email.utils
import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
PROCESSED = ROOT / "inbox" / "trade_ideas" / "processed"

from scripts.ti_picks_parser import (  # reuse the live parser's extraction (single source)
    classify, get_html_body, extract_forwarded_sender, extract_ti_swing_picks,
)
from engine.external_intel import capture_picks
import json


def main() -> int:
    verbose = "--verbose" in sys.argv
    emls = sorted(PROCESSED.glob("*.eml"))
    if not emls:
        print(f"no .eml in {PROCESSED}")
        return 1

    import sqlite3
    c = sqlite3.connect(str(ROOT / "data" / "trader.db"))
    before = c.execute("SELECT COUNT(*) FROM external_picks").fetchone()[0]
    c.close()
    print(f"BACKFILL start: {len(emls)} processed emails | external_picks before = {before}")

    pick_emails = total_picks = total_added = skipped = errors = 0
    for p in emls:
        try:
            with open(p, "rb") as f:
                msg = email.message_from_binary_file(f, policy=email.policy.default)
            from_addr = str(msg.get("From", ""))
            subject = str(msg.get("Subject", ""))
            received = str(msg.get("Date", ""))
            orig = extract_forwarded_sender(msg)
            if orig:
                from_addr = orig
            ft = classify(from_addr, subject)
            if ft != "ti_swing_picks":
                skipped += 1
                continue
            html = get_html_body(msg)
            picks = extract_ti_swing_picks(html)
            if not picks:
                skipped += 1
                continue
            try:
                pd = email.utils.parsedate_to_datetime(received).date().isoformat()
            except Exception:
                pd = datetime.date.today().isoformat()
            mapped = [{
                "ticker": pk["ticker"], "action": "buy",
                "entry": pk.get("entry"), "stop": pk.get("stop"),
                "note": (pk.get("raw_text") or "")[:200],
                "raw_json": json.dumps({k: pk.get(k) for k in ("ticker", "entry", "stop", "strategy")}),
            } for pk in picks if pk.get("ticker")]
            added = capture_picks(mapped, "TI Swing Picks (email)", pd)
            pick_emails += 1
            total_picks += len(mapped)
            total_added += added
            if verbose:
                print(f"  {p.name[:40]} [{pd}]: {len(mapped)} picks, +{added} new "
                      f"({[m['ticker'] for m in mapped]})")
        except Exception as e:
            errors += 1
            print(f"  ERROR {p.name[:40]}: {type(e).__name__}: {e}")

    c = sqlite3.connect(str(ROOT / "data" / "trader.db"))
    after = c.execute("SELECT COUNT(*) FROM external_picks").fetchone()[0]
    rng = c.execute("SELECT MIN(pick_date), MAX(pick_date) FROM external_picks").fetchone()
    c.close()
    print(f"BACKFILL done: pick-emails={pick_emails} skipped={skipped} errors={errors}")
    print(f"  picks parsed={total_picks} NEW added={total_added} (idempotent)")
    print(f"  external_picks: {before} -> {after} | date range {rng[0]} .. {rng[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
