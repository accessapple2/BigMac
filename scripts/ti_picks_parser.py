#!/usr/bin/env python3
"""HM-CA: TI email parser.

Parse one .eml, classify via from+subject, extract picks where possible,
insert one row into signal-center/signals.db::intelligence_feed.

Usage:
    venv/bin/python3 scripts/ti_picks_parser.py <path-to-eml>

Exit codes:
    0 = inserted (or classified SKIP intentionally)
    2 = file not found
    3 = parse failed
    4 = DB insert failed
"""
from __future__ import annotations
import argparse
import email
import email.policy
import json
import logging
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SC_DB = ROOT / "signal-center" / "signals.db"
LOG_FILE = ROOT / "logs" / "ti_picks_parser.log"


def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("ti_picks_parser")
    log.setLevel(logging.INFO)
    log.propagate = False
    for h in list(log.handlers):
        log.removeHandler(h)
    fh = logging.FileHandler(LOG_FILE, mode="a")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    log.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
    log.addHandler(sh)
    return log


def classify(from_addr: str, subject: str) -> str:
    """Return one of the 7 feed_type values, or 'SKIP'."""
    fa = (from_addr or "").lower()
    s  = (subject or "").lower()
    if "noreply" in fa and "x.ai" in fa:
        return "grok_kirk_scan"
    if "noreply" in fa and "danelfin" in fa:
        return "danelfin_ai_rank"
    if "info" in fa and "trade-ideas.com" in fa:
        return "ti_official"
    if "hello" in fa and "mail.trade-ideas.com" in fa:
        if "trade of the week" in s:
            return "trade_of_week"
        if "here's what makes this" in s or "here’s what makes this" in s:
            return "ti_barrie_picks"
        if "a-list" in s or "tickers" in s or "swing pick" in s:
            return "ti_swing_picks"
        return "ti_unknown"
    if "hello" in fa and "trendspider.com" in fa:
        return "trendspider_alerts"
    return "SKIP"


def get_html_body(msg) -> str:
    """Return HTML body if available, else plain text."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                try: return part.get_content() or ""
                except Exception: pass
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try: return part.get_content() or ""
                except Exception: pass
        return ""
    try:
        return msg.get_content() or ""
    except Exception:
        return ""



def get_plain_body(msg) -> str:
    """Return plain text body if available, empty string otherwise."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try: return part.get_content() or ""
                except Exception: pass
        return ""
    try:
        return msg.get_content() or ""
    except Exception:
        return ""


def extract_forwarded_sender(msg) -> str | None:
    """If this email is a forward, return the original sender from the body.

    Looks for a 'Forwarded message' marker (Gmail/DuckDuckGo style) then the
    next From: line, extracts the email address inside angle brackets.

    Returns None if not a forward or no original sender found.
    """
    bodies = []
    plain = get_plain_body(msg)
    if plain:
        bodies.append(plain)
    html = get_html_body(msg)
    if html and html != plain:
        # Strip HTML tags lightly for regex purposes
        bodies.append(re.sub(r"<[^>]+>", " ", html))
    blob = "\n".join(bodies)
    if not blob:
        return None
    marker_re = re.compile(r"(Forwarded message|Begin forwarded message)", re.IGNORECASE)
    m = marker_re.search(blob)
    if not m:
        return None
    after = blob[m.start():]
    # Find first From: that has an email address in <...>
    fm = re.search(r"From\s*:\s*[^<\n]*?<\s*([^>\s]+@[^>\s]+)\s*>", after, re.IGNORECASE)
    if not fm:
        return None
    return fm.group(1).strip()


def extract_ti_swing_picks(html: str) -> list:
    """Andy / A-List structured picks: ticker headers + Entry Alert / Stop Loss
    sentences. Best-effort heuristic — returns [] on shape mismatch.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html or "", "lxml")
    picks = []
    seen = set()
    for tag in soup.find_all(["strong", "b", "h1", "h2", "h3"]):
        text = (tag.get_text(strip=True) or "").upper()
        m = re.fullmatch(r"[A-Z]{1,5}", text)
        if not m:
            continue
        ticker = m.group(0)
        if ticker in seen:
            continue
        # Gather surrounding context (next ~1200 chars of text).
        ctx_parts = []
        node = tag
        for _ in range(40):
            node = node.next_element
            if node is None:
                break
            try:
                t = node.get_text(" ", strip=True) if hasattr(node, "get_text") else str(node).strip()
                if t:
                    ctx_parts.append(t)
            except Exception:
                continue
            if sum(len(p) for p in ctx_parts) > 1200:
                break
        ctx = " ".join(ctx_parts)
        entry_m = re.search(r"Entry\s*Alert[^$]{0,80}\$([0-9]+(?:\.[0-9]+)?)", ctx, re.IGNORECASE)
        stop_m  = re.search(r"Stop\s*Loss[^$]{0,80}\$([0-9]+(?:\.[0-9]+)?)", ctx, re.IGNORECASE)
        if not (entry_m or stop_m):
            continue
        picks.append({
            "ticker": ticker,
            "entry": float(entry_m.group(1)) if entry_m else None,
            "stop":  float(stop_m.group(1))  if stop_m  else None,
            "strategy": "swing",
            "raw_text": ctx[:500],
        })
        seen.add(ticker)
    return picks


def build_payload(eml_path: Path, feed_type: str, from_addr: str,
                  subject: str, received_at: str, html: str) -> dict:
    payload = {
        "source_email_id": eml_path.name,
        "sender": from_addr,
        "subject": subject,
        "received_at": received_at,
        "picks": [],
        "raw_html_excerpt": (html or "")[:4000],
    }
    if feed_type == "ti_swing_picks":
        try:
            payload["picks"] = extract_ti_swing_picks(html)
        except Exception as e:
            payload["pick_extraction_error"] = f"{type(e).__name__}: {e}"
    return payload


def insert_row(log, feed_type: str, payload: dict) -> int:
    SC_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SC_DB), timeout=30)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO intelligence_feed (feed_type, data) VALUES (?, ?)",
            (feed_type, json.dumps(payload)),
        )
        conn.commit()
        rid = cur.lastrowid
        log.info("inserted feed_type=%s id=%d picks=%d",
                 feed_type, rid, len(payload.get("picks", [])))
        return rid
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("eml_path")
    args = ap.parse_args()
    log = setup_logging()
    eml_path = Path(args.eml_path)
    if not eml_path.exists():
        log.error("file not found: %s", eml_path)
        sys.exit(2)
    try:
        with open(eml_path, "rb") as f:
            msg = email.message_from_binary_file(f, policy=email.policy.default)
    except Exception as e:
        log.exception("parse failed: %s", e)
        sys.exit(3)
    from_addr = str(msg.get("From", ""))
    subject   = str(msg.get("Subject", ""))
    received  = str(msg.get("Date", ""))
    # If this is a forwarded message, use the original sender for classification.
    # (Gmail/DuckDuckGo forwarding rewrites outer From: header.)
    original = extract_forwarded_sender(msg)
    if original:
        log.info("forwarded message: original sender=%r (outer From=%r)", original, from_addr)
        from_addr = original
    feed_type = classify(from_addr, subject)
    log.info("eml=%s from=%r subject=%r -> feed_type=%s",
             eml_path.name, from_addr, subject, feed_type)
    if feed_type == "SKIP":
        log.info("SKIP — sender pattern does not match any TI source")
        sys.exit(0)
    html = get_html_body(msg)
    payload = build_payload(eml_path, feed_type, from_addr, subject, received, html)
    try:
        insert_row(log, feed_type, payload)
    except Exception as e:
        log.exception("DB insert failed: %s", e)
        sys.exit(4)
    sys.exit(0)


if __name__ == "__main__":
    main()
