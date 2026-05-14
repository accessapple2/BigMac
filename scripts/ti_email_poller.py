#!/usr/bin/env python3
"""HM-CA: TI email IMAP poller.

Connects to OllieTradeMinds@gmail.com via IMAP, fetches UNSEEN messages,
writes each as INBOX_YYYYMMDD_HHMMSS_<uid>.eml to inbox/trade_ideas/,
marks messages \\Seen. Idempotent.

Run:
    venv/bin/python3 scripts/ti_email_poller.py [--once] [--verbose]
"""
from __future__ import annotations
import argparse
import imaplib
import logging
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INBOX_DIR = ROOT / "inbox" / "trade_ideas"
LOG_FILE = ROOT / "logs" / "ti_email_poller.log"


def load_env(env_path: Path) -> dict:
    env = {}
    if not env_path.exists():
        return env
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip()
    return env


def setup_logging(verbose: bool):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    log = logging.getLogger("ti_email_poller")
    log.setLevel(level)
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


def fetch_unseen(log, env: dict) -> int:
    user = env.get("TI_GMAIL_USER")
    pw   = env.get("TI_GMAIL_APP_PASSWORD")
    host = env.get("TI_IMAP_HOST", "imap.gmail.com")
    port = int(env.get("TI_IMAP_PORT", "993"))
    if not user or not pw:
        log.error("TI_GMAIL_USER or TI_GMAIL_APP_PASSWORD missing from .env (NOT logging values)")
        return -1
    log.info("Connecting to %s:%d as %s", host, port, user)
    try:
        conn = imaplib.IMAP4_SSL(host, port)
        conn.login(user, pw)
    except Exception as e:
        log.exception("IMAP connect/login failed: %s", type(e).__name__)
        return -1
    try:
        conn.select("INBOX")
        typ, data = conn.search(None, "UNSEEN")
        if typ != "OK":
            log.error("IMAP SEARCH UNSEEN failed: typ=%s", typ)
            return -1
        uids = (data[0] or b"").split()
        log.info("UNSEEN message count: %d", len(uids))
        INBOX_DIR.mkdir(parents=True, exist_ok=True)
        fetched = 0
        for uid in uids:
            try:
                typ, msg_data = conn.fetch(uid, "(RFC822)")
                if typ != "OK" or not msg_data or msg_data[0] is None:
                    log.warning("Fetch failed for uid=%s", uid.decode(errors="replace"))
                    continue
                raw = msg_data[0][1]
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                uid_s = uid.decode(errors="replace")
                fn = INBOX_DIR / f"INBOX_{ts}_{uid_s}.eml"
                fn.write_bytes(raw)
                conn.store(uid, "+FLAGS", "\\Seen")
                log.info("saved %s (%d bytes)", fn.name, len(raw))
                fetched += 1
            except Exception as e:
                log.exception("uid=%s failed: %s", uid, e)
        log.info("fetched=%d", fetched)
        return fetched
    finally:
        try: conn.close()
        except Exception: pass
        try: conn.logout()
        except Exception: pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="one-shot mode (default)")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()
    log = setup_logging(args.verbose)
    env = load_env(ROOT / ".env")
    n = fetch_unseen(log, env)
    if n < 0:
        sys.exit(2)
    log.info("done. fetched=%d", n)


if __name__ == "__main__":
    main()
