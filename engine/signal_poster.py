"""signal_poster.py — Fire-and-forget POST to Signal Center (port 9000).

Usage:
    from engine.signal_poster import post_to_9000
    post_to_9000("BRIDGE_VOTE", {"consensus": "BUY", "confidence": 85})
"""
from __future__ import annotations
import json
import threading
import urllib.request
import urllib.error
import time
import logging

logger = logging.getLogger(__name__)

_9000_URL = "http://127.0.0.1:9000/api/feed"
_TIMEOUT = 2  # seconds — never block main thread


def _post(feed_type: str, data: dict) -> None:
    """Internal: runs in daemon thread."""
    try:
        payload = json.dumps({"type": feed_type, **data}).encode()
        req = urllib.request.Request(
            _9000_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT):
            pass
    except Exception:
        pass  # 9000 down or slow — silently ignore


def post_to_9000(feed_type: str, data: dict) -> None:
    """Post intelligence to Signal Center in a background thread. Never blocks."""
    t = threading.Thread(target=_post, args=(feed_type, data), daemon=True)
    t.start()
