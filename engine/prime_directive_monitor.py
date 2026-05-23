"""HM-PRIME-DIRECTIVE-MONITOR v1 — 2026-05-22.

Polls USTR / Commerce Dept / Federal Register RSS feeds for tariff +
trade-policy events. On detection, emits to the canonical events bus
as `event_type='macro'`, `source='prime_directive'` with the headline
+ affected sector ETFs in the payload.

Downstream consumers (IC squadron Risk Officer, regime_router future
auto-tuner) read the events table for tariff events to trigger a
24h heightened IC scan on affected sectors.

Spec: overnight cook Bundle D1.

Behavior:
  * Schedule: every 30 minutes during market hours (main.py scheduler)
  * Dedup window: 4 hours (same headline doesn't re-fire)
  * Crash-safe: any RSS / parse / emit error logs `[PRIME-DIRECTIVE-ERROR]`
    and continues to the next feed (HM-Z/HM-AA error posture)
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

from rich.console import Console

console = Console()

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"

# RSS feeds — all free, government-source
_FEEDS: list[tuple[str, str]] = [
    ("ustr", "https://ustr.gov/rss.xml"),
    ("commerce", "https://www.commerce.gov/feeds/news"),
    (
        "fed_register_ita",
        "https://www.federalregister.gov/api/v1/documents.rss"
        "?conditions[agencies][]=international-trade-administration",
    ),
]

# Keyword matching — case-insensitive substring search over title + summary.
_TARIFF_KEYWORDS: list[str] = [
    "tariff",
    "section 232",
    "section 301",
    "trade barrier",
    "import duty",
    "countervailing",
    "anti-dumping",
    "antidumping",
    "trade war",
    "trade restriction",
    "export control",
]

# Keyword → affected sector ETFs (broad-stroke mapping; v1 is intentionally
# coarse — refine in v2 when we have outcome data).
_SECTOR_MAP: dict[str, list[str]] = {
    "tariff": ["XLI", "XLB", "XLK"],
    "section 232": ["XLB", "XLE"],   # metals, energy
    "section 301": ["XLK", "XLY"],    # tech, consumer disc
    "trade barrier": ["XLI", "XLB"],
    "import duty": ["XLY", "XLP"],
    "countervailing": ["XLB", "XLI"],
    "anti-dumping": ["XLB", "XLI"],
    "antidumping": ["XLB", "XLI"],
    "trade war": ["XLI", "XLB", "XLK", "XLY"],
    "trade restriction": ["XLI", "XLB", "XLK"],
    "export control": ["XLK"],        # semis
}

# Simple regex-based RSS item extraction so we don't add a feedparser
# dependency. Handles the common <item><title>X</title>...<description>Y
# </description></item> shape used by all three feeds.
_ITEM_RE = re.compile(
    r"<item>(.*?)</item>", re.IGNORECASE | re.DOTALL
)
_TITLE_RE = re.compile(
    r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
    re.IGNORECASE | re.DOTALL,
)
_DESC_RE = re.compile(
    r"<description[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>",
    re.IGNORECASE | re.DOTALL,
)
_LINK_RE = re.compile(r"<link[^>]*>(.*?)</link>", re.IGNORECASE | re.DOTALL)


def _strip_html(s: str) -> str:
    """Cheap HTML strip — RSS descriptions often contain embedded tags."""
    return re.sub(r"<[^>]+>", "", s or "").strip()


def _fetch_feed(url: str, timeout: float = 10.0) -> str:
    """GET an RSS feed. Returns raw XML body or empty string on error."""
    import requests
    try:
        r = requests.get(url, timeout=timeout, headers={
            "User-Agent": "OllieTrades-PrimeDirective/1.0 (+monitor)",
        })
        if r.status_code != 200:
            console.log(
                f"[yellow][PRIME-DIRECTIVE] {url} returned "
                f"HTTP {r.status_code}"
            )
            return ""
        return r.text
    except Exception as e:
        console.log(
            f"[red][PRIME-DIRECTIVE-ERROR] fetch {url}: "
            f"{type(e).__name__}: {e!r}"
        )
        return ""


def _parse_items(xml: str) -> list[dict]:
    """Parse RSS items into [{title, link, summary}]."""
    items: list[dict] = []
    for m in _ITEM_RE.finditer(xml or ""):
        block = m.group(1)
        title_m = _TITLE_RE.search(block)
        desc_m = _DESC_RE.search(block)
        link_m = _LINK_RE.search(block)
        title = _strip_html(title_m.group(1)) if title_m else ""
        summary = _strip_html(desc_m.group(1)) if desc_m else ""
        link = (link_m.group(1).strip() if link_m else "")
        if title:
            items.append({"title": title, "link": link, "summary": summary})
    return items


def _matches_keyword(text: str) -> list[str]:
    """Return list of tariff keywords that match this text (lowercased)."""
    if not text:
        return []
    low = text.lower()
    return [kw for kw in _TARIFF_KEYWORDS if kw in low]


def _affected_sectors(matched_keywords: Iterable[str]) -> list[str]:
    """Aggregate unique sector ETFs across the matched keywords."""
    out: list[str] = []
    seen: set[str] = set()
    for kw in matched_keywords:
        for sym in _SECTOR_MAP.get(kw, []):
            if sym not in seen:
                seen.add(sym)
                out.append(sym)
    return out


def _was_recently_emitted(title: str, hours: int = 4) -> bool:
    """Dedup: has this exact title been emitted in the last N hours?

    Checks the events table for source='prime_directive' rows whose
    payload contains the headline. Fail-safe: any DB error returns
    False (allow the emit through).
    """
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=10)
        try:
            row = conn.execute(
                "SELECT 1 FROM events "
                " WHERE source='prime_directive' "
                "   AND event_type='macro' "
                "   AND payload LIKE ? "
                "   AND ts >= datetime('now', ?) "
                " LIMIT 1",
                (f'%{title[:80]}%', f'-{int(hours)} hours'),
            ).fetchone()
            return row is not None
        finally:
            conn.close()
    except Exception:
        return False


def scan_prime_directive() -> dict:
    """Run one scan pass across all configured feeds.

    Returns a summary dict {items_total, items_matched, emitted_count,
    errors}.
    """
    from engine.events_bus import emit_event

    items_total = 0
    items_matched = 0
    emitted_count = 0
    errors = 0

    for feed_name, feed_url in _FEEDS:
        xml = _fetch_feed(feed_url)
        if not xml:
            errors += 1
            continue
        try:
            items = _parse_items(xml)
        except Exception as e:
            errors += 1
            console.log(
                f"[red][PRIME-DIRECTIVE-ERROR] parse {feed_name}: "
                f"{type(e).__name__}: {e!r}"
            )
            continue
        items_total += len(items)

        for it in items:
            haystack = f"{it.get('title','')} {it.get('summary','')}"
            matched = _matches_keyword(haystack)
            if not matched:
                continue
            items_matched += 1
            title = it.get("title", "")[:200]
            if _was_recently_emitted(title):
                continue
            sectors = _affected_sectors(matched)
            try:
                emit_event(
                    source="prime_directive",
                    event_type="macro",
                    symbol=None,
                    payload={
                        "feed": feed_name,
                        "headline": title,
                        "link": it.get("link", ""),
                        "summary": it.get("summary", "")[:600],
                        "matched_keywords": matched,
                        "affected_sectors": sectors,
                    },
                )
                emitted_count += 1
                console.log(
                    f"[cyan][PRIME-DIRECTIVE] headline="
                    f"{title[:80]!r} sectors={sectors}"
                )
            except Exception as e:
                errors += 1
                console.log(
                    f"[red][PRIME-DIRECTIVE-ERROR] emit: "
                    f"{type(e).__name__}: {e!r}"
                )

    return {
        "ts": datetime.utcnow().isoformat() + "Z",
        "feeds": len(_FEEDS),
        "items_total": items_total,
        "items_matched": items_matched,
        "emitted_count": emitted_count,
        "errors": errors,
    }


if __name__ == "__main__":
    import json as _json
    print(_json.dumps(scan_prime_directive(), indent=2))
