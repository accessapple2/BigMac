"""
TradeMinds — Fast Scanner
==========================
Lightweight intraday scanner using qwen3:14b via Ollama.
Scans each ticker in ~30 seconds. Checks UOA alerts for unusual options
activity. Sends macOS desktop notifications with sound cues.

Signals
-------
  BUY   → Blow   sound
  SELL  → Basso  sound
  WATCH → Pop    sound

Usage
-----
  # One-shot scan
  python3 -m engine.fast_scanner --tickers MU,AAPL,NVDA

  # Continuous daemon (every 15 min, 6:30 AM–1:00 PM AZ time, Mon–Fri)
  python3 -m engine.fast_scanner --daemon

  # Override tickers in daemon mode
  python3 -m engine.fast_scanner --daemon --tickers SPY,QQQ,NVDA,TSLA
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OLLAMA_BASE    = "http://localhost:11434"
MODEL          = "qwen3:14b"

# Two DB paths: main trader data + UOA (separate DB)
DATA_DB  = "data/trader.db"
UOA_DB   = "trader.db"           # UOA alerts live in the root-level DB

OLLAMA_OPTIONS = {
    "temperature":   0.3,
    "num_predict":   512,
    "think":         False,       # disable chain-of-thought for speed
}

# Arizona time = UTC-7 (no DST)
AZ_UTC_OFFSET  = -7
MARKET_OPEN_H  = 6               # 6:30 AM AZ = 9:30 AM ET
MARKET_OPEN_M  = 30
MARKET_CLOSE_H = 13              # 1:00 PM AZ = 4:00 PM ET
DAEMON_INTERVAL_MINUTES = 15

# macOS notification sounds per signal
SOUNDS = {
    "BUY":   "Blow",
    "SELL":  "Basso",
    "WATCH": "Pop",
    "HOLD":  None,               # no notification for HOLD
}

DEFAULT_TICKERS = [
    "SPY", "QQQ", "NVDA", "AAPL", "MSFT", "TSLA",
    "AMD", "META", "AMZN", "GOOGL", "ORCL", "MU",
    "NOW", "AVGO", "PLTR", "DELL",
]

logger = logging.getLogger("fast_scanner")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create fast_scan_results table if it doesn't exist."""
    conn = _conn(DATA_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fast_scan_results (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT    NOT NULL,
            signal        TEXT    NOT NULL,   -- BUY | SELL | WATCH | HOLD
            confidence    INTEGER,            -- 1-10
            price         REAL,
            thesis        TEXT,
            key_risk      TEXT,
            uoa_summary   TEXT,               -- UOA alert snippet if present
            model         TEXT    DEFAULT 'qwen3:14b',
            scan_duration_s REAL,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    logger.info("DB ready")


# ---------------------------------------------------------------------------
# UOA context
# ---------------------------------------------------------------------------

def _get_uoa_context(ticker: str) -> str:
    """Pull recent UOA alerts for ticker from the UOA database."""
    try:
        conn = _conn(UOA_DB)
        rows = conn.execute("""
            SELECT severity, alert_type, description, created_at
            FROM uoa_alerts
            WHERE ticker = ?
              AND created_at >= datetime('now', '-48 hours')
            ORDER BY created_at DESC
            LIMIT 5
        """, (ticker,)).fetchall()
        conn.close()
        if not rows:
            return ""
        lines = [f"  [{r['severity']}] {r['alert_type']}: {r['description']}" for r in rows]
        return "Recent UOA Alerts (last 48h):\n" + "\n".join(lines)
    except Exception as e:
        logger.debug(f"UOA fetch skipped for {ticker}: {e}")
        return ""


# ---------------------------------------------------------------------------
# Price context
# ---------------------------------------------------------------------------

def _get_price_context(ticker: str) -> dict:
    """Fetch price + technicals. Falls back to DB cache."""
    ctx: dict = {"ticker": ticker, "price": None, "change_pct": None}

    try:
        from engine.market_data import get_stock_price
        data = get_stock_price(ticker)
        if data and data.get("price"):
            ctx.update({
                "price":      data.get("price"),
                "change_pct": data.get("change_pct"),
                "volume":     data.get("volume"),
                "high":       data.get("high"),
                "low":        data.get("low"),
            })
            return ctx
    except Exception:
        pass

    # Fallback: latest row from universe_scan
    try:
        conn = _conn(DATA_DB)
        row = conn.execute("""
            SELECT close AS price, rsi, volume_ratio, gap_pct, signals
            FROM universe_scan
            WHERE ticker = ?
            ORDER BY created_at DESC LIMIT 1
        """, (ticker,)).fetchone()
        conn.close()
        if row:
            ctx.update({
                "price":        row["price"],
                "rsi":          row["rsi"],
                "volume_ratio": row["volume_ratio"],
                "gap_pct":      row["gap_pct"],
                "cached_signals": json.loads(row["signals"] or "[]"),
            })
    except Exception:
        pass

    return ctx


# ---------------------------------------------------------------------------
# Debate context
# ---------------------------------------------------------------------------

def _get_debate_context(ticker: str) -> str:
    """Fetch most recent debate summary for ticker."""
    try:
        conn = _conn(DATA_DB)
        row = conn.execute("""
            SELECT picard_decision, adjusted_conviction, picard_synthesis,
                   risk_rating, created_at
            FROM debate_history_v2
            WHERE ticker = ?
            ORDER BY id DESC LIMIT 1
        """, (ticker,)).fetchone()
        conn.close()
        if row:
            return (
                f"Last debate: {row['picard_decision']} "
                f"(conviction {row['adjusted_conviction']}/10, "
                f"risk {row['risk_rating']}) — {row['picard_synthesis']}"
            )
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------

def _call_ollama(prompt: str) -> str:
    """Call Ollama synchronously via /api/chat. Returns raw text or empty string on error."""
    payload = {
        "model":   MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream":  False,
        "options": {k: v for k, v in OLLAMA_OPTIONS.items() if k != "think"},
    }
    try:
        resp = requests.post(
            f"{OLLAMA_BASE}/api/chat",
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("message", {}).get("content", "")
    except Exception as e:
        logger.error(f"Ollama error: {e}")
        return ""


def _parse_json(text: str) -> dict:
    """Extract JSON from model output, stripping think blocks and fences."""
    cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    if cleaned.startswith("```"):
        cleaned = "\n".join(l for l in cleaned.splitlines() if not l.strip().startswith("```"))
    start = cleaned.find("{")
    end   = cleaned.rfind("}") + 1
    if start != -1 and end > start:
        try:
            return json.loads(cleaned[start:end])
        except json.JSONDecodeError:
            pass
    return {}


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

PROMPT_TEMPLATE = """\
You are a fast intraday scanner analyst for {ticker}.

PRICE DATA:
{price_block}

{debate_block}

{uoa_block}

Your job: give a rapid-fire intraday signal for {ticker}.

Signal options: BUY | SELL | WATCH | HOLD

Rules:
- BUY   = clear near-term upside catalyst or momentum, act now
- SELL  = clear near-term downside risk, avoid or exit
- WATCH = interesting setup forming, monitor closely, not yet actionable
- HOLD  = nothing new, no action needed

Respond ONLY with this exact JSON (no other text):
{{"signal":"BUY|SELL|WATCH|HOLD","confidence":<1-10>,"thesis":"<1 sentence max>","key_risk":"<1 sentence max>"}}"""


def _build_prompt(ticker: str, price: dict, debate: str, uoa: str) -> str:
    price_lines = []
    for k, v in price.items():
        if k == "ticker" or v is None:
            continue
        if k == "cached_signals":
            price_lines.append(f"  signals: {', '.join(v)}" if v else "")
        else:
            price_lines.append(f"  {k}: {v}")
    price_block = "\n".join(l for l in price_lines if l)

    debate_block = f"PRIOR DEBATE CONTEXT:\n{debate}" if debate else ""
    uoa_block    = uoa if uoa else ""

    return PROMPT_TEMPLATE.format(
        ticker=ticker,
        price_block=price_block or "  (no price data available)",
        debate_block=debate_block,
        uoa_block=uoa_block,
    )


# ---------------------------------------------------------------------------
# Notification
# ---------------------------------------------------------------------------

def _notify(ticker: str, signal: str, confidence: int, thesis: str):
    """Send a macOS desktop notification via osascript."""
    sound = SOUNDS.get(signal)
    if sound is None:
        return   # HOLD — no notification

    title    = f"TradeMinds: {signal} {ticker}"
    subtitle = f"Confidence {confidence}/10"
    body     = thesis[:120] if thesis else signal

    script = (
        f'display notification "{body}" '
        f'with title "{title}" '
        f'subtitle "{subtitle}" '
        f'sound name "{sound}"'
    )
    try:
        subprocess.run(["osascript", "-e", script], check=True, capture_output=True)
        logger.info(f"Notification sent: {signal} {ticker}")
    except Exception as e:
        logger.warning(f"Notification failed: {e}")


# ---------------------------------------------------------------------------
# Core scan
# ---------------------------------------------------------------------------

def scan_ticker(ticker: str) -> dict:
    """
    Run a fast scan on a single ticker.
    Returns a result dict with signal, confidence, thesis, key_risk.
    """
    ticker = ticker.upper().strip()
    logger.info(f"Scanning {ticker}…")
    t0 = time.time()

    price   = _get_price_context(ticker)
    debate  = _get_debate_context(ticker)
    uoa     = _get_uoa_context(ticker)
    prompt  = _build_prompt(ticker, price, debate, uoa)

    raw     = _call_ollama(prompt)
    parsed  = _parse_json(raw)

    elapsed = round(time.time() - t0, 1)

    signal     = parsed.get("signal",     "HOLD").upper()
    confidence = parsed.get("confidence", 5)
    thesis     = parsed.get("thesis",     raw[:200] if raw else "No response")
    key_risk   = parsed.get("key_risk",   "")

    # Validate signal value
    if signal not in ("BUY", "SELL", "WATCH", "HOLD"):
        signal = "WATCH"

    # UOA one-liner for storage
    uoa_summary = uoa.splitlines()[1] if uoa and "\n" in uoa else uoa[:120] if uoa else ""

    result = {
        "ticker":           ticker,
        "signal":           signal,
        "confidence":       confidence,
        "price":            price.get("price"),
        "thesis":           thesis,
        "key_risk":         key_risk,
        "uoa_summary":      uoa_summary,
        "model":            MODEL,
        "scan_duration_s":  elapsed,
    }

    logger.info(
        f"{ticker}: {signal} ({confidence}/10) in {elapsed}s — {thesis[:60]}…"
    )
    return result


def _save_result(result: dict) -> int:
    """Save scan result to fast_scan_results. Returns row id."""
    conn = _conn(DATA_DB)
    cur = conn.execute("""
        INSERT INTO fast_scan_results
            (ticker, signal, confidence, price, thesis, key_risk,
             uoa_summary, model, scan_duration_s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        result["ticker"], result["signal"], result["confidence"],
        result["price"],  result["thesis"], result["key_risk"],
        result["uoa_summary"], result["model"], result["scan_duration_s"],
    ))
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


# ---------------------------------------------------------------------------
# Batch scan
# ---------------------------------------------------------------------------

def run_scan(tickers: list[str]) -> list[dict]:
    """Scan a list of tickers sequentially. Returns all results."""
    init_db()
    results = []
    for ticker in tickers:
        result = scan_ticker(ticker)
        row_id = _save_result(result)
        result["id"] = row_id
        _notify(result["ticker"], result["signal"], result["confidence"], result["thesis"])
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Market hours check (Arizona time = UTC-7, no DST)
# ---------------------------------------------------------------------------

def _az_now() -> datetime:
    """Current time in Arizona (UTC-7, fixed, no DST)."""
    from datetime import timedelta
    return datetime.now(timezone.utc) + timedelta(hours=AZ_UTC_OFFSET)


def is_market_hours() -> bool:
    """
    Returns True if current AZ time is within market scan window:
    Mon–Fri, 6:30 AM – 1:00 PM Arizona time.
    """
    now = _az_now()
    if now.weekday() >= 5:          # Saturday=5, Sunday=6
        return False
    open_minutes  = MARKET_OPEN_H  * 60 + MARKET_OPEN_M
    close_minutes = MARKET_CLOSE_H * 60
    current       = now.hour * 60 + now.minute
    return open_minutes <= current < close_minutes


def _minutes_until_open() -> int:
    """Minutes until next market open (used by daemon sleep)."""
    from datetime import timedelta
    now = _az_now()
    # Roll forward to next weekday open
    candidate = now.replace(hour=MARKET_OPEN_H, minute=MARKET_OPEN_M, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    # Skip weekends
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    delta = (candidate - now).total_seconds()
    return max(1, int(delta / 60))


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------

def run_daemon(tickers: list[str]):
    """
    Continuous daemon: scans tickers every DAEMON_INTERVAL_MINUTES during
    market hours. Sleeps until next open when outside market hours.
    """
    init_db()
    logger.info(
        f"Daemon started — {len(tickers)} tickers, "
        f"interval={DAEMON_INTERVAL_MINUTES}m, "
        f"window=06:30–13:00 AZ"
    )

    while True:
        if not is_market_hours():
            wait = _minutes_until_open()
            now_str = _az_now().strftime("%H:%M AZ")
            logger.info(f"Outside market hours ({now_str}). Sleeping {wait}m until open.")
            time.sleep(wait * 60)
            continue

        logger.info(f"=== Scan cycle at {_az_now().strftime('%H:%M AZ')} ===")
        try:
            results = []
            for ticker in tickers:
                result = scan_ticker(ticker)
                row_id = _save_result(result)
                result["id"] = row_id
                _notify(result["ticker"], result["signal"], result["confidence"], result["thesis"])
                results.append(result)

            # Summarise the cycle
            buys   = [r["ticker"] for r in results if r["signal"] == "BUY"]
            sells  = [r["ticker"] for r in results if r["signal"] == "SELL"]
            watchs = [r["ticker"] for r in results if r["signal"] == "WATCH"]
            logger.info(
                f"Cycle done — BUY:{buys or 'none'}  "
                f"SELL:{sells or 'none'}  WATCH:{watchs or 'none'}"
            )
        except Exception as e:
            logger.error(f"Scan cycle error: {e}", exc_info=True)

        logger.info(f"Sleeping {DAEMON_INTERVAL_MINUTES}m until next cycle.")
        time.sleep(DAEMON_INTERVAL_MINUTES * 60)


# ---------------------------------------------------------------------------
# CLI output
# ---------------------------------------------------------------------------

def _print_results(results: list[dict]):
    print("\n" + "=" * 64)
    print("FAST SCAN RESULTS")
    print("=" * 64)
    for r in results:
        signal_icon = {"BUY": "🟢", "SELL": "🔴", "WATCH": "🟡", "HOLD": "⚪"}.get(r["signal"], "⚪")
        print(f"\n{signal_icon}  {r['ticker']:8s}  {r['signal']:5s}  {r['confidence']}/10  "
              f"{'$'+str(r['price']) if r['price'] else '—':>10}  ({r['scan_duration_s']}s)")
        print(f"   Thesis:   {r['thesis']}")
        if r.get("key_risk"):
            print(f"   Risk:     {r['key_risk']}")
        if r.get("uoa_summary"):
            print(f"   UOA:      {r['uoa_summary'][:80]}")
    print("\n" + "=" * 64)
    buys  = [r["ticker"] for r in results if r["signal"] == "BUY"]
    sells = [r["ticker"] for r in results if r["signal"] == "SELL"]
    watch = [r["ticker"] for r in results if r["signal"] == "WATCH"]
    print(f"BUY:   {', '.join(buys)  or 'none'}")
    print(f"SELL:  {', '.join(sells) or 'none'}")
    print(f"WATCH: {', '.join(watch) or 'none'}")
    print("=" * 64 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [fast_scanner] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="TradeMinds Fast Scanner — qwen3:14b intraday signals"
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default="",
        help="Comma-separated tickers (e.g. MU,AAPL,NVDA). "
             "Defaults to built-in watchlist if omitted.",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuously every 15 minutes during market hours.",
    )
    args = parser.parse_args()

    tickers = (
        [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if args.tickers
        else DEFAULT_TICKERS
    )

    if args.daemon:
        run_daemon(tickers)
    else:
        results = run_scan(tickers)
        _print_results(results)
