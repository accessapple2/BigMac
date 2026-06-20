#!/usr/bin/env python3
"""
UHURA Step 4 — Backtest + Report
For each gate_pass signal, fetch Alpaca minute bars and compute T+5/15/30/60/240 returns.
Generates go/no-go report to stdout and data/uhura_report.md.

Hypotheses tested:
  H1: Earnings BULLISH/BEARISH → momentum edge within 15min (t15_ret > 0 for BULLISH)
  H2: Merger news → edge holds at T+60 (t60_ret > 0 for BULLISH)
  H3: Macro news → excess return vs SPY benchmark < 0.5% (noise hypothesis)
  H4: Urgency>=4 AND confidence>=0.80 → tighter T+15 win rate vs all-news baseline
"""
import os
import time
import sqlite3
import requests
import statistics
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from db import get_conn  # noqa: E402

APCA_KEY = os.environ["APCA_API_KEY_ID"]
APCA_SECRET = os.environ["APCA_API_SECRET_KEY"]
HEADERS = {"APCA-API-KEY-ID": APCA_KEY, "APCA-API-SECRET-KEY": APCA_SECRET}
BARS_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"

MOVE_THRESHOLD = 0.03       # 3% move for "meaningful" signal
WINDOW_MINUTES = 250        # fetch enough bars for T+240 + buffer
REPORT_PATH = Path(__file__).resolve().parents[2] / "data" / "uhura_report.md"


def fetch_bars(symbol: str, start: str, n_bars: int = WINDOW_MINUTES) -> list[dict]:
    """Fetch 1-min bars starting at `start` UTC ISO string."""
    # Convert to RFC3339
    dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = dt + timedelta(minutes=n_bars + 30)
    params = {
        "timeframe": "1Min",
        "start": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": n_bars,
        "adjustment": "raw",
        "feed": "iex",
    }
    try:
        r = requests.get(
            BARS_URL.format(symbol=symbol), headers=HEADERS, params=params, timeout=15
        )
        r.raise_for_status()
        return r.json().get("bars", [])
    except requests.RequestException as e:
        print(f"    bars error {symbol}: {e}")
        return []


def compute_ret(bars: list[dict], offset_min: int, open_price: float) -> float | None:
    """Return relative to open_price at bar index ~offset_min."""
    if len(bars) <= offset_min:
        return None
    bar = bars[min(offset_min, len(bars) - 1)]
    close = bar.get("c") or bar.get("close")
    if not close or open_price == 0:
        return None
    return (close - open_price) / open_price


def main() -> None:
    conn = get_conn()

    rows = conn.execute(
        """SELECT s.id, s.ticker, s.published_at, s.sentiment,
                  s.confidence, s.event_type, s.urgency
           FROM uhura_signals s
           LEFT JOIN uhura_backtest_results b ON b.signal_id = s.id
           WHERE s.gate_pass = 1 AND b.id IS NULL
           ORDER BY s.published_at ASC
           LIMIT 2000""",
    ).fetchall()

    total = len(rows)
    print(f"Signals to backtest: {total}")
    if not total:
        print("Nothing to backtest. Run gate.py first, or all signals already processed.")
        conn.close()
        return

    ok = 0
    skip = 0
    for i, row in enumerate(rows):
        sig_id, ticker, published_at, sentiment, confidence, event_type, urgency = (
            row["id"], row["ticker"], row["published_at"], row["sentiment"],
            row["confidence"], row["event_type"], row["urgency"],
        )

        # Fetch bars for ticker and SPY
        bars = fetch_bars(ticker, published_at)
        spy_bars = fetch_bars("SPY", published_at)
        time.sleep(0.12)

        if not bars or not bars[0].get("o"):
            skip += 1
            continue

        open_price = bars[0]["o"]
        spy_open = spy_bars[0]["o"] if spy_bars and spy_bars[0].get("o") else None

        def spy_ret(offset: int) -> float | None:
            return compute_ret(spy_bars, offset, spy_open) if spy_open else None

        t5 = compute_ret(bars, 5, open_price)
        t15 = compute_ret(bars, 15, open_price)
        t30 = compute_ret(bars, 30, open_price)
        t60 = compute_ret(bars, 60, open_price)
        t240 = compute_ret(bars, 240, open_price)

        move_hit = 1 if t30 is not None and abs(t30) >= MOVE_THRESHOLD else 0

        try:
            conn.execute(
                """INSERT OR IGNORE INTO uhura_backtest_results
                   (signal_id, ticker, event_type, sentiment, urgency, confidence,
                    bar_open, t5_ret, t15_ret, t30_ret, t60_ret, t240_ret,
                    spy_t5, spy_t15, spy_t30, spy_t60, spy_t240, move_threshold_hit)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    sig_id, ticker, event_type, sentiment, urgency, confidence,
                    open_price, t5, t15, t30, t60, t240,
                    spy_ret(5), spy_ret(15), spy_ret(30), spy_ret(60), spy_ret(240),
                    move_hit,
                ),
            )
            ok += 1
        except sqlite3.Error as e:
            print(f"    DB err signal {sig_id}: {e}")
            skip += 1

        if (i + 1) % 100 == 0:
            conn.commit()
            print(f"  {i + 1}/{total} done ({ok} ok, {skip} skip)")

    conn.commit()
    print(f"\nBacktest complete: {ok} ok, {skip} skip")

    # --- Generate report ---
    _generate_report(conn)
    conn.close()


def _mean(vals: list) -> float | None:
    clean = [v for v in vals if v is not None]
    return statistics.mean(clean) if len(clean) >= 5 else None


def _winrate(rets: list, direction: str) -> float | None:
    clean = [v for v in rets if v is not None]
    if len(clean) < 5:
        return None
    wins = sum(1 for v in clean if (v > 0 if direction == "BULLISH" else v < 0))
    return wins / len(clean)


def _generate_report(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """SELECT event_type, sentiment, urgency, confidence,
                  t5_ret, t15_ret, t30_ret, t60_ret, t240_ret,
                  spy_t5, spy_t15, spy_t30, spy_t60, spy_t240,
                  move_threshold_hit
           FROM uhura_backtest_results"""
    ).fetchall()

    if not rows:
        print("No backtest results to report yet.")
        return

    total_n = len(rows)
    lines = [
        "# UHURA News-Event Drift Study — Backtest Report",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Total signals backtested: {total_n}",
        "",
        "---",
        "",
        "## H1 — Earnings momentum (T+15)",
        "",
    ]

    def bucket(rows_all, event=None, sentiment=None, min_urgency=None, min_conf=None):
        out = []
        for r in rows_all:
            if event and r["event_type"] != event:
                continue
            if sentiment and r["sentiment"] != sentiment:
                continue
            if min_urgency and (r["urgency"] or 0) < min_urgency:
                continue
            if min_conf and (r["confidence"] or 0) < min_conf:
                continue
            out.append(r)
        return out

    def fmt_stats(label: str, bucket_rows, horizon: str = "t15_ret",
                  direction: str = "BULLISH") -> str:
        rets = [r[horizon] for r in bucket_rows]
        spy_col = "spy_" + horizon.replace("_ret", "")
        spy = [r[spy_col] for r in bucket_rows]
        exc = [
            (r - s) for r, s in zip(rets, spy)
            if r is not None and s is not None
        ]
        n = len([v for v in rets if v is not None])
        mean_r = _mean(rets)
        mean_exc = _mean(exc) if exc else None
        wr = _winrate(rets, direction)
        move_hit = sum(1 for r in bucket_rows if r["move_threshold_hit"]) / max(len(bucket_rows), 1)

        parts = [f"**{label}** (n={n})"]
        if mean_r is not None:
            parts.append(f"mean {mean_r*100:+.2f}%")
        if mean_exc is not None:
            parts.append(f"excess vs SPY {mean_exc*100:+.2f}%")
        if wr is not None:
            parts.append(f"WR {wr*100:.1f}%")
        parts.append(f"move≥3% {move_hit*100:.1f}%")
        return " | ".join(parts)

    # H1: Earnings
    for sent in ("BULLISH", "BEARISH"):
        b = bucket(rows, event="earnings", sentiment=sent)
        lines.append(fmt_stats(f"Earnings {sent} → T+15", b, "t15_ret", sent))
    lines.append(fmt_stats("Earnings ALL → T+15", bucket(rows, event="earnings"), "t15_ret", "BULLISH"))
    lines += ["", "## H2 — Merger edge at T+60", ""]
    for sent in ("BULLISH", "BEARISH"):
        b = bucket(rows, event="merger", sentiment=sent)
        lines.append(fmt_stats(f"Merger {sent} → T+60", b, "t60_ret", sent))

    lines += ["", "## H3 — Macro noise vs SPY (T+30)", ""]
    macro_b = bucket(rows, event="macro")
    macro_bull = bucket(rows, event="macro", sentiment="BULLISH")
    macro_bear = bucket(rows, event="macro", sentiment="BEARISH")
    lines.append(fmt_stats("Macro ALL → T+30", macro_b, "t30_ret", "BULLISH"))
    lines.append(fmt_stats("Macro BULLISH → T+30", macro_bull, "t30_ret", "BULLISH"))
    lines.append(fmt_stats("Macro BEARISH → T+30", macro_bear, "t30_ret", "BEARISH"))

    lines += ["", "## H4 — High-urgency / high-confidence filter (T+15)", ""]
    all_b = list(rows)
    hq = bucket(rows, min_urgency=4, min_conf=0.80)
    lines.append(fmt_stats("All signals baseline → T+15", all_b, "t15_ret", "BULLISH"))
    lines.append(fmt_stats("Urgency≥4 & Conf≥0.80 → T+15", hq, "t15_ret", "BULLISH"))

    lines += ["", "## Event type distribution", ""]
    from collections import Counter
    ec = Counter(r["event_type"] for r in rows)
    sc = Counter(r["sentiment"] for r in rows)
    for et, cnt in ec.most_common():
        lines.append(f"- {et}: {cnt}")
    lines.append("")
    for sent, cnt in sc.most_common():
        lines.append(f"- {sent}: {cnt}")

    # Go/no-go verdict
    lines += ["", "---", "## Verdict", ""]
    h1_wr = _winrate([r["t15_ret"] for r in bucket(rows, event="earnings", sentiment="BULLISH")], "BULLISH")
    h4_wr = _winrate([r["t15_ret"] for r in hq], "BULLISH")
    h4_base = _winrate([r["t15_ret"] for r in all_b], "BULLISH")

    verdicts = []
    if h1_wr and h1_wr >= 0.58:
        verdicts.append("H1 PASS — earnings momentum WR ≥ 58%")
    elif h1_wr:
        verdicts.append(f"H1 FAIL — earnings WR {h1_wr*100:.1f}% < 58% threshold")
    else:
        verdicts.append("H1 INSUFFICIENT DATA")

    if h4_wr and h4_base and h4_wr >= h4_base + 0.05:
        verdicts.append(f"H4 PASS — high-quality filter lifts WR by {(h4_wr-h4_base)*100:.1f}pp")
    elif h4_wr and h4_base:
        verdicts.append(f"H4 FAIL — high-quality filter WR {h4_wr*100:.1f}% vs baseline {h4_base*100:.1f}%")
    else:
        verdicts.append("H4 INSUFFICIENT DATA")

    go = sum(1 for v in verdicts if "PASS" in v)
    lines.append(f"**GO signal:** {go}/4 hypotheses passed")
    for v in verdicts:
        lines.append(f"- {v}")

    report = "\n".join(lines)
    print("\n" + "=" * 60)
    print(report)
    print("=" * 60)
    REPORT_PATH.write_text(report)
    print(f"\nReport written: {REPORT_PATH}")


if __name__ == "__main__":
    main()
