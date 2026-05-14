#!/usr/bin/env python3
"""Targeted backfill of 13 mega-cap symbols whose market_cap was NULL after
the 2026-05-10 universe_refresh failure.

Replaces sentinel values (99999999999) committed in 36cb5c6 with real
Polygon/yfinance values. One-shot script.

Run:
    cd ~/autonomous-trader
    venv/bin/python3 scripts/backfill_13_megacaps_2026-05-14.py
"""
from __future__ import annotations
import os
import sys
import time
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from engine.universe_refresh import (
    _polygon_key,
    _fetch_market_cap_yfinance,
    _fetch_ticker_details_polygon,
    _check_options_eligible,
)

TARGETS = ["AAPL", "MSFT", "GOOGL", "AVGO", "JPM", "CRM", "PEP",
           "AMGN", "DHR", "MRVL", "HOOD", "CDE", "UPRO"]
ETF_TARGETS = {"UPRO"}
MIN_MARKET_CAP = 5_000_000_000
DB_PATH = "data/trader.db"

LOG_PATH      = "/tmp/backfill_run_2026-05-14.log"
PRE_SNAPSHOT  = "/tmp/backfill_pre_snapshot_2026-05-14.txt"
POST_SNAPSHOT = "/tmp/backfill_post_snapshot_2026-05-14.txt"
REPORT_PATH   = "data/scotty_backfill_13_megacaps_2026-05-14.md"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH, mode="w"),
              logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("backfill")


def snapshot_rows(path: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    placeholders = ",".join("?" * len(TARGETS))
    cur.execute(
        f"SELECT symbol, market_cap, options_eligible, ticker_type, last_updated "
        f"FROM scan_universe WHERE symbol IN ({placeholders}) ORDER BY symbol",
        TARGETS,
    )
    rows = cur.fetchall()
    conn.close()
    with open(path, "w") as f:
        f.write(f"# Snapshot {datetime.now().isoformat(timespec='seconds')}\n\n")
        f.write(f"{'symbol':<8} {'market_cap':>18} {'opt':>4} {'type':>6}  {'last_updated':<26}\n")
        f.write("-" * 70 + "\n")
        for sym, mc, opt, tt, lu in rows:
            mc_str = f"{mc:,.0f}" if mc is not None else "NULL"
            f.write(f"{sym:<8} {mc_str:>18} {opt or 0:>4} {tt or '?':>6}  {lu or '?':<26}\n")
    return {r[0]: r for r in rows}


def lookup_symbol(symbol: str, polygon_key: str) -> dict:
    out = {"market_cap": None, "ticker_type": None, "options_eligible": None,
           "source": "none", "errors": []}
    if polygon_key:
        try:
            mc, tt = _fetch_ticker_details_polygon(polygon_key, symbol)
            time.sleep(0.25)  # 5cps throttle
            if mc is not None:
                out["market_cap"] = mc
                out["ticker_type"] = tt
                out["source"] = "polygon"
                try:
                    out["options_eligible"] = _check_options_eligible(polygon_key, symbol)
                    time.sleep(0.25)
                except Exception as e:
                    out["errors"].append(f"options check raised: {e}")
                    out["options_eligible"] = False
                return out
            else:
                out["errors"].append("polygon returned None")
        except Exception as e:
            out["errors"].append(f"polygon raised: {e}")
    else:
        out["errors"].append("no polygon key in env/config")

    try:
        mc = _fetch_market_cap_yfinance(symbol)
        if mc is not None:
            out["market_cap"] = mc
            out["source"] = "yfinance"
            return out
        else:
            out["errors"].append("yfinance returned None")
    except Exception as e:
        out["errors"].append(f"yfinance raised: {e}")

    return out


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("Targeted backfill of 13 mega-caps")
    log.info("=" * 60)

    pre_map = snapshot_rows(PRE_SNAPSHOT)
    log.info("Pre-snapshot saved (%d rows) → %s", len(pre_map), PRE_SNAPSHOT)

    polygon_key = _polygon_key()
    log.info("Polygon key: %s", "present" if polygon_key else "MISSING (yfinance only)")

    writes, skipped_lookup, skipped_validation = [], [], []

    for sym in TARGETS:
        log.info("-" * 50)
        result = lookup_symbol(sym, polygon_key)
        mc = result["market_cap"]
        src = result["source"]
        errs = "; ".join(result["errors"]) or "n/a"

        if mc is None:
            log.error("[BACKFILL] %s: market_cap=NULL source=polygon,yfinance → SKIPPED (reason: %s)",
                      sym, errs)
            skipped_lookup.append((sym, errs))
            continue

        if sym in ETF_TARGETS:
            tt = "ETF"
            tt_note = " (forced ETF per spec)"
        else:
            tt = result.get("ticker_type") or "CS"
            tt_note = ""

        if tt == "CS" and mc < MIN_MARKET_CAP:
            log.warning("[BACKFILL] %s: market_cap=%.0f source=%s → SKIPPED "
                        "(reason: below $5B threshold, unexpected)", sym, mc, src)
            skipped_validation.append((sym, mc, src))
            continue

        if result.get("options_eligible") is not None:
            opt = 1 if result["options_eligible"] else 0
        else:
            pre = pre_map.get(sym)
            opt = (pre[2] if pre and pre[2] is not None else 0) or 0
            if sym in ETF_TARGETS:
                opt = 1  # UPRO has options chain

        log.info("[BACKFILL] %s: market_cap=%.0f source=%s type=%s%s opt=%d → UPDATED",
                 sym, mc, src, tt, tt_note, opt)
        writes.append({"symbol": sym, "market_cap": mc, "options_eligible": opt,
                       "ticker_type": tt, "source": src})

    log.info("-" * 50)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        for w in writes:
            cur.execute(
                "UPDATE scan_universe SET market_cap=?, options_eligible=?, ticker_type=?, last_updated=? WHERE symbol=?",
                (w["market_cap"], w["options_eligible"], w["ticker_type"], now_iso, w["symbol"]),
            )
        conn.commit()
        log.info("Transaction committed: %d writes applied", len(writes))
    except Exception as e:
        log.exception("Transaction failed — rolling back: %s", e)
        conn.rollback()
        conn.close()
        sys.exit(2)
    finally:
        conn.close()

    post_map = snapshot_rows(POST_SNAPSHOT)
    log.info("Post-snapshot saved → %s", POST_SNAPSHOT)

    try:
        from engine.universe import get_active_universe
        wl = set(get_active_universe(force_refresh=True))
    except Exception as e:
        log.exception("watchlist verification raised: %s", e)
        wl = set()
    in_wl = [s for s in TARGETS if s in wl]
    not_wl = [s for s in TARGETS if s not in wl]
    log.info("Watchlist contains %d/13 targets", len(in_wl))
    if not_wl:
        log.warning("Targets NOT in watchlist: %s", not_wl)

    elapsed = time.time() - start
    write_map = {w["symbol"]: w for w in writes}
    skip_lookup_map = {s[0]: s[1] for s in skipped_lookup}
    skip_val_map = {s[0]: (s[1], s[2]) for s in skipped_validation}

    lines = [
        "# Scotty: Targeted Backfill — 13 Mega-Caps — 2026-05-14",
        "",
        "Replaces sentinel values from commit 36cb5c6 with real Polygon/yfinance data.",
        f"Run elapsed: **{elapsed:.1f}s**.",
        "",
        "## Summary",
        "",
        f"- Total targets: {len(TARGETS)}",
        f"- Updated:       {len(writes)}",
        f"- Skipped (lookup failure):     {len(skipped_lookup)}",
        f"- Skipped (validation failure): {len(skipped_validation)}",
        f"- In watchlist after run:       {len(in_wl)}/13",
        "",
        "## Diff — sentinel/old → real",
        "",
        "| symbol | pre market_cap | post market_cap | source | type | opt | status |",
        "|---|---|---|---|---|---|---|",
    ]
    for sym in TARGETS:
        pre = pre_map.get(sym)
        post = post_map.get(sym)
        pre_mc  = f"{pre[1]:,.0f}"  if pre  and pre[1]  is not None else "NULL"
        post_mc = f"{post[1]:,.0f}" if post and post[1] is not None else "NULL"
        post_tt = post[3] if post else "?"
        post_opt = post[2] if post else "?"
        if sym in write_map:
            src = write_map[sym]["source"]
            status = "UPDATED"
        elif sym in skip_lookup_map:
            src = "—"
            status = f"SKIPPED (lookup: {skip_lookup_map[sym]})"
        elif sym in skip_val_map:
            mc, s = skip_val_map[sym]
            src = s
            status = f"SKIPPED (validation: ${mc:,.0f} < $5B)"
        else:
            src, status = "—", "?"
        lines.append(f"| {sym} | {pre_mc} | {post_mc} | {src} | {post_tt} | {post_opt} | {status} |")

    lines.extend([
        "",
        "## Watchlist verification",
        "",
        f"- `get_active_universe(force_refresh=True)` size: {len(wl)}",
        f"- IN watchlist ({len(in_wl)}/13): {', '.join(in_wl) if in_wl else '—'}",
    ])
    if not_wl:
        lines.append(f"- **NOT in watchlist ({len(not_wl)}/13)**: {', '.join(not_wl)} — investigate")
    lines.extend([
        "",
        "## Per-symbol [BACKFILL] log lines",
        "",
        "```",
    ])
    try:
        with open(LOG_PATH) as f:
            lines.extend(line.rstrip() for line in f if "[BACKFILL]" in line)
    except Exception:
        lines.append("(log file unavailable)")
    lines.append("```")
    lines.append("")

    if skipped_lookup or skipped_validation:
        lines.extend(["## Action items", ""])
        if skipped_lookup:
            lines.append("**Lookup failures** (sentinel value preserved — re-attempt later):")
            for sym, errs in skipped_lookup:
                lines.append(f"  - **{sym}**: {errs}")
        if skipped_validation:
            lines.append("**Validation failures** (sentinel preserved — investigate symbol):")
            for sym, mc, s in skipped_validation:
                lines.append(f"  - **{sym}**: ${mc:,.0f} from {s} (below $5B mega-cap threshold)")
        lines.append("")

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))
    log.info("Report written to %s", REPORT_PATH)

    log.info("=" * 60)
    log.info("DONE in %.1fs — updated=%d skipped_lookup=%d skipped_validation=%d",
             elapsed, len(writes), len(skipped_lookup), len(skipped_validation))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
