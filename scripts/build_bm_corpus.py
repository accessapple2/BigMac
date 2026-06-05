#!/usr/bin/env python3
"""HM-BM-BAKEOFF Phase 1 — build the production-matching test corpus.

Option (A) composition (Admiral-approved 2026-06-05):
  - stock_long  : asset_type='stock', action='SELL', 50/50 win/loss, losers binding, seed=42
  - cover_short : action='COVER' (short/hedge proxy) — include all available
  - option      : asset_type='option' — include all available
  - Exclude known_contaminated=1. Closed = realized_pnl NOT NULL, executed_at last 30d.
  - Do NOT force 100 — natural yield.

Output: data/bm_corpus_v1.jsonl  (committed per spec). NEVER deletes — if an
output exists it is archived (.bak_<ts>) before the new one is written.

Point-in-time market context (VIX/SPY/ATR/signals) is NOT reconstructed in v1
(no reliable historical point-in-time source; reconstructing risks fabrication).
Fields are emitted as null with context_note so the gap is explicit and equal
across all candidates (fair relative bakeoff).
"""
from __future__ import annotations
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import numpy as np

REPO = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
DB = REPO / "data" / "trader.db"
OUT = REPO / "data" / "bm_corpus_v1.jsonl"

SEED = 42
WINDOW = "executed_at >= date('now','-30 day')"
CLEAN = f"realized_pnl IS NOT NULL AND {WINDOW} AND COALESCE(known_contaminated,0)=0"

FIELDS = ("id, symbol, entry_price, exit_price, qty, realized_pnl, player_id, "
          "asset_type, option_type, strike_price, expiry_date, executed_at, "
          "reasoning, sources, confidence, strategy_id, signal_id")


def rows_for(conn, where):
    cur = conn.execute(f"SELECT {FIELDS} FROM trades WHERE {CLEAN} AND {where} ORDER BY id")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def to_corpus_row(r, stratum):
    return {
        "trade_id": r["id"],
        "stratum": stratum,
        "symbol": r["symbol"],
        "entry_price": r["entry_price"],
        "exit_price": r["exit_price"],
        "qty": r["qty"],
        "realized_pnl": r["realized_pnl"],
        "outcome": "win" if (r["realized_pnl"] or 0) > 0 else "loss" if (r["realized_pnl"] or 0) < 0 else "breakeven",
        "player_id": r["player_id"],
        "asset_type": r["asset_type"],
        "option_type": r["option_type"],
        "strike_price": r["strike_price"],
        "expiry_date": r["expiry_date"],
        "opened_at": None,                # not separately stored; single-row closed trade
        "closed_at": r["executed_at"],
        "reasoning": r["reasoning"],
        "strategy_tags": r["sources"],    # spec 'strategy_tags' -> trades.sources (strategy_id is NULL)
        "confidence": r["confidence"],
        "strategy_id": r["strategy_id"],
        "signal_id": r["signal_id"],
        # honest context gaps — not reconstructed in v1, equal across candidates
        "vix_at_entry": None,
        "spy_price_at_entry": None,
        "sector": None,
        "atr_at_entry": None,
        "recent_strategy_signals_24h": None,
        "context_note": "point-in-time market context not reconstructed in v1 corpus",
    }


def main():
    if not DB.exists():
        sys.exit(f"ERROR: {DB} not found")
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)

    stock_w = rows_for(conn, "asset_type='stock' AND action='SELL' AND realized_pnl>0")
    stock_l = rows_for(conn, "asset_type='stock' AND action='SELL' AND realized_pnl<0")
    covers  = rows_for(conn, "action='COVER'")
    options = rows_for(conn, "asset_type='option'")
    conn.close()

    np.random.seed(SEED)
    n = min(len(stock_w), len(stock_l))            # 50/50, losers binding
    w_idx = sorted(np.random.choice(len(stock_w), size=n, replace=False).tolist())
    l_idx = sorted(np.random.choice(len(stock_l), size=n, replace=False).tolist()) if len(stock_l) > n else list(range(len(stock_l)))
    sel_stock = [stock_w[i] for i in w_idx] + [stock_l[i] for i in l_idx]

    corpus = (
        [to_corpus_row(r, "stock_long") for r in sel_stock]
        + [to_corpus_row(r, "cover_short") for r in covers]
        + [to_corpus_row(r, "option") for r in options]
    )

    # NEVER DELETE: archive an existing corpus before overwrite
    if OUT.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        bak = OUT.with_name(f"{OUT.name}.bak_{ts}")
        os.rename(OUT, bak)
        print(f"[archive] existing corpus -> {bak.name}")

    tmp = OUT.with_suffix(".jsonl.tmp")
    with open(tmp, "w") as f:
        for row in corpus:
            f.write(json.dumps(row) + "\n")
    os.replace(tmp, OUT)

    # ── report ─────────────────────────────────────────────────────────────
    def wl(rows):
        w = sum(1 for r in rows if (r["realized_pnl"] or 0) > 0)
        l = sum(1 for r in rows if (r["realized_pnl"] or 0) < 0)
        return w, l
    sw, sl = wl(sel_stock); cw, cl = wl(covers); ow, ol = wl(options)
    print(f"availability(clean): stock {len(stock_w)}W/{len(stock_l)}L | covers {len(covers)} | options {len(options)}")
    print("=== CORPUS BUILT: data/bm_corpus_v1.jsonl ===")
    print(f"  stock_long : {len(sel_stock):3d}  ({sw}W / {sl}L)")
    print(f"  cover_short: {len(covers):3d}  ({cw}W / {cl}L)")
    print(f"  option     : {len(options):3d}  ({ow}W / {ol}L)")
    print(f"  TOTAL      : {len(corpus):3d}")
    print(f"  seed={SEED} | excluded known_contaminated | window=last 30d")


if __name__ == "__main__":
    main()
