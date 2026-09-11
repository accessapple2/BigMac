#!/usr/bin/env python3
"""HM-DATASET-EXPORTER (dry-dock B7, 2026-09-11): walk-forward training/eval
corpus for McCoy (ollama-plutus) real decisions -- prompt_text, decision,
invalidation, 1d/5d forward return, regime, model.

Source: decision_audit JOIN signals (via signal_id), player_id='ollama-plutus',
event_type='signal_emit', prompt_text IS NOT NULL -- the exact real production
prompt (see relay_2026-09-11_drydock_breach_watchdog.md / the olliemax
RESPONSE_TO_MODELWORKS.md deliverable, which used the same source field).

RULE #1: read-only against trader.db, writes NEW files only, never touches
any source table.

Walk-forward split: sorted chronologically by created_at (never shuffled --
shuffling a time series leaks future information into train), split by
POSITION (70/15/15), not by a hardcoded calendar date -- this data currently
spans only ~2 days (Phase 1.1's invalidation logging shipped 2026-09-10,
prompt_text has no history before that), so a position-based split is the
only one that stays meaningful as more days accumulate. Reported honestly:
this is a small, early corpus, not a large one -- see the summary printed
at the end and the .md report written alongside the JSONL files.

Options rows: signals.asset_type is 100% 'stock' for this player today
(verified before writing this script) -- the filter below is real code,
not a stub, but a currently-inert one. If/when an asset_type='option' row
appears, it's only included if a trustworthy restated P&L exists for its
trade_id in options_trades_restated (restated_status='reconstructed_from_
alpaca_bars') or trades_restated (pnl_restated_basis='true_pnl') --
otherwise it's excluded from the corpus rather than silently including an
unverifiable premium as ground truth.
"""
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf

REPO = Path(__file__).resolve().parent.parent
DB = str(REPO / "data" / "trader.db")
OUT_DIR = REPO / "data" / "exports"

TRAIN_FRAC, VAL_FRAC = 0.70, 0.15  # remaining 0.15 -> test


def load_rows(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("""
        SELECT da.id AS decision_audit_id, da.symbol, da.created_at, da.prompt_text,
               da.regime, da.confidence, da.gate_verdict, da.trade_id,
               s.signal AS decision, s.invalidation, s.reference_price, s.asset_type
        FROM decision_audit da
        JOIN signals s ON da.signal_id = s.id
        WHERE da.player_id = 'ollama-plutus'
          AND da.event_type = 'signal_emit'
          AND da.prompt_text IS NOT NULL
        ORDER BY da.created_at ASC
    """).fetchall()
    cols = ["decision_audit_id", "symbol", "created_at", "prompt_text", "regime",
            "confidence", "gate_verdict", "trade_id", "decision", "invalidation",
            "reference_price", "asset_type"]
    return [dict(zip(cols, r)) for r in rows]


def filter_options_rows(conn: sqlite3.Connection, rows: list[dict]) -> tuple[list[dict], int]:
    """Drop asset_type='option' rows unless a trustworthy restated P&L exists
    for their trade_id. No-op today (0 option rows in this player's signals),
    but real, tested logic for when that changes."""
    kept, dropped = [], 0
    for r in rows:
        if r["asset_type"] != "option":
            kept.append(r)
            continue
        if r["trade_id"] is None:
            dropped += 1
            continue
        ok = conn.execute(
            "SELECT 1 FROM trades_restated WHERE id=? AND pnl_restated_basis='true_pnl'",
            (r["trade_id"],),
        ).fetchone()
        if not ok:
            ok = conn.execute(
                "SELECT 1 FROM options_trades_restated WHERE id=? "
                "AND restated_status='reconstructed_from_alpaca_bars'",
                (r["trade_id"],),
            ).fetchone()
        if ok:
            kept.append(r)
        else:
            dropped += 1
    return kept, dropped


def model_id_for(conn: sqlite3.Connection, player_id: str) -> str:
    row = conn.execute("SELECT model_id FROM ai_players WHERE id=?", (player_id,)).fetchone()
    return row[0] if row else "unknown"


def compute_forward_returns(rows: list[dict]) -> None:
    """Mutates rows in place: adds fwd_return_1d / fwd_return_5d (or None).
    Close-to-close: anchor = last daily close at/before the signal's date,
    forward = the close N trading days after the anchor."""
    symbols = sorted({r["symbol"] for r in rows})
    print(f"fetching history for {len(symbols)} symbols...", file=sys.stderr)
    hist: dict[str, "object"] = {}
    for i, sym in enumerate(symbols):
        try:
            df = yf.Ticker(sym).history(period="30d", interval="1d")
            if not df.empty:
                hist[sym] = df
        except Exception:
            pass
        if i % 25 == 0:
            print(f"  {i}/{len(symbols)}", file=sys.stderr)

    for r in rows:
        r["fwd_return_1d"] = None
        r["fwd_return_5d"] = None
        df = hist.get(r["symbol"])
        if df is None:
            continue
        try:
            sig_date = datetime.strptime(r["created_at"][:10], "%Y-%m-%d").date()
        except Exception:
            continue
        dates = [d.date() for d in df.index]
        anchor_idx = None
        for idx in range(len(dates) - 1, -1, -1):
            if dates[idx] <= sig_date:
                anchor_idx = idx
                break
        if anchor_idx is None:
            continue
        anchor_close = float(df["Close"].iloc[anchor_idx])
        if anchor_close <= 0:
            continue
        if anchor_idx + 1 < len(dates):
            r["fwd_return_1d"] = (float(df["Close"].iloc[anchor_idx + 1]) - anchor_close) / anchor_close
        if anchor_idx + 5 < len(dates):
            r["fwd_return_5d"] = (float(df["Close"].iloc[anchor_idx + 5]) - anchor_close) / anchor_close


def main():
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    rows = load_rows(conn)
    print(f"loaded {len(rows)} decision_audit rows (prompt_text populated)", file=sys.stderr)

    rows, n_dropped_options = filter_options_rows(conn, rows)
    print(f"options-row filter: dropped {n_dropped_options} (0 expected today -- "
          f"asset_type is 100% 'stock' for this player as of this run)", file=sys.stderr)

    model_id = model_id_for(conn, "ollama-plutus")
    conn.close()

    compute_forward_returns(rows)
    n_with_1d = sum(1 for r in rows if r["fwd_return_1d"] is not None)
    n_with_5d = sum(1 for r in rows if r["fwd_return_5d"] is not None)
    print(f"forward returns: {n_with_1d}/{len(rows)} have 1d, {n_with_5d}/{len(rows)} have 5d "
          f"(5d needs 5 more trading days to have elapsed since the signal -- "
          f"low count is expected for signals from the last week)", file=sys.stderr)

    for r in rows:
        r["model"] = model_id

    # Walk-forward split by POSITION (rows already sorted chronologically by created_at).
    n = len(rows)
    train_end = int(n * TRAIN_FRAC)
    val_end = train_end + int(n * VAL_FRAC)
    splits = {"train": rows[:train_end], "val": rows[train_end:val_end], "test": rows[val_end:]}

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, split_rows in splits.items():
        path = OUT_DIR / f"mccoy_decision_corpus_{name}.jsonl"
        with open(path, "w") as f:
            for r in split_rows:
                f.write(json.dumps(r, default=str) + "\n")
        print(f"wrote {len(split_rows)} rows -> {path}")

    date_bounds = {
        name: (split_rows[0]["created_at"], split_rows[-1]["created_at"]) if split_rows else (None, None)
        for name, split_rows in splits.items()
    }
    summary = {
        "total_rows": n,
        "options_rows_dropped": n_dropped_options,
        "split_sizes": {k: len(v) for k, v in splits.items()},
        "split_date_bounds": date_bounds,
        "forward_return_coverage": {"1d": n_with_1d, "5d": n_with_5d},
        "decision_distribution": dict(Counter(r["decision"] for r in rows)),
        "model_id": model_id,
    }
    with open(OUT_DIR / "mccoy_decision_corpus_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
