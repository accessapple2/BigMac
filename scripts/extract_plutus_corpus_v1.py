#!/usr/bin/env python3
"""HM-PLUTUS-CORPUS-V1 2026-05-20 — extract training corpus for Plutus-Critic.

Format A (per task spec):
  {
    "prompt": "Trade context: symbol=X, strategy=Y, regime=Z, spy_change=A%, vix=B, reasoning=<entry reasoning>",
    "completion": "Verdict: <WIN|LOSS>, realized_pnl=<N>, critique=<what worked/failed>"
  }

Source: closed SELL trades in data/trader.db with realized_pnl != 0 and the
matching entry-side BUY's reasoning. JOIN to regime_history for regime + SPY
close-to-close change on the SELL date.

PR #45 (HM-SIGNAL-TRADE-FK) added signals.id → trades.signal_id linkage, but
zero historical rows have it populated yet. So we recover signal context via
the BUY's `reasoning` field (LLM rationale at entry, persisted on every buy).

Output: data/plutus_corpus_v1.jsonl (one JSON object per line).
"""
from __future__ import annotations

import json
import sqlite3
import statistics
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB = REPO_ROOT / "data" / "trader.db"
OUT = REPO_ROOT / "data" / "plutus_corpus_v1.jsonl"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB))
    c.row_factory = sqlite3.Row
    return c


def _grade_critique(pnl: float, pnl_pct: float | None) -> tuple[str, str]:
    """Synthesize verdict + brief critique from PnL alone.

    The critique is intentionally short and template-based — finer-grained
    grading is the model's job to learn. We provide the supervision signal
    (WIN/LOSS + magnitude) and a one-line frame.
    """
    if pnl > 0:
        verdict = "WIN"
        if pnl_pct is not None and pnl_pct >= 5:
            critique = "Entry thesis confirmed; meaningful gain captured."
        elif pnl_pct is not None and pnl_pct >= 1:
            critique = "Modest win — entry was directionally correct."
        else:
            critique = "Marginal win, near-breakeven on a long hold."
    elif pnl < 0:
        verdict = "LOSS"
        if pnl_pct is not None and pnl_pct <= -10:
            critique = "Catastrophic loss — entry thesis broke down or stop set too wide."
        elif pnl_pct is not None and pnl_pct <= -3:
            critique = "Significant loss; stop fired or exit logic caught downside."
        else:
            critique = "Small loss — minor adverse move, low conviction recoverable."
    else:
        verdict = "BREAKEVEN"
        critique = "Position closed near entry."
    return verdict, critique


def build_corpus() -> list[dict]:
    c = _conn()
    # Fetch SELL trades that have realized_pnl != 0 and reasoning (we use the
    # SELL row's reasoning if present, but augment with the entry-side BUY).
    sells = c.execute(
        """
        SELECT
            id, player_id, symbol, qty,
            entry_price, exit_price, price AS sell_price,
            realized_pnl, reasoning AS sell_reasoning,
            executed_at, season,
            CASE WHEN entry_price > 0 THEN
                ROUND((price - entry_price) / entry_price * 100.0, 2)
            ELSE NULL END AS pnl_pct
        FROM trades
        WHERE action = 'SELL'
          AND realized_pnl IS NOT NULL
          AND realized_pnl != 0
        ORDER BY executed_at;
        """
    ).fetchall()

    # Helper: lookup the most recent prior BUY for a (player_id, symbol) pair.
    def _find_entry_buy(player_id: str, symbol: str, before_ts: str) -> dict | None:
        row = c.execute(
            """
            SELECT reasoning, confidence, executed_at, sources, timeframe
            FROM trades
            WHERE player_id = ? AND symbol = ? AND action = 'BUY'
              AND executed_at <= ?
            ORDER BY executed_at DESC LIMIT 1
            """,
            (player_id, symbol, before_ts),
        ).fetchone()
        return dict(row) if row else None

    # Helper: regime + SPY change for the exit date (close-to-close from regime_history).
    def _regime_context(date_str: str) -> dict:
        out = {"regime": None, "spy_change_pct": None, "vix": None}
        try:
            today = c.execute(
                "SELECT regime, spy_close FROM regime_history WHERE date=? LIMIT 1",
                (date_str,),
            ).fetchone()
            prior = c.execute(
                "SELECT spy_close FROM regime_history WHERE date < ? ORDER BY date DESC LIMIT 1",
                (date_str,),
            ).fetchone()
            if today:
                out["regime"] = today["regime"]
                if today["spy_close"] and prior and prior["spy_close"]:
                    out["spy_change_pct"] = round(
                        (today["spy_close"] - prior["spy_close"]) / prior["spy_close"] * 100.0, 3
                    )
        except Exception:
            pass
        return out

    corpus: list[dict] = []
    skipped_no_entry = 0
    skipped_no_reasoning = 0

    for s in sells:
        exit_ts = s["executed_at"]
        if not exit_ts:
            continue
        date_str = exit_ts[:10]  # YYYY-MM-DD
        # Recover entry context
        entry = _find_entry_buy(s["player_id"], s["symbol"], exit_ts)
        if entry is None:
            skipped_no_entry += 1
            continue
        entry_reasoning = (entry.get("reasoning") or "").strip()
        if not entry_reasoning or len(entry_reasoning) < 10:
            # Fall back to the SELL's own reasoning if present — useful for
            # mechanical exits (Autopilot trims) that may carry context.
            entry_reasoning = (s["sell_reasoning"] or "").strip()
        if not entry_reasoning or len(entry_reasoning) < 10:
            skipped_no_reasoning += 1
            continue

        ctx = _regime_context(date_str)
        verdict, critique = _grade_critique(s["realized_pnl"], s["pnl_pct"])

        # Compose prompt + completion in the user's specified Format A
        prompt = (
            f"Trade context: symbol={s['symbol']}, "
            f"player={s['player_id']}, "
            f"timeframe={entry.get('timeframe') or 'SWING'}, "
            f"regime={ctx['regime']}, "
            f"spy_change={ctx['spy_change_pct']}%, "
            f"reasoning={entry_reasoning[:600]}"
        )
        completion = (
            f"Verdict: {verdict}, "
            f"realized_pnl={s['realized_pnl']:.2f} ({s['pnl_pct'] if s['pnl_pct'] is not None else '?'}%), "
            f"critique={critique}"
        )

        corpus.append({
            "prompt": prompt,
            "completion": completion,
            # Out-of-band metadata for analysis (not consumed at training time)
            "_meta": {
                "trade_id": s["id"],
                "player_id": s["player_id"],
                "symbol": s["symbol"],
                "exit_date": date_str,
                "realized_pnl": s["realized_pnl"],
                "pnl_pct": s["pnl_pct"],
                "verdict": verdict,
                "regime": ctx["regime"],
                "spy_change_pct": ctx["spy_change_pct"],
            },
        })

    c.close()
    print(f"  → Sells available:          {len(sells)}")
    print(f"  → Skipped (no entry BUY):   {skipped_no_entry}")
    print(f"  → Skipped (no reasoning):   {skipped_no_reasoning}")
    print(f"  → Corpus rows built:        {len(corpus)}")
    return corpus


def main() -> int:
    print(f"══════ HM-PLUTUS-CORPUS-V1 ══════")
    print(f"DB:  {DB}")
    print(f"Out: {OUT}")
    print()
    corpus = build_corpus()
    if not corpus:
        print("  No corpus rows built. Exiting.")
        return 1
    # Distribution stats
    verdicts = [r["_meta"]["verdict"] for r in corpus]
    wins = verdicts.count("WIN")
    losses = verdicts.count("LOSS")
    breakeven = verdicts.count("BREAKEVEN")
    reasoning_lengths = [
        len(r["prompt"].split("reasoning=", 1)[-1]) for r in corpus
    ]
    print()
    print("══ DISTRIBUTION ══")
    print(f"  WINS:      {wins} ({wins/len(corpus)*100:.1f}%)")
    print(f"  LOSSES:    {losses} ({losses/len(corpus)*100:.1f}%)")
    print(f"  BREAKEVEN: {breakeven}")
    print()
    print(f"  Reasoning length — mean: {statistics.mean(reasoning_lengths):.0f}, "
          f"median: {int(statistics.median(reasoning_lengths))}, "
          f"min: {min(reasoning_lengths)}, max: {max(reasoning_lengths)}")
    print()

    # Per-regime breakdown
    regime_counts: dict = {}
    for r in corpus:
        rg = r["_meta"]["regime"]
        regime_counts[rg] = regime_counts.get(rg, 0) + 1
    print("  Per-regime counts:")
    for rg, n in sorted(regime_counts.items(), key=lambda kv: -kv[1]):
        print(f"    {rg!r:25} {n}")
    print()

    # Write JSONL — one object per line
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        for r in corpus:
            f.write(json.dumps(r) + "\n")
    print(f"  → Wrote {len(corpus)} rows to {OUT}")
    print(f"  → File size: {OUT.stat().st_size / 1024:.1f} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
