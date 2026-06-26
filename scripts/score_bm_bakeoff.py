#!/usr/bin/env python3
"""HM-BM-BAKEOFF Phase 3 — score the blind critiques per model.

Reads : data/bm_critiques_v1.jsonl  (one row per (trade_id, model) critique)
Writes: nothing (stdout ranked table only)

Each critique is a BLIND entry-decision review: the model emitted a SCORE (1-10)
and a VERDICT (WIN|LOSS) WITHOUT knowing the outcome. The harness recorded the
*actual* outcome alongside. This scorer measures, per model, how well that blind
call tracked reality:

  - hit_rate        : verdict (WIN/LOSS) == actual_outcome (win/loss)
  - separation      : mean score on eventual WINNERS minus mean score on LOSERS.
                      Positive = the 1-10 score discriminates outcomes even when
                      the binary verdict is biased (the live corpus skews LOSS).
  - conv_wtd_score  : directional-correctness × conviction. Conviction is the
                      score's distance from neutral (|score-5.5|/4.5 ∈ [0,1]);
                      signed +conviction when the verdict was right, -conviction
                      when wrong. A confident-correct call scores near +1, a
                      confident-wrong call near -1, a hedge near 0. Mean over rows.

Rank is by conv_wtd_score (the "directional-correctness × conviction-weight"
metric), with separation and hit_rate shown alongside.

Usage:
  python3 scripts/score_bm_bakeoff.py                 # default jsonl
  python3 scripts/score_bm_bakeoff.py <path.jsonl>    # score a specific file/.bak
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
DEFAULT_IN = REPO / "data" / "bm_critiques_v1.jsonl"

_NEUTRAL = 5.5      # midpoint of the 1-10 score scale
_SPAN = 4.5         # max distance from neutral (|1-5.5| = |10-5.5| = 4.5)


def _conviction(score: int) -> float:
    """Magnitude of deviation from a neutral 5.5 score, normalised to [0,1]."""
    return min(1.0, abs(score - _NEUTRAL) / _SPAN)


def load(path: Path) -> list[dict]:
    if not path.exists():
        sys.exit(f"ERROR: critiques file not found: {path}")
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue            # tolerate a torn final line during a live run
    return rows


def score(rows: list[dict]) -> tuple[list[dict], int]:
    """Aggregate per-model metrics. Returns (ranked_rows, skipped_count)."""
    by_model: dict[str, list[dict]] = defaultdict(list)
    skipped = 0
    for r in rows:
        verdict = (r.get("verdict") or "").upper()
        outcome = (r.get("actual_outcome") or "").lower()
        s = r.get("score")
        if not r.get("ok") or verdict not in ("WIN", "LOSS") \
                or outcome not in ("win", "loss") or s is None:
            skipped += 1
            continue
        by_model[r["model"]].append(r)

    out = []
    for model, recs in by_model.items():
        n = len(recs)
        hits = sum(1 for r in recs if (r["verdict"].upper() == "WIN") == (r["actual_outcome"].lower() == "win"))
        win_scores = [r["score"] for r in recs if r["actual_outcome"].lower() == "win"]
        loss_scores = [r["score"] for r in recs if r["actual_outcome"].lower() == "loss"]
        mean_win = sum(win_scores) / len(win_scores) if win_scores else 0.0
        mean_loss = sum(loss_scores) / len(loss_scores) if loss_scores else 0.0
        conv = 0.0
        for r in recs:
            correct = (r["verdict"].upper() == "WIN") == (r["actual_outcome"].lower() == "win")
            c = _conviction(r["score"])
            conv += c if correct else -c
        lats = [r["latency_s"] for r in recs if isinstance(r.get("latency_s"), (int, float))]
        out.append({
            "model": model,
            "n": n,
            "hit_rate": hits / n,
            "separation": mean_win - mean_loss,
            "conv_wtd": conv / n,
            "mean_score": sum(r["score"] for r in recs) / n,
            "mean_latency": (sum(lats) / len(lats)) if lats else 0.0,
        })
    out.sort(key=lambda d: d["conv_wtd"], reverse=True)
    return out, skipped


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_IN
    rows = load(path)
    ranked, skipped = score(rows)

    print(f"══════ HM-BM-BAKEOFF Phase 3 — scoring {path.name} ══════")
    print(f"  rows read: {len(rows)} | scored: {sum(r['n'] for r in ranked)} | skipped: {skipped}")
    print()
    hdr = f"{'#':>2}  {'model':22s} {'n':>4} {'hit_rate':>9} {'separation':>11} {'conv_wtd':>9} {'mean_sc':>8} {'lat_s':>7}"
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))
    for i, r in enumerate(ranked, 1):
        print(f"{i:>2}  {r['model']:22s} {r['n']:>4} "
              f"{r['hit_rate']:>8.1%} {r['separation']:>+11.2f} "
              f"{r['conv_wtd']:>+9.3f} {r['mean_score']:>8.2f} {r['mean_latency']:>7.2f}")
    if not ranked:
        print("  (no scoreable rows yet)")
    print()
    print("  rank = conv_wtd (directional-correctness × conviction). "
          "separation>0 ⇒ score discriminates outcomes.")


if __name__ == "__main__":
    main()
