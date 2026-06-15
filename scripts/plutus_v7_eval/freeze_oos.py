"""HM-PLUTUS-V7-EVAL Phase 0 — freeze the OOS eval set (idempotent, create-only).

Source = the 178-row held-out TEST split (plutus_corpus_v6.test.jsonl), which is OOS w.r.t.
v1/v6 training. Verifies zero leakage vs train/val. If oos_set_v1.jsonl already exists it is
REUSED untouched and reported.
"""
from __future__ import annotations
import collections, os, sys
from common import (TEST_SET, TRAIN_SET, VAL_SET, OOS_SET, read_jsonl, write_jsonl,
                    parse_realized)


def build():
    if os.path.exists(OOS_SET):
        rows = read_jsonl(OOS_SET)
        print(f"[idempotent] {OOS_SET} exists — REUSING {len(rows)} frozen scenarios, untouched.")
        report(rows, reused=True)
        return rows

    test = read_jsonl(TEST_SET)
    train_p = {r["prompt"] for r in read_jsonl(TRAIN_SET)}
    val_p = {r["prompt"] for r in read_jsonl(VAL_SET)}
    leak = [r for r in test if r["prompt"] in train_p or r["prompt"] in val_p]
    if leak:
        print(f"[WARN] leakage detected: {len(leak)} test prompts appear in train/val — "
              f"these are EXCLUDED from the frozen set.")
    test = [r for r in test if r["prompt"] not in train_p and r["prompt"] not in val_p]

    rows = []
    for i, r in enumerate(test):
        meta = r.get("_meta", {})
        cat = meta.get("category", "unknown")
        verdict, pnl, pct = parse_realized(r["prompt"], meta)
        rows.append({
            "id": f"oos-{i:04d}",
            "category": cat,
            "symbol": meta.get("symbol"),
            "prompt": r["prompt"],
            "realized_verdict": verdict,        # WIN/LOSS for trade_critique; may be None
            "realized_pnl": pnl,
            "realized_pnl_pct": pct,
            "decisive_risk": "unknown",         # corpus has no such field; judge assesses Risk-ID vs context
            "reference": r.get("completion"),   # kept for traceability; NOT shown to judges (would bias to template)
            "source": "plutus_corpus_v6.test (held-out, OOS w.r.t. training)",
        })
    write_jsonl(OOS_SET, rows)
    print(f"[created] {OOS_SET} — {len(rows)} scenarios; leakage removed: {len(leak)}")
    report(rows, reused=False)
    return rows


def report(rows, reused):
    cats = collections.Counter(r["category"] for r in rows)
    n = len(rows)
    print(f"--- OOS set: {n} scenarios ({'reused' if reused else 'new'}) ---")
    for c, k in cats.most_common():
        print(f"    {c:20s} {k:4d}  ({100*k/n:4.1f}%)")
    if n < 100:
        print(f"[FLAG] small-n: {n} < 100 usable scenarios — per-category reads are weak.")
    for c, k in cats.items():
        if k < 20:
            print(f"[FLAG] thin category '{c}': n={k} — treat its per-category means as indicative only.")


if __name__ == "__main__":
    build()
