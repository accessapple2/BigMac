"""HM-PLUTUS-V7-CORPUS Phase A1 — build the scenario pool from real closed trades.

Reuses the proven scenario builder (extract_plutus_corpus_v2.build_trade_critique_corpus) so the
prompt format is IDENTICAL to what plutus/the eval expect. We keep only the SCENARIO (prompt +
realized outcome) and DISCARD the v6-style synthetic completion — targets are authored fresh in A2/B.

HARD leakage exclusion: any scenario whose prompt is in data/plutus_eval/oos_set_v1.jsonl (the 178
frozen OOS prompts) is dropped — the eval set must stay clean. Read-only on trader.db. Create-only out.
"""
from __future__ import annotations
import os, sys, re, collections, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))  # scripts/
import common  # noqa: E402
from common import OOS_SET, read_jsonl, write_jsonl, parse_realized  # noqa: E402

POOL = os.path.join(common.DATA, "plutus_v7", "scenario_pool.jsonl")
TEST_SRC = os.path.join(common.DATA, "plutus_corpus_v6.test.jsonl")  # OOS source (for trade_id leakage)
_REGIME = re.compile(r"Regime:\s*([A-Z_]+)")
_AGENT = re.compile(r"Agent:\s*([^\n]+)")


def oos_trade_ids():
    """trade_ids behind the frozen OOS trade_critique scenarios — the robust leakage key
    (prompt text can drift between snapshots; trade_id is stable)."""
    ids = set()
    for l in open(TEST_SRC):
        m = json.loads(l).get("_meta", {})
        if m.get("trade_id") is not None:
            ids.add(m["trade_id"])
    return ids


def outcome_bucket(pct):
    if pct is None:
        return "unknown"
    if pct > 1.0:
        return "win"
    if pct < -1.0:
        return "loss"
    return "flat"


def main():
    from extract_plutus_corpus_v2 import build_trade_critique_corpus
    records = build_trade_critique_corpus()
    print(f"[pool] build_trade_critique_corpus returned {len(records)} trade scenarios")

    oos_prompts = {s["prompt"] for s in read_jsonl(OOS_SET)}
    oos_tids = oos_trade_ids()
    pool, leak_tid, leak_prompt = [], 0, 0
    for i, r in enumerate(records):
        p = r["prompt"]
        meta = r.get("_meta", {})
        tid = meta.get("trade_id")
        if tid is not None and tid in oos_tids:   # robust leakage key
            leak_tid += 1
            continue
        if p in oos_prompts:                       # belt-and-suspenders
            leak_prompt += 1
            continue
        verdict, pnl, pct = parse_realized(p, meta)
        rm = _REGIME.search(p)
        ag = _AGENT.search(p)
        pool.append({
            "id": f"sc-{i:04d}",
            "trade_id": tid,
            "prompt": p,
            "symbol": meta.get("symbol"),
            "agent": (ag.group(1).strip() if ag else meta.get("player_id")),
            "regime": rm.group(1) if rm else None,
            "realized_verdict": verdict,
            "realized_pnl": pnl,
            "realized_pnl_pct": pct,
            "outcome": outcome_bucket(pct),
            "decisive_risk": "unknown",
        })
    write_jsonl(POOL, pool)

    print(f"[pool] wrote {len(pool)} scenarios → {POOL}")
    print(f"[pool] leakage-excluded vs OOS: by trade_id={leak_tid}, by prompt-only={leak_prompt}, "
          f"total={leak_tid + leak_prompt}")
    def dist(key, top=12):
        c = collections.Counter(x[key] for x in pool)
        return c.most_common(top)
    print("outcome:", dict(dist("outcome")))
    print("regime :", dict(dist("regime")))
    print("top tickers:", dist("symbol", 12))
    print("agents :", dict(dist("agent", 12)))


if __name__ == "__main__":
    main()
