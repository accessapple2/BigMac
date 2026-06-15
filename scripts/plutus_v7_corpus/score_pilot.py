"""HM-PLUTUS-V7-CORPUS Phase A2 — score the Grok-authored PILOT targets through the (tightened) eval.

Validates the author+prompt BEFORE mass production: the authored critiques must clear the v1
trade_critique baseline by +0.15 and lift risk_id + actionability with low restatement. Scored the
SAME way v1/v6 were (2 judges, decisive_risk='unknown' anchor → fair apples-to-apples; we do NOT feed
Grok's own decisive_risk back as the anchor, which would be circular). .168 sequential, one judge at
a time. Idempotent.
"""
from __future__ import annotations
import os, sys, json, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common, score  # noqa: E402
from common import read_jsonl, write_jsonl, JUDGES, DIMS  # noqa: E402

POOL = os.path.join(common.DATA, "plutus_v7", "scenario_pool.jsonl")
AUTHORED = os.path.join(common.DATA, "plutus_v7", "authored_pilot.jsonl")
RAW = os.path.join(common.DATA, "plutus_v7", "judge_raw_pilot.jsonl")
CARD = os.path.join(common.DATA, "plutus_v7", "scorecard_pilot.json")

# v1 baseline under the TIGHTENED rubric (the rubric the pilot is scored with), trade_critique
V1_TIGHT = {"overall": 1.393, "risk_id": 0.632, "directional_lean": 1.632, "calibration": 1.459,
            "actionability": 1.259, "non_redundancy": 1.45, "format_concision": 1.927,
            "restatement": 0.054, "direction_pct": 56.4}


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 3) if xs else None


def run():
    pool = {p["id"]: p for p in read_jsonl(POOL)}
    authored = read_jsonl(AUTHORED)
    done = set()
    raws = read_jsonl(RAW) if os.path.exists(RAW) else []
    for r in raws:
        done.add((r["id"], r["judge"]))
    for judge in JUDGES:
        pending = [a for a in authored if (a["id"], judge) not in done]
        print(f"[pilot {judge}] {len(pending)} to score")
        for i, a in enumerate(pending, 1):
            p = pool[a["id"]]
            scen = {"prompt": p["prompt"], "realized_verdict": p["realized_verdict"],
                    "realized_pnl_pct": p["realized_pnl_pct"], "decisive_risk": "unknown"}
            sc, _ = score.judge_one(judge, scen, a["critique"])
            raws.append({"id": a["id"], "judge": judge, "scores": sc, "unscorable": sc is None})
            if i % 12 == 0 or i == len(pending):
                write_jsonl(RAW, raws)
        write_jsonl(RAW, raws)
    aggregate(pool, authored, raws)


def aggregate(pool, authored, raws):
    item = collections.defaultdict(dict)
    for r in raws:
        if r["scores"]:
            item[r["id"]][r["judge"]] = r["scores"]
    # per-dim means
    dims = {}
    for d in DIMS:
        vals = [_mean([js[d] for js in item[a["id"]].values()]) for a in authored if a["id"] in item]
        dims[d] = _mean(vals)
    overall = _mean([_mean([_mean([js[d] for js in item[a["id"]].values()]) for d in DIMS])
                     for a in authored if a["id"] in item])
    # guards
    rr, dirm, lead = [], [], []
    for a in authored:
        p = pool[a["id"]]
        rr.append(score.restatement_ratio(a["critique"], p["prompt"]))
        dm = score.direction_vs_realized(a["critique"], p["realized_verdict"])
        if dm is not None:
            dirm.append(1.0 if dm else 0.0)
        lead.append(1.0 if score.leads_with_verdict(a["critique"]) else 0.0)
    guards = {"restatement_ratio_mean": _mean(rr),
              "direction_match_pct": round(100*_mean(dirm), 1) if dirm else None,
              "leads_verdict_pct": round(100*_mean(lead), 1)}

    gate = {
        "overall_pass": overall >= V1_TIGHT["overall"] + 0.15,
        "risk_id_lift": dims["risk_id"] - V1_TIGHT["risk_id"],
        "actionability_lift": dims["actionability"] - V1_TIGHT["actionability"],
        "restatement_ok": guards["restatement_ratio_mean"] <= V1_TIGHT["restatement"],
        "no_dim_regress_>0.1": all((dims[d] or 0) >= V1_TIGHT[d] - 0.1 for d in DIMS),
    }
    out = {"n": len([a for a in authored if a["id"] in item]), "author_model": authored[0]["author_model"],
           "pilot_dims": dims, "pilot_overall": overall, "pilot_guards": guards,
           "v1_baseline_tightened": V1_TIGHT, "gate_vs_v1_plus_0.15": gate}
    with open(CARD, "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))
    print(f"\n>>> PILOT overall {overall} vs v1 {V1_TIGHT['overall']} (need ≥{round(V1_TIGHT['overall']+0.15,3)}): "
          f"{'PASS' if gate['overall_pass'] else 'FAIL'}")
    print(f">>> risk_id {dims['risk_id']} (v1 {V1_TIGHT['risk_id']}, +{gate['risk_id_lift']:.3f}); "
          f"actionability {dims['actionability']} (v1 {V1_TIGHT['actionability']}, +{gate['actionability_lift']:.3f})")
    print(f">>> restatement {guards['restatement_ratio_mean']} (v1 {V1_TIGHT['restatement']}): "
          f"{'OK' if gate['restatement_ok'] else 'HIGHER-than-v1'}")


if __name__ == "__main__":
    run()
