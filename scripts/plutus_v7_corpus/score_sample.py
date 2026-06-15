"""HM-PLUTUS-V7-CORPUS Phase B — spot-score ~36 v2 re-authored tc targets through the tightened eval.

Re-validates that temp-0.6 + opening-variety did NOT slip quality below the gate before freezing 1,030
rows as training data. Same scoring path as the pilot (2 judges, decisive_risk='unknown' anchor).
"""
from __future__ import annotations
import os, sys, json, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common, score  # noqa: E402
from common import DATA, read_jsonl, write_jsonl, JUDGES, DIMS  # noqa: E402
from score_pilot import V1_TIGHT  # reuse the tightened v1 baseline

POOL = os.path.join(DATA, "plutus_v7", "scenario_pool.jsonl")
AUTHORED = os.path.join(DATA, "plutus_v7", "authored_tc.jsonl")
RAW = os.path.join(DATA, "plutus_v7", "judge_raw_sample_v2.jsonl")
CARD = os.path.join(DATA, "plutus_v7", "scorecard_sample_v2.json")
N = 36


def pick(authored):
    by = collections.defaultdict(list)
    for a in authored:
        by[a["outcome"]].append(a)
    for k in by:
        by[k].sort(key=lambda a: a["id"])
    quota = {"win": 16, "loss": 14, "flat": 6}
    out = []
    for o, q in quota.items():
        out += by.get(o, [])[:q]
    return out


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 3) if xs else None


def main():
    pool = {p["id"]: p for p in read_jsonl(POOL)}
    sample = pick(read_jsonl(AUTHORED))
    done = set()
    raws = read_jsonl(RAW) if os.path.exists(RAW) else []
    for r in raws:
        done.add((r["id"], r["judge"]))
    for judge in JUDGES:
        pending = [a for a in sample if (a["id"], judge) not in done]
        print(f"[sample {judge}] {len(pending)} to score")
        for i, a in enumerate(pending, 1):
            p = pool[a["id"]]
            scen = {"prompt": p["prompt"], "realized_verdict": p["realized_verdict"],
                    "realized_pnl_pct": p["realized_pnl_pct"], "decisive_risk": "unknown"}
            sc, _ = score.judge_one(judge, scen, a["critique"])
            raws.append({"id": a["id"], "judge": judge, "scores": sc, "unscorable": sc is None})
            if i % 12 == 0 or i == len(pending):
                write_jsonl(RAW, raws)
        write_jsonl(RAW, raws)

    item = collections.defaultdict(dict)
    for r in raws:
        if r["scores"]:
            item[r["id"]][r["judge"]] = r["scores"]
    dims = {d: _mean([_mean([js[d] for js in item[a["id"]].values()]) for a in sample if a["id"] in item])
            for d in DIMS}
    overall = _mean([_mean([_mean([js[d] for js in item[a["id"]].values()]) for d in DIMS])
                     for a in sample if a["id"] in item])
    rr, dirm, lead = [], [], []
    for a in sample:
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
        "risk_id_lift": round(dims["risk_id"] - V1_TIGHT["risk_id"], 3),
        "actionability_lift": round(dims["actionability"] - V1_TIGHT["actionability"], 3),
        "restatement_ok": guards["restatement_ratio_mean"] <= V1_TIGHT["restatement"],
        "no_dim_regress_>0.1": all((dims[d] or 0) >= V1_TIGHT[d] - 0.1 for d in DIMS),
    }
    out = {"n": len([a for a in sample if a["id"] in item]), "temp": 0.6, "v2": True,
           "dims": dims, "overall": overall, "guards": guards,
           "v1_baseline_tightened": V1_TIGHT, "gate": gate}
    with open(CARD, "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))
    print(f"\n>>> v2 sample overall {overall} vs gate {round(V1_TIGHT['overall']+0.15,3)}: "
          f"{'PASS' if gate['overall_pass'] else 'FAIL'}")


if __name__ == "__main__":
    main()
