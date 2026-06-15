"""HM-PLUTUS-V7-CORPUS — re-score the CACHED 178x2 generations under the tightened risk_id rubric.

No new generation (reuses data/plutus_eval/gen_v1_v6.jsonl). Re-judges with the full 6-dim prompt
(now carrying the tightened risk_id criterion from score.py/common.py) so risk_id is measured in the
same context as the locked baseline. Writes to data/plutus_v7/ — the locked scorecard_v1_v6.json is
NOT touched. Reports risk_id κ + v1/v6 risk_id means vs the locked baseline. Idempotent.
"""
from __future__ import annotations
import os, sys, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common, score  # noqa: E402
from common import OOS_SET, GEN_FILE, JUDGES, DIMS, read_jsonl, write_jsonl  # noqa: E402

OUT_RAW = os.path.join(common.DATA, "plutus_v7", "judge_raw_riskid_tightened.jsonl")
OUT_CARD = os.path.join(common.DATA, "plutus_v7", "scorecard_riskid_tightened.json")
# locked baseline (from scorecard_v1_v6.json) for the before/after comparison
OLD = {"kappa_risk_id": 0.126,
       "v1": {"tc": 1.273, "all": 1.084}, "v6": {"tc": 1.005, "all": 0.921}}


def run():
    scen = {s["id"]: s for s in read_jsonl(OOS_SET)}
    gens = read_jsonl(GEN_FILE)
    done = set()
    raws = read_jsonl(OUT_RAW) if os.path.exists(OUT_RAW) else []
    for r in raws:
        done.add((r["scenario_id"], r["model"], r["judge"]))
    for judge in JUDGES:
        pending = [(g, m) for g in gens for m in ("v1", "v6")
                   if (g["scenario_id"], m, judge) not in done]
        print(f"[rescore {judge}] {len(pending)} to judge (skip {len(gens)*2 - len(pending)})")
        for i, (g, m) in enumerate(pending, 1):
            s = scen[g["scenario_id"]]
            sc, _ = score.judge_one(judge, s, g[f"{m}_text"])
            raws.append({"scenario_id": g["scenario_id"], "category": s["category"],
                         "model": m, "judge": judge, "scores": sc, "unscorable": sc is None})
            if i % 20 == 0 or i == len(pending):
                write_jsonl(OUT_RAW, raws)
                print(f"  {judge}: {i}/{len(pending)}")
        write_jsonl(OUT_RAW, raws)
    report(raws, scen)


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 3) if xs else None


def report(raws, scen):
    # per-item risk_id = mean of the two judges
    item = {}
    for r in raws:
        if not r["scores"]:
            continue
        item.setdefault((r["scenario_id"], r["model"]), {})[r["judge"]] = r["scores"]["risk_id"]
    def means(model, cat):
        vals = []
        for (sid, m), jd in item.items():
            if m != model:
                continue
            if cat != "ALL" and scen[sid]["category"] != cat:
                continue
            vals.append(_mean(list(jd.values())))
        return _mean(vals)
    # kappa over risk_id pairs
    pairs = [(jd[JUDGES[0]], jd[JUDGES[1]]) for jd in item.values()
             if JUDGES[0] in jd and JUDGES[1] in jd]
    k = score.cohen_kappa(pairs)
    out = {
        "dim": "risk_id", "rubric": "tightened (decisive-risk anchored)",
        "kappa": {"before": OLD["kappa_risk_id"], "after": k},
        "v1": {"tc_before": OLD["v1"]["tc"], "tc_after": means("v1", "trade_critique"),
               "all_before": OLD["v1"]["all"], "all_after": means("v1", "ALL")},
        "v6": {"tc_before": OLD["v6"]["tc"], "tc_after": means("v6", "trade_critique"),
               "all_before": OLD["v6"]["all"], "all_after": means("v6", "ALL")},
        "n_pairs": len(pairs),
    }
    with open(OUT_CARD, "w") as f:
        json.dump(out, f, indent=2)
    print("\n===== risk_id RE-SCORE (tightened rubric) =====")
    print(json.dumps(out, indent=2))
    rose = (k or 0) > 0.2
    print(f"\nκ {OLD['kappa_risk_id']} -> {k} : {'ROSE above ~0.2' if rose else 'STILL <~0.2 (keep risk_id directional-only; gate on overall delta)'}")
    print(f"new v1 risk_id baseline (trade_critique): {out['v1']['tc_after']}")


if __name__ == "__main__":
    run()
