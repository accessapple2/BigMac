"""HM-PLUTUS-V7-TRAIN — THE OOS GATE. Generate trained plutus-v7 critiques on the frozen 178-row OOS
set and score them through the TIGHTENED eval, exactly as v1/v6 were. Non-circular: those scenarios are
NOT in v7's corpus (leakage=0, asserted). Compare v7 vs the v1 tightened baseline + the locked gate.

GPU discipline: generate ALL v7 critiques first (plutus-v7 resident), then judge one model at a time.
"""
from __future__ import annotations
import os, sys, json, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common, score  # noqa: E402
from common import DATA, OOS_SET, read_jsonl, write_jsonl, JUDGES, DIMS, ollama_generate  # noqa: E402
from score_pilot import V1_TIGHT  # tightened v1 baseline + gate reference

V7_MODEL = "plutus-v7c:latest"
SYSTEM = "You are Plutus, financial intelligence officer for OllieTrades. Analyze trades, evaluate signals, critique positions. Report to the Admiral. Be concise."
GEN = os.path.join(DATA, "plutus_v7", "gen_v7c_oos.jsonl")
RAW = os.path.join(DATA, "plutus_v7", "judge_raw_v7c_oos.jsonl")
CARD = os.path.join(DATA, "plutus_v7", "scorecard_v7c_oos.json")
# v1 full tightened baseline (trade_critique + ALL), from judge_raw_riskid_tightened.jsonl
V1_TC = V1_TIGHT  # overall 1.393 + per-dim


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 3) if xs else None


def generate(scen):
    done = {r["id"]: r for r in read_jsonl(GEN)} if os.path.exists(GEN) else {}
    out = list(done.values())
    for s in scen:
        if s["id"] in done:
            continue
        txt = ollama_generate(V7_MODEL, s["prompt"], num_predict=512, temperature=0.3, system=SYSTEM)
        out.append({"id": s["id"], "category": s["category"], "v7c_text": txt})
        write_jsonl(GEN, out)
    print(f"[gen] {len(out)} v7 critiques on OOS")
    return {r["id"]: r for r in out}


def judge(scen, gens):
    done = set()
    raws = read_jsonl(RAW) if os.path.exists(RAW) else []
    for r in raws:
        done.add((r["id"], r["judge"]))
    for j in JUDGES:
        pending = [s for s in scen if (s["id"], j) not in done]
        print(f"[judge {j}] {len(pending)}")
        for i, s in enumerate(pending, 1):
            sc, _ = score.judge_one(j, {**s, "decisive_risk": "unknown"}, gens[s["id"]]["v7c_text"])
            raws.append({"id": s["id"], "category": s["category"], "judge": j,
                         "scores": sc, "unscorable": sc is None})
            if i % 20 == 0 or i == len(pending):
                write_jsonl(RAW, raws)
        write_jsonl(RAW, raws)
    return raws


def aggregate(scen, gens, raws):
    scen_by = {s["id"]: s for s in scen}
    item = collections.defaultdict(dict)
    for r in raws:
        if r["scores"]:
            item[r["id"]][r["judge"]] = r["scores"]

    def dims_for(cat):
        ids = [s["id"] for s in scen if (cat == "ALL" or s["category"] == cat)]
        d = {}
        for dim in DIMS:
            d[dim] = _mean([_mean([js[dim] for js in item[i].values()]) for i in ids if i in item])
        ov = _mean([_mean([_mean([js[dim] for js in item[i].values()]) for dim in DIMS])
                    for i in ids if i in item])
        return ov, d, len([i for i in ids if i in item])

    tc_ov, tc_d, tc_n = dims_for("trade_critique")
    all_ov, all_d, all_n = dims_for("ALL")
    # guards on trade_critique
    rr, dirm, lead = [], [], []
    for s in scen:
        if s["category"] != "trade_critique":
            continue
        t = gens[s["id"]]["v7c_text"]
        rr.append(score.restatement_ratio(t, s["prompt"]))
        dm = score.direction_vs_realized(t, s.get("realized_verdict"))
        if dm is not None:
            dirm.append(1.0 if dm else 0.0)
        lead.append(1.0 if score.leads_with_verdict(t) else 0.0)
    guards = {"restatement_ratio_mean": _mean(rr),
              "direction_match_pct": round(100*_mean(dirm), 1) if dirm else None,
              "leads_verdict_pct": round(100*_mean(lead), 1)}

    gate = {
        "overall_pass": (tc_ov >= V1_TC["overall"] + 0.15),
        "overall": {"v7": tc_ov, "v1": V1_TC["overall"], "need": round(V1_TC["overall"]+0.15, 3)},
        "no_dim_regress_gt_0.1": {d: {"v7": tc_d[d], "v1": V1_TC[d], "ok": (tc_d[d] or 0) >= V1_TC[d]-0.1}
                                  for d in DIMS},
        "restatement_ok": guards["restatement_ratio_mean"] <= V1_TC["restatement"],
        "restatement": {"v7": guards["restatement_ratio_mean"], "v1": V1_TC["restatement"]},
        "direction_ok": (guards["direction_match_pct"] or 0) >= V1_TC["direction_pct"],
        "direction": {"v7": guards["direction_match_pct"], "v1": V1_TC["direction_pct"]},
    }
    gate["ALL_CONDITIONS_PASS"] = bool(gate["overall_pass"] and gate["restatement_ok"]
                                       and gate["direction_ok"]
                                       and all(v["ok"] for v in gate["no_dim_regress_gt_0.1"].values()))
    out = {"model": V7_MODEL, "n_trade_critique": tc_n, "n_all": all_n,
           "v7_trade_critique": {"overall": tc_ov, "dims": tc_d, "guards": guards},
           "v7_all": {"overall": all_ov, "dims": all_d},
           "v1_baseline_tightened": V1_TC, "GATE": gate}
    with open(CARD, "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))
    print(f"\n>>> v7 trade_critique overall {tc_ov} vs v1 {V1_TC['overall']} (need >={gate['overall']['need']})")
    print(f">>> GATE ALL CONDITIONS PASS: {gate['ALL_CONDITIONS_PASS']}")


if __name__ == "__main__":
    scen = read_jsonl(OOS_SET)
    gens = generate(scen)
    raws = judge(scen, gens)
    aggregate(scen, gens, raws)
