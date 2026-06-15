"""HM-PLUTUS-V7-EVAL Phase 1+3 — absolute rubric scorer + guards + aggregation.

- Programmatic guards (no judge, deterministic): leads_with_verdict, direction_vs_realized,
  restatement_ratio (output-vs-prompt trigram overlap = the direct catch for input-echo),
  length-band.
- Two judges on .168 (qwen3:14b, gpt-oss:20b) score each critique 0/1/2 on 6 dims.
- GPU discipline: judges run in the OUTER loop, so exactly ONE judge model is resident at a
  time (back-to-back calls keep it warm; keep_alive=30s, never pinned).
- Defensive JSON parse: strip fences, retry once, mark unscorable on 2nd failure and exclude
  from means (count reported).
- Idempotent: judge_raw.jsonl is appended/skipped per (scenario,model,judge).

Usage:
  python3 score.py --smoke 3      # end-to-end smoke on first 3 scenarios
  python3 score.py                # full run + scorecard
"""
from __future__ import annotations
import json, os, re, sys, collections
from common import (OOS_SET, GEN_FILE, JUDGE_RAW, SCORECARD, JUDGES, JUDGE_CFG, DIMS, DIM_HELP,
                    read_jsonl, write_jsonl, ollama_generate)

# --------------------------------------------------------------------------- guards
_VERDICT_LEAD = re.compile(r"^\W{0,4}\**\s*verdict\b", re.I)
_POS = re.compile(r"\b(win|winner|successful|success|profit|gain|confirmed|bullish lean correct)\b", re.I)
_NEG = re.compile(r"\b(loss|lost|losing|failed|failure|unsuccessful|catastrophic|adverse|wrong)\b", re.I)

def _trigrams(text):
    w = re.findall(r"[a-z0-9]+", (text or "").lower())
    return set(zip(w, w[1:], w[2:]))

def restatement_ratio(output, prompt):
    """Fraction of output trigrams that also occur in the prompt (high = echoing input)."""
    o = _trigrams(output)
    if not o:
        return 0.0
    p = _trigrams(prompt)
    return round(len(o & p) / len(o), 4)

def leads_with_verdict(text):
    return bool(_VERDICT_LEAD.match((text or "").lstrip()))

def direction_vs_realized(text, realized_verdict):
    """Heuristic: does the critique's framing match the realized WIN/LOSS? None if N/A."""
    if realized_verdict not in ("WIN", "LOSS"):
        return None
    pos, neg = len(_POS.findall(text or "")), len(_NEG.findall(text or ""))
    if pos == neg:
        return False  # ambiguous framing counts as a miss
    pred = "WIN" if pos > neg else "LOSS"
    return pred == realized_verdict

def length_band(text, lo=200, hi=4000):
    return lo <= len(text or "") <= hi

def guards_for(text, scenario):
    return {
        "restatement_ratio": restatement_ratio(text, scenario["prompt"]),
        "leads_verdict": leads_with_verdict(text),
        "direction_match": direction_vs_realized(text, scenario.get("realized_verdict")),
        "length_in_band": length_band(text),
        "chars": len(text or ""),
    }

# --------------------------------------------------------------------------- judge
def judge_prompt(scenario, critique):
    realized = ""
    if scenario.get("realized_verdict"):
        realized = (f"\nREALIZED OUTCOME (ground truth): {scenario['realized_verdict']} "
                    f"(P&L {scenario.get('realized_pnl_pct')}%)")
    dr = scenario.get("decisive_risk")
    if dr and dr != "unknown":
        realized += f"\nDECISIVE RISK (anchor for risk_id — score 2 only if the critique names this): {dr}"
    rubric = "\n".join(f"- {d}: {DIM_HELP[d]}" for d in DIMS)
    return f"""You are a strict, neutral evaluator of trading-critique quality. Score the CRITIQUE \
on each dimension as an integer 0, 1, or 2 using the rubric. Judge ONLY the critique's quality \
against the scenario and the realized outcome — do not reward length or restating the input.

SCENARIO:
{scenario['prompt']}{realized}

CRITIQUE TO SCORE:
{critique}

RUBRIC (0/1/2 each):
{rubric}

Output ONLY a single JSON object, no prose, no markdown fences, no reasoning:
{{"risk_id":0,"directional_lean":0,"calibration":0,"actionability":0,"non_redundancy":0,"format_concision":0}}"""

def parse_scores(raw):
    if not raw:
        return None
    s = raw.strip().replace("```json", "").replace("```", "")
    s = re.sub(r"<think>.*?</think>", "", s, flags=re.S)  # strip reasoning blocks
    m = re.search(r"\{[^{}]*\}", s, re.S)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except Exception:
        return None
    out = {}
    for d in DIMS:
        v = obj.get(d)
        if not isinstance(v, (int, float)) or int(v) not in (0, 1, 2):
            return None
        out[d] = int(v)
    return out

def judge_one(judge_model, scenario, critique):
    p = judge_prompt(scenario, critique)
    cfg = JUDGE_CFG[judge_model]
    for attempt in range(2):
        raw = ollama_generate(judge_model, p, num_predict=cfg["num_predict"],
                              temperature=0.0, think=cfg["think"])
        sc = parse_scores(raw)
        if sc is not None:
            return sc, raw
    return None, raw  # unscorable

# --------------------------------------------------------------------------- run
def run(limit=None):
    scen = {s["id"]: s for s in read_jsonl(OOS_SET)}
    gens = read_jsonl(GEN_FILE)
    if limit:
        ids = list(scen.keys())[:limit]
        gens = [g for g in gens if g["scenario_id"] in ids]
    done = set()
    if os.path.exists(JUDGE_RAW):
        for r in read_jsonl(JUDGE_RAW):
            done.add((r["scenario_id"], r["model"], r["judge"]))
    raws = read_jsonl(JUDGE_RAW) if os.path.exists(JUDGE_RAW) else []

    # OUTER = judge → exactly one judge model resident at a time
    for judge in JUDGES:
        pending = [(g, m) for g in gens for m in ("v1", "v6")
                   if (g["scenario_id"], m, judge) not in done]
        print(f"[judge {judge}] {len(pending)} critiques to score "
              f"(skipping {len(gens)*2 - len(pending)} already done)")
        for i, (g, m) in enumerate(pending, 1):
            s = scen[g["scenario_id"]]
            crit = g[f"{m}_text"]
            sc, raw = judge_one(judge, s, crit)
            raws.append({"scenario_id": g["scenario_id"], "category": s["category"],
                         "model": m, "judge": judge,
                         "scores": sc, "unscorable": sc is None})
            if i % 20 == 0 or i == len(pending):
                write_jsonl(JUDGE_RAW, raws)
                print(f"  {judge}: {i}/{len(pending)} (unscorable so far: "
                      f"{sum(1 for r in raws if r['judge']==judge and r['unscorable'])})")
        write_jsonl(JUDGE_RAW, raws)
    aggregate()

# --------------------------------------------------------------------------- aggregate
def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 3) if xs else None

def cohen_kappa(pairs):
    """Unweighted Cohen's kappa over (a,b) integer pairs in {0,1,2}."""
    pairs = [(a, b) for a, b in pairs if a is not None and b is not None]
    n = len(pairs)
    if n == 0:
        return None
    cats = [0, 1, 2]
    po = sum(1 for a, b in pairs if a == b) / n
    fa = collections.Counter(a for a, _ in pairs)
    fb = collections.Counter(b for _, b in pairs)
    pe = sum((fa[c]/n) * (fb[c]/n) for c in cats)
    if pe == 1:
        return 1.0
    return round((po - pe) / (1 - pe), 3)

def aggregate():
    scen = {s["id"]: s for s in read_jsonl(OOS_SET)}
    gens = {g["scenario_id"]: g for g in read_jsonl(GEN_FILE)}
    raws = read_jsonl(JUDGE_RAW)

    # per-item per-dim = mean of the two judges; collect unscorable
    item = collections.defaultdict(lambda: collections.defaultdict(dict))  # (sid,model)->dim->{judge:score}
    unscorable = collections.Counter()
    for r in raws:
        key = (r["scenario_id"], r["model"])
        if r["unscorable"] or r["scores"] is None:
            unscorable[(r["model"], r["judge"])] += 1
            continue
        for d, v in r["scores"].items():
            item[key][d][r["judge"]] = v

    cats = sorted(set(s["category"] for s in scen.values())) + ["ALL"]
    scorecard = {"meta": {"n_scenarios": len(scen), "judges": JUDGES, "dims": DIMS},
                 "unscorable": {f"{m}/{j}": c for (m, j), c in sorted(unscorable.items())},
                 "by_model": {}}

    for model in ("v1", "v6"):
        scorecard["by_model"][model] = {}
        for cat in cats:
            sids = [sid for sid, s in scen.items() if cat == "ALL" or s["category"] == cat]
            dim_means, per_item_overall = {}, []
            for d in DIMS:
                vals = [_mean(list(item[(sid, model)][d].values()))
                        for sid in sids if (sid, model) in item and item[(sid, model)][d]]
                dim_means[d] = _mean(vals)
            # overall = mean over items of (mean of that item's 6 dim means)
            for sid in sids:
                if (sid, model) in item:
                    dm = [_mean(list(item[(sid, model)][d].values())) for d in DIMS
                          if item[(sid, model)][d]]
                    if dm:
                        per_item_overall.append(_mean(dm))
            # guards
            g_rr, g_dir, g_lead, g_band = [], [], [], []
            for sid in sids:
                if sid not in gens:
                    continue
                gd = guards_for(gens[sid][f"{model}_text"], scen[sid])
                g_rr.append(gd["restatement_ratio"])
                if gd["direction_match"] is not None:
                    g_dir.append(1.0 if gd["direction_match"] else 0.0)
                g_lead.append(1.0 if gd["leads_verdict"] else 0.0)
                g_band.append(1.0 if gd["length_in_band"] else 0.0)
            scorecard["by_model"][model][cat] = {
                "n_items": len(per_item_overall),
                "overall_mean": _mean(per_item_overall),
                "dims": dim_means,
                "guards": {
                    "restatement_ratio_mean": _mean(g_rr),
                    "direction_match_pct": (round(100*_mean(g_dir), 1) if g_dir else None),
                    "leads_verdict_pct": round(100*_mean(g_lead), 1) if g_lead else None,
                    "length_in_band_pct": round(100*_mean(g_band), 1) if g_band else None,
                },
            }

    # inter-judge agreement (per dim, pooled over both models)
    ij = {}
    for d in DIMS:
        pairs = []
        for key, dims in item.items():
            jd = dims.get(d, {})
            if JUDGES[0] in jd and JUDGES[1] in jd:
                pairs.append((jd[JUDGES[0]], jd[JUDGES[1]]))
        exact = round(100*sum(1 for a, b in pairs if a == b)/len(pairs), 1) if pairs else None
        mad = round(sum(abs(a-b) for a, b in pairs)/len(pairs), 3) if pairs else None
        ij[d] = {"n_pairs": len(pairs), "cohen_kappa": cohen_kappa(pairs),
                 "exact_agreement_pct": exact, "mean_abs_diff": mad}
    scorecard["inter_judge_agreement"] = ij

    write_jsonl(SCORECARD.replace(".json", ".tmp.jsonl"), [scorecard])  # not used; keep .json canonical
    os.remove(SCORECARD.replace(".json", ".tmp.jsonl"))
    with open(SCORECARD, "w") as f:
        json.dump(scorecard, f, indent=2)
    print(f"[aggregate] wrote {SCORECARD}")
    _print_summary(scorecard)
    return scorecard

def _print_summary(sc):
    print("\n===== SCORECARD =====")
    for model in ("v1", "v6"):
        a = sc["by_model"][model]["ALL"]; tc = sc["by_model"][model].get("trade_critique", {})
        print(f"{model}: ALL overall={a['overall_mean']} (n={a['n_items']})  "
              f"trade_critique overall={tc.get('overall_mean')} (n={tc.get('n_items')})  "
              f"restate(tc)={tc.get('guards',{}).get('restatement_ratio_mean')}")
    print("inter-judge κ:", {d: v["cohen_kappa"] for d, v in sc["inter_judge_agreement"].items()})
    print("unscorable:", sc["unscorable"])


if __name__ == "__main__":
    if "--aggregate-only" in sys.argv:
        aggregate()
    elif "--smoke" in sys.argv:
        n = int(sys.argv[sys.argv.index("--smoke")+1])
        run(limit=n)
    else:
        run()
