"""HM-PLUTUS-V7-EVAL Phase 2 — generate v1 & v6-eval critiques per frozen scenario.

GPU-economy / shared-box default: REUSE the committed, identical generations from the v6
bakeoff (scripts/plutus_v6/results/bakeoff_gen.json — same models, same 178 prompts) when
present, and only call .168 for any scenario the bakeoff did not cover. This avoids burning
generation GPU on the shared live-fleet box. plutus generation is temp=0.3 (non-deterministic),
so reusing the committed outputs is also the reproducible choice. Flagged in the findings doc.

Output: data/plutus_eval/gen_v1_v6.jsonl. Idempotent — re-run skips scenarios already present.
Run with --force-fresh to ignore the bakeoff cache and generate everything on .168.
"""
from __future__ import annotations
import json, os, sys
from common import (OOS_SET, GEN_FILE, BAKEOFF_GEN, PLUTUS_MODELS, read_jsonl, write_jsonl,
                    ollama_generate)

SYSTEM = "You are Plutus, financial intelligence officer for OllieTrades. Analyze trades, evaluate signals, critique positions. Report to the Admiral. Be concise."


def load_bakeoff_cache():
    if not os.path.exists(BAKEOFF_GEN):
        return {}
    cache = {}
    for g in json.load(open(BAKEOFF_GEN)):
        gen = g.get("gen", {})
        if "v1" in gen and "v6_eval" in gen:
            cache[g["prompt"]] = (gen["v1"], gen["v6_eval"])
    return cache


def main(force_fresh=False):
    scen = read_jsonl(OOS_SET)
    done = {r["scenario_id"]: r for r in read_jsonl(GEN_FILE)} if os.path.exists(GEN_FILE) else {}
    cache = {} if force_fresh else load_bakeoff_cache()
    print(f"[generate] {len(scen)} scenarios; already done {len(done)}; bakeoff-cache {len(cache)}")

    out = []
    reused = fresh = 0
    for s in scen:
        sid = s["id"]
        if sid in done:
            out.append(done[sid]); continue
        if s["prompt"] in cache:
            v1, v6 = cache[s["prompt"]]; reused += 1; src = "bakeoff_gen_reuse"
        else:
            v1 = ollama_generate(PLUTUS_MODELS["v1"], s["prompt"], system=SYSTEM)
            v6 = ollama_generate(PLUTUS_MODELS["v6"], s["prompt"], system=SYSTEM)
            fresh += 1; src = "fresh_.168"
            print(f"  [.168] generated {sid} ({fresh} fresh)")
        out.append({"scenario_id": sid, "category": s["category"],
                    "v1_text": v1, "v6_text": v6, "gen_source": src})
    write_jsonl(GEN_FILE, out)
    print(f"[generate] wrote {len(out)} → {GEN_FILE} (reused {reused}, fresh {fresh})")


if __name__ == "__main__":
    main(force_fresh="--force-fresh" in sys.argv)
