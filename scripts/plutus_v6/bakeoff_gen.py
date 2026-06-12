"""Phase 1: generate v1 + v6-eval critiques for all 178 held-out test prompts.
Grouped by model (all of one model, then the other) so ollama keeps one model
resident instead of swapping VRAM every call. Output: bakeoff_gen.json"""
import json, urllib.request, time

TEST = "/home/bigmac/plutus_corpus_v6.test.jsonl"
OUT  = "/home/bigmac/bakeoff_gen.json"
MODELS = {"v1": "plutus-v1", "v6_eval": "plutus-v6-eval"}
OPTS = {"temperature": 0.3, "num_predict": 340}

def load(fn):
    with open(fn) as f:
        return [json.loads(l) for l in f if l.strip()]

def gen(model, prompt, retries=2):
    body = json.dumps({"model": model, "prompt": prompt, "stream": False, "options": OPTS}).encode()
    for a in range(retries+1):
        try:
            req = urllib.request.Request("http://localhost:11434/api/generate", data=body,
                                         headers={"Content-Type": "application/json"})
            return json.load(urllib.request.urlopen(req, timeout=240))["response"].strip()
        except Exception as e:
            if a == retries:
                return f"__GEN_ERROR__: {e}"
            time.sleep(3)

rows = load(TEST)
print(f"Loaded {len(rows)} test rows")
out = []
for i, r in enumerate(rows):
    out.append({
        "idx": i,
        "prompt": r["prompt"],
        "reference": r["completion"],
        "meta": r.get("_meta", {}),
        "gen": {},
    })

for key, model in MODELS.items():
    t0 = time.time()
    print(f"=== generating {key} ({model}) ===", flush=True)
    for rec in out:
        rec["gen"][key] = gen(model, rec["prompt"])
    print(f"  {key} done in {time.time()-t0:.0f}s", flush=True)

with open(OUT, "w") as f:
    json.dump(out, f)
errs = sum(1 for r in out for v in r["gen"].values() if v.startswith("__GEN_ERROR__"))
print(f"Saved {len(out)} paired generations to {OUT} | gen_errors={errs}")
