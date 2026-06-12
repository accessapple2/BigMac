"""Phase 2: blind pairwise LLM-judge of v1 vs v6-eval over the 178 held-out
critiques. Order-balanced by idx parity (no position bias). Judge = strongest
neutral non-plutus model present. Output: bakeoff_scored.json + summary."""
import json, urllib.request, re, time, sys, os

GEN = "/home/bigmac/bakeoff_gen.json"
OUT = os.environ.get("OUT", "/home/bigmac/bakeoff_scored.json")

def ollama_models():
    req = urllib.request.Request("http://localhost:11434/api/tags")
    return [m["name"] for m in json.load(urllib.request.urlopen(req, timeout=20))["models"]]

# pick judge: env override, else strongest neutral, never a plutus model
avail = ollama_models()
PREF = ["qwen3:14b", "deepseek-r1:14b", "qwen3:8b", "qwen2.5-coder:7b"]
JUDGE = os.environ.get("JUDGE_MODEL")
if not JUDGE:
    JUDGE = next((m for p in PREF for m in avail if m.split(":")[0] == p.split(":")[0] and (":" not in p or m == p or m.startswith(p))), None)
if JUDGE is None:
    JUDGE = next((m for m in avail if "plutus" not in m.lower()), avail[0])
# reasoning models (gpt-oss) think before answering -> need a bigger budget
NUMPRED = 700 if "gpt-oss" in JUDGE or "deepseek-r1" in JUDGE else 120
print(f"available={avail}\nJUDGE={JUDGE} num_predict={NUMPRED} OUT={OUT}", flush=True)

RUBRIC = (
    "You are an impartial senior trading-desk reviewer. A junior analyst wrote two "
    "critiques (A and B) of the SAME completed trade. The trade context below ALREADY "
    "states the realized P&L, so this is about critique QUALITY, not predicting outcome.\n"
    "Judge on: (1) consistency with the stated outcome, (2) specific, non-generic insight, "
    "(3) actionable improvement, (4) appropriate concision, (5) no fabricated facts.\n\n"
    "TRADE CONTEXT:\n{ctx}\n\nCRITIQUE A:\n{a}\n\nCRITIQUE B:\n{b}\n\n"
    'Reply with ONLY strict JSON, no prose: {{"winner":"A"|"B"|"tie","reason":"<=15 words"}}'
)

def judge(ctx, a, b, retries=2):
    prompt = RUBRIC.format(ctx=ctx, a=a, b=b)
    body = json.dumps({"model": JUDGE, "prompt": prompt, "stream": False,
                       "think": False, "options": {"temperature": 0.0, "num_predict": NUMPRED}}).encode()
    for att in range(retries+1):
        try:
            req = urllib.request.Request("http://localhost:11434/api/generate", data=body,
                                         headers={"Content-Type": "application/json"})
            txt = json.load(urllib.request.urlopen(req, timeout=180))["response"]
            txt = re.sub(r"<think>.*?</think>", "", txt, flags=re.S)
            m = re.search(r'\{[^{}]*"winner"[^{}]*\}', txt, re.S)
            if m:
                w = json.loads(m.group(0))
                if w.get("winner") in ("A", "B", "tie"):
                    return w
            mm = re.search(r'"winner"\s*:\s*"(A|B|tie)"', txt)
            if mm:
                return {"winner": mm.group(1), "reason": "(regex-extracted)"}
        except Exception as e:
            if att == retries:
                return {"winner": "error", "reason": str(e)[:80]}
            time.sleep(2)
    return {"winner": "error", "reason": "unparseable"}

def verdict_token(txt):
    m = re.search(r"\b(WIN|LOSS)\b", txt or "", re.I)
    return m.group(1).upper() if m else None

data = json.load(open(GEN))
tally = {"v1": 0, "v6_eval": 0, "tie": 0, "error": 0}
by_cat = {}
vd = {"v1_ok": 0, "v6_eval_ok": 0, "n": 0}

t0 = time.time()
for rec in data:
    i = rec["idx"]
    # order-balance: even idx -> A=v1,B=v6_eval ; odd -> A=v6_eval,B=v1
    if i % 2 == 0:
        A_key, B_key = "v1", "v6_eval"
    else:
        A_key, B_key = "v6_eval", "v1"
    res = judge(rec["prompt"], rec["gen"][A_key], rec["gen"][B_key])
    w = res["winner"]
    winner = {"A": A_key, "B": B_key}.get(w, w)  # "tie"/"error" pass through
    rec["judge"] = {"winner_model": winner, "raw": w, "reason": res.get("reason", ""), "A": A_key, "B": B_key}
    tally[winner] = tally.get(winner, 0) + 1
    cat = rec["meta"].get("category", "unknown")
    c = by_cat.setdefault(cat, {"v1": 0, "v6_eval": 0, "tie": 0, "error": 0})
    c[winner] = c.get(winner, 0) + 1
    # verdict-direction sanity (rows with a labeled verdict)
    gt = rec["meta"].get("verdict")
    if gt in ("WIN", "LOSS"):
        vd["n"] += 1
        if verdict_token(rec["gen"]["v1"]) == gt: vd["v1_ok"] += 1
        if verdict_token(rec["gen"]["v6_eval"]) == gt: vd["v6_eval_ok"] += 1
    if (i+1) % 25 == 0:
        print(f"  judged {i+1}/{len(data)}  running={tally}", flush=True)

json.dump({"judge_model": JUDGE, "tally": tally, "by_category": by_cat,
           "verdict_dir": vd, "records": data}, open(OUT, "w"))

n = len(data)
scored = n - tally["error"]
print("\n================ BAKE-OFF RESULT (v6-eval vs v1, 178 held-out) ================")
print(f"Judge model : {JUDGE}")
print(f"Scored      : {scored}/{n}  (judge_errors={tally['error']})")
print(f"v6-eval wins: {tally['v6_eval']}")
print(f"v1 wins     : {tally['v1']}")
print(f"ties        : {tally['tie']}")
if scored - tally["tie"] > 0:
    wr = tally["v6_eval"] / (tally["v6_eval"] + tally["v1"]) * 100
    print(f"v6-eval win-rate (excl. ties): {wr:.1f}%  [{tally['v6_eval']}/{tally['v6_eval']+tally['v1']}]")
print("\nBy category (v6_eval / v1 / tie / err):")
for cat, c in sorted(by_cat.items()):
    print(f"  {cat:18s} {c['v6_eval']:>3} / {c['v1']:>3} / {c['tie']:>3} / {c['error']:>3}")
if vd["n"]:
    print(f"\nVerdict-direction sanity (n={vd['n']}): v1 {vd['v1_ok']}/{vd['n']}  v6-eval {vd['v6_eval_ok']}/{vd['n']}")
print(f"\nelapsed={time.time()-t0:.0f}s | saved {OUT}")
print("DONE_BAKEOFF")
