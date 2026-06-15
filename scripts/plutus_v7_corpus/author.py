"""HM-PLUTUS-V7-CORPUS Phase A2 — author REFERENCE-QUALITY trade critiques with Grok (xAI).

Direct, read-only outbound call to xAI /v1/chat/completions (replicates engine.team_advisor_grok
mechanics) — does NOT call run_grok_subadvisor() (that writes to trader.db). Logs exact token counts +
cost (usage.cost_in_usd_ticks). Authoring instruction maps 1:1 to the eval rubric: name THE decisive
risk, give concrete actionable fixes (stop/size/exit), be outcome-aware (NEVER praise a loss), no
input-restatement, lead with Verdict, be concise.

Pilot: `python3 author.py --pilot 36`  (stratified across outcome/regime/ticker/agent).
Idempotent: skips scenarios already in authored_pilot.jsonl.
"""
from __future__ import annotations
import os, sys, re, json, time, urllib.request, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common  # noqa: E402
from common import read_jsonl, write_jsonl  # noqa: E402

POOL = os.path.join(common.DATA, "plutus_v7", "scenario_pool.jsonl")
AUTHORED = os.path.join(common.DATA, "plutus_v7", "authored_pilot.jsonl")
COST_LOG = os.path.join(common.DATA, "plutus_v7", "author_cost_log.jsonl")
REPO = common.REPO
XAI_URL = "https://api.x.ai/v1/chat/completions"
MODEL = os.environ.get("GROK_MODEL", "grok-4.20-0309-non-reasoning")

AUTHOR_SYSTEM = (
    "You are Plutus, the elite financial intelligence officer for OllieTrades — a ruthless, "
    "outcome-aware post-trade critic. You are writing REFERENCE-QUALITY training targets. For each "
    "completed trade you receive full context and the REALIZED P&L. Write a tight critique that:\n"
    "1. Leads with `Verdict: WIN` or `Verdict: LOSS` matching the realized P&L sign (flat/tiny = the "
    "sign of P&L).\n"
    "2. Names THE SINGLE DECISIVE RISK that actually drove or threatened the realized outcome, tied to "
    "this trade's specifics (not a generic list).\n"
    "3. Gives 1-2 CONCRETE, ACTIONABLE fixes — specific stop placement, position-size change, or exit/"
    "scale rule (numbers where possible).\n"
    "4. Is OUTCOME-AWARE: NEVER praise a losing trade; NEVER call a loss 'well-executed'. On a win, still "
    "name the risk that could have flipped it.\n"
    "5. Does NOT restate or echo the input context — add NEW analysis only.\n"
    "6. Is concise: 3-5 sentences, no headers, no bullet padding.\n"
    'Return ONLY a JSON object: {"decisive_risk": "<one phrase>", "critique": "<the critique text, '
    'starting with Verdict:>"}'
)


def load_key():
    for k in ("XAI_API_KEY", "GROK_API_KEY"):
        if os.environ.get(k):
            return os.environ[k]
    env = os.path.join(REPO, ".env")
    if os.path.exists(env):
        for line in open(env):
            m = re.match(r"\s*(XAI_API_KEY|GROK_API_KEY)\s*=\s*(.+)\s*$", line)
            if m:
                return m.group(2).strip().strip('"').strip("'")
    raise SystemExit("xAI API key not found (XAI_API_KEY/GROK_API_KEY in env or .env)")


def call_grok(key, user_prompt, max_tokens=600, timeout=90):
    payload = {"model": MODEL, "temperature": 0.4, "max_tokens": max_tokens,
               "messages": [{"role": "system", "content": AUTHOR_SYSTEM},
                            {"role": "user", "content": user_prompt}]}
    req = urllib.request.Request(XAI_URL, data=json.dumps(payload).encode(),
                                 headers={"Authorization": f"Bearer {key}",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read().decode())
    content = d["choices"][0]["message"]["content"]
    u = d.get("usage", {})
    itok = u.get("prompt_tokens", len(user_prompt) // 4)
    otok = u.get("completion_tokens", len(content) // 4)
    ticks = u.get("cost_in_usd_ticks")
    cost = (float(ticks) * 1e-10) if ticks is not None else (itok * 1.25 + otok * 2.50) / 1e6
    return content, itok, otok, cost


def parse_authored(raw):
    s = re.sub(r"```json|```", "", raw or "").strip()
    m = re.search(r"\{.*\}", s, re.S)
    if m:
        try:
            o = json.loads(m.group(0))
            if o.get("critique"):
                return o.get("decisive_risk"), o["critique"].strip()
        except Exception:
            pass
    return None, (raw or "").strip()  # fallback: whole text as critique


def stratified_pick(pool, n):
    """Deterministic spread across outcome x regime, then round-robin distinct tickers/agents."""
    by = collections.defaultdict(list)
    for s in pool:
        by[(s["outcome"], s["regime"])].append(s)
    for k in by:
        by[k].sort(key=lambda s: (str(s["symbol"]), s["id"]))
    # ensure loss coverage (where v6 was over-agreeable): weight outcomes win/loss/flat ~ 45/40/15
    quota_outcome = {"win": round(n * 0.45), "loss": round(n * 0.40), "flat": n - round(n*0.45) - round(n*0.40)}
    picked, seen_sym = [], collections.Counter()
    for outcome, oquota in quota_outcome.items():
        cells = sorted([k for k in by if k[0] == outcome], key=str)
        ci = 0
        while oquota > 0 and cells:
            k = cells[ci % len(cells)]
            # pick the next unused scenario in this cell preferring an unseen ticker
            cand = next((s for s in by[k] if s not in picked and seen_sym[s["symbol"]] == 0), None) \
                or next((s for s in by[k] if s not in picked), None)
            if cand:
                picked.append(cand); seen_sym[cand["symbol"]] += 1; oquota -= 1
            ci += 1
            if ci > len(cells) * 50:
                break
    return picked


def main(n):
    pool = read_jsonl(POOL)
    done = {r["id"]: r for r in read_jsonl(AUTHORED)} if os.path.exists(AUTHORED) else {}
    targets = stratified_pick(pool, n)
    key = load_key()
    out = list(done.values())
    costs = read_jsonl(COST_LOG) if os.path.exists(COST_LOG) else []
    tot_in = tot_out = 0
    tot_cost = sum(c.get("cost", 0) for c in costs)
    authored = 0
    for s in targets:
        if s["id"] in done:
            continue
        try:
            raw, itok, otok, cost = call_grok(key, s["prompt"])
        except Exception as e:
            print(f"  [grok ERROR] {s['id']}: {type(e).__name__}: {e!r}")
            continue
        dr, crit = parse_authored(raw)
        out.append({"id": s["id"], "trade_id": s.get("trade_id"), "symbol": s["symbol"],
                    "outcome": s["outcome"], "regime": s["regime"], "agent": s["agent"],
                    "realized_verdict": s["realized_verdict"], "realized_pnl_pct": s["realized_pnl_pct"],
                    "decisive_risk": dr, "critique": crit, "author_model": MODEL})
        costs.append({"id": s["id"], "in_tok": itok, "out_tok": otok, "cost": cost})
        tot_in += itok; tot_out += otok; tot_cost += cost; authored += 1
        write_jsonl(AUTHORED, out); write_jsonl(COST_LOG, costs)
        print(f"  authored {s['id']} {s['symbol']} {s['outcome']} | {itok}+{otok}tok ${cost:.5f}")
        time.sleep(0.3)
    oc = collections.Counter(r["outcome"] for r in out)
    print(f"\n[author] pilot total authored={len(out)} (new this run {authored}); outcomes={dict(oc)}")
    print(f"[author] tokens this run in={tot_in} out={tot_out}; CUMULATIVE author cost=${tot_cost:.4f} "
          f"(model {MODEL})")


if __name__ == "__main__":
    n = int(sys.argv[sys.argv.index("--pilot") + 1]) if "--pilot" in sys.argv else 36
    main(n)
