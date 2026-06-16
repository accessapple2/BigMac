"""HM-PLUTUS-V8-CORPUS Phase A — author trade_critique targets with Claude Haiku 4.5.

Second author for diversity: forensic post-trade analyst persona (vs Grok's Plutus
intelligence-officer persona). Same rubric constraints, different structural framing.
Direct, read-only Anthropic API call — does NOT touch trader.db. Idempotent.

Usage:
  python3 author_v8_haiku.py             # full run (1030 scenarios)
  python3 author_v8_haiku.py --pilot 20  # smoke test
"""
from __future__ import annotations
import os, sys, re, json, time, threading, urllib.request, collections
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common
from common import read_jsonl

POOL = os.path.join(common.DATA, "plutus_v7", "scenario_pool.jsonl")
OUT  = os.path.join(common.DATA, "plutus_v8", "authored_tc_haiku.jsonl")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-haiku-4-5-20251001"
COST_IN  = 0.80 / 1e6   # $/token input
COST_OUT = 4.00 / 1e6   # $/token output
WORKERS = 5
_lock = threading.Lock()

# Forensic auditor persona — distinct from Grok's "Plutus intelligence officer"
FORENSIC_SYSTEM = (
    "You are a forensic post-trade analyst reviewing completed trades for OllieTrades. "
    "Your role is to produce REFERENCE-QUALITY training examples that teach a critic model "
    "what rigorous, evidence-based trade analysis looks like. For each completed trade you "
    "receive full context and the REALIZED P&L. Write a tight, analytical critique that:\n"
    "1. Leads with `Verdict: WIN` or `Verdict: LOSS` matching the realized P&L sign "
    "(flat/tiny = the sign of P&L).\n"
    "2. Identifies THE SINGLE DECISIVE RISK — the specific, trade-context-bound factor that "
    "most determined the realized outcome. Not a generic category; name the exact mechanism.\n"
    "3. Gives 1-2 CONCRETE, QUANTITATIVE fixes: specific stop placement, sizing adjustments, "
    "or exit triggers with numbers where the data supports them.\n"
    "4. Stays OUTCOME-AWARE: on a loss, dissect what failed without excuses; on a win, name "
    "the risk that could have flipped it. NEVER praise a losing trade or call a loss "
    "'well-managed'.\n"
    "5. Adds NEW insight — do NOT restate or paraphrase the input context.\n"
    "6. Stays tight: 3-5 sentences, no headers, no bullets, no filler phrases.\n"
    "7. VARY your structural opening. BANNED openers (never start with these):\n"
    "   - 'Entry timing caught...' / 'Entry timing nailed...' / 'Entry timing...'\n"
    "   - 'The decisive risk was...' / 'The single decisive risk...'\n"
    "   - 'Despite [regime]...'\n"
    "   Lead instead with one of these frames — rotate across critiques:\n"
    "   (a) the regime/tape context and its specific effect on the trade's thesis\n"
    "   (b) the position's sizing relative to the risk it accepted\n"
    "   (c) the specific signal component that validated or failed first\n"
    "   (d) the quantified gap between expectation and realized outcome\n"
    "   (e) the key decision point the agent faced and how it was resolved\n"
    "   Strong structural diversity across critiques is required.\n"
    'Return ONLY a JSON object: {"decisive_risk": "<one phrase>", "critique": "<critique text '
    'starting with Verdict:>"}'
)


def _load_key():
    for k in ("ANTHROPIC_API_KEY", "CLAUDE_API_KEY"):
        if os.environ.get(k):
            return os.environ[k]
    env = os.path.join(common.REPO, ".env")
    if os.path.exists(env):
        for line in open(env):
            m = re.match(r"\s*(ANTHROPIC_API_KEY|CLAUDE_API_KEY)\s*=\s*(.+)", line)
            if m:
                return m.group(2).strip().strip('"').strip("'")
    raise SystemExit("ANTHROPIC_API_KEY not found in env or .env")


def call_haiku(key, user_prompt, max_tokens=600, timeout=90):
    payload = {"model": MODEL, "max_tokens": max_tokens, "temperature": 0.7,
               "system": FORENSIC_SYSTEM,
               "messages": [{"role": "user", "content": user_prompt}]}
    req = urllib.request.Request(
        ANTHROPIC_URL, data=json.dumps(payload).encode(),
        headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read().decode())
    text = d["content"][0]["text"] if d.get("content") else ""
    u = d.get("usage", {})
    itok = u.get("input_tokens", len(user_prompt) // 4)
    otok = u.get("output_tokens", len(text) // 4)
    cost = itok * COST_IN + otok * COST_OUT
    return text, itok, otok, cost


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
    return None, (raw or "").strip()


def _flush(rows):
    with _lock:
        with open(OUT, "w") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main(n=None):
    pool = read_jsonl(POOL)
    if n:
        pool = pool[:n]
    rows = read_jsonl(OUT) if os.path.exists(OUT) else []
    done = {r["id"] for r in rows}
    todo = [s for s in pool if s["id"] not in done]
    print(f"[haiku_author] pool={len(pool)} done={len(done)} todo={len(todo)} workers={WORKERS}")
    if not todo:
        print("[haiku_author] All done.")
        return
    key = _load_key()

    def work(s):
        raw, itok, otok, cost = call_haiku(key, s["prompt"])
        dr, crit = parse_authored(raw)
        return {"id": s["id"], "trade_id": s.get("trade_id"), "symbol": s["symbol"],
                "outcome": s["outcome"], "regime": s["regime"], "agent": s["agent"],
                "realized_verdict": s["realized_verdict"], "realized_pnl_pct": s["realized_pnl_pct"],
                "decisive_risk": dr, "critique": crit, "author_model": MODEL,
                "in_tok": itok, "out_tok": otok, "cost": cost}

    n_new = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(work, s): s for s in todo}
        for fut in as_completed(futs):
            s = futs[fut]
            try:
                rows.append(fut.result()); n_new += 1
            except Exception as e:
                print(f"  [ERROR] {s['id']}: {type(e).__name__}: {e!r}")
                continue
            if n_new % 25 == 0 or n_new == len(todo):
                _flush(rows)
                cum = sum(r.get("cost") or 0 for r in rows)
                print(f"  {n_new}/{len(todo)} authored; cum cost ${cum:.4f}")

    _flush(rows)
    cum = sum(r.get("cost") or 0 for r in rows)
    oc = collections.Counter(r["outcome"] for r in rows)
    print(f"[haiku_author] DONE rows={len(rows)} (new {n_new}); outcomes={dict(oc)}")
    print(f"[haiku_author] TOTAL cost ${cum:.4f} (model {MODEL})")


if __name__ == "__main__":
    n = int(sys.argv[sys.argv.index("--pilot") + 1]) if "--pilot" in sys.argv else None
    main(n)
