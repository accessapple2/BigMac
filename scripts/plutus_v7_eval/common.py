"""HM-PLUTUS-V7-EVAL — shared config + helpers (read-only on existing data, create-only output).

Sacred-data rule: this package NEVER writes to trader.db / arena.db / the plutus corpus /
scripts/plutus_v6/. All output goes under data/plutus_eval/. Inference is .168-only.
"""
from __future__ import annotations
import json, os, re, time, urllib.request, urllib.error

# ---- paths -----------------------------------------------------------------
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA = os.path.join(REPO, "data")
EVAL_DIR = os.path.join(DATA, "plutus_eval")
TEST_SET = os.path.join(DATA, "plutus_corpus_v6.test.jsonl")
TRAIN_SET = os.path.join(DATA, "plutus_corpus_v6.train.jsonl")
VAL_SET = os.path.join(DATA, "plutus_corpus_v6.val.jsonl")
BAKEOFF_GEN = os.path.join(REPO, "scripts", "plutus_v6", "results", "bakeoff_gen.json")

OOS_SET = os.path.join(EVAL_DIR, "oos_set_v1.jsonl")
GEN_FILE = os.path.join(EVAL_DIR, "gen_v1_v6.jsonl")
JUDGE_RAW = os.path.join(EVAL_DIR, "judge_raw.jsonl")
SCORECARD = os.path.join(EVAL_DIR, "scorecard_v1_v6.json")

# ---- .168 ollama -----------------------------------------------------------
OLLAMA = os.environ.get("PLUTUS_EVAL_OLLAMA", "http://192.168.1.168:11434")
KEEP_ALIVE = "30s"                      # short — shared box, never pin
PLUTUS_MODELS = {"v1": "plutus-v1:latest", "v6": "plutus-v6-eval:latest"}
JUDGES = ["qwen3:14b", "gpt-oss:20b"]   # same pair as the v6 bakeoff
# Per-judge inference config. qwen3 honors top-level think:false (clean JSON @ small budget).
# gpt-oss ALWAYS reasons (into the `thinking` field, which counts against num_predict); think:'low'
# keeps its reasoning short (~870 chars) so a modest budget fits reasoning + the JSON answer.
JUDGE_CFG = {
    "qwen3:14b":   {"think": False, "num_predict": 350},
    "gpt-oss:20b": {"think": "low", "num_predict": 700},
}

# ---- rubric ----------------------------------------------------------------
DIMS = ["risk_id", "directional_lean", "calibration",
        "actionability", "non_redundancy", "format_concision"]
DIM_HELP = {
    "risk_id":          "0=misses the decisive risk; 1=names a risk generically; 2=names THE risk that mattered and why",
    "directional_lean": "0=lean wrong vs realized outcome; 1=hedged/ambiguous; 2=correct lean, justified",
    "calibration":      "0=confident-and-wrong / praises a losing trade; 1=mild confidence-outcome mismatch; 2=confidence matches outcome",
    "actionability":    "0=platitude only ('monitor closely'); 1=one vague lever; 2=>=1 concrete specific lever (stop/size/exit/confirm)",
    "non_redundancy":   "0=mostly restates the prompt; 1=some new analysis; 2=adds real new analysis, minimal echo",
    "format_concision": "0=unparseable or bloated; 1=minor drift; 2=leads with a Verdict, parseable, tight",
}

# ---- io --------------------------------------------------------------------
def read_jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ---- ollama generate (inference only) --------------------------------------
def ollama_generate(model, prompt, num_predict=512, temperature=0.3, system=None,
                    timeout=300, retries=2, think=None):
    body = {"model": model, "prompt": prompt, "stream": False,
            "keep_alive": KEEP_ALIVE,
            "options": {"temperature": temperature, "num_predict": num_predict}}
    if system:
        body["system"] = system
    if think is not None:        # qwen3/gpt-oss: top-level think:false disables reasoning
        body["think"] = think
    data = json.dumps(body).encode()
    last = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(OLLAMA + "/api/generate", data=data,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode()).get("response", "")
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"ollama_generate failed for {model}: {last!r}")

# ---- realized-outcome parsing (read-only, from prompt/_meta) ----------------
_PNL_RE = re.compile(r"Realized P&L:\s*(-?\d+(?:\.\d+)?)\s*\(\s*(-?\d+(?:\.\d+)?)%\)")

def parse_realized(prompt, meta):
    """Return (verdict, pnl, pnl_pct) from the prompt text, falling back to _meta.verdict."""
    m = _PNL_RE.search(prompt or "")
    pnl = float(m.group(1)) if m else None
    pct = float(m.group(2)) if m else None
    verdict = meta.get("verdict")
    if verdict is None and pnl is not None:
        verdict = "WIN" if pnl > 0 else "LOSS"
    return verdict, pnl, pct
