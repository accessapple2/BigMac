#!/usr/bin/env python3
"""HM-FORGE Phase 1.3 — War-Room-style model bake-off harness.

Runs ON Ollie Max (.168). Replays a FIXED, frozen War-Room CSP-debate prompt
(representative of McCoy's high-VIX CSP verdict turn) across N models via the
local Ollama HTTP API with format=json, and emits a single markdown scorecard.

Captured per model: eval tok/s, wall-clock per turn, JSON-validity rate on the
structured-output schema, VRAM peak (nvidia-smi sampled during the run).

  ssh olliemax 'python3 /tmp/hm_forge_bench.py \
      --models plutus-v1:latest,gpt-oss:20b[,gemma4:12b-it-qat] --runs 5' \
      > docs/HM-FORGE-BENCH-SCORECARD.md

NOTE ON FIDELITY: this uses a frozen prompt, NOT a live engine.debate_engine
replay (which needs live market/positions context and would import heavy engine
modules). It measures the model layer apples-to-apples; a true engine-replay is
a follow-up if exact-context fidelity is required.

OPERATIONAL: loading a 13GB model (gpt-oss:20b) EVICTS the live fleet from the
RTX 5080's 16GB. Run ONLY in a market-closed window. Each model is loaded, the
fleet's keep_alive will re-warm afterwards.
"""
import argparse, json, subprocess, time, urllib.request, statistics

OLLAMA = "http://127.0.0.1:11434/api/chat"

# Frozen War-Room CSP debate turn — McCoy-style high-VIX cash-secured-put verdict.
SYS = (
    "You are a disciplined options-income strategist on a trading war-room panel. "
    "Given a cash-secured-put candidate, return a STRICT JSON verdict only."
)
USER = (
    "Ticker NVDA at $172.40, IV rank 61, VIX 19.8, regime CAUTIOUS-BULL. "
    "Candidate: sell the 30-delta cash-secured put, 32 DTE, premium $4.85, "
    "strike $160. Earnings in 41 days (outside expiry). "
    "Decide whether to SELL, PASS, or WAIT. Respond with JSON only, keys: "
    '{"verdict":"SELL|PASS|WAIT","conviction":0.0-1.0,"reason":"<=20 words"}'
)
SCHEMA_KEYS = {"verdict", "conviction", "reason"}
VALID_VERDICTS = {"SELL", "PASS", "WAIT"}


def vram_used_mib() -> int:
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10).stdout.strip().splitlines()
        return max(int(x) for x in out if x.strip())
    except Exception:
        return -1


def one_call(model: str) -> dict:
    body = json.dumps({
        "model": model,
        "messages": [{"role": "system", "content": SYS},
                     {"role": "user", "content": USER}],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.2, "num_ctx": 4096},
    }).encode()
    t0 = time.monotonic()
    req = urllib.request.Request(OLLAMA, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=180) as r:
        resp = json.load(r)
    wall = time.monotonic() - t0
    ec = resp.get("eval_count", 0)
    ed = resp.get("eval_duration", 1) or 1          # ns
    tok_s = ec / (ed / 1e9) if ed else 0.0
    content = resp.get("message", {}).get("content", "")
    valid = False
    try:
        obj = json.loads(content)
        valid = SCHEMA_KEYS.issubset(obj) and obj.get("verdict") in VALID_VERDICTS \
            and isinstance(obj.get("conviction"), (int, float))
    except Exception:
        pass
    return {"wall": wall, "tok_s": tok_s, "valid": valid}


def bench(model: str, runs: int) -> dict:
    # warm the model once (load), then measure `runs` turns
    try:
        one_call(model)
    except Exception as e:
        return {"model": model, "error": f"{type(e).__name__}: {e}"}
    walls, toks, valids, peak = [], [], 0, 0
    for _ in range(runs):
        peak = max(peak, vram_used_mib())
        try:
            m = one_call(model)
        except Exception as e:
            return {"model": model, "error": f"{type(e).__name__}: {e}", "partial": len(walls)}
        peak = max(peak, vram_used_mib())
        walls.append(m["wall"]); toks.append(m["tok_s"]); valids += m["valid"]
    return {
        "model": model, "runs": runs,
        "tok_s": round(statistics.mean(toks), 1),
        "wall_med": round(statistics.median(walls), 2),
        "json_valid_pct": round(100 * valids / runs, 1),
        "vram_peak_mib": peak,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", required=True, help="comma-separated ollama tags")
    ap.add_argument("--runs", type=int, default=5)
    a = ap.parse_args()
    rows = [bench(m.strip(), a.runs) for m in a.models.split(",") if m.strip()]
    ts = subprocess.run(["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"], capture_output=True, text=True).stdout.strip()
    print(f"# HM-FORGE Phase 1.3 — War-Room Bake-off Scorecard\n")
    print(f"_Captured {ts} on Ollie Max (.168, RTX 5080 16GB), {a.runs} runs/model, "
          f"frozen NVDA CSP debate prompt, format=json._\n")
    print("| Model | tok/s | median wall (s) | JSON-valid % | VRAM peak (MiB) |")
    print("|---|--:|--:|--:|--:|")
    for r in rows:
        if "error" in r:
            print(f"| {r['model']} | ERROR | — | — | {r.get('error','')} |")
        else:
            print(f"| {r['model']} | {r['tok_s']} | {r['wall_med']} | "
                  f"{r['json_valid_pct']} | {r['vram_peak_mib']} |")


if __name__ == "__main__":
    main()
