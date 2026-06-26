#!/usr/bin/env python3
"""HM-BM-BAKEOFF Phase 2 — run the 3-candidate critic bakeoff over the corpus.

Reads  : data/bm_corpus_v1.jsonl  (101 production-matching closed trades, seed=42)
Writes : data/bm_critiques_v1.jsonl  (one row per (trade_id, model) critique)
Target : OLLIE_URL (Ollie Max RTX 5080, .168) — same box the live Critic uses.

Design (fair relative bakeoff):
  - Each candidate model critiques the ENTRY decision of each trade with the
    OUTCOME WITHHELD (no exit_price / realized_pnl / outcome in the prompt).
    The model returns SCORE 1-10 + VERDICT(WIN|LOSS) + REASONING, mirroring the
    production engine/scout_critic.py format so results are comparable to live.
  - We record the blind prediction alongside the *actual* outcome so Phase 3 can
    score each model (verdict accuracy, score↔outcome separation, latency).
  - 3 models × 101 trades = 303 critiques.
  - Determinism: temperature=0, seed=42, qwen3 think-disabled via {"think":False}
    (top-level key — the only reliable disable; /no_think is unreliable).

NEVER DELETES (Captain doctrine): an existing output is archived (.bak_<ts>)
before a fresh full run. Default mode is RESUMABLE — already-written
(trade_id, model) pairs are skipped and appended to, so a crash mid-run costs
nothing. Use --fresh to archive + restart.

Usage:
  python3 scripts/run_bm_bakeoff.py                # resume/continue (default)
  python3 scripts/run_bm_bakeoff.py --fresh        # archive existing, start over
  python3 scripts/run_bm_bakeoff.py --models a,b,c # override candidate set
  python3 scripts/run_bm_bakeoff.py --dry-run      # warm + 1 critique/model, no full run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
sys.path.insert(0, str(REPO))
from config import OLLIE_URL  # noqa: E402  — Ollie Max .168

CORPUS = REPO / "data" / "bm_corpus_v1.jsonl"
OUT = REPO / "data" / "bm_critiques_v1.jsonl"
GEN_URL = f"{OLLIE_URL}/api/generate"

# ── Candidate set ───────────────────────────────────────────────────────────
# plutus-v1 = incumbent McCoy finance brain (config.py ollama-plutus → plutus-v1)
# qwen3:8b  = current live engine/scout_critic.py CRITIC_MODEL
# qwen3:14b = larger challenger (original spec candidate; Captain-confirmed 2026-06-05)
CANDIDATES = ["plutus-v1:latest", "qwen3:8b", "qwen3:14b"]

SEED = 42
CRITIC_TIMEOUT = 90          # per-critique HTTP timeout (s)
KEEP_ALIVE = "30m"           # keep each model resident across its 101 critiques
WARM_TIMEOUT = 180           # cold 14B-class load can be slow


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def load_corpus() -> list[dict]:
    if not CORPUS.exists():
        sys.exit(f"ERROR: corpus not found: {CORPUS}")
    rows = [json.loads(l) for l in CORPUS.read_text().splitlines() if l.strip()]
    if not rows:
        sys.exit("ERROR: corpus is empty")
    return rows


def build_prompt(t: dict) -> str:
    """Entry-decision critique prompt — OUTCOME WITHHELD.

    Mirrors engine/scout_critic.py: SCORE 1-10 + REASONING, plus a VERDICT so we
    can measure blind win/loss calibration against the real result.
    """
    opt = ""
    if t.get("asset_type") == "option":
        opt = (f"OPTION: type={t.get('option_type')} strike={t.get('strike_price')} "
               f"expiry={t.get('expiry_date')}\n")
    return (
        "You are the Critic, a disciplined finance risk reviewer. Evaluate the "
        "ENTRY decision below on its merits at the moment of entry. You do NOT "
        "know the outcome.\n\n"
        f"TICKER: {t.get('symbol')} | ASSET: {t.get('asset_type')} | "
        f"STRATUM: {t.get('stratum')}\n"
        f"{opt}"
        f"ENTRY PRICE: {t.get('entry_price')} | QTY: {t.get('qty')} | "
        f"CONFIDENCE: {t.get('confidence')}\n"
        f"STRATEGY TAGS: {t.get('strategy_tags') or '(none)'}\n"
        f"ENTRY REASONING: {t.get('reasoning') or '(none recorded)'}\n\n"
        "Judge thesis strength, risk/reward, and whether the entry was "
        "disciplined. Respond ONLY with:\n"
        "SCORE: [1-10]\n"
        "VERDICT: [WIN|LOSS]\n"
        "REASONING: [2 sentences max]"
    )


def parse_response(text: str) -> dict:
    """Tolerant parse — handles plain and Markdown-decorated output.

    Models vary in formatting: 'SCORE: 4', '**SCORE:** 4/10', '- Score = 4'.
    We regex the whole text case-insensitively so a model is never penalized
    (ok=False) for cosmetic formatting — that would silently tilt the bakeoff.
    """
    import re
    score, verdict, reasoning = None, None, None
    m = re.search(r"SCORE\W{0,4}(\d{1,2})", text, re.IGNORECASE)
    if m:
        score = max(1, min(10, int(m.group(1))))
    m = re.search(r"VERDICT\W{0,6}(WIN|LOSS)", text, re.IGNORECASE)
    if m:
        verdict = m.group(1).upper()
    m = re.search(r"REASONING\W{0,4}(.+)", text, re.IGNORECASE | re.DOTALL)
    if m:
        reasoning = m.group(1).strip().lstrip("*# ").strip()[:400]
    if reasoning is None:
        reasoning = text.strip()[:400]
    return {"score": score, "verdict": verdict, "reasoning": reasoning}


def _overrides_for(model: str) -> dict:
    """Per-model request overrides (substring match on the model tag).

    Non-default models need a different request shape to be evaluated fairly:
      - gpt-oss : a reasoning-effort system line (it's a reasoning model)
      - gemma4  : a 32k context so the critique prompt isn't 4k-truncated
    All other models keep the uniform payload.
    """
    ov: dict = {}
    if "gpt-oss" in model:
        ov["system"] = "Reasoning: medium"
    if "gemma4" in model:
        ov["num_ctx"] = 32768
    return ov


def _gen(model: str, prompt: str, timeout: int,
         keep_alive: str | int = KEEP_ALIVE) -> dict:
    ov = _overrides_for(model)
    options = {"temperature": 0, "seed": SEED}
    if "num_ctx" in ov:
        options["num_ctx"] = ov["num_ctx"]
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "think": False,                       # qwen3 think-disable (only reliable key)
        "keep_alive": keep_alive,
        "options": options,
    }
    if "system" in ov:
        payload["system"] = ov["system"]
    r = requests.post(GEN_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def warm(model: str) -> None:
    """Warm a SINGLE model immediately before its batch.

    VRAM sequencing: we deliberately do NOT warm all candidates up front —
    gpt-oss:20b alone is ~14GB on the 16GB 5080, so co-loading would overflow.
    One model resident at a time; unload() evicts before the next warm.
    """
    t0 = time.time()
    try:
        _gen(model, "ready", timeout=WARM_TIMEOUT)
        print(f"  [warm OK ] {model:22s} {time.time()-t0:6.1f}s")
    except Exception as e:
        print(f"  [warm FAIL] {model:22s} {e}")
        sys.exit(f"ERROR: cannot warm {model}; aborting before burning a partial run")


def unload(model: str) -> None:
    """Evict a model from VRAM (keep_alive=0) so the next loads alone."""
    try:
        _gen(model, "bye", timeout=30, keep_alive=0)
        print(f"  [unload  ] {model:22s}")
    except Exception as e:                     # non-fatal — next warm reclaims VRAM anyway
        print(f"  [unload? ] {model:22s} {type(e).__name__}: {e}")


def critique(model: str, t: dict) -> dict:
    t0 = time.time()
    rec = {
        "trade_id": t["trade_id"], "model": model, "stratum": t.get("stratum"),
        "symbol": t.get("symbol"), "asset_type": t.get("asset_type"),
        "actual_outcome": t.get("outcome"), "actual_pnl": t.get("realized_pnl"),
        "ts": _now(),
    }
    try:
        raw = _gen(model, build_prompt(t), timeout=CRITIC_TIMEOUT).get("response", "")
        rec.update(parse_response(raw))
        rec["ok"] = rec.get("score") is not None
        rec["error"] = None
    except Exception as e:
        rec.update({"score": None, "verdict": None, "reasoning": None,
                    "ok": False, "error": str(e)[:200]})
    rec["latency_s"] = round(time.time() - t0, 2)
    return rec


def done_pairs() -> set[tuple]:
    if not OUT.exists():
        return set()
    seen = set()
    for line in OUT.read_text().splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
            if r.get("ok"):                   # only completed-OK pairs count as done
                seen.add((r["trade_id"], r["model"]))
        except Exception:
            pass
    return seen


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", help="comma-separated override of candidate set")
    ap.add_argument("--fresh", action="store_true", help="archive existing output, restart")
    ap.add_argument("--dry-run", action="store_true", help="warm + 1 critique/model only")
    args = ap.parse_args()

    models = [m.strip() for m in args.models.split(",")] if args.models else list(CANDIDATES)
    corpus = load_corpus()

    print("══════ HM-BM-BAKEOFF Phase 2 ══════")
    print(f"  corpus   : {len(corpus)} trades")
    print(f"  models   : {models}")
    print(f"  target   : {len(corpus) * len(models)} critiques → {OUT.name}")

    if args.fresh and OUT.exists():           # NEVER DELETE — archive then restart
        bak = OUT.with_name(f"{OUT.name}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        os.rename(OUT, bak)
        print(f"  [archive] existing output -> {bak.name}")

    if args.dry_run:
        print(f"[{_now()}] DRY RUN — warm + 1 critique per model (sequential VRAM)")
        for m in models:
            warm(m)
            r = critique(m, corpus[0])
            print(f"  {m:22s} score={r.get('score')} verdict={r.get('verdict')} "
                  f"{r['latency_s']}s ok={r['ok']} :: {(r.get('reasoning') or r.get('error'))[:70]}")
            unload(m)
        return

    seen = done_pairs()
    if seen:
        print(f"  [resume] {len(seen)} (trade,model) pairs already done — skipping them")

    total = len(corpus) * len(models)
    n_done = len(seen)
    fails = 0
    # append mode — every critique flushed immediately (crash-safe, never-delete)
    with open(OUT, "a") as f:
        for m in models:
            pending = [t for t in corpus if (t["trade_id"], m) not in seen]
            if not pending:                    # whole model already done on resume
                continue
            warm(m)                            # only this model resident for its batch
            for t in pending:
                rec = critique(m, t)
                f.write(json.dumps(rec) + "\n")
                f.flush()
                os.fsync(f.fileno())
                n_done += 1
                if not rec["ok"]:
                    fails += 1
                if n_done % 10 == 0 or not rec["ok"]:
                    print(f"  [{n_done:3d}/{total}] {m:18s} tid={rec['trade_id']} "
                          f"score={rec.get('score')} v={rec.get('verdict')} "
                          f"{rec['latency_s']}s{' FAIL' if not rec['ok'] else ''}")
            unload(m)                          # evict before the next model warms

    print(f"[{_now()}] DONE — {n_done}/{total} critiques in {OUT.name} ({fails} failed)")
    if fails:
        print("  Re-run without --fresh to retry the failed pairs (resumable).")


if __name__ == "__main__":
    main()
