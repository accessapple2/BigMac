#!/usr/bin/env python3
"""HM-PLUTUS-V6-CORPUS — FREE-source assembly (DATA ONLY, NO training).

Decision 2026-06-07: do NOT re-subscribe rallie.ai. Build v6 from free sources.
This script ASSEMBLES the corpus + splits to disk so the only remaining step is
scheduling a GPU training window. It does NOT train and does NOT touch the live
fleet's GPU.

Sources (per drafts/HM-PLUTUS-V6-CORPUS.md, free-build variant):
  1. Own realized outcomes — trader.db trade critiques (ground truth, most relevant).
     Reuses the proven extractor scripts/extract_plutus_corpus_v2.build_trade_critique_corpus().
  2. Plutus v1 critique reviews (target 500-1,000) — PENDING. This is a FORWARD
     collection (review v1's live critiques over 2-3 weeks); there is no critique-log
     table to batch-harvest today, so it contributes 0 now and fills in later.
  3. Retain best v4/v5 (~300) — regression guard. Per the v6 spec "no identity
     tiling", we EXCLUDE persona/identity examples (identity moves to the Modelfile
     SYSTEM prompt) and retain domain examples (signal_analysis, market_qa,
     rallie_review) + a capped set of strong trade critiques.

Output: data/plutus_corpus_v6.{jsonl,train,val,test} + plutus_corpus_v6.stats.json
"""
from __future__ import annotations
import json, re, random, hashlib, importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
random.seed(42)  # deterministic split (no Date/random-at-runtime surprises)

RETAIN_CAP = 300
EXCLUDE_CATEGORIES = {"persona", "identity"}  # v6: identity → Modelfile SYSTEM prompt
_AUTO_TAG = re.compile(r"\b(AUTO-STOP|AUTO-TARGET|AUTO-[A-Z_]+)\b")


def _strip(s):
    return _AUTO_TAG.sub("", s).strip() if isinstance(s, str) else s


def _clean(ex):
    for k in ("prompt", "completion", "instruction", "input", "output"):
        if k in ex:
            ex[k] = _strip(ex[k])
    return ex


def _key(ex):
    base = (ex.get("prompt") or ex.get("instruction") or "") + (ex.get("input") or "")
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


# ── Source 1: own realized outcomes (fresh from current trader.db) ──
spec = importlib.util.spec_from_file_location("ev2", ROOT / "scripts" / "extract_plutus_corpus_v2.py")
ev2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ev2)
src1 = [_clean(e) for e in ev2.build_trade_critique_corpus()]

# ── Source 2: PENDING (forward-collection) ──
src2 = []

# ── Source 3: retain v4/v5 (exclude identity; cap RETAIN_CAP) ──
src3 = []
v5_path = DATA / "plutus_corpus_v5.jsonl"
if v5_path.exists():
    v5 = []
    for line in open(v5_path):
        line = line.strip()
        if not line:
            continue
        try:
            v5.append(json.loads(line))
        except Exception:
            continue
    # domain (non-trade-critique) first, then strong trade critiques, excluding identity
    domain = [e for e in v5 if e.get("_meta", {}).get("category") not in EXCLUDE_CATEGORIES
              and e.get("_meta", {}).get("category") != "trade_critique"]
    crit = [e for e in v5 if e.get("_meta", {}).get("category") == "trade_critique"]
    retained = domain + crit
    src3 = [_clean(e) for e in retained[:RETAIN_CAP]]

# ── Combine + dedup (Source 1 wins on collision) ──
seen, combined = set(), []
for ex in src1 + src2 + src3:
    k = _key(ex)
    if k in seen:
        continue
    seen.add(k)
    combined.append(ex)
random.shuffle(combined)

n = len(combined)
ntr, nv = int(n * 0.8), int(n * 0.1)
train, val, test = combined[:ntr], combined[ntr:ntr + nv], combined[ntr + nv:]


def _dump(name, rows):
    with open(DATA / f"plutus_corpus_v6.{name}.jsonl", "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


_dump("train", train)
_dump("val", val)
_dump("test", test)
with open(DATA / "plutus_corpus_v6.jsonl", "w") as f:
    for r in combined:
        f.write(json.dumps(r) + "\n")

stats = {
    "total": n,
    "source1_own_outcomes": len(src1),
    "source2_critique_reviews_PENDING": len(src2),
    "source3_retain_v4v5": len(src3),
    "dedup_removed": (len(src1) + len(src2) + len(src3)) - n,
    "split": {"train": len(train), "val": len(val), "test": len(test)},
    "target_2500_gap": max(0, 2500 - n),
    "note": "Source 2 (v1 critique reviews, 500-1000) is a forward-collection — not batchable today; "
            "it closes the gap to the 2,500 target as Plutus v1 critiques accrue. NO TRAINING run by this script.",
}
with open(DATA / "plutus_corpus_v6.stats.json", "w") as f:
    json.dump(stats, f, indent=2)

print(json.dumps(stats, indent=2))
print("\n--- composition by category ---")
import collections
cats = collections.Counter(e.get("_meta", {}).get("category", "?") for e in combined)
for k, v in cats.most_common():
    print(f"  {k}: {v}")
print("\n--- 2 sample examples ---")
for ex in combined[:2]:
    print(json.dumps({k: ex[k] for k in ex if k != "_meta"})[:420], "...")
