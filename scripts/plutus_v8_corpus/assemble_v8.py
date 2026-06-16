"""HM-PLUTUS-V8-CORPUS Phase C — assemble, diversity report, freeze, split.

Reads quality-filtered scores, applies thresholds (Haiku ≥1.5, Grok ≥1.3 or all Grok
if --no-grok-filter), appends v7 debate_critique + signal_analysis, enforces OOS exclusion,
reports diversity metrics (lexical + optional bge-m3), then writes the frozen corpus.

Usage:
  python3 assemble_v8.py              # full assembly (requires filter_scores.jsonl)
  python3 assemble_v8.py --no-bge     # skip bge-m3 (lexical diversity only)
  python3 assemble_v8.py --no-grok-filter  # include all Grok v7 targets (skip score filter)
"""
from __future__ import annotations
import os, sys, re, json, math, random, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common
from common import DATA, OOS_SET, read_jsonl, write_jsonl

POOL_FILE    = os.path.join(DATA, "plutus_v7", "scenario_pool.jsonl")
HAIKU_FILE   = os.path.join(DATA, "plutus_v8", "authored_tc_haiku.jsonl")
GROK_FILE    = os.path.join(DATA, "plutus_v7", "authored_tc.jsonl")
SCORES_FILE  = os.path.join(DATA, "plutus_v8", "filter_scores.jsonl")
V7_CORPUS    = os.path.join(DATA, "plutus_v7", "plutus_corpus_v7.jsonl")
OUT          = os.path.join(DATA, "plutus_v8", "plutus_corpus_v8.jsonl")
STATS        = os.path.join(DATA, "plutus_v8", "plutus_corpus_v8.stats.json")
TEMPLATE_CAP = 0.02
HAIKU_FLOOR  = 1.5
GROK_FLOOR   = 1.3


def normalize(text):
    s = re.sub(r"[A-Z]{2,}", "S", text or "")
    s = re.sub(r"[-+]?\d[\d,.]*%?", "N", s)
    s = re.sub(r"[^a-zA-Z ]", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()


def opening(text):
    body = (text or "").split("\n", 1)[-1] if "\n" in (text or "") else (text or "")
    return " ".join(re.findall(r"[a-zA-Z]+", body)[:6]).lower()


def dedup_cap(rows, key_fn, cap=TEMPLATE_CAP):
    n = len(rows)
    max_per = max(1, math.ceil(cap * n))
    seen, kept, dropped = collections.Counter(), [], 0
    for r in rows:
        k = key_fn(r)
        if seen[k] >= max_per:
            dropped += 1; continue
        seen[k] += 1; kept.append(r)
    return kept, dropped, max_per


def diversity_report_bge(completions):
    """Compute mean pairwise cosine similarity with bge-m3. Returns None if unavailable."""
    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        return None
    try:
        model = SentenceTransformer("BAAI/bge-m3")
        sample = completions[:500] if len(completions) > 500 else completions
        embs = model.encode(sample, batch_size=32, show_progress_bar=False, normalize_embeddings=True)
        # sample 5000 random pairs from NxN
        rng = random.Random(42)
        n = len(embs)
        pairs = [(rng.randrange(n), rng.randrange(n)) for _ in range(min(5000, n*(n-1)//2))]
        sims = [float(np.dot(embs[i], embs[j])) for i, j in pairs if i != j]
        return round(sum(sims) / len(sims), 4) if sims else None
    except Exception as e:
        print(f"[bge-m3] failed: {e}")
        return None


def main(no_bge=False, no_grok_filter=False):
    pool_by_id = {s["id"]: s for s in read_jsonl(POOL_FILE)}
    oos_prompts = {s["prompt"] for s in read_jsonl(OOS_SET)}

    # --- load quality filter scores ---
    if not no_grok_filter and os.path.exists(SCORES_FILE):
        scores = {(r["id"], r["author"]): r for r in read_jsonl(SCORES_FILE)}
    else:
        scores = {}

    def _passes(sid, author, floor):
        if no_grok_filter and author == "grok":
            return True
        r = scores.get((sid, author))
        return r is not None and r.get("pass", False)

    # --- trade_critique: Haiku keepers ---
    haiku = read_jsonl(HAIKU_FILE) if os.path.exists(HAIKU_FILE) else []
    haiku_rows = []
    for a in haiku:
        s = pool_by_id.get(a["id"])
        if not s or not a.get("critique"):
            continue
        if s["prompt"] in oos_prompts:
            continue
        if not _passes(a["id"], "haiku", HAIKU_FLOOR):
            continue
        haiku_rows.append({"prompt": s["prompt"], "completion": a["critique"],
                           "_meta": {"category": "trade_critique", "trade_id": a.get("trade_id"),
                                     "symbol": a.get("symbol"), "verdict": a.get("realized_verdict"),
                                     "source": "haiku_authored_v8"}})

    # --- trade_critique: Grok keepers ---
    grok = read_jsonl(GROK_FILE) if os.path.exists(GROK_FILE) else []
    grok_rows = []
    for a in grok:
        s = pool_by_id.get(a["id"])
        if not s or not a.get("critique"):
            continue
        if s["prompt"] in oos_prompts:
            continue
        if not no_grok_filter and not _passes(a["id"], "grok", GROK_FLOOR):
            continue
        grok_rows.append({"prompt": s["prompt"], "completion": a["critique"],
                          "_meta": {"category": "trade_critique", "trade_id": a.get("trade_id"),
                                    "symbol": a.get("symbol"), "verdict": a.get("realized_verdict"),
                                    "source": "grok_authored_v7"}})

    tc_rows = haiku_rows + grok_rows
    n_tc0 = len(tc_rows)
    tc_rows, tc_dropped, max_per = dedup_cap(tc_rows, lambda r: normalize(r["completion"]))

    # --- KEEP debate_critique + signal_analysis from v7 ---
    v7 = read_jsonl(V7_CORPUS) if os.path.exists(V7_CORPUS) else []
    def keep_cat(cat):
        return [r for r in v7 if r.get("_meta", {}).get("category") == cat
                and r["prompt"] not in oos_prompts]
    debate_rows = keep_cat("debate_critique")
    signal_rows = keep_cat("signal_analysis")

    corpus = tc_rows + debate_rows + signal_rows
    leak = sum(1 for r in corpus if r["prompt"] in oos_prompts)
    assert leak == 0, f"LEAKAGE: {leak} OOS prompts in v8 corpus"

    # --- diversity report (lexical) ---
    tc_completions = [r["completion"] for r in tc_rows]
    opens = collections.Counter(opening(c) for c in tc_completions)
    norms = collections.Counter(normalize(c) for c in tc_completions)
    uniq_pct = round(100 * len(norms) / max(1, len(tc_completions)), 1)
    top_opener_pct = round(100 * opens.most_common(1)[0][1] / max(1, len(tc_completions)), 1) \
                     if opens else 0.0
    distinct_6w = len(opens)
    haiku_opens = collections.Counter(opening(r["completion"]) for r in haiku_rows[:len(tc_rows)])
    grok_opens  = collections.Counter(opening(r["completion"]) for r in grok_rows[:len(tc_rows)])

    # --- diversity report (bge-m3) ---
    bge_similarity = None
    if not no_bge:
        print("[bge-m3] computing embedding-space spread...")
        bge_similarity = diversity_report_bge(tc_completions)
        if bge_similarity is None:
            print("[bge-m3] unavailable (sentence-transformers not installed or model missing)")

    # --- split 80/10/10 ---
    rng = random.Random(42)
    idx = list(range(len(corpus))); rng.shuffle(idx)
    ntr = int(0.8 * len(corpus)); nva = int(0.1 * len(corpus))
    train = [corpus[i] for i in idx[:ntr]]
    val   = [corpus[i] for i in idx[ntr:ntr+nva]]
    test  = [corpus[i] for i in idx[ntr+nva:]]
    write_jsonl(OUT, corpus)
    write_jsonl(OUT.replace(".jsonl", ".train.jsonl"), train)
    write_jsonl(OUT.replace(".jsonl", ".val.jsonl"), val)
    write_jsonl(OUT.replace(".jsonl", ".test.jsonl"), test)

    cat_counts = collections.Counter(r["_meta"]["category"] for r in corpus)
    stats = {
        "total": len(corpus),
        "by_category": dict(cat_counts),
        "trade_critique": {
            "haiku_keepers": len(haiku_rows), "grok_keepers": len(grok_rows),
            "pre_dedup": n_tc0, "template_dropped": tc_dropped, "final": len(tc_rows),
            "uniqueness_pct": uniq_pct,
            "distinct_6w_openers": distinct_6w,
            "top_opener_share_pct": top_opener_pct,
            "top_8_openers": opens.most_common(8),
            "top_4_haiku_openers": haiku_opens.most_common(4),
            "top_4_grok_openers": grok_opens.most_common(4),
            "v7_baseline_top_opener_pct": 13.7,
        },
        "bge_m3_mean_pairwise_similarity": bge_similarity,
        "debate_critique": {"kept": len(debate_rows)},
        "signal_analysis": {"kept": len(signal_rows)},
        "split": {"train": len(train), "val": len(val), "test": len(test)},
        "oos_leakage": leak,
    }
    with open(STATS, "w") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps(stats, indent=2))
    print(f"\n>>> v8 trade_critique: {len(tc_rows)} rows "
          f"({len(haiku_rows)} haiku + {len(grok_rows)} grok)")
    print(f">>> top opener share: {top_opener_pct}% (v7 baseline: 13.7%) "
          f"-- {'IMPROVED' if top_opener_pct < 13.7 else 'NO IMPROVEMENT'}")
    if bge_similarity is not None:
        print(f">>> bge-m3 mean pairwise sim: {bge_similarity} (lower = more diverse)")


if __name__ == "__main__":
    no_bge = "--no-bge" in sys.argv
    no_grok_filter = "--no-grok-filter" in sys.argv
    main(no_bge=no_bge, no_grok_filter=no_grok_filter)
