"""HM-PLUTUS-V8-CORPUS Phase B — quality-filter score both Haiku and Grok targets.

Scores each authored target through qwen3:14b only (single judge, fast pass — sufficient
for a quality floor; OOS gate uses dual-judge for precision). Idempotent per (id, author).

Output: data/plutus_v8/filter_scores.jsonl — one row per target with mean score + pass flag.

Usage:
  python3 score_v8_filter.py             # score Haiku targets (primary)
  python3 score_v8_filter.py --grok      # also retroactively score Grok v7 targets
  python3 score_v8_filter.py --smoke 5   # smoke: first 5 Haiku only
"""
from __future__ import annotations
import os, sys, json, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common, score
from common import DATA, DIMS, read_jsonl, write_jsonl, ollama_generate

POOL_FILE    = os.path.join(DATA, "plutus_v7", "scenario_pool.jsonl")
HAIKU_FILE   = os.path.join(DATA, "plutus_v8", "authored_tc_haiku.jsonl")
GROK_FILE    = os.path.join(DATA, "plutus_v7", "authored_tc.jsonl")
SCORES_RAW   = os.path.join(DATA, "plutus_v8", "filter_scores_raw.jsonl")
SCORES_AGG   = os.path.join(DATA, "plutus_v8", "filter_scores.jsonl")

JUDGE        = "qwen3:14b"            # single fast judge for filter pass
HAIKU_FLOOR  = 1.5                    # keep Haiku targets with mean judge ≥ this
GROK_FLOOR   = 1.3                    # keep Grok targets with mean judge ≥ this


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 3) if xs else None


def score_targets(pool_by_id, authored, author_tag, limit=None):
    """Score authored targets idempotently; returns list of raw score records."""
    raws = read_jsonl(SCORES_RAW) if os.path.exists(SCORES_RAW) else []
    done = {(r["id"], r["author"]) for r in raws}
    pending = [(a, pool_by_id[a["id"]]) for a in authored
               if a["id"] in pool_by_id and a.get("critique") and (a["id"], author_tag) not in done]
    if limit:
        pending = pending[:limit]
    print(f"[score_filter {author_tag}] pending={len(pending)} (skip {len(authored)-len(pending)} done)")
    for i, (a, s) in enumerate(pending, 1):
        sc, _ = score.judge_one(JUDGE, s, a["critique"])
        raws.append({"id": a["id"], "author": author_tag, "judge": JUDGE,
                     "scores": sc, "unscorable": sc is None})
        if i % 25 == 0 or i == len(pending):
            write_jsonl(SCORES_RAW, raws)
            print(f"  {author_tag}: {i}/{len(pending)} scored")
    write_jsonl(SCORES_RAW, raws)
    return raws


def aggregate_scores(raws):
    """Aggregate raw scores → one row per (id, author) with mean + pass flag."""
    by = collections.defaultdict(list)
    for r in raws:
        if r["scores"]:
            by[(r["id"], r["author"])].append(r["scores"])
    agg = []
    for (sid, author), score_list in by.items():
        dim_means = {d: _mean([s.get(d) for s in score_list]) for d in DIMS}
        overall = _mean(list(dim_means.values()))
        floor = HAIKU_FLOOR if author == "haiku" else GROK_FLOOR
        agg.append({"id": sid, "author": author, "dim_means": dim_means,
                    "overall": overall, "pass": (overall is not None and overall >= floor)})
    write_jsonl(SCORES_AGG, agg)
    print(f"[aggregate] {len(agg)} total scored")
    for author_tag in ("haiku", "grok"):
        sub = [r for r in agg if r["author"] == author_tag]
        if not sub:
            continue
        passed = sum(1 for r in sub if r["pass"])
        mean_ov = _mean([r["overall"] for r in sub if r["overall"] is not None])
        print(f"  {author_tag}: {len(sub)} scored, {passed} pass "
              f"(floor={HAIKU_FLOOR if author_tag=='haiku' else GROK_FLOOR}), mean={mean_ov}")
    return agg


def main(include_grok=False, smoke=None):
    pool_by_id = {s["id"]: s for s in read_jsonl(POOL_FILE)}
    haiku = read_jsonl(HAIKU_FILE) if os.path.exists(HAIKU_FILE) else []
    if not haiku:
        raise SystemExit(f"No Haiku targets found at {HAIKU_FILE} — run author_v8_haiku.py first")

    raws = score_targets(pool_by_id, haiku, "haiku", limit=smoke)
    if include_grok:
        grok = read_jsonl(GROK_FILE) if os.path.exists(GROK_FILE) else []
        if grok:
            raws = score_targets(pool_by_id, grok, "grok", limit=smoke)
        else:
            print(f"[warn] Grok file not found: {GROK_FILE}")
    aggregate_scores(read_jsonl(SCORES_RAW) if os.path.exists(SCORES_RAW) else raws)


if __name__ == "__main__":
    grok = "--grok" in sys.argv
    smoke = int(sys.argv[sys.argv.index("--smoke")+1]) if "--smoke" in sys.argv else None
    main(include_grok=grok, smoke=smoke)
