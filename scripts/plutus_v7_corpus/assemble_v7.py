"""HM-PLUTUS-V7-CORPUS Phase B — dedup authored tc + KEEP debate/signal + assemble + freeze + split.

- trade_critique: Grok-authored targets (authored_tc.jsonl), HARD-deduped two ways:
  (a) template cap — any normalized near-duplicate form capped at <=2% of the set, excess dropped;
  (b) structural diversity report — opening-phrase distribution + uniqueness %.
- debate_critique: KEEP v6's (100% unique), EXCLUDING any row whose prompt is in the frozen OOS set.
- signal_analysis: KEEP v6's, OOS-excluded, then deduped.
- OOS discipline: every category is scrubbed of the 178 frozen OOS prompts before freeze (verified 0).
- Freeze plutus_corpus_v7.jsonl + deterministic 80/10/10 split (seed 42).
"""
from __future__ import annotations
import os, sys, re, json, math, random, collections
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import common  # noqa: E402
from common import DATA, OOS_SET, read_jsonl, write_jsonl  # noqa: E402

POOL = os.path.join(DATA, "plutus_v7", "scenario_pool.jsonl")
AUTHORED = os.path.join(DATA, "plutus_v7", "authored_tc.jsonl")
V6 = os.path.join(DATA, "plutus_corpus_v6.jsonl")
OUT = os.path.join(DATA, "plutus_v7", "plutus_corpus_v7.jsonl")
STATS = os.path.join(DATA, "plutus_v7", "plutus_corpus_v7.stats.json")
TEMPLATE_CAP = 0.02


def normalize(text):
    s = re.sub(r"[A-Z]{2,}", "S", text or "")          # tickers / regime tokens
    s = re.sub(r"[-+]?\d[\d,\.]*%?", "N", s)             # numbers
    s = re.sub(r"[^a-zA-Z ]", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()


def opening(text):
    body = (text or "").split("\n", 1)[-1] if "\n" in (text or "") else (text or "")
    return " ".join(re.findall(r"[a-zA-Z]+", body)[:6]).lower()


def dedup_cap(rows, key_fn, cap=TEMPLATE_CAP):
    """Drop rows so that no normalized form exceeds `cap` share of the kept set."""
    n = len(rows)
    max_per = max(1, math.ceil(cap * n))
    seen = collections.Counter()
    kept, dropped = [], 0
    for r in rows:
        k = key_fn(r)
        if seen[k] >= max_per:
            dropped += 1
            continue
        seen[k] += 1
        kept.append(r)
    return kept, dropped, max_per


def main():
    pool = {p["id"]: p for p in read_jsonl(POOL)}
    oos_prompts = {s["prompt"] for s in read_jsonl(OOS_SET)}
    authored = read_jsonl(AUTHORED)

    # --- trade_critique: build rows + diversity report + dedup ---
    tc_rows = []
    for a in authored:
        p = pool.get(a["id"])
        if not p or not a.get("critique"):
            continue
        if p["prompt"] in oos_prompts:           # belt-and-suspenders (pool is already trade_id-clean)
            continue
        tc_rows.append({"prompt": p["prompt"], "completion": a["critique"],
                        "_meta": {"category": "trade_critique", "trade_id": a.get("trade_id"),
                                  "symbol": a.get("symbol"), "verdict": a.get("realized_verdict"),
                                  "source": "grok_authored_v7"}})
    n_tc0 = len(tc_rows)
    norms = collections.Counter(normalize(r["completion"]) for r in tc_rows)
    opens = collections.Counter(opening(r["completion"]) for r in tc_rows)
    uniq_pct = round(100 * len(norms) / max(1, n_tc0), 1)
    tc_rows, tc_dropped, max_per = dedup_cap(tc_rows, lambda r: normalize(r["completion"]))

    # --- KEEP debate_critique + signal_analysis from v6, OOS-excluded ---
    v6 = read_jsonl(V6)
    def keep(cat, dedup):
        rows = [r for r in v6 if r.get("_meta", {}).get("category") == cat
                and r["prompt"] not in oos_prompts]
        before = len(rows)
        if dedup:
            rows, dropped, _ = dedup_cap(rows, lambda r: normalize(r["completion"]), cap=1.0)
            # cap=1.0 means only exact-normalized dups removed (keep 1 each)
            seen, uniq = set(), []
            for r in rows:
                k = normalize(r["completion"])
                if k in seen:
                    continue
                seen.add(k); uniq.append(r)
            return uniq, before, before - len(uniq)
        return rows, before, 0
    debate_rows, deb_before, deb_drop = keep("debate_critique", dedup=False)
    signal_rows, sig_before, sig_drop = keep("signal_analysis", dedup=True)

    corpus = tc_rows + debate_rows + signal_rows
    # final leakage assert
    leak = sum(1 for r in corpus if r["prompt"] in oos_prompts)
    assert leak == 0, f"LEAKAGE: {leak} OOS prompts in v7 corpus"

    # --- deterministic 80/10/10 split (seed 42) ---
    rng = random.Random(42)
    idx = list(range(len(corpus)))
    rng.shuffle(idx)
    ntr = int(0.8 * len(corpus)); nva = int(0.1 * len(corpus))
    train = [corpus[i] for i in idx[:ntr]]
    val = [corpus[i] for i in idx[ntr:ntr+nva]]
    test = [corpus[i] for i in idx[ntr+nva:]]
    write_jsonl(OUT, corpus)
    write_jsonl(OUT.replace(".jsonl", ".train.jsonl"), train)
    write_jsonl(OUT.replace(".jsonl", ".val.jsonl"), val)
    write_jsonl(OUT.replace(".jsonl", ".test.jsonl"), test)

    cat_counts = collections.Counter(r["_meta"]["category"] for r in corpus)
    stats = {
        "total": len(corpus),
        "by_category": dict(cat_counts),
        "trade_critique": {"authored": n_tc0, "kept": len(tc_rows), "template_dropped": tc_dropped,
                           "cap_per_template": max_per, "uniqueness_pct": uniq_pct,
                           "distinct_openings": len(opens), "top_openings": opens.most_common(8)},
        "debate_critique": {"v6_oos_excluded_kept": deb_before, "kept": len(debate_rows)},
        "signal_analysis": {"v6_oos_excluded": sig_before, "dedup_dropped": sig_drop, "kept": len(signal_rows)},
        "split": {"train": len(train), "val": len(val), "test": len(test)},
        "oos_leakage_in_corpus": leak,
        "author_cost_usd": round(sum((a.get("cost") or 0) for a in authored), 4),
    }
    with open(STATS, "w") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
