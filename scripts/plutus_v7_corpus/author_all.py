"""HM-PLUTUS-V7-CORPUS Phase B — mass-author trade_critique targets via Grok (same path as the pilot).

Authors EVERY scenario in scenario_pool.jsonl using author.call_grok + the validated AUTHOR_SYSTEM
prompt (direct read-only xAI; NO trader.db writes). Seeds from the 36 validated pilot targets and is
idempotent (resume skips ids already in authored_tc.jsonl). Light concurrency (threads) to cut
wall-clock; xAI handles concurrent requests. Per-row cost embedded; running total logged.
"""
from __future__ import annotations
import os, sys, json, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plutus_v7_eval"))
import author  # noqa: E402  (reuses load_key, call_grok, parse_authored, AUTHOR_SYSTEM, MODEL)
import common  # noqa: E402
from common import read_jsonl  # noqa: E402

POOL = author.POOL
PILOT = author.AUTHORED
OUT = os.path.join(common.DATA, "plutus_v7", "authored_tc.jsonl")
WORKERS = 5
_lock = threading.Lock()


def _flush(rows):
    with _lock:
        with open(OUT, "w") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    pool = read_jsonl(POOL)
    rows = read_jsonl(OUT) if os.path.exists(OUT) else []
    done = {r["id"] for r in rows}
    # NOTE: pilot-seeding intentionally DISABLED — the v1 pilot used the pre-diversity prompt and
    # carries the stock "The decisive risk was..." opener; re-author everything under the v2 prompt.
    if os.environ.get("SEED_PILOT") == "1" and os.path.exists(PILOT):
        for p in read_jsonl(PILOT):
            if p["id"] not in done:
                p = dict(p); p.setdefault("cost", None); rows.append(p); done.add(p["id"])
    todo = [s for s in pool if s["id"] not in done]
    print(f"[author_all] pool={len(pool)} done={len(done)} todo={len(todo)} workers={WORKERS}")
    key = author.load_key()
    _flush(rows)

    def work(s):
        raw, itok, otok, cost = author.call_grok(key, s["prompt"])
        dr, crit = author.parse_authored(raw)
        return {"id": s["id"], "trade_id": s.get("trade_id"), "symbol": s["symbol"],
                "outcome": s["outcome"], "regime": s["regime"], "agent": s["agent"],
                "realized_verdict": s["realized_verdict"], "realized_pnl_pct": s["realized_pnl_pct"],
                "decisive_risk": dr, "critique": crit, "author_model": author.MODEL,
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
            if n_new % 25 == 0:
                _flush(rows)
                cost = sum(r.get("cost") or 0 for r in rows)
                print(f"  {n_new}/{len(todo)} new; total rows {len(rows)}; cum cost ${cost:.4f}")
    _flush(rows)
    cost = sum(r.get("cost") or 0 for r in rows)
    print(f"[author_all] DONE rows={len(rows)} (new {n_new}); TOTAL author cost ${cost:.4f} (model {author.MODEL})")


if __name__ == "__main__":
    main()
