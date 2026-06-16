#!/usr/bin/env python3
"""
recall_refresh.py — HM-DEJAVU corpus refresh. Run with .venv-recall (needs sqlite_vec).

Rebuilds the bge-m3 trade-SETUP substrate so the de-ja-vu recall signal never queries a stale
snapshot (the one-time bake-off corpus rots otherwise — the same drift trap we keep fighting).
It maintains TWO stores in lock-step, built from the SAME corpus + vectors in one run:

  * vec_trades_bge  — sqlite-vec vec0 table (bake-off lineage; for .venv-recall analysis)
  * recall_corpus   — plain table with the embedding stored as a JSON array, so the LIVE trader
                      (.venv, no sqlite_vec) can read it and do pure-python KNN.

Corpus = closed trades (action IN ('SELL','COVER'), realized_pnl NOT NULL) -> each mapped to its
ENTRY (BUY) reasoning = the setup text, deduped by normalized setup (one representative each).
The canonical make_setup_text()/normalize_setup() come from engine.setup_similarity_signal so the
corpus and the live query embed/dedup identically.

Modes:
  (default)   incremental — embed only closed trades NEW since the last run (and whose normalized
              setup isn't already represented). Cheap; safe to cron.
  --full      drop + rebuild both stores from scratch.
  --distances print the bge-m3 L2 nearest-neighbor distance distribution (THRESH calibration aid).

Additive + reversible: only adds rows/tables. trader.db corpus is otherwise untouched; no flags,
no live decision path. Never deletes data.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import sqlite_vec

# CANONICAL text/dedup helpers come from the engine module (single definition shared with the live
# query path, so corpus + query embed/dedup identically). Self-contained otherwise — no dependency
# on the (untracked) bake-off script.
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
from engine.setup_similarity_signal import (  # noqa: E402
    EMBED_DIM, EMBED_MODEL, OLLAMA_EMBED_URL, make_setup_text, normalize_setup,
)


def embed_batch(model: str, texts: list[str]) -> list[list[float]]:
    """Batch-embed via Ollama on the Ollie Box (.168), 64 at a time, 3 retries w/ backoff.
    Same plumbing the HM-RECALL bake-off used."""
    out: list[list[float]] = []
    for i in range(0, len(texts), 64):
        chunk = texts[i:i + 64]
        body = json.dumps({"model": model, "input": chunk, "keep_alive": "2m"}).encode()
        req = urllib.request.Request(
            OLLAMA_EMBED_URL, data=body, headers={"Content-Type": "application/json"})
        for attempt in range(3):
            try:
                r = json.loads(urllib.request.urlopen(req, timeout=180).read().decode())
                out.extend(r["embeddings"])
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(3)
    return out

DB = str(_ROOT / "data" / "trader.db")
VEC_TABLE = "vec_trades_bge"
N_CORPUS = int(os.environ.get("RECALL_N_CORPUS", 500))  # most-recent closed trades to consider


def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, timeout=45)
    c.execute("PRAGMA busy_timeout=45000")
    c.enable_load_extension(True)
    sqlite_vec.load(c)
    c.enable_load_extension(False)
    return c


def build_corpus() -> list[dict]:
    """Closed trades -> deduped setup rows (most-recent representative per normalized setup)."""
    c = sqlite3.connect(DB, timeout=45)
    closed = c.execute(
        "SELECT id, player_id, symbol, realized_pnl, executed_at, timeframe FROM trades "
        "WHERE action IN ('SELL','COVER') AND realized_pnl IS NOT NULL "
        "ORDER BY executed_at DESC LIMIT ?", (N_CORPUS,)).fetchall()
    rows = []
    for tid, pid, sym, pnl, ts, tf in closed:
        ent = c.execute(
            "SELECT reasoning FROM trades WHERE player_id=? AND symbol=? AND action='BUY' "
            "AND executed_at<=? AND reasoning IS NOT NULL AND length(trim(reasoning))>0 "
            "ORDER BY executed_at DESC LIMIT 1", (pid, sym, ts)).fetchone()
        setup = ent[0].strip() if (ent and ent[0]) else ""
        text = make_setup_text(sym, tf, setup)
        rows.append({"trade_id": tid, "symbol": sym,
                     "outcome": "win" if pnl > 0 else "loss",
                     "pnl": round(float(pnl), 2), "text": text,
                     "normalized": normalize_setup(text)})
    c.close()
    seen, deduped = set(), []
    for r in rows:
        if r["normalized"] in seen:
            continue
        seen.add(r["normalized"])
        deduped.append(r)
    return deduped


def ensure_corpus_table(c: sqlite3.Connection) -> None:
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS recall_corpus (
            trade_id    INTEGER PRIMARY KEY,
            symbol      TEXT NOT NULL,
            outcome     TEXT NOT NULL,
            pnl         REAL,
            setup_text  TEXT,
            normalized  TEXT,
            emb_json    TEXT NOT NULL,
            embedded_at TEXT NOT NULL
        )
        """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_recall_corpus_norm ON recall_corpus(normalized)")
    c.commit()


def ensure_vec_table(c: sqlite3.Connection) -> None:
    c.execute(f"CREATE VIRTUAL TABLE IF NOT EXISTS {VEC_TABLE} USING vec0("
              f"trade_id INTEGER PRIMARY KEY, outcome TEXT, symbol TEXT, emb FLOAT[{EMBED_DIM}])")


def _insert(c: sqlite3.Connection, rows: list[dict], vecs: list[list[float]], now: str) -> None:
    for r, v in zip(rows, vecs):
        c.execute(f"INSERT OR REPLACE INTO {VEC_TABLE}(trade_id,outcome,symbol,emb) VALUES (?,?,?,?)",
                  (r["trade_id"], r["outcome"], r["symbol"], sqlite_vec.serialize_float32(v)))
        c.execute(
            "INSERT OR REPLACE INTO recall_corpus "
            "(trade_id,symbol,outcome,pnl,setup_text,normalized,emb_json,embedded_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (r["trade_id"], r["symbol"], r["outcome"], r["pnl"], r["text"], r["normalized"],
             _to_json(v), now))
    c.commit()


def _to_json(vec: list[float]) -> str:
    import json
    return json.dumps([round(float(x), 7) for x in vec])


def refresh(full: bool = False) -> dict:
    corpus = build_corpus()
    c = conn()
    if full:
        c.execute(f"DROP TABLE IF EXISTS {VEC_TABLE}")
        c.execute("DROP TABLE IF EXISTS recall_corpus")
        c.commit()
    ensure_vec_table(c)
    ensure_corpus_table(c)

    if full:
        new_rows = corpus
    else:
        have_ids = {r[0] for r in c.execute("SELECT trade_id FROM recall_corpus").fetchall()}
        have_norm = {r[0] for r in c.execute("SELECT normalized FROM recall_corpus").fetchall()}
        new_rows = [r for r in corpus
                    if r["trade_id"] not in have_ids and r["normalized"] not in have_norm]

    if not new_rows:
        n_corpus = c.execute("SELECT count(*) FROM recall_corpus").fetchone()[0]
        c.close()
        return {"mode": "full" if full else "incremental", "new": 0, "embedded": 0,
                "corpus_total": n_corpus}

    t0 = time.time()
    vecs = embed_batch(EMBED_MODEL, [r["text"] for r in new_rows])
    assert len(vecs) == len(new_rows) and len(vecs[0]) == EMBED_DIM, \
        f"embed dim/len mismatch {len(vecs)}x{len(vecs[0]) if vecs else 0}"
    now = datetime.now(timezone.utc).isoformat()
    _insert(c, new_rows, vecs, now)
    n_vec = c.execute(f"SELECT count(*) FROM {VEC_TABLE}").fetchone()[0]
    n_corpus = c.execute("SELECT count(*) FROM recall_corpus").fetchone()[0]
    c.close()
    return {"mode": "full" if full else "incremental", "new": len(new_rows),
            "embedded": len(new_rows), "embed_secs": round(time.time() - t0, 1),
            "vec_total": n_vec, "corpus_total": n_corpus}


def distances() -> None:
    """Print the bge-m3 L2 nearest-neighbor distance distribution over the corpus (calibration)."""
    import json
    import math
    c = sqlite3.connect(DB, timeout=45)
    rows = c.execute("SELECT trade_id, normalized, emb_json FROM recall_corpus").fetchall()
    c.close()
    embs = [(tid, norm, json.loads(ej)) for tid, norm, ej in rows]
    if len(embs) < 2:
        print("need >=2 corpus rows; run --full first")
        return

    def l2(a, b):
        return math.sqrt(sum((x - y) * (x - y) for x, y in zip(a, b)))

    nearest = []
    for i, (tid, norm, e) in enumerate(embs):
        best = min((l2(e, e2) for j, (_, n2, e2) in enumerate(embs)
                    if j != i and n2 != norm), default=None)
        if best is not None:
            nearest.append(best)
    nearest.sort()
    n = len(nearest)
    def pct(p): return nearest[min(n - 1, int(p * n))]
    print(f"corpus={len(embs)} nearest-neighbor L2 distance distribution:")
    print(f"  min={nearest[0]:.3f}  p10={pct(0.10):.3f}  p25={pct(0.25):.3f}  "
          f"median={pct(0.50):.3f}  p75={pct(0.75):.3f}  p90={pct(0.90):.3f}  max={nearest[-1]:.3f}")
    print("  -> DIST_THRESH should sit near/above the bulk of true-analog distances "
          "(roughly p75-p90) so genuine setups match but unrelated ones abstain.")


def main() -> None:
    ap = argparse.ArgumentParser(description="HM-DEJAVU recall corpus refresh")
    ap.add_argument("--full", action="store_true", help="drop + rebuild both stores")
    ap.add_argument("--distances", action="store_true", help="print L2 NN distance distribution")
    args = ap.parse_args()
    if args.distances:
        distances()
        return
    summary = refresh(full=args.full)
    import json
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
