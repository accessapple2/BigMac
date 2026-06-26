#!/usr/bin/env python3
"""HM-RECALL — 4-way embedding bake-off for trade-SETUP similarity. Run with .venv-recall.
Additive + reversible: trader.db gains vec_trades_{bge,qwen,gemma,nomic}; DROP to undo. No flags,
no signals, ai_players untouched, live fleet untouched. Embeds the ENTRY setup text (NOT realized
P&L — outcome is a tag, not embedded, so neighbors cluster by setup not by result)."""
import sqlite3, json, urllib.request, sys, time
import sqlite_vec

DB = "/Users/bigmac/autonomous-trader/data/trader.db"
OLLAMA = "http://192.168.1.168:11434/api/embed"
N_CORPUS = 500   # most-recent closed trades (manageable embed time; plenty for similarity)
MODELS = [  # (label, ollama_model, dim, table)
    ("bge",   "bge-m3",              1024, "vec_trades_bge"),
    ("qwen",  "qwen3-embedding:0.6b", 1024, "vec_trades_qwen"),
    ("gemma", "embeddinggemma",       768, "vec_trades_gemma"),
    ("nomic", "nomic-embed-text",     768, "vec_trades_nomic"),
]


def conn():
    c = sqlite3.connect(DB, timeout=45)
    c.execute("PRAGMA busy_timeout=45000")
    c.enable_load_extension(True); sqlite_vec.load(c); c.enable_load_extension(False)
    return c


def build_corpus():
    """Each closed trade -> its ENTRY (BUY) reasoning = the setup text. Outcome tag = pnl sign."""
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
        rich = bool(ent and ent[0] and len(ent[0].strip()) > 5)
        setup = (ent[0].strip() if rich else f"{tf or 'SWING'} trade")
        text = f"{sym} {tf or 'SWING'}: {setup}"[:1200]   # setup ONLY (no realized P&L)
        rows.append({"trade_id": tid, "symbol": sym, "outcome": "win" if pnl > 0 else "loss",
                     "pnl": round(float(pnl), 2), "text": text, "rich": rich})
    c.close()
    # DEDUP identical re-entries: normalize (drop digits/punct/case), keep ONE representative
    # per distinct normalized setup (most-recent = first, since ordered DESC). Kills MRVL x5 etc.
    import re as _re
    seen, deduped = set(), []
    for r in rows:
        key = _re.sub(r"[^a-z ]", " ", r["text"].lower())
        key = _re.sub(r"\s+", " ", key).strip()
        if key in seen:
            continue
        seen.add(key); deduped.append(r)
    return deduped


def embed_batch(model, texts):
    out = []
    for i in range(0, len(texts), 64):
        chunk = texts[i:i+64]
        body = json.dumps({"model": model, "input": chunk, "keep_alive": "2m"}).encode()
        req = urllib.request.Request(OLLAMA, data=body, headers={"Content-Type": "application/json"})
        for attempt in range(3):
            try:
                r = json.loads(urllib.request.urlopen(req, timeout=180).read().decode())
                out.extend(r["embeddings"]); break
            except Exception as e:
                if attempt == 2:
                    raise
                time.sleep(3)
    return out


def main():
    corpus = build_corpus()
    print(f"[corpus] {len(corpus)} closed-trade setups "
          f"(win={sum(1 for r in corpus if r['outcome']=='win')} "
          f"loss={sum(1 for r in corpus if r['outcome']=='loss')})")
    texts = [r["text"] for r in corpus]
    vectors = {}  # label -> list[vec] aligned to corpus
    for label, model, dim, table in MODELS:
        t0 = time.time()
        vs = embed_batch(model, texts)
        assert len(vs) == len(corpus) and len(vs[0]) == dim, f"{label} dim/len mismatch {len(vs)}x{len(vs[0]) if vs else 0}"
        vectors[label] = vs
        print(f"[embed] {label:5s} ({model}) {len(vs)} x{dim} in {time.time()-t0:.0f}s")

    c = conn()
    for label, model, dim, table in MODELS:
        c.execute(f"DROP TABLE IF EXISTS {table}")
        c.execute(f"CREATE VIRTUAL TABLE {table} USING vec0("
                  f"trade_id INTEGER PRIMARY KEY, outcome TEXT, symbol TEXT, emb FLOAT[{dim}])")
        for r, v in zip(corpus, vectors[label]):
            c.execute(f"INSERT INTO {table}(trade_id,outcome,symbol,emb) VALUES (?,?,?,?)",
                      (r["trade_id"], r["outcome"], r["symbol"], sqlite_vec.serialize_float32(v)))
        c.commit()
        print(f"[store] {table}: {c.execute(f'SELECT count(*) FROM {table}').fetchone()[0]} rows")

    # ---- bake-off: 10 most-recent as probes ----
    by_id = {r["trade_id"]: r for r in corpus}
    idx = {r["trade_id"]: i for i, r in enumerate(corpus)}
    # Preferred cross-ticker probes (distinct setup archetypes); fall back to rich+distinct-ticker.
    pref = [2855, 2854, 2843, 2839]   # AXP dip-buy / MS grade-A / JTAI convergence-short / CHRW capitol
    probes = [by_id[t] for t in pref if t in by_id]
    if len(probes) < 4:
        seen = {p["symbol"] for p in probes}
        for r in corpus:
            if r["rich"] and r["symbol"] not in seen:
                probes.append(r); seen.add(r["symbol"])
            if len(probes) >= 4:
                break
    print(f"\n[corpus after dedup] {len(corpus)} distinct setups")
    print("=" * 100 + "\nBAKE-OFF (deduped) — top-5 nearest, WITH full neighbor setup text\n" + "=" * 100)
    for p in probes:
        print(f"\n{'#'*92}\nPROBE [{p['trade_id']}] {p['symbol']} ({p['outcome']}, ${p['pnl']})\n  TEXT: {p['text']}")
        for label, model, dim, table in MODELS:
            qv = sqlite_vec.serialize_float32(vectors[label][idx[p['trade_id']]])
            nn = c.execute(
                f"SELECT trade_id, outcome, symbol, distance FROM {table} "
                f"WHERE emb MATCH ? ORDER BY distance LIMIT 6", (qv,)).fetchall()
            nn = [x for x in nn if x[0] != p["trade_id"]][:5]
            print(f"  -- {label} ({model}) --")
            for _id, oc, sym, dist in nn:
                txt = by_id.get(_id, {}).get("text", "?")
                print(f"     {sym}/{oc} d={dist:.2f} :: {txt[:150]}")
    c.close()
    print("\n[done] vec_trades_{bge,qwen,gemma,nomic} live in trader.db (DROP to undo). "
          "Human verdict: which model's neighbors are genuinely the SAME setup?")


if __name__ == "__main__":
    main()
