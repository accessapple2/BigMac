"""engine/recall_prompt.py — HM-XO-PLAN-2026-09 Phase 2 recall-in-prompt (spec item B.1).

Injects a compact 2-line summary of the K most similar past decisions
(by bge-m3 setup-text embedding) into a candidate's prompt, gated behind
config.RECALL_IN_PROMPT_ENABLED (default OFF) so it can be run as its own
bakeoff arm (8B with recall vs. 8B without).

ADDITIVE ONLY, per RULE #1: new file, no changes to
engine/setup_similarity_signal.py (HM-DEJAVU) -- reuses its module-level
embedding/corpus/KNN primitives (make_setup_text, normalize_setup,
embed_candidate, _load_corpus, _l2, EMBED_DIM) exactly as they exist,
rather than forking them. Do not duplicate those definitions here.

NOT WIRED INTO engine/providers/base.py::build_prompt() TONIGHT — that is
the live, shared prompt builder every trading agent's real scan calls
through. This module is built and benchmarked standalone; wiring it into
the live prompt path is a deliberate follow-up (same posture as the S8
rotation held for tomorrow), not something to fold into a multi-item
build session even behind a default-OFF flag.

WHAT THIS DOES NOT DO YET — call these out, don't fake them:
  - recall_corpus (data/trader.db) has no sector or regime columns today,
    only symbol/outcome/pnl. "Match on ticker, then sector, then regime"
    as a tiered fallback isn't implementable against the current corpus.
    This does ticker-proximity KNN only -- which the embedding already
    favors by construction (make_setup_text embeds "{SYMBOL} {TF}:
    {setup}", so same-ticker setups cluster closest in embedding space
    before sector/regime would ever need to break a tie).
  - recall_corpus has no realized 1d/5d forward-return columns either --
    only outcome (win/loss) and pnl (the trade's own realized P&L, not a
    fixed-horizon forward return). Reports outcome/pnl per neighbor
    instead. True 1d/5d forward returns are exactly what tomorrow's
    dataset exporter (held per the Captain, 2026-09-11) is meant to
    produce -- once that lands, this module should switch to reading
    those columns instead of pnl/outcome. Flagged, not faked.

Caching: corpus load + JSON-deserialize (the only real cost besides the
embed call itself) is cached in-process for CORPUS_CACHE_TTL_S, since the
corpus only changes when scripts/recall_refresh.py runs as a periodic
batch job, not per-scan-cycle. The candidate embed call itself can't be
cached generically (setup text differs per call).
"""
from __future__ import annotations

import time

from engine.setup_similarity_signal import (
    EMBED_DIM,
    _l2,
    _load_corpus,
    embed_candidate,
    make_setup_text,
    normalize_setup,
)

try:
    from rich.console import Console
    console = Console()
except Exception:  # pragma: no cover
    class _Stub:
        def log(self, *a, **k):
            print(*a)
    console = _Stub()

DEFAULT_K = 5
CORPUS_CACHE_TTL_S = 600  # 10 min -- recall_refresh.py runs far less often than this

_corpus_cache: dict = {"loaded_at": 0.0, "rows": []}


def _cached_corpus(db_path: str | None = None) -> list[dict]:
    now = time.time()
    if now - _corpus_cache["loaded_at"] > CORPUS_CACHE_TTL_S:
        import sqlite3
        from engine.setup_similarity_signal import _conn
        try:
            conn = _conn(db_path)
            try:
                rows = _load_corpus(conn)
            finally:
                conn.close()
        except sqlite3.OperationalError:
            rows = []
        _corpus_cache["rows"] = rows
        _corpus_cache["loaded_at"] = now
    return _corpus_cache["rows"]


def get_recall_neighbors(symbol: str, timeframe: str | None, setup: str | None,
                          k: int = DEFAULT_K, db_path: str | None = None,
                          candidate_emb: list[float] | None = None) -> list[dict]:
    """K nearest distinct-normalized historical analogs by bge-m3 L2 distance.
    Returns [] on any abstain condition (can't embed, empty corpus, no
    neighbors after self/dup exclusion) -- same ABSTAIN posture as
    setup_similarity_signal.recall(), never guesses. No distance-threshold
    cutoff here (unlike recall() the confirmatory-vote signal) -- this is
    context for the prompt, not a trade-authorizing vote, so showing the
    k nearest analogs even if imperfect is more useful than showing none.

    candidate_emb: optional pre-computed embedding, bypassing the live
    bge-m3 call -- mirrors setup_similarity_signal.recall()'s own
    parameter, same purpose (testing the KNN/formatting path independent
    of embed-service availability).
    """
    text = make_setup_text(symbol, timeframe, setup)
    emb = candidate_emb if candidate_emb is not None else embed_candidate(text)
    if emb is None:
        return []

    corpus = _cached_corpus(db_path)
    if not corpus:
        return []

    cand_norm = normalize_setup(text)
    scored = sorted(((_l2(emb, r["emb"]), r) for r in corpus), key=lambda t: t[0])

    neighbors: list[dict] = []
    seen_norms: set[str] = set()
    for dist, r in scored:
        nkey = r["normalized"]
        if nkey == cand_norm or nkey in seen_norms:
            continue
        seen_norms.add(nkey)
        neighbors.append({
            "trade_id": r["trade_id"], "symbol": r["symbol"],
            "outcome": r["outcome"], "pnl": r["pnl"], "distance": round(dist, 4),
        })
        if len(neighbors) >= k:
            break
    return neighbors


def build_recall_prompt_section(symbol: str, timeframe: str | None, setup: str | None,
                                 k: int = DEFAULT_K, db_path: str | None = None,
                                 candidate_emb: list[float] | None = None) -> str:
    """Two-line prompt-injection block, or "" if disabled/abstain.

    Gated on config.RECALL_IN_PROMPT_ENABLED so this can be A/B'd as a
    bakeoff arm (8B with recall vs. without) without touching the live
    prompt path for every other agent. candidate_emb: see
    get_recall_neighbors() -- test-only bypass of the live embed call.
    """
    try:
        from config import RECALL_IN_PROMPT_ENABLED
    except ImportError:
        RECALL_IN_PROMPT_ENABLED = False
    if not RECALL_IN_PROMPT_ENABLED:
        return ""

    try:
        neighbors = get_recall_neighbors(symbol, timeframe, setup, k=k, db_path=db_path,
                                          candidate_emb=candidate_emb)
    except Exception as e:
        console.log(f"[yellow]recall_prompt: abstain on error: {type(e).__name__}: {e!r}")
        return ""
    if not neighbors:
        return ""

    n = len(neighbors)
    wins = sum(1 for x in neighbors if x["outcome"] == "win")
    avg_pnl = sum(x["pnl"] or 0.0 for x in neighbors) / n
    detail = ", ".join(
        f"{x['symbol']}({x['outcome']},{'+' if (x['pnl'] or 0) >= 0 else ''}{x['pnl']:.2f})"
        for x in neighbors
    )
    return (
        f"Recall ({n} similar past setups, by outcome/pnl -- NOT fixed-horizon forward "
        f"returns, see engine/recall_prompt.py): {wins}W/{n - wins}L, avg pnl "
        f"{'+' if avg_pnl >= 0 else ''}{avg_pnl:.2f}\n"
        f"Nearest analogs: {detail}"
    )
