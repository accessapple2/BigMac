#!/usr/bin/env python3
"""
setup_similarity_signal.py — HM-DEJAVU setup-similarity recall signal (Phase 2.5).

CONFIRMATORY-ONLY (same rail as fred_bankrate_signal.py / institutional_13f_signal.py):
this NEVER originates a trade. Given a candidate's setup text it asks the bge-m3 trade
substrate "this looks like N past setups, X% won, avg $Y" and returns that as a low-weight
context read the witness layer MAY count as ONE confirmatory source — and only once the
fleet already has >= MIN_FLEET_VOTES independent directional votes (sole-voter -> never).

Read path (live .venv, stdlib only — NO sqlite_vec):
  The bge-m3 embeddings live in two places, both rebuilt together by scripts/recall_refresh.py:
    * vec_trades_bge  — sqlite-vec vec0 table (bake-off lineage; queried only in .venv-recall)
    * recall_corpus   — plain table with the embedding stored as a JSON array
  This module reads recall_corpus and does a pure-Python L2 KNN (193 x 1024 = trivial), so the
  live trader needs NO sqlite_vec dependency (it isn't installed in the live .venv, by design —
  same isolation posture as vectorbt -> .venv-backtest).

Doctrine:
  - is_trigger is hardcoded False. Do not change.
  - ABSTAIN (return None) when there is no real analog (nearest distance > DIST_THRESH),
    when the candidate can't be embedded, or when the corpus is missing/empty. Never force a match.
  - Default OFF via config.SETUP_SIMILARITY_ENABLED. When OFF, market_vote() returns None
    (witness gets nothing); recall()/shadow_log() still compute so we can shadow-validate.
  - Read-only on trader.db's corpus. Shadow rows are append-only, never deleted.

  NOTE: make_setup_text() / normalize_setup() are the CANONICAL definitions of "the setup
  text" and its dedup key. scripts/recall_refresh.py imports them from HERE so the corpus and
  the query are embedded/deduped identically. Do not fork these.
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    from rich.console import Console
    console = Console()
except Exception:  # pragma: no cover
    class _Stub:
        def log(self, *a, **k):
            print(*a)
    console = _Stub()

_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")

# bge-m3 won the HM-RECALL 4-way bake-off (scripts/recall_bakeoff.py). Embeds run on the
# Ollie Box (.168) — same host the war room / refresh use. Keyless, free, local.
OLLAMA_EMBED_URL = os.environ.get("RECALL_OLLAMA_URL", "http://192.168.1.168:11434/api/embed")
EMBED_MODEL = "bge-m3"
EMBED_DIM = 1024

# KNN params. K=8 nearest distinct-setup neighbors (after dedup). vec_trades_bge was built with
# vec0's default L2 (euclidean) distance, so we match it here for THRESH continuity.
K_NEIGHBORS = 8
# Nearest-neighbor distance above which we judge "no real analog" and ABSTAIN. Calibrated against
# the bge-m3 L2 distribution on this corpus (scripts/recall_refresh.py --distances): genuine
# analogs land <=~0.56 (p90), live candidates probed <=0.55, while deliberate no-analog gibberish
# lands ~0.96 — 0.75 sits in the clean gap (accepts real, abstains on no-analog). Override via env.
DIST_THRESH = float(os.environ.get("RECALL_DIST_THRESH", 0.75))

# Win-rate -> lean. Need at least MIN_MATCHES_FOR_LEAN analogs before leaning off neutral
# (thin recall = no opinion). Context-class weight 0.5 (slow/structural, like FRED's macro lean),
# NOT the 1.0 technical-confirmatory class.
MIN_MATCHES_FOR_LEAN = 3
WINRATE_CONFIRM = 0.65   # >= this share of analogs won  -> 'confirm' (BULLISH context)
WINRATE_CAUTION = 0.35   # <= this share of analogs won  -> 'caution' (BEARISH context)
CONFIRMATORY_WEIGHT = 0.5

# Confirmatory-only rail (mirrors fred_bankrate_signal.MIN_FLEET_VOTES).
MIN_FLEET_VOTES = 2

_EMBED_TIMEOUT_S = float(os.environ.get("RECALL_EMBED_TIMEOUT_S", 30))


# ─── Canonical setup text + dedup key (shared with recall_refresh.py) ─────────

def make_setup_text(symbol: str, timeframe: str | None, setup: str | None) -> str:
    """The exact string we embed, for BOTH corpus rows and query candidates.

    Mirrors scripts/recall_bakeoff.py::build_corpus: '{SYM} {TF}: {setup}' truncated to 1200.
    Setup text is the ENTRY reasoning only (never realized P&L — outcome is a tag, not embedded,
    so neighbors cluster by setup not by result)."""
    tf = (timeframe or "SWING")
    s = (setup or "").strip()
    if len(s) <= 5:
        s = f"{tf} trade"
    return f"{symbol} {tf}: {s}"[:1200]


def normalize_setup(text: str) -> str:
    """Dedup key: drop digits/punctuation/case so identical re-entries collapse to one
    representative (the 61% re-entry fix — kills 'MRVL x5' neighbor spam)."""
    key = re.sub(r"[^a-z ]", " ", text.lower())
    return re.sub(r"\s+", " ", key).strip()


# ─── Embedding ────────────────────────────────────────────────────────────────

def embed_candidate(text: str) -> list[float] | None:
    """Embed one candidate setup via bge-m3 on the Ollie Box. Returns the vector, or None on
    any failure (-> caller ABSTAINS, never guesses)."""
    body = json.dumps({"model": EMBED_MODEL, "input": text, "keep_alive": "2m"}).encode()
    req = urllib.request.Request(
        OLLAMA_EMBED_URL, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_EMBED_TIMEOUT_S) as r:
            data = json.loads(r.read().decode("utf-8"))
        embs = data.get("embeddings") or []
        if not embs or len(embs[0]) != EMBED_DIM:
            console.log(f"[yellow]dejavu: embed bad shape len={len(embs)}")
            return None
        return [float(x) for x in embs[0]]
    except Exception as e:
        console.log(f"[yellow]dejavu: embed failed: {type(e).__name__}: {e!r}")
        return None


# ─── Corpus load + KNN (pure python) ─────────────────────────────────────────

def _conn(db_path: str | None = None) -> sqlite3.Connection:
    c = sqlite3.connect(db_path or _DB_PATH, timeout=15)
    c.execute("PRAGMA busy_timeout=15000")
    c.row_factory = sqlite3.Row
    return c


def _load_corpus(conn: sqlite3.Connection) -> list[dict]:
    """Load recall_corpus rows with embeddings deserialized. Empty list if the table is missing
    (no refresh has run yet) -> caller abstains."""
    try:
        rows = conn.execute(
            "SELECT trade_id, symbol, outcome, pnl, normalized, emb_json FROM recall_corpus"
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    out = []
    for r in rows:
        try:
            emb = json.loads(r["emb_json"])
        except Exception:
            continue
        if len(emb) != EMBED_DIM:
            continue
        out.append({"trade_id": r["trade_id"], "symbol": r["symbol"],
                    "outcome": r["outcome"], "pnl": r["pnl"],
                    "normalized": r["normalized"], "emb": emb})
    return out


def _l2(a: list[float], b: list[float]) -> float:
    """Euclidean distance — matches vec_trades_bge's vec0 default metric."""
    return math.sqrt(sum((x - y) * (x - y) for x, y in zip(a, b)))


def recall(symbol: str, timeframe: str | None, setup: str | None,
           k: int = K_NEIGHBORS, db_path: str | None = None,
           candidate_emb: list[float] | None = None) -> dict | None:
    """Core recall read. Returns a context dict, or None to ABSTAIN.

    ABSTAIN when: candidate can't be embedded, corpus is empty, or the NEAREST neighbor is
    farther than DIST_THRESH (no genuine analog). Neighbors are deduped by normalized setup
    text (no duplicate-ticker / re-entry spam) before aggregation.

    The returned `vote`/`lean` derives from the analogs' win-rate but is CONTEXT only —
    is_trigger is always False and this never authorizes a trade on its own.
    """
    text = make_setup_text(symbol, timeframe, setup)
    emb = candidate_emb if candidate_emb is not None else embed_candidate(text)
    if emb is None:
        return None

    own_close = False
    try:
        conn = _conn(db_path)
        try:
            corpus = _load_corpus(conn)
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]dejavu: corpus load: {type(e).__name__}: {e!r}")
        return None
    if not corpus:
        return None

    cand_norm = normalize_setup(text)
    scored = sorted(
        ((_l2(emb, r["emb"]), r) for r in corpus), key=lambda t: t[0])

    # ABSTAIN if the single nearest analog is too far — no real precedent for this setup.
    if scored[0][0] > DIST_THRESH:
        return None

    # Collect up to k nearest DISTINCT-normalized neighbors (dedup re-entry spam). Skip a neighbor
    # that is the candidate's own identical setup text (don't let a setup "match itself").
    neighbors: list[dict] = []
    seen_norms: set[str] = set()
    for dist, r in scored:
        nkey = r["normalized"]
        if nkey == cand_norm:
            own_close = True
            continue
        if nkey in seen_norms:
            continue
        seen_norms.add(nkey)
        neighbors.append({"trade_id": r["trade_id"], "symbol": r["symbol"],
                          "outcome": r["outcome"], "pnl": r["pnl"], "distance": round(dist, 4)})
        if len(neighbors) >= k:
            break

    if not neighbors:
        return None

    n = len(neighbors)
    wins = sum(1 for x in neighbors if x["outcome"] == "win")
    win_rate = round(wins / n, 4)
    avg_pnl = round(sum((x["pnl"] or 0.0) for x in neighbors) / n, 2)
    nearest = neighbors[0]["distance"]
    lean = _lean_from_winrate(win_rate, n)

    return {
        "source": "setup_similarity_dejavu",
        "symbol": symbol,
        "is_trigger": False,                 # DOCTRINE: confirmatory only
        "vote": lean,                        # confirm | neutral | caution (context)
        "n_matches": n,
        "win_rate": win_rate,
        "avg_pnl": avg_pnl,
        "nearest_distance": nearest,
        "candidate_text": text,
        "candidate_matched_self": own_close,
        "neighbors": neighbors,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }


def _lean_from_winrate(win_rate: float, n: int) -> str:
    """High historical win-rate among analogs -> 'confirm'; low -> 'caution'; thin/mixed -> neutral."""
    if n < MIN_MATCHES_FOR_LEAN:
        return "neutral"
    if win_rate >= WINRATE_CONFIRM:
        return "confirm"
    if win_rate <= WINRATE_CAUTION:
        return "caution"
    return "neutral"


# ─── Confirmatory-only convergence contract ──────────────────────────────────

def confirmatory_vote(fleet_directional_votes: int, lean: str | None) -> dict:
    """De-ja-vu recall may CONFIRM an existing fleet convergence but may NEVER ORIGINATE.
    Counts only once the fleet has >= MIN_FLEET_VOTES directional votes (mirror FRED/13F)."""
    is_directional = lean in ("confirm", "caution")
    is_sole_voter = fleet_directional_votes < MIN_FLEET_VOTES
    counts = is_directional and not is_sole_voter

    trade_permitted_on_dejavu_alone = False
    assert not (is_sole_voter and counts), (
        "setup-similarity is confirmatory-only: the sole voter must never count toward a "
        "trade (MIN_FLEET_VOTES=%d not met)" % MIN_FLEET_VOTES
    )
    assert trade_permitted_on_dejavu_alone is False

    return {
        "source": "setup_similarity_dejavu",
        "lean": lean,
        "direction": ("BULLISH" if lean == "confirm"
                      else "BEARISH" if lean == "caution" else "NEUTRAL"),
        "counts_toward_convergence": counts,
        "is_sole_voter": is_sole_voter,
        "fleet_directional_votes": fleet_directional_votes,
        "min_fleet_votes_required": MIN_FLEET_VOTES,
        "trade_permitted_on_dejavu_alone": trade_permitted_on_dejavu_alone,
        "is_trigger": False,
    }


def market_vote(symbol: str, timeframe: str | None, setup: str | None,
                db_path: str | None = None, enabled: bool | None = None) -> dict | None:
    """Gated witness entrypoint. Returns a confirmatory lean dict for ONE candidate, or None to
    ABSTAIN. Returns None whenever the flag is OFF — the witness gets nothing until we choose to
    wire it (a LATER ticket). Wiring into McCoy/Archer is NOT done here."""
    if enabled is None:
        try:
            from config import SETUP_SIMILARITY_ENABLED as enabled
        except Exception:
            enabled = False
    if not enabled:
        return None
    read = recall(symbol, timeframe, setup, db_path=db_path)
    if read is None or read["vote"] not in ("confirm", "caution"):
        return None  # abstain — no analog or no directional lean
    # Doctrine guardrail asserted in code: dejavu can never count as the sole voter.
    assert confirmatory_vote(0, read["vote"])["counts_toward_convergence"] is False
    direction = "BULLISH" if read["vote"] == "confirm" else "BEARISH"
    reasoning = (f"[confirm] dejavu: {symbol} looks like {read['n_matches']} past setups, "
                 f"{round(read['win_rate']*100)}% won, avg ${read['avg_pnl']} "
                 f"(nearest d={read['nearest_distance']})")
    return {"direction": direction, "weight": CONFIRMATORY_WEIGHT, "reasoning": reasoning,
            "n": read["n_matches"], "win_rate": read["win_rate"], "avg_pnl": read["avg_pnl"],
            "nearest_distance": read["nearest_distance"]}


# ─── Shadow log (append-only; never deleted) ─────────────────────────────────

def ensure_shadow_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS setup_similarity_shadow (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL,
            timeframe       TEXT,
            n_matches       INTEGER,
            win_rate        REAL,
            avg_pnl         REAL,
            nearest_distance REAL,
            lean            TEXT,
            abstained       INTEGER NOT NULL DEFAULT 0,
            candidate_text  TEXT,
            created_at      TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dejavu_shadow_sym "
                 "ON setup_similarity_shadow(symbol, created_at DESC)")
    conn.commit()


def shadow_log(symbol: str, timeframe: str | None, setup: str | None,
               db_path: str | None = None) -> dict | None:
    """Compute the recall read and record it to setup_similarity_shadow REGARDLESS of the flag —
    this is the shadow-validation feed (the witness is NOT touched). Returns the read (or None)."""
    read = recall(symbol, timeframe, setup, db_path=db_path)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn = _conn(db_path)
        try:
            ensure_shadow_schema(conn)
            if read is None:
                conn.execute(
                    "INSERT INTO setup_similarity_shadow "
                    "(symbol, timeframe, abstained, candidate_text, created_at) VALUES (?,?,?,?,?)",
                    (symbol, timeframe, 1, make_setup_text(symbol, timeframe, setup), now))
            else:
                conn.execute(
                    "INSERT INTO setup_similarity_shadow "
                    "(symbol, timeframe, n_matches, win_rate, avg_pnl, nearest_distance, lean, "
                    " abstained, candidate_text, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (symbol, timeframe, read["n_matches"], read["win_rate"], read["avg_pnl"],
                     read["nearest_distance"], read["vote"], 0, read["candidate_text"], now))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]dejavu: shadow_log: {type(e).__name__}: {e!r}")
    return read


# ─── CLI: spike + abstain test ───────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3:
        # ad-hoc: setup_similarity_signal.py SYMBOL "setup text..." [timeframe]
        sym = sys.argv[1].upper()
        txt = sys.argv[2]
        tf = sys.argv[3] if len(sys.argv) > 3 else "SWING"
        print(json.dumps(recall(sym, tf, txt), indent=2))
        sys.exit(0)

    # Default spike: pull ~5 recent closed-trade setups and print their recall reads, then an
    # abstain test on a deliberate no-analog setup.
    conn = _conn()
    recent = conn.execute(
        "SELECT t.symbol, t.timeframe, "
        "  (SELECT reasoning FROM trades b WHERE b.player_id=t.player_id AND b.symbol=t.symbol "
        "   AND b.action='BUY' AND b.executed_at<=t.executed_at AND b.reasoning IS NOT NULL "
        "   ORDER BY b.executed_at DESC LIMIT 1) AS setup "
        "FROM trades t WHERE t.action IN ('SELL','COVER') AND t.realized_pnl IS NOT NULL "
        "ORDER BY t.executed_at DESC LIMIT 5").fetchall()
    conn.close()
    print("=" * 90 + "\nHM-DEJAVU spike — recall reads for 5 recent candidates\n" + "=" * 90)
    for r in recent:
        read = recall(r["symbol"], r["timeframe"], r["setup"])
        if read is None:
            print(f"\n{r['symbol']} ({r['timeframe']}): ABSTAIN (no analog / no embed)")
            continue
        print(f"\n{r['symbol']} ({r['timeframe']}): {read['vote'].upper()} — "
              f"{read['n_matches']} analogs, {round(read['win_rate']*100)}% won, "
              f"avg ${read['avg_pnl']}, nearest d={read['nearest_distance']}")
        for nb in read["neighbors"][:5]:
            print(f"    {nb['symbol']}/{nb['outcome']} d={nb['distance']} ${nb['pnl']}")

    print("\n" + "=" * 90 + "\nAbstain test — deliberate no-analog gibberish setup\n" + "=" * 90)
    gib = recall("ZZZZ", "SWING",
                 "quantum llama hovercraft eel underwater basket weaving championship 1899")
    print("result:", "ABSTAIN (None) ✓" if gib is None else f"NOT abstained: {gib['vote']}")
