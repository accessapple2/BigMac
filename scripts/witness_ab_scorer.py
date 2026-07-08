#!/usr/bin/env python3
"""
Witness A/B scorer — scripts/witness_ab_scorer.py

Post-market batch: loads gpt-oss:20b and deepseek-r1:14b SEQUENTIALLY on
Ollie Max (.168), scores queued war-room debates against McCoy (plutus-v1),
logs to witness_ab table, marks processed.

VRAM discipline:
  - deepseek-r1:14b (9 GB) first, keep_alive=0s → unload before gpt-oss
  - gpt-oss:20b (13 GB) second, keep_alive=0s → unload when done
  - Never runs during RTH — cron fires at 21:30 UTC (30min after close)
  - CAP: SCORE_CAP most-recent unprocessed debates per run (latency budget)

Guardrails:
  - Read-only access to war_room_debates and signal data
  - witness_ab rows are NEVER read by the live trading system
  - No writes to trades, gates, scanners, or any decision table
  - Any single-debate failure is logged and skipped; run continues
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB = ROOT / "data" / "trader.db"
OLLAMA_URL = "http://192.168.1.168:11434"
SCORE_CAP = 300   # max debates scored per model per run (300×2 models ≈ 2.5–4 hrs total)

# ── Model taxonomy (HM-SHADOW-AB-WITNESS 2026-06-29) ─────────────────────────
#
# PRIMARY A/B (active, scored each night):
#   deepseek-r1:14b  — 9 GB,  distilled-R1 reasoning, first pass
#   gpt-oss:20b      — 13 GB, GPT-family OSS, second pass (evicts everything)
#   McCoy baseline   — plutus-v1:latest, 4.7 GB, live seat (verdict from debate ctx)
#
# LIBRARY — PULL-BUT-HOLD (pulled, ready to rotate in after primary read at gate):
#   mistral-small3.1:24b  — 15 GB, Mistral dense 24B (freshly pulled 2026-06-29)
#   qwen3:14b             — 9.3 GB, Qwen3 dense 14.8B (present 4 weeks; "qwen3.6:14b" tag DNE)
#   gemma4:e4b            — 9.6 GB, Gemma4 MoE (freshly pulled 2026-06-29)
#   Activate: add to WITNESS_MODELS below; sequential ordering matters (VRAM)
#
# EXCLUDED — do NOT add:
#   ornith-1.0:35b        — coding model, 35B won't fit 16GB VRAM, Scotty's lane
#   deepseek-v4           — frontier-size, needs 40-80GB+, API-only breaks free-fleet
#   glm-5.2              — same
#   qwen3.7              — same
#   llama-4-maverick     — same
#
# ─────────────────────────────────────────────────────────────────────────────

WITNESS_MODELS = [
    "deepseek-r1:14b",   # 9 GB — first pass
    "gpt-oss:20b",       # 13 GB — second pass (evicts everything; runs last)
]

# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(str(DB), timeout=20)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _ensure_tables(conn) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS witness_queue (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            debate_id    TEXT NOT NULL,
            ticker       TEXT,
            queued_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            debate_context TEXT NOT NULL,
            mccoy_take   TEXT,
            processed    INTEGER DEFAULT 0,
            UNIQUE(debate_id)
        );
        CREATE TABLE IF NOT EXISTS witness_ab (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            model            TEXT NOT NULL,
            debate_id        TEXT NOT NULL,
            ticker           TEXT,
            ts               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            verdict          TEXT,
            critique         TEXT,
            latency_ms       INTEGER,
            mccoy_verdict    TEXT,
            agreed_with_mccoy INTEGER
        );
        CREATE INDEX IF NOT EXISTS witness_ab_debate_model
            ON witness_ab(debate_id, model);
    """)
    conn.commit()


# ── Verdict extraction ────────────────────────────────────────────────────────

_BULL = re.compile(r"\b(buy|long|bull|upside|calls|breakout|moon|rip)\b", re.I)
_BEAR = re.compile(r"\b(sell|short|bear|downside|puts|drop|fade|dump)\b", re.I)


def _extract_verdict(text: str | None) -> str:
    if not text:
        return "NEUTRAL"
    bulls = len(_BULL.findall(text))
    bears = len(_BEAR.findall(text))
    if bulls > bears:
        return "BULLISH"
    if bears > bulls:
        return "BEARISH"
    return "NEUTRAL"


def _agrees(v1: str, v2: str) -> int:
    """1 = exact match, 0 = any divergence (incl. directional-vs-neutral)."""
    return 1 if v1 == v2 else 0


# ── Probe helper — load-test a single cheap call ─────────────────────────────

def _probe(model: str) -> bool:
    """Fire a minimal completion to confirm the model loads. Returns True on success."""
    try:
        from engine.providers.ollama_provider import OllamaProvider
        prov = OllamaProvider(
            player_id="ab-probe", model=model,
            url=OLLAMA_URL, timeout=180, keep_alive="0s",
        )
        resp = prov.call_model("Reply with one word: READY")
        return bool(resp and resp.strip())
    except Exception as e:
        print(f"[witness-ab] probe FAILED for {model}: {e}", flush=True)
        return False


# ── Score one witness model against all queued debates ───────────────────────

def _score_model(model: str, debates: list[sqlite3.Row], conn) -> int:
    """Load model, score all debates, unload (keep_alive=0s). Returns scored count."""
    from engine.providers.ollama_provider import OllamaProvider
    from engine.war_room import generate_hot_take, CREW_NAMES

    short = model.split(":")[0].replace("/", "-")
    player_id = f"ab-witness-{short}"

    print(f"\n[witness-ab] ── {model} ({len(debates)} debates) ──", flush=True)

    prov = OllamaProvider(
        player_id=player_id, model=model,
        url=OLLAMA_URL, timeout=180, keep_alive="0s",
    )

    scored = 0
    for row in debates:
        debate_id = row["debate_id"]
        ticker = row["ticker"] or "?"
        mccoy_take = row["mccoy_take"]
        try:
            ctx = json.loads(row["debate_context"])
        except Exception:
            print(f"[witness-ab]   skip {debate_id}: bad JSON", flush=True)
            continue

        symbol = ctx.get("symbol", ticker)
        price_data = ctx.get("price_data") or {"price": 0, "change_pct": 0}
        round_takes = ctx.get("round_takes") or []

        try:
            t0 = time.perf_counter()
            critique = generate_hot_take(prov, player_id, symbol, price_data, round_takes)
            latency_ms = int((time.perf_counter() - t0) * 1000)
        except Exception as e:
            print(f"[witness-ab]   {debate_id} error: {e}", flush=True)
            continue

        if not critique:
            print(f"[witness-ab]   {debate_id} → empty take (skip)", flush=True)
            continue

        verdict = _extract_verdict(critique)
        mccoy_verdict = _extract_verdict(mccoy_take)
        agreed = _agrees(verdict, mccoy_verdict)

        conn.execute(
            "INSERT INTO witness_ab "
            "(model, debate_id, ticker, verdict, critique, latency_ms, "
            " mccoy_verdict, agreed_with_mccoy) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (model, debate_id, ticker, verdict, critique, latency_ms,
             mccoy_verdict, agreed),
        )
        conn.commit()
        scored += 1
        agree_str = {1: "AGREE", 0: "DIVERGE", -1: "NEUTRAL"}[agreed]
        print(f"[witness-ab]   {ticker:<6} {verdict:<8} mccoy={mccoy_verdict:<8} "
              f"{agree_str}  {latency_ms}ms", flush=True)

    return scored


# ── Mark debates processed (only after BOTH models have scored them) ──────────

def _mark_processed(conn, debate_ids: list[str]) -> None:
    for did in debate_ids:
        both_scored = conn.execute(
            "SELECT COUNT(DISTINCT model) FROM witness_ab WHERE debate_id=?", (did,)
        ).fetchone()[0]
        if both_scored >= len(WITNESS_MODELS):
            conn.execute(
                "UPDATE witness_queue SET processed=1 WHERE debate_id=?", (did,)
            )
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"[witness-ab] starting {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC",
          flush=True)

    conn = _conn()
    _ensure_tables(conn)

    # Pull unprocessed debates, most-recent first, capped at SCORE_CAP.
    # THROUGHPUT DESIGN (decided 2026-06-30, revisit 2026-07-07):
    #   Option 1 (CURRENT): score everything — SCORE_CAP ≥ daily max, zero bias.
    #   Option 2: most-recent-N sample — time-of-day bias, NEVER use (corrupts A/B signal).
    #   Option 3: ORDER BY RANDOM() — representative sample if runtime forces a cap < daily max.
    # At 178–204 debates/day, SCORE_CAP=300 covers the full queue; revisit if volume exceeds cap.
    rows = conn.execute(
        "SELECT * FROM witness_queue WHERE processed=0 "
        "ORDER BY queued_at DESC LIMIT ?",
        (SCORE_CAP,),
    ).fetchall()

    if not rows:
        print("[witness-ab] witness_queue empty — nothing to score", flush=True)
        conn.close()
        return

    # Skip debates already scored by a given model (idempotent re-runs)
    already = {
        (r["debate_id"], r["model"])
        for r in conn.execute(
            "SELECT debate_id, model FROM witness_ab WHERE debate_id IN ("
            + ",".join("?" * len(rows)) + ")",
            [r["debate_id"] for r in rows],
        ).fetchall()
    }

    debate_ids = [r["debate_id"] for r in rows]
    total_scored = 0

    for model in WITNESS_MODELS:
        to_score = [r for r in rows if (r["debate_id"], model) not in already]
        if not to_score:
            print(f"[witness-ab] {model}: all {len(rows)} already scored — skip", flush=True)
            continue

        print(f"[witness-ab] probing {model} ...", flush=True)
        if not _probe(model):
            print(f"[witness-ab] {model}: probe failed, skipping", flush=True)
            continue

        n = _score_model(model, to_score, conn)
        total_scored += n
        print(f"[witness-ab] {model}: scored {n}/{len(to_score)}", flush=True)

        # Explicit unload via zero-TTL completion (belt+suspenders with keep_alive=0s)
        try:
            import requests
            requests.post(f"{OLLAMA_URL}/api/generate",
                          json={"model": model, "keep_alive": 0}, timeout=15)
            print(f"[witness-ab] {model}: unload signal sent", flush=True)
        except Exception as e:
            print(f"[witness-ab] {model}: unload signal FAILED: {e}", flush=True)

    _mark_processed(conn, debate_ids)
    conn.close()

    print(f"\n[witness-ab] done — {total_scored} witness scores logged", flush=True)


if __name__ == "__main__":
    main()
