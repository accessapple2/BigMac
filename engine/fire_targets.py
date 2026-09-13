"""engine/fire_targets.py — HM-FIRE-TARGETS-LIVE-FLEET-2026-09-13.

Who may appear on a Bridge order-path control (the Research tab's Fire Agent list,
Smart Money BUY rows, Classic alert-card BUY). One source for both the UI lists
(/api/fleet/fire-targets) and the /api/paper-trader/manual-trade BUY refusal, so a
control can never offer an agent the server would refuse. Mirrors
paper_trader.buy()'s own entry gates (human, HALT, AUDITION, BENCH). Read-only.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"

# Seats whose decisions come from a model — the only ones the Fire Agent list offers.
MODEL_PROVIDERS = frozenset({"ollama", "openai", "anthropic", "xai", "google", "mlx", "crewai"})


def _load(db_path) -> list[dict]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(
            "SELECT id, display_name, provider, model_id, halt_mode, is_active, is_human, crew_role "
            "FROM ai_players")]
    finally:
        conn.close()


def _refusal(row: dict) -> str | None:
    if row["halt_mode"] != "active":
        return f"halt_mode={row['halt_mode']}"
    if not row["is_active"]:
        return "is_active=0"
    if row["is_human"]:
        return "human seat"
    if row["crew_role"] == "auditioning":
        return "auditioning (signal-only)"
    from engine.paper_trader import _bench_block_reason
    return _bench_block_reason(row["id"])


def halt_modes(db_path=None) -> dict[str, str]:
    return {r["id"]: r["halt_mode"] for r in _load(db_path or DB_PATH)}


def fire_target_refusal(player_id: str, db_path=None) -> str | None:
    """None if `player_id` may open a position via a manual fire, else the reason."""
    row = next((r for r in _load(db_path or DB_PATH) if r["id"] == player_id), None)
    if row is None:
        return "not in ai_players"
    return _refusal(row)


def fire_targets(db_path=None) -> dict:
    """targets: model-backed seats for the Fire Agent list (with the model_id each is
    actually running). eligible_ids: every manual-trade-resolvable agent that may open a
    position. excluded: model seats that are NOT offered, with why."""
    from engine.crew_specialization import CREW_MANIFEST
    targets, eligible, excluded = [], [], []
    for r in _load(db_path or DB_PATH):
        if r["id"] not in CREW_MANIFEST:
            continue
        name = r["display_name"] or r["id"]
        why = _refusal(r)
        if why:
            if r["provider"] in MODEL_PROVIDERS:
                excluded.append({"player_id": r["id"], "display_name": name,
                                 "model_id": r["model_id"], "reason": why})
            continue
        eligible.append(r["id"])
        if r["provider"] in MODEL_PROVIDERS:
            targets.append({"player_id": r["id"], "display_name": name,
                            "model_id": r["model_id"], "crew_role": r["crew_role"]})
    targets.sort(key=lambda t: t["display_name"].lower())
    return {"targets": targets, "eligible_ids": sorted(eligible), "excluded": excluded}
