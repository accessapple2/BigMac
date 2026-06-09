"""engine/reveille.py — HM-REVEILLE Pre-Market Brief Generator (Phase 1).

Repackages OT's existing morning intel into the XO packet format
(sitrep -> tape -> catalyst map -> buckets -> posture -> triggers), synthesizes
via a local LLM (plutus-v1 by default), and delivers via HM-UHURA-HAILS
(ntfy ollie-premarket + HTML email) + a dashboard card.

Discipline (load-bearing):
  - READ-ONLY on all DBs. Sacred DBs (trader.db / arena.db / tractor.db) never
    written. The only write is data/reveille_brief.json (own artifact).
  - Market-day-aware: the scheduler gates on market_calendar.is_trading_day().
  - FAIL-CLOSED: SUSPECT intel (failed validation) is excluded from the
    actionable read (counted + listed separately, never silently included).
    Market-data is "manual / TBD" in Phase 1 (live adapter is Phase 2), so the
    DEGRADED banner logic exists but only trips once a live feed is wired.
  - Synthesis never fabricates: prompt instructs cite-per-claim, tag free/paid,
    mark gaps. Phase 1 ships without live market data by design.

Phase 1 reuses: morning_briefing.generate_daily_intel_report (substrate),
alert_channels.push_ntfy/send_email (delivery), market_calendar (calendar/tz),
external_picks (intel). Net-new here: XO-packet synthesis + read-time SUSPECT
validation + catalyst calendar load.
"""
from __future__ import annotations

import os
import re
import json
import sqlite3
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRADER_DB        = os.path.join(_ROOT, "data", "trader.db")
CATALYST_JSON    = os.path.join(_ROOT, "config", "catalyst_calendar.json")
BRIEF_JSON       = os.path.join(_ROOT, "data", "reveille_brief.json")
PREMARKET_TOPIC  = os.environ.get("NTFY_PREMARKET_TOPIC", "ollie-premarket")

try:
    from config import OLLIE_URL as _OLLIE_URL
except Exception:  # pragma: no cover - config always present in prod
    _OLLIE_URL = "http://192.168.1.168:11434"
OLLAMA_URL    = os.getenv("ADVISORY_OLLAMA_URL", os.getenv("OLLAMA_BASE_URL", _OLLIE_URL))
REVEILLE_MODEL = os.getenv("REVEILLE_MODEL", "plutus-v1")   # local, finance-tuned; Q-Grok is the override
CATALYST_LOOKAHEAD_DAYS = 10


# ── LLM ──────────────────────────────────────────────────────────────────────

def _ollama(prompt: str, system: str = "", model: str | None = None, timeout: int = 120) -> str:
    """Call Ollama /api/generate and return text. Empty string on failure (caller
    treats empty synthesis as a hard fail — never fabricates a fallback)."""
    payload = {"model": model or REVEILLE_MODEL, "prompt": prompt, "stream": False}
    if system:
        payload["system"] = system
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=timeout)
        if r.ok:
            return (r.json().get("response") or "").strip()
        logger.warning("reveille _ollama non-ok: %s", r.status_code)
    except Exception as e:
        logger.warning("reveille _ollama failed: %s: %r", type(e).__name__, e)
    return ""


# ── Inputs ───────────────────────────────────────────────────────────────────

def load_catalysts(today: str) -> list[dict]:
    """Upcoming catalysts within the lookahead window. Read-only JSON."""
    try:
        with open(CATALYST_JSON) as fh:
            data = json.load(fh)
        out = []
        for c in data.get("catalysts", []):
            d = c.get("date", "")
            if d >= today:  # ISO dates sort lexically
                out.append(c)
        out.sort(key=lambda c: c.get("date", ""))
        return out[:12]
    except Exception as e:
        logger.warning("reveille load_catalysts failed: %s: %r", type(e).__name__, e)
        return []


_TICKER_IN_NOTE = re.compile(r"\$([A-Z]{1,5})\b")


def validate_picks(rows: list[sqlite3.Row]) -> tuple[list[dict], list[dict]]:
    """FAIL-CLOSED split of ingested picks into (clean, suspect).

    A pick is SUSPECT when its risk geometry is broken or its body references a
    different ticker than its row (the BBY class). Suspect picks are EXCLUDED
    from the actionable read and surfaced separately — never silently used.
    """
    clean, suspect = [], []
    for r in rows:
        d = dict(r)
        tk = (d.get("ticker") or "").upper()
        entry, stop, note = d.get("entry"), d.get("stop"), (d.get("note") or "")
        reasons = []
        if entry is not None and stop is not None:
            if stop >= entry:
                reasons.append(f"stop {stop} >= entry {entry}")
            elif entry > 0 and (entry - stop) / entry > 0.5:
                reasons.append(f"risk {(entry - stop) / entry:.0%} > 50% (implausible stop)")
        # ticker-in-body mismatch (the BBY case): a cashtag naming a different ticker
        cashtags = {m.group(1) for m in _TICKER_IN_NOTE.finditer(note)}
        if cashtags and tk and cashtags - {tk}:
            reasons.append(f"body references {sorted(cashtags - {tk})} != row {tk}")
        if reasons:
            d["_suspect_reasons"] = reasons
            suspect.append(d)
        else:
            clean.append(d)
    return clean, suspect


def get_recent_picks() -> tuple[list[dict], list[dict]]:
    """external_picks from the last 24h, validated. READ-ONLY."""
    try:
        conn = sqlite3.connect(TRADER_DB, timeout=15)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT source, pick_date, ticker, action, entry, stop, note, submitted_at
                 FROM external_picks
                WHERE submitted_at >= datetime('now', '-24 hours')
                ORDER BY submitted_at DESC LIMIT 50"""
        ).fetchall()
        conn.close()
        return validate_picks(rows)
    except Exception as e:
        logger.warning("reveille get_recent_picks failed: %s: %r", type(e).__name__, e)
        return [], []


# ── Assembly ─────────────────────────────────────────────────────────────────

def assemble(fresh: bool = True) -> dict:
    """Gather the raw substrate for the brief. READ-ONLY.

    fresh=True regenerates the daily intel report (used by the 05:45 scheduled
    run, which precedes the 06:00 report — self-trigger avoids stale substrate).
    fresh=False reads the existing disk JSON (used by dry-runs / on-demand).
    """
    from engine.market_calendar import az_now
    today = az_now().strftime("%Y-%m-%d")

    intel, degraded = {}, None
    try:
        from engine.morning_briefing import generate_daily_intel_report
        if fresh:
            intel = generate_daily_intel_report(force=True, push_ntfy=False)
        else:
            intel = generate_daily_intel_report(disk_only=True)
        if intel.get("unavailable"):
            degraded = "intel substrate unavailable (daily report not generated)"
    except Exception as e:
        degraded = f"intel assembly error: {type(e).__name__}"
        logger.warning("reveille assemble intel failed: %s: %r", type(e).__name__, e)

    clean_picks, suspect_picks = get_recent_picks()
    catalysts = load_catalysts(today)

    # PHASE 2: live pre-market FUTURES tape via the swappable adapter (was the Phase 1 [gap];
    # sector/movers are already in the intel substrate). FAIL-CLOSED: empty tape on a market
    # day trips the DEGRADED banner rather than presenting nothing as fresh.
    futures, md_meta = {}, {"source": "none", "tier": "free"}
    try:
        from engine import market_adapter
        from engine.market_calendar import is_trading_day
        futures, md_meta = market_adapter.futures()
        if not futures and is_trading_day(az_now().date()):
            degraded = (degraded + " | " if degraded else "") + "live futures tape empty (adapter returned nothing)"
    except Exception as e:
        degraded = (degraded + " | " if degraded else "") + f"market tape error: {type(e).__name__}"
        logger.warning("reveille assemble tape failed: %s: %r", type(e).__name__, e)

    return {
        "date": today,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "intel": intel,
        "clean_picks": clean_picks,
        "suspect_picks": suspect_picks,
        "catalysts": catalysts,
        "futures": futures,
        "market_data_status": md_meta.get("tier", "free"),   # PHASE 2: real tier (free), not manual_tbd
        "market_data_source": md_meta.get("source"),
        "degraded": degraded,
    }


# ── Synthesis (XO packet) ──────────────────────────────────────────────────────

_SYSTEM = (
    "You are the XO of the OllieTrades research fleet writing the daily pre-market "
    "brief for the Admiral. Paper-research only; never imply live execution. "
    "GROUNDING RULES (non-negotiable): cite the source of every claim inline; tag "
    "intel as (free) or (paid) when known; if a figure is missing, write '[gap — not "
    "in feed]' rather than inventing it; NEVER fabricate a number. Be concise and tactical."
)


def _packet_prompt(sub: dict) -> str:
    intel = sub.get("intel", {}) or {}
    gp = intel.get("game_plan", {}) or {}
    setups = [s for s in (intel.get("technical_setups") or []) if isinstance(s, dict)]
    cats = sub.get("catalysts", [])
    clean = sub.get("clean_picks", [])
    fut = sub.get("futures") or {}
    fut_str = "; ".join(
        f"{k} {v.get('price')}" + (f" ({v.get('change_pct'):+.2f}%)" if v.get('change_pct') is not None else "")
        for k, v in fut.items()
    ) or "[gap — adapter returned no futures]"
    src = sub.get("market_data_source") or "adapter"

    lines = [
        f"DATE: {sub['date']}",
        "",
        "RAW SUBSTRATE (assemble into the packet below; do not invent beyond this):",
        f"- Game plan: regime={gp.get('regime','?')} tone={gp.get('tone','?')} "
        f"VIX={gp.get('vix','?')} F&G={gp.get('fg_score','?')} headline={gp.get('headline','?')!r}",
        f"- Sector rotation: {json.dumps(intel.get('sector_rotation', {}), default=str)[:600]}",
        f"- Technical setups ({len(setups)}): "
        + "; ".join(f"{s.get('symbol')}({s.get('confidence','?')})" for s in setups[:8]),
        f"- Congress radar (48h): {json.dumps(intel.get('congress_radar', []), default=str)[:400]}",
        f"- AH movers: {json.dumps(intel.get('ah_movers', []), default=str)[:300]}",
        "- Ingested TI picks (validated CLEAN only): "
        + ("; ".join(f"{p['ticker']} entry={p.get('entry')} stop={p.get('stop')}" for p in clean[:10]) or "none"),
        "- Upcoming catalysts: "
        + ("; ".join(f"{c['date']} {c['label']} [{c['weight']}]" for c in cats) or "none"),
        f"- LIVE FUTURES TAPE ({src}, {sub.get('market_data_status','free')}): {fut_str}",
        "",
        "Write the brief in EXACTLY these six sections, headers verbatim:",
        "1. SITREP — 2-3 line executive read.",
        "2. THE TAPE — overnight/pre-market; note where live data is a [gap].",
        "3. CATALYST MAP — the upcoming catalysts above, weighted; flag the dominant one.",
        "4. WATCHLISTS BY BUCKET — group the setups/picks into momentum / hedge-defensive / event buckets.",
        "5. POSTURE — the fleet stance into the session, gated by the dominant catalyst.",
        "6. TRIGGERS — what invalidates / confirms the read.",
    ]
    return "\n".join(lines)


def synthesize(sub: dict) -> str:
    """LLM pass → the XO packet text. Empty string = hard fail (no fabricated fallback)."""
    return _ollama(_packet_prompt(sub), system=_SYSTEM)


# ── Rendering & delivery ───────────────────────────────────────────────────────

def _headline(sub: dict, packet: str) -> str:
    gp = (sub.get("intel", {}) or {}).get("game_plan", {}) or {}
    dom = next((c for c in sub.get("catalysts", []) if c.get("weight") == "high"), None)
    bits = [f"🌅 Reveille {sub['date']}"]
    if gp.get("regime"):
        bits.append(f"regime {gp['regime']}")
    if dom:
        bits.append(f"next: {dom['label']} {dom['date']}")
    if sub.get("degraded"):
        bits.append("⚠️ DEGRADED")
    return " · ".join(bits)


def build_html(sub: dict, packet: str) -> str:
    deg = sub.get("degraded")
    banner = (f"<div style='background:#7f1d1d;color:#fff;padding:8px;font-weight:bold'>"
              f"⚠️ DEGRADED — {deg}</div>") if deg else ""
    suspect = sub.get("suspect_picks", [])
    suspect_html = ""
    if suspect:
        rows = "".join(
            f"<tr><td>{s.get('ticker')}</td><td>{'; '.join(s.get('_suspect_reasons', []))}</td></tr>"
            for s in suspect
        )
        suspect_html = (
            "<h3>⚠️ Excluded (SUSPECT — failed validation, not in actionable read)</h3>"
            "<table border='1' cellpadding='6' style='border-collapse:collapse'>"
            f"<tr><th>Ticker</th><th>Reason</th></tr>{rows}</table>"
        )
    body = packet.replace("\n", "<br>")
    return (
        f"<div style='font-family:system-ui,Arial,sans-serif;max-width:720px'>"
        f"{banner}"
        f"<h2>🖖 OllieTrades — Pre-Market Brief</h2>"
        f"<p style='color:#888'>{sub['date']} · synthesized by {REVEILLE_MODEL} · "
        f"research/paper read — RULE #1 untouched</p>"
        f"<div style='white-space:normal;line-height:1.5'>{body}</div>"
        f"{suspect_html}"
        f"<p style='color:#888;font-size:12px'>Generated {sub['generated_at']} UTC · "
        f"market data: {sub.get('market_data_status')}</p></div>"
    )


def build_brief(fresh: bool = True) -> dict:
    """Assemble → validate → synthesize → render. Writes own JSON. No sacred-DB writes."""
    sub = assemble(fresh=fresh)
    packet = synthesize(sub)
    if not packet:
        sub["degraded"] = (sub.get("degraded") or "") + " | synthesis failed (LLM returned empty)"
        packet = "[SITREP] Synthesis unavailable — LLM returned no output. No brief content fabricated."
    brief = {
        **{k: sub[k] for k in ("date", "generated_at", "market_data_status", "degraded")},
        "model": REVEILLE_MODEL,
        "packet": packet,
        "headline": _headline(sub, packet),
        "html": build_html(sub, packet),
        "clean_pick_count": len(sub.get("clean_picks", [])),
        "suspect_picks": sub.get("suspect_picks", []),
        "catalysts": sub.get("catalysts", []),
    }
    try:
        with open(BRIEF_JSON, "w") as fh:
            json.dump(brief, fh, indent=2, default=str)
    except Exception as e:
        logger.warning("reveille brief save failed: %s: %r", type(e).__name__, e)
    return brief


def deliver(brief: dict) -> dict:
    """Push headline → ollie-premarket + HTML digest email (via HM-UHURA-HAILS)."""
    from engine import alert_channels as ac
    res = {}
    res["ntfy"] = ac.push_ntfy(PREMARKET_TOPIC, brief["headline"], brief["packet"][:1500],
                               priority="default", tags="sunrise,ollietrades")
    res["email"] = ac.send_email(f"Pre-Market Brief {brief['date']}", brief["html"])
    return res


def run_reveille(dry_run: bool = False, fresh: bool = True) -> dict:
    """Entry point. dry_run=True builds + returns without delivering."""
    brief = build_brief(fresh=fresh)
    if dry_run:
        brief["_delivery"] = "skipped (dry_run)"
        return brief
    brief["_delivery"] = deliver(brief)
    return brief


if __name__ == "__main__":
    import sys
    _dry = "--dry-run" in sys.argv
    _fresh = "--fresh" in sys.argv
    b = run_reveille(dry_run=_dry, fresh=_fresh)
    print("=== HEADLINE ===")
    print(b["headline"])
    print("\n=== PACKET ===")
    print(b["packet"])
    print(f"\n=== meta: model={b['model']} clean_picks={b['clean_pick_count']} "
          f"suspect={len(b['suspect_picks'])} degraded={b.get('degraded')} delivery={b.get('_delivery')} ===")
