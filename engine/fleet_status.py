"""engine/fleet_status.py — one-command Fleet Status (read-only aggregator).

Single source of the truth Scotty otherwise assembles by hand at session start.
Consumed by THREE callers off ONE function (get_fleet_status):
  - GET /api/fleet/status            (dashboard/app.py)
  - `make status`                    (imports this module directly — NO server)
  - Kirk briefings + dead-man watchdog

Consolidates situation_report.py / reveille.py / fleet_auditor.py truth:
  * the 4 _EXECUTION_ENABLED gates (verified LIVE off the files, never assumed)
  * market regime (regime_history)
  * active / exit_only / full fleet counts (ai_players.halt_mode)
  * unpushed commit count (git)
  * service health (trader :8080, signal-center :9000, Ollama .168)
  * last-briefing timestamps (data/kirk_briefings/*.md.sent sidecars)

READ-ONLY by construction: no DB writes, no order path, no NTFY/alert emission.
RULE #1 (Schwab hands-off) is structurally untouched — this only reads. Imports
are kept stdlib-light so the CLI path runs fast without booting the server.
"""
from __future__ import annotations

import glob
import json
import os
import re
import socket
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "trader.db"
BRIEFINGS_DIR = REPO_ROOT / "data" / "kirk_briefings"

# The canonical 4 execution gates (mirrors scripts/saturday_kill.sh GATE_FILES).
GATE_FILES = {
    "bull_call_spread_v1": "strategies/bull_call_spread_v1.py",
    "bear_put_spread_v1": "strategies/bear_put_spread_v1.py",
    "bull_spread_v1": "strategies/bull_spread_v1.py",
    "executor": "strategies/executor.py",
}
_GATE_RE = re.compile(r"^_EXECUTION_ENABLED:?\s*bool\s*=\s*True", re.MULTILINE)

BRIEFING_MODES = ("premarket", "open_check", "power_hour", "after_close")
OLLIE_URL = os.getenv("OLLIE_URL", "http://192.168.1.168:11434")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _az_now_iso() -> str:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/Phoenix")).isoformat(timespec="seconds")


# ── Gatherers (each degrades to an {"error": ...} dict, never raises) ────────
def _gates() -> dict:
    """The 4 _EXECUTION_ENABLED gates, verified LIVE off the source files."""
    out: dict = {}
    for name, rel in GATE_FILES.items():
        p = REPO_ROOT / rel
        try:
            out[name] = bool(_GATE_RE.search(p.read_text()))
        except Exception as e:
            out[name] = None
            out.setdefault("_errors", []).append(f"{name}: {type(e).__name__}")
    truths = [v for k, v in out.items() if not k.startswith("_")]
    out["all_pass"] = all(v is True for v in truths) and len(truths) == 4
    return out


def _regime() -> dict:
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            "SELECT date, regime, size_modifier, spy_close, ma_8, ma_21, "
            "cross_days_ago FROM regime_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not r:
            return {"regime": None}
        return {
            "regime": r["regime"], "size_modifier": r["size_modifier"],
            "spy_close": r["spy_close"], "ma_8": r["ma_8"], "ma_21": r["ma_21"],
            "cross_days_ago": r["cross_days_ago"], "as_of": r["date"],
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def _fleet_counts() -> dict:
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
        rows = dict(conn.execute(
            "SELECT halt_mode, COUNT(*) FROM ai_players GROUP BY halt_mode"
        ).fetchall())
        conn.close()
        active = int(rows.get("active", 0))
        exit_only = int(rows.get("exit_only", 0))
        full = int(rows.get("full", 0))
        return {"active": active, "exit_only": exit_only, "full": full,
                "total": active + exit_only + full}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def _unpushed() -> dict:
    """Commits ahead of upstream. None if no upstream / not a checkout."""
    try:
        out = subprocess.run(
            ["git", "rev-list", "--count", "@{u}..HEAD"],
            cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=10,
        )
        if out.returncode != 0:
            return {"count": None, "note": out.stderr.strip()[:80] or "no upstream"}
        return {"count": int(out.stdout.strip())}
    except Exception as e:
        return {"count": None, "note": f"{type(e).__name__}: {e}"}


def _port_up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return True
    except Exception:
        return False


def _ollama_up() -> bool:
    import urllib.request
    try:
        req = urllib.request.Request(f"{OLLIE_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status == 200
    except Exception:
        return False


def _services() -> dict:
    trader = _port_up(8080)
    sigcenter = _port_up(9000)
    ollama = _ollama_up()
    return {
        "trader_8080": trader,
        "signal_center_9000": sigcenter,
        "ollama_168": ollama,
        "all_up": bool(trader and sigcenter and ollama),
    }


def _briefings() -> dict:
    """Last-delivered timestamp per Kirk briefing mode, from .md.sent sidecars."""
    out: dict = {}
    now = _utc_now()
    for mode in BRIEFING_MODES:
        matches = sorted(glob.glob(str(BRIEFINGS_DIR / f"*_{mode}.md.sent")))
        if not matches:
            out[mode] = {"sent_at": None, "age_min": None}
            continue
        latest = matches[-1]  # filenames are date-prefixed → lexically sortable
        try:
            sent_at = json.loads(Path(latest).read_text()).get("sent_at")
            dt = datetime.fromisoformat(sent_at.replace("Z", "+00:00"))
            age = round((now - dt).total_seconds() / 60.0, 1)
            out[mode] = {"sent_at": sent_at, "age_min": age}
        except Exception:
            out[mode] = {"sent_at": None, "age_min": None}
    return out


# ── Public API ───────────────────────────────────────────────────────────────
def get_fleet_status() -> dict:
    """The single fleet-truth dict. Read-only; never raises (sub-failures
    surface as per-section error fields)."""
    gates = _gates()
    services = _services()
    return {
        "generated_at_az": _az_now_iso(),
        "ok": bool(gates.get("all_pass") and services.get("all_up")),
        "gates": gates,
        "regime": _regime(),
        "fleet": _fleet_counts(),
        "git": _unpushed(),
        "services": services,
        "briefings": _briefings(),
    }


def format_status(d: dict) -> str:
    g = d.get("gates", {})
    r = d.get("regime", {})
    f = d.get("fleet", {})
    s = d.get("services", {})
    git = d.get("git", {})
    b = d.get("briefings", {})

    def chk(v):
        return "✓" if v is True else ("✗" if v is False else "?")

    L = [f"FLEET STATUS · {d.get('generated_at_az','')}  [{'OK' if d.get('ok') else 'ATTN'}]"]
    L.append(f"  Gates       {chk(g.get('all_pass'))} all_pass  "
             + " ".join(f"{k}={chk(v)}" for k, v in g.items()
                        if k not in ('all_pass',) and not k.startswith('_')))
    L.append(f"  Regime      {r.get('regime','?')} (size×{r.get('size_modifier','?')}, "
             f"SPY {r.get('spy_close','?')} vs MA8 {r.get('ma_8','?')}/MA21 {r.get('ma_21','?')})")
    L.append(f"  Fleet       active={f.get('active','?')} exit_only={f.get('exit_only','?')} "
             f"full={f.get('full','?')} (total {f.get('total','?')})")
    L.append(f"  Git         unpushed={git.get('count')}"
             + (f" ({git.get('note')})" if git.get('note') else ""))
    L.append(f"  Services    trader:8080 {chk(s.get('trader_8080'))}  "
             f"signal:9000 {chk(s.get('signal_center_9000'))}  "
             f"ollama.168 {chk(s.get('ollama_168'))}")
    L.append("  Briefings   " + "  ".join(
        f"{m}={'—' if b.get(m,{}).get('age_min') is None else str(b[m]['age_min'])+'m'}"
        for m in BRIEFING_MODES))
    return "\n".join(L)


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="One-command fleet status (read-only)")
    ap.add_argument("--json", action="store_true", help="emit raw JSON")
    args = ap.parse_args()
    d = get_fleet_status()
    print(json.dumps(d, indent=2) if args.json else format_status(d))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
