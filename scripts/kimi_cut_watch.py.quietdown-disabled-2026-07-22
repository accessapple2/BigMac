#!/usr/bin/env python3
"""HM-KIMI-CUT-WATCH — let the data call the cut when the sample is fair.

ollama-kimi sits at 22 closed / −0.67 Sharpe — 3 snaps shy of the CUT bar
(negative Sharpe AND >=25 closed). This NTFYs the Captain the moment it crosses
that bar, so the cut is made on a fair sample, not a thin one. NTFY-ONLY — it
NEVER auto-halts (the Captain confirms). Fires once (state-file guard), then quiet.

Local cron, read-only DB, stdlib only. Same Sharpe methodology as
engine/agent_scorecard.py (return-on-cost, clipped ±5R, clean-trade boundary).
"""
import socket
import sqlite3
import statistics
import sys
import threading
import urllib.request
from pathlib import Path

ROOT = Path("/Users/bigmac/autonomous-trader")
DB = ROOT / "data" / "trader.db"
STATE = ROOT / "data" / "kimi_cut_watch.state"
NTFY = "https://ntfy.sh/ollietrades-admin"
AGENT = "ollama-kimi"
MIN_SNAPS = 25

_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def _ntfy(title: str, body: str) -> None:
    """HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10: this box has no working IPv6
    route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, 2026-07-07) — forces IPv4 via a
    local getaddrinfo monkeypatch."""
    try:
        req = urllib.request.Request(
            NTFY, data=body.encode("utf-8"),
            headers={"Title": title.encode("ascii", "replace").decode("ascii"),
                     "Priority": "default", "Tags": "balance_scale,chart"},
            method="POST")
        with _ntfy_ipv4_lock:
            socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                urllib.request.urlopen(req, timeout=8)
            finally:
                socket.getaddrinfo = _orig_getaddrinfo
    except Exception as e:
        print(f"[kimi-cut-watch] ntfy failed: {e}", file=sys.stderr)


def main() -> int:
    if STATE.exists():   # already fired once — stay quiet
        return 0
    try:
        c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=15)
        rows = c.execute(
            "SELECT asset_type, qty, entry_price, realized_pnl FROM trades "
            "WHERE player_id=? AND realized_pnl IS NOT NULL AND realized_pnl != 0 "
            "AND (known_contaminated IS NULL OR known_contaminated = 0)",
            (AGENT,)).fetchall()
        c.close()
    except Exception as e:
        print(f"[kimi-cut-watch] db read failed: {e}", file=sys.stderr)
        return 1

    n = len(rows)
    rets = []
    for at, qty, ep, pnl in rows:
        if ep and qty:
            mult = 100.0 if (at or "") == "option" else 1.0
            cost = abs(float(ep) * float(qty) * mult)
            if cost > 0:
                rets.append(max(-5.0, min(5.0, float(pnl) / cost)))
    sharpe = (statistics.mean(rets) / statistics.stdev(rets)
              if len(rets) > 1 and statistics.stdev(rets) > 0 else 0.0)

    if n >= MIN_SNAPS and sharpe < 0:
        _ntfy(f"⚖️ {AGENT} reached cut threshold",
              f"{AGENT}: {n} closed trades, Sharpe {sharpe:+.2f} (still negative). "
              f"It cleared the {MIN_SNAPS}-snap CUT bar on a fair sample — your call to halt "
              f"(no auto-halt). To cut: halt_mode='full'.")
        STATE.write_text(f"{n},{sharpe:.3f}")
        print(f"[kimi-cut-watch] NTFYed — {AGENT} {n} snaps, Sharpe {sharpe:+.2f}")
    else:
        print(f"[kimi-cut-watch] {AGENT}: {n} snaps, Sharpe {sharpe:+.2f} "
              f"({'positive — no cut' if sharpe >= 0 else f'<{MIN_SNAPS} snaps, hold'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
