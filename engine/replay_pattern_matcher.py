"""HM-REPLAY-PATTERN-ALERT — match live agent signals against historical winners.

Builds a pattern library from winning round-trips (``pnl_pct >= threshold``),
grouped by ``(player_id, direction)``, then flags fresh live BUY/SHORT signals
whose agent + direction + confidence resemble a past winner's entry.

Data-model notes (verified 2026-06-05 against dashboard.app.trade_round_trips):
  * ``pnl_pct`` and ``direction`` are NOT stored columns — they are derived the
    same way the /api/trades/round-trips endpoint derives them:
      - direction: action LIKE 'SELL%' -> 'long', action = 'COVER' -> 'short'
      - long  pnl_pct = (exit - entry) / entry * 100
      - short pnl_pct = (entry - exit) / entry * 100
  * Fingerprints come from round-trip ENTRIES. A live ENTRY signal is BUY
    (opens a long) or SHORT (opens a short). SELL/COVER are EXITS and HOLD /
    BUY_CALL are skipped — matching an exit against an entry pattern is noise.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

_DEFAULT_DB = os.path.expanduser("~/autonomous-trader/data/trader.db")

# Module-level NTFY dedup: "player:symbol:signal" -> first-fired epoch. TTL 2h.
# Lives at module scope so it survives the per-request matcher instances the
# endpoint creates; only the NTFY push is gated by it (toasts dedup client-side).
_FIRED: dict = {}
_FIRED_TTL_SECS = 2 * 3600

# A live signal is only "fresh" enough to alert on if emitted this recently.
_FRESH_MINUTES = 15

# Confidence floor clamp — a single low-confidence winner shouldn't drag an
# agent's fingerprint floor down to ~0 and make every weak live signal match.
_MIN_CONFIDENCE_FLOOR = 0.60

# Live signal -> entry direction. Entries only (see module docstring).
_SIGNAL_DIRECTION = {"BUY": "long", "SHORT": "short"}


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    """Parse a canonical space-UTC DB timestamp to a NAIVE datetime, tolerant of
    'T' / fractional-second suffixes. Naive on purpose: freshness compares DB
    timestamps to each other in their own (writer) frame — never to a process
    clock, which is unreliable in the long-running trader (see module note in
    ReplayPatternMatcher.match_live_signals). Returns None on unparseable input."""
    if not value:
        return None
    txt = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(txt[:19] if fmt.endswith("%S") else txt[:16], fmt)
        except Exception:
            continue
    return None


class ReplayPatternMatcher:
    """Builds winner fingerprints and matches them against live signals."""

    def __init__(self, db_path: str = _DEFAULT_DB):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        # Read-only URI connection — never contends with the live trader's
        # writes (per external-write-starvation doctrine, reads are safe).
        c = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=10)
        c.row_factory = sqlite3.Row
        return c

    def build_winner_fingerprints(
        self, min_pnl_pct: float = 3.0, season: Optional[int] = None
    ) -> dict:
        """Return ``{(player_id, direction): fingerprint}`` for agents that have
        closed round-trips at ``pnl_pct >= min_pnl_pct``.

        Mirrors trade_round_trips' derivation: direction + pnl_pct are computed
        in Python because neither is a stored column.
        """
        sql = """
            SELECT t.player_id, t.symbol, t.action,
                   t.entry_price, t.exit_price, t.confidence
              FROM trades t
             WHERE (t.action LIKE 'SELL%' OR t.action = 'COVER')
               AND t.entry_price > 0
               AND t.exit_price  > 0
               AND t.realized_pnl IS NOT NULL
        """
        params: list = []
        if season is not None:
            sql += " AND t.season = ?"
            params.append(int(season))

        conn = self._conn()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()

        # Accumulate per (player_id, direction).
        groups: dict = {}
        for r in rows:
            action = (r["action"] or "").upper()
            direction = "long" if action.startswith("SELL") else (
                "short" if action == "COVER" else "unknown"
            )
            if direction == "unknown":
                continue
            ep = r["entry_price"] or 0
            xp = r["exit_price"] or 0
            if not ep or ep <= 0:
                continue
            pnl_pct = (ep - xp) / ep * 100.0 if direction == "short" else (xp - ep) / ep * 100.0
            if pnl_pct < min_pnl_pct:
                continue
            conf = float(r["confidence"] or 0.0)
            key = (r["player_id"], direction)
            g = groups.setdefault(key, {
                "pnls": [], "confs": [],
                "best_symbol": r["symbol"], "best_pnl": pnl_pct,
            })
            g["pnls"].append(pnl_pct)
            g["confs"].append(conf)
            if pnl_pct > g["best_pnl"]:
                g["best_pnl"] = pnl_pct
                g["best_symbol"] = r["symbol"]

        fingerprints: dict = {}
        for (player_id, direction), g in groups.items():
            n = len(g["pnls"])
            # Floor = lowest confidence among winning entries, clamped so one
            # low-conf winner can't open the gate to every weak live signal.
            observed_floor = min(g["confs"]) if g["confs"] else 0.0
            min_conf = max(_MIN_CONFIDENCE_FLOOR, observed_floor)
            fingerprints[(player_id, direction)] = {
                "player_id": player_id,
                "direction": direction,
                "min_confidence": round(min_conf, 4),
                "avg_pnl_pct": round(sum(g["pnls"]) / n, 4),
                "sample_count": n,
                "best_example_symbol": g["best_symbol"],
                "best_example_pnl_pct": round(g["best_pnl"], 4),
            }
        return fingerprints

    def match_live_signals(
        self, signals: list, fingerprints: Optional[dict] = None,
        min_pnl_pct: float = 3.0, season: Optional[int] = None,
    ) -> list:
        """Return live signals (sorted by match_score desc) whose agent +
        entry-direction + confidence resemble a winning fingerprint."""
        if fingerprints is None:
            fingerprints = self.build_winner_fingerprints(min_pnl_pct, season)

        # Freshness reference is the NEWEST signal's own timestamp, NOT a process
        # clock. The long-running trader process runs ~7h behind real UTC (its
        # time.gmtime()/datetime.now()/pytz/zoneinfo are all skewed; verified via
        # /api/_tz_probe 2026-06-05), so any process-now vs DB-created_at compare
        # is meaningless. created_at values share one writer frame, so comparing
        # them to max(created_at) is skew-proof: "fresh" = within _FRESH_MINUTES
        # of the most recent signal.
        parsed = [(_parse_dt(s.get("created_at")), s) for s in (signals or [])]
        stamps = [d for d, _ in parsed if d is not None]
        if not stamps:
            return []
        reference = max(stamps)
        cutoff = reference - timedelta(minutes=_FRESH_MINUTES)
        matches: list = []
        for created, s in parsed:
            sig = (s.get("signal") or "").upper()
            direction = _SIGNAL_DIRECTION.get(sig)
            if not direction:                       # HOLD / SELL / COVER / BUY_CALL
                continue
            if created is None or created < cutoff:  # stale or unparseable
                continue
            fp = fingerprints.get((s.get("player_id"), direction))
            if not fp:
                continue
            conf = float(s.get("confidence") or 0.0)
            if conf < fp["min_confidence"]:
                continue
            reasoning = (s.get("reasoning") or "").replace("\n", " ").strip()
            matches.append({
                "symbol": s.get("symbol"),
                "player_id": s.get("player_id"),
                "display_name": s.get("display_name") or s.get("player_id"),
                "signal": sig,
                "confidence": round(conf, 4),
                "direction": direction,
                "matched_fingerprint": {
                    "avg_pnl_pct": fp["avg_pnl_pct"],
                    "sample_count": fp["sample_count"],
                    "best_example_symbol": fp["best_example_symbol"],
                    "best_example_pnl_pct": fp["best_example_pnl_pct"],
                },
                "reasoning_snippet": reasoning[:120],
                "created_at": s.get("created_at"),
                "match_score": round(conf * fp["sample_count"] * fp["avg_pnl_pct"] / 100.0, 4),
            })
        matches.sort(key=lambda m: m["match_score"], reverse=True)
        return matches

    @staticmethod
    def should_notify(match: dict) -> bool:
        """True at most once per (player:symbol:signal) per 2h — gates the
        server-side NTFY push only (toasts dedup client-side per session)."""
        import time as _t
        now = _t.time()
        # Purge expired keys so the dict can't grow unbounded.
        for k in [k for k, ts in _FIRED.items() if now - ts > _FIRED_TTL_SECS]:
            _FIRED.pop(k, None)
        key = f"{match.get('player_id')}:{match.get('symbol')}:{match.get('signal')}"
        if key in _FIRED:
            return False
        _FIRED[key] = now
        return True
