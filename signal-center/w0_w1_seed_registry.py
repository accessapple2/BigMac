#!/usr/bin/env python3
"""Seed source_registry (W1 §2.3). Idempotent UPSERT — never deletes rows.

ts_format families (resolved by engine.source_gate.source_freshness):
  db_max:<table>.<col>      -> SELECT max(col) from signals.db (no network)
  bridge_iso:<ep>:<dotpath> -> _bridge_get(ep) then walk dotted path, parse ISO
  file_mtime:<glob>         -> newest matching file's mtime
  manual                    -> no automatic ts (UNKNOWN unless updated by hand)
  none                      -> archive; never gated, never freshness-checked

cadence/criticality assignments are per W1 §2.3. Webull -> archive, enabled=0.
Snapshot base cadence (Schwab 3d, metals 7d) also lives in
engine.source_gate.SNAPSHOT_CADENCE_DAYS; stored in notes for transparency.
"""
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")

# (source_id, display_name, endpoint, cadence_class, criticality, ts_field, ts_format, enabled, notes)
ROWS = [
    # ── signal-center DB-backed sources (no network) ──────────────────────────
    ("signals",          "Trade Signals",     "(signals.db)",          "intraday", "live_decision", "created_at",   "db_max:trade_signals.created_at",   1, "canonical signal feed; scored by W0"),
    ("predictions",      "Predictions",       "(signals.db)",          "daily",    "context",       "created_at",   "db_max:predictions.created_at",     1, "legacy daily snapshot predictions"),
    ("signal_outcomes",  "Signal Outcomes",   "(signals.db)",          "intraday", "context",       "last_updated", "db_max:signal_outcomes.last_updated", 1, "path-aware tracker; W0 substrate parent"),
    ("intelligence",     "Intelligence Feed", "(signals.db)",          "daily",    "context",       "created_at",   "db_max:intelligence_feed.created_at", 1, ""),
    ("execution_log",    "Execution Log",     "(signals.db)",          "intraday", "context",       "executed_at",  "db_max:execution_log.executed_at",  1, ""),
    ("daily_snapshot",   "Daily Snapshot",    "(signals.db)",          "daily",    "context",       "created_at",   "db_max:daily_snapshot.created_at",  1, ""),
    # ── live-decision sources (these GATE consensus/expectancy when RED) ───────
    ("riker_synthesis",  "Riker Synthesis",   "/api/riker/synthesis",  "daily",    "live_decision", "timestamp",    "bridge_iso:/api/riker/synthesis:timestamp", 1, "XO 10-min roll-up; shown as Intelligence"),
    ("bridge_consensus", "Consensus Briefing","/api/bridge/consensus", "daily",    "live_decision", "created_at",   "bridge_iso:/api/bridge/consensus:created_at", 1, "drives per-ticker matrix; refuse if stardate > 1 trading day"),
    # ── trader.db-backed advisory (cross-DB db_max selector) ──────────────────
    ("kirk_advisory",    "Kirk Advisory",     "(trader.db)",           "daily",    "context",       "created_at",   "db_max:trader:kirk_advisory_log.created_at", 1, "Oracle advisory surface; producer DEAD since 2026-05-18 — flags RED until re-homed (see DAEMON-GRAVEYARD rehome plan)"),
    # ── bridge context sources ────────────────────────────────────────────────
    ("cto_briefing",     "CTO Briefing",      "/api/cto/briefing",     "daily",    "context",       "latest.created_at", "bridge_iso:/api/cto/briefing:latest.created_at", 1, "44KB Spock report"),
    ("macro",            "Macro / FRED",      "/api/macro",            "monthly",  "context",       "consumer_sentiment.date", "bridge_iso:/api/macro:consumer_sentiment.date", 1, "slow cadence expected — fine"),
    ("movers",           "Movers",            "/api/movers",           "intraday", "context",       "fetched_at",   "bridge_iso:/api/movers:fetched_at", 1, ""),
    ("holdings_top",     "Holdings Top",      "/api/holdings-top",     "intraday", "context",       "timestamp",    "bridge_iso:/api/holdings-top:timestamp", 1, ""),
    ("morning_brief",    "Morning Brief",     "/api/morning-brief",    "daily",    "context",       "generated_at", "bridge_iso:/api/morning-brief:generated_at", 1, ""),
    ("scanner_status",   "Scanner Status",    "/api/scanner/status",   "intraday", "context",       "fetched_at",   "bridge_iso:/api/scanner/status:fetched_at", 1, "best-effort ts; UNKNOWN if absent"),
    # ── snapshot imports ──────────────────────────────────────────────────────
    ("schwab_snapshot",  "Schwab Snapshot",   "(CSV import)",          "snapshot", "context",       "mtime",        "file_mtime:/Users/bigmac/autonomous-trader/inbox/*.csv", 1, "3-day manual CSV import; cadence_days=3"),
    ("metals",           "Metals Holdings",   "(manual)",              "snapshot", "context",       "",             "manual", 1, "physical holdings; cadence_days=7"),
    # ── archive / quarantined (never live) ────────────────────────────────────
    ("webull_trades",    "Webull Trades",     "/api/webull/trades",    "archive",  "archive",       "",             "none", 0, "ARCHIVE/NOT LIVE — liquidated 2026-02-27 (~94d); never in any live path"),
]

UPSERT = """
INSERT INTO source_registry
  (source_id, display_name, endpoint, cadence_class, criticality, ts_field, ts_format, enabled, notes)
VALUES (?,?,?,?,?,?,?,?,?)
ON CONFLICT(source_id) DO UPDATE SET
  display_name=excluded.display_name, endpoint=excluded.endpoint,
  cadence_class=excluded.cadence_class, criticality=excluded.criticality,
  ts_field=excluded.ts_field, ts_format=excluded.ts_format,
  notes=excluded.notes
  -- NOTE: do NOT overwrite `enabled` on re-seed — manual quarantine wins.
"""


def main():
    db = sqlite3.connect(DB_PATH)
    for r in ROWS:
        existing = db.execute("SELECT enabled FROM source_registry WHERE source_id=?", (r[0],)).fetchone()
        if existing is None:
            db.execute(UPSERT, r)
        else:
            # preserve operator-set enabled flag
            db.execute(UPSERT, (r[0], r[1], r[2], r[3], r[4], r[5], r[6], existing[0], r[8]))
    db.commit()
    n = db.execute("SELECT COUNT(*) FROM source_registry").fetchone()[0]
    print("source_registry rows:", n)
    for row in db.execute("SELECT source_id, cadence_class, criticality, enabled FROM source_registry ORDER BY criticality, source_id"):
        print("  %-18s %-9s %-13s enabled=%d" % row)
    db.close()


if __name__ == "__main__":
    main()
