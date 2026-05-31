#!/usr/bin/env python3
"""HM-PRODUCER-RETIRE — mark the 2 superseded legacy producers RETIRED in the
source-health set, and reflect that the `signals` feed is no longer a live trade
input. Idempotent. NO data rows touched (trade_signals untouched).

- etf_regime_trader / options_flow_scanner: legacy-only producers (lived only in
  /Users/bigmac/ollietrades, archived 2026-05-31). Marked criticality='retired',
  cadence_class='retired', enabled=0 -> /api/sources/health renders RETIRED, not
  a RED fault. These are health-grid markers only (not substrate provenance —
  W0 keys substrate on 'signals', so historical rows stay scoreable).

- signals: demoted live_decision -> context. Per HM-PRODUCER-RETIRE consumer
  check, NOTHING that trades reads trade_signals (neo-matrix consumes it
  observation-only via exit_only; rest is display/tracking/W0 scoring). So its
  staleness must not flag the consensus gate degraded. (riker_synthesis UNKNOWN
  is a separate, pre-existing live_decision item — intentionally NOT touched.)
"""
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")

RETIRED_PRODUCERS = [
    ("etf_regime_trader", "ETF Regime Trader (RETIRED)", "(archived)", "retired", "retired", "", "none", 0,
     "RETIRED 2026-05-31 HM-PRODUCER-RETIRE. Legacy-only (/ollietrades, archived). 10d-edge rebuild candidate gated on HM-VALIDATION-RIGOR deflation."),
    ("options_flow_scanner", "Options Flow Scanner (RETIRED)", "(archived)", "retired", "retired", "", "none", 0,
     "RETIRED 2026-05-31 HM-PRODUCER-RETIRE. Legacy-only (/ollietrades, archived). Superseded by HM-FLOW-NATIVE."),
]


def main():
    db = sqlite3.connect(DB_PATH)
    for r in RETIRED_PRODUCERS:
        db.execute("""
            INSERT INTO source_registry
              (source_id, display_name, endpoint, cadence_class, criticality, ts_field, ts_format, enabled, notes)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_id) DO UPDATE SET
              display_name=excluded.display_name, cadence_class=excluded.cadence_class,
              criticality=excluded.criticality, enabled=excluded.enabled, notes=excluded.notes
        """, r)
    # demote signals -> context (no live trade reads it; see docstring)
    db.execute("UPDATE source_registry SET criticality='context', "
               "notes=COALESCE(notes,'')||' | demoted live_decision->context 2026-05-31 (write-orphaned for trading; W0 substrate only)' "
               "WHERE source_id='signals' AND criticality!='context'")
    db.commit()
    print("=== retired producers + signals criticality ===")
    for row in db.execute("SELECT source_id, cadence_class, criticality, enabled FROM source_registry "
                          "WHERE source_id IN ('etf_regime_trader','options_flow_scanner','signals') ORDER BY source_id"):
        print("  %-20s %-9s %-13s enabled=%d" % row)
    db.close()


if __name__ == "__main__":
    main()
