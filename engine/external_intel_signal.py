"""
engine/external_intel_signal.py — HM-TI-CONVERGENCE-VOTE
========================================================

Promotes Trade-Ideas (and other external intel sources) from shadow-tracked
benchmark to a convergence vote in the fleet's scheduled scoring path
(engine/strategies.py::scan_strategies).

DESIGN (revised 2026-06-04 post-discovery)
------------------------------------------
- Reads external_picks rows within LOOKBACK_HOURS for sources in PROMOTED_SOURCES
- Emits trigger dicts in shape (a) — the run_strategies trigger contract,
  consumed by engine.strategies.score_convergence
- Synthesizes target_price from entry+stop using REWARD_MULTIPLE (TI emails
  don't carry a target; score_convergence requires R/R >= 1.5)
- Skips picks with null/invalid entry/stop (entry > stop required for long bias)
- Most-recent-pick-per-ticker wins (rare collision but handled)

INTEGRATION (handled in engine/strategies.py::scan_strategies)
--------------------------------------------------------------
Before the per-ticker loop:
    from engine.external_intel_signal import collect_by_ticker as collect_ti_triggers
    ti_triggers = collect_ti_triggers()       # one DB read per scan

Inside the per-ticker loop, immediately AFTER triggered = run_strategies(...):
    ti_trigger = ti_triggers.get(ticker)
    if ti_trigger and triggered:              # GATE ON: never trigger alone
        triggered.append(ti_trigger)
        logger.info("HM-TI-CONVERGENCE-VOTE: appended TI vote for %s", ticker)

The `and triggered` gate matches Admiral's intent: TI is purely additive —
can lift existing fleet convergence over the threshold, can never trigger
a trade alone.

DICT SHAPE (matches strategies.py run_strategies output, lines 399-407)
-----------------------------------------------------------------------
{
    "signal_type":  "BUY",
    "name":         "TI:<source>",
    "type":         "EXTERNAL_INTEL",
    "desc":         "Trade-Ideas swing pick — entry $X, stop $Y (pick_id=N)",
    "entry_price":  float,
    "stop_price":   float,
    "target_price": float,   # = entry + REWARD_MULTIPLE × (entry - stop)
}

SACRED-DATA
-----------
Read-only on external_picks. Writes nothing. Errors swallowed — this
function NEVER raises into the scoring path. Returns empty dict on any
error.

CALIBRATION KNOBS (top of file)
-------------------------------
- LOOKBACK_HOURS:    24h captures one trading day's TI batch.
- PROMOTED_SOURCES:  source-name whitelist. Expand as performance allows.
- REWARD_MULTIPLE:   2.0 → synthesized R/R = 2.0 (safe margin above
                     score_convergence's 1.5 floor).
"""

from __future__ import annotations

import logging
import sqlite3
from engine.db_conn import get_conn
import contextlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── CONFIG ──────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"

LOOKBACK_HOURS = 24

PROMOTED_SOURCES = {
    "TI Swing Picks (email)",
}

# target_price = entry + REWARD_MULTIPLE × (entry - stop)
# 2.0 → R/R of 2.0, safely above score_convergence's 1.5 floor.
REWARD_MULTIPLE = 2.0


# ── PUBLIC API ──────────────────────────────────────────────────────────────

def collect_by_ticker() -> dict[str, dict[str, Any]]:
    """Return TI picks as run_strategies-compatible trigger dicts, keyed by ticker.

    This is the primary consumer entry point — used by engine/strategies.py
    to inject a TI vote into the per-ticker triggered list before
    score_convergence runs.

    Each value matches the shape produced by run_strategies and consumed
    by score_convergence:
        {
            "signal_type":  "BUY",
            "name":         str,
            "type":         "EXTERNAL_INTEL",
            "desc":         str,
            "entry_price":  float,
            "stop_price":   float,
            "target_price": float,    # = entry + REWARD_MULTIPLE × risk
        }

    Rules:
      - Skips rows with null entry or stop
      - Skips rows where stop >= entry (would make risk ≤ 0)
      - On ticker collision, most-recent submitted_at wins (DESC + skip-if-seen)

    Returns empty dict on any error — never raises into the scoring path.
    """
    out: dict[str, dict[str, Any]] = {}

    try:
        if not PROMOTED_SOURCES:
            return {}

        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        ).isoformat()

        placeholders = ",".join("?" * len(PROMOTED_SOURCES))
        query = f"""
            SELECT id, source, pick_date, ticker, action, entry, stop, note, submitted_at
            FROM external_picks
            WHERE submitted_at >= ?
              AND source IN ({placeholders})
            ORDER BY submitted_at DESC
        """

        with contextlib.closing(get_conn(str(DB_PATH))) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, (cutoff, *PROMOTED_SOURCES)).fetchall()

        kept = 0
        skipped = 0
        for row in rows:
            entry = row["entry"]
            stop  = row["stop"]

            if entry is None or stop is None:
                skipped += 1
                continue
            if stop >= entry:
                logger.warning(
                    "external_intel: skip %s — stop %.2f >= entry %.2f (pick_id=%d)",
                    row["ticker"], stop, entry, row["id"],
                )
                skipped += 1
                continue

            ticker = row["ticker"]

            # Most-recent-wins on collision (DESC order, first-seen kept)
            if ticker in out:
                continue

            risk   = entry - stop
            target = entry + REWARD_MULTIPLE * risk

            out[ticker] = {
                "signal_type":  "BUY",
                "name":         f"TI:{row['source']}",
                "type":         "EXTERNAL_INTEL",
                "desc": (
                    f"Trade-Ideas swing pick — entry ${entry:.2f}, "
                    f"stop ${stop:.2f} (pick_id={row['id']})"
                ),
                "entry_price":  float(entry),
                "stop_price":   float(stop),
                "target_price": float(target),
            }
            kept += 1

        logger.info(
            "external_intel.collect_by_ticker: kept=%d skipped=%d "
            "(lookback=%dh, R/R=%.1f)",
            kept, skipped, LOOKBACK_HOURS, REWARD_MULTIPLE,
        )
        return out

    except Exception as e:
        logger.error(
            "external_intel.collect_by_ticker failed: %s: %s",
            type(e).__name__, e,
        )
        return {}


def collect() -> list[dict[str, Any]]:
    """Flat list of triggers (debugging/logging). Production wire uses
    collect_by_ticker() instead — the scan_strategies loop needs lookup.
    """
    return list(collect_by_ticker().values())


# ── CLI smoke-test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    triggers = collect_by_ticker()
    print(f"\n{len(triggers)} live external-intel triggers "
          f"(lookback={LOOKBACK_HOURS}h, R/R={REWARD_MULTIPLE}):\n")
    for ticker, t in triggers.items():
        rr = ((t['target_price'] - t['entry_price']) /
              (t['entry_price'] - t['stop_price']))
        print(f"  {ticker:6s} entry=${t['entry_price']:.2f} "
              f"stop=${t['stop_price']:.2f} target=${t['target_price']:.2f} "
              f"R/R={rr:.2f}")
        print(f"         name={t['name']!r}")
