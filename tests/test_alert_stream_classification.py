"""HM-BUG-BATCH-2026-07-10 item 7 — ALERT STREAM SEPARATION.

Ops/health sentinel alerts and trading-signal alerts shared one
notification channel and looked identical -- a "database is locked"
warning and a stock tip rendered as the same toast. These tests pin
engine.alert_channels.classify_alert_stream(), using the ticket's own
examples verbatim: "HM-OPS-*, heartbeat, backlog, DB errors" -> ops;
"ORB, MACD, replay matches" -> signal.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.alert_channels import classify_alert_stream  # noqa: E402


@pytest.mark.parametrize("alert_type", [
    "sentinel_signals_v2_queue",       # backlog
    "sentinel_rikers_heartbeat",       # heartbeat
    "sentinel_lock_errors",            # DB errors -- the ticket's literal example
    "sentinel_fd_count",
    "hm-u-close_position-ConnectionError",   # architecture-class unhandled exception
    "hm-u-reconciliation-ValueError",
    "hm-push-health-warning",
    "hm-at-gamma-schwab-cadence-red_alert",
    "hm-i-b-item5-drift-2026-07-10",
    "hm-holly-live-error",
    "source-health-watcher-dead",
    "source-health-watcher-stale",
    "scan_liveness",
    "war_room_slow_cycle",
    "wr_layer1_watch_summary",
    "event_tape_staleness",
    "scotty-kirk-ingest",
    "eod_report",
    "congress_scrape_zero",
    "degenerate_confidence",
    "tuning_crew_zero_adjustments",
    "tuning_crew_zero_scored",
    "holdings_stale",
    "guardian_sweep_sells",
    "troi_csp_cap_breach",
    "deployment_floor",  # HM-DEPLOYMENT-FLOOR: fleet capital state, not a trade idea
    "alert_channel",  # unclassified fallback -- conservative default
])
def test_classifies_as_ops(alert_type):
    assert classify_alert_stream(alert_type) == "ops"


@pytest.mark.parametrize("alert_type", [
    "dyn_rsi_oversold_AAPL",
    "dyn_macd_crossover_TSLA",          # MACD -- the ticket's literal example
    "dyn_volume_spike_NVDA",
    "bk_orb_bull_AAPL",                 # ORB -- the ticket's literal example
    "bk_box_bear_SPY",
    "bk_avwap_bull_QQQ",
    "replay_match:mccoy:AAPL:BUY_CALL", # replay matches -- the ticket's literal example
    "stuck_stop:navigator:TSLA",
    "spread-fill-1234",
    "spread-exit-profit_target-1234",
    "hm-csp-assigned-SPY",
    "hm-csp-assign-failed-SPY",
    "hm-v-single-BUY",
    "hm-v-spread-open-bull_put",
    "auto-spread-gate-flip",
    "auto-spread-submit",
    "strategy_lab_proposal",
    "ollietrades_signal",
    "user_price_above",
    "bbkc_squeeze_release_TSLA",
])
def test_classifies_as_signal(alert_type):
    assert classify_alert_stream(alert_type) == "signal"


def test_none_and_empty_default_to_signal():
    assert classify_alert_stream(None) == "signal"
    assert classify_alert_stream("") == "signal"


def test_classification_is_case_insensitive():
    assert classify_alert_stream("SENTINEL_LOCK_ERRORS") == "ops"
    assert classify_alert_stream("Hm-U-Close_Position-Foo") == "ops"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
