"""HM-BUG-BATCH-2026-07-10 item 10 — /api/portfolio/real Schwab staleness.

Bug: schwab.last_updated reported "2026-07-09" (today) while the underlying
holdings snapshot was actually from 2026-06-05 -- 34 days old. last_updated
reflects "when the sync script last ran," not "how fresh the data is."
Root cause confirmed live: the Schwab OAuth refresh token is expired/revoked
(logs/schwab_live_sync.log shows "Refresh token is invalid, expired or
revoked" on every RTH cron cycle), so the live-API path fails every 15
minutes and silently falls back to re-stamping the same month-old CSV
snapshot with a fresh last_updated timestamp.

Fixed by adding a dedicated snapshot_ts field (both sync scripts) and
computing snapshot_age_days/snapshot_stale (>7 days) from it in
dashboard/app.py::portfolio_real(), with a fallback parse from the legacy
`notes` text for files written before snapshot_ts existed.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _write_holdings(tmp_path, schwab_block):
    rh_path = tmp_path / "real_holdings.json"
    rh_path.write_text(json.dumps({
        "last_updated": datetime.now().strftime("%Y-%m-%d"),
        "accounts": {"schwab": schwab_block},
    }))
    return rh_path


def test_stale_snapshot_flagged_past_seven_days(tmp_path):
    """The literal reported scenario: a snapshot_ts from over a month ago
    must be flagged stale with the real age in days, even though
    last_updated (unrelated, sync-run-time) says today."""
    import dashboard.app as app_module

    old_snapshot = (datetime.now() - timedelta(days=34)).strftime("%Y-%m-%dT%H:%M:%S")
    rh_path = _write_holdings(tmp_path, {
        "cash_balance": 28053.90,
        "positions": [],
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S EDT"),  # "today" -- the misleading field
        "snapshot_ts": old_snapshot,
        "notes": f"Auto-synced from schwab_holdings snapshot 42. 0 equity positions. Snapshot time: {old_snapshot}.",
    })

    # portfolio_real() builds its path via Path(__file__).resolve().parent.parent
    # / "data" / "real_holdings.json" and reads it with open() -- patching
    # Path itself is brittle against that exact expression, so intercept
    # open() instead (what the function actually calls) and redirect just
    # the real_holdings.json read to our tmp fixture.
    real_open = open
    def _fake_open(path, *a, **kw):
        if str(path).endswith("real_holdings.json"):
            return real_open(rh_path, *a, **kw)
        return real_open(path, *a, **kw)

    with patch("builtins.open", side_effect=_fake_open):
        result = app_module.portfolio_real()

    assert result["schwab"]["snapshot_ts"] == old_snapshot
    assert result["schwab"]["snapshot_age_days"] == 34
    assert result["schwab"]["snapshot_stale"] is True
    assert any("34 days old" in n for n in result["notes"])


def test_fresh_snapshot_not_flagged_stale(tmp_path):
    """A snapshot from a few hours ago must NOT trip the staleness warning."""
    import dashboard.app as app_module

    fresh_snapshot = datetime.now().strftime("%Y-%m-%d %H:%M:%S EDT")
    rh_path = _write_holdings(tmp_path, {
        "cash_balance": 28053.90,
        "positions": [],
        "last_updated": fresh_snapshot,
        "snapshot_ts": fresh_snapshot,
        "notes": "Live Schwab API sync.",
    })

    real_open = open
    def _fake_open(path, *a, **kw):
        if str(path).endswith("real_holdings.json"):
            return real_open(rh_path, *a, **kw)
        return real_open(path, *a, **kw)

    with patch("builtins.open", side_effect=_fake_open):
        result = app_module.portfolio_real()

    assert result["schwab"]["snapshot_age_days"] == 0
    assert result["schwab"]["snapshot_stale"] is False


def test_legacy_file_without_snapshot_ts_falls_back_to_notes_parse(tmp_path):
    """A real_holdings.json written before snapshot_ts existed (only the
    free-text `notes` field carries the snapshot time) must still get a
    correct staleness read -- the fallback the fix needs to work
    immediately, without waiting for the next sync cycle."""
    import dashboard.app as app_module

    old_snapshot_display = "05:38 PM ET, 2026/06/05"
    rh_path = _write_holdings(tmp_path, {
        "cash_balance": 28053.90,
        "positions": [],
        "last_updated": "2026-07-09",
        # no snapshot_ts key at all -- simulates the pre-fix on-disk file
        "notes": f"Auto-synced from schwab_holdings snapshot 42. 0 equity positions. Snapshot time: {old_snapshot_display}.",
    })

    real_open = open
    def _fake_open(path, *a, **kw):
        if str(path).endswith("real_holdings.json"):
            return real_open(rh_path, *a, **kw)
        return real_open(path, *a, **kw)

    with patch("builtins.open", side_effect=_fake_open):
        result = app_module.portfolio_real()

    assert result["schwab"]["snapshot_ts"] == old_snapshot_display
    assert result["schwab"]["snapshot_age_days"] is not None
    assert result["schwab"]["snapshot_age_days"] > 7
    assert result["schwab"]["snapshot_stale"] is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
