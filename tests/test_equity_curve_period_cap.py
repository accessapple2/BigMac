"""HM-EQUITY-CURVE-PERIOD-CAP 2026-07-10.

dashboard/app.py::_get_alpaca_equity_and_spy_raw() called Alpaca's
portfolio/history endpoint with start=<season_start> and no `period` param.
Verified directly against the live Alpaca API: omitting `period` makes the
endpoint silently default to a ~1-month window measured FROM `start`, not
"start to now" -- the account equity curve had been stuck at 2026-05-23
(one month after the 2026-04-24 season start) for 7+ weeks and would never
self-heal. A fixed period (e.g. "3M") anchors to `start` the same way and
would resurface the identical bug once that period elapsed (confirmed via
a live test). period="all" (no start) is the only combination that always
reaches "now" -- pre-season rows it returns are dropped for free by the
existing SPY-bars join (spy_map only contains dates >= season start).

This test verifies the request itself, not live Alpaca behavior (already
verified manually against the real API) -- the fix is a parameter change,
so the assertion that matters is "period=all, no start" on the outbound
request.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fake_response(json_data):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def test_portfolio_history_request_uses_period_all_not_start():
    import dashboard.app as app_module

    # Reset the module-level cache so this test isn't served a stale/mocked
    # value left over from another test or the live process's own state.
    app_module._ALPACA_EQUITY_SPY_CACHE.update({"data": None, "ts": 0.0, "error": None})

    calls = []

    def fake_fetch(url, headers, params, timeout=10):
        calls.append((url, params))
        if "portfolio/history" in url:
            return _fake_response({
                "timestamp": [1751500800, 1751587200],  # 2026-07-03, 2026-07-04
                "equity": [12345.0, 12400.0],
            })
        return _fake_response({"bars": [
            {"t": "2026-07-03T00:00:00Z", "c": 700.0},
            {"t": "2026-07-04T00:00:00Z", "c": 701.0},
        ]})

    with patch.object(app_module, "_fetch_with_backoff", side_effect=fake_fetch):
        data = app_module._get_alpaca_equity_and_spy_raw()

    portfolio_calls = [c for c in calls if "portfolio/history" in c[0]]
    assert len(portfolio_calls) == 1
    _, params = portfolio_calls[0]

    assert params.get("period") == "all"
    assert "start" not in params

    assert data["timestamps"] == [1751500800, 1751587200]
    assert data["equity"] == [12345.0, 12400.0]
