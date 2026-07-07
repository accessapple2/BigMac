#!/usr/bin/env python3
"""HM-OPS-SENTINEL P2.4 daily GEX collector (2026-07-06, Admiral-approved).

The durable `data/flow_gex.db` gex_snapshots table -- the after-hours/cold-
cache fallback `dashboard/app.py::_canonical_gex_cached` reads when the
in-memory intraday cache is cold and the market's closed -- had gone stale
for a month (last row 2026-06-05) because nothing in the repo or crontab
ever called `engine.options_flow_gex.collect()`. This is that missing call,
run once daily shortly after close via crontab (NOT launchd -- see
CLAUDE.md's LaunchAgent Reboot Lifecycle section on why cron is the
durable choice on this box).

Skips cleanly (exit 0, one log line) on weekends/holidays -- a holiday's
chain data would be stale/thin and not worth collecting. The write-side
collapsed-wall guard lives in options_flow_gex.persist() itself (protects
every caller, not just this cron), so this script doesn't duplicate it.
"""
import sys
from pathlib import Path

ROOT = Path.home() / "autonomous-trader"
sys.path.insert(0, str(ROOT))

from engine.market_calendar import az_now, is_trading_day  # noqa: E402


def main() -> int:
    today = az_now().date()
    if not is_trading_day(today):
        print(f"[hm_gex_daily_collect] {today} not a trading day -- skipping")
        return 0

    from engine.options_flow_gex import collect

    result = collect(("SPY", "QQQ"))
    for underlying, pair in result.items():
        gex = pair.get("gex") or {}
        if gex.get("error"):
            print(f"[hm_gex_daily_collect] {underlying}: GEX error -- {gex['error']}")
        else:
            print(
                f"[hm_gex_daily_collect] {underlying}: spot={gex.get('spot')} "
                f"call_wall={gex.get('call_wall')} put_wall={gex.get('put_wall')} "
                f"gamma_flip={gex.get('gamma_flip')} (persist() applies its own "
                f"collapsed-wall skip if degenerate)"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
