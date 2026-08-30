#!/usr/bin/env python3
"""HM-FD-LOCK-SENTINEL (2026-07-06, post-incident prevention).

Independent watchdog for the failure mode behind today's morning lock storm
(HM-SQLITE-CONN-FD-LEAK / HM-RIKER-SYNTHESIS-LOCK-CONTENTION in
docs/XO_BACKLOG.md): leaked sqlite connections on main.py -> FD growth ->
lock contention -> "database is locked" errors feeding back into more leaks.

Runs as its own cron entry (5-10 min cadence), independent of main.py's own
scheduler, so a main.py hang or restart doesn't also kill the thing watching
it (see CLAUDE.md "Alarms must not share a failure mode with what they watch").

Four checks, each alerts independently (own alert_type, own rate limit):
  1. FD count on data/trader.db (+ -wal/-shm) for the main.py PID.
       WARNING at >150, RED_ALERT at >250 (Admiral-revised 2026-07-06 --
       the original 60/120 false-fired against an observed healthy
       140-170 plateau on day one of live sentinel data; revisit after a
       week of real readings). Trend context (delta over time) is included
       in the alert text either way so a firing is self-diagnosing.
  2. rikers_log heartbeat age (engine/riker_synthesis.py, cron */10).
       RED_ALERT if >25 min stale AND currently market hours.
  3. "database is locked" occurrences in trader_error.log in the last
       LOCK_WINDOW_MIN minutes. Any occurrence -> WARNING (this is the
       symptom the FD leak produces; catch it directly too).
  4. signals_v2 pending-queue depth + oldest-pending age
       (HM-SIGNALS-V2-FIFO-STARVATION). WARNING if pending>3000 or
       oldest-pending age >48h.
  5. Daily-collector freshness (GEX + fred_carts), market-hours-aware.
       RED_ALERT/WARNING if stale past COLLECTOR_STALE_MARKET_HOURS.
       See check_collector_freshness().
  6. status.ollietrades.com's internal heartbeat file (ANY hours, not
       market-hours-gated). RED_ALERT if >15 min stale. See
       check_status_page_heartbeat() -- added after HM-STATUSPAGE-FREEZE-
       2026-08-29 (the page's "Last checked" froze ~11.5h with no internal
       signal since it had no independent heartbeat, only computed
       on-request).

HM-SENTINEL-ACK (2026-07-12): each alert can be acknowledged (suppressed)
via scripts/hm_sentinel_ack.py, which writes data/.hm_ops_sentinel_acks.json.
An ack suppresses notification for its alert_type UNLESS the check's own
metric value exceeds the ack's optional ceiling -- so acking a known,
understood condition doesn't create a permanent blind spot if it later
gets meaningfully worse. This script only ever READS the acks file; only
the ack CLI writes it, so this stays read-only against everything except
its own state/checkpoint file.

The signals_v2 queue check's ceiling metric is elapsed *market* hours
(engine.market_calendar.market_hours_elapsed), not wall-clock hours -- a
Friday-evening backlog sitting untouched all weekend shouldn't burn an
ack's escalation budget just because 48+ wall-clock hours passed while the
market was closed the entire time. All other checks use their existing
wall-clock/count metrics as-is (market-hours-awareness isn't meaningful
for FD count or heartbeat staleness).

Exit codes:
  0 - all checks healthy (or all firing alerts were acked and under ceiling)
  2 - at least one non-acked (or ceiling-breached) alert fired
  1 - error running the checks themselves
"""
from __future__ import annotations

import json
import logging
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# HM-NTFY-OBSERVABILITY (2026-07-07): this standalone cron script never
# configured logging, so engine.alert_channels's logger.info("ntfy sent...")
# calls were silently dropped by Python's default level filtering (INFO is
# below the WARNING default) -- only logger.warning("ntfy failed...") ever
# reached this script's own log. That made every historical entry in
# logs/hm_ops_sentinel_cron.log look like a 100% failure rate even though
# successes could have been happening invisibly the whole time -- a real
# diagnostic dead-end hit while investigating HM-NTFY-IPV6-NOROUTE. Scoped
# to this script's own process only (not engine/alert_channels.py itself,
# which main.py also imports and which must not have its logging behavior
# changed for the live trading process).
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "trader.db"
ERROR_LOG = ROOT / "logs" / "trader_error.log"
STATE_PATH = ROOT / "data" / ".hm_ops_sentinel_state.json"
ACKS_PATH = ROOT / "data" / ".hm_ops_sentinel_acks.json"

FD_WARN_THRESHOLD = 150   # Admiral-revised 2026-07-06: observed healthy plateau
FD_RED_THRESHOLD = 250    # 140-170 during a normal session; 60/120 false-fired daily.
# Revisit after a week of sentinel data once a real baseline is established.
HEARTBEAT_STALE_MIN = 25
LOCK_WINDOW_MIN = 10
QUEUE_PENDING_WARN = 3000
QUEUE_OLDEST_WARN_HOURS = 48

# An alert tuple is (level_kw, alert_type, message, metric_value). metric_value
# is the single number an ack's ceiling compares against -- None means "no
# comparable scalar for this alert type," so an ack on it is a permanent
# suppression (no ceiling is possible) regardless of what --ceiling was given.
AlertTuple = tuple[str, str, str, "float | None"]


def _load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        STATE_PATH.write_text(json.dumps(state))
    except Exception as e:
        print(f"[sentinel] failed to persist state: {e}", file=sys.stderr)


def _load_acks() -> dict:
    """Read-only. scripts/hm_sentinel_ack.py owns writes to this file."""
    try:
        return json.loads(ACKS_PATH.read_text())
    except Exception:
        return {}


def _is_suppressed(alert_type: str, metric_value: "float | None", acks: dict) -> bool:
    """True if an ack on `alert_type` should suppress this firing.

    No ack -> never suppressed. Ack with no ceiling (or metric_value is None,
    i.e. this alert type has no single comparable number) -> suppressed
    unconditionally until unacked. Ack with a ceiling -> suppressed only
    while metric_value stays at or below that ceiling; a breach re-fires
    so an acked condition can't silently get worse forever.
    """
    ack = acks.get(alert_type)
    if not ack:
        return False
    ceiling = ack.get("ceiling")
    if ceiling is None or metric_value is None:
        return True
    return metric_value <= ceiling


def _main_pid() -> int | None:
    r = subprocess.run(
        ["pgrep", "-f", "[m]ain.py"], capture_output=True, text=True
    )
    pids = [p for p in r.stdout.split() if p.isdigit()]
    return int(pids[0]) if pids else None


def check_fd_count(alerts: list[AlertTuple]) -> dict:
    pid = _main_pid()
    if pid is None:
        alerts.append((
            "red_alert", "sentinel_main_py_down",
            "HM-OPS-SENTINEL: main.py is not running (pgrep found no match). "
            "trader_keepalive_cron should catch this within 5 min; flagging directly too.",
            None,
        ))
        return {"pid": None, "fd_count": None}

    # Absolute path: cron's minimal PATH (/usr/bin:/bin) doesn't include
    # /usr/sbin, where lsof actually lives on macOS -- a bare "lsof" here
    # raised FileNotFoundError on every single cron tick (confirmed via
    # logs/hm_ops_sentinel_cron.log: 100% failure since this went live),
    # silently disabling all 4 checks (main()'s single try/except around
    # all four sequential checks means check #1 crashing kills #2-4 too).
    r = subprocess.run(["/usr/sbin/lsof", "-p", str(pid)], capture_output=True, text=True)
    fd_count = sum(1 for line in r.stdout.splitlines() if "trader.db" in line)

    # Growth-rate context, keyed to this PID so a restart resets the baseline
    # instead of comparing across processes. A genuine leak climbs
    # monotonically; healthy load plateaus -- this makes a RED firing
    # self-diagnosing (still climbing = real; flat = re-tune the threshold)
    # without silently overriding the Admiral-specified static thresholds.
    state = _load_state()
    prev = state.get("fd_samples", {}).get(str(pid))
    now_iso = datetime.now(timezone.utc).isoformat()
    trend = ""
    if prev:
        delta = fd_count - prev["fd_count"]
        mins = max(1.0, (datetime.now(timezone.utc) - datetime.fromisoformat(prev["ts"])).total_seconds() / 60.0)
        trend = f" ({delta:+d} over {mins:.0f} min, {delta / mins:+.1f}/min)"
    state.setdefault("fd_samples", {})
    state["fd_samples"] = {str(pid): {"fd_count": fd_count, "ts": now_iso}}  # single-entry, this PID only
    _save_state(state)

    if fd_count > FD_RED_THRESHOLD:
        alerts.append((
            "red_alert", "sentinel_fd_red",
            f"HM-OPS-SENTINEL: main.py (PID {pid}) holds {fd_count} FDs on "
            f"trader.db/-wal/-shm (> {FD_RED_THRESHOLD} red threshold){trend}. "
            f"Same precursor pattern as this morning's HM-SQLITE-CONN-FD-LEAK "
            f"lock storm -- check for a new leak site before this compounds. "
            f"NOTE: if the trend is flat/negative, this is likely a healthy "
            f"market-hours plateau, not an active leak -- see HM-OPS-SENTINEL "
            f"filing in XO_BACKLOG for today's observed baseline.",
            float(fd_count),
        ))
    elif fd_count > FD_WARN_THRESHOLD:
        alerts.append((
            "warning", "sentinel_fd_warn",
            f"HM-OPS-SENTINEL: main.py (PID {pid}) holds {fd_count} FDs on "
            f"trader.db/-wal/-shm (> {FD_WARN_THRESHOLD} warn threshold){trend}.",
            float(fd_count),
        ))
    return {"pid": pid, "fd_count": fd_count}


# RETIRED 2026-08-29 (HM-GEX-COLLECTOR-DEAD remediation pass): riker_synthesis
# was permanently retired at the code level 2026-06-24 (main.py's scheduler for
# it removed, not just paused -- see CLAUDE.md "Riker XO synthesis job"). Its
# rikers_log heartbeat has been dead since (confirmed MAX(created_at) =
# 2026-07-22 20:21:23, i.e. the stand-down's own kill timestamp, not a later
# failure) and will never write again. Re-enabling this check as-is would fire
# a false RED_ALERT on every single cron tick forever. Kept in place per this
# repo's Archive Convention (retired code stays, doesn't get deleted) but no
# longer called from main() below.
def check_riker_heartbeat(alerts: list[AlertTuple]) -> dict:
    import sqlite3

    from engine.risk_manager import RiskManager

    conn = sqlite3.connect(DB_PATH, timeout=10)
    row = conn.execute("SELECT MAX(created_at) FROM rikers_log").fetchone()
    conn.close()
    last = row[0] if row else None
    if last is None:
        return {"last": None, "age_min": None}

    last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    age_min = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0

    market_now = RiskManager.is_market_hours() == "market"
    if age_min > HEARTBEAT_STALE_MIN and market_now:
        alerts.append((
            "red_alert", "sentinel_riker_heartbeat",
            f"HM-OPS-SENTINEL: rikers_log heartbeat is {age_min:.0f} min stale "
            f"(last write {last} UTC) during market hours -- riker_synthesis "
            f"cron (*/10) may be failing its final persist (database is "
            f"locked was the root cause this morning).",
            age_min,
        ))
    return {"last": last, "age_min": round(age_min, 1)}


# HM-GEX-COLLECTOR-DEAD (2026-08-29): the GEX daily collector cron script was
# renamed away by the 2026-07-22 fleet stand-down (workaround for the
# crontab-write EINTR bug, see CLAUDE.md's LaunchAgent Reboot Lifecycle /
# HM-CRONTAB-EINTR) and never restored when the fleet resumed trading -- the
# staged, corrected crontab at backups/crontab_20260722_quietdown_STAGED_
# NOT_YET_INSTALLED.txt was never installed either. 39 days of "file not
# found" in logs/gex_collector.log, completely silent because THIS sentinel
# was swept into the same stand-down and disabled too (same-failure-mode
# blind spot CLAUDE.md already warns about under "Alarms must not share a
# failure mode with what they watch"). This check is the fix: independent of
# the collector cron, catches any daily-cadence collector going stale during
# market hours, not just GEX.
FLOW_GEX_DB_PATH = ROOT / "data" / "flow_gex.db"
COLLECTOR_STALE_MARKET_HOURS = 7.0   # > one full RTH session (~6.5h) untouched


def check_collector_freshness(alerts: list[AlertTuple]) -> dict:
    """Freshness check for daily-cadence collectors that write a durable
    last-updated timestamp: GEX (data/flow_gex.db gex_snapshots.asof, per
    underlying) and macro/FRED CARTS (data/trader.db fred_carts.fetched_at).
    Market-hours-elapsed, not wall-clock (see [[feedback_freshness_count_sessions]]
    doctrine) -- a weekend/overnight gap must not false-fire.

    fear-greed (engine/fear_greed.py) and congress (engine/congress_scraper.py)
    are deliberately NOT checked here: both are request-time-computed with no
    "last successful collector run" row to go stale (fear-greed recomputes
    live every call; congress has its own 30-min TTL cache + a dedicated
    zero-result watchdog, _record_scrape_health, that already NTFYs after 3
    consecutive empty scrapes). Neither shares the file-deletion /
    entitlement-loss failure mode this check targets. Checked 2026-08-29.
    """
    from engine.market_calendar import is_us_market_open, market_hours_elapsed

    if not is_us_market_open():
        return {"checked": False, "reason": "market closed"}

    results: dict = {}

    try:
        import sqlite3
        conn = sqlite3.connect(FLOW_GEX_DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT underlying, MAX(asof) FROM gex_snapshots GROUP BY underlying"
        ).fetchall()
        conn.close()
        for underlying, asof in rows:
            if not asof:
                continue
            asof_dt = datetime.strptime(asof, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_mh = market_hours_elapsed(asof_dt)
            results[f"gex_{underlying}"] = round(age_mh, 1)
            if age_mh > COLLECTOR_STALE_MARKET_HOURS:
                alerts.append((
                    "red_alert", f"sentinel_collector_stale_gex_{underlying.lower()}",
                    f"HM-OPS-SENTINEL: {underlying} GEX snapshot is {age_mh:.1f} "
                    f"market-hours stale (last asof {asof} UTC, > "
                    f"{COLLECTOR_STALE_MARKET_HOURS}h threshold). "
                    f"scripts/hm_gex_daily_collect.py (cron 13:05 AZ weekdays) "
                    f"may be failing -- check logs/gex_collector.log.",
                    age_mh,
                ))
    except Exception as e:
        print(f"[sentinel] gex freshness check error: {type(e).__name__}: {e}", file=sys.stderr)

    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH, timeout=10)
        row = conn.execute("SELECT MAX(fetched_at) FROM fred_carts").fetchone()
        conn.close()
        fetched_at = row[0] if row else None
        if fetched_at:
            fetched_dt = datetime.strptime(fetched_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            age_mh = market_hours_elapsed(fetched_dt)
            results["fred_carts"] = round(age_mh, 1)
            if age_mh > COLLECTOR_STALE_MARKET_HOURS:
                alerts.append((
                    "warning", "sentinel_collector_stale_fred_carts",
                    f"HM-OPS-SENTINEL: fred_carts macro data is {age_mh:.1f} "
                    f"market-hours stale (last fetched_at {fetched_at} UTC). "
                    f"main.py's run_carts_persist (06:00 AZ daily) may be "
                    f"failing -- check trader_error.log for "
                    f"engine.fred_data.persist_carts_all exceptions.",
                    age_mh,
                ))
    except Exception as e:
        print(f"[sentinel] fred_carts freshness check error: {type(e).__name__}: {e}", file=sys.stderr)

    return results


STATUS_PAGE_HEARTBEAT_PATH = ROOT / "data" / ".status_page_heartbeat.json"
STATUS_PAGE_STALE_MIN = 15.0


def check_status_page_heartbeat(alerts: list[AlertTuple]) -> dict:
    """HM-STATUSPAGE-FREEZE-2026-08-29: status.ollietrades.com froze
    "Last checked" for ~11.5h (Fri 21:54 -> Sat 09:25) with zero indication
    anything was wrong internally -- root cause was that scripts/status_page.py
    had NO independent heartbeat at all, only computed a fresh timestamp
    on-request (do_GET). An external watchdog caught it after ~11.5h; nothing
    internal did. scripts/status_page.py now runs its own checks on a fixed
    5-min cadence in a background thread, independent of HTTP traffic, and
    persists the result to STATUS_PAGE_HEARTBEAT_PATH.

    Deliberately checks the PERSISTED FILE's age, not the live web page's
    displayed "Last checked" -- probing the page itself would trivially look
    fresh the instant anything (including this sentinel) requests it, which
    is the exact self-defeating check that let the original freeze go
    undetected in the first place.

    Unlike the GEX/fred_carts check above, this runs ANY hours -- the status
    page's whole purpose is being checkable 24/7, unlike GEX which is
    inherently tied to market data availability.
    """
    try:
        raw = json.loads(STATUS_PAGE_HEARTBEAT_PATH.read_text())
        checked_at = raw.get("checked_at")
        if not checked_at:
            return {"status_page_heartbeat_age_min": None}
        checked_dt = datetime.strptime(checked_at, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - checked_dt).total_seconds() / 60.0
        if age_min > STATUS_PAGE_STALE_MIN:
            alerts.append((
                "red_alert", "sentinel_status_page_heartbeat_stale",
                f"HM-OPS-SENTINEL: status.ollietrades.com's internal heartbeat "
                f"is {age_min:.1f} min stale (last write {checked_at}, > "
                f"{STATUS_PAGE_STALE_MIN:.0f}min threshold). "
                f"scripts/status_page.py's background heartbeat thread may have "
                f"died, or the process itself may be down -- check "
                f"`launchctl print system/com.trademinds.statuspage` and "
                f"logs/status_page_error.log.",
                age_min,
            ))
        return {"status_page_heartbeat_age_min": round(age_min, 1)}
    except FileNotFoundError:
        # First run before the heartbeat thread has written once yet, or the
        # thread genuinely never started -- either way, worth a WARNING, not
        # silent (a RED_ALERT here would false-fire for the first 5 min after
        # every restart, which is routine, not an incident).
        alerts.append((
            "warning", "sentinel_status_page_heartbeat_missing",
            "HM-OPS-SENTINEL: status.ollietrades.com heartbeat file "
            f"({STATUS_PAGE_HEARTBEAT_PATH}) doesn't exist yet -- normal for "
            "the first 5 min after a status_page restart, otherwise the "
            "background heartbeat thread never started.",
            None,
        ))
        return {"status_page_heartbeat_age_min": None}
    except Exception as e:
        print(f"[sentinel] status_page heartbeat check error: {type(e).__name__}: {e}", file=sys.stderr)
        return {"status_page_heartbeat_age_min": None}


SOURCE_HEALTH_HEARTBEAT_PATH = ROOT / "data" / "source_health_watcher_heartbeat.json"
SOURCE_HEALTH_STALE_MIN = 35.0  # matches main.py's own _SOURCE_HEALTH_HB_STALE_S

MLX_QWEN3_HEARTBEAT_PATH = ROOT / "data" / "mlx_qwen3_heartbeat.json"
MLX_QWEN3_STALE_MIN = 20.0  # probe runs every 5 min (StartInterval) -- 4 missed ticks


def check_source_health_watcher_heartbeat(alerts: list[AlertTuple]) -> dict:
    """OPS TRIAGE item 2 (2026-08-29): scripts/source_health_watcher.py died
    2026-07-22 (same stand-down, same root cause as HM-GEX-COLLECTOR-DEAD --
    renamed to *.quietdown-disabled, crontab literal text never updated) and
    stayed dead for 54,682 minutes (~38 days) before this fix.

    main.py already has an in-process dead-man's-switch for this exact
    heartbeat (_bg_source_health_dms, main.py ~line 2514, scheduled every 30
    min) -- and it had been firing correctly the ENTIRE time
    ("Alert dispatched [warning/source-health-watcher-stale]" every ~30 min
    in trader_error.log, going back to shortly after the 07-22 death). The
    alert was never the gap. The gap: it fires at AlertLevel.WARNING, which
    DECOM-SILENCE (2026-07-19, blanket ntfy silence ahead of Gate 2 removal)
    has muted since before this even died -- the alert existed and worked
    perfectly, computed and "dispatched" every 30 minutes for 38 days, and
    zero of those dispatches ever reached a phone.

    This check exists specifically to reach a phone despite that: fires
    RED_ALERT (not WARNING), which routes through the PUSHOVER-RED-ALERT
    lane (2026-08-28) that DECOM-SILENCE does not gate. Same heartbeat file
    as main.py's in-process check, same staleness threshold (35 min, ~3.5
    missed 10-min cron runs) -- genuinely redundant coverage on a different
    mechanism (main.py in-process vs. this independent cron), which matters
    because the in-process check shares fate with main.py itself: if main.py
    is the thing that's unhealthy, its own dead-man's-switch dies with it.
    """
    try:
        raw = json.loads(SOURCE_HEALTH_HEARTBEAT_PATH.read_text())
        last_run = raw.get("last_run")
        if last_run is None:
            return {"source_health_heartbeat_age_min": None}
        age_min = (datetime.now(timezone.utc).timestamp() - float(last_run)) / 60.0
        if age_min > SOURCE_HEALTH_STALE_MIN:
            alerts.append((
                "red_alert", "sentinel_source_health_watcher_heartbeat_stale",
                f"HM-OPS-SENTINEL: source_health_watcher's heartbeat is "
                f"{age_min:.1f} min stale (last run {raw.get('last_run_iso', '?')}, "
                f"> {SOURCE_HEALTH_STALE_MIN:.0f}min threshold). This watcher is "
                f"itself the backstop for source-staleness alerting (incl. the "
                f"Movers-gap class of incident) -- check "
                f"`crontab -l | grep source_health_watcher` and "
                f"logs/source_health_watcher_cron.log.",
                age_min,
            ))
        return {"source_health_heartbeat_age_min": round(age_min, 1)}
    except FileNotFoundError:
        alerts.append((
            "warning", "sentinel_source_health_watcher_heartbeat_missing",
            "HM-OPS-SENTINEL: source_health_watcher heartbeat file "
            f"({SOURCE_HEALTH_HEARTBEAT_PATH}) doesn't exist yet -- normal for "
            "the first 10 min after a fresh install, otherwise the cron job "
            "never ran even once.",
            None,
        ))
        return {"source_health_heartbeat_age_min": None}
    except Exception as e:
        print(f"[sentinel] source_health_watcher heartbeat check error: {type(e).__name__}: {e}", file=sys.stderr)
        return {"source_health_heartbeat_age_min": None}


def check_mlx_qwen3_heartbeat(alerts: list[AlertTuple]) -> dict:
    """HM-MLX-QWEN3-REVIVAL-2026-08-29: mlx-qwen3's local MLX server (port
    8899) died 2026-07-18 with ZERO supervision -- no launchd, no cron,
    nothing watching it -- and stayed dead six weeks until this fix.
    Revived under com.ollietrades.mlx-qwen3.plist (KeepAlive=true); this
    check watches scripts/mlx_qwen3_probe.py's heartbeat (written every 5
    min via com.ollietrades.mlx-qwen3-probe.plist), on a different
    mechanism than the server itself per the "alarm must not share a
    failure mode with what it watches" doctrine.

    Two distinct findings, different severity:
      - Heartbeat file stale/missing -> RED_ALERT. The PROBE itself has
        stopped running (bypasses DECOM-SILENCE, same reasoning as
        check_source_health_watcher_heartbeat).
      - Heartbeat fresh but last probe reported unhealthy -> WARNING. The
        probe is fine; the actual mlx_lm.server is unreachable. KeepAlive
        should self-heal a crash within seconds, so a WARNING (not
        RED_ALERT) is proportionate unless it persists.
    """
    try:
        raw = json.loads(MLX_QWEN3_HEARTBEAT_PATH.read_text())
        last_run = raw.get("last_run")
        if last_run is None:
            return {"mlx_qwen3_heartbeat_age_min": None}
        age_min = (datetime.now(timezone.utc).timestamp() - float(last_run)) / 60.0
        if age_min > MLX_QWEN3_STALE_MIN:
            alerts.append((
                "red_alert", "sentinel_mlx_qwen3_heartbeat_stale",
                f"HM-OPS-SENTINEL: mlx-qwen3's probe heartbeat is {age_min:.1f} min "
                f"stale (last run {raw.get('last_run_iso', '?')}, > "
                f"{MLX_QWEN3_STALE_MIN:.0f}min threshold) -- the probe itself has "
                f"stopped running. Check `launchctl print gui/501/"
                f"com.ollietrades.mlx-qwen3-probe` and logs/mlx_qwen3_probe.err.log.",
                age_min,
            ))
        elif raw.get("healthy") is False:
            alerts.append((
                "warning", "sentinel_mlx_qwen3_unhealthy",
                f"HM-OPS-SENTINEL: mlx-qwen3 server unreachable as of the last probe "
                f"({raw.get('last_run_iso', '?')}): {raw.get('detail')}. "
                f"KeepAlive should self-heal a crash quickly -- check "
                f"`launchctl print gui/501/com.ollietrades.mlx-qwen3` if this persists.",
                0.0,
            ))
        return {"mlx_qwen3_heartbeat_age_min": round(age_min, 1), "healthy": raw.get("healthy")}
    except FileNotFoundError:
        alerts.append((
            "warning", "sentinel_mlx_qwen3_heartbeat_missing",
            f"HM-OPS-SENTINEL: mlx-qwen3 probe heartbeat file "
            f"({MLX_QWEN3_HEARTBEAT_PATH}) doesn't exist yet -- normal for the "
            "first 5 min after a fresh install, otherwise the probe never ran.",
            None,
        ))
        return {"mlx_qwen3_heartbeat_age_min": None}
    except Exception as e:
        print(f"[sentinel] mlx_qwen3 heartbeat check error: {type(e).__name__}: {e}", file=sys.stderr)
        return {"mlx_qwen3_heartbeat_age_min": None}


_CRON_MISSING_FILE_SIGNATURES = (
    "No such file or directory",
    "can't open file",
    "ModuleNotFoundError",
)


def check_cron_missing_scripts(alerts: list[AlertTuple]) -> dict:
    """OPS TRIAGE item 2 (2026-08-29), "no more unwatched watchers anywhere."

    A generic, self-maintaining check rather than N hand-built heartbeats:
    reads `crontab -l` directly, and for every active (non-comment) entry
    that redirects to a logs/*.log file, tails that log for the exact
    failure signature this whole investigation kept finding -- a cron job
    whose target script was renamed/deleted (the 2026-07-22 stand-down's
    *.quietdown-disabled rename, crontab literal text never updated).

    Deliberately NOT an mtime/staleness check: a dead-script cron entry's
    log keeps getting touched every single tick (cron appends the shell's
    "No such file or directory" error each time) -- the log looks
    perfectly *fresh* by mtime alone while being 100% useless. That's
    exactly how GEX and source_health_watcher's deaths went unnoticed for
    39-54 days even though something was "writing to the log" the whole
    time. Content, not recency, is the actual signal here.

    Covers every current AND future entry automatically (re-reads crontab
    live each run) -- no per-script registration needed, unlike the
    heartbeat-file checks above.
    """
    import re
    import subprocess

    results: dict = {"scanned": 0, "broken": []}
    try:
        cron_out = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, timeout=10
        ).stdout
    except Exception as e:
        print(f"[sentinel] cron missing-script check error (crontab -l): {type(e).__name__}: {e}", file=sys.stderr)
        return results

    log_re = re.compile(r">>\s*(\S+\.log)\b")
    for line in cron_out.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("@reboot"):
            continue
        m = log_re.search(line)
        if not m:
            continue  # a handful of entries log internally (exec >>) rather than via the crontab redirect -- not covered here
        log_path = Path(m.group(1))
        if not log_path.is_absolute():
            log_path = ROOT / log_path
        results["scanned"] += 1
        try:
            with open(log_path, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - 4000))
                tail = f.read().decode("utf-8", errors="replace")
        except FileNotFoundError:
            continue  # log never written yet -- not this check's concern
        except Exception:
            continue
        # Only the true LAST non-empty line(s) -- a 4000-byte window can span
        # a genuine fix (e.g. the script was just restored) mixed with older
        # error lines from before the fix, which would false-positive on a
        # simple "does this chunk contain the signature anywhere" check. The
        # most recent tick's own outcome is the only thing that matters.
        last_lines = [ln for ln in tail.splitlines() if ln.strip()][-1:]
        if any(sig in ln for ln in last_lines for sig in _CRON_MISSING_FILE_SIGNATURES):
            results["broken"].append(str(log_path.name))

    if results["broken"]:
        alerts.append((
            "warning", "sentinel_cron_missing_script",
            f"HM-OPS-SENTINEL: {len(results['broken'])} active cron entr"
            f"{'y is' if len(results['broken']) == 1 else 'ies are'} silently "
            f"failing every tick -- most recent log line matches a "
            f"file-not-found signature ('No such file or directory'/'can't "
            f"open file'/'ModuleNotFoundError'). Could be the cron target "
            f"script itself missing/renamed, OR a script that runs fine but "
            f"can't find a data file it depends on -- check the log itself, "
            f"not just this list, before assuming which: "
            f"{', '.join(sorted(results['broken']))}.",
            float(len(results["broken"])),
        ))
    return results


# HM-LAUNCHD-REVIVAL-2026-08-29: the 18 com.ollietrades.* gui/501 LaunchAgents
# reactivated today after the 2026-07-22 stand-down (see
# docs/XO_BACKLOG.md HM-STANDDOWN-SUCCESSOR-2026-08-29 for the full
# disposition). Registry maps label -> (log path relative to ROOT,
# max staleness in hours before it's worth a look) -- staleness ceilings are
# generous multiples of each job's own calendar cadence (weekly jobs get
# ~9 days, weekday-daily jobs get ~48h, daily jobs get ~30h) so a normal
# weekend gap never false-fires.
LAUNCHD_JOB_REGISTRY: dict[str, tuple[str, float]] = {
    "universe-refresh":                  ("logs/universe-refresh.log", 216.0),
    "model-watcher":                     ("logs/model_watcher.log", 216.0),
    "iv-backfill":                       ("logs/iv-backfill.out.log", 48.0),
    "danelfin-update":                   ("logs/danelfin_update.log", 216.0),  # lives under ~/ollietrades/logs, checked via absolute fallback below
    "enrichment-poller":                 ("logs/enrichment_poller.log", 216.0),
    "ti-email-poller":                   ("logs/ti_email_poller_stdout.log", 48.0),
    "uhura-watch":                       ("logs/uhura_watch/launchd.out.log", 192.0),
    "scotty":                            ("logs/scotty.out.log", 48.0),
    "nightly-backtest":                  ("logs/nightly_backtest.log", 30.0),  # lives under ~/ollietrades/logs, checked via absolute fallback below
    "nightly-regression":                ("logs/nightly_regression_launchd.log", 30.0),
    "daily-watch":                       ("logs/daily_watch_stdout.log", 30.0),
    "morning-an2-observation":           ("logs/morning_an2_observation_stdout.log", 48.0),
    "stale-trim-obs":                    ("logs/stale_trim.out.log", 48.0),
    "finetune-reminder":                 ("logs/finetune_reminder.log", 30.0),
    "hm-signals-v2-monday-check":        ("logs/hm_signals_v2_monday_check_stdout.log", 192.0),
    "hm-signals-v2-monday-check-verify": ("logs/hm_signals_v2_monday_check_verify_stdout.log", 192.0),
    # HM-SENTINEL-REGISTRY-FIX-2026-08-30: was "logs/archer_briefing.log" (the
    # plist's StandardOutPath) -- that file is permanently 0 bytes because
    # engine/archer_morning_synthesis.py's real output goes through Python's
    # `logging` module, which defaults to stderr, not stdout. Confirmed live:
    # archer_briefing.log empty, archer_briefing_err.log has today's real
    # 06:25 briefing content. Repointed to the file that's actually written.
    # hm-wr-dur-monday-check REMOVED (not just repointed): confirmed via
    # plist read to be a one-shot StartCalendarInterval hardcoded to
    # 2026-07-20 (RunAtLoad=false) -- never fires again regardless of
    # enabled state. Retired via fleet_lifecycle.py 2026-08-30 (same
    # dead-one-shot pattern as hm-signals-v2-monday-check/-verify, missed in
    # that earlier pass). Ledger-skip already excludes it from `stale`
    # going forward; removed here too since the registry's own docstring
    # says it tracks the 08-29-reactivated set, which this no longer is.
    "archer-briefing":                   ("logs/archer_briefing_err.log", 30.0),
}
# Three of the registry's logs live under the ~/ollietrades/ tree (a
# sibling project dir), not this repo's own logs/ -- rather than a second
# hardcoded absolute-path map, resolve each entry against both roots and
# use whichever exists.
_ALT_LOG_ROOT = Path.home() / "ollietrades"
_LAUNCHAGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
_KNOWN_JOB_PREFIXES = ("com.ollietrades", "com.trademinds")


def _resolve_job_label(name: str) -> str | None:
    """Same resolution scripts/fleet_lifecycle.py uses -- most jobs are
    com.ollietrades.*, a few (premarket, signal-center) are com.trademinds.*."""
    for prefix in _KNOWN_JOB_PREFIXES:
        if (_LAUNCHAGENTS_DIR / f"{prefix}.{name}.plist").exists():
            return f"{prefix}.{name}"
    return None


def _ledger_latest_by_target(target_type: str) -> dict[str, dict]:
    """Latest fleet_lifecycle_ledger row per target_name for one target_type.
    Empty dict (not a raise) if the ledger table or DB is unreachable --
    every caller degrades to "no ledger opinion" rather than failing."""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT l.* FROM fleet_lifecycle_ledger l
            INNER JOIN (
                SELECT target_name, MAX(created_at) AS mx FROM fleet_lifecycle_ledger
                WHERE target_type = ? GROUP BY target_name
            ) latest ON l.target_name = latest.target_name AND l.created_at = latest.mx
            WHERE l.target_type = ?
        """, (target_type, target_type)).fetchall()
        conn.close()
        return {r["target_name"]: dict(r) for r in rows}
    except Exception as e:
        print(f"[sentinel] fleet_lifecycle_ledger read error ({target_type}): {type(e).__name__}: {e}", file=sys.stderr)
        return {}


_PAUSE_LEDGER_ACTIONS = {"halt", "bench", "shadow"}
_ACTIVE_LEDGER_ACTIONS = {"active", "revive"}


def check_launchd_jobs_health(alerts: list[AlertTuple]) -> dict:
    """OPS TRIAGE follow-up (2026-08-29): every job the fleet_lifecycle
    ledger currently says should be running gets freshness coverage, same
    "no unwatched watchers" philosophy as check_cron_missing_scripts --
    adapted for launchd's calendar scheduling instead of crontab's
    tail-the-log-for-an-error-signature approach.

    Skips any job the ledger's latest entry marks halt/bench/shadow/retire
    -- an intentionally-off job going log-stale is not a finding, it's the
    plan working. (Whether the *live* launchd state actually matches that
    ledger entry is a DIFFERENT check --
    see check_fleet_lifecycle_drift below.)

    Staleness ceilings (LAUNCHD_JOB_REGISTRY) are generous multiples of
    each job's own calendar cadence (weekly jobs ~9 days, weekday-daily
    ~48h, daily ~30h) so a normal weekend gap never false-fires.

    Never raises -- a missing log just lands as "not stale yet", a ledger
    read failure just means every registered job is treated as active
    (fail toward checking, not toward silence).
    """
    ledger = _ledger_latest_by_target("job")
    results: dict = {"checked": 0, "skipped_by_ledger": [], "stale": []}
    now = time.time()
    for label, (rel_log, max_age_hours) in LAUNCHD_JOB_REGISTRY.items():
        entry = ledger.get(label)
        if entry and entry["action"] not in _ACTIVE_LEDGER_ACTIONS:
            results["skipped_by_ledger"].append(label)
            continue
        results["checked"] += 1

        log_path = ROOT / rel_log
        if not log_path.exists():
            log_path = _ALT_LOG_ROOT / rel_log
        if not log_path.exists():
            continue  # never written yet post-reactivation -- not stale, just new
        age_hours = (now - log_path.stat().st_mtime) / 3600.0
        if age_hours > max_age_hours:
            results["stale"].append({"label": label, "age_hours": round(age_hours, 1), "ceiling_hours": max_age_hours})

    if results["stale"]:
        worst = max(results["stale"], key=lambda s: s["age_hours"] - s["ceiling_hours"])
        alerts.append((
            "warning", "sentinel_launchd_job_stale",
            f"HM-OPS-SENTINEL: {len(results['stale'])} launchd job log"
            f"{'s are' if len(results['stale']) != 1 else ' is'} stale "
            f"past its ceiling -- worst: {worst['label']} "
            f"({worst['age_hours']}h since last write, ceiling {worst['ceiling_hours']}h). "
            f"Full list: {', '.join(s['label'] for s in results['stale'])}.",
            float(len(results["stale"])),
        ))
    return results


def check_fleet_lifecycle_drift(alerts: list[AlertTuple]) -> dict:
    """HM-FLEET-LIFECYCLE-2026-08-29: "manual plist/cron edits to fleet jobs
    become a sentinel finding of their own: any job whose live state
    disagrees with the lifecycle ledger = drift alert." Two failure modes:

      1. Drift -- the ledger's latest recorded action for a target and its
         ACTUAL live state disagree (someone hand-edited a plist, ran raw
         launchctl/SQL instead of scripts/fleet_lifecycle.py, or a future
         stand-down forgets to record itself). Covers both jobs
         (launchctl print-disabled) and agents (ai_players.halt_mode).
      2. Overdue review -- a pause-type ledger entry (halt/bench/shadow)
         whose resume_by or review_by date has passed. Per
         scripts/fleet_lifecycle.py's own order-doc template: "a sentinel
         finding against this target before its review-by date is a false
         alarm; after it, it is a legitimate 'this pause was forgotten'
         alert." This is that alert.

    Never raises -- unreachable launchctl/DB just means this check reports
    nothing rather than failing the whole sentinel run.
    """
    results: dict = {"job_drift": [], "agent_drift": [], "overdue": []}

    job_ledger = _ledger_latest_by_target("job")
    try:
        disabled_out = subprocess.run(
            ["launchctl", "print-disabled", "gui/501"],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception as e:
        print(f"[sentinel] launchd disabled-check error: {type(e).__name__}: {e}", file=sys.stderr)
        disabled_out = ""
    for label, entry in job_ledger.items():
        # Resolve the real label prefix the same way fleet_lifecycle.py does --
        # most are com.ollietrades.*, a few (premarket, signal-center) are
        # com.trademinds.*. Try both rather than assuming.
        full_label = _resolve_job_label(label)
        if full_label is None:
            continue  # no plist to check against (e.g. the documented 'crew' orphan) -- can't verify, don't guess
        live_disabled = f'"{full_label}" => disabled' in disabled_out
        ledger_says_active = entry["action"] in _ACTIVE_LEDGER_ACTIONS
        if ledger_says_active == live_disabled:  # active-but-disabled, or halted-but-enabled
            results["job_drift"].append({"name": label, "ledger_action": entry["action"],
                                          "live_disabled": live_disabled})

    agent_ledger = _ledger_latest_by_target("agent")
    _expected_halt_mode = {"active": "active", "revive": "active", "retire": "full",
                            "bench": "full", "halt": "full", "shadow": "exit_only"}
    if agent_ledger:
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=10)
            conn.row_factory = sqlite3.Row
            live_rows = {r["id"]: r["halt_mode"] for r in conn.execute(
                "SELECT id, COALESCE(halt_mode,'active') AS halt_mode FROM ai_players").fetchall()}
            conn.close()
        except Exception as e:
            print(f"[sentinel] ai_players drift-check read error: {type(e).__name__}: {e}", file=sys.stderr)
            live_rows = {}
        for name, entry in agent_ledger.items():
            live_mode = live_rows.get(name)
            if live_mode is None:
                continue  # agent no longer exists -- not a drift finding
            expected = _expected_halt_mode.get(entry["action"])
            if expected and live_mode != expected:
                results["agent_drift"].append({"name": name, "ledger_action": entry["action"],
                                                "expected_halt_mode": expected, "live_halt_mode": live_mode})

    today = datetime.now(timezone.utc).date().isoformat()
    for target_type, ledger in (("job", job_ledger), ("agent", agent_ledger)):
        for name, entry in ledger.items():
            if entry["action"] not in _PAUSE_LEDGER_ACTIONS:
                continue
            for date_field in ("resume_by", "review_by"):
                d = entry.get(date_field)
                if d and d < today:
                    results["overdue"].append({"target_type": target_type, "name": name,
                                                "action": entry["action"], "date_field": date_field,
                                                "date": d})
                    break  # one overdue mention per target is enough

    if results["job_drift"] or results["agent_drift"]:
        n = len(results["job_drift"]) + len(results["agent_drift"])
        names = [d["name"] for d in results["job_drift"] + results["agent_drift"]]
        alerts.append((
            "warning", "sentinel_lifecycle_drift",
            f"HM-OPS-SENTINEL: {n} target{'s' if n != 1 else ''} where live state "
            f"disagrees with the fleet_lifecycle_ledger's latest recorded action -- "
            f"someone likely bypassed scripts/fleet_lifecycle.py: {', '.join(names)}.",
            float(n),
        ))
    if results["overdue"]:
        names = [f"{o['name']} ({o['action']}, {o['date_field']} {o['date']})" for o in results["overdue"]]
        alerts.append((
            "warning", "sentinel_lifecycle_review_overdue",
            f"HM-OPS-SENTINEL: {len(results['overdue'])} paused target"
            f"{'s' if len(results['overdue']) != 1 else ''} past their resume_by/review_by "
            f"date: {', '.join(names)}.",
            float(len(results["overdue"])),
        ))
    return results


def check_lock_errors(alerts: list[AlertTuple]) -> dict:
    """Count "database is locked" occurrences appended since the last run.

    trader_error.log lines are "HH:MM:SS [LRS] message" with NO date, and the
    file spans multiple days between weekly rotations -- a line like
    "21:31:58 ..." from last night reads as numerically LATER than "12:11"
    right now, which would falsely look like "10 minutes ago" under a
    wall-clock time-window comparison. Sidestep the ambiguity entirely: track
    a persisted byte offset and only scan bytes appended since the previous
    sentinel run (this cron's own cadence, 5-10 min, IS the window). Resets
    to 0 if the file is smaller than the last offset (rotated).
    """
    if not ERROR_LOG.exists():
        return {"lock_errors": 0}

    state = _load_state()
    last_offset = state.get("error_log_offset", None)
    size = ERROR_LOG.stat().st_size

    if last_offset is None or last_offset > size:
        # First run, or file rotated since last run -- nothing to compare
        # against yet; start the checkpoint at EOF rather than scanning the
        # whole multi-day file (which would double-count the 07:xx storm).
        new_text = ""
    else:
        with ERROR_LOG.open("rb") as f:
            f.seek(last_offset)
            new_text = f.read().decode("utf-8", errors="replace")

    state["error_log_offset"] = size
    _save_state(state)

    count = sum(1 for line in new_text.splitlines() if "database is locked" in line)

    if count > 0:
        alerts.append((
            "warning", "sentinel_lock_errors",
            f"HM-OPS-SENTINEL: {count} \"database is locked\" occurrence(s) "
            f"in trader_error.log in the last {LOCK_WINDOW_MIN} min.",
            float(count),
        ))
    return {"lock_errors": count}


def check_signals_v2_queue(alerts: list[AlertTuple]) -> dict:
    import sqlite3

    from engine.market_calendar import market_hours_elapsed

    conn = sqlite3.connect(DB_PATH, timeout=10)
    pending = conn.execute(
        "SELECT COUNT(*) FROM signals_v2 WHERE status='pending'"
    ).fetchone()[0]
    oldest = conn.execute(
        "SELECT MIN(created_at) FROM signals_v2 WHERE status='pending'"
    ).fetchone()[0]
    conn.close()

    oldest_age_hours = None
    oldest_age_market_hours = None
    if oldest:
        oldest_dt = datetime.strptime(oldest, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        oldest_age_hours = (datetime.now(timezone.utc) - oldest_dt).total_seconds() / 3600.0
        oldest_age_market_hours = market_hours_elapsed(oldest_dt)

    reasons = []
    if pending > QUEUE_PENDING_WARN:
        reasons.append(f"pending={pending} (> {QUEUE_PENDING_WARN})")
    if oldest_age_hours is not None and oldest_age_hours > QUEUE_OLDEST_WARN_HOURS:
        reasons.append(f"oldest-pending age={oldest_age_hours:.0f}h (> {QUEUE_OLDEST_WARN_HOURS}h, "
                        f"{oldest_age_market_hours:.1f} market-hours)")

    if reasons:
        alerts.append((
            "warning", "sentinel_signals_v2_queue",
            f"HM-OPS-SENTINEL: signals_v2 pending queue -- {', '.join(reasons)}. "
            f"See HM-SIGNALS-V2-FIFO-STARVATION in docs/XO_BACKLOG.md.",
            # Ceiling metric is elapsed MARKET hours, not wall-clock -- a
            # weekend/overnight gap shouldn't consume an ack's escalation
            # budget when nothing could have drained anyway. Falls back to
            # pending count if the queue is non-empty with no parseable
            # oldest timestamp (shouldn't happen in practice).
            oldest_age_market_hours if oldest_age_market_hours is not None else float(pending),
        ))
    return {
        "pending": pending,
        "oldest": oldest,
        "oldest_age_hours": round(oldest_age_hours, 1) if oldest_age_hours is not None else None,
        "oldest_age_market_hours": round(oldest_age_market_hours, 1) if oldest_age_market_hours is not None else None,
    }


def _dispatch(alerts: list[AlertTuple], dry_run: bool = False) -> None:
    if dry_run:
        for level_kw, alert_type, message, _metric in alerts:
            print(f"[sentinel] [DRY RUN] would dispatch [{level_kw}/{alert_type}]: {message}")
        return
    try:
        from engine.alert_channels import AlertLevel, send_alert
        level_map = {"info": AlertLevel.INFO, "warning": AlertLevel.WARNING, "red_alert": AlertLevel.RED_ALERT}
        for level_kw, alert_type, message, _metric in alerts:
            send_alert(
                message=message,
                level=level_map[level_kw],
                alert_type=alert_type,
                rate_limit_secs=1800,  # 30 min -- cron runs every 5-10 min, don't spam every tick
            )
            print(f"[sentinel] ALERT dispatched [{level_kw}/{alert_type}]: {message[:100]}")
    except Exception as e:
        print(f"[sentinel] alert dispatch failed: {type(e).__name__}: {e}", file=sys.stderr)


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    alerts: list[AlertTuple] = []
    try:
        fd_status = check_fd_count(alerts)
        lock_status = check_lock_errors(alerts)
        queue_status = check_signals_v2_queue(alerts)
        collector_status = check_collector_freshness(alerts)
        status_page_status = check_status_page_heartbeat(alerts)
        source_health_status = check_source_health_watcher_heartbeat(alerts)
        mlx_qwen3_status = check_mlx_qwen3_heartbeat(alerts)
        cron_status = check_cron_missing_scripts(alerts)
        launchd_status = check_launchd_jobs_health(alerts)
        lifecycle_drift_status = check_fleet_lifecycle_drift(alerts)
    except Exception as e:
        print(f"[sentinel] error running checks: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    print(f"[sentinel] fd={fd_status} lock={lock_status} queue={queue_status} "
          f"collectors={collector_status} status_page={status_page_status} "
          f"source_health={source_health_status} mlx_qwen3={mlx_qwen3_status} cron={cron_status} "
          f"launchd={launchd_status} lifecycle_drift={lifecycle_drift_status}")

    acks = _load_acks()
    fired = [a for a in alerts if not _is_suppressed(a[1], a[3], acks)]
    suppressed = [a for a in alerts if _is_suppressed(a[1], a[3], acks)]

    for level_kw, alert_type, message, metric_value in suppressed:
        ack = acks.get(alert_type, {})
        ceiling = ack.get("ceiling")
        print(f"[sentinel] SUPPRESSED (acked {ack.get('acked_at', '?')} by "
              f"{ack.get('acked_by', '?')}, ceiling={ceiling}, metric={metric_value}) "
              f"[{level_kw}/{alert_type}]: {message[:100]}")

    if fired:
        _dispatch(fired, dry_run=dry_run)
        return 2

    if suppressed:
        print(f"[sentinel] OK -- {len(suppressed)} alert(s) acked and within ceiling, nothing dispatched")
    else:
        print("[sentinel] OK -- all checks within threshold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
