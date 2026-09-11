"""
engine/alert_channels.py — Phase 3.7 Unified Alert Channels

Dispatches alerts to multiple channels based on severity:
  INFO    → ntfy.sh only
  WARNING → ntfy.sh + browser push (via DB notification)
  RED ALERT → all channels (ntfy, browser push, email)

Rate limit: 1 alert per 5 minutes per alert_type.

Usage:
    from engine.alert_channels import send_alert, AlertLevel
    send_alert("VaR threshold breached", AlertLevel.WARNING, "var_breach")
    send_alert("System offline", AlertLevel.RED_ALERT, "system_down")

CIC commands (handled by handle_cic_command):
    "alerts on" / "alerts off"
    "alert test"
    "set alert email address@example.com"
"""
from __future__ import annotations

import os
import json
import socket as _socket
import sqlite3
import threading
import time as _time
import logging
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

_DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

NTFY_TOPIC       = os.environ.get("NTFY_TOPIC", "ollietrades-admin")
NTFY_ADMIN_TOPIC = os.environ.get("NTFY_ADMIN_TOPIC", "") or NTFY_TOPIC
NTFY_CREW_TOPIC  = os.environ.get("NTFY_CREW_TOPIC", "") or NTFY_TOPIC
NTFY_BASE        = "https://ntfy.sh"

SMTP_HOST  = os.environ.get("SMTP_HOST", "")
SMTP_PORT  = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER  = os.environ.get("SMTP_USER", "")
SMTP_PASS  = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_APP_PASSWORD", "")
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")

RATE_LIMIT_SECS = 300   # 5 minutes per alert_type
ALERTS_ENABLED_KEY = "alert_channels_enabled"

# HM-UHURA-HAILS (2026-06-08) — standardized outbound topics + independent toggles
NTFY_PREMARKET_TOPIC = os.environ.get("NTFY_PREMARKET_TOPIC", "ollie-premarket")
NTFY_SIGNALS_TOPIC   = os.environ.get("NTFY_SIGNALS_TOPIC",   "ollie-signals")
NTFY_CRITICAL_TOPIC  = os.environ.get("NTFY_CRITICAL_TOPIC",  "ollie-critical")

ALERTS_NTFY_ENABLED  = os.environ.get("ALERTS_NTFY_ENABLED",  "True").lower()  in ("1", "true", "yes", "on")
ALERTS_EMAIL_ENABLED = os.environ.get("ALERTS_EMAIL_ENABLED", "False").lower() in ("1", "true", "yes", "on")
ALERTS_EMAIL_TO      = os.environ.get("ALERTS_EMAIL_TO", "") or ALERT_EMAIL_TO


def _under_pytest() -> bool:
    """HM-FALSE-RED-ALERT 2026-09-09: root cause of the recurring false
    RED_ALERT (season-rotation-abort, polygon-limiter-fail-loud) was
    confirmed from the instrumented abort payload: caller='test', argv is
    pytest, stack traces into tests/test_season_rotation_reactivation_scope.py
    -- the pre-commit hook's own test run was firing real Pushover/ntfy/email
    sends every commit (the historical ~12-day recurrence cadence matches
    commit days exactly). `PYTEST_CURRENT_TEST` is set automatically by
    pytest for the duration of every test, no conftest wiring needed for
    that half; `OT_ALERTS_DISABLED` is an explicit opt-in for any other
    context (interactive scripts, etc.) that wants the same guarantee.
    Checked first, before any network call, in every real send function
    below -- not just the send_alert() dispatcher, since _send_pushover/
    _send_ntfy/_send_email have other callers too."""
    return bool(os.environ.get("PYTEST_CURRENT_TEST") or os.environ.get("OT_ALERTS_DISABLED"))


class AlertLevel:
    INFO      = "info"
    WARNING   = "warning"
    RED_ALERT = "red_alert"


# HM-BUG-BATCH-2026-07-10 item 7 (ALERT STREAM SEPARATION): ops/health
# sentinel alerts and trading-signal alerts currently share one notification
# channel and look identical -- a "database is locked" warning and a stock
# tip render as the same toast. This is the single classifier both the
# notifications API and the frontend keyed off notif.type use so ops alerts
# can get sticky/system-banner treatment while signal alerts keep
# auto-dismissing.
#
# Enumerated from every `alert_type=` call site in the codebase (grep
# 'alert_type=' across engine/*.py, scripts/*.py, dashboard/app.py, main.py
# -- 59 distinct patterns as of this pass). Prefix-matched via startswith()
# so f-string-interpolated per-symbol/per-error-class variants (e.g.
# "dyn_rsi_oversold_AAPL", "hm-u-close_position-ConnectionError") still
# classify correctly without listing every possible suffix.
#
# Ops/health/infra (sticky until acknowledged): hm_ops_sentinel.py's checks
# (heartbeat/backlog/DB-lock/FD-count -- "sentinel_*"), HM-U architecture-
# class unhandled exceptions (order-submission code throwing, not a trading
# decision), data-pipeline/source health watchers, model-quality monitoring
# (degenerate_confidence), and fleet-wide protective actions (guardian
# sweep, CSP cap breach) -- system took action or something broke, not "here
# is a trade idea."
OPS_ALERT_TYPE_PREFIXES = (
    "sentinel_",                       # hm_ops_sentinel.py: FD count, heartbeat, DB locks, queue backlog
    "hm-u-",                           # HM-U doctrine: architecture-class unhandled exceptions
    "hm-push-health-",                 # git push health watchdog
    "hm-at-gamma-schwab-cadence-",     # Schwab data-sync cadence check
    "hm-i-b-item5-drift",              # reconciliation drift
    "hm-holly-",                       # Holly process health / missing-dependency errors
    "source-health-watcher-",          # source health watcher dead/stale
    "scan_liveness",                   # scanner heartbeat
    "war_room_slow_cycle",             # War Room cycle-time degradation
    "wr_layer1_watch_summary",         # War Room layer-1 monitoring summary
    "event_tape_staleness",            # event tape data feed staleness
    "scotty-kirk-ingest",              # Kirk ingest pipeline health
    "eod_report",                      # EOD report generation status
    "congress_scrape_zero",            # congress-trades scraper returned nothing
    "degenerate_confidence",           # AI model producing suspicious fixed confidence
    "tuning_crew_zero_",               # weekly tuning crew produced zero output
    "holdings_",                       # real_holdings.json staleness (Kirk advisory)
    "guardian_sweep_sells",            # fleet-wide protective forced-sell sweep
    "troi_csp_cap_breach",             # agent risk-cap breach
    "deployment_floor",                # HM-DEPLOYMENT-FLOOR: fleet under-deployed for regime -- about
                                        # the fleet's own capital state, not a specific trade idea
    "alert_channel",                   # unclassified fallback (source= was never passed) -- default conservative
)


def classify_alert_stream(alert_type: str) -> str:
    """'ops' or 'signal'. Default is 'signal' -- trading-content alerts
    (dyn_* RSI/MACD/volume, bk_orb/bk_box/bk_avwap scanner picks,
    replay_match, stuck_stop, spread-fill/exit, ollietrades_signal, etc.)
    outnumber ops alerts and are the safe default for anything not
    explicitly recognized as infra/system-health."""
    t = (alert_type or "").lower()
    for prefix in OPS_ALERT_TYPE_PREFIXES:
        if t.startswith(prefix.lower()):
            return "ops"
    return "signal"


# ── State ──────────────────────────────────────────────────────────────────────

_rate_state: dict[str, float] = {}   # alert_type → last_sent_ts
_state_lock = threading.Lock()
_alerts_enabled = True               # toggled by CIC commands


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=20)
    c.row_factory = sqlite3.Row
    return c


def _load_state() -> None:
    """Load persistent state (enabled flag, email) from settings table."""
    global _alerts_enabled, ALERT_EMAIL_TO
    try:
        c = _conn()
        rows = c.execute(
            "SELECT key, value FROM settings WHERE key IN (?,?)",
            (ALERTS_ENABLED_KEY, "alert_email_to")
        ).fetchall()
        c.close()
        for r in rows:
            if r["key"] == ALERTS_ENABLED_KEY:
                _alerts_enabled = (r["value"] or "1") != "0"
            elif r["key"] == "alert_email_to" and r["value"]:
                ALERT_EMAIL_TO = r["value"]
        # HM-NTFY-RATE-PERSIST 2026-05-28: restore rate-limit timestamps so NTFY
        # dedup survives restarts (was in-memory only → alerts re-fired post-restart).
        rs = _conn()
        rsrow = rs.execute(
            "SELECT value FROM settings WHERE key=?", ("alert_rate_state",)
        ).fetchone()
        rs.close()
        if rsrow and rsrow["value"]:
            _rate_state.update({k: float(v) for k, v in json.loads(rsrow["value"]).items()})
        _storm_load()
    except Exception:
        pass


def _save_setting(key: str, value: str) -> None:
    try:
        c = _conn()
        c.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?,?)", (key, value)
        )
        c.commit()
        c.close()
    except Exception as e:
        logger.warning("alert_channels: save_setting failed: %s", e)


def _db_notification(title: str, body: str, severity: str, source: str = "") -> None:
    """Insert into notifications table — browser sees this via /api/notifications.

    HM-DYNALERTS-HYGIENE 2026-07-07: two fixes in this pass.
    (1) `source` param, additive -- writes `type = source or "alert_channel"`
    so emit-time Contact Classification tiering (Rung 1 card) can tell
    dynamic/user alerts (dyn_*/user_*) apart from generic alert_channel
    traffic, per the notifications.type column the /api/notifications poll
    already returns verbatim.
    (2) Column-name bug found while verifying the INSERT against the live
    schema before this edit (per directive instruction, not an incidental
    find): the prior INSERT wrote `created_at`, a column that has never
    existed on `notifications` (real column: `timestamp`, DEFAULT
    CURRENT_TIMESTAMP) -- every call has been silently failing since this
    function was written (confirmed empirically: zero rows with
    type='alert_channel' exist in the live table). The bare
    `except Exception: pass` swallowed every failure invisibly. Fixed by
    dropping the now-redundant explicit column (the table default already
    stamps it) rather than guessing at a `datetime('now')` value for a
    column that already self-populates.
    """
    try:
        c = _conn()
        c.execute(
            "INSERT INTO notifications(title, body, severity, type, icon) "
            "VALUES(?,?,?,?,?)",
            (title, body, severity, source or "alert_channel", "🔔")
        )
        c.commit()
        c.close()
    except Exception:
        pass


# ── Rate limiter ───────────────────────────────────────────────────────────────

def _rate_ok(alert_type: str, rate_limit_secs: int = RATE_LIMIT_SECS) -> bool:
    """True if we haven't successfully SENT this alert_type in the last
    rate_limit_secs. Read-only check -- does NOT consume the window itself;
    call _mark_rate_limit_sent() after a confirmed successful delivery to
    do that (see HM-ALERT-RATE-ON-FAILURE below for why the split matters).

    HM-U (2026-05-05): rate_limit_secs parameterized. Default preserves the
    module-level RATE_LIMIT_SECS=300 (5 min) for the 12 existing callers.
    HM-U architecture-path callers pass rate_limit_secs=86400 (24h) per
    'first occurrence per error class per day' policy (CLAUDE.md § Error
    Handling Posture).
    """
    with _state_lock:
        last = _rate_state.get(alert_type, 0)
        return _time.time() - last >= rate_limit_secs


# ── Storm breaker (HM-ALERT-STORM-BREAKER, dry-dock C11, 2026-09-11) ────────────
# Per-alert_type rate limiting (_rate_ok above) doesn't catch a storm made of
# many DIFFERENT alert_types firing together (the real 07:33 lock-storm
# incident this repo has already lived through: multiple distinct alert
# sources all tripping within the same window, each individually passing its
# own per-type rate limit). This tracks loud-channel (pushover-eligible:
# WARNING+RED_ALERT) dispatch timestamps globally, persisted like the
# rate-limit state above, and once volume in a rolling window crosses a
# threshold, individual pushes stop reaching Pushover -- replaced by ONE
# rate-limited storm notice -- until the window quiets back down. Everything
# still reaches the DB (_db_notification) regardless of storm state; this
# only throttles the phone-interrupting channel.
_STORM_WINDOW_S = 600      # 10 min rolling window
_STORM_THRESHOLD = 8       # loud-channel dispatches within the window to trip it
_STORM_NOTICE_COOLDOWN_S = 1800  # don't re-notify "still storming" more than every 30 min
_storm_timestamps: list[float] = []
_storm_last_notice_ts = 0.0


def _storm_load() -> None:
    global _storm_timestamps, _storm_last_notice_ts
    try:
        c = _conn()
        row = c.execute("SELECT value FROM settings WHERE key=?", ("alert_storm_state",)).fetchone()
        c.close()
        if row and row["value"]:
            state = json.loads(row["value"])
            _storm_timestamps = list(state.get("timestamps", []))
            _storm_last_notice_ts = float(state.get("last_notice_ts", 0.0))
    except Exception:
        pass


def _storm_save() -> None:
    try:
        _save_setting("alert_storm_state", json.dumps({
            "timestamps": _storm_timestamps, "last_notice_ts": _storm_last_notice_ts,
        }))
    except Exception:
        pass


def _storm_record_and_check() -> bool:
    """Record one loud-channel dispatch attempt now; return True if a storm
    is currently active (volume in the last _STORM_WINDOW_S seconds >=
    _STORM_THRESHOLD, evaluated AFTER recording this one)."""
    global _storm_timestamps
    with _state_lock:
        now = _time.time()
        _storm_timestamps = [t for t in _storm_timestamps if now - t < _STORM_WINDOW_S] + [now]
        active = len(_storm_timestamps) >= _STORM_THRESHOLD
    _storm_save()
    return active


def _storm_notice_due() -> bool:
    """True once per _STORM_NOTICE_COOLDOWN_S while a storm is active --
    the single 'still storming' ping that replaces N suppressed individual
    pushes, itself rate-limited so the storm notice can't itself storm."""
    global _storm_last_notice_ts
    with _state_lock:
        now = _time.time()
        if now - _storm_last_notice_ts < _STORM_NOTICE_COOLDOWN_S:
            return False
        _storm_last_notice_ts = now
    _storm_save()
    return True


def _mark_rate_limit_sent(alert_type: str) -> None:
    """HM-ALERT-RATE-ON-FAILURE (2026-07-07): the window used to be consumed
    inside _rate_ok() at CHECK time, before send_alert() even attempted
    delivery -- so a genuine network failure (e.g. HM-NTFY-IPV6-NOROUTE
    above: 0/20 real sends from a cron-invoked process, 100% silent
    failure) ALSO silently burned the retry budget. The sentinel's own log
    said "ALERT dispatched" every 5-min cron tick, but nothing had actually
    reached a phone, and the next real attempt wouldn't fire for the full
    1800s window either -- an alarm's own failure mode compounding its
    retry mechanism. Call only after send_alert() confirms at least one
    channel actually delivered.
    """
    with _state_lock:
        _rate_state[alert_type] = _time.time()
        _snapshot = dict(_rate_state)
    # HM-NTFY-RATE-PERSIST 2026-05-28: persist to settings so dedup survives restarts
    try:
        _save_setting("alert_rate_state", json.dumps(_snapshot))
    except Exception:
        pass


# ── Channel senders ────────────────────────────────────────────────────────────

# HM-NTFY-IPV6-NOROUTE (2026-07-07): this box has no working IPv6 route to
# ntfy.sh -- confirmed directly (socket.create_connection to ntfy.sh's AAAA
# address raises OSError [Errno 65] No route to host, 100% reproducible,
# both Python interpreters on this box, IPv4 always succeeds). Real-world
# symptom: scripts/hm_ops_sentinel.py's cron invocation (.venv/python3.14)
# got 0/20 successful ntfy sends in logs/hm_ops_sentinel_cron.log -- every
# single alert this whole time silently never reached a phone, while the
# sentinel's own log said "ALERT dispatched" (send_alert() doesn't raise on
# a network failure inside _send_ntfy, it just returns False and logs a
# warning -- exactly the "alarm shares a failure mode with silence" case).
# git_push_health_check.py (venv/python3.9, once-daily cron) has succeeded
# via the same _send_ntfy() code, so the getaddrinfo() address-family
# ordering that picks IPv6 first is context-dependent (interpreter/cron
# invocation specifics), not fully root-caused -- rather than depend on
# ordering being consistent across execution contexts, force IPv4 for the
# duration of this call. IPv6 not working anywhere on this box means the
# forced window carries no real cost to other concurrent socket use.
_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = _socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, _socket.AF_INET, type, proto, flags)


def _send_ntfy(title: str, message: str, priority: str = "default", tags: str = "ollietrades", topic: str = "") -> bool:
    """Push via ntfy.sh (iPhone / Android / browser). topic overrides NTFY_TOPIC."""
    if _under_pytest():
        logger.info("ntfy suppressed (pytest/OT_ALERTS_DISABLED): [%s] %s", title, message[:80])
        return False
    # DECOM-SILENCE 2026-07-19 — LIFTED 2026-09-11 (Admiral decision, dry-dock
    # C11 follow-up). Was: all ntfy pushes silenced ahead of Gate 2 full
    # removal. Gate 2 landed weeks ago; the guard was never revisited, and
    # every INFO/WARNING alert had zero real phone delivery for ~2 months
    # as a result (see relay_2026-09-11_C11_pushover_redesign.md). Restored
    # to real delivery.
    _topic = topic or NTFY_TOPIC
    if not _topic:
        return False
    try:
        ascii_title = title.encode("ascii", errors="replace").decode("ascii").strip()
        req = urllib.request.Request(
            f"{NTFY_BASE}/{_topic}",
            data=f"{title}\n{message}".encode("utf-8"),
            headers={
                "Title":        ascii_title or "TradeMinds",
                "Priority":     priority,
                "Tags":         tags,
                "Content-Type": "text/plain; charset=utf-8",
            },
            method="POST",
        )
        with _ntfy_ipv4_lock:
            _socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                with urllib.request.urlopen(req, timeout=8) as r:
                    logger.info("ntfy sent [%s]: %s", r.status, ascii_title)
            finally:
                _socket.getaddrinfo = _orig_getaddrinfo
        return True
    except Exception as e:
        logger.warning("ntfy failed: %s", e)
        return False


def _send_pushover(title: str, message: str, priority: int = 0) -> bool:
    """PUSHOVER-RED-ALERT 2026-08-28 — originally RED_ALERT lane only,
    extended to WARNING 2026-09-11 (dry-dock C11, tiered routing) since
    ntfy stays fully silenced (DECOM-SILENCE 2026-07-19 -- see
    _send_ntfy, an unconditional early-return, unreverted since) and
    without this, WARNING-level alerts reach no phone at all. Priority
    (Pushover scale: -2 lowest/no-notify .. 2 emergency-repeat) is the
    tier: RED_ALERT callers pass 1, WARNING callers pass -1 (quiet
    notification, no sound/vibration -- present in the app, not
    interrupting). Priority 2 is reserved for GPU buy alerts and never
    used here.

    HM-PUSHOVER-ENV-REPOINT (2026-09-11, C11 correction): the OllieTrades
    Pushover app's own PUSHOVER_TOKEN/PUSHOVER_USER were added to .env on
    2026-09-09 -- no separate app needed (an earlier pass here wrongly
    assumed one did and added now-removed PUSHOVER_OLLIETRADES_TOKEN/_USER
    env vars that were never populated). Tries os.environ first (.env, via
    config.py's load_dotenv -- also loaded directly below so this module
    works standalone too), falling back to the older /usr/local/etc/
    pushover.env file creds on an actual SEND failure, not just absence --
    found live 2026-09-11: .env's PUSHOVER_TOKEN is 120 chars (Pushover's
    format is 30; their API rejects it outright, "application token is
    invalid"), while the file's token is correctly shaped and was the one
    actually delivering RED_ALERT before this pass. A presence-only
    fallback would have gone dark the moment this repoint shipped, using a
    bad value with no retry. **The .env value itself needs a real fix
    (wrong secret pasted in on 2026-09-09?) -- flagged, not silently
    worked around forever.**
    """
    if _under_pytest():
        logger.info("pushover suppressed (pytest/OT_ALERTS_DISABLED): %s", title[:80])
        return False
    import urllib.parse, urllib.request
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(override=False)  # don't clobber anything already set by the caller

    def _file_creds() -> tuple[str | None, str | None]:
        env = {}
        try:
            for line in open("/usr/local/etc/pushover.env"):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
        except Exception as e:
            logger.warning("pushover env file unreadable: %s", e)
        return env.get("PUSHOVER_TOKEN"), env.get("PUSHOVER_USER")

    candidates: list[tuple[str, str, str]] = []
    env_tok, env_usr = os.environ.get("PUSHOVER_TOKEN"), os.environ.get("PUSHOVER_USER")
    if env_tok and env_usr:
        candidates.append((env_tok, env_usr, ".env"))
    file_tok, file_usr = _file_creds()
    if file_tok and file_usr and (file_tok, file_usr) != (env_tok, env_usr):
        candidates.append((file_tok, file_usr, "/usr/local/etc/pushover.env"))
    if not candidates:
        logger.warning("pushover creds missing (checked .env and the fallback file)")
        return False

    # HM-PUSHOVER-TIMESTAMP-2026-09-09: explicit send-time, not left to
    # Pushover's own receipt-time default. Found while investigating a
    # RED_ALERT that displayed a ~12-day-old timestamp (HM-FALSE-RED-ALERT,
    # docs/XO_BACKLOG.md 2026-08-29, recurred 2026-09-09) -- this function
    # never sent a `timestamp` field, so whatever Pushover displayed wasn't
    # controlled here. Pinning it removes that as a variable regardless of
    # any upstream queuing/delay between construction and this call.
    last_err = None
    for tok, usr, source in candidates:
        fields = {"token": tok, "user": usr, "title": title[:250],
                  "message": message[:1024], "priority": priority,
                  "timestamp": int(_time.time())}
        try:
            req = urllib.request.Request(
                "https://api.pushover.net/1/messages.json",
                data=urllib.parse.urlencode(fields).encode())
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()
            logger.info("pushover sent via %s: %s", source, title[:60])
            return True
        except Exception as e:
            last_err = e
            logger.warning("pushover failed via %s: %s", source, e)
    logger.warning("pushover failed on all %d credential source(s): %s", len(candidates), last_err)
    return False


def _send_email(subject: str, body: str, to: str = "") -> bool:
    """Send email via SMTP. Requires SMTP_HOST, SMTP_USER, SMTP_PASS in .env."""
    if _under_pytest():
        logger.info("email suppressed (pytest/OT_ALERTS_DISABLED): %s", subject[:80])
        return False
    to_addr = to or ALERT_EMAIL_TO
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, to_addr]):
        return False
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[TradeMinds] {subject}"
        msg["From"]    = SMTP_USER
        msg["To"]      = to_addr
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo()
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [to_addr], msg.as_string())
        logger.info("Email sent to %s: %s", to_addr, subject)
        return True
    except Exception as e:
        logger.warning("Email failed: %s", e)
        return False


# ── Public API ─────────────────────────────────────────────────────────────────

def push_ntfy(topic: str, title: str, body: str, priority: str = "default", tags=None) -> bool:
    """HM-UHURA-HAILS — single outbound ntfy primitive. Honors ALERTS_NTFY_ENABLED.
    `tags` may be a list/tuple or a comma-string."""
    if not ALERTS_NTFY_ENABLED:
        return False
    _tags = ",".join(tags) if isinstance(tags, (list, tuple)) else (tags or "ollietrades")
    return _send_ntfy(title, body, priority, _tags, topic)


def send_email(subject: str, html_body: str, to: str | None = None) -> bool:
    """HM-UHURA-HAILS — HTML email via Gmail SMTP. Honors ALERTS_EMAIL_ENABLED.
    Never logs the app password."""
    if _under_pytest():
        logger.info("email suppressed (pytest/OT_ALERTS_DISABLED): %s", subject[:80])
        return False
    if not ALERTS_EMAIL_ENABLED:
        return False
    to_addr = to or ALERTS_EMAIL_TO
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, to_addr]):
        logger.warning("send_email skipped — SMTP not fully configured")
        return False
    try:
        import smtplib, re as _re
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[TradeMinds] {subject}"
        msg["From"] = SMTP_USER
        msg["To"]   = to_addr
        msg.attach(MIMEText(_re.sub(r"<[^>]+>", "", html_body), "plain", "utf-8"))  # text fallback
        msg.attach(MIMEText(html_body, "html", "utf-8"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo(); s.starttls(); s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [to_addr], msg.as_string())
        logger.info("HTML email sent to %s: %s", to_addr, subject)
        return True
    except Exception as e:
        logger.warning("send_email failed: %s: %r", type(e).__name__, e)
        return False


def send_alert(
    message: str,
    level: str = AlertLevel.INFO,
    alert_type: str = "general",
    title: str = "",
    bypass_rate_limit: bool = False,
    audience: str = "admin",   # "admin" | "crew" | "all"
    rate_limit_secs: int = RATE_LIMIT_SECS,  # HM-U: per-call override; default 300s, HM-U callers pass 86400 (24h)
    source: str = "",  # HM-DYNALERTS-HYGIENE 2026-07-07: emit-time Contact Classification
                        # tiering -- threaded to _db_notification's `type` column so Rung 1's
                        # ACTIONABLE/INFORMATIONAL split works. Appended LAST (not inserted
                        # among existing params) so no positional-arg caller can collide with it.
                        # Additive/optional: every existing caller is unaffected.
) -> dict:
    """
    Send alert to appropriate channels based on level.

    Returns dict with channel results: {ntfy, email, browser}.

    HM-U (2026-05-05): rate_limit_secs parameter added. Default preserves
    5-min behavior for existing callers; HM-U architecture-path callers pass
    86400 (per CLAUDE.md § Error Handling Posture, principle 3).
    """
    _load_state()
    if not _alerts_enabled:
        return {"skipped": "alerts disabled"}

    if not bypass_rate_limit and not _rate_ok(alert_type, rate_limit_secs):
        return {"skipped": f"rate_limited (cooldown {rate_limit_secs}s per type)"}

    if not title:
        prefix = {"info": "ℹ️", "warning": "⚠️", "red_alert": "🚨"}.get(level, "📢")
        title = f"{prefix} TradeMinds {level.replace('_', ' ').title()}"

    ntfy_priority = {
        AlertLevel.INFO:      "default",
        AlertLevel.WARNING:   "high",
        AlertLevel.RED_ALERT: "urgent",
    }.get(level, "default")

    ntfy_tags = {
        AlertLevel.INFO:      "ollietrades",
        AlertLevel.WARNING:   "warning,ollietrades",
        AlertLevel.RED_ALERT: "rotating_light,ollietrades",
    }.get(level, "ollietrades")

    # Resolve ntfy topic(s) based on audience
    def _ntfy_topics() -> list[str]:
        if audience == "crew":
            return [t for t in [NTFY_CREW_TOPIC] if t]
        if audience == "all":
            topics = []
            if NTFY_ADMIN_TOPIC: topics.append(NTFY_ADMIN_TOPIC)
            if NTFY_CREW_TOPIC and NTFY_CREW_TOPIC != NTFY_ADMIN_TOPIC:
                topics.append(NTFY_CREW_TOPIC)
            return topics or [NTFY_TOPIC]
        return [t for t in [NTFY_ADMIN_TOPIC] if t]  # default: admin only

    results: dict = {}

    # HM-BUG-BATCH-2026-07-09: `source` defaults to "" for the large majority
    # of callers (e.g. hm_ops_sentinel.py never passes it), which made every
    # one of those alerts collapse to the generic notifications.type value
    # "alert_channel" -- no stable per-check identifier reached the browser,
    # so the /classic toast layer had nothing to dedup identical repeats on.
    # `alert_type` (e.g. "sentinel_signals_v2_queue") is already a stable,
    # caller-specific key used for the rate limiter above -- fall back to it
    # here too. An explicit `source` still wins (Contact Classification
    # tiering, dyn_*/user_* distinction per send_alert's docstring above).
    _notif_type = source or alert_type

    # INFO → ntfy only
    if level == AlertLevel.INFO:
        results["ntfy"] = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in _ntfy_topics())
        _db_notification(title, message, "info", _notif_type)

    # WARNING → ntfy (silenced, see _send_ntfy) + browser notification (DB)
    # + Pushover at quiet priority (HM-ALERT-STORM-BREAKER / C11 tiered
    # routing, 2026-09-11) -- WARNING previously had no real phone delivery
    # at all while ntfy stays under DECOM-SILENCE, which is the specific
    # "alerts need to be usable before we undock" gap C11 closes.
    elif level == AlertLevel.WARNING:
        results["ntfy"]    = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in _ntfy_topics())
        _db_notification(title, message, "warning", _notif_type)
        results["browser"] = True
        if _storm_record_and_check():
            if _storm_notice_due():
                results["pushover"] = _send_pushover(
                    "STORM — alerts suppressed", f"{_STORM_THRESHOLD}+ alerts in the last "
                    f"{_STORM_WINDOW_S // 60} min. Individual pushes paused; check the dashboard "
                    f"for the full list. Most recent: {title}", priority=-1)
        else:
            results["pushover"] = _send_pushover(title, message, priority=-1)  # quiet, no sound

    # RED ALERT → all channels
    elif level == AlertLevel.RED_ALERT:
        crit_topics = _ntfy_topics() + [NTFY_CRITICAL_TOPIC]   # HM-UHURA-HAILS: keep admin topic AND add critical lane
        results["ntfy"]    = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in crit_topics)
        # RED_ALERT is never storm-suppressed outright (that would be the
        # exact "alarm shares a failure mode with the thing it watches"
        # doctrine violation) -- it always gets its own individual push,
        # below. It DOES still contribute to the shared storm counter (so a
        # RED_ALERT flood correctly counts toward tripping the breaker for
        # subsequent WARNING alerts too) and piggybacks one extra
        # storm-context line onto its own message when a storm notice is due.
        _storm_active = _storm_record_and_check()
        _storm_suffix = ""
        if _storm_active and _storm_notice_due():
            _storm_suffix = f"\n\n[STORM: {_STORM_THRESHOLD}+ alerts in {_STORM_WINDOW_S // 60} min]"
        results["pushover"] = _send_pushover(f"RED ALERT: {title}", message + _storm_suffix, priority=1)
        _db_notification(title, message, "critical", _notif_type)
        results["browser"] = True
        results["email"]   = _send_email(title, f"{message}\n\nLevel: RED ALERT\nType: {alert_type}")

    # Only consume the rate-limit window on a confirmed EXTERNAL delivery
    # (see _mark_rate_limit_sent's docstring). Deliberately checks ntfy/email
    # specifically, not results.get("browser") -- that key is hardcoded True
    # unconditionally above (a pre-existing inaccuracy, not touched here) and
    # would make `any(results.values())` always truthy for WARNING/RED_ALERT,
    # silently defeating this fix for exactly the levels the sentinel uses.
    if not bypass_rate_limit and (results.get("ntfy") or results.get("email")
                                  or results.get("pushover")):
        _mark_rate_limit_sent(alert_type)

    logger.info("Alert dispatched [%s/%s]: %s", level, alert_type, message[:80])
    return results


def send_test_alert(channel: str | None = None) -> dict:
    """Send test alert to all (or specific) channels — bypasses rate limit."""
    msg = "Test alert from USS TradeMinds. All systems nominal."
    title = "🧪 TradeMinds Test Alert"
    results = {}
    if channel in (None, "ntfy"):
        results["ntfy"] = _send_ntfy(title, msg, "default", "test,ollietrades")
    if channel in (None, "email"):
        results["email"] = _send_email(title, msg)
    if channel in (None, "browser"):
        _db_notification(title, msg, "info")
        results["browser"] = True
    return results


# ── CIC command handler ────────────────────────────────────────────────────────

def handle_cic_command(command: str) -> str | None:
    """
    Parse and execute alert-related CIC commands.
    Returns a response string if handled, None otherwise.
    """
    global _alerts_enabled, ALERT_EMAIL_TO
    cmd = command.strip().lower()

    if cmd in ("alerts on", "enable alerts"):
        _alerts_enabled = True
        _save_setting(ALERTS_ENABLED_KEY, "1")
        return "✅ Alert channels enabled."

    if cmd in ("alerts off", "disable alerts"):
        _alerts_enabled = False
        _save_setting(ALERTS_ENABLED_KEY, "0")
        return "🔕 Alert channels disabled."

    if cmd in ("alert test", "test alert", "test alerts"):
        results = send_test_alert()
        lines = ["🧪 Test alert sent:"]
        for ch, ok in results.items():
            lines.append(f"  {ch}: {'✅' if ok else '❌'}")
        return "\n".join(lines)

    if cmd.startswith("set alert email "):
        email = command.strip()[len("set alert email "):].strip()
        if "@" in email and "." in email:
            ALERT_EMAIL_TO = email
            _save_setting("alert_email_to", email)
            return f"📧 Alert email set to: {email}"
        return "❌ Invalid email address."

    if cmd in ("alert status", "alerts status"):
        status = "enabled" if _alerts_enabled else "disabled"
        email_status = ALERT_EMAIL_TO or "not configured"
        ntfy_status  = (NTFY_TOPIC[:4] + "****") if NTFY_TOPIC else "not configured"
        smtp_status  = "configured" if all([SMTP_HOST, SMTP_USER, SMTP_PASS]) else "not configured"
        return (
            f"📡 Alert Channels ({status}):\n"
            f"  ntfy.sh: {ntfy_status}\n"
            f"  email: {email_status} (SMTP: {smtp_status})\n"
            f"  browser: always active (DB notifications)\n"
            f"  rate limit: {RATE_LIMIT_SECS}s per alert type"
        )

    return None   # Not an alert command


# ── Convenience shortcuts ──────────────────────────────────────────────────────

def alert_info(message: str, alert_type: str = "info",
               rate_limit_secs: int = RATE_LIMIT_SECS) -> None:
    """Fire-and-forget INFO alert in a background thread.

    HM-U: rate_limit_secs forwarded to send_alert; default preserves 5-min.
    """
    threading.Thread(
        target=send_alert,
        kwargs={"message": message, "level": AlertLevel.INFO,
                "alert_type": alert_type, "rate_limit_secs": rate_limit_secs},
        daemon=True,
    ).start()


def alert_warning(message: str, alert_type: str = "warning",
                  rate_limit_secs: int = RATE_LIMIT_SECS) -> None:
    """Fire-and-forget WARNING alert in a background thread.

    HM-U: rate_limit_secs forwarded to send_alert; default preserves 5-min.
    """
    threading.Thread(
        target=send_alert,
        kwargs={"message": message, "level": AlertLevel.WARNING,
                "alert_type": alert_type, "rate_limit_secs": rate_limit_secs},
        daemon=True,
    ).start()


def alert_red(message: str, alert_type: str = "red_alert", title: str = "🚨 RED ALERT",
              rate_limit_secs: int = RATE_LIMIT_SECS) -> None:
    """Fire-and-forget RED ALERT in a background thread.

    HM-U: rate_limit_secs forwarded to send_alert; default preserves 5-min.
    """
    threading.Thread(
        target=send_alert,
        kwargs={"message": message, "level": AlertLevel.RED_ALERT,
                "alert_type": alert_type, "title": title,
                "rate_limit_secs": rate_limit_secs},
        daemon=True,
    ).start()


# Load state at module import
_load_state()
