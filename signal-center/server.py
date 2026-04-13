"""
TradeMinds Signal Command Center — Port 9000
The ultimate signal aggregator. Every indicator. Every grade.
"""
from flask import Flask, send_file, jsonify, request, send_from_directory, Response, stream_with_context, session, redirect, url_for, make_response
import requests
import sqlite3
import json
import os
import subprocess
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from queue import Queue, Empty
import sys
import time as _time
import logging as _sc_log

# ── Load environment ─────────────────────────────────────────────────────────
_sc_dir = os.path.dirname(os.path.abspath(__file__))
_env_path = os.path.join(_sc_dir, "..", ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _ef:
        for _line in _ef:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

import pyotp as _sc_pyotp

app = Flask(__name__)
app.secret_key = os.environ.get("TRADEMINDS_SECRET", "")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=24)

# ── Auth credentials ─────────────────────────────────────────────────────────
_SC_USER = os.environ.get("DASHBOARD_USER", "")
_SC_PASS = os.environ.get("DASHBOARD_PASS", "")
_SC_TOTP_SECRET = os.environ.get("TOTP_SECRET", "")
_sc_totp = _sc_pyotp.TOTP(_SC_TOTP_SECRET) if _SC_TOTP_SECRET else None
_LOCALHOST = {"127.0.0.1", "::1", "localhost"}

# ── Security logger ───────────────────────────────────────────────────────────
_logs_dir = os.path.join(_sc_dir, "..", "logs")
os.makedirs(_logs_dir, exist_ok=True)
_sec_log = _sc_log.getLogger("sc.security")
if not _sec_log.handlers:
    _sh = _sc_log.FileHandler(os.path.join(_logs_dir, "security.log"))
    _sh.setFormatter(_sc_log.Formatter("%(asctime)s %(message)s"))
    _sec_log.addHandler(_sh)
_sec_log.setLevel(_sc_log.WARNING)

# ── Login failure tracking: ip → {"count": int, "blocked_until": float} ──────
_sc_failures: dict = {}
_SC_MAX_ATTEMPTS = 5
_SC_BLOCK_SECS   = 900  # 15 minutes

# ── AI / bot user-agents to block ─────────────────────────────────────────────
_SC_BOT_UA = [
    "googlebot", "bingbot", "yandexbot", "baiduspider",
    "scrapy", "python-requests", "python-urllib",
    "wget/", "crawl", "spider", "semrushbot", "ahrefsbot",
    # AI crawlers
    "gptbot", "chatgpt-user", "ccbot", "anthropic-ai",
    "claude-web", "google-extended", "bytespider", "amazonbot",
]

# ── Login page HTML ───────────────────────────────────────────────────────────
_SC_LOGIN_PAGE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>OllieTrades Signal Center — Authorization Required</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0a0e1a;color:#e0e6f0;font-family:'Courier New',monospace;
  display:flex;align-items:center;justify-content:center;min-height:100vh;padding:16px;
  background-image:radial-gradient(ellipse at 50% 0%,#1a2040 0%,#0a0e1a 70%)}}
.box{{background:linear-gradient(135deg,#111827,#1a2040);border:1px solid #2d4a7a;
  border-radius:16px;padding:32px 28px 28px;width:100%;max-width:340px;
  box-shadow:0 0 40px rgba(0,188,212,0.12);text-align:center}}
.badge{{font-size:44px;margin-bottom:10px}}
h1{{font-size:17px;color:#60a5fa;letter-spacing:2px;margin-bottom:3px}}
.sub{{font-size:11px;color:#f59e0b;letter-spacing:1px;margin-bottom:24px}}
label{{display:block;font-size:11px;color:#94a3b8;margin-bottom:3px;letter-spacing:1px;text-align:left}}
input{{width:100%;padding:10px 12px;background:#0f172a;border:1px solid #334155;
  border-radius:6px;color:#e0e6f0;font-family:inherit;font-size:16px;margin-bottom:14px;
  outline:none;transition:border .2s}}
input:focus{{border-color:#3b82f6}}
button{{width:100%;padding:13px;background:linear-gradient(135deg,#2563eb,#1d4ed8);
  border:none;border-radius:6px;color:#fff;font-family:inherit;font-size:15px;
  font-weight:bold;cursor:pointer;letter-spacing:1px}}
button:hover{{background:linear-gradient(135deg,#3b82f6,#2563eb)}}
.err{{background:#7f1d1d;border:1px solid #dc2626;border-radius:6px;padding:10px;
  font-size:13px;color:#fca5a5;margin-bottom:14px}}
.foot{{margin-top:18px;font-size:10px;color:#334155}}
</style>
</head>
<body>
<div class="box">
  <div class="badge">📡</div>
  <h1>SIGNAL CENTER</h1>
  <div class="sub">AUTHORIZED PERSONNEL ONLY</div>
  {error}
  <form method="POST" action="/login">
    <label>OFFICER IDENTIFICATION</label>
    <input type="text" name="username" autocomplete="username" required>
    <label>ACCESS CODE</label>
    <input type="password" name="password" autocomplete="current-password" required>
    <button type="submit">ENGAGE ▶</button>
  </form>
  <div class="foot">OllieTrades Signal Center • Port 9000</div>
</div>
</body>
</html>"""

_SC_TOTP_PAGE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>OllieTrades Signal Center — 2FA Verification</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0a0e1a;color:#e0e6f0;font-family:'Courier New',monospace;
  display:flex;align-items:center;justify-content:center;min-height:100vh;padding:16px;
  background-image:radial-gradient(ellipse at 50% 0%,#1a2040 0%,#0a0e1a 70%)}}
.box{{background:linear-gradient(135deg,#111827,#1a2040);border:1px solid #2d4a7a;
  border-radius:16px;padding:32px 28px 28px;width:100%;max-width:340px;
  box-shadow:0 0 40px rgba(0,188,212,0.12);text-align:center}}
.badge{{font-size:44px;margin-bottom:10px}}
h1{{font-size:17px;color:#60a5fa;letter-spacing:2px;margin-bottom:3px}}
.sub{{font-size:11px;color:#f59e0b;letter-spacing:1px;margin-bottom:24px}}
label{{display:block;font-size:11px;color:#94a3b8;margin-bottom:3px;letter-spacing:1px;text-align:left}}
input{{width:100%;padding:10px 12px;background:#0f172a;border:1px solid #334155;
  border-radius:6px;color:#e0e6f0;font-family:inherit;font-size:24px;letter-spacing:8px;
  text-align:center;margin-bottom:14px;outline:none;transition:border .2s}}
input:focus{{border-color:#3b82f6}}
button{{width:100%;padding:13px;background:linear-gradient(135deg,#2563eb,#1d4ed8);
  border:none;border-radius:6px;color:#fff;font-family:inherit;font-size:15px;
  font-weight:bold;cursor:pointer;letter-spacing:1px}}
button:hover{{background:linear-gradient(135deg,#3b82f6,#2563eb)}}
.err{{background:#7f1d1d;border:1px solid #dc2626;border-radius:6px;padding:10px;
  font-size:13px;color:#fca5a5;margin-bottom:14px}}
.hint{{font-size:11px;color:#475569;margin-bottom:16px;line-height:1.5}}
.foot{{margin-top:18px;font-size:10px;color:#334155}}
</style>
</head>
<body>
<div class="box">
  <div class="badge">🔐</div>
  <h1>2FA VERIFICATION</h1>
  <div class="sub">AUTHENTICATOR REQUIRED</div>
  {error}
  <div class="hint">Enter the 6-digit code from your authenticator app for <strong>OllieTrades</strong></div>
  <form method="POST" action="/login?step=2">
    <label>6-DIGIT CODE</label>
    <input type="text" name="totp_code" maxlength="6" pattern="[0-9]{{6}}" inputmode="numeric" autocomplete="one-time-code" autofocus required>
    <button type="submit">VERIFY ▶</button>
  </form>
  <div style="margin-top:14px;"><a href="/login" style="font-size:11px;color:#334155;text-decoration:none;">← Back to login</a></div>
  <div class="foot">OllieTrades Signal Center • Port 9000</div>
</div>
</body>
</html>"""


def _sc_check_session() -> bool:
    return session.get("authenticated") is True


def _sc_is_localhost() -> bool:
    return (request.remote_addr or "") in _LOCALHOST
BRIDGE = "http://127.0.0.1:8080"
PIN    = "2026"
DB_PATH     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")
EXPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)

# ── Trade-levels in-memory cache (15-min TTL) ────────────────────────────────
_TL_TTL     = 900          # seconds
_tl_cache   = {}           # symbol → {"ts": float, "data": dict}
_tl_lock    = threading.Lock()

DAILY_WATCHLIST_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "daily_watchlist.json"
)
FIXED_WATCHLIST = [
    "SPY", "QQQ", "NVDA", "TSLA", "AAPL",
    "META", "AMZN", "MSFT", "AMD", "GOOGL",
]

# ── Session management ──────────────────────────────────────────────────────
_session_lock   = threading.Lock()
_session_cookie = {}

def _get_session():
    global _session_cookie
    try:
        r = requests.post(
            f"{BRIDGE}/login/pin",
            json={"pin": PIN},
            allow_redirects=False,
            timeout=5,
        )
        cookies = dict(r.cookies)
        if cookies:
            with _session_lock:
                _session_cookie = cookies
            return cookies
    except Exception as e:
        print(f"[signal-center] Auth failed: {e}")
    return {}

def _bridge_get(endpoint, timeout=5):
    global _session_cookie
    with _session_lock:
        cookies = dict(_session_cookie)
    try:
        r = requests.get(f"{BRIDGE}{endpoint}", cookies=cookies, timeout=timeout, allow_redirects=True)
        if r.status_code in (302, 303, 307, 308, 401):
            cookies = _get_session()
            with _session_lock:
                _session_cookie = cookies
            r = requests.get(f"{BRIDGE}{endpoint}", cookies=cookies, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None

# Authenticate at startup
_get_session()

# ── Database ────────────────────────────────────────────────────────────────
def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS signal_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT    NOT NULL,
            signal_name TEXT    NOT NULL,
            value       TEXT,
            score       INTEGER,
            grade       TEXT,
            raw_data    TEXT,
            source      TEXT DEFAULT 'bridge'
        );
        CREATE TABLE IF NOT EXISTS daily_snapshot (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            date         TEXT NOT NULL,
            master_score INTEGER,
            master_grade TEXT,
            signal_data  TEXT,
            crew_data    TEXT,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS predictions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            snap_date    TEXT NOT NULL,
            symbol       TEXT NOT NULL,
            price_at     REAL,
            master_score INTEGER,
            regime       TEXT,
            recommendation TEXT,
            tp1          REAL,
            tp2          REAL,
            stop_loss    REAL,
            rr           REAL,
            signal_json  TEXT,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS prediction_results (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER,
            snap_date    TEXT NOT NULL,
            symbol       TEXT NOT NULL,
            price_at     REAL,
            price_next   REAL,
            pct_change   REAL,
            hit_tp1      INTEGER DEFAULT 0,
            hit_tp2      INTEGER DEFAULT 0,
            hit_sl       INTEGER DEFAULT 0,
            correct      INTEGER DEFAULT 0,
            checked_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS prediction_accuracy (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            period_date  TEXT NOT NULL,
            total        INTEGER DEFAULT 0,
            correct      INTEGER DEFAULT 0,
            hit_tp1      INTEGER DEFAULT 0,
            hit_tp2      INTEGER DEFAULT 0,
            hit_sl       INTEGER DEFAULT 0,
            accuracy_pct REAL,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_signal_time  ON signal_history(timestamp);
        CREATE INDEX IF NOT EXISTS idx_signal_name  ON signal_history(signal_name);
        CREATE INDEX IF NOT EXISTS idx_daily_date   ON daily_snapshot(date);
        CREATE INDEX IF NOT EXISTS idx_pred_date    ON predictions(snap_date);
        CREATE INDEX IF NOT EXISTS idx_pred_sym     ON predictions(symbol);
        CREATE INDEX IF NOT EXISTS idx_res_date     ON prediction_results(snap_date);

        -- ── Trade Signals (from ai_brain) ───────────────────────────────────
        CREATE TABLE IF NOT EXISTS trade_signals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            type          TEXT NOT NULL DEFAULT 'SWING',
            symbol        TEXT NOT NULL,
            action        TEXT NOT NULL,
            entry_price   REAL,
            stop_loss     REAL,
            take_profit   REAL,
            confidence    INTEGER,
            agent_name    TEXT,
            model_used    TEXT,
            reasoning     TEXT,
            context_json  TEXT,
            sources_json  TEXT,
            timeframe     TEXT DEFAULT 'SWING',
            status        TEXT NOT NULL DEFAULT 'NEW',
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            executed_at   TEXT,
            dismissed_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id           INTEGER NOT NULL,
            tracked_entry       REAL,
            tracked_high        REAL,
            tracked_low         REAL,
            tracked_current     REAL,
            would_hit_tp        INTEGER DEFAULT 0,
            would_hit_sl        INTEGER DEFAULT 0,
            theoretical_pnl     REAL,
            actual_pnl          REAL,
            tracking_start      TEXT DEFAULT CURRENT_TIMESTAMP,
            last_updated        TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (signal_id) REFERENCES trade_signals(id)
        );
        CREATE INDEX IF NOT EXISTS idx_ts_created  ON trade_signals(created_at);
        CREATE INDEX IF NOT EXISTS idx_ts_symbol   ON trade_signals(symbol);
        CREATE INDEX IF NOT EXISTS idx_ts_status   ON trade_signals(status);
        CREATE INDEX IF NOT EXISTS idx_so_signal   ON signal_outcomes(signal_id);

        -- ── Execution Log (one-click Alpaca trade tracking) ───────────────────
        CREATE TABLE IF NOT EXISTS execution_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id     INTEGER,
            symbol        TEXT NOT NULL,
            direction     TEXT NOT NULL DEFAULT 'BUY',
            qty           REAL,
            entry_price   REAL,
            fill_price    REAL,
            stop_loss     REAL,
            tp1           REAL,
            tp2           REAL,
            tp3           REAL,
            grade         TEXT,
            prob          REAL,
            source        TEXT,
            alpaca_order_id TEXT,
            status        TEXT NOT NULL DEFAULT 'PENDING',
            executed_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            notes         TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_exec_symbol ON execution_log(symbol);
        CREATE INDEX IF NOT EXISTS idx_exec_ts     ON execution_log(executed_at);
    """)
    db.commit()
    db.close()

init_db()

def score_to_grade(score):
    if score >= 80: return 'A'
    if score >= 65: return 'B'
    if score >= 50: return 'C'
    if score >= 35: return 'D'
    return 'E'

# ── SSE subscriber pool ──────────────────────────────────────────────────────
_sse_subscribers: list = []
_sse_lock = threading.Lock()


def _push_to_sse(data: dict):
    """Broadcast a dict to all live SSE subscribers."""
    msg = json.dumps(data)
    with _sse_lock:
        dead = []
        for q in _sse_subscribers:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            _sse_subscribers.remove(q)


def _speak_signal(text: str):
    """Voice alert via edge-tts AndrewNeural (macOS afplay)."""
    try:
        import tempfile
        out = tempfile.mktemp(suffix=".mp3")
        subprocess.Popen(
            ["edge-tts", "--voice", "en-US-AndrewNeural", "--text", text, "--write-media", out],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ).wait(timeout=12)
        subprocess.Popen(["afplay", out])
    except Exception:
        pass


def _macos_notify(title: str, body: str):
    """macOS notification via osascript."""
    try:
        safe_body = body.replace('"', "'")[:200]
        safe_title = title.replace('"', "'")[:80]
        subprocess.Popen([
            "osascript", "-e",
            f'display notification "{safe_body}" with title "{safe_title}" sound name "Ping"'
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def _post_war_room(message: str):
    """Post to TradeMinds war room feed on port 8080."""
    try:
        requests.post(
            f"{BRIDGE}/api/war-room/post",
            json={"message": message, "source": "signal-center"},
            timeout=3,
        )
    except Exception:
        pass


def _update_outcome(signal_id: int, entry_price: float, stop_loss: float, take_profit: float):
    """Initialize outcome tracking row for a new signal."""
    try:
        db = get_db()
        db.execute(
            "INSERT OR IGNORE INTO signal_outcomes "
            "(signal_id, tracked_entry, tracked_high, tracked_low, tracked_current) "
            "VALUES (?, ?, ?, ?, ?)",
            (signal_id, entry_price, entry_price, entry_price, entry_price)
        )
        db.commit()
        db.close()
    except Exception:
        pass


def _outcome_tracker_loop():
    """Background thread: update tracked prices every 15 min during market hours."""
    import sqlite3 as _sq
    _TRADER_DB = "/Users/bigmac/autonomous-trader/data/trader.db"
    while True:
        try:
            _time.sleep(900)  # 15 minutes
            now = datetime.now()
            # Only run 6am-8pm local time on weekdays
            if now.weekday() >= 5 or not (6 <= now.hour < 20):
                continue
            sc_db = _sq.connect(DB_PATH, check_same_thread=False, timeout=10)
            sc_db.row_factory = _sq.Row
            # Get open signals from last 7 days
            signals = sc_db.execute("""
                SELECT ts.id, ts.symbol, ts.entry_price, ts.stop_loss, ts.take_profit
                FROM trade_signals ts
                WHERE ts.created_at >= datetime('now', '-7 days')
                  AND ts.status IN ('NEW', 'EXECUTED')
            """).fetchall()

            # Get current prices for each symbol via yfinance (best-effort)
            syms = list(set(s["symbol"] for s in signals))
            prices = {}
            if syms:
                try:
                    import yfinance as yf
                    tickers = yf.Tickers(" ".join(syms))
                    for sym in syms:
                        try:
                            info = tickers.tickers[sym].fast_info
                            prices[sym] = float(info.last_price or 0)
                        except Exception:
                            pass
                except Exception:
                    pass

            now_str = datetime.now().isoformat()
            for sig in signals:
                cur = prices.get(sig["symbol"], 0)
                if cur <= 0:
                    continue
                entry = sig["entry_price"] or cur
                sl = sig["stop_loss"] or 0
                tp = sig["take_profit"] or 0
                # Fetch existing outcome row
                out = sc_db.execute(
                    "SELECT * FROM signal_outcomes WHERE signal_id=?", (sig["id"],)
                ).fetchone()
                if not out:
                    sc_db.execute(
                        "INSERT INTO signal_outcomes (signal_id, tracked_entry, tracked_high, tracked_low, tracked_current) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (sig["id"], entry, cur, cur, cur)
                    )
                else:
                    new_high = max(out["tracked_high"] or cur, cur)
                    new_low = min(out["tracked_low"] or cur, cur)
                    hit_tp = 1 if (tp > 0 and new_high >= tp) else (out["would_hit_tp"] or 0)
                    hit_sl = 1 if (sl > 0 and new_low <= sl) else (out["would_hit_sl"] or 0)
                    theo_pnl = ((cur - entry) / entry * 100) if entry > 0 else 0
                    sc_db.execute("""
                        UPDATE signal_outcomes
                        SET tracked_high=?, tracked_low=?, tracked_current=?,
                            would_hit_tp=?, would_hit_sl=?, theoretical_pnl=?,
                            last_updated=?
                        WHERE signal_id=?
                    """, (new_high, new_low, cur, hit_tp, hit_sl,
                          round(theo_pnl, 2), now_str, sig["id"]))
            sc_db.commit()
            sc_db.close()
        except Exception as e:
            print(f"[outcome-tracker] error: {e}")


# Start outcome tracker background thread
threading.Thread(target=_outcome_tracker_loop, daemon=True, name="outcome-tracker").start()


# ── Auth / security hooks ────────────────────────────────────────────────────

@app.before_request
def _auth_gate():
    ip   = request.remote_addr or "unknown"
    ua   = (request.headers.get("User-Agent") or "").lower()
    path = request.path

    # Block AI / known bot user-agents (allow localhost)
    if ip not in _LOCALHOST:
        for bad_ua in _SC_BOT_UA:
            if bad_ua in ua:
                _sec_log.warning("SC_BOT_BLOCKED ip=%s ua=%s path=%s", ip, ua[:120], path)
                return make_response("Forbidden", 403)

    # Always allow login/logout + static assets
    if path in ("/login", "/logout", "/robots.txt") or path.startswith("/static/"):
        return None

    # Localhost bypass ONLY for /api/* (main.py posts signals, crew_scanner fetches levels)
    # UI pages (/ and others) always require a session, even from localhost
    if _sc_is_localhost() and path.startswith("/api/"):
        return None

    # Check session
    if not _sc_check_session():
        if path.startswith("/api/"):
            return jsonify({"error": "Authentication required"}), 401
        return redirect("/login")
    return None


@app.after_request
def _security_headers(response):
    response.headers["X-Frame-Options"]        = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-XSS-Protection"]       = "1; mode=block"
    response.headers["Referrer-Policy"]        = "no-referrer"
    response.headers["Content-Security-Policy"] = "default-src 'self' 'unsafe-inline' 'unsafe-eval' https:"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Server"] = "OllieTrades"  # obscure real server identity
    return response


@app.route('/login', methods=['GET', 'POST'])
def sc_login():
    ip = request.remote_addr or "unknown"
    now = _time.time()
    step = request.args.get("step", "")

    # Check if IP is blocked
    failure = _sc_failures.get(ip, {"count": 0, "blocked_until": 0.0})
    if failure["blocked_until"] > now:
        remaining = int(failure["blocked_until"] - now)
        _sec_log.warning("SC_LOGIN_BLOCKED ip=%s remaining=%ds", ip, remaining)
        return make_response(
            _SC_LOGIN_PAGE.format(error=f'<div class="err">⛔ IP blocked for {remaining}s — too many failed attempts</div>'),
            429
        )

    # ── GET requests ─────────────────────────────────────────────────────────
    if request.method == "GET":
        if step == "2" and session.get("totp_pending"):
            return _SC_TOTP_PAGE.format(error="")
        return _SC_LOGIN_PAGE.format(error="")

    # ── Step 2: TOTP verification ─────────────────────────────────────────────
    if step == "2":
        if not session.get("totp_pending"):
            return redirect("/login")
        code = (request.form.get("totp_code") or "").strip()
        if _sc_totp and _sc_totp.verify(code, valid_window=1):
            pending_user = session.get("totp_pending_user", _SC_USER)
            session.pop("totp_pending", None)
            session.pop("totp_pending_user", None)
            session.permanent = True
            session["authenticated"] = True
            session["username"] = pending_user
            _sc_failures.pop(ip, None)
            _sec_log.warning("SC_LOGIN_2FA_OK ip=%s user=%s", ip, pending_user)
            return redirect("/")
        # Invalid code
        failure["count"] = failure.get("count", 0) + 1
        if failure["count"] >= _SC_MAX_ATTEMPTS:
            failure["blocked_until"] = now + _SC_BLOCK_SECS
            failure["count"] = 0
            _sec_log.warning("SC_LOGIN_FAIL_BLOCK ip=%s attempts=%d", ip, _SC_MAX_ATTEMPTS)
        else:
            _sec_log.warning("SC_LOGIN_TOTP_FAIL ip=%s attempt=%d", ip, failure["count"])
        _sc_failures[ip] = failure
        return make_response(_SC_TOTP_PAGE.format(error='<div class="err">⛔ Invalid code — try again</div>'), 401)

    # ── Step 1: username + password ───────────────────────────────────────────
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()

    if username == _SC_USER and password == _SC_PASS:
        # 2FA disabled — authenticate directly
        session.permanent = True
        session["authenticated"] = True
        session["username"] = username
        _sc_failures.pop(ip, None)
        _sec_log.warning("SC_LOGIN_OK ip=%s user=%s", ip, username)
        return redirect("/")

    # Failed login
    failure["count"] = failure.get("count", 0) + 1
    if failure["count"] >= _SC_MAX_ATTEMPTS:
        failure["blocked_until"] = now + _SC_BLOCK_SECS
        failure["count"] = 0
        _sec_log.warning("SC_LOGIN_FAIL_BLOCK ip=%s attempts=%d", ip, _SC_MAX_ATTEMPTS)
    else:
        _sec_log.warning("SC_LOGIN_FAIL ip=%s user=%s attempt=%d", ip, username, failure["count"])
    _sc_failures[ip] = failure
    return make_response(
        _SC_LOGIN_PAGE.format(error='<div class="err">⛔ ACCESS DENIED — Invalid credentials</div>'),
        401
    )


@app.route('/logout')
def sc_logout():
    session.clear()
    return redirect("/login")


@app.route('/robots.txt')
def sc_robots():
    return Response("User-agent: *\nDisallow: /\n", mimetype="text/plain")


# ── Routes ──────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return send_file('index.html')


@app.route('/api/me')
def sc_me():
    """Return the current user's username from the signal center session."""
    username = session.get('username', _SC_USER)
    if _sc_check_session():
        return jsonify({"username": username, "authenticated": True})
    return jsonify({"authenticated": False}), 401


@app.route('/api/active-users')
def sc_active_users():
    """Proxy active-users from port 8080 (where session tracking lives)."""
    import urllib.request as _ur
    try:
        with _ur.urlopen('http://127.0.0.1:8080/api/active-users', timeout=4) as r:
            data = r.read()
        return app.response_class(data, status=200, mimetype='application/json')
    except Exception:
        return jsonify({"count": 1, "users": [session.get('username', _SC_USER)]})

# --- /api/signals/all cache (SWR pattern) ------------------------------------
_signals_cache: dict = {"data": None, "ts": 0.0, "refreshing": False}
_signals_lock = threading.Lock()
_SIGNALS_TTL        = 60    # serve from cache for 60 s
_SIGNALS_SWR_MAX    = 120   # stale-while-revalidate up to 120 s

_SIGNALS_ENDPOINTS = {
    'regime':             '/api/regime',
    'leaderboard':        '/api/arena/leaderboard',
    'vix':                '/api/market/vix',
    'gex':                '/api/gex/SPY',
    'fear_greed':         '/api/fear-greed',
    'breadth':            '/api/breadth',
    'congress':           '/api/congress/trades',
    'metals':             '/api/metals/signals',
    'status':             '/api/status',
    'positions':          '/api/alpaca/positions',
    'options_flow':       '/api/market/options-flow',
    'options_alignment':  '/api/market/options-alignment',
    'bull_bear':          '/api/bull-bear/all?model=all',
    'consensus':          '/api/bridge/consensus',
    'cross_asset':        '/api/cross-asset',
    'high_iv':            '/api/high-iv',
    'convergence':        '/api/navigator/convergence',
    'signal_tracker':     '/api/signal-tracker',
    'gamma_env':          '/api/gamma-environment',
    'ghost_trades':       '/api/ghost-trades',
    'economic':           '/api/macro',
    'volume_radar':       '/api/volume-radar',
    'smart_money':        '/api/smart-money',
    'insider':            '/api/insider-trades',
    'earnings':           '/api/market/earnings',
    'dayblade':           '/api/dayblade/status',
    'analytics':          '/api/arena/analytics',
    'risk_radar':         '/api/risk-radar',
    'market_movers':      '/api/market-movers',
    'fast_scan':          '/api/fast-scan',
    'ema_pullback':       '/api/ema-pullback',
    'gex_overlay':        '/api/gex-overlay/levels?symbol=SPY',
    'flow_lean':          '/api/market/options-flow',
    'critical_alerts':    '/api/volume-radar',
    'red_alert_score':    '/api/red-alert/status',
    'holly_winners':      '/api/holly/winners',
}


def _fetch_all_signals() -> dict:
    """Fetch all bridge endpoints in parallel and persist to history."""
    results: dict = {}
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(_bridge_get, ep): key
                   for key, ep in _SIGNALS_ENDPOINTS.items()}
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                results[key] = fut.result()
            except Exception:
                results[key] = None

    # Persist to history (only non-None results)
    try:
        db  = get_db()
        now = datetime.now().isoformat()
        for key, data in results.items():
            if data is not None:
                db.execute(
                    "INSERT INTO signal_history (timestamp, signal_name, value, raw_data, source) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (now, key, str(data)[:200], json.dumps(data), 'bridge')
                )
        db.commit()
        db.close()
    except Exception as e:
        print(f"[signal-center] History save error: {e}")

    return results


def _bg_refresh_signals():
    """Background thread: fetch fresh signals and update cache."""
    try:
        data = _fetch_all_signals()
        with _signals_lock:
            _signals_cache["data"] = data
            _signals_cache["ts"]   = _time.time()
    except Exception as e:
        print(f"[signal-center] Background refresh error: {e}")
    finally:
        with _signals_lock:
            _signals_cache["refreshing"] = False


@app.route('/api/signals/all')
def all_signals():
    now = _time.time()
    with _signals_lock:
        cached_data  = _signals_cache["data"]
        cached_ts    = _signals_cache["ts"]
        is_refreshing = _signals_cache["refreshing"]

    age = now - cached_ts

    # Fresh cache — return immediately
    if cached_data is not None and age < _SIGNALS_TTL:
        return jsonify(cached_data)

    # Stale but usable — return stale data and kick off background refresh
    if cached_data is not None and age < _SIGNALS_SWR_MAX:
        if not is_refreshing:
            with _signals_lock:
                _signals_cache["refreshing"] = True
            threading.Thread(target=_bg_refresh_signals, daemon=True).start()
        return jsonify(cached_data)

    # Cache empty or too stale — block once to build it
    data = _fetch_all_signals()
    with _signals_lock:
        _signals_cache["data"] = data
        _signals_cache["ts"]   = _time.time()
        _signals_cache["refreshing"] = False
    return jsonify(data)

@app.route('/api/signals/history')
def signal_history():
    signal_name = request.args.get('signal', '')
    days  = int(request.args.get('days', 7))
    limit = int(request.args.get('limit', 500))
    db    = get_db()
    since = (datetime.now() - timedelta(days=days)).isoformat()
    if signal_name:
        rows = db.execute(
            "SELECT id, timestamp, signal_name, value, score, grade, source "
            "FROM signal_history WHERE signal_name=? AND timestamp>? "
            "ORDER BY timestamp DESC LIMIT ?",
            (signal_name, since, limit)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, timestamp, signal_name, value, score, grade, source "
            "FROM signal_history WHERE timestamp>? ORDER BY timestamp DESC LIMIT ?",
            (since, limit)
        ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/signals/snapshot')
def daily_snapshots():
    days = int(request.args.get('days', 30))
    db   = get_db()
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    rows = db.execute(
        "SELECT * FROM daily_snapshot WHERE date>? ORDER BY date DESC", (since,)
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/export/<fmt>')
def export_data(fmt):
    days        = int(request.args.get('days', 30))
    signal_name = request.args.get('signal', '')
    db          = get_db()
    since       = (datetime.now() - timedelta(days=days)).isoformat()
    if signal_name:
        rows = db.execute(
            "SELECT * FROM signal_history WHERE signal_name=? AND timestamp>? ORDER BY timestamp",
            (signal_name, since)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM signal_history WHERE timestamp>? ORDER BY timestamp", (since,)
        ).fetchall()
    data = [dict(r) for r in rows]
    db.close()

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    if fmt == 'json':
        fname = f"signals_{ts}.json"
        fpath = os.path.join(EXPORTS_DIR, fname)
        with open(fpath, 'w') as f:
            json.dump(data, f, indent=2)
        return send_from_directory(EXPORTS_DIR, fname, as_attachment=True)
    if fmt == 'csv':
        import csv
        fname = f"signals_{ts}.csv"
        fpath = os.path.join(EXPORTS_DIR, fname)
        with open(fpath, 'w', newline='') as f:
            if data:
                w = csv.DictWriter(f, fieldnames=data[0].keys())
                w.writeheader()
                w.writerows(data)
        return send_from_directory(EXPORTS_DIR, fname, as_attachment=True)
    return jsonify({"error": "Use /api/export/csv or /api/export/json"}), 400

@app.route('/api/import', methods=['POST'])
def import_data():
    try:
        data = request.get_json()
        if not isinstance(data, list):
            return jsonify({"error": "Expected JSON array"}), 400
        db = get_db()
        for row in data:
            db.execute(
                "INSERT INTO signal_history (timestamp, signal_name, value, score, grade, raw_data, source) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (row.get('timestamp', datetime.now().isoformat()),
                 row.get('signal_name', ''),
                 row.get('value', ''),
                 row.get('score'),
                 row.get('grade'),
                 row.get('raw_data', ''),
                 row.get('source', 'import'))
            )
        db.commit()
        db.close()
        return jsonify({"imported": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stats')
def stats():
    db = get_db()
    total     = db.execute("SELECT COUNT(*) FROM signal_history").fetchone()[0]
    unique    = db.execute("SELECT COUNT(DISTINCT signal_name) FROM signal_history").fetchone()[0]
    oldest    = db.execute("SELECT MIN(timestamp) FROM signal_history").fetchone()[0]
    newest    = db.execute("SELECT MAX(timestamp) FROM signal_history").fetchone()[0]
    snapshots = db.execute("SELECT COUNT(*) FROM daily_snapshot").fetchone()[0]
    db.close()
    return jsonify({
        "total_records":   total,
        "unique_signals":  unique,
        "oldest_record":   oldest,
        "newest_record":   newest,
        "daily_snapshots": snapshots,
        "db_path":         DB_PATH,
    })

def _compute_trade_levels(symbol):
    """Compute trade levels for symbol and return a plain dict (not a Response).
    Caller is responsible for caching.  Raises on error."""
    bars    = _bridge_get(f'/api/charts/ohlcv?symbol={symbol}&timeframe=1D&limit=60', timeout=8)
    gex     = _bridge_get(f'/api/gex-overlay/levels?symbol={symbol}', timeout=5)
    regime  = _bridge_get('/api/regime', timeout=5)

    if not bars or not isinstance(bars, list) or len(bars) < 5:
        raise ValueError("Insufficient price data")

    from collections import defaultdict
    day_buckets = defaultdict(list)
    for b in bars:
        t = b.get('time', 0)
        day_key = int(t) // 86400
        day_buckets[day_key].append(b)

    daily = []
    for day_key in sorted(day_buckets.keys()):
        day_bars = day_buckets[day_key]
        opens  = [float(b.get('open',  b.get('o', 0))) for b in day_bars]
        highs  = [float(b.get('high',  b.get('h', 0))) for b in day_bars]
        lows   = [float(b.get('low',   b.get('l', 0))) for b in day_bars]
        closes = [float(b.get('close', b.get('c', 0))) for b in day_bars]
        daily.append({
            'open':  opens[0],
            'high':  max(highs),
            'low':   min(lows),
            'close': closes[-1],
        })

    if len(daily) < 2:
        raise ValueError("Need at least 2 days of data")

    latest = daily[-1]
    price  = float(latest['close'])
    if not price:
        raise ValueError("No price data")

    atr_vals = []
    for i in range(1, min(15, len(daily))):
        c  = daily[-(i)]
        p  = daily[-(i+1)]
        h, l, pc = c['high'], c['low'], p['close']
        if h and l and pc:
            atr_vals.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(atr_vals) / len(atr_vals) if atr_vals else price * 0.015

    recent = daily[-15:] if len(daily) >= 15 else daily
    highs_list = sorted([c['high'] for c in recent], reverse=True)
    lows_list  = sorted([c['low']  for c in recent])
    resistance = highs_list[1] if len(highs_list) > 1 else price * 1.03
    support    = lows_list[1]  if len(lows_list)  > 1 else price * 0.97
    if support    >= price: support    = price - atr * 1.5
    if resistance <= price: resistance = price + atr * 1.5

    gex_flip = put_wall = call_wall = None
    if gex and not gex.get('error'):
        gex_flip  = gex.get('gamma_flip')
        put_wall  = gex.get('put_wall')
        call_wall = gex.get('call_wall')

    regime_label = ''
    if regime:
        regime_label = (regime.get('regime') or regime.get('label') or '').upper()
    if   'BULL'     in regime_label: risk_mult = 1.2
    elif 'CAUTIOUS' in regime_label: risk_mult = 0.85
    elif 'BEAR'     in regime_label: risk_mult = 0.65
    elif 'CRISIS'   in regime_label: risk_mult = 0.4
    else:                             risk_mult = 1.0

    def r2(v): return round(v, 2)

    l_entry_lo = r2(price - atr * 0.25)
    l_entry_hi = r2(price + atr * 0.10)
    l_sl       = r2(max(support - atr * 0.3, price - atr * 1.5))
    l_risk     = r2(price - l_sl)
    l_tp1      = r2(price + l_risk * 0.75 * risk_mult)
    l_tp2      = r2(price + l_risk * 2.0 * risk_mult)
    l_tp3      = r2(min(call_wall or price * 1.1, price + l_risk * 3.0 * risk_mult))
    l_rr       = round(l_risk * 2 * risk_mult / l_risk, 1) if l_risk > 0 else 0

    s_entry_lo = r2(price - atr * 0.10)
    s_entry_hi = r2(price + atr * 0.25)
    s_sl       = r2(min(resistance + atr * 0.3, price + atr * 1.5))
    s_risk     = r2(s_sl - price)
    s_tp1      = r2(price - s_risk * 0.75 * risk_mult)
    s_tp2      = r2(price - s_risk * 2.0 * risk_mult)
    s_tp3      = r2(max(put_wall or price * 0.9, price - s_risk * 3.0 * risk_mult))
    s_rr       = round(s_risk * 2 * risk_mult / s_risk, 1) if s_risk > 0 else 0

    rec = ('LONG'  if 'BULL'   in regime_label else
           'SHORT' if 'BEAR'   in regime_label or 'CRISIS' in regime_label else
           'NEUTRAL')
    size_pct = int(risk_mult * 100)

    return {
        "symbol":       symbol,
        "price":        r2(price),
        "atr":          r2(atr),
        "atr_pct":      round(atr / price * 100, 2),
        "regime":       regime_label or 'UNKNOWN',
        "risk_mult":    risk_mult,
        "recommendation": rec,
        "sizing":       f"{size_pct}% position size ({regime_label or 'NEUTRAL'})",
        "support":      r2(support),
        "resistance":   r2(resistance),
        "gex": {
            "gamma_flip": gex_flip,
            "put_wall":   put_wall,
            "call_wall":  call_wall,
        },
        "long": {
            "entry_lo": l_entry_lo,
            "entry_hi": l_entry_hi,
            "stop_loss": l_sl,
            "risk":     l_risk,
            "tp1":      l_tp1,
            "tp2":      l_tp2,
            "tp3":      l_tp3,
            "rr":       l_rr,
            "sl_pct":   round((price - l_sl) / price * 100, 2),
            "tp2_pct":  round((l_tp2 - price) / price * 100, 2),
        },
        "short": {
            "entry_lo": s_entry_lo,
            "entry_hi": s_entry_hi,
            "stop_loss": s_sl,
            "risk":     s_risk,
            "tp1":      s_tp1,
            "tp2":      s_tp2,
            "tp3":      s_tp3,
            "rr":       s_rr,
            "sl_pct":   round((s_sl - price) / price * 100, 2),
            "tp2_pct":  round((price - s_tp2) / price * 100, 2),
        },
    }


def _get_trade_levels_cached(symbol):
    """Return cached levels dict or compute fresh. Never raises — returns None on failure."""
    symbol = symbol.upper()
    now = _time.time()
    with _tl_lock:
        entry = _tl_cache.get(symbol)
        if entry and (now - entry["ts"]) < _TL_TTL:
            return entry["data"]
    try:
        data = _compute_trade_levels(symbol)
        with _tl_lock:
            _tl_cache[symbol] = {"ts": now, "data": data}
        return data
    except Exception as e:
        print(f"[trade-levels] {symbol}: {e}")
        return None


@app.route('/api/trade-levels/<symbol>')
def trade_levels(symbol):
    """Entry range, stop loss, and take profit levels for a symbol (cached 15 min)."""
    symbol = symbol.upper()
    data = _get_trade_levels_cached(symbol)
    if data is None:
        return jsonify({"error": "Insufficient price data", "symbol": symbol}), 404
    return jsonify(data)


@app.route('/api/trade-levels/bulk')
def trade_levels_bulk():
    """Return trade levels for multiple symbols at once.
    Query param: ?symbols=AMD,NVDA,MSFT  (comma-separated, max 40)
    Returns: {"AMD": {...}, "NVDA": {...}, "errors": ["GLW"]}
    All symbols served from cache if warm; stale entries recomputed inline.
    """
    raw = request.args.get("symbols", "")
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()][:40]
    if not symbols:
        return jsonify({"error": "symbols param required"}), 400

    results = {}
    errors  = []
    for sym in symbols:
        data = _get_trade_levels_cached(sym)
        if data:
            results[sym] = data
        else:
            errors.append(sym)

    return jsonify({"levels": results, "errors": errors,
                    "cached": len(results), "failed": len(errors)})


def _warm_watchlist_levels():
    """Background thread: pre-compute trade levels for the daily watchlist every 15 min."""
    # Initial delay — let port 8080 finish startup
    _time.sleep(30)
    while True:
        try:
            now = datetime.now()
            # Only warm during weekday market hours (6 AM – 8 PM local)
            if now.weekday() < 5 and 6 <= now.hour < 20:
                # Read today's watchlist; fall back to fixed list
                symbols = list(FIXED_WATCHLIST)
                try:
                    wl_path = os.path.normpath(DAILY_WATCHLIST_PATH)
                    if os.path.exists(wl_path):
                        with open(wl_path) as f:
                            wl = json.load(f)
                        today = datetime.now().strftime("%Y-%m-%d")
                        if wl.get("scan_date") == today:
                            symbols = wl.get("symbols", symbols)
                except Exception:
                    pass

                warmed = failed = 0
                for sym in symbols:
                    try:
                        _get_trade_levels_cached(sym)
                        warmed += 1
                    except Exception:
                        failed += 1
                print(f"[trade-levels-warm] {now.strftime('%H:%M')} — warmed {warmed}/{len(symbols)} symbols ({failed} failed)")
        except Exception as e:
            print(f"[trade-levels-warm] error: {e}")
        _time.sleep(_TL_TTL)   # re-warm every 15 min


threading.Thread(target=_warm_watchlist_levels, daemon=True, name="tl-warm").start()


# ── Prediction Tracker ────────────────────────────────────────────────────────

def _fetch_top25():
    """Fetch Top 25 stocks from fast-scan with current prices and trade levels."""
    scan = _bridge_get('/api/fast-scan')
    regime_data = _bridge_get('/api/regime')
    regime = ''
    if regime_data:
        regime = (regime_data.get('regime') or regime_data.get('label') or '').upper()

    # fast-scan returns {results: [...], count: N} sorted by score desc
    raw = []
    if scan and isinstance(scan, dict):
        raw = scan.get('results') or []
    elif scan and isinstance(scan, list):
        raw = scan
    # Sort by score descending, take top 25
    raw = sorted(raw, key=lambda x: x.get('score', 0), reverse=True)[:25]

    results = []
    for item in raw:
        sym = (item.get('ticker') or item.get('symbol') or '').upper()
        if not sym:
            continue
        score = item.get('score') or 0
        price = item.get('price') or 0
        # Get trade levels from our own endpoint
        levels = {}
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:9000/api/trade-levels/{sym}", timeout=8
            ) as r:
                levels = json.loads(r.read())
        except Exception:
            pass
        if levels.get('error'):
            levels = {}
        price = levels.get('price') or price
        long_d = levels.get('long') or {}
        results.append({
            'symbol':         sym,
            'price_at':       price,
            'master_score':   score,
            'regime':         regime,
            'recommendation': levels.get('recommendation', ''),
            'tp1':            long_d.get('tp1'),
            'tp2':            long_d.get('tp2'),
            'stop_loss':      long_d.get('stop_loss'),
            'rr':             long_d.get('rr'),
            'signal_json':    json.dumps(item),
        })
    return results


@app.route('/api/predictions/snapshot', methods=['POST', 'GET'])
@app.route('/api/predictions/auto-snapshot', methods=['POST', 'GET'])
def predictions_snapshot():
    """Snapshot today's Top 25 with trade projections. INSERT only."""
    today = datetime.now().strftime('%Y-%m-%d')
    db = get_db()
    # Check if already snapped today
    existing = db.execute(
        "SELECT COUNT(*) FROM predictions WHERE snap_date=?", (today,)
    ).fetchone()[0]
    if existing > 0:
        db.close()
        return jsonify({"status": "already_snapped", "date": today, "count": existing})

    rows = _fetch_top25()
    if not rows:
        db.close()
        return jsonify({"error": "Could not fetch Top 25", "date": today}), 500

    now = datetime.now().isoformat()
    for r in rows:
        db.execute(
            "INSERT INTO predictions "
            "(snap_date, symbol, price_at, master_score, regime, recommendation, "
            " tp1, tp2, stop_loss, rr, signal_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (today, r['symbol'], r['price_at'], r['master_score'], r['regime'],
             r['recommendation'], r['tp1'], r['tp2'], r['stop_loss'], r['rr'],
             r['signal_json'], now)
        )
    db.commit()
    db.close()
    return jsonify({"status": "snapped", "date": today, "count": len(rows),
                    "tickers": [r['symbol'] for r in rows]})


def _get_ohlcv_range(symbol, days_back=5):
    """Fetch OHLCV and return (current_price, high_since, low_since) over days_back days."""
    bars = _bridge_get(
        f'/api/charts/ohlcv?symbol={symbol}&timeframe=1D&limit={max(days_back * 8, 60)}',
        timeout=10
    )
    if not bars or not isinstance(bars, list):
        return 0, 0, 0
    from collections import defaultdict
    day_buckets = defaultdict(list)
    for b in bars:
        day_key = int(b.get('time', 0)) // 86400
        day_buckets[day_key].append(b)
    daily = []
    for dk in sorted(day_buckets.keys()):
        db_bars = day_buckets[dk]
        highs  = [float(b.get('high',  b.get('h', 0))) for b in db_bars]
        lows   = [float(b.get('low',   b.get('l', 0))) for b in db_bars]
        closes = [float(b.get('close', b.get('c', 0))) for b in db_bars]
        daily.append({'high': max(highs), 'low': min(lows), 'close': closes[-1]})
    if not daily:
        return 0, 0, 0
    # Last N days
    recent = daily[-max(days_back, 1):]
    price_now  = daily[-1]['close']
    high_since = max(d['high']  for d in recent)
    low_since  = min(d['low']   for d in recent)
    return price_now, high_since, low_since


@app.route('/api/predictions/check')
def predictions_check():
    """Compare past predictions vs actual prices, using OHLCV high/low to detect TP/SL touches.
    INSERT only — safe to call multiple times per day."""
    days = int(request.args.get('days', 1))
    check_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    db = get_db()
    preds = db.execute(
        "SELECT * FROM predictions WHERE snap_date=?", (check_date,)
    ).fetchall()
    if not preds:
        # Show available dates
        avail = db.execute(
            "SELECT DISTINCT snap_date FROM predictions ORDER BY snap_date DESC LIMIT 5"
        ).fetchall()
        db.close()
        return jsonify({
            "error": f"No predictions for {check_date}",
            "available": [r['snap_date'] for r in avail],
        }), 404

    results = []
    now_str = datetime.now().isoformat()
    best  = {"symbol": "", "pct": -999}
    worst = {"symbol": "", "pct":  999}
    tp1_hits = tp2_hits = sl_hits = wins = 0

    for p in preds:
        sym        = p['symbol']
        price_then = p['price_at'] or 0
        tp1 = p['tp1']  or 0
        tp2 = p['tp2']  or 0
        sl  = p['stop_loss'] or 0
        rec = (p['recommendation'] or '').upper()

        if not price_then:
            continue

        # Use OHLCV to find high/low since prediction — catches intra-period TP/SL touches
        price_now, high_since, low_since = _get_ohlcv_range(sym, days_back=max(days, 1))
        if not price_now:
            continue

        pct = round((price_now - price_then) / price_then * 100, 2)

        # TP/SL: check against high/low (more accurate than just close)
        hit_tp1 = 1 if tp1 and high_since >= tp1 else 0
        hit_tp2 = 1 if tp2 and high_since >= tp2 else 0
        hit_sl  = 1 if sl  and low_since  <= sl  else 0
        if hit_tp1: tp1_hits += 1
        if hit_tp2: tp2_hits += 1
        if hit_sl:  sl_hits  += 1

        # "Correct" = price moved in the predicted direction
        correct = 0
        if   rec == 'LONG'    and pct > 0:       correct = 1
        elif rec == 'SHORT'   and pct < 0:       correct = 1
        elif rec == 'NEUTRAL' and abs(pct) < 1:  correct = 1
        if correct: wins += 1

        outcome = ("WIN"  if correct else
                   "STOP" if hit_sl  else
                   "FLAT" if abs(pct) < 0.5 else "LOSS")

        if pct > best['pct']:  best  = {"symbol": sym, "pct": pct}
        if pct < worst['pct']: worst = {"symbol": sym, "pct": pct}

        # INSERT only if not already checked today
        already = db.execute(
            "SELECT id FROM prediction_results WHERE snap_date=? AND symbol=?",
            (check_date, sym)
        ).fetchone()
        if not already:
            db.execute(
                "INSERT INTO prediction_results "
                "(prediction_id, snap_date, symbol, price_at, price_next, pct_change, "
                " hit_tp1, hit_tp2, hit_sl, correct, checked_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (p['id'], check_date, sym, price_then, price_now, pct,
                 hit_tp1, hit_tp2, hit_sl, correct, now_str)
            )
        results.append({
            'symbol':     sym,
            'price_then': price_then,
            'price_now':  round(price_now, 2),
            'high_since': round(high_since, 2),
            'low_since':  round(low_since, 2),
            'pct_change': pct,
            'hit_tp1':    hit_tp1,
            'hit_tp2':    hit_tp2,
            'hit_sl':     hit_sl,
            'correct':    correct,
            'outcome':    outcome,
            'rec':        rec,
        })

    db.commit()
    total   = len(results)
    pct_acc = round(wins / total * 100, 1) if total else 0
    avg_ret = round(sum(r['pct_change'] for r in results) / total, 2) if total else 0

    # Accuracy summary — INSERT only
    if total:
        already_acc = db.execute(
            "SELECT id FROM prediction_accuracy WHERE period_date=?", (check_date,)
        ).fetchone()
        if not already_acc:
            db.execute(
                "INSERT INTO prediction_accuracy "
                "(period_date, total, correct, hit_tp1, hit_tp2, hit_sl, accuracy_pct, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (check_date, total, wins, tp1_hits, tp2_hits, sl_hits, pct_acc, now_str)
            )
            db.commit()
    db.close()

    # Sort results by pct_change desc for readability
    results.sort(key=lambda x: x['pct_change'], reverse=True)
    return jsonify({
        "check_date":    check_date,
        "days_later":    days,
        "total":         total,
        "wins":          wins,
        "accuracy_pct":  pct_acc,
        "avg_return_pct": avg_ret,
        "tp1_hit_rate":  round(tp1_hits / total * 100, 1) if total else 0,
        "tp2_hit_rate":  round(tp2_hits / total * 100, 1) if total else 0,
        "sl_hit_rate":   round(sl_hits  / total * 100, 1) if total else 0,
        "best":          best,
        "worst":         worst,
        "accuracy":      f"{wins}/{total}",
        "results":       results,
    })


@app.route('/api/predictions/history')
def predictions_history():
    """Accuracy over time from prediction_accuracy table."""
    days  = int(request.args.get('days', 30))
    db    = get_db()
    since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    rows  = db.execute(
        "SELECT period_date, total, correct, hit_tp1, hit_tp2, hit_sl, accuracy_pct "
        "FROM prediction_accuracy WHERE period_date >= ? ORDER BY period_date DESC",
        (since,)
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/predictions/leaderboard')
def predictions_leaderboard():
    """Best and worst predicted tickers over all time."""
    db  = get_db()
    rows = db.execute("""
        SELECT symbol,
               COUNT(*) as total,
               SUM(correct) as correct,
               SUM(hit_tp1) as tp1_hits,
               SUM(hit_tp2) as tp2_hits,
               SUM(hit_sl) as sl_hits,
               ROUND(AVG(pct_change), 2) as avg_pct,
               ROUND(CAST(SUM(correct) AS REAL) / COUNT(*) * 100, 1) as win_rate
        FROM prediction_results
        GROUP BY symbol
        HAVING total >= 1
        ORDER BY win_rate DESC, tp2_hits DESC
    """).fetchall()
    db.close()
    data = [dict(r) for r in rows]
    return jsonify({"leaderboard": data[:10], "laggards": list(reversed(data))[:5]})


@app.route('/api/predictions/analysis/<symbol>')
def predictions_analysis(symbol):
    """Deep WHY analysis — why was this pick made, why was it right/wrong, lessons."""
    symbol = symbol.upper()
    db = get_db()
    preds = db.execute(
        "SELECT * FROM predictions WHERE symbol=? ORDER BY snap_date DESC LIMIT 20",
        (symbol,)
    ).fetchall()
    results = db.execute(
        "SELECT * FROM prediction_results WHERE symbol=? ORDER BY snap_date DESC LIMIT 20",
        (symbol,)
    ).fetchall()

    # Current trade levels
    levels = None
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:9000/api/trade-levels/{symbol}", timeout=8
        ) as r:
            levels = json.loads(r.read())
    except Exception:
        pass

    # ── Current WHY ──────────────────────────────────────────────────────────
    why_now = []
    if levels and not levels.get('error'):
        regime   = levels.get('regime', 'UNKNOWN')
        rec      = levels.get('recommendation', 'NEUTRAL')
        atr      = levels.get('atr', 0)
        atr_pct  = levels.get('atr_pct', 0)
        long_d   = levels.get('long') or {}
        gex      = levels.get('gex') or {}
        price    = levels.get('price', 0)
        rr       = long_d.get('rr', 0)

        why_now.append(f"Regime: {regime} → {rec} bias")
        why_now.append(f"ATR ${atr:.2f} ({atr_pct:.1f}% daily range) — "
                       + ("high vol, size down" if atr_pct > 4 else
                          "elevated" if atr_pct > 2.5 else "calm"))
        if gex.get('gamma_flip'):
            flip = gex['gamma_flip']
            side = 'above' if price > flip else 'below'
            why_now.append(
                f"GEX flip ${flip:.2f} — price {side} → "
                + ("dealers long gamma, moves dampened" if side == 'above'
                   else "dealers short gamma, moves amplified")
            )
        if rr:
            why_now.append(f"Long R/R = {rr}:1 — "
                           + ("excellent" if rr >= 2.5 else
                              "acceptable" if rr >= 1.5 else "borderline"))
        if gex.get('call_wall'):
            why_now.append(f"Call wall ${gex['call_wall']:.2f} = resistance ceiling")
        if gex.get('put_wall'):
            why_now.append(f"Put wall ${gex['put_wall']:.2f} = support floor")

    # ── Per-prediction WHY picked / WHY result ────────────────────────────────
    pred_analysis = []
    for pred in preds:
        pred = dict(pred)
        result_row = next(
            (dict(r) for r in results if r['snap_date'] == pred['snap_date']),
            None
        )
        signals_str = (pred.get('signal_json') or '')
        try:
            sig_data = json.loads(signals_str) if signals_str else {}
        except Exception:
            sig_data = {}
        signals_text = ' '.join(str(sig_data.get('signals', ''))).upper()

        why_picked = []
        if 'VOLUME' in signals_text or 'VOL' in signals_text:
            why_picked.append("Volume surge — institutional interest")
        if 'UPTREND' in signals_text:
            why_picked.append("Price in uptrend — momentum")
        if 'BREAKOUT' in signals_text:
            why_picked.append("Breakout above resistance")
        if 'MACD' in signals_text:
            why_picked.append("MACD crossover signal")
        if 'GAP' in signals_text:
            why_picked.append("Gap up — strong opening momentum")
        if 'RSI' in signals_text and 'OVERSOLD' in signals_text:
            why_picked.append("RSI oversold — bounce candidate")
        score = pred.get('master_score') or pred.get('score') or 0
        if score >= 80:
            why_picked.append(f"Score {score} (A-grade) — very high confidence")
        elif score >= 65:
            why_picked.append(f"Score {score} (B-grade) — above average confidence")
        if not why_picked:
            why_picked.append(f"Score {score}, regime {pred.get('regime','?')}")

        why_result = []
        if result_row:
            outcome = result_row.get('outcome') or ('WIN' if result_row.get('correct') else 'LOSS')
            pct = result_row.get('pct_change') or 0
            if result_row.get('hit_tp2'):
                why_result.append("Hit TP2 — thesis fully confirmed, strong follow-through")
            elif result_row.get('hit_tp1'):
                why_result.append("Hit TP1 — initial target reached, thesis confirmed")
            elif result_row.get('hit_sl'):
                why_result.append("Hit stop loss — thesis invalidated")
                regime = pred.get('regime', '')
                rec    = pred.get('recommendation', '')
                if 'BEAR' in regime.upper() and rec == 'LONG':
                    why_result.append("Regime mismatch: LONG taken in BEAR regime")
                if score < 60:
                    why_result.append(f"Low score ({score}) — should have been skipped")
            else:
                why_result.append(f"Price moved {pct:+.1f}% — "
                                   + ("in predicted direction" if (result_row.get('correct')) else "against prediction"))

        pred_analysis.append({
            "date":        pred.get('snap_date'),
            "score":       score,
            "regime":      pred.get('regime'),
            "rec":         pred.get('recommendation'),
            "price_pred":  pred.get('price_at'),
            "tp1":         pred.get('tp1'),
            "tp2":         pred.get('tp2'),
            "sl":          pred.get('stop_loss'),
            "why_picked":  why_picked,
            "why_result":  why_result,
            "result":      result_row,
        })

    # ── Grade accuracy breakdown ──────────────────────────────────────────────
    grade_acc = {}
    for r in results:
        snap = next((dict(p) for p in preds if p['snap_date'] == r['snap_date']), {})
        grade = snap.get('master_grade') or score_to_grade(snap.get('master_score') or snap.get('score') or 50)
        if grade not in grade_acc:
            grade_acc[grade] = {'total': 0, 'wins': 0}
        grade_acc[grade]['total'] += 1
        if r['correct']:
            grade_acc[grade]['wins'] += 1
    for g in grade_acc:
        t = grade_acc[g]['total']
        grade_acc[g]['accuracy'] = round(grade_acc[g]['wins'] / t * 100, 1) if t else 0

    # ── Lessons ───────────────────────────────────────────────────────────────
    total_r   = len(results)
    wins_r    = sum(r['correct'] for r in results)
    tp1_hits  = sum(r['hit_tp1'] for r in results)
    tp2_hits  = sum(r['hit_tp2'] for r in results)
    lessons = []
    if total_r:
        wr = round(wins_r / total_r * 100, 1)
        if wr >= 65:
            lessons.append(f"System has edge on {symbol} ({wr}% win rate) — keep it on watchlist")
        elif wr <= 40:
            lessons.append(f"System struggles with {symbol} ({wr}%) — reduce reliance or remove")
        if tp1_hits / total_r >= 0.5:
            lessons.append(f"TP1 hit {round(tp1_hits/total_r*100)}% of the time — signals are directionally accurate")
        if tp2_hits / total_r >= 0.3:
            lessons.append(f"TP2 hit {round(tp2_hits/total_r*100)}% of the time — strong follow-through stock")
        a_grade = grade_acc.get('A', {})
        if a_grade.get('accuracy', 0) >= 70:
            lessons.append("A-grade predictions are reliable — trust scores ≥ 80")

    db.close()
    return jsonify({
        "symbol":        symbol,
        "why_now":       why_now,
        "levels":        levels,
        "grade_accuracy": grade_acc,
        "total_predictions": total_r,
        "wins":          wins_r,
        "accuracy_pct":  round(wins_r / total_r * 100, 1) if total_r else 0,
        "tp1_hits":      tp1_hits,
        "tp2_hits":      tp2_hits,
        "lessons":       lessons,
        "predictions":   pred_analysis,
    })


@app.route('/api/predictions/top5')
def predictions_top5():
    """Top 5 most consistently accurate predictions with WHY analysis."""
    db   = get_db()
    rows = db.execute("""
        SELECT pr.symbol,
               COUNT(*)                                                          as total,
               SUM(pr.correct)                                                   as wins,
               ROUND(CAST(SUM(pr.correct) AS REAL) / COUNT(*) * 100, 1)         as win_rate,
               ROUND(AVG(pr.pct_change), 2)                                      as avg_return,
               SUM(pr.hit_tp1)                                                   as tp1_hits,
               SUM(pr.hit_tp2)                                                   as tp2_hits,
               ROUND(AVG(p.master_score), 0)                                     as avg_score,
               GROUP_CONCAT(DISTINCT p.signal_json)                              as all_signals
        FROM prediction_results pr
        JOIN predictions p ON p.id = pr.prediction_id
        GROUP BY pr.symbol
        HAVING total >= 2
        ORDER BY win_rate DESC, avg_return DESC
        LIMIT 5
    """).fetchall()

    top5 = []
    for r in rows:
        r = dict(r)
        why = []
        raw_sigs = (r.get('all_signals') or '').upper()
        if 'VOLUME' in raw_sigs:
            why.append("Volume signals consistently predict moves")
        if 'UPTREND' in raw_sigs or 'BREAKOUT' in raw_sigs:
            why.append("Trend/breakout signals reliable for this stock")
        avg_score = r.get('avg_score') or 0
        if avg_score >= 70:
            why.append(f"High avg score ({int(avg_score)}) = strong signal confidence")
        total = r.get('total') or 1
        if (r.get('tp2_hits') or 0) / total >= 0.3:
            why.append("Frequently hits TP2 — strong follow-through")
        if not why:
            why.append(f"{r['win_rate']}% win rate over {total} predictions")
        r['why_accurate'] = why
        r.pop('all_signals', None)  # don't send raw blob
        top5.append(r)

    db.close()
    return jsonify(top5)


@app.route('/api/quant-signals')
def quant_signals():
    """Quant fund-style signals: momentum, mean reversion, cross-asset, smart money."""
    # Gather raw data from Bridge
    regime    = _bridge_get('/api/regime')
    breadth   = _bridge_get('/api/breadth')
    gex       = _bridge_get('/api/gex/SPY')
    cross     = _bridge_get('/api/cross-asset')
    smart     = _bridge_get('/api/smart-money')
    insider   = _bridge_get('/api/insider-trades')
    congress  = _bridge_get('/api/congress/trades')
    vix       = _bridge_get('/api/market/vix')
    movers    = _bridge_get('/api/market-movers')

    signals = {}

    # ── Momentum ─────────────────────────────────────────────────────────────
    mom_score = 50
    mom_why   = []
    regime_label = ''
    if regime:
        regime_label = (regime.get('regime') or regime.get('label') or '').upper()
        if 'BULL' in regime_label:
            mom_score += 20; mom_why.append("Bullish regime (+20)")
        elif 'BEAR' in regime_label or 'CRISIS' in regime_label:
            mom_score -= 20; mom_why.append("Bearish regime (-20)")
    if breadth:
        adv = breadth.get('advancing', 0) or 0
        dec = breadth.get('declining', 0) or 0
        if adv + dec > 0:
            pct = adv / (adv + dec)
            if pct > 0.6: mom_score += 10; mom_why.append(f"Breadth positive {pct:.0%} advancing")
            elif pct < 0.4: mom_score -= 10; mom_why.append(f"Breadth negative {pct:.0%} advancing")
    if vix:
        vix_val = vix.get('vix') or vix.get('value') or 0
        if isinstance(vix_val, (int, float)):
            if vix_val < 15: mom_score += 10; mom_why.append(f"VIX {vix_val:.1f} low (risk-on)")
            elif vix_val > 25: mom_score -= 15; mom_why.append(f"VIX {vix_val:.1f} elevated (risk-off)")
    signals['momentum'] = {
        'score': min(100, max(0, mom_score)),
        'grade': score_to_grade(mom_score),
        'why':   mom_why,
        'label': 'Trend Momentum',
    }

    # ── Mean Reversion ───────────────────────────────────────────────────────
    mr_score = 50
    mr_why   = []
    if gex:
        gamma_flip = gex.get('gamma_flip') or gex.get('flip_level') or 0
        put_wall   = gex.get('put_wall') or 0
        call_wall  = gex.get('call_wall') or 0
        if put_wall and call_wall and gamma_flip:
            range_pct = (call_wall - put_wall) / gamma_flip * 100 if gamma_flip else 0
            if range_pct < 3: mr_score += 15; mr_why.append(f"GEX walls tight ({range_pct:.1f}%) → pinning")
            elif range_pct > 8: mr_score -= 10; mr_why.append(f"GEX walls wide ({range_pct:.1f}%) → trending")
    signals['mean_reversion'] = {
        'score': min(100, max(0, mr_score)),
        'grade': score_to_grade(mr_score),
        'why':   mr_why,
        'label': 'Mean Reversion',
    }

    # ── Cross-Asset ──────────────────────────────────────────────────────────
    ca_score = 50
    ca_why   = []
    if cross:
        if isinstance(cross, dict):
            alignment = cross.get('alignment') or cross.get('score') or ''
            if isinstance(alignment, str):
                if 'BULL' in alignment.upper(): ca_score += 15; ca_why.append(f"Cross-asset alignment: {alignment}")
                elif 'BEAR' in alignment.upper(): ca_score -= 15; ca_why.append(f"Cross-asset alignment: {alignment}")
            elif isinstance(alignment, (int, float)):
                ca_score = int(alignment)
                ca_why.append(f"Cross-asset score: {alignment}")
    signals['cross_asset'] = {
        'score': min(100, max(0, ca_score)),
        'grade': score_to_grade(ca_score),
        'why':   ca_why,
        'label': 'Cross-Asset Flow',
    }

    # ── Smart Money ──────────────────────────────────────────────────────────
    sm_score = 50
    sm_why   = []
    if smart and isinstance(smart, dict):
        bias = (smart.get('bias') or smart.get('signal') or '').upper()
        if 'BULL' in bias: sm_score += 20; sm_why.append(f"Smart money: {bias}")
        elif 'BEAR' in bias: sm_score -= 20; sm_why.append(f"Smart money: {bias}")
        flow = smart.get('net_flow') or smart.get('flow') or 0
        if isinstance(flow, (int, float)) and flow:
            sm_why.append(f"Net flow: ${flow:+,.0f}")
    signals['smart_money'] = {
        'score': min(100, max(0, sm_score)),
        'grade': score_to_grade(sm_score),
        'why':   sm_why,
        'label': 'Smart Money',
    }

    # ── Insider + Congress Overlap ────────────────────────────────────────────
    ic_score = 50
    ic_why   = []
    buy_syms = set()
    if insider and isinstance(insider, list):
        buys = [t for t in insider if (t.get('transaction') or '').upper() in ('BUY', 'P-PURCHASE')]
        if buys:
            buy_syms.update(t.get('ticker', '') for t in buys[:20])
            ic_score += min(15, len(buys) * 2)
            ic_why.append(f"Insider buys: {len(buys)} recent ({', '.join(list(buy_syms)[:3])}...)")
    if congress and isinstance(congress, list):
        c_buys = [t for t in congress if (t.get('type') or '').upper() in ('BUY', 'PURCHASE')]
        if c_buys:
            c_syms = set(t.get('ticker', '') for t in c_buys[:20])
            overlap = buy_syms & c_syms
            if overlap:
                ic_score += 15
                ic_why.append(f"Congress + Insider overlap: {', '.join(list(overlap)[:4])}")
            else:
                ic_score += 5
                ic_why.append(f"Congress buys: {len(c_buys)} recent")
    signals['insider_congress'] = {
        'score': min(100, max(0, ic_score)),
        'grade': score_to_grade(ic_score),
        'why':   ic_why,
        'label': 'Insider + Congress',
        'overlap_tickers': list(buy_syms)[:10],
    }

    # ── Composite quant score ─────────────────────────────────────────────────
    scores = [s['score'] for s in signals.values()]
    composite = round(sum(scores) / len(scores))
    return jsonify({
        'composite_score': composite,
        'composite_grade': score_to_grade(composite),
        'regime':          regime_label,
        'signals':         signals,
        'generated_at':    datetime.now().isoformat(),
    })


# ── Trade Signal endpoints ───────────────────────────────────────────────────

@app.route('/alerts')
@app.route('/static/alerts.html')
def alerts_page():
    return send_file(os.path.join(os.path.dirname(__file__), 'static', 'alerts.html'))


@app.route('/api/signal', methods=['POST'])
def receive_signal():
    """Receive a new trade signal from ai_brain.py."""
    try:
        payload = request.get_json(force=True)
        if not payload:
            return jsonify({"error": "empty payload"}), 400

        symbol     = str(payload.get("symbol", "")).upper()
        action     = str(payload.get("action", "")).upper()
        confidence = int(payload.get("confidence", 0))
        sig_type   = str(payload.get("type", "SWING"))
        agent      = str(payload.get("agent", ""))
        model      = str(payload.get("model", ""))
        reasoning  = str(payload.get("reasoning", ""))[:1000]
        sources    = payload.get("sources", [])
        timeframe  = str(payload.get("timeframe", "SWING"))
        entry_price = float(payload.get("price", 0) or 0)
        stop_loss   = float(payload.get("stop_loss", 0) or 0)
        take_profit = float(payload.get("take_profit", 0) or 0)
        ctx_summary = str(payload.get("context_summary", ""))

        db = get_db()
        cur = db.execute("""
            INSERT INTO trade_signals
            (type, symbol, action, entry_price, stop_loss, take_profit,
             confidence, agent_name, model_used, reasoning,
             context_json, sources_json, timeframe, status, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'NEW',?)
        """, (
            sig_type, symbol, action, entry_price, stop_loss, take_profit,
            confidence, agent, model, reasoning,
            json.dumps({"context_summary": ctx_summary}),
            json.dumps(sources if isinstance(sources, list) else [sources]),
            timeframe,
            datetime.now().isoformat(),
        ))
        signal_id = cur.lastrowid
        db.commit()
        db.close()

        # Initialize outcome tracking
        _update_outcome(signal_id, entry_price, stop_loss, take_profit)

        # Build full signal dict for SSE push
        sig_data = {
            "id": signal_id,
            "type": sig_type,
            "symbol": symbol,
            "action": action,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "confidence": confidence,
            "agent_name": agent,
            "model_used": model,
            "reasoning": reasoning,
            "sources": sources if isinstance(sources, list) else [sources],
            "timeframe": timeframe,
            "status": "NEW",
            "created_at": datetime.now().isoformat(),
            "rr_ratio": round((take_profit - entry_price) / max(entry_price - stop_loss, 0.01), 2) if (take_profit > entry_price > stop_loss > 0) else None,
        }

        # Push to live SSE subscribers
        _push_to_sse({"event": "new_signal", "signal": sig_data})

        # Notifications for high confidence signals
        if confidence >= 80:
            action_word = {"BUY": "buying", "BUY_CALL": "buying calls on",
                           "BUY_PUT": "buying puts on", "SHORT": "shorting"}.get(action, action.lower())
            voice_text = (
                f"Captain, {agent} recommends {action_word} {symbol} "
                f"at {entry_price:.2f}, confidence {confidence} percent."
            )
            threading.Thread(target=_speak_signal, args=(voice_text,), daemon=True).start()
            threading.Thread(target=_macos_notify, args=(
                f"Signal: {action} {symbol} — {confidence}%",
                f"{agent}: {reasoning[:120]}"
            ), daemon=True).start()
            threading.Thread(target=_post_war_room, args=(
                f"🚨 [{sig_type}] {agent} → {action} {symbol} @ ${entry_price:.2f} | "
                f"Conf: {confidence}% | {reasoning[:100]}"
            ,), daemon=True).start()

        return jsonify({"ok": True, "signal_id": signal_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/signals', methods=['GET'])
def list_signals():
    """List signals with optional filters."""
    sig_type = request.args.get('type', '')
    symbol   = request.args.get('symbol', '').upper()
    status   = request.args.get('status', '')
    limit    = int(request.args.get('limit', 20))
    days     = int(request.args.get('days', 7))

    db    = get_db()
    since = (datetime.now() - timedelta(days=days)).isoformat()
    wheres = ["created_at >= ?"]
    params: list = [since]
    if sig_type: wheres.append("type = ?");   params.append(sig_type)
    if symbol:   wheres.append("symbol = ?"); params.append(symbol)
    if status:   wheres.append("status = ?"); params.append(status)
    params.append(limit)

    rows = db.execute(
        f"SELECT * FROM trade_signals WHERE {' AND '.join(wheres)} "
        f"ORDER BY created_at DESC LIMIT ?",
        params
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/signals/active', methods=['GET'])
def active_signals():
    """Return only unactioned (NEW) signals from last 24h."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM trade_signals WHERE status='NEW' "
        "AND created_at >= datetime('now', '-24 hours') "
        "ORDER BY confidence DESC, created_at DESC LIMIT 50"
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/signals/<int:signal_id>', methods=['GET'])
def get_signal(signal_id):
    """Full signal detail + outcome."""
    db = get_db()
    sig = db.execute("SELECT * FROM trade_signals WHERE id=?", (signal_id,)).fetchone()
    if not sig:
        db.close()
        return jsonify({"error": "not found"}), 404
    out = db.execute("SELECT * FROM signal_outcomes WHERE signal_id=?", (signal_id,)).fetchone()
    db.close()
    result = dict(sig)
    result["outcome"] = dict(out) if out else None
    try:
        result["sources"] = json.loads(result.get("sources_json") or "[]")
        result["context"] = json.loads(result.get("context_json") or "{}")
    except Exception:
        pass
    return jsonify(result)


@app.route('/api/signals/<int:signal_id>/execute', methods=['POST'])
def execute_signal_endpoint(signal_id):
    """Execute a signal via TradeMinds paper trader. REQUIRES button click — never auto-executes."""
    db = get_db()
    sig = db.execute("SELECT * FROM trade_signals WHERE id=?", (signal_id,)).fetchone()
    if not sig:
        db.close()
        return jsonify({"error": "signal not found"}), 404
    if sig["status"] != "NEW":
        db.close()
        return jsonify({"error": f"signal already {sig['status']}"}), 400

    # Get player_id from request body (which portfolio to execute for)
    body = request.get_json(force=True) or {}
    player_id = body.get("player_id", "claude-trader")

    try:
        # Add parent dir to path for engine imports
        parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if parent not in sys.path:
            sys.path.insert(0, parent)
        from engine.paper_trader import buy as _buy
        result = _buy(
            player_id,
            sig["symbol"],
            sig["entry_price"] or 0,
            reasoning=f"[Signal Center] {sig['reasoning'][:200]}",
            confidence=(sig["confidence"] or 0) / 100.0,
        )
        if result:
            now_str = datetime.now().isoformat()
            db.execute(
                "UPDATE trade_signals SET status='EXECUTED', executed_at=? WHERE id=?",
                (now_str, signal_id)
            )
            db.commit()
            db.close()
            _push_to_sse({"event": "signal_executed", "signal_id": signal_id})
            return jsonify({"ok": True, "result": result, "executed_at": now_str})
        else:
            db.close()
            return jsonify({"ok": False, "error": "paper_trader returned no result"}), 400
    except Exception as e:
        db.close()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/signals/<int:signal_id>/dismiss', methods=['POST'])
def dismiss_signal(signal_id):
    """Mark a signal as dismissed."""
    db = get_db()
    sig = db.execute("SELECT id, status FROM trade_signals WHERE id=?", (signal_id,)).fetchone()
    if not sig:
        db.close()
        return jsonify({"error": "not found"}), 404
    now_str = datetime.now().isoformat()
    db.execute(
        "UPDATE trade_signals SET status='DISMISSED', dismissed_at=? WHERE id=?",
        (now_str, signal_id)
    )
    db.commit()
    db.close()
    _push_to_sse({"event": "signal_dismissed", "signal_id": signal_id})
    return jsonify({"ok": True, "dismissed_at": now_str})


@app.route('/api/signals/scorecard', methods=['GET'])
def signals_scorecard():
    """Accuracy stats by type, agent, and theoretical P&L."""
    days = int(request.args.get('days', 30))
    db   = get_db()
    since = (datetime.now() - timedelta(days=days)).isoformat()

    by_type = db.execute("""
        SELECT ts.type,
               COUNT(*) as total,
               SUM(CASE WHEN so.would_hit_tp=1 THEN 1 ELSE 0 END) as hit_tp,
               SUM(CASE WHEN so.would_hit_sl=1 AND so.would_hit_tp=0 THEN 1 ELSE 0 END) as hit_sl,
               AVG(so.theoretical_pnl) as avg_pnl
        FROM trade_signals ts
        LEFT JOIN signal_outcomes so ON so.signal_id=ts.id
        WHERE ts.created_at >= ?
        GROUP BY ts.type ORDER BY total DESC
    """, (since,)).fetchall()

    by_agent = db.execute("""
        SELECT ts.agent_name,
               COUNT(*) as total,
               SUM(CASE WHEN so.would_hit_tp=1 THEN 1 ELSE 0 END) as hit_tp,
               AVG(so.theoretical_pnl) as avg_pnl
        FROM trade_signals ts
        LEFT JOIN signal_outcomes so ON so.signal_id=ts.id
        WHERE ts.created_at >= ?
        GROUP BY ts.agent_name ORDER BY total DESC
    """, (since,)).fetchall()

    overall = db.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN so.would_hit_tp=1 THEN 1 ELSE 0 END) as hit_tp,
               SUM(CASE WHEN so.would_hit_sl=1 THEN 1 ELSE 0 END) as hit_sl,
               AVG(so.theoretical_pnl) as avg_pnl,
               SUM(so.theoretical_pnl) as total_pnl
        FROM trade_signals ts
        LEFT JOIN signal_outcomes so ON so.signal_id=ts.id
        WHERE ts.created_at >= ? AND ts.status != 'DISMISSED'
    """, (since,)).fetchone()

    # Top missed opportunities (dismissed but would have hit TP)
    missed = db.execute("""
        SELECT ts.symbol, ts.action, ts.confidence, ts.agent_name, ts.entry_price,
               ts.take_profit, so.theoretical_pnl, ts.created_at
        FROM trade_signals ts
        JOIN signal_outcomes so ON so.signal_id=ts.id
        WHERE ts.status='DISMISSED' AND so.would_hit_tp=1
          AND ts.created_at >= ?
        ORDER BY so.theoretical_pnl DESC LIMIT 5
    """, (since,)).fetchall()

    db.close()
    return jsonify({
        "by_type":  [dict(r) for r in by_type],
        "by_agent": [dict(r) for r in by_agent],
        "overall":  dict(overall) if overall else {},
        "missed_opportunities": [dict(r) for r in missed],
        "days": days,
    })


@app.route('/api/signals/outcomes', methods=['GET'])
def signal_outcomes():
    """Return outcome tracking for all recent signals."""
    days  = int(request.args.get('days', 7))
    since = (datetime.now() - timedelta(days=days)).isoformat()
    db    = get_db()
    rows  = db.execute("""
        SELECT ts.id, ts.symbol, ts.action, ts.type, ts.confidence,
               ts.agent_name, ts.entry_price, ts.stop_loss, ts.take_profit,
               ts.status, ts.created_at,
               so.tracked_current, so.tracked_high, so.tracked_low,
               so.would_hit_tp, so.would_hit_sl, so.theoretical_pnl, so.last_updated
        FROM trade_signals ts
        LEFT JOIN signal_outcomes so ON so.signal_id=ts.id
        WHERE ts.created_at >= ?
        ORDER BY ts.created_at DESC LIMIT 100
    """, (since,)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/brain-context', methods=['GET'])
def brain_context_proxy():
    """Proxy to TradeMinds brain context on port 8080."""
    symbol    = request.args.get('symbol', 'SPY').upper()
    player_id = request.args.get('player_id', 'claude-trader')
    try:
        r = requests.get(
            f"{BRIDGE}/api/brain-context",
            params={"symbol": symbol, "player_id": player_id},
            timeout=15,
        )
        if r.status_code == 200:
            return jsonify(r.json())
        # Fall back to importing directly if bridge unavailable
        parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if parent not in sys.path:
            sys.path.insert(0, parent)
        from engine.brain_context import build_full_context_raw
        return jsonify({"ok": True, **build_full_context_raw(player_id, symbol)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/health', methods=['GET'])
def signal_center_health():
    """Signal center status."""
    db   = get_db()
    tot  = db.execute("SELECT COUNT(*) FROM trade_signals").fetchone()[0]
    new  = db.execute("SELECT COUNT(*) FROM trade_signals WHERE status='NEW' AND created_at >= datetime('now', '-24 hours')").fetchone()[0]
    exe  = db.execute("SELECT COUNT(*) FROM trade_signals WHERE status='EXECUTED'").fetchone()[0]
    last = db.execute("SELECT created_at FROM trade_signals ORDER BY created_at DESC LIMIT 1").fetchone()
    db.close()
    return jsonify({
        "ok": True,
        "total_signals": tot,
        "active_signals": new,
        "executed_signals": exe,
        "last_signal_at": last[0] if last else None,
        "sse_subscribers": len(_sse_subscribers),
        "outcome_tracker": "running",
    })


@app.route('/api/signals/stream', methods=['GET'])
def signals_sse_stream():
    """Server-Sent Events stream for real-time signal push. Connect with EventSource('/api/signals/stream')."""
    def generate():
        q: Queue = Queue(maxsize=100)
        with _sse_lock:
            _sse_subscribers.append(q)
        try:
            # Send connection confirmation
            yield "data: " + json.dumps({"event": "connected", "ts": datetime.now().isoformat()}) + "\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"data: {msg}\n\n"
                except Empty:
                    # Heartbeat keep-alive
                    yield ": ping\n\n"
        finally:
            with _sse_lock:
                if q in _sse_subscribers:
                    _sse_subscribers.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )



# ── Intelligence Feed ─────────────────────────────────────────────────────────
_FEED_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")

def _ensure_feed_table():
    """Create intelligence_feed table if missing."""
    try:
        db = sqlite3.connect(_FEED_DB_PATH, timeout=5)
        db.execute("""
            CREATE TABLE IF NOT EXISTS intelligence_feed (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_type  TEXT NOT NULL,
                data       TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("CREATE INDEX IF NOT EXISTS idx_feed_type_ts ON intelligence_feed(feed_type, created_at)")
        db.commit()
        db.close()
    except Exception:
        pass

_ensure_feed_table()

# Colors for feed types (used in SSE metadata)
_FEED_COLORS = {
    "BRIDGE_VOTE": "#4499ff",
    "RED_ALERT": "#ff4444",
    "GEX_UPDATE": "#ffdd00",
    "VOLUME_SPIKE": "#ff9900",
    "CONGRESS": "#aa44ff",
    "REGIME_CHANGE": "#ffffff",
    "LESSON": "#00cc66",
    "BOOTSTRAP": "#00aaff",
    "SYSTEM": "#888888",
    "SCREENER": "#00ffcc",
    "GEX": "#ffdd00",
}

# Critical types that trigger voice + notification
_CRITICAL_TYPES = {"REGIME_CHANGE", "RED_ALERT"}


@app.route('/api/feed', methods=['POST'])
def receive_feed():
    """Universal intake for all TradeMinds intelligence sources."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        feed_type = str(data.get("type", "SYSTEM")).upper()
        # Remove 'type' from the data payload since we store it separately
        payload = {k: v for k, v in data.items() if k != "type"}
        payload_str = json.dumps(payload)

        db = sqlite3.connect(_FEED_DB_PATH, timeout=5)
        db.execute(
            "INSERT INTO intelligence_feed (feed_type, data) VALUES (?,?)",
            (feed_type, payload_str)
        )
        db.commit()
        db.close()

        # Push to SSE subscribers
        _push_to_sse({
            "event": "feed",
            "type": feed_type,
            "data": payload,
            "color": _FEED_COLORS.get(feed_type, "#888888"),
            "ts": datetime.now().isoformat(),
        })

        # Voice alert for critical events
        if feed_type in _CRITICAL_TYPES:
            msg = payload.get("level") or payload.get("regime") or feed_type
            threading.Thread(
                target=lambda: _speak_signal(f"Alert: {msg}"),
                daemon=True
            ).start()

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/feed', methods=['GET'])
def get_feed():
    """Return last 100 feed entries, optionally filtered by type."""
    feed_type = request.args.get("type", "").upper()
    limit = min(int(request.args.get("limit", 100)), 500)
    try:
        db = sqlite3.connect(_FEED_DB_PATH, timeout=5)
        db.row_factory = sqlite3.Row
        if feed_type:
            rows = db.execute(
                "SELECT * FROM intelligence_feed WHERE feed_type=? ORDER BY created_at DESC LIMIT ?",
                (feed_type, limit)
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM intelligence_feed ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        db.close()
        result = []
        for r in rows:
            try:
                d = json.loads(r["data"])
            except Exception:
                d = {"raw": r["data"]}
            result.append({
                "id": r["id"],
                "type": r["feed_type"],
                "data": d,
                "color": _FEED_COLORS.get(r["feed_type"], "#888888"),
                "created_at": r["created_at"],
            })
        return jsonify({"feed": result, "count": len(result)})
    except Exception as e:
        return jsonify({"feed": [], "error": str(e)})


@app.route('/api/intelligence-summary', methods=['GET'])
def intelligence_summary():
    """Single endpoint returning the full state of TradeMinds intelligence."""
    out: dict = {}
    try:
        db = sqlite3.connect(_FEED_DB_PATH, timeout=5)
        db.row_factory = sqlite3.Row

        for feed_type in ("BRIDGE_VOTE", "RED_ALERT", "GEX_UPDATE", "REGIME_CHANGE"):
            row = db.execute(
                "SELECT data, created_at FROM intelligence_feed WHERE feed_type=? ORDER BY created_at DESC LIMIT 1",
                (feed_type,)
            ).fetchone()
            if row:
                try:
                    d = json.loads(row["data"])
                    d["_ts"] = row["created_at"]
                    out[feed_type.lower()] = d
                except Exception:
                    pass

        # Feed stats today
        today = datetime.now().strftime("%Y-%m-%d")
        count = db.execute(
            "SELECT COUNT(*) FROM intelligence_feed WHERE created_at >= ?", (today,)
        ).fetchone()[0]
        out["feed_entries_today"] = count
        db.close()

        # Bridge proxy
        try:
            r = requests.get(f"{BRIDGE}/api/bridge/consensus", timeout=3)
            if r.ok:
                out["last_bridge_vote"] = r.json()
        except Exception:
            pass
        # Signal count
        try:
            r2 = requests.get(f"{BRIDGE}/api/signals/active", timeout=3)
            if r2.ok:
                d2 = r2.json()
                out["active_signals"] = len(d2.get("signals", d2 if isinstance(d2, list) else []))
        except Exception:
            pass

        out["ts"] = datetime.now().isoformat()
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e), "ts": datetime.now().isoformat()})


@app.route('/api/screener', methods=['POST', 'GET'])
def signal_center_screener():
    """Proxy screener to dashboard or run standalone."""
    try:
        if request.method == "POST":
            filters = request.get_json(force=True, silent=True) or {}
        else:
            filters = dict(request.args)
        # Try proxying to dashboard first
        try:
            r = requests.post(f"{BRIDGE}/api/screener/pro", json=filters, timeout=10)
            if r.ok:
                return jsonify(r.json())
        except Exception:
            pass
        # Fallback: run locally
        from engine.screener_engine import run_screener
        results = run_screener(filters)
        return jsonify({"results": results, "count": len(results)})
    except Exception as e:
        return jsonify({"results": [], "error": str(e)})


@app.route('/api/proxy/benchmark', methods=['GET'])
def proxy_benchmark():
    """Proxy GET /api/benchmark/summary from port 8080 (avoids CORS from port 9000 pages)."""
    try:
        days = request.args.get("days", "30")
        r = requests.get(f"{BRIDGE}/api/benchmark/summary?days={days}", timeout=8)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route('/api/proxy/benchmark/detail', methods=['GET'])
def proxy_benchmark_detail():
    """Proxy GET /api/benchmark?days=N from port 8080."""
    try:
        days = request.args.get("days", "30")
        r = requests.get(f"{BRIDGE}/api/benchmark?days={days}", timeout=8)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route('/api/proxy/bootstrap', methods=['GET'])
def proxy_bootstrap():
    """Proxy GET /api/bootstrap-report from port 8080 (avoids CORS from port 9000 pages)."""
    try:
        r = requests.get(f"{BRIDGE}/api/bootstrap-report", timeout=8)
        return jsonify(r.json()), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── 8-Week EMA Pullback Screener ─────────────────────────────────────────────
_ema_pullback_cache: dict = {"ts": 0.0, "data": None}
_ema_pullback_lock  = threading.Lock()
_EMA_PULLBACK_TTL   = 3600 * 4   # 4-hour cache (weekly data is stable intraday)


def _calc_ema(prices: list, period: int) -> list:
    """Calculate EMA for a list of prices. Returns list same length (NaN-padded)."""
    result = [None] * len(prices)
    if len(prices) < period:
        return result
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    result[period - 1] = ema
    for i in range(period, len(prices)):
        ema = prices[i] * k + ema * (1 - k)
        result[i] = ema
    return result


def _run_ema_pullback_scan(symbols: list) -> dict:
    """Core screener logic. Returns the full API response dict."""
    try:
        import yfinance as yf
    except ImportError:
        return {"error": "yfinance not available", "matches": [], "total_scanned": 0,
                "total_matches": 0, "generated_at": datetime.now().isoformat()}

    # Fetch SPY weekly closes for relative-strength baseline (need 55+ weeks)
    spy_closes = []
    try:
        spy_tk  = yf.Ticker("SPY")
        spy_hist = spy_tk.history(period="2y", interval="1wk", auto_adjust=True)
        spy_closes = spy_hist["Close"].dropna().tolist()
    except Exception:
        pass

    spy_4w_change = None
    if len(spy_closes) >= 5:
        spy_4w_change = (spy_closes[-1] - spy_closes[-5]) / spy_closes[-5] * 100

    matches   = []
    scanned   = 0
    errors    = 0

    for sym in symbols:
        try:
            tk   = yf.Ticker(sym)
            hist = tk.history(period="2y", interval="1wk", auto_adjust=True)
            hist = hist.dropna(subset=["Close"])
            closes = hist["Close"].tolist()

            if len(closes) < 55:   # need 50 weeks + buffer
                continue

            scanned += 1

            # ── Calculate EMAs ────────────────────────────────────────────────
            ema8_series  = _calc_ema(closes, 8)
            ema20_series = _calc_ema(closes, 20)
            ema50_series = _calc_ema(closes, 50)

            price    = closes[-1]
            ema8     = ema8_series[-1]
            ema20    = ema20_series[-1]
            ema50    = ema50_series[-1]
            ema8_2wa = ema8_series[-3]   # value 2 weeks ago (index -3 = current-2)

            if None in (ema8, ema20, ema50, ema8_2wa):
                continue

            # ── STEP 1: Bullish Trend Filter ──────────────────────────────────
            above_20w  = price > ema20
            above_50w  = price > ema50
            ema8_slope = ema8 > ema8_2wa

            if not (above_20w and above_50w and ema8_slope):
                continue

            # ── STEP 2: Pullback Filter (entry zone) ──────────────────────────
            deviation_pct = (price - ema8) / ema8 * 100   # + = above, - = below

            # Within 3% above 8w EMA (price >= ema8 AND price <= ema8 * 1.03)
            in_zone = 0.0 <= deviation_pct <= 3.0

            # Has NOT closed below the 8w EMA on the last weekly candle
            last_close_above = closes[-1] >= ema8

            if not (in_zone and last_close_above):
                continue

            # ── STEP 3: Relative Strength vs SPY (4-week) ────────────────────
            rs_vs_spy = None
            if len(closes) >= 5 and spy_4w_change is not None:
                stock_4w = (closes[-1] - closes[-5]) / closes[-5] * 100
                rs_vs_spy = stock_4w - spy_4w_change
                if stock_4w <= spy_4w_change:
                    continue   # underperforming SPY

            # ── Passed all filters — build output ────────────────────────────
            why = []
            why.append(f"Price ${price:.2f} above 20w EMA ${ema20:.2f}")
            why.append(f"Price above 50w EMA ${ema50:.2f}")
            slope_pct = (ema8 - ema8_2wa) / ema8_2wa * 100
            why.append(f"8w EMA sloping up +{slope_pct:.2f}% over 2 weeks")
            why.append(f"Pullback zone: {deviation_pct:.2f}% above 8w EMA ${ema8:.2f}")
            if rs_vs_spy is not None:
                why.append(f"RS vs SPY +{rs_vs_spy:.2f}% over 4 weeks")

            # Score: tighter to EMA = higher score (deviation 0% → 100, 3% → 70)
            base_score = 100 - int(deviation_pct * 10)   # 0%=100, 3%=70
            if rs_vs_spy is not None:
                rs_bonus = min(10, int(rs_vs_spy))
                base_score = min(100, base_score + rs_bonus)
            score = max(0, min(100, base_score))

            matches.append({
                "symbol":        sym,
                "score":         score,
                "grade":         score_to_grade(score),
                "price":         round(price, 2),
                "ema_8w":        round(ema8, 2),
                "ema_20w":       round(ema20, 2),
                "ema_50w":       round(ema50, 2),
                "deviation_pct": round(deviation_pct, 2),
                "rs_vs_spy_4w":  round(rs_vs_spy, 2) if rs_vs_spy is not None else None,
                "why":           why,
            })

        except Exception:
            errors += 1
            continue

    # Sort by score descending
    matches.sort(key=lambda x: x["score"], reverse=True)

    # Overall signal score = avg of top matches (or 50 if none)
    overall_score = 50
    if matches:
        overall_score = min(100, round(sum(m["score"] for m in matches) / len(matches)))

    return {
        "signal":        "8-Week EMA Pullback",
        "score":         overall_score,
        "grade":         score_to_grade(overall_score),
        "matches":       matches,
        "total_scanned": scanned,
        "total_matches": len(matches),
        "spy_4w_change": round(spy_4w_change, 2) if spy_4w_change is not None else None,
        "generated_at":  datetime.now().isoformat(),
    }


@app.route('/api/ema-pullback')
def ema_pullback():
    """8-Week EMA Pullback screener.

    Scans the watchlist for stocks in a bullish trend that have pulled back
    to touch (or come within 3%) of their 8-week EMA while outperforming SPY.

    Filters:
      Step 1 — Bullish trend: price > 20w EMA, price > 50w EMA, 8w EMA sloping up
      Step 2 — Pullback zone: price within 3% above 8w EMA, no weekly close below it
      Step 3 — Relative strength: stock 4w % change > SPY 4w % change

    Optional query params:
      ?symbols=AAPL,NVDA,...   override the default watchlist
      ?refresh=1               bypass cache and force a fresh scan
    """
    force = request.args.get("refresh", "0") == "1"

    with _ema_pullback_lock:
        now = _time.time()
        if not force and _ema_pullback_cache["data"] and (now - _ema_pullback_cache["ts"]) < _EMA_PULLBACK_TTL:
            return jsonify(_ema_pullback_cache["data"])

    # Build symbol list
    custom = request.args.get("symbols", "")
    if custom:
        symbols = [s.strip().upper() for s in custom.split(",") if s.strip()]
    else:
        # Try daily watchlist, fall back to FIXED_WATCHLIST + SPY/QQQ
        symbols = list(FIXED_WATCHLIST)
        try:
            wl_path = os.path.normpath(DAILY_WATCHLIST_PATH)
            if os.path.exists(wl_path):
                with open(wl_path) as _f:
                    _wl = json.load(_f)
                today = datetime.now().strftime("%Y-%m-%d")
                if _wl.get("scan_date") == today:
                    symbols = _wl.get("symbols", symbols)
        except Exception:
            pass

        # Pull any additional symbols from fast-scan bridge results
        try:
            scan = _bridge_get('/api/fast-scan', timeout=4)
            if scan and isinstance(scan, dict):
                extras = [r.get("symbol") or r.get("ticker") for r in scan.get("results", [])]
                extras = [s for s in extras if s]
                symbols = list(dict.fromkeys(symbols + extras))  # dedupe, preserve order
        except Exception:
            pass

    result = _run_ema_pullback_scan(symbols)

    with _ema_pullback_lock:
        _ema_pullback_cache["ts"]   = _time.time()
        _ema_pullback_cache["data"] = result

    return jsonify(result)


# ── Trade Cards — enriched signal cards with full execution context ─────────

@app.route('/api/trade-cards', methods=['GET'])
def trade_cards():
    """Return top-N trade cards: signals enriched with trade levels, GEX, regime, sizing."""
    limit  = min(int(request.args.get('limit', 20)), 50)
    grade  = request.args.get('grade', '')   # filter: A, B, C
    try:
        db    = get_db()
        query = """
            SELECT ts.id, ts.symbol, ts.action, ts.entry_price, ts.stop_loss,
                   ts.take_profit, ts.confidence, ts.agent_name, ts.reasoning,
                   ts.timeframe, ts.status, ts.created_at,
                   so.theoretical_pnl, so.would_hit_tp, so.would_hit_sl,
                   so.tracked_current
            FROM trade_signals ts
            LEFT JOIN signal_outcomes so ON so.signal_id = ts.id
            WHERE ts.status = 'NEW'
              AND ts.created_at >= datetime('now', '-48 hours')
        """
        params = []
        if grade:
            conf_map = {'A': 75, 'B': 60, 'C': 0}
            conf_min = conf_map.get(grade.upper(), 0)
            conf_max = conf_map.get({'A':'A','B':'B','C':'C'}.get(grade.upper(),'C'), 100)
            query += " AND ts.confidence >= ?"
            params.append(conf_min)
        query += " ORDER BY ts.confidence DESC, ts.created_at DESC LIMIT ?"
        params.append(limit)

        rows   = db.execute(query, params).fetchall()
        db.close()

        cards = []
        for row in rows:
            sym   = row['symbol']
            conf  = int(row['confidence'] or 0)
            prob  = conf / 100.0

            # Determine grade + tier
            if conf >= 75:
                sig_grade, tier, can_execute = 'A', 'GREEN', True
            elif conf >= 60:
                sig_grade, tier, can_execute = 'B', 'YELLOW', True
            else:
                sig_grade, tier, can_execute = 'C', 'RED', False

            # Get trade levels (cached)
            levels = _get_trade_levels_cached(sym) or {}
            long_l = levels.get('long', {})
            price  = float(levels.get('price', 0) or row['entry_price'] or 0)
            stop   = float(long_l.get('stop_loss') or row['stop_loss'] or 0)
            tp1    = float(long_l.get('tp1') or row['take_profit'] or 0)
            tp2    = float(long_l.get('tp2') or 0)
            tp3    = float(long_l.get('tp3') or 0)
            rr     = float(long_l.get('rr') or 0)

            # Position size recommendation (5% base, scaled by regime)
            regime  = levels.get('regime', 'UNKNOWN')
            r_mult  = float(levels.get('risk_mult') or 1.0)
            pos_pct = round(5.0 * r_mult, 1)

            # Risk/reward
            risk   = price - stop if price > stop > 0 else 0
            reward = tp2 - price  if tp2 > price    else (tp1 - price if tp1 > price else 0)
            rr_val = round(reward / risk, 2) if risk > 0 else 0

            # GEX levels
            gex    = levels.get('gex', {})

            cards.append({
                'id':           row['id'],
                'symbol':       sym,
                'direction':    row['action'] or 'BUY',
                'entry_price':  round(price, 2),
                'stop_loss':    round(stop, 2),
                'tp1':          round(tp1, 2),
                'tp2':          round(tp2, 2),
                'tp3':          round(tp3, 2),
                'probability':  prob,
                'confidence':   conf,
                'grade':        sig_grade,
                'tier':         tier,
                'can_execute':  can_execute,
                'sc_score':     conf,
                'rr':           rr_val,
                'regime':       regime,
                'pos_size_pct': pos_pct,
                'gex':          gex,
                'agent':        row['agent_name'],
                'reasoning':    (row['reasoning'] or '')[:200],
                'timeframe':    row['timeframe'],
                'status':       row['status'],
                'created_at':   row['created_at'],
                'theo_pnl':     round(float(row['theoretical_pnl'] or 0), 2),
                'hit_tp':       bool(row['would_hit_tp']),
                'hit_sl':       bool(row['would_hit_sl']),
                'current_price':float(row['tracked_current'] or price),
            })

        return jsonify({'cards': cards, 'count': len(cards)})
    except Exception as e:
        return jsonify({'error': str(e), 'cards': []}), 500


@app.route('/api/execute-trade', methods=['POST'])
def execute_trade():
    """One-click execute: place a market order on Alpaca and log it.
    Body: {signal_id, symbol, direction, qty, entry_price, stop_loss, tp1, tp2, tp3, grade, prob, source}
    """
    try:
        data      = request.get_json(force=True, silent=True) or {}
        signal_id = data.get('signal_id')
        symbol    = str(data.get('symbol', '')).upper()
        direction = str(data.get('direction', 'BUY')).upper()
        qty       = float(data.get('qty', 0))
        entry     = float(data.get('entry_price', 0))
        stop      = float(data.get('stop_loss', 0))
        tp1       = float(data.get('tp1', 0))
        tp2       = float(data.get('tp2', 0))
        tp3       = float(data.get('tp3', 0))
        grade     = str(data.get('grade', 'B'))
        prob      = float(data.get('prob', 0.60))
        source    = str(data.get('source', 'signal_center'))

        if not symbol or qty <= 0:
            return jsonify({'error': 'symbol and qty required'}), 400

        # Only GREEN (A) and YELLOW (B) get execute button — enforce server-side
        if grade not in ('A', 'B'):
            return jsonify({'error': 'Only Grade A and B signals can be executed'}), 403

        # Call Alpaca bridge
        alpaca_order_id = None
        fill_price      = entry
        alpaca_status   = 'SIMULATED'

        try:
            if direction == 'BUY':
                r = requests.post(f"{BRIDGE}/api/alpaca/buy",
                                  json={'symbol': symbol, 'qty': qty},
                                  timeout=10)
            else:
                r = requests.post(f"{BRIDGE}/api/alpaca/sell",
                                  json={'symbol': symbol, 'qty': qty},
                                  timeout=10)
            if r.ok:
                resp = r.json()
                alpaca_order_id = resp.get('order_id')
                fill_price      = float(resp.get('filled_avg_price') or entry)
                alpaca_status   = 'FILLED' if resp.get('success') else 'PENDING'
            else:
                alpaca_status = f'ERROR_{r.status_code}'
        except Exception as ae:
            alpaca_status = f'BRIDGE_ERROR: {str(ae)[:80]}'

        # Log execution
        db  = get_db()
        now = datetime.now().isoformat()
        db.execute("""
            INSERT INTO execution_log
              (signal_id, symbol, direction, qty, entry_price, fill_price,
               stop_loss, tp1, tp2, tp3, grade, prob, source,
               alpaca_order_id, status, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (signal_id, symbol, direction, qty, entry, fill_price,
              stop, tp1, tp2, tp3, grade, prob, source,
              alpaca_order_id, alpaca_status, now))

        # Mark signal as executed
        if signal_id:
            db.execute(
                "UPDATE trade_signals SET status='EXECUTED', executed_at=? WHERE id=?",
                (now, signal_id)
            )
        db.commit()
        db.close()

        return jsonify({
            'ok':             True,
            'symbol':         symbol,
            'direction':      direction,
            'qty':            qty,
            'fill_price':     fill_price,
            'alpaca_order_id': alpaca_order_id,
            'alpaca_status':  alpaca_status,
            'executed_at':    now,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/execution-log', methods=['GET'])
def get_execution_log():
    """Return recent execution history."""
    limit = min(int(request.args.get('limit', 50)), 200)
    try:
        db   = get_db()
        rows = db.execute("""
            SELECT id, signal_id, symbol, direction, qty, entry_price, fill_price,
                   stop_loss, tp1, tp2, tp3, grade, prob, source,
                   alpaca_order_id, status, executed_at, notes
            FROM execution_log
            ORDER BY executed_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        db.close()
        return jsonify({'executions': [dict(r) for r in rows], 'count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/live-feeds', methods=['GET'])
def live_feeds():
    """Return all live data feeds for the Signal Center tactical dashboard.
    Aggregates: GEX overlay, flow lean, volume radar, CRITICAL alerts,
    crew consensus, regime, RedAlert score, earnings, congress trades,
    Neo/Ollie recommendations.
    """
    feeds = {}
    bridge_endpoints = {
        'regime':          '/api/regime',
        'gex_overlay':     '/api/gex-overlay/levels?symbol=SPY',
        'flow_lean':       '/api/market/options-flow',
        'volume_radar':    '/api/volume-radar',
        'crew_consensus':  '/api/bridge/consensus',
        'troi_signal':     '/api/bull-bear/all?model=troi',
        'red_alert':       '/api/red-alert/status',
        'earnings':        '/api/market/earnings',
        'congress':        '/api/congress/trades',
        'vix':             '/api/market/vix',
        'breadth':         '/api/breadth',
        'fear_greed':      '/api/fear-greed',
    }
    for key, ep in bridge_endpoints.items():
        feeds[key] = _bridge_get(ep, timeout=5)

    # CRITICAL volume alerts (100x+ volume)
    try:
        vol_data = feeds.get('volume_radar') or {}
        alerts   = vol_data.get('alerts', []) if isinstance(vol_data, dict) else []
        feeds['critical_alerts'] = [
            a for a in alerts
            if float(a.get('relative_volume') or a.get('vol_ratio') or 0) >= 100
        ]
    except Exception:
        feeds['critical_alerts'] = []

    # Neo/Ollie last recommendations from ollie_super_trades
    try:
        _TRADER_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'trader.db')
        tdb = sqlite3.connect(_TRADER_DB, timeout=5)
        tdb.row_factory = sqlite3.Row
        neo_rows = tdb.execute("""
            SELECT symbol, entry_price, signal_source, signal_grade, success_prob, created_at
            FROM ollie_super_trades
            WHERE created_at >= datetime('now', '-24 hours')
            ORDER BY created_at DESC LIMIT 10
        """).fetchall()
        tdb.close()
        feeds['ollie_neo_recent'] = [dict(r) for r in neo_rows]
    except Exception:
        feeds['ollie_neo_recent'] = []

    feeds['ts'] = datetime.now().isoformat()
    return jsonify(feeds)


@app.route('/api/spread-builder/<symbol>', methods=['GET'])
def spread_builder(symbol):
    """Auto-calculate options spread based on regime and VIX.
    Returns: vertical spread (bull call/bear put), iron condor, or single leg.
    Includes max profit, max loss, breakeven, probability of profit.
    """
    symbol = symbol.upper()
    try:
        levels = _get_trade_levels_cached(symbol) or {}
        regime = levels.get('regime', 'UNKNOWN')
        price  = float(levels.get('price', 0))
        atr    = float(levels.get('atr', price * 0.02 if price else 1))

        # Get VIX
        vix_data  = _bridge_get('/api/market/vix', timeout=5) or {}
        vix       = float(vix_data.get('vix') or vix_data.get('value') or 20)

        r2 = lambda v: round(v, 2)

        rec = {
            'symbol':  symbol,
            'price':   r2(price),
            'regime':  regime,
            'vix':     round(vix, 1),
        }

        if price <= 0:
            return jsonify({'error': 'No price data', 'symbol': symbol}), 404

        # Choose spread type based on regime + VIX
        if 'BULL' in regime and vix < 20:
            # Bull call spread
            short_strike = r2(price * 1.03)
            long_strike  = r2(price * 1.00)
            max_profit   = r2((short_strike - long_strike) * 0.6 * 100)   # ~60% of width
            max_loss     = r2((short_strike - long_strike) * 0.4 * 100)   # premium paid
            breakeven    = r2(long_strike + max_loss / 100)
            pop          = round(0.55 + (1 - vix / 100) * 0.1, 2)
            rec.update({
                'spread_type':  'BULL_CALL_SPREAD',
                'long_leg':     {'action': 'BUY',  'strike': long_strike,  'type': 'CALL'},
                'short_leg':    {'action': 'SELL', 'strike': short_strike, 'type': 'CALL'},
                'max_profit':   max_profit,
                'max_loss':     max_loss,
                'breakeven':    breakeven,
                'prob_profit':  pop,
                'suggested_dte': 30 if vix < 15 else 21,
            })
        elif 'BEAR' in regime or 'CRISIS' in regime:
            # Bear put spread
            long_strike  = r2(price * 1.00)
            short_strike = r2(price * 0.97)
            max_profit   = r2((long_strike - short_strike) * 0.6 * 100)
            max_loss     = r2((long_strike - short_strike) * 0.4 * 100)
            breakeven    = r2(long_strike - max_loss / 100)
            pop          = round(0.55 + (vix - 20) / 100, 2)
            rec.update({
                'spread_type':  'BEAR_PUT_SPREAD',
                'long_leg':     {'action': 'BUY',  'strike': long_strike,  'type': 'PUT'},
                'short_leg':    {'action': 'SELL', 'strike': short_strike, 'type': 'PUT'},
                'max_profit':   max_profit,
                'max_loss':     max_loss,
                'breakeven':    breakeven,
                'prob_profit':  pop,
                'suggested_dte': 21,
            })
        else:
            # Iron condor for sideways/high-VIX
            call_short = r2(price + atr * 2)
            call_long  = r2(price + atr * 3)
            put_short  = r2(price - atr * 2)
            put_long   = r2(price - atr * 3)
            wing_width = r2(call_long - call_short)
            premium    = r2(wing_width * 0.35 * 100)
            max_loss   = r2((wing_width - premium / 100) * 100)
            pop        = round(0.65 - (vix - 20) / 200, 2)
            rec.update({
                'spread_type':  'IRON_CONDOR',
                'call_spread':  {'short': call_short, 'long': call_long},
                'put_spread':   {'short': put_short,  'long': put_long},
                'max_profit':   premium,
                'max_loss':     max_loss,
                'breakeven_call': r2(call_short + premium / 100),
                'breakeven_put':  r2(put_short  - premium / 100),
                'prob_profit':  pop,
                'suggested_dte': 45 if vix > 25 else 30,
            })

        return jsonify(rec)
    except Exception as e:
        return jsonify({'error': str(e), 'symbol': symbol}), 500


if __name__ == '__main__':
    print(f"Signal Command Center → http://127.0.0.1:9000")
    print(f"Database : {DB_PATH}")
    print(f"Exports  : {EXPORTS_DIR}")
    app.run(host='127.0.0.1', port=9000, debug=False, threaded=True)
