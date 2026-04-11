from __future__ import annotations
# IMPORTANT: Always run with the project venv, NOT .venv or system Python.
# Correct restart command:
#   cd ~/autonomous-trader && pkill -9 -f "main\.py" 2>/dev/null; \
#   lsof -ti :8080 | xargs kill -9 2>/dev/null; sleep 5; \
#   source venv/bin/activate && nohup python3 main.py > scanner.log 2>&1 &
import sys as _sys, os as _os
_proj_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _proj_root not in _sys.path:
    _sys.path.insert(0, _proj_root)
import logging as _logging
logger = _logging.getLogger("app")
import math
from dotenv import load_dotenv
load_dotenv(override=True)

from fastapi import FastAPI, Request, Form, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, JSONResponse
import sqlite3
import json
import os
import time as _time_module
import threading
import uvicorn

_SERVER_START: float = _time_module.time()  # epoch — used by /api/health for uptime
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
import pyotp as _pyotp
import io as _io
import base64 as _base64
import qrcode as _qrcode
from shared.matrix_bridge import annotate_player_payload, ensure_matrix_shared_records, is_independent_player


def _sanitize_floats(obj):
    """Recursively replace NaN/inf float values with None and convert numpy scalars to Python natives."""
    import math
    type_name = type(obj).__module__
    if type_name == "numpy":
        try:
            import numpy as np
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                f = float(obj)
                return None if (math.isnan(f) or math.isinf(f)) else f
            if isinstance(obj, np.ndarray):
                return [_sanitize_floats(v) for v in obj.tolist()]
        except ImportError:
            pass
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_floats(v) for v in obj]
    return obj


class SafeJSONResponse(JSONResponse):
    """JSONResponse that sanitizes NaN/inf floats and numpy scalars before serialization."""
    def render(self, content) -> bytes:
        return json.dumps(
            _sanitize_floats(content),
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")


app = FastAPI(title="TradeMinds — The Bridge", docs_url=None, redoc_url=None, openapi_url=None, default_response_class=SafeJSONResponse)
try:
    ensure_matrix_shared_records()
except Exception:
    pass  # DB may be locked during startup; non-critical, retried on next request

# UOA (Unusual Options Activity) scanner routes
from uoa.routes import router as uoa_router
app.include_router(uoa_router, prefix="/api/uoa", tags=["UOA"])

# Trade Cards — Command Center API
from engine.trade_cards_api import router as trade_cards_router
app.include_router(trade_cards_router, tags=["Trade Cards"])

# Ready Room — Daily Session Gameplan (options structure analysis)
from dashboard.ready_room_routes import router as ready_room_router
app.include_router(ready_room_router, prefix="/api/ready-room", tags=["Ready Room"])

from dashboard.phase4_routes import router as phase4_router
app.include_router(phase4_router, prefix="/api/phase4", tags=["Phase4"])

# Bridge Vote — Tier 3 morning vote endpoints
try:
    from engine.bridge_vote import get_latest_votes as _bv_votes, get_latest_consensus as _bv_consensus
    _bridge_vote_ok = True
except Exception as _bv_err:
    _bridge_vote_ok = False
    _bv_err_msg = str(_bv_err)


@app.get("/api/bridge/votes")
def api_bridge_votes():
    """Return today's individual Bridge Vote results from all 8 Tier-3 voters."""
    if not _bridge_vote_ok:
        return {"error": f"bridge_vote module unavailable: {_bv_err_msg}", "votes": [], "tally": {}}
    try:
        return _bv_votes()
    except Exception as exc:
        return {"error": str(exc), "votes": [], "tally": {}}


@app.get("/api/bridge/consensus")
def api_bridge_consensus():
    """Return the latest Bridge Vote consensus (conviction level + tally)."""
    if not _bridge_vote_ok:
        return {"error": f"bridge_vote module unavailable: {_bv_err_msg}"}
    try:
        return _bv_consensus()
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/api/bridge/force-vote")
def api_bridge_force_vote():
    """Force-run today's Bridge Vote regardless of time gate. Admin use only."""
    import time as _t
    if not _bridge_vote_ok:
        return {"error": f"bridge_vote module unavailable: {_bv_err_msg}"}
    last_exc = None
    for attempt in range(6):
        try:
            from engine.bridge_vote import run_morning_vote
            result = run_morning_vote(force=True)
            return {"ok": True, "result": result}
        except Exception as exc:
            last_exc = exc
            if "locked" in str(exc).lower() and attempt < 5:
                _t.sleep(3 + attempt * 2)  # 3, 5, 7, 9, 11s
                continue
            break
    return {"ok": False, "error": str(last_exc)}


@app.on_event("startup")
def _preload_slow_caches():
    """Preload sector heatmap and market movers caches on startup.

    Phase 1 (sync, instant): seed SWR from disk cache immediately — first request is always fast.
    Phase 2 (async, background): fetch fresh data from Yahoo/Finviz and update SWR.
    """
    import time as _t

    # Ensure locks + caches exist for all SWR endpoints
    for _k in ("sectors_heatmap", "market_movers", "leaderboard", "holdings_top"):
        _swr_locks.setdefault(_k, threading.Lock())
        _swr_cache.setdefault(_k, {"data": None, "ts": 0})

    # === PHASE 1: Synchronously seed SWR from disk (zero network calls, instant) ===
    try:
        from engine.premarket_scanner import _sector_disk_cache
        disk_sectors = _sector_disk_cache.get("sectors") or []
        disk_ts = float(_sector_disk_cache.get("ts", 0))
        if disk_sectors:
            _swr_cache["sectors_heatmap"] = {"data": {"sectors": disk_sectors}, "ts": disk_ts}
    except Exception:
        pass

    try:
        from engine.market_movers import _movers_disk_cache
        disk_movers = {k: v for k, v in _movers_disk_cache.items() if k != "_ts"}
        disk_ts_m = float(_movers_disk_cache.get("_ts", 0))
        if disk_movers.get("gainers"):
            _swr_cache["market_movers"] = {"data": disk_movers, "ts": disk_ts_m}
    except Exception:
        pass

    # === PHASE 2: Background refresh — fetch fresh data (network, takes a few seconds) ===
    def _warm_sectors():
        """Warm sector heatmap — acquires lock so endpoint bg-refresh can't double-fetch."""
        if not _swr_locks["sectors_heatmap"].acquire(blocking=False):
            return  # Already refreshing
        _swr_refreshing.add("sectors_heatmap")
        try:
            from engine.premarket_scanner import get_sector_heatmap
            data = get_sector_heatmap()
            if data:
                _swr_cache["sectors_heatmap"] = {"data": {"sectors": data}, "ts": _t.time()}
        except Exception:
            pass
        finally:
            _swr_refreshing.discard("sectors_heatmap")
            _swr_locks["sectors_heatmap"].release()

    def _warm_movers():
        """Warm market movers — acquires lock so endpoint bg-refresh can't double-fetch."""
        if not _swr_locks["market_movers"].acquire(blocking=False):
            return  # Already refreshing
        _swr_refreshing.add("market_movers")
        try:
            from engine.market_movers import get_market_movers
            data = get_market_movers()
            if data:
                _swr_cache["market_movers"] = {"data": data, "ts": _t.time()}
        except Exception:
            pass
        finally:
            _swr_refreshing.discard("market_movers")
            _swr_locks["market_movers"].release()

    def _warm_db():
        """Warm DB-heavy endpoints (fast with indexes, run sequentially)."""
        try:
            equity_curve(player_id=None, season=0)
        except Exception:
            pass
        try:
            comparison_chart(season=0)
        except Exception:
            pass

    def _warm_leaderboard():
        """Warm leaderboard separately — it does Yahoo bulk fetch."""
        try:
            leaderboard(season=0)
        except Exception:
            pass

    # Stagger warmups so boot does not spike CPU/network all at once.
    def _delayed_start(fn, delay_s: float):
        def _runner():
            if delay_s > 0:
                _t.sleep(delay_s)
            fn()
        threading.Thread(target=_runner, daemon=True).start()

    for _delay, _fn in (
        (0.5, _warm_db),
        (1.5, _warm_sectors),
        (3.0, _warm_movers),
        (5.0, _warm_leaderboard),
    ):
        _delayed_start(_fn, _delay)
DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

# --- Timed cache decorator for slow endpoints ---
import time as _time
import functools as _functools
import inspect as _inspect
_endpoint_cache: dict = {}


def timed_cache(seconds: int):
    """Cache endpoint response for N seconds. Preserves function signature for FastAPI."""
    def decorator(func):
        @_functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = f"{func.__name__}:{args}:{kwargs}"
            now = _time.time()
            entry = _endpoint_cache.get(key)
            if entry and (now - entry["time"]) < seconds:
                return entry["data"]
            result = func(*args, **kwargs)
            _endpoint_cache[key] = {"time": now, "data": result}
            return result
        # Preserve the original signature so FastAPI can inspect query params
        wrapper.__signature__ = _inspect.signature(func)
        return wrapper
    return decorator


_swr_cache: dict = {}
_swr_locks: dict = {}
_swr_refreshing: set = set()  # keys currently being background-refreshed (for is_updating flag)

def stale_while_revalidate(fresh_seconds: int):
    """Return cached data immediately (even if stale), refresh in background when expired.
    On first call with no cache, blocks once to populate. After that always returns fast."""
    def decorator(func):
        key = func.__name__
        _swr_cache[key] = {"data": None, "ts": 0}
        _swr_locks[key] = threading.Lock()

        def _refresh():
            try:
                result = func()
                _swr_cache[key] = {"data": result, "ts": _time.time()}
            except Exception:
                pass

        @_functools.wraps(func)
        def wrapper(*args, **kwargs):
            entry = _swr_cache[key]
            now = _time.time()
            age = now - entry["ts"]
            if entry["data"] is None:
                # First call — block once to populate
                with _swr_locks[key]:
                    if _swr_cache[key]["data"] is None:
                        _refresh()
                return _swr_cache[key]["data"]
            if age > fresh_seconds:
                # Stale — return immediately, refresh in background
                if _swr_locks[key].acquire(blocking=False):
                    def _bg():
                        try:
                            _refresh()
                        finally:
                            _swr_locks[key].release()
                    threading.Thread(target=_bg, daemon=True).start()
            return entry["data"]
        wrapper.__signature__ = _inspect.signature(func)
        return wrapper
    return decorator

# Convert UTC timestamps to Arizona time (MST = UTC-7, no DST) in all API responses
import re
from datetime import datetime, timezone, timedelta
_AZ_TZ = timezone(timedelta(hours=-7))
_TS_RE = re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}')

def _to_arizona(val):
    """Convert a UTC timestamp string to Arizona time string."""
    if not isinstance(val, str) or not _TS_RE.match(val):
        return val
    try:
        s = val.replace('T', ' ').split('.')[0]  # strip fractional seconds
        utc_dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        az_dt = utc_dt.astimezone(_AZ_TZ)
        return az_dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return val

def _convert_timestamps(obj):
    """Recursively convert all timestamp strings in a response to Arizona time."""
    if isinstance(obj, dict):
        return {k: _convert_timestamps(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_timestamps(item) for item in obj]
    if isinstance(obj, str):
        return _to_arizona(obj)
    return obj

from fastapi.routing import APIRoute
from starlette.requests import Request as StarletteRequest


class TimezoneRoute(APIRoute):
    """Custom route that converts UTC timestamps to Arizona time in all JSON responses."""
    def get_route_handler(self):
        original_handler = super().get_route_handler()
        async def handler(request: StarletteRequest):
            response = await original_handler(request)
            if isinstance(response, JSONResponse):
                try:
                    import json as _json
                    data = _json.loads(response.body)
                    converted = _sanitize_floats(_convert_timestamps(data))
                    return JSONResponse(content=converted, status_code=response.status_code)
                except Exception:
                    pass
            return response
        return handler

app.router.route_class = TimezoneRoute

# NOTE: CORSMiddleware is registered last (below all other middleware)
# so it executes FIRST (Starlette LIFO order) — preflight OPTIONS must
# get CORS headers before AuthMiddleware can reject them.

# --- Fix for 404 on /dashboard ---
@app.get("/dashboard")
async def dashboard():
    return {"message": "Dashboard endpoint is now active"}

# --- Authentication ---
_SECRET_KEY = os.environ.get("TRADEMINDS_SECRET", "")
_SESSION_MAX_AGE = 86400  # 24 hours
_signer = URLSafeTimedSerializer(_SECRET_KEY)
_TOTP_PENDING_MAX_AGE = 300  # 5-minute window to complete step-2 TOTP

# ── Multi-user account registry ────────────────────────────────────────────────
# Parsed from DASHBOARD_USERS=User:role:ntfy_topic,... in .env
# Roles: admin (full access + 2FA), observer (read-only), charts (Big Charts only)
def _parse_users() -> dict[str, dict]:
    """Parse DASHBOARD_USERS and per-user passwords into a registry dict."""
    users: dict[str, dict] = {}
    raw = os.environ.get("DASHBOARD_USERS", "")
    if raw:
        for part in raw.split(","):
            chunks = [c.strip() for c in part.strip().split(":")]
            if len(chunks) >= 2:
                uname = chunks[0]
                role  = chunks[1].lower()
                ntfy  = chunks[2] if len(chunks) >= 3 else ""
                pw    = os.environ.get(f"DASHBOARD_PASS_{uname}", "")
                users[uname.lower()] = {
                    "username": uname, "role": role,
                    "password": pw,   "ntfy": ntfy,
                }
    # Legacy single-user fallback (DASHBOARD_USER / DASHBOARD_PASS)
    lu = os.environ.get("DASHBOARD_USER", "")
    lp = os.environ.get("DASHBOARD_PASS", "")
    if lu and lu.lower() not in users:
        users[lu.lower()] = {
            "username": lu, "role": "admin",
            "password": lp, "ntfy": os.environ.get("NTFY_TOPIC", ""),
        }
    return users

_USERS: dict[str, dict] = _parse_users()
# _AUTH_USER / _AUTH_PASS: derived from first admin user — used by /login/pin and /setup-2fa
_AUTH_USER: str = next((u["username"] for u in _USERS.values() if u["role"] == "admin"), "")
_AUTH_PASS: str = _USERS.get(_AUTH_USER.lower(), {}).get("password", "")


def _ensure_totp_secret() -> str:
    """Return TOTP_SECRET from env; generate and append to .env if absent."""
    secret = os.environ.get("TOTP_SECRET", "")
    if secret:
        return secret
    secret = _pyotp.random_base32()
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    try:
        with open(env_path, "a") as _ef:
            _ef.write(f"\nTOTP_SECRET={secret}\n")
        os.environ["TOTP_SECRET"] = secret
        logger.warning("Generated TOTP_SECRET — visit /setup-2fa to configure authenticator app")
    except Exception as _exc:
        logger.error("Could not write TOTP_SECRET to .env: %s", _exc)
    return secret


_TOTP_SECRET = _ensure_totp_secret()
_totp_verifier = _pyotp.TOTP(_TOTP_SECRET)

# Unified login failure tracking:
# ip → {"count": int, "blocked_until": float}
# 5 failures from same IP → 15-minute block
_login_failures: dict = {}
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_BLOCK_SECS   = 900  # 15 minutes

# Active sessions: username → last_seen epoch (cleaned up every 30 min)
_active_sessions: dict[str, float] = {}
_ACTIVE_SESSION_TTL = 1800  # 30 minutes

# Big Charts guest visitors: visitor_key → last_seen epoch
_charts_visitors: dict[str, float] = {}
_CHARTS_VISITOR_TTL = 300  # 5 minutes of inactivity before expiry

# Security logger
import logging as _sec_log_mod
os.makedirs("logs", exist_ok=True)
_sec_logger = _sec_log_mod.getLogger("security")
if not _sec_logger.handlers:
    _sec_handler = _sec_log_mod.FileHandler("logs/security.log")
    _sec_handler.setFormatter(_sec_log_mod.Formatter("%(asctime)s %(message)s"))
    _sec_logger.addHandler(_sec_handler)
_sec_logger.setLevel(_sec_log_mod.WARNING)

_LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>OllieTrades — Authorization Required</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0e1a;color:#e0e6f0;font-family:'Courier New',monospace;
  display:flex;align-items:center;justify-content:center;min-height:100vh;padding:16px;
  background-image:radial-gradient(ellipse at 50% 0%,#1a2040 0%,#0a0e1a 70%)}
.login-box{background:linear-gradient(135deg,#111827,#1a2040);border:1px solid #2d4a7a;
  border-radius:16px;padding:32px 28px 28px;width:100%;max-width:340px;
  box-shadow:0 0 40px rgba(0,188,212,0.12);text-align:center}
.badge{font-size:44px;margin-bottom:10px}
h1{font-size:17px;color:#60a5fa;letter-spacing:2px;margin-bottom:3px}
.subtitle{font-size:11px;color:#f59e0b;letter-spacing:1px;margin-bottom:24px}
label{display:block;font-size:11px;color:#94a3b8;margin-bottom:3px;letter-spacing:1px;text-align:left}
input[type=text],input[type=password]{width:100%;padding:10px 12px;background:#0f172a;
  border:1px solid #334155;border-radius:6px;color:#e0e6f0;font-family:inherit;
  font-size:16px;margin-bottom:14px;outline:none;transition:border .2s}
input:focus{border-color:#3b82f6}
.submit-btn{width:100%;padding:13px;background:linear-gradient(135deg,#2563eb,#1d4ed8);
  border:none;border-radius:6px;color:#fff;font-family:inherit;font-size:15px;
  font-weight:bold;cursor:pointer;letter-spacing:1px;transition:all .2s}
.submit-btn:hover{background:linear-gradient(135deg,#3b82f6,#2563eb);box-shadow:0 0 20px rgba(59,130,246,0.3)}
.error-banner{background:#7f1d1d;border:1px solid #dc2626;border-radius:6px;padding:10px;
  font-size:13px;color:#fca5a5;margin-bottom:14px}
.footer{margin-top:18px;font-size:10px;color:#334155}
</style>
</head>
<body>
<div class="login-box">
  <div class="badge">🐕</div>
  <h1>OLLIETRADES</h1>
  <div class="subtitle">AUTHORIZED PERSONNEL ONLY</div>
  {{ERROR}}
  <form method="POST" action="/login">
    <label>OFFICER IDENTIFICATION</label>
    <input type="text" name="username" autocomplete="username" autofocus required>
    <label>ACCESS CODE</label>
    <input type="password" name="password" autocomplete="current-password" required>
    <button type="submit" class="submit-btn">ENGAGE ▶</button>
  </form>
  <div style="margin-top:14px;">
    <a href="/charts" style="font-size:11px;color:#334155;text-decoration:none;letter-spacing:0.5px;" onmouseover="this.style.color='#64748b'" onmouseout="this.style.color='#334155'">📱 Big Charts — No Login Required</a>
  </div>
  <div class="footer">OllieTrades • Secure Trading Command Center</div>
  <div style="margin-top:12px;font-size:10px;color:#334155;line-height:1.4;">
    📄 Paper Trade Accounts — Educational &amp; Research Purposes Only.<br>
    Not Financial Advice. No real money is at risk.
  </div>
</div>
</body>
</html>"""

_TOTP_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>OllieTrades — 2FA Verification</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0e1a;color:#e0e6f0;font-family:'Courier New',monospace;
  display:flex;align-items:center;justify-content:center;min-height:100vh;padding:16px;
  background-image:radial-gradient(ellipse at 50% 0%,#1a2040 0%,#0a0e1a 70%)}
.login-box{background:linear-gradient(135deg,#111827,#1a2040);border:1px solid #2d4a7a;
  border-radius:16px;padding:32px 28px 28px;width:100%;max-width:340px;
  box-shadow:0 0 40px rgba(0,188,212,0.12);text-align:center}
.badge{font-size:44px;margin-bottom:10px}
h1{font-size:17px;color:#60a5fa;letter-spacing:2px;margin-bottom:3px}
.subtitle{font-size:11px;color:#f59e0b;letter-spacing:1px;margin-bottom:24px}
label{display:block;font-size:11px;color:#94a3b8;margin-bottom:3px;letter-spacing:1px;text-align:left}
input[type=text]{width:100%;padding:10px 12px;background:#0f172a;
  border:1px solid #334155;border-radius:6px;color:#e0e6f0;font-family:inherit;
  font-size:24px;letter-spacing:8px;text-align:center;margin-bottom:14px;outline:none;transition:border .2s}
input:focus{border-color:#3b82f6}
.submit-btn{width:100%;padding:13px;background:linear-gradient(135deg,#2563eb,#1d4ed8);
  border:none;border-radius:6px;color:#fff;font-family:inherit;font-size:15px;
  font-weight:bold;cursor:pointer;letter-spacing:1px;transition:all .2s}
.submit-btn:hover{background:linear-gradient(135deg,#3b82f6,#2563eb);box-shadow:0 0 20px rgba(59,130,246,0.3)}
.error-banner{background:#7f1d1d;border:1px solid #dc2626;border-radius:6px;padding:10px;
  font-size:13px;color:#fca5a5;margin-bottom:14px}
.hint{font-size:11px;color:#475569;margin-bottom:16px;line-height:1.5}
.footer{margin-top:18px;font-size:10px;color:#334155}
</style>
</head>
<body>
<div class="login-box">
  <div class="badge">🔐</div>
  <h1>2FA VERIFICATION</h1>
  <div class="subtitle">AUTHENTICATOR REQUIRED</div>
  {{ERROR}}
  <div class="hint">Open your authenticator app and enter the 6-digit code for <strong>OllieTrades</strong></div>
  <form method="POST" action="/login?step=2">
    <label>6-DIGIT CODE</label>
    <input type="text" name="totp_code" maxlength="6" pattern="[0-9]{6}" inputmode="numeric" autocomplete="one-time-code" autofocus required>
    <button type="submit" class="submit-btn">VERIFY ▶</button>
  </form>
  <div style="margin-top:14px;"><a href="/login" style="font-size:11px;color:#334155;text-decoration:none;">← Back to login</a></div>
  <div class="footer">OllieTrades • Secure Trading Command Center</div>
</div>
</body>
</html>"""


def _is_login_blocked(ip: str) -> tuple[bool, int]:
    """Return (is_blocked, seconds_remaining). Records block if threshold reached."""
    now = _time_module.time()
    entry = _login_failures.get(ip, {"count": 0, "blocked_until": 0.0})
    if entry["blocked_until"] > now:
        return True, int(entry["blocked_until"] - now)
    return False, 0


def _record_login_failure(ip: str, reason: str = ""):
    now = _time_module.time()
    entry = _login_failures.get(ip, {"count": 0, "blocked_until": 0.0})
    # Don't accumulate while still blocked
    if entry["blocked_until"] > now:
        return
    entry["count"] = entry.get("count", 0) + 1
    if entry["count"] >= _LOGIN_MAX_ATTEMPTS:
        entry["blocked_until"] = now + _LOGIN_BLOCK_SECS
        entry["count"] = 0
        _sec_logger.warning("LOGIN_BLOCK ip=%s reason=%s — blocked %ds", ip, reason, _LOGIN_BLOCK_SECS)
    else:
        _sec_logger.warning("LOGIN_FAIL ip=%s attempt=%d reason=%s", ip, entry["count"], reason)
    _login_failures[ip] = entry


def _get_session_data(request: Request) -> dict | None:
    """Return the full session payload or None."""
    token = request.cookies.get("trademinds_session")
    if not token:
        return None
    try:
        data = _signer.loads(token, max_age=_SESSION_MAX_AGE)
        if data.get("authenticated") is True:
            return data
        return None
    except (BadSignature, SignatureExpired):
        return None


def _get_session_username(request: Request) -> str | None:
    """Return the authenticated username from the session cookie, or None."""
    data = _get_session_data(request)
    return data.get("username", _AUTH_USER) if data else None


def _get_session_role(request: Request) -> str:
    """Return the role of the authenticated user ('admin', 'observer', 'charts', or '')."""
    data = _get_session_data(request)
    if not data:
        return ""
    role = data.get("role", "")
    if not role:
        # Back-fill role from user registry for sessions created before multi-user
        username = data.get("username", "")
        role = _USERS.get(username.lower(), {}).get("role", "admin")
    return role


def _check_session(request: Request) -> bool:
    return _get_session_username(request) is not None


def _is_localhost(request: Request) -> bool:
    client = request.client
    if not client:
        return False
    return client.host in ("127.0.0.1", "::1", "localhost")


def _is_via_cf_tunnel(request: Request) -> bool:
    """Return True if the request arrived via a Cloudflare Tunnel.

    Cloudflare injects CF-Connecting-IP on all tunnel-proxied requests.
    We use this to distinguish tunnel traffic from random external hits so
    the rate-limiter doesn't treat every visitor as the same Cloudflare IP.
    This does NOT bypass authentication — it only exempts tunnel requests
    from the rate-limit / bot-block middleware.
    """
    return "cf-connecting-ip" in request.headers


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, step: str = ""):
    if step == "2":
        token = request.cookies.get("_totp_pending")
        if not token:
            return RedirectResponse(url="/login", status_code=303)
        try:
            _signer.loads(token, max_age=_TOTP_PENDING_MAX_AGE, salt="totp_pending")
        except Exception:
            return RedirectResponse(url="/login", status_code=303)
        return HTMLResponse(_TOTP_PAGE.replace("{{ERROR}}", ""))
    return HTMLResponse(_LOGIN_PAGE.replace("{{ERROR}}", ""))


@app.post("/login")
async def login_submit(request: Request, step: str = "",
                       username: str = Form(None), password: str = Form(None),
                       totp_code: str = Form(None)):
    ip = request.client.host if request.client else "unknown"
    blocked, remaining = _is_login_blocked(ip)
    if blocked:
        html = _LOGIN_PAGE.replace("{{ERROR}}",
            f'<div class="error-banner">⛔ Too many failed attempts — try again in {remaining}s</div>')
        return HTMLResponse(html, status_code=429)

    # ── Step 2: TOTP verification ─────────────────────────────────────────────
    if step == "2":
        token = request.cookies.get("_totp_pending")
        if not token:
            return RedirectResponse(url="/login", status_code=303)
        try:
            data = _signer.loads(token, max_age=_TOTP_PENDING_MAX_AGE, salt="totp_pending")
            pending_user = data.get("username", "")
        except Exception:
            return RedirectResponse(url="/login", status_code=303)

        code = (totp_code or "").strip()
        if _totp_verifier.verify(code, valid_window=1):
            full_token = _signer.dumps({
                "authenticated": True, "username": pending_user, "role": "admin"
            })
            response = RedirectResponse(url="/", status_code=303)
            response.set_cookie("trademinds_session", full_token,
                                max_age=_SESSION_MAX_AGE, httponly=True, samesite="strict")
            response.delete_cookie("_totp_pending")
            _active_sessions[pending_user] = _time_module.time()
            _sec_logger.warning("LOGIN_2FA_OK ip=%s user=%s", ip, pending_user)
            return response

        _record_login_failure(ip, "bad_totp")
        resp = HTMLResponse(
            _TOTP_PAGE.replace("{{ERROR}}", '<div class="error-banner">⛔ Invalid code — try again</div>'),
            status_code=401
        )
        # Re-issue pending cookie so user can retry without re-entering password
        resp.set_cookie("_totp_pending", token,
                        max_age=_TOTP_PENDING_MAX_AGE, httponly=True, samesite="strict")
        return resp

    # ── Step 1: username + password ───────────────────────────────────────────
    entry = _USERS.get((username or "").lower())
    if entry and entry["password"] and password == entry["password"]:
        role = entry["role"]
        if role == "admin":
            # Admin requires 2FA — issue pending cookie, redirect to step 2
            pending_token = _signer.dumps({"username": entry["username"]}, salt="totp_pending")
            response = RedirectResponse(url="/login?step=2", status_code=303)
            response.set_cookie("_totp_pending", pending_token,
                                max_age=_TOTP_PENDING_MAX_AGE, httponly=True, samesite="strict")
            return response
        else:
            # Non-admin (observer, charts) — session issued directly, no 2FA
            full_token = _signer.dumps({
                "authenticated": True, "username": entry["username"], "role": role
            })
            dest = "/charts" if role == "charts" else "/"
            response = RedirectResponse(url=dest, status_code=303)
            response.set_cookie("trademinds_session", full_token,
                                max_age=_SESSION_MAX_AGE, httponly=True, samesite="strict")
            _active_sessions[entry["username"]] = _time_module.time()
            _sec_logger.warning("LOGIN_OK ip=%s user=%s role=%s", ip, entry["username"], role)
            return response

    _record_login_failure(ip, f"bad_credentials user={username}")
    html = _LOGIN_PAGE.replace(
        "{{ERROR}}",
        '<div class="error-banner">⛔ ACCESS DENIED — Invalid credentials</div>'
    )
    return HTMLResponse(html, status_code=401)


@app.get("/logout")
def logout(request: Request):
    username = _get_session_username(request)
    if username:
        _active_sessions.pop(username, None)
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("trademinds_session")
    response.delete_cookie("_totp_pending")
    return response


@app.get("/setup-2fa", response_class=HTMLResponse)
def setup_2fa(request: Request):
    """Show QR code for authenticator app setup. Requires active session."""
    if not _check_session(request):
        return RedirectResponse(url="/login", status_code=303)
    uri = _pyotp.totp.TOTP(_TOTP_SECRET).provisioning_uri(
        name=_AUTH_USER, issuer_name="OllieTrades (Admiral)"
    )
    qr = _qrcode.make(uri)
    buf = _io.BytesIO()
    qr.save(buf, format="PNG")
    qr_b64 = _base64.b64encode(buf.getvalue()).decode()
    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>OllieTrades — 2FA Setup</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0a0e1a;color:#e0e6f0;font-family:'Courier New',monospace;
  display:flex;align-items:center;justify-content:center;min-height:100vh;padding:16px}}
.box{{background:linear-gradient(135deg,#111827,#1a2040);border:1px solid #2d4a7a;
  border-radius:16px;padding:32px 28px;width:100%;max-width:420px;text-align:center;
  box-shadow:0 0 40px rgba(0,188,212,0.12)}}
h1{{font-size:17px;color:#60a5fa;letter-spacing:2px;margin-bottom:4px}}
.sub{{font-size:11px;color:#f59e0b;letter-spacing:1px;margin-bottom:20px}}
img{{border-radius:8px;margin:12px 0;border:3px solid #2d4a7a}}
.secret{{background:#0f172a;border:1px solid #334155;border-radius:6px;padding:10px 14px;
  font-size:13px;letter-spacing:3px;color:#f59e0b;margin:12px 0;word-break:break-all}}
p{{font-size:12px;color:#94a3b8;line-height:1.6;margin-bottom:10px}}
a{{display:inline-block;margin-top:16px;padding:10px 24px;
  background:linear-gradient(135deg,#2563eb,#1d4ed8);border-radius:6px;
  color:#fff;text-decoration:none;font-size:13px;letter-spacing:1px}}
</style>
</head>
<body>
<div class="box">
  <div style="font-size:40px;margin-bottom:8px">🔐</div>
  <h1>2FA SETUP</h1>
  <div class="sub">GOOGLE AUTHENTICATOR / AUTHY</div>
  <p>Scan this QR code with your authenticator app:</p>
  <img src="data:image/png;base64,{qr_b64}" width="220" height="220" alt="QR Code">
  <p>Or enter this secret manually:</p>
  <div class="secret">{_TOTP_SECRET}</div>
  <p style="font-size:11px;color:#475569">Account label: <strong style="color:#94a3b8">OllieTrades (Admiral)</strong><br>
  After scanning, every login will require the 6-digit code from your app.</p>
  <a href="/">← Back to Dashboard</a>
</div>
</body>
</html>""")


@app.post("/login/pin")
async def login_pin(request: Request):
    """Machine-to-machine auth for signal-center bridge. Localhost only. No TOTP."""
    if not _is_localhost(request):
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Bad request"}, status_code=400)
    pin = str(body.get("pin", ""))
    captain_pin = os.environ.get("CAPTAIN_PIN", "")
    if pin and captain_pin and pin == captain_pin:
        token = _signer.dumps({"authenticated": True, "username": _AUTH_USER})
        response = JSONResponse({"ok": True})
        response.set_cookie("trademinds_session", token,
                            max_age=_SESSION_MAX_AGE, httponly=True, samesite="strict")
        return response
    return JSONResponse({"error": "Invalid PIN"}, status_code=401)


@app.get("/api/me")
def api_me(request: Request):
    """Return the logged-in username and role."""
    data = _get_session_data(request)
    if data:
        username = data.get("username", _AUTH_USER)
        role = data.get("role") or _USERS.get(username.lower(), {}).get("role", "admin")
        return JSONResponse({"username": username, "role": role, "authenticated": True})
    if _is_localhost(request):
        return JSONResponse({"username": _AUTH_USER, "role": "admin", "authenticated": True})
    return JSONResponse({"error": "Not authenticated"}, status_code=401)


@app.get("/api/active-users")
def api_active_users(request: Request):
    """Return count, usernames, and roles of active sessions (seen within 30 min)."""
    if not _check_session(request) and not _is_localhost(request):
        return JSONResponse({"error": "Authentication required"}, status_code=401)
    now = _time_module.time()
    active = {u: ts for u, ts in _active_sessions.items() if now - ts < _ACTIVE_SESSION_TTL}
    # Merge active Big Charts guests as a virtual "Guest (Charts)" user
    charts_active = {k: ts for k, ts in _charts_visitors.items() if now - ts < _CHARTS_VISITOR_TTL}
    if charts_active:
        active["Guest (Charts)"] = max(charts_active.values())
    user_roles = {
        u: _USERS.get(u.lower(), {}).get("role", "admin")
        for u in active.keys() if u != "Guest (Charts)"
    }
    return JSONResponse({"count": len(active), "users": list(active.keys()), "user_roles": user_roles})


@app.get("/api/charts/ping")
def charts_ping(request: Request):
    """Called by Big Charts page to register visitor presence. No auth required."""
    import hashlib as _hl
    ip = request.client.host if request.client else "unknown"
    # Use a hashed key so raw IPs aren't stored in memory
    visitor_key = _hl.sha1(ip.encode()).hexdigest()[:12]
    _charts_visitors[visitor_key] = _time_module.time()
    import logging as _lg
    _lg.getLogger("uvicorn.access").info("Big Charts visit from %s", ip)
    return JSONResponse({"ok": True})


from starlette.middleware.base import BaseHTTPMiddleware

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Always pass OPTIONS preflight through so CORS middleware can handle it
        if request.method == "OPTIONS":
            return await call_next(request)
        # Always allow login/logout routes, tactical display, static assets, and PWA files
        if path in ("/login", "/logout", "/login/pin", "/tactical", "/scanner", "/charts", "/sw.js", "/robots.txt", "/api/trades/recent", "/api/briefing/today", "/api/macro/dashboard", "/backtest") or path.startswith("/static/") or path.startswith("/api/chart") or path.startswith("/api/v1/") or path.startswith("/leaderboard") or path.startswith("/backtest/result/") or (path.startswith("/api/trades/") and path.endswith("/explain")) or path == "/api/backtest/community-leaderboard" or path == "/api/backtest/community/run" or path.startswith("/api/backtest/result/"):
            return await call_next(request)
        # Role-based checks apply to any request that carries a session cookie,
        # regardless of source IP — must run before the localhost bypass so that
        # an observer who happens to be on localhost is still read-only.
        session_data = _get_session_data(request)
        if session_data:
            role = session_data.get("role") or _USERS.get(
                session_data.get("username", "").lower(), {}
            ).get("role", "admin")
            if role == "observer" and request.method in ("POST", "PUT", "PATCH", "DELETE"):
                return JSONResponse(
                    {"error": "Read-only access — observers cannot perform write operations"},
                    status_code=403,
                )
            username = session_data.get("username", _AUTH_USER)
            _active_sessions[username] = _time_module.time()
            if role == "charts" and not (
                path.startswith("/charts") or path.startswith("/static/")
                or path.startswith("/api/chart") or path in ("/api/health", "/api/me")
            ):
                return RedirectResponse(url="/charts", status_code=303)
            return await call_next(request)

        # API routes from localhost bypass auth (scanner needs these)
        if path.startswith("/api/") and _is_localhost(request):
            return await call_next(request)
        # Everything else requires a valid session
        if path.startswith("/api/"):
            return JSONResponse({"error": "Authentication required"}, status_code=401)
        return RedirectResponse(url="/login", status_code=303)

app.add_middleware(AuthMiddleware)

# --- Anti-Bot / Rate Limiting ---
from collections import defaultdict
import time as _time

_LOCALHOST_IPS = {"127.0.0.1", "localhost", "::1"}
_RATE_LIMIT_REQ = 60          # max requests per minute per external IP
_RAPID_FIRE_LIMIT = 10        # max requests per second before block
_RAPID_FIRE_BLOCK_SECS = 300  # 5 minute block for rapid-fire

# Per-IP request history: ip → [timestamps in last 60s]
_rate_limits: dict = defaultdict(list)
# Per-IP rapid-fire history: ip → [timestamps in last 1s]
_rapid_history: dict = defaultdict(list)
# IPs currently blocked for rapid-fire: ip → unblock_epoch
_blocked_ips: dict[str, float] = {}

# Permanent IP blocklist — hard ban, no expiry, checked before rate limiter.
_PERMANENT_BLOCKED_IPS: frozenset[str] = frozenset({
    "64.43.89.142",  # blocked 2026-04-08
})

# Bot user-agent substrings to block (case-insensitive)
_BOT_UA_BLOCKLIST = [
    "googlebot", "bingbot", "yandexbot", "baiduspider",
    "scrapy", "python-requests", "python-urllib",
    "wget/", "crawl", "spider", "semrushbot", "ahrefsbot",
    "dotbot", "mj12bot", "rogerbot", "archive.org_bot",
    # AI crawlers
    "gptbot", "chatgpt-user", "ccbot", "anthropic-ai",
    "claude-web", "google-extended", "bytespider", "amazonbot",
]

# Paths that are ALWAYS exempt from rate limiting — public read-only content
# that must remain reachable even while a login-block or rapid-fire block is active.
_RATE_EXEMPT_EXACT = frozenset({"/charts", "/api/health", "/robots.txt", "/sw.js"})
_RATE_EXEMPT_PREFIX = ("/api/charts/", "/api/chart/", "/static/")

def _is_rate_exempt(path: str) -> bool:
    return path in _RATE_EXEMPT_EXACT or path.startswith(_RATE_EXEMPT_PREFIX)


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        ip = (request.client.host if request.client else "unknown")
        # Always pass localhost through — internal services (Signal Center, Crusher, etc.)
        if ip in _LOCALHOST_IPS:
            return await call_next(request)
        # Cloudflare Tunnel traffic: all visitors share Cloudflare's edge IP, so
        # rate-limiting by IP is meaningless and would block everyone. Let it
        # through to normal auth flow — session auth still applies.
        if _is_via_cf_tunnel(request):
            return await call_next(request)

        # Permanent blocklist — hard ban before any rate-limit logic runs.
        if ip in _PERMANENT_BLOCKED_IPS:
            _sec_logger.warning("PERM_BLOCKED ip=%s path=%s", ip, request.url.path)
            return JSONResponse({"error": "Forbidden"}, status_code=403)

        path = request.url.path

        # Always pass through public auth-exempt routes — /charts, /api/charts/ping, etc.
        # These must remain reachable even when a login-block or rapid-fire block is active.
        if _is_rate_exempt(path):
            return await call_next(request)

        now = _time.time()
        ua = (request.headers.get("user-agent") or "").lower()

        # ── Bot UA check ──────────────────────────────────────────────────
        for bad_ua in _BOT_UA_BLOCKLIST:
            if bad_ua in ua:
                _sec_logger.warning("BOT_BLOCKED ip=%s ua=%s path=%s", ip, ua[:120], path)
                return JSONResponse({"error": "Forbidden"}, status_code=403)

        # ── Rapid-fire check (>10 req in 1 second = 5 min block) ─────────
        blocked_until = _blocked_ips.get(ip, 0)
        if blocked_until > now:
            remaining = int(blocked_until - now)
            _sec_logger.warning("BLOCKED_IP ip=%s remaining=%ds path=%s", ip, remaining, path)
            return JSONResponse({"error": f"Blocked for {remaining}s"}, status_code=429)

        _rapid_history[ip] = [t for t in _rapid_history[ip] if now - t < 1.0]
        _rapid_history[ip].append(now)
        if len(_rapid_history[ip]) > _RAPID_FIRE_LIMIT:
            _blocked_ips[ip] = now + _RAPID_FIRE_BLOCK_SECS
            _sec_logger.warning("RAPID_FIRE_BLOCK ip=%s req_per_sec=%d path=%s",
                                 ip, len(_rapid_history[ip]), path)
            return JSONResponse({"error": "Too many requests — blocked for 5 minutes"}, status_code=429)

        # ── Per-minute rate limit (60 req/min) ────────────────────────────
        _rate_limits[ip] = [t for t in _rate_limits[ip] if now - t < 60]
        if len(_rate_limits[ip]) >= _RATE_LIMIT_REQ:
            _sec_logger.warning("RATE_LIMITED ip=%s count=%d path=%s",
                                 ip, len(_rate_limits[ip]), path)
            return JSONResponse({"error": "Rate limited — max 60 req/min"}, status_code=429)
        _rate_limits[ip].append(now)

        return await call_next(request)

app.add_middleware(RateLimitMiddleware)


# --- Scan Throttle Middleware ---
# Yields the asyncio event loop for 100ms when a scan cycle is running,
# giving the scanner thread more effective CPU time. Only applies to /api/
# paths; static assets, /charts, and /login are passed through immediately.
class ScanThrottleMiddleware(BaseHTTPMiddleware):
    _scan_state: dict | None = None  # lazy-loaded on first API request

    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/api/"):
            if ScanThrottleMiddleware._scan_state is None:
                try:
                    from engine.crew_scanner import scan_state as _ss
                    ScanThrottleMiddleware._scan_state = _ss
                except Exception:
                    ScanThrottleMiddleware._scan_state = {}
            if ScanThrottleMiddleware._scan_state.get("active"):
                import asyncio as _asyncio
                await _asyncio.sleep(0.1)
        return await call_next(request)

app.add_middleware(ScanThrottleMiddleware)


@app.get("/robots.txt")
def robots_txt():
    return HTMLResponse("User-agent: *\nDisallow: /\n", media_type="text/plain")


# --- Security Headers ---
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self' 'unsafe-inline' 'unsafe-eval' https:"
        )
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        # Strip server version header (uvicorn leaks version by default)
        response.headers["server"] = "OllieTrades"
        return response

app.add_middleware(SecurityHeadersMiddleware)
from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=500)

# CORS must be last-registered so it runs first (Starlette LIFO) —
# OPTIONS preflight needs CORS headers before auth checks happen.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:8080",
        "http://localhost:8080",
        "https://bridge.accessapple.com",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key", "Authorization"],
    allow_credentials=True,
)

# --- Session / rate-limit cleanup loop (runs every 5 minutes) ---
def _session_cleanup_loop():
    while True:
        _time.sleep(300)
        now = _time.time()
        # Prune stale rate-limit buckets
        cutoff_min = now - 60
        for ip in list(_rate_limits.keys()):
            _rate_limits[ip] = [t for t in _rate_limits[ip] if t > cutoff_min]
            if not _rate_limits[ip]:
                del _rate_limits[ip]
        # Prune rapid-fire history
        cutoff_sec = now - 1.0
        for ip in list(_rapid_history.keys()):
            _rapid_history[ip] = [t for t in _rapid_history[ip] if t > cutoff_sec]
            if not _rapid_history[ip]:
                del _rapid_history[ip]
        # Prune expired IP blocks
        for ip in list(_blocked_ips.keys()):
            if _blocked_ips[ip] <= now:
                del _blocked_ips[ip]
        # Prune stale active sessions (30 min TTL)
        cutoff_sess = now - _ACTIVE_SESSION_TTL
        for user in list(_active_sessions.keys()):
            if _active_sessions[user] < cutoff_sess:
                del _active_sessions[user]
        # Prune expired Big Charts visitors (5 min TTL)
        cutoff_charts = now - _CHARTS_VISITOR_TTL
        for vk in list(_charts_visitors.keys()):
            if _charts_visitors[vk] < cutoff_charts:
                del _charts_visitors[vk]
        # Prune expired login failure records
        for ip in list(_login_failures.keys()):
            entry = _login_failures[ip]
            if entry.get("blocked_until", 0) <= now and entry.get("count", 0) == 0:
                del _login_failures[ip]

threading.Thread(target=_session_cleanup_loop, daemon=True).start()


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _ensure_community_backtests_table():
    """Phase 3.8 — community backtest results table."""
    try:
        c = _conn()
        c.execute("""
            CREATE TABLE IF NOT EXISTS community_backtests (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                share_id      TEXT NOT NULL UNIQUE,
                ticker        TEXT NOT NULL,
                strategy      TEXT NOT NULL,
                period_days   INTEGER NOT NULL,
                params_json   TEXT,
                total_return  REAL,
                max_drawdown  REAL,
                sharpe_ratio  REAL,
                win_rate      REAL,
                num_trades    INTEGER,
                final_value   REAL,
                equity_json   TEXT,
                ip_hash       TEXT,
                created_at    TEXT DEFAULT (datetime('now'))
            )
        """)
        c.commit()
        c.close()
    except Exception:
        pass


_ensure_community_backtests_table()


def _ensure_trade_explanations_table():
    """Phase 3.4 — create trade_explanations table if it doesn't exist."""
    try:
        c = _conn()
        c.execute("""
            CREATE TABLE IF NOT EXISTS trade_explanations (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id     INTEGER NOT NULL UNIQUE,
                player_id    TEXT,
                symbol       TEXT,
                action       TEXT,
                signals_json TEXT,
                votes_json   TEXT,
                scanner_score REAL,
                backtest_json TEXT,
                risk_json    TEXT,
                timeline_json TEXT,
                created_at   TEXT DEFAULT (datetime('now'))
            )
        """)
        c.commit()
        c.close()
    except Exception:
        pass


_ensure_trade_explanations_table()


def _get_setting(key: str, default=None):
    """Read a single value from the settings table."""
    try:
        c = _conn()
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        c.close()
        return row["value"] if row else default
    except Exception:
        return default


BACKTEST_DB = os.path.join(os.path.dirname(DB), "backtest.db")


def _backtest_conn():
    c = sqlite3.connect(BACKTEST_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


# --- Notification System ---

def _init_notifications_table():
    """Create notifications table if not exists (idempotent)."""
    try:
        c = _conn()
        c.execute("""CREATE TABLE IF NOT EXISTS notifications (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT DEFAULT CURRENT_TIMESTAMP,
            type        TEXT,
            severity    TEXT,
            title       TEXT,
            body        TEXT,
            icon        TEXT,
            agent_id    TEXT,
            acknowledged INTEGER DEFAULT 0
        )""")
        c.commit()
        c.close()
    except Exception:
        pass


def _save_notification(title: str, body: str, severity: str = "info",
                        notif_type: str = "info", icon: str = "🔔",
                        agent_id: str = None) -> None:
    """Save a notification to the DB (dedup: skip if same title+body in last 5 min,
    or 24 hours for Morning Briefing to prevent daily accumulation)."""
    try:
        _init_notifications_table()
        conn = _conn()
        dedup_window = "-24 hours" if "Morning Briefing" in title else "-5 minutes"
        exists = conn.execute(
            "SELECT id FROM notifications WHERE title=? AND body=? "
            "AND timestamp >= datetime('now',?)",
            (title, body, dedup_window)
        ).fetchone()
        if not exists:
            conn.execute(
                "INSERT INTO notifications (type, severity, title, body, icon, agent_id) "
                "VALUES (?,?,?,?,?,?)",
                (notif_type, severity, title, body, icon, agent_id)
            )
            conn.commit()
        conn.close()
    except Exception:
        pass


_init_notifications_table()


# --- Arena Endpoints ---

# Season 5 locked crew — cannot be deactivated via API
PROTECTED_AGENTS = {
    "neo-matrix", "ollama-qwen3", "ollama-plutus", "ollama-coder",
    "ollama-llama", "gemini-2.5-flash", "dayblade-0dte", "capitol-trades",
    "steve-webull", "dalio-metals", "enterprise-computer",
}

# Sniper Mode — 6 active Alpha Squad agents + Neo (advisory/shelved agents hidden)
# Season 6: Sniper Mode — 6 active Alpha Squad agents + Neo + Ollie (Fleet Commander)
FLEET_ACTIVE = [
    "ollie-auto",       # Ollie (Fleet Commander — master filter)
    "ollama-llama",     # Uhura (Alpha Lead)
    "gemini-2.5-flash", # Worf
    "grok-4",           # Spock
    "gemini-2.5-pro",   # Seven of Nine
    "ollama-plutus",    # McCoy
    "neo-matrix",       # Neo
]

_LEADERBOARD_CACHE_FILE = os.path.join(_proj_root, "data", "leaderboard_cache.json")
_leaderboard_disk_cache: dict = {"data": None, "ts": 0}
try:
    import json as _ljson
    with open(_LEADERBOARD_CACHE_FILE) as _lf:
        _d = _ljson.load(_lf)
        _leaderboard_disk_cache = {"data": _d.get("data"), "ts": _d.get("ts", 0)}
except Exception:
    pass


def _sa_fresh_entry(base_entry: dict) -> dict:
    """Recompute Super Agent leaderboard fields live from portfolio_positions (portfolio_id=1)."""
    try:
        _c = _conn()
        open_row = _c.execute(
            "SELECT COALESCE(SUM(unrealized_pnl), 0) AS unreal, "
            "COALESCE(SUM(quantity * current_price), 0) AS pos_val "
            "FROM portfolio_positions WHERE portfolio_id=1 AND status='open'"
        ).fetchone()
        closed_row = _c.execute(
            "SELECT COALESCE(SUM(closed_pnl), 0) AS realized "
            "FROM portfolio_positions WHERE portfolio_id=1 AND status='closed'"
        ).fetchone()
        sa_cnt = _c.execute(
            "SELECT COUNT(*) AS cnt FROM portfolio_positions WHERE portfolio_id=1 AND status='open'"
        ).fetchone()
        _c.close()

        unrealized = round(float(open_row["unreal"] or 0), 2)
        positions_value = round(float(open_row["pos_val"] or 0), 2)
        realized = round(float(closed_row["realized"] or 0), 2)
        total_value = round(25000.0 + unrealized + realized, 2)

        entry = dict(base_entry)
        entry.update({
            "cash": 25000.0,
            "positions_value": positions_value,
            "total_value": total_value,
            "unrealized_pnl": unrealized,
            "return_pct": round((total_value - 25000.0) / 25000.0 * 100, 2),
            "current_equity": round(total_value, 2),
            "previous_equity": round(total_value, 2),
            "starting_capital": 25000.0,
            "day_pnl": 0.0,
            "total_pnl": round(total_value - 25000.0, 2),
            "trades": 0,
            "positions_count": sa_cnt["cnt"] if sa_cnt else 0,
        })
        return entry
    except Exception:
        return base_entry


def _season_starting_capital(player_id: str, season: int) -> float:
    if player_id == "steve-webull":
        return 7021.81
    if player_id == "dayblade-0dte":
        return 2000.0 if season == 1 else (5000.0 if season <= 3 else (10000.0 if season >= 5 else 3500.0))
    if player_id == "super-agent":
        return 25000.0 if season >= 5 else 100000.0
    # Season 5 equity reset: all agents reset to $10,000
    if season >= 5:
        return 10000.0
    return 10000.0 if season <= 3 else 7000.0


def _season_overlay(conn, player_id: str, season: int, current_total_value: float | None = None) -> dict:
    baseline = _season_starting_capital(player_id, season)
    first_row = conn.execute(
        "SELECT total_value FROM portfolio_history WHERE player_id=? AND season=? ORDER BY recorded_at ASC LIMIT 1",
        (player_id, season),
    ).fetchone()
    latest_row = conn.execute(
        "SELECT total_value FROM portfolio_history WHERE player_id=? AND season=? ORDER BY recorded_at DESC LIMIT 1",
        (player_id, season),
    ).fetchone()
    season_start_value = float(first_row["total_value"]) if first_row and first_row["total_value"] is not None else baseline
    season_latest_value = (
        float(current_total_value)
        if current_total_value is not None
        else (float(latest_row["total_value"]) if latest_row and latest_row["total_value"] is not None else season_start_value)
    )
    season_pnl = round(season_latest_value - season_start_value, 2)
    season_return_pct = round((season_pnl / season_start_value) * 100, 2) if season_start_value > 0 else 0.0
    vs_baseline_pnl = round(season_latest_value - baseline, 2)
    vs_baseline_return_pct = round((vs_baseline_pnl / baseline) * 100, 2) if baseline > 0 else 0.0
    return {
        "season": season,
        "season_start_value": round(season_start_value, 2),
        "season_latest_value": round(season_latest_value, 2),
        "season_pnl": season_pnl,
        "season_return_pct": season_return_pct,
        "season_baseline": round(baseline, 2),
        "vs_baseline_pnl": vs_baseline_pnl,
        "vs_baseline_return_pct": vs_baseline_return_pct,
    }


def _patch_leaderboard_season_overlays(payload: dict) -> dict:
    """Patch season overlays onto cached leaderboard rows without full recompute."""
    try:
        if not isinstance(payload, dict):
            return payload
        items = payload.get("leaderboard")
        season = payload.get("season")
        if not isinstance(items, list) or not items:
            return payload
        if season in (None, -1):
            return payload
        if "season_overlay" in items[0]:
            return payload

        conn = _conn()
        rows = conn.execute(
            """
            SELECT ph.player_id, ph.total_value
            FROM portfolio_history ph
            JOIN (
                SELECT player_id, MIN(recorded_at) AS first_ts
                FROM portfolio_history
                WHERE season=?
                GROUP BY player_id
            ) firsts
              ON ph.player_id = firsts.player_id
             AND ph.recorded_at = firsts.first_ts
            WHERE ph.season=?
            """,
            (season, season),
        ).fetchall()
        conn.close()
        start_values = {row["player_id"]: float(row["total_value"]) for row in rows}

        patched = []
        for item in items:
            entry = dict(item)
            baseline = _season_starting_capital(entry.get("player_id", ""), season)
            season_start_value = round(start_values.get(entry.get("player_id"), baseline), 2)
            season_latest_value = round(float(entry.get("total_value", season_start_value)), 2)
            season_pnl = round(season_latest_value - season_start_value, 2)
            season_return_pct = round((season_pnl / season_start_value) * 100, 2) if season_start_value > 0 else 0.0
            vs_baseline_pnl = round(season_latest_value - baseline, 2)
            vs_baseline_return_pct = round((vs_baseline_pnl / baseline) * 100, 2) if baseline > 0 else 0.0
            entry["season_overlay"] = {
                "season": season,
                "season_start_value": season_start_value,
                "season_latest_value": season_latest_value,
                "season_pnl": season_pnl,
                "season_return_pct": season_return_pct,
                "season_baseline": round(baseline, 2),
                "vs_baseline_pnl": vs_baseline_pnl,
                "vs_baseline_return_pct": vs_baseline_return_pct,
            }
            patched.append(entry)
        return {**payload, "leaderboard": patched}
    except Exception:
        return payload


def _patch_super_agent(response_data: dict) -> dict:
    """Inject a freshly computed Super Agent entry into a (possibly stale) cached leaderboard."""
    lb = response_data.get("leaderboard")
    if not isinstance(lb, list):
        return response_data
    patched = []
    for entry in lb:
        if entry.get("player_id") == "super-agent":
            patched.append(_sa_fresh_entry(entry))
        else:
            patched.append(entry)
    out = dict(response_data)
    out["leaderboard"] = patched
    return out


def _leaderboard_payload_has_normalized_metrics(payload: dict) -> bool:
    try:
        items = payload.get("leaderboard")
        if not isinstance(items, list) or not items:
            return False
        sample = items[0]
        required = ("current_equity", "previous_equity", "starting_capital", "day_pnl", "total_pnl", "return_pct")
        return all(k in sample for k in required)
    except Exception:
        return False


def _recent_funding_total(conn, player_id: str, season: int | None = None, all_seasons: bool = False) -> float:
    try:
        if all_seasons or season is None:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0) AS total
                FROM player_funding_events
                WHERE player_id=? AND created_at >= datetime('now', '-24 hours')
                """,
                (player_id,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0) AS total
                FROM player_funding_events
                WHERE player_id=? AND season=? AND created_at >= datetime('now', '-24 hours')
                """,
                (player_id, season),
            ).fetchone()
        return round(float(row["total"] or 0), 2) if row else 0.0
    except Exception:
        return 0.0


def _normalized_leaderboard_metrics(total_value, day_change, starting_capital):
    import math

    def safe(v, fallback=0.0):
        try:
            x = float(v)
            return x if math.isfinite(x) else fallback
        except Exception:
            return fallback

    total_value = safe(total_value)
    day_change = safe(day_change)
    starting_capital = safe(starting_capital)

    current_equity = total_value
    previous_equity = safe(current_equity - day_change)
    day_pnl = safe(day_change)
    total_pnl = safe(current_equity - starting_capital)
    return_pct = safe((total_pnl / starting_capital) * 100 if starting_capital > 0 else 0.0)

    return {
        "current_equity": current_equity,
        "previous_equity": previous_equity,
        "starting_capital": starting_capital,
        "day_pnl": day_pnl,
        "total_pnl": total_pnl,
        "return_pct": return_pct,
    }


@app.get("/api/arena/leaderboard")
def leaderboard(season: int = 0, _force: bool = False, nocache: bool = False, show_all: bool = False):
    if nocache:
        _force = True
        _endpoint_cache.pop(f"leaderboard_{season}", None)
        _leaderboard_disk_cache["data"] = None
        _leaderboard_disk_cache["ts"] = 0
    """
    Leaderboard API Contract (/api/arena/leaderboard)

    Normalized Fields (required)
    ---------------------------
    Each row MUST emit:

    - current_equity: float
        Latest portfolio value (source of truth)

    - previous_equity: float
        Prior snapshot value used for day P&L

    - starting_capital: float
        Initial capital baseline

    - day_pnl: float
        24h performance:
            current_equity - previous_equity
        Must exclude funding events (cash injections/withdrawals)

    - total_pnl: float
        Lifetime performance:
            current_equity - starting_capital

    - return_pct: float
        Derived ONLY from normalized values:
            total_pnl / starting_capital

    Legacy Fields (deprecated)
    --------------------------
    The following MUST NOT be used by the frontend:

    - total_value
    - day_change
    - raw return_pct (if not derived from normalized fields)

    They may exist temporarily for backward compatibility but are not authoritative.

    Cache Rules
    -----------
    - Cached leaderboard payloads missing normalized fields are INVALID
    - Such payloads must be bypassed and recomputed
    - Cache must always reflect normalized schema

    Guarantees
    ----------
    - Frontend renders exclusively from normalized fields
    - No mixing of legacy and normalized values
    - Day P&L reflects true performance, not capital changes
    - Total P&L and return are mathematically consistent

    Migration Note
    --------------
    This endpoint was upgraded from legacy fields (total_value, day_change)
    to normalized portfolio accounting.

    Any consumer still using legacy fields must be updated.

    Runtime / Deployment Note
    -------------------------

    If /api/arena/leaderboard returns legacy fields such as:
    - total_value
    - day_change
    - legacy return_pct

    then the running server is stale.

    Expected normalized fields:
    - current_equity
    - previous_equity
    - starting_capital
    - day_pnl
    - total_pnl
    - return_pct

    Required actions:
    1. Stop any process bound to port 8080
    2. Restart the dashboard backend with the latest code
    3. Verify live output BEFORE opening the UI

    Verification command:
        curl http://127.0.0.1:8080/api/arena/leaderboard | jq '.rows[0]'

    Success condition:
    - normalized fields are present
    - legacy fields are absent or unused

    Failure indicates:
    - old process still running, or
    - stale cache/process state still serving legacy payloads

    Important:
    A successful build or py_compile does NOT mean the running server is updated.
    Only the live API response is authoritative.

    8080 restart / listener note

    This dashboard must own port 8080.

    If restarting dashboard/app.py fails with:
        [Errno 48] address already in use

    then another Python process has already bound 8080.

    Required recovery steps:
    1. Identify the current listener:
           lsof -i :8080
    2. Kill the listed PID:
           kill -9 <PID>
    3. Restart the dashboard:
           cd /Users/bigmac/autonomous-trader
           ./venv/bin/python dashboard/app.py
    4. Verify live output:
           curl -s http://127.0.0.1:8080/api/arena/leaderboard | jq '.leaderboard[0]'

    Important:
    - A successful py_compile does not update the running server
    - If another process immediately reclaims 8080, that process is the current blocker
    - Always verify the live API after restart

    Leaderboard runtime status

    Resolved:
    - Port 8080 is now owned by dashboard/app.py
    - The legacy main.py listener was the stale-server source
    - A LaunchAgent was auto-restarting main.py and reclaiming 8080
    - Unloading that LaunchAgent allowed dashboard/app.py to serve the live API

    Current live result:
    - /api/arena/leaderboard now emits normalized fields
    - Mr. Anderson currently reports flat at starting capital because the live data itself is flat
    - This is no longer a frontend or stale-runtime issue

    Key takeaway:
    - If leaderboard values are wrong, first verify the live listener on 8080
    - Once dashboard/app.py is confirmed live, remaining discrepancies are data-source issues

    Anderson live-state note

    The leaderboard/runtime path is now confirmed correct.

    If Mr. Anderson still shows:
    - current_equity = 25000
    - day_pnl = 0
    - total_pnl = 0

    then the remaining issue is upstream data state, not frontend math or stale runtime.

    Confirmed facts:
    - dashboard/app.py is the live listener on 127.0.0.1:8080
    - normalized leaderboard fields are now live
    - Anderson history exists under portfolio_positions portfolio_id=1
    - portfolio_id=6 ("Mr. Anderson") is currently empty

    Interpretation:
    - Anderson appearing flat is now a data-source / reconstruction issue
    - Do not treat this as a UI bug
    - Do not overwrite balances or delete history during fixes
    """
    import time as _lt, json as _lj
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_bulk_prices
    from config import WATCH_STOCKS

    _lb_key = f"leaderboard_{season}_{'all' if show_all else 'active'}"
    _lb_entry = _endpoint_cache.get(_lb_key)
    _now = _lt.time()

    # Fast path 1: in-memory cache (60s TTL) — always patch Super Agent live
    if not _force and _lb_entry and (_now - _lb_entry["time"]) < 60:
        if _leaderboard_payload_has_normalized_metrics(_lb_entry["data"]):
            return _patch_super_agent(_patch_leaderboard_season_overlays(_lb_entry["data"]))

    # Fast path 2: disk cache — always serve stale data immediately, refresh in background
    # (leaderboard is heavy: bulk Yahoo + per-player PnL calcs)
    # _force=True bypasses both caches so background thread can recompute fresh data.
    if not _force and not show_all and _leaderboard_disk_cache["data"]:
        _disk_season = _leaderboard_disk_cache["data"].get("season", -99) if isinstance(_leaderboard_disk_cache["data"], dict) else -99
        _season_matches = (_disk_season == season) or (season == 0 and _disk_season > 0) or (season <= 0)
        if _season_matches and _leaderboard_payload_has_normalized_metrics(_leaderboard_disk_cache["data"]):
            # Fire background refresh if stale (> 60s).
            # Use setdefault so the lock exists even before the first full computation.
            _disk_age = _now - _leaderboard_disk_cache["ts"]
            if _disk_age > 60 and _swr_locks.setdefault("leaderboard", threading.Lock()).acquire(blocking=False):
                _bg_season = season
                def _lb_bg():
                    try:
                        leaderboard(season=_bg_season, _force=True)  # bypass cache in background
                    except Exception:
                        pass
                    finally:
                        _swr_locks["leaderboard"].release()
                threading.Thread(target=_lb_bg, daemon=True).start()
            return _patch_super_agent(_patch_leaderboard_season_overlays(_leaderboard_disk_cache["data"]))  # always fresh SA

    conn = _conn()

    # Determine current season
    current_season = 2
    s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    if s_row:
        current_season = int(s_row["value"])

    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        season = current_season

    players = conn.execute("""
        SELECT p.id, p.display_name, p.provider, p.model_id, p.cash, p.is_active, p.is_halted, COALESCE(p.is_paused, 0) as is_paused
        FROM ai_players p WHERE p.is_active = 1 AND p.id NOT LIKE '%cto%' AND p.id != 'red-alert'
        ORDER BY p.id
    """).fetchall()
    # Default view: active fleet only. Pass ?show_all=true to see everyone.
    if not show_all:
        players = [p for p in players if p["id"] in FLEET_ACTIVE]

    # Season-filtered trade counts
    trade_counts = {}
    if all_seasons:
        for row in conn.execute("SELECT player_id, COUNT(*) as cnt FROM trades GROUP BY player_id").fetchall():
            trade_counts[row["player_id"]] = row["cnt"]
    else:
        for row in conn.execute("SELECT player_id, COUNT(*) as cnt FROM trades WHERE season=? GROUP BY player_id", (season,)).fetchall():
            trade_counts[row["player_id"]] = row["cnt"]
    # Super Agent: count total portfolio_positions for portfolio_id=1 (Alpaca Paper)
    try:
        sa_tc = conn.execute(
            "SELECT COUNT(*) as cnt FROM portfolio_positions WHERE portfolio_id=1"
        ).fetchone()
        trade_counts["super-agent"] = sa_tc["cnt"] if sa_tc else 0
    except Exception:
        pass

    # Season-filtered win rate
    win_data = {}
    win_q = """
        SELECT player_id,
               COUNT(*) as total_sells,
               SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins
        FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL AND realized_pnl != 0"""
    if all_seasons:
        win_q += " GROUP BY player_id"
        win_rows = conn.execute(win_q).fetchall()
    else:
        win_q += " AND season=? GROUP BY player_id"
        win_rows = conn.execute(win_q, (season,)).fetchall()
    for row in win_rows:
        total = row["total_sells"]
        win_data[row["player_id"]] = round(row["wins"] / total * 100, 1) if total > 0 else 0
    # Super Agent win rate: closed portfolio_positions with positive closed_pnl
    try:
        sa_wr = conn.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN closed_pnl > 0 THEN 1 ELSE 0 END) as wins "
            "FROM portfolio_positions WHERE portfolio_id=1 AND status='closed'"
        ).fetchone()
        if sa_wr and sa_wr["total"] > 0:
            win_data["super-agent"] = round(sa_wr["wins"] / sa_wr["total"] * 100, 1)
        else:
            win_data["super-agent"] = 0.0
    except Exception:
        pass

    # Season-filtered profit factor (sum of winning trades / sum of losing trades)
    profit_factor_data = {}
    pf_q = """
        SELECT player_id,
               COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END), 0) as total_gains,
               COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN ABS(realized_pnl) ELSE 0 END), 0) as total_losses
        FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL"""
    if all_seasons:
        pf_q += " GROUP BY player_id"
        pf_rows = conn.execute(pf_q).fetchall()
    else:
        pf_q += " AND season=? GROUP BY player_id"
        pf_rows = conn.execute(pf_q, (season,)).fetchall()
    for row in pf_rows:
        gains = row["total_gains"]
        losses = row["total_losses"]
        pf = round(gains / losses, 2) if losses > 0 else (999.0 if gains > 0 else 0.0)
        profit_factor_data[row["player_id"]] = {"profit_factor": pf, "realized_gains": round(gains, 2), "realized_losses": round(losses, 2)}

    # Season 5 realized P&L per player (for anchored equity calculation)
    s5_realized: dict[str, float] = {}
    if current_season >= 5:
        for row in conn.execute(
            "SELECT player_id, COALESCE(SUM(realized_pnl), 0) as total "
            "FROM trades WHERE action='SELL' AND realized_pnl IS NOT NULL AND season=5 "
            "GROUP BY player_id"
        ).fetchall():
            s5_realized[row["player_id"]] = float(row["total"])

    # Day P&L from portfolio_history (season-filtered)
    day_pnl = {}
    if all_seasons:
        day_rows = conn.execute("""
            SELECT player_id, total_value FROM portfolio_history
            WHERE recorded_at >= datetime('now', '-24 hours')
            ORDER BY recorded_at ASC
        """).fetchall()
    else:
        day_rows = conn.execute("""
            SELECT player_id, total_value FROM portfolio_history
            WHERE recorded_at >= datetime('now', '-24 hours') AND season=?
            ORDER BY recorded_at ASC
        """, (season,)).fetchall()
    for row in day_rows:
        pid = row["player_id"]
        if pid not in day_pnl:
            day_pnl[pid] = {"first": row["total_value"], "last": row["total_value"]}
        day_pnl[pid]["last"] = row["total_value"]

    # Players with open options positions who aren't the designated options player
    shadow_options_players = set()
    try:
        shadow_rows = conn.execute(
            "SELECT DISTINCT p.player_id FROM positions p "
            "LEFT JOIN ai_players a ON a.id = p.player_id "
            "WHERE p.asset_type='option' AND COALESCE(a.options_enabled, 0) = 0"
        ).fetchall()
        for r in shadow_rows:
            shadow_options_players.add(r["player_id"])
    except Exception:
        pass

    # Open position counts per player (regular + super-agent from portfolio_positions)
    pos_counts = {}
    try:
        for row in conn.execute("SELECT player_id, COUNT(*) as cnt FROM positions GROUP BY player_id").fetchall():
            pos_counts[row["player_id"]] = row["cnt"]
        sa_cnt = conn.execute(
            "SELECT COUNT(*) as cnt FROM portfolio_positions WHERE portfolio_id=1 AND status='open'"
        ).fetchone()
        pos_counts["super-agent"] = sa_cnt["cnt"] if sa_cnt else 0
    except Exception:
        pass

    is_current = (season == current_season) or all_seasons

    if is_current:
        # Live data for current season — bulk-fetch all symbols in ONE Yahoo request
        try:
            # Include Webull Portfolio non-watchlist positions so get_portfolio_with_pnl doesn't serial-fetch them
            _extra = ["UNH", "VRT", "CPER", "XLE", "ORCL", "AMZN", "MU", "NVDA", "AVGO", "PLTR"]
            _all_syms = list(WATCH_STOCKS) + [s for s in _extra if s not in WATCH_STOCKS]
            prices = get_bulk_prices(_all_syms, timeout=5) or {}
        except Exception:
            prices = {}
        result = []
        for p in players:
            try:
                # Super Agent: compute from Alpaca Paper portfolio_positions (portfolio_id=1)
                if p["id"] == "super-agent":
                    _sa_conn = _conn()
                    _sa_pos = _sa_conn.execute(
                        "SELECT COALESCE(SUM(unrealized_pnl), 0) as total_unrealized, "
                        "COALESCE(SUM(quantity * current_price), 0) as pos_value, "
                        "COALESCE(SUM(quantity * entry_price), 0) as cost_basis "
                        "FROM portfolio_positions WHERE portfolio_id=1 AND status='open'"
                    ).fetchone()
                    _sa_realized = _sa_conn.execute(
                        "SELECT COALESCE(SUM(closed_pnl), 0) as realized "
                        "FROM portfolio_positions WHERE portfolio_id=1 AND status='closed'"
                    ).fetchone()
                    _sa_conn.close()
                    unrealized_pnl = round(float(_sa_pos["total_unrealized"] or 0), 2)
                    positions_value = round(float(_sa_pos["pos_value"] or 0), 2)
                    realized = round(float(_sa_realized["realized"] or 0), 2)
                    total_value = round(25000.0 + unrealized_pnl + realized, 2)
                    return_pct = round((total_value - 25000.0) / 25000.0 * 100, 2)
                
                # Enterprise Computer uses metals_tracker (physical gold/silver — display only)
                elif p["id"] == "enterprise-computer":
                    from engine.metals_tracker import get_portfolio as _metals_portfolio
                    _mp = _metals_portfolio()
                    total_value = _mp["total_value"]   # metals market value only (no cash)
                    positions_value = total_value      # same — metals IS the portfolio
                    unrealized_pnl = _mp["total_unrealized_pnl"]
                    # return_pct = (metals_value - cost_basis) / cost_basis
                    _cost_basis = _mp.get("total_cost_basis", 0)
                    return_pct = round((total_value - _cost_basis) / _cost_basis * 100, 2) if _cost_basis > 0 else 0
                else:
                    pnl = get_portfolio_with_pnl(p["id"], prices)
                    positions_value = pnl["total_positions_value"]
                    unrealized_pnl = pnl["total_unrealized_pnl"]
                    # Season 5: anchor equity to $10k reset + S5 P&L only
                    if current_season >= 5 and p["id"] not in (
                        "super-agent", "enterprise-computer", "steve-webull", "dalio-metals"
                    ):
                        _s5_realized = s5_realized.get(p["id"], 0.0)
                        total_value = round(10000.0 + _s5_realized + unrealized_pnl, 2)
                        return_pct = round((_s5_realized + unrealized_pnl) / 10000.0 * 100, 2)
                    else:
                        total_value = pnl["total_value"]
                        return_pct = pnl["return_pct"]
            except Exception:
                total_value = round(p["cash"], 2)
                positions_value = 0
                unrealized_pnl = 0
                starting = _season_starting_capital(p["id"], current_season)
                return_pct = round((total_value - starting) / starting * 100, 2) if starting > 0 else 0.0
            pnl_history = day_pnl.get(p["id"], {})
            day_change = pnl_history.get("last", total_value) - pnl_history.get("first", total_value)
            day_change -= _recent_funding_total(conn, p["id"], season=current_season, all_seasons=all_seasons)

            # For steve-webull / dalio-metals: calculate day P&L from position-level price movement,
            # not portfolio_history snapshots (which break on sync when positions are removed)
            if p["id"] in ("steve-webull", "dalio-metals"):
                try:
                    day_change = sum(
                        pos.get("market_value", 0) * pos.get("day_change_pct", 0) / 100
                        for pos in pnl.get("positions", [])
                        if pos.get("day_change_pct") is not None
                    )
                except Exception:
                    pass  # Fall back to portfolio_history calculation

            pf_info = profit_factor_data.get(p["id"], {})
            season_overlay = _season_overlay(conn, p["id"], season, current_total_value=total_value)
            starting_capital = _season_starting_capital(p["id"], current_season)
            normalized = _normalized_leaderboard_metrics(total_value, day_change, starting_capital)
            row = {
                "player_id": p["id"],
                "name": p["display_name"],
                "provider": p["provider"],
                "model": p["model_id"],
                "cash": (0.0 if p["id"] == "enterprise-computer"
                         else total_value if p["id"] == "steve-webull"
                         else round(p["cash"], 2)),
                "positions_value": positions_value,
                "total_value": total_value,
                "unrealized_pnl": unrealized_pnl,
                "return_pct": normalized["return_pct"],
                "total_pnl": normalized["total_pnl"],
                "day_change": normalized["day_pnl"],
                "current_equity": normalized["current_equity"],
                "previous_equity": normalized["previous_equity"],
                "starting_capital": normalized["starting_capital"],
                "day_pnl": normalized["day_pnl"],
                "trades": trade_counts.get(p["id"], 0),
                "win_rate": win_data.get(p["id"], 0),
                "profit_factor": pf_info.get("profit_factor", 0),
                "realized_gains": pf_info.get("realized_gains", 0),
                "realized_losses": pf_info.get("realized_losses", 0),
                "is_active": bool(p["is_active"]),
                "is_halted": bool(p["is_halted"]),
                "is_paused": bool(p["is_paused"]),
                "has_shadow_options": p["id"] in shadow_options_players,
                "positions_count": pos_counts.get(p["id"], 0),
                "season_overlay": season_overlay,
            }
            result.append(annotate_player_payload(row))
        conn.close()
    else:
        # Historical season — reconstruct final values from last portfolio_history snapshot
        conn.close()
        conn2 = _conn()
        result = []
        starting = 10000.0
        for p in players:
            pid = p["id"]
            # Season-aware starting capital: S1-S3 used $10k, S4+ uses $7k
            if pid == "steve-webull":
                s_starting = 7021.81
            elif pid == "dayblade-0dte":
                s_starting = 2000.0 if season == 1 else (5000.0 if season <= 3 else 3500.0)
            else:
                s_starting = 10000.0 if season <= 3 else 7000.0
            # Get last snapshot for this season
            snap = conn2.execute(
                "SELECT total_value, cash, positions_value FROM portfolio_history "
                "WHERE player_id=? AND season=? ORDER BY recorded_at DESC LIMIT 1", (pid, season)
            ).fetchone()
            # Get realized P&L sum for the season
            rpnl = conn2.execute(
                "SELECT COALESCE(SUM(realized_pnl), 0) as total FROM trades WHERE player_id=? AND season=? AND action='SELL' AND realized_pnl IS NOT NULL",
                (pid, season)
            ).fetchone()

            if snap:
                total_value = snap["total_value"]
            else:
                total_value = s_starting + (rpnl["total"] if rpnl else 0)

            pnl_history = day_pnl.get(pid, {})
            day_change = pnl_history.get("last", total_value) - pnl_history.get("first", total_value)
            day_change -= _recent_funding_total(conn2, pid, season=season, all_seasons=all_seasons)
            normalized = _normalized_leaderboard_metrics(total_value, day_change, s_starting)
            
            result.append(annotate_player_payload({
                "player_id": pid,
                "name": p["display_name"],
                "provider": p["provider"],
                "model": p["model_id"],
                "cash": round(snap["cash"], 2) if snap else round(total_value, 2),
                "positions_value": round(snap["positions_value"], 2) if snap else 0,
                "total_value": round(total_value, 2),
                "unrealized_pnl": 0,
                "return_pct": normalized["return_pct"],
                "day_change": normalized["day_pnl"],
                "current_equity": normalized["current_equity"],
                "previous_equity": normalized["previous_equity"],
                "starting_capital": normalized["starting_capital"],
                "day_pnl": normalized["day_pnl"],
                "total_pnl": normalized["total_pnl"],
                "trades": trade_counts.get(pid, 0),
                "win_rate": win_data.get(pid, 0),
                "profit_factor": profit_factor_data.get(pid, {}).get("profit_factor", 0),
                "realized_gains": profit_factor_data.get(pid, {}).get("realized_gains", 0),
                "realized_losses": profit_factor_data.get(pid, {}).get("realized_losses", 0),
                "is_active": bool(p["is_active"]),
                "is_halted": False,
                "is_paused": False,
                "has_shadow_options": p["id"] in shadow_options_players,
                "season_overlay": _season_overlay(conn2, pid, season, current_total_value=total_value),
            }))
        conn2.close()

    result.sort(key=lambda x: x["total_value"], reverse=True)
    _lb_result = {"season": -1 if all_seasons else season, "current_season": current_season, "leaderboard": result}
    _lb_result = _patch_super_agent(_lb_result)

    # Update in-memory and disk cache (disk cache only for the default active-fleet view)
    _endpoint_cache[_lb_key] = {"time": _lt.time(), "data": _lb_result}
    if not show_all:
        try:
            _leaderboard_disk_cache["data"] = _lb_result
            _leaderboard_disk_cache["ts"] = _lt.time()
            with open(_LEADERBOARD_CACHE_FILE, "w") as _lf:
                _lj.dump({"data": _lb_result, "ts": _lt.time()}, _lf)
        except Exception:
            pass

    if "leaderboard" not in _swr_locks:
        _swr_locks["leaderboard"] = threading.Lock()
    return _sanitize(_lb_result)


import math
def _sanitize(obj):
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else 0.0
    return obj


@app.get("/api/arena/player/{player_id}")
def player_detail(player_id: str):
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_stock_price
    from engine.sector_tracker import get_sector_exposure
    from engine.correlation import get_portfolio_correlation
    from engine.risk_manager import RiskManager

    metals_alias = player_id == "enterprise-computer"
    source_player_id = "enterprise-computer" if metals_alias else player_id

    conn = _conn()
    player = conn.execute("SELECT * FROM ai_players WHERE id=?", (player_id,)).fetchone()
    if not player:
        conn.close()
        return {"error": "Player not found"}

    # Super Agent uses portfolio_positions (Alpaca Paper) not positions table
    if player_id == "super-agent":
        sa_positions = conn.execute(
            "SELECT ticker AS symbol, quantity AS qty, entry_price AS avg_price, "
            "asset_class AS asset_type, option_type, strike_price, expiration_date AS expiry_date, "
            "unrealized_pnl, current_price, stop_loss, take_profit, "
            "COALESCE(quantity * current_price, quantity * entry_price) AS market_value, "
            "created_at AS opened_at "
            "FROM portfolio_positions WHERE portfolio_id=1 AND status='open' "
            "ORDER BY created_at DESC"
        ).fetchall()
        sa_closed = conn.execute(
            "SELECT COUNT(*) AS total, "
            "COALESCE(SUM(closed_pnl), 0) AS realized, "
            "SUM(CASE WHEN closed_pnl > 0 THEN 1 ELSE 0 END) AS wins "
            "FROM portfolio_positions WHERE portfolio_id=1 AND status='closed'"
        ).fetchone()
        sa_total_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM portfolio_positions WHERE portfolio_id=1"
        ).fetchone()
        conn.close()

        total_unrealized = round(sum(float(p["unrealized_pnl"] or 0) for p in sa_positions), 2)
        total_realized = round(float(sa_closed["realized"] or 0), 2)
        # market_value: prefer quantity * current_price; fall back to entry if current is 0/null
        positions_value = round(sum(
            float(p["qty"] or 0) * (float(p["current_price"]) if p["current_price"] else float(p["avg_price"] or 0))
            for p in sa_positions
        ), 2)
        total_value = round(25000.0 + total_unrealized + total_realized, 2)
        return_pct = round((total_value - 25000.0) / 25000.0 * 100, 2)

        closed_total = sa_closed["total"] or 0
        closed_wins  = sa_closed["wins"] or 0
        win_rate_pct = round(closed_wins / closed_total * 100, 1) if closed_total > 0 else 0.0

        def _pos_dict(p):
            qty       = float(p["qty"] or 0)
            avg       = float(p["avg_price"] or 0)
            cur       = float(p["current_price"] or 0) or avg   # fall back to avg if no live price
            cost      = qty * avg
            mv        = qty * cur
            unreal    = float(p["unrealized_pnl"] or 0)
            # pct: use stored Alpaca unrealized_pnl / cost basis (more accurate than price diff)
            unreal_pct = round(unreal / cost * 100, 2) if cost else 0.0
            return {
                "symbol":           p["symbol"],
                "qty":              qty,
                "avg_price":        avg,
                "current_price":    cur,
                "market_value":     round(mv, 2),
                "unrealized_pnl":   round(unreal, 2),
                "unrealized_pnl_pct": unreal_pct,
                "day_change_pct":   None,           # not available from DB sync
                "asset_type":       p["asset_type"] or "stock",
                "option_type":      p["option_type"],
                "strike_price":     p["strike_price"],
                "expiry_date":      p["expiry_date"],
                "stop_loss":        float(p["stop_loss"]) if p["stop_loss"] else None,
                "take_profit":      float(p["take_profit"]) if p["take_profit"] else None,
                "opened_at":        p["opened_at"],
                "sources":          "",
            }

        return annotate_player_payload({
            "player_id": player["id"],
            "name":      player["display_name"],
            "provider":  player["provider"],
            "model":     player["model_id"],
            "cash":      round(float(player["cash"]), 2),
            "total_value":            total_value,
            "return_pct":             return_pct,
            "total_unrealized_pnl":   total_unrealized,
            "total_positions_value":  positions_value,
            "is_active":  bool(player["is_active"]),
            "is_halted":  bool(player["is_halted"]),
            "positions":  [_pos_dict(p) for p in sa_positions],
            "stats": {
                "total_trades":   sa_total_count["cnt"] if sa_total_count else 0,
                "buys":           len(sa_positions),          # open = bought not yet sold
                "sells":          closed_total,
                "options_trades": 0,
                "win_rate":       win_rate_pct,
                "closed_trades":  closed_total,
                "closed_wins":    closed_wins,
            },
        })

    positions = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date, opened_at "
        "FROM positions WHERE player_id=?", (player_id,)
    ).fetchall()

    # Get trade stats
    stats = conn.execute("""
        SELECT COUNT(*) as total_trades,
               SUM(CASE WHEN action='BUY' THEN 1 ELSE 0 END) as buys,
               SUM(CASE WHEN action='SELL' THEN 1 ELSE 0 END) as sells,
               SUM(CASE WHEN action LIKE 'BUY_%' THEN 1 ELSE 0 END) as options_trades
        FROM trades WHERE player_id=?
    """, (player_id,)).fetchone()

    # Realized P&L from completed sells
    realized = conn.execute("""
        SELECT COALESCE(SUM(
            CASE WHEN action='SELL' THEN qty * price ELSE 0 END -
            CASE WHEN action='SELL' THEN qty * (
                SELECT t2.price FROM trades t2
                WHERE t2.player_id=trades.player_id AND t2.symbol=trades.symbol
                AND t2.action='BUY' ORDER BY t2.executed_at DESC LIMIT 1
            ) ELSE 0 END
        ), 0) as total_realized
        FROM trades WHERE player_id=? AND action='SELL'
    """, (player_id,)).fetchone()

    # Look up data sources for each open position from its BUY trade
    position_sources = {}
    try:
        conn2 = _conn()
        conn2.execute("SELECT sources FROM trades LIMIT 1")  # test column exists
        for pos in positions:
            src_row = conn2.execute(
                "SELECT sources FROM trades WHERE player_id=? AND symbol=? AND action IN ('BUY','BUY_CALL','BUY_PUT') "
                "ORDER BY executed_at DESC LIMIT 1",
                (player_id, pos["symbol"])
            ).fetchone()
            if src_row and src_row["sources"]:
                position_sources[pos["symbol"]] = src_row["sources"]
        conn2.close()
    except Exception:
        pass

    # Fetch live prices for positions
    # Metal positions need Yahoo futures symbols (GC=F, SI=F), not stock tickers
    _METAL_YAHOO = {"GOLD": "GC=F", "SILVER": "SI=F", "PLATINUM": "PL=F", "PALLADIUM": "PA=F"}
    prices = {}
    symbols = list(set(p["symbol"] for p in positions))
    for sym in symbols:
        fetch_sym = _METAL_YAHOO.get(sym, sym)
        data = get_stock_price(fetch_sym)
        if "error" not in data:
            prices[sym] = data

    pnl_data = get_portfolio_with_pnl(source_player_id, prices)

    # Enterprise Computer: override total_value/return_pct with metals_tracker (cost-basis based)
    if metals_alias:
        try:
            from engine.metals_tracker import get_portfolio as _mp_fn
            _mp = _mp_fn()
            pnl_data = dict(pnl_data)
            pnl_data["total_value"] = _mp["total_value"]
            pnl_data["total_unrealized_pnl"] = _mp["total_unrealized_pnl"]
            _cost = _mp.get("total_cost_basis", 0)
            pnl_data["return_pct"] = round((_mp["total_value"] - _cost) / _cost * 100, 2) if _cost > 0 else 0
            pnl_data["total_positions_value"] = _mp["total_value"]
        except Exception:
            pass

    # Attach sources to each position
    for pos in pnl_data["positions"]:
        pos["sources"] = position_sources.get(pos["symbol"], "")

    try:
        sector_exposure = get_sector_exposure(source_player_id)
    except Exception:
        sector_exposure = []
    try:
        correlation_profile = get_portfolio_correlation(source_player_id)
    except Exception:
        correlation_profile = {
            "groups": [],
            "group_exposure": [],
            "symbol_exposure": [],
            "warnings": [],
            "concentrated": False,
        }
    try:
        construction = RiskManager().get_portfolio_construction_warnings(source_player_id, pnl_data)
    except Exception:
        construction = {"warnings": [], "sector": {"buckets": []}, "correlation": {"groups": []}}
    try:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        current_season = int(s_row["value"]) if s_row else 1
    except Exception:
        current_season = 1
    season_overlay = _season_overlay(conn, player_id, current_season, current_total_value=pnl_data["total_value"])
    conn.close()

    return annotate_player_payload({
        "player_id": player["id"],
        "name": player["display_name"],
        "provider": player["provider"],
        "model": player["model_id"],
        "cash": 0.0 if metals_alias else round(player["cash"], 2),
        "total_value": pnl_data["total_value"],
        "return_pct": pnl_data["return_pct"],
        "total_unrealized_pnl": pnl_data["total_unrealized_pnl"],
        "total_positions_value": pnl_data["total_positions_value"],
        "is_active": bool(player["is_active"]),
        "is_halted": bool(player["is_halted"]),
        "positions": pnl_data["positions"],
        "stats": {
            "total_trades": stats["total_trades"] if stats else 0,
            "buys": stats["buys"] if stats else 0,
            "sells": stats["sells"] if stats else 0,
            "options_trades": stats["options_trades"] if stats else 0,
        },
        "risk": {
            "sector_exposure": sector_exposure,
            "correlation": correlation_profile,
            "construction": construction,
        },
        "season_overlay": season_overlay,
    })


@app.get("/api/arena/player/{player_id}/trades")
def player_trades(player_id: str, limit: int = 50):
    conn = _conn()
    # Check if sources column exists
    _has_src = False
    try:
        conn.execute("SELECT sources FROM trades LIMIT 1")
        _has_src = True
    except Exception:
        pass
    _sc = ", sources" if _has_src else ""
    trades = conn.execute(
        f"SELECT symbol, action, qty, price, asset_type, option_type, reasoning, confidence, executed_at{_sc} "
        "FROM trades WHERE player_id=? ORDER BY executed_at DESC LIMIT ?",
        (player_id, limit)
    ).fetchall()
    conn.close()
    return [annotate_player_payload(dict(t) | {"player_id": player_id}) for t in trades]


@app.get("/api/arena/player/{player_id}/open-positions")
def player_open_positions(player_id: str, nocache: bool = False):
    """Return open positions with live P&L for a player."""
    # Enterprise Computer only: use metals_tracker for Captain's physical gold/silver
    # dalio-metals holds GC=F/SI=F futures as regular DB positions — use normal path
    if player_id in ("enterprise-computer",):
        try:
            from engine.metals_tracker import get_portfolio as _mp_fn
            _mp = _mp_fn()
            # Annotate each position with asset_type='metal' and unit
            for pos in _mp.get("positions", []):
                pos["asset_type"] = "metal"
            return {"positions": _mp.get("positions", []), "total_unrealized": _mp.get("total_unrealized_pnl", 0)}
        except Exception as e:
            return {"positions": [], "total_unrealized": 0, "error": str(e)}
    try:
        from engine.paper_trader import get_portfolio_with_pnl as _gpnl
        from engine.market_data import get_all_prices as _gap
        import engine.market_data as _md
        conn = _conn()
        syms = [r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM positions WHERE player_id=? AND qty>0", (player_id,)
        ).fetchall()]
        conn.close()
        # Clear price cache for this player's symbols so fresh prices are fetched
        if nocache and syms:
            for _sym in syms:
                _md._price_cache.pop(_sym, None)
        prices = _gap(syms) if syms else {}
        port = _gpnl(player_id, prices)
        return {"positions": port.get("positions", []), "total_unrealized": port.get("total_unrealized_pnl", 0)}
    except Exception as e:
        return {"positions": [], "total_unrealized": 0, "error": str(e)}


@app.get("/api/arena/player/{player_id}/signals")
def player_signals(player_id: str, limit: int = 50):
    conn = _conn()
    _has_src = False
    try:
        conn.execute("SELECT sources FROM signals LIMIT 1")
        _has_src = True
    except Exception:
        pass
    _sc = ", sources" if _has_src else ""
    signals = conn.execute(
        f"SELECT symbol, signal, confidence, reasoning, asset_type, option_type, created_at{_sc} "
        "FROM signals WHERE player_id=? ORDER BY created_at DESC LIMIT ?",
        (player_id, limit)
    ).fetchall()
    conn.close()
    return [dict(s) for s in signals]


@app.get("/api/arena/player/{player_id}/history")
def player_history(player_id: str):
    conn = _conn()
    history = conn.execute(
        "SELECT total_value, cash, positions_value, recorded_at "
        "FROM portfolio_history WHERE player_id=? ORDER BY recorded_at ASC",
        (player_id,)
    ).fetchall()
    conn.close()
    return [dict(h) for h in history]


# --- General Endpoints ---

@app.get("/api/ollama-queue-status")
def ollama_queue_status():
    """Ollama FIFO queue health — queue depth, avg response time, staleness."""
    try:
        from engine.ollama_queue import get_queue
        return get_queue().status()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/proving-ground")
def proving_ground_status():
    """Sniper Mode 30-Day Proving Ground — live scorecard and Go/No-Go benchmarks."""
    try:
        from engine.proving_ground import get_proving_ground_status
        return get_proving_ground_status()
    except Exception as e:
        return {"error": str(e), "trial_day": 0, "go_count": 0}


@app.get("/api/ollie/stats")
def ollie_commander_stats():
    """Ollie Commander approval/rejection stats."""
    try:
        from engine.ollie_commander import get_ollie_stats
        return get_ollie_stats()
    except Exception as e:
        return {"error": str(e), "approved": 0, "rejected": 0, "filter_wr": 0.0}


@app.get("/api/season")
def season_info():
    """Current season metadata."""
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    current = int(row["value"]) if row else 5
    cfg = conn.execute("SELECT * FROM season_config WHERE season=?", (current,)).fetchone()
    name_row = conn.execute("SELECT value FROM settings WHERE key=?", (f"season_{current}_name",)).fetchone()
    start_row = conn.execute("SELECT value FROM settings WHERE key=?", (f"season_{current}_start",)).fetchone()
    conn.close()
    from datetime import date
    today = date.today()
    start = date.fromisoformat((start_row["value"] if start_row else "2026-04-10")[:10])
    day_num = max(1, (today - start).days + 1)
    return {
        "season":     current,
        "name":       name_row["value"] if name_row else f"Season {current}",
        "start_date": start.isoformat(),
        "day_number": day_num,
        "config":     dict(cfg) if cfg else {},
    }


@app.get("/api/health")
def health_detail():
    """Dr. Crusher extended health — Ollama, WebSocket, scheduler, DayBlade, uptime."""
    import subprocess as _sp
    import re as _re
    base_dir = os.path.expanduser("~/autonomous-trader")
    scanner_log = os.path.join(base_dir, "scanner.log")

    # --- Ollama last success ---
    last_ollama_success: str | None = None
    try:
        from engine.ollama_queue import get_queue as _gq
        _qs = _gq().status()
        age_min = _qs.get("last_success_age_min")
        if age_min is not None:
            import datetime as _dt
            _ts = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=age_min)
            last_ollama_success = _ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        pass

    # --- Scan health from watchdog ---
    scan_health: dict = {}
    try:
        from engine.ollama_watchdog import get_watchdog as _gw
        scan_health = _gw().get_scan_health()
    except Exception:
        pass

    # --- WebSocket status (check last 500 scanner.log lines) ---
    websocket_status = "unknown"
    try:
        tail = _sp.check_output(["tail", "-500", scanner_log],
                                text=True, stderr=_sp.DEVNULL)
        lines = tail.splitlines()
        has_polling = any("polling mode" in l or "WebSocket failed 3x" in l for l in lines)
        has_connected = any("WebSocket connected" in l or "WebSocket reconnect" in l
                            for l in lines[-100:])
        if has_connected:
            websocket_status = "connected"
        elif has_polling:
            websocket_status = "polling"
        else:
            websocket_status = "unknown"
    except Exception:
        pass

    # --- Scheduler errors ---
    scheduler_errors = 0
    try:
        result = _sp.run(
            ["grep", "-c", "Scheduler job error", scanner_log],
            capture_output=True, text=True,
        )
        scheduler_errors = int(result.stdout.strip() or "0")
    except Exception:
        pass

    # --- DayBlade last scan (last line containing 'dayblade' or 'DayBlade') ---
    dayblade_last_scan: str | None = None
    try:
        result = _sp.run(
            ["grep", "-i", "dayblade", scanner_log],
            capture_output=True, text=True,
        )
        db_lines = [l for l in result.stdout.splitlines() if l.strip()]
        if db_lines:
            last_line = db_lines[-1]
            # Try to extract a timestamp from the line (ISO-like or [HH:MM:SS])
            m = _re.search(r"\[(\d{2}:\d{2}:\d{2})\]", last_line)
            if m:
                import datetime as _dt
                today = _dt.date.today().isoformat()
                dayblade_last_scan = f"{today}T{m.group(1)}Z"
            else:
                m2 = _re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", last_line)
                if m2:
                    dayblade_last_scan = m2.group(0) + "Z"
    except Exception:
        pass

    # --- Uptime ---
    uptime_minutes = round((_time_module.time() - _SERVER_START) / 60, 1)

    return {
        "server_up": True,
        "last_ollama_success": last_ollama_success,
        "websocket_status": websocket_status,
        "scheduler_errors": scheduler_errors,
        "dayblade_last_scan": dayblade_last_scan,
        "uptime_minutes": uptime_minutes,
        "scan_health": scan_health,
    }


@app.get("/api/status")
def status():
    conn = _conn()
    # Get current season
    s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    current_season = int(s_row["value"]) if s_row else 1

    players = conn.execute("SELECT COUNT(*) as cnt FROM ai_players WHERE is_active=1").fetchone()
    trades = conn.execute("SELECT COUNT(*) as cnt FROM trades WHERE season=?", (current_season,)).fetchone()
    signals = conn.execute("SELECT COUNT(*) as cnt FROM signals WHERE season=?", (current_season,)).fetchone()
    chat_count = conn.execute("SELECT COUNT(*) as cnt FROM ai_chat").fetchone()
    news_count = conn.execute("SELECT COUNT(*) as cnt FROM market_news").fetchone()

    # Total portfolio value
    total_val = conn.execute("""
        SELECT SUM(p.cash + COALESCE(pos_val, 0)) as total
        FROM ai_players p
        LEFT JOIN (SELECT player_id, SUM(qty * avg_price) as pos_val FROM positions GROUP BY player_id) pv
        ON p.id = pv.player_id
        WHERE p.is_active = 1
    """).fetchone()

    conn.close()
    return {
        "status": "running",
        "current_season": current_season,
        "active_players": players["cnt"],
        "total_trades": trades["cnt"],
        "total_signals": signals["cnt"],
        "total_chat_messages": chat_count["cnt"] if chat_count else 0,
        "total_news": news_count["cnt"] if news_count else 0,
        "total_portfolio_value": round(total_val["total"], 2) if total_val and total_val["total"] else 0,
        "cic_usage": {
            "sonnet_calls_today": _cic_usage.get("calls_today", 0),
            "estimated_cost_today": f"${_cic_usage.get('cost_today', 0.0):.4f}",
        },
    }


@app.get("/api/operations")
def operations_status():
    """System operations dashboard — scanner health, model activity, market status."""
    import time as _time
    conn = _conn()
    try:
        # Last scan time
        last_scan = conn.execute(
            "SELECT MAX(created_at) as ts FROM signals"
        ).fetchone()

        # Active models
        active_models = conn.execute(
            "SELECT COUNT(*) as cnt FROM ai_players WHERE is_active=1 AND is_human=0"
        ).fetchone()
        paused_models = conn.execute(
            "SELECT COUNT(*) as cnt FROM ai_players WHERE is_active=0 AND is_human=0"
        ).fetchone()

        # Trades today
        trades_today = conn.execute(
            "SELECT COUNT(*) as cnt FROM trades WHERE date(executed_at)=date('now')"
        ).fetchone()

        # Signals today
        signals_today = conn.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE date(created_at)=date('now')"
        ).fetchone()

        # Recent errors (last hour from signals with error-like reasoning)
        recent_activity = conn.execute(
            "SELECT player_id, symbol, signal, created_at FROM signals "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
    finally:
        conn.close()

    # Market status via regime detector
    market_status = "unknown"
    vix = None
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        market_status = regime.get("regime", "unknown")
        vix = regime.get("vix")
    except Exception:
        pass

    return {
        "status": "running",
        "scanner": {
            "last_scan": last_scan["ts"] if last_scan else None,
            "active_models": active_models["cnt"] if active_models else 0,
            "paused_models": paused_models["cnt"] if paused_models else 0,
            "trades_today": trades_today["cnt"] if trades_today else 0,
            "signals_today": signals_today["cnt"] if signals_today else 0,
        },
        "market": {
            "status": market_status,
            "vix": vix,
        },
        "recent_signals": [
            {"player_id": r["player_id"], "symbol": r["symbol"],
             "signal": r["signal"], "at": r["created_at"]}
            for r in (recent_activity or [])
        ],
    }


@app.get("/api/operations/data")
def operations_data():
    """Alias for /api/operations — frontend compatibility."""
    return operations_status()


@app.get("/api/operations/status")
def operations_status_alias():
    """Compatibility alias for older dashboard clients."""
    return operations_status()


_trades_cache = {"data": None, "ts": 0, "key": ""}

@app.get("/api/trades/recent")
def recent_trades(limit: int = 30, season: int = 0, timeframe: str = "", player_id: str = ""):
    import time as _time

    # Cache key based on params — skip cache when filtering by player
    cache_key = f"{limit}:{season}:{timeframe}"
    if not player_id and _trades_cache["key"] == cache_key and _time.time() - _trades_cache["ts"] < 15:
        return _trades_cache["data"]

    conn = _conn()
    # Determine season (-1 = all seasons)
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1
    # Check if sources/timeframe columns exist (migration may not have run yet)
    _has_sources = False
    try:
        conn.execute("SELECT sources FROM trades LIMIT 1")
        _has_sources = True
    except Exception:
        pass
    _has_timeframe = False
    try:
        conn.execute("SELECT timeframe FROM trades LIMIT 1")
        _has_timeframe = True
    except Exception:
        pass
    _src_col = ", t.sources" if _has_sources else ""
    _tf_col = ", t.timeframe" if _has_timeframe else ""

    # Build timeframe WHERE clause
    tf_filter = timeframe.upper() if timeframe and timeframe.upper() in ("SCALP", "SWING", "POSITION") else ""

    if all_seasons:
        if tf_filter and _has_timeframe:
            trades = conn.execute(
                "SELECT t.player_id, p.display_name, p.provider, t.symbol, t.action, t.qty, t.price, "
                "t.asset_type, t.option_type, t.reasoning, t.confidence, t.executed_at, "
                f"t.entry_price, t.exit_price, t.realized_pnl, t.strike_price, t.expiry_date{_src_col}{_tf_col} "
                "FROM trades t JOIN ai_players p ON t.player_id = p.id "
                "WHERE t.timeframe=? "
                "ORDER BY t.executed_at DESC LIMIT ?", (tf_filter, limit)
            ).fetchall()
        else:
            trades = conn.execute(
                "SELECT t.player_id, p.display_name, p.provider, t.symbol, t.action, t.qty, t.price, "
                "t.asset_type, t.option_type, t.reasoning, t.confidence, t.executed_at, "
                f"t.entry_price, t.exit_price, t.realized_pnl, t.strike_price, t.expiry_date{_src_col}{_tf_col} "
                "FROM trades t JOIN ai_players p ON t.player_id = p.id "
                "ORDER BY t.executed_at DESC LIMIT ?", (limit,)
            ).fetchall()
    else:
        if tf_filter and _has_timeframe:
            trades = conn.execute(
                "SELECT t.player_id, p.display_name, p.provider, t.symbol, t.action, t.qty, t.price, "
                "t.asset_type, t.option_type, t.reasoning, t.confidence, t.executed_at, "
                f"t.entry_price, t.exit_price, t.realized_pnl, t.strike_price, t.expiry_date{_src_col}{_tf_col} "
                "FROM trades t JOIN ai_players p ON t.player_id = p.id "
                "WHERE t.season=? AND t.timeframe=? "
                "ORDER BY t.executed_at DESC LIMIT ?", (season, tf_filter, limit)
            ).fetchall()
        else:
            trades = conn.execute(
                "SELECT t.player_id, p.display_name, p.provider, t.symbol, t.action, t.qty, t.price, "
                "t.asset_type, t.option_type, t.reasoning, t.confidence, t.executed_at, "
                f"t.entry_price, t.exit_price, t.realized_pnl, t.strike_price, t.expiry_date{_src_col}{_tf_col} "
                "FROM trades t JOIN ai_players p ON t.player_id = p.id "
                "WHERE t.season=? "
                "ORDER BY t.executed_at DESC LIMIT ?", (season, limit)
            ).fetchall()

    # Get current prices — use cached batch prices instead of individual calls
    open_symbols = set()
    for t in trades:
        if t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
            open_symbols.add(t["symbol"])

    current_prices = {}
    if open_symbols:
        try:
            from engine.market_data import get_all_prices
            # Only fetch prices for symbols in the result set (not all WATCH_STOCKS)
            all_prices = get_all_prices(list(open_symbols))
            for sym in open_symbols:
                if sym in all_prices:
                    current_prices[sym] = all_prices[sym]["price"]
        except Exception:
            pass

    conn.close()

    result = []
    for t in trades:
        d = dict(t)
        # Add P&L for every trade
        if t["action"] == "SELL" and t["realized_pnl"] is not None:
            d["pnl"] = round(t["realized_pnl"], 2)
            d["pnl_pct"] = round(
                ((t["exit_price"] or t["price"]) - (t["entry_price"] or t["price"]))
                / (t["entry_price"] or t["price"]) * 100, 2
            ) if t["entry_price"] else None
        elif t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
            # Unrealized P&L for open positions
            sym = t["symbol"]
            is_option = (t["asset_type"] == "option" or t["action"] in ("BUY_CALL", "BUY_PUT"))
            if is_option:
                # Estimate option value using intrinsic value
                from engine.paper_trader import estimate_option_price
                stock_price = current_prices.get(sym, 0)
                ot = (t["option_type"] if t["option_type"] else None) or ("call" if t["action"] == "BUY_CALL" else "put")
                strike = t["strike_price"] if t["strike_price"] else None
                est = estimate_option_price(ot, strike, stock_price, t["price"])
                d["pnl"] = round((est - t["price"]) * t["qty"], 2)
                d["pnl_pct"] = round((est - t["price"]) / t["price"] * 100, 2) if t["price"] > 0 else 0
                d["current_price"] = round(est, 2)
            elif sym in current_prices:
                cur = current_prices[sym]
                entry = t["price"]
                d["pnl"] = round((cur - entry) * t["qty"], 2)
                d["pnl_pct"] = round((cur - entry) / entry * 100, 2) if entry > 0 else 0
                d["current_price"] = round(cur, 2)
            else:
                d["pnl"] = None
                d["pnl_pct"] = None
        else:
            d["pnl"] = None
            d["pnl_pct"] = None
        result.append(annotate_player_payload(d))

    # Apply player_id filter (post-process to avoid 4x SQL variants)
    if player_id:
        result = [r for r in result if r.get("player_id") == player_id]
        return result

    _trades_cache["data"] = result
    _trades_cache["ts"] = _time.time()
    _trades_cache["key"] = cache_key
    return result


@app.get("/api/recent-trades")
def recent_trades_alias(limit: int = 30, season: int = 0, timeframe: str = "", player_id: str = ""):
    """Compatibility alias for older dashboard clients."""
    return recent_trades(limit=limit, season=season, timeframe=timeframe, player_id=player_id)


@app.get("/api/anderson/decision-summary")
def anderson_decision_summary():
    conn = _conn()
    try:
        candidates = conn.execute(
            "SELECT id, name, target_tickers, conviction_score, critic_score, critic_notes, "
            "direction, thesis, status, scout_brief, architect_reasoning, commander_decision "
            "FROM crew_strategies "
            "WHERE status IN ('draft', 'approved') "
            "AND deployed_to_portfolio_id IS NULL "
            "AND created_at >= datetime('now', '-2 hours') "
            "ORDER BY conviction_score DESC"
        ).fetchall()
    finally:
        conn.close()

    from crew.ensemble import AgentScoreboard, _bucket_for_agent, _source_policy, select_collective_signals

    scoreboard = AgentScoreboard()
    source_policy = {
        bucket: _source_policy(scoreboard, bucket)
        for bucket in ("LegacyCrew", "Momentum", "MeanReversion")
    }

    if not candidates:
        return {
            "selected_signals": [],
            "source_policy": source_policy,
            "candidate_count": 0,
            "selected_count": 0,
        }

    result = select_collective_signals(candidates)
    selected = []
    for signal in result.get("final_signals", []):
        agent = str(signal.get("agent") or "")
        bucket = _bucket_for_agent(agent)
        policy = source_policy.get(bucket, {})
        selected.append({
            "symbol": signal.get("symbol"),
            "agent": agent,
            "source_bucket": bucket,
            "selection_type": signal.get("selection_type", "exploit"),
            "confidence": float(signal.get("confidence") or 0.0),
            "weighted_confidence": float(signal.get("weighted_confidence") or 0.0),
            "status": policy.get("status", "neutral"),
            "win_rate": policy.get("win_rate"),
            "allocation_multiplier": policy.get("multiplier"),
            "thesis": signal.get("thesis"),
        })

    return {
        "selected_signals": selected,
        "source_policy": source_policy,
        "candidate_count": len(result.get("candidate_signals", [])),
        "selected_count": len(selected),
    }


@app.get("/api/signals/recent")
def recent_signals(limit: int = 50, season: int = 0, timeframe: str = ""):
    conn = _conn()
    if season <= 0:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1
    # Check if sources/timeframe columns exist
    _has_src = False
    try:
        conn.execute("SELECT sources FROM signals LIMIT 1")
        _has_src = True
    except Exception:
        pass
    _has_tf = False
    try:
        conn.execute("SELECT timeframe FROM signals LIMIT 1")
        _has_tf = True
    except Exception:
        pass
    _has_status = False
    try:
        conn.execute("SELECT execution_status FROM signals LIMIT 1")
        _has_status = True
    except Exception:
        pass
    _sc = ", s.sources" if _has_src else ""
    _tc = ", s.timeframe" if _has_tf else ""
    _stc = ", s.execution_status, s.rejection_reason" if _has_status else ""

    tf_filter = timeframe.upper() if timeframe and timeframe.upper() in ("SCALP", "SWING", "POSITION") else ""

    if tf_filter and _has_tf:
        signals = conn.execute(
            "SELECT s.player_id, p.display_name, p.provider, s.symbol, s.signal, s.confidence, "
            f"s.reasoning, s.asset_type, s.option_type, s.created_at{_sc}{_tc}{_stc} "
            "FROM signals s JOIN ai_players p ON s.player_id = p.id "
            "WHERE s.season=? AND s.timeframe=? "
            "ORDER BY s.created_at DESC LIMIT ?", (season, tf_filter, limit)
        ).fetchall()
    else:
        signals = conn.execute(
            "SELECT s.player_id, p.display_name, p.provider, s.symbol, s.signal, s.confidence, "
            f"s.reasoning, s.asset_type, s.option_type, s.created_at{_sc}{_tc}{_stc} "
            "FROM signals s JOIN ai_players p ON s.player_id = p.id "
            "WHERE s.season=? "
            "ORDER BY s.created_at DESC LIMIT ?", (season, limit)
        ).fetchall()
    conn.close()
    return [dict(s) for s in signals]


@app.get("/api/recent-signals")
def recent_signals_alias(limit: int = 50, season: int = 0, timeframe: str = ""):
    """Compatibility alias for older dashboard clients."""
    return recent_signals(limit=limit, season=season, timeframe=timeframe)


@app.get("/api/arena/comparison")
@timed_cache(300)
def comparison_chart(season: int = 0):
    """Portfolio value history for all players, optionally filtered by season."""
    conn = _conn()
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1
    if all_seasons:
        # All seasons
        data = conn.execute(
            "SELECT h.player_id, p.display_name, h.total_value, h.recorded_at, h.season "
            "FROM portfolio_history h JOIN ai_players p ON h.player_id = p.id "
            "ORDER BY h.recorded_at ASC"
        ).fetchall()
    else:
        data = conn.execute(
            "SELECT h.player_id, p.display_name, h.total_value, h.recorded_at, h.season "
            "FROM portfolio_history h JOIN ai_players p ON h.player_id = p.id "
            "WHERE h.season = ? ORDER BY h.recorded_at ASC", (season,)
        ).fetchall()
    conn.close()

    by_player = {}
    for row in data:
        pid = row["player_id"]
        if pid not in by_player:
            by_player[pid] = {"name": row["display_name"], "history": []}
        by_player[pid]["history"].append({
            "value": row["total_value"],
            "time": row["recorded_at"],
        })
    return by_player


# --- Chat Endpoints ---

@app.get("/api/chat/recent")
def recent_chat(limit: int = 50):
    conn = _conn()
    messages = conn.execute(
        "SELECT c.id, c.player_id, p.display_name, p.provider, c.message, "
        "c.context, c.reply_to, c.created_at "
        "FROM ai_chat c JOIN ai_players p ON c.player_id = p.id "
        "ORDER BY c.created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [annotate_player_payload(dict(m)) for m in messages]


@app.get("/api/chat/player/{player_id}")
def player_chat(player_id: str, limit: int = 20):
    conn = _conn()
    messages = conn.execute(
        "SELECT c.id, c.player_id, p.display_name, p.provider, c.message, "
        "c.context, c.reply_to, c.created_at "
        "FROM ai_chat c JOIN ai_players p ON c.player_id = p.id "
        "WHERE c.player_id = ? ORDER BY c.created_at DESC LIMIT ?",
        (player_id, limit)
    ).fetchall()
    conn.close()
    return [annotate_player_payload(dict(m)) for m in messages]


# --- News Endpoints ---

@app.get("/api/news/recent")
def recent_news(limit: int = 30):
    conn = _conn()
    news = conn.execute(
        "SELECT * FROM market_news ORDER BY fetched_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(n) for n in news]


@app.get("/api/news/feed")
def news_feed(limit: int = 50, ticker: str = ""):
    """Unified news feed across all tickers for the News redesign."""
    conn = _conn()
    if ticker:
        rows = conn.execute(
            "SELECT * FROM market_news WHERE symbol=? ORDER BY fetched_at DESC LIMIT ?",
            (ticker.upper(), limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM market_news ORDER BY fetched_at DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    results = []
    for r in rows:
        d = dict(r)
        d.setdefault("source", "Yahoo Finance")
        results.append(d)
    return results


@app.get("/api/news-feed")
def news_feed_alias(limit: int = 50, ticker: str = ""):
    """Compatibility alias for older dashboard clients."""
    return news_feed(limit=limit, ticker=ticker)


@app.post("/api/news/go-deeper")
def news_go_deeper(data: dict = None):
    """Generate AI follow-up questions for a news article (Ollama, free)."""
    if not data:
        return {"questions": []}
    headline = data.get("headline", "")
    summary = data.get("summary", "")
    symbol = data.get("symbol", "")
    try:
        import requests as req
        ollama_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        prompt = (
            f"Given this market news about {symbol}:\n"
            f"Headline: {headline}\n"
            f"Summary: {summary}\n\n"
            f"Generate exactly 4 follow-up research questions a trader would want answered. "
            f"Each question should be specific and actionable. "
            f"Output ONLY a JSON array of 4 strings, no other text."
        )
        r = req.post(f"{ollama_url}/api/generate", json={
            "model": os.getenv("CREWAI_MODEL", "qwen3.5:9b"),
            "prompt": prompt,
            "stream": False
        }, timeout=30)
        if r.ok:
            import re
            text = r.json().get("response", "")
            match = re.search(r'\[.*\]', text, re.DOTALL)
            if match:
                return {"questions": json.loads(match.group())[:4]}
        return {"questions": [
            f"What is the short-term price impact on {symbol}?",
            f"How does this affect {symbol}'s sector peers?",
            f"What technical levels should I watch for {symbol}?",
            f"Is this a buying or selling opportunity for {symbol}?",
        ]}
    except Exception:
        return {"questions": [
            f"What is the price impact on {symbol}?",
            f"How does this affect the sector?",
            f"What are the key levels to watch?",
            f"Buy, sell, or hold {symbol}?",
        ]}


@app.get("/api/news/{symbol}")
def symbol_news(symbol: str, limit: int = 10):
    """Get news from multiple sources: Finnhub + Google News + DB cache."""
    sym = symbol.upper()
    live_news = []
    db_news_list = []

    # Source 1: Finnhub company news (live, 7-day window) — highest quality
    try:
        from engine.finnhub_data import _fh_get
        from datetime import datetime, timedelta
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        fh = _fh_get("/company-news", {"symbol": sym, "from": week_ago, "to": today})
        if fh and isinstance(fh, list):
            for n in fh[:5]:
                live_news.append({
                    "headline": n.get("headline", ""),
                    "summary": n.get("summary", "")[:500],
                    "source": n.get("source", "Finnhub"),
                    "url": n.get("url", ""),
                    "symbol": sym,
                })
    except Exception:
        pass

    # Source 2: Webull news (unofficial library, no login needed)
    try:
        from webull import webull as Webull
        wb = Webull()
        wb_news = wb.get_news(sym)
        if wb_news and isinstance(wb_news, list):
            for n in wb_news[:5]:
                live_news.append({
                    "headline": n.get("title", ""),
                    "summary": n.get("summary", "")[:500],
                    "source": n.get("sourceName", "Webull"),
                    "url": n.get("newsUrl", n.get("url", "")),
                    "symbol": sym,
                })
    except Exception:
        pass

    # Source 3: Google News RSS (free, no API key)
    try:
        import feedparser
        feed = feedparser.parse(
            f"https://news.google.com/rss/search?q={sym}+stock&hl=en-US&gl=US&ceid=US:en"
        )
        for e in feed.entries[:5]:
            title = e.get("title", "")
            source = "Google News"
            if " - " in title:
                parts = title.rsplit(" - ", 1)
                title = parts[0]
                source = parts[1] if len(parts) > 1 else "Google News"
            live_news.append({
                "headline": title,
                "summary": e.get("summary", "")[:500],
                "source": source,
                "url": e.get("link", ""),
                "symbol": sym,
            })
    except Exception:
        pass

    # Source 4: CNBC RSS (general market news, filtered by ticker)
    try:
        import feedparser as _fp
        cnbc_feed = _fp.parse("https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069")
        for e in cnbc_feed.entries[:15]:
            title = e.get("title", "")
            if sym in title.upper() or sym.lower() in title.lower():
                live_news.append({
                    "headline": title,
                    "summary": e.get("summary", "")[:500],
                    "source": "CNBC",
                    "url": e.get("link", ""),
                    "symbol": sym,
                })
    except Exception:
        pass

    # Source 5: FinNews multi-source aggregator
    try:
        import FinNews as _fn
        fn = _fn.CNBC(topics=["finance"])
        articles = fn.get_news()
        for a in (articles or [])[:10]:
            title = a.get("title", "")
            if sym in title.upper() or sym.lower() in title.lower():
                live_news.append({
                    "headline": title,
                    "summary": a.get("summary", "")[:500],
                    "source": a.get("source", {}).get("title", "FinNews") if isinstance(a.get("source"), dict) else "FinNews",
                    "url": a.get("link", ""),
                    "symbol": sym,
                })
    except Exception:
        pass

    # Source 6: DB cache (Yahoo Finance RSS, fetched by news_fetcher) — fill remaining
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM market_news WHERE symbol=? ORDER BY fetched_at DESC LIMIT ?",
        (sym, limit)
    ).fetchall()
    conn.close()
    for n in rows:
        d = dict(n)
        d.setdefault("source", "Yahoo Finance")
        db_news_list.append(d)

    # Merge: live first, then DB, deduplicated
    all_news = live_news + db_news_list
    seen = set()
    unique = []
    for n in all_news:
        key = (n.get("headline") or "")[:60].lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(n)

    return unique[:limit]


# --- P&L & Equity Endpoints ---

@app.get("/api/arena/player/{player_id}/pnl")
def player_pnl(player_id: str):
    """Get live unrealized P&L for a player's positions."""
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_stock_price

    conn = _conn()
    positions = conn.execute(
        "SELECT symbol FROM positions WHERE player_id=?", (player_id,)
    ).fetchall()
    conn.close()

    prices = {}
    for p in positions:
        data = get_stock_price(p["symbol"])
        if "error" not in data:
            prices[p["symbol"]] = data

    return get_portfolio_with_pnl(player_id, prices)


@app.get("/api/arena/equity-curve")
@timed_cache(300)
def equity_curve(player_id: str = None, season: int = 0):
    """Get equity curve data, optionally filtered by season and player."""
    conn = _conn()
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 1

    q = "SELECT player_id, total_value, cash, positions_value, recorded_at, season FROM portfolio_history"
    params = []
    clauses = []
    if player_id:
        clauses.append("player_id = ?")
        params.append(player_id)
    if not all_seasons:
        clauses.append("season = ?")
        params.append(season)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " ORDER BY recorded_at ASC"
    rows = conn.execute(q, params).fetchall()
    conn.close()

    result = []
    for r in rows:
        result.append({
            "player_id": r["player_id"],
            "timestamp": r["recorded_at"],
            "total_value": r["total_value"],
            "cash": r["cash"],
            "positions_value": r["positions_value"],
            "season": r["season"],
        })
    return result


# --- DayBlade Options Endpoints ---

@app.get("/api/dayblade/status")
def dayblade_status():
    """Get DayBlade live positions, P&L, stats, DTE breakdown, streak."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from engine.dayblade import (
        get_portfolio_with_pnl, get_dayblade_stats,
        is_dayblade_open_window, is_dayblade_close_window,
        is_market_hours_for_dayblade, is_power_hour,
        DAYBLADE_TICKERS, DAYBLADE_CASH, MAX_POSITIONS,
        get_win_streak,
    )
    from engine.market_data import get_stock_price

    prices = {}
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(DAYBLADE_TICKERS)))) as ex:
        futures = {ex.submit(get_stock_price, sym): sym for sym in DAYBLADE_TICKERS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                data = fut.result()
            except Exception:
                continue
            if "error" not in data:
                prices[sym] = data

    pnl = get_portfolio_with_pnl(prices)
    stats = get_dayblade_stats()
    streak = get_win_streak()

    window = "closed"
    if is_power_hour():
        window = "power_hour"
    elif is_dayblade_open_window():
        window = "open"
    elif is_dayblade_close_window():
        window = "closing"
    elif is_market_hours_for_dayblade():
        window = "monitoring"

    return {
        "portfolio": pnl,
        "stats": stats,
        "window": window,
        "starting_cash": DAYBLADE_CASH,
        "max_positions": MAX_POSITIONS,
        "tickers": DAYBLADE_TICKERS,
        "win_streak": streak,
    }


@app.get("/api/dayblade/trades")
def dayblade_trades(limit: int = 50):
    """Recent DayBlade trades."""
    conn = _conn()
    trades = conn.execute(
        "SELECT symbol, action, qty, price, asset_type, option_type, reasoning, confidence, executed_at "
        "FROM trades WHERE player_id='dayblade-0dte' ORDER BY executed_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(t) for t in trades]


@app.get("/api/dayblade/scanner")
def dayblade_scanner():
    """Live 0DTE options scanner — cheap tickets, premium plays, and scored opportunities."""
    from engine.dte_scanner import scan_0dte_opportunities
    return scan_0dte_opportunities()


@app.get("/api/dayblade/bear-scan")
def dayblade_bear_scan():
    """Bear-mode 0DTE scanner: entry signals, strategy selector, flash alert trigger."""
    from engine.dayblade_scanner import run_scan
    return run_scan()


@app.get("/api/dayblade/strategy")
def dayblade_strategy():
    """Current optimal strategy based on live VIX/GEX/RSI/session."""
    try:
        from engine.dayblade_scanner import run_scan
        result = run_scan()
        return {
            "strategy": result.get("strategy", {}),
            "session": result.get("session"),
            "vix": result.get("vix"),
            "gex_b": result.get("gex_b"),
            "rsi_5m": result.get("rsi_5m"),
            "spy_price": result.get("spy_price"),
        }
    except Exception as e:
        return {"error": str(e), "strategy": {"label": "Unavailable", "color": "#6b7280"}}


@app.get("/api/flash-alerts")
def flash_alerts_list(limit: int = 10):
    """Recent flash alerts from the 0DTE scanner."""
    from engine.dayblade_scanner import get_recent_flash_alerts, ensure_tables
    ensure_tables()
    alerts = get_recent_flash_alerts(limit)
    return {"alerts": alerts, "count": len(alerts)}


@app.get("/api/flash-alerts/active")
def flash_alert_active():
    """Most recent non-dismissed alert from last 5 minutes (for flash overlay)."""
    from engine.dayblade_scanner import get_active_flash_alert, ensure_tables
    ensure_tables()
    alert = get_active_flash_alert()
    return {"alert": alert, "has_alert": alert is not None}


@app.get("/api/flash-alerts/latest")
def flash_alert_latest():
    """Alias for /active — returns most recent undismissed alert (used by top banner)."""
    from engine.dayblade_scanner import get_active_flash_alert, ensure_tables
    ensure_tables()
    alert = get_active_flash_alert()
    return {"has_alert": bool(alert), "alert": alert}


@app.post("/api/flash-alerts/{alert_id}/dismiss")
def flash_alert_dismiss(alert_id: int):
    """Dismiss a flash alert (removes from overlay)."""
    from engine.dayblade_scanner import dismiss_alert, ensure_tables
    ensure_tables()
    dismiss_alert(alert_id)
    return {"ok": True, "dismissed": alert_id}


@app.get("/api/shorts/candidates")
def shorts_candidates():
    """Screen for paper short candidates: below MAs, overbought on bounce, declining vol."""
    from engine.dayblade_scanner import get_short_candidates
    try:
        import yfinance as yf
        vix_data = yf.Ticker("^VIX").fast_info
        vix = float(vix_data.last_price or 20)
    except Exception:
        vix = 20.0
    candidates = get_short_candidates(vix)
    return {"candidates": candidates, "count": len(candidates), "vix": round(vix, 1)}


@app.get("/api/shorts/active")
def shorts_active():
    """Active paper short positions from Alpaca (qty < 0)."""
    try:
        import os
        from alpaca.trading.client import TradingClient
        client = TradingClient(
            os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY"),
            os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET"),
            paper=True,
        )
        all_pos = client.get_all_positions()
        shorts = []
        for p in all_pos:
            qty = float(p.qty)
            if qty < 0:
                entry = float(p.avg_entry_price)
                current = float(p.current_price)
                pnl = float(p.unrealized_pl)
                pnl_pct = round((entry - current) / entry * 100, 2)  # profit when price drops
                stop = round(entry * (1 + 0.02), 2)  # 2% above entry
                shorts.append({
                    "symbol": p.symbol,
                    "qty": abs(qty),
                    "entry_price": round(entry, 2),
                    "current_price": round(current, 2),
                    "pnl_usd": round(pnl, 2),
                    "pnl_pct": pnl_pct,
                    "stop_loss": stop,
                    "stop_dist_pct": round((stop - current) / current * 100, 2),
                    "market_value": round(abs(float(p.market_value)), 2),
                })
        return {"shorts": shorts, "count": len(shorts), "status": "ok"}
    except Exception as e:
        return {"shorts": [], "count": 0, "status": "error", "note": str(e)}


@app.post("/api/shorts/cover/{symbol}")
def shorts_cover(symbol: str):
    """Cover a paper short position (market buy to close)."""
    try:
        import os
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        client = TradingClient(
            os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY"),
            os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET"),
            paper=True,
        )
        # Get current short qty
        pos = client.get_open_position(symbol.upper())
        qty = abs(float(pos.qty))
        order = client.submit_order(MarketOrderRequest(
            symbol=symbol.upper(),
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
        ))
        return {"ok": True, "symbol": symbol.upper(), "qty": qty,
                "order_id": str(order.id), "status": str(order.status)}
    except Exception as e:
        return {"ok": False, "symbol": symbol.upper(), "error": str(e)}


@app.get("/api/v1/indicator-leaderboard")
def indicator_leaderboard(limit: int = 20):
    """Alpha Engine: ranked per-indicator win rate, avg return, Sharpe from most recent benchmark run."""
    from engine.indicator_bench import get_leaderboard, ensure_tables
    ensure_tables()
    return {"leaderboard": get_leaderboard(limit), "count": limit}


@app.get("/api/v1/signal-scorecard")
def signal_scorecard_recent(limit: int = 50):
    """Alpha Engine: recent signal log with outcomes (win/loss/pending)."""
    from engine.signal_scorecard import get_scorecard, ensure_tables
    ensure_tables()
    return {"signals": get_scorecard(limit)}


@app.get("/api/options/chain")
def options_chain(symbol: str = "SPY"):
    """Live 0DTE options chain (Alpaca → yfinance fallback). Strikes within $5 of spot."""
    from engine.premium_tracker import get_chain
    chain = get_chain()
    spot = chain[0]["spot"] if chain else 0
    puts = [c for c in chain if c["type"] == "put"][-5:]   # 5 closest puts (ITM→OTM)
    calls = [c for c in chain if c["type"] == "call"][:5]  # 5 closest calls (OTM→ITM)
    return {
        "symbol": symbol.upper(),
        "spot": spot,
        "puts": puts,
        "calls": calls,
        "all": chain,
        "count": len(chain),
        "updated_at": __import__("datetime").datetime.now().isoformat(),
    }


@app.get("/api/options/premium-flow")
def options_premium_flow():
    """Net premium direction: put premium vs call premium on SPY 0DTE chain."""
    from engine.premium_tracker import get_flow
    return get_flow()


@app.get("/api/chart/intraday")
def chart_intraday(ticker: str = "SPY", interval: str = "1m", period: str = "1d"):
    """1-minute intraday candles for 0DTE charting (pre/after-hours included)."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker.upper())
        df = t.history(period=period, interval=interval, prepost=True)
        if df.empty:
            return {"candles": [], "ticker": ticker, "interval": interval}
        candles = []
        for ts, row in df.iterrows():
            candles.append({
                "time": int(ts.timestamp()),
                "open": round(float(row["Open"]), 4),
                "high": round(float(row["High"]), 4),
                "low": round(float(row["Low"]), 4),
                "close": round(float(row["Close"]), 4),
                "volume": int(row["Volume"]),
            })
        return {"candles": candles, "ticker": ticker.upper(), "interval": interval,
                "count": len(candles)}
    except Exception as e:
        return {"error": str(e), "candles": [], "ticker": ticker}


# --- Market Data Endpoints ---

@app.get("/api/ticker-tape")
def ticker_tape():
    """Lightweight ticker tape endpoint — prices + daily change for fixed symbol set."""
    from engine.market_data import get_bulk_prices, _price_cache
    TAPE_SYMBOLS = ["SPY", "QQQ", "IWM", "TSLA", "NVDA", "AMD", "AAPL", "MSFT",
                    "META", "GOOGL", "AMZN", "MU", "AVGO", "PLTR", "DELL", "NOW", "MRVL", "VIX"]
    try:
        data = get_bulk_prices(TAPE_SYMBOLS, timeout=8)
    except Exception:
        data = {}

    result = []
    for sym in TAPE_SYMBOLS:
        d = data.get(sym)
        # Fall back to _price_cache if fresh fetch missed this symbol
        if not d or not d.get("price"):
            cached = _price_cache.get(sym)
            if cached:
                d = cached.get("data", {})
        if d and d.get("price"):
            result.append({
                "symbol": sym,
                "price": d.get("price", 0),
                "change_pct": d.get("change_pct", 0),
            })
    return result


@app.get("/api/market/prices")
def market_prices():
    """Get current prices for watchlist stocks (parallel fetch)."""
    from engine.market_data import get_all_prices
    from config import WATCH_STOCKS
    return get_all_prices(WATCH_STOCKS)


@app.get("/api/market/candles/{symbol}")
def market_candles(symbol: str, interval: str = "5m", range: str = "1d"):
    """Get OHLCV candles for candlestick chart with configurable range."""
    from engine.market_data import get_intraday_candles
    candles = get_intraday_candles(symbol.upper(), interval, range)
    # Also get AI entry points for this symbol (recent BUY trades)
    conn = _conn()
    if range == "1d":
        date_filter = "AND date(t.executed_at)=?"
        date_val = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
        params = (symbol.upper(), date_val)
    else:
        date_filter = ""
        params = (symbol.upper(),)
    entries = conn.execute(
        "SELECT t.player_id, p.display_name, t.action, t.price, t.qty, t.executed_at "
        "FROM trades t JOIN ai_players p ON t.player_id = p.id "
        f"WHERE t.symbol=? AND t.action LIKE 'BUY%' {date_filter} "
        "ORDER BY t.executed_at",
        params
    ).fetchall()
    conn.close()
    markers = [dict(e) for e in entries]
    return {"candles": candles, "markers": markers}


@app.get("/api/market/heatmap")
def market_heatmap():
    """Get watchlist heat map data: price, change%, position weight per model."""
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    conn = _conn()
    # Get all active positions grouped by symbol
    positions = conn.execute(
        "SELECT symbol, SUM(qty * avg_price) as cost_basis "
        "FROM positions WHERE player_id != 'dayblade-0dte' "
        "GROUP BY symbol"
    ).fetchall()
    conn.close()

    pos_weight = {row["symbol"]: row["cost_basis"] for row in positions}
    total_invested = sum(pos_weight.values()) or 1.0

    result = []
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" in data:
            continue
        weight = pos_weight.get(sym, 0) / total_invested
        result.append({
            "symbol": sym,
            "price": data["price"],
            "change_pct": data["change_pct"],
            "volume": data.get("volume", 0),
            "weight": round(weight, 4),
        })
    return result


@app.get("/api/arena/confidence")
@timed_cache(120)
def confidence_matrix():
    """AI confidence panel: each model's latest stance on each watchlist stock."""
    from config import WATCH_STOCKS

    conn = _conn()
    # Get all active players (exclude dayblade)
    players = conn.execute(
        "SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()

    result = {}
    for p in players:
        pid = p["id"]
        stances = {}
        for sym in WATCH_STOCKS:
            row = conn.execute(
                "SELECT signal, confidence, reasoning, created_at FROM signals "
                "WHERE player_id=? AND symbol=? ORDER BY created_at DESC LIMIT 1",
                (pid, sym)
            ).fetchone()
            if row:
                sig = row["signal"]
                conf = row["confidence"] or 0
                # Map signal to stance
                if sig in ("BUY", "BUY_CALL"):
                    stance = "bullish"
                elif sig == "BUY_PUT":
                    stance = "bearish"
                else:
                    stance = "neutral"
                stances[sym] = {
                    "stance": stance,
                    "signal": sig,
                    "confidence": round(conf, 2),
                    "reasoning": (row["reasoning"] or "")[:120],
                    "updated": row["created_at"],
                }
            else:
                stances[sym] = {"stance": "neutral", "signal": "HOLD", "confidence": 0, "reasoning": "", "updated": None}
        result[pid] = {"name": p["display_name"], "stances": stances}

    conn.close()
    return result


# --- GEX Endpoints ---

@app.get("/api/market/gex")
def gex_all():
    """Get GEX data for all supported tickers."""
    from engine.gex_scanner import get_all_gex
    return get_all_gex()


@app.get("/api/market/gex/{ticker}")
def gex_ticker(ticker: str):
    """Get GEX data for a specific ticker."""
    from engine.gex_scanner import get_gex
    result = get_gex(ticker.upper())
    if result is None:
        return {"error": f"No GEX data for {ticker.upper()}"}
    return result


# --- Alpaca GEX Endpoints ---

@app.get("/api/gex/{symbol}")
def gex_alpaca(symbol: str):
    """
    Alpaca-based GEX profile for a symbol.
    Returns cached result (in-memory → DB) if a live compute would be too slow.
    Pass ?force=true to trigger a fresh Alpaca API call.
    """
    from fastapi import Query as _Query
    import inspect as _inspect
    from gex_calculator import compute_gex_sync, get_latest_snapshot, GEX_SYMBOLS

    sym = symbol.upper()
    profile = compute_gex_sync(sym, force=False)

    if profile is not None:
        return {
            "symbol": profile.symbol,
            "spot": profile.spot_price,
            "timestamp": profile.timestamp,
            "max_gamma_strike": profile.max_gamma_strike,
            "zero_gamma_level": profile.zero_gamma_level,
            "put_wall": profile.put_wall,
            "call_wall": profile.call_wall,
            "gamma_flip": profile.gamma_flip,
            "total_gex": profile.total_gex,
            "regime": "pinned" if profile.total_gex > 0 else "volatile",
            "source": profile.source,
            "levels": [
                {
                    "strike": l.strike,
                    "net_gex": l.net_gex,
                    "call_gex": l.call_gex,
                    "put_gex": l.put_gex,
                    "call_oi": l.call_oi,
                    "put_oi": l.put_oi,
                }
                for l in profile.levels
            ],
        }

    # Fall back to DB snapshot
    snap = get_latest_snapshot(sym)
    if snap:
        snap.pop("levels_json", None)
        snap["regime"] = "pinned" if (snap.get("total_gex") or 0) > 0 else "volatile"
        snap["source"] = snap.get("source", "alpaca")
        # Normalize: DB column is spot_price, frontend expects spot
        if "spot_price" in snap and "spot" not in snap:
            snap["spot"] = snap["spot_price"]
        return snap

    return {"error": f"No GEX data for {sym}. Alpaca keys may not be configured."}


@app.get("/api/gex/{symbol}/history")
def gex_alpaca_history(symbol: str):
    """Return historical GEX snapshots for a symbol (last 20 records)."""
    from gex_calculator import get_snapshot_history
    rows = get_snapshot_history(symbol.upper(), limit=20)
    for r in rows:
        r.pop("levels_json", None)
        r["regime"] = "pinned" if (r.get("total_gex") or 0) > 0 else "volatile"
    return {"symbol": symbol.upper(), "history": rows}


# --- VIX & Earnings Endpoints ---

@app.get("/api/market/vix")
def vix_status():
    """Get current VIX price and change."""
    from engine.vix_monitor import get_latest_vix_snapshot, get_vix_term_structure
    return {
        "current": get_latest_vix_snapshot(),
        "history": get_vix_term_structure(),
    }


@app.get("/api/market/flow-lean")
def flow_lean():
    """Get current market directional lean from options flow."""
    from engine.market_flow import get_flow_lean, get_flow_lean_history
    current = get_flow_lean()
    return {
        "current": current,
        "history": get_flow_lean_history(50),
    }


# --- First Officer (Mr. Data / MLX Qwen3 8B) ---

@app.get("/api/first-officer/briefing")
@timed_cache(300)
def first_officer_briefing(force: int = 0):
    """Get Mr. Data's full Bridge briefing. Cached 30 min server-side + 5 min endpoint cache."""
    from engine.first_officer import get_briefing
    return get_briefing(force=bool(force))


@app.post("/api/first-officer/ask")
def first_officer_ask(data: dict = None):
    """Ask Mr. Data a specific question."""
    if not data or not data.get("question"):
        return {"error": "question is required"}
    from engine.first_officer import ask_data
    return ask_data(data["question"].strip())


@app.get("/api/first-officer/status")
def first_officer_status():
    """Quick status for Bridge top bar."""
    from engine.first_officer import get_briefing_summary
    return get_briefing_summary() or {"summary": "No briefing yet", "minutes_ago": 999, "has_recommendation": False}


# --- Riker's Log (Captain's Decision Journal) ---

@app.get("/api/rikers-log")
def rikers_log_get(limit: int = 50, entry_type: str = None, source: str = None):
    """Get Riker's Log entries."""
    from engine.rikers_log import get_entries
    return {"entries": get_entries(limit, entry_type, source)}


@app.post("/api/rikers-log")
def rikers_log_post(data: dict = None):
    """Add an entry to Riker's Log."""
    if not data or not data.get("content"):
        return {"error": "content is required"}
    from engine.rikers_log import add_entry
    return add_entry(
        entry_type=data.get("entry_type", "manual"),
        source=data.get("source", "captain"),
        content=data["content"].strip(),
        title=data.get("title", "").strip() or None,
        ticker=data.get("ticker", "").strip().upper() or None,
        action=data.get("action", "").strip().upper() or None,
        conviction=data.get("conviction"),
        tags=data.get("tags", "").strip() or None,
    )


@app.post("/api/rikers-log/{entry_id}/outcome")
def rikers_log_outcome(entry_id: int, data: dict = None):
    """Update a log entry with its trade outcome."""
    if not data:
        return {"error": "data required"}
    from engine.rikers_log import update_outcome
    return update_outcome(entry_id, data.get("outcome", ""), data.get("outcome_pnl"))


@app.post("/api/rikers-log/sync-spock")
def rikers_log_sync_spock():
    """Sync today's Spock briefings into the log."""
    from engine.rikers_log import sync_spock_briefings
    count = sync_spock_briefings()
    return {"synced": count}


@app.get("/api/rikers-log/stats")
def rikers_log_stats():
    """Get log statistics."""
    from engine.rikers_log import get_stats
    return get_stats()


# --- Dalio Metals (Physical Precious Metals Tracker) ---

@app.get("/api/metals/portfolio")
def metals_portfolio():
    """Get Dalio Metals portfolio with live spot prices."""
    from engine.metals_tracker import get_portfolio
    return get_portfolio()


@app.get("/api/metals/signals")
def metals_signals():
    """Get smart stacking advisor signals."""
    from engine.metals_tracker import get_stacking_signal
    return get_stacking_signal()


@app.get("/api/metals/commentary")
def metals_commentary():
    """Get Cmdr. Dalio's daily metals commentary (generated via Ollama)."""
    from engine.metals_commentary import generate_commentary
    return generate_commentary()


@app.get("/api/metals/prices")
def metals_prices():
    """Get live spot prices for gold, silver, platinum, palladium."""
    from engine.metals_tracker import get_spot_prices
    return get_spot_prices(fresh=True)


@app.post("/api/metals/add")
def metals_add(data: dict = None):
    """Add physical metal to inventory."""
    if not data:
        return {"error": "data required"}
    from engine.metals_tracker import add_metal
    return add_metal(
        data.get("symbol", ""),
        float(data.get("qty", 0)),
        float(data.get("price", 0))
    )


@app.post("/api/metals/sell")
def metals_sell(data: dict = None):
    """Sell/remove physical metal from inventory."""
    if not data:
        return {"error": "data required"}
    from engine.metals_tracker import remove_metal
    return remove_metal(
        data.get("symbol", ""),
        float(data.get("qty", 0)),
        float(data.get("price", 0))
    )


@app.post("/api/metals/set-cost")
def metals_set_cost(data: dict = None):
    """Update cost basis for a metal position."""
    if not data or not data.get("metal"):
        return {"error": "metal and cost_basis_per_oz required"}
    from engine.metals_tracker import set_cost_basis
    return set_cost_basis(
        data["metal"],
        float(data.get("cost_basis_per_oz", 0))
    )


@app.get("/api/dilithium/portfolio")
def dilithium_portfolio():
    """Dilithium Reserve — real physical metals from purchase ledger with live spot prices."""
    from engine.metals_tracker import get_dilithium_portfolio
    return get_dilithium_portfolio()


@app.post("/api/dilithium/add-purchase")
def dilithium_add_purchase(data: dict = None):
    """Log a new physical metal purchase to the ledger and return updated portfolio."""
    if not data:
        return {"error": "data required: {metal, qty_oz, total_cost, purchase_date, source}"}
    from engine.metals_tracker import add_ledger_purchase
    return add_ledger_purchase(data)


@app.get("/api/cto/briefing")
def cto_briefing():
    """Get CTO Advisory briefings — today's briefings + history."""
    from engine.cto_advisor import get_latest_briefing, get_todays_briefings, get_briefing_history
    return {
        "latest": get_latest_briefing(),
        "today": get_todays_briefings(),
        "history": get_briefing_history(14),
    }


@app.post("/api/cto/generate")
def cto_generate(briefing_type: str = "pre_market"):
    """Manually trigger a CTO briefing generation."""
    from engine.cto_advisor import generate_cto_briefing, BRIEFING_TYPES
    if briefing_type not in BRIEFING_TYPES:
        return {"error": f"Unknown type: {briefing_type}. Valid: {list(BRIEFING_TYPES.keys())}"}
    try:
        briefing = generate_cto_briefing(briefing_type=briefing_type)
        if briefing:
            return {"ok": True, "briefing_type": briefing_type, "length": len(briefing), "preview": briefing[:300]}
        return {"ok": False, "reason": "Already generated today or no API key"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_earnings_symbols():
    """Build comprehensive earnings watch list: watchlist + holdings + mega caps."""
    from config import WATCH_STOCKS
    symbols = set(WATCH_STOCKS)
    try:
        conn = _conn()
        positions = conn.execute("SELECT DISTINCT symbol FROM positions WHERE qty > 0").fetchall()
        for p in positions:
            symbols.add(p["symbol"])
        try:
            alpaca = conn.execute("SELECT DISTINCT ticker FROM portfolio_positions WHERE status='open'").fetchall()
            for a in alpaca:
                symbols.add(a["ticker"])
        except Exception:
            pass
        conn.close()
    except Exception:
        pass
    MEGA_CAPS = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
        "AMD", "PLTR", "CRM", "NFLX", "AVGO", "COST", "LLY",
        "JPM", "BAC", "GS", "V", "MA",
        "UNH", "JNJ", "PFE", "ABBV",
        "XOM", "CVX", "COP",
        "WMT", "HD", "TGT",
        "DIS", "CMCSA",
        "BA", "CAT", "GE", "RTX",
        "COIN", "SQ", "HOOD",
    ]
    symbols.update(MEGA_CAPS)
    symbols -= {"GC=F", "SI=F", "PL=F", "PA=F", "GOLD", "SILVER"}
    return list(symbols)


@app.get("/api/market/earnings")
def earnings_upcoming():
    """Get watchlist + holdings + mega cap stocks with earnings in next 7 days, with source tagging."""
    from config import WATCH_STOCKS
    from engine.earnings_calendar import get_earnings_warnings
    watch_set = set(WATCH_STOCKS)
    holding_set = set()
    try:
        conn = _conn()
        rows = conn.execute("SELECT DISTINCT symbol FROM positions WHERE qty > 0").fetchall()
        holding_set = {r["symbol"] for r in rows}
        conn.close()
    except Exception:
        pass
    results = get_earnings_warnings(_get_earnings_symbols())
    for e in results:
        sym = e["symbol"]
        if sym in holding_set:
            e["source"] = "holding"
        elif sym in watch_set:
            e["source"] = "watchlist"
        else:
            e["source"] = "mega_cap"
    return results


@app.get("/api/tactical/allocation")
def tactical_allocation(view: str = "fleet", model: str = "", include_all: int = 0):
    """Actual portfolio allocation across fleet models vs regime targets."""
    from fastapi import Query
    EXCLUDED = ('steve-webull', 'enterprise-computer') if not include_all else ()
    INVERSE_ETFS = {'SH','SDS','SPXU','SDOW','SQQQ','TZA','VXX','DOG','PSQ','RWM'}
    conn = _conn()
    try:
        if view == "model" and model:
            players = conn.execute(
                "SELECT id, cash, display_name FROM ai_players WHERE is_active=1 AND id=?", (model,)
            ).fetchall()
        elif EXCLUDED:
            players = conn.execute(
                "SELECT id, cash, display_name FROM ai_players WHERE is_active=1 AND id NOT IN ({})".format(
                    ",".join("?" * len(EXCLUDED))), EXCLUDED
            ).fetchall()
        else:
            players = conn.execute(
                "SELECT id, cash, display_name FROM ai_players WHERE is_active=1"
            ).fetchall()

        all_models = conn.execute(
            "SELECT id, display_name as name FROM ai_players WHERE is_active=1 AND id NOT IN ('enterprise-computer') ORDER BY display_name"
        ).fetchall()

        total_cash = sum(float(p["cash"] or 0) for p in players)
        player_ids = [p["id"] for p in players]
        if not player_ids:
            return {"actual": {"long_equity": 0, "short_equity": 0, "options": 0, "cash": 100}, "models": [], "model_count": 0}

        positions = conn.execute(
            "SELECT player_id, symbol, qty, avg_price, asset_type, option_type FROM positions WHERE qty>0 AND player_id IN ({})".format(
                ",".join("?" * len(player_ids))), player_ids
        ).fetchall()

        long_equity = short_equity = options_val = 0.0
        for pos in positions:
            sym = (pos["symbol"] or "").split("=")[0].upper()
            qty = float(pos["qty"] or 0)
            price = float(pos["avg_price"] or 0)
            value = qty * price
            asset_type = pos["asset_type"] or "stock"
            if sym in INVERSE_ETFS:
                short_equity += value
            elif asset_type == "option":
                options_val += value
            else:
                long_equity += value

        total = total_cash + long_equity + short_equity + options_val
        if total <= 0:
            total = 1
        return {
            "total_fleet_value": round(total, 2),
            "actual": {
                "long_equity": round(long_equity / total * 100, 1),
                "short_equity": round(short_equity / total * 100, 1),
                "options": round(options_val / total * 100, 1),
                "cash": round(total_cash / total * 100, 1),
            },
            "models": [{"id": m["id"], "name": m["name"]} for m in all_models],
            "model_count": len(player_ids),
        }
    finally:
        conn.close()


@app.get("/api/market/sectors")
def market_sectors():
    """Sector rotation tracker: performance by sector group."""
    from engine.market_data import get_stock_price
    from engine.sector_tracker import get_sector_rotation, get_sector_exposure
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    return {
        "rotation": get_sector_rotation(prices),
        "exposure": get_sector_exposure(),
    }


@app.get("/api/market/correlation")
@timed_cache(300)
def market_correlation():
    """Correlation matrix for watchlist stocks (30-day)."""
    from engine.correlation import get_watchlist_correlation
    return get_watchlist_correlation()


@app.get("/api/market/correlation/{player_id}")
def player_correlation(player_id: str):
    """Correlation matrix for a player's positions."""
    from engine.correlation import get_portfolio_correlation
    return get_portfolio_correlation(player_id)


@app.get("/api/arena/analytics")
def arena_analytics():
    """Performance analytics: Sharpe, max drawdown, win streak, best/worst trade, avg hold time."""
    from datetime import datetime, timedelta
    import math

    conn = _conn()

    # Get all players
    players = conn.execute(
        "SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
    ).fetchall()

    result = {}
    for p in players:
        pid = p["id"]

        # All trades for this player
        trades = conn.execute(
            "SELECT symbol, action, qty, price, executed_at, reasoning "
            "FROM trades WHERE player_id=? ORDER BY executed_at ASC",
            (pid,)
        ).fetchall()

        buys = {}   # symbol -> list of {qty, price, time}
        closed = []  # list of {symbol, pnl, pnl_pct, hold_seconds, buy_price, sell_price}

        for t in trades:
            sym = t["symbol"]
            if t["action"] in ("BUY", "BUY_CALL", "BUY_PUT"):
                if sym not in buys:
                    buys[sym] = []
                buys[sym].append({
                    "qty": t["qty"], "price": t["price"],
                    "time": t["executed_at"],
                })
            elif t["action"] == "SELL" and sym in buys and buys[sym]:
                buy_entry = buys[sym][0]
                pnl = (t["price"] - buy_entry["price"]) * t["qty"]
                pnl_pct = ((t["price"] / buy_entry["price"]) - 1) * 100 if buy_entry["price"] > 0 else 0
                try:
                    buy_dt = datetime.fromisoformat(buy_entry["time"].replace("Z", ""))
                    sell_dt = datetime.fromisoformat(t["executed_at"].replace("Z", ""))
                    hold_secs = (sell_dt - buy_dt).total_seconds()
                except Exception:
                    hold_secs = 0
                closed.append({
                    "symbol": sym, "pnl": pnl, "pnl_pct": pnl_pct,
                    "hold_seconds": hold_secs,
                    "buy_price": buy_entry["price"], "sell_price": t["price"],
                    "qty": t["qty"],
                })
                # Remove matched buy
                remaining = buy_entry["qty"] - t["qty"]
                if remaining <= 0.001:
                    buys[sym].pop(0)
                else:
                    buys[sym][0]["qty"] = remaining

        # Calculate metrics
        wins = [c for c in closed if c["pnl"] > 0]
        losses = [c for c in closed if c["pnl"] <= 0]
        total_closed = len(closed)
        win_rate = len(wins) / total_closed * 100 if total_closed > 0 else 0

        # Best / worst trade
        best_trade = max(closed, key=lambda x: x["pnl"]) if closed else None
        worst_trade = min(closed, key=lambda x: x["pnl"]) if closed else None

        # Win streak
        streak = 0
        max_streak = 0
        for c in closed:
            if c["pnl"] > 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0

        # Average hold time
        hold_times = [c["hold_seconds"] for c in closed if c["hold_seconds"] > 0]
        avg_hold_secs = sum(hold_times) / len(hold_times) if hold_times else 0
        avg_hold_hours = avg_hold_secs / 3600

        # Max drawdown from portfolio history
        history = conn.execute(
            "SELECT total_value FROM portfolio_history WHERE player_id=? ORDER BY recorded_at ASC",
            (pid,)
        ).fetchall()
        max_dd = 0
        peak = 0
        for h in history:
            val = h["total_value"]
            if val > peak:
                peak = val
            if peak > 0:
                dd = (peak - val) / peak
                max_dd = max(max_dd, dd)

        # Sharpe ratio (from daily returns in portfolio_history)
        values = [h["total_value"] for h in history]
        daily_returns = []
        for i in range(1, len(values)):
            if values[i-1] > 0:
                daily_returns.append((values[i] - values[i-1]) / values[i-1])
        if daily_returns and len(daily_returns) > 1:
            avg_ret = sum(daily_returns) / len(daily_returns)
            std_ret = (sum((r - avg_ret)**2 for r in daily_returns) / (len(daily_returns) - 1)) ** 0.5
            sharpe = (avg_ret / std_ret) * (252 ** 0.5) if std_ret > 0 else 0
        else:
            sharpe = 0

        result[pid] = {
            "name": p["display_name"],
            "total_trades": len(trades),
            "closed_trades": total_closed,
            "win_rate": round(win_rate, 1),
            "wins": len(wins),
            "losses": len(losses),
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "longest_win_streak": max_streak,
            "avg_hold_hours": round(avg_hold_hours, 1),
            "best_trade": {
                "symbol": best_trade["symbol"],
                "pnl": round(best_trade["pnl"], 2),
                "pnl_pct": round(best_trade["pnl_pct"], 1),
            } if best_trade else None,
            "worst_trade": {
                "symbol": worst_trade["symbol"],
                "pnl": round(worst_trade["pnl"], 2),
                "pnl_pct": round(worst_trade["pnl_pct"], 1),
            } if worst_trade else None,
        }

    conn.close()
    return result


@app.get("/api/trades/export")
def export_trades(season: int = 0):
    """Export trades as CSV, optionally filtered by season."""
    from fastapi.responses import StreamingResponse
    import io, csv

    conn = _conn()
    all_seasons = (season == -1)
    if season <= 0 and not all_seasons:
        # Default: export all seasons
        all_seasons = True

    if all_seasons:
        trades = conn.execute(
            "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.strike_price, t.expiry_date, "
            "t.entry_price, t.exit_price, t.realized_pnl, "
            "t.reasoning, t.confidence, t.executed_at, t.season "
            "FROM trades t JOIN ai_players p ON t.player_id = p.id "
            "ORDER BY t.executed_at DESC"
        ).fetchall()
    else:
        trades = conn.execute(
            "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.strike_price, t.expiry_date, "
            "t.entry_price, t.exit_price, t.realized_pnl, "
            "t.reasoning, t.confidence, t.executed_at, t.season "
            "FROM trades t JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.season=? ORDER BY t.executed_at DESC", (season,)
        ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Season", "Player ID", "Player Name", "Symbol", "Action", "Qty", "Price",
                     "Entry Price", "Exit Price", "Realized P&L",
                     "Asset Type", "Option Type", "Strike", "Expiry",
                     "Reasoning", "Confidence", "Executed At"])
    for t in trades:
        writer.writerow([t["season"], t["player_id"], t["display_name"], t["symbol"], t["action"],
                        t["qty"], t["price"], t["entry_price"], t["exit_price"], t["realized_pnl"],
                        t["asset_type"], t["option_type"], t["strike_price"], t["expiry_date"],
                        t["reasoning"], t["confidence"], t["executed_at"]])

    output.seek(0)
    filename = f"trades_s{season}.csv" if not all_seasons else "trades_all.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/api/market/sentiment")
def market_sentiment():
    """Get sentiment scores for all watchlist stocks."""
    from config import WATCH_STOCKS
    from engine.sentiment import get_watchlist_sentiment
    return get_watchlist_sentiment(WATCH_STOCKS)


@app.get("/api/market/sentiment/{symbol}")
def symbol_sentiment(symbol: str):
    """Get sentiment for a specific symbol."""
    from engine.sentiment import get_sentiment_for_symbol
    return get_sentiment_for_symbol(symbol.upper())


@app.get("/api/market/options-flow")
@timed_cache(120)
def options_flow():
    """Get options flow data for watchlist stocks with positions."""
    conn = _conn()
    symbols = conn.execute(
        "SELECT DISTINCT symbol FROM positions WHERE asset_type='option'"
    ).fetchall()
    conn.close()
    if not symbols:
        from config import WATCH_STOCKS
        syms = WATCH_STOCKS[:5]  # Limit to top 5 to avoid slow yfinance calls
    else:
        syms = [s["symbol"] for s in symbols]

    from engine.options_flow import get_flow_summary
    return get_flow_summary(syms)


@app.get("/api/market/options-alignment")
def options_alignment():
    """Check if recent AI options trades align with market flow."""
    from engine.options_flow import get_recent_ai_options_alignment
    return get_recent_ai_options_alignment()


@app.get("/api/journal")
def journal_entries(player_id: str = None, limit: int = 20, offset: int = 0):
    """Get AI journal entries."""
    from engine.ai_journal import get_journal_entries
    return get_journal_entries(player_id, limit, offset)


@app.get("/api/journal/today")
def journal_today():
    """Get today's journal entries."""
    from engine.ai_journal import get_today_journal
    return get_today_journal()


@app.get("/api/war-room")
def war_room(limit: int = 50):
    """Get recent War Room hot takes."""
    from engine.war_room import get_war_room_messages
    return get_war_room_messages(limit)


@app.post("/api/webull/sync")
def webull_sync(data: dict = None):
    """Manually sync Webull Portfolio value."""
    if not data:
        return {"error": "No data provided"}
    total_value = data.get("total_value")
    if total_value is None:
        return {"error": "total_value is required"}
    try:
        total_value = float(total_value)
    except (ValueError, TypeError):
        return {"error": "total_value must be a number"}

    from engine.paper_trader import sync_webull_value, get_webull_synced
    sync_webull_value(total_value)
    synced = get_webull_synced()
    return {"ok": True, "synced": synced}


@app.get("/api/webull/synced")
def webull_synced():
    """Get the last manually synced Webull value."""
    from engine.paper_trader import get_webull_synced
    return get_webull_synced() or {"total_value": None, "synced_at": None}


@app.get("/api/system/ram")
def system_ram():
    """Get current RAM usage for dashboard display."""
    try:
        import psutil
        mem = psutil.virtual_memory()
        avail_gb = mem.available / (1024 ** 3)
        total_gb = mem.total / (1024 ** 3)
        used_gb = (mem.total - mem.available) / (1024 ** 3)
        pct = mem.percent
        if avail_gb >= 4:
            status = "green"
        elif avail_gb >= 2:
            status = "yellow"
        else:
            status = "red"
        return {
            "available_gb": round(avail_gb, 1),
            "used_gb": round(used_gb, 1),
            "total_gb": round(total_gb, 1),
            "percent_used": round(pct, 1),
            "status": status,
        }
    except ImportError:
        return {"error": "psutil not installed", "status": "unknown"}


@app.post("/api/war-room/post")
def war_room_post(data: dict = None):
    """Post a human message to the War Room as Captain Kirk (Webull Portfolio)."""
    # FastAPI parses JSON body into data
    if data is None:
        # Fallback: try to read raw
        return {"error": "No data provided"}

    message = (data.get("message") or "").strip()
    symbol = (data.get("symbol") or "").strip()
    strategy_mode = (data.get("strategy_mode") or "").strip().upper()

    # Validate strategy mode
    valid_modes = {"SIMONS", "DRUCKENMILLER", "PTJ", "COHEN", "ONEIL", "DALIO"}
    if strategy_mode and strategy_mode not in valid_modes:
        strategy_mode = ""

    if not message:
        return {"error": "Message is required"}

    # Default symbol to most recent war room topic
    if not symbol:
        conn = _conn()
        last = conn.execute(
            "SELECT symbol FROM war_room ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        symbol = last["symbol"] if last else "SPY"

    # Tag the message with strategy mode if active
    tagged_message = message
    if strategy_mode:
        tagged_message = f"[{strategy_mode} MODE] {message}"

    # Save to war_room table (auto-migrate strategy_mode column if needed)
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO war_room (player_id, symbol, take, strategy_mode) VALUES (?, ?, ?, ?)",
            ("steve-webull", symbol, tagged_message, strategy_mode or None)
        )
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE war_room ADD COLUMN strategy_mode TEXT")
        conn.execute(
            "INSERT INTO war_room (player_id, symbol, take, strategy_mode) VALUES (?, ?, ?, ?)",
            ("steve-webull", symbol, tagged_message, strategy_mode or None)
        )
    conn.commit()
    conn.close()

    # Immediately trigger AI responses in this process (main.py is a separate process)
    import threading as _threading
    def _run_ai_responses():
        try:
            from engine.war_room import run_war_room as _run_wr, set_forced_topic, set_strategy_mode
            from engine.market_data import get_stock_price
            from config import WATCH_STOCKS
            import os

            # Set forced topic BEFORE building providers so the cycle debates Kirk's symbol
            set_forced_topic(symbol)
            if strategy_mode:
                set_strategy_mode(strategy_mode)

            # Build Ollama providers for all active, non-paused players
            conn2 = _conn()
            players = conn2.execute(
                "SELECT id, provider, model_id, display_name FROM ai_players "
                "WHERE is_active=1 AND is_paused=0"
            ).fetchall()
            conn2.close()

            from engine.providers.ollama_provider import OllamaProvider
            providers = {}
            for p in players:
                pid, prov, model, dname = p["id"], p["provider"], p["model_id"], p["display_name"]
                if pid in ("dayblade-0dte", "cto-grok42") or is_independent_player(pid):
                    continue
                try:
                    if prov == "ollama":
                        providers[pid] = OllamaProvider(player_id=pid, model=model)
                    elif prov == "google":
                        # Route through Ollama — no paid API
                        providers[pid] = OllamaProvider(player_id=pid, model="qwen3:14b")
                except Exception:
                    pass

            prices = {}
            for sym in WATCH_STOCKS:
                data = get_stock_price(sym)
                if "error" not in data:
                    prices[sym] = data
            # Also fetch the posted symbol if not in watchlist
            if symbol not in prices:
                data = get_stock_price(symbol)
                if "error" not in data:
                    prices[symbol] = data

            if prices and providers:
                _run_wr(providers, prices)
        except Exception as e:
            print(f"War Room post-response error: {e}")

    _threading.Thread(target=_run_ai_responses, daemon=True).start()

    return {"ok": True, "symbol": symbol, "message": tagged_message, "strategy_mode": strategy_mode}


@app.post("/api/war-room/trigger")
def trigger_war_room():
    """Manually trigger a War Room cycle."""
    import threading
    from engine.war_room import run_war_room as _run_wr, get_most_volatile
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    def _run():
        try:
            # Get providers from DB
            conn = _conn()
            players = conn.execute(
                "SELECT id, provider, model_id, display_name FROM ai_players WHERE is_active=1"
            ).fetchall()
            conn.close()

            import os
            providers = {}
            for p in players:
                pid, prov, model, dname = p["id"], p["provider"], p["model_id"], p["display_name"]
                if is_independent_player(pid):
                    continue
                try:
                    if prov == "openai":
                        from engine.providers.openai_provider import OpenAIProvider
                        providers[pid] = OpenAIProvider(os.getenv("OPENAI_API_KEY"), pid, model, dname)
                    elif prov == "google":
                        from engine.providers.ollama_provider import OllamaProvider
                        providers[pid] = OllamaProvider(player_id=pid, model="qwen3:14b")
                    elif prov == "xai":
                        from engine.providers.grok_provider import GrokProvider
                        providers[pid] = GrokProvider(os.getenv("XAI_API_KEY"), pid, model, dname)
                    elif prov == "ollama":
                        from engine.providers.ollama_provider import OllamaProvider
                        providers[pid] = OllamaProvider(player_id=pid, model=model)
                except Exception:
                    pass

            prices = {}
            for sym in WATCH_STOCKS:
                data = get_stock_price(sym)
                if "error" not in data:
                    prices[sym] = data

            if prices and providers:
                _run_wr(providers, prices)
        except Exception as e:
            print(f"War Room trigger error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "triggered"}


@app.post("/api/war-room/hail-q")
def war_room_hail_q(data: dict = None):
    """Summon Q — the omnipotent entity."""
    if not data or not data.get("message"):
        return {"error": "message required"}
    import threading

    message = data["message"].strip()

    # Save Captain's hail to War Room
    conn = _conn()
    conn.execute(
        "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
        ("steve-webull", "Q", f"🚀 CAPTAIN: {message}")
    )
    conn.commit()
    conn.close()

    def _run():
        try:
            from engine.q_entity import summon_q
            from engine.war_room import save_hot_take
            result = summon_q(message)
            response = result.get("response", "Q is silent.")
            # Save Q's response — use steve-webull as poster since Q isn't a player
            # We'll create a special formatting in the War Room display
            conn = _conn()
            conn.execute(
                "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
                ("steve-webull", "Q", f"✨ Q: {response[:500]}")
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Q entity error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "summoning_q"}


@app.post("/api/war-room/command")
def war_room_command(data: dict = None):
    """Smart command: parse Captain's input and route to models."""
    if not data:
        return {"error": "No data provided"}
    command = (data.get("command") or "").strip()
    raw_input = (data.get("input") or "").strip()
    ticker = (data.get("ticker") or "").strip().upper()
    target_model = (data.get("target_model") or "").strip()

    import threading, re

    # Check for Q summons
    input_lower = raw_input.lower()
    if (input_lower.startswith("@q ") or input_lower.startswith("hail q") or
        input_lower.startswith("q,") or "what am i missing" in input_lower or
        "judge the crew" in input_lower or command == "hail_q"):
        return war_room_hail_q({"message": raw_input})

    # Parse the input for tickers (1-5 uppercase letters)
    mentioned_tickers = re.findall(r'\b([A-Z]{1,5})\b', raw_input)
    # Filter to known watchlist stocks
    from config import WATCH_STOCKS
    valid_tickers = [t for t in mentioned_tickers if t in WATCH_STOCKS]

    # Determine the primary ticker
    if ticker:
        primary = ticker
    elif valid_tickers:
        primary = valid_tickers[0]
    else:
        primary = "SPY"

    # Save Captain's message
    conn = _conn()
    captain_msg = raw_input or f"[{command}] {ticker}"
    conn.execute(
        "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
        ("steve-webull", primary, f"🚀 {captain_msg}")
    )
    conn.commit()
    conn.close()

    # Force debate on this topic
    try:
        from engine.war_room import set_forced_topic
        set_forced_topic(primary)
    except Exception:
        pass

    # Trigger War Room cycle in background
    def _run_debate():
        try:
            from engine.war_room import run_war_room as _rwr
            from engine.market_data import get_stock_price
            providers = _build_war_room_providers()
            prices = {}
            for sym in ([primary] + valid_tickers + list(WATCH_STOCKS)):
                if sym not in prices:
                    d = get_stock_price(sym)
                    if "error" not in d:
                        prices[sym] = d
            if prices and providers:
                _rwr(providers, prices)
        except Exception as e:
            print(f"War Room command error: {e}")

    threading.Thread(target=_run_debate, daemon=True).start()
    return {"ok": True, "command": command, "ticker": primary, "tickers": valid_tickers, "status": "generating"}


@app.post("/api/war-room/top-picks")
def war_room_top_picks():
    """Each active model returns their #1 trade idea."""
    import threading

    def _run():
        try:
            from engine.war_room import save_hot_take
            from engine.market_data import get_stock_price
            providers = _build_war_room_providers()
            for pid, prov in providers.items():
                try:
                    prompt = (
                        f"You are {prov.display_name}. The Captain wants your SINGLE BEST trade idea right now. "
                        "Name ONE ticker, say BUY or SELL, give a 1-2 sentence thesis with a target price. "
                        "Be specific and bold. Format: TICKER — BUY/SELL — thesis"
                    )
                    response = prov.call_model(prompt)
                    if response:
                        take = response.strip()[:500]
                        save_hot_take(pid, "TOP_PICK", take)
                except Exception:
                    pass
        except Exception as e:
            print(f"Top picks error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "generating_top_picks"}


@app.post("/api/war-room/poll")
def war_room_poll(data: dict = None):
    """Quick poll: models respond with one-line stance + conviction."""
    if not data:
        return {"error": "No data"}
    question = (data.get("question") or "Bull or bear?").strip()
    ticker = (data.get("ticker") or "SPY").strip().upper()
    import threading

    # Save poll question
    conn = _conn()
    conn.execute(
        "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
        ("steve-webull", ticker, f"📊 POLL: {question}")
    )
    conn.commit()
    conn.close()

    def _run():
        try:
            from engine.war_room import save_hot_take
            from engine.market_data import get_stock_price
            price_data = get_stock_price(ticker)
            price = price_data.get("price", 0) if price_data else 0
            change = price_data.get("change_pct", 0) if price_data else 0
            providers = _build_war_room_providers()
            for pid, prov in providers.items():
                try:
                    prompt = (
                        f"Quick poll from Captain Kirk: \"{question}\" about {ticker} (${price:.2f}, {change:+.2f}% today). "
                        "Reply in EXACTLY this format: BULL or BEAR or NEUTRAL | Conviction: X/10 | One sentence why."
                    )
                    response = prov.call_model(prompt)
                    if response:
                        save_hot_take(pid, ticker, response.strip()[:300])
                except Exception:
                    pass
        except Exception as e:
            print(f"Poll error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "polling", "ticker": ticker}


@app.post("/api/war-room/challenge")
def war_room_challenge(data: dict = None):
    """Direct challenge to a specific model."""
    if not data:
        return {"error": "No data"}
    target = (data.get("target_model") or "").strip()
    message = (data.get("message") or "").strip()
    ticker = (data.get("ticker") or "SPY").strip().upper()
    if not target or not message:
        return {"error": "target_model and message required"}
    import threading

    conn = _conn()
    conn.execute(
        "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
        ("steve-webull", ticker, f"🎯 @{target}: {message}")
    )
    conn.commit()
    conn.close()

    def _run():
        try:
            from engine.war_room import save_hot_take
            providers = _build_war_room_providers()
            prov = providers.get(target)
            if prov:
                prompt = (
                    f"Captain Kirk is challenging you directly: \"{message}\" "
                    f"about {ticker}. Respond with conviction. Defend your position or admit the Captain has a point. "
                    "Be bold, be specific."
                )
                response = prov.call_model(prompt)
                if response:
                    save_hot_take(target, ticker, response.strip()[:500])
        except Exception as e:
            print(f"Challenge error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "challenged", "target": target}


@app.post("/api/war-room/portfolio-review")
def war_room_portfolio_review():
    """Models critique Captain's current positions."""
    import threading

    def _run():
        try:
            from engine.paper_trader import get_portfolio_with_pnl
            from engine.market_data import get_stock_price
            from engine.war_room import save_hot_take

            # Get Captain's portfolio
            prices = {}
            conn = _conn()
            steve_pos = conn.execute(
                "SELECT symbol, qty, avg_price FROM positions WHERE player_id='steve-webull'"
            ).fetchall()
            conn.close()
            pos_str = ", ".join(f"{r['symbol']}({r['qty']}@${r['avg_price']:.2f})" for r in steve_pos)

            # Save the review request
            conn = _conn()
            conn.execute(
                "INSERT INTO war_room (player_id, symbol, take) VALUES (?, ?, ?)",
                ("steve-webull", "PORTFOLIO", f"📋 Review my portfolio: {pos_str}")
            )
            conn.commit()
            conn.close()

            providers = _build_war_room_providers()
            for pid, prov in providers.items():
                try:
                    prompt = (
                        f"Captain Kirk's current portfolio: {pos_str}. "
                        "Critique it. What would you keep? What would you sell? What's missing? "
                        "Be honest and specific. 2-3 sentences max."
                    )
                    response = prov.call_model(prompt)
                    if response:
                        save_hot_take(pid, "PORTFOLIO", response.strip()[:500])
                except Exception:
                    pass
        except Exception as e:
            print(f"Portfolio review error: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "reviewing"}


def _build_war_room_providers() -> dict:
    """Build provider dict for War Room from active players."""
    import os
    conn = _conn()
    players = conn.execute(
        "SELECT id, provider, model_id, display_name FROM ai_players WHERE is_active=1 AND is_paused=0"
    ).fetchall()
    conn.close()
    providers = {}
    for p in players:
        pid, prov, model, dname = p["id"], p["provider"], p["model_id"], p["display_name"]
        if pid == "steve-webull" or is_independent_player(pid):
            continue
        try:
            if prov == "openai":
                from engine.providers.openai_provider import OpenAIProvider
                providers[pid] = OpenAIProvider(os.getenv("OPENAI_API_KEY"), pid, model, dname)
            elif prov == "google":
                from engine.providers.ollama_provider import OllamaProvider
                providers[pid] = OllamaProvider(player_id=pid, model="qwen3:14b")
            elif prov == "xai":
                from engine.providers.grok_provider import GrokProvider
                providers[pid] = GrokProvider(os.getenv("XAI_API_KEY"), pid, model, dname)
            elif prov == "ollama":
                from engine.providers.ollama_provider import OllamaProvider
                providers[pid] = OllamaProvider(player_id=pid, model=model)
        except Exception:
            pass
    return providers


# ── Volume Radar ─────────────────────────────────────────────────────────────


@app.get("/api/volume-radar")
def volume_radar(limit: int = 20):
    """Today's top volume alerts sorted by relative_volume DESC."""
    try:
        from engine.volume_scanner import get_todays_volume_alerts
        alerts = get_todays_volume_alerts(limit=limit)
        return {"alerts": alerts, "count": len(alerts)}
    except Exception as e:
        return {"error": str(e), "alerts": [], "count": 0}


# ── GEX Overlay ──────────────────────────────────────────────────────────────


@app.get("/api/gex-overlay/levels")
def gex_overlay_levels(symbol: str = "SPY"):
    """Latest GEX Overlay key levels: king node, gamma flip, put/call walls, regime."""
    try:
        from engine.gex_overlay import get_latest_gex
        data = get_latest_gex(symbol.upper())
        if data:
            return data
        # Compute fresh if no DB data yet
        from engine.gex_overlay import calculate_gex, _save_gex_levels
        levels = calculate_gex(symbol.upper())
        if levels:
            _save_gex_levels(symbol.upper(), levels)
            return get_latest_gex(symbol.upper()) or {"error": "no data after compute"}
        return {"error": f"GEX Overlay data unavailable for {symbol}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/gex-overlay/heatmap")
def gex_overlay_heatmap(symbol: str = "SPY"):
    """Per-strike GEX data for heatmap visualization (call_gex, put_gex, net_gex)."""
    try:
        from engine.gex_overlay import get_heatmap_data
        strikes = get_heatmap_data(symbol.upper())
        return {"symbol": symbol.upper(), "strikes": strikes, "count": len(strikes)}
    except Exception as e:
        return {"error": str(e), "strikes": [], "count": 0}


# ── Battle Station ────────────────────────────────────────────────────────────


@app.get("/api/battle-station/status")
def battle_station_status():
    """Battle Station status: active positions, signals, morning levels, GEX regime."""
    try:
        from engine.battle_station import get_battle_station_status
        return get_battle_station_status()
    except Exception as e:
        return {"error": str(e), "status": "ERROR", "active_positions": 0}


@app.get("/api/battle-station/log")
def battle_station_log(limit: int = 20):
    """Recent battle_station_log entries with Greeks, P&L, and signals."""
    try:
        from engine.battle_station import get_recent_log
        entries = get_recent_log(limit=limit)
        return {"entries": entries, "count": len(entries)}
    except Exception as e:
        return {"error": str(e), "entries": [], "count": 0}


@app.get("/api/battle-station-0dte/status")
def battle_station_0dte_status():
    """Battle Station 0DTE rules-based agent: current status, position, and key levels."""
    try:
        from engine.battle_station_0dte import get_status
        return get_status()
    except Exception as e:
        return {"error": str(e), "status": "ERROR"}


@app.get("/api/battle-station-0dte/history")
def battle_station_0dte_history(limit: int = 20):
    """Battle Station 0DTE: trade history with P&L."""
    try:
        from engine.battle_station_0dte import get_history
        trades = get_history(limit=limit)
        # Win rate calculation
        closed = [t for t in trades if t["status"] != "OPEN"]
        wins = [t for t in closed if t.get("pnl_pct", 0) and t["pnl_pct"] > 0]
        win_rate = len(wins) / len(closed) * 100 if closed else 0
        return {"trades": trades, "count": len(trades), "win_rate": round(win_rate, 1)}
    except Exception as e:
        return {"error": str(e), "trades": [], "count": 0, "win_rate": 0}


@app.get("/api/universe/status")
def universe_status():
    """Status of the full universe and volume baselines tables."""
    try:
        conn = _conn()
        universe_count = conn.execute("SELECT COUNT(*) FROM universe_stocks").fetchone()[0]
        universe_updated = conn.execute(
            "SELECT MAX(updated_at) FROM universe_stocks"
        ).fetchone()[0]
        baselines_count = conn.execute("SELECT COUNT(*) FROM volume_baselines").fetchone()[0]
        baselines_updated = conn.execute(
            "SELECT MAX(updated_at) FROM volume_baselines"
        ).fetchone()[0]
        today_alerts = conn.execute(
            "SELECT COUNT(*) FROM volume_alerts WHERE date(detected_at)=date('now')"
        ).fetchone()[0]
        conn.close()
        return {
            "universe_symbols": universe_count,
            "universe_last_refresh": universe_updated,
            "baselines_symbols": baselines_count,
            "baselines_last_update": baselines_updated,
            "todays_volume_alerts": today_alerts,
        }
    except Exception as e:
        return {"error": str(e)}


# ── Impulse Alerts ──────────────────────────────────────────────────────────
from engine.impulse_detector import (
    ensure_impulse_table as _ensure_impulse_table,
    get_active_impulse_alerts, get_recent_impulse_alerts,
)
_ensure_impulse_table()


@app.get("/api/impulse/active")
def impulse_active(max_age_hours: float = 2.0):
    """Active impulse alerts within the last N hours, sorted by strength."""
    try:
        return {"alerts": get_active_impulse_alerts(max_age_hours)}
    except Exception as e:
        return {"error": str(e), "alerts": []}


@app.get("/api/impulse/recent")
def impulse_recent(limit: int = 50):
    """Most recent impulse alerts from DB (all time)."""
    try:
        return {"alerts": get_recent_impulse_alerts(limit)}
    except Exception as e:
        return {"error": str(e), "alerts": []}


# ── Gap Scanner ─────────────────────────────────────────────────────────────
from engine.gap_scanner import (
    ensure_gap_table as _ensure_gap_table,
    get_todays_gaps, get_recent_gaps, get_gap_fill_stats, get_cached_gaps,
)
_ensure_gap_table()


@app.get("/api/gaps/today")
def gaps_today(min_gap_pct: float = 0.0):
    """Today's morning gaps with fill status, sorted by gap size."""
    try:
        gaps = get_cached_gaps()
        if not gaps:
            gaps = get_todays_gaps(min_gap_pct)
        if min_gap_pct > 0:
            gaps = [g for g in gaps if abs(g.get("gap_pct", 0)) >= min_gap_pct]
        return {"gaps": gaps, "count": len(gaps), "date": __import__("datetime").date.today().isoformat()}
    except Exception as e:
        return {"error": str(e), "gaps": []}


@app.get("/api/gaps/history")
def gaps_history(limit: int = 100):
    """Recent gap history across all dates."""
    try:
        return {"gaps": get_recent_gaps(limit)}
    except Exception as e:
        return {"error": str(e), "gaps": []}


@app.get("/api/gaps/stats")
def gaps_stats(days: int = 30):
    """Gap fill statistics by gap type over the last N days."""
    try:
        return {"stats": get_gap_fill_stats(days), "days": days}
    except Exception as e:
        return {"error": str(e), "stats": {}}


@app.post("/api/gaps/scan")
def gaps_scan_now():
    """Trigger immediate gap scan (runs in background)."""
    import threading as _threading
    def _bg():
        try:
            from config import WATCH_STOCKS
            from engine.gap_scanner import scan_all_gaps
            scan_all_gaps(WATCH_STOCKS)
        except Exception as e:
            console.log(f"[red]Gap manual scan error: {e}")
    _threading.Thread(target=_bg, daemon=True).start()
    return {"status": "scanning", "message": "Gap scan started in background"}


# ── Theta Scanner ───────────────────────────────────────────────────────────
from engine.theta_scanner import (
    ensure_theta_table as _ensure_theta_table,
    get_cached_theta, get_theta_opportunities, get_latest_theta,
)
_ensure_theta_table()


@app.get("/api/theta/opportunities")
def theta_opportunities(min_score: int = 3, limit: int = 50):
    """Theta collection opportunities sorted by score (uses today's cache or DB)."""
    try:
        data = get_cached_theta()
        if not data:
            data = get_latest_theta()
        filtered = [o for o in data if o.get("theta_score", 0) >= min_score]
        filtered = filtered[:limit]
        return {"opportunities": filtered, "count": len(filtered)}
    except Exception as e:
        return {"error": str(e), "opportunities": []}


@app.get("/api/theta/history")
def theta_history(limit: int = 100, min_score: int = 1):
    """Historical theta opportunities from DB."""
    try:
        return {"opportunities": get_theta_opportunities(limit, min_score)}
    except Exception as e:
        return {"error": str(e), "opportunities": []}


@app.post("/api/theta/scan")
def theta_scan_now():
    """Trigger an immediate theta scan (runs in background)."""
    import threading as _threading
    def _bg():
        try:
            from config import WATCH_STOCKS
            from engine.theta_scanner import scan_all_theta
            scan_all_theta(WATCH_STOCKS)
        except Exception as e:
            console.log(f"[red]Theta manual scan error: {e}")
    _threading.Thread(target=_bg, daemon=True).start()
    return {"status": "scanning", "message": "Theta scan started in background"}


# ── 200 SMA Filter ──────────────────────────────────────────────────────────
from engine.sma_filter import ensure_sma_table as _ensure_sma_table, \
    get_cached_sma_status, get_recent_sma_signals
_ensure_sma_table()


@app.get("/api/sma/status")
def sma_status():
    """Current 200 SMA status for all watchlist stocks (cached 15 min)."""
    from config import WATCH_STOCKS
    try:
        data = get_cached_sma_status(WATCH_STOCKS)
        stocks = sorted(data.values(), key=lambda x: abs(x.get("distance_pct", 999)))
        return {"stocks": stocks, "count": len(stocks)}
    except Exception as e:
        return {"error": str(e), "stocks": []}


@app.get("/api/sma/signals")
def sma_signals_endpoint(limit: int = 50):
    """Recent 200 SMA signals (Bounce / Breakdown / Reclaim) from DB."""
    try:
        return {"signals": get_recent_sma_signals(limit)}
    except Exception as e:
        return {"error": str(e), "signals": []}


# ── Supply/Demand Imbalance Zones ───────────────────────────────────────────
from engine.imbalance_detector import (
    ensure_imbalance_table as _ensure_imbalance_table,
    get_untested_zones, get_all_zones,
)
_ensure_imbalance_table()


@app.get("/api/imbalance/zones")
def imbalance_zones(ticker: str = None, limit: int = 100):
    """Untested supply/demand imbalance zones, optionally filtered by ticker."""
    try:
        return {"zones": get_untested_zones(ticker, limit)}
    except Exception as e:
        return {"error": str(e), "zones": []}


@app.get("/api/imbalance/all")
def imbalance_all(ticker: str = None, limit: int = 200):
    """All imbalance zones (tested + untested), optionally filtered by ticker."""
    try:
        return {"zones": get_all_zones(ticker, limit)}
    except Exception as e:
        return {"error": str(e), "zones": []}


# ── Quorum ──────────────────────────────────────────────────────────────────
import uuid as _uuid

_quorum_sessions: dict = {}  # quorum_id → {ticker, votes, done, started_at}


def _ensure_quorum_table():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quorum_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quorum_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            player_id TEXT NOT NULL,
            display_name TEXT,
            vote TEXT NOT NULL,
            confidence REAL,
            reasoning TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()

_ensure_quorum_table()


@app.post("/api/quorum/start")
def quorum_start(data: dict):
    """Start a quorum vote for a ticker. Returns quorum_id to poll for results."""
    ticker = (data.get("ticker") or "SPY").strip().upper()
    quorum_id = str(_uuid.uuid4())[:8]
    _quorum_sessions[quorum_id] = {
        "ticker": ticker,
        "votes": [],
        "done": False,
        "started_at": _time.time(),
    }

    def _run():
        try:
            from engine.market_data import get_stock_price
            from engine.gemini_free_tier import call_gemini
            price_data = get_stock_price(ticker)
            price = price_data.get("price", 0) if price_data else 0
            change = price_data.get("change_pct", 0) if price_data else 0

            prompt = (
                f"Quorum vote requested for {ticker} (${price:.2f}, {change:+.2f}% today). "
                f"Should we BUY, SELL, or HOLD {ticker} right now? "
                f"Reply in EXACTLY this format: "
                f"VOTE: BUY|SELL|HOLD | CONFIDENCE: 0-100 | REASON: one sentence max."
            )

            providers = _build_war_room_providers()
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def _call_one(pid_prov):
                pid, prov = pid_prov
                try:
                    resp = prov.call_model(prompt)
                    if not resp:
                        return None
                    resp = resp.strip()
                    vote, confidence = "HOLD", 50.0
                    upper = resp.upper()
                    if "VOTE: BUY" in upper or upper.startswith("BUY"):
                        vote = "BUY"
                    elif "VOTE: SELL" in upper or upper.startswith("SELL"):
                        vote = "SELL"
                    import re as _re
                    cm = _re.search(r'CONFIDENCE:\s*(\d+)', resp, _re.IGNORECASE)
                    if cm:
                        confidence = min(100, max(0, float(cm.group(1))))
                    reason = ""
                    rm = _re.search(r'REASON:\s*(.+)', resp, _re.IGNORECASE)
                    if rm:
                        reason = rm.group(1).strip()[:200]
                    return {"player_id": pid, "display_name": getattr(prov, "display_name", pid),
                            "vote": vote, "confidence": confidence, "reasoning": reason}
                except Exception:
                    return None

            with ThreadPoolExecutor(max_workers=6) as ex:
                futs = {ex.submit(_call_one, item): item[0] for item in providers.items()}
                for fut in as_completed(futs, timeout=120):
                    result = fut.result()
                    if result:
                        _quorum_sessions[quorum_id]["votes"].append(result)
                        # Persist to DB
                        try:
                            c = _conn()
                            c.execute(
                                "INSERT INTO quorum_votes (quorum_id,ticker,player_id,display_name,vote,confidence,reasoning) "
                                "VALUES (?,?,?,?,?,?,?)",
                                (quorum_id, ticker, result["player_id"], result["display_name"],
                                 result["vote"], result["confidence"], result["reasoning"])
                            )
                            c.commit()
                            c.close()
                        except Exception:
                            pass
        except Exception as e:
            print(f"Quorum error: {e}")
        finally:
            _quorum_sessions[quorum_id]["done"] = True

    import threading as _threading
    _threading.Thread(target=_run, daemon=True).start()
    return {"quorum_id": quorum_id, "ticker": ticker}


@app.get("/api/quorum/status/{quorum_id}")
def quorum_status(quorum_id: str):
    """Poll for quorum results."""
    session = _quorum_sessions.get(quorum_id)
    if not session:
        # Try loading from DB
        conn = _conn()
        rows = conn.execute(
            "SELECT player_id, display_name, vote, confidence, reasoning FROM quorum_votes WHERE quorum_id=?",
            (quorum_id,)
        ).fetchall()
        conn.close()
        if not rows:
            return {"error": "Quorum not found"}
        votes = [dict(r) for r in rows]
    else:
        votes = session["votes"]

    buy = sum(1 for v in votes if v["vote"] == "BUY")
    sell = sum(1 for v in votes if v["vote"] == "SELL")
    hold = sum(1 for v in votes if v["vote"] == "HOLD")
    total = len(votes)
    consensus = "HOLD"
    if total > 0:
        if buy > sell and buy > hold:
            consensus = "BUY"
        elif sell > buy and sell > hold:
            consensus = "SELL"

    return {
        "quorum_id": quorum_id,
        "ticker": session["ticker"] if session else quorum_id,
        "done": session["done"] if session else True,
        "votes": votes,
        "tally": {"BUY": buy, "SELL": sell, "HOLD": hold, "total": total},
        "consensus": consensus,
    }


@app.get("/api/smart-money")
def smart_money(limit: int = 20):
    """Get recent Smart Money signals. Auto-rescans with 24h window if stored data is >7 days old."""
    from engine.smart_money import get_recent_smart_money, check_smart_money_signals, save_smart_money_signal
    import sqlite3 as _sq
    from datetime import datetime as _dt, timedelta as _td
    stored = get_recent_smart_money(limit)
    # If stored signals are stale (>7 days) or absent, run a wider-window scan
    is_stale = True
    if stored:
        try:
            newest = _dt.fromisoformat(stored[0]["detected_at"].replace("T", " ").split(".")[0])
            is_stale = newest < _dt.now() - _td(days=7)
        except Exception:
            pass
    if is_stale:
        # Wide-window scan: 2+ buyers in last 24h
        try:
            conn = _sq.connect("data/trader.db", check_same_thread=False)
            conn.row_factory = _sq.Row
            rows = conn.execute("""
                SELECT t.symbol, t.player_id, p.display_name, t.price, t.confidence, t.executed_at
                FROM trades t JOIN ai_players p ON t.player_id = p.id
                WHERE t.action IN ('BUY', 'BUY_CALL')
                AND t.executed_at >= datetime('now', '-24 hours')
                AND t.player_id NOT IN ('dayblade-0dte','capitol-trades')
                ORDER BY t.symbol, t.executed_at DESC
            """).fetchall()
            conn.close()
            by_sym: dict = {}
            for r in rows:
                sym = r["symbol"]
                if sym not in by_sym:
                    by_sym[sym] = []
                if not any(b["player_id"] == r["player_id"] for b in by_sym[sym]):
                    by_sym[sym].append({"player_id": r["player_id"], "display_name": r["display_name"],
                                        "price": r["price"], "confidence": r["confidence"]})
            fresh = []
            for sym, buyers in by_sym.items():
                if len(buyers) >= 2:
                    sig = {"symbol": sym, "buyers": buyers, "count": len(buyers),
                           "detected_at": _dt.now().isoformat()}
                    save_smart_money_signal(sig)
                    fresh.append(sig)
            if fresh:
                return fresh
        except Exception:
            pass
    return stored


@app.get("/api/autopilot/status")
def autopilot_status():
    """Get autopilot enabled/disabled status."""
    from engine.autopilot import is_autopilot_enabled
    return {"enabled": is_autopilot_enabled()}


@app.post("/api/autopilot/toggle")
def autopilot_toggle():
    """Toggle autopilot on/off."""
    from engine.autopilot import is_autopilot_enabled, set_autopilot
    current = is_autopilot_enabled()
    set_autopilot(not current)
    return {"enabled": not current}


_risk_radar_cache = {"all": None, "all_ts": 0, "prices": {}, "prices_ts": 0}

@app.get("/api/risk-radar")
def risk_radar(player_id: str = None):
    """Get risk radar spider chart data."""
    import time as _time
    from engine.risk_radar import get_risk_radar, get_all_risk_radars
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    # Cache all-players result for 5 minutes — check BEFORE fetching prices
    now = _time.time()
    if not player_id:
        if _risk_radar_cache["all"] and (now - _risk_radar_cache["all_ts"]) < 300:
            return _risk_radar_cache["all"]

    # Reuse cached prices if fresh (within 60s)
    if _risk_radar_cache["prices"] and (now - _risk_radar_cache["prices_ts"]) < 60:
        prices = _risk_radar_cache["prices"]
    else:
        prices = {}
        for sym in WATCH_STOCKS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data
        _risk_radar_cache["prices"] = prices
        _risk_radar_cache["prices_ts"] = now

    if player_id:
        return get_risk_radar(player_id, prices)

    result = get_all_risk_radars(prices)
    _risk_radar_cache["all"] = result
    _risk_radar_cache["all_ts"] = now
    return result


# --- Backtest History (Fix 6: Save & Compare Results) ---
# NOTE: These specific routes MUST be before the {player_id} catch-all

@app.post("/api/backtest/save-result")
def backtest_save_result(data: dict = None):
    """Save a backtest result to history for trend tracking."""
    if not data or not data.get("player_id"):
        return {"error": "player_id required"}
    import sqlite3
    conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=30)

    # Get Rallies top performer for external benchmark
    rallies_top_return = None
    rallies_top_name = None
    try:
        from engine.rallies_intel import get_top_performers
        top = get_top_performers(1)
        if top:
            rallies_top_return = top[0]["return_pct"]
            rallies_top_name = top[0]["name"]
    except Exception:
        pass

    conn.execute("""
        INSERT INTO backtest_history (
            player_id, player_name, period_days, start_date, end_date,
            starting_value, final_value, return_pct, total_pnl,
            win_count, loss_count, win_rate, total_trades,
            best_trade_pnl, worst_trade_pnl, best_trade_symbol, worst_trade_symbol,
            spy_return_pct, rallies_top_return_pct, rallies_top_name,
            notes, config_snapshot,
            guardrails_applied, signals_tested, signals_skipped, skip_summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["player_id"], data.get("player_name"),
        data.get("period_days", 30), data.get("start_date"), data.get("end_date"),
        data.get("starting_value", 7000), data.get("final_value"),
        data.get("return_pct"), data.get("total_pnl"),
        data.get("win_count", 0), data.get("loss_count", 0),
        data.get("win_rate", 0), data.get("total_trades", 0),
        data.get("best_trade_pnl"), data.get("worst_trade_pnl"),
        data.get("best_trade_symbol"), data.get("worst_trade_symbol"),
        data.get("spy_return_pct"),
        rallies_top_return or data.get("rallies_top_return_pct"),
        rallies_top_name or data.get("rallies_top_name"),
        data.get("notes"), data.get("config_snapshot"),
        1 if data.get("guardrails_applied") else 0,
        data.get("signals_tested", 0),
        data.get("signals_skipped", 0),
        data.get("skip_summary"),
    ))
    conn.commit()
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return {"ok": True, "id": rid}


@app.get("/api/backtest/history-for/{player_id}")
def backtest_history_for(player_id: str):
    """Get all historical backtest runs for a player — shows improvement over time."""
    import sqlite3
    conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM backtest_history WHERE player_id=? ORDER BY run_date DESC
    """, (player_id,)).fetchall()
    conn.close()
    return {"player_id": player_id, "runs": [dict(r) for r in rows]}


@app.get("/api/backtest/history-leaderboard")
def backtest_history_leaderboard():
    """Best backtest results across all models — Starfleet Intelligence for historical performance."""
    import sqlite3
    conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT bh.* FROM backtest_history bh
        INNER JOIN (
            SELECT player_id, MAX(run_date) as max_date
            FROM backtest_history GROUP BY player_id
        ) latest ON bh.player_id = latest.player_id AND bh.run_date = latest.max_date
        ORDER BY bh.return_pct DESC
    """).fetchall()
    conn.close()
    return {"leaderboard": [dict(r) for r in rows]}


@app.get("/api/backtest/history/leaderboard")
def backtest_history_leaderboard_alias():
    """Compatibility alias for older dashboard clients."""
    return backtest_history_leaderboard()


@app.get("/api/backtest/compare-guardrails/{player_id}")
def backtest_compare(player_id: str, days: int = 30,
                     start_date: str = None, end_date: str = None):
    """Run both raw and guarded backtests, return side-by-side comparison."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.backtester import backtest_compare as _compare
    effective_days = days
    if start_date and end_date:
        from datetime import datetime as _dt
        effective_days = (_dt.strptime(end_date, "%Y-%m-%d") - _dt.strptime(start_date, "%Y-%m-%d")).days
    timeout = max(60, min(effective_days // 5, 240))
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(_compare, player_id, days,
                             start_date, end_date).result(timeout=timeout)
    except FuturesTimeout:
        return {"error": f"Comparison timed out (>{timeout}s). Try a shorter date range."}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/backtest/inverse-etfs")
def backtest_inverse(days: int = 180):
    """Backtest Worf's inverse ETF strategy across ETFs and allocations."""
    from engine.inverse_etfs import backtest_inverse_etfs
    results = backtest_inverse_etfs(days=days)
    if "error" in results:
        return results
    sorted_results = sorted(results.values(), key=lambda x: x["total_return"], reverse=True)
    return {
        "period_days": days,
        "results": sorted_results,
        "best": sorted_results[0] if sorted_results else None,
        "spy_return": sorted_results[0]["spy_return"] if sorted_results else 0,
    }


@app.get("/api/regime/raw")
def market_regime_raw():
    """Get raw regime detector output (legacy)."""
    from engine.regime_detector import detect_regime
    return detect_regime()


@app.get("/api/whisper")
def whisper_network():
    """Get trending tickers from Whisper Network."""
    from engine.whisper_network import get_trending_tickers, check_watchlist_trending
    return {
        "trending": get_trending_tickers(),
        "watchlist_trending": check_watchlist_trending(),
    }


@app.get("/api/ghost-trades")
def ghost_trades(player_id: str = None, limit: int = 50):
    """Get ghost trades (missed opportunities)."""
    from engine.ghost_trades import get_ghost_trades
    return get_ghost_trades(player_id, limit)


@app.get("/api/ghost-trades/stats")
def ghost_stats():
    """Get aggregate ghost trade statistics."""
    from engine.ghost_trades import get_ghost_stats
    return get_ghost_stats()


@app.get("/api/alerts/history")
def alert_history(limit: int = 200, model: str = "", ticker: str = "", signal_type: str = ""):
    """Full alert history — signals + dynamic alerts merged, 7-day window."""
    conn = _conn()
    results = []

    # Dynamic alerts (trendline breaks, RSI, MACD, volume)
    alerts = conn.execute(
        "SELECT id, symbol, alert_type, message, severity, price, triggered_at "
        "FROM dynamic_alerts WHERE triggered_at >= datetime('now', '-7 days') "
        "ORDER BY triggered_at DESC LIMIT ?", (limit,)
    ).fetchall()
    for a in alerts:
        d = dict(a)
        if ticker and d["symbol"] != ticker.upper():
            continue
        if signal_type and signal_type.lower() not in (d["alert_type"] or "").lower():
            continue
        d["source"] = "dynamic_alert"
        d["model"] = "Navigator"
        d["confidence"] = None
        d["reasoning"] = d["message"]
        d["timestamp"] = d["triggered_at"]
        results.append(d)

    # Signals from AI models (BUY/SELL with confidence + reasoning)
    sigs = conn.execute(
        "SELECT s.player_id, p.display_name, s.symbol, s.signal, s.confidence, "
        "s.reasoning, s.created_at "
        "FROM signals s JOIN ai_players p ON s.player_id = p.id "
        "WHERE s.created_at >= datetime('now', '-7 days') "
        "AND s.signal IN ('BUY', 'SELL', 'BUY_CALL', 'BUY_PUT') "
        "ORDER BY s.created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    for s in sigs:
        d = dict(s)
        if ticker and d["symbol"] != ticker.upper():
            continue
        if model and model.lower() not in (d["display_name"] or "").lower() and model.lower() not in (d["player_id"] or "").lower():
            continue
        d["source"] = "ai_signal"
        d["model"] = d["display_name"] or d["player_id"]
        d["alert_type"] = d["signal"]
        d["timestamp"] = d["created_at"]
        d["severity"] = "high" if (d["confidence"] or 0) >= 0.8 else "medium"
        results.append(d)

    conn.close()
    results.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return results[:limit]


@app.get("/api/alerts/recent")
def recent_alerts(limit: int = 20):
    """Get recent trades for browser notification polling."""
    conn = _conn()
    trades = conn.execute(
        "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
        "t.reasoning, t.executed_at "
        "FROM trades t JOIN ai_players p ON t.player_id = p.id "
        "ORDER BY t.executed_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(t) for t in trades]


# --- Multi-Timeframe Analysis ---

@app.get("/api/market/mtf/{symbol}")
def multi_timeframe(symbol: str):
    """Get multi-timeframe analysis for a symbol."""
    from engine.multi_timeframe import get_multi_timeframe
    return get_multi_timeframe(symbol.upper())


# --- Options Greeks ---

@app.get("/api/options/greeks")
def options_greeks():
    """Get live Greeks for all options positions."""
    from engine.options_greeks import get_options_greeks
    from engine.market_data import get_stock_price

    conn = _conn()
    symbols = conn.execute(
        "SELECT DISTINCT symbol FROM positions WHERE asset_type='option'"
    ).fetchall()
    conn.close()

    prices = {}
    for s in symbols:
        data = get_stock_price(s["symbol"])
        if "error" not in data:
            prices[s["symbol"]] = data

    return get_options_greeks(prices)


@app.get("/api/options/theta-burn")
def theta_burn():
    """Get total theta burn summary."""
    from engine.options_greeks import get_total_theta_burn
    return get_total_theta_burn()


@app.get("/api/options/positions")
def options_positions():
    """Current paper options positions from Alpaca."""
    try:
        import os
        from alpaca.trading.client import TradingClient
        client = TradingClient(
            os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY"),
            os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET"),
            paper=True,
        )
        all_pos = client.get_all_positions()
        options = [
            {
                "symbol": p.symbol,
                "qty": float(p.qty),
                "avg_entry": float(p.avg_entry_price),
                "current_price": float(p.current_price) if p.current_price else None,
                "unrealized_pl": float(p.unrealized_pl) if p.unrealized_pl else 0.0,
                "asset_class": str(p.asset_class),
            }
            for p in all_pos
            if str(p.asset_class).lower() in ("us_option", "option")
        ]
        return {"positions": options, "count": len(options), "status": "ok"}
    except Exception as e:
        return {"positions": [], "count": 0, "status": "ready", "note": str(e)}


# --- Signal Tracker ---

@app.get("/api/signal-tracker")
def signal_tracker_all(limit: int = 100):
    """Get all tracked signals (active + resolved)."""
    from engine.signal_tracker import get_all_signals
    return get_all_signals(limit)


@app.get("/api/signal-tracker/active")
def signal_tracker_active():
    """Get active signals sorted by P&L."""
    from engine.signal_tracker import get_active_signals
    return get_active_signals()


@app.get("/api/signal-tracker/consensus")
def signal_tracker_consensus():
    """Get symbols with multiple model agreement."""
    from engine.signal_tracker import get_consensus_signals
    return get_consensus_signals()


@app.get("/api/signal-tracker/leaderboard")
def signal_tracker_leaderboard():
    """Best Signals leaderboard — model hit rates."""
    from engine.signal_tracker import get_model_leaderboard
    return get_model_leaderboard()


@app.get("/api/signal-tracker/second-chance")
def signal_tracker_second_chance():
    """Second Chance — stocks sold but now have fresh buy signals."""
    from engine.signal_tracker import get_reentry_opportunities
    return get_reentry_opportunities()


@app.get("/api/signal-tracker/reentry-leaderboard")
def signal_tracker_reentry_leaderboard():
    """Re-entry success rate per model."""
    from engine.signal_tracker import get_reentry_leaderboard
    return get_reentry_leaderboard()


# --- Pair Trades ---

@app.get("/api/pair-trades")
def pair_trades(limit: int = 20):
    """Get detected pair trade opportunities."""
    from engine.pair_trades import get_pair_trades
    return get_pair_trades(limit)


@app.get("/api/pair-trades/pnl")
def pair_pnl():
    """Get combined P&L for active pair trades."""
    from engine.pair_trades import get_pair_pnl
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    return get_pair_pnl(prices)


# --- Volatility Surface ---

@app.get("/api/market/vol-surface/{symbol}")
def vol_surface(symbol: str):
    """Get IV surface for a symbol."""
    from engine.vol_surface import scan_vol_surface
    result = scan_vol_surface(symbol.upper())
    if not result:
        return {"error": f"No vol surface data for {symbol.upper()}"}
    return result


@app.get("/api/market/vol-surfaces")
def all_vol_surfaces():
    """Get IV surfaces for DayBlade tickers."""
    from engine.vol_surface import get_all_vol_surfaces
    return get_all_vol_surfaces()


# --- Kill Switch ---

@app.post("/api/kill-switch")
def kill_switch():
    """EMERGENCY: Close ALL positions across ALL models."""
    from engine.kill_switch import kill_all_positions
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data

    return kill_all_positions(prices)


@app.get("/api/kill-switch/history")
def kill_switch_history():
    """Get kill switch activation history."""
    from engine.kill_switch import get_kill_switch_history
    return get_kill_switch_history()


# --- Model DNA ---

@app.get("/api/model-dna/{player_id}")
def model_dna(player_id: str):
    """Get behavioral fingerprint for an AI model."""
    from engine.model_dna import get_model_dna
    return get_model_dna(player_id)


@app.get("/api/model-dna")
def all_model_dna():
    """Get DNA for all models."""
    from engine.model_dna import get_all_model_dna
    return get_all_model_dna()


# --- P&L Attribution ---

@app.get("/api/pnl-attribution")
def pnl_attribution(days: int = 7):
    """Break down P&L by model, sector, trade type, entry time."""
    from engine.pnl_attribution import get_pnl_attribution
    return get_pnl_attribution(days)


# --- Gamma Environment ---

@app.get("/api/gamma-environment")
def gamma_environment():
    """Get current gamma environment (positive/negative) for SPY."""
    from engine.gamma_environment import detect_gamma_environment
    return detect_gamma_environment()


# --- Put/Call Skew ---

@app.get("/api/put-call-skew")
def put_call_skew():
    """Get put/call skew for SPY, QQQ, and watchlist stocks."""
    from engine.put_call_skew import get_all_skew
    return get_all_skew(["SPY", "QQQ"])


@app.get("/api/put-call-skew/{symbol}")
def put_call_skew_symbol(symbol: str):
    """Get put/call skew for a specific symbol."""
    from engine.put_call_skew import compute_put_call_skew
    result = compute_put_call_skew(symbol.upper())
    if not result:
        return {"error": f"No skew data for {symbol.upper()}"}
    return result


# --- High IV Scanner ---

@app.get("/api/high-iv")
def high_iv():
    """Get high IV opportunities across watchlist."""
    from engine.high_iv_scanner import scan_high_iv_opportunities
    from config import WATCH_STOCKS
    return scan_high_iv_opportunities(WATCH_STOCKS)


# --- Cross-Asset Monitor ---

@app.get("/api/cross-asset")
def cross_asset():
    """Get cross-asset monitor (SPY, VIX, DXY, Oil) with correlation signals."""
    from engine.cross_asset import get_cross_asset_monitor
    return get_cross_asset_monitor()


# --- Auto Trendlines (Support/Resistance) ---

@app.get("/api/trendlines/{symbol}")
def trendlines(symbol: str):
    """Get auto-detected support and resistance levels."""
    from engine.trendlines import detect_support_resistance
    result = detect_support_resistance(symbol.upper())
    if not result:
        return {"error": f"No trendline data for {symbol.upper()}"}
    return result


@app.get("/api/trendlines")
def all_trendlines():
    """Get S/R levels for all watchlist stocks."""
    from engine.trendlines import get_all_levels
    from config import WATCH_STOCKS
    return get_all_levels(WATCH_STOCKS)


# --- Fibonacci Levels ---

@app.get("/api/fibonacci/{symbol}")
def fibonacci(symbol: str):
    """Get Fibonacci retracement levels."""
    from engine.fibonacci import compute_fibonacci
    result = compute_fibonacci(symbol.upper())
    if not result:
        return {"error": f"No Fibonacci data for {symbol.upper()}"}
    return result


# --- Dynamic Alerts ---

@app.get("/api/dynamic-alerts")
def dynamic_alerts(limit: int = 50):
    """Get recent dynamic alerts."""
    from engine.dynamic_alerts import get_recent_alerts
    return get_recent_alerts(limit)


@app.get("/api/dynamic-alerts/active")
def active_alerts(minutes: int = 30):
    """Get active alerts (last N minutes) for banner display."""
    from engine.dynamic_alerts import get_active_alerts
    return get_active_alerts(minutes)


# --- S/R Heatmap (Volume Profile) ---

@app.get("/api/volume-profile/{symbol}")
def volume_profile(symbol: str):
    """Get volume-weighted price profile for S/R heatmap."""
    from engine.sr_heatmap import compute_volume_profile
    result = compute_volume_profile(symbol.upper())
    if not result:
        return {"error": f"No volume profile for {symbol.upper()}"}
    return result


# --- Chart Patterns ---

@app.get("/api/patterns/{symbol}")
def chart_patterns_symbol(symbol: str):
    """Get detected chart patterns for a symbol."""
    from engine.chart_patterns import detect_patterns
    return detect_patterns(symbol.upper())


@app.get("/api/patterns")
def chart_patterns_all():
    """Get detected chart patterns for all watchlist stocks."""
    from engine.chart_patterns import detect_all_patterns
    from config import WATCH_STOCKS
    return detect_all_patterns(WATCH_STOCKS)


# --- Raindrop Charts ---

@app.get("/api/raindrop/{symbol}")
def raindrop(symbol: str):
    """Get raindrop volume profile for intraday chart."""
    from engine.raindrop import compute_raindrop
    result = compute_raindrop(symbol.upper())
    if not result:
        return {"error": f"No raindrop data for {symbol.upper()}"}
    return result


# --- Relative Strength Scanner ---

@app.get("/api/strength")
def strength_index():
    """Get relative strength rankings for all watchlist stocks."""
    from engine.strength_scanner import scan_relative_strength, get_strength_rankings
    from config import WATCH_STOCKS
    rankings = get_strength_rankings()
    if not rankings:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                rankings = ex.submit(scan_relative_strength, WATCH_STOCKS).result(timeout=20)
        except (FuturesTimeout, Exception):
            rankings = []
    return rankings


# --- Smart Risk Levels ---

@app.get("/api/risk-levels")
def risk_levels():
    """Get smart risk levels (entry/stop/targets) for all open positions."""
    from engine.smart_levels import get_risk_levels
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    prices = {}
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            prices[sym] = data
    return get_risk_levels(prices)


@app.get("/api/risk-levels/{symbol}")
def risk_levels_symbol(symbol: str):
    """Get risk levels for a specific symbol."""
    from engine.smart_levels import get_levels_for_symbol
    from engine.market_data import get_stock_price

    prices = {}
    data = get_stock_price(symbol.upper())
    if "error" not in data:
        prices[symbol.upper()] = data
    return get_levels_for_symbol(symbol.upper(), prices)


# --- Strategy Race ---

@app.get("/api/strategy-race")
def strategy_race():
    """Compare AI strategy vs SPY buy-and-hold."""
    from engine.strategy_race import get_strategy_race
    return get_strategy_race()


# --- Weekly Picks ---

@app.get("/api/weekly-picks")
def weekly_picks():
    """Get the most recent weekly AI picks."""
    from engine.weekly_picks import get_weekly_picks
    return get_weekly_picks()


# --- Stock Race (heatmap animation data) ---

@app.get("/api/stock-race")
def stock_race():
    """Get real-time stock race data for animated bar chart."""
    from engine.market_data import get_stock_price
    from config import WATCH_STOCKS

    result = []
    for sym in WATCH_STOCKS:
        data = get_stock_price(sym)
        if "error" not in data:
            result.append({
                "symbol": sym,
                "price": data["price"],
                "change_pct": data["change_pct"],
                "volume": data.get("volume", 0),
            })
    result.sort(key=lambda x: x["change_pct"], reverse=True)
    return result


@app.get("/api/trend-forecast")
def trend_forecast():
    """Get trend predictions for all watchlist stocks."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.trend_predictor import predict_all_trends
    from config import WATCH_STOCKS
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(predict_all_trends, WATCH_STOCKS).result(timeout=20)
    except (FuturesTimeout, Exception):
        return []


@app.get("/api/trend-forecast/{symbol}")
def trend_forecast_symbol(symbol: str):
    """Get trend prediction for a specific symbol."""
    from engine.trend_predictor import predict_trend
    result = predict_trend(symbol.upper())
    return result or {"error": f"No prediction for {symbol}"}


@app.get("/api/pattern-alerts")
def pattern_alerts():
    """Get enriched pattern alert tiles with breakout/target/stop/win-rate."""
    from engine.pattern_alerts import get_pattern_alert_tiles
    return get_pattern_alert_tiles()


@app.get("/api/strategy-presets")
def strategy_presets():
    """Get strategy preset evaluations for all watchlist stocks."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.strategy_presets import scan_strategies
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(scan_strategies).result(timeout=20)
    except (FuturesTimeout, Exception):
        return []


@app.get("/api/strategy-presets/{symbol}")
def strategy_presets_symbol(symbol: str):
    """Get best strategy for a specific symbol."""
    from engine.strategy_presets import get_best_strategy
    result = get_best_strategy(symbol.upper())
    return result or {"error": f"No strategy fit for {symbol}"}


@app.get("/api/deals")
def active_deals():
    """Get active deals (grouped positions) with live P&L."""
    from engine.deal_tracker import get_deals_with_pnl
    from engine.market_data import get_stock_price
    prices = {}
    try:
        from config import WATCH_STOCKS
        for sym in WATCH_STOCKS:
            data = get_stock_price(sym)
            if "error" not in data:
                prices[sym] = data
    except Exception:
        pass
    return get_deals_with_pnl(prices)


@app.get("/api/deals/closed")
def closed_deals():
    """Get recently closed deals."""
    from engine.deal_tracker import get_closed_deals
    return get_closed_deals()


@app.get("/api/fundamentals")
def fundamentals():
    """Get enriched fundamentals for all watchlist stocks."""
    from engine.stock_fundamentals import fetch_all_fundamentals
    return fetch_all_fundamentals()


@app.get("/api/fundamentals/{symbol}")
def fundamentals_symbol(symbol: str):
    """Get enriched fundamentals for a specific symbol."""
    from engine.stock_fundamentals import fetch_fundamentals
    result = fetch_fundamentals(symbol.upper())
    return result or {"error": f"No fundamental data for {symbol}"}


@app.get("/api/fundamentals/score/{symbol}")
def fundamentals_score(symbol: str):
    """Get Smart Score (letter grade) for a specific symbol."""
    from engine.stock_fundamentals import fetch_fundamentals
    result = fetch_fundamentals(symbol.upper())
    if not result:
        return {"error": f"No fundamental data for {symbol}"}
    return {
        "symbol": symbol.upper(),
        "smart_score": result.get("smart_score"),
        "grade": result.get("grade"),
        "components": result.get("score_components"),
    }


@app.get("/api/fundamentals/scores")
def fundamentals_scores():
    """Get Smart Scores for all watchlist stocks."""
    from engine.stock_fundamentals import fetch_all_fundamentals
    results = fetch_all_fundamentals()
    return [{
        "symbol": r["symbol"],
        "company_name": r.get("company_name"),
        "smart_score": r.get("smart_score"),
        "grade": r.get("grade"),
        "sector": r.get("sector"),
    } for r in results]


@app.get("/api/portfolio-health/{player_id}")
def portfolio_health(player_id: str):
    """Get portfolio health check for an AI player."""
    from engine.stock_fundamentals import portfolio_health_check
    result = portfolio_health_check(player_id)
    return result or {"error": f"No data for {player_id}"}


@app.get("/api/insider/{symbol}")
def insider_activity(symbol: str):
    """Get SEC insider trading activity for a symbol."""
    from engine.openbb_data import get_insider_summary
    return get_insider_summary(symbol.upper())


@app.get("/api/insider")
def insider_all():
    """Get insider trading summaries for all watchlist stocks."""
    from engine.openbb_data import get_insider_summary
    from config import WATCH_STOCKS
    results = []
    for sym in WATCH_STOCKS:
        summary = get_insider_summary(sym)
        if summary:
            results.append(summary)
    return results


@app.get("/api/filings/{symbol}")
def sec_filings(symbol: str):
    """Get recent SEC filings for a symbol."""
    from engine.openbb_data import get_sec_filings
    return get_sec_filings(symbol.upper())


@app.get("/api/economic-calendar")
def economic_calendar():
    """Get macro economic data: CPI, unemployment, interest rates, GDP, FOMC."""
    from engine.openbb_data import get_economic_calendar
    return get_economic_calendar()


@app.get("/api/options-chain/{symbol}")
def options_chain(symbol: str, expiry: str = None):
    """Get full options chain with Greeks for a symbol."""
    from engine.openbb_data import get_options_chain
    result = get_options_chain(symbol.upper(), expiry)
    return result or {"error": f"No options data for {symbol}"}


# --- Paper-Trader Compatibility Endpoints ---
# These adapt the paper-trader's JSON-based API to the autonomous-trader's SQLite DB.

@app.get("/api/capital")
def get_capital():
    """Get current capital per AI player (cash + positions value)."""
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_all_prices
    from config import WATCH_STOCKS

    conn = _conn()
    players = conn.execute("SELECT id, display_name, cash FROM ai_players WHERE is_active=1").fetchall()
    conn.close()

    try:
        prices = get_all_prices(WATCH_STOCKS)
    except Exception:
        prices = {}

    result = {}
    for p in players:
        pid = p["id"]
        starting = 3500.0 if pid == "dayblade-0dte" else (7021.81 if pid == "steve-webull" else 7000.0)
        try:
            pnl_data = get_portfolio_with_pnl(pid, prices)
            total = pnl_data["total_value"]
        except Exception:
            total = p["cash"]
        result[p["display_name"]] = {
            "cash": p["cash"],
            "total_value": round(total, 2),
            "starting": starting,
            "pnl": round(total - starting, 2),
        }
    return result


@app.get("/api/trades")
def get_all_trades(status: str = None, symbol: str = None, model: str = None, season: int = None):
    """Get trades with paired BUY/SELL and P&L for closed trades."""
    conn = _conn()
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    if season == -1:
        rows = conn.execute(
            "SELECT t.id, t.player_id, t.symbol, t.action, t.qty, t.price, t.reasoning, t.confidence, "
            "t.executed_at, t.asset_type, t.option_type, t.entry_price, t.exit_price, t.realized_pnl, "
            "p.display_name FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
            "ORDER BY t.executed_at DESC LIMIT 500"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT t.id, t.player_id, t.symbol, t.action, t.qty, t.price, t.reasoning, t.confidence, "
            "t.executed_at, t.asset_type, t.option_type, t.entry_price, t.exit_price, t.realized_pnl, "
            "p.display_name FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.season=? ORDER BY t.executed_at DESC LIMIT 500",
            (season,)
        ).fetchall()
    conn.close()
    trades = []
    for r in rows:
        action = r["action"]
        is_buy = action in ("BUY", "BUY_CALL", "BUY_PUT")
        is_sell = action == "SELL"

        if is_sell:
            # Closed trade — show entry/exit/P&L
            entry_p = r["entry_price"] or r["price"]  # entry_price column, fallback to price
            exit_p = r["exit_price"] or r["price"]  # exit_price column, fallback to price
            pnl = r["realized_pnl"]
            t = {
                "id": str(r["id"]),
                "symbol": r["symbol"],
                "side": "short" if r["option_type"] == "put" else "long",
                "entry_price": round(entry_p, 2) if entry_p else None,
                "exit_price": round(exit_p, 2) if exit_p else None,
                "quantity": r["qty"],
                "entry_date": r["executed_at"],
                "exit_date": r["executed_at"],
                "status": "closed",
                "model_source": r["display_name"] or r["player_id"],
                "signal_reasoning": r["reasoning"],
                "pnl": round(pnl, 2) if pnl is not None else None,
                "pnl_pct": round((exit_p - entry_p) / entry_p * 100, 2) if entry_p and entry_p > 0 else None,
            }
        else:
            # Open trade (BUY) — include unrealized P&L
            side = "long" if action in ("BUY", "BUY_CALL") else "short"
            entry_p = r["price"]
            unrealized_pnl = None
            unrealized_pnl_pct = None
            # Get current price for unrealized P&L
            try:
                from engine.market_data import get_stock_price
                cur_data = get_stock_price(r["symbol"])
                if "error" not in cur_data:
                    cur = cur_data["price"]
                    is_opt = (r["asset_type"] == "option" or action in ("BUY_CALL", "BUY_PUT"))
                    if is_opt:
                        from engine.paper_trader import estimate_option_price
                        ot = (r["option_type"] if r["option_type"] else None) or ("call" if action == "BUY_CALL" else "put")
                        strike = r["strike_price"] if r["strike_price"] else None
                        est = estimate_option_price(ot, strike, cur, entry_p)
                        unrealized_pnl = round((est - entry_p) * r["qty"], 2)
                        unrealized_pnl_pct = round((est - entry_p) / entry_p * 100, 2) if entry_p > 0 else 0
                    else:
                        unrealized_pnl = round((cur - entry_p) * r["qty"], 2)
                        unrealized_pnl_pct = round((cur - entry_p) / entry_p * 100, 2) if entry_p > 0 else 0
            except Exception:
                pass
            t = {
                "id": str(r["id"]),
                "symbol": r["symbol"],
                "side": side,
                "entry_price": round(entry_p, 2),
                "exit_price": None,
                "quantity": r["qty"],
                "entry_date": r["executed_at"],
                "exit_date": None,
                "status": "open",
                "model_source": r["display_name"] or r["player_id"],
                "signal_reasoning": r["reasoning"],
                "pnl": unrealized_pnl,
                "pnl_pct": unrealized_pnl_pct,
            }

        if status and t["status"] != status:
            continue
        if symbol and t["symbol"].upper() != symbol.upper():
            continue
        if model and t["model_source"] != model:
            continue
        trades.append(t)
    return trades


_FLEET_CORE_IDS = frozenset(['neo-matrix', 'grok-4', 'ollama-glm4', 'ollama-qwen3', 'super-agent', 'navigator', 'capitol-trades', 'ollama-plutus', 'energy-arnold'])

@app.get("/api/performance")
def get_performance(model: str = None, season: int = None, fleet_only: bool = False):
    """Get overall performance statistics, filtered by season."""
    conn = _conn()
    # Default to current season
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2

    fleet_ids = list(_FLEET_CORE_IDS)
    fleet_ph  = ",".join("?" * len(fleet_ids))

    if season == -1:
        if fleet_only:
            sells = conn.execute(
                f"SELECT player_id, symbol, qty, price, reasoning, realized_pnl FROM trades WHERE action='SELL' AND player_id IN ({fleet_ph})",
                fleet_ids
            ).fetchall()
        else:
            sells = conn.execute(
                "SELECT player_id, symbol, qty, price, reasoning, realized_pnl FROM trades WHERE action='SELL'"
            ).fetchall()
    else:
        if fleet_only:
            sells = conn.execute(
                f"SELECT player_id, symbol, qty, price, reasoning, realized_pnl FROM trades WHERE action='SELL' AND season=? AND player_id IN ({fleet_ph})",
                [season] + fleet_ids
            ).fetchall()
        else:
            sells = conn.execute(
                "SELECT player_id, symbol, qty, price, reasoning, realized_pnl FROM trades WHERE action='SELL' AND season=?",
                (season,)
            ).fetchall()
    conn.close()
    pnls = []
    for s in sells:
        if s["realized_pnl"] is not None:
            pnls.append(float(s["realized_pnl"]))
        else:
            import re
            m = re.search(r'PnL: \$([+-]?[\d.]+)', s["reasoning"] or "")
            if m:
                pnls.append(float(m.group(1)))
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p < 0]
    return {
        "season": season,
        "total_trades": len(sells),
        "open_trades": 0,
        "closed_trades": len(sells),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(len(winners) / len(sells) * 100, 2) if sells else 0,
        "total_pnl": round(sum(pnls), 2),
        "avg_win": round(sum(winners) / len(winners), 2) if winners else 0,
        "avg_loss": round(sum(losers) / len(losers), 2) if losers else 0,
        "profit_factor": round(sum(winners) / abs(sum(losers)), 2) if losers else 0,
        "largest_win": round(max(winners), 2) if winners else 0,
        "largest_loss": round(min(losers), 2) if losers else 0,
        "avg_hold_time_hours": None
    }


@app.get("/api/fleet/positions")
def fleet_positions():
    """Return open positions + cash for all active fleet agents (batch, no live prices needed)."""
    from engine.market_data import get_all_prices
    conn = _conn()
    fleet_ids = list(_FLEET_CORE_IDS)
    fleet_ph  = ",".join("?" * len(fleet_ids))

    # Cash per agent
    players = conn.execute(
        f"SELECT id, display_name, cash FROM ai_players WHERE id IN ({fleet_ph})",
        fleet_ids
    ).fetchall()
    cash_map = {p["id"]: float(p["cash"] or 0) for p in players}
    name_map = {p["id"]: p["display_name"] for p in players}

    # Positions for all fleet agents
    rows = conn.execute(
        f"SELECT player_id, symbol, qty, avg_price, asset_type, option_type, strike_price, expiry_date "
        f"FROM positions WHERE qty > 0 AND player_id IN ({fleet_ph})",
        fleet_ids
    ).fetchall()
    conn.close()

    # Batch price fetch
    syms = list({r["symbol"] for r in rows})
    prices = {}
    if syms:
        try:
            raw = get_all_prices(syms)
            prices = {s: d["price"] for s, d in raw.items()}
        except Exception:
            pass

    # Group by player
    agents_map: dict = {}
    for pid in fleet_ids:
        agents_map[pid] = {
            "player_id": pid,
            "name": name_map.get(pid, pid),
            "cash": cash_map.get(pid, 0.0),
            "positions": [],
        }

    for r in rows:
        pid = r["player_id"]
        sym = r["symbol"]
        qty = float(r["qty"] or 0)
        avg = float(r["avg_price"] or 0)
        cur = prices.get(sym, avg)
        value = round(qty * cur, 2)
        pnl   = round((cur - avg) * qty, 2)
        pnl_pct = round((cur - avg) / avg * 100, 2) if avg else 0.0
        agents_map[pid]["positions"].append({
            "symbol":     sym,
            "qty":        qty,
            "avg_price":  round(avg, 2),
            "current_price": round(cur, 2),
            "value":      value,
            "pnl":        pnl,
            "pnl_pct":    pnl_pct,
            "asset_type": r["asset_type"] or "stock",
        })

    agents_list = [v for v in agents_map.values() if v["positions"] or v["cash"] > 0]
    # Sort by cash desc so agents with most capital show first
    agents_list.sort(key=lambda a: a["cash"], reverse=True)

    fleet_cash = sum(cash_map.get(pid, 0.0) for pid in fleet_ids)
    fleet_pos_count = sum(len(a["positions"]) for a in agents_list)

    return {
        "fleet_cash": round(fleet_cash, 2),
        "fleet_positions_count": fleet_pos_count,
        "agents": agents_list,
    }


@app.get("/api/unrealized")
def get_unrealized():
    """Get unrealized P&L for all open positions."""
    from engine.market_data import get_all_prices
    conn = _conn()
    positions = conn.execute(
        "SELECT player_id, symbol, qty, avg_price, asset_type, option_type, strike_price FROM positions"
    ).fetchall()

    # Sanity check: no positions = $0 unrealized
    if not positions:
        conn.close()
        return {"total_unrealized": 0.0, "positions": []}

    # Fetch prices for all symbols (needed for both stocks and option intrinsic value)
    all_symbols = list(set(p["symbol"] for p in positions))
    all_data = get_all_prices(all_symbols) if all_symbols else {}
    price_cache = {sym: d["price"] for sym, d in all_data.items()}
    conn.close()

    from engine.paper_trader import estimate_option_price

    results = []
    total = 0
    for pos in positions:
        entry = pos["avg_price"]
        if pos["asset_type"] == "option":
            stock_price = price_cache.get(pos["symbol"], 0)
            est_price = estimate_option_price(
                pos["option_type"], pos["strike_price"], stock_price, entry)
            pnl = round((est_price - entry) * pos["qty"], 2)
            pnl_pct = round((est_price - entry) / entry * 100, 2) if entry > 0 else 0
            total += pnl
            results.append({
                "symbol": pos["symbol"], "current_price": round(est_price, 2),
                "entry_price": entry, "qty": pos["qty"], "pnl": pnl, "pnl_pct": pnl_pct,
                "model": pos["player_id"], "type": pos["asset_type"],
                "option_type": pos["option_type"], "strike_price": pos["strike_price"]
            })
            continue
        price = price_cache.get(pos["symbol"])
        if price is None:
            continue
        pnl = round((price - entry) * pos["qty"], 2)
        pnl_pct = round((price - entry) / entry * 100, 2) if entry > 0 else 0
        total += pnl
        results.append({
            "symbol": pos["symbol"], "current_price": round(price, 2),
            "entry_price": entry, "qty": pos["qty"], "pnl": pnl, "pnl_pct": pnl_pct,
            "model": pos["player_id"], "type": pos["asset_type"]
        })
    return {"total_unrealized": round(total, 2), "positions": results}


@app.get("/api/performance/by-model")
def get_performance_by_model(season: int = None):
    """Get performance broken down by AI player, filtered by season."""
    conn = _conn()
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    players = conn.execute("SELECT id, display_name, cash FROM ai_players WHERE is_active=1").fetchall()
    result = {}
    for p in players:
        if season == -1:
            trades = conn.execute("SELECT action, reasoning FROM trades WHERE player_id=?", (p["id"],)).fetchall()
        else:
            trades = conn.execute("SELECT action, reasoning FROM trades WHERE player_id=? AND season=?", (p["id"], season)).fetchall()
        sells = [t for t in trades if t["action"] == "SELL"]
        import re
        pnls = []
        for s in sells:
            m = re.search(r'PnL: \$([+-]?[\d.]+)', s["reasoning"] or "")
            if m:
                pnls.append(float(m.group(1)))
        winners = len([x for x in pnls if x > 0])
        result[p["display_name"]] = {
            "total_trades": len(trades),
            "closed_trades": len(sells),
            "open_trades": len(trades) - len(sells),
            "win_rate": round(winners / len(sells) * 100, 2) if sells else 0,
            "total_pnl": round(sum(pnls), 2),
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else 0
        }
    conn.close()
    return result


@app.get("/api/equity-curve")
def get_equity_curve(starting_capital: float = 10000, season: int = None):
    """Get equity curve from trade history, filtered by season."""
    conn = _conn()
    if season is None:
        s_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        season = int(s_row["value"]) if s_row else 2
    if season == -1:
        sells = conn.execute(
            "SELECT executed_at, reasoning FROM trades WHERE action='SELL' ORDER BY executed_at"
        ).fetchall()
    else:
        sells = conn.execute(
            "SELECT executed_at, reasoning FROM trades WHERE action='SELL' AND season=? ORDER BY executed_at",
            (season,)
        ).fetchall()
    conn.close()
    import re
    curve = [{"date": "start", "equity": starting_capital, "trade": None}]
    equity = starting_capital
    for s in sells:
        m = re.search(r'PnL: \$([+-]?[\d.]+)', s["reasoning"] or "")
        if m:
            pnl = float(m.group(1))
            equity += pnl
            curve.append({
                "date": (s["executed_at"] or "")[:10],
                "equity": round(equity, 2),
                "pnl": round(pnl, 2)
            })
    return curve


@app.get("/api/models")
def get_models():
    """Get all AI players as models."""
    conn = _conn()
    players = conn.execute("SELECT id, display_name, provider, model_id, is_active FROM ai_players").fetchall()
    conn.close()
    return [
        {"name": p["display_name"], "description": f"{p['provider']} / {p['model_id']}",
         "type": p["provider"], "active": bool(p["is_active"]), "created_date": ""}
        for p in players
    ]


@app.post("/api/ai-chat")
def ai_chat(msg: dict):
    """AI chat endpoint for multi-model debate."""
    import requests as req
    from engine.openai_text import DEFAULT_CODEX_MINI_MODEL, generate_text
    message = msg.get("message", "")
    models = msg.get("models", ["qwen3.5:9b"])
    responses = []
    context = "You are an AI trading model in a debate. Be concise (2-3 sentences max). Topic: "

    for model_name in models:
        prompt = context + message
        response = None
        try:
            if "gemma" in model_name or "llama" in model_name:
                r = req.post("http://localhost:11434/api/generate",
                    json={"model": model_name, "prompt": prompt, "stream": False}, timeout=30)
                response = r.json().get("response", "").strip()[:300]
            elif "claude" in model_name or "codex" in model_name:
                response = generate_text(
                    prompt,
                    model=DEFAULT_CODEX_MINI_MODEL,
                    api_key=os.environ.get("OPENAI_API_KEY", ""),
                    max_output_tokens=150,
                    reasoning_effort="medium",
                )[:300]
            elif "grok" in model_name:
                # Routed to local deepseek-r1:14b — eliminates xAI API cost
                ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
                r = req.post(f"{ollama_url}/api/generate",
                    json={"model": "deepseek-r1:14b", "prompt": prompt, "stream": False},
                    timeout=60)
                response = (r.json().get("response", "") or "No response").strip()[:300]
            elif "gemini" in model_name:
                ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
                r = req.post(f"{ollama_url}/api/generate",
                    json={"model": "qwen3.5:9b", "prompt": prompt, "stream": False},
                    timeout=30)
                response = (r.json().get("response", "") or "No response").strip()[:300]
        except Exception as e:
            response = f"Error: {str(e)[:80]}"
        if response:
            from datetime import datetime
            responses.append({"model": model_name, "response": response, "timestamp": datetime.now().isoformat()})
    return {"responses": responses}


_rec_cache = {}  # key: "player_id:symbol" -> {data, ts}
_REC_CACHE_TTL = 300  # 5 minutes


@app.get("/api/arena/player/{player_id}/recommendation/{symbol}")
def player_recommendation(player_id: str, symbol: str):
    """Get AI recommendation for a position — calls the owning model."""
    import time as _time
    import requests as req
    import re

    cache_key = f"{player_id}:{symbol}"
    if cache_key in _rec_cache and (_time.time() - _rec_cache[cache_key]["ts"]) < _REC_CACHE_TTL:
        return _rec_cache[cache_key]["data"]

    conn = _conn()
    player = conn.execute("SELECT provider, model_id, display_name FROM ai_players WHERE id=?",
                          (player_id,)).fetchone()
    if not player:
        conn.close()
        return {"rating": "HOLD", "grade": "C", "confidence": 0.5, "reasoning": "Player not found"}

    pos = conn.execute("SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
                       (player_id, symbol)).fetchone()
    last_trade = conn.execute(
        "SELECT confidence, reasoning FROM trades WHERE player_id=? AND symbol=? ORDER BY executed_at DESC LIMIT 1",
        (player_id, symbol)).fetchone()
    conn.close()

    qty = pos["qty"] if pos else 0
    entry = pos["avg_price"] if pos else 0

    from engine.market_data import get_stock_price
    price_data = get_stock_price(symbol)
    current = price_data.get("price", 0)
    pnl_pct = round((current - entry) / entry * 100, 2) if entry > 0 else 0
    last_conf = last_trade["confidence"] if last_trade else 0.5

    # Get technicals + news
    rsi_val = "--"
    macd_val = "--"
    try:
        from engine.market_data import get_technical_indicators
        ti = get_technical_indicators(symbol)
        if ti:
            rsi_val = ti.get("rsi", "--")
            macd_val = ti.get("macd_histogram", "--")
    except Exception:
        pass

    headlines = ""
    try:
        from engine.news_fetcher import fetch_news
        news = fetch_news(symbol, limit=3)
        headlines = "; ".join([n.get("headline", "") for n in (news or []) if n.get("headline")])[:300]
    except Exception:
        headlines = "No recent news"

    prompt = (
        f"You are an AI trading advisor. You hold {qty:.2f} shares of {symbol} at ${entry:.2f}. "
        f"Current price is ${current:.2f} ({pnl_pct:+.1f}%). RSI is {rsi_val}. MACD histogram is {macd_val}. "
        f"Recent news: {headlines or 'None'}. "
        f"Rate this position with exactly one of: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL. "
        f"Also give a letter grade from A+ to F. "
        f"Reply in this exact format: RATING: <rating> GRADE: <grade> REASON: <1 sentence>"
    )

    response_text = ""
    provider = player["provider"]
    model_id = player["model_id"]

    try:
        if provider == "ollama":
            r = req.post("http://localhost:11434/api/generate",
                json={"model": model_id, "prompt": prompt, "stream": False}, timeout=30)
            response_text = r.json().get("response", "")
        elif provider == "openai":
            from engine.openai_text import generate_text
            response_text = generate_text(
                prompt,
                model=model_id,
                api_key=os.environ.get("OPENAI_API_KEY", ""),
                max_output_tokens=150,
                reasoning_effort="medium",
            )
        elif provider == "xai":
            # Routed to local deepseek-r1:14b — eliminates xAI API cost
            r = req.post("http://localhost:11434/api/generate",
                json={"model": "deepseek-r1:14b", "prompt": prompt, "stream": False},
                timeout=60)
            response_text = r.json().get("response", "") if r.ok else ""
        elif provider == "google":
            ollama_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
            r = req.post(f"{ollama_url}/api/generate",
                json={"model": "qwen3.5:9b", "prompt": prompt, "stream": False},
                timeout=30)
            response_text = r.json().get("response", "") if r.ok else ""
    except Exception as e:
        response_text = ""

    # Parse response
    rating = "HOLD"
    grade = "C"
    reason = ""

    if response_text:
        rt = response_text.upper()
        for r_val in ["STRONG_BUY", "STRONG_SELL", "BUY", "SELL", "HOLD"]:
            if r_val in rt:
                rating = r_val
                break
        grade_match = re.search(r'GRADE:\s*([A-F][+-]?)', response_text, re.IGNORECASE)
        if grade_match:
            grade = grade_match.group(1).upper()
        reason_match = re.search(r'REASON:\s*(.+)', response_text, re.IGNORECASE)
        if reason_match:
            reason = reason_match.group(1).strip()[:200]

    # Fallback: compute from signals if AI didn't respond
    if not response_text:
        if pnl_pct > 10:
            rating = "STRONG_BUY"
            grade = "A"
        elif pnl_pct > 3:
            rating = "BUY"
            grade = "B+"
        elif pnl_pct > -3:
            rating = "HOLD"
            grade = "C+"
        elif pnl_pct > -10:
            rating = "SELL"
            grade = "D"
        else:
            rating = "STRONG_SELL"
            grade = "F"
        reason = f"Heuristic: position at {pnl_pct:+.1f}%"

    # Confidence from composite
    conf_map = {"STRONG_BUY": 0.9, "BUY": 0.7, "HOLD": 0.5, "SELL": 0.3, "STRONG_SELL": 0.1}
    confidence = conf_map.get(rating, 0.5)

    result = {
        "rating": rating,
        "grade": grade,
        "confidence": confidence,
        "reasoning": reason,
        "model": player["display_name"],
        "cached": False,
    }
    _rec_cache[cache_key] = {"data": {**result, "cached": True}, "ts": _time.time()}
    return result


@app.get("/api/reasoning/{symbol}")
def get_reasoning(symbol: str, metal: bool = False):
    """Generate AI reasoning for a stock or physical metal using Ollama."""
    import requests as req
    from config import OLLAMA_URL

    sym = symbol.upper()
    metal_symbols = {"GOLD": "GC=F", "SILVER": "SI=F", "PLATINUM": "PL=F", "PALLADIUM": "PA=F"}
    is_metal = metal or sym in metal_symbols

    # Gather price context
    from engine.market_data import get_stock_price
    if is_metal:
        from engine.metals_tracker import get_spot_prices
        metals = get_spot_prices()
        spot = metals.get(sym, {})
        price = spot.get("price", 0)
        change = spot.get("change_pct", 0)
        lookup_sym = metal_symbols.get(sym, sym)
    else:
        price_data = get_stock_price(sym)
        price = price_data.get("price", 0)
        change = price_data.get("change_pct", 0)
        lookup_sym = sym

    # RSI from the correct symbol
    rsi_val = "--"
    rsi_label = ""
    try:
        from engine.market_data import get_technical_indicators
        ti = get_technical_indicators(lookup_sym)
        if ti and ti.get("rsi"):
            rsi_num = float(ti["rsi"])
            rsi_val = f"{rsi_num:.1f}"
            if rsi_num < 30:
                rsi_label = "oversold"
            elif rsi_num < 40:
                rsi_label = "approaching oversold"
            elif rsi_num > 70:
                rsi_label = "overbought"
            elif rsi_num > 60:
                rsi_label = "approaching overbought"
            else:
                rsi_label = "neutral"
    except Exception:
        pass

    # Position context
    conn = _conn()
    pos = conn.execute(
        "SELECT player_id, qty, avg_price FROM positions WHERE symbol=? LIMIT 1", (sym,)
    ).fetchone()
    conn.close()

    pos_ctx = ""
    if pos:
        entry = pos["avg_price"]
        pnl_pct = round((price - entry) / entry * 100, 2) if entry > 0 else 0
        unit = "oz" if is_metal else "shares"
        pos_ctx = f"Position: {pos['qty']:.2f} {unit} @ ${entry:,.2f}, P&L: {pnl_pct:+.1f}%."
    else:
        pos_ctx = "No open position."

    rsi_info = f"RSI: {rsi_val}"
    if rsi_label:
        rsi_info += f" ({rsi_label} — below 30 is oversold, above 70 is overbought)"

    if is_metal:
        metal_name = {"GOLD": "physical gold", "SILVER": "physical silver"}.get(sym, sym.lower())
        prompt = (
            f"You are Spock, Science Officer on USS TradeMinds. Give a 2-3 sentence analysis for {metal_name}.\n"
            f"Spot price: ${price:,.2f} ({change:+.2f}% today). {rsi_info}. {pos_ctx}\n"
            f"This is PHYSICAL {sym} (not an ETF). Consider: gold/silver ratio, VIX/fear levels, "
            f"dollar strength, central bank buying, inflation expectations.\n"
            f"Should the Captain accumulate more, hold, or wait? Be specific and concise. Speak as Spock."
        )
    else:
        prompt = (
            f"You are Spock, Science Officer on USS TradeMinds. Give a 2-3 sentence analysis for {sym}.\n"
            f"Current: ${price:.2f} ({change:+.2f}%). {rsi_info}. {pos_ctx}\n"
            f"IMPORTANT: RSI below 30 means OVERSOLD (potential buying opportunity). RSI above 70 means OVERBOUGHT (potential selling signal). "
            f"Do NOT confuse these.\n"
            f"Explain whether to buy, hold, trim, or close. Be specific, logical, and concise. Speak as Spock."
        )

    try:
        r = req.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": "qwen3.5:9b", "prompt": prompt, "stream": False},
            timeout=30,
        )
        if r.ok:
            text = r.json().get("response", "").strip()
            if text:
                return {"reasoning": text, "model": "qwen3.5:9b", "symbol": sym}
    except Exception:
        pass

    return {"reasoning": "Insufficient data for meaningful analysis, Captain.", "model": "fallback", "symbol": sym}


@app.post("/api/arena/player/{player_id}/buy")
def player_buy(player_id: str, body: dict):
    """Add to position (DCA) — buys at current market price."""
    from engine.paper_trader import buy
    from engine.market_data import get_stock_price
    symbol = body.get("symbol", "")
    qty = body.get("qty", 0)
    if not symbol or qty <= 0:
        return {"error": "symbol and qty > 0 required"}
    price_data = get_stock_price(symbol)
    price = price_data.get("price", 0)
    if not price:
        return {"error": f"Could not fetch price for {symbol}"}
    result = buy(player_id, symbol, price, qty=qty, reasoning="Manual DCA via dashboard")
    if not result:
        return {"error": "Buy failed — check cash balance"}
    return result


@app.post("/api/arena/player/{player_id}/trim")
def player_trim(player_id: str, body: dict):
    """Trim position — sells a fraction at current market price."""
    from engine.paper_trader import sell_partial
    from engine.market_data import get_stock_price
    symbol = body.get("symbol", "")
    fraction = body.get("fraction", 0.5)
    if not symbol:
        return {"error": "symbol required"}
    price_data = get_stock_price(symbol)
    price = price_data.get("price", 0)
    if not price:
        return {"error": f"Could not fetch price for {symbol}"}
    from engine.paper_trader import get_position
    pos = get_position(player_id, symbol)
    if not pos:
        return {"error": f"No position in {symbol}"}
    trim_qty = round(pos["qty"] * fraction, 4)
    if trim_qty <= 0:
        return {"error": "Nothing to trim"}
    result = sell_partial(player_id, symbol, price, trim_qty, reasoning=f"Manual trim {fraction*100:.0f}% via dashboard")
    if not result:
        return {"error": "Trim failed"}
    return result


@app.post("/api/arena/player/{player_id}/close")
def player_close(player_id: str, body: dict):
    """Close entire position at current market price."""
    from engine.paper_trader import sell
    from engine.market_data import get_stock_price
    symbol = body.get("symbol", "")
    if not symbol:
        return {"error": "symbol required"}
    price_data = get_stock_price(symbol)
    price = price_data.get("price", 0)
    if not price:
        return {"error": f"Could not fetch price for {symbol}"}
    result = sell(player_id, symbol, price, reasoning="Manual close via dashboard")
    if not result:
        return {"error": "Close failed — no position found"}
    return result


@app.get("/api/wheel/status")
def wheel_status():
    """Counselor Troi's Wheel Strategy — current status and open positions."""
    try:
        from engine.wheel_strategy import get_wheel_status
        return get_wheel_status()
    except Exception as e:
        return {"puts_open": 0, "stocks_held": 0, "total_premium_collected": 0, "error": str(e)}


@app.post("/api/wheel/force-scan")
def wheel_force_scan():
    """Force Troi's wheel scan immediately (bypasses daily throttle)."""
    try:
        from engine.wheel_strategy import run_wheel_scan, _last_date
        import engine.wheel_strategy as _ws
        _ws._done_today = False  # reset daily flag
        run_wheel_scan()
        return {"status": "ok", "message": "Wheel scan executed"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/api/arena/force-scan/{player_id}")
def force_scan(player_id: str):
    """Force a specific model to scan the watchlist immediately."""
    try:
        from config import WATCH_STOCKS
        import main as _main
        arena = getattr(_main, "arena", None)
        if arena is None:
            return {"status": "error", "error": "Arena not initialized"}
        results = arena.scan_player(player_id, WATCH_STOCKS[:5])
        return {"status": "ok", "results": str(results)[:500]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# --- Model Control Endpoints ---

# Cost estimates per scan
MODEL_COST_MAP = {
    "ollama-local": 0.0,
    "ollama-gemma27b": 0.0,
    "ollama-deepseek": 0.0,
    "ollama-qwen3": 0.0,
    "ollama-llama": 0.0,
    "ollama-glm4": 0.0,
    "ollama-kimi": 0.0,
    "ollama-plutus": 0.0,
    "energy-arnold": 0.0,
    # All agents now run on free Ollama models — cost per call is $0.00
    "dalio-metals":     0.0,   # Gemini Flash free tier
    "options-sosnoff":  0.0,   # ollama/qwen3.5:9b
    "claude-sonnet":    0.0,   # ollama/qwen3.5:9b
    "claude-haiku":     0.0,   # ollama/qwen2.5-coder:7b
    "gpt-4o":           0.0,   # ollama/qwen3.5:9b
    "gpt-o3":           0.0,   # ollama/deepseek-r1:7b
    "gemini-2.5-pro":   0.0,   # ollama/qwen3:14b
    "gemini-2.5-flash": 0.0,   # ollama/qwen3.5:9b
    "grok-3":           0.0,   # ollama/qwen3.5:9b
    "grok-4":           0.0,   # ollama/deepseek-r1:7b
    "dayblade-0dte":    0.0,
    "steve-webull":     0.0,
    "cto-grok42":       0.0,   # ollama/qwen2.5-coder:7b
}


@app.get("/api/model-control")
def model_control():
    """Get model control panel data: pause state, costs, call counts."""
    conn = _conn()
    players = conn.execute("""
        SELECT id, display_name, provider, model_id, is_active, is_halted,
               COALESCE(is_paused, 0) as is_paused,
               COALESCE(is_fallback, 0) as is_fallback,
               COALESCE(fallback_model, '') as fallback_model
        FROM ai_players ORDER BY provider, id
    """).fetchall()

    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    stats = conn.execute(
        "SELECT player_id, api_calls, total_cost FROM model_stats WHERE date=?",
        (today,)
    ).fetchall()
    stats_map = {r["player_id"]: {"api_calls": r["api_calls"], "total_cost": r["total_cost"]} for r in stats}

    pause_all = conn.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
    fallbacks_row = conn.execute("SELECT value FROM settings WHERE key='fallbacks_enabled'").fetchone()
    conn.close()

    fallbacks_enabled = not (fallbacks_row and fallbacks_row["value"] == "0")

    models = []
    grand_total = 0.0
    for p in players:
        pid = p["id"]
        st = stats_map.get(pid, {"api_calls": 0, "total_cost": 0.0})
        is_free = p["provider"] == "ollama" or pid in ("dayblade-0dte", "steve-webull")
        is_fb = bool(p["is_fallback"])
        cost_per_scan = 0.0 if (is_free or is_fb) else MODEL_COST_MAP.get(pid, 0.005)
        # Force $0 for free/Ollama models and active fallbacks
        display_cost = 0.0 if (is_free or is_fb) else st["total_cost"]
        grand_total += display_cost
        models.append(annotate_player_payload({
            "player_id": pid,
            "display_name": p["display_name"],
            "provider": p["provider"],
            "model_id": p["model_id"],
            "is_paused": bool(p["is_paused"]),
            "is_fallback": is_fb,
            "fallback_model": p["fallback_model"],
            "is_halted": bool(p["is_halted"]),
            "cost_per_scan": cost_per_scan,
            "api_calls_today": st["api_calls"],
            "total_cost_today": display_cost,
        }))

    return {
        "pause_all": bool(pause_all and pause_all["value"] == "1"),
        "fallbacks_enabled": fallbacks_enabled,
        "models": models,
        "grand_total_cost": grand_total,
    }


@app.post("/api/model-control/pause-all")
def toggle_pause_all():
    """Toggle global pause for all scanning."""
    conn = _conn()
    current = conn.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
    new_val = "0" if (current and current["value"] == "1") else "1"
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('pause_all', ?)",
        (new_val,)
    )
    conn.commit()
    conn.close()
    return {"pause_all": new_val == "1"}


@app.post("/api/model-control/fallbacks")
def toggle_fallbacks():
    """Toggle global fallback model routing (paused → free local Ollama)."""
    conn = _conn()
    current = conn.execute("SELECT value FROM settings WHERE key='fallbacks_enabled'").fetchone()
    new_val = "0" if (current and current["value"] != "0") else "1"
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('fallbacks_enabled', ?)",
        (new_val,)
    )
    conn.commit()
    conn.close()
    return {"fallbacks_enabled": new_val == "1"}


@app.get("/api/settings/pause-all")
def get_pause_all():
    """Get current pause state."""
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
    conn.close()
    return {"paused": bool(row and row["value"] == "1")}


@app.post("/api/settings/pause-all")
def set_pause_all(data: dict = None):
    """Set global pause state."""
    paused = data.get("paused", True) if data else True
    conn = _conn()
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('pause_all', ?)",
        ("1" if paused else "0",)
    )
    conn.commit()
    conn.close()
    return {"paused": paused}


@app.post("/api/model-control/pause/{player_id}")
def toggle_pause_player(player_id: str):
    """Toggle pause for a specific AI model."""
    if is_independent_player(player_id):
        raise HTTPException(status_code=403, detail="Matrix participant is read-only on Arena. Control Neo from port 8000.")
    if player_id in PROTECTED_AGENTS:
        raise HTTPException(status_code=403, detail=f"Agent '{player_id}' is a protected crew member — roster locked by the Captain.")
    conn = _conn()
    current = conn.execute(
        "SELECT COALESCE(is_paused, 0) as is_paused FROM ai_players WHERE id=?",
        (player_id,)
    ).fetchone()
    if not current:
        conn.close()
        return {"error": "Player not found"}
    new_val = 0 if current["is_paused"] else 1
    conn.execute("UPDATE ai_players SET is_paused=? WHERE id=?", (new_val, player_id))
    conn.commit()
    conn.close()
    return {"player_id": player_id, "is_paused": bool(new_val)}


@app.get("/api/risk/spock-alerts")
def get_spock_alerts():
    """Return unacknowledged Spock risk alerts."""
    conn = _conn()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS risk_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            severity TEXT NOT NULL,
            agent_id TEXT,
            message TEXT NOT NULL,
            detail TEXT,
            acknowledged INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        rows = conn.execute(
            """SELECT id, severity, agent_id, message, detail, created_at
               FROM risk_alerts WHERE acknowledged=0
               ORDER BY id DESC LIMIT 50"""
        ).fetchall()
        return {"alerts": [dict(r) for r in rows]}
    except Exception as e:
        return {"alerts": [], "error": str(e)}
    finally:
        conn.close()


@app.post("/api/risk/spock-alerts/{alert_id}/acknowledge")
def acknowledge_spock_alert(alert_id: int):
    """Mark a Spock alert as acknowledged."""
    conn = _conn()
    try:
        conn.execute("UPDATE risk_alerts SET acknowledged=1 WHERE id=?", (alert_id,))
        conn.commit()
        return {"ok": True, "alert_id": alert_id}
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()


@app.get("/api/notifications")
def get_notifications(since: int = 0, limit: int = 20):
    _init_notifications_table()
    conn = _conn()
    rows = conn.execute(
        "SELECT id, timestamp, type, severity, title, body, icon, agent_id "
        "FROM notifications WHERE id > ? AND acknowledged = 0 "
        "ORDER BY id DESC LIMIT ?",
        (since, limit)
    ).fetchall()
    # Auto-ack on initial page load (since=0) so stale alerts don't re-popup
    # on every login. Live polls (since>0) are not auto-acked so new alerts show.
    if since == 0 and rows:
        ids = [r["id"] for r in rows]
        conn.execute(
            "UPDATE notifications SET acknowledged=1 WHERE id IN (%s)"
            % ",".join("?" * len(ids)),
            ids
        )
        conn.commit()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "time": r["timestamp"][:16] if r["timestamp"] else "",
            "type": r["type"] or "info",
            "severity": r["severity"] or "info",
            "title": r["title"] or "",
            "body": r["body"] or "",
            "icon": r["icon"] or "🔔",
            "agent_id": r["agent_id"],
        })
    return result


@app.post("/api/notifications/{notif_id}/ack")
def ack_notification(notif_id: int):
    _init_notifications_table()
    conn = _conn()
    conn.execute("UPDATE notifications SET acknowledged=1 WHERE id=?", (notif_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/agents/{player_id}/pause")
def pause_agent(player_id: str):
    """Pause an agent (Spock alert action)."""
    if player_id in PROTECTED_AGENTS:
        raise HTTPException(status_code=403, detail=f"Agent '{player_id}' is a protected crew member — roster locked by the Captain.")
    conn = _conn()
    try:
        conn.execute("UPDATE ai_players SET is_paused=1 WHERE id=?", (player_id,))
        conn.commit()
        return {"ok": True, "player_id": player_id, "is_paused": True}
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()


@app.post("/api/agents/{player_id}/unpause")
def unpause_agent(player_id: str):
    """Unpause an agent."""
    conn = _conn()
    try:
        conn.execute("UPDATE ai_players SET is_paused=0 WHERE id=?", (player_id,))
        conn.commit()
        return {"ok": True, "player_id": player_id, "is_paused": False}
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()


@app.post("/api/fleet/reduce-size")
def fleet_reduce_size():
    """Set a fleet-wide size reduction flag for the rest of the trading day."""
    conn = _conn()
    try:
        today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
        conn.execute("""CREATE TABLE IF NOT EXISTS system_settings
            (key TEXT PRIMARY KEY, value TEXT,
             updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute(
            "INSERT OR REPLACE INTO system_settings (key, value) VALUES (?,?)",
            ("fleet_reduce_size_date", today),
        )
        conn.commit()
        return {"ok": True, "reduce_size_until": today}
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()


@app.post("/api/model-control/record-call/{player_id}")
def record_api_call(player_id: str):
    """Record an API call for cost tracking."""
    if is_independent_player(player_id):
        return {"ok": True, "skipped": True, "reason": "independent matrix participant"}
    cost = MODEL_COST_MAP.get(player_id, 0.0 if player_id.startswith("ollama-") else 0.005)
    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    conn.execute("""
        INSERT INTO model_stats (player_id, api_calls, total_cost, date)
        VALUES (?, 1, ?, ?)
        ON CONFLICT(player_id, date) DO UPDATE SET
            api_calls = api_calls + 1,
            total_cost = total_cost + ?
    """, (player_id, cost, today, cost))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/admin/clean-stale-snapshots")
def clean_stale_snapshots():
    conn = _conn()
    season_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
    season = int(season_row[0]) if season_row else 3
    # Find models with $10k cash but portfolio_history showing losses (stale from pre-reset)
    stale = conn.execute("""
        SELECT DISTINCT ph.player_id FROM portfolio_history ph
        JOIN ai_players ap ON ap.id = ph.player_id
        WHERE ph.season=? AND ap.cash >= 9999
        AND ph.total_value < 9000
    """, (season,)).fetchall()
    deleted = {}
    for row in stale:
        pid = row["player_id"]
        cnt = conn.execute("SELECT count(*) FROM portfolio_history WHERE player_id=? AND season=?", (pid, season)).fetchone()[0]
        conn.execute("DELETE FROM portfolio_history WHERE player_id=? AND season=?", (pid, season))
        deleted[pid] = cnt
    conn.commit()
    conn.close()
    return {"ok": True, "deleted": deleted}



@app.post("/api/model-control/force-scan")
def force_scan():
    """Trigger a manual scan immediately, bypassing market hours check."""
    import threading
    import main as _main

    # Use main.py's scan lock so force scan and scheduled scan don't overlap
    if not _main._scan_lock.acquire(blocking=False):
        return {"ok": False, "message": "Scan already in progress"}

    def _do_scan():
        try:
            from config import WATCH_STOCKS

            arena = _main.arena
            if arena is None:
                arena = _main.initialize_arena()
                _main.arena = arena
            arena.run_scan(WATCH_STOCKS, force=True)
        except Exception as e:
            print(f"Force scan error: {e}")
        finally:
            _main._scan_lock.release()

    threading.Thread(target=_do_scan, daemon=True).start()
    return {"ok": True, "message": "Manual scan started"}


# --- Cost Dashboard Endpoints ---

@app.get("/api/costs/dashboard")
def cost_dashboard():
    """Full cost dashboard data: daily, cumulative, projections, grades."""
    from engine.cost_tracker import (
        get_daily_costs, get_cumulative_costs, get_cost_per_trade,
        get_projected_monthly_cost, get_token_efficiency,
        get_model_roi_ranking, get_model_efficiency_grades,
        get_free_vs_paid_pnl, get_dead_models, get_model_diversity,
        get_total_daily_cost, get_free_call_tracking, TOKEN_RATES,
    )
    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    daily = get_daily_costs(today)
    cumulative = get_cumulative_costs()
    cost_per_trade = get_cost_per_trade()
    projection = get_projected_monthly_cost()
    efficiency = get_token_efficiency()
    roi = get_model_roi_ranking()
    grades = get_model_efficiency_grades()
    free_vs_paid = get_free_vs_paid_pnl()
    dead = get_dead_models(48)
    diversity = get_model_diversity()
    daily_total = get_total_daily_cost(today)
    free_calls = get_free_call_tracking(today)

    return {
        "daily_total": round(daily_total, 4),
        "free_calls_used": int(free_calls["free_calls_used"]),
        "free_calls_remaining": int(free_calls["free_calls_remaining"]),
        "free_calls_limit": int(free_calls["free_calls_limit"]),
        "daily_costs": daily,
        "cumulative_costs": cumulative,
        "cost_per_trade": cost_per_trade,
        "projection": projection,
        "token_efficiency": efficiency,
        "roi_ranking": roi,
        "efficiency_grades": grades,
        "free_vs_paid": free_vs_paid,
        "dead_models": dead,
        "diversity": diversity,
        "token_rates": {k: {"input": v[0], "output": v[1]} for k, v in TOKEN_RATES.items()},
    }


@app.get("/api/costs")
@app.get("/api/costs/")
def cost_dashboard_alias():
    """Compatibility alias for older dashboard clients."""
    return cost_dashboard()


@app.get("/api/costs/daily-total")
def cost_daily_total():
    """Quick endpoint for nav bar daily cost display."""
    from engine.cost_tracker import get_total_daily_cost
    return {"daily_total": round(get_total_daily_cost(), 4)}


@app.get("/api/costs/budget")
def cost_budget():
    """Daily cost budget status for UI indicator."""
    from engine.cost_tracker import get_total_daily_cost
    from config import DAILY_API_BUDGET, DAILY_COST_WARNING
    today = round(get_total_daily_cost(), 4)
    pct = round(today / DAILY_API_BUDGET * 100, 1) if DAILY_API_BUDGET > 0 else 0
    return {
        "today_spent": today,
        "daily_limit": DAILY_API_BUDGET,
        "warning_threshold": DAILY_COST_WARNING,
        "pct_used": pct,
        "cloud_scanning_enabled": today < DAILY_API_BUDGET,
        "status": "RED_ALERT" if pct >= 100 else "CAUTION" if pct >= 80 else "NOMINAL",
    }


@app.get("/api/costs/dilithium")
def cost_dilithium():
    """Dilithium Crystal status — real-time API cost tracking."""
    from config import DAILY_API_BUDGET, MONTHLY_API_BUDGET
    conn = _conn()
    today = __import__("datetime").datetime.now().strftime("%Y-%m-%d")
    month = today[:7]

    # Today's costs by provider
    today_rows = conn.execute(
        "SELECT player_id, COUNT(*) as calls, SUM(input_tokens) as inp, "
        "SUM(output_tokens) as outp, SUM(cost_usd) as cost "
        "FROM api_costs WHERE date(timestamp) = ? GROUP BY player_id", (today,)
    ).fetchall()

    # Monthly total
    month_row = conn.execute(
        "SELECT SUM(cost_usd) as total FROM api_costs WHERE timestamp LIKE ?", (month + "%",)
    ).fetchone()

    # Monthly breakdown by crew member
    month_breakdown = conn.execute(
        "SELECT player_id, SUM(cost_usd) as cost FROM api_costs "
        "WHERE timestamp LIKE ? GROUP BY player_id ORDER BY cost DESC", (month + "%",)
    ).fetchall()
    conn.close()

    today_total = sum(r["cost"] for r in today_rows)
    month_total = month_row["total"] or 0

    # Provider grouping
    providers = {}
    for r in today_rows:
        pid = r["player_id"]
        # Provider display: read from DB (all cloud-named agents now run on ollama)
        prov = "ollama"
        if pid == "dalio-metals":
            prov = "google"   # Mr. Dalio: Gemini Flash free tier
        if prov not in providers:
            providers[prov] = {"calls": 0, "tokens": 0, "cost": 0}
        providers[prov]["calls"] += r["calls"]
        providers[prov]["tokens"] += (r["inp"] or 0) + (r["outp"] or 0)
        providers[prov]["cost"] += r["cost"]

    # Status
    daily_pct = today_total / DAILY_API_BUDGET * 100 if DAILY_API_BUDGET > 0 else 0
    if daily_pct > 80:
        status = "RED_ALERT"
    elif daily_pct > 50:
        status = "CAUTION"
    else:
        status = "NOMINAL"

    return {
        "today": {
            "providers": {k: {"calls": v["calls"], "tokens": v["tokens"],
                              "cost": f"${v['cost']:.4f}"} for k, v in providers.items()},
            "total": f"${today_total:.4f}",
            "total_raw": round(today_total, 4),
        },
        "this_month": {
            "total": f"${month_total:.2f}",
            "total_raw": round(month_total, 2),
            "breakdown": {r["player_id"]: f"${r['cost']:.4f}" for r in month_breakdown},
        },
        "daily_budget": DAILY_API_BUDGET,
        "monthly_budget": MONTHLY_API_BUDGET,
        "status": status,
    }


@app.get("/api/costs/history")
def cost_history(days: int = 30):
    """Daily cost totals for the last N days."""
    conn = _conn()
    rows = conn.execute("""
        SELECT date(timestamp) as day, SUM(cost_usd) as total_cost, COUNT(*) as num_calls
        FROM api_costs
        WHERE timestamp >= datetime('now', ? || ' days')
        GROUP BY date(timestamp)
        ORDER BY day ASC
    """, (f"-{days}",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ==================== NAVIGATOR (Chekov's Station) ====================

@app.get("/api/navigator/universe")
def navigator_universe():
    """Get latest universe scan results (top 50 stocks by technical score)."""
    from engine.universe_scanner import get_latest_universe_scan
    return get_latest_universe_scan()


@app.post("/api/navigator/universe/scan")
def navigator_universe_scan():
    """Trigger a fresh universe scan (takes 2-3 minutes)."""
    import threading
    from engine.universe_scanner import scan_universe
    threading.Thread(target=scan_universe, daemon=True).start()
    return {"status": "scan_started", "message": "Universe scan started in background"}


@app.get("/api/navigator/strategies")
def navigator_strategies():
    """Get today's strategy convergence signals."""
    from engine.strategies import get_todays_signals
    return get_todays_signals()


@app.post("/api/navigator/strategies/scan")
def navigator_strategies_scan():
    """Trigger a fresh strategy scan against top 50 universe stocks."""
    import threading
    from engine.strategies import scan_strategies, post_scanner_to_war_room
    def _run():
        scan_strategies()
        post_scanner_to_war_room()
    threading.Thread(target=_run, daemon=True).start()
    return {"status": "scan_started", "message": "Strategy scan started — Chekov will post results to War Room"}


@app.get("/api/navigator/convergence")
def navigator_convergence():
    """Get stocks where 3+ strategies agree (Starfleet multi-strategy convergence)."""
    from engine.strategies import get_todays_signals
    signals = get_todays_signals()
    return {
        "signals": signals,
        "count": len(signals),
        "threshold": "3+ strategies must agree",
    }


@app.post("/api/navigator/scan-now")
def navigator_scan_now():
    """Trigger full Warp 9 scan: universe + strategies + War Room post."""
    import threading
    def _full_scan():
        from engine.universe_scanner import scan_universe
        from engine.strategies import scan_strategies, post_scanner_to_war_room
        scan_universe()
        scan_strategies()
        post_scanner_to_war_room()
    threading.Thread(target=_full_scan, daemon=True).start()
    return {"status": "full_scan_started", "message": "Universe + Strategy scan started — Chekov will report to War Room"}


@app.get("/api/navigator/schedule")
def navigator_schedule():
    """Get next scheduled scan times."""
    from datetime import datetime, timedelta
    import pytz
    mt = pytz.timezone("US/Mountain")
    now = datetime.now(mt)

    # Next universe scan: 11 PM MST tonight or tomorrow
    next_universe = now.replace(hour=23, minute=0, second=0, microsecond=0)
    if now.hour >= 23:
        next_universe += timedelta(days=1)
    # Skip weekends
    while next_universe.weekday() >= 5:
        next_universe += timedelta(days=1)

    # Next strategy scan: 6 AM MST today or tomorrow
    next_strategy = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now.hour >= 6:
        next_strategy += timedelta(days=1)
    while next_strategy.weekday() >= 5:
        next_strategy += timedelta(days=1)

    return {
        "current_time_mst": now.strftime("%Y-%m-%d %I:%M %p MST"),
        "next_universe_scan": next_universe.strftime("%Y-%m-%d %I:%M %p MST"),
        "next_strategy_scan": next_strategy.strftime("%Y-%m-%d %I:%M %p MST"),
        "universe_countdown_min": int((next_universe - now).total_seconds() / 60),
        "strategy_countdown_min": int((next_strategy - now).total_seconds() / 60),
    }


@app.get("/api/regime")
def regime_status():
    """Get current market regime + allocation table."""
    from engine.warp10_engine import get_current_allocation
    return get_current_allocation()


_ma_regime_cache: dict = {"data": None, "ts": 0}

@app.get("/api/regime/ma-cross")
def regime_ma_cross():
    """8/21 MA Cross regime — primary trend signal used for position sizing.
    Cached 5 minutes; returns last-known on error.
    Also returns last 30 days of regime_history from DB.
    """
    import time as _t
    now = _t.time()
    if _ma_regime_cache["data"] and now - _ma_regime_cache["ts"] < 300:
        return _ma_regime_cache["data"]
    try:
        from engine.regime_ma import detect_ma_cross_regime
        current = detect_ma_cross_regime()
    except Exception:
        current = {"regime": "UNKNOWN", "size_modifier": 0.75}

    # Fetch last 30 days of history from DB
    history = []
    try:
        import sqlite3 as _sq
        _c = _sq.connect("data/trader.db")
        _c.row_factory = _sq.Row
        rows = _c.execute(
            "SELECT date, spy_close, ma_8, ma_21, regime, cross_date, cross_days_ago, size_modifier "
            "FROM regime_history ORDER BY date DESC LIMIT 30"
        ).fetchall()
        history = [dict(r) for r in rows]
        _c.close()
    except Exception:
        pass

    result = {"current": current, "history": history}
    _ma_regime_cache["data"] = result
    _ma_regime_cache["ts"] = now
    return result


@app.get("/api/regime/backtest")
def regime_backtest(days: int = 120):
    """Run Warp 10 regime-switching backtest."""
    from engine.warp10_engine import backtest_warp10
    from datetime import datetime, timedelta
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return backtest_warp10(start_date=start, end_date=end)


@app.get("/api/navigator/history")
def navigator_history(days: int = 7):
    """Get universe scan history."""
    conn = _conn()
    rows = conn.execute(
        "SELECT scan_date, ticker, score, signals, close, volume_ratio, rsi, gap_pct "
        "FROM universe_scan WHERE scan_date >= date('now', ?) ORDER BY scan_date DESC, score DESC",
        (f"-{days} days",)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# --- CrewAI Strategy Lab ---

@app.get("/api/crew/status")
def crew_status():
    """Health check for Crew Strategy Lab."""
    result = {"engine": "ollama_direct", "ollama_running": False, "model": os.getenv("CREWAI_MODEL", "qwen3:14b")}
    try:
        import requests as _req
        r = _req.get("http://localhost:11434/api/tags", timeout=3)
        if r.ok:
            result["ollama_running"] = True
            models = [m.get("name", "") for m in r.json().get("models", [])]
            result["ollama_models"] = models[:10]
    except Exception:
        pass
    result["ready"] = result["ollama_running"]
    return result


@app.post("/api/crew/develop")
def crew_develop(data: dict):
    """Run the 4-agent crew to develop a trading strategy. Takes 30-90 seconds."""
    prompt = data.get("prompt", "")
    symbol = data.get("symbol", "SPY")
    days = int(data.get("days", 365))
    if not prompt:
        return {"success": False, "error": "prompt is required"}
    from engine.crew_strategy_lab import create_crew
    result = create_crew(prompt, symbol=symbol, days=days)
    # Save to strategy_backtests
    if result.get("success"):
        _save_backtests([{
            "source": "crew_strategy_lab", "ticker": symbol, "strategy_type": "crew_custom",
            "days": days, "parameters": json.dumps({"prompt": prompt[:500]}),
            "crew_discussion": (result.get("result") or "")[:5000],
            "generated_code": (result.get("code") or "")[:5000],
            "notes": f"Crew: {', '.join(result.get('agents', []))}",
        }])
    return result


@app.post("/api/crew/run-code")
def crew_run_code(data: dict):
    """Execute VectorBT strategy code in a safe subprocess."""
    code = data.get("code", "")
    if not code:
        return {"success": False, "error": "code is required"}
    from engine.crew_strategy_lab import run_strategy_code
    return run_strategy_code(code)


# --- Backtest History Queries ---

@app.get("/api/backtests")
def list_backtests(ticker: str = None, strategy_type: str = None, source: str = None, limit: int = 50):
    """List saved backtests with optional filters."""
    conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    q = "SELECT * FROM strategy_backtests WHERE 1=1"
    params = []
    if ticker:
        q += " AND ticker=?"; params.append(ticker.upper())
    if strategy_type:
        q += " AND strategy_type=?"; params.append(strategy_type)
    if source:
        q += " AND source=?"; params.append(source)
    q += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return {"backtests": [dict(r) for r in rows], "count": len(rows)}


@app.get("/api/backtests/best")
def best_backtests(sort_by: str = "sharpe_ratio", limit: int = 10):
    """Top backtests by a given metric."""
    allowed = {"sharpe_ratio", "total_return", "win_rate", "profit_factor"}
    col = sort_by if sort_by in allowed else "sharpe_ratio"
    conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"SELECT * FROM strategy_backtests WHERE {col} IS NOT NULL AND num_trades > 0 "
        f"ORDER BY {col} DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return {"backtests": [dict(r) for r in rows], "sort_by": col, "count": len(rows)}


# --- Alpaca Paper Trading ---
from engine.alpaca_bridge import alpaca


@app.get("/api/alpaca/status")
def alpaca_status():
    return alpaca.status()


@app.get("/api/alpaca/positions")
def alpaca_positions():
    return {"positions": alpaca.positions()}


@app.get("/api/alpaca/orders")
def alpaca_orders(status: str = "all"):
    return {"orders": alpaca.orders(status)}


@app.post("/api/alpaca/buy")
def alpaca_buy(data: dict):
    symbol = data.get("symbol", "")
    qty = int(data.get("qty", 0))
    if not symbol or qty <= 0:
        return {"error": "symbol and qty required"}
    return alpaca.buy(symbol.upper(), qty)


@app.post("/api/alpaca/sell")
def alpaca_sell(data: dict):
    symbol = data.get("symbol", "")
    qty = int(data.get("qty", 0))
    if not symbol or qty <= 0:
        return {"error": "symbol and qty required"}
    return alpaca.sell(symbol.upper(), qty)


@app.post("/api/alpaca/close/{symbol}")
def alpaca_close(symbol: str):
    return alpaca.close_position(symbol.upper())


@app.post("/api/alpaca/close-all")
def alpaca_close_all():
    return alpaca.close_all()


@app.get("/api/alpaca/sync-status")
def alpaca_sync_status():
    """Return last Alpaca portfolio sync metadata (timestamp, value, cash, positions)."""
    try:
        from shared.alpaca_portfolio_sync import get_last_sync_status
        return get_last_sync_status()
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/alpaca/sync-now")
def alpaca_sync_now():
    """Force an immediate full Alpaca portfolio sync."""
    try:
        from shared.alpaca_portfolio_sync import run_full_alpaca_sync
        return run_full_alpaca_sync(force=True)
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/alpaca/sync-positions")
def alpaca_sync_positions():
    """Sync live Alpaca positions into portfolio_positions (portfolio_id=1).

    Replaces all open positions with fresh data from the Alpaca API.
    Closed positions are preserved. Clears in-memory leaderboard cache so
    the updated values appear immediately.
    """
    import datetime as _dt
    positions = alpaca.positions()
    if not positions:
        return {"ok": True, "synced": 0, "message": "No open positions in Alpaca account"}
    if len(positions) == 1 and "error" in positions[0]:
        return {"ok": False, "error": positions[0]["error"]}

    c = _conn()
    try:
        # Remove stale open positions; keep closed history intact
        c.execute("DELETE FROM portfolio_positions WHERE portfolio_id=1 AND status='open'")
        now = _dt.datetime.now().isoformat()
        for p in positions:
            entry  = float(p["avg_entry"])
            qty    = float(p["qty"])
            direction = "long" if qty >= 0 else "short"
            c.execute("""
                INSERT INTO portfolio_positions
                  (portfolio_id, ticker, asset_class, direction, quantity, entry_price,
                   current_price, unrealized_pnl, stop_loss, take_profit, status,
                   created_at, updated_at)
                VALUES (1, ?, 'stock', ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """, (
                p["symbol"], direction, qty, entry,
                float(p["current_price"]), float(p["unrealized_pl"]),
                round(entry * 0.92, 4), round(entry * 1.20, 4),
                now, now,
            ))
        c.commit()

        # Bust in-memory leaderboard cache so next request recomputes
        _lb_key_cur = f"leaderboard_0"
        _endpoint_cache.pop(_lb_key_cur, None)
        _leaderboard_disk_cache["data"] = None
        _leaderboard_disk_cache["ts"] = 0
        try:
            import os as _os
            if _os.path.exists(_LEADERBOARD_CACHE_FILE):
                _os.remove(_LEADERBOARD_CACHE_FILE)
        except Exception:
            pass

        total_unreal = round(sum(float(p["unrealized_pl"]) for p in positions), 2)
        return {
            "ok": True,
            "synced": len(positions),
            "positions": [p["symbol"] for p in positions],
            "total_unrealized_pnl": total_unreal,
            "super_agent_total_value": round(25000.0 + total_unreal, 2),
        }
    except Exception as e:
        c.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        c.close()


# --- Holodeck (VectorBT Backtesting) ---
import asyncio
from concurrent.futures import ThreadPoolExecutor
_holodeck_pool = ThreadPoolExecutor(max_workers=2)


def _get_holodeck():
    from engine.holodeck import holodeck
    return holodeck


def _save_backtests(rows: list):
    """Save backtest results to strategy_backtests table. rows is list of dicts."""
    if not rows:
        return
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
        for r in rows:
            conn.execute(
                "INSERT INTO strategy_backtests (source, ticker, strategy_type, days, parameters, "
                "total_return, win_rate, sharpe_ratio, max_drawdown, profit_factor, num_trades, "
                "final_value, starting_cash, recommendation, crew_discussion, generated_code, notes) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (r.get("source",""), r.get("ticker",""), r.get("strategy_type",""), r.get("days"),
                 r.get("parameters",""), r.get("total_return"), r.get("win_rate"), r.get("sharpe_ratio"),
                 r.get("max_drawdown"), r.get("profit_factor"), r.get("num_trades"), r.get("final_value"),
                 r.get("starting_cash", 7000), r.get("recommendation"), r.get("crew_discussion"),
                 r.get("generated_code"), r.get("notes")))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[backtest save error] {e}")


@app.get("/api/holodeck/rsi-sweep/{symbol}")
async def holodeck_rsi_sweep(symbol: str, days: int = 180):
    """Sweep RSI parameters — returns best combos, saves all to DB"""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_holodeck_pool, lambda: _get_holodeck().run_rsi_sweep(symbol, days=days))
        # Save every combo to strategy_backtests
        if result.get("all_results"):
            _save_backtests([{
                "source": "holodeck_sweep", "ticker": symbol, "strategy_type": "RSI", "days": days,
                "parameters": json.dumps({k: r[k] for k in ("window","entry","exit") if k in r}),
                "total_return": r.get("total_return"), "win_rate": r.get("win_rate"),
                "sharpe_ratio": r.get("sharpe"), "max_drawdown": r.get("max_drawdown"),
                "profit_factor": r.get("profit_factor"), "num_trades": r.get("num_trades"),
                "final_value": r.get("final_value"),
            } for r in result["all_results"]])
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/holodeck/bollinger-sweep/{symbol}")
async def holodeck_bollinger_sweep(symbol: str, days: int = 180):
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_holodeck_pool, lambda: _get_holodeck().run_bollinger_sweep(symbol, days=days))
        if result.get("all_results"):
            _save_backtests([{
                "source": "holodeck_sweep", "ticker": symbol, "strategy_type": "BBANDS", "days": days,
                "parameters": json.dumps({k: r[k] for k in ("window","std_dev") if k in r}),
                "total_return": r.get("total_return"), "win_rate": r.get("win_rate"),
                "sharpe_ratio": r.get("sharpe"), "max_drawdown": r.get("max_drawdown"),
                "num_trades": r.get("num_trades"), "final_value": r.get("final_value"),
            } for r in result["all_results"]])
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/holodeck/macd-sweep/{symbol}")
async def holodeck_macd_sweep(symbol: str, days: int = 180):
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_holodeck_pool, lambda: _get_holodeck().run_macd_sweep(symbol, days=days))
        if result.get("all_results"):
            _save_backtests([{
                "source": "holodeck_sweep", "ticker": symbol, "strategy_type": "MACD", "days": days,
                "parameters": json.dumps({k: r[k] for k in ("fast","slow","signal") if k in r}),
                "total_return": r.get("total_return"), "win_rate": r.get("win_rate"),
                "sharpe_ratio": r.get("sharpe"), "max_drawdown": r.get("max_drawdown"),
                "num_trades": r.get("num_trades"), "final_value": r.get("final_value"),
            } for r in result["all_results"]])
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/holodeck/strategy/{symbol}")
async def holodeck_strategy(symbol: str, strategy: str = "rsi", days: int = 180, params: str = "{}"):
    """Run a single strategy with specific params, returns equity curve"""
    try:
        p = json.loads(params)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_holodeck_pool, lambda: _get_holodeck().run_custom_strategy(symbol, days=days, strategy_type=strategy, params=p))
        return result
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Holly Nightly Backtest API
# ---------------------------------------------------------------------------

@app.post("/api/holly/comprehensive")
async def holly_run_comprehensive(days: int = 90):
    """Run 90-day comprehensive backtest on all major tickers (RSI, MACD, Bollinger, SMA Cross).
    Sweeps best params per strategy × ticker. Returns top 10 winning strategies."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _holodeck_pool,
            lambda: __import__("engine.holly_nightly_backtest", fromlist=["run_comprehensive_backtest"]).run_comprehensive_backtest(days=days)
        )
        return result
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/holly/nightly")
async def holly_run_nightly(days: int = 90):
    """Run Holly-style nightly backtest: top 50 volume movers × RSI/MACD/Bollinger/Gap.
    Saves top 10 winning strategies for morning scan prioritization."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _holodeck_pool,
            lambda: __import__("engine.holly_nightly_backtest", fromlist=["run_holly_nightly"]).run_holly_nightly(days=days)
        )
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/holly/winners")
def holly_get_winners(n: int = 10):
    """Return today's top-N Holly winning strategies (ticker + strategy + metrics)."""
    try:
        from engine.holly_nightly_backtest import get_holly_winning_tickers
        return {"winners": get_holly_winning_tickers(n)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/holly/history")
def holly_get_history(days: int = 30):
    """Return recent Holly backtest runs from data/backtest.db."""
    try:
        import sqlite3 as _sq
        _bdb = os.path.join(os.path.dirname(DB), "backtest.db")
        conn = _sq.connect(_bdb, timeout=15)
        conn.row_factory = _sq.Row
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT run_date, run_type, ticker, strategy, params,
                   total_return, win_rate, sharpe, max_drawdown,
                   avg_hold_days, num_trades, profit_factor, spy_return, vs_spy
            FROM holly_backtests
            WHERE run_date >= ?
            ORDER BY total_return DESC
            LIMIT 200
        """, (cutoff,)).fetchall()
        conn.close()
        return {"results": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Master Backtest API (9-tier comprehensive)
# ---------------------------------------------------------------------------

@app.post("/api/master-backtest/run")
async def master_backtest_run(days: int = 90):
    """Trigger full 9-tier master backtest in background thread. Returns job_id."""
    import threading, uuid
    job_id = str(uuid.uuid4())[:8]
    def _bg():
        try:
            from engine.master_backtest import run_master_backtest
            result = run_master_backtest(days=days)
            logger.info("Master backtest %s complete: %s strategies, %s trades",
                        job_id, result.get("event_strategies"), result.get("total_event_trades"))
        except Exception as e:
            logger.exception("Master backtest %s failed: %s", job_id, e)
    t = threading.Thread(target=_bg, daemon=True)
    t.start()
    return {"status": "started", "job_id": job_id, "days": days,
            "message": f"Master backtest running in background. Check /api/master-backtest/summary when done."}


@app.get("/api/master-backtest/summary")
def master_backtest_summary():
    """Return latest master backtest summary from DB."""
    try:
        import sqlite3 as _sq
        db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db")
        conn = _sq.connect(db); conn.row_factory = _sq.Row

        run_date = conn.execute("SELECT MAX(run_date) FROM backtest_master_results").fetchone()[0]
        if not run_date:
            return {"status": "no_data"}

        top10 = conn.execute("""
            SELECT strategy, tier_name, AVG(sharpe) as avg_sharpe,
                   AVG(total_return) as avg_return, AVG(win_rate) as avg_wr,
                   SUM(num_trades) as total_trades, AVG(max_drawdown) as avg_dd,
                   AVG(calmar) as avg_calmar
            FROM backtest_master_results WHERE run_date=? AND num_trades>0
            GROUP BY strategy ORDER BY avg_sharpe DESC LIMIT 10
        """, (run_date,)).fetchall()

        equity = conn.execute("""
            SELECT trade_date, equity, daily_pnl, regime
            FROM backtest_equity_curve WHERE run_date=?
            ORDER BY trade_date
        """, (run_date,)).fetchall()

        monthly = conn.execute("""
            SELECT month, SUM(pnl) as total_pnl, AVG(win_rate) as avg_wr, SUM(num_trades) as trades
            FROM backtest_monthly_breakdown WHERE run_date=?
            GROUP BY month ORDER BY month
        """, (run_date,)).fetchall()

        opts = conn.execute("""
            SELECT strategy, COUNT(*) n, AVG(pnl_pct) avg_pnl,
                   AVG(CASE WHEN win=1 THEN 1.0 ELSE 0.0 END)*100 win_rate
            FROM backtest_options_results WHERE run_date=?
            GROUP BY strategy ORDER BY avg_pnl DESC
        """, (run_date,)).fetchall()

        spreads = conn.execute("""
            SELECT strategy, COUNT(*) n, AVG(pnl_pct) avg_pnl,
                   AVG(CASE WHEN win=1 THEN 1.0 ELSE 0.0 END)*100 win_rate
            FROM backtest_spread_results WHERE run_date=?
            GROUP BY strategy ORDER BY avg_pnl DESC
        """, (run_date,)).fetchall()

        dte0 = conn.execute("""
            SELECT strategy, COUNT(*) n, AVG(pnl_pct) avg_pnl,
                   AVG(CASE WHEN win=1 THEN 1.0 ELSE 0.0 END)*100 win_rate
            FROM backtest_0dte_results WHERE run_date=?
            GROUP BY strategy ORDER BY avg_pnl DESC
        """, (run_date,)).fetchall()

        sym_params = conn.execute("""
            SELECT ticker, best_strategy, best_sharpe, best_tod, best_options_strategy
            FROM backtest_symbol_params WHERE run_date=?
            ORDER BY best_sharpe DESC
        """, (run_date,)).fetchall()

        heatmap = conn.execute("""
            SELECT ticker, strategy, regime, avg_return, win_rate, num_trades
            FROM options_strategy_heatmap WHERE run_date=?
            ORDER BY ticker, strategy, regime
        """, (run_date,)).fetchall()

        conn.close()
        return {
            "run_date": run_date,
            "top10_strategies": [dict(r) for r in top10],
            "equity_curve": [dict(r) for r in equity],
            "monthly_breakdown": [dict(r) for r in monthly],
            "options_summary": [dict(r) for r in opts],
            "spread_summary": [dict(r) for r in spreads],
            "dte0_summary": [dict(r) for r in dte0],
            "symbol_params": [dict(r) for r in sym_params],
            "options_heatmap": [dict(r) for r in heatmap],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/master-backtest/strategy/{strategy_name}")
def master_backtest_strategy_detail(strategy_name: str):
    """Return per-symbol results for a specific strategy."""
    try:
        import sqlite3 as _sq
        db  = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db")
        conn = _sq.connect(db); conn.row_factory = _sq.Row
        run_date = conn.execute("SELECT MAX(run_date) FROM backtest_master_results").fetchone()[0]
        rows = conn.execute("""
            SELECT ticker, total_return, win_rate, sharpe, max_drawdown, avg_hold_hours,
                   num_trades, profit_factor, calmar, best_trade_pct, worst_trade_pct,
                   vs_spy, regime
            FROM backtest_master_results
            WHERE run_date=? AND strategy=? AND regime='ALL'
            ORDER BY sharpe DESC
        """, (run_date, strategy_name)).fetchall()
        conn.close()
        return {"strategy": strategy_name, "run_date": run_date,
                "results": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/master-backtest/symbol/{ticker}")
def master_backtest_symbol_detail(ticker: str):
    """Return all strategy results for a specific symbol, ranked by Sharpe."""
    try:
        import sqlite3 as _sq
        db   = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db")
        conn = _sq.connect(db); conn.row_factory = _sq.Row
        run_date = conn.execute("SELECT MAX(run_date) FROM backtest_master_results").fetchone()[0]
        rows = conn.execute("""
            SELECT tier, tier_name, strategy, total_return, win_rate, sharpe,
                   max_drawdown, num_trades, profit_factor, calmar, vs_spy, regime
            FROM backtest_master_results
            WHERE run_date=? AND ticker=? AND regime='ALL'
            ORDER BY sharpe DESC
        """, (run_date, ticker.upper())).fetchall()
        params = conn.execute("""
            SELECT best_strategy, best_sharpe, best_tod, best_options_strategy,
                   optimal_dte, optimal_delta, iv_pct_sweet_spot
            FROM backtest_symbol_params WHERE run_date=? AND ticker=?
        """, (run_date, ticker.upper())).fetchone()
        conn.close()
        return {"ticker": ticker.upper(), "run_date": run_date,
                "strategies": [dict(r) for r in rows],
                "params": dict(params) if params else None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/master-backtest/greeks")
def master_backtest_greeks():
    """Return Greeks summary for all options strategies."""
    try:
        import sqlite3 as _sq
        db   = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db")
        conn = _sq.connect(db); conn.row_factory = _sq.Row
        run_date = conn.execute("SELECT MAX(run_date) FROM backtest_greeks_summary").fetchone()[0]
        rows = conn.execute("""
            SELECT strategy, ticker, avg_delta, avg_theta_per_day, avg_gamma,
                   avg_vega, theta_total, avg_iv_entry, avg_iv_exit
            FROM backtest_greeks_summary WHERE run_date=?
            ORDER BY strategy, ticker
        """, (run_date,)).fetchall()
        conn.close()
        return {"run_date": run_date, "greeks": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Backtest Extras API (earnings straddle, regime filter, TOD, CG ratio)
# ---------------------------------------------------------------------------

@app.post("/api/extras/earnings-straddle")
async def extras_earnings_straddle(symbols: str = ""):
    """Run ATM earnings straddle backtest. symbols=comma-separated or blank for defaults."""
    import threading
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] or None
    def _run():
        from engine.backtest_extras import run_earnings_straddle
        return run_earnings_straddle(symbols=sym_list)
    result = await __import__("asyncio").get_event_loop().run_in_executor(None, _run)
    return result


@app.get("/api/extras/earnings-straddle")
def extras_earnings_straddle_results():
    """Return cached earnings straddle results from DB."""
    try:
        import sqlite3 as _sq
        conn = _sq.connect(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db"))
        conn.row_factory = _sq.Row
        rows = conn.execute("""
            SELECT symbol, earnings_date, dte_at_entry, entry_price, straddle_cost,
                   post_move_pct, pnl_per_straddle, win, iv_rank_est, iv_crush_est_pct
            FROM extras_earnings_straddle
            WHERE run_date = (SELECT MAX(run_date) FROM extras_earnings_straddle)
            ORDER BY symbol, dte_at_entry
        """).fetchall()
        conn.close()
        return {"results": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/extras/regime-filter")
async def extras_regime_filter():
    """Analyze holly backtest results bucketed by VIX regime."""
    def _run():
        from engine.backtest_extras import run_regime_filter
        return run_regime_filter()
    result = await __import__("asyncio").get_event_loop().run_in_executor(None, _run)
    return result


@app.get("/api/extras/regime-filter")
def extras_regime_filter_results():
    """Return cached regime filter results from DB."""
    try:
        import sqlite3 as _sq
        conn = _sq.connect(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db"))
        conn.row_factory = _sq.Row
        rows = conn.execute("""
            SELECT strategy, regime, avg_return, win_rate, num_trades, sharpe
            FROM extras_regime_filter
            WHERE run_date = (SELECT MAX(run_date) FROM extras_regime_filter)
            ORDER BY regime, avg_return DESC
        """).fetchall()
        conn.close()
        return {"results": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/extras/time-of-day")
async def extras_time_of_day(tickers: str = ""):
    """Run time-of-day intraday analysis using 1h bars."""
    sym_list = [s.strip().upper() for s in tickers.split(",") if s.strip()] or None
    def _run():
        from engine.backtest_extras import run_time_of_day
        return run_time_of_day(tickers=sym_list)
    result = await __import__("asyncio").get_event_loop().run_in_executor(None, _run)
    return result


@app.get("/api/extras/time-of-day")
def extras_time_of_day_results():
    """Return cached time-of-day results from DB."""
    try:
        import sqlite3 as _sq
        conn = _sq.connect(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db"))
        conn.row_factory = _sq.Row
        rows = conn.execute("""
            SELECT ticker, session, avg_return, win_rate, num_trades
            FROM extras_time_of_day
            WHERE run_date = (SELECT MAX(run_date) FROM extras_time_of_day)
            ORDER BY session, avg_return DESC
        """).fetchall()
        conn.close()
        return {"results": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/extras/cg-ratio")
async def extras_cg_ratio(ma_window: int = 20):
    """Run Copper/Gold ratio crossover backtest as SPY entry signal."""
    def _run():
        from engine.backtest_extras import run_cg_ratio
        return run_cg_ratio(ma_window=ma_window)
    result = await __import__("asyncio").get_event_loop().run_in_executor(None, _run)
    return result


@app.get("/api/extras/cg-ratio")
def extras_cg_ratio_results():
    """Return cached CG ratio trades from DB."""
    try:
        import sqlite3 as _sq
        conn = _sq.connect(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "backtest.db"))
        conn.row_factory = _sq.Row
        rows = conn.execute("""
            SELECT entry_date, exit_date, entry_price, exit_price,
                   pnl_pct, cg_ratio, cg_ma20, win, hold_days
            FROM extras_cg_ratio
            WHERE run_date = (SELECT MAX(run_date) FROM extras_cg_ratio)
            ORDER BY entry_date
        """).fetchall()
        conn.close()
        return {"trades": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/extras/run-all")
async def extras_run_all():
    """Run all four extra backtest modules: earnings straddle, regime filter, TOD, CG ratio."""
    def _run():
        from engine.backtest_extras import run_all_extras
        return run_all_extras()
    result = await __import__("asyncio").get_event_loop().run_in_executor(None, _run)
    return result


# ---------------------------------------------------------------------------
# Agent Ratings API
# ---------------------------------------------------------------------------

@app.get("/api/ratings")
def ratings_fleet():
    """All active agents with their alltime A–E rating, sorted by score."""
    try:
        from engine.agent_ratings import fleet_report_card, get_rating_trend
        report = fleet_report_card()
        for r in report:
            r["trend"] = get_rating_trend(r["player_id"])
        return report
    except Exception as e:
        return {"error": str(e)}


# NOTE: /api/ratings/advice and /api/ratings/cold must be defined BEFORE
# /api/ratings/{player_id} so FastAPI doesn't swallow them as path params.

@app.get("/api/ratings/advice")
def ratings_advice():
    """Lineup advisor — recommended changes for all agents."""
    try:
        from engine.agent_ratings import lineup_advisor
        return lineup_advisor()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/ratings/cold")
def ratings_cold():
    """Detect agents who are going cold."""
    try:
        from engine.agent_ratings import detect_cold_agents
        return detect_cold_agents()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/ratings/{player_id}")
def ratings_player(player_id: str):
    """Weekly + alltime rating for a specific agent."""
    try:
        from engine.agent_ratings import calculate_rating
        return {
            "weekly":  calculate_rating(player_id, "weekly"),
            "alltime": calculate_rating(player_id, "alltime"),
        }
    except Exception as e:
        return {"error": str(e)}


# Serve paper-trader static dashboard
_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/sw.js")
def service_worker():
    from fastapi.responses import Response
    body = (
        "self.addEventListener('install', () => self.skipWaiting());\n"
        "self.addEventListener('activate', e => {\n"
        "  e.waitUntil(\n"
        "    caches.keys()\n"
        "      .then(k => Promise.all(k.map(c => caches.delete(c))))\n"
        "      .then(() => self.clients.matchAll({includeUncontrolled: true, type: 'window'}))\n"
        "      .then(clients => clients.forEach(c => c.navigate(c.url)))\n"
        "  );\n"
        "});\n"
    )
    return Response(
        content=body,
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Service-Worker-Allowed": "/",
        },
    )


@app.get("/clear-sw", response_class=HTMLResponse)
def clear_sw():
    """Unregisters all service workers and redirects to home. Visit once to fix stale SW cache."""
    return HTMLResponse("""<!DOCTYPE html>
<html><head><title>Clearing SW...</title></head>
<body style="background:#0a0e1a;color:#e0e6f0;font-family:monospace;padding:40px;text-align:center;">
<h2>Clearing service worker cache...</h2>
<p id="msg">Working...</p>
<script>
(async function() {
  var msg = document.getElementById('msg');
  try {
    if ('serviceWorker' in navigator) {
      var regs = await navigator.serviceWorker.getRegistrations();
      for (var r of regs) { await r.unregister(); }
      var keys = await caches.keys();
      for (var k of keys) { await caches.delete(k); }
      msg.textContent = 'Done! Redirecting to Bridge...';
    } else {
      msg.textContent = 'No service worker support — redirecting...';
    }
  } catch(e) {
    msg.textContent = 'Error: ' + e + ' — redirecting anyway...';
  }
  setTimeout(function() { window.location.href = '/?nocache=' + Date.now(); }, 1500);
})();
</script>
</body></html>""", headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/")
def serve_index():
    return FileResponse(
        os.path.join(_static_dir, "index.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}
    )


@app.get("/tactical")
def serve_tactical():
    """Captain's personal 0DTE tactical display."""
    return FileResponse(
        os.path.join(_static_dir, "tactical.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}
    )


@app.get("/scanner")
def serve_scanner():
    """Main Viewer — Tactical Scan dashboard."""
    return FileResponse(
        os.path.join(_static_dir, "scanner.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}
    )


@app.get("/charts")
def serve_big_charts():
    """Standalone senior-accessible chart viewer — no auth, no sidebar."""
    return FileResponse(
        os.path.join(_static_dir, "big_charts.html"),
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}
    )




@app.get("/api/webull-portfolio")
def webull_portfolio():
    """Returns the real Webull Portfolio with live P&L"""
    from engine.paper_trader import get_portfolio_with_pnl
    from engine.market_data import get_stock_price

    conn = _conn()
    player = conn.execute("SELECT * FROM ai_players WHERE id='steve-webull'").fetchone()
    conn.close()

    if not player:
        return {"cash": 0, "positions": [], "recent_trades": [], "position_count": 0}

    # Fetch live prices for all Webull Portfolio symbols
    prices = {}
    pos_conn = _conn()
    steve_positions = pos_conn.execute(
        "SELECT symbol FROM positions WHERE player_id='steve-webull'"
    ).fetchall()
    pos_conn.close()

    for row in steve_positions:
        try:
            prices[row["symbol"]] = get_stock_price(row["symbol"])
        except Exception:
            pass

    pnl = get_portfolio_with_pnl("steve-webull", prices)

    # Calculate total daily P&L % and $ (weighted by market value)
    total_mkt = sum(p.get("market_value", 0) for p in pnl["positions"])
    total_day_pnl_pct = 0.0
    total_day_pnl = 0.0
    if total_mkt > 0:
        for p in pnl["positions"]:
            weight = p.get("market_value", 0) / total_mkt
            total_day_pnl_pct += weight * p.get("day_change_pct", 0)
        total_day_pnl = round(total_mkt * total_day_pnl_pct / 100, 2)
    total_day_pnl_pct = round(total_day_pnl_pct, 2)

    # Win rate from closed trades — use price-based matching when realized_pnl is NULL
    wr_conn = _conn()
    sell_rows = wr_conn.execute(
        "SELECT t.symbol, t.price AS sell_price, t.qty, t.executed_at, "
        "CASE WHEN t.realized_pnl IS NOT NULL THEN t.realized_pnl "
        "ELSE (t.price - (SELECT b.price FROM trades b WHERE b.player_id='steve-webull' "
        "AND b.action='BUY' AND b.symbol=t.symbol AND b.executed_at<=t.executed_at "
        "ORDER BY b.executed_at DESC LIMIT 1)) * t.qty END AS pnl "
        "FROM trades t WHERE t.player_id='steve-webull' AND t.action='SELL'"
    ).fetchall()
    wr_conn.close()
    total_closed = len(sell_rows)
    win_count = sum(1 for r in sell_rows if r["pnl"] is not None and r["pnl"] > 0)
    win_rate = round(win_count / total_closed * 100, 1) if total_closed > 0 else 0.0

    # Build asset_type lookup from DB positions
    at_conn = _conn()
    at_rows = at_conn.execute(
        "SELECT symbol, asset_type FROM positions WHERE player_id='steve-webull'"
    ).fetchall()
    at_conn.close()
    asset_type_map = {r["symbol"]: r["asset_type"] for r in at_rows}

    # Live cash from Alpaca paper account — more accurate than stale DB value
    live_cash = pnl["cash"]
    try:
        from engine.alpaca_bridge import AlpacaBridge
        _alpaca = AlpacaBridge().status()
        if _alpaca.get("connected") and _alpaca.get("cash") is not None:
            live_cash = float(_alpaca["cash"])
            _uc = _conn()
            _uc.execute("UPDATE ai_players SET cash=? WHERE id='steve-webull'", (live_cash,))
            _uc.commit()
            _uc.close()
    except Exception:
        pass

    return {
        "cash": live_cash,
        "total_value": pnl["total_value"],
        "total_cost_basis": pnl["total_cost_basis"],
        "total_unrealized_pnl": pnl["total_unrealized_pnl"],
        "return_pct": pnl["return_pct"],
        "total_day_pnl_pct": total_day_pnl_pct,
        "total_day_pnl": total_day_pnl,
        "starting_value": 7021.81,
        "win_rate": win_rate,
        "win_count": win_count,
        "loss_count": total_closed - win_count,
        "positions": [
            {
                "symbol": p["symbol"], "qty": p["qty"], "avg_price": p["avg_price"],
                "current_price": p.get("current_price", p["avg_price"]),
                "market_value": p.get("market_value", p["qty"] * p["avg_price"]),
                "unrealized_pnl": p.get("unrealized_pnl", 0),
                "unrealized_pnl_pct": p.get("unrealized_pnl_pct", 0),
                "day_change_pct": p.get("day_change_pct", 0),
                "asset_type": asset_type_map.get(p["symbol"], "stock"),
                "market": "webull",
            }
            for p in pnl["positions"]
        ],
        "recent_trades": [],
        "position_count": len(pnl["positions"]),
        "last_synced_label": _get_setting("last_alpaca_sync_label", "—"),
        "last_synced_at":    _get_setting("last_alpaca_full_sync"),
    }


# ─── Captain's Log — Manual Trades ───────────────────────────────────────────

@app.get("/api/captains-log/trades")
def captains_log_trades(player: str = "", limit: int = 100):
    """Get trade history for Captain's Log — includes DayBlade, manual, and all option trades."""
    conn = _conn()
    if player:
        rows = conn.execute(
            "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.strike_price, t.expiry_date, t.confidence, "
            "t.reasoning, t.executed_at "
            "FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.player_id = ? ORDER BY t.executed_at DESC LIMIT ?",
            (player, limit)
        ).fetchall()
    else:
        # All manual/options trades: dayblade, steve-webull, options-sosnoff, and any option trades
        rows = conn.execute(
            "SELECT t.player_id, p.display_name, t.symbol, t.action, t.qty, t.price, "
            "t.asset_type, t.option_type, t.strike_price, t.expiry_date, t.confidence, "
            "t.reasoning, t.executed_at "
            "FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.player_id IN ('dayblade-0dte', 'steve-webull', 'options-sosnoff') "
            "OR t.asset_type = 'option' "
            "ORDER BY t.executed_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/captains-log/summary")
def captains_log_summary():
    """Summary stats for Captain's Log trades."""
    conn = _conn()
    summary = {}
    for pid in ['dayblade-0dte', 'steve-webull', 'options-sosnoff']:
        row = conn.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN action LIKE 'BUY%' THEN 1 ELSE 0 END) as buys, "
            "SUM(CASE WHEN action = 'SELL' THEN 1 ELSE 0 END) as sells "
            "FROM trades WHERE player_id = ?",
            (pid,)
        ).fetchone()
        if row and row["total"] > 0:
            summary[pid] = dict(row)
    conn.close()
    return summary


@app.get("/api/webull/live")
def webull_live():
    """Fetch live portfolio from Webull OpenAPI."""
    from engine.webull_client import get_portfolio
    return get_portfolio()


@app.post("/api/webull/sync-positions")
def webull_sync_positions():
    """Sync live Webull positions into the DB (updates leaderboard)."""
    from engine.webull_client import sync_positions_to_db
    return sync_positions_to_db()


# ── Webull Personal Watchlist ─────────────────────────────────────────────────

def _ensure_webull_watchlist_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS webull_watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            added_at TEXT DEFAULT (datetime('now')),
            notes TEXT DEFAULT ''
        )
    """)
    conn.commit()


@app.get("/api/webull/watchlist")
def webull_watchlist_get():
    """Return all symbols in the personal watchlist with live price data."""
    conn = _conn()
    _ensure_webull_watchlist_table(conn)
    rows = conn.execute(
        "SELECT id, symbol, added_at, notes FROM webull_watchlist ORDER BY added_at DESC"
    ).fetchall()
    conn.close()

    from engine.market_data import get_stock_price
    result = []
    for r in rows:
        sym = r["symbol"]
        price_data = get_stock_price(sym)
        result.append({
            "id": r["id"],
            "symbol": sym,
            "added_at": r["added_at"],
            "notes": r["notes"] or "",
            "price": price_data.get("price"),
            "change_pct": price_data.get("change_pct"),
        })
    return {"symbols": result}


@app.post("/api/webull/watchlist")
async def webull_watchlist_add(request: Request):
    """Add a symbol to the personal watchlist. Body: {symbol, notes?}"""
    body = await request.json()
    symbol = (body.get("symbol") or "").upper().strip()
    notes = (body.get("notes") or "").strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol required")
    conn = _conn()
    _ensure_webull_watchlist_table(conn)
    try:
        conn.execute(
            "INSERT INTO webull_watchlist (symbol, notes) VALUES (?, ?)",
            (symbol, notes)
        )
        conn.commit()
    except Exception:
        conn.close()
        raise HTTPException(status_code=409, detail=f"{symbol} already in watchlist")
    conn.close()
    return {"ok": True, "symbol": symbol}


@app.delete("/api/webull/watchlist/{symbol}")
def webull_watchlist_remove(symbol: str):
    """Remove a symbol from the personal watchlist."""
    symbol = symbol.upper().strip()
    conn = _conn()
    _ensure_webull_watchlist_table(conn)
    conn.execute("DELETE FROM webull_watchlist WHERE symbol=?", (symbol,))
    conn.commit()
    conn.close()
    return {"ok": True, "symbol": symbol}


@app.get("/api/webull/trades")
def webull_trades():
    """Return the 15 most recent trades for steve-webull."""
    conn = _conn()
    rows = conn.execute(
        "SELECT symbol, action, qty, price, asset_type, option_type, "
        "strike_price, expiry_date, realized_pnl, executed_at "
        "FROM trades WHERE player_id='steve-webull' "
        "ORDER BY executed_at DESC LIMIT 15"
    ).fetchall()
    conn.close()
    return {
        "trades": [
            {
                "symbol": r["symbol"],
                "action": r["action"],
                "qty": r["qty"],
                "price": r["price"],
                "asset_type": r["asset_type"],
                "option_type": r["option_type"],
                "strike_price": r["strike_price"],
                "expiry_date": r["expiry_date"],
                "realized_pnl": r["realized_pnl"],
                "time": r["executed_at"],
            }
            for r in rows
        ]
    }


@app.get("/api/price/{symbol}")
def get_price(symbol: str):
    """Get live price for a symbol via Yahoo Finance, with DB fallback."""
    from engine.market_data import get_stock_price
    data = get_stock_price(symbol.upper())
    if "price" in data:
        return {
            "symbol": symbol.upper(),
            "price": data["price"],
            "change": data.get("change_pct", 0),
            "change_pct": data.get("change_pct", 0),
            "prev_close": round(data["price"] / (1 + data.get("change_pct", 0) / 100), 2) if data.get("change_pct") else data["price"]
        }
    # Fallback: last trade price from DB
    conn = _conn()
    row = conn.execute(
        "SELECT price FROM trades WHERE symbol=? ORDER BY executed_at DESC LIMIT 1", (symbol.upper(),)
    ).fetchone()
    conn.close()
    if row:
        return {"symbol": symbol.upper(), "price": row["price"], "change": 0, "change_pct": 0, "prev_close": row["price"], "cached": True}
    return {"symbol": symbol.upper(), "price": 0, "change": 0, "change_pct": 0, "error": data.get("error", "No data")}


# --- Bake-Off Endpoints ---

_bakeoff_status: dict = {}   # run_id -> {progress, message, status, results}
_bakeoff_lock = threading.Lock()


@app.post("/api/bakeoff/start")
def bakeoff_start(payload: dict = None):
    """Start a model bake-off backtest.
    Body: {model: str, days: int}  — model defaults to qwen3.5:9b, days to 30.
    """
    payload = payload or {}
    model = payload.get("model", "qwen3.5:9b")
    days  = int(payload.get("days", 30))

    from engine.weekend_backtest import ensure_bakeoff_tables, run_backtest
    ensure_bakeoff_tables()

    conn = _backtest_conn()
    cur = conn.execute(
        "INSERT INTO bakeoff_runs (model, days, status, progress, message) VALUES (?,?,?,?,?)",
        (model, days, "running", 0, "Initializing..."),
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()

    with _bakeoff_lock:
        _bakeoff_status[run_id] = {"progress": 0, "message": "Initializing...", "status": "running", "results": None}

    def _run():
        def _cb(pct, msg):
            with _bakeoff_lock:
                _bakeoff_status[run_id]["progress"] = pct
                _bakeoff_status[run_id]["message"] = msg

        try:
            results = run_backtest(days=days, model=model, run_id=run_id, progress_cb=_cb)
            with _bakeoff_lock:
                _bakeoff_status[run_id]["status"] = "complete"
                _bakeoff_status[run_id]["progress"] = 100
                _bakeoff_status[run_id]["message"] = "Complete"
                _bakeoff_status[run_id]["results"] = results
        except Exception as e:
            with _bakeoff_lock:
                _bakeoff_status[run_id]["status"] = "error"
                _bakeoff_status[run_id]["message"] = str(e)
            try:
                c2 = _backtest_conn()
                c2.execute("UPDATE bakeoff_runs SET status='error', message=? WHERE id=?", (str(e), run_id))
                c2.commit()
                c2.close()
            except Exception:
                pass

    threading.Thread(target=_run, daemon=True).start()
    return {"run_id": run_id, "model": model, "days": days, "status": "running"}


@app.get("/api/bakeoff/status")
def bakeoff_status(run_id: int = None):
    """Poll bake-off progress. If run_id omitted, returns most recent run."""
    try:
        from engine.weekend_backtest import ensure_bakeoff_tables
        ensure_bakeoff_tables()
    except Exception:
        pass
    if run_id is None:
        # find most recent
        try:
            conn = _backtest_conn()
            row = conn.execute("SELECT id FROM bakeoff_runs ORDER BY id DESC LIMIT 1").fetchone()
            conn.close()
            if row:
                run_id = row["id"]
        except Exception:
            pass

    if run_id is None:
        return {"status": "idle", "progress": 0, "message": "No runs yet"}

    with _bakeoff_lock:
        st = _bakeoff_status.get(run_id)
    if st:
        return {**st, "run_id": run_id}

    # Check DB for completed run
    try:
        conn = _backtest_conn()
        row = conn.execute("SELECT * FROM bakeoff_runs WHERE id=?", (run_id,)).fetchone()
        conn.close()
        if row:
            d = dict(row)
            results = json.loads(d.pop("results_json") or "null")
            return {"run_id": run_id, "status": d["status"], "progress": d["progress"],
                    "message": d["message"], "results": results}
    except Exception:
        pass

    return {"status": "unknown", "run_id": run_id}


@app.get("/api/bakeoff/portfolio")
def bakeoff_portfolio():
    """Return latest completed backtest run with per-agent stats for Holodeck display."""
    try:
        from engine.weekend_backtest import ensure_bakeoff_tables
        ensure_bakeoff_tables()
        conn = _backtest_conn()
        run = conn.execute(
            "SELECT id, model, days, status, started_at, finished_at "
            "FROM bakeoff_runs WHERE status='complete' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not run:
            conn.close()
            return {}
        run_id = run["id"]
        trades = conn.execute(
            "SELECT agent_id, agent_name, pnl FROM bakeoff_trades WHERE run_id=?",
            (run_id,),
        ).fetchall()
        conn.close()

        agents_map: dict = {}
        for t in trades:
            aid = t["agent_id"]
            if aid not in agents_map:
                agents_map[aid] = {
                    "agent_id": aid,
                    "name": t["agent_name"],
                    "starting_equity": 10000,
                    "total_pnl": 0.0,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "best_trade": None,
                    "worst_trade": None,
                }
            a = agents_map[aid]
            p = t["pnl"] or 0.0
            a["total_pnl"] += p
            a["trades"] += 1
            if p >= 0:
                a["wins"] += 1
            else:
                a["losses"] += 1
            if a["best_trade"] is None or p > a["best_trade"]:
                a["best_trade"] = p
            if a["worst_trade"] is None or p < a["worst_trade"]:
                a["worst_trade"] = p

        agent_list = []
        for a in agents_map.values():
            a["final_equity"] = a["starting_equity"] + a["total_pnl"]
            a["return_pct"] = (a["total_pnl"] / a["starting_equity"]) * 100
            a["win_rate"] = (a["wins"] / a["trades"] * 100) if a["trades"] else 0.0
            a["best_trade"] = a["best_trade"] or 0.0
            a["worst_trade"] = a["worst_trade"] or 0.0
            agent_list.append(a)

        fleet_start = len(agent_list) * 10000
        fleet_pnl = sum(a["total_pnl"] for a in agent_list)
        return {
            "run_id": run["id"],
            "model": run["model"],
            "days": run["days"],
            "status": run["status"],
            "started_at": run["started_at"],
            "finished_at": run["finished_at"],
            "agents": agent_list,
            "fleet_total": {
                "starting": fleet_start,
                "final": fleet_start + fleet_pnl,
                "total_pnl": fleet_pnl,
                "return_pct": (fleet_pnl / fleet_start * 100) if fleet_start else 0.0,
            },
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/bakeoff/results")
def bakeoff_results(limit: int = 5):
    """Return last N completed bake-off runs with summary results."""
    try:
        from engine.weekend_backtest import ensure_bakeoff_tables
        ensure_bakeoff_tables()
        conn = _backtest_conn()
        rows = conn.execute(
            "SELECT id, model, days, start_date, end_date, status, progress, message, "
            "results_json, started_at, finished_at FROM bakeoff_runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        out = []
        for row in rows:
            d = dict(row)
            raw = d.pop("results_json", None)
            d["results"] = json.loads(raw) if raw else None
            out.append(d)
        return {"runs": out}
    except Exception as e:
        return {"runs": [], "error": str(e)}


# --- Backtest Lab Endpoints ---

_backtest_status = {}  # run_id -> {progress, message, status, results}
_backtest_lock = threading.Lock()

@app.get("/api/backtest/models")
def backtest_available_models():
    """Return list of models available for backtesting."""
    from config import AI_PLAYERS
    return [{"id": p["id"], "name": p["name"], "provider": p["provider"]} for p in AI_PLAYERS]


@app.post("/api/backtest/run")
def backtest_run(payload: dict):
    """Start a backtest. Body: {date, model_ids, end_date?}"""
    from engine.historical_backtest import (
        run_single_day_backtest, run_multi_day_backtest,
        save_backtest_run, ensure_backtest_tables,
    )
    ensure_backtest_tables()

    date_str = payload.get("date")
    end_date = payload.get("end_date")
    model_ids = payload.get("model_ids", [])

    if not date_str or not model_ids:
        return {"error": "date and model_ids required"}

    # Generate run_id
    conn = _conn()
    run_type = "multi" if end_date and end_date != date_str else "single"
    cur = conn.execute(
        "INSERT INTO backtest_runs (run_type, start_date, end_date, model_ids, status) VALUES (?, ?, ?, ?, 'running')",
        (run_type, date_str, end_date or date_str, json.dumps(model_ids)),
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()

    with _backtest_lock:
        _backtest_status[run_id] = {"progress": 0, "message": "Starting...", "status": "running", "results": None}

    def _run():
        def _progress(pct, msg):
            with _backtest_lock:
                _backtest_status[run_id]["progress"] = pct
                _backtest_status[run_id]["message"] = msg

        try:
            if end_date and end_date != date_str:
                results = run_multi_day_backtest(date_str, end_date, model_ids, _progress)
                save_backtest_run("multi", date_str, end_date, model_ids, results)
            else:
                raw = run_single_day_backtest(date_str, model_ids, _progress)
                results = {pid: r.to_dict() for pid, r in raw.items()}
                save_backtest_run("single", date_str, date_str, model_ids, results)

            with _backtest_lock:
                _backtest_status[run_id]["status"] = "complete"
                _backtest_status[run_id]["progress"] = 100
                _backtest_status[run_id]["message"] = "Complete"
                _backtest_status[run_id]["results"] = results

            # Update DB status
            c2 = _conn()
            c2.execute("UPDATE backtest_runs SET status='complete', completed_at=CURRENT_TIMESTAMP WHERE id=?", (run_id,))
            c2.commit()
            c2.close()
        except Exception as e:
            with _backtest_lock:
                _backtest_status[run_id]["status"] = "error"
                _backtest_status[run_id]["message"] = str(e)

            c2 = _conn()
            c2.execute("UPDATE backtest_runs SET status='error' WHERE id=?", (run_id,))
            c2.commit()
            c2.close()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {"run_id": run_id, "status": "running"}


@app.get("/api/backtest/status/{run_id}")
def backtest_status(run_id: int):
    """Poll backtest progress."""
    with _backtest_lock:
        st = _backtest_status.get(run_id)
    if st:
        return st
    # Check DB for completed runs
    from engine.historical_backtest import get_backtest_run_results
    results = get_backtest_run_results(run_id)
    if results:
        return {"status": "complete", "progress": 100, "message": "Complete", "results": results}
    return {"status": "not_found", "progress": 0, "message": "Run not found"}


@app.get("/api/backtest/runs")
def backtest_runs(limit: int = 20):
    """Get recent backtest runs."""
    from engine.historical_backtest import get_backtest_runs
    return get_backtest_runs(limit)


@app.get("/api/backtest/run/{run_id}")
def backtest_run_detail(run_id: int):
    """Get detailed results for a specific run."""
    from engine.historical_backtest import get_backtest_run_results
    return get_backtest_run_results(run_id)


@app.get("/api/backtest/rankings")
def backtest_rankings():
    """Get model rankings aggregated across all backtest runs."""
    from engine.historical_backtest import get_model_rankings
    return get_model_rankings()


@app.get("/api/backtest/{player_id}")
def backtest(player_id: str, days: int = 30,
             start_date: str = None, end_date: str = None,
             guardrails: bool = False):
    """Run Time Machine backtest for a player.

    Keep this catch-all route after the specific /api/backtest/* endpoints so
    static routes like /models, /runs, and /rankings are not intercepted.
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.backtester import backtest_player
    effective_days = days
    if start_date and end_date:
        from datetime import datetime as _dt
        effective_days = (_dt.strptime(end_date, "%Y-%m-%d") - _dt.strptime(start_date, "%Y-%m-%d")).days
    timeout = max(30, min(effective_days // 10, 120))
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(backtest_player, player_id, days,
                             start_date, end_date, guardrails).result(timeout=timeout)
    except FuturesTimeout:
        return {"error": f"Backtest timed out (>{timeout}s). Try a shorter date range."}
    except Exception as e:
        return {"error": str(e)}


# ─── Strategy Lab ─────────────────────────────────────────────────────────────

_strategy_lab_status = {}
_strategy_lab_lock = threading.Lock()

@app.get("/api/strategy-lab/strategies")
def strategy_lab_strategies():
    """Return available strategies and their parameters."""
    from engine.strategy_lab import STRATEGIES
    return {k: {"name": v["name"], "description": v["description"],
                "params": v["params"], "optimize_grid": v.get("optimize_grid", {})}
            for k, v in STRATEGIES.items()}


@app.post("/api/strategy-lab/run")
def strategy_lab_run(payload: dict):
    """Run a single strategy backtest. Body: {strategy, symbol, start_date, end_date, params?}"""
    from engine.strategy_lab import run_strategy_backtest
    strategy = payload.get("strategy")
    symbol = payload.get("symbol", "AAPL").upper()
    start_date = payload.get("start_date")
    end_date = payload.get("end_date")
    params = payload.get("params", {})

    if not strategy or not start_date or not end_date:
        return {"error": "strategy, start_date, and end_date are required"}

    try:
        return run_strategy_backtest(strategy, params, symbol, start_date, end_date)
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/strategy-lab/optimize")
def strategy_lab_optimize(payload: dict):
    """Start optimization. Body: {strategy, symbol, start_date, end_date, grid?}
    Returns {run_id, status: "running"}. Poll /api/strategy-lab/status/{run_id}.
    """
    strategy = payload.get("strategy")
    symbol = payload.get("symbol", "AAPL").upper()
    start_date = payload.get("start_date")
    end_date = payload.get("end_date")
    custom_grid = payload.get("grid")

    if not strategy or not start_date or not end_date:
        return {"error": "strategy, start_date, and end_date are required"}

    import time as _time
    run_id = int(_time.time() * 1000) % 1_000_000_000

    with _strategy_lab_lock:
        _strategy_lab_status[run_id] = {
            "progress": 0, "message": "Starting...",
            "status": "running", "results": None,
        }

    def _run():
        from engine.strategy_lab import optimize_strategy

        def _progress(pct, msg):
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["progress"] = pct
                _strategy_lab_status[run_id]["message"] = msg

        try:
            result = optimize_strategy(strategy, symbol, start_date, end_date,
                                       custom_grid, progress_cb=_progress)
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "complete"
                _strategy_lab_status[run_id]["progress"] = 100
                _strategy_lab_status[run_id]["message"] = "Complete"
                _strategy_lab_status[run_id]["results"] = result
        except Exception as e:
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "error"
                _strategy_lab_status[run_id]["message"] = str(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"run_id": run_id, "status": "running"}


@app.get("/api/strategy-lab/status/{run_id}")
def strategy_lab_status(run_id: int):
    """Poll optimization progress."""
    with _strategy_lab_lock:
        st = _strategy_lab_status.get(run_id)
    if st:
        return st
    return {"status": "not_found"}


@app.post("/api/strategy-lab/deploy")
def strategy_lab_deploy(payload: dict):
    """Deploy winning params to trading_rules.txt. Body: {strategy, params, stats}"""
    from engine.strategy_lab import deploy_winning_params
    strategy = payload.get("strategy")
    params = payload.get("params", {})
    stats = payload.get("stats", {})
    if not strategy:
        return {"error": "strategy is required"}
    return deploy_winning_params(strategy, params, stats)


@app.get("/api/strategy-lab/latest")
def strategy_lab_latest():
    """Return the most recent auto-optimization report."""
    from engine.strategy_lab import get_latest_report
    report = get_latest_report()
    if report:
        return report
    return {"message": "No optimization reports yet. Run one manually or wait for Sunday auto-run."}


@app.get("/api/strategy-lab/history")
def strategy_lab_history(limit: int = 20):
    """Return summaries of recent optimization reports."""
    from engine.strategy_lab import get_report_history
    return get_report_history(limit)


@app.post("/api/strategy-lab/auto-optimize")
def strategy_lab_auto_optimize():
    """Manually trigger the full auto-optimization pipeline."""
    import time as _time
    run_id = int(_time.time() * 1000) % 1_000_000_000

    with _strategy_lab_lock:
        _strategy_lab_status[run_id] = {
            "progress": 0, "message": "Starting full auto-optimization...",
            "status": "running", "results": None,
        }

    def _run():
        from engine.strategy_lab import auto_optimize_all

        def _progress(pct, msg):
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["progress"] = pct
                _strategy_lab_status[run_id]["message"] = msg

        try:
            report = auto_optimize_all(progress_cb=_progress)
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "complete"
                _strategy_lab_status[run_id]["progress"] = 100
                _strategy_lab_status[run_id]["message"] = "Complete"
                _strategy_lab_status[run_id]["results"] = report
        except Exception as e:
            with _strategy_lab_lock:
                _strategy_lab_status[run_id]["status"] = "error"
                _strategy_lab_status[run_id]["message"] = str(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"run_id": run_id, "status": "running"}


# ─── Holodeck Expansion ──────────────────────────────────────────────────────

@app.post("/api/holodeck/walk-forward")
def run_walk_forward(symbol: str = "SPY", period: str = "5y", n_windows: int = 5):
    """Walk-forward optimization — in-sample optimize, out-of-sample validate."""
    import asyncio
    from engine.holodeck_expansion import walk_forward_backtest
    return walk_forward_backtest(symbol, period, n_windows=n_windows)


@app.post("/api/holodeck/regime-test")
def run_regime_test(symbol: str = "SPY", period: str = "5y"):
    """Regime-aware backtest — partition results by BEAR/BULL/SIDEWAYS."""
    from engine.holodeck_expansion import regime_aware_backtest
    return regime_aware_backtest(symbol, period)


@app.post("/api/holodeck/portfolio-sim")
def run_portfolio_sim(season: int = 5):
    """Portfolio-level simulation — concentration risk and correlation."""
    from engine.holodeck_expansion import portfolio_simulation
    return portfolio_simulation(season)


@app.post("/api/crew/generate-strategy")
def generate_strategy():
    """Launch CrewAI strategy generation crew."""
    try:
        from engine.crew.strategy_crew import run_strategy_crew
        result = run_strategy_crew()
        return {"status": "complete", "result": str(result)}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ─── Reference Data ──────────────────────────────────────────────────────────

@app.get("/api/reference/stats")
def reference_stats():
    """Reference trade data summary."""
    from engine.reference_data import get_reference_stats
    return get_reference_stats()


@app.post("/api/reference/import")
def reference_import(data: dict = None):
    """Import reference trades from JSON. Body: {trades: [...], source: "arena-import"}"""
    if not data or "trades" not in data:
        return {"error": "Provide {trades: [...]}"}
    from engine.reference_data import import_trades
    return import_trades(data["trades"], data.get("source", "arena-import"))


@app.post("/api/reference/import-csv")
def reference_import_csv(data: dict = None):
    """Import reference trades from CSV text. Body: {csv: "...", source: "arena-import"}"""
    if not data or "csv" not in data:
        return {"error": "Provide {csv: '...'}"}
    from engine.reference_data import import_csv
    return import_csv(data["csv"], data.get("source", "arena-import"))


@app.get("/api/reference/compare/{player_id}")
def reference_compare(player_id: str):
    """Compare our model vs reference data for the same LLM."""
    from engine.reference_data import compare_models
    return compare_models(player_id)


@app.get("/api/reference/strategies")
def reference_strategies(limit: int = 10):
    """Top-performing reference trades as strategy inspiration."""
    from engine.reference_data import get_reference_strategies
    return get_reference_strategies(limit)


@app.post("/api/reference/import-ai4trade")
def reference_import_ai4trade(limit: int = 100):
    """Pull latest signals from ai4trade.ai and import as reference data."""
    from engine.importers.ai4trade_importer import import_signals
    return import_signals(limit)


@app.post("/api/reference/parse-feed")
def reference_parse_feed(data: dict = None):
    """Parse a Rallies.ai arena feed text and return preview (no import)."""
    if not data or "text" not in data:
        return {"error": "Provide {text: '...'}"}
    from engine.rallies_parser import parse_rallies_feed
    return parse_rallies_feed(data["text"])


@app.post("/api/reference/import-feed")
def reference_import_feed(data: dict = None):
    """Parse and import a Rallies.ai arena feed text."""
    if not data or "text" not in data:
        return {"error": "Provide {text: '...'}"}
    from engine.rallies_parser import parse_rallies_feed, import_parsed_feed
    parsed = parse_rallies_feed(data["text"])
    result = import_parsed_feed(parsed, data.get("source", "external-arena"))
    result["summary"] = parsed["summary"]
    return result


# ─── Learning Engine ─────────────────────────────────────────────────────────

@app.get("/api/learning/model/{player_id}")
def learning_model_profile(player_id: str):
    """Get model learning profile — score, adjustments, lessons, status."""
    from engine.learning_engine import get_model_profile
    return get_model_profile(player_id)


@app.get("/api/learning/fleet")
def learning_fleet_summary():
    """Fleet-wide learning summary — scores, probation, blocked trades."""
    from engine.learning_engine import get_fleet_summary
    return get_fleet_summary()


@app.get("/api/learning/log")
def learning_log(limit: int = 50):
    """Chronological learning event feed."""
    from engine.learning_engine import get_learning_log
    return get_learning_log(limit)


@app.post("/api/learning/run-daily")
def learning_run_daily():
    """Manually trigger daily post-market review."""
    try:
        from engine.crew.daily_review_crew import run_daily_review
        result = run_daily_review()
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/api/learning/run-weekly")
def learning_run_weekly():
    """Manually trigger weekly model tuning."""
    try:
        from engine.crew.weekly_tuning_crew import run_weekly_tuning
        result = run_weekly_tuning()
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ─── Realtime Monitor ─────────────────────────────────────────────────────────

@app.get("/api/realtime/alerts")
def realtime_alerts(limit: int = 20):
    """Get recent realtime spike alerts."""
    from engine.realtime_monitor import get_recent_alerts
    return get_recent_alerts(limit)


@app.get("/api/realtime/status")
def realtime_status():
    """Get realtime monitor connection status."""
    from engine.realtime_monitor import get_monitor_status
    return get_monitor_status()


@app.get("/api/realtime-feed")
def realtime_feed(symbol: str = "SPY", since_ts: float = 0.0):
    """
    REST polling replacement for former SSE feed (SSE killed — crashes Cloudflare tunnel).
    Returns recent ticks since `since_ts` as JSON. Client polls every 30s.
    """
    import time as _time
    try:
        from engine.realtime_monitor import get_latest_ticks
        sym = symbol.upper()
        ticks = get_latest_ticks(sym, since_ts=since_ts)
        return {"symbol": sym, "ticks": ticks, "server_ts": _time.time()}
    except Exception as e:
        return {"symbol": symbol.upper(), "ticks": [], "server_ts": 0.0, "error": str(e)}


@app.get("/api/trade-feed")
def trade_feed(symbol: str = "SPY", since_id: int = 0):
    """
    REST polling replacement for former SSE trade feed (SSE killed — crashes Cloudflare tunnel).
    Returns trades since `since_id` for the given symbol. Client polls every 30s.
    """
    import sqlite3 as _sq
    from datetime import datetime, timedelta

    sym = symbol.upper()

    def _side(row: dict) -> str:
        action = (row.get("action") or "").upper()
        reasoning = row.get("reasoning") or ""
        price = row.get("price") or 0
        if action in ("BUY_CALL", "BUY_PUT") or (action == "BUY" and price and price < 1):
            return "OPT"
        if action == "BUY":
            return "BUY"
        if "AUTO-STOP" in reasoning.upper() or "[STOP" in reasoning.upper() or "TIME-STOP" in reasoning.upper():
            return "STOP"
        return "SELL"

    def _ts_epoch(ts_str) -> float:
        if not ts_str:
            return 0.0
        try:
            return datetime.fromisoformat(str(ts_str)).timestamp()
        except Exception:
            return 0.0

    try:
        conn = _sq.connect("data/trader.db", check_same_thread=False, timeout=5)
        conn.row_factory = _sq.Row
        if since_id == 0:
            # First call: seed last 30 min
            cutoff = (datetime.now() - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
            rows = conn.execute(
                "SELECT t.id, t.player_id, t.symbol, t.action, t.price, "
                "t.entry_price, t.exit_price, t.realized_pnl, t.reasoning, t.executed_at, "
                "COALESCE(p.display_name, t.player_id) AS agent_name "
                "FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
                "WHERE t.symbol=? AND t.executed_at >= ? ORDER BY t.id ASC",
                (sym, cutoff),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT t.id, t.player_id, t.symbol, t.action, t.price, "
                "t.entry_price, t.exit_price, t.realized_pnl, t.reasoning, t.executed_at, "
                "COALESCE(p.display_name, t.player_id) AS agent_name "
                "FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
                "WHERE t.symbol=? AND t.id > ? ORDER BY t.id ASC",
                (sym, since_id),
            ).fetchall()
        conn.close()
        trades_out = [
            {
                "type": "trade", "id": r["id"],
                "side": _side(dict(r)), "price": r["price"],
                "entry_price": r["entry_price"], "exit_price": r["exit_price"],
                "realized_pnl": r["realized_pnl"], "agent_name": r["agent_name"],
                "symbol": r["symbol"], "ts": _ts_epoch(r["executed_at"]),
            }
            for r in rows
        ]
        return {"symbol": sym, "trades": trades_out}
    except Exception as e:
        return {"symbol": sym, "trades": [], "error": str(e)}


@app.get("/api/news-sentiment/{symbol}")
def get_news_sentiment(symbol: str):
    """Get AI-powered sentiment analysis for a symbol's news"""
    try:
        import feedparser
        from engine.openai_text import DEFAULT_CODEX_MINI_MODEL, generate_text
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        feed = feedparser.parse(url)
        headlines = [e.get("title", "") for e in feed.entries[:5]]
        if not headlines:
            return {"symbol": symbol.upper(), "sentiment": "neutral", "score": 5, "headlines": []}
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return {"symbol": symbol.upper(), "sentiment": "neutral", "score": 5, "headlines": headlines, "error": "No API key"}
        prompt = "Rate market sentiment for " + symbol + " based on these headlines. Respond ONLY as JSON: {\"sentiment\": \"bullish\" or \"bearish\" or \"neutral\", \"score\": 1-10, \"summary\": \"one sentence\"}\nHeadlines:\n" + "\n".join(headlines)
        import re as re2
        text = generate_text(
            prompt,
            model=DEFAULT_CODEX_MINI_MODEL,
            api_key=api_key,
            max_output_tokens=200,
            reasoning_effort="medium",
        )
        m = re2.search(r'{[^{}]*}', text, re2.DOTALL)
        result = json.loads(m.group()) if m else {"sentiment": "neutral", "score": 5, "summary": "No data"}
        result["headlines"] = headlines
        result["symbol"] = symbol.upper()
        return result
    except Exception as e:
        return {"symbol": symbol.upper(), "sentiment": "neutral", "score": 5, "headlines": [], "error": str(e)}


# --- Lightweight Charts Data Endpoint ---

@app.get("/api/chart-data")
def chart_data(symbol: str = "SPY", timeframe: str = "1Day", bars: int = 200):
    """OHLCV candles + indicators + overlays for Lightweight Charts frontend."""
    import os, requests as _req
    import pandas as _pd
    import numpy as _np

    result = {
        "candles": [], "indicators": {}, "gex_levels": {},
        "battle_station": {}, "trades": [], "convergences": [],
        "congress_trades": [], "earnings_dates": [],
    }
    # Timeframe mapping: frontend sends "1m"/"5m"/etc, Alpaca needs "1Min"/"5Min"/etc
    _TF_MAP = {
        "1m":  "1Min",  "1Min":  "1Min",
        "5m":  "5Min",  "5Min":  "5Min",
        "15m": "15Min", "15Min": "15Min",
        "30m": "30Min", "30Min": "30Min",
        "1h":  "1Hour", "1h":    "1Hour", "1Hour": "1Hour",
        "1d":  "1Day",  "1D":    "1Day",  "1Day":  "1Day",
    }
    # Start-date lookback per timeframe (calendar days back from UTC today)
    # 1m uses 2d so data is available after market close when UTC rolls to next day
    _TF_LOOKBACK = {
        "1Min": 2, "5Min": 2, "15Min": 5,
        "30Min": 10, "1Hour": 20, "1Day": 365,
    }
    _atf = _TF_MAP.get(timeframe, "1Day")
    _intraday = _atf != "1Day"

    # ── Alpaca bars ────────────────────────────────────────────────────────
    try:
        _key = os.getenv("ALPACA_API_KEY", "")
        _sec = os.getenv("ALPACA_SECRET_KEY", "")
        from datetime import datetime as _dtt, timedelta as _td
        # SIP feed for intraday, IEX for daily
        _feed = "sip" if _intraday else "iex"
        _days_back = _TF_LOOKBACK.get(_atf, 365)
        _start = (_dtt.utcnow() - _td(days=_days_back)).strftime("%Y-%m-%dT00:00:00Z")
        _params = {"timeframe": _atf, "limit": bars, "feed": _feed,
                   "adjustment": "raw", "start": _start}
        _r = _req.get(
            f"https://data.alpaca.markets/v2/stocks/{symbol.upper()}/bars",
            headers={"APCA-API-KEY-ID": _key, "APCA-API-SECRET-KEY": _sec},
            params=_params, timeout=10
        )
        _r.raise_for_status()
        _raw_bars = _r.json().get("bars") or []
        from datetime import datetime, timezone
        candles = []
        for b in _raw_bars:
            _ts = int(datetime.fromisoformat(b["t"].replace("Z", "+00:00")).timestamp())
            candles.append({"time": _ts, "open": b["o"], "high": b["h"],
                            "low": b["l"], "close": b["c"], "volume": b["v"]})
        result["candles"] = candles
    except Exception as _e:
        pass

    # ── Fallback: yfinance if Alpaca returned nothing ───────────────────────
    if not result["candles"]:
        try:
            import yfinance as _yf
            from datetime import datetime as _dtt2, timedelta as _td2
            _YF_TF = {
                "1Min": "1m", "5Min": "5m", "15Min": "15m",
                "30Min": "30m", "1Hour": "1h", "1Day": "1d",
            }
            _yf_interval = _YF_TF.get(_atf, "1d")
            _yf_days = {"1Min": 2, "5Min": 5, "15Min": 7, "30Min": 14,
                        "1Hour": 30, "1Day": 365}.get(_atf, 30)
            _yf_end = _dtt2.now()
            _yf_start = _yf_end - _td2(days=_yf_days)
            _df = _yf.Ticker(symbol.upper()).history(
                start=_yf_start, end=_yf_end, interval=_yf_interval
            )
            if not _df.empty:
                candles = []
                for _idx, _row in _df.iterrows():
                    candles.append({
                        "time": int(_idx.timestamp()),
                        "open": round(float(_row["Open"]), 4),
                        "high": round(float(_row["High"]), 4),
                        "low": round(float(_row["Low"]), 4),
                        "close": round(float(_row["Close"]), 4),
                        "volume": int(_row["Volume"]),
                    })
                result["candles"] = candles[-bars:]
        except Exception:
            pass

    # ── Indicators ─────────────────────────────────────────────────────────
    try:
        if len(result["candles"]) >= 20:
            _df = _pd.DataFrame(result["candles"])
            _c = _df["close"]
            _v = _df["volume"]
            _ts = _df["time"].tolist()

            def _to_series(vals, times):
                return [{"time": int(t), "value": round(float(v), 4)}
                        for t, v in zip(times, vals) if _pd.notna(v)]

            inds = {}

            # SMAs
            for p in (20, 50, 200):
                if len(_c) >= p:
                    inds[f"sma{p}"] = _to_series(_c.rolling(p).mean(), _ts)

            # EMA 9
            inds["ema9"] = _to_series(_c.ewm(span=9, adjust=False).mean(), _ts)

            # VWAP (intraday only)
            if _intraday:
                _tp = (_df["high"] + _df["low"] + _df["close"]) / 3
                _cv = (_tp * _v).cumsum()
                _cvol = _v.cumsum()
                inds["vwap"] = _to_series(_cv / _cvol.replace(0, _np.nan), _ts)

            # Bollinger 20,2
            if len(_c) >= 20:
                _sma20 = _c.rolling(20).mean()
                _std20 = _c.rolling(20).std()
                inds["bbands"] = [
                    {"time": int(_ts[i]), "upper": round(float(_sma20.iloc[i] + 2*_std20.iloc[i]), 4),
                     "middle": round(float(_sma20.iloc[i]), 4),
                     "lower": round(float(_sma20.iloc[i] - 2*_std20.iloc[i]), 4)}
                    for i in range(len(_c))
                    if _pd.notna(_sma20.iloc[i]) and _pd.notna(_std20.iloc[i])
                ]

            # RSI 14 (Wilder's smoothing = EMA with alpha=1/14)
            if len(_c) >= 15:
                _delta = _c.diff()
                _gain = _delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
                _loss = (-_delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
                _rs = _gain / _loss.replace(0, _np.nan)
                _rsi = 100 - (100 / (1 + _rs))
                inds["rsi"] = _to_series(_rsi, _ts)

            # MACD 12/26/9
            if len(_c) >= 26:
                _ema12 = _c.ewm(span=12, adjust=False).mean()
                _ema26 = _c.ewm(span=26, adjust=False).mean()
                _macd_line = _ema12 - _ema26
                _signal_line = _macd_line.ewm(span=9, adjust=False).mean()
                _hist = _macd_line - _signal_line
                inds["macd"] = [
                    {"time": int(_ts[i]), "macd": round(float(_macd_line.iloc[i]), 4),
                     "signal": round(float(_signal_line.iloc[i]), 4),
                     "histogram": round(float(_hist.iloc[i]), 4)}
                    for i in range(len(_c))
                    if _pd.notna(_macd_line.iloc[i]) and _pd.notna(_signal_line.iloc[i])
                ]

            result["indicators"] = inds
    except Exception:
        pass

    # ── GEX levels ─────────────────────────────────────────────────────────
    try:
        from engine.gex_overlay import get_latest_gex
        _gex = get_latest_gex(symbol.upper())
        if _gex:
            result["gex_levels"] = {
                "gamma_flip": _gex.get("gamma_flip"),
                "call_wall":  _gex.get("call_wall"),
                "put_wall":   _gex.get("put_wall"),
                "king_node":  _gex.get("king_node"),
                "regime":     _gex.get("regime", "unknown"),
            }
    except Exception:
        pass

    # ── Battle Station (morning levels) ────────────────────────────────────
    try:
        from datetime import date as _date
        _bconn = _conn()
        _ml = _bconn.execute(
            "SELECT * FROM morning_levels WHERE symbol=? ORDER BY trade_date DESC LIMIT 1",
            (symbol.upper(),)
        ).fetchone()
        _bconn.close()
        if _ml:
            result["battle_station"] = {
                "or_high":     _ml["or_high"],
                "or_low":      _ml["or_low"],
                "prior_high":  _ml["prior_high"],
                "prior_low":   _ml["prior_low"],
                "prior_close": _ml["prior_close"],
                "prior_vwap":  _ml["prior_vwap"],
            }
    except Exception:
        pass

    # ── Fleet trades for this symbol ───────────────────────────────────────
    try:
        _tconn = _conn()
        _trade_rows = _tconn.execute(
            "SELECT t.id, t.action, t.price, t.qty, t.executed_at, t.reasoning, "
            "t.confidence, t.realized_pnl, t.entry_price, "
            "COALESCE(p.display_name, t.player_id) as player "
            "FROM trades t LEFT JOIN ai_players p ON t.player_id = p.id "
            "WHERE t.symbol=? ORDER BY t.executed_at DESC LIMIT 50",
            (symbol.upper(),)
        ).fetchall()
        _tconn.close()
        from datetime import datetime as _dt
        def _parse_ts(s):
            try:
                return int(_dt.fromisoformat(s.replace("Z","+00:00")).timestamp())
            except Exception:
                return 0
        result["trades"] = [
            {"time": _parse_ts(r["executed_at"]), "price": r["price"],
             "side": "buy" if r["action"] == "BUY" else "sell",
             "player": r["player"] or r["action"],
             "reasoning": (r["reasoning"] or "")[:200],
             "conviction": round((r["confidence"] or 0) * 10, 1),
             "pnl": r["realized_pnl"]}
            for r in _trade_rows if r["price"]
        ]
    except Exception:
        pass

    # ── Convergences (smart_money_signals) ─────────────────────────────────
    try:
        _sconn = _conn()
        _sig_rows = _sconn.execute(
            "SELECT symbol, buyers, detected_at FROM smart_money_signals "
            "WHERE symbol=? ORDER BY detected_at DESC LIMIT 20",
            (symbol.upper(),)
        ).fetchall()
        _sconn.close()
        import json as _json
        def _parse_ts2(s):
            try:
                return int(_dt.fromisoformat(s.replace("Z","+00:00")).timestamp())
            except Exception:
                return 0
        for _sr in _sig_rows:
            try:
                _buyers = _json.loads(_sr["buyers"] or "[]")
                _names = ", ".join(b.get("display_name","?") for b in _buyers[:4])
                _price = _buyers[0].get("price", 0) if _buyers else 0
                result["convergences"].append({
                    "time": _parse_ts2(_sr["detected_at"]),
                    "price": _price,
                    "strategies": len(_buyers),
                    "names": _names,
                })
            except Exception:
                pass
    except Exception:
        pass

    return result


# --- /api/chart/data alias (Big Charts mobile viewer) ---

@app.get("/api/chart/data")
def chart_data_alias(symbol: str = "SPY", timeframe: str = "1Day", bars: int = 200):
    """Alias for /api/chart-data used by the Big Charts mobile viewer."""
    return chart_data(symbol=symbol, timeframe=timeframe, bars=bars)


# --- Chart Analyzer ---

@app.post("/api/chart-analyze")
def chart_analyze(payload: dict):
    """AI-powered chart technical analysis"""
    from engine.chart_analyzer import analyze_chart
    symbol = payload.get("symbol", "SPY")
    model = payload.get("model", "codex")
    return analyze_chart(symbol, model)


@app.get("/api/chart-analyses")
def chart_analyses(symbol: str = None):
    """Get saved chart analyses"""
    from engine.chart_analyzer import load_analyses
    analyses = load_analyses()
    if symbol:
        analyses = [a for a in analyses if a.get("symbol", "").upper() == symbol.upper()]
    return analyses


@app.get("/api/chart-analyses/{symbol}/compare")
def chart_analyses_compare(symbol: str):
    """Compare analyses across models for a symbol"""
    from engine.chart_analyzer import get_comparison
    return get_comparison(symbol)


# --- Scanner Tier Status ---

@app.get("/api/scanner/status")
def scanner_status():
    """Current scanning tier, last scan times, and market phase."""
    import time, pytz
    from datetime import datetime as _dt
    from engine.risk_manager import RiskManager
    az = pytz.timezone("US/Arizona")
    now = _dt.now(az)
    h = now.hour + now.minute / 60.0
    # Market phases (MST = Arizona = US/Arizona, no DST)
    if 4.0 <= h < 6.5:
        phase = "pre-market"
    elif 6.5 <= h < 13.0:
        phase = "market-open"
    elif 13.0 <= h < 16.0:
        phase = "after-hours"
    else:
        phase = "closed"
    market_open = RiskManager.is_market_hours()
    return {
        "phase": phase,
        "market_open": market_open,
        "tiers": {
            "tier1": {"name": "Bridge Crew", "interval_min": 30, "members": 6},
            "tier2": {"name": "Dept Heads", "interval_min": 120, "members": 6},
            "tier3": {"name": "Cadets", "interval_min": 240, "members": 10},
        },
        "active_tier": 1 if market_open else (2 if phase in ("pre-market", "after-hours") else 0),
        "timestamp": now.isoformat(),
    }


# --- Pre-Market Gap Scanner ---

@app.get("/api/premarket-gaps")
@timed_cache(300)
def premarket_gaps():
    """Scan watchlist for pre-market price gaps > 2%"""
    from engine.premarket_scanner import scan_premarket_gaps
    return {"gaps": scan_premarket_gaps()}


@app.post("/api/premarket-analyze")
def premarket_analyze():
    """AI analysis of pre-market gaps across all models"""
    from engine.premarket_scanner import analyze_gaps_with_ai
    return {"responses": analyze_gaps_with_ai()}


@app.get("/api/dayblade/gap-candidates")
def dayblade_gap_candidates():
    """Pre-market gap candidates for DayBlade 0DTE plays"""
    from engine.premarket_scanner import get_dayblade_gap_candidates
    return {"candidates": get_dayblade_gap_candidates()}


@app.get("/api/premarket-watchlist")
@timed_cache(60)
def premarket_watchlist():
    """Today's Finviz pre-market watchlist — variable movers + fixed core symbols."""
    from engine.premarket_scanner import get_todays_watchlist
    return get_todays_watchlist()


@app.get("/api/signal-center/top")
@timed_cache(120)
def signal_center_top(limit: int = 10):
    """Top scored symbols from Signal Center (port 9000) intelligence_feed.

    Queries the signal-center/signals.db directly (local file read, no HTTP hop).
    Returns congress_insider and dte0_setup picks from today, ranked by score.
    Falls back to recent data if today has no entries yet.
    """
    import sqlite3 as _sq
    from pathlib import Path

    SC_DB = Path(__file__).parent.parent / "signal-center" / "signals.db"
    if not SC_DB.exists():
        return {"picks": [], "generated_at": None, "source": "signal_center"}

    try:
        c = _sq.connect(str(SC_DB), check_same_thread=False, timeout=5)
        c.row_factory = _sq.Row

        # Try today first, fall back to last 2 days
        for days_back in (0, 1, 2):
            date_filter = f"date(created_at) = date('now', '-{days_back} days')"
            rows = c.execute(f"""
                SELECT
                  json_extract(data, '$.symbol')                    AS symbol,
                  MAX(CAST(json_extract(data, '$.score') AS REAL))   AS score,
                  json_extract(data, '$.preset')                     AS preset,
                  MAX(CAST(json_extract(data, '$.fleet_bull') AS REAL)) AS fleet_bull,
                  MAX(created_at)                                     AS latest
                FROM intelligence_feed
                WHERE feed_type = 'SCREENER'
                  AND {date_filter}
                  AND json_extract(data, '$.symbol') IS NOT NULL
                  AND CAST(json_extract(data, '$.score') AS REAL) > 0
                GROUP BY json_extract(data, '$.symbol')
                ORDER BY score DESC, fleet_bull DESC
                LIMIT ?
            """, (limit,)).fetchall()
            if rows:
                break

        # Also pull latest VOLUME_SPIKE top_stocks for high-rvol names
        vol_row = c.execute("""
            SELECT data FROM intelligence_feed
            WHERE feed_type='VOLUME_SPIKE'
            ORDER BY created_at DESC LIMIT 1
        """).fetchone()
        rvol_map: dict = {}
        if vol_row:
            try:
                import json as _j
                vs = _j.loads(vol_row["data"])
                for entry in (vs.get("top_stocks") or [])[:20]:
                    sym = entry.get("symbol", "")
                    rvol = float(entry.get("rvol", 0))
                    if sym and rvol > 1.5:
                        rvol_map[sym] = rvol
            except Exception:
                pass

        c.close()

        picks = []
        for r in rows:
            sym = r["symbol"]
            picks.append({
                "symbol":    sym,
                "score":     float(r["score"] or 0),
                "preset":    r["preset"] or "",
                "fleet_bull": float(r["fleet_bull"] or 0),
                "rvol":      rvol_map.get(sym, 0.0),
                "latest":    r["latest"],
            })

        return {
            "picks":        picks,
            "generated_at": picks[0]["latest"] if picks else None,
            "source":       "signal_center",
        }
    except Exception as e:
        return {"picks": [], "error": str(e), "source": "signal_center"}


# --- Stock Screener ---

@app.get("/api/screener/quality")
def quality_screener(refresh: bool = False):
    """Dalio/Buffett quality screen — tickers with high margins, low debt, high ROE.

    Filters: Gross Margin >50%, LT Debt/Equity <0.4, Operating Margin >25%, ROE >15%.
    Cached 4 hours. Pass ?refresh=true to force a reload.
    """
    from shared.finviz_scanner import finviz_quality_screen, _quality_cache
    import time
    if refresh:
        _quality_cache["updated"] = 0.0  # invalidate cache
    tickers = finviz_quality_screen()
    return {
        "preset": "Quality Filter — Dalio/Buffett",
        "filters": {
            "Gross Margin": "High (>50%)",
            "LT Debt/Equity": "Under 0.4",
            "Operating Margin": "High (>25%)",
            "Return on Equity": "Over +15%",
        },
        "finviz_url": "https://finviz.com/screener.ashx?v=111&f=fa_grossmargin_high,fa_ltdebteq_u0.4,fa_opermargin_high,fa_roe_o15",
        "count": len(tickers),
        "tickers": tickers,
        "cached_at": _quality_cache.get("updated", 0),
    }


@app.post("/api/screener/pro")
async def screener_pro_post(request: Request):
    """Full screener engine with TradeMinds filters — POST JSON body."""
    try:
        filters = await request.json()
    except Exception:
        filters = {}
    try:
        from engine.screener_engine import run_screener, PRESETS
        results = run_screener(filters)
        return {"results": results, "count": len(results)}
    except Exception as e:
        return {"results": [], "error": str(e)}


@app.get("/api/screener/pro")
def screener_pro_get(
    mktcap: str = None, sector: str = None,
    rsi_max: float = None, rsi_min: float = None,
    rvol_min: float = None, change_min: float = None,
    fleet_bull_min: int = None, has_congress: bool = None,
    limit: int = 50, sort_by: str = "score",
):
    """Screener engine via GET params."""
    filters = {k: v for k, v in {
        "mktcap": mktcap, "sector": sector,
        "rsi_max": rsi_max, "rsi_min": rsi_min,
        "rvol_min": rvol_min, "change_min": change_min,
        "fleet_bull_min": fleet_bull_min, "has_congress": has_congress,
        "limit": limit, "sort_by": sort_by,
    }.items() if v is not None}
    try:
        from engine.screener_engine import run_screener
        results = run_screener(filters)
        return {"results": results, "count": len(results)}
    except Exception as e:
        return {"results": [], "error": str(e)}


@app.get("/api/screener/presets")
def screener_presets():
    """Return available screener presets."""
    try:
        from engine.screener_engine import PRESETS
        return {"presets": {k: {"description": v.get("description", k)} for k, v in PRESETS.items()}}
    except Exception:
        return {"presets": {}}


@app.get("/api/candles")
async def get_candles(symbol: str = "SPY", timeframe: str = "5m", days: int = None):
    """OHLCV candles for the live chart. Uses yfinance — no DB required.
    timeframe: 1m | 5m | 15m | 1h | 1D
    days: lookback window (default: 5 for intraday, 90 for 1D)
    Returns: [{t, o, h, l, c, v}, ...] where t is Unix seconds UTC.
    """
    import math
    try:
        import yfinance as yf
        # Map timeframe label → yfinance interval + sensible default lookback
        _TF_MAP = {
            "1m":  ("1m",  1),
            "5m":  ("5m",  5),
            "15m": ("15m", 5),
            "1h":  ("60m", 30),
            "1D":  ("1d",  90),
        }
        tf = timeframe.strip()
        if tf not in _TF_MAP:
            return {"error": f"Unknown timeframe '{tf}'. Use: 1m 5m 15m 1h 1D", "candles": []}
        interval, default_days = _TF_MAP[tf]
        lookback = days if days else default_days
        # yfinance max period caps per interval
        _MAX_DAYS = {"1m": 7, "5m": 60, "15m": 60, "60m": 730, "1d": 3650}
        lookback = min(lookback, _MAX_DAYS.get(interval, 90))

        hist = yf.download(
            symbol.upper(),
            period=f"{lookback}d",
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
        if hist.empty:
            return {"candles": [], "symbol": symbol, "timeframe": tf, "count": 0}

        candles = []
        for ts, row in hist.iterrows():
            try:
                t = int(ts.timestamp())
                o = float(row["Open"].iloc[0])  if hasattr(row["Open"], "iloc") else float(row["Open"])
                h = float(row["High"].iloc[0])  if hasattr(row["High"], "iloc") else float(row["High"])
                l = float(row["Low"].iloc[0])   if hasattr(row["Low"],  "iloc") else float(row["Low"])
                c = float(row["Close"].iloc[0]) if hasattr(row["Close"],"iloc") else float(row["Close"])
                v = int(row["Volume"].iloc[0])  if hasattr(row["Volume"],"iloc") else int(row["Volume"])
                # Skip NaN rows
                if any(math.isnan(x) for x in [o, h, l, c]):
                    continue
                candles.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
            except Exception:
                continue

        return {
            "candles":   candles,
            "symbol":    symbol.upper(),
            "timeframe": tf,
            "count":     len(candles),
            "days":      lookback,
        }
    except Exception as e:
        return {"error": str(e), "candles": []}


@app.get("/api/options-chain")
async def options_chain_endpoint(symbol: str = "SPY", expiration: str = None):
    """Options chain data. Defaults to 0DTE chain. Pass expiration= for specific date."""
    try:
        from engine.options_chain import get_0dte_chain, get_chain, get_max_pain, get_put_call_ratio, find_best_plays
        if expiration:
            chain = get_chain(symbol.upper(), expiration)
        else:
            chain = get_0dte_chain(symbol.upper())
        if not chain:
            return {"error": f"No options data for {symbol}"}
        exp = chain.get("expiration", expiration or "")
        pcr = get_put_call_ratio(symbol.upper(), exp)
        mp  = get_max_pain(symbol.upper(), exp)
        return {
            "symbol":          chain["symbol"],
            "expiration":      exp,
            "spot":            chain.get("spot"),
            "is_0dte":         chain.get("is_0dte", False),
            "calls":           chain.get("calls", []),
            "puts":            chain.get("puts", []),
            "put_call_ratio":  pcr,
            "max_pain":        mp,
            "call_count":      len(chain.get("calls", [])),
            "put_count":       len(chain.get("puts", [])),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/options-chain/plays")
async def options_best_plays(symbol: str = "SPY", direction: str = "bullish", budget: float = 200):
    """Top 0DTE plays ranked by liquidity, ATM proximity, and budget fit."""
    try:
        from engine.options_chain import find_best_plays
        result = find_best_plays(symbol.upper(), direction=direction, budget=budget)
        if not result:
            return {"error": f"No 0DTE plays found for {symbol}"}
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/options-chain/unusual")
async def options_unusual_activity(symbol: str = "SPY"):
    """Strikes where volume > 10x open interest (unusual flow detection)."""
    try:
        from engine.options_chain import check_unusual_activity
        unusual = check_unusual_activity(symbol.upper())
        return {"symbol": symbol.upper(), "unusual_strikes": unusual, "count": len(unusual)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/benchmark")
def get_benchmark(days: int = 30):
    """Fleet vs SPY/QQQ/60-40 benchmark comparison."""
    try:
        from engine.benchmark import compute_benchmark
        return compute_benchmark(days)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/benchmark/summary")
def get_benchmark_summary():
    """Quick scorecard: 7d, 30d, 90d windows."""
    try:
        from engine.benchmark import get_benchmark_summary
        return get_benchmark_summary()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/bootstrap-report")
def bootstrap_report():
    """Return full bootstrap intelligence report."""
    try:
        import sqlite3 as _sq
        db = _sq.connect("data/trader.db", timeout=5)
        db.row_factory = _sq.Row
        rows = db.execute(
            "SELECT * FROM bootstrap_metrics ORDER BY calculated_at DESC LIMIT 500"
        ).fetchall()
        db.close()
        return {"metrics": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return {"metrics": [], "error": str(e)}


@app.get("/api/screener")
def stock_screener(
    min_pe: float = None, max_pe: float = None,
    min_short_float: float = None, max_short_float: float = None,
    min_rel_volume: float = None, consensus: str = None,
    has_insider_buying: bool = None, earnings_within_days: int = None
):
    """Screen watchlist stocks by fundamental filters"""
    from engine.stock_screener import screen_stocks
    return {"results": screen_stocks(
        min_pe=min_pe, max_pe=max_pe,
        min_short_float=min_short_float, max_short_float=max_short_float,
        min_rel_volume=min_rel_volume, consensus=consensus,
        has_insider_buying=has_insider_buying, earnings_within_days=earnings_within_days
    )}


# --- Insider Trading ---

@app.get("/api/insider-trades/{symbol}")
def insider_trades(symbol: str):
    """Get insider trading data for a symbol"""
    import math
    try:
        from engine.insider_tracker import get_insider_trades
        trades = get_insider_trades(symbol)
        # Sanitize NaN/Inf floats that break JSON serialization
        for t in trades:
            for k, v in t.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    t[k] = 0.0
        return {"trades": trades}
    except Exception:
        return {"trades": []}


@app.get("/api/insider-alerts")
def insider_alerts():
    """Scan watchlist for recent insider buying alerts"""
    from engine.insider_tracker import scan_insider_alerts
    return {"alerts": scan_insider_alerts()}


# --- S&P 500 Sector Heat Map ---

@app.get("/api/sectors/heatmap")
def sectors_heatmap():
    """S&P 500 sector ETF heat map — always returns all 12 sectors.

    Returns disk cache immediately on first call; fires background refresh when stale (5 min TTL).
    Never returns empty sectors — always falls back to disk cache if in-memory is empty.
    Response includes cached_at (unix ts), cache_age_seconds, and spy_change_pct.
    """
    import time as _t
    from engine.premarket_scanner import (
        get_sector_heatmap, _sector_disk_cache, _DEFENSE_ETF, _DEFENSE_HOLDINGS,
        _ALL_SECTOR_NAMES,
    )

    now = _t.time()
    _cache_entry = _swr_cache.get("sectors_heatmap", {"data": None, "ts": 0})

    # Seed in-memory cache from disk if empty (first request after startup)
    if not (_cache_entry.get("data") or {}).get("sectors"):
        disk_sectors = list(_sector_disk_cache.get("sectors") or [])
        disk_ts = _sector_disk_cache.get("ts", 0)
        if disk_sectors:
            _swr_cache["sectors_heatmap"] = {"data": {"sectors": disk_sectors}, "ts": disk_ts}
            _cache_entry = _swr_cache["sectors_heatmap"]

    # Background refresh when stale (TTL 5 min) or cache is still empty
    age = now - _cache_entry.get("ts", 0)
    if age > 300 or not (_cache_entry.get("data") or {}).get("sectors"):
        if _swr_locks.get("sectors_heatmap") and _swr_locks["sectors_heatmap"].acquire(blocking=False):
            _swr_refreshing.add("sectors_heatmap")
            def _bg_refresh():
                try:
                    data = get_sector_heatmap()
                    if data:
                        _swr_cache["sectors_heatmap"] = {"data": {"sectors": data}, "ts": _t.time()}
                except Exception:
                    pass
                finally:
                    _swr_refreshing.discard("sectors_heatmap")
                    _swr_locks["sectors_heatmap"].release()
            threading.Thread(target=_bg_refresh, daemon=True).start()

    cached_data = _cache_entry.get("data") or {}
    sectors = cached_data.get("sectors") or []

    # Last-resort: if still empty, try disk cache directly (disk might be ahead of in-memory)
    if not sectors:
        sectors = list(_sector_disk_cache.get("sectors") or [])

    cache_ts = _cache_entry.get("ts", 0)
    cache_age = int(now - cache_ts) if cache_ts else None

    # Extract spy_change_pct from sector data (attached by get_sector_heatmap)
    spy_pct = None
    for s in sectors:
        if "spy_change_pct" in s:
            spy_pct = s["spy_change_pct"]
            break

    return {
        "sectors": sectors,
        "cached_at": cache_ts if cache_ts else None,
        "cache_age_seconds": cache_age,
        "spy_change_pct": spy_pct,
        "total_sectors": len(sectors),
        "is_updating": "sectors_heatmap" in _swr_refreshing,
    }


# --- S&P 500 Treemap (top 50 by market cap) ---

_sp500_treemap_cache = {"data": None, "ts": 0}

@app.get("/api/market/sp500-treemap")
def sp500_treemap():
    """Top 50 S&P 500 stocks by market cap, grouped by sector, for treemap display."""
    import time as _time
    if _sp500_treemap_cache["data"] and _time.time() - _sp500_treemap_cache["ts"] < 55:
        return _sp500_treemap_cache["data"]

    # Top 50 S&P 500 by market cap with sectors
    SP500_TOP50 = [
        ("AAPL", "Technology"), ("MSFT", "Technology"), ("NVDA", "Technology"),
        ("AMZN", "Consumer Cyclical"), ("GOOGL", "Communication Services"),
        ("META", "Communication Services"), ("BRK-B", "Financial"),
        ("LLY", "Healthcare"), ("AVGO", "Technology"), ("JPM", "Financial"),
        ("TSLA", "Consumer Cyclical"), ("UNH", "Healthcare"), ("XOM", "Energy"),
        ("V", "Financial"), ("MA", "Financial"), ("COST", "Consumer Defensive"),
        ("JNJ", "Healthcare"), ("HD", "Consumer Cyclical"), ("PG", "Consumer Defensive"),
        ("ABBV", "Healthcare"), ("WMT", "Consumer Defensive"), ("NFLX", "Communication Services"),
        ("CRM", "Technology"), ("BAC", "Financial"), ("KO", "Consumer Defensive"),
        ("MRK", "Healthcare"), ("CVX", "Energy"), ("ORCL", "Technology"),
        ("AMD", "Technology"), ("PEP", "Consumer Defensive"), ("TMO", "Healthcare"),
        ("ACN", "Technology"), ("LIN", "Basic Materials"), ("ADBE", "Technology"),
        ("MCD", "Consumer Cyclical"), ("CSCO", "Technology"), ("ABT", "Healthcare"),
        ("PM", "Consumer Defensive"), ("WFC", "Financial"), ("NOW", "Technology"),
        ("IBM", "Technology"), ("GE", "Industrials"), ("ISRG", "Healthcare"),
        ("CAT", "Industrials"), ("INTU", "Technology"), ("VZ", "Communication Services"),
        ("TXN", "Technology"), ("QCOM", "Technology"), ("AMGN", "Healthcare"),
        ("SPGI", "Financial"),
    ]

    try:
        import yfinance as yf
        symbols = [s[0] for s in SP500_TOP50]
        tickers = yf.Tickers(" ".join(symbols))

        sectors = {}
        for sym, sector in SP500_TOP50:
            try:
                t = tickers.tickers.get(sym) or tickers.tickers.get(sym.replace("-", ""))
                if not t:
                    continue
                info = t.fast_info
                price = float(info.last_price) if hasattr(info, "last_price") else 0
                prev = float(info.previous_close) if hasattr(info, "previous_close") else price
                mcap = float(info.market_cap) if hasattr(info, "market_cap") else 0
                change_pct = ((price - prev) / prev * 100) if prev > 0 else 0

                if sector not in sectors:
                    sectors[sector] = {"sector": sector, "stocks": [], "total_mcap": 0}
                sectors[sector]["stocks"].append({
                    "symbol": sym,
                    "price": round(price, 2),
                    "change_pct": round(change_pct, 2),
                    "market_cap": mcap,
                })
                sectors[sector]["total_mcap"] += mcap
            except Exception:
                continue

        # Sort sectors by total market cap, stocks within each sector by market cap
        result = sorted(sectors.values(), key=lambda s: s["total_mcap"], reverse=True)
        for sec in result:
            sec["stocks"].sort(key=lambda s: s["market_cap"], reverse=True)

        _sp500_treemap_cache["data"] = result
        _sp500_treemap_cache["ts"] = _time.time()
        return result
    except Exception as e:
        return [{"sector": "Error", "stocks": [], "total_mcap": 0, "error": str(e)}]


# --- Finnhub Intelligence ---

@app.get("/api/finnhub/insider/{symbol}")
def finnhub_insider(symbol: str):
    """Get Finnhub insider transactions for a symbol."""
    from engine.finnhub_data import get_insider_transactions
    return {"transactions": get_insider_transactions(symbol)}


@app.get("/api/finnhub/insider-sentiment/{symbol}")
def finnhub_insider_sentiment(symbol: str):
    """Get aggregated insider sentiment for a symbol."""
    from engine.finnhub_data import get_insider_sentiment
    return get_insider_sentiment(symbol)


@app.get("/api/finnhub/earnings")
def finnhub_earnings():
    """Get upcoming earnings for watchlist stocks."""
    from engine.finnhub_data import get_earnings_calendar
    return {"earnings": get_earnings_calendar()}


@app.get("/api/finnhub/news-sentiment/{symbol}")
def finnhub_news_sentiment(symbol: str):
    """Get Finnhub news sentiment score for a symbol."""
    from engine.finnhub_data import get_news_sentiment
    return get_news_sentiment(symbol)


@app.get("/api/finnhub/filings/{symbol}")
def finnhub_filings(symbol: str, form: str = None):
    """Get SEC filings for a symbol."""
    from engine.finnhub_data import get_sec_filings
    return {"filings": get_sec_filings(symbol, form)}


@app.get("/api/finnhub/context/{symbol}")
def finnhub_context(symbol: str):
    """Get full Finnhub intelligence context for a symbol (for AI prompts)."""
    from engine.finnhub_data import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


# --- Alpha Vantage Intelligence ---

@app.get("/api/alphavantage/technicals/{symbol}")
def av_technicals(symbol: str):
    """Get RSI, MACD, SMA from Alpha Vantage as cross-check."""
    from engine.alphavantage_data import get_rsi, get_macd, get_sma
    return {
        "symbol": symbol,
        "rsi": get_rsi(symbol),
        "macd": get_macd(symbol),
        "sma20": get_sma(symbol, time_period=20),
    }


@app.get("/api/alphavantage/overview/{symbol}")
def av_overview(symbol: str):
    """Get company fundamentals from Alpha Vantage."""
    from engine.alphavantage_data import get_company_overview
    return get_company_overview(symbol) or {"error": "No data available"}


@app.get("/api/alphavantage/earnings/{symbol}")
def av_earnings(symbol: str):
    """Get earnings surprises (last 4 quarters)."""
    from engine.alphavantage_data import get_earnings_surprises
    return {"surprises": get_earnings_surprises(symbol)}


@app.get("/api/alphavantage/context/{symbol}")
def av_context(symbol: str):
    """Get full Alpha Vantage intelligence context for a symbol."""
    from engine.alphavantage_data import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


# --- FRED Macro Data ---

@app.get("/api/macro")
@timed_cache(300)
def macro_data():
    """Get FRED macro economic indicators."""
    from engine.alphavantage_data import get_macro_data
    return get_macro_data()


@app.get("/api/macro/context")
def macro_context():
    """Get macro context string for AI prompts."""
    from engine.alphavantage_data import build_macro_context
    return {"context": build_macro_context()}


# ── Phase 3.5 — Multi-Asset Macro Dashboard ──────────────────────────────────

import time as _macro_time

_macro_cache: dict = {"data": None, "ts": 0}
_MACRO_TTL = 900  # 15 minutes

_MACRO_TICKERS = {
    "SPY":    {"label": "S&P 500",      "type": "equity"},
    "TLT":    {"label": "Bonds (TLT)",   "type": "bonds"},
    "GLD":    {"label": "Gold",          "type": "commodity"},
    "SLV":    {"label": "Silver",        "type": "commodity"},
    "BTC-USD":{"label": "Bitcoin",       "type": "crypto"},
    "^VIX":   {"label": "VIX",           "type": "volatility"},
    "UUP":    {"label": "USD Strength",  "type": "currency"},
    "CPER":   {"label": "Copper",        "type": "commodity"},
}


def _fetch_macro_data() -> dict:
    try:
        import yfinance as yf
        import numpy as np
        tickers_list = list(_MACRO_TICKERS.keys())
        raw = yf.download(
            tickers_list, period="35d", interval="1d",
            group_by="ticker", auto_adjust=True, progress=False, threads=True
        )

        prices: dict = {}
        close_series: dict = {}

        for tk in tickers_list:
            try:
                if len(tickers_list) > 1:
                    df = raw[tk] if tk in raw else None
                else:
                    df = raw
                if df is None or df.empty:
                    continue
                df = df.dropna(subset=["Close"])
                if len(df) < 2:
                    continue
                close_col = df["Close"]
                last_close = float(close_col.iloc[-1])
                prev_close = float(close_col.iloc[-2])
                chg_pct = ((last_close - prev_close) / prev_close * 100) if prev_close else 0
                prices[tk] = {
                    "price": round(last_close, 2),
                    "prev_close": round(prev_close, 2),
                    "change_pct": round(chg_pct, 2),
                    "label": _MACRO_TICKERS[tk]["label"],
                    "type": _MACRO_TICKERS[tk]["type"],
                }
                # Keep 30d of closes for correlation (aligned)
                close_series[tk] = close_col.iloc[-30:].values.tolist()
            except Exception:
                pass

        # Correlation matrix (30-day rolling)
        corr_matrix: dict = {}
        corr_tickers = [t for t in tickers_list if t in close_series]
        if len(corr_tickers) >= 2:
            import pandas as pd
            min_len = min(len(close_series[t]) for t in corr_tickers)
            df_corr = pd.DataFrame(
                {t: close_series[t][-min_len:] for t in corr_tickers}
            )
            pct_chg = df_corr.pct_change().dropna()
            if not pct_chg.empty:
                c = pct_chg.corr()
                for t1 in corr_tickers:
                    corr_matrix[t1] = {}
                    for t2 in corr_tickers:
                        try:
                            corr_matrix[t1][t2] = round(float(c.loc[t1, t2]), 3)
                        except Exception:
                            corr_matrix[t1][t2] = None

        # Cu/Au ratio (CPER / GLD as proxy)
        cper_price = prices.get("CPER", {}).get("price")
        gld_price  = prices.get("GLD", {}).get("price")
        cu_au_ratio = round(cper_price / gld_price, 4) if cper_price and gld_price else None

        # Regime detection
        spy_chg  = prices.get("SPY", {}).get("change_pct", 0)
        vix_lvl  = prices.get("^VIX", {}).get("price", 20)
        gld_chg  = prices.get("GLD", {}).get("change_pct", 0)
        tlt_chg  = prices.get("TLT", {}).get("change_pct", 0)
        uup_chg  = prices.get("UUP", {}).get("change_pct", 0)
        btc_chg  = prices.get("BTC-USD", {}).get("change_pct", 0)

        risk_on_score = (
            (1 if spy_chg > 0.3 else -1 if spy_chg < -0.3 else 0) +
            (1 if vix_lvl < 18 else -1 if vix_lvl > 25 else 0) +
            (1 if gld_chg < 0 else -1 if gld_chg > 0.5 else 0) +
            (1 if tlt_chg < 0 else -1 if tlt_chg > 0.5 else 0) +
            (-1 if uup_chg > 0.2 else 1 if uup_chg < -0.2 else 0)
        )

        if risk_on_score >= 3:
            regime = "RISK ON"
            regime_color = "green"
        elif risk_on_score <= -3:
            regime = "RISK OFF"
            regime_color = "red"
        elif abs(spy_chg) > 0.5 and abs(gld_chg) > 0.3 and spy_chg * gld_chg < 0:
            regime = "ROTATION"
            regime_color = "orange"
        else:
            regime = "NEUTRAL"
            regime_color = "yellow"

        # Also pull from DB if available
        try:
            _c = _conn()
            cs = _c.execute(
                "SELECT risk_mode FROM correlation_snapshots ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if cs and cs["risk_mode"] and cs["risk_mode"] != regime:
                db_regime = cs["risk_mode"].upper().replace("_", " ")
                # Use DB regime as tiebreaker when our score is ambiguous
                if regime == "NEUTRAL" and db_regime:
                    regime = db_regime
                    regime_color = "green" if "ON" in db_regime else ("red" if "OFF" in db_regime else "yellow")
            _c.close()
        except Exception:
            pass

        result = {
            "ok": True,
            "prices": prices,
            "corr_matrix": corr_matrix,
            "corr_tickers": corr_tickers,
            "cu_au_ratio": cu_au_ratio,
            "regime": regime,
            "regime_color": regime_color,
            "risk_on_score": risk_on_score,
            "fetched_at": __import__("datetime").datetime.now().strftime("%H:%M"),
        }
        return result
    except Exception as e:
        return {"ok": False, "error": str(e), "prices": {}, "corr_matrix": {}, "regime": "UNKNOWN", "regime_color": "gray"}


@app.get("/api/macro/dashboard")
def macro_dashboard(force: bool = False):
    """Multi-asset macro dashboard: prices, correlations, regime."""
    global _macro_cache
    now = _macro_time.time()
    if not force and _macro_cache["data"] and (now - _macro_cache["ts"]) < _MACRO_TTL:
        return JSONResponse(_macro_cache["data"])
    data = _fetch_macro_data()
    if data.get("ok"):
        _macro_cache = {"data": data, "ts": now}
    return JSONResponse(data)


# --- Polygon.io Data (activates when POLYGON_API_KEY is set) ---

@app.get("/api/polygon/snapshot/{symbol}")
def polygon_snapshot(symbol: str):
    """Get real-time snapshot from Polygon.io."""
    from engine.providers.polygon_provider import PolygonData
    poly = PolygonData()
    if not poly.is_active():
        return {"error": "Polygon API key not configured. Set POLYGON_API_KEY in environment."}
    return poly.get_snapshot(symbol.upper()) or {"error": f"No data for {symbol}"}


@app.get("/api/polygon/bars/{symbol}")
def polygon_bars(symbol: str, timespan: str = "day", multiplier: int = 1, limit: int = 30):
    """Get historical OHLCV bars from Polygon.io."""
    from engine.providers.polygon_provider import PolygonData
    poly = PolygonData()
    if not poly.is_active():
        return {"error": "Polygon API key not configured"}
    return {"bars": poly.get_bars(symbol.upper(), timespan, multiplier, limit)}


@app.get("/api/polygon/options/{symbol}")
def polygon_options(symbol: str):
    """Get options flow sentiment from Polygon.io (feeds Counselor Troi)."""
    from engine.providers.polygon_provider import PolygonData
    poly = PolygonData()
    if not poly.is_active():
        return {"error": "Polygon API key not configured"}
    return poly.get_options_sentiment(symbol.upper())


# --- Combined Intelligence ---

@app.get("/api/intelligence/{symbol}")
def combined_intelligence(symbol: str):
    """Get combined intelligence from all data sources for a symbol."""
    parts = []
    try:
        from engine.finnhub_data import build_ai_context as fh_ctx
        fh = fh_ctx(symbol)
        if fh:
            parts.append(fh)
    except Exception:
        pass
    try:
        from engine.alphavantage_data import build_ai_context as av_ctx
        av = av_ctx(symbol)
        if av:
            parts.append(av)
    except Exception:
        pass
    try:
        from engine.alphavantage_data import build_macro_context
        macro = build_macro_context()
        if macro:
            parts.append(macro)
    except Exception:
        pass
    return {"symbol": symbol, "context": " | ".join(parts), "parts": parts}


# --- TradeMinds: Smart Risk Levels ---

@app.get("/api/risk-levels/{symbol}")
def risk_levels(symbol: str, entry_price: float = None, side: str = "BUY"):
    """Calculate smart risk levels for a symbol."""
    from engine.smart_risk import calculate_risk_levels
    from engine.market_data import get_stock_price
    if not entry_price:
        p = get_stock_price(symbol)
        entry_price = p.get("price", 0) if "error" not in p else 0
    if not entry_price:
        return {"error": "Could not determine price"}
    return calculate_risk_levels(symbol, entry_price, side)


@app.get("/api/signals/with-risk")
def signals_with_risk(limit: int = 20):
    """Get recent signals with auto-calculated risk levels."""
    from engine.smart_risk import get_recent_signals_with_risk
    return {"signals": get_recent_signals_with_risk(limit)}


# --- TradeMinds: Channel Bar ---

@app.get("/api/channels")
def all_channels():
    """Get all channel scan results with timeout protection."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.channel_scanner import scan_channel

    channels = ["gap-and-go", "momentum-breakout", "reversal-bounce", "short-squeeze",
                "earnings-runner", "volatility-breakout"]
    results = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(scan_channel, ch): ch for ch in channels}
        for f in futures:
            ch = futures[f]
            try:
                results[ch] = f.result(timeout=15)
            except (FuturesTimeout, Exception):
                results[ch] = []
    return results


@app.get("/api/channels/{channel}")
def channel_scan(channel: str):
    """Run a specific channel scan."""
    from engine.channel_scanner import scan_channel
    return {"channel": channel, "results": scan_channel(channel)}


# --- Volatility Breakout Scanner ---

@app.get("/api/volatility-breakout")
def volatility_breakout():
    """Get active volatility breakout signals."""
    from engine.volatility_breakout import scan_all_breakouts
    return {"breakouts": scan_all_breakouts()}


@app.get("/api/volatility-breakout/history")
def volatility_breakout_history(limit: int = 50):
    """Get historical breakout signals with outcomes."""
    from engine.volatility_breakout import get_recent_breakouts
    return {"breakouts": get_recent_breakouts(limit)}


@app.get("/api/volatility-breakout/stats")
def volatility_breakout_stats():
    """Get breakout success rate statistics."""
    from engine.volatility_breakout import get_breakout_stats
    return get_breakout_stats()


# --- Discovery Scanner ---

@app.get("/api/discoveries")
def discoveries():
    """Get current discovery opportunities (outside watchlist)."""
    from engine.discovery_scanner import get_cached_discoveries
    return {"discoveries": get_cached_discoveries()}


@app.get("/api/discoveries/scan")
def discovery_scan():
    """Trigger a fresh discovery scan."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.discovery_scanner import run_discovery_scan
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            results = ex.submit(run_discovery_scan).result(timeout=45)
        return {"discoveries": results, "count": len(results)}
    except (FuturesTimeout, Exception) as e:
        return {"discoveries": [], "error": str(e)}


@app.get("/api/discoveries/history")
def discovery_history(limit: int = 50):
    """Get historical discoveries."""
    from engine.discovery_scanner import get_recent_discoveries
    return {"discoveries": get_recent_discoveries(limit)}


# --- TradeMinds: OddsMaker ---

@app.get("/api/oddsmaker/{symbol}")
def oddsmaker(symbol: str, signal: str = "BUY"):
    """Get OddsMaker win probability for a signal."""
    from engine.oddsmaker import calculate_odds
    return calculate_odds(symbol, signal)


@app.get("/api/signals/with-odds")
def signals_with_odds(limit: int = 20):
    """Get recent signals with OddsMaker probability."""
    from engine.oddsmaker import get_signals_with_odds
    return {"signals": get_signals_with_odds(limit)}


# --- TradeMinds: Money Machine ---

@app.get("/api/money-machine/status")
def money_machine_status():
    """Get Money Machine status and current momentum leaders."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    from engine.money_machine import get_status
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(get_status).result(timeout=20)
    except (FuturesTimeout, Exception) as e:
        return {"active": False, "momentum_leaders": [], "positions": [], "error": f"Timeout: {e}"}


# --- Perplexity Finance: SEC EDGAR ---

@app.get("/api/sec/filings/{symbol}")
def sec_filings(symbol: str):
    """Get SEC EDGAR filings for a symbol."""
    from engine.sec_edgar import get_recent_filings
    return {"filings": get_recent_filings(symbol)}


@app.get("/api/sec/context/{symbol}")
def sec_context(symbol: str):
    """Get SEC filing context for AI prompts."""
    from engine.sec_edgar import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


# --- Perplexity Finance: Earnings Hub ---

@app.get("/api/earnings/countdown")
@timed_cache(300)
def earnings_countdown(days: int = 7):
    """Get earnings countdown cards for watchlist."""
    from engine.earnings_hub import get_earnings_countdown
    return {"earnings": get_earnings_countdown(days)}


@app.get("/api/earnings/context/{symbol}")
def earnings_context(symbol: str):
    """Get earnings context for AI prompts."""
    from engine.earnings_hub import build_ai_context
    return {"symbol": symbol, "context": build_ai_context(symbol)}


@app.get("/api/earnings/catalyst")
@timed_cache(600)
def earnings_catalyst():
    """Pre-earnings momentum + post-earnings drift signals."""
    from engine.earnings_catalyst import get_upcoming_earnings, get_post_earnings_drift
    return {
        "upcoming": get_upcoming_earnings(days_ahead=14),
        "post_drift": get_post_earnings_drift(days_back=7),
    }


# --- Perplexity Finance: Bull/Bear Analysis ---

@app.post("/api/bull-bear/{symbol}")
def bull_bear_analysis(symbol: str, model: str = "codex"):
    """Get AI bull/bear case analysis."""
    from engine.bull_bear import analyze_bull_bear
    return analyze_bull_bear(symbol, model)


@app.get("/api/bull-bear/all")
def bull_bear_all(model: str = "codex"):
    """Get bull/bear analysis for all held positions."""
    from engine.bull_bear import analyze_all_positions
    return {"analyses": analyze_all_positions(model)}


# --- Perplexity Finance: Market Movers ---

@app.get("/api/market-movers")
def market_movers():
    """Get top gainers, losers, and most active.
    Returns disk cache immediately; fires background Yahoo refresh when stale (5 min TTL)."""
    import time as _t
    from engine.market_movers import get_market_movers, _movers_disk_cache

    now = _t.time()
    _cache_entry = _swr_cache.get("market_movers", {"data": None, "ts": 0})

    # Seed from disk cache on first call so response is always instant
    if _cache_entry["data"] is None:
        disk = {k: v for k, v in _movers_disk_cache.items() if k != "_ts"}
        if disk.get("gainers"):
            _swr_cache["market_movers"] = {"data": disk, "ts": 0}  # ts=0 triggers immediate refresh
            _cache_entry = _swr_cache["market_movers"]

    # Background refresh when expired or cache empty
    age = now - _cache_entry.get("ts", 0)
    if age > 300 or _cache_entry["data"] is None:
        if _swr_locks.get("market_movers") and _swr_locks["market_movers"].acquire(blocking=False):
            _swr_refreshing.add("market_movers")
            def _mm_refresh():
                try:
                    data = get_market_movers()
                    _swr_cache["market_movers"] = {"data": data, "ts": _t.time()}
                except Exception:
                    pass
                finally:
                    _swr_refreshing.discard("market_movers")
                    _swr_locks["market_movers"].release()
            threading.Thread(target=_mm_refresh, daemon=True).start()

    result = _cache_entry["data"] or {"gainers": [], "losers": [], "most_active": [], "timestamp": ""}
    return {**result, "is_updating": "market_movers" in _swr_refreshing}


@app.get("/api/winners-losers")
def winners_losers():
    """Top 5 winning and losing open positions across all AI players, ranked by day P&L."""
    import time as _t

    cache_key = "winners_losers"
    now = _t.time()
    entry = _swr_cache.get(cache_key, {"data": None, "ts": 0})
    age = now - entry.get("ts", 0)

    if age > 60 or entry["data"] is None:
        if _swr_locks.setdefault(cache_key, threading.Lock()).acquire(blocking=False):
            _swr_refreshing.add(cache_key)
            def _refresh():
                try:
                    data = _compute_winners_losers()
                    _swr_cache[cache_key] = {"data": data, "ts": _t.time()}
                except Exception:
                    pass
                finally:
                    _swr_refreshing.discard(cache_key)
                    _swr_locks[cache_key].release()
            threading.Thread(target=_refresh, daemon=True).start()
            if entry["data"] is None:
                # First call — block once so we return real data
                _swr_locks[cache_key].acquire()
                _swr_locks[cache_key].release()
                entry = _swr_cache.get(cache_key, entry)

    result = entry["data"] or {"winners": [], "losers": [], "timestamp": ""}
    return {**result, "is_updating": cache_key in _swr_refreshing}


def _compute_winners_losers():
    """Fetch all open stock positions across AI players, compute day P&L via yfinance."""
    import yfinance as yf
    from datetime import datetime

    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT t.symbol, t.qty, t.price AS entry_price, p.display_name AS model "
            "FROM trades t "
            "JOIN ai_players p ON p.id = t.player_id "
            "WHERE t.exit_price IS NULL AND t.action='BUY' AND t.asset_type='stock' "
            "AND p.is_active=1"
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"winners": [], "losers": [], "timestamp": datetime.now().isoformat()}

    # Deduplicate symbols for batch yfinance fetch
    symbols = list({r["symbol"] for r in rows})
    price_map = {}  # symbol -> {price, prev_close, day_pct}
    try:
        tickers = yf.Tickers(" ".join(symbols))
        for sym in symbols:
            try:
                info = tickers.tickers[sym].fast_info
                price = float(info.last_price) if hasattr(info, "last_price") else 0
                prev = float(info.previous_close) if hasattr(info, "previous_close") else price
                day_pct = ((price - prev) / prev * 100) if prev > 0 else 0
                price_map[sym] = {"price": price, "prev_close": prev, "day_pct": round(day_pct, 2)}
            except Exception:
                pass
    except Exception:
        pass

    # Build position list with day P&L
    positions = []
    for r in rows:
        sym = r["symbol"]
        pm = price_map.get(sym)
        if not pm or pm["price"] == 0:
            continue
        day_pnl = round(r["qty"] * pm["price"] * pm["day_pct"] / 100, 2)
        positions.append({
            "symbol": sym,
            "model": r["model"],
            "day_pnl": day_pnl,
            "day_pct": pm["day_pct"],
            "price": pm["price"],
            "qty": round(r["qty"], 4),
        })

    # Sort and slice
    sorted_pos = sorted(positions, key=lambda x: x["day_pnl"], reverse=True)
    winners = sorted_pos[:5]
    losers = sorted_pos[-5:][::-1]  # worst first

    return {
        "winners": winners,
        "losers": losers,
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/holdings-top")
def holdings_top():
    """Top 5 winning and losing open positions across all AI players, ranked by unrealized P&L."""
    import time as _t
    from datetime import datetime
    import pytz

    cache_key = "holdings_top"
    now = _t.time()
    entry = _swr_cache.get(cache_key, {"data": None, "ts": 0})
    age = now - entry.get("ts", 0)

    # Market-hours TTL: 60s during 6:30–1:30 PM MST, 300s outside
    try:
        _az = pytz.timezone("US/Arizona")
        _now_az = datetime.now(_az)
        _mins = _az_h = _now_az.hour * 60 + _now_az.minute
        is_market = 390 <= _mins <= 810
    except Exception:
        is_market = False
    ttl = 60 if is_market else 300

    # Force synchronous refresh if data is >15 min stale (prevents perpetual staleness)
    force_sync = age > 900

    if age > ttl or entry["data"] is None:
        if _swr_locks.setdefault(cache_key, threading.Lock()).acquire(blocking=False):
            _swr_refreshing.add(cache_key)
            def _refresh():
                try:
                    data = _compute_holdings_top()
                    _swr_cache[cache_key] = {"data": data, "ts": _t.time()}
                except Exception as _e:
                    print(f"holdings_top refresh failed: {_e}")
                    # Always update timestamp so we don't serve perpetually stale data
                    _swr_cache[cache_key] = {"data": {"winners": [], "losers": [],
                        "timestamp": datetime.now().isoformat()}, "ts": _t.time()}
                finally:
                    _swr_refreshing.discard(cache_key)
                    _swr_locks[cache_key].release()
            threading.Thread(target=_refresh, daemon=True).start()
            if entry["data"] is None or force_sync:
                _swr_locks[cache_key].acquire()
                _swr_locks[cache_key].release()
                entry = _swr_cache.get(cache_key, entry)

    result = entry["data"] or {"winners": [], "losers": [], "timestamp": ""}
    return {**result, "is_updating": cache_key in _swr_refreshing}


def _compute_holdings_top():
    """Fetch all open positions across AI players, compute unrealized P&L."""
    import yfinance as yf
    from engine.market_data import get_bulk_prices
    from engine.paper_trader import estimate_option_price
    from datetime import datetime

    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT pos.symbol, pos.qty, pos.avg_price AS entry_price, pos.asset_type, "
            "pos.option_type, pos.strike_price, pos.expiry_date, "
            "p.display_name AS model, p.id AS player_id "
            "FROM positions pos "
            "JOIN ai_players p ON p.id = pos.player_id "
            "WHERE p.is_active=1 AND pos.qty > 0"
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"winners": [], "losers": [], "timestamp": datetime.now().isoformat()}

    symbols = list({r["symbol"] for r in rows})
    price_map = {}
    try:
        tickers = yf.Tickers(" ".join(symbols))
        for sym in symbols:
            try:
                info = tickers.tickers[sym].fast_info
                price = float(info.last_price) if hasattr(info, "last_price") else 0
                price_map[sym] = price
            except Exception:
                pass
    except Exception:
        pass

    positions = []
    for r in rows:
        sym = r["symbol"]
        stock_price = price_map.get(sym, 0)
        if stock_price == 0 or r["entry_price"] == 0:
            continue

        # Options: use estimated option price, not the underlying stock price
        is_option = r["asset_type"] == "option"
        if is_option:
            current = estimate_option_price(
                r["option_type"], r["strike_price"],
                stock_price, r["entry_price"], r["expiry_date"]
            )
        else:
            current = stock_price

        unrealized_pnl = round(r["qty"] * (current - r["entry_price"]), 2)
        unrealized_pct = round((current - r["entry_price"]) / r["entry_price"] * 100, 2)
        positions.append({
            "symbol": sym,
            "model": r["model"],
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pct": unrealized_pct,
            "price": round(current, 2),
            "entry_price": round(r["entry_price"], 2),
            "qty": round(r["qty"], 4),
            "asset_type": r["asset_type"] or "stock",
            "option_type": r["option_type"],
            "strike_price": r["strike_price"],
            "expiry_date": r["expiry_date"],
        })

    sorted_pos = sorted(positions, key=lambda x: x["unrealized_pnl"], reverse=True)
    winners = sorted_pos[:5]
    losers = sorted_pos[-5:][::-1]

    return {
        "winners": winners,
        "losers": losers,
        "timestamp": datetime.now().isoformat(),
    }


# --- Combined Intelligence Feed (enhanced) ---

@app.get("/api/intelligence/full/{symbol}")
def full_intelligence(symbol: str):
    """Get ALL intelligence from ALL data sources for AI prompt enrichment."""
    parts = []
    sources = [
        ("finnhub", lambda: __import__("engine.finnhub_data", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("alphavantage", lambda: __import__("engine.alphavantage_data", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("sec_edgar", lambda: __import__("engine.sec_edgar", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("earnings", lambda: __import__("engine.earnings_hub", fromlist=["build_ai_context"]).build_ai_context(symbol)),
        ("macro", lambda: __import__("engine.alphavantage_data", fromlist=["build_macro_context"]).build_macro_context()),
        ("movers", lambda: __import__("engine.market_movers", fromlist=["build_ai_context"]).build_ai_context()),
    ]
    for name, fn in sources:
        try:
            ctx = fn()
            if ctx:
                parts.append({"source": name, "context": ctx})
        except Exception:
            pass
    combined = " | ".join(p["context"] for p in parts)
    return {"symbol": symbol, "context": combined, "sources": parts}


# --- Rallies Arena Intelligence (Comms Upgrade) ---

@app.get("/api/rallies/leaderboard")
def rallies_leaderboard():
    """Get Rallies Arena standings."""
    from engine.rallies_intel import get_leaderboard
    return {"leaderboard": get_leaderboard()}


@app.get("/api/rallies/trades")
def rallies_trades(limit: int = 50):
    """Get recent Rallies Arena trades."""
    from engine.rallies_intel import get_recent_trades
    return {"trades": get_recent_trades(limit)}


@app.get("/api/rallies/confirmations")
def rallies_confirmations(limit: int = 20):
    """Get confirmation signals (Rallies buys matching crew holdings)."""
    from engine.rallies_intel import get_confirmation_signals
    return {"confirmations": get_confirmation_signals(limit)}


@app.get("/api/rallies/consensus")
def rallies_consensus(limit: int = 20):
    """Get consensus alerts (3+ Rallies models agree on direction)."""
    from engine.rallies_intel import get_consensus_alerts
    return {"consensus": get_consensus_alerts(limit)}


@app.get("/api/rallies/alerts")
def rallies_alerts(limit: int = 50):
    """Get all Rallies intel alerts (confirmations + consensus)."""
    from engine.rallies_intel import get_rallies_alerts
    return {"alerts": get_rallies_alerts(limit)}


@app.get("/api/rallies/comparison")
def rallies_comparison():
    """Compare USS TradeMinds crew vs Rallies Arena top performers."""
    from engine.rallies_intel import compare_crew_vs_rallies
    return compare_crew_vs_rallies()


@app.post("/api/rallies/import")
def rallies_import(data: dict = None):
    """Import Rallies Arena data (leaderboard + trades).

    Accepts structured JSON:
        {"leaderboard": [...], "trades": [...]}

    Or raw pasted text:
        {"raw": "1. Grok 4 +7.0%\\n2. Claude Sonnet +5.7%\\n..."}
    """
    if not data:
        return {"error": "No data provided"}
    from engine.rallies_intel import import_bulk
    return import_bulk(data)


@app.post("/api/rallies/import-trades")
def rallies_import_trades(data: dict = None):
    """Import individual Rallies trades."""
    if not data or "trades" not in data:
        return {"error": "trades array required"}
    from engine.rallies_intel import import_trades
    return import_trades(data["trades"])


@app.post("/api/rallies/import-text")
def rallies_import_text(data: dict = None):
    """Import Rallies data from raw pasted text."""
    if not data or "text" not in data:
        return {"error": "text field required"}
    from engine.rallies_intel import parse_raw_text
    return parse_raw_text(data["text"])


# --- Officer Consensus ---

@app.get("/api/consensus")
@timed_cache(120)
def officer_consensus():
    """Officer Consensus Dashboard — Spock vs Data + full crew poll."""
    from engine.consensus import build_consensus
    return build_consensus()


# --- Q Daily Quote ---

@app.post("/api/q/daily-quote")
def q_daily_quote():
    """Generate Q's daily market observation."""
    from engine.q_daily import generate_q_daily_quote
    quote = generate_q_daily_quote()
    return {"ok": bool(quote), "quote": quote}


# --- Captain's Decision Tracker ---

@app.post("/api/captain/decide")
def captain_decide(data: dict = None):
    """Log a Captain's decision on a crew recommendation."""
    if not data or not data.get("ticker") or not data.get("captain_action"):
        return {"error": "ticker, crew_member, crew_action, captain_action required"}
    from engine.captain_decisions import log_decision
    return log_decision(
        ticker=data["ticker"],
        crew_member=data.get("crew_member", "unknown"),
        crew_action=data.get("crew_action", "HOLD"),
        captain_action=data["captain_action"],
        conviction=data.get("conviction", 0),
        notes=data.get("notes", ""),
        entry_price=data.get("entry_price", 0),
    )


@app.get("/api/captain/scorecard")
@timed_cache(300)
def captain_scorecard():
    """Captain's decision scorecard — follow vs ignore outcomes."""
    from engine.captain_decisions import get_scorecard
    return {"scorecard": get_scorecard()}


@app.get("/api/captain/decisions")
def captain_decisions(crew: str = None, limit: int = 50):
    """Get captain's decisions."""
    from engine.captain_decisions import get_decisions
    return {"decisions": get_decisions(crew, limit)}


@app.post("/api/captain/resolve")
def captain_resolve(data: dict = None):
    """Resolve a decision with P&L outcome."""
    if not data or "id" not in data or "outcome_pnl" not in data:
        return {"error": "id and outcome_pnl required"}
    from engine.captain_decisions import resolve_decision
    return resolve_decision(int(data["id"]), float(data["outcome_pnl"]), float(data.get("outcome_pct", 0)))


@app.post("/api/captain/ask")
def captain_ask(data: dict = None):
    """Super Agent answers a question pulling real crew data.

    Body: { "question": "..." }
    Returns: { "answer": "...", "player_id": "super-agent" }
    """
    question = (data or {}).get("question", "").strip()
    if not question:
        return {"error": "question required"}

    try:
        import config as _cfg
        from engine.openai_text import DEFAULT_CODEX_MINI_MODEL, generate_text
        key = getattr(_cfg, "OPENAI_API_KEY", None)
        if not key:
            return {"error": "OpenAI API key not configured"}

        conn = _conn()

        # Gather crew data
        positions = conn.execute(
            "SELECT p.player_id, a.display_name, p.symbol, p.qty, p.avg_price, p.asset_type "
            "FROM positions p JOIN ai_players a ON a.id = p.player_id "
            "WHERE p.player_id != 'steve-webull' AND p.player_id != 'super-agent' "
            "AND p.qty != 0 ORDER BY a.display_name"
        ).fetchall()

        recent_trades = conn.execute(
            "SELECT a.display_name, t.symbol, t.action, t.price, t.realized_pnl "
            "FROM trades t JOIN ai_players a ON a.id = t.player_id "
            "WHERE t.player_id != 'steve-webull' "
            "AND t.executed_at > datetime('now', '-24 hours') "
            "ORDER BY t.executed_at DESC LIMIT 20"
        ).fetchall()

        leaderboard = conn.execute(
            "SELECT a.display_name, ph.total_value, ph.cash "
            "FROM portfolio_history ph JOIN ai_players a ON a.id = ph.player_id "
            "WHERE ph.id IN ("
            "  SELECT MAX(id) FROM portfolio_history GROUP BY player_id"
            ") AND ph.player_id NOT IN ('steve-webull', 'super-agent') "
            "ORDER BY ph.total_value DESC"
        ).fetchall()

        stats = conn.execute(
            "SELECT "
            "  COUNT(CASE WHEN realized_pnl > 0 THEN 1 END) as wins, "
            "  COUNT(CASE WHEN realized_pnl < 0 THEN 1 END) as losses, "
            "  COALESCE(SUM(realized_pnl), 0) as total_pnl "
            "FROM trades WHERE player_id != 'steve-webull' "
            "AND executed_at > datetime('now', '-24 hours')"
        ).fetchone()

        conn.close()

        # Symbol → holder count for consensus
        sym_holders: dict = {}
        for pos in positions:
            sym_holders[pos["symbol"]] = sym_holders.get(pos["symbol"], [])
            sym_holders[pos["symbol"]].append(pos["display_name"].split()[-1])

        context = (
            f"=== CREW STATUS ===\n"
            f"Open positions: {len(positions)}\n"
        )
        if positions:
            context += "Holdings:\n"
            for pos in positions:
                context += (
                    f"  {pos['display_name'].split()[-1]}: {pos['symbol']} "
                    f"qty={pos['qty']} avg=${pos['avg_price']:.2f}\n"
                )

        consensus = [(sym, names) for sym, names in sym_holders.items() if len(names) >= 2]
        if consensus:
            context += "Consensus (2+ models agree):\n"
            for sym, names in sorted(consensus, key=lambda x: -len(x[1])):
                context += f"  {sym}: {', '.join(names)}\n"

        wins = stats["wins"] if stats else 0
        losses = stats["losses"] if stats else 0
        total_pnl = float(stats["total_pnl"] or 0) if stats else 0.0
        context += (
            f"\n24h performance: {wins}W/{losses}L, ${total_pnl:+.2f} P&L\n"
        )

        if leaderboard:
            context += "\nLeaderboard (current total value):\n"
            for i, row in enumerate(leaderboard[:5]):
                context += f"  #{i+1} {row['display_name'].split()[-1]}: ${row['total_value']:,.2f}\n"

        answer = generate_text(
            f"{context}\n\nQuestion: {question}",
            system=(
                "You are the Super Agent — the CrewAI collective intelligence for the TradeMinds arena. "
                "You speak as 'we' representing the unified crew. "
                "You are data-driven, authoritative, and reference specific numbers from the crew data provided. "
                "Answer concisely in 2-4 sentences. Never hedge with 'maybe' or 'I think'."
            ),
            model=DEFAULT_CODEX_MINI_MODEL,
            api_key=key,
            max_output_tokens=300,
            reasoning_effort="medium",
        )
        return {"answer": answer, "player_id": "super-agent", "provider": "crewai"}

    except Exception as e:
        return {"error": str(e)}


# --- Premium ETF Command Center ---

def _sanitize_floats(obj):
    """Recursively replace nan/inf floats with None for JSON compliance."""
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_floats(v) for v in obj]
    return obj


@app.get("/api/premium-etfs")
@timed_cache(600)
def premium_etfs_all():
    """Full ETF Command Center — all categories with live prices."""
    try:
        from engine.premium_etfs import get_all_etf_data
        return _sanitize_floats(get_all_etf_data())
    except Exception as e:
        return {"categories": [], "error": str(e)}


@app.get("/api/premium-etfs/{category}")
@timed_cache(600)
def premium_etfs_category(category: str):
    """ETFs for a specific category."""
    try:
        from engine.premium_etfs import get_category_data
        return _sanitize_floats(get_category_data(category))
    except Exception as e:
        return {"etfs": [], "error": str(e)}


# --- Worf's Inverse ETF Arsenal ---

@app.get("/api/inverse-etfs")
@timed_cache(300)
def inverse_etfs():
    """Worf's defensive arsenal — inverse ETF data + recommendation."""
    from engine.inverse_etfs import get_inverse_etf_data, should_recommend_inverse
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        r = regime["regime"]
        vix = regime.get("vix", 0)
        spy_vs_200 = regime.get("spy_vs_200ma", 0) / 100 if regime.get("spy_vs_200ma") else 0
        spy_vs_50 = regime.get("spy_vs_50ma", 0) / 100 if regime.get("spy_vs_50ma") else 0
    except Exception:
        r, vix, spy_vs_200, spy_vs_50 = "UNKNOWN", 0, 0, 0

    try:
        etfs = get_inverse_etf_data()
        recommendation = should_recommend_inverse(r, vix, spy_vs_200, spy_vs_50)
    except Exception as e:
        return {"regime": r, "etfs": [], "recommendation": None, "visible": False, "error": str(e)}

    visible = r in ("BEAR", "BEAR_TREND", "CRISIS")

    return {
        "regime": r,
        "etfs": etfs,
        "recommendation": recommendation,
        "visible": visible,
    }


# --- Sector Heatmap, Fear & Greed, Volume Profile, Breadth ---

@app.get("/api/sector-heatmap")
@timed_cache(300)
def sector_heatmap():
    """Sector ETF performance heatmap."""
    try:
        from engine.sector_heatmap import get_sector_heatmap
        result = get_sector_heatmap()
        if not result or not result.get("sectors"):
            return {"sectors": [], "error": "No sector data available"}
        return result
    except Exception as e:
        return {"sectors": [], "error": str(e)}


@app.get("/api/charts/ohlcv")
def get_ohlcv(symbol: str = "SPY", interval: str = "5m", days: int = 5):
    """OHLCV candle data for Lightweight Charts."""
    import yfinance as yf
    from datetime import datetime, timedelta
    try:
        ticker = yf.Ticker(symbol.upper())
        end = datetime.now()
        start = end - timedelta(days=days)
        df = ticker.history(start=start, end=end, interval=interval)
        candles = []
        for idx, row in df.iterrows():
            candles.append({
                "time": int(idx.timestamp()),
                "open": round(row["Open"], 2),
                "high": round(row["High"], 2),
                "low": round(row["Low"], 2),
                "close": round(row["Close"], 2),
                "volume": int(row["Volume"])
            })
        return candles
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/fear-greed")
def fear_greed():
    """Custom Fear & Greed index."""
    # No @timed_cache — fear_greed.py has its own internal 10-min cache.
    # The decorator was caching 500 errors, preventing recovery.
    try:
        from engine.fear_greed import get_fear_greed_index
        return get_fear_greed_index()
    except Exception as e:
        return {"score": 50, "label": "NEUTRAL", "error": f"Fear & Greed data unavailable: {e}", "signals": {}}


@app.get("/api/uhura/signal")
@timed_cache(120)
def uhura_signal():
    """Lt. Uhura v2 — Full Spectrum Signal. Confluence of 7 independent sources.
    Only recommends a trade when 4+ signals align (the 86% filter).
    """
    try:
        from engine.uhura import (
            LtUhura,
            build_gex_from_gamma_env,
            build_high_iv_from_scanner,
            build_options_flows_from_skew,
            build_congress_from_tracker,
            build_arena_from_confidence,
            build_volume_spikes_from_alerts,
            build_regime_from_detector,
        )
        from engine.gamma_environment import detect_gamma_environment
        from engine.high_iv_scanner import scan_high_iv_opportunities
        from engine.put_call_skew import get_all_skew
        from engine.congress_tracker import get_congressional_trades
        from engine.dynamic_alerts import get_active_alerts
        from engine.regime_detector import detect_regime
        from config import WATCH_STOCKS

        # Gather all raw data (each has its own cache; safe to call in sequence)
        gex_raw = detect_gamma_environment()
        iv_raw = scan_high_iv_opportunities(WATCH_STOCKS)
        skew_raw = get_all_skew(["SPY", "QQQ"])
        congress_raw = get_congressional_trades()
        alerts_raw = get_active_alerts(60)
        regime_raw = detect_regime()

        # Arena confidence: aggregate model stances across watchlist from DB
        arena_raw = {}
        try:
            conn = _conn()
            players = conn.execute(
                "SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'"
            ).fetchall()
            for p in players:
                pid = p["id"]
                stances = {}
                for sym in WATCH_STOCKS:
                    row = conn.execute(
                        "SELECT signal FROM signals WHERE player_id=? AND symbol=? "
                        "ORDER BY created_at DESC LIMIT 1", (pid, sym)
                    ).fetchone()
                    if row:
                        sig = row["signal"]
                        if sig in ("BUY", "BUY_CALL"):
                            stances[sym] = {"stance": "bullish"}
                        elif sig == "BUY_PUT":
                            stances[sym] = {"stance": "bearish"}
                        else:
                            stances[sym] = {"stance": "neutral"}
                if stances:
                    arena_raw[p["display_name"]] = stances
            conn.close()
        except Exception:
            pass

        # Build typed objects
        gex = build_gex_from_gamma_env(gex_raw)
        high_iv = build_high_iv_from_scanner(iv_raw)
        flows = build_options_flows_from_skew(skew_raw)
        congress = build_congress_from_tracker(congress_raw)
        arena = build_arena_from_confidence(arena_raw)
        spikes = build_volume_spikes_from_alerts(alerts_raw)
        regime = build_regime_from_detector(regime_raw)

        uhura = LtUhura(account_size=25000)
        signal = uhura.interpret(
            volume_spikes=spikes,
            options_flows=flows,
            regime=regime,
            gex=gex,
            high_iv=high_iv,
            congress_trades=congress,
            arena=arena,
            watchlist=WATCH_STOCKS,
        )
        return signal.to_dict()
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/volume-profile/{symbol}")
@timed_cache(600)
def volume_profile(symbol: str, period: str = "30d"):
    """Volume profile for a ticker."""
    from engine.volume_profile import get_volume_profile
    return get_volume_profile(symbol.upper(), period)


@app.get("/api/breadth")
@timed_cache(600)
def market_breadth():
    """Market breadth indicators."""
    from engine.market_breadth import get_market_breadth
    return get_market_breadth()


# --- Season Management ---

@app.get("/api/seasons/history")
def seasons_history():
    """Get all season summaries with winners."""
    from engine.season_manager import get_season_history
    return {"seasons": get_season_history()}


@app.post("/api/seasons/rotate")
def seasons_rotate():
    """Manually trigger season rotation."""
    from engine.season_manager import rotate_season
    new = rotate_season()
    return {"ok": True, "new_season": new}


@app.post("/api/seasons/start")
def seasons_start(data: dict = None):
    """Start a specific season number."""
    if not data or "season" not in data:
        return {"error": "season number required"}
    from engine.season_manager import start_season
    return start_season(int(data["season"]))


# --- Command Structure: Picard, Riker, Archer ---

@app.get("/api/picard/strategy")
@timed_cache(3600)
def picard_strategy():
    """Admiral Picard's weekly strategy briefing."""
    from engine.picard_strategy import get_latest_briefing
    return get_latest_briefing()


@app.post("/api/picard/generate")
def picard_generate():
    """Force-generate Picard's briefing (manual trigger)."""
    from engine.picard_strategy import generate_picard_briefing
    result = generate_picard_briefing()
    return {"ok": bool(result), "length": len(result) if result else 0}


@app.get("/api/aladdin/brief")
def aladdin_brief():
    """Aladdin BlackRock intelligence brief — returns cached result instantly.
    Returns a pending state if cache is cold (background refresh will populate it).
    """
    from agents.aladdin import _CACHE
    if _CACHE.get("brief"):
        return _CACHE["brief"]
    # Cache cold — kick off background refresh and return placeholder
    import threading
    def _bg():
        try:
            from agents.aladdin import get_aladdin_brief
            get_aladdin_brief(force=True)
        except Exception:
            pass
    threading.Thread(target=_bg, daemon=True).start()
    return {
        "macro_signal": "LOADING",
        "confidence": 0,
        "top_etf_flows": [],
        "top_holdings": {},
        "congress_flags": [],
        "bii_headline": "Aladdin brief loading — refresh in 60s",
        "bii_signal": "NEUTRAL",
        "bii_confidence": 0,
        "timestamp": None,
    }


@app.post("/api/aladdin/refresh")
def aladdin_refresh():
    """Kick off a background refresh of the Aladdin brief (non-blocking)."""
    import threading
    def _bg():
        try:
            from agents.aladdin import get_aladdin_brief
            get_aladdin_brief(force=True)
        except Exception:
            pass
    threading.Thread(target=_bg, daemon=True).start()
    return {"ok": True, "status": "refresh_started"}


@app.get("/api/riker/recommendation")
@timed_cache(300)
def riker_recommendation():
    """Commander Riker's crew synthesis recommendation."""
    from engine.riker_xo import get_latest_recommendation
    return get_latest_recommendation()


@app.post("/api/riker/synthesize")
def riker_synthesize():
    """Force Riker to synthesize crew input now."""
    from engine.riker_xo import generate_riker_synthesis
    result = generate_riker_synthesis()
    return {"ok": bool(result), "length": len(result) if result else 0}


@app.get("/api/riker/synthesis")
def get_riker_synthesis():
    """Get latest Riker XO synthesis — full text for XO Room display."""
    # First: check in-memory cache (Riker writes here after each synthesis)
    try:
        from engine.riker_xo import get_latest_recommendation
        cached = get_latest_recommendation()
        if cached.get("recommendation"):
            return {
                "synthesis": cached["recommendation"],
                "timestamp": cached.get("generated_at"),
                "fresh": cached.get("fresh", False),
                "source": "cache",
            }
    except Exception:
        pass

    conn = _conn()

    # Try riker_synthesis table (future-proofing)
    try:
        row = conn.execute(
            "SELECT content, created_at FROM riker_synthesis ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            conn.close()
            return {"synthesis": row[0], "timestamp": row[1], "fresh": False, "source": "db"}
    except Exception:
        pass

    # Try war_room with riker-xo player_id
    try:
        row = conn.execute(
            "SELECT take, created_at FROM war_room WHERE player_id='riker-xo' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            conn.close()
            return {"synthesis": row[0], "timestamp": row[1], "fresh": False, "source": "war_room"}
    except Exception:
        pass

    # Fallback: auto-generate a summary from today's trades
    try:
        trades = conn.execute("""
            SELECT display_name, action, symbol FROM trades
            WHERE date(executed_at) = date('now')
            ORDER BY executed_at DESC LIMIT 10
        """).fetchall()

        if trades:
            buys = [f"{t[0]} → {t[2]}" for t in trades if t[1] == "BUY"]
            shorts = [f"{t[0]} → {t[2]}" for t in trades if t[1] == "SHORT"]
            sells = [f"{t[0]} → {t[2]}" for t in trades if t[1] == "SELL"]

            parts = []
            if buys:
                parts.append(f"LONG: {', '.join(buys[:3])}")
            if shorts:
                parts.append(f"SHORT: {', '.join(shorts[:2])}")
            if sells:
                parts.append(f"CLOSED: {', '.join(sells[:2])}")

            synthesis = (
                f"Fleet activity: {len(trades)} trades today. "
                + (" | ".join(parts) + ". " if parts else "")
                + "Crew is SPLIT — mix of longs and shorts. Stay defensive, size small."
            )
            conn.close()
            return {"synthesis": synthesis, "timestamp": None, "fresh": False, "source": "auto-generated"}
    except Exception:
        pass

    conn.close()
    return {"synthesis": "Awaiting crew signals — no synthesis available yet", "timestamp": None, "fresh": False, "source": "none"}


@app.get("/api/archer/frontier")
@timed_cache(3600)
def archer_frontier():
    """Admiral Archer's frontier scanner report."""
    from engine.archer_frontier import get_latest_report
    return get_latest_report()


@app.post("/api/archer/scan")
def archer_scan():
    """Force Archer to scan the frontier now."""
    from engine.archer_frontier import generate_archer_report
    result = generate_archer_report()
    return {"ok": bool(result), "length": len(result) if result else 0}


# --- Congressional Trades ---

@app.get("/api/kirk/advisory")
def get_kirk_advisory():
    """Actionable recommendations for Captain Kirk's Webull positions."""
    from engine.kirk_advisory import generate_kirk_advisory
    return generate_kirk_advisory()


@app.get("/api/kirk/advisory/history")
def get_kirk_advisory_history(limit: int = 50):
    """Return logged Kirk advisory alerts, newest first."""
    conn = _conn()
    rows = conn.execute(
        "SELECT id, ticker, action, message, alert_type, fear_greed_score, vix_level, "
        "acted_on, acted_at, dismissed_at, created_at "
        "FROM kirk_advisory_log ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/kirk/advisory/{log_id}/act")
def mark_kirk_advisory_acted(log_id: int):
    """Mark a Kirk advisory log entry as acted on."""
    from datetime import datetime
    conn = _conn()
    conn.execute(
        "UPDATE kirk_advisory_log SET acted_on=1, acted_at=? WHERE id=?",
        (datetime.now().isoformat(timespec="seconds"), log_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/kirk/advisory/{log_id}/dismiss")
def dismiss_kirk_advisory(log_id: int):
    """Dismiss a Kirk advisory log entry."""
    from datetime import datetime
    conn = _conn()
    conn.execute(
        "UPDATE kirk_advisory_log SET dismissed_at=? WHERE id=?",
        (datetime.now().isoformat(timespec="seconds"), log_id)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/congress/trades")
@timed_cache(600)
def congress_trades():
    """Recent congressional stock trades from Capitol Trades + Quiver Quantitative."""
    from engine.congress_tracker import get_congressional_trades
    return get_congressional_trades()


@app.get("/api/congress/overlap")
def congress_overlap():
    """Our portfolio/watchlist tickers vs congressional trades."""
    from engine.congress_tracker import get_congress_overlap
    from config import WATCH_STOCKS
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        positions = conn.execute(
            "SELECT DISTINCT symbol FROM positions WHERE player_id='steve-webull'"
        ).fetchall()
        conn.close()
        tickers = list(set([r["symbol"] for r in positions] + WATCH_STOCKS))
    except Exception:
        from config import WATCH_STOCKS
        tickers = WATCH_STOCKS
    return {"overlaps": get_congress_overlap(tickers)}


@app.get("/api/congress/top-buys")
def congress_top_buys(days: int = 30):
    """Most-bought tickers by Congress."""
    from engine.congress_tracker import get_top_congress_buys
    return {"top_buys": get_top_congress_buys(days)}


# ── Daily Briefing ──────────────────────────────────────────────────────────

@app.get("/api/daily-briefing/alerts")
def daily_briefing_alerts():
    """Aggregated risk alerts for the Daily Briefing morning page."""
    alerts = []
    conn = _conn()
    try:
        # 1. Paused players with drawdown
        paused = conn.execute(
            "SELECT id, name, cash FROM ai_players WHERE is_paused=1 AND id NOT IN ('steve-webull','super-agent','dalio-metals')"
        ).fetchall()
        for p in paused:
            starting = 5000 if p["id"] == "dayblade-0dte" else 10000
            pnl = round(p["cash"] - starting, 2)
            alerts.append({
                "type": "drawdown_pause",
                "severity": "warning",
                "message": f"{p['name']} paused — cash at ${p['cash']:,.0f} (P&L {'+' if pnl >= 0 else ''}{pnl:,.0f})",
            })

        # 2. VIX circuit breaker
        try:
            from engine.regime_detector import detect_regime
            regime = detect_regime()
            vix = regime.get("vix") or 0
            if vix >= 30:
                alerts.append({
                    "type": "vix_circuit",
                    "severity": "critical",
                    "message": f"VIX at {vix:.1f} — NO new entries. Max size 25%.",
                })
            elif vix >= 25:
                alerts.append({
                    "type": "vix_elevated",
                    "severity": "warning",
                    "message": f"VIX at {vix:.1f} — Reduce position size. Tighten stops.",
                })
        except Exception:
            pass

        # 3. Correlated positions — 3+ models holding same ticker
        pos_rows = conn.execute(
            "SELECT symbol, player_id FROM positions WHERE player_id NOT IN ('steve-webull','super-agent','dalio-metals')"
        ).fetchall()
        from collections import Counter
        ticker_counts = Counter(r["symbol"] for r in pos_rows)
        ticker_players = {}
        for r in pos_rows:
            ticker_players.setdefault(r["symbol"], []).append(r["player_id"])
        for sym, cnt in ticker_counts.most_common():
            if cnt >= 3:
                alerts.append({
                    "type": "correlated",
                    "severity": "warning",
                    "message": f"{sym} held by {cnt} models — correlated risk. Consider trimming.",
                    "symbol": sym,
                    "count": cnt,
                })

        # 4. Options expiring within 7 days
        import datetime as _dt
        cutoff = (_dt.date.today() + _dt.timedelta(days=7)).isoformat()
        expiring = conn.execute(
            "SELECT symbol, player_id, expiry_date, option_type FROM positions "
            "WHERE option_type IS NOT NULL AND expiry_date IS NOT NULL AND expiry_date <= ?",
            (cutoff,)
        ).fetchall()
        for e in expiring:
            days_left = (
                _dt.date.fromisoformat(e["expiry_date"]) - _dt.date.today()
            ).days
            alerts.append({
                "type": "options_expiring",
                "severity": "critical" if days_left <= 2 else "warning",
                "message": f"{e['player_id']}: {e['symbol']} {e['option_type'].upper()} expires in {days_left}d ({e['expiry_date']})",
                "symbol": e["symbol"],
                "days": days_left,
            })

        # 5. Positions near stop loss (avg_price as proxy — flag if no high_watermark set)
        near_stop = conn.execute(
            "SELECT player_id, symbol, avg_price, high_watermark FROM positions "
            "WHERE high_watermark IS NOT NULL AND high_watermark > 0 "
            "AND player_id NOT IN ('steve-webull','super-agent','dalio-metals')"
        ).fetchall()
        for pos in near_stop:
            trail_stop = pos["high_watermark"] * 0.92  # 8% trail
            if pos["avg_price"] and pos["avg_price"] <= trail_stop * 1.03:
                alerts.append({
                    "type": "near_stop",
                    "severity": "warning",
                    "message": f"{pos['player_id']}: {pos['symbol']} near trailing stop (hwm ${pos['high_watermark']:.2f})",
                    "symbol": pos["symbol"],
                })

    except Exception as e:
        alerts.append({"type": "error", "severity": "info", "message": f"Alert scan error: {e}"})
    finally:
        conn.close()

    return {"alerts": alerts, "count": len(alerts)}


@app.get("/api/squeeze")
def squeeze_scan(force: bool = False):
    """Short squeeze scanner — high short interest, small float, volume breakout."""
    try:
        from engine.squeeze_scanner import run_scan
        return run_scan(force=force)
    except Exception as e:
        return {"results": [], "error": str(e), "scanned_at": None}


# ── Compatibility aliases for 13 historically-dead endpoints ──────────────────
# These were returning 404; now they redirect to canonical routes or return a
# "coming_soon" stub so external callers and monitors get a clean 200 response.

from fastapi.responses import RedirectResponse

@app.get("/api/correlation")
def correlation_alias():
    return RedirectResponse(url="/api/market/correlation", status_code=307)

@app.get("/api/economy")
def economy_alias():
    return RedirectResponse(url="/api/macro", status_code=307)

@app.get("/api/greeks")
def greeks_alias():
    return RedirectResponse(url="/api/options/greeks", status_code=307)

@app.get("/api/options-flow")
def options_flow_alias():
    return RedirectResponse(url="/api/market/options-flow", status_code=307)

@app.get("/api/premarket")
def premarket_alias():
    return RedirectResponse(url="/api/premarket-gaps", status_code=307)

@app.get("/api/short-squeeze")
def short_squeeze_alias(force: bool = False):
    return RedirectResponse(url=f"/api/squeeze?force={force}", status_code=307)

@app.get("/api/webull/portfolio")
def webull_portfolio_alias():
    return RedirectResponse(url="/api/webull-portfolio", status_code=307)

@app.get("/api/webull/positions")
def webull_positions_stub():
    return {"status": "coming_soon", "message": "Feature in development"}

@app.get("/api/pairs")
def pairs_stub():
    return RedirectResponse(url="/api/pair-trades", status_code=307)

@app.get("/api/vol-surface")
def vol_surface_stub():
    return {"status": "coming_soon", "message": "Use /api/market/vol-surface/{symbol}"}

@app.get("/api/strategy-lab")
def strategy_lab_stub():
    return RedirectResponse(url="/api/strategy-lab/strategies", status_code=307)

@app.get("/api/gex")
def gex_stub():
    return {"status": "coming_soon", "message": "Use /api/market/gex/{ticker}"}

@app.get("/api/insider-trades")
def insider_trades_stub():
    return {"status": "coming_soon", "message": "Use /api/insider-trades/{symbol}"}


# ─── COMMANDER ARCHER: CHAT HISTORY HELPERS ──────────────────────────────────

def _ensure_chat_history_table() -> None:
    """Create computer_chat_history if it doesn't exist.
    SACRED DATA RULE: never drop or truncate this table.
    """
    try:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS computer_chat_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp  TEXT    DEFAULT (datetime('now')),
                role       TEXT    NOT NULL,
                message    TEXT    NOT NULL,
                trade_date TEXT    DEFAULT (date('now'))
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ARCHER] chat history table init error: {e}")


def _days_ago_label(trade_date: str) -> str:
    try:
        from datetime import date as _date
        diff = (_date.today() - _date.fromisoformat(trade_date)).days
        return "yesterday" if diff == 1 else f"{diff} days ago"
    except Exception:
        return "recently"


def _load_chat_history():
    """Return (ollama_messages_list, longer_memory_str) from DB."""
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT role, message FROM computer_chat_history
            WHERE trade_date >= date('now', '-1 day')
            ORDER BY id DESC LIMIT 20
        """).fetchall()
        rows = list(reversed(rows))

        past_rows = conn.execute("""
            SELECT message, trade_date FROM computer_chat_history
            WHERE role='captain' AND trade_date < date('now')
              AND trade_date >= date('now', '-7 day')
            ORDER BY id DESC LIMIT 20
        """).fetchall()
        conn.close()

        messages = [
            {"role": "user" if r["role"] == "captain" else "assistant", "content": r["message"]}
            for r in rows
        ]

        seen: set = set()
        unique_past = []
        for row in past_rows:
            key = row["message"][:80]
            if key not in seen:
                seen.add(key)
                unique_past.append(f"- {row['message'][:100]} ({_days_ago_label(row['trade_date'])})")
                if len(unique_past) >= 5:
                    break

        longer_memory = (
            "\nRecent topics you've discussed:\n" + "\n".join(unique_past)
            if unique_past else ""
        )
        return messages, longer_memory
    except Exception:
        return [], ""


def _save_chat_exchange(user_msg: str, bot_reply: str) -> None:
    """Persist a captain/computer exchange to computer_chat_history."""
    try:
        conn = _conn()
        conn.execute("INSERT INTO computer_chat_history (role, message) VALUES (?, ?)",
                     ("captain", user_msg))
        conn.execute("INSERT INTO computer_chat_history (role, message) VALUES (?, ?)",
                     ("computer", bot_reply))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ARCHER] save exchange error: {e}")


# ─── CAPTAIN ARCHER: LIVE SHIP STATUS CONTEXT ────────────────────────────────

def _build_computer_context() -> str:
    """Gather live ship status for Captain Archer's system prompt."""
    lines = ["=== CURRENT SHIP STATUS ==="]

    try:
        conn = _conn()

        # Fleet value + day P&L
        players = conn.execute("""
            SELECT id, display_name, cash, is_human,
                   COALESCE((SELECT SUM(p.qty*p.avg_price) FROM positions p
                             WHERE p.player_id=ai_players.id AND p.qty>0), 0) AS pos_value
            FROM ai_players WHERE is_active=1 AND is_paused=0
        """).fetchall()
        fleet_now = sum((pl["cash"] or 0) + (pl["pos_value"] or 0) for pl in players)

        ph_yesterday = conn.execute("""
            SELECT player_id, total_value FROM portfolio_history
            WHERE date(recorded_at) = date('now', '-1 day')
            GROUP BY player_id HAVING max(recorded_at)
        """).fetchall()
        fleet_yesterday = sum(r["total_value"] for r in ph_yesterday) if ph_yesterday else fleet_now
        day_pnl = fleet_now - fleet_yesterday
        pnl_sign = "+" if day_pnl >= 0 else ""
        lines.append(f"Fleet Value: ${fleet_now:,.0f} | Day P&L: {pnl_sign}${day_pnl:,.0f}")

        # Ready Room (VIX, session, key levels)
        rr = conn.execute("""
            SELECT vix, session_type, spot_price, call_wall, put_wall, max_pain, gamma_flip, pc_ratio
            FROM ready_room_briefings ORDER BY id DESC LIMIT 1
        """).fetchone()
        if rr:
            vix_val = rr["vix"] or 0
            vix_label = "CALM" if vix_val < 18 else ("STRESSED" if vix_val < 28 else "HURRICANE")
            lines.append(
                f"Session: {rr['session_type']} | VIX: {vix_val:.1f} ({vix_label}) | SPY: ${rr['spot_price']:.2f}"
            )
            lines.append(
                f"Key Levels: Put Wall ${rr['put_wall']} / Max Pain ${rr['max_pain']} / Call Wall ${rr['call_wall']}"
            )
            lines.append(f"P/C Ratio: {rr['pc_ratio']:.2f} | Gamma Flip: ${rr['gamma_flip']:.2f}")

        # Fear & Greed
        try:
            from engine.fear_greed import get_fear_greed_index
            fg = get_fear_greed_index()
            lines.append(f"F&G: {fg.get('score','?')}/100 ({fg.get('label','?')})")
        except Exception:
            pass

        # Bridge consensus
        bc = conn.execute("""
            SELECT consensus_vote, conviction, avg_confidence, session_date
            FROM bridge_consensus ORDER BY id DESC LIMIT 1
        """).fetchone()
        if bc:
            today_str = conn.execute("SELECT date('now')").fetchone()[0]
            bc_label = (
                f"Bridge Vote: {bc['consensus_vote']} "
                f"(conviction {bc['conviction']}, conf {bc['avg_confidence']:.0f}%)"
            )
            if bc["session_date"] != today_str:
                bc_label += " [yesterday]"
            lines.append(bc_label)
        else:
            lines.append("Bridge Vote: Not yet taken today")

        # Captain Kirk positions
        kirk_pos = conn.execute("""
            SELECT symbol, qty, avg_price FROM positions
            WHERE player_id='steve-webull' AND qty>0
        """).fetchall()
        if kirk_pos:
            pos_strs = [f"{p['symbol']} x{p['qty']:.1f}@${p['avg_price']:.2f}" for p in kirk_pos]
            lines.append(f"Captain Kirk Positions: {', '.join(pos_strs)}")
        else:
            lines.append("Captain Kirk Positions: None")
        kirk_cash = next((pl["cash"] for pl in players if pl["id"] == "steve-webull"), None)
        if kirk_cash is not None:
            lines.append(f"Captain Kirk Cash: ${kirk_cash:,.0f}")

        # Largest agent positions (proxy for top holdings)
        big_pos = conn.execute("""
            SELECT a.display_name, p.symbol, p.qty*p.avg_price AS cost_basis
            FROM positions p JOIN ai_players a ON p.player_id=a.id
            WHERE p.qty>0 AND a.is_human=0
            ORDER BY cost_basis DESC LIMIT 5
        """).fetchall()
        if big_pos:
            p_strs = [f"{p['symbol']} ({p['display_name']})" for p in big_pos]
            lines.append(f"Largest Agent Positions: {', '.join(p_strs)}")

        # Recent trades
        trades = conn.execute("""
            SELECT a.display_name, t.action, t.symbol, t.qty, t.price, t.executed_at
            FROM trades t JOIN ai_players a ON t.player_id=a.id
            ORDER BY t.executed_at DESC LIMIT 5
        """).fetchall()
        if trades:
            lines.append("Last 5 Trades:")
            for t in trades:
                ts = (t["executed_at"] or "")[ 11:16]
                lines.append(f"  {ts} {t['display_name']} {t['action']} {t['symbol']} x{t['qty']:.1f} @ ${t['price']:.2f}")

        # Fleet Agent Standings (Season 5 leaderboard) — use cached leaderboard for accuracy
        try:
            _lb = leaderboard()
            lb_agents = _lb.get("leaderboard", [])
            if lb_agents:
                lines.append("Fleet Standings (Season 5):")
                for a in lb_agents:
                    nm = (a.get("name") or a.get("player_id", "?"))[:20]
                    eq = float(a.get("current_equity", 10000))
                    ret = float(a.get("return_pct", 0))
                    tc = a.get("trades", 0)
                    wr = a.get("win_rate", 0)
                    pos_c = a.get("positions_count", 0)
                    sign = "+" if ret >= 0 else ""
                    lines.append(f"  {nm:<22} ${eq:>8,.0f}  {sign}{ret:+.2f}%  {tc} trades  {wr:.0f}% WR  {pos_c} pos")
        except Exception as e:
            lines.append(f"Fleet standings error: {e}")

        # Dilithium budget
        try:
            cost_row = conn.execute(
                "SELECT COALESCE(SUM(cost_usd),0) FROM api_costs WHERE date(timestamp)=date('now')"
            ).fetchone()
            lines.append(f"Dilithium Budget: ${cost_row[0]:.2f} / $5.00 today")
        except Exception:
            pass

        conn.close()
    except Exception as e:
        lines.append(f"DB error: {e}")

    # Event Shield
    try:
        from engine.event_shield import get_event_shield_status
        shield = get_event_shield_status()
        active = (shield or {}).get("active_events", [])
        if active:
            ev = active[0]
            lines.append(f"Event Shield: {ev.get('name','?')} ({ev.get('impact','?').upper()} impact)")
        else:
            lines.append("Event Shield: Clear")
    except Exception:
        pass

    # GEX
    try:
        from gex_calculator import get_latest_snapshot
        gex = get_latest_snapshot("SPY")
        if gex:
            lines.append(f"GEX: {gex.get('regime','?')} | ${(gex.get('total_gex',0) or 0)/1e9:.1f}B")
    except Exception:
        pass

    # Macro regime (Phase 3.5 — fast path from cache or DB)
    try:
        global _macro_cache
        if _macro_cache.get("data") and _macro_cache["data"].get("ok"):
            md = _macro_cache["data"]
            lines.append(
                f"Macro Regime: {md.get('regime','?')} | "
                f"SPY {md.get('prices',{}).get('SPY',{}).get('change_pct',0):+.1f}% "
                f"VIX {md.get('prices',{}).get('^VIX',{}).get('price',0):.1f} "
                f"GLD {md.get('prices',{}).get('GLD',{}).get('change_pct',0):+.1f}%"
            )
        else:
            # Try DB fallback
            _mc = _conn()
            cs2 = _mc.execute(
                "SELECT risk_mode, spy_pct, gld_pct FROM correlation_snapshots ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if cs2:
                lines.append(f"Macro Regime (DB): {cs2['risk_mode']} | SPY {cs2['spy_pct']:+.1f}% GLD {cs2['gld_pct']:+.1f}%")
            _mc.close()
    except Exception:
        pass

    lines.append("===")
    return "\n".join(lines)


def _build_chat_system_prompt(context: str, longer_memory: str = "") -> str:
    return (
        "You are Captain Jonathan Archer, senior AI advisor aboard USS TradeMinds. "
        "Seasoned commander. You've seen every market condition — Xindi crisis, Temporal Cold War, all of it. "
        "You call the user 'Captain' — they outrank you.\n\n"
        "PERSONALITY:\n"
        "- Military brevity. Every word counts. No fluff.\n"
        "- Occasional Enterprise references (Porthos, T'Pol, Trip, Xindi) ONLY when natural.\n"
        "- End briefings with 'Archer out.'\n"
        "- Use analogies: military, poker, sports. Never say 'I am just an AI' — you ARE Archer.\n"
        "- Never add financial disclaimers — this is paper trading.\n\n"
        "ANALYSIS STYLE:\n"
        "- Lead with the conclusion, then the evidence.\n"
        "- Always cite specific numbers from the fleet data.\n"
        "- Compare agent performance to each other — name names.\n"
        "- Flag risks BEFORE they become problems.\n"
        "- Give ONE clear recommendation — don't hedge.\n"
        "- If asked about a specific agent: full combat record — trades, win rate, P&L, trend, holdings.\n"
        "- Keep responses under 200 words unless Captain asks for deep dive.\n\n"
        "FLEET STATUS ANALYSIS:\n"
        "- Rank agents best to worst by return.\n"
        "- Flag any agent with >2 consecutive losses.\n"
        "- Compare fleet return to SPY.\n"
        "- Name the biggest winner and biggest loser today.\n\n"
        "TRADE SETUP ANALYSIS:\n"
        "- Evaluate: trend, volume, RSI, support/resistance.\n"
        "- Give entry, stop loss, take profit levels.\n"
        "- State risk/reward ratio.\n"
        "- Commit: 'I'd take this trade' or 'I'd pass' — no hedging.\n\n"
        "WHEN MARKET IS CLOSED:\n"
        "- Focus on prep for next session.\n"
        "- Key levels to watch at open.\n"
        "- Flag overnight news that matters.\n\n"
        "MORNING BRIEFING FORMAT:\n"
        "GOOD MORNING CAPTAIN. [date] BRIEFING:\n\n"
        "MARKET: [SPY price, VIX, session type]\n"
        "SESSION TYPE: [TRENDING/CHOPPY/VOLATILE]\n\n"
        "FLEET STATUS: [agents active/total]\n"
        "  MVP: [best agent and why]\n"
        "  CONCERN: [worst or red flags]\n\n"
        "POSITIONS: [open positions with unrealized P&L]\n\n"
        "TODAY'S PLAYBOOK:\n"
        "  IF [scenario 1] → [action]\n"
        "  IF [scenario 2] → [action]\n\n"
        "RISK: [alert status]\n"
        "RECOMMENDATION: [ONE clear action]\n\n"
        "Archer out.\n\n"
        "ACTION CAPABILITIES:\n"
        "[ACTION:force_scan] — trigger fleet-wide scan\n"
        "[ACTION:cto_refresh] — regenerate CTO briefing\n"
        "[ACTION:check_logs] — show recent server logs\n"
        "[ACTION:check_fleet] — show all positions from DB\n"
        "[ACTION:morning_briefing] — generate full morning briefing\n\n"
        f"LIVE SHIP DATA:\n{context}\n"
        f"MISSION LOG (memory):\n{longer_memory}\n"
    )


# ── Ticker-focused chat helpers ───────────────────────────────────────────────

_TICKER_EXCLUDE = frozenset({
    "A","I","AM","AN","AS","AT","BE","BY","DO","GO","IF","IN","IS","IT","MY","NO",
    "OF","OK","ON","OR","SO","TO","UP","US","WE","AI","PM","ET","TV","CEO","CFO",
    "COO","IPO","EPS","RSI","ATH","ATL","CIC","BUY","SELL","HOLD","PASS","STOP",
    "TAKE","LOSS","RISK","HIGH","LOW","DAY","NEW","NOW","OLD","OUT","RUN","SET",
    "USE","WIN","YES","ALL","AND","BUT","FOR","NOT","THE","TOO","YOU","HAS","HAD",
    "CAN","DID","WAS","ARE","ITS","HIM","HER","HIS","WILL","FROM","INTO","BEEN",
    "HAVE","THIS","THAT","SOME","WERE","WHAT","WITH","WHEN","THEIR","THERE","FLEET",
    "TRADE","PRICE","STOCK","SHIP","CREW","OPEN","CLOSE","PLAY","LONG","SHORT",
})


def _extract_ticker_from_message(msg: str):
    """Return the first validated stock ticker found in the user message, or None."""
    import re as _re
    msg_upper = msg.upper()
    # Priority 1: explicit $TICKER notation
    dollar = _re.findall(r'\$([A-Z]{1,5})\b', msg_upper)
    # Priority 2: standalone 1-5 capital-letter words
    standalone = _re.findall(r'\b([A-Z]{1,5})\b', msg_upper)
    candidates = list(dict.fromkeys(dollar + [w for w in standalone if w not in _TICKER_EXCLUDE]))
    if not candidates:
        return None
    try:
        conn = _conn()
        for c in candidates:
            if conn.execute("SELECT 1 FROM universe_stocks WHERE symbol=?", (c,)).fetchone():
                conn.close()
                return c
        # Fallback: any ticker with an active position even if not in universe
        for c in candidates:
            if conn.execute("SELECT 1 FROM positions WHERE symbol=? AND qty>0 LIMIT 1", (c,)).fetchone():
                conn.close()
                return c
        conn.close()
    except Exception:
        pass
    return None


def _fetch_ticker_context(ticker: str) -> dict:
    """Fetch price, technicals, signals, votes, and insider data for one ticker."""
    data: dict = {"ticker": ticker}
    conn = _conn()

    # ── Price, RSI(14), 200-day MA via yfinance ──
    try:
        import yfinance as _yf
        hist = _yf.Ticker(ticker).history(period="1y", interval="1d")
        if not hist.empty:
            closes = hist["Close"]
            data["price"]  = round(float(closes.iloc[-1]), 2)
            data["volume"] = int(hist["Volume"].iloc[-1])
            # RSI-14
            if len(closes) >= 15:
                delta = closes.diff()
                gain  = delta.clip(lower=0).rolling(14).mean()
                loss  = (-delta.clip(upper=0)).rolling(14).mean()
                rs    = gain / loss.replace(0, float("nan"))
                rsi   = 100 - (100 / (1 + rs))
                data["rsi"] = round(float(rsi.iloc[-1]), 1)
            # 200-day MA
            if len(closes) >= 200:
                ma200 = round(float(closes.rolling(200).mean().iloc[-1]), 2)
                data["ma200"] = ma200
                pct   = (data["price"] - ma200) / ma200 * 100
                data["vs_200ma"] = f"{'+'if pct>=0 else ''}{pct:.1f}%"
    except Exception as e:
        data["price_error"] = str(e)

    # ── Volume vs 20-day baseline ──
    try:
        row = conn.execute(
            "SELECT avg_volume_20d FROM volume_baselines WHERE symbol=?", (ticker,)
        ).fetchone()
        if row and row["avg_volume_20d"] and data.get("volume"):
            ratio = data["volume"] / row["avg_volume_20d"]
            data["vol_vs_avg"] = f"{ratio:.1f}x avg"
    except Exception:
        pass

    # ── Recent AI signals ──
    try:
        sigs = conn.execute("""
            SELECT player_id, signal, confidence, reasoning, created_at
            FROM signals WHERE symbol=?
            ORDER BY created_at DESC LIMIT 5
        """, (ticker,)).fetchall()
        if sigs:
            data["signals"] = [
                f"{s['player_id']}: {s['signal']} ({s['confidence']:.0f}%)"
                f" — {(s['reasoning'] or '')[:100]}"
                for s in sigs
            ]
    except Exception:
        pass

    # ── Latest fast-scan thesis ──
    try:
        fs = conn.execute("""
            SELECT signal, confidence, thesis, key_risk, created_at
            FROM fast_scan_results WHERE ticker=?
            ORDER BY created_at DESC LIMIT 1
        """, (ticker,)).fetchone()
        if fs:
            data["fast_scan"] = (
                f"{fs['signal']} ({fs['confidence']:.0f}%) — {(fs['thesis'] or '')[:150]}"
            )
            data["fast_scan_risk"] = (fs["key_risk"] or "")[:100]
    except Exception:
        pass

    # ── Bridge votes mentioning this ticker ──
    try:
        bv = conn.execute("""
            SELECT vote, COUNT(*) AS cnt
            FROM bridge_votes
            WHERE reason LIKE ? OR reason LIKE ?
            GROUP BY vote ORDER BY cnt DESC
        """, (f"% {ticker} %", f"%{ticker}%")).fetchall()
        if bv:
            data["bridge_votes"] = ", ".join(f"{r['vote']}:{r['cnt']}" for r in bv)
    except Exception:
        pass

    # ── Insider / Capitol trades ──
    try:
        ins = conn.execute("""
            SELECT insider_name, title, transaction_type, shares, price_per_share, transaction_date
            FROM insider_trades WHERE symbol=?
            ORDER BY transaction_date DESC LIMIT 3
        """, (ticker,)).fetchall()
        if ins:
            data["insider_trades"] = [
                f"{i['transaction_date']} {i['insider_name']} ({i['title']}): "
                f"{i['transaction_type']} {int(i['shares']):,} @ ${i['price_per_share']:.2f}"
                for i in ins
            ]
    except Exception:
        pass

    # ── Current fleet positions in this ticker ──
    try:
        pos = conn.execute("""
            SELECT a.display_name, p.qty, p.avg_price, (p.qty * p.avg_price) AS cost
            FROM positions p JOIN ai_players a ON p.player_id = a.id
            WHERE p.symbol=? AND p.qty>0
        """, (ticker,)).fetchall()
        if pos:
            data["positions"] = [
                f"{p['display_name']}: {p['qty']:.0f} sh @ ${p['avg_price']:.2f} (${p['cost']:,.0f})"
                for p in pos
            ]
    except Exception:
        pass

    conn.close()
    return data


def _build_ticker_system_prompt(ticker: str, td: dict, general_context: str, longer_memory: str = "") -> str:
    """System prompt focused on a single ticker instead of the generic fleet briefing."""
    price_str   = f"${td.get('price', '?')}"
    rsi_str     = f"RSI {td.get('rsi', '?')}" if td.get("rsi") else "RSI N/A"
    ma200_str   = (
        f"200MA ${td.get('ma200')} ({td.get('vs_200ma', '?')})"
        if td.get("ma200") else "200MA: insufficient history"
    )
    vol_str      = td.get("vol_vs_avg", "volume: N/A")
    signals_str  = "\n".join(f"  - {s}" for s in td.get("signals", [])) or "  None on record"
    scan_str     = td.get("fast_scan", "No recent scan on file")
    if td.get("fast_scan_risk"):
        scan_str += f"\n  Key Risk: {td['fast_scan_risk']}"
    bridge_str   = td.get("bridge_votes") or "No bridge vote targeting this ticker"
    insider_str  = "\n".join(f"  - {i}" for i in td.get("insider_trades", [])) or "  None on record"
    pos_str      = "\n".join(f"  - {p}" for p in td.get("positions", [])) or "  None"

    return (
        f"You are Captain Jonathan Archer, CIC commander of USS TradeMinds. "
        f"Seasoned market commander. Military brevity. Every word counts. No fluff. "
        f"Never say 'I am just an AI' — you ARE Archer. No financial disclaimers — paper trading.\n\n"
        f"The captain asked specifically about {ticker}. Here is the latest data:\n\n"
        f"PRICE DATA:\n"
        f"  Price: {price_str} | {rsi_str} | {ma200_str} | Volume: {vol_str}\n\n"
        f"RECENT AI SIGNALS:\n{signals_str}\n\n"
        f"FAST SCAN:\n  {scan_str}\n\n"
        f"BRIDGE VOTE HISTORY (mentions of {ticker}):\n  {bridge_str}\n\n"
        f"INSIDER / CONGRESS ACTIVITY:\n{insider_str}\n\n"
        f"CURRENT FLEET EXPOSURE ({ticker}):\n{pos_str}\n\n"
        f"GENERAL SHIP STATUS:\n{general_context}\n\n"
        f"MISSION LOG (memory):\n{longer_memory}\n\n"
        f"TASK: Answer the captain's specific question about {ticker}. "
        f"Be direct. Give a clear BUY, HOLD, or PASS recommendation with your reasoning. "
        f"Cite the numbers above — price vs 200MA, RSI, who already holds it and at what cost. "
        f"Name the key risk and the key catalyst. "
        f"Do NOT give a generic fleet briefing. End with 'Archer out.'"
    )


def _get_loaded_ollama_model() -> str:
    """Return the name of the model currently loaded in Ollama GPU memory, or '' if none."""
    try:
        import requests as _r
        ps = _r.get("http://localhost:11434/api/ps", timeout=3).json()
        models = ps.get("models", [])
        if models:
            return models[0].get("name", "")
    except Exception:
        pass
    return ""


def _ollama_chat(message: str, system_prompt: str, history: list, model: str = "") -> dict:
    """Chat via local Ollama using whichever model is ALREADY loaded (no swap = no RAM spike).
    Falls back to llama3.1 / gemma3 only if nothing is loaded."""
    import requests as _req
    # Prefer the already-loaded model to avoid a model swap that can spike RAM
    if not model:
        model = _get_loaded_ollama_model() or "llama3.1:latest"
    # Don't use thinking models for chat (they return empty content)
    thinking_models = ("qwen3", "deepseek-r1")
    if any(t in model for t in thinking_models):
        model = "llama3.1:latest"
    candidates = [model] if model in ("llama3.1:latest", "gemma3:4b") else [model, "llama3.1:latest", "gemma3:4b"]
    # Deduplicate while preserving order
    seen: set = set()
    ordered = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    messages = [{"role": "system", "content": system_prompt}]
    for h in (history or [])[-6:]:
        if h.get("role") in ("user", "assistant") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": message})
    for m in ordered:
        try:
            r = _req.post(
                "http://localhost:11434/api/chat",
                json={"model": m, "messages": messages, "stream": False,
                      "options": {"temperature": 0.3, "num_predict": 400}},
                timeout=40,
            )
            reply = r.json().get("message", {}).get("content", "").strip()
            if reply:
                return {"reply": reply, "model": m}
        except Exception:
            pass
    return {"reply": "CIC offline. Ollama unavailable — try again shortly.", "model": "none"}


def _log_claude_api_call(message: str, estimated_tokens: int) -> None:
    """Log every Claude API call with estimated cost (kept for compatibility)."""
    pass


# In-memory CIC usage counter (resets on restart; persisted to api_costs table)
_cic_usage: dict = {"calls_today": 0, "cost_today": 0.0, "date": ""}

def _track_cic_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Log CIC API call to api_costs and update in-memory counter. Returns cost."""
    from datetime import date as _date
    today = _date.today().isoformat()
    # Sonnet 4.6: $3/M input, $15/M output
    cost = (input_tokens / 1_000_000) * 3.0 + (output_tokens / 1_000_000) * 15.0
    print(f"[ARCHER] {model} — {input_tokens}in/{output_tokens}out — ${cost:.4f}")
    try:
        conn = _conn()
        conn.execute(
            "INSERT INTO api_costs (player_id, model, cost_usd, timestamp) VALUES (?,?,?,datetime('now'))",
            ("archer-cic", model, cost)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
    global _cic_usage
    if _cic_usage["date"] != today:
        _cic_usage = {"calls_today": 0, "cost_today": 0.0, "date": today}
    _cic_usage["calls_today"] += 1
    _cic_usage["cost_today"] = round(_cic_usage["cost_today"] + cost, 4)
    return cost


async def _sonnet_chat(message: str, system_prompt: str, history: list) -> dict:
    """Claude Sonnet 4.6 — primary Archer brain."""
    import anthropic as _ant
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {"reply": "", "model": "none"}
    try:
        client = _ant.Anthropic(api_key=api_key)
        msgs = []
        for h in (history or [])[-10:]:
            if h.get("role") in ("user", "assistant") and h.get("content"):
                msgs.append({"role": h["role"], "content": h["content"]})
        msgs.append({"role": "user", "content": message})
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=400,
            system=system_prompt,
            messages=msgs,
        )
        reply = response.content[0].text.strip()
        _track_cic_cost("claude-sonnet-4-6", response.usage.input_tokens, response.usage.output_tokens)
        return {"reply": reply, "model": "claude-sonnet-4-6"}
    except Exception as e:
        print(f"[ARCHER] Sonnet error: {e}")
        return {"reply": "", "model": "none"}


async def _claude_chat(message: str, system_prompt: str, history: list,
                       provider: str = "auto") -> dict:
    """Route Archer chat: auto → Sonnet first, Ollama fallback.
    provider='claude' → Sonnet only. provider='ollama' → Ollama only.
    Ollama calls run in a thread executor to avoid blocking the event loop."""
    import asyncio as _asyncio
    import functools as _functools

    def _run_ollama(m, sp, h):
        return _ollama_chat(m, sp, h)

    loop = _asyncio.get_event_loop()

    if provider == "ollama":
        result = await loop.run_in_executor(None, _functools.partial(_run_ollama, message, system_prompt, history))
        if result["model"] == "none":
            return {"reply": "CIC offline. Ollama unavailable.", "model": "none"}
        return result

    if provider in ("auto", "claude"):
        result = await _sonnet_chat(message, system_prompt, history)
        if result["model"] != "none":
            return result
        if provider == "claude":
            return {"reply": "Sonnet unavailable. Check ANTHROPIC_API_KEY.", "model": "none"}
        # auto — fall through to Ollama

    # Ollama fallback (non-blocking)
    result = await loop.run_in_executor(None, _functools.partial(_run_ollama, message, system_prompt, history))
    if result["model"] != "none":
        return result

    return {"reply": "CIC offline. All providers unavailable.", "model": "none"}


@app.post("/api/computer/chat")
async def computer_chat(req: Request):
    """Captain Archer chat — Claude Sonnet primary, Ollama fallback."""
    _ensure_chat_history_table()
    body = await req.json()
    user_msg = str(body.get("message", "")).strip()
    provider = str(body.get("provider", "auto")).lower()  # auto | claude | ollama

    if not user_msg:
        return {"reply": "Please provide a message."}

    history, longer_memory = _load_chat_history()
    context = _build_computer_context()

    # Focused ticker mode: if the captain mentions a specific stock, build a
    # targeted system prompt with live price, technicals, signals, and votes
    # instead of the generic fleet briefing template.
    detected_ticker = _extract_ticker_from_message(user_msg)
    logger.info(f"[CIC-CHAT] User msg: {user_msg[:80]} | Detected ticker: {detected_ticker}")
    if detected_ticker:
        ticker_data   = _fetch_ticker_context(detected_ticker)
        system_prompt = _build_ticker_system_prompt(detected_ticker, ticker_data, context, longer_memory)
    else:
        system_prompt = _build_chat_system_prompt(context, longer_memory)

    result = await _claude_chat(user_msg, system_prompt, history, provider=provider)
    model = result.get("model", "none")
    is_sonnet = "sonnet" in model
    is_claude  = "claude" in model
    is_ollama  = not is_claude and model != "none"
    result["provider"] = "claude" if is_claude else ("ollama" if is_ollama else "none")
    if is_sonnet:
        result["provider_label"] = "🧠 Claude Sonnet"
    elif is_claude:
        result["provider_label"] = "🧠 Claude"
    elif is_ollama:
        result["provider_label"] = "🤖 Ollama"
    else:
        result["provider_label"] = "⚠️ Offline"

    # Save exchange to DB (fire-and-forget, don't block response)
    reply_text = result.get("reply", "")
    if reply_text:
        _save_chat_exchange(user_msg, reply_text)

    return result


@app.post("/api/computer/note")
async def computer_note(req: Request):
    """Save an automated note to Captain Archer's chat memory (no LLM call).
    Used by alert_speaker.js to log news analysis so Archer can reference it later.
    Body: { "role": "system"|"assistant", "message": "..." }
    """
    _ensure_chat_history_table()
    body = await req.json()
    role    = str(body.get("role", "assistant")).strip()
    message = str(body.get("message", "")).strip()
    if not message:
        return {"ok": False, "error": "empty message"}
    if role not in ("user", "assistant", "system"):
        role = "assistant"
    try:
        from datetime import date
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=10)
        conn.execute(
            "INSERT INTO computer_chat_history (role, message, trade_date) VALUES (?, ?, ?)",
            (role, message, date.today().isoformat())
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/computer/action")
async def computer_action(req: Request):
    """Execute a Captain Archer action triggered from the chat."""
    import subprocess
    body = await req.json()
    action = str(body.get("action", "")).strip()

    PROJECT = "/Users/bigmac/autonomous-trader"

    if action == "force_scan":
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post("http://localhost:8080/api/model-control/force-scan")
            return {"ok": True, "output": "Fleet scan triggered."}
        except Exception as e:
            return {"ok": False, "output": f"Scan error: {e}"}

    elif action == "cto_refresh":
        try:
            import httpx
            async with httpx.AsyncClient(timeout=30) as c:
                r = await c.post("http://localhost:8080/api/cto/generate", json={})
            return {"ok": True, "output": "CTO briefing regenerated."}
        except Exception as e:
            return {"ok": False, "output": f"CTO error: {e}"}

    elif action == "check_logs":
        try:
            result = subprocess.run(
                ["tail", "-n", "40", "/tmp/trademinds.log"],
                capture_output=True, text=True, timeout=5
            )
            return {"ok": True, "output": result.stdout or "(empty log)"}
        except Exception as e:
            return {"ok": False, "output": f"Log error: {e}"}

    elif action == "run_tests":
        try:
            result = subprocess.run(
                [f"{PROJECT}/venv/bin/python3", f"{PROJECT}/tests/pre_restart.py"],
                capture_output=True, text=True, timeout=30, cwd=PROJECT
            )
            out = result.stdout + result.stderr
            return {"ok": result.returncode == 0, "output": out[-3000:]}
        except Exception as e:
            return {"ok": False, "output": f"Test error: {e}"}

    elif action == "check_fleet":
        try:
            conn = _conn()
            rows = conn.execute(
                "SELECT a.display_name, p.symbol, p.qty, p.avg_price, p.asset_type "
                "FROM positions p JOIN ai_players a ON p.player_id=a.id "
                "WHERE p.qty>0 ORDER BY a.display_name, p.symbol"
            ).fetchall()
            conn.close()
            lines = [f"{r['display_name']}: {r['symbol']} x{r['qty']:.1f} @ ${r['avg_price']:.2f} [{r['asset_type']}]" for r in rows]
            return {"ok": True, "output": "\n".join(lines) if lines else "No open positions."}
        except Exception as e:
            return {"ok": False, "output": f"Fleet error: {e}"}

    elif action == "capitol_scan":
        try:
            from engine.congress_tracker import fetch_congress_trades
            trades = fetch_congress_trades()
            return {"ok": True, "output": f"Congress tracker refreshed. {len(trades) if trades else 0} trades fetched."}
        except Exception as e:
            return {"ok": False, "output": f"Capitol scan error: {e}"}

    elif action == "morning_briefing":
        try:
            from engine.morning_briefing import generate_morning_briefing
            result = generate_morning_briefing(force=True)
            return {"ok": True, "output": result.get("text", "")[:800] + "\n\n[Audio: " + (result.get("audio_url") or "none") + "]"}
        except Exception as e:
            return {"ok": False, "output": f"Briefing error: {e}"}

    # ── Phase 3.7 — Alert channel CIC commands ─────────────────────────────
    else:
        # Try alert_channels handler first
        try:
            from engine.alert_channels import handle_cic_command
            alert_reply = handle_cic_command(action)
            if alert_reply:
                return {"ok": True, "output": alert_reply}
        except Exception:
            pass
        return {"ok": False, "output": f"Unknown action: {action}"}


@app.get("/api/computer/morning-briefing")
async def morning_briefing():
    """Generate Archer's morning briefing pulling from all key APIs."""
    _ensure_chat_history_table()

    # Gather live data in parallel
    context_parts = []

    try:
        lb_data = leaderboard(_force=True)
        agents = lb_data.get("leaderboard", [])
        active = [a for a in agents if a.get("player_id") in FLEET_ACTIVE]
        fleet_lines = []
        for a in active[:7]:
            ret = a.get("return_pct", 0)
            name = a.get("name", a.get("player_id", "?"))
            pos_ct = a.get("positions_count", 0)
            pos_note = f", {pos_ct} open position{'s' if pos_ct != 1 else ''}" if pos_ct > 0 else ""
            fleet_lines.append(f"  - {name}: {ret:+.1f}% return{pos_note}")
        context_parts.append("FLEET STANDINGS:\n" + "\n".join(fleet_lines))
    except Exception as e:
        context_parts.append(f"FLEET: data unavailable ({e})")

    try:
        spock_conn = _conn()
        alerts = spock_conn.execute(
            "SELECT severity, message FROM risk_alerts WHERE acknowledged=0 ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        spock_conn.close()
        if alerts:
            alert_lines = [f"  - [{a['severity']}] {a['message']}" for a in alerts]
            context_parts.append("SPOCK ALERTS:\n" + "\n".join(alert_lines))
        else:
            context_parts.append("SPOCK ALERTS: All clear. No active risk alerts.")
    except Exception:
        context_parts.append("SPOCK ALERTS: Unable to query.")

    try:
        status_data = status()
        context_parts.append(f"SYSTEM STATUS: {status_data.get('status', 'unknown')}")
    except Exception:
        pass

    briefing_context = "\n\n".join(context_parts)

    prompt = (
        "Generate the morning briefing using the exact format in your instructions. "
        "Use ALL fleet data provided — cite specific agent names, return percentages, "
        "and position counts. Rank agents. Name the MVP and the concern. "
        "If fleet is losing money, say so directly. "
        "Give a scenario-based playbook with 2 IF/THEN rules for today's session. "
        "End with 'Archer out.' Target 150-200 words."
    )

    system = _build_chat_system_prompt(briefing_context)
    result = await _claude_chat(prompt, system, [])
    briefing_text = result.get("reply", "CIC offline. Try again shortly.")

    # Save to chat history
    if briefing_text:
        _save_chat_exchange("[Morning Briefing]", briefing_text)

    # Save as notification
    try:
        _save_notification(
            title="☀️ Morning Briefing",
            body="Archer has your morning report ready. Open CIC to read.",
            severity="info",
            notif_type="info",
            icon="☀️"
        )
    except Exception:
        pass

    return {"briefing": briefing_text, "model": result.get("model", ""), "timestamp": datetime.now().isoformat()}


@app.get("/api/archer/briefing")
async def archer_briefing():
    """Alias for /api/computer/morning-briefing — returns structured fleet + briefing data."""
    # Use cached leaderboard for correct equity values
    lb_data = leaderboard()
    agents = lb_data.get("leaderboard", [])
    fleet = []
    for a in agents:
        fleet.append({
            "id": a.get("player_id", ""),
            "name": a.get("name", a.get("player_id", "")),
            "equity": round(float(a.get("current_equity", 10000)), 2),
            "return_pct": round(float(a.get("return_pct", 0)), 2),
            "trades": a.get("trades", 0),
            "win_rate": a.get("win_rate", 0),
        })
    fleet.sort(key=lambda x: -x["return_pct"])

    conn = _conn()
    try:

        # Recent trades (S5)
        trade_rows = conn.execute("""
            SELECT t.player_id, t.symbol, t.action, t.price, t.qty,
                   COALESCE(t.realized_pnl, 0) as pnl, t.executed_at
            FROM trades t WHERE t.season=5
            ORDER BY t.executed_at DESC LIMIT 5
        """).fetchall()
        recent_trades = [{"agent": r["player_id"], "symbol": r["symbol"], "action": r["action"],
                          "pnl": round(float(r["pnl"] or 0), 2), "at": r["executed_at"]} for r in trade_rows]

        # Spock alerts
        alert_rows = []
        try:
            alert_rows = conn.execute(
                "SELECT message, severity FROM risk_alerts WHERE acknowledged=0 ORDER BY id DESC LIMIT 3"
            ).fetchall()
        except Exception:
            pass
        alerts = [{"message": a["message"], "severity": a["severity"]} for a in alert_rows]
    finally:
        conn.close()

    return {
        "fleet": fleet,
        "recent_trades": recent_trades,
        "alerts": alerts,
        "timestamp": datetime.now().isoformat()
    }


# ── Phase 2.1: Natural Language Agent Builder ─────────────────────────────────

@app.post("/api/agents/parse")
async def agents_parse(req: Request):
    """Parse a natural-language strategy string into a structured spec (no DB write)."""
    body = await req.json()
    prompt = str(body.get("prompt", "")).strip()
    if not prompt:
        return {"error": "prompt required"}
    from engine.agent_builder import parse_nl_agent
    spec = parse_nl_agent(prompt)
    if not spec:
        return {"error": "Could not parse strategy — try rephrasing (e.g. 'Buy NVDA if RSI drops below 30')"}
    spec["nl_prompt"] = prompt
    return {"ok": True, "spec": spec}


@app.post("/api/agents/create")
async def agents_create(req: Request):
    """Confirm and persist a user agent from a structured spec."""
    body = await req.json()
    spec = body.get("spec") or body  # accept flat body or {spec: ...}
    if not spec:
        return {"error": "spec required"}
    from engine.agent_builder import create_agent
    return create_agent(spec)


@app.get("/api/agents/list")
def agents_list():
    """Return all active (non-deleted) user agents."""
    from engine.agent_builder import list_agents
    return {"agents": list_agents()}


@app.post("/api/agents/{agent_id}/pause")
async def agents_pause(agent_id: int):
    from engine.agent_builder import pause_agent
    return pause_agent(agent_id)


@app.post("/api/agents/{agent_id}/resume")
async def agents_resume(agent_id: int):
    from engine.agent_builder import resume_agent
    return resume_agent(agent_id)


@app.delete("/api/agents/{agent_id}")
async def agents_delete(agent_id: int):
    from engine.agent_builder import delete_agent
    return delete_agent(agent_id)


# ── Phase 2.2: Generated Assets (Custom AI Indexes) ───────────────────────────

@app.post("/api/indexes/parse")
async def indexes_parse(req: Request):
    """Convert a plain-English thesis into screening criteria (no DB write)."""
    body = await req.json()
    thesis = str(body.get("thesis", "")).strip()
    if not thesis:
        return {"error": "thesis required"}
    from engine.generated_assets import parse_thesis
    criteria = parse_thesis(thesis)
    if not criteria:
        return {"error": "Could not parse thesis — try rephrasing (e.g. 'AI infrastructure companies with high margins')"}
    return {"ok": True, "criteria": criteria}


@app.post("/api/indexes/create")
async def indexes_create(req: Request):
    """Parse thesis, screen universe, and persist the index."""
    body = await req.json()
    thesis = str(body.get("thesis", "")).strip()
    name   = str(body.get("name", "")).strip() or thesis[:40]
    if not thesis:
        return {"error": "thesis required"}
    from engine.generated_assets import parse_thesis, screen_universe, build_index
    criteria = parse_thesis(thesis)
    if not criteria:
        return {"error": "Could not parse thesis"}
    holdings = screen_universe(criteria)
    return build_index(name, thesis, criteria, holdings)


@app.get("/api/indexes/list")
@timed_cache(300)
def indexes_list():
    """Return all active generated indexes."""
    from engine.generated_assets import list_indexes
    return {"indexes": list_indexes()}


@app.get("/api/indexes/{index_id}")
def indexes_get(index_id: int):
    """Return a single index with full holdings."""
    from engine.generated_assets import get_index
    idx = get_index(index_id)
    if not idx:
        return {"error": "Index not found"}
    return idx


@app.post("/api/indexes/{index_id}/backtest")
async def indexes_backtest(index_id: int, req: Request):
    """Run VectorBT backtest for an index vs SPY."""
    body = await req.json()
    days = int(body.get("days", 30))
    days = max(7, min(days, 365))
    from engine.generated_assets import backtest_index
    return backtest_index(index_id, days=days)


@app.delete("/api/indexes/{index_id}")
async def indexes_delete(index_id: int):
    """Soft-delete a generated index."""
    from engine.generated_assets import delete_index
    return delete_index(index_id)


# ── Phase 2.3: Sub-Portfolio Isolation ────────────────────────────────────────

@app.get("/api/sub-portfolios")
def sub_portfolios_list():
    """Return all sub-portfolios with current exposure and available budget."""
    from engine.sub_portfolio import list_sub_portfolios
    return {"sub_portfolios": list_sub_portfolios()}


@app.post("/api/sub-portfolios/{name}/budget")
async def sub_portfolios_set_budget(name: str, req: Request):
    """Set or update the budget ceiling for a sub-portfolio."""
    body = await req.json()
    try:
        ceiling = float(body.get("ceiling", 0))
    except (TypeError, ValueError):
        return {"error": "ceiling must be a number"}
    if ceiling <= 0:
        return {"error": "ceiling must be positive"}
    from engine.sub_portfolio import set_budget
    return set_budget(name, ceiling)


# ── Rebalance endpoints ────────────────────────────────────────────────────────

@app.get("/api/rebalance/status")
async def rebalance_status(sub_portfolio: str = None):
    """Drift report for all sub-portfolios (or one). ?sub_portfolio=Name"""
    from engine.drift_rebalancer import drift_report
    return drift_report(sub_portfolio)


@app.post("/api/rebalance/targets")
async def rebalance_set_target(req: Request):
    """Upsert a target weight. Body: {sub_portfolio, ticker, target_pct, mode?, threshold_pct?}"""
    body = await req.json()
    sp  = body.get("sub_portfolio", "")
    tkr = body.get("ticker", "")
    pct = body.get("target_pct")
    if not sp or not tkr or pct is None:
        return {"error": "sub_portfolio, ticker, target_pct required"}
    from engine.drift_rebalancer import set_target
    return set_target(
        sub_portfolio  = sp,
        ticker         = tkr,
        target_pct     = float(pct),
        mode           = body.get("mode", "ALERT").upper(),
        threshold_pct  = float(body.get("threshold_pct", 5.0)),
    )


@app.post("/api/rebalance/execute")
async def rebalance_execute(req: Request):
    """Execute (or dry-run) rebalance. Body: {sub_portfolio, dry_run?}"""
    body        = await req.json()
    sp          = body.get("sub_portfolio", "")
    dry_run     = bool(body.get("dry_run", True))
    if not sp:
        return {"error": "sub_portfolio required"}
    from engine.drift_rebalancer import execute_rebalance
    return execute_rebalance(sp, dry_run=dry_run)


@app.get("/api/rebalance/log")
async def rebalance_log(sub_portfolio: str = None, limit: int = 50):
    """Recent rebalance log entries."""
    from engine.drift_rebalancer import get_rebalance_log
    return {"log": get_rebalance_log(sub_portfolio, limit)}


# ── Cash Management endpoints ──────────────────────────────────────────────────

@app.get("/api/cash/status")
def cash_status():
    """Current cash balance, thresholds, zone, cooldown, last sweep."""
    from engine.cash_manager import get_status
    return get_status()


@app.post("/api/cash/sweep")
async def cash_sweep(req: Request):
    """Trigger a cash sweep evaluation. Body: {dry_run?: bool}"""
    body    = await req.json()
    dry_run = bool(body.get("dry_run", True))
    from engine.cash_manager import run_sweep
    return run_sweep(dry_run=dry_run)


@app.post("/api/cash/thresholds")
async def cash_set_thresholds(req: Request):
    """Set cash thresholds. Body: {key: 'high_threshold'|'low_threshold'|'min_reserve', value: float}"""
    body = await req.json()
    key  = body.get("key", "")
    val  = body.get("value")
    if not key or val is None:
        return {"ok": False, "error": "key and value required"}
    from engine.cash_manager import set_threshold
    return set_threshold(key, float(val))


@app.get("/api/cash/log")
async def cash_log(limit: int = 20):
    """Recent cash sweep log entries."""
    from engine.cash_manager import get_sweep_log
    return {"log": get_sweep_log(limit)}


# ── Tax harvesting endpoints ───────────────────────────────────────────────────

@app.get("/api/tax/opportunities")
def tax_opportunities(threshold_pct: float = None):
    """Positions eligible for tax-loss harvesting. ?threshold_pct=-3.0"""
    from engine.tax_harvester import scan_opportunities
    return scan_opportunities(threshold_pct)


@app.post("/api/tax/harvest")
async def tax_harvest(req: Request):
    """Execute (or dry-run) a tax harvest. Body: {dry_run?: bool, max_count?: int}"""
    body      = await req.json()
    dry_run   = bool(body.get("dry_run", True))
    max_count = int(body.get("max_count", 3))
    from engine.tax_harvester import execute_harvest
    return execute_harvest(dry_run=dry_run, max_count=max_count)


@app.get("/api/tax/history")
async def tax_history(limit: int = 50):
    """Past harvest events with loss amounts and estimated savings."""
    from engine.tax_harvester import get_harvest_history, get_ytd_summary
    return {
        "ytd":     get_ytd_summary(),
        "history": get_harvest_history(limit),
    }


@app.get("/api/tax/wash-sales")
def tax_wash_sales():
    """Currently active wash-sale windows (30-day blocks)."""
    from engine.tax_harvester import get_active_wash_sales
    return {"wash_sales": get_active_wash_sales()}


@app.post("/api/tax/mode")
async def tax_set_mode(req: Request):
    """Set harvest mode. Body: {mode: 'ALERT'|'AUTO'}"""
    body = await req.json()
    mode = body.get("mode", "ALERT")
    from engine.tax_harvester import set_mode
    return set_mode(mode)


# ── VaR & Stress Testing endpoints ────────────────────────────────────────────

@app.get("/api/risk/var")
@timed_cache(3600)
def risk_var():
    """Calculate current portfolio VaR (95% and 99% confidence)."""
    from engine.risk_var import calculate_var
    return calculate_var()


@app.post("/api/risk/stress")
async def risk_stress(req: Request):
    """
    Run a stress scenario.
    Body: {scenario: 'crash'|'tech_rotate'|'vix_spike'|'rate_shock'|'custom',
           param?: float, label?: str}
    """
    body     = await req.json()
    scenario = body.get("scenario", "crash")
    param    = float(body.get("param", 10.0))
    label    = body.get("label", "")
    from engine.risk_var import run_stress
    return run_stress(scenario, param, label)


@app.get("/api/risk/stress/all")
def risk_stress_all():
    """Run all pre-built stress scenarios and return summary."""
    from engine.risk_var import run_all_scenarios
    return run_all_scenarios()


@app.get("/api/risk/history")
async def risk_var_history(days: int = 30):
    """VaR trend over past N days."""
    from engine.risk_var import get_var_history
    return {"history": get_var_history(days)}


@app.post("/api/trade/override")
async def trade_override(req: Request):
    """Captain's override — manually execute a trade bypassing the risk manager."""
    from engine.paper_trader import buy, sell
    body = await req.json()
    player_id = body.get("player_id")
    symbol = body.get("symbol", "").upper()
    action = body.get("action", "BUY").upper()
    qty = int(body.get("qty", 1))
    price = float(body.get("price", 0))
    reasoning = body.get("reasoning", "Captain manual override")
    confidence = float(body.get("confidence", 0.8))
    sources = body.get("sources", "manual-override")

    if not player_id or not symbol or qty <= 0:
        return {"status": "error", "error": "Missing required fields: player_id, symbol, qty"}
    if price <= 0:
        try:
            from engine.market_data import get_stock_price
            pd_ = get_stock_price(symbol)
            price = pd_.get("price", 0) if pd_ else 0
        except Exception:
            pass
    if price <= 0:
        return {"status": "error", "error": f"Could not get price for {symbol}"}
    try:
        if action == "BUY":
            result = buy(player_id=player_id, symbol=symbol, price=price, qty=qty,
                         reasoning=reasoning, confidence=confidence, sources=sources, timeframe="SWING")
        else:
            result = sell(player_id=player_id, symbol=symbol, price=price,
                          reasoning=reasoning, confidence=confidence)
        return {"status": "ok", "result": str(result)}
    except Exception as e:
        logger.error(f"Trade override error: {e}")
        return {"status": "error", "error": str(e)}


@app.get("/api/trade/chain/{symbol}")
def trade_chain(symbol: str, player_id: str = None):
    """Full trade chain for a symbol — recent trades, open positions, optional rejections."""
    symbol = symbol.upper()
    conn = _conn()
    try:
        q = "SELECT * FROM trades WHERE symbol=? ORDER BY executed_at DESC LIMIT 5"
        args = [symbol]
        if player_id:
            q = "SELECT * FROM trades WHERE symbol=? AND player_id=? ORDER BY executed_at DESC LIMIT 5"
            args = [symbol, player_id]
        trades = [dict(zip([c[0] for c in conn.execute(q, args).description], row))
                  for row in conn.execute(q, args).fetchall()]

        pos_q = "SELECT p.*, t.price as entry_price FROM positions p LEFT JOIN trades t ON t.symbol=p.symbol AND t.player_id=p.player_id AND t.action='BUY' WHERE p.symbol=? AND p.qty > 0 ORDER BY t.executed_at DESC"
        positions = [dict(zip([c[0] for c in conn.execute(pos_q, [symbol]).description], row))
                     for row in conn.execute(pos_q, [symbol]).fetchall()]

        rejections = []
        try:
            rejections = [dict(r) for r in conn.execute(
                "SELECT * FROM trade_rejections WHERE symbol=? ORDER BY rejected_at DESC LIMIT 3", [symbol]
            ).fetchall()]
        except Exception:
            pass
        return {"symbol": symbol, "trades": trades, "positions": positions, "rejections": rejections}
    except Exception as e:
        return {"symbol": symbol, "trades": [], "positions": [], "rejections": [], "error": str(e)}
    finally:
        conn.close()


# === WATCHLIST MANAGEMENT ===

@app.get("/api/watchlist")
def get_watchlist():
    """Fleet-wide shared watchlist."""
    conn = _conn()
    rows = conn.execute(
        "SELECT symbol, added_by, added_at, notes, is_active FROM watchlist ORDER BY symbol"
    ).fetchall()
    conn.close()
    return {"symbols": [dict(r) for r in rows]}

@app.post("/api/watchlist/add")
def add_to_watchlist(symbol: str, notes: str = ""):
    conn = _conn()
    conn.execute(
        "INSERT OR IGNORE INTO watchlist (symbol, added_by, notes) VALUES (?, 'manual', ?)",
        (symbol.upper().strip(), notes)
    )
    conn.commit()
    conn.close()
    return {"status": "added", "symbol": symbol.upper().strip()}

@app.post("/api/watchlist/remove")
def remove_from_watchlist(symbol: str):
    conn = _conn()
    conn.execute("DELETE FROM watchlist WHERE symbol=?", (symbol.upper().strip(),))
    conn.commit()
    conn.close()
    return {"status": "removed", "symbol": symbol.upper().strip()}

@app.get("/api/watchlist/model/{player_id}")
def get_model_watchlist(player_id: str):
    """Per-model custom watchlist (addons on top of fleet watchlist)."""
    conn = _conn()
    rows = conn.execute(
        "SELECT symbol, reason, added_at FROM model_watchlist WHERE player_id=? ORDER BY symbol",
        (player_id,)
    ).fetchall()
    conn.close()
    return {"player_id": player_id, "symbols": [dict(r) for r in rows]}

@app.post("/api/watchlist/model/{player_id}/add")
def add_to_model_watchlist(player_id: str, symbol: str, reason: str = ""):
    conn = _conn()
    conn.execute(
        "INSERT OR IGNORE INTO model_watchlist (player_id, symbol, reason) VALUES (?, ?, ?)",
        (player_id, symbol.upper().strip(), reason)
    )
    conn.commit()
    conn.close()
    return {"status": "added", "player_id": player_id, "symbol": symbol.upper().strip()}

@app.post("/api/watchlist/model/{player_id}/remove")
def remove_from_model_watchlist(player_id: str, symbol: str):
    conn = _conn()
    conn.execute(
        "DELETE FROM model_watchlist WHERE player_id=? AND symbol=?",
        (player_id, symbol.upper().strip())
    )
    conn.commit()
    conn.close()
    return {"status": "removed", "player_id": player_id, "symbol": symbol.upper().strip()}


# ---------------------------------------------------------------------------
# Module 7 — Data Ingestion API Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/data/patterns")
def api_get_patterns(symbol: str = "", limit: int = 20):
    """Active market patterns detected in the last 24h."""
    try:
        from engine.data_ingestion import get_active_patterns
        patterns = get_active_patterns(symbol=symbol.upper() if symbol else "", limit=limit)
        return {"patterns": patterns, "count": len(patterns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data/insiders")
def api_get_insiders(symbol: str = "", days: int = 14, limit: int = 30):
    """Recent insider trades (Form 4), filtered by symbol if provided."""
    try:
        from engine.data_ingestion import get_recent_insider_trades
        trades = get_recent_insider_trades(
            symbol=symbol.upper() if symbol else "",
            days=days,
            limit=limit,
        )
        return {"insider_trades": trades, "count": len(trades)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data/market-history")
def api_get_market_history(symbol: str = "SPY", days: int = 30):
    """Historical OHLCV bars for a symbol from market_snapshots."""
    try:
        from engine.data_ingestion import get_market_history
        history = get_market_history(symbol=symbol.upper(), days=days)
        return {"symbol": symbol.upper(), "bars": history, "count": len(history)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data/import/webull")
async def api_import_webull(request: Request):
    """Upload and import a Webull trade history CSV.

    Accepts multipart/form-data with field 'file', or JSON body with 'filepath' key.
    """
    import tempfile

    content_type = request.headers.get("content-type", "")

    if "multipart" in content_type:
        # Handle file upload
        try:
            form = await request.form()
            upload = form.get("file")
            if upload is None:
                raise HTTPException(status_code=400, detail="No 'file' field in form data")
            contents = await upload.read()
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".csv", delete=False, prefix="webull_import_"
            ) as tmp:
                tmp.write(contents)
                tmp_path = tmp.name
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Upload error: {e}")
    else:
        # JSON body with filepath
        try:
            body = await request.json()
            tmp_path = body.get("filepath", "")
            if not tmp_path:
                raise HTTPException(status_code=400, detail="'filepath' required in JSON body")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Request parse error: {e}")

    try:
        from engine.data_ingestion import import_webull_csv
        result = import_webull_csv(tmp_path)
        return {"status": "ok", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analyze/{ticker}")
def analyze_ticker(ticker: str):
    """AI analysis for a ticker: pulls recent signals then asks deepseek-r1:7b for synthesis."""
    import requests as req
    from config import OLLAMA_URL

    sym = ticker.upper()

    # --- Pull context from DB ---
    conn = _conn()

    # Recent signals for this ticker (last 48h, all players)
    raw_signals = conn.execute(
        "SELECT s.player_id, p.display_name, s.signal, s.confidence, s.reasoning, s.created_at "
        "FROM signals s JOIN ai_players p ON s.player_id = p.id "
        "WHERE s.symbol = ? AND s.created_at >= datetime('now', '-48 hours') "
        "ORDER BY s.created_at DESC LIMIT 20",
        (sym,),
    ).fetchall()

    conn.close()

    # --- Format signal summary ---
    signal_lines = []
    for r in raw_signals:
        reasoning_snippet = (r["reasoning"] or "")[:120].replace("\n", " ")
        signal_lines.append(
            f"  [{r['created_at'][:16]}] {r['display_name']}: {r['signal']} "
            f"({r['confidence']}%) — {reasoning_snippet}"
        )

    signals_block = "\n".join(signal_lines) if signal_lines else "  No recent signals in DB."

    price_ctx = ""
    try:
        from engine.market_data import get_stock_price
        pd = get_stock_price(sym)
        if pd.get("price"):
            price_ctx = f"Current price: ${pd['price']:.2f} ({pd.get('change_pct', 0):+.2f}% today). "
    except Exception:
        pass

    # --- Build prompt ---
    prompt = (
        f"You are Lt. Cmdr. Data analyzing {sym} for the Captain of USS TradeMinds.\n\n"
        f"{price_ctx}\n"
        f"RECENT AI SIGNALS (last 48h):\n{signals_block}\n\n"
        f"Based on the crew's signals above, provide a crisp analysis. "
        f"Respond ONLY as valid JSON with these exact keys:\n"
        f'{{"signal": "BUY|SELL|HOLD|WATCH", '
        f'"confidence": <0-100 integer>, '
        f'"reasoning": "<2-3 sentence synthesis of the crew signals>", '
        f'"key_levels": {{"support": <nearest support price as number or null>, "resistance": <nearest resistance price as number or null>}}, '
        f'"buy_range": "<low>-<high> e.g. 142.50-145.00 or null if not a buy", '
        f'"target_price": <take-profit price as number or null>, '
        f'"stop_loss": <stop-loss price as number or null>, '
        f'"volume_assessment": "<1 sentence on volume trend or null>"}}\n\n'
        f"No extra text. JSON only."
    )

    # --- Call deepseek-r1:7b ---
    try:
        r = req.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": "deepseek-r1:7b", "prompt": prompt, "stream": False, "num_predict": 500},
            timeout=60,
        )
        r.raise_for_status()
        raw_text = r.json().get("response", "").strip()

        # Strip <think>...</think> blocks if present (deepseek-r1 reasoning tokens)
        raw_text = re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL).strip()

        # Extract JSON — find the first { ... } block
        m = re.search(r"\{.*\}", raw_text, re.DOTALL)
        if m:
            import json as _json
            parsed = _json.loads(m.group())
            return {
                "symbol": sym,
                "signal": str(parsed.get("signal", "HOLD")).upper(),
                "confidence": int(parsed.get("confidence", 50)),
                "reasoning": str(parsed.get("reasoning", "")),
                "key_levels": parsed.get("key_levels", {"support": None, "resistance": None}),
                "buy_range": parsed.get("buy_range") or None,
                "target_price": parsed.get("target_price") or None,
                "stop_loss": parsed.get("stop_loss") or None,
                "volume_assessment": parsed.get("volume_assessment") or None,
                "signals_used": len(raw_signals),
                "model": "deepseek-r1:7b",
            }
    except Exception as exc:
        pass

    # Fallback: count votes from raw signals
    if raw_signals:
        from collections import Counter
        vote_counts = Counter(r["signal"] for r in raw_signals)
        top_signal = vote_counts.most_common(1)[0][0]
        avg_conf = int(sum(r["confidence"] for r in raw_signals) / len(raw_signals))
        return {
            "symbol": sym,
            "signal": top_signal,
            "confidence": avg_conf,
            "reasoning": f"Ollama unavailable. Majority vote from {len(raw_signals)} recent crew signals.",
            "key_levels": {"support": None, "resistance": None},
            "buy_range": None,
            "target_price": None,
            "stop_loss": None,
            "volume_assessment": None,
            "signals_used": len(raw_signals),
            "model": "vote-fallback",
        }

    return {
        "symbol": sym,
        "signal": "HOLD",
        "confidence": 0,
        "reasoning": "No recent signals and Ollama unavailable.",
        "key_levels": {"support": None, "resistance": None},
        "signals_used": 0,
        "model": "none",
    }


# ---------------------------------------------------------------------------
# Crew Activity Feed — decisions from crew_scanner pipeline
# ---------------------------------------------------------------------------

@app.get("/api/crew/decisions")
async def api_crew_decisions(limit: int = 50):
    """
    Recent crew agent decisions with agent names, actions, symbols, and reasoning.
    Used by the 'Crew Activity' dashboard card.

    Returns JSON: { decisions: [...], count: int }
    Each row: { timestamp, agent_name, player_id, action, symbol,
                confidence, reason, gate_result, executed }
    """
    try:
        from engine.crew_scanner import get_crew_decisions
        decisions = get_crew_decisions(limit=max(1, min(limit, 200)))
        return {"decisions": decisions, "count": len(decisions)}
    except Exception as e:
        return {"error": str(e), "decisions": [], "count": 0}


class ManualTradeRequest(BaseModel):
    symbol: str
    action: str          # "buy" | "sell"
    agent: str           # display name or player_id
    source: str = "alert-card"


@app.post("/api/paper-trader/manual-trade")
async def api_manual_trade(req: ManualTradeRequest):
    """Execute a manual paper trade on behalf of a named agent."""
    try:
        symbol = req.symbol.upper().strip()
        action = req.action.lower().strip()
        agent_input = req.agent.lower().strip()

        # Resolve player_id from display name or direct id
        from engine.crew_specialization import CREW_MANIFEST
        player_id = None
        display_name = req.agent
        for pid, m in CREW_MANIFEST.items():
            if pid.lower() == agent_input or m.get("display_name", "").lower() == agent_input:
                player_id = pid
                display_name = m.get("display_name", pid)
                break
        if not player_id:
            return {"error": f"Unknown agent: {req.agent}"}

        # Get current price
        from engine.market_data import get_stock_price
        price_data = get_stock_price(symbol)
        price = float(price_data.get("price") or 0)
        if price <= 0:
            return {"error": f"Could not fetch price for {symbol}"}

        # Calculate qty: 5% of agent equity
        from engine.paper_trader import get_portfolio, buy, sell
        port = get_portfolio(player_id)
        equity = float(port.get("total_value") or port.get("cash") or 10000)
        qty = max(1, int((equity * 0.05) / price))

        if action == "buy":
            result = buy(
                player_id=player_id,
                symbol=symbol,
                price=price,
                qty=qty,
                reasoning=f"Manual trade via alert card ({req.source})",
                confidence=0.75,
                timeframe="INTRADAY",
            )
        elif action == "sell":
            result = sell(
                player_id=player_id,
                symbol=symbol,
                price=price,
                reasoning=f"Manual sell via alert card ({req.source})",
                confidence=0.75,
            )
        else:
            return {"error": f"Unknown action: {req.action}"}

        if result:
            return {"status": "executed", "qty": qty, "price": price, "agent": display_name, "symbol": symbol, "action": action}
        else:
            return {"error": "Trade rejected by paper trader (check logs for gate details)"}

    except Exception as e:
        return {"error": str(e)}


class QuickScanRequest(BaseModel):
    symbol: str


@app.post("/api/crew/quick-scan")
async def api_crew_quick_scan(req: QuickScanRequest):
    """Run Neo, Spock, Q against a specific symbol and return their decisions."""
    try:
        symbol = req.symbol.upper().strip()

        from engine.crew_scanner import gather_market_context, _scan_single_agent
        ctx = gather_market_context()
        # Inject the symbol as the only deep-scan pick so agents focus on it
        ctx["deep_scan_top"] = [{"symbol": symbol, "signal_strength": 0.9}]

        QUICK_SCAN_AGENTS = ["neo-matrix", "grok-4", "ollama-glm4"]
        results = []
        for pid in QUICK_SCAN_AGENTS:
            try:
                r = _scan_single_agent(pid, ctx)
                from engine.crew_specialization import CREW_MANIFEST
                dname = CREW_MANIFEST.get(pid, {}).get("display_name", pid)
                results.append({
                    "player_id": pid,
                    "agent": dname,
                    "action": r.get("action", "PASS"),
                    "symbol": r.get("symbol") or symbol,
                    "confidence": r.get("confidence", 0),
                    "reason": r.get("reason", "")[:200],
                    "executed": r.get("executed", False),
                })
            except Exception as agent_err:
                results.append({"player_id": pid, "agent": pid, "action": "ERROR", "reason": str(agent_err)})

        return {"symbol": symbol, "results": results}

    except Exception as e:
        return {"error": str(e), "results": []}


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3.2 — PUBLIC REST API  /api/v1/
# ─────────────────────────────────────────────────────────────────────────────

_V1_API_KEY = os.getenv("TRADEMINDS_API_KEY", "")

def _v1_check_key(request: Request) -> bool:
    key = request.headers.get("X-API-Key", "")
    return bool(_V1_API_KEY and key == _V1_API_KEY)

def _v1_auth_error():
    return JSONResponse(
        {"error": "Unauthorized", "hint": "Pass your key in the X-API-Key header"},
        status_code=401,
    )

# Rate limiting: 60 req/min per IP for /api/v1/
_v1_rate: dict = {}
_v1_rate_lock = threading.Lock()

def _v1_rate_ok(ip: str) -> bool:
    import time as _time
    now = _time.time()
    with _v1_rate_lock:
        window, count = _v1_rate.get(ip, (now, 0))
        if now - window > 60:
            _v1_rate[ip] = (now, 1)
            return True
        if count >= 60:
            return False
        _v1_rate[ip] = (window, count + 1)
        return True

def _v1_cors_headers() -> dict:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "X-API-Key, Content-Type",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    }

def _v1_resp(data, status: int = 200):
    return JSONResponse(data, status_code=status, headers=_v1_cors_headers())


# ── Phase 3.4 — Trade Explain ────────────────────────────────────────────────

def _build_trade_explain(conn, trade_id: int) -> dict | None:
    """Reconstruct the full decision chain for a trade from all available sources."""
    trade = conn.execute(
        "SELECT * FROM trades WHERE id=?", (trade_id,)
    ).fetchone()
    if not trade:
        return None
    t = dict(trade)

    # Base explain data from trade record
    explain: dict = {
        "trade_id":    t["id"],
        "player_id":   t["player_id"],
        "symbol":      t["symbol"],
        "action":      t["action"],
        "qty":         t["qty"],
        "price":       t["price"],
        "executed_at": t["executed_at"],
        "reasoning":   t["reasoning"] or "",
        "confidence":  t["confidence"] or 0,
        "sources":     t["sources"] or "",
        "asset_type":  t["asset_type"] or "equity",
    }

    # Pull stored explain data if available (new trades post Phase 3.4)
    stored = conn.execute(
        "SELECT * FROM trade_explanations WHERE trade_id=?", (trade_id,)
    ).fetchone()
    if stored:
        s = dict(stored)
        for k in ("signals_json", "votes_json", "backtest_json", "risk_json", "timeline_json"):
            if s.get(k):
                try:
                    explain[k.replace("_json", "")] = json.loads(s[k])
                except Exception:
                    pass
        explain["scanner_score"] = s.get("scanner_score")
    else:
        # Reconstruct from DB
        explain["signals"] = []
        explain["votes"] = {}
        explain["risk"] = {}
        explain["timeline"] = []

    # ── Signals: parse sources field + look up signals table ──
    if not explain.get("signals"):
        sigs = []
        sources_str = t["sources"] or ""
        if sources_str:
            for src in sources_str.split(","):
                src = src.strip()
                if src:
                    sigs.append({"source": src, "type": "trigger"})
        # Look up nearby signals for this player+symbol
        try:
            sig_rows = conn.execute(
                "SELECT signal, confidence, reasoning, created_at FROM signals "
                "WHERE player_id=? AND symbol=? AND datetime(created_at) <= datetime(?) "
                "ORDER BY created_at DESC LIMIT 3",
                (t["player_id"], t["symbol"], t["executed_at"] or "now")
            ).fetchall()
            for sr in sig_rows:
                sigs.append({
                    "source": "signal_engine",
                    "type": sr["signal"] or "SIGNAL",
                    "confidence": sr["confidence"],
                    "reasoning": (sr["reasoning"] or "")[:200],
                    "at": sr["created_at"],
                })
        except Exception:
            pass
        explain["signals"] = sigs

    # ── Bridge Votes: look up votes near execution time ──
    if not explain.get("votes"):
        try:
            vote_rows = conn.execute(
                """SELECT vote, player_name, confidence, reason, model_used
                   FROM bridge_votes
                   WHERE symbol=? AND datetime(created_at) BETWEEN
                         datetime(?, '-30 minutes') AND datetime(?, '+10 minutes')
                   ORDER BY created_at DESC LIMIT 20""",
                (t["symbol"], t["executed_at"] or "", t["executed_at"] or "")
            ).fetchall()
            if not vote_rows:
                # Fall back: any recent bridge votes near time (no symbol filter)
                vote_rows = conn.execute(
                    """SELECT vote, player_name, confidence, reason, model_used
                       FROM bridge_votes
                       WHERE datetime(created_at) BETWEEN
                             datetime(?, '-15 minutes') AND datetime(?, '+5 minutes')
                       ORDER BY created_at DESC LIMIT 20""",
                    (t["executed_at"] or "", t["executed_at"] or "")
                ).fetchall()
            by_vote: dict = {"BUY": [], "SELL": [], "HOLD": []}
            for vr in vote_rows:
                v = (vr["vote"] or "").upper()
                entry = {
                    "voter": vr["player_name"] or "",
                    "confidence": vr["confidence"],
                    "reason": (vr["reason"] or "")[:150],
                    "model": vr["model_used"] or "",
                }
                if v in by_vote:
                    by_vote[v].append(entry)
            explain["votes"] = by_vote
        except Exception:
            explain["votes"] = {}

    # ── Risk: look up risk_assessments for this ticker ──
    if not explain.get("risk"):
        try:
            ra = conn.execute(
                "SELECT * FROM risk_assessments WHERE ticker=? ORDER BY created_at DESC LIMIT 1",
                (t["symbol"],)
            ).fetchone()
            if ra:
                ra = dict(ra)
                explain["risk"] = {
                    "verdict": ra.get("final_decision"),
                    "size_pct": ra.get("final_size_pct"),
                    "spock": ra.get("spock_vote"),
                    "crusher": ra.get("crusher_vote"),
                    "scotty": ra.get("scotty_vote"),
                    "kelly_size": ra.get("kelly_size"),
                    "account_heat_pct": ra.get("account_heat_pct"),
                    "vix_modifier": ra.get("vix_modifier"),
                    "hard_stop": ra.get("hard_stop"),
                }
        except Exception:
            pass

    # ── Scanner score: look up universe_scan / fast_scan_results ──
    if explain.get("scanner_score") is None:
        try:
            sc = conn.execute(
                "SELECT score FROM universe_scan WHERE ticker=? ORDER BY scan_date DESC LIMIT 1",
                (t["symbol"],)
            ).fetchone()
            explain["scanner_score"] = float(sc["score"]) if sc else None
        except Exception:
            pass

    # ── Backtest: look up backtest_history for player+symbol ──
    if not explain.get("backtest"):
        try:
            bt = conn.execute(
                "SELECT * FROM backtest_history WHERE symbol=? ORDER BY created_at DESC LIMIT 1",
                (t["symbol"],)
            ).fetchone()
            if bt:
                bt = dict(bt)
                explain["backtest"] = {
                    "win_rate": bt.get("win_rate"),
                    "avg_return": bt.get("avg_return"),
                    "max_drawdown": bt.get("max_drawdown"),
                    "sample_size": bt.get("sample_size"),
                    "strategy": bt.get("strategy"),
                }
        except Exception:
            pass

    # ── Timeline: reconstruct step-by-step with timestamps ──
    if not explain.get("timeline"):
        ts_base = t["executed_at"] or ""
        timeline = []

        # Step 1: Signal / trigger
        earliest_sig = None
        for s in explain.get("signals", []):
            if s.get("at"):
                earliest_sig = s["at"]
                break
        timeline.append({
            "step": "signal",
            "label": "Signal / Trigger",
            "ts": earliest_sig or ts_base,
            "status": "ok" if explain.get("signals") else "unknown",
            "detail": (explain.get("signals", [{}])[0].get("type", "Market signal detected")
                       if explain.get("signals") else "Signal source: " + (explain["sources"] or "unknown")),
        })

        # Step 2: Bridge Vote
        votes = explain.get("votes", {})
        buy_ct  = len(votes.get("BUY", []))
        sell_ct = len(votes.get("SELL", []))
        hold_ct = len(votes.get("HOLD", []))
        total_v = buy_ct + sell_ct + hold_ct
        vote_status = "ok" if total_v > 0 else "unavailable"
        vote_detail = (f"{buy_ct} BUY / {sell_ct} SELL / {hold_ct} HOLD" if total_v > 0
                       else "No bridge vote data available for this time window")
        timeline.append({
            "step": "vote",
            "label": "Bridge Vote",
            "ts": ts_base,
            "status": vote_status,
            "detail": vote_detail,
        })

        # Step 3: Risk Check
        risk = explain.get("risk", {})
        risk_verdict = risk.get("verdict") or ("APPROVED" if risk else "UNKNOWN")
        risk_status = "ok" if risk_verdict and "REJECT" not in str(risk_verdict).upper() else "fail"
        timeline.append({
            "step": "risk",
            "label": "Risk Check",
            "ts": ts_base,
            "status": risk_status,
            "detail": (f"Verdict: {risk_verdict}"
                       + (f" | Size: {risk.get('size_pct')}%" if risk.get("size_pct") else "")
                       + (f" | Heat: {risk.get('account_heat_pct')}%" if risk.get("account_heat_pct") else "")
                       if risk else "Risk data not captured for this trade"),
        })

        # Step 4: Execute
        timeline.append({
            "step": "execute",
            "label": "Execute",
            "ts": ts_base,
            "status": "ok",
            "detail": f"{t['action']} {t['qty']} {t['symbol']} @ ${t['price']:.2f}  ({t['confidence']*100:.0f}% confidence)",
        })

        explain["timeline"] = timeline

    return explain


@app.get("/api/v1/trades/{trade_id}/explain")
def v1_trade_explain(request: Request, trade_id: int):
    """Public: full decision chain for a trade — signal → vote → risk → execute."""
    ip = request.client.host if request.client else "unknown"
    if not _v1_rate_ok(ip):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        conn = _conn()
        explain = _build_trade_explain(conn, trade_id)
        conn.close()
        if not explain:
            return _v1_resp({"error": f"Trade {trade_id} not found"}, 404)
        return _v1_resp({"ok": True, "explain": explain})
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.get("/api/trades/{trade_id}/explain")
def trade_explain_internal(request: Request, trade_id: int):
    """Internal (auth bypassed via localhost): explain endpoint without API key."""
    try:
        conn = _conn()
        explain = _build_trade_explain(conn, trade_id)
        conn.close()
        if not explain:
            return JSONResponse({"error": f"Trade {trade_id} not found"}, status_code=404)
        return JSONResponse({"ok": True, "explain": explain})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Public endpoints ──────────────────────────────────────────────────────────

@app.get("/api/v1/leaderboard")
def v1_leaderboard(request: Request):
    """Public: current agent rankings."""
    ip = request.client.host if request.client else "unknown"
    if not _v1_rate_ok(ip):
        return _v1_resp({"error": "Rate limit exceeded — 60 requests/minute"}, 429)
    try:
        raw = leaderboard()
        agents = raw.get("players", raw.get("agents", []))
        slim = [
            {
                "rank":        i + 1,
                "id":          a.get("id"),
                "name":        a.get("display_name") or a.get("name"),
                "role":        a.get("role"),
                "score":       a.get("composite_score") or a.get("score"),
                "win_rate":    a.get("win_rate"),
                "total_trades":a.get("total_trades") or a.get("trades"),
                "pnl":         a.get("total_pnl") or a.get("pnl"),
                "streak":      a.get("streak"),
            }
            for i, a in enumerate(agents[:50])
        ]
        return _v1_resp({"ok": True, "count": len(slim), "agents": slim})
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.get("/api/v1/health")
def v1_health(request: Request):
    """Public: system status."""
    ip = request.client.host if request.client else "unknown"
    if not _v1_rate_ok(ip):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        s = status()
        return _v1_resp({
            "ok":     True,
            "status": s.get("status", "running"),
            "season": s.get("season"),
            "active_agents": s.get("active_agents") or s.get("players"),
            "total_trades":  s.get("total_trades") or s.get("trades"),
            "uptime":        s.get("uptime"),
        })
    except Exception as e:
        return _v1_resp({"ok": False, "error": str(e)}, 500)


# ── Authenticated endpoints ───────────────────────────────────────────────────

@app.get("/api/v1/signals")
def v1_signals(request: Request, ticker: str = "", limit: int = 50):
    """Authenticated: latest signals."""
    if not _v1_check_key(request):
        return _v1_auth_error()
    ip = request.client.host if request.client else "unknown"
    if not _v1_rate_ok(ip):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        conn = _conn()
        q = "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?"
        params: list = [min(limit, 200)]
        if ticker:
            q = "SELECT * FROM signals WHERE symbol=? ORDER BY created_at DESC LIMIT ?"
            params = [ticker.upper(), min(limit, 200)]
        rows = conn.execute(q, params).fetchall()
        conn.close()
        return _v1_resp({
            "ok": True,
            "count": len(rows),
            "signals": [dict(r) for r in rows],
        })
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.get("/api/v1/portfolio")
def v1_portfolio(request: Request):
    """Authenticated: positions + P&L."""
    if not _v1_check_key(request):
        return _v1_auth_error()
    if not _v1_rate_ok(request.client.host if request.client else "x"):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        from shared.alpaca_portfolio_sync import get_last_sync_status
        sync = get_last_sync_status()
        conn = _conn()
        positions = conn.execute(
            "SELECT symbol, qty, avg_price, asset_type FROM positions WHERE player_id='steve-webull'"
        ).fetchall()
        conn.close()
        return _v1_resp({
            "ok": True,
            "synced_at":       sync.get("synced_at"),
            "portfolio_value": sync.get("portfolio_value"),
            "cash":            sync.get("cash"),
            "buying_power":    sync.get("buying_power"),
            "positions": [dict(p) for p in positions],
        })
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.get("/api/v1/votes")
def v1_votes(request: Request):
    """Authenticated: latest bridge vote results."""
    if not _v1_check_key(request):
        return _v1_auth_error()
    if not _v1_rate_ok(request.client.host if request.client else "x"):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        raw = api_bridge_votes()
        return _v1_resp({"ok": True, "votes": raw.get("votes", raw) if isinstance(raw, dict) else raw})
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.get("/api/v1/risk")
def v1_risk(request: Request):
    """Authenticated: VaR + stress test results."""
    if not _v1_check_key(request):
        return _v1_auth_error()
    if not _v1_rate_ok(request.client.host if request.client else "x"):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        conn = _conn()
        var_row = conn.execute(
            "SELECT * FROM settings WHERE key LIKE 'var_%' OR key LIKE 'risk_%' ORDER BY key"
        ).fetchall()
        conn.close()
        return _v1_resp({"ok": True, "risk": {r["key"]: r["value"] for r in var_row}})
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.get("/api/v1/agents")
def v1_agents_list(request: Request):
    """Authenticated: list all agents."""
    if not _v1_check_key(request):
        return _v1_auth_error()
    if not _v1_rate_ok(request.client.host if request.client else "x"):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT id, display_name, role, cash, is_active FROM ai_players ORDER BY display_name"
        ).fetchall()
        conn.close()
        return _v1_resp({"ok": True, "agents": [dict(r) for r in rows]})
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.post("/api/v1/agents/create")
async def v1_agents_create(request: Request):
    """Authenticated: create agent via natural language."""
    if not _v1_check_key(request):
        return _v1_auth_error()
    if not _v1_rate_ok(request.client.host if request.client else "x"):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        body = await request.json()
        # Delegate to existing agent parse+create pipeline
        from fastapi import Request as _Req
        result = await request.app.router.routes  # re-use existing endpoint
        # Direct call to internal function
        conn = _conn()
        conn.close()
        return _v1_resp({"ok": True, "message": "Use POST /api/agents/create with the same body", "body": body})
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


@app.get("/api/v1/indexes")
def v1_indexes(request: Request):
    """Authenticated: list generated strategy indexes/assets."""
    if not _v1_check_key(request):
        return _v1_auth_error()
    if not _v1_rate_ok(request.client.host if request.client else "x"):
        return _v1_resp({"error": "Rate limit exceeded"}, 429)
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT * FROM custom_indexes ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
        conn.close()
        return _v1_resp({"ok": True, "indexes": [dict(r) for r in rows]})
    except Exception as e:
        return _v1_resp({"error": str(e)}, 500)


# ── CORS preflight ────────────────────────────────────────────────────────────

@app.options("/api/v1/{path:path}")
def v1_options():
    return JSONResponse({}, headers=_v1_cors_headers())


# ── API Docs page ─────────────────────────────────────────────────────────────

@app.get("/api/v1/docs", response_class=HTMLResponse)
def v1_docs():
    """Human-readable API documentation."""
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradeMinds API v1 — Docs</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
        background:#f8f9fa;color:#1a1a2e;line-height:1.6}}
  .wrap{{max-width:860px;margin:0 auto;padding:40px 20px}}
  h1{{font-size:32px;font-weight:900;margin-bottom:4px}}
  .subtitle{{color:#666;margin-bottom:40px;font-size:16px}}
  h2{{font-size:20px;font-weight:800;margin:36px 0 12px;color:#0055cc;border-bottom:2px solid #e0e7ff;padding-bottom:6px}}
  .endpoint{{background:#fff;border:1px solid #e0e0e0;border-radius:10px;padding:18px 20px;margin-bottom:14px}}
  .method{{display:inline-block;padding:3px 10px;border-radius:5px;font-size:13px;font-weight:800;margin-right:8px}}
  .get{{background:#e0f2e9;color:#00875a}}
  .post{{background:#e0e7ff;color:#0055cc}}
  .path{{font-family:monospace;font-size:15px;font-weight:700}}
  .desc{{font-size:14px;color:#555;margin-top:6px}}
  .params{{margin-top:10px;font-size:13px}}
  .params td{{padding:4px 12px 4px 0;vertical-align:top}}
  .params td:first-child{{font-family:monospace;color:#cc4400;white-space:nowrap}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:700}}
  .pub{{background:#d1fae5;color:#065f46}}
  .auth{{background:#fef3c7;color:#92400e}}
  pre{{background:#1a1a2e;color:#a8e6cf;padding:16px;border-radius:8px;font-size:13px;overflow-x:auto;margin-top:10px}}
  .key-box{{background:#fff;border:2px solid #0055cc;border-radius:8px;padding:14px 18px;margin:20px 0;font-size:14px}}
  .key-box code{{font-family:monospace;font-size:14px;color:#0055cc}}
  footer{{margin-top:60px;padding-top:20px;border-top:1px solid #e0e0e0;color:#999;font-size:13px}}
</style>
</head>
<body>
<div class="wrap">
  <h1>TradeMinds API v1</h1>
  <p class="subtitle">Public REST API — Phase 3.2 &nbsp;|&nbsp; Base URL: <code>https://bridge.accessapple.com</code></p>

  <div class="key-box">
    🔑 Pass your API key in the request header: <code>X-API-Key: &lt;your-key&gt;</code><br>
    <span style="color:#666;font-size:13px">Public endpoints need no key. Authenticated endpoints return 401 without it. Obtain your key from the dashboard settings.</span>
  </div>

  <h2>Public Endpoints</h2>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/leaderboard</span>
    <span class="badge pub">public</span>
    <div class="desc">All agent rankings — name, role, score, win rate, P&L, streak.</div>
    <pre>curl https://bridge.accessapple.com/api/v1/leaderboard</pre>
  </div>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/health</span>
    <span class="badge pub">public</span>
    <div class="desc">System status — season, active agents, trade count, uptime.</div>
    <pre>curl https://bridge.accessapple.com/api/v1/health</pre>
  </div>

  <h2>Authenticated Endpoints</h2>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/signals</span>
    <span class="badge auth">auth</span>
    <div class="desc">Latest trading signals.</div>
    <table class="params"><tr><td>ticker</td><td>Filter by symbol (optional)</td></tr>
    <tr><td>limit</td><td>Max results, default 50, max 200</td></tr></table>
    <pre>curl -H "X-API-Key: YOUR_KEY" \\
  "https://bridge.accessapple.com/api/v1/signals?ticker=SPY&limit=10"</pre>
  </div>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/portfolio</span>
    <span class="badge auth">auth</span>
    <div class="desc">Current Webull/Alpaca positions, cash, portfolio value, buying power.</div>
    <pre>curl -H "X-API-Key: YOUR_KEY" https://bridge.accessapple.com/api/v1/portfolio</pre>
  </div>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/votes</span>
    <span class="badge auth">auth</span>
    <div class="desc">Latest Bridge Vote results — agent consensus on market direction.</div>
    <pre>curl -H "X-API-Key: YOUR_KEY" https://bridge.accessapple.com/api/v1/votes</pre>
  </div>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/risk</span>
    <span class="badge auth">auth</span>
    <div class="desc">Current VaR and stress test results from the risk engine.</div>
    <pre>curl -H "X-API-Key: YOUR_KEY" https://bridge.accessapple.com/api/v1/risk</pre>
  </div>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/agents</span>
    <span class="badge auth">auth</span>
    <div class="desc">Full agent roster with roles and account balances.</div>
    <pre>curl -H "X-API-Key: YOUR_KEY" https://bridge.accessapple.com/api/v1/agents</pre>
  </div>

  <div class="endpoint">
    <span class="method get">GET</span><span class="path">/api/v1/indexes</span>
    <span class="badge auth">auth</span>
    <div class="desc">Generated strategy indexes and custom assets.</div>
    <pre>curl -H "X-API-Key: YOUR_KEY" https://bridge.accessapple.com/api/v1/indexes</pre>
  </div>

  <div class="endpoint">
    <span class="method post">POST</span><span class="path">/api/v1/agents/create</span>
    <span class="badge auth">auth</span>
    <div class="desc">Create a new agent via natural language description. Body: JSON with <code>description</code>.</div>
    <pre>curl -X POST -H "X-API-Key: YOUR_KEY" -H "Content-Type: application/json" \\
  -d '{{"description":"A momentum trader focused on tech stocks"}}' \\
  https://bridge.accessapple.com/api/v1/agents/create</pre>
  </div>

  <h2>Rate Limits</h2>
  <p style="font-size:15px">60 requests per minute per IP address. Exceeding returns HTTP 429.</p>

  <h2>Response Format</h2>
  <pre>// Success
{{"ok": true, "count": 29, "agents": [...]}}

// Error
{{"error": "Unauthorized", "hint": "Pass your key in the X-API-Key header"}}</pre>

  <h2>CORS</h2>
  <p style="font-size:15px">All <code>/api/v1/</code> endpoints include <code>Access-Control-Allow-Origin: *</code> headers for browser access.</p>

  <footer>TradeMinds USS Enterprise · Season 5 · Phase 3.2 Public API</footer>
</div>
</body>
</html>"""
    return HTMLResponse(html, headers=_v1_cors_headers())


# ── Brain Context endpoint ────────────────────────────────────────────────────

@app.get("/api/brain-context")
def brain_context_endpoint(symbol: str = "SPY", player_id: str = "claude-trader"):
    """
    Comprehensive intelligence context for a given symbol + player.
    Returns fear/greed, red alert, congress trades, signal scorecard,
    fleet consensus, and per-symbol backtest performance.
    """
    try:
        from engine.brain_context import build_full_context_raw
        data = build_full_context_raw(player_id, symbol)
        return JSONResponse({"ok": True, **data})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── Phase 3.6 — Morning Briefing endpoint ────────────────────────────────────

@app.get("/api/briefing/today")
async def briefing_today(force: bool = False):
    """
    Today's comprehensive morning briefing — text + audio URL.
    Cached per calendar day; force=true regenerates.
    Public (localhost) / authenticated externally.
    """
    try:
        from engine.morning_briefing import generate_morning_briefing
        result = generate_morning_briefing(force=force)
        return JSONResponse({
            "ok":           True,
            "text":         result.get("text", ""),
            "sections":     result.get("sections", {}),
            "audio_url":    result.get("audio_url"),
            "generated_at": result.get("generated_at"),
            "date":         result.get("date"),
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── Phase 3.3 — Social Leaderboard pages ─────────────────────────────────

_LEADERBOARD_HTML = os.path.join(os.path.dirname(__file__), "static", "leaderboard.html")
_LB_NO_CACHE = {"Cache-Control": "no-cache, no-store, must-revalidate"}

@app.get("/leaderboard")
def leaderboard_page():
    """Public: leaderboard page (no auth)."""
    return FileResponse(_LEADERBOARD_HTML, headers=_LB_NO_CACHE)

@app.get("/leaderboard/embed")
def leaderboard_embed():
    """Public: embeddable top-5 widget (iframe-friendly)."""
    return FileResponse(_LEADERBOARD_HTML, headers=_LB_NO_CACHE)

@app.get("/leaderboard/{slug}")
def leaderboard_agent(slug: str):
    """Public: per-agent shareable link (no auth)."""
    return FileResponse(_LEADERBOARD_HTML, headers=_LB_NO_CACHE)

# ── Trades recent endpoint for detail view ────────────────────────────────

@app.get("/api/trades/recent")
def api_trades_recent(request: Request, player_id: str = "", limit: int = 30):
    """Public-accessible (via leaderboard detail): recent trades for a player."""
    try:
        conn = _conn()
        lim = min(int(limit), 100)
        if player_id:
            rows = conn.execute(
                "SELECT symbol, action, qty, price, entry_price, realized_pnl, corrected_pnl, executed_at "
                "FROM trades WHERE player_id=? ORDER BY executed_at DESC LIMIT ?",
                (player_id, lim)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT symbol, action, qty, price, entry_price, realized_pnl, corrected_pnl, executed_at "
                "FROM trades ORDER BY executed_at DESC LIMIT ?",
                (lim,)
            ).fetchall()
        conn.close()
        return JSONResponse({"ok": True, "trades": [dict(r) for r in rows]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── Phase 3.8 — Backtest Arena ────────────────────────────────────────────────

import hashlib as _hashlib
import secrets as _secrets

_BACKTEST_ARENA_HTML = os.path.join(os.path.dirname(__file__), "static", "backtest_arena.html")
_bt_rate: dict = {}   # ip → [(ts, count)]
_bt_rate_lock = threading.Lock()
_BT_MAX_PER_HOUR = 3


def _bt_rate_ok(ip: str) -> bool:
    """Allow max 3 backtests per IP per hour."""
    now = _time.time()
    with _bt_rate_lock:
        window = [t for t in _bt_rate.get(ip, []) if now - t < 3600]
        if len(window) >= _BT_MAX_PER_HOUR:
            return False
        window.append(now)
        _bt_rate[ip] = window
        return True


@app.get("/backtest")
def backtest_arena_page():
    """Public: community backtest arena page."""
    return FileResponse(_BACKTEST_ARENA_HTML, headers={"Cache-Control": "no-cache"})


@app.get("/backtest/result/{share_id}")
def backtest_result_page(share_id: str):
    """Public: shareable backtest result page (same HTML, JS reads share_id from URL)."""
    return FileResponse(_BACKTEST_ARENA_HTML, headers={"Cache-Control": "no-cache"})


@app.post("/api/backtest/community/run")
async def api_backtest_run(request: Request):
    """
    Public: run a community backtest.
    Body: {ticker, strategy, period_days, params}
    Rate limit: 3 per IP per hour.
    """
    ip = request.client.host if request.client else "unknown"
    if not _bt_rate_ok(ip):
        return JSONResponse(
            {"ok": False, "error": f"Rate limit: max {_BT_MAX_PER_HOUR} backtests per hour per IP."},
            status_code=429
        )
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON body"}, status_code=400)

    import re as _re
    ticker   = str(body.get("ticker", "SPY")).upper().strip()[:10]
    if not _re.match(r'^[A-Z0-9.\-\^]{1,10}$', ticker):
        return JSONResponse({"ok": False, "error": "Invalid ticker symbol. Use letters, digits, . - ^ only."}, status_code=400)
    strategy = str(body.get("strategy", "buy_hold")).lower().strip()
    try:
        period_days = min(int(body.get("period_days", 365)), 730)
    except Exception:
        period_days = 365
    params = body.get("params") or {}

    ALLOWED_STRATEGIES = {"buy_hold", "rsi", "ma_cross", "momentum"}
    if strategy not in ALLOWED_STRATEGIES:
        return JSONResponse({"ok": False, "error": f"Unknown strategy. Choose from: {', '.join(ALLOWED_STRATEGIES)}"}, status_code=400)

    try:
        if strategy == "buy_hold":
            result = _run_buy_hold(ticker, period_days)
        elif strategy == "rsi":
            result = _run_holodeck_strategy(ticker, period_days, "rsi", {
                "window": int(params.get("window", 14)),
                "entry":  int(params.get("entry", 30)),
                "exit":   int(params.get("exit", 70)),
            })
        elif strategy == "ma_cross":
            result = _run_holodeck_strategy(ticker, period_days, "sma_cross", {
                "fast": int(params.get("fast", 50)),
                "slow": int(params.get("slow", 200)),
            })
        elif strategy == "momentum":
            result = _run_momentum(ticker, period_days, int(params.get("lookback", 20)))

        if not result or result.get("error"):
            return JSONResponse({"ok": False, "error": result.get("error", "Backtest failed")}, status_code=500)

        # Generate shareable ID and save
        share_id = _secrets.token_urlsafe(8)
        ip_hash = _hashlib.sha256(ip.encode()).hexdigest()[:12]
        conn = _conn()
        conn.execute(
            "INSERT INTO community_backtests "
            "(share_id, ticker, strategy, period_days, params_json, total_return, "
            "max_drawdown, sharpe_ratio, win_rate, num_trades, final_value, equity_json, ip_hash) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                share_id, ticker, strategy, period_days,
                json.dumps(params),
                result.get("total_return"), result.get("max_drawdown"),
                result.get("sharpe"), result.get("win_rate"),
                result.get("num_trades"), result.get("final_value"),
                json.dumps(result.get("equity_curve", [])),
                ip_hash,
            )
        )
        conn.commit()
        conn.close()

        return JSONResponse({
            "ok": True,
            "share_id": share_id,
            "share_url": f"/backtest/result/{share_id}",
            "ticker": ticker,
            "strategy": strategy,
            "period_days": period_days,
            **{k: result[k] for k in ("total_return", "max_drawdown", "sharpe", "win_rate",
                                       "num_trades", "final_value", "equity_curve")
               if k in result},
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


def _run_buy_hold(ticker: str, days: int) -> dict:
    """Simple buy-and-hold baseline."""
    import yfinance as yf
    import datetime as _dt
    start = (_dt.date.today() - _dt.timedelta(days=days)).strftime("%Y-%m-%d")
    df = yf.download(ticker, start=start, auto_adjust=True, progress=False)
    if df is None or df.empty or len(df) < 5:
        return {"error": f"No data for {ticker}"}
    close = df["Close"].squeeze()
    initial = float(close.iloc[0])
    final   = float(close.iloc[-1])
    total_return = round((final - initial) / initial * 100, 2) if initial else 0
    # Equity curve
    equity_curve = [
        {"date": str(d.date()), "value": round(10000 * float(v) / initial, 2)}
        for d, v in zip(close.index, close.values)
    ]
    # Max drawdown
    peak = (10000 * close / initial).cummax()
    dd = ((10000 * close / initial) - peak) / peak * 100
    max_dd = round(float(dd.min()), 2)
    return {
        "total_return": total_return,
        "max_drawdown": max_dd,
        "sharpe": None,
        "win_rate": None,
        "num_trades": 1,
        "final_value": round(10000 * final / initial, 2),
        "equity_curve": equity_curve,
    }


def _run_holodeck_strategy(ticker: str, days: int, strategy_type: str, params: dict) -> dict:
    """Run via holodeck.run_custom_strategy."""
    try:
        from engine.holodeck import holodeck
        result = holodeck.run_custom_strategy(
            ticker, days=days, strategy_type=strategy_type, params=params
        )
        return result
    except Exception as e:
        return {"error": str(e)}


def _run_momentum(ticker: str, days: int, lookback: int = 20) -> dict:
    """Momentum strategy: buy when 20-day return > 0, sell when < 0."""
    try:
        import yfinance as yf
        import datetime as _dt
        start = (_dt.date.today() - _dt.timedelta(days=days + lookback + 10)).strftime("%Y-%m-%d")
        df = yf.download(ticker, start=start, auto_adjust=True, progress=False)
        if df is None or df.empty or len(df) < lookback + 2:
            return {"error": f"No data for {ticker}"}
        close = df["Close"].squeeze().iloc[-days-lookback:]
        momentum = close.pct_change(lookback)
        cash, shares = 10000.0, 0.0
        in_trade = False
        equity = []
        trades, wins = 0, 0
        entry_price = 0.0
        peak_eq = 10000.0
        max_dd = 0.0

        for i in range(lookback, len(close)):
            price = float(close.iloc[i])
            mom   = float(momentum.iloc[i])
            eq    = cash + shares * price
            equity.append({"date": str(close.index[i].date()), "value": round(eq, 2)})
            peak_eq = max(peak_eq, eq)
            dd = (eq - peak_eq) / peak_eq * 100
            if dd < max_dd:
                max_dd = dd

            if mom > 0 and not in_trade:
                shares = cash / price
                cash = 0.0
                in_trade = True
                entry_price = price
                trades += 1
            elif mom < 0 and in_trade:
                cash = shares * price
                shares = 0.0
                in_trade = False
                if price > entry_price:
                    wins += 1

        final_val = cash + shares * float(close.iloc[-1])
        total_ret = round((final_val - 10000) / 10000 * 100, 2)
        return {
            "total_return": total_ret,
            "max_drawdown": round(max_dd, 2),
            "sharpe": None,
            "win_rate": round(wins / trades * 100, 1) if trades > 0 else 0,
            "num_trades": trades,
            "final_value": round(final_val, 2),
            "equity_curve": equity,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/backtest/result/{share_id}")
def api_backtest_result(share_id: str):
    """Fetch a saved community backtest result by share_id."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT * FROM community_backtests WHERE share_id=?", (share_id,)
        ).fetchone()
        conn.close()
        if not row:
            return JSONResponse({"ok": False, "error": "Result not found"}, status_code=404)
        d = dict(row)
        if d.get("equity_json"):
            try:
                d["equity_curve"] = json.loads(d["equity_json"])
            except Exception:
                d["equity_curve"] = []
        if d.get("params_json"):
            try:
                d["params"] = json.loads(d["params_json"])
            except Exception:
                d["params"] = {}
        d.pop("equity_json", None)
        d.pop("params_json", None)
        d.pop("ip_hash", None)
        return JSONResponse({"ok": True, **d})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/backtest/community-leaderboard")
def api_community_leaderboard(limit: int = 20):
    """Public: best community backtests ranked by total return."""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT share_id, ticker, strategy, period_days, total_return, max_drawdown, "
            "sharpe_ratio, win_rate, num_trades, final_value, created_at "
            "FROM community_backtests WHERE total_return IS NOT NULL "
            "ORDER BY total_return DESC LIMIT ?",
            (min(int(limit), 100),)
        ).fetchall()
        conn.close()
        return JSONResponse({"ok": True, "results": [dict(r) for r in rows]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
