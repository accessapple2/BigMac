"""
scripts/site_test.py — OllieTrades automated site health check

Tests every major API endpoint + auth flow.
Saves results to data/site_test_results.json.
"""

import json
import time
import os
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime

BASE = "http://127.0.0.1:8080"
TIMEOUT = 15

# Load credentials from .env
def _load_env():
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    creds = {"user": "", "pass": ""}
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("DASHBOARD_USER="):
                    creds["user"] = line.split("=", 1)[1].strip().strip('"')
                elif line.startswith("DASHBOARD_PASS="):
                    creds["pass"] = line.split("=", 1)[1].strip().strip('"')
    except Exception:
        pass
    return creds


def _get(path, cookie=None, timeout=TIMEOUT):
    url = BASE + path
    req = urllib.request.Request(url)
    if cookie:
        req.add_header("Cookie", cookie)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ms = int((time.time() - t0) * 1000)
            body = resp.read()
            return resp.status, ms, len(body), None
    except urllib.error.HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        return e.code, ms, 0, str(e)
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        return 0, ms, 0, str(e)


def _post_login(username, password):
    """POST /login, capture Set-Cookie header without following redirect."""
    url = BASE + "/login"
    data = urllib.parse.urlencode({"username": username, "password": password}).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    opener = urllib.request.build_opener(NoRedirect())
    t0 = time.time()
    try:
        with opener.open(req, timeout=TIMEOUT) as resp:
            ms = int((time.time() - t0) * 1000)
            cookie = _extract_session_cookie(resp.headers.get("Set-Cookie", ""))
            return resp.status, ms, cookie
    except urllib.error.HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        headers = e.headers if hasattr(e, 'headers') else {}
        cookie = _extract_session_cookie(headers.get("Set-Cookie", "") if headers else "")
        return e.code, ms, cookie
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        return 0, ms, None


def _extract_session_cookie(set_cookie_header):
    if not set_cookie_header or "trademinds_session=" not in set_cookie_header:
        return None
    for part in set_cookie_header.split(";"):
        part = part.strip()
        if part.startswith("trademinds_session="):
            return part
    return None


# Endpoints to test: (path, label, expected_status, max_ms)
ENDPOINTS = [
    ("/api/health",              "Health check",          200, 2000),
    ("/api/regime",              "Market regime",         200, 5000),
    ("/api/fleet/positions",     "Fleet positions",       200, 5000),   # /api/positions 404s — correct path is fleet/positions
    ("/api/trades",              "Trades",                200, 12000),
    ("/api/trades/recent",       "Recent trades",         200, 3000),
    ("/api/arena/leaderboard",   "Arena leaderboard",     200, 5000),
    ("/api/recent-signals",      "Recent signals",        200, 5000),
    ("/api/premarket-watchlist", "Premarket watchlist",   200, 5000),
    ("/api/screener/quality",    "Screener quality",      200, 8000),
    ("/api/capital",             "Capital",               200, 3000),
    ("/api/market-movers",       "Market movers",         200, 8000),
    ("/api/status",              "System status",         200, 3000),
]


def run():
    creds = _load_env()
    results = {
        "run_at": datetime.now().isoformat(),
        "base_url": BASE,
        "endpoints": [],
        "auth_tests": [],
        "summary": {}
    }

    print(f"\n{'='*65}")
    print(f"  OllieTrades Site Test — {results['run_at'][:19]}")
    print(f"  Target: {BASE}")
    print(f"{'='*65}\n")

    # ── Auth tests ───────────────────────────────────────────────────
    print("AUTH TESTS")
    print("-" * 65)

    # 1. Valid login
    login_status, login_ms, session_cookie = _post_login(creds["user"], creds["pass"])
    login_ok = session_cookie is not None
    results["auth_tests"].append({
        "test": "POST /login with valid credentials",
        "status": login_status, "ms": login_ms, "pass": login_ok,
        "detail": "session cookie returned" if login_ok else f"no cookie — status {login_status}"
    })
    print(f"  [{'PASS' if login_ok else 'FAIL'}] Login (valid creds)      "
          f"status={login_status}  {login_ms}ms  "
          f"{'cookie OK' if login_ok else 'NO COOKIE'}")

    # 2. No-auth protection — GET / without session should redirect to /login.
    #    Must use no-redirect opener so we see the 3xx, not the final login page.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    _nr_opener = urllib.request.build_opener(NoRedirect())
    t0 = time.time()
    try:
        with _nr_opener.open(urllib.request.Request(BASE + "/"), timeout=TIMEOUT) as r:
            root_status, root_ms = r.status, int((time.time()-t0)*1000)
    except urllib.error.HTTPError as e:
        root_status, root_ms = e.code, int((time.time()-t0)*1000)
    except Exception:
        root_status, root_ms = 0, int((time.time()-t0)*1000)
    auth_enforced = root_status in (301, 302, 303)
    results["auth_tests"].append({
        "test": "GET / without session (expect redirect to /login)",
        "status": root_status, "ms": root_ms, "pass": auth_enforced,
        "detail": "redirected to /login" if auth_enforced else f"unexpected {root_status} — not redirecting"
    })
    print(f"  [{'PASS' if auth_enforced else 'FAIL'}] GET / no-auth            "
          f"status={root_status}  {root_ms}ms  "
          f"{'redirected OK' if auth_enforced else 'NOT REDIRECTING'}")

    # 3. Bad credentials
    bad_status, bad_ms, bad_cookie = _post_login(creds["user"], "wrongpassword1234")
    bad_ok = bad_cookie is None
    results["auth_tests"].append({
        "test": "POST /login with wrong password (expect no cookie)",
        "status": bad_status, "ms": bad_ms, "pass": bad_ok,
        "detail": "correctly rejected" if bad_ok else "GAVE COOKIE ON BAD PASSWORD"
    })
    print(f"  [{'PASS' if bad_ok else 'FAIL'}] Login (bad password)      "
          f"status={bad_status}  {bad_ms}ms  "
          f"{'rejected OK' if bad_ok else 'GAVE SESSION!'}")
    print()

    if not session_cookie:
        print("  FATAL: Login failed — cannot proceed with endpoint tests.")
        print(f"  Hint: Check DASHBOARD_USER/DASHBOARD_PASS in .env\n")
        results["summary"] = {"fatal": "Login failed", "passed": 0, "failed": 3}
        _save(results)
        return

    # ── Endpoint tests ────────────────────────────────────────────────
    print("ENDPOINT TESTS  (authenticated)")
    print("-" * 65)
    print(f"  {'Endpoint':<35} {'Status':>6}  {'ms':>6}  {'KB':>6}  Result")
    print(f"  {'-'*35}  {'------':>6}  {'------':>6}  {'------':>6}  ------")

    total_ms = 0
    passed = 0
    failed = 0
    slowest = ("", 0)
    errors = []

    for path, label, expect_status, max_ms in ENDPOINTS:
        status, ms, size, err = _get(path, cookie=session_cookie)
        ok = (status == expect_status) and (ms <= max_ms)
        kb = size / 1024

        note = ""
        if status != expect_status:
            note = f"got {status} expected {expect_status}"
            ok = False
        elif ms > max_ms:
            note = f"SLOW ({ms}ms > {max_ms}ms)"
            ok = False
        elif err:
            note = err[:50]

        flag = "PASS" if ok else "FAIL"
        print(f"  [{flag}] {path:<35} {status:>6}  {ms:>6}  {kb:>5.1f}k  {note}")

        if ms > slowest[1]:
            slowest = (path, ms)
        total_ms += ms
        if ok:
            passed += 1
        else:
            failed += 1
            errors.append(f"{path}: {note or err or 'failed'}")

        results["endpoints"].append({
            "path": path, "label": label,
            "status": status, "ms": ms, "size_bytes": size,
            "pass": ok, "error": err, "note": note
        })

    auth_passed = sum(1 for t in results["auth_tests"] if t["pass"])
    auth_failed = len(results["auth_tests"]) - auth_passed
    total_passed = passed + auth_passed
    total_failed = failed + auth_failed

    results["summary"] = {
        "total_endpoints": len(ENDPOINTS),
        "endpoints_passed": passed,
        "endpoints_failed": failed,
        "auth_passed": auth_passed,
        "auth_failed": auth_failed,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "total_load_ms": total_ms,
        "slowest_endpoint": slowest[0],
        "slowest_ms": slowest[1],
        "errors": errors,
    }

    print()
    print("=" * 65)
    print("  REPORT CARD")
    print("=" * 65)
    print(f"  Endpoints:  {passed}/{len(ENDPOINTS)} passed")
    print(f"  Auth tests: {auth_passed}/{len(results['auth_tests'])} passed")
    print(f"  Total load: {total_ms}ms across {len(ENDPOINTS)} endpoints")
    print(f"  Avg/call:   {total_ms // len(ENDPOINTS)}ms")
    print(f"  Slowest:    {slowest[0]}  ({slowest[1]}ms)")
    if errors:
        print(f"\n  Failures:")
        for e in errors:
            print(f"    ✗ {e}")
    if total_failed == 0:
        print(f"\n  ✓ ALL {total_passed} TESTS PASSED")
    else:
        print(f"\n  ✗ {total_failed} TEST(S) FAILED  ({total_passed} passed)")
    print()

    _save(results)


def _save(results):
    out = os.path.join(os.path.dirname(__file__), '..', 'data', 'site_test_results.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"  Results → data/site_test_results.json\n")


if __name__ == "__main__":
    run()
