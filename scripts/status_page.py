#!/usr/bin/env python3
"""status.ollietrades.com — public, no-auth, up/down health for bigmac,
Ollie Max, trader service, and the cloudflared tunnel.

Admiral-approved 2026-07-02. Deliberately minimal: no auth, no secrets,
no HTTP write paths -- read-only health checks only, safe to expose
publicly. The heartbeat file below (HM-STATUS-HEARTBEAT) is a purely
internal, local-disk write from a background thread -- not reachable or
influenced by any HTTP request, so it doesn't change that posture.
"""
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import urllib.request

PORT = 8090
TRADER_URL = "http://localhost:8080/api/status"

# HM-STATUS-HEARTBEAT (2026-08-29, HM-STATUSPAGE-FREEZE-2026-08-29 follow-up):
# _build_status()/do_GET() only ever compute "checked_at" AT REQUEST TIME --
# there was no independent heartbeat at all. During a Fri 21:54 -> Sat 09:25
# window with zero incoming requests to the page, "Last checked" froze for
# ~11.5h looking exactly like an outage (the process itself never crashed or
# restarted -- confirmed live, same PID spanning the whole window). An
# external watchdog caught it after ~11.5h; nothing internal did.
#
# This background thread runs the SAME checks on a fixed 5-min cadence,
# independent of HTTP traffic, and persists the result to a local JSON
# sidecar. hm_ops_sentinel.py reads THIS file's age (not the web page's
# displayed timestamp, which would trivially "look fresh" the instant
# anything -- including the sentinel's own probe -- requests the page).
_HEARTBEAT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", ".status_page_heartbeat.json",
)
_HEARTBEAT_INTERVAL_S = 300  # 5 min


def _write_heartbeat() -> None:
    while True:
        try:
            status = _build_status()
            tmp_path = _HEARTBEAT_PATH + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(status, f)
            os.replace(tmp_path, _HEARTBEAT_PATH)
        except Exception:
            pass  # never let a heartbeat-write failure kill the loop
        time.sleep(_HEARTBEAT_INTERVAL_S)


def _check_http(url: str, timeout: float = 4.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return 200 <= r.status < 500  # anything short of a hard 5xx counts as "up"
    except Exception:
        return False


def _check_tunnel() -> bool:
    try:
        out = subprocess.run(
            ["pgrep", "-f", "cloudflared tunnel"], capture_output=True, timeout=3
        )
        return out.returncode == 0
    except Exception:
        return False


def _build_status() -> dict:
    # bigmac: this script only runs if bigmac itself is up -- trivially true.
    bigmac_up = True
    trader_up = _check_http(TRADER_URL)
    tunnel_up = _check_tunnel()
    return {
        "bigmac": bigmac_up,
        "trader": trader_up,
        "tunnel": tunnel_up,
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }


def _render_html(status: dict) -> str:
    def row(label, ok):
        color = "#22c55e" if ok else "#ef4444"
        text = "UP" if ok else "DOWN"
        return (
            f'<div style="display:flex;justify-content:space-between;padding:10px 16px;'
            f'border-bottom:1px solid #333;"><span>{label}</span>'
            f'<span style="color:{color};font-weight:700;">{text}</span></div>'
        )

    rows = (
        row("bigmac (Mac Mini)", status["bigmac"])
        + row("Trader service", status["trader"])
        + row("Cloudflare Tunnel", status["tunnel"])
    )
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>OllieTrades Status</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body {{ background:#0a0a0f; color:#e5e5e5; font-family:-apple-system,sans-serif;
        margin:0; padding:24px; }}
.card {{ max-width:480px; margin:40px auto; background:#111118; border-radius:12px;
         overflow:hidden; border:1px solid #333; }}
.head {{ padding:16px; font-size:18px; font-weight:700; border-bottom:1px solid #333; }}
.foot {{ padding:10px 16px; font-size:12px; color:#888; }}
</style></head>
<body>
<div class="card">
  <div class="head">OllieTrades — System Status</div>
  {rows}
  <div class="foot">Last checked: {status['checked_at']} · Paper trading only, no real money.</div>
</div>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # keep this quiet; it's polled frequently

    def do_GET(self):
        status = _build_status()
        if self.path == "/api/status":
            body = json.dumps(status).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        body = _render_html(status).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    threading.Thread(target=_write_heartbeat, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[status_page] serving on :{PORT}")
    server.serve_forever()
