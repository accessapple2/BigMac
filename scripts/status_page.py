#!/usr/bin/env python3
"""status.ollietrades.com — public, no-auth, up/down health for bigmac,
Ollie Max, trader service, and the cloudflared tunnel.

Admiral-approved 2026-07-02. Deliberately minimal: no auth, no secrets,
no write paths -- read-only health checks only, safe to expose publicly.
"""
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import urllib.request

PORT = 8090
TRADER_URL = "http://localhost:8080/api/status"


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
            import json
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
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[status_page] serving on :{PORT}")
    server.serve_forever()
