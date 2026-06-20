#!/usr/bin/env python3
"""
route_catalog.py — Route & dependency enumerator for TradeMinds apps.

Parses routes WITHOUT importing app.py/server.py (no side effects).
Uses regex to extract decorators from source files.

Usage:
    python3 scripts/route_catalog.py

Output:
    Prints CSV-style route table to stdout.
    Safe to re-run (idempotent, read-only).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

ROOT = Path(__file__).parent.parent

# ── Source files to scan ──────────────────────────────────────────────────────

DASHBOARD_SOURCES = [
    # (filepath, url_prefix)
    (ROOT / "dashboard" / "app.py", ""),
    (ROOT / "dashboard" / "phase4_routes.py", "/api/phase4"),
    (ROOT / "dashboard" / "ready_room_routes.py", "/api/ready-room"),
    (ROOT / "dashboard" / "auth.py", ""),
    (ROOT / "engine" / "trade_cards_api.py", ""),           # no prefix (full paths in file)
    (ROOT / "engine" / "backtest_api.py", ""),              # prefix /api/backtest in router def
    (ROOT / "engine" / "intelligence_api.py", ""),          # prefix /api/intelligence in router def
    (ROOT / "uoa" / "routes.py", "/api/uoa"),
]

SIGNAL_CENTER_SOURCES = [
    (ROOT / "signal-center" / "server.py", ""),
]


@dataclass
class Route:
    source_file: str
    path: str
    method: str
    has_params: bool
    app: str  # "dashboard" | "signal-center"
    raw_line: int = 0
    notes: str = ""


# ── Regex patterns ────────────────────────────────────────────────────────────

# FastAPI / APIRouter: @app.get("/path") @router.post("/path")
FASTAPI_RE = re.compile(
    r'@(?:app|router)\.(get|post|put|delete|patch)\s*\(\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)

# Flask: @app.route("/path", methods=["GET","POST"])  or  @app.route("/path")
FLASK_ROUTE_RE = re.compile(
    r'@app\.route\s*\(\s*["\']([^"\']+)["\'](?:.*?methods\s*=\s*\[([^\]]*)\])?',
    re.IGNORECASE | re.DOTALL,
)

# Flask app.add_url_rule(...)
FLASK_ADD_RULE_RE = re.compile(
    r'app\.add_url_rule\s*\(\s*["\']([^"\']+)["\'].*?methods\s*=\s*\[([^\]]*)\]',
    re.IGNORECASE | re.DOTALL,
)

PARAM_RE = re.compile(r'[<{]')


def _parse_flask_methods(methods_str: Optional[str]) -> list[str]:
    if not methods_str:
        return ["GET"]
    return [m.strip().strip("\"'") for m in methods_str.split(",") if m.strip()]


def extract_routes_from_file(filepath: Path, prefix: str, app_label: str) -> list[Route]:
    """Parse route decorators from a source file without importing it."""
    if not filepath.exists():
        print(f"  [WARN] Not found: {filepath}", file=sys.stderr)
        return []

    content = filepath.read_text(errors="replace")
    lines = content.splitlines()
    routes: list[Route] = []

    # Check framework
    is_flask = "from flask import" in content or "Flask(__name__" in content
    is_fastapi = "from fastapi import" in content or "FastAPI(" in content

    if is_flask:
        # Flask: @app.route
        for i, line in enumerate(lines, 1):
            m = re.match(
                r'\s*@app\.route\s*\(\s*["\']([^"\']+)["\'](?:[^)]*?methods\s*=\s*\[([^\]]*)\])?',
                line
            )
            if m:
                path = prefix + m.group(1)
                raw_methods = m.group(2)
                methods = _parse_flask_methods(raw_methods)
                has_params = bool(PARAM_RE.search(path))
                for meth in methods:
                    routes.append(Route(
                        source_file=str(filepath.relative_to(ROOT)),
                        path=path,
                        method=meth.upper(),
                        has_params=has_params,
                        app=app_label,
                        raw_line=i,
                    ))

    elif is_fastapi:
        # FastAPI: @app.get / @router.post etc.
        for i, line in enumerate(lines, 1):
            m = FASTAPI_RE.match(line.strip())
            if m:
                method = m.group(1).upper()
                path = prefix + m.group(2)
                has_params = bool(PARAM_RE.search(path))
                routes.append(Route(
                    source_file=str(filepath.relative_to(ROOT)),
                    path=path,
                    method=method,
                    has_params=has_params,
                    app=app_label,
                    raw_line=i,
                ))

    return routes


def enumerate_all_routes() -> tuple[list[Route], list[Route]]:
    """Return (dashboard_routes, signal_center_routes)."""
    dashboard: list[Route] = []
    for filepath, prefix in DASHBOARD_SOURCES:
        dashboard.extend(extract_routes_from_file(filepath, prefix, "dashboard"))

    signal_center: list[Route] = []
    for filepath, prefix in SIGNAL_CENTER_SOURCES:
        signal_center.extend(extract_routes_from_file(filepath, prefix, "signal-center"))

    return dashboard, signal_center


def print_routes(routes: list[Route]) -> None:
    print(f"{'PATH':<60} {'METHOD':<8} {'HAS_PARAMS':<12} {'SOURCE'}")
    print("-" * 120)
    for r in sorted(routes, key=lambda x: (x.path, x.method)):
        print(f"{r.path:<60} {r.method:<8} {str(r.has_params):<12} {r.source_file}:{r.raw_line}")


def main() -> None:
    print("=" * 80)
    print("TradeMinds Route Catalog")
    print("=" * 80)

    dashboard_routes, sc_routes = enumerate_all_routes()

    print(f"\n=== DASHBOARD (:8080) — {len(dashboard_routes)} routes ===\n")
    print_routes(dashboard_routes)

    print(f"\n=== SIGNAL-CENTER (:9000) — {len(sc_routes)} routes ===\n")
    print_routes(sc_routes)

    # Summarize
    dash_get = [r for r in dashboard_routes if r.method == "GET" and not r.has_params]
    dash_mut = [r for r in dashboard_routes if r.method != "GET"]
    dash_param = [r for r in dashboard_routes if r.has_params]

    sc_get = [r for r in sc_routes if r.method == "GET" and not r.has_params]
    sc_mut = [r for r in sc_routes if r.method != "GET"]
    sc_param = [r for r in sc_routes if r.has_params]

    print("\n=== SUMMARY ===")
    print(f"Dashboard   total={len(dashboard_routes)}  GET_no_params={len(dash_get)}  mutation={len(dash_mut)}  has_params={len(dash_param)}")
    print(f"SignalCenter total={len(sc_routes)}  GET_no_params={len(sc_get)}  mutation={len(sc_mut)}  has_params={len(sc_param)}")


if __name__ == "__main__":
    main()
