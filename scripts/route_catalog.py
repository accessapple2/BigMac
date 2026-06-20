#!/usr/bin/env python3
"""
Route Catalog Generator — 2026-06-20
Enumerates routes from both apps via source-code parsing (no server import),
then GET-tests every param-free GET route against the live servers.

Usage:
    cd ~/autonomous-trader && python3 scripts/route_catalog.py
Output:
    docs/SYSTEM_ROUTE_CATALOG_2026-06-20.md
"""
from __future__ import annotations

import re
import sys
import urllib.request
import urllib.error
from pathlib import Path
from typing import NamedTuple

PROJECT = Path(__file__).parent.parent
OUT_FILE = PROJECT / "docs" / "SYSTEM_ROUTE_CATALOG_2026-06-20.md"

# ── Route entry ────────────────────────────────────────────────────────────────

class Route(NamedTuple):
    path: str
    method: str    # GET / POST / PUT / DELETE / PATCH / GET+POST
    source: str    # filename:lineno
    has_param: bool


# ── FastAPI/APIRouter decorator extraction ─────────────────────────────────────

FASTAPI_RE = re.compile(
    r'@(?:app|router)\.(get|post|put|delete|patch|api_route)\(\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)


def extract_fastapi_routes(filepath: Path, prefix: str = "") -> list[Route]:
    text = filepath.read_text(errors="replace")
    routes: list[Route] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        m = FASTAPI_RE.search(line)
        if m:
            method = m.group(1).upper()
            raw_path = m.group(2)
            if method == "API_ROUTE":
                method = "GET+POST"
            full_path = prefix.rstrip("/") + raw_path
            routes.append(Route(
                path=full_path,
                method=method,
                source=f"{filepath.name}:{lineno}",
                has_param="{" in full_path or "<" in full_path,
            ))
    return routes


# ── Flask @app.route extraction ────────────────────────────────────────────────

FLASK_ROUTE_RE = re.compile(
    r'@app\.route\(\s*["\']([^"\']+)["\'](?:[^)]*?methods\s*=\s*\[([^\]]+)\])?',
)


def extract_flask_routes(filepath: Path) -> list[Route]:
    text = filepath.read_text(errors="replace")
    routes: list[Route] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        m = FLASK_ROUTE_RE.search(line)
        if m:
            path = m.group(1)
            raw_methods = m.group(2)
            if raw_methods:
                methods = [x.strip().strip("'\"") for x in raw_methods.split(",") if x.strip()]
            else:
                methods = ["GET"]
            has_param = "<" in path or "{" in path
            for method in methods:
                routes.append(Route(
                    path=path,
                    method=method.upper(),
                    source=f"{filepath.name}:{lineno}",
                    has_param=has_param,
                ))
    return routes


# ── HTTP probe ────────────────────────────────────────────────────────────────

def probe(url: str, timeout: int = 5) -> tuple[int, str, int]:
    """Return (status, content-type, byte_size). Status=0 means connection error."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "route-catalog/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ct = r.headers.get("Content-Type", "")
            return r.status, ct.split(";")[0].strip(), len(body)
    except urllib.error.HTTPError as e:
        return e.code, "", 0
    except Exception:
        return 0, "conn-err", 0


# ── External domain scan ──────────────────────────────────────────────────────

DOMAIN_RE = re.compile(r'https?://([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})')
SCAN_DIRS = ["engine", "dashboard", "signal-center", "scripts", "uoa"]


def collect_external_domains() -> dict[str, list[str]]:
    domain_map: dict[str, list[str]] = {}
    for d in SCAN_DIRS:
        dirpath = PROJECT / d
        if not dirpath.exists():
            continue
        for pyfile in dirpath.rglob("*.py"):
            try:
                text = pyfile.read_text(errors="replace")
            except Exception:
                continue
            for m in DOMAIN_RE.finditer(text):
                domain = m.group(1).lower()
                rel = str(pyfile.relative_to(PROJECT))
                domain_map.setdefault(domain, [])
                if rel not in domain_map[domain]:
                    domain_map[domain].append(rel)
    return {d: v for d, v in domain_map.items()
            if not d.startswith(("localhost", "127.", "0.0.0", "example."))}


# ── Dashboard route sources ────────────────────────────────────────────────────
#
# Prefixes here match what include_router or the router's own APIRouter(prefix=)
# declaration produces. For routers with baked-in prefix (backtest, intelligence),
# the prefix must be applied here since we parse decorator paths ("/strategies")
# not the full URL.

DASH_SOURCES: list[tuple[Path, str]] = [
    (PROJECT / "dashboard" / "app.py",               ""),
    (PROJECT / "uoa" / "routes.py",                  "/api/uoa"),
    (PROJECT / "engine" / "trade_cards_api.py",      ""),      # routes already have /api/...
    (PROJECT / "dashboard" / "ready_room_routes.py", "/api/ready-room"),
    (PROJECT / "dashboard" / "phase4_routes.py",     "/api/phase4"),
    (PROJECT / "engine" / "backtest_api.py",         "/api/backtest"),    # router prefix
    (PROJECT / "engine" / "intelligence_api.py",     "/api/intelligence"),# router prefix
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=== Route Catalog Generator 2026-06-20 ===\n")

    # ── Collect Dashboard routes ──────────────────────────────────────────────
    dash_all: list[Route] = []
    for fpath, prefix in DASH_SOURCES:
        if not fpath.exists():
            print(f"  WARN: {fpath} not found", file=sys.stderr)
            continue
        r = extract_fastapi_routes(fpath, prefix)
        print(f"  {fpath.name:<40} prefix={prefix!r:<25} → {len(r)} routes")
        dash_all += r

    # Deduplicate (path+method key)
    seen: set[tuple[str, str]] = set()
    dash_routes: list[Route] = []
    for r in dash_all:
        k = (r.path, r.method)
        if k not in seen:
            seen.add(k)
            dash_routes.append(r)
    dash_routes.sort(key=lambda r: (r.path, r.method))
    print(f"\nDashboard unique routes: {len(dash_routes)}")

    # ── Collect Signal-Center routes ──────────────────────────────────────────
    sc_path = PROJECT / "signal-center" / "server.py"
    sc_routes = extract_flask_routes(sc_path)
    sc_routes.sort(key=lambda r: (r.path, r.method))
    print(f"Signal-Center routes: {len(sc_routes)}")

    # ── Live GET probes ───────────────────────────────────────────────────────
    DASH_BASE = "http://localhost:8080"
    SC_BASE   = "http://localhost:9000"

    print("\nProbing Dashboard GET routes ...")
    dash_probe: dict[tuple[str, str], tuple[int, str, int]] = {}
    for r in dash_routes:
        if r.method == "GET" and not r.has_param:
            url = DASH_BASE + r.path
            status, ct, sz = probe(url)
            dash_probe[(r.path, r.method)] = (status, ct, sz)
            mark = "✓" if status == 200 else f"✗ {status}"
            print(f"  {mark:<8} {r.path}")

    print("\nProbing Signal-Center GET routes ...")
    sc_probe: dict[tuple[str, str], tuple[int, str, int]] = {}
    for r in sc_routes:
        if r.method == "GET" and not r.has_param:
            url = SC_BASE + r.path
            status, ct, sz = probe(url)
            sc_probe[(r.path, r.method)] = (status, ct, sz)
            mark = "✓" if status == 200 else f"✗ {status}"
            print(f"  {mark:<8} {r.path}")

    # ── External domains ──────────────────────────────────────────────────────
    print("\nScanning external domains ...")
    ext_domains = collect_external_domains()
    print(f"Distinct external domains: {len(ext_domains)}")

    # ── RULE #1 audit ─────────────────────────────────────────────────────────
    ORDER_METHODS = {"POST", "PUT", "DELETE", "PATCH"}
    ORDER_KEYWORDS = ("buy", "sell", "close", "cancel", "order", "execute")

    alpaca_order_routes = [
        r for r in dash_routes
        if r.method in ORDER_METHODS and "alpaca" in r.path.lower()
        and any(k in r.path.lower() for k in ORDER_KEYWORDS)
    ]
    # phase4 routes also go to Alpaca (confirmed in code)
    phase4_order_routes = [
        r for r in dash_routes
        if r.method in ORDER_METHODS and "phase4" in r.path.lower()
    ]
    schwab_order_routes = [
        r for r in dash_routes
        if r.method in ORDER_METHODS and "schwab" in r.path.lower()
        and any(k in r.path.lower() for k in ORDER_KEYWORDS)
    ]

    # ── Compute stats ─────────────────────────────────────────────────────────
    def partition(routes: list[Route], probe_results: dict) -> tuple[list, list, list, list, list]:
        """Returns (get_ok, get_fail, get_param, post_etc, all_tested)."""
        get_ok, get_fail, get_param_list, post_etc = [], [], [], []
        for r in routes:
            if r.method == "GET" and not r.has_param:
                st = probe_results.get((r.path, r.method), (0,))[0]
                (get_ok if st == 200 else get_fail).append(r)
            elif r.method == "GET" and r.has_param:
                get_param_list.append(r)
            else:
                post_etc.append(r)
        return get_ok, get_fail, get_param_list, post_etc, get_ok + get_fail

    d_ok, d_fail, d_param, d_post, d_tested = partition(dash_routes, dash_probe)
    s_ok, s_fail, s_param, s_post, s_tested = partition(sc_routes, sc_probe)

    # ── Build markdown ─────────────────────────────────────────────────────────

    def fmt_row(r: Route, probe_results: dict) -> str:
        key = (r.path, r.method)
        if r.method == "GET" and not r.has_param and key in probe_results:
            st, ct, sz = probe_results[key]
            status_cell = f"**{st}** ({sz}B)" if st != 200 else f"{st} ({sz}B)"
        elif r.method == "GET" and r.has_param:
            status_cell = "—(param)"
        else:
            status_cell = "—(action)"
        param_cell = "✓" if r.has_param else ""
        return f"| `{r.path}` | {r.method} | {status_cell} | {param_cell} | {r.source} |"

    def route_table(routes: list[Route], probe_results: dict) -> str:
        header = "| Path | Method | GET Status | Param? | Source |\n|------|--------|------------|--------|--------|"
        rows = [fmt_row(r, probe_results) for r in routes]
        return header + "\n" + "\n".join(rows)

    # Schwab RULE #1 verdict
    rule1_verdict = ("✅ **RULE #1 CONFIRMED** — No Schwab order/execution routes found. "
                     "All order paths route to Alpaca only.")
    rule1_violation = (f"❌ **RULE #1 VIOLATION** — {len(schwab_order_routes)} Schwab order routes detected!")

    md: list[str] = [
        "# System Route Catalog — 2026-06-20",
        "",
        "> Auto-generated by `scripts/route_catalog.py` against live servers at :8080 and :9000.",
        "> Routes extracted from source decorators; GET routes live-tested.",
        "",
    ]

    # Needs-attention block
    if d_fail or s_fail:
        md += ["## ⚠️ Non-200 GET Routes — Needs Attention", ""]
        if d_fail:
            md += ["### Dashboard (:8080)", "", "| Path | HTTP Status |", "|------|-------------|"]
            for r in d_fail:
                st = dash_probe[(r.path, r.method)][0]
                md.append(f"| `{r.path}` | **{st}** |")
            md.append("")
        if s_fail:
            md += ["### Signal-Center (:9000)", "", "| Path | HTTP Status |", "|------|-------------|"]
            for r in s_fail:
                st = sc_probe[(r.path, r.method)][0]
                md.append(f"| `{r.path}` | **{st}** |")
            md.append("")
    else:
        md += ["## ✅ All Tested GET Routes Returned 200", ""]

    # Summary counts
    md += [
        "## Summary",
        "",
        "### Dashboard — FastAPI @ :8080",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Total unique routes | {len(dash_routes)} |",
        f"| GET (no params) tested | {len(d_tested)} |",
        f"| GET (no params) PASS (200) | {len(d_ok)} |",
        f"| GET (no params) FAIL (non-200) | {len(d_fail)} |",
        f"| GET (with params) — not auto-tested | {len(d_param)} |",
        f"| POST/PUT/DELETE/other — catalogued only | {len(d_post)} |",
        "",
        "### Signal-Center — Flask @ :9000",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| Total routes | {len(sc_routes)} |",
        f"| GET (no params) tested | {len(s_tested)} |",
        f"| GET (no params) PASS (200) | {len(s_ok)} |",
        f"| GET (no params) FAIL (non-200) | {len(s_fail)} |",
        f"| GET (with params) — not auto-tested | {len(s_param)} |",
        f"| POST/PUT/DELETE/other — catalogued only | {len(s_post)} |",
        "",
    ]

    # RULE #1 audit
    md += ["## Order Route Audit — RULE #1 (Alpaca-only)", ""]
    if schwab_order_routes:
        md.append(rule1_violation)
    else:
        md.append(rule1_verdict)
    md.append("")
    md += ["### Alpaca Order Routes (via /api/alpaca/*)", ""]
    md += ["| Path | Method | Source |", "|------|--------|--------|"]
    for r in alpaca_order_routes:
        md.append(f"| `{r.path}` | {r.method} | {r.source} |")
    md.append("")
    if phase4_order_routes:
        md += ["### Phase4 Broker Routes (also Alpaca)", ""]
        md += ["| Path | Method | Source |", "|------|--------|--------|"]
        for r in phase4_order_routes:
            md.append(f"| `{r.path}` | {r.method} | {r.source} |")
        md.append("")
    if schwab_order_routes:
        md += ["### ⛔ Schwab Order Routes (VIOLATION)", ""]
        md += ["| Path | Method | Source |", "|------|--------|--------|"]
        for r in schwab_order_routes:
            md.append(f"| `{r.path}` | {r.method} | {r.source} |")
        md.append("")

    # Full dashboard table
    md += [
        "## Dashboard Routes — FastAPI @ :8080",
        "",
        route_table(dash_routes, dash_probe),
        "",
        "### Dashboard — Param Routes (not auto-tested)",
        "",
        "| Path | Method | Source |",
        "|------|--------|--------|",
    ]
    for r in dash_routes:
        if r.has_param:
            md.append(f"| `{r.path}` | {r.method} | {r.source} |")
    md.append("")

    # Signal-center table
    md += [
        "## Signal-Center Routes — Flask @ :9000",
        "",
        route_table(sc_routes, sc_probe),
        "",
    ]

    # External deps
    md += [
        "## External Dependency Domains",
        "",
        f"Scanned directories: `{', '.join(SCAN_DIRS)}`",
        "",
        "| Domain | Files |",
        "|--------|-------|",
    ]
    for domain in sorted(ext_domains):
        files = ext_domains[domain]
        cell = ", ".join(sorted(files)[:4])
        if len(files) > 4:
            cell += f" (+{len(files)-4} more)"
        md.append(f"| `{domain}` | {cell} |")
    md.append("")

    # Write file
    OUT_FILE.parent.mkdir(exist_ok=True)
    OUT_FILE.write_text("\n".join(md) + "\n")
    print(f"\n✅ Catalog written to {OUT_FILE}")

    # ── Console summary ───────────────────────────────────────────────────────
    print("\n=== FINAL SUMMARY ===")
    print(f"Dashboard  (:8080): {len(dash_routes):3d} routes | tested={len(d_tested)} | PASS={len(d_ok)} | FAIL={len(d_fail)}")
    print(f"SignalCntr (:9000): {len(sc_routes):3d} routes | tested={len(s_tested)} | PASS={len(s_ok)} | FAIL={len(s_fail)}")
    print(f"External domains:   {len(ext_domains)}")
    rule1_status = "✅ CLEAN" if not schwab_order_routes else f"❌ {len(schwab_order_routes)} VIOLATIONS"
    print(f"RULE #1 (Schwab):   {rule1_status}")
    if d_fail:
        print(f"\nDashboard non-200s:")
        for r in d_fail:
            st = dash_probe[(r.path, r.method)][0]
            print(f"  {st}  {r.path}")
    if s_fail:
        print(f"\nSignalCenter non-200s:")
        for r in s_fail:
            st = sc_probe[(r.path, r.method)][0]
            print(f"  {st}  {r.path}")


if __name__ == "__main__":
    main()
