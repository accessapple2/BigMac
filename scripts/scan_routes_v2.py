#!/usr/bin/env python3
"""
scripts/scan_routes_v2.py — Hardened route scanner for autonomous-trader.

Fixes 5 defect classes (route_catalog_corrections.md):
  1. Non-fetch caller detection (onclick/dynamic-URL/Python-HTTP/curl)
  2. Exclude .bak* / backup files from scan scope
  3. Dedupe include_router (one registration, not source + mounting file)
  4. Exact path-prefix match — /api/user-agents/ != /api/agents/
  5. Resolve mount prefixes via file-scoped import tracking so that
     multiple sub-routers sharing the variable name "router" in their
     own files don't collide in a global alias map

Verdicts:
  KEEP     — caller(s) found OR sub-router on a mounted router (conservative)
  ORPHANED — on app directly, no callers found
  SHADOW   — same method+effective_path from two different source files

This script NEVER executes cuts. RULE #7 holds.

Usage:
  python3 scripts/scan_routes_v2.py             # full catalog, stdout
  python3 scripts/scan_routes_v2.py --gate      # 15-route acceptance gate only
  python3 scripts/scan_routes_v2.py --out FILE  # write catalog to FILE
"""

import os
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict

# ── config ─────────────────────────────────────────────────────────────────────
ROOT = Path(os.environ.get("AT_ROOT", Path.home() / "autonomous-trader"))

EXCL_DIR_NAMES = frozenset({
    ".venv", ".venv-backtest", ".venv-crew", ".venv-deps", ".venv-recall",
    "__pycache__", "node_modules", "_archive", "backups", "backups_archive",
    ".git", ".claude", ".github", ".githooks", "venv",
})
# Defect 2: never scan bak / backup files
BAK_EXACT = (".bak",)
BAK_SUB   = (".bak_", ".backup", ".bak.")
ROUTE_EXTS  = frozenset({".py"})
CALLER_EXTS = frozenset({".py", ".html", ".js", ".ts", ".sh"})

# ── data ───────────────────────────────────────────────────────────────────────
@dataclass
class Route:
    method: str
    raw_path: str
    effective_path: str
    src_file: str          # relative to ROOT
    src_line: int
    router_var: str
    callers: list = field(default_factory=list)
    verdict: str = "UNKNOWN"

@dataclass
class Mount:
    def_file: str          # file where router var is DEFINED
    var_name: str          # var name IN that definition file
    prefix: str
    mount_file: str

@dataclass
class JSDispatcher:
    fn_name: str
    url_prefix: str

# ── utilities ──────────────────────────────────────────────────────────────────
def is_excluded(p: Path) -> bool:
    for part in p.parts:
        if part in EXCL_DIR_NAMES:
            return True
    name = p.name
    for sub in BAK_SUB:
        if sub in name:
            return True
    for suf in BAK_EXACT:
        if name.endswith(suf):
            return True
    return False


def files(exts: frozenset) -> list[Path]:
    return sorted(
        p for p in ROOT.rglob("*")
        if p.is_file() and p.suffix in exts and not is_excluded(p)
    )


def rel(p: Path) -> str:
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def read(p: Path) -> list[str]:
    try:
        return p.read_text(errors="replace").splitlines()
    except OSError:
        return []


def module_to_relfile(module: str) -> str:
    """Convert 'engine.trade_cards_api' → 'engine/trade_cards_api.py'."""
    return module.replace(".", "/") + ".py"

# ── route-definition patterns ──────────────────────────────────────────────────
# FastAPI: @var.get("/path")  @var.post("/path", ...)
RE_FASTAPI = re.compile(
    r'@(\w+)\.(get|post|put|delete|patch|options|head)\(\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)
# Flask: @var.route("/path", methods=["GET", "POST"])
RE_FLASK = re.compile(
    r'@(\w+)\.route\(\s*["\']([^"\']+)["\'](?:[^)]*methods\s*=\s*\[([^\]]*)\])?',
    re.IGNORECASE,
)
# include_router(var, prefix="...")
RE_INCLUDE = re.compile(
    r'\.include_router\(\s*(\w+)(?:[^)]*?prefix\s*=\s*["\']([^"\']*)["\'])?',
)
# register_blueprint(var, url_prefix="...")
RE_BP_REG = re.compile(
    r'\.register_blueprint\(\s*(\w+)(?:[^)]*?url_prefix\s*=\s*["\']([^"\']*)["\'])?',
)
# from module import name as alias
RE_FROM_AS = re.compile(r'from\s+([\w.]+)\s+import\s+(\w+)\s+as\s+(\w+)')
# from module import name1, name2  (no 'as', handles comma lists)
RE_FROM_PLAIN = re.compile(r'from\s+([\w.]+)\s+import\s+((?:\w+(?:\s*,\s*\w+)*))(?:\s*$|\s*#)')

# ── caller-detection patterns ──────────────────────────────────────────────────
# fetch("/api/path")
RE_FETCH = re.compile(r"""fetch\(\s*["'](/api/[^"'?#]+)""")
# JS dispatcher: function foo(param) { ... fetch('/api/prefix/' + param ... }
RE_JS_DISP = re.compile(
    r"function\s+(\w+)\s*\(\s*\w+\s*\)(?:[^{]*)\{(?:[^}]*?)fetch\s*\(\s*['\"]([^'\"]+)['\"]\s*\+",
    re.DOTALL,
)
# onclick="fn('arg')" or onclick='fn("arg")'
RE_ONCLICK = re.compile(r"""onclick=["'](\w+)\(['"]([\w/. -]+)['"]\)""")
# Python HTTP: requests.get/post(BASE + "/api/path")
RE_PY_CONCAT = re.compile(
    r'(?:requests|httpx|aiohttp)\.\w+\(\s*\w+\s*\+\s*["\']([/][^"\']+)["\']'
)
# Python HTTP: requests.get("http://host/api/path") or requests.get("/api/path")
RE_PY_FULL = re.compile(
    r'(?:requests|httpx|aiohttp)\.\w+\(\s*(?:f["\']|["\'])(?:http://[\w.:]+)?(/api/[^"\'{}]+)["\']'
)
# curl / wget
RE_CURL = re.compile(r'curl\b[^#\n]*?(?:http://[\w.:]+)?(/api/[\w/{}._-]+)')
RE_WGET = re.compile(r'wget\b[^#\n]*?(?:http://[\w.:]+)?(/api/[\w/{}._-]+)')

# ── pass 1: collect route definitions ─────────────────────────────────────────
def collect_routes(py_files: list[Path]) -> list[Route]:
    routes: list[Route] = []
    for path in py_files:
        lines = read(path)
        src = rel(path)
        for i, line in enumerate(lines, 1):
            # Skip comment lines — prevents false matches in doc comments
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            # FastAPI
            m = RE_FASTAPI.search(line)
            if m:
                var, verb, raw = m.group(1), m.group(2).upper(), m.group(3)
                routes.append(Route(verb, raw, raw, src, i, var))
                continue
            # Flask
            m = RE_FLASK.search(line)
            if m:
                var, raw = m.group(1), m.group(2)
                methods_str = m.group(3) or ""
                verbs = [
                    v.strip().strip("'\"").upper()
                    for v in methods_str.split(",")
                    if v.strip().strip("'\"")
                ] or ["GET"]
                for verb in verbs:
                    routes.append(Route(verb, raw, raw, src, i, var))
    return routes


# ── pass 2: build file-scoped import maps and mount table ─────────────────────
def collect_mounts(py_files: list[Path]) -> tuple[list[Mount], dict]:
    # Per-file import map: file → {local_name → (def_file, orig_name)}
    import_maps: dict[str, dict[str, tuple[str, str]]] = defaultdict(dict)

    for path in py_files:
        lines = read(path)
        src = rel(path)
        for line in lines:
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            # from module import name as alias
            m = RE_FROM_AS.search(line)
            if m:
                module, orig, alias = m.group(1), m.group(2), m.group(3)
                import_maps[src][alias] = (module_to_relfile(module), orig)
                continue
            # from module import name1, name2
            m = RE_FROM_PLAIN.search(line)
            if m:
                module = m.group(1)
                def_file = module_to_relfile(module)
                for name in [n.strip() for n in m.group(2).split(",")]:
                    name = name.strip()
                    if name:
                        import_maps[src][name] = (def_file, name)

    mounts: list[Mount] = []
    for path in py_files:
        lines = read(path)
        src = rel(path)
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            for pattern, prefix_group in ((RE_INCLUDE, 2), (RE_BP_REG, 2)):
                m = pattern.search(line)
                if m:
                    local_var = m.group(1)
                    prefix = m.group(prefix_group) or ""
                    # Resolve local_var to (def_file, orig_name)
                    if local_var in import_maps[src]:
                        def_file, orig_name = import_maps[src][local_var]
                    else:
                        # Defined in the same file (e.g., sub-app defined locally)
                        def_file, orig_name = src, local_var
                    mounts.append(Mount(def_file, orig_name, prefix, src))

    return mounts, import_maps


# ── pass 3: resolve effective paths ───────────────────────────────────────────
def resolve_paths(routes: list[Route], mounts: list[Mount]) -> set[tuple[str, str]]:
    # Mount map: (def_file, var_name) → prefix (first mount wins)
    mount_map: dict[tuple[str, str], str] = {}
    for m in mounts:
        key = (m.def_file, m.var_name)
        if key not in mount_map:
            mount_map[key] = m.prefix

    mounted_keys: set[tuple[str, str]] = set()
    for r in routes:
        key = (r.src_file, r.router_var)
        if key in mount_map:
            r.effective_path = mount_map[key] + r.raw_path
            mounted_keys.add(key)
        else:
            r.effective_path = r.raw_path
    return mounted_keys


# ── pass 4: detect JS dispatcher functions ─────────────────────────────────────
def collect_js_dispatchers(caller_files: list[Path]) -> list[JSDispatcher]:
    dispatchers: list[JSDispatcher] = []
    for path in caller_files:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        for m in RE_JS_DISP.finditer(text):
            fn_name   = m.group(1)
            url_prefix = m.group(2)
            if url_prefix.startswith("/api/"):
                dispatchers.append(JSDispatcher(fn_name, url_prefix))
    return dispatchers


# ── pass 5: detect callers ────────────────────────────────────────────────────
def detect_callers(routes: list[Route], caller_files: list[Path],
                   dispatchers: list[JSDispatcher]) -> None:
    disp_map = {d.fn_name: d.url_prefix for d in dispatchers}
    route_paths = {r.effective_path for r in routes}

    def match_route(url: str) -> str | None:
        if url in route_paths:
            return url
        for rp in route_paths:
            if _path_matches(rp, url):
                return rp
        return None

    def add_caller(rpath: str, src: str, lineno: int) -> None:
        ref = f"{src}:{lineno}"
        for r in routes:
            if r.effective_path == rpath and ref not in r.callers:
                r.callers.append(ref)

    for path in caller_files:
        lines = read(path)
        src = rel(path)
        for i, line in enumerate(lines, 1):
            # fetch("/api/...")
            for m in RE_FETCH.finditer(line):
                url = m.group(1).rstrip("/")
                rp = match_route(url)
                if rp:
                    add_caller(rp, src, i)
            # onclick="fn('arg')"
            for m in RE_ONCLICK.finditer(line):
                fn, arg = m.group(1), m.group(2)
                if fn in disp_map:
                    url = (disp_map[fn] + arg).rstrip("/")
                    rp = match_route(url)
                    if rp:
                        add_caller(rp, src, i)
            # Python HTTP concat
            for m in RE_PY_CONCAT.finditer(line):
                url = m.group(1).rstrip("/")
                rp = match_route(url)
                if rp:
                    add_caller(rp, src, i)
            # Python HTTP full URL
            for m in RE_PY_FULL.finditer(line):
                url = m.group(1).rstrip("/")
                rp = match_route(url)
                if rp:
                    add_caller(rp, src, i)
            # curl / wget
            for pattern in (RE_CURL, RE_WGET):
                for m in pattern.finditer(line):
                    url = m.group(1).rstrip("/")
                    rp = match_route(url)
                    if rp:
                        add_caller(rp, src, i)


def _path_matches(template: str, url: str) -> bool:
    t_parts = template.split("/")
    u_parts = url.split("/")
    if len(t_parts) != len(u_parts):
        return False
    for t, u in zip(t_parts, u_parts):
        if t.startswith("{") and t.endswith("}"):
            continue
        if t != u:
            return False
    return True


# ── pass 6: classify ──────────────────────────────────────────────────────────
def classify(routes: list[Route], mounted_keys: set[tuple[str, str]]) -> None:
    for r in routes:
        if r.callers:
            r.verdict = "KEEP"
        elif (r.src_file, r.router_var) in mounted_keys:
            # Conservative: sub-router route on a mounted router is accessible
            r.verdict = "KEEP"
        else:
            r.verdict = "ORPHANED"


# ── pass 7: detect shadows ────────────────────────────────────────────────────
def detect_shadows(routes: list[Route]) -> None:
    # Defect 4: exact effective_path comparison (no prefix stripping)
    # Defect 3: only one entry per route (definition file only, not mount file)
    seen: dict[tuple, list[Route]] = defaultdict(list)
    for r in routes:
        seen[(r.method, r.effective_path)].append(r)
    for key, group in seen.items():
        if len(group) > 1:
            srcs = {r.src_file for r in group}
            if len(srcs) > 1:
                for r in group:
                    r.verdict = "SHADOW"


# ── acceptance gate ────────────────────────────────────────────────────────────
GATE_KEEP = [
    "/api/morpheus/action/fire-kirk",
    "/api/morpheus/action/refresh-schwab",
    "/api/morpheus/action/run-advisory-team",
    "/api/crew/sunday",
    "/api/crew/learning/sync",
    "/api/portfolios/{portfolio_id}",
    "/api/portfolios/positions/close",
]
GATE_ORPHANED = [
    "/api/rallies/import-trades",
    "/api/execute-trade",
    "/api/execution-log",
    "/api/morpheus/action/mark-acted-on",
]
GATE_NO_SHADOW = [
    "/api/wheel/status",
    "/api/wheel/{ticker}",
    "/api/agents/{player_id}/pause",
    "/api/user-agents/{agent_id}/pause",
]


def run_gate(routes: list[Route]) -> bool:
    by_path: dict[str, list[Route]] = defaultdict(list)
    for r in routes:
        by_path[r.effective_path].append(r)

    results: list[tuple[str, str, str, bool]] = []

    for path in GATE_KEEP:
        rs = by_path.get(path, [])
        actual = rs[0].verdict if rs else "MISSING"
        results.append((path, "KEEP", actual, actual == "KEEP"))

    for path in GATE_ORPHANED:
        rs = by_path.get(path, [])
        actual = rs[0].verdict if rs else "MISSING"
        results.append((path, "ORPHANED", actual, actual == "ORPHANED"))

    for path in GATE_NO_SHADOW:
        rs = by_path.get(path, [])
        if not rs:
            results.append((path, "NO-SHADOW", "MISSING", False))
        else:
            shadowed = any(r.verdict == "SHADOW" for r in rs)
            actual = "SHADOW" if shadowed else rs[0].verdict
            ok = not shadowed and actual != "MISSING"
            results.append((path, "NO-SHADOW", actual, ok))

    passed = sum(1 for *_, ok in results if ok)
    total  = len(results)

    print(f"\n{'═'*72}")
    print(f"  ACCEPTANCE GATE — {passed}/{total} pass")
    print(f"{'═'*72}")
    print(f"  {'ROUTE':<50} {'EXPECTED':<12} {'ACTUAL':<12} STATUS")
    print(f"  {'-'*50} {'-'*12} {'-'*12} ------")
    for path, expected, actual, ok in results:
        status = "PASS" if ok else "FAIL ←"
        print(f"  {path:<50} {expected:<12} {actual:<12} {status}")
    print(f"{'═'*72}\n")
    return passed == total


# ── catalog output ─────────────────────────────────────────────────────────────
def print_catalog(routes: list[Route], out=None) -> None:
    f = open(out, "w") if out else sys.stdout
    hdr = f"{'VERDICT':<12} {'METHOD':<7} {'EFFECTIVE PATH':<55} {'FILE':<50} LINE  CALLERS"
    print(hdr, file=f)
    print("-" * len(hdr), file=f)
    for r in sorted(routes, key=lambda x: (x.verdict, x.effective_path)):
        callers = "; ".join(r.callers[:3]) + ("…" if len(r.callers) > 3 else "")
        print(
            f"{r.verdict:<12} {r.method:<7} {r.effective_path:<55} "
            f"{r.src_file:<50} {r.src_line:<5} {callers}",
            file=f,
        )
    if out:
        f.close()
        print(f"[scan] Catalog written to {out}")


# ── main ───────────────────────────────────────────────────────────────────────
def main():
    gate_only = "--gate" in sys.argv
    out_file  = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--out" and i < len(sys.argv) - 1:
            out_file = sys.argv[i + 1]

    print(f"[scan] Root: {ROOT}", file=sys.stderr)

    py_files     = files(ROUTE_EXTS)
    caller_files = files(CALLER_EXTS)
    print(f"[scan] {len(py_files)} Python files, {len(caller_files)} caller files",
          file=sys.stderr)

    routes               = collect_routes(py_files)
    mounts, import_maps  = collect_mounts(py_files)
    mounted_keys         = resolve_paths(routes, mounts)
    dispatchers          = collect_js_dispatchers(caller_files)
    detect_callers(routes, caller_files, dispatchers)
    classify(routes, mounted_keys)
    detect_shadows(routes)

    print(f"[scan] {len(routes)} routes, {len(mounts)} mounts, "
          f"{len(dispatchers)} JS dispatchers", file=sys.stderr)

    ok = run_gate(routes)

    if gate_only:
        sys.exit(0 if ok else 1)

    if out_file:
        print_catalog(routes, out_file)
    else:
        print_catalog(routes)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
