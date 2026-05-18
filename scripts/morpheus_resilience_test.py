"""HM-MORPHEUS-RESILIENCE-TEST-HARNESS (2026-05-17)

Banked from HM-MORPHEUS Ship 1 verification (commit 39bded7) — the in-vivo
resilience test that day failed to demonstrate per-source failure because
Python's `sys.modules` cache survived file renames. This harness monkey-patches
each loader IN-PROCESS, then exercises `morpheus_awareness` to verify each
source-failure path produces sources_failed += 1 + sources_loaded -= 1 cleanly.

Usage:
  cd ~/autonomous-trader
  venv/bin/python3 scripts/morpheus_resilience_test.py
  venv/bin/python3 scripts/morpheus_resilience_test.py --loader portfolio

No production code touched. Imports signal-center/server.py as a module + patches
loaders via monkeypatch pattern. Verifies the per-source try/except contract
holds against actual loader failures (not just code-inspection trust).
"""
import argparse
import importlib.util
import json
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
SC = REPO / "signal-center"


def _load_server_module():
    """Import signal-center/server.py as a module without binding to a port."""
    sys.path.insert(0, str(REPO))
    sys.path.insert(0, str(SC))
    spec = importlib.util.spec_from_file_location("sc_server", str(SC / "server.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


LOADERS = [
    "_morpheus_load_portfolio",
    "_morpheus_load_kirk_alerts",
    "_morpheus_load_advisory",
    "_morpheus_load_intelligence",
    "_morpheus_load_signals",
    "_morpheus_load_predictions",
    "_morpheus_load_commits",
]


def _baseline_call(mod):
    """Call morpheus_awareness with all loaders nominal. Returns parsed JSON dict."""
    with mod.app.test_request_context("/api/morpheus/awareness"):
        # Reset cache to force fresh call
        mod._MORPHEUS_CACHE = {"data": None, "ts": 0.0, "etag": None}
        resp = mod.morpheus_awareness()
        body = resp.get_data(as_text=True)
        return json.loads(body)


def _break_loader_and_call(mod, loader_name):
    """Replace one loader with a raise-Exception lambda + reset cache + call."""
    original = getattr(mod, loader_name)
    def _broken(*a, **kw):
        raise RuntimeError(f"resilience-test injected failure in {loader_name}")
    setattr(mod, loader_name, _broken)
    try:
        with mod.app.test_request_context("/api/morpheus/awareness"):
            mod._MORPHEUS_CACHE = {"data": None, "ts": 0.0, "etag": None}
            resp = mod.morpheus_awareness()
            body = resp.get_data(as_text=True)
            return json.loads(body)
    finally:
        setattr(mod, loader_name, original)


def main(only=None):
    print("Loading signal-center/server.py as module (no port bind)...")
    mod = _load_server_module()
    print("Helpers present:", all(hasattr(mod, n) for n in LOADERS))

    print("\n=== BASELINE call (all loaders nominal) ===")
    base = _baseline_call(mod)
    base_loaded = set(base.get("sources_loaded", []))
    base_failed = base.get("sources_failed", [])
    print(f"  sources_loaded: {sorted(base_loaded)} ({len(base_loaded)} of 7)")
    print(f"  sources_failed: {len(base_failed)}")
    if len(base_failed) > 0:
        for f in base_failed:
            print(f"    - {f}")

    targets = [only] if only else LOADERS
    pass_count = 0
    fail_count = 0
    print(f"\n=== PER-LOADER RESILIENCE TEST ({len(targets)} loader(s)) ===")
    for loader in targets:
        loader_source_name = loader.replace("_morpheus_load_", "")
        result = _break_loader_and_call(mod, loader)
        loaded = set(result.get("sources_loaded", []))
        failed = result.get("sources_failed", [])
        failed_names = {f.get("source") for f in failed}
        ok = (
            loader_source_name not in loaded
            and loader_source_name in failed_names
            and result.get("ts") is not None
        )
        flag = "PASS" if ok else "FAIL"
        print(f"  {flag}: break={loader_source_name:18s} loaded={sorted(loaded)} failed={sorted(failed_names)}")
        if not ok:
            fail_count += 1
            print(f"    expected failed_names contains '{loader_source_name}'; got {sorted(failed_names)}")
        else:
            pass_count += 1

    print()
    total = pass_count + fail_count
    print(f"RESULT: {pass_count}/{total} loaders pass resilience contract.")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HM-MORPHEUS resilience test harness")
    parser.add_argument("--loader", help="Test only one loader (e.g. portfolio, kirk_alerts)")
    args = parser.parse_args()
    only_full = f"_morpheus_load_{args.loader}" if args.loader else None
    sys.exit(main(only=only_full))
