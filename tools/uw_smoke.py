"""
UnusualWhales endpoint-path confirmation harness.

Purpose
-------
Confirm candidate API paths return required fields BEFORE any path is written
into engine/uw_client.py ENDPOINTS.  Run one capability at a time; update the
path once the row prints OK.

Usage
-----
    export UW_API_KEY="<your key>"
    # UW_BASE defaults to the confirmed base URL; override only if it changes
    .venv/bin/python tools/uw_smoke.py [capability_name ...]

    # Run confirmed earnings first (unblocks guard + session fix):
    .venv/bin/python tools/uw_smoke.py earnings_bmo earnings_amc

    # Run all:
    .venv/bin/python tools/uw_smoke.py

Interpreting output
-------------------
  OK  + fields:all        → write that path into ENDPOINTS in engine/uw_client.py
  OK  + MISSING [...]     → path exists but schema differs; find the real field
                            names in docs, update the normalizer, don't force-map
  403                     → tier-gated; scanner section stays UNVERIFIED — be
                            honest rather than faking it
  ts_field: NONE-stamp-fetch-time
                          → no data timestamp; stamp with fetch time and label
                            source=uw-fetch so freshness badge can't lie
  ERROR                   → network / auth failure; check UW_BASE and UW_API_KEY

After all rows print OK: see "After green" steps in the spec (task B).
"""
from __future__ import annotations

import os
import sys
import textwrap

try:
    import requests
except ImportError:
    sys.exit("pip install requests first (use .venv)")

# ---------------------------------------------------------------------------
# Auth — read from env; never hard-code or log
# ---------------------------------------------------------------------------

# Base URL confirmed from UW docs + OpenAPI spec (2025-10-22).
# Override via UW_BASE env var only if the upstream URL changes.
_BASE = os.environ.get("UW_BASE", "https://api.unusualwhales.com").rstrip("/")
_KEY  = os.environ.get("UW_API_KEY", "")

if not _KEY:
    sys.exit(
        "Set UW_API_KEY before running.\n"
        "  export UW_API_KEY='<your key>'\n"
        "Auth is Bearer token; update _HEADERS below if UW changes the scheme."
    )

# Adjust to confirmed auth format (Bearer / query param / custom header).
_HEADERS = {"Authorization": f"Bearer {_KEY}"}

# ---------------------------------------------------------------------------
# Capability table
#
# candidate_path — a path to TRY from the UW docs. Replace with the actual
#                  path once you've found it in the documentation.
#                  *** Do NOT treat these as confirmed paths — verify first. ***
#
# required_fields — the fields this codebase depends on. If the smoke test
#                   reports MISSING, find the real field name in the JSON
#                   and update the normalizer in uw_client.py accordingly.
# ---------------------------------------------------------------------------

CHECKS: dict[str, tuple[str, list[str]]] = {
    # CONFIRMED paths (from UW docs + OpenAPI 2025-10-22) — run these first.
    # Session is derived from which endpoint the ticker appears in, not a field.
    # Required fields here are what the row must contain; exact key names
    # (report_date vs date, ticker vs symbol) are still confirmed by smoke output.
    "earnings_bmo":   ("/api/earnings/premarket",  ["ticker", "date"]),   # BMO reporters
    "earnings_amc":   ("/api/earnings/afterhours", ["ticker", "date"]),   # AMC reporters

    # FILL_FROM_OPENAPI — get exact path strings from:
    #   https://api.unusualwhales.com/api/openapi   (machine-readable)
    #   https://api.unusualwhales.com/docs          (Accept: text/plain)
    # Required fields below use the REAL UW field names (corrected from prior placeholders).
    "flow_alerts":    ("FILL_FROM_OPENAPI", [
        "ticker", "type", "strike", "expiry",
        "total_premium", "total_size", "has_sweep",
    ]),
    "short_interest": ("FILL_FROM_OPENAPI", [
        "short_interest", "days_to_cover",
    ]),
    "greek_exposure": ("FILL_FROM_OPENAPI", [
        "strike", "gamma_exposure",
    ]),
    "option_chain":   ("FILL_FROM_OPENAPI", [
        "strike", "expiry", "bid", "ask",
    ]),
}

# Fields that indicate when the data was generated (check in order).
_TS_CANDIDATES = ("updated_at", "timestamp", "as_of", "date", "created_at")


def _probe(name: str, path: str, required: list[str]) -> str:
    """Hit one candidate path and report the verdict."""
    if path.startswith("FILL_FROM_OPENAPI"):
        return f"{name:<16} SKIP    path not yet filled in — get from OpenAPI spec first"
    url = _BASE + path
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
    except Exception as exc:
        return f"{name:<16} ERROR   {exc}"

    if resp.status_code == 401:
        return f"{name:<16} 401     auth failed — check UW_API_KEY and auth header format"
    if resp.status_code == 403:
        return f"{name:<16} 403     tier-gated — not in your plan; this section stays UNVERIFIED"
    if resp.status_code == 404:
        return f"{name:<16} 404     path not found — check UW docs for the real path"
    if not resp.ok:
        snippet = resp.text[:120].replace("\n", " ")
        return f"{name:<16} {resp.status_code}     {snippet}"

    try:
        body = resp.json()
    except Exception:
        return f"{name:<16} PARSE   response is not JSON — check Accept header"

    # Unwrap common envelope shapes
    data = body.get("data") or body
    row  = data[0] if isinstance(data, list) else data

    if not isinstance(row, dict):
        return f"{name:<16} SHAPE   could not extract a row dict from response"

    missing = [f for f in required if f not in row]
    ts_field = next((k for k in _TS_CANDIDATES if k in row), None)

    ts_note = ts_field if ts_field else "NONE-stamp-fetch-time (mark source=uw-fetch)"
    field_note = "all" if not missing else f"MISSING {missing}"

    return f"{name:<16} OK      fields:{field_note}  ts_field:{ts_note}"


def main(caps: list[str] | None = None) -> None:
    targets = caps or list(CHECKS)
    unknown = [c for c in targets if c not in CHECKS]
    if unknown:
        sys.exit(f"Unknown capability name(s): {unknown}\nValid: {list(CHECKS)}")

    print(f"Smoke against: {_BASE}\n")
    print(f"{'Capability':<16} {'Result'}")
    print("-" * 72)
    for name in targets:
        path, req = CHECKS[name]
        print(_probe(name, path, req))
    print()
    print(textwrap.dedent("""
        Next steps per row:
          OK + fields:all  → write that path into ENDPOINTS in engine/uw_client.py
          MISSING [...]    → wrong field names; find real names in docs, update normalizer
          403              → tier-gated; leave capability UNVERIFIED in ENDPOINTS
          NONE ts          → stamp fetch time; mark source=uw-fetch in normalizer
    """).strip())


if __name__ == "__main__":
    main(sys.argv[1:] or None)
