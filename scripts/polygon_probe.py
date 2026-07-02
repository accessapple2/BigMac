#!/usr/bin/env python3
"""
scripts/polygon_probe.py -- HM-POLYGON-PROBE 2026-07-01.
Read-only capability probe of the current Polygon.io plan tier (Stocks Starter +
Options Starter). Makes GET requests only, never repoints any live code path.
Reports which endpoints work, real-time vs delayed, and which fields are null,
so a follow-up ticket can decide which of the 6 known Polygon-gap sites are
safe to repoint vs must stay on Alpaca/yfinance.

Usage: ./.venv/bin/python3 scripts/polygon_probe.py
"""
import sys
import time
import requests

sys.path.insert(0, ".")
import config  # noqa: E402 -- loads .env via load_dotenv(), never print config.POLYGON_API_KEY

TICKERS = ["SPY", "NVDA", "WDC"]
BASE = "https://api.polygon.io"


def _get(path, params=None):
    params = dict(params or {})
    params["apiKey"] = config.POLYGON_API_KEY
    t0 = time.perf_counter()
    try:
        r = requests.get(f"{BASE}{path}", params=params, timeout=15)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        try:
            body = r.json()
        except ValueError:
            body = {}
        return r.status_code, latency_ms, body, dict(r.headers)
    except requests.RequestException as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return None, latency_ms, {"error": str(e)}, {}


def _rate_limit_note(headers):
    # Polygon doesn't always expose standard X-RateLimit-* headers on Starter
    # tiers; report whatever is actually present rather than assuming a shape.
    keys = [k for k in headers if "ratelimit" in k.lower() or "retry" in k.lower()]
    if not keys:
        return "no rate-limit headers present in response"
    return ", ".join(f"{k}={headers[k]}" for k in keys)


def probe_last_quote(ticker):
    # v2 NBBO last-quote endpoint (stocks)
    code, ms, body, headers = _get(f"/v2/last/nbbo/{ticker}")
    status = body.get("status", "?")
    results = body.get("results") or {}
    populated = [k for k, v in results.items() if v not in (None, "", [])]
    return {
        "endpoint": f"/v2/last/nbbo/{ticker}",
        "http": code, "latency_ms": ms, "api_status": status,
        "fields_populated": populated,
        "rate_limit": _rate_limit_note(headers),
        "raw_status_snippet": str(body)[:200],
    }


def probe_snapshot_ticker(ticker):
    code, ms, body, headers = _get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}")
    status = body.get("status", "?")
    ticker_obj = body.get("ticker") or (body.get("tickers") or [{}])[0] if isinstance(body.get("tickers"), list) else body.get("ticker")
    ticker_obj = ticker_obj or {}
    populated = [k for k, v in ticker_obj.items() if v not in (None, "", [], {})]
    last_trade = ticker_obj.get("lastTrade") or {}
    last_quote = ticker_obj.get("lastQuote") or {}
    return {
        "endpoint": f"/v2/snapshot/.../tickers/{ticker}",
        "http": code, "latency_ms": ms, "api_status": status,
        "fields_populated": populated,
        "lastTrade_populated": bool(last_trade),
        "lastQuote_populated": bool(last_quote),
        "rate_limit": _rate_limit_note(headers),
        "raw_status_snippet": str(body)[:200],
    }


def probe_options_chain(ticker):
    code, ms, body, headers = _get(
        f"/v3/snapshot/options/{ticker}", params={"limit": 5}
    )
    status = body.get("status", "?")
    results = body.get("results") or []
    missing_fields = set()
    sample_fields = {}
    if results:
        sample = results[0]
        for key in ("day", "greeks", "last_quote", "last_trade", "open_interest", "implied_volatility"):
            val = sample.get(key)
            sample_fields[key] = "present" if val not in (None, {}, []) else "NULL"
            if val in (None, {}, []):
                missing_fields.add(key)
    return {
        "endpoint": f"/v3/snapshot/options/{ticker}",
        "http": code, "latency_ms": ms, "api_status": status,
        "n_contracts_returned": len(results),
        "sample_fields": sample_fields,
        "missing_fields": sorted(missing_fields),
        "rate_limit": _rate_limit_note(headers),
        "raw_status_snippet": str(body)[:200],
    }


def main():
    if not config.POLYGON_API_KEY:
        print("[polygon-probe] FATAL: POLYGON_API_KEY not set in environment. Aborting.", file=sys.stderr)
        sys.exit(1)

    print("=" * 78)
    print("POLYGON CAPABILITY PROBE -- 2026-07-01 (read-only, zero repointing)")
    print("=" * 78)

    all_results = {"last_quote": [], "snapshot": [], "options_chain": []}
    for ticker in TICKERS:
        print(f"\n--- {ticker} ---")

        r = probe_last_quote(ticker)
        all_results["last_quote"].append(r)
        print(f"[last_quote] http={r['http']} api_status={r['api_status']} "
              f"latency={r['latency_ms']}ms fields={r['fields_populated']}")
        if r["api_status"] not in ("OK", "ok"):
            print(f"             raw: {r['raw_status_snippet']}")

        time.sleep(0.3)
        r = probe_snapshot_ticker(ticker)
        all_results["snapshot"].append(r)
        print(f"[snapshot]   http={r['http']} api_status={r['api_status']} "
              f"latency={r['latency_ms']}ms lastTrade={r['lastTrade_populated']} "
              f"lastQuote={r['lastQuote_populated']} fields={r['fields_populated']}")
        if r["api_status"] not in ("OK", "ok"):
            print(f"             raw: {r['raw_status_snippet']}")

        time.sleep(0.3)
        r = probe_options_chain(ticker)
        all_results["options_chain"].append(r)
        print(f"[options]    http={r['http']} api_status={r['api_status']} "
              f"latency={r['latency_ms']}ms n_contracts={r['n_contracts_returned']} "
              f"sample_fields={r['sample_fields']}")
        if r["api_status"] not in ("OK", "ok"):
            print(f"             raw: {r['raw_status_snippet']}")

        time.sleep(0.3)

    print("\n" + "=" * 78)
    print("CAPABILITY MATRIX")
    print("=" * 78)
    print(f"{'endpoint':<40} {'works?':<8} {'latency':<10} {'delay':<12} {'fields missing'}")
    print("-" * 78)
    for r in all_results["last_quote"]:
        works = "PASS" if r["http"] == 200 and r["api_status"] in ("OK", "ok") else "FAIL"
        print(f"{r['endpoint']:<40} {works:<8} {r['latency_ms']}ms{'':<5} "
              f"{'unknown (see notes)':<12} "
              f"{'-' if r['fields_populated'] else 'ALL (empty results)'}")
    for r in all_results["snapshot"]:
        works = "PASS" if r["http"] == 200 and r["api_status"] in ("OK", "ok") else "FAIL"
        missing = "-" if (r["lastTrade_populated"] or r["lastQuote_populated"]) else "lastTrade/lastQuote"
        print(f"{r['endpoint']:<40} {works:<8} {r['latency_ms']}ms{'':<5} "
              f"{'unknown (see notes)':<12} {missing}")
    for r in all_results["options_chain"]:
        works = "PASS" if r["http"] == 200 and r["api_status"] in ("OK", "ok") and r["n_contracts_returned"] > 0 else "FAIL"
        missing = ", ".join(r["missing_fields"]) if r["missing_fields"] else "-"
        print(f"{r['endpoint']:<40} {works:<8} {r['latency_ms']}ms{'':<5} "
              f"{'unknown (see notes)':<12} {missing}")

    print("\nNOTE: 'delay' (real-time vs 15-min-delayed) cannot be determined from a")
    print("single snapshot alone -- it requires comparing the quote timestamp against")
    print("wall-clock time during market hours. See script output timestamps above")
    print("vs the run time for a manual delay estimate.")


if __name__ == "__main__":
    main()
