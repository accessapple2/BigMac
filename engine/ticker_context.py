"""
ticker_context.py  --  Grounding layer for USS OllieTrades War Room agents.

Fixes agents inventing the company behind a symbol (WDC -> "Washington Digital
Corridor") and fabricating price levels, by prepending an authoritative
FACTUAL CONTEXT block before each agent speaks. If nothing resolves, it
instructs the agent to ABSTAIN (NEUTRAL) instead of hallucinating.
"""

import os
import requests

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")
_BASE = "https://api.polygon.io"
_TIMEOUT = 6

SYMBOL_MAP = {
    "WDC":  ("Western Digital Corporation", "Technology / Data Storage (HDD & memory)"),
    "NVDA": ("NVIDIA Corporation",          "Technology / Semiconductors (GPUs)"),
    "MU":   ("Micron Technology",           "Technology / Memory & Semiconductors"),
    "PANW": ("Palo Alto Networks",          "Technology / Cybersecurity"),
    "VST":  ("Vistra Corp",                 "Utilities / Power Generation"),
    "CEG":  ("Constellation Energy",        "Utilities / Nuclear Power"),
    "FCX":  ("Freeport-McMoRan",            "Materials / Copper & Gold Mining"),
    "VOO":  ("Vanguard S&P 500 ETF",        "Broad-Market Equity ETF"),
    "SPY":  ("SPDR S&P 500 ETF Trust",      "Broad-Market Equity ETF"),
    "QQQ":  ("Invesco QQQ Trust",           "Nasdaq-100 ETF"),
    "JEPQ": ("JPMorgan Nasdaq Equity Premium Income ETF", "Covered-Call / Income ETF"),
    "STX":  ("Seagate Technology",          "Technology / Data Storage (HDD)"),
    "SNDK": ("SanDisk Corporation",         "Technology / Flash Memory"),
    "MRVL": ("Marvell Technology",          "Technology / Semiconductors"),
}


def resolve_symbol(symbol):
    sym = symbol.upper().strip()
    if sym in SYMBOL_MAP:
        name, sector = SYMBOL_MAP[sym]
        return {"name": name, "sector": sector, "source": "static"}
    if POLYGON_API_KEY:
        try:
            r = requests.get(f"{_BASE}/v3/reference/tickers/{sym}",
                             params={"apiKey": POLYGON_API_KEY}, timeout=_TIMEOUT)
            if r.ok:
                res = r.json().get("results", {})
                name = res.get("name")
                sector = res.get("sic_description") or res.get("type") or "Unknown sector"
                if name:
                    return {"name": name, "sector": sector, "source": "polygon"}
        except requests.RequestException:
            pass
    return {"name": None, "sector": None, "source": "unresolved"}


def price_context(symbol):
    if not POLYGON_API_KEY:
        return None
    sym = symbol.upper().strip()
    out = {}
    try:
        r = requests.get(
            f"{_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{sym}",
            params={"apiKey": POLYGON_API_KEY}, timeout=_TIMEOUT)
        if r.ok:
            t = r.json().get("ticker", {})
            last = (t.get("lastTrade") or {}).get("p") or (t.get("day") or {}).get("c")
            if last:
                out["price"] = float(last)
            if t.get("todaysChangePerc") is not None:
                out["pct_change"] = float(t["todaysChangePerc"])
    except requests.RequestException:
        pass
    try:
        import datetime as _dt
        end = _dt.date.today()
        start = end - _dt.timedelta(days=10)
        r = requests.get(
            f"{_BASE}/v2/aggs/ticker/{sym}/range/1/day/{start}/{end}",
            params={"apiKey": POLYGON_API_KEY, "limit": 10}, timeout=_TIMEOUT)
        if r.ok:
            bars = r.json().get("results", []) or []
            if bars:
                lows = [b["l"] for b in bars if "l" in b]
                highs = [b["h"] for b in bars if "h" in b]
                if lows and highs:
                    out["low5"] = min(lows)
                    out["high5"] = max(highs)
    except requests.RequestException:
        pass
    return out or None


def build_grounding_block(symbol):
    sym = symbol.upper().strip()
    meta = resolve_symbol(sym)
    px = price_context(sym)
    if not meta["name"] and not px:
        return ("FACTUAL CONTEXT: No verified data is available for "
                f"{sym}. Do NOT guess what this company is or invent price levels. "
                "Respond NEUTRAL | Conviction: 0/10 | insufficient data to ground a view.")
    lines = ["FACTUAL CONTEXT (authoritative — do NOT contradict or embellish):"]
    if meta["name"]:
        lines.append(f"- {sym} = {meta['name']} ({meta['sector']}).")
    else:
        lines.append(f"- {sym}: company name unverified — do not guess it.")
    if px:
        if "price" in px and "pct_change" in px:
            lines.append(f"- Last price ${px['price']:.2f} ({px['pct_change']:+.2f}% today).")
        elif "price" in px:
            lines.append(f"- Last price ${px['price']:.2f}.")
        if "low5" in px and "high5" in px:
            lines.append(f"- Real 5-day range: ${px['low5']:.2f} – ${px['high5']:.2f}.")
    lines.append("- Do NOT invent support/resistance, earnings dates, or price targets "
                 "beyond the numbers above. If you cite a level, it must come from this block.")
    return "\n".join(lines)


if __name__ == "__main__":
    for s in ["WDC", "NVDA", "SPY", "ZZZZ"]:
        print(f"\n=== {s} ===")
        print(build_grounding_block(s))
