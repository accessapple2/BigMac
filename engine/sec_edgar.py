"""SEC EDGAR integration — fetch and summarize filings for watchlist stocks."""
from __future__ import annotations
import requests
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from rich.console import Console

import config

console = Console()

_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions"
_EDGAR_HEADERS = {"User-Agent": "TradeMinds research@trademinds.local", "Accept": "application/json"}

# CIK lookup cache
_cik_cache = {}
_CIK_FILE = Path("data/sec_cik_cache.json")


def _load_cik_cache():
    global _cik_cache
    if _CIK_FILE.exists():
        try:
            _cik_cache = json.loads(_CIK_FILE.read_text())
        except Exception:
            pass


def _save_cik_cache():
    _CIK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _CIK_FILE.write_text(json.dumps(_cik_cache, indent=2))


def get_cik(symbol: str) -> str | None:
    """Look up SEC CIK number for a ticker symbol."""
    _load_cik_cache()
    sym = symbol.upper()
    if sym in _cik_cache:
        return _cik_cache[sym]

    try:
        r = requests.get(
            "https://www.sec.gov/cgi-bin/browse-edgar",
            params={
                "action": "getcompany",
                "company": sym,
                "CIK": sym,
                "type": "10-K",
                "dateb": "",
                "owner": "include",
                "count": "1",
                "search_text": "",
                "output": "atom",
            },
            headers=_EDGAR_HEADERS,
            timeout=10,
        )
        if r.status_code == 200:
            # Try ticker->CIK mapping file instead
            r2 = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers=_EDGAR_HEADERS,
                timeout=10,
            )
            if r2.status_code == 200:
                data = r2.json()
                for entry in data.values():
                    if entry.get("ticker", "").upper() == sym:
                        cik = str(entry["cik_str"]).zfill(10)
                        _cik_cache[sym] = cik
                        _save_cik_cache()
                        return cik
    except Exception as e:
        console.log(f"[red]SEC CIK lookup error: {e}")
    return None


def get_recent_filings(symbol: str, form_types: list = None) -> list:
    """Get recent SEC filings for a symbol.
    form_types: ['10-K', '10-Q', '8-K'] etc. Default: 10-K, 10-Q, 8-K.
    """
    if form_types is None:
        form_types = ["10-K", "10-Q", "8-K"]

    cik = get_cik(symbol)
    if not cik:
        # Fallback: try Finnhub SEC filings
        try:
            from engine.finnhub_data import get_sec_filings
            return get_sec_filings(symbol)
        except Exception:
            return []

    try:
        url = f"{_EDGAR_SUBMISSIONS}/CIK{cik}.json"
        r = requests.get(url, headers=_EDGAR_HEADERS, timeout=10)
        if r.status_code != 200:
            return []

        data = r.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocDescription", [])

        results = []
        for i in range(min(len(forms), 50)):
            if forms[i] in form_types:
                acc = accessions[i].replace("-", "")
                results.append({
                    "symbol": symbol.upper(),
                    "form": forms[i],
                    "filed_date": dates[i] if i < len(dates) else "",
                    "description": descriptions[i] if i < len(descriptions) else "",
                    "url": f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc}",
                })
                if len(results) >= 10:
                    break
        return results
    except Exception as e:
        console.log(f"[red]SEC EDGAR error for {symbol}: {e}")
        return []


def build_ai_context(symbol: str) -> str:
    """Build SEC filing context for AI model prompts."""
    filings = get_recent_filings(symbol)
    if not filings:
        return ""

    recent = filings[:3]
    parts = []
    for f in recent:
        filed = f.get("filed_date", "")
        if filed >= (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d"):
            parts.append(f"{symbol} SEC {f['form']} filed {filed}")

    return " | ".join(parts) if parts else ""
