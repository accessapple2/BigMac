"""Congressional Trades Scraper — free sources (Capitol Trades + Quiver Quantitative).

Capitol Trades: HTML scrape from capitoltrades.com/trades
Quiver Quant: JSON API from api.quiverquant.com (free, no key needed)
"""
from __future__ import annotations
import requests
import time
import re
from rich.console import Console

console = Console()

_congress_cache = {"data": None, "ts": 0}
_CACHE_TTL = 1800  # 30 minutes


def scrape_capitol_trades(limit: int = 50, pages: int = 4) -> list:
    """Scrape recent trades from capitoltrades.com (multiple pages)."""
    trades = []
    from scrapling import Adaptor

    _headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com/search?q=capitoltrades",
    }

    def _cell_text(cell) -> str:
        return cell.get_all_text(separator="", strip=True)

    for page_num in range(1, pages + 1):
      try:
        url = f"https://www.capitoltrades.com/trades?page={page_num}" if page_num > 1 else "https://www.capitoltrades.com/trades"
        resp = requests.get(url, headers=_headers, timeout=20)
        if resp.status_code != 200:
            console.log(f"[yellow]Capitol Trades page {page_num} HTTP {resp.status_code}")
            break

        page = Adaptor(resp.text, url=url)
        rows = page.css("tbody tr")
        if not rows:
            break

        for row in rows[:limit]:
            cells = row.css("td")
            if len(cells) < 8:
                continue

            # Cell 0: Politician (e.g., "Mitch McConnellRepublicanSenateKY")
            pol_full = _cell_text(cells[0])
            pol_name = pol_full
            for splitter in ["Democrat", "Republican"]:
                if splitter in pol_name:
                    pol_name = pol_name[:pol_name.index(splitter)].strip()
                    break
            party = "D" if "Democrat" in pol_full else "R" if "Republican" in pol_full else ""

            # Cell 1: Company + Ticker (e.g., "Wells Fargo & CoWFC:US")
            issuer_text = _cell_text(cells[1])
            ticker_match = re.search(r'([A-Z]{1,5}):US', issuer_text)
            if ticker_match:
                ticker = ticker_match.group(1)
                company = issuer_text[:ticker_match.start()].strip()
            else:
                # No public ticker — private fund or treasury
                ticker = ""
                company = issuer_text.replace("N/A", "").strip()

            # Cell 2: Filed date (e.g., "19 Mar2026")
            filed_raw = _cell_text(cells[2])
            # Cell 3: Trade date
            traded_raw = _cell_text(cells[3])

            # Fix date spacing: "19 Mar2026" → "19 Mar 2026"
            filed = re.sub(r'(\w{3})(\d{4})', r'\1 \2', filed_raw)
            traded = re.sub(r'(\w{3})(\d{4})', r'\1 \2', traded_raw)

            # Cell 5: Owner
            owner = _cell_text(cells[5]) if len(cells) > 5 else ""

            # Cell 6: Type (buy/sell)
            type_text = _cell_text(cells[6]).lower() if len(cells) > 6 else ""
            trade_type = "BUY" if "buy" in type_text or "purchase" in type_text else "SELL"

            # Cell 7: Size
            size = _cell_text(cells[7]) if len(cells) > 7 else ""

            # Skip entries without a public ticker (private funds, treasuries)
            if not ticker:
                continue

            trades.append({
                "politician": pol_name[:50],
                "party": party,
                "ticker": ticker,
                "company": company[:60],
                "type": trade_type,
                "size": size,
                "trade_date": traded,
                "filed_date": filed,
                "owner": owner,
                "source": "Capitol Trades",
                "source_url": "https://www.capitoltrades.com/trades",
            })

        if len(trades) >= limit:
            break
        time.sleep(0.5)  # polite delay between pages
      except Exception as e:
        console.log(f"[yellow]Capitol Trades page {page_num} error: {e}")
        break

    console.log(f"[cyan]Capitol Trades: scraped {len(trades)} trades from {pages} pages")
    return trades[:limit]


def scrape_quiver_quant(limit: int = 50) -> list:
    """Fetch recent trades from Quiver Quantitative free API."""
    trades = []
    try:
        r = requests.get(
            "https://api.quiverquant.com/beta/live/congresstrading",
            headers={"User-Agent": "TradeMinds/1.0 (Congress Tracker)"},
            timeout=15,
        )
        if not r.ok:
            console.log(f"[yellow]Quiver Quant HTTP {r.status_code} — API may require auth, using Capitol Trades only")
            return trades

        data = r.json()
        if not isinstance(data, list):
            return trades

        for entry in data[:limit]:
            ticker = (entry.get("Ticker") or "").strip()
            if not ticker or len(ticker) > 5:
                continue

            politician = entry.get("Representative", "Unknown")
            party = entry.get("Party", "")
            transaction = entry.get("Transaction", "")
            trade_type = "BUY" if "purchase" in transaction.lower() else "SELL"
            size = entry.get("Range", "")
            filed_date = entry.get("ReportDate", "")
            trade_date = entry.get("TransactionDate", "")

            trades.append({
                "politician": politician[:50],
                "party": party,
                "ticker": ticker,
                "type": trade_type,
                "size": size,
                "trade_date": trade_date,
                "filed_date": filed_date,
                "excess_return": "",
                "source": "Quiver Quantitative",
                "source_url": f"https://www.quiverquant.com/congresstrading/stock/{ticker}",
            })

        console.log(f"[cyan]Quiver Quant: fetched {len(trades)} trades")
    except Exception as e:
        console.log(f"[yellow]Quiver Quant error: {e}")

    return trades


def get_all_congress_trades() -> list:
    """Get merged, cached congress trades from all free sources."""
    now = time.time()
    if _congress_cache["data"] and now - _congress_cache["ts"] < _CACHE_TTL:
        return _congress_cache["data"]

    all_trades = []

    # Run both scrapers with a pause between to avoid rate limiting
    try:
        all_trades.extend(scrape_quiver_quant())
    except Exception as e:
        console.log(f"[red]Quiver Quant failed: {e}")

    time.sleep(1)

    try:
        all_trades.extend(scrape_capitol_trades())
    except Exception as e:
        console.log(f"[red]Capitol Trades failed: {e}")

    # Deduplicate by ticker + politician prefix + trade_date
    seen = set()
    unique = []
    for t in all_trades:
        key = f"{t.get('ticker','')}-{t.get('politician','')[:15]}-{t.get('trade_date','')}"
        if key not in seen:
            seen.add(key)
            unique.append(t)

    # Sort by filed date descending
    unique.sort(key=lambda x: x.get("filed_date", ""), reverse=True)

    _congress_cache["data"] = unique
    _congress_cache["ts"] = now
    return unique


def get_congress_trades_for_ticker(symbol: str) -> list:
    """Get congress trades for a specific ticker."""
    return [t for t in get_all_congress_trades() if t.get("ticker", "").upper() == symbol.upper()]
