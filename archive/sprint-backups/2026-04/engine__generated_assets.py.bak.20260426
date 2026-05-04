"""
generated_assets.py — Custom AI-generated indexes for TradeMinds.

User enters a plain-English investment thesis → Ollama parses it into
screening criteria → universe_stocks is queried → matched stocks scored/
weighted → stored as a named index → VectorBT backtests it vs SPY.
"""
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import requests

from config import OLLIE_URL as _OLLIE_URL

logger = logging.getLogger("generated_assets")

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
OLLAMA_URL = os.environ.get("OLLAMA_URL", _OLLIE_URL)  # Ollie Box GPU (was localhost)
MAX_HOLDINGS = 25


# ── DB ─────────────────────────────────────────────────────────────────────────

def _conn():
    return sqlite3.connect(DB_PATH, timeout=30)


def _init_table():
    with _conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS generated_indexes (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT NOT NULL,
                thesis       TEXT NOT NULL,
                criteria     TEXT NOT NULL,
                holdings     TEXT NOT NULL,
                backtest_30  TEXT,
                backtest_90  TEXT,
                backtest_365 TEXT,
                created_at   TEXT DEFAULT (datetime('now')),
                status       TEXT DEFAULT 'ACTIVE'
            )
        """)
        db.commit()


# ── Ollama parse ───────────────────────────────────────────────────────────────

_PARSE_SYSTEM = """You are a quantitative analyst. Convert an investment thesis into screening criteria.
Return ONLY valid JSON with these exact keys (use null for anything unknown):
{
  "sector": "Technology",
  "keywords": ["AI", "infrastructure", "cloud"],
  "min_market_cap": 1000000000,
  "max_price": 500.0,
  "min_price": null,
  "fundamental_filters": {
    "min_revenue_growth": null,
    "max_pe": null,
    "min_margin": null
  }
}
Output JSON only — no explanation, no markdown fences."""


def parse_thesis(thesis: str) -> Optional[dict]:
    """Convert plain-English thesis to structured screening criteria via Ollama."""
    model = "qwen3:8b"  # 2026-04-24: qwen3.5:9b renamed to qwen3:8b
    try:
        ps = requests.get(f"{OLLAMA_URL}/api/ps", timeout=3).json()
        loaded = (ps.get("models") or [{}])[0].get("name", "")
        if loaded:
            model = loaded
    except Exception:
        pass

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": _PARSE_SYSTEM},
                    {"role": "user",   "content": thesis},
                ],
                "stream": False,
                "options": {"temperature": 0.05, "num_predict": 400},
            },
            timeout=45,
        )
        raw = r.json().get("message", {}).get("content", "").strip()
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip().rstrip("`").strip()
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception as e:
        logger.error(f"[GeneratedAssets] parse_thesis error: {e}")
    return None


# ── Screening ──────────────────────────────────────────────────────────────────

def screen_universe(criteria: dict) -> list:
    """
    Screen universe_stocks against parsed criteria.
    Returns list of {symbol, name, score, weight, reasons} sorted by score desc,
    capped at MAX_HOLDINGS.
    """
    sector   = (criteria.get("sector") or "").lower()
    keywords = [k.lower() for k in (criteria.get("keywords") or [])]
    max_price  = criteria.get("max_price")
    min_price  = criteria.get("min_price")
    min_mktcap = criteria.get("min_market_cap")
    ff = criteria.get("fundamental_filters") or {}
    min_rev_growth = ff.get("min_revenue_growth")
    max_pe         = ff.get("max_pe")
    min_margin     = ff.get("min_margin")

    db = _conn()
    try:
        rows = db.execute("""
            SELECT u.symbol, u.name, f.data, f.smart_score
            FROM universe_stocks u
            LEFT JOIN stock_fundamentals f ON f.symbol = u.symbol
            ORDER BY u.symbol
        """).fetchall()
    finally:
        db.close()

    candidates = []
    for symbol, name, fund_json, smart_score in rows:
        score   = 0
        reasons = []
        name_lower = (name or "").lower()

        # Keyword match
        for kw in keywords:
            if kw in name_lower or kw in symbol.lower():
                score += 15
                reasons.append(f"keyword:{kw}")

        # Sector match via name heuristics
        if sector:
            for sw in sector.split():
                if len(sw) > 3 and sw in name_lower:
                    score += 10
                    reasons.append(f"sector:{sw}")

        # Parse fundamentals
        fund = {}
        if fund_json:
            try:
                fund = json.loads(fund_json) if isinstance(fund_json, str) else fund_json
            except Exception:
                pass

        price     = fund.get("price") or fund.get("regularMarketPrice") or fund.get("currentPrice")
        pe        = fund.get("trailingPE") or fund.get("forwardPE")
        margin    = fund.get("profitMargins") or fund.get("grossMargins")
        rev_growth = fund.get("revenueGrowth") or fund.get("earningsGrowth")
        mktcap    = fund.get("marketCap")

        # Hard filters (only when data present)
        if price is not None:
            if max_price and price > max_price:
                continue
            if min_price and price < min_price:
                continue
            score += 5
            reasons.append(f"${price:.0f}")

        if mktcap is not None:
            if min_mktcap and mktcap < min_mktcap:
                continue
            if mktcap > 10_000_000_000:
                score += 10
                reasons.append("large_cap")
            elif mktcap > 1_000_000_000:
                score += 5
                reasons.append("mid_cap")

        if pe is not None and pe > 0:
            if max_pe and pe > max_pe:
                score -= 10
            else:
                score += 5
                reasons.append(f"PE:{pe:.1f}")

        if margin is not None:
            mpct = margin * 100 if abs(margin) < 1 else margin
            if min_margin and mpct < min_margin:
                score -= 5
            elif mpct > 15:
                score += 8
                reasons.append(f"margin:{mpct:.1f}%")

        if rev_growth is not None:
            rgpct = rev_growth * 100 if abs(rev_growth) < 1 else rev_growth
            if min_rev_growth and rgpct < min_rev_growth:
                score -= 5
            elif rgpct > 10:
                score += 12
                reasons.append(f"rev+{rgpct:.1f}%")

        if smart_score and smart_score > 60:
            score += int(smart_score * 0.1)
            reasons.append(f"ss:{smart_score}")

        if score >= 10:
            candidates.append({
                "symbol": symbol,
                "name": name or symbol,
                "score": min(score, 100),
                "reasons": reasons,
            })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:MAX_HOLDINGS]

    total_score = sum(c["score"] for c in top) or 1
    for c in top:
        c["weight"] = round(c["score"] / total_score * 100, 2)

    return top


# ── Build & persist ────────────────────────────────────────────────────────────

def build_index(name: str, thesis: str, criteria: dict, holdings: list) -> dict:
    """Persist the index. Returns {ok, index_id, holdings_count}."""
    _init_table()
    if not holdings:
        return {"error": "No holdings matched the criteria — try broader keywords"}
    with _conn() as db:
        cur = db.execute(
            "INSERT INTO generated_indexes (name, thesis, criteria, holdings) VALUES (?, ?, ?, ?)",
            (name, thesis, json.dumps(criteria), json.dumps(holdings)),
        )
        db.commit()
    return {"ok": True, "index_id": cur.lastrowid, "holdings_count": len(holdings)}


# ── VectorBT backtest ──────────────────────────────────────────────────────────

def backtest_index(index_id: int, days: int = 30) -> dict:
    """
    Backtest the index vs SPY using VectorBT.
    Returns {ok, results: {index_return, spy_return, alpha, sharpe, chart_data}}.
    """
    _init_table()
    db = _conn()
    row = db.execute(
        "SELECT holdings FROM generated_indexes WHERE id = ?", (index_id,)
    ).fetchone()
    db.close()
    if not row:
        return {"error": "Index not found"}

    holdings = json.loads(row[0])
    symbols = [h["symbol"] for h in holdings[:15]]  # cap for speed
    raw_weights = [h["weight"] for h in holdings[:15]]
    total_w = sum(raw_weights) or 1
    weights = [w / total_w for w in raw_weights]

    start = (datetime.now() - timedelta(days=days + 10)).strftime("%Y-%m-%d")

    try:
        import numpy as np
        import pandas as pd
        import yfinance as yf

        # Download prices (yfinance direct — more reliable for large ticker sets)
        price_df = yf.download(symbols, start=start, progress=False, auto_adjust=True)
        if isinstance(price_df.columns, pd.MultiIndex):
            prices = price_df["Close"] if "Close" in price_df.columns.get_level_values(0) else price_df.iloc[:, :len(symbols)]
        else:
            prices = price_df[["Close"]] if "Close" in price_df.columns else price_df

        # Handle single-ticker case
        if isinstance(prices, pd.Series):
            prices = prices.to_frame(name=symbols[0])

        available = [s for s in symbols if s in prices.columns]
        if not available:
            return {"error": "Could not fetch price data for any holdings"}

        avail_w = [weights[symbols.index(s)] for s in available]
        tw = sum(avail_w) or 1
        avail_w = [w / tw for w in avail_w]

        prices_avail = prices[available].dropna(how="all")
        returns = prices_avail.pct_change().dropna()
        port_returns = (returns * avail_w).sum(axis=1)

        # SPY
        spy_df = yf.download("SPY", start=start, progress=False, auto_adjust=True)
        spy_close = spy_df["Close"] if "Close" in spy_df.columns else spy_df.iloc[:, 0]
        spy_returns = spy_close.pct_change().dropna()

        # Align
        common_idx = port_returns.index.intersection(spy_returns.index)
        port_returns = port_returns.reindex(common_idx).dropna()
        spy_returns  = spy_returns.reindex(common_idx).dropna()

        index_total = float(((1 + port_returns).prod() - 1) * 100)
        spy_total   = float(((1 + spy_returns).prod() - 1) * 100)

        sharpe = 0.0
        if port_returns.std() > 0:
            sharpe = float(port_returns.mean() / port_returns.std() * (252 ** 0.5))

        cum_index = (1 + port_returns).cumprod()
        cum_spy   = (1 + spy_returns).cumprod()

        tail = min(days, len(cum_index))
        dates_list = [str(d)[:10] for d in cum_index.index[-tail:]]
        index_vals = [round((v - 1) * 100, 2) for v in cum_index.values[-tail:]]
        spy_vals   = [round((v - 1) * 100, 2) for v in cum_spy.values[-tail:]]

        result = {
            "index_return":  round(index_total, 2),
            "spy_return":    round(spy_total, 2),
            "alpha":         round(index_total - spy_total, 2),
            "sharpe":        round(sharpe, 3),
            "days":          days,
            "holdings_used": len(available),
            "chart_data":    {"dates": dates_list, "index": index_vals, "spy": spy_vals},
        }

        col = {30: "backtest_30", 90: "backtest_90", 365: "backtest_365"}.get(days, "backtest_30")
        with _conn() as db2:
            db2.execute(
                f"UPDATE generated_indexes SET {col} = ? WHERE id = ?",
                (json.dumps(result), index_id),
            )
            db2.commit()

        return {"ok": True, "results": result}

    except Exception as e:
        logger.error(f"[GeneratedAssets] backtest_index error: {e}")
        return {"error": str(e)}


# ── CRUD ───────────────────────────────────────────────────────────────────────

def list_indexes() -> list:
    _init_table()
    db = _conn()
    rows = db.execute(
        """SELECT id, name, thesis, holdings, backtest_30, backtest_90, created_at
           FROM generated_indexes WHERE status = 'ACTIVE'
           ORDER BY created_at DESC"""
    ).fetchall()
    db.close()
    out = []
    for r in rows:
        holdings = json.loads(r[3]) if r[3] else []
        out.append({
            "id":             r[0],
            "name":           r[1],
            "thesis":         r[2],
            "holdings_count": len(holdings),
            "top_holdings":   holdings[:5],
            "backtest_30":    json.loads(r[4]) if r[4] else None,
            "backtest_90":    json.loads(r[5]) if r[5] else None,
            "created_at":     r[6],
        })
    return out


def get_index(index_id: int) -> Optional[dict]:
    _init_table()
    db = _conn()
    row = db.execute(
        """SELECT id, name, thesis, criteria, holdings,
                  backtest_30, backtest_90, backtest_365, created_at
           FROM generated_indexes WHERE id = ?""",
        (index_id,),
    ).fetchone()
    db.close()
    if not row:
        return None
    return {
        "id":           row[0], "name":     row[1], "thesis":   row[2],
        "criteria":     json.loads(row[3]) if row[3] else {},
        "holdings":     json.loads(row[4]) if row[4] else [],
        "backtest_30":  json.loads(row[5]) if row[5] else None,
        "backtest_90":  json.loads(row[6]) if row[6] else None,
        "backtest_365": json.loads(row[7]) if row[7] else None,
        "created_at":   row[8],
    }


def delete_index(index_id: int) -> dict:
    _init_table()
    with _conn() as db:
        db.execute(
            "UPDATE generated_indexes SET status = 'DELETED' WHERE id = ?",
            (index_id,),
        )
        db.commit()
    return {"ok": True}
