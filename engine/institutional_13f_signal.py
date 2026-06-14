#!/usr/bin/env python3
"""
institutional_13f_signal.py — SEC EDGAR 13F net-new-institutional-buyers signal.

CONFIRMATORY-ONLY (same rail as fred_bankrate_signal.py): this NEVER originates a
trade. It returns a per-ticker lean ('confirm'/'neutral'/'caution') that UHURA's
confluence may count as ONE confirmatory source, and only when the fleet already has
>= MIN_FLEET_VOTES independent directional votes (sole-voter -> never counts).

Source: SEC EDGAR full-text search (efts.sec.gov, free, no key) over form 13F-HR.
The count of 13F-HR filings mentioning an issuer in its quarterly FILING window ≈ the
number of institutional managers holding it (holders_count). The QoQ change in that
count ≈ net new institutional buyers.

  PROXY CAVEAT (documented, by design): the ABSOLUTE holders_count is an exact-phrase
  full-text match on the issuer name, so it is name-string sensitive (e.g. "NVIDIA
  CORP" undercounts vs filings that write "NVIDIA CORPORATION"). The QoQ DELTA
  (net_new_buyers) is the robust signal — the SAME phrase is used both quarters, so
  the matching bias cancels. The lean derives from the delta, not the absolute. This
  is a SLOW/structural context voter (13F files ~45 days after quarter-end, quarterly).

Doctrine: is_trigger hardcoded False; default OFF via
INSTITUTIONAL_13F_CONFIRMATORY_VOTE_ENABLED; rows written, never deleted.
"""

from __future__ import annotations

import json
import os
import sqlite3
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    from rich.console import Console
    console = Console()
except Exception:  # pragma: no cover
    class _Stub:
        def log(self, *a, **k):
            print(*a)
    console = _Stub()

_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")
_USER_AGENT = os.environ.get(
    "SEC_EDGAR_UA", "OllieTrades-13F-signal/1.0 (superapple@duck.com)")

# ─── Tunable defaults ────────────────────────────────────────────────────────
# Lean off neutral when net-new buyers move >= THRESHOLD_PCT of the prior quarter's
# holder count AND the absolute move clears MIN_ABS filers (noise floor on thin names).
THRESHOLD_PCT = 3.0
MIN_ABS = 25
FILING_WINDOW_DAYS = 60          # 13F due ~45d after quarter-end; widen to 60 for stragglers
_FTS_CAP = 10000                 # EDGAR FTS caps total at 10k ('gte')

# Confirmatory-only rail (mirrors fred_bankrate_signal.MIN_FLEET_VOTES).
MIN_FLEET_VOTES = 2

# Context-class weight (0.5) — slow/structural, like FRED's macro lean. NOT the 1.0
# technical-confirmatory class used by the intraday/daily HM-BK scanners.
CONFIRMATORY_WEIGHT = 0.5

_TICKERS_CACHE: dict = {"data": None, "ts": 0.0}
ARCHIVE_DIR = Path(os.environ.get("OLLIE_INTEL_ARCHIVE", "data/intel_archive"))


# ─── EDGAR helpers ───────────────────────────────────────────────────────────

def _get_json(url: str, timeout: int = 25, retries: int = 4) -> dict:
    """GET JSON with retry+backoff on transient EDGAR 5xx / rate throttling.
    efts.sec.gov returns intermittent HTTP 500 under burst; a short backoff clears it."""
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT,
                                               "Accept": "application/json"})
    last = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(0.6 * (2 ** attempt))  # 0.6, 1.2, 2.4s
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last = e
            if attempt < retries - 1:
                time.sleep(0.6 * (2 ** attempt))
                continue
            raise
    raise last  # pragma: no cover


def issuer_name(ticker: str) -> str | None:
    """Official issuer title for `ticker` from SEC company_tickers.json (cached 24h)."""
    now = time.time()
    if _TICKERS_CACHE["data"] is None or now - _TICKERS_CACHE["ts"] > 86400:
        try:
            ct = _get_json("https://www.sec.gov/files/company_tickers.json")
            _TICKERS_CACHE["data"] = {v["ticker"].upper(): v["title"] for v in ct.values()}
            _TICKERS_CACHE["ts"] = now
        except Exception as e:
            console.log(f"[yellow]13f: company_tickers load: {type(e).__name__}: {e!r}")
            if _TICKERS_CACHE["data"] is None:
                return None
    return _TICKERS_CACHE["data"].get(ticker.upper())


def _fts_holders(name: str, start: str, end: str) -> tuple[int, str]:
    """(count, relation) of 13F-HR filings mentioning `name` filed in [start, end]."""
    params = urllib.parse.urlencode(
        {"q": f'"{name}"', "forms": "13F-HR", "startdt": start, "enddt": end})
    d = _get_json(f"https://efts.sec.gov/LATEST/search-index?{params}")
    tot = d.get("hits", {}).get("total", {})
    return int(tot.get("value", 0) or 0), tot.get("relation", "eq")


# ─── Quarter math ────────────────────────────────────────────────────────────

_Q_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


def _q_end_date(quarter: str) -> date:
    y, q = int(quarter[:4]), int(quarter[-1])
    mo, dy = _Q_END[q]
    return date(y, mo, dy)


def prior_quarter(quarter: str) -> str:
    y, q = int(quarter[:4]), int(quarter[-1])
    return f"{y-1}Q4" if q == 1 else f"{y}Q{q-1}"


def filing_window(quarter: str) -> tuple[str, str]:
    """Window in which `quarter`'s 13F-HR filings post (q_end+1 .. q_end+FILING_WINDOW)."""
    qe = _q_end_date(quarter)
    return (qe + timedelta(days=1)).isoformat(), (qe + timedelta(days=FILING_WINDOW_DAYS)).isoformat()


def latest_filed_quarter(today: date | None = None) -> str:
    """Most recent quarter whose 13F filing window has substantially elapsed
    (q_end + 50 days passed -> the bulk of filers have reported)."""
    today = today or datetime.now(timezone.utc).date()
    y = today.year
    cands = [f"{yy}Q{qq}" for yy in (y, y - 1) for qq in (1, 2, 3, 4)]
    cands.sort(key=_q_end_date, reverse=True)
    for q in cands:
        if (_q_end_date(q) + timedelta(days=50)) <= today:
            return q
    return cands[-1]


# ─── Lean ────────────────────────────────────────────────────────────────────

def _lean(net: int, prior: int) -> str:
    if prior <= 0 or abs(net) < MIN_ABS:
        return "neutral"
    pct = net / prior * 100.0
    if pct >= THRESHOLD_PCT:
        return "confirm"    # more institutions bought QoQ -> BULLISH
    if pct <= -THRESHOLD_PCT:
        return "caution"    # institutions exiting QoQ -> BEARISH
    return "neutral"


# ─── DB ──────────────────────────────────────────────────────────────────────

def _conn(db_path: str | None = None) -> sqlite3.Connection:
    c = sqlite3.connect(db_path or _DB_PATH, timeout=10)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _ensure_schema(conn: sqlite3.Connection) -> None:
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS institutional_flow_13f (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker          TEXT NOT NULL,
                quarter         TEXT NOT NULL,
                holders_count   INTEGER NOT NULL,
                new_buyers      INTEGER NOT NULL,
                exited_holders  INTEGER NOT NULL,
                net_new_buyers  INTEGER NOT NULL,
                lean            TEXT NOT NULL,
                asof_filed      TEXT,
                source          TEXT DEFAULT 'sec_edgar_13f_fts',
                created_at      TEXT NOT NULL,
                UNIQUE(ticker, quarter)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_13f_ticker_q "
            "ON institutional_flow_13f(ticker, quarter DESC)"
        )
        conn.commit()
    except Exception as e:
        console.log(f"[yellow]13f: _ensure_schema: {type(e).__name__}: {e!r}")


# ─── Ingest ──────────────────────────────────────────────────────────────────

def ingest_quarter(tickers: list[str], quarter: str | None = None,
                   db_path: str | None = None, sleep_s: float = 0.2) -> dict:
    """Compute holders_count for `quarter` (default = latest filed) and the prior
    quarter, derive net-new buyers + lean, upsert one row per ticker. Returns a
    summary. Polite ~5 req/s SEC pacing via sleep_s."""
    quarter = quarter or latest_filed_quarter()
    pq = prior_quarter(quarter)
    cur_s, cur_e = filing_window(quarter)
    pri_s, pri_e = filing_window(pq)
    conn = _conn(db_path)
    rows, errors, capped, skipped = 0, 0, 0, 0
    try:
        _ensure_schema(conn)
        now = datetime.now(timezone.utc).isoformat()
        for tk in tickers:
            nm = issuer_name(tk)
            if not nm:
                skipped += 1   # not a 13F issuer (ETF / no company_tickers entry)
                continue
            try:
                cur, rel_c = _fts_holders(nm, cur_s, cur_e)
                time.sleep(sleep_s)
                pri, rel_p = _fts_holders(nm, pri_s, pri_e)
                time.sleep(sleep_s)
            except Exception as e:
                console.log(f"[yellow]13f: FTS {tk}: {type(e).__name__}: {e!r}")
                errors += 1
                continue
            if rel_c == "gte" or rel_p == "gte":
                capped += 1  # one side hit the 10k FTS cap -> delta is a floor estimate
            net = cur - pri
            lean = _lean(net, pri)
            conn.execute(
                "INSERT INTO institutional_flow_13f "
                "(ticker, quarter, holders_count, new_buyers, exited_holders, "
                " net_new_buyers, lean, asof_filed, source, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(ticker, quarter) DO UPDATE SET "
                " holders_count=excluded.holders_count, new_buyers=excluded.new_buyers, "
                " exited_holders=excluded.exited_holders, net_new_buyers=excluded.net_new_buyers, "
                " lean=excluded.lean, asof_filed=excluded.asof_filed, created_at=excluded.created_at",
                (tk.upper(), quarter, cur, max(net, 0), max(-net, 0), net, lean,
                 cur_e, "sec_edgar_13f_fts", now),
            )
            rows += 1
        conn.commit()
    finally:
        conn.close()
    return {"quarter": quarter, "prior": pq, "rows": rows, "errors": errors,
            "skipped_not_issuer": skipped, "capped": capped}


# ─── Read / vote ─────────────────────────────────────────────────────────────

def get_signal(ticker: str, db_path: str | None = None) -> dict:
    """Latest stored quarter's net-new buyers + lean for `ticker`. is_trigger False."""
    try:
        conn = _conn(db_path)
        try:
            _ensure_schema(conn)
            r = conn.execute(
                "SELECT * FROM institutional_flow_13f WHERE ticker=? "
                "ORDER BY quarter DESC LIMIT 1", (ticker.upper(),)).fetchone()
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]13f: get_signal {ticker}: {type(e).__name__}: {e!r}")
        r = None
    if not r:
        return {"source": "sec_edgar_13f", "ticker": ticker.upper(), "is_trigger": False,
                "vote": "neutral", "net_new_buyers": None, "quarter": None}
    return {"source": "sec_edgar_13f", "ticker": r["ticker"], "is_trigger": False,
            "vote": r["lean"], "net_new_buyers": r["net_new_buyers"],
            "holders_count": r["holders_count"], "quarter": r["quarter"]}


def confirmatory_vote(fleet_directional_votes: int, lean: str | None) -> dict:
    """13F net-new buyers may CONFIRM an existing fleet convergence but may NEVER
    ORIGINATE. Counts only once the fleet has >= MIN_FLEET_VOTES directional votes."""
    is_directional = lean in ("confirm", "caution")
    is_sole_voter = fleet_directional_votes < MIN_FLEET_VOTES
    counts = is_directional and not is_sole_voter

    trade_permitted_on_13f_alone = False
    assert not (is_sole_voter and counts), (
        "13F is confirmatory-only: the sole voter must never count toward a trade "
        "(MIN_FLEET_VOTES=%d not met)" % MIN_FLEET_VOTES
    )
    assert trade_permitted_on_13f_alone is False

    return {
        "source": "sec_edgar_13f",
        "lean": lean,
        "direction": ("BULLISH" if lean == "confirm"
                      else "BEARISH" if lean == "caution" else "NEUTRAL"),
        "counts_toward_convergence": counts,
        "is_sole_voter": is_sole_voter,
        "fleet_directional_votes": fleet_directional_votes,
        "min_fleet_votes_required": MIN_FLEET_VOTES,
        "trade_permitted_on_13f_alone": trade_permitted_on_13f_alone,
        "is_trigger": False,
    }


def _latest_quarter_in_db(conn) -> str | None:
    r = conn.execute("SELECT MAX(quarter) FROM institutional_flow_13f").fetchone()
    return r[0] if r else None


def market_vote(watchlist: list[str] | None = None, db_path: str | None = None,
                enabled: bool | None = None) -> dict | None:
    """Aggregate the latest stored quarter's leans across the watchlist into ONE
    market-level confirmatory lean, or None to ABSTAIN (flag OFF / no fresh quarter)."""
    if enabled is None:
        try:
            from config import INSTITUTIONAL_13F_CONFIRMATORY_VOTE_ENABLED as enabled
        except Exception:
            enabled = False
    if not enabled:
        return None
    try:
        conn = _conn(db_path)
        try:
            _ensure_schema(conn)
            q = _latest_quarter_in_db(conn)
            if not q:
                return None
            rows = [dict(r) for r in conn.execute(
                "SELECT ticker, lean, net_new_buyers FROM institutional_flow_13f "
                "WHERE quarter=?", (q,)).fetchall()]
        finally:
            conn.close()
    except Exception as e:
        console.log(f"[yellow]13f: market_vote: {type(e).__name__}: {e!r}")
        return None
    if watchlist:
        wl = set(watchlist)
        rows = [r for r in rows if r["ticker"] in wl]
    rows = [r for r in rows if r["lean"] in ("confirm", "caution")]
    if not rows:
        return None  # abstain — no directional 13F lean in the fresh quarter
    assert confirmatory_vote(0, "confirm")["counts_toward_convergence"] is False

    bull = [r for r in rows if r["lean"] == "confirm"]
    bear = [r for r in rows if r["lean"] == "caution"]
    if len(bull) > len(bear):
        direction, n, ex = "BULLISH", len(bull), bull[0]["ticker"]
    elif len(bear) > len(bull):
        direction, n, ex = "BEARISH", len(bear), bear[0]["ticker"]
    else:
        direction, n, ex = "NEUTRAL", 0, rows[0]["ticker"]
    reasoning = (f"[confirm] {q} 13F net-new buyers: {len(bull)} accumulating / "
                 f"{len(bear)} distributing e.g. {ex}")
    return {"direction": direction, "weight": CONFIRMATORY_WEIGHT,
            "reasoning": reasoning, "n": n, "bull": len(bull), "bear": len(bear),
            "quarter": q}


# ─── Archive (never delete) ──────────────────────────────────────────────────

def archive_snapshot(summary: dict) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = ARCHIVE_DIR / f"institutional_13f_{stamp}.json"
    path.write_text(json.dumps(summary, indent=2))
    return path


if __name__ == "__main__":
    import sys
    tks = sys.argv[1:] or ["AAPL", "NVDA", "TSLA"]
    s = ingest_quarter(tks)
    print(json.dumps(s, indent=2))
    for t in tks:
        print(t, get_signal(t))
