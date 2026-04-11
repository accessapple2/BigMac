"""
tax_harvester.py — Tax-loss harvesting for USS TradeMinds.

Flow:
  1. Pull positions from Alpaca (live) or DB (fallback)
  2. Find unrealized loss > threshold (default -3%)
  3. For each loser: sell → buy correlated substitute immediately
  4. Log harvest + wash_sale_expiry (sale_date + 30 days)
  5. Block repurchase of original ticker within 30-day window

Safety:
  - Max 3 harvests per day
  - Paper only — no live account mutations
  - ALERT mode (default): captain approval required
  - AUTO mode: execute immediately

Tables (all INSERT-only):
  correlation_pairs   — ticker → substitute mapping
  tax_harvests        — every harvest event logged
  wash_sale_log       — active/expired 30-day windows

CIC commands:
  "tax harvest check"         → show current opportunities
  "tax harvest execute"       → run harvest (AUTO mode)
  "tax harvest mode alert"    → require captain approval
  "wash sales"                → active wash-sale windows

API:
  GET /api/tax/opportunities   — harvestable positions
  GET /api/tax/history         — past harvests
  GET /api/tax/wash-sales      — active 30-day blocks
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("tax_harvester")

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

# ── Configuration ──────────────────────────────────────────────────────────────

DEFAULT_LOSS_THRESHOLD_PCT = -3.0   # trigger harvest if loss % ≤ this
MAX_HARVESTS_PER_DAY       = 3
WASH_SALE_DAYS             = 30

# Player used to execute harvest trades
_HARVEST_PLAYER = "claude-sonnet"

# Correlated substitutes — kept intentionally minimal and non-contentious
# These are standard ETF/large-cap pairs used in published TLH strategies.
_CORRELATION_PAIRS: dict[str, str] = {
    # Tech
    "NVDA": "AMD",
    "AMD":  "NVDA",
    "MSFT": "ORCL",
    "ORCL": "MSFT",
    "AAPL": "MSFT",
    "GOOGL": "META",
    "META": "GOOGL",
    "AMZN": "MSFT",
    "TSLA": "RIVN",
    # Broad market ETFs
    "SPY":  "VOO",
    "VOO":  "IVV",
    "IVV":  "SPY",
    "QQQ":  "QQQM",
    "QQQM": "QQQ",
    "IWM":  "VB",
    "VB":   "IWM",
    # Sector ETFs
    "XLK":  "VGT",
    "VGT":  "XLK",
    "XLE":  "VDE",
    "VDE":  "XLE",
    "XLF":  "VFH",
    "VFH":  "XLF",
    # Semis
    "MU":   "MCHP",
    "MCHP": "MU",
    "AVGO": "QCOM",
    "QCOM": "AVGO",
    # Cloud
    "NOW":  "CRM",
    "CRM":  "NOW",
    "DELL": "HPE",
    "HPE":  "DELL",
    # Financials
    "JPM":  "BAC",
    "BAC":  "JPM",
    "GS":   "MS",
    "MS":   "GS",
    # Healthcare
    "JNJ":  "ABT",
    "ABT":  "JNJ",
    "UNH":  "CVS",
    "CVS":  "UNH",
}


# ── DB ─────────────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _init_tables() -> None:
    with _conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS correlation_pairs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker      TEXT NOT NULL,
                substitute  TEXT NOT NULL,
                notes       TEXT,
                added_at    TEXT DEFAULT (datetime('now')),
                UNIQUE(ticker)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS tax_harvests (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id       TEXT NOT NULL,
                ticker_sold     TEXT NOT NULL,
                qty_sold        REAL NOT NULL,
                sell_price      REAL NOT NULL,
                cost_basis      REAL NOT NULL,
                loss_amount     REAL NOT NULL,
                loss_pct        REAL NOT NULL,
                substitute_bought TEXT,
                sub_qty         REAL,
                sub_price       REAL,
                mode            TEXT NOT NULL DEFAULT 'ALERT',
                result          TEXT NOT NULL,
                wash_sale_expiry TEXT,
                estimated_tax_saving REAL,
                created_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS wash_sale_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id   TEXT NOT NULL,
                ticker      TEXT NOT NULL,
                sold_at     TEXT NOT NULL,
                expiry_at   TEXT NOT NULL,
                sell_price  REAL,
                loss_amount REAL,
                active      INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS tax_harvester_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        # Seed correlation pairs from built-in dict (INSERT OR IGNORE)
        for tkr, sub in _CORRELATION_PAIRS.items():
            db.execute(
                "INSERT OR IGNORE INTO correlation_pairs (ticker, substitute) VALUES (?,?)",
                (tkr, sub),
            )
        db.commit()


# ── Settings ───────────────────────────────────────────────────────────────────

def _get_setting(key: str, default: str) -> str:
    try:
        db = _conn()
        row = db.execute(
            "SELECT value FROM tax_harvester_settings WHERE key=?", (key,)
        ).fetchone()
        db.close()
        return row["value"] if row else default
    except Exception:
        return default


def _set_setting(key: str, value: str) -> None:
    with _conn() as db:
        db.execute(
            """INSERT INTO tax_harvester_settings (key, value, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                   updated_at=datetime('now')""",
            (key, value),
        )
        db.commit()


def get_mode() -> str:
    return _get_setting("mode", "ALERT").upper()


def set_mode(mode: str) -> dict:
    m = mode.upper()
    if m not in ("ALERT", "AUTO"):
        return {"ok": False, "error": "mode must be ALERT or AUTO"}
    _set_setting("mode", m)
    return {"ok": True, "mode": m}


def get_loss_threshold() -> float:
    return float(_get_setting("loss_threshold_pct", str(DEFAULT_LOSS_THRESHOLD_PCT)))


# ── Wash-sale helpers ──────────────────────────────────────────────────────────

def is_wash_sale_blocked(player_id: str, ticker: str) -> tuple[bool, Optional[str]]:
    """
    Returns (blocked: bool, expiry_date: str | None).
    Checks wash_sale_log for active windows.
    """
    try:
        db = _conn()
        row = db.execute(
            """SELECT expiry_at FROM wash_sale_log
               WHERE player_id=? AND ticker=? AND active=1
                 AND expiry_at > datetime('now')
               ORDER BY expiry_at DESC LIMIT 1""",
            (player_id, ticker.upper()),
        ).fetchone()
        db.close()
        if row:
            return True, row["expiry_at"][:10]
        return False, None
    except Exception:
        return False, None


def _record_wash_sale(player_id: str, ticker: str, sell_price: float,
                      loss_amount: float) -> str:
    """Insert wash sale window. Returns expiry date string."""
    sold_at  = datetime.now().isoformat()[:19]
    expiry   = (datetime.now() + timedelta(days=WASH_SALE_DAYS)).isoformat()[:19]
    with _conn() as db:
        db.execute(
            """INSERT INTO wash_sale_log
               (player_id, ticker, sold_at, expiry_at, sell_price, loss_amount)
               VALUES (?,?,?,?,?,?)""",
            (player_id, ticker.upper(), sold_at, expiry,
             round(sell_price, 2), round(loss_amount, 2)),
        )
        db.commit()
    return expiry[:10]


def get_active_wash_sales() -> list[dict]:
    """All active wash-sale windows (not yet expired)."""
    _init_tables()
    db = _conn()
    rows = db.execute(
        """SELECT id, player_id, ticker, sold_at, expiry_at,
                  sell_price, loss_amount, created_at
           FROM wash_sale_log
           WHERE active=1 AND expiry_at > datetime('now')
           ORDER BY expiry_at ASC"""
    ).fetchall()
    db.close()
    result = []
    for r in rows:
        d = dict(r)
        exp = datetime.fromisoformat(d["expiry_at"])
        d["days_remaining"] = max(0, (exp - datetime.now()).days)
        result.append(d)
    return result


# ── Daily limit ────────────────────────────────────────────────────────────────

def _harvests_today() -> int:
    try:
        db = _conn()
        row = db.execute(
            "SELECT COUNT(*) FROM tax_harvests WHERE date(created_at)=date('now')"
            " AND result NOT IN ('DRY_RUN','BLOCKED')",
        ).fetchone()
        db.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0


# ── Substitute lookup ──────────────────────────────────────────────────────────

def _get_substitute(ticker: str) -> Optional[str]:
    """Look up correlated substitute from DB (seeded from built-in dict)."""
    try:
        db = _conn()
        row = db.execute(
            "SELECT substitute FROM correlation_pairs WHERE ticker=?",
            (ticker.upper(),),
        ).fetchone()
        db.close()
        return row["substitute"] if row else None
    except Exception:
        return _CORRELATION_PAIRS.get(ticker.upper())


# ── Live price ────────────────────────────────────────────────────────────────

def _live_price(ticker: str, fallback: float = 0.0) -> float:
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).fast_info
        p = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
        return float(p) if p else fallback
    except Exception:
        return fallback


# ── Position sources ───────────────────────────────────────────────────────────

def _positions_from_alpaca() -> list[dict]:
    """Fetch positions from Alpaca paper account."""
    try:
        from engine.alpaca_bridge import alpaca
        positions = alpaca.positions()
        if positions and not (len(positions) == 1 and "error" in positions[0]):
            return positions
    except Exception:
        pass
    return []


def _positions_from_db() -> list[dict]:
    """Fallback: load positions from ai_players DB with live prices."""
    db = _conn()
    rows = db.execute(
        """SELECT p.player_id, p.symbol AS ticker, p.qty, p.avg_price,
                  ap.is_human
           FROM positions p
           JOIN ai_players ap ON ap.id = p.player_id
           WHERE p.qty > 0 AND p.asset_type = 'stock' AND ap.is_human = 0"""
    ).fetchall()
    db.close()
    result = []
    for r in rows:
        avg = float(r["avg_price"])
        live = _live_price(r["ticker"], avg)
        mv  = live * float(r["qty"])
        cost = avg * float(r["qty"])
        upl  = mv - cost
        uplpc = ((live - avg) / avg * 100) if avg > 0 else 0.0
        result.append({
            "player_id":      r["player_id"],
            "symbol":         r["ticker"],
            "qty":            float(r["qty"]),
            "avg_entry":      avg,
            "current_price":  live,
            "market_value":   round(mv, 2),
            "unrealized_pl":  round(upl, 2),
            "unrealized_plpc": round(uplpc, 2),
        })
    return result


# ── Opportunity scanner ────────────────────────────────────────────────────────

def scan_opportunities(threshold_pct: Optional[float] = None) -> dict:
    """
    Identify positions below the loss threshold with an available substitute.
    Returns {opportunities: [...], total_harvestable_loss, checked_at}
    """
    _init_tables()
    thr = threshold_pct if threshold_pct is not None else get_loss_threshold()

    # Try Alpaca first; fall back to DB
    positions = _positions_from_alpaca() or _positions_from_db()

    opps = []
    for pos in positions:
        plpc = float(pos.get("unrealized_plpc", 0))
        if plpc > thr:
            continue  # not a loss or not deep enough

        ticker = pos.get("symbol", "").upper()
        sub    = _get_substitute(ticker)
        player = pos.get("player_id", _HARVEST_PLAYER)

        # Wash-sale check: is this ticker already in a 30-day window?
        blocked, expiry = is_wash_sale_blocked(player, ticker)
        wash_blocked    = blocked

        loss_amt = float(pos.get("unrealized_pl", 0))
        opps.append({
            "player_id":        player,
            "symbol":           ticker,
            "qty":              float(pos.get("qty", 0)),
            "avg_entry":        float(pos.get("avg_entry", 0)),
            "current_price":    float(pos.get("current_price", 0)),
            "market_value":     float(pos.get("market_value", 0)),
            "unrealized_pl":    round(loss_amt, 2),
            "unrealized_plpc":  round(plpc, 2),
            "substitute":       sub,
            "has_substitute":   sub is not None,
            "wash_blocked":     wash_blocked,
            "wash_expiry":      expiry,
            "harvestable":      sub is not None and not wash_blocked,
        })

    opps.sort(key=lambda x: x["unrealized_plpc"])  # worst loss first
    total_loss = sum(o["unrealized_pl"] for o in opps if o["harvestable"])
    # Rough estimate: 15% cap gains rate × loss
    estimated_saving = abs(total_loss) * 0.15

    return {
        "opportunities":          opps,
        "total_count":            len(opps),
        "harvestable_count":      sum(1 for o in opps if o["harvestable"]),
        "total_harvestable_loss": round(total_loss, 2),
        "estimated_tax_saving":   round(estimated_saving, 2),
        "threshold_pct":          thr,
        "mode":                   get_mode(),
        "checked_at":             datetime.now().isoformat()[:19],
    }


# ── Execute harvest ────────────────────────────────────────────────────────────

def execute_harvest(dry_run: bool = True,
                    max_count: int = MAX_HARVESTS_PER_DAY) -> dict:
    """
    Run the harvest: sell losers, buy substitutes.
    dry_run=True → plan only; dry_run=False → execute via paper_trader.
    Respects daily limit and wash-sale blocks.
    """
    _init_tables()
    today_count = _harvests_today()
    if today_count >= MAX_HARVESTS_PER_DAY and not dry_run:
        return {
            "ok": False,
            "error": f"Daily harvest limit reached ({MAX_HARVESTS_PER_DAY}/day). "
                     f"Already ran {today_count} today.",
        }

    mode     = "DRY_RUN" if dry_run else get_mode()
    scan     = scan_opportunities()
    opps     = [o for o in scan["opportunities"] if o["harvestable"]]
    opps     = opps[:max_count]

    actions   = []
    executed  = 0
    total_loss = 0.0
    total_saving = 0.0

    for opp in opps:
        ticker = opp["symbol"]
        sub    = opp["substitute"]
        player = opp["player_id"]
        qty    = opp["qty"]
        price  = opp["current_price"]
        cost   = opp["avg_entry"]
        loss   = opp["unrealized_pl"]
        loss_pct = opp["unrealized_plpc"]

        if price <= 0:
            logger.warning("[TaxHarvest] %s price=0 — skipping", ticker)
            continue

        # Substitute live price
        sub_price = _live_price(sub, 0.0)
        if sub_price <= 0:
            logger.warning("[TaxHarvest] %s sub %s price=0 — skipping", ticker, sub)
            continue

        # How many shares of substitute to buy with proceeds?
        proceeds = qty * price
        sub_qty  = round(proceeds / sub_price, 4)

        tax_saving = abs(loss) * 0.15  # 15% cap gains estimate
        expiry_date = None

        act = {
            "player_id":      player,
            "ticker_sold":    ticker,
            "qty_sold":       round(qty, 4),
            "sell_price":     round(price, 2),
            "cost_basis":     round(cost, 2),
            "loss_amount":    round(loss, 2),
            "loss_pct":       round(loss_pct, 2),
            "substitute":     sub,
            "sub_qty":        sub_qty,
            "sub_price":      round(sub_price, 2),
            "estimated_tax_saving": round(tax_saving, 2),
            "wash_expiry":    None,
            "sell_result":    "DRY_RUN" if dry_run else "PENDING",
            "buy_result":     "DRY_RUN" if dry_run else "PENDING",
            "executed":       False,
        }

        if not dry_run:
            sell_ok = False
            buy_ok  = False

            # 1. Sell losing position
            try:
                from engine.paper_trader import sell
                r = sell(
                    player_id = player,
                    symbol    = ticker,
                    price     = price,
                    reasoning = f"[TaxHarvest] loss {loss_pct:.1f}% — harvesting, sub: {sub}",
                    confidence = 80.0,
                )
                sell_ok = r is not None
                act["sell_result"] = "OK" if sell_ok else "BLOCKED"
            except Exception as e:
                act["sell_result"] = f"ERROR: {e}"

            # 2. Record wash-sale window before buy attempt
            if sell_ok:
                expiry_date = _record_wash_sale(player, ticker, price, loss)
                act["wash_expiry"] = expiry_date

                # 3. Buy substitute (immediately)
                try:
                    from engine.paper_trader import buy
                    r = buy(
                        player_id  = player,
                        symbol     = sub,
                        price      = sub_price,
                        qty        = sub_qty,
                        reasoning  = f"[TaxHarvest] substitute for {ticker} harvest",
                        confidence = 75.0,
                        timeframe  = "SWING",
                    )
                    buy_ok = r is not None
                    act["buy_result"] = "OK" if buy_ok else "BLOCKED"
                except Exception as e:
                    act["buy_result"] = f"ERROR: {e}"

                if sell_ok:
                    executed += 1
                    act["executed"] = True

        # Log harvest (INSERT only)
        log_result = "DRY_RUN" if dry_run else (
            "OK" if act.get("executed") else "PARTIAL"
        )
        with _conn() as db:
            db.execute(
                """INSERT INTO tax_harvests
                   (player_id, ticker_sold, qty_sold, sell_price, cost_basis,
                    loss_amount, loss_pct, substitute_bought, sub_qty, sub_price,
                    mode, result, wash_sale_expiry, estimated_tax_saving)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (player, ticker, round(qty, 4), round(price, 2), round(cost, 2),
                 round(loss, 2), round(loss_pct, 2), sub, sub_qty, round(sub_price, 2),
                 mode, log_result, expiry_date, round(tax_saving, 2)),
            )
            db.commit()

        total_loss   += loss
        total_saving += tax_saving
        actions.append(act)

    return {
        "ok":                    True,
        "dry_run":               dry_run,
        "mode":                  mode,
        "opportunities_found":   len(scan["opportunities"]),
        "harvests_planned":      len(opps),
        "harvests_executed":     executed,
        "total_loss_harvested":  round(total_loss, 2),
        "total_estimated_saving": round(total_saving, 2),
        "actions":               actions,
        "daily_count_after":     today_count + (executed if not dry_run else 0),
        "daily_limit":           MAX_HARVESTS_PER_DAY,
        "executed_at":           datetime.now().isoformat()[:19],
    }


# ── History ────────────────────────────────────────────────────────────────────

def get_harvest_history(limit: int = 50) -> list[dict]:
    _init_tables()
    db = _conn()
    rows = db.execute(
        """SELECT id, player_id, ticker_sold, qty_sold, sell_price, cost_basis,
                  loss_amount, loss_pct, substitute_bought, sub_qty, sub_price,
                  mode, result, wash_sale_expiry, estimated_tax_saving, created_at
           FROM tax_harvests ORDER BY id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_ytd_summary() -> dict:
    """YTD harvested losses and estimated tax savings."""
    _init_tables()
    year = str(datetime.now().year)
    db   = _conn()
    row  = db.execute(
        """SELECT COUNT(*) AS cnt,
                  SUM(loss_amount) AS total_loss,
                  SUM(estimated_tax_saving) AS total_saving
           FROM tax_harvests
           WHERE result NOT IN ('DRY_RUN','BLOCKED')
             AND strftime('%Y', created_at) = ?""",
        (year,),
    ).fetchone()
    db.close()
    return {
        "year":               year,
        "harvest_count":      int(row["cnt"] or 0),
        "total_loss_harvested": round(float(row["total_loss"] or 0), 2),
        "estimated_tax_saving": round(float(row["total_saving"] or 0), 2),
    }
