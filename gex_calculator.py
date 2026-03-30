"""
TradeMinds GEX Calculator — Alpaca-based
=========================================
Fetches options chain (Greeks) + contracts (open interest) from Alpaca,
computes Gamma Exposure per strike, and identifies key MM positioning levels.

Formula:
    GEX = gamma × open_interest × 100 × spot² × 0.01
Sign convention (dealer):
    Calls → +GEX  (dealers short calls → long gamma → pin price)
    Puts  → −GEX  (dealers short puts  → short gamma → amplify moves)

Key levels:
    max_gamma_strike  — strongest price magnet / pin
    zero_gamma_level  — where net GEX crosses zero (flip from pinned → volatile)
    put_wall          — largest put-side GEX below spot (support)
    call_wall         — largest call-side GEX above spot (resistance)
    gamma_flip        — zero-crossing nearest to spot (directional trigger)

Usage:
    profile = compute_gex_sync("SPY")
    print(profile.max_gamma_strike, profile.gamma_flip)
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from dotenv import load_dotenv
from rich.console import Console

load_dotenv()
console = Console()

DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

GEX_SYMBOLS = ["SPY", "QQQ", "NVDA", "TSLA", "AAPL"]
CACHE_TTL = 900  # 15 minutes


# ── Dataclasses ────────────────────────────────────────────────────────────


@dataclass
class GEXLevel:
    """Per-strike GEX breakdown."""
    strike: float
    net_gex: float
    call_gex: float
    put_gex: float
    call_oi: int = 0
    put_oi: int = 0


@dataclass
class GEXProfile:
    """Complete GEX analysis for a symbol."""
    symbol: str
    spot_price: float
    timestamp: str
    levels: list            # list[GEXLevel]
    max_gamma_strike: float  # strongest pin
    zero_gamma_level: float  # regime flip
    put_wall: float          # support below spot
    call_wall: float         # resistance above spot
    gamma_flip: float        # zero-cross nearest to spot
    total_gex: float         # + = pinned, - = volatile
    source: str = "alpaca"


# ── Database ───────────────────────────────────────────────────────────────


def _init_db() -> None:
    """Create gex_snapshots table if it doesn't exist. Safe to call repeatedly."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gex_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                spot_price REAL,
                max_gamma_strike REAL,
                zero_gamma_level REAL,
                put_wall REAL,
                call_wall REAL,
                gamma_flip REAL,
                total_gex REAL,
                levels_json TEXT,
                source TEXT DEFAULT 'alpaca',
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[red]GEX: DB init error: {e}")


def _save_snapshot(profile: GEXProfile) -> None:
    """Persist a GEXProfile to gex_snapshots."""
    try:
        _init_db()
        levels_json = json.dumps([
            {
                "strike": l.strike,
                "net_gex": l.net_gex,
                "call_gex": l.call_gex,
                "put_gex": l.put_gex,
                "call_oi": l.call_oi,
                "put_oi": l.put_oi,
            }
            for l in profile.levels
        ])
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            INSERT INTO gex_snapshots
              (symbol, timestamp, spot_price, max_gamma_strike, zero_gamma_level,
               put_wall, call_wall, gamma_flip, total_gex, levels_json, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                profile.symbol, profile.timestamp, profile.spot_price,
                profile.max_gamma_strike, profile.zero_gamma_level,
                profile.put_wall, profile.call_wall, profile.gamma_flip,
                profile.total_gex, levels_json, profile.source,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[red]GEX: snapshot save error: {e}")


def get_latest_snapshot(symbol: str) -> dict | None:
    """Return the most recent gex_snapshots row for a symbol (or None)."""
    try:
        _init_db()
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        row = conn.execute(
            "SELECT * FROM gex_snapshots WHERE symbol=? ORDER BY created_at DESC LIMIT 1",
            (symbol.upper(),),
        ).fetchone()
        conn.close()
        if row:
            r = dict(row)
            r["levels"] = json.loads(r.get("levels_json") or "[]")
            return r
        return None
    except Exception as e:
        console.log(f"[red]GEX: DB read error: {e}")
        return None


def get_snapshot_history(symbol: str, limit: int = 20) -> list:
    """Return last N gex_snapshots rows for a symbol."""
    try:
        _init_db()
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        rows = conn.execute(
            "SELECT * FROM gex_snapshots WHERE symbol=? ORDER BY created_at DESC LIMIT ?",
            (symbol.upper(), limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        console.log(f"[red]GEX: DB history error: {e}")
        return []


# ── Calculator ─────────────────────────────────────────────────────────────


class GEXCalculator:
    """
    Compute Gamma Exposure from Alpaca options data.

    Requires ALPACA_API_KEY + ALPACA_SECRET_KEY env vars.
    """

    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        from alpaca.data.historical.option import OptionHistoricalDataClient
        from alpaca.trading.client import TradingClient

        self._data_client = OptionHistoricalDataClient(api_key, secret_key)
        self._trading_client = TradingClient(api_key, secret_key, paper=paper)
        self._api_key = api_key
        self._secret_key = secret_key

    async def compute(
        self,
        symbol: str,
        expiration_date_gte: Optional[str] = None,
        expiration_date_lte: Optional[str] = None,
    ) -> GEXProfile:
        """
        Full GEX profile for a symbol.

        Steps:
          1. Spot price (market_data → Alpaca stock quote)
          2. Fetch option chain (Greeks) + contracts (OI) in parallel
          3. Merge on contract symbol, skip 0DTE / missing Greeks
          4. Aggregate GEX per strike
          5. Derive key levels, persist, return
        """
        symbol = symbol.upper()

        # ── 1. Spot price ──────────────────────────────────────────────
        loop = asyncio.get_event_loop()
        spot = await loop.run_in_executor(None, self._get_spot, symbol)
        if not spot or spot <= 0:
            raise ValueError(f"GEX: cannot get spot price for {symbol}")

        # Default window: today → 60 DTE
        today = date.today().isoformat()
        end60 = (date.today() + timedelta(days=60)).isoformat()
        exp_gte = expiration_date_gte or today
        exp_lte = expiration_date_lte or end60

        # ── 2. Parallel fetch ──────────────────────────────────────────
        chain_f = loop.run_in_executor(None, self._fetch_chain, symbol, exp_gte, exp_lte)
        contracts_f = loop.run_in_executor(None, self._fetch_contracts, symbol, exp_gte, exp_lte)
        chain, contracts = await asyncio.gather(chain_f, contracts_f)

        # ── 3. Build OI lookup ─────────────────────────────────────────
        oi_map: dict[str, tuple] = {}   # sym → (oi, strike_float, is_call)
        for c in contracts:
            try:
                oi = int(c.open_interest or 0)
                strike = float(c.strike_price)
                is_call = c.type.value.lower() == "call"
                if oi > 0 and strike > 0:
                    oi_map[c.symbol] = (oi, strike, is_call)
            except Exception:
                continue

        # ── 4. Aggregate per strike ────────────────────────────────────
        strikes_map: dict[float, dict] = {}

        for contract_sym, snapshot in chain.items():
            greeks = snapshot.greeks
            if greeks is None:
                continue
            gamma = greeks.gamma
            if gamma is None or float(gamma) <= 0:
                continue

            entry = oi_map.get(contract_sym)
            if entry is None:
                continue
            oi, strike, is_call = entry

            gex = self._gex_contrib(float(gamma), oi, spot, is_call)

            if strike not in strikes_map:
                strikes_map[strike] = {
                    "call_gex": 0.0, "put_gex": 0.0,
                    "call_oi": 0, "put_oi": 0,
                }
            if is_call:
                strikes_map[strike]["call_gex"] += gex
                strikes_map[strike]["call_oi"] += oi
            else:
                strikes_map[strike]["put_gex"] += gex   # already negative
                strikes_map[strike]["put_oi"] += oi

        if not strikes_map:
            raise ValueError(f"GEX: no usable strikes for {symbol} (chain={len(chain)}, oi_map={len(oi_map)})")

        # ── 5. Build GEXLevel list ─────────────────────────────────────
        levels: list[GEXLevel] = []
        for strike, sd in strikes_map.items():
            net = sd["call_gex"] + sd["put_gex"]
            levels.append(GEXLevel(
                strike=round(strike, 2),
                net_gex=round(net, 2),
                call_gex=round(sd["call_gex"], 2),
                put_gex=round(sd["put_gex"], 2),
                call_oi=sd["call_oi"],
                put_oi=sd["put_oi"],
            ))
        levels.sort(key=lambda x: x.strike)

        # Filter to ±15% of spot for relevance (widen if too sparse)
        lo, hi = spot * 0.85, spot * 1.15
        relevant = [l for l in levels if lo <= l.strike <= hi] or levels

        # ── 6. Key levels ──────────────────────────────────────────────
        max_gamma_lv = max(relevant, key=lambda x: x.net_gex)
        total_gex = sum(l.net_gex for l in relevant)

        below = [l for l in relevant if l.strike < spot]
        above = [l for l in relevant if l.strike > spot]

        # Put wall = most negative put_gex below spot
        put_wall_lv = min(below, key=lambda x: x.put_gex, default=max_gamma_lv)
        # Call wall = most positive call_gex above spot
        call_wall_lv = max(above, key=lambda x: x.call_gex, default=max_gamma_lv)

        zero_gamma = self._find_zero_gamma(relevant, spot)

        profile = GEXProfile(
            symbol=symbol,
            spot_price=round(spot, 2),
            timestamp=datetime.now().isoformat(),
            levels=relevant,
            max_gamma_strike=max_gamma_lv.strike,
            zero_gamma_level=zero_gamma,
            put_wall=put_wall_lv.strike,
            call_wall=call_wall_lv.strike,
            gamma_flip=zero_gamma,
            total_gex=round(total_gex, 2),
            source="alpaca",
        )

        _save_snapshot(profile)
        return profile

    # ── Internal helpers ───────────────────────────────────────────────

    def _get_spot(self, symbol: str) -> Optional[float]:
        """Get spot price: market_data → Alpaca stock snapshot fallback."""
        try:
            from engine.market_data import get_stock_price
            d = get_stock_price(symbol)
            if d and "price" in d and not d.get("error"):
                return float(d["price"])
        except Exception:
            pass
        try:
            from alpaca.data.historical.stock import StockHistoricalDataClient
            from alpaca.data.requests import StockLatestQuoteRequest
            sc = StockHistoricalDataClient(self._api_key, self._secret_key)
            resp = sc.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=symbol))
            q = resp.get(symbol)
            if q:
                mid = ((q.ask_price or 0) + (q.bid_price or 0)) / 2.0
                if mid > 0:
                    return mid
        except Exception:
            pass
        return None

    def _fetch_chain(self, symbol: str, exp_gte: str, exp_lte: str) -> dict:
        """Fetch OptionsSnapshot dict from Alpaca (Greeks per contract)."""
        from alpaca.data.requests import OptionChainRequest
        try:
            req = OptionChainRequest(
                underlying_symbol=symbol,
                expiration_date_gte=exp_gte,
                expiration_date_lte=exp_lte,
            )
            result = self._data_client.get_option_chain(req)
            return result or {}
        except Exception as e:
            console.log(f"[red]GEX: chain fetch error for {symbol}: {e}")
            return {}

    def _fetch_contracts(self, symbol: str, exp_gte: str, exp_lte: str) -> list:
        """Paginate through OptionContractsResponse for OI data.

        Uses limit=1000 (Alpaca max) to minimise pages. SPY has ~6232 contracts
        across a 60-day window — 7 pages at limit=1000.
        """
        from alpaca.trading.requests import GetOptionContractsRequest
        contracts, page_token, pages = [], None, 0
        while pages < 50:      # safety cap — 50k contracts max
            try:
                req = GetOptionContractsRequest(
                    underlying_symbols=[symbol],
                    expiration_date_gte=exp_gte,
                    expiration_date_lte=exp_lte,
                    limit=1000,
                    page_token=page_token,
                )
                resp = self._trading_client.get_option_contracts(req)
                batch = resp.option_contracts or []
                contracts.extend(batch)
                page_token = resp.next_page_token
                pages += 1
                if not page_token or not batch:
                    break
                time.sleep(0.05)
            except Exception as e:
                console.log(f"[red]GEX: contracts fetch error ({pages} pages): {e}")
                break
        return contracts

    @staticmethod
    def _gex_contrib(gamma: float, oi: int, spot: float, is_call: bool) -> float:
        """Per-contract GEX. Positive for calls (pin), negative for puts (trend)."""
        raw = gamma * oi * 100 * (spot ** 2) * 0.01
        return raw if is_call else -raw

    @staticmethod
    def _find_zero_gamma(levels: list, spot: float) -> float:
        """
        Linear interpolation to find net-GEX zero crossing nearest to spot.
        Returns spot if no crossing exists.
        """
        sorted_lv = sorted(levels, key=lambda x: x.strike)
        best, best_dist = spot, float("inf")
        for i in range(len(sorted_lv) - 1):
            a, b = sorted_lv[i], sorted_lv[i + 1]
            if a.net_gex * b.net_gex <= 0 and (a.net_gex != 0 or b.net_gex != 0):
                denom = b.net_gex - a.net_gex
                x0 = a.strike if denom == 0 else (
                    a.strike + (b.strike - a.strike) * (-a.net_gex / denom)
                )
                dist = abs(x0 - spot)
                if dist < best_dist:
                    best_dist = dist
                    best = round(x0, 2)
        return best


# ── Module-level singleton + sync wrapper ─────────────────────────────────

_calc: Optional[GEXCalculator] = None
_calc_lock = threading.Lock()

_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()


def _get_calculator() -> Optional[GEXCalculator]:
    global _calc
    with _calc_lock:
        if _calc is not None:
            return _calc
        key = os.getenv("ALPACA_API_KEY", "")
        secret = os.getenv("ALPACA_SECRET_KEY", "")
        if not key or not secret:
            return None
        try:
            _calc = GEXCalculator(key, secret)
            return _calc
        except Exception as e:
            console.log(f"[red]GEX: Calculator init failed: {e}")
            return None


def compute_gex_sync(symbol: str, force: bool = False) -> Optional[GEXProfile]:
    """
    Thread-safe synchronous GEX compute.

    - Checks in-memory cache (15 min TTL)
    - Runs async compute in a fresh thread (avoids event-loop conflicts)
    - Falls back to DB snapshot if API unavailable
    """
    symbol = symbol.upper()
    now = time.time()

    with _cache_lock:
        entry = _cache.get(symbol)
        if entry and not force and (now - entry["ts"]) < CACHE_TTL:
            return entry["profile"]

    calc = _get_calculator()
    if calc is None:
        return None

    def _run() -> GEXProfile:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(calc.compute(symbol))
        finally:
            loop.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            profile = ex.submit(_run).result(timeout=120)
        with _cache_lock:
            _cache[symbol] = {"profile": profile, "ts": now}
        return profile
    except Exception as e:
        console.log(f"[red]GEX: compute_gex_sync error for {symbol}: {e}")
        return None


def get_gex(symbol: str, force: bool = False) -> Optional[GEXProfile]:
    """Primary public API. Returns cached profile or triggers fresh compute."""
    return compute_gex_sync(symbol, force=force)


# ── Prompt builder ─────────────────────────────────────────────────────────


def _profile_from_snapshot(snap: dict) -> GEXProfile:
    """Reconstruct a GEXProfile from a DB row dict."""
    levels = [
        GEXLevel(
            strike=l["strike"],
            net_gex=l["net_gex"],
            call_gex=l["call_gex"],
            put_gex=l["put_gex"],
            call_oi=l.get("call_oi", 0),
            put_oi=l.get("put_oi", 0),
        )
        for l in snap.get("levels", [])
    ]
    return GEXProfile(
        symbol=snap["symbol"],
        spot_price=snap["spot_price"] or 0.0,
        timestamp=snap["timestamp"] or "",
        levels=levels,
        max_gamma_strike=snap["max_gamma_strike"] or 0.0,
        zero_gamma_level=snap["zero_gamma_level"] or 0.0,
        put_wall=snap["put_wall"] or 0.0,
        call_wall=snap["call_wall"] or 0.0,
        gamma_flip=snap["gamma_flip"] or 0.0,
        total_gex=snap["total_gex"] or 0.0,
        source=snap.get("source", "alpaca"),
    )


def build_alpaca_gex_prompt_section(symbol: str) -> str:
    """
    GEX context block for AI prompt injection.
    Fast path: in-memory cache → DB snapshot (no live API call).
    """
    profile: Optional[GEXProfile] = None

    with _cache_lock:
        entry = _cache.get(symbol.upper())
        if entry:
            profile = entry["profile"]

    if profile is None:
        snap = get_latest_snapshot(symbol)
        if snap:
            profile = _profile_from_snapshot(snap)

    if profile is None or not profile.levels:
        return ""

    regime = (
        "PINNED (mean-reverting — fade extremes)"
        if profile.total_gex > 0
        else "VOLATILE (trending — ride momentum)"
    )

    age_note = ""
    try:
        snap_dt = datetime.fromisoformat(profile.timestamp)
        age_m = int((datetime.now() - snap_dt).total_seconds() / 60)
        age_note = f" [{age_m}m old]"
    except Exception:
        pass

    return "\n".join([
        f"=== ALPACA GEX — {profile.symbol}{age_note} ===",
        f"Spot: ${profile.spot_price:.2f} | Regime: {regime}",
        f"Total Net GEX: {profile.total_gex:+,.0f}",
        f"Max Gamma (PIN): ${profile.max_gamma_strike:.0f}",
        f"Call Wall (Resistance): ${profile.call_wall:.0f}",
        f"Put Wall (Support): ${profile.put_wall:.0f}",
        f"Gamma Flip (Vol Trigger): ${profile.gamma_flip:.0f}",
        "",
        "GEX rules:",
        "- Above gamma flip: dealers buy dips → price PINNED → mean-revert from edges",
        "- Below gamma flip: dealers sell rallies → TRENDING → follow momentum",
        "- Call wall = ceiling, put wall = floor — trade within this range or break with conviction",
    ])


# ── Scheduler helper ───────────────────────────────────────────────────────


def refresh_alpaca_gex(symbols: list | None = None) -> list:
    """
    Refresh Alpaca GEX for a list of symbols.
    Checks market hours internally. Called by the main.py scheduler.
    """
    try:
        from engine.risk_manager import RiskManager
        if not RiskManager.is_market_hours():
            return []
    except Exception:
        pass

    symbols = symbols or GEX_SYMBOLS
    results = []
    for sym in symbols:
        try:
            profile = compute_gex_sync(sym, force=True)
            if profile:
                regime = "PINNED" if profile.total_gex > 0 else "VOLATILE"
                console.log(
                    f"[cyan]Alpaca GEX {sym}: "
                    f"pin=${profile.max_gamma_strike:.0f} "
                    f"flip=${profile.gamma_flip:.0f} "
                    f"regime={regime}"
                )
                results.append(profile)
        except Exception as e:
            console.log(f"[red]Alpaca GEX refresh error for {sym}: {e}")
    return results
