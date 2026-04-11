"""GEX Overlay — strategic gamma exposure levels for all crew members.

Calculates per-strike GEX for SPY, QQQ, and any actively-held options names.
Identifies:
  - King Node: highest positive GEX strike (price magnet by EOD)
  - Gamma Flip: where net GEX crosses zero (vol regime boundary)
  - Put Wall: highest put OI strike (downside floor)
  - Call Wall: highest call OI strike (upside ceiling)
  - Gamma Walls: top 3 strikes above/below spot by |GEX|

Primary data: CBOE CDN (actual gammas from delayed quotes — already in gex_scanner.py).
Fallback: Yahoo Finance direct HTTP + Black-Scholes gamma approximation.

Updates every 15 minutes during market hours via update_all_gex_levels().
Posts War Room alert when regime changes (positive <-> negative gamma).
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import time
from datetime import datetime
from typing import Optional

import requests
from scipy.stats import norm

TRADER_DB = "data/trader.db"
GEX_SYMBOLS = ["SPY", "QQQ"]   # Core always-tracked symbols
RISK_FREE_RATE = 0.05

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [gex_overlay] %(levelname)s: %(message)s",
)
logger = logging.getLogger("gex_overlay")

# Track last regime per symbol to detect changes and avoid War Room spam
_last_regime: dict[str, str] = {}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _init_tables():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS gex_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                calc_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                spot_price REAL,
                king_node REAL,
                gamma_flip REAL,
                put_wall REAL,
                call_wall REAL,
                gamma_walls_above TEXT,
                gamma_walls_below TEXT,
                total_gex REAL,
                regime TEXT
            )
        """)
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_gex_sym_time ON gex_levels(symbol, calc_time)"
        )
        c.commit()
    # Append-only schema migration — add composite columns if absent
    for col, ctype in [
        ("composite_score", "REAL"),
        ("composite_signal", "TEXT"),
        ("composite_strength", "TEXT"),
    ]:
        try:
            with _conn() as c:
                c.execute(f"ALTER TABLE gex_levels ADD COLUMN {col} {ctype}")
                c.commit()
        except Exception as e:
            msg = str(e).lower()
            if "duplicate" in msg or "already exists" in msg:
                pass  # column present — normal after first run
            else:
                logger.debug(f"Migration note for {col}: {e}")


# ---------------------------------------------------------------------------
# Black-Scholes gamma (for Yahoo fallback)
# ---------------------------------------------------------------------------

def _bs_gamma(S: float, K: float, T: float, sigma: float, r: float = RISK_FREE_RATE) -> float:
    """Black-Scholes gamma for a single option."""
    if S <= 0 or K <= 0 or sigma <= 0:
        return 0.0
    T = max(T, 1 / 252)  # min 1 trading day to avoid divide-by-zero
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        return norm.pdf(d1) / (S * sigma * math.sqrt(T))
    except Exception:
        return 0.0


def _dte_years(expiration_epoch: int) -> float:
    """Convert expiration Unix timestamp to years from now."""
    seconds_left = max(0, expiration_epoch - time.time())
    return max(seconds_left / (365.25 * 24 * 3600), 1 / 252)


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def _fetch_cboe(symbol: str) -> dict | None:
    """Fetch GEX per-strike data from existing gex_scanner (CBOE CDN, has real gammas)."""
    try:
        from engine.gex_scanner import get_gex
        return get_gex(symbol, force=True)
    except Exception as e:
        logger.warning(f"CBOE fetch failed for {symbol}: {e}")
        return None


def _fetch_yahoo_chain(symbol: str, expiration_epoch: int | None = None) -> dict | None:
    """Fetch options chain via Yahoo Finance direct HTTP (no yfinance).

    Returns dict with 'spot', 'calls', 'puts', 'expiration', 'all_expirations' or None.
    """
    url = f"https://query2.finance.yahoo.com/v7/finance/options/{symbol}"
    params: dict = {}
    if expiration_epoch:
        params["date"] = expiration_epoch

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        result = data.get("optionChain", {}).get("result", [])
        if not result:
            return None

        r = result[0]
        quote = r.get("quote", {})
        spot = (
            quote.get("regularMarketPrice")
            or quote.get("ask")
            or quote.get("bid")
        )
        if not spot:
            return None

        options = r.get("options", [])
        if not options:
            return None

        chain = options[0]
        return {
            "spot": float(spot),
            "calls": chain.get("calls", []),
            "puts": chain.get("puts", []),
            "expiration": chain.get("expirationDate"),
            "all_expirations": r.get("expirationDates", []),
        }
    except Exception as e:
        logger.warning(f"Yahoo chain fetch failed for {symbol}: {e}")
        return None


def _get_nearest_expiry_chain(symbol: str) -> dict | None:
    """Get the nearest-expiration chain from Yahoo (today if 0DTE options exist)."""
    chain = _fetch_yahoo_chain(symbol)
    if not chain:
        return None

    today_ts = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
    today_end = today_ts + 86400
    all_exps = sorted(chain.get("all_expirations", []))

    # If we already have today's expiration loaded, return it
    exp = chain.get("expiration")
    if exp and today_ts <= exp <= today_end:
        return chain

    # Find nearest future expiration
    future_exps = [e for e in all_exps if e >= today_ts]
    if not future_exps:
        return chain

    nearest = future_exps[0]
    if nearest == exp:
        return chain  # already loaded

    # Fetch that specific expiration
    return _fetch_yahoo_chain(symbol, nearest)


def _compute_gex_from_yahoo(chain: dict) -> dict | None:
    """Compute per-strike GEX from Yahoo chain using Black-Scholes gamma.

    Returns dict with 'spot', 'strikes' (list of per-strike dicts).
    """
    spot = chain.get("spot", 0)
    if not spot:
        return None

    calls = chain.get("calls", [])
    puts = chain.get("puts", [])
    expiration = chain.get("expiration")
    T = _dte_years(expiration) if expiration else 1 / 252

    strikes_map: dict[float, dict] = {}

    def _add_leg(opts, is_call: bool):
        for opt in opts:
            K = opt.get("strike")
            iv = opt.get("impliedVolatility") or opt.get("implied_volatility")
            oi = opt.get("openInterest") or opt.get("open_interest") or 0
            if not K or not iv or oi <= 0:
                continue
            try:
                K = float(K)
                iv = float(iv)
                oi = int(oi)
            except (ValueError, TypeError):
                continue

            gamma = _bs_gamma(spot, K, T, iv)
            gex_val = spot * gamma * oi * 100 * spot * 0.01

            if K not in strikes_map:
                strikes_map[K] = {
                    "strike": K,
                    "call_gex": 0.0, "put_gex": 0.0,
                    "call_oi": 0, "put_oi": 0,
                }

            if is_call:
                strikes_map[K]["call_gex"] += gex_val
                strikes_map[K]["call_oi"] += oi
            else:
                strikes_map[K]["put_gex"] -= gex_val  # puts negative GEX
                strikes_map[K]["put_oi"] += oi

    _add_leg(calls, is_call=True)
    _add_leg(puts, is_call=False)

    if not strikes_map:
        return None

    strikes_list = []
    for s in strikes_map.values():
        s["net_gex"] = round(s["call_gex"] + s["put_gex"], 2)
        strikes_list.append(s)

    strikes_list.sort(key=lambda x: x["strike"])

    # Filter ±10% of spot
    lower, upper = spot * 0.90, spot * 1.10
    relevant = [s for s in strikes_list if lower <= s["strike"] <= upper] or strikes_list

    return {"spot": spot, "strikes": relevant}


# ---------------------------------------------------------------------------
# Composite momentum score helpers
# ---------------------------------------------------------------------------

def _build_chain_df_from_cboe(strikes: list):
    """Build a pandas DataFrame from CBOE strikes list (OI only — no volume/IV)."""
    try:
        import pandas as pd
        rows = []
        for s in strikes:
            k = s.get("strike")
            if not k:
                continue
            if s.get("call_oi", 0) > 0:
                rows.append({"type": "call", "strike": float(k),
                             "oi": int(s["call_oi"]), "volume": 0, "iv": 0.0})
            if s.get("put_oi", 0) > 0:
                rows.append({"type": "put", "strike": float(k),
                             "oi": int(s["put_oi"]), "volume": 0, "iv": 0.0})
        return pd.DataFrame(rows) if rows else None
    except ImportError:
        return None
    except Exception:
        return None


def _build_chain_df_from_yahoo(chain: dict):
    """Build a pandas DataFrame from Yahoo chain dict (has volume + IV)."""
    try:
        import pandas as pd
        rows = []
        for opt in chain.get("calls", []):
            k = opt.get("strike")
            if not k:
                continue
            rows.append({
                "type": "call",
                "strike": float(k),
                "oi": int(opt.get("openInterest") or opt.get("open_interest") or 0),
                "volume": int(opt.get("volume") or 0),
                "iv": float(opt.get("impliedVolatility") or opt.get("implied_volatility") or 0),
            })
        for opt in chain.get("puts", []):
            k = opt.get("strike")
            if not k:
                continue
            rows.append({
                "type": "put",
                "strike": float(k),
                "oi": int(opt.get("openInterest") or opt.get("open_interest") or 0),
                "volume": int(opt.get("volume") or 0),
                "iv": float(opt.get("impliedVolatility") or opt.get("implied_volatility") or 0),
            })
        return pd.DataFrame(rows) if rows else None
    except ImportError:
        return None
    except Exception:
        return None


def _compute_composite(chain_df, levels: dict) -> dict:
    """Call gex_engine.calculate_composite_score. Returns score/signal/strength or nulls."""
    _null = {"composite_score": None, "composite_signal": None, "composite_strength": None}
    if chain_df is None:
        return _null
    try:
        from engine.gex_engine import calculate_composite_score
        spot = levels.get("spot_price") or 0
        regime_raw = levels.get("regime", "")
        gex_profile = {
            "regime": "positive" if regime_raw == "positive_gamma" else "negative",
            "gamma_flip": levels.get("gamma_flip"),
        }
        result = calculate_composite_score(chain_df, gex_profile, spot)
        return {
            "composite_score": result.get("composite"),
            "composite_signal": result.get("signal"),
            "composite_strength": result.get("strength"),
        }
    except Exception as e:
        logger.debug(f"Composite score skipped: {e}")
        return _null


# ---------------------------------------------------------------------------
# Key level extraction
# ---------------------------------------------------------------------------

def _extract_key_levels(data: dict) -> dict:
    """Extract king_node, gamma_flip, put_wall, call_wall, gamma_walls from strike data.

    Works with output from both gex_scanner (CBOE) and _compute_gex_from_yahoo.
    """
    spot = data.get("spot", 0)
    strikes = data.get("strikes", [])

    if not strikes:
        return {}

    above = [s for s in strikes if s["strike"] > spot]
    below = [s for s in strikes if s["strike"] <= spot]

    # King node: strike with highest POSITIVE net GEX
    king_node = None
    positive_strikes = [s for s in strikes if s.get("net_gex", 0) > 0]
    if positive_strikes:
        king_node = max(positive_strikes, key=lambda s: s["net_gex"])["strike"]

    # Gamma flip: where net GEX sign changes (first crossing nearest to spot)
    gamma_flip = None
    sorted_strikes = sorted(strikes, key=lambda x: x["strike"])
    best_flip_dist = float("inf")
    for i in range(len(sorted_strikes) - 1):
        curr = sorted_strikes[i]["net_gex"]
        nxt = sorted_strikes[i + 1]["net_gex"]
        if curr * nxt < 0:  # sign change
            mid = (sorted_strikes[i]["strike"] + sorted_strikes[i + 1]["strike"]) / 2
            dist = abs(mid - spot)
            if dist < best_flip_dist:
                best_flip_dist = dist
                gamma_flip = round(mid, 2)

    # Put wall: highest put OI (downside floor)
    put_wall = None
    put_strikes = [s for s in strikes if s.get("put_oi", 0) > 0]
    if put_strikes:
        put_wall = max(put_strikes, key=lambda s: s.get("put_oi", 0))["strike"]

    # Call wall: highest call OI (upside ceiling)
    call_wall = None
    call_strikes = [s for s in strikes if s.get("call_oi", 0) > 0]
    if call_strikes:
        call_wall = max(call_strikes, key=lambda s: s.get("call_oi", 0))["strike"]

    # Gamma walls: top 3 strikes above/below by |net_gex|
    gamma_walls_above = [
        s["strike"]
        for s in sorted(above, key=lambda x: abs(x.get("net_gex", 0)), reverse=True)[:3]
    ]
    gamma_walls_below = [
        s["strike"]
        for s in sorted(below, key=lambda x: abs(x.get("net_gex", 0)), reverse=True)[:3]
    ]

    total_gex = sum(s.get("net_gex", 0) for s in strikes)
    regime = "positive_gamma" if total_gex >= 0 else "negative_gamma"

    return {
        "spot_price": spot,
        "king_node": king_node,
        "gamma_flip": gamma_flip,
        "put_wall": put_wall,
        "call_wall": call_wall,
        "gamma_walls_above": gamma_walls_above,
        "gamma_walls_below": gamma_walls_below,
        "total_gex": round(total_gex, 2),
        "regime": regime,
        "strikes": strikes,  # full data for heatmap endpoint
    }


# ---------------------------------------------------------------------------
# Main calculation
# ---------------------------------------------------------------------------

def calculate_gex(symbol: str) -> dict | None:
    """Calculate GEX key levels for a symbol.

    Primary: CBOE CDN via gex_scanner (actual gammas — most accurate).
    Fallback: Yahoo Finance direct HTTP + Black-Scholes gamma approximation.

    Returns dict with king_node, gamma_flip, put_wall, call_wall, gamma_walls, regime.
    """
    symbol = symbol.upper()

    # 1. CBOE (has real gammas)
    cboe_data = _fetch_cboe(symbol)
    if cboe_data and cboe_data.get("strikes"):
        levels = _extract_key_levels(cboe_data)
        if levels and levels.get("spot_price"):
            logger.debug(
                f"GEX {symbol} (CBOE): king={levels.get('king_node')} "
                f"flip={levels.get('gamma_flip')} regime={levels.get('regime')}"
            )
            chain_df = _build_chain_df_from_cboe(cboe_data["strikes"])
            levels.update(_compute_composite(chain_df, levels))
            return levels

    # 2. Yahoo + Black-Scholes fallback
    logger.info(f"CBOE fallback for {symbol} → trying Yahoo direct HTTP")
    chain = _get_nearest_expiry_chain(symbol)
    if chain:
        computed = _compute_gex_from_yahoo(chain)
        if computed:
            levels = _extract_key_levels(computed)
            if levels and levels.get("spot_price"):
                chain_df = _build_chain_df_from_yahoo(chain)
                levels.update(_compute_composite(chain_df, levels))
                return levels

    logger.warning(f"GEX calculation failed for {symbol}")
    return None


# ---------------------------------------------------------------------------
# DB persistence and regime tracking
# ---------------------------------------------------------------------------

def _save_gex_levels(symbol: str, levels: dict):
    """Persist computed GEX levels to gex_levels table."""
    _init_tables()
    try:
        with _conn() as c:
            base_params = (
                symbol,
                datetime.now().isoformat(),
                levels.get("spot_price"),
                levels.get("king_node"),
                levels.get("gamma_flip"),
                levels.get("put_wall"),
                levels.get("call_wall"),
                json.dumps(levels.get("gamma_walls_above", [])),
                json.dumps(levels.get("gamma_walls_below", [])),
                levels.get("total_gex"),
                levels.get("regime"),
            )
            try:
                c.execute(
                    """INSERT INTO gex_levels
                       (symbol, calc_time, spot_price, king_node, gamma_flip,
                        put_wall, call_wall, gamma_walls_above, gamma_walls_below,
                        total_gex, regime,
                        composite_score, composite_signal, composite_strength)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    base_params + (
                        levels.get("composite_score"),
                        levels.get("composite_signal"),
                        levels.get("composite_strength"),
                    ),
                )
            except Exception:
                # Composite columns not yet migrated — insert without them
                c.execute(
                    """INSERT INTO gex_levels
                       (symbol, calc_time, spot_price, king_node, gamma_flip,
                        put_wall, call_wall, gamma_walls_above, gamma_walls_below,
                        total_gex, regime)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    base_params,
                )
            c.commit()
    except Exception as e:
        logger.warning(f"Failed to save GEX levels for {symbol}: {e}")


def _check_regime_change(symbol: str, new_regime: str) -> bool:
    """Return True if regime changed from last recorded value."""
    old = _last_regime.get(symbol)
    _last_regime[symbol] = new_regime
    return old is not None and old != new_regime


def _post_regime_alert(symbol: str, regime: str, levels: dict):
    """Post War Room alert on gamma regime change."""
    try:
        from engine.war_room import save_hot_take
        king = levels.get("king_node")
        flip = levels.get("gamma_flip")
        king_str = f"${king:.0f}" if king else "?"
        flip_str = f"${flip:.0f}" if flip else "?"

        if regime == "negative_gamma":
            msg = (
                f"🔮 GEX ALERT: {symbol} gamma regime shifted to NEGATIVE GAMMA. "
                f"Dealers now amplifying moves. Expect increased volatility. "
                f"King Node: {king_str}. All hands: tighten stops."
            )
        else:
            msg = (
                f"🔮 GEX ALERT: {symbol} gamma regime shifted to POSITIVE GAMMA. "
                f"Dealers dampening volatility. Price gravitating to King Node {king_str}. "
                f"Gamma Flip: {flip_str}. Range-bound conditions expected."
            )

        save_hot_take("navigator", symbol, msg)
        logger.info(f"GEX regime change posted to War Room: {symbol} → {regime}")
    except Exception as e:
        logger.warning(f"GEX regime War Room post failed: {e}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def update_all_gex_levels() -> None:
    """Update GEX for SPY, QQQ, and any symbols DayBlade/Sulu hold options on.

    Called every 15 minutes during market hours.
    """
    try:
        from engine.risk_manager import RiskManager
        mh = RiskManager.is_market_hours()
        if not mh:  # False on weekends; string values on weekdays are truthy
            return
    except Exception:
        pass

    _init_tables()
    symbols = list(GEX_SYMBOLS)

    # Add symbols from active options positions
    try:
        c = sqlite3.connect(TRADER_DB, check_same_thread=False)
        c.row_factory = sqlite3.Row
        rows = c.execute(
            "SELECT DISTINCT symbol FROM positions "
            "WHERE player_id IN ('dayblade-0dte', 'sulu-lt') AND asset_type='option'"
        ).fetchall()
        c.close()
        for r in rows:
            if r["symbol"] not in symbols:
                symbols.append(r["symbol"])
    except Exception:
        pass

    logger.info(f"Updating GEX levels for: {symbols}")

    for sym in symbols:
        try:
            levels = calculate_gex(sym)
            if levels:
                _save_gex_levels(sym, levels)
                new_regime = levels.get("regime", "")
                if new_regime and _check_regime_change(sym, new_regime):
                    _post_regime_alert(sym, new_regime, levels)
        except Exception as e:
            logger.warning(f"GEX update failed for {sym}: {e}")
        time.sleep(0.5)

    # Prune rows older than 7 days to keep table lean
    try:
        with _conn() as c:
            c.execute(
                "DELETE FROM gex_levels WHERE calc_time < datetime('now', '-7 days')"
            )
            c.commit()
    except Exception:
        pass


def get_latest_gex(symbol: str) -> dict | None:
    """Return most recent gex_levels row for symbol from DB."""
    _init_tables()
    symbol = symbol.upper()
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT * FROM gex_levels WHERE symbol=? ORDER BY calc_time DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        if row:
            keys = row.keys()
            return {
                "symbol": row["symbol"],
                "calc_time": row["calc_time"],
                "spot_price": row["spot_price"],
                "king_node": row["king_node"],
                "gamma_flip": row["gamma_flip"],
                "put_wall": row["put_wall"],
                "call_wall": row["call_wall"],
                "gamma_walls_above": json.loads(row["gamma_walls_above"] or "[]"),
                "gamma_walls_below": json.loads(row["gamma_walls_below"] or "[]"),
                "total_gex": row["total_gex"],
                "regime": row["regime"],
                "composite_score": row["composite_score"] if "composite_score" in keys else None,
                "composite_signal": row["composite_signal"] if "composite_signal" in keys else None,
                "composite_strength": row["composite_strength"] if "composite_strength" in keys else None,
            }
    except Exception as e:
        logger.warning(f"get_latest_gex failed for {symbol}: {e}")
    return None


def get_gex_context_for_prompt() -> str:
    """Build formatted GEX text for injection into ALL AI model prompts.

    If no DB data exists, attempts a fresh calculation.
    """
    _init_tables()
    lines = []

    for sym in ["SPY", "QQQ"]:
        gex = get_latest_gex(sym)
        if not gex:
            # Try computing fresh
            try:
                levels = calculate_gex(sym)
                if levels:
                    _save_gex_levels(sym, levels)
                    gex = get_latest_gex(sym)
            except Exception:
                pass

        if not gex:
            continue

        regime = gex.get("regime", "")
        if regime == "positive_gamma":
            regime_label = "POSITIVE GAMMA (dealers dampening vol, expect range-bound)"
        else:
            regime_label = "NEGATIVE GAMMA (dealers amplifying vol, expect big moves)"

        detail_parts = []
        if gex.get("king_node"):
            detail_parts.append(f"King Node ${gex['king_node']:.0f}")
        if gex.get("gamma_flip"):
            detail_parts.append(f"Gamma Flip ${gex['gamma_flip']:.0f}")
        if gex.get("put_wall"):
            detail_parts.append(f"Put Wall ${gex['put_wall']:.0f}")
        if gex.get("call_wall"):
            detail_parts.append(f"Call Wall ${gex['call_wall']:.0f}")
        detail_parts.append(f"Regime: {regime_label}")
        lines.append(f"{sym}: " + " | ".join(detail_parts))

    if not lines:
        return ""

    # Get latest calc timestamp
    latest_time = ""
    try:
        gex = get_latest_gex("SPY") or get_latest_gex("QQQ")
        if gex and gex.get("calc_time"):
            ct = str(gex["calc_time"])
            latest_time = ct.split("T")[1][:5] if "T" in ct else ct[:16]
    except Exception:
        pass

    header = f"=== GEX OVERLAY (updated {latest_time}) ===" if latest_time else "=== GEX OVERLAY ==="
    return "\n".join([header] + lines)


def get_heatmap_data(symbol: str) -> list[dict]:
    """Return per-strike GEX data for the heatmap endpoint.

    Computes fresh (per-strike data is not persisted to DB).
    """
    symbol = symbol.upper()
    try:
        levels = calculate_gex(symbol)
        if levels and levels.get("strikes"):
            return [
                {
                    "strike": s["strike"],
                    "call_gex": round(s.get("call_gex", 0), 2),
                    "put_gex": round(s.get("put_gex", 0), 2),
                    "net_gex": round(s.get("net_gex", 0), 2),
                    "call_oi": s.get("call_oi", 0),
                    "put_oi": s.get("put_oi", 0),
                }
                for s in levels["strikes"]
            ]
    except Exception as e:
        logger.warning(f"Heatmap data failed for {symbol}: {e}")
    return []
