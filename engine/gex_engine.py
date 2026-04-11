"""
TradeMinds GEX Engine v2 — Lt. Uhura's Gamma Exposure Module
=============================================================
Season 5 | Stardate 2026.03

UPGRADE LAYER on top of existing engine/gex_scanner.py
Adds: composite scoring, historical DB storage, Uhura/Debate integration,
      FlashAlpha validation, Ship's Computer queries

Deployment: ~/autonomous-trader/engine/gex_engine.py
Sits alongside: ~/autonomous-trader/engine/gex_scanner.py (existing CBOE scraper)

Usage:
    python engine/gex_engine.py                    # Morning scan (SPY, QQQ, IWM)
    python engine/gex_engine.py --ticker SPY       # Single ticker
    python engine/gex_engine.py --validate         # Cross-check vs FlashAlpha
    python engine/gex_engine.py --query SPY        # Quick Ship's Computer query
"""

import numpy as np
import pandas as pd
import sqlite3
import requests
import json
import argparse
import os
import sys
import traceback
from datetime import datetime, timedelta
from scipy.stats import norm


# ============================================================
# CONFIGURATION
# ============================================================

CORE_TICKERS = ["SPY", "QQQ", "IWM"]
DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
FLASHALPHA_API_KEY = os.environ.get("FLASHALPHA_API_KEY", "")
RISK_FREE_RATE = 0.043
NUM_EXPIRATIONS = 4
STRIKE_RANGE_PCT = 0.15

DIV_YIELDS = {
    "SPY": 0.013, "QQQ": 0.006, "IWM": 0.012,
    "DIA": 0.018, "XLF": 0.016, "XLE": 0.035,
}


# ============================================================
# STEP 1: DATA SOURCE ADAPTER
# Tries your existing gex_scanner.py first, falls back to yfinance
# ============================================================

class GEXDataAdapter:
    """
    Adapter that pulls GEX data from whatever source is available.
    Priority: existing gex_scanner.py → CBOE direct → yfinance fallback
    """

    def __init__(self):
        self.source = None
        self._detect_source()

    def _detect_source(self):
        """Auto-detect the best available data source."""

        # Try 1: Import existing gex_scanner
        try:
            # Add parent dir to path so we can import engine modules
            engine_dir = os.path.dirname(os.path.abspath(__file__))
            if engine_dir not in sys.path:
                sys.path.insert(0, engine_dir)
            parent_dir = os.path.dirname(engine_dir)
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)

            import gex_scanner
            self.scanner = gex_scanner
            self.source = "gex_scanner"
            print("  [DATA] Using existing gex_scanner.py (CBOE scraper)")
            return
        except ImportError:
            pass

        # Try 2: Direct CBOE API (same as your scanner likely uses)
        try:
            test_url = "https://cdn.cboe.com/api/global/delayed_quotes/options/SPY.json"
            resp = requests.get(test_url, timeout=5)
            if resp.status_code == 200:
                self.source = "cboe_direct"
                print("  [DATA] Using direct CBOE delayed quotes API")
                return
        except Exception:
            pass

        # Try 3: yfinance fallback
        try:
            import yfinance
            self.yf = yfinance
            self.source = "yfinance"
            print("  [DATA] Using yfinance fallback (unlimited, free)")
            return
        except ImportError:
            pass

        print("  [!] No data source available. Install yfinance: pip install yfinance")
        self.source = None

    def fetch(self, ticker):
        """
        Fetch GEX data for a ticker. Returns standardized dict:
        {
            "spot": float,
            "per_strike": DataFrame with columns [strike, call_gex, put_gex, net_gex, oi, volume],
            "net_gex": float,
            "gamma_flip": float,
            "call_wall": float,
            "put_wall": float,
            "regime": str,
            "king_strike": float,
            "king_value": float,
            "raw_chain": DataFrame (full options chain for scoring)
        }
        """
        if self.source == "gex_scanner":
            return self._fetch_from_scanner(ticker)
        elif self.source == "cboe_direct":
            return self._fetch_from_cboe(ticker)
        elif self.source == "yfinance":
            return self._fetch_from_yfinance(ticker)
        else:
            return None

    # ---------------------------------------------------------
    # Source A: Your existing gex_scanner.py
    # ---------------------------------------------------------
    def _fetch_from_scanner(self, ticker):
        """
        Adapt output from your existing gex_scanner.py.
        This handles multiple possible function signatures:
          - get_gex(ticker) / scan_gex(ticker) / calculate_gex(ticker)
          - fetch_options(ticker) + calculate_gex(data)
        """
        scanner = self.scanner
        result = None

        # Try common function names your scanner might use
        for func_name in ["get_gex", "scan_gex", "calculate_gex", "fetch_gex",
                          "get_gex_data", "scan_ticker", "run_scan"]:
            fn = getattr(scanner, func_name, None)
            if fn and callable(fn):
                try:
                    result = fn(ticker)
                    break
                except Exception as e:
                    print(f"  [!] {func_name}({ticker}) failed: {e}")
                    continue

        if result is None:
            print(f"  [!] Could not call gex_scanner for {ticker}, falling back to CBOE direct")
            return self._fetch_from_cboe(ticker)

        # Normalize the result — handle dict or tuple or dataframe
        return self._normalize_scanner_output(result, ticker)

    def _normalize_scanner_output(self, result, ticker):
        """
        Convert whatever gex_scanner returns into our standard format.
        Handles common output shapes.
        """
        # If result is already a dict with the keys we need
        if isinstance(result, dict):
            spot = result.get("spot") or result.get("spot_price") or result.get("current_price", 0)
            net_gex = result.get("net_gex") or result.get("total_gex") or result.get("gex", 0)
            gamma_flip = result.get("gamma_flip") or result.get("flip") or result.get("zero_gamma", spot)
            call_wall = result.get("call_wall") or result.get("resistance") or result.get("call_resistance")
            put_wall = result.get("put_wall") or result.get("support") or result.get("put_support")
            regime = result.get("regime") or ("positive" if net_gex > 0 else "negative")

            # Try to get per-strike data
            per_strike = result.get("per_strike") or result.get("strikes") or result.get("by_strike")
            raw_chain = result.get("chain") or result.get("options") or result.get("raw_chain")

            if isinstance(per_strike, list):
                per_strike = pd.DataFrame(per_strike)
            if isinstance(raw_chain, list):
                raw_chain = pd.DataFrame(raw_chain)

            # Build per_strike if we only have a list of dicts
            if per_strike is not None and isinstance(per_strike, pd.DataFrame):
                # Normalize column names
                col_map = {
                    "gex": "net_gex", "gamma_exposure": "net_gex",
                    "open_interest": "oi", "openInterest": "oi",
                    "strike_price": "strike", "strikePrice": "strike",
                }
                per_strike = per_strike.rename(columns=col_map)

                if "net_gex" in per_strike.columns:
                    if "call_gex" not in per_strike.columns:
                        per_strike["call_gex"] = per_strike["net_gex"].clip(lower=0)
                    if "put_gex" not in per_strike.columns:
                        per_strike["put_gex"] = per_strike["net_gex"].clip(upper=0)

            # Find king strike
            king_strike = None
            king_value = None
            if per_strike is not None and "net_gex" in per_strike.columns and len(per_strike) > 0:
                king_idx = per_strike["net_gex"].abs().idxmax()
                king_strike = per_strike.loc[king_idx, "strike"]
                king_value = per_strike.loc[king_idx, "net_gex"]

            return {
                "spot": spot,
                "per_strike": per_strike,
                "net_gex": net_gex,
                "gamma_flip": gamma_flip,
                "call_wall": call_wall,
                "put_wall": put_wall,
                "regime": regime,
                "king_strike": king_strike,
                "king_value": king_value,
                "raw_chain": raw_chain,
                "source": "gex_scanner",
            }

        # If result is a tuple (common pattern: data, spot)
        if isinstance(result, tuple) and len(result) >= 2:
            data, spot = result[0], result[1]
            if isinstance(data, pd.DataFrame):
                return self._build_profile_from_dataframe(data, spot, ticker)

        print(f"  [!] Unrecognized output from gex_scanner: {type(result)}")
        return self._fetch_from_cboe(ticker)

    # ---------------------------------------------------------
    # Source B: Direct CBOE API (same source your scanner uses)
    # ---------------------------------------------------------
    def _fetch_from_cboe(self, ticker):
        """Fetch directly from CBOE delayed quotes API."""
        url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker}.json"

        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"  [!] CBOE returned {resp.status_code} for {ticker}")
                if self.source != "yfinance":
                    return self._fetch_from_yfinance(ticker)
                return None

            data = resp.json()
            return self._parse_cboe_data(data, ticker)

        except Exception as e:
            print(f"  [!] CBOE fetch failed: {e}")
            return self._fetch_from_yfinance(ticker)

    def _parse_cboe_data(self, data, ticker):
        """Parse CBOE delayed quotes JSON into standard format."""
        try:
            # CBOE format: data.data.options is a list of option contracts
            spot = data.get("data", {}).get("close", 0) or data.get("data", {}).get("current_price", 0)
            options_list = data.get("data", {}).get("options", [])

            if not options_list or not spot:
                return None

            q = DIV_YIELDS.get(ticker, 0.01)
            today = datetime.now()
            rows = []

            for opt in options_list:
                try:
                    strike = float(opt.get("strike", 0))
                    if abs(strike - spot) / spot > STRIKE_RANGE_PCT:
                        continue

                    opt_type = opt.get("option_type", "").lower()
                    if opt_type not in ("call", "put"):
                        # Try parsing from option symbol
                        opt_type = "put" if "P" in str(opt.get("option", ""))[-10:] else "call"

                    oi = int(opt.get("open_interest", 0) or 0)
                    volume = int(opt.get("volume", 0) or 0)
                    iv = float(opt.get("iv", 0) or opt.get("implied_volatility", 0) or 0)
                    gamma = float(opt.get("gamma", 0) or 0)
                    delta = float(opt.get("delta", 0) or 0)

                    if oi == 0:
                        continue

                    # Parse expiration
                    exp_str = opt.get("expiration_date", "") or opt.get("expiry", "")
                    if exp_str:
                        try:
                            exp_date = datetime.strptime(exp_str[:10], "%Y-%m-%d")
                            T = max((exp_date - today).days / 365.0, 1 / 365.0)
                        except ValueError:
                            T = 30 / 365.0  # default ~1 month
                    else:
                        T = 30 / 365.0

                    # Calculate GEX
                    if gamma > 0:
                        gex = gamma * oi * 100 * spot * spot * 0.01
                    elif iv > 0:
                        # Recalculate gamma from BS if CBOE didn't provide it
                        gamma = _calc_bs_gamma(spot, strike, iv, T, RISK_FREE_RATE, q)
                        gex = gamma * oi * 100 * spot * spot * 0.01
                    else:
                        gex = 0

                    if opt_type == "put":
                        gex *= -1

                    rows.append({
                        "strike": strike,
                        "type": opt_type,
                        "oi": oi,
                        "volume": volume,
                        "iv": iv,
                        "gamma": gamma,
                        "delta": delta,
                        "gex": gex,
                        "T": T,
                    })
                except (ValueError, TypeError):
                    continue

            if not rows:
                return None

            df = pd.DataFrame(rows)
            return self._build_profile_from_dataframe(df, spot, ticker)

        except Exception as e:
            print(f"  [!] CBOE parse error: {e}")
            traceback.print_exc()
            return None

    # ---------------------------------------------------------
    # Source C: yfinance fallback
    # ---------------------------------------------------------
    def _fetch_from_yfinance(self, ticker):
        """Fallback to yfinance if CBOE/scanner unavailable."""
        try:
            import yfinance as yf
        except ImportError:
            print("  [!] yfinance not installed. pip install yfinance")
            return None

        try:
            t = yf.Ticker(ticker)
            spot = None
            try:
                spot = t.fast_info["lastPrice"]
            except Exception:
                spot = t.info.get("regularMarketPrice") or t.info.get("previousClose")

            if not spot:
                return None

            expirations = t.options[:NUM_EXPIRATIONS]
            q = DIV_YIELDS.get(ticker, 0.01)
            today = datetime.now()
            rows = []

            for exp_str in expirations:
                try:
                    chain = t.option_chain(exp_str)
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
                    T = max((exp_date - today).days / 365.0, 1 / 365.0)

                    for _, row in chain.calls.iterrows():
                        strike = row["strike"]
                        if abs(strike - spot) / spot > STRIKE_RANGE_PCT:
                            continue
                        iv = row.get("impliedVolatility", 0)
                        oi = int(row.get("openInterest", 0) or 0)
                        vol = int(row.get("volume", 0) or 0)
                        if oi == 0 or pd.isna(iv) or iv <= 0:
                            continue
                        gamma = _calc_bs_gamma(spot, strike, iv, T, RISK_FREE_RATE, q)
                        gex = gamma * oi * 100 * spot * spot * 0.01
                        rows.append({"strike": strike, "type": "call", "oi": oi,
                                     "volume": vol, "iv": iv, "gamma": gamma,
                                     "delta": row.get("delta", 0) or 0, "gex": gex, "T": T})

                    for _, row in chain.puts.iterrows():
                        strike = row["strike"]
                        if abs(strike - spot) / spot > STRIKE_RANGE_PCT:
                            continue
                        iv = row.get("impliedVolatility", 0)
                        oi = int(row.get("openInterest", 0) or 0)
                        vol = int(row.get("volume", 0) or 0)
                        if oi == 0 or pd.isna(iv) or iv <= 0:
                            continue
                        gamma = _calc_bs_gamma(spot, strike, iv, T, RISK_FREE_RATE, q)
                        gex = gamma * oi * 100 * spot * spot * 0.01 * -1
                        rows.append({"strike": strike, "type": "put", "oi": oi,
                                     "volume": vol, "iv": iv, "gamma": gamma,
                                     "delta": row.get("delta", 0) or 0, "gex": gex, "T": T})
                except Exception:
                    continue

            if not rows:
                return None

            df = pd.DataFrame(rows)
            return self._build_profile_from_dataframe(df, spot, ticker)

        except Exception as e:
            print(f"  [!] yfinance error: {e}")
            return None

    # ---------------------------------------------------------
    # Shared: Build standard profile from raw chain DataFrame
    # ---------------------------------------------------------
    def _build_profile_from_dataframe(self, df, spot, ticker):
        """Build standard GEX profile from a raw chain DataFrame."""
        # Aggregate by strike
        per_strike = df.groupby("strike").agg(
            net_gex=("gex", "sum"),
            total_oi=("oi", "sum"),
            total_volume=("volume", "sum"),
        ).reset_index()

        # Call vs put GEX
        call_gex = df[df["type"] == "call"].groupby("strike")["gex"].sum()
        put_gex = df[df["type"] == "put"].groupby("strike")["gex"].sum()
        per_strike["call_gex"] = per_strike["strike"].map(call_gex).fillna(0)
        per_strike["put_gex"] = per_strike["strike"].map(put_gex).fillna(0)
        per_strike = per_strike.sort_values("strike")

        net_gex = per_strike["net_gex"].sum()
        regime = "positive" if net_gex > 0 else "negative"

        # Walls
        pos = per_strike[per_strike["net_gex"] > 0]
        neg = per_strike[per_strike["net_gex"] < 0]
        call_wall = pos.loc[pos["net_gex"].idxmax(), "strike"] if len(pos) > 0 else None
        put_wall = neg.loc[neg["net_gex"].idxmin(), "strike"] if len(neg) > 0 else None

        # King strike
        king_idx = per_strike["net_gex"].abs().idxmax()
        king_strike = per_strike.loc[king_idx, "strike"]
        king_value = per_strike.loc[king_idx, "net_gex"]

        # Gamma flip
        gamma_flip = _find_gamma_flip(per_strike, spot)

        return {
            "spot": spot,
            "per_strike": per_strike,
            "net_gex": net_gex,
            "gamma_flip": gamma_flip,
            "call_wall": call_wall,
            "put_wall": put_wall,
            "regime": regime,
            "king_strike": king_strike,
            "king_value": king_value,
            "raw_chain": df,
            "source": self.source,
        }


# ============================================================
# HELPERS (module-level)
# ============================================================

def _calc_bs_gamma(S, K, vol, T, r=0.043, q=0.0):
    """Black-Scholes gamma."""
    if T <= 0 or vol <= 0 or S <= 0:
        return 0.0
    d1 = (np.log(S / K) + (r - q + 0.5 * vol**2) * T) / (vol * np.sqrt(T))
    return np.exp(-q * T) * norm.pdf(d1) / (S * vol * np.sqrt(T))


def _find_gamma_flip(per_strike, spot):
    """Find where net GEX crosses zero nearest to spot."""
    df = per_strike.sort_values("strike").reset_index(drop=True)
    for i in range(len(df) - 1):
        a, b = df.loc[i, "net_gex"], df.loc[i + 1, "net_gex"]
        sa, sb = df.loc[i, "strike"], df.loc[i + 1, "strike"]
        if a * b < 0:
            flip = sa + (sb - sa) * abs(a) / (abs(a) + abs(b))
            if abs(flip - spot) / spot < 0.10:
                return round(flip, 2)
    return spot


# ============================================================
# STEP 2: COMPOSITE MOMENTUM SCORING
# ============================================================

def calculate_composite_score(raw_chain, gex_profile, spot):
    """
    Multi-signal composite score (0-100):
      30% GEX Signal      — regime & distance to gamma flip
      25% Delta Flow       — call vs put volume ratio
      20% Gamma Squeeze    — OI concentration near spot
      15% Vanna Signal     — call/put IV differential
      10% IV Skew Signal   — put premium over calls
    """
    df = raw_chain
    if df is None or len(df) == 0:
        return _neutral_score()

    # --- GEX Signal (30%) ---
    gamma_flip = gex_profile.get("gamma_flip", spot)
    flip_distance = (spot - gamma_flip) / spot if gamma_flip else 0

    if gex_profile["regime"] == "positive":
        gex_score = 50 + min(flip_distance * 500, 40)
    else:
        gex_score = 50 - min(abs(flip_distance) * 500, 40)
    gex_score = max(10, min(90, gex_score))

    # --- Delta Flow (25%) ---
    calls = df[df["type"] == "call"]
    puts = df[df["type"] == "put"]
    call_vol = calls["volume"].sum() if "volume" in df.columns else 0
    put_vol = puts["volume"].sum() if "volume" in df.columns else 0
    total_vol = call_vol + put_vol
    delta_score = (call_vol / total_vol * 100) if total_vol > 0 else 50

    # --- Gamma Squeeze (20%) ---
    total_oi = df["oi"].sum() if "oi" in df.columns else 0
    near_spot = df[abs(df["strike"] - spot) / spot < 0.02] if "strike" in df.columns else df
    near_oi = near_spot["oi"].sum() if "oi" in df.columns else 0
    squeeze_score = min((near_oi / total_oi * 500), 100) if total_oi > 0 else 0

    # --- Vanna Signal (15%) ---
    if "iv" in df.columns:
        atm_calls = calls[abs(calls["strike"] - spot) / spot < 0.03]
        atm_puts = puts[abs(puts["strike"] - spot) / spot < 0.03]
        avg_call_iv = atm_calls["iv"].mean() if len(atm_calls) > 0 else 0
        avg_put_iv = atm_puts["iv"].mean() if len(atm_puts) > 0 else 0
    else:
        avg_call_iv, avg_put_iv = 0, 0

    if avg_call_iv > 0 and avg_put_iv > 0:
        vanna_score = max(10, min(90, 50 + (1 - avg_call_iv / avg_put_iv) * 100))
    else:
        vanna_score = 50

    # --- IV Skew (10%) ---
    if avg_put_iv > 0:
        skew = avg_put_iv - avg_call_iv
        skew_score = max(10, min(90, 50 + (skew / avg_put_iv) * 200))
    else:
        skew_score = 50

    # --- Composite ---
    composite = (0.30 * gex_score + 0.25 * delta_score +
                 0.20 * squeeze_score + 0.15 * vanna_score + 0.10 * skew_score)

    if composite >= 65:
        signal, strength = "BULLISH", ("strong" if composite >= 75 else "moderate")
    elif composite <= 35:
        signal, strength = "BEARISH", ("strong" if composite <= 25 else "moderate")
    else:
        signal, strength = "NEUTRAL", ("weak" if abs(composite - 50) < 10 else "leaning")

    return {
        "composite": round(composite, 1),
        "signal": signal,
        "strength": strength,
        "components": {
            "gex": round(gex_score, 1),
            "delta_flow": round(delta_score, 1),
            "gamma_squeeze": round(squeeze_score, 1),
            "vanna": round(vanna_score, 1),
            "iv_skew": round(skew_score, 1),
        },
        "metadata": {
            "call_volume": int(call_vol),
            "put_volume": int(put_vol),
            "near_spot_oi_pct": round(near_oi / total_oi * 100, 1) if total_oi > 0 else 0,
            "avg_call_iv": round(avg_call_iv * 100, 1) if avg_call_iv else 0,
            "avg_put_iv": round(avg_put_iv * 100, 1) if avg_put_iv else 0,
        }
    }


def _neutral_score():
    return {
        "composite": 50.0, "signal": "NEUTRAL", "strength": "no_data",
        "components": {"gex": 50, "delta_flow": 50, "gamma_squeeze": 0, "vanna": 50, "iv_skew": 50},
        "metadata": {"call_volume": 0, "put_volume": 0, "near_spot_oi_pct": 0, "avg_call_iv": 0, "avg_put_iv": 0},
    }


# ============================================================
# STEP 3: FLASHALPHA VALIDATION
# ============================================================

def validate_with_flashalpha(ticker, our_profile):
    """Cross-check vs FlashAlpha (uses 1 of 5 free daily calls)."""
    if not FLASHALPHA_API_KEY:
        return {"status": "skipped", "reason": "No FLASHALPHA_API_KEY set"}

    try:
        resp = requests.get(
            f"https://lab.flashalpha.com/v1/exposure/gex/{ticker}",
            headers={"X-Api-Key": FLASHALPHA_API_KEY}, timeout=10
        )
        if resp.status_code == 429:
            return {"status": "rate_limited"}
        if resp.status_code != 200:
            return {"status": "error", "reason": f"HTTP {resp.status_code}"}

        fa = resp.json()
        our_flip = our_profile.get("gamma_flip")
        fa_flip = fa.get("gamma_flip")
        flip_diff = abs(our_flip - fa_flip) if (our_flip and fa_flip) else None
        regime_match = (our_profile.get("regime") == fa.get("regime", "").replace("_gamma", ""))

        return {
            "status": "ok",
            "flashalpha": {"net_gex": fa.get("net_gex"), "gamma_flip": fa_flip, "regime": fa.get("regime")},
            "comparison": {
                "gamma_flip_diff": flip_diff,
                "regime_match": regime_match,
                "flip_diff_pct": round(flip_diff / our_flip * 100, 2) if (flip_diff and our_flip) else None,
            }
        }
    except Exception as e:
        return {"status": "error", "reason": str(e)}


# ============================================================
# STEP 4: DATABASE STORAGE
# ============================================================

def init_db(db_path=DB_PATH):
    """Create GEX tables if they don't exist. NEVER drops existing tables."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS gex_levels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL, timestamp TEXT NOT NULL, spot REAL,
        net_gex REAL, gamma_flip REAL, call_wall REAL, put_wall REAL,
        king_strike REAL, king_value REAL, regime TEXT,
        composite_score REAL, signal TEXT, strength TEXT,
        gex_component REAL, delta_flow_component REAL,
        gamma_squeeze_component REAL, vanna_component REAL, iv_skew_component REAL,
        call_volume INTEGER, put_volume INTEGER, near_spot_oi_pct REAL,
        avg_call_iv REAL, avg_put_iv REAL,
        validation_status TEXT, validation_flip_diff REAL, validation_regime_match INTEGER,
        data_source TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS gex_strikes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL, timestamp TEXT NOT NULL,
        strike REAL, net_gex REAL, call_gex REAL, put_gex REAL,
        total_oi INTEGER, total_volume INTEGER
    )""")
    conn.commit()
    conn.close()


def store_gex_data(ticker, profile, score, validation=None, db_path=DB_PATH):
    """Save to trader.db. Sacred data rule: append only, never delete."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    now = datetime.now().isoformat()

    c.execute("""INSERT INTO gex_levels (
        ticker, timestamp, spot, net_gex, gamma_flip, call_wall, put_wall,
        king_strike, king_value, regime, composite_score, signal, strength,
        gex_component, delta_flow_component, gamma_squeeze_component,
        vanna_component, iv_skew_component,
        call_volume, put_volume, near_spot_oi_pct, avg_call_iv, avg_put_iv,
        validation_status, validation_flip_diff, validation_regime_match, data_source
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        ticker, now, profile["spot"], profile["net_gex"],
        profile["gamma_flip"], profile["call_wall"], profile["put_wall"],
        profile["king_strike"], profile["king_value"], profile["regime"],
        score["composite"], score["signal"], score["strength"],
        score["components"]["gex"], score["components"]["delta_flow"],
        score["components"]["gamma_squeeze"], score["components"]["vanna"],
        score["components"]["iv_skew"],
        score["metadata"]["call_volume"], score["metadata"]["put_volume"],
        score["metadata"]["near_spot_oi_pct"],
        score["metadata"]["avg_call_iv"], score["metadata"]["avg_put_iv"],
        validation.get("status") if validation else None,
        validation.get("comparison", {}).get("gamma_flip_diff") if validation else None,
        validation.get("comparison", {}).get("regime_match") if validation else None,
        profile.get("source", "unknown"),
    ))

    ps = profile.get("per_strike")
    if ps is not None and isinstance(ps, pd.DataFrame):
        for _, row in ps.iterrows():
            c.execute("""INSERT INTO gex_strikes
                (ticker, timestamp, strike, net_gex, call_gex, put_gex, total_oi, total_volume)
                VALUES (?,?,?,?,?,?,?,?)""", (
                ticker, now, row.get("strike"), row.get("net_gex"),
                row.get("call_gex", 0), row.get("put_gex", 0),
                int(row.get("total_oi", 0)), int(row.get("total_volume", 0)),
            ))

    conn.commit()
    conn.close()


# ============================================================
# STEP 5: AGENT INTEGRATION HELPERS
# ============================================================

def get_latest_gex_for_uhura(ticker="SPY", db_path=DB_PATH):
    """Pull latest GEX for Lt. Uhura's confluence filter."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""SELECT ticker, spot, net_gex, gamma_flip, call_wall, put_wall,
                        regime, composite_score, signal, strength,
                        king_strike, king_value, timestamp
                 FROM gex_levels WHERE ticker = ?
                 ORDER BY timestamp DESC LIMIT 1""", (ticker,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None

    return {
        "ticker": row[0], "spot": row[1], "net_gex": row[2],
        "gamma_flip": row[3], "call_wall": row[4], "put_wall": row[5],
        "regime": row[6], "composite_score": row[7], "signal": row[8],
        "strength": row[9], "king_strike": row[10], "king_value": row[11],
        "timestamp": row[12],
        "above_flip": row[1] > row[3] if row[3] else None,
        "distance_to_flip_pct": round(abs(row[1] - row[3]) / row[1] * 100, 2) if row[3] else None,
        "distance_to_call_wall": round(row[4] - row[1], 2) if row[4] else None,
        "distance_to_put_wall": round(row[1] - row[5], 2) if row[5] else None,
    }


def get_gex_for_debate(ticker="SPY", db_path=DB_PATH):
    """Format GEX for Riker/Worf/Picard debate pipeline."""
    gex = get_latest_gex_for_uhura(ticker, db_path)
    if not gex:
        return None

    bull, bear = [], []
    if gex["regime"] == "positive":
        bull.append(f"Positive gamma — dealers dampen volatility, support dip-buying")
        bear.append(f"Positive gamma caps upside at call wall ${gex['call_wall']}")
    else:
        bear.append(f"Negative gamma — dealer hedging amplifies moves both directions")
        bull.append(f"Negative gamma = explosive upside potential if momentum flips")

    if gex["above_flip"]:
        bull.append(f"Price ${gex['spot']:.2f} above gamma flip ${gex['gamma_flip']:.2f} — stabilizing zone")
    else:
        bear.append(f"Price ${gex['spot']:.2f} below gamma flip ${gex['gamma_flip']:.2f} — volatile zone")

    if gex["composite_score"] >= 60:
        bull.append(f"Composite score {gex['composite_score']}/100 — {gex['signal']}")
    elif gex["composite_score"] <= 40:
        bear.append(f"Composite score {gex['composite_score']}/100 — {gex['signal']}")

    return {
        "riker_bull_ammo": bull,
        "worf_bear_ammo": bear,
        "picard_context": {
            "regime": gex["regime"], "composite": gex["composite_score"],
            "signal": gex["signal"],
            "key_levels": {"gamma_flip": gex["gamma_flip"], "call_wall": gex["call_wall"],
                           "put_wall": gex["put_wall"], "king_strike": gex["king_strike"]},
        }
    }


def quick_gex_query(ticker="SPY", db_path=DB_PATH):
    """
    Ship's Computer quick query — returns plain-English summary.
    Checks local DB first, falls back to FlashAlpha API.
    """
    # Try local data first (free, no API call)
    gex = get_latest_gex_for_uhura(ticker, db_path)
    if gex:
        age_mins = 0
        try:
            ts = datetime.fromisoformat(gex["timestamp"])
            age_mins = (datetime.now() - ts).total_seconds() / 60
        except Exception:
            pass

        stale = " (stale — data is {:.0f}min old)".format(age_mins) if age_mins > 60 else ""
        regime_desc = ("positive (dealers dampen moves)" if gex["regime"] == "positive"
                       else "negative (dealers amplify moves)")

        lines = [
            f"GEX Report: {ticker} — {gex['timestamp'][:16]}{stale}",
            f"  Spot:        ${gex['spot']:.2f}",
            f"  Net GEX:     ${gex['net_gex']:,.0f}",
            f"  Regime:      {regime_desc}",
            f"  Gamma Flip:  ${gex['gamma_flip']:.2f}",
            f"  Call Wall:   ${gex['call_wall']:.2f}" if gex["call_wall"] else None,
            f"  Put Wall:    ${gex['put_wall']:.2f}" if gex["put_wall"] else None,
            f"  Score:       {gex['composite_score']}/100 → {gex['signal']} ({gex['strength']})",
            f"  Above Flip:  {'YES — calm waters' if gex['above_flip'] else 'NO — choppy seas'}",
        ]
        return "\n".join(l for l in lines if l)

    # Fallback: FlashAlpha API
    if FLASHALPHA_API_KEY:
        try:
            resp = requests.get(
                f"https://lab.flashalpha.com/v1/exposure/gex/{ticker}",
                headers={"X-Api-Key": FLASHALPHA_API_KEY}, timeout=10
            )
            if resp.status_code == 200:
                fa = resp.json()
                regime = fa.get("regime", "unknown")
                return (f"GEX Report: {ticker} (via FlashAlpha)\n"
                        f"  Net GEX:     ${fa.get('net_gex', 0):,.0f}\n"
                        f"  Gamma Flip:  ${fa.get('gamma_flip', 'N/A')}\n"
                        f"  Regime:      {regime}")
        except Exception:
            pass

    return f"No GEX data available for {ticker}. Run: python engine/gex_engine.py --ticker {ticker}"


# ============================================================
# STEP 6: MAIN PIPELINE
# ============================================================

def scan_ticker(ticker, adapter, validate=False, store=True, verbose=True):
    """Full scan pipeline for one ticker."""
    if verbose:
        print(f"\n{'='*60}")
        print(f"  GEX SCAN: {ticker}")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

    # Fetch
    if verbose:
        print(f"  [1/5] Fetching data ({adapter.source})...")
    profile = adapter.fetch(ticker)
    if not profile:
        print(f"  [!] No data for {ticker}")
        return None, None, None

    spot = profile["spot"]
    if verbose:
        print(f"        Spot: ${spot:.2f} | Source: {profile.get('source', '?')}")
        print(f"        Net GEX: ${profile['net_gex']/1e6:,.1f}M | Regime: {profile['regime'].upper()}")
        print(f"        Gamma Flip: ${profile['gamma_flip']:.2f}")
        if profile['call_wall']:
            print(f"        Call Wall:  ${profile['call_wall']:.2f}")
        if profile['put_wall']:
            print(f"        Put Wall:   ${profile['put_wall']:.2f}")
        if profile['king_strike']:
            print(f"        King Strike: ${profile['king_strike']:.2f} (${profile['king_value']/1e6:,.1f}M)")

    # Score
    if verbose:
        print(f"  [2/5] Computing composite score...")
    raw = profile.get("raw_chain")
    score = calculate_composite_score(raw, profile, spot)
    if verbose:
        print(f"        Score: {score['composite']}/100 → {score['signal']} ({score['strength']})")
        c = score["components"]
        print(f"        GEX={c['gex']:.0f}  Delta={c['delta_flow']:.0f}  "
              f"Squeeze={c['gamma_squeeze']:.0f}  Vanna={c['vanna']:.0f}  Skew={c['iv_skew']:.0f}")

    # Validate
    validation = None
    if validate:
        if verbose:
            print(f"  [3/5] Validating vs FlashAlpha...")
        validation = validate_with_flashalpha(ticker, profile)
        if verbose and validation["status"] == "ok":
            vc = validation["comparison"]
            print(f"        FA flip: ${validation['flashalpha']['gamma_flip']}  "
                  f"Diff: ${vc['gamma_flip_diff']:.2f}  Match: {'YES' if vc['regime_match'] else 'NO'}")
        elif verbose:
            print(f"        {validation['status']}: {validation.get('reason', '')}")
    else:
        if verbose:
            print(f"  [3/5] Validation skipped (use --validate)")

    # Store
    if store:
        if verbose:
            print(f"  [4/5] Storing to trader.db...")
        try:
            store_gex_data(ticker, profile, score, validation)
            if verbose:
                print(f"        Saved ✓")
        except Exception as e:
            print(f"  [!] DB error: {e}")

    if verbose:
        print(f"  [5/5] Done ✓")

    return profile, score, validation


def run_morning_scan(tickers=None, validate=False):
    """Morning pre-market GEX scan."""
    tickers = tickers or CORE_TICKERS
    print("\n" + "=" * 60)
    print("  TRADEMINDS GEX ENGINE v2 — MORNING SCAN")
    print(f"  {datetime.now().strftime('%A %Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    init_db()
    adapter = GEXDataAdapter()

    results = {}
    for ticker in tickers:
        profile, score, val = scan_ticker(ticker, adapter, validate=validate)
        if profile:
            results[ticker] = {"gex": profile, "score": score, "validation": val}

    # Summary
    print(f"\n{'='*60}")
    print("  SCAN SUMMARY")
    print(f"{'='*60}")
    print(f"  {'Ticker':<8} {'Spot':>10} {'Net GEX':>12} {'Regime':<10} {'Score':>6} {'Signal':<10}")
    print("  " + "-" * 58)
    for t, d in results.items():
        g, s = d["gex"], d["score"]
        print(f"  {t:<8} ${g['spot']:>8.2f} ${g['net_gex']/1e6:>8.1f}M "
              f"{g['regime']:<10} {s['composite']:>5.1f} {s['signal']:<10}")
    print("=" * 60)
    return results


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TradeMinds GEX Engine v2")
    parser.add_argument("--ticker", "-t", type=str, help="Single ticker to scan")
    parser.add_argument("--validate", "-v", action="store_true", help="Cross-check vs FlashAlpha")
    parser.add_argument("--scan", "-s", action="store_true", help="Scan core tickers")
    parser.add_argument("--query", "-q", type=str, help="Ship's Computer quick query")
    parser.add_argument("--no-store", action="store_true", help="Skip DB storage")
    parser.add_argument("--db", type=str, help="Override database path")

    args = parser.parse_args()
    if args.db:
        DB_PATH = args.db

    if args.query:
        print(quick_gex_query(args.query))
    elif args.ticker:
        if not args.no_store:
            init_db()
        adapter = GEXDataAdapter()
        scan_ticker(args.ticker, adapter, validate=args.validate, store=not args.no_store)
    else:
        run_morning_scan(validate=args.validate)
