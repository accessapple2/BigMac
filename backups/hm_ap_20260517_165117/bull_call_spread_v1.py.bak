"""
Bull Call Spread v1 — adaptive options execution path for bull-side signals.

Symmetric counterpart to bear_put_spread_v1. 14 DTE target, $750 cap.
Paper gate: 15 trades + positive expectancy before enabling live execution.

IV-rank-driven structure (inverted from bear):
  IV rank < 30  → bull_call_spread (debit: buy ATM call, sell OTM call)
  IV rank 30-60 → dead zone, no entry
  IV rank > 60  → bull_put_spread  (credit: sell OTM put, buy further OTM put)

Signal consumer (tiered):
  Tier 1 (high): BMB bar trigger + TB tiebreaker active → fire alone
  Tier 2 (low):  Kirk/BUY vote + TB active + P/C < 0.7 → all three required

Regime gate:
  Allow: BULL, BULL_STRONG, NEUTRAL, CAUTIOUS
  Block: BEAR, BEAR_STRONG, CRISIS, HIGH_VOL, UNKNOWN

Dedup:
  - 24h within-strategy symbol dedup
  - Skip if bull_spread_v1 has open position on same symbol

Strategy ID: "bull_call_spread_v1"
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from engine.halt_gate import HALTED_EMIT_FILTER

from .base import Strategy, MarketContext, StrategySignal
from .iv_rank import get_iv_rank
from .chain_lookup import get_spread_quote
from .mock_data import is_mock_mode, mock_spot_price, SpreadQuote


# ── Constants ──────────────────────────────────────────────────────────────────

STRATEGY_ID = "bull_call_spread_v1"

IV_LOW  = 30.0   # below → debit call spread
IV_HIGH = 60.0   # above → credit put spread

DTE_TARGET = 14

WIDTHS = [10.0, 5.0, 3.0]

MAX_RISK_USD: float = 750.0

# Quality gates
DEBIT_MAX_COST_RATIO = 0.40
DEBIT_MIN_RR         = 1.5
CREDIT_MIN_RATIO     = 0.25
CREDIT_MIN_RR        = 1.3

TB_CONF_THRESHOLD = 85
TB_LOOKBACK_HOURS = 6

_last_signal_ts: dict[str, datetime] = {}

_EXECUTION_ENABLED: bool = True

TIER_1 = ["SPY", "QQQ", "IWM"]
TIER_2 = ["AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "TSLA"]

_ROOT       = Path(__file__).resolve().parent.parent
_DB_PATH    = _ROOT / "data" / "trader.db"
_SC_DB_PATH = _ROOT / "signal-center" / "signals.db"

# Regimes that block bull entries
_BLOCK_REGIMES = {"BEAR", "BEAR_STRONG", "CRISIS", "HIGH_VOL", "UNKNOWN"}


# ── Data helpers ───────────────────────────────────────────────────────────────

def _get_spot(ticker: str) -> Optional[float]:
    if is_mock_mode():
        return mock_spot_price(ticker)
    try:
        from .alpaca_chain_client import _fetch_spot
        return _fetch_spot(ticker)
    except Exception as e:
        print(f"[bull_call_spread_v1] spot lookup failed for {ticker}: {e}")
        return None


def _check_bmb(ticker: str) -> bool:
    """
    Bull Momentum Breakout check — same criteria as the live BMB rule.
    price > 20d high  AND  vol_ratio > 1.5  AND  50 ≤ RSI ≤ 70  AND  ADX > 25
    """
    if is_mock_mode():
        return True

    try:
        import numpy as np
        import yfinance as yf
        df = yf.download(ticker, period="60d", progress=False, auto_adjust=True)
        if df is None or len(df) < 22:
            return False

        closes  = df["Close"].values.flatten().astype(float)
        highs   = df["High"].values.flatten().astype(float)
        lows    = df["Low"].values.flatten().astype(float)
        volumes = df["Volume"].values.flatten().astype(float)

        price   = float(closes[-1])
        high_20 = float(np.max(highs[-21:-1]))

        vol_avg   = float(np.mean(volumes[-21:-1])) if len(volumes) >= 21 else 1.0
        vol_ratio = float(volumes[-1]) / vol_avg if vol_avg > 0 else 1.0

        def _rsi(c: "np.ndarray", period: int = 14) -> float:
            if len(c) < period + 1:
                return 50.0
            deltas = np.diff(c[-period - 1:].astype(float))
            gains  = np.where(deltas > 0, deltas, 0.0)
            losses = np.where(deltas < 0, -deltas, 0.0)
            avg_g  = float(np.mean(gains))
            avg_l  = float(np.mean(losses))
            return 100.0 - 100.0 / (1.0 + avg_g / avg_l) if avg_l > 0 else 100.0

        rsi = _rsi(closes)

        def _adx(h: "np.ndarray", lo: "np.ndarray", c: "np.ndarray", period: int = 14) -> float:
            if len(c) < period * 2 + 2:
                return 0.0
            tr  = np.maximum(h[1:] - lo[1:],
                  np.maximum(np.abs(h[1:] - c[:-1]), np.abs(lo[1:] - c[:-1])))
            pdm = np.where((h[1:] - h[:-1]) > (lo[:-1] - lo[1:]),
                           np.maximum(h[1:] - h[:-1], 0.0), 0.0)
            mdm = np.where((lo[:-1] - lo[1:]) > (h[1:] - h[:-1]),
                           np.maximum(lo[:-1] - lo[1:], 0.0), 0.0)

            def wilder(arr: "np.ndarray") -> "np.ndarray":
                out = np.zeros(len(arr))
                out[period - 1] = arr[:period].sum()
                for i in range(period, len(arr)):
                    out[i] = out[i - 1] - out[i - 1] / period + arr[i]
                return out

            atr_s = wilder(tr); pdm_s = wilder(pdm); mdm_s = wilder(mdm)
            safe  = np.where(atr_s > 0, atr_s, 1.0)
            pdi   = np.where(atr_s > 0, 100.0 * pdm_s / safe, 0.0)
            mdi   = np.where(atr_s > 0, 100.0 * mdm_s / safe, 0.0)
            di_sum = pdi + mdi
            dx_arr = np.where(di_sum > 0, 100.0 * np.abs(pdi - mdi) / np.where(di_sum > 0, di_sum, 1.0), 0.0)
            dx_sl  = dx_arr[period - 1:]
            if len(dx_sl) < period:
                return 0.0
            adx_out = np.zeros(len(dx_sl))
            adx_out[period - 1] = float(np.mean(dx_sl[:period]))
            for i in range(period, len(dx_sl)):
                adx_out[i] = (adx_out[i - 1] * (period - 1) + dx_sl[i]) / period
            return float(adx_out[-1])

        adx = _adx(highs, lows, closes)

        return (price > high_20 and vol_ratio > 1.5
                and 50.0 <= rsi <= 70.0 and adx > 25.0)

    except Exception as e:
        print(f"[bull_call_spread_v1] BMB check error for {ticker}: {e}")
        return False


def _get_tb_active(ticker: str) -> bool:
    """True if Tractor Beam has a recent high-confidence signal for ticker."""
    if is_mock_mode():
        return True
    if not _SC_DB_PATH.exists():
        return False
    try:
        conn = sqlite3.connect(str(_SC_DB_PATH), timeout=3)
        row = conn.execute(
            "SELECT confidence FROM trade_signals "
            "WHERE agent_name='tractor-beam' AND symbol=? AND confidence >= ? "
            "AND created_at >= datetime('now', ?) "
            "ORDER BY confidence DESC LIMIT 1",
            (ticker, TB_CONF_THRESHOLD, f"-{TB_LOOKBACK_HOURS} hours"),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception as e:
        print(f"[bull_call_spread_v1] TB check error for {ticker}: {e}")
        return False


def _get_pc_ratio(ticker: str) -> float:
    """
    Market P/C ratio: options_flow_history → CBOE CSV → IV-skew proxy.
    Returns the ratio (lower = more bullish call buying).
    """
    if is_mock_mode():
        return 0.6  # bullish mock

    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=3)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT put_call_ratio FROM options_flow_history "
            "WHERE created_at >= datetime('now', '-24 hours') "
            "ORDER BY created_at DESC LIMIT 1",
        ).fetchone()
        conn.close()
        if row and row["put_call_ratio"] is not None:
            return float(row["put_call_ratio"])
    except Exception as e:
        print(f"[bull_call_spread_v1] PCR DB error: {e}")

    try:
        import requests
        resp = requests.get(
            "https://cdn.cboe.com/data/us/options/market_statistics/daily_pcr.csv",
            timeout=6,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if resp.status_code == 200:
            lines = [l for l in resp.text.strip().splitlines() if l.strip()]
            if len(lines) >= 2:
                val = float(lines[-1].split(",")[1].strip())
                if 0.1 <= val <= 5.0:
                    return val
    except Exception:
        pass

    # IV-skew proxy: low IV rank → call-heavy (bullish) market
    try:
        snap = get_iv_rank(ticker, record=False)
        if snap and snap.iv_rank < 40:
            return 0.65  # low IV = call bias = bullish
        return 1.05      # elevated IV = put bias = bearish
    except Exception:
        return 1.05


def _check_tier2_buy_signal(ticker: str) -> bool:
    """
    True if any AI-player BUY/BUY_CALL/bull vote landed for this ticker in
    the last 4 hours. Reads the signals table (replaces legacy strategy_signals
    source which has been empty since 2026-04-15 — see Round 2 NEW-2 investigation).
    """
    if is_mock_mode():
        return True
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=3)
        conn.row_factory = sqlite3.Row
        # HM-C: filter halted-player emissions from scorecard/calibration math
        # (decisional: halted player BUYs must NOT trigger tier-2 spread entry)
        row = conn.execute(
            f"SELECT id FROM signals "
            f"WHERE symbol=? "
            f"AND (UPPER(signal) LIKE '%BUY%' OR UPPER(signal) LIKE '%CALL%' "
            f"     OR UPPER(signal) LIKE '%BULL%') "
            f"AND created_at >= datetime('now', '-4 hours') "
            f"AND {HALTED_EMIT_FILTER} "
            f"LIMIT 1",
            (ticker,),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception as e:
        print(f"[bull_call_spread_v1] tier2 signal check error for {ticker}: {e}")
        return False


def _bull_spread_v1_has_position(ticker: str) -> bool:
    """
    True if bull_spread_v1 has an open position on this ticker.
    Prevents stacking two bull credit spreads on the same symbol.
    """
    if is_mock_mode():
        return False
    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=3)
        row = conn.execute(
            "SELECT id FROM options_trades "
            "WHERE strategy_id='bull_spread_v1' AND symbol=? AND exec_status='open' "
            "LIMIT 1",
            (ticker,),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception as e:
        print(f"[bull_call_spread_v1] bull_spread_v1 dedup check error: {e}")
        return False


def _is_dedup_blocked(ticker: str) -> bool:
    last = _last_signal_ts.get(ticker)
    if last is None:
        return False
    return (datetime.now(timezone.utc) - last) < timedelta(hours=24)


def _mark_fired(ticker: str) -> None:
    _last_signal_ts[ticker] = datetime.now(timezone.utc)


# ── Width selection ────────────────────────────────────────────────────────────

def _select_bull_call_spread_width(
    ticker: str,
    structure: str,
    dte: int,
    spot: float,
    max_risk: float,
) -> tuple[float | None, SpreadQuote | None]:
    """
    Select widest width satisfying MAX_RISK_USD and quality gates.

    Quality gates:
      bull_call_spread (debit):  net_debit/width ≤ 0.40  AND  R/R ≥ 1.5
      bull_put_spread  (credit): net_credit/width ≥ 0.25 AND  R/R ≥ 1.3

    Returns (0.0, None) if no width satisfies; (None, None) on structure skip.
    """
    for width in WIDTHS:
        quote = get_spread_quote(ticker, structure, dte, width)
        if quote is None or quote.max_loss <= 0:
            continue

        if quote.max_loss > max_risk:
            continue

        if structure == "bull_call_spread":
            ratio  = quote.net_debit / width
            rr     = quote.max_profit / quote.max_loss
            passes = ratio <= DEBIT_MAX_COST_RATIO and rr >= DEBIT_MIN_RR
            quality = f"cost/width={ratio*100:.1f}% R/R={rr:.2f}x"
        else:  # bull_put_spread (credit)
            ratio  = quote.net_credit / width
            rr     = quote.max_profit / quote.max_loss
            passes = ratio >= CREDIT_MIN_RATIO and rr >= CREDIT_MIN_RR
            quality = f"credit/width={ratio*100:.1f}% R/R={rr:.2f}x"

        if passes:
            print(
                f"[bull_call_spread_v1] {ticker} {structure} dte={dte}: "
                f"width=${width:.0f} max_loss=${quote.max_loss:.0f} "
                f"(cap=${max_risk:.0f}) {quality}"
            )
            return width, quote

    print(
        f"[bull_call_spread_v1] no width fits risk_cap=${max_risk:.0f} "
        f"for {ticker} {structure} dte={dte}"
    )
    return 0.0, None


# ── Strategy class ─────────────────────────────────────────────────────────────

class BullCallSpreadV1(Strategy):
    strategy_id     = STRATEGY_ID
    display_name    = "Bull Call Spread v1"
    enabled_default = False
    description = (
        "Adaptive bull spread: debit call (IV<30) or credit put (IV>60). "
        "14 DTE, $750 cap. Tier-1: BMB+TB. "
        "Tier-2: Kirk/BUY+TB+P/C<0.7. "
        "Skips symbol if bull_spread_v1 has open position. "
        "Paper gate: 15 trades + positive expectancy."
    )

    def evaluate(self, ctx: MarketContext) -> list[StrategySignal]:
        # Block in bear / crisis regimes
        if ctx.regime in _BLOCK_REGIMES:
            return []

        signals: list[StrategySignal] = []
        universe = TIER_1 + TIER_2

        for ticker in universe:
            # ── Dedup checks ──────────────────────────────────────────────
            if _is_dedup_blocked(ticker):
                continue
            if _bull_spread_v1_has_position(ticker):
                print(f"[bull_call_spread_v1] {ticker}: bull_spread_v1 open — skip")
                continue

            # ── Tier detection ────────────────────────────────────────────
            bmb_fired = _check_bmb(ticker)
            tb_active = _get_tb_active(ticker)

            if bmb_fired and tb_active:
                tier = 1
            else:
                buy_signal = _check_tier2_buy_signal(ticker)
                if not (buy_signal and tb_active):
                    continue
                pc_ratio = _get_pc_ratio(ticker)
                if pc_ratio >= 0.7:
                    continue
                tier = 2

            # ── IV rank → structure ───────────────────────────────────────
            iv_result = get_iv_rank(ticker, record=True)
            if iv_result is None:
                continue
            iv_rank = iv_result.iv_rank

            if iv_rank < IV_LOW:
                structure = "bull_call_spread"
            elif iv_rank > IV_HIGH:
                structure = "bull_put_spread"
            else:
                print(f"[bull_call_spread_v1] {ticker}: IV rank {iv_rank:.1f} dead zone — skip")
                continue

            # ── Spot ──────────────────────────────────────────────────────
            spot = iv_result.spot if iv_result.spot else _get_spot(ticker)
            if spot is None or spot <= 0:
                continue

            # ── Width selection ───────────────────────────────────────────
            width, quote = _select_bull_call_spread_width(
                ticker, structure, DTE_TARGET, spot, MAX_RISK_USD
            )
            if quote is None:
                continue

            # ── Confidence ────────────────────────────────────────────────
            if structure == "bull_call_spread":
                confidence = min(1.0, (IV_LOW - iv_rank) / IV_LOW) if iv_rank < IV_LOW else 0.1
            else:
                confidence = min(1.0, (iv_rank - IV_HIGH) / (100.0 - IV_HIGH))
            if tier == 1:
                confidence = min(1.0, confidence + 0.15)

            # ── Signal ────────────────────────────────────────────────────
            reasoning = (
                f"{ticker}: IV rank {iv_rank:.1f} → {structure} tier={tier} "
                f"(regime={ctx.regime}, DTE={DTE_TARGET}, width=${width:.0f}, "
                f"max_loss=${quote.max_loss:.0f})"
            )
            payload = {
                "structure":               structure,
                "dte":                     DTE_TARGET,
                "iv_rank":                 iv_rank,
                "spot_at_signal":          spot,
                "tier":                    tier,
                "execution_enabled":       _EXECUTION_ENABLED,
                "long_leg": {
                    "action":      quote.long_leg.action,
                    "option_type": quote.long_leg.option_type,
                    "strike":      quote.long_leg.strike,
                    "expiration":  quote.long_leg.expiration,
                    "premium":     quote.long_leg.premium,
                },
                "short_leg": {
                    "action":      quote.short_leg.action,
                    "option_type": quote.short_leg.option_type,
                    "strike":      quote.short_leg.strike,
                    "expiration":  quote.short_leg.expiration,
                    "premium":     quote.short_leg.premium,
                },
                "net_debit":               quote.net_debit,
                "net_credit":              quote.net_credit,
                "max_profit_per_contract": quote.max_profit,
                "max_loss_per_contract":   quote.max_loss,
                "width":                   width,
            }

            sig = StrategySignal(
                strategy_id=self.strategy_id,
                ticker=ticker,
                action="open",
                asset_type="spread",
                direction="bull",
                max_risk_usd=quote.max_loss,
                confidence=confidence,
                exit_tag="bullcallspread-textbook",
                payload=payload,
                reasoning=reasoning,
            )
            signals.append(sig)
            _mark_fired(ticker)

            # ── ntfy ──────────────────────────────────────────────────────
            try:
                from engine.ntfy import notify_bull_spread
                notify_bull_spread(
                    symbol=ticker,
                    structure=structure,
                    iv_rank=iv_rank,
                    long_strike=quote.long_leg.strike,
                    short_strike=quote.short_leg.strike,
                    expiration=quote.long_leg.expiration,
                    net_cost=quote.net_debit if structure == "bull_call_spread" else quote.net_credit,
                    max_loss=quote.max_loss,
                    max_profit=quote.max_profit,
                    tier=tier,
                    spot=spot,
                    regime=ctx.regime,
                )
            except Exception as _ntfy_err:
                print(f"[bull_call_spread_v1] ntfy error: {_ntfy_err}")

        return signals


# ── Auto-register at import ────────────────────────────────────────────────────
try:
    from .registry import registry as _registry
    _registry().register(BullCallSpreadV1(enabled=True))
except Exception as _reg_err:
    print(f"[bull_call_spread_v1] auto-registration skipped: {_reg_err}")
