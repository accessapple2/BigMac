"""
Bull Spread v1 — OllieTrades Strategy Registry first strategy.

Spec locked 2026-04-22:
  - Debit call (IV rank < 40) OR credit put (IV rank >= 40)
  - Universe: Tier 1 (SPY QQQ IWM) + Tier 2 (AAPL MSFT NVDA META GOOGL AMZN TSLA)
  - DTE: momentum 0-5, pullback 10-21, neutral 10
  - Size: $200-500 risk per leg
  - A/B exit test: textbook (2 ct, 50/50/1DTE) + scaleout (4 ct, 50%/75%/runner)
  - Paper only. Real-money gate: 30 trades + positive expectancy.

Task 5: emits real StrategySignal objects. No execution (Task 6).
"""
from __future__ import annotations
import sqlite3                              # HM-AB: for _already_open self-skip check
from pathlib import Path                    # HM-AB: for _DB_PATH constant
from .base import Strategy, MarketContext, StrategySignal
from .iv_rank import get_iv_rank
from .setup_classifier import classify, dte_for_setup
from .chain_lookup import get_spread_quote, select_width
from .mock_data import is_mock_mode, mock_spot_price, SpreadQuote

# HM-AB (2026-05-05): DB path for self-symbol-skip dedup. Mirrors
# strategies/bull_call_spread_v1.py:71 convention.
_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"


def _get_spot(ticker: str):
    """Route spot-price lookup to mock or Polygon."""
    if is_mock_mode():
        return mock_spot_price(ticker)
    try:
        from .polygon_client import fetch_spot_price
        return fetch_spot_price(ticker)
    except Exception as e:
        print(f"[bull_spread_v1] spot lookup failed for {ticker}: {e}")
        return None


# HM-AB (2026-05-05): self-symbol-skip dedup. Reciprocal of
# strategies/bull_call_spread_v1.py:_bull_spread_v1_has_position
# (lines 271-291). Closes the gap that allowed 19 SPY bull_put_spreads
# to stack onto themselves (rows 14-32 in options_trades) before the
# halt at commit 44c80c2. Module-level function mirroring sibling's
# style (own connection, bare except, print log).
def _already_open(ticker: str) -> bool:
    """True if bull_spread_v1 has an open position on this ticker.

    Prevents stacking bull_spread_v1 entries on the same symbol — the
    reciprocal check that bull_call_spread_v1 already has against
    bull_spread_v1 (one-way → now both-ways).
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
        print(f"[bull_spread_v1] self-symbol-skip check error for {ticker}: {e}")
        return False


TIER_1 = ["SPY", "QQQ", "IWM"]
TIER_2 = ["AAPL", "MSFT", "NVDA", "META", "GOOGL", "AMZN", "TSLA"]

IV_THRESHOLD = 40.0
EXIT_A_TAG = "bullspread-textbook"
EXIT_A_CONTRACTS = 2
EXIT_B_TAG = "bullspread-scaleout"
EXIT_B_CONTRACTS = 4

# ═══════════════════════════════════════════════════════════════════════
# HALT-2026-05-05 → HM-AB-unhalted-2026-05-05
# Strategy accumulated 19 open SPY bull_put_spreads + 4 ghost-failed rows
# in <1 day post-gate-flip because it lacked a same-strategy self-skip
# check (reciprocal of bull_call_spread_v1.py:280-287's dedup against
# bull_spread_v1). Halted at commit 44c80c2 to stop further stacking.
#
# The 19 existing positions ride per Admiral directive — they're real
# Alpaca paper positions, max-loss-capped, same-expiration. exit_manager
# handles them via HM-AC-Option-B's MLEG close path (commit 19c6746)
# on TP / SL / expiration cadence.
#
# HM-AB (2026-05-05): self-symbol-skip helper added (_already_open above)
# closes the stacking gap. Both halt gates now flipped to True; the
# strategy resumes firing — but cannot stack on its own open positions
# (smoke-verified: _already_open('SPY')=True blocks SPY re-entry).
_EXECUTION_ENABLED: bool = True   # HM-AB-unhalted-2026-05-05: was False (44c80c2 halt)

# ═══════════════════════════════════════════════════════════════════════
# FIRST TRADE MODE — controlled rollout controls
# Flip FIRST_TRADE_MODE = False only AFTER several successful trades.
# Admiral decision 2026-04-22: keep tight controls for initial cycles.
# ═══════════════════════════════════════════════════════════════════════
FIRST_TRADE_MODE: bool = True

# Universe override when FIRST_TRADE_MODE is True
FIRST_TRADE_UNIVERSE = ["SPY"]

# Max contracts per position when FIRST_TRADE_MODE is True
FIRST_TRADE_MAX_CONTRACTS: int = 1

# Max dollars at risk per position (1 contract) when FIRST_TRADE_MODE is True.
# Matches spec upper end ($200-500); $200 floor is too tight for ATM SPY spreads.
FIRST_TRADE_MAX_RISK: float = 500.0

# Exit tags allowed when FIRST_TRADE_MODE is True
FIRST_TRADE_ALLOWED_TAGS = {"bullspread-textbook"}
# ═══════════════════════════════════════════════════════════════════════


def _select_first_trade_width(
    ticker: str,
    structure: str,
    dte: int,
    spot: float,
    max_risk: float,
) -> tuple[float | None, SpreadQuote | None]:
    """Select the widest width that satisfies FIRST_TRADE_MAX_RISK and quality gates.

    Iterates wide -> narrow; returns (width, quote) for the FIRST candidate that
    passes BOTH:
      - max_loss <= max_risk  (credit: max_loss = width - credit; debit: max_loss = debit)
      - credit/width >= 0.25  for bull_put_spread  (credit quality gate)
      - R/R     >= 1.3x       for bull_call_spread (reward-to-risk gate)

    FIRST_TRADE_MODE: credit put spreads use caller DTE; debit call spreads
    override DTE to 21 (Gate 2 fix — R/R infeasible at 10 DTE within $500 cap).
    # Fixed 2026-04-24: expanded DTE to 21 for debit structures (see Gate 2 investigation —
    #   R/R does not work at ATM 10 DTE within the $500 risk cap; 21 DTE resolves this).

    Width ladder [15, 10, 5] is hardcoded for SPY-range tickers (~$700).
    # TODO: scale as [round(spot*0.02/5)*5, round(spot*0.014/5)*5, round(spot*0.007/5)*5]
    #       when FIRST_TRADE_UNIVERSE expands beyond SPY.

    API budget: up to 3 widths × 3 Alpaca calls each (spot + contracts + snapshots)
    = up to 9 calls per evaluate cycle; Alpaca paper 200 req/min is safe.

    Returns (None, None) for skipped structures; (0.0, None) if no width satisfies gates.
    Caller's `if quote is None: continue` handles both cases correctly.
    """
    # Gate 2 fix 2026-04-24: debit call spreads (bull_call_spread) need 21 DTE to
    # achieve R/R >= 1.3 within FIRST_TRADE_MAX_RISK=$500. Caller-supplied dte is
    # overridden here, before the width-ladder runs. Credit put spreads
    # (bull_put_spread) fall through to the width-ladder with caller dte unchanged.
    if structure == "bull_call_spread":
        dte = 21
        print(f"[first_trade_width] bull_call_spread DTE override -> 21 (Gate 2 fix 2026-04-24)")

    WIDTHS = [15.0, 10.0, 5.0]

    for width in WIDTHS:
        quote = get_spread_quote(ticker, structure, dte, width)
        if quote is None or quote.max_loss <= 0:
            continue

        # Gate 1: risk cap
        if quote.max_loss > max_risk:
            continue

        # Gate 2: quality threshold
        if structure == "bull_put_spread":
            ratio = quote.net_credit / width
            passes = ratio >= 0.25
            quality_str = f"credit/width={ratio*100:.1f}%"
        else:
            rr = quote.max_profit / quote.max_loss
            passes = rr >= 1.3
            quality_str = f"R/R={rr:.2f}x"

        if passes:
            print(
                f"[first_trade_width] {ticker} {structure} dte={dte}: "
                f"selected width=${width:.0f}  max_loss=${quote.max_loss:.0f}"
                f"  (cap=${max_risk:.0f})  {quality_str}"
            )
            return width, quote

    # Refinement 3: no width fit — log and signal skip
    print(
        f"[first_trade_width] no width fits risk_cap=${max_risk:.0f} "
        f"for {ticker} {structure} dte={dte}"
    )
    return 0.0, None


class BullSpreadV1(Strategy):
    strategy_id = "bull_spread_v1"
    display_name = "Bull Spread v1"
    enabled_default = False
    description = (
        "Debit call OR credit put spreads by IV rank. "
        "Mixed DTE by setup. Parallel A/B exit test. "
        "Paper only. Real-money gate: 30 trades + positive expectancy."
    )

    def evaluate(self, ctx: MarketContext) -> list[StrategySignal]:
        # HALT-2026-05-05: belt-and-braces gate — defense-in-depth alongside
        # the registry-level enabled=False below. Either path alone halts
        # signal emission; both together provide redundant safety.
        if not _EXECUTION_ENABLED:
            return []

        # Only trade in BULL regime per spec
        if ctx.regime != "BULL":
            return []

        signals: list[StrategySignal] = []

        # Apply FIRST_TRADE_MODE narrowing
        if FIRST_TRADE_MODE:
            universe = FIRST_TRADE_UNIVERSE
        else:
            universe = TIER_1 + TIER_2

        for ticker in universe:
            # HM-AB (2026-05-05): self-symbol-skip — don't stack on existing
            # bull_spread_v1 positions. Mirrors sibling pattern at
            # strategies/bull_call_spread_v1.py:382-384. Placed at top of
            # loop body so we skip BEFORE expensive IV-rank fetch.
            if _already_open(ticker):
                print(f"[bull_spread_v1] {ticker}: bull_spread_v1 open — skip")
                continue

            # 1. IV rank determines the structure
            iv_result = get_iv_rank(ticker, record=True)
            if iv_result is None:
                continue
            iv_rank = iv_result.iv_rank

            structure = ("bull_call_spread" if iv_rank < IV_THRESHOLD
                         else "bull_put_spread")

            # 2. Setup classification -> DTE
            setup = classify(ticker)
            dte = dte_for_setup(setup)

            # 3. Spot + width -> quote
            # Reuse spot already fetched inside get_iv_rank — avoids a
            # redundant Polygon call. Fall back to _get_spot only if missing.
            spot = iv_result.spot if iv_result.spot else _get_spot(ticker)
            if spot is None:
                continue

            # FIRST_TRADE_MODE: risk-capped width ladder (wide -> narrow).
            # Production: spot-based fixed width via select_width().
            if FIRST_TRADE_MODE:
                width, quote = _select_first_trade_width(
                    ticker, structure, dte, spot, FIRST_TRADE_MAX_RISK
                )
            else:
                width = select_width(spot)
                quote = get_spread_quote(ticker, structure, dte, width)
            if quote is None:
                continue

            per_contract_risk = quote.max_loss
            if per_contract_risk <= 0:
                continue

            reasoning = (
                f"{ticker}: IV rank {iv_rank:.1f} -> {structure} "
                f"(setup={setup}, DTE={dte}, width=${width}, "
                f"per-contract risk=${per_contract_risk:.2f})"
            )

            # Confidence: higher when IV rank is further from threshold
            if structure == "bull_put_spread":
                confidence = min(1.0, iv_rank / 100.0)
            else:
                confidence = min(1.0, (100.0 - iv_rank) / 100.0)

            # Common payload structure for both A/B positions
            base_payload = {
                "structure": structure,
                "dte": dte,
                "setup": setup,
                "iv_rank": iv_rank,
                "spot_at_signal": spot,
                "long_leg": {
                    "action": quote.long_leg.action,
                    "option_type": quote.long_leg.option_type,
                    "strike": quote.long_leg.strike,
                    "expiration": quote.long_leg.expiration,
                    "premium": quote.long_leg.premium,
                },
                "short_leg": {
                    "action": quote.short_leg.action,
                    "option_type": quote.short_leg.option_type,
                    "strike": quote.short_leg.strike,
                    "expiration": quote.short_leg.expiration,
                    "premium": quote.short_leg.premium,
                },
                "net_debit": quote.net_debit,
                "net_credit": quote.net_credit,
                "max_profit_per_contract": quote.max_profit,
                "max_loss_per_contract": quote.max_loss,
                "width": width,
            }

            # Position A — textbook
            effective_a_contracts = (
                min(EXIT_A_CONTRACTS, FIRST_TRADE_MAX_CONTRACTS)
                if FIRST_TRADE_MODE else EXIT_A_CONTRACTS
            )
            signals.append(StrategySignal(
                strategy_id=self.strategy_id, ticker=ticker,
                action="open", asset_type="spread", direction="bull",
                max_risk_usd=per_contract_risk * effective_a_contracts,
                confidence=confidence, exit_tag=EXIT_A_TAG,
                payload={
                    **base_payload,
                    "contracts": effective_a_contracts,
                    "exit_rule": "50% TP, 50% SL, 1 DTE hard close",
                },
                reasoning=reasoning,
            ))

            # Position B — scaleout (suppressed in FIRST_TRADE_MODE)
            if not FIRST_TRADE_MODE or EXIT_B_TAG in FIRST_TRADE_ALLOWED_TAGS:
                signals.append(StrategySignal(
                    strategy_id=self.strategy_id, ticker=ticker,
                    action="open", asset_type="spread", direction="bull",
                    max_risk_usd=per_contract_risk * EXIT_B_CONTRACTS,
                    confidence=confidence, exit_tag=EXIT_B_TAG,
                    payload={
                        **base_payload,
                        "contracts": EXIT_B_CONTRACTS,
                        "exit_rule": "Close 2@50%, 1@75%, 1 runner to 1 DTE, 50% SL",
                    },
                    reasoning=reasoning,
                ))

        return signals


# Auto-register at import time so the singleton picks up this strategy
# whenever bull_spread_v1 is imported anywhere (scheduler, tests, REPL).
# Wrapped in try/except: if registration fails (DB lock, sqlite race),
# the import still succeeds; scheduler can retry on next tick.
try:
    from .registry import registry as _registry
    # HM-AB-unhalted-2026-05-05: was enabled=False under HALT-2026-05-05 halt
    # (commit 44c80c2). Self-symbol-skip helper (_already_open above, HM-AB)
    # now closes the stacking gap that triggered the halt. Both gates True
    # again; strategy resumes firing on non-SPY symbols (SPY blocked while
    # 19 existing positions remain open).
    _registry().register(BullSpreadV1(enabled=True))
except Exception as _reg_err:
    print(f"[bull_spread_v1] auto-registration skipped: {_reg_err}")
