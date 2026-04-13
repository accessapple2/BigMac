from __future__ import annotations
from datetime import datetime, time as dtime
import os
import sqlite3

DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)


class RiskManager:
    # === UNIVERSAL TRADE GUARDRAILS (Strategy Lab S4 backtest + Rallies Arena lessons) ===
    # Rallies Arena winner Grok 4: +8.1% with ~5 trades/month, 40% cash.
    # Our Geordi: -98.98% with 2,795 trades. The fix is discipline.

    # Per-model daily trade limits (HARD — cannot be overridden)
    MAX_TRADES_PER_DAY = {
        "default": 3,            # Most models: max 3 trades per day
        "ollama-local": 2,       # Geordi: max 2 (was unlimited, made 2795)
        "gemini-2.5-flash": 2,   # Worf: max 2 (made 481)
        "grok-4": 8,             # Spock: 100% WR, 15 trades proven
        "ollama-qwen3": 3,       # Dax: 1 bad trade, cautious
        "ollama-plutus": 3,      # McCoy: no closed trades yet S6
        "ollama-coder": 3,       # Data: no closed trades yet S6
        "ollama-llama": 2,       # Uhura: 19% WR history — watch closely
        "energy-arnold": 2,      # Trip Tucker: energy only, max 2
        "options-sosnoff": 3,    # Counselor Troi: wheel strategy, max 3 (one per position)
        "navigator": 8,          # Chekov: positive P&L, scanner role
        "neo-matrix": 5,         # Neo: 1 trade only, prove first
        "ollie-auto": 15,        # Ollie Super Trader: signal-driven, up to 15/day
    }

    # Universal limits
    UNIVERSAL_MIN_CASH_PCT = 0.20         # Always keep 20% cash minimum
    UNIVERSAL_MIN_CONVICTION = 0.65       # Minimum conviction for any trade

    # Bear market overrides (VIX > 30 OR SPY below 200-day MA)
    BEAR_MAX_TRADES_PER_DAY = 1           # Only 1 trade per day in bear
    BEAR_MIN_CASH_PCT = 0.35              # 35% cash in bear (V2: was 40%, deploy more in best picks)
    BEAR_MIN_CONVICTION = 0.80            # Need 80% conviction in bear
    BEAR_MAX_POSITIONS = 8                # Max 8 positions in bear (user override — sequential Ollama)
    NORMAL_MAX_POSITIONS = 8              # Max 8 positions (user override — sequential Ollama)
    CORRELATION_LOOKBACK_DAYS = 60
    CORRELATION_THRESHOLD = 0.70
    CORRELATED_GROUP_MAX_PCT = 0.40
    PER_SYMBOL_MAX_PCT = 0.18
    BEAR_PER_SYMBOL_MAX_PCT = 0.15
    SECTOR_WARNING_THRESHOLD = 0.85
    WARNING_ONLY_PLAYERS = {"neo-matrix", "enterprise-computer", "steve-webull", "super-agent"}

    # Models with their own stop-loss rules — excluded from the fleet trailing stop.
    # These models keep their existing per-model stops instead.
    # geordi: -8% hard stop  |  sulu: -3% intraday stop  |  trip: -7% sector stop
    FLEET_TRAILING_STOP_OPT_OUT = {"ollama-local", "dayblade-sulu", "energy-arnold"}

    # Defensive tickers always allowed in BEAR mode (inverse ETFs, treasuries, metals, volatility)
    BEAR_DEFENSIVE_TICKERS = {
        "SH", "SDS", "SPXU",             # Inverse S&P 500
        "SQQQ", "PSQ", "QID",            # Inverse QQQ
        "TLT", "TLH", "IEF", "SHY",     # Treasuries
        "GLD", "SLV", "IAU", "SGOL",     # Gold / Silver
        "UVXY", "VXX", "VIXY",           # Volatility
    }

    # Per-model position limits — all set to 8 (user override for 16GB Mac Mini)
    MAX_POSITIONS_PER_MODEL = {
        "default": 8,
        "grok-4": 8,             # Spock (paused — kept for reference)
        "ollama-local": 8,       # Geordi (paused — kept for reference)
        "gemini-2.5-flash": 8,   # Worf
        "ollama-qwen3": 8,       # Scotty (paused)
        "ollama-plutus": 8,      # Bones (inactive)
        "energy-arnold": 8,      # Trip Tucker
        "options-sosnoff": 5,    # Counselor Troi: options can be spread wider
        "navigator": 5,          # Chekov: convergence auto-trader, max 5 positions
    }

    # Minimum holding periods (days) — stop day-trading, swing/position trade
    MIN_HOLD_DAYS = {
        "default": 5,            # Hold at least 5 trading days
        "grok-4": 7,             # Spock: 7 days (like Rallies Grok 4)
        "gemini-2.5-flash": 5,   # Worf: 5 days (CAN SLIM holds)
        "ollama-local": 5,       # Geordi: 5 days
        "ollama-qwen3": 10,      # Scotty: 10 days for catalysts to play out
        "ollama-plutus": 7,      # Bones: 7 days for quant signals
    }

    @staticmethod
    def get_max_position_size(conviction: float, is_bear: bool) -> float:
        """V2: Conviction-scaled position sizing — let winners run.
        Rallies Grok 4 put 33% in MU at high conviction."""
        if is_bear:
            if conviction >= 0.90:
                return 0.25  # 25% max even in bear for ultra-high conviction
            elif conviction >= 0.80:
                return 0.15  # 15% for high conviction in bear
            else:
                return 0.10  # 10% for moderate conviction in bear
        else:
            if conviction >= 0.90:
                return 0.33  # 33% — matches Rallies Grok 4's MU allocation
            elif conviction >= 0.80:
                return 0.25  # 25% for high conviction
            elif conviction >= 0.70:
                return 0.20  # 20% for good conviction
            else:
                return 0.15  # 15% for moderate conviction

    @staticmethod
    def get_stop_loss_pct(conviction: float) -> float:
        """V2: Conviction-scaled stop-loss — wider stops for higher conviction.
        Rallies Grok 4 held MU through -5% to -8% dips on way to +73%."""
        if conviction >= 0.90:
            return 0.18  # -18% stop for ultra-high conviction
        elif conviction >= 0.80:
            return 0.15  # -15% stop for high conviction
        elif conviction >= 0.70:
            return 0.12  # -12% stop for good conviction
        else:
            return 0.08  # -8% tight stop for low conviction

    # Per-model specific guardrails
    MODEL_GUARDRAILS = {
        "ollama-local": {  # Geordi — CRITICAL: -98.98% with 2,795 trades
            "max_daily_trades": 2,         # Hard limit (universal also caps at 2)
            "max_daily_trades_vix30": 0,   # VIX > 30 → NO TRADES (hold mode)
            "all_stop_vix": 30,            # VIX > 30 → ALL STOP for Geordi
            "stop_loss_pct": 0.08,         # Tighter 8% stop (was 12%)
            "max_position_pct": 0.15,      # Hard cap 15% per trade
            "mandatory_stop_loss": True,   # Every trade MUST have a stop
            "min_conviction": 0.65,        # Conviction floor
            "min_cash_pct": 0.30,          # 30% cash at all times
            "max_losing_positions": 3,     # 3+ losers → go to cash
        },
        "grok-4": {  # Spock — -17.07% with 513 trades (Rallies Grok 4: +8.1% with ~5)
            "max_daily_trades": 3,         # Hard limit
            "min_thesis_length": 50,       # Stricter thesis (was 20 chars)
            "thesis_must_cite_data": True,  # Must reference specific indicators
            "min_conviction": 0.75,        # Raised from 0.50 → 0.75
            "min_conviction_vix30": 0.85,  # VIX > 30 → need 0.85+ conviction
            "bear_market_cash_pct": 0.50,  # BEAR regime → 50% cash minimum
            "min_cash_pct": 0.30,          # 30% cash at all times (Rallies keeps 40%)
            "revenge_trade_cooldown_hrs": 24,  # Wait 24h after a loss
        },
        "gemini-2.5-flash": {  # Worf — Head of Security, disciplined risk enforcer
            "max_daily_trades": 2,         # Hard limit
            "block_buys_vix30": True,      # VIX > 30 → reject ALL buys (WATCHLIST mode)
            "block_buys_spy_below_200": True,  # SPY < 200MA → CAN SLIM requires uptrend
            "min_conviction": 0.70,        # Higher conviction floor
        },
    }

    def __init__(self, max_position_pct=0.20, max_positions=5, stop_loss_pct=0.12,
                 take_profit_tiers=None, max_daily_trades=30, max_drawdown_pct=0.20,
                 min_cash_reserve_pct=0.15, options_max_pct=0.10, options_total_max_pct=0.35):
        self.max_position_pct = max_position_pct
        self.max_positions = max_positions
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_tiers = take_profit_tiers or [
            (0.10, 0.50),  # +10% → sell 50% of position
            (0.15, 0.50),  # +15% → sell 50% of remaining
            (0.25, 0.50),  # +25% → sell 50% of remaining
            (0.50, 1.00),  # +50% → sell everything left
        ]
        self.max_daily_trades = max_daily_trades
        self.max_drawdown_pct = max_drawdown_pct
        self.min_cash_reserve_pct = min_cash_reserve_pct
        self.options_max_pct = options_max_pct
        self.options_total_max_pct = options_total_max_pct

    def get_model_guardrail(self, player_id: str, key: str, default=None):
        """Get a per-model guardrail value, falling back to default."""
        guardrails = self.MODEL_GUARDRAILS.get(player_id, {})
        return guardrails.get(key, default)

    def _get_fear_greed_score(self) -> "float | None":
        """Get current Fear & Greed score (0-100). Uses the 10-min cache in fear_greed.py."""
        try:
            from engine.fear_greed import get_fear_greed_index
            fg = get_fear_greed_index()
            score = fg.get("score")
            return float(score) if score is not None else None
        except Exception:
            return None

    @staticmethod
    def get_fg_position_multiplier(fg_score: "float | None") -> float:
        """Scale max position size based on Fear & Greed regime.

        Extreme Fear (0-20):  1.25× — buy the blood, market oversold
        Fear / Neutral (20-60): 1.00× — normal sizing
        Greed (60-80):        0.75× — reduce exposure, crowd is leaning long
        Extreme Greed (80-100): 0.50× — protect capital, tops are dangerous

        This caps the *maximum* allowed position. Models can still size smaller.
        """
        if fg_score is None:
            return 1.0
        if fg_score <= 20:
            return 1.25   # Extreme Fear: deploy more
        elif fg_score <= 60:
            return 1.00   # Fear + Neutral: normal
        elif fg_score <= 80:
            return 0.75   # Greed: trim max
        else:
            return 0.50   # Extreme Greed: protect capital

    def _get_vix_price(self) -> float:
        """Get current VIX price, cached for the scan cycle."""
        try:
            from engine.vix_monitor import get_vix_status
            vix = get_vix_status()
            return vix.get("price", 0) if vix else 0
        except Exception:
            return 0

    def _get_spy_below_200ma(self) -> bool:
        """Check if SPY is below its 200-day moving average."""
        try:
            from engine.market_data import get_technical_indicators
            ind = get_technical_indicators("SPY")
            if ind and ind.get("sma_200") and ind.get("price"):
                return ind["price"] < ind["sma_200"]
            # Fallback: check from scan context
            from engine.market_data import get_stock_price
            spy = get_stock_price("SPY")
            if spy and ind and ind.get("sma_200"):
                return spy["price"] < ind["sma_200"]
        except Exception:
            pass
        return False

    def check_global_bear_standdown(self, player_id: str, portfolio: dict,
                                      confidence: float = 0.0) -> tuple:
        """BEAR MARKET STANDDOWN PROTOCOL — applies to ALL models.
        Triggered when: SPY below 200-day MA AND VIX > 30 (or > 35 if F&G < 35).
        Effects:
          - Max 3 trades/day across ALL models combined
          - 50% minimum cash requirement
          - Only trades with conviction > 0.80 allowed
        Returns (blocked: bool, reason: str)."""
        vix_price = self._get_vix_price()
        fg_score = self._get_fear_greed_score()
        standdown_vix = 35 if (fg_score is not None and fg_score < 35) else 30
        if vix_price <= standdown_vix:
            return False, "OK"
        spy_below_200 = self._get_spy_below_200ma()
        if not spy_below_200:
            return False, "OK"

        # Global standdown is active
        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )

        # 50% cash minimum for ALL players
        if total_value > 0 and portfolio["cash"] / total_value < 0.50:
            return True, (
                f"🚨 BEAR STANDDOWN: SPY < 200MA + VIX={vix_price:.1f} — "
                f"cash {portfolio['cash']/total_value:.0%} < 50% minimum. No buys."
            )

        # Conviction floor 0.80 for all models
        if confidence < 0.80:
            return True, (
                f"🚨 BEAR STANDDOWN: SPY < 200MA + VIX={vix_price:.1f} — "
                f"conviction {confidence:.0%} < 80% standdown minimum"
            )

        # Max 3 trades/day across ALL models combined
        try:
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            conn = sqlite3.connect(DB, check_same_thread=False)
            global_count = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE date(executed_at)=?",
                (today,)
            ).fetchone()[0]
            conn.close()
            if global_count >= 3:
                return True, (
                    f"🚨 BEAR STANDDOWN: SPY < 200MA + VIX={vix_price:.1f} — "
                    f"{global_count} fleet trades today, max 3 in standdown"
                )
        except Exception:
            pass

        return False, "OK"

    def is_bear_market(self) -> bool:
        """Check if we're in bear market conditions (VIX > 30 OR SPY < 200MA).

        Threshold raised from 25 to 30 — VIX 25-30 is elevated but tradeable.
        Rallies.ai models actively trade at VIX 25-30 with reduced sizing.
        """
        vix_price = self._get_vix_price()
        if vix_price > 30:
            return True
        return self._get_spy_below_200ma()

    def is_defensive_ticker(self, symbol: str) -> bool:
        """Check if ticker is defensive (inverse ETFs, treasuries, metals, volatility)."""
        if symbol.upper() in self.BEAR_DEFENSIVE_TICKERS:
            return True
        # Also check Worf's inverse arsenal and Dalio's metals recommendations
        try:
            conn = sqlite3.connect(DB, check_same_thread=False, timeout=10)
            # Worf inverse arsenal picks
            row = conn.execute(
                "SELECT symbol FROM signals WHERE player_id IN ('gemini-2.5-flash','dalio-metals') "
                "AND symbol=? AND signal='BUY' AND created_at > datetime('now','-7 days')",
                (symbol.upper(),)
            ).fetchone()
            conn.close()
            if row:
                return True
        except Exception:
            pass
        return False

    def check_bear_market_mode(self, player_id: str, portfolio: dict,
                                confidence: float = 0.0, symbol: str = "") -> tuple:
        """Per-model + universal bear market checks. Returns (blocked: bool, reason: str)."""
        from rich.console import Console
        _console = Console()

        vix_price = self._get_vix_price()
        spy_below_200 = self._get_spy_below_200ma()
        # Recovery Override: when F&G < 35, raise VIX bear threshold from 30 → 35
        fg_score = self._get_fear_greed_score()
        bear_vix_threshold = 35 if (fg_score is not None and fg_score < 35) else 30
        is_bear = vix_price > bear_vix_threshold or spy_below_200

        if not is_bear:
            return False, "OK"

        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )
        cash_pct = portfolio["cash"] / total_value if total_value > 0 else 1.0

        # === UNIVERSAL: Bear market cash floor (all models, even defensives) ===
        if cash_pct < self.BEAR_MIN_CASH_PCT:
            return True, (
                f"🚨 BEAR MODE: cash {cash_pct:.0%} < {self.BEAR_MIN_CASH_PCT:.0%} minimum. "
                f"No new buys until cash raised."
            )

        # === GEORDI: ALL STOP when VIX ≥ threshold (recovery override raises threshold) ===
        all_stop_vix = self.get_model_guardrail(player_id, "all_stop_vix")
        if all_stop_vix:
            all_stop_vix = max(all_stop_vix, bear_vix_threshold)
        if all_stop_vix and vix_price >= all_stop_vix:
            return True, (
                f"🛑 ALL STOP: VIX={vix_price:.1f} ≥ {all_stop_vix} — "
                f"{player_id} going to cash, no new trades"
            )

        # === GEORDI: Too many losing positions → go to cash ===
        max_losers = self.get_model_guardrail(player_id, "max_losing_positions")
        if max_losers:
            losing_count = sum(
                1 for p in portfolio["positions"]
                if p.get("current_price", p["avg_price"]) < p["avg_price"]
            )
            if losing_count >= max_losers:
                return True, (
                    f"🛑 LOSS LIMIT: {losing_count} losing positions ≥ {max_losers} max — "
                    f"go to cash, no new buys until at least 1 position is profitable"
                )

        # === SPOCK: Bear regime → 50% cash minimum ===
        bear_cash_pct = self.get_model_guardrail(player_id, "bear_market_cash_pct")
        if bear_cash_pct and is_bear:
            if cash_pct < bear_cash_pct:
                return True, (
                    f"BEAR MARKET MODE: VIX={vix_price:.1f} — "
                    f"cash is {cash_pct:.0%}, must be ≥{bear_cash_pct:.0%}. "
                    f"No new buys until cash raised."
                )

        # === Per-model cash floor ===
        model_min_cash = self.get_model_guardrail(player_id, "min_cash_pct")
        if model_min_cash and cash_pct < model_min_cash:
            return True, (
                f"CASH FLOOR: {player_id} cash {cash_pct:.0%} < {model_min_cash:.0%} minimum. "
                f"No new buys."
            )

        # === DEFENSIVE TICKERS: Always allowed in bear mode at full size ===
        if symbol and self.is_defensive_ticker(symbol):
            _console.log(f"[bold cyan]BEAR MODE: Defensive buy allowed on {symbol}")
            return False, "OK"

        # === WORF: Block ALL buys when VIX > threshold (WATCHLIST mode — defensives already passed) ===
        if self.get_model_guardrail(player_id, "block_buys_vix30") and vix_price > bear_vix_threshold:
            return True, (
                f"🧘 WATCHLIST MODE: VIX={vix_price:.1f} > {bear_vix_threshold} — "
                f"Lt. Cmdr. Worf: DEFENSIVE POSTURE. CAN SLIM requires confirmed uptrend. HOLD/SELL only."
            )

        # === WORF: Block ALL buys when SPY < 200MA ===
        if self.get_model_guardrail(player_id, "block_buys_spy_below_200") and spy_below_200:
            return True, (
                f"🧘 WATCHLIST MODE: SPY below 200-day MA — "
                f"CAN SLIM M-criteria (Market direction) fails. HOLD/SELL only."
            )

        # === SPOCK: Raised conviction floor when VIX > threshold ===
        min_conv_vix30 = self.get_model_guardrail(player_id, "min_conviction_vix30")
        if min_conv_vix30 and vix_price > bear_vix_threshold and confidence < min_conv_vix30:
            return True, (
                f"HIGH VIX MODE: VIX={vix_price:.1f} — "
                f"conviction {confidence:.0%} < {min_conv_vix30:.0%} required when VIX > 30"
            )

        # === HIGH-CONFIDENCE BUYS: Allow at 50% size (handled in check_buy) ===
        if confidence >= self.BEAR_MIN_CONVICTION:
            _console.log(
                f"[bold yellow]BEAR MODE: Allowing high-confidence buy "
                f"({confidence:.0%}) on {symbol} at 50% size"
            )
            return False, "BEAR_HALF_SIZE"

        # === BLOCK: Low confidence offensive buys ===
        return True, (
            f"🚨 BEAR MODE: conviction {confidence:.0%} < {self.BEAR_MIN_CONVICTION:.0%} "
            f"minimum (VIX={vix_price:.1f}, SPY<200MA={spy_below_200})"
        )

    def get_effective_daily_limit(self, player_id: str) -> int:
        """Get the effective daily trade limit, accounting for bear market conditions."""
        is_bear = self.is_bear_market()

        # In bear market, universal cap is 1/day (overrides model defaults)
        # Chekov (navigator) exempt — convergence signals are scanner-driven, not AI guesses
        # Dalio (dalio-metals) exempt — All Weather trades in all regimes by design
        if is_bear and player_id not in ("navigator", "dalio-metals"):
            # Check model-specific VIX override first (e.g., Geordi=0 when VIX>30)
            vix30_limit = self.get_model_guardrail(player_id, "max_daily_trades_vix30")
            if vix30_limit is not None:
                return vix30_limit
            return self.BEAR_MAX_TRADES_PER_DAY

        # Normal market: use per-model limit from MAX_TRADES_PER_DAY dict
        return self.MAX_TRADES_PER_DAY.get(
            player_id,
            self.MAX_TRADES_PER_DAY["default"]
        )

    def check_buy(self, player_id: str, symbol: str, price: float, qty: float,
                  portfolio: dict, asset_type: str = "stock", is_0dte: bool = False,
                  max_position_override: float = 0.0, confidence: float = 0.0) -> tuple:
        """Returns (allowed: bool, reason: str).

        max_position_override: if > 0, temporarily raises the position cap
        (e.g. 0.40 for triple-aligned conviction trades).
        """
        # Options-only players: block direct stock buys (must enter via put assignment)
        OPTIONS_ONLY_PLAYERS = {"options-sosnoff"}
        if player_id in OPTIONS_ONLY_PLAYERS and asset_type == "stock":
            return False, f"Troi (Wheel Strategy): direct stock buys blocked — use cash-secured puts to enter positions"

        # Per-model bear market mode + global standdown checks
        # Sulu (day trader) exempt — intraday positions close same day, bear regime is irrelevant
        # Dalio (All Weather) exempt — risk parity trades in all regimes by design
        # Anderson (Alpaca Paper) exempt — WARNING_ONLY portfolio, 75% drawdown is pre-existing
        if player_id not in ("dayblade-sulu", "dalio-metals", "super-agent"):
            bear_blocked, bear_reason = self.check_bear_market_mode(
                player_id, portfolio, confidence=confidence, symbol=symbol
            )
            if bear_blocked:
                return False, bear_reason
        bear_reason = "OK"
        # High-confidence bear buys get 50% position size reduction
        bear_half_size = (bear_reason == "BEAR_HALF_SIZE")

        # UNIVERSAL minimum conviction (0.65 for all models)
        effective_min_conv = self.UNIVERSAL_MIN_CONVICTION
        model_min_conv = self.get_model_guardrail(player_id, "min_conviction")
        if model_min_conv:
            effective_min_conv = max(effective_min_conv, model_min_conv)
        if confidence < effective_min_conv:
            return False, f"LOW_CONVICTION: {confidence:.0%} below {effective_min_conv:.0%} minimum"

        cost = qty * price
        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )
        if total_value <= 0:
            return False, "Portfolio value is zero"

        # V2: Conviction-scaled position sizing (let winners run)
        is_bear = self.is_bear_market()
        effective_cap = self.get_max_position_size(confidence, is_bear)
        # Per-model hard caps still apply
        model_max_pct = self.get_model_guardrail(player_id, "max_position_pct")
        if model_max_pct:
            effective_cap = min(effective_cap, model_max_pct)
        if max_position_override > 0 and not model_max_pct:
            effective_cap = max(effective_cap, max_position_override)
        # Bear mode high-confidence buys: 50% position size
        if bear_half_size:
            effective_cap *= 0.50
        # Fleet guardrail: scale by Fear & Greed regime (buy blood, trim greed)
        fg_score = self._get_fear_greed_score()
        fg_mult = self.get_fg_position_multiplier(fg_score)
        effective_cap *= fg_mult

        # GEX regime check — reduce size in volatile gamma, block near call wall
        try:
            import sys, os
            _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if _root not in sys.path:
                sys.path.insert(0, _root)
            from gex_calculator import get_latest_snapshot
            gex = get_latest_snapshot("SPY")
            if gex:
                is_volatile = (gex.get("total_gex") or 0) < 0
                if is_volatile:
                    effective_cap *= 0.75  # negative gamma: 25% size reduction
                    call_wall = gex.get("call_wall")
                    if call_wall and price >= call_wall * 0.99:
                        return False, f"GEX: {symbol} @ ${price:.2f} near call wall ${call_wall} — dealer resistance, likely rejection"
        except Exception:
            pass

        # Position size limit (1% tolerance for floating-point rounding)
        if cost / total_value > effective_cap * 1.01:
            return False, f"Position exceeds {effective_cap:.0%} limit"

        if asset_type == "stock":
            symbol_cap = self.BEAR_PER_SYMBOL_MAX_PCT if is_bear else self.PER_SYMBOL_MAX_PCT
            current_symbol_value = sum(
                p["qty"] * p.get("current_price", p["avg_price"])
                for p in portfolio["positions"]
                if p.get("symbol") == symbol and p.get("asset_type", "stock") == "stock"
            )
            proposed_symbol_pct = (current_symbol_value + cost) / total_value
            if proposed_symbol_pct > symbol_cap * 1.01:
                return False, (
                    f"SYMBOL_CAP: {symbol} would be {proposed_symbol_pct:.0%} "
                    f"of portfolio, above {symbol_cap:.0%} cap"
                )

            try:
                from engine.correlation import get_position_correlation_profile
                corr_profile = get_position_correlation_profile(
                    portfolio["positions"],
                    proposed_symbol=symbol,
                    proposed_cost=cost,
                    total_value=total_value,
                    threshold=self.CORRELATION_THRESHOLD,
                )
                for group in corr_profile.get("group_exposure", []):
                    if not group.get("includes_proposed"):
                        continue
                    group_pct = group["pct_of_portfolio"] / 100.0
                    if group_pct > self.CORRELATED_GROUP_MAX_PCT * 1.01:
                        joined = ", ".join(group["symbols"])
                        return False, (
                            f"CORRELATED_GROUP_CAP: {joined} would reach {group_pct:.0%} "
                            f"of portfolio, above {self.CORRELATED_GROUP_MAX_PCT:.0%} cap"
                        )
            except Exception:
                pass

            if player_id not in self.WARNING_ONLY_PLAYERS:
                try:
                    from engine.sector_tracker import build_sector_bucket_profile
                    sector_profile = build_sector_bucket_profile(
                        portfolio["positions"],
                        proposed_symbol=symbol,
                        proposed_value=cost,
                        total_value=total_value,
                    )
                    for bucket in sector_profile.get("buckets", []):
                        if not bucket.get("includes_proposed"):
                            continue
                        if bucket["status"] == "over_cap":
                            return False, (
                                f"SECTOR_BUCKET_CAP: {bucket['sector']} would be {bucket['pct']:.1f}% "
                                f"of portfolio, above {bucket['cap_pct']:.0f}% cap"
                            )
                except Exception:
                    pass

        # Options / 0DTE limit (5% per ticker)
        if asset_type == "option" or is_0dte:
            if cost / total_value > self.options_max_pct * 1.01:
                return False, f"Options position exceeds {self.options_max_pct:.0%} per-ticker limit"
            # Total options exposure cap
            existing_options_value = sum(
                p["qty"] * p.get("current_price", p["avg_price"])
                for p in portfolio["positions"]
                if p.get("asset_type") == "option"
            )
            if (existing_options_value + cost) / total_value > self.options_total_max_pct * 1.01:
                return False, f"Total options exposure would exceed {self.options_total_max_pct:.0%} limit"

        # V3: Per-model position limits (fewer picks, bigger bets)
        unique_symbols = set(p["symbol"] for p in portfolio["positions"])
        unique_positions = len(unique_symbols)
        already_has = symbol in unique_symbols
        is_bear = self.is_bear_market()
        model_max_pos = self.MAX_POSITIONS_PER_MODEL.get(
            player_id, self.MAX_POSITIONS_PER_MODEL["default"])
        effective_max_pos = min(model_max_pos, self.BEAR_MAX_POSITIONS) if is_bear else model_max_pos
        if unique_positions >= effective_max_pos and not already_has:
            return False, f"MAX_POSITIONS_REACHED: {player_id} at {unique_positions}/{effective_max_pos} positions"

        # Cash reserve: universal 20% min, bear 40% min, model-specific overrides
        effective_cash_floor = self.UNIVERSAL_MIN_CASH_PCT
        model_cash = self.get_model_guardrail(player_id, "min_cash_pct")
        if model_cash:
            effective_cash_floor = max(effective_cash_floor, model_cash)
        if self.is_bear_market():
            effective_cash_floor = max(effective_cash_floor, self.BEAR_MIN_CASH_PCT)
        remaining = portfolio["cash"] - cost
        if remaining / total_value < effective_cash_floor:
            return False, f"CASH_FLOOR: Would breach {effective_cash_floor:.0%} cash reserve"

        # Daily trade limit (VIX-aware: Geordi=5 normal/2 when VIX>25, Spock=10, default=30)
        model_daily_limit = self.get_effective_daily_limit(player_id)
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            conn = sqlite3.connect(DB, check_same_thread=False)
            count = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE player_id=? AND date(executed_at)=?",
                (player_id, today)
            ).fetchone()[0]
            conn.close()
            if count >= model_daily_limit:
                return False, f"Daily trade limit ({model_daily_limit}) reached"
        except Exception:
            pass

        return True, "OK"

    def get_portfolio_construction_warnings(self, player_id: str, portfolio: dict) -> dict:
        """Warning-only portfolio construction diagnostics for UI overlays."""
        total_value = portfolio["cash"] + sum(
            p["qty"] * p.get("current_price", p["avg_price"])
            for p in portfolio["positions"]
        )
        warnings = []
        sector = {"buckets": [], "warnings": [], "caps": {}}
        corr = {"groups": [], "group_exposure": [], "warnings": []}

        try:
            from engine.sector_tracker import build_sector_bucket_profile
            sector = build_sector_bucket_profile(portfolio["positions"], total_value=total_value)
            warnings.extend(sector.get("warnings", []))
        except Exception:
            pass

        try:
            from engine.correlation import get_position_correlation_profile
            corr = get_position_correlation_profile(portfolio["positions"], total_value=total_value)
            warnings.extend(corr.get("warnings", []))
        except Exception:
            pass

        return {
            "player_id": player_id,
            "sector": sector,
            "correlation": corr,
            "warnings": warnings,
            "updated": datetime.now().isoformat(),
        }

    def check_stop_loss_take_profit(self, player_id: str, positions: list,
                                     current_prices: dict) -> list:
        """Returns list of sell actions triggered by SL, tiered TP, or options expiry.

        Tiered TP: sells 25% of position at each tier (10%, 15%, 25%, 50%).
        Stop-loss: sells 100% of position.
        Options expiry: auto-close options within AUTO_CLOSE_DTE days of expiry.
        """
        actions = []
        # Track which tiers have already been hit per position
        filled_tiers = self._get_filled_tiers(player_id)

        for pos in positions:
            # Auto-close options approaching expiry
            if pos.get("asset_type") == "option" and pos.get("expiry_date"):
                try:
                    from config import OPTIONS_AUTO_CLOSE_DTE
                    expiry = datetime.strptime(pos["expiry_date"], "%Y-%m-%d").date()
                    days_left = (expiry - datetime.now().date()).days
                    if days_left <= OPTIONS_AUTO_CLOSE_DTE:
                        actions.append({
                            "symbol": pos["symbol"],
                            "action": "SELL",
                            "qty": pos["qty"],
                            "reason": f"Options expiry auto-close ({days_left}d left, exp {pos['expiry_date']})",
                            "asset_type": "option",
                            "option_type": pos.get("option_type"),
                        })
                        continue
                except Exception:
                    pass
            symbol = pos["symbol"]
            price_data = current_prices.get(symbol)
            if not price_data:
                continue
            stock_price = price_data.get("price", 0)
            if stock_price <= 0 or pos["avg_price"] <= 0:
                continue

            # For options, estimate current premium and enforce stop-loss
            if pos.get("asset_type") == "option":
                from engine.paper_trader import estimate_option_price
                current = estimate_option_price(
                    pos.get("option_type"), pos.get("strike_price"),
                    stock_price, pos["avg_price"], pos.get("expiry_date")
                )
                # Options stop-loss: exit if premium drops below threshold
                from config import OPTIONS_STOP_LOSS_PCT
                opt_sl_pct = OPTIONS_STOP_LOSS_PCT if OPTIONS_STOP_LOSS_PCT else 0.50
                if pos["avg_price"] > 0 and current < pos["avg_price"] * (1 - opt_sl_pct):
                    actions.append({
                        "symbol": symbol,
                        "action": "SELL",
                        "qty": pos["qty"],
                        "reason": f"Options stop-loss triggered: premium down {((pos['avg_price'] - current) / pos['avg_price'] * 100):.0f}% (limit: -{opt_sl_pct*100:.0f}%)",
                        "asset_type": "option",
                        "option_type": pos.get("option_type"),
                    })
                    continue
            else:
                current = stock_price

            pnl_pct = (current - pos["avg_price"]) / pos["avg_price"]

            # === FLEET TRAILING STOP (opt-out, checked every scan cycle) ===
            # Always update high watermark for all models (used for display + tracking).
            high_wm = pos.get("high_watermark") or pos["avg_price"]
            if current > high_wm:
                high_wm = current
                self._update_high_watermark(player_id, symbol, high_wm)

            gain_from_entry = (current - pos["avg_price"]) / pos["avg_price"] if pos["avg_price"] > 0 else 0

            # Fleet trailing stop: applies to models WITHOUT their own stop rules.
            # Opt-out models (geordi -8%, sulu -3%, trip -7%) keep their per-model stops below.
            # Activates only after position is up >= 5% from entry.
            # Trail: 3% below the high watermark, floor at breakeven (entry price).
            if player_id not in self.FLEET_TRAILING_STOP_OPT_OUT and gain_from_entry >= 0.05:
                fleet_trail_price = high_wm * 0.97  # 3% below high watermark
                # Breakeven floor: once up 5%+ we never stop below entry
                fleet_trail_price = max(fleet_trail_price, pos["avg_price"])
                if current <= fleet_trail_price:
                    actions.append({
                        "symbol": symbol,
                        "action": "SELL",
                        "qty": pos["qty"],
                        "reason": (
                            f"Fleet trailing stop: ${current:.2f} ≤ ${fleet_trail_price:.2f} "
                            f"(high: ${high_wm:.2f}, 3% trail, entry +{gain_from_entry:.1%})"
                        ),
                        "asset_type": pos.get("asset_type", "stock"),
                        "option_type": pos.get("option_type"),
                    })
                    continue

            # Stop-loss: sell entire position (per-model override: Geordi=8%, default=12%)
            model_sl = self.get_model_guardrail(player_id, "stop_loss_pct", self.stop_loss_pct)
            if pnl_pct <= -model_sl:
                actions.append({
                    "symbol": symbol,
                    "action": "SELL",
                    "qty": pos["qty"],
                    "reason": f"Stop-loss at {pnl_pct:.1%}",
                    "asset_type": pos.get("asset_type", "stock"),
                    "option_type": pos.get("option_type"),
                })
                continue

            # Tiered take-profit: sell % of remaining at each tier
            # e.g. 100 shares → +10%: sell 50 → +15%: sell 25 → +25%: sell 12 → +50%: sell 13
            pos_key = f"{player_id}:{symbol}"
            hit_tiers = filled_tiers.get(pos_key, set())
            remaining_qty = pos["qty"]

            for tier_pct, sell_frac in self.take_profit_tiers:
                if pnl_pct >= tier_pct and tier_pct not in hit_tiers:
                    sell_qty = round(remaining_qty * sell_frac, 4)
                    if sell_qty > 0:
                        actions.append({
                            "symbol": symbol,
                            "action": "SELL_PARTIAL" if sell_frac < 1.0 else "SELL",
                            "qty": sell_qty,
                            "reason": f"Take-profit tier {tier_pct:.0%} hit (+{pnl_pct:.1%}), selling {sell_frac:.0%} of remaining",
                            "asset_type": pos.get("asset_type", "stock"),
                            "option_type": pos.get("option_type"),
                            "tier": tier_pct,
                        })
                        remaining_qty -= sell_qty
                        hit_tiers.add(tier_pct)

            if hit_tiers:
                self._save_filled_tiers(player_id, symbol, hit_tiers)

        return actions

    def _get_filled_tiers(self, player_id: str) -> dict:
        """Load which TP tiers have already been triggered from DB."""
        try:
            conn = sqlite3.connect(DB, check_same_thread=False)
            rows = conn.execute(
                "SELECT symbol, reasoning FROM trades WHERE player_id=? AND reasoning LIKE 'Take-profit tier%'",
                (player_id,)
            ).fetchall()
            conn.close()
            result = {}
            for symbol, reasoning in rows:
                key = f"{player_id}:{symbol}"
                if key not in result:
                    result[key] = set()
                # Extract tier from "Take-profit tier 10% hit"
                for tier_pct, _ in self.take_profit_tiers:
                    if f"tier {tier_pct:.0%}" in reasoning:
                        result[key].add(tier_pct)
            return result
        except Exception:
            return {}

    def _save_filled_tiers(self, player_id: str, symbol: str, tiers: set):
        """Tiers are tracked via trade reasoning strings, no extra storage needed."""
        pass

    @staticmethod
    def _get_trailing_stop_pct(gain_pct: float) -> float:
        """V3: Dynamic trailing stop — tighter trail as gains grow, locking in profits.
        gain_pct is the % gain from entry (e.g. 0.20 = up 20%)."""
        if gain_pct >= 0.20:     # Up 20%+ → trail at 10% below high
            return 0.10
        elif gain_pct >= 0.10:   # Up 10%+ → trail at 12% below high
            return 0.12
        elif gain_pct >= 0.05:   # Up 5%+ → trail at 15% below high
            return 0.15
        else:                    # Small gain → tight 5% trail
            return 0.05

    def _update_high_watermark(self, player_id: str, symbol: str, price: float):
        """Update the high watermark for a position."""
        try:
            conn = sqlite3.connect(DB, check_same_thread=False)
            conn.execute(
                "UPDATE positions SET high_watermark=? WHERE player_id=? AND symbol=? AND asset_type='stock'",
                (price, player_id, symbol)
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    def check_drawdown(self, player_id: str, starting_cash: float = 7000.0) -> tuple:
        """Returns (is_halted: bool, drawdown_pct: float)."""
        try:
            conn = sqlite3.connect(DB, check_same_thread=False)
            # Get current season to avoid comparing against inflated S1 peaks
            season_row = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
            season = int(season_row[0]) if season_row else 1
            peak = conn.execute(
                "SELECT MAX(total_value) FROM portfolio_history WHERE player_id=? AND season=?",
                (player_id, season)
            ).fetchone()
            latest = conn.execute(
                "SELECT total_value FROM portfolio_history WHERE player_id=? AND season=? ORDER BY recorded_at DESC LIMIT 1",
                (player_id, season)
            ).fetchone()
            conn.close()

            peak_val = peak[0] if peak and peak[0] else starting_cash
            current_val = latest[0] if latest and latest[0] else starting_cash

            if peak_val <= 0:
                return False, 0.0

            drawdown = (peak_val - current_val) / peak_val
            return drawdown >= self.max_drawdown_pct, drawdown
        except Exception:
            return False, 0.0

    @staticmethod
    def is_extended_trading_hours() -> bool:
        """Return True during ollie-auto's extended trading windows (ET):
        Pre-market:  7:00 AM – 9:30 AM ET
        After-hours: 4:00 PM – 6:00 PM ET
        Returns False on weekends.
        """
        import pytz
        et = pytz.timezone("US/Eastern")
        now = datetime.now(et)
        if now.weekday() >= 5:
            return False
        t = now.time()
        return (dtime(7, 0) <= t < dtime(9, 30)) or (dtime(16, 0) <= t < dtime(18, 0))

    @staticmethod
    def is_market_hours() -> str | bool:
        """Check if within trading hours (Mountain Time).
        Pre-market:  4:00 AM - 9:30 AM ET  →  2:00 AM - 7:30 AM MT
        Market:      9:30 AM - 4:00 PM ET  →  7:30 AM - 2:00 PM MT
        Post-market: 4:00 PM - 8:00 PM ET  →  2:00 PM - 6:00 PM MT
        Extended AH: 8:00 PM - 11:00 PM ET →  6:00 PM - 9:00 PM MT (earnings reactions)
        Closed:     11:00 PM - 4:00 AM ET  →  9:00 PM - 2:00 AM MT
        """
        import pytz
        mt = pytz.timezone("US/Mountain")
        now = datetime.now(mt)

        # Weekend check
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            return False

        t = now.time()
        pre_open = dtime(2, 0)       # 2:00 AM MT (4:00 AM ET)
        market_open = dtime(7, 30)    # 7:30 AM MT (9:30 AM ET)
        market_close = dtime(14, 0)   # 2:00 PM MT (4:00 PM ET)
        post_close = dtime(18, 0)     # 6:00 PM MT (8:00 PM ET)
        extended_close = dtime(21, 0) # 9:00 PM MT (11:00 PM ET)

        if pre_open <= t < market_open:
            return "pre_market"
        elif market_open <= t < market_close:
            return "market"
        elif market_close <= t < post_close:
            return "post_market"
        elif post_close <= t < extended_close:
            return "post_market"  # Extended after-hours for earnings reactions
        else:
            return False
