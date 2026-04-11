"""
╔══════════════════════════════════════════════════════════════╗
║  USS TradeMinds — Lt. Uhura v2: Full Spectrum Flow Officer  ║
║  "All hailing frequencies open, Captain."                   ║
║                                                             ║
║  Target: Match 86% backtest win rate on options trades      ║
╚══════════════════════════════════════════════════════════════╝

v2 UPGRADES over v1:
  - Reads GEX environment (positive/negative gamma)
  - Uses GEX strike levels for precise strike selection
  - Integrates High IV Scanner for premium pricing
  - Cross-references Congress trades for conviction boost
  - Reads cross-model intelligence (arena confidence)
  - Uses Vol Surface for optimal strike pricing
  - Reads dedicated Put/Call Skew endpoint
  - Multi-signal confluence scoring (needs 4+ signals aligned)
  - NO TRADE unless confluence >= 4 (this is how we hit 86%)

Signal Chain:
  Whale Tracker + GEX + Options Flow + High IV + Congress
    + Arena Confidence + Vol Surface + Regime
      → Uhura (interpretation) → Spock (reasoning)
        → Dalio (macro confirm) → Anderson (execution)

PHILOSOPHY:
  The 86% backtest works because it's SELECTIVE.
  Uhura v2 only fires when multiple independent signals align.
  No signal = no trade. Patience IS the edge.
"""

import logging
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

logger = logging.getLogger("uhura")


# ─── ENUMS ───────────────────────────────────────────────────

class FlowBias(Enum):
    STRONG_BEARISH = "STRONG_BEARISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    BULLISH = "BULLISH"
    STRONG_BULLISH = "STRONG_BULLISH"


class TradeType(Enum):
    BEAR_PUT_SPREAD = "BEAR_PUT_SPREAD"
    BEAR_CALL_CREDIT = "BEAR_CALL_CREDIT"      # sell call spread (high IV)
    BULL_PUT_CREDIT = "BULL_PUT_CREDIT"          # sell put spread (high IV)
    BULL_CALL_SPREAD = "BULL_CALL_SPREAD"
    IRON_CONDOR = "IRON_CONDOR"
    LONG_PUT = "LONG_PUT"
    LONG_CALL = "LONG_CALL"
    NO_TRADE = "NO_TRADE"


class GammaRegime(Enum):
    """Dealer gamma positioning — changes EVERYTHING about trade selection."""
    POSITIVE = "POSITIVE"    # Mean-reverting, sell premium, fade moves
    NEGATIVE = "NEGATIVE"    # Trending, buy premium, ride momentum
    NEUTRAL = "NEUTRAL"


class Conviction(Enum):
    MAXIMUM = "MAXIMUM"      # 6+ signals aligned — full size
    HIGH = "HIGH"            # 5 signals — 75% size
    MEDIUM = "MEDIUM"        # 4 signals — 50% size
    LOW = "LOW"              # <4 signals — NO TRADE (this is the 86% filter)


# ─── DATA MODELS ─────────────────────────────────────────────

@dataclass
class VolumeSpike:
    ticker: str
    multiplier: float
    price: float
    timestamp: str

    @property
    def is_significant(self) -> bool:
        return self.multiplier >= 2.5


@dataclass
class OptionsFlow:
    ticker: str
    call_volume: int
    put_volume: int
    pc_ratio: float
    has_uoa: bool

    @property
    def flow_direction(self) -> str:
        if self.pc_ratio >= 1.5:
            return "HEAVY_PUTS"
        elif self.pc_ratio >= 1.1:
            return "SLIGHT_BEARISH"
        elif self.pc_ratio >= 0.9:
            return "NEUTRAL"
        elif self.pc_ratio >= 0.6:
            return "SLIGHT_BULLISH"
        else:
            return "HEAVY_CALLS"


@dataclass
class GEXData:
    """Gamma exposure data from /api/gamma-environment and /api/market/gex/SPY"""
    gamma_regime: GammaRegime          # POSITIVE or NEGATIVE
    gex_flip_level: Optional[float]    # price where gamma flips sign
    call_resistance: Optional[float]   # CR — price ceiling from gamma
    put_support: Optional[float]       # PS — price floor from gamma
    hvl: Optional[float]               # High Volume Level — magnet/pivot
    top_gamma_strikes: list = field(default_factory=list)  # [{strike, gex_value}, ...]

    @property
    def range_width(self) -> Optional[float]:
        """Distance between put support and call resistance = expected range."""
        if self.call_resistance and self.put_support:
            return self.call_resistance - self.put_support
        return None


@dataclass
class HighIVData:
    """From /api/high-iv — stocks with elevated implied volatility."""
    vix: float
    vix_percentile: Optional[float]    # where VIX sits vs 1yr range
    high_iv_tickers: list = field(default_factory=list)  # [{ticker, iv_rank}, ...]

    @property
    def is_elevated(self) -> bool:
        return self.vix >= 25.0

    @property
    def is_extreme(self) -> bool:
        return self.vix >= 35.0

    @property
    def favor_selling_premium(self) -> bool:
        """When IV is high, selling premium (credit spreads) has edge."""
        return self.vix >= 22.0


@dataclass
class CongressTrade:
    """From Congress Tracker — Capitol Trades + Quiver Quant."""
    politician: str
    party: str           # "D" or "R"
    ticker: str
    action: str          # "BUY" or "SELL"
    size: str            # "$1K-$15K", "$50K-$100K", etc.
    date: str
    source: str          # "Capitol" or "Quiver"


@dataclass
class ArenaConfidence:
    """From /api/arena/confidence — what the crew thinks."""
    bullish_models: list = field(default_factory=list)   # model names
    bearish_models: list = field(default_factory=list)
    neutral_models: list = field(default_factory=list)

    @property
    def consensus(self) -> str:
        b = len(self.bullish_models)
        r = len(self.bearish_models)
        if b > r + 1:
            return "BULLISH"
        elif r > b + 1:
            return "BEARISH"
        return "MIXED"

    @property
    def consensus_strength(self) -> float:
        """0.0 = split, 1.0 = unanimous."""
        total = len(self.bullish_models) + len(self.bearish_models) + len(self.neutral_models)
        if total == 0:
            return 0.0
        majority = max(len(self.bullish_models), len(self.bearish_models), len(self.neutral_models))
        return majority / total


@dataclass
class MarketRegime:
    trend: str               # BEAR_TREND, BULL_TREND, CHOPPY
    spy_price: float
    spy_vs_50ma_pct: float
    spy_vs_200ma_pct: float
    vix: float
    oil_price: Optional[float] = None
    oil_change_pct: Optional[float] = None
    dxy_change_pct: Optional[float] = None
    catalysts: list = field(default_factory=list)

    @property
    def is_bearish(self) -> bool:
        return self.trend in ("BEAR_TREND", "CRASH")

    @property
    def is_bullish(self) -> bool:
        return self.trend in ("BULL_TREND", "STRONG_BULL")

    @property
    def cross_asset_bearish(self) -> bool:
        """Oil AND dollar both rising = bearish for equities."""
        if self.oil_change_pct and self.dxy_change_pct:
            return self.oil_change_pct > 1.0 and self.dxy_change_pct > 0.5
        return False


@dataclass
class VolSurfaceData:
    """From /api/vol-surface — IV across strikes for mispricing."""
    ticker: str
    atm_iv: float                # at-the-money implied vol
    otm_put_iv: Optional[float]  # OTM put implied vol (skew)
    otm_call_iv: Optional[float] # OTM call implied vol
    cheap_strikes: list = field(default_factory=list)   # strikes where IV < historical
    expensive_strikes: list = field(default_factory=list)


# ─── CONFLUENCE SIGNAL — Individual Signal Votes ─────────────

@dataclass
class SignalVote:
    """One signal source's vote on direction."""
    source: str          # e.g. "whale_tracker", "gex", "congress"
    direction: str       # "BEARISH", "BULLISH", "NEUTRAL"
    weight: float        # 0.0 to 2.0 (some signals count more)
    reasoning: str

    def __str__(self):
        icon = "🔴" if self.direction == "BEARISH" else "🟢" if self.direction == "BULLISH" else "⚪"
        return f"{icon} {self.source}: {self.direction} (wt:{self.weight}) — {self.reasoning}"


# ─── UHURA SIGNAL OUTPUT ─────────────────────────────────────

@dataclass
class UhuraSignal:
    timestamp: str
    flow_bias: FlowBias
    conviction: Conviction
    recommended_trade: TradeType
    reasoning: str
    tickers_flagged: list
    regime_context: str
    gamma_regime: GammaRegime

    # Confluence details
    total_signals: int = 0
    aligned_signals: int = 0
    signal_votes: list = field(default_factory=list)

    # Trade parameters
    suggested_ticker: Optional[str] = None
    suggested_direction: Optional[str] = None
    suggested_structure: Optional[str] = None
    max_risk_pct: float = 2.0
    position_size_modifier: float = 1.0   # 0.5 for negative gamma

    # GEX-informed levels
    gex_support: Optional[float] = None
    gex_resistance: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "flow_bias": self.flow_bias.value,
            "conviction": self.conviction.value,
            "recommended_trade": self.recommended_trade.value,
            "reasoning": self.reasoning,
            "tickers_flagged": self.tickers_flagged,
            "regime_context": self.regime_context,
            "gamma_regime": self.gamma_regime.value,
            "total_signals": self.total_signals,
            "aligned_signals": self.aligned_signals,
            "signal_votes": [str(v) for v in self.signal_votes],
            "suggested_ticker": self.suggested_ticker,
            "suggested_direction": self.suggested_direction,
            "suggested_structure": self.suggested_structure,
            "max_risk_pct": self.max_risk_pct,
            "position_size_modifier": self.position_size_modifier,
            "gex_support": self.gex_support,
            "gex_resistance": self.gex_resistance,
        }

    def __str__(self) -> str:
        lines = [
            "╔══════════════════════════════════════════════════════════╗",
            "║       📡 LT. UHURA v2 — FULL SPECTRUM SIGNAL           ║",
            "╚══════════════════════════════════════════════════════════╝",
            f"  Timestamp:      {self.timestamp}",
            f"  Flow Bias:      {self.flow_bias.value}",
            f"  Conviction:     {self.conviction.value}",
            f"  Confluence:     {self.aligned_signals}/{self.total_signals} signals aligned",
            f"  Gamma Regime:   {self.gamma_regime.value}",
            f"  Trade Type:     {self.recommended_trade.value}",
            f"  Direction:      {self.suggested_direction or 'N/A'}",
            f"  Structure:      {self.suggested_structure or 'N/A'}",
            f"  Max Risk:       {self.max_risk_pct}% of account",
            f"  Size Modifier:  {self.position_size_modifier}x",
            f"  GEX Support:    ${self.gex_support}" if self.gex_support else "",
            f"  GEX Resistance: ${self.gex_resistance}" if self.gex_resistance else "",
            f"  Flagged:        {', '.join(self.tickers_flagged)}",
            f"  Regime:         {self.regime_context}",
            "",
            "  ─── SIGNAL VOTES ───",
        ]
        for vote in self.signal_votes:
            lines.append(f"  {vote}")
        lines.append("")
        lines.append(f"  REASONING: {self.reasoning}")
        lines.append("═" * 58)
        return "\n".join([l for l in lines if l != ""])


# ─── THE UHURA v2 ENGINE ─────────────────────────────────────

class LtUhura:
    """
    Lt. Uhura v2 — Full Spectrum Communications Officer.

    Reads EVERY signal source on the ship and synthesizes into
    one actionable recommendation. Only fires when 4+ independent
    signals align (confluence filter = the 86% edge).

    Usage:
        uhura = LtUhura(account_size=25000)
        signal = uhura.interpret(
            volume_spikes=[...],
            options_flows=[...],
            regime=MarketRegime(...),
            gex=GEXData(...),
            high_iv=HighIVData(...),
            congress_trades=[...],
            arena=ArenaConfidence(...),
            vol_surface=VolSurfaceData(...)   # optional
        )
        print(signal)
    """

    # Minimum confluence to take a trade (THE 86% FILTER)
    MIN_CONFLUENCE = 4

    def __init__(self, account_size: float = 25000.0):
        self.account_size = account_size
        logger.info("📡 Lt. Uhura v2 reporting for duty. All frequencies open.")

    # ── SIGNAL 1: Volume Spikes ──────────────────────────────

    def _vote_volume_spikes(self, spikes: list[VolumeSpike]) -> SignalVote:
        if not spikes:
            return SignalVote("whale_tracker", "NEUTRAL", 0.5, "No volume data")

        significant = [s for s in spikes if s.is_significant]
        unique_tickers = set(s.ticker for s in spikes)
        cluster = len(unique_tickers) >= 3

        if cluster and len(significant) >= 2:
            top = max(spikes, key=lambda s: s.multiplier)
            return SignalVote(
                "whale_tracker", "BEARISH" if len(spikes) > 5 else "BULLISH",
                1.5, f"CLUSTER: {len(unique_tickers)} tickers, top {top.ticker} at {top.multiplier}x"
            )
        elif significant:
            top = max(significant, key=lambda s: s.multiplier)
            return SignalVote(
                "whale_tracker", "NEUTRAL", 0.8,
                f"{len(significant)} significant spikes, top {top.ticker} at {top.multiplier}x"
            )
        return SignalVote("whale_tracker", "NEUTRAL", 0.3, "Low spike activity")

    # ── SIGNAL 2: Options Flow (P/C Ratios + UOA) ───────────

    def _vote_options_flow(self, flows: list[OptionsFlow]) -> SignalVote:
        if not flows:
            return SignalVote("options_flow", "NEUTRAL", 0.5, "No flow data")

        total_vol = sum(f.call_volume + f.put_volume for f in flows)
        if total_vol == 0:
            return SignalVote("options_flow", "NEUTRAL", 0.3, "Zero volume")

        weighted_pc = sum(
            f.pc_ratio * (f.call_volume + f.put_volume) for f in flows
        ) / total_vol

        uoa_tickers = [f.ticker for f in flows if f.has_uoa]
        weight = 1.0 + (0.3 * len(uoa_tickers))  # UOA boosts weight

        if weighted_pc >= 1.4:
            return SignalVote("options_flow", "BEARISH", weight,
                              f"P/C {weighted_pc:.2f} heavy puts. UOA: {uoa_tickers or 'none'}")
        elif weighted_pc >= 1.1:
            return SignalVote("options_flow", "BEARISH", weight * 0.7,
                              f"P/C {weighted_pc:.2f} slight put bias")
        elif weighted_pc <= 0.6:
            return SignalVote("options_flow", "BULLISH", weight,
                              f"P/C {weighted_pc:.2f} heavy calls. UOA: {uoa_tickers or 'none'}")
        elif weighted_pc <= 0.85:
            return SignalVote("options_flow", "BULLISH", weight * 0.7,
                              f"P/C {weighted_pc:.2f} slight call bias")
        return SignalVote("options_flow", "NEUTRAL", 0.5,
                          f"P/C {weighted_pc:.2f} balanced")

    # ── SIGNAL 3: GEX / Gamma Environment ────────────────────

    def _vote_gex(self, gex: Optional[GEXData], spy_price: float) -> SignalVote:
        if not gex:
            return SignalVote("gex", "NEUTRAL", 0.5, "No GEX data")

        notes = []

        # Where is price relative to GEX levels?
        if gex.call_resistance and spy_price >= gex.call_resistance * 0.995:
            notes.append(f"At call resistance ${gex.call_resistance} — ceiling")
            direction = "BEARISH"
        elif gex.put_support and spy_price <= gex.put_support * 1.005:
            notes.append(f"At put support ${gex.put_support} — floor")
            direction = "BULLISH"
        elif gex.hvl:
            if spy_price > gex.hvl:
                notes.append(f"Above HVL ${gex.hvl} — bullish bias")
                direction = "BULLISH"
            else:
                notes.append(f"Below HVL ${gex.hvl} — bearish bias")
                direction = "BEARISH"
        else:
            direction = "NEUTRAL"
            notes.append("No clear GEX directional signal")

        # Gamma regime note
        if gex.gamma_regime == GammaRegime.POSITIVE:
            notes.append("Positive gamma: mean-reverting, sell premium favored")
        elif gex.gamma_regime == GammaRegime.NEGATIVE:
            notes.append("Negative gamma: trending, directional trades favored")

        return SignalVote("gex", direction, 1.5, " | ".join(notes))

    # ── SIGNAL 4: Market Regime ──────────────────────────────

    def _vote_regime(self, regime: MarketRegime) -> SignalVote:
        notes = [f"{regime.trend} | VIX {regime.vix}"]

        if regime.spy_vs_50ma_pct < -5:
            notes.append(f"Extended below 50MA ({regime.spy_vs_50ma_pct}%)")
        if regime.cross_asset_bearish:
            notes.append("Oil+Dollar rising = equity headwind")

        if regime.is_bearish:
            return SignalVote("regime", "BEARISH", 1.5, " | ".join(notes))
        elif regime.is_bullish:
            return SignalVote("regime", "BULLISH", 1.5, " | ".join(notes))
        return SignalVote("regime", "NEUTRAL", 0.8, " | ".join(notes))

    # ── SIGNAL 5: Congress Trades ────────────────────────────

    def _vote_congress(self, trades: list[CongressTrade], watchlist: list[str] = None) -> SignalVote:
        if not trades:
            return SignalVote("congress", "NEUTRAL", 0.3, "No recent congress trades")

        # Focus on trades in our watchlist or SPY/QQQ related
        relevant = trades
        if watchlist:
            relevant = [t for t in trades if t.ticker in watchlist or t.ticker in ("SPY", "QQQ", "VOO")]

        if not relevant:
            return SignalVote("congress", "NEUTRAL", 0.3, "No relevant congress trades")

        buys = [t for t in relevant if t.action == "BUY"]
        sells = [t for t in relevant if t.action == "SELL"]

        # Big money congress buys = bullish signal
        big_buys = [t for t in buys if "$50K" in t.size or "$100K" in t.size
                    or "$250K" in t.size or "$500K" in t.size or "$1M" in t.size]
        big_sells = [t for t in sells if "$50K" in t.size or "$100K" in t.size
                     or "$250K" in t.size or "$500K" in t.size or "$1M" in t.size]

        if len(big_sells) > len(big_buys) + 1:
            top_seller = big_sells[0]
            return SignalVote("congress", "BEARISH", 1.2,
                              f"Congress selling: {top_seller.politician} sold {top_seller.ticker} ({top_seller.size})")
        elif len(big_buys) > len(big_sells) + 1:
            top_buyer = big_buys[0]
            return SignalVote("congress", "BULLISH", 1.2,
                              f"Congress buying: {top_buyer.politician} bought {top_buyer.ticker} ({top_buyer.size})")
        return SignalVote("congress", "NEUTRAL", 0.3,
                          f"{len(buys)} buys, {len(sells)} sells — mixed")

    # ── SIGNAL 6: Arena Confidence (Cross-Model) ─────────────

    def _vote_arena(self, arena: Optional[ArenaConfidence]) -> SignalVote:
        if not arena:
            return SignalVote("arena", "NEUTRAL", 0.5, "No arena data")

        consensus = arena.consensus
        strength = arena.consensus_strength

        if consensus == "BEARISH" and strength >= 0.6:
            return SignalVote("arena", "BEARISH", 1.0 + strength,
                              f"Crew consensus BEARISH ({len(arena.bearish_models)} models: "
                              f"{', '.join(arena.bearish_models[:3])})")
        elif consensus == "BULLISH" and strength >= 0.6:
            return SignalVote("arena", "BULLISH", 1.0 + strength,
                              f"Crew consensus BULLISH ({len(arena.bullish_models)} models: "
                              f"{', '.join(arena.bullish_models[:3])})")
        return SignalVote("arena", "NEUTRAL", 0.5,
                          f"Crew split: {len(arena.bullish_models)}B / "
                          f"{len(arena.bearish_models)}R / {len(arena.neutral_models)}N")

    # ── SIGNAL 7: High IV Environment ────────────────────────

    def _vote_high_iv(self, high_iv: Optional[HighIVData]) -> SignalVote:
        if not high_iv:
            return SignalVote("high_iv", "NEUTRAL", 0.3, "No IV data")

        if high_iv.is_extreme:
            return SignalVote("high_iv", "NEUTRAL", 1.0,
                              f"VIX {high_iv.vix} EXTREME — credit spreads strongly favored, "
                              f"avoid buying premium")
        elif high_iv.favor_selling_premium:
            return SignalVote("high_iv", "NEUTRAL", 0.8,
                              f"VIX {high_iv.vix} elevated — credit spreads have edge")
        return SignalVote("high_iv", "NEUTRAL", 0.3,
                          f"VIX {high_iv.vix} normal — debit or credit both fine")

    # ── CONFLUENCE CALCULATOR ────────────────────────────────

    def _calculate_confluence(self, votes: list[SignalVote]) -> dict:
        """
        Count how many independent signals agree on direction.
        This is THE key filter. 4+ aligned = trade. <4 = no trade.
        """
        # Only count non-neutral votes
        directional = [v for v in votes if v.direction != "NEUTRAL"]
        bearish = [v for v in directional if v.direction == "BEARISH"]
        bullish = [v for v in directional if v.direction == "BULLISH"]

        # Weighted scoring
        bear_weight = sum(v.weight for v in bearish)
        bull_weight = sum(v.weight for v in bullish)

        if bear_weight > bull_weight:
            dominant = "BEARISH"
            aligned = len(bearish)
            weighted_score = bear_weight - bull_weight
        elif bull_weight > bear_weight:
            dominant = "BULLISH"
            aligned = len(bullish)
            weighted_score = bull_weight - bear_weight
        else:
            dominant = "NEUTRAL"
            aligned = 0
            weighted_score = 0

        return {
            "dominant_direction": dominant,
            "aligned_count": aligned,
            "total_directional": len(directional),
            "total_signals": len(votes),
            "bear_count": len(bearish),
            "bull_count": len(bullish),
            "bear_weight": bear_weight,
            "bull_weight": bull_weight,
            "weighted_score": weighted_score,
        }

    # ── TRADE RECOMMENDER (GEX-aware + IV-aware) ────────────

    def _recommend_trade(
        self,
        direction: str,
        conviction: Conviction,
        gex: Optional[GEXData],
        high_iv: Optional[HighIVData],
        regime: MarketRegime,
    ) -> tuple[TradeType, Optional[str], Optional[str], float, float]:
        """
        Select trade type based on:
        1. Direction from confluence
        2. Gamma regime (positive = sell premium, negative = buy premium)
        3. IV environment (high = credit spreads, low = debit spreads)
        4. GEX levels for strike selection

        Returns: (trade_type, direction, structure, max_risk_pct, size_modifier)
        """
        if conviction == Conviction.LOW:
            return (TradeType.NO_TRADE, None, None, 0.0, 1.0)

        spy = regime.spy_price
        is_positive_gamma = gex and gex.gamma_regime == GammaRegime.POSITIVE
        is_negative_gamma = gex and gex.gamma_regime == GammaRegime.NEGATIVE
        sell_premium = high_iv and high_iv.favor_selling_premium

        # Size modifier: negative gamma = reduce size (volatile)
        size_mod = 0.5 if is_negative_gamma else 1.0

        # Risk allocation by conviction
        risk_map = {
            Conviction.MAXIMUM: 2.5,
            Conviction.HIGH: 2.0,
            Conviction.MEDIUM: 1.5,
        }
        max_risk = risk_map.get(conviction, 1.0)

        # ── Use GEX levels for strike selection ──
        cr = gex.call_resistance if gex else None
        ps = gex.put_support if gex else None

        if direction == "BEARISH":
            if is_positive_gamma and sell_premium:
                # POSITIVE GAMMA + HIGH IV = sell call credit spread
                short_strike = cr if cr else round((spy + 10) / 5) * 5
                long_strike = short_strike + 10
                return (
                    TradeType.BEAR_CALL_CREDIT, "SHORT",
                    f"SPY SELL {short_strike}C / BUY {long_strike}C credit spread — "
                    f"next monthly (CR at ${cr})" if cr else
                    f"SPY SELL {short_strike}C / BUY {long_strike}C credit spread — next monthly",
                    max_risk, size_mod
                )
            elif is_negative_gamma:
                # NEGATIVE GAMMA = trending — buy put debit spread
                long_strike = round(spy / 5) * 5
                short_strike = ps if ps else long_strike - 20
                short_strike = round(short_strike / 5) * 5
                return (
                    TradeType.BEAR_PUT_SPREAD, "SHORT",
                    f"SPY BUY {long_strike}P / SELL {short_strike}P debit spread — "
                    f"next monthly (PS at ${ps})" if ps else
                    f"SPY BUY {long_strike}P / SELL {short_strike}P debit spread — next monthly",
                    max_risk, size_mod
                )
            else:
                long_strike = round(spy / 5) * 5
                short_strike = long_strike - 15
                return (
                    TradeType.BEAR_PUT_SPREAD, "SHORT",
                    f"SPY BUY {long_strike}P / SELL {short_strike}P debit spread — next monthly",
                    max_risk, size_mod
                )

        elif direction == "BULLISH":
            if is_positive_gamma and sell_premium:
                # POSITIVE GAMMA + HIGH IV = sell put credit spread
                short_strike = ps if ps else round((spy - 10) / 5) * 5
                long_strike = short_strike - 10
                return (
                    TradeType.BULL_PUT_CREDIT, "LONG",
                    f"SPY SELL {short_strike}P / BUY {long_strike}P credit spread — "
                    f"next monthly (PS at ${ps})" if ps else
                    f"SPY SELL {short_strike}P / BUY {long_strike}P credit spread — next monthly",
                    max_risk, size_mod
                )
            elif is_negative_gamma:
                # NEGATIVE GAMMA = trending — buy call debit spread
                long_strike = round(spy / 5) * 5
                short_strike = cr if cr else long_strike + 20
                short_strike = round(short_strike / 5) * 5
                return (
                    TradeType.BULL_CALL_SPREAD, "LONG",
                    f"SPY BUY {long_strike}C / SELL {short_strike}C debit spread — "
                    f"next monthly (CR at ${cr})" if cr else
                    f"SPY BUY {long_strike}C / SELL {short_strike}C debit spread — next monthly",
                    max_risk, size_mod
                )
            else:
                long_strike = round(spy / 5) * 5
                short_strike = long_strike + 15
                return (
                    TradeType.BULL_CALL_SPREAD, "LONG",
                    f"SPY BUY {long_strike}C / SELL {short_strike}C debit spread — next monthly",
                    max_risk, size_mod
                )

        else:
            # NEUTRAL with conviction? Iron condor using GEX levels
            if is_positive_gamma and sell_premium:
                put_short = ps if ps else round((spy - 15) / 5) * 5
                put_long = put_short - 5
                call_short = cr if cr else round((spy + 15) / 5) * 5
                call_long = call_short + 5
                return (
                    TradeType.IRON_CONDOR, None,
                    f"SPY {put_long}/{put_short}P — {call_short}/{call_long}C iron condor — "
                    f"next monthly (GEX range ${ps}-${cr})" if ps and cr else
                    f"SPY {put_long}/{put_short}P — {call_short}/{call_long}C iron condor — next monthly",
                    max_risk * 0.7, size_mod
                )
            return (TradeType.NO_TRADE, None, None, 0.0, 1.0)

    # ── MAIN INTERPRET METHOD ────────────────────────────────

    def interpret(
        self,
        volume_spikes: list[VolumeSpike],
        options_flows: list[OptionsFlow],
        regime: MarketRegime,
        gex: Optional[GEXData] = None,
        high_iv: Optional[HighIVData] = None,
        congress_trades: list[CongressTrade] = None,
        arena: Optional[ArenaConfidence] = None,
        vol_surface: Optional[VolSurfaceData] = None,
        watchlist: list[str] = None,
    ) -> UhuraSignal:
        """
        🎯 Main entry point — Full Spectrum Analysis.

        Step 1: Collect votes from every signal source
        Step 2: Calculate confluence (how many agree?)
        Step 3: Only trade if 4+ signals align (THE 86% FILTER)
        Step 4: Use GEX + IV to select optimal trade structure
        Step 5: Package into UhuraSignal for Spock
        """
        logger.info("📡 Uhura v2 — Full spectrum scan initiated...")

        # ── Step 1: Collect all votes ──
        votes = [
            self._vote_volume_spikes(volume_spikes),
            self._vote_options_flow(options_flows),
            self._vote_gex(gex, regime.spy_price),
            self._vote_regime(regime),
            self._vote_congress(congress_trades or [], watchlist),
            self._vote_arena(arena),
            self._vote_high_iv(high_iv),
        ]

        for v in votes:
            logger.info(f"  {v}")

        # ── Step 2: Calculate confluence ──
        conf = self._calculate_confluence(votes)
        logger.info(f"  CONFLUENCE: {conf['aligned_count']} aligned "
                     f"({conf['dominant_direction']}) | "
                     f"Bears:{conf['bear_count']} Bulls:{conf['bull_count']} | "
                     f"Weighted: {conf['weighted_score']:.1f}")

        # ── Step 3: Determine conviction (THE 86% FILTER) ──
        aligned = conf["aligned_count"]
        weighted = conf["weighted_score"]

        if aligned >= 6 and weighted >= 6.0:
            conviction = Conviction.MAXIMUM
        elif aligned >= 5 and weighted >= 4.0:
            conviction = Conviction.HIGH
        elif aligned >= self.MIN_CONFLUENCE and weighted >= 3.0:
            conviction = Conviction.MEDIUM
        else:
            conviction = Conviction.LOW
            logger.info(f"  ⚠️  CONFLUENCE TOO LOW ({aligned} < {self.MIN_CONFLUENCE}) — NO TRADE")

        # ── Step 4: Trade recommendation ──
        direction = conf["dominant_direction"]
        trade_type, trade_dir, structure, max_risk, size_mod = self._recommend_trade(
            direction, conviction, gex, high_iv, regime
        )

        # ── Step 5: Build flow bias ──
        if direction == "BEARISH":
            flow_bias = FlowBias.STRONG_BEARISH if weighted >= 6 else FlowBias.BEARISH
        elif direction == "BULLISH":
            flow_bias = FlowBias.STRONG_BULLISH if weighted >= 6 else FlowBias.BULLISH
        else:
            flow_bias = FlowBias.NEUTRAL

        # ── Step 6: Build reasoning ──
        reasoning_parts = []
        for v in votes:
            if v.direction != "NEUTRAL":
                reasoning_parts.append(f"[{v.source}] {v.reasoning}")

        if conviction == Conviction.LOW:
            reasoning_parts.append(
                f"FILTERED: Only {aligned}/{len(votes)} signals aligned. "
                f"Need {self.MIN_CONFLUENCE}+ for trade. PATIENCE IS THE EDGE."
            )

        all_tickers = list(set(
            [s.ticker for s in volume_spikes] +
            [f.ticker for f in options_flows if f.has_uoa]
        ))

        gamma_regime = gex.gamma_regime if gex else GammaRegime.NEUTRAL

        signal = UhuraSignal(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            flow_bias=flow_bias,
            conviction=conviction,
            recommended_trade=trade_type,
            reasoning=" | ".join(reasoning_parts),
            tickers_flagged=all_tickers,
            regime_context=f"{regime.trend} | VIX {regime.vix} | SPY ${regime.spy_price}",
            gamma_regime=gamma_regime,
            total_signals=len(votes),
            aligned_signals=aligned,
            signal_votes=votes,
            suggested_ticker="SPY",
            suggested_direction=trade_dir,
            suggested_structure=structure,
            max_risk_pct=max_risk,
            position_size_modifier=size_mod,
            gex_support=gex.put_support if gex else None,
            gex_resistance=gex.call_resistance if gex else None,
        )

        logger.info(f"📡 Uhura v2 signal: {flow_bias.value} | {conviction.value} | "
                     f"{trade_type.value}")
        return signal


# ─── HELPERS: Parse data from TradeMinds endpoints ───────────

def parse_volume_spikes(raw_alerts: list[dict]) -> list[VolumeSpike]:
    return [
        VolumeSpike(
            ticker=a["ticker"],
            multiplier=a["multiplier"],
            price=a["price"],
            timestamp=a["timestamp"],
        )
        for a in raw_alerts
    ]


def parse_options_flows(raw_flows: list[dict]) -> list[OptionsFlow]:
    return [
        OptionsFlow(
            ticker=f["ticker"],
            call_volume=f["call_volume"],
            put_volume=f["put_volume"],
            pc_ratio=f["pc_ratio"],
            has_uoa=f.get("has_uoa", False),
        )
        for f in raw_flows
    ]


def parse_gex_data(raw: dict) -> GEXData:
    """Parse from /api/gamma-environment + /api/market/gex/SPY"""
    regime_str = raw.get("gamma_regime", "NEUTRAL").upper()
    regime = GammaRegime.POSITIVE if "POS" in regime_str else (
        GammaRegime.NEGATIVE if "NEG" in regime_str else GammaRegime.NEUTRAL
    )
    return GEXData(
        gamma_regime=regime,
        gex_flip_level=raw.get("gex_flip"),
        call_resistance=raw.get("call_resistance") or raw.get("CR"),
        put_support=raw.get("put_support") or raw.get("PS"),
        hvl=raw.get("hvl") or raw.get("HVL"),
        top_gamma_strikes=raw.get("top_strikes", []),
    )


def parse_high_iv(raw: dict) -> HighIVData:
    """Parse from /api/high-iv"""
    return HighIVData(
        vix=raw.get("vix", 0),
        vix_percentile=raw.get("vix_percentile"),
        high_iv_tickers=raw.get("tickers", []),
    )


def parse_congress_trades(raw_list: list[dict]) -> list[CongressTrade]:
    """Parse from Congress Tracker endpoint"""
    return [
        CongressTrade(
            politician=t.get("politician", t.get("name", "Unknown")),
            party=t.get("party", "?"),
            ticker=t.get("ticker", t.get("symbol", "?")),
            action=t.get("action", t.get("type", "?")).upper(),
            size=t.get("size", t.get("amount", "?")),
            date=t.get("date", t.get("filed_date", "?")),
            source=t.get("source", "Unknown"),
        )
        for t in raw_list
    ]


def parse_arena_confidence(raw: dict) -> ArenaConfidence:
    """Parse from /api/arena/confidence"""
    return ArenaConfidence(
        bullish_models=raw.get("bullish", []),
        bearish_models=raw.get("bearish", []),
        neutral_models=raw.get("neutral", []),
    )


# ─── LIVE DATA ADAPTER ─────────────────────────��─────────────

def build_gex_from_gamma_env(env: dict) -> GEXData:
    """
    Convert /api/gamma-environment response to GEXData.
    Keys: environment, magnets (list of {type, strike, ...}), gamma_flip, spot
    """
    env_str = env.get("environment", "unknown").lower()
    if env_str == "positive":
        regime = GammaRegime.POSITIVE
    elif env_str == "negative":
        regime = GammaRegime.NEGATIVE
    else:
        regime = GammaRegime.NEUTRAL

    spot = env.get("spot", 0)
    magnets = env.get("magnets", [])

    # Extract nearest call wall above spot as resistance
    call_walls = sorted(
        [m["strike"] for m in magnets if m.get("type") == "call_wall" and m.get("strike", 0) >= spot],
        key=lambda x: x
    )
    put_walls = sorted(
        [m["strike"] for m in magnets if m.get("type") == "put_wall" and m.get("strike", 0) <= spot],
        key=lambda x: -x
    )

    call_resistance = call_walls[0] if call_walls else None
    put_support = put_walls[0] if put_walls else None

    return GEXData(
        gamma_regime=regime,
        gex_flip_level=env.get("gamma_flip"),
        call_resistance=call_resistance,
        put_support=put_support,
        hvl=None,
        top_gamma_strikes=[{"strike": m["strike"], "gex_value": m.get("gex", 0)} for m in magnets],
    )


def build_high_iv_from_scanner(raw: dict) -> HighIVData:
    """Convert /api/high-iv response to HighIVData."""
    opportunities = raw.get("opportunities", [])
    tickers = [{"ticker": o["symbol"], "iv_rank": o["iv_rank"]} for o in opportunities if "symbol" in o]
    return HighIVData(
        vix=raw.get("vix", 0),
        vix_percentile=raw.get("vix_percentile"),
        high_iv_tickers=tickers,
    )


def build_options_flows_from_skew(skew_list: list[dict]) -> list[OptionsFlow]:
    """Convert /api/put-call-skew response to OptionsFlow list."""
    flows = []
    for s in skew_list:
        if not s:
            continue
        flows.append(OptionsFlow(
            ticker=s.get("symbol", "?"),
            call_volume=int(s.get("total_call_oi", 0)),
            put_volume=int(s.get("total_put_oi", 0)),
            pc_ratio=float(s.get("pc_ratio", 1.0)),
            has_uoa=False,
        ))
    return flows


def build_congress_from_tracker(raw: dict) -> list[CongressTrade]:
    """Convert /api/congress/trades response to CongressTrade list."""
    trades = raw.get("trades", []) if isinstance(raw, dict) else raw
    result = []
    for t in trades:
        tx = t.get("transaction", t.get("action", "")).upper()
        if "PURCHASE" in tx or tx == "BUY":
            action = "BUY"
        elif "SALE" in tx or tx == "SELL":
            action = "SELL"
        else:
            continue  # skip exchanges, options exercises, etc.
        result.append(CongressTrade(
            politician=t.get("politician", "Unknown"),
            party=t.get("party", "?"),
            ticker=t.get("ticker", "?"),
            action=action,
            size=t.get("amount_range", t.get("size", "?")),
            date=t.get("transaction_date", t.get("date", "?")),
            source=t.get("source", "Unknown"),
        ))
    return result


def build_arena_from_confidence(raw: dict) -> ArenaConfidence:
    """
    Convert /api/arena/confidence response to ArenaConfidence.
    Response is {player_id: {symbol: {stance, signal, confidence, ...}}}
    Aggregate each model's majority stance across all symbols.
    """
    bullish_models, bearish_models, neutral_models = [], [], []

    for player_id, stances in raw.items():
        if not isinstance(stances, dict):
            continue
        bull_count = sum(1 for s in stances.values() if s.get("stance") == "bullish")
        bear_count = sum(1 for s in stances.values() if s.get("stance") == "bearish")
        total = bull_count + bear_count + sum(1 for s in stances.values() if s.get("stance") == "neutral")
        if total == 0:
            continue
        # Use display_name or player_id as model name
        name = player_id.replace("-", " ").title()
        if bull_count > bear_count and bull_count / total >= 0.5:
            bullish_models.append(name)
        elif bear_count > bull_count and bear_count / total >= 0.5:
            bearish_models.append(name)
        else:
            neutral_models.append(name)

    return ArenaConfidence(
        bullish_models=bullish_models,
        bearish_models=bearish_models,
        neutral_models=neutral_models,
    )


def build_volume_spikes_from_alerts(alerts: list[dict]) -> list[VolumeSpike]:
    """Convert /api/dynamic-alerts/active volume_spike alerts to VolumeSpike list."""
    spikes = []
    for a in alerts:
        if a.get("type") != "volume_spike":
            continue
        spikes.append(VolumeSpike(
            ticker=a.get("symbol", "?"),
            multiplier=float(a.get("vol_ratio", 2.0)),
            price=float(a.get("price", 0)),
            timestamp=a.get("triggered_at", ""),
        ))
    return spikes


def build_regime_from_detector(raw: dict) -> MarketRegime:
    """Convert /api/regime/raw response to MarketRegime."""
    return MarketRegime(
        trend=raw.get("regime", "CHOPPY"),
        spy_price=float(raw.get("spy_price", 0)),
        spy_vs_50ma_pct=float(raw.get("spy_vs_50ma", 0)),
        spy_vs_200ma_pct=float(raw.get("spy_vs_200ma", 0)),
        vix=float(raw.get("vix", 20)),
    )


# ─── DEMO: Run with today's actual data ─────────────────────

def demo_with_todays_data():
    """
    🎯 Demo using the exact data from Steve's dashboard (March 30, 2026).
    Shows how the full spectrum analysis works.
    """

    # Volume spikes from Whale Tracker
    raw_spikes = [
        {"ticker": "QQQ",  "multiplier": 2.7, "price": 557.57, "timestamp": "14:09:11"},
        {"ticker": "QQQ",  "multiplier": 2.0, "price": 557.46, "timestamp": "13:58:04"},
        {"ticker": "QQQ",  "multiplier": 2.0, "price": 557.55, "timestamp": "13:44:44"},
        {"ticker": "QQQ",  "multiplier": 2.0, "price": 557.46, "timestamp": "13:11:40"},
        {"ticker": "QQQ",  "multiplier": 3.2, "price": 557.86, "timestamp": "13:04:35"},
        {"ticker": "TSLA", "multiplier": 2.1, "price": 355.30, "timestamp": "13:03:50"},
        {"ticker": "NVDA", "multiplier": 2.1, "price": 165.12, "timestamp": "13:01:50"},
        {"ticker": "ONDS", "multiplier": 2.2, "price": 8.14,   "timestamp": "13:00:36"},
        {"ticker": "META", "multiplier": 2.1, "price": 536.23, "timestamp": "12:59:51"},
        {"ticker": "AVGO", "multiplier": 2.0, "price": 293.36, "timestamp": "12:59:47"},
    ]

    # Options flow from UOA table
    raw_flows = [
        {"ticker": "GOOGL", "call_volume": 38719,  "put_volume": 27249,  "pc_ratio": 0.70, "has_uoa": False},
        {"ticker": "META",  "call_volume": 157374, "put_volume": 156683, "pc_ratio": 1.00, "has_uoa": True},
        {"ticker": "TQQQ",  "call_volume": 133068, "put_volume": 81279,  "pc_ratio": 0.61, "has_uoa": False},
    ]

    # GEX data from /api/gamma-environment + /api/market/gex/SPY
    gex = GEXData(
        gamma_regime=GammaRegime.NEGATIVE,
        gex_flip_level=635.0,
        call_resistance=645.0,
        put_support=620.0,
        hvl=632.0,
        top_gamma_strikes=[
            {"strike": 630, "gex": -1200000000},
            {"strike": 640, "gex": -800000000},
            {"strike": 620, "gex": -600000000},
        ],
    )

    # High IV data
    high_iv = HighIVData(
        vix=30.53,
        vix_percentile=82,
        high_iv_tickers=[
            {"ticker": "TSLA", "iv_rank": 88},
            {"ticker": "NVDA", "iv_rank": 76},
            {"ticker": "META", "iv_rank": 71},
        ],
    )

    # Congress trades (recent)
    congress = [
        CongressTrade("Tommy Tuberville", "R", "NVDA", "SELL", "$50K-$100K", "2026-03-27", "Capitol"),
        CongressTrade("Nancy Pelosi", "D", "GOOGL", "BUY", "$250K-$500K", "2026-03-25", "Quiver"),
        CongressTrade("Dan Crenshaw", "R", "MSFT", "SELL", "$15K-$50K", "2026-03-26", "Capitol"),
    ]

    # Arena confidence (what the crew thinks)
    arena = ArenaConfidence(
        bullish_models=["Gemma3_4B"],
        bearish_models=["Claude_Sonnet", "GPT_o3", "Gemini_2.5_Pro", "Mr_Dalio"],
        neutral_models=["Claude_Haiku", "GPT_4o"],
    )

    # Market regime from CTO Advisory
    regime = MarketRegime(
        trend="BEAR_TREND",
        spy_price=631.97,
        spy_vs_50ma_pct=-6.83,
        spy_vs_200ma_pct=-4.51,
        vix=30.53,
        oil_price=69.50,
        oil_change_pct=-0.8,
        dxy_change_pct=0.2,
        catalysts=["NVDA GTC fallout", "MU earnings reaction", "FOMC positioning"],
    )

    # Parse
    spikes = parse_volume_spikes(raw_spikes)
    flows = parse_options_flows(raw_flows)

    # Create Uhura v2 and interpret
    uhura = LtUhura(account_size=25000)
    signal = uhura.interpret(
        volume_spikes=spikes,
        options_flows=flows,
        regime=regime,
        gex=gex,
        high_iv=high_iv,
        congress_trades=congress,
        arena=arena,
        watchlist=["SPY", "QQQ", "NVDA", "META", "TSLA", "GOOGL", "AVGO", "AMD"],
    )

    print(signal)
    print()
    print("=" * 58)
    print("Signal dict (for Spock integration):")
    for k, v in signal.to_dict().items():
        if k == "signal_votes":
            print(f"  {k}:")
            for vote in v:
                print(f"    {vote}")
        else:
            print(f"  {k}: {v}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    demo_with_todays_data()
