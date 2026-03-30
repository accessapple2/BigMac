"""Scan Context Builder — assembles rich data package for every AI model's scan.

Pulls from existing engine modules (VIX, regime, sentiment, options flow,
earnings, whisper, cross-asset, confidence, ghost trades) and formats into
a structured text block that sits at the top of every model's prompt.
"""
from __future__ import annotations
from rich.console import Console

console = Console()

# Energy/commodity tickers for Arnold Energy model
ENERGY_TICKERS = {"XOM", "CVX", "COP", "OXY", "DVN", "EOG", "FANG", "MPC",
                  "XLE", "XOP", "OIH", "USO", "CCJ", "FCX", "NEM", "CLF"}


def build_scan_context(prices: dict, indicators: dict, player_id: str = "") -> str:
    """Build the shared data context block for all models.

    Returns a formatted text block with market regime, per-stock technicals,
    options data, catalysts, and cross-model intelligence.
    """
    sections = []

    # === MARKET REGIME ===
    sections.append(_build_regime_block())

    # === SECTOR PERFORMANCE (V3) ===
    sections.append(_build_sector_performance_block(prices))

    # === WATCHLIST ===
    sections.append(_build_watchlist_block(prices, indicators, player_id))

    # === CATALYSTS ===
    sections.append(_build_catalyst_block())

    # === OPTIONS DATA ===
    sections.append(_build_options_block(player_id))

    # === ARENA INTELLIGENCE ===
    sections.append(_build_arena_intel_block(player_id))

    # === TACTICAL DISPLAY (Warp 10 regime allocation) ===
    try:
        from engine.warp10_engine import get_current_allocation, REGIME_ALLOCATIONS
        ra = get_current_allocation()
        regime = ra.get("regime", "CAUTIOUS")
        alloc = ra.get("allocation", {})
        tac = [f"\n=== 🎯 TACTICAL DISPLAY — Regime: {regime} ==="]
        tac.append(f"{alloc.get('description', '')}")
        tac.append(f"VIX: {ra.get('vix', '?')} | SPY: ${ra.get('spy_price', '?')}")
        tac.append(f"Allocation: Long Eq {alloc.get('long_equity',0)*100:.0f}% | "
                   f"Bear Call Spreads {alloc.get('bear_call_spreads',0)*100:.0f}% | "
                   f"Iron Condors {alloc.get('iron_condors',0)*100:.0f}% | "
                   f"Shorts {alloc.get('short_equity',0)*100:.0f}% | "
                   f"Cash {alloc.get('cash',0)*100:.0f}%")
        if regime == "BULL":
            tac.append("STANDING ORDER: Focus on LONG EQUITY from Chekov's scanner. Let winners run.")
        elif regime == "CAUTIOUS":
            tac.append("STANDING ORDER: Balanced — selective longs + premium selling. Tighter stops.")
        elif regime == "BEAR":
            tac.append("STANDING ORDER: DEFENSIVE — minimal new longs, focus on bear call spreads + shorts.")
        elif regime == "CRISIS":
            tac.append("STANDING ORDER: BATTLESTATIONS — 50% cash, only shorts + premium selling.")
        sections.append("\n".join(tac))
    except Exception:
        pass

    # === EARNINGS CATALYST (Warp 10 Final) ===
    try:
        from engine.earnings_catalyst import build_earnings_catalyst_section
        earn_block = build_earnings_catalyst_section()
        if earn_block:
            sections.append(earn_block)
    except Exception:
        pass

    # === UNIVERSE SCANNER (Chekov's nightly sweep) ===
    try:
        from engine.universe_scanner import build_universe_prompt_section
        universe_block = build_universe_prompt_section()
        if universe_block:
            sections.append(universe_block)
    except Exception:
        pass

    # === STRATEGY CONVERGENCE (Holly-style multi-strategy signals) ===
    try:
        from engine.strategies import build_strategy_prompt_section
        strat_block = build_strategy_prompt_section()
        if strat_block:
            sections.append(strat_block)
    except Exception:
        pass

    # === RALLIES ARENA INTEL (External AI Competition) ===
    try:
        from engine.rallies_intel import build_rallies_intel_block
        rallies_block = build_rallies_intel_block()
        if rallies_block:
            sections.append(rallies_block)
    except Exception:
        pass

    return "\n\n".join(s for s in sections if s)


def _build_sector_performance_block(prices: dict) -> str:
    """V3: Sector performance rankings so models know what's working."""
    if not prices:
        return ""
    try:
        from engine.sector_tracker import get_sector_rotation
        sectors = get_sector_rotation(prices)
        if not sectors:
            return ""

        sectors.sort(key=lambda s: s["avg_change_pct"], reverse=True)
        outperforming = [s for s in sectors if s["avg_change_pct"] > 0.3 and s["sector"] != "Index"]
        underperforming = [s for s in sectors if s["avg_change_pct"] < -0.3 and s["sector"] != "Index"]

        lines = ["=== SECTOR PERFORMANCE (V3) ==="]
        if outperforming:
            out_str = ", ".join(f"{s['sector']} ({s['avg_change_pct']:+.1f}%)" for s in outperforming)
            lines.append(f"OUTPERFORMING: {out_str}")
        if underperforming:
            under_str = ", ".join(f"{s['sector']} ({s['avg_change_pct']:+.1f}%)" for s in underperforming)
            lines.append(f"UNDERPERFORMING: {under_str}")

        if outperforming:
            hot_symbols = []
            for s in outperforming:
                hot_symbols.extend(s["symbols"])
            lines.append(f"FOCUS: Only trade stocks in outperforming sectors: {', '.join(hot_symbols)}")
        elif not underperforming:
            lines.append("FOCUS: No clear sector leadership — be selective or stay in cash")
        else:
            lines.append("FOCUS: All sectors weak — stay in cash or trade only highest conviction")

        return "\n".join(lines)
    except Exception:
        return ""


def _build_regime_block() -> str:
    """Market regime: VIX, SPY/QQQ, crude oil, sentiment, flow."""
    lines = ["=== MARKET REGIME ==="]

    # VIX
    try:
        from engine.vix_monitor import get_vix_status
        vix = get_vix_status()
        if vix and vix.get("price"):
            lines.append(f"VIX: {vix['price']:.2f} ({vix.get('change_pct', 0):+.1f}%)")
    except Exception:
        pass

    # Regime
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        if regime:
            lines.append(f"Regime: {regime.get('regime', '?')} — {regime.get('description', '')}")
            lines.append(f"Aggression: {regime.get('aggression_modifier', 1.0):.1f}")
    except Exception:
        pass

    # SPY/QQQ from prices cache or direct fetch
    try:
        from engine.market_data import get_stock_price
        for sym in ("SPY", "QQQ"):
            d = get_stock_price(sym)
            if d and "price" in d:
                lines.append(f"{sym}: ${d['price']:.2f} ({d.get('change_pct', 0):+.2f}%)")
    except Exception:
        pass

    # Crude oil
    try:
        from engine.market_data import get_stock_price
        oil = get_stock_price("CL=F")
        if oil and "price" in oil:
            lines.append(f"Crude Oil (WTI): ${oil['price']:.2f} ({oil.get('change_pct', 0):+.2f}%)")
    except Exception:
        pass

    # Options flow lean
    try:
        from engine.market_flow import get_flow_lean
        fl = get_flow_lean()
        if fl:
            lines.append(f"Options Flow: {fl['lean']} (conviction: {fl.get('conviction', 0)})")
    except Exception:
        pass

    # Cross-asset sizing
    try:
        from engine.cross_asset import get_vix_sizing_factor
        factor = get_vix_sizing_factor()
        if factor < 1.0:
            lines.append(f"Position Sizing Multiplier: {factor:.2f}x (VIX elevated)")
    except Exception:
        pass

    return "\n".join(lines) if len(lines) > 1 else ""


def _build_watchlist_block(prices: dict, indicators: dict, player_id: str) -> str:
    """Per-stock summary: price, change%, RSI, relative volume, sector."""
    if not prices:
        return ""

    # Filter to energy tickers for Arnold
    if player_id == "energy-arnold":
        filtered = {s: d for s, d in prices.items() if s in ENERGY_TICKERS}
        if filtered:
            prices = filtered

    lines = [f"=== WATCHLIST ({len(prices)} stocks) ==="]

    # Earnings data for per-stock annotation
    earnings_map = {}
    try:
        from engine.earnings_calendar import get_earnings_warnings
        from config import WATCH_STOCKS
        warnings = get_earnings_warnings(list(prices.keys()))
        for e in warnings:
            earnings_map[e["symbol"]] = e.get("days_until", "?")
    except Exception:
        pass

    for symbol, data in sorted(prices.items()):
        price = data.get("price", 0)
        change = data.get("change_pct", 0)
        ind = indicators.get(symbol, {})
        rsi = ind.get("rsi", "–")
        vol_ratio = ind.get("volume_ratio", "–")

        parts = [f"{symbol} | ${price:.2f} | {change:+.2f}%"]
        if rsi != "–":
            rsi_tag = " OVERSOLD" if rsi < 30 else " OVERBOUGHT" if rsi > 70 else ""
            parts.append(f"RSI:{rsi:.0f}{rsi_tag}")
        if vol_ratio != "–":
            parts.append(f"RelVol:{vol_ratio:.1f}x")

        # Earnings tag
        days = earnings_map.get(symbol)
        if days is not None and days <= 14:
            if days <= 3:
                parts.append(f"⚡ EARNINGS IN {days}d")
            else:
                parts.append(f"Earnings:{days}d")

        lines.append("  " + " | ".join(parts))

    return "\n".join(lines)


def _build_catalyst_block() -> str:
    """Upcoming catalysts: earnings, events."""
    lines = ["=== CATALYSTS NEXT 14 DAYS ==="]

    try:
        from engine.earnings_hub import get_earnings_countdown
        earnings = get_earnings_countdown(days_ahead=14)
        if earnings:
            for e in earnings[:10]:
                sym = e.get("symbol", "?")
                date = e.get("date", "?")
                days = e.get("days_until", "?")
                timing = e.get("timing", "")
                surprise = e.get("last_surprise_pct")
                tag = f" (last surprise: {surprise:+.1f}%)" if surprise else ""
                lines.append(f"  {date} ({days}d): {sym} earnings {timing}{tag}")
        else:
            lines.append("  No earnings in next 14 days")
    except Exception:
        lines.append("  Earnings data unavailable")

    # Trending / whisper
    try:
        from engine.whisper_network import get_trending_tickers
        trending = get_trending_tickers()
        if trending:
            names = [f"{t['symbol']}({t['change_pct']:+.1f}%)" for t in trending[:5]]
            lines.append(f"  Trending: {', '.join(names)}")
    except Exception:
        pass

    return "\n".join(lines) if len(lines) > 1 else ""


def _build_options_block(player_id: str) -> str:
    """Options data: flow, put/call, GEX, high IV."""
    # Only include detailed options data for options-focused models
    is_options_model = player_id in ("options-sosnoff", "dayblade-0dte")

    lines = ["=== OPTIONS DATA ==="]

    try:
        from engine.market_flow import get_flow_lean
        fl = get_flow_lean()
        if fl:
            lines.append(f"  Flow Lean: {fl['lean']} | Conviction: {fl.get('conviction', 0)}")
            if fl.get("per_symbol"):
                top = sorted(fl["per_symbol"], key=lambda x: abs(x.get("net", 0)), reverse=True)[:5]
                for s in top:
                    lines.append(f"    {s.get('symbol','?')}: net ${s.get('net',0):+,.0f} ({s.get('direction','?')})")
    except Exception:
        pass

    # GEX for SPY
    try:
        from engine.gex_scanner import get_gex_magnets
        magnets = get_gex_magnets("SPY")
        if magnets:
            levels = [f"${m['strike']}" for m in magnets[:3]]
            lines.append(f"  SPY GEX Magnets: {', '.join(levels)}")
    except Exception:
        pass

    # High IV stocks (detailed for options models)
    if is_options_model:
        try:
            from engine.market_data import get_stock_price
            # High IV data is typically computed in the dashboard; simplified here
            lines.append("  [Detailed IV data in per-stock analysis]")
        except Exception:
            pass

    return "\n".join(lines) if len(lines) > 1 else ""


def _build_arena_intel_block(player_id: str) -> str:
    """Cross-model intelligence: stances, consensus, ghost trades."""
    lines = ["=== ARENA INTELLIGENCE ==="]

    # Consensus signals (3+ models agree)
    try:
        from engine.signal_tracker import get_consensus_signals
        consensus = get_consensus_signals()
        if consensus:
            for c in consensus[:3]:
                models = ", ".join(c.get("models", [])[:4])
                lines.append(
                    f"  🔥 CONSENSUS: {c['symbol']} — {c['model_count']} models bullish "
                    f"(avg conf: {c.get('avg_confidence', 0):.0%}) [{models}]"
                )
    except Exception:
        pass

    # Smart money signals
    try:
        from engine.smart_money import get_recent_smart_money
        sm = get_recent_smart_money(limit=3)
        if sm:
            for s in sm:
                buyers = s.get("buyers", "")
                lines.append(f"  Smart Money: {s.get('symbol','?')} — bought by {buyers}")
    except Exception:
        pass

    # Top ghost trades (models that wanted to buy but didn't)
    try:
        from engine.ghost_trades import get_ghost_trades
        ghosts = get_ghost_trades(limit=5)
        if ghosts:
            top = [g for g in ghosts if g.get("confidence", 0) >= 0.70][:3]
            for g in top:
                pnl = g.get("outcome_pnl_pct")
                pnl_str = f" (would be {pnl:+.1f}%)" if pnl is not None else ""
                lines.append(
                    f"  Ghost: {g.get('symbol','?')} conf={g.get('confidence',0):.0%}{pnl_str} "
                    f"— {(g.get('reasoning','')[:80])}"
                )
    except Exception:
        pass

    return "\n".join(lines) if len(lines) > 1 else ""


def build_earnings_alerts(prices: dict) -> str:
    """Special earnings flags: upcoming + post-earnings IV crush."""
    alerts = []
    try:
        from engine.earnings_calendar import get_earnings_warnings
        warnings = get_earnings_warnings(list(prices.keys()))
        for e in warnings:
            days = e.get("days_until", 999)
            sym = e["symbol"]
            if days <= 3:
                alerts.append(f"⚡ {sym}: EARNINGS IN {days} DAYS — catalyst play / iron condor opportunity")
    except Exception:
        pass

    # Post-earnings IV crush detection (stock reported within 24h)
    try:
        import sqlite3
        conn = sqlite3.connect("data/trader.db", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        recent = conn.execute(
            "SELECT DISTINCT symbol FROM market_news "
            "WHERE headline LIKE '%earnings%' AND fetched_at >= datetime('now', '-24 hours')"
        ).fetchall()
        conn.close()
        for r in recent:
            sym = r["symbol"]
            if sym in prices:
                alerts.append(f"📉 {sym}: POST-EARNINGS IV CRUSH — premium selling opportunity")
    except Exception:
        pass

    return "\n".join(alerts) if alerts else ""
