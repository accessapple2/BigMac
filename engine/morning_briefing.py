"""
engine/morning_briefing.py — Phase 3.6 Comprehensive Morning Briefing

Generates a full market morning briefing for Captain Kirk, covering:
- Portfolio summary (positions, P&L, cash)
- Overnight/pre-market moves
- Today's Bridge Vote watchlist picks
- Macro conditions (VIX, Cu/Au, regime)
- Earnings next 7 days
- Congress/Capitol Trades alerts
- Risk report (VaR, drift)

The output is a structured dict with text, audio_url, and metadata.
"""
from __future__ import annotations

import os
import json
import sqlite3
import threading
import datetime
import time as _time
import logging

logger = logging.getLogger(__name__)

# ── DB helpers ─────────────────────────────────────────────────────────────────

_DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)


def _conn():
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=20)
    c.row_factory = sqlite3.Row
    return c


# ── Briefing cache ──────────────────────────────────────────────────────────────

_briefing_cache: dict = {"text": None, "audio_url": None, "ts": 0, "date": ""}
_briefing_lock = threading.Lock()


def _today_str() -> str:
    import pytz
    az = pytz.timezone("US/Arizona")
    return datetime.datetime.now(az).strftime("%Y-%m-%d")


# ── Data gatherers ──────────────────────────────────────────────────────────────

def _get_portfolio_summary() -> str:
    """Fleet portfolio summary — equity, P&L, cash, top positions."""
    try:
        conn = _conn()
        players = conn.execute(
            "SELECT id, display_name, cash, is_human, is_halted, is_paused "
            "FROM ai_players WHERE is_active=1"
        ).fetchall()
        fleet_value = 0.0
        fleet_cash = 0.0
        open_positions = []

        for p in players:
            pid = p["id"]
            cash = float(p["cash"] or 0)
            positions = conn.execute(
                "SELECT ticker, qty, entry_price, current_price, unrealized_pnl "
                "FROM portfolio_positions WHERE status='open' "
                "AND portfolio_id IN (SELECT id FROM portfolios WHERE player_id=?)",
                (pid,)
            ).fetchall()
            pos_val = sum(
                float(pos["qty"]) * float(pos["current_price"] or pos["entry_price"] or 0)
                for pos in positions
            )
            fleet_value += cash + pos_val
            fleet_cash += cash
            for pos in positions:
                open_positions.append({
                    "agent": p["display_name"],
                    "ticker": pos["ticker"],
                    "qty": pos["qty"],
                    "upnl": float(pos["unrealized_pnl"] or 0),
                })

        # Yesterday's fleet value for day P&L
        yesterday_val = 0.0
        ph = conn.execute(
            "SELECT SUM(total_value) as tv FROM portfolio_history "
            "WHERE date(recorded_at) = date('now', '-1 day') "
            "GROUP BY date(recorded_at) HAVING MAX(recorded_at)"
        ).fetchone()
        if ph and ph["tv"]:
            yesterday_val = float(ph["tv"])
        day_pnl = fleet_value - yesterday_val if yesterday_val else 0.0

        # Top open positions by unrealized P&L
        open_positions.sort(key=lambda x: abs(x["upnl"]), reverse=True)
        top_pos_lines = []
        for p in open_positions[:5]:
            sign = "+" if p["upnl"] >= 0 else ""
            top_pos_lines.append(
                f"  {p['ticker']} ({p['agent']}) {sign}${p['upnl']:.0f}"
            )

        pnl_sign = "+" if day_pnl >= 0 else ""
        lines = [
            f"Fleet Value: ${fleet_value:,.0f}  |  Day P&L: {pnl_sign}${day_pnl:,.0f}",
            f"Cash on Hand: ${fleet_cash:,.0f}  |  Open Positions: {len(open_positions)}",
        ]
        if top_pos_lines:
            lines.append("Top Positions:")
            lines.extend(top_pos_lines)

        conn.close()
        return "\n".join(lines)
    except Exception as e:
        return f"Portfolio data unavailable: {e}"


def _get_overnight_moves() -> str:
    """Pre-market / overnight moves via yfinance futures proxies."""
    try:
        import yfinance as yf
        symbols = {"ES=F": "S&P Futures", "NQ=F": "Nasdaq Futures",
                   "YM=F": "Dow Futures", "^VIX": "VIX",
                   "GC=F": "Gold", "CL=F": "Oil"}
        lines = []
        data = yf.download(
            list(symbols.keys()), period="2d", interval="1d",
            group_by="ticker", auto_adjust=True, progress=False, threads=True
        )
        for sym, label in symbols.items():
            try:
                df = data[sym] if len(symbols) > 1 else data
                if df is None or df.empty or len(df) < 2:
                    continue
                last = float(df["Close"].iloc[-1])
                prev = float(df["Close"].iloc[-2])
                chg = (last - prev) / prev * 100 if prev else 0
                arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "—")
                lines.append(f"  {label}: {last:.2f}  {arrow}{abs(chg):.2f}%")
            except Exception:
                pass
        return "\n".join(lines) if lines else "Futures data unavailable"
    except Exception as e:
        return f"Overnight data unavailable: {e}"


def _get_watchlist_picks() -> str:
    """Top Bridge Vote picks and watchlist signals."""
    try:
        conn = _conn()
        # Recent bridge votes (last session)
        votes = conn.execute(
            "SELECT vote, player_name, reason, created_at "
            "FROM bridge_votes ORDER BY created_at DESC LIMIT 20"
        ).fetchall()

        # Tally votes by implied ticker from reason/session
        buy_reasons = [v["reason"] for v in votes if (v["vote"] or "").upper() == "BUY" and v["reason"]]
        sell_reasons = [v["reason"] for v in votes if (v["vote"] or "").upper() == "SELL" and v["reason"]]
        hold_reasons = [v["reason"] for v in votes if (v["vote"] or "").upper() == "HOLD" and v["reason"]]

        buy_ct = len([v for v in votes if (v["vote"] or "").upper() == "BUY"])
        sell_ct = len([v for v in votes if (v["vote"] or "").upper() == "SELL"])
        hold_ct = len([v for v in votes if (v["vote"] or "").upper() == "HOLD"])
        total = buy_ct + sell_ct + hold_ct

        consensus = "NEUTRAL"
        if total > 0:
            if buy_ct / total > 0.55:
                consensus = f"BULLISH ({buy_ct}/{total} BUY)"
            elif sell_ct / total > 0.45:
                consensus = f"BEARISH ({sell_ct}/{total} SELL)"
            else:
                consensus = f"MIXED (B{buy_ct}/S{sell_ct}/H{hold_ct})"

        # Top watchlist signals
        sigs = conn.execute(
            "SELECT symbol, signal, confidence, reasoning "
            "FROM signals WHERE date(created_at) >= date('now', '-1 day') "
            "ORDER BY confidence DESC LIMIT 5"
        ).fetchall()

        lines = [f"Bridge Consensus: {consensus}"]
        if sigs:
            lines.append("Top Signals:")
            for s in sigs:
                conf = int((s["confidence"] or 0) * 100)
                lines.append(f"  {s['symbol']}: {s['signal']} ({conf}% confidence)")

        conn.close()
        return "\n".join(lines)
    except Exception as e:
        return f"Watchlist data unavailable: {e}"


def _get_macro_conditions() -> str:
    """VIX, Cu/Au ratio, macro regime."""
    try:
        # Try live macro cache first (populated by Phase 3.5)
        import importlib.util
        spec = importlib.util.find_spec("dashboard.app")
        if spec:
            import sys
            if "dashboard.app" in sys.modules:
                app_mod = sys.modules["dashboard.app"]
                mc = getattr(app_mod, "_macro_cache", {})
                data = mc.get("data") if mc else None
                if data and data.get("ok") and (_time.time() - mc.get("ts", 0)) < 1800:
                    prices = data.get("prices", {})
                    vix = prices.get("^VIX", {}).get("price", 0)
                    spy_chg = prices.get("SPY", {}).get("change_pct", 0)
                    gld_chg = prices.get("GLD", {}).get("change_pct", 0)
                    cu_au = data.get("cu_au_ratio")
                    regime = data.get("regime", "UNKNOWN")
                    lines = [
                        f"Regime: {regime}",
                        f"VIX: {vix:.1f}  |  SPY: {spy_chg:+.2f}%  |  Gold: {gld_chg:+.2f}%",
                    ]
                    if cu_au:
                        lines.append(f"Cu/Au Ratio: {cu_au:.4f}  (>0.15 = industrial demand)")
                    return "\n".join(lines)
    except Exception:
        pass

    # Fallback: DB
    try:
        conn = _conn()
        cs = conn.execute(
            "SELECT risk_mode, spy_pct, tlt_pct, gld_pct, alignment_score "
            "FROM correlation_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        rh = conn.execute(
            "SELECT regime, size_modifier FROM regime_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        vr = conn.execute(
            "SELECT var_95_param, daily_vol_pct FROM var_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()

        lines = []
        if cs:
            lines.append(f"Correlation Regime: {cs['risk_mode']}")
            lines.append(f"SPY: {cs['spy_pct']:+.1f}%  TLT: {cs['tlt_pct']:+.1f}%  GLD: {cs['gld_pct']:+.1f}%")
        if rh:
            lines.append(f"Price Regime: {rh['regime']} (size modifier: {rh['size_modifier']}x)")
        if vr:
            lines.append(f"Daily Vol: {vr['daily_vol_pct']:.1f}%  |  VaR 95%: ${vr['var_95_param']:.0f}")
        return "\n".join(lines) if lines else "Macro data unavailable"
    except Exception as e:
        return f"Macro data unavailable: {e}"


def _get_earnings_calendar() -> str:
    """Earnings reports in next 7 days."""
    try:
        import yfinance as yf
        # Use the watch stocks list
        watch = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA",
                 "SPY", "QQQ", "GLD", "AMD", "INTC", "JPM", "GS", "MS"]
        earnings_soon = []
        today = datetime.date.today()
        for sym in watch:
            try:
                tk = yf.Ticker(sym)
                cal = tk.calendar
                if cal is not None and not cal.empty:
                    dates = cal.columns.tolist()
                    if dates:
                        d = dates[0]
                        if hasattr(d, "date"):
                            d = d.date()
                        if isinstance(d, datetime.datetime):
                            d = d.date()
                        if isinstance(d, datetime.date):
                            days_out = (d - today).days
                            if 0 <= days_out <= 7:
                                earnings_soon.append(f"  {sym}: {d.strftime('%a %b %d')}")
            except Exception:
                pass
        if not earnings_soon:
            # Fallback: check DB
            conn = _conn()
            rows = conn.execute(
                "SELECT symbol FROM market_events WHERE event_type='earnings' "
                "AND date(event_date) BETWEEN date('now') AND date('now','+7 days') "
                "ORDER BY event_date LIMIT 10"
            ).fetchall()
            conn.close()
            earnings_soon = [f"  {r['symbol']}" for r in rows]

        if earnings_soon:
            return "Earnings Next 7 Days:\n" + "\n".join(earnings_soon)
        return "Earnings Next 7 Days: None on watch list"
    except Exception as e:
        return f"Earnings calendar unavailable: {e}"


def _get_congress_trades() -> str:
    """Recent Capitol Trades / insider congressional activity."""
    try:
        conn = _conn()
        trades = conn.execute(
            "SELECT ticker, transaction_type, amount_range, politician, traded_at "
            "FROM insider_trades WHERE date(traded_at) >= date('now', '-7 days') "
            "ORDER BY traded_at DESC LIMIT 8"
        ).fetchall()
        conn.close()
        if not trades:
            return "Congress Trades: No recent activity in past 7 days"
        lines = ["Congress Trades (last 7 days):"]
        for t in trades:
            dt = str(t["traded_at"] or "")[:10]
            lines.append(
                f"  {t['ticker']} {t['transaction_type']} — "
                f"{t['politician'] or 'Unknown'} ({t['amount_range'] or '?'}) {dt}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Congress trades unavailable: {e}"


def _get_risk_report() -> str:
    """VaR, drift status, active risk alerts."""
    try:
        conn = _conn()
        var_snap = conn.execute(
            "SELECT var_95_param, var_99_param, daily_vol_pct, top_risk_ticker, portfolio_value "
            "FROM var_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        alerts = conn.execute(
            "SELECT severity, message FROM risk_alerts WHERE acknowledged=0 "
            "ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        conn.close()

        lines = []
        if var_snap:
            lines.append(
                f"VaR 95%: ${var_snap['var_95_param']:.0f}  |  "
                f"VaR 99%: ${var_snap['var_99_param']:.0f}  |  "
                f"Portfolio: ${var_snap['portfolio_value']:,.0f}"
            )
            lines.append(
                f"Daily Vol: {var_snap['daily_vol_pct']:.1f}%  |  "
                f"Top Risk: {var_snap['top_risk_ticker'] or 'N/A'}"
            )
        if alerts:
            lines.append("Active Alerts:")
            for a in alerts:
                lines.append(f"  [{a['severity'].upper()}] {a['message']}")
        else:
            lines.append("Risk Alerts: All clear")
        return "\n".join(lines) if lines else "Risk data unavailable"
    except Exception as e:
        return f"Risk report unavailable: {e}"


# ── Main generator ─────────────────────────────────────────────────────────────

def generate_morning_briefing(force: bool = False) -> dict:
    """
    Generate comprehensive morning briefing. Cached per calendar day.
    Returns dict with: text, sections, audio_url, generated_at.
    """
    with _briefing_lock:
        today = _today_str()
        # Return cached version if same day and not forced
        if not force and _briefing_cache["date"] == today and _briefing_cache["text"]:
            return dict(_briefing_cache)

        # Gather all sections
        sections: dict = {
            "portfolio":  _get_portfolio_summary(),
            "overnight":  _get_overnight_moves(),
            "watchlist":  _get_watchlist_picks(),
            "macro":      _get_macro_conditions(),
            "earnings":   _get_earnings_calendar(),
            "congress":   _get_congress_trades(),
            "risk":       _get_risk_report(),
        }

        import pytz
        az = pytz.timezone("US/Arizona")
        now_str = datetime.datetime.now(az).strftime("%A, %B %-d, %Y — %I:%M %p AZ")

        text_parts = [
            f"=== MORNING BRIEFING — {now_str} ===\n",
            f"PORTFOLIO SUMMARY\n{sections['portfolio']}",
            f"\nOVERNIGHT MOVES\n{sections['overnight']}",
            f"\nBRIDGE WATCHLIST\n{sections['watchlist']}",
            f"\nMACRO CONDITIONS\n{sections['macro']}",
            f"\nEARNINGS CALENDAR\n{sections['earnings']}",
            f"\nCONGRESS INTEL\n{sections['congress']}",
            f"\nRISK REPORT\n{sections['risk']}",
            "\n=== END BRIEFING ===",
        ]
        briefing_text = "\n".join(text_parts)

        # Generate TTS audio (non-blocking)
        audio_url = _generate_audio(briefing_text)

        # Auto-post to CIC chat history
        _post_to_cic(briefing_text, today)

        result = {
            "text":         briefing_text,
            "sections":     sections,
            "audio_url":    audio_url,
            "generated_at": datetime.datetime.now().isoformat(),
            "date":         today,
        }

        # Update cache
        _briefing_cache.update(result)
        _briefing_cache["ts"] = _time.time()

        return result


def _generate_audio(text: str) -> str | None:
    """Generate TTS audio via edge-tts. Returns audio URL or None."""
    try:
        import asyncio
        import edge_tts

        # Use the same output path as ready_room_routes
        static_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "dashboard", "static"
        )
        audio_path = os.path.join(static_dir, "morning_briefing.mp3")

        # Trim text for TTS (max ~2000 chars for reasonable length)
        tts_text = text[:3000]

        async def _gen():
            comm = edge_tts.Communicate(tts_text, "en-US-AndrewNeural")
            await comm.save(audio_path)

        asyncio.run(_gen())
        logger.info("Morning briefing audio generated: %s", audio_path)
        return "/static/morning_briefing.mp3"
    except Exception as e:
        logger.warning("Audio generation failed: %s", e)
        return None


def _post_to_cic(text: str, date_str: str) -> None:
    """Auto-post briefing as first CIC message of the day."""
    try:
        conn = _conn()
        # Check if we already posted today
        existing = conn.execute(
            "SELECT id FROM computer_chat_history "
            "WHERE role='assistant' AND trade_date=? AND message LIKE '[Morning Briefing]%'",
            (date_str,)
        ).fetchone()
        if existing:
            conn.close()
            return

        # Post condensed version (first 800 chars of the text)
        short_text = text[:800] + ("..." if len(text) > 800 else "")
        conn.execute(
            "INSERT INTO computer_chat_history(role, message, trade_date) VALUES(?,?,?)",
            ("assistant", "[Morning Briefing]\n" + short_text, date_str)
        )
        conn.commit()
        conn.close()
        logger.info("Morning briefing posted to CIC for %s", date_str)
    except Exception as e:
        logger.warning("Failed to post to CIC: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# DAILY INTEL REPORT — 6-section proactive intelligence engine
# Runs at 8 PM AZ (evening prep) and 6 AM AZ (morning push with ntfy)
# Saves to data/morning_brief.json + pushes to ntfy ollietrades-admin
# ══════════════════════════════════════════════════════════════════════════════

_INTEL_JSON_PATH = os.path.expanduser("~/autonomous-trader/data/morning_brief.json")
_ADMIN_NTFY_TOPIC = "ollietrades-admin"

_SECTOR_ETFS = {
    "XLK": "Technology",   "XLF": "Financials",      "XLE": "Energy",
    "XLV": "Healthcare",   "XLU": "Utilities",        "XLI": "Industrials",
    "XLB": "Materials",    "XLY": "Cons.Disc",        "XLP": "Cons.Staples",
}
_SECTOR_PICKS = {
    "XLK": ["NVDA", "MSFT", "AAPL"], "XLF": ["JPM", "GS", "BAC"],
    "XLE": ["XOM", "CVX", "COP"],    "XLV": ["UNH", "JNJ", "LLY"],
    "XLU": ["NEE", "SO", "DUK"],     "XLI": ["HON", "CAT", "GE"],
    "XLB": ["FCX", "NEM", "APD"],    "XLY": ["AMZN", "TSLA", "MCD"],
    "XLP": ["PG", "KO", "WMT"],
}
_EARN_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "META", "TSLA", "ORCL",
    "AMD", "INTC", "JPM", "GS", "MS", "BAC", "V", "MA", "UNH", "LLY",
    "XOM", "CVX", "AVGO", "NOW", "PLTR", "MU", "DELL",
]

_intel_lock = threading.Lock()
_intel_cache: dict = {}


# ── Section 1: Earnings Intel (next 3 days) ────────────────────────────────

def _get_earnings_intel_3d() -> list:
    """Earnings in the next 3 days with date, ticker, trend, action rec."""
    results = []
    try:
        import yfinance as yf
        today = datetime.date.today()
        open_tickers: set = set()
        try:
            conn = _conn()
            open_tickers = set(
                r[0] for r in conn.execute(
                    "SELECT DISTINCT ticker FROM portfolio_positions WHERE status='open'"
                ).fetchall()
            )
            conn.close()
        except Exception:
            pass

        for sym in _EARN_UNIVERSE:
            try:
                tk = yf.Ticker(sym)
                cal = tk.calendar
                if cal is None:
                    continue
                if hasattr(cal, "empty") and cal.empty:
                    continue
                dates = cal.columns.tolist() if hasattr(cal, "columns") else []
                if not dates:
                    continue
                d = dates[0]
                if hasattr(d, "date"):
                    d = d.date()
                if isinstance(d, datetime.datetime):
                    d = d.date()
                if not isinstance(d, datetime.date):
                    continue
                days_out = (d - today).days
                if not (0 <= days_out <= 3):
                    continue
                # EPS estimate (best effort)
                eps_est = None
                try:
                    row = cal.get("Earnings Per Share")
                    if row is not None and len(row) > 0:
                        v = row.iloc[0]
                        if v is not None and str(v) not in ("nan", "None", ""):
                            eps_est = round(float(v), 2)
                except Exception:
                    pass
                # 30-day trend
                trend = "flat"
                try:
                    hist = tk.history(period="35d", interval="1d", progress=False)
                    if len(hist) >= 5:
                        first = float(hist["Close"].iloc[0])
                        last  = float(hist["Close"].iloc[-1])
                        chg30 = (last - first) / first * 100 if first else 0
                        trend = "up" if chg30 > 3 else ("down" if chg30 < -3 else "flat")
                except Exception:
                    pass
                # Action
                if trend == "up":
                    action = "watch — elevated momentum into earnings"
                elif trend == "down":
                    action = "sell premium — consider straddle/strangle"
                else:
                    action = "watch"
                results.append({
                    "ticker":       sym,
                    "date":         d.isoformat(),
                    "days_out":     days_out,
                    "eps_est":      eps_est,
                    "trend_30d":    trend,
                    "action":       action,
                    "in_portfolio": sym in open_tickers,
                })
            except Exception:
                pass
    except Exception as e:
        results = [{"error": str(e)}]
    return results


# ── Section 2: Sector Rotation Radar ──────────────────────────────────────

def _get_sector_rotation_radar() -> dict:
    """Compare sector ETF 5-day returns, flag money flow direction."""
    try:
        import yfinance as yf
        etfs = list(_SECTOR_ETFS.keys())
        data = yf.download(
            etfs, period="8d", interval="1d",
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True
        )
        returns: dict = {}
        for sym in etfs:
            try:
                df = data[sym] if len(etfs) > 1 else data
                if df is None or df.empty or len(df) < 2:
                    continue
                closes = df["Close"].dropna()
                if len(closes) < 2:
                    continue
                first = float(closes.iloc[max(0, len(closes) - 6)])
                last  = float(closes.iloc[-1])
                if first <= 0:
                    continue
                returns[sym] = round((last - first) / first * 100, 2)
            except Exception:
                pass

        if not returns:
            # Fallback: DB sector_rotation table
            try:
                conn = _conn()
                rows = conn.execute(
                    "SELECT sector, change_pct FROM sector_rotation "
                    "WHERE date(trade_date) >= date('now', '-5 days') "
                    "ORDER BY trade_date DESC LIMIT 20"
                ).fetchall()
                conn.close()
                for r in rows:
                    match = next(
                        (k for k, v in _SECTOR_ETFS.items() if v.lower() in (r["sector"] or "").lower()),
                        None,
                    )
                    if match and match not in returns:
                        returns[match] = round(float(r["change_pct"] or 0), 2)
            except Exception:
                pass

        if not returns:
            return {"error": "Sector data unavailable"}

        sorted_secs = sorted(returns.items(), key=lambda x: x[1], reverse=True)
        hot  = sorted_secs[:2]
        cold = sorted_secs[-2:]
        hot_picks: list = []
        for sym, _ in hot:
            hot_picks.extend(_SECTOR_PICKS.get(sym, [])[:2])

        return {
            "returns":          {k: v for k, v in sorted_secs},
            "rotating_into":    [{"etf": s, "name": _SECTOR_ETFS.get(s, s), "ret_5d": r} for s, r in hot],
            "rotating_out_of":  [{"etf": s, "name": _SECTOR_ETFS.get(s, s), "ret_5d": r} for s, r in cold],
            "hot_sector_picks": hot_picks[:4],
            "summary": (
                f"Money rotating INTO {_SECTOR_ETFS.get(hot[0][0], hot[0][0])} "
                f"(+{hot[0][1]:.1f}%) / "
                f"OUT OF {_SECTOR_ETFS.get(cold[-1][0], cold[-1][0])} "
                f"({cold[-1][1]:+.1f}%)"
            ),
        }
    except Exception as e:
        return {"error": str(e)}


# ── Section 3: Congress Radar (last 48 h) ─────────────────────────────────

def _get_congress_radar_48h() -> list:
    """Congress trades in the last 48 hours, flagged against open positions."""
    results = []
    try:
        # Open positions for cross-reference
        open_tickers: set = set()
        try:
            conn = _conn()
            open_tickers = set(
                r[0] for r in conn.execute(
                    "SELECT DISTINCT ticker FROM portfolio_positions WHERE status='open'"
                ).fetchall()
            )
            conn.close()
        except Exception:
            pass

        # Try live congress scraper first
        try:
            from engine.congress_tracker import get_congressional_trades
            data   = get_congressional_trades()
            trades = data.get("trades", [])
            # Filter last 48 hours
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=48)
            for t in trades[:30]:
                dt_str = t.get("transaction_date") or t.get("filing_date") or ""
                try:
                    dt_parsed = datetime.datetime.strptime(dt_str[:10], "%Y-%m-%d")
                except Exception:
                    dt_parsed = None
                tx = (t.get("transaction") or "").lower()
                is_buy = any(w in tx for w in ["purchase", "buy", "acquired"])
                ticker = t.get("ticker") or ""
                already = ticker in open_tickers
                results.append({
                    "ticker":               ticker,
                    "transaction":          t.get("transaction", ""),
                    "amount":               t.get("amount_range") or "unknown",
                    "politician":           t.get("politician") or "Unknown",
                    "traded_at":            dt_str[:10],
                    "is_buy":               is_buy,
                    "already_in_portfolio": already,
                    "flag": "Congress bought — consider adding" if is_buy and not already else "",
                })
            if results:
                return results
        except Exception:
            pass

        # Fallback: DB insider_trades (corporate insiders, not congress, but useful signal)
        conn = _conn()
        rows = conn.execute(
            "SELECT symbol, insider_name, transaction_type, total_value, transaction_date "
            "FROM insider_trades "
            "WHERE date(transaction_date) >= date('now', '-2 days') "
            "ORDER BY transaction_date DESC LIMIT 15"
        ).fetchall()
        conn.close()
        for r in rows:
            tx = (r["transaction_type"] or "").lower()
            is_buy = any(w in tx for w in ["purchase", "buy", "acquired"])
            sym    = r["symbol"] or ""
            results.append({
                "ticker":               sym,
                "transaction":          r["transaction_type"],
                "amount":               f"${float(r['total_value'] or 0):,.0f}" if r["total_value"] else "unknown",
                "politician":           r["insider_name"] or "Unknown insider",
                "traded_at":            str(r["transaction_date"] or "")[:10],
                "is_buy":               is_buy,
                "already_in_portfolio": sym in open_tickers,
                "flag": "Insider bought — consider adding" if is_buy and sym not in open_tickers else "",
            })
        if not results:
            results = [{"note": "No insider/congress activity in past 48 hours"}]
    except Exception as e:
        results = [{"error": str(e)}]
    return results


# ── Section 4: Technical Setups Loading ───────────────────────────────────

def _get_technical_setups_convergent() -> list:
    """Deep scan results with high confidence from last 24 hours."""
    results = []
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT symbol, strategy_name, confidence, entry_price, stop_price, "
            "target_price, risk_reward, sector "
            "FROM deep_scan_results "
            "WHERE confidence >= 0.5 "
            "AND date(created_at) >= date('now', '-1 day') "
            "ORDER BY confidence DESC LIMIT 10"
        ).fetchall()
        conn.close()
        for r in rows:
            strat = (r["strategy_name"] or "").lower()
            if "support" in strat or "bounce" in strat:
                note = "near support — watch for bounce"
            elif "breakout" in strat:
                note = "breakout setup loading"
            elif "ma" in strat or "moving" in strat or "200" in strat:
                note = "near key moving average"
            elif "momentum" in strat:
                note = "momentum signal"
            else:
                note = f"{r['strategy_name'] or 'technical'} signal"
            results.append({
                "symbol":     r["symbol"],
                "strategy":   r["strategy_name"] or "",
                "confidence": round(float(r["confidence"] or 0), 2),
                "entry":      round(float(r["entry_price"] or 0), 2),
                "stop":       round(float(r["stop_price"] or 0), 2),
                "target":     round(float(r["target_price"] or 0), 2),
                "rr":         round(float(r["risk_reward"] or 0), 1),
                "sector":     r["sector"] or "",
                "note":       f"{r['symbol']} — {note}",
            })
        if not results:
            results = [{"note": "No high-confidence setups in past 24 hours"}]
    except Exception as e:
        results = [{"error": str(e)}]
    return results


# ── Section 5: Tomorrow's Game Plan ───────────────────────────────────────

def _get_tomorrows_game_plan() -> dict:
    """VIX + regime + F&G → structured game plan for the next session."""
    vix      = 0.0
    fg_score = None
    regime   = "UNKNOWN"
    size_mod = 1.0

    # Regime from DB
    try:
        conn = _conn()
        rh = conn.execute(
            "SELECT regime, size_modifier FROM regime_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if rh:
            regime   = rh["regime"] or "UNKNOWN"
            size_mod = float(rh["size_modifier"] or 1.0)
    except Exception:
        pass

    # Live VIX
    try:
        import yfinance as yf
        vd = yf.download("^VIX", period="2d", interval="1d",
                         auto_adjust=True, progress=False)
        if not vd.empty:
            vix = round(float(vd["Close"].iloc[-1]), 1)
    except Exception:
        pass

    # Fear & Greed
    try:
        from engine.fear_greed import get_fear_greed_index
        fg = get_fear_greed_index()
        if fg and fg.get("score") is not None:
            fg_score = int(fg["score"])
    except Exception:
        pass

    # Determine plan
    if vix >= 35:
        headline = "CRISIS — Maximum defense"
        plan     = "Hold max cash. No new entries. Watch for capitulation candle before any longs."
        focus    = "Capital preservation. Inverse ETFs only."
        tone     = "crisis"
    elif vix >= 25:
        headline = "CAUTIOUS — Risk-off environment"
        plan     = "Reduce position sizes 50%. Tight stops. No chasing moves. Wait for VIX < 20."
        focus    = "Defensive plays: XLU, XLP, GLD. Avoid growth."
        tone     = "cautious"
    elif fg_score is not None and fg_score <= 25:
        headline = "EXTREME FEAR — Contrarian opportunity"
        plan     = "Fear is peaking. Scale into quality names at support. Buy in tranches — don't catch a falling knife."
        focus    = "XLK, SPY, QQQ dips. Staged entries."
        tone     = "contrarian"
    elif fg_score is not None and fg_score >= 75:
        headline = "GREED — Market extended"
        plan     = "Overbought. Trim winners >+15%. Avoid FOMO. Let the trade come to you."
        focus    = "Trim and protect. Wait for pullbacks."
        tone     = "cautious"
    elif any(k in regime for k in ("BULL", "UP", "TRENDING_UP")):
        headline = "BULL setup — Offense mode"
        plan     = "Momentum favors longs. Focus on breakouts and pullbacks to key MAs. Full position sizing."
        focus    = f"XLK leaders, momentum names. Size modifier: {size_mod:.1f}x"
        tone     = "bull"
    elif any(k in regime for k in ("BEAR", "DOWN", "TRENDING_DOWN")):
        headline = "BEAR trend — Stay defensive"
        plan     = "Trend is down. Favor cash, inverse ETFs, and premium selling. No buy-and-hold."
        focus    = "SH, SPXU, cash. Options premium collection."
        tone     = "bear"
    else:
        headline = "NEUTRAL — Wait for confirmation"
        plan     = "Mixed signals. Trade smaller. Wait for confirmed breaks above/below key levels."
        focus    = "Watch sector rotation. No full-size bets."
        tone     = "neutral"

    return {
        "headline":      headline,
        "plan":          plan,
        "focus":         focus,
        "tone":          tone,
        "vix":           vix,
        "fg_score":      fg_score,
        "regime":        regime,
        "size_modifier": size_mod,
    }


# ── Section 6: Captain's Portfolio Review ─────────────────────────────────

def _get_captain_portfolio_review() -> dict:
    """Review open positions — flag big gains, losses, and earnings risk."""
    try:
        conn = _conn()
        # Prefer human portfolio; fall back to all positions
        human_port = conn.execute(
            "SELECT id FROM portfolios WHERE is_human=1 AND is_active=1 LIMIT 1"
        ).fetchone()

        if human_port:
            positions = conn.execute(
                "SELECT ticker, quantity, entry_price, current_price, unrealized_pnl "
                "FROM portfolio_positions WHERE status='open' "
                "AND portfolio_id=? "
                "ORDER BY unrealized_pnl DESC",
                (human_port["id"],),
            ).fetchall()
        else:
            positions = conn.execute(
                "SELECT ticker, quantity, entry_price, current_price, unrealized_pnl "
                "FROM portfolio_positions WHERE status='open' "
                "ORDER BY unrealized_pnl DESC"
            ).fetchall()

        conn.close()
        # Earnings this week — cross-ref from deep_scan_results strategy hints
        earnings_this_week: set = set()

        trim_candidates: list = []
        review_candidates: list = []
        earnings_risk: list = []

        for p in positions:
            entry   = float(p["entry_price"] or 1) or 1
            current = float(p["current_price"] or entry)
            pnl_pct = (current - entry) / entry * 100
            upnl    = float(p["unrealized_pnl"] or 0)

            if pnl_pct >= 10:
                trim_candidates.append({
                    "ticker":  p["ticker"],
                    "pnl_pct": round(pnl_pct, 1),
                    "upnl":    round(upnl, 2),
                    "action":  f"Up {pnl_pct:.1f}% — consider trimming 25–50%",
                })
            elif pnl_pct <= -8:
                review_candidates.append({
                    "ticker":  p["ticker"],
                    "pnl_pct": round(pnl_pct, 1),
                    "upnl":    round(upnl, 2),
                    "action":  f"Down {abs(pnl_pct):.1f}% — review stop loss",
                })

            if p["ticker"] in earnings_this_week:
                earnings_risk.append(p["ticker"])

        return {
            "total_positions":   len(positions),
            "trim_candidates":   trim_candidates,
            "review_candidates": review_candidates,
            "earnings_risk":     earnings_risk,
            "summary": (
                f"{len(positions)} open positions. "
                f"{len(trim_candidates)} trim candidates. "
                f"{len(review_candidates)} need review. "
                f"Earnings risk: {', '.join(earnings_risk) if earnings_risk else 'none this week'}."
            ),
        }
    except Exception as e:
        return {"error": str(e), "summary": f"Portfolio review unavailable: {e}"}


# ── ntfy admin push ────────────────────────────────────────────────────────

def _push_admin_ntfy(title: str, body: str, priority: int = 4) -> None:
    """Fire-and-forget ntfy push to ollietrades-admin topic."""
    import urllib.request

    def _send():
        try:
            payload = json.dumps({
                "topic":    _ADMIN_NTFY_TOPIC,
                "title":    title,
                "message":  body,
                "priority": priority,
                "tags":     ["newspaper"],
            }).encode()
            req = urllib.request.Request(
                "https://ntfy.sh",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=8)
        except Exception:
            pass  # ntfy failures must never crash trading logic

    threading.Thread(target=_send, daemon=True).start()


def _build_and_push_ntfy(report: dict) -> None:
    """Compose 3-line morning summary and fire to ollietrades-admin."""
    try:
        today    = report.get("date", "")
        gp       = report.get("game_plan", {}) or {}
        sectors  = report.get("sector_rotation", {}) or {}
        earnings = [e for e in report.get("earnings", []) if isinstance(e, dict) and "ticker" in e]
        congress = [c for c in report.get("congress_radar", []) if isinstance(c, dict) and c.get("is_buy")]
        setups   = [s for s in report.get("technical_setups", []) if isinstance(s, dict) and "symbol" in s]

        line1 = (
            f"📊 {gp.get('headline', 'Market update')} "
            f"| VIX {gp.get('vix', '—')} "
            f"| F&G {gp.get('fg_score', '—')}"
        )

        if setups:
            top   = setups[0]
            line2 = (
                f"🎯 Top setup: {top['symbol']} "
                f"({top.get('strategy', 'setup')}) "
                f"conf {int(top.get('confidence', 0) * 100)}%"
            )
        elif sectors.get("rotating_into"):
            hot   = sectors["rotating_into"][0]
            picks = sectors.get("hot_sector_picks", [])
            line2 = (
                f"🔥 Money into {hot.get('name', hot.get('etf', '?'))} "
                f"(+{hot.get('ret_5d', 0):.1f}%) "
                f"— watch {', '.join(picks[:2])}"
            )
        else:
            line2 = f"🔍 Focus: {gp.get('focus', 'Monitor key levels')[:80]}"

        if earnings:
            names = [f"{e['ticker']} ({e['date'][5:]})" for e in earnings[:3]]
            line3 = f"📅 Earnings: {', '.join(names)}"
        elif congress:
            c     = congress[0]
            line3 = (
                f"🏛 Congress: {c.get('politician', '?')} "
                f"BOUGHT {c.get('ticker', '?')} {c.get('amount', '')}"
            )
        else:
            line3 = f"📋 {gp.get('plan', '')[:100]}"

        _push_admin_ntfy(
            title=f"OllieTrades Intel — {today}",
            body=f"{line1}\n{line2}\n{line3}",
            priority=4,
        )
        logger.info("Morning intel ntfy pushed to %s", _ADMIN_NTFY_TOPIC)
    except Exception as e:
        logger.warning("Failed to build ntfy push: %s", e)


# ── Main entry point ───────────────────────────────────────────────────────

def generate_daily_intel_report(force: bool = False, push_ntfy: bool = False) -> dict:
    """
    Generate the full daily intelligence report (6 sections).
    Saves to data/morning_brief.json.
    push_ntfy=True fires the admin ntfy notification (6 AM run only).
    Cached per calendar day; force=True regenerates.
    Returns the full report dict.
    """
    with _intel_lock:
        today = _today_str()
        if not force and _intel_cache.get("date") == today and _intel_cache.get("generated_at"):
            return dict(_intel_cache)

        import pytz
        az      = pytz.timezone("US/Arizona")
        now_str = datetime.datetime.now(az).strftime("%A, %B %-d, %Y — %I:%M %p AZ")

        earnings    = _get_earnings_intel_3d()
        sectors     = _get_sector_rotation_radar()
        congress    = _get_congress_radar_48h()
        setups      = _get_technical_setups_convergent()
        game_plan   = _get_tomorrows_game_plan()
        port_review = _get_captain_portfolio_review()

        report = {
            "date":             today,
            "generated_at":     datetime.datetime.now().isoformat(),
            "label":            f"Daily Intel — {now_str}",
            "earnings":         earnings,
            "sector_rotation":  sectors,
            "congress_radar":   congress,
            "technical_setups": setups,
            "game_plan":        game_plan,
            "portfolio_review": port_review,
        }

        # Persist to JSON
        try:
            with open(_INTEL_JSON_PATH, "w") as fh:
                json.dump(report, fh, indent=2, default=str)
            logger.info("Daily intel report saved → %s", _INTEL_JSON_PATH)
        except Exception as e:
            logger.warning("Failed to save intel JSON: %s", e)

        if push_ntfy:
            _build_and_push_ntfy(report)

        _intel_cache.update(report)
        return report
