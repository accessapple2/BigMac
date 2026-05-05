"""Post-Earnings Drift agent — short-side specialist.

Watches gap-down + VWAP rejection in a 1-48hr window after earnings.
Bypasses the firmwide earnings blackout BY DESIGN — this is the only
agent allowed to trade that window, and only short-side setups.

Gated like bull_spread_v1: paper-only until 30 trades + positive expectancy.
Respects per-player halt flag in trader.db (paper_trader.py:550 pattern).
"""
from __future__ import annotations
from datetime import datetime, timedelta
import os


_AGENT_NAME    = "post_earnings_drift"
_DISPLAY_NAME  = "Post-Earnings Drift"
_GATE_TRADES   = 30
_MIN_GAP_PCT   = -2.0
_VWAP_REJ_PCT  = -0.3
_WIN_HRS_MIN   = 1.0
_WIN_HRS_MAX   = 48.0


def _hours_since(earnings_dt):
    return (datetime.now() - earnings_dt).total_seconds() / 3600.0


def _in_window(earnings_dt):
    h = _hours_since(earnings_dt)
    return _WIN_HRS_MIN <= h <= _WIN_HRS_MAX


def _vwap(highs, lows, closes, volumes):
    if not closes or not volumes or sum(volumes) == 0:
        return 0.0
    typical = [(h + l + c) / 3.0 for h, l, c in zip(highs, lows, closes)]
    pv = sum(t * v for t, v in zip(typical, volumes))
    return pv / sum(volumes)


class PostEarningsDriftAgent:
    def __init__(self, enabled=True):
        self.name = _AGENT_NAME
        self.display_name = _DISPLAY_NAME
        self.enabled = enabled
        self.gate_trades = _GATE_TRADES
        self.gated = True

    def is_halted(self):
        try:
            import sqlite3
            db = os.path.expanduser("~/autonomous-trader/data/trader.db")
            con = sqlite3.connect(db, timeout=2.0)
            cur = con.cursor()
            cur.execute(
                "SELECT is_halted FROM agent_state WHERE agent=? LIMIT 1",
                (self.name,),
            )
            row = cur.fetchone()
            con.close()
            return bool(row and row[0])
        except Exception:
            return False

    def scan(self, market_data):
        if not self.enabled or self.is_halted():
            return []

        signals = []
        for symbol, data in (market_data or {}).items():
            try:
                bars = data.get("bars") or []
                earn = data.get("earnings_dt")
                regime = data.get("regime", "")
                if not bars or earn is None:
                    continue
                if not isinstance(earn, datetime):
                    earn = datetime.fromisoformat(str(earn))
                if not _in_window(earn):
                    continue
                if len(bars) < 5:
                    continue

                opens   = [b[0] for b in bars]
                highs   = [b[1] for b in bars]
                lows    = [b[2] for b in bars]
                closes  = [b[3] for b in bars]
                volumes = [b[4] for b in bars]

                prev_close = closes[-2] if len(closes) >= 2 else opens[-1]
                today_open = opens[-1]
                last_close = closes[-1]
                if prev_close <= 0:
                    continue
                gap_pct = ((today_open - prev_close) / prev_close) * 100.0

                vwap = _vwap(highs, lows, closes, volumes)
                if vwap <= 0:
                    continue
                vwap_rej_pct = ((last_close - vwap) / vwap) * 100.0

                if gap_pct > _MIN_GAP_PCT:
                    continue
                if vwap_rej_pct > _VWAP_REJ_PCT:
                    continue

                hrs = _hours_since(earn)
                entry  = round(last_close, 2)
                stop   = round(entry * 1.02, 2)
                target = round(entry * 0.94, 2)
                conf = min(1.0, abs(gap_pct) / 8.0 + abs(vwap_rej_pct) / 4.0)

                signals.append({
                    "agent": self.name,
                    "symbol": symbol,
                    "action": "SHORT",
                    "signal_type": "SHORT",
                    "confidence": round(conf, 2),
                    "reason": "post_earnings_drift",
                    "entry_price": entry,
                    "stop_price": stop,
                    "target_price": target,
                    "meta": {
                        "gap_pct": round(gap_pct, 2),
                        "vwap": round(vwap, 2),
                        "vwap_rej_pct": round(vwap_rej_pct, 2),
                        "hours_since_earnings": round(hrs, 1),
                        "regime": regime,
                        "gated": self.gated,
                        "paper_only": self.gated,
                    },
                })

                try:
                    from engine.ntfy import notify_post_earnings_short
                    notify_post_earnings_short(
                        symbol=symbol, price=entry, gap_pct=gap_pct,
                        vwap=vwap, hours_since_earnings=hrs,
                        reasoning="gated=" + str(self.gated) + " regime=" + str(regime),
                    )
                except Exception:
                    pass

            except Exception:
                continue

        return signals


_agent = PostEarningsDriftAgent(enabled=True)


def get_post_earnings_brief(market_data):
    return _agent.scan(market_data)
