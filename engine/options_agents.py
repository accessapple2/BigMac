"""
Production options agents — all rules-based, deterministic, paper-only.

Agents:
  - QuarkIronCondor    : Iron condors on SPY/QQQ in CHOP/BULL/BEAR regimes
  - McCoyBullPut       : Bull put spreads in BULL/CHOP, high-quality names
  - AndersonBearCall   : Bear call spreads in BEAR or overbought BULL
  - CoveredCallAgent   : Covered calls on existing paper equity positions

All agents respect:
  - Regime gating (see agent.can_fire())
  - Daily loss limit per agent
  - Max concurrent positions per agent
  - Market hours only (9:30 AM – 4:00 PM ET)
  - Paper-only — NEVER calls real broker API
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from datetime import time as dtime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from engine.options_exec import open_options_trade, close_options_trade  # noqa: F401

DB_PATH = "data/trader.db"
ET = ZoneInfo("America/New_York")


# ============================================================
# BASE CLASS
# ============================================================

class OptionsAgent:
    """Base class for all options agents."""

    agent_id: str = ""
    structure: str = ""
    book_tag: str = "fleet"
    max_concurrent: int = 0
    max_loss_per_trade: float = 0.0
    daily_loss_limit: float = 0.0

    allowed_regimes: set = set()
    require_vix_under: Optional[float] = None
    require_vix_over:  Optional[float] = None

    def now_et(self) -> datetime:
        return datetime.now(ET)

    def is_market_hours(self) -> bool:
        n = self.now_et()
        if n.weekday() >= 5:
            return False
        return dtime(9, 30) <= n.time() <= dtime(16, 0)

    def open_positions_count(self) -> int:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        n = c.fetchone()[0]
        conn.close()
        return n

    def today_realized_pnl(self) -> float:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            """SELECT COALESCE(SUM(pnl),0) FROM options_trades
               WHERE agent_id=? AND status='closed' AND DATE(exit_date)=DATE('now')""",
            (self.agent_id,),
        )
        pnl = c.fetchone()[0]
        conn.close()
        return pnl

    def can_fire(self, regime: str, vix: float) -> tuple[bool, str]:
        """Returns (allowed, reason_if_blocked)."""
        if not self.is_market_hours():
            return False, "outside market hours"
        if self.open_positions_count() >= self.max_concurrent:
            return False, f"max concurrent positions ({self.max_concurrent}) reached"
        daily_pnl = self.today_realized_pnl()
        if self.daily_loss_limit and daily_pnl <= -self.daily_loss_limit:
            return False, f"daily loss limit hit ({daily_pnl:.2f})"
        if self.allowed_regimes and regime not in self.allowed_regimes:
            return False, f"regime {regime!r} not in allowed {self.allowed_regimes}"
        if self.require_vix_under is not None and vix >= self.require_vix_under:
            return False, f"vix {vix} >= {self.require_vix_under}"
        if self.require_vix_over is not None and vix <= self.require_vix_over:
            return False, f"vix {vix} <= {self.require_vix_over}"
        return True, "ok"

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        """
        Return a list of potential entry signals. Each is a dict describing
        the proposed trade. Does NOT execute anything.
        """
        raise NotImplementedError

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        """
        Review open positions and return list of recommended close actions.
        current_quotes: {symbol: underlying_price}
        """
        raise NotImplementedError


# ============================================================
# QUARK IRON CONDOR
# ============================================================

class QuarkIronCondor(OptionsAgent):
    """
    Iron condor primary agent.
    Backtest: +249.6%, 82.1% WR, 319 trades.
    Sells strangles at ~22 delta with 5-point wings, 7-21 DTE.
    """
    agent_id        = "quark-ic"
    structure       = "iron_condor"
    book_tag        = "fleet"
    max_concurrent  = 8
    max_loss_per_trade = 400.0
    daily_loss_limit   = 800.0

    allowed_regimes  = {"CHOP", "BULL", "BEAR"}
    require_vix_under = 25.0

    universe      = ["SPY", "QQQ"]
    target_delta  = 0.22
    wing_width    = 5
    target_dte_min = 7
    target_dte_max = 21

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []

        # Only queue one new condor per cycle to avoid stacking
        if self.open_positions_count() >= 1:
            return []

        # IC fires on regime alone — range-bound credit collection, not directional
        return [{
            "agent_id": self.agent_id,
            "structure": "iron_condor",
            "symbol": self.universe[0],
            "reason": f"CHOP/BULL regime, VIX {vix:.1f} < 25, range collection",
            "params": {
                "target_delta": self.target_delta,
                "wing_width": self.wing_width,
                "dte_min": self.target_dte_min,
                "dte_max": self.target_dte_max,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        """
        Close triggers:
          - 7 DTE time-stop (was 21 DTE at entry)
          - Short put breached (underlying < short put strike)
          - Short call breached (underlying > short call strike)
        """
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()

        actions = []
        for row in rows:
            legs = json.loads(row["legs_json"])
            short_put  = next((l for l in legs if l["side"] == "short" and l["type"] == "put"),  None)
            short_call = next((l for l in legs if l["side"] == "short" and l["type"] == "call"), None)
            price = current_quotes.get(row["symbol"])

            try:
                dte = (datetime.fromisoformat(row["expiration"]).date() - datetime.now().date()).days
                if dte <= 7:
                    actions.append({"trade_id": row["id"], "reason": "7_dte_time_stop"})
                    continue
            except Exception:
                pass

            if price and short_put and price < short_put["strike"]:
                actions.append({"trade_id": row["id"], "reason": "short_put_breach"})
                continue
            if price and short_call and price > short_call["strike"]:
                actions.append({"trade_id": row["id"], "reason": "short_call_breach"})
        return actions


# ============================================================
# McCOY BULL PUT SPREAD
# ============================================================

class McCoyBullPut(OptionsAgent):
    """
    Bull put spreads in BULL or CHOP on high-quality names.
    Backtest: +15.1%, 75% WR.
    Requires at least one fleet BUY convergence signal on a name in universe.
    """
    agent_id        = "mccoy-bps"
    structure       = "bull_put_spread"
    book_tag        = "fleet"
    max_concurrent  = 5
    max_loss_per_trade = 300.0
    daily_loss_limit   = 600.0

    allowed_regimes  = {"BULL", "CHOP"}
    require_vix_under = 22.0

    universe      = ["SPY", "QQQ", "NVDA", "AAPL", "MSFT", "META"]
    target_delta  = 0.30
    wing_width    = 5
    target_dte_min = 7
    target_dte_max = 14

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []

        if not convergence_signals:
            return []

        buy_tickers = {
            s.get("ticker", s.get("symbol", ""))
            for s in convergence_signals
            if str(s.get("action", "")).upper() == "BUY"
        }
        candidates = [t for t in self.universe if t in buy_tickers]
        if not candidates:
            return []

        return [{
            "agent_id": self.agent_id,
            "structure": "bull_put_spread",
            "symbol": candidates[0],
            "reason": f"Fleet BUY convergence on {candidates[0]}, {regime} regime",
            "params": {
                "target_delta": self.target_delta,
                "wing_width": self.wing_width,
                "dte_min": self.target_dte_min,
                "dte_max": self.target_dte_max,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()

        actions = []
        for row in rows:
            try:
                dte = (datetime.fromisoformat(row["expiration"]).date() - datetime.now().date()).days
                if dte <= 3:
                    actions.append({"trade_id": row["id"], "reason": "3_dte_time_stop"})
                    continue
            except Exception:
                pass

            legs = json.loads(row["legs_json"])
            short_put = next((l for l in legs if l["side"] == "short" and l["type"] == "put"), None)
            price = current_quotes.get(row["symbol"])
            if price and short_put and price < short_put["strike"]:
                actions.append({"trade_id": row["id"], "reason": "short_put_breach"})
        return actions


# ============================================================
# ANDERSON BEAR CALL SPREAD
# ============================================================

class AndersonBearCall(OptionsAgent):
    """
    Bear call spreads. Tactical counterweight.
    Backtest: -21.0%, 35.2% WR — kept with tight gating.
    Only fires in BEAR regime or explicitly overbought BULL (RSI > 75).
    """
    agent_id        = "anderson-bcs"
    structure       = "bear_call_spread"
    book_tag        = "fleet"
    max_concurrent  = 3
    max_loss_per_trade = 500.0
    daily_loss_limit   = 500.0

    allowed_regimes  = {"BEAR", "BULL"}
    require_vix_over = 18.0

    universe      = ["SPY", "QQQ"]
    target_delta  = 0.25
    wing_width    = 5
    target_dte_min = 7
    target_dte_max = 14

    def _is_overbought(self) -> bool:
        """
        Conservative placeholder — returns False until a real RSI endpoint
        is wired. This prevents Anderson from firing in BULL at all until
        we confirm the overbought detection is reliable.
        """
        return False

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []

        # In BULL regime, require confirmed overbought
        if regime == "BULL" and not self._is_overbought():
            return []

        return [{
            "agent_id": self.agent_id,
            "structure": "bear_call_spread",
            "symbol": self.universe[0],
            "reason": f"{regime} regime, VIX {vix:.1f} > 18",
            "params": {
                "target_delta": self.target_delta,
                "wing_width": self.wing_width,
                "dte_min": self.target_dte_min,
                "dte_max": self.target_dte_max,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()

        actions = []
        for row in rows:
            try:
                dte = (datetime.fromisoformat(row["expiration"]).date() - datetime.now().date()).days
                if dte <= 3:
                    actions.append({"trade_id": row["id"], "reason": "3_dte_time_stop"})
                    continue
            except Exception:
                pass

            legs = json.loads(row["legs_json"])
            short_call = next((l for l in legs if l["side"] == "short" and l["type"] == "call"), None)
            price = current_quotes.get(row["symbol"])
            if price and short_call and price > short_call["strike"]:
                actions.append({"trade_id": row["id"], "reason": "short_call_breach"})
        return actions


# ============================================================
# COVERED CALL AGENT
# ============================================================

class CoveredCallAgent(OptionsAgent):
    """
    Sells covered calls on existing paper equity positions.
    Backtest: -8.0%, 24.3% WR — weak alone, complements longs.
    Only fires when fleet already holds >= 100 shares of an underlying.
    _get_qualifying_positions() is stubbed; wire to real positions in follow-up.
    """
    agent_id        = "covered-call"
    structure       = "covered_call"
    book_tag        = "fleet"
    max_concurrent  = 5
    max_loss_per_trade = 0.0       # N/A — "loss" = underlying called away
    daily_loss_limit   = 500.0

    allowed_regimes  = {"BULL", "CHOP"}
    require_vix_under = 25.0

    target_delta  = 0.30
    target_dte_min = 21
    target_dte_max = 45

    def _get_qualifying_positions(self) -> List[Dict]:
        """
        Stub: returns positions with qty >= 100 shares.
        Wire to /api/alpaca/positions in a follow-up sprint.
        """
        return []

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []

        positions = self._get_qualifying_positions()
        if not positions:
            return []

        pos = positions[0]
        return [{
            "agent_id": self.agent_id,
            "structure": "covered_call",
            "symbol": pos["symbol"],
            "reason": f"Covering {pos['qty']} shares of {pos['symbol']}",
            "params": {
                "target_delta": self.target_delta,
                "dte_min": self.target_dte_min,
                "dte_max": self.target_dte_max,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()

        actions = []
        for row in rows:
            try:
                dte = (datetime.fromisoformat(row["expiration"]).date() - datetime.now().date()).days
                if dte <= 3:
                    actions.append({"trade_id": row["id"], "reason": "3_dte_time_stop"})
            except Exception:
                pass
        return actions


# ============================================================
# GHOST TRADER BASE CLASS
# ============================================================

class GhostAgent(OptionsAgent):
    """
    Ghost traders run paper-only with isolated book_tag='ghost'.
    They consume fleet signals but contribute ZERO weight to convergence.
    Purpose: collect statistical evidence over 60+ days before any
    promotion to production tier.
    """
    book_tag = "ghost"
    role = "ghost"
    ghost_max_loss_per_book_pct = 0.20  # ghost book drawdown hard cap

    def can_fire(self, regime: str, vix: float) -> tuple[bool, str]:
        allowed, reason = super().can_fire(regime, vix)
        if not allowed:
            return allowed, reason
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT starting_capital, current_cash FROM options_books WHERE book_tag='ghost'"
        )
        row = c.fetchone()
        conn.close()
        if row:
            start, cash = row
            if start and cash < start * (1 - self.ghost_max_loss_per_book_pct):
                return False, f"ghost book drawdown > 20% (cash={cash:.2f} from {start:.2f})"
        return True, "ok"


# ============================================================
# GHOST 1: KIRK BULL CALL
# ============================================================

class GhostKirkBullCall(GhostAgent):
    """
    Bull call spreads on strong BULL convergence (3+ fleet agents agree).
    No backtest data exists — live paper will tell us whether this structure
    has edge. Runs alongside GhostLongCall as a spread-vs-naked comparison.
    """
    agent_id        = "ghost-kirk-bc"
    structure       = "bull_call_spread"
    max_concurrent  = 2
    max_loss_per_trade = 250.0
    daily_loss_limit   = 250.0

    allowed_regimes  = {"BULL"}
    require_vix_under = 20.0

    universe           = ["SPY", "QQQ", "NVDA", "AAPL", "MSFT"]
    target_dte_min     = 14
    target_dte_max     = 30
    target_delta_long  = 0.45
    spread_width       = 5

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []
        if not convergence_signals:
            return []
        ticker_votes: Dict[str, int] = {}
        for s in convergence_signals:
            if str(s.get("action", "")).upper() != "BUY":
                continue
            t = s.get("ticker", s.get("symbol", "")).upper()
            if t in self.universe:
                ticker_votes[t] = ticker_votes.get(t, 0) + 1
        strong = [t for t, n in ticker_votes.items() if n >= 3]
        if not strong:
            return []
        return [{
            "agent_id": self.agent_id,
            "structure": "bull_call_spread",
            "symbol": strong[0],
            "reason": f"3+ fleet BUY convergence on {strong[0]}, BULL + VIX {vix:.1f}",
            "params": {
                "target_delta_long": self.target_delta_long,
                "spread_width": self.spread_width,
                "dte_min": self.target_dte_min,
                "dte_max": self.target_dte_max,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()
        actions = []
        for row in rows:
            try:
                dte = (datetime.fromisoformat(row["expiration"]).date() - datetime.now().date()).days
                if dte <= 5:
                    actions.append({"trade_id": row["id"], "reason": "5_dte_time_stop"})
            except Exception:
                pass
        return actions


# ============================================================
# GHOST 2: KIRK 0DTE BULL CALL
# ============================================================

class GhostKirk0DTEBullCall(GhostAgent):
    """
    Same-day bull call spreads on SPY/QQQ. Tests 0DTE leverage hypothesis
    with defined-risk spread (not naked). Hard-closed by 3:45 PM ET.
    """
    agent_id        = "ghost-kirk-0dte-bc"
    structure       = "bull_call_spread"
    max_concurrent  = 1
    max_loss_per_trade = 150.0
    daily_loss_limit   = 300.0

    allowed_regimes  = {"BULL"}
    require_vix_under = 22.0

    universe       = ["SPY", "QQQ"]
    spread_width   = 3
    earliest_entry = dtime(10, 0)
    latest_entry   = dtime(14, 0)
    hard_close     = dtime(15, 45)

    def can_fire(self, regime: str, vix: float) -> tuple[bool, str]:
        allowed, reason = super().can_fire(regime, vix)
        if not allowed:
            return allowed, reason
        n = self.now_et().time()
        if n < self.earliest_entry:
            return False, f"before entry window ({self.earliest_entry})"
        if n > self.latest_entry:
            return False, f"after entry cutoff ({self.latest_entry})"
        return True, "ok"

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []
        ticker_votes: Dict[str, int] = {}
        for s in convergence_signals:
            if str(s.get("action", "")).upper() != "BUY":
                continue
            t = s.get("ticker", s.get("symbol", "")).upper()
            if t in self.universe:
                ticker_votes[t] = ticker_votes.get(t, 0) + 1
        strong = [t for t, n in ticker_votes.items() if n >= 2]
        if not strong:
            return []
        return [{
            "agent_id": self.agent_id,
            "structure": "bull_call_spread",
            "symbol": strong[0],
            "reason": f"0DTE leverage test, {strong[0]} 2+ BUY convergence",
            "params": {
                "target_delta_long": 0.50,
                "spread_width": self.spread_width,
                "dte_min": 0,
                "dte_max": 0,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()
        actions = []
        now = self.now_et().time()
        for row in rows:
            if now >= self.hard_close:
                actions.append({"trade_id": row["id"], "reason": "0dte_hard_close_345pm"})
        return actions


# ============================================================
# GHOST 3: LONG CALL (SCIENTIFIC CONTROL)
# ============================================================

class GhostLongCall(GhostAgent):
    """
    Naked long calls. Backtest: -179%.
    Purpose: validate or invalidate the backtest finding in live paper.
    Fires on same signals as GhostKirkBullCall — direct spread-vs-naked A/B.
    """
    agent_id        = "ghost-long-call"
    structure       = "long_call"
    max_concurrent  = 1
    max_loss_per_trade = 150.0
    daily_loss_limit   = 200.0

    allowed_regimes  = {"BULL"}
    require_vix_under = 22.0

    universe       = ["SPY", "QQQ", "NVDA"]
    target_dte_min = 14
    target_dte_max = 30
    target_delta   = 0.50

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []
        ticker_votes: Dict[str, int] = {}
        for s in convergence_signals:
            if str(s.get("action", "")).upper() != "BUY":
                continue
            t = s.get("ticker", s.get("symbol", "")).upper()
            if t in self.universe:
                ticker_votes[t] = ticker_votes.get(t, 0) + 1
        strong = [t for t, n in ticker_votes.items() if n >= 3]
        if not strong:
            return []
        return [{
            "agent_id": self.agent_id,
            "structure": "long_call",
            "symbol": strong[0],
            "reason": f"CONTROL: naked long call on {strong[0]} — validating -179% backtest",
            "params": {
                "target_delta": self.target_delta,
                "dte_min": self.target_dte_min,
                "dte_max": self.target_dte_max,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()
        actions = []
        for row in rows:
            try:
                dte = (datetime.fromisoformat(row["expiration"]).date() - datetime.now().date()).days
                if dte <= 5:
                    actions.append({"trade_id": row["id"], "reason": "5_dte_time_stop"})
            except Exception:
                pass
        return actions


# ============================================================
# GHOST 4: NAKED PUT (SCIENTIFIC CONTROL)
# ============================================================

class GhostNakedPut(GhostAgent):
    """
    Naked long puts. Backtest: +0.3% on 10 trades (sample too small).
    Only fires in BEAR/CHOP with VIX > 20 to gather more data.
    """
    agent_id        = "ghost-naked-put"
    structure       = "long_put"
    max_concurrent  = 1
    max_loss_per_trade = 150.0
    daily_loss_limit   = 200.0

    allowed_regimes = {"BEAR", "CHOP"}
    require_vix_over = 20.0

    universe       = ["SPY", "QQQ"]
    target_dte_min = 14
    target_dte_max = 30
    target_delta   = 0.40

    def scan(self, regime: str, vix: float, convergence_signals: List[Dict]) -> List[Dict]:
        allowed, reason = self.can_fire(regime, vix)
        if not allowed:
            return []
        ticker_votes: Dict[str, int] = {}
        for s in convergence_signals:
            if str(s.get("action", "")).upper() != "SELL":
                continue
            t = s.get("ticker", s.get("symbol", "")).upper()
            if t in self.universe:
                ticker_votes[t] = ticker_votes.get(t, 0) + 1
        strong = [t for t, n in ticker_votes.items() if n >= 2]
        # In BEAR with no SELL signals, still consider SPY
        if not strong and regime == "BEAR":
            strong = ["SPY"]
        if not strong:
            return []
        return [{
            "agent_id": self.agent_id,
            "structure": "long_put",
            "symbol": strong[0],
            "reason": f"CONTROL: naked long put on {strong[0]} — {regime}, VIX {vix:.1f}",
            "params": {
                "target_delta": self.target_delta,
                "dte_min": self.target_dte_min,
                "dte_max": self.target_dte_max,
            },
        }]

    def manage_open_positions(self, current_quotes: Dict[str, float]) -> List[Dict]:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT * FROM options_trades WHERE agent_id=? AND status='open'",
            (self.agent_id,),
        )
        rows = c.fetchall()
        conn.close()
        actions = []
        for row in rows:
            try:
                dte = (datetime.fromisoformat(row["expiration"]).date() - datetime.now().date()).days
                if dte <= 5:
                    actions.append({"trade_id": row["id"], "reason": "5_dte_time_stop"})
            except Exception:
                pass
        return actions


# ============================================================
# AGENT REGISTRY + RUNNER
# ============================================================

ALL_AGENTS: Dict[str, OptionsAgent] = {
    # Production book
    "quark-ic":            QuarkIronCondor(),
    "mccoy-bps":           McCoyBullPut(),
    "anderson-bcs":        AndersonBearCall(),
    "covered-call":        CoveredCallAgent(),
    # Ghost research book
    "ghost-kirk-bc":       GhostKirkBullCall(),
    "ghost-kirk-0dte-bc":  GhostKirk0DTEBullCall(),
    "ghost-long-call":     GhostLongCall(),
    "ghost-naked-put":     GhostNakedPut(),
}


def run_scan_cycle(
    regime: str,
    vix: float,
    convergence_signals: List[Dict],
    current_quotes: Dict[str, float],
) -> Dict[str, Dict]:
    """
    Dry-run scan of all options agents.
    Returns {agent_id: {"signals": [...], "closes": [...]}}.
    Does NOT execute any trades — pure signal generation.
    Execution happens via a separate confirm step after Admiral review.
    """
    out: Dict[str, Dict] = {}
    for aid, agent in ALL_AGENTS.items():
        try:
            signals = agent.scan(regime, vix, convergence_signals)
            closes  = agent.manage_open_positions(current_quotes)
            out[aid] = {"signals": signals, "closes": closes}
        except Exception as e:
            out[aid] = {"signals": [], "closes": [], "error": str(e)[:200]}
    return out
