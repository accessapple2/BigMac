"""HM-TB-DEAD-GATE-DROPPED 2026-07-10.

bull_call_spread_v1.py and bear_put_spread_v1.py both required
tb_active=True (a Tractor Beam confidence check) as a mandatory AND-gate
for both their Tier-1 and Tier-2 signal paths. Traced end-to-end: Tractor
Beam's signal source (signal-center/signals.db, agent_name='tractor-beam')
has been dead since 2026-04-14 -- before either strategy was even written
(2026-05-01) -- so tb_active has been False 100% of the time for their
entire operational history, blocking both tiers unconditionally regardless
of price action. Traced all four files CLAUDE.md's Fleet Roster doctrine
names as "the live Tractor Beam functionality" (engine/strategies.py,
crew_scanner.py, phaser_lock.py, reveille.py) and confirmed none of them
produce fresh tractor-beam signals -- two are dead-end consumers of the
same empty table, two are unrelated systems. See docs/XO_BACKLOG.md
HM-ARMED-DORMANT-SPREAD-STRATEGIES.

Fix: tb_active is still computed (cheap, harmless -- a one-line revert if
a real TB replacement ever exists) but no longer required for either tier.
These tests confirm a signal fires on Tier-1/Tier-2 conditions alone, with
_get_tb_active mocked to return False (its permanent real-world state).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategies.base import MarketContext
from strategies.mock_data import OptionLeg, SpreadQuote


def _now():
    return datetime(2026, 7, 10, tzinfo=timezone.utc)


def _iv_snapshot(iv_rank, spot):
    from strategies.iv_rank import IVSnapshot
    return IVSnapshot(ticker="SPY", iv_rank=iv_rank, source="test",
                      current_iv=0.15, spot=spot)


def _quote(ticker="SPY", structure="bull_call_spread"):
    long_leg = OptionLeg(action="buy", option_type="call", strike=700.0,
                         expiration="2026-08-15", premium=10.0)
    short_leg = OptionLeg(action="sell", option_type="call", strike=710.0,
                          expiration="2026-08-15", premium=6.0)
    return SpreadQuote(ticker=ticker, structure=structure, long_leg=long_leg,
                       short_leg=short_leg, net_debit=4.0, net_credit=0.0,
                       max_profit=6.0, max_loss=4.0, width=10.0, dte=14)


def test_bull_call_spread_v1_tier1_fires_without_tb(monkeypatch):
    import strategies.bull_call_spread_v1 as m

    monkeypatch.setattr(m, "_is_dedup_blocked", lambda t: False)
    monkeypatch.setattr(m, "_bull_spread_v1_has_position", lambda t: False)
    monkeypatch.setattr(m, "_check_bmb", lambda t: t == "SPY")
    monkeypatch.setattr(m, "_get_tb_active", lambda t: False)  # permanent real-world state
    monkeypatch.setattr(m, "_check_tier2_buy_signal", lambda t: False)
    monkeypatch.setattr(m, "get_iv_rank", lambda t, record=True: _iv_snapshot(10.0, 700.0))
    monkeypatch.setattr(m, "_select_bull_call_spread_width",
                        lambda t, s, dte, spot, risk: (10.0, _quote()))
    monkeypatch.setattr(m, "_mark_fired", lambda t: None)

    ctx = MarketContext(as_of=_now(), regime="BULL", vix=15.0, spy_price=700.0)
    signals = m.BullCallSpreadV1().evaluate(ctx)

    spy_signals = [s for s in signals if s.ticker == "SPY"]
    assert len(spy_signals) == 1
    assert spy_signals[0].payload["tier"] == 1


def test_bull_call_spread_v1_tier2_fires_without_tb(monkeypatch):
    import strategies.bull_call_spread_v1 as m

    monkeypatch.setattr(m, "_is_dedup_blocked", lambda t: False)
    monkeypatch.setattr(m, "_bull_spread_v1_has_position", lambda t: False)
    monkeypatch.setattr(m, "_check_bmb", lambda t: False)  # no tier-1
    monkeypatch.setattr(m, "_get_tb_active", lambda t: False)
    monkeypatch.setattr(m, "_check_tier2_buy_signal", lambda t: t == "SPY")
    monkeypatch.setattr(m, "_get_pc_ratio", lambda t: 0.5)  # < 0.7, passes
    monkeypatch.setattr(m, "get_iv_rank", lambda t, record=True: _iv_snapshot(10.0, 700.0))
    monkeypatch.setattr(m, "_select_bull_call_spread_width",
                        lambda t, s, dte, spot, risk: (10.0, _quote()))
    monkeypatch.setattr(m, "_mark_fired", lambda t: None)

    ctx = MarketContext(as_of=_now(), regime="BULL", vix=15.0, spy_price=700.0)
    signals = m.BullCallSpreadV1().evaluate(ctx)

    spy_signals = [s for s in signals if s.ticker == "SPY"]
    assert len(spy_signals) == 1
    assert spy_signals[0].payload["tier"] == 2


def test_bear_put_spread_v1_tier1_fires_without_tb(monkeypatch):
    import strategies.bear_put_spread_v1 as m

    monkeypatch.setattr(m, "_is_dedup_blocked", lambda t: False)
    monkeypatch.setattr(m, "_check_bbd", lambda t: t == "SPY")
    monkeypatch.setattr(m, "_get_tb_active", lambda t: False)  # permanent real-world state
    monkeypatch.setattr(m, "_check_tier2_short_signal", lambda t: False)
    monkeypatch.setattr(m, "get_iv_rank", lambda t, record=True: _iv_snapshot(10.0, 700.0))
    monkeypatch.setattr(m, "_select_bear_spread_width",
                        lambda t, s, dte, spot, risk: (10.0, _quote(structure="bear_put_spread")))
    monkeypatch.setattr(m, "_mark_fired", lambda t: None)

    ctx = MarketContext(as_of=_now(), regime="BEAR", vix=15.0, spy_price=700.0)
    signals = m.BearPutSpreadV1().evaluate(ctx)

    spy_signals = [s for s in signals if s.ticker == "SPY"]
    assert len(spy_signals) == 1
    assert spy_signals[0].payload["tier"] == 1


def test_bear_put_spread_v1_tier2_fires_without_tb(monkeypatch):
    import strategies.bear_put_spread_v1 as m

    monkeypatch.setattr(m, "_is_dedup_blocked", lambda t: False)
    monkeypatch.setattr(m, "_check_bbd", lambda t: False)  # no tier-1
    monkeypatch.setattr(m, "_get_tb_active", lambda t: False)
    monkeypatch.setattr(m, "_check_tier2_short_signal", lambda t: t == "SPY")
    monkeypatch.setattr(m, "_get_pc_ratio", lambda t: 1.5)  # > 1.0, passes
    monkeypatch.setattr(m, "get_iv_rank", lambda t, record=True: _iv_snapshot(10.0, 700.0))
    monkeypatch.setattr(m, "_select_bear_spread_width",
                        lambda t, s, dte, spot, risk: (10.0, _quote(structure="bear_put_spread")))
    monkeypatch.setattr(m, "_mark_fired", lambda t: None)

    ctx = MarketContext(as_of=_now(), regime="BEAR", vix=15.0, spy_price=700.0)
    signals = m.BearPutSpreadV1().evaluate(ctx)

    spy_signals = [s for s in signals if s.ticker == "SPY"]
    assert len(spy_signals) == 1
    assert spy_signals[0].payload["tier"] == 2
