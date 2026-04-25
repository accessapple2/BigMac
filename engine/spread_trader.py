"""
Spread execution module — bear call spreads + iron condors.
SCAFFOLDING ONLY — all execution gated behind SPREADS_ENABLED = False.
Real strategy logic + Alpaca multi-leg orders come in a separate ship order.
"""
import sqlite3
import json as _json

SPREADS_ENABLED = False  # MASTER GATE — flip only after full Alpaca options wiring is tested


def bear_call_spread(agent_id, ticker, short_strike, long_strike,
                     expiry, qty=1, reasoning=""):
    """
    Open a bear call spread (SELL lower call, BUY higher call, same expiry).
    Max profit = net credit received. Max loss = strike_width - credit.

    Args:
        agent_id: player_id of the originating agent
        ticker: underlying symbol (e.g. 'SPY')
        short_strike: strike to SELL (lower)
        long_strike: strike to BUY (higher, caps max loss)
        expiry: option expiry string e.g. '2026-05-16'
        qty: number of contracts (default 1)
        reasoning: signal reasoning for audit trail
    """
    if not SPREADS_ENABLED:
        print(f"[SPREADS] bear_call_spread {ticker} {short_strike}/{long_strike} blocked — SPREADS_ENABLED=False")
        return None

    assert long_strike > short_strike, "Bear call: long_strike must be HIGHER than short_strike"
    assert qty > 0, "qty must be positive"

    conn = sqlite3.connect("data/trader.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO trades(symbol, action, qty, player_id, reasoning, spread_data, execution_type) "
        "VALUES(?, 'SPREAD_BEAR_CALL', ?, ?, ?, ?, 'simulated')",
        (
            ticker, qty, agent_id, reasoning,
            _json.dumps({"short": short_strike, "long": long_strike, "expiry": expiry}),
        )
    )
    spread_id = cur.lastrowid
    conn.commit()
    conn.close()

    # Stub: real impl uses alpaca-py LeggedOrder / OptionLegRequest for multi-leg
    print(f"[SPREADS] would submit bear call: {ticker} SELL {short_strike}C / BUY {long_strike}C exp {expiry} x{qty}")
    return {"spread_id": spread_id, "submitted": False, "reason": "SPREADS_ENABLED=False"}


def iron_condor(agent_id, ticker, put_long, put_short, call_short, call_long,
                expiry, qty=1, reasoning=""):
    """
    Open an iron condor (bull put spread + bear call spread combined).
    Profit if underlying stays between put_short and call_short at expiry.

    Args:
        put_long: BUY put at this lower strike (max-loss floor)
        put_short: SELL put at this strike (collect premium)
        call_short: SELL call at this strike (collect premium)
        call_long: BUY call at this upper strike (max-loss cap)
    """
    if not SPREADS_ENABLED:
        print(f"[SPREADS] iron_condor {ticker} blocked — SPREADS_ENABLED=False")
        return None

    assert put_long < put_short < call_short < call_long, \
        "Iron condor strikes must satisfy: put_long < put_short < call_short < call_long"
    assert qty > 0, "qty must be positive"

    conn = sqlite3.connect("data/trader.db")
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO trades(symbol, action, qty, player_id, reasoning, spread_data, execution_type) "
        "VALUES(?, 'SPREAD_IRON_CONDOR', ?, ?, ?, ?, 'simulated')",
        (
            ticker, qty, agent_id, reasoning,
            _json.dumps({
                "put_long": put_long, "put_short": put_short,
                "call_short": call_short, "call_long": call_long,
                "expiry": expiry,
            }),
        )
    )
    spread_id = cur.lastrowid
    conn.commit()
    conn.close()

    print(f"[SPREADS] would submit iron condor: {ticker} "
          f"BUY {put_long}P / SELL {put_short}P / SELL {call_short}C / BUY {call_long}C exp {expiry} x{qty}")
    return {"spread_id": spread_id, "submitted": False, "reason": "SPREADS_ENABLED=False (stub)"}


# ── To enable spreads (DOCUMENT ONLY — do not run until wired) ──
# 1. Implement real Alpaca multi-leg option order in this file
#    (alpaca-py: MarketOrderRequest with legs=[] for multi-leg)
# 2. Add strategy logic to a dedicated agent (e.g. Counselor Troi)
# 3. Paper-test with qty=1 on liquid SPY/QQQ options for ≥30 days
# 4. Set SPREADS_ENABLED = True
# 5. Update CLAUDE.md with promotion decision date + OOS metrics
