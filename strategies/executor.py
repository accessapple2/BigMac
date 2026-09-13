"""
Execution adapter: routes StrategySignal -> alpaca_options paper orders.

EXECUTION GATE: _EXECUTION_ENABLED is a module-level constant, currently
**True** — live paper execution is ENABLED. The False->True flip (~2026-05-05)
was DELIBERATE and Admiral-confirmed; this is the live gate the autonomous
paper trader runs on. NOT an env var, NOT runtime config — must be edited in
source to change. Do NOT flip back to False without Admiral sign-off (that
reverts the whole fleet to emit-only).
"""
from __future__ import annotations
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from .base import StrategySignal


# ═══════════════════════════════════════════════════════════════════════
# EXECUTION GATE — LIVE (True). Admiral-confirmed deliberate (~2026-05-05).
# Do NOT flip to False without Admiral sign-off (reverts fleet to emit-only).
# ═══════════════════════════════════════════════════════════════════════
_EXECUTION_ENABLED: bool = True
# ═══════════════════════════════════════════════════════════════════════


DB_PATH = Path(__file__).parent.parent / "data" / "trader.db"


@dataclass
class ExecutionResult:
    signal_id: Optional[int]
    ticker: str
    exit_tag: str
    status: str  # "emit_only" | "executed" | "rejected" | "error"
    broker_order_id: Optional[str] = None
    options_trade_id: Optional[int] = None
    reason: Optional[str] = None
    executed_at: Optional[datetime] = None


def execute_signal(signal: StrategySignal, signal_id: Optional[int] = None) -> ExecutionResult:
    """
    Execute a StrategySignal. With _EXECUTION_ENABLED=True (current/live),
    routes to _execute_live. If the gate is ever flipped back to False,
    returns 'emit_only' instead.
    """
    if not _EXECUTION_ENABLED:
        return ExecutionResult(
            signal_id=signal_id,
            ticker=signal.ticker,
            exit_tag=signal.exit_tag,
            status="emit_only",
            reason="_EXECUTION_ENABLED is False — gated until Task 7 complete",
        )
    return _execute_live(signal, signal_id)


def _execute_live(signal: StrategySignal, signal_id: Optional[int]) -> ExecutionResult:
    """Live path. Only reachable when _EXECUTION_ENABLED is True."""
    try:
        from engine.alpaca_options import submit_vertical_spread
    except ImportError as e:
        return ExecutionResult(
            signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
            status="error",
            reason=f"alpaca_options import failed: {e}"
        )

    payload = signal.payload
    structure = payload.get("structure")
    # HM-EXECUTOR-STRUCTURE-WHITELIST-GAP 2026-07-10: bear_put_spread_v1
    # emits "bear_put_spread"/"bear_call_spread" (bear_put_spread_v1.py
    # lines 449/451) but neither was in this whitelist -- every signal
    # that strategy ever generated was rejected before submission, since
    # it was wired up (confirmed: zero options_trades rows for
    # strategy_id='bear_put_spread_v1', ever). submit_vertical_spread()'s
    # own docstring already documents bear-put-spread support by name
    # (buy_symbol=higher strike, sell_symbol=lower strike), and the
    # payload shape (long_leg/short_leg/net_debit/net_credit) is already
    # identical to the bull-spread strategies this whitelist was written
    # for -- this was purely a missing tuple entry, not a missing feature.
    if structure not in ("bull_call_spread", "bull_put_spread",
                          "bear_put_spread", "bear_call_spread"):
        return ExecutionResult(
            signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
            status="rejected",
            reason=f"Unknown structure: {structure}"
        )

    contracts = payload.get("contracts", 1)
    long_leg = payload["long_leg"]
    short_leg = payload["short_leg"]

    # Real alpaca signature: submit_vertical_spread(
    #   player_id, buy_symbol, sell_symbol, qty, strategy
    # )
    player_id = f"strategy:{signal.strategy_id}"
    buy_symbol = _occ_symbol(signal.ticker, long_leg)
    sell_symbol = _occ_symbol(signal.ticker, short_leg)

    try:
        result = submit_vertical_spread(
            player_id=player_id,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
            qty=contracts,
            strategy=structure,
        )
    except Exception as e:
        return ExecutionResult(
            signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
            status="error",
            reason=f"alpaca submit raised: {type(e).__name__}: {e}"
        )

    # submit_vertical_spread returns dict: {"order_id": ..., "filled_avg_price": ..., ...} or {"skipped": True, ...}
    fill_price = None
    if isinstance(result, dict):
        if result.get("skipped"):
            return ExecutionResult(
                signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
                status="rejected",
                reason=f"alpaca skipped: {result}"
            )
        broker_order_id = result.get("order_id") or result.get("id")
        # HM-OPTIONS-REAL-FILLS-2026-09-12: real fill magnitude from Alpaca's
        # own order response (polled in submit_vertical_spread()). Alpaca's
        # own credit/debit SIGN convention for an MLEG order's filled_avg_price
        # is not independently verified anywhere in this codebase (the same
        # gap docs/XO_BACKLOG.md's HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET
        # already flagged for the close side) -- so the sign is NOT taken
        # from Alpaca. Instead |fill_price| is combined with the STRUCTURAL
        # sign of the spread type in _record_options_trade(), a mathematical
        # fact (a bull call / bear put spread is always a net debit; a bull
        # put / bear call spread is always a net credit -- the lower strike
        # of a call, or higher strike of a put, is always worth more), not an
        # assumption about Alpaca's API.
        fill_price = result.get("filled_avg_price")
    else:
        broker_order_id = getattr(result, "id", None)
        broker_order_id = str(broker_order_id) if broker_order_id else None

    trade_id = _record_options_trade(signal, broker_order_id, signal_id, fill_price)

    return ExecutionResult(
        signal_id=signal_id, ticker=signal.ticker, exit_tag=signal.exit_tag,
        status="executed",
        broker_order_id=broker_order_id,
        options_trade_id=trade_id,
        executed_at=datetime.now(timezone.utc),
    )


def _occ_symbol(underlying: str, leg: dict, expiration: Optional[str] = None) -> str:
    """Build OCC option symbol: e.g. SPY250425C00700000.

    HM-OPTIONS-CLOSE-SCHEMA-MISMATCH-2026-09-12: the canonical legs_json
    schema (HM-BULL-SPREAD-V1-SCHEMA-CANONICALIZE, 2026-05-17) stores
    {side, type, strike, qty, entry_price} per leg -- no per-leg
    'expiration' (that lives once on the options_trades row itself) and
    the option-type key is 'type', not 'option_type'. This function's open-
    side caller (_record_options_trade's payload shape) still supplies
    both `option_type` and `expiration` on its own leg dicts, so both key
    names are accepted; `expiration` is required from *some* source (either
    the `expiration` param, for a canonical-schema leg read back from the
    DB, or leg['expiration'], for the open-side payload shape).
    """
    exp_str = expiration or leg.get("expiration")
    exp = date.fromisoformat(exp_str)
    yy = exp.strftime("%y")
    mm = exp.strftime("%m")
    dd = exp.strftime("%d")
    opt_type = leg.get("type") or leg.get("option_type")
    cp = "C" if opt_type == "call" else "P"
    strike_int = int(round(leg["strike"] * 1000))
    return f"{underlying}{yy}{mm}{dd}{cp}{strike_int:08d}"


@dataclass
class CloseResult:
    position_id: int
    contracts_closed: int
    reason: str
    status: str  # "logged" | "executed" | "error"
    broker_order_id: Optional[str] = None
    closed_at: Optional[datetime] = None


def close_position(intent) -> CloseResult:
    """
    Close (partially or fully) an open options_trades position.

    Task 7b: gate is still False — logs the intent and increments
    contracts_closed_so_far in the DB. Does NOT call alpaca.

    When _EXECUTION_ENABLED flips True (Admiral sign-off), the live
    path will fire alpaca close orders and then update the DB.
    """
    if not _EXECUTION_ENABLED:
        # Log-only: record partial close in DB so scaleout ladder advances
        _increment_closed(intent.position_id, intent.contracts_to_close, intent.reason)
        return CloseResult(
            position_id=intent.position_id,
            contracts_closed=intent.contracts_to_close,
            reason=intent.reason,
            status="logged",
            closed_at=datetime.now(timezone.utc),
        )
    return _close_live(intent)


# HM-AC-Option-B (2026-05-05): per-leg fallback helper. Used for 1-leg single
# positions and >2-leg structures (iron condors). 2-leg vertical spreads use
# the atomic MLEG close path (close_vertical_spread) instead.
def _close_legs_individually(
    legs, symbol, player_id, contracts_to_close,
    close_options_position, submit_single_option,
    expiration: Optional[str] = None,
) -> list[dict]:
    """Iterate legs and call single-leg close primitives per leg.

    Long leg (side='long')  → close_options_position (sell-to-close).
    Short leg (side='short') → submit_single_option(side='buy') (BTC).

    HM-OPTIONS-CLOSE-SCHEMA-MISMATCH-2026-09-12: this used to check
    leg.get("action", "buy") -- a key the canonical legs_json schema
    (2026-05-17) never writes, so it silently defaulted every leg to "buy"
    and closed BOTH legs the same way (sell-to-close), regardless of which
    was actually long or short. Confirmed live against real stored rows
    (id 140's legs_json: {"side": "long", ...}/{"side": "short", ...}, no
    "action" key at all) before fixing, not assumed. Now reads "side"
    (falling back to "action"/"buy"|"sell" only for any pre-canonicalization
    row that might still exist).

    Pre-HM-AC-Option-B this was the only close path; now it's a fallback for
    structures the new atomic MLEG close doesn't cover (1-leg, 4-leg ICs).
    """
    out: list[dict] = []
    for leg in legs:
        occ = _occ_symbol(symbol, leg, expiration=expiration)
        side = leg.get("side")
        if side is None:
            # Legacy pre-canonicalization shape fallback.
            side = "long" if leg.get("action", "buy") == "buy" else "short"
        try:
            if side == "long":
                # Long leg: sell to close
                r = close_options_position(
                    player_id=player_id,
                    contract_symbol=occ,
                    qty=contracts_to_close,
                )
            else:
                # Short leg: buy to close
                r = submit_single_option(
                    player_id=player_id,
                    contract_symbol=occ,
                    qty=contracts_to_close,
                    side="buy",
                )
            out.append({"leg": occ, "side": side, "result": r})
        except Exception as e:
            out.append({"leg": occ, "side": side, "error": f"{type(e).__name__}: {e}"})
    return out


def _close_live(intent) -> CloseResult:
    """
    Actual close execution. Only reachable when _EXECUTION_ENABLED is True.

    Dispatches by leg count:
      - 2-leg vertical spread (BTO+STO) → close_vertical_spread (MLEG, atomic).
        HM-AC-Option-B (2026-05-05): per-leg single-leg close path was rejected
        by Alpaca with 'insufficient buying power for cash-secured put' because
        Alpaca interpreted single-leg SELL on a put-leg of an open MLEG spread
        as opening a fresh SHORT-PUT. The MLEG close uses SELL_TO_CLOSE +
        BUY_TO_CLOSE intents so Alpaca recognizes it as a true close.
      - 1-leg single position → close_options_position (long) or
        submit_single_option(side='buy') (short). Existing path preserved.
      - >2 legs (e.g. iron condor) → fallback to per-leg loop with a warning;
        proper MLEG close for 4-leg ICs is HM-AC-extension scope.

    Then increments contracts_closed_so_far in DB.
    """
    try:
        from engine.alpaca_options import (
            close_options_position, submit_single_option,
            close_vertical_spread,  # HM-AC-Option-B
        )
    except ImportError as e:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=intent.reason, status="error",
            closed_at=datetime.now(timezone.utc),
        )

    # Load position details from options_trades
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute(
            "SELECT symbol, legs_json, strategy_id, expiration, structure, "
            "entry_credit_debit, contracts, contracts_closed_so_far "
            "FROM options_trades WHERE id = ?",
            (intent.position_id,),
        )
        row = cur.fetchone()
        conn.close()
    except Exception as e:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=intent.reason, status="error",
            closed_at=datetime.now(timezone.utc),
        )

    if not row:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=f"position {intent.position_id} not found",
            status="error", closed_at=datetime.now(timezone.utc),
        )

    (symbol, legs_json_str, strategy_id, expiration, structure,
     entry_credit_debit, contracts_total, contracts_closed_before) = row
    player_id = f"strategy:{strategy_id}"
    contracts_total = contracts_total or 1
    contracts_closed_before = contracts_closed_before or 0
    is_full_close = (contracts_closed_before + intent.contracts_to_close) >= contracts_total

    try:
        legs = json.loads(legs_json_str)
    except Exception as e:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=f"legs_json parse failed: {e}",
            status="error", closed_at=datetime.now(timezone.utc),
        )

    close_results = []
    close_fill_price = None  # real Alpaca close fill magnitude, if we get one

    # HM-AC-Option-B (2026-05-05): 2-leg vertical spread → atomic MLEG close.
    if len(legs) == 2:
        # HM-OPTIONS-CLOSE-SCHEMA-MISMATCH-2026-09-12: was l.get("action")=="buy"/
        # "sell" -- a key the canonical legs_json schema never writes (confirmed
        # against real stored rows, e.g. id 140: {"side": "long"/"short", ...},
        # no "action" key at all). Every real 2-leg close since the 2026-05-17
        # canonicalization landed here as "missing buy/sell pair" and silently
        # fell through to the per-leg fallback below, which had the identical
        # bug (see _close_legs_individually's fix note) -- the atomic MLEG close
        # has never actually fired for a real canonical-schema position.
        long_leg = next((l for l in legs if l.get("side") == "long"), None)
        short_leg = next((l for l in legs if l.get("side") == "short"), None)
        if long_leg and short_leg:
            long_occ = _occ_symbol(symbol, long_leg, expiration=expiration)
            short_occ = _occ_symbol(symbol, short_leg, expiration=expiration)
            try:
                r = close_vertical_spread(
                    player_id=player_id,
                    long_symbol=long_occ,
                    short_symbol=short_occ,
                    qty=intent.contracts_to_close,
                    strategy=strategy_id,
                )
                close_results.append({"leg": f"{long_occ}+{short_occ}", "result": r})
                if isinstance(r, dict):
                    close_fill_price = r.get("filled_avg_price")
            except Exception as e:
                close_results.append({"leg": f"{long_occ}+{short_occ}",
                                      "error": f"{type(e).__name__}: {e}"})
        else:
            # Malformed 2-leg (both same side, or an unrecognized side value) —
            # fall through to per-leg.
            print(f"[executor] {intent.position_id}: 2 legs but missing long/short pair; "
                  f"falling back to per-leg close (HM-AC-Option-B-fallback)")
            close_results = _close_legs_individually(
                legs, symbol, player_id, intent.contracts_to_close,
                close_options_position, submit_single_option,
                expiration=expiration,
            )
    else:
        # HM-AC-Option-B: per-leg loop preserved for 1-leg (single position) and
        # >2-leg (iron condor — known-broken via single-leg, see HM-AC-extension TODO).
        if len(legs) > 2:
            print(f"[executor] {intent.position_id}: {len(legs)}-leg structure — "
                  f"using per-leg close (HM-AC-extension TODO for atomic MLEG close)")
        close_results = _close_legs_individually(
            legs, symbol, player_id, intent.contracts_to_close,
            close_options_position, submit_single_option,
            expiration=expiration,
        )

    errors = [r for r in close_results if "error" in r]
    if errors:
        return CloseResult(
            position_id=intent.position_id, contracts_closed=0,
            reason=f"leg close failed: {errors}",
            status="error", closed_at=datetime.now(timezone.utc),
        )

    _increment_closed(intent.position_id, intent.contracts_to_close, intent.reason)

    # HM-OPTIONS-REAL-FILLS-2026-09-12: a real close fill on a FULL close of a
    # known debit/credit structure gets a real exit_credit_debit + pnl, using
    # the same structural-sign trick as the open side (see _DEBIT_STRUCTURES/
    # _CREDIT_STRUCTURES) -- closing a debit spread is a credit (you're
    # selling it back), closing a credit spread is a debit (buying it back).
    # This is the opposite-of-entry-sign relationship the restatement
    # script's own hand-verified formula already uses
    # (exit_credit_debit = sum_legs(-sign_open * exit_price * ...)) -- not a
    # new assumption, the same one already proven correct against real rows.
    # Partial closes and any close whose fill never confirmed (poll timeout)
    # are left exactly as before this fix (pnl/exit_credit_debit untouched).
    if (is_full_close and close_fill_price is not None and close_fill_price >= 0
            and structure in _DEBIT_STRUCTURES | _CREDIT_STRUCTURES
            and entry_credit_debit is not None):
        exit_credit_debit = (abs(close_fill_price) if structure in _DEBIT_STRUCTURES
                             else -abs(close_fill_price))
        pnl = entry_credit_debit + exit_credit_debit
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute(
                "UPDATE options_trades SET exit_credit_debit=?, pnl=?, "
                "restatement_basis='real_fill' WHERE id=?",
                (exit_credit_debit, pnl, intent.position_id),
            )
            conn.commit()
            conn.close()
            print(f"[executor] position #{intent.position_id}: real close fill "
                  f"exit_credit_debit={exit_credit_debit:.2f} pnl={pnl:.2f}")
        except Exception as e:
            print(f"[executor] pnl writeback failed for #{intent.position_id}: {e}")

    broker_ref = str([r.get("result", {}).get("order_id") for r in close_results])
    return CloseResult(
        position_id=intent.position_id,
        contracts_closed=intent.contracts_to_close,
        reason=intent.reason,
        status="executed",
        broker_order_id=broker_ref,
        closed_at=datetime.now(timezone.utc),
    )


def _increment_closed(position_id: int, count: int, reason: str,
                      total: int = 0) -> None:
    """Increment contracts_closed_so_far and update exec_status if fully closed.

    exit_date and exit_reason are only written when the position transitions
    to 'closed' (contracts_closed_so_far reaches contracts). Partial closes
    leave both fields untouched.

    The `total` parameter is accepted for call-site clarity but is unused —
    the SQL compares against the `contracts` column directly, so a single
    UPDATE handles both partial and full closes atomically.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.execute(
            """
            UPDATE options_trades
               SET contracts_closed_so_far =
                       MIN(contracts, contracts_closed_so_far + ?),
                   exec_status =
                       CASE WHEN contracts_closed_so_far + ? >= contracts
                            THEN 'closed' ELSE 'open' END,
                   -- HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET 2026-07-10: this
                   -- was the only live close path for bull_spread_v1 /
                   -- bull_call_spread_v1 / bear_put_spread_v1 and NEVER set
                   -- `status` (only `exec_status`) -- every P&L/win-rate
                   -- query in the system filters on status='closed', so a
                   -- position closed via this path stayed permanently
                   -- invisible to reporting. pnl/exit_credit_debit are NOT
                   -- computed here (see docs/XO_BACKLOG.md
                   -- HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET -- MLEG close
                   -- fill-price sign convention is unverified; leaving pnl
                   -- NULL is the existing, already-tolerated state rather
                   -- than risk a silently-backwards number).
                   status =
                       CASE WHEN contracts_closed_so_far + ? >= contracts
                            THEN 'closed' ELSE status END,
                   exit_date =
                       CASE WHEN contracts_closed_so_far + ? >= contracts
                            THEN CURRENT_TIMESTAMP ELSE exit_date END,
                   exit_reason =
                       CASE WHEN contracts_closed_so_far + ? >= contracts
                            THEN COALESCE(exit_reason || ' | ', '') || ?
                            ELSE exit_reason END
             WHERE id = ?
            """,
            (count, count, count, count, count, reason, position_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            print(f"[executor] _increment_closed: position {position_id} not found")
            return
        row = conn.execute(
            "SELECT contracts_closed_so_far, contracts, exec_status "
            "FROM options_trades WHERE id = ?", (position_id,)
        ).fetchone()
        if row:
            new_closed, total_ct, new_status = row
            print(f"[executor] position #{position_id}: closed {count} contracts "
                  f"({new_closed}/{total_ct}) reason={reason} status={new_status}")
    except Exception as e:
        print(f"[executor] _increment_closed failed: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


# HM-OPTIONS-REAL-FILLS-2026-09-12: which vertical-spread structures are
# always a net DEBIT vs always a net CREDIT -- a mathematical property (the
# lower strike of a call spread, or the higher strike of a put spread, is
# always worth more), not a per-trade guess. Used to sign Alpaca's real
# (unsigned) fill magnitude correctly without trusting an unverified sign
# convention from the broker's own MLEG response.
_DEBIT_STRUCTURES = {"bull_call_spread", "bear_put_spread"}
_CREDIT_STRUCTURES = {"bull_put_spread", "bear_call_spread"}


def _record_options_trade(signal: StrategySignal, order_id: Optional[str],
                          signal_id: Optional[int],
                          fill_price: Optional[float] = None) -> Optional[int]:
    """Record open spread into options_trades. Matches real legacy schema.

    HM-OPTIONS-REAL-FILLS-2026-09-12: fill_price, when given, is the real
    Alpaca fill magnitude (see _execute_live()'s comment on why the sign is
    derived structurally rather than trusted from Alpaca). When present,
    entry_credit_debit is the real fill (signed by the spread's structural
    debit/credit type) and restatement_basis is stamped 'real_fill'
    immediately -- this row will never need the restatement script, its
    price already IS the verified answer. legs_json's own per-leg
    entry_price stays the pre-trade quote (contract identity -- strike,
    expiration, side -- is what matters there; entry_credit_debit alone is
    authoritative for P&L). When fill_price is absent (poll timed out,
    order still working), behavior is unchanged from before this fix -- the
    payload's pre-trade quote is recorded as entry_credit_debit, exactly as
    it always was, and restatement_basis stays NULL for a later pass to
    reconcile if needed.
    """
    import json as _json
    try:
        conn = sqlite3.connect(str(DB_PATH))
        payload = signal.payload
        structure = payload.get("structure", "unknown")

        # HM-BULL-SPREAD-V1-SCHEMA-CANONICALIZE 2026-05-17 G50.a/G51-A/G52-strict:
        # writer emits canonical {side, type, strike, qty, entry_price} schema
        # (was legacy {action, option_type, strike, expiration, premium}).
        # Readers — engine/options_utils.py, engine/reconciliation.py,
        # strategies/exit_manager.py, scripts/kill_bull_spread.py — patched
        # to canonical-only in same commit. 25 legacy rows migrated via DB
        # UPDATE same commit (id=28 active + 20 closed + 4 failed). Per Admiral
        # G50.a (inline) + G51-A (full scope) + G52-strict (no read adapter).
        contracts_int = int(payload.get("contracts") or 1)
        legs_json = _json.dumps([
            {
                "side":        "long" if payload["long_leg"]["action"] == "buy" else "short",
                "type":        payload["long_leg"]["option_type"],
                "strike":      payload["long_leg"]["strike"],
                "qty":         contracts_int,
                "entry_price": payload["long_leg"]["premium"],
            },
            {
                "side":        "long" if payload["short_leg"]["action"] == "buy" else "short",
                "type":        payload["short_leg"]["option_type"],
                "strike":      payload["short_leg"]["strike"],
                "qty":         contracts_int,
                "entry_price": payload["short_leg"]["premium"],
            },
        ])

        # entry_credit_debit: positive = net credit received, negative = net debit paid
        net_debit = payload.get("net_debit", 0) or 0
        net_credit = payload.get("net_credit", 0) or 0
        entry_credit_debit = net_credit - net_debit
        restatement_basis = None

        # HM-OPTIONS-REAL-FILLS-2026-09-12: a real fill overrides the
        # pre-trade quote. Sign comes from the spread's own structural type
        # (see _DEBIT_STRUCTURES/_CREDIT_STRUCTURES above), not from Alpaca's
        # response -- that sign is unverified, the magnitude is not.
        if fill_price is not None and fill_price >= 0:
            if structure in _DEBIT_STRUCTURES:
                entry_credit_debit = -abs(fill_price)
                restatement_basis = "real_fill"
            elif structure in _CREDIT_STRUCTURES:
                entry_credit_debit = abs(fill_price)
                restatement_basis = "real_fill"
            # else: unknown structure (shouldn't reach here, the whitelist
            # above _execute_live already rejects it) -- fall back silently
            # to the pre-trade quote already computed above.

        # agent_id prefix lets us filter strategy vs legacy agent trades
        agent_id = f"strategy:{signal.strategy_id}"

        # Canonical expiration: long leg's expiration date
        expiration = payload["long_leg"]["expiration"]

        cur = conn.execute("""
            INSERT INTO options_trades
                (agent_id, symbol, structure, expiration, legs_json,
                 entry_credit_debit, entry_date,
                 strategy_id, exit_tag, broker_order_id, signal_id, exec_status,
                 restatement_basis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
        """, (
            agent_id, signal.ticker, structure, expiration, legs_json,
            entry_credit_debit, datetime.now(timezone.utc).isoformat(),
            signal.strategy_id, signal.exit_tag, order_id, signal_id,
            restatement_basis,
        ))
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        print(f"[executor] record failed: {e}")
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass
