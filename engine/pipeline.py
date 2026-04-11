"""
TradeMinds — Full Pipeline
===========================
End-to-end automation: Chekov scanner → Debate Engine → Scenario Modeler
→ Portfolio Optimizer → Rebalancer.

Stages:
  1. Chekov scanner  — get_todays_signals() → top N tickers by confidence
  2. Debate Engine   — 12-agent bull/bear + Picard + Risk Triad per ticker
  3. Scenario Modeler — Bull / Base / Bear with EV per ticker
  4. Portfolio Optimizer — concentration/sector/correlation flags + actions
  5. Rebalancer      — size trades, optionally execute on Alpaca

Usage (CLI):
    python -m engine.pipeline steve-webull
    python -m engine.pipeline steve-webull --tickers 5
    python -m engine.pipeline steve-webull --tickers 10 --execute
    python -m engine.pipeline steve-webull --tickers AAPL,MSFT,NVDA

Usage (import):
    from engine.pipeline import run_pipeline
    result = run_pipeline("steve-webull", top_n=10)
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime
from typing import Any

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TRADER_DB = "data/trader.db"
DEFAULT_PLAYER = "steve-webull"
DEFAULT_TOP_N = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pipeline] %(levelname)s: %(message)s",
)
logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT NOT NULL,
                tickers_scanned TEXT,
                tickers_debated TEXT,
                debate_results TEXT,
                scenario_results TEXT,
                optimizer_summary TEXT,
                rebalancer_summary TEXT,
                total_seconds REAL,
                error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.commit()


# ---------------------------------------------------------------------------
# Stage 1 — Chekov scanner
# ---------------------------------------------------------------------------

def _get_scanner_tickers(top_n: int) -> list[dict]:
    """Get today's top signals from Chekov. Falls back to recent discoveries."""
    # Primary: today's strategy signals
    try:
        from engine.strategies import get_todays_signals
        signals = get_todays_signals()
        if signals:
            logger.info(f"Chekov found {len(signals)} signals today")
            return signals[:top_n]
    except Exception as e:
        logger.warning(f"get_todays_signals failed: {e}")

    # Fallback 1: recent discoveries
    try:
        from engine.discovery_scanner import get_recent_discoveries
        discoveries = get_recent_discoveries(limit=top_n * 2)
        if discoveries:
            logger.info(f"Using {len(discoveries)} recent discoveries as fallback")
            return [{"ticker": d.get("ticker") or d.get("symbol"), "confidence": 0.5}
                    for d in discoveries if d.get("ticker") or d.get("symbol")][:top_n]
    except Exception as e:
        logger.warning(f"get_recent_discoveries failed: {e}")

    # Fallback 2: most active tickers
    try:
        from engine.discovery_scanner import get_most_active_tickers
        rows = get_most_active_tickers(top_n)
        if rows:
            logger.info(f"Using {len(rows)} most active tickers as fallback")
            return [
                {"ticker": r.get("symbol") or r.get("ticker") or r, "confidence": 0.3}
                for r in rows
                if isinstance(r, dict) and (r.get("symbol") or r.get("ticker"))
                or isinstance(r, str) and r
            ][:top_n]
    except Exception as e:
        logger.warning(f"get_most_active_tickers failed: {e}")

    logger.error("All scanner sources failed — no tickers to process")
    return []


def _parse_ticker_list(raw: str) -> list[dict]:
    """Parse comma-separated ticker string into signal dicts."""
    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    return [{"ticker": t, "confidence": 1.0} for t in tickers]


# ---------------------------------------------------------------------------
# Stage 2 — Debate Engine (per ticker)
# ---------------------------------------------------------------------------

def _run_debate(ticker: str) -> dict:
    """Run full debate for a ticker. Returns debate result dict."""
    t0 = time.time()
    try:
        from engine.debate_engine import run_full_debate
        result = asyncio.run(run_full_debate(ticker))
        elapsed = time.time() - t0
        if result:
            decision = result.get("picard", {}).get("decision", "UNKNOWN")
            conviction = result.get("risk_triad", {}).get("adjusted_conviction", "?")
            logger.info(f"  {ticker}: {decision} conviction={conviction}/10 ({elapsed:.0f}s)")
            return result
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"  {ticker}: debate failed ({elapsed:.0f}s): {e}")
    return {"ticker": ticker, "error": "debate failed"}


# ---------------------------------------------------------------------------
# Stage 3 — Scenario Modeler (per ticker)
# ---------------------------------------------------------------------------

def _run_scenario(ticker: str, debate: dict) -> dict:
    """Run scenario model for a ticker. Returns scenario result dict."""
    t0 = time.time()
    try:
        from engine.scenario_modeler import run_scenario_model
        # Pass price from debate context if available
        price = None
        if debate.get("stock_data") and isinstance(debate["stock_data"], dict):
            price = debate["stock_data"].get("price")
        result = run_scenario_model(ticker, price_override=price)
        elapsed = time.time() - t0
        if result and "error" not in result:
            ev = result.get("expected_value_pct")
            ev_str = f"{ev:+.1f}%" if ev is not None else "N/A"
            logger.info(f"  {ticker}: EV={ev_str} ({elapsed:.0f}s)")
            return result
        logger.warning(f"  {ticker}: scenario failed: {result.get('error', '?')}")
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"  {ticker}: scenario error ({elapsed:.0f}s): {e}")
    return {"ticker": ticker, "error": "scenario failed"}


# ---------------------------------------------------------------------------
# Stage 4 — Portfolio Optimizer (once, after all tickers)
# ---------------------------------------------------------------------------

def _run_optimizer(player_id: str) -> dict:
    """Run portfolio optimizer for player. Returns optimizer result dict."""
    t0 = time.time()
    try:
        from engine.portfolio_optimizer import run_optimizer
        result = run_optimizer(player_id)
        elapsed = time.time() - t0
        if result and "error" not in result:
            n = len(result.get("actions", []))
            summary = result.get("summary", "")[:80]
            logger.info(f"Optimizer: {n} actions for {player_id} ({elapsed:.0f}s)")
            logger.info(f"  Summary: {summary}")
            return result
        logger.warning(f"Optimizer failed: {result.get('error', '?')}")
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"Optimizer error ({elapsed:.0f}s): {e}")
    return {"player_id": player_id, "error": "optimizer failed"}


# ---------------------------------------------------------------------------
# Stage 5 — Rebalancer (once, at end)
# ---------------------------------------------------------------------------

def _run_rebalancer(player_id: str, execute: bool = False) -> dict:
    """Run rebalancer for player. Returns rebalancer result dict."""
    t0 = time.time()
    try:
        from engine.rebalancer import run_rebalancer
        result = run_rebalancer(player_id, execute=execute)
        elapsed = time.time() - t0
        if result and "error" not in result:
            trades = result.get("trades", [])
            logger.info(f"Rebalancer: {len(trades)} trades for {player_id} ({elapsed:.0f}s)")
            for t in trades[:5]:
                sym = t.get("symbol", "?")
                action = t.get("action", "?")
                qty = t.get("qty", "?")
                price = t.get("price")
                pstr = f"@${price:.2f}" if price else ""
                executed = " [EXECUTED]" if t.get("executed") else ""
                logger.info(f"  {action} {qty}sh {sym}{pstr}{executed}")
            return result
        logger.warning(f"Rebalancer failed: {result.get('error', '?')}")
    except Exception as e:
        elapsed = time.time() - t0
        logger.error(f"Rebalancer error ({elapsed:.0f}s): {e}")
    return {"player_id": player_id, "error": "rebalancer failed"}


# ---------------------------------------------------------------------------
# Save run to DB
# ---------------------------------------------------------------------------

def _save_run(
    player_id: str,
    scanned: list[str],
    debated: list[str],
    debate_results: list[dict],
    scenario_results: list[dict],
    optimizer: dict,
    rebalancer: dict,
    total_seconds: float,
    error: str | None = None,
) -> int:
    """Persist pipeline run summary. Returns row id."""
    init_db()

    def _summarize_debates(results: list[dict]) -> list[dict]:
        return [
            {
                "ticker": r.get("ticker"),
                "decision": r.get("picard", {}).get("decision"),
                "conviction": r.get("risk_triad", {}).get("adjusted_conviction"),
            }
            for r in results
        ]

    def _summarize_scenarios(results: list[dict]) -> list[dict]:
        return [
            {
                "ticker": r.get("ticker"),
                "ev_pct": r.get("expected_value_pct"),
                "bull_pct": r.get("bull", {}).get("probability"),
                "bear_pct": r.get("bear", {}).get("probability"),
            }
            for r in results
        ]

    try:
        with _conn() as c:
            cur = c.execute(
                """INSERT INTO pipeline_runs
                   (player_id, tickers_scanned, tickers_debated,
                    debate_results, scenario_results,
                    optimizer_summary, rebalancer_summary,
                    total_seconds, error)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    player_id,
                    json.dumps(scanned),
                    json.dumps(debated),
                    json.dumps(_summarize_debates(debate_results)),
                    json.dumps(_summarize_scenarios(scenario_results)),
                    json.dumps({"summary": optimizer.get("summary"), "actions": optimizer.get("actions", [])}),
                    json.dumps({"trades": rebalancer.get("trades", [])}),
                    total_seconds,
                    error,
                ),
            )
            c.commit()
            return cur.lastrowid
    except Exception as e:
        logger.warning(f"DB save failed: {e}")
        return -1


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    player_id: str = DEFAULT_PLAYER,
    top_n: int = DEFAULT_TOP_N,
    execute: bool = False,
    tickers_override: list[str] | None = None,
) -> dict:
    """
    Run full pipeline end-to-end.

    Args:
        player_id:        Portfolio owner (e.g. "steve-webull")
        top_n:            Max tickers to process from scanner
        execute:          If True, send approved trades to Alpaca (blocked for human players)
        tickers_override: If provided, skip scanner and use these tickers

    Returns:
        Full result dict with per-stage output.
    """
    init_db()
    t_start = time.time()
    run_ts = datetime.now().isoformat()

    logger.info("=" * 60)
    logger.info(f"USS TradeMinds Pipeline — {run_ts}")
    logger.info(f"Player: {player_id}  |  Max tickers: {top_n}  |  Execute: {execute}")
    logger.info("=" * 60)

    # -----------------------------------------------------------------------
    # Stage 1: Scanner
    # -----------------------------------------------------------------------
    logger.info("\n[1/5] CHEKOV SCANNER")
    if tickers_override:
        signals = [{"ticker": t.upper(), "confidence": 1.0} for t in tickers_override]
        logger.info(f"  Using override tickers: {[s['ticker'] for s in signals]}")
    else:
        signals = _get_scanner_tickers(top_n)

    if not signals:
        err = "No tickers from scanner — aborting pipeline"
        logger.error(err)
        return {"error": err, "player_id": player_id}

    tickers = [s["ticker"] for s in signals]
    logger.info(f"  Tickers ({len(tickers)}): {', '.join(tickers)}")

    # -----------------------------------------------------------------------
    # Stage 2: Debate Engine (sequential to avoid Ollama queue exhaustion)
    # -----------------------------------------------------------------------
    logger.info(f"\n[2/5] DEBATE ENGINE ({len(tickers)} tickers)")
    debate_results: list[dict] = []
    debated_tickers: list[str] = []

    for ticker in tickers:
        logger.info(f"  Debating {ticker}...")
        result = _run_debate(ticker)
        debate_results.append(result)
        if "error" not in result:
            debated_tickers.append(ticker)

    # Filter: only proceed with LEAN_BUY or BUY decisions
    bullish_tickers = []
    for r in debate_results:
        decision = r.get("picard", {}).get("decision", "")
        if decision in ("BUY", "LEAN_BUY", "STRONG_BUY"):
            bullish_tickers.append(r.get("ticker", ""))

    if bullish_tickers:
        logger.info(f"  Bullish decisions: {', '.join(bullish_tickers)}")
    else:
        logger.info("  No bullish decisions — proceeding with all debated tickers for scenarios")
        bullish_tickers = debated_tickers  # still run scenarios for context

    # -----------------------------------------------------------------------
    # Stage 3: Scenario Modeler (for each successfully debated ticker)
    # -----------------------------------------------------------------------
    logger.info(f"\n[3/5] SCENARIO MODELER ({len(bullish_tickers)} tickers)")
    scenario_results: list[dict] = []
    debate_by_ticker = {r.get("ticker"): r for r in debate_results}

    for ticker in bullish_tickers:
        logger.info(f"  Modeling {ticker}...")
        debate = debate_by_ticker.get(ticker, {})
        result = _run_scenario(ticker, debate)
        scenario_results.append(result)

    # -----------------------------------------------------------------------
    # Stage 4: Portfolio Optimizer
    # -----------------------------------------------------------------------
    logger.info(f"\n[4/5] PORTFOLIO OPTIMIZER ({player_id})")
    optimizer_result = _run_optimizer(player_id)

    # -----------------------------------------------------------------------
    # Stage 5: Rebalancer
    # -----------------------------------------------------------------------
    logger.info(f"\n[5/5] REBALANCER ({player_id}  execute={execute})")
    rebalancer_result = _run_rebalancer(player_id, execute=execute)

    # -----------------------------------------------------------------------
    # Wrap up
    # -----------------------------------------------------------------------
    total_seconds = time.time() - t_start

    run_id = _save_run(
        player_id=player_id,
        scanned=tickers,
        debated=debated_tickers,
        debate_results=debate_results,
        scenario_results=scenario_results,
        optimizer=optimizer_result,
        rebalancer=rebalancer_result,
        total_seconds=total_seconds,
    )

    result = {
        "run_id": run_id,
        "player_id": player_id,
        "tickers_scanned": tickers,
        "tickers_debated": debated_tickers,
        "debate_results": debate_results,
        "scenario_results": scenario_results,
        "optimizer": optimizer_result,
        "rebalancer": rebalancer_result,
        "total_seconds": total_seconds,
        "created_at": run_ts,
    }

    logger.info("\n" + "=" * 60)
    logger.info(f"Pipeline complete in {total_seconds:.0f}s  (run_id={run_id})")
    logger.info("=" * 60)

    return result


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------

def _print_result(result: dict):
    if "error" in result:
        print(f"\nPipeline Error: {result['error']}")
        return

    print(f"\n{'=' * 65}")
    print(f"PIPELINE RUN #{result.get('run_id', '?')}  |  {result['player_id']}")
    print(f"Completed in {result['total_seconds']:.0f}s  |  {result['created_at'][:19]}")
    print("=" * 65)

    # Scanner
    tickers = result.get("tickers_scanned", [])
    print(f"\n[1] Scanner: {len(tickers)} tickers → {', '.join(tickers)}")

    # Debates
    print(f"\n[2] Debates:")
    for r in result.get("debate_results", []):
        ticker = r.get("ticker", "?")
        if "error" in r:
            print(f"    {ticker}: ERROR")
            continue
        decision = r.get("picard", {}).get("decision", "?")
        conviction = r.get("risk_triad", {}).get("adjusted_conviction", "?")
        print(f"    {ticker}: {decision}  conviction={conviction}/10")

    # Scenarios
    print(f"\n[3] Scenarios:")
    for r in result.get("scenario_results", []):
        ticker = r.get("ticker", "?")
        if "error" in r:
            print(f"    {ticker}: ERROR")
            continue
        ev = r.get("expected_value_pct")
        ev_str = f"{ev:+.1f}%" if ev is not None else "N/A"
        bull = r.get("bull", {})
        base = r.get("base", {})
        bear = r.get("bear", {})
        print(f"    {ticker}: EV={ev_str}  "
              f"bull={bull.get('probability', '?')}%  "
              f"base={base.get('probability', '?')}%  "
              f"bear={bear.get('probability', '?')}%")

    # Optimizer
    opt = result.get("optimizer", {})
    print(f"\n[4] Optimizer:")
    if "error" in opt:
        print(f"    ERROR: {opt['error']}")
    else:
        for a in opt.get("actions", []):
            sym = a.get("symbol", "?").ljust(6)
            action = a.get("action", "?").ljust(8)
            urgency = a.get("urgency", "?")
            print(f"    {action} {sym}  [{urgency}]  {a.get('rationale', '')}")
        if opt.get("summary"):
            print(f"    → {opt['summary']}")

    # Rebalancer
    reb = result.get("rebalancer", {})
    print(f"\n[5] Rebalancer:")
    if "error" in reb:
        print(f"    ERROR: {reb['error']}")
    else:
        for t in reb.get("trades", []):
            sym = t.get("symbol", "?").ljust(6)
            action = t.get("action", "?").ljust(6)
            qty = str(t.get("qty", "?")).ljust(4)
            price = t.get("price")
            pstr = f"@${price:.2f}" if price else ""
            executed = " ✓" if t.get("executed") else ""
            print(f"    {action} {qty}sh {sym}{pstr}{executed}")

    print("=" * 65)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="TradeMinds Full Pipeline",
        usage="pipeline.py [player_id] [--tickers N_OR_COMMA_LIST] [--execute]",
    )
    parser.add_argument(
        "player_id",
        nargs="?",
        default=DEFAULT_PLAYER,
        help=f"Player ID (default: {DEFAULT_PLAYER})",
    )
    parser.add_argument(
        "--tickers",
        default=str(DEFAULT_TOP_N),
        help=(
            f"Number of top tickers from scanner (default: {DEFAULT_TOP_N}), "
            "OR comma-separated list to skip scanner entirely (e.g. AAPL,MSFT,NVDA)"
        ),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Send approved trades to Alpaca (blocked for human players)",
    )
    args = parser.parse_args()

    # Parse --tickers: int → top_n from scanner; str with comma → override list
    tickers_override = None
    top_n = DEFAULT_TOP_N
    if "," in args.tickers:
        tickers_override = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        top_n = len(tickers_override)
    else:
        try:
            top_n = int(args.tickers)
        except ValueError:
            # Single ticker without comma
            tickers_override = [args.tickers.strip().upper()]
            top_n = 1

    result = run_pipeline(
        player_id=args.player_id,
        top_n=top_n,
        execute=args.execute,
        tickers_override=tickers_override,
    )

    _print_result(result)

    if "error" in result:
        sys.exit(1)
