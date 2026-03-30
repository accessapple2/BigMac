"""
CrewAI pipeline orchestrating the full strategy-writing flow.

Scout → Architect → Backtester → Critic → Commander
"""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime

from crewai import Crew, Task, Process

from shared.ollama_lock import OllamaLock

log = logging.getLogger("crew.pipeline")


def _learning_context() -> str:
    try:
        from crew.learning import get_learning_context
        return get_learning_context()
    except Exception as e:
        log.warning(f"[pipeline] Could not load learning context: {e}")
        return ""

from crew.agents import (
    create_scout,
    create_architect,
    create_backtester,
    create_critic,
    create_commander,
)

DB_PATH = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _log_run(run_type, trigger, agents_used, strategy_id, debate_rounds,
             revision_count, outcome, tokens, cost, duration, error=None):
    """Log every crew run to crew_runs table."""
    conn = _db()
    try:
        conn.execute(
            """INSERT INTO crew_runs (
                run_type, trigger, agents_used, strategy_id, debate_rounds,
                revision_count, outcome, total_tokens_used, total_cost_usd,
                duration_seconds, error_log
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_type, trigger, agents_used, strategy_id, debate_rounds,
             revision_count, outcome, tokens, cost, duration, error),
        )
        conn.commit()
    finally:
        conn.close()


class CrewPipeline:
    """Orchestrates the full CrewAI strategy-writing pipeline."""

    def run_full_pipeline(
        self,
        focus_area: str = "market opportunities",
        target_asset_class: str = "stock",
        target_portfolio_id: int = 1,
        trigger: str = "manual",
    ) -> dict:
        """Run the full sequential crew: Scout → Architect → Backtester → Critic → Commander."""
        start = time.time()
        error = None
        outcome = "unknown"
        strategy_id = None

        try:
            # Create agents
            scout = create_scout()
            architect = create_architect()
            backtester = create_backtester()
            critic = create_critic()
            commander = create_commander()

            # Load learning context (past trade outcomes) to guide this run
            learning_ctx = _learning_context()

            # Define tasks with chained context
            scout_task = Task(
                description=(
                    f"{learning_ctx}\n\n"
                    f"Scan all available data sources for opportunities in: {focus_area}. "
                    f"Target asset class: {target_asset_class}. "
                    "STEP 1: Call get_live_discoveries to see what the ship's scanners have flagged. "
                    "STEP 2: Call scan_finviz_elite with 'unusual_volume' and 'gainers' for live Finviz data. "
                    "STEP 3: Call query_news_sources for relevant news on the best candidates. "
                    "Pick ONE best opportunity — the ticker with the strongest multi-source confirmation. "
                    "Produce a scout brief with the thesis, why the crowd is wrong, key catalysts, and timing."
                ),
                expected_output=(
                    "A structured scout brief with: opportunity_name, ticker (single best pick), "
                    "thesis, catalysts, contrarian_angle, timing, conviction (1-10), asset_class suggestion. "
                    "Include which data sources confirmed the opportunity."
                ),
                agent=scout,
            )

            architect_task = Task(
                description=(
                    "Take the Scout's opportunity brief and design a complete executable strategy. "
                    "STEP 1 — LYNCH SCREEN (mandatory): Call lookup_lynch_fundamentals for EVERY "
                    "target ticker. Record P/E, PEG, gross margin trend, cash position, debt ratio, "
                    "and insider activity. Include the full lynch_screen object in your JSON output. "
                    "If more than 3 of 6 fields are N/A, set speculative=true on the strategy. "
                    "STEP 2 — STRATEGY DESIGN: Output a JSON strategy with: name, asset_class, "
                    "direction, thesis, entry_rules (specific conditions), exit_rules (profit "
                    "targets + time stops), stop_loss_rule (exact percentage or level), "
                    "position_size_rule, target_tickers, lynch_screen, speculative, and vehicle "
                    "choice. For options: specify option_strategy (calls/puts/spreads). "
                    "For spreads: specify spread_config with legs. "
                    "Check backtest history for similar past strategies to learn from."
                ),
                expected_output=(
                    "A complete strategy JSON with all fields populated: name, asset_class, "
                    "direction, thesis, entry_rules, exit_rules, stop_loss_rule, "
                    "position_size_rule, target_tickers, option_strategy (if applicable), "
                    "spread_config (if applicable), conviction_score, lynch_screen (object with "
                    "pe, peg, gross_margin, cash_position, debt_ratio, insider_activity), "
                    "speculative (boolean — true if >3 Lynch fields are N/A)."
                ),
                agent=architect,
                context=[scout_task],
            )

            backtest_task = Task(
                description=(
                    "Validate the Architect's strategy through backtesting analysis. "
                    "Evaluate expected Sharpe ratio, max drawdown, win rate, and profit factor "
                    "based on historical patterns and similar strategies in backtest_history. "
                    "ALL results MUST be recorded. Flag any strategy with Sharpe < 0.5 "
                    "or max drawdown > 25%. Provide honest assessment even if bearish."
                ),
                expected_output=(
                    "Backtest report with: estimated_sharpe, estimated_max_drawdown, "
                    "estimated_win_rate, estimated_profit_factor, similar_strategies_performance, "
                    "risk_flags, overall_assessment (pass/fail/marginal)."
                ),
                agent=backtester,
                context=[architect_task],
            )

            critic_task = Task(
                description=(
                    "Score the strategy on 6 dimensions (1-10 each): "
                    "1) Thesis strength — is the edge real? "
                    "2) Risk/reward — asymmetric upside? "
                    "3) Backtest quality — does the data support it? "
                    "4) Market regime fit — right strategy for current conditions? "
                    "5) Vehicle choice — is this the best instrument? "
                    "6) Portfolio fit — does it complement existing positions? "
                    f"Check exposure in portfolio {target_portfolio_id}. "
                    "Score < 6 on any dimension = REJECT with specific revision requests. "
                    "SPECULATIVE GATE: If the Architect flagged speculative=true (>3 Lynch "
                    "fundamental fields were N/A), the conviction_score threshold rises from "
                    "6 to 8. Any speculative strategy scoring below 8 overall MUST be REJECTED. "
                    "Explicitly note which Lynch fields were missing and why you believe the "
                    "edge exists despite thin fundamental data. "
                    "Maximum 2 revision cycles."
                ),
                expected_output=(
                    "Critic scorecard with scores for each dimension, overall_score (average), "
                    "verdict (APPROVE/REJECT/REVISE), revision_requests (if any), "
                    "portfolio_fit_notes, concentration_risk_check, "
                    "speculative_flag_acknowledged (bool), lynch_gaps_noted (list of N/A fields)."
                ),
                agent=critic,
                context=[architect_task, backtest_task],
            )

            commander_task = Task(
                description=(
                    "Make the final go/no-go decision. Review all crew inputs. "
                    "CRITICAL: NEVER deploy to any portfolio with is_human=1 (Webull). "
                    "Alpaca Paper (portfolio_id=1) is the ONLY allowed destination. "
                    "If GO — you MUST complete BOTH steps in order:\n"
                    "  STEP 1: Call save_strategy with status='approved' and all strategy fields.\n"
                    "  STEP 2: Call execute_paper_trade with a JSON object containing ticker, "
                    "direction, dollar_amount (default 2000), stop_loss_pct and take_profit_pct "
                    "from the strategy's rules, and the strategy_id from step 1.\n"
                    "If NO-GO: call save_strategy with status='rejected'. "
                    "Do NOT end the task without calling execute_paper_trade on a GO decision."
                ),
                expected_output=(
                    "Commander decision with: verdict (GO/NO-GO), and if GO: "
                    "the save_strategy result (strategy_id), the execute_paper_trade result "
                    "(executed=true, ticker, qty, alpaca_order_id, dollar_value), "
                    "stop_loss and take_profit prices set."
                ),
                agent=commander,
                context=[scout_task, architect_task, backtest_task, critic_task],
            )

            # Assemble and run crew
            crew = Crew(
                agents=[scout, architect, backtester, critic, commander],
                tasks=[scout_task, architect_task, backtest_task, critic_task, commander_task],
                process=Process.sequential,
                verbose=True,
            )

            log.info("Waiting for Ollama lock (arena scanner may be running)...")
            with OllamaLock("crew_full_pipeline"):
                log.info("Ollama lock acquired, crew pipeline starting...")
                result = crew.kickoff()
            outcome = "completed"

            # Try to extract strategy_id from result
            try:
                result_str = str(result)
                if '"id"' in result_str:
                    parsed = json.loads(result_str)
                    strategy_id = parsed.get("id")
            except (json.JSONDecodeError, TypeError):
                pass

            return {
                "status": "completed",
                "result": str(result),
                "strategy_id": strategy_id,
                "duration": round(time.time() - start, 1),
            }

        except Exception as e:
            error = str(e)
            outcome = "error"
            return {
                "status": "error",
                "error": error,
                "duration": round(time.time() - start, 1),
            }

        finally:
            _log_run(
                run_type="full_pipeline",
                trigger=trigger,
                agents_used="scout,architect,backtester,critic,commander",
                strategy_id=strategy_id,
                debate_rounds=1,
                revision_count=0,
                outcome=outcome,
                tokens=0,
                cost=0.0,
                duration=round(time.time() - start, 1),
                error=error,
            )

    def run_scout_only(self, focus_area: str = "market opportunities") -> dict:
        """Quick opportunity scan using just the Scout agent."""
        start = time.time()
        error = None
        outcome = "unknown"

        try:
            scout = create_scout()

            task = Task(
                description=(
                    f"Quick scan for opportunities in: {focus_area}. "
                    "Check news, congressional trades, and metals. "
                    "Return your top 3 opportunities ranked by conviction."
                ),
                expected_output=(
                    "Top 3 opportunities with: name, ticker(s), thesis, conviction (1-10), "
                    "asset_class, urgency (immediate/this_week/this_month)."
                ),
                agent=scout,
            )

            crew = Crew(agents=[scout], tasks=[task], process=Process.sequential, verbose=True)

            log.info("Waiting for Ollama lock (arena scanner may be running)...")
            with OllamaLock("crew_scout_only", timeout=60):
                log.info("Ollama lock acquired, scout scan starting...")
                result = crew.kickoff()
            outcome = "completed"

            return {"status": "completed", "result": str(result), "duration": round(time.time() - start, 1)}

        except Exception as e:
            error = str(e)
            outcome = "error"
            return {"status": "error", "error": error, "duration": round(time.time() - start, 1)}

        finally:
            _log_run("scout_only", "manual", "scout", None, 0, 0, outcome, 0, 0.0,
                     round(time.time() - start, 1), error)

    def run_review_existing(self, strategy_id: int) -> dict:
        """Re-evaluate an existing strategy through Critic + Commander."""
        start = time.time()
        error = None
        outcome = "unknown"

        try:
            conn = _db()
            row = conn.execute("SELECT * FROM crew_strategies WHERE id = ?", (strategy_id,)).fetchone()
            conn.close()

            if not row:
                return {"status": "error", "error": f"Strategy {strategy_id} not found."}

            strategy_json = json.dumps(dict(row), default=str)

            critic = create_critic()
            commander = create_commander()

            critic_task = Task(
                description=(
                    f"Re-evaluate this existing strategy:\n{strategy_json}\n\n"
                    "Score on all 6 dimensions. Has anything changed that affects the thesis? "
                    "Check current portfolio exposure."
                ),
                expected_output="Updated critic scorecard with verdict.",
                agent=critic,
            )

            commander_task = Task(
                description=(
                    f"Review the Critic's re-evaluation of strategy {strategy_id}. "
                    "Decide: keep active, pause, or kill. Update strategy status accordingly."
                ),
                expected_output="Commander decision: keep/pause/kill with reasoning.",
                agent=commander,
                context=[critic_task],
            )

            crew = Crew(
                agents=[critic, commander],
                tasks=[critic_task, commander_task],
                process=Process.sequential,
                verbose=True,
            )

            log.info("Waiting for Ollama lock (arena scanner may be running)...")
            with OllamaLock("crew_review_existing"):
                log.info("Ollama lock acquired, strategy review starting...")
                result = crew.kickoff()
            outcome = "completed"

            return {
                "status": "completed",
                "result": str(result),
                "strategy_id": strategy_id,
                "duration": round(time.time() - start, 1),
            }

        except Exception as e:
            error = str(e)
            outcome = "error"
            return {"status": "error", "error": error, "duration": round(time.time() - start, 1)}

        finally:
            _log_run("review_existing", "manual", "critic,commander", strategy_id, 1, 0,
                     outcome, 0, 0.0, round(time.time() - start, 1), error)

    def run_sunday_review(self) -> dict:
        """
        Sunday special: review last week's performance, generate 2-3 new strategies.

        Generates:
        1. One conservative stock strategy
        2. One aggressive options/spreads strategy
        3. One macro metals hedge

        Runs BEFORE Strategy Lab optimizes.
        """
        start = time.time()
        results = []

        # Get weekly performance summary to prepend to all focus areas
        try:
            from crew.learning import get_weekly_summary
            weekly_ctx = get_weekly_summary()
        except Exception as e:
            log.warning(f"[sunday_review] Could not load weekly summary: {e}")
            weekly_ctx = ""

        weekly_prefix = f"{weekly_ctx}\n\nBased on the above weekly results, " if weekly_ctx else ""

        # Strategy 1: Conservative stock
        r1 = self.run_full_pipeline(
            focus_area=f"{weekly_prefix}conservative stock opportunities — value plays, dividend aristocrats, sector leaders with strong fundamentals",
            target_asset_class="stock",
            target_portfolio_id=1,
            trigger="sunday_review",
        )
        results.append({"type": "conservative_stock", **r1})

        # Strategy 2: Aggressive options/spreads
        r2 = self.run_full_pipeline(
            focus_area=f"{weekly_prefix}aggressive options plays — high IV situations, earnings catalysts, momentum breakouts suitable for spreads or directional options",
            target_asset_class="option",
            target_portfolio_id=1,
            trigger="sunday_review",
        )
        results.append({"type": "aggressive_options", **r2})

        # Strategy 3: Macro metals hedge
        r3 = self.run_full_pipeline(
            focus_area=f"{weekly_prefix}macro metals hedge — gold/silver allocation based on inflation expectations, dollar weakness, geopolitical risk, central bank policy",
            target_asset_class="metals",
            target_portfolio_id=1,
            trigger="sunday_review",
        )
        results.append({"type": "metals_hedge", **r3})

        total_duration = round(time.time() - start, 1)

        return {
            "status": "completed",
            "strategies_generated": len([r for r in results if r.get("status") == "completed"]),
            "results": results,
            "total_duration": total_duration,
        }


def run_crew(
    focus_area: str = "market opportunities",
    target_asset_class: str = "stock",
    target_portfolio_id: int = 1,
    trigger: str = "manual",
) -> dict:
    """Convenience function to run the full pipeline."""
    pipeline = CrewPipeline()
    return pipeline.run_full_pipeline(focus_area, target_asset_class, target_portfolio_id, trigger)
