import importlib.util
import sqlite3
import tempfile
from pathlib import Path


def _load_ensemble_module():
    module_path = Path(__file__).resolve().parent / "crew" / "ensemble.py"
    spec = importlib.util.spec_from_file_location("ensemble_under_test", module_path)
    ensemble = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ensemble)
    return ensemble


def _legacy_outcome_stats(db_path: Path) -> dict:
    if not db_path.exists():
        return {"executed": 0, "wins": 0, "losses": 0, "win_rate": None}

    conn = sqlite3.connect(str(db_path))
    try:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(crew_trade_results)").fetchall()
        }
        if "source_bucket" in columns:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS executed,
                    SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN outcome IN ('loss', 'stopped') THEN 1 ELSE 0 END) AS losses
                FROM crew_trade_results
                WHERE source_bucket='LegacyCrew'
                """
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS executed,
                    SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN outcome IN ('loss', 'stopped') THEN 1 ELSE 0 END) AS losses
                FROM crew_trade_results
                """
            ).fetchone()
    except sqlite3.OperationalError:
        conn.close()
        return {"executed": 0, "wins": 0, "losses": 0, "win_rate": None}
    finally:
        conn.close()

    executed = int(row[0] or 0)
    wins = int(row[1] or 0)
    losses = int(row[2] or 0)
    win_rate = (wins / executed) if executed else None
    return {
        "executed": executed,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
    }


def main():
    ensemble = _load_ensemble_module()
    repo_root = Path(__file__).resolve().parent
    legacy_db_path = repo_root / "data" / "trader.db"

    sample_candles = {
        "AAPL": [180, 181, 182, 183, 187],
        "TSLA": [250, 249, 248, 247, 240],
        "MSFT": [100, 101, 102, 103, 106],
    }

    def fake_get_intraday_candles(symbol, interval="1d", range_="1mo"):
        prices = sample_candles.get(symbol, [])
        return [{"close": price} for price in prices]

    ensemble.get_intraday_candles = fake_get_intraday_candles

    base_scoreboard = ensemble.AgentScoreboard
    actual_scoreboard_path = repo_root / "data" / "agent_scoreboard.json"
    actual_scoreboard = base_scoreboard(path=str(actual_scoreboard_path))

    scoreboard_path = Path(tempfile.gettempdir()) / "ensemble_selection_scoreboard_report.json"
    if scoreboard_path.exists():
        scoreboard_path.unlink()

    class TestScoreboard(base_scoreboard):
        def __init__(self, stats=None, path=None):
            super().__init__(stats=stats, path=str(scoreboard_path))

    ensemble.AgentScoreboard = TestScoreboard

    candidate_rows = [
        {
            "id": 101,
            "name": "Crew Apple Breakout",
            "target_tickers": '["AAPL"]',
            "conviction_score": 6.0,
            "critic_score": 8.0,
            "critic_notes": "Constructive follow-through",
            "direction": "long",
            "thesis": "Crew likes AAPL continuation",
            "status": "approved",
            "scout_brief": "AAPL relative strength",
            "architect_reasoning": "Trend intact",
            "commander_decision": "Deploy if top-ranked",
        },
        {
            "id": 102,
            "name": "Crew Tesla Reversal",
            "target_tickers": '["TSLA"]',
            "conviction_score": 3.0,
            "critic_score": 5.0,
            "critic_notes": "Needs tighter sizing",
            "direction": "long",
            "thesis": "Crew sees bounce setup",
            "status": "approved",
            "scout_brief": "TSLA oversold",
            "architect_reasoning": "Mean reversion possible",
            "commander_decision": "Keep on watch",
        },
        {
            "id": 103,
            "name": "Crew Microsoft Drift",
            "target_tickers": '["MSFT"]',
            "conviction_score": 2.0,
            "critic_score": 4.0,
            "critic_notes": "Low edge",
            "direction": "long",
            "thesis": "Slow continuation",
            "status": "draft",
            "scout_brief": "MSFT steady bid",
            "architect_reasoning": "Trend mild",
            "commander_decision": "Only if no better ideas",
        },
    ]

    buckets = ("LegacyCrew", "Momentum", "MeanReversion")
    totals = {bucket: 0 for bucket in buckets}
    weighted_sums = {bucket: 0.0 for bucket in buckets}

    for cycle in range(20):
        result = ensemble.select_collective_signals(
            candidate_rows,
            top_n=2,
            random_seed=7,
        )
        for signal in result["final_signals"]:
            bucket = ensemble._selection_bucket(signal)
            if bucket not in totals:
                continue
            totals[bucket] += 1
            weighted_sums[bucket] += float(signal.get("weighted_confidence") or 0.0)

    total_selected = sum(totals.values())

    print("ENSEMBLE CONTRIBUTOR REPORT")
    print("cycles=20")
    print(f"total_selected_signals={total_selected}")
    print(f"actual_outcome_scoreboard={actual_scoreboard_path}")
    print("")
    for bucket in buckets:
        selected = totals[bucket]
        pct = (selected / total_selected * 100.0) if total_selected else 0.0
        avg_weighted = (weighted_sums[bucket] / selected) if selected else 0.0
        if bucket == "LegacyCrew":
            legacy_stats = _legacy_outcome_stats(legacy_db_path)
            executed = legacy_stats["executed"]
            wins = legacy_stats["wins"]
            losses = legacy_stats["losses"]
            raw_win_rate = legacy_stats["win_rate"]
            if executed < ensemble.DEFAULT_MIN_OUTCOME_SAMPLE:
                status = "neutral"
            elif raw_win_rate is not None and raw_win_rate < 0.40:
                status = "probation"
            elif raw_win_rate is not None and raw_win_rate > 0.60:
                status = "favored"
            else:
                status = "neutral"
        else:
            policy = ensemble._source_policy(actual_scoreboard, bucket)
            wins = policy["wins"]
            losses = policy["losses"]
            executed = policy["executed"]
            raw_win_rate = policy["win_rate"]
            status = policy["status"]
        win_rate = (raw_win_rate * 100.0) if raw_win_rate is not None else 0.0
        print(
            f"{bucket}: "
            f"status={status} "
            f"selected={selected} "
            f"pct={pct:.1f}% "
            f"executed={executed} "
            f"wins={wins} "
            f"losses={losses} "
            f"win_rate={win_rate:.1f}% "
            f"avg_weighted_confidence={avg_weighted:.3f}"
        )

    print("")
    print(f"simulation_scoreboard_path={scoreboard_path}")


if __name__ == "__main__":
    main()
