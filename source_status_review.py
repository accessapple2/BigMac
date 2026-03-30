import importlib.util
import sqlite3
from pathlib import Path
from typing import Optional


def _load_ensemble_module():
    module_path = Path(__file__).resolve().parent / "crew" / "ensemble.py"
    spec = importlib.util.spec_from_file_location("ensemble_review", module_path)
    ensemble = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ensemble)
    return ensemble


def _legacy_outcome_stats(db_path: Path) -> dict:
    if not db_path.exists():
        return {"executed": 0, "wins": 0, "losses": 0, "win_rate": None}

    conn = sqlite3.connect(str(db_path))
    try:
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
    except sqlite3.OperationalError:
        conn.close()
        return {"executed": 0, "wins": 0, "losses": 0, "win_rate": None}
    finally:
        conn.close()

    executed = int(row[0] or 0)
    wins = int(row[1] or 0)
    losses = int(row[2] or 0)
    win_rate = (wins / executed) if executed else None
    return {"executed": executed, "wins": wins, "losses": losses, "win_rate": win_rate}


def _source_benchmark_cycle_return(db_path: Path, source_bucket: str) -> Optional[float]:
    source_player_map = {
        "Ray": "dalio-metals",
    }
    player_id = source_player_map.get(source_bucket)
    if not player_id:
        return None

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cycle = conn.execute(
            """
            SELECT benchmark_cycle_start, benchmark_start_equity
            FROM player_benchmark_cycles
            WHERE player_id=? AND COALESCE(is_active, 1)=1
            ORDER BY benchmark_cycle_start DESC, id DESC
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()
        if not cycle:
            return None
        current = conn.execute(
            "SELECT cash FROM ai_players WHERE id=?",
            (player_id,),
        ).fetchone()
        pos = conn.execute(
            "SELECT COALESCE(SUM(qty * avg_price), 0) AS positions_value FROM positions WHERE player_id=?",
            (player_id,),
        ).fetchone()
    finally:
        conn.close()

    start_equity = float(cycle["benchmark_start_equity"] or 0.0)
    current_equity = float(current["cash"] or 0.0) + float(pos["positions_value"] or 0.0)
    if start_equity <= 0:
        return None
    return round(((current_equity - start_equity) / start_equity) * 100.0, 2)


def _current_status(ensemble, scoreboard, bucket: str, db_path: Path) -> dict:
    if bucket == "LegacyCrew":
        stats = _legacy_outcome_stats(db_path)
        executed = stats["executed"]
        win_rate = stats["win_rate"]
        if executed == 0:
            status = "insufficient-data"
        elif executed < ensemble.DEFAULT_MIN_OUTCOME_SAMPLE:
            status = "neutral"
        elif win_rate is not None and win_rate < 0.40:
            status = "probation"
        elif win_rate is not None and win_rate > 0.60:
            status = "favored"
        else:
            status = "neutral"
        return {
            "status": status,
            **stats,
        }

    if bucket in ("Momentum", "MeanReversion"):
        policy = ensemble._source_policy(scoreboard, bucket)
        return {
            "status": policy["status"],
            "executed": policy["executed"],
            "wins": policy["wins"],
            "losses": policy["losses"],
            "win_rate": policy["win_rate"],
        }

    return {"status": "neutral", "executed": 0, "wins": 0, "losses": 0, "win_rate": None}


def _recommended_status(min_sample: int, executed: int, win_rate: Optional[float], benchmark_cycle_return: Optional[float]) -> tuple[str, str]:
    if executed < min_sample:
        if benchmark_cycle_return is not None and benchmark_cycle_return <= -5.0:
            return "probation", "Low trade sample, but benchmark-cycle return is below -5%"
        return "neutral", f"Below minimum sample threshold ({executed}/{min_sample})"

    if benchmark_cycle_return is not None:
        if benchmark_cycle_return <= -5.0:
            return "probation", "Benchmark-cycle return is below -5%"
        if benchmark_cycle_return >= 5.0 and (win_rate or 0.0) >= 0.55:
            return "favored", "Benchmark-cycle return is above +5% with solid win rate"

    if win_rate is None:
        return "neutral", "No comparable win-rate data available"
    if win_rate < 0.40:
        return "probation", "Win rate is below 40%"
    if win_rate > 0.60:
        return "favored", "Win rate is above 60%"
    return "neutral", "Results are mixed and remain within the neutral band"


def main():
    repo_root = Path(__file__).resolve().parent
    db_path = repo_root / "data" / "trader.db"
    ensemble = _load_ensemble_module()
    scoreboard = ensemble.AgentScoreboard(path=str(repo_root / "data" / "agent_scoreboard.json"))
    min_sample = ensemble.DEFAULT_MIN_OUTCOME_SAMPLE

    buckets = ("LegacyCrew", "Momentum", "MeanReversion")

    print("PHASE_6_3_SOURCE_STATUS_REVIEW")
    print(f"minimum_sample={min_sample}")
    print("")
    print("CONTRIBUTOR_MANAGED_SOURCES")

    fully_attributable = []
    insufficient = []
    for bucket in buckets:
        current = _current_status(ensemble, scoreboard, bucket, db_path)
        cycle_return = _source_benchmark_cycle_return(db_path, bucket)
        recommended, reason = _recommended_status(
            min_sample=min_sample,
            executed=current["executed"],
            win_rate=current["win_rate"],
            benchmark_cycle_return=cycle_return,
        )
        win_rate_pct = f"{(current['win_rate'] * 100.0):.1f}%" if current["win_rate"] is not None else "n/a"
        cycle_return_pct = f"{cycle_return:.2f}%" if cycle_return is not None else "n/a"
        print(
            f"{bucket}: "
            f"current_status={current['status']} "
            f"recommended_status={recommended} "
            f"executed={current['executed']} "
            f"wins={current['wins']} "
            f"losses={current['losses']} "
            f"win_rate={win_rate_pct} "
            f"benchmark_cycle_return={cycle_return_pct} "
            f"reason={reason}"
        )
        if current["executed"] > 0:
            fully_attributable.append(bucket)
        else:
            insufficient.append(bucket)

    print("")
    print("FULLY_ATTRIBUTABLE_SOURCES")
    if fully_attributable:
        for bucket in fully_attributable:
            print(f"{bucket}: classification=fully-attributable")
    else:
        print("none")
    print("")
    print("BENCHMARK_ONLY_AGENTS")
    ray_cycle_return = _source_benchmark_cycle_return(db_path, "Ray")
    ray_cycle_return_pct = f"{ray_cycle_return:.2f}%" if ray_cycle_return is not None else "n/a"
    print(
        "Ray: "
        "classification=benchmark-only "
        "current_status=separate "
        "recommended_status=separate "
        f"benchmark_cycle_return={ray_cycle_return_pct} "
        "reason=Not part of Anderson contributor-source governance"
    )
    print("")
    print("UNCLASSIFIED_OR_INSUFFICIENT_DATA")
    if insufficient:
        for bucket in insufficient:
            print(f"{bucket}: classification=insufficient-data reason=No attributable executed source outcomes yet")
    else:
        print("none")


if __name__ == "__main__":
    main()
