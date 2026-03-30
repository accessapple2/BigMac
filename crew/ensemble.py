import json
import os
from typing import Any, Optional

from agents.mean_reversion import MeanReversionAgent
from agents.momentum import MomentumAgent
from engine.agent_manager import AgentManager
from engine.agent_scoreboard import AgentScoreboard
from engine.market_data import get_intraday_candles


DEFAULT_MIN_WEIGHTED_CONFIDENCE = float(
    os.environ.get("MR_ANDERSON_MIN_WEIGHTED_CONFIDENCE", "0.02")
)
DEFAULT_TOP_N = int(os.environ.get("MR_ANDERSON_TOP_N", "1"))
DEFAULT_EXPLORATION_PCT = float(
    os.environ.get("MR_ANDERSON_EXPLORATION_PCT", "0.20")
)
DEFAULT_FORCED_EXPLORE_EVERY = int(
    os.environ.get("MR_ANDERSON_FORCED_EXPLORE_EVERY", "5")
)
DEFAULT_MIN_OUTCOME_SAMPLE = int(
    os.environ.get("MR_ANDERSON_MIN_OUTCOME_SAMPLE", "3")
)

_SOURCE_FIELDS = (
    ("scout", "scout_brief"),
    ("architect", "architect_reasoning"),
    ("critic", "critic_notes"),
    ("commander", "commander_decision"),
)


class _CandidateSignalAgent:
    def __init__(self, signal: dict):
        self.name = f"strategy-{signal.get('id', 'unknown')}"
        self._signal = signal

    def scan(self, _market_data):
        return [dict(self._signal)]


class _PhaseSignalAgent:
    def __init__(self, phase_agent):
        self.name = phase_agent.name
        self._phase_agent = phase_agent

    def scan(self, market_data):
        signals = []
        for signal in self._phase_agent.scan(market_data):
            symbol = str(signal.get("symbol") or "").upper()
            if not symbol:
                continue
            confidence = max(0.0, _safe_float(signal.get("confidence")))
            action = str(signal.get("action") or "BUY").upper()
            direction = "short" if action == "SELL" else "long"
            signals.append(
                {
                    "id": f"phase-{self.name.lower()}-{symbol}",
                    "name": f"{self.name} {symbol}",
                    "symbol": symbol,
                    "target_tickers": [symbol],
                    "direction": direction,
                    "action": action,
                    "confidence": confidence,
                    "conviction_score": round(confidence * 100, 2),
                    "critic_score": None,
                    "weight": 1.0,
                    "thesis": signal.get("reason") or self.name,
                    "status": "phase_live",
                    "source_agent_metadata": [
                        {
                            "agent": self.name,
                            "field": "phase_signal",
                            "excerpt": str(signal.get("reason") or self.name)[:160],
                        }
                    ],
                }
            )
        return signals


def _as_dict(row: Any) -> dict:
    if isinstance(row, dict):
        return dict(row)
    return {k: row[k] for k in row.keys()}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_tickers(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(t).upper() for t in raw if str(t).strip()]
    text = str(raw).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(t).upper() for t in parsed if str(t).strip()]
    except Exception:
        pass
    return [t.strip().upper() for t in text.split(",") if t.strip()]


def _build_source_agent_metadata(candidate: dict) -> list[dict]:
    metadata = []
    for agent_name, field_name in _SOURCE_FIELDS:
        value = candidate.get(field_name)
        if not value:
            continue
        excerpt = " ".join(str(value).split())
        metadata.append(
            {
                "agent": agent_name,
                "field": field_name,
                "excerpt": excerpt[:160],
            }
        )
    return metadata


def _weighted_components(candidate: dict) -> tuple[float, float]:
    confidence = max(0.0, _safe_float(candidate.get("conviction_score"))) / 10.0
    critic_score = candidate.get("critic_score")
    weight = 1.0 if critic_score is None else max(0.0, _safe_float(critic_score)) / 10.0
    return confidence, weight


def _build_signal(candidate_row: Any) -> dict:
    candidate = _as_dict(candidate_row)
    tickers = _parse_tickers(candidate.get("target_tickers"))
    confidence, weight = _weighted_components(candidate)
    source_agent_metadata = _build_source_agent_metadata(candidate)

    return {
        **candidate,
        "symbol": tickers[0] if tickers else "",
        "target_tickers": tickers,
        "confidence": confidence,
        "weight": weight,
        "source_agent_metadata": source_agent_metadata,
    }


def _build_market_data(candidate_signals: list[dict]) -> dict[str, list[float]]:
    market_data: dict[str, list[float]] = {}
    symbols: list[str] = []
    for signal in candidate_signals:
        for symbol in signal.get("target_tickers") or []:
            if symbol and symbol not in symbols:
                symbols.append(symbol)

    for symbol in symbols:
        try:
            candles = get_intraday_candles(symbol, interval="1d", range_="1mo")
        except Exception:
            candles = []
        closes = [
            float(candle["close"])
            for candle in candles
            if isinstance(candle, dict) and candle.get("close") is not None
        ]
        if len(closes) >= 5:
            market_data[symbol] = closes[-5:]
    return market_data


def _dedupe_execution_signals(signals: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for signal in signals:
        symbol = str(signal.get("symbol") or "").upper()
        direction = str(signal.get("direction") or signal.get("action") or "long").lower()
        key = (symbol, direction)
        if not symbol or key in seen:
            continue
        seen.add(key)
        deduped.append(signal)
    return deduped


def _log_selection_cycle(result: dict) -> None:
    candidate_signals = list(result.get("candidate_signals", []))
    final_signals = list(result.get("final_signals", []))
    phase_count = sum(
        1 for signal in candidate_signals
        if signal.get("agent") in {"Momentum", "MeanReversion"}
    )
    legacy_count = sum(
        1 for signal in candidate_signals
        if str(signal.get("agent") or "").startswith("strategy-")
    )

    print(
        f"[mr-anderson:ensemble] candidates={len(candidate_signals)} "
        f"phase={phase_count} legacy={legacy_count} selected={len(final_signals)}"
    )
    for signal in final_signals:
        direction = str(signal.get("direction") or signal.get("action") or "long").upper()
        selection_type = str(signal.get("selection_type") or "exploit")
        print(
            f"[mr-anderson:ensemble] selected "
            f"agent={signal.get('agent', '?')} "
            f"symbol={signal.get('symbol', '?')} "
            f"direction={direction} "
            f"selection={selection_type} "
            f"confidence={_safe_float(signal.get('confidence')):.3f} "
            f"weighted_confidence={_safe_float(signal.get('weighted_confidence')):.3f}"
        )


def _selection_bucket(signal: dict) -> str:
    agent = str(signal.get("agent") or "")
    return _bucket_for_agent(agent)


def _bucket_for_agent(agent: str) -> str:
    if agent == "Momentum":
        return "Momentum"
    if agent == "MeanReversion":
        return "MeanReversion"
    if agent.startswith("strategy-"):
        return "LegacyCrew"
    return agent or "Unknown"


def _source_policy(scoreboard: AgentScoreboard, bucket: str) -> dict:
    stats = scoreboard.get_stats(bucket)
    wins = int(stats.get("wins", 0))
    losses = int(stats.get("losses", 0))
    executed = wins + losses
    win_rate = (wins / executed) if executed else None

    if executed < DEFAULT_MIN_OUTCOME_SAMPLE:
        status = "neutral"
        multiplier = 1.0
    elif win_rate is not None and win_rate < 0.40:
        status = "probation"
        multiplier = 0.5
    elif win_rate is not None and win_rate > 0.60:
        status = "favored"
        multiplier = 1.0
    else:
        status = "neutral"
        multiplier = 1.0

    return {
        "bucket": bucket,
        "executed": executed,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "status": status,
        "multiplier": multiplier,
    }


class _EnsembleScoreboardProxy:
    def __init__(self, scoreboard: AgentScoreboard):
        self._scoreboard = scoreboard
        self.stats = scoreboard.stats
        self.path = scoreboard.path

    def get_stats(self, agent_name):
        return self._scoreboard.get_stats(agent_name)

    def save(self):
        self._scoreboard.save()

    def get_weight(self, agent_name):
        base_weight = self._scoreboard.get_weight(agent_name)
        policy = _source_policy(self._scoreboard, _bucket_for_agent(str(agent_name or "")))
        return round(base_weight * policy["multiplier"], 4)


def _record_source_selection_counts(scoreboard: AgentScoreboard, final_signals: list[dict]) -> dict[str, int]:
    cycle_counts = {"LegacyCrew": 0, "Momentum": 0, "MeanReversion": 0}
    touched = set()
    for signal in final_signals:
        bucket = _selection_bucket(signal)
        cycle_counts[bucket] = cycle_counts.get(bucket, 0) + 1
        stats = scoreboard.stats.setdefault(bucket, {"signals": 0, "wins": 0, "losses": 0})
        stats["signals"] = int(stats.get("signals", 0)) + 1
        touched.add(bucket)

    if touched:
        scoreboard.save()

    return {
        bucket: int(scoreboard.get_stats(bucket).get("signals", 0))
        for bucket in ("LegacyCrew", "Momentum", "MeanReversion")
    }


def _meta_state(scoreboard: AgentScoreboard) -> dict:
    return scoreboard.stats.setdefault("_ensemble_meta", {"cycles": 0})


def _maybe_force_explore(
    scoreboard: AgentScoreboard,
    result: dict,
    *,
    forced_explore_every: int,
) -> Optional[dict]:
    if forced_explore_every <= 0:
        return None

    meta = _meta_state(scoreboard)
    meta["cycles"] = int(meta.get("cycles", 0)) + 1
    cycle_number = meta["cycles"]
    if cycle_number % forced_explore_every != 0:
        scoreboard.save()
        return None

    selected_keys = {
        (
            str(signal.get("symbol") or "").upper(),
            str(signal.get("direction") or signal.get("action") or "long").lower(),
        )
        for signal in result.get("final_signals", [])
    }

    eligible = []
    for signal in result.get("surviving_signals", []):
        bucket = _selection_bucket(signal)
        if bucket not in {"LegacyCrew", "Momentum", "MeanReversion"}:
            continue
        if scoreboard.get_stats(bucket).get("signals", 0) != 0:
            continue
        if signal.get("agent") in {selected.get("agent") for selected in result.get("final_signals", [])}:
            continue
        key = (
            str(signal.get("symbol") or "").upper(),
            str(signal.get("direction") or signal.get("action") or "long").lower(),
        )
        if not key[0] or key in selected_keys:
            continue
        eligible.append(signal)

    if not eligible:
        scoreboard.save()
        return None

    injected = dict(
        max(
            eligible,
            key=lambda signal: _safe_float(signal.get("weighted_confidence")),
        )
    )
    injected["selection_type"] = "forced_explore"
    result["final_signals"] = list(result.get("final_signals", [])) + [injected]
    scoreboard.save()
    return injected


def select_collective_signals(
    candidate_rows: list[Any],
    *,
    min_weighted_confidence: float = DEFAULT_MIN_WEIGHTED_CONFIDENCE,
    top_n: int = DEFAULT_TOP_N,
    exploration_pct: float = DEFAULT_EXPLORATION_PCT,
    forced_explore_every: int = DEFAULT_FORCED_EXPLORE_EVERY,
    random_seed: int = 7,
) -> dict:
    candidate_signals = [_build_signal(row) for row in candidate_rows]
    market_data = _build_market_data(candidate_signals)
    agents = [_CandidateSignalAgent(signal) for signal in candidate_signals]
    if market_data:
        agents.extend(
            [
                _PhaseSignalAgent(MomentumAgent()),
                _PhaseSignalAgent(MeanReversionAgent()),
            ]
        )

    scoreboard = AgentScoreboard()
    policy_scoreboard = _EnsembleScoreboardProxy(scoreboard)
    manager = AgentManager(agents, scoreboard=policy_scoreboard)
    effective_top_n = max(1, min(2, top_n))
    result = manager.run(
        market_data=market_data,
        top_n=effective_top_n,
        min_weighted_confidence=min_weighted_confidence,
        exploration_pct=exploration_pct,
        random_seed=random_seed,
        return_diagnostics=True,
    )
    result["min_weighted_confidence"] = min_weighted_confidence
    result["top_n"] = effective_top_n
    result["exploration_pct"] = exploration_pct
    result["market_data_symbols"] = sorted(market_data.keys())
    result["candidate_signals"] = list(result.get("candidate_signals", []))
    result["surviving_signals"] = list(result.get("surviving_signals", []))
    result["exploit_signals"] = list(result.get("exploit_signals", []))
    result["explore_signals"] = list(result.get("explore_signals", []))
    result["final_signals"] = _dedupe_execution_signals(list(result.get("final_signals", [])))
    forced_signal = _maybe_force_explore(
        scoreboard,
        result,
        forced_explore_every=forced_explore_every,
    )
    if forced_signal:
        result["forced_explore_signal"] = forced_signal
    result["selection_source_totals"] = _record_source_selection_counts(scoreboard, result["final_signals"])
    result["source_policy"] = {
        bucket: _source_policy(scoreboard, bucket)
        for bucket in ("LegacyCrew", "Momentum", "MeanReversion")
    }
    _log_selection_cycle(result)
    return result
