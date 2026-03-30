import math
import random


class AgentManager:
    def __init__(self, agents, scoreboard=None):
        self.agents = agents
        self.scoreboard = scoreboard

    def run(
        self,
        market_data,
        top_n=None,
        min_weighted_confidence=None,
        exploration_pct=0.0,
        random_seed=None,
        return_diagnostics=False,
    ):
        results = []
        rng = random.Random(random_seed)

        for agent in self.agents:
            signals = agent.scan(market_data)

            for s in signals:
                s["agent"] = agent.name

                # apply weight if scoreboard exists
                if self.scoreboard:
                    weight = self.scoreboard.get_weight(agent.name)
                    s["weighted_confidence"] = s["confidence"] * weight
                else:
                    weight = s.get("weight", 1.0)
                    s["weighted_confidence"] = s["confidence"] * weight

            results.extend(signals)

        results.sort(key=lambda x: x["weighted_confidence"], reverse=True)
        candidate_signals = list(results)
        if min_weighted_confidence is not None:
            results = [
                signal for signal in results
                if signal["weighted_confidence"] >= min_weighted_confidence
            ]
        surviving_signals = list(results)

        if top_n is None:
            final_signals = surviving_signals
            exploit_signals = list(final_signals)
            explore_signals = []
        else:
            capped_top_n = max(0, top_n)
            explore_n = 0
            if capped_top_n > 1 and exploration_pct > 0 and surviving_signals:
                explore_n = min(
                    len(surviving_signals),
                    max(1, math.ceil(capped_top_n * exploration_pct)),
                )
            exploit_n = max(0, capped_top_n - explore_n)

            exploit_signals = list(surviving_signals[:exploit_n])
            remaining = list(surviving_signals[exploit_n:])
            if explore_n > 0 and remaining:
                explore_n = min(explore_n, len(remaining))
                explore_signals = rng.sample(remaining, explore_n)
                explore_signals.sort(
                    key=lambda signal: signal["weighted_confidence"],
                    reverse=True,
                )
            else:
                explore_signals = []

            final_signals = exploit_signals + explore_signals
            final_signals.sort(
                key=lambda signal: (
                    0 if signal in exploit_signals else 1,
                    -signal["weighted_confidence"],
                ),
            )

        for signal in exploit_signals:
            signal["selection_type"] = "exploit"
        for signal in explore_signals:
            signal["selection_type"] = "explore"
        for signal in final_signals:
            signal.setdefault("selection_type", "exploit")

        if return_diagnostics:
            return {
                "candidate_signals": candidate_signals,
                "surviving_signals": surviving_signals,
                "exploit_signals": exploit_signals,
                "explore_signals": explore_signals,
                "final_signals": final_signals,
            }
        return final_signals
