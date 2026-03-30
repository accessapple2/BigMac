import json
import os


class AgentScoreboard:
    MIN_COMPLETED_OUTCOMES = 3
    NEUTRAL_WEIGHT = 1.0

    def __init__(self, stats=None, path=None):
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.path = path or os.path.join(repo_root, "data", "agent_scoreboard.json")
        self.stats = stats or {}
        self.load()

    def load(self):
        if not os.path.exists(self.path):
            return
        with open(self.path) as f:
            self.stats = json.load(f)

    def save(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self.stats, f, indent=2, sort_keys=True)

    def get_stats(self, agent_name):
        return self.stats.get(agent_name, {"signals": 0, "wins": 0, "losses": 0})

    def record_trade_outcome(self, agent_name, won):
        stats = self.stats.setdefault(agent_name, {"signals": 0, "wins": 0, "losses": 0})
        if won:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        self.save()

    def get_weight(self, agent_name):
        stats = self.get_stats(agent_name)
        wins = stats.get("wins", 0)
        losses = stats.get("losses", 0)
        total = wins + losses

        if total < self.MIN_COMPLETED_OUTCOMES:
            return self.NEUTRAL_WEIGHT

        return round(wins / total, 2)
