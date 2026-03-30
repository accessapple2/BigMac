class AgentScoreboard:
    def __init__(self):
        self.stats = {}

    def ensure_agent(self, agent_name):
        if agent_name not in self.stats:
            self.stats[agent_name] = {
                "wins": 0,
                "losses": 0,
                "signals": 0
            }

    def record_signal(self, agent_name):
        self.ensure_agent(agent_name)
        self.stats[agent_name]["signals"] += 1

    def record_result(self, agent_name, won):
        self.ensure_agent(agent_name)
        if won:
            self.stats[agent_name]["wins"] += 1
        else:
            self.stats[agent_name]["losses"] += 1

    def get_weight(self, agent_name):
        self.ensure_agent(agent_name)
        wins = self.stats[agent_name]["wins"]
        losses = self.stats[agent_name]["losses"]
        score = wins - losses
        return max(0.5, 1 + score * 0.1)

    def summary(self):
        return self.stats