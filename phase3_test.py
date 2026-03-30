import os

from agents.momentum import MomentumAgent
from agents.mean_reversion import MeanReversionAgent
from engine.agent_manager import AgentManager
from engine.agent_scoreboard import AgentScoreboard

market_data = {
    "AAPL": [180, 181, 182, 183, 187],
    "TSLA": [250, 249, 248, 247, 240],
    "MSFT": [100, 101, 102, 103, 106],
}

agents = [
    MomentumAgent(),
    MeanReversionAgent(),
]

scoreboard_path = os.path.join("data", "agent_scoreboard_phase38.json")
seed_stats = {
    "Momentum": {"signals": 10, "wins": 6, "losses": 4},
    "MeanReversion": {"signals": 2, "wins": 1, "losses": 0},
}

scoreboard = AgentScoreboard(stats=seed_stats, path=scoreboard_path)
scoreboard.stats = seed_stats
scoreboard.save()

manager = AgentManager(agents, scoreboard=scoreboard)
signals = manager.run(market_data, top_n=3, min_weighted_confidence=0.02)

print("SCOREBOARD WEIGHTS")
for agent in agents:
    stats = scoreboard.get_stats(agent.name)
    completed = stats["wins"] + stats["losses"]
    print(
        f"- {agent.name}: completed={completed} "
        f"weight={scoreboard.get_weight(agent.name)}"
    )

print("\nSURVIVING SIGNALS")
for signal in signals:
    print(
        f"- {signal['agent']} {signal['symbol']} {signal['action']} "
        f"confidence={signal['confidence']:.3f} "
        f"weighted_confidence={signal['weighted_confidence']:.3f} "
        f"reason={signal['reason']}"
    )

scoreboard.record_trade_outcome("Momentum", won=True)
scoreboard.record_trade_outcome("MeanReversion", won=False)

print("\nUPDATED SCOREBOARD WEIGHTS")
for agent in agents:
    stats = scoreboard.get_stats(agent.name)
    completed = stats["wins"] + stats["losses"]
    print(
        f"- {agent.name}: completed={completed} "
        f"weight={scoreboard.get_weight(agent.name)}"
    )

print(f"\nPERSISTED SCOREBOARD: {scoreboard.path}")
print(scoreboard.stats)
