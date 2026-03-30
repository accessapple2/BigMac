import importlib.util
from pathlib import Path


_ENSEMBLE_PATH = Path(__file__).with_name("crew") / "ensemble.py"
_SPEC = importlib.util.spec_from_file_location("crew_ensemble_test_module", _ENSEMBLE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(_MODULE)
select_collective_signals = _MODULE.select_collective_signals


def _print_signal(prefix: str, signal: dict):
    contributors = ",".join(meta["agent"] for meta in signal["source_agent_metadata"]) or "crew"
    print(
        f"{prefix} strategy={signal['id']} symbol={signal['symbol']} "
        f"weighted_confidence={signal['weighted_confidence']:.3f} "
        f"selection={signal.get('selection_type', '-') } "
        f"conviction={float(signal.get('conviction_score') or 0):.1f}/10 "
        f"contributors={contributors}"
    )


sample_candidates = [
    {
        "id": 101,
        "name": "NVDA Momentum Breakout",
        "target_tickers": '["NVDA"]',
        "conviction_score": 8.8,
        "critic_score": 8.5,
        "critic_notes": "Momentum, catalyst, and portfolio fit all aligned.",
        "direction": "long",
        "thesis": "Breakout with strong volume and catalyst support.",
        "status": "approved",
        "scout_brief": "Scout flagged NVDA from live discoveries and AI buy signals.",
        "architect_reasoning": "Architect structured a clean breakout entry.",
        "commander_decision": "GO",
    },
    {
        "id": 102,
        "name": "AAPL Reversal",
        "target_tickers": '["AAPL"]',
        "conviction_score": 7.6,
        "critic_score": 7.2,
        "critic_notes": "Quality setup but less urgent than NVDA.",
        "direction": "long",
        "thesis": "Mean-reversion bounce off support.",
        "status": "approved",
        "scout_brief": "Scout saw support hold and improving tape.",
        "architect_reasoning": "Architect defined a tight stop and target.",
        "commander_decision": "GO",
    },
    {
        "id": 103,
        "name": "TSLA Weak Short",
        "target_tickers": '["TSLA"]',
        "conviction_score": 4.0,
        "critic_score": 4.5,
        "critic_notes": "Weak support for immediate execution.",
        "direction": "short",
        "thesis": "Momentum deteriorating, but follow-through uncertain.",
        "status": "draft",
        "scout_brief": "Scout saw relative weakness.",
        "architect_reasoning": "Architect outlined a short trigger.",
        "commander_decision": "NO-GO",
    },
]


selection = select_collective_signals(
    sample_candidates,
    min_weighted_confidence=0.02,
    top_n=2,
    exploration_pct=0.20,
    random_seed=7,
)

print(f"TOTAL CANDIDATE SIGNALS RECEIVED BY ANDERSON: {len(selection['candidate_signals'])}")
for signal in selection["candidate_signals"]:
    _print_signal("CANDIDATE", signal)

print(f"\nSURVIVING SIGNALS AFTER FILTERING: {len(selection['surviving_signals'])}")
for signal in selection["surviving_signals"]:
    _print_signal("SURVIVED", signal)

print(f"\nEXPLOIT SIGNALS: {len(selection['exploit_signals'])}")
for signal in selection["exploit_signals"]:
    _print_signal("EXPLOIT", signal)

print(f"\nEXPLORE SIGNALS: {len(selection['explore_signals'])}")
for signal in selection["explore_signals"]:
    _print_signal("EXPLORE", signal)

print(f"\nFINAL SIGNALS SELECTED FOR EXECUTION: {len(selection['final_signals'])}")
for signal in selection["final_signals"]:
    _print_signal("SELECTED", signal)
