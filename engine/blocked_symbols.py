#!/usr/bin/env python3
"""
Blocked Symbols Per Agent — XO Coaching Report findings.
Symbols that consistently bleed for specific agents.
"""

BLOCKED_SYMBOLS: dict[str, list[str]] = {
    # dayblade-0dte: broad index ETFs are the biggest drag (SPY -$649, QQQ -$539)
    "dayblade-0dte": ["SPY", "QQQ", "IWM", "DIA"],

    # ollama-local: repeat offenders (MU -$2,961, AVGO -$2,228, AMD -$2,055)
    "ollama-local":  ["MU", "AVGO", "AMD"],

    # dayblade-sulu: META cost $525 across 3 trades at 0% WR
    "dayblade-sulu": ["META"],
}

GLOBAL_BLOCKED: list[str] = []


def is_symbol_blocked(agent_id: str, symbol: str) -> bool:
    """Return True if agent should not trade this symbol."""
    sym = symbol.upper()
    if sym in GLOBAL_BLOCKED:
        return True
    return sym in BLOCKED_SYMBOLS.get(agent_id, [])


def get_blocked_symbols(agent_id: str) -> list[str]:
    """Return combined global + agent-specific blocked list."""
    return list(set(GLOBAL_BLOCKED + BLOCKED_SYMBOLS.get(agent_id, [])))


if __name__ == "__main__":
    print("🚫 BLOCKED SYMBOLS BY AGENT")
    print("=" * 40)
    for agent, syms in BLOCKED_SYMBOLS.items():
        print(f"  {agent}: {', '.join(syms)}")
    print(f"  GLOBAL: {', '.join(GLOBAL_BLOCKED) or 'none'}")
