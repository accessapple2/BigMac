"""Shared instrument-class filter — money-market / mutual-fund share classes.

HM-CAPITOL-FUND-FILTER (2026-06-11). Single source of truth for "is this ticker a
non-equity fund vehicle?", consumed by BOTH:
  - engine/crew_scanner.py::capitol_rules  (congress copycat — drop before scoring)
  - engine/archer/intel_sources.py::get_congress  (Archer intel — drop before the
    LLM briefing, which otherwise hallucinates an asset class, e.g. AFAXX→"crypto")

Kept here (not inline) so the heuristic + denylist can never drift between the two
call sites. Lightweight: no heavy imports, safe to import from anywhere.

Money-market / mutual-fund symbols are cash-sweep / fund share classes, NOT tradable
equity signals. Congress members parking cash in them (e.g. Tom Suozzi → AFAXX, the
American Funds U.S. Govt Money Market fund) is noise, not smart-money flow.
"""
from __future__ import annotations

# Explicit money-market / cash-sweep denylist — backstops the 5-letter-X pattern
# for any that don't fit (or that we want named for clarity).
_MONEY_MARKET_TICKERS = frozenset({
    "AFAXX", "VMFXX", "VMRXX", "SPAXX", "FDRXX", "SWVXX", "FZFXX", "SPRXX",
    "VUSXX", "TTTXX", "SNVXX", "SNSXX", "FNSXX", "MVRXX", "FGXXX", "PINXX",
})


def is_non_equity_ticker(ticker: str) -> bool:
    """True if *ticker* is a mutual-fund / money-market share class (untradeable as
    an equity). Heuristic (cheapest available — no per-symbol API call): NASDAQ
    5-letter symbols ending in X are fund share classes (AFAXX money market,
    VFIAX index fund, AGTHX growth fund), plus an explicit money-market denylist.
    Legit equity tickers are 1-4 letters, or 5-letter not ending in X (GOOGL), or
    a 1-letter X (US Steel) — none of which match, so real equities are untouched.
    """
    t = (ticker or "").strip().upper()
    if not t:
        return False
    if t in _MONEY_MARKET_TICKERS:
        return True
    if len(t) == 5 and t.endswith("X"):
        return True
    return False
