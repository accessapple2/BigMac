# 🔧 SCOTTY — HM-CB: Polygon Stocks Primary for Candles
### Opus 4.7 · Durable upstream fix · Then E4 frontend re-attempt

> **Captain's orders, Mr. Scott:** Polygon Stocks Starter is ALREADY PAID. The CLAUDE.md "approved-in-principle, not activated" line is stale. Polygon is THE durable fix for cold-candles — sub-500ms for any symbol. Wire it as PRIMARY; Alpaca + yfinance become fallbacks. After verification, re-apply HM-BJ.E4 frontend swap with proper browser test per memory #27.

## Pre-flight
- Polygon creds in `.env`
- Current `get_intraday_candles` structure post-HM-CA
- Working tree + last commits

## CB.0 — Discovery
- Polygon API key validation (no key print)
- Direct cold-symbol probe (SPY/IBM/NVDA/MMM/XOM/ORCL)
- Polygon response shape

HALT only if: key missing OR cold latency >2s.

## CB.1 — Wire Polygon as primary
Edit `engine/market_data.py:get_intraday_candles`:
- Polygon first → Alpaca → yfinance fallback
- Typed catches + `[yellow]` log per CLAUDE.md doctrine
- Anchor `# === HM-CB ===`
- Same output shape

Commit: `feat(market_data): HM-CB — Polygon Stocks Starter as primary candles source (Alpaca + yfinance fallbacks)`.

## CB.2 — Push + restart + verify
- Cold-symbol full-stack latency
- Scorecard latency

Success: every cold symbol < 1s. HALT if not.

## CB.3 — Re-apply HM-BJ.E4 frontend (LOCAL, NO COMMIT)
## CB.4 — Captain browser test (memory #27)
## CB.5 — Commit frontend only if WORKING
## CB.6 — Closure
