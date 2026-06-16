# two-book-bridge.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## Architecture: Two-Book Bridge Policy (Option β, established 2026-05-05)

**OllieTrades operates two separate books, by design.** Source: HM-I
investigation (`docs/HM-I_BRIDGE_SCOPE_INVESTIGATION_2026-05-05.md`); Admiral
decision 2026-05-05.

### The two books
1. **Internal AI fleet book** — `positions` table for all `player_id != 'webull'`.
   Research / calibration. The legacy fleet (ollama-plutus, qwen3-8b-flash,
   deepseek-7b-grok4, ollama-qwen3, energy-arnold, capitol-trades,
   gemini-2.5-flash, etc.) writes here. **Never forwards to Alpaca.**
2. **Alpaca paper book** — Alpaca's live broker state, mirrored locally as the
   `webull` player's positions. Real-on-broker activity for routed players +
   spread strategies.

### What routes to Alpaca
- The **routed players** in `engine/paper_trader.py::_EXECUTION_PORTFOLIO_BY_PLAYER`:
  - `super-agent` (Mr. Anderson) → Alpaca Paper (portfolio id=1) — **HALTED
    `halt_mode='full'` since 2026-05-11** (`is_paused=1` reconcile; last actually
    traded **2026-03-28**, 16 lifetime trades, 0 open positions). The routing
    entry persists but is INERT while halted — this row is the routing map, NOT
    a claim that Anderson is live. No recurring cost (api_costs empty; crewai
    pinned/not loaded). Reviving = an Admiral decision. (Doc truth-up 2026-05-31,
    HM-SUPER-AGENT-VERIFY — was silently listed as a live router for 10+ weeks.)
  - `ollie-auto` → Alpaca Paper (portfolio id=1)
  - `neo-matrix` → Neo Matrix (portfolio id=7) — flipped to `halt_mode='active'`
    2026-05-13 (HM-AN2.3, "the show must go on Maestro!")
  - `dalio-metals` → Enterprise Computer (portfolio id=5, physical-metals
    tracker, `route_mode=tracking`, log-only)
- The **spread strategies** (post-gate-flip 2026-05-04, gated on `player_id in OPTIONS_PLAYERS` at `engine/alpaca_options.py:711` inside `execute_options_signal` (line 688)):
  - `bull_call_spread_v1`, `bear_put_spread_v1`, `executor` — route via
    `engine/alpaca_options.py::execute_options_signal`, a third forward path
    that bypasses the player-keyed routing table.

### What stays internal
Every other player. The 9+ active legacy-fleet agents emit signals and trades
into the `positions` table only. Their entries never reach Alpaca paper.

### How forwarding gates work
`engine/paper_trader.py::_forward_to_alpaca` (line 244) is gated on
`route["route_mode"] == "trading"` at all three call sites:
- BUY (line 1546), full-SELL (line 1850), partial-SELL (line 2045 — gated as
  of 2026-05-05 commit `d06c33c` per HM-I Option ε).

Players whose mapped portfolio resolves to `route_mode=paper` (default) or
`route_mode=tracking` (Enterprise Computer) never forward to the broker.

### Why two books, not one
- Spread strategies and routed players need real broker state for honest
  execution paths.
- Legacy fleet is research / calibration — separating their book from the
  broker preserves test isolation.
- Shorts and futures (GC=F, SI=F) live in the internal book naturally;
  Alpaca paper can't accept futures.
- Legacy fleet halt/retirement decisions don't pollute the broker state.

### Naming discipline
- "Arena Paper" = the default unmapped routing destination (no DB row;
  `route_mode=paper`). Most legacy-fleet agents land here.
- "Alpaca Paper" = `portfolios.id=1`, the actual broker connection.
- **Different things despite similar names.** Future dashboard work will make
  this visually distinct (HM-I-β followup).
