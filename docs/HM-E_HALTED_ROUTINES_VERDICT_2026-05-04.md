# HM-E — Halted-Player Daily Routines Investigation
*2026-05-04 evening, Scotty investigation, no fixes applied*

## Question

Are halted players still running daily routines (ai_journal, market commentary, signal emission, etc.) and consuming Ollie Box GPU cycles for outputs that aren't used?

## Inventory

### Halted players (4)

| player_id | halt_mode | halted_at | reason |
|---|---|---|---|
| `dayblade-sulu` | exit_only | 2026-03-31 | S6.3 bench: R:R 0.10, dormant |
| `grok-3` | exit_only | 2026-04-25 | S6 routing zombie, retired |
| `ollama-llama` | exit_only | 2026-04-25 | S6 routing zombie, retired |
| `gemini-2.5-pro` | exit_only | 2026-04-30 | qwen3:14b too heavy, retired |

### Post-halt write activity per table

| Table | sulu | ollama-llama | gemini-2.5-pro | grok-3 |
|---|---|---|---|---|
| `signals` (last write date) | 196 (2026-04-07) | 947 (**2026-05-01**) | 0 (2026-03-17) | 0 (2026-03-17) |
| `watchlist_signals` | 0 (2026-03-30) | **13** (2026-05-01) | 0 (2026-03-17) | 0 (2026-03-17) |
| `trades` (action=SELL) | 6 (2026-03-31) | 7 (**2026-05-03**) | 0 | 0 |
| `ai_journal` | **16** (2026-05-04 02:03) | **6** (**2026-05-04 02:04**) | 0 (2026-04-20) | 0 (2026-04-20) |

### Most-recent activity by routine

- **Signal emission:** stopped for all 4 halted players. Last halted-player signal write was ollama-llama on 2026-05-01, 3 days ago. The 1,143 pre-existing post-halt rows are all PRE-HM-C (HM-C suppression at `paper_trader.py:1873`/`save_signal` works — those rows are flagged `halted_emit=1`).
- **ai_journal:** **STILL ACTIVE TODAY** for sulu + ollama-llama. Latest entries 2026-05-04 02:03 (sulu) and 02:04 (ollama-llama). Other 2 halted players removed from `arena.providers` at some point and stopped naturally.
- **Trades:** 13 post-halt trades, **all SELL** action. Legitimate under `halt_mode='exit_only'` per `paper_trader.py:1090` halt gate. Not waste.
- **watchlist_signals:** matches HM-D's 13 post-halt-leak rows from ollama-llama (HM-C-flagged).

---

## Code-path analysis

### Signal emission — halt-aware ✅
`engine/paper_trader.py:1862,1872-1880, save_signal()` calls `halt_gate.can_emit_signal(conn, player_id)` and suppresses the DB write if the player is not active. HM-C added this. **Caveat:** the LLM call to GENERATE the signal happens upstream of `save_signal`, so the halt gate suppresses the DB row but does NOT suppress the upstream LLM cycle. However, signal emission for halted players appears to have stopped at the scheduler level (no halted-player signals written for 3+ days), so this concern is moot in practice.

### ai_journal — NOT halt-aware ⚠️
`main.py:520, run_journal()`:
```python
for pid, provider in arena.providers.items():
    try:
        entry = generate_journal_entry(provider, pid, prices)
        if entry:
            save_journal_entry(pid, entry)
```

No halt-mode check. Iterates every provider in `arena.providers`. `generate_journal_entry()` at `engine/ai_journal.py:18` also has no halt check — only an "already wrote today" idempotency check.

Result: the 2 halted players still in `arena.providers` (sulu, ollama-llama) get a journal LLM call every day during market or post_market sessions.

### Trades / sell-side — halt-aware ✅
`engine/paper_trader.py:1090-1093` `sell()` reads `halt_mode` and explicitly permits SELLs in `exit_only`. The 13 post-halt SELLs are valid exits. Working as designed.

### gemini-2.5-pro / grok-3 — naturally excluded
These two players are halted AND removed from `arena.providers`. Last journal 2026-04-20, last signal 2026-03-17. No active routines.

---

## Verdict: **B — Some waste (modest)**

- **Daily LLM cost:** ~2 calls/day to Ollie Box for journal generation on sulu + ollama-llama. Each call is a few seconds of GPU. Cost is trivial in absolute terms (negligible $/day equivalent on a local GPU; modest seconds of latency on the Ollie Box queue).
- **Output usage:** zero readers of halted-player journal entries identified in the audit. The journals exist purely as artifacts no human or downstream system consumes.
- **Trend:** without intervention, this continues indefinitely as long as sulu and ollama-llama remain in `arena.providers`. The other 2 halted players already stopped via removal from `arena.providers` — the same pattern applied to sulu/llama would solve it.

Verdict A doesn't fit (there IS waste). Verdict C doesn't fit (the waste is a few LLM calls/day, not "significant"). B is the right size.

## Recommended action

**Optional fix (HM-E-fix), ~5 min, low risk:**

Add a halt-mode filter in one of two places:

**Option 1 (preferred):** `engine/ai_journal.py::generate_journal_entry()` — gate at the function entry by adding a halt-mode check before the LLM call:
```python
# HM-E-fix: skip journal generation for halted players
halt_check = conn.execute(
    "SELECT halt_mode FROM ai_players WHERE id=?", (player_id,)
).fetchone()
if halt_check and (halt_check[0] or "active") != "active":
    conn.close()
    return None
```
Single source of truth, catches any future caller of the function.

**Option 2:** `main.py::run_journal()` — gate at the loop:
```python
for pid, provider in arena.providers.items():
    if not _is_active(pid):  # new helper
        continue
    ...
```
Localized but doesn't catch any other journal-generation paths (`engine/leader_signal.py:183` also writes to `ai_journal` — would need same check).

**Option 1 wins** because it places the gate at the LLM-cost source, regardless of caller.

## Open questions for the Admiral

1. **Are halted-player journal entries used anywhere I missed?** The audit found zero readers, but if a dashboard panel or weekly review surfaces them, removing the writes would lose that surface area.
2. **Should the same halt-aware pattern be applied preemptively to other "daily routine" callers** (`engine/leader_signal.py:183`, market-commentary generators, etc.)? Listing them would be a separate small audit.
3. **Are sulu and ollama-llama meant to be entirely retired (remove from `arena.providers`) or kept available for future reactivation?** If the latter, the halt-gate-in-routine pattern is right; if the former, the cleaner action is to drop them from arena loading at startup (matching what was done for gemini-2.5-pro / grok-3).
