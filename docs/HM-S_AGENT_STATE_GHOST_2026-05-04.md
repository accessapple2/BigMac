# HM-S — agent_state Ghost Investigation
*2026-05-04 evening, Scotty investigation, no fixes applied*

## Question

CLAUDE.md and the HM-A spec describe `agent_state.is_halted` as the drawdown-halt source of truth. The `agent_state` table does not exist in `data/trader.db`. **Is the drawdown-halt feature actually working in production?**

---

## Verdict: **C — Feature is dead but harmless** + documentation drift

The drawdown-halt protection cited in CLAUDE.md **DOES work in production**, but **it does not read from `agent_state`** as documented. The only actual reader of `agent_state` is the post_earnings_drift agent's per-instance disable flag, which silently degrades to "not halted" via a bare `except: return False`. PED is paper-only via a separate `gated=True` flag, so the dead halt-check cannot cause real-money damage.

**No URGENT flag.** Live gate-flip soak is safe to continue.

---

## Phase 2.1 — Where (if anywhere) does agent_state live?

Searched 13 `.db` files in the repo (`./data/*.db`, `./signal-center/*.db`, root-level `./trader.db`, `./autonomous_trader.db`, .venv DBs):

```
agent_state: NOT FOUND IN ANY DB.
```

`data/trader.db` tables matching `agent`: `agent_allocation`, `agent_id_aliases`, `agent_memory`, `agent_ratings`, `debate_agent_verdicts`, `user_agents`. None of these match the schema described in code.

---

## Phase 2.2 — Code path inventory

Total `agent_state` references in non-archived production code: **2**

| File:line | Type | Statement |
|---|---|---|
| `main.py:3478` | comment | `# Halt-respect via agent_state.is_halted (paper_trader.py:550 pattern).` |
| `agents/post_earnings_drift.py:56` | `SELECT` (only reader) | `SELECT is_halted FROM agent_state WHERE agent=? LIMIT 1` |

**Zero writers.** No `INSERT INTO agent_state`, no `UPDATE agent_state`, no `CREATE TABLE agent_state` anywhere in the codebase or SQL fixtures. The feature was never built — the table was referenced in code without ever being created.

### CLAUDE.md vs reality

CLAUDE.md states:
> The `is_halted` column is preserved for the drawdown-halt system in `ai_brain.py` and `risk_manager.py` (which reads from a different table — `agent_state`, not `ai_players`...

**This is factually incorrect:**

```bash
$ grep -n "agent_state" engine/ai_brain.py engine/risk_manager.py
# (no matches)
```

Neither `ai_brain.py` nor `risk_manager.py` references `agent_state`. The actual drawdown-halt path is:

- `engine/ai_brain.py:817`: `is_halted, drawdown = self.risk.check_drawdown(player_id)`
- `engine/risk_manager.py:867-893` `check_drawdown()`: reads `portfolio_history` (3,562 rows in live DB), computes `(peak - current) / peak >= max_drawdown_pct (0.20)`, returns transient `(bool, float)`.

The drawdown halt is **runtime-recomputed every signal cycle**, not stored in any flag table. CLAUDE.md misdescribes the architecture entirely.

---

## Phase 2.3 — Reachability evidence

### post_earnings_drift IS scheduled and IS imported

- `main.py:3486`: `from agents.post_earnings_drift import _agent as ped_agent`
- `main.py:3487`: `if ped_agent.is_halted(): ... return`
- `main.py:3541`: `schedule.every(15).minutes.do(run_post_earnings_drift)`

So the code path runs every 15 minutes during the live trader process.

### The agent_state SELECT silently degrades to False

`agents/post_earnings_drift.py:49-63`:
```python
def is_halted(self):
    try:
        ...
        cur.execute(
            "SELECT is_halted FROM agent_state WHERE agent=? LIMIT 1",
            (self.name,),
        )
        row = cur.fetchone()
        ...
        return bool(row and row[0])
    except Exception:
        return False  # ← silent fallback when table doesn't exist
```

When the missing table causes `OperationalError: no such table: agent_state`, the bare `except Exception` swallows it and returns `False`. PED's caller at `main.py:3487` then sees "not halted" and proceeds to `ped_agent.scan(md)`.

### Zero log evidence of PED activity today

```
$ grep -i "post_earnings_drift\|PED " logs/trader.log
(no matches today)

$ grep -i "agent_state" logs/trader.log
(no matches)

$ grep -i "drawdown" logs/trader.log
(no matches)
```

Possible interpretations:
- PED's schedule fires every 15 min, but `fetch_earnings(universe)` returns no fresh post-earnings tickers in the 1–48hr window today, so `md` is empty, `scan()` returns `[]`, no log line is emitted (no-op silent path).
- The 15-min lockout (`_ped_state["last_run"]`) plus quiet days may compound silence.
- No errors are visible because the bare `except` swallows them.

This is **silent compatibility-by-luck.** The mechanism doesn't error because the exception is swallowed; the agent doesn't trade because no signals match; nobody notices the table is missing.

### PED is paper-only by separate gating

```python
# agents/post_earnings_drift.py
_GATE_TRADES = 30
class PostEarningsDriftAgent:
    def __init__(self, enabled=True):
        ...
        self.gated = True   # line 47

    def scan(...):
        ...
        signals.append({
            ...
            "meta": {
                ...
                "gated": self.gated,
                "paper_only": self.gated,    # line 130
            },
        })
```

`self.gated = True` is hard-coded. So even if PED's broken `is_halted()` lets it run unhalted AND fresh post-earnings tickers appear AND scan() emits SHORT signals, those signals are flagged `paper_only=True` and any downstream executor must respect that gate.

---

## Safety implications for the live gate-flip

| System | Status | Source of truth | Working? |
|---|---|---|---|
| **20% drawdown halt** (all agents) | ✅ Functional | `portfolio_history` peak/current, recomputed each cycle | YES |
| **Per-player halt_mode** (`exit_only`/`full`) | ✅ Functional | `ai_players.halt_mode` (post-HM-B single source) | YES |
| **PED per-agent halt flag** | ❌ Dead | `agent_state.is_halted` (table missing) → silent False | NO, but contained |
| **Manual halt SQL runbook** | ✅ Functional | `UPDATE ai_players SET halt_mode='exit_only'` | YES |

The two protections that matter for the gate-flip soak (drawdown auto-halt + manual halt runbook) are both fully functional. PED's broken halt is contained because PED is paper-only (`gated=True`).

**The risk is not in the running system. The risk is in the documentation.** A future contributor reading CLAUDE.md might:
- Believe drawdown-halt has a persistent flag table (it doesn't — it's transient)
- Try to pause the drawdown-halt system by inserting an `agent_state` row (would do nothing)
- Try to halt PED by inserting an `agent_state` row (would not error, but would also not work since the table doesn't exist)

---

## Recommended action

### Priority 1 — Documentation fix (~5 min, MEDIUM priority)

Update CLAUDE.md "Why both `is_halted` and `halt_mode`" section. The text:
> The `is_halted` column is preserved for the drawdown-halt system in `ai_brain.py` and `risk_manager.py` (which reads from a different table — `agent_state`, not `ai_players` — but the column-name parity prevents accidental drift).

…should become something like:
> The drawdown-halt protection in `engine/risk_manager.py::check_drawdown()` is computed transiently from `portfolio_history` peak/current ratio (>= 20% drawdown ⇒ halt), not from any flag table. There is no `agent_state` table in the live DB despite a referenced query in `agents/post_earnings_drift.py:56` (which silently degrades to "not halted" via bare `except`). HM-S 2026-05-04 confirmed the broken PED halt-flag is contained by `gated=True` paper-only gating.

### Priority 2 — Optional PED cleanup (~10 min, LOW priority, deferred)

Two paths to fix the dead `is_halted()` in `agents/post_earnings_drift.py:49-63`:

- **Option α (preferred):** Replace with a simpler `enabled` toggle. PED already has `self.enabled` at line 45 and `if not self.enabled or self.is_halted()` at line 66. Just delete `is_halted()`, keep `if not self.enabled`. Minus 15 lines of dead code.
- **Option β:** Create the `agent_state` table + admin UI/SQL pattern to actually use it. More work, more surface area, and HM-B just dropped a parallel halt column for the same reason. Reject β.

Defer to a future session. Not blocking gate-flip soak.

### Priority 3 — Investigate why no PED log lines (~15 min, LOW)

Even with `is_halted()` returning False, PED's silent path is suspicious. Worth a Tuesday probe: trigger `run_post_earnings_drift()` manually (or watch for the next 15-min cycle with debug logging) and confirm the function actually executes. If it never executes, the schedule wiring may also be broken — not a safety issue but architectural debt.

---

## Open questions for the Admiral

1. **Is the drawdown-halt mechanism (transient `check_drawdown` from portfolio_history) what you intended, or did you intend a persistent flag table?** The current implementation is robust against process restarts (every cycle recomputes) but cannot be manually overridden — the only way to "unhalt" a drawdown-halted agent is to wait for the portfolio to recover above the peak threshold, OR to manually insert a higher peak in `portfolio_history`. Both are awkward.
2. **PED is paper-only (`gated=True`) — is that intentional or stale config?** Per the file's docstring: "Gated like bull_spread_v1: paper-only until 30 trades + positive expectancy." If PED has been running for weeks with zero log evidence and zero trades, the gate-promotion criterion may never be met.
3. **Should HM-S be split into HM-S-docs (CLAUDE.md fix) and HM-S-code (PED cleanup)?** The docs fix is fast and high-value. The code cleanup is small but introduces a behavior question (does anyone use PED at all?).
