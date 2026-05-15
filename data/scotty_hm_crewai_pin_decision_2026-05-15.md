# Scotty: HM-CREWAI-PIN Decision Doc — 2026-05-15

**Status:** decision-ready. No code edited. No pip installed in live venv.

**Yesterday's scope doc:** `data/scotty_hm_crewai_pin_scope_2026-05-14.md`

## TL;DR — Captain recommendation

**Path D — Decouple `crew/__init__.py` from `crew.agents`** (new option,
discovered during scope refresh).

  - Yesterday's Path A (pin to 0.1.32) is **INVALIDATED** by scratch-venv test.
  - Path B (modernize) is **already done in `.venv-crew`** — main_crew.py
    runs there with crewai 1.11.1. The live trader (main.py, port 8080)
    doesn't *use* crewai; it just *transitively imports* it via
    `crew/__init__.py` → `crew/agents.py` → `from crewai.tools import tool`.
  - Path D: one-line guard in `crew/__init__.py` so dashboard's
    `crew.ensemble` import stops dragging `crew.agents` into the chain.
    Zero pip changes. Zero impact on main_crew.py. Unblocks LANE 2.

## Scratch-venv evidence (Path A invalidation)

Built `/tmp/scratch_crewai_pin_<TS>` with Python 3.9 + `crewai==0.1.32`:

```
$ python -c "from crewai.tools import tool"
ImportError: cannot import name 'tool' from 'crewai.tools'
(/private/tmp/scratch_crewai_pin_.../lib/python3.9/site-packages/crewai/tools/__init__.py)
```

Inspecting what 0.1.32's `crewai.tools` actually exports:

```python
>>> import crewai.tools
>>> [x for x in dir(crewai.tools) if not x.startswith('_')]
['agent_tools', 'cache_tools']
```

The `tool` decorator did NOT exist in `crewai.tools` at 0.1.32. It was
added LATER (present in 1.11.1, confirmed in `.venv-crew`). So pinning
backward to 0.1.32 does **not** restore the broken import. Yesterday's
hypothesis was wrong; pin-backward path is dead.

Also tested `crewai==0.5.0` on Python 3.9: import chain itself fails with
`TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'`
because `crewai/utilities/rpm_controller.py` uses PEP-604 `X | None`
syntax (Python 3.10+ only).

Also tested `pip install crewai-tools` on Python 3.9: `ERROR: No matching
distribution found`. Modern crewai-tools requires Python 3.10+.

## Architecture clarified

The repo actually has TWO trader entry points by design:

| Process | Port | Python | crewai | Used by |
|---|---|---|---|---|
| `main.py` (live trader) | 8080 | `venv/bin/python3` (3.9.6) | 0.5.0 (broken on 3.9, unused at runtime) | Captain — production |
| `main_crew.py` | 8000 | `.venv-crew/bin/python3` (3.12.13) | 1.11.1 + crewai-tools 1.11.1 | Reserved for crewai-dependent agents |

`main.py` does **NOT** import `crew` directly (grep confirmed). The live
trader's process is entirely crewai-free at runtime.

The broken import surfaces through `dashboard/app.py:3125`:
```python
from crew.ensemble import AgentScoreboard, _bucket_for_agent, _source_policy, ...
```
This is a lazy import inside a route handler — but the moment Python loads
`crew`, the package's `crew/__init__.py` eager-imports `crew.agents`,
which fails on the missing `tool` decorator.

`crew/ensemble.py` itself has **zero** crewai imports:
```
imports: json, os, typing
        agents.mean_reversion, agents.momentum
        engine.agent_manager, engine.agent_scoreboard, engine.market_data
```

Confirmed `trader_error.log` shows the `ImportError` firing in an ASGI
request context — the LANE 2 audit's 500-error endpoints map directly to
routes that hit `crew.ensemble`.

## Path comparison

### ❌ Path A — pin crewai==0.1.32 (yesterday's recommendation)

**INVALIDATED.** `crewai.tools.tool` does not exist at 0.1.32. Doesn't fix
anything. Dead path.

### 🟡 Path B — modernize live venv to Python 3.12 + crewai 1.11.1

**Architecturally heavy, but technically valid.** Would require:

1. New Python 3.12 venv for the live trader (replace `venv/`)
2. Re-install every dependency under 3.12
3. Verify all 100+ pip packages work on 3.12 (pandas, scipy, yfinance,
   alpaca-py, polygon-api-client, all langchain-* …)
4. Trader restart with new interpreter
5. Test every code path that depends on a moved API
6. Live trading risk during transition

**Time estimate:** 4-8 hours of careful migration work + extended
soak window before Captain trusts it. Touches every file that imports
anything pip-installable.

**Pros:** future-proofs, addresses Python 3.9 EOL warnings (visible in
my pip output today), enables crewai-tools etc.

**Cons:** big-bang change. Risk-cost not justified by the LANE 2 endpoint
fix alone. Would belong in a separate **HM-PYTHON-3.12-MIGRATE** ticket
with its own scope, soak, and rollback plan.

### ✅ Path D — Decouple `crew/__init__.py` from `crew.agents` (RECOMMENDED)

**Surgical, one-file change.** Replace the eager import in
`crew/__init__.py` with a guarded one:

```python
# Before:
from crew.agents import (
    create_scout, create_architect, create_backtester,
    create_critic, create_commander,
)

# After:
try:
    from crew.agents import (
        create_scout, create_architect, create_backtester,
        create_critic, create_commander,
    )
except ImportError:
    # crewai not available in this venv (live trader uses venv/, which has
    # incompatible crewai 0.5.0 on Python 3.9). main_crew.py runs in
    # .venv-crew with the working crewai 1.11.1 and re-loads these symbols.
    create_scout = create_architect = create_backtester = None
    create_critic = create_commander = None
```

**Result:**
- Live trader (venv) — dashboard can now `from crew.ensemble import ...` without crashing
- main_crew.py (.venv-crew) — unchanged, the import resolves cleanly to the real symbols
- LANE 2 endpoints stop 500'ing
- No pip install. No requirements.txt change. No Python upgrade.

**Pros:**
- Smallest possible diff (~6 lines)
- Reversible (single git revert)
- Zero blast radius — main_crew.py and `.venv-crew` flow unchanged
- Honestly reflects the two-process architecture

**Cons:**
- Leaves a latent ImportError trap in the live venv — any future code in
  the live trader that tries to call `create_scout()` etc. will hit
  `TypeError: 'NoneType' object is not callable`. Mitigation: add a
  pytest assertion that confirms these are None in the live venv +
  truthy in `.venv-crew`.
- Does not modernize anything. Python 3.9 EOL warnings persist.
- Pydantic V1/V2 mixing warnings persist.

## Detailed apply sequence (Path D)

```bash
cd ~/autonomous-trader
git checkout -b hm-crewai-pin-decouple

# 1. Read current crew/__init__.py to confirm the exact import block
sed -n '3,10p' crew/__init__.py

# 2. Edit crew/__init__.py — wrap the import in try/except
#    (use Edit tool with the diff above)

# 3. Verify trader-side venv loads dashboard route without ImportError:
venv/bin/python3 -c "from crew.ensemble import AgentScoreboard; print('ok')"

# 4. Verify main_crew.py-side venv still resolves the real symbols:
.venv-crew/bin/python3 -c "from crew import create_scout; print(create_scout)"
# Should print <function create_scout at 0x...>

# 5. Run trader-side smoke tests:
venv/bin/python3 -c "import dashboard.app; print('dashboard imports ok')"

# 6. Write test: tests/test_crew_init_guard.py
#    - Asserts that in venv/, create_scout is None (graceful fallback)
#    - Skips when crewai 1.11.1 is the importable version

# 7. Commit + push + PR
```

## Test plan

```python
# tests/test_crew_init_guard.py
import sys
from importlib import import_module, reload

def test_crew_init_handles_broken_crewai():
    """When crewai.tools.tool is missing (live trader's Python 3.9 venv),
    importing crew package must not raise — the fallback symbols are None.
    """
    import crew
    # The guard fires when crewai is incompatible; fallback sets symbols to None.
    # In a venv where crewai 1.11.1 is present, the imports succeed instead.
    # Either is acceptable; the regression we're testing is "import doesn't crash".
    assert hasattr(crew, "create_scout")

def test_crew_ensemble_imports_without_crewai():
    """The route handler at dashboard/app.py:3125 should be able to import
    crew.ensemble without dragging crew.agents (which needs crewai.tools.tool).
    """
    from crew.ensemble import AgentScoreboard
    assert AgentScoreboard is not None
```

## Rollback

Single revert: `git revert <commit>`. The pre-Path-D state restores
exactly. No data migration, no service state to undo.

## Implementation order

1. Captain approves Path D (this doc).
2. Scotty opens `hm-crewai-pin-decouple` branch with the one-line guard.
3. Tests added (`tests/test_crew_init_guard.py`).
4. PR opened with diff + test plan.
5. Captain merges.
6. **No trader restart needed** — the guard fires on the next dashboard
   request that touches `crew.ensemble`, no service-level bytecode reload
   required because dashboard reloads modules on cold-routes naturally.
   *(But verify after merge — see test plan above.)*
7. Verify LANE 2's 500 endpoints now return 200.

## Risk profile

| Risk | Impact | Mitigation |
|---|---|---|
| Dashboard route assumes `create_scout` truthy | TypeError 500 | grep for callers in dashboard; none expected since dashboard uses `crew.ensemble` not `crew.agents` |
| main_crew.py also picks up the guard accidentally | None — the guard only fires when import FAILS; in `.venv-crew` it succeeds | n/a — guard is exception-driven |
| Future engineer copies the guard pattern without understanding | Spread of try/except import noise | docstring on the guard explains the two-venv reason |

## Related followups (NOT in this PR)

- **HM-PYTHON-3.12-MIGRATE** — proper modernization of the live venv.
  Not urgent; deferred until a feature needs it.
- **HM-CREWAI-MODERNIZE** — once Python 3.12 is in play, move main_crew.py
  conventions into the live venv. Currently `.venv-crew` is fine for the
  crewai-dependent main_crew.py and it's untouched by today's work.
- **crew/ensemble.py audit** — confirm no other transitive crewai
  imports. (Verified clean today, but worth a periodic check.)

## Captain action

- [ ] Review this doc
- [ ] Approve Path D OR redirect to Path B (modernize) OR redirect to a
  fourth option I haven't considered
- [ ] On approval: Scotty proceeds with the branch + PR

## Files
- This doc: `data/scotty_hm_crewai_pin_decision_2026-05-15.md`
- Memory note: `project_hm_crewai_pin_ready.md` (in auto-memory)
- Yesterday's scope: `data/scotty_hm_crewai_pin_scope_2026-05-14.md` (kept for history)
- Scratch venv evidence: `/tmp/scratch_crewai_pin_<TS>/` (left in place, ephemeral)
