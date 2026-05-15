# Scotty: HM-CREWAI-PIN Scope — 2026-05-14

**Status:** read-only scoping. No package installed/pinned. No code edited.

## Current state

```
crewai 0.5.0
  Location: /Users/bigmac/autonomous-trader/venv/lib/python3.9/site-packages
  Required-by: (none — appears unused at top level but imported by crew/agents.py)

crewai-tools         NOT INSTALLED
langchain 0.1.0      installed (compatible with crewai 0.5.0)
langchain-openai 0.0.2.post1
langchain-core 0.1.23
langchain-community 0.0.20
```

## Broken import

```python
# crew/agents.py:14-15
from crewai import Agent
from crewai.tools import tool          # ← FAILS at runtime
```

Live test:

```
ImportError: cannot import name 'tool' from 'crewai.tools'
(/Users/bigmac/autonomous-trader/venv/lib/python3.9/site-packages/crewai/tools/__init__.py)
```

The `crewai.tools` submodule exists but its `__init__.py` no longer exports
`tool`. This is a backward-incompatible change introduced when crewai's tool
ecosystem was extracted into a separate `crewai-tools` package.

## Git history

```
859a4f0  "TradeMinds Season 5"            ← imported `from crewai.tools import tool` here
5498c34  "OllieTrades April 10 ..."       ← prior baseline
50ef95c  "HM-C: filter halted_emit=0"     ← unrelated edit to crew/agents.py
8eabdd2  "fix(kirk): HM-BK-residual"      ← last touch, 2 days ago
```

The broken import has been in the codebase since the S5 era. It worked
historically (on older crewai versions) but broke when the venv was upgraded
to crewai 0.5.0 at some point. Without the original `pip freeze` from when
this code last ran, we can't pin to the exact working version — but available
PyPI versions give us a tight bracket:

```
Available crewai versions:
  0.5.0   ← current (broken)
  0.1.32, 0.1.24, 0.1.23, 0.1.17, 0.1.16, 0.1.15, 0.1.14, 0.1.7,
  0.1.6, 0.1.5, 0.1.3, 0.1.2, 0.1.1, 0.1.0
```

The major version jump 0.1.32 → 0.5.0 is where the API broke.
**Hypothesis: 0.1.32 is the last version where `from crewai.tools import tool` worked.**

## Two upgrade paths

### Path A — PIN BACKWARD to crewai 0.1.32 (minimum-change)

Requirements:
- `pip install crewai==0.1.32` (replace 0.5.0)
- No code edits needed in `crew/agents.py`
- Update `requirements.txt` to pin `crewai==0.1.32`

**Pros:**
- Smallest diff, no code change
- Restores the function the S5 author intended

**Cons:**
- Going backward is technical debt — eventually we will be forced to migrate
  (security patches, langchain compat, Python 3.9 EOL)
- Pydantic V1/V2 mixing warning is currently present in 0.5.0; downgrading may
  reintroduce a different set of warnings
- May conflict with other deps in venv that already moved past crewai 0.1.x
  assumptions (langchain 0.1.0 is contemporary with crewai 0.5.0; downgrade
  may require langchain downgrade too — risk of cascading deps)

**Verification before pinning:** run `pip install crewai==0.1.32` in a SCRATCH
venv (not the live one) and confirm `from crewai.tools import tool` resolves
+ that langchain is still compatible.

### Path B — UPGRADE FORWARD to crewai 0.5.x + install crewai-tools (modernize)

Requirements:
- `pip install crewai-tools` (the extracted package)
- Edit `crew/agents.py:15` to update the import:
  ```python
  from crewai_tools import tool        # or whatever the new import path is
  ```
- Test that `Agent` class API hasn't shifted (crewai 0.1 → 0.5 is a major
  version bump — likely some Agent fields renamed or removed)

**Pros:**
- Forward-compatible, aligned with current crewai ecosystem
- Pydantic V2 native (resolves the V1/V2 mixing warning)
- Stays in sync with langchain 0.1.x which is already installed

**Cons:**
- Code changes needed beyond just the one import (Agent class API may differ)
- Need to verify each method we use on `Agent` still exists in 0.5.x
- Higher risk of subtle behavior changes that don't surface as import errors

**Verification before upgrade:** in a SCRATCH venv, `pip install crewai==0.5.0
crewai-tools`, then `import crew.agents` and check for any `AttributeError`s.

## Recommended path

**Path A (pin backward to 0.1.32) for this fix.** Reasons:

1. The broken import is in `crew/agents.py` which is part of the active
   trading fleet (Kirk, Pike, navigator's chekov_rules). Behavior changes
   from a major-version upgrade are higher-risk than freezing on the version
   the code was written for.

2. The 2 currently-broken endpoints flagged by LANE 2 audit are minor
   (debug-tier dashboard endpoints, not in trader hot path). The fix is
   urgent-low.

3. **Bigger principle:** when the code expects vN of a library, pin to vN.
   Don't upgrade libraries that have already-written code unless the upgrade
   has a specific motivation (security fix, blocking feature). Modernization
   to crewai 0.5.x is a separate, larger workstream.

4. Pydantic V1/V2 warnings can be addressed in a future HM-CREWAI-MODERNIZE
   ticket after observability is in place to catch any behavior regressions.

## Safe-apply sequence

```bash
cd ~/autonomous-trader

# 1. Snapshot current venv state in case rollback needed
venv/bin/pip freeze > /tmp/pip_freeze_pre_crewai_pin_$(date +%Y%m%d_%H%M%S).txt

# 2. Verify the fix in a SCRATCH venv first
python3 -m venv /tmp/scratch_crewai
/tmp/scratch_crewai/bin/pip install crewai==0.1.32 langchain langchain-openai
/tmp/scratch_crewai/bin/python -c "from crewai.tools import tool; print('ok')"

# 3. If scratch works, apply to live venv
venv/bin/pip install crewai==0.1.32

# 4. Smoke gate
venv/bin/python -c "from crewai.tools import tool; print('ok')"
venv/bin/python -c "from crew.agents import *; print('ok')" 2>&1 | head -5

# 5. Update requirements.txt to pin
echo "crewai==0.1.32" >> requirements.txt  # or sed-replace existing line

# 6. Commit on new branch (do NOT push to main directly)
git checkout -b hm-crewai-pin
git add requirements.txt
git commit -m "fix(deps): pin crewai to 0.1.32 (HM-CREWAI-PIN) — restores from crewai.tools import tool"

# 7. Verify trader smoke test
venv/bin/python -c "import main; print('main imports ok')"
venv/bin/python -c "import dashboard.app; print('dashboard imports ok')"

# 8. Push + PR review
git push -u origin hm-crewai-pin
```

**Post-merge:** trader restart required (same as HM-CD-ROUTES). Do post-close.

## Risk

| Risk | Impact | Mitigation |
|---|---|---|
| crewai 0.1.32 unavailable | install fails | unlikely; PyPI keeps old versions indefinitely |
| langchain incompat with old crewai | other agents break | test in scratch venv first |
| Behavior diff between 0.5 and 0.1 | live trading logic changes | crew/agents.py code is unchanged; behavior should match what S5 author intended |
| Pydantic V1 deprecation in future | eventually forced to upgrade | track in HM-CREWAI-MODERNIZE backlog |

## Followup

- File **HM-CREWAI-MODERNIZE** as a separate ticket: forward-port to crewai
  0.5.x + crewai-tools. Captain decides timing.
- Audit other latent imports that could break with future crewai upgrades:
  `grep -rn "from crewai" --include="*.py" .`

## Captain action

- [ ] Review this scope doc
- [ ] Decide Path A (pin) vs Path B (modernize)
- [ ] If Path A: run scratch-venv verification, then live install + commit
- [ ] Branch `hm-crewai-pin` → PR → merge → trader restart post-close
