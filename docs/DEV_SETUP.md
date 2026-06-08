# Dev Setup

## Activate the commit-time invariant gate (one-time, per clone)

This repo ships a version-controlled pre-commit hook (`.githooks/pre-commit`)
that blocks any commit which breaks a safety invariant — notably the O-Tasty
Phase-2 paper-executor guards (flag-pinned-False, no-ungated-submit, paper-only
client, OTASTY-scoped creds, account-pinned `PA3YVDTUH5CB`, no Schwab / no fleet
broker-wrapper) and the Kirk holdings guard.

Git cannot auto-activate a tracked hooks directory on clone, and the deps venv
it runs against (`.venv-deps/`) is gitignored. So after a fresh clone, run:

```sh
./scripts/install_hooks.sh
```

This sets `core.hooksPath → .githooks` and builds `.venv-deps` with `pytest` +
the project deps. Idempotent — re-run it after `requirements.txt` changes.

### What the gate runs

```sh
.venv-deps/bin/pytest -q \
  tests/test_otasty_shadow_invariants.py \
  tests/test_kirk_holdings_guard.py
```

A failing invariant prints `INVARIANT FAILED — commit blocked` and aborts the
commit (exit 1). This is the local checkpoint that would have caught c7c36ff
(the Phase-2 Alpaca wiring) before it landed.

### Layers of defense

| Layer | Where | Catches |
|-------|-------|---------|
| Pre-commit hook | local, `.githooks/pre-commit` | invariant break at commit time |
| `otasty-invariant.yml` | GitHub Actions | same O-Tasty invariants on push/PR (remote backstop) |

> The pytest config sets `pythonpath = ['.']` (`pyproject.toml`) so the
> console-script `.venv-deps/bin/pytest` resolves `import engine.*` — without it
> the Kirk guard would fail to import and falsely block every commit.
