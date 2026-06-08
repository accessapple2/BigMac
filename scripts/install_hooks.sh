#!/bin/sh
# One-time dev bootstrap: activate the version-controlled commit-time invariant
# gate (.githooks/pre-commit) and build the deps venv it runs against.
#
# Run once per fresh clone:
#     ./scripts/install_hooks.sh
#
# Idempotent — safe to re-run (e.g. after requirements.txt changes).
#
# Why this isn't automatic: git cannot auto-activate a tracked hooks dir on
# clone (security), and .venv-deps/ is gitignored. This script wires both.
set -e

# Resolve repo root regardless of where the script is invoked from.
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

echo "→ Pointing git at the tracked hooks dir (.githooks)…"
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit 2>/dev/null || true

if [ ! -x ".venv-deps/bin/pytest" ]; then
  echo "→ Creating .venv-deps (deps venv for the invariant gate)…"
  python3 -m venv .venv-deps
fi

echo "→ Installing test + runtime deps into .venv-deps…"
./.venv-deps/bin/python -m pip install --quiet --upgrade pip
# Lean set covering the guarded tests' actual import chain (otasty = stdlib;
# the Kirk guard pulls engine.kirk_advisory → pandas/numpy/ccxt/crypto/...).
# Deliberately NOT `-r requirements.txt` — that drags in openbb (heavy/slow) for
# no benefit here. The smoke-run below is the safety net: if a future guarded
# test imports something new, this fails loudly and the list gets extended.
./.venv-deps/bin/python -m pip install --quiet \
  pytest pandas numpy pytz ccxt cryptography orjson rich requests python-dateutil

echo "→ Smoke-running the invariant gate…"
./.venv-deps/bin/pytest -q \
  tests/test_otasty_shadow_invariants.py \
  tests/test_kirk_holdings_guard.py

echo ""
echo "✅ Hook active. The pre-commit gate now blocks any commit that breaks the"
echo "   O-Tasty Phase-2 safety invariants or the Kirk holdings guard."
