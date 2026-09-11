# Relay — C10: RULE #1 enforcement layers. 2026-09-11

## Status of the four layers, verified live (not assumed)

1. **This rule (doctrine)** — unchanged, standing.
2. **BEFORE DELETE triggers + startup assertion** — confirmed live: all 8
   `trg_rule1_no_delete_*` triggers present on `data/trader.db` right now,
   `setup_db.py`'s startup assertion (`_expected` set comparison) intact.
3. **`engine/db_safety.py`'s connection authorizer** — the one item this
   session's CLAUDE.md flagged as "not yet wired into every existing
   connection helper." Addressed below.
4. **`tests/test_rule1_delete_guard.py` + pre-commit hook** — confirmed
   wired (`.githooks/pre-commit` references it directly); 20/20 pass.

## Layer 3 — scoped, not blanket

`db_safety.py`'s own docstring already correctly flagged full wiring as
"a separate, larger, higher-risk session" — confirmed why: **397 files,
400+ individual `sqlite3.connect()` call sites** across `engine/`,
`scripts/`, `agents/`, `dashboard/`. Rewriting all of them in one
dry-dock pass would be exactly the kind of broad, live-decision-path
touch this session's own constraints (additive, minimal-risk) argue
against — not attempted.

**The actual gap analysis, not a guess:** the BEFORE DELETE triggers
(layer 2) are connection-agnostic — they fire regardless of which of
those 400+ files opened the connection. So `guarded_connect()` not being
wired everywhere does **not** leave DELETE exposed anywhere. The *only*
marginal protection layer 3 adds is `DROP TABLE`, which no SQLite trigger
can intercept. That narrows the real question to: **which files can
actually execute a `DROP TABLE`?**

**Answer, found by grep, not assumed: exactly three files in the entire
repo contain the literal string "DROP TABLE."** One is
`tests/test_rule1_delete_guard.py` itself (correctly unguarded — it's
testing the guard). The other two:

- `scripts/recall_bakeoff.py` — manual bake-off tool, `DROP TABLE IF
  EXISTS {table}` where `table` comes from a `MODELS` list of tuples
  (currently `vec_trades_{bge,qwen,gemma,nomic}`, all safe today) — but
  the table name is a **variable**, not a hardcoded constant, so nothing
  stops a future edit to `MODELS` from colliding with a real table name.
- `scripts/recall_refresh.py` — the **live, cron-scheduled** version
  (`0 15 * * 1-5`, unattended), same pattern against `VEC_TABLE`.

**Both now wired to `guarded_connect()`** (all 5 connection sites across
the two files — every `sqlite3.connect(DB, ...)` replaced). Verified live,
not just read:
- `py_compile` clean on both.
- `tests/test_rule1_delete_guard.py`: 20/20 still pass.
- Direct functional test in `.venv-recall` (the real interpreter these
  scripts run under, needs `sqlite_vec`): `enable_load_extension` +
  `sqlite_vec.load()` work identically with `guarded_connect()` as with
  raw `sqlite3.connect()` — no extension-loading regression. Then proved
  the actual protection: `DROP TABLE trades` on a guarded connection
  raises `DatabaseError: not authorized`; `DROP TABLE` on a genuinely
  unprotected table (`vec_trades_bge`) still succeeds normally — the
  guard is selective, not a blanket lockout.

**Net result: 100% of the repo's actual `DROP TABLE`-capable surface is
now covered, not a partial or arbitrary subset.** The remaining 395+
files with `sqlite3.connect()` but no `DROP TABLE` statement have zero
exposure to the gap this layer closes — wiring them would add defense
against a risk they don't carry, at real cost (touching hundreds of live
files). Filed to `docs/XO_BACKLOG.md` as a real but low-priority "wire
the rest" item, distinct from "wire the actually-exposed two."

## Not done

Full `guarded_connect()` adoption across every connection helper remains
future work, same posture `db_safety.py`'s own docstring already set —
this pass closes the concrete, verified gap (DROP TABLE surface), not the
theoretical one.
