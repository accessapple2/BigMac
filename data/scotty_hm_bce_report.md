# HM-BC.E — init_db Fail-Loud Hardening — Discovery Report (BCE.0)

**Date:** 2026-05-12
**Operator:** Scotty (Opus 4.7)
**Status:** BCE.0 discovery, BCE.1 apply, and BCE.C verify all complete. Captain confirmed Q1=narrow, Q2=raise+log.error (no NTFY).

---

## Background

HM-BC.1 (2026-05-11) discovered that `engine/ghost_scoring.py::init_db()` silently auto-recreated `data/ghost_trades.db` after the file was renamed away. The mechanism: `sqlite3.connect("/path/to/missing.db")` in Python **silently creates** the file at the requested path; `CREATE TABLE IF NOT EXISTS` then populates an empty schema; callers see "everything works, just no data." Silent data loss.

This hardening makes `init_db()` refuse to operate against a missing canonical DB.

---

## init_db inventory — 18 modules

```
engine/ghost_scoring.py:73      init_db          ← HM-BC offender
engine/scenario_modeler.py:58   init_db
engine/gex_engine.py:638        init_db(db_path=DB_PATH)
engine/portfolio_optimizer.py:64 init_db
engine/rebalancer.py:68         init_db
engine/debate_engine.py:631     init_db
engine/pipeline.py:63           init_db
engine/fast_scanner.py:99       init_db
engine/ready_room.py:54         _init_db
engine/alpha_signals.py:85      _init_db
engine/deep_scan.py:142         _init_db
engine/holodeck_readyroom.py:20 _init_db
engine/team_advisor_grok.py:87  _init_db
engine/oi_tracker.py:25         _init_db
engine/pattern_matcher.py:31    _init_db
engine/wb_advisory_team.py:69   _init_db
engine/correlation_monitor.py:24 _init_db
engine/bridge_vote.py:69        _init_db
```

(Plus `scripts/recapitalize_player.py::ensure_schema` and `scripts/benchmark_cycle_player.py::ensure_schema` — different shape, not in scope.)

Every one of these calls `sqlite3.connect(...)` against a path that *should* already exist, and every one of them will silently auto-create the file if it doesn't. They are all latent silent-revival points by the same mechanism.

## The known offender — `engine/ghost_scoring.py`

```python
ROOT       = Path(__file__).resolve().parent.parent
DB_PATH    = ROOT / "data" / "ghost_trades.db"

def _ghost():
    """Open ghost trades DB."""
    c = sqlite3.connect(str(DB_PATH), timeout=15)       # ← silent create
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=15000")
    return c

def init_db():
    conn = _ghost()                                      # ← silently creates DB_PATH
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ghost_trades (...)
    """)
```

The bug class: `_ghost()` is the single entry point and has no existence check.

### External callers

```
dashboard/app.py:16983   from engine.ghost_scoring import get_scorecard, init_db
dashboard/app.py:16997   from engine.ghost_scoring import get_recent_trades, init_db
dashboard/app.py:17010   from engine.ghost_scoring import capture_new_signals, check_outcomes, init_db
engine/ghost_scoring.py:440 run_daemon() → init_db()    (daemon is NOT running — com.ollietrades.ghost-advisor was archived in HM-BC.3)
engine/ghost_scoring.py:460 __main__ CLI               (manual only)
```

**ghost_scoring is NOT in the trader startup path.** It's invoked from three FastAPI endpoints on demand and from the (archived) ghost-advisor daemon. Restart-loop risk for a hard `raise` is zero — worst case is an HTTP 500 on the affected endpoint, which is exactly the desired loud-fail surface.

### Logger pattern in target file

```python
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [GHOST] %(message)s")
log = logging.getLogger("ghost_scoring")
```

Python `logging` (not rich-console). Errors land in `trader_error.log` per the 2026-05-05 sink-split doctrine. Good.

---

## Proposed fix pattern

```python
# === HM-BC.E ===
def _ghost():
    """Open ghost trades DB. Fails loudly if the canonical DB is missing."""
    if not DB_PATH.exists():
        log.error(
            "ghost_scoring: canonical DB missing at %s — refusing to auto-create. "
            "Check that data/ghost_trades.db was not renamed or moved. "
            "If this is a first-run bootstrap, run `python -m engine.ghost_scoring init` explicitly.",
            DB_PATH,
        )
        raise FileNotFoundError(
            f"ghost_scoring canonical DB missing at {DB_PATH}. "
            f"HM-BC.E refuses silent auto-create; see CLAUDE.md 'Ghost Tracking Architecture'."
        )
    c = sqlite3.connect(str(DB_PATH), timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=15000")
    return c

def init_db():
    """Idempotent schema setup. Requires DB_PATH to already exist (HM-BC.E)."""
    conn = _ghost()
    conn.executescript("""...""")
# === /HM-BC.E ===
```

Notes:
- `log.error()` lands in `trader_error.log` (Python logging sink) — first place to look on incident.
- `raise FileNotFoundError` is the right exception class — it's exactly what's happening, and existing callers in `dashboard/app.py` already wrap endpoints in try/except that will yield HTTP 500 with traceback.
- One opt-in escape hatch for legitimate bootstrap (CLI `python -m engine.ghost_scoring init`) — but that's a follow-up if needed, NOT in BCE.1.
- Matches CLAUDE.md error-handling posture principle 2 (`{type(e).__name__}: {e!r}` style logs) at the call sites; here we own both sides.

---

## Q1 — Scope recommendation

**Recommend: NARROW (ghost_scoring.py only).**

Reasoning:
- HM-BC has concrete evidence ghost_scoring was the offender. The other 17 sites are *latent* — same mechanism, no observed incident.
- Several of the other init_db sites likely *want* auto-create on first run (e.g., `bridge_vote`, `oi_tracker`, `correlation_monitor` look like rolling local state that's legitimately bootstrapped). Touching them without per-file analysis risks breaking legitimate paths.
- CLAUDE.md error-handling posture (2026-05-05) explicitly says "**Going-forward, not retroactive.** The posture applies to: Every new code change that touches exception handling; Every code path that produces an error during normal operation that we discover."
- We only "discovered" ghost_scoring; per-posture, that's the one we fix.
- Future natural maintenance of the other 17 files will pick up the pattern as it touches them.

**Alternative: BROAD** — would mean per-file evaluation of whether each DB should fail-loud or auto-create. That's a separate audit (HM-BCE-broad if/when warranted). NOT this commit.

## Q2 — Failure mode recommendation

**Recommend: RAISE `FileNotFoundError` + `log.error` (no NTFY).**

Reasoning:
- `raise` is loud enough — caller endpoints surface HTTP 500 + traceback in trader_error.log. That's exactly the "next time this happens we'll notice immediately" outcome HM-BC.E exists to enable.
- launchd respawn-loop risk: **zero**. ghost_scoring is not in `main.py` startup; the trader will not crash from this. Only the affected dashboard endpoints fail.
- NTFY is reserved for architecture-class paths per CLAUDE.md (broker submits, halt_mode writes, position-of-record). Ghost-scoring is research/observability — important but not in the critical-write set. Skip NTFY to avoid alert-fatigue.
- `log.error` produces a forensic line in `trader_error.log` with the canonical path and an actionable hint ("check that the file was not renamed or moved"). That's what you read when the 500 lands.

**Alternative: log-and-skip-gracefully** — would mean returning `None` or an empty connection. Risk: still hides the issue if logs aren't watched. The whole point of HM-BC.E is to stop being quiet about this class of bug. Rejected.

**Alternative: raise + NTFY** — overkill for non-architecture-class path. Rejected unless Captain wants belt-and-braces.

---

## Summary for Captain

| Q | Recommended | Why |
|---|---|---|
| Q1 scope | **ghost_scoring.py only** | CLAUDE.md going-forward posture; only file with proven incident |
| Q2 failure mode | **raise FileNotFoundError + log.error** | Loud, forensic, no NTFY noise, zero respawn risk |

If Captain confirms, BCE.1 is a ~15-line diff at `_ghost()` in `engine/ghost_scoring.py`, no service restart needed (defensive code only fires when DB is missing).

---

## BCE.1 — Applied (Captain approved both recommendations)

Diff applied to `engine/ghost_scoring.py::_ghost()`, wrapped with `# === HM-BC.E ===` / `# === /HM-BC.E ===` anchors. No other changes. `init_db()` inherits the guard via its existing `_ghost()` call — no edit needed there.

### Smoke results (using `./venv/bin/python3` per BHBI lesson)

| Check | Result |
|---|---|
| `py_compile engine/ghost_scoring.py` | ✅ syntax OK |
| `from engine.ghost_scoring import init_db, _ghost` | ✅ import OK |
| Guard fires when DB_PATH is missing | ✅ `FileNotFoundError` raised + log.error line written |
| Real DB path still works | ✅ 784 rows in `ghost_trades` (matches HM-BC.1 restoration count) |

The guard's log line in practice:

```
2026-05-12 07:02:37,431 [GHOST] ghost_scoring: canonical DB missing at
/tmp/definitely_missing_ghost_db_hm_bc_e.db — refusing to auto-create.
Check that data/ghost_trades.db was not renamed or moved. See CLAUDE.md
'Ghost Tracking Architecture' for the two-system layout.
```

Lands in `trader_error.log` (Python logger sink) per the 2026-05-05 sink-split doctrine.

## BCE.C — Verify

- Anchor present in source: `# === HM-BC.E ===` at the start of `_ghost()`.
- No service restart performed (per directive — defensive code only fires on missing-DB).
- Live trader still at PID 8591 (untouched by this commit).

## Reversibility

Single-file change, anchor-marked. Revert with one `git revert` or by stripping the anchored block.

## Followups (NOT in this commit)

- **HM-BCE-broad:** per-file audit of the remaining 17 `init_db`/`_init_db` sites. Each needs evaluation of whether silent auto-create is the bug (HM-BC class) or the feature (first-run bootstrap class). NOT scheduled.
- Opt-in bootstrap CLI (`python -m engine.ghost_scoring init --bootstrap`) is the natural escape hatch if/when a legitimate fresh-DB scenario is needed. Defer until requested.

