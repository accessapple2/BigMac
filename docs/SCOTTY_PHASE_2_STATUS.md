# SCOTTY 2.7 — Phase 2 Status

> Five autonomous-safe tasks shipped + status report + push to `origin/main`.
> No live-trading state mutated, no halts touched, no service restarts.

**Date:** 2026-05-08
**Branch:** `main`
**Commits added this sprint:** 6 (Tasks 1–5 + status doc)

---

## 1. Tasks shipped

| # | Task | Status | Commit | Evidence |
|---|---|---|---|---|
| 1 | Track Phase 1 orphan docs | **SHIPPED** | `6d41f58` | 8 files, 1840 insertions — `git log -1 --stat 6d41f58` |
| 2 | Auth Phase 0 — helper + tests + runbook | **SHIPPED** | `53b9113` | 3 files, 506 insertions; **11/11 pytest green** (`tests/test_auth.py`); zero callers of `verify_admin_token` outside the module + its test |
| 3 | HM-AY-β model watcher digest probe | **SHIPPED** | `020df0c` | All 6 standard installed models now report real upstream digests matching locals; verified in fresh `--dry-run` |
| 4 | DEAD_TABLES_AUDIT writer-grep finalization | **SHIPPED** | `162607a` | Append-only Section 6, 86 lines; UNCERTAIN bucket reduced from 24 → 0; 6 promoted to TRULY DEAD, 1 to WIRED-NEVER-FIRED, 16 reclassified ACTIVE |
| 5 | Track launchd plists in `infra/launchd/` | **SHIPPED** | `0086c34` | 3 plists + README, 178 lines; canonical files at `~/Library/LaunchAgents/` untouched |
| 6 | This status report | **SHIPPED** | (this commit) | — |

---

## 2. Test results

### Auth Phase 0 — `pytest tests/test_auth.py -v`

```
============================= test session starts ==============================
platform darwin -- Python 3.9.6, pytest-8.4.2, pluggy-1.6.0
collected 11 items

tests/test_auth.py::test_valid_totp_returns_200 PASSED                   [  9%]
tests/test_auth.py::test_invalid_totp_returns_401 PASSED                 [ 18%]
tests/test_auth.py::test_service_token_valid_returns_200 PASSED          [ 27%]
tests/test_auth.py::test_service_token_invalid_returns_401 PASSED        [ 36%]
tests/test_auth.py::test_recovery_key_valid_then_reuse_rejects PASSED    [ 45%]
tests/test_auth.py::test_missing_authorization_header_returns_401 PASSED [ 54%]
tests/test_auth.py::test_non_bearer_scheme_returns_401 PASSED            [ 63%]
tests/test_auth.py::test_empty_bearer_token_returns_401 PASSED           [ 72%]
tests/test_auth.py::test_constant_time_compare_used_in_source PASSED     [ 81%]
tests/test_auth.py::test_totp_path_does_not_consume_recovery PASSED      [ 90%]
tests/test_auth.py::test_no_secrets_in_module PASSED                     [100%]

============================== 11 passed in 0.19s ==============================
```

Tests run via `/Users/bigmac/autonomous-trader/venv/bin/python3 -m pytest`
(the trader runtime venv, where `fastapi` 0.115.14 + `pyotp` 2.9.0 live).
`pytest 8.4.2` was installed into that venv during the sprint — the trader
runtime itself never imports pytest, so this addition is dev-only.

### Model watcher post-fix — `python3 scripts/model_watcher.py --dry-run`

Layer 1 (installed digests) — all 6 standard library models now report
real upstream digests where they previously read `(unknown)`:

```
- `phi3:mini`        — local 4f2222927938 / upstream 4f2222927938 — current
- `gemma3:4b`        — local a2af6cc3eb7f / upstream a2af6cc3eb7f — current
- `mistral:7b`       — local 6577803aa9a0 / upstream 6577803aa9a0 — current
- `qwen3:8b`         — local 500a1f067a9f / upstream 500a1f067a9f — current
- `qwen3:14b`        — local bdbd181c33f2 / upstream bdbd181c33f2 — current
- `deepseek-r1:14b`  — local c333b7232bdb / upstream c333b7232bdb — current
- `plutus`           — local (not installed) / upstream (unknown) — current
- `picard`           — local (not installed) / upstream (unknown) — current
- `pike`             — local (not installed) / upstream (unknown) — current
```

`plutus`/`picard`/`pike` remain `(unknown)` because they are custom
Modelfile builds not present in the public Ollama library — the watcher
correctly reports `(unknown)` rather than crashing or false-positiving.

**Aggregate post-fix:** 0 installed updates · 34 HF fresh · 0 GH new
releases (one HF fresh dropped from the day's 35 due to the 7-day cutoff
moving forward an hour between runs — expected).

---

## 3. Wall clock + commit count

| | |
|---|---|
| Commits added | **6** (5 task commits + status report) |
| Lines added | ~3,100 across docs / code / plists |
| Tests added | 11 (auth Phase 0) — 11 pass |
| Production routes touched | **0** |
| `paper_trader.py` / `main.py` / gate files touched | **0** |
| Service restarts | **0** |
| Halt mutations | **0** |
| `_EXECUTION_ENABLED` flips | **0** (all 4 still True, untouched) |
| `SPREAD_CANNIBALIZATION_GUARD_ENABLED` flips | **0** (still True) |
| `DROP TABLE` calls | **0** |

---

## 4. Surprises / new findings

### HM-AY-β was a one-line bug, not a design flaw

The Phase 1 finding "registry endpoint doesn't return digests" was wrong
in spirit: the registry **does** return digests, but on a non-standard
header `ollama-content-digest` rather than the OCI-standard
`Docker-Content-Digest`. The original probe read the standard header,
got `None`, and silently set `update_available=False` for every installed
model. Fix: HEAD request + read `ollama-content-digest` (with
`Docker-Content-Digest` retained as a defensive fallback). Verified
across 6 models — all return correct 12-char digests matching locals.

### UNCERTAIN bucket was bigger than ~17

The Captain's brief estimated ~17 UNCERTAIN tables; the actual count was
**24**. The split fell more favorably than expected:

- **6 → TRULY DEAD** (no writer anywhere): `gemini_failover`, `news_impact`,
  `short_watchlist`, `strategy_optimization`, `strategy_scores`,
  `trade_explanations`. These promote into the existing TRULY DEAD bucket
  and become candidates for the Batch 1 DROP migration once Admiral
  greenlights it.
- **1 → WIRED-NEVER-FIRED**: `orcl_gex_alerts` (writer in
  `engine/orcl_gex_alerts.py`, but the module is imported by zero active
  code — already flagged in Kill List Section F).
- **16 → ACTIVE-but-empty**: tables on live runtime paths that simply
  haven't received their first write yet (cold features, gated paths,
  newly-added schemas). Removed from the audit — they need feature-
  readiness reviews per their owning module, not DROP planning.

### `dashboard/auth.py` env names diverge from `DASHBOARD_AUTH_PLAN.md`

The Phase plan doc uses `ADMIN_TOTP_SECRET`, `ADMIN_BEARER`, and a
file-based `~/.ollietrades-recovery` raw-token file. The Captain's Phase 0
spec for this sprint changed to `OLLIETRADES_TOTP_SECRET`,
`OLLIETRADES_SERVICE_TOKEN`, `OLLIETRADES_RECOVERY_KEY_HASH` (a hash, not
the raw key). Implementation followed the Captain's spec — the runbook
(`docs/AUTH_SETUP.md`) aligns with the new names. If Phase 1 wiring
later needs to reference these, use the `OLLIETRADES_*` names. The plan
doc Section 3 code block is now stale on the env-var names but the
overall pattern is unchanged.

### One transient ntfy bug found + fixed in flight

The Phase 1 model-watcher first real-run failed its ntfy POST with
`'latin-1' codec can't encode character '—'` because the title
contained an em-dash. HTTP headers are latin-1; the fix added
`title.encode("ascii", "replace")` to the ntfy helper. Already shipped
under `e4d1bf4` in the previous sprint — noted here so it's documented.

### Trader venv was missing `pytest`

Phase 0 tests required pytest in the venv that has fastapi + pyotp
(`/Users/bigmac/autonomous-trader/venv/`, python 3.9). The host python
3.14 has pytest but no fastapi. Pytest 8.4.2 was installed into the
trader venv to run the suite. The trader runtime itself never imports
pytest, so this is a dev-only addition with no production impact.

---

## 5. What's still outstanding for Admiral go

These are not blocking the push — they are the ledger of what's queued
behind explicit Admiral decisions.

### Auth Phase 1+
- Generate the three secrets per `docs/AUTH_SETUP.md`
- Phase 1 wiring: `+1 line per route × ~50 mutating routes` in
  `dashboard/app.py` (`Depends(verify_admin_token)`). The helper +
  tests + runbook are all ready.

### DEAD_TABLES Batch 1 DROP
- Post-finalization, the TRULY DEAD bucket is now **14 tables** (8
  original + 6 newly classified). Pre-DROP checklist still requires:
  - Backup verified off-host
  - Migration script reviewed
  - Each table cross-checked against staging restore
- WIRED-NEVER-FIRED bucket is **5 tables** — investigate why each
  writer never fires; do not drop.
- The 16 ACTIVE-but-empty tables are removed from audit and need
  feature-readiness reviews, not DROP planning.

### Sniper Mode closure (Saturday)
- `docs/SNIPER_MODE_CLOSURE_PLAN.md` (now tracked under `6d41f58`) is
  a Saturday-window action — still gated on Admiral.

### Phase 1 model watcher follow-ups
- Layer 2 (Ollama finance watchlist) probes 5 names that aren't in the
  public Ollama library (`plutus`, `finma`, `fingpt`, `llama-finance`,
  `finance-llm`). Each correctly reports `(not found)`. If any of these
  ever publishes to the public registry, they'll start producing real
  digests automatically — no code change needed. If not, consider
  swapping these for actually-published finance models or simply drop
  the layer.
- `AI4Finance-Foundation/FinMem-LLM-StockTrading` and `hkust-nlp/PIXIU`
  return HTTP 404 on `releases/latest` — they have never cut tagged
  GitHub releases. Each weekly report flags the 404 cleanly; consider
  swapping these for repos that do tag, or accept the noise.

### Plist tracking expansion
- `infra/launchd/` covers the 3 plists added under HM-AY-α series.
  Roughly 20+ other `com.ollietrades.*` and the trader plist itself
  remain untracked. Bringing the rest in is a future sprint.

### Open items unchanged from prior carry-forward
- iv_history Day-N verification cadence (Daily 9:45 MST)
- `/api/wheel/status` intermittent 500 (`dashboard/app.py:7592`)
- Chrome extension Profile 5 reinstall check
- Alert ACK hygiene
- Ghost scorecard calibration via `/api/signals/scorecard`
- Bridge_votes collection stalled 2026-05-01 13:01 (per `2026-05-03 reconciliation`)

---

## 6. Push readiness

All 6 commits ahead of `origin/main`. No untracked production code, no
modified gate / strategy / `paper_trader.py` files, no secrets in any
diff. Push authorized in the Captain's brief — proceeding under Task 7.
