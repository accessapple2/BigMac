# Relay — 2026-07-21 — OllieTrades Lite: Phase 0-1 scaffold + Ollama fix

## Context

Red Alert Yankee Echo Sierra directive (OllieTrades Lite build) — Captain
scoped this session to **Phase 0-1 prep only**, plus Phase 2 model pulls
opportunistically if disk allowed. Explicitly **no launchd jobs, no engine
scripts, nothing resident/scheduled** this session — the fleet owns bigmac
through tomorrow's close and quiet-down; Lite goes live post-trip on a
future explicit trigger.

## Shipped

- **Ollama fixed.** The Ollama.app *cask* is broken on this Mac — its CLI
  binary hangs inside `dyld_start` (confirmed via `/usr/bin/sample`, never
  reaches `main()`), and `brew reinstall --cask ollama` stalls indefinitely
  at "Backing up App" (reproduced twice, 0% CPU, no progress). Abandoned the
  cask entirely: `brew uninstall --cask ollama --force` (clean) →
  `brew install ollama` (plain formula, headless, no `.app`/no GUI) — worked
  immediately. `ollama --version` → `0.32.1`, `ollama serve` bound to
  `127.0.0.1:11434` on the first try, GPU discovered (Apple M4 Metal, 11.8
  GiB). **Lesson for this box: always use the Homebrew formula, never the
  cask, for Ollama.**
- **`~/ollietrades-lite/` scaffolded** — `engine/`, `reports/`, `logs/`,
  `config/`, `CLAUDE.md` (doctrine: paper-only, never-delete, `trader.db`
  read-only, no Polygon, launchd-only scheduling). Git-initialized,
  committed (`1b06474`).
- **Models pulled** (Phase 2, disk allowed): `qwen3:14b` (9.3GB, ANALYST
  role) and `phi4-mini:3.8b` (2.5GB, SCANNER role). Confirmed via
  `ollama list`. Disk went from ~20GB free → ~12GB free after both pulls —
  healthy remaining margin.
- **Repo pushed.** Created `accessapple2/ollietrades-lite` (private) via
  `gh repo create`, remote set to SSH (matching this repo's convention),
  pushed `main` — `1b06474` now live at
  `github.com:accessapple2/ollietrades-lite`.

## Open / deferred (by design, not oversight)

- **Alpaca paper-key verification: skipped, not done.** Traced the loader
  (`config.py:306-307`, `.env` at `~/autonomous-trader/.env`) but a live
  check against `paper-api.alpaca.markets/v2/account` was blocked outright
  by HM-SHIELDS (`block-live-trading.sh`) — it flags any command referencing
  `.env` as secret-exfil, even a script that only prints boolean/length and
  masked account fields, never the raw key. Did not attempt to route around
  the guard. Captain chose to skip verification this session rather than
  loosen the hook.
- **Phase 3 (engine scripts: scanner.py/analyst.py/report.py) — not
  started.** Out of scope this session.
- **Phase 4 (launchd jobs) — not started, explicitly deferred.** No
  `com.ollietrades.lite.*` plists exist. Go-live is gated on a future
  explicit trigger, not on code/model readiness.

## Live-verification results

- `curl http://localhost:11434/api/version` → `{"version":"0.32.1"}`
- `ollama list` → `qwen3:14b` (9.3GB) + `phi4-mini:3.8b` (2.5GB) both present
- `df -h /` → 12GB free (from ~20GB pre-pull)
- `git -C ~/ollietrades-lite log --oneline` → `1b06474` on `main`, pushed,
  tracking `origin/main`
- `gh repo view accessapple2/ollietrades-lite` reachable, private

No fleet-affecting changes made — all work isolated to `~/ollietrades-lite/`
and the Ollama install (a shared system dependency, now healthier than it
was; the retired `com.ollietrades.ollama-keepalive.plist.retired-20260614`
launchd job was left untouched, not reinstated).
