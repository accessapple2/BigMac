# HM-DRIFT-MONITOR — Claims-vs-Disk / Config-vs-Truth Daily Drift Monitor (SPEC ONLY)

**Status:** spec doc only — no code, no schema changes, no execution. **BUILD
gated on Admiral approval post-trip.** Zero code this turn.
**Seed:** `CLAUDE.md` → "Drift Catalog 2026-05-17" (10 drift classes).
**Pre-registered:** probe set + thresholds fixed before first run (same
discipline as `drafts/HM-FORGE-PHASE4-AB-SCORECARD-SPEC.md`).

## 1. Mission
Detect **claims-vs-disk** and **config-vs-truth** drift **daily** — the class of
gap where the documentation/config asserts one thing and the running system /
DB / disk says another. Today's session alone surfaced two live instances:
- **Stale fleet counts** — `CLAUDE.md` says 21 while the DB shows 24/5/47
  (different count definitions never reconciled in the doc).
- **"ARC COMPLETE" overstate** — a Daemon-Graveyard claim of completion that the
  actual cron/process state does not fully support.

A daily read-only sweep catches these in ~24h instead of at the next deep audit.

## 2. The 10 catalog classes → concrete probes

Each row maps a 2026-05-17 Drift Catalog class to a **read-only probe** and the
**diff** it emits. No probe writes, mutates, or restarts anything.

| # | Catalog class (CLAUDE.md) | Probe (read-only) | Drift = WARN when |
|---|---------------------------|-------------------|-------------------|
| 1 | Model assignment silent bypasses (main.py per-call overrides shadow `ai_players.model_id`) | Parse per-call model overrides in `main.py` / `crew_scanner`; diff vs `ai_players.model_id` for each routed player | runtime override ≠ DB `model_id` for any player |
| 2 | Hidden bypasses outside `config.AI_PLAYERS` scope | Enumerate every model-assignment site (grep routing call-sites across `main.py`, `crew_scanner`, `config.AI_PLAYERS`); diff the union vs the documented roster | any assignment site not represented in the doc roster |
| 3 | Role-vs-reality gaps (doc says LLM, reality is rule-based; role blocked by gate) | For each agent, compare documented role vs whether its code path actually issues an LLM call / is gate-blocked | documented mechanism ≠ code-path mechanism |
| 4 | Dead-code gates (`PAID_MODEL_IDS` guard unreachable) | Static reachability check on named guard constants (`PAID_MODEL_IDS`, gate flags) — is the guard on any live call path? | a doctrine guard has zero reachable callers |
| 5 | Fleet-config-vs-reality (model assumed on wrong host) | `ssh olliemax 'ollama list'` (read-only) → diff vs **expected roster** (the 9-model `FLEET` array in `scripts/hm_forge_phase15.sh`) | model present/absent on `.168` ≠ expected; unexpected host placement |
| 6 | Docs-vs-bind reality (claimed network bind ≠ actual `127.0.0.1`) | `lsof -nP -iTCP -sTCP:LISTEN` for 8080/9000 → diff bind addr vs CLAUDE.md "network bindings" claims | claimed bind ≠ observed listener addr |
| 7 | Wrong-engine claims (`debate_engine.py` described as CrewAI) | grep docs for engine descriptions; assert against actual imports (`asyncio`/`aiohttp`, not `crewai`) in the named file | doc names a framework the file does not import |
| 8 | Table-confusion (`war_room_debates` vs `debate_history_v2`) | For each documented `/api/...` endpoint→table claim, diff vs the table the route actually queries | endpoint doc table ≠ queried table; row-count claim off by >X% |
| 9 | "gitignored per convention" — wasn't | `git check-ignore` each path CLAUDE.md claims is gitignored-by-convention | path claimed ignored but `git check-ignore` says tracked |
| 10 | Dormant-code-becomes-production (zero-caller fn wired into hot path) | Track caller-count of flagged dormant functions (e.g. `options_exec.open_options_trade`); flag transitions 0→N | a previously-zero-caller fn is now on a per-cycle path |

**Cross-cutting probes (not 1:1 with a class, but the same drift family):**
- **doc-claims vs DB truth** — fleet counts (`ai_players` by `halt_mode`), gate
  states (the 4 gates' DB/flag values) diffed vs the numbers asserted in
  `CLAUDE.md`. *(Catches today's 21-vs-24/5/47.)*
- **prompt-hash vs versioned prompts** — hash each live prompt template; diff vs
  the committed/versioned prompt to detect un-versioned edits.
- **cron expected-set vs `crontab -l`** — diff the documented expected cron set
  (kirk ×4, off-host backup 20:00, A2 crons, pager/forge one-shots) vs actual
  `crontab -l`; flag missing **and** unexpected/expired-but-present one-shots.
- **doctrine flags: CLAUDE.md vs code constants** — for each doctrine flag named
  in CLAUDE.md, diff its asserted value vs the actual `config.py` / module
  constant.
- **`.sent` sidecar gaps** — for each expected daily artifact (kirk briefings ×4,
  etc.), confirm the `.md` has its `.sent` sidecar; flag generated-but-not-sent
  or expected-but-not-generated.

All probes are **read-only**. `.168` access is read-only `ollama list` over LAN
SSH (no writes — consistent with the no-`.168`-writes constraint).

## 3. Output
- **Daily 16:00 AZ sweep** (after `after_close`, before EOD).
- **One NTFY** to `ollietrades-admin`:
  - **INFO** when the sweep is clean (explicit "0 drift across N probes" — a
    clean run is *stated*, never silent, so a missing NTFY means the sweep
    itself failed).
  - **WARN** with the diff rows (class, probe, claimed, observed) when any probe
    drifts.
- **Dated md ledger:** `data/drift_monitor_<YYYY-MM-DD>.md` — additive, never
  overwritten/deleted (sacred-data rule). Plus a JSON sidecar (see §4).

## 4. Output schema — aligned to kirk R3 sidecar naming

```json
{
  "schema_version": 1,
  "consumer": null,
  "kind": "drift_monitor",
  "date": "<YYYY-MM-DD>",
  "generated_at_az": "<ISO8601 America/Phoenix>",
  "probes_run": 0,
  "drift_count": 0,
  "status": "clean | drift",
  "findings": [
    {
      "class": "<catalog # or cross-cutting name>",
      "probe": "<probe id>",
      "claimed": "<what doc/config asserts>",
      "observed": "<what disk/DB/process shows>",
      "severity": "INFO | WARN"
    }
  ]
}
```

`schema_version` int, `consumer: null` until a reader wires in, snake_case keys,
ISO `*_az` timestamps — identical convention to `kirk_briefing.py` R3 and the
Phase 4 grader.

## 5. Safety posture
- **Read-only** across docs, DB, processes, crontab, and `.168` (`ollama list`
  only) — **zero execution-path touch.** All four gates and **RULE #1
  (Schwab hands-off)** unaffected.
- The monitor **never auto-corrects** drift — it reports. Reconciling a flagged
  drift (editing CLAUDE.md, fixing a bind, retiring dead code) is a separate,
  human-reviewed action. Auto-editing docs from a probe would itself be a new
  drift vector.
- Sacred-data: ledgers/sidecars additive; never overwrite/delete.

## 6. Integration
- **Spec #2 (drift taxonomy):** when #2 ships, each finding's `class` maps onto
  the taxonomy's canonical class ids; this monitor becomes a **producer** into
  the taxonomy rather than carrying its own ad-hoc class strings.
- **Spec #3 (HM-DRIFT-RECON):** #3's empty-`strategy_scores` finding is a
  **doc-claims-vs-DB-truth** instance this monitor would independently surface —
  #4 = daily *config/claims* drift, #3 = weekly *performance* drift. Together
  they cover both halves of "the doc cannot be trusted without re-verifying."

## 7. Known spec anomalies (flagged, not blockers)
1. **Some probes are static-analysis, not a clean query** (classes 1–4, 7, 10).
   These need a small, maintained map of "named guard/override/dormant-fn sites"
   — that map is itself a drift surface and must be reviewed when routing code
   changes. Start with the exact sites the 2026-05-17 catalog already named.
2. **"Expected set" definitions must live somewhere versioned.** The cron
   expected-set, doctrine-flag list, and model roster each need a committed
   source-of-truth file for the probe to diff against; absent that, the probe
   has nothing to compare to. Bootstrapping those expected-sets is build Step 0.
3. **Count-definition mismatch is not always drift.** Today's "21 vs 24/5/47"
   is partly *different count definitions* (total vs active vs by-halt_mode).
   The probe must compare **like-for-like definitions**, or it will WARN on a
   semantic difference that is correct-but-underspecified. Encode the exact
   count definition alongside each claim.

## 8. Out of scope (separate epics)
The drift **taxonomy** (spec #2); auto-remediation of any flagged drift;
historical drift trend analysis; alerting policy beyond the single daily NTFY.
