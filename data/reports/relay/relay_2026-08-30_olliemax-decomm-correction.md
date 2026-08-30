# Relay — 2026-08-30 — correction: olliemax is decommissioned, not down

## Context

Corrects `relay_2026-08-30_recall-refresh-enable-blocked.md` (same night,
earlier). That report read a real connection failure
(`[Errno 65] No route to host` against Ollie Max, 192.168.1.168) as "Ollie
Max is down, will resume when it's back." **Wrong.** Ollie Max was
decommissioned 2026-07 and is never coming back — already documented in
`e7c3e7d` (2026-08-29, "consolidate routing to local com.ollama.serve,
retire olliemax"). Admiral caught the error and ordered: don't wait for
Ollie Max, repoint recall_refresh off it, widen the stale-ref sweep to the
config/env layer of everything revived tonight, and verify (not assume)
the fleet's actual LLM routing before repeating any claim about it.

## What was done

1. **Repointed recall's embedding target.** Found where it evaded the
   `e7c3e7d` sweep: `engine/setup_similarity_signal.py:56`
   (`OLLAMA_EMBED_URL`, hardcoded default) and, found in the same pass,
   `scripts/recall_bakeoff.py:10` (one-time bake-off script, not in the
   live call chain, fixed anyway for consistency). Both repointed to
   `127.0.0.1:11434`, same `OLLAMA_URL`-env-default convention as
   everywhere else e7c3e7d already fixed. Pulled `bge-m3` locally (wasn't
   present) — confirmed embedding-only (`family: bert`,
   `capabilities: ["embedding"]`), can't double as a second chat model
   alongside the qwen3:8b alias set.
2. **Re-ran the catch-up, verified live.** 3 runs: run 1 embedded 3 rows
   (252→255), runs 2–3 stable at 0 new. **Final count: 255.** Found (not
   assumed) that the original "~3-run full catch-up" estimate was wrong for
   a different reason than the outage: `recall_refresh.py` always considers
   the 500 most-recent-by-date closed trades, not "the next 500 unembedded"
   — the remaining ~1,340-row gap is older than that window and is
   structurally unreachable by normal incremental runs, not a backlog still
   draining. Flagged as a separate decision (a deliberate one-time wider
   sweep, e.g. temporary `RECALL_N_CORPUS` override) if the Admiral wants it
   closed — not done here, out of scope of tonight's ask.
3. **Widened the sweep.** Checked config/env, not just script text, for all
   6 scripts revived tonight and every module they import — clean.
   `origin_healthcheck.sh`'s full host list is 100% localhost, no olliemax
   entry that would alert forever.
4. **Verified the fleet-LLM-backend claim instead of repeating it.** Built
   the real provider list via `engine.agent_routing.build_all_providers()`
   against the live DB (the exact function `main.py` calls at startup). All
   5 active LLM-routed seats resolve to `http://localhost:11434`. **The
   earlier "check Ollie Max before Monday, it might be the fleet's LLM
   backend" flag was wrong and is retracted** — the live fleet was never
   routed through olliemax.

## Corrected artifacts

`docs/XO_BACKLOG.md` (`HM-QUIETDOWN-FINAL-DISPOSITIONS`, recall_refresh_run.sh
entry) and the live crontab comment for that job both now say "repointed off
decommissioned olliemax" with the verified 255 count, not "waiting for Ollie
Max."

## Not done this pass

Did not attempt to close the remaining ~1,340-row embedding gap (would need
a deliberate wider one-time sweep, separate decision). Did not investigate
whether other, non-tonight-revived parts of the codebase have similar
evaded-the-sweep olliemax references — this pass was scoped to the 6+1
scripts from tonight's batch plus their import chains, not a repo-wide
re-audit.
