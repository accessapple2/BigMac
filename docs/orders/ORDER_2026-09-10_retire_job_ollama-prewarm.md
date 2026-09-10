# TOMBSTONE — ollama-prewarm (job)

**Retired:** 2026-09-10
**Reason:** Parked dark since the 2026-07-22 trip-quietdown stand-down
(`scripts/ollama_prewarm.sh` renamed `.quietdown-disabled-2026-07-22`,
crontab line `45 6 * * 1-5 ... ollama_prewarm.sh` commented out) pending
revisit "after the 2026-09-04 qwen3:8b un-aliasing; `OLLAMA_KEEP_ALIVE=-1`
(set 08-27) may have already mooted the cold-start failure mode it exists
to prevent" (`docs/XO_BACKLOG.md:10746-10748`,
`data/reports/relay/relay_2026-08-30_revive-retire-batch.md:39`). Confirmed
superseded: the olliemax hardware migration shipped with
`OLLAMA_KEEP_ALIVE=-1` live on the current inference host — the shared
model no longer idle-unloads at all, which structurally eliminates the
~06:51 AZ cold-start-wedge pattern this script existed to pre-empt.

Independently confirmed stale even setting the KEEP_ALIVE question aside:
the script hardcodes `HOST="http://192.168.1.168:11434"` — the pre-
migration Ollie Max address. Live inference now routes through
`config.OLLAMA_URL` / `.env`'s `OLLAMA_URL=http://100.95.195.20:11434`
(Tailscale). Reviving the script as written would prewarm an address that
is no longer the inference host at all, not just a redundant no-op.

**Confirmed nothing depends on it before retiring:**
- Crontab line already commented (not live).
- No launchd plist exists for this job (`find /Library/LaunchDaemons
  ~/Library/LaunchAgents -iname "*prewarm*"` — zero results) — this was
  always a plain cron job, never launchd.
- `grep -rl ollama_warmup` (its log file name) across the repo — no other
  script reads `logs/ollama_warmup.log` or `ollama_warmup_cron.log`.
- Its only external effect besides the log write was an NTFY push to
  `ollietrades-admin` on failure only — nothing consumes that as a signal
  of success (no downstream healthcheck keyed on its presence).

**Backfilled via manual ledger row, not `scripts/fleet_lifecycle.py`:**
the tool's `_apply_job_change()` only handles launchd targets
(`launchctl enable/disable` against a resolved `.plist`) — it has no cron
code path at all (confirmed: zero "cron" handling in
`scripts/fleet_lifecycle.py` outside its own doctrine-comment header).
Running `retire --type job` for real against a cron-only target raises
`RuntimeError: no plist found` and aborts with no ledger row written, by
the tool's own "refuses to do partial work" design. Same shape as ledger
row 26 (`job|crew|retire`, backfilled) — a cron/launchd-orphan case the
tool structurally can't execute, recorded by hand instead. **Flagging
this as a real gap**, not routed around silently: `fleet_lifecycle.py`'s
own doctrine claims coverage for every "agent/job state change," but ~40
cron-based scripts in this repo (`crontab -l`) have no first-class
lifecycle path today — only launchd jobs do. Worth a backlog item if the
doctrine is meant to cover cron too.

This is permanent under current criteria. No resume-by date — revival
requires a new explicit `revive` order (and, if ever revived, the script
itself needs its hardcoded host updated to the live `OLLAMA_URL` first —
reviving it unmodified would silently target a dead address).
