# reboot-lifecycle.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

### Scheduler-owned jobs — keep parallel launchd plists archived

When a job is registered in the in-process `schedule` library inside `main.py`
(Riker XO at `main.py:4226`, the daemons at module scope), a parallel
`~/Library/LaunchAgents/com.ollietrades.*-cron.plist` that hits the same
endpoint is a **double-fire footgun**. The in-process scheduler tick is the
authoritative path; the launchd cron only causes contention (duplicate
signal-center hits, duplicate NTFY, duplicate DB writes).

**Audit pattern when in doubt:** for any
`~/Library/LaunchAgents/com.ollietrades.*-cron.plist`, grep `main.py` for the
equivalent `schedule.every(...).do(<job>)` registration. If both exist,
archive the plist — `main.py` wins.

**Schwab watcher launchd→cron migration (2026-05-28, HM-SCHWAB-WATCHER-CRON).**
The Schwab CSV watcher (`scripts/schwab_csv_watcher.sh`, scans `inbox/` every
60s) and its 48h staleness alarm (`scripts/schwab_cadence_check.py`, daily
06:30) ran ONLY via launchd plists (`com.ollietrades.schwab-{watcher,cadence}`).
Those plists do NOT auto-load at boot here — the recurring **`launchctl
bootstrap gui/$UID` "Domain does not support specified action" over SSH +
RunAtLoad-needs-Aqua-session** failure mode (see "LaunchAgent Reboot Lifecycle"
above). Both went silent after a reboot; the real-world portfolio pipeline
froze 2026-05-23→05-28 undetected. **Fix:** migrated both to crontab
(`* * * * *` watcher, `30 6 * * *` cadence) — cron survives reboot here, same
as the trader/signal-center/cloudflared `@reboot` wrappers. Plists archived to
`archive/launchagents_2026-05-28/`; cron is now the SOLE trigger. **Rule: any
service that must survive reboot belongs on cron, not a bare launchd plist, on
this box.**
