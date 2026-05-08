# `infra/launchd/` — Tracked LaunchAgent Plists

These plists are copies of the LaunchAgents that drive the OllieTrades ops
fleet on bigmac. The canonical install location is
`~/Library/LaunchAgents/` — these copies exist purely so the schedule + entry
points are reproducible from the repo (disaster recovery, host migration,
review trail).

**Editing one of these files does not change the running schedule.** Edit
the canonical copy under `~/Library/LaunchAgents/` and `launchctl unload &&
launchctl load` to apply, then sync the copy back here in the same commit.

To restore on a fresh host:

```bash
cp infra/launchd/com.ollietrades.<name>.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.ollietrades.<name>.plist
launchctl list | grep <name>   # PID == "-" means scheduled but idle
```

---

## `com.ollietrades.offhost-backup.plist`

**What:** runs `scripts/offhost_backup.sh` — off-host rsync to the Ollie Box,
shipped under HM-AY-α #1.

**Schedule:** daily at **06:30 local** (`StartCalendarInterval`). Does not
run at load.

**Logs:** `logs/offhost_backup.stdout.log`, `logs/offhost_backup.stderr.log`

**Install:**
```bash
launchctl load ~/Library/LaunchAgents/com.ollietrades.offhost-backup.plist
```

---

## `com.ollietrades.model-watcher.plist`

**What:** runs `scripts/model_watcher.py` — finance-focused weekly model
watch (HM-AY-α #6 / SCOTTY 2.6). Three layers: installed-Ollama digests,
Ollama finance watchlist, Hugging Face + GitHub release polling. Reports
only — never auto-pulls.

**Schedule:** weekly **Sunday 09:00 local** (Weekday=0,
Hour=9). Does not run at load.

**Logs:** `logs/model_watcher.log`, `logs/model_watcher.err`

**Install:**
```bash
launchctl load ~/Library/LaunchAgents/com.ollietrades.model-watcher.plist
```

---

## `com.ollietrades.schwab-watcher.plist`

**What:** runs `scripts/schwab_csv_watcher.sh` — Schwab CSV import watcher
(HM-AY-α #3, row-level error handling + delta guard + ntfy on diff).

**Schedule:** every **60 seconds** (`StartInterval=60`), runs at load.
Polls the watch directory for fresh Schwab statement drops.

**Logs:** `logs/schwab_watcher_stdout.log`, `logs/schwab_watcher_stderr.log`

**Install:**
```bash
launchctl load ~/Library/LaunchAgents/com.ollietrades.schwab-watcher.plist
```

---

## What's NOT tracked here (yet)

The host runs many more plists at `~/Library/LaunchAgents/com.ollietrades.*`
(archer-briefing, crusher, danelfin-update, etfregime, fleet-auditor,
ghost-advisor, ghost-trader, iv-backfill, morningbriefing,
nightly-backtest, nightly-regression, ollama-keepalive, ollie-scan,
optionsflow, etc.) plus the trader itself at
`~/Library/LaunchAgents/com.trademinds.trader.plist`. Adding the rest is a
follow-up sprint — this snapshot covers the three plists added under the
HM-AY-α series so the off-host-backup, model-watcher, and Schwab-watcher
work is reproducible from the repo without rummaging through `~/Library`.
