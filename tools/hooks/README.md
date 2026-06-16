# tools/hooks — Claude Code hook REBUILD SEED (HM-VAULT)

**These files are a rebuild seed, NOT the live hooks.** The ACTIVE, authoritative
copies of `block-live-trading.sh` and `statusline.sh` live in `~/.claude/hooks/`
and `~/.claude/` respectively, registered in `~/.claude/settings.json`.
`prime-groundtruth.sh` is the exception: its live copy is in-repo at
`.claude/hooks/prime-groundtruth.sh`; the seed copy here has been removed (orphan).
This directory exists so the hooks are recoverable after a box wipe on the road
(they live under `~/.claude`, which is outside this repo and not otherwise backed up).

> Interim measure. Phase 5 plugin work will make hook distribution single-source;
> until then this is a versioned copy you restore by hand.

## What's here

| File | Role |
|------|------|
| `block-live-trading.sh` | PreToolUse guard (HM-SHIELDS). Blocks live-order tool calls. Seed copy. |
| ~~`prime-groundtruth.sh`~~ | Seed copy removed (orphan). Live copy is `.claude/hooks/prime-groundtruth.sh` in-repo. |
| `statusline.sh` | HM-GAUGE colorblind-safe statusline (blue/amber, numbers always shown). Seed copy. |
| `settings.snapshot.json` | FULL snapshot of `~/.claude/settings.json` (permissions, hooks, model, theme, statusLine). No secrets. Restore wholesale. |
| `settings-hooks.snippet.json` | Just the `hooks` block — granular merge target if you only want hooks (subset of the full snapshot). No secrets. |

`rtk-rewrite.sh` (the RTK command proxy) is also registered in the snippet but is
**RTK-managed** — it's installed/updated by `rtk`, not vendored here. Its checksum
lives in `~/.claude/hooks/.rtk-hook.sha256`.

## Restore after a box wipe

> **⚠ PROVEN by dry-run 2026-06-15.** Steps below reflect what ACTUALLY worked
> (or failed) in an isolated `/tmp/rebuild-test` simulation.

> **Path dependency (hard requirement):** both `prime-groundtruth.sh` (its
> `DB=` line) and the `settings.json` SessionStart command hard-code
> `~/autonomous-trader`. The repo MUST be cloned to exactly `~/autonomous-trader`.
> Cloning to any other path breaks prime silently (DB not found → graceful exit 0)
> and would require updating both the script and the hook command manually.

> **DB dependency:** `data/trader.db` is gitignored. A fresh clone has NO database.
> `prime-groundtruth.sh` detects this gracefully and exits 0 with no output —
> it will NOT error. But to get a live ground-truth primer at session start, you
> must restore the DB from backup (step 4 below).

```bash
# 0. Clone to EXACTLY ~/autonomous-trader (hard path requirement — see above)
git clone git@github.com:accessapple2/BigMac.git ~/autonomous-trader
cd ~/autonomous-trader

# 1. Copy the hook scripts back into the active location
mkdir -p ~/.claude/hooks
cp tools/hooks/block-live-trading.sh ~/.claude/hooks/
chmod +x ~/.claude/hooks/block-live-trading.sh
# prime-groundtruth.sh: lives in-repo at .claude/hooks/ — already present after clone.
chmod +x .claude/hooks/prime-groundtruth.sh

# 1b. Restore the colorblind-safe statusline (lives at ~/.claude/statusline.sh, not in hooks/)
cp tools/hooks/statusline.sh ~/.claude/statusline.sh
chmod +x ~/.claude/statusline.sh

# 2. Restore settings.json. Simplest = wholesale copy of the full snapshot:
#      cp tools/hooks/settings.snapshot.json ~/.claude/settings.json
#    (this brings back permissions, hooks, model:sonnet, theme, statusLine in one shot).
#    GRANULAR alternative — if ~/.claude/settings.json already exists and you only want
#    the hooks: merge the `hooks` block from settings-hooks.snippet.json (drop `_comment`),
#    and add the statusLine key: "statusLine": { "type": "command", "command": "~/.claude/statusline.sh" }
#    NOTE: model + outputStyle need a NEW session to take effect (permissions/hooks reload live).

# 3. Re-install rtk (provides rtk-rewrite.sh) per the RTK setup, then verify:
shasum -a 256 ~/.claude/hooks/block-live-trading.sh .claude/hooks/prime-groundtruth.sh
#    compare against the checksums below.

# 4. Restore data/trader.db from backup (REQUIRED for prime-groundtruth.sh to emit live state)
#    Without this step the harness works but prime exits silently (graceful no-op, not an error).
#    Sources: Time Machine snapshot of ~/autonomous-trader/data/, or external backup.
mkdir -p data
cp /path/to/backup/trader.db data/trader.db   # adjust path
```

## Active-hook checksums (drift detection)

Recorded 2026-06-15 against the ACTIVE copies in `~/.claude/hooks/`. If a future
`shasum -a 256` of the active hook differs from BOTH the value here and the
vendored copy, the seed has drifted from the live hook — re-vendor (`cp` active →
`tools/hooks/`) and update this table.

| Hook | sha256 (active == vendored, 2026-06-15) |
|------|-----------------------------------------|
| `block-live-trading.sh` | `67458a257a0de04a92c4786854a74ccdab7e5ba118a8730514bc6f6b7ce1672d` |
| `.claude/hooks/prime-groundtruth.sh` (in-repo, no seed copy) | `4209e685c18f3ec880248370a994d223be8700ec940f82c9f5a0099cdfe5002a` |
| `statusline.sh` (at `~/.claude/statusline.sh`) | `9b7b36ff6d603871f02f28233036f822cb8378134e4cc452d96c234db59408cb` |
| `settings.snapshot.json` (at `~/.claude/settings.json`) | `5ab552c11c0d06b34c0b1c854dc5139ee3cda49bce2cff45840d67850f21c674` |

> `settings.json` drifts more often than the hooks (any permission/model/theme change). When you
> change it intentionally, re-snapshot (`cp ~/.claude/settings.json tools/hooks/settings.snapshot.json`)
> and update this hash — a mismatch here just means "seed is older than active," expected after edits.

Verify seed integrity at any time:

```bash
cd ~/autonomous-trader
# block-live-trading.sh: seed copy vs live copy
shasum -a 256 tools/hooks/block-live-trading.sh ~/.claude/hooks/block-live-trading.sh
# prime-groundtruth.sh: in-repo copy (no seed copy — it is the canonical source)
shasum -a 256 .claude/hooks/prime-groundtruth.sh
```
