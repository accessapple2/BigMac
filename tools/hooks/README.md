# tools/hooks — Claude Code hook REBUILD SEED (HM-VAULT)

**These files are a rebuild seed, NOT the live hooks.** The ACTIVE, authoritative
copies live in `~/.claude/hooks/` and are registered in `~/.claude/settings.json`.
This directory exists so the hooks are recoverable after a box wipe on the road
(they live under `~/.claude`, which is outside this repo and not otherwise backed up).

> Interim measure. Phase 5 plugin work will make hook distribution single-source;
> until then this is a versioned copy you restore by hand.

## What's here

| File | Role |
|------|------|
| `block-live-trading.sh` | PreToolUse guard (HM-SHIELDS). Blocks live-order tool calls. Seed copy. |
| `prime-groundtruth.sh` | SessionStart primer. Seed copy. |
| `statusline.sh` | HM-GAUGE colorblind-safe statusline (blue/amber, numbers always shown). Seed copy. |
| `settings-hooks.snippet.json` | The `hooks` block from `~/.claude/settings.json`. Merge target on restore. No secrets. |

`rtk-rewrite.sh` (the RTK command proxy) is also registered in the snippet but is
**RTK-managed** — it's installed/updated by `rtk`, not vendored here. Its checksum
lives in `~/.claude/hooks/.rtk-hook.sha256`.

## Restore after a box wipe

```bash
# 1. Copy the hook scripts back into the active location
mkdir -p ~/.claude/hooks
cp tools/hooks/block-live-trading.sh ~/.claude/hooks/
cp tools/hooks/prime-groundtruth.sh ~/.claude/hooks/
chmod +x ~/.claude/hooks/*.sh

# 1b. Restore the colorblind-safe statusline (lives at ~/.claude/statusline.sh, not in hooks/)
cp tools/hooks/statusline.sh ~/.claude/statusline.sh
chmod +x ~/.claude/statusline.sh

# 2. Merge the `hooks` block from settings-hooks.snippet.json into
#    ~/.claude/settings.json (drop the `_comment` key). If settings.json doesn't
#    exist yet, create it with at least the `hooks` object from the snippet.
#    Also add the statusLine registration (top-level key):
#      "statusLine": { "type": "command", "command": "~/.claude/statusline.sh" }

# 3. Re-install rtk (provides rtk-rewrite.sh) per the RTK setup, then verify:
shasum -a 256 ~/.claude/hooks/block-live-trading.sh ~/.claude/hooks/prime-groundtruth.sh
#    compare against the checksums below.
```

## Active-hook checksums (drift detection)

Recorded 2026-06-15 against the ACTIVE copies in `~/.claude/hooks/`. If a future
`shasum -a 256` of the active hook differs from BOTH the value here and the
vendored copy, the seed has drifted from the live hook — re-vendor (`cp` active →
`tools/hooks/`) and update this table.

| Hook | sha256 (active == vendored, 2026-06-15) |
|------|-----------------------------------------|
| `block-live-trading.sh` | `67458a257a0de04a92c4786854a74ccdab7e5ba118a8730514bc6f6b7ce1672d` |
| `prime-groundtruth.sh` | `4209e685c18f3ec880248370a994d223be8700ec940f82c9f5a0099cdfe5002a` |
| `statusline.sh` (at `~/.claude/statusline.sh`) | `9b7b36ff6d603871f02f28233036f822cb8378134e4cc452d96c234db59408cb` |

Verify seed integrity at any time:

```bash
cd ~/autonomous-trader
shasum -a 256 tools/hooks/block-live-trading.sh tools/hooks/prime-groundtruth.sh
```
