# 🧹 SCOTTY — Working-tree cleanup
### Single-phase housekeeping, no service touch

> Captain's orders: Working tree has 40+ untracked files from yesterday's 11-epic chain. Sweep them into proper homes: directives → archive, scotty reports → keep tracked (audit trail), sacred-DB backups → archive, .gitignore patterns for future. Then commit.

## Pre-flight

```bash
cd ~/autonomous-trader
git status --short | wc -l | xargs -I {} echo "  {} untracked items"
```

## Plan

1. **HM-*.md directives** → `archive/scotty_directives/` (keep them, just move out of root)
2. **data/scotty_hm_*.md + data/scotty_phase*.md** → keep tracked (audit trail of decisions)
3. **data/*.db, *-shm, *-wal pre-* backups** → add to .gitignore (sacred-DB backups don't belong in git)
4. **data/model_watch_log.jsonl** → add to .gitignore (rotating log)
5. **archive/stubs/, reports/, shared/*.pre-*** → add to .gitignore as transient
6. **backups/main.py.pre-*** → add to .gitignore (file-level snapshots, not source)

## Phase CLEAN.0 — Discovery (NO writes)

```bash
echo "── Full untracked inventory ──"
git status --short

echo ""
echo "── Size of sacred-DB backups (do these belong in git? NO) ──"
ls -lah data/*.pre-* data/*.legacy* 2>/dev/null | awk '{print $5, $9}'

echo ""
echo "── Existing .gitignore (extend, don't replace) ──"
cat .gitignore 2>/dev/null | head -30
```

Show me the inventory + proposed .gitignore additions BEFORE writing anything. HALT.

## Phase CLEAN.1 — Apply (after Captain approves)

- mkdir -p archive/scotty_directives
- mv HM-*.md archive/scotty_directives/
- Extend .gitignore with proposed patterns
- git add archive/scotty_directives/ .gitignore data/scotty_hm_*.md
- Commit: `chore: HM-CLEAN — archive directives, gitignore sacred-DB backups + transients`
- Push inline

## Phase CLEAN.C — Verify

```bash
git status --short
echo "── Should be empty above ──"
git log origin/main --oneline | head -3
```

ntfy: `🧹 HM-CLEAN complete — working tree clean`.
