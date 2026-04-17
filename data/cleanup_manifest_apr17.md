# Cleanup Manifest — April 17, 2026

## Archive Location
`~/bigmac_migration_retired/20260416/`

## Files Moved to Archive (retired orphans)

| File | Size | Reason |
|------|-----:|--------|
| `~/sync_portfolio.py` | 5.3KB | Orphaned Webull→paper-trader bridge (predecessor system). No references in autonomous-trader. Last modified 2026-03-08. |
| `~/trader.py` | 9.7KB | Original gemma3:4b standalone paper trader. Predecessor to autonomous-trader. Writes to ~/trades.db only. |
| `~/trades.db` | 24KB | Stub DB for trader.py. Seed cash $10k, zero trades, zero positions. Never traded. |
| `~/file_inventory.txt` | 64KB | Investigation snapshot copied for reference. |

**Space reclaimed from home root:** ~40KB (files were small; bulk of space in paper-trader log)

## Files Truncated (live processes — file handles preserved)

| File | Before | After | Method |
|------|-------:|------:|--------|
| `~/paper-trader/server.log` | **181MB** | 0B | `: >` (truncate in place, handle valid) |

**Tail archived:** `~/bigmac_migration_retired/20260416/paper-trader-server-tail-5000.log` (352KB)

**Space reclaimed from paper-trader:** ~181MB

## Total Space Reclaimed
~181MB (dominated by paper-trader log)

## Files Skipped — Pending Admiral Review

| File | Last Modified | Assessment |
|------|--------------|------------|
| `~/start-trademinds.sh` | 2026-03-29 | Stale launcher (uses nohup + ngrok; superseded by launchd + cloudflared). Safe to archive. **Admiral: OK to move?** |
| `~/cloudflared.log` | 2026-04-07 | Active tunnel log. Last session Apr 7. Keep until cloudflared is reconfigured on G1. |

## Log Rotation — Pending
`~/paper-trader/server.log` has no newsyslog or logrotate entry.
Follow-up: add to `/etc/newsyslog.conf` or create a launchd-based rotation.
Suggested rotation: 50MB max, keep 3 archives.

## New Infrastructure Added This Session

| File | Purpose |
|------|---------|
| `engine/metals_sync.py` | Syncs metals_ledger → portfolio_positions with live GC=F / SI=F prices |
| `~/Library/LaunchAgents/com.trademinds.webull-sync.plist` | Auto-syncs Webull positions at 6:00 AM + 1:05 PM AZ |
| `~/Library/LaunchAgents/com.trademinds.metals-sync.plist` | Auto-syncs metals prices at 6:15 AM + 1:10 PM AZ |
