# HM-BL.E2 — Captain Answers

## Decision: Option A only

- Add canary to scripts/health_check.py
- Query: SELECT COUNT(*) FROM positions WHERE qty = 0 OR qty IS NULL
- NTFY topic: ollietrades-admin
- Trigger: stale > 0 (silent when clean, no daily 0-row pings)
- Anchor: # === HM-BL.E2 ===

## Skip
- Option B (defensive UPDATE→DELETE rewrite) — defer until canary fires
- Option C (one-shot script) — defer

## Ship
- Phase 1 only in next session
- Self-verify by manually inserting test 0-qty row, confirm NTFY fires, DELETE the test row
- Paste-back: green + screenshot of NTFY, then close HM-BL.E2
