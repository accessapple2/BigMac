# HM-CLAUDE-MD-TRIM

## Issue
CLAUDE.md is 71.5k chars (> 40.0k threshold). Scotty (Claude Code) warns this
impacts performance — every turn loads the full file into context.

## Fix
- Audit CLAUDE.md sections for staleness
- Archive completed/deprecated sections to docs/CLAUDE-archive-YYYY-MM.md
- Goal: keep CLAUDE.md under 40k chars
- Use `/memory` slash command in Claude Code to inspect/edit interactively

## Priority
Low — quality-of-life for Scotty. Tackle on next session start when context is
fresh.
