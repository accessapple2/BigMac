# Bridge Handoff — Saturday 2026-05-16 Session 1 Source Material

Delivered by Captain on 2026-05-15 evening for Saturday's frontend overhaul.

## Files (read order)

1. **ollietrades-bridge-handoff.md** — Design system spec (14 sections). The master document.
2. **concept-cards-batch-1-reviews.md** — XO + Scotty perspective reviews on all 5 artifacts. Read second for grounding.
3. **CharacterIcons.tsx** — 12 character avatar components (610 LOC TSX). Source for vanilla port.
4. **ConceptGlyphs.tsx** — 14 concept glyph components (~300 LOC TSX). Source for vanilla port.
5. **concept-cards-batch-1.md** — 10 Concept Cards in section-4 five-section shape.
6. **ticker-command-card-spec.md** — Implementation contract for Session 3 (Ticker Card v1). NOT this Saturday's scope.

## Origin

Produced by a parallel Claude-on-Chrome (Opus 4.7) design session during the same window Captain + XO + Scotty shipped 7 PRs to main on the trader/backend side.

## Stack mismatch (key issue)

All TSX files assume React. Bridge production is vanilla HTML/CSS/JS in dashboard/static/index.html (35,530 LOC monolith). Path A (vanilla port) is the approved approach per Scotty's engineering call on 2026-05-15.

## Saturday plan

Per scotty_bridge_overhaul_session1_plan_2026-05-16.md (Scotty drafts tonight):
- PR 1a: Palette reconcile (CSS only, zero risk)
- PR 1b: static/character-icons.js (pure addition, low risk)
- PR 1c: static/concept-glyphs.js (pure addition, low risk)
- PR 1d: Replace avatars in War Room + leaderboard (visible change, deep smoke)

Captain uses Saturday evening + Sunday. Monday 06:00 AZ untouched for Layer 1 validation.
