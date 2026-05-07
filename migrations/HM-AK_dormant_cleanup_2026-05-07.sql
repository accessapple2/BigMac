-- HM-AK 2026-05-07 — Fleet roster cleanup: halt 12 dormant agents.
--
-- Diagnosis (per docs/OPS_LOG.md 2026-05-07 entry): 12 ai_players rows had
-- halt_mode='active' AND is_active=1 but zero trades, signals (or fixed-pool
-- 25-sig bootstrap), and zero war_room posts in the last 7 days. Six are
-- paid-API zombies that should have been benched under the Free-Models-First
-- doctrine (CLAUDE.md, 2026-04-13). Six are dormant Ollama agents (likely
-- scaffolded but never promoted).
--
-- Pre-state (2026-05-07 ~10:30 MST):
--   halt_mode='active'    : 37 rows
--   halt_mode='full'      :  9 rows
--   halt_mode='exit_only' :  4 rows
--
-- Post-state expected:
--   halt_mode='active'    : 25 rows  (37 - 12)
--   halt_mode='full'      : 20 rows  ( 9 + 11)
--   halt_mode='exit_only' :  5 rows  ( 4 +  1)
--
-- gemini-2.5-flash has 2 open positions — uses 'exit_only' to allow close-out.
-- All other 11 agents have zero open positions — safe to halt 'full'.
--
-- Rollback (if needed):
--   UPDATE ai_players
--      SET halt_mode='active', halted_at=NULL, halt_reason=NULL
--    WHERE halt_reason LIKE 'HM-AK 2026-05-07%';

BEGIN;

-- 11 zombies with no positions: halt_mode='full'
UPDATE ai_players
   SET halt_mode = 'full',
       halted_at = CURRENT_TIMESTAMP,
       halt_reason = 'HM-AK 2026-05-07 dormant cleanup (no activity 7d; see OPS_LOG)'
 WHERE id IN (
     'claude-haiku',
     'claude-sonnet',
     'gpt-4o',
     'gpt-o3',
     'grok-4',
     'qwen-coder-haiku',
     'qwen3-14b-grok3',
     'qwen3-8b-4o',
     'qwen3-8b-o3',
     'ollama-glm4',
     'ollama-gemma27b'
 );

-- gemini-2.5-flash: 2 open positions, exit_only allows close-out
UPDATE ai_players
   SET halt_mode = 'exit_only',
       halted_at = CURRENT_TIMESTAMP,
       halt_reason = 'HM-AK 2026-05-07 dormant cleanup (2 open positions, exit_only for close-out)'
 WHERE id = 'gemini-2.5-flash';

COMMIT;

-- Verification:
-- SELECT halt_mode, COUNT(*) FROM ai_players GROUP BY halt_mode;
-- SELECT id, halt_mode, halt_reason FROM ai_players WHERE halt_reason LIKE 'HM-AK%' ORDER BY id;
