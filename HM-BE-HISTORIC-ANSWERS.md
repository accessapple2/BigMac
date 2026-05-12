# HM-BE-historic — Captain Answers

## Approved: Phase 1 verification

Verify model-string semantics in next session:
- grep engine/super_backtest_v4.py for .get("model") and ["model"]
- grep engine/proving_ground.py and engine/weekend_backtest.py for qwen3-14b-pro id consumers
- Determine: metadata-only OR load-bearing
- Discovery report only — no code changes Phase 1

## Phase 2 + Phase 3: combined session, post Phase 1 sign-off

Order (atomic, backup-checkpointed):
1. cp data/trader.db backups/trader.db.pre-HM-BE-historic.$(date +%Y%m%d)
2. Phase 2 code rename in super_backtest_v4.py (4 string replaces, anchor # === HM-BE-historic ===)
3. launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
4. Verify pgrep -af main.py shows new PID, port 8080 bound, no [red]
5. Phase 3 DB UPDATEs:
   UPDATE ai_players SET display_name = 'Dalio Macro 8B' WHERE id = 'qwen3-14b-pro';
   UPDATE ai_players SET display_name = 'Gemini 2.5 Pro' WHERE id = 'gemini-2.5-pro';
6. Verify dashboard renders correct display names
7. git push

## If Phase 1 finds load-bearing strings

- Rename to truth anyway (qwen3:8b is canonical per config.py:166)
- Add CLAUDE.md note: 'qwen3-14b-pro pre-2026-04-20 OOS ran against 14b; post-swap runs against 8b. Historic numbers not directly comparable.'
- Do NOT re-run OOS — qwen3-14b-pro is not the alpha producer; ollama-coder is

## Canonical display_name for gemini-2.5-pro

'Gemini 2.5 Pro'
