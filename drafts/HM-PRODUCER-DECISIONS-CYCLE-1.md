Admiral picks path. Scotty executes ONLY after explicit go.

══════════════════════════════════════════════════════════
PHASE 4 — SHIP THE DECISION
══════════════════════════════════════════════════════════

Per Admiral decision, one of:

PATH 1 — REVIVE
  - Verify model availability (Ollama pull if needed)
  - Restart producer process (launchctl kickstart)
  - Verify signals start landing within 30 min
  - Commit message: "HM-UHURA-PRODUCER-REVIVAL: re-enable 
                     uhura-llama after [N] days idle."
  - Update CLAUDE.md zombie set: remove Uhura
  - Soak 7 days before declaring success

PATH 2 — RETIRE
  - Set halt='full', halt_reason='retired-fleet-composition'
  - Update CLAUDE.md zombie set: confirm Uhura
  - Update Bridge Consensus renderer to hide Uhura column 
    instead of showing it perpetually neutral
  - Commit message: "HM-UHURA-PRODUCER-RETIRE: permanent 
                     halt, render-side dimout."

PATH 3 — FIX AGGREGATOR (Class C)
  - Bank HM-CONSENSUS-AGGREGATOR-INTAKE-BUG as separate epic
  - This cycle closes with documentation only
  - No producer state change

PATH 4 — UPDATE RENDERER (Class A)
  - Frontend annotation: "Uhura halted by design" badge
  - One ship, frontend only
  - Producer state unchanged

══════════════════════════════════════════════════════════
GUARDRAILS — APPLY TO EVERY PRODUCER CYCLE
══════════════════════════════════════════════════════════

- NEVER modify ai_players.halt without Admiral explicit go.
- NEVER restart a halted producer without Phase 3 brief.
- Sacred DB rule: AUDIT READS ONLY in Phase 1.
- NEVER DELETE/DROP/TRUNCATE producer rows — retirement is 
  halt='full' + halt_reason, never removal.
- If Phase 2 reveals the producer is actually IMPORTANT to 
  a downstream consumer (Battle Station, Counselor Troi, 
  etc.), bank that as a SEPARATE epic. Don't try to retire 
  something with downstream dependents.
- If Admiral asks "what's the right call?" — present the 
  matrix honestly with trade-offs, don't push a preferred 
  answer.

══════════════════════════════════════════════════════════
TARGET ROSTER (use this template for each)
══════════════════════════════════════════════════════════

Cycle 1: Uhura (this draft)
Cycle 2: Data (Lt. Cmdr. Data, model: devstral-small-2 or
         similar — verify in audit)
Cycle 3: Bull-Bear Consensus producer(s) (0/28 models)
Cycle 4: Plutus-Decider (decision already on Saturday May 24 
         roll-up agenda per memory)
Cycle 5+: Any new producer review as bank tickets surface

══════════════════════════════════════════════════════════
EXECUTION ORDER FOR FIRST FIRE
══════════════════════════════════════════════════════════

1. Run Phase 1 (audit) — ~20 min
2. Run Phase 2 (classify) — ~10 min  
3. Produce Phase 3 brief — ~5 min
4. STOP. Await Admiral decision.
5. Execute Phase 4 path per Admiral go.
6. Commit with chosen path's commit message.
7. Update BANKED_ITEMS file: close 
   HM-UHURA-PRODUCER-REVIVAL-DECISION with path taken.

END HM-PRODUCER-DECISIONS-CYCLE-1
