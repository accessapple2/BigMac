# Scotty — Questions for Admiral (2026-05-10)

These items were skipped because the directive's premise didn't match current repo state. Skipped per Standing Rule #8 (stop on ambiguity).

## Q1 — energy-arnold "parser fix"

**Found:** No parser failures in `logs/trader.log`. The only `energy-arnold` log lines are governance-gated ("MANDATE BLOCKED: energy-arnold is a Bridge Voter" — intentional). DB row `ai_players` shows `model_id=qwen3:8b`; `main.py:104` instantiates `OllamaProvider("energy-arnold", "qwen3:8b", ...)`.

**Only drift detected:** `main.py:225` Tier-2 comment block still says `# Trip Tucker (phi3:mini)` — the model is actually qwen3:8b.

**Question:** Did "energy-arnold parser fix" mean this comment drift, or was there a deeper parser/output-shape bug I missed? If the latter, please point to a specific log line or symptom.

**Default:** Task 3 will fix the Tier-2 comment block only.

---

## Q2 — May 9 options-flow alert with "0.0x baseline"

**Found:** No `0.0x`, `baseline = 0`, `/ baseline`, or `/baseline` pattern anywhere in `engine/` / `signals/` / `agents/`. `engine/volume_baselines.py` is a rolling 20-day-average producer with no zero-division surface. Weekend gates exist at 30+ sites including `engine/options_agents.py:53,84` and `healthcheck.py:121-123,316,375,548,591-593`.

**Question:** Was the alerter retired/removed in a recent sprint? Or is the bug somewhere I'm not looking — e.g. a tractor-beam upstream signal poster outside this repo?

**Default:** Task 1 skipped. No code change attempted.

---

## Q3 — NEW-1 second gate

**Found:** No `NEW-1`, `gate_2`, `secondary_gate`, or "second gate" hits in code or `docs/XO_BACKLOG.md`.

**Question:** Was NEW-1 absorbed into the HM-AK halt_mode work (shipped 2026-05-07, `engine/risk_radar.py:170`, `engine/autopilot.py:65`)? Or is it a separate concept that hasn't been ticketed?

**Default:** No action.

---

## Q4 — Tier-2 landmine (M-1)

**Found:** No "M-1" or "landmine" identifier in `docs/XO_BACKLOG.md` head 100. Tier-2 references in code are intentional gating (e.g. tractor-beam filter at `engine/risk_radar.py:170`).

**Question:** What is the actual identifier? Is "M-1" a Crusher-audit ticket number or something else?

**Default:** No action.
