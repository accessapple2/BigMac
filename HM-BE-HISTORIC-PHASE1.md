# HM-BE-historic — Phase 1 Discovery Report

**Date:** 2026-05-12
**Phase:** 1 (Discovery, no code changes)
**Auditor:** Claude / Q
**Refs:** HM-BE-HISTORIC-ANSWERS.md at 9b4f3a7

## TL;DR

- **super_backtest_v4.py model strings: METADATA-ONLY.** All 4 `.get("model")` callsites are DB writes or pretty-print — never invoke an LLM. Phase 2 rename of lines 93 + 117 is safe.
- **proving_ground.py + weekend_backtest.py: ID-ONLY consumers.** They reference `qwen3-14b-pro` as a player id; neither reads a `model` field. Out of Phase 2 scope.
- **NEW LOAD-BEARING FINDING (outside super_backtest_v4.py): `main.py:101`** wires `OllamaProvider("qwen3-14b-pro", "qwen3:14b", url=OLLIE_URL, timeout=180)`. The second positional arg is the runtime model string. Captain decision needed on whether to fold this into Phase 2.

## Detailed findings

### 1. super_backtest_v4.py — `.get("model")` callsites (the original Phase 1 question)

Four matches:

```
1413: spec.get("model", "")  → SQL INSERT into v4_agent_run_metrics row (column ~4)
1577: spec.get("model", "")  → SQL INSERT (cautious-tier results table)
1733: spec.get("model", "")  → SQL INSERT (bear-tier results table)
1974: spec.get("model", "")[:18]  → console pretty-print, truncated for column width
```

None of these invoke an LLM. The string is stored as historical label or rendered for the operator. **Verdict: METADATA-ONLY in super_backtest_v4.py.**

Backing specs (the source of the `model` field):

```
line 93   AGENT_SPECS:        "qwen3-14b-pro": {"name": "Seven", "model": "qwen3:14b", ...}
line 117  SNIPER_FLEET_V4:    "qwen3-14b-pro": {"name": "Seven", "model": "qwen3:14b", ...}
```

These are the 2 spec entries (4 string slots if `name` + `model` are both rewritten → matches Captain's "4 string replaces" count).

### 2. proving_ground.py — `qwen3-14b-pro` consumers

```
line 38: "qwen3-14b-pro",   # Seven   (entry in SNIPER_AGENTS list)
```

No `.get("model")` or `["model"]` anywhere in the file. The id appears only as a list member. **Verdict: ID-ONLY; no model-string semantics; no action needed in Phase 2.**

### 3. weekend_backtest.py — `qwen3-14b-pro` consumers

```
line 63:  "qwen3-14b-pro": {"stop": -0.04, "target": 0.06, "max_days": 5}
line 108: {"id": "qwen3-14b-pro", "name": "Seven", "strategy": "pure quant data", ...}
```

No `.get("model")` or `["model"]` anywhere in the file. The id is keyed for stop/target params and listed in BAKEOFF_AGENTS with `name`/`strategy`/`hint` — no `model` field. **Verdict: ID-ONLY; no model-string semantics; no action needed in Phase 2.**

### 4. CRITICAL ADJACENT FINDING — main.py:101 is LOAD-BEARING

```python
providers.append(OllamaProvider("qwen3-14b-pro", "qwen3:14b", url=OLLIE_URL, timeout=180))
```

The 2nd positional arg becomes `self.model_id` (see `engine/providers/ollama_provider.py:8`) and is sent verbatim to the Ollama API in `call_model()`:

```python
payload = {"model": self.model_id, "prompt": prompt, ...}
```

**This is what actually runs at runtime.** The `qwen3-14b-pro` player invokes `qwen3:14b` on the Ollama server, NOT `qwen3:8b`.

This contradicts:
- `config.py:166` MODELS registry (claims `model="qwen3:8b"` for this id)
- `cost_tracker.py:49` comment ("HM-BE: aligned with runtime; downgraded 2026-04-20")

The MODELS registry in `config.py` is not consumed by `main.py`'s provider wiring — `main.py` hand-constructs each provider with literal model strings. So config.py:166's "8b" is aspirational/stale, not enforced.

**Implication for historic backtest data:**
- Pre-2026-04-20: ran against qwen3:14b (per main.py — never changed)
- Post-2026-04-20: STILL running against qwen3:14b (main.py:101 was not updated as the cost_tracker comment claimed)
- The "downgrade" never landed for `qwen3-14b-pro`. Historic numbers are internally consistent (all qwen3:14b), so they ARE comparable to current numbers, contrary to the CLAUDE.md note Captain pre-drafted.

### 5. Other `qwen3-14b-pro` consumers (catalog, no action needed)

```
crew_scanner.py:863       — display-only strategy hint
crew_specialization.py    — display + benching flags
consensus.py:56           — emoji + display label
drift_rebalancer.py:42    — id list
sub_portfolio.py:27       — id list
cost_tracker.py:49        — display-only cost row
fallback.py:33            — fallback model string ("qwen3:14b")  → LOAD-BEARING when primary fails
reference_data.py:36      — "gemini-pro" → "qwen3-14b-pro" mapping (id alias)
providers/base.py:373     — display narrative
providers/gemini_provider.py:21 — default player_id arg (NOT the actual provider for qwen3-14b-pro)
dashboard/static/index.html (4 lines) — display labels in JS
_archive/2026-04-26/* — archived old versions, ignore
```

`fallback.py:33` echoes the same `qwen3:14b` runtime model as main.py:101 — consistent.

## Phase 2 + 3 — Updated Captain decision points

Original Phase 2 plan (verbatim from HM-BE-HISTORIC-ANSWERS.md):
> 2. Phase 2 code rename in super_backtest_v4.py (4 string replaces, anchor # === HM-BE-historic ===)

This remains safe and metadata-only. The 4 replaces are presumably:
- Line 93: `"name": "Seven"` → `"name": "Dalio Macro 8B"` AND `"model": "qwen3:14b"` → `"model": "qwen3:8b"`
- Line 117: same pair in SNIPER_FLEET_V4

**Captain decision A — adjacent main.py:101 + fallback.py:33:**
- Option A.1: Defer to a follow-up HM. Phase 2 only touches super_backtest_v4.py as originally scoped.
- Option A.2 (recommended): Fold main.py:101 + fallback.py:33 into Phase 2. The CLAUDE.md note Captain drafted implies belief that runtime is already on qwen3:8b — only the labels are wrong. Reality is the opposite. If the *intent* of the 2026-04-20 swap was to downgrade qwen3-14b-pro to qwen3:8b, this lands that intent. If the intent was metadata-only, leave main.py:101 alone but rewrite the CLAUDE.md note to match reality.

**Captain decision B — CLAUDE.md note wording (depends on A):**
- If A.1 chosen (defer main.py): drop the "pre-2026-04-20 OOS ran against 14b; post-swap runs against 8b" note — that statement is false. The truthful note is "qwen3-14b-pro runs against qwen3:14b at runtime despite display label saying 'Dalio Macro 8B' / 'qwen3:8b'. Historic numbers are internally consistent."
- If A.2 chosen (rename + actually downgrade main.py:101): the original CLAUDE.md note becomes accurate from 2026-05-12 forward (the actual swap date, not 2026-04-20). Phrase as: "qwen3-14b-pro pre-2026-05-12 OOS ran against qwen3:14b; post-2026-05-12 runs against qwen3:8b. Historic numbers not directly comparable."

**HALT — Phase 2/3 ship awaits Captain decision on A.1 vs A.2.**

## Files touched in Phase 1

None. Discovery only.
