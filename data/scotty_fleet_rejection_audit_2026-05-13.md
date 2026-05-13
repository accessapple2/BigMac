# HM-FLEET-REJECTION-AUDIT (V2) — 2026-05-13T16:07

**V2 correction:** original grep patterns missed the actual log shapes.
Used `below confidence` / `Daily limit` / `Max positions` / `Quality gate` —
actual log prefixes are `LOW_CONVICTION:` / `Daily trade limit` /
`MAX_POSITIONS_REACHED:` / `QUALITY_GATE_FAILED:`. V2 uses correct anchors.

## Rejection-pattern totals (last 7d, actual data)

```
  HALTED           471
  MANDATE          2717
  Conviction       2314
  Max-positions    1134
  Daily-limit      1237
  Quality-gate     2790
  Spread-cannib    398
```

## Per-player rejection profile

| Player | HALTED | MANDATE | Conviction | Max-positions | Daily-limit | Quality | Spread-cannib |
|---|---|---|---|---|---|---|---|
| alpaca-mirror | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| capitol-trades | 0 | 0 | 7 | 2 | 0 | 0 | 0 |
| cto-grok42 | 0 | 0 | 0 | 0 | 13 | 1 | 0 |
| dalio-metals | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| deepseek-7b-grok4 | 0 | 74 | 24 | 341 | 0 | 2 | 0 |
| energy-arnold | 0 | 1083 | 0 | 0 | 0 | 0 | 0 |
| enterprise-computer | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| mlx-qwen3 | 0 | 251 | 0 | 0 | 0 | 0 | 0 |
| navigator | 0 | 0 | 0 | 542 | 0 | 0 | 0 |
| ollama-coder | 0 | 429 | 0 | 0 | 0 | 0 | 0 |
| ollama-deepseek | 0 | 21 | 0 | 0 | 0 | 0 | 0 |
| ollama-kimi | 0 | 10 | 0 | 0 | 0 | 0 | 0 |
| ollama-plutus | 0 | 0 | 0 | 31 | 226 | 6 | 0 |
| ollama-qwen3 | 0 | 34 | 0 | 151 | 513 | 6 | 0 |
| ollie-auto | 0 | 0 | 1408 | 17 | 0 | 2767 | 0 |
| options-sosnoff | 0 | 327 | 0 | 0 | 0 | 0 | 0 |
| qwen3-14b-pro | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| qwen3-8b-flash | 0 | 35 | 0 | 50 | 439 | 8 | 0 |
| qwen3-8b-sonnet | 0 | 164 | 0 | 0 | 0 | 0 | 0 |
| red-alert | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## Captain action items

1. Review high-MANDATE players (energy-arnold, ollama-coder, mlx-qwen3, deepseek-7b-grok4)
2. Review high-Conviction-floor players — calibration question (need to see them now)
3. Flag stale-active emitters with 0 fills + >50 rejections for halt_mode='exit_only'

