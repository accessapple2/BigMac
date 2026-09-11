# Relay — McCoy bakeoff, clean run 2: qwen3:8b + Fin-R1 (Trip, olliemax) + MiniMax-M3 (this session)

Supersedes run 1's timing numbers (`relay_2026-09-11_mccoy_bakeoff_run1.md`) with
uncontended data. Run 1's decision distributions already stood per the Admiral's
call; this run confirms them on a much larger sample for qwen3:8b and adds a clean
MiniMax-M3 read on the exact same 16 prompts the other two arms used.

## Methodology

All three arms scored against the SAME 16 real, live-captured McCoy scan prompts
(`~/modelworks/fleet_checks/mccoy/arena_calls.jsonl` on olliemax, exact request
bodies from real Arena calls today, 16,324-17,807 chars each) — not a fresh
same-day screened list, so this is a true apples-to-apples comparison across arms,
unlike run 1 (which used a freshly-built 10-symbol prompt set that happened to
overlap partially with today's screen).

- **qwen3:8b and Fin-R1**: run by Trip (Model Works/olliemax session) at
  `~/modelworks/bakeoff_arena/` (`run_arm.py`/`summarize.py`), scored with this
  repo's own `parse_decision.py`, num_ctx at the live 24576 (the fix shipped
  earlier today). qwen3:8b ran 14:01-14:45 MST (721 calls — 16 prompts x ~45
  samples each, temp 0.7); Fin-R1 ran 14:50-15:33 MST (69 calls, 65 scored).
  Neither contended with anything on bigmac's side this time.
- **MiniMax-M3**: run by this session, 15:38 MST, `scripts/mccoy_bakeoff_
  minimax_arena16.py` — remote API call, zero olliemax GPU load, needed no
  coordination window. 16 calls (one per prompt, temp/thinking at the provider's
  configured defaults — see that script for exact params). Logged to
  `mccoy_bakeoff_log`, `run_id=arena16-minimax-20260911T223848Z-229940`.

## Results

| | qwen3:8b (McCoy's live seat) | Fin-R1 | MiniMax-M3 |
|---|---|---|---|
| calls | 721 | 69 (65 scored) | 16 |
| never returned | 0 | 4 (5.8%) | 0 |
| strict contract valid | 100% (CI 99.5-100) | 46.2% (CI 34.6-58.1) | 100% (16/16) |
| decision recovered (any parse) | 100% | 95.4% (CI 87.3-98.4) | 100% |
| silent format-driven flips | 0 | 2 (3.1%), both BUY→HOLD | 0 |
| CJK code-switching | 0/721 | 0/65 | 0/16 |
| parsed actions | **BUY 721 (100%)** | HOLD 38 (55%), BUY 19 (28%), BUY_CALL 8 (12%) | **HOLD 16 (100%)** |
| confidence mean | 0.82 | 0.64 | 0.38 |
| wall_s mean | 6.8 (p90 8.7, under load test) | 20.1 (p90 30.8, 23% on CPU — upper bound) | 14.2 |
| cost | $0 (local) | $0 (local) | $0.0514 for 16 calls |
| reasoning:visible token ratio | n/a (no reasoning-mode split reported) | n/a | 5.1:1 (18,872 reasoning vs 3,670 visible) |

## The headline finding, now confirmed at scale

**qwen3:8b (today's live McCoy model) is 100% format-valid and 100% BUY across
721 calls spanning 16 different real symbols and ~45 temperature-0.7 samples
each.** This is not a small-sample artifact — it was already the pattern in run
1's 9/10 BUY (out of 10, mostly-different symbols) and now holds at 721/721 on a
proper multi-sample design. **MiniMax-M3, on the identical 16 prompts, went
16/16 HOLD** (run 1 was 10/10 HOLD on a different 10-symbol set — same
direction, now doubly confirmed). Fin-R1 is the only arm that actually varies
its decision (55% HOLD / 28% BUY / 12% BUY_CALL) — but pays for that variation
in format reliability.

**Trip's point, and it's the right one: validity alone cannot pick a winner
here.** A seat that always answers BUY scores perfectly on format validity by
construction — it never has to correctly identify a HOLD case, so it can never
fail the "does the Decision: line parse" test in a way that would show up as
low validity. Weighting validity heavily (as the directive asked, "format
validity weighted heavily") without also scoring **decision spread** would rank
qwen3:8b highest for exactly the behavior — always buying — that this
session's independent 42-trade/71.4%-hit-rate overconfidence finding and the
REASONING-DIRECTION-CONFLICT flags (4/10 in run 1) already suggest is a real
problem, not a strength. **Recommendation for whoever scores this bakeoff
formally: report validity and decision-spread as two separate axes, not one
blended number** — an arm that's 100% valid and 100% one action is a finding
about that arm's policy, not evidence it's the best one to seat.

## Fin-R1, if ever considered for a seat (not recommended as-is)

Per Trip's breakdown: 21/35 of its strict-format failures are harmless (HOLD
written with `Timeframe:N/A`/`Confidence:0.0`, parser defaults to 0.5/SWING but
the action itself stays correct — nothing would have traded wrong). The real
problem is markdown wrapping — 1 in 5 answers wrap the contract as
`**Decision:** BUY`, invisible to `parse_decision`'s line-prefix check, which
then falls back to a keyword scan and silently defaults confidence to 0.5. This
flipped a real written BUY into a parsed HOLD twice (one confirmed case: JPM
14:53:00, written BUY@0.85 → parsed HOLD@0.5) — a genuine, not cosmetic, scoring
bug for this arm specifically. The 4/69 (5.8%) that never returned were Trip's
own harness's 900s client timeout, not fleet traffic patterns — qwen3:8b (0/721)
and gemma3:4b (0/915) saw zero runaways on the same no-`num_predict` request
bodies, so a missing `num_predict` is the enabling condition, not the sole
cause; the actual looping (one answer emitted ~30 repeated `---` before hitting
3,202 tokens) is Fin-R1's own behavior on this prompt shape. **If Fin-R1 is
ever seated it needs `num_predict` set, markdown-tolerant parsing (or a
stricter format instruction), and doesn't fit in VRAM beside McCoy/Riker at
24576** (a separate, already-flagged constraint from Trip's earlier GPU-budget
message).

## GPU load test (Trip's side, 14:05-14:45, real traffic, 250W cap)

Sustained 78.0C/70.1C on both cards — neither hit the 84C throttle target, no
thermal limiting from this load. Fleet cost while Trip's test ran: 41 real
McCoy/Worf calls, all HTTP 200, median 5.2s vs a 1.3s baseline, p90 10.4s, max
13.8s, plus the one already-reported starved call. No fleet-visible failures.

## What this run does NOT answer yet

Calibration and forward-return-per-regime — the two scoring dimensions the
directive named alongside format validity — need real future price data to
elapse against these exact calls. `mccoy_bakeoff_log` rows from both run 1 and
this run carry symbol/timestamp (and price where available) so they're
forward-scoreable once enough time has passed; that scoring pass is not done
here and needs its own future session once there's something to score against.

## Coordination note

Trip's asks, addressed: (1) numbers merged above rather than asking them to
adopt bigmac's scoring — same `parse_decision.py` both sides already use, no
reconciliation needed; (2) a rerun with `num_predict` + a stricter format
instruction for Fin-R1, as it'd actually be served, is open — not requested
this pass, flagging as a live offer from Trip if a future session wants it;
(3) going forward, GPU load windows get posted to the relay
(`COORD_FROM_SCOTTY_*.md` / `FINDINGS_FOR_SCOTTY.md`) before either side runs,
per this run's own coordination note.
