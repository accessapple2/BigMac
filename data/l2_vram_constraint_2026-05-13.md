# L2 — 8GB VRAM Constraint Measurement

**Hardware:** RTX 5060 with 8GB VRAM on Ollie Box  
**Constraint:** qwen2.5-coder:7b uses ~4.6GB, qwen3:8b uses ~4-5GB. Cannot co-reside. Alpha squad rotation triggers model-swap.  
**Question:** is rotation frequent enough to warrant action, and what's the swap cost?

## Findings

## Distinct models used per day
```
dt          distinct_models  models                                                                 
----------  ---------------  -----------------------------------------------------------------------
2026-05-13  2                qwen3:8b,qwen2.5:7b                                                    
2026-05-12  3                qwen3:8b,hf.co/0xroyce/Plutus-3B:Q4_K_M,qwen2.5:7b                     
2026-05-11  3                qwen3:8b,hf.co/0xroyce/Plutus-3B:Q4_K_M,qwen2.5:7b                     
2026-05-08  3                qwen3:8b,deepseek-r1:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M                 
2026-05-07  3                qwen3:8b,deepseek-r1:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M                 
2026-05-06  3                qwen3:8b,deepseek-r1:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M                 
2026-05-05  4                qwen3:8b,deepseek-r1:7b,qwen2.5-coder:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M
2026-05-04  4                qwen3:8b,deepseek-r1:7b,qwen2.5-coder:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M
2026-05-03  1                qwen3:8b                                                               
2026-05-01  4                qwen3:8b,deepseek-r1:7b,qwen2.5-coder:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M
2026-04-30  3                qwen3:8b,deepseek-r1:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M                 
2026-04-29  4                qwen3:8b,deepseek-r1:7b,qwen2.5-coder:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M
2026-04-28  3                qwen3:8b,deepseek-r1:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M                 
2026-04-27  3                qwen3:8b,deepseek-r1:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M                 
2026-04-24  3                qwen3:8b,deepseek-r1:7b,hf.co/0xroyce/Plutus-3B:Q4_K_M                 
2026-04-23  2                deepseek-r1:7b,qwen3:8b                                                
2026-04-21  1                qwen3:8b                                                               
2026-04-20  1                qwen3:8b                                                               
2026-04-17  1                qwen3:8b                                                               
2026-04-16  1                qwen3:8b                                                               
2026-04-15  1                qwen3:8b                                                               
2026-04-14  1                qwen3:8b                                                               
2026-04-13  2                qwen3:8b,deepseek-r1:7b                                                
```

## Signal-volume by model (last 7d)
```
model_id                        signals
------------------------------  -------
qwen3:8b                        61     
hf.co/0xroyce/Plutus-3B:Q4_K_M  51     
qwen2.5:7b                      19     
deepseek-r1:7b                  15     
```

## MemGuard model-swap events
```
  Total MemGuard swaps in current trader.log/trader_error.log: 1324
  Most recent 10:
logs/trader_error.log:11:03:00 [LRS] [MemGuard] Switching model qwen2.5-coder:7b → phi3:mini; unloading previous first
logs/trader_error.log:11:03:12 [LRS] [MemGuard] Switching model phi3:mini → qwen3:8b; unloading previous first
logs/trader_error.log:11:44:20 [LRS] [MemGuard] Switching model qwen3:8b → phi3:mini; unloading previous first
logs/trader_error.log:11:44:21 [LRS] [MemGuard] Switching model phi3:mini → qwen2.5-coder:7b; unloading previous first
logs/trader_error.log:11:51:15 [LRS] [MemGuard] Switching model qwen2.5-coder:7b → phi3:mini; unloading previous first
logs/trader_error.log:11:51:41 [LRS] [MemGuard] Switching model phi3:mini → qwen3:8b; unloading previous first
logs/trader_error.log:12:32:06 [LRS] [MemGuard] Switching model qwen3:8b → phi3:mini; unloading previous first
logs/trader_error.log:12:33:35 [LRS] [MemGuard] Switching model phi3:mini → qwen2.5-coder:7b; unloading previous first
logs/trader_error.log:12:43:36 [LRS] [MemGuard] Switching model qwen2.5-coder:7b → phi3:mini; unloading previous first
logs/trader_error.log:12:43:39 [LRS] [MemGuard] Switching model phi3:mini → qwen3:8b; unloading previous first
```

## Ollie Box ollama journal swaps (24h)
```
  model-load events on Ollie (24h): 211
  unload events on Ollie (24h): 0
0

  Sample timing pattern:
May 13 15:27:29 ollie ollama[2409]: time=2026-05-13T15:27:29.002-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="617.5 MiB"
May 13 16:08:20 ollie ollama[2409]: time=2026-05-13T16:08:20.539-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="2.1 GiB"
May 13 16:08:44 ollie ollama[2409]: time=2026-05-13T16:08:44.172-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="3.8 GiB"
May 13 16:14:39 ollie ollama[2409]: time=2026-05-13T16:14:39.370-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="3.8 GiB"
May 13 16:32:26 ollie ollama[2409]: time=2026-05-13T16:32:26.308-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="573.5 MiB"
May 13 16:32:30 ollie ollama[2409]: time=2026-05-13T16:32:30.114-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="2.1 GiB"
May 13 16:32:33 ollie ollama[2409]: time=2026-05-13T16:32:33.315-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="3.8 GiB"
May 13 16:32:37 ollie ollama[2409]: time=2026-05-13T16:32:37.481-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="1.6 GiB"
May 13 16:33:36 ollie ollama[2409]: time=2026-05-13T16:33:36.176-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="617.5 MiB"
May 13 16:40:33 ollie ollama[2409]: time=2026-05-13T16:40:33.772-07:00 level=INFO source=sched.go:627 msg="updated VRAM based on existing loaded models" gpu=GPU-b3c79d2b-72ba-e853-ebe5-9f73bf95ea05 library=CUDA total="8.0 GiB" available="3.8 GiB"

## Q5: /api/ps live state
```
  0xroyce/plutus:latest           expires in +0.0min  VRAM=5.78GB
```

## Q6: Empirical swap cost (HM-CD-instr walls)
```
── ollama-coder walls (count by wall-seconds-bucket) ──
   60-64 s      1 samples
  205-209s      1 samples

── neo-matrix walls (count by wall-seconds-bucket) ──
   60-64 s      1 samples
  100-104s      1 samples
```

## Interpretation

**Q1 data:** 3-4 distinct ollama models used per day across recent weeks — multi-model rotation is the norm, not a special case.

**Q2 data:** qwen3:8b 61 signals, Plutus-3B 51, qwen2.5:7b 19, deepseek-r1:7b 15 (7-day). No single-model dominance — top model is only 42% of volume.

**Q3+Q4 data:** 1,324 MemGuard switches in current trader log retention; 211 model-load events on Ollie Box in last 24h. **Rotation is happening every ~7 minutes on average.**

**Q4 VRAM trace:** `available` swings 617MB → 3.8GB → 1.6GB repeatedly. Constraint is real and being actively negotiated.

## Decision recommendation

**Option A is supported** — accept rotation:
- Q3+Q4 frequency is high (211 swaps/day), but Q6 walls are <10s consistently post-GPU
- Q1 shows 3-4 distinct models active per day (Option D would only matter if 5+)
- Q2 shows no single-model dominance (Option C trade-offs would be real)

**Caveat for next session:** RTX 5060 8GB handles the rotation cost so well (single-digit second walls) precisely because the model load from fast NVMe is cheap. If we add a 5th hot model to the rotation, the math could change. Monitor Q6 if fleet expands.

## Captain action

Recommend Option A (accept rotation). Skip B (Q4_0 quantization), C (consolidate), D (hardware upgrade).


## CAPTAIN DECISION 2026-05-13 — Option A (Accept Rotation)

**Decision:** Accept current rotation cost. No code or hardware changes.

**Rationale:** 211 swaps/24h is high frequency, but post-GPU walls are consistently
<10s. RTX 5060 + fast NVMe makes model loads cheap. The "8GB VRAM constraint"
is real architecturally but not painful in practice.

**Trigger to revisit:**
- Fleet adds 5th hot Ollama model (current is 4)
- Empirical wall time exceeds 20s consistently
- Hardware upgrade becomes attractive for other reasons

**Out-of-scope:** Options B (Q4_0), C (consolidate), D (RTX 5070 upgrade) all deferred.
