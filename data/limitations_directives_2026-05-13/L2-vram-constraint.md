# L2 — 8GB VRAM Constraint

**Reality:** RTX 5060 has 8GB VRAM, not 32GB system RAM.
- qwen2.5-coder:7b uses 4.59GB
- qwen3:8b uses ~4-5GB
- They cannot co-reside. Alpha squad rotation will model-swap every cycle.

## Empirical measurement needed (Tier 1, no Captain consult)

Need data: how often does the alpha squad pair rotate? What's the model-swap cost
on the RTX 5060 with fast NVMe?

```sql
-- Query 1: How many distinct models used per day by alpha squad?
SELECT date(t.executed_at, 'localtime') AS dt,
       COUNT(DISTINCT ap.model_id) AS distinct_models
FROM trades t
JOIN ai_players ap ON t.player_id = ap.id
WHERE ap.provider = 'ollama'
  AND date(t.executed_at, 'localtime') >= date('now', '-30 days')
GROUP BY dt;

-- Query 2: From Ollie Box journalctl, count [MemGuard] Switching events / day
journalctl -u ollama --since '24 hours ago' | grep -c "Switching model"
```

## Mitigation options (Captain decision after measurement)

**A. Accept rotation cost** (current state)
   - First call after swap: ~3-8s (model load from NVMe)
   - Subsequent calls: ~1-3s
   - Acceptable for advisory tier

**B. Use smaller quantization for second model**
   - qwen3:8b at Q4_K_M is ~4.5GB; at Q4_0 it's ~3.5GB
   - Two models at Q4_0 might co-reside in 8GB
   - Cost: marginal accuracy loss
   - Test: download qwen3:8b-q4_0 and measure

**C. Drop one of the two main models**
   - qwen2.5-coder:7b is the coder slot (ollama-coder, data-tng)
   - qwen3:8b is the 7-agent hot path
   - Picking ONE and routing both groups to it = no swap
   - Cost: coder loses coding-specialized model
   - Test: route ollama-coder to qwen3:8b for a week, A/B

**D. Hardware upgrade**
   - RTX 5060 8GB → RTX 5070 12GB or RTX 5070 Ti 16GB
   - Cost: $400-800
   - Removes constraint entirely
   - Captain budget decision

## Recommendation

Measure first (Tier 1 query, 5 min). Then Captain picks A/B/C/D.
Most likely outcome: A (accept rotation, current cost is sub-10s and acceptable).
