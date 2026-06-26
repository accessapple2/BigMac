# Kirk Super Scan — Daily Newsletter Prompt v2

**Runs:** Daily 06:00 America/Phoenix
**Change from v1:** "Make it actionable" → "Make it specific and scoreable." Added baseline framing. Added required machine-readable observation block so the newsletter auto-feeds `signal_observations` instead of being hand-extracted.

---

## PROMPT (paste this as the Kirk scan instruction)

> Do a super scan for swing trade opportunities: latest best short-term, intraday, day-trade buys, best gap opportunity, and best value options (calls/puts). Include physical metals and ETFs. Report the **top 3 Bull Spread Calls**, **top 3 Short Opportunities**, and **top Short Squeeze candidates**.
>
> **Specificity is mandatory — vague picks are useless.** For every name you surface, include:
> - **Ticker** and a **reference price** (current or last close)
> - **Direction** (long / short / bullish-spread)
> - For spreads: **specific strikes + expiration (DTE)** and **net debit**
> - For shorts AND squeezes: **short % of float** (number, not "high")
> - **Entry zone, stop, and target** — concrete levels, not "key support"
> - One-line **thesis** and a **horizon** in days
>
> If you cannot find a real reference price or short-interest number for a name, **drop it** — do not pad the list with consensus mega-caps that have no specific setup.
>
> **BASELINE NOTICE (include verbatim at top of newsletter while baseline window is active):** *"OllieTrades is in its 30-day baseline window. Everything below is logged as an OBSERVATION and scored later. No positions are staged. RULE #1 holds — Alpaca paper only, Schwab read-only."*
>
> After the human-readable newsletter, append the structured block below exactly, with no commentary, so it can be ingested automatically.

---

## REQUIRED TRAILING BLOCK (newsletter must end with this)

Emit a fenced ```json block tagged `OBSERVATION_BLOCK`. One object per surfaced name. Schema must match `signal_observations` ingestion:

```json
OBSERVATION_BLOCK
{
  "meta": {
    "source": "Kirk Super Scan",
    "captured_at": "YYYY-MM-DD",
    "capture_mode": "OBSERVATION_ONLY",
    "baseline_window": true,
    "execution": "NONE"
  },
  "observations": [
    {
      "ticker": "",
      "claim_type": "swing_long | bull_call_spread | short | squeeze_candidate | etf_metals | metals_macro",
      "direction": "long | short | bullish",
      "thesis": "",
      "reference_price": null,
      "entry_zone": null,
      "stop": null,
      "target": null,
      "spread_strikes": null,
      "dte": null,
      "short_pct_float": null,
      "horizon_days": 0,
      "specificity": "low | med | high",
      "scoreable_in_window": true,
      "entry_price_anchor": null
    }
  ]
}
```

**Rules for the block:**
- `entry_price_anchor` stays `null` — Scotty stamps it from Polygon at ingestion (canonical source).
- `reference_price` is what the scan *claims*; `entry_price_anchor` is what Polygon *confirms*. Keep both so we can later check if Kirk's quoted price was even accurate.
- Anything with a horizon past the baseline end date → `scoreable_in_window: false`.
- Grade `specificity` honestly: consensus mega-cap with no levels = `low`; named but loose = `med`; specific non-consensus setup with real short % and levels = `high`.

---

## INGESTION (Scotty, daily)

Script: `scripts/scotty_kirk_ingest.py`

1. Parse the `OBSERVATION_BLOCK` from the newsletter output.
2. Stamp `entry_price_anchor` from Polygon (06:00 last close, `/v2/aggs/ticker/{sym}/prev`).
3. Insert into `signal_observations`, `is_context=1`, no execution path.
4. Log row count; NTFY hard-alert if the block is missing (June-20 silent-failure mode fix).

**Cron (crontab -e):**
```
# Kirk newsletter save: daily at 06:00 AZ (note: AZ = MST = UTC-7, no DST)
# 06:00 MST = 13:00 UTC
# Step 1 — generate newsletter (your Grok runner goes here)
# Step 2 — ingest block
30 13 * * 1-5 cd /Users/bigmac/autonomous-trader && .venv/bin/python3 scripts/scotty_kirk_ingest.py data/kirk/$(date +\%Y-\%m-\%d).txt >> logs/kirk_ingest.log 2>&1
```

**Manual run:**
```bash
python3 scripts/scotty_kirk_ingest.py data/kirk/2026-06-22.txt
# or pipe:
cat newsletter.txt | python3 scripts/scotty_kirk_ingest.py -
```

**Grading (at any horizon checkpoint):**
```bash
python3 scripts/grade_kirk_scan.py all
```

---

## Non-exchange tickers (no Polygon price)

- `GOLD_SPOT`, `SILVER_SPOT`, `WTI` — anchor stamped as `null`; grade vs reference_price manually or via separate metals feed.

---

## Specificity legend

| Grade | Meaning |
|-------|---------|
| `low` | Consensus mega-cap, no levels — anyone's model says it |
| `med` | Named ticker, thesis, but no concrete entry/stop/short-% |
| `high` | Specific, non-consensus, real short % float, concrete levels |
