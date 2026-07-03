# DRAFT — Wheel leveraged-ETF risk rule — FOR SIGN-OFF, NOT APPLIED

## Important finding before the draft itself: the existing door1 blocklist has a coverage gap

`engine/wheel_strategy.py` already has a `LEVERAGED_ETF_BLOCKLIST` from
2026-06-19 ("door1: no new 3x-leveraged CSP writes — tail risk outweighs
premium income"), and it's genuinely enforced there. But there are **two**
CSP-writing systems, not one:

| System | File | Agent ID | Respects the blocklist? |
|---|---|---|---|
| Wheel Strategy | `engine/wheel_strategy.py` | `options-sosnoff` | Yes — confirmed in code |
| Shadow CSP | `engine/shadow_csp.py` | `shadow-qwen35-csp` | **No — no blocklist reference at all** |

Confirmed in the live data: of 37 open SOXL/UPRO CSPs, 36 predate 2026-06-19
(legitimately grandfathered), but **one — `shadow-qwen35-csp` UPRO,
2026-06-28** — was opened well after door1, from the system that was never
wired to the blocklist. This isn't hypothetical; it already happened.

**Whatever gets applied should live in one shared place both files import**,
not get re-added to `wheel_strategy.py` alone and silently miss
`shadow_csp.py` a second time.

## (a) Hard cap: leveraged legs ≤ 25% of wheel book assignment exposure

Current state (from this session's stress test, `docs/HANDOFF.md`): leveraged
legs (SOXL+UPRO) are **100%** of the $146,746.20 aggregate SPY-10%-shock
assignment exposure — SPY/QQQ legs currently contribute $0 (comfortably OTM).
A 25% cap means **no new leveraged entries at all until existing positions
run off and the leveraged share drops under 25%** — at today's concentration,
this cap would currently block every new leveraged-ETF CSP by itself,
independent of the regime gate below.

**Proposed check** (pseudocode, goes wherever each system currently checks
`LEVERAGED_ETF_BLOCKLIST` / would need to add such a check):
```python
LEVERAGED_ETF_EXPOSURE_CAP_PCT = 0.25  # of total open wheel-book CSP exposure

def leveraged_exposure_share() -> float:
    """Sum |max_loss| for open CSPs, leveraged vs total, across BOTH
    wheel-writing systems (options-sosnoff AND shadow-qwen35-csp)."""
    with get_db() as db:
        rows = db.execute(
            "SELECT symbol, max_loss FROM options_trades "
            "WHERE structure='csp' AND status='open'"
        ).fetchall()
    total = sum(abs(r["max_loss"] or 0) for r in rows)
    leveraged = sum(abs(r["max_loss"] or 0) for r in rows if r["symbol"] in LEVERAGED_ETF_TICKERS)
    return (leveraged / total) if total > 0 else 0.0

def leveraged_entry_allowed(new_position_max_loss: float) -> bool:
    """Would adding this new leveraged position push the leveraged share
    over the cap? Blocks BEFORE the trade, not after."""
    with get_db() as db:
        rows = db.execute(
            "SELECT symbol, max_loss FROM options_trades "
            "WHERE structure='csp' AND status='open'"
        ).fetchall()
    total = sum(abs(r["max_loss"] or 0) for r in rows) + abs(new_position_max_loss)
    leveraged = sum(abs(r["max_loss"] or 0) for r in rows if r["symbol"] in LEVERAGED_ETF_TICKERS) + abs(new_position_max_loss)
    return (leveraged / total) <= LEVERAGED_ETF_EXPOSURE_CAP_PCT if total > 0 else True
```

## (b) Regime gate: no NEW leveraged-ETF CSPs while SPY < gamma flip or VIX > 20

Data sources already exist for both conditions — no new plumbing needed:
- Gamma flip: `gex_snapshots` table (`engine/gamma_context.py`), `spot_price` vs `gamma_flip` columns, refreshed intraday.
- VIX: already used elsewhere in this codebase (e.g. `engine/wheel_strategy.py`'s own `MIN_VIX = 18` gate already reads VIX for a *different* purpose — reuse that source).

```python
VIX_LEVERAGED_CSP_CEILING = 20.0

def leveraged_regime_gate_open() -> tuple[bool, str]:
    """Returns (allowed, reason). Blocks NEW leveraged-ETF CSP writes only --
    does not touch existing positions (see (c) below)."""
    spy_snap = _latest_gex_snapshot("SPY")  # spot_price, gamma_flip
    if spy_snap and spy_snap["spot_price"] < spy_snap["gamma_flip"]:
        return False, f"SPY {spy_snap['spot_price']} below gamma flip {spy_snap['gamma_flip']}"
    vix = get_current_vix()
    if vix is not None and vix > VIX_LEVERAGED_CSP_CEILING:
        return False, f"VIX {vix} above {VIX_LEVERAGED_CSP_CEILING} ceiling"
    return True, "ok"
```

## (c) Existing positions run to expiry — no forced unwinds

No code change needed for this — it's an explicit *absence* of action. Worth
stating clearly in the rule's own comment so nobody "helpfully" adds a
force-close sweep later: this rule only gates *new* entries, exactly like
the existing door1 blocklist already does for `wheel_strategy.py`.

## Which pending/actual entries this week would have been blocked

Only one leveraged-ETF CSP entry occurred this week (2026-06-26 through
2026-07-02): **`shadow-qwen35-csp` UPRO, 2026-06-28T04:30:09** (2 contracts,
strike $116.26). Checked against both proposed conditions independently:

- **25% cap**: would block it — leveraged share was already ~100% of open
  wheel-book exposure before this trade, nowhere close to under 25%.
- **Regime gate**: would *also* independently block it — `gex_snapshots`
  shows SPY spot $732.14 vs gamma_flip $740.9 on 2026-06-28 (spot below
  flip). VIX for that exact timestamp not pulled for this draft (would need
  a historical VIX series lookup) — the gamma-flip condition alone is
  sufficient to confirm the block.

No other leveraged-ETF entries (SOXL/UPRO/TQQQ/TNA/etc.) occurred this week
from either system — every other new CSP this week was SPY/QQQ (not subject
to this rule).

## Recommended implementation shape (draft, not applied)
1. New shared module (e.g. `engine/leveraged_wheel_risk.py`) holding
   `LEVERAGED_ETF_TICKERS`, `LEVERAGED_ETF_EXPOSURE_CAP_PCT`,
   `VIX_LEVERAGED_CSP_CEILING`, and the two check functions above — single
   source of truth, imported by both `wheel_strategy.py` and `shadow_csp.py`.
2. `wheel_strategy.py`'s existing `LEVERAGED_ETF_BLOCKLIST` check becomes:
   keep the full blocklist (still the simplest, strictest control) OR relax
   it to the new %-cap + regime gate if the intent is "allow some leveraged
   exposure, just bounded" rather than "zero new leveraged writes ever" —
   these are two different policies and worth an explicit choice before
   implementing (the door1 comment reads as "zero new," the new request
   reads as "capped, not zero" — worth confirming which is actually wanted
   going forward, since they're not the same rule).
3. `shadow_csp.py` gets the same check added for the first time — this is
   the part that actually closes the confirmed gap.
