# dayblade-0dte Two-Path Diagnosis — 2026-05-06

**Status:** Investigation only. No code changes. dayblade-0dte remains halted (`halt_mode='full'`).
**Cross-ref:** `docs/XO_BACKLOG.md` HM-AF (~line 471), today's halt at 10:43:54 MST.
**Investigator:** Claude (Admiral-approved scope).

---

## TL;DR

The two log signatures `engine/alpaca_options.py:251` and `engine/alpaca_options.py:315` are the **same call site** — the success log inside `submit_single_option`. The line number shifted from 251 → 315 when commit `1eeff7d` (HM-V + HM-AA-broad + HM-AC-Option-A, 2026-05-05 12:59 MST) added 147 lines above it. The pre-1eeff7d bytecode in the long-running PID kept emitting `:251`; the post-restart process emits `:315`.

Both fires originate from a single architectural path:

```
main.py scheduler (every 2 min)
  → run_battle_station_monitor (main.py:1002)
    → engine/battle_station.py::monitor_active_options (line 684)
      → _auto_close (battle_station.py:657)
        → close_options_position("dayblade-0dte", ...) (alpaca_options.py:585)
          → submit_single_option(player_id, contract_symbol, qty, side="sell")
            → success log at alpaca_options.py:315 (was :251)
```

**The real "EOD sweep" is a different path** — it uses `close_all_options()` (line 590) which calls `client.close_position()` directly and emits log lines at `alpaca_options.py:621` ("Alpaca options EOD close: ...") and `:638` ("EOD: N position(s) closed (..)"). Confirmed in `logs/trader.log:374400-374404` at 12:48:23 on 2026-05-05.

The cannibalization SELLs are NOT from the EOD sweep — they are from the 2-minute battle_station monitor, which scans every Alpaca options position regardless of which agent opened it and regardless of DTE/leg-role.

---

## 1. Function at `engine/alpaca_options.py:251`

**Pre-1eeff7d source layout** (mapped via `git show 19c6746:engine/alpaca_options.py` — the previous commit before the HM-V bundle):

At line :251 was the `console.log(f"[bold cyan]Alpaca OPTIONS {side.upper()} ...")` success line inside `submit_single_option(...)`. In the current source, that exact log line is at **line 315**. The two are the same statement.

In the **current** file at line 251, you find the closing of a docstring inside `_preflight_buying_power` (HM-AC Option-A added after 19c6746). That is unrelated to the log lines and is only the position the line counter happens to point to today.

**Function:** `submit_single_option`
**Signature** (current file line 272):
```python
def submit_single_option(
    player_id: str, contract_symbol: str, qty: int, side: str = "buy"
) -> dict:
```
**Docstring:**
> Submit a single-leg options market order.
>
> Args:
>     player_id: Must be in OPTIONS_PLAYERS or this is a no-op.
>     contract_symbol: OCC format, e.g. 'SPY260404C00580000'.
>     qty: Number of contracts (capped at MAX_SINGLE_CONTRACTS).
>     side: 'buy' or 'sell'.
> Returns dict with success/error.

It is a single-leg market order shim. It does not look at existing position sign — Alpaca's market order on an existing-long position closes that long; on an existing-short position increases the short. It has no spread-leg awareness.

---

## 2. Function at `engine/alpaca_options.py:315`

Same as §1 — `submit_single_option`, post-bundle line number for the success-log statement:
```python
console.log(f"[bold cyan]Alpaca OPTIONS {side.upper()} {qty}x {contract_symbol} — {player_id} order={order.id}")
```

---

## 3. Are :251 and :315 two methods or two call sites?

**Same statement, same function, same call site.** The line shift is bytecode-vs-disk drift introduced by commit `1eeff7d` adding 147 lines above the success log. The pre-bundle PID (firing on 2026-05-05) emitted `:251`. After the natural restart between 2026-05-05 12:56 MST and 2026-05-06 08:14 MST, the new process emitted `:315`. Verified by:
- `git log --oneline -- engine/alpaca_options.py` → `1eeff7d` is the most recent functional change.
- `git show 1eeff7d --stat` → +147 / -4 lines, with the bulk added above the `submit_single_option` body.
- `grep "Alpaca OPTIONS"` in current file returns exactly one match at line 315.

There is exactly ONE location in the file that emits the `Alpaca OPTIONS SELL Nx ...` line. The 6 trader.log fires (5 on 2026-05-05, 1 on 2026-05-06) all come through it.

---

## 4. Trigger path — what fires the SELL

### 4a. Caller chain

`submit_single_option` is called by **`close_options_position`** (line 585):
```python
def close_options_position(player_id: str, contract_symbol: str, qty: int) -> dict:
    """Close (sell to close) a specific options position."""
    return submit_single_option(player_id, contract_symbol, qty, side="sell")
```

`close_options_position` is called from exactly one site under the dayblade-0dte player_id:
- `engine/battle_station.py:668` inside `_auto_close(pos, reason)`:
  ```python
  close_options_position("dayblade-0dte", option_sym, qty)
  ```
  player_id is **hardcoded** to `"dayblade-0dte"` regardless of which strategy actually owns the position.

(There is also one ML caller in `execute_options_signal` at `alpaca_options.py:695`, but that is the entry path for buys — not the SELL trigger here.)

### 4b. What feeds `_auto_close`

`_auto_close` is called from `monitor_active_options` (battle_station.py:684) at line 763:
```python
if signal == "CLOSE_NOW":
    logger.warning(f"CLOSE_NOW triggered: {option_sym} — {reason}")
    _auto_close(pos, reason)
```

`monitor_active_options` builds its work list from `_get_alpaca_options_positions()` (battle_station.py:287), which calls `client.get_all_positions()` and yields **every** options-asset-class position on the Alpaca paper account. There is no filter on:
- `player_id` ownership (Alpaca has no concept of player_id; the broker just sees positions)
- DTE
- spread-leg vs single-leg structure
- long-vs-short (`qty` is normalized via `abs(float(pos.qty))` at line 319 — sign is dropped)

### 4c. Scheduler

`main.py:2580`:
```python
schedule.every(2).minutes.do(run_battle_station_monitor)
```
Plus a 55-second internal dedupe guard at `main.py:1006`. So the loop fires on the 2-minute boundary during market hours, examining every options position in the broker book.

### 4d. CLOSE_NOW trigger conditions (`battle_station.py:_generate_signal`, lines 583-654)

Five `CLOSE_NOW` rules. The DTE-relevant ones:
- **Rule 1 (line 602):** `pnl_pct <= -50` — fires regardless of DTE.
- **Rule 2 (line 606):** `is_0dte AND minutes_left < 90 AND pnl_pct < 0` — 0DTE only.
- **Rule 3 (line 612):** `is_0dte AND minutes_left < 30 AND pnl_pct < 0` — 0DTE only.
- **Rule 4 (lines 615-628):** wrong-side-of-gamma-flip on the position's `option_type` — fires regardless of DTE.

`is_0dte` is computed at line 594: `is_0dte = expiry_date == date.today() if expiry_date else False`. So on a non-0DTE position, only Rules 1 and 4 are reachable. Rule 1 (the −50% drawdown rule) is the broadest and will close any options position on Alpaca — long, short, hedge leg, opener leg, naked, or spread — once it falls below −50% pnl_pct.

### 4e. The cannibalization mechanism

For a `bull_put_spread` MLEG fill (long lower-strike PUT + short higher-strike PUT), Alpaca holds two book entries:
- LONG PUT row: `qty = +1, +3, +5` etc., `unrealized_plpc` reflects premium-paid decay.
- SHORT PUT row: `qty = -1, -3, -5` etc., `unrealized_plpc` reflects credit-collected decay.

`_get_alpaca_options_positions` discards the sign. `_generate_signal` runs Rules 1 and 4 against `unrealized_plpc`. When either side trips, `_auto_close` calls `submit_single_option(side="sell")`, which:
- On the LONG leg: market sell-to-close → leg is gone, **the SHORT leg now sits naked**.
- On the SHORT leg: market sell on a short → **doubles the short** (Alpaca will go more negative; eventually buying-power rejects it, but during the window it leaves naked-then-extra-short exposure).

This is exactly the HM-AF pattern: bull_put_spread_v1 opens a defined-risk spread; battle_station closes one leg as if it were a standalone long option, leaving naked short PUTs.

---

## 5. Why a 9-DTE position fired despite the "0dte" name

`SPY260515P00727000` is the LONG (protective) leg of a bull_put_spread opened by `bull_spread_v1` at **08:12:34** on 2026-05-06 (`logs/trader.log:388722`, `alpaca_options.py:393` = `submit_vertical_spread` success). 9 calendar DTE / 7 trading DTE.

**There is no DTE filter in the battle_station monitor path.** The agent's name `dayblade-0dte` is misleading:
- The hardcoded player_id at `battle_station.py:668` *attributes* the SELL to dayblade-0dte for accounting, but the **monitor scope is everything in the Alpaca options book**.
- The "0dte" filter exists only inside `_generate_signal`'s 0DTE-specific rules (Rules 2, 3) — those rules do NOT fire on a 9-DTE position, but Rules 1 and 4 still can.

Two minutes after the spread opened (`08:14:39`), Rule 1 (−50% drawdown — possible if entry premium was thin and quote-snap rejection bumped the long leg into a paper loss) or Rule 4 (gamma-flip wrong-side) fired against the long-leg row. `_auto_close` cannibalized the spread's hedge.

So: there is a missing scope filter, not just a missing DTE filter. The agent's identity is "dayblade-0dte", but its monitor harvests positions belonging to other agents (bull_spread_v1 in this case).

---

## 6. The "EOD sweep" reference

The string `"dayblade-sulu + dayblade-0dte EOD sweep"` is at `main.py:2268`:
```python
close_all_options("dayblade-sulu + dayblade-0dte EOD sweep")
```
Inside the Sulu daily 12:45-PM-MST close block (main.py:2237-2271). It calls `engine/alpaca_options.py::close_all_options`, defined at line 590:

```python
def close_all_options(player_id: str | None = None) -> dict:
    """Close ALL open options positions on Alpaca paper account.

    Called at 12:45 PM MST / 3:45 PM ET EOD sweep.
    If player_id is provided, filters log message but still closes everything
    (Alpaca doesn't track per-player — we close all to be safe).
    """
```

**Eligibility:** every position in `client.get_all_positions()` whose `asset_class` is `us_option`/`option`, OR whose symbol length > 10. There is no DTE, agent, or leg filter. The `player_id` argument is purely cosmetic for the log line at line 638; it does not narrow the close set.

`close_all_options` uses `client.close_position(symbol, ClosePositionRequest(qty=...))` (line 620) — Alpaca's position-flatten endpoint, which auto-detects the right side from the qty sign. That makes its log line different from `submit_single_option`'s. It emits:
- `Alpaca options EOD close: {symbol} x{qty}` per position (line 621, `alpaca_options.py:621`)
- `Alpaca options EOD: {N} position(s) closed ({who})` aggregate (line 638, `alpaca_options.py:638`)

Confirmed in `logs/trader.log:374400-374404` (2026-05-05 12:48:23):
```
Alpaca options EOD close: SPY260515P00723000 x4    alpaca_options.py:490
Alpaca options EOD close: SPY260515P00724000 x8    alpaca_options.py:490
Alpaca options EOD close: SPY260515P00725000 x1    alpaca_options.py:490
Alpaca options EOD: 3 position(s) closed           alpaca_options.py:507
   (dayblade-sulu + dayblade-0dte EOD sweep)
```
(Lines `:490` and `:507` are the pre-1eeff7d positions of the same statements; today they're at `:621` and `:638`.)

**This EOD sweep also has the cross-agent scope problem** — it would happily close legs of bull_put_spread/bull_call_spread positions if they happen to be open at 12:45 MST. The 2026-05-05 log shows this almost happening: it succeeded on 3 SHORT puts (the ones it had buying power to repurchase) and got `insufficient options buying power` errors on x720 and x718 (lines 374386-374398). Same architectural defect as battle_station's monitor.

The five 2026-05-05 SELL fires are NOT from this EOD sweep:
- 08:41:38 — battle_station 2-min monitor.
- 12:52:53 / 12:52:54 / 12:52:56 — battle_station 2-min monitor (4-7 minutes AFTER the 12:48 EOD sweep ran). These were the long-leg SELLs that finished the cannibalization the EOD sweep had started on the short legs.
- 12:56:33 — battle_station 2-min monitor.

The 2026-05-06 08:14:39 fire is also battle_station, fired ~2 minutes after a fresh bull_spread_v1 opened a 9-DTE SPY put spread.

---

## 7. Recommended scope for the HM-AF Item 2 fix

The defect is **not specific to dayblade-0dte**. dayblade-0dte is the visible attribution because of the hardcoded `player_id` string in `battle_station.py:668`, but the actual cross-agent scope leak occurs in two places that share one root cause: closes that operate on the broker book without leg/owner awareness.

### Three contaminated paths

| Path | File / Line | Trigger | Cross-agent scope? |
|---|---|---|---|
| **P1: battle_station 2-min monitor** | `battle_station.py:684` → `_auto_close:657` → `close_options_position` → `submit_single_option` | Every 2 min in market hours, on every options position via `_get_alpaca_options_positions` | YES — iterates `client.get_all_positions()` |
| **P2: Sulu/dayblade-0dte EOD sweep** | `main.py:2268` → `close_all_options:590` | Daily 12:45 MST | YES — by design closes everything |
| **P3: dayblade.py natural close path** | `dayblade.py:502` → `close_all_options` | Per-position SELL inside `sell_position` | YES — calls `close_all_options` after every dayblade sell |

P3 (`engine/dayblade.py:501-502`) is particularly subtle: it fires `close_all_options(DAYBLADE_PLAYER)` after EACH dayblade sell, not just on EOD. Every time T'Pol exits a single 0DTE position, it runs the broker-wide flatten on EVERY options leg in the account. This has likely been silently cannibalizing spreads on every dayblade trade since the 2026-05-04 gate flip — the visibility gap is that no single SELL-to-close-a-long is logged dramatically; the issue only manifests when a SHORT spread leg is left naked. Worth pulling 2026-05-04 → 2026-05-06 trade log to count.

### Recommended fix scope

Three layers, in priority order:

**Layer 1 (must-fix — the blast radius):**
Add a **spread-leg awareness filter** before any position is eligible for auto-close in P1, P2, and P3. The filter should:
- Resolve each Alpaca position back to whether it is a leg of a known multi-leg structure (cross-reference the `options_trades` or per-strategy positions table by `contract_symbol`).
- Skip positions tagged as a leg of `bull_put_spread_v1`, `bear_put_spread_v1`, `bull_call_spread_v1`, or any future multi-leg strategy.
- Or, equivalently, route closes through `close_vertical_spread` (alpaca_options.py:433, MLEG-aware) when a paired leg is detected.

This is a single utility (e.g. `is_spread_leg(symbol) -> str | None` returning the strategy name or `None`) plus three call-site guards. Same fix applied uniformly to P1/P2/P3.

**Layer 2 (must-fix — wrong-side-of-book):**
In `battle_station._get_alpaca_options_positions` (line 287), preserve `qty` sign as a separate field (e.g. `is_short`). In `_auto_close`, route SHORT-side closes to `submit_single_option(side="buy")` (buy-to-close). Today's hardcoded `side="sell"` is correct only for long positions; it doubles down on shorts.

**Layer 3 (architecture cleanup):**
Stop hardcoding `player_id="dayblade-0dte"` in `battle_station.py:668`. Resolve owner from the position record (via the strategy/positions table). If the position has no internal owner record, fall back to the EOD-sweep convention but log loudly — no agent should be able to silently inherit attribution for trades it didn't open.

### Why broader than dayblade-0dte

- P3 (`dayblade.py:502`) cannibalizes on **every dayblade sell**, not just EOD — fixing only battle_station would miss the highest-frequency leak.
- P2 (`main.py:2268`) runs daily and would re-create the issue on any future spread held over 12:45 MST.
- The fix is structural (add leg-awareness once) — it costs roughly the same as a dayblade-0dte-only fix and prevents the same incident with different attribution next time bull_call_spread or any future MLEG strategy is in the book.

### Out-of-scope confirmations (don't expand)

- `close_vertical_spread` (alpaca_options.py:433) is already MLEG-aware (it submits a paired close MLEG order). It is not a contaminated path.
- `submit_vertical_spread` and `submit_iron_condor` are open paths, not close paths — not relevant to cannibalization.
- The 6 ai_brain.py auto-TP loop deferral noted in commit `1eeff7d` is HM-AD, separate scope.

---

## Appendix — exact log evidence for the cannibalization timeline

### 2026-05-05
- **08:41:38** — `Alpaca OPTIONS SELL 1x SPY260515P00718000 — dayblade-0dte` (`alpaca_options.py:251`, line 367490) — battle_station single fire.
- **12:48:23** — `close_all_options` runs the official EOD sweep, succeeds on 3 short puts, fails on 2 with insufficient-buying-power (`alpaca_options.py:494`, lines 374386-374404). Already cross-agent: those 5 contracts are spread legs.
- **12:52:53 / 12:52:54 / 12:52:56** — battle_station fires 3 single-leg SELLs (`alpaca_options.py:251`, lines 374466-374472), 4 minutes after EOD sweep, on the long legs (P00718, P00719, P00720) the EOD sweep couldn't reach.
- **12:56:33** — battle_station fires another single-leg SELL (`alpaca_options.py:251`, line 374520).

### 2026-05-06
- **08:12:34** — `bull_spread_v1` opens fresh SPY 9-DTE bull put spread (`alpaca_options.py:393`, line 388722).
- **08:14:39** — battle_station fires single-leg SELL on the long leg `SPY260515P00727000` (`alpaca_options.py:315`, line 388735), 2 minutes after the spread opened.
- **10:43:54** (per Admiral notes) — dayblade-0dte halted via `halt_mode='full'`.

The line-number shift (`:251` → `:315`) between 12:56 on 2026-05-05 and 08:14 on 2026-05-06 confirms a service restart in that window picked up the post-1eeff7d disk source. Same statement, same caller, same defect.
