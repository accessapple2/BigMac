# 🔧 SCOTTY — HM-BK + HM-BL: Kirk Bridge Singleton + $ATH Leak
### Two Surgical Fixes · Opus 4.7 · Discover → Diff → Apply

> **Captain's orders, Mr. Scott:** Two real findings from today's diagnostic ride. **HM-BK** — Kirk advisory rebuilds AlpacaBridge object every 2 min instead of reusing a singleton (inefficient, may contribute to CLOSE_WAIT churn we saw earlier). **HM-BL** — the literal string `$ATH` is leaking into a yfinance ticker fetch (we saw "$ATH possibly delisted" in trader_error.log). Probably an all-time-high flag accidentally serialized as a ticker. Both small, code-only.

## Pre-flight

```bash
cd ~/autonomous-trader
git log origin/main --oneline | head -3
git status --short
```

## Phase BKBL.0 — Discovery (NO writes)

### HM-BK side: AlpacaBridge re-instantiation

```bash
echo "── 1. Kirk advisory call path that triggers bridge init ──"
grep -rn "AlpacaBridge\|alpaca_bridge\|kirk.*advisory\|total_portfolio" dashboard/app.py engine/ 2>/dev/null | head -20

echo ""
echo "── 2. AlpacaBridge __init__ log line (we know this fires every 2 min) ──"
grep -n "Alpaca Paper Trading bridge initialized\|class AlpacaBridge" engine/alpaca_bridge.py 2>/dev/null | head -5
sed -n '1,40p' engine/alpaca_bridge.py 2>/dev/null

echo ""
echo "── 3. Singleton precedent in the codebase (any existing pattern to mirror?) ──"
grep -rn "_instance\|@functools.lru_cache\|@singleton" engine/ | head -10
```

### HM-BL side: $ATH leak

```bash
echo "── 4. Search for $ATH literal across codebase ──"
grep -rn '\$ATH\|"ATH"\|all.time.high.*ticker\|all_time_high' engine/ scripts/ main.py dashboard/app.py 2>/dev/null | head -20

echo ""
echo "── 5. Recent $ATH error context in logs (what was it being fetched FOR?) ──"
grep -B 3 -A 3 '\$ATH possibly delisted' ~/autonomous-trader/logs/trader_error.log 2>/dev/null | head -20

echo ""
echo "── 6. yfinance fetch sites that take symbol as arg ──"
grep -rn "yf.download\|yf.Ticker\|fetch.*symbol\|fetch_quote\|get_quote" engine/ shared/ 2>/dev/null | head -15
```

Document for Captain:
- HM-BK: where AlpacaBridge() gets re-instantiated, and the simplest singleton pattern (module-level instance or `@lru_cache`)
- HM-BL: where `$ATH` originates (likely a flag/marker variable being serialized as a symbol)
- Q1: HM-BK approach — module-level `_bridge = AlpacaBridge()` lazy singleton, OR `@lru_cache` on factory, OR explicit dependency-injected singleton
- Q2: HM-BL approach — guard the input (refuse non-alphanumeric tickers) OR fix the leak at source (find where `$ATH` is set and prevent it from being passed)

Write `data/scotty_hm_bkbl_report.md`. **HALT.** ntfy.

## Phase BKBL.1 — HM-BK (smaller; ship first)

Diff with `# === HM-BK ===`. Compile (venv/bin/python3). Commit `fix(kirk): HM-BK — reuse AlpacaBridge singleton instead of rebuilding every 2 min`. ntfy.

## Phase BKBL.2 — HM-BL

Diff with `# === HM-BL ===`. Compile. Commit `fix(scanner): HM-BL — guard against $ATH literal leaking into ticker fetch`. ntfy.

## Phase BKBL.C — Verify

```bash
grep -n "HM-BK\|HM-BL" engine/ dashboard/ 2>/dev/null | head -10
echo ""
echo "── Watch trader.log for 4+ minutes — bridge init should now appear ONCE per restart, not every 2 min ──"
LAST_INIT=$(grep -c "Alpaca Paper Trading bridge initialized" ~/autonomous-trader/logs/trader.log)
echo "  Bridge init count BEFORE restart: $LAST_INIT"
```

## Phase BKBL.D — Closure + push + restart + cadence verify (INLINE)

```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 30
NEW_PID=$(launchctl list | grep com.trademinds.trader | awk '{print $1}')
echo "  New PID: $NEW_PID"

echo ""
echo "── Wait 5 min, then count bridge inits since restart ──"
sleep 300
BRIDGE_INIT_COUNT=$(grep -c "Alpaca Paper Trading bridge initialized" ~/autonomous-trader/logs/trader.log)
echo "  Post-restart bridge init log line count: 1 expected (vs 1 every 2min before)"
echo "  Last 3 bridge init lines:"
grep "Alpaca Paper Trading bridge initialized" ~/autonomous-trader/logs/trader.log | tail -3
```

ntfy: `🏁 HM-BKBL complete — bridge singleton + $ATH guard live`.
