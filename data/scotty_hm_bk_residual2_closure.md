# HM-BK-residual2 — Closure (UNREPRODUCIBLE / KNOWN-ACCEPTABLE)

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** CLOSED — no code change; document as known-acceptable

## Investigation

HM-BK closed by reducing AlpacaBridge instantiation to a single module-level singleton in `engine/alpaca_bridge.py:185`. The "residual" banner emissions visible in `logs/trader.log` were re-investigated.

## Findings

### Single instantiation site (verified clean)

```
engine/alpaca_bridge.py:185:alpaca = AlpacaBridge()
```

This is the only `AlpacaBridge(...)` call in the codebase. The comment at `engine/total_portfolio.py:194-196` explicitly notes that the prior pre-HM-BK pattern (`AlpacaBridge()` per call) was the bug, and now everything reuses this singleton.

### All consumers import the singleton

10 consumer sites grep-confirmed using `from engine.alpaca_bridge import alpaca` (or aliased): `paper_trader.py`, `cash_manager.py`, `tax_harvester.py`, `reconciliation.py` (2×), `total_portfolio.py`, `dashboard/phase4_routes.py`, `dashboard/app.py` (4×), `scripts/snapshot_real_portfolio.py`. None re-instantiate.

### No multiprocessing fork

`grep -rn 'multiprocessing\|Process(\|os.fork' main.py engine/` → zero hits in the trader process tree.

### Banner re-emits come from distinct launchd processes

The banner fires from `AlpacaBridge.__init__` (`alpaca_bridge.py:20`) exactly once per Python interpreter. The 10+ banner timestamps in `trader.log` correspond to imports across separate launchd-managed processes, each of which has its own interpreter and its own module cache:

| Plist                                       | Process role                      |
|---------------------------------------------|-----------------------------------|
| com.trademinds.trader                       | Main trader (pid 28464)           |
| com.trademinds.signal-center                | Signal Center API (pid 18380)     |
| com.trademinds.scanner                      | Pre-market scanner (pid 56963)    |
| com.trademinds.mcp                          | MCP server (pid 841)              |
| com.trademinds.watchdog                     | Watchdog (pid 90816)              |
| com.ollietrades.real-portfolio-snapshot     | Periodic snapshot script          |
| com.trademinds.healthcheck                  | Periodic health check             |
| com.trademinds.metals-sync                  | Metals sync script                |
| com.trademinds.premarket                    | Pre-market routines               |
| com.trademinds.webull-sync                  | Webull sync (legacy, winding down)|

Each plist invokes a fresh Python process that imports `engine.alpaca_bridge` cleanly, instantiates the module-level singleton once, and emits one banner. **This is correct behavior** — Python module caching is per-process and there is no IPC/shared-state mechanism (nor should there be for what is purely a log line).

### What the singleton actually prevents

HM-BK's singleton prevents the prior `AlpacaBridge()` per-call pattern within a single process (which was hitting the bridge `__init__` thousands of times per scan cycle). That goal is achieved and verified.

## Conclusion

HM-BK-residual2 is not a defect. The remaining banner emissions are per-process imports across distinct launchd plists, which is normal multi-process architecture. The only way to reduce them further would be:

- (a) Suppress the banner conditionally based on log-level / env var (cosmetic, no behavioral benefit; could blind operators to bridge-init failures in ancillary processes)
- (b) Convert the bridge into a network service all processes call (large architectural change, no value)

Neither is justified. **No commit; document as known-acceptable.**

## Cross-references

- HM-BK closure: prior singleton work in `engine/alpaca_bridge.py:185` + total_portfolio.py:194-198 comment
- Plist roster: `~/Library/LaunchAgents/com.trademinds.*.plist` + `com.ollietrades.real-portfolio-snapshot.plist`
