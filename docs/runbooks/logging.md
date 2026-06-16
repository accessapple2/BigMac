# logging.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## Logging Sink Split (trader.log vs trader_error.log)

OllieTrades logs to two files with different sinks:

| File | Sink | What goes here |
|---|---|---|
| `logs/trader.log` | Rich `console.log(...)` calls | Per-cycle agent output, strategy ticks, market data, formatted user-facing log lines |
| `logs/trader_error.log` | Python `logger.info / .warning / .error` calls | Structured Python logging — including `engine.alert_channels` NTFY dispatch logs |

**Implication for investigations:** when checking whether a NTFY actually
fired, search `trader_error.log` for entries like:
- `[LRS] Alert dispatched [warning/{alert_type}]: {message}`
- `[LRS] ntfy sent [200]: ?? TradeMinds {Level}`

Searching only `trader.log` will miss NTFY firings — they POST at HTTP 200
and produce correctly; they just land in the other file.
