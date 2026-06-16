#!/usr/bin/env bash
# HM-SHIELDS — PreToolUse paper-only safety guard. Pure bash + grep -E (portable; no -P,
# no heredoc-in-$()). Reads the PreToolUse JSON on stdin; BLOCKS (exit 2, reason to stderr)
# any tool call that routes to a LIVE order path / flips paper->live / exfils secrets.
# Trick: neutralize the paper host FIRST so the live-order match can't false-trip on paper-api.
payload="$(cat)"
block() { printf '[HM-SHIELDS] BLOCKED -- %s. Paper-only invariant enforced.\n' "$1" >&2; exit 2; }

# Neutralize known-safe paper host (paper orders + data feed are allowed).
scrub="${payload//paper-api.alpaca.markets/PAPER_HOST}"

# --- Live ROUTING signatures (command + edit/write content) ---
printf '%s' "$scrub"   | grep -Eq 'api\.alpaca\.markets/v2/orders' && block "LIVE order submission"
printf '%s' "$scrub"   | grep -Eq '(_BASE|BASE_URL|base_url|TRADING_BASE)[[:space:]]*=[[:space:]"'"'"'\\]*https://api\.alpaca\.markets' && block "flip trading base to LIVE"
printf '%s' "$payload" | grep -Eq 'ALPACA_LIVE|real[_-]?money|paper[[:space:]]*=[[:space:]]*False' && block "live-trading / real-money flag"

# --- Secret-exfil idioms (Bash command only, so code content does not false-trip) ---
if printf '%s' "$payload" | grep -Eq '"tool_name"[[:space:]]*:[[:space:]]*"Bash"'; then
  printf '%s' "$payload" | grep -Eq '(^|[^[:alnum:]_])env[[:space:]]*\|'              && block "env dump (secret exfil)"
  printf '%s' "$payload" | grep -Eq 'cat[^\\]*\.env'                                   && block "cat .env (secret exfil)"
  printf '%s' "$payload" | grep -Eq 'grep[^\\]*(ALPACA_|SCHWAB_|CF_[A-Za-z_]*token)'   && block "grep secrets (exfil)"
fi

exit 0
