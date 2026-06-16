#!/usr/bin/env bash
# HM-GAUGE — colorblind-safe Claude Code statusline.
# Admiral is colorblind: BLUE + ORANGE/AMBER only, NEVER red/green. Color is SECONDARY —
# every state also shows a number/text label, so the line is fully legible with color stripped.
# Reads the status JSON on stdin (schema: code.claude.com/docs/en/statusline). null-safe via jq //.
input="$(cat)"

# --- extract fields (null-safe) ---
model="$(printf '%s' "$input"  | jq -r '.model.display_name // "?"')"
pct="$(printf '%s' "$input"    | jq -r '.context_window.used_percentage // 0' | cut -d. -f1)"
cost="$(printf '%s' "$input"   | jq -r '.cost.total_cost_usd // 0')"
dir="$(printf '%s' "$input"    | jq -r '.workspace.current_dir // .cwd // "."')"

# git branch is NOT in the JSON — derive it; fall back to a text label (never blank).
branch="$(git -C "$dir" branch --show-current 2>/dev/null)"
[ -z "$branch" ] && branch="$(git -C "$dir" rev-parse --short HEAD 2>/dev/null)"
[ -z "$branch" ] && branch="no-git"

# format cost: $1.2345 -> 2dp when >=1, else 4dp (keep sub-cent visible for dev sessions)
cost_fmt="$(awk -v c="$cost" 'BEGIN{ if (c+0 >= 1) printf "%.2f", c; else printf "%.4f", c }')"

# --- palette: two hues only (blue / amber), tuned for the light-daltonized theme ---
BLUE=$'\033[38;5;26m'     # deep blue
AMBER=$'\033[38;5;208m'   # orange/amber (clearly distinct from blue; not red)
BOLD=$'\033[1m'
DIM=$'\033[2m'
RST=$'\033[0m'

# context %: number ALWAYS shown; hue shades blue->amber as it climbs, escalation via WEIGHT
# (bold) at the top end — still amber, never a new/red hue. Color is emphasis, not the signal.
if   [ "$pct" -ge 80 ] 2>/dev/null; then ctx_color="${AMBER}${BOLD}"
elif [ "$pct" -ge 50 ] 2>/dev/null; then ctx_color="${AMBER}"
else                                     ctx_color="${BLUE}"
fi

printf '%s · ctx %s%% · $%s · %s\n' \
  "${BLUE}${BOLD}${model}${RST}" \
  "${ctx_color}${pct}${RST}" \
  "${AMBER}${cost_fmt}${RST}" \
  "${DIM}${branch}${RST}"
