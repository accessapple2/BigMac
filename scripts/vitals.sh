#!/bin/bash
# vitals.sh — full hardware + network + trend monitor for bigmac + ollie
# Usage: ./scripts/vitals.sh          (one-shot)
#        watch -n 5 ./scripts/vitals.sh   (live refresh every 5s)
#
# Writes temp samples to data/vitals_history.log for trend display

HISTORY=~/autonomous-trader/data/vitals_history.log
mkdir -p ~/autonomous-trader/data

# Capture GPU temp for history tracking
NOW=$(date '+%Y-%m-%d %H:%M:%S')
OLLIE_GPU_TEMP=$(ssh -o ConnectTimeout=2 -o BatchMode=yes -o StrictHostKeyChecking=no \
  bigmac@192.168.1.166 \
  "nvidia-smi --query-gpu=temperature.gpu --format=csv,noheader,nounits 2>/dev/null" 2>/dev/null)
if [ -n "$OLLIE_GPU_TEMP" ] && [ "$OLLIE_GPU_TEMP" -eq "$OLLIE_GPU_TEMP" ] 2>/dev/null; then
  echo "$NOW,ollie_gpu,$OLLIE_GPU_TEMP" >> "$HISTORY"
fi

clear
echo "╔════════════════════════════════════════════════════════════╗"
printf  "║  FLEET VITALS  |  %-41s║\n" "$NOW"
echo "╚════════════════════════════════════════════════════════════╝"

# ── BIGMAC ──────────────────────────────────────────────────────
echo ""
echo "┌─ BIGMAC ────────────────────────────────────────────────┐"

FREE_PCT=$(memory_pressure 2>/dev/null | grep "System-wide" | grep -oE "[0-9]+%")
CPU_LOAD=$(uptime | grep -oE "load averages: [0-9.]+ [0-9.]+ [0-9.]+" | cut -d: -f2- | xargs)
DISK=$(df -h / 2>/dev/null | awk 'NR==2 {printf "%s used / %s total (%s free)", $3, $2, $4}')

printf "│ RAM free:    %-47s│\n" "${FREE_PCT:-unknown}"
printf "│ Load avg:    %-47s│\n" "${CPU_LOAD:-unknown} (1m 5m 15m)"
printf "│ Disk /:      %-47s│\n" "${DISK:-unknown}"

# LAN interface + lifetime bytes
LAN_IF=$(route -n get 192.168.1.166 2>/dev/null | grep interface | awk '{print $2}')
if [ -n "$LAN_IF" ]; then
  NET_IN=$(netstat -ib 2>/dev/null | awk -v iface="$LAN_IF" \
    '$1==iface && $NF~/^[0-9]+$/ {print $7; exit}')
  NET_OUT=$(netstat -ib 2>/dev/null | awk -v iface="$LAN_IF" \
    '$1==iface && $NF~/^[0-9]+$/ {print $10; exit}')
  if [ -n "$NET_IN" ] && [ -n "$NET_OUT" ]; then
    NET_STR=$(awk "BEGIN {printf \"in=%.1f MB  out=%.1f MB\", $NET_IN/1048576, $NET_OUT/1048576}")
  else
    NET_STR="(unavailable)"
  fi
  printf "│ LAN %-3s:     %-47s│\n" "$LAN_IF" "$NET_STR (lifetime)"
fi

# Sockets
SOCKETS=$(lsof -iTCP -sTCP:ESTABLISHED 2>/dev/null | wc -l | tr -d ' ')
LISTEN=$(lsof -iTCP -sTCP:LISTEN 2>/dev/null | wc -l | tr -d ' ')
printf "│ Sockets:     %-47s│\n" "${SOCKETS} established / ${LISTEN} listening"

echo "└──────────────────────────────────────────────────────────┘"

# ── OLLIE ───────────────────────────────────────────────────────
echo ""
echo "┌─ OLLIE (192.168.1.166) ─────────────────────────────────┐"

OLLIE_OUT=$(ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no \
  bigmac@192.168.1.166 '
GPU=$(nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw \
  --format=csv,noheader,nounits 2>/dev/null)
if [ -n "$GPU" ]; then
  echo "$GPU" | awk -F", " \
    "{printf \"GPU util:   %s%%\nVRAM:       %s / %s MiB\nGPU temp:   %s°C\nGPU power:  %s W\n\", \$1, \$2, \$3, \$4, \$5}"
else
  echo "GPU:        (nvidia-smi unavailable)"
fi

MEM=$(free -h 2>/dev/null | awk "/^Mem:/ {print \$3 \" used / \" \$2 \" total (\" \$7 \" avail)\"}")
echo "RAM:        ${MEM:-(unavailable)}"

LOAD=$(uptime 2>/dev/null | grep -oE "load average: [0-9.]+, [0-9.]+, [0-9.]+" | sed "s/load average: //")
echo "Load avg:   ${LOAD:-(unavailable)} (1m 5m 15m)"

DISK=$(df -h / 2>/dev/null | awk "NR==2 {printf \"%s used / %s total (%s free)\", \$3, \$2, \$4}")
echo "Disk /:     ${DISK:-(unavailable)}"

MODEL_DISK=$(du -sh /usr/share/ollama/.ollama/models 2>/dev/null | awk "{print \$1}" || \
             du -sh ~/.ollama/models 2>/dev/null | awk "{print \$1}")
echo "Ollama:     ${MODEL_DISK:-(unavailable)} models on disk"

LOADED=$(ollama ps 2>/dev/null | tail -n +2 | awk "{print \$1, \$5}" | head -1)
echo "Model:      ${LOADED:-(none loaded)}"

OLLIE_EST=$(ss -tn state established 2>/dev/null | tail -n +2 | wc -l)
OLLIE_LSN=$(ss -tnl 2>/dev/null | tail -n +2 | wc -l)
echo "Sockets:    $OLLIE_EST established / $OLLIE_LSN listening"

CPU_TEMP=$(sensors 2>/dev/null | grep -E "Tctl:|Package id" | head -1 | awk "{print \$2}" | tr -d "+")
if [ -n "$CPU_TEMP" ]; then
  echo "CPU temp:   $CPU_TEMP"
fi
' 2>/dev/null)

if [ -z "$OLLIE_OUT" ]; then
  printf "│ %-58s│\n" "UNREACHABLE"
else
  while IFS= read -r line; do
    printf "│ %-58s│\n" "$line"
  done <<< "$OLLIE_OUT"
fi

echo "└──────────────────────────────────────────────────────────┘"

# ── GPU TEMP TREND (last 24hr) ──────────────────────────────────
echo ""
echo "┌─ OLLIE GPU TEMP — LAST 24HR ────────────────────────────┐"

if [ -f "$HISTORY" ] && [ -s "$HISTORY" ]; then
  # macOS date -v-24H; Linux date -d
  ONE_DAY_AGO=$(date -v-24H '+%Y-%m-%d %H:%M:%S' 2>/dev/null || \
                date -d '24 hours ago' '+%Y-%m-%d %H:%M:%S' 2>/dev/null)

  TREND=$(grep ",ollie_gpu," "$HISTORY" | \
    awk -F"," -v cutoff="$ONE_DAY_AGO" '$1 >= cutoff {print $1, $3}' | \
    awk '{
      hour = substr($2, 1, 2)
      temp = $3 + 0
      sum[hour] += temp
      cnt[hour]++
      if (temp > mx[hour]) mx[hour] = temp
      if (mn[hour] == 0 || temp < mn[hour]) mn[hour] = temp
    }
    END {
      for (h in sum) {
        avg = int(sum[h] / cnt[h])
        bar = ""
        steps = int((avg - 30) / 3)
        if (steps < 0) steps = 0
        if (steps > 20) steps = 20
        for (i = 0; i < steps; i++) bar = bar "█"
        printf "%s %3d %3d %3d %s\n", h, avg, mn[h], mx[h], bar
      }
    }' | sort | \
    awk '{printf "│ %s:00  avg=%d°C [%d..%d]  %s\n", $1, $2, $3, $4, $5}')

  if [ -n "$TREND" ]; then
    echo "$TREND"
  else
    printf "│ %-58s│\n" "No readings in last 24hr yet"
  fi

  SAMPLES=$(grep -c ",ollie_gpu," "$HISTORY" 2>/dev/null || echo 0)
  printf "│ %-58s│\n" "$SAMPLES total samples in history"
else
  printf "│ %-58s│\n" "No history yet — run again in a few minutes for trend"
fi

echo "└──────────────────────────────────────────────────────────┘"
echo ""
