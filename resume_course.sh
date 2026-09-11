#!/bin/bash
echo "================================================"
echo " RESUME COURSE — OllieTrades Fleet Check"
echo " $(date)"
echo "================================================"

CD=/Users/bigmac/autonomous-trader
cd $CD

echo ""
echo "[ 1/7 ] Checking Python processes..."
PROCS=$(ps aux | grep python | grep -v grep | wc -l)
if [ "$PROCS" -gt 0 ]; then
  echo "  ✓ $PROCS process(es) running"
else
  echo "  ✗ No processes found — starting main.py..."
  source venv/bin/activate
  nohup python3 main.py >> logs/main.log 2>&1 &
  echo "  ✓ main.py started"
fi

echo ""
echo "[ 2/7 ] Checking Ollama..."
# HM-HARDCODED-HOST-SWEEP-2026-09-10: was hardcoded to the decommissioned
# pre-migration Ollie Box (.166) with no env indirection at all. Fails
# loud if OLLIE_URL isn't in .env rather than silently checking a dead host.
OLLIE_HOST=$(grep '^OLLIE_URL=' "$CD/.env" | cut -d= -f2-)
if [ -z "$OLLIE_HOST" ]; then
  echo "  ✗ OLLIE_URL not set in .env — cannot check Ollama"
else
  OLLAMA=$(curl -s --max-time 5 "$OLLIE_HOST" | grep -c "Ollama" || true)
  if [ "$OLLAMA" -gt 0 ]; then
    echo "  ✓ Ollie ($OLLIE_HOST) responding"
  else
    echo "  ✗ Ollie unreachable — check $OLLIE_HOST manually"
  fi
fi

echo ""
echo "[ 3/7 ] Checking launchd jobs..."
for JOB in morningbriefing etfregime optionsflow ghosttrader; do
  STATUS=$(launchctl list | grep "ollietrades.$JOB" | awk '{print $1}')
  if [ "$STATUS" = "-" ] || [ -z "$STATUS" ]; then
    echo "  ✗ $JOB not running — starting..."
    launchctl start com.ollietrades.$JOB
  else
    echo "  ✓ $JOB running (PID $STATUS)"
  fi
done

echo ""
echo "[ 4/7 ] Checking dashboard port 8080..."
PORT=$(lsof -i :8080 | grep LISTEN | wc -l)
if [ "$PORT" -gt 0 ]; then
  echo "  ✓ Dashboard online at port 8080"
else
  echo "  ✗ Dashboard not responding"
fi

echo ""
echo "[ 5/7 ] Checking Signal Center port 9000..."
PORT9=$(lsof -i :9000 | grep LISTEN | wc -l)
if [ "$PORT9" -gt 0 ]; then
  echo "  ✓ Signal Center online at port 9000"
else
  echo "  ✗ Signal Center not responding"
fi

echo ""
echo "[ 6/7 ] Checking database health..."
DB=$CD/arena.db
if [ -f "$DB" ]; then
  SIZE=$(du -sh "$DB" | cut -f1)
  echo "  ✓ arena.db exists ($SIZE)"
else
  echo "  ✗ arena.db missing — check immediately"
fi
DB2=$CD/trader.db
if [ -f "$DB2" ]; then
  SIZE2=$(du -sh "$DB2" | cut -f1)
  echo "  ✓ trader.db exists ($SIZE2)"
else
  echo "  ✗ trader.db missing — check immediately"
fi

echo ""
echo "[ 7/7 ] Checking Alpaca connection..."
ALPACA=$(curl -s https://api.alpaca.markets/v2/clock \
  -H "APCA-API-KEY-ID: $(grep APCA_API_KEY_ID $CD/.env | cut -d= -f2)" \
  -H "APCA-API-SECRET-KEY: $(grep APCA_API_SECRET_KEY $CD/.env | cut -d= -f2)" \
  | grep -c "is_open" || true)
if [ "$ALPACA" -gt 0 ]; then
  echo "  ✓ Alpaca API connected"
else
  echo "  ✗ Alpaca API not responding — check keys"
fi

echo ""
echo "================================================"
echo " FLEET STATUS SUMMARY"
echo "================================================"
ps aux | grep python | grep -v grep | awk '{print "  Running: " $11 " " $12}'
echo ""
echo "  Make it so. 🖖"
echo "================================================"
