#!/usr/bin/env bash
#
# friday_open_close.sh — pre-Saturday Sniper closeout (5 fractional longs)
#
# Closes ollie-auto's 5 open Sniper positions on Alpaca paper before
# tomorrow's KILL window. Default mode is --dry-run. Execution requires:
#
#   bash scripts/friday_open_close.sh --execute MARKET-OPEN
#   then interactive "CLOSE" typed at the confirm prompt.
#
# Hard refusals:
#   - Wrong date (must be 2026-05-08 Fri)
#   - Position set drifted from expected 5
#   - Confirmation token != "CLOSE"
#
# Block-waits until NYSE open (9:30 ET = 13:30 UTC during US DST), then
# submits 5 MKT SELL orders with client_order_id
# sniper-closeout-<SYMBOL>-20260508. After 90s, pulls fill status and
# ntfys ollietrades-admin.
#
# IMPORTANT (Scotty 3.1 fix): Arizona does NOT observe DST. AZ is UTC-7
# year-round. NYSE open in May (EDT) is 13:30 UTC = 06:30 AZ — NOT
# 07:30 AZ. The original v1 anchored to a literal "07:30:00 MST" string
# which would have fired one hour LATE. v2 anchors to 13:30 UTC, which
# is unambiguous and DST-safe.
#

set -u

ROOT="${HOME}/autonomous-trader"
PY="${ROOT}/venv/bin/python3"
LOG_DIR="${ROOT}/logs"
NTFY_TOPIC="ollietrades-admin"
TS_RUN=$(date +%Y%m%dT%H%M%S)
LOG_FILE="${LOG_DIR}/friday_open_close_${TS_RUN}.log"
TODAY="2026-05-08"
# Scotty 3.1 fix: anchor to 13:30 UTC (= 9:30 ET in DST = 6:30 AZ year-round).
# AZ is UTC-7 with no DST, so a literal "07:30 AZ" string was 1 hour late.
# 13:30 UTC is unambiguous and DST-safe.
TARGET_UTC_DATE="$(date -u +%Y-%m-%d)"
TARGET_EPOCH=$(TZ=UTC date -j -f "%Y-%m-%d %H:%M:%S" "${TARGET_UTC_DATE} 13:30:00" "+%s" 2>/dev/null)

# Expected position set (symbol qty)
EXPECTED_LIST="WFC:2.99 LNG:0.98 AMGN:0.37 GS:0.13 JPM:0.41"

mkdir -p "${LOG_DIR}"

log() {
    local ts="$(date '+%Y-%m-%dT%H:%M:%S%z')"
    printf '[%s] %s\n' "${ts}" "$*" | tee -a "${LOG_FILE}"
}

ntfy() {
    local title="$1" body="$2" prio="${3:-default}"
    local safe_title="$(printf '%s' "${title}" | iconv -f utf-8 -t ascii//TRANSLIT 2>/dev/null || printf '%s' "${title}")"
    curl -s -H "Title: ${safe_title}" -H "Priority: ${prio}" \
         -d "${body}" "https://ntfy.sh/${NTFY_TOPIC}" >/dev/null 2>&1 || true
}

die() {
    log "FATAL: $*"
    ntfy "Friday close aborted" "$*" "default"
    exit 1
}

# --- Args ---
MODE="dry-run"
EXEC_TOKEN=""
for a in "$@"; do
    case "${a}" in
        --dry-run)        MODE="dry-run" ;;
        --execute)        MODE="execute" ;;
        MARKET-OPEN)      EXEC_TOKEN="MARKET-OPEN" ;;
        -h|--help)
            cat <<EOF
Usage: bash scripts/friday_open_close.sh [--dry-run | --execute MARKET-OPEN]

  --dry-run                 (default) verify positions match, print order
                            plan, do nothing
  --execute MARKET-OPEN     after CLOSE prompt, block until 13:30 UTC
                            (= 9:30 ET = 6:30 AZ year-round; AZ has no
                            DST), then submit 5 MKT SELL orders, poll
                            fills, ntfy summary

Refuses to run on any date other than 2026-05-08 (Fri).
EOF
            exit 0 ;;
        *) die "unknown arg: ${a}" ;;
    esac
done

if [ "${MODE}" = "execute" ] && [ "${EXEC_TOKEN}" != "MARKET-OPEN" ]; then
    die "--execute requires the literal token MARKET-OPEN"
fi

log "================================================================"
log "Friday open close orchestrator (mode=${MODE})"
log "Working dir: ${ROOT}"
log "Log file: ${LOG_FILE}"
log "================================================================"

# --- Pre-flight 1: date ---
TODAY_NOW="$(date +%Y-%m-%d)"
DOW="$(date +%u)"  # 1=Mon ... 7=Sun
log "Pre-flight 1  date=${TODAY_NOW} dow=${DOW}"
if [ "${TODAY_NOW}" != "${TODAY}" ]; then
    die "Pre-flight 1 ❌  expected date ${TODAY}, got ${TODAY_NOW}"
fi
if [ "${DOW}" != "5" ]; then
    die "Pre-flight 1 ❌  expected DOW=5 (Fri), got ${DOW}"
fi
log "Pre-flight 1 ✓  Friday 2026-05-08 confirmed"

# --- Pre-flight 2: pull positions from Alpaca + verify match ---
log "Pre-flight 2  pulling current Alpaca positions..."
POS_JSON="$(${PY} - 2>/dev/null <<'PYEOF'
import os, sys, json, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.expanduser("~/autonomous-trader"))
from dotenv import load_dotenv
load_dotenv(os.path.expanduser("~/autonomous-trader/.env"))
try:
    from alpaca.trading.client import TradingClient
    key = os.getenv("APCA_API_KEY_ID","")
    sec = os.getenv("APCA_API_SECRET_KEY","")
    if not key or not sec:
        print(json.dumps({"_error": "APCA_API_KEY_ID/APCA_API_SECRET_KEY missing"}))
        sys.exit(0)
    client = TradingClient(key, sec, paper=True)
    out = []
    for p in client.get_all_positions():
        out.append({
            "symbol": p.symbol,
            "qty": float(p.qty),
            "side": p.side.value if hasattr(p.side, 'value') else str(p.side),
            "avg_entry_price": float(p.avg_entry_price),
            "current_price": float(p.current_price) if p.current_price else None,
            "unrealized_pl": float(p.unrealized_pl) if p.unrealized_pl else 0.0,
        })
    print(json.dumps(out))
except Exception as e:
    print(json.dumps({"_error": f"{type(e).__name__}: {e}"}))
PYEOF
)"

if printf '%s' "${POS_JSON}" | grep -q '"_error"'; then
    die "Pre-flight 2 ❌  Alpaca query failed: ${POS_JSON}"
fi

log "Pre-flight 2  raw positions JSON size: $(printf '%s' "${POS_JSON}" | wc -c) bytes"

# Parse + diff via python helper.
# Diff semantics (per CLAUDE.md Two-Book Bridge Policy):
#   - Alpaca paper book holds positions for MULTIPLE routed players
#     (super-agent, neo-matrix via id=7, spread strategies, plus the 5
#     ollie-auto Sniper picks we want to close).
#   - "Drift" means: any of the 5 EXPECTED ollie-auto Sniper symbols
#     missing OR with qty mismatch. Extra Alpaca positions from other
#     routed players are EXPECTED and NOT drift — they will be
#     untouched by the symbol-specific MKT SELL orders below.
DIFF_OUT="$(POS_JSON_ENV="${POS_JSON}" EXPECTED_ENV="${EXPECTED_LIST}" ${PY} - 2>/dev/null <<'PYEOF'
import os, json, warnings
warnings.filterwarnings("ignore")
positions = json.loads(os.environ["POS_JSON_ENV"])
expected = {}
for kv in os.environ["EXPECTED_ENV"].split():
    sym, qty = kv.split(":")
    expected[sym] = float(qty)

actual = {p["symbol"]: p["qty"] for p in positions}
price_map = {p["symbol"]: (p.get("current_price"), p.get("unrealized_pl")) for p in positions}

ok = True
lines = []
for sym, exp_qty in sorted(expected.items()):
    if sym not in actual:
        lines.append(f"  MISSING: {sym} expected qty={exp_qty}")
        ok = False
    else:
        diff = abs(actual[sym] - exp_qty)
        marker = "OK" if diff < 0.01 else f"DRIFT (delta={diff:.4f})"
        if diff >= 0.01: ok = False
        lines.append(f"  {sym:6s} actual={actual[sym]:>8.4f}  expected={exp_qty:>6.2f}  {marker}")

# Extras = Alpaca positions not in our expected set. NOT drift — these
# belong to other routed players and will be untouched.
extras = sorted(set(actual.keys()) - set(expected.keys()))

print("MATCH" if ok else "DRIFT")
print("---")
for line in lines:
    print(line)
print("---")
print("Live quotes (Alpaca current_price + uPnL) for expected 5:")
for sym in sorted(expected.keys()):
    cp, upl = price_map.get(sym, (None, None))
    cp_s = f"${cp:.2f}" if cp is not None else "N/A"
    upl_s = f"${upl:+.2f}" if upl is not None else "N/A"
    print(f"  {sym:6s} px={cp_s}  uPnL={upl_s}")
print("---")
if extras:
    print(f"Other Alpaca positions (NOT touched — owned by other routed players): {len(extras)}")
    for sym in extras:
        cp, upl = price_map.get(sym, (None, None))
        cp_s = f"${cp:.2f}" if cp is not None else "N/A"
        print(f"  {sym:25s} qty={actual[sym]:>8.2f}  px={cp_s}")
PYEOF
)"

POS_STATUS="$(printf '%s\n' "${DIFF_OUT}" | head -1)"
log "Pre-flight 2  position diff:"
printf '%s\n' "${DIFF_OUT}" | tail -n +2 | sed 's/^/  /' | tee -a "${LOG_FILE}"

if [ "${POS_STATUS}" != "MATCH" ]; then
    if [ "${MODE}" = "execute" ]; then
        ntfy "Friday close DRIFT" "Position set drifted from expected 5. See ${LOG_FILE}. Refusing to fire." "high"
        die "Pre-flight 2 ❌  position drift; refusing to fire"
    else
        log "Pre-flight 2 ⚠️   position drift detected (would block --execute)"
        ntfy "Friday close DRIFT (dry-run)" "Position set drifted from expected 5 during dry-run. See ${LOG_FILE}." "high"
    fi
else
    log "Pre-flight 2 ✓  all 5 positions match expected qtys"
fi

# --- Order construction preview ---
log ""
log "──────── Order Plan (5 MKT SELL orders @ 13:30 UTC = 9:30 ET = 6:30 AZ) ────────"
for kv in ${EXPECTED_LIST}; do
    sym="${kv%:*}"
    qty="${kv#*:}"
    log "  symbol=${sym}  qty=${qty}  side=sell  type=market  tif=day  client_order_id=sniper-closeout-${sym}-20260508"
done
log "──────────────────────────────────────────────────────────────"

# --- Mode branch ---
if [ "${MODE}" = "dry-run" ]; then
    log ""
    log "Mode is dry-run — NO orders submitted."
    log "Run with --execute MARKET-OPEN on Friday before 13:30 UTC (6:30 AZ) to perform."
    log "================================================================"
    exit 0
fi

# --- Interactive CLOSE confirmation ---
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "About to close 5 ollie-auto Sniper positions at NYSE open (13:30 UTC = 6:30 AZ)."
echo "Type  CLOSE  (capitals, no quotes) to proceed, anything else to abort:"
echo "═══════════════════════════════════════════════════════════════════"
read -r CONFIRM
if [ "${CONFIRM}" != "CLOSE" ]; then
    log "User did not type CLOSE (got: '${CONFIRM}'). Aborting."
    exit 1
fi
log "Confirmation received: CLOSE"

# --- Block-wait until 13:30 UTC (NYSE open during DST = 6:30 AZ) ---
NOW_EPOCH="$(date +%s)"
WAIT_SEC=$(( TARGET_EPOCH - NOW_EPOCH ))
log "Block-wait: target ${TARGET_UTC_DATE} 13:30:00 UTC (epoch ${TARGET_EPOCH})"
log "  current UTC: $(date -u '+%Y-%m-%d %H:%M:%S')"
log "  current AZ:  $(date '+%Y-%m-%d %H:%M:%S %Z')"
log "  now epoch=${NOW_EPOCH}  wait=${WAIT_SEC}s"

if [ "${WAIT_SEC}" -gt 0 ]; then
    log "Sleeping ${WAIT_SEC}s..."
    sleep "${WAIT_SEC}"
elif [ "${WAIT_SEC}" -lt -300 ]; then
    die "Already 5+ min past 13:30 UTC (NYSE open) — aborting; run individual close orders manually instead"
else
    log "Already past target by $((-WAIT_SEC))s — proceeding"
fi

log "Firing orders at $(date '+%Y-%m-%d %H:%M:%S %Z')"

# --- Submit 5 MKT SELL orders + poll fills (90s) ---
SUBMIT_OUT="$(${PY} - <<PYEOF 2>&1
import os, sys, json, time
sys.path.insert(0, os.path.expanduser("~/autonomous-trader"))
from dotenv import load_dotenv
load_dotenv(os.path.expanduser("~/autonomous-trader/.env"))
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

key = os.getenv("APCA_API_KEY_ID","")
sec = os.getenv("APCA_API_SECRET_KEY","")
client = TradingClient(key, sec, paper=True)

orders_to_submit = [
    ("WFC", 2.99),
    ("LNG", 0.98),
    ("AMGN", 0.37),
    ("GS", 0.13),
    ("JPM", 0.41),
]

submitted = []
errors = []
for sym, qty in orders_to_submit:
    try:
        req = MarketOrderRequest(
            symbol=sym,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
            client_order_id=f"sniper-closeout-{sym}-20260508",
        )
        o = client.submit_order(req)
        submitted.append({"symbol": sym, "qty": qty, "order_id": str(o.id), "client_order_id": o.client_order_id, "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")})
    except Exception as e:
        errors.append({"symbol": sym, "qty": qty, "error": f"{type(e).__name__}: {e}"})

print("=== SUBMITTED ===")
for s in submitted:
    print(json.dumps(s))
print("=== ERRORS ===")
for e in errors:
    print(json.dumps(e))

# Poll fills for up to 90s
print("=== POLLING FILLS (90s) ===")
deadline = time.time() + 90
fills = {}
order_ids = [s["order_id"] for s in submitted]
while time.time() < deadline and len(fills) < len(order_ids):
    for oid in order_ids:
        if oid in fills: continue
        try:
            o = client.get_order_by_id(oid)
            status = str(o.status.value if hasattr(o.status,'value') else o.status)
            if status in ("filled", "partially_filled", "canceled", "rejected", "expired"):
                fills[oid] = {
                    "symbol": o.symbol,
                    "status": status,
                    "filled_qty": float(o.filled_qty or 0),
                    "filled_avg_price": float(o.filled_avg_price) if o.filled_avg_price else None,
                }
        except Exception as ex:
            pass
    time.sleep(2)

# Final read for any leftover
for oid in order_ids:
    if oid not in fills:
        try:
            o = client.get_order_by_id(oid)
            fills[oid] = {
                "symbol": o.symbol,
                "status": str(o.status.value if hasattr(o.status,'value') else o.status),
                "filled_qty": float(o.filled_qty or 0),
                "filled_avg_price": float(o.filled_avg_price) if o.filled_avg_price else None,
            }
        except Exception:
            fills[oid] = {"symbol": "?", "status": "unknown"}

print("=== FILLS ===")
total_filled = 0
for oid, info in fills.items():
    print(json.dumps({"order_id": oid, **info}))
    if info.get("status") == "filled":
        total_filled += 1
print(f"=== TOTAL: {total_filled}/{len(order_ids)} filled ===")
PYEOF
)"

log "Submission + poll output:"
printf '%s\n' "${SUBMIT_OUT}" | sed 's/^/  /' | tee -a "${LOG_FILE}"

# Extract summary
TOTAL_LINE="$(printf '%s\n' "${SUBMIT_OUT}" | grep "^=== TOTAL:" | tail -1)"
SUMMARY="${TOTAL_LINE:-(no summary)}"

ntfy "Sniper closeout complete" \
    "${SUMMARY} — ${LOG_FILE}" \
    "default"

log "================================================================"
log "Friday open close orchestrator complete."
log "Summary: ${SUMMARY}"
log "================================================================"
exit 0
