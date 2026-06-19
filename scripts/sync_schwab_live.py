#!/opt/homebrew/bin/python3
"""HM-SCHWAB-LIVE-SYNC — primary live-API sync for real_holdings.json (Schwab block).

Pulls live positions+balances from the Schwab API (read-only GET) and writes ONLY
the 'schwab' account block in data/real_holdings.json. CSV snapshot
(sync_schwab_to_real_holdings.py) remains the fallback, triggered by cron's `||`.

DOCTRINE (CLAUDE.md): Schwab is real cash and OUT OF THE FLEET LOOP. This script
is read-only reporting only — it GETs account data and writes a local JSON file.
It NEVER places an order, never routes to any agent/scanner/trade-signal path.

Interpreter: MUST run under /opt/homebrew/bin/python3 — the `schwab` package lives
there, NOT in the project .venv/venv.

Exit codes (drive cron fallback):
  0  success (wrote file) OR outside market hours / weekend (clean skip — no fallback)
  1  API failure DURING market hours (no write) → cron `||` runs the CSV fallback
"""
from __future__ import annotations
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
JSON_PATH = REPO_ROOT / "data" / "real_holdings.json"
ACCOUNT_LAST4 = os.environ.get("SCHWAB_ACCOUNT_LAST4", "7015")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [schwab-live-sync] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# Cron has no cwd / env — make engine.schwab_client importable and load the repo .env
# so SCHWAB_CLIENT_ID/SECRET resolve regardless of where cron invokes us.
sys.path.insert(0, str(REPO_ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass


def _et_market_open(now_et: datetime) -> bool:
    """True only during regular trading hours: Mon-Fri 09:30-16:00 ET."""
    if now_et.weekday() >= 5:           # Sat/Sun
        return False
    minutes = now_et.hour * 60 + now_et.minute
    return (9 * 60 + 30) <= minutes < (16 * 60)


def _map_position(p: dict) -> dict:
    """Map a Schwab API position to the real_holdings.json position shape
    (compatible with sync_schwab_to_real_holdings.py). grade/smart_score are
    CSV-path enrichments not available live → left None."""
    inst = p.get("instrument", {}) or {}
    lq = float(p.get("longQuantity", 0) or 0)
    sq = float(p.get("shortQuantity", 0) or 0)
    qty = lq - sq
    mkt_val = p.get("marketValue")
    avg_cost = p.get("averagePrice")
    price = round(mkt_val / qty, 4) if (mkt_val is not None and qty) else None
    gain_d = p.get("longOpenProfitLoss", p.get("currentDayProfitLoss"))
    avg_cost_f = float(avg_cost) if avg_cost is not None else None
    cost_basis = round(avg_cost_f * qty, 4) if (avg_cost_f is not None and qty) else None
    gain_dollar_f = round(float(gain_d), 2) if gain_d is not None else None
    gain_pct = round(gain_dollar_f / cost_basis * 100, 2) if (gain_dollar_f is not None and cost_basis) else None
    return {
        "symbol": inst.get("symbol"),
        "qty": qty,
        "avg_cost": round(avg_cost_f, 4) if avg_cost_f is not None else None,
        "price": price,
        "market_value": round(float(mkt_val), 2) if mkt_val is not None else None,
        "gain_dollar": gain_dollar_f,
        "gain_pct": gain_pct,
        "day_change_pct": (round(float(p["currentDayProfitLossPercentage"]), 2)
                           if p.get("currentDayProfitLossPercentage") is not None else None),
        "day_change_dollar": (round(float(p["currentDayProfitLoss"]), 2)
                              if p.get("currentDayProfitLoss") is not None else None),
        "grade": None,
        "smart_score": None,
        "notes": f"live_api position; asset_type={inst.get('assetType')}",
    }


def main() -> int:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if not _et_market_open(now_et):
        log.info("outside RTH (%s ET) — skip, no write, no fallback", now_et.strftime("%a %H:%M"))
        return 0

    if not JSON_PATH.exists():
        log.warning("%s not found — cannot update; deferring to fallback", JSON_PATH)
        return 1

    # ── Read-only live pull ────────────────────────────────────────────────
    try:
        from engine.schwab_client import get_client
        from schwab.client import Client
        client = get_client()
        resp = client.get_accounts(fields=Client.Account.Fields.POSITIONS)
        accounts = resp.json()
    except Exception as e:
        log.warning("live API failed (%s: %s) — no write; cron fallback to CSV", type(e).__name__, e)
        return 1

    # Pick the brokerage account by last4 (never log the full number)
    target = None
    for a in accounts or []:
        sa = a.get("securitiesAccount", a)
        if str(sa.get("accountNumber", "")).endswith(ACCOUNT_LAST4):
            target = sa
            break
    if target is None and accounts:
        target = (accounts[0].get("securitiesAccount", accounts[0]))
    if target is None:
        log.warning("no Schwab account in API response — no write; cron fallback")
        return 1

    bal = target.get("currentBalances", {}) or {}
    cash = bal.get("cashBalance", bal.get("totalCash", bal.get("cashAvailableForTrading", 0))) or 0
    raw_positions = target.get("positions", []) or []
    positions = [_map_position(p) for p in raw_positions]
    last4 = str(target.get("accountNumber", ""))[-4:]
    acct_type = target.get("type", "")

    # Portfolio-level totals (cost-basis return, not live-price return)
    total_cost_basis = sum((p.get("avg_cost") or 0) * (p.get("qty") or 0) for p in positions)
    total_gain_dollar = sum(p.get("gain_dollar") or 0 for p in positions)
    total_gain_pct = round(total_gain_dollar / total_cost_basis * 100, 2) if total_cost_basis else None

    # ── Write ONLY the schwab block; preserve all other accounts ───────────
    with open(JSON_PATH) as f:
        data = json.load(f)
    schwab = data.get("accounts", {}).get("schwab", {})
    schwab["label"] = "Schwab"
    schwab["role"] = "primary"
    schwab["is_active"] = True
    schwab["cash_balance"] = round(float(cash), 2)
    schwab["account_id"] = f"...{last4}"
    schwab["account_name"] = f"Schwab {acct_type} (Brokerage)"
    schwab["positions"] = positions
    schwab["total_cost_basis"] = round(total_cost_basis, 2)
    schwab["total_gain_dollar"] = round(total_gain_dollar, 2)
    schwab["total_gain_pct"] = total_gain_pct
    schwab["source"] = "live_api"
    schwab["last_updated"] = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")
    schwab["notes"] = (
        f"Live Schwab API sync at {now_et.strftime('%Y-%m-%d %H:%M ET')}. "
        f"{len(positions)} positions, cash ${round(float(cash), 2)}."
    )
    data.setdefault("accounts", {})["schwab"] = schwab
    data["last_updated"] = now_et.strftime("%Y-%m-%d")

    tmp = JSON_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, JSON_PATH)   # atomic
    log.info("wrote schwab block (source=live_api): cash $%.2f, %d positions, acct ...%s",
             float(cash), len(positions), last4)
    return 0


if __name__ == "__main__":
    sys.exit(main())
