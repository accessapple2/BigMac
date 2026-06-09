#!/usr/bin/env python3
"""
fred_bankrate_signal.py — Bankrate Monitor (BRM) deposit-rate intel feed.

Confirmatory-only TI vote. Same posture as external_intel_signal.py / Tractor Beam:
this NEVER triggers a trade. It returns a lean ("confirm" / "neutral" / "caution")
that the confluence layer (UHURA v2) may count as ONE confirmatory source.

Data: FRED release 742 (Bankrate Monitor National Index).
Fetch path: keyless fredgraph CSV endpoint by default. If FRED_API_KEY is set in
the environment, the JSON API is used instead (revisions-aware, more robust).

Doctrine:
  - is_trigger is hardcoded False. Do not change.
  - Snapshots are archived with a dated suffix, never overwritten/deleted.
"""

from __future__ import annotations

import csv
import io
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# --- Series catalog (FRED release 742, all 15) -----------------------------

ALL_SERIES = {
    # deposit APYs — these feed the vote
    "BRMSA0104":  "Savings Account APY (weekly)",
    "BRMINTCA01": "Interest Checking Account APY (weekly)",
    "BRMCDS0101": "1-Year CD APY (weekly)",
    "BRMCDS0102": "5-Year CD APY (weekly)",
    # config-available context, NOT voted
    "BRMCC01":    "Credit Card APR",
    "BRMUPL01":   "Unsecured Personal Loan Rate",
    "BRMALR0101": "Auto Loan 60mo New Car",
    "BRMALR0102": "Auto Loan 48mo Used Car",
    "BRMHELOC01": "HELOC Rate",
    "BRMARM01":   "5/1 ARM Rate",
    "BRMTG0101":  "15-Year Fixed Mortgage",
    "BRMTG0102":  "30-Year Fixed Mortgage",
    "BRMSA0101":  "Savings Account APY (annual)",
    "BRMSA0102":  "Savings Account APY (semiannual)",
    "BRMSA0103":  "Savings Account APY (monthly)",
}

# Only these drive the confirmatory lean.
DEPOSIT_VOTE_SERIES = ["BRMSA0104", "BRMINTCA01", "BRMCDS0101", "BRMCDS0102"]

# How many recent weekly observations to compare for the trend read.
LOOKBACK_OBS = 8

# bps move (averaged across deposit series) needed to lean off neutral.
THRESHOLD_BPS = 5.0

ARCHIVE_DIR = Path(os.environ.get("OLLIE_INTEL_ARCHIVE", "data/intel_archive"))

_USER_AGENT = "OllieTrades-FRED-intel/1.0 (confirmatory-only)"


# --- Fetch ------------------------------------------------------------------

def _fetch_csv(series_id: str, lookback: int) -> list[tuple[str, float]]:
    """Keyless fredgraph CSV. Returns [(date, value), ...] oldest->newest."""
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:
        text = resp.read().decode("utf-8")
    rows: list[tuple[str, float]] = []
    reader = csv.reader(io.StringIO(text))
    next(reader, None)  # header: observation_date,<SERIES_ID>
    for row in reader:
        if len(row) < 2:
            continue
        date_s, val_s = row[0], row[1].strip()
        if val_s in ("", "."):  # FRED missing marker
            continue
        try:
            rows.append((date_s, float(val_s)))
        except ValueError:
            continue
    return rows[-lookback:]


def _fetch_api(series_id: str, lookback: int, api_key: str) -> list[tuple[str, float]]:
    """FRED JSON API path (used only when FRED_API_KEY is present)."""
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={api_key}&file_type=json"
        f"&sort_order=desc&limit={lookback}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    obs: list[tuple[str, float]] = []
    for o in reversed(data.get("observations", [])):  # back to oldest->newest
        v = o.get("value", ".")
        if v in ("", "."):
            continue
        try:
            obs.append((o["date"], float(v)))
        except (ValueError, KeyError):
            continue
    return obs


def _fetch(series_id: str, lookback: int) -> list[tuple[str, float]]:
    key = os.environ.get("FRED_API_KEY")
    if key:
        try:
            return _fetch_api(series_id, lookback, key)
        except Exception:
            pass  # fall back to keyless CSV on any API hiccup
    return _fetch_csv(series_id, lookback)


# --- Vote logic -------------------------------------------------------------

def _series_trend_bps(obs: list[tuple[str, float]]) -> float | None:
    """Net change in basis points, first vs last observation in window."""
    if len(obs) < 2:
        return None
    first, last = obs[0][1], obs[-1][1]
    return (last - first) * 100.0  # percent -> bps


def get_signal(lookback: int = LOOKBACK_OBS) -> dict:
    """
    Returns a confirmatory-only vote dict.

    Interpretation:
      deposit APYs RISING   -> banks bidding for funding / tightening tone -> 'caution'
      deposit APYs FALLING  -> funding pressure easing / risk-on tolerant  -> 'confirm'
      flat within threshold -> 'neutral'

    This is a confluence input, never a trigger. is_trigger is always False.
    """
    per_series = {}
    moves: list[float] = []
    for sid in DEPOSIT_VOTE_SERIES:
        try:
            obs = _fetch(sid, lookback)
            bps = _series_trend_bps(obs)
        except Exception as e:
            per_series[sid] = {"error": str(e)}
            continue
        if bps is None:
            per_series[sid] = {"obs": len(obs), "bps": None}
            continue
        per_series[sid] = {
            "label": ALL_SERIES[sid],
            "latest": obs[-1][1],
            "latest_date": obs[-1][0],
            "bps_change": round(bps, 1),
        }
        moves.append(bps)

    avg_bps = round(sum(moves) / len(moves), 1) if moves else None

    if avg_bps is None:
        lean = "neutral"
        reason = "no usable deposit-rate observations"
    elif avg_bps >= THRESHOLD_BPS:
        lean = "caution"
        reason = f"deposit APYs rising avg {avg_bps}bps over {lookback} obs"
    elif avg_bps <= -THRESHOLD_BPS:
        lean = "confirm"
        reason = f"deposit APYs easing avg {avg_bps}bps over {lookback} obs"
    else:
        lean = "neutral"
        reason = f"deposit APYs flat ({avg_bps}bps, within +/-{THRESHOLD_BPS})"

    return {
        "source": "fred_bankrate",
        "release": "742 Bankrate Monitor National Index",
        "is_trigger": False,            # DOCTRINE: confirmatory only
        "vote": lean,                   # confirm | neutral | caution
        "avg_deposit_bps": avg_bps,
        "lookback_obs": lookback,
        "reason": reason,
        "series": per_series,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
    }


# --- Archive (never delete) -------------------------------------------------

def archive_snapshot(signal: dict) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = ARCHIVE_DIR / f"fred_bankrate_{stamp}.json"
    path.write_text(json.dumps(signal, indent=2))
    return path


# --- CLI --------------------------------------------------------------------

if __name__ == "__main__":
    sig = get_signal()
    print(json.dumps(sig, indent=2))
    p = archive_snapshot(sig)
    print(f"\n[archived] {p}")
