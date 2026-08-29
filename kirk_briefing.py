#!/usr/bin/env python3
"""
kirk_briefing.py — KIRK-DAILYBRIEF v1 (net-new, all 4 modes).

Admiral-approved net-new full-scope build (2026-06-10), superseding the orphaned
"Amendment 1" (which assumed a base pipeline that never existed on this box).

Four read-only daily market briefings pushed to NTFY ollietrades-admin and
archived (never overwritten, never deleted) to data/kirk_briefings/:

    --mode premarket    05:45 AZ fixed                 KIRK [PREMARKET]
    --mode open_check   market open  + 29min (~06:59)  KIRK [OPEN+29]
    --mode power_hour   market close - 15min (~12:45)  KIRK [POWER HOUR]
    --mode after_close  market close + 15min (~13:15)  KIRK [AFTER CLOSE]

DOCTRINE COMPLIANCE
-------------------
* RULE #1 (Schwab hands-off): this script NEVER touches Schwab and NEVER places,
  modifies, or cancels any order on any venue. Every data call is read-only GET.
  The only "fleet" read is Alpaca PAPER positions (after_close reconcile,
  report-only). No order path exists in this file.
* "Free Models First": synthesis is Grok-primary ONLY while under the shared
  GROK_ADVISOR_DAILY_CAP (engine.team_advisor_grok ledger); otherwise local
  Ollama on bigmac (see config.OLLIE_URL; Ollie Max/.168 decommissioned, HM-
  KIRK-RESTORE 2026-08-28). If both are unreachable the briefing still renders
  from deterministic real data — a dead LLM never blocks a briefing.
* market_calendar is the authoritative holiday / early-close / hours source
  (full NYSE table through 2027, observed-day + early-close aware). Used in
  preference to a live Polygon status call so the schedule gate has no network
  dependency. On early-close days the close-relative targets shift automatically.
* Never delete data: archives + .sent sidecars + .audittest moves are additive.

SCHEDULING
----------
Each mode computes its target AZ time from the actual ET session (early-close
aware). If fired before target it sleeps to target (max 90 min). On a market
holiday/weekend it logs and exits 0 silently. Cron one-shots, not a service.

DELIVERY AUDIT (Job B, after_close)
-----------------------------------
after_close checks today's premarket/open_check/power_hour .md + .sent sidecars.
All present+sent -> "AUDIT: 3/3 briefings delivered." Missing -> regen via the
--catchup path (labeled LATE REGEN, data as-of-now). Generated-but-unsent ->
re-push. Any failure is flagged LOUDLY at the TOP of the after_close NTFY.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

# Repo root on sys.path so `engine.*` / `strategies.*` import when run by path.
_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from engine.market_calendar import (  # authoritative hours/holiday source
    ET,
    MARKET_OPEN_TIME,
    MARKET_CLOSE_TIME,
    EARLY_CLOSE_TIME,
    az_now,
    is_trading_day,
    is_early_close_day,
    is_us_market_holiday,
    get_holiday_name,
)
from zoneinfo import ZoneInfo

_AZ = ZoneInfo("America/Phoenix")

# ── Constants ───────────────────────────────────────────────────────────────
MODES = ("premarket", "open_check", "power_hour", "after_close")
ARCHIVE_DIR = _REPO_ROOT / "data" / "kirk_briefings"
NTFY_TOPIC = os.getenv("KIRK_BRIEFING_TOPIC", "ollietrades-admin")
NTFY_BODY_MAX = 3800  # ntfy.sh practical message ceiling; full text goes to .md

# Index + sector/metal probes (Polygon snapshot tickers).
INDEX_TICKERS = ["SPY", "QQQ", "DIA", "IWM", "VIXY"]
SECTOR_METAL_TICKERS = ["SMH", "GLD", "SLV", "XLK", "XLE", "XLF"]
# Live option-chain candidates for the spread sections (real strikes only).
SPREAD_CANDIDATES = ["SPY", "QQQ", "SMH"]
# Squeeze watchlist seed (SI% > 40 float). Directive: GRPN, HTZ, BETR.
SQUEEZE_WATCHLIST = [
    s.strip().upper()
    for s in os.getenv("KIRK_SQUEEZE_WATCHLIST", "GRPN,HTZ,BETR").split(",")
    if s.strip()
]

MODE_TITLES = {
    "premarket": "PREMARKET",
    "open_check": "OPEN+29",
    "power_hour": "POWER HOUR",
    "after_close": "AFTER CLOSE",
}


def _log(msg: str) -> None:
    print(f"[kirk_briefing {az_now().strftime('%Y-%m-%d %H:%M:%S')} AZ] {msg}",
          flush=True)


# ── NTFY (synchronous, returns success — needed for the .sent delivery audit) ─
# HM-NTFY-IPV6-NOROUTE (2026-07-07, ported 2026-07-09): this box has no working
# IPv6 route to ntfy.sh (see engine/alert_channels.py::_send_ntfy for the original
# diagnosis) — force IPv4 for the duration of the call, same pattern, so this
# script's push doesn't hit the same OSError(65, "No route to host") failure.
_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


# HM-KIRK-NTFY-429-RETRY (2026-08-28): every single briefing push has been
# failing with ntfy.sh 429 since ~2026-07-20 (confirmed live in
# logs/kirk_briefing_cron.log) — this box runs enough other ntfy-emitting
# jobs that the shared anonymous per-IP limit is frequently exhausted during
# market hours. A manual retry minutes later succeeds fine, so the limit is
# a transient token-bucket, not a hard daily wall — worth 2-3 spaced retries
# before giving up rather than dropping the message after one 429.
NTFY_MAX_ATTEMPTS = 4
NTFY_RETRY_BACKOFF = (5, 20, 60)  # seconds between attempts 1->2, 2->3, 3->4


def push_ntfy(title: str, body: str, priority: int = 4) -> bool:
    """POST one ntfy.sh message synchronously, retrying on 429. Returns True
    on HTTP 200 from any attempt.

    Distinct from engine.ntfy._fire (fire-and-forget, swallows result): Job B's
    delivery guarantee needs to KNOW the push landed. Same ntfy.sh JSON shape.
    """
    body = body if len(body) <= NTFY_BODY_MAX else (
        body[:NTFY_BODY_MAX] + "\n…(truncated — full briefing archived)"
    )
    data = json.dumps({
        "topic": NTFY_TOPIC,
        "title": title,
        "message": body,
        "priority": priority,
        "tags": ["rocket"],
    }).encode()

    for attempt in range(1, NTFY_MAX_ATTEMPTS + 1):
        req = urllib.request.Request(
            "https://ntfy.sh", data=data,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with _ntfy_ipv4_lock:
                socket.getaddrinfo = _ipv4_only_getaddrinfo
                try:
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        _log(f"ntfy push [{resp.status}] attempt {attempt}: {title}")
                        return resp.status == 200
                finally:
                    socket.getaddrinfo = _orig_getaddrinfo
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < NTFY_MAX_ATTEMPTS:
                wait = NTFY_RETRY_BACKOFF[attempt - 1]
                if e.headers and e.headers.get("Retry-After", "").isdigit():
                    wait = max(wait, int(e.headers["Retry-After"]))
                _log(f"ntfy push 429 (attempt {attempt}/{NTFY_MAX_ATTEMPTS}), "
                     f"retrying in {wait}s: {title}")
                time.sleep(wait)
                continue
            _log(f"ntfy push FAILED (HTTPError {e.code}, attempt {attempt}): {title}")
            return False
        except Exception as e:
            _log(f"ntfy push FAILED ({type(e).__name__}: {e!r}, attempt {attempt}): {title}")
            return False
    return False


# ── LLM synthesis: Grok-primary (under shared cap) → Ollama fallback ─────────
_SYSTEM_PROMPT = (
    "You are Kirk, tactical market briefer for an Alpaca PAPER-TRADING research "
    "desk (no real money; all execution is paper). Write terse, scannable "
    "markdown. Use ONLY the DATA block provided — never invent tickers, prices, "
    "option strikes, or events. If a datum is absent write 'n/a'. Be decisive "
    "and concrete. No preamble, no disclaimers, no restating the instructions."
)


def _synthesize(prompt: str) -> tuple[str, str]:
    """Return (narrative_text, model_used). Grok if under the shared daily cap,
    else local Ollama. Returns ('', 'none') if both are unreachable — callers
    must degrade gracefully (the deterministic data sections still render)."""
    import requests  # available in .venv (2.33.0)

    # Grok primary — gated on the SHARED team_advisor_grok daily cost cap so
    # briefings + advisor never blow the same xAI budget. Don't fork the ladder.
    use_grok = False
    try:
        from engine.team_advisor_grok import get_daily_cost, DAILY_COST_CAP
        api_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
        if api_key and get_daily_cost() < (0.9 * DAILY_COST_CAP):
            use_grok = True
    except Exception:
        use_grok = False

    if use_grok:
        try:
            model = os.getenv("GROK_MODEL", "grok-4.20-0309-non-reasoning")
            api_key = os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY", "")
            resp = requests.post(
                "https://api.x.ai/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}",
                         "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.3,
                    "max_tokens": 1600,
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            usage = data.get("usage", {})
            _log_briefing_cost(usage)
            if content:
                return content, model
        except Exception as e:
            _log(f"Grok synth failed ({type(e).__name__}: {e!r}) — falling back to Ollama")

    # Ollama fallback (free, local bigmac).
    # HM-KIRK-RESTORE 2026-08-28: OLLIE_URL now resolves to localhost:11434
    # (config.py, olliemax decommissioned) — no code change needed there. Model
    # default changed qwen3:8b -> ministral-3:3b: standing GPU rule requires
    # this fallback use a model the trader already rotates through, never
    # trigger a fresh load. Note ministral-3:3b is currently an ALIAS of
    # qwen3:8b (same weights, ID 500a1f067a9f — see project memory
    # project_ollama_model_aliases_2026-08-25) but Ollama's residency
    # tracking is per-tag, not per-blob, so the two names still count as a
    # "different model" for swap/eviction purposes -- the tag choice matters.
    try:
        from config import OLLIE_URL
        base = os.getenv("ADVISORY_OLLAMA_URL",
                         os.getenv("OLLAMA_BASE_URL", OLLIE_URL))
        model = os.getenv("CREWAI_MODEL", "ministral-3:3b")
        resp = requests.post(
            f"{base}/api/generate",
            json={
                "model": model,
                "prompt": _SYSTEM_PROMPT + "\n\n" + prompt,
                "stream": False,
                "think": False,  # qwen3 think-disable (top-level key is the only reliable one)
            },
            timeout=120,
        )
        resp.raise_for_status()
        content = (resp.json().get("response") or "").strip()
        if content:
            return content, f"ollama:{model}"
    except Exception as e:
        _log(f"Ollama synth failed ({type(e).__name__}: {e!r}) — rendering data-only")

    return "", "none"


def _log_briefing_cost(usage: dict) -> None:
    """Attribute briefing Grok spend to api_costs (player_id='kirk-briefing',
    call_type='kirk_briefing') using xAI's exact billed ticks. Best-effort."""
    try:
        ticks = usage.get("cost_in_usd_ticks")
        cost = (float(ticks) * 1e-10) if ticks is not None else 0.0
        in_tok = int(usage.get("prompt_tokens") or 0)
        out_tok = int(usage.get("completion_tokens") or 0)
        conn = sqlite3.connect("data/trader.db", timeout=10)
        conn.execute(
            "INSERT INTO api_costs "
            "(player_id, call_type, input_tokens, output_tokens, cost_usd, timestamp) "
            "VALUES (?,?,?,?,?,?)",
            ("kirk-briefing", "kirk_briefing", in_tok, out_tok, cost,
             datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # accounting must never break a briefing


# ── Read-only data gatherers (every one degrades to {} / [] on any failure) ──
def _safe(fn, default):
    try:
        return fn()
    except Exception as e:
        _log(f"data gather '{getattr(fn, '__name__', fn)}' failed: {type(e).__name__}: {e!r}")
        return default


def gather_quotes(tickers: list[str]) -> dict[str, dict]:
    """{ticker: {price, prev_close, change_pct}} via Polygon snapshot (bulk)."""
    from strategies.polygon_client import fetch_ticker_snapshots
    snaps = fetch_ticker_snapshots(tickers)
    out: dict[str, dict] = {}
    for t, s in snaps.items():
        close = s.get("close")
        prev = s.get("prev_close")
        chg = ((close - prev) / prev * 100.0) if (close and prev) else None
        out[t] = {"price": close, "prev_close": prev,
                  "change_pct": round(chg, 2) if chg is not None else None}
    return out


def gather_movers(limit: int = 8) -> dict[str, list]:
    """Top gainers/losers/volume leaders from the full-market snapshot.

    Liquidity filter: price >= $2 and volume >= 500k to keep names tradeable.
    Polygon Starter is 15-min delayed and pre-open has thin 'day' data — labeled
    accordingly by callers. Never raises.
    """
    from strategies.polygon_client import fetch_market_snapshot
    rows = fetch_market_snapshot()
    liquid = [r for r in rows
              if (r.get("price") or 0) >= 2 and (r.get("volume") or 0) >= 500_000]
    by_chg = sorted(liquid, key=lambda r: r.get("change", 0))
    by_vol = sorted(liquid, key=lambda r: r.get("volume", 0), reverse=True)

    def _fmt(r):
        return {"ticker": r["ticker"], "price": round(r["price"], 2),
                "change_pct": round(r.get("change", 0), 2),
                "volume": int(r.get("volume", 0))}

    return {
        "gainers": [_fmt(r) for r in by_chg[::-1][:limit]],
        "losers": [_fmt(r) for r in by_chg[:limit]],
        "volume_leaders": [_fmt(r) for r in by_vol[:limit]],
        "universe": len(liquid),
    }


def gather_spreads(tickers: list[str]) -> list[dict]:
    """Real bull-call-spread quotes (live Polygon chains — NO invented strikes).

    Uses strategies.polygon_client.build_spread_quote at ~14 DTE / $5 width.
    Returns [] silently if chains are unavailable (degrade, never block).
    """
    from strategies.polygon_client import build_spread_quote
    out = []
    for t in tickers:
        q = _safe(lambda t=t: build_spread_quote(t, "bull_call_spread", 14, 5.0), None)
        if not q:
            continue
        out.append({
            "ticker": t,
            "long_strike": q.long_leg.strike,
            "short_strike": q.short_leg.strike,
            "expiration": q.long_leg.expiration,
            "net_debit": round(q.net_debit, 2),
            "max_profit": round(q.max_profit, 2),
            "max_loss": round(q.max_loss, 2),
            "width": q.width,
            "dte": q.dte,
        })
    return out


def gather_fred() -> dict:
    """FRED Bankrate deposit-rate lean — [CONTEXT-ONLY], never a trade signal."""
    from engine.fred_bankrate_signal import get_signal
    sig = get_signal()
    return {"vote": sig.get("vote"), "avg_deposit_bps": sig.get("avg_deposit_bps"),
            "reason": sig.get("reason")}


def gather_gex() -> dict:
    """Best-effort GEX regime for SPY. Degrades to {} on any failure."""
    from gex_calculator import get_latest_snapshot
    gex = get_latest_snapshot("SPY") or {}
    if not gex:
        return {}
    total = gex.get("total_gex", 0) or 0
    return {"regime": "positive/pinned" if total > 0 else "negative/volatile",
            "total_gex": total, "put_wall": gex.get("put_wall"),
            "call_wall": gex.get("call_wall")}


def gather_signal_center() -> dict:
    """Best-effort Signal Center summary (localhost:9000). Degrades to {}."""
    import requests
    url = os.getenv("SIGNAL_CENTER_URL", "http://127.0.0.1:9000")
    r = requests.get(f"{url}/api/sources/health", timeout=5)
    r.raise_for_status()
    data = r.json()
    return {"sources_health": data} if data else {}


def gather_paper_positions() -> list[dict]:
    """Alpaca PAPER positions (read-only) for the after_close fleet reconcile."""
    from engine.alpaca_bridge import alpaca
    pos = alpaca.positions()
    return [p for p in pos if "error" not in p]


# ── Deterministic data rendering (real numbers — grounding for synthesis) ────
def _fmt_quote_line(t: str, q: dict) -> str:
    p = q.get("price")
    c = q.get("change_pct")
    p_s = f"${p:,.2f}" if isinstance(p, (int, float)) else "n/a"
    c_s = f"{c:+.2f}%" if isinstance(c, (int, float)) else "n/a"
    return f"  {t:<5} {p_s:>11}  {c_s}"


def _fmt_mover_line(m: dict) -> str:
    return (f"  {m['ticker']:<6} ${m['price']:>8,.2f}  {m['change_pct']:+6.2f}%  "
            f"vol {m['volume']:,}")


def _render_data_block(data: dict) -> str:
    """Compact, human-readable dump of all gathered data. Numbers come from here
    (never from the LLM) so option strikes / prices are always real."""
    L: list[str] = []
    idx = data.get("indices", {})
    if idx:
        L.append("INDICES")
        for t in INDEX_TICKERS:
            if t in idx:
                L.append(_fmt_quote_line(t, idx[t]))
    sec = data.get("sectors", {})
    if sec:
        L.append("\nSECTORS & METALS")
        for t in SECTOR_METAL_TICKERS:
            if t in sec:
                L.append(_fmt_quote_line(t, sec[t]))
    mv = data.get("movers", {})
    if mv:
        if mv.get("gainers"):
            L.append(f"\nTOP GAINERS (liquid, n={mv.get('universe', '?')})")
            L += [_fmt_mover_line(m) for m in mv["gainers"]]
        if mv.get("losers"):
            L.append("\nTOP LOSERS")
            L += [_fmt_mover_line(m) for m in mv["losers"]]
        if mv.get("volume_leaders"):
            L.append("\nVOLUME LEADERS")
            L += [_fmt_mover_line(m) for m in mv["volume_leaders"]]
    sp = data.get("spreads", [])
    if sp:
        L.append("\nLIVE BULL CALL SPREADS (real Polygon chains, ~14DTE/$5w)")
        for s in sp:
            L.append(f"  {s['ticker']} ${s['long_strike']:.0f}/${s['short_strike']:.0f} "
                     f"exp {s['expiration']} | debit ${s['net_debit']:.2f} "
                     f"maxP ${s['max_profit']:.0f} maxL ${s['max_loss']:.0f}")
    elif "spreads" in data:
        L.append("\nLIVE BULL CALL SPREADS: n/a — chains unavailable this run")
    sq = data.get("squeeze", {})
    if sq:
        L.append("\nSQUEEZE BOARD (watchlist vs live)")
        for t in SQUEEZE_WATCHLIST:
            q = sq.get(t)
            if q:
                L.append(_fmt_quote_line(t, q))
            else:
                L.append(f"  {t:<5}         n/a")
    fred = data.get("fred", {})
    if fred:
        L.append(f"\n[CONTEXT-ONLY] FRED Bankrate deposit lean: {fred.get('vote')} "
                 f"({fred.get('reason')})")
    gex = data.get("gex", {})
    if gex:
        L.append(f"\nGEX (SPY): {gex.get('regime')} | put_wall {gex.get('put_wall')} "
                 f"| call_wall {gex.get('call_wall')}")
    pp = data.get("paper_positions")
    if pp is not None:
        L.append(f"\nALPACA PAPER FLEET ({len(pp)} positions)")
        for p in pp:
            L.append(f"  {p['symbol']:<6} qty {p['qty']:>8.2f}  "
                     f"P/L {p['unrealized_plpc']:+.2f}%  mv ${p['market_value']:,.0f}")
    return "\n".join(L) if L else "(no data available this run)"


# ── Machine-readable sidecar (R3 — pre-wires the Reflexion scoring loop) ─────
def _parse_json(text: str):
    """Best-effort: strip ``` fences and parse a JSON object. Returns None on fail."""
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        t = "\n".join(t.splitlines()[1:])
        if t.rstrip().endswith("```"):
            t = t.rsplit("```", 1)[0]
    # Trim to the outermost object braces.
    i, j = t.find("{"), t.rfind("}")
    if i == -1 or j == -1 or j <= i:
        return None
    try:
        return json.loads(t[i:j + 1])
    except Exception:
        return None


def _grade_premarket_setups(day: str) -> list[dict]:
    """power_hour grades[]: grade today's premarket setups vs the live tape.

    Reads YYYY-MM-DD_premarket.json (this mode's own sidecar from the morning)
    and tags each setup WORKED | FAILED | NO_TRIGGER. Direction inferred from
    trigger-vs-invalidation (trigger>=invalidation -> long; else short).
    Returns [] if the premarket sidecar is absent/unparseable (schema-only,
    no consumer yet)."""
    path = ARCHIVE_DIR / f"{day}_premarket.json"
    if not path.exists():
        return []
    try:
        setups = (json.loads(path.read_text()).get("setups") or [])
    except Exception:
        return []
    grades = []
    for s in setups:
        tkr = (s.get("ticker") or "").upper()
        try:
            trig = float(s.get("trigger"))
            inval = float(s.get("invalidation"))
        except (TypeError, ValueError):
            grades.append({"ticker": tkr, "grade": "NO_TRIGGER", "note": "unparseable levels"})
            continue
        q = _safe(lambda tkr=tkr: gather_quotes([tkr]), {}).get(tkr, {})
        px = q.get("price")
        if not isinstance(px, (int, float)):
            grades.append({"ticker": tkr, "grade": "NO_TRIGGER", "note": "no price"})
            continue
        long_setup = trig >= inval
        if long_setup:
            g = "WORKED" if px >= trig else ("FAILED" if px <= inval else "NO_TRIGGER")
        else:
            g = "WORKED" if px <= trig else ("FAILED" if px >= inval else "NO_TRIGGER")
        grades.append({"ticker": tkr, "trigger": trig, "invalidation": inval,
                       "current": px, "grade": g})
    return grades


def build_structured_sidecar(mode: str, data: dict, narrative: str) -> dict:
    """Stable machine-readable schema written alongside every .md (R3).

    No consumer yet — schema only, so the future Reflexion wiring is a reader
    task, not a parser rewrite. Always returns the full key set (defaults) so
    the shape is stable even when the LLM is unavailable."""
    day = az_now().strftime("%Y-%m-%d")
    sidecar: dict = {
        "schema_version": 1,
        "consumer": None,            # Reflexion scoring loop wires in later
        "mode": mode,
        "date": day,
        "generated_at_az": az_now().isoformat(),
        "regime_call": None,
        "binary_events": [],
        "setups": [],
    }
    # Deterministic regime fallback from SPY (so the field is never null on a
    # live tape even if the LLM extraction fails).
    chg = ((data.get("indices") or {}).get("SPY") or {}).get("change_pct")
    if isinstance(chg, (int, float)):
        sidecar["regime_call"] = (
            "risk-off" if chg < -0.2 else "risk-on" if chg > 0.2 else "neutral"
        )
    # LLM structured extraction (best-effort; degrades to the deterministic shape).
    if narrative:
        prompt = (
            "From the briefing below output STRICT JSON ONLY — no prose, no code "
            "fences. Keys: regime_call (risk-on|risk-off|neutral|mixed), "
            "binary_events (array of strings), setups (array of objects each with "
            'string fields "ticker", "trigger", "invalidation"). Use ONLY facts '
            "present in the briefing; empty arrays if none.\n\nBRIEFING:\n" + narrative
        )
        parsed = _parse_json(_synthesize(prompt)[0])
        if isinstance(parsed, dict):
            if parsed.get("regime_call"):
                sidecar["regime_call"] = parsed["regime_call"]
            if isinstance(parsed.get("binary_events"), list):
                sidecar["binary_events"] = parsed["binary_events"]
            if isinstance(parsed.get("setups"), list):
                sidecar["setups"] = parsed["setups"]
    if mode == "power_hour":
        sidecar["grades"] = _grade_premarket_setups(day)
    return sidecar


# ── Per-mode data gather + prompt skeleton ──────────────────────────────────
def _gather_for_mode(mode: str) -> dict:
    data: dict = {}
    if mode in ("premarket", "open_check", "power_hour", "after_close"):
        data["indices"] = _safe(lambda: gather_quotes(INDEX_TICKERS), {})
        data["sectors"] = _safe(lambda: gather_quotes(SECTOR_METAL_TICKERS), {})
        data["movers"] = _safe(lambda: gather_movers(), {})
        data["fred"] = _safe(gather_fred, {})
        data["gex"] = _safe(gather_gex, {})
    if mode == "premarket":
        data["spreads"] = _safe(lambda: gather_spreads(SPREAD_CANDIDATES), [])
        data["squeeze"] = _safe(lambda: gather_quotes(SQUEEZE_WATCHLIST), {})
    if mode == "after_close":
        data["paper_positions"] = _safe(gather_paper_positions, [])
        data["signal_center"] = _safe(gather_signal_center, {})
    return data


_PROMPT_SKELETONS = {
    "premarket": (
        "Write the KIRK PREMARKET read as prose (4-8 tight lines). Cover, in order: "
        "REGIME CALL (risk-on/off; what broke/holds); TODAY'S BINARY EVENTS if any "
        "are evident in the data else 'none flagged'; up to 3 SWING SETUPS each with "
        "a trigger and an invalidation; INTRADAY FOCUS; GAP WATCH (continuation vs "
        "fade); METALS/ETF regime; a one-line take on the listed bull call spreads "
        "(reference ONLY the strikes shown); top shorts if any losers stand out; "
        "squeeze board note; EXECUTION RULES reminder (paper-only, size small, no "
        "trade in the first/last 5 min). Reference only the DATA below."
    ),
    "open_check": (
        "Write the KIRK OPEN+29 delta read (max ~10 lines, prose). Cover: REGIME "
        "CONFIRM/DENY vs a normal open; gaps held/filled/faded; any NEW ADDS; any "
        "DROPS (invalidated); binary-event result if evident; ADJUSTED RULES. This "
        "is a DELTA — do not restate a full premarket. Reference only the DATA below."
    ),
    "power_hour": (
        "Write the KIRK POWER HOUR wrap (prose, ~8 lines). Cover: DAY TAPE SUMMARY; "
        "what worked/failed vs a typical morning read (be honest, qualitative); "
        "OVERNIGHT RISK; HOLD vs FLATTEN guidance for a paper fleet; TOMORROW "
        "PRELOAD (1-2 lines). Reference only the DATA below."
    ),
    "after_close": (
        "Write the KIRK AFTER CLOSE straggler sweep (prose, ~12 lines), DELTA vs "
        "power_hour only. Cover Job A: closing-auction moves; after-hours gappers "
        "(post-close earnings/news if evident); late-settling EOD data; PAPER FLEET "
        "EOD RECONCILE (note any position that looks off vs a hold/flatten plan — "
        "REPORT-ONLY, no action); overnight watch adds. Reference only the DATA below."
    ),
}


# ── Build one briefing → (full markdown, machine-readable sidecar) ───────────
def build_briefing(mode: str, catchup: bool = False) -> tuple[str, dict]:
    now = az_now()
    data = _gather_for_mode(mode)
    data_block = _render_data_block(data)

    prompt = (
        f"{_PROMPT_SKELETONS[mode]}\n\n"
        f"=== DATA (as of {now.strftime('%Y-%m-%d %H:%M')} AZ) ===\n{data_block}\n"
    )
    narrative, model_used = _synthesize(prompt)
    # R2: any synthesis failure → data-only brief with a loud header, never silence.
    degraded = (model_used == "none")

    late = " — LATE REGEN" if catchup else ""
    title_line = f"# KIRK [{MODE_TITLES[mode]}{late}] — {now.strftime('%Y-%m-%d')}"
    if degraded:
        title_line += "  ⚠️ SYNTHESIS DEGRADED (data-only)"
    header = [
        title_line,
        f"_generated {now.strftime('%Y-%m-%d %H:%M:%S')} AZ"
        + (" · data as-of-now (LATE REGEN)" if catchup else "")
        + f" · synth={model_used}_",
        "",
    ]
    read = (narrative if narrative
            else "_⚠️ SYNTHESIS DEGRADED — LLM (Grok + local Ollama) unreachable. "
                 "Data-only brief; numbers below are live and authoritative._")
    body = ["## KIRK'S READ", read, ""]
    body += ["## DATA", "```", data_block, "```", ""]
    body += ["---", "_Alpaca paper-trading research only. Read-only briefing; "
             "no orders placed. Schwab untouched (RULE #1)._"]
    md = "\n".join(header + body)

    sidecar = build_structured_sidecar(mode, data, narrative)
    sidecar["synthesis_degraded"] = degraded
    return md, sidecar


# ── Delivery: archive (dated, never overwrite-destroy) + push + .sent sidecar ─
def _archive_path(mode: str, day: str | None = None) -> Path:
    day = day or az_now().strftime("%Y-%m-%d")
    return ARCHIVE_DIR / f"{day}_{mode}.md"


def _sent_path(mode: str, day: str | None = None) -> Path:
    return _archive_path(mode, day).with_suffix(".md.sent")


def _json_path(mode: str, day: str | None = None) -> Path:
    day = day or az_now().strftime("%Y-%m-%d")
    return ARCHIVE_DIR / f"{day}_{mode}.json"


def deliver(mode: str, md_text: str, sidecar: dict | None = None,
            catchup: bool = False, top_banner: str = "") -> bool:
    """Archive the briefing (.md + machine-readable .json), push to NTFY, and
    write a .sent sidecar on success. Returns the push success bool. Archiving
    happens regardless of push result."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    path = _archive_path(mode)
    path.write_text(md_text)
    _log(f"archived → {path}")
    if sidecar is not None:
        _json_path(mode).write_text(json.dumps(sidecar, indent=2))
        _log(f"sidecar → {_json_path(mode)}")

    late = " — LATE REGEN" if catchup else ""
    title = f"KIRK [{MODE_TITLES[mode]}{late}] — {az_now().strftime('%Y-%m-%d')}"
    # NTFY body: strip the markdown header line; prepend any audit banner.
    push_body = md_text
    if top_banner:
        push_body = top_banner.rstrip() + "\n\n" + push_body
    ok = push_ntfy(title, push_body)
    if ok:
        _sent_path(mode).write_text(
            json.dumps({"sent_at": datetime.utcnow().isoformat() + "Z",
                        "topic": NTFY_TOPIC, "title": title, "catchup": catchup})
        )
        _log(f"sent sidecar → {_sent_path(mode)}")
    return ok


# ── Job B: delivery audit (after_close) ─────────────────────────────────────
def delivery_audit() -> tuple[str, bool]:
    """Verify today's premarket/open_check/power_hour landed (md + .sent).

    Recovery:
      * md missing            -> regenerate via build_briefing(catchup=True) + deliver
      * md present, no .sent   -> re-push the archived md, write .sent
    Returns (audit_block_markdown, any_failure).
    """
    day = az_now().strftime("%Y-%m-%d")
    checked = ["premarket", "open_check", "power_hour"]
    lines: list[str] = []
    any_fail = False
    delivered = 0

    for m in checked:
        md = _archive_path(m, day)
        sent = _sent_path(m, day)
        if md.exists() and sent.exists():
            delivered += 1
            lines.append(f"  ✅ {m}: generated + sent")
            continue
        any_fail = True
        if not md.exists():
            lines.append(f"  ❌ {m}: MISSING — regenerating (LATE REGEN)")
            _log(f"[audit] {m} md missing — regenerating")
            text, sc = build_briefing(m, catchup=True)
            ok = deliver(m, text, sc, catchup=True)
            lines.append(f"      → regen {'sent ✅' if ok else 'push FAILED ❌'}")
            if ok:
                delivered += 1
        else:  # md present but unsent
            lines.append(f"  ⚠️  {m}: generated but UNSENT — re-pushing")
            _log(f"[audit] {m} unsent — re-pushing archived md")
            title = f"KIRK [{MODE_TITLES[m]} — RESEND] — {day}"
            ok = push_ntfy(title, md.read_text())
            if ok:
                _sent_path(m, day).write_text(
                    json.dumps({"sent_at": datetime.utcnow().isoformat() + "Z",
                                "topic": NTFY_TOPIC, "title": title, "resend": True})
                )
                delivered += 1
                lines.append("      → resend sent ✅")
            else:
                lines.append("      → resend push FAILED ❌")

    if not any_fail:
        block = f"AUDIT: {delivered}/3 briefings delivered."
    else:
        block = ("## DELIVERY AUDIT\n" + "\n".join(lines)
                 + f"\n\nAUDIT: {delivered}/3 delivered after recovery.")
    return block, any_fail


# ── Scheduling ──────────────────────────────────────────────────────────────
def _et_session_times(d):
    open_et = ET.localize(datetime.combine(d, MARKET_OPEN_TIME))
    close_t = EARLY_CLOSE_TIME if is_early_close_day(d) else MARKET_CLOSE_TIME
    close_et = ET.localize(datetime.combine(d, close_t))
    return open_et, close_et


def compute_target_az(mode: str, now_az: datetime) -> datetime:
    """Target AZ datetime for the mode, computed from the actual ET session
    (early-close aware so half-days shift close-relative targets automatically)."""
    d = now_az.date()
    open_et, close_et = _et_session_times(d)
    if mode == "premarket":
        return now_az.replace(hour=5, minute=45, second=0, microsecond=0)
    if mode == "open_check":
        return (open_et + timedelta(minutes=29)).astimezone(_AZ)
    if mode == "power_hour":
        return (close_et - timedelta(minutes=15)).astimezone(_AZ)
    if mode == "after_close":
        return (close_et + timedelta(minutes=15)).astimezone(_AZ)
    raise ValueError(f"unknown mode {mode}")


def sleep_to_target(mode: str) -> None:
    """Sleep until the mode's target AZ time (max 90 min). No-op if already past."""
    target = compute_target_az(mode, az_now())
    delta = (target - az_now()).total_seconds()
    if delta <= 0:
        _log(f"[{mode}] fired at/after target {target.strftime('%H:%M')} AZ — running now")
        return
    sleep_s = min(delta, 90 * 60)
    _log(f"[{mode}] target {target.strftime('%H:%M')} AZ; sleeping {sleep_s:.0f}s")
    time.sleep(sleep_s)


# ── Main ────────────────────────────────────────────────────────────────────
def run_mode(mode: str, *, dry_run: bool, catchup: bool, no_sleep: bool) -> int:
    today = az_now().date()

    # Holiday/weekend gate — log + exit 0 silently (cron-friendly).
    if not dry_run and not is_trading_day(today):
        reason = (get_holiday_name(today) or "weekend") if is_us_market_holiday(today) else "weekend"
        _log(f"[{mode}] market closed today ({reason}) — exit 0")
        return 0

    if not dry_run and not no_sleep:
        sleep_to_target(mode)

    _log(f"[{mode}] building briefing (catchup={catchup}, dry_run={dry_run})")
    md, sidecar = build_briefing(mode, catchup=catchup)

    # Job B audit is part of after_close.
    banner = ""
    if mode == "after_close":
        if dry_run:
            audit_block, any_fail = ("AUDIT (dry-run): would verify 3/3 "
                                     "premarket/open_check/power_hour .md + .sent.", False)
        else:
            audit_block, any_fail = delivery_audit()
        md = md + "\n\n" + audit_block
        if any_fail:
            banner = "🚨 AUDIT FAIL: a prior briefing did not land — recovered/regen'd. See below."

    if dry_run:
        print("\n" + "=" * 72)
        print(md)
        print("=" * 72)
        print("\n--- machine-readable sidecar (.json) ---")
        print(json.dumps(sidecar, indent=2))
        _log(f"[{mode}] DRY-RUN complete — no push, no archive")
        return 0

    ok = deliver(mode, md, sidecar, catchup=catchup, top_banner=banner)
    _log(f"[{mode}] delivered (push_ok={ok})")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="KIRK-DAILYBRIEF v1 — 4-mode market briefings")
    ap.add_argument("--mode", required=True, choices=MODES)
    ap.add_argument("--dry-run", action="store_true",
                    help="render + print, no push/archive/sleep")
    ap.add_argument("--catchup", action="store_true",
                    help="LATE REGEN: label data as-of-now (audit recovery)")
    ap.add_argument("--no-sleep", action="store_true",
                    help="skip sleep-to-target (for an immediate live test)")
    args = ap.parse_args()
    try:
        return run_mode(args.mode, dry_run=args.dry_run,
                        catchup=args.catchup, no_sleep=args.no_sleep)
    except Exception as e:
        _log(f"[{args.mode}] FATAL {type(e).__name__}: {e!r}")
        # Best-effort alert so a silent crash doesn't go unseen.
        push_ntfy(f"KIRK [{MODE_TITLES.get(args.mode, args.mode)}] — CRASH",
                  f"{type(e).__name__}: {e!r}", priority=5)
        return 1


if __name__ == "__main__":
    sys.exit(main())
