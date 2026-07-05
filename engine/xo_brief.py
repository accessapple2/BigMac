"""
engine/xo_brief.py  —  OllieTrades XO Brief Generator
=====================================================

Web-grounded market briefs at 5 session cadences, fanned out to Claude AND Grok
with equivalent live search, returned as a strict JSON envelope the whole agent
ecosystem can ingest + a human markdown view for the Bridge.

TWO TRIGGER MODES:

  A) SCHEDULED HEARTBEAT (cron) — ET:
       premarket        08:30   plan the day, gaps, overnight + econ calendar
       opening          09:35   opening VOLUME + range pulse, fast read
       midday           12:30   trend/lull check, manage stops, reversals
       closing_30       15:30   EOD positioning, MOC feel, lock gains, set o/n stops
       afterhours_scalp 16:15   AH movers, earnings reactions, paper scalp candidates

  B) EVENT-DRIVEN ALERTS — Holly/Ollie volume scanners + news watcher push
     events; XO reacts with a focused single-symbol "what's the catalyst + paper
     read" brief. OT already does the volume DETECTION; this layer adds the web
     catalyst the local scanners can't see. Per-symbol cooldown prevents spam.

USAGE:
    # scheduled (cron):
    python -m engine.xo_brief --session premarket --provider both
    python -m engine.xo_brief --print-crontab

    # event-driven daemon (subscribes to the scanner/news alert stream):
    python -m engine.xo_brief --watch --provider claude

    # fire one alert by hand / from another process:
    python -m engine.xo_brief --trigger NVDA --reason volume --detail "3.2x avg vol, +4%"

SACRED RULES (enforced in prompt + post-parse guard):
    1. Paper-only execution (Alpaca paper). NEVER a live/Schwab order.
    2. Schwab is display-only, hands-off.
    3. Never DELETE / DROP / TRUNCATE. This module is append-only.

The brief is read-only narrative. It NEVER places anything. Downstream agents
decide what to do with it; OllieTrades' existing gates still govern execution.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import time
import traceback
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=True)
except ImportError:
    pass
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# CONFIG (env-overridable; sane defaults so it runs out of the box)
# ---------------------------------------------------------------------------
OT_ROOT          = Path(os.getenv("OT_ROOT", Path(__file__).resolve().parent.parent))
BRIEF_DIR        = Path(os.getenv("OT_BRIEF_DIR", OT_ROOT / "data" / "xo_briefs"))
UHURA_SIGNAL_DIR = Path(os.getenv("OT_UHURA_SIGNAL_DIR", OT_ROOT / "data" / "signals"))
CORPUS_PATH      = Path(os.getenv("OT_BRIEF_CORPUS", OT_ROOT / "data" / "learning" / "xo_brief_corpus.jsonl"))

ANTHROPIC_MODEL  = os.getenv("XO_ANTHROPIC_MODEL", "claude-sonnet-4-6")
GROK_MODEL       = os.getenv("XO_GROK_MODEL", "grok-4.3")
WEB_SEARCH_MAX_USES = int(os.getenv("XO_WEB_SEARCH_MAX_USES", "8"))

# Cost cap (USD/day) — same shared-cap pattern as engine.team_advisor_grok's
# GROK_ADVISOR_DAILY_CAP: 90%-of-cap gates further spend, ledger is the
# common api_costs table (player_id='xo-brief'). Override with XO_DAILY_COST_CAP.
XO_DAILY_COST_CAP = float(os.getenv("XO_DAILY_COST_CAP", "0.50"))
_XO_PLAYER_ID = "xo-brief"
_XO_DB_PATH = OT_ROOT / "data" / "trader.db"
# Claude Sonnet 4.6 pricing (per 1M tokens) — used to log exact-enough spend
# into the shared ledger; no per-call tick-exact field on this endpoint.
_XO_INPUT_RATE_PER_M = 3.00
_XO_OUTPUT_RATE_PER_M = 15.00

# UHURA wiring: context-only, low weight, like the FRED bankrate signal.
UHURA_WEIGHT     = float(os.getenv("XO_UHURA_WEIGHT", "0.5"))

# --- Event-driven alert mode (volume from Holly/Ollie, or breaking news) ------
ALERT_PROVIDER        = os.getenv("XO_ALERT_PROVIDER", "claude")  # one fast call by default
ALERT_SEARCH_MAX_USES = int(os.getenv("XO_ALERT_SEARCH_MAX_USES", "4"))  # tighter than scheduled
ALERT_COOLDOWN_SEC    = int(os.getenv("XO_ALERT_COOLDOWN_SEC", "1200"))  # 20 min per symbol
ALERT_MAX_PER_HOUR    = int(os.getenv("XO_ALERT_MAX_PER_HOUR", "12"))    # global firehose cap
ALERT_QUEUE_PATH      = Path(os.getenv("OT_ALERT_QUEUE", OT_ROOT / "data" / "alerts" / "scanner_alerts.jsonl"))
ALERT_CURSOR_PATH     = Path(os.getenv("OT_ALERT_CURSOR", OT_ROOT / "data" / "alerts" / ".cursor"))
THROTTLE_STATE_PATH   = Path(os.getenv("OT_ALERT_THROTTLE", OT_ROOT / "data" / "alerts" / ".throttle.json"))

SACRED_RULES = (
    "SACRED RULES (non-negotiable):\n"
    "1. PAPER-ONLY. All execution is Alpaca paper. NEVER suggest, imply, or "
    "describe a live-money or Schwab order. Schwab is display-only, hands-off.\n"
    "2. This brief is read-only narrative. It does NOT place trades. It only "
    "informs the OllieTrades agents, which run their own gated paper execution.\n"
    "3. Never produce destructive database language (DELETE/DROP/TRUNCATE)."
)

# ---------------------------------------------------------------------------
# SESSION DEFINITIONS
# ---------------------------------------------------------------------------
SESSIONS: dict[str, dict[str, Any]] = {
    "premarket": {
        "et": "08:30",
        "focus": (
            "PRE-MARKET PLAN. It is before the 09:30 ET open. Cover: overnight "
            "futures (ES/NQ) and global tape, the day's economic calendar / Fed "
            "events, any overnight news on held names, gap-up/gap-down setups, and "
            "the plan for the session. No intraday calls yet — set the playbook."
        ),
    },
    "opening": {
        "et": "09:35",
        "focus": (
            "OPENING READ, ~5 min after the 09:30 open — the high-volume window. "
            "Cover: opening drive direction, gap follow-through vs. fade on held + "
            "watchlist names, and the first real volume leaders. This window is fast "
            "and noisy — flag fakeouts. NOTE: live volume spikes are caught in real "
            "time by the event-driven alert path (Holly/Ollie), so keep this brief to "
            "directional context, not chasing ticks."
        ),
    },
    "midday": {
        "et": "12:30",
        "focus": (
            "MIDDAY CHECK. Cover: is the morning trend intact or fading into the "
            "lunch lull, any midday reversals, stop-management on open positions, "
            "and whether to hold dry powder or deploy into a confirmed move."
        ),
    },
    "closing_30": {
        "et": "15:30",
        "focus": (
            "CLOSING 30 MINUTES. Cover: end-of-day positioning, the feel of "
            "market-on-close imbalances, locking gains on extended names, tightening "
            "or setting overnight trailing stops, and what to carry vs. flatten."
        ),
    },
    "afterhours_scalp": {
        "et": "16:15",
        "focus": (
            "AFTER-HOURS SCALP SCAN, just after the 16:00 close. Cover: the biggest "
            "AH movers, earnings/news reactions, and concrete PAPER scalp candidates "
            "for the after-hours session — each with a trigger, a stop, and an "
            "invalidation. Thin liquidity: flag it. Populate the 'scalps' array."
        ),
    },
}

# ---------------------------------------------------------------------------
# OUTPUT SCHEMA  (the contract every agent in the ecosystem reads)
# ---------------------------------------------------------------------------
SCHEMA_DOC = """
Respond with ONLY a single valid JSON object (no prose, no markdown fences),
matching this schema exactly:

{
  "macro": {
    "regime": "risk-on | risk-off | mixed | choppy",
    "tape": "<1-2 sentence read of the broad tape>",
    "catalysts": ["<dated/imminent catalysts, e.g. 'FOMC decision 6/17 2pm ET'>"]
  },
  "holdings_read": [
    {"symbol":"VST","stance":"hold|trim|add|exit|watch",
     "note":"<short>","stop_action":"<e.g. 'tighten to 9% / $185'>"}
  ],
  "actions": [
    {"symbol":"SPCX","action":"trim 1-2","rationale":"<short>","paper_only":true}
  ],
  "watchlist": [
    {"symbol":"OKLO","setup":"<short>","trigger":"<price/condition>",
     "invalidation":"<price/condition>"}
  ],
  "scalps": [
    {"symbol":"...","direction":"long|short","trigger":"...","stop":"...",
     "invalidation":"...","note":"thin AH liquidity"}
  ],
  "risk_flags": ["<anything that could blow up the plan>"],
  "sources": [{"title":"...","url":"..."}],
  "brief_markdown": "<the full human-readable XO brief, terse, ticker lines>",
  "rules_ack": ["paper_only","schwab_hands_off","no_destructive_db"]
}

Fill 'scalps' only for the afterhours_scalp session (empty list otherwise).
Every action MUST have "paper_only": true. brief_markdown is the human view.
"""


def build_system_prompt(session: str) -> str:
    return (
        "You are XO, the market analyst for OllieTrades — a multi-agent autonomous "
        "PAPER-trading research platform. You speak in a terse, decisive 'executive "
        "officer' voice: ticker lines, clear stances, real numbers, no filler.\n\n"
        f"{SACRED_RULES}\n\n"
        f"SESSION: {session}\n{SESSIONS[session]['focus']}\n\n"
        "Use web search aggressively to ground EVERY market claim in current, dated "
        "facts (prices, news, catalysts) — never assert price action from memory. "
        "Tie the macro read directly to the holdings you are given.\n\n"
        f"{SCHEMA_DOC}"
    )


def build_user_payload(session: str, portfolio: dict[str, Any]) -> str:
    """Canonical, provider-agnostic input. Claude and Grok get byte-identical text."""
    now_et = dt.datetime.now(ET)
    return json.dumps(
        {
            "as_of_et": now_et.isoformat(timespec="minutes"),
            "trading_day": now_et.date().isoformat(),
            "session": session,
            "portfolio": portfolio,
            "instruction": (
                "Produce the XO brief for this session per your system prompt and "
                "schema. Search the web for current conditions before writing."
            ),
        },
        indent=2,
    )


ALERT_FOCUS = (
    "EVENT ALERT — a live trigger fired on ONE symbol (a volume spike from the "
    "Holly/Ollie scanners, or a breaking-news hit). Be FAST and NARROW. Scope "
    "everything to this one name: what is moving it right now, is the move real or "
    "noise, is the volume confirming, and is there a PAPER trade here — give a "
    "trigger, a stop, and an invalidation. Do not survey the whole book; this is a "
    "single-name reaction. Put the trade in 'scalps' (or 'actions' if it's a held "
    "position). Search just enough to confirm the catalyst via web — speed matters."
)


def build_alert_system_prompt(symbol: str) -> str:
    return (
        "You are XO, the market analyst for OllieTrades — a multi-agent autonomous "
        "PAPER-trading research platform. Terse executive-officer voice.\n\n"
        f"{SACRED_RULES}\n\n"
        f"EVENT ALERT on {symbol}.\n{ALERT_FOCUS}\n\n"
        f"{SCHEMA_DOC}"
    )


def build_alert_payload(trigger: dict[str, Any], portfolio: dict[str, Any]) -> str:
    sym = trigger.get("symbol", "")
    held = next((p for p in portfolio.get("positions", []) if p.get("symbol") == sym), None)
    now_et = dt.datetime.now(ET)
    return json.dumps(
        {
            "as_of_et": now_et.isoformat(timespec="minutes"),
            "trading_day": now_et.date().isoformat(),
            "mode": "alert",
            "trigger": trigger,            # {kind, symbol, detail, source, ts}
            "position_if_held": held,      # None if not in the book
            "instruction": (
                f"A live trigger fired on {sym}. Confirm the catalyst via web search "
                "and produce a fast single-name reaction brief per the schema."
            ),
        },
        indent=2,
    )


# ===========================================================================
# INTEGRATION SEAMS  —  point these at OllieTrades' real internals.
# Defaults degrade gracefully so the module runs today.
# ===========================================================================
def get_portfolio_state() -> dict[str, Any]:
    """Live Alpaca PAPER state: cash, equity, day P&L, positions.
    Uses alpaca-py if keys are present; otherwise returns an empty skeleton."""
    try:
        from alpaca.trading.client import TradingClient  # type: ignore

        client = TradingClient(
            os.environ["APCA_API_KEY_ID"],
            os.environ["APCA_API_SECRET_KEY"],
            paper=True,  # RULE #1 enforced at the wire: paper account only.
        )
        acct = client.get_account()
        positions = client.get_all_positions()
        equity = float(acct.equity)
        last_equity = float(acct.last_equity or equity)
        return {
            "account": {
                "cash": float(acct.cash),
                "equity": equity,
                "portfolio_value": float(acct.portfolio_value),
                "day_pl": round(equity - last_equity, 2),
                "day_pl_pct": round((equity / last_equity - 1) * 100, 2) if last_equity else 0.0,
            },
            "positions": [
                {
                    "symbol": p.symbol,
                    "qty": float(p.qty),
                    "avg_entry": float(p.avg_entry_price),
                    "current": float(p.current_price),
                    "market_value": float(p.market_value),
                    "unrealized_pl": float(p.unrealized_pl),
                    "unrealized_plpc": round(float(p.unrealized_plpc) * 100, 2),
                }
                for p in positions
            ],
        }
    except Exception as e:  # pragma: no cover - degraded mode
        print(f"[xo_brief] portfolio pull degraded ({e}); sending empty book", file=sys.stderr)
        return {"account": {}, "positions": [], "_note": "live state unavailable"}


def write_to_bridge(session: str, envelope: dict[str, Any]) -> Path:
    """Persist markdown + JSON for the Bridge. Append-only filenames per run."""
    BRIEF_DIR.mkdir(parents=True, exist_ok=True)
    stamp = envelope["generated_at"].replace(":", "").replace("-", "")
    base = BRIEF_DIR / f"{envelope['trading_day']}_{session}_{envelope['provider']}_{stamp}"
    base.with_suffix(".json").write_text(json.dumps(envelope, indent=2))
    base.with_suffix(".md").write_text(envelope.get("brief_markdown", ""))
    # Stable "latest" pointers the Bridge can render without globbing.
    (BRIEF_DIR / f"latest_{session}.md").write_text(envelope.get("brief_markdown", ""))
    (BRIEF_DIR / "latest.json").write_text(json.dumps(envelope, indent=2))
    return base.with_suffix(".json")


def emit_to_uhura(envelope: dict[str, Any]) -> None:
    """Emit a context-only signal for UHURA v2, same pattern as fred_bankrate
    (low weight, append to a polled signal file). Never overwrites history."""
    UHURA_SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    signal = {
        "source": "xo_brief",
        "type": "context",
        "weight": UHURA_WEIGHT,
        "session": envelope["session"],
        "provider": envelope["provider"],
        "generated_at": envelope["generated_at"],
        "regime": envelope.get("macro", {}).get("regime"),
        "risk_flags": envelope.get("risk_flags", []),
        "actions": envelope.get("actions", []),
    }
    with (UHURA_SIGNAL_DIR / "xo_brief.signal.jsonl").open("a") as f:
        f.write(json.dumps(signal) + "\n")


def append_learning_corpus(envelope: dict[str, Any]) -> None:
    """Append one training row per brief for the learning agents (debate/eval).
    JSONL, append-only — never rewrites prior rows."""
    CORPUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "id": f"{envelope['trading_day']}_{envelope['session']}_{envelope['provider']}_{envelope['generated_at']}",
        "session": envelope["session"],
        "provider": envelope["provider"],
        "generated_at": envelope["generated_at"],
        "input_portfolio": envelope.get("_input_portfolio"),
        "output": {k: v for k, v in envelope.items() if not k.startswith("_")},
    }
    with CORPUS_PATH.open("a") as f:
        f.write(json.dumps(row) + "\n")


# ===========================================================================
# COST GUARD (shared api_costs ledger — same pattern as engine.team_advisor_grok)
# ===========================================================================
def _xo_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_XO_DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def get_daily_cost() -> float:
    """Total XO brief spend today (UTC date) from the shared api_costs ledger."""
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    try:
        conn = _xo_conn()
        row = conn.execute(
            "SELECT SUM(cost_usd) AS total FROM api_costs "
            "WHERE player_id=? AND date(timestamp)=?",
            (_XO_PLAYER_ID, today),
        ).fetchone()
        conn.close()
        return float(row["total"] or 0) if row else 0.0
    except Exception:
        return 0.0


def _log_cost(input_tok: int, output_tok: int, cost_usd: float, call_type: str) -> None:
    try:
        conn = _xo_conn()
        conn.execute(
            "INSERT INTO api_costs "
            "(player_id, call_type, input_tokens, output_tokens, cost_usd, timestamp) "
            "VALUES (?,?,?,?,?,?)",
            (_XO_PLAYER_ID, call_type, input_tok, output_tok, cost_usd,
             dt.datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[xo_brief] cost log failed: {e}", file=sys.stderr)


# ===========================================================================
# PROVIDER ADAPTERS
# ===========================================================================
def call_claude(system_prompt: str, user_payload: str, max_uses: int = WEB_SEARCH_MAX_USES) -> str:
    import anthropic  # pip install anthropic

    daily_cost = get_daily_cost()
    if daily_cost >= XO_DAILY_COST_CAP * 0.9:
        raise RuntimeError(
            f"xo_brief daily cost cap reached (${daily_cost:.2f} of ${XO_DAILY_COST_CAP:.2f} "
            "90% threshold) — Claude call skipped, try --provider grok or again tomorrow"
        )

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    resp = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=4096,
        system=system_prompt,
        messages=[{"role": "user", "content": user_payload}],
        tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": max_uses}],
    )
    in_tok = getattr(resp.usage, "input_tokens", 0) or 0
    out_tok = getattr(resp.usage, "output_tokens", 0) or 0
    cost = (in_tok / 1_000_000) * _XO_INPUT_RATE_PER_M + (out_tok / 1_000_000) * _XO_OUTPUT_RATE_PER_M
    _log_cost(in_tok, out_tok, cost, "xo_brief_claude")
    # With server-side web search the response is multi-block; the answer lives
    # in the trailing text block(s).
    return "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")


def call_grok(system_prompt: str, user_payload: str, max_uses: int = WEB_SEARCH_MAX_USES) -> str:
    from openai import OpenAI  # pip install openai  (xAI is OpenAI-compatible)

    client = OpenAI(api_key=os.environ["XAI_API_KEY"], base_url="https://api.x.ai/v1")
    resp = client.chat.completions.create(
        model=GROK_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_payload},
        ],
        max_completion_tokens=4096,  # grok-4.x reasoning models use this, not max_tokens
        extra_body={
            "search_parameters": {  # xAI Live Search, forced on for time-sensitive briefs
                "mode": "on",
                "return_citations": True,
                "max_search_results": max(5, max_uses * 2),
                "sources": [{"type": "web"}, {"type": "news"}, {"type": "x"}],
            }
        },
    )
    return resp.choices[0].message.content or ""


PROVIDERS = {"claude": call_claude, "grok": call_grok}


# ===========================================================================
# PARSE + GUARD
# ===========================================================================
def extract_json(raw: str) -> dict[str, Any]:
    """Pull the JSON object out of a model response, tolerating stray fences/prose."""
    cleaned = re.sub(r"```(?:json)?|```", "", raw).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start == -1 or end == -1:
            raise
        return json.loads(cleaned[start : end + 1])


_DESTRUCTIVE = re.compile(r"\b(DELETE|DROP|TRUNCATE)\b", re.IGNORECASE)


def validate_and_guard(envelope: dict[str, Any]) -> dict[str, Any]:
    """Enforce the sacred rules on parsed output before it touches the ecosystem."""
    envelope.setdefault("macro", {})
    for key in ("holdings_read", "actions", "watchlist", "scalps", "risk_flags", "sources"):
        envelope.setdefault(key, [])
    # Force paper_only on every action; strip anything that smells like live exec.
    for a in envelope["actions"]:
        a["paper_only"] = True
    envelope["rules_ack"] = ["paper_only", "schwab_hands_off", "no_destructive_db"]
    # Defensive: reject destructive DB language anywhere in the text.
    blob = json.dumps(envelope)
    if _DESTRUCTIVE.search(blob):
        raise ValueError("Guard tripped: destructive DB language in brief output.")
    return envelope


# ===========================================================================
# ORCHESTRATION
# ===========================================================================
def run_brief(session: str, provider: str) -> dict[str, Any]:
    if session not in SESSIONS:
        raise ValueError(f"unknown session {session!r}; choose from {list(SESSIONS)}")
    if provider not in PROVIDERS:
        raise ValueError(f"unknown provider {provider!r}; choose from {list(PROVIDERS)}")

    portfolio = get_portfolio_state()
    system_prompt = build_system_prompt(session)
    user_payload = build_user_payload(session, portfolio)

    raw = PROVIDERS[provider](system_prompt, user_payload)
    envelope = validate_and_guard(extract_json(raw))

    # Server-stamped fields — never trust the model for these.
    envelope.update(
        schema_version="1.0",
        session=session,
        provider=provider,
        generated_at=dt.datetime.now(ET).isoformat(timespec="seconds"),
        trading_day=dt.datetime.now(ET).date().isoformat(),
        _input_portfolio=portfolio,
    )

    write_to_bridge(session, envelope)
    emit_to_uhura(envelope)
    append_learning_corpus(envelope)
    return envelope


def run_both(session: str) -> list[dict[str, Any]]:
    """Run Claude + Grok on the identical request. Both rows land in the corpus,
    giving the learning agents a built-in two-model debate to compare/score."""
    out = []
    for provider in ("claude", "grok"):
        try:
            out.append(run_brief(session, provider))
        except Exception as e:
            print(f"[xo_brief] {provider} failed: {e}", file=sys.stderr)
            traceback.print_exc()
    return out


# ===========================================================================
# EVENT-DRIVEN ALERT PATH  (Holly/Ollie volume + breaking news)
# ===========================================================================
class AlertThrottle:
    """Per-symbol cooldown + global hourly cap, persisted so it survives restarts.
    Keeps one ripping name from firing a brief on every tick."""

    def __init__(self, path: Path, cooldown_sec: int, max_per_hour: int):
        self.path = path
        self.cooldown = cooldown_sec
        self.max_per_hour = max_per_hour
        self.state = {"last": {}, "hour": []}
        if path.exists():
            try:
                self.state = json.loads(path.read_text())
            except Exception:
                pass

    def allow(self, symbol: str) -> bool:
        now = time.time()
        self.state["hour"] = [t for t in self.state["hour"] if now - t < 3600]
        if len(self.state["hour"]) >= self.max_per_hour:
            return False
        return now - self.state["last"].get(symbol, 0) >= self.cooldown

    def record(self, symbol: str) -> None:
        now = time.time()
        self.state["last"][symbol] = now
        self.state["hour"].append(now)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.state))


def poll_scanner_alerts() -> list[dict[str, Any]]:
    """SEAM: read NEW triggers off the shared alert queue.

    Holly, Ollie, and the news watcher all append one JSON line per event to
    ALERT_QUEUE_PATH:
        {"kind":"volume","symbol":"VST","detail":"3.2x rel-vol, new 30d high",
         "source":"holly","ts":"2026-06-16T09:41:00-04:00"}
        {"kind":"news","symbol":"SPCX","detail":"announces AI deal","source":"alpaca_news","ts":...}

    This reads only lines appended since the last poll (byte-offset cursor), so it
    never reprocesses history and never mutates the queue (append-only).
    """
    if not ALERT_QUEUE_PATH.exists():
        return []
    offset = 0
    if ALERT_CURSOR_PATH.exists():
        try:
            offset = int(ALERT_CURSOR_PATH.read_text().strip() or "0")
        except Exception:
            offset = 0
    size = ALERT_QUEUE_PATH.stat().st_size
    if offset > size:  # queue was rotated/truncated upstream — restart from top
        offset = 0
    triggers: list[dict[str, Any]] = []
    with ALERT_QUEUE_PATH.open("r") as f:
        f.seek(offset)
        for line in f:
            line = line.strip()
            if line:
                try:
                    triggers.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        new_offset = f.tell()
    ALERT_CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    ALERT_CURSOR_PATH.write_text(str(new_offset))
    return triggers


def run_alert_brief(trigger: dict[str, Any], provider: str = ALERT_PROVIDER) -> dict[str, Any]:
    """Fast, single-symbol reaction brief. Same envelope + sinks as a session
    brief, so every agent reads one contract whether it's scheduled or event-driven."""
    if provider not in PROVIDERS:
        raise ValueError(f"unknown provider {provider!r}")
    symbol = trigger.get("symbol", "?")
    portfolio = get_portfolio_state()
    system_prompt = build_alert_system_prompt(symbol)
    user_payload = build_alert_payload(trigger, portfolio)

    raw = PROVIDERS[provider](system_prompt, user_payload, max_uses=ALERT_SEARCH_MAX_USES)
    envelope = validate_and_guard(extract_json(raw))
    envelope.update(
        schema_version="1.0",
        session=f"alert:{trigger.get('kind', 'event')}:{symbol}",
        provider=provider,
        generated_at=dt.datetime.now(ET).isoformat(timespec="seconds"),
        trading_day=dt.datetime.now(ET).date().isoformat(),
        _trigger=trigger,
        _input_portfolio=portfolio,
    )
    write_to_bridge(envelope["session"].replace(":", "_"), envelope)
    emit_to_uhura(envelope)
    append_learning_corpus(envelope)
    return envelope


def watch(poll_interval: int = 5, provider: str = ALERT_PROVIDER) -> None:
    """Long-running daemon: poll the alert queue, throttle, fire scoped briefs.
    Run under your existing supervisor (the trader watchdog / a LaunchDaemon),
    so it self-revives like cloudflared and the keepalive crons."""
    throttle = AlertThrottle(THROTTLE_STATE_PATH, ALERT_COOLDOWN_SEC, ALERT_MAX_PER_HOUR)
    print(f"[xo_brief] watch started; polling {ALERT_QUEUE_PATH} every {poll_interval}s", file=sys.stderr)
    while True:
        try:
            for trig in poll_scanner_alerts():
                sym = trig.get("symbol")
                if not sym:
                    continue
                if not throttle.allow(sym):
                    continue  # cooled down or hourly cap hit
                try:
                    env = run_alert_brief(trig, provider)
                    throttle.record(sym)
                    print(f"[xo_brief] ALERT {sym} ({trig.get('kind')}) -> brief fired", file=sys.stderr)
                    print(env.get("brief_markdown", ""))
                except Exception as e:
                    print(f"[xo_brief] alert brief failed for {sym}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[xo_brief] watch loop error: {e}", file=sys.stderr)
        time.sleep(poll_interval)


CRONTAB = """# OllieTrades XO briefs — Arizona box, so pin ET (AZ has no DST).
# Scheduled heartbeat (5 anchors). Intraday volume/news is handled separately
# by the event-driven watcher (run `--watch` under your supervisor, NOT cron).
CRON_TZ=America/New_York
30 8  * * 1-5  cd {root} && python -m engine.xo_brief --session premarket        --provider both >> logs/xo_brief.log 2>&1
35 9  * * 1-5  cd {root} && python -m engine.xo_brief --session opening          --provider both >> logs/xo_brief.log 2>&1
30 12 * * 1-5  cd {root} && python -m engine.xo_brief --session midday           --provider both >> logs/xo_brief.log 2>&1
30 15 * * 1-5  cd {root} && python -m engine.xo_brief --session closing_30       --provider both >> logs/xo_brief.log 2>&1
15 16 * * 1-5  cd {root} && python -m engine.xo_brief --session afterhours_scalp --provider both >> logs/xo_brief.log 2>&1

# Event-driven alert daemon (long-running; supervise like the trader watchdog):
#   cd {root} && python -m engine.xo_brief --watch --provider claude
"""


def _auto_session() -> str:
    """Pick the nearest past session bracket by ET wall clock."""
    t = dt.datetime.now(ET).time()
    if t < dt.time(9, 35):
        return "premarket"
    if t < dt.time(12, 30):
        return "opening"
    if t < dt.time(15, 30):
        return "midday"
    if t < dt.time(16, 15):
        return "closing_30"
    return "afterhours_scalp"


def _print_results(results: list[dict[str, Any]]) -> None:
    for r in results:
        print(f"\n=== {r['provider'].upper()} / {r['session']} @ {r['generated_at']} ===")
        print(r.get("brief_markdown", "(no markdown)"))


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)

    # ------------------------------------------------------------------
    # SHORT-FORM DISPATCH  (positional sugar; no leading dashes)
    # ------------------------------------------------------------------
    if not raw or (raw and not raw[0].startswith("-")):
        first = raw[0] if raw else ""

        # xo watch [claude|grok]
        if first == "watch":
            provider = raw[1] if len(raw) > 1 and raw[1] in PROVIDERS else ALERT_PROVIDER
            watch(provider=provider)
            return 0

        # xo trigger SYMBOL ["detail string"] [--reason kind]
        if first == "trigger":
            if len(raw) < 2:
                print("usage: xo trigger SYMBOL [\"detail\"] [--reason volume|news|gap]", file=sys.stderr)
                return 1
            symbol = raw[1].upper()
            detail = raw[2] if len(raw) > 2 and not raw[2].startswith("-") else ""
            reason = "volume"
            provider = ALERT_PROVIDER
            for i, a in enumerate(raw[2:], 2):
                if a == "--reason" and i + 1 < len(raw):
                    reason = raw[i + 1]
                if a == "--provider" and i + 1 < len(raw):
                    provider = raw[i + 1]
            trig = {
                "kind": reason,
                "symbol": symbol,
                "detail": detail,
                "source": "manual",
                "ts": dt.datetime.now(ET).isoformat(timespec="seconds"),
            }
            env = run_alert_brief(trig, provider=provider)
            print(f"\n=== ALERT {symbol} ({reason}) / {env['provider']} ===")
            print(env.get("brief_markdown", "(no markdown)"))
            return 0

        # xo [session] [--provider claude|grok|both]  OR bare `xo`
        if first in SESSIONS or first == "":
            session = first if first in SESSIONS else _auto_session()
            remaining = raw[1:] if first in SESSIONS else raw
            provider = "both"
            for i, a in enumerate(remaining):
                if a == "--provider" and i + 1 < len(remaining):
                    provider = remaining[i + 1]
                elif a in PROVIDERS or a == "both":
                    provider = a
            results = run_both(session) if provider == "both" else [run_brief(session, provider)]
            _print_results(results)
            return 0

    # ------------------------------------------------------------------
    # FULL --FLAG FORM  (cron lines, scripting, --print-crontab)
    # ------------------------------------------------------------------
    ap = argparse.ArgumentParser(description="OllieTrades XO brief generator")
    ap.add_argument("--session", choices=list(SESSIONS), help="scheduled heartbeat brief")
    ap.add_argument("--provider", choices=["claude", "grok", "both"], default="both")
    ap.add_argument("--print-crontab", action="store_true", help="emit crontab lines and exit")
    ap.add_argument("--watch", action="store_true", help="run the alert daemon")
    ap.add_argument("--poll-interval", type=int, default=5, help="watch poll seconds (default 5)")
    ap.add_argument("--trigger", metavar="SYMBOL", help="fire one alert brief by hand")
    ap.add_argument("--reason", default="volume", help="alert kind for --trigger (volume|news|gap)")
    ap.add_argument("--detail", default="", help="alert detail string for --trigger")
    args = ap.parse_args(raw)

    if args.print_crontab:
        print(CRONTAB.format(root=OT_ROOT))
        return 0

    alert_provider = ALERT_PROVIDER if args.provider == "both" else args.provider

    if args.watch:
        watch(poll_interval=args.poll_interval, provider=alert_provider)
        return 0

    if args.trigger:
        trig = {
            "kind": args.reason,
            "symbol": args.trigger.upper(),
            "detail": args.detail,
            "source": "manual",
            "ts": dt.datetime.now(ET).isoformat(timespec="seconds"),
        }
        env = run_alert_brief(trig, provider=alert_provider)
        print(f"\n=== ALERT {trig['symbol']} ({trig['kind']}) / {env['provider']} ===")
        print(env.get("brief_markdown", "(no markdown)"))
        return 0

    if not args.session:
        ap.error("need one of --session, --watch, --trigger, or --print-crontab")

    results = run_both(args.session) if args.provider == "both" else [run_brief(args.session, args.provider)]
    _print_results(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
