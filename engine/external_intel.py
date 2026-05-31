"""engine/external_intel.py — HM-EXTERNAL-INTEL: capture the Admiral's pasted intelligence
into structured storage OT can learn from. (Months of TI emails / Bloomberg / Rallies
reasoning lived only in chats; external_picks captured 4 picks once then died — this revives
+ sustains it.)

TWO TIERS, each proves its OWN value:
  TIER 1 (structured picks, high-value, ~no NLP): TI Swing Picks are already semi-structured
    (ticker/action/entry/stop/note) — the pick IS a tradeable signal. Capture → watchlist +
    follow-TI shadow (tracked, not auto-traded) + cross-validation vs OT's own picks.
  TIER 2 (free-text NLP, research, VALIDATE-OR-IT'S-NOISE): NER tickers + sentiment + catalyst
    from prose → CANDIDATE features for Ollie's GB gate, which must BEAT the no-edge baseline
    (Phase 1+2 showed clean features gave OOS AUC 0.534 — text features face a HIGH bar). Kept
    OUT of live gating until proven OOS-positive in shadow. Built to learn WHETHER text helps.

INGESTION (both feed the same store):
  - paste-box: dashboard "paste intelligence" (source+date+text) → capture_paste()
  - email-forward: a cron polls an inbox → capture_picks() (DEPENDS on an inbox OT can read —
    open question for the Admiral; paste-box covers TI picks in the interim).

Nothing here auto-trades or live-gates. Propose/stage only.
"""
from __future__ import annotations

import os
import re
import json
import sqlite3
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
DB = "data/trader.db"

# ── Schema ────────────────────────────────────────────────────────────────────
def ensure_tables() -> None:
    c = sqlite3.connect(DB, timeout=30.0)
    # external_picks already exists (revived as the live target); ensure shape.
    c.execute("""CREATE TABLE IF NOT EXISTS external_picks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, pick_date TEXT, ticker TEXT,
        action TEXT, entry REAL, stop REAL, note TEXT, raw_json TEXT, submitted_at TEXT)""")
    # free-text intelligence (Tier 2 capture) + extracted features
    c.execute("""CREATE TABLE IF NOT EXISTS external_intel_text (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, intel_date TEXT, raw_text TEXT,
        tickers TEXT, sentiment REAL, catalysts TEXT, captured_at TEXT,
        UNIQUE(source, intel_date, raw_text))""")
    # follow-TI shadow tracker (tracked, NOT auto-traded; measured by price lookup)
    c.execute("""CREATE TABLE IF NOT EXISTS ti_shadow_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT, pick_id INTEGER, ticker TEXT, source TEXT,
        entry REAL, stop REAL, opened_at TEXT, exit_price REAL, exit_at TEXT,
        pnl_pct REAL, status TEXT DEFAULT 'open')""")
    c.commit(); c.close()


# ── TIER 1: structured TI pick parsing + capture ──────────────────────────────
# Lines look like: "AMX buy @26.54 stop 25.61 — 4-day high breakout"
#                  "PLTR  buy  entry 148.29  stop 143.29  pullback to 20/50 MA"
# Decomposed field extraction (a single mega-regex dropped the optional entry group —
# 2026-05-31 verify bug); each field has its own anchored search → robust.
_TICKER_RE = re.compile(r"^\s*\$?([A-Z]{1,5})\b")
_ACTION_RE = re.compile(r"\b(buy|sell|long|short|watch)\b", re.IGNORECASE)
_ENTRY_RE = re.compile(r"(?:@|\bentry[:\s]*)\s*\$?(\d+(?:\.\d+)?)", re.IGNORECASE)
_STOP_RE = re.compile(r"\bstop[:\s]*\$?(\d+(?:\.\d+)?)", re.IGNORECASE)
_NOTE_RE = re.compile(r"[—\-:]\s*([A-Za-z].+)$")
_NON_TICKERS = {"BUY", "SELL", "THE", "AND", "FOR", "AT", "ON", "TI", "MA", "A", "I",
                "OT", "AI", "EPS", "PE", "USD"}


def parse_ti_picks(text: str, source: str, pick_date: str) -> list[dict]:
    """Parse a pasted TI Swing Picks block into structured picks. One pick per non-empty
    line that names a ticker. Each field extracted by its own anchored pattern (robust to
    field order). Keeps raw line in raw_json for audit."""
    picks: list[dict] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or len(line) < 3:
            continue
        tm = _TICKER_RE.match(line)
        if not tm:
            continue
        tk = tm.group(1).upper()
        if tk in _NON_TICKERS:
            continue
        am = _ACTION_RE.search(line)
        em = _ENTRY_RE.search(line)
        sm = _STOP_RE.search(line)
        nm = _NOTE_RE.search(line)
        # a real pick line should carry at least an action OR an entry/stop level
        if not (am or em or sm):
            continue
        picks.append({
            "ticker": tk,
            "action": (am.group(1).lower() if am else "buy"),
            "entry": float(em.group(1)) if em else None,
            "stop": float(sm.group(1)) if sm else None,
            "note": (nm.group(1).strip() if nm else ""),
            "raw_json": json.dumps({"line": line, "source": source}),
        })
    return picks


def capture_picks(picks: list[dict], source: str, pick_date: str) -> int:
    """Insert structured picks into external_picks (idempotent on source+date+ticker)."""
    ensure_tables()
    c = sqlite3.connect(DB, timeout=30.0)
    now = datetime.now(timezone.utc).isoformat()
    added = 0
    for p in picks:
        dup = c.execute(
            "SELECT 1 FROM external_picks WHERE source=? AND pick_date=? AND ticker=?",
            (source, pick_date, p["ticker"])).fetchone()
        if dup:
            continue
        c.execute(
            """INSERT INTO external_picks
               (source,pick_date,ticker,action,entry,stop,note,raw_json,submitted_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (source, pick_date, p["ticker"], p["action"], p.get("entry"), p.get("stop"),
             p.get("note"), p.get("raw_json"), now))
        added += 1
    c.commit(); c.close()
    return added


# ── TIER 1 payoff (i): watchlist ──────────────────────────────────────────────
def ti_watchlist(days: int = 7) -> list[dict]:
    c = sqlite3.connect(DB, timeout=30.0); c.row_factory = sqlite3.Row
    rows = c.execute(
        "SELECT ticker, action, entry, stop, note, source, pick_date FROM external_picks "
        "WHERE pick_date >= date('now', ?) ORDER BY pick_date DESC",
        (f"-{days} day",)).fetchall()
    c.close()
    return [dict(r) for r in rows]


# ── TIER 1 payoff (ii): follow-TI shadow (tracked, NOT auto-traded) ────────────
def ti_shadow_record() -> int:
    """Open a shadow position for each captured pick not yet shadowed. Tracked in
    ti_shadow_trades + measured by price lookup later — NOT a real trade (no trades-table
    write, no broker). Mirrors the ghost-tracking architecture (parallel measurement)."""
    ensure_tables()
    c = sqlite3.connect(DB, timeout=30.0); c.row_factory = sqlite3.Row
    picks = c.execute(
        """SELECT p.id, p.ticker, p.entry, p.stop, p.source FROM external_picks p
           LEFT JOIN ti_shadow_trades s ON s.pick_id = p.id
           WHERE s.id IS NULL AND p.action IN ('buy','long')""").fetchall()
    now = datetime.now(timezone.utc).isoformat()
    n = 0
    for p in picks:
        c.execute(
            "INSERT INTO ti_shadow_trades (pick_id,ticker,source,entry,stop,opened_at,status) "
            "VALUES (?,?,?,?,?,?, 'open')",
            (p["id"], p["ticker"], p["source"], p["entry"], p["stop"], now))
        n += 1
    c.commit(); c.close()
    return n


def ti_shadow_scorecard(price_fn=None) -> dict:
    """Score the follow-TI shadow: did following TI's picks make money? price_fn(ticker)->px
    (defaults to engine.market_data.get_stock_price). Clean-aggregator discipline: separable,
    date-stamped, parallel to the live book (never co-mingled)."""
    if price_fn is None:
        try:
            from engine.market_data import get_stock_price
            price_fn = lambda t: float((get_stock_price(t) or {}).get("price") or 0)
        except Exception:
            price_fn = lambda t: 0.0
    c = sqlite3.connect(DB, timeout=30.0); c.row_factory = sqlite3.Row
    rows = c.execute("SELECT * FROM ti_shadow_trades").fetchall()
    rets, stopped = [], 0
    for r in rows:
        entry = r["entry"] or 0
        stop = r["stop"] or 0
        if not entry:
            continue
        px = r["exit_price"] if r["status"] == "closed" else price_fn(r["ticker"])
        if not px:
            continue
        # Apply the stop as a floor: if current price is at/below the stop, the swing
        # would have exited at the stop (approximation — no intraday history, so a name
        # that dipped to stop then recovered is scored mark-to-current; flagged below).
        if stop and px <= stop:
            px = stop
            stopped += 1
        rets.append((px - entry) / entry * 100)
    c.close()
    if not rets:
        return {"n": 0, "note": "no shadow trades with prices yet"}
    wins = sum(1 for x in rets if x > 0)
    return {
        "n": len(rets), "stopped_out": stopped,
        "avg_return_pct": round(sum(rets) / len(rets), 2),
        "win_rate": round(wins / len(rets) * 100, 1),
        "best_pct": round(max(rets), 1), "worst_pct": round(min(rets), 1),
        "note": ("follow-TI shadow — TRACKED, not auto-traded (parallel ghost tracker, "
                 "clean-aggregator discipline: separable, date-stamped, never co-mingled "
                 "with the live book). Stop applied as a floor; mark-to-current otherwise. "
                 "Early/low-N — needs time."),
    }


# ── TIER 1 payoff (iii): cross-validation OT picks vs TI answer-key ────────────
def cross_validate(ot_picks: list[str], ti_picks: list[str]) -> dict:
    """Agreement/miss diff between OT's own swing picks and TI's (the answer key)."""
    ot, ti = set(t.upper() for t in ot_picks), set(t.upper() for t in ti_picks)
    return {
        "agree": sorted(ot & ti),                 # high-confidence (both flagged)
        "ot_only": sorted(ot - ti),               # OT's unique calls (TI didn't flag)
        "ti_only_ot_missed": sorted(ti - ot),     # TI caught, OT MISSED — learn why
        "overlap_pct": round(len(ot & ti) / len(ti) * 100, 1) if ti else 0.0,
    }


# ── TIER 2: free-text capture + extraction (research; validate-or-it's-noise) ──
_POS = {"beat", "surge", "rally", "upgrade", "bullish", "breakout", "soar", "jump", "gain",
        "strong", "outperform", "buy", "accumulate", "momentum", "record", "growth"}
_NEG = {"miss", "plunge", "downgrade", "bearish", "breakdown", "fall", "drop", "weak",
        "underperform", "sell", "cut", "warn", "lawsuit", "probe", "decline", "loss"}
_CATALYSTS = {"earnings", "fda", "upgrade", "downgrade", "merger", "acquisition", "guidance",
              "buyback", "dividend", "split", "offering", "approval", "recall", "lawsuit",
              "partnership", "contract", "ipo", "insider"}


def _known_tickers() -> set[str]:
    try:
        c = sqlite3.connect(DB, timeout=15.0)
        s = set(r[0] for r in c.execute("SELECT DISTINCT symbol FROM scan_universe"))
        c.close()
        return s
    except Exception:
        return set()


def extract_features(text: str) -> dict:
    """NER tickers ($CASHTAG or known-symbol match) + lexicon sentiment + catalyst tags.
    Deliberately simple — the POINT is whether even rough text features add OOS signal."""
    txt = text or ""
    cash = set(m.group(1).upper() for m in re.finditer(r"\$([A-Za-z]{1,5})\b", txt))
    known = _known_tickers()
    words = re.findall(r"\b[A-Z]{1,5}\b", txt)
    matched = cash | {w for w in words if w in known}
    low = txt.lower()
    pos = sum(low.count(w) for w in _POS)
    neg = sum(low.count(w) for w in _NEG)
    sentiment = round((pos - neg) / (pos + neg), 3) if (pos + neg) else 0.0
    cats = sorted({k for k in _CATALYSTS if k in low})
    return {"tickers": sorted(matched), "sentiment": sentiment, "catalysts": cats}


def capture_text(source: str, intel_date: str, text: str) -> dict:
    """Tier-2 capture: store raw free text + extracted features. Trivial capture; the HARD
    part (does it add tradeable signal?) is gated separately in tier2_validation_status."""
    ensure_tables()
    f = extract_features(text)
    c = sqlite3.connect(DB, timeout=30.0)
    try:
        c.execute(
            "INSERT OR IGNORE INTO external_intel_text "
            "(source,intel_date,raw_text,tickers,sentiment,catalysts,captured_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (source, intel_date, text, json.dumps(f["tickers"]), f["sentiment"],
             json.dumps(f["catalysts"]), datetime.now(timezone.utc).isoformat()))
        c.commit()
    finally:
        c.close()
    return f


def tier2_validation_status() -> dict:
    """Can Tier-2 text features even be OOS-validated yet? Honest gate: they must be JOINED to
    labeled trade outcomes (like backtest_v5_ollie_decisions, ~540 rows) and BEAT OOS AUC
    0.534. Today there is no text-corpus-aligned-to-outcomes — so the gauntlet can't run.
    Reports what's needed; does NOT fake a result."""
    c = sqlite3.connect(DB, timeout=15.0)
    n_text = c.execute("SELECT COUNT(*) FROM external_intel_text").fetchone()[0] \
        if c.execute("SELECT 1 FROM sqlite_master WHERE name='external_intel_text'").fetchone() else 0
    c.close()
    MIN_CORPUS = 200   # rough floor to attempt a regularized text-feature gate
    return {
        "text_corpus_rows": n_text,
        "min_corpus_to_attempt": MIN_CORPUS,
        "ready_to_validate": n_text >= MIN_CORPUS,
        "baseline_to_beat": "Ollie GB gate OOS AUC 0.534 (clean features had no edge)",
        "blocker": ("Need ≥%d captured text items ALIGNED to trade outcomes (symbol+date→"
                    "realized PnL) to add text features to the GB gate and run the SAME OOS "
                    "gauntlet. Capturing text is done; outcome-alignment + corpus size is the "
                    "gate. Until then text features stay OUT of any live gating." % MIN_CORPUS),
        "honest_expectation": ("Clean price features showed NO OOS edge (0.534); text features "
                               "may add NOISE not signal. Build to LEARN whether they help — "
                               "no improvement is a valid finding."),
    }


def capture_paste(source: str, intel_date: str, text: str, kind: str = "auto") -> dict:
    """Unified paste-box entry. kind='picks' → Tier-1 structured; kind='text' → Tier-2 free
    text; 'auto' → try pick-parse, fall back to text capture. Returns what landed."""
    ensure_tables()
    if kind in ("picks", "auto"):
        picks = parse_ti_picks(text, source, intel_date)
        # Route to Tier 1 when it's structured picks: explicit kind='picks', OR multiple
        # parsed picks, OR any pick carries a concrete entry/stop level (a prose sentence
        # that merely starts with a ticker-like word won't have "stop 4.50"). This fixes a
        # single-line pick paste being misrouted to Tier-2 text (2026-05-31 verify bug).
        structured = any(p.get("entry") is not None or p.get("stop") is not None for p in picks)
        if picks and (kind == "picks" or len(picks) >= 2 or structured):
            added = capture_picks(picks, source, intel_date)
            return {"tier": 1, "captured_picks": added, "parsed": len(picks)}
    f = capture_text(source, intel_date, text)
    return {"tier": 2, "extracted": f}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json as _j
    ensure_tables()
    print("ensure_tables OK")
