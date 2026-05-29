#!/usr/bin/env python3
"""
Build a Plutus-Critic training corpus (Format A: grade + lesson) from trader.db.

Sources joined into a single corpus:
  - trade_outcomes JOIN trades  (fleet outcomes, P&L-derived grades)
  - daily_lessons               (explicit grade + lesson text)

This is a v4-shape extractor. NOT the HM-PLUTUS-V6-CORPUS build — see that
spec for the rallie.ai + Plutus-output-review + v4/v5-retention pipeline,
which is currently gated SPEC-ONLY pending HM-LESSON-VERIFY-DATA-SOURCE-FIRST.

Outputs:
  data/plutus_corpus_trader_db.jsonl            (full corpus)
  data/plutus_corpus_trader_db.train.jsonl      (80%)
  data/plutus_corpus_trader_db.val.jsonl        (10%)
  data/plutus_corpus_trader_db.test.jsonl       (10%)
  data/plutus_corpus_trader_db.stats.json       (counts, distributions, mismatches)
"""

from __future__ import annotations

import hashlib
import json
import random
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "trader.db"
OUT_DIR = ROOT / "data"
STEM = "plutus_corpus_trader_db"

SEED = 42
INSTRUCTION = (
    "You are Plutus, an expert trade critic. "
    "Grade this trade and explain why."
)


def grade_from_pnl(pnl_pct: float | None) -> str | None:
    """Synthesize letter grade from realized P&L percent. None if no P&L."""
    if pnl_pct is None:
        return None
    if pnl_pct >= 5:
        return "A"
    if pnl_pct >= 1:
        return "B"
    if pnl_pct >= -1:
        return "C"
    if pnl_pct >= -3:
        return "D"
    return "F"


def build_input(t: dict[str, Any]) -> str:
    return (
        "Trade to grade:\n"
        f"  Symbol: {t.get('symbol', '?')}\n"
        f"  Action: {t.get('action', '?')}\n"
        f"  Entry:  ${t.get('entry_price', '?')}\n"
        f"  Exit:   ${t.get('exit_price') or 'open'}\n"
        f"  P&L:    ${t.get('pnl_dollars', '?')} ({t.get('pnl_pct', '?')}%)\n"
        f"  Hold:   {t.get('hold_hours', '?')}h\n"
        f"  Regime: {t.get('regime', '?')}\n"
        f"  VIX:    {t.get('vix', '?')}\n"
        f"  Strategy: {t.get('strategy', '?')}\n"
        f"  Conviction at entry: {t.get('conviction', '?')}\n"
        f"  Reasoning: {(t.get('reasoning') or '')[:400]}\n"
    )


def build_output(grade: str, lesson: str | None) -> str:
    return f"GRADE: {grade}\nLESSON: {lesson or '(no lesson recorded)'}"


def hash_input(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def fetch_fleet(c: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    trade_outcomes JOIN trades — fleet trades with realized P&L.

    Note: v4 extract_corpus.py used `t.thesis as reasoning`, but `trades` has
    no `thesis` column (it has `reasoning`). The v4 corpus was therefore
    missing reasoning text for every fleet row. We use `t.reasoning` here.
    """
    return c.execute(
        """
        SELECT
            t.symbol,
            t.action,
            t.price                       AS entry_price,
            o.exit_price                  AS exit_price,
            o.pnl_dollars                 AS pnl_dollars,
            o.pnl_percent                 AS pnl_pct,
            o.hold_duration_hours         AS hold_hours,
            o.regime_at_entry             AS regime,
            o.vix_at_entry                AS vix,
            o.fear_greed_at_entry         AS fear_greed,
            o.strategy_name               AS strategy,
            o.conviction_at_entry         AS conviction,
            o.outcome                     AS outcome,
            t.reasoning                   AS reasoning,
            o.created_at                  AS ts
        FROM trade_outcomes o
        JOIN trades t ON o.trade_id = t.id
        WHERE o.pnl_dollars IS NOT NULL
        """
    ).fetchall()


def fetch_lessons(c: sqlite3.Connection) -> list[sqlite3.Row]:
    """daily_lessons — explicit grade + lesson text (gold standard)."""
    return c.execute(
        """
        SELECT
            symbol,
            action,
            entry_price                   AS entry_price,
            current_price                 AS exit_price,
            pnl                           AS pnl_dollars,
            CASE
                WHEN entry_price > 0
                THEN (current_price - entry_price) / entry_price * 100.0
                ELSE NULL
            END                           AS pnl_pct,
            regime                        AS regime,
            grade                         AS grade,
            lesson                        AS lesson,
            created_at                    AS ts
        FROM daily_lessons
        WHERE grade IS NOT NULL
        """
    ).fetchall()


def split_8_1_1(items: list[dict[str, Any]]) -> tuple[list, list, list]:
    """Deterministic 80/10/10 split."""
    rng = random.Random(SEED)
    shuffled = items[:]
    rng.shuffle(shuffled)
    n = len(shuffled)
    n_train = int(n * 0.8)
    n_val = int(n * 0.9)
    return shuffled[:n_train], shuffled[n_train:n_val], shuffled[n_val:]


def write_jsonl(items: list[dict[str, Any]], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for x in items:
            f.write(json.dumps(x, ensure_ascii=False))
            f.write("\n")


def main() -> None:
    if not DB.exists():
        raise SystemExit(f"trader.db not found at {DB}")

    mismatches: list[str] = []

    with sqlite3.connect(DB) as c:
        c.row_factory = sqlite3.Row

        # Confirm expected columns are still present (live schema probe).
        trades_cols = {r["name"] for r in c.execute("PRAGMA table_info(trades)")}
        if "thesis" in trades_cols:
            mismatches.append(
                "trades.thesis exists — v4 extractor's `t.thesis as reasoning` "
                "would now work; this script still uses t.reasoning."
            )
        if "reasoning" not in trades_cols:
            mismatches.append(
                "trades.reasoning missing — extractor will emit empty reasoning."
            )

        fleet = fetch_fleet(c)
        lessons = fetch_lessons(c)

    print(f"  fleet outcomes:  {len(fleet)}")
    print(f"  daily_lessons:   {len(lessons)}")

    corpus: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Source 1: fleet (synthesize grade from P&L percent).
    fleet_kept = 0
    fleet_dropped_no_grade = 0
    for r in fleet:
        d = dict(r)
        g = grade_from_pnl(d.get("pnl_pct"))
        if g is None:
            fleet_dropped_no_grade += 1
            continue
        inp = build_input(d)
        h = hash_input(inp)
        if h in seen:
            continue
        seen.add(h)
        lesson_text = (
            f"{g} — Strategy '{d.get('strategy', '?')}' produced "
            f"{d.get('pnl_pct', '?')}% in {d.get('regime', '?')} regime."
        )
        corpus.append(
            {
                "instruction": INSTRUCTION,
                "input": inp,
                "output": build_output(g, lesson_text),
                "source": "fleet",
                "ts": d.get("ts"),
            }
        )
        fleet_kept += 1

    # Source 2: daily_lessons (explicit grade + lesson text — gold standard).
    lessons_kept = 0
    for r in lessons:
        d = dict(r)
        inp = build_input(d)
        h = hash_input(inp)
        if h in seen:
            continue
        seen.add(h)
        corpus.append(
            {
                "instruction": INSTRUCTION,
                "input": inp,
                "output": build_output(d["grade"], d["lesson"]),
                "source": "daily_lessons",
                "ts": d.get("ts"),
            }
        )
        lessons_kept += 1

    print(f"\n  corpus total: {len(corpus)} "
          f"(fleet={fleet_kept}, daily_lessons={lessons_kept})")

    train, val, test = split_8_1_1(corpus)

    full_path = OUT_DIR / f"{STEM}.jsonl"
    train_path = OUT_DIR / f"{STEM}.train.jsonl"
    val_path = OUT_DIR / f"{STEM}.val.jsonl"
    test_path = OUT_DIR / f"{STEM}.test.jsonl"
    stats_path = OUT_DIR / f"{STEM}.stats.json"

    write_jsonl(corpus, full_path)
    write_jsonl(train, train_path)
    write_jsonl(val, val_path)
    write_jsonl(test, test_path)

    grade_dist = dict(
        Counter(
            x["output"].split("\n", 1)[0].replace("GRADE: ", "")
            for x in corpus
        )
    )
    source_dist = dict(Counter(x["source"] for x in corpus))

    stats = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "db_path": str(DB),
        "seed": SEED,
        "totals": {
            "fleet_rows_fetched": len(fleet),
            "fleet_kept": fleet_kept,
            "fleet_dropped_no_grade": fleet_dropped_no_grade,
            "lessons_rows_fetched": len(lessons),
            "lessons_kept": lessons_kept,
            "corpus_total": len(corpus),
            "dedup_removed": (len(fleet) + len(lessons))
            - (fleet_kept + lessons_kept + fleet_dropped_no_grade),
        },
        "splits": {
            "train": len(train),
            "val": len(val),
            "test": len(test),
        },
        "grade_distribution": grade_dist,
        "source_distribution": source_dist,
        "schema_mismatches": mismatches,
    }
    stats_path.write_text(json.dumps(stats, indent=2))

    print(f"\n  wrote {full_path.name} ({len(corpus)})")
    print(f"        {train_path.name} ({len(train)})")
    print(f"        {val_path.name} ({len(val)})")
    print(f"        {test_path.name} ({len(test)})")
    print(f"        {stats_path.name}")
    if mismatches:
        print(f"\n  schema mismatches:")
        for m in mismatches:
            print(f"    - {m}")
    print(f"\n  grade distribution: {grade_dist}")


if __name__ == "__main__":
    main()
