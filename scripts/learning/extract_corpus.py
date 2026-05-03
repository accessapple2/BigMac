#!/usr/bin/env python3
"""
Build fine-tuning corpus for Plutus-Critic (Format A) and Plutus-Decider (Format B).

Sources (joined):
  - trades + trade_outcomes  (graded outcomes from YOUR fleet)
  - daily_lessons            (text lessons with grades A-F)
  - reference_trades         (ai4trade.ai + rallies — external graded trades)

Outputs:
  - data/training/corpus_A.jsonl  (Trade Critic — INPUT trade → OUTPUT grade+lesson)
  - data/training/corpus_B.jsonl  (Trade Decider — INPUT context → OUTPUT decision)
  - data/training/corpus_stats.json (distribution, splits, dedup counts)
  - 80/10/10 train/val/test splits (deterministic seed=42)
"""
import json, sqlite3, hashlib, random
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "trader.db"
OUT = ROOT / "data" / "training"
OUT.mkdir(parents=True, exist_ok=True)

random.seed(42)

def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def grade_from_pnl(pnl_dollars, pnl_pct, conviction=0.5):
    """Synthesize letter grade from P&L when explicit grade absent."""
    if pnl_pct is None: return None
    p = pnl_pct
    if p >= 5: return "A"
    if p >= 1: return "B"
    if p >= -1: return "C"
    if p >= -3: return "D"
    return "F"

def build_critic_input(t):
    """Format A INPUT — trade context for the critic."""
    return (
        f"Trade to grade:\n"
        f"  Symbol: {t.get('symbol','?')}\n"
        f"  Action: {t.get('action','?')}\n"
        f"  Entry:  ${t.get('entry_price', '?')}\n"
        f"  Exit:   ${t.get('exit_price', '?') or 'open'}\n"
        f"  P&L:    ${t.get('pnl_dollars','?')} ({t.get('pnl_pct','?')}%)\n"
        f"  Hold:   {t.get('hold_hours','?')}h\n"
        f"  Regime: {t.get('regime','?')}\n"
        f"  VIX:    {t.get('vix','?')}\n"
        f"  Strategy: {t.get('strategy','?')}\n"
        f"  Conviction at entry: {t.get('conviction','?')}\n"
        f"  Reasoning: {(t.get('reasoning','') or '')[:400]}\n"
    )

def build_critic_output(grade, lesson):
    """Format A OUTPUT — grade + reasoning."""
    return f"GRADE: {grade}\nLESSON: {lesson or '(no lesson recorded)'}"

def build_decider_input(t):
    """Format B INPUT — pre-trade context."""
    return (
        f"Market context:\n"
        f"  Symbol: {t.get('symbol','?')}\n"
        f"  Regime: {t.get('regime','?')}\n"
        f"  VIX:    {t.get('vix','?')}\n"
        f"  Fear/Greed: {t.get('fear_greed','?')}\n"
        f"  Available signals: {(t.get('reasoning','') or '')[:400]}\n"
        f"\nWhat is your trade decision?"
    )

def build_decider_output(t):
    """Format B OUTPUT — decision + conviction + reasoning."""
    return (
        f"ACTION: {t.get('action','?')}\n"
        f"CONVICTION: {t.get('conviction','?')}\n"
        f"OUTCOME: {t.get('outcome','?')}\n"
        f"REASONING: {(t.get('reasoning','') or '')[:300]}"
    )

def hash_example(text):
    return hashlib.sha256(text.encode()).hexdigest()[:16]

# ─────── EXTRACT ───────
c = conn()
print("→ Querying sources...")

# Source 1: trades + trade_outcomes (graded fleet trades)
fleet = c.execute("""
    SELECT
        t.symbol, t.action, t.price as entry_price,
        o.exit_price, o.pnl_dollars, o.pnl_percent as pnl_pct,
        o.hold_duration_hours as hold_hours,
        o.regime_at_entry as regime, o.vix_at_entry as vix,
        o.fear_greed_at_entry as fear_greed,
        o.strategy_name as strategy, o.conviction_at_entry as conviction,
        o.outcome, t.thesis as reasoning,
        o.created_at as ts
    FROM trade_outcomes o
    JOIN trades t ON o.trade_id = t.id
    WHERE o.pnl_dollars IS NOT NULL
""").fetchall()
print(f"  fleet outcomes: {len(fleet)}")

# Source 2: daily_lessons (explicit grades + lesson text)
lessons = c.execute("""
    SELECT
        symbol, action, entry_price, current_price as exit_price,
        pnl as pnl_dollars,
        CASE WHEN entry_price > 0 THEN (current_price - entry_price)/entry_price * 100 ELSE NULL END as pnl_pct,
        regime, grade, lesson, created_at as ts
    FROM daily_lessons
    WHERE grade IS NOT NULL
""").fetchall()
print(f"  daily_lessons: {len(lessons)}")

# Source 3: reference_trades (ai4trade + rallies)
refs = c.execute("""
    SELECT
        symbol, action, price as entry_price,
        pnl as pnl_dollars, pnl_pct, regime,
        confidence as conviction, outcome, reasoning,
        source, model_name, traded_at as ts
    FROM reference_trades
    WHERE pnl IS NOT NULL OR outcome IS NOT NULL
""").fetchall()
print(f"  reference_trades: {len(refs)}")

# ─────── BUILD CORPUS A (CRITIC) ───────
print("\n→ Building corpus_A (Critic)...")
corpus_A = []
seen_A = set()

# Fleet (synthesize grade from pnl)
for r in fleet:
    d = dict(r)
    g = grade_from_pnl(d['pnl_dollars'], d['pnl_pct'], d.get('conviction', 0.5))
    if not g: continue
    inp = build_critic_input(d)
    h = hash_example(inp)
    if h in seen_A: continue
    seen_A.add(h)
    corpus_A.append({
        "instruction": "You are Plutus, an expert trade critic. Grade this trade and explain why.",
        "input": inp,
        "output": build_critic_output(g, f"{g} — Strategy '{d.get('strategy','?')}' produced {d.get('pnl_pct','?')}% in {d.get('regime','?')} regime."),
        "source": "fleet",
        "ts": d.get('ts'),
    })

# Daily lessons (explicit grade + lesson text — gold standard)
for r in lessons:
    d = dict(r)
    inp = build_critic_input(d)
    h = hash_example(inp)
    if h in seen_A: continue
    seen_A.add(h)
    corpus_A.append({
        "instruction": "You are Plutus, an expert trade critic. Grade this trade and explain why.",
        "input": inp,
        "output": build_critic_output(d['grade'], d['lesson']),
        "source": "daily_lessons",
        "ts": d.get('ts'),
    })

# Reference (synthesize grade)
for r in refs:
    d = dict(r)
    g = grade_from_pnl(d['pnl_dollars'], d['pnl_pct'])
    if not g: continue
    inp = build_critic_input(d)
    h = hash_example(inp)
    if h in seen_A: continue
    seen_A.add(h)
    corpus_A.append({
        "instruction": "You are Plutus, an expert trade critic. Grade this trade and explain why.",
        "input": inp,
        "output": build_critic_output(g, f"{g} — External {d.get('source','?')} trade by {d.get('model_name','?')}, {d.get('pnl_pct','?')}% return."),
        "source": f"ref_{d.get('source','unknown')}",
        "ts": d.get('ts'),
    })

print(f"  corpus_A total: {len(corpus_A)}")

# ─────── BUILD CORPUS B (DECIDER) ───────
print("\n→ Building corpus_B (Decider) — sits on disk, not trained yet...")
corpus_B = []
seen_B = set()

for src_name, rows in [("fleet", fleet), ("reference", refs)]:
    for r in rows:
        d = dict(r)
        if not d.get('action') or not d.get('reasoning'):
            continue
        inp = build_decider_input(d)
        h = hash_example(inp)
        if h in seen_B: continue
        seen_B.add(h)
        corpus_B.append({
            "instruction": "You are Plutus, an expert trader. Make a decision based on this market context.",
            "input": inp,
            "output": build_decider_output(d),
            "source": src_name,
            "ts": d.get('ts'),
        })

print(f"  corpus_B total: {len(corpus_B)}")

# ─────── SPLIT 80/10/10 ───────
def split(corpus):
    random.shuffle(corpus)
    n = len(corpus)
    train = corpus[:int(n*0.8)]
    val = corpus[int(n*0.8):int(n*0.9)]
    test = corpus[int(n*0.9):]
    return train, val, test

A_train, A_val, A_test = split(corpus_A)
B_train, B_val, B_test = split(corpus_B)

# ─────── WRITE FILES ───────
print("\n→ Writing JSONL files...")
def write_jsonl(items, path):
    with open(path, "w") as f:
        for x in items:
            f.write(json.dumps(x) + "\n")
    print(f"  {path.name}: {len(items)} examples")

write_jsonl(A_train, OUT / "corpus_A_train.jsonl")
write_jsonl(A_val, OUT / "corpus_A_val.jsonl")
write_jsonl(A_test, OUT / "corpus_A_test.jsonl")
write_jsonl(B_train, OUT / "corpus_B_train.jsonl")
write_jsonl(B_val, OUT / "corpus_B_val.jsonl")
write_jsonl(B_test, OUT / "corpus_B_test.jsonl")

# ─────── STATS ───────
def grade_dist(corpus):
    return dict(Counter(x['output'].split('\n')[0].replace('GRADE: ','') for x in corpus if x['output'].startswith('GRADE')))

def source_dist(corpus):
    return dict(Counter(x['source'] for x in corpus))

stats = {
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
    "corpus_A": {
        "total": len(corpus_A),
        "train": len(A_train), "val": len(A_val), "test": len(A_test),
        "grade_distribution": grade_dist(corpus_A),
        "source_distribution": source_dist(corpus_A),
    },
    "corpus_B": {
        "total": len(corpus_B),
        "train": len(B_train), "val": len(B_val), "test": len(B_test),
        "source_distribution": source_dist(corpus_B),
        "status": "RESERVED — not trained until Plutus-Critic validated",
    },
    "dedup_removed_A": (len(fleet) + len(lessons) + len(refs)) - len(corpus_A),
}
(OUT / "corpus_stats.json").write_text(json.dumps(stats, indent=2))
print(f"\n  stats → {OUT / 'corpus_stats.json'}")

# ─────── UPDATE PIPELINE STATE ───────
state_path = ROOT / "data" / "training_state.json"
state = json.loads(state_path.read_text())
state["phase"] = "2_training_pending"
state["phases"]["1_extraction"]["status"] = "complete"
state["phases"]["1_extraction"]["completed_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
state["phases"]["1_extraction"]["corpus_A_examples"] = len(corpus_A)
state["phases"]["1_extraction"]["corpus_B_examples"] = len(corpus_B)
state["phases"]["2_train_critic_A"]["status"] = "ready"
state["next_action"] = f"Train Plutus-Critic on corpus_A ({len(A_train)} train + {len(A_val)} val examples). Block 3 next."
state_path.write_text(json.dumps(state, indent=2))
print(f"\n  ✅ pipeline state advanced → 2_training_pending")

# ─────── SUMMARY ───────
print("\n═══ EXTRACTION COMPLETE ═══")
print(f"  corpus_A (Critic):   {len(corpus_A)} examples ({len(A_train)} train / {len(A_val)} val / {len(A_test)} test)")
print(f"  corpus_B (Decider):  {len(corpus_B)} examples (RESERVED)")
print(f"  grade distribution:  {grade_dist(corpus_A)}")
print(f"  output dir:          {OUT}")
