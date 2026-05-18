"""HM-BULL-SPREAD-V1-SCHEMA-CANONICALIZE Phase 2 synthetic test.

Reads id=28 legacy legs_json from DB, applies translation in-memory,
exercises the 3 reader paths (exit_manager, reconciliation, kill_bull_spread)
to verify translation produces consumable shapes BEFORE any DB write.

No DB writes. No code changes. Pure in-memory verification.
"""
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DB = "data/trader.db"
TEST_ID = 28


def translate_legacy_to_canonical(legacy_legs: list, contracts: int) -> list:
    """Apply the schema migration per audit translation table."""
    canonical = []
    for leg in legacy_legs:
        side = "long" if leg.get("action") == "buy" else "short"
        canonical.append({
            "side":        side,
            "type":        leg.get("option_type"),
            "strike":      leg.get("strike"),
            "qty":         int(contracts),
            "entry_price": leg.get("premium"),
        })
    return canonical


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT id, status, structure, symbol, expiration, contracts, legs_json, entry_credit_debit "
        "FROM options_trades WHERE id=?",
        (TEST_ID,),
    ).fetchone()
    conn.close()
    if row is None:
        print(f"FAIL: id={TEST_ID} not found")
        return 1

    legacy_legs = json.loads(row["legs_json"])
    canonical_legs = translate_legacy_to_canonical(legacy_legs, row["contracts"])
    print(f"PRE  legs: {legacy_legs}")
    print(f"POST legs: {canonical_legs}")

    invariants: dict[str, bool] = {}

    # I1 — translation produces correct leg count
    invariants["I1 leg count preserved"] = (len(legacy_legs) == len(canonical_legs))

    # I2 — buy → long, sell → short
    sides_correct = all(
        (lg["side"] == "long" if leg["action"] == "buy" else lg["side"] == "short")
        for leg, lg in zip(legacy_legs, canonical_legs)
    )
    invariants["I2 buy→long sell→short mapping"] = sides_correct

    # I3 — type/strike/entry_price preservation
    type_strike_preserved = all(
        leg["option_type"] == lg["type"] and
        leg["strike"] == lg["strike"] and
        leg["premium"] == lg["entry_price"]
        for leg, lg in zip(legacy_legs, canonical_legs)
    )
    invariants["I3 type/strike/entry_price preserved"] = type_strike_preserved

    # I4 — qty injected from parent.contracts
    qty_correct = all(lg["qty"] == row["contracts"] for lg in canonical_legs)
    invariants["I4 qty injected from parent.contracts"] = qty_correct

    # I5 — expiration dropped from per-leg (parent column carries it)
    expiration_dropped = all("expiration" not in lg for lg in canonical_legs)
    invariants["I5 expiration dropped from per-leg"] = expiration_dropped

    # I6 — exit_manager.estimate_position_value-shape verification
    # The reader does: next((l for l in legs if l["action"] == "buy"), None)
    # After patch it will do: next((l for l in legs if l["side"] == "long"), None)
    # Verify the post-patch lookup finds both legs cleanly
    long_leg = next((l for l in canonical_legs if l.get("side") == "long"), None)
    short_leg = next((l for l in canonical_legs if l.get("side") == "short"), None)
    invariants["I6 exit_manager finds both long+short via side key"] = (long_leg is not None and short_leg is not None)

    # I7 — width computation works (exit_manager.py:157)
    if long_leg and short_leg:
        try:
            width = abs(short_leg["strike"] - long_leg["strike"])
            invariants["I7 width computation works"] = (width > 0)
        except Exception as e:
            invariants["I7 width computation works"] = False
            print(f"  width error: {e}")
    else:
        invariants["I7 width computation works"] = False

    # I8 — reconciliation._build_occ_symbol-style lookup (canonical keys)
    # Post-patch: option_type=leg.get("type") instead of leg.get("option_type")
    # Verify type key is present + populated
    types_present = all(lg.get("type") in ("put", "call") for lg in canonical_legs)
    invariants["I8 reconciliation can derive type"] = types_present

    # I9 — kill_bull_spread.occ_symbol-style (uses leg["type"] + leg["strike"] + parent expiration)
    occ_symbols = []
    for lg in canonical_legs:
        try:
            from datetime import date
            exp = date.fromisoformat(row["expiration"][:10])  # parent column, NOT per-leg
            yy = exp.strftime("%y")
            mm = exp.strftime("%m")
            dd = exp.strftime("%d")
            cp = "C" if lg["type"] == "call" else "P"
            strike_int = int(round(float(lg["strike"]) * 1000))
            occ = f"{row['symbol']}{yy}{mm}{dd}{cp}{strike_int:08d}"
            occ_symbols.append(occ)
        except Exception as e:
            print(f"  occ_symbol error: {e}")
            invariants["I9 kill_bull_spread occ_symbol works"] = False
            break
    else:
        invariants["I9 kill_bull_spread occ_symbol works"] = (len(occ_symbols) == len(canonical_legs))
        print(f"  occ_symbols: {occ_symbols}")

    # I10 — JSON round-trip preserves shape
    json_roundtrip = json.dumps(canonical_legs)
    parsed = json.loads(json_roundtrip)
    invariants["I10 json round-trip preserves shape"] = (parsed == canonical_legs)

    print()
    print("INVARIANT CHECK:")
    all_pass = True
    for name, passed in invariants.items():
        flag = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  {flag}: {name}")

    print()
    if all_pass:
        print(f"RESULT: PASS — {len(invariants)}/{len(invariants)} invariants. Phase 3 unblocked.")
        return 0
    print(f"RESULT: FAIL — STOP. Fall back to Path X.b (defer until id=28 closes Fri).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
