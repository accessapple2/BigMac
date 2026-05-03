#!/usr/bin/env python3
"""Print fine-tune pipeline status. Run anytime: python3 scripts/learning/status.py"""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
s = json.loads((ROOT / "data" / "training_state.json").read_text())

print(f"\n═══ FINE-TUNE PIPELINE STATUS ═══")
print(f"Phase: {s['phase']}")
print(f"Next:  {s['next_action']}\n")
icons = {"complete":"✅","ready":"🟢","in_progress":"🔄","pending":"⏸ ","blocked":"🔒"}
for name, p in s["phases"].items():
    status = p["status"]
    extra = ""
    if status == "complete" and p.get("completed_at"):
        extra = f"  ({p['completed_at'][:10]})"
    elif status == "in_progress" and p.get("started_at"):
        from datetime import datetime, timezone
        d = (datetime.now(timezone.utc) -
             datetime.fromisoformat(p['started_at'].replace('Z','+00:00'))).days
        extra = f"  (day {d}/{p.get('days_required','?')})"
    print(f"  {icons.get(status,'?')} {name}: {status}{extra}")
print()
