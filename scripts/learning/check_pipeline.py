#!/usr/bin/env python3
"""
Fine-tune pipeline reminder.
Reads data/training_state.json, decides if a reminder is due, sends ntfy alert.
Runs daily at 9 AM via launchd.
"""
import json, os, sys, requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "data" / "training_state.json"
NTFY = "https://ntfy.sh/ollietrades-admin"

def load():
    return json.loads(STATE.read_text())

def save(s):
    STATE.write_text(json.dumps(s, indent=2))

def days_since(iso):
    if not iso: return None
    dt = datetime.fromisoformat(iso.replace("Z","+00:00"))
    return (datetime.now(timezone.utc) - dt).days

def ping(title, msg, priority="default", tags=""):
    try:
        requests.post(NTFY, data=msg.encode(),
                      headers={"Title": title, "Priority": priority, "Tags": tags},
                      timeout=10)
        return True
    except Exception as e:
        print(f"ntfy failed: {e}")
        return False

def main():
    s = load()
    p = s["phases"]
    msgs = []

    # Phase 1 → 2 nag
    if p["1_extraction"]["status"] == "complete" and p["2_train_critic_A"]["status"] == "blocked":
        d = days_since(p["1_extraction"]["completed_at"])
        if d is not None and d >= 1:
            msgs.append(("📊 Plutus-Critic READY to train",
                        f"corpus_A.jsonl built {d}d ago. Run training next session.",
                        "high", "memo,brain"))
            p["2_train_critic_A"]["status"] = "ready"

    # Phase 2 just completed → start the validation clock
    if p["2_train_critic_A"]["status"] == "complete" and p["3_validate_critic_A"]["status"] == "blocked":
        p["3_validate_critic_A"]["status"] = "in_progress"
        p["3_validate_critic_A"]["started_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        msgs.append(("🎯 Plutus-Critic LIVE — validation clock started",
                    "14 days of live grading. Reminder on day 7 + day 14.",
                    "default", "rocket,clock"))

    # Phase 3 mid-validation reminders
    if p["3_validate_critic_A"]["status"] == "in_progress":
        d = days_since(p["3_validate_critic_A"]["started_at"])
        req = p["3_validate_critic_A"]["days_required"]
        if d == 7:
            msgs.append(("⏳ Plutus-Critic — 7 days in",
                        f"Halfway through validation. {req-d}d to go. Check Model DNA dashboard.",
                        "default", "hourglass"))
        elif d >= req:
            msgs.append(("✅ Plutus-Critic VALIDATED — ready for Phase 3",
                        f"{d}d of grading complete. FLIP B ON: train Plutus-Decider with corpus_B + critic-graded data.",
                        "high", "checkered_flag,gear"))
            p["3_validate_critic_A"]["status"] = "complete"
            p["4_train_decider_B"]["status"] = "ready"
            s["next_action"] = "Train Plutus-Decider-v1 (Phase B). corpus_B.jsonl already built; regrade with Plutus-Critic first."

    # Phase 4 nag escalator
    if p["4_train_decider_B"]["status"] == "ready":
        d = days_since(p["3_validate_critic_A"].get("started_at"))
        ready_d = (d - p["3_validate_critic_A"]["days_required"]) if d else 0
        if ready_d in (0, 3, 7):
            pri = "high" if ready_d >= 3 else "default"
            msgs.append((f"🚩 Plutus-Decider waiting {ready_d}d",
                        "B is ready. Don't let it rot.",
                        pri, "warning,gear"))

    save(s)

    if msgs:
        for title, msg, pri, tags in msgs:
            ping(title, msg, pri, tags)
            print(f"sent: {title}")
    else:
        print(f"no reminders due. current phase: {s['phase']}")

if __name__ == "__main__":
    main()
