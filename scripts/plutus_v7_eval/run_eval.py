"""HM-PLUTUS-V7-EVAL — one-command orchestrator: freeze -> generate -> score -> scorecard.

    python3 scripts/plutus_v7_eval/run_eval.py

To re-run for a future v7: add plutus-v7 to PLUTUS_MODELS in common.py (as a third label) and
extend generate.py/score.py to score it the same way, then re-run this. To refresh the OOS set
with post-cutoff real trades later: delete data/plutus_eval/oos_set_v1.jsonl and re-run after
adding a post-cutoff source to freeze_oos.py (the rest is idempotent).
"""
import freeze_oos, generate, score

if __name__ == "__main__":
    freeze_oos.build()
    generate.main()
    score.run()
