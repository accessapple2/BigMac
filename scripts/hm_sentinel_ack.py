#!/usr/bin/env python3
"""HM-SENTINEL-ACK (2026-07-12): manage hm_ops_sentinel.py alert acknowledgements.

Writes data/.hm_ops_sentinel_acks.json -- hm_ops_sentinel.py only ever reads
this file, so acking never touches trader.db or any production data, only
this script's own metadata file (per XO directive: "keep it read-only
against production data -- this only touches notification logic").

An ack suppresses hm_ops_sentinel.py notifications for one alert_type until
either unacked or (if --ceiling was given) the check's own metric value
exceeds that ceiling, at which point it fires again automatically -- so
acking a known condition can't create a permanent blind spot if it later
gets meaningfully worse.

Usage:
    scripts/hm_sentinel_ack.py list
    scripts/hm_sentinel_ack.py ack <alert_type> [--ceiling N] [--note TEXT] [--by NAME]
    scripts/hm_sentinel_ack.py unack <alert_type>

Known alert_type values (from scripts/hm_ops_sentinel.py) and their ceiling units:
    sentinel_fd_warn            FD count
    sentinel_fd_red             FD count
    sentinel_riker_heartbeat    heartbeat age, minutes
    sentinel_lock_errors        lock-error count in the check window
    sentinel_signals_v2_queue   oldest-pending age in ELAPSED MARKET HOURS
                                 (engine.market_calendar.market_hours_elapsed),
                                 NOT wall-clock hours -- nights/weekends/
                                 holidays don't count. One trading day is
                                 ~6.5 market-hours.
    sentinel_main_py_down       no scalar metric -- --ceiling is ignored,
                                 ack is always a permanent suppression

Example (the 5:20 PM signals_v2 queue-depth alert, escalate if it's still
undrained after ~2 trading sessions' worth of market time post-ack):
    scripts/hm_sentinel_ack.py ack sentinel_signals_v2_queue --ceiling 13 \\
        --note "known weekend residual, HM-SIGNALS-V2-FIFO-STARVATION in XO_BACKLOG" \\
        --by Admiral
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ACKS_PATH = ROOT / "data" / ".hm_ops_sentinel_acks.json"


def _load() -> dict:
    try:
        return json.loads(ACKS_PATH.read_text())
    except Exception:
        return {}


def _save(acks: dict) -> None:
    ACKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    ACKS_PATH.write_text(json.dumps(acks, indent=2, sort_keys=True) + "\n")


def cmd_list(_args: argparse.Namespace) -> int:
    acks = _load()
    if not acks:
        print("No acks recorded.")
        return 0
    for alert_type, ack in sorted(acks.items()):
        print(f"{alert_type}")
        print(f"    acked_at:  {ack.get('acked_at')}")
        print(f"    acked_by:  {ack.get('acked_by')}")
        print(f"    ceiling:   {ack.get('ceiling')}")
        print(f"    note:      {ack.get('note')}")
    return 0


def cmd_ack(args: argparse.Namespace) -> int:
    acks = _load()
    acks[args.alert_type] = {
        "acked_at": datetime.now(timezone.utc).isoformat(),
        "acked_by": args.by,
        "ceiling": args.ceiling,
        "note": args.note,
    }
    _save(acks)
    print(f"Acked {args.alert_type!r} (ceiling={args.ceiling}). "
          f"Written to {ACKS_PATH}")
    return 0


def cmd_unack(args: argparse.Namespace) -> int:
    acks = _load()
    if args.alert_type not in acks:
        print(f"{args.alert_type!r} has no ack recorded -- nothing to do.")
        return 0
    del acks[args.alert_type]
    _save(acks)
    print(f"Unacked {args.alert_type!r}. Written to {ACKS_PATH}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="show all current acks")

    p_ack = sub.add_parser("ack", help="acknowledge (suppress) an alert type")
    p_ack.add_argument("alert_type")
    p_ack.add_argument("--ceiling", type=float, default=None,
                        help="re-fire if the check's metric exceeds this value (omit for permanent suppression)")
    p_ack.add_argument("--note", default="", help="why this is acked")
    p_ack.add_argument("--by", default="", help="who acked it")

    p_unack = sub.add_parser("unack", help="remove an ack, alert fires normally again")
    p_unack.add_argument("alert_type")

    args = parser.parse_args()
    if args.command == "list":
        return cmd_list(args)
    if args.command == "ack":
        return cmd_ack(args)
    if args.command == "unack":
        return cmd_unack(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
