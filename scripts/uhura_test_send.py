#!/usr/bin/env python3
"""HM-UHURA-HAILS acceptance harness (2026-06-08).

Sends one test ntfy to each standardized topic + one test HTML email, and
prints per-channel pass/fail. Run on-demand (NOT scheduled) after subscribing
on the phone to ollie-premarket / ollie-signals / ollie-critical.

  python3 scripts/uhura_test_send.py

Email only fires when ALERTS_EMAIL_ENABLED=True AND SMTP fully configured
(app password set). Never prints the app password.
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
sys.path.insert(0, _ROOT)

from engine import alert_channels as ac  # noqa: E402


def main() -> int:
    print("=== HM-UHURA-HAILS test send ===")
    print(f"ntfy enabled : {ac.ALERTS_NTFY_ENABLED}")
    print(f"email enabled: {ac.ALERTS_EMAIL_ENABLED}")
    print(f"email to     : {ac.ALERTS_EMAIL_TO or '(unset)'}")
    print(f"smtp config  : host={'set' if ac.SMTP_HOST else 'MISSING'} "
          f"user={'set' if ac.SMTP_USER else 'MISSING'} "
          f"pass={'set' if ac.SMTP_PASS else 'MISSING'}")  # never prints the value
    print()

    topics = [
        (ac.NTFY_PREMARKET_TOPIC, "🌅 Reveille test", "HM-UHURA-HAILS: ollie-premarket channel live.", "default"),
        (ac.NTFY_SIGNALS_TOPIC,   "📡 Signals test",  "HM-UHURA-HAILS: ollie-signals channel live.",   "default"),
        (ac.NTFY_CRITICAL_TOPIC,  "🚨 Critical test", "HM-UHURA-HAILS: ollie-critical channel live.",   "urgent"),
    ]
    results = {}
    for topic, title, body, prio in topics:
        ok = ac.push_ntfy(topic, title, body, priority=prio, tags="test,ollietrades")
        results[f"ntfy:{topic}"] = ok
        print(f"  ntfy -> {topic:<16} {'OK' if ok else 'FAIL'}")

    html = (
        "<h2>🖖 HM-UHURA-HAILS</h2>"
        "<p>Outbound HTML email channel is <b>live</b>.</p>"
        "<table border='1' cellpadding='6' style='border-collapse:collapse'>"
        "<tr><th>Channel</th><th>Status</th></tr>"
        "<tr><td>ntfy</td><td>tested</td></tr>"
        "<tr><td>SMTP (HTML)</td><td>this message</td></tr>"
        "</table>"
        "<p style='color:#888'>Test send — no action required.</p>"
    )
    email_ok = ac.send_email("HM-UHURA-HAILS test", html)
    results["email"] = email_ok
    print(f"  email -> {ac.ALERTS_EMAIL_TO or '(unset)':<16} "
          f"{'OK' if email_ok else 'SKIPPED/FAIL (email disabled or SMTP unconfigured)'}")

    print()
    n_ntfy = sum(1 for k, v in results.items() if k.startswith('ntfy:') and v)
    print(f"=== ntfy {n_ntfy}/3 sent | email {'sent' if email_ok else 'not sent'} ===")
    return 0 if n_ntfy == 3 else 1


if __name__ == "__main__":
    raise SystemExit(main())
