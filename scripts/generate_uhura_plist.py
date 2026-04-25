#!/usr/bin/env python3
"""Generate the launchd plist for uhura-watch."""
from pathlib import Path

REPO = "/Users/bigmac/autonomous-trader"
out = Path.home() / "Library/LaunchAgents/com.ollietrades.uhura-watch.plist"

intervals = []
for weekday in range(1, 6):
    for minute in (30, 45):
        intervals.append(f'        <dict><key>Weekday</key><integer>{weekday}</integer><key>Hour</key><integer>6</integer><key>Minute</key><integer>{minute}</integer></dict>')
    for hour in range(7, 13):
        for minute in (0, 15, 30, 45):
            intervals.append(f'        <dict><key>Weekday</key><integer>{weekday}</integer><key>Hour</key><integer>{hour}</integer><key>Minute</key><integer>{minute}</integer></dict>')
    intervals.append(f'        <dict><key>Weekday</key><integer>{weekday}</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>0</integer></dict>')

plist = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ollietrades.uhura-watch</string>
    <key>WorkingDirectory</key>
    <string>{REPO}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>{REPO}/scripts/uhura_watch.py</string>
    </array>
    <key>StartCalendarInterval</key>
    <array>
{chr(10).join(intervals)}
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>BIGMAC_REPO</key><string>{REPO}</string>
        <key>UHURA_NTFY_TOPIC</key><string>ollietrades-watch</string>
        <key>PYTHONUNBUFFERED</key><string>1</string>
    </dict>
    <key>StandardOutPath</key>
    <string>{REPO}/logs/uhura_watch/launchd.out.log</string>
    <key>StandardErrorPath</key>
    <string>{REPO}/logs/uhura_watch/launchd.err.log</string>
    <key>RunAtLoad</key><false/>
    <key>KeepAlive</key><false/>
</dict>
</plist>'''

out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(plist)
print(f"Wrote {out} with {len(intervals)} scheduled runs")
