"""tests/test_cron_missing_scripts_sentinel.py — OPS TRIAGE item 2,
"no more unwatched watchers anywhere."

check_cron_missing_scripts() reads crontab -l directly and tails each
active entry's log for a file-not-found signature -- deliberately checking
only the true LAST non-empty line (not an arbitrary byte window), since a
dead-script cron entry's log keeps getting touched every tick (cron
appends the error each time) and a byte-window scan would false-positive
on a script that was JUST fixed (old error lines still inside the window,
mixed with the new success line).
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import hm_ops_sentinel as sentinel  # noqa: E402


def _fake_crontab_run(lines: str):
    proc = MagicMock()
    proc.stdout = lines
    return proc


def test_broken_entry_detected_by_last_line():
    with tempfile.TemporaryDirectory() as d:
        log_path = Path(d) / "dead.log"
        log_path.write_text(
            "some old output\n"
            "/bin/python3: can't open file '/x/dead.py': [Errno 2] No such file or directory\n"
        )
        cron = f"*/10 * * * * /usr/bin/python3 /x/dead.py >> {log_path} 2>&1\n"
        with patch("subprocess.run", return_value=_fake_crontab_run(cron)):
            alerts: list = []
            result = sentinel.check_cron_missing_scripts(alerts)
        assert result["scanned"] == 1
        assert log_path.name in result["broken"]
        assert len(alerts) == 1
        assert alerts[0][0] == "warning"
        assert alerts[0][1] == "sentinel_cron_missing_script"


def test_recently_fixed_entry_not_flagged_despite_old_errors_in_tail():
    """The exact false-positive this check must avoid: a script that JUST
    got restored, so its log's last line is a success, but earlier lines
    (still within any byte-window scan) are old failures from before the
    fix. Must not flag it."""
    with tempfile.TemporaryDirectory() as d:
        log_path = Path(d) / "fixed.log"
        log_path.write_text(
            "/bin/python3: can't open file '/x/fixed.py': [Errno 2] No such file or directory\n"
            "/bin/python3: can't open file '/x/fixed.py': [Errno 2] No such file or directory\n"
            "[fixed] ok, ran clean\n"
        )
        cron = f"*/10 * * * * /usr/bin/python3 /x/fixed.py >> {log_path} 2>&1\n"
        with patch("subprocess.run", return_value=_fake_crontab_run(cron)):
            alerts: list = []
            result = sentinel.check_cron_missing_scripts(alerts)
        assert result["broken"] == []
        assert alerts == []


def test_healthy_entry_not_flagged():
    with tempfile.TemporaryDirectory() as d:
        log_path = Path(d) / "healthy.log"
        log_path.write_text("[healthy] ran fine\n")
        cron = f"0 6 * * 1-5 /usr/bin/python3 /x/healthy.py >> {log_path} 2>&1\n"
        with patch("subprocess.run", return_value=_fake_crontab_run(cron)):
            alerts: list = []
            result = sentinel.check_cron_missing_scripts(alerts)
        assert result["broken"] == []
        assert alerts == []


def test_reboot_and_commented_entries_skipped():
    with tempfile.TemporaryDirectory() as d:
        log_path = Path(d) / "dead.log"
        log_path.write_text("No such file or directory\n")
        cron = (
            f"# */10 * * * * /usr/bin/python3 /x/dead.py >> {log_path} 2>&1\n"
            f"@reboot /bin/zsh /x/reboot_script.sh >> {log_path} 2>&1\n"
        )
        with patch("subprocess.run", return_value=_fake_crontab_run(cron)):
            alerts: list = []
            result = sentinel.check_cron_missing_scripts(alerts)
        assert result["scanned"] == 0
        assert alerts == []


def test_entries_without_log_redirect_skipped_not_crashed():
    cron = "30 20 * * * /bin/bash /x/no_redirect.sh\n"
    with patch("subprocess.run", return_value=_fake_crontab_run(cron)):
        alerts: list = []
        result = sentinel.check_cron_missing_scripts(alerts)
    assert result["scanned"] == 0
    assert alerts == []


def test_missing_log_file_skipped_not_flagged():
    """A log that's never been written yet (brand new cron entry) isn't
    this check's concern -- FileNotFoundError on the log itself, not on
    the script it's watching."""
    cron = "*/10 * * * * /usr/bin/python3 /x/new.py >> /nonexistent/path/new.log 2>&1\n"
    with patch("subprocess.run", return_value=_fake_crontab_run(cron)):
        alerts: list = []
        result = sentinel.check_cron_missing_scripts(alerts)
    assert result["broken"] == []
    assert alerts == []
