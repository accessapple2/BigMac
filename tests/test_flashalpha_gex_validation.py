"""tests/test_flashalpha_gex_validation.py -- HM-FLASHALPHA-GEX-VALIDATION-2026-09-12.

Never makes a real network call -- FlashAlpha's free tier is 5 requests/day
TOTAL, shared with the Admiral's own manual use, so a test suite that
actually called the live API would be exactly the kind of accidental spend
this script exists to prevent. Every test mocks _their_gex()/_our_gex()
directly, never requests.get.

Priorities, in order of what would actually hurt if broken:
1. The budget gate -- a second invocation the same day must not spend.
2. Structural-only checks -- must not fire on ordinary value divergence
   (the exact failure mode this script exists to avoid repeating).
3. Fail-closed on our own compute failing (no comparison possible -> don't
   spend FlashAlpha's budget on a comparison with nothing to compare).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.flashalpha_gex_validation as fav  # noqa: E402


@pytest.fixture
def validation_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_trader.db"
    monkeypatch.setattr(fav, "DB", str(db_path))
    return db_path


_OURS_NORMAL = {"spot": 218.0, "net_gex": 5.0e8, "call_wall": 225.0, "put_wall": 210.0, "gamma_flip": 217.0}
_THEIRS_NORMAL_DIVERGENT_VALUE = {
    # Same structure (call above, put below, same sign, flip in band) but
    # very different MAGNITUDES -- this is the "expected, not a bug" case.
    "spot": 219.5, "net_gex": 1.2e9, "call_wall": 222.0, "put_wall": 212.0, "gamma_flip": 220.0,
}


# ── 1. Budget gate ───────────────────────────────────────────────────────

def test_first_run_today_is_not_already_ran(validation_db):
    fav._init_db()
    assert fav._already_ran_today() is False


def test_second_run_same_day_is_skipped_and_spends_nothing(validation_db):
    fav._init_db()
    with patch.object(fav, "_pick_expiration", return_value="2026-10-16"), \
         patch.object(fav, "_our_gex", return_value=_OURS_NORMAL), \
         patch.object(fav, "_their_gex", return_value=_THEIRS_NORMAL_DIVERGENT_VALUE) as m_theirs:
        assert fav.main() == 0
        assert m_theirs.call_count == 1

    # Second invocation, same day -- must not call _their_gex again.
    with patch.object(fav, "_pick_expiration", return_value="2026-10-16"), \
         patch.object(fav, "_our_gex", return_value=_OURS_NORMAL), \
         patch.object(fav, "_their_gex", return_value=_THEIRS_NORMAL_DIVERGENT_VALUE) as m_theirs2:
        assert fav.main() == 0
        assert m_theirs2.call_count == 0  # the whole point of this test


def test_a_failed_flashalpha_call_does_not_count_as_a_spent_day(validation_db):
    """A call that errors/skips before FlashAlpha actually answers must not
    block a real attempt later the same day -- only a LOGGED row (meaning a
    real response came back) should count against the daily budget."""
    fav._init_db()
    with patch.object(fav, "_pick_expiration", return_value="2026-10-16"), \
         patch.object(fav, "_our_gex", return_value=_OURS_NORMAL), \
         patch.object(fav, "_their_gex", return_value=None):
        fav.main()
    assert fav._already_ran_today() is False


def test_our_own_compute_failing_never_calls_flashalpha(validation_db):
    """No comparison possible -> don't spend FlashAlpha's shared budget on
    a call with nothing to compare it against."""
    fav._init_db()
    with patch.object(fav, "_pick_expiration", return_value="2026-10-16"), \
         patch.object(fav, "_our_gex", return_value=None), \
         patch.object(fav, "_their_gex") as m_theirs:
        fav.main()
        m_theirs.assert_not_called()


def test_no_real_expiration_found_never_calls_flashalpha(validation_db):
    fav._init_db()
    with patch.object(fav, "_pick_expiration", return_value=None), \
         patch.object(fav, "_their_gex") as m_theirs:
        fav.main()
        m_theirs.assert_not_called()


# ── 2. Structural checks -- the actual point of this validation ─────────

def test_structural_match_despite_large_value_divergence():
    """The exact case the Admiral called out: different magnitudes are
    EXPECTED and must not fire a false alarm."""
    match, reason = fav._structural_check(_OURS_NORMAL, _THEIRS_NORMAL_DIVERGENT_VALUE)
    assert match is True
    assert reason is None


def test_wall_on_wrong_side_is_a_contradiction():
    theirs = dict(_THEIRS_NORMAL_DIVERGENT_VALUE, call_wall=215.0)  # below their own spot (219.5)
    match, reason = fav._structural_check(_OURS_NORMAL, theirs)
    assert match is False
    assert reason == "their_wall_wrong_side"


def test_opposite_net_gex_sign_is_a_contradiction():
    theirs = dict(_THEIRS_NORMAL_DIVERGENT_VALUE, net_gex=-1.2e9)
    match, reason = fav._structural_check(_OURS_NORMAL, theirs)
    assert match is False
    assert reason == "opposite_net_gex_sign"


def test_flip_outside_band_is_a_contradiction():
    theirs = dict(_THEIRS_NORMAL_DIVERGENT_VALUE, gamma_flip=300.0)  # >15% above their spot
    match, reason = fav._structural_check(_OURS_NORMAL, theirs)
    assert match is False
    assert reason == "their_flip_out_of_band"


def test_missing_wall_is_a_contradiction_not_a_silent_skip():
    theirs = dict(_THEIRS_NORMAL_DIVERGENT_VALUE, call_wall=None)
    match, reason = fav._structural_check(_OURS_NORMAL, theirs)
    assert match is False
    assert reason == "their_wall_missing"


def test_contradiction_is_logged_and_alerted(validation_db):
    fav._init_db()
    theirs = dict(_THEIRS_NORMAL_DIVERGENT_VALUE, net_gex=-1.2e9)
    with patch.object(fav, "_pick_expiration", return_value="2026-10-16"), \
         patch.object(fav, "_our_gex", return_value=_OURS_NORMAL), \
         patch.object(fav, "_their_gex", return_value=theirs), \
         patch("engine.alert_channels.send_alert") as m_alert:
        fav.main()
        m_alert.assert_called_once()
        assert m_alert.call_args.kwargs["alert_type"] == "flashalpha_gex_structural_contradiction"

    conn = sqlite3.connect(str(fav.DB))
    row = conn.execute(
        "SELECT structural_match, contradiction_type FROM gex_validation_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert row == (0, "opposite_net_gex_sign")


def test_matching_structure_does_not_alert(validation_db):
    fav._init_db()
    with patch.object(fav, "_pick_expiration", return_value="2026-10-16"), \
         patch.object(fav, "_our_gex", return_value=_OURS_NORMAL), \
         patch.object(fav, "_their_gex", return_value=_THEIRS_NORMAL_DIVERGENT_VALUE), \
         patch("engine.alert_channels.send_alert") as m_alert:
        fav.main()
        m_alert.assert_not_called()
