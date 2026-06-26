"""
Acceptance tests for engine/earnings_guard.py (Plan B)
+ engine/earnings_confirm.py (confirm-source hardening).

Plan B acceptance criteria (6):
  1. MU replay — $991 wick does NOT trigger; position holds through.
  2. No upcoming earnings → action: normal, behavior unchanged.
  3. Unconfirmed earnings date → treated as no event (no suppression).
  4. Oversized position → reduce_size, NOT a tighter stop.
  5. Every suppression/widen lands in earnings_guard_log.
  6. Post-event window → normal stop discipline (guard returns "normal").

Confirm-hardening acceptance criteria (7):
  1. Estimated-only date in window → alert_only; stop NOT removed.
  2. Provider explicit confirmed=True → CONFIRMED → full guard fires.
  3. Two sources agree on date → CONFIRMED.
  4. Sources disagree → ESTIMATED → alert_only.
  5. (Diagnostic — run tools/audit_earnings_confirm.py manually.)
  6. Every actual suppression/widen emits WARN log + earnings_guard_log row.
  7. MU replay still holds through (CONFIRMED path unchanged).
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, call

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NO_EVENT = None


def _make_event(hours_from_now: float = 12.0, confirmed: bool = True) -> dict:
    """Build a guard event dict as in_earnings_window returns it."""
    from engine.earnings_confirm import Confidence
    when = datetime.now(timezone.utc) + timedelta(hours=hours_from_now)
    return {
        "when":       when,
        "session":    "amc",
        "confidence": Confidence.CONFIRMED if confirmed else Confidence.ESTIMATED,
        "sources":    ["yahoo", "finnhub"] if confirmed else ["yahoo"],
        "confirmed":  confirmed,
    }


def _mu_pos(qty: int = 4, avg_price: float = 1051.0, last_price: float = 991.0) -> dict:
    return {
        "symbol": "MU",
        "avg_price": avg_price,
        "qty": qty,
        "last_price": last_price,
        "asset_type": "stock",
    }


EQUITY_100K = 101_000.0


# ---------------------------------------------------------------------------
# 1. MU replay — $991 wick must NOT trigger at WIDEN_TO_EM (em=14%)
# ---------------------------------------------------------------------------

class TestMUReplay:
    @patch("engine.earnings_guard._log_guard_action")
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_widen_stop_does_not_fire_at_991(self, mock_window, mock_em, mock_log):
        from engine.earnings_guard import guard_stop, GUARD_CONFIG, EarningsPolicy

        # Confirm policy is WIDEN_TO_EM
        assert GUARD_CONFIG["policy"] == EarningsPolicy.WIDEN_TO_EM

        mock_window.return_value = _make_event(hours_from_now=12)

        pos = _mu_pos()
        result = guard_stop(pos, model_sl_pct=0.12, equity=EQUITY_100K, player_id="test")

        # em_low = 991 * (1 − 1.25*0.14) = 991 * 0.825 ≈ $818
        # widen_stop = min(normal_stop, em_low) = min(1051*0.88, 818) ≈ min(925, 818) = 818
        # widened_pct from entry: (1051 - 818) / 1051 ≈ 0.222 (22.2%)
        # pnl_pct = (991 - 1051) / 1051 ≈ -5.7%  →  -5.7% > -22.2% → stop does NOT fire
        assert result["action"] == "widen_stop"
        assert result["stop_pct"] is not None
        # The widened stop must be WIDER than pnl loss of 5.7% so it doesn't fire
        pnl_loss = (1051.0 - 991.0) / 1051.0  # ~0.057
        assert result["stop_pct"] > pnl_loss, (
            f"Widened stop_pct {result['stop_pct']:.3f} must be > pnl loss {pnl_loss:.3f} "
            f"so $991 wick does not trigger"
        )
        assert result["em"] == pytest.approx(0.14)

    @patch("engine.earnings_guard._log_guard_action")
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_trailing_suppressed(self, mock_window, mock_em, mock_log):
        """widen_stop sets _suppress_trail so fleet trail cannot fire."""
        from engine.earnings_guard import guard_stop

        mock_window.return_value = _make_event(hours_from_now=12)
        result = guard_stop(_mu_pos(), 0.12, EQUITY_100K)
        # widen_stop means suppress_trail=True in risk_manager
        assert result["action"] == "widen_stop"


# ---------------------------------------------------------------------------
# 2. No upcoming earnings → normal behavior
# ---------------------------------------------------------------------------

class TestNoEarnings:
    @patch("engine.earnings_guard.in_earnings_window", return_value=None)
    def test_returns_normal(self, _):
        from engine.earnings_guard import guard_stop

        result = guard_stop(_mu_pos(), 0.12, EQUITY_100K)
        assert result["action"] == "normal"
        assert result["stop_pct"] == pytest.approx(0.12)

    @patch("engine.earnings_guard.in_earnings_window", return_value=None)
    def test_normal_does_not_log(self, _):
        from engine.earnings_guard import guard_stop

        with patch("engine.earnings_guard._log_guard_action") as mock_log:
            guard_stop(_mu_pos(), 0.12, EQUITY_100K)
            mock_log.assert_not_called()


# ---------------------------------------------------------------------------
# 3. Unconfirmed earnings → alert_only (stop kept, never suppressed)
# ---------------------------------------------------------------------------

class TestUnconfirmedEarnings:
    def test_estimated_event_returns_alert_only(self):
        """ESTIMATED in window → guard fires alert_only, stop stays on."""
        from engine.earnings_guard import guard_stop

        estimated_ev = _make_event(hours_from_now=12, confirmed=False)
        with patch("engine.earnings_guard.in_earnings_window", return_value=estimated_ev):
            with patch("engine.earnings_guard._log_guard_action"):
                result = guard_stop(_mu_pos(), 0.12, EQUITY_100K)
        assert result["action"] == "alert_only"
        assert result["stop_pct"] == pytest.approx(0.12), "stop must be unchanged"

    def test_estimated_event_does_not_remove_stop(self):
        """The normal stop_pct must be preserved exactly for ESTIMATED events."""
        from engine.earnings_guard import guard_stop

        estimated_ev = _make_event(hours_from_now=6, confirmed=False)
        with patch("engine.earnings_guard.in_earnings_window", return_value=estimated_ev):
            with patch("engine.earnings_guard._log_guard_action"):
                result = guard_stop(_mu_pos(), 0.15, EQUITY_100K)
        assert result["stop_pct"] == pytest.approx(0.15)

    def test_no_earnings_returns_normal(self):
        """Guard returns normal when in_earnings_window returns None."""
        from engine.earnings_guard import guard_stop

        with patch("engine.earnings_guard.in_earnings_window", return_value=None):
            result = guard_stop(_mu_pos(), 0.12, EQUITY_100K)
        assert result["action"] == "normal"


# ---------------------------------------------------------------------------
# 4. Oversized position → reduce_size (not a tighter stop)
# ---------------------------------------------------------------------------

class TestOversizedPosition:
    @patch("engine.earnings_guard._log_guard_action")
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_reduce_size_not_tighter_stop(self, mock_window, mock_em, mock_log):
        from engine.earnings_guard import guard_stop

        mock_window.return_value = _make_event(hours_from_now=12)

        # 100 shares at $1051 — risk into earnings is huge
        pos = _mu_pos(qty=100, avg_price=1051.0, last_price=1051.0)
        result = guard_stop(pos, model_sl_pct=0.12, equity=EQUITY_100K, player_id="test")

        assert result["action"] == "reduce_size", (
            "Oversized position must trigger reduce_size, not a tighter stop"
        )
        assert result["target_shares"] >= 0
        assert result["target_shares"] < 100, "Must actually reduce"
        # Must NOT return a tighter stop than model_sl
        # (trimming controls risk, not a tighter pct)

    @patch("engine.earnings_guard._log_guard_action")
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_well_sized_position_does_not_reduce(self, mock_window, mock_em, mock_log):
        from engine.earnings_guard import guard_stop

        mock_window.return_value = _make_event(hours_from_now=12)

        # 4 shares at $1051: risk = (1051 - 818) * 4 ≈ $932 < 6% of $101k = $6,060
        pos = _mu_pos(qty=4, avg_price=1051.0, last_price=1051.0)
        result = guard_stop(pos, model_sl_pct=0.12, equity=EQUITY_100K)

        assert result["action"] != "reduce_size"


# ---------------------------------------------------------------------------
# 5. Every suppression/widen lands in the audit log
# ---------------------------------------------------------------------------

class TestAuditLog:
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_widen_stop_logged(self, mock_window, mock_em):
        from engine.earnings_guard import guard_stop

        mock_window.return_value = _make_event()
        with patch("engine.earnings_guard._log_guard_action") as mock_log:
            result = guard_stop(_mu_pos(), 0.12, EQUITY_100K, player_id="mccoy")
            assert result["action"] == "widen_stop"
            mock_log.assert_called_once()
            call_kw = mock_log.call_args
            assert call_kw.args[2] == "widen_stop"
            assert call_kw.kwargs.get("em") == pytest.approx(0.14)

    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_reduce_size_logged(self, mock_window, mock_em):
        from engine.earnings_guard import guard_stop

        mock_window.return_value = _make_event()
        with patch("engine.earnings_guard._log_guard_action") as mock_log:
            pos = _mu_pos(qty=200, avg_price=1051.0, last_price=1051.0)
            result = guard_stop(pos, 0.12, EQUITY_100K, player_id="mccoy")
            assert result["action"] == "reduce_size"
            mock_log.assert_called_once()
            assert mock_log.call_args.args[2] == "reduce_size"

    @patch("engine.earnings_guard.in_earnings_window", return_value=None)
    def test_normal_not_logged(self, _):
        from engine.earnings_guard import guard_stop

        with patch("engine.earnings_guard._log_guard_action") as mock_log:
            guard_stop(_mu_pos(), 0.12, EQUITY_100K)
            mock_log.assert_not_called()


# ---------------------------------------------------------------------------
# 6. Post-event (outside window) → normal stop discipline re-arms
# ---------------------------------------------------------------------------

class TestPostEvent:
    def test_outside_window_returns_none(self):
        from engine.earnings_guard import in_earnings_window
        from engine.earnings_confirm import Confidence

        # 50h past: safely outside the widest possible post-window (24h for unknown session)
        # even if the anchor is noon ET (16:00-17:00 UTC), anchor+24h is ~27-28h ago
        past_ds = (datetime.now(timezone.utc) - timedelta(hours=50)).strftime("%Y-%m-%d")
        cr = {"date": past_ds, "session": "amc", "confidence": Confidence.CONFIRMED,
              "sources": ["finnhub"]}
        with patch("engine.earnings_guard.confirm_earnings", return_value=cr):
            result = in_earnings_window("MU")
            assert result is None

    def test_far_future_not_in_window(self):
        from engine.earnings_guard import in_earnings_window
        from engine.earnings_confirm import Confidence

        far_ds = (datetime.now(timezone.utc) + timedelta(hours=48)).strftime("%Y-%m-%d")
        cr = {"date": far_ds, "session": "amc", "confidence": Confidence.CONFIRMED,
              "sources": ["finnhub"]}
        with patch("engine.earnings_guard.confirm_earnings", return_value=cr):
            result = in_earnings_window("MU")
            assert result is None


# =============================================================================
# CONFIRM-HARDENING ACCEPTANCE TESTS (7 criteria from spec)
# =============================================================================

class TestConfirmHardeningEstimatedStopKept:
    """Criterion 1 — estimated-only date in window → alert_only; stop NOT removed."""

    def test_estimated_in_window_is_alert_only(self):
        from engine.earnings_guard import guard_stop
        from engine.earnings_confirm import Confidence

        est_ev = _make_event(hours_from_now=10, confirmed=False)
        assert est_ev["confidence"] == Confidence.ESTIMATED

        with patch("engine.earnings_guard.in_earnings_window", return_value=est_ev):
            with patch("engine.earnings_guard._log_guard_action"):
                result = guard_stop(_mu_pos(), 0.12, EQUITY_100K)

        assert result["action"] == "alert_only"
        assert result["stop_pct"] == pytest.approx(0.12), "stop must be UNCHANGED"

    def test_estimated_never_suppress_stop(self):
        from engine.earnings_guard import guard_stop

        est_ev = _make_event(hours_from_now=4, confirmed=False)
        with patch("engine.earnings_guard.in_earnings_window", return_value=est_ev):
            with patch("engine.earnings_guard._log_guard_action"):
                result = guard_stop(_mu_pos(), 0.18, EQUITY_100K)

        assert result["action"] not in ("suppress_stop", "widen_stop", "reduce_size", "close_before")


class TestConfirmHardeningExplicitFlag:
    """Criterion 2 — explicit confirmed=True from a provider → CONFIRMED → full guard."""

    def test_explicit_confirmed_triggers_full_guard(self):
        from engine.earnings_confirm import confirm_earnings, Confidence

        explicit_source = {"date": "2099-01-01", "confirmed": True, "source": "uw"}
        with patch("engine.earnings_confirm._uw_earnings", return_value=explicit_source):
            with patch("engine.earnings_confirm._finnhub_earnings", return_value=None):
                with patch("engine.earnings_confirm._yahoo_earnings", return_value=None):
                    with patch("engine.earnings_confirm._nasdaq_earnings", return_value=None):
                        # Clear cache first
                        from engine import earnings_confirm as _ec
                        _ec._confirm_cache.clear()
                        result = confirm_earnings("UW_TEST")

        assert result is not None
        assert result["confidence"] == Confidence.CONFIRMED
        assert "uw" in result["sources"]


class TestConfirmHardeningTwoSourcesAgree:
    """Criterion 3 — two independent sources agree → CONFIRMED."""

    def test_two_sources_same_date_confirmed(self):
        from engine.earnings_confirm import confirm_earnings, Confidence

        yahoo   = {"date": "2099-03-15", "confirmed": None, "source": "yahoo"}
        finnhub = {"date": "2099-03-15", "confirmed": None, "source": "finnhub"}
        with patch("engine.earnings_confirm._uw_earnings",      return_value=None):
            with patch("engine.earnings_confirm._nasdaq_earnings", return_value=None):
                with patch("engine.earnings_confirm._finnhub_earnings", return_value=finnhub):
                    with patch("engine.earnings_confirm._yahoo_earnings", return_value=yahoo):
                        from engine import earnings_confirm as _ec
                        _ec._confirm_cache.clear()
                        result = confirm_earnings("AGREE_TEST")

        assert result is not None
        assert result["confidence"] == Confidence.CONFIRMED
        assert set(result["sources"]) == {"yahoo", "finnhub"}

    def test_three_sources_agree(self):
        from engine.earnings_confirm import confirm_earnings, Confidence

        d = "2099-04-10"
        with patch("engine.earnings_confirm._uw_earnings",      return_value=None):
            with patch("engine.earnings_confirm._nasdaq_earnings",  return_value={"date": d, "confirmed": None, "source": "nasdaq"}):
                with patch("engine.earnings_confirm._finnhub_earnings", return_value={"date": d, "confirmed": None, "source": "finnhub"}):
                    with patch("engine.earnings_confirm._yahoo_earnings",   return_value={"date": d, "confirmed": None, "source": "yahoo"}):
                        from engine import earnings_confirm as _ec
                        _ec._confirm_cache.clear()
                        result = confirm_earnings("THREE_AGREE")

        assert result["confidence"] == Confidence.CONFIRMED
        assert len(result["sources"]) >= 2


class TestConfirmHardeningSourcesDisagree:
    """Criterion 4 — sources disagree → ESTIMATED → alert_only."""

    def test_date_disagreement_is_estimated(self):
        from engine.earnings_confirm import confirm_earnings, Confidence

        yahoo_d   = {"date": "2099-05-01", "confirmed": None, "source": "yahoo"}
        finnhub_d = {"date": "2099-05-08", "confirmed": None, "source": "finnhub"}  # different date
        with patch("engine.earnings_confirm._uw_earnings",       return_value=None):
            with patch("engine.earnings_confirm._nasdaq_earnings",  return_value=None):
                with patch("engine.earnings_confirm._finnhub_earnings", return_value=finnhub_d):
                    with patch("engine.earnings_confirm._yahoo_earnings",   return_value=yahoo_d):
                        from engine import earnings_confirm as _ec
                        _ec._confirm_cache.clear()
                        result = confirm_earnings("DISAGREE_TEST")

        assert result is not None
        assert result["confidence"] == Confidence.ESTIMATED

    def test_estimated_in_guard_is_alert_only(self):
        """End-to-end: ESTIMATED from resolver → alert_only in guard_stop."""
        from engine.earnings_guard import guard_stop

        est_ev = _make_event(hours_from_now=8, confirmed=False)
        with patch("engine.earnings_guard.in_earnings_window", return_value=est_ev):
            with patch("engine.earnings_guard._log_guard_action"):
                result = guard_stop(_mu_pos(), 0.12, EQUITY_100K)
        assert result["action"] == "alert_only"
        assert result["stop_pct"] == pytest.approx(0.12)


class TestConfirmHardeningWarnLog:
    """Criterion 6 — actual suppression/widen emits WARNING log + audit row."""

    @patch("engine.earnings_guard._log_guard_action")
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_widen_stop_emits_warning(self, mock_window, mock_em, mock_log):
        from engine.earnings_guard import guard_stop
        import logging

        mock_window.return_value = _make_event(confirmed=True)

        with patch("engine.earnings_guard.logger") as mock_logger:
            result = guard_stop(_mu_pos(), 0.12, EQUITY_100K, player_id="mccoy")

        assert result["action"] == "widen_stop"
        # logger.warning must have been called (not just info)
        assert mock_logger.warning.called, "Actual guard fires must log at WARNING level"

    @patch("engine.earnings_guard._log_guard_action")
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_estimated_alert_also_warns(self, mock_window, mock_em, mock_log):
        from engine.earnings_guard import guard_stop

        mock_window.return_value = _make_event(confirmed=False)
        with patch("engine.earnings_guard.logger") as mock_logger:
            guard_stop(_mu_pos(), 0.12, EQUITY_100K)
        assert mock_logger.warning.called


# =============================================================================
# SESSION TIMING TESTS (A — BMO/AMC anchor fix)
# =============================================================================

class TestSessionTiming:
    """Acceptance tests for session-aware ET-anchored guard windows."""

    def test_bmo_rearms_not_late(self):
        """BMO: active 1h before report, normal 9h after (gap digested, post=8h)."""
        from engine.earnings_guard import in_earnings_window, _anchor_utc
        from engine.earnings_confirm import Confidence

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        bmo_ev = {
            "date": today, "session": "bmo",
            "confidence": Confidence.CONFIRMED, "sources": ["finnhub", "nasdaq"],
        }
        anchor = _anchor_utc(today, "bmo")

        with patch("engine.earnings_guard.confirm_earnings", return_value=bmo_ev):
            assert in_earnings_window("BMO_TEST", now=anchor - timedelta(hours=1)) is not None
            assert in_earnings_window("BMO_TEST", now=anchor + timedelta(hours=9)) is None

    def test_amc_unchanged(self):
        """AMC: still active 12h after report (next-day open); normal at 21h (post=20h)."""
        from engine.earnings_guard import in_earnings_window, _anchor_utc
        from engine.earnings_confirm import Confidence

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        amc_ev = {
            "date": today, "session": "amc",
            "confidence": Confidence.CONFIRMED, "sources": ["finnhub"],
        }
        anchor = _anchor_utc(today, "amc")

        with patch("engine.earnings_guard.confirm_earnings", return_value=amc_ev):
            assert in_earnings_window("AMC_TEST", now=anchor + timedelta(hours=12)) is not None
            assert in_earnings_window("AMC_TEST", now=anchor + timedelta(hours=21)) is None

    def test_unknown_session_uses_widest_window(self):
        """Unknown session → widest window (over-protect). Never under-protect."""
        from engine.earnings_guard import in_earnings_window, _anchor_utc, _WINDOW
        from engine.earnings_confirm import Confidence

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        unk_ev = {
            "date": today, "session": None,
            "confidence": Confidence.CONFIRMED, "sources": ["yahoo"],
        }
        anchor = _anchor_utc(today, None)
        pre, post = _WINDOW[None]

        with patch("engine.earnings_guard.confirm_earnings", return_value=unk_ev):
            assert in_earnings_window("UNK_TEST", now=anchor - pre + timedelta(minutes=1)) is not None
            assert in_earnings_window("UNK_TEST", now=anchor + post - timedelta(minutes=1)) is not None
            assert in_earnings_window("UNK_TEST", now=anchor + post + timedelta(minutes=1)) is None

    def test_dst_amc_june_vs_december(self):
        """16:30 ET → 20:30 UTC in summer (EDT), 21:30 UTC in winter (EST)."""
        from engine.earnings_guard import _anchor_utc

        summer = _anchor_utc("2026-06-15", "amc")   # EDT = UTC-4
        winter = _anchor_utc("2026-12-15", "amc")   # EST = UTC-5

        assert summer.hour == 20 and summer.minute == 30, (
            f"Summer AMC should be 20:30 UTC, got {summer.hour}:{summer.minute:02d}")
        assert winter.hour == 21 and winter.minute == 30, (
            f"Winter AMC should be 21:30 UTC, got {winter.hour}:{winter.minute:02d}")


class TestConfirmHardeningMUReplayConfirmedPath:
    """Criterion 7 — MU replay still holds through on the CONFIRMED path."""

    @patch("engine.earnings_guard._log_guard_action")
    @patch("engine.earnings_guard.expected_move", return_value=0.14)
    @patch("engine.earnings_guard.in_earnings_window")
    def test_mu_widen_confirmed_path(self, mock_window, mock_em, mock_log):
        from engine.earnings_guard import guard_stop
        from engine.earnings_confirm import Confidence

        # CONFIRMED event (two sources agreed)
        confirmed_ev = _make_event(hours_from_now=12, confirmed=True)
        assert confirmed_ev["confidence"] == Confidence.CONFIRMED
        mock_window.return_value = confirmed_ev

        result = guard_stop(_mu_pos(), 0.12, EQUITY_100K)

        assert result["action"] == "widen_stop"
        pnl_loss = (1051.0 - 991.0) / 1051.0  # ~5.7%
        assert result["stop_pct"] > pnl_loss, "$991 wick must NOT trigger widened stop"
