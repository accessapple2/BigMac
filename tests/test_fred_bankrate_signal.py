"""
tests/test_fred_bankrate_signal.py — FRED-BANKRATE epic.

Covers the confirmatory-only rail:
  - confirmatory_vote() guardrail: sole voter NEVER counts / NEVER permits a trade,
    counts only once the fleet has >= MIN_FLEET_VOTES directional votes.
  - get_signal() never emits a trigger + caches.
  - Uhura confluence fold-in: a confirmatory vote can LIFT an existing convergence
    over the gate but can NEVER originate one.
  - (network) live pull of all 4 deposit APY series.

Run:  .venv/bin/python3 -m pytest tests/test_fred_bankrate_signal.py -v
Live: .venv/bin/python3 -m pytest tests/test_fred_bankrate_signal.py -v -m network
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from engine import fred_bankrate_signal as fb
from engine.fred_bankrate_signal import MIN_FLEET_VOTES, confirmatory_vote, get_signal


# ── confirmatory_vote() — the asserted guardrail ────────────────────────────

@pytest.mark.unit
def test_sole_voter_never_counts():
    """FRED alone (0 other fleet votes) must NOT count and must NOT permit a trade."""
    r = confirmatory_vote(fleet_directional_votes=0, lean="confirm")
    assert r["is_sole_voter"] is True
    assert r["counts_toward_convergence"] is False
    assert r["trade_permitted_on_fred_alone"] is False
    assert r["is_trigger"] is False


@pytest.mark.unit
def test_one_fleet_vote_still_below_minimum():
    """One other vote is still < MIN_FLEET_VOTES(2) → FRED cannot confirm yet."""
    assert MIN_FLEET_VOTES == 2
    r = confirmatory_vote(fleet_directional_votes=1, lean="caution")
    assert r["counts_toward_convergence"] is False
    assert r["trade_permitted_on_fred_alone"] is False


@pytest.mark.unit
def test_confirms_once_fleet_has_minimum():
    """With >= MIN_FLEET_VOTES aligned fleet votes, a directional lean confirms."""
    r = confirmatory_vote(fleet_directional_votes=2, lean="confirm")
    assert r["is_sole_voter"] is False
    assert r["counts_toward_convergence"] is True
    assert r["direction"] == "BULLISH"
    # Even when it counts, it can never authorize a trade *by itself*.
    assert r["trade_permitted_on_fred_alone"] is False


@pytest.mark.unit
def test_caution_maps_bearish_and_counts():
    r = confirmatory_vote(fleet_directional_votes=3, lean="caution")
    assert r["counts_toward_convergence"] is True
    assert r["direction"] == "BEARISH"


@pytest.mark.unit
def test_neutral_lean_never_counts_even_with_quorum():
    """A neutral macro lean contributes nothing regardless of fleet size."""
    r = confirmatory_vote(fleet_directional_votes=5, lean="neutral")
    assert r["counts_toward_convergence"] is False
    assert r["direction"] == "NEUTRAL"


@pytest.mark.unit
def test_guardrail_holds_across_the_whole_range():
    """Brute-force the invariant: sole voter never counts, for every lean."""
    for n in range(0, 8):
        for lean in ("confirm", "caution", "neutral"):
            r = confirmatory_vote(fleet_directional_votes=n, lean=lean)
            if n < MIN_FLEET_VOTES:
                assert r["counts_toward_convergence"] is False, (n, lean)
            assert r["trade_permitted_on_fred_alone"] is False
            assert r["is_trigger"] is False


# ── get_signal() — never a trigger, and caches ──────────────────────────────

@pytest.fixture
def fake_fred(monkeypatch):
    """Stub the network so get_signal() is deterministic and offline."""
    def _fake_fetch(series_id, lookback):
        # falling deposit APYs → lean 'confirm'
        base = {"BRMSA0104": 4.50, "BRMINTCA01": 0.50,
                "BRMCDS0101": 4.80, "BRMCDS0102": 3.90}[series_id]
        return [(f"2026-04-{i+1:02d}", base - i * 0.10) for i in range(lookback)]
    fb._SIGNAL_CACHE.update({"data": None, "ts": 0.0, "lookback": None})
    monkeypatch.setattr(fb, "_fetch", _fake_fetch)
    return _fake_fetch


@pytest.mark.unit
def test_get_signal_is_never_a_trigger(fake_fred):
    sig = get_signal(force_refresh=True)
    assert sig["is_trigger"] is False
    assert sig["vote"] in ("confirm", "neutral", "caution")
    assert sig["vote"] == "confirm"            # falling APYs
    assert len(sig["series"]) == 4             # all 4 deposit series present


@pytest.mark.unit
def test_get_signal_caches(fake_fred, monkeypatch):
    calls = {"n": 0}
    orig = fake_fred

    def _counting(series_id, lookback):
        calls["n"] += 1
        return orig(series_id, lookback)

    monkeypatch.setattr(fb, "_fetch", _counting)
    a = get_signal(force_refresh=True)         # 4 fetches
    assert calls["n"] == 4
    b = get_signal()                           # served from cache → no new fetch
    assert calls["n"] == 4
    assert a is b
    c = get_signal(force_refresh=True)         # bypass → 4 more
    assert calls["n"] == 8


# ── Uhura confluence fold-in: confirm, never originate ──────────────────────

def _dir_vote(source, direction, weight=1.0):
    from engine.uhura import SignalVote
    return SignalVote(source, direction, weight, "test")


def _fred_vote(direction):
    from engine.uhura import SignalVote
    return SignalVote("fred_bankrate", direction, 0.5, "macro", is_confirmatory=True)


@pytest.mark.unit
def test_fred_cannot_originate_a_convergence():
    """FRED confirmatory vote with no fleet votes → aligned stays 0 (no origination)."""
    from engine.uhura import LtUhura
    eng = LtUhura()
    conf = eng._calculate_confluence([_fred_vote("BULLISH")])
    assert conf["aligned_count"] == 0
    assert conf["confirmatory_applied"] == 0


@pytest.mark.unit
def test_fred_does_not_count_below_minimum():
    """One fleet vote + FRED agreeing → FRED still does not count (need >= 2)."""
    from engine.uhura import LtUhura
    eng = LtUhura()
    conf = eng._calculate_confluence([_dir_vote("gex", "BULLISH"), _fred_vote("BULLISH")])
    assert conf["aligned_count"] == 1
    assert conf["confirmatory_applied"] == 0


@pytest.mark.unit
def test_fred_confirms_and_lifts_over_the_gate():
    """3 aligned fleet votes (below the 4-gate) + FRED agreeing → lifted to 4."""
    from engine.uhura import LtUhura
    eng = LtUhura()
    votes = [_dir_vote("gex", "BULLISH"), _dir_vote("flow", "BULLISH"),
             _dir_vote("regime", "BULLISH"), _fred_vote("BULLISH")]
    conf = eng._calculate_confluence(votes)
    assert conf["confirmatory_applied"] == 1
    assert conf["aligned_count"] == 4          # 3 fleet + 1 confirm == gate (MIN_CONFLUENCE)
    assert conf["aligned_count"] >= eng.MIN_CONFLUENCE


@pytest.mark.unit
def test_fred_does_not_confirm_the_wrong_side():
    """FRED bearish while the fleet is bullish → does not count."""
    from engine.uhura import LtUhura
    eng = LtUhura()
    votes = [_dir_vote("gex", "BULLISH"), _dir_vote("flow", "BULLISH"),
             _fred_vote("BEARISH")]
    conf = eng._calculate_confluence(votes)
    assert conf["dominant_direction"] == "BULLISH"
    assert conf["confirmatory_applied"] == 0
    assert conf["aligned_count"] == 2


# ── Live integration: pull all 4 deposit APY series from FRED ───────────────

@pytest.mark.network
def test_live_pull_all_four_series():
    """Real FRED fetch — all 4 deposit APY series return usable values."""
    sig = get_signal(force_refresh=True)
    assert sig["is_trigger"] is False
    series = sig["series"]
    for sid in fb.DEPOSIT_VOTE_SERIES:
        assert sid in series, f"missing {sid}"
        row = series[sid]
        # either a usable latest value, or an explicit error/empty marker
        if "latest" in row:
            assert isinstance(row["latest"], (int, float))
            assert 0.0 <= row["latest"] < 50.0   # APY sanity bound
    print("\nLive FRED sample:")
    for sid in fb.DEPOSIT_VOTE_SERIES:
        print(f"  {sid:12s} {fb.ALL_SERIES[sid]:38s} -> {series.get(sid)}")
    print(f"  avg_deposit_bps={sig['avg_deposit_bps']}  vote={sig['vote']}")
