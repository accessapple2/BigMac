"""tests/test_bridge_consistency.py — HM-BRIDGE-CONSISTENCY-2026-09-13.

Keeps the 9/12 Bridge Classic repairs from drifting back. Fixture DBs + the real route
functions in dashboard/app.py; front-end logic is exercised by extracting the real JS
functions from index.html and running them under node with stubbed DOM/fetch. No live
network, no write-capable calls (OT_ALERTS_DISABLED=1 via conftest).

Two tests are strict xfails that document bridge items still OPEN — they turn into
failures (XPASS) the moment the underlying bug is fixed, forcing the marker off:
  * test_single_regime_across_panels  (item 1: Riker reads a different regime classifier)
  * test_leaderboard_backend_sorts_by_return (item 2: backend sorts on total_value)
"""
from __future__ import annotations

import copy
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import types
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import dashboard.app as app  # noqa: E402
from engine import bridge_staleness as bs  # noqa: E402

INDEX = (ROOT / "dashboard" / "static" / "index.html").read_text()
V2 = (ROOT / "dashboard" / "static" / "bridge-v2.html").read_text()
APP_SRC = (ROOT / "dashboard" / "app.py").read_text()
MEDALS = {"\U0001F947": 1, "\U0001F948": 2, "\U0001F949": 3}


# ── helpers ──────────────────────────────────────────────────────────────────

def _js_function(src: str, name: str) -> str:
    """Source of a top-level JS function: from its column-0 declaration to the next
    column-0 closing brace. (A brace/quote scanner mis-reads regex literals like /'/g.)"""
    m = re.search(r"^(?:async\s+)?function\s+" + re.escape(name) + r"\s*\(", src, re.M)
    assert m, f"top-level function {name} not found"
    end = re.search(r"^\}[ \t]*$", src[m.end():], re.M)
    assert end, f"no column-0 closing brace for {name}"
    return src[m.start():m.end() + end.end()]


def _node(script: str) -> dict:
    node = shutil.which("node")
    assert node, "node is required for the Bridge front-end consistency tests"
    r = subprocess.run([node, "-e", script], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def _stub_modules(monkeypatch, *names):
    for n in names:
        monkeypatch.setitem(sys.modules, n, types.ModuleType(n))


# ── 1. regime ────────────────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="Bridge item 1 OPEN: Riker's prompt reads engine.regime_detector (50/200MA) "
                          "while /api/regime serves regime_history (8/21MA). Reveille not covered yet.")
def test_single_regime_across_panels(tmp_path, monkeypatch):
    (tmp_path / "data").mkdir()
    c = sqlite3.connect(tmp_path / "data" / "trader.db")
    c.execute("CREATE TABLE regime_history (regime TEXT, size_modifier REAL, date TEXT)")
    c.execute("INSERT INTO regime_history VALUES ('BEAR_CROSS', 0.25, ?)", (datetime.now().date().isoformat(),))
    c.commit()
    c.close()
    monkeypatch.chdir(tmp_path)

    import engine.warp10_engine as w
    monkeypatch.setattr(w, "get_current_allocation", lambda: {"regime": "BULL", "size_modifier": 1.0})
    api_regime = app.regime_status().get("regime")
    if api_regime != "BEAR_CROSS":
        pytest.fail(f"fixture precondition: /api/regime returned {api_regime!r}")

    import engine.regime_detector as rd
    monkeypatch.setattr(rd, "detect_regime",
                        lambda: {"regime": "BULL_TREND", "vix": 15.8, "spy_price": 764.2, "spy_change": 0.8})
    _stub_modules(monkeypatch, "engine.cto_advisor", "engine.first_officer", "engine.consensus",
                  "engine.alphavantage_data", "engine.picard_strategy", "engine.crew_dissent")
    import engine.riker_xo as rx
    monkeypatch.setattr(rx, "_cache", {"recommendation": None, "ts": 0})
    captured: dict = {}

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"response": "stub", "done_reason": "stop"}

    def _post(url, json=None, timeout=None):
        captured["prompt"] = json["prompt"]
        return _Resp()

    monkeypatch.setattr(rx.requests, "post", _post)
    rx._do_riker_synthesis()
    m = re.search(r"REGIME: (\S+)", captured.get("prompt", ""))
    if not m:
        pytest.fail("Riker prompt carried no REGIME line")
    assert m.group(1) == api_regime


# ── 2. leaderboard ───────────────────────────────────────────────────────────

def test_leaderboard_sorted_and_ranked():
    """Ranks are assigned client-side; they must be 1..N over VISIBLE rows (a benched
    player used to leave a gap), in the order the server sent (float return)."""
    fn = _js_function(INDEX, "buildArenaLbRows")
    players = [
        {"player_id": "a", "name": "A", "provider": "p", "model": "m", "return_pct": 2.18},
        {"player_id": "bench", "name": "Seven", "provider": "p", "model": "m", "return_pct": 1.0},
        {"player_id": "b", "name": "B", "provider": "p", "model": "m", "return_pct": 0.0},
        {"player_id": "c", "name": "C", "provider": "p", "model": "m", "return_pct": "-0.02"},
        {"player_id": "d", "name": "D", "provider": "p", "model": "m", "return_pct": -7.0185},
    ]
    script = (
        "var _LB_BENCHED={bench:true};var MCOLORS={};var ACTIVE_PLAYER_IDS=new Set();"
        "function getLegend(){return ''}function getInitial(){return 'X'}"
        "function getTierBadge(){return ''}function getCrewRole(){return ''}"
        + fn + ";console.log(JSON.stringify({h:buildArenaLbRows(" + json.dumps(players) + ",'x',true)}))"
    )
    html = _node(script)["h"]
    ranks = [MEDALS.get(r, None) or int(r) for r in re.findall(r'<td class="rank-cell">(.*?)</td>', html)]
    shown = re.findall(r'data-player="([^"]+)" style', html)
    assert shown == ["a", "b", "c", "d"]
    assert ranks == [1, 2, 3, 4]
    rets = [float(p["return_pct"]) for p in players if p["player_id"] in shown]
    assert all(x >= y for x, y in zip(rets, rets[1:]))


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="Bridge item 2 OPEN: /api/arena/leaderboard sorts on total_value, not return_pct")
def test_leaderboard_backend_sorts_by_return():
    assert re.search(r'result\.sort\(key=lambda x: (float\()?x\["return_pct"\]', APP_SRC)


# ── 2b. agent counts ─────────────────────────────────────────────────────────

def test_agent_counts_single_source(tmp_path, monkeypatch):
    db = tmp_path / "trader.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE ai_players (id TEXT, halt_mode TEXT, is_active INTEGER)")
    rows = [("a%d" % i, "active", 1) for i in range(3)] + [("e%d" % i, "exit_only", 1) for i in range(2)] \
        + [("f%d" % i, "full", 0) for i in range(4)]
    c.executemany("INSERT INTO ai_players VALUES (?,?,?)", rows)
    c.commit()
    c.close()

    real_connect = sqlite3.connect

    def _redirect(target, *a, **k):
        t = str(target)
        if t.endswith(".db") or ".db?" in t:
            if t.startswith("file:"):
                t, k["uri"] = f"file:{db}?mode=ro", True
            else:
                t = str(db)
        return real_connect(t, *a, **k)

    monkeypatch.setattr(sqlite3, "connect", _redirect)
    import engine.shadow_csp_scorecard as sc

    def _no_csp():
        raise RuntimeError("not under test")

    monkeypatch.setattr(sc, "compute", _no_csp)
    status = app.systems_status()
    detail = next(s["detail"] for s in status["systems"] if s["name"] == "Trading agents")
    m = re.match(r"(\d+) active · (\d+) halted \((\d+) exit-only, (\d+) full\)", detail)
    assert m, detail
    active, halted, exit_only, full = map(int, m.groups())

    import engine.fleet_status as fs
    monkeypatch.setattr(fs, "DB_PATH", db)
    fleet = fs._fleet_counts()
    assert (active, exit_only, full) == (fleet["active"], fleet["exit_only"], fleet["full"])
    assert active + halted == fleet["total"] == len(rows)


# ── 5. season label ──────────────────────────────────────────────────────────

def test_season_label_derived(tmp_path, monkeypatch):
    db = tmp_path / "trader.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE settings (key TEXT, value TEXT)")
    c.execute("CREATE TABLE season_config (season INTEGER)")
    c.executemany("INSERT INTO settings VALUES (?,?)",
                  [("current_season", "9"), ("season_9_start", "2026-09-11")])
    c.commit()
    c.close()

    def _conn():
        cc = sqlite3.connect(db)
        cc.row_factory = sqlite3.Row
        return cc

    monkeypatch.setattr(app, "_conn", _conn)
    info = app.season_info()
    assert info["season"] == 9 and info["name"] == "Season 9"

    # Acceptance #5's grep, minus comment lines and the two historical tooltips
    # ("Seven of Nine — benched Season 5", "Sulu — benched Season 5") which state when
    # they were benched, not the current season.
    hits = []
    for fname, src in (("index.html", INDEX), ("bridge-v2.html", V2)):
        for i, line in enumerate(src.splitlines(), 1):
            s = line.strip()
            if s.startswith(("//", "<!--", "*", "#")) or "benched Season 5" in line:
                continue
            if re.search(r"SEASON 5|Season 5", line):
                hits.append(f"{fname}:{i}: {s[:120]}")
    assert not hits, hits


# ── 10. DSR gate ─────────────────────────────────────────────────────────────

def test_dsr_hidden_below_gate(monkeypatch):
    payload = {
        "graduate_n": 30,
        "baseline": {"agent": "options-sosnoff", "n_closed": 0, "dsr": None},
        "seats": [
            {"agent": "shadow-qwen35-csp", "n_closed": 3, "dsr": 0.9876, "graduation": {"verdict": "HOLD"}},
            {"agent": "shadow-plutus-csp", "n_closed": 29, "dsr": 0.96, "graduation": {"verdict": "HOLD"}},
            {"agent": "graduated", "n_closed": 30, "dsr": 0.97, "graduation": {"verdict": "HOLD"}},
        ],
    }
    import engine.shadow_csp_scorecard as sc
    monkeypatch.setattr(sc, "compute", lambda: payload)
    out = app.shadow_csp_standings()
    assert out["seats"][0]["dsr"] is None and out["seats"][0]["dsr_withheld"] == "N=3 < 30"
    assert out["seats"][1]["dsr"] is None
    assert out["seats"][2]["dsr"] == 0.97 and "dsr_withheld" not in out["seats"][2]
    assert payload["seats"][0]["dsr"] == 0.9876, "route must not mutate the scorecard's dict"

    # both tiers gate on the payload's graduate_n, not a literal
    assert "var gate=d.graduate_n||30;" in INDEX and "n<gate" in INDEX
    assert "const gate    = d.graduate_n || 30;" in V2 and "nClosed >= gate" in V2
    assert "nClosed >= 5" not in V2


# ── 3. staleness contract ────────────────────────────────────────────────────

OLD_UTC = "2026-07-21 13:05:20"


def _canon(asof):
    return {"underlying": "SPY", "spot": 764.0, "gamma_flip": 752.0, "call_wall": 748.0, "put_wall": 607.0,
            "king_node": 740.0, "regime": "x", "strikes": [], "magnets": [], "_asof": asof, "_src": "test"}


@pytest.mark.parametrize("stale", [True, False])
def test_stale_flag_present(monkeypatch, stale):
    utc_ts = OLD_UTC if stale else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    local_ts = "2026-07-21T13:05:20" if stale else datetime.now().isoformat()
    monkeypatch.setattr(app, "_canonical_gex_cached", lambda sym: _canon(utc_ts))
    import engine.gex_scanner as gs
    monkeypatch.setattr(gs, "GEX_TICKERS", ["SPY"])
    import engine.battle_station_0dte as b0
    monkeypatch.setattr(b0, "get_status", lambda: {"status": "ARMED", "levels_as_of": utc_ts})
    import engine.gamma_environment as ge
    monkeypatch.setattr(ge, "detect_gamma_environment", lambda: {"environment": "negative", "as_of": local_ts})

    payloads = {
        "/api/market/gex": app.gex_all()[0],
        "/api/market/gex/{ticker}": app.gex_ticker("SPY"),
        "/api/gex-overlay/levels": app.gex_overlay_levels("SPY"),
        "/api/gex-overlay/heatmap": app.gex_overlay_heatmap("SPY"),
        "/api/battle-station-0dte/status": app.battle_station_0dte_status(),
        "/api/gamma-environment": app.gamma_environment(),
    }
    for route, p in payloads.items():
        missing = {"as_of", "source", "age_hours", "stale"} - set(p)
        assert not missing, f"{route} missing {missing}"
        assert p["stale"] is stale, (route, p["as_of"], p["age_hours"])
        assert p["as_of"], route

    # both tiers carry the one render rule and wire it to every GEX consumer
    for fname, src, calls in (("index.html", INDEX, 4), ("bridge-v2.html", V2, 3)):
        assert "function otStaleApply(" in src and "function otStaleText(" in src, fname
        assert src.count("otStaleApply(") - 1 >= calls, fname


def test_stamp_helper_contract():
    missing = bs.stamp({"x": 1}, None, "src", 24)
    assert missing["stale"] is True and missing["age_hours"] is None
    now_utc = datetime.now(timezone.utc)
    naive_utc = (now_utc - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    assert bs.stamp({}, naive_utc, "s", 24)["stale"] is False
    assert 1.9 < bs.age_hours(naive_utc) < 2.1
    naive_local = (datetime.now() - timedelta(hours=2)).isoformat()
    assert 1.9 < bs.age_hours(naive_local, naive_tz="local") < 2.1
    assert bs.stamp({}, (now_utc - timedelta(hours=25)).timestamp(), "s", 24)["stale"] is True
    src = {"a": 1}
    bs.stamp(src, naive_utc, "s", 24)
    assert src == {"a": 1}, "stamp must not mutate its input"


# ── 8. Archer briefing ───────────────────────────────────────────────────────

def test_archer_no_dupes_no_fffd(tmp_path, monkeypatch):
    import engine.archer_morning_synthesis as am
    db = tmp_path / "trader.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE institutional_signals (ticker TEXT, signal TEXT, reasoning TEXT, "
              "scan_date TEXT, created_at TEXT)")
    today = datetime.now(timezone.utc).date().isoformat()
    c.executemany("INSERT INTO institutional_signals VALUES (?,?,?,?,?)", [
        ("NUKZ", "STRONG_SELL", "6 insider sells → net −$4.2M — CEO included", today, today + " 10:00:03"),
        ("NUKZ", "STRONG_SELL", "second filing — CFO", today, today + " 10:00:02"),
        ("NUKZ", "STRONG_SELL", "third filing — director", today, today + " 10:00:01"),
        ("AAPL", "BUY", "13F add — 2.1M sh", today, today + " 09:00:00"),
    ])
    c.commit()
    c.close()
    monkeypatch.setattr(am, "DB_TRADER", db)
    uhura = am.get_uhura_signals()
    assert [u["ticker"] for u in uhura].count("NUKZ") == 1

    monkeypatch.setattr(am, "get_kirk_advisory", lambda: [])
    monkeypatch.setattr(am, "get_whale_detections", lambda: [])
    monkeypatch.setattr(am, "get_scanner_top_signals", lambda: [])
    monkeypatch.setattr(am, "get_market_regime", lambda: {"regime": "BEAR_CROSS", "vix": 15.8})
    text = am.build_briefing()
    assert "�" not in text
    assert not re.search(r"\w \? \w", text), "character substituted with '?'"
    assert "→" in text and "—" in text, "non-ASCII must survive the writer"
    assert text.count("NUKZ") == 1
    dupes = [ln for ln, n in Counter(l.strip() for l in text.splitlines() if l.strip()).items() if n > 1]
    assert not dupes, dupes


# ── 9. consensus panel ───────────────────────────────────────────────────────

def test_consensus_filters_empty_votes():
    fn = _js_function(INDEX, "fetchRikerRecommendation")
    cons = {"overall_agreement": 50, "tickers": {
        "NVDA": {"spock": {"action": "BUY"}, "data": None, "uhura": None, "comparison": "split"},
        "ZZEMPTYA": {},
        "ZZEMPTYB": {"spock": None, "data": None, "uhura": None},
    }}
    script = (
        "var ELS={};function mkEl(){return {style:{},textContent:'',innerHTML:'',"
        "classList:{toggle(){},add(){},remove(){}},setAttribute(){},querySelector(){return null}}}"
        "var document={getElementById:function(id){return ELS[id]||(ELS[id]=mkEl())}};"
        "var RESP={'/api/riker/recommendation':{},'/api/consensus':" + json.dumps(cons)
        + ",'/api/rikers-log?limit=5':{entries:[]}};"
        "function fetch(u){return Promise.resolve({ok:true,json:function(){return Promise.resolve(RESP[u]||{})}})}"
        "function openTickerDetail(){}"
        + fn +
        ";fetchRikerRecommendation().then(function(){console.log(JSON.stringify({h:ELS.xoConsensus.innerHTML}))})"
        ".catch(function(e){console.log(JSON.stringify({err:String(e)}))});"
    )
    out = _node(script)
    assert "err" not in out, out
    h = out["h"]
    assert "NVDA" in h
    assert "ZZEMPTYA" not in h and "ZZEMPTYB" not in h
    assert "+ 2 more tickers scanned with no crew votes" in h
