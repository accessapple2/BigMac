// shelly_net_watchdog.js — HM-SHELLY-PREP-V2 (2026-07-01) network watchdog.
//
// NOT a repo-executed script — this runs ON the Shelly plug itself (mJS, the
// restricted on-device JS engine on Gen2+/Gen3 Shelly firmware). Paste
// verbatim into the plug's own web UI: Settings > Scripts > + Add Script >
// paste > Save > enable "Run on startup" > Start.
//
// Used AS-IS, IDENTICAL, on BOTH the Allo router plug (.244) and the
// Starlink Mini plug (RV rig, GL-MT3000 network) — directive item 3 asked
// for one script covering both. Never installed on the bigmac (.245) or
// olliemax (.246) plugs — those are manual-only (see CLAUDE.md doctrine:
// never self-cycling on DB hosts).
//
// CAVEAT (documented honestly, not tested against real hardware in this
// session — no plug exists yet to run this on): mJS on Shelly Gen2/3 has no
// raw ICMP socket API, so "ping 8.8.8.8 / 1.1.1.1" here is implemented as an
// HTTP-reachability proxy via the documented Shelly "HTTP.GET" RPC method,
// not a true ICMP ping. Any HTTP-level response (even non-2xx) still proves
// the network path is live; only a genuine connect/timeout failure counts as
// "down". Verify the exact Shelly.call("HTTP.GET", ...) callback signature
// against the actual on-device Script editor's autocomplete/docs before
// relying on this in production — mJS API details can vary slightly by
// firmware version and this could not be run against real hardware to confirm.
//
// Logic: check both targets every 60s. If BOTH fail for 15 CONSECUTIVE
// checks (15 minutes), power-cycle this plug's own switched output: off,
// wait 20s, on. Flap guard: at most one cycle per 30 minutes (tracked as
// "checks since last cycle", not wall-clock time, to avoid depending on an
// uptime API this session couldn't verify either). The fail streak resets
// to 0 on ANY successful check (either target).
//
// Cycling the SWITCHED OUTPUT does not kill this script: on a Shelly Plug,
// the device's own WiFi/MCU stays powered from mains at all times -- only
// the socket the target device is plugged into gets cut. The script keeps
// running throughout its own cycle.

let CHECK_INTERVAL_MS = 60000;   // 60s per check
let FAIL_CHECKS_TRIGGER = 15;    // 15 consecutive both-fail checks = 15 min
let CYCLE_OFF_MS = 20000;        // 20s off during a cycle
let FLAP_GUARD_CHECKS = 30;      // 30 checks (~30 min) minimum between cycles
let HTTP_TIMEOUT_S = 5;

let failStreak = 0;
let checksSinceLastCycle = 999999;  // large so the very first trigger isn't blocked

function checkOne(url, cb) {
  Shelly.call("HTTP.GET", { url: url, timeout: HTTP_TIMEOUT_S }, function (result, error_code, error_message) {
    // error_code === 0 means the HTTP layer completed a round trip (any
    // status code) -- treat that as "network path up" for this reachability
    // proxy. A nonzero error_code (timeout/DNS/connect-refused at the
    // network layer) is the only thing counted as "down".
    let up = (error_code === 0);
    cb(up);
  });
}

function doCycle() {
  print("[net-watchdog] TRIGGER -- cycling switched output (off " + JSON.stringify(CYCLE_OFF_MS) + "ms, then on)");
  checksSinceLastCycle = 0;
  failStreak = 0;
  Shelly.call("Switch.Set", { id: 0, on: false }, function () {
    Timer.set(CYCLE_OFF_MS, false, function () {
      Shelly.call("Switch.Set", { id: 0, on: true }, function () {
        print("[net-watchdog] cycle complete -- output back ON");
      });
    });
  });
}

function runCheck() {
  checksSinceLastCycle = checksSinceLastCycle + 1;
  checkOne("http://8.8.8.8", function (up1) {
    checkOne("http://1.1.1.1", function (up2) {
      let bothDown = (!up1 && !up2);
      if (!bothDown) {
        if (failStreak > 0) {
          print("[net-watchdog] recovered -- resetting fail streak (was " + JSON.stringify(failStreak) + ")");
        }
        failStreak = 0;
        return;
      }
      failStreak = failStreak + 1;
      print("[net-watchdog] both targets unreachable -- fail streak " + JSON.stringify(failStreak) + "/" + JSON.stringify(FAIL_CHECKS_TRIGGER));
      if (failStreak < FAIL_CHECKS_TRIGGER) {
        return;
      }
      if (checksSinceLastCycle < FLAP_GUARD_CHECKS) {
        print("[net-watchdog] trigger reached but flap guard active (" + JSON.stringify(checksSinceLastCycle) + "/" + JSON.stringify(FLAP_GUARD_CHECKS) + " checks since last cycle) -- skipping, NOT resetting streak");
        return;
      }
      doCycle();
    });
  });
}

Timer.set(CHECK_INTERVAL_MS, true, runCheck);
print("[net-watchdog] HM-SHELLY-PREP-V2 loaded -- checking every " + JSON.stringify(CHECK_INTERVAL_MS / 1000) +
      "s, trigger at " + JSON.stringify(FAIL_CHECKS_TRIGGER) + " consecutive both-fail checks, flap guard " +
      JSON.stringify(FLAP_GUARD_CHECKS) + " checks (~" + JSON.stringify(FLAP_GUARD_CHECKS / 60 * CHECK_INTERVAL_MS / 1000) + "min)");
