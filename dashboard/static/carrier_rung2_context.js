/* ============================================================================
   CARRIER RUNG 2 — CONTEXT-ON-ARRIVAL  ·  bridge-v2 drop-in module
   Pairs with Rung 1 (commit 21e8279). Call CarrierContext.load(sym, src, mountEl)
   from inside openContact() AFTER the CONTACT card is shown.

   DOCTRINE (hard, enforced in code):
   - READ-ONLY. Every slot hits an existing GET endpoint. No writes.
   - COLORBLIND: amber (notable) / blue (baseline) + numbers always shown.
   - FAIL-SOFT: dead/slow/empty slot shows "no data". 4s per-slot timeout.
   - FRESHNESS: every value carries an age where the endpoint provides one.

   *** CONTAMINATION GUARD ***
   fwd_return_1d is currently the scanner's PROJECTED target, not realized return.
   Until the realized-return rewire (P1) lands, this module MUST NOT surface any
   fwd_return/edge/alpha number. Fleet Read is STUBBED for this reason.
   assertNoEdge() is the in-code backstop — leave it in.
   ============================================================================ */
(function (global) {
  "use strict";

  var TIMEOUT_MS = 4000;
  var CACHE_MS   = 30000;
  var _cache     = {};

  // ---- contamination guard ----
  var EDGE_KEYS = /fwd_return|avg_fwd|edge|alpha|projected_return/i;
  function assertNoEdge(label, value) {
    if (EDGE_KEYS.test(label) || (typeof value === "string" && EDGE_KEYS.test(value))) {
      return "— (edge metric withheld: realized-return rewire pending)";
    }
    return value;
  }

  function ageStr(tsLike) {
    if (!tsLike) return "";
    var t = new Date(tsLike).getTime();
    if (isNaN(t)) return "";
    var m = Math.max(0, Math.round((Date.now() - t) / 60000));
    if (m > 60 * 24) return Math.floor(m / 60 / 24) + "d";
    if (m > 60)      return Math.floor(m / 60) + "h";
    return m + "m";
  }

  function getJSON(url) {
    var ctrl = new AbortController();
    var to   = setTimeout(function () { ctrl.abort(); }, TIMEOUT_MS);
    return fetch(url, { headers: { Accept: "application/json" }, signal: ctrl.signal })
      .then(function (r) {
        clearTimeout(to);
        if (!r.ok) throw new Error(r.status);
        return r.json();
      })
      .catch(function (e) { clearTimeout(to); throw e; });
  }

  function num(v, digits) {
    return (v == null || isNaN(+v)) ? null : Number(v).toFixed(digits == null ? 2 : digits);
  }
  function tone(notable) { return notable ? "amber" : "blue"; }

  // ---- SLOT FETCHERS (adapted to bigmac's real JSON shapes) ----

  function slotPrice(sym) {
    // /api/price/{sym} → {symbol, price, change, change_pct, prev_close}
    return getJSON("/api/price/" + encodeURIComponent(sym)).then(function (d) {
      var last = num(d.price, 2);
      var chg  = num(d.change_pct, 2);
      if (last == null) return { label: "Last / Mark", empty: true };
      var notable = chg != null && Math.abs(+chg) >= 2.0;
      var v = "$" + last + (chg != null ? "  (" + chg + "%)" : "");
      return { label: "Last / Mark", value: v, tone: tone(notable), age: "" };
    });
  }

  function slotFlow(sym) {
    // /api/market/flow-lean → {current:{lean,net_flow_m,per_symbol:[{symbol,call_premium,put_premium,call_vol,put_vol,...}],recorded_at}}
    return getJSON("/api/market/flow-lean").then(function (d) {
      var cur  = d.current || {};
      var rows = cur.per_symbol || [];
      var row  = rows.find ? rows.find(function (r) { return r.symbol === sym; }) : null;
      if (!row) {
        // No per-sym data — show market-wide lean as context
        if (!cur.lean) return { label: "Options Flow", empty: true };
        var netM = num(cur.net_flow_m, 1);
        var notable = cur.conviction != null && cur.conviction >= 15;
        return {
          label: "Options Flow",
          value: cur.lean + (netM != null ? "  $" + netM + "M net (market)" : ""),
          tone: tone(notable),
          age: ageStr(cur.recorded_at)
        };
      }
      var pc   = (row.put_vol && row.call_vol) ? num(row.put_vol / row.call_vol, 2) : null;
      var netP = (row.call_premium && row.put_premium) ? num((row.call_premium - row.put_premium) / 1e6, 1) : null;
      var notable = pc != null && (pc >= 1.3 || pc <= 0.6);
      var v = (netP != null ? "$" + netP + "M net" : "") + (pc != null ? "  P/C " + pc : "");
      return { label: "Options Flow", value: v || "—", tone: tone(notable), age: ageStr(cur.recorded_at) };
    });
  }

  function slotCongress(sym) {
    // /api/congress/top-buys → {top_buys: [{ticker, buy_count, politicians, signal_strength}]}
    return getJSON("/api/congress/top-buys").then(function (d) {
      var rows = (d.top_buys || []);
      var row  = rows.find ? rows.find(function (r) { return r.ticker === sym; }) : null;
      if (!row) return { label: "Congress / Insider", empty: true };
      var pols    = (row.politicians || []).slice(0, 2).join(", ");
      var str     = (row.signal_strength || "").toUpperCase();
      var notable = /STRONG|MODERATE/.test(str);
      var v = "BUY · " + str + (pols ? " · " + pols : "");
      return { label: "Congress / Insider", value: v, tone: tone(notable), age: "" };
    });
  }

  function slotGEX(sym) {
    // /api/gex-snapshot → {data: {SYM: {spot, total_gex, gamma_flip, regime, asof, ...}}}
    return getJSON("/api/gex-snapshot").then(function (d) {
      var row = (d.data || {})[sym];
      if (!row) return { label: "Gamma / GEX", empty: true };
      var gex  = num(row.total_gex != null ? row.total_gex / 1e9 : null, 1);
      var flip = num(row.gamma_flip, 2);
      var spot = row.spot != null ? +row.spot : null;
      var notable = flip != null && spot != null && Math.abs(spot - +flip) / spot < 0.01;
      var regime  = (row.regime || "").replace("·", "·").slice(0, 30);
      var v = (gex != null ? gex + "B GEX" : "") +
              (flip != null ? "  flip $" + flip : "") +
              (regime ? "  " + regime : "");
      return { label: "Gamma / GEX", value: v || "—", tone: tone(notable), age: ageStr(row.asof) };
    });
  }

  function slotTape(sym) {
    // /api/news/{sym} → [{headline, summary, source, url, symbol}, ...]
    return getJSON("/api/news/" + encodeURIComponent(sym)).then(function (d) {
      var rows = Array.isArray(d) ? d : [];
      if (!rows.length) return { label: "Live Tape", empty: true };
      var head = (rows[0].headline || rows[0].title || "");
      if (!head) return { label: "Live Tape", empty: true };
      var clip = head.length > 72 ? head.slice(0, 69) + "…" : head;
      return { label: "Live Tape", value: clip, tone: "blue", age: "" };
    });
  }

  // Fleet Read — STUBBED until realized-return rewire (P1) lands
  function slotFleetRead(/* sym */) {
    return Promise.resolve({
      label: "Fleet Read",
      value: "pending realized-return rewire (P1)",
      tone: "blue",
      stub: true
    });
  }

  // Per-source emphasis: reorder slots to the contact's nature
  var EMPHASIS = {
    bk_avwap:     ["price", "flow", "gex", "congress", "tape"],
    bk_orb:       ["price", "flow", "gex", "congress", "tape"],
    bk_box:       ["price", "flow", "gex", "congress", "tape"],
    uhura:        ["tape", "price", "flow", "congress", "gex"],
    congress:     ["congress", "price", "flow", "tape", "gex"],
    insider:      ["congress", "price", "flow", "tape", "gex"],
    options_flow: ["flow", "gex", "price", "congress", "tape"],
    volatility:   ["flow", "gex", "price", "tape", "congress"]
  };
  var FETCH        = { price: slotPrice, flow: slotFlow, congress: slotCongress, gex: slotGEX, tape: slotTape };
  var DEFAULT_ORDER = ["price", "flow", "congress", "gex", "tape"];

  function renderSlot(s) {
    var el      = document.createElement("div");
    el.className = "ctx-slot";
    var label   = s.label || "—";
    var val     = s.empty ? "no data" : assertNoEdge(label, s.value);
    var toneCls = s.empty ? "faint" : (s.tone === "amber" ? "amber" : "blue");
    var age     = s.age ? '<span class="ctx-age">' + s.age + "</span>" : "";
    el.innerHTML =
      '<span class="ctx-lbl">' + label + "</span>" +
      '<span class="ctx-val ' + toneCls + '">' + val + age + "</span>";
    return el;
  }

  function paint(mountEl, slots) {
    mountEl.innerHTML = "";
    slots.order.forEach(function (k) {
      mountEl.appendChild(renderSlot(slots.results[k] || { label: k, empty: true }));
    });
    slotFleetRead().then(function (s) { mountEl.appendChild(renderSlot(s)); });
  }

  function load(sym, src, mountEl) {
    if (!mountEl) return;
    var c = _cache[sym];
    if (c && Date.now() - c.t < CACHE_MS) { paint(mountEl, c.slots); return; }

    var order = EMPHASIS[(src || "").toLowerCase()] || DEFAULT_ORDER;
    mountEl.innerHTML = "";
    var placeholders = {};
    order.forEach(function (k) {
      var ph       = document.createElement("div");
      ph.className = "ctx-slot loading";
      ph.innerHTML = '<span class="ctx-lbl">' + k + '</span><span class="ctx-val faint">loading…</span>';
      mountEl.appendChild(ph);
      placeholders[k] = ph;
    });
    // Fleet Read last — always stubbed
    var fr = document.createElement("div");
    mountEl.appendChild(fr);
    slotFleetRead(sym).then(function (s) { fr.replaceWith(renderSlot(s)); });

    var results = {};
    order.forEach(function (k) {
      FETCH[k](sym)
        .then(function (s) {
          results[k] = s;
          placeholders[k].replaceWith(renderSlot(s));
        })
        .catch(function () {
          var s = { label: k, empty: true };
          results[k] = s;
          placeholders[k].replaceWith(renderSlot(s));
        });
    });

    setTimeout(function () {
      _cache[sym] = { t: Date.now(), slots: { order: order, results: results } };
    }, TIMEOUT_MS + 200);
  }

  global.CarrierContext = { load: load };
})(window);
