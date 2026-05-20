/**
 * lcars.js — LCARS Star Trek Interface Shell
 * Injects: elbow corner piece, section label bar, mobile bottom tab bar.
 * Does NOT touch any existing element IDs, API calls, or WebSocket connections.
 */
(function () {
  'use strict';

  /* ── CONFIG ─────────────────────────────────────────── */
  var TABS = [
    { icon: '🖥️', label: 'BRIDGE',   section: 'dashboard',    fn: null },
    { icon: '🏆', label: 'FLEET',    section: 'leaderboard',  fn: null },
    { icon: '🔬', label: 'AGENTS',   section: 'agent-deepdive', fn: function() { if (typeof ddAutoLoad === 'function') ddAutoLoad(); } },
    { icon: '📡', label: 'SECTORS',  section: 'sector-watch', fn: null },
    { icon: '👨‍✈️', label: 'CAPTAIN', section: 'webull',       fn: null },
  ];

  /* Maps section name → user-facing LCARS panel title + subtitle */
  var SECTION_LABELS = {
    'dashboard':     { title: 'MAIN BRIDGE',      sub: 'BRIDGE STATUS & TACTICAL OVERVIEW' },
    'ready-room':    { title: 'READY ROOM',        sub: 'ADMIRAL\'S DAILY BRIEFING' },
    'ollie':         { title: 'OLLIE COMMANDER',   sub: 'FLEET EXECUTIVE OFFICER' },
    'feed':          { title: 'CREW ACTIVITY',     sub: 'LIVE AGENT DECISION FEED' },
    'dayblade':      { title: 'BATTLE STATION',    sub: '0DTE SCALPING OPS' },
    'charts':        { title: 'CHARTS',            sub: 'TECHNICAL ANALYSIS' },
    'livechart':     { title: 'LIVE CHART',        sub: 'REAL-TIME PRICE ACTION' },
    'screener-pro':  { title: 'SCREENER PRO',      sub: 'SIGNAL DISCOVERY ENGINE' },
    'sector-watch':  { title: 'SECTOR WATCH',      sub: 'MARKET SECTOR INTELLIGENCE' },
    'big-charts':    { title: 'BIG CHARTS',        sub: 'FULL-SCREEN VIEW' },
    'alert-history': { title: 'ALERTS',            sub: 'SIGNAL HISTORY LOG' },
    'live-scanner':  { title: 'LIVE SCANNER',      sub: 'REAL-TIME SIGNAL SCAN' },
    'ghost-scorecard':{ title: 'GHOST SCORECARD',  sub: 'PAPER TRADE PERFORMANCE' },
    'leaderboard':   { title: 'FLEET STATUS',      sub: 'AGENT PERFORMANCE LEADERBOARD' },
    'agent-deepdive':{ title: 'AGENTS',            sub: 'DEEP DIVE ANALYSIS' },
    'metals':        { title: 'DILITHIUM',         sub: 'PRECIOUS METALS & COMMODITIES' },
    'congress':      { title: 'CONGRESS',          sub: 'CAPITOL TRADES TRACKER' },
    'webull':        { title: 'CAPTAIN\'S PORTFOLIO', sub: 'WEBULL BENCHMARK POSITIONS' },
    'model-control': { title: 'MODELS',            sub: 'AI ENGINE CONFIGURATION' },
    'fear-greed':    { title: 'FEAR & GREED',      sub: 'MARKET SENTIMENT INDEX' },
    'heatmap':       { title: 'SECTORS',           sub: 'SECTOR HEATMAP' },
    'macro':         { title: 'MACRO',             sub: 'GLOBAL ECONOMIC DATA' },
    'news':          { title: 'NEWS',              sub: 'MARKET INTELLIGENCE' },
    'costs':         { title: 'COSTS',             sub: 'OPERATING EXPENDITURE' },
    'navigator':     { title: 'NAVIGATOR',         sub: 'CHEKOV SIGNAL SCANNER' },
    'war-room':      { title: 'WAR ROOM',          sub: 'STRATEGIC OPS CENTER' },
    'holodeck':      { title: 'HOLODECK',          sub: 'SIMULATION ENVIRONMENT' },
    'xo-room':       { title: 'XO ROOM',           sub: 'EXECUTIVE OFFICER BRIEFING' },
    'trades':        { title: 'TRADE LOG',         sub: 'EXECUTION HISTORY' },
    'chat':          { title: 'CHAT',              sub: 'AI INTERFACE' },
    'chart':         { title: 'CHART',             sub: 'PRICE ANALYSIS' },
  };

  /* ── ELBOW INJECTION ─────────────────────────────────── */
  function injectElbow() {
    if (document.getElementById('lcars-elbow')) return;
    var el = document.createElement('div');
    el.id = 'lcars-elbow';
    /* SVG elbow: orange block fills top-left of sidebar,
       with a curved cutout giving the LCARS "elbow" shape.
       Right edge extends into a horizontal top bar cap. */
    el.innerHTML = '<svg viewBox="0 0 220 88" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none">' +
      /* Full black background */
      '<rect width="220" height="88" fill="#000"/>' +
      /* Main orange block — fills sidebar width, leaves space for label column */
      '<rect x="0" y="0" width="148" height="56" fill="#FF9900" rx="0" ry="0"/>' +
      /* Curved cutout at bottom-right of orange block (the LCARS elbow curve) */
      '<path d="M 148 56 Q 148 72 164 72 L 220 72 L 220 56 Z" fill="#000"/>' +
      /* Right-side small orange pill strip at top */
      '<rect x="154" y="0" width="66" height="30" fill="#FF9900" rx="0"/>' +
      /* Right-side lilac strip below orange */
      '<rect x="154" y="34" width="66" height="22" fill="#CC99CC" rx="0"/>' +
      /* Narrow separator lines */
      '<rect x="148" y="0" width="4" height="30" fill="#000"/>' +
      '<rect x="148" y="34" width="4" height="22" fill="#000"/>' +
      /* "TradeMinds" text in elbow */
      '<text x="10" y="36" font-family="Antonio,sans-serif" font-size="16" font-weight="700" fill="#000" letter-spacing="2">TRADEMINDS</text>' +
      '<text x="10" y="50" font-family="Antonio,sans-serif" font-size="9" font-weight="400" fill="rgba(0,0,0,0.6)" letter-spacing="3">BRIDGE SYSTEM</text>' +
      '</svg>';
    document.body.insertBefore(el, document.body.firstChild);
  }

  /* ── SECTION LABEL BAR ───────────────────────────────── */
  function injectSectionBar() {
    if (document.getElementById('lcars-section-bar')) return;
    var bar = document.createElement('div');
    bar.id = 'lcars-section-bar';
    bar.innerHTML =
      '<div style="width:4px;height:28px;background:#FF9900;border-radius:0 2px 2px 0;flex-shrink:0;"></div>' +
      '<div>' +
        '<div id="lcars-section-title">MAIN BRIDGE</div>' +
        '<div id="lcars-section-subtitle">BRIDGE STATUS &amp; TACTICAL OVERVIEW</div>' +
      '</div>' +
      '<div style="margin-left:auto;font-family:\'Share Tech Mono\',monospace;font-size:11px;color:#CC99CC;" id="lcars-stardate"></div>';
    var main = document.querySelector('.main');
    if (main) main.insertBefore(bar, main.firstChild);
  }

  /* ── STARDATE CLOCK ──────────────────────────────────── */
  function updateStardate() {
    var el = document.getElementById('lcars-stardate');
    if (!el) return;
    var now = new Date();
    /* Rough stardate: year.dayOfYear + fractional time */
    var start = new Date(now.getFullYear(), 0, 0);
    var diff = now - start;
    var oneDay = 1000 * 60 * 60 * 24;
    var dayOfYear = Math.floor(diff / oneDay);
    var frac = Math.floor((now.getHours() * 60 + now.getMinutes()) / 14.4);
    var sd = (2600 + (now.getFullYear() - 2000) * 5 + Math.floor(dayOfYear / 73)) + '.' + (frac < 10 ? '0' : '') + frac;
    var realDate = now.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric', timeZone: 'America/Phoenix' });
    el.textContent = 'STARDATE ' + sd + ' · ' + realDate;
  }

  /* ── MOBILE BOTTOM TAB BAR ───────────────────────────── */
  function injectTabBar() {
    if (document.getElementById('lcars-tab-bar')) return;
    var bar = document.createElement('div');
    bar.id = 'lcars-tab-bar';
    TABS.forEach(function (t, i) {
      var btn = document.createElement('button');
      btn.className = 'lcars-tab';
      btn.dataset.section = t.section;
      btn.setAttribute('aria-label', t.label);
      btn.innerHTML = '<span class="tab-icon">' + t.icon + '</span>' +
                      '<span>' + t.label + '</span>';
      btn.addEventListener('click', function () {
        if (typeof showSection === 'function') {
          showSection(t.section);
          if (t.fn) t.fn();
        }
        setActiveTab(t.section);
        /* Close mobile sidebar if open */
        var sb = document.querySelector('.sidebar');
        var ov = document.getElementById('mobileOverlay');
        if (sb && sb.classList.contains('mobile-open')) {
          sb.classList.remove('mobile-open');
          if (ov) ov.classList.remove('active');
        }
      });
      bar.appendChild(btn);
    });
    document.body.appendChild(bar);
  }

  /* ── ACTIVE STATE MANAGEMENT ─────────────────────────── */
  function setActiveTab(sectionName) {
    var tabs = document.querySelectorAll('.lcars-tab');
    tabs.forEach(function (t) {
      t.classList.toggle('active', t.dataset.section === sectionName);
    });
  }

  function updateSectionLabel(sectionName) {
    var info = SECTION_LABELS[sectionName] || { title: sectionName.toUpperCase().replace(/-/g, ' '), sub: '' };
    var titleEl = document.getElementById('lcars-section-title');
    var subEl   = document.getElementById('lcars-section-subtitle');
    if (titleEl) titleEl.textContent = info.title;
    if (subEl)   subEl.textContent   = info.sub;
  }

  /* ── HOOK INTO showSection ───────────────────────────── */
  function hookShowSection() {
    /* Wait until showSection is defined (it's in a <script> block in index.html) */
    if (typeof showSection !== 'function') {
      setTimeout(hookShowSection, 100);
      return;
    }
    var _original = showSection;
    window.showSection = function (name) {
      _original.apply(this, arguments);
      setActiveTab(name);
      updateSectionLabel(name);
    };
  }

  /* ── INIT ────────────────────────────────────────────── */
  function init() {
    injectElbow();
    injectSectionBar();
    injectTabBar();
    updateStardate();
    setInterval(updateStardate, 30000);
    hookShowSection();

    /* Set initial active state for dashboard */
    setTimeout(function () {
      setActiveTab('dashboard');
      updateSectionLabel('dashboard');
    }, 200);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

}());
