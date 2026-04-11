/**
 * Ship's Computer - USS TradeMinds Audio Briefing
 * Docked inside the Ship's Computer chat panel.
 * Include via: <script src="/static/ship_computer.js"></script>
 */
(function () {
  'use strict';

  // ─── INJECT CSS ─────────────────────────────────────────────────────────────
  const style = document.createElement('style');
  style.textContent = `
    /* ── Keyframes ── */
    @keyframes sc-pulse {
      0%, 100% { box-shadow: 0 0 15px rgba(0,200,255,0.2), 0 0 30px rgba(0,200,255,0.1); }
      50%       { box-shadow: 0 0 30px rgba(0,200,255,0.5), 0 0 60px rgba(0,200,255,0.2); }
    }
    @keyframes sc-flicker {
      0%,100%  { opacity: 1;    }
      5%       { opacity: 0.85; }
      10%      { opacity: 1;    }
      50%      { opacity: 0.95; }
      55%      { opacity: 1;    }
      90%      { opacity: 0.9;  }
      95%      { opacity: 1;    }
    }
    @keyframes sc-glitch {
      0%, 98%, 100% { transform: none; }
      99%           { transform: translateX(2px) skewX(1deg); }
    }
    @keyframes sc-ring {
      0%   { transform: scale(0.3); opacity: 0.8; }
      100% { transform: scale(2.5); opacity: 0;   }
    }
    @keyframes sc-bar-anim {
      0%, 100% { height: 3px;  }
      50%      { height: 14px; }
    }
    @keyframes sc-fab-available {
      0%, 100% { box-shadow: 0 0 12px rgba(0,200,255,0.2); }
      50%      { box-shadow: 0 0 22px rgba(0,200,255,0.55), 0 0 40px rgba(0,200,255,0.2); }
    }

    /* ── Floating AUDIO Button ── */
    #sc-fab {
      position: fixed;
      bottom: 24px;
      right: 24px;
      z-index: 9999;
      width: 64px;
      height: auto;
      border-radius: 12px;
      background: rgba(10,14,23,0.95);
      border: 2px solid rgba(0,200,255,0.5);
      box-shadow: 0 0 12px rgba(0,200,255,0.3);
      cursor: pointer;
      animation: sc-fab-available 2.5s ease-in-out infinite;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-direction: column;
      gap: 4px;
      padding: 6px 4px 5px;
      outline: none;
      user-select: none;
      transition: transform 0.15s ease;
    }
    #sc-fab:hover  { transform: scale(1.08); }
    #sc-fab:active { transform: scale(0.96); }
    #sc-fab-label {
      font-size: 8px;
      font-family: monospace;
      letter-spacing: 1px;
      color: rgba(0,200,255,0.85);
      line-height: 1;
    }
    #sc-fab.sc-active {
      background: radial-gradient(circle, rgba(0,200,255,0.25) 0%, rgba(0,100,200,0.15) 100%);
      border-color: rgba(0,200,255,0.8);
      animation: sc-pulse 1.2s ease-in-out infinite;
    }

    /* ── Hologram Panel (docked inside chat) ── */
    #sc-holo-panel {
      display: none;
      flex-direction: column;
      background: linear-gradient(180deg, rgba(0,5,20,0.97) 0%, rgba(0,8,28,0.95) 100%);
      border-bottom: 1px solid rgba(0,200,255,0.2);
      padding: 10px 12px 8px;
      position: relative;
      flex-shrink: 0;
      overflow: hidden;
    }
    #sc-holo-panel::before {
      content: '';
      position: absolute;
      inset: 0;
      background: repeating-linear-gradient(
        0deg,
        transparent 0px, transparent 3px,
        rgba(0,200,255,0.025) 3px, rgba(0,200,255,0.025) 4px
      );
      pointer-events: none;
    }
    #sc-holo-panel-inner {
      display: flex;
      align-items: flex-start;
      gap: 12px;
      position: relative;
      z-index: 1;
    }

    /* ── Hologram Figure (compact 120px) ── */
    #sc-holo-figure {
      position: relative;
      width: 90px;
      height: 120px;
      flex-shrink: 0;
      animation: sc-flicker 4s infinite, sc-glitch 6s infinite;
      filter: drop-shadow(0 0 8px rgba(0,200,255,0.6)) drop-shadow(0 0 16px rgba(0,160,255,0.3));
      border-radius: 8px;
      overflow: hidden;
    }
    #sc-holo-img {
      display: block;
      width: 100%;
      height: 100%;
      object-fit: cover;
      object-position: center top;
      border-radius: 8px;
      filter: saturate(0.3) brightness(1.2) sepia(0.3) hue-rotate(160deg);
      opacity: 0.85;
    }
    #sc-holo-figure::before {
      content: '';
      position: absolute;
      inset: 0;
      background: rgba(0,160,255,0.15);
      border-radius: 8px;
      pointer-events: none;
      z-index: 1;
    }
    #sc-holo-figure::after {
      content: '';
      position: absolute;
      inset: 0;
      background: repeating-linear-gradient(
        0deg,
        transparent 0px, transparent 2px,
        rgba(0,200,255,0.05) 2px, rgba(0,200,255,0.05) 3px
      );
      pointer-events: none;
      border-radius: 8px;
      z-index: 2;
    }
    #sc-holo-figure.speaking {
      filter: drop-shadow(0 0 12px rgba(0,220,255,0.85)) drop-shadow(0 0 24px rgba(0,180,255,0.5));
    }

    /* ── Ring Container ── */
    #sc-ring-container {
      position: absolute;
      left: 50%; top: 50%;
      transform: translate(-50%, -50%);
      pointer-events: none;
      width: 0; height: 0;
    }
    .sc-ring-el {
      position: absolute;
      width: 90px; height: 90px;
      margin-left: -45px; margin-top: -45px;
      border-radius: 50%;
      border: 1.5px solid rgba(0,200,255,0.4);
      animation: sc-ring 1.8s ease-out infinite;
    }
    .sc-ring-el:nth-child(2) { animation-delay: 0.6s;  }
    .sc-ring-el:nth-child(3) { animation-delay: 1.2s;  }

    /* ── Right-side content ── */
    #sc-holo-content {
      flex: 1;
      min-width: 0;
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    #sc-header {
      font-family: monospace;
      font-size: 9px;
      letter-spacing: 2px;
      color: rgba(0,200,255,0.4);
      text-transform: uppercase;
    }

    /* ── Waveform ── */
    #sc-waveform {
      display: flex;
      align-items: flex-end;
      gap: 3px;
      height: 18px;
    }
    .sc-bar {
      width: 3px;
      height: 3px;
      border-radius: 2px;
      background: rgba(0,200,255,0.7);
      transition: height 0.15s ease;
    }
    #sc-holo-figure.speaking ~ * .sc-bar,
    .sc-waveform-active .sc-bar {
      animation: sc-bar-anim 0.6s ease-in-out infinite;
    }
    .sc-bar:nth-child(1) { animation-delay: 0.00s; }
    .sc-bar:nth-child(2) { animation-delay: 0.10s; }
    .sc-bar:nth-child(3) { animation-delay: 0.20s; }
    .sc-bar:nth-child(4) { animation-delay: 0.10s; }
    .sc-bar:nth-child(5) { animation-delay: 0.00s; }

    /* ── Text Display ── */
    #sc-text-display {
      font-family: monospace;
      font-size: 12px;
      color: rgba(0,200,255,0.9);
      line-height: 1.5;
      min-height: 36px;
      overflow: hidden;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
    }
    #sc-text-cursor {
      display: inline-block;
      width: 6px; height: 12px;
      background: rgba(0,200,255,0.8);
      vertical-align: middle;
      animation: sc-pulse 0.8s ease-in-out infinite;
      margin-left: 2px;
    }

    /* ── Status line ── */
    #sc-status {
      font-family: monospace;
      font-size: 9px;
      color: rgba(0,200,255,0.35);
      letter-spacing: 2px;
      min-height: 12px;
    }

    /* ── Loading ── */
    #sc-loading {
      font-family: monospace;
      font-size: 10px;
      color: rgba(0,200,255,0.5);
      letter-spacing: 2px;
    }
    .sc-dot-blink {
      animation: sc-pulse 1s ease-in-out infinite;
      display: inline-block;
    }

    /* ── Controls row ── */
    #sc-btn-row {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }
    .sc-btn {
      background: rgba(0,200,255,0.08);
      border: 1px solid rgba(0,200,255,0.3);
      color: rgba(0,200,255,0.85);
      padding: 4px 10px;
      border-radius: 12px;
      font-family: monospace;
      font-size: 10px;
      letter-spacing: 1px;
      cursor: pointer;
      transition: all 0.2s;
      white-space: nowrap;
    }
    .sc-btn:hover { background: rgba(0,200,255,0.18); border-color: rgba(0,200,255,0.6); }
    .sc-btn:disabled { opacity: 0.3; cursor: not-allowed; }

    /* ── Close button ── */
    #sc-close {
      position: absolute;
      top: 8px; right: 10px;
      background: rgba(255,0,0,0.1);
      border: 1px solid rgba(255,0,0,0.3);
      color: #ff4444;
      font-size: 24px;
      font-weight: bold;
      line-height: 1;
      min-width: 36px; min-height: 36px;
      display: flex; align-items: center; justify-content: center;
      border-radius: 6px;
      cursor: pointer;
      z-index: 2;
      padding: 0;
      transition: all 0.2s;
    }
    #sc-close:hover { background: rgba(255,0,0,0.2); border-color: rgba(255,0,0,0.6); }

    /* ── Nav Row ── */
    #sc-nav-row {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      margin-top: 6px;
      position: relative;
      z-index: 1;
    }
    .sc-nav-btn {
      background: rgba(255,120,0,0.08);
      border: 1px solid rgba(255,120,0,0.35);
      color: rgba(255,120,0,0.9);
      padding: 4px 10px;
      border-radius: 12px;
      font-family: monospace;
      font-size: 10px;
      letter-spacing: 1px;
      cursor: pointer;
      transition: all 0.2s;
      white-space: nowrap;
    }
    .sc-nav-btn:hover { background: rgba(255,120,0,0.18); border-color: rgba(255,120,0,0.7); }
    .sc-nav-btn.sc-hidden { display: none; }

    /* ── Chat input disabled state ── */
    #computer-chat-input.sc-briefing-active {
      opacity: 0.45;
      background: rgba(0,0,0,0.2) !important;
      cursor: not-allowed;
    }
  `;
  document.head.appendChild(style);

  // ─── STATE ───────────────────────────────────────────────────────────────────
  let panelInitialized = false;
  let advisoryData     = null;
  let speechSentences  = [];
  let currentSentIdx   = 0;
  let isPaused         = false;
  let isSpeaking       = false;
  let briefingDone     = false;
  let volumeLevel      = 0.8;
  let typewriterTimer  = null;
  let inBriefingMode   = false;

  // Edge-TTS audio element
  let audioEl      = null;
  let usingEdgeTTS = false;

  // DOM refs
  let holoPanel, holoFig, waveform, textDisplay, statusEl,
      btnPause, btnStop, btnRefresh, navRow, loadingEl, ringContainer;

  // sc-fab removed — floating Archer (#archer-float) in index.html serves as the entry point

  // ─── BUILD HOLO PANEL (lazy, injected into chat panel) ──────────────────────
  function buildHoloPanel() {
    if (panelInitialized) return;
    panelInitialized = true;

    holoPanel = document.createElement('div');
    holoPanel.id = 'sc-holo-panel';

    // Close button
    const closeBtn = document.createElement('button');
    closeBtn.id = 'sc-close';
    closeBtn.textContent = '✕';
    closeBtn.addEventListener('click', closeBriefing);
    holoPanel.appendChild(closeBtn);

    // Inner row (figure + content)
    const inner = document.createElement('div');
    inner.id = 'sc-holo-panel-inner';

    // Ring container
    ringContainer = document.createElement('div');
    ringContainer.id = 'sc-ring-container';
    for (let i = 0; i < 3; i++) {
      const ring = document.createElement('div');
      ring.className = 'sc-ring-el';
      ring.style.animationPlayState = 'paused';
      ringContainer.appendChild(ring);
    }

    // Hologram figure
    const figWrap = document.createElement('div');
    figWrap.style.position = 'relative';

    holoFig = document.createElement('div');
    holoFig.id = 'sc-holo-figure';

    const holoImg = document.createElement('img');
    holoImg.id  = 'sc-holo-img';
    holoImg.src = '/static/archer-portrait.png?v=4';
    holoImg.alt = 'Captain Archer — CIC';
    holoImg.draggable = false;
    holoFig.appendChild(holoImg);

    figWrap.appendChild(ringContainer);
    figWrap.appendChild(holoFig);
    inner.appendChild(figWrap);

    // Right-side content
    const content = document.createElement('div');
    content.id = 'sc-holo-content';

    const header = document.createElement('div');
    header.id = 'sc-header';
    header.textContent = "USS TRADEMINDS — CIC";
    content.appendChild(header);

    // Loading
    loadingEl = document.createElement('div');
    loadingEl.id = 'sc-loading';
    loadingEl.innerHTML = 'ACCESSING STARFLEET DATABASE<span class="sc-dot-blink">...</span>';
    loadingEl.style.display = 'none';
    content.appendChild(loadingEl);

    // Waveform
    waveform = document.createElement('div');
    waveform.id = 'sc-waveform';
    for (let i = 0; i < 5; i++) {
      const bar = document.createElement('div');
      bar.className = 'sc-bar';
      waveform.appendChild(bar);
    }
    content.appendChild(waveform);

    // Text display
    textDisplay = document.createElement('div');
    textDisplay.id = 'sc-text-display';
    content.appendChild(textDisplay);

    // Status
    statusEl = document.createElement('div');
    statusEl.id = 'sc-status';
    content.appendChild(statusEl);

    // Control buttons
    const btnRow = document.createElement('div');
    btnRow.id = 'sc-btn-row';

    btnPause = document.createElement('button');
    btnPause.className = 'sc-btn';
    btnPause.textContent = '⏸ PAUSE';
    btnPause.addEventListener('click', togglePause);

    btnStop = document.createElement('button');
    btnStop.className = 'sc-btn';
    btnStop.textContent = '⏹ STOP';
    btnStop.addEventListener('click', stopSpeech);

    btnRefresh = document.createElement('button');
    btnRefresh.className = 'sc-btn';
    btnRefresh.textContent = '🔄 REFRESH';
    btnRefresh.addEventListener('click', refreshAdvisory);

    btnRow.appendChild(btnPause);
    btnRow.appendChild(btnStop);
    btnRow.appendChild(btnRefresh);
    content.appendChild(btnRow);

    inner.appendChild(content);
    holoPanel.appendChild(inner);

    // Nav row
    navRow = document.createElement('div');
    navRow.id = 'sc-nav-row';
    navRow.classList.add('sc-hidden');

    const navDefs = [
      { label: '📊 BRIDGE',   action: function() { exitBriefingMode(); window.location.href = '/'; } },
      { label: '🔎 SCANNER',  action: function() { exitBriefingMode(); window.location.href = '/scanner'; } },
      { label: '⚡ NEW SCAN', action: newScan },
      { label: '🔄 REPEAT',   action: repeatBriefing },
    ];
    navDefs.forEach(function (def) {
      const btn = document.createElement('button');
      btn.className = 'sc-nav-btn';
      btn.textContent = def.label;
      btn.addEventListener('click', def.action);
      navRow.appendChild(btn);
    });
    holoPanel.appendChild(navRow);

    // ── Inject into chat panel ──────────────────────────────────────────────
    // Insert after the chat header, before messages
    const chatPanel = document.getElementById('computerChatPanel');
    const chatHeader = document.getElementById('computer-chat-header');
    if (chatPanel && chatHeader && chatHeader.nextSibling) {
      chatPanel.insertBefore(holoPanel, chatHeader.nextSibling);
    } else if (chatPanel) {
      chatPanel.insertBefore(holoPanel, chatPanel.firstChild);
    } else {
      // Fallback: append to body (shouldn't happen)
      document.body.appendChild(holoPanel);
    }

    // ── Chat input listener: typing exits briefing mode ─────────────────────
    const chatInput = document.getElementById('computer-chat-input');
    if (chatInput) {
      chatInput.addEventListener('focus', function() {
        if (inBriefingMode) {
          exitBriefingMode();
        }
      });
      chatInput.addEventListener('input', function() {
        if (inBriefingMode) {
          exitBriefingMode();
        }
      });
    }

    // Keyboard: Escape closes briefing
    document.addEventListener('keydown', function(e) {
      if (e.key === 'Escape' && inBriefingMode) {
        closeBriefing();
      }
      if ((e.key === ' ' || e.code === 'Space') && inBriefingMode &&
          document.activeElement !== document.getElementById('computer-chat-input')) {
        e.preventDefault();
        togglePause();
      }
    });
  }

  // ─── OPEN BRIEFING ───────────────────────────────────────────────────────────
  function openBriefing() {
    // Toggle off if already active
    if (inBriefingMode) { closeBriefing(); return; }

    // 1. Ensure chat panel is open (without focusing chat input)
    //    If chat is showing plain messages, close it first then reopen for hologram
    const chatPanel = document.getElementById('computerChatPanel');
    if (chatPanel && !chatPanel.classList.contains('open')) {
      chatPanel.classList.add('open');
      // Keep _computerChatOpen flag in sync with index.html
      if (typeof window._setChatOpenFlag === 'function') window._setChatOpenFlag(true);
    }

    // 2. Hide chat content — hologram takes the stage
    _hideChatContent();

    // 3. Build and show holo panel
    buildHoloPanel();
    if (holoPanel) holoPanel.style.display = 'flex';

    // 4. Enter briefing mode
    inBriefingMode = true;
    fab.classList.add('sc-active');

    // 5. Disable chat input (belt-and-suspenders — already hidden)
    setChatInputDisabled(true);

    // 6. Reset and start
    briefingDone = false;
    if (navRow) navRow.classList.add('sc-hidden');
    startAdvisory();
  }

  // ─── CLOSE / EXIT ────────────────────────────────────────────────────────────
  function closeBriefing() {
    stopSpeech();
    if (holoPanel) holoPanel.style.display = 'none';
    _showChatContent();
    inBriefingMode = false;
    fab.classList.remove('sc-active');
    setChatInputDisabled(false);
  }

  // Exit briefing mode without closing chat (e.g. user starts typing)
  function exitBriefingMode() {
    stopSpeech();
    if (holoPanel) holoPanel.style.display = 'none';
    _showChatContent();
    inBriefingMode = false;
    fab.classList.remove('sc-active');
    setChatInputDisabled(false);
  }

  // ─── SHOW / HIDE CHAT CONTENT (mutually exclusive with hologram) ─────────────
  function _showChatContent() {
    var msgs   = document.getElementById('computer-chat-messages');
    var inpRow = document.getElementById('computer-chat-input-row');
    if (msgs)   msgs.style.display   = '';
    if (inpRow) inpRow.style.display = '';
  }

  function _hideChatContent() {
    var msgs   = document.getElementById('computer-chat-messages');
    var inpRow = document.getElementById('computer-chat-input-row');
    if (msgs)   msgs.style.display   = 'none';
    if (inpRow) inpRow.style.display = 'none';
  }

  // ─── CHAT INPUT CONTROL ──────────────────────────────────────────────────────
  function setChatInputDisabled(disabled) {
    const inp = document.getElementById('computer-chat-input');
    const btn = document.getElementById('computer-chat-send');
    if (inp) {
      inp.disabled = disabled;
      inp.placeholder = disabled ? 'Briefing in progress...' : 'Ask the computer...';
      if (disabled) {
        inp.classList.add('sc-briefing-active');
      } else {
        inp.classList.remove('sc-briefing-active');
      }
    }
    if (btn) btn.disabled = disabled;
  }

  // ─── ADVISORY FETCH ──────────────────────────────────────────────────────────
  function startAdvisory() {
    clearTypewriter();
    stopAudio();
    if (textDisplay) textDisplay.innerHTML = '';
    if (statusEl)    statusEl.textContent  = '';
    setLoadingVisible(true);
    setSpeakingState(false);
    usingEdgeTTS = false;

    fetch('/api/ready-room/advisory')
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        advisoryData = data;
        setLoadingVisible(false);
        buildSentences(data);
        beginBriefing();
      })
      .catch(function (err) {
        setLoadingVisible(false);
        advisoryData = null;
        const fallback = "Unable to reach Starfleet Command. Advisory data unavailable. " +
                         "Standing by for manual orders, Captain.";
        speechSentences = [fallback];
        currentSentIdx  = 0;
        beginBriefing();
      });
  }

  function buildSentences(d) {
    const mr  = d.market_read  || {};
    const ap  = d.action_plan  || {};
    const wf  = Array.isArray(d.watch_for) ? d.watch_for : [];
    const wim = Array.isArray(d.what_it_means) ? d.what_it_means : [];

    // ── Helper: find a signal by group name ───────────────────────────────
    function sig(groupFragment) {
      return wim.find(function(s) {
        return (s.signal_group || '').toLowerCase().indexOf(groupFragment) !== -1;
      }) || {};
    }

    // ── Sentence 1: alert level + short condition ─────────────────────────
    var signal = (mr.signal || 'UNKNOWN').toUpperCase();
    var alertWord = signal === 'GO'          ? 'all clear'
                  : signal === 'CAUTION'     ? 'yellow alert'
                  : signal === 'STAND_DOWN'  ? 'red alert'
                  : signal === 'RED_ALERT'   ? 'red alert'
                  : 'status update';
    // Pull just the first clause of the headline (before em-dash or period), normalize case
    var rawHeadline = (mr.headline || 'market conditions are being assessed')
      .replace(/^[^\w]*/u, '')                    // strip leading emoji/symbols
      .replace(/\s*[—–]\s*.*/u, '')               // drop everything after first em-dash
      .replace(/\.\s*.*/u, '')                    // drop after first period
      .trim()
      .toLowerCase();
    // Capitalize first letter
    rawHeadline = rawHeadline.charAt(0).toUpperCase() + rawHeadline.slice(1);
    var s1 = 'Captain, ' + alertWord + '. ' + (rawHeadline || 'monitoring conditions') + '.';

    // ── Sentence 2: VIX ───────────────────────────────────────────────────
    var vixSig  = sig('vix');
    var vixRead = (vixSig.reading || '').match(/[\d.]+/);
    var vixNum  = vixRead ? parseFloat(vixRead[0]) : null;
    var vixMood = vixNum === null ? 'unknown'
                : vixNum < 18    ? 'calm'
                : vixNum < 25    ? 'nervous'
                : 'stormy';
    var s2 = vixNum !== null
      ? 'VIX at ' + vixNum + ', ' + vixMood + '.'
      : (vixSig.line1 ? vixSig.line1 + '.' : '');

    // ── Sentence 3: breadth ───────────────────────────────────────────────
    var breadthSig  = sig('breadth');
    var breadthRead = (breadthSig.reading || '').match(/(\d+)\/(\d+)/);
    var s3 = '';
    if (breadthRead) {
      var bNum = parseInt(breadthRead[1], 10);
      var bDen = parseInt(breadthRead[2], 10);
      var bWord = bNum >= 8 ? 'strong' : bNum >= 6 ? 'mixed' : bNum >= 4 ? 'selective' : 'weak';
      s3 = 'Breadth ' + bNum + ' of ' + bDen + ', ' + bWord + '.';
    } else if (breadthSig.line1) {
      s3 = breadthSig.line1 + '.';
    }

    // ── Sentence 4: most notable divergence or alert ─────────────────────
    // Scan all signals for the first non-empty line3_bullet or notable line1
    var s4 = '';
    for (var i = 0; i < wim.length; i++) {
      var bullets = wim[i].line3_bullets || [];
      if (bullets.length) {
        // Strip leading emoji/bullet chars and technical symbols
        var cleaned = bullets[0]
          .replace(/^[•⚠✓→\-\s]*/u, '')
          .replace(/\(P\/C[\d\s.]+\)/g, '')
          .trim();
        if (cleaned.length > 10) { s4 = cleaned + '.'; break; }
      }
    }
    // Fallback: use options or intermarket line1 if no bullet found
    if (!s4) {
      var optSig = sig('option');
      if (optSig.line1 && optSig.color !== 'green') s4 = optSig.line1 + '.';
    }

    // ── Sentence 5: posture + single action ──────────────────────────────
    var primary = (ap.primary || '').replace(/^[🟢🟡🔴]\s*/u, '');
    var postureWord = /green|GO/i.test(primary)     ? 'Green light'
                    : /selective|SELECTIVE/i.test(primary) ? 'Selective'
                    : /defense|DEFENSE/i.test(primary)     ? 'Defense mode'
                    : primary.split(/[\s—]/)[0];
    var details = Array.isArray(ap.details) ? ap.details : [];
    // First detail: strip the leading "→ " and truncate to first clause
    var actionStr = '';
    if (details.length) {
      actionStr = details[0]
        .replace(/^→\s*/u, '')
        .replace(/\s*[—–]\s*.*/u, '')
        .replace(/\.\s*.*/u, '')
        .trim();
    }
    var s5 = postureWord ? 'Posture: ' + postureWord + (actionStr ? '. ' + actionStr + '.' : '.') : '';

    // ── Sentence 6: key trigger ───────────────────────────────────────────
    var s6 = '';
    if (wf.length) {
      var trigger = wf[0]
        .replace(/^IF\s+/i, '')
        .replace(/\s*→\s*/, ' — ')
        .replace(/\.\s*.*/u, '')
        .trim();
      s6 = 'Watch for ' + trigger + '.';
    }

    // ── Sentence 7: sign-off ──────────────────────────────────────────────
    var s7 = 'Standing by for orders, Captain.';

    speechSentences = [s1, s2, s3, s4, s5, s6, s7]
      .filter(Boolean)
      .map(function(s) {
        return s.trim().replace(/\.\./g, '.').replace(/\s+/g, ' ');
      })
      .filter(function(s) { return s.length > 4; });

    currentSentIdx = 0;
  }

  // ─── BRIEFING ENTRY POINT ────────────────────────────────────────────────────
  function beginBriefing() {
    currentSentIdx = 0;
    isPaused       = false;
    briefingDone   = false;
    if (btnPause) btnPause.textContent = '⏸ PAUSE';
    if (statusEl) statusEl.textContent = 'GENERATING VOICE...';

    const fullText = speechSentences.join(' ');

    fetch('/api/ready-room/speak', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ text: fullText }),
    })
      .then(function (r) {
        if (!r.ok) throw new Error('speak HTTP ' + r.status);
        return r.json();
      })
      .then(function (data) {
        if (data.error) throw new Error(data.error);
        usingEdgeTTS = true;
        if (statusEl) statusEl.textContent = 'TRANSMITTING...';
        playEdgeTTS(data.audio_url + '?t=' + Date.now());
      })
      .catch(function (err) {
        console.warn('Edge TTS unavailable:', err);
        usingEdgeTTS = false;
        if (statusEl) statusEl.textContent = 'AUDIO OFFLINE';
        onBriefingComplete();
      });
  }

  // ─── EDGE-TTS PLAYBACK ───────────────────────────────────────────────────────
  function playEdgeTTS(url) {
    stopAudio();

    audioEl = new Audio(url);
    audioEl.volume = volumeLevel;
    audioEl.preload = 'auto';

    audioEl.addEventListener('timeupdate', onAudioTimeUpdate);
    audioEl.addEventListener('playing',   function () { setSpeakingState(true);  });
    audioEl.addEventListener('pause',     function () { setSpeakingState(false); });
    audioEl.addEventListener('ended',     function () { setSpeakingState(false); onBriefingComplete(); });
    audioEl.addEventListener('error',     function (e) {
      console.warn('Audio playback error:', e);
      usingEdgeTTS = false;
      setSpeakingState(false);
      if (statusEl) statusEl.textContent = 'AUDIO ERROR';
      onBriefingComplete();
    });

    showSentence(0);

    // Trigger Archer avatar lip sync
    try {
      document.dispatchEvent(new CustomEvent('archer-speaking', { detail: { audio: audioEl } }));
    } catch(e) {}

    audioEl.play().catch(function (e) {
      if (statusEl) statusEl.textContent = 'TAP RESUME TO PLAY';
      console.warn('Autoplay blocked:', e);
    });
  }

  let _lastSentShown = -1;
  function onAudioTimeUpdate() {
    if (!audioEl || !speechSentences.length) return;
    const dur = audioEl.duration;
    if (!dur || !isFinite(dur)) return;
    const fraction = audioEl.currentTime / dur;
    const idx = Math.min(
      Math.floor(fraction * speechSentences.length),
      speechSentences.length - 1
    );
    if (idx !== _lastSentShown) {
      _lastSentShown = idx;
      showSentence(idx);
    }
  }

  function showSentence(idx) {
    currentSentIdx = idx;
    clearTypewriter();
    if (speechSentences[idx]) {
      typewriterEffect(speechSentences[idx], null);
    }
  }

  function stopAudio() {
    if (audioEl) {
      audioEl.pause();
      audioEl.src = '';
      audioEl = null;
    }
    _lastSentShown = -1;
  }

  // ─── TYPEWRITER ──────────────────────────────────────────────────────────────
  function typewriterEffect(text, onDone) {
    clearTypewriter();
    if (!textDisplay) return;
    textDisplay.innerHTML = '';
    let i = 0;
    const cursor = document.createElement('span');
    cursor.id = 'sc-text-cursor';
    textDisplay.appendChild(cursor);

    function tick() {
      if (i < text.length) {
        cursor.insertAdjacentText('beforebegin', text[i]);
        i++;
        const delay = text[i - 1] === '.' || text[i - 1] === ',' ? 80 : 22;
        typewriterTimer = setTimeout(tick, delay);
      } else {
        cursor.remove();
        if (onDone) onDone();
      }
    }
    tick();
  }

  function clearTypewriter() {
    if (typewriterTimer) {
      clearTimeout(typewriterTimer);
      typewriterTimer = null;
    }
  }

  // ─── STATE HELPERS ────────────────────────────────────────────────────────────
  function setSpeakingState(speaking) {
    if (!holoFig) return;
    if (speaking) {
      holoFig.classList.add('speaking');
      setRings(true);
      setWaveBars(true);
    } else {
      holoFig.classList.remove('speaking');
      setRings(false);
      setWaveBars(false);
    }
  }

  function setRings(active) {
    if (!ringContainer) return;
    ringContainer.querySelectorAll('.sc-ring-el').forEach(function (r) {
      r.style.animationPlayState = active ? 'running' : 'paused';
    });
  }

  function setWaveBars(active) {
    if (!waveform) return;
    waveform.querySelectorAll('.sc-bar').forEach(function (b, idx) {
      b.style.animation = active
        ? `sc-bar-anim ${(0.4 + Math.random() * 0.4).toFixed(2)}s ease-in-out infinite ${(idx * 0.1).toFixed(2)}s`
        : 'none';
      b.style.height = active ? '' : '3px';
    });
  }

  function setLoadingVisible(show) {
    if (!loadingEl) return;
    loadingEl.style.display = show ? 'block' : 'none';
  }

  function onBriefingComplete() {
    briefingDone = true;
    setSpeakingState(false);
    isSpeaking = false;
    if (statusEl) statusEl.textContent = 'BRIEFING COMPLETE';
    if (navRow) navRow.classList.remove('sc-hidden');
    // Re-enable chat input so user can ask questions
    setChatInputDisabled(false);
    // Keep inBriefingMode = true so the holo panel stays visible
    // User exits by clicking nav button, typing, or ✕
  }

  // ─── CONTROLS ────────────────────────────────────────────────────────────────
  function togglePause() {
    if (!audioEl) return;
    if (isPaused) {
      isPaused = false;
      if (btnPause) btnPause.textContent = '⏸ PAUSE';
      audioEl.play();
      if (statusEl) statusEl.textContent = 'TRANSMITTING...';
    } else {
      isPaused = true;
      if (btnPause) btnPause.textContent = '▶ RESUME';
      audioEl.pause();
      if (statusEl) statusEl.textContent = 'PAUSED';
    }
  }

  function stopSpeech() {
    isPaused   = false;
    isSpeaking = false;
    clearTypewriter();
    stopAudio();
    setSpeakingState(false);
    if (statusEl)  statusEl.textContent  = 'STANDBY';
    if (btnPause)  btnPause.textContent  = '⏸ PAUSE';
  }

  function refreshAdvisory() {
    stopSpeech();
    advisoryData = null;
    if (navRow) navRow.classList.add('sc-hidden');
    briefingDone = false;
    setChatInputDisabled(true);
    startAdvisory();
  }

  function repeatBriefing() {
    stopSpeech();
    if (!advisoryData) { refreshAdvisory(); return; }
    currentSentIdx = 0;
    if (navRow) navRow.classList.add('sc-hidden');
    briefingDone = false;
    setChatInputDisabled(true);
    beginBriefing();
  }

  function newScan() {
    stopSpeech();
    if (statusEl) statusEl.textContent = 'INITIATING SCAN...';
    fetch('/api/ready-room/run', { method: 'POST' })
      .then(function () { window.location.reload(); })
      .catch(function () { window.location.reload(); });
  }

  // ─── AUTO-BRIEF MODE ─────────────────────────────────────────────────────────
  if (localStorage.getItem('sc_auto_brief') === 'true') {
    setTimeout(function () { openBriefing(); }, 1500);
  }

  // ─── EXPOSE PUBLIC API ───────────────────────────────────────────────────────
  window.shipComputer = {
    open:  openBriefing,
    close: closeBriefing,
    stop:  stopSpeech,
    isActive: function () { return inBriefingMode; },
    /** Stop hologram and reveal chat content — called by toggleComputerChat */
    stopAndShowChat: function () {
      stopSpeech();
      if (holoPanel) holoPanel.style.display = 'none';
      _showChatContent();
      inBriefingMode = false;
      fab.classList.remove('sc-active');
      setChatInputDisabled(false);
    },
  };

  // ─── EXTERNAL STOP EVENT (from index.html stopArcherNow()) ──────────────────
  document.addEventListener('archer-stop-requested', function () {
    stopSpeech();
    if (holoPanel) holoPanel.style.display = 'none';
    _showChatContent();
    inBriefingMode = false;
    if (fab) fab.classList.remove('sc-active');
    setChatInputDisabled(false);
  });

})();
