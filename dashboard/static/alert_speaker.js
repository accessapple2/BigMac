/**
 * alert_speaker.js — Captain Archer voice announcement system
 * Self-contained IIFE. Injects nav toggle, polls 7 event sources, queues TTS via edge-tts.
 * No browser speechSynthesis fallback. Silence on any failure.
 */
(function () {
  'use strict';

  // ── Config ────────────────────────────────────────────────────────────────
  var RATE_LIMIT_MS   = 60000;   // min gap between any two announcements
  var QUEUE_GAP_MS    = 1200;    // gap between queued items
  var MAX_QUEUE       = 3;       // drop oldest if queue overflows
  var SPEAK_TIMEOUT   = 5000;    // abort fetch after this many ms

  var NEWS_SECTOR_MAP = {
    'tariff':        ['XLI','XLB','AAPL','TSLA','CAT','DE'],
    'trade':         ['XLI','XLB','AAPL','TSLA','CAT','DE'],
    'fed':           ['XLF','TLT','JPM','BAC','GS'],
    'rate':          ['XLF','TLT','JPM','BAC','GS'],
    'oil':           ['XLE','CVX','XOM','USO','OXY'],
    'energy':        ['XLE','CVX','XOM','USO','OXY'],
    'inflation':     ['GLD','TLT','XLP','KO','PG'],
    'cpi':           ['GLD','TLT','XLP','KO','PG'],
    'tech':          ['XLK','NVDA','AAPL','MSFT','GOOGL','QQQ'],
    'china':         ['BABA','FXI','TSM','NVDA','AAPL'],
    'ai':            ['NVDA','AMD','AVGO','MSFT','GOOGL','SMH'],
    'semiconductor': ['NVDA','AMD','AVGO','TSM','SMH','INTC'],
    'crypto':        ['COIN','MSTR','MARA'],
    'defense':       ['LMT','RTX','NOC','GD'],
    'pharma':        ['XLV','JNJ','PFE','LLY','UNH'],
    'housing':       ['XHB','DHI','LEN','HD','LOW'],
    'banks':         ['XLF','JPM','BAC','GS','MS','C'],
    'retail':        ['XRT','AMZN','WMT','TGT','COST'],
  };

  // ── State ────────────────────────────────────────────────────────────────
  var _enabled  = localStorage.getItem('archer_voice_alerts') === 'true';
  var _volume   = parseFloat(localStorage.getItem('archer_alert_volume') || '0.8');
  var _queue    = [];
  var _playing  = false;
  var _lastSpeak = 0;
  var _seen     = {};   // key → true (permanent dedup within session)

  // Per-poller state
  var _lastTradeId    = null;
  var _lastCondition  = null;
  var _lastBridgeKey  = null;
  var _lastKirkKey    = null;
  var _lastAlertId    = null;
  var _lastNewsKey    = null;
  var _lastNewsTime   = 0;      // epoch ms of last news announcement
  var _seenEvents     = {};     // event_id → 'approaching' | 'cleared'
  var _allPositions   = null;   // cached [{player_name, symbol}] from last fleet fetch

  // ── Nav UI injection ─────────────────────────────────────────────────────
  function _injectNav() {
    var navRight = document.querySelector('.nav-right');
    if (!navRight) { setTimeout(_injectNav, 500); return; }

    var wrapper = document.createElement('div');
    wrapper.id  = 'archerAudioControls';
    wrapper.style.cssText = 'display:inline-flex;align-items:center;gap:4px;margin-right:4px;';

    var btn = document.createElement('button');
    btn.id        = 'archerVoiceBtn';
    btn.className = 'nav-icon-btn';
    btn.title     = 'Captain Archer voice alerts';
    btn.textContent = _enabled ? '🔊' : '🔇';
    btn.onclick   = function () { ArcherAudio.toggle(); };

    var slider = document.createElement('input');
    slider.id    = 'archerVolSlider';
    slider.type  = 'range';
    slider.min   = '0';
    slider.max   = '1';
    slider.step  = '0.05';
    slider.value = String(_volume);
    slider.title = 'Voice volume';
    slider.style.cssText = 'width:52px;cursor:pointer;vertical-align:middle;accent-color:#38bdf8;';
    slider.oninput = function () { ArcherAudio.setVolume(parseFloat(this.value)); };

    wrapper.appendChild(btn);
    wrapper.appendChild(slider);

    var sep = navRight.querySelector('.nav-sep');
    if (sep) {
      navRight.insertBefore(wrapper, sep);
    } else {
      navRight.insertBefore(wrapper, navRight.lastElementChild);
    }
  }

  function _updateBtn() {
    var btn = document.getElementById('archerVoiceBtn');
    if (btn) btn.textContent = _enabled ? '🔊' : '🔇';
  }

  // ── TTS core ─────────────────────────────────────────────────────────────
  function _truncate15(text) {
    return text.trim().split(/\s+/).slice(0, 15).join(' ');
  }

  function _playNext() {
    if (_playing || _queue.length === 0) return;
    if (Date.now() - _lastSpeak < RATE_LIMIT_MS) {
      _queue = [];   // rate-limited — drop stale queue
      return;
    }
    _playing = true;
    var item = _queue.shift();

    var ctrl = new AbortController();
    var tid  = setTimeout(function () { ctrl.abort(); }, SPEAK_TIMEOUT);

    fetch('/api/ready-room/speak', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ text: item.text }),
      signal:  ctrl.signal
    })
    .then(function (r) {
      clearTimeout(tid);
      if (!r.ok) throw new Error('speak ' + r.status);
      return r.json();
    })
    .then(function (d) {
      if (!d.audio_url) throw new Error('no audio_url');
      var audio = new Audio(d.audio_url + '?t=' + Date.now());
      audio.volume = _volume;
      audio.onended = function () {
        _lastSpeak = Date.now();
        _playing   = false;
        setTimeout(_playNext, QUEUE_GAP_MS);
      };
      audio.onerror = function () {
        _playing = false;
        setTimeout(_playNext, QUEUE_GAP_MS);
      };
      audio.play().catch(function () { _playing = false; });
    })
    .catch(function () {
      clearTimeout(tid);
      _playing = false;
    });
  }

  function _enqueue(text, key) {
    if (!_enabled) return;
    if (_seen[key]) return;
    _seen[key] = true;

    text = _truncate15(text);
    if (_queue.length >= MAX_QUEUE) {
      // Combine overflow: replace last item with combined summary
      _queue = [{ text: 'Multiple alerts. Check board.', key: 'combined:' + Date.now() }];
      _seen['combined:' + Date.now()] = true;
    }
    _queue.push({ text: text, key: key });
    _playNext();
  }

  // ── Save note to Archer memory ────────────────────────────────────────────
  function _saveNote(message) {
    fetch('/api/computer/note', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ role: 'assistant', message: message })
    }).catch(function () {});
  }

  // ── Fetch all positions for news cross-reference ──────────────────────────
  function _refreshPositions() {
    fetch('/api/fleet/positions')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        var list = Array.isArray(d) ? d : (d.positions || d.data || []);
        _allPositions = list;
      })
      .catch(function () {});
    // Also try alternate endpoint if needed
    if (!_allPositions) {
      fetch('/api/alpaca/positions')
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (d) {
          if (!d || _allPositions) return;
          var list = Array.isArray(d) ? d : (d.positions || d.data || []);
          _allPositions = list;
        })
        .catch(function () {});
    }
  }

  function _positionsForSymbols(syms) {
    if (!_allPositions || !_allPositions.length) return [];
    var symSet = {};
    syms.forEach(function (s) { symSet[s.toUpperCase()] = true; });
    var hits = [];
    _allPositions.forEach(function (p) {
      var sym = (p.symbol || p.ticker || '').toUpperCase();
      if (symSet[sym]) {
        var name = p.player_name || p.player_id || p.name || 'Agent';
        // Shorten long names to first name only
        name = name.split(/[\s,]/)[0];
        hits.push({ name: name, symbol: sym });
      }
    });
    return hits;
  }

  // ── Pollers ───────────────────────────────────────────────────────────────

  // a) Trade executed (10s)
  function _pollTrades() {
    fetch('/api/alerts/recent?limit=1')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d || !d.length) return;
        var t  = d[0];
        var id = t.id || t.trade_id || t.timestamp || '';
        if (!id || id === _lastTradeId) return;
        _lastTradeId = id;
        var player = (t.player_name || t.player_id || 'Agent').split(/[\s,]/)[0];
        var action = (t.action || 'traded').toUpperCase();
        var verb   = action.indexOf('BUY') !== -1 ? 'bought' : action === 'SELL' ? 'sold' : 'traded';
        var sym    = (t.symbol || '').toUpperCase();
        var price  = t.price ? ' at ' + parseFloat(t.price).toFixed(0) : '';
        _enqueue(player + ' ' + verb + ' ' + sym + price + '.', 'trade:' + id);
      })
      .catch(function () {});
    setTimeout(_pollTrades, 10000);
  }

  // b) Red Alert — condition change (60s)
  function _pollCondition() {
    fetch('/api/ready-room/condition')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        var cond = (d.condition || d.label || '').toUpperCase();
        if (!cond || cond === _lastCondition) return;
        var prev = _lastCondition;
        _lastCondition = cond;
        if (prev === null) return;  // first load — no announce
        var label = cond === 'GREEN' ? 'all clear'
                  : cond === 'YELLOW' ? 'yellow alert'
                  : cond === 'RED' ? 'red alert' : cond.toLowerCase();
        _enqueue('Condition changed. ' + label + '.', 'cond:' + cond + ':' + Date.now());
      })
      .catch(function () {});
    setTimeout(_pollCondition, 60000);
  }

  // b2) Red Alert — fired alert events (30s)
  function _pollAlerts() {
    fetch('/api/ready-room/alerts')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        var alerts = Array.isArray(d) ? d : (d.alerts || []);
        if (!alerts.length) return;
        var top = alerts[0];
        var id  = top.id || top.alert_id || (top.title + ':' + (top.fired_at || ''));
        if (!id || id === _lastAlertId) return;
        _lastAlertId = id;
        var title = (top.title || top.alert_type || 'Market alert').replace(/_/g, ' ').toLowerCase();
        var sev   = (top.severity || '').toUpperCase();
        var prefix = sev === 'CRITICAL' || sev === 'HIGH' ? 'Captain, red alert. ' : 'Alert. ';
        _enqueue(prefix + title + '.', 'alert:' + id);
      })
      .catch(function () {});
    setTimeout(_pollAlerts, 30000);
  }

  // c) Bridge vote (5min)
  function _pollBridge() {
    fetch('/api/bridge/consensus')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        var vote  = (d.consensus_vote || '').toUpperCase();
        var buys  = d.buy_votes  || 0;
        var sells = d.sell_votes || 0;
        var total = d.total_voters || 1;
        var key   = vote + ':' + buys + ':' + sells;
        if (!vote || key === _lastBridgeKey) return;
        _lastBridgeKey = key;
        var pct = Math.round((Math.max(buys, sells) / total) * 100);
        var dir = vote === 'BUY' ? 'buy' : vote === 'SELL' ? 'sell' : 'hold';
        var cnt = Math.max(buys, sells);
        _enqueue('Bridge vote complete. Crew recommends ' + dir + '. ' + cnt + ' of ' + total + ' in agreement.', 'bridge:' + key);
      })
      .catch(function () {});
    setTimeout(_pollBridge, 300000);
  }

  // d) Kirk advisory (2min)
  function _pollKirk() {
    fetch('/api/kirk/advisory')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        var fresh = d.generated_at || d.timestamp || '';
        if (!fresh || fresh === _lastKirkKey) return;
        _lastKirkKey = fresh;
        var positions = d.positions || [];
        if (!positions.length) return;
        var top    = positions[0];
        var action = (top.action || 'review').toLowerCase();
        var sym    = (top.symbol || '').toUpperCase();
        var pnl    = typeof top.pnl_pct === 'number'
          ? ' ' + (top.pnl_pct >= 0 ? 'up' : 'down') + ' ' + Math.abs(top.pnl_pct).toFixed(1) + ' percent'
          : '';
        _enqueue('Captain, Kirk advisory. ' + action + ' ' + sym + pnl + '.', 'kirk:' + fresh);
      })
      .catch(function () {});
    setTimeout(_pollKirk, 120000);
  }

  // f) News alerts (15min) — smart contextual with sector + position mapping
  function _pollNews() {
    fetch('/api/ready-room/news-pulse')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;

        // Build dedup key from top themes + mood direction
        var themes = d.top_themes || [];
        var mood   = d.signal || d.mood_score || '';
        var newsKey = themes.slice(0, 2).join('|') + ':' + mood;

        var now = Date.now();
        if (newsKey === _lastNewsKey) return;
        if (now - _lastNewsTime < 900000) return;  // max 1/15min

        _lastNewsKey  = newsKey;
        _lastNewsTime = now;

        // Find affected tickers from top theme
        var affectedSyms = [];
        themes.forEach(function (theme) {
          var t = theme.toLowerCase();
          Object.keys(NEWS_SECTOR_MAP).forEach(function (keyword) {
            if (t.indexOf(keyword) !== -1) {
              NEWS_SECTOR_MAP[keyword].forEach(function (sym) {
                if (affectedSyms.indexOf(sym) === -1) affectedSyms.push(sym);
              });
            }
          });
        });

        // Cross-reference with all positions
        var hits = _positionsForSymbols(affectedSyms);
        var topTheme = (themes[0] || 'market news').toLowerCase();

        var text;
        if (hits.length > 0) {
          var holdStr = hits[0].name + ' holds ' + hits[0].symbol;
          if (hits.length > 1) holdStr += ', ' + hits[1].name + ' holds ' + hits[1].symbol;
          text = 'News alert. ' + topTheme + ' headlines. ' + holdStr + ' affected.';
        } else {
          text = 'News alert. ' + topTheme + ' headlines. Watch ' + (affectedSyms.slice(0,2).join(' ') || 'the board') + '.';
        }

        _enqueue(text, 'news:' + newsKey);

        // Save note to Archer memory
        var note = '[NEWS ALERT ' + new Date().toLocaleTimeString() + '] ' +
          'Theme: ' + themes.join(', ') + '. ' +
          'Mood: ' + (d.news_summary || mood) + '. ' +
          'Affected sectors: ' + affectedSyms.slice(0, 5).join(', ') + '. ' +
          (hits.length ? 'Positions at risk: ' + hits.map(function(h){ return h.name + '/' + h.symbol; }).join(', ') + '.' : '');
        _saveNote(note);
      })
      .catch(function () {});
    setTimeout(_pollNews, 900000);
  }

  // g) Event Shield — scheduled events approaching / clearing (5min)
  function _pollEvents() {
    fetch('/api/ready-room/events')
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d) return;
        var events = Array.isArray(d) ? d : (d.events || d.upcoming || []);
        var now = Date.now();

        events.forEach(function (ev) {
          var id  = ev.id || ev.event_id || (ev.title + ':' + ev.event_time);
          var sev = (ev.severity || ev.impact || '').toUpperCase();
          if (sev !== 'HIGH' && sev !== 'CRITICAL') return;

          // Parse event time
          var evMs = 0;
          try { evMs = new Date(ev.event_time || ev.time || '').getTime(); } catch(e) {}
          if (!evMs) return;

          var minsAway = (evMs - now) / 60000;

          // Approaching: within 30 min and not yet announced as approaching
          if (minsAway > 0 && minsAway <= 30 && _seenEvents[id] !== 'approaching') {
            _seenEvents[id] = 'approaching';
            var minsRound = Math.round(minsAway);
            var name = (ev.title || ev.name || 'Scheduled event').replace(/_/g, ' ');
            _enqueue('Heads up Captain. ' + name + ' in ' + minsRound + ' minutes.', 'ev-approach:' + id);
          }

          // Cleared: 5-35 min past event time
          var minsPast = (now - evMs) / 60000;
          if (minsPast > 5 && minsPast <= 35 && _seenEvents[id] !== 'cleared') {
            _seenEvents[id] = 'cleared';
            var name2 = (ev.title || ev.name || 'Event').replace(/_/g, ' ');
            _enqueue('Event clear. ' + name2 + ' has passed. Normal operations.', 'ev-clear:' + id);
          }
        });
      })
      .catch(function () {});
    setTimeout(_pollEvents, 300000);
  }

  // ── Boot ──────────────────────────────────────────────────────────────────
  function _boot() {
    _injectNav();
    _refreshPositions();
    // Stagger pollers to avoid burst on load
    setTimeout(_pollTrades,     2000);
    setTimeout(_pollCondition,  5000);
    setTimeout(_pollAlerts,     8000);
    setTimeout(_pollBridge,    11000);
    setTimeout(_pollKirk,      14000);
    setTimeout(_pollNews,      20000);
    setTimeout(_pollEvents,    25000);
    // Refresh position cache every 10 minutes
    setInterval(_refreshPositions, 600000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _boot);
  } else {
    _boot();
  }

  // ── Public API ────────────────────────────────────────────────────────────
  window.ArcherAudio = {
    toggle: function () {
      _enabled = !_enabled;
      localStorage.setItem('archer_voice_alerts', String(_enabled));
      _updateBtn();
      if (!_enabled) { _queue = []; }
    },
    setVolume: function (v) {
      _volume = Math.max(0, Math.min(1, v));
      localStorage.setItem('archer_alert_volume', String(_volume));
      var s = document.getElementById('archerVolSlider');
      if (s) s.value = String(_volume);
    },
    isEnabled:  function () { return _enabled; },
    getVolume:  function () { return _volume; },
    /** External callers (dynamic-alerts, red-alert banner, etc.) */
    speak: function (text, key) {
      _enqueue(text, key || ('ext:' + Date.now()));
    }
  };

})();
