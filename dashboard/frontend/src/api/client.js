const BASE = '/api'

async function fetchJSON(path) {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

export const api = {
  // Arena
  getLeaderboard: (season) => fetchJSON(`/arena/leaderboard${season ? `?season=${season}` : ''}`),
  getPlayer: (id) => fetchJSON(`/arena/player/${id}`),
  getPlayerTrades: (id, limit = 50) => fetchJSON(`/arena/player/${id}/trades?limit=${limit}`),
  getPlayerSignals: (id, limit = 50) => fetchJSON(`/arena/player/${id}/signals?limit=${limit}`),
  getPlayerHistory: (id) => fetchJSON(`/arena/player/${id}/history`),
  getComparison: (season) => fetchJSON(`/arena/comparison${season ? `?season=${season}` : ''}`),
  getPlayerPnL: (id) => fetchJSON(`/arena/player/${id}/pnl`),
  getPlayerOpenPositions: (id) => fetchJSON(`/arena/player/${id}/open-positions`),
  getEquityCurve: (playerId, season) => {
    const params = []
    if (playerId) params.push(`player_id=${playerId}`)
    if (season) params.push(`season=${season}`)
    return fetchJSON(`/arena/equity-curve${params.length ? '?' + params.join('&') : ''}`)
  },
  getStatus: () => fetchJSON('/status'),

  // Trades & Signals
  getRecentTrades: (limit = 50, season, timeframe) => fetchJSON(`/trades/recent?limit=${limit}${season ? `&season=${season}` : ''}${timeframe ? `&timeframe=${timeframe}` : ''}`),
  getRecentSignals: (limit = 50, season, timeframe) => fetchJSON(`/signals/recent?limit=${limit}${season ? `&season=${season}` : ''}${timeframe ? `&timeframe=${timeframe}` : ''}`),
  getAndersonDecisionSummary: () => fetchJSON('/anderson/decision-summary'),

  // Chat
  getRecentChat: (limit = 50) => fetchJSON(`/chat/recent?limit=${limit}`),
  getPlayerChat: (id, limit = 20) => fetchJSON(`/chat/player/${id}?limit=${limit}`),

  // News
  getRecentNews: (limit = 30) => fetchJSON(`/news/recent?limit=${limit}`),
  getSymbolNews: (symbol, limit = 10) => fetchJSON(`/news/${symbol}?limit=${limit}`),

  // Market
  getMarketPrices: () => fetchJSON('/market/prices'),

  // Fundamentals & OpenBB
  getFundamentals: () => fetchJSON('/fundamentals'),
  getFundamentalsSymbol: (symbol) => fetchJSON(`/fundamentals/${symbol}`),
  getSmartScores: () => fetchJSON('/fundamentals/scores'),
  getSmartScore: (symbol) => fetchJSON(`/fundamentals/score/${symbol}`),
  getPortfolioHealth: (playerId) => fetchJSON(`/portfolio-health/${playerId}`),
  getInsider: () => fetchJSON('/insider'),
  getInsiderSymbol: (symbol) => fetchJSON(`/insider/${symbol}`),
  getFilings: (symbol) => fetchJSON(`/filings/${symbol}`),
  getEconomicCalendar: () => fetchJSON('/economic-calendar'),
  getOptionsChain: (symbol, expiry) => fetchJSON(`/options-chain/${symbol}${expiry ? `?expiry=${expiry}` : ''}`),

  // Model Control
  getModelControl: () => fetchJSON('/model-control'),
  togglePauseAll: () => fetch(`${BASE}/model-control/pause-all`, { method: 'POST' }).then(r => r.json()),
  togglePausePlayer: (id) => fetch(`${BASE}/model-control/pause/${id}`, { method: 'POST' }).then(r => r.json()),
  forceScan: () => fetch(`${BASE}/model-control/force-scan`, { method: 'POST' }).then(r => r.json()),

  // Backtest Lab
  getBacktestModels: () => fetchJSON('/backtest/models'),
  runBacktest: (payload) => fetch(`${BASE}/backtest/run`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }).then(r => r.json()),
  getBacktestStatus: (runId) => fetchJSON(`/backtest/status/${runId}`),
  getBacktestRuns: (limit = 20) => fetchJSON(`/backtest/runs?limit=${limit}`),
  getBacktestRunDetail: (runId) => fetchJSON(`/backtest/run/${runId}`),
  getBacktestRankings: () => fetchJSON('/backtest/rankings'),
  timeMachine: (playerId, { days, startDate, endDate } = {}) => {
    const params = new URLSearchParams()
    if (startDate) params.set('start_date', startDate)
    if (endDate) params.set('end_date', endDate)
    if (days && !startDate) params.set('days', days)
    return fetchJSON(`/backtest/${playerId}?${params}`)
  },

  // Strategy Lab
  getStrategies: () => fetchJSON('/strategy-lab/strategies'),
  runStrategyBacktest: (payload) => fetch(`${BASE}/strategy-lab/run`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }).then(r => r.json()),
  runOptimize: (payload) => fetch(`${BASE}/strategy-lab/optimize`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }).then(r => r.json()),
  getOptimizeStatus: (runId) => fetchJSON(`/strategy-lab/status/${runId}`),
  deployStrategy: (payload) => fetch(`${BASE}/strategy-lab/deploy`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }).then(r => r.json()),
  getLatestOptimization: () => fetchJSON('/strategy-lab/latest'),
  getOptimizationHistory: (limit = 20) => fetchJSON(`/strategy-lab/history?limit=${limit}`),
  runAutoOptimize: () => fetch(`${BASE}/strategy-lab/auto-optimize`, {
    method: 'POST',
  }).then(r => r.json()),

  // Realtime Monitor
  getRealtimeAlerts: (limit = 20) => fetchJSON(`/realtime/alerts?limit=${limit}`),
  getRealtimeStatus: () => fetchJSON('/realtime/status'),

  // Cost Dashboard
  getCostDashboard: () => fetchJSON('/costs/dashboard'),
  getDailyCostTotal: () => fetchJSON('/costs/daily-total'),
  getCostBudget: () => fetchJSON('/costs/budget'),
  getCostHistory: (days = 30) => fetchJSON(`/costs/history?days=${days}`),

  // Chart Analyzer — 120s timeout for Ollama (local inference is slow)
  analyzeChart: (symbol, model) => {
    const ctrl = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), 120000)
    return fetch(`${BASE}/chart-analyze`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol, model }),
      signal: ctrl.signal,
    }).then(r => { clearTimeout(timer); return r.json() })
      .catch(e => { clearTimeout(timer); throw e })
  },
  getChartAnalyses: (symbol) => fetchJSON(`/chart-analyses${symbol ? `?symbol=${symbol}` : ''}`),
  getChartComparison: (symbol) => fetchJSON(`/chart-analyses/${symbol}/compare`),

  // Pre-Market Gaps
  getPremarketGaps: () => fetchJSON('/premarket-gaps'),
  analyzePremarketGaps: () => fetch(`${BASE}/premarket-analyze`, { method: 'POST' }).then(r => r.json()),
  getDaybladeGapCandidates: () => fetchJSON('/dayblade/gap-candidates'),

  // Stock Screener
  runScreener: (filters) => {
    const params = new URLSearchParams()
    if (filters.min_pe) params.set('min_pe', filters.min_pe)
    if (filters.max_pe) params.set('max_pe', filters.max_pe)
    if (filters.min_short_float) params.set('min_short_float', filters.min_short_float)
    if (filters.max_short_float) params.set('max_short_float', filters.max_short_float)
    if (filters.min_rel_volume) params.set('min_rel_volume', filters.min_rel_volume)
    if (filters.consensus) params.set('consensus', filters.consensus)
    if (filters.has_insider_buying) params.set('has_insider_buying', 'true')
    if (filters.earnings_within_days) params.set('earnings_within_days', filters.earnings_within_days)
    return fetchJSON(`/screener?${params.toString()}`)
  },

  // Insider Trading
  getInsiderTrades: (symbol) => fetchJSON(`/insider-trades/${symbol}`),
  getInsiderAlerts: () => fetchJSON('/insider-alerts'),

  // Sector Heatmap
  getSectorHeatmap: () => fetchJSON('/sectors/heatmap'),

  // Market Movers
  getMarketMovers: () => fetchJSON('/market-movers'),
  getWinnersLosers: () => fetchJSON('/winners-losers'),
  getHoldingsTop: () => fetchJSON('/holdings-top'),

  // CTO Advisory
  getCTOBriefing: () => fetchJSON('/cto/briefing'),

  // Admiral Picard — Ready Room
  getPicardStrategy: () => fetchJSON('/picard/strategy'),
  generatePicardBriefing: () => fetch(`${BASE}/picard/generate`, { method: 'POST' }).then(r => r.json()),

  // Market Flow Lean
  getFlowLean: () => fetchJSON('/market/flow-lean'),

  // 8/21 MA Cross Regime
  getMaRegime: () => fetchJSON('/regime/ma-cross'),

  // VIX
  getVix: () => fetchJSON('/market/vix'),

  // Earnings
  getEarnings: () => fetchJSON('/market/earnings'),

  // Consensus
  getConsensus: () => fetchJSON('/consensus'),

  // Impulse Alerts
  getImpulseActive: (maxAgeHours = 2) => fetchJSON(`/impulse/active?max_age_hours=${maxAgeHours}`),
  getImpulseRecent: (limit = 50) => fetchJSON(`/impulse/recent?limit=${limit}`),

  // Imbalance Zones
  getImbalanceZones: (ticker, limit = 100) => fetchJSON(`/imbalance/zones${ticker ? `?ticker=${ticker}&limit=${limit}` : `?limit=${limit}`}`),
  getImbalanceAll: (ticker, limit = 200) => fetchJSON(`/imbalance/all${ticker ? `?ticker=${ticker}&limit=${limit}` : `?limit=${limit}`}`),

  // Theta Scanner
  getThetaOpportunities: (minScore = 3, limit = 50) => fetchJSON(`/theta/opportunities?min_score=${minScore}&limit=${limit}`),
  getThetaHistory: (limit = 100) => fetchJSON(`/theta/history?limit=${limit}`),
  triggerThetaScan: () => fetch(`${BASE}/theta/scan`, { method: 'POST' }).then(r => r.json()),

  // Gap Scanner
  getGapsToday: (minGapPct = 0) => fetchJSON(`/gaps/today${minGapPct > 0 ? `?min_gap_pct=${minGapPct}` : ''}`),
  getGapsHistory: (limit = 100) => fetchJSON(`/gaps/history?limit=${limit}`),
  getGapStats: (days = 30) => fetchJSON(`/gaps/stats?days=${days}`),
  triggerGapScan: () => fetch(`${BASE}/gaps/scan`, { method: 'POST' }).then(r => r.json()),

  // 200 SMA Filter
  getSmaStatus: () => fetchJSON('/sma/status'),
  getSmaSignals: (limit = 50) => fetchJSON(`/sma/signals?limit=${limit}`),

  // Quorum
  startQuorum: (ticker) => fetch(`${BASE}/quorum/start`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker }),
  }).then(r => r.json()),
  getQuorumStatus: (quorumId) => fetchJSON(`/quorum/status/${quorumId}`),

  // War Room
  getWarRoom: (limit = 50) => fetchJSON(`/war-room?limit=${limit}`),
  postWarRoomMessage: (data) => fetch(`${BASE}/war-room/post`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  }).then(r => r.json()),

  // Intelligence — new pages
  getCorrelation: () => fetchJSON('/market/correlation'),
  getFearGreed: () => fetchJSON('/fear-greed'),
  getNavigatorConvergence: () => fetchJSON('/navigator/convergence'),
  getNavigatorUniverse: () => fetchJSON('/navigator/universe'),
  getPremiumETFs: () => fetchJSON('/premium-etfs'),
  getRealtimeAlertsHistory: (limit = 100) => fetchJSON(`/realtime/alerts?limit=${limit}`),

  // Super Agent — Captain's Orders
  captainAsk: (question) => fetch(`${BASE}/captain/ask`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ question }),
  }).then(r => r.json()),

  // Daily Briefing
  getRegime: () => fetchJSON('/regime/raw'),
  getDilithium: () => fetchJSON('/costs/dilithium'),
  getDailyBriefingAlerts: () => fetchJSON('/daily-briefing/alerts'),

  // Alpaca sync
  syncAlpacaPositions: () => fetch(`${BASE}/alpaca/sync-positions`, { method: 'POST' }).then(r => r.json()),
  getAlpacaStatus: () => fetchJSON('/alpaca/status'),
  getAlpacaPositions: () => fetchJSON('/alpaca/positions'),

  // Squeeze Scanner
  getSqueeze: (force = false) => fetchJSON(`/squeeze${force ? '?force=true' : ''}`),

  // UOA (Unusual Options Activity)
  getUOADashboard: () => fetchJSON('/uoa/dashboard'),
  getUOAAlerts: (severity) => fetchJSON(`/uoa/alerts${severity ? `?severity=${severity}` : ''}`),
  getUOAFlow: (ticker) => fetchJSON(`/uoa/flow/${ticker}`),
  triggerUOAScan: (type = 'quick') => fetch(`${BASE}/uoa/scan/${type}`, { method: 'POST' }).then(r => r.json()),
}
