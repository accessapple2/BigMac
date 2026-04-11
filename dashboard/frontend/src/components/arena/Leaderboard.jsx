import React, { useState, useEffect, useRef, useCallback } from 'react'
import { api } from '../../api/client'
import { formatMoney, formatPercent, getCurrentValue, getDayPnL, getDisplayCapital, getPortfolioDisplayName, getReturnPct, getTotalPnL, isTrackingOnlyPortfolio, safeNumber } from '../../utils/numbers'

const PROVIDER_COLORS = {
  anthropic: '#22c55e',
  openai: '#22c55e',
  google: '#3b82f6',
  xai: '#ef4444',
  ollama: '#94a3b8',
  webull: '#fbbf24',
  crewai: '#f59e0b',
  matrix: '#00bcd4',
}

const PROVIDER_AVATARS = {
  anthropic: 'OA',
  openai: 'GP',
  google: 'GE',
  xai: 'GK',
  ollama: 'OL',
  webull: 'WB',
  crewai: '⭐',
  matrix: 'NE',
}

// Module-level storage — survives re-renders, unmounts, and parent polling ticks
const _expandedPlayers = new Set()
const _portfolioCache = {}

const IB = 'http://127.0.0.1:5001'

function PortfolioInline({ playerId }) {
  const [positions, setPositions] = useState(null)
  const [loading, setLoading] = useState(true)
  const [chartSymbol, setChartSymbol] = useState(null)
  const intervalRef = useRef(null)

  const fetchPositions = useCallback(async () => {
    try {
      const player = await api.getPlayer(playerId)
      const newPositions = player?.positions || []
      const key = JSON.stringify(newPositions)
      // Only update state if data actually changed
      if (_portfolioCache[playerId] !== key) {
        _portfolioCache[playerId] = key
        setPositions(newPositions)
      }
    } catch {
      // leave previous data in place
    } finally {
      setLoading(false)
    }
  }, [playerId])

  useEffect(() => {
    fetchPositions()
    intervalRef.current = setInterval(fetchPositions, 5000)
    return () => clearInterval(intervalRef.current)
  }, [fetchPositions])

  if (loading && positions === null) {
    return <div className="lb-portfolio-inline"><span className="lb-portfolio-loading">Loading positions...</span></div>
  }

  if (!positions || positions.length === 0) {
    return <div className="lb-portfolio-inline"><span className="lb-portfolio-empty">No open positions</span></div>
  }

  const stockSymbols = (positions || []).filter(p => !p.option_type).map(p => p.symbol)

  return (
    <div className="lb-portfolio-inline" onClick={e => e.stopPropagation()}>

      {/* ── Chart All Positions — full-width colored bar above table ── */}
      {stockSymbols.length > 0 && (
        <div style={{
          display: 'flex', alignItems: 'center', gap: 8,
          padding: '8px 12px', margin: '4px 0 6px',
          background: 'linear-gradient(90deg, #0f2d1f 0%, #0a1f2e 100%)',
          border: '1px solid #16a34a', borderRadius: 6,
        }}>
          <span style={{ fontSize: 11, color: '#86efac', fontWeight: 700, letterSpacing: 0.5 }}>
            CHARTS
          </span>
          <button
            onClick={() => window.open(`${IB}/ib_multichart.html?symbols=${stockSymbols.join(',')}&tf=D`, '_blank')}
            style={{
              background: '#16a34a', border: 'none', borderRadius: 5,
              color: '#fff', fontSize: 12, fontWeight: 700,
              padding: '4px 14px', cursor: 'pointer', letterSpacing: 0.3,
            }}
          >
            📊 Chart All {stockSymbols.length} Positions
          </button>
          <span style={{ fontSize: 10, color: '#4ade80', marginLeft: 2 }}>
            {stockSymbols.join(' · ')}
          </span>
        </div>
      )}

      {chartSymbol && (
        <div className="ib-iframe-panel">
          <div className="ib-iframe-header">
            <span>{chartSymbol}</span>
            <button onClick={() => setChartSymbol(null)}>✕</button>
          </div>
          <iframe
            src={`${IB}/ib_chart.html?symbol=${chartSymbol}`}
            title={`Chart ${chartSymbol}`}
            className="ib-iframe"
          />
        </div>
      )}

      <table className="lb-positions-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Type</th>
            <th className="num">Qty</th>
            <th className="num">Avg</th>
            <th className="num">Current</th>
            <th className="num">Mkt Value</th>
            <th className="num">P&L</th>
            <th className="num">P&L %</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((pos, i) => {
            const pnl = safeNumber(pos.unrealized_pnl, 0)
            const pnlPct = safeNumber(pos.unrealized_pnl_pct, 0)
            const isPositive = (value - (player.starting_capital ?? 7000)) >= 0
            return (
              <tr key={i} style={{ cursor: 'pointer' }} onClick={() => setChartSymbol(pos.symbol)}>
                <td style={{ whiteSpace: 'nowrap' }}>
                  <strong style={{ marginRight: 6 }}>{pos.symbol}</strong>
                  <button
                    onClick={(e) => { e.stopPropagation(); window.open(`${IB}/ib_chart.html?symbol=${pos.symbol}`, '_blank') }}
                    title={`Chart ${pos.symbol} in IB`}
                    style={{
                      display: 'inline-flex', alignItems: 'center',
                      background: '#1e3a5f', border: '1px solid #3b82f6',
                      borderRadius: 4, cursor: 'pointer',
                      fontSize: 13, padding: '1px 5px', lineHeight: 1,
                      verticalAlign: 'middle',
                    }}
                  >📈</button>
                </td>
                <td>
                  {pos.option_type
                    ? <span className={`trade-action ${pos.option_type}`}>{pos.option_type.toUpperCase()}</span>
                    : <span className="trade-action stock">STOCK</span>}
                </td>
                <td className="num mono">{safeNumber(pos.qty, 0).toFixed(2)}</td>
                <td className="num mono">{formatMoney(safeNumber(pos.avg_price, 0))}</td>
                <td className="num mono">{formatMoney(safeNumber(pos.current_price ?? pos.avg_price, 0))}</td>
                <td className="num mono">{formatMoney(safeNumber(pos.market_value ?? (safeNumber(pos.qty, 0) * safeNumber(pos.avg_price, 0)), 0))}</td>
                <td className={`num mono ${isPos ? 'positive' : 'negative'}`}>
                  {isPos ? '+' : ''}{formatMoney(pnl)}
                </td>
                <td className={`num mono ${isPos ? 'positive' : 'negative'}`}>
                  {formatPercent(pnlPct, 1, true)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const LS_KEY = 'tm_expanded_players'

export default function Leaderboard({ data, onSelect, season, hidePaused = false, showPausedToggle = false }) {
  // expanded is React state backed by module-level Set + localStorage for full persistence
  const [expanded, setExpanded] = useState(() => {
    try {
      const saved = localStorage.getItem(LS_KEY)
      if (saved) {
        const ids = JSON.parse(saved)
        ids.forEach(id => _expandedPlayers.add(id))
        return new Set(ids)
      }
    } catch {}
    return new Set(_expandedPlayers)
  })
  const [showPaused, setShowPaused] = useState(false)
  const [sortCol, setSortCol] = useState('return_pct')
  const [sortDir, setSortDir] = useState('desc')

  if (!data.length) {
    return <div className="empty-state">No players yet. Start the scanner to begin.</div>
  }

  const toggleExpand = (e, playerId) => {
    e.stopPropagation()
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(playerId)) {
        next.delete(playerId)
        _expandedPlayers.delete(playerId)
        delete _portfolioCache[playerId]
      } else {
        next.add(playerId)
        _expandedPlayers.add(playerId)
      }
      try { localStorage.setItem(LS_KEY, JSON.stringify([...next])) } catch {}
      return next
    })
  }

  // Filter paused models based on props
  const shouldHidePaused = hidePaused || (showPausedToggle && !showPaused)
  const pausedCount = data.filter(p => p.is_paused).length

  const getStarting = (player) =>
  player.player_id === 'super-agent' ? 25000
  : player.player_id === 'dayblade-0dte' ? (season === 1 ? 2000 : 5000)
  : player.player_id === 'steve-webull' ? 7049.68
  : 7000

  const handleSort = (col) => {
    if (sortCol === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortCol(col); setSortDir('desc') }
  }

 const sortedData = [...(shouldHidePaused ? data.filter(p => !p.is_paused) : data)].sort((a, b) => {
  let av = 0, bv = 0
  if (sortCol === 'total_value') {
    av = a.current_equity ?? a.total_value ?? a.cash ?? 0
    bv = b.current_equity ?? b.total_value ?? b.cash ?? 0
  } else if (sortCol === 'day_change') {
    av = a.day_pnl ?? a.day_change ?? 0
    bv = b.day_pnl ?? b.day_change ?? 0
  } else if (sortCol === 'total_pnl') {
    av = a.total_pnl ?? 0
    bv = b.total_pnl ?? 0
  } else if (sortCol === 'return_pct') {
    av = a.return_pct ?? 0
    bv = b.return_pct ?? 0
  }
  return sortDir === 'desc' ? bv - av : av - bv
})

  const arrow = (col) => sortCol === col ? (sortDir === 'desc' ? ' ▼' : ' ▲') : ' ⇅'
  const thStyle = (col) => ({
    cursor: 'pointer', userSelect: 'none', padding: '6px 10px',
    color: sortCol === col ? '#94a3b8' : '#475569',
    fontSize: 10, fontWeight: 700, letterSpacing: 0.5,
    borderBottom: '1px solid #1e2336', background: '#0d1117',
    whiteSpace: 'nowrap',
  })

  return (
    <div className="leaderboard">
      {showPausedToggle && pausedCount > 0 && (
        <div style={{
          display: 'flex', justifyContent: 'flex-end', padding: '4px 8px 8px',
        }}>
          <button
            onClick={() => setShowPaused(!showPaused)}
            style={{
              padding: '4px 12px', borderRadius: 6, fontSize: 11, fontWeight: 600,
              background: showPaused ? '#2a2a4a' : '#1a1a2e',
              color: showPaused ? '#ef4444' : '#64748b',
              border: '1px solid #333', cursor: 'pointer',
            }}
          >
            {showPaused ? `Hide ${pausedCount} Paused` : `Show ${pausedCount} Paused`}
          </button>
        </div>
      )}
      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 0, marginBottom: 4 }}>
        <span onClick={() => handleSort('total_value')} style={thStyle('total_value')}>
          Account Value{arrow('total_value')}
        </span>
        <span onClick={() => handleSort('day_change')} style={thStyle('day_change')}>
          Day P&L{arrow('day_change')}
        </span>
        <span onClick={() => handleSort('total_pnl')} style={thStyle('total_pnl')}>
          Total P&L{arrow('total_pnl')}
        </span>
        <span onClick={() => handleSort('return_pct')} style={thStyle('return_pct')}>
          Return %{arrow('return_pct')}
        </span>
      </div>
      {sortedData.map((player, i) => {
        const value = player.current_equity ?? player.total_value ?? player.cash ?? 0
const totalPnL = player.total_pnl ?? 0
const dayPnL = player.day_pnl ?? player.day_change ?? 0
const returnPct = player.return_pct ?? 0
        const startingCash = player.starting_capital ?? getStarting(player)
        const displayCapital = value
       const isPositive = value >= startingCash
        const color = displayCapital < startingCash ? '#ef4444' : '#22c55e'
        const isExpanded = expanded.has(player.player_id)
        const displayName = getPortfolioDisplayName(player)
        const trackingOnly = isTrackingOnlyPortfolio(player)
     
        
      
        return (
          <div key={player.player_id} className={`lb-entry ${isExpanded ? 'expanded' : ''}`}>
            <div
              className={`leaderboard-row ${player.is_paused ? 'paused' : ''}`}
              onClick={(e) => { e.stopPropagation(); toggleExpand(e, player.player_id) }}
            >
              <div className="lb-rank">
             <span className={`rank rank-${i + 1}`}>{i + 1}</span>
              </div>
              <div
                className="lb-avatar"
                style={{ background: color, cursor: 'pointer' }}
                onClick={(e) => { e.stopPropagation(); onSelect(player.player_id) }}
                title="Open full detail"
              >
                {PROVIDER_AVATARS[player.provider] || '??'}
              </div>
              <div className="lb-info">
                <div className="lb-name" style={{ color }}>{displayName}</div>
                <div className="lb-provider">{player.provider}</div>
              </div>
              <div className="lb-value">
                <div className={`lb-total ${isPositive ? 'positive' : 'negative'}`}>
                  {formatMoney(value)}
                </div>
                <div className={`lb-return ${isPositive ? 'positive' : 'negative'}`}>
                  {formatPercent(returnPct, 2, true)}
                </div>
                
              </div>
              <div className="lb-stats">
                <div className="lb-stat">
                <span className="lb-stat-label">Open</span>
                 <span className="lb-stat-value">{player.positions_count ?? 0}</span>
                </div>
                <div className="lb-stat">
                  <span className="lb-stat-label">Win</span>
                  <span className="lb-stat-value">{formatPercent(safeNumber(player.win_rate, 0), 0)}</span>
                </div>
                <div className="lb-stat">
                  <span className="lb-stat-label">Unreal P&L</span>
                  <span className={`lb-stat-value ${safeNumber(player.unrealized_pnl, 0) >= 0 ? 'positive' : 'negative'}`}>
                    {safeNumber(player.unrealized_pnl, 0) >= 0 ? '+' : ''}{formatMoney(safeNumber(player.unrealized_pnl, 0))}
                  </span>
                </div>
                <div className="lb-stat">
                  <span className="lb-stat-label">Day</span>
                  <span className={`lb-stat-value ${dayPnL >= 0 ? 'positive' : 'negative'}`}>
                    {dayPnL >= 0 ? '+' : ''}{formatMoney(dayPnL)}
                  </span>
                </div>
                <div className="lb-stat">
                  <span className="lb-stat-label">Total</span>
                  <span className={`lb-stat-value ${totalPnL >= 0 ? 'positive' : 'negative'}`}>
                    {totalPnL >= 0 ? '+' : ''}{formatMoney(totalPnL)}
                  </span>
                </div>
              </div>
              <div className="lb-status">
                {player.is_paused ? (
                  <span className="status-badge paused">PAUSED</span>
                ) : trackingOnly ? (
                  <span style={{
                    marginLeft: 6,
                    fontSize: 10,
                    padding: '2px 6px',
                    borderRadius: 6,
                    background: '#27272a',
                    color: '#facc15',
                  }}>
                    TRACKING ONLY
                  </span>
                ) : player.is_halted ? (
                  <span className="status-badge halted">HALTED</span>
                ) : player.is_active ? (
                  <span className="status-badge active">ACTIVE</span>
                ) : (
                  <span className="status-badge inactive">INACTIVE</span>
                )}
                {player.has_shadow_options && (
                  <span
                    className="status-badge"
                    title="This model has open options positions but is not designated as an options trader. Positions were auto-reclassified from stock trades."
                    style={{ background: '#78350f', color: '#fbbf24', border: '1px solid #f59e0b', marginTop: 2, cursor: 'help' }}
                  >
                    ⚠ OPTIONS
                  </span>
                )}
              </div>
              <button
                className="lb-expand-btn"
                onClick={(e) => { e.stopPropagation(); onSelect(player.player_id) }}
                title="Open full detail"
              >
                &#8594;
              </button>
            </div>
            {isExpanded && <PortfolioInline playerId={player.player_id} />}
          </div>
        )
      })}
    </div>
  )
}
