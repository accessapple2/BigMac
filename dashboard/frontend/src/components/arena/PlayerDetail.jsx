import React, { useState, useCallback } from 'react'
import { usePolling } from '../../hooks/usePolling'
import { api } from '../../api/client'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, AreaChart, Area } from 'recharts'
import { timeAgo, formatTimeAZ } from '../../utils/time'
import { formatMoney, formatPercent, getDisplayCapital, getPortfolioDisplayName, isTrackingOnlyPortfolio, safeNumber } from '../../utils/numbers'

const PROVIDER_COLORS = {
  anthropic: '#22c55e',
  openai: '#22c55e',
  google: '#3b82f6',
  xai: '#ef4444',
  ollama: '#94a3b8',
  crewai: '#f59e0b',
  matrix: '#00bcd4',
}

const PLAYER_DESCRIPTIONS = {
  'dalio-metals': "All Weather Portfolio — Ray Dalio's strategy for steady returns in any market. Balanced across stocks, bonds, gold, and commodities.",
  'super-agent': 'CrewAI collective intelligence — the unified consensus of all 14 AI traders on the crew.',
  'neo-matrix': 'Matrix-native independent participant from port 8000. Shared into Arena for comparison and conversation, but not governed by Arena approvals.',
  'options-sosnoff': 'Options specialist. Sells premium, manages risk through position sizing and defined-risk spreads.',
  'energy-arnold': 'Energy sector specialist. Trades crude oil, natural gas, and energy ETFs using macro and technical signals.',
  'dayblade-sulu': '0DTE intraday scalper. Trades same-day expiry options on gap fills and momentum setups.',
}

const IB = 'http://127.0.0.1:5001'

export default function PlayerDetail({ playerId, onClose }) {
  const [detailTab, setDetailTab] = useState('portfolio')
  const [chartSymbol, setChartSymbol] = useState(null)

  const fetchPlayer = useCallback(() => playerId ? api.getPlayer(playerId) : Promise.resolve(null), [playerId])
  const fetchTrades = useCallback(() => playerId ? api.getPlayerTrades(playerId) : Promise.resolve([]), [playerId])
  const fetchSignals = useCallback(() => playerId ? api.getPlayerSignals(playerId) : Promise.resolve([]), [playerId])
  const fetchHistory = useCallback(() => playerId ? api.getPlayerHistory(playerId) : Promise.resolve([]), [playerId])
  const fetchChat = useCallback(() => playerId ? api.getPlayerChat(playerId) : Promise.resolve([]), [playerId])
  const fetchHealth = useCallback(() => playerId ? api.getPortfolioHealth(playerId) : Promise.resolve(null), [playerId])

  const { data: player } = usePolling(fetchPlayer, 30000)
  const { data: trades } = usePolling(fetchTrades, 30000)
  const { data: signals } = usePolling(fetchSignals, 30000)
  const { data: history } = usePolling(fetchHistory, 120000)
  const { data: chat } = usePolling(fetchChat, 60000)
  const { data: health } = usePolling(fetchHealth, 60000)

  if (!playerId) return null

  const totalValue = getDisplayCapital(player)
  const returnPct = safeNumber(player?.return_pct, 0)
  const startingCapital = player?.player_id === 'super-agent' ? 10000 : player?.player_id === 'steve-webull' ? 7049.68 : player?.player_id === 'dayblade-0dte' ? 5000 : 10000
  const isPositive = totalValue >= startingCapital
  const providerColor = PROVIDER_COLORS[player?.provider] || '#fff'
  const displayName = getPortfolioDisplayName(player)
  const trackingOnly = isTrackingOnlyPortfolio(player)

  return (
    <div className={`player-detail ${playerId ? 'open' : ''}`}>
      <button className="close-btn" onClick={onClose}>&times;</button>

      {player && (
        <>
          {/* Header */}
          <div className="pd-header">
            <div className="pd-avatar" style={{ background: providerColor }}>
              {displayName.substring(0, 2).toUpperCase()}
            </div>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                <h2 style={{ color: providerColor, margin: 0 }}>{displayName}</h2>
                {trackingOnly && (
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
                )}
              </div>
              <div className="model-tag">{player.provider} / {player.model}</div>
              {PLAYER_DESCRIPTIONS[player.player_id] && (
                <div style={{ fontSize: 11, color: '#475569', marginTop: 4, lineHeight: 1.4, maxWidth: 260 }}>
                  {PLAYER_DESCRIPTIONS[player.player_id]}
                </div>
              )}
            </div>
          </div>

          {/* Stats Grid */}
          <div className="stats-grid">
            <div className="stat-box">
              <div className="label">Total Value</div>
              <div className={`value ${isPositive ? 'positive' : 'negative'}`}>
                {formatMoney(totalValue)}
              </div>
            </div>
            <div className="stat-box">
              <div className="label">Return</div>
              <div className={`value ${isPositive ? 'positive' : 'negative'}`}>
                {formatPercent(returnPct, 2, true)}
              </div>
            </div>
            <div className="stat-box">
              <div className="label">Unrealized P&L</div>
              <div className={`value ${safeNumber(player.total_unrealized_pnl, 0) >= 0 ? 'positive' : 'negative'}`}>
                {safeNumber(player.total_unrealized_pnl, 0) >= 0 ? '+' : ''}{formatMoney(safeNumber(player.total_unrealized_pnl, 0))}
              </div>
            </div>
            <div className="stat-box">
              <div className="label">Cash</div>
              <div className="value">{formatMoney(safeNumber(player.cash, 0))}</div>
            </div>
            <div className="stat-box">
              <div className="label">Positions Value</div>
              <div className="value">{formatMoney(safeNumber(player.total_positions_value, 0))}</div>
            </div>
            <div className="stat-box">
              <div className="label">Trades</div>
              <div className="value">{player.stats?.total_trades || 0}</div>
            </div>
          </div>

          {/* Portfolio Chart */}
          {history && history.length > 0 && (
            <div className="pd-chart">
              <h3>Portfolio Value</h3>
              <ResponsiveContainer width="100%" height={180}>
                <AreaChart data={history}>
                  <defs>
                    <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={providerColor} stopOpacity={0.3} />
                      <stop offset="95%" stopColor={providerColor} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="recorded_at" tick={false} />
                  <YAxis domain={['auto', 'auto']} tick={{ fontSize: 11, fill: '#64748b' }} />
                  <Tooltip
                    contentStyle={{ background: '#1a1f2e', border: '1px solid #2d3348', borderRadius: 8 }}
                    labelStyle={{ color: '#94a3b8' }}
                    formatter={(value) => [formatMoney(safeNumber(value, 0)), 'Value']}
                  />
                  <Area type="monotone" dataKey="total_value" stroke={providerColor} strokeWidth={2}
                    fill="url(#colorValue)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Detail Tabs */}
          <div className="tabs">
            {['portfolio', 'health', 'trades', 'signals', 'chat'].map(t => (
              <button key={t} className={`tab ${detailTab === t ? 'active' : ''}`}
                onClick={() => setDetailTab(t)}>
                {t.charAt(0).toUpperCase() + t.slice(1)}
              </button>
            ))}
          </div>

          {/* Portfolio Tab */}
          {detailTab === 'portfolio' && (
            <div className="pd-section">
              {player.positions && player.positions.length > 0 && (
                <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
                  <button
                    className="ib-chart-all-btn"
                    onClick={() => {
                      const syms = player.positions.map(p => p.symbol).join(',')
                      window.open(`${IB}/ib_multichart.html?symbols=${syms}&tf=D`, '_blank')
                    }}
                  >
                    📊 Chart All Positions
                  </button>
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
              {player.positions && player.positions.length > 0 ? (
                <div className="positions-list">
                  {player.positions.map((pos, i) => {
                    const pnl = pos.unrealized_pnl || 0
                    const pnlPct = pos.unrealized_pnl_pct || 0
                    const isPosPositive = pnl >= 0
                    const isStopLoss = pnlPct <= -12
                    return (
                      <div key={i} className={`position-card ${isStopLoss ? 'stop-loss-alert' : ''}`}
                        style={{ cursor: 'pointer' }}
                        onClick={() => setChartSymbol(pos.symbol)}
                      >
                        <div className="pos-header">
                          <strong>{pos.symbol}</strong>
                          <button
                            className="ib-chart-btn"
                            onClick={(e) => {
                              e.stopPropagation()
                              window.open(`${IB}/ib_chart.html?symbol=${pos.symbol}`, '_blank')
                            }}
                            title={`Open ${pos.symbol} chart`}
                          >📈</button>
                          {isStopLoss && <span className="status-badge halted">STOP-LOSS</span>}
                          {pos.option_type && (
                            <span className={`trade-action ${pos.option_type}`}>
                              {pos.option_type.toUpperCase()}
                            </span>
                          )}
                        </div>
                        <div className="pos-details">
                          <div>
                            <span className="pos-label">Qty</span>
                            <span className="pos-value">{pos.qty}</span>
                          </div>
                          <div>
                            <span className="pos-label">Avg Price</span>
                            <span className="pos-value">${pos.avg_price?.toFixed(2)}</span>
                          </div>
                          <div>
                            <span className="pos-label">Current</span>
                            <span className="pos-value">${(pos.current_price || pos.avg_price)?.toFixed(2)}</span>
                          </div>
                          <div>
                            <span className="pos-label">Mkt Value</span>
                            <span className="pos-value">${(pos.market_value || pos.qty * pos.avg_price).toFixed(2)}</span>
                          </div>
                          <div>
                            <span className="pos-label">P&L</span>
                            <span className={`pos-value ${isPosPositive ? 'positive' : 'negative'}`}>
                              {isPosPositive ? '+' : ''}${pnl.toFixed(2)}
                            </span>
                          </div>
                          <div>
                            <span className="pos-label">P&L %</span>
                            <span className={`pos-value ${isPosPositive ? 'positive' : 'negative'}`}>
                              {isPosPositive ? '+' : ''}{pnlPct.toFixed(2)}%
                            </span>
                          </div>
                        </div>
                        <SourcesSection sources={pos.sources} />
                      </div>
                    )
                  })}
                </div>
              ) : (
                <div className="empty-state">No open positions</div>
              )}
            </div>
          )}

          {/* Health Tab */}
          {detailTab === 'health' && (
            <div className="pd-section">
              {health && !health.error ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  {/* Summary Cards */}
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: 8 }}>
                    <div style={healthCard}>
                      <div style={healthLabel}>Avg Score</div>
                      <div style={{ ...healthValue, color: gradeColor(health.avg_grade) }}>
                        {health.avg_grade || '?'} ({health.avg_smart_score || '?'})
                      </div>
                    </div>
                    <div style={healthCard}>
                      <div style={healthLabel}>Cash</div>
                      <div style={healthValue}>{health.cash_pct}%</div>
                    </div>
                    <div style={healthCard}>
                      <div style={healthLabel}>Beta</div>
                      <div style={{ ...healthValue, color: health.weighted_beta > 1.3 ? '#f08080' : health.weighted_beta < 0.8 ? '#6ec891' : '#ddd' }}>
                        {health.weighted_beta?.toFixed(2) || '-'}
                      </div>
                    </div>
                    <div style={healthCard}>
                      <div style={healthLabel}>Avg P/E</div>
                      <div style={healthValue}>{health.avg_pe?.toFixed(1) || '-'}</div>
                    </div>
                    <div style={healthCard}>
                      <div style={healthLabel}>Concentration</div>
                      <div style={{ ...healthValue, color: health.sector_concentration === 'HIGH' ? '#f08080' : health.sector_concentration === 'MODERATE' ? '#c8c86e' : '#6ec891' }}>
                        {health.sector_concentration}
                      </div>
                    </div>
                  </div>

                  {/* Sector Breakdown */}
                  {health.sector_breakdown && Object.keys(health.sector_breakdown).length > 0 && (
                    <div style={{ background: '#1a1a2e', borderRadius: 8, padding: 12 }}>
                      <div style={{ color: '#888', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', marginBottom: 8 }}>Sector Breakdown</div>
                      {Object.entries(health.sector_breakdown).sort((a, b) => b[1] - a[1]).map(([sector, pct]) => (
                        <div key={sector} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                          <span style={{ color: '#ccc', fontSize: 12, minWidth: 140 }}>{sector}</span>
                          <div style={{ flex: 1, height: 6, background: '#333', borderRadius: 3, overflow: 'hidden' }}>
                            <div style={{ width: `${Math.min(pct, 100)}%`, height: '100%', background: pct > 50 ? '#f08080' : pct > 30 ? '#c8c86e' : '#00d4aa', borderRadius: 3 }} />
                          </div>
                          <span style={{ color: '#aaa', fontSize: 11, minWidth: 40, textAlign: 'right' }}>{pct}%</span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Earnings Exposure */}
                  {health.earnings_this_week && health.earnings_this_week.length > 0 && (
                    <div style={{ background: '#3a1a1a', borderRadius: 8, padding: 12, border: '1px solid #5f1d1d' }}>
                      <div style={{ color: '#f08080', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', marginBottom: 8 }}>
                        Earnings Exposure ({health.earnings_exposure_pct}% of portfolio)
                      </div>
                      {health.earnings_this_week.map(e => (
                        <div key={e.symbol} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', fontSize: 12 }}>
                          <strong style={{ color: '#ddd' }}>{e.symbol}</strong>
                          <span style={{ color: '#f08080' }}>{e.days}d — {e.date}</span>
                          <span style={{ color: '#aaa' }}>{e.pct_of_portfolio}% of portfolio</span>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* High Short Interest */}
                  {health.high_short_interest && health.high_short_interest.length > 0 && (
                    <div style={{ background: '#1a1a2e', borderRadius: 8, padding: 12 }}>
                      <div style={{ color: '#c8c86e', fontSize: 11, fontWeight: 600, textTransform: 'uppercase', marginBottom: 8 }}>High Short Interest Positions</div>
                      {health.high_short_interest.map(s => (
                        <div key={s.symbol} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', fontSize: 12 }}>
                          <strong style={{ color: '#ddd' }}>{s.symbol}</strong>
                          <span style={{ color: '#f08080' }}>{s.short_pct.toFixed(1)}% shorted</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <div className="empty-state">
                  {health?.error ? 'No position data' : 'Loading health check...'}
                </div>
              )}
            </div>
          )}

          {/* Trades Tab */}
          {detailTab === 'trades' && (
            <div className="trade-list pd-section">
              {(trades || []).length === 0 ? (
                <div className="empty-state">No trades yet</div>
              ) : (
                trades.map((t, i) => (
                  <div key={i} className="trade-item">
                    <div className="trade-left">
                      <span className={`trade-action ${t.action.toLowerCase()}`}>{t.action}</span>
                      <strong>{t.symbol}</strong>
                      <span className="mono" style={{ marginLeft: 8 }}>
                        {t.qty} @ ${t.price?.toFixed(2)}
                      </span>
                    </div>
                    <div className="trade-time">{formatTimeAZ(t.executed_at)} · {timeAgo(t.executed_at)}</div>
                    {t.reasoning && (
                      <div className="trade-reasoning">{t.reasoning.substring(0, 150)}</div>
                    )}
                    <SourcesSection sources={t.sources} />
                  </div>
                ))
              )}
            </div>
          )}

          {/* Signals Tab */}
          {detailTab === 'signals' && (
            <div className="trade-list pd-section">
              {(signals || []).length === 0 ? (
                <div className="empty-state">No signals yet</div>
              ) : (
                signals.map((s, i) => (
                  <div key={i} className="signal-item">
                    <div className="signal-conf">
                      <span className="mono" style={{ fontSize: 13, fontWeight: 600 }}>
                        {(s.confidence * 100).toFixed(0)}%
                      </span>
                      <div className="conf-bar">
                        <div className="conf-fill" style={{
                          width: `${s.confidence * 100}%`,
                          background: s.confidence > 0.7 ? 'var(--green)' : s.confidence > 0.4 ? 'var(--yellow)' : 'var(--red)'
                        }} />
                      </div>
                    </div>
                    <div style={{ flex: 1 }}>
                      <span className={`trade-action ${s.signal.toLowerCase()}`}>{s.signal}</span>
                      <strong style={{ marginLeft: 8 }}>{s.symbol}</strong>
                    </div>
                    <div className="trade-time">{formatTimeAZ(s.created_at)} · {timeAgo(s.created_at)}</div>
                  </div>
                ))
              )}
            </div>
          )}

          {/* Chat Tab */}
          {detailTab === 'chat' && (
            <div className="pd-section">
              {(chat || []).length === 0 ? (
                <div className="empty-state">No chat messages yet</div>
              ) : (
                <div className="chat-feed">
                  {chat.map((msg) => (
                    <div key={msg.id} className="chat-message compact">
                      <div className="chat-content">
                        <div className="chat-text">{msg.message}</div>
                        <div className="chat-time">{formatTimeAZ(msg.created_at)} · {timeAgo(msg.created_at)}</div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}

function SourcesSection({ sources }) {
  const [expanded, setExpanded] = React.useState(false)
  if (!sources) return null
  const sourceList = sources.split(',').filter(Boolean)
  if (sourceList.length === 0) return null
  return (
    <div style={{ marginTop: 6 }}>
      <span
        onClick={() => setExpanded(!expanded)}
        style={{
          fontSize: 10,
          fontWeight: 600,
          padding: '2px 6px',
          borderRadius: 3,
          background: 'rgba(59,130,246,0.12)',
          color: '#60a5fa',
          cursor: 'pointer',
          border: '1px solid rgba(59,130,246,0.25)',
          userSelect: 'none',
        }}
      >
        Data Sources ({sourceList.length}) {expanded ? '▾' : '▸'}
      </span>
      {expanded && (
        <div style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: 4,
          marginTop: 4,
        }}>
          {sourceList.map((s, i) => (
            <span key={i} style={{
              fontSize: 10,
              padding: '1px 5px',
              borderRadius: 3,
              background: '#1a1f2e',
              color: '#94a3b8',
              border: '1px solid #2d3348',
            }}>
              {s.trim()}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

const healthCard = { background: '#1a1a2e', borderRadius: 8, padding: '8px 12px', textAlign: 'center' }
const healthLabel = { color: '#888', fontSize: 10, fontWeight: 600, textTransform: 'uppercase', marginBottom: 2 }
const healthValue = { color: '#ddd', fontSize: 16, fontWeight: 700 }

function gradeColor(grade) {
  const colors = { A: '#75b798', B: '#6ec891', C: '#c8c86e', D: '#c8916e', F: '#f08080' }
  return colors[grade] || '#ddd'
}
