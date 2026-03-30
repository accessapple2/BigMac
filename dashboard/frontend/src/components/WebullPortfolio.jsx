import React, { useState, useEffect, useCallback } from 'react'
import { formatMoney, formatPercent, getDisplayCapital, safeNumber } from '../utils/numbers'

const IB = 'http://127.0.0.1:5001'

export default function WebullPortfolio({ compact = false }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [chartSymbol, setChartSymbol] = useState(null)

  const fetchPortfolio = useCallback(async () => {
    try {
      const res = await fetch('/api/webull-portfolio')
      const json = await res.json()
      setData(json)
      setLastUpdated(new Date())
    } catch (e) {
      console.error('Portfolio fetch failed:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchPortfolio()
    const id = setInterval(fetchPortfolio, 60000)
    return () => clearInterval(id)
  }, [fetchPortfolio])

  if (loading) return <div className="loading">Loading portfolio...</div>

  const portfolio = data || {}
  console.log('PORTFOLIO DEBUG', portfolio)
  const positions = portfolio?.positions || []
  const totalValue = getDisplayCapital(portfolio)
  const totalPnl = safeNumber(data?.total_unrealized_pnl, 0)
  const returnPct = safeNumber(data?.return_pct, 0)
  const costBasis = safeNumber(data?.total_cost_basis ?? data?.starting_value, 0)
  const totalDayPnl = safeNumber(data?.total_day_pnl_pct, 0)
  const isPositive = totalPnl >= 0
  const isDayPositive = totalDayPnl >= 0

  return (
    <div>
      {/* Portfolio Summary */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <h2>SBO Plus AI Portfolio</h2>
          <span className="card-badge" style={{ background: '#fbbf24', color: '#000' }}>LIVE</span>
        </div>
        <div style={{ display: 'flex', gap: 32, padding: '12px 0', flexWrap: 'wrap' }}>
          <div>
            <div style={{ color: '#94a3b8', fontSize: '.8em', marginBottom: 4 }}>MARKET VALUE</div>
            <div style={{ color: '#e2e8f0', fontWeight: 700, fontSize: '1.4em', fontFamily: 'JetBrains Mono, monospace' }}>
              {formatMoney(totalValue)}
            </div>
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: '.8em', marginBottom: 4 }}>COST BASIS</div>
            <div style={{ color: '#94a3b8', fontWeight: 700, fontSize: '1.4em', fontFamily: 'JetBrains Mono, monospace' }}>
              {formatMoney(costBasis)}
            </div>
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: '.8em', marginBottom: 4 }}>UNREALIZED P&L</div>
            <div style={{ color: isPositive ? '#00e676' : '#ef4444', fontWeight: 700, fontSize: '1.4em', fontFamily: 'JetBrains Mono, monospace' }}>
              {isPositive ? '+' : ''}{formatMoney(totalPnl)}
            </div>
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: '.8em', marginBottom: 4 }}>RETURN</div>
            <div style={{ color: isPositive ? '#00e676' : '#ef4444', fontWeight: 700, fontSize: '1.4em', fontFamily: 'JetBrains Mono, monospace' }}>
              {formatPercent(returnPct, 2, true)}
            </div>
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: '.8em', marginBottom: 4 }}>DAY P&L</div>
            <div style={{ color: isDayPositive ? '#00e676' : '#ef4444', fontWeight: 700, fontSize: '1.4em', fontFamily: 'JetBrains Mono, monospace' }}>
              {formatPercent(totalDayPnl, 2, true)}
            </div>
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: '.8em', marginBottom: 4 }}>POSITIONS</div>
            <div style={{ color: '#e2e8f0', fontWeight: 700, fontSize: '1.4em' }}>
              {positions.length}
            </div>
          </div>
        </div>
        {lastUpdated && (
          <div style={{ color: '#64748b', fontSize: '.75em', paddingTop: 4 }}>
            Updated {lastUpdated.toLocaleTimeString('en-US', { timeZone: 'America/Phoenix', hour: 'numeric', minute: '2-digit', hour12: true })}
          </div>
        )}
      </div>

      {/* Positions Table */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <h2>Holdings</h2>
          <span className="card-badge">{positions.length}</span>
          {positions.length > 0 && (
            <button
              className="ib-chart-all-btn"
              onClick={() => {
                const syms = positions.map(p => p.symbol).join(',')
                window.open(`${IB}/ib_multichart.html?symbols=${syms}&tf=D`, '_blank')
              }}
            >
              📊 Chart All Positions
            </button>
          )}
        </div>
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
        {positions.length === 0 ? (
          <div className="empty-state">No open positions.</div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table className="lb-positions-table" style={{ width: '100%' }}>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th className="num">Shares</th>
                  <th className="num">Avg Cost</th>
                  <th className="num">Price</th>
                  <th className="num">Mkt Value</th>
                  <th className="num">P&L</th>
                  <th className="num">P&L %</th>
                  <th className="num">Day P&L %</th>
                </tr>
              </thead>
              <tbody>
                {positions
                  .sort((a, b) => (b.market_value || 0) - (a.market_value || 0))
                  .map((p, i) => {
                    const pnl = safeNumber(p.unrealized_pnl, 0)
                    const pnlPct = safeNumber(p.unrealized_pnl_pct, 0)
                    const dayPct = safeNumber(p.day_change_pct, 0)
                    const isPos = pnl >= 0
                    const isDayPos = dayPct >= 0
                    return (
                      <tr key={i} style={{ cursor: 'pointer' }} onClick={() => setChartSymbol(p.symbol)}>
                        <td>
                          <strong>{p.symbol}</strong>
                          <button
                            className="ib-chart-btn"
                            onClick={(e) => { e.stopPropagation(); window.open(`${IB}/ib_chart.html?symbol=${p.symbol}`, '_blank') }}
                            title={`Chart ${p.symbol}`}
                          >📈</button>
                        </td>
                        <td className="num mono">{p.qty}</td>
                        <td className="num mono">{formatMoney(safeNumber(p.avg_price, 0))}</td>
                        <td className="num mono">{formatMoney(safeNumber(p.current_price ?? p.avg_price, 0))}</td>
                        <td className="num mono">{formatMoney(safeNumber(p.market_value, 0))}</td>
                        <td className={`num mono ${isPos ? 'positive' : 'negative'}`}>
                          {isPos ? '+' : ''}{formatMoney(pnl)}
                        </td>
                        <td className={`num mono ${isPos ? 'positive' : 'negative'}`}>
                          {formatPercent(pnlPct, 1, true)}
                        </td>
                        <td className={`num mono ${isDayPos ? 'positive' : 'negative'}`} style={{ fontWeight: 600 }}>
                          {formatPercent(dayPct, 2, true)}
                        </td>
                      </tr>
                    )
                  })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
