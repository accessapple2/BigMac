import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { timeAgo } from '../utils/time'

// Colorblind-safe: icon + text label, not just color
const SIGNAL_META = {
  '200 SMA Bounce':    { icon: '◆▲', label: 'Bounce',    bg: '#052e16', border: '#22c55e', text: '#22c55e' },
  '200 SMA Breakdown': { icon: '▼!',  label: 'Breakdown', bg: '#2d0a0a', border: '#ef4444', text: '#ef4444' },
  '200 SMA Reclaim':   { icon: '▲!',  label: 'Reclaim',   bg: '#052e16', border: '#00d4aa', text: '#00d4aa' },
}

function positionMeta(above, distPct) {
  const abs = Math.abs(distPct)
  if (abs <= 2) return { icon: '◆', label: 'Testing', color: '#eab308' }
  if (above)    return { icon: '▲', label: 'Above',   color: '#22c55e' }
  return              { icon: '▼', label: 'Below',   color: '#ef4444' }
}

export default function SMADashboard() {
  const [tab, setTab] = useState('watchlist')

  const fetchStatus  = useCallback(() => api.getSmaStatus(), [])
  const fetchSignals = useCallback(() => api.getSmaSignals(50), [])
  const { data: statusData, loading: statusLoading } = usePolling(fetchStatus, 900000)  // 15 min
  const { data: sigData,    loading: sigLoading }    = usePolling(fetchSignals, 60000)

  const stocks  = statusData?.stocks || []
  const signals = sigData?.signals   || []

  const tabStyle = (id) => ({
    padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
    background: tab === id ? '#00d4aa' : '#1a1a2e',
    color:      tab === id ? '#0a0a1a' : '#94a3b8',
    border: '1px solid #333',
  })

  return (
    <div>
      {/* Tab switcher */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        <button style={tabStyle('watchlist')} onClick={() => setTab('watchlist')}>Watchlist Status</button>
        <button style={tabStyle('signals')}   onClick={() => setTab('signals')}>Signal History</button>
      </div>

      {/* Legend */}
      <div className="card" style={{ marginBottom: 16, padding: '10px 16px' }}>
        <div style={{ display: 'flex', gap: 20, flexWrap: 'wrap', fontSize: 12, color: '#94a3b8' }}>
          <span><span style={{ color: '#22c55e', fontWeight: 700 }}>▲</span> Above 200 SMA</span>
          <span><span style={{ color: '#eab308', fontWeight: 700 }}>◆</span> Testing (±2%)</span>
          <span><span style={{ color: '#ef4444', fontWeight: 700 }}>▼</span> Below 200 SMA</span>
          <span><span style={{ color: '#00d4aa', fontWeight: 700 }}>▲!</span> Reclaim signal</span>
          <span><span style={{ color: '#ef4444', fontWeight: 700 }}>▼!</span> Breakdown signal</span>
          <span><span style={{ color: '#22c55e', fontWeight: 700 }}>◆▲</span> Bounce signal</span>
        </div>
      </div>

      {tab === 'watchlist' && (
        <div className="card">
          <div className="card-header">
            <h2>Watchlist — 200 SMA Status</h2>
            <span className="card-badge">{stocks.length} stocks</span>
          </div>
          {statusLoading && stocks.length === 0 ? (
            <div className="loading">Computing 200 SMAs via Yahoo Finance…</div>
          ) : stocks.length === 0 ? (
            <div className="empty-state">No data yet — SMA scan runs every 4 hours.</div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['Symbol', '200 SMA', 'Price', 'Distance', 'Position', 'Signal'].map(h => (
                    <th key={h} style={{ padding: '8px 10px', textAlign: 'left', fontSize: 10,
                      fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {stocks.map(s => {
                  const pos  = positionMeta(s.above_sma200, s.distance_pct)
                  const sig  = s.signal_type ? SIGNAL_META[s.signal_type] : null
                  const dist = s.distance_pct
                  return (
                    <tr key={s.symbol} style={{ borderBottom: '1px solid #1e293b' }}>
                      <td style={{ padding: '10px 10px', fontWeight: 700, fontFamily: 'monospace' }}>{s.symbol}</td>
                      <td style={{ padding: '10px 10px', fontFamily: 'monospace', color: '#94a3b8' }}>
                        ${s.sma_200?.toFixed(2) ?? '—'}
                      </td>
                      <td style={{ padding: '10px 10px', fontFamily: 'monospace' }}>
                        ${s.current_price?.toFixed(2) ?? '—'}
                      </td>
                      <td style={{ padding: '10px 10px', fontFamily: 'monospace',
                        color: dist > 0 ? '#22c55e' : dist < 0 ? '#ef4444' : '#94a3b8' }}>
                        {dist != null ? `${dist > 0 ? '+' : ''}${dist.toFixed(2)}%` : '—'}
                      </td>
                      <td style={{ padding: '10px 10px' }}>
                        <span style={{ fontWeight: 700, color: pos.color, marginRight: 5 }}>{pos.icon}</span>
                        <span style={{ fontSize: 11, color: pos.color }}>{pos.label}</span>
                      </td>
                      <td style={{ padding: '10px 10px' }}>
                        {sig ? (
                          <span style={{
                            display: 'inline-flex', alignItems: 'center', gap: 5,
                            padding: '2px 8px', borderRadius: 4,
                            background: sig.bg, border: `1px solid ${sig.border}`,
                            fontSize: 11, fontWeight: 700, color: sig.text,
                          }}>
                            {sig.icon} {sig.label}
                          </span>
                        ) : (
                          <span style={{ color: '#334155', fontSize: 11 }}>—</span>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
        </div>
      )}

      {tab === 'signals' && (
        <div className="card">
          <div className="card-header">
            <h2>200 SMA Signal History</h2>
            <span className="card-badge">{signals.length} signals</span>
          </div>
          {sigLoading && signals.length === 0 ? (
            <div className="loading">Loading signals…</div>
          ) : signals.length === 0 ? (
            <div className="empty-state">No 200 SMA signals recorded yet.</div>
          ) : (
            <div className="trade-list">
              {signals.map((s, i) => {
                const sig = SIGNAL_META[s.signal_type] || {}
                const dist = s.distance_pct
                return (
                  <div key={i} style={{
                    padding: '12px 16px', borderBottom: '1px solid #1e293b',
                    borderLeft: `3px solid ${sig.border || '#334155'}`,
                    display: 'flex', alignItems: 'flex-start', gap: 12,
                  }}>
                    <div style={{ minWidth: 32, fontWeight: 800, fontSize: 16, color: sig.text || '#94a3b8' }}>
                      {sig.icon || '◆'}
                    </div>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 3 }}>
                        <span style={{ fontWeight: 700, fontFamily: 'monospace', fontSize: 14 }}>{s.ticker}</span>
                        <span style={{
                          fontSize: 11, fontWeight: 700, padding: '1px 7px', borderRadius: 4,
                          background: sig.bg || '#1e293b', color: sig.text || '#94a3b8',
                          border: `1px solid ${sig.border || '#334155'}`,
                        }}>{s.signal_type}</span>
                      </div>
                      <div style={{ fontSize: 12, color: '#94a3b8', fontFamily: 'monospace' }}>
                        Price ${s.current_price?.toFixed(2)} &nbsp;|&nbsp;
                        200 SMA ${s.sma_200_value?.toFixed(2)} &nbsp;|&nbsp;
                        <span style={{ color: dist > 0 ? '#22c55e' : '#ef4444' }}>
                          {dist != null ? `${dist > 0 ? '+' : ''}${dist.toFixed(2)}%` : ''}
                        </span>
                      </div>
                    </div>
                    <div style={{ fontSize: 11, color: '#64748b', whiteSpace: 'nowrap' }}>
                      {s.date}<br />
                      <span style={{ fontSize: 10 }}>{timeAgo(s.detected_at)}</span>
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
