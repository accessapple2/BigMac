import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { timeAgo, formatTimeAZ } from '../utils/time'
import { LastUpdated } from './AutoRefreshToggle'

const SIGNAL_LABELS = {
  volume_spike:  'Vol Spike',
  price_impulse: 'Price Impulse',
  breakout:      'Breakout',
}

const DIR_META = {
  bullish: { icon: '▲', label: 'Bullish', border: '#22c55e', bg: '#052e16', text: '#22c55e' },
  bearish: { icon: '▼', label: 'Bearish', border: '#ef4444', bg: '#2d0a0a', text: '#ef4444' },
}

function StrengthBar({ score }) {
  const pct = (score / 10) * 100
  const color = score >= 7 ? '#ef4444' : score >= 4 ? '#eab308' : '#22c55e'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{ width: 60, height: 6, background: '#1e293b', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 3, transition: 'width 0.3s' }} />
      </div>
      <span style={{ fontSize: 11, fontFamily: 'monospace', color, fontWeight: 700 }}>{score}/10</span>
    </div>
  )
}

function SignalTags({ types }) {
  if (!types) return null
  return (
    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginTop: 4 }}>
      {types.split(',').map(t => t.trim()).filter(Boolean).map(t => (
        <span key={t} style={{
          fontSize: 10, fontWeight: 700, padding: '1px 6px', borderRadius: 3,
          background: '#1e293b', color: '#94a3b8', border: '1px solid #334155',
          textTransform: 'uppercase', letterSpacing: 0.5,
        }}>
          {SIGNAL_LABELS[t] || t}
        </span>
      ))}
    </div>
  )
}

export default function ImpulseAlerts() {
  const [tab, setTab] = useState('active')

  const fetchActive = useCallback(() => api.getImpulseActive(2), [])
  const fetchRecent = useCallback(() => api.getImpulseRecent(100), [])

  const { data: activeData, loading: activeLoading, lastUpdated } = usePolling(fetchActive, 60000)
  const { data: recentData, loading: recentLoading }               = usePolling(fetchRecent, 120000)

  const active = activeData?.alerts || []
  const recent = recentData?.alerts || []

  const tabStyle = (id) => ({
    padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
    background: tab === id ? '#00d4aa' : '#1a1a2e',
    color:      tab === id ? '#0a0a1a' : '#94a3b8',
    border: '1px solid #333',
  })

  function AlertRow({ alert, compact = false }) {
    const dir = DIR_META[alert.direction] || DIR_META.bullish
    return (
      <div style={{
        padding: compact ? '10px 14px' : '14px 16px',
        borderBottom: '1px solid #1e293b',
        borderLeft: `3px solid ${dir.border}`,
        display: 'flex', alignItems: 'flex-start', gap: 12,
        background: 'transparent',
      }}>
        {/* Direction icon */}
        <div style={{ minWidth: 28, paddingTop: 2 }}>
          <span style={{ fontSize: 20, fontWeight: 800, color: dir.text, lineHeight: 1 }}>{dir.icon}</span>
        </div>

        {/* Main content */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 3 }}>
            <span style={{ fontWeight: 800, fontFamily: 'monospace', fontSize: 15 }}>{alert.ticker}</span>
            <span style={{
              fontSize: 11, fontWeight: 700, padding: '1px 8px', borderRadius: 4,
              background: dir.bg, color: dir.text, border: `1px solid ${dir.border}`,
            }}>{dir.icon} {dir.label}</span>
            <StrengthBar score={alert.strength_score} />
          </div>

          <div style={{ fontSize: 12, color: '#94a3b8', fontFamily: 'monospace', marginBottom: 3 }}>
            Vol {alert.volume_ratio?.toFixed(1)}×
            {alert.atr_ratio > 0 ? ` · ATR ${alert.atr_ratio?.toFixed(1)}×` : ''}
            {alert.entry_zone ? ` · Entry ${alert.entry_zone}` : ''}
            {alert.stop_level ? ` · Stop $${Number(alert.stop_level).toFixed(2)}` : ''}
          </div>
          <SignalTags types={alert.signal_types} />
        </div>

        {/* Timestamp */}
        <div style={{ fontSize: 11, color: '#64748b', textAlign: 'right', whiteSpace: 'nowrap' }}>
          {alert.detected_at ? timeAgo(alert.detected_at) : ''}
        </div>
      </div>
    )
  }

  return (
    <div>
      {/* Tabs */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        <button style={tabStyle('active')} onClick={() => setTab('active')}>
          Active ({active.length})
        </button>
        <button style={tabStyle('history')} onClick={() => setTab('history')}>
          History
        </button>
      </div>

      {/* Legend */}
      <div className="card" style={{ marginBottom: 16, padding: '10px 16px' }}>
        <div style={{ display: 'flex', gap: 20, flexWrap: 'wrap', fontSize: 12, color: '#94a3b8' }}>
          <span><span style={{ color: '#22c55e', fontWeight: 700 }}>▲</span> Bullish impulse</span>
          <span><span style={{ color: '#ef4444', fontWeight: 700 }}>▼</span> Bearish impulse</span>
          <span style={{ color: '#64748b' }}>Strength: <span style={{ color: '#22c55e' }}>1–3</span> low · <span style={{ color: '#eab308' }}>4–6</span> moderate · <span style={{ color: '#ef4444' }}>7–10</span> high</span>
        </div>
      </div>

      {tab === 'active' && (
        <div className="card">
          <div className="card-header">
            <h2>Active Impulse Alerts</h2>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <LastUpdated time={lastUpdated} />
              <span className="card-badge">{active.length} active</span>
            </div>
          </div>
          {activeLoading && active.length === 0 ? (
            <div className="loading">Checking watchlist for impulses…</div>
          ) : active.length === 0 ? (
            <div className="empty-state">No active impulse alerts in the last 2 hours.</div>
          ) : (
            <div className="trade-list">
              {active.map((a, i) => <AlertRow key={i} alert={a} />)}
            </div>
          )}
        </div>
      )}

      {tab === 'history' && (
        <div className="card">
          <div className="card-header">
            <h2>Impulse Alert History</h2>
            <span className="card-badge">{recent.length} alerts</span>
          </div>
          {recentLoading && recent.length === 0 ? (
            <div className="loading">Loading history…</div>
          ) : recent.length === 0 ? (
            <div className="empty-state">No impulse alerts recorded yet.</div>
          ) : (
            <div className="trade-list">
              {recent.map((a, i) => <AlertRow key={i} alert={a} />)}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
