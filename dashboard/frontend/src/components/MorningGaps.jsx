import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'
import { timeAgo } from '../utils/time'

// Gap type metadata
const GAP_TYPE_META = {
  Common:    { color: '#94a3b8', bg: '#1e293b', border: '#475569',
               desc: 'Low volume, range-bound. ~75% fill rate.' },
  Breakaway: { color: '#f97316', bg: '#431407', border: '#c2410c',
               desc: 'High volume, breaks key level. Trend starter. ~15% fill rate.' },
  Runaway:   { color: '#a78bfa', bg: '#2e1065', border: '#7c3aed',
               desc: 'Mid-trend continuation on high volume. ~20% fill rate.' },
  Exhaustion:{ color: '#fbbf24', bg: '#451a03', border: '#d97706',
               desc: 'End of trend, climactic volume. Reversal likely. ~65% fill rate.' },
}

const FILL_STATUS_META = {
  OPEN:    { label: 'OPEN',    color: '#eab308', bg: '#422006', border: '#ca8a04' },
  PARTIAL: { label: 'PARTIAL', color: '#60a5fa', bg: '#1e3a5f', border: '#2563eb' },
  FILLED:  { label: 'FILLED',  color: '#4ade80', bg: '#052e16', border: '#16a34a' },
}

function GapTypeBadge({ type }) {
  const meta = GAP_TYPE_META[type] || GAP_TYPE_META.Common
  return (
    <span title={meta.desc} style={{
      fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4,
      background: meta.bg, color: meta.color, border: `1px solid ${meta.border}`,
      cursor: 'help',
    }}>
      {type}
    </span>
  )
}

function FillStatusBadge({ status }) {
  const meta = FILL_STATUS_META[status] || FILL_STATUS_META.OPEN
  return (
    <span style={{
      fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4,
      background: meta.bg, color: meta.color, border: `1px solid ${meta.border}`,
      fontFamily: 'monospace',
    }}>
      {meta.label}
    </span>
  )
}

function VolumeBar({ ratio }) {
  const pct = Math.min(100, (ratio / 4) * 100)  // cap at 4x for display
  const color = ratio >= 2 ? '#f97316' : ratio >= 1.5 ? '#eab308' : '#64748b'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ width: 40, height: 4, background: '#1e293b', borderRadius: 2, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 2 }} />
      </div>
      <span style={{ fontSize: 10, fontFamily: 'monospace', color, fontWeight: 700 }}>
        {ratio?.toFixed(1)}x
      </span>
    </div>
  )
}

function FillBar({ fillProb }) {
  const color = fillProb >= 60 ? '#4ade80' : fillProb >= 30 ? '#eab308' : '#f87171'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ width: 36, height: 4, background: '#1e293b', borderRadius: 2, overflow: 'hidden' }}>
        <div style={{ width: `${fillProb}%`, height: '100%', background: color, borderRadius: 2 }} />
      </div>
      <span style={{ fontSize: 10, fontFamily: 'monospace', color, fontWeight: 700 }}>
        {fillProb?.toFixed(0)}%
      </span>
    </div>
  )
}

function GapRow({ gap }) {
  const isUp = gap.gap_direction === 'up'
  const dirColor = isUp ? '#4ade80' : '#f87171'
  const dirIcon  = isUp ? '▲' : '▼'

  return (
    <div style={{
      padding: '11px 16px',
      borderBottom: '1px solid #1e293b',
      borderLeft: `3px solid ${dirColor}`,
      display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap',
    }}>
      {/* Direction + ticker */}
      <div style={{ minWidth: 80, display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{ fontSize: 16, fontWeight: 800, color: dirColor, lineHeight: 1 }}>{dirIcon}</span>
        <span style={{ fontWeight: 800, fontFamily: 'monospace', fontSize: 14 }}>{gap.ticker}</span>
      </div>

      {/* Gap % */}
      <span style={{
        fontSize: 14, fontWeight: 800, fontFamily: 'monospace',
        color: dirColor, minWidth: 60,
      }}>
        {gap.gap_pct > 0 ? '+' : ''}{gap.gap_pct?.toFixed(2)}%
      </span>

      {/* Type badge */}
      <GapTypeBadge type={gap.gap_type} />

      {/* Fill probability */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
        <span style={{ fontSize: 10, color: '#64748b' }}>fill</span>
        <FillBar fillProb={gap.fill_probability} />
      </div>

      {/* Volume ratio */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
        <span style={{ fontSize: 10, color: '#64748b' }}>vol</span>
        <VolumeBar ratio={gap.volume_ratio} />
      </div>

      {/* Status */}
      <FillStatusBadge status={gap.fill_status || 'OPEN'} />

      {/* Price context */}
      <div style={{ flex: 1, fontSize: 11, color: '#64748b', fontFamily: 'monospace', textAlign: 'right' }}>
        open ${gap.open_price?.toFixed(2)}
        <span style={{ color: '#475569', marginLeft: 6 }}>
          prev ${gap.prev_close?.toFixed(2)}
        </span>
        {gap.fill_time_minutes && (
          <span style={{ color: '#4ade80', marginLeft: 6 }}>
            ✓ {gap.fill_time_minutes}min
          </span>
        )}
      </div>
    </div>
  )
}

function StatsRow({ stats }) {
  if (!stats || Object.keys(stats).length === 0) return null
  return (
    <div style={{
      display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
      gap: 8, padding: '10px 16px', borderBottom: '1px solid #1e293b',
    }}>
      {Object.entries(stats).map(([type, data]) => {
        const meta = GAP_TYPE_META[type] || GAP_TYPE_META.Common
        return (
          <div key={type} style={{
            padding: '8px 10px', borderRadius: 6,
            background: '#0f172a', border: `1px solid ${meta.border}`,
          }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: meta.color, marginBottom: 4 }}>
              {type}
            </div>
            <div style={{ fontSize: 10, color: '#64748b', lineHeight: 1.6 }}>
              <div>Total: <span style={{ color: '#94a3b8' }}>{data.total}</span></div>
              <div>Fill rate: <span style={{ color: '#94a3b8' }}>{data.fill_rate_pct}%</span></div>
              {data.avg_fill_minutes && (
                <div>Avg fill: <span style={{ color: '#94a3b8' }}>{data.avg_fill_minutes}min</span></div>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}

export default function MorningGaps() {
  const [tab, setTab] = useState('today')
  const [filterType, setFilterType] = useState('All')
  const [scanning, setScanning] = useState(false)

  const fetchToday   = useCallback(() => api.getGapsToday(), [])
  const fetchHistory = useCallback(() => api.getGapsHistory(150), [])
  const fetchStats   = useCallback(() => api.getGapStats(30), [])

  const { data: todayData,   loading: todayLoading,   lastUpdated } = usePolling(fetchToday,   30000)
  const { data: historyData, loading: historyLoading }               = usePolling(fetchHistory, 120000)
  const { data: statsData }                                          = usePolling(fetchStats,   300000)

  const today   = todayData?.gaps   || []
  const history = historyData?.gaps || []
  const stats   = statsData?.stats  || {}

  const GAP_TYPES = ['All', 'Common', 'Breakaway', 'Runaway', 'Exhaustion']

  const filteredToday   = filterType === 'All' ? today   : today.filter(g => g.gap_type === filterType)
  const filteredHistory = filterType === 'All' ? history : history.filter(g => g.gap_type === filterType)

  const tabStyle = (id) => ({
    padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
    background: tab === id ? '#00d4aa' : '#1a1a2e',
    color:      tab === id ? '#0a0a1a' : '#94a3b8',
    border: '1px solid #333',
  })

  async function handleScan() {
    setScanning(true)
    try { await api.triggerGapScan() }
    finally { setTimeout(() => setScanning(false), 3000) }
  }

  const openCount  = today.filter(g => g.fill_status === 'OPEN').length
  const filledCount = today.filter(g => g.fill_status === 'FILLED').length

  return (
    <div>
      {/* Tabs + controls */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <button style={tabStyle('today')} onClick={() => setTab('today')}>
          Today ({today.length})
        </button>
        <button style={tabStyle('history')} onClick={() => setTab('history')}>
          History
        </button>
        <button style={tabStyle('stats')} onClick={() => setTab('stats')}>
          Fill Stats
        </button>

        {/* Type filter */}
        <div style={{ display: 'flex', gap: 4, marginLeft: 8 }}>
          {GAP_TYPES.map(t => {
            const meta = t !== 'All' ? GAP_TYPE_META[t] : null
            const active = filterType === t
            return (
              <button
                key={t}
                onClick={() => setFilterType(t)}
                style={{
                  padding: '3px 8px', borderRadius: 4, fontSize: 10, fontWeight: 700,
                  cursor: 'pointer', border: '1px solid',
                  background: active ? (meta ? meta.bg : '#1e293b') : 'transparent',
                  color: active ? (meta ? meta.color : '#e2e8f0') : '#64748b',
                  borderColor: active ? (meta ? meta.border : '#475569') : '#334155',
                }}
              >
                {t}
              </button>
            )
          })}
        </div>

        {/* Scan now */}
        <button
          onClick={handleScan}
          disabled={scanning}
          style={{
            marginLeft: 'auto', padding: '6px 14px', borderRadius: 6, fontSize: 11,
            fontWeight: 700, cursor: scanning ? 'not-allowed' : 'pointer',
            background: scanning ? '#1e293b' : '#0f2720',
            color: scanning ? '#64748b' : '#00d4aa',
            border: '1px solid #059669', opacity: scanning ? 0.6 : 1,
          }}
        >
          {scanning ? 'Scanning…' : '▶ Scan Now'}
        </button>
      </div>

      {/* Legend */}
      <div className="card" style={{ marginBottom: 16, padding: '10px 16px' }}>
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 11, color: '#94a3b8' }}>
          <span><span style={{ color: '#4ade80', fontWeight: 700 }}>▲</span> Gap Up</span>
          <span><span style={{ color: '#f87171', fontWeight: 700 }}>▼</span> Gap Down</span>
          <span>
            <span style={{ color: '#94a3b8', fontWeight: 700 }}>Common</span> ~75% fill ·{' '}
            <span style={{ color: '#f97316', fontWeight: 700 }}>Breakaway</span> ~15% fill ·{' '}
            <span style={{ color: '#a78bfa', fontWeight: 700 }}>Runaway</span> ~20% fill ·{' '}
            <span style={{ color: '#fbbf24', fontWeight: 700 }}>Exhaustion</span> ~65% fill
          </span>
          <span style={{ color: '#64748b' }}>
            Vol bar: <span style={{ color: '#f97316' }}>≥2x</span> = high volume
          </span>
        </div>
      </div>

      {/* Today tab */}
      {tab === 'today' && (
        <div>
          {/* Summary badges */}
          {today.length > 0 && (
            <div style={{ display: 'flex', gap: 10, marginBottom: 12 }}>
              <span style={{
                padding: '4px 12px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                background: '#422006', color: '#eab308', border: '1px solid #ca8a04',
              }}>
                {openCount} OPEN
              </span>
              {filledCount > 0 && (
                <span style={{
                  padding: '4px 12px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                  background: '#052e16', color: '#4ade80', border: '1px solid #16a34a',
                }}>
                  {filledCount} FILLED
                </span>
              )}
              {today.length - openCount - filledCount > 0 && (
                <span style={{
                  padding: '4px 12px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                  background: '#1e3a5f', color: '#60a5fa', border: '1px solid #2563eb',
                }}>
                  {today.length - openCount - filledCount} PARTIAL
                </span>
              )}
            </div>
          )}

          <div className="card">
            <div className="card-header">
              <h2>Morning Gaps</h2>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <LastUpdated time={lastUpdated} />
                <span className="card-badge">{filteredToday.length} gaps</span>
              </div>
            </div>
            {todayLoading && today.length === 0 ? (
              <div className="loading">Scanning for morning gaps…</div>
            ) : filteredToday.length === 0 ? (
              <div className="empty-state">
                {today.length === 0
                  ? 'No gaps detected today. Market may not be open yet, or all gaps are < 0.5%.'
                  : `No ${filterType} gaps today.`}
              </div>
            ) : (
              <div className="trade-list">
                {filteredToday.map((g, i) => <GapRow key={i} gap={g} />)}
              </div>
            )}
          </div>
        </div>
      )}

      {/* History tab */}
      {tab === 'history' && (
        <div className="card">
          <div className="card-header">
            <h2>Gap History</h2>
            <span className="card-badge">{filteredHistory.length} records</span>
          </div>
          {historyLoading && history.length === 0 ? (
            <div className="loading">Loading gap history…</div>
          ) : filteredHistory.length === 0 ? (
            <div className="empty-state">No gap history yet.</div>
          ) : (
            <div className="trade-list">
              {filteredHistory.map((g, i) => (
                <div key={i} style={{ position: 'relative' }}>
                  <GapRow gap={g} />
                  <span style={{
                    position: 'absolute', top: 6, right: 12,
                    fontSize: 10, color: '#475569',
                  }}>
                    {g.date}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Stats tab */}
      {tab === 'stats' && (
        <div className="card">
          <div className="card-header">
            <h2>Gap Fill Statistics (30 days)</h2>
          </div>
          {Object.keys(stats).length === 0 ? (
            <div className="empty-state">No statistics yet. Data builds up over time.</div>
          ) : (
            <>
              <StatsRow stats={stats} />
              <div style={{ padding: '12px 16px', fontSize: 12, color: '#64748b' }}>
                <p>Fill rate = % of gaps where price returned to previous close level during the same trading day.</p>
                <p>Avg fill time = average minutes from market open to gap fill.</p>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
