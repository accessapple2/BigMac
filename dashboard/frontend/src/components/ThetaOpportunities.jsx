import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'

const STRATEGY_META = {
  'Iron Condor':   { color: '#a78bfa', bg: '#2e1065', border: '#7c3aed' },
  'Credit Spread': { color: '#60a5fa', bg: '#1e3a5f', border: '#2563eb' },
  'Short Strangle':{ color: '#f97316', bg: '#431407', border: '#c2410c' },
  'Covered Call':  { color: '#34d399', bg: '#052e16', border: '#059669' },
  'Avoid':         { color: '#f87171', bg: '#2d0a0a', border: '#b91c1c' },
  'Monitor':       { color: '#94a3b8', bg: '#1e293b', border: '#475569' },
}

function getStrategyMeta(strategy) {
  if (!strategy) return STRATEGY_META['Monitor']
  for (const key of Object.keys(STRATEGY_META)) {
    if (strategy.includes(key)) return STRATEGY_META[key]
  }
  return STRATEGY_META['Monitor']
}

function StrategyBadge({ strategy }) {
  const parts = (strategy || '').split(',').map(s => s.trim()).filter(Boolean)
  if (!parts.length) return null
  return (
    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
      {parts.map((s, i) => {
        const meta = getStrategyMeta(s)
        return (
          <span key={i} style={{
            fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 4,
            background: meta.bg, color: meta.color, border: `1px solid ${meta.border}`,
            whiteSpace: 'nowrap',
          }}>
            {s}
          </span>
        )
      })}
    </div>
  )
}

function ThetaScoreBar({ score }) {
  const pct = (score / 10) * 100
  const color = score >= 7 ? '#a78bfa' : score >= 5 ? '#60a5fa' : score >= 3 ? '#34d399' : '#64748b'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{ width: 56, height: 6, background: '#1e293b', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 3, transition: 'width 0.3s' }} />
      </div>
      <span style={{ fontSize: 11, fontFamily: 'monospace', color, fontWeight: 700 }}>{score}/10</span>
    </div>
  )
}

function IVRankBar({ ivRank }) {
  const pct = Math.min(100, ivRank)
  const color = ivRank >= 70 ? '#f97316' : ivRank >= 50 ? '#eab308' : ivRank >= 30 ? '#60a5fa' : '#64748b'
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ width: 50, height: 4, background: '#1e293b', borderRadius: 2, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 2 }} />
      </div>
      <span style={{ fontSize: 10, fontFamily: 'monospace', color, fontWeight: 700 }}>{ivRank?.toFixed(0)}%</span>
    </div>
  )
}

function OppRow({ opp }) {
  const [expanded, setExpanded] = useState(false)
  const earnings = opp.earnings_warning
  const rangebound = opp.is_range_bound
  const avoid = (opp.strategy_type || '').startsWith('Avoid')

  return (
    <div
      onClick={() => setExpanded(e => !e)}
      style={{
        padding: '12px 16px',
        borderBottom: '1px solid #1e293b',
        borderLeft: `3px solid ${avoid ? '#b91c1c' : earnings ? '#b45309' : '#7c3aed'}`,
        cursor: 'pointer',
        background: expanded ? 'rgba(124,58,237,0.04)' : 'transparent',
      }}
    >
      {/* Main row */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
        <span style={{ fontSize: 15, fontWeight: 800, fontFamily: 'monospace', minWidth: 52 }}>
          ⏱ {opp.ticker}
        </span>
        <ThetaScoreBar score={opp.theta_score} />
        <IVRankBar ivRank={opp.iv_rank} />
        <div style={{ flex: 1 }}>
          <StrategyBadge strategy={opp.strategy_type} />
        </div>
        {earnings && (
          <span style={{
            fontSize: 10, fontWeight: 700, padding: '1px 7px', borderRadius: 4,
            background: '#451a03', color: '#f59e0b', border: '1px solid #b45309',
          }}>
            ⚠ Earnings {opp.earnings_date}
          </span>
        )}
        {rangebound && !earnings && (
          <span style={{
            fontSize: 10, fontWeight: 600, padding: '1px 6px', borderRadius: 4,
            background: '#052e16', color: '#34d399', border: '1px solid #059669',
          }}>
            ↔ Range-Bound
          </span>
        )}
        <span style={{ fontSize: 11, color: '#475569', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>
          ${opp.spot_price?.toFixed(2)}
        </span>
      </div>

      {/* Expanded details */}
      {expanded && (
        <div style={{
          marginTop: 10, padding: '10px 0',
          borderTop: '1px solid #1e293b',
          display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '8px 16px',
          fontSize: 12, color: '#94a3b8', fontFamily: 'monospace',
        }}>
          <div><span style={{ color: '#64748b' }}>IV Rank: </span>
            <span style={{ color: '#f97316', fontWeight: 700 }}>{opp.iv_rank?.toFixed(1)}%</span>
          </div>
          <div><span style={{ color: '#64748b' }}>IV Level: </span>
            <span style={{ color: '#e2e8f0' }}>{opp.current_iv?.toFixed(1)}%</span>
          </div>
          <div><span style={{ color: '#64748b' }}>IV Pctile: </span>
            <span style={{ color: '#e2e8f0' }}>{opp.iv_percentile?.toFixed(0)}%</span>
          </div>
          <div><span style={{ color: '#64748b' }}>Expiry: </span>
            <span style={{ color: '#e2e8f0' }}>{opp.expiration} ({opp.dte} DTE)</span>
          </div>
          {opp.short_strike_call && (
            <div><span style={{ color: '#64748b' }}>Short Call: </span>
              <span style={{ color: '#f87171' }}>${opp.short_strike_call?.toFixed(2)}</span>
            </div>
          )}
          {opp.short_strike_put && (
            <div><span style={{ color: '#64748b' }}>Short Put: </span>
              <span style={{ color: '#4ade80' }}>${opp.short_strike_put?.toFixed(2)}</span>
            </div>
          )}
          {opp.long_strike_call && (
            <div><span style={{ color: '#64748b' }}>Long Call: </span>
              <span style={{ color: '#94a3b8' }}>${opp.long_strike_call?.toFixed(2)}</span>
            </div>
          )}
          {opp.long_strike_put && (
            <div><span style={{ color: '#64748b' }}>Long Put: </span>
              <span style={{ color: '#94a3b8' }}>${opp.long_strike_put?.toFixed(2)}</span>
            </div>
          )}
          <div><span style={{ color: '#64748b' }}>Daily θ: </span>
            <span style={{ color: '#a78bfa', fontWeight: 700 }}>
              ${opp.estimated_daily_theta?.toFixed(2)}/contract
            </span>
          </div>
          {opp.max_risk && opp.max_risk > 0 && (
            <div><span style={{ color: '#64748b' }}>Max Risk: </span>
              <span style={{ color: '#f87171' }}>${opp.max_risk?.toFixed(0)}</span>
            </div>
          )}
          {opp.max_risk && opp.estimated_daily_theta > 0 && opp.max_risk > 0 && (
            <div><span style={{ color: '#64748b' }}>θ/Risk: </span>
              <span style={{ color: '#a78bfa' }}>
                {((opp.estimated_daily_theta / opp.max_risk) * 100).toFixed(2)}%/day
              </span>
            </div>
          )}
          <div><span style={{ color: '#64748b' }}>Scan date: </span>
            <span style={{ color: '#475569' }}>{opp.scan_date}</span>
          </div>
        </div>
      )}
    </div>
  )
}

export default function ThetaOpportunities() {
  const [tab, setTab] = useState('live')
  const [minScore, setMinScore] = useState(3)
  const [scanning, setScanning] = useState(false)

  const fetchLive    = useCallback(() => api.getThetaOpportunities(minScore, 50), [minScore])
  const fetchHistory = useCallback(() => api.getThetaHistory(100), [])

  const { data: liveData,    loading: liveLoading,    lastUpdated } = usePolling(fetchLive,    300000)  // 5 min
  const { data: historyData, loading: historyLoading }               = usePolling(fetchHistory, 600000) // 10 min

  const opps    = liveData?.opportunities || []
  const history = historyData?.opportunities || []

  const tabStyle = (id) => ({
    padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
    background: tab === id ? '#7c3aed' : '#1a1a2e',
    color:      tab === id ? '#fff'    : '#94a3b8',
    border: '1px solid #333',
  })

  async function handleScan() {
    setScanning(true)
    try {
      await api.triggerThetaScan()
    } finally {
      setTimeout(() => setScanning(false), 3000)
    }
  }

  return (
    <div>
      {/* Tabs + controls */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <button style={tabStyle('live')} onClick={() => setTab('live')}>
          Live ({opps.length})
        </button>
        <button style={tabStyle('history')} onClick={() => setTab('history')}>
          History
        </button>

        {/* Min score filter */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginLeft: 8 }}>
          <span style={{ fontSize: 11, color: '#64748b' }}>Min score:</span>
          {[1, 3, 5, 7].map(s => (
            <button
              key={s}
              onClick={() => setMinScore(s)}
              style={{
                padding: '3px 8px', borderRadius: 4, fontSize: 11, fontWeight: 700,
                cursor: 'pointer', border: '1px solid',
                background: minScore === s ? '#2e1065' : 'transparent',
                color: minScore === s ? '#a78bfa' : '#64748b',
                borderColor: minScore === s ? '#7c3aed' : '#334155',
              }}
            >
              {s}+
            </button>
          ))}
        </div>

        {/* Scan now button */}
        <button
          onClick={handleScan}
          disabled={scanning}
          style={{
            marginLeft: 'auto', padding: '6px 14px', borderRadius: 6, fontSize: 11,
            fontWeight: 700, cursor: scanning ? 'not-allowed' : 'pointer',
            background: scanning ? '#1e293b' : '#2e1065',
            color: scanning ? '#64748b' : '#a78bfa',
            border: '1px solid #7c3aed', opacity: scanning ? 0.6 : 1,
          }}
        >
          {scanning ? '⏱ Scanning…' : '⏱ Scan Now'}
        </button>
      </div>

      {/* Legend */}
      <div className="card" style={{ marginBottom: 16, padding: '10px 16px' }}>
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 12, color: '#94a3b8' }}>
          <span><span style={{ color: '#a78bfa', fontWeight: 700 }}>⏱</span> Theta opportunity (sell premium)</span>
          <span><span style={{ color: '#f97316', fontWeight: 700 }}>IV Rank</span> = premium richness (higher = fatter premiums)</span>
          <span style={{ color: '#64748b' }}>Score: <span style={{ color: '#34d399' }}>1–4</span> watch · <span style={{ color: '#60a5fa' }}>5–6</span> good · <span style={{ color: '#a78bfa' }}>7–10</span> strong</span>
          <span>Click any row to expand strike + risk details</span>
        </div>
      </div>

      {/* Live tab */}
      {tab === 'live' && (
        <div className="card">
          <div className="card-header">
            <h2>⏱ Theta Opportunities</h2>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <LastUpdated time={lastUpdated} />
              <span className="card-badge">{opps.length} found</span>
            </div>
          </div>
          {liveLoading && opps.length === 0 ? (
            <div className="loading">Scanning for theta opportunities…</div>
          ) : opps.length === 0 ? (
            <div className="empty-state">
              No theta opportunities found (score ≥ {minScore}). Try lowering the minimum score or trigger a scan.
            </div>
          ) : (
            <div className="trade-list">
              {opps.map((o, i) => <OppRow key={i} opp={o} />)}
            </div>
          )}
        </div>
      )}

      {/* History tab */}
      {tab === 'history' && (
        <div className="card">
          <div className="card-header">
            <h2>Theta Scan History</h2>
            <span className="card-badge">{history.length} records</span>
          </div>
          {historyLoading && history.length === 0 ? (
            <div className="loading">Loading history…</div>
          ) : history.length === 0 ? (
            <div className="empty-state">No theta scan history yet.</div>
          ) : (
            <div className="trade-list">
              {history.map((o, i) => <OppRow key={i} opp={o} />)}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
