import React, { useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'

const STATUS_COLORS = {
  favored: { bg: '#052e16', color: '#22c55e', border: '#15803d' },
  neutral: { bg: '#0f172a', color: '#94a3b8', border: '#334155' },
  probation: { bg: '#3b0a0a', color: '#f87171', border: '#b91c1c' },
}

const SELECTION_COLORS = {
  exploit: { bg: '#052e16', color: '#22c55e' },
  explore: { bg: '#1e1b4b', color: '#a5b4fc' },
  forced_explore: { bg: '#3b2f0a', color: '#fbbf24' },
}

function PolicyBadge({ status }) {
  const meta = STATUS_COLORS[status] || STATUS_COLORS.neutral
  return (
    <span style={{
      padding: '2px 8px',
      borderRadius: 999,
      fontSize: 10,
      fontWeight: 800,
      letterSpacing: 0.4,
      textTransform: 'uppercase',
      background: meta.bg,
      color: meta.color,
      border: `1px solid ${meta.border}`,
    }}>
      {status}
    </span>
  )
}

function SelectionBadge({ type }) {
  const meta = SELECTION_COLORS[type] || SELECTION_COLORS.exploit
  return (
    <span style={{
      padding: '2px 6px',
      borderRadius: 6,
      fontSize: 10,
      fontWeight: 700,
      background: meta.bg,
      color: meta.color,
      textTransform: 'uppercase',
    }}>
      {type}
    </span>
  )
}

export default function AndersonDecisionSummary() {
  const fetchSummary = useCallback(() => api.getAndersonDecisionSummary(), [])
  const { data, loading, lastUpdated } = usePolling(fetchSummary, 30000)

  if (loading && !data) return <div className="card"><div className="loading">Loading Anderson decision summary...</div></div>

  const selected = data?.selected_signals || []
  const sourcePolicy = data?.source_policy || {}

  return (
    <div className="card">
      <div className="card-header">
        <h2>Anderson Decision Summary</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <LastUpdated time={lastUpdated} />
          <span className="card-badge">{selected.length} selected</span>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 10, padding: '0 16px 12px' }}>
        {Object.entries(sourcePolicy).map(([bucket, policy]) => (
          <div key={bucket} style={{ background: '#0f172a', border: '1px solid #1e293b', borderRadius: 10, padding: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
              <strong style={{ color: '#e2e8f0', fontSize: 13 }}>{bucket}</strong>
              <PolicyBadge status={policy.status} />
            </div>
            <div style={{ fontSize: 11, color: '#94a3b8', display: 'grid', gap: 4 }}>
              <div>Win rate: {policy.win_rate == null ? 'n/a' : `${(policy.win_rate * 100).toFixed(1)}%`}</div>
              <div>Executed: {policy.executed ?? 0}</div>
              <div>Allocation: {policy.multiplier != null ? `${policy.multiplier.toFixed(2)}x` : 'n/a'}</div>
            </div>
          </div>
        ))}
      </div>

      {selected.length === 0 ? (
        <div className="empty-state">No current Anderson selections. Source policy remains visible above.</div>
      ) : (
        <div style={{ overflowX: 'auto', padding: '0 16px 16px' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ textAlign: 'left', color: '#64748b', borderBottom: '1px solid #1e293b' }}>
                {['Symbol', 'Selection', 'Source', 'Confidence', 'Weighted', 'Status', 'Win Rate', 'Alloc'].map(h => (
                  <th key={h} style={{ padding: '8px 6px' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {selected.map((signal, idx) => (
                <tr key={`${signal.symbol}-${idx}`} style={{ borderBottom: '1px solid #0f172a' }}>
                  <td style={{ padding: '8px 6px', fontWeight: 800, color: '#e2e8f0' }}>{signal.symbol}</td>
                  <td style={{ padding: '8px 6px' }}><SelectionBadge type={signal.selection_type} /></td>
                  <td style={{ padding: '8px 6px', color: '#94a3b8' }}>{signal.agent}</td>
                  <td style={{ padding: '8px 6px', fontFamily: 'monospace', color: '#e2e8f0' }}>{(signal.confidence || 0).toFixed(3)}</td>
                  <td style={{ padding: '8px 6px', fontFamily: 'monospace', color: '#e2e8f0' }}>{(signal.weighted_confidence || 0).toFixed(3)}</td>
                  <td style={{ padding: '8px 6px' }}><PolicyBadge status={signal.status} /></td>
                  <td style={{ padding: '8px 6px', color: '#94a3b8' }}>{signal.win_rate == null ? 'n/a' : `${(signal.win_rate * 100).toFixed(1)}%`}</td>
                  <td style={{ padding: '8px 6px', color: '#94a3b8' }}>{signal.allocation_multiplier != null ? `${signal.allocation_multiplier.toFixed(2)}x` : 'n/a'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
