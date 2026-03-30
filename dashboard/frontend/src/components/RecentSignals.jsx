import React, { useCallback, useMemo, useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { timeAgo, formatTimeAZ } from '../utils/time'
import { LastUpdated } from './AutoRefreshToggle'
import QuorumPanel from './QuorumPanel'

const PROVIDER_COLORS = {
  anthropic: '#22c55e',
  openai: '#22c55e',
  google: '#3b82f6',
  xai: '#ef4444',
  ollama: '#94a3b8',
}

const TF_META = {
  SCALP:    { label: 'S',  full: 'Scalp',    title: 'Scalp — intraday, < 1 day',   bg: '#1e3a5f', color: '#60a5fa', border: '#2563eb' },
  SWING:    { label: 'SW', full: 'Swing',    title: 'Swing — 2–10 days',            bg: '#1a3327', color: '#34d399', border: '#059669' },
  POSITION: { label: 'P',  full: 'Position', title: 'Position — 10+ days',          bg: '#3b1f5e', color: '#c084fc', border: '#7c3aed' },
}

const TF_FILTERS = ['All', 'SCALP', 'SWING', 'POSITION']

// Colorblind-safe status indicators (shape + color, never color alone)
const STATUS_META = {
  EXECUTED: { icon: '✓', label: 'Executed', bg: '#14532d', color: '#86efac', border: '#16a34a', title: 'Trade was executed' },
  REJECTED: { icon: '✕', label: 'Rejected', bg: '#450a0a', color: '#fca5a5', border: '#dc2626', title: 'Trade was rejected by risk controls' },
  PENDING:  { icon: '◷', label: 'Pending',  bg: '#1c1917', color: '#a8a29e', border: '#57534e', title: 'Signal pending execution' },
  SKIPPED:  { icon: '⊘', label: 'Skipped',  bg: '#1e1b4b', color: '#a5b4fc', border: '#4338ca', title: 'HOLD — no trade taken' },
}

function StatusBadge({ status, reason }) {
  const meta = STATUS_META[status] || STATUS_META.PENDING
  return (
    <span title={reason ? `${meta.title}: ${reason}` : meta.title} style={{
      fontSize: 10, fontWeight: 800, padding: '2px 6px', borderRadius: 4,
      background: meta.bg, color: meta.color, border: `1px solid ${meta.border}`,
      fontFamily: 'monospace', letterSpacing: 0.5, whiteSpace: 'nowrap',
      display: 'inline-flex', alignItems: 'center', gap: 3,
    }}>
      <span style={{ fontSize: 11 }}>{meta.icon}</span>{meta.label}
    </span>
  )
}

function TimeframeBadge({ tf }) {
  const meta = TF_META[tf] || TF_META.SWING
  return (
    <span title={meta.title} style={{
      fontSize: 10, fontWeight: 800, padding: '1px 6px', borderRadius: 4,
      background: meta.bg, color: meta.color, border: `1px solid ${meta.border}`,
      fontFamily: 'monospace', letterSpacing: 0.5,
    }}>
      {meta.label}
    </span>
  )
}

export default function RecentSignals({ filterPlayer, onFilterPlayer }) {
  const [tfFilter, setTfFilter] = useState('All')
  const [quorumTicker, setQuorumTicker] = useState(null)

  const fetchSignals = useCallback(
    () => api.getRecentSignals(50, undefined, tfFilter === 'All' ? undefined : tfFilter),
    [tfFilter]
  )
  const { data: signals, loading, lastUpdated } = usePolling(fetchSignals, 60000)

  if (loading && !signals) return <div className="loading">Loading signals...</div>

  const allSignals = signals || []

  // Unique players from current data
  const players = useMemo(() => {
    const seen = new Map()
    allSignals.forEach(s => {
      if (s.player_id && !seen.has(s.player_id))
        seen.set(s.player_id, { id: s.player_id, name: s.display_name, provider: s.provider })
    })
    return Array.from(seen.values())
  }, [allSignals])

  const visibleSignals = filterPlayer ? allSignals.filter(s => s.player_id === filterPlayer) : allSignals

  return (
    <>
      {quorumTicker && <QuorumPanel ticker={quorumTicker} onClose={() => setQuorumTicker(null)} />}
      <div className="card">
        <div className="card-header">
          <h2>AI Signals Feed</h2>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <LastUpdated time={lastUpdated} />
            <span className="card-badge">{visibleSignals.length} signals</span>
          </div>
        </div>

        {/* Player filter pills */}
        {players.length > 0 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, padding: '8px 16px', borderBottom: '1px solid #1e293b', alignItems: 'center' }}>
            <span style={{ fontSize: 10, fontWeight: 700, color: '#475569', letterSpacing: 1, marginRight: 2 }}>PLAYER</span>
            <button
              onClick={() => onFilterPlayer?.(null)}
              style={{
                padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                cursor: 'pointer', border: '1px solid',
                background: !filterPlayer ? '#1e293b' : 'transparent',
                color: !filterPlayer ? '#e2e8f0' : '#64748b',
                borderColor: !filterPlayer ? '#475569' : '#334155',
              }}
            >
              All
            </button>
            {players.map(p => {
              const active = filterPlayer === p.id
              const color = PROVIDER_COLORS[p.provider] || '#94a3b8'
              return (
                <button
                  key={p.id}
                  onClick={() => onFilterPlayer?.(active ? null : p.id)}
                  style={{
                    padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                    cursor: 'pointer', border: `1px solid ${active ? color : '#334155'}`,
                    background: active ? `${color}22` : 'transparent',
                    color: active ? color : '#64748b',
                    display: 'inline-flex', alignItems: 'center', gap: 5,
                  }}
                >
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: color, flexShrink: 0 }} />
                  {p.name}
                </button>
              )
            })}
          </div>
        )}

        {/* Timeframe filter */}
        <div style={{ display: 'flex', gap: 6, padding: '8px 16px', borderBottom: '1px solid #1e293b' }}>
          {TF_FILTERS.map(f => {
            const meta = f !== 'All' ? TF_META[f] : null
            const active = tfFilter === f
            return (
              <button
                key={f}
                onClick={() => setTfFilter(f)}
                style={{
                  padding: '4px 12px', borderRadius: 6, fontSize: 11, fontWeight: 700,
                  cursor: 'pointer', border: '1px solid',
                  background: active ? (meta ? meta.bg : '#1e293b') : 'transparent',
                  color: active ? (meta ? meta.color : '#e2e8f0') : '#64748b',
                  borderColor: active ? (meta ? meta.border : '#475569') : '#334155',
                }}
              >
                {f === 'All' ? 'All' : `${TF_META[f].label} ${TF_META[f].full}`}
              </button>
            )
          })}
        </div>

        {visibleSignals.length === 0 ? (
          <div className="empty-state">
            {filterPlayer
              ? `No signals from ${players.find(p => p.id === filterPlayer)?.name ?? filterPlayer}.`
              : 'No signals yet. Waiting for AI analysis.'}
          </div>
        ) : (
          <div className="trade-list" style={{ maxHeight: 600 }}>
            {visibleSignals.map((s, i) => (
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
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                    <StatusBadge status={s.execution_status || 'PENDING'} reason={s.rejection_reason} />
                    <span className={`trade-action ${s.signal.toLowerCase()}`}>{s.signal}</span>
                    <strong>{s.symbol}</strong>
                    {s.timeframe && <TimeframeBadge tf={s.timeframe} />}
                    <span className="trade-by" style={{ color: PROVIDER_COLORS[s.provider] || '#94a3b8' }}>
                      {s.display_name}
                    </span>
                  </div>
                  {s.execution_status === 'REJECTED' && s.rejection_reason && (
                    <div style={{ fontSize: 11, color: '#fca5a5', marginTop: 2, fontStyle: 'italic' }}>
                      ✕ {s.rejection_reason}
                    </div>
                  )}
                  {s.reasoning && (
                    <div className="trade-reasoning">{s.reasoning.substring(0, 200)}</div>
                  )}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <button
                    title={`Request Quorum on ${s.symbol}`}
                    onClick={() => setQuorumTicker(s.symbol)}
                    style={{
                      background: 'none', border: '1px solid #334155', borderRadius: 6,
                      color: '#94a3b8', fontSize: 14, cursor: 'pointer', padding: '2px 7px',
                      lineHeight: 1.4,
                    }}
                  >⚖</button>
                  <div className="trade-time">
                    {formatTimeAZ(s.created_at)} · {timeAgo(s.created_at)}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </>
  )
}
