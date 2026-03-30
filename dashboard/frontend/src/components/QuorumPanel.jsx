import React, { useState, useEffect, useRef } from 'react'
import { api } from '../api/client'

const VOTE_COLORS = { BUY: '#22c55e', SELL: '#ef4444', HOLD: '#eab308' }
const CONSENSUS_BG = { BUY: '#052e16', SELL: '#2d0a0a', HOLD: '#1c1a06' }

export default function QuorumPanel({ ticker, onClose }) {
  const [quorumId, setQuorumId] = useState(null)
  const [status, setStatus] = useState(null)
  const [error, setError] = useState(null)
  const pollRef = useRef(null)

  useEffect(() => {
    api.startQuorum(ticker)
      .then(r => {
        if (r.error) { setError(r.error); return }
        setQuorumId(r.quorum_id)
      })
      .catch(e => setError(e.message))
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [ticker])

  useEffect(() => {
    if (!quorumId) return
    const poll = async () => {
      try {
        const s = await api.getQuorumStatus(quorumId)
        setStatus(s)
        if (s.done) clearInterval(pollRef.current)
      } catch (e) {
        setError(e.message)
        clearInterval(pollRef.current)
      }
    }
    poll()
    pollRef.current = setInterval(poll, 2000)
    return () => clearInterval(pollRef.current)
  }, [quorumId])

  const votes = status?.votes || []
  const tally = status?.tally || { BUY: 0, SELL: 0, HOLD: 0, total: 0 }
  const consensus = status?.consensus || 'HOLD'
  const done = status?.done || false

  return (
    <div style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', zIndex: 1000,
      display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16,
    }}
      onClick={e => { if (e.target === e.currentTarget) onClose() }}
    >
      <div style={{
        background: '#0f172a', border: '1px solid #1e293b', borderRadius: 12,
        width: '100%', maxWidth: 540, maxHeight: '85vh', overflow: 'hidden',
        display: 'flex', flexDirection: 'column',
      }}>
        {/* Header */}
        <div style={{ padding: '16px 20px', borderBottom: '1px solid #1e293b', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <span style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1 }}>Quorum Vote</span>
            <h2 style={{ margin: 0, fontSize: 20, fontFamily: 'monospace', color: '#e2e8f0' }}>{ticker}</h2>
          </div>
          <button onClick={onClose} style={{ background: 'none', border: 'none', color: '#64748b', fontSize: 20, cursor: 'pointer', lineHeight: 1 }}>✕</button>
        </div>

        {error && (
          <div style={{ padding: '12px 20px', color: '#ef4444', fontSize: 13 }}>Error: {error}</div>
        )}

        {/* Tally bars */}
        {tally.total > 0 && (
          <div style={{ padding: '16px 20px', borderBottom: '1px solid #1e293b' }}>
            {['BUY', 'SELL', 'HOLD'].map(v => {
              const count = tally[v] || 0
              const pct = tally.total > 0 ? Math.round((count / tally.total) * 100) : 0
              return (
                <div key={v} style={{ marginBottom: 8 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                    <span style={{ fontSize: 11, fontWeight: 700, color: VOTE_COLORS[v] }}>{v}</span>
                    <span style={{ fontSize: 11, color: '#94a3b8', fontFamily: 'monospace' }}>{count} ({pct}%)</span>
                  </div>
                  <div style={{ height: 6, background: '#1e293b', borderRadius: 3, overflow: 'hidden' }}>
                    <div style={{
                      height: '100%', width: `${pct}%`, background: VOTE_COLORS[v],
                      borderRadius: 3, transition: 'width 0.4s ease',
                    }} />
                  </div>
                </div>
              )
            })}
          </div>
        )}

        {/* Consensus banner */}
        {done && tally.total > 0 && (
          <div style={{
            margin: '0 20px 8px', marginTop: 12, padding: '10px 16px', borderRadius: 8,
            background: CONSENSUS_BG[consensus] || '#0f172a',
            border: `1px solid ${VOTE_COLORS[consensus]}`,
            display: 'flex', alignItems: 'center', gap: 12,
          }}>
            <span style={{ fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1 }}>Consensus</span>
            <span style={{ fontSize: 20, fontWeight: 800, fontFamily: 'monospace', color: VOTE_COLORS[consensus] }}>{consensus}</span>
            <span style={{ fontSize: 12, color: '#94a3b8', marginLeft: 'auto' }}>{tally.total} votes</span>
          </div>
        )}

        {/* Loading indicator */}
        {!done && (
          <div style={{ padding: '8px 20px', display: 'flex', alignItems: 'center', gap: 8 }}>
            <div style={{
              width: 8, height: 8, borderRadius: '50%', background: '#00d4aa',
              animation: 'pulse 1s infinite',
            }} />
            <span style={{ fontSize: 12, color: '#64748b' }}>
              Polling {votes.length} model{votes.length !== 1 ? 's' : ''} so far…
            </span>
          </div>
        )}

        {/* Individual votes */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '8px 20px 16px' }}>
          {votes.length === 0 && !error && (
            <div style={{ color: '#64748b', fontSize: 13, textAlign: 'center', paddingTop: 20 }}>
              Waiting for models to respond…
            </div>
          )}
          {votes.map((v, i) => (
            <div key={i} style={{
              padding: '10px 12px', marginBottom: 6, borderRadius: 8, background: '#1e293b',
              borderLeft: `3px solid ${VOTE_COLORS[v.vote] || '#94a3b8'}`,
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: v.reasoning ? 4 : 0 }}>
                <span style={{ fontSize: 12, fontWeight: 700, color: VOTE_COLORS[v.vote], fontFamily: 'monospace', minWidth: 36 }}>{v.vote}</span>
                <span style={{ fontSize: 12, color: '#e2e8f0', fontWeight: 600 }}>{v.display_name || v.player_id}</span>
                {v.confidence != null && (
                  <span style={{ marginLeft: 'auto', fontSize: 11, color: '#64748b', fontFamily: 'monospace' }}>{Math.round(v.confidence)}%</span>
                )}
              </div>
              {v.reasoning && (
                <div style={{ fontSize: 11, color: '#94a3b8', lineHeight: 1.4 }}>{v.reasoning}</div>
              )}
            </div>
          ))}
        </div>
      </div>

      <style>{`
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
      `}</style>
    </div>
  )
}
