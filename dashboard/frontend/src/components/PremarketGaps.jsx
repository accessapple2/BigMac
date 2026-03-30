import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

const DAYBLADE_TICKERS = ['SPY', 'QQQ', 'NVDA', 'TSLA', 'AAPL']

export default function PremarketGaps() {
  const { data } = usePolling(api.getPremarketGaps, 60000)
  const [aiAnalysis, setAiAnalysis] = useState(null)
  const [analyzing, setAnalyzing] = useState(false)

  const gaps = data?.gaps || []

  async function analyzeGaps() {
    setAnalyzing(true)
    try {
      const result = await api.analyzePremarketGaps()
      setAiAnalysis(result.responses || [])
    } catch(e) { alert(e.message) }
    finally { setAnalyzing(false) }
  }

  return (
    <div>
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <h2>Pre-Market Gap Scanner</h2>
          <div style={{ display: 'flex', gap: 8 }}>
            <button onClick={analyzeGaps} disabled={analyzing}
              style={{ padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, background: '#a855f7', color: '#fff', border: 'none', cursor: 'pointer' }}>
              {analyzing ? 'Analyzing...' : 'AI Analyze Gaps'}
            </button>
          </div>
        </div>

        {gaps.length === 0 ? (
          <div style={{ color: '#64748b', padding: 20, textAlign: 'center' }}>No gaps &gt;2% detected — market may be closed</div>
        ) : (
          <>
            {/* Gap Cards */}
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 16 }}>
              {gaps.slice(0, 10).map(g => {
                const color = g.gap_pct >= 0 ? '#22c55e' : '#ef4444'
                const sign = g.gap_pct >= 0 ? '+' : ''
                return (
                  <div key={g.symbol} style={{ background: '#0f172a', borderRadius: 10, padding: '12px 16px', minWidth: 130, borderLeft: `3px solid ${color}` }}>
                    <div style={{ fontFamily: 'monospace', fontWeight: 700, fontSize: 15 }}>{g.symbol}</div>
                    <div style={{ fontFamily: 'monospace', fontSize: 18, fontWeight: 700, color }}>{sign}{g.gap_pct.toFixed(2)}%</div>
                    <div style={{ fontSize: 11, color: '#64748b' }}>${g.prev_close?.toFixed(2)} → ${(g.premarket_price || 0).toFixed(2)}</div>
                    {DAYBLADE_TICKERS.includes(g.symbol) && <span style={{ background: '#f59e0b', color: '#000', padding: '1px 6px', borderRadius: 4, fontSize: 9, fontWeight: 700, marginTop: 4, display: 'inline-block' }}>0DTE</span>}
                  </div>
                )
              })}
            </div>

            {/* Table */}
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['Symbol','Prev Close','Pre-Market','Gap %','Volume','0DTE'].map(h => (
                    <th key={h} style={{ padding: 8, textAlign: 'left', fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {gaps.map(g => {
                  const color = g.gap_pct >= 0 ? '#22c55e' : '#ef4444'
                  const sign = g.gap_pct >= 0 ? '+' : ''
                  return (
                    <tr key={g.symbol} style={{ borderBottom: '1px solid #1e293b' }}>
                      <td style={{ padding: 10, fontWeight: 700, fontFamily: 'monospace' }}>{g.symbol}</td>
                      <td style={{ padding: 10, fontFamily: 'monospace' }}>${g.prev_close?.toFixed(2)}</td>
                      <td style={{ padding: 10, fontFamily: 'monospace' }}>${(g.premarket_price || 0).toFixed(2)}</td>
                      <td style={{ padding: 10, fontFamily: 'monospace', fontWeight: 700, color }}>{sign}{g.gap_pct.toFixed(2)}%</td>
                      <td style={{ padding: 10, fontFamily: 'monospace' }}>{((g.volume || 0) / 1000).toFixed(0)}k</td>
                      <td style={{ padding: 10 }}>{DAYBLADE_TICKERS.includes(g.symbol) ? <span style={{ color: '#f59e0b', fontWeight: 700 }}>YES</span> : '-'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </>
        )}
      </div>

      {/* AI Analysis Results */}
      {aiAnalysis && aiAnalysis.length > 0 && (
        <div className="card">
          <div className="card-header"><h2>AI Gap Analysis</h2></div>
          {aiAnalysis.map((resp, i) => (
            <div key={i} style={{ background: '#0f172a', borderRadius: 10, padding: 14, marginBottom: 10 }}>
              <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 8, color: '#e2e8f0' }}>{(resp.model || '').toUpperCase()}</div>
              {resp.analyses ? resp.analyses.map((a, j) => (
                <div key={j} style={{ padding: '8px 0', borderBottom: '1px solid #1e293b' }}>
                  <span style={{ fontWeight: 700, fontFamily: 'monospace' }}>{a.symbol}</span>
                  {' '}
                  <span style={{ color: a.setup === 'gap-and-go' ? '#22c55e' : '#ef4444', fontWeight: 600 }}>{a.setup}</span>
                  {a.dte_candidate && <span style={{ background: '#f59e0b', color: '#000', padding: '1px 6px', borderRadius: 4, fontSize: 10, fontWeight: 700, marginLeft: 6 }}>0DTE</span>}
                  <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 4 }}>{a.recommendation}</div>
                </div>
              )) : (
                <div style={{ fontSize: 12, whiteSpace: 'pre-wrap', color: '#94a3b8' }}>{resp.raw || resp.error || 'No analysis'}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
