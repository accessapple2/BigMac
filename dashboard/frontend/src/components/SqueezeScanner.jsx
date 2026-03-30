import { useState, useEffect, useCallback } from 'react'
import { api } from '../api/client'

const SCORE_COLOR = (score) => {
  if (score >= 9) return '#ff4444'
  if (score >= 7) return '#ff8800'
  if (score >= 5) return '#ffcc00'
  return '#888'
}

const SCORE_LABEL = (score) => {
  if (score >= 9) return '🔥 EXTREME'
  if (score >= 7) return '⚡ HIGH'
  if (score >= 5) return '📈 MODERATE'
  return 'LOW'
}

export default function SqueezeScanner() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const fetchData = useCallback(async (force = false) => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.getSqueeze(force)
      setData(res)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchData(false)
  }, [fetchData])

  const results = data?.results || []
  const scannedAt = data?.scanned_at
    ? new Date(data.scanned_at).toLocaleTimeString()
    : null

  return (
    <div style={{ padding: '0 1rem 1rem' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.1rem', color: '#fff' }}>
            ⚡ Squeeze Finder
          </h2>
          <div style={{ fontSize: '0.75rem', color: '#666', marginTop: '0.2rem' }}>
            Short interest &gt;20% · Float &lt;20M · Volume &gt;2× · RSI &lt;70
          </div>
        </div>
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
          {scannedAt && (
            <span style={{ fontSize: '0.7rem', color: '#555' }}>
              Last scan: {scannedAt}
            </span>
          )}
          <button
            onClick={() => fetchData(true)}
            disabled={loading}
            style={{
              background: loading ? '#333' : '#1a3a1a',
              border: '1px solid #2a5a2a',
              color: loading ? '#555' : '#4ade80',
              padding: '0.35rem 0.8rem',
              borderRadius: '6px',
              cursor: loading ? 'not-allowed' : 'pointer',
              fontSize: '0.78rem',
            }}
          >
            {loading ? 'Scanning…' : '↻ Rescan'}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div style={{ background: '#2a1010', border: '1px solid #5a2020', borderRadius: '8px', padding: '0.8rem', color: '#f87171', marginBottom: '1rem' }}>
          {error}
        </div>
      )}

      {/* Loading skeleton */}
      {loading && results.length === 0 && (
        <div style={{ color: '#555', textAlign: 'center', padding: '3rem', fontSize: '0.9rem' }}>
          Scanning Finviz + yfinance for squeeze setups…
        </div>
      )}

      {/* No results */}
      {!loading && results.length === 0 && !error && (
        <div style={{ color: '#555', textAlign: 'center', padding: '3rem', fontSize: '0.9rem' }}>
          No squeeze candidates found right now. Hit Rescan to run a fresh scan.
        </div>
      )}

      {/* Results table */}
      {results.length > 0 && (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.82rem' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #333', color: '#666', textAlign: 'left' }}>
                <th style={{ padding: '0.4rem 0.6rem' }}>Ticker</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'right' }}>Short %</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'right' }}>Float</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'right' }}>Vol Ratio</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'right' }}>Price</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'right' }}>Day %</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'right' }}>RSI</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'right' }}>10d High</th>
                <th style={{ padding: '0.4rem 0.6rem', textAlign: 'center' }}>Score</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => (
                <tr
                  key={r.ticker}
                  style={{
                    borderBottom: '1px solid #1a1a1a',
                    background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)',
                  }}
                >
                  <td style={{ padding: '0.45rem 0.6rem', fontWeight: 600, color: '#e2e8f0' }}>
                    {r.ticker}
                    {r.above_10d_high && (
                      <span style={{ marginLeft: '0.3rem', fontSize: '0.7rem', color: '#4ade80' }}>▲BO</span>
                    )}
                  </td>
                  <td style={{ padding: '0.45rem 0.6rem', textAlign: 'right', color: '#f97316' }}>
                    {r.short_interest_pct}%
                  </td>
                  <td style={{ padding: '0.45rem 0.6rem', textAlign: 'right', color: '#94a3b8' }}>
                    {r.float_m < 1 ? `${(r.float_m * 1000).toFixed(0)}K` : `${r.float_m}M`}
                  </td>
                  <td style={{
                    padding: '0.45rem 0.6rem', textAlign: 'right',
                    color: r.vol_ratio >= 5 ? '#f87171' : r.vol_ratio >= 2 ? '#fb923c' : '#94a3b8'
                  }}>
                    {r.vol_ratio}×
                  </td>
                  <td style={{ padding: '0.45rem 0.6rem', textAlign: 'right', color: '#e2e8f0' }}>
                    ${r.price.toFixed(2)}
                  </td>
                  <td style={{
                    padding: '0.45rem 0.6rem', textAlign: 'right',
                    color: r.day_change_pct >= 0 ? '#4ade80' : '#f87171'
                  }}>
                    {r.day_change_pct >= 0 ? '+' : ''}{r.day_change_pct.toFixed(2)}%
                  </td>
                  <td style={{
                    padding: '0.45rem 0.6rem', textAlign: 'right',
                    color: r.rsi >= 70 ? '#f87171' : r.rsi >= 60 ? '#fb923c' : '#94a3b8'
                  }}>
                    {r.rsi}
                  </td>
                  <td style={{ padding: '0.45rem 0.6rem', textAlign: 'right', color: '#64748b', fontSize: '0.75rem' }}>
                    {r.above_10d_high ? '✓ Break' : '–'}
                  </td>
                  <td style={{ padding: '0.45rem 0.6rem', textAlign: 'center' }}>
                    <span style={{
                      display: 'inline-block',
                      background: `${SCORE_COLOR(r.score)}22`,
                      border: `1px solid ${SCORE_COLOR(r.score)}55`,
                      color: SCORE_COLOR(r.score),
                      borderRadius: '6px',
                      padding: '0.15rem 0.5rem',
                      fontWeight: 700,
                      fontSize: '0.78rem',
                      minWidth: '2rem',
                    }}>
                      {r.score}
                    </span>
                    {r.score > 8 && (
                      <div style={{ fontSize: '0.65rem', color: '#ff4444', marginTop: '0.1rem' }}>
                        {SCORE_LABEL(r.score)}
                      </div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Footer note */}
      {results.length > 0 && (
        <div style={{ marginTop: '0.8rem', fontSize: '0.7rem', color: '#444', textAlign: 'right' }}>
          Scores &gt;8 auto-post to War Room from Chekov · ▲BO = breaking 10-day high
        </div>
      )}
    </div>
  )
}
