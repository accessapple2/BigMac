import React, { useState, useEffect } from 'react'
import { api } from '../api/client'

const SMA_ICONS = {
  '200 SMA Bounce':    { icon: '◆▲', color: '#22c55e' },
  '200 SMA Breakdown': { icon: '▼!', color: '#ef4444' },
  '200 SMA Reclaim':   { icon: '▲!', color: '#00d4aa' },
}

function SmaBadge({ symbol, smaMap }) {
  const s = smaMap[symbol]
  if (!s) return <span style={{ color: '#334155', fontSize: 11 }}>—</span>
  const sig = s.signal_type ? SMA_ICONS[s.signal_type] : null
  const dist = s.distance_pct
  const icon = sig ? sig.icon : (Math.abs(dist) <= 2 ? '◆' : s.above_sma200 ? '▲' : '▼')
  const color = sig ? sig.color : (Math.abs(dist) <= 2 ? '#eab308' : s.above_sma200 ? '#22c55e' : '#ef4444')
  return (
    <span title={`200 SMA $${s.sma_200?.toFixed(2)} | ${dist > 0 ? '+' : ''}${dist?.toFixed(2)}%`}
      style={{ fontWeight: 700, color, fontSize: 13 }}>
      {icon}
    </span>
  )
}

export default function StockScreener() {
  const [filters, setFilters] = useState({})
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(false)
  const [smaMap, setSmaMap] = useState({})

  useEffect(() => {
    api.getSmaStatus().then(d => {
      const m = {}
      for (const s of (d.stocks || [])) m[s.symbol] = s
      setSmaMap(m)
    }).catch(() => {})
  }, [])

  function updateFilter(key, val) {
    setFilters(f => ({ ...f, [key]: val }))
  }

  async function runScreen() {
    setLoading(true)
    try {
      const data = await api.runScreener(filters)
      setResults(data.results || [])
    } catch(e) { alert(e.message) }
    finally { setLoading(false) }
  }

  const inputStyle = { width: 80, padding: '6px 8px', borderRadius: 6, background: '#0f172a', color: '#e2e8f0', border: '1px solid #333', fontSize: 12, fontFamily: 'monospace' }
  const labelStyle = { fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 }

  return (
    <div>
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <h2>Stock Screener</h2>
          <button onClick={runScreen} disabled={loading}
            style={{ padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, background: '#00d4aa', color: '#0a0a1a', border: 'none', cursor: 'pointer' }}>
            {loading ? 'Screening...' : 'Screen'}
          </button>
        </div>
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 16 }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={labelStyle}>P/E Range</div>
            <div style={{ display: 'flex', gap: 4 }}>
              <input type="number" placeholder="Min" style={inputStyle} onChange={e => updateFilter('min_pe', e.target.value)} />
              <input type="number" placeholder="Max" style={inputStyle} onChange={e => updateFilter('max_pe', e.target.value)} />
            </div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={labelStyle}>Short Float %</div>
            <div style={{ display: 'flex', gap: 4 }}>
              <input type="number" placeholder="Min" style={inputStyle} onChange={e => updateFilter('min_short_float', e.target.value)} />
              <input type="number" placeholder="Max" style={inputStyle} onChange={e => updateFilter('max_short_float', e.target.value)} />
            </div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={labelStyle}>Rel Volume Min</div>
            <input type="number" placeholder="1.5" step="0.1" style={inputStyle} onChange={e => updateFilter('min_rel_volume', e.target.value)} />
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={labelStyle}>Consensus</div>
            <select style={{ ...inputStyle, width: 90 }} onChange={e => updateFilter('consensus', e.target.value)}>
              <option value="">Any</option>
              <option value="buy">Buy</option>
              <option value="hold">Hold</option>
              <option value="sell">Sell</option>
            </select>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={labelStyle}>Earnings ≤ Days</div>
            <input type="number" placeholder="7" style={inputStyle} onChange={e => updateFilter('earnings_within_days', e.target.value)} />
          </div>
          <div style={{ display: 'flex', alignItems: 'flex-end', paddingBottom: 2 }}>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
              <input type="checkbox" onChange={e => updateFilter('has_insider_buying', e.target.checked)} style={{ accentColor: '#00d4aa' }} />
              Insider Buying
            </label>
          </div>
        </div>

        {results !== null && (
          results.length === 0 ? (
            <div style={{ color: '#64748b', padding: 20, textAlign: 'center' }}>No stocks match filters</div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['Symbol','Price','P/E','Short %','Rel Vol','Consensus','Insider','200 SMA'].map(h => (
                    <th key={h} style={{ padding: 8, textAlign: 'left', fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {results.map(s => (
                  <tr key={s.symbol} style={{ borderBottom: '1px solid #1e293b' }}>
                    <td style={{ padding: 10, fontWeight: 700, fontFamily: 'monospace' }}>{s.symbol}</td>
                    <td style={{ padding: 10, fontFamily: 'monospace' }}>${(s.price || 0).toFixed(2)}</td>
                    <td style={{ padding: 10, fontFamily: 'monospace' }}>{s.pe_ratio != null ? s.pe_ratio.toFixed(1) : 'N/A'}</td>
                    <td style={{ padding: 10, fontFamily: 'monospace' }}>{s.short_float != null ? s.short_float.toFixed(1) + '%' : 'N/A'}</td>
                    <td style={{ padding: 10, fontFamily: 'monospace' }}>{s.rel_volume != null ? s.rel_volume.toFixed(1) + 'x' : 'N/A'}</td>
                    <td style={{ padding: 10 }}>{s.consensus || 'N/A'}</td>
                    <td style={{ padding: 10 }}>{s.insider_buying ? <span style={{ color: '#22c55e', fontWeight: 700 }}>YES</span> : '-'}</td>
                    <td style={{ padding: 10 }}><SmaBadge symbol={s.symbol} smaMap={smaMap} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        )}
      </div>
    </div>
  )
}
