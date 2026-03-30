import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

export default function InsiderTracker() {
  const [symbol, setSymbol] = useState('')
  const [trades, setTrades] = useState(null)
  const [scanning, setScanning] = useState(false)
  const [alerts, setAlerts] = useState(null)

  async function scanAlerts() {
    setScanning(true)
    try {
      const data = await api.getInsiderAlerts()
      setAlerts(data.alerts || [])
    } catch(e) { alert(e.message) }
    finally { setScanning(false) }
  }

  async function lookupInsider() {
    if (!symbol) return
    try {
      const data = await api.getInsiderTrades(symbol.toUpperCase())
      setTrades(data.trades || [])
    } catch(e) { alert(e.message) }
  }

  return (
    <div>
      {/* Alerts Card */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-header">
          <h2>Insider Trading Alerts</h2>
          <button onClick={scanAlerts} disabled={scanning}
            style={{ padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, background: '#00d4aa', color: '#0a0a1a', border: 'none', cursor: 'pointer' }}>
            {scanning ? 'Scanning...' : 'Scan Watchlist'}
          </button>
        </div>

        {alerts === null ? (
          <div style={{ color: '#64748b', padding: 20, textAlign: 'center' }}>Click "Scan Watchlist" to check for insider activity</div>
        ) : alerts.length === 0 ? (
          <div style={{ color: '#64748b', padding: 20, textAlign: 'center' }}>No significant insider activity detected</div>
        ) : (
          alerts.map((a, i) => (
            <div key={i} style={{ background: '#0f172a', borderRadius: 10, padding: 14, marginBottom: 8, borderLeft: '3px solid #22c55e' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                  <span style={{ fontWeight: 700, fontFamily: 'monospace', fontSize: 15 }}>{a.symbol}</span>
                  {' '}<span style={{ fontSize: 12, color: '#22c55e', fontWeight: 600 }}>INSIDER BUY</span>
                </div>
                <div style={{ fontFamily: 'monospace', fontWeight: 700, color: '#22c55e' }}>${((a.value || 0) / 1000).toFixed(0)}k</div>
              </div>
              <div style={{ fontSize: 12, color: '#94a3b8', marginTop: 4 }}>
                {a.insider_name || 'Unknown'} ({a.title || ''}) — {(a.shares || 0).toLocaleString()} shares — {a.date || ''}
              </div>
            </div>
          ))
        )}
      </div>

      {/* Lookup Card */}
      <div className="card">
        <div className="card-header">
          <h2>Lookup Insider Trades</h2>
          <div style={{ display: 'flex', gap: 6 }}>
            <input value={symbol} onChange={e => setSymbol(e.target.value.toUpperCase())} placeholder="AAPL"
              style={{ width: 80, padding: '6px 10px', borderRadius: 6, background: '#0f172a', color: '#e2e8f0', border: '1px solid #333', fontSize: 13, fontFamily: 'monospace' }}
              onKeyDown={e => e.key === 'Enter' && lookupInsider()} />
            <button onClick={lookupInsider}
              style={{ padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, background: '#00d4aa', color: '#0a0a1a', border: 'none', cursor: 'pointer' }}>
              Lookup
            </button>
          </div>
        </div>

        {trades !== null && (
          trades.length === 0 ? (
            <div style={{ color: '#64748b', padding: 20, textAlign: 'center' }}>No insider trades found</div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid #333' }}>
                  {['Name','Title','Type','Shares','Value','Date'].map(h => (
                    <th key={h} style={{ padding: 8, textAlign: 'left', fontSize: 10, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 1 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {trades.map((t, i) => {
                  const isBuy = (t.transaction_type || '').toLowerCase().includes('buy') || (t.transaction_type || '').toLowerCase().includes('purchase')
                  return (
                    <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
                      <td style={{ padding: 10, fontWeight: 600 }}>{t.name || 'N/A'}</td>
                      <td style={{ padding: 10, fontSize: 11, color: '#94a3b8' }}>{t.title || 'N/A'}</td>
                      <td style={{ padding: 10, color: isBuy ? '#22c55e' : '#ef4444', fontWeight: 600 }}>{t.transaction_type || 'N/A'}</td>
                      <td style={{ padding: 10, fontFamily: 'monospace' }}>{(t.shares || 0).toLocaleString()}</td>
                      <td style={{ padding: 10, fontFamily: 'monospace' }}>${((t.value || 0) / 1000).toFixed(0)}k</td>
                      <td style={{ padding: 10, fontFamily: 'monospace', fontSize: 11 }}>{t.date || 'N/A'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )
        )}
      </div>
    </div>
  )
}
