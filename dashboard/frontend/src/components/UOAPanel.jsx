import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

// Colorblind-friendly palette: blue (#2563eb) for calls/bullish, orange (#ea580c) for puts/bearish
const CALL_COLOR = '#2563eb'
const PUT_COLOR = '#ea580c'
const NEUTRAL_COLOR = '#64748b'

const SEV_STYLE = {
  CRITICAL: { bg: '#7f1d1d', color: '#fca5a5', label: 'CRITICAL' },
  HIGH:     { bg: '#78350f', color: '#fbbf24', label: 'HIGH' },
  MEDIUM:   { bg: '#1e3a5f', color: '#93c5fd', label: 'MEDIUM' },
  LOW:      { bg: '#1e293b', color: '#94a3b8', label: 'LOW' },
}

function SeverityBadge({ severity }) {
  const s = SEV_STYLE[severity] || SEV_STYLE.LOW
  return (
    <span style={{
      display: 'inline-block', padding: '2px 8px', borderRadius: 4, fontSize: 11,
      fontWeight: 700, background: s.bg, color: s.color,
    }}>{s.label}</span>
  )
}

function DirectionArrow({ type, sentiment }) {
  if (type === 'CALL' || sentiment === 'BULLISH') return <span style={{ color: CALL_COLOR, fontWeight: 700 }} title="Bullish / Call">&#9650; CALL</span>
  if (type === 'PUT' || sentiment === 'BEARISH') return <span style={{ color: PUT_COLOR, fontWeight: 700 }} title="Bearish / Put">&#9660; PUT</span>
  return <span style={{ color: NEUTRAL_COLOR }}>&#9644; {type || 'N/A'}</span>
}

export default function UOAPanel() {
  const { data, loading, error } = usePolling(api.getUOADashboard, 60000)
  const [scanning, setScanning] = useState(false)

  const handleScan = async () => {
    setScanning(true)
    try { await api.triggerUOAScan('quick') } catch {}
    setTimeout(() => setScanning(false), 3000)
  }

  if (loading && !data) return <div className="loading">Loading UOA data...</div>
  if (error) return <div className="empty-state">UOA scanner not available. Run a scan first.</div>
  if (!data) return <div className="empty-state">No UOA data yet. Click Scan to start.</div>

  const { alert_counts = {}, top_alerts = [], bearish_flow = [], last_scan, total_alerts_today = 0 } = data

  return (
    <div className="arena-layout">
      {/* Header card with scan stats */}
      <div className="card">
        <div className="card-header">
          <h2>Unusual Options Activity</h2>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <span className="card-badge">{total_alerts_today} alerts today</span>
            <button
              onClick={handleScan}
              disabled={scanning}
              style={{
                padding: '4px 12px', borderRadius: 4, border: '1px solid #334155',
                background: scanning ? '#1e293b' : '#0f172a', color: '#e2e8f0',
                cursor: scanning ? 'wait' : 'pointer', fontSize: 12,
              }}
            >{scanning ? 'Scanning...' : 'Quick Scan'}</button>
          </div>
        </div>

        {/* Severity summary row */}
        <div style={{ display: 'flex', gap: 16, padding: '12px 16px', flexWrap: 'wrap' }}>
          {['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'].map(sev => (
            <div key={sev} style={{ textAlign: 'center', minWidth: 80 }}>
              <div style={{ fontSize: 28, fontWeight: 700, fontFamily: 'monospace', color: SEV_STYLE[sev].color }}>
                {alert_counts[sev] || 0}
              </div>
              <SeverityBadge severity={sev} />
            </div>
          ))}
          {last_scan && (
            <div style={{ marginLeft: 'auto', fontSize: 11, color: '#475569', textAlign: 'right' }}>
              <div>Last scan: {last_scan.scan_date} {last_scan.scan_time}</div>
              <div>{last_scan.tickers_scanned} tickers, {last_scan.duration_seconds}s</div>
            </div>
          )}
        </div>
      </div>

      {/* Top Alerts table */}
      <div className="card">
        <div className="card-header">
          <h2>Top Alerts</h2>
          <span className="card-badge">{top_alerts.length} shown</span>
        </div>
        {top_alerts.length === 0 ? (
          <div className="empty-state">No alerts today. Run a scan to detect unusual activity.</div>
        ) : (
          <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: '#64748b', textAlign: 'left' }}>
                {['Severity', 'Ticker', 'Direction', 'Strike', 'Expiry', 'Vol/OI', 'Premium', 'Score', 'Type'].map(h => (
                  <th key={h} style={{ padding: '6px 10px', borderBottom: '1px solid #1e293b' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {top_alerts.map((a, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #0f172a' }}>
                  <td style={{ padding: '5px 10px' }}><SeverityBadge severity={a.severity} /></td>
                  <td style={{ padding: '5px 10px', fontWeight: 700, color: '#e2e8f0' }}>{a.ticker}</td>
                  <td style={{ padding: '5px 10px' }}><DirectionArrow type={a.contract_type} /></td>
                  <td style={{ padding: '5px 10px', fontFamily: 'monospace', color: '#cbd5e1' }}>
                    {a.strike ? `$${Number(a.strike).toFixed(0)}` : '-'}
                  </td>
                  <td style={{ padding: '5px 10px', color: '#94a3b8' }}>{a.expiration || '-'}</td>
                  <td style={{ padding: '5px 10px', fontFamily: 'monospace', color: a.vol_oi_ratio >= 10 ? '#fbbf24' : '#cbd5e1' }}>
                    {a.vol_oi_ratio ? `${a.vol_oi_ratio.toFixed(1)}x` : '-'}
                  </td>
                  <td style={{ padding: '5px 10px', fontFamily: 'monospace', color: a.premium_total >= 100000 ? '#fbbf24' : '#cbd5e1' }}>
                    {a.premium_total ? `$${Number(a.premium_total).toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '-'}
                  </td>
                  <td style={{ padding: '5px 10px' }}>
                    <div style={{
                      width: 40, height: 6, borderRadius: 3, background: '#1e293b', overflow: 'hidden',
                    }}>
                      <div style={{
                        width: `${Math.min(a.convergence_score || 0, 100)}%`, height: '100%', borderRadius: 3,
                        background: (a.convergence_score || 0) >= 60 ? '#ef4444' : (a.convergence_score || 0) >= 35 ? '#f59e0b' : '#3b82f6',
                      }} />
                    </div>
                    <span style={{ fontSize: 10, color: '#64748b' }}>{(a.convergence_score || 0).toFixed(0)}</span>
                  </td>
                  <td style={{ padding: '5px 10px', fontSize: 11, color: '#64748b' }}>{a.alert_type}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Bearish Flow / Put Wall section */}
      {bearish_flow.length > 0 && (
        <div className="card">
          <div className="card-header">
            <h2>Heaviest Put Flow</h2>
            <span className="card-badge" style={{ background: '#7c2d12', color: PUT_COLOR }}>Bearish Positioning</span>
          </div>
          <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: '#64748b', textAlign: 'left' }}>
                {['Ticker', 'Put/Call Ratio', 'Put Premium', 'Max Vol/OI'].map(h => (
                  <th key={h} style={{ padding: '6px 10px', borderBottom: '1px solid #1e293b' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {bearish_flow.map((b, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #0f172a' }}>
                  <td style={{ padding: '5px 10px', fontWeight: 700, color: '#e2e8f0' }}>
                    <span style={{ color: PUT_COLOR, marginRight: 4 }}>&#9660;</span>{b.ticker}
                  </td>
                  <td style={{ padding: '5px 10px', fontFamily: 'monospace', color: PUT_COLOR, fontWeight: 700 }}>
                    {b.put_call_ratio ? `${b.put_call_ratio.toFixed(1)}x` : '-'}
                  </td>
                  <td style={{ padding: '5px 10px', fontFamily: 'monospace', color: '#cbd5e1' }}>
                    {b.total_put_premium ? `$${Number(b.total_put_premium).toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '-'}
                  </td>
                  <td style={{ padding: '5px 10px', fontFamily: 'monospace', color: '#cbd5e1' }}>
                    {b.max_vol_oi_ratio ? `${b.max_vol_oi_ratio.toFixed(1)}x` : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
