import React, { useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

export default function LiveAlerts() {
  const fetchAlerts = useCallback(() => api.getRealtimeAlerts(10), [])
  const fetchStatus = useCallback(() => api.getRealtimeStatus(), [])
  const { data: alerts } = usePolling(fetchAlerts, 10000)  // 10s poll
  const { data: status } = usePolling(fetchStatus, 30000)  // 30s poll

  if (!alerts || alerts.length === 0) {
    return (
      <div style={{
        background: '#1a1a2e', borderBottom: '2px solid #2a2a4a',
        padding: '8px 16px', display: 'flex', alignItems: 'center', gap: 12,
        fontSize: 13, color: '#999',
      }}>
        <span style={{ fontSize: 14 }}>
          {status?.connected ? '📡' : '⏳'}
        </span>
        <span style={{ color: '#e0e0e0', fontWeight: 600 }}>
          Realtime Monitor: {status?.connected ? `tracking ${status?.tracked_symbols || 0} symbols` : 'connecting...'}
        </span>
      </div>
    )
  }

  return (
    <div style={{
      background: '#1a1a2e', borderBottom: '2px solid #2a2a4a',
      padding: '6px 0', overflow: 'hidden', position: 'relative',
    }}>
      <div style={{
        display: 'flex', gap: 24, animation: alerts.length > 3 ? 'scroll-ticker 30s linear infinite' : 'none',
        padding: '6px 16px', alignItems: 'center',
      }}>
        <span style={{ fontSize: 14 }}>
          {status?.connected ? '📡' : '⏳'}
        </span>
        {alerts.map((a, i) => {
          const isCritical = a.severity === 'critical'
          const msg = a.message || ''
          const isUp = msg.includes('+')
          const isVolume = msg.toLowerCase().includes('volume')
          const icon = isVolume ? '📊' : isUp ? '⚡' : '🔻'
          const dirLabel = isVolume ? '' : isUp ? ' ▲ UP' : ' ▼ DOWN'
          const pctMatch = msg.match(/[+-][\d.]+%/)
          const time = a.triggered_at ? new Date(a.triggered_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : ''
          const isNew = a.triggered_at && (Date.now() - new Date(a.triggered_at).getTime()) < 60000
          return (
            <div key={i} style={{
              display: 'flex', alignItems: 'center', gap: 8,
              whiteSpace: 'nowrap', flexShrink: 0,
              fontSize: 13,
              borderLeft: `3px solid ${isCritical ? '#ff6b6b' : '#6c63ff'}`,
              paddingLeft: 8,
              animation: isNew ? 'alert-pulse 2s ease-in-out 3' : 'none',
            }}>
              <span style={{ fontSize: 15 }}>{icon}</span>
              <span style={{
                color: '#ffffff', fontWeight: 700, fontSize: 13,
              }}>
                {a.symbol}
              </span>
              <span style={{
                color: '#ffffff', fontWeight: 700, fontSize: 13,
              }}>
                {pctMatch?.[0] || ''}{dirLabel}
              </span>
              {a.price > 0 && (
                <span style={{ color: '#b0b0b0', fontWeight: 600, fontSize: 12 }}>${a.price.toFixed(2)}</span>
              )}
              <span style={{
                color: isCritical ? '#ff9f43' : '#7c75d4',
                fontWeight: 700, fontSize: 10,
                padding: '2px 6px', borderRadius: 3,
                background: isCritical ? 'rgba(255,159,67,0.15)' : 'rgba(124,117,212,0.15)',
              }}>
                {a.alert_type === 'realtime_trade' ? 'TRADE' : 'SPIKE'}
              </span>
              <span style={{ color: '#666', fontSize: 11, fontWeight: 500 }}>{time}</span>
              {i < alerts.length - 1 && (
                <span style={{ color: '#333', margin: '0 4px' }}>│</span>
              )}
            </div>
          )
        })}
      </div>

      <style>{`
        @keyframes scroll-ticker {
          0% { transform: translateX(0); }
          100% { transform: translateX(-50%); }
        }
        @keyframes alert-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
      `}</style>
    </div>
  )
}
