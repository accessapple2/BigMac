import React, { useState, useEffect } from 'react'
import { getAutoRefresh, setAutoRefresh, onAutoRefreshChange } from '../hooks/usePolling'

export function AutoRefreshToggle() {
  const [on, setOn] = useState(getAutoRefresh())

  useEffect(() => onAutoRefreshChange(setOn), [])

  return (
    <button
      onClick={() => setAutoRefresh(!on)}
      title={on ? 'Auto-refresh ON (click to pause)' : 'Auto-refresh OFF (click to resume)'}
      style={{
        background: 'none', border: 'none', cursor: 'pointer',
        display: 'flex', alignItems: 'center', gap: 4, padding: '2px 6px',
        fontSize: 11, color: on ? '#22c55e' : '#64748b',
      }}
    >
      <span style={{
        width: 7, height: 7, borderRadius: '50%',
        background: on ? '#22c55e' : '#64748b',
        boxShadow: on ? '0 0 4px #22c55e' : 'none',
      }} />
      {on ? 'Live' : 'Paused'}
    </button>
  )
}

export function LastUpdated({ time }) {
  if (!time) return null
  const str = time.toLocaleTimeString('en-US', {
    timeZone: 'America/Phoenix', hour: 'numeric', minute: '2-digit', hour12: true
  })
  return (
    <span style={{ fontSize: 10, color: '#64748b', fontStyle: 'italic' }}>
      Updated {str}
    </span>
  )
}
