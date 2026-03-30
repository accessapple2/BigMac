import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'

const TABS = [
  { id: 'gainers',     label: '▲ Gainers',    color: '#4ade80' },
  { id: 'losers',      label: '▼ Losers',      color: '#f87171' },
  { id: 'most_active', label: '◆ Most Active', color: '#60a5fa' },
]

function SkeletonRow() {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '9px 16px', borderBottom: '1px solid #1e293b' }}>
      <div style={{ width: 48, height: 12, background: '#1e293b', borderRadius: 3 }} />
      <div style={{ flex: 1, height: 10, background: '#1e293b', borderRadius: 3 }} />
      <div style={{ width: 56, height: 12, background: '#1e293b', borderRadius: 3 }} />
      <div style={{ width: 44, height: 12, background: '#1e293b', borderRadius: 3 }} />
    </div>
  )
}

function MoverRow({ stock, tab }) {
  const isPos = stock.change_pct >= 0
  const color = tab === 'most_active' ? '#60a5fa' : isPos ? '#4ade80' : '#f87171'
  const vol = stock.volume >= 1e6
    ? `${(stock.volume / 1e6).toFixed(1)}M`
    : stock.volume >= 1e3
    ? `${(stock.volume / 1e3).toFixed(0)}K`
    : String(stock.volume || 0)

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 10,
      padding: '9px 16px', borderBottom: '1px solid #1e293b',
    }}>
      <span style={{ fontFamily: 'monospace', fontWeight: 700, fontSize: 13, minWidth: 52, color: '#e2e8f0' }}>
        {stock.symbol}
      </span>
      <span style={{ flex: 1, fontSize: 11, color: '#64748b', fontFamily: 'monospace' }}>
        vol {vol}
      </span>
      {stock.price > 0 && (
        <span style={{ fontSize: 12, fontFamily: 'monospace', color: '#94a3b8', minWidth: 60, textAlign: 'right' }}>
          ${stock.price.toFixed(2)}
        </span>
      )}
      <span style={{ fontSize: 13, fontFamily: 'monospace', fontWeight: 700, color, minWidth: 60, textAlign: 'right' }}>
        {isPos ? '+' : ''}{stock.change_pct?.toFixed(2)}%
      </span>
    </div>
  )
}

export default function MarketMovers() {
  const [tab, setTab] = useState('gainers')
  const { data, loading, lastUpdated } = usePolling(api.getMarketMovers, 60000)
  const isUpdating = data?.is_updating === true
  const rows = data?.[tab] || []
  const isLoading = (loading && !data) || (isUpdating && rows.length === 0)

  return (
    <div className="card">
      <div className="card-header">
        <h2>Market Movers</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {isUpdating && (
            <span style={{ fontSize: 9, color: '#60a5fa', fontWeight: 700, letterSpacing: 0.5 }}>
              ↻ updating...
            </span>
          )}
          <LastUpdated time={lastUpdated} />
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 4, padding: '8px 16px', borderBottom: '1px solid #1e293b' }}>
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{
            padding: '4px 12px', borderRadius: 6, fontSize: 11, fontWeight: 700,
            cursor: 'pointer', border: '1px solid',
            background: tab === t.id ? '#0f172a' : 'transparent',
            color: tab === t.id ? t.color : '#64748b',
            borderColor: tab === t.id ? t.color : '#334155',
          }}>
            {t.label}
          </button>
        ))}
      </div>

      {isLoading
        ? Array.from({ length: 5 }).map((_, i) => <SkeletonRow key={i} />)
        : rows.length === 0
        ? <div className="empty-state">No data available.</div>
        : rows.map(s => <MoverRow key={s.symbol} stock={s} tab={tab} />)
      }

      {data?.timestamp && (
        <div style={{ padding: '6px 16px', fontSize: 10, color: '#334155', textAlign: 'right' }}>
          Watchlist ({data.gainers?.length + data.losers?.length > 0 ? 'top 5 each' : ''})
        </div>
      )}
    </div>
  )
}
