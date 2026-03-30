import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'

const IB = 'http://127.0.0.1:5001'

function SkeletonRow() {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 14px', borderBottom: '1px solid #1e293b' }}>
      <div style={{ width: 44, height: 11, background: '#1e293b', borderRadius: 3 }} />
      <div style={{ flex: 1, height: 10, background: '#1e293b', borderRadius: 3 }} />
      <div style={{ width: 52, height: 11, background: '#1e293b', borderRadius: 3 }} />
      <div style={{ width: 48, height: 11, background: '#1e293b', borderRadius: 3 }} />
    </div>
  )
}

function PositionRow({ pos, isWinner }) {
  const color = isWinner ? '#4ade80' : '#f87171'
  const arrow = isWinner ? '▲' : '▼'
  const label = isWinner ? 'WIN' : 'LOSS'

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '8px 14px', borderBottom: '1px solid #1e293b',
    }}>
      {/* Ticker + chart button */}
      <span style={{ fontFamily: 'monospace', fontWeight: 700, fontSize: 13, minWidth: 48, color: '#e2e8f0' }}>
        {pos.symbol}
      </span>
      <button
        onClick={() => window.open(`${IB}/ib_chart.html?symbol=${pos.symbol}`, '_blank')}
        title={`Chart ${pos.symbol}`}
        style={{
          background: 'none', border: '1px solid #334155', borderRadius: 4,
          color: '#60a5fa', cursor: 'pointer', fontSize: 10, padding: '1px 5px',
          lineHeight: 1.4, flexShrink: 0,
        }}
      >
        📈
      </button>

      {/* Model name */}
      <span style={{ flex: 1, fontSize: 10, color: '#64748b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {pos.model}
      </span>

      {/* Day P&L $ */}
      <span style={{ fontFamily: 'monospace', fontSize: 12, fontWeight: 700, color, minWidth: 64, textAlign: 'right' }}>
        {arrow} ${Math.abs(pos.day_pnl).toFixed(2)}
      </span>

      {/* Day % */}
      <span style={{ fontFamily: 'monospace', fontSize: 11, color, minWidth: 48, textAlign: 'right' }}>
        {pos.day_pct >= 0 ? '+' : ''}{pos.day_pct?.toFixed(2)}%
      </span>

      {/* Price */}
      <span style={{ fontFamily: 'monospace', fontSize: 11, color: '#94a3b8', minWidth: 52, textAlign: 'right' }}>
        ${pos.price?.toFixed(2)}
      </span>
    </div>
  )
}

function SectionHeader({ label, color }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '6px 14px', background: '#0d1526', borderBottom: '1px solid #1e293b',
    }}>
      <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: 1, color }}>{label}</span>
    </div>
  )
}

export default function WinnersLosers() {
  const { data, loading, lastUpdated } = usePolling(api.getWinnersLosers, 60000)
  const isLoading = loading && !data
  const isUpdating = data?.is_updating === true

  const winners = data?.winners || []
  const losers  = data?.losers  || []

  return (
    <div className="card">
      <div className="card-header">
        <h2>🏆 Top Winners &amp; 💀 Losers</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {isUpdating && (
            <span style={{ fontSize: 9, color: '#60a5fa', fontWeight: 700, letterSpacing: 0.5 }}>
              ↻ updating...
            </span>
          )}
          <LastUpdated time={lastUpdated} />
        </div>
      </div>

      {/* Column header */}
      <div style={{
        display: 'flex', gap: 8, padding: '4px 14px',
        borderBottom: '1px solid #1e293b', fontSize: 9,
        color: '#475569', fontWeight: 700, letterSpacing: 0.5,
      }}>
        <span style={{ minWidth: 48 }}>TICKER</span>
        <span style={{ width: 24 }} />
        <span style={{ flex: 1 }}>MODEL</span>
        <span style={{ minWidth: 64, textAlign: 'right' }}>DAY P&amp;L $</span>
        <span style={{ minWidth: 48, textAlign: 'right' }}>DAY %</span>
        <span style={{ minWidth: 52, textAlign: 'right' }}>PRICE</span>
      </div>

      <SectionHeader label="▲ TOP WINNERS" color="#4ade80" />
      {isLoading
        ? Array.from({ length: 5 }).map((_, i) => <SkeletonRow key={i} />)
        : winners.length === 0
        ? <div className="empty-state">No open positions</div>
        : winners.map((p, i) => <PositionRow key={i} pos={p} isWinner={true} />)
      }

      <SectionHeader label="▼ TOP LOSERS" color="#f87171" />
      {isLoading
        ? Array.from({ length: 5 }).map((_, i) => <SkeletonRow key={i} />)
        : losers.length === 0
        ? <div className="empty-state">No open positions</div>
        : losers.map((p, i) => <PositionRow key={i} pos={p} isWinner={false} />)
      }

      <div style={{ padding: '5px 14px', fontSize: 9, color: '#334155', textAlign: 'right' }}>
        All AI players · open stock positions · by day P&amp;L
      </div>
    </div>
  )
}
