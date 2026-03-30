import React from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'

// Colorblind-safe: blue for winners, orange for losers
const WIN_COLOR = '#2563eb'
const LOSE_COLOR = '#ea580c'

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
  const color = isWinner ? WIN_COLOR : LOSE_COLOR
  const arrow = isWinner ? '\u25B2' : '\u25BC'
  const label = isWinner ? 'WIN' : 'LOSS'

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '8px 14px', borderBottom: '1px solid #1e293b',
    }}>
      <span style={{ fontFamily: 'monospace', fontWeight: 700, fontSize: 13, minWidth: 48, color: '#e2e8f0' }}>
        {pos.symbol}
      </span>

      {/* Model name */}
      <span style={{ flex: 1, fontSize: 10, color: '#64748b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {pos.model}
      </span>

      {/* Unrealized P&L $ */}
      <span style={{ fontFamily: 'monospace', fontSize: 12, fontWeight: 700, color, minWidth: 72, textAlign: 'right' }}>
        {arrow} ${Math.abs(pos.unrealized_pnl).toFixed(2)}
      </span>

      {/* Unrealized % */}
      <span style={{ fontFamily: 'monospace', fontSize: 11, color, minWidth: 56, textAlign: 'right' }}>
        {pos.unrealized_pct >= 0 ? '+' : ''}{pos.unrealized_pct?.toFixed(2)}%
      </span>

      {/* Label */}
      <span style={{
        fontSize: 9, fontWeight: 700, letterSpacing: 0.5, color,
        minWidth: 32, textAlign: 'center',
        padding: '1px 4px', borderRadius: 3,
        background: isWinner ? 'rgba(37,99,235,0.15)' : 'rgba(234,88,12,0.15)',
      }}>
        {label}
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

export default function HoldingsTopWL() {
  const { data, loading, lastUpdated } = usePolling(api.getHoldingsTop, 60000)
  const isLoading = loading && !data
  const isUpdating = data?.is_updating === true

  const winners = data?.winners || []
  const losers  = data?.losers  || []

  return (
    <div className="card">
      <div className="card-header">
        <h2>{'\uD83C\uDFC6'} Top Winners &amp; {'\uD83D\uDC80'} Losers — Holdings</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {isUpdating && (
            <span style={{ fontSize: 9, color: '#60a5fa', fontWeight: 700, letterSpacing: 0.5 }}>
              {'\u21BB'} updating...
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
        <span style={{ flex: 1 }}>MODEL</span>
        <span style={{ minWidth: 72, textAlign: 'right' }}>UNREAL P&amp;L</span>
        <span style={{ minWidth: 56, textAlign: 'right' }}>UNREAL %</span>
        <span style={{ minWidth: 32, textAlign: 'center' }}></span>
        <span style={{ minWidth: 52, textAlign: 'right' }}>PRICE</span>
      </div>

      <SectionHeader label={'\u25B2 TOP WINNERS'} color={WIN_COLOR} />
      {isLoading
        ? Array.from({ length: 5 }).map((_, i) => <SkeletonRow key={i} />)
        : winners.length === 0
        ? <div className="empty-state">No open positions</div>
        : winners.map((p, i) => <PositionRow key={i} pos={p} isWinner={true} />)
      }

      <SectionHeader label={'\u25BC TOP LOSERS'} color={LOSE_COLOR} />
      {isLoading
        ? Array.from({ length: 5 }).map((_, i) => <SkeletonRow key={i} />)
        : losers.length === 0
        ? <div className="empty-state">No open positions</div>
        : losers.map((p, i) => <PositionRow key={i} pos={p} isWinner={false} />)
      }

      <div style={{ padding: '5px 14px', fontSize: 9, color: '#334155', textAlign: 'right' }}>
        All AI players · open stock positions · by unrealized P&amp;L
      </div>
    </div>
  )
}
