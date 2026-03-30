import React, { useCallback, useMemo, useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { timeAgo, formatTimeAZ } from '../utils/time'
import { LastUpdated } from './AutoRefreshToggle'

const PROVIDER_COLORS = {
  anthropic: '#22c55e',
  openai: '#22c55e',
  google: '#3b82f6',
  xai: '#ef4444',
  ollama: '#94a3b8',
  matrix: '#00bcd4',
}

const TF_META = {
  SCALP:    { label: 'S',  full: 'Scalp',    title: 'Scalp — intraday, < 1 day',   bg: '#1e3a5f', color: '#60a5fa', border: '#2563eb' },
  SWING:    { label: 'SW', full: 'Swing',    title: 'Swing — 2–10 days',            bg: '#1a3327', color: '#34d399', border: '#059669' },
  POSITION: { label: 'P',  full: 'Position', title: 'Position — 10+ days',          bg: '#3b1f5e', color: '#c084fc', border: '#7c3aed' },
}

const TF_FILTERS = ['All', 'SCALP', 'SWING', 'POSITION']

function TimeframeBadge({ tf }) {
  const meta = TF_META[tf] || TF_META.SWING
  return (
    <span title={meta.title} style={{
      fontSize: 10, fontWeight: 800, padding: '1px 6px', borderRadius: 4,
      background: meta.bg, color: meta.color, border: `1px solid ${meta.border}`,
      fontFamily: 'monospace', letterSpacing: 0.5,
    }}>
      {meta.label}
    </span>
  )
}

function PnlBadge({ pnl, pnlPct, action }) {
  if (pnl === null || pnl === undefined) return null
  const isProfit = pnl >= 0
  const color = isProfit ? '#22c55e' : '#ef4444'
  const sign = isProfit ? '+' : ''
  const label = action === 'SELL' ? '' : ' (unrealized)'
  return (
    <span style={{
      color,
      fontWeight: 600,
      fontSize: '0.85em',
      marginLeft: 8,
      padding: '2px 6px',
      borderRadius: 4,
      background: isProfit ? 'rgba(34,197,94,0.12)' : 'rgba(239,68,68,0.12)',
    }}>
      {sign}${pnl.toFixed(2)}
      {pnlPct !== null && pnlPct !== undefined && (
        <span style={{ opacity: 0.8, marginLeft: 4 }}>({sign}{pnlPct.toFixed(1)}%)</span>
      )}
      {label && <span style={{ opacity: 0.6, fontSize: '0.8em' }}>{label}</span>}
    </span>
  )
}

function SourcesBadge({ sources }) {
  const [expanded, setExpanded] = React.useState(false)
  if (!sources) return null
  const sourceList = sources.split(',').filter(Boolean)
  if (sourceList.length === 0) return null
  return (
    <span style={{ position: 'relative', display: 'inline-block' }}>
      <span
        onClick={(e) => { e.stopPropagation(); setExpanded(!expanded) }}
        style={{
          fontSize: '0.7em',
          fontWeight: 600,
          marginLeft: 6,
          padding: '1px 5px',
          borderRadius: 3,
          background: 'rgba(59,130,246,0.15)',
          color: '#60a5fa',
          cursor: 'pointer',
          border: '1px solid rgba(59,130,246,0.3)',
          userSelect: 'none',
        }}
      >
        {sourceList.length} sources {expanded ? '▾' : '▸'}
      </span>
      {expanded && (
        <span style={{
          position: 'absolute',
          top: '100%',
          left: 0,
          zIndex: 100,
          background: '#1a1f2e',
          border: '1px solid #2d3348',
          borderRadius: 6,
          padding: '6px 10px',
          marginTop: 4,
          minWidth: 160,
          fontSize: '0.75em',
          lineHeight: 1.6,
          boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
        }}>
          {sourceList.map((s, i) => (
            <div key={i} style={{ color: '#94a3b8', whiteSpace: 'nowrap' }}>
              <span style={{ color: '#60a5fa', marginRight: 4 }}>●</span>{s.trim()}
            </div>
          ))}
        </span>
      )}
    </span>
  )
}

function OptionBadge({ assetType, optionType, strikePrice, expiryDate }) {
  if (assetType !== 'option') return null
  const color = optionType === 'call' ? '#22c55e' : '#ef4444'
  return (
    <span style={{
      color,
      fontSize: '0.75em',
      fontWeight: 600,
      marginLeft: 4,
      padding: '1px 4px',
      borderRadius: 3,
      border: `1px solid ${color}`,
    }}>
      {(optionType || '?').toUpperCase()}
      {strikePrice ? ` $${strikePrice}` : ''}
      {expiryDate ? ` ${expiryDate}` : ''}
    </span>
  )
}

export default function RecentTrades({ compact = false, season, filterPlayer, onFilterPlayer }) {
  const [tfFilter, setTfFilter] = useState('All')

  const fetchTrades = useCallback(
    () => api.getRecentTrades(compact ? 10 : 30, season || undefined, tfFilter === 'All' ? undefined : tfFilter),
    [compact, season, tfFilter]
  )
  const { data: trades, loading, lastUpdated } = usePolling(fetchTrades, 15000)

  if (loading && !trades) return <div className="loading">Loading trades...</div>

  const items = trades || []

  // Unique players from current data — preserves insertion order (most recent first)
  const players = useMemo(() => {
    const seen = new Map()
    items.forEach(t => {
      if (t.player_id && !seen.has(t.player_id))
        seen.set(t.player_id, { id: t.player_id, name: t.display_name, provider: t.provider })
    })
    return Array.from(seen.values())
  }, [items])

  const visibleItems = filterPlayer ? items.filter(t => t.player_id === filterPlayer) : items

  return (
    <div className={compact ? '' : 'card'}>
      {!compact && (
        <div className="card-header">
          <h2>Recent Trades</h2>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <LastUpdated time={lastUpdated} />
            <span className="card-badge">{visibleItems.length} trades</span>
          </div>
        </div>
      )}

      {/* Player filter pills (non-compact only) */}
      {!compact && players.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, padding: '8px 16px', borderBottom: '1px solid #1e293b', alignItems: 'center' }}>
          <span style={{ fontSize: 10, fontWeight: 700, color: '#475569', letterSpacing: 1, marginRight: 2 }}>PLAYER</span>
          <button
            onClick={() => onFilterPlayer?.(null)}
            style={{
              padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700,
              cursor: 'pointer', border: '1px solid',
              background: !filterPlayer ? '#1e293b' : 'transparent',
              color: !filterPlayer ? '#e2e8f0' : '#64748b',
              borderColor: !filterPlayer ? '#475569' : '#334155',
            }}
          >
            All
          </button>
          {players.map(p => {
            const active = filterPlayer === p.id
            const color = PROVIDER_COLORS[p.provider] || '#94a3b8'
            return (
              <button
                key={p.id}
                onClick={() => onFilterPlayer?.(active ? null : p.id)}
                style={{
                  padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                  cursor: 'pointer', border: `1px solid ${active ? color : '#334155'}`,
                  background: active ? `${color}22` : 'transparent',
                  color: active ? color : '#64748b',
                  display: 'inline-flex', alignItems: 'center', gap: 5,
                }}
              >
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: color, flexShrink: 0 }} />
                {p.name}
              </button>
            )
          })}
        </div>
      )}

      {/* Timeframe filter (non-compact only) */}
      {!compact && (
        <div style={{ display: 'flex', gap: 6, padding: '8px 16px', borderBottom: '1px solid #1e293b' }}>
          {TF_FILTERS.map(f => {
            const meta = f !== 'All' ? TF_META[f] : null
            const active = tfFilter === f
            return (
              <button
                key={f}
                onClick={() => setTfFilter(f)}
                style={{
                  padding: '4px 12px', borderRadius: 6, fontSize: 11, fontWeight: 700,
                  cursor: 'pointer', border: '1px solid',
                  background: active ? (meta ? meta.bg : '#1e293b') : 'transparent',
                  color: active ? (meta ? meta.color : '#e2e8f0') : '#64748b',
                  borderColor: active ? (meta ? meta.border : '#475569') : '#334155',
                }}
              >
                {f === 'All' ? 'All' : `${TF_META[f].label} ${TF_META[f].full}`}
              </button>
            )
          })}
        </div>
      )}

      {visibleItems.length === 0 ? (
        <div className="empty-state">
          {filterPlayer
            ? `No trades from ${players.find(p => p.id === filterPlayer)?.name ?? filterPlayer}.`
            : season ? `No Season ${season} trades yet.` : 'No trades yet. Waiting for AI decisions.'}
        </div>
      ) : (
        <div className="trade-list" style={{ maxHeight: compact ? 350 : 600 }}>
          {visibleItems.map((t, i) => (
            <div key={i} className="trade-item">
              <div className="trade-left">
                <span className={`trade-action ${t.action.toLowerCase()}`}>{t.action}</span>
                <strong className="trade-symbol">{t.symbol}</strong>
                <OptionBadge
                  assetType={t.asset_type}
                  optionType={t.option_type}
                  strikePrice={t.strike_price}
                  expiryDate={t.expiry_date}
                />
                {t.timeframe && !compact && <TimeframeBadge tf={t.timeframe} />}
                <span className="mono trade-qty">
                  {t.qty} @ ${t.price?.toFixed(2)}
                </span>
                <PnlBadge pnl={t.pnl} pnlPct={t.pnl_pct} action={t.action} />
                <SourcesBadge sources={t.sources} />
              </div>
              <div className="trade-right">
                <span className="trade-by" style={{ color: PROVIDER_COLORS[t.provider] || '#94a3b8' }}>
                  {t.display_name}
                </span>
                <span className="trade-time" title={formatTimeAZ(t.executed_at)}>{formatTimeAZ(t.executed_at)} · {timeAgo(t.executed_at)}</span>
              </div>
              {!compact && t.reasoning && (
                <div className="trade-reasoning">{t.reasoning.substring(0, 200)}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
