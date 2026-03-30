import React, { useState } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'

// Static company name lookup for common tickers
const NAMES = {
  AAPL:'Apple', MSFT:'Microsoft', NVDA:'NVIDIA', AVGO:'Broadcom', CRM:'Salesforce',
  'BRK-B':'Berkshire', JPM:'JPMorgan', V:'Visa', MA:'Mastercard', BAC:'Bank of America',
  LLY:'Eli Lilly', UNH:'UnitedHealth', JNJ:'J&J', ABBV:'AbbVie', MRK:'Merck',
  XOM:'ExxonMobil', CVX:'Chevron', COP:'ConocoPhillips', SLB:'SLB', EOG:'EOG Res.',
  AMZN:'Amazon', TSLA:'Tesla', HD:'Home Depot', MCD:"McDonald's", NKE:'Nike',
  PG:'Procter&Gamble', COST:'Costco', KO:'Coca-Cola', PEP:'PepsiCo', WMT:'Walmart',
  GE:'GE Aerospace', CAT:'Caterpillar', UNP:'Union Pacific', HON:'Honeywell', BA:'Boeing',
  LIN:'Linde', APD:'Air Products', SHW:'Sherwin-Williams', FCX:'Freeport', NEM:'Newmont',
  PLD:'Prologis', AMT:'Am. Tower', EQIX:'Equinix', SPG:'Simon Property', O:'Realty Income',
  NEE:'NextEra', SO:'Southern Co.', DUK:'Duke Energy', CEG:'Constellation', SRE:'Sempra',
  META:'Meta', GOOGL:'Alphabet', NFLX:'Netflix', DIS:'Disney', CMCSA:'Comcast',
  // Defense/Aero
  LMT:'Lockheed Martin', RTX:'RTX Corp', NOC:'Northrop Grumman', GD:'General Dynamics',
  LHX:'L3Harris', HII:'Huntington Ingalls', LDOS:'Leidos', BAH:'Booz Allen',
}

function heatColor(pct) {
  if (pct >= 2)    return { bg: '#052e16', color: '#4ade80', border: '#16a34a' }
  if (pct >= 1)    return { bg: '#0a2e1a', color: '#86efac', border: '#15803d' }
  if (pct >= 0.3)  return { bg: '#0f2a12', color: '#bbf7d0', border: '#166534' }
  if (pct >= 0)    return { bg: '#1a2e1a', color: '#6ee7b7', border: '#1e3a2a' }
  if (pct >= -0.3) return { bg: '#2e1a1a', color: '#fca5a5', border: '#3a1e1e' }
  if (pct >= -1)   return { bg: '#2e0a0a', color: '#f87171', border: '#7f1d1d' }
  if (pct >= -2)   return { bg: '#2e0505', color: '#ef4444', border: '#991b1b' }
  return            { bg: '#1e0000', color: '#dc2626', border: '#7f1d1d' }
}

function SkeletonCard() {
  return (
    <div style={{
      background: '#0f172a', border: '1px solid #1e293b', borderRadius: 8,
      padding: '12px 14px', display: 'flex', flexDirection: 'column', gap: 8,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <div style={{ width: 80, height: 12, background: '#1e293b', borderRadius: 3 }} />
          <div style={{ width: 40, height: 10, background: '#1e293b', borderRadius: 3 }} />
        </div>
        <div style={{ width: 52, height: 22, background: '#1e293b', borderRadius: 4 }} />
      </div>
      <div style={{ display: 'flex', gap: 4 }}>
        {[60, 50, 55, 45, 58].map((w, i) => (
          <div key={i} style={{ width: w, height: 18, background: '#1e293b', borderRadius: 3 }} />
        ))}
      </div>
    </div>
  )
}

function HoldingRow({ h, onTickerClick }) {
  const c = heatColor(h.change_pct)
  const isPos = h.change_pct >= 0
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 10,
      padding: '5px 0', borderBottom: '1px solid #0f172a',
    }}>
      <button
        onClick={() => onTickerClick(h.symbol)}
        style={{
          minWidth: 52, fontFamily: 'monospace', fontWeight: 700, fontSize: 12,
          color: '#00d4aa', background: 'none', border: 'none', cursor: 'pointer',
          padding: 0, textAlign: 'left', textDecoration: 'underline',
          textDecorationColor: '#0e7490',
        }}
      >
        {h.symbol}
      </button>
      <span style={{ flex: 1, fontSize: 11, color: '#64748b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {NAMES[h.symbol] || ''}
      </span>
      {h.price > 0 && (
        <span style={{ fontSize: 11, fontFamily: 'monospace', color: '#94a3b8', minWidth: 52, textAlign: 'right' }}>
          ${h.price.toFixed(2)}
        </span>
      )}
      <span style={{
        fontSize: 11, fontFamily: 'monospace', fontWeight: 700,
        color: c.color, minWidth: 52, textAlign: 'right',
      }}>
        {isPos ? '+' : ''}{h.change_pct?.toFixed(2)}%
      </span>
    </div>
  )
}

function SectorCard({ sector, onTickerClick }) {
  const [expanded, setExpanded] = useState(false)
  const colors = heatColor(sector.change_pct)
  const isPos = sector.change_pct >= 0
  const previewHoldings = sector.holdings?.slice(0, 5) || []

  return (
    <div style={{
      background: colors.bg, border: `1px solid ${colors.border}`,
      borderRadius: 8, padding: '12px 14px',
      display: 'flex', flexDirection: 'column', gap: 8,
      gridColumn: expanded ? '1 / -1' : undefined,
      transition: 'all 0.15s ease',
    }}>
      {/* Header — click to expand */}
      <div
        onClick={() => setExpanded(e => !e)}
        style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', cursor: 'pointer' }}
      >
        <div>
          <div style={{ fontSize: 12, fontWeight: 700, color: '#e2e8f0' }}>{sector.sector}</div>
          <div style={{ fontSize: 10, color: '#64748b', fontFamily: 'monospace' }}>
            {sector.etf}
            {sector.source === 'finviz' && (
              <span style={{ color: '#475569', marginLeft: 5 }}>· finviz</span>
            )}
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ fontSize: 18, fontWeight: 800, fontFamily: 'monospace', color: colors.color }}>
            {isPos ? '+' : ''}{sector.change_pct?.toFixed(2)}%
          </div>
          <span style={{ fontSize: 12, color: '#475569' }}>{expanded ? '▲' : '▼'}</span>
        </div>
      </div>

      {/* Collapsed: preview chips */}
      {!expanded && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
          {previewHoldings.map(h => {
            const hc = heatColor(h.change_pct)
            return (
              <span
                key={h.symbol}
                title={`${h.symbol}: ${h.change_pct >= 0 ? '+' : ''}${h.change_pct?.toFixed(2)}%`}
                onClick={e => { e.stopPropagation(); onTickerClick(h.symbol) }}
                style={{
                  fontSize: 9, fontWeight: 700, fontFamily: 'monospace',
                  padding: '2px 5px', borderRadius: 3, cursor: 'pointer',
                  background: hc.bg, color: hc.color, border: `1px solid ${hc.border}`,
                }}
              >
                {h.symbol}
              </span>
            )
          })}
          {sector.holdings?.length > 5 && (
            <span style={{ fontSize: 9, color: '#475569', alignSelf: 'center' }}>
              +{sector.holdings.length - 5} more
            </span>
          )}
        </div>
      )}

      {/* Expanded: full holdings table */}
      {expanded && (
        <div style={{ borderTop: `1px solid ${colors.border}`, paddingTop: 8 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0 24px' }}>
            {(sector.holdings || []).map(h => (
              <HoldingRow key={h.symbol} h={h} onTickerClick={onTickerClick} />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function formatAge(seconds) {
  if (seconds == null) return null
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  return m > 0 ? `${h}h ${m}m ago` : `${h}h ago`
}

export default function SectorHeatmap({ onTickerClick }) {
  const { data, loading, lastUpdated } = usePolling(api.getSectorHeatmap, 60000)
  const sectors = data?.sectors || []
  const isLoading = loading && sectors.length === 0
  const isUpdating = data?.is_updating === true
  const cacheAge = data?.cache_age_seconds ?? null
  const isStale = cacheAge != null && cacheAge > 600  // > 10 min = market likely closed
  const spyPct = data?.spy_change_pct ?? null

  // Default ticker click: open ChartAnalyzer via URL hash or callback
  const handleTicker = onTickerClick || ((sym) => {
    window.dispatchEvent(new CustomEvent('tm:navigate', { detail: { page: 'chart-analyzer', symbol: sym } }))
  })

  return (
    <div className="card">
      <div className="card-header">
        <h2>Sector Heatmap</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {isUpdating && (
            <span style={{ fontSize: 9, color: '#60a5fa', fontWeight: 700, letterSpacing: 0.5 }}>
              ↻ updating...
            </span>
          )}
          {cacheAge != null && (
            <span style={{ fontSize: 10, color: isStale ? '#f59e0b' : '#64748b' }}>
              {isStale ? '⏸ ' : ''}Last updated: {formatAge(cacheAge)}
            </span>
          )}
          {!isLoading && <span className="card-badge">{data?.total_sectors ?? sectors.length} sectors</span>}
        </div>
      </div>

      {/* Market closed banner */}
      {isStale && !isLoading && (
        <div style={{
          margin: '0 16px 0', padding: '6px 12px', borderRadius: 6,
          background: 'rgba(245,158,11,0.1)', border: '1px solid rgba(245,158,11,0.3)',
          fontSize: 11, color: '#f59e0b', display: 'flex', alignItems: 'center', gap: 6,
        }}>
          <span>⏸</span>
          <span>Market closed — showing today's close ({formatAge(cacheAge)})</span>
          {spyPct != null && (
            <span style={{ marginLeft: 'auto', fontFamily: 'monospace', fontWeight: 700 }}>
              SPY {spyPct >= 0 ? '+' : ''}{spyPct.toFixed(2)}%
            </span>
          )}
        </div>
      )}

      {/* Color legend */}
      {!isLoading && (
        <div style={{
          display: 'flex', gap: 10, padding: '8px 16px 12px',
          borderBottom: '1px solid #1e293b', flexWrap: 'wrap', alignItems: 'center',
        }}>
          {[
            { label: '≥+2%', bg: '#052e16', color: '#4ade80' },
            { label: '+1–2%', bg: '#0a2e1a', color: '#86efac' },
            { label: '0–1%', bg: '#0f2a12', color: '#bbf7d0' },
            { label: '0–(1)%', bg: '#2e1a1a', color: '#fca5a5' },
            { label: '(1–2)%', bg: '#2e0a0a', color: '#f87171' },
            { label: '≤(2)%', bg: '#1e0000', color: '#dc2626' },
          ].map(l => (
            <span key={l.label} style={{
              fontSize: 9, fontWeight: 700, padding: '2px 6px', borderRadius: 3,
              background: l.bg, color: l.color, fontFamily: 'monospace',
            }}>
              {l.label}
            </span>
          ))}
          <span style={{ fontSize: 10, color: '#475569' }}>Click card to expand · Click ticker to view chart</span>
        </div>
      )}

      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(210px, 1fr))',
        gap: 10, padding: 16,
      }}>
        {isLoading
          ? Array.from({ length: 12 }).map((_, i) => <SkeletonCard key={i} />)
          : sectors.map(s => <SectorCard key={s.sector} sector={s} onTickerClick={handleTicker} />)
        }
      </div>
    </div>
  )
}
