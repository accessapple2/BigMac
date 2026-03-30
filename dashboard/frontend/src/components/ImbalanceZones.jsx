import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { LastUpdated } from './AutoRefreshToggle'

const ZONE_META = {
  demand: { icon: '◆', label: 'Demand', color: '#22c55e', bg: '#052e16', border: '#22c55e' },
  supply: { icon: '◇', label: 'Supply',  color: '#ef4444', bg: '#2d0a0a', border: '#ef4444' },
}

const TF_LABEL = { daily: 'D', hourly: '1H' }

function StrengthDots({ score }) {
  return (
    <div style={{ display: 'flex', gap: 2, alignItems: 'center' }}>
      {[...Array(10)].map((_, i) => (
        <div key={i} style={{
          width: 5, height: 10, borderRadius: 2,
          background: i < score
            ? (score >= 7 ? '#ef4444' : score >= 4 ? '#eab308' : '#22c55e')
            : '#1e293b',
        }} />
      ))}
      <span style={{ fontSize: 10, fontFamily: 'monospace', color: '#64748b', marginLeft: 3 }}>{score}/10</span>
    </div>
  )
}

function ZoneRow({ zone, compact = false }) {
  const meta = ZONE_META[zone.zone_type] || ZONE_META.demand
  const isApproaching = zone.approaching
  return (
    <div style={{
      padding: compact ? '8px 14px' : '12px 16px',
      borderBottom: '1px solid #1e293b',
      borderLeft: `3px solid ${isApproaching ? '#f59e0b' : meta.border}`,
      display: 'flex', alignItems: 'center', gap: 12,
    }}>
      {/* Zone type icon */}
      <div style={{ minWidth: 20, textAlign: 'center' }}>
        <span style={{ fontSize: 18, color: meta.color, fontWeight: 800 }}>{meta.icon}</span>
      </div>

      {/* Main info */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 3 }}>
          <span style={{ fontWeight: 800, fontFamily: 'monospace', fontSize: 14 }}>{zone.ticker}</span>
          <span style={{
            fontSize: 10, fontWeight: 700, padding: '1px 7px', borderRadius: 4,
            background: meta.bg, color: meta.color, border: `1px solid ${meta.border}`,
          }}>
            {meta.icon} {meta.label}
          </span>
          <span style={{
            fontSize: 10, padding: '1px 5px', borderRadius: 3,
            background: '#1e293b', color: '#94a3b8', border: '1px solid #334155',
            fontFamily: 'monospace', fontWeight: 700,
          }}>
            {TF_LABEL[zone.timeframe] || zone.timeframe}
          </span>
          {isApproaching && (
            <span style={{
              fontSize: 10, fontWeight: 700, padding: '1px 7px', borderRadius: 4,
              background: '#451a03', color: '#f59e0b', border: '1px solid #b45309',
            }}>
              → {zone.distance_pct?.toFixed(2)}% away
            </span>
          )}
          {zone.tested === 1 && (
            <span style={{
              fontSize: 10, padding: '1px 5px', borderRadius: 3,
              background: '#1e293b', color: '#64748b', border: '1px solid #334155',
            }}>tested</span>
          )}
        </div>
        <div style={{ fontSize: 12, color: '#94a3b8', fontFamily: 'monospace' }}>
          ${zone.price_low?.toFixed(2)} – ${zone.price_high?.toFixed(2)}
          {zone.created_date ? <span style={{ color: '#475569', marginLeft: 8 }}>formed {zone.created_date}</span> : null}
        </div>
        <div style={{ marginTop: 4 }}>
          <StrengthDots score={zone.zone_strength || 0} />
        </div>
      </div>
    </div>
  )
}

export default function ImbalanceZones() {
  const [tab, setTab] = useState('untested')
  const [filterTicker, setFilterTicker] = useState('')

  const fetchUntested = useCallback(() => api.getImbalanceZones(null, 200), [])
  const fetchAll      = useCallback(() => api.getImbalanceAll(null, 300), [])

  const { data: untestedData, loading: untestedLoading, lastUpdated } = usePolling(fetchUntested, 120000)
  const { data: allData,      loading: allLoading }                   = usePolling(fetchAll,      180000)

  const untested = untestedData?.zones || []
  const all      = allData?.zones || []

  const tickerFilter = filterTicker.trim().toUpperCase()
  const filteredUntested = tickerFilter ? untested.filter(z => z.ticker === tickerFilter) : untested
  const filteredAll      = tickerFilter ? all.filter(z => z.ticker === tickerFilter) : all

  // Split untested into approaching vs normal
  const approaching = filteredUntested.filter(z => z.approaching)
  const standard    = filteredUntested.filter(z => !z.approaching)

  const tabStyle = (id) => ({
    padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
    background: tab === id ? '#00d4aa' : '#1a1a2e',
    color:      tab === id ? '#0a0a1a' : '#94a3b8',
    border: '1px solid #333',
  })

  return (
    <div>
      {/* Tabs */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <button style={tabStyle('untested')} onClick={() => setTab('untested')}>
          Untested ({filteredUntested.length})
        </button>
        <button style={tabStyle('all')} onClick={() => setTab('all')}>
          All Zones ({filteredAll.length})
        </button>
        <input
          value={filterTicker}
          onChange={e => setFilterTicker(e.target.value)}
          placeholder="Filter by ticker…"
          style={{
            marginLeft: 'auto', padding: '5px 10px', borderRadius: 6, fontSize: 12,
            background: '#1a1a2e', color: '#e2e8f0', border: '1px solid #333',
            outline: 'none', width: 130,
          }}
        />
      </div>

      {/* Legend */}
      <div className="card" style={{ marginBottom: 16, padding: '10px 16px' }}>
        <div style={{ display: 'flex', gap: 20, flexWrap: 'wrap', fontSize: 12, color: '#94a3b8' }}>
          <span><span style={{ color: '#22c55e', fontWeight: 700 }}>◆</span> Demand zone (bullish — unfilled buy orders)</span>
          <span><span style={{ color: '#ef4444', fontWeight: 700 }}>◇</span> Supply zone (bearish — unfilled sell orders)</span>
          <span><span style={{ color: '#f59e0b', fontWeight: 700 }}>→</span> Approaching (within 1%)</span>
          <span style={{ color: '#64748b' }}>Strength: <span style={{ color: '#22c55e' }}>1–3</span> weak · <span style={{ color: '#eab308' }}>4–6</span> moderate · <span style={{ color: '#ef4444' }}>7–10</span> strong</span>
        </div>
      </div>

      {/* Untested tab */}
      {tab === 'untested' && (
        <div>
          {approaching.length > 0 && (
            <div className="card" style={{ marginBottom: 12, border: '1px solid #b45309' }}>
              <div className="card-header" style={{ borderBottom: '1px solid #b45309' }}>
                <h2 style={{ color: '#f59e0b' }}>→ Approaching Zones</h2>
                <span className="card-badge" style={{ background: '#451a03', color: '#f59e0b' }}>
                  {approaching.length} zone{approaching.length !== 1 ? 's' : ''}
                </span>
              </div>
              <div className="trade-list">
                {approaching.map((z, i) => <ZoneRow key={i} zone={z} />)}
              </div>
            </div>
          )}

          <div className="card">
            <div className="card-header">
              <h2>Untested Imbalance Zones</h2>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <LastUpdated time={lastUpdated} />
                <span className="card-badge">{filteredUntested.length} zones</span>
              </div>
            </div>
            {untestedLoading && untested.length === 0 ? (
              <div className="loading">Scanning for imbalance zones…</div>
            ) : standard.length === 0 && approaching.length === 0 ? (
              <div className="empty-state">No untested imbalance zones found. Run a scan to populate zones.</div>
            ) : (
              <div className="trade-list">
                {standard.map((z, i) => <ZoneRow key={i} zone={z} />)}
              </div>
            )}
          </div>
        </div>
      )}

      {/* All zones tab */}
      {tab === 'all' && (
        <div className="card">
          <div className="card-header">
            <h2>All Imbalance Zones</h2>
            <span className="card-badge">{filteredAll.length} zones</span>
          </div>
          {allLoading && all.length === 0 ? (
            <div className="loading">Loading zone history…</div>
          ) : filteredAll.length === 0 ? (
            <div className="empty-state">No imbalance zones recorded yet.</div>
          ) : (
            <div className="trade-list">
              {filteredAll.map((z, i) => <ZoneRow key={i} zone={z} />)}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
