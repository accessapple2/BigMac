import React from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'

function TrendBadge({ trend }) {
  if (!trend) return null
  const colors = {
    RISING: { bg: '#1a3a2a', text: '#6ec891', arrow: '\u2191' },
    FALLING: { bg: '#3a1a1a', text: '#c86e6e', arrow: '\u2193' },
    MIXED: { bg: '#3a3a1a', text: '#c8c86e', arrow: '\u2194' },
    STABLE: { bg: '#2a2a2a', text: '#999', arrow: '\u2192' },
  }
  const c = colors[trend] || colors.STABLE
  return (
    <span style={{
      background: c.bg, color: c.text, padding: '2px 8px',
      borderRadius: '4px', fontSize: '11px', fontWeight: 600, marginLeft: '8px'
    }}>
      {c.arrow} {trend}
    </span>
  )
}

function MacroCard({ title, value, unit, date, trend, description }) {
  return (
    <div style={{
      background: '#1a1a1a', borderRadius: '8px', padding: '16px',
      display: 'flex', flexDirection: 'column', gap: '8px'
    }}>
      <div style={{ color: '#888', fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.5px', fontWeight: 600 }}>
        {title}
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: '4px' }}>
        <span style={{ fontSize: '28px', fontWeight: 700, color: '#fff' }}>{value}</span>
        <span style={{ fontSize: '14px', color: '#888' }}>{unit}</span>
        <TrendBadge trend={trend} />
      </div>
      {date && <div style={{ fontSize: '11px', color: '#555' }}>As of {date}</div>}
      {description && <div style={{ fontSize: '12px', color: '#777' }}>{description}</div>}
    </div>
  )
}

export default function EconomicCalendar() {
  const { data: econ } = usePolling(api.getEconomicCalendar, 120000)
  const { data: earnings } = usePolling(() => api.getMarketEarnings?.() || Promise.resolve([]), 120000)

  if (!econ) {
    return (
      <div className="card">
        <div className="card-header"><h2>Economic Calendar</h2></div>
        <div style={{ padding: '24px', color: '#666', textAlign: 'center' }}>Loading macro data...</div>
      </div>
    )
  }

  const { cpi, unemployment, interest_rate, gdp, fomc } = econ

  return (
    <div>
      {/* Macro indicators grid */}
      <div className="card" style={{ marginBottom: '16px' }}>
        <div className="card-header">
          <h2>Macro Indicators</h2>
          <span className="card-badge">Live</span>
        </div>
        <div style={{
          display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
          gap: '12px', padding: '16px'
        }}>
          {cpi && (
            <MacroCard
              title="CPI (Inflation)"
              value={cpi.value}
              unit="% YoY"
              date={cpi.date}
              trend={cpi.trend}
            />
          )}
          {unemployment && (
            <MacroCard
              title="Unemployment Rate"
              value={unemployment.value}
              unit="%"
              date={unemployment.date}
              trend={unemployment.trend}
            />
          )}
          {interest_rate && (
            <MacroCard
              title="Interest Rate"
              value={interest_rate.value}
              unit="%"
              date={interest_rate.date}
              trend={interest_rate.trend}
            />
          )}
          {gdp && (
            <MacroCard
              title="GDP"
              value={`$${gdp.value_billions.toFixed(0)}B`}
              unit=""
              date={gdp.date}
              description={`QoQ Growth: ${gdp.qoq_growth > 0 ? '+' : ''}${gdp.qoq_growth}%`}
            />
          )}
        </div>
      </div>

      {/* Market Regime Summary */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
        {/* FOMC */}
        <div className="card">
          <div className="card-header">
            <h2>FOMC / Fed</h2>
            <span className="card-badge">{fomc?.length || 0} docs</span>
          </div>
          <div style={{ padding: '12px' }}>
            {fomc && fomc.length > 0 ? (
              fomc.slice(0, 8).map((item, i) => (
                <div key={i} style={{
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  padding: '8px 0', borderBottom: '1px solid #1a1a1a'
                }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ color: '#ddd', fontSize: '13px', fontWeight: 500 }}>
                      {item.title || 'FOMC Document'}
                    </div>
                    <div style={{ color: '#666', fontSize: '11px' }}>{item.date}</div>
                  </div>
                </div>
              ))
            ) : (
              <div style={{ color: '#555', padding: '12px', textAlign: 'center' }}>No FOMC data available</div>
            )}
          </div>
        </div>

        {/* Macro Impact Summary */}
        <div className="card">
          <div className="card-header">
            <h2>Macro Impact</h2>
          </div>
          <div style={{ padding: '16px' }}>
            <MacroImpactItem
              label="Inflation Environment"
              value={cpi ? (cpi.value > 3 ? 'HOT' : cpi.value > 2 ? 'WARM' : 'COOL') : '-'}
              detail={cpi ? `${cpi.value}% CPI suggests ${cpi.value > 3 ? 'hawkish Fed, pressure on growth stocks' : cpi.value > 2 ? 'moderate policy, balanced outlook' : 'potential easing, risk-on'}` : ''}
              color={cpi ? (cpi.value > 3 ? '#c86e6e' : cpi.value > 2 ? '#c8c86e' : '#6ec891') : '#666'}
            />
            <MacroImpactItem
              label="Labor Market"
              value={unemployment ? (unemployment.value < 4 ? 'TIGHT' : unemployment.value < 5 ? 'BALANCED' : 'LOOSE') : '-'}
              detail={unemployment ? `${unemployment.value}% unemployment — ${unemployment.value < 4 ? 'wage pressure, consumer strength' : unemployment.value < 5 ? 'healthy equilibrium' : 'weakening demand, defensive posture'}` : ''}
              color={unemployment ? (unemployment.value < 4 ? '#6ec891' : unemployment.value < 5 ? '#c8c86e' : '#c86e6e') : '#666'}
            />
            <MacroImpactItem
              label="Rate Environment"
              value={interest_rate ? (interest_rate.value > 5 ? 'RESTRICTIVE' : interest_rate.value > 3 ? 'NEUTRAL' : 'ACCOMMODATIVE') : '-'}
              detail={interest_rate ? `${interest_rate.value}% rate — ${interest_rate.value > 5 ? 'high cost of capital, favors value' : interest_rate.value > 3 ? 'normal rates, stock picking matters' : 'cheap money, favors growth'}` : ''}
              color={interest_rate ? (interest_rate.value > 5 ? '#c86e6e' : interest_rate.value > 3 ? '#c8c86e' : '#6ec891') : '#666'}
            />
            <MacroImpactItem
              label="GDP Momentum"
              value={gdp ? (gdp.qoq_growth > 1 ? 'EXPANDING' : gdp.qoq_growth > 0 ? 'SLOW GROWTH' : 'CONTRACTING') : '-'}
              detail={gdp ? `${gdp.qoq_growth > 0 ? '+' : ''}${gdp.qoq_growth}% QoQ — ${gdp.qoq_growth > 1 ? 'strong expansion, risk-on' : gdp.qoq_growth > 0 ? 'moderate growth' : 'recession risk, defensive'}` : ''}
              color={gdp ? (gdp.qoq_growth > 1 ? '#6ec891' : gdp.qoq_growth > 0 ? '#c8c86e' : '#c86e6e') : '#666'}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

function MacroImpactItem({ label, value, detail, color }) {
  return (
    <div style={{ padding: '10px 0', borderBottom: '1px solid #1a1a1a' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ color: '#999', fontSize: '13px' }}>{label}</span>
        <span style={{ color, fontWeight: 700, fontSize: '13px' }}>{value}</span>
      </div>
      {detail && <div style={{ color: '#666', fontSize: '11px', marginTop: '4px' }}>{detail}</div>}
    </div>
  )
}
