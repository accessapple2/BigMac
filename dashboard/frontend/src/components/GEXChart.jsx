import React, { useState, useEffect, useCallback, useRef } from 'react'
import {
  BarChart, Bar, Cell, XAxis, YAxis, Tooltip,
  ReferenceLine, ResponsiveContainer, CartesianGrid,
} from 'recharts'

const SYMBOLS = ['SPY', 'QQQ', 'NVDA', 'TSLA', 'AAPL']

// CB-safe palette (matches existing TradeMinds theme)
const COLOR_POS_DEFAULT = '#3b82f6'   // blue — call wall / positive GEX
const COLOR_NEG_DEFAULT = '#f97316'   // orange — put wall / negative GEX
const COLOR_SPOT = '#f59e0b'           // amber reference line

function isColorblind() {
  try {
    return document.documentElement.getAttribute('data-cb') === 'true' ||
      document.documentElement.getAttribute('data-cb') === '1'
  } catch { return false }
}

function StatPill({ label, value, highlight }) {
  return (
    <div style={{
      background: 'var(--card-bg, #111827)',
      border: '1px solid var(--card-border, #1e3a5f)',
      borderRadius: 6,
      padding: '6px 12px',
      textAlign: 'center',
      minWidth: 100,
    }}>
      <div style={{ fontSize: 11, color: 'var(--text-muted, #64748b)', letterSpacing: 1 }}>{label}</div>
      <div style={{
        fontSize: 15,
        fontWeight: 'bold',
        color: highlight || 'var(--text, #e0e6f0)',
        marginTop: 2,
        fontFamily: "'Courier New', monospace",
      }}>{value}</div>
    </div>
  )
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  return (
    <div style={{
      background: 'var(--card-bg, #0f172a)',
      border: '1px solid var(--card-border, #1e3a5f)',
      borderRadius: 6,
      padding: '8px 12px',
      fontSize: 11,
      fontFamily: "'Courier New', monospace",
    }}>
      <div style={{ color: '#94a3b8', marginBottom: 4 }}>Strike ${label}</div>
      <div style={{ color: d?.net_gex >= 0 ? COLOR_POS_DEFAULT : COLOR_NEG_DEFAULT }}>
        Net GEX: {(d?.net_gex || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}
      </div>
      <div style={{ color: COLOR_POS_DEFAULT, opacity: 0.8 }}>
        Call GEX: {(d?.call_gex || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}
      </div>
      <div style={{ color: COLOR_NEG_DEFAULT, opacity: 0.8 }}>
        Put GEX: {(d?.put_gex || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}
      </div>
      {d?.call_oi > 0 && (
        <div style={{ color: '#64748b', marginTop: 4 }}>
          C OI: {d.call_oi.toLocaleString()} / P OI: {d.put_oi.toLocaleString()}
        </div>
      )}
    </div>
  )
}

export default function GEXChart() {
  const [symbol, setSymbol] = useState('SPY')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [lastUpdated, setLastUpdated] = useState(null)
  const timerRef = useRef(null)

  const fetchGEX = useCallback(async (sym) => {
    setLoading(true)
    setError(null)
    try {
      const resp = await fetch(`/api/gex/${sym}`)
      const json = await resp.json()
      if (json.error) {
        setError(json.error)
        setData(null)
      } else {
        setData(json)
        setLastUpdated(new Date())
      }
    } catch (e) {
      setError(e.message)
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [])

  // Auto-refresh every 30s (same cadence as the Bridge)
  useEffect(() => {
    fetchGEX(symbol)
    timerRef.current = setInterval(() => fetchGEX(symbol), 30000)
    return () => clearInterval(timerRef.current)
  }, [symbol, fetchGEX])

  // Chart data — filter to strikes within ±8% of spot for readability
  const chartData = React.useMemo(() => {
    if (!data?.levels || !data?.spot) return []
    const lo = data.spot * 0.92
    const hi = data.spot * 1.08
    return data.levels
      .filter(l => l.strike >= lo && l.strike <= hi)
      .map(l => ({
        strike: l.strike,
        net_gex: l.net_gex,
        call_gex: l.call_gex,
        put_gex: l.put_gex,
        call_oi: l.call_oi || 0,
        put_oi: l.put_oi || 0,
      }))
  }, [data])

  const regimeColor = data?.total_gex > 0 ? COLOR_POS_DEFAULT : COLOR_NEG_DEFAULT
  const regimeLabel = data?.regime === 'pinned'
    ? '⬤ PINNED'
    : '⬤ VOLATILE'

  return (
    <div style={{ fontFamily: "'Courier New', monospace" }}>

      {/* Header row */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', gap: 6 }}>
          {SYMBOLS.map(s => (
            <button
              key={s}
              onClick={() => setSymbol(s)}
              style={{
                padding: '4px 12px',
                borderRadius: 6,
                border: '1px solid',
                borderColor: symbol === s ? '#3b82f6' : 'var(--card-border, #1e3a5f)',
                background: symbol === s ? '#1e3a5f' : 'var(--card-bg, #111827)',
                color: symbol === s ? '#93c5fd' : 'var(--text-muted, #64748b)',
                cursor: 'pointer',
                fontSize: 12,
                letterSpacing: 1,
                fontFamily: 'inherit',
              }}
            >
              {s}
            </button>
          ))}
        </div>
        <button
          onClick={() => fetchGEX(symbol)}
          disabled={loading}
          style={{
            padding: '4px 12px',
            borderRadius: 6,
            border: '1px solid #334155',
            background: '#1e293b',
            color: loading ? '#64748b' : '#60a5fa',
            cursor: loading ? 'wait' : 'pointer',
            fontSize: 12,
            fontFamily: 'inherit',
          }}
        >
          {loading ? '...' : '↻ Refresh'}
        </button>
        {lastUpdated && (
          <span style={{ fontSize: 10, color: '#64748b' }}>
            Updated {lastUpdated.toLocaleTimeString()}
          </span>
        )}
      </div>

      {error && (
        <div style={{
          background: '#1c0f0f', border: '1px solid #7f1d1d',
          borderRadius: 6, padding: '10px 14px',
          color: '#fca5a5', fontSize: 12, marginBottom: 12,
        }}>
          {error}
          {error.includes('Alpaca keys') && (
            <div style={{ marginTop: 4, color: '#94a3b8' }}>
              Set ALPACA_API_KEY + ALPACA_SECRET_KEY in your .env file.
            </div>
          )}
        </div>
      )}

      {data && (
        <>
          {/* Key levels stat pills */}
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 16 }}>
            <StatPill label="SPOT" value={`$${data.spot?.toFixed(2)}`} />
            <StatPill
              label="REGIME"
              value={regimeLabel}
              highlight={regimeColor}
            />
            <StatPill
              label="MAX GAMMA (PIN)"
              value={`$${data.max_gamma_strike?.toFixed(0)}`}
              highlight={COLOR_POS_DEFAULT}
            />
            <StatPill
              label="GAMMA FLIP"
              value={`$${data.gamma_flip?.toFixed(0)}`}
              highlight={data.spot > data.gamma_flip ? COLOR_POS_DEFAULT : COLOR_NEG_DEFAULT}
            />
            <StatPill
              label="CALL WALL"
              value={`$${data.call_wall?.toFixed(0)}`}
              highlight={COLOR_POS_DEFAULT}
            />
            <StatPill
              label="PUT WALL"
              value={`$${data.put_wall?.toFixed(0)}`}
              highlight={COLOR_NEG_DEFAULT}
            />
            <StatPill
              label="TOTAL GEX"
              value={(data.total_gex || 0) >= 0
                ? `+${(data.total_gex || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}`
                : (data.total_gex || 0).toLocaleString('en-US', { maximumFractionDigits: 0 })
              }
              highlight={data.total_gex >= 0 ? COLOR_POS_DEFAULT : COLOR_NEG_DEFAULT}
            />
          </div>

          {/* Bar chart */}
          {chartData.length > 0 ? (
            <>
              <div style={{ fontSize: 10, color: '#64748b', marginBottom: 6, letterSpacing: 1 }}>
                NET GAMMA EXPOSURE BY STRIKE (±8% of spot) &nbsp;
                <span style={{ color: COLOR_POS_DEFAULT }}>■ Positive (Call Wall / Pin)</span>
                &nbsp;
                <span style={{ color: COLOR_NEG_DEFAULT }}>■ Negative (Put Wall / Trend)</span>
                &nbsp;
                <span style={{ color: COLOR_SPOT }}>│ Spot</span>
              </div>
              <ResponsiveContainer width="100%" height={260}>
                <BarChart
                  data={chartData}
                  margin={{ top: 4, right: 8, left: 8, bottom: 4 }}
                  barCategoryGap="15%"
                >
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="rgba(148,163,184,0.08)"
                    vertical={false}
                  />
                  <XAxis
                    dataKey="strike"
                    tick={{ fill: '#64748b', fontSize: 10, fontFamily: "'Courier New', monospace" }}
                    tickFormatter={v => `$${v}`}
                    axisLine={{ stroke: '#1e3a5f' }}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fill: '#64748b', fontSize: 10, fontFamily: "'Courier New', monospace" }}
                    tickFormatter={v => v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}K` : v}
                    axisLine={{ stroke: '#1e3a5f' }}
                    tickLine={false}
                    width={48}
                  />
                  <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(59,130,246,0.06)' }} />
                  <ReferenceLine y={0} stroke="#334155" strokeWidth={1} />
                  {/* Spot price vertical line */}
                  <ReferenceLine
                    x={chartData.reduce((closest, d) =>
                      Math.abs(d.strike - data.spot) < Math.abs(closest - data.spot)
                        ? d.strike : closest,
                      chartData[0]?.strike || data.spot
                    )}
                    stroke={COLOR_SPOT}
                    strokeWidth={2}
                    strokeDasharray="4 3"
                    label={{
                      value: `SPOT $${data.spot?.toFixed(0)}`,
                      position: 'top',
                      fill: COLOR_SPOT,
                      fontSize: 10,
                      fontFamily: "'Courier New', monospace",
                    }}
                  />
                  <Bar dataKey="net_gex" radius={[2, 2, 0, 0]}>
                    {chartData.map((entry, idx) => (
                      <Cell
                        key={idx}
                        fill={entry.net_gex >= 0 ? COLOR_POS_DEFAULT : COLOR_NEG_DEFAULT}
                        fillOpacity={0.85}
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </>
          ) : (
            <div style={{ color: '#64748b', fontSize: 12, padding: '20px 0', textAlign: 'center' }}>
              No strike data in ±8% range of spot price.
            </div>
          )}

          {/* Interpretation */}
          <div style={{
            marginTop: 12,
            padding: '10px 14px',
            background: 'var(--card-bg, #111827)',
            border: '1px solid var(--card-border, #1e3a5f)',
            borderRadius: 6,
            fontSize: 11,
            color: '#64748b',
            lineHeight: 1.6,
          }}>
            <strong style={{ color: '#94a3b8' }}>GEX Interpretation:</strong>
            &nbsp;
            <span style={{ color: COLOR_POS_DEFAULT }}>Blue bars</span> = positive GEX (call walls) → dealers buy dips here → price tends to PIN.&nbsp;
            <span style={{ color: COLOR_NEG_DEFAULT }}>Orange bars</span> = negative GEX (put walls) → dealers sell rallies → expect momentum through these.
            &nbsp;Gamma flip (${data.gamma_flip?.toFixed(0)}): above = pinned, below = volatile.
            {data.timestamp && (
              <span style={{ marginLeft: 8, color: '#475569' }}>
                Data: {data.source || 'alpaca'} · {new Date(data.timestamp).toLocaleString()}
              </span>
            )}
          </div>
        </>
      )}

      {!data && !error && !loading && (
        <div style={{ color: '#64748b', fontSize: 12, padding: '20px 0', textAlign: 'center' }}>
          Select a symbol to view GEX profile.
        </div>
      )}
    </div>
  )
}
