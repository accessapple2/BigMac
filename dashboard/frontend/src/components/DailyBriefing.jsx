import React, { useState, useEffect, useCallback } from 'react'
import { api } from '../api/client'
import { getDisplayCapital, getPortfolioDisplayName, isTrackingOnlyPortfolio } from '../utils/numbers'

// ── localStorage checklist — keyed by date ──────────────────────────────────
const todayKey = () => `tm_briefing_${new Date().toISOString().slice(0, 10)}`

const CHECKLIST_ITEMS = [
  { id: 'gaps',        label: 'Check Pre-Market Gaps',                  page: 'premarket',     icon: '📊' },
  { id: 'news',        label: 'Review overnight news on open positions', page: 'starfleet-intel', icon: '📰' },
  { id: 'flow',        label: 'Check Options Flow for unusual activity', page: 'options-flow',  icon: '🌊' },
  { id: 'congress',    label: 'Check Capitol Trades for Congress overlap', page: 'capitol-trades', icon: '🏛' },
  { id: 'warroom',     label: 'Review War Room debates',                page: 'war-room',       icon: '🔥' },
  { id: 'vix',         label: 'Check VIX — if >25 reduce size, if >30 no new entries', page: null, icon: '⚡' },
  { id: 'charts',      label: 'Mark key levels on charts',              href: 'http://localhost:5001/ib_multichart.html', icon: '📈' },
  { id: 'maxloss',     label: 'Set max loss for the day',               page: null,             icon: '🛡' },
]

function loadChecklist() {
  try {
    const raw = localStorage.getItem(todayKey())
    return raw ? JSON.parse(raw) : {}
  } catch { return {} }
}

function saveChecklist(state) {
  try { localStorage.setItem(todayKey(), JSON.stringify(state)) } catch {}
}

// ── helpers ──────────────────────────────────────────────────────────────────
function vixLabel(vix) {
  if (!vix) return { label: '—', color: '#64748b' }
  if (vix >= 30) return { label: 'HIGH — Circuit Breaker', color: '#ef4444' }
  if (vix >= 25) return { label: 'Elevated — Reduce Size', color: '#f59e0b' }
  if (vix >= 18) return { label: 'Normal', color: '#22c55e' }
  return { label: 'Low — Complacent?', color: '#3b82f6' }
}

function regimeBias(regime) {
  if (!regime) return { label: '—', color: '#64748b' }
  const r = regime.toUpperCase()
  if (r.includes('BULL') || r === 'MELT_UP') return { label: 'Bull', color: '#22c55e' }
  if (r.includes('BEAR') || r === 'CRASH_MODE') return { label: 'Bear', color: '#ef4444' }
  return { label: 'Neutral', color: '#94a3b8' }
}

function fmtUsd(v) {
  if (v == null) return '—'
  return `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function fmtPct(v) {
  if (v == null) return '—'
  return `${v >= 0 ? '+' : ''}${Number(v).toFixed(2)}%`
}

const PROVIDER_COLORS = {
  anthropic: '#22c55e', openai: '#22c55e', google: '#3b82f6',
  xai: '#ef4444', ollama: '#94a3b8', webull: '#fbbf24', crewai: '#f59e0b', matrix: '#00bcd4',
}
const PROVIDER_AVATARS = {
  anthropic: 'OA', openai: 'GP', google: 'GE', xai: 'GK',
  ollama: 'OL', webull: 'WB', crewai: '⭐', matrix: 'NE',
}

// ── Section wrapper ───────────────────────────────────────────────────────────
function Section({ title, children, badge }) {
  return (
    <div style={{
      background: '#0d1117', border: '1px solid #1e293b', borderRadius: 10,
      marginBottom: 16, overflow: 'hidden',
    }}>
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '10px 16px', borderBottom: '1px solid #1e293b',
        background: '#111827',
      }}>
        <span style={{ fontSize: 15, fontWeight: 700, color: '#e2e8f0', letterSpacing: 0.3 }}>{title}</span>
        {badge != null && (
          <span style={{
            fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 10,
            background: '#1e293b', color: '#64748b',
          }}>{badge}</span>
        )}
      </div>
      <div style={{ padding: '12px 16px' }}>{children}</div>
    </div>
  )
}

// ── 1. Market Regime ─────────────────────────────────────────────────────────
function MarketRegimeSection({ regime, dilithium }) {
  const vix = regime?.vix
  const { label: vixLbl, color: vixColor } = vixLabel(vix)
  const spy200 = regime?.spy_vs_200ma
  const bias = regimeBias(regime?.regime)
  const dilCost = dilithium?.today_cost ?? dilithium?.total ?? null

  const pills = [
    {
      label: 'VIX',
      value: vix != null ? vix.toFixed(1) : '—',
      sub: vixLbl,
      color: vixColor,
    },
    {
      label: 'SPY vs 200MA',
      value: spy200 != null ? `${spy200 >= 0 ? '+' : ''}${spy200.toFixed(2)}%` : '—',
      sub: spy200 != null ? (spy200 >= 0 ? 'Above 200MA' : 'Below 200MA') : '—',
      color: spy200 != null ? (spy200 >= 0 ? '#22c55e' : '#ef4444') : '#64748b',
    },
    {
      label: 'Market Bias',
      value: bias.label,
      sub: regime?.regime?.replace(/_/g, ' ') || '—',
      color: bias.color,
    },
    {
      label: 'Dilithium Cost',
      value: dilCost != null ? fmtUsd(dilCost) : '—',
      sub: 'AI spend today',
      color: '#94a3b8',
    },
  ]

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 10 }}>
      {pills.map(p => (
        <div key={p.label} style={{
          background: '#111827', border: '1px solid #1e293b', borderRadius: 8,
          padding: '10px 14px',
        }}>
          <div style={{ fontSize: 10, color: '#475569', fontWeight: 700, letterSpacing: 0.5, marginBottom: 4 }}>
            {p.label}
          </div>
          <div style={{ fontSize: 20, fontWeight: 800, color: p.color, fontFamily: 'JetBrains Mono, monospace' }}>
            {p.value}
          </div>
          <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>{p.sub}</div>
        </div>
      ))}
    </div>
  )
}

// ── 2. Pre-Market Checklist ───────────────────────────────────────────────────
function ChecklistSection({ onNavigate }) {
  const [checks, setChecks] = useState(loadChecklist)

  const toggle = (id) => {
    setChecks(prev => {
      const next = { ...prev, [id]: !prev[id] }
      saveChecklist(next)
      return next
    })
  }

  const reset = () => {
    saveChecklist({})
    setChecks({})
  }

  const done = Object.values(checks).filter(Boolean).length

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
        <span style={{ fontSize: 11, color: '#64748b' }}>
          {done}/{CHECKLIST_ITEMS.length} complete
        </span>
        <button
          onClick={reset}
          style={{
            fontSize: 10, padding: '3px 10px', borderRadius: 5, cursor: 'pointer',
            background: '#1e293b', border: '1px solid #334155', color: '#64748b',
            fontWeight: 600,
          }}
        >
          Reset Checklist
        </button>
      </div>

      {/* Progress bar */}
      <div style={{
        height: 4, background: '#1e293b', borderRadius: 2, marginBottom: 12, overflow: 'hidden',
      }}>
        <div style={{
          height: '100%', borderRadius: 2,
          width: `${(done / CHECKLIST_ITEMS.length) * 100}%`,
          background: done === CHECKLIST_ITEMS.length ? '#22c55e' : '#3b82f6',
          transition: 'width 0.3s ease',
        }} />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {CHECKLIST_ITEMS.map(item => {
          const checked = !!checks[item.id]
          return (
            <div
              key={item.id}
              style={{
                display: 'flex', alignItems: 'center', gap: 10,
                padding: '8px 12px', borderRadius: 7,
                background: checked ? '#052e16' : '#0f172a',
                border: `1px solid ${checked ? '#166534' : '#1e293b'}`,
                cursor: 'pointer',
                transition: 'all 0.15s',
              }}
              onClick={() => toggle(item.id)}
            >
              {/* Checkbox */}
              <div style={{
                width: 18, height: 18, borderRadius: 4, flexShrink: 0,
                border: `2px solid ${checked ? '#22c55e' : '#334155'}`,
                background: checked ? '#22c55e' : 'transparent',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                {checked && <span style={{ fontSize: 11, color: '#fff', lineHeight: 1 }}>✓</span>}
              </div>

              <span style={{ fontSize: 13 }}>{item.icon}</span>

              <span style={{
                fontSize: 13, color: checked ? '#4ade80' : '#94a3b8',
                textDecoration: checked ? 'line-through' : 'none',
                flex: 1,
              }}>
                {item.label}
              </span>

              {/* Link button */}
              {(item.page || item.href) && (
                <button
                  onClick={(e) => {
                    e.stopPropagation()
                    if (item.href) window.open(item.href, '_blank')
                    else onNavigate(item.page)
                  }}
                  style={{
                    fontSize: 10, padding: '2px 8px', borderRadius: 4, cursor: 'pointer',
                    background: '#1e293b', border: '1px solid #334155', color: '#94a3b8',
                    fontWeight: 600, flexShrink: 0,
                  }}
                >
                  Open →
                </button>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── 3. Crew Status ───────────────────────────────────────────────────────────
function CrewStatusSection({ leaderboard }) {
  if (!leaderboard?.length) {
    return <div style={{ color: '#475569', fontSize: 13 }}>Loading crew status…</div>
  }

  const getStarting = (p) =>
    p.player_id === 'super-agent' ? 25000
    : p.player_id === 'dayblade-0dte' ? 5000
    : p.player_id === 'steve-webull' ? 7049.68
    : 10000

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      {leaderboard.slice(0, 16).map((p) => {
        const starting = getStarting(p)
        const pnl = getDisplayCapital(p) - starting
        const isPos = pnl >= 0
        const color = PROVIDER_COLORS[p.provider] || '#94a3b8'
        const avatar = PROVIDER_AVATARS[p.provider] || '??'
        const hasPositions = (p.positions_count || 0) > 0

        return (
          <div key={p.player_id} style={{
            display: 'flex', alignItems: 'center', gap: 8,
            padding: '6px 10px', borderRadius: 6,
            background: '#111827', border: '1px solid #1e293b',
          }}>
            {/* Avatar */}
            <div style={{
              width: 26, height: 26, borderRadius: 5, flexShrink: 0,
              background: color, display: 'flex', alignItems: 'center',
              justifyContent: 'center', fontSize: 10, fontWeight: 800, color: '#fff',
            }}>
              {avatar}
            </div>

            {/* Name */}
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {getPortfolioDisplayName(p)}
              </div>
            </div>

            {/* Status badge */}
            <span style={{
              fontSize: 9, fontWeight: 700, padding: '2px 6px', borderRadius: 3,
              background: p.is_paused ? '#2d1b00' : isTrackingOnlyPortfolio(p) ? '#1e293b' : '#052e16',
              color: p.is_paused ? '#f59e0b' : '#e2e8f0',
              border: `1px solid ${p.is_paused ? '#92400e' : isTrackingOnlyPortfolio(p) ? '#475569' : '#166534'}`,
              flexShrink: 0,
            }}>
              {p.is_paused ? 'PAUSED' : isTrackingOnlyPortfolio(p) ? 'TRACKING ONLY' : 'ACTIVE'}
            </span>

            {/* Positions */}
            <span style={{ fontSize: 11, color: '#475569', width: 50, textAlign: 'right', flexShrink: 0 }}>
              {hasPositions ? `${p.positions_count || '?'} pos` : 'cash'}
            </span>

            {/* P&L */}
            <span style={{
              fontSize: 12, fontWeight: 700, fontFamily: 'JetBrains Mono, monospace',
              color: isPos ? '#22c55e' : '#ef4444',
              width: 80, textAlign: 'right', flexShrink: 0,
            }}>
              {isPos ? '+' : ''}{fmtUsd(pnl).replace('$', '')}
            </span>
          </div>
        )
      })}
    </div>
  )
}

// ── 4. Risk Alerts ───────────────────────────────────────────────────────────
function RiskAlertsSection({ alerts }) {
  if (!alerts) {
    return <div style={{ color: '#475569', fontSize: 13 }}>Scanning for alerts…</div>
  }
  if (alerts.length === 0) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '10px 14px', borderRadius: 7,
        background: '#052e16', border: '1px solid #166534', color: '#4ade80', fontSize: 13,
      }}>
        ✅ All clear — no risk alerts right now
      </div>
    )
  }

  const severityStyle = {
    critical: { bg: '#2d0a0a', border: '#7f1d1d', color: '#ef4444', icon: '🚨' },
    warning:  { bg: '#1c1407', border: '#92400e', color: '#f59e0b', icon: '⚠️' },
    info:     { bg: '#0f172a', border: '#1e293b', color: '#94a3b8', icon: 'ℹ️' },
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {alerts.map((a, i) => {
        const s = severityStyle[a.severity] || severityStyle.info
        return (
          <div key={i} style={{
            display: 'flex', gap: 10, padding: '8px 12px', borderRadius: 7,
            background: s.bg, border: `1px solid ${s.border}`,
          }}>
            <span style={{ fontSize: 14, flexShrink: 0 }}>{s.icon}</span>
            <span style={{ fontSize: 12, color: s.color, lineHeight: 1.5 }}>{a.message}</span>
          </div>
        )
      })}
    </div>
  )
}

// ── 5. Today's Calendar ───────────────────────────────────────────────────────
function CalendarSection({ earnings, econCalendar }) {
  const earningsList = Array.isArray(earnings) ? earnings : (earnings?.earnings || earnings?.warnings || [])
  const today = new Date().toISOString().slice(0, 10)
  const cutoff = new Date(Date.now() + 7 * 86400000).toISOString().slice(0, 10)

  const upcomingEarnings = earningsList.filter(e => {
    const d = e.earnings_date || e.date || ''
    return d >= today && d <= cutoff
  }).slice(0, 10)

  const econEvents = (econCalendar || []).filter(e => {
    const d = (e.date || '').slice(0, 10)
    return d >= today && d <= cutoff
  }).slice(0, 8)

  const isEmpty = upcomingEarnings.length === 0 && econEvents.length === 0

  if (isEmpty) {
    return (
      <div style={{ color: '#475569', fontSize: 13 }}>
        No earnings or major economic events in the next 7 days.
      </div>
    )
  }

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
      {/* Earnings */}
      <div>
        <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, letterSpacing: 0.5, marginBottom: 8 }}>
          EARNINGS (7 DAYS)
        </div>
        {upcomingEarnings.length === 0 ? (
          <div style={{ color: '#475569', fontSize: 12 }}>None in watchlist</div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {upcomingEarnings.map((e, i) => (
              <div key={i} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '5px 10px', borderRadius: 5,
                background: '#111827', border: '1px solid #1e293b',
              }}>
                <span style={{ fontSize: 13, fontWeight: 700, color: '#e2e8f0', fontFamily: 'monospace' }}>
                  {e.symbol || e.ticker}
                </span>
                <span style={{ fontSize: 11, color: '#f59e0b' }}>
                  {e.earnings_date || e.date}
                  {e.time && ` ${e.time}`}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Economic Events */}
      <div>
        <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, letterSpacing: 0.5, marginBottom: 8 }}>
          ECONOMIC EVENTS
        </div>
        {econEvents.length === 0 ? (
          <div style={{ color: '#475569', fontSize: 12 }}>No major events</div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {econEvents.map((e, i) => (
              <div key={i} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '5px 10px', borderRadius: 5,
                background: '#111827', border: '1px solid #1e293b',
              }}>
                <span style={{ fontSize: 12, color: '#e2e8f0', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {e.event || e.name || e.title}
                </span>
                <span style={{ fontSize: 10, color: '#64748b', marginLeft: 8, flexShrink: 0 }}>
                  {(e.date || '').slice(5)}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

// ── 6. Captain's Top 3 ───────────────────────────────────────────────────────
function CaptainTop3Section() {
  const [loading, setLoading] = useState(false)
  const [ideas, setIdeas] = useState(null)
  const [error, setError] = useState(null)

  const fetch3 = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.captainAsk(
        'Give me exactly 3 specific trade ideas for today based on current crew positions, ' +
        'recent signals, and market conditions. For each: stock ticker, direction (LONG/SHORT), ' +
        'key catalyst, and one-line thesis. Format as: 1. TICKER — direction — thesis. Be concise.'
      )
      setIdeas(res?.answer || null)
    } catch (e) {
      setError('Super Agent unavailable')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetch3()
  }, [fetch3])

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
        <span style={{ fontSize: 11, color: '#475569' }}>
          Powered by Super Agent (CrewAI collective intelligence)
        </span>
        <button
          onClick={fetch3}
          disabled={loading}
          style={{
            fontSize: 10, padding: '3px 10px', borderRadius: 5, cursor: loading ? 'default' : 'pointer',
            background: '#1e293b', border: '1px solid #334155', color: loading ? '#475569' : '#94a3b8',
            fontWeight: 600,
          }}
        >
          {loading ? 'Asking…' : '↻ Refresh'}
        </button>
      </div>

      {loading && !ideas && (
        <div style={{ color: '#475569', fontSize: 13, padding: '12px 0' }}>
          ⭐ Super Agent is analyzing crew data…
        </div>
      )}

      {error && (
        <div style={{ color: '#ef4444', fontSize: 13, padding: '8px 12px', background: '#2d0a0a', borderRadius: 6 }}>
          {error}
        </div>
      )}

      {ideas && (
        <div style={{
          background: '#0a1628', border: '1px solid #1e3a5f', borderRadius: 8,
          padding: '12px 16px',
        }}>
          <div style={{
            fontFamily: 'JetBrains Mono, monospace', fontSize: 13,
            color: '#e2e8f0', lineHeight: 1.8, whiteSpace: 'pre-wrap',
          }}>
            {ideas}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Main Export ───────────────────────────────────────────────────────────────
export default function DailyBriefing({ leaderboard, onNavigate }) {
  const [regime, setRegime] = useState(null)
  const [dilithium, setDilithium] = useState(null)
  const [alerts, setAlerts] = useState(null)
  const [earnings, setEarnings] = useState(null)
  const [econCalendar, setEconCalendar] = useState(null)

  const load = useCallback(async () => {
    const [r, d, a, e, ec] = await Promise.allSettled([
      api.getRegime(),
      api.getDilithium(),
      api.getDailyBriefingAlerts(),
      api.getEarnings(),
      api.getEconomicCalendar(),
    ])
    if (r.status === 'fulfilled') setRegime(r.value)
    if (d.status === 'fulfilled') setDilithium(d.value)
    if (a.status === 'fulfilled') setAlerts(a.value?.alerts || [])
    if (e.status === 'fulfilled') setEarnings(e.value)
    if (ec.status === 'fulfilled') {
      const raw = ec.value
      setEconCalendar(Array.isArray(raw) ? raw : (raw?.events || raw?.calendar || []))
    }
  }, [])

  useEffect(() => { load() }, [load])

  const today = new Date().toLocaleDateString('en-US', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
  })

  return (
    <div style={{ maxWidth: 900, margin: '0 auto' }}>
      {/* Header */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
        marginBottom: 20,
      }}>
        <div>
          <div style={{ fontSize: 11, color: '#475569', marginBottom: 2, letterSpacing: 0.5 }}>
            {today}
          </div>
          <p style={{ margin: 0, fontSize: 13, color: '#64748b', lineHeight: 1.6 }}>
            Your morning trading checklist. Run through each section before placing a trade.
          </p>
        </div>
        <button
          onClick={load}
          style={{
            fontSize: 11, padding: '5px 14px', borderRadius: 6, cursor: 'pointer',
            background: '#1e293b', border: '1px solid #334155', color: '#64748b',
            fontWeight: 600, flexShrink: 0,
          }}
        >
          ↻ Refresh All
        </button>
      </div>

      <Section title="🌅 Market Regime">
        <MarketRegimeSection regime={regime} dilithium={dilithium} />
      </Section>

      <Section title="📋 Pre-Market Checklist" badge={`${Object.values(loadChecklist()).filter(Boolean).length}/${CHECKLIST_ITEMS.length}`}>
        <ChecklistSection onNavigate={onNavigate} />
      </Section>

      <Section title="🤖 Crew Status" badge={leaderboard?.filter(p => !p.is_paused).length + ' active'}>
        <CrewStatusSection leaderboard={leaderboard} />
      </Section>

      <Section
        title="⚠️ Risk Alerts"
        badge={alerts?.filter(a => a.severity === 'critical').length > 0
          ? `${alerts.filter(a => a.severity === 'critical').length} critical`
          : alerts?.length > 0 ? `${alerts.length} warnings` : 'clear'}
      >
        <RiskAlertsSection alerts={alerts} />
      </Section>

      <Section title="📅 Today's Calendar">
        <CalendarSection earnings={earnings} econCalendar={econCalendar} />
      </Section>

      <Section title="🎯 Captain's Top 3 — Trade Ideas">
        <CaptainTop3Section />
      </Section>
    </div>
  )
}
