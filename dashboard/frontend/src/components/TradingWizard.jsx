import React, { useState, useEffect, useCallback, useRef } from 'react'
import { api } from '../api/client'
import { getDisplayCapital, getPortfolioDisplayName, isTrackingOnlyPortfolio } from '../utils/numbers'

// ── LocalStorage helpers ─────────────────────────────────────────────────────
const todayStr = () => new Date().toISOString().slice(0, 10)
const LS = {
  get: (k, def = null) => { try { const v = localStorage.getItem(k); return v !== null ? JSON.parse(v) : def } catch { return def } },
  set: (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)) } catch {} },
}

const WIZARD_AUTO_KEY   = 'tm_wizard_auto'        // 'auto' | 'manual' | 'disabled'
const WIZARD_DONE_KEY   = () => `tm_wizard_done_${todayStr()}`
const WIZARD_SKIP_KEY   = () => `tm_wizard_skip_${todayStr()}`
const WIZARD_LIMITS_KEY = 'tm_wizard_limits'
const SESSION_KEY       = () => `tm_session_${todayStr()}`

// ── Market hours check (8:30–9:30 AM ET) ─────────────────────────────────────
function isPreMarketWindow() {
  const now = new Date()
  const day = now.getDay()
  if (day === 0 || day === 6) return false               // weekend
  // Convert to ET (UTC-5 or UTC-4 daylight)
  const etOffset = isDST(now) ? -4 : -5
  const etHour = now.getUTCHours() + etOffset
  const etMin  = now.getUTCMinutes()
  const etMins = etHour * 60 + etMin
  return etMins >= 510 && etMins < 570                   // 8:30–9:30 ET
}
// Market open window: 9:30–9:35 AM ET (5-min trigger window)
export function isMarketOpenWindow() {
  const now = new Date()
  const day = now.getDay()
  if (day === 0 || day === 6) return false
  const etOffset = isDST(now) ? -4 : -5
  const etHour = now.getUTCHours() + etOffset
  const etMin  = now.getUTCMinutes()
  const etMins = etHour * 60 + etMin
  return etMins >= 570 && etMins < 575                   // 9:30–9:35 ET
}
function isDST(d) {
  const jan = new Date(d.getFullYear(), 0, 1).getTimezoneOffset()
  const jul = new Date(d.getFullYear(), 6, 1).getTimezoneOffset()
  return d.getTimezoneOffset() < Math.max(jan, jul)
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function vixColor(v) {
  if (!v) return '#64748b'
  if (v >= 30) return '#ef4444'
  if (v >= 25) return '#f59e0b'
  if (v >= 18) return '#22c55e'
  return '#3b82f6'
}
function vixLabel(v) {
  if (!v) return '—'
  if (v >= 30) return 'HIGH — No new entries'
  if (v >= 25) return 'Elevated — Reduce size'
  if (v >= 18) return 'Normal'
  return 'Low (complacent?)'
}
function regimeBias(r) {
  if (!r) return { label: '—', color: '#64748b' }
  const s = r.toUpperCase()
  if (s.includes('BULL') || s === 'MELT_UP') return { label: 'Bullish', color: '#22c55e' }
  if (s.includes('BEAR') || s === 'CRASH_MODE') return { label: 'Bearish', color: '#ef4444' }
  return { label: 'Choppy / Neutral', color: '#94a3b8' }
}
const PROVIDER_COLORS = { anthropic: '#22c55e', openai:'#22c55e', google:'#3b82f6', xai:'#ef4444', ollama:'#94a3b8', webull:'#fbbf24', crewai:'#f59e0b', matrix:'#00bcd4' }
const PROVIDER_AVATARS = { anthropic: 'OA', openai:'GP', google:'GE', xai:'GK', ollama:'OL', webull:'WB', crewai:'⭐', matrix:'NE' }

// ── Step wrapper ──────────────────────────────────────────────────────────────
function StepCard({ icon, title, subtitle, children, loading }) {
  return (
    <div style={{ flex: 1, overflowY: 'auto' }}>
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 28, marginBottom: 6 }}>{icon}</div>
        <div style={{ fontSize: 20, fontWeight: 800, color: '#e2e8f0' }}>{title}</div>
        {subtitle && <div style={{ fontSize: 13, color: '#64748b', marginTop: 3 }}>{subtitle}</div>}
      </div>
      {loading
        ? <div style={{ color: '#475569', fontSize: 13, padding: '20px 0' }}>Loading…</div>
        : children}
    </div>
  )
}

// ── Step 1: Market Regime ─────────────────────────────────────────────────────
function Step1({ data }) {
  const r = data?.regime
  const bias = regimeBias(r?.regime)
  const pills = [
    { label: 'VIX', value: r?.vix?.toFixed(1) || '—', color: vixColor(r?.vix), sub: vixLabel(r?.vix) },
    { label: 'SPY vs 200MA', value: r?.spy_vs_200ma != null ? `${r.spy_vs_200ma >= 0 ? '+' : ''}${r.spy_vs_200ma.toFixed(2)}%` : '—', color: r?.spy_vs_200ma != null ? (r.spy_vs_200ma >= 0 ? '#22c55e' : '#ef4444') : '#64748b', sub: r?.spy_vs_200ma != null ? (r.spy_vs_200ma >= 0 ? 'Above 200MA' : 'Below 200MA') : '—' },
    { label: 'Bias', value: bias.label, color: bias.color, sub: r?.regime?.replace(/_/g, ' ') || '—' },
  ]
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', gap: 10 }}>
        {pills.map(p => (
          <div key={p.label} style={{
            flex: 1, background: '#111827', border: '1px solid #1e293b', borderRadius: 8,
            padding: '10px 14px',
          }}>
            <div style={{ fontSize: 9, color: '#475569', fontWeight: 700, letterSpacing: 0.5 }}>{p.label}</div>
            <div style={{ fontSize: 22, fontWeight: 800, color: p.color, fontFamily: 'monospace', margin: '4px 0' }}>{p.value}</div>
            <div style={{ fontSize: 11, color: '#64748b' }}>{p.sub}</div>
          </div>
        ))}
      </div>
      {r?.description && (
        <div style={{
          padding: '10px 14px', background: '#0f172a', borderRadius: 8,
          border: '1px solid #1e293b', fontSize: 13, color: '#94a3b8', lineHeight: 1.6,
        }}>
          {r.description}
        </div>
      )}
      {data?.earnings?.length > 0 && (
        <div>
          <div style={{ fontSize: 10, color: '#64748b', fontWeight: 700, letterSpacing: 0.5, marginBottom: 6 }}>EARNINGS THIS WEEK</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {data.earnings.slice(0, 6).map((e, i) => (
              <span key={i} style={{
                padding: '3px 10px', borderRadius: 4, fontSize: 12, fontWeight: 700,
                background: '#1c1407', border: '1px solid #92400e', color: '#f59e0b',
                fontFamily: 'monospace',
              }}>
                {e.symbol || e.ticker} {e.earnings_date ? `(${e.earnings_date.slice(5)})` : ''}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Step 2: Crew Status ───────────────────────────────────────────────────────
function Step2({ leaderboard }) {
  const getStarting = (p) => p.player_id === 'super-agent' ? 25000 : p.player_id === 'dayblade-0dte' ? 5000 : p.player_id === 'steve-webull' ? 7049.68 : 10000
  const active = (leaderboard || []).filter(p => !p.is_paused)
  const paused = (leaderboard || []).filter(p => p.is_paused)
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', gap: 12, marginBottom: 4 }}>
        <div style={{ padding: '8px 16px', borderRadius: 8, background: '#052e16', border: '1px solid #166534', color: '#4ade80', fontSize: 13, fontWeight: 700 }}>
          ✅ {active.length} Active
        </div>
        {paused.length > 0 && (
          <div style={{ padding: '8px 16px', borderRadius: 8, background: '#1c1407', border: '1px solid #92400e', color: '#f59e0b', fontSize: 13, fontWeight: 700 }}>
            ⏸ {paused.length} Paused
          </div>
        )}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4, maxHeight: 280, overflowY: 'auto' }}>
        {(leaderboard || []).slice(0, 14).map(p => {
          const starting = getStarting(p)
          const pnl = getDisplayCapital(p) - starting
          const color = PROVIDER_COLORS[p.provider] || '#94a3b8'
          return (
            <div key={p.player_id} style={{
              display: 'flex', alignItems: 'center', gap: 8,
              padding: '5px 10px', borderRadius: 6,
              background: p.is_paused ? '#1c1407' : '#111827',
              border: `1px solid ${p.is_paused ? '#92400e' : '#1e293b'}`,
            }}>
              <div style={{
                width: 22, height: 22, borderRadius: 4, background: color,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 9, fontWeight: 800, color: '#fff', flexShrink: 0,
              }}>
                {PROVIDER_AVATARS[p.provider] || '??'}
              </div>
              <span style={{ fontSize: 12, color, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{getPortfolioDisplayName(p)}</span>
              <span style={{ fontSize: 9, color: p.is_paused ? '#f59e0b' : '#e2e8f0', fontWeight: 700 }}>
                {p.is_paused ? 'PAUSED' : isTrackingOnlyPortfolio(p) ? 'TRACKING ONLY' : 'ACTIVE'}
              </span>
              {(p.positions_count || 0) > 0 && (
                <span style={{ fontSize: 10, color: '#64748b', width: 40, textAlign: 'right' }}>
                  {p.positions_count}pos
                </span>
              )}
              <span style={{
                fontSize: 11, fontFamily: 'monospace', fontWeight: 700,
                color: pnl >= 0 ? '#22c55e' : '#ef4444',
                width: 72, textAlign: 'right',
              }}>
                {pnl >= 0 ? '+' : ''}${Math.abs(pnl).toFixed(0)}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Step 3: Risk Alerts ───────────────────────────────────────────────────────
function Step3({ alerts }) {
  if (!alerts) return <div style={{ color: '#475569', fontSize: 13 }}>Scanning…</div>
  if (alerts.length === 0) return (
    <div style={{
      padding: '20px', borderRadius: 10, background: '#052e16', border: '1px solid #166534',
      color: '#4ade80', fontSize: 16, fontWeight: 700, textAlign: 'center',
    }}>
      ✅ All Clear<br />
      <span style={{ fontSize: 13, fontWeight: 400, color: '#86efac' }}>No risk alerts detected.</span>
    </div>
  )
  const sev = { critical: { bg: '#2d0a0a', border: '#7f1d1d', color: '#ef4444', icon: '🚨' }, warning: { bg: '#1c1407', border: '#92400e', color: '#f59e0b', icon: '⚠️' }, info: { bg: '#0f172a', border: '#1e293b', color: '#94a3b8', icon: 'ℹ️' } }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {alerts.map((a, i) => {
        const s = sev[a.severity] || sev.info
        return (
          <div key={i} style={{ display: 'flex', gap: 10, padding: '8px 12px', borderRadius: 7, background: s.bg, border: `1px solid ${s.border}` }}>
            <span style={{ fontSize: 14, flexShrink: 0 }}>{s.icon}</span>
            <span style={{ fontSize: 12, color: s.color, lineHeight: 1.5 }}>{a.message}</span>
          </div>
        )
      })}
    </div>
  )
}

// ── Step 4: Pre-Market Gaps ───────────────────────────────────────────────────
function Step4({ gaps }) {
  const items = (Array.isArray(gaps) ? gaps : gaps?.gaps || []).slice(0, 5)
  if (!items.length) return <div style={{ color: '#475569', fontSize: 13 }}>No significant pre-market gaps found.</div>
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {items.map((g, i) => {
        const pct = g.gap_pct ?? g.change_pct ?? 0
        const isPos = pct >= 0
        return (
          <div key={i} style={{
            display: 'flex', alignItems: 'center', gap: 12,
            padding: '10px 14px', borderRadius: 8,
            background: isPos ? '#052e16' : '#2d0a0a',
            border: `1px solid ${isPos ? '#166534' : '#7f1d1d'}`,
          }}>
            <span style={{ fontSize: 14, fontWeight: 800, fontFamily: 'monospace', color: '#e2e8f0', width: 60 }}>
              {g.symbol || g.ticker}
            </span>
            <span style={{ fontSize: 20, fontWeight: 800, color: isPos ? '#22c55e' : '#ef4444', fontFamily: 'monospace', width: 80 }}>
              {isPos ? '+' : ''}{pct.toFixed(2)}%
            </span>
            <span style={{ fontSize: 12, fontWeight: 700, color: isPos ? '#22c55e' : '#ef4444' }}>
              {isPos ? '▲ GAP UP' : '▼ GAP DOWN'}
            </span>
            {g.volume_ratio != null && (
              <span style={{ fontSize: 11, color: '#64748b', marginLeft: 'auto' }}>
                {g.volume_ratio?.toFixed(1)}x vol
              </span>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ── Step 5: Captain's Orders ──────────────────────────────────────────────────
function Step5() {
  const [loading, setLoading] = useState(true)
  const [answer, setAnswer] = useState(null)
  const [error, setError] = useState(null)
  const fetched = useRef(false)

  useEffect(() => {
    if (fetched.current) return
    fetched.current = true
    api.captainAsk(
      'Give me exactly 3 specific trade ideas for today based on current crew positions, recent signals, and market conditions. ' +
      'For each: stock ticker, direction (LONG/SHORT/WAIT), key catalyst, one-line thesis. ' +
      'Format: 1. TICKER — DIRECTION — thesis. Keep each under 2 sentences.'
    )
      .then(r => setAnswer(r?.answer || 'No response'))
      .catch(() => setError('Super Agent unavailable — check API key'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div style={{ color: '#475569', fontSize: 13, padding: '20px 0' }}>⭐ Super Agent is analyzing crew data…</div>
  if (error) return <div style={{ color: '#ef4444', fontSize: 13, padding: '10px 14px', background: '#2d0a0a', borderRadius: 7 }}>{error}</div>
  return (
    <div style={{ background: '#0a1628', border: '1px solid #1e3a5f', borderRadius: 10, padding: '14px 18px' }}>
      <div style={{ fontSize: 11, color: '#3b82f6', fontWeight: 700, letterSpacing: 0.5, marginBottom: 10 }}>
        ⭐ SUPER AGENT — COLLECTIVE INTELLIGENCE
      </div>
      <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 13, color: '#e2e8f0', lineHeight: 1.9, whiteSpace: 'pre-wrap' }}>
        {answer}
      </div>
    </div>
  )
}

// ── Step 6: Set Limits ────────────────────────────────────────────────────────
function Step6({ limits, setLimits }) {
  const focuses = ['Day Trade', 'Swing', 'Options', 'Watch Only']
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      <div>
        <label style={{ fontSize: 11, color: '#64748b', fontWeight: 700, display: 'block', marginBottom: 6 }}>MAX LOSS TODAY ($)</label>
        <input
          type="number"
          value={limits.maxLoss || ''}
          onChange={e => setLimits(prev => ({ ...prev, maxLoss: e.target.value }))}
          placeholder="e.g. 200"
          style={{
            background: '#111827', border: '1px solid #334155', borderRadius: 7,
            color: '#ef4444', fontSize: 18, fontWeight: 700, fontFamily: 'monospace',
            padding: '10px 14px', width: '100%', outline: 'none', boxSizing: 'border-box',
          }}
        />
      </div>
      <div>
        <label style={{ fontSize: 11, color: '#64748b', fontWeight: 700, display: 'block', marginBottom: 6 }}>MAX TRADES TODAY</label>
        <input
          type="number"
          value={limits.maxTrades || ''}
          onChange={e => setLimits(prev => ({ ...prev, maxTrades: e.target.value }))}
          placeholder="e.g. 3"
          style={{
            background: '#111827', border: '1px solid #334155', borderRadius: 7,
            color: '#e2e8f0', fontSize: 18, fontWeight: 700, fontFamily: 'monospace',
            padding: '10px 14px', width: '100%', outline: 'none', boxSizing: 'border-box',
          }}
        />
      </div>
      <div>
        <label style={{ fontSize: 11, color: '#64748b', fontWeight: 700, display: 'block', marginBottom: 8 }}>TODAY'S FOCUS</label>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
          {focuses.map(f => {
            const sel = limits.focus === f
            return (
              <button
                key={f}
                onClick={() => setLimits(prev => ({ ...prev, focus: f }))}
                style={{
                  padding: '8px 16px', borderRadius: 7, fontSize: 13, fontWeight: 700,
                  cursor: 'pointer', border: `2px solid ${sel ? '#3b82f6' : '#1e293b'}`,
                  background: sel ? '#1e3a5f' : '#111827',
                  color: sel ? '#60a5fa' : '#64748b',
                  transition: 'all 0.15s',
                }}
              >
                {f}
              </button>
            )
          })}
        </div>
      </div>
    </div>
  )
}

// ── Main Wizard ───────────────────────────────────────────────────────────────
const STEPS = [
  { label: 'Market Regime', icon: '🌅' },
  { label: 'Crew Status',   icon: '🤖' },
  { label: 'Risk Alerts',   icon: '⚠️' },
  { label: 'Pre-Market',    icon: '📊' },
  { label: "Captain's Orders", icon: '🎯' },
  { label: 'Set Limits',    icon: '🛡️' },
]

export default function TradingWizard({ leaderboard, open, onClose, onSessionStart }) {
  const [step, setStep] = useState(0)
  const [data, setData] = useState({})
  const [loading, setLoading] = useState(true)
  const [limits, setLimits] = useState(() => LS.get(WIZARD_LIMITS_KEY, { maxLoss: '', maxTrades: '', focus: 'Swing' }))

  // Load all data on mount
  useEffect(() => {
    if (!open) return
    setLoading(true)
    Promise.allSettled([
      api.getRegime(),
      api.getDailyBriefingAlerts(),
      api.getPremarketGaps(),
      api.getEarnings(),
    ]).then(([r, a, g, e]) => {
      setData({
        regime: r.status === 'fulfilled' ? r.value : null,
        alerts: a.status === 'fulfilled' ? (a.value?.alerts || []) : [],
        gaps: g.status === 'fulfilled' ? g.value : null,
        earnings: e.status === 'fulfilled' ? (Array.isArray(e.value) ? e.value : (e.value?.earnings || e.value?.warnings || [])) : [],
      })
      setLoading(false)
    })
  }, [open])

  const next = () => {
    if (step < STEPS.length - 1) setStep(s => s + 1)
  }

  const finish = () => {
    const session = {
      maxLoss: limits.maxLoss,
      maxTrades: limits.maxTrades,
      focus: limits.focus || 'Swing',
      startTime: new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
    }
    LS.set(WIZARD_LIMITS_KEY, limits)
    LS.set(WIZARD_DONE_KEY(), true)
    LS.set(SESSION_KEY(), session)
    onSessionStart(session)
    onClose()
  }

  const skipToday = () => {
    LS.set(WIZARD_SKIP_KEY(), true)
    onClose()
  }

  if (!open) return null

  const pct = ((step + 1) / STEPS.length) * 100

  const btns = {
    0: 'Looks good, continue →',
    1: 'Got it →',
    2: 'Understood →',
    3: 'Reviewed →',
    4: 'Orders received →',
    5: null, // finish button
  }

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 9999,
      background: 'rgba(0,0,0,0.75)', backdropFilter: 'blur(4px)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      padding: 20,
    }}>
      <div style={{
        background: '#0d1117', border: '1px solid #1e293b', borderRadius: 14,
        width: '100%', maxWidth: 560, maxHeight: '90vh',
        display: 'flex', flexDirection: 'column',
        boxShadow: '0 25px 60px rgba(0,0,0,0.6)',
      }}>
        {/* Header */}
        <div style={{ padding: '16px 20px 0', flexShrink: 0 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <div style={{ fontSize: 11, color: '#475569', fontWeight: 700, letterSpacing: 0.5 }}>
              🧭 TRADING DAY WIZARD — STEP {step + 1} OF {STEPS.length}
            </div>
            <button
              onClick={skipToday}
              style={{
                fontSize: 11, color: '#475569', background: 'none', border: 'none',
                cursor: 'pointer', padding: '2px 8px',
              }}
            >
              Skip for today ✕
            </button>
          </div>

          {/* Progress bar */}
          <div style={{ height: 4, background: '#1e293b', borderRadius: 2, marginBottom: 6, overflow: 'hidden' }}>
            <div style={{
              height: '100%', borderRadius: 2, background: '#3b82f6',
              width: `${pct}%`, transition: 'width 0.3s ease',
            }} />
          </div>

          {/* Step pills */}
          <div style={{ display: 'flex', gap: 4, paddingBottom: 14, borderBottom: '1px solid #1e293b', flexWrap: 'wrap' }}>
            {STEPS.map((s, i) => (
              <div key={i} style={{
                fontSize: 10, padding: '2px 7px', borderRadius: 4, fontWeight: 600,
                background: i === step ? '#1e3a5f' : i < step ? '#052e16' : '#111827',
                color: i === step ? '#60a5fa' : i < step ? '#4ade80' : '#475569',
                cursor: i < step ? 'pointer' : 'default',
              }}
                onClick={() => i < step && setStep(i)}
              >
                {s.icon} {s.label}
              </div>
            ))}
          </div>
        </div>

        {/* Step content */}
        <div style={{ padding: '20px 20px 0', flex: 1, overflowY: 'auto', minHeight: 0 }}>
          {step === 0 && (
            <StepCard icon="🌅" title="Market Regime Check" subtitle="Understand today's macro conditions before you trade." loading={loading}>
              <Step1 data={data} />
            </StepCard>
          )}
          {step === 1 && (
            <StepCard icon="🤖" title="Your Crew Status" subtitle="Which AI models are active, what they hold, and who's in drawdown." loading={loading}>
              <Step2 leaderboard={leaderboard} />
            </StepCard>
          )}
          {step === 2 && (
            <StepCard icon="⚠️" title="Risk Alerts" subtitle="Circuit breakers, correlated positions, expiring options." loading={loading}>
              <Step3 alerts={data.alerts} />
            </StepCard>
          )}
          {step === 3 && (
            <StepCard icon="📊" title="Pre-Market Gaps" subtitle="Top 5 stocks moving significantly before the open." loading={loading}>
              <Step4 gaps={data.gaps} />
            </StepCard>
          )}
          {step === 4 && (
            <StepCard icon="🎯" title="Captain's Orders" subtitle="Super Agent's top 3 trade ideas based on crew intelligence.">
              <Step5 />
            </StepCard>
          )}
          {step === 5 && (
            <StepCard icon="🛡️" title="Set Your Limits" subtitle="Define your rules before placing a single trade.">
              <Step6 limits={limits} setLimits={setLimits} />
            </StepCard>
          )}
        </div>

        {/* Footer */}
        <div style={{
          padding: '16px 20px', borderTop: '1px solid #1e293b', flexShrink: 0,
          display: 'flex', justifyContent: 'flex-end', gap: 10, alignItems: 'center',
        }}>
          {step > 0 && (
            <button
              onClick={() => setStep(s => s - 1)}
              style={{
                padding: '10px 18px', borderRadius: 8, fontSize: 13, fontWeight: 600,
                background: '#111827', border: '1px solid #1e293b', color: '#64748b',
                cursor: 'pointer',
              }}
            >
              ← Back
            </button>
          )}
          {step < STEPS.length - 1 ? (
            <button
              onClick={next}
              style={{
                padding: '10px 22px', borderRadius: 8, fontSize: 13, fontWeight: 700,
                background: '#2563eb', border: 'none', color: '#fff', cursor: 'pointer',
              }}
            >
              {btns[step]}
            </button>
          ) : (
            <button
              onClick={finish}
              style={{
                padding: '10px 22px', borderRadius: 8, fontSize: 13, fontWeight: 700,
                background: '#16a34a', border: 'none', color: '#fff', cursor: 'pointer',
              }}
            >
              ✅ Begin trading session
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Session Banner ────────────────────────────────────────────────────────────
export function SessionBanner({ session, onReopen }) {
  if (!session) return null
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 12,
      padding: '4px 14px', background: '#052e16', borderBottom: '1px solid #166534',
      fontSize: 12, color: '#86efac', flexShrink: 0,
    }}>
      <span>🛡️ Max loss: {session.maxLoss ? `$${Number(session.maxLoss).toLocaleString()}` : 'none'}</span>
      <span style={{ color: '#166534' }}>·</span>
      <span>🎯 Focus: {session.focus || 'Swing'}</span>
      {session.maxTrades && <>
        <span style={{ color: '#166534' }}>·</span>
        <span>🔄 Max trades: {session.maxTrades}</span>
      </>}
      <span style={{ color: '#166534' }}>·</span>
      <span>Session started {session.startTime}</span>
      <button
        onClick={onReopen}
        style={{
          marginLeft: 'auto', fontSize: 10, padding: '2px 8px', borderRadius: 4,
          background: '#166534', border: 'none', color: '#86efac', cursor: 'pointer',
          fontWeight: 700,
        }}
      >
        🧭 Wizard
      </button>
    </div>
  )
}

// ── Nav Button ────────────────────────────────────────────────────────────────
export function WizardNavButton({ onClick }) {
  return (
    <button
      onClick={onClick}
      title="Trading Day Wizard"
      style={{
        display: 'flex', alignItems: 'center', gap: 5,
        padding: '4px 10px', borderRadius: 6, fontSize: 12, fontWeight: 700,
        background: '#1e3a5f', border: '1px solid #2563eb', color: '#60a5fa',
        cursor: 'pointer', letterSpacing: 0.2, flexShrink: 0,
      }}
    >
      🧭
    </button>
  )
}

// ── Wizard settings gear ──────────────────────────────────────────────────────
export { LS, WIZARD_AUTO_KEY, WIZARD_DONE_KEY, WIZARD_SKIP_KEY, SESSION_KEY, isPreMarketWindow }
