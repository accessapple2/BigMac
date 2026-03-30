import React, { useState, useMemo, useCallback, useEffect } from 'react'
import { usePolling } from './hooks/usePolling'
import { api } from './api/client'
import Leaderboard from './components/arena/Leaderboard'
import PlayerDetail from './components/arena/PlayerDetail'
import ComparisonChart from './components/arena/ComparisonChart'
import RecentTrades from './components/RecentTrades'
import RecentSignals from './components/RecentSignals'
import AIChatFeed from './components/chat/AIChatFeed'
import NewsFeed from './components/news/NewsFeed'
import MarketTicker from './components/MarketTicker'
import WebullPortfolio from './components/WebullPortfolio'
import Fundamentals from './components/Fundamentals'
import EconomicCalendar from './components/EconomicCalendar'
import ModelControl from './components/ModelControl'
import BacktestLab from './components/BacktestLab'
import CostDashboard from './components/CostDashboard'
import ChartAnalyzer from './components/ChartAnalyzer'
import PremarketGaps from './components/PremarketGaps'
import StockScreener from './components/StockScreener'
import SqueezeScanner from './components/SqueezeScanner'
import InsiderTracker from './components/InsiderTracker'
import StrategyLab from './components/StrategyLab'
import UOAPanel from './components/UOAPanel'
import GEXChart from './components/GEXChart'
import SMADashboard from './components/SMADashboard'
import ImpulseAlerts from './components/ImpulseAlerts'
import ImbalanceZones from './components/ImbalanceZones'
import ThetaOpportunities from './components/ThetaOpportunities'
import MorningGaps from './components/MorningGaps'
import ReadyRoom from './components/ReadyRoom'
import SectorHeatmap from './components/SectorHeatmap'
import MarketMovers from './components/MarketMovers'
import WinnersLosers from './components/WinnersLosers'
import HoldingsTopWL from './components/HoldingsTopWL'
import BubblePnL from './components/BubblePnL'
import TradeNotifications from './components/TradeNotifications'
import LiveAlerts from './components/LiveAlerts'
import CTOAdvisory from './components/CTOAdvisory'
import OfficerConsensus from './components/OfficerConsensus'
import DailyBriefing from './components/DailyBriefing'
import AndersonDecisionSummary from './components/AndersonDecisionSummary'
import TradingWizard, { SessionBanner, WizardNavButton, LS, WIZARD_AUTO_KEY, WIZARD_DONE_KEY, WIZARD_SKIP_KEY, SESSION_KEY, isPreMarketWindow, isMarketOpenWindow } from './components/TradingWizard'
import { AutoRefreshToggle, LastUpdated } from './components/AutoRefreshToggle'
import { formatMoney, formatPercent, getDisplayCapital, getPortfolioDisplayName, safeNumber } from './utils/numbers'

// ── Intelligence page components ────────────────────────────────────────────

function NavigatorIntel() {
  const { data: conv, loading: lc } = usePolling(api.getNavigatorConvergence, 60000)
  const { data: univ, loading: lu } = usePolling(api.getNavigatorUniverse, 60000)
  const signals = conv?.signals || []
  const universe = univ?.universe || []
  return (
    <div className="arena-layout">
      <div className="card">
        <div className="card-header">
          <h2>Navigator — Convergence Signals</h2>
          <span className="card-badge">{signals.length} signals</span>
        </div>
        {lc ? <div className="loading">Loading…</div> : signals.length === 0 ? (
          <div className="empty-state">No convergence signals right now.</div>
        ) : (
          <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: '#64748b', textAlign: 'left' }}>
                {['Symbol', 'Direction', 'Strength', 'Strategies'].map(h => (
                  <th key={h} style={{ padding: '6px 10px', borderBottom: '1px solid #1e293b' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {signals.map((s, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #0f172a' }}>
                  <td style={{ padding: '5px 10px', fontWeight: 700, color: '#e2e8f0' }}>{s.symbol}</td>
                  <td style={{ padding: '5px 10px', color: s.direction === 'LONG' ? '#22c55e' : '#ef4444', fontWeight: 700 }}>{s.direction}</td>
                  <td style={{ padding: '5px 10px', color: '#94a3b8', fontFamily: 'monospace' }}>{(s.strength || 0).toFixed(2)}</td>
                  <td style={{ padding: '5px 10px', color: '#64748b', fontSize: 11 }}>{(s.strategies || []).join(', ')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
      {universe.length > 0 && (
        <div className="card">
          <div className="card-header">
            <h2>Navigator Universe</h2>
            <span className="card-badge">{universe.length} symbols</span>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: '12px 16px' }}>
            {universe.map(sym => (
              <span key={sym} style={{
                padding: '3px 10px', borderRadius: 4, fontSize: 11, fontWeight: 700,
                background: '#1e293b', color: '#94a3b8', fontFamily: 'monospace',
              }}>{sym}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function AlertHistory() {
  const { data, loading } = usePolling(api.getRealtimeAlertsHistory, 30000)
  const alerts = data || []
  return (
    <div className="arena-layout">
      <div className="card">
        <div className="card-header">
          <h2>Alert History</h2>
          <span className="card-badge">{alerts.length} alerts</span>
        </div>
        {loading ? <div className="loading">Loading…</div> : alerts.length === 0 ? (
          <div className="empty-state">No recent alerts.</div>
        ) : (
          <div style={{ maxHeight: 600, overflowY: 'auto' }}>
            {alerts.map((a, i) => (
              <div key={i} style={{
                padding: '8px 16px', borderBottom: '1px solid #0f172a',
                display: 'flex', alignItems: 'flex-start', gap: 10,
              }}>
                <span style={{ fontSize: 11, color: '#475569', whiteSpace: 'nowrap', marginTop: 2 }}>
                  {a.created_at ? new Date(a.created_at).toLocaleTimeString() : '—'}
                </span>
                <span style={{
                  fontSize: 10, fontWeight: 700, padding: '2px 7px', borderRadius: 3,
                  background: a.alert_type === 'BUY' ? '#052e16' : a.alert_type === 'SELL' ? '#2d0a0a' : '#1e293b',
                  color: a.alert_type === 'BUY' ? '#22c55e' : a.alert_type === 'SELL' ? '#ef4444' : '#94a3b8',
                  whiteSpace: 'nowrap',
                }}>{a.alert_type || 'INFO'}</span>
                <span style={{ fontSize: 12, color: '#e2e8f0' }}>{a.message || JSON.stringify(a)}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function FearGreedPage() {
  const { data, loading } = usePolling(api.getFearGreed, 300000)
  if (loading) return <div className="loading">Loading…</div>
  const fg = data || {}
  const value = fg.score ?? fg.value ?? null
  const label = fg.label || (value == null ? '—' : value < 25 ? 'Extreme Fear' : value < 45 ? 'Fear' : value < 55 ? 'Neutral' : value < 75 ? 'Greed' : 'Extreme Greed')
  const color = value == null ? '#64748b' : value < 25 ? '#ef4444' : value < 45 ? '#f97316' : value < 55 ? '#eab308' : value < 75 ? '#22c55e' : '#16a34a'
  const signals = fg.signals || {}
  return (
    <div className="arena-layout">
      <div className="card">
        <div className="card-header"><h2>Fear & Greed Index</h2></div>
        <div style={{ padding: 32, textAlign: 'center' }}>
          {value != null ? (
            <>
              <div style={{ fontSize: 72, fontWeight: 900, color, fontFamily: 'JetBrains Mono, monospace', lineHeight: 1 }}>{Math.round(value)}</div>
              <div style={{ fontSize: 18, color, fontWeight: 700, marginTop: 8 }}>{label}</div>
              {Object.keys(signals).length > 0 && (
                <div style={{ marginTop: 16, display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center' }}>
                  {Object.entries(signals).map(([key, sig]) => {
                    const sv = sig.score ?? sig.value
                    const sc = sv == null ? '#64748b' : sv < 40 ? '#ef4444' : sv < 60 ? '#eab308' : '#22c55e'
                    return (
                      <div key={key} style={{ padding: '6px 12px', borderRadius: 6, background: '#1e293b', minWidth: 90 }}>
                        <div style={{ fontSize: 9, color: '#64748b', fontWeight: 700, letterSpacing: 1 }}>{key.toUpperCase()}</div>
                        <div style={{ fontSize: 14, fontWeight: 700, color: sc, fontFamily: 'monospace' }}>
                          {sv != null ? Math.round(sv) : '—'}
                        </div>
                        {sig.signal && <div style={{ fontSize: 9, color: '#475569' }}>{sig.signal}</div>}
                      </div>
                    )
                  })}
                </div>
              )}
            </>
          ) : (
            <div style={{ color: '#64748b' }}>Data unavailable</div>
          )}
        </div>
      </div>
    </div>
  )
}

function ETFCommandPage() {
  const { data, loading } = usePolling(api.getPremiumETFs, 300000)
  const catObj = data?.categories || {}
  const regime = data?.regime
  const recommendation = data?.recommendation
  const categories = Object.entries(catObj)
  return (
    <div className="arena-layout">
      {recommendation && (
        <div className="card">
          <div className="card-header"><h2>ETF Command Center</h2></div>
          <div style={{ padding: '12px 16px', background: '#0d1117' }}>
            <div style={{ fontSize: 13, color: '#e2e8f0', fontWeight: 600 }}>{recommendation.message}</div>
            <div style={{ fontSize: 11, color: '#64748b', marginTop: 6 }}>
              Regime: <strong style={{ color: '#94a3b8' }}>{regime}</strong>
              {' · '}Primary: <strong style={{ color: '#22c55e' }}>{recommendation.primary}</strong>
              {' · '}Avoid: <strong style={{ color: '#ef4444' }}>{recommendation.avoid}</strong>
            </div>
          </div>
        </div>
      )}
      {loading ? <div className="loading">Loading…</div> : categories.length === 0 ? (
        <div className="empty-state">No ETF data available.</div>
      ) : categories.map(([catKey, cat]) => (
        <div key={catKey} className="card" style={{ marginBottom: 8 }}>
          <div className="card-header" style={{ padding: '10px 16px' }}>
            <h2 style={{ fontSize: 13 }}>{cat.emoji} {cat.label || catKey} <span style={{ color: '#64748b', fontSize: 11 }}>— {cat.owner}</span></h2>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, padding: '10px 16px 12px' }}>
            {(cat.etfs || []).map((etf, ei) => {
              const pct = etf.change_1d
              return (
                <div key={ei} style={{
                  padding: '5px 12px', borderRadius: 5, fontSize: 11, fontWeight: 700,
                  background: pct == null ? '#1e293b' : pct >= 0 ? '#052e16' : '#2d0a0a',
                  color: pct == null ? '#94a3b8' : pct >= 0 ? '#22c55e' : '#ef4444',
                  fontFamily: 'monospace', border: '1px solid #1e293b',
                }}>
                  <span style={{ color: '#e2e8f0' }}>{etf.ticker}</span>
                  {pct != null && <span style={{ marginLeft: 6 }}>{pct >= 0 ? '+' : ''}{pct.toFixed(2)}%</span>}
                </div>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}

function CorrelationPage() {
  const { data, loading } = usePolling(api.getCorrelation, 300000)
  // API returns { matrix: [[...]], symbols: [...], warnings: [...] }
  const matrixArr = data?.matrix || null
  const symbols = data?.symbols || []
  const warnings = data?.warnings || []
  return (
    <div className="arena-layout">
      <div className="card">
        <div className="card-header">
          <h2>Market Correlation</h2>
          <span style={{ fontSize: 11, color: '#64748b' }}>30-day rolling · {symbols.length} symbols</span>
        </div>
        {loading ? <div className="loading">Loading…</div> : !matrixArr ? (
          <div className="empty-state">No correlation data available.</div>
        ) : (
          <>
            {warnings.length > 0 && (
              <div style={{ padding: '8px 16px', background: '#1c1000', borderBottom: '1px solid #292400' }}>
                <span style={{ fontSize: 10, color: '#fbbf24', fontWeight: 700 }}>HIGH CORRELATION: </span>
                <span style={{ fontSize: 10, color: '#92400e' }}>{warnings.join(' · ')}</span>
              </div>
            )}
            <div style={{ overflowX: 'auto', padding: '12px 16px' }}>
              <table style={{ fontSize: 11, borderCollapse: 'collapse', fontFamily: 'monospace' }}>
                <thead>
                  <tr>
                    <th style={{ padding: '4px 8px', color: '#64748b', textAlign: 'left' }}></th>
                    {symbols.map(s => (
                      <th key={s} style={{ padding: '4px 8px', color: '#64748b', textAlign: 'center', fontSize: 10 }}>{s}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {symbols.map((row, ri) => (
                    <tr key={row}>
                      <td style={{ padding: '3px 8px', fontWeight: 700, color: '#94a3b8' }}>{row}</td>
                      {symbols.map((col, ci) => {
                        const v = matrixArr[ri]?.[ci] ?? null
                        const bg = v == null ? 'transparent' : v >= 0.8 ? '#1c2e1c' : v >= 0.5 ? '#0a2e1a' : v >= 0.2 ? '#1e293b' : v >= -0.2 ? '#0f172a' : v >= -0.5 ? '#2d1a0a' : '#2d0a0a'
                        const fg = v == null ? '#475569' : ri === ci ? '#475569' : v >= 0.8 ? '#f59e0b' : v >= 0.5 ? '#22c55e' : v >= 0.2 ? '#86efac' : v >= -0.2 ? '#94a3b8' : v >= -0.5 ? '#f97316' : '#ef4444'
                        return (
                          <td key={col} style={{ padding: '3px 8px', textAlign: 'center', background: bg, color: fg }}>
                            {ri === ci ? '—' : v != null ? v.toFixed(2) : '—'}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

function StarfleetIntelPage() {
  const [question, setQuestion] = useState('')
  const [answer, setAnswer] = useState(null)
  const [asking, setAsking] = useState(false)

  const ask = async () => {
    const q = question.trim()
    if (!q || asking) return
    setAsking(true)
    setAnswer(null)
    try {
      const res = await api.captainAsk(q)
      setAnswer(res.answer || res.error || 'No response')
    } catch {
      setAnswer('Error reaching Super Agent')
    } finally {
      setAsking(false)
    }
  }

  return (
    <div className="arena-layout">
      {/* Captain's Orders — Super Agent Q&A */}
      <div className="card">
        <div className="card-header">
          <h2>⭐ Captain's Orders — Super Agent</h2>
          <span style={{ fontSize: 11, color: '#f59e0b' }}>CrewAI Collective Intelligence</span>
        </div>
        <div style={{ padding: '12px 16px' }}>
          <div style={{ fontSize: 12, color: '#64748b', marginBottom: 10 }}>
            Ask the Super Agent anything about crew positions, strategy, or market conditions. It responds with real crew data.
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <input
              value={question}
              onChange={e => setQuestion(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && ask()}
              placeholder="e.g. What is the crew most convicted on? What's our biggest risk?"
              style={{
                flex: 1, padding: '8px 12px', borderRadius: 6, fontSize: 12,
                background: '#0f172a', border: '1px solid #334155', color: '#e2e8f0',
                outline: 'none',
              }}
            />
            <button
              onClick={ask}
              disabled={asking || !question.trim()}
              style={{
                padding: '8px 18px', borderRadius: 6, fontSize: 12, fontWeight: 700,
                background: asking ? '#1e293b' : '#f59e0b', color: asking ? '#64748b' : '#0a0a1a',
                border: 'none', cursor: asking ? 'default' : 'pointer',
              }}
            >
              {asking ? 'Asking…' : 'Ask'}
            </button>
          </div>
          {answer && (
            <div style={{
              marginTop: 12, padding: '12px 14px', borderRadius: 6,
              background: '#0d1f0d', border: '1px solid #166534',
              fontSize: 13, color: '#86efac', lineHeight: 1.6,
            }}>
              <span style={{ fontSize: 10, fontWeight: 700, color: '#f59e0b', display: 'block', marginBottom: 6 }}>
                ⭐ SUPER AGENT
              </span>
              {answer}
            </div>
          )}
        </div>
      </div>

      <CTOAdvisory />
      <OfficerConsensus />
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────

// Compact sector bar for arena dashboard — "The Bridge" summary
function SectorBridgeBar({ onNavigate }) {
  const { data } = usePolling(api.getSectorHeatmap, 60000)
  const { data: maData } = usePolling(api.getMaRegime, 300000)
  const sectors = data?.sectors || []
  const spyPct = data?.spy_change_pct ?? null
  const cacheAge = data?.cache_age_seconds ?? null
  const TOTAL_SECTORS = 12  // Always 12 — Defense/Aero + 11 standard

  // 8/21 MA Cross regime data
  const ma = maData?.current ?? null

  if (sectors.length === 0) return null

  function chipColor(pct) {
    if (pct >= 2)    return { bg: '#052e16', color: '#4ade80', border: '#16a34a' }
    if (pct >= 1)    return { bg: '#0a2e1a', color: '#86efac', border: '#15803d' }
    if (pct >= 0.3)  return { bg: '#0f2a12', color: '#bbf7d0', border: '#166534' }
    if (pct >= 0)    return { bg: '#1a2e1a', color: '#6ee7b7', border: '#1e3a2a' }
    if (pct >= -0.3) return { bg: '#2e1a1a', color: '#fca5a5', border: '#3a1e1e' }
    if (pct >= -1)   return { bg: '#2e0a0a', color: '#f87171', border: '#7f1d1d' }
    if (pct >= -2)   return { bg: '#2e0505', color: '#ef4444', border: '#991b1b' }
    return            { bg: '#1e0000', color: '#dc2626', border: '#7f1d1d' }
  }

  // "X/12 beating SPY" — how many sectors outperform SPY today
  const beatingSpy = spyPct != null
    ? sectors.filter(s => s.change_pct > spyPct).length
    : sectors.filter(s => s.change_pct > 0).length
  const beatLabel = spyPct != null
    ? `${beatingSpy}/${TOTAL_SECTORS} beating SPY (${spyPct >= 0 ? '+' : ''}${spyPct.toFixed(2)}%)`
    : `${beatingSpy}/${TOTAL_SECTORS} positive`

  const isStale = cacheAge != null && cacheAge > 600

  return (
    <div style={{
      background: '#0d1117', border: `1px solid ${isStale ? 'rgba(245,158,11,0.3)' : '#1e293b'}`, borderRadius: 8,
      padding: '8px 12px',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <span style={{ fontSize: 10, fontWeight: 700, color: '#64748b', letterSpacing: 1 }}>
          THE BRIDGE — SECTOR OVERVIEW
        </span>
        <span style={{
          fontSize: 10, fontFamily: 'monospace', fontWeight: 700,
          color: beatingSpy >= TOTAL_SECTORS / 2 ? '#22c55e' : '#ef4444',
        }}>
          {beatLabel}
        </span>
        {isStale && (
          <span style={{ fontSize: 9, color: '#f59e0b' }}>⏸ stale</span>
        )}
        <button
          onClick={() => onNavigate('sectors')}
          style={{ fontSize: 9, color: '#00d4aa', background: 'none', border: 'none', cursor: 'pointer', marginLeft: 'auto', padding: 0 }}
        >
          Full Heatmap →
        </button>
      </div>
      {/* 8/21 MA Cross Regime Row */}
      {ma && (() => {
        const regimeColors = {
          BULL_CROSS:    { bg: '#052e16', color: '#4ade80', icon: '▲' },
          CAUTIOUS_BULL: { bg: '#0f2a12', color: '#86efac', icon: '△' },
          CAUTIOUS_BEAR: { bg: '#2e1a0a', color: '#fbbf24', icon: '▽' },
          BEAR_CROSS:    { bg: '#2e0a0a', color: '#f87171', icon: '▼' },
        }
        const rc = regimeColors[ma.regime] || { bg: '#1e293b', color: '#94a3b8', icon: '?' }
        const regimeLabel = (ma.regime || '').replace('_', ' ')
        const crossDate = ma.cross_date ? new Date(ma.cross_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : null
        const daysAgo = ma.cross_days_ago ?? null
        const modifier = ma.size_modifier != null ? `${Math.round(ma.size_modifier * 100)}%` : null
        return (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 9, fontWeight: 700, color: '#64748b', letterSpacing: 0.5 }}>8/21 MA</span>
            <span style={{
              display: 'inline-flex', alignItems: 'center', gap: 4,
              background: rc.bg, color: rc.color,
              border: `1px solid ${rc.color}44`,
              borderRadius: 4, padding: '2px 6px', fontSize: 10, fontWeight: 700, fontFamily: 'monospace',
            }}>
              {rc.icon} {regimeLabel}
              {crossDate && <span style={{ color: '#94a3b8', fontWeight: 400 }}>({crossDate}{daysAgo != null ? ` · ${daysAgo}d` : ''})</span>}
            </span>
            {ma.spy_close != null && (
              <span style={{ fontSize: 9, fontFamily: 'monospace', color: '#94a3b8' }}>
                SPY <span style={{ color: '#e2e8f0' }}>${ma.spy_close?.toFixed(2)}</span>
                {' · '}8MA=<span style={{ color: ma.spy_close >= ma.spy_ma8 ? '#4ade80' : '#f87171' }}>${ma.spy_ma8?.toFixed(2)}</span>
                {' · '}21MA=<span style={{ color: ma.spy_close >= ma.spy_ma21 ? '#4ade80' : '#f87171' }}>${ma.spy_ma21?.toFixed(2)}</span>
              </span>
            )}
            {modifier && (
              <span style={{
                fontSize: 9, fontFamily: 'monospace', fontWeight: 700,
                color: ma.size_modifier >= 1 ? '#4ade80' : ma.size_modifier >= 0.75 ? '#fbbf24' : '#f87171',
              }}>
                pos size: {modifier}
              </span>
            )}
          </div>
        )
      })()}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
        {sectors.map(s => {
          const c = chipColor(s.change_pct)
          const isPos = s.change_pct >= 0
          const beatsSpy = spyPct != null && s.change_pct > spyPct
          return (
            <button
              key={s.sector}
              onClick={() => onNavigate('sectors')}
              title={`${s.sector} (${s.etf}): ${isPos ? '+' : ''}${s.change_pct?.toFixed(2)}%${spyPct != null ? ` · ${beatsSpy ? '↑ beating' : '↓ trailing'} SPY` : ''}`}
              style={{
                display: 'flex', alignItems: 'center', gap: 4,
                padding: '3px 7px', borderRadius: 4, cursor: 'pointer',
                background: c.bg, color: c.color,
                border: `1px solid ${beatsSpy ? '#22c55e' : c.border}`,
                fontSize: 10, fontWeight: 700, fontFamily: 'monospace',
                whiteSpace: 'nowrap',
              }}
            >
              <span style={{ color: '#94a3b8', fontWeight: 400 }}>{s.sector}</span>
              <span>{isPos ? '+' : ''}{s.change_pct?.toFixed(2)}%</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}

const NAV_GROUPS = [
  { group: 'COMMAND CENTER', items: [
    { id: 'arena', label: 'The Bridge', icon: '\u{1F3E0}' },
    { id: 'war-room', label: 'War Room', icon: '\u2694' },
    { id: 'ready-room', label: "Riker's Log", icon: '\u{1F4CB}' },
    { id: 'journal', label: 'Journal', icon: '\u{1F4D3}' },
    { id: 'daily-briefing', label: 'Trading Day Wizard', icon: '\u{1F9ED}', description: 'Morning checklist. Market regime, crew status, risk alerts, and Captain\'s top trade ideas — all in one place.' },
  ]},
  { group: 'PORTFOLIOS', items: [
    { id: 'leaderboard', label: 'Leaderboard', icon: '\u{1F3C6}' },
    { id: 'portfolio', label: 'Alpaca Paper', icon: '\u{1F4BC}' },
    { id: 'neo', label: 'Neo', icon: '\u{1F576}' },
    { id: 'positions', label: 'Positions / Orders', icon: '\u{1F4DD}' },
    { id: 'risk', label: 'Risk / Portfolio', icon: '\u26A0' },
    { id: 'webull', label: 'Webull (Human)', icon: '\u{1F4B0}' },
    { id: 'models', label: 'Metals', icon: '\u{1F5A5}' },
    { id: 'trades', label: 'Analytics', icon: '\u{1F4CA}' },
    { id: 'cto', label: 'P&L Attribution', icon: '\u{1F4C8}' },
  ]},
  { group: 'SCANNERS', items: [
    { id: 'navigator-intel', label: 'Navigator (Chekov)', icon: '\u{1F52D}' },
    { id: 'intraday', label: 'Intraday Charts', icon: '\u{1F4F9}', description: 'Live minute-by-minute price charts for active trading sessions.' },
    { id: 'screener', label: 'Screener', icon: '\u{1F50D}', description: 'Find stocks meeting specific criteria. Filter by momentum, volume, RSI and more.' },
    { id: 'alert-history', label: 'Signal Tracker', icon: '\u{1F4E1}' },
    { id: 'squeeze', label: 'Risk Radar', icon: '\u{1F3AF}', description: 'Short squeeze scanner — high short interest, small float, volume breakouts.' },
    { id: 'impulse', label: 'Dynamic Alerts', icon: '\u{1F514}' },
  ]},
  { group: 'OPTIONS', items: [
    { id: 'uoa', label: 'Unusual Options', icon: '\u{1F40B}', description: 'Unusual Options Activity scanner. Detects smart money bets via volume/OI spikes, big premiums, and put/call anomalies.' },
    { id: 'options-flow', label: 'Options Flow', icon: '\u{1F4CA}', description: 'See where big money is betting with options. Large unusual options trades often signal major moves ahead.' },
    { id: 'options-greeks', label: 'Greeks', icon: '\u{1F52C}', description: 'Understand the risk of any options trade. Delta, Gamma, Theta explained in plain English.' },
    { id: 'gex', label: 'GEX Map', icon: '\u{1F5FA}', description: 'Gamma Exposure shows where the market is likely to get pinned or make big moves based on options positioning.' },
  ]},
  { group: 'INTELLIGENCE', items: [
    { id: 'capitol-trades', label: 'Capitol Trades', icon: '\u{1F3DB}', description: 'Track what US Congress members are buying and selling. Politicians often trade ahead of major legislation.' },
    { id: 'insiders', label: 'Insider Trades', icon: '\u{1F454}', description: 'Corporate executives buying their own company stock is a strong bullish signal.' },
    { id: 'fear-greed', label: 'Fear & Greed', icon: '\u{1F631}', description: 'The market mood meter. Extreme fear often means buying opportunity. Extreme greed means caution.' },
    { id: 'economy', label: 'Economy', icon: '\u{1F30D}' },
    { id: 'fundamentals', label: 'Earnings Hub', icon: '\u{1F4C5}' },
    { id: 'correlation', label: 'Correlation', icon: '\u{1F517}', description: 'See which stocks move together. Helps avoid putting all your eggs in one basket.' },
    { id: 'premarket', label: 'Pre-Market', icon: '\u23F0', description: 'Stocks opening significantly higher or lower than yesterday. Gap trades are popular short-term strategies.' },
  ]},
  { group: 'RESEARCH', items: [
    { id: 'strategy-lab', label: 'Strategy Lab', icon: '\u{1F9EA}' },
    { id: 'starfleet-intel', label: 'Strategy Race', icon: '\u{1F3CE}' },
    { id: 'movers', label: 'Stock Race', icon: '\u{1F4CA}' },
    { id: 'backtest', label: 'Backtester', icon: '\u{1F519}', description: 'Test any trading strategy against historical data. See how it would have performed before risking real money.' },
  ]},
  { group: 'SETTINGS', items: [
    { id: 'costs', label: 'Cost Tracker', icon: '$' },
    { id: 'news', label: 'News Feed', icon: '\u{1F4F0}' },
    { id: 'sectors', label: 'Sector Heatmap', icon: '\u{1F321}' },
    { id: 'gaps', label: 'Morning Gaps', icon: '\u25B2\u25BC' },
    { id: 'chart-analyzer', label: 'Chart Analyzer', icon: '\u{1F4C8}' },
    { id: 'seasons', label: 'Seasons', icon: '\u{1F4C5}' },
    { id: 'etf-command', label: 'ETF Command', icon: '\u{1F4E6}' },
    { id: 'comms', label: 'Comms', icon: '\u{1F4AC}' },
  ]},
]

const LEAN_NAV_ITEMS = {
  arena: { label: 'Dashboard' },
  leaderboard: { label: 'Leaderboard' },
  portfolio: { label: 'Mr Anderson / Collective' },
  neo: { label: 'Neo Benchmark' },
  trades: { label: 'Trade History' },
  'chart-analyzer': { label: 'Chart / Indicators' },
  costs: { label: 'Allocation / Ratings' },
  positions: { label: 'Positions / Orders' },
  risk: { label: 'Risk / Portfolio' },
  models: { label: 'Model Controls' },
}

const ACTIVE_NAV_GROUPS = NAV_GROUPS
  .map(group => ({
    ...group,
    items: group.items
      .filter(item => LEAN_NAV_ITEMS[item.id])
      .map(item => ({ ...item, ...LEAN_NAV_ITEMS[item.id] })),
    subsections: (group.subsections || [])
      .map(section => ({
        ...section,
        items: section.items
          .filter(item => LEAN_NAV_ITEMS[item.id])
          .map(item => ({ ...item, ...LEAN_NAV_ITEMS[item.id] })),
      }))
      .filter(section => section.items.length > 0),
  }))
  .filter(group => group.items.length > 0 || (group.subsections || []).length > 0)

// Flat list for page rendering (includes subsection items)
const NAV_ITEMS = ACTIVE_NAV_GROUPS.flatMap(g => [
  ...g.items,
  ...(g.subsections || []).flatMap(s => s.items),
])

const PROVIDER_COLORS = {
  anthropic: '#22c55e',
  openai: '#22c55e',
  google: '#3b82f6',
  xai: '#ef4444',
  ollama: '#94a3b8',
  dayblade: '#f59e0b',
  matrix: '#00bcd4',
}

export default function App() {
  const [selectedPlayer, setSelectedPlayer] = useState(null)
  const [filterPlayer, setFilterPlayer] = useState(null)
  const [page, setPage] = useState('arena')
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [season, setSeason] = useState(0) // 0 = current, -1 = all seasons
  const [navCollapsed, setNavCollapsed] = useState(() => {
    try { return JSON.parse(localStorage.getItem('tm_nav_collapsed') || '{}') } catch { return {} }
  })
  const [highlightedPill, setHighlightedPill] = useState('super-agent')
  const [chartFocus, setChartFocus] = useState('SPY')
  const [posPanel, setPosPanel] = useState(false)
  const [posData, setPosData] = useState(null)
  const [posLoading, setPosLoading] = useState(false)

  // ── Trading Day Wizard state ────────────────────────────────────────────────
  const [wizardOpen, setWizardOpen] = useState(false)
  const [session, setSession] = useState(() => LS.get(SESSION_KEY(), null))
  const [marketOpenBanner, setMarketOpenBanner] = useState(false)

  // Auto-launch at 9:30 AM ET market open — check every 30s
  useEffect(() => {
    const check = () => {
      if (!isMarketOpenWindow()) return
      setMarketOpenBanner(true)
      const mode = LS.get(WIZARD_AUTO_KEY, 'auto')
      if (mode === 'disabled') return
      if (mode !== 'auto') return
      if (LS.get(WIZARD_DONE_KEY(), false)) return
      if (LS.get(WIZARD_SKIP_KEY(), false)) return
      setWizardOpen(true)
    }
    check()
    const t = setInterval(check, 30000)
    return () => clearInterval(t)
  }, [])

  // Handle cross-component navigation (e.g. ticker clicks from SectorHeatmap → ChartAnalyzer)
  useEffect(() => {
    const handler = (e) => {
      if (e.detail?.page) setPage(e.detail.page)
    }
    window.addEventListener('tm:navigate', handler)
    return () => window.removeEventListener('tm:navigate', handler)
  }, [])

  const { data: status } = usePolling(api.getStatus, 5000)
  const { data: dailyCost } = usePolling(api.getDailyCostTotal, 30000)
  const fetchLeaderboard = useCallback(() => api.getLeaderboard(season || undefined), [season])
  const { data: leaderboardData, loading: leaderboardLoading, error: leaderboardError, lastUpdated: lbUpdated } = usePolling(fetchLeaderboard, 30000)

  // Extract leaderboard array (API returns {season, current_season, leaderboard})
  const leaderboard = Array.isArray(leaderboardData) ? leaderboardData : (leaderboardData?.leaderboard || [])
  const currentSeason = leaderboardData?.current_season || status?.current_season || 2
  const viewingSeason = season === -1 ? -1 : (leaderboardData?.season || currentSeason)

  // Compute individual player stats for highlighted pill
  const playerStats = useMemo(() => {
    if (!leaderboard || leaderboard.length === 0) return null
    const p = leaderboard.find(x => x.player_id === highlightedPill)
    if (!p) return null
    const displayCapital = p.current_equity ?? p.total_value ?? p.cash ?? getDisplayCapital(p)
const starting = p.starting_capital ?? (p.player_id === 'super-agent' ? 25000 : (p.player_id === 'dayblade-0dte' ? 5000 : (p.player_id === 'steve-webull' ? 7049.68 : 7000)))
const pnl = displayCapital - starting
const color = p.player_id === 'steve-webull' ? '#fbbf24' : (displayCapital < starting ? '#ef4444' : '#22c55e')
    const totalPnl = displayCapital - starting
    const unrealizedPnl = safeNumber(p.unrealized_pnl, 0)
    const winRate = safeNumber(p.win_rate, 0)
    const gains = safeNumber(p.realized_gains, 0)
    const losses = safeNumber(p.realized_losses, 0)
    const profitFactor = losses > 0 ? gains / losses : (gains > 0 ? 999 : 0)
    const openPositions = p.positions || 0
    return { totalPnl, unrealizedPnl, winRate, profitFactor, openPositions, name: p.name }
  }, [leaderboard, highlightedPill, viewingSeason])

  // Fetch positions for highlighted pill (triggered by posPanel open)
  useEffect(() => {
    if (!posPanel || !highlightedPill) return
    setPosLoading(true)
    const fetchFn = highlightedPill === 'super-agent'
      ? api.getAlpacaPositions
      : () => api.getPlayerOpenPositions(highlightedPill)
    fetchFn().then(data => {
      // Normalize: Alpaca returns array, open-positions returns {positions:[]}
      setPosData(Array.isArray(data) ? data : (data.positions || []))
      setPosLoading(false)
    }).catch(() => { setPosData([]); setPosLoading(false) })
  }, [posPanel, highlightedPill])

  // Top 5 AI models for pills (exclude Steve — he gets his own pill)
  const topModels = useMemo(() => {
    if (!leaderboard || leaderboard.length === 0) return []
    return [...leaderboard]
      .filter(p => p.player_id !== 'steve-webull' && !p.is_paused)
      .sort((a, b) => getDisplayCapital(b) - getDisplayCapital(a))
      .slice(0, 5)
  }, [leaderboard])

  // Steve's portfolio (always shown as benchmark)
  const steveData = useMemo(() => {
    if (!leaderboard) return null
    return leaderboard.find(p => p.player_id === 'steve-webull') || null
  }, [leaderboard])

  return (
    <div className="app-layout">
      {/* Sidebar */}
      <nav className={`sidebar ${sidebarCollapsed ? 'collapsed' : ''}`}>
        <div className="sidebar-header">
          <div className="logo">
            <span className="logo-icon">TM</span>
            {!sidebarCollapsed && <span className="logo-text">TradeMinds</span>}
          </div>
          <button className="collapse-btn" onClick={() => setSidebarCollapsed(!sidebarCollapsed)}>
            {sidebarCollapsed ? '\u2192' : '\u2190'}
          </button>
        </div>

        <div className="nav-items">
          {ACTIVE_NAV_GROUPS.map(({ group, items, subsections }) => {
            const isGroupCollapsed = navCollapsed[group]
            const toggleGroup = (g) => {
              const next = { ...navCollapsed, [g]: !navCollapsed[g] }
              setNavCollapsed(next)
              localStorage.setItem('tm_nav_collapsed', JSON.stringify(next))
            }
            return (
              <div key={group} className="nav-group">
                {!sidebarCollapsed && (
                  <div
                    className="nav-group-header"
                    onClick={() => toggleGroup(group)}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 4, padding: '8px 12px 4px',
                      color: '#64748b', fontSize: 10, fontWeight: 700, letterSpacing: 1,
                      cursor: 'pointer', userSelect: 'none',
                    }}
                  >
                    <span style={{ fontSize: 8, width: 12 }}>{isGroupCollapsed ? '\u25B6' : '\u25BC'}</span>
                    {group}
                  </div>
                )}
                {!isGroupCollapsed && (
                  <>
                    {items.map(item => (
                      <button
                        key={item.id}
                        className={`nav-item ${page === item.id ? 'active' : ''}`}
                        onClick={() => setPage(item.id)}
                        title={item.label}
                      >
                        <span className="nav-icon">{item.icon}</span>
                        {!sidebarCollapsed && <span className="nav-label">{item.label}</span>}
                      </button>
                    ))}
                    {subsections && subsections.map(({ group: sg, items: si }) => {
                      const isSectionCollapsed = navCollapsed[sg]
                      return (
                        <div key={sg}>
                          {!sidebarCollapsed && (
                            <div
                              onClick={() => toggleGroup(sg)}
                              style={{
                                display: 'flex', alignItems: 'center', gap: 4,
                                padding: '6px 12px 3px 20px',
                                color: '#475569', fontSize: 9, fontWeight: 700, letterSpacing: 1,
                                cursor: 'pointer', userSelect: 'none',
                                borderLeft: '2px solid #334155', marginLeft: 8,
                              }}
                            >
                              <span style={{ fontSize: 7, width: 10 }}>{isSectionCollapsed ? '\u25B6' : '\u25BC'}</span>
                              {sg}
                            </div>
                          )}
                          {!isSectionCollapsed && si.map(item => (
                            <button
                              key={item.id}
                              className={`nav-item ${page === item.id ? 'active' : ''}`}
                              onClick={() => setPage(item.id)}
                              title={item.label}
                              style={{ paddingLeft: sidebarCollapsed ? undefined : 24 }}
                            >
                              <span className="nav-icon">{item.icon}</span>
                              {!sidebarCollapsed && <span className="nav-label">{item.label}</span>}
                            </button>
                          ))}
                        </div>
                      )
                    })}
                  </>
                )}
              </div>
            )
          })}
        </div>

        {!sidebarCollapsed && (
          <div className="sidebar-footer">
            {leaderboard && leaderboard.length > 0 && (
              <div style={{ padding: '6px 10px 8px', borderBottom: '1px solid #1e2336' }}>
                <div style={{ fontSize: 9, color: '#64748b', fontWeight: 700, letterSpacing: 1, marginBottom: 4 }}>MODEL CAPITAL</div>
                {[...leaderboard]
                  .filter(p => !p.is_paused)
                  .sort((a, b) => getDisplayCapital(b) - getDisplayCapital(a))
                  .map(p => {
                    // --- CLEAN CAPITAL + COLOR LOGIC (single source of truth) ---
const displayCapital = p.current_equity ?? p.total_value ?? p.cash ?? getDisplayCapital(p)

const starting =
  p.starting_capital ??
  (p.player_id === 'super-agent'
    ? 25000
    : p.player_id === 'dayblade-0dte'
      ? 5000
      : p.player_id === 'steve-webull'
        ? 7049.68
        : 7000)

const pnl = displayCapital - starting

const color =
  p.player_id === 'steve-webull'
    ? '#fbbf24'
    : displayCapital < starting
      ? '#ef4444'
      : '#22c55e'

const displayName = getPortfolioDisplayName(p)
                    return (
                      <div key={p.player_id} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, padding: '1px 0' }}>
                        <span style={{ color, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 90 }}>
                          {p.player_id === 'steve-webull' ? '\u{1F464} Steve' : displayName.split(' ')[0]}
                        </span>
                        <span style={{ fontFamily: 'JetBrains Mono, monospace', color: pnl >= 0 ? '#22c55e' : '#ef4444' }}>
                          {formatMoney(displayCapital)}
                        </span>
                      </div>
                    )
                  })
                }
              </div>
            )}
            {status && (
              <>
                <div className="status-indicator">
                  <span className="status-dot" />
                  <span>Live</span>
                </div>
                <div className="status-stats">
                  <div>{status.active_players} AIs</div>
                  <div>{status.total_trades} trades</div>
                </div>
              </>
            )}
          </div>
        )}
      </nav>

      {/* Main Content */}
      <main className="main-content">
        <LiveAlerts />
        <MarketTicker />

        {/* Session Banner (shows after wizard completion) */}
        <SessionBanner session={session} onReopen={() => setWizardOpen(true)} />

        {/* Market Open Banner — auto-shows at 9:30 AM ET */}
        {marketOpenBanner && (
          <div style={{
            background: '#052e16', borderBottom: '2px solid #166534',
            padding: '8px 16px', display: 'flex', alignItems: 'center', gap: 12,
            color: '#86efac', fontSize: 13, fontWeight: 600,
          }}>
            <span>🔔 MARKET OPEN — 9:30 AM ET</span>
            <button onClick={() => { setWizardOpen(true); setMarketOpenBanner(false) }}
              style={{ marginLeft: 8, background: '#166534', border: 'none', color: '#86efac',
                       cursor: 'pointer', padding: '4px 12px', borderRadius: 4, fontSize: 12, fontWeight: 700 }}>
              Launch Wizard 🧭
            </button>
            <button onClick={() => setMarketOpenBanner(false)}
              style={{ marginLeft: 'auto', background: 'none', border: 'none', color: '#86efac',
                       cursor: 'pointer', fontSize: 16, lineHeight: 1 }}>
              ✕
            </button>
          </div>
        )}

        <header className="page-header">
          <div className="page-title">
            <h1>{NAV_ITEMS.find(n => n.id === page)?.label || 'Dashboard'}</h1>
            {(() => {
              const desc = NAV_ITEMS.find(n => n.id === page)?.description
              return desc ? (
                <p style={{ margin: '2px 0 0', fontSize: 12, color: '#475569', fontWeight: 400, lineHeight: 1.4 }}>
                  {desc}
                </p>
              ) : null
            })()}
            {status && (
              <span className="header-stats">
                {formatMoney(safeNumber(status.total_portfolio_value, 0))} total AUM
              </span>
            )}
          </div>
          {status && (
            <div className="header-meta">
              <span>{status.total_trades} trades</span>
              <span>{status.total_signals} signals</span>
              {dailyCost && (
                <span style={{ color: safeNumber(dailyCost.daily_total, 0) === 0 ? '#22c55e' : safeNumber(dailyCost.daily_total, 0) < 1 ? '#eab308' : '#ef4444' }}>
                  API: ${safeNumber(dailyCost.daily_total, 0).toFixed(4)}
                </span>
              )}
              <AutoRefreshToggle />
              <TradeNotifications onTradeClick={() => setPage('trades')} />
              <WizardNavButton onClick={() => setWizardOpen(true)} />
              <a href="/logout" style={{
                background: 'none', border: '1px solid #334155', borderRadius: 4,
                color: '#94a3b8', fontSize: 11, fontWeight: 700, letterSpacing: 0.5,
                padding: '3px 8px', cursor: 'pointer', textDecoration: 'none',
              }}>LOGOUT</a>
            </div>
          )}
        </header>

        <div className="page-content">
          {/* DASHBOARD HOME — reorganized layout */}
          {page === 'arena' && (
            <div className="arena-layout">
              {/* CTO Advisory at top */}
              <CTOAdvisory compact />

              {/* Officer Consensus */}
              <OfficerConsensus compact />

              {/* The Bridge — compact sector heatmap */}
              <SectorBridgeBar onNavigate={setPage} />

              {/* Top Model Pills */}
              <div className="model-pills-bar">
                {steveData && (() => {
                  const stevePnl = getDisplayCapital(steveData) - 7049.68
                  const stevePos = stevePnl >= 0
                  const isHL = highlightedPill === 'steve-webull'
                  return (
                    <div className="model-pill" onClick={() => { setHighlightedPill('steve-webull'); setPosPanel(false) }}
                      style={{ borderColor: '#fbbf24', background: isHL ? 'rgba(251,191,36,0.18)' : 'rgba(251,191,36,0.08)',
                               outline: isHL ? '2px solid #fbbf24' : 'none', outlineOffset: 2 }}>
                      <span style={{ fontSize: 13 }}>{'\u{1F464}'}</span>
                      <span className="pill-name" style={{ color: '#fbbf24' }}>Steve</span>
                      <span className={`pill-pnl ${stevePos ? 'positive' : 'negative'}`}>
                        {stevePos ? '\u25B2' : '\u25BC'} {formatPercent(safeNumber(steveData.return_pct, 0), 1, true)}
                      </span>
                    </div>
                  )
                })()}
                {topModels.length > 0 ? topModels.map((m, i) => {
                  const color = PROVIDER_COLORS[m.provider] || '#94a3b8'
                  const displayCapital = getDisplayCapital(m)
                  const starting = m.player_id === 'dayblade-0dte' ? (viewingSeason === 1 ? 2000 : 5000) : 10000
                  const pnl = displayCapital - starting
                  const isPos = pnl >= 0
                  const isHL = highlightedPill === m.player_id
                  const displayName = getPortfolioDisplayName(m)
                  return (
                    <div key={m.player_id} className="model-pill" onClick={() => { setHighlightedPill(m.player_id); setPosPanel(false) }}
                      style={{ borderColor: color, background: isHL ? `${color}22` : undefined,
                               outline: isHL ? `2px solid ${color}` : 'none', outlineOffset: 2 }}>
                      <span className="pill-rank">#{i + 1}</span>
                      <div className="pill-name" style={{ color }}>
                        {displayName}
                      </div>
                      <span className={`pill-pnl ${isPos ? 'positive' : 'negative'}`}>
                        {isPos ? '+' : ''}{formatMoney(pnl)}
                      </span>
                    </div>
                  )
                }) : (
                  <div style={{ color: '#64748b', fontSize: 13, padding: '8px 0' }}>
                    {leaderboardLoading ? 'Loading models...' : leaderboardError ? 'Error loading data' : 'Waiting for data...'}
                  </div>
                )}
              </div>

              {/* Season Selector + Stat Cards */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <span style={{ color: '#94a3b8', fontSize: 12, fontWeight: 600 }}>SEASON</span>
                {Array.from({ length: currentSeason }, (_, i) => i + 1).map(s => (
                  <button key={s} onClick={() => setSeason(s)}
                    style={{
                      padding: '4px 12px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                      background: (season === 0 ? currentSeason : season) === s ? '#00d4aa' : '#1a1a2e',
                      color: (season === 0 ? currentSeason : season) === s ? '#0a0a1a' : '#94a3b8',
                      border: '1px solid #333', cursor: 'pointer',
                    }}>
                    S{s}{s === currentSeason ? ' (Live)' : ''}
                  </button>
                ))}
                <button onClick={() => setSeason(-1)}
                  style={{
                    padding: '4px 12px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                    background: season === -1 ? '#00d4aa' : '#1a1a2e',
                    color: season === -1 ? '#0a0a1a' : '#94a3b8',
                    border: '1px solid #333', cursor: 'pointer',
                  }}>
                  All Time
                </button>
              </div>
              {playerStats && (
                <>
                  <div style={{ fontSize: 11, color: '#64748b', fontWeight: 600, marginBottom: 4, letterSpacing: 0.5 }}>
                    {playerStats.name?.toUpperCase()} STATS
                  </div>
                  <div className="arena-stat-cards">
                    <div className="arena-stat-card">
                      <div className="asc-label">
                        Total P&L
                        <span className="asc-season-tag">{viewingSeason === -1 ? 'All' : `S${viewingSeason}`}</span>
                      </div>
                      <div className={`asc-value ${playerStats.totalPnl >= 0 ? 'positive' : 'negative'}`}>
                        {playerStats.totalPnl >= 0 ? '+' : ''}{formatMoney(playerStats.totalPnl)}
                      </div>
                    </div>
                    <div className="arena-stat-card" onClick={() => setPosPanel(p => !p)}
                      style={{ cursor: 'pointer', borderColor: posPanel ? '#00d4aa' : undefined }}>
                      <div className="asc-label">Unrealized P&L ↗</div>
                      <div className={`asc-value ${playerStats.unrealizedPnl >= 0 ? 'positive' : 'negative'}`}>
                        {playerStats.unrealizedPnl >= 0 ? '+' : ''}{formatMoney(playerStats.unrealizedPnl)}
                      </div>
                    </div>
                    <div className="arena-stat-card">
                      <div className="asc-label">Win Rate</div>
                      <div className="asc-value">{formatPercent(playerStats.winRate, 1)}</div>
                    </div>
                    <div className="arena-stat-card" onClick={() => setPosPanel(p => !p)}
                      style={{ cursor: 'pointer', borderColor: posPanel ? '#00d4aa' : undefined }}>
                      <div className="asc-label">Open Positions ↗</div>
                      <div className="asc-value">{playerStats.openPositions}</div>
                    </div>
                  </div>
                  {posPanel && (
                    <div style={{ background: '#0d1117', border: '1px solid #1e293b', borderRadius: 8, padding: 12, marginTop: 4 }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                        <span style={{ color: '#00d4aa', fontSize: 12, fontWeight: 700 }}>
                          {playerStats.name} — Open Positions
                        </span>
                        <button onClick={() => setPosPanel(false)}
                          style={{ background: 'none', border: 'none', color: '#64748b', cursor: 'pointer', fontSize: 16, lineHeight: 1 }}>✕</button>
                      </div>
                      {posLoading ? (
                        <div style={{ color: '#64748b', fontSize: 12, padding: '8px 0' }}>Loading positions...</div>
                      ) : posData && posData.length > 0 ? (
                        <div style={{ overflowX: 'auto' }}>
                          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                            <thead>
                              <tr style={{ color: '#64748b', borderBottom: '1px solid #1e293b' }}>
                                <th style={{ textAlign: 'left', padding: '4px 8px' }}>Ticker</th>
                                <th style={{ textAlign: 'left', padding: '4px 8px' }}>Type</th>
                                <th style={{ textAlign: 'right', padding: '4px 8px' }}>Qty</th>
                                <th style={{ textAlign: 'right', padding: '4px 8px' }}>Entry</th>
                                <th style={{ textAlign: 'right', padding: '4px 8px' }}>Current</th>
                                <th style={{ textAlign: 'right', padding: '4px 8px' }}>Day P&L</th>
                                <th style={{ textAlign: 'right', padding: '4px 8px' }}>Unreal P&L</th>
                                <th style={{ padding: '4px 4px' }}></th>
                              </tr>
                            </thead>
                            <tbody>
                              {posData.map((pos, i) => {
                                // Normalize: Alpaca vs arena positions have different field names
                                const sym = pos.symbol || pos.ticker
                                const type = pos.asset_type || pos.assetType || 'stock'
                                const unit = type === 'metal' ? (pos.unit || 'oz') : 'sh'
                                const qty = parseFloat(pos.qty || pos.quantity || pos.shares || 0)
                                const entry = parseFloat(pos.avg_price || pos.avg_entry_price || pos.avgEntryPrice || 0)
                                const current = parseFloat(pos.current_price || pos.currentPrice || pos.lastPrice || entry)
                                const dayPnl = 0
                                const unPnl = pos.unrealized_pnl != null
                                  ? parseFloat(pos.unrealized_pnl)
                                  : parseFloat(pos.unrealizedPL || pos.unrealized_pl || (qty * (current - entry)))
                                const isPos = unPnl >= 0
                                return (
                                  <tr key={i} style={{ borderBottom: '1px solid #0f172a' }}>
                                    <td style={{ padding: '4px 8px', fontWeight: 600, color: '#e2e8f0' }}>{sym}</td>
                                    <td style={{ padding: '4px 8px', color: '#94a3b8', textTransform: 'capitalize' }}>{type}</td>
                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: '#e2e8f0' }}>{qty} {unit}</td>
                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: '#94a3b8' }}>${entry.toFixed(2)}</td>
                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: '#e2e8f0' }}>${current.toFixed(2)}</td>
                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: dayPnl >= 0 ? '#22c55e' : '#ef4444' }}>
                                      {dayPnl >= 0 ? '+' : ''}${dayPnl.toFixed(2)}
                                    </td>
                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: isPos ? '#22c55e' : '#ef4444' }}>
                                      {isPos ? '+' : ''}${unPnl.toFixed(2)}
                                    </td>
                                    <td style={{ padding: '4px 4px', textAlign: 'center' }}>
                                      <button onClick={() => window.open(`http://127.0.0.1:5001/ib_chart.html?symbol=${sym}`, '_blank')}
                                        style={{ background: 'none', border: '1px solid #334155', borderRadius: 4, color: '#94a3b8', cursor: 'pointer', fontSize: 11, padding: '1px 4px' }}>📈</button>
                                    </td>
                                  </tr>
                                )
                              })}
                            </tbody>
                          </table>
                        </div>
                      ) : (
                        <div style={{ color: '#64748b', fontSize: 12, padding: '8px 0' }}>No open positions.</div>
                      )}
                    </div>
                  )}
                </>
              )}

              {/* Leaderboard (active only on dashboard) */}
              <div className="card">
                <div className="card-header">
                  <h2>Leaderboard</h2>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <LastUpdated time={lbUpdated} />
                    <span className="card-badge">{leaderboard?.filter(p => !p.is_paused).length || 0} active</span>
                  </div>
                </div>
                <Leaderboard data={leaderboard || []} onSelect={setSelectedPlayer} season={viewingSeason} hidePaused />
              </div>

              {/* P&L Tracker — single agent equity curve with selector */}
              <BubblePnL leaderboard={leaderboard} />

              {/* Holdings: Top Winners & Top Losers by unrealized P&L */}
              <HoldingsTopWL />

              {/* Trade Feed (last 10) */}
              <div className="card">
                <div className="card-header">
                  <h2>Latest Trades</h2>
                </div>
                <RecentTrades compact limit={10} season={viewingSeason > 0 ? viewingSeason : undefined} />
              </div>
            </div>
          )}

          {/* Daily Briefing */}
          {page === 'daily-briefing' && (
            <DailyBriefing leaderboard={leaderboard} onNavigate={setPage} />
          )}

          {/* Admiral Picard — Ready Room */}
          {page === 'ready-room' && <ReadyRoom />}

          {/* CTO Advisory full page */}
          {page === 'cto' && (
            <>
              <CTOAdvisory />
              <OfficerConsensus />
            </>
          )}

          {/* Leaderboard full page with show paused toggle */}
          {page === 'leaderboard' && (
            <div className="arena-layout">
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <span style={{ color: '#94a3b8', fontSize: 12, fontWeight: 600 }}>SEASON</span>
                {Array.from({ length: currentSeason }, (_, i) => i + 1).map(s => (
                  <button key={s} onClick={() => setSeason(s)}
                    style={{
                      padding: '4px 12px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                      background: (season === 0 ? currentSeason : season) === s ? '#00d4aa' : '#1a1a2e',
                      color: (season === 0 ? currentSeason : season) === s ? '#0a0a1a' : '#94a3b8',
                      border: '1px solid #333', cursor: 'pointer',
                    }}>
                    S{s}{s === currentSeason ? ' (Live)' : ''}
                  </button>
                ))}
                <button onClick={() => setSeason(-1)}
                  style={{
                    padding: '4px 12px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                    background: season === -1 ? '#00d4aa' : '#1a1a2e',
                    color: season === -1 ? '#0a0a1a' : '#94a3b8',
                    border: '1px solid #333', cursor: 'pointer',
                  }}>
                  All Time
                </button>
              </div>
              <div className="card">
                <div className="card-header">
                  <h2>Arena Leaderboard</h2>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <LastUpdated time={lbUpdated} />
                    <span className="card-badge">{leaderboard?.length || 0} players</span>
                  </div>
                </div>
                <Leaderboard data={leaderboard || []} onSelect={setSelectedPlayer} season={viewingSeason} showPausedToggle />
              </div>
              <div className="card">
                <div className="card-header"><h2>Performance Comparison</h2></div>
                <ComparisonChart season={viewingSeason === -1 ? -1 : viewingSeason} />
              </div>
            </div>
          )}

          {/* War Room page */}
          {page === 'war-room' && <AIChatFeed warRoom />}

          {/* Trading Journal (signals + journal entries) */}
          {page === 'journal' && <RecentSignals filterPlayer={filterPlayer} onFilterPlayer={setFilterPlayer} />}

          {/* AI Portfolios — click any model pill to see detail */}
          {page === 'portfolio' && (
            <div className="arena-layout">
              <div className="card">
                <div className="card-header">
                  <h2>Mr Anderson / Collective</h2>
                  <span className="card-badge">super-agent</span>
                </div>
                <Leaderboard
                  data={(leaderboard || []).filter(p => p.player_id === 'super-agent')}
                  onSelect={setSelectedPlayer}
                  season={viewingSeason}
                  hidePaused
                />
              </div>
              <AndersonDecisionSummary />
              <RecentSignals filterPlayer="super-agent" />
              <div className="card">
                <div className="card-header"><h2>Collective Trade Feed</h2></div>
                <RecentTrades season={viewingSeason > 0 ? viewingSeason : undefined} filterPlayer="super-agent" />
              </div>
            </div>
          )}

          {page === 'neo' && (
            <div className="arena-layout">
              <div className="card">
                <div className="card-header">
                  <h2>Neo / Matrix Benchmark</h2>
                  <span className="card-badge">read only</span>
                </div>
                <Leaderboard
                  data={(leaderboard || []).filter(p => p.player_id === 'neo-matrix')}
                  onSelect={setSelectedPlayer}
                  season={viewingSeason}
                  hidePaused
                />
              </div>
              <div className="card">
                <div className="card-header"><h2>Neo Trade Feed</h2></div>
                <RecentTrades season={viewingSeason > 0 ? viewingSeason : undefined} filterPlayer="neo-matrix" />
              </div>
            </div>
          )}

          {page === 'models' && <ModelControl />}
          {page === 'costs' && <CostDashboard />}
          {page === 'fundamentals' && <Fundamentals />}
          {page === 'economy' && <EconomicCalendar />}
          {page === 'trades' && (
            <div className="arena-layout">
              <RecentTrades season={viewingSeason > 0 ? viewingSeason : undefined} filterPlayer={filterPlayer} onFilterPlayer={setFilterPlayer} />
              <RecentSignals filterPlayer={filterPlayer} onFilterPlayer={setFilterPlayer} />
            </div>
          )}
          {page === 'news' && <NewsFeed />}
          {page === 'webull' && <WebullPortfolio />}
          {page === 'backtest' && <BacktestLab />}
          {page === 'positions' && (
            <div className="arena-layout">
              <WebullPortfolio />
              <div className="card">
                <div className="card-header">
                  <h2>Recent Orders</h2>
                  <span className="card-badge">latest executions</span>
                </div>
                <RecentTrades compact season={viewingSeason > 0 ? viewingSeason : undefined} />
              </div>
            </div>
          )}
          {page === 'risk' && (
            <div className="arena-layout">
              <div className="card">
                <div className="card-header"><h2>Portfolio Risk Snapshot</h2></div>
                <Leaderboard data={leaderboard || []} onSelect={setSelectedPlayer} season={viewingSeason} hidePaused />
              </div>
              <BubblePnL leaderboard={leaderboard} />
              <div className="card">
                <div className="card-header"><h2>Portfolio Comparison</h2></div>
                <ComparisonChart season={viewingSeason === -1 ? -1 : viewingSeason} />
              </div>
            </div>
          )}
          {page === 'gex' && (
            <div className="arena-layout">
              <div className="card">
                <div className="card-header">
                  <h2>GEX MAP — Gamma Exposure</h2>
                  <span style={{ fontSize: 11, color: '#64748b', letterSpacing: 1 }}>
                    Alpaca options data · MM positioning levels · 30s auto-refresh
                  </span>
                </div>
                <GEXChart />
              </div>
            </div>
          )}
          {page === 'chart-analyzer' && (
            <div className="arena-layout">
              <div className="card">
                <div className="card-header">
                  <h2>Chart / Indicators</h2>
                  <span className="card-badge">candles · volume · 20 EMA · 50 SMA · 200 SMA · VWAP · RSI · MACD</span>
                </div>
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', padding: '0 16px 12px' }}>
                  {['SPY','QQQ','NVDA','TSLA','AAPL','MSFT','META','AMZN'].map(sym => (
                    <button
                      key={sym}
                      onClick={() => setChartFocus(sym)}
                      style={{
                        padding: '6px 12px', borderRadius: 6, fontSize: 12, fontWeight: 700,
                        cursor: 'pointer', border: '1px solid #334155',
                        background: chartFocus === sym ? '#00d4aa' : '#0f172a',
                        color: chartFocus === sym ? '#04130f' : '#94a3b8',
                      }}
                    >
                      {sym}
                    </button>
                  ))}
                </div>
                <div style={{ padding: '0 16px 16px' }}>
                  <iframe
                    src={`http://127.0.0.1:5001/ib_chart.html?symbol=${chartFocus}`}
                    title={`Chart ${chartFocus}`}
                    style={{ width: '100%', height: 720, border: '1px solid #1e293b', borderRadius: 10, background: '#020617' }}
                  />
                </div>
              </div>
              <ChartAnalyzer />
            </div>
          )}
          {page === 'intraday' && <ChartAnalyzer />}
          {page === 'premarket' && <PremarketGaps />}
          {page === 'screener' && <StockScreener />}
          {page === 'squeeze' && <SqueezeScanner />}
          {page === 'sma' && <SMADashboard />}
          {page === 'impulse' && <ImpulseAlerts />}
          {page === 'imbalance' && <ImbalanceZones />}
          {page === 'theta' && <ThetaOpportunities />}
          {page === 'gaps' && <MorningGaps />}
          {page === 'sectors' && <SectorHeatmap />}
          {page === 'movers' && <><MarketMovers /><WinnersLosers /></>}
          {page === 'insiders' && <InsiderTracker />}
          {page === 'strategy-lab' && <StrategyLab />}

          {/* ── INTELLIGENCE pages ── */}
          {page === 'starfleet-intel' && <StarfleetIntelPage />}
          {page === 'comms' && <AIChatFeed warRoom />}
          {page === 'navigator-intel' && <NavigatorIntel />}
          {page === 'alert-history' && <AlertHistory />}
          {page === 'fear-greed' && <FearGreedPage />}

          {/* ── MARKETS pages ── */}
          {page === 'capitol-trades' && <InsiderTracker />}
          {page === 'etf-command' && <ETFCommandPage />}
          {page === 'correlation' && <CorrelationPage />}
          {page === 'seasons' && (
            <div className="arena-layout">
              <div className="card">
                <div className="card-header"><h2>Season Performance History</h2></div>
                <ComparisonChart season={-1} />
              </div>
            </div>
          )}

          {/* ── OPTIONS pages ── */}
          {page === 'uoa' && <UOAPanel />}
          {page === 'options-flow' && <ThetaOpportunities />}
          {page === 'options-greeks' && <ThetaOpportunities />}
        </div>
      </main>

      <PlayerDetail
        playerId={selectedPlayer}
        onClose={() => setSelectedPlayer(null)}
      />

      {/* Trading Day Wizard */}
      <TradingWizard
        leaderboard={leaderboard}
        open={wizardOpen}
        onClose={() => setWizardOpen(false)}
        onSessionStart={(s) => setSession(s)}
      />
    </div>
  )
}
