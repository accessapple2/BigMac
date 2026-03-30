import React, { useState, useCallback } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { clampPercent, formatMoney, formatPercent, safeNumber } from '../utils/numbers'

const PROVIDER_COLORS = {
  anthropic: '#22c55e', openai: '#22c55e', google: '#3b82f6',
  xai: '#ef4444', ollama: '#94a3b8', dayblade: '#f59e0b',
}

function costColor(cost) {
  if (cost === 0) return '#22c55e'
  if (cost < 0.01) return '#84cc16'
  if (cost < 0.05) return '#eab308'
  return '#ef4444'
}

function gradeColor(grade) {
  const map = { A: '#22c55e', B: '#84cc16', C: '#eab308', D: '#f97316', F: '#ef4444' }
  return map[grade] || '#94a3b8'
}

export default function CostDashboard() {
  const [tab, setTab] = useState('overview')
  const fetchData = useCallback(() => api.getCostDashboard(), [])
  const fetchBudget = useCallback(() => api.getCostBudget(), [])
  const fetchModelControl = useCallback(() => api.getModelControl(), [])
  const { data, loading } = usePolling(fetchData, 60000)
  const { data: budget } = usePolling(fetchBudget, 60000)
  const { data: modelControl } = usePolling(fetchModelControl, 60000)

  if (loading || !data) return <div className="loading">Loading cost data...</div>

  // Temporary debug: inspect cost payload shape for free-calls field mapping.
  // Remove after payload validation is complete.
  console.log("FREE CALLS DEBUG", data)

  return (
    <div className="cost-dashboard">
      {/* Tabs */}
      <div className="tabs" style={{ marginBottom: 16 }}>
        {['overview', 'roi', 'grades', 'diversity'].map(t => (
          <button key={t} className={`tab ${tab === t ? 'active' : ''}`}
            onClick={() => setTab(t)}>
            {t === 'overview' ? 'Cost Overview' : t === 'roi' ? 'ROI Ranking' :
             t === 'grades' ? 'Efficiency Grades' : 'Model Diversity'}
          </button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab data={data} budget={budget} modelControl={modelControl} />}
      {tab === 'roi' && <ROITab data={data} />}
      {tab === 'grades' && <GradesTab data={data} />}
      {tab === 'diversity' && <DiversityTab data={data} />}
    </div>
  )
}

function OverviewTab({ data, budget, modelControl }) {
  const proj = data.projection || {}
  const fvp = data.free_vs_paid || {}
  const dead = data.dead_models || []
  const todaySpend = safeNumber(budget?.today_spent, safeNumber(data.daily_total, 0))
  const projectedMonthly = safeNumber(proj.total_monthly, 0)
  const dailyBudget = safeNumber(budget?.daily_limit, 5)
  const rawPct = dailyBudget > 0 ? (todaySpend / dailyBudget) * 100 : 0
  const pct = clampPercent(rawPct, 0, 999)
  const meterColor = pct >= 100 ? '#ef4444' : pct >= 80 ? '#eab308' : '#22c55e'
  const freeCallsRemaining = safeNumber(data?.free_calls_remaining, 0)
  const freeCallsLimit = safeNumber(data?.free_calls_limit, 0)
  const freeCallsUsed = safeNumber(data?.free_calls_used, 0)
  const freeCalls = (modelControl?.models || [])
    .filter(m => (m.cost_per_scan || 0) === 0)
    .reduce((sum, m) => sum + (m.api_calls_today || 0), 0)
  const topModel = [...(modelControl?.models || [])]
    .sort((a, b) => (b.total_cost_today || 0) - (a.total_cost_today || 0))[0]
  const usage = `${formatMoney(todaySpend)} / ${formatMoney(dailyBudget)}`
  const quota = formatPercent(pct, 0)
  const plan = budget?.status ?? 'NOMINAL'
  const bucket = pct >= 100 ? 'over-budget' : pct >= 80 ? 'warning' : 'nominal'
  const model = topModel?.model_id ?? '—'

  return (
    <>
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="card-header">
          <h2>Cost Tracker</h2>
          <span className="card-badge" style={{ color: meterColor }}>
            {Math.min(999, Math.max(0, pct)).toFixed(0)}%
          </span>
        </div>
        <div style={{ padding: '0 16px 16px' }}>
          <div style={{ fontSize: 13, color: '#e2e8f0', marginBottom: 6 }}>Today: <strong>{formatMoney(todaySpend)}</strong></div>
          <div style={{ fontSize: 13, color: '#94a3b8', marginBottom: 4 }}>Projected month: {formatMoney(projectedMonthly)}</div>
          <div style={{ fontSize: 13, color: '#94a3b8', marginBottom: 10 }}>Daily budget: {formatMoney(dailyBudget)}</div>
          <div style={{ height: 8, width: '100%', borderRadius: 999, background: '#27272a', overflow: 'hidden' }}>
            <div
              style={{
                height: '100%',
                width: `${clampPercent(pct)}%`,
                borderRadius: 999,
                background: meterColor,
              }}
            />
          </div>
          <div style={{ marginTop: 6, fontSize: 12, color: '#64748b' }}>
            {formatMoney(todaySpend)} / {formatMoney(dailyBudget)} today ({formatPercent(pct, 0)})
          </div>
          <div style={{ marginTop: 10, display: 'grid', gap: 4, fontSize: 12, color: '#a1a1aa' }}>
            <div style={{ marginBottom: 4 }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: '#e2e8f0' }}>Free Calls</div>
              <div>Remaining: {freeCallsRemaining}</div>
              <div>Used: {freeCallsUsed} / {freeCallsLimit}</div>
              <div>Total free-model calls today: {freeCalls}</div>
            </div>
            <div>Usage: {usage}</div>
            <div>Quota: {quota}</div>
            <div>Plan: {plan}</div>
            <div>Bucket: {bucket}</div>
            <div>Model: {model}</div>
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="arena-stat-cards" style={{ marginBottom: 16 }}>
        <div className="arena-stat-card">
          <div className="asc-label">Today's Cost</div>
          <div className="asc-value" style={{ color: costColor(safeNumber(data.daily_total, 0)) }}>
            ${safeNumber(data.daily_total, 0).toFixed(4)}
          </div>
        </div>
        <div className="arena-stat-card">
          <div className="asc-label">Projected Monthly</div>
          <div className="asc-value" style={{ color: costColor(projectedMonthly) }}>
            {formatMoney(projectedMonthly)}
          </div>
        </div>
        <div className="arena-stat-card">
          <div className="asc-label">Free Models P&L</div>
          <div className={`asc-value ${safeNumber(fvp.free?.total_pnl, 0) >= 0 ? 'positive' : 'negative'}`}>
            {safeNumber(fvp.free?.total_pnl, 0) >= 0 ? '+' : ''}{formatMoney(safeNumber(fvp.free?.total_pnl, 0))}
          </div>
        </div>
        <div className="arena-stat-card">
          <div className="asc-label">Paid Models P&L</div>
          <div className={`asc-value ${safeNumber(fvp.paid?.total_pnl, 0) >= 0 ? 'positive' : 'negative'}`}>
            {safeNumber(fvp.paid?.total_pnl, 0) >= 0 ? '+' : ''}{formatMoney(safeNumber(fvp.paid?.total_pnl, 0))}
          </div>
        </div>
      </div>

      {/* Per-Model Daily Costs */}
      <div className="card">
        <div className="card-header">
          <h2>Per-Model Costs (Today)</h2>
        </div>
        <div className="trade-list">
          {Object.entries(data.daily_costs || {}).length === 0 ? (
            <div className="empty-state">No API calls today yet.</div>
          ) : (
            Object.entries(data.daily_costs).sort((a, b) => b[1].total_cost - a[1].total_cost).map(([pid, c]) => (
              <div key={pid} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                  <strong>{pid}</strong>
                  <span style={{ marginLeft: 12, color: '#64748b', fontSize: 12 }}>
                    {c.num_calls} calls | {c.total_input?.toLocaleString()} in / {c.total_output?.toLocaleString()} out tokens
                  </span>
                </div>
                <span style={{ color: costColor(c.total_cost), fontWeight: 700, fontFamily: 'JetBrains Mono, monospace' }}>
                  ${c.total_cost.toFixed(4)}
                </span>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Cumulative Costs */}
      <div className="card" style={{ marginTop: 12 }}>
        <div className="card-header">
          <h2>Cumulative Costs (All-Time)</h2>
        </div>
        <div className="trade-list">
          {Object.entries(data.cumulative_costs || {}).sort((a, b) => b[1].total_cost - a[1].total_cost).map(([pid, c]) => (
            <div key={pid} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div>
                <strong>{pid}</strong>
                <span style={{ marginLeft: 12, color: '#64748b', fontSize: 12 }}>
                  {c.num_calls} calls | cost/trade: ${(data.cost_per_trade?.[pid]?.cost_per_trade || 0).toFixed(4)}
                </span>
              </div>
              <span style={{ color: costColor(c.total_cost), fontWeight: 700, fontFamily: 'JetBrains Mono, monospace' }}>
                ${c.total_cost.toFixed(4)}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Monthly Projection */}
      {proj.by_model && Object.keys(proj.by_model).length > 0 && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="card-header">
            <h2>Monthly Projection</h2>
            <span className="card-badge" style={{ color: costColor(proj.total_monthly || 0) }}>
              ${(proj.total_monthly || 0).toFixed(2)}/mo
            </span>
          </div>
          <div className="trade-list">
            {Object.entries(proj.by_model).sort((a, b) => b[1].projected_monthly - a[1].projected_monthly).map(([pid, p]) => (
              <div key={pid} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>{pid}</span>
                <span style={{ fontFamily: 'JetBrains Mono, monospace', color: costColor(p.projected_monthly) }}>
                  ${p.daily_avg.toFixed(4)}/day = ${p.projected_monthly.toFixed(2)}/mo
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Free vs Paid */}
      <div className="grid-2" style={{ marginTop: 12 }}>
        <div className="card">
          <div className="card-header"><h2>Free Models (Local)</h2></div>
          <div className="trade-list">
            {(fvp.free?.models || []).map(m => (
              <div key={m.player_id} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>{m.name}</span>
                <span className={m.pnl >= 0 ? 'positive' : 'negative'} style={{ fontWeight: 600 }}>
                  {m.pnl >= 0 ? '+' : ''}${m.pnl.toFixed(2)}
                </span>
              </div>
            ))}
          </div>
        </div>
        <div className="card">
          <div className="card-header"><h2>Paid Models (Cloud)</h2></div>
          <div className="trade-list">
            {(fvp.paid?.models || []).map(m => (
              <div key={m.player_id} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>{m.name}</span>
                <span className={m.pnl >= 0 ? 'positive' : 'negative'} style={{ fontWeight: 600 }}>
                  {m.pnl >= 0 ? '+' : ''}${m.pnl.toFixed(2)}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Dead Models */}
      {dead.length > 0 && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="card-header">
            <h2>Stale Models (No trade in 48h+)</h2>
            <span className="card-badge" style={{ background: '#ef444422', color: '#ef4444' }}>
              {dead.length} stale
            </span>
          </div>
          <div className="trade-list">
            {dead.map(d => (
              <div key={d.id} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>{d.display_name} <span style={{ color: '#64748b' }}>({d.provider})</span></span>
                <span style={{ color: '#f97316', fontSize: 12 }}>
                  {d.last_trade ? `Last: ${d.hours_since}h ago` : 'Never traded'}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </>
  )
}

function ROITab({ data }) {
  const roi = data.roi_ranking || []
  return (
    <div className="card">
      <div className="card-header">
        <h2>Model ROI Ranking</h2>
        <span className="card-badge">Profit per $ spent</span>
      </div>
      <div className="trade-list">
        {roi.map((m, i) => (
          <div key={m.player_id} className="trade-item" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span className={`rank rank-${i + 1}`} style={{ width: 28, textAlign: 'center', fontWeight: 700 }}>
              {i + 1}
            </span>
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <strong style={{ color: PROVIDER_COLORS[m.provider] || '#fff' }}>{m.name}</strong>
                {m.is_free && <span style={{ fontSize: 10, padding: '1px 5px', borderRadius: 3, background: '#22c55e22', color: '#22c55e' }}>FREE</span>}
              </div>
              <div style={{ fontSize: 12, color: '#64748b' }}>
                P&L: <span className={m.pnl >= 0 ? 'positive' : 'negative'}>{m.pnl >= 0 ? '+' : ''}${m.pnl.toFixed(2)}</span>
                {' | '}Cost: <span style={{ color: costColor(m.api_cost) }}>${m.api_cost.toFixed(4)}</span>
                {' | '}Net: <span className={m.net_pnl >= 0 ? 'positive' : 'negative'}>{m.net_pnl >= 0 ? '+' : ''}${m.net_pnl.toFixed(2)}</span>
              </div>
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 18, fontWeight: 700, fontFamily: 'JetBrains Mono, monospace', color: m.roi >= 1000 ? '#22c55e' : m.roi >= 100 ? '#84cc16' : m.roi >= 0 ? '#eab308' : '#ef4444' }}>
                {m.roi >= 999999 ? 'INF' : m.roi >= 10000 ? `${(m.roi/1000).toFixed(0)}K` : m.roi.toFixed(0)}x
              </div>
              <div style={{ fontSize: 10, color: '#64748b' }}>ROI</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function GradesTab({ data }) {
  const grades = data.efficiency_grades || []
  const eff = data.token_efficiency || {}

  return (
    <>
      <div className="card">
        <div className="card-header">
          <h2>Model Efficiency Grades</h2>
          <span className="card-badge">Win Rate + P&L + Cost + Tokens</span>
        </div>
        <div className="trade-list">
          {grades.map((g, i) => (
            <div key={g.player_id} className="trade-item" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{
                width: 36, height: 36, borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center',
                background: gradeColor(g.grade) + '22', color: gradeColor(g.grade),
                fontSize: 18, fontWeight: 800,
              }}>
                {g.grade}
              </div>
              <div style={{ flex: 1 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <strong style={{ color: PROVIDER_COLORS[g.provider] || '#fff' }}>{g.name}</strong>
                  <span style={{ fontSize: 11, color: '#64748b' }}>Score: {g.score}</span>
                </div>
                <div style={{ fontSize: 12, color: '#64748b', display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                  <span>WR: {g.win_rate}%</span>
                  <span>Avg P&L: <span className={g.avg_pnl >= 0 ? 'positive' : 'negative'}>${g.avg_pnl.toFixed(2)}</span></span>
                  <span>Cost/Trade: ${g.cost_per_trade.toFixed(4)}</span>
                  <span>Avg Tokens: {g.avg_output_tokens}</span>
                  <span>Trades: {g.total_trades}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Token Efficiency */}
      <div className="card" style={{ marginTop: 12 }}>
        <div className="card-header">
          <h2>Token Efficiency (Scan Calls)</h2>
          <span className="card-badge">Lower = cheaper per decision</span>
        </div>
        <div className="trade-list">
          {Object.entries(eff).sort((a, b) => (a[1].avg_total || 0) - (b[1].avg_total || 0)).map(([pid, e]) => (
            <div key={pid} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>{pid} <span style={{ color: '#64748b', fontSize: 12 }}>({e.num_calls} scans)</span></span>
              <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 12 }}>
                <span style={{ color: '#3b82f6' }}>{Math.round(e.avg_input || 0)} in</span>
                {' + '}
                <span style={{ color: '#a855f7' }}>{Math.round(e.avg_output || 0)} out</span>
                {' = '}
                <span style={{ fontWeight: 600 }}>{Math.round(e.avg_total || 0)} total</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </>
  )
}

function DiversityTab({ data }) {
  const div = data.diversity || {}
  const alerts = div.concentration_alerts || []
  const symbols = div.symbols_held || {}

  return (
    <>
      {/* Diversity Score */}
      <div className="arena-stat-cards" style={{ marginBottom: 16 }}>
        <div className="arena-stat-card">
          <div className="asc-label">Overlap Score</div>
          <div className="asc-value" style={{ color: div.diversity_rating === 'healthy' ? '#22c55e' : div.diversity_rating === 'moderate' ? '#eab308' : '#ef4444' }}>
            {div.avg_overlap_pct || 0}%
          </div>
        </div>
        <div className="arena-stat-card">
          <div className="asc-label">Diversity Rating</div>
          <div className="asc-value" style={{ color: div.diversity_rating === 'healthy' ? '#22c55e' : div.diversity_rating === 'moderate' ? '#eab308' : '#ef4444', fontSize: 16 }}>
            {(div.diversity_rating || 'unknown').replace('_', ' ').toUpperCase()}
          </div>
        </div>
        <div className="arena-stat-card">
          <div className="asc-label">Models with Positions</div>
          <div className="asc-value">{div.total_models_with_positions || 0}</div>
        </div>
        <div className="arena-stat-card">
          <div className="asc-label">Unique Symbols Held</div>
          <div className="asc-value">{Object.keys(symbols).length}</div>
        </div>
      </div>

      {/* Concentration Alerts */}
      {alerts.length > 0 && (
        <div className="card" style={{ marginBottom: 12 }}>
          <div className="card-header">
            <h2>Concentration Alerts</h2>
            <span className="card-badge" style={{ background: '#ef444422', color: '#ef4444' }}>
              {alerts.length} warnings
            </span>
          </div>
          <div className="trade-list">
            {alerts.map(a => (
              <div key={a.symbol} className="trade-item" style={{ display: 'flex', justifyContent: 'space-between' }}>
                <div>
                  <strong style={{ color: '#ef4444' }}>{a.symbol}</strong>
                  <span style={{ marginLeft: 8, color: '#64748b', fontSize: 12 }}>
                    Held by {a.holders} models
                  </span>
                </div>
                <span style={{ fontSize: 12, color: '#64748b' }}>{a.models.join(', ')}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Position Map */}
      <div className="card">
        <div className="card-header"><h2>Position Overlap Map</h2></div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, padding: 16 }}>
          {Object.entries(symbols).sort((a, b) => b[1] - a[1]).map(([sym, count]) => (
            <div key={sym} style={{
              padding: '6px 12px', borderRadius: 8, fontSize: 13, fontWeight: 600,
              background: count >= 5 ? '#ef444422' : count >= 3 ? '#eab30822' : '#22c55e22',
              color: count >= 5 ? '#ef4444' : count >= 3 ? '#eab308' : '#22c55e',
              border: `1px solid ${count >= 5 ? '#ef4444' : count >= 3 ? '#eab308' : '#22c55e'}33`,
            }}>
              {sym} ({count})
            </div>
          ))}
        </div>
      </div>
    </>
  )
}
