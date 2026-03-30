import React, { useState, useCallback, useMemo } from 'react'
import { usePolling } from '../hooks/usePolling'
import { api } from '../api/client'
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'

// Colorblind-safe palette
const AGENT_COLORS = {
  'super-agent': '#2563eb',
  'claude-sonnet': '#22c55e',
  'claude-haiku': '#16a34a',
  'gpt-4o': '#22c55e',
  'gpt-o3': '#16a34a',
  'gemini-2.5-pro': '#3b82f6',
  'gemini-2.5-flash': '#60a5fa',
  'grok-3': '#ef4444',
  'grok-4': '#f97316',
  'ollama-local': '#94a3b8',
  'ollama-kimi': '#06b6d4',
  'steve-webull': '#fbbf24',
  'options-sosnoff': '#f472b6',
  'energy-arnold': '#fb923c',
  'dalio-metals': '#facc15',
  'dayblade-sulu': '#38bdf8',
}
const FALLBACK = '#60a5fa'

function getStarting(pid) {
  if (pid === 'super-agent') return 25000
  if (pid === 'dayblade-0dte') return 5000
  if (pid === 'steve-webull') return 7049.68
  return 10000
}

export default function BubblePnL({ leaderboard }) {
  const [selectedAgent, setSelectedAgent] = useState('super-agent')

  const fetchCurve = useCallback(
    () => api.getEquityCurve(selectedAgent, -1),
    [selectedAgent]
  )
  const { data: curveData, loading } = usePolling(fetchCurve, 120000)

  // Build chart data from equity curve
  const chartData = useMemo(() => {
    if (!curveData || !Array.isArray(curveData)) return []
    return curveData.map(pt => ({
      time: pt.timestamp?.slice(5, 16)?.replace('T', ' ') || '',
      value: pt.total_value,
    }))
  }, [curveData])

  const starting = getStarting(selectedAgent)
  const color = AGENT_COLORS[selectedAgent] || FALLBACK

  // Build agent list from leaderboard
  const agents = useMemo(() => {
    if (!leaderboard || !Array.isArray(leaderboard)) return []
    return leaderboard
      .filter(p => p.player_id && !p.is_paused)
      .map(p => ({ id: p.player_id, name: p.name || p.player_id, total_value: p.total_value }))
  }, [leaderboard])

  // Use leaderboard total_value for P&L (live), fall back to equity curve last point
  const lbEntry = agents.find(a => a.id === selectedAgent)
  const current = lbEntry?.total_value
    || (chartData.length > 0 ? chartData[chartData.length - 1].value : null)
    || starting
  const pnl = current - starting
  const pnlPct = starting > 0 ? ((current - starting) / starting * 100) : 0
  const isUp = pnl >= 0

  return (
    <div className="card">
      <div className="card-header">
        <h2>P&L Tracker</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{
            fontFamily: 'monospace', fontSize: 14, fontWeight: 700,
            color: isUp ? '#2563eb' : '#ea580c',
          }}>
            {isUp ? '\u25B2' : '\u25BC'} {isUp ? '+' : ''}${pnl.toFixed(0)} ({pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(1)}%)
          </span>
        </div>
      </div>

      {/* Agent selector pills */}
      <div style={{
        display: 'flex', flexWrap: 'wrap', gap: 4,
        padding: '8px 14px', borderBottom: '1px solid #1e293b',
        alignItems: 'center',
      }}>
        {agents.map(a => {
          const active = a.id === selectedAgent
          const c = AGENT_COLORS[a.id] || FALLBACK
          return (
            <button
              key={a.id}
              onClick={() => setSelectedAgent(a.id)}
              style={{
                padding: '3px 8px', borderRadius: 12, fontSize: 10, fontWeight: 700,
                cursor: 'pointer',
                border: `1px solid ${active ? c : '#334155'}`,
                background: active ? `${c}22` : 'transparent',
                color: active ? c : '#64748b',
              }}
            >
              {a.name}
            </button>
          )
        })}
      </div>

      {/* Chart */}
      {loading && !chartData.length ? (
        <div className="loading" style={{ height: 200 }}>Loading...</div>
      ) : chartData.length === 0 ? (
        <div className="empty-state" style={{ height: 200 }}>No equity data for this agent yet.</div>
      ) : (
        <div style={{ padding: '8px 0' }}>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={color} stopOpacity={0.3} />
                  <stop offset="95%" stopColor={color} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <XAxis dataKey="time" tick={false} stroke="#1e293b" />
              <YAxis domain={['auto', 'auto']} tick={{ fontSize: 10, fill: '#64748b' }} stroke="#1e293b" width={55}
                tickFormatter={v => `$${(v/1000).toFixed(1)}k`} />
              <ReferenceLine y={starting} stroke="#334155" strokeDasharray="3 3" />
              <Tooltip
                contentStyle={{ background: '#1a1f2e', border: '1px solid #2d3348', borderRadius: 6, fontSize: 11 }}
                formatter={(v) => [`$${Number(v).toFixed(2)}`, 'Value']}
              />
              <Area type="monotone" dataKey="value" stroke={color} strokeWidth={2}
                fill="url(#pnlGrad)" dot={false} activeDot={{ r: 3 }} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}
