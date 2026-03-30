import React, { useCallback, useState } from 'react'
import { usePolling } from '../../hooks/usePolling'
import { api } from '../../api/client'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend, ReferenceLine } from 'recharts'

const PLAYER_COLORS = {
  'ollama-local': '#94a3b8',
  'claude-sonnet': '#22c55e',
  'claude-haiku': '#16a34a',
  'gpt-4o': '#22c55e',
  'gpt-o3': '#16a34a',
  'gemini-2.5-pro': '#3b82f6',
  'gemini-2.5-flash': '#60a5fa',
  'grok-3': '#ef4444',
  'grok-4': '#f97316',
  'ollama-kimi': '#06b6d4',
  'steve-webull': '#fbbf24',
  'options-sosnoff': '#f472b6',
  'energy-arnold': '#fb923c',
  'navigator': '#a3e635',
  'dalio-metals': '#facc15',
  'dayblade-sulu': '#38bdf8',
}

// Fallback color palette for unknown players
const FALLBACK_COLORS = ['#e879f9','#4ade80','#f87171','#fb923c','#a78bfa','#34d399','#60a5fa']

function playerColor(pid, index) {
  return PLAYER_COLORS[pid] || FALLBACK_COLORS[index % FALLBACK_COLORS.length]
}

export default function ComparisonChart({ season }) {
  const fetchComparison = useCallback(() => api.getComparison(season || undefined), [season])
  const { data, loading } = usePolling(fetchComparison, 120000)
  const [hiddenPlayers, setHiddenPlayers] = useState(new Set())

  if (loading || !data) return <div className="loading">Loading chart data...</div>

  const playerIds = Object.keys(data)
  if (playerIds.length === 0) {
    return <div className="empty-state">No performance data yet. Waiting for first scan cycle.</div>
  }

  const visibleIds = playerIds.filter(pid => !hiddenPlayers.has(pid))

  // Build unified timeline data
  const timeMap = {}
  for (const pid of visibleIds) {
    for (const point of data[pid].history) {
      const time = point.time
      if (!timeMap[time]) timeMap[time] = { time }
      timeMap[time][pid] = point.value
    }
  }
  const chartData = Object.values(timeMap).sort((a, b) => a.time.localeCompare(b.time))

  function togglePlayer(pid) {
    setHiddenPlayers(prev => {
      const next = new Set(prev)
      next.has(pid) ? next.delete(pid) : next.add(pid)
      return next
    })
  }

  function showAll() { setHiddenPlayers(new Set()) }
  function hideAll() { setHiddenPlayers(new Set(playerIds)) }

  return (
    <div>
      {/* Player toggle pills */}
      <div style={{
        display: 'flex', flexWrap: 'wrap', gap: 5,
        padding: '8px 16px 10px', borderBottom: '1px solid #1e293b',
        alignItems: 'center',
      }}>
        <span style={{ fontSize: 10, fontWeight: 700, color: '#475569', letterSpacing: 1, marginRight: 2 }}>CURVES</span>
        <button
          onClick={showAll}
          style={{
            padding: '3px 9px', borderRadius: 20, fontSize: 10, fontWeight: 700,
            cursor: 'pointer', border: '1px solid #334155',
            background: hiddenPlayers.size === 0 ? '#1e293b' : 'transparent',
            color: hiddenPlayers.size === 0 ? '#e2e8f0' : '#64748b',
          }}
        >
          All
        </button>
        <button
          onClick={hideAll}
          style={{
            padding: '3px 9px', borderRadius: 20, fontSize: 10, fontWeight: 700,
            cursor: 'pointer', border: '1px solid #334155',
            background: 'transparent', color: '#64748b',
          }}
        >
          None
        </button>
        <span style={{ width: 1, height: 16, background: '#1e293b', margin: '0 2px' }} />
        {playerIds.map((pid, i) => {
          const hidden = hiddenPlayers.has(pid)
          const color = playerColor(pid, i)
          const name = data[pid]?.name || pid
          return (
            <button
              key={pid}
              onClick={() => togglePlayer(pid)}
              title={hidden ? `Show ${name}` : `Hide ${name}`}
              style={{
                padding: '3px 10px', borderRadius: 20, fontSize: 11, fontWeight: 700,
                cursor: 'pointer',
                border: `1px solid ${hidden ? '#334155' : color}`,
                background: hidden ? 'transparent' : `${color}22`,
                color: hidden ? '#475569' : color,
                display: 'inline-flex', alignItems: 'center', gap: 5,
                textDecoration: hidden ? 'line-through' : 'none',
                opacity: hidden ? 0.5 : 1,
              }}
            >
              <span style={{
                width: 8, height: 8, borderRadius: '50%',
                background: hidden ? '#475569' : color, flexShrink: 0,
                ...(pid === 'steve-webull' ? { borderRadius: 2 } : {}),
              }} />
              {name}
            </button>
          )
        })}
      </div>

      <div className="chart-container">
        <ResponsiveContainer width="100%" height={350}>
          <LineChart data={chartData}>
            <XAxis dataKey="time" tick={false} stroke="#2d3348" />
            <YAxis domain={['auto', 'auto']} tick={{ fontSize: 11, fill: '#64748b' }} stroke="#2d3348" />
            <ReferenceLine y={10000} stroke="#2d3348" strokeDasharray="3 3" label={{ value: '$10k', fill: '#64748b', fontSize: 11 }} />
            <Tooltip
              contentStyle={{
                background: '#1a1f2e',
                border: '1px solid #2d3348',
                borderRadius: 8,
                fontSize: 12
              }}
              labelStyle={{ color: '#94a3b8' }}
              formatter={(value, name) => [`$${Number(value).toFixed(2)}`, data[name]?.name || name]}
            />
            {visibleIds.map((pid, i) => (
              <Line
                key={pid}
                type="monotone"
                dataKey={pid}
                stroke={playerColor(pid, i)}
                strokeWidth={pid === 'steve-webull' ? 3 : 2}
                strokeDasharray={pid === 'steve-webull' ? '8 4' : undefined}
                dot={false}
                connectNulls
                activeDot={{ r: 4 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
